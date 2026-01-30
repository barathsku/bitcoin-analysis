"""
API Client with retry logic, rate limiting, and exponential backoff
"""

import time
import random
import logging
from typing import Dict, Any, Optional
import requests

logger = logging.getLogger(__name__)


class RedisTokenBucket:
    """Token bucket using Redis for rate limiting across different Airflow workers"""

    # Lua script for atomic token bucket operations
    # Keys: [rate_limit_key]
    # Args: [rate (tokens/sec), capacity, requested_tokens, current_timestamp]
    # Returns: [allowed (0/1), sleep_time_needed]
    LUA_SCRIPT = """
    local key = KEYS[1]
    local rate = tonumber(ARGV[1])
    local capacity = tonumber(ARGV[2])
    local requested = tonumber(ARGV[3])
    local now = tonumber(ARGV[4])

    local tokens_key = key .. ":tokens"
    local timestamp_key = key .. ":ts"

    local last_tokens = tonumber(redis.call("get", tokens_key))
    if last_tokens == nil then
        last_tokens = capacity
    end

    local last_refill = tonumber(redis.call("get", timestamp_key))
    if last_refill == nil then
        last_refill = now
    end

    local delta = math.max(0, now - last_refill)
    local filled_tokens = math.min(capacity, last_tokens + (delta * rate))
    
    if filled_tokens >= requested then
        local new_tokens = filled_tokens - requested
        redis.call("set", tokens_key, new_tokens)
        redis.call("set", timestamp_key, now)
        -- Set expiry to avoid stale keys (1 hour)
        redis.call("expire", tokens_key, 3600)
        redis.call("expire", timestamp_key, 3600)
        return {1, 0}
    else
        local missing = requested - filled_tokens
        local wait_time = missing / rate
        return {0, wait_time}
    end
    """

    def __init__(self, redis_client: Any, key: str, tokens_per_minute: int):
        """
        Initialize distributed token bucket

        Args:
            redis_client: Initialized redis.Redis client
            key: Unique key for this rate limiter
            tokens_per_minute: Number of tokens allowed per minute
        """
        self.redis = redis_client
        self.key = f"rate_limit:{key}"
        self.capacity = tokens_per_minute
        self.rate = tokens_per_minute / 60.0  # tokens per second
        self.script = self.redis.register_script(self.LUA_SCRIPT)

    def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens, blocks incoming requests until tokens are available

        Args:
            tokens: Number of tokens to acquire
        """
        while True:
            now = time.time()
            allowed, wait_time = self.script(
                keys=[self.key], args=[self.rate, self.capacity, tokens, now]
            )

            if allowed:
                return

            sleep_time = float(wait_time)
            time.sleep(min(sleep_time + 0.01, 1.0))


class APIClient:
    """HTTP client with exponential backoff retry and rate limiting"""

    def __init__(
        self,
        base_url: str,
        auth_config: Dict[str, Any],
        rate_limit_rpm: int = 30,
        max_retries: int = 3,
        timeout: int = 30,
        redis_client: Optional[Any] = None,
        rate_limit_key: Optional[str] = None,
    ):
        """
        Initialize API client

        Args:
            base_url: Base URL for the API
            auth_config: Authentication configuration
            rate_limit_rpm: Requests per minute limit
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
            redis_client: Optional Redis client for distributed rate limiting
            rate_limit_key: Key to use for distributed rate limiting
        """
        self.base_url = base_url.rstrip("/")
        self.auth_config = auth_config

        if redis_client and rate_limit_key:
            self.rate_limiter = RedisTokenBucket(
                redis_client, rate_limit_key, rate_limit_rpm
            )
        else:
            logger.warning(
                "No rate limiter configured (Redis client missing). Requests will not be throttled."
            )
            self.rate_limiter = None

        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers including authentication"""
        headers = {}

        if self.auth_config.get("type") == "header":
            header_name = self.auth_config["header_name"]
            api_key = self.auth_config.get("api_key", "")
            prefix = self.auth_config.get("prefix", "")
            # Only add prefix if it's defined and not empty
            if prefix:
                headers[header_name] = f"{prefix}{api_key}"
            else:
                headers[header_name] = api_key

        return headers

    def _exponential_backoff(self, attempt: int) -> float:
        """
        Calculate exponential backoff with jitter

        Args:
            attempt: Current retry attempt number (0-indexed)

        Returns:
            Sleep duration in seconds
        """
        base_delay = 2**attempt  # Exponential: 1, 2, 4, 8, ...
        jitter = random.uniform(0, 1)  # Random jitter
        return min(base_delay + jitter, 60)  # Cap at 60 seconds

    def request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        json_data: Optional[Dict[str, Any]] = None,
        headers: Optional[Dict[str, str]] = None,
    ) -> Dict[str, Any]:
        """
        Make an HTTP request with retry logic

        Args:
            method: HTTP method (GET, POST, etc.)
            endpoint: API endpoint (relative to base_url)
            params: Query parameters
            json_data: JSON request body
            headers: Additional headers

        Returns:
            Response JSON as dict

        Raises:
            requests.exceptions.RequestException: If all retries failed
        """
        if self.rate_limiter:
            self.rate_limiter.acquire()

        if endpoint.startswith("http"):
            url = endpoint
        else:
            url = f"{self.base_url}{endpoint}"

        request_headers = self._build_headers()
        if headers:
            request_headers.update(headers)

        last_exception = None
        for attempt in range(self.max_retries):
            try:
                logger.info(
                    f"Request {method} {url} (attempt {attempt + 1}/{self.max_retries})"
                )

                response = self.session.request(
                    method=method,
                    url=url,
                    params=params,
                    json=json_data,
                    headers=request_headers,
                    timeout=self.timeout,
                )

                response.raise_for_status()

                logger.info(f"Success: {method} {url}")
                return response.json()

            except requests.exceptions.HTTPError as e:
                # Don't retry on 4xx client errors (except 429 rate limit)
                if (
                    400 <= e.response.status_code < 500
                    and e.response.status_code != 429
                ):
                    logger.error(f"Client error {e.response.status_code}: {e}")
                    raise

                last_exception = e
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")

            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as e:
                last_exception = e
                logger.warning(f"Network error (attempt {attempt + 1}): {e}")

            if attempt < self.max_retries - 1:
                backoff_time = self._exponential_backoff(attempt)
                logger.info(f"Retrying in {backoff_time:.2f} seconds...")
                time.sleep(backoff_time)

        logger.error(f"All {self.max_retries} retries failed for {method} {url}")
        raise last_exception
