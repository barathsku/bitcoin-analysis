"""
API Client with retry logic, rate limiting, and exponential backoff.
"""

import time
import random
import logging
from typing import Dict, Any, Optional
from threading import Lock
import requests

logger = logging.getLogger(__name__)


class TokenBucket:
    """Thread-safe token bucket for rate limiting."""

    def __init__(self, tokens_per_minute: int):
        """
        Initialize token bucket.

        Args:
            tokens_per_minute: Number of tokens (requests) allowed per minute
        """
        self.capacity = tokens_per_minute
        self.tokens = tokens_per_minute
        self.fill_rate = tokens_per_minute / 60.0  # Tokens per second
        self.last_update = time.time()
        self.lock = Lock()

    def _refill(self):
        """Refill tokens based on elapsed time."""
        now = time.time()
        elapsed = now - self.last_update
        self.tokens = min(self.capacity, self.tokens + elapsed * self.fill_rate)
        self.last_update = now

    def acquire(self, tokens: int = 1) -> None:
        """
        Acquire tokens. Blocks until tokens are available.

        Args:
            tokens: Number of tokens to acquire
        """
        with self.lock:
            while True:
                self._refill()
                if self.tokens >= tokens:
                    self.tokens -= tokens
                    return
                # Calculate wait time until we have enough tokens
                tokens_needed = tokens - self.tokens
                wait_time = tokens_needed / self.fill_rate
                time.sleep(min(wait_time, 0.1))  # Sleep at most 100ms at a time


class APIClient:
    """HTTP client with exponential backoff retry and rate limiting."""

    def __init__(
        self,
        base_url: str,
        auth_config: Dict[str, Any],
        rate_limit_rpm: int = 30,
        max_retries: int = 3,
        timeout: int = 30,
    ):
        """
        Initialize API client.

        Args:
            base_url: Base URL for the API
            auth_config: Authentication configuration
            rate_limit_rpm: Requests per minute limit
            max_retries: Maximum number of retry attempts
            timeout: Request timeout in seconds
        """
        self.base_url = base_url.rstrip("/")
        self.auth_config = auth_config
        self.rate_limiter = TokenBucket(rate_limit_rpm)
        self.max_retries = max_retries
        self.timeout = timeout
        self.session = requests.Session()

    def _build_headers(self) -> Dict[str, str]:
        """Build request headers including authentication."""
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
        Calculate exponential backoff with jitter.

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
        Make an HTTP request with retry logic.

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
