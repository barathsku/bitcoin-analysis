import pytest
from unittest.mock import MagicMock
import time
from common.core.api_client import RedisTokenBucket


class TestRedisTokenBucket:
    @pytest.fixture
    def mock_redis(self):
        redis_mock = MagicMock()
        script_mock = MagicMock()
        redis_mock.register_script.return_value = script_mock
        return redis_mock

    def test_init_registers_script(self, mock_redis):
        bucket = RedisTokenBucket(mock_redis, "test_key", 60)
        assert bucket.key == "rate_limit:test_key"
        assert bucket.capacity == 60
        assert bucket.rate == 1.0
        mock_redis.register_script.assert_called_once_with(RedisTokenBucket.LUA_SCRIPT)

    def test_acquire_success(self, mock_redis):
        bucket = RedisTokenBucket(mock_redis, "test_key", 60)

        # Configure script mock to return success (allowed=1, wait_time=0)
        bucket.script.return_value = [1, 0]

        start_time = time.time()
        bucket.acquire(1)
        end_time = time.time()

        # Should be fast (no sleep)
        assert end_time - start_time < 0.1

        # Verify script was called with correct args
        # args=[rate, capacity, tokens, now]
        call_args = bucket.script.call_args
        assert call_args[1]["keys"] == ["rate_limit:test_key"]
        args = call_args[1]["args"]
        assert args[0] == 1.0  # rate
        assert args[1] == 60  # capacity
        assert args[2] == 1  # tokens

    def test_acquire_blocks_when_rate_limited(self, mock_redis):
        bucket = RedisTokenBucket(mock_redis, "test_key", 60)

        # First call returns wait (allowed=0, wait_time=0.2)
        # Second call returns success (allowed=1, wait_time=0)
        bucket.script.side_effect = [[0, 0.2], [1, 0]]

        start_time = time.time()
        bucket.acquire(1)
        end_time = time.time()

        # Should have slept at least 0.2s
        assert end_time - start_time >= 0.2

        # Verify both calls were made
        assert bucket.script.call_count == 2
