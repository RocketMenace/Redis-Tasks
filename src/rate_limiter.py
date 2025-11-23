import logging
import time
import uuid
from redis import Redis, RedisError
from redis.asyncio import ConnectionPool

from settings import get_cache_client, create_connection_pool, RedisSettings

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

logger = logging.getLogger(__name__)


class RateLimitExceed(Exception):
    pass


class RateLimiter:
    def __init__(self, settings: RedisSettings, max_requests: int = 5, window_seconds: int = 3):
        self._pool: ConnectionPool = create_connection_pool(settings=settings)
        self._client: Redis = get_cache_client(pool=self._pool)
        self.max_requests = max_requests
        self.window_seconds = window_seconds

    def test(self, key: str = "default") -> bool:
        try:
            redis_key = f"rate_limit:{key}"
            current_time = time.time()
            window_start = current_time - self.window_seconds

            self._client.zremrangebyscore(redis_key, 0, window_start)

            count = self._client.zcard(redis_key)

            if count < self.max_requests:
                request_id = str(uuid.uuid4())
                self._client.zadd(redis_key, {request_id: current_time})
                self._client.expire(redis_key, self.window_seconds + 1)
                return True
            return False
        except RedisError as e:
            logger.info("Error in rate limiter: %s", e)
            raise e


def make_api_request(rate_limiter: RateLimiter, key: str = "default"):
    if not rate_limiter.test(key=key):
        raise RateLimitExceed
    else:
        pass


if __name__ == "__main__":
    redis_settings = RedisSettings(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    limiter = RateLimiter(settings=redis_settings, max_requests=5, window_seconds=3)

    import random

    for i in range(20):
        time.sleep(random.uniform(0.1, 0.5))

        try:
            make_api_request(limiter, key="test_user")
            print(f"Request {i+1}: All good")
        except RateLimitExceed:
            print(f"Request {i+1}: Rate limit exceeded!")
