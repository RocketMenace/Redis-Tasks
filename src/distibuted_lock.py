from datetime import timedelta
import time
import functools
import logging
from typing import Callable, Any
from redis import Redis, RedisError
from redis.asyncio import ConnectionPool
from redis.lock import Lock

from settings import get_cache_client, create_connection_pool, RedisSettings

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


class DistributedLockError(Exception):
    pass


class DistributedLock:
    def __init__(self, settings: RedisSettings):
        self._pool: ConnectionPool = create_connection_pool(settings=settings)
        self._client: Redis = get_cache_client(pool=self._pool)

    def acquire_lock(
        self,
        lock_key: str,
        timeout: float,
        blocking: bool = True,
        blocking_timeout: float | None = None,
    ) -> Lock | None:
        try:
            lock = Lock(
                redis=self._client,
                name=lock_key,
                timeout=timeout,
                sleep=0.1,
                blocking=blocking,
                blocking_timeout=blocking_timeout,
            )
            acquired = lock.acquire()
            if acquired:
                return lock
            return None
        except RedisError as e:
            logger.error("Failed to acquire lock %s: %s", lock_key, e)
            raise DistributedLockError(f"Failed to acquire lock: {e}") from e

    @staticmethod
    def release_lock(self, lock: Lock) -> None:
        try:
            lock.release()
        except RedisError as e:
            logger.error("Failed to release lock: %s", e)
            raise DistributedLockError(f"Failed to release lock: {e}") from e


_lock_manager: DistributedLock | None = None


def initialize_lock_manager(settings: RedisSettings) -> None:
    global _lock_manager
    _lock_manager = DistributedLock(settings=settings)


def single(
    max_processing_time: timedelta,
    blocking: bool = True,
    blocking_timeout: float | None = None,
):
    def decorator(func: Callable) -> Callable:
        if _lock_manager is None:
            raise RuntimeError(
                "Distributed lock manager not initialized. "
                "Call initialize_lock_manager(settings) first."
            )

        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            func_name = func.__name__
            lock_key = f"lock:{func_name}"

            timeout_seconds = max_processing_time.total_seconds()
            lock = _lock_manager.acquire_lock(
                lock_key=lock_key,
                timeout=timeout_seconds,
                blocking=blocking,
                blocking_timeout=blocking_timeout,
            )

            if lock is None:
                raise DistributedLockError(
                    f"Could not acquire lock for function {func_name}. "
                    "Another instance is already running."
                )

            try:
                logger.info("Acquired lock for function %s", func_name)
                result = func(*args, **kwargs)
                return result
            finally:
                _lock_manager.release_lock(lock)
                logger.info("Released lock for function %s", func_name)

        return wrapper

    return decorator


if __name__ == "__main__":
    redis_settings = RedisSettings(
        host="localhost", port=6379, db=0, decode_responses=True
    )
    initialize_lock_manager(redis_settings)

    @single(max_processing_time=timedelta(seconds=30))
    def process_transaction() -> None:
        time.sleep(20)
        print("Transaction processed")

    process_transaction()
