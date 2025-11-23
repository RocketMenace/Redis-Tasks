from settings import get_cache_client, create_connection_pool, RedisSettings
from redis import Redis, ConnectionPool, RedisError
import logging
import json

logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")

logger = logging.getLogger(__name__)


class RedisQueue:
    def __init__(self, settings: RedisSettings):
        self._pool: ConnectionPool = create_connection_pool(settings=settings)
        self._client: Redis = get_cache_client(pool=self._pool)
        self._name = "main"

    def publish(self, msg: dict[str, int]) -> None:
        try:
            msg = json.dumps(msg)
            self._client.rpush(self._name, msg)
            logger.info("Message %s is published", msg)
        except RedisError as e:
            logger.error(
                "Failed to publish message %s to queue: %s because of error %s",
                msg,
                self._name,
                e,
            )
            raise e

    def consume(self) -> dict[str, int]:
        try:
            msg = self._client.lpop(name=self._name)
            logger.info("Message %s is consumed", msg)
            msg = json.loads(msg)
            return msg

        except RedisError as e:
            logger.error(
                "Failed to consume message %s because of error %s",
                e,
            )
            raise e


if __name__ == "__main__":
    redis_settings = RedisSettings(
        host="localhost", port=6379, db=1, decode_responses=True
    )
    q = RedisQueue(settings=redis_settings)

    q.publish({"a": 1})
    q.publish({"b": 2})
    q.publish({"c": 3})

    assert q.consume() == {"a": 1}
    assert q.consume() == {"b": 2}
    assert q.consume() == {"c": 3}
