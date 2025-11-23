from dataclasses import dataclass
from redis import ConnectionPool, Redis


@dataclass(frozen=True, eq=False, kw_only=True)
class RedisSettings:
    host: str
    port: int
    db: int
    decode_responses: bool


def create_connection_pool(*, settings: RedisSettings) -> ConnectionPool:
    return ConnectionPool(
        host=settings.host,
        port=settings.port,
        db=settings.db,
        decode_responses=settings.decode_responses,
    )


def get_cache_client(*, pool: ConnectionPool) -> Redis:
    return Redis.from_pool(connection_pool=pool)
