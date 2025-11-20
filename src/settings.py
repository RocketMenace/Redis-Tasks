from dataclasses import dataclass


@dataclass(frozen=True, eq=False, kw_only=True)
class RedisSettings:
    host: str
    port: int
    db: int
    decode_responses: bool
    max_connections: int


settings = RedisSettings(
    host="localhost", port=5432, db=1, decode_responses=True, max_connections=20
)
