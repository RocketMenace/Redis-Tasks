# Redis-Tasks

A Python library providing Redis-based utilities for distributed systems, including rate limiting, distributed locking, and message queuing.

## Modules

### `settings.py`
Redis connection configuration and connection pool management. Provides `RedisSettings` dataclass and helper functions for creating Redis connection pools and clients.

### `rate_limiter.py`
Sliding window rate limiter implementation using Redis sorted sets. Tracks request counts within a configurable time window and returns `True` if the limit hasn't been exceeded, `False` otherwise.

### `distibuted_lock.py`
Distributed locking mechanism using Redis locks. Provides a decorator `@single` to ensure only one instance of a function runs at a time across multiple processes or servers.

### `redis_queue.py`
Simple message queue implementation using Redis lists. Supports publishing and consuming JSON-serialized messages in a FIFO (first-in-first-out) manner.
