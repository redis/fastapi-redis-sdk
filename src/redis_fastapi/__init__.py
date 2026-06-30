"""fastapi-redis-sdk - The official Redis integration with FastAPI."""

__version__ = "0.1.0"

from redis_fastapi.cache import (
    CacheHitException,
    add_redis_caching,
    cache,
    cache_evict,
    cache_put,
    default_key_builder,
)
from redis_fastapi.cache_backend import CacheBackend, SyncCacheBackend
from redis_fastapi.config import RedisSettings, get_settings
from redis_fastapi.deps import (
    AsyncRedisDep,
    CacheBackendDep,
    SyncCacheBackendDep,
    get_async_redis,
    get_cache_backend,
    get_sync_cache_backend,
)
from redis_fastapi.lifespan import redis_lifespan
from redis_fastapi.rate_limit import (
    RateLimitResult,
    default_rate_limit_key_builder,
    rate_limit,
)
from redis_fastapi.setup import FastAPIRedis
from redis_fastapi.telemetry import disable_telemetry, enable_telemetry
from redis_fastapi.types import Coder, JsonCoder, KeyBuilder

__all__ = [
    "AsyncRedisDep",
    "CacheBackend",
    "CacheBackendDep",
    "CacheHitException",
    "Coder",
    "JsonCoder",
    "KeyBuilder",
    "FastAPIRedis",
    "RedisSettings",
    "SyncCacheBackend",
    "SyncCacheBackendDep",
    "add_redis_caching",
    "cache",
    "cache_evict",
    "cache_put",
    "default_key_builder",
    "default_rate_limit_key_builder",
    "disable_telemetry",
    "enable_telemetry",
    "get_async_redis",
    "get_cache_backend",
    "get_settings",
    "get_sync_cache_backend",
    "RateLimitResult",
    "rate_limit",
    "redis_lifespan",
]
