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
    RateLimitBackendDep,
    SyncCacheBackendDep,
    SyncRateLimitBackendDep,
    get_async_redis,
    get_cache_backend,
    get_rate_limit_backend,
    get_sync_cache_backend,
    get_sync_rate_limit_backend,
)
from redis_fastapi.lifespan import redis_lifespan
from redis_fastapi.rate import Rate, parse_rate
from redis_fastapi.ratelimit import (
    Identifier,
    RateLimitExceeded,
    RateLimitMiddleware,
    add_redis_rate_limiting,
    ip_identifier,
    rate_limit,
)
from redis_fastapi.ratelimit_backend import (
    RateLimitBackend,
    RateLimitResult,
    SyncRateLimitBackend,
)
from redis_fastapi.setup import FastAPIRedis
from redis_fastapi.telemetry import disable_telemetry, enable_telemetry
from redis_fastapi.types import (
    Coder,
    JsonCoder,
    KeyBuilder,
    pydantic_model_coder,
)

__all__ = [
    "AsyncRedisDep",
    "CacheBackend",
    "CacheBackendDep",
    "CacheHitException",
    "Coder",
    "Identifier",
    "JsonCoder",
    "KeyBuilder",
    "FastAPIRedis",
    "Rate",
    "RateLimitBackend",
    "RateLimitBackendDep",
    "RateLimitExceeded",
    "RateLimitMiddleware",
    "RateLimitResult",
    "RedisSettings",
    "SyncCacheBackend",
    "SyncCacheBackendDep",
    "SyncRateLimitBackend",
    "SyncRateLimitBackendDep",
    "add_redis_caching",
    "add_redis_rate_limiting",
    "cache",
    "cache_evict",
    "cache_put",
    "default_key_builder",
    "disable_telemetry",
    "enable_telemetry",
    "get_async_redis",
    "get_cache_backend",
    "get_rate_limit_backend",
    "get_settings",
    "get_sync_cache_backend",
    "pydantic_model_coder",
    "get_sync_rate_limit_backend",
    "ip_identifier",
    "parse_rate",
    "rate_limit",
    "redis_lifespan",
]
