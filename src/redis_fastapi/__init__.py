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
    SessionDep,
    SessionStoreDep,
    SyncCacheBackendDep,
    SyncRateLimitBackendDep,
    SyncSessionStoreDep,
    get_async_redis,
    get_cache_backend,
    get_rate_limit_backend,
    get_session,
    get_session_store,
    get_sync_cache_backend,
    get_sync_rate_limit_backend,
    get_sync_session_store,
)
from redis_fastapi.lifespan import redis_lifespan
from redis_fastapi.rate import Rate, parse_rate
from redis_fastapi.ratelimit import (
    CannotIdentifyClient,
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
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionInfo,
    SessionMetadata,
    SessionRecord,
    SessionStore,
    SyncSessionStore,
)
from redis_fastapi.session_events import SessionEvents
from redis_fastapi.sessions import (
    CookieSpec,
    Session,
    SessionConfigurationError,
    SessionError,
    SessionMiddleware,
    SessionStoreError,
    add_redis_sessions,
    build_cookie,
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
    "CannotIdentifyClient",
    "Coder",
    "CookieSpec",
    "FastAPIRedis",
    "Identifier",
    "JsonCoder",
    "KeyBuilder",
    "Rate",
    "RateLimitBackend",
    "RateLimitBackendDep",
    "RateLimitExceeded",
    "RateLimitMiddleware",
    "RateLimitResult",
    "RedisSessionStore",
    "RedisSettings",
    "Session",
    "SessionConfigurationError",
    "SessionDep",
    "SessionError",
    "SessionEvents",
    "SessionInfo",
    "SessionMetadata",
    "SessionMiddleware",
    "SessionRecord",
    "SessionStore",
    "SessionStoreDep",
    "SessionStoreError",
    "SyncCacheBackend",
    "SyncCacheBackendDep",
    "SyncRateLimitBackend",
    "SyncRateLimitBackendDep",
    "SyncSessionStore",
    "SyncSessionStoreDep",
    "add_redis_caching",
    "add_redis_rate_limiting",
    "add_redis_sessions",
    "build_cookie",
    "cache",
    "cache_evict",
    "cache_put",
    "default_key_builder",
    "disable_telemetry",
    "enable_telemetry",
    "get_async_redis",
    "get_cache_backend",
    "get_rate_limit_backend",
    "get_session",
    "get_session_store",
    "get_settings",
    "get_sync_cache_backend",
    "get_sync_rate_limit_backend",
    "get_sync_session_store",
    "ip_identifier",
    "parse_rate",
    "pydantic_model_coder",
    "rate_limit",
    "redis_lifespan",
]
