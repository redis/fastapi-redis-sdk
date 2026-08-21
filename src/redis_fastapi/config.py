"""Configuration for fastapi-redis-sdk using Pydantic Settings.

Following FastAPI's recommended pattern for settings:
https://fastapi.tiangolo.com/advanced/settings
"""

from __future__ import annotations

import warnings
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from typing import Any

from pydantic import Field, SecretStr, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict
from redis.driver_info import DriverInfo

LIB_NAME: str = "fastapi-redis-sdk"
try:
    LIB_VERSION: str = version("fastapi-redis-sdk")
except PackageNotFoundError:
    # Package not installed (e.g. running from source via sys.path).
    from redis_fastapi import __version__ as LIB_VERSION
DRIVER_INFO: DriverInfo = DriverInfo().add_upstream_driver(LIB_NAME, LIB_VERSION)
CACHE_STATUS_HEADER: str = "X-Redis-Cache"


class RedisSettings(BaseSettings):
    """Central configuration for the FastAPI Redis integration.

    Supports two connection modes:

    1. **URL mode** (default): set ``url`` to a full Redis URL.
    2. **KV mode**: set ``host``, ``port``, ``db``, ``password``, etc.

    When ``url`` is provided it takes precedence over KV fields.

    All settings can be configured via environment variables with the ``REDIS_`` prefix.
    For example: ``REDIS_URL``, ``REDIS_HOST``, ``REDIS_PORT``, etc.

    Supports reading from ``.env`` files automatically.
    """

    # -- Connection: URL mode --------------------------------------------------
    url: str | None = Field(
        default=None,
        description="Full Redis connection URL (redis://...)",
    )

    # -- Connection: KV mode ---------------------------------------------------
    host: str = Field(
        default="localhost",
        description="Redis server hostname",
    )
    port: int = Field(
        default=6379,
        ge=1,
        le=65535,
        description="Redis server port (1-65535)",
    )
    db: int = Field(
        default=0,
        ge=0,
        description="Redis database number (0-15)",
    )
    username: str | None = Field(
        default=None,
        description="Redis username (Redis 6+)",
    )
    password: SecretStr | None = Field(
        default=None,
        description="Redis password (stored securely)",
    )

    # -- TLS -------------------------------------------------------------------
    ssl: bool = Field(
        default=False,
        description="Enable TLS/SSL encryption",
    )
    ssl_certfile: str | None = Field(
        default=None,
        description="Path to client certificate file",
    )
    ssl_keyfile: str | None = Field(
        default=None,
        description="Path to client private key file",
    )
    ssl_ca_certs: str | None = Field(
        default=None,
        description="Path to CA certificate bundle",
    )
    ssl_check_hostname: bool = Field(
        default=True,
        description="Verify hostname in TLS certificate",
    )

    # -- Pool ------------------------------------------------------------------
    max_connections: int | None = Field(
        default=None,
        ge=1,
        description="Maximum connections in pool (None = unbounded)",
    )
    socket_timeout: float | None = Field(
        default=None,
        ge=0,
        description="Socket read/write timeout in seconds",
    )
    socket_connect_timeout: float | None = Field(
        default=None,
        ge=0,
        description="Socket connect timeout in seconds",
    )

    # -- Cluster ---------------------------------------------------------------
    cluster: bool = Field(
        default=False,
        description="Enable Redis Cluster mode",
    )

    # -- Prefix ----------------------------------------------------------------
    prefix: str = Field(
        default="redis:fastapi",
        description="Global prefix for all Redis keys",
    )

    # -- Cache defaults --------------------------------------------------------
    default_ttl: int = Field(
        default=0,
        ge=0,
        description=(
            "Default cache TTL in seconds. "
            "0 means no automatic expiration (cache entries persist until "
            "explicitly evicted or removed by Redis eviction policy). "
            "Set a positive value to enable automatic expiry."
        ),
    )
    warn_unbounded_cache: bool = Field(
        default=True,
        description=(
            "Warn at startup when entries are cached without a TTL on a Redis "
            "server that cannot evict them - maxmemory 0, or a noeviction / "
            "volatile-* policy.  Set to false to silence the check."
        ),
    )

    # -- Rate limiting ---------------------------------------------------------
    rate_limit_default_limit: int = Field(
        default=0,
        ge=0,
        description=(
            "Default request limit for the app-wide global rate limiter. "
            "0 disables the global limiter (per-route rate_limit() dependencies "
            "still work).  Set a positive value to limit every route."
        ),
    )
    rate_limit_default_window: int = Field(
        default=60,
        ge=1,
        description="Default rate-limit window in seconds for the global limiter.",
    )
    rate_limit_fail_closed: bool = Field(
        default=False,
        description=(
            "When Redis is unreachable, reject requests (503/429) instead of "
            "failing open (allowing them).  Defaults to fail-open."
        ),
    )
    rate_limit_emit_headers: bool = Field(
        default=True,
        description="Emit X-RateLimit-Limit/-Remaining/-Reset response headers.",
    )
    rate_limit_ietf_headers: bool = Field(
        default=False,
        description=("Emit IETF draft RateLimit / RateLimit-Policy response headers."),
    )
    # -- Telemetry -------------------------------------------------------------
    otel_enabled: bool = Field(
        default=False,
        description="Enable OpenTelemetry instrumentation for cache operations",
    )
    otel_redis_enabled: bool = Field(
        default=False,
        description="Also initialize redis-py native OTel (connection/command metrics)",
    )

    # -- KV fields that are silently ignored when url is set -----------------
    _KV_FIELDS: frozenset[str] = frozenset(
        {"host", "port", "db", "username", "password"}
    )

    @model_validator(mode="after")
    def _warn_url_with_kv(self) -> RedisSettings:
        """Emit a warning when ``url`` is set alongside KV fields."""
        if self.url is not None:
            overlap = self._KV_FIELDS & self.model_fields_set
            if overlap:
                warnings.warn(
                    f"Both 'url' and {sorted(overlap)} are set. "
                    "When 'url' is provided the KV fields are ignored.",
                    UserWarning,
                    stacklevel=2,
                )
        return self

    # Pydantic Settings configuration
    model_config = SettingsConfigDict(
        env_prefix="REDIS_",  # All env vars start with REDIS_
        env_file=".env",  # Read from .env file if present
        env_file_encoding="utf-8",
        case_sensitive=False,  # REDIS_URL = redis_url = REDIS_url
        extra="ignore",  # Ignore extra env vars
    )

    def _tls_kwargs(self) -> dict[str, Any]:
        """Build SSL-related kwargs for ``ConnectionPool`` / ``from_url``."""
        if not self.ssl:
            return {}
        kw: dict[str, Any] = {"ssl": True}
        if self.ssl_certfile:
            kw["ssl_certfile"] = self.ssl_certfile
        if self.ssl_keyfile:
            kw["ssl_keyfile"] = self.ssl_keyfile
        if self.ssl_ca_certs:
            kw["ssl_ca_certs"] = self.ssl_ca_certs
        kw["ssl_check_hostname"] = self.ssl_check_hostname
        return kw

    def _pool_kwargs(self) -> dict[str, Any]:
        """Build pool-related kwargs shared by all pool constructors."""
        kw: dict[str, Any] = {"driver_info": DRIVER_INFO}
        if self.max_connections is not None:
            kw["max_connections"] = self.max_connections
        if self.socket_timeout is not None:
            kw["socket_timeout"] = self.socket_timeout
        if self.socket_connect_timeout is not None:
            kw["socket_connect_timeout"] = self.socket_connect_timeout
        kw.update(self._tls_kwargs())
        return kw

    def connection_kwargs(self) -> dict[str, Any]:
        """Return the full set of kwargs for pool/client construction.

        If ``url`` is set the dict contains ``{"url": ..., **pool_kwargs}``.
        Otherwise, it contains ``{"host": ..., "port": ..., **pool_kwargs}``.
        """
        kw = self._pool_kwargs()
        if self.url is not None:
            kw["url"] = self.url
        else:
            kw["host"] = self.host
            kw["port"] = self.port
            kw["db"] = self.db
            if self.username is not None:
                kw["username"] = self.username
            if self.password is not None:
                # Extract the secret value from SecretStr
                kw["password"] = self.password.get_secret_value()
        return kw

    def pattern_prefix(self, pattern: str) -> str:
        """Return the full prefix for a given pattern name.

        Example: ``settings.pattern_prefix("cache")`` → ``"redis:fastapi:cache"``
        """
        return f"{self.prefix}:{pattern}"


@lru_cache
def get_settings() -> RedisSettings:
    """Get cached RedisSettings instance.

    This function uses ``@lru_cache`` to return the same Settings object
    on every call, preventing reading from ``.env`` file multiple times.

    Following FastAPI's recommended pattern for settings:
    https://fastapi.tiangolo.com/advanced/settings

    Usage as a dependency in FastAPI endpoints:

        from redis_fastapi import get_settings
        from fastapi import Depends

        @app.get("/config")
        async def show_config(settings: Annotated[RedisSettings, Depends(get_settings)]):
            return {"host": settings.host}

    Usage in non-endpoint code:

        from redis_fastapi import get_settings

        settings = get_settings()
        print(settings.host)

    Returns:
        Cached settings instance loaded from environment variables and .env file.
    """
    return RedisSettings()
