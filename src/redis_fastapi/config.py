"""Configuration for fastapi-redis-sdk using Pydantic Settings.

Following FastAPI's recommended pattern for settings:
https://fastapi.tiangolo.com/advanced/settings
"""

from __future__ import annotations

import json
import re
import warnings
from functools import lru_cache
from importlib.metadata import PackageNotFoundError, version
from typing import Annotated, Any, Literal

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from redis.driver_info import DriverInfo

LIB_NAME: str = "fastapi-redis-sdk"
try:
    LIB_VERSION: str = version("fastapi-redis-sdk")
except PackageNotFoundError:
    # Package not installed (e.g. running from source via sys.path).
    from redis_fastapi import __version__ as LIB_VERSION
DRIVER_INFO: DriverInfo = DriverInfo().add_upstream_driver(LIB_NAME, LIB_VERSION)
CACHE_STATUS_HEADER: str = "X-Redis-Cache"

# Scope keys the caching and session features use to agree about a response,
# rather than each appending headers independently.  They live here because
# neither feature may import the other: caching must work with sessions absent,
# and deps.py already imports sessions, so cache -> sessions would be a cycle.
CACHE_ROUTE_SCOPE_KEY: str = "redis_cache_route"
"""Set by ``cache()``: this route owns its ``Cache-Control``."""
CACHE_SUPPRESS_VARY_SCOPE_KEY: str = "redis_cache_no_vary"
"""Set by ``cache(vary_on_session=False)``: the body does not vary by cookie."""

# Cookie attributes are interpolated into a response header, so each is
# constrained to characters that cannot terminate or split one.
_COOKIE_NAME_RE = re.compile(r"[A-Za-z0-9_-]+")
_COOKIE_DOMAIN_RE = re.compile(r"[A-Za-z0-9.-]+")
_CTL_RE = re.compile(r"[\x00-\x1f\x7f]")


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
    # -- Sessions --------------------------------------------------------------
    session_cookie_name: str = Field(
        default="session",
        description=(
            "Name of the session cookie.  Matches Starlette's and "
            "starsessions' default so a migration keeps existing cookie names."
        ),
    )
    session_cookie_domain: str | None = Field(
        default=None,
        description=(
            "Cookie Domain attribute.  None scopes the cookie to the exact "
            "host that set it; setting it also exposes the cookie to "
            "subdomains."
        ),
    )
    session_cookie_path: str = Field(
        default="/",
        description="Cookie Path attribute.",
    )
    session_cookie_same_site: Literal["lax", "strict", "none"] = Field(
        default="lax",
        description=(
            "Cookie SameSite attribute.  'none' requires "
            "session_cookie_https_only=True."
        ),
    )
    session_cookie_https_only: bool = Field(
        default=True,
        description=(
            "Add Secure to the session cookie, so the browser sends it over "
            "HTTPS only.  On by default; turn it off for local development "
            "over plain HTTP and nowhere else."
        ),
    )
    session_idle_ttl: int = Field(
        default=1800,
        ge=0,
        description=(
            "Idle clock, in seconds.  The session dies this long after the "
            "last request that carried its cookie.  Stored as the TTL of hash "
            "field 'd'.  0 disables the idle clock, and the field then takes "
            "session_gc_ttl."
        ),
    )
    session_absolute_ttl: int = Field(
        default=28800,
        ge=0,
        description=(
            "Absolute clock, in seconds.  The session dies this long after "
            "creation however active the user is.  Stored as the TTL of hash "
            "field 'a', which is written once and never refreshed.  0 disables "
            "it, and the field then takes session_gc_ttl."
        ),
    )
    session_gc_ttl: int = Field(
        default=2592000,
        gt=0,
        description=(
            "Backstop TTL for a field whose real deadline is unknown: "
            "cookie-only mode, or session_absolute_ttl=0.  Never reached in "
            "normal operation; it exists so Redis can always collect an "
            "abandoned key."
        ),
    )
    session_refresh_on_load: bool = Field(
        default=True,
        description=(
            "True: the load uses HGETEX, so any request carrying the cookie "
            "restarts the idle clock in the same round trip.  False: only a "
            "request that touched the session refreshes it, at the cost of a "
            "second round trip."
        ),
    )
    session_fail_closed: bool = Field(
        default=False,
        description=(
            "Behaviour when Redis is unreachable on READ.  False yields an "
            "empty session, so the caller looks anonymous and the "
            "application's own authorization rejects them.  True raises "
            "instead.  Writes always raise, whatever this is set to."
        ),
    )
    session_always_save: bool = Field(
        default=False,
        description=(
            "Write the payload on every request that touched the session, "
            "even when no mutation was detected.  The escape route for a "
            "change inside a nested value, which no dict subclass can see.  "
            "Costs a write on every request that read the session, so prefer "
            "reassigning the top-level key.  An empty session is exempt: "
            "reading one does not create it, because 'touched' includes a "
            "plain read and that would mint a key per anonymous visitor."
        ),
    )
    session_principal_keys: Annotated[list[str], NoDecode] = Field(
        default_factory=lambda: ["user_id"],
        description=(
            "Session keys the rotation trigger watches.  A change to any of "
            "them on a successful response rotates the session ID.  Add 'role' "
            "or 'scopes' for OWASP's privilege-change rotation.  From the "
            "environment, comma-separated: REDIS_SESSION_PRINCIPAL_KEYS="
            "user_id,role.  A JSON array is still accepted."
        ),
    )
    session_events_enabled: bool = Field(
        default=False,
        description=(
            "Subscribe to Redis notifications and call registered handlers "
            "when a session ends.  Best-effort: on a server that cannot "
            "supply them the store logs one warning at startup and the "
            "handlers never fire."
        ),
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

    # -- Cookie attribute validation -------------------------------------------
    #
    # These three are interpolated straight into a ``Set-Cookie`` header.  The
    # session *value* has been charset-checked since the first version
    # precisely because an unvalidated one is a header-injection vector; the
    # name, path and domain reach the same header by the same route and were
    # not checked at all.  A CR or LF in any of them splits the header.

    @field_validator("session_principal_keys", mode="before")
    @classmethod
    def _split_principal_keys(cls, value: Any) -> Any:
        """Accept ``user_id,role`` from the environment, and JSON as well.

        The only list-typed setting in this package, so this is the precedent
        rather than a break from one.  Comma-separated is what an operator
        will type, what every other tool in a ``.env`` file accepts, and what
        Django, Rails and Spring Boot all take for the same kind of setting.
        JSON-in-an-environment-variable is the outlier - and it was not a
        choice we made, it is what pydantic-settings does with any field whose
        annotation is not a scalar.

        ``NoDecode`` on the annotation is what makes this reachable.  Without
        it ``EnvSettingsSource`` calls ``json.loads`` on the raw string and
        raises ``SettingsError`` **before** any validator on this class runs,
        so a comma-separated value did not fail validation - the application
        did not start.  A ``mode="before"`` validator alone does not help; it
        never sees the value.

        The JSON branch is kept so that no existing configuration breaks.

        The one cost: a session key containing a comma cannot be named from
        the environment.  Session keys are Python identifiers in every
        realistic case, and the JSON form is still there for one that is not.

        **Empty is refused**, and this check is why splitting is not purely a
        convenience.  Before this validator, ``REDIS_SESSION_PRINCIPAL_KEYS=``
        left blank could not get through at all - ``json.loads("")`` fails and
        the application does not start.  Splitting a blank value yields ``[]``
        instead, and an empty list makes ``_default_principal_of`` return the
        same sentinel on every request, so no privilege change and no sign-in
        ever rotates the ID.  That is the fixation this feature exists to
        prevent, arrived at through a typo in a ``.env`` file.
        """
        if isinstance(value, str):
            text = value.strip()
            if text.startswith("["):
                try:
                    value = json.loads(text)
                except json.JSONDecodeError as exc:
                    raise ValueError(
                        "session_principal_keys looked like a JSON array but "
                        f"could not be parsed ({exc}); comma-separated is "
                        "simpler: user_id,role"
                    ) from exc
            else:
                # Empty parts dropped, so a trailing comma is not a key "".
                value = [part.strip() for part in text.split(",") if part.strip()]
        if isinstance(value, (list, tuple, set)) and not value:
            raise ValueError(
                "session_principal_keys must name at least one session key.  "
                "An empty list means no sign-in and no privilege change ever "
                "rotates the session ID, which is the fixation this feature "
                "exists to prevent.  Pass principal_of=... to decide "
                "rotation some other way."
            )
        return value

    @field_validator("session_cookie_name")
    @classmethod
    def _check_cookie_name(cls, value: str) -> str:
        """RFC 6265 token characters, narrowed to what a cookie name needs."""
        if not value or not _COOKIE_NAME_RE.fullmatch(value):
            raise ValueError(
                "session_cookie_name must be one or more of letters, digits, "
                f"'-' and '_'; got {value!r}"
            )
        return value

    @field_validator("session_cookie_path")
    @classmethod
    def _check_cookie_path(cls, value: str) -> str:
        if not value.startswith("/") or _CTL_RE.search(value) or ";" in value:
            raise ValueError(
                "session_cookie_path must start with '/' and contain no "
                f"control characters or ';'; got {value!r}"
            )
        return value

    @field_validator("session_cookie_domain")
    @classmethod
    def _check_cookie_domain(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if not value or not _COOKIE_DOMAIN_RE.fullmatch(value):
            raise ValueError(
                "session_cookie_domain must be a hostname of letters, digits, "
                f"'-' and '.'; got {value!r}"
            )
        return value

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
