"""Cache backend abstraction for dependency-injection based caching.

Provides a high-level caching API that handles serialization,
key prefixing, and eviction-group management on top of a Redis client.
"""

from __future__ import annotations

import logging
from datetime import timedelta
from typing import Any

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisError

from redis_fastapi.config import get_settings
from redis_fastapi.telemetry import (
    cache_span,
    record_cache_request,
    record_cache_write,
    timed_operation,
)
from redis_fastapi.types import Coder, JsonCoder

_MISSING = object()  # sentinel for distinguishing None from "no default"

logger = logging.getLogger(__name__)

# Server-side Lua script: SCAN + UNLINK in a single EVAL round trip.
# ARGV[1] = glob pattern, ARGV[2] = SCAN COUNT hint.
# Returns the total number of keys deleted.
# Keys are batched in groups of 1000 to avoid Lua stack overflow from unpack().
_DELETE_BY_PATTERN_SCRIPT = """
local cursor = "0"
local total  = 0
local batch  = 1000
repeat
    local result = redis.call("SCAN", cursor, "MATCH", ARGV[1], "COUNT", ARGV[2])
    cursor = result[1]
    local keys = result[2]
    for i = 1, #keys, batch do
        local j = math.min(i + batch - 1, #keys)
        total = total + redis.call("UNLINK", unpack(keys, i, j))
    end
until cursor == "0"
return total
"""


class CacheBackend:
    """High-level cache operations backed by Redis.

    Handles serialization, key building, and eviction-group management so
    callers don't need to work with raw Redis commands.

    Args:
        redis: An async Redis client (provided via dependency injection).
        eviction_group: Default eviction group for all keys.  Can be overridden per call.
        coder: Serializer for values.  Defaults to :class:`JsonCoder`.
    """

    def __init__(
        self,
        redis: AsyncRedis | AsyncRedisCluster,
        *,
        eviction_group: str = "",
        coder: type[Coder] | None = None,
    ) -> None:
        self._redis = redis
        self._eviction_group = eviction_group
        self._coder: type[Coder] = coder or JsonCoder
        settings = get_settings()
        self._prefix = settings.pattern_prefix("cache")

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _build_key(self, key: str, eviction_group: str | None = None) -> str:
        """Build a fully-qualified Redis key.

        Structure: ``{prefix}:{{{eviction_group}}}:{key}``

        The eviction group is wrapped in Redis hash-tag braces so that
        all keys sharing a group map to the same hash slot.  This is
        required for Lua-based bulk eviction in Redis Cluster and is
        harmless in standalone mode.

        Args:
            key: The bare cache key.
            eviction_group: Override the instance-level eviction group.
        """
        grp = eviction_group if eviction_group is not None else self._eviction_group
        parts: list[str] = []
        if self._prefix:
            parts.append(self._prefix)
        if grp:
            parts.append(f"{{{grp}}}")
        parts.append(key)
        return ":".join(parts)

    def _group_pattern(self, eviction_group: str | None = None) -> str:
        """Build a glob pattern matching all keys in an eviction group.

        Args:
            eviction_group: Override the instance-level eviction group.
        """
        grp = eviction_group if eviction_group is not None else self._eviction_group
        parts: list[str] = []
        if self._prefix:
            parts.append(self._prefix)
        if grp:
            parts.append(f"{{{grp}}}")
        parts.append("*")
        return ":".join(parts)

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def get(
        self,
        key: str,
        *,
        default: Any = _MISSING,
        eviction_group: str | None = None,
    ) -> Any | None:
        """Retrieve and deserialize a cached value.

        Args:
            key: The cache key to look up.
            default: Value to return on cache miss.  Defaults to ``None``.
            eviction_group: Override the instance-level eviction group for this call.

        Returns ``default`` (which is ``None`` if not provided) on cache
        miss or deserialization error.
        """
        _default = None if default is _MISSING else default
        grp = eviction_group if eviction_group is not None else self._eviction_group
        full_key = self._build_key(key, eviction_group)
        with cache_span(
            "cache.backend.get",
            attributes={"cache.key": full_key, "cache.eviction_group": grp},
        ) as span:
            with timed_operation("get", eviction_group=grp):
                try:
                    raw = await self._redis.get(full_key)
                except (RedisError, OSError):
                    logger.warning(
                        "Error reading cache key '%s'", full_key, exc_info=True
                    )
                    return _default
                if raw is None:
                    record_cache_request(result="miss", eviction_group=grp)
                    if span is not None:
                        span.set_attribute("cache.hit", False)
                    return _default
                record_cache_request(result="hit", eviction_group=grp)
                if span is not None:
                    span.set_attribute("cache.hit", True)
                try:
                    return self._coder.decode(
                        raw if isinstance(raw, str) else raw.decode()
                    )
                except (ValueError, UnicodeDecodeError):
                    logger.warning(
                        "Error decoding cache key '%s'", full_key, exc_info=True
                    )
                    return _default

    async def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: int | timedelta | None = None,
        eviction_group: str | None = None,
    ) -> None:
        """Serialize and store a value in the cache.

        Args:
            key: The cache key to store under.
            value: The value to serialize and cache.
            ttl: Time-to-live as seconds (``int``) or a
                :class:`~datetime.timedelta`.  ``None`` or value below ``1``
                means the key will not be automatically expired.
            eviction_group: Override the instance-level eviction group for this call.

        Raises:
            ValueError: If *ttl* is negative.
        """
        grp = eviction_group if eviction_group is not None else self._eviction_group
        full_key = self._build_key(key, eviction_group)
        ttl_seconds = int(ttl.total_seconds()) if isinstance(ttl, timedelta) else ttl
        with cache_span(
            "cache.backend.set",
            attributes={
                "cache.key": full_key,
                "cache.ttl": ttl_seconds or 0,
                "cache.eviction_group": grp,
            },
        ):
            with timed_operation("set", eviction_group=grp):
                try:
                    encoded = self._coder.encode(value)
                    if ttl_seconds is not None and ttl_seconds > 0:
                        await self._redis.set(full_key, encoded, ex=ttl_seconds)
                    else:
                        await self._redis.set(full_key, encoded)
                    record_cache_write(write_type="miss_fill", eviction_group=grp)
                except (RedisError, OSError):
                    logger.warning(
                        "Error writing cache key '%s'", full_key, exc_info=True
                    )

    async def delete(self, key: str, *, eviction_group: str | None = None) -> bool:
        """Delete a single cached entry.  Returns ``True`` if it existed.

        Args:
            key: The cache key to delete.
            eviction_group: Override the instance-level eviction group.
        """
        grp = eviction_group if eviction_group is not None else self._eviction_group
        full_key = self._build_key(key, eviction_group)
        with cache_span(
            "cache.backend.delete",
            attributes={"cache.key": full_key, "cache.eviction_group": grp},
        ):
            with timed_operation("evict", eviction_group=grp):
                try:
                    return bool(await self._redis.delete(full_key))
                except (RedisError, OSError):
                    logger.warning(
                        "Error deleting cache key '%s'", full_key, exc_info=True
                    )
                    return False

    async def has(self, key: str, *, eviction_group: str | None = None) -> bool:
        """Check whether a key exists without deserializing its value.

        Uses the Redis ``EXISTS`` command (O(1), no data transfer).

        Args:
            key: The cache key to check.
            eviction_group: Override the instance-level eviction group.
        """
        full_key = self._build_key(key, eviction_group)
        with cache_span(
            "cache.backend.has",
            attributes={"cache.key": full_key},
        ):
            try:
                return bool(await self._redis.exists(full_key))
            except (RedisError, OSError):
                logger.warning("Error checking cache key '%s'", full_key, exc_info=True)
                return False

    async def delete_group(
        self, eviction_group: str | None = None, *, scan_count: int = 500
    ) -> int:
        """Delete all keys in an eviction group.  Returns count of keys deleted.

        Attempts a server-side Lua script (``SCAN`` + ``UNLINK`` in a
        single ``EVAL`` round trip).  Falls back to Python-side
        ``scan_iter`` + ``delete`` when ``EVAL`` is unavailable (e.g.
        fakeredis in unit tests).

        .. warning::

            When *eviction_group* is ``None`` or empty **and** no instance-level
            eviction group was set, this deletes **all** cache keys under the
            global prefix (i.e. a full cache wipe).

        Args:
            eviction_group: Override the instance-level eviction group for this call.
                Pass ``None`` (or omit) to use the instance-level eviction group.
                If both are empty, **all** cached keys are deleted.
            scan_count: Hint passed to ``SCAN``'s ``COUNT`` option.  Larger
                values mean fewer internal iterations on the server at the
                cost of briefly blocking other commands.  Default ``500`` is
                a good balance for group eviction.
        """
        grp = eviction_group if eviction_group is not None else self._eviction_group
        pattern = self._group_pattern(eviction_group)
        if not grp:
            logger.warning(
                "delete_group() called without an eviction group — "
                "this will delete ALL cache keys matching '%s'",
                pattern,
            )
        with cache_span(
            "cache.backend.delete_group",
            attributes={"cache.eviction_group": grp},
        ) as span:
            with timed_operation("evict", eviction_group=grp):
                try:
                    count = await self._eval_delete(pattern, scan_count)
                except (RedisError, OSError):
                    # EVAL not supported (fakeredis, ACL restrictions, etc.)
                    # - fall back to Python-side scan + delete.
                    count = await self._scan_delete(pattern, eviction_group)
                if span is not None:
                    span.set_attribute("cache.keys_deleted", count)
                return count

    async def _eval_delete(self, pattern: str, scan_count: int) -> int:
        """Server-side SCAN + UNLINK via a Lua script (single round trip).

        Args:
            pattern: Glob pattern matching the keys to delete.
            scan_count: Hint for ``SCAN``'s ``COUNT`` option.
        """
        result = await self._redis.eval(
            _DELETE_BY_PATTERN_SCRIPT,
            0,  # numkeys - pattern is passed as ARGV
            pattern,
            str(scan_count),
        )
        return int(str(result))

    async def _scan_delete(self, pattern: str, eviction_group: str | None) -> int:
        """Fallback: Python-side scan_iter + batched delete.

        Args:
            pattern: Glob pattern matching the keys to delete.
            eviction_group: Eviction group label used for error logging.
        """
        deleted = 0
        batch_size = 1000
        try:
            batch: list[str] = []
            async for k in self._redis.scan_iter(match=pattern):
                batch.append(k)
                if len(batch) >= batch_size:
                    deleted += await self._redis.delete(*batch)
                    batch.clear()
            if batch:
                deleted += await self._redis.delete(*batch)
        except (RedisError, OSError):
            logger.warning(
                "Error clearing eviction group '%s'",
                eviction_group or self._eviction_group,
                exc_info=True,
            )
        return deleted


class SyncCacheBackend:
    """Synchronous facade over :class:`CacheBackend`.

    Every method delegates to the underlying async backend via
    :func:`anyio.from_thread.run`, which dispatches the coroutine back
    to the event loop that owns the current worker thread.

    .. warning::

        This class **only works from FastAPI-managed worker threads**
        (i.e. sync endpoints and sync dependencies).  Calling methods
        from the main thread or an arbitrary thread raises
        ``RuntimeError``.
    """

    def __init__(self, backend: CacheBackend) -> None:
        self._backend = backend

    @staticmethod
    def _run(func: Any, *args: Any) -> Any:
        """Call an async callable from a worker thread via the event loop."""
        import anyio.from_thread

        return anyio.from_thread.run(func, *args)

    def get(
        self,
        key: str,
        *,
        default: Any = _MISSING,
        eviction_group: str | None = None,
    ) -> Any | None:
        """Retrieve and deserialize a cached value (blocking)."""
        return self._run(
            lambda: self._backend.get(
                key, default=default, eviction_group=eviction_group
            )
        )

    def set(
        self,
        key: str,
        value: Any,
        *,
        ttl: int | timedelta | None = None,
        eviction_group: str | None = None,
    ) -> None:
        """Serialize and store a value in the cache (blocking)."""
        self._run(
            lambda: self._backend.set(
                key, value, ttl=ttl, eviction_group=eviction_group
            )
        )

    def delete(self, key: str, *, eviction_group: str | None = None) -> bool:
        """Delete a single cached entry (blocking).  Returns ``True`` if it existed."""
        result: bool = self._run(
            lambda: self._backend.delete(key, eviction_group=eviction_group)
        )
        return result

    def has(self, key: str, *, eviction_group: str | None = None) -> bool:
        """Check whether a key exists (blocking)."""
        result: bool = self._run(
            lambda: self._backend.has(key, eviction_group=eviction_group)
        )
        return result

    def delete_group(
        self, eviction_group: str | None = None, *, scan_count: int = 500
    ) -> int:
        """Delete all keys in an eviction group (blocking).  Returns count of keys deleted."""
        result: int = self._run(
            lambda: self._backend.delete_group(eviction_group, scan_count=scan_count)
        )
        return result
