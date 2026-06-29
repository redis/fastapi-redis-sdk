"""Rate-limit backend abstraction for the INCREX window-counter limiter.

Provides a small, imperative API over Redis for the fixed-window
(window-counter) rate-limiting pattern.  The primary path uses the atomic
``INCREX`` command (Redis 8.8+); when the server does not support it the
backend transparently falls back to an equivalent atomic Lua script.

See https://redis.io/docs/latest/commands/increx/#pattern-window-counter-rate-limiter
"""

from __future__ import annotations

import logging
import math
import time
from dataclasses import dataclass
from typing import Any

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisError

from redis_fastapi.config import get_settings

logger = logging.getLogger(__name__)

# Atomic INCR + conditional-EXPIRE + bound-check, equivalent to
# ``INCREX key BYINT cost UBOUND limit EX window ENX`` for servers without
# INCREX (Redis < 8.8).  Run via EVAL.
#   KEYS[1] = counter key
#   ARGV[1] = cost, ARGV[2] = limit, ARGV[3] = window seconds
# Returns {new_value, actual_increment, pttl_ms}.
_WINDOW_COUNTER_SCRIPT = """
local cost   = tonumber(ARGV[1])
local limit  = tonumber(ARGV[2])
local window = tonumber(ARGV[3])
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
if current + cost > limit then
    return {current, 0, redis.call("PTTL", KEYS[1])}
end
local newval = redis.call("INCRBY", KEYS[1], cost)
if newval == cost then
    redis.call("EXPIRE", KEYS[1], window)
end
return {newval, cost, redis.call("PTTL", KEYS[1])}
"""


@dataclass
class _BackendCapabilities:
    """Process-lifetime cache of server capability detection.

    A single instance is shared by every per-request :class:`RateLimitBackend`
    (see :func:`~redis_fastapi.deps.get_rate_limit_backend`) so the ``INCREX``
    support of the connected server is discovered **once per process** rather
    than re-probed on every request.  ``None`` means "not yet known".

    Attributes:
        supports_increx: Whether the server accepts the ``INCREX`` command.
        script: The lazily-registered Lua window-counter script object.
    """

    supports_increx: bool | None = None
    script: object | None = None


@dataclass(frozen=True)
class RateLimitResult:
    """Outcome of a single rate-limit check.

    Attributes:
        allowed: Whether the request is within the limit.
        limit: The configured request limit for the window.
        remaining: Requests remaining in the current window (>= 0).
        reset_after: Seconds until the current window resets.
        reset_at: Unix epoch second at which the window resets.
        retry_after: Seconds the client should wait before retrying
            (mirrors ``reset_after``; meaningful on rejection).
        backend: Which execution path served the check — ``"increx"`` or
            ``"lua"``.  Empty for non-consuming reads
            (:meth:`RateLimitBackend.peek`) and the Redis-unreachable path.
        degraded: ``True`` when Redis was unreachable and this result is a
            fail-open/fail-closed fallback rather than a real counter check.
            Lets callers distinguish a genuine ``allowed``/``limited`` outcome
            from one produced during an outage (e.g. to record an ``error``
            metric or add a header).
    """

    allowed: bool
    limit: int
    remaining: int
    reset_after: int
    reset_at: int
    retry_after: int
    backend: str = ""
    degraded: bool = False


class RateLimitBackend:
    """Window-counter rate limiting backed by Redis.

    Args:
        redis: An async Redis client (provided via dependency injection).
        scope: Default scope segment for all keys.  Can be overridden per call.
        capabilities: Shared server-capability cache.  Pass the same instance
            across requests (the DI factory sources it from the pool state) so
            INCREX / EVAL support is detected once per process.  Defaults to a
            fresh per-instance cache for standalone use.
    """

    def __init__(
        self,
        redis: AsyncRedis | AsyncRedisCluster,
        *,
        scope: str = "",
        capabilities: _BackendCapabilities | None = None,
    ) -> None:
        self._redis = redis
        self._scope = scope
        settings = get_settings()
        self._prefix = settings.pattern_prefix("ratelimit")
        self._fail_closed = settings.rate_limit_fail_closed
        # Capability flags live on a shared, pool-lifetime object so the failed
        # round trip that detects an unsupported command is paid once per
        # process, not once per request.  The backend degrades INCREX
        # (Redis 8.8+) -> atomic Lua (Redis 2.6+).  A server supporting neither
        # (scripting disabled) surfaces as a backend error (fail-open/closed),
        # never as non-atomic counting.
        self._caps = (
            capabilities if capabilities is not None else _BackendCapabilities()
        )

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------

    def _build_key(self, identifier: str, scope: str | None = None) -> str:
        """Build a fully-qualified Redis key.

        Structure: ``{prefix}:{scope}:{identifier}`` — a **flat** key with **no**
        Redis Cluster hash-tag around the scope.  Rate limiting only ever does
        single-key operations (``hit`` / ``reset`` / ``peek``), so there is no
        bulk command that needs keys co-located on one slot.  Keeping the key
        flat lets Redis distribute per-client counters across all cluster slots
        (by CRC16 of the whole key), avoiding the hot shard a shared hash-tag
        would create and preserving horizontal scaling.
        """
        grp = scope if scope is not None else self._scope
        parts: list[str] = []
        if self._prefix:
            parts.append(self._prefix)
        if grp:
            parts.append(grp)
        parts.append(identifier)
        return ":".join(parts)

    # ------------------------------------------------------------------
    # Core operation
    # ------------------------------------------------------------------

    async def hit(
        self,
        identifier: str,
        *,
        limit: int,
        window: int,
        scope: str | None = None,
        cost: int = 1,
        fail_closed: bool | None = None,
    ) -> RateLimitResult:
        """Register a request against *identifier* and return the outcome.

        Increments the window counter by *cost*.  If the increment would
        exceed *limit* the counter is left unchanged and ``allowed`` is
        ``False``.

        On Redis errors the result depends on *fail_closed* (default from
        ``REDIS_RATE_LIMIT_FAIL_CLOSED``): fail-open returns ``allowed=True``,
        fail-closed returns ``allowed=False``.
        """
        full_key = self._build_key(identifier, scope)
        try:
            new_value, actual_incr, pttl_ms, backend = await self._execute(
                full_key, cost=cost, limit=limit, window=window
            )
        except (RedisError, OSError):
            closed = self._fail_closed if fail_closed is None else fail_closed
            logger.warning(
                "Rate-limit backend error for key '%s' (fail_%s)",
                full_key,
                "closed" if closed else "open",
                exc_info=True,
            )
            return self._degraded_result(limit, window, allowed=not closed)

        allowed = actual_incr != 0
        # PTTL is -1 (no expiry) or -2 (missing) in edge cases; treat as a
        # full fresh window rather than reporting a nonsensical reset.
        reset_after = math.ceil(pttl_ms / 1000) if pttl_ms > 0 else window
        remaining = max(0, limit - new_value)
        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=remaining,
            reset_after=reset_after,
            reset_at=int(time.time()) + reset_after,
            retry_after=reset_after,
            backend=backend,
        )

    async def _execute(
        self, full_key: str, *, cost: int, limit: int, window: int
    ) -> tuple[int, int, int, str]:
        """Run the best window-counter, returning ``(new, incr, pttl_ms, backend)``.

        Tries the atomic ``INCREX`` (Redis 8.8+), then the atomic Lua script.
        ``INCREX`` support is probed once and memoised on the shared capability
        cache after its first "unknown command" error, so the failed round trip
        is paid at most once per process.  Lua is the terminal tier: if the
        server supports neither (e.g. scripting disabled), the ``RedisError``
        propagates to :meth:`hit`, which turns it into the degraded
        fail-open/closed result rather than counting non-atomically.  ``backend``
        names the tier that served the check (``"increx"`` / ``"lua"``).
        """
        if self._caps.supports_increx is not False:
            try:
                new_value, actual_incr, pttl_ms = await self._increx(
                    full_key, cost=cost, limit=limit, window=window
                )
                self._caps.supports_increx = True
                return new_value, actual_incr, pttl_ms, "increx"
            except RedisError as exc:
                if not _is_unknown_command(exc):
                    raise
                # Redis < 8.8 - downgrade permanently to the Lua path.
                logger.info("INCREX unavailable; using the Lua window counter")
                self._caps.supports_increx = False

        new_value, actual_incr, pttl_ms = await self._eval_window_counter(
            full_key, cost=cost, limit=limit, window=window
        )
        return new_value, actual_incr, pttl_ms, "lua"

    async def _increx(
        self, full_key: str, *, cost: int, limit: int, window: int
    ) -> tuple[int, int, int]:
        """Execute ``INCREX`` and ``PTTL`` in one round-trip.

        ``INCREX`` is atomic and returns ``[new_value, actual_increment]`` but
        no TTL, so the window's remaining time needs a separate ``PTTL``.
        Pipelining the two (both target the same key, hence the same cluster
        slot) collapses what would otherwise be two sequential round-trips into
        one, halving the network cost of the fast path.  ``transaction=False``
        skips the ``MULTI``/``EXEC`` wrapping — ``INCREX`` is already atomic on
        its own and the ``PTTL`` read needs no isolation from it.

        Returns ``(new_value, actual_increment, pttl_ms)``.
        """
        pipe = self._redis.pipeline(transaction=False)
        pipe.execute_command(
            "INCREX",
            full_key,
            "BYINT",
            cost,
            "UBOUND",
            limit,
            "EX",
            window,
            "ENX",
        )
        pipe.pttl(full_key)
        increx_result, pttl_ms = await pipe.execute()
        new_value, actual_incr = increx_result[0], increx_result[1]
        return int(new_value), int(actual_incr), int(pttl_ms)

    async def _eval_window_counter(
        self, full_key: str, *, cost: int, limit: int, window: int
    ) -> tuple[int, int, int]:
        """Execute the Lua window-counter fallback, returning ``(new, incr, pttl)``."""
        if self._caps.script is None:
            self._caps.script = self._redis.register_script(_WINDOW_COUNTER_SCRIPT)
        result = await self._caps.script(  # type: ignore[operator]
            keys=[full_key], args=[cost, limit, window]
        )
        return int(result[0]), int(result[1]), int(result[2])

    @staticmethod
    def _degraded_result(limit: int, window: int, *, allowed: bool) -> RateLimitResult:
        """Build a result for the Redis-unreachable path."""
        return RateLimitResult(
            allowed=allowed,
            limit=limit,
            remaining=limit if allowed else 0,
            reset_after=window,
            reset_at=int(time.time()) + window,
            retry_after=window,
            degraded=True,
        )

    # ------------------------------------------------------------------
    # Auxiliary operations
    # ------------------------------------------------------------------

    async def reset(self, identifier: str, *, scope: str | None = None) -> bool:
        """Clear the counter for *identifier*.  Returns ``True`` if it existed."""
        full_key = self._build_key(identifier, scope)
        try:
            return bool(await self._redis.delete(full_key))
        except (RedisError, OSError):
            logger.warning(
                "Error resetting rate-limit key '%s'", full_key, exc_info=True
            )
            return False

    async def peek(
        self,
        identifier: str,
        *,
        limit: int,
        window: int,
        scope: str | None = None,
    ) -> RateLimitResult:
        """Report the current state without consuming a request."""
        full_key = self._build_key(identifier, scope)
        try:
            raw = await self._redis.get(full_key)
            pttl_ms = int(await self._redis.pttl(full_key))
        except (RedisError, OSError):
            logger.warning("Error reading rate-limit key '%s'", full_key, exc_info=True)
            return self._degraded_result(limit, window, allowed=not self._fail_closed)
        current = int(raw) if raw is not None else 0
        reset_after = math.ceil(pttl_ms / 1000) if pttl_ms > 0 else window
        return RateLimitResult(
            allowed=current < limit,
            limit=limit,
            remaining=max(0, limit - current),
            reset_after=reset_after,
            reset_at=int(time.time()) + reset_after,
            retry_after=reset_after,
        )


def _is_unknown_command(exc: RedisError) -> bool:
    """Heuristic: does *exc* indicate the server lacks the INCREX command?"""
    msg = str(exc).lower()
    return "unknown command" in msg or "wrong number of arguments" in msg


class SyncRateLimitBackend:
    """Synchronous facade over :class:`RateLimitBackend`.

    Every method delegates to the async backend via
    :func:`anyio.from_thread.run`.  Only usable from FastAPI-managed worker
    threads (sync endpoints / dependencies), mirroring ``SyncCacheBackend``.
    """

    def __init__(self, backend: RateLimitBackend) -> None:
        self._backend = backend

    @staticmethod
    def _run(func: Any, *args: Any) -> Any:
        import anyio.from_thread

        return anyio.from_thread.run(func, *args)

    def hit(
        self,
        identifier: str,
        *,
        limit: int,
        window: int,
        scope: str | None = None,
        cost: int = 1,
        fail_closed: bool | None = None,
    ) -> RateLimitResult:
        """Register a request and return the outcome (blocking)."""
        result: RateLimitResult = self._run(
            lambda: self._backend.hit(
                identifier,
                limit=limit,
                window=window,
                scope=scope,
                cost=cost,
                fail_closed=fail_closed,
            )
        )
        return result

    def reset(self, identifier: str, *, scope: str | None = None) -> bool:
        """Clear the counter for *identifier* (blocking)."""
        result: bool = self._run(lambda: self._backend.reset(identifier, scope=scope))
        return result

    def peek(
        self,
        identifier: str,
        *,
        limit: int,
        window: int,
        scope: str | None = None,
    ) -> RateLimitResult:
        """Report current state without consuming a request (blocking)."""
        result: RateLimitResult = self._run(
            lambda: self._backend.peek(
                identifier, limit=limit, window=window, scope=scope
            )
        )
        return result
