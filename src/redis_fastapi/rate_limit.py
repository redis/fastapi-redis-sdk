"""Redis-backed fixed-window rate limiting for FastAPI endpoints.

Usage::

    from fastapi import Depends, FastAPI
    from redis_fastapi import FastAPIRedis, rate_limit

    app = FastAPI()
    FastAPIRedis(app).lifespan()

    @app.get("/items", dependencies=[Depends(rate_limit(limit=100, window=60))])
    async def get_items():
        return {"ok": True}
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from inspect import isawaitable
from typing import TYPE_CHECKING, Any, cast

from fastapi import Depends, HTTPException, Request
from redis.asyncio import Redis as AsyncRedis_
from redis.exceptions import RedisError, ResponseError
from starlette.status import HTTP_429_TOO_MANY_REQUESTS

from redis_fastapi.deps import _get_pool_state, get_async_redis
from redis_fastapi.telemetry import rate_limit_span, record_rate_limit_request

if TYPE_CHECKING:
    from redis_fastapi.deps import AsyncClient, _PoolState

logger = logging.getLogger(__name__)

# Lua script for atomic increment-bound-expiry fallback.
# Returns {current_value, actual_incr (0|1), retry_after_seconds}.
_RATE_LIMIT_LUA = """
local current = tonumber(redis.call("GET", KEYS[1]) or "0")
local limit = tonumber(ARGV[1])
local window = tonumber(ARGV[2])

if current >= limit then
    local ttl = redis.call("TTL", KEYS[1])
    return {current, 0, (ttl >= 0 and ttl or window)}
end

local next_value = redis.call("INCR", KEYS[1])

if next_value == 1 or redis.call("TTL", KEYS[1]) < 0 then
    redis.call("EXPIRE", KEYS[1], window)
end

local ttl = redis.call("TTL", KEYS[1])
return {next_value, 1, (ttl >= 0 and ttl or window)}
"""

DEFAULT_RATE_LIMIT_PREFIX = "redis:fastapi:ratelimit"

RateLimitKeyBuilder = Callable[..., str | Awaitable[str]]


def default_rate_limit_key_builder(request: Request) -> str:
    client = request.client.host if request.client else "unknown"
    path = request.url.path.strip("/").replace("/", ":")
    return f"{client}:{request.method}:{path}"


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    current: int
    remaining: int
    retry_after: int
    backend: str


async def _check_increx_supported(redis: AsyncClient) -> bool:
    try:
        await redis.execute_command("INCREX", "__ratelimit_probe__", "BYINT", 0)
        await redis.delete("__ratelimit_probe__")
        return True
    except ResponseError:
        return False


async def _check_rate_limit(
    redis: AsyncClient,
    key: str,
    limit: int,
    window: int,
    pool_state: _PoolState,
) -> RateLimitResult:
    if pool_state.increx_supported == "unknown":
        pool_state.increx_supported = (
            "supported" if await _check_increx_supported(redis) else "unsupported"
        )

    if pool_state.increx_supported == "supported":
        try:
            increx_result = await redis.execute_command(
                "INCREX",
                key,
                "BYINT",
                1,
                "UBOUND",
                limit,
                "EX",
                window,
                "ENX",
            )
            if increx_result is None:
                raise RedisError("INCREX returned None")
            raw_new_value, raw_actual_incr = increx_result
            new_value = int(raw_new_value)
            actual_incr = int(raw_actual_incr)
            allowed = actual_incr == 1
            remaining = max(0, limit - new_value)
            ttl = await redis.ttl(key)
            retry_after = max(0, ttl) if ttl >= 0 else window
            return RateLimitResult(
                allowed=allowed,
                current=new_value,
                remaining=remaining,
                retry_after=retry_after,
                backend="increx",
            )
        except ResponseError:
            pool_state.increx_supported = "unsupported"

    if pool_state._rate_limit_script is None:
        pool_state._rate_limit_script = cast(AsyncRedis_, redis).register_script(
            _RATE_LIMIT_LUA
        )

    raw_current, raw_actual_incr, raw_ttl = await pool_state._rate_limit_script(
        keys=[key],
        args=[str(limit), str(window)],
    )
    current_val = int(raw_current)
    actual_incr = int(raw_actual_incr)
    ttl = int(raw_ttl)

    allowed = actual_incr == 1
    remaining = max(0, limit - current_val)
    retry_after = max(0, ttl) if ttl >= 0 else window
    return RateLimitResult(
        allowed=allowed,
        current=current_val,
        remaining=remaining,
        retry_after=retry_after,
        backend="lua",
    )


def rate_limit(
    limit: int,
    window: int,
    *,
    key_builder: RateLimitKeyBuilder | None = None,
    prefix: str | None = None,
) -> Any:
    if limit < 1:
        raise ValueError("limit must be >= 1")
    if window < 1:
        raise ValueError("window must be >= 1")

    builder = key_builder or default_rate_limit_key_builder
    key_prefix = prefix or DEFAULT_RATE_LIMIT_PREFIX

    async def _dependency(
        request: Request,
        redis: AsyncClient = Depends(get_async_redis),
    ) -> None:
        pool_state = _get_pool_state(request.app)

        raw_key = builder(request)
        if isawaitable(raw_key):
            raw_key = await raw_key
        key = f"{key_prefix}:{raw_key}"

        try:
            result = await _check_rate_limit(redis, key, limit, window, pool_state)
        except RedisError:
            logger.warning(
                "Redis error during rate limit check, allowing request", exc_info=True
            )
            return

        span_attrs = {
            "rate_limit.key": key,
            "rate_limit.limit": limit,
            "rate_limit.window": window,
            "rate_limit.allowed": result.allowed,
            "rate_limit.remaining": result.remaining,
            "rate_limit.backend": result.backend,
        }
        with rate_limit_span("rate_limit.check", attributes=span_attrs):
            record_rate_limit_request(
                result="allowed" if result.allowed else "blocked",
            )

        if not result.allowed:
            raise HTTPException(
                status_code=HTTP_429_TOO_MANY_REQUESTS,
                detail="Too Many Requests",
                headers={"Retry-After": str(result.retry_after)},
            )

    return _dependency
