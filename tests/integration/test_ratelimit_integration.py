"""Integration tests for rate limiting against a real Redis server.

Verify the atomic path (INCREX on Redis 8.8+, Lua otherwise) is actually
used and that concurrent requests never exceed the limit (no TOCTOU overshoot).
"""

import asyncio

import pytest
import redis.asyncio as async_redis

from redis_fastapi.ratelimit_backend import RateLimitBackend
from tests.conftest import requires_redis

pytestmark = [pytest.mark.integration, requires_redis, pytest.mark.asyncio]


async def test_allows_then_rejects(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    backend = RateLimitBackend(real_async_redis)
    outcomes = [
        (await backend.hit(test_prefix, limit=3, window=60)).allowed for _ in range(5)
    ]
    assert outcomes == [True, True, True, False, False]


async def test_uses_atomic_path(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    backend = RateLimitBackend(real_async_redis)
    result = await backend.hit(test_prefix, limit=5, window=60)
    # A real server must serve the check via an atomic tier (INCREX or Lua).
    assert result.backend in {"increx", "lua"}


async def test_concurrent_requests_never_exceed_limit(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    backend = RateLimitBackend(real_async_redis)
    limit = 10
    results = await asyncio.gather(
        *[backend.hit(test_prefix, limit=limit, window=60) for _ in range(50)]
    )
    allowed = sum(1 for r in results if r.allowed)
    assert allowed == limit  # atomic: no overshoot


async def test_window_expiry_resets(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    backend = RateLimitBackend(real_async_redis)
    r1 = await backend.hit(test_prefix, limit=1, window=1)
    assert r1.allowed
    assert (await backend.hit(test_prefix, limit=1, window=1)).allowed is False
    await asyncio.sleep(1.2)
    # window elapsed → counter reset
    assert (await backend.hit(test_prefix, limit=1, window=1)).allowed is True
