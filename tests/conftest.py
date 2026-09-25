"""Shared test fixtures for fastapi-redis-sdk."""

import os
import sys
import uuid
from collections.abc import AsyncIterator, Generator

# Windows 3.10–3.11: ProactorEventLoop hangs with fakeredis / pytest-asyncio.
# Force SelectorEventLoop.  3.12+ works fine with Proactor, and 3.14 deprecates
# the policy API entirely.
if sys.platform == "win32" and sys.version_info < (3, 12):
    import asyncio

    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

import fakeredis
import fakeredis.aioredis
import pytest
import redis as sync_redis
import redis.asyncio as async_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.cache import cache
from redis_fastapi.deps import get_async_redis
from redis_fastapi.setup import FastAPIRedis

REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")

# Env-var keys that pydantic-settings would read via ``env_prefix="REDIS_"``.
# CI runners often export REDIS_HOST, REDIS_PORT, etc. for the service
# container, which pollutes ``model_fields_set`` and triggers spurious
# "url + KV overlap" warnings (promoted to errors by filterwarnings).
_REDIS_ENV_KEYS: frozenset[str] = frozenset(
    k for k in os.environ if k.startswith("REDIS_")
)


@pytest.fixture(autouse=True)
def _clean_redis_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """Remove REDIS_* env vars so RedisSettings is not polluted by CI."""
    for key in _REDIS_ENV_KEYS:
        monkeypatch.delenv(key, raising=False)


# ---------------------------------------------------------------------------
# Skip helpers
# ---------------------------------------------------------------------------


def _is_redis_available() -> bool:
    try:
        r = sync_redis.Redis.from_url(REDIS_URL)
        r.ping()
        r.close()
        return True
    except Exception:
        return False


requires_redis = pytest.mark.skipif(
    not _is_redis_available(), reason="Redis not reachable"
)


def about(actual: int, expected: int) -> bool:
    """TTL equality, allowing for a second boundary crossing mid-test.

    Redis counts down in whole seconds, so a TTL set to N reads back as N or
    N-1 depending on where the call landed.  Asserting equality makes the
    suite flaky for no gain; the guarantees under test are all about which
    clock moved, not about sub-second precision.

    **Use this, not ``<= N``, for "the clock did not move".**  ``HTTL``
    returns ``-2`` for a missing field and ``-1`` for a field with no expiry,
    and both satisfy ``<= N``.  A one-sided bound is therefore happiest of all
    about the strongest possible failure - the field being deleted outright -
    and it also passes when the deadline was *shortened*, which for the
    absolute clock is as wrong as extending it.
    """
    return expected - 1 <= actual <= expected


# ---------------------------------------------------------------------------
# Real Redis fixtures (integration)
# ---------------------------------------------------------------------------


@pytest.fixture()
def redis_url() -> str:
    return REDIS_URL


@pytest.fixture()
def real_redis(redis_url: str) -> Generator[sync_redis.Redis, None, None]:
    """Sync Redis client for integration tests. Flushes DB on teardown."""
    r = sync_redis.Redis.from_url(redis_url, decode_responses=True)
    yield r
    r.flushdb()
    r.close()


@pytest.fixture()
async def real_async_redis(
    redis_url: str,
) -> AsyncIterator[async_redis.Redis]:
    """Async Redis client for integration tests. Flushes DB on teardown."""
    r = async_redis.Redis.from_url(redis_url, decode_responses=True)
    yield r
    await r.flushdb()
    await r.aclose()


@pytest.fixture()
def test_prefix() -> str:
    """Unique prefix per test to avoid key collisions."""
    return f"test-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Fake Redis fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def fake_redis() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis()


@pytest.fixture()
def fake_async_redis() -> fakeredis.aioredis.FakeRedis:
    return fakeredis.aioredis.FakeRedis()


# ---------------------------------------------------------------------------
# App factory + client fixtures (DI-based)
# ---------------------------------------------------------------------------

_call_count: int = 0


def _create_test_app(fake_async: fakeredis.aioredis.FakeRedis) -> FastAPI:
    """Build a minimal FastAPI app with DI-based cached endpoints."""
    app = FastAPI()
    FastAPIRedis(app).caching()

    @app.get("/cached", dependencies=[Depends(cache(ttl=300))])
    async def cached_endpoint() -> dict:
        global _call_count
        _call_count += 1
        return {"value": _call_count}

    @app.get("/ns", dependencies=[Depends(cache(ttl=300, eviction_group="myns"))])
    async def grouped_endpoint() -> dict:
        return {"ns": True}

    async def _fake_async_redis() -> fakeredis.aioredis.FakeRedis:
        return fake_async

    app.dependency_overrides[get_async_redis] = _fake_async_redis

    return app


@pytest.fixture()
def app(fake_async_redis: fakeredis.aioredis.FakeRedis) -> FastAPI:
    global _call_count
    _call_count = 0
    return _create_test_app(fake_async_redis)


@pytest.fixture()
def client(app: FastAPI) -> Generator[TestClient, None, None]:
    """TestClient wrapping the DI-based cached app."""
    with TestClient(app) as c:
        yield c
