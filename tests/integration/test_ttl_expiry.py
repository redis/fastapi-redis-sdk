"""Integration tests for TTL expiry with real Redis.

Scenario #2: TTL expires -> next GET is MISS.
"""

from __future__ import annotations

import time
from collections.abc import Generator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.cache import cache
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import expire_now, requires_redis

_call_count: int = 0


def _build_app(ttl: int) -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/short-ttl", dependencies=[Depends(cache(ttl=ttl))])
    async def short_ttl() -> dict:
        global _call_count
        _call_count += 1
        return {"value": _call_count}

    return app


@pytest.fixture()
def short_ttl_client(
    real_redis: sync_redis.Redis,
) -> Generator[TestClient, None, None]:
    global _call_count
    _call_count = 0
    app = _build_app(ttl=1)
    with TestClient(app) as c:
        yield c
    real_redis.flushdb()


@requires_redis
@pytest.mark.integration
@pytest.mark.slow
class TestTTLExpiry:
    """Scenario #2: TTL expires -> next GET is MISS."""

    def test_cache_expires_after_ttl(
        self, short_ttl_client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        r1 = short_ttl_client.get("/short-ttl")
        assert r1.headers["X-Redis-Cache"] == "MISS"
        val1 = r1.json()["value"]

        # Should be a HIT immediately
        r2 = short_ttl_client.get("/short-ttl")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.json()["value"] == val1

        # The entry carries the route's TTL; let it run out
        (key,) = real_redis.scan_iter(match="*:cache:short-ttl")
        assert real_redis.ttl(key) in (0, 1)
        expire_now(real_redis, key)

        # Should be a MISS again
        r3 = short_ttl_client.get("/short-ttl")
        assert r3.headers["X-Redis-Cache"] == "MISS"
        assert r3.json()["value"] == val1 + 1

    def test_remaining_ttl_decreases(self, short_ttl_client: TestClient) -> None:
        r1 = short_ttl_client.get("/short-ttl")
        assert r1.headers["X-Redis-Cache"] == "MISS"
        max_age_1 = int(r1.headers["Cache-Control"].replace("max-age=", ""))
        assert max_age_1 == 1  # ttl=1

        # After a brief delay the remaining TTL should be <= original
        time.sleep(0.3)
        r2 = short_ttl_client.get("/short-ttl")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        max_age_2 = int(r2.headers["Cache-Control"].replace("max-age=", ""))
        assert max_age_2 <= max_age_1


_no_ttl_count: int = 0


def _build_no_ttl_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan().caching()

    @app.get("/no-ttl", dependencies=[Depends(cache())])
    async def no_ttl() -> dict:
        global _no_ttl_count
        _no_ttl_count += 1
        return {"value": _no_ttl_count}

    return app


@pytest.fixture()
def no_ttl_client(
    real_redis: sync_redis.Redis,
) -> Generator[TestClient, None, None]:
    global _no_ttl_count
    _no_ttl_count = 0
    app = _build_no_ttl_app()
    with TestClient(app) as c:
        yield c
    real_redis.flushdb()


@requires_redis
@pytest.mark.integration
class TestDefaultNoExpiry:
    """Default TTL (0) means cache entries persist indefinitely."""

    def test_no_ttl_entry_persists(
        self, no_ttl_client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        """Entry cached with default TTL=0 does not expire."""
        r1 = no_ttl_client.get("/no-ttl")
        assert r1.headers["X-Redis-Cache"] == "MISS"
        val1 = r1.json()["value"]

        # Immediate HIT
        r2 = no_ttl_client.get("/no-ttl")
        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.json()["value"] == val1

        # No TTL at all, so nothing can expire it
        (key,) = real_redis.scan_iter(match="*:cache:no-ttl")
        assert real_redis.ttl(key) == -1
        r3 = no_ttl_client.get("/no-ttl")
        assert r3.headers["X-Redis-Cache"] == "HIT"
        assert r3.json()["value"] == val1

    def test_no_ttl_cache_control_is_no_cache(self, no_ttl_client: TestClient) -> None:
        """Default TTL=0 emits Cache-Control: no-cache (revalidate via ETag)."""
        r = no_ttl_client.get("/no-ttl")
        cc = r.headers["Cache-Control"]
        assert cc == "no-cache"
        assert "max-age" not in cc

    def test_no_ttl_redis_key_has_no_expiry(
        self, no_ttl_client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        """Key stored with default TTL=0 has no TTL set in Redis."""
        no_ttl_client.get("/no-ttl")

        # Find the cache key
        keys = real_redis.keys("redis:fastapi:cache:*")
        assert len(keys) >= 1
        ttl = real_redis.ttl(keys[0])
        # -1 means no expiry set
        assert ttl == -1
