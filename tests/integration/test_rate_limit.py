"""Integration tests for Redis-backed rate limiting.

Tests the ``rate_limit()`` dependency through a full request -> Redis -> response
cycle against a real Redis instance.
"""

from __future__ import annotations

from collections.abc import Generator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.rate_limit import rate_limit
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

_call_count: int = 0


def _build_app() -> FastAPI:
    app = FastAPI()
    FastAPIRedis(app).lifespan()

    @app.get("/limited", dependencies=[Depends(rate_limit(limit=3, window=60))])
    async def limited_endpoint() -> dict:
        global _call_count
        _call_count += 1
        return {"value": _call_count}

    return app


@pytest.fixture()
def integ_client(
    real_redis: sync_redis.Redis,
) -> Generator[TestClient, None, None]:
    global _call_count
    _call_count = 0
    app = _build_app()
    with TestClient(app) as c:
        yield c
    real_redis.flushdb()


@requires_redis
@pytest.mark.integration
class TestRateLimitIntegration:
    def test_requests_under_limit_succeed(self, integ_client: TestClient) -> None:
        r1 = integ_client.get("/limited")
        assert r1.status_code == 200
        assert r1.json()["value"] == 1

        r2 = integ_client.get("/limited")
        assert r2.status_code == 200
        assert r2.json()["value"] == 2

        r3 = integ_client.get("/limited")
        assert r3.status_code == 200
        assert r3.json()["value"] == 3

    def test_request_over_limit_returns_429(self, integ_client: TestClient) -> None:
        for _ in range(3):
            integ_client.get("/limited")

        r4 = integ_client.get("/limited")
        assert r4.status_code == 429
        assert "Retry-After" in r4.headers

    def test_retry_after_header_present(self, integ_client: TestClient) -> None:
        for _ in range(3):
            integ_client.get("/limited")

        r = integ_client.get("/limited")
        retry_after = int(r.headers["Retry-After"])
        assert retry_after > 0

    def test_different_routes_have_independent_counters(
        self, integ_client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/a", dependencies=[Depends(rate_limit(limit=1, window=60))])
        async def a() -> dict:
            return {"ok": True}

        @app.get("/b", dependencies=[Depends(rate_limit(limit=1, window=60))])
        async def b() -> dict:
            return {"ok": True}

        with TestClient(app) as c:
            assert c.get("/a").status_code == 200
            assert c.get("/b").status_code == 200
            assert c.get("/a").status_code == 429
            assert c.get("/b").status_code == 429

        real_redis.flushdb()

    def test_sync_endpoint_supported(self, real_redis: sync_redis.Redis) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/sync", dependencies=[Depends(rate_limit(limit=5, window=60))])
        def sync_endpoint() -> dict:
            return {"ok": True}

        with TestClient(app) as c:
            for _ in range(5):
                r = c.get("/sync")
                assert r.status_code == 200

            r6 = c.get("/sync")
            assert r6.status_code == 429

        real_redis.flushdb()

    def test_window_expiry_allows_again(
        self, integ_client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        for _ in range(3):
            integ_client.get("/limited")

        r4 = integ_client.get("/limited")
        assert r4.status_code == 429

        real_redis.flushall()

        r5 = integ_client.get("/limited")
        assert r5.status_code == 200

    def test_custom_key_builder_separates_buckets(
        self, real_redis: sync_redis.Redis
    ) -> None:
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        def user_key_builder(request):
            return request.headers.get("X-User-Id", "default")

        @app.get(
            "/user-limited",
            dependencies=[
                Depends(
                    rate_limit(
                        limit=1,
                        window=60,
                        key_builder=user_key_builder,
                    )
                )
            ],
        )
        async def user_limited() -> dict:
            return {"ok": True}

        with TestClient(app) as c:
            r1 = c.get("/user-limited", headers={"X-User-Id": "alice"})
            assert r1.status_code == 200

            r2 = c.get("/user-limited", headers={"X-User-Id": "bob"})
            assert r2.status_code == 200

            r3 = c.get("/user-limited", headers={"X-User-Id": "alice"})
            assert r3.status_code == 429

        real_redis.flushdb()
