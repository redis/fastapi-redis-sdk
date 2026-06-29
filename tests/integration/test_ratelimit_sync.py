"""Integration tests for SyncRateLimitBackendDep in sync (``def``) endpoints.

Mirrors ``tests/integration/test_cache_backend.py::TestSyncCacheBackendDep``:
a real FastAPI app with synchronous ``def`` endpoints that inject the sync
rate-limit facade, driven through ``TestClient`` against a real Redis server.

This exercises the full path a sync endpoint takes — FastAPI dispatches the
``def`` handler on the AnyIO worker threadpool, and ``SyncRateLimitBackend``
bridges each call back to the event loop via ``anyio.from_thread.run`` — which
the async-only unit tests cannot cover.

Each test uses the unique ``test_prefix`` as the rate-limit identifier and
flushes the DB on teardown so counters never leak between tests.
"""

from __future__ import annotations

import time

import pytest
import redis as sync_redis
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from redis_fastapi import FastAPIRedis
from redis_fastapi.deps import SyncRateLimitBackendDep
from tests.conftest import requires_redis


@requires_redis
@pytest.mark.integration
class TestSyncRateLimitBackendDep:
    """``SyncRateLimitBackendDep`` works from sync endpoints via the anyio bridge."""

    def test_hit_allows_then_rejects(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """First ``limit`` requests pass; subsequent ones are rejected (429)."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/limited")
        def limited(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=3, window=60)
            if not result.allowed:
                raise HTTPException(status_code=429, detail="rate limited")
            return {"remaining": result.remaining}

        with TestClient(app) as tc:
            codes = [tc.get("/limited").status_code for _ in range(5)]

        assert codes == [200, 200, 200, 429, 429]
        real_redis.flushdb()

    def test_remaining_counts_down(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """``remaining`` decreases by one per consumed request."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/limited")
        def limited(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=3, window=60)
            return {"allowed": result.allowed, "remaining": result.remaining}

        with TestClient(app) as tc:
            remaining = [tc.get("/limited").json()["remaining"] for _ in range(3)]

        assert remaining == [2, 1, 0]
        real_redis.flushdb()

    def test_cost_consumes_multiple(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """A request with ``cost > 1`` consumes the whole budget at once."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.post("/limited")
        def limited(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=10, window=60, cost=4)
            return {"allowed": result.allowed, "remaining": result.remaining}

        with TestClient(app) as tc:
            first = tc.post("/limited").json()
            second = tc.post("/limited").json()
            third = tc.post("/limited").json()  # would need 4, only 2 left

        assert first == {"allowed": True, "remaining": 6}
        assert second == {"allowed": True, "remaining": 2}
        assert third == {"allowed": False, "remaining": 2}  # counter unchanged
        real_redis.flushdb()

    def test_peek_does_not_consume(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """``peek`` reports state without spending from the window."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/peek")
        def peek(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.peek(test_prefix, limit=2, window=60)
            return {"allowed": result.allowed, "remaining": result.remaining}

        @app.get("/hit")
        def hit(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=2, window=60)
            return {"allowed": result.allowed, "remaining": result.remaining}

        with TestClient(app) as tc:
            # Peeking repeatedly must not consume the budget.
            assert tc.get("/peek").json() == {"allowed": True, "remaining": 2}
            assert tc.get("/peek").json() == {"allowed": True, "remaining": 2}
            # A real hit then consumes one slot.
            assert tc.get("/hit").json() == {"allowed": True, "remaining": 1}
            assert tc.get("/peek").json() == {"allowed": True, "remaining": 1}

        real_redis.flushdb()

    def test_reset_clears_counter(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """``reset`` clears the counter so the next request is allowed again."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/limited")
        def limited(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=1, window=60)
            return {"allowed": result.allowed}

        @app.post("/reset")
        def reset(rl: SyncRateLimitBackendDep) -> dict:
            return {"existed": rl.reset(test_prefix)}

        with TestClient(app) as tc:
            assert tc.get("/limited").json()["allowed"] is True
            assert tc.get("/limited").json()["allowed"] is False  # over limit
            assert tc.post("/reset").json()["existed"] is True
            # Counter cleared → allowed once more.
            assert tc.get("/limited").json()["allowed"] is True
            # Resetting a now-missing key reports it did not exist.
            tc.post("/reset")  # consume the freshly-created key
            assert tc.post("/reset").json()["existed"] is False

        real_redis.flushdb()

    def test_scope_isolation(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """Counters under different scopes are independent."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/limited/{scope}")
        def limited(scope: str, rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=1, window=60, scope=scope)
            return {"allowed": result.allowed}

        with TestClient(app) as tc:
            # Each scope gets its own single-slot budget.
            assert tc.get("/limited/a").json()["allowed"] is True
            assert tc.get("/limited/b").json()["allowed"] is True
            # Second hit on the same scope is rejected.
            assert tc.get("/limited/a").json()["allowed"] is False

        real_redis.flushdb()

    def test_window_expiry_resets(
        self, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """Once the window elapses, the counter resets and requests pass again."""
        app = FastAPI()
        FastAPIRedis(app).lifespan()

        @app.get("/limited")
        def limited(rl: SyncRateLimitBackendDep) -> dict:
            result = rl.hit(test_prefix, limit=1, window=1)
            return {"allowed": result.allowed}

        with TestClient(app) as tc:
            assert tc.get("/limited").json()["allowed"] is True
            assert tc.get("/limited").json()["allowed"] is False
            time.sleep(1.2)  # let the 1-second window expire
            assert tc.get("/limited").json()["allowed"] is True

        real_redis.flushdb()
