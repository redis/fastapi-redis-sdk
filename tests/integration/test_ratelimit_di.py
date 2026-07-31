"""Integration tests for the declarative ``rate_limit()`` DI dependency.

Complements ``test_ratelimit_integration.py`` (backend only) and
``test_ratelimit_sync.py`` (sync facade only): this drives the full public
surface — ``Depends(rate_limit(...))``, the ``RateLimitMiddleware`` (header
injection **and** the global limiter), the ``RateLimitExceeded`` -> 429 handler,
and the ``X-RateLimit-*`` / IETF ``RateLimit`` headers — through ``TestClient``
against a **real Redis server**.

Each test builds its own app, keys counters off the request identity (the
``TestClient`` client host, or an ``X-Forwarded-For`` value when trusting a
proxy), and flushes the DB on teardown so counters never leak between tests.
"""

from __future__ import annotations

from unittest.mock import patch

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi import FastAPIRedis, rate_limit
from redis_fastapi.config import RedisSettings
from tests.conftest import requires_redis


@requires_redis
@pytest.mark.integration
class TestRateLimitDependency:
    """``Depends(rate_limit(...))`` end-to-end against a real Redis server."""

    def test_allows_then_rejects_with_headers(
        self, real_redis: sync_redis.Redis
    ) -> None:
        """First ``limit`` requests pass; the next is a 429 carrying headers."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get("/limited", dependencies=[Depends(rate_limit("2/minute"))])
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                r1 = tc.get("/limited")
                r2 = tc.get("/limited")
                r3 = tc.get("/limited")

            # Allowed responses carry the X-RateLimit-* trio, decreasing.
            assert r1.status_code == 200
            assert r1.headers["X-RateLimit-Limit"] == "2"
            assert int(r1.headers["X-RateLimit-Remaining"]) == 1
            assert int(r2.headers["X-RateLimit-Remaining"]) == 0
            assert int(r1.headers["X-RateLimit-Reset"]) > 0
            # IETF headers are opt-in and off by default.
            assert "RateLimit-Policy" not in r1.headers

            # Over the limit: 429 with Retry-After and the exhausted counter.
            assert r3.status_code == 429
            assert int(r3.headers["Retry-After"]) > 0
            assert r3.headers["X-RateLimit-Limit"] == "2"
            assert r3.headers["X-RateLimit-Remaining"] == "0"
            assert r3.json() == {"detail": "Too Many Requests"}
        finally:
            real_redis.flushdb()

    def test_ietf_headers_opt_in(self, real_redis: sync_redis.Redis) -> None:
        """``ietf_headers=True`` emits the standards-track RateLimit fields."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get(
            "/i",
            dependencies=[Depends(rate_limit("5/minute", ietf_headers=True))],
        )
        async def i() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                r = tc.get("/i")
            assert r.status_code == 200
            assert "q=5;w=60" in r.headers["RateLimit-Policy"]
            assert "RateLimit" in r.headers
        finally:
            real_redis.flushdb()

    def test_global_limiter_via_middleware(self, real_redis: sync_redis.Redis) -> None:
        """A global rate wired on the builder limits every route via middleware."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting(global_rate="1/minute")

        # No per-route dependency: the middleware enforces the app-wide limit.
        @app.get("/anything")
        async def anything() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                first = tc.get("/anything")
                second = tc.get("/anything")

            assert first.status_code == 200
            assert first.headers["X-RateLimit-Limit"] == "1"
            assert second.status_code == 429
            assert int(second.headers["Retry-After"]) > 0
        finally:
            real_redis.flushdb()


@requires_redis
@pytest.mark.integration
class TestTrustProxy:
    """``rate_limit_trust_proxy`` controls whether X-Forwarded-For sets identity."""

    def test_disabled_ignores_xff(self, real_redis: sync_redis.Redis) -> None:
        """Default (untrusted): XFF is ignored, so all callers share one bucket."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get("/limited", dependencies=[Depends(rate_limit("1/minute"))])
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                # Different XFF values, but the setting is off -> same counter.
                first = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
                second = tc.get("/limited", headers={"X-Forwarded-For": "8.8.8.8"})
            assert first.status_code == 200
            assert second.status_code == 429
        finally:
            real_redis.flushdb()

    def test_enabled_keys_by_xff(self, real_redis: sync_redis.Redis) -> None:
        """Trusted: each X-Forwarded-For client gets its own counter."""
        trusting = RedisSettings(rate_limit_trust_proxy=True)
        # `_client_ip` reads the setting at request time via ratelimit.get_settings;
        # patch it there and build the dependency under the patch.
        with patch("redis_fastapi.ratelimit.get_settings", return_value=trusting):
            app = FastAPI()
            FastAPIRedis(app).lifespan().rate_limiting()

            @app.get("/limited", dependencies=[Depends(rate_limit("1/minute"))])
            async def limited() -> dict[str, str]:
                return {"ok": "yes"}

            try:
                with TestClient(app) as tc:
                    # Two distinct proxied clients: each allowed once.
                    a1 = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
                    b1 = tc.get("/limited", headers={"X-Forwarded-For": "8.8.8.8"})
                    # The first client's second request exhausts its own bucket.
                    a2 = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
                assert a1.status_code == 200
                assert b1.status_code == 200  # separate counter, not shared
                assert a2.status_code == 429
            finally:
                real_redis.flushdb()
