"""Integration tests for the declarative ``rate_limit()`` DI dependency.

Complements ``test_ratelimit_integration.py`` (backend only) and
``test_ratelimit_sync.py`` (sync facade only): this drives the full public
surface — ``Depends(rate_limit(...))``, the ``RateLimitMiddleware`` (header
injection **and** the global limiter), the ``RateLimitExceeded`` -> 429 handler,
and the ``X-RateLimit-*`` / IETF ``RateLimit`` headers — through ``TestClient``
against a **real Redis server**.

Each test builds its own app, keys counters off the request identity (the
``TestClient`` client host), and flushes the DB on teardown so counters never
leak between tests.
"""

from __future__ import annotations

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi import FastAPIRedis, rate_limit
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
class TestProxyHeadersAreNotTrusted:
    """``X-Forwarded-For`` never sets the rate-limit identity.

    Regression guard for the removed ``REDIS_RATE_LIMIT_TRUST_PROXY`` flag,
    which keyed the counter on the header's **left-most** hop.  Proxies append
    to XFF rather than overwrite it, so that hop is caller-supplied even behind
    a correctly configured proxy — a client could rotate identities to evade its
    own limit, or forge a victim's to exhaust theirs.  Proxy trust now belongs
    to the ASGI server; these tests pin the header out of the identity.
    """

    def test_varying_xff_shares_one_counter(self, real_redis: sync_redis.Redis) -> None:
        """Different XFF values from one peer hit the same bucket."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get("/limited", dependencies=[Depends(rate_limit("1/minute"))])
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                first = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
                second = tc.get("/limited", headers={"X-Forwarded-For": "8.8.8.8"})
            assert first.status_code == 200
            assert second.status_code == 429
        finally:
            real_redis.flushdb()

    def test_no_setting_re_enables_per_xff_keying(
        self, real_redis: sync_redis.Redis
    ) -> None:
        """Rotation is impossible by configuration, not merely off by default."""
        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get("/limited", dependencies=[Depends(rate_limit("2/minute"))])
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                # Three distinct forged hops against a limit of 2.  Under the old
                # flag each opened its own counter and all three passed.
                codes = [
                    tc.get("/limited", headers={"X-Forwarded-For": hop}).status_code
                    for hop in ("9.9.9.9", "8.8.8.8", "7.7.7.7")
                ]
            assert codes == [200, 200, 429]
        finally:
            real_redis.flushdb()

    def test_custom_identifier_can_opt_into_proxy_awareness(
        self, real_redis: sync_redis.Redis
    ) -> None:
        """The documented escape hatch for servers without proxy-header support.

        Mirrors the recipe in ``docs/guide/rate-limiting.md`` § Behind a proxy:
        gate on the immediate peer, then walk XFF right-to-left for the first
        hop the trust list does not cover.
        """
        trusted_peers = frozenset({"testclient"})

        def xff_identifier(request: Request) -> str:
            peer = request.client.host if request.client is not None else ""
            if peer not in trusted_peers:
                return peer  # untrusted peer: its own address is the identity
            hops = [
                hop.strip()
                for hop in request.headers.get("X-Forwarded-For", "").split(",")
                if hop.strip()
            ]
            for hop in reversed(hops):
                if hop not in trusted_peers:
                    return hop
            return peer

        app = FastAPI()
        FastAPIRedis(app).lifespan().rate_limiting()

        @app.get(
            "/limited",
            dependencies=[Depends(rate_limit("1/minute", identifier=xff_identifier))],
        )
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        try:
            with TestClient(app) as tc:
                # The peer is trusted here, so the forwarded hop is honoured and
                # the two proxied clients get separate counters.
                a1 = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
                b1 = tc.get("/limited", headers={"X-Forwarded-For": "8.8.8.8"})
                a2 = tc.get("/limited", headers={"X-Forwarded-For": "9.9.9.9"})
            assert a1.status_code == 200
            assert b1.status_code == 200
            assert a2.status_code == 429
        finally:
            real_redis.flushdb()
