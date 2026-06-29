"""Unit tests for the rate_limit() dependency, identifiers, and middleware."""

from collections.abc import Generator
from unittest.mock import patch

import fakeredis.aioredis
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.deps import get_rate_limit_backend
from redis_fastapi.ratelimit import (
    RateLimitResult,
    _client_ip,
    _metric_result,
    ip_identifier,
)
from redis_fastapi.ratelimit_backend import RateLimitBackend
from redis_fastapi.setup import FastAPIRedis


def _build_app(
    fake: fakeredis.aioredis.FakeRedis,
    **builder_kwargs: object,
) -> FastAPI:
    """App with a backend bound to *fake* and the given rate_limiting() config."""
    app = FastAPI()
    FastAPIRedis(app).rate_limiting(**builder_kwargs)  # type: ignore[arg-type]

    backend = RateLimitBackend(fake)

    async def _override() -> RateLimitBackend:
        return backend

    app.dependency_overrides[get_rate_limit_backend] = _override
    return app


@pytest.fixture()
def fake(
    fake_async_redis: fakeredis.aioredis.FakeRedis,
) -> fakeredis.aioredis.FakeRedis:
    return fake_async_redis


# ---------------------------------------------------------------------------
# Identifiers
# ---------------------------------------------------------------------------


def _request(
    headers: dict[str, str] | None = None, client_host: str = "1.1.1.1"
) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/x",
        "headers": [
            (k.lower().encode(), v.encode()) for k, v in (headers or {}).items()
        ],
        "client": (client_host, 12345),
        "query_string": b"",
    }
    return Request(scope)


@pytest.mark.unit
class TestIdentifiers:
    def test_client_ip_ignores_xff_by_default(self) -> None:
        req = _request({"X-Forwarded-For": "9.9.9.9"})
        assert _client_ip(req, trust_proxy=False) == "1.1.1.1"

    def test_client_ip_honours_xff_when_trusted(self) -> None:
        req = _request({"X-Forwarded-For": "9.9.9.9, 10.0.0.1"})
        assert _client_ip(req, trust_proxy=True) == "9.9.9.9"

    def test_ip_identifier_is_client_ip_only(self) -> None:
        # Route separation lives in the scope (default: the route template), not
        # the identifier, so the identifier is the client IP only.
        assert ip_identifier(_request()) == "1.1.1.1"

    def test_custom_identifier_keys_by_arbitrary_request_attribute(self) -> None:
        # Per-user / per-API-key limiting is done by supplying your own
        # identifier callable — no built-in helper required.
        def api_key_identifier(request: Request) -> str:
            return request.headers.get("X-API-Key", "anonymous")

        req = _request({"X-API-Key": "abc123"})
        assert api_key_identifier(req) == "abc123"
        assert api_key_identifier(_request()) == "anonymous"


# ---------------------------------------------------------------------------
# Per-route dependency
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestRateLimitDependency:
    def _client(self, app: FastAPI) -> Generator[TestClient, None, None]:
        with TestClient(app) as c:
            yield c

    def test_allows_then_429(self, fake: fakeredis.aioredis.FakeRedis) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get("/limited", dependencies=[Depends(rate_limit("2/minute"))])
        async def limited() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            assert client.get("/limited").status_code == 200
            assert client.get("/limited").status_code == 200
            r = client.get("/limited")
            assert r.status_code == 429
            assert r.headers["Retry-After"]
            assert r.json() == {"detail": "Too Many Requests"}

    def test_headers_present_and_decreasing(
        self, fake: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get("/h", dependencies=[Depends(rate_limit("5/minute"))])
        async def h() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            r1 = client.get("/h")
            r2 = client.get("/h")
            assert r1.headers["X-RateLimit-Limit"] == "5"
            assert int(r1.headers["X-RateLimit-Remaining"]) == 4
            assert int(r2.headers["X-RateLimit-Remaining"]) == 3
            assert int(r1.headers["X-RateLimit-Reset"]) > 0
            assert "RateLimit-Policy" not in r1.headers  # ietf opt-in, off by default

    def test_ietf_headers_opt_in(self, fake: fakeredis.aioredis.FakeRedis) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get(
            "/i",
            dependencies=[Depends(rate_limit("5/minute", ietf_headers=True))],
        )
        async def i() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            r = client.get("/i")
            assert "q=5;w=60" in r.headers["RateLimit-Policy"]
            assert "RateLimit" in r.headers

    def test_header_families_are_independent(
        self, fake: fakeredis.aioredis.FakeRedis
    ) -> None:
        # IETF-only: X-RateLimit-* must NOT be forced on when emit_headers=False.
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get(
            "/ietf-only",
            dependencies=[
                Depends(rate_limit("5/minute", emit_headers=False, ietf_headers=True))
            ],
        )
        async def ietf_only() -> dict[str, str]:
            return {"ok": "yes"}

        @app.get(
            "/x-only",
            dependencies=[
                Depends(rate_limit("5/minute", emit_headers=True, ietf_headers=False))
            ],
        )
        async def x_only() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            ietf = client.get("/ietf-only")
            assert "RateLimit-Policy" in ietf.headers
            assert "X-RateLimit-Limit" not in ietf.headers  # not forced on

            x = client.get("/x-only")
            assert "X-RateLimit-Limit" in x.headers
            assert "RateLimit-Policy" not in x.headers

    def test_skip_when_bypasses(self, fake: fakeredis.aioredis.FakeRedis) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get(
            "/s",
            dependencies=[
                Depends(
                    rate_limit(
                        "1/minute",
                        skip_when=lambda r: r.headers.get("X-Internal") == "1",
                    )
                )
            ],
        )
        async def s() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            headers = {"X-Internal": "1"}
            # Many calls, all skipped → never 429, no headers emitted.
            for _ in range(5):
                r = client.get("/s", headers=headers)
                assert r.status_code == 200
                assert "X-RateLimit-Limit" not in r.headers

    def test_custom_on_limit_exceeded(self, fake: fakeredis.aioredis.FakeRedis) -> None:
        from redis_fastapi.ratelimit import rate_limit

        def custom(request: Request, result: RateLimitResult) -> JSONResponse:
            return JSONResponse({"error": "slow down"}, status_code=429)

        app = _build_app(fake)

        @app.get(
            "/c",
            dependencies=[Depends(rate_limit("1/minute", on_limit_exceeded=custom))],
        )
        async def c() -> dict[str, str]:
            return {"ok": "yes"}

        with TestClient(app) as client:
            assert client.get("/c").status_code == 200
            r = client.get("/c")
            assert r.status_code == 429
            assert r.json() == {"error": "slow down"}
            assert r.headers["Retry-After"]  # our headers still applied

    def test_scope_isolates_routes(self, fake: fakeredis.aioredis.FakeRedis) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        @app.get("/a", dependencies=[Depends(rate_limit("1/minute", scope="a"))])
        async def a() -> dict[str, str]:
            return {"r": "a"}

        @app.get("/b", dependencies=[Depends(rate_limit("1/minute", scope="b"))])
        async def b() -> dict[str, str]:
            return {"r": "b"}

        with TestClient(app) as client:
            assert client.get("/a").status_code == 200
            assert client.get("/b").status_code == 200  # different scope, own counter
            assert client.get("/a").status_code == 429

    def test_explicit_scope_shares_one_counter_across_routes(
        self, fake: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        # An explicit scope names one bucket shared by every route that uses it
        # (the old `shared=True`); the route template no longer separates them.
        limit = rate_limit("2/minute", scope="grp")

        @app.get("/x", dependencies=[Depends(limit)])
        async def x() -> dict[str, str]:
            return {"r": "x"}

        @app.get("/y", dependencies=[Depends(limit)])
        async def y() -> dict[str, str]:
            return {"r": "y"}

        with TestClient(app) as client:
            assert client.get("/x").status_code == 200
            assert client.get("/y").status_code == 200
            # Both routes drew from the same shared counter; the 3rd is over.
            assert client.get("/x").status_code == 429

    def test_default_scope_counts_routes_independently(
        self, fake: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        # No scope → each route's template is its scope, so counters are per-route.
        limit = rate_limit("1/minute")

        @app.get("/x", dependencies=[Depends(limit)])
        async def x() -> dict[str, str]:
            return {"r": "x"}

        @app.get("/y", dependencies=[Depends(limit)])
        async def y() -> dict[str, str]:
            return {"r": "y"}

        with TestClient(app) as client:
            assert client.get("/x").status_code == 200
            assert client.get("/y").status_code == 200  # own counter, own template
            assert client.get("/x").status_code == 429

    def test_path_params_share_one_bucket(
        self, fake: fakeredis.aioredis.FakeRedis
    ) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = _build_app(fake)

        # The default scope is the route *template*, not the concrete path, so
        # different path-parameter values share one counter (Flask-Limiter
        # parity) — a client cannot bypass the limit by varying the id.
        @app.get("/items/{item_id}", dependencies=[Depends(rate_limit("2/minute"))])
        async def item(item_id: int) -> dict[str, int]:
            return {"id": item_id}

        with TestClient(app) as client:
            assert client.get("/items/1").status_code == 200
            assert client.get("/items/2").status_code == 200
            # Third distinct id is still over the shared /items/{item_id} budget.
            assert client.get("/items/3").status_code == 429


# ---------------------------------------------------------------------------
# Global middleware
# ---------------------------------------------------------------------------


@pytest.mark.unit
class TestGlobalMiddleware:
    def test_global_limit_across_routes(
        self,
        fake: fakeredis.aioredis.FakeRedis,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        import redis_fastapi.ratelimit as rl
        from redis_fastapi.ratelimit_backend import RateLimitBackend

        async def _fake_backend(_request: Request) -> RateLimitBackend:
            return RateLimitBackend(fake)

        # The middleware resolves the backend directly (not via Depends).
        monkeypatch.setattr(rl, "get_rate_limit_backend", _fake_backend)

        app = FastAPI()
        # Constant key so the limit spans all routes, not per-path.
        FastAPIRedis(app).rate_limiting(
            global_rate="2/minute",
            identifier=lambda r: "global",
        )

        @app.get("/one")
        async def one() -> dict[str, str]:
            return {"r": "one"}

        @app.get("/two")
        async def two() -> dict[str, str]:
            return {"r": "two"}

        with TestClient(app) as client:
            assert client.get("/one").status_code == 200
            assert client.get("/two").status_code == 200
            # third request anywhere is over the shared global limit
            r = client.get("/one")
            assert r.status_code == 429
            assert r.headers["Retry-After"]


class _BrokenPipeline:
    """Pipeline whose execution always fails (simulates a downed server)."""

    def execute_command(self, *args: object, **kwargs: object) -> "_BrokenPipeline":
        return self

    def pttl(self, *args: object, **kwargs: object) -> "_BrokenPipeline":
        return self

    async def execute(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("redis down")


class _BrokenRedis:
    """Async Redis stand-in whose commands always fail."""

    def pipeline(self, *args: object, **kwargs: object) -> _BrokenPipeline:
        return _BrokenPipeline()

    async def execute_command(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("redis down")

    async def get(self, *args: object) -> object:
        raise RedisConnectionError("redis down")


@pytest.mark.unit
class TestMetricResult:
    """The telemetry ``result`` label reflects allowed / limited / error."""

    @staticmethod
    def _result(*, allowed: bool, degraded: bool = False) -> RateLimitResult:
        return RateLimitResult(
            allowed=allowed,
            limit=1,
            remaining=0,
            reset_after=1,
            reset_at=1,
            retry_after=1,
            degraded=degraded,
        )

    def test_mapping(self) -> None:
        assert _metric_result(self._result(allowed=True)) == "allowed"
        assert _metric_result(self._result(allowed=False)) == "limited"
        # A degraded (Redis-unreachable) result is "error" regardless of the
        # fail-open/closed outcome, so an outage is visible in metrics.
        assert _metric_result(self._result(allowed=True, degraded=True)) == "error"
        assert _metric_result(self._result(allowed=False, degraded=True)) == "error"

    def test_error_recorded_when_redis_unreachable(self) -> None:
        from redis_fastapi.ratelimit import rate_limit

        app = FastAPI()
        FastAPIRedis(app).rate_limiting()
        backend = RateLimitBackend(_BrokenRedis())  # type: ignore[arg-type]

        async def _override() -> RateLimitBackend:
            return backend

        app.dependency_overrides[get_rate_limit_backend] = _override

        @app.get("/e", dependencies=[Depends(rate_limit("1/minute"))])
        async def e() -> dict[str, str]:
            return {"ok": "yes"}

        with (
            patch("redis_fastapi.ratelimit.record_rate_limit_request") as spy,
            TestClient(app) as client,
        ):
            # Fail-open (default): the request is allowed despite Redis being
            # down, but the check is counted as an error, not a normal allow.
            assert client.get("/e").status_code == 200

        recorded = [call.kwargs.get("result") for call in spy.call_args_list]
        assert "error" in recorded
        assert "allowed" not in recorded
