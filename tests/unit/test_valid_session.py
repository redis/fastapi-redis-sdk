"""Unit tests for ``valid_session()``, against ``fakeredis``.

The gate answers one question - did this request arrive with a session this
application created in an earlier response? - and, with ``issued_within``, a
second: was that session's ID issued recently?  Each test below pins one line
of ``docs/specs/session-di-factory-research.md`` (S-1, S-1.5, S-1.6, S-2 and
Section 4).
"""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from fastapi.security import APIKeyCookie, HTTPBasic, OAuth2PasswordBearer
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

import redis_fastapi.sessions as sessions_module
from redis_fastapi import cache, valid_session
from redis_fastapi.config import get_settings
from redis_fastapi.deps import (
    SessionDep,
    SessionStateDep,
    SessionStoreDep,
    get_async_redis,
)
from redis_fastapi.exceptions import SessionConfigurationError
from redis_fastapi.session_backend import FIELD_DATA, RedisSessionStore, SessionState
from redis_fastapi.sessions import add_redis_sessions
from redis_fastapi.setup import FastAPIRedis


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    return RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)


def _reason_of(request: Request, reason: str) -> Response:
    """An ``on_reject`` that reports the reason, so tests can assert it."""
    return JSONResponse({"reason": reason}, status_code=401)


def _app(
    store: Any,
    *,
    gate: Any = None,
    before: list[Any] | None = None,
    **session_kwargs: Any,
) -> FastAPI:
    """An app with a gated route, and routes that create and change sessions."""
    app = FastAPI()
    add_redis_sessions(app, store=store, **session_kwargs)
    gate = gate if gate is not None else valid_session(on_reject=_reason_of)

    @app.post("/login")
    async def login(session: SessionDep) -> dict:
        session["user_id"] = 42
        return {}

    @app.post("/login/{uid}")
    async def login_as(uid: int, session: SessionDep) -> dict:
        session["user_id"] = uid
        return {}

    @app.post("/basket")
    async def basket(session: SessionDep) -> dict:
        session["basket"] = [1]
        return {}

    @app.post("/reauth")
    async def reauth(state: SessionStateDep, st: SessionStoreDep) -> dict:
        await st.reauthenticate(state)
        return {}

    @app.get("/public")
    async def public() -> dict:
        return {"ok": True}

    deps = [*(before or []), Depends(gate)]

    @app.get("/gated", dependencies=deps)
    async def gated(session: SessionDep) -> dict:
        return {"ok": True, "user_id": session.get("user_id")}

    return app


def _cookie(sid: str) -> dict[str, str]:
    return {"cookie": f"session={sid}"}


class _FailingStore(RedisSessionStore):
    """A store whose every read fails, as an unreachable server does."""

    async def _read(self, session_id: str, *, refresh_idle: int | None) -> Any:
        raise RedisConnectionError("Redis is down")


class _ProtocolOnlyStore:
    """A store that implements the protocol but not ``load_with_status``."""

    def __init__(self, inner: RedisSessionStore) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        if name == "load_with_status":
            raise AttributeError(name)
        return getattr(self._inner, name)


# ---------------------------------------------------------------------------
# S-1: the three reasons
# ---------------------------------------------------------------------------


class TestReasons:
    def test_no_cookie_is_missing(self, store) -> None:
        with TestClient(_app(store)) as client:
            response = client.get("/gated")
        assert response.status_code == 401
        assert response.json() == {"reason": "missing"}

    @pytest.mark.parametrize("value", ["x", "!" * 64, "abc;def"])
    def test_a_malformed_cookie_is_missing_and_counted(
        self, store, monkeypatch, value
    ) -> None:
        counted: list[tuple[str, str]] = []
        monkeypatch.setattr(
            sessions_module,
            "record_session_operation",
            lambda **kw: counted.append((kw["operation"], kw["result"])),
        )
        with TestClient(_app(store)) as client:
            response = client.get("/gated", headers=_cookie(value))
            client.get("/public", headers=_cookie(value))
        assert response.json() == {"reason": "missing"}
        assert counted == [("load", "malformed"), ("load", "malformed")], (
            "the malformed counter must increment on gated and ungated routes"
        )

    def test_a_well_formed_id_never_issued_is_expired(self, store) -> None:
        with TestClient(_app(store)) as client:
            response = client.get("/gated", headers=_cookie(store.new_id()))
        assert response.json() == {"reason": "expired"}

    @pytest.mark.parametrize("ending", ["idle", "absolute", "revoked"])
    def test_a_session_that_ended_is_expired(
        self, store, fake_async_redis, ending
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login")
            sid = client.cookies["session"]
            key = store.session_key(sid)
            if ending == "idle":
                asyncio.run(fake_async_redis.hdel(key, FIELD_DATA))
            elif ending == "absolute":
                asyncio.run(fake_async_redis.pexpire(key, 1))
                time.sleep(0.01)
            else:
                asyncio.run(store.delete(sid))
            response = client.get("/gated")
        assert response.json() == {"reason": "expired"}

    def test_a_live_session_passes_and_varies_by_cookie(self, store) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login")
            response = client.get("/gated")
        assert response.status_code == 200
        assert response.json() == {"ok": True, "user_id": 42}
        assert "Cookie" in response.headers["vary"]

    def test_an_anonymous_session_passes(self, store) -> None:
        """S-1 is not about identity."""
        with TestClient(_app(store)) as client:
            client.post("/basket")
            response = client.get("/gated")
        assert response.status_code == 200
        assert response.json()["user_id"] is None

    def test_a_failed_read_is_unavailable(self, fake_async_redis) -> None:
        failing = _FailingStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        with TestClient(_app(failing)) as client:
            response = client.get("/gated", headers=_cookie(failing.new_id()))
        assert response.json() == {"reason": "unavailable"}

    def test_a_protocol_only_store_reports_a_failed_read_as_expired(
        self, fake_async_redis
    ) -> None:
        """Without ``load_with_status`` a failure looks like an unknown ID.

        The wording is wrong during an outage, but the gate still rejects.
        """
        failing = _FailingStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        with TestClient(_app(_ProtocolOnlyStore(failing))) as client:
            response = client.get("/gated", headers=_cookie(failing.new_id()))
        assert response.json() == {"reason": "expired"}


# ---------------------------------------------------------------------------
# S-1: a session ended or created earlier in the same request
# ---------------------------------------------------------------------------


async def _writes(session: SessionDep) -> None:
    session["basket"] = [1]


async def _revokes(state: SessionStateDep, st: SessionStoreDep) -> None:
    await st.revoke(state)


async def _clears(session: SessionDep) -> None:
    session.clear()


async def _rotates(state: SessionStateDep, st: SessionStoreDep) -> None:
    await st.rotate(state)


class TestEarlierInTheRequest:
    def test_a_session_created_before_the_gate_does_not_count(self, store) -> None:
        """Valid means created in an *earlier* response."""
        with TestClient(_app(store, before=[Depends(_writes)])) as client:
            response = client.get("/gated")
        assert response.json() == {"reason": "missing"}

    @pytest.mark.parametrize("ender", [_revokes, _clears])
    def test_a_session_ended_before_the_gate_is_missing(self, store, ender) -> None:
        with TestClient(_app(store, before=[Depends(ender)])) as client:
            client.post("/login")
            response = client.get("/gated")
        assert response.json() == {"reason": "missing"}

    def test_a_rotation_before_the_gate_passes(self, store) -> None:
        with TestClient(_app(store, before=[Depends(_rotates)])) as client:
            client.post("/login")
            response = client.get("/gated")
        assert response.status_code == 200


# ---------------------------------------------------------------------------
# S-1: configuration mistakes fail loudly
# ---------------------------------------------------------------------------


class TestConfiguration:
    def test_a_route_excluded_by_skip_raises(self, store) -> None:
        app = _app(store, skip=lambda request: request.url.path == "/gated")
        with TestClient(app) as client:
            with pytest.raises(SessionConfigurationError, match="/gated"):
                client.get("/gated")

    def test_sessions_not_set_up_raises(self) -> None:
        app = FastAPI()

        @app.get("/gated", dependencies=[Depends(valid_session())])
        async def gated() -> dict:
            return {}

        with TestClient(app) as client:
            with pytest.raises(SessionConfigurationError):
                client.get("/gated")


# ---------------------------------------------------------------------------
# S-1: on_reject
# ---------------------------------------------------------------------------


class TestOnReject:
    def test_a_sync_callback_s_response_is_sent_intact(self, store) -> None:
        def reject(request: Request, reason: str) -> Response:
            return JSONResponse({"r": reason}, status_code=503, headers={"x-a": "1"})

        with TestClient(_app(store, gate=valid_session(on_reject=reject))) as client:
            response = client.get("/gated")
        assert response.status_code == 503
        assert response.json() == {"r": "missing"}
        assert response.headers["x-a"] == "1"

    def test_an_async_callback_is_awaited(self, store) -> None:
        async def reject(request: Request, reason: str) -> Response:
            return JSONResponse({"r": reason}, status_code=403)

        with TestClient(_app(store, gate=valid_session(on_reject=reject))) as client:
            response = client.get("/gated")
        assert response.status_code == 403

    def test_a_callback_that_raises_keeps_its_exception(self, store) -> None:
        def reject(request: Request, reason: str) -> Response:
            raise HTTPException(418, "teapot")

        with TestClient(_app(store, gate=valid_session(on_reject=reject))) as client:
            response = client.get("/gated")
        assert response.status_code == 418

    def test_a_callback_that_returns_nothing_fails_closed(self, store) -> None:
        """A missing ``return`` must not let the request through."""
        ran: list[bool] = []

        def reject(request: Request, reason: str) -> Response:
            return None  # type: ignore[return-value]

        app = _app(store, gate=valid_session(on_reject=reject))

        @app.get(
            "/gated-and-counted",
            dependencies=[Depends(valid_session(on_reject=reject))],
        )
        async def counted() -> dict:
            ran.append(True)
            return {}

        with TestClient(app, raise_server_exceptions=False) as client:
            response = client.get("/gated-and-counted")
        assert response.status_code == 500
        assert ran == []

    def test_the_default_rejection_is_a_401(self, store) -> None:
        with TestClient(_app(store, gate=valid_session())) as client:
            response = client.get("/gated")
        assert response.status_code == 401
        assert response.json() == {"detail": "No valid session"}


# ---------------------------------------------------------------------------
# S-1: the WWW-Authenticate challenge
# ---------------------------------------------------------------------------


class TestChallenge:
    def _challenge(self, store, challenge: Any, **gate_kwargs: Any) -> Response:
        app = _app(store, gate=valid_session(**gate_kwargs), challenge=challenge)
        with TestClient(app) as client:
            return client.get("/gated")

    def test_no_setting_sends_no_header(self, store) -> None:
        assert "www-authenticate" not in self._challenge(store, None).headers

    @pytest.mark.parametrize(
        ("scheme", "expected"),
        [
            (APIKeyCookie(name="session"), "APIKey"),
            (OAuth2PasswordBearer(tokenUrl="/token"), "Bearer"),
            (
                'Cookie realm="shop" form-action="/login"',
                'Cookie realm="shop" form-action="/login"',
            ),
        ],
    )
    def test_a_scheme_or_a_string_sets_the_header(
        self, store, scheme, expected
    ) -> None:
        response = self._challenge(store, scheme)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == expected

    def test_a_callable_receives_the_reason(self, store) -> None:
        seen: list[str] = []

        def challenge(request: Request, reason: str) -> str | None:
            seen.append(reason)
            return None if reason == "unavailable" else "APIKey"

        response = self._challenge(store, challenge)
        assert seen == ["missing"]
        assert response.headers["www-authenticate"] == "APIKey"

    def test_a_callable_returning_none_sends_no_header(self, store) -> None:
        response = self._challenge(store, lambda request, reason: None)
        assert "www-authenticate" not in response.headers

    def test_on_reject_owns_its_response(self, store) -> None:
        response = self._challenge(store, "APIKey", on_reject=_reason_of)
        assert "www-authenticate" not in response.headers

    @pytest.mark.parametrize("challenge", ["Basic", 'basic realm="x"', HTTPBasic()])
    def test_basic_is_refused_at_setup(self, store, challenge) -> None:
        with pytest.raises(SessionConfigurationError, match="Basic"):
            add_redis_sessions(FastAPI(), store=store, challenge=challenge)

    def test_a_callable_returning_basic_is_refused_at_request_time(self, store) -> None:
        app = _app(
            store, gate=valid_session(), challenge=lambda request, reason: "Basic"
        )
        with TestClient(app) as client:
            with pytest.raises(SessionConfigurationError, match="Basic"):
                client.get("/gated")

    def test_the_builder_passes_the_challenge_on(self, store) -> None:
        app = FastAPI()
        FastAPIRedis(app).sessions(store=store, challenge="APIKey")

        @app.get("/gated", dependencies=[Depends(valid_session())])
        async def gated() -> dict:
            return {}

        with TestClient(app) as client:
            assert client.get("/gated").headers["www-authenticate"] == "APIKey"


# ---------------------------------------------------------------------------
# S-1: cookies the gate refuses
# ---------------------------------------------------------------------------


class TestRefusedCookies:
    def test_a_write_after_an_unknown_id_issues_a_new_one(self, store) -> None:
        """The client's value is never adopted (S-1.7)."""
        forged = store.new_id()
        with TestClient(_app(store)) as client:
            response = client.post("/basket", headers=_cookie(forged))
        assert response.cookies["session"] != forged

    @pytest.mark.parametrize("value", ["not valid!", "a" * 43])
    def test_a_refused_cookie_is_not_cleared(self, store, value) -> None:
        """It may belong to another application on the same domain."""
        with TestClient(_app(store)) as client:
            response = client.get("/gated", headers=_cookie(value))
        assert "set-cookie" not in response.headers


# ---------------------------------------------------------------------------
# S-1.5 and Section 4: headers on gated responses
# ---------------------------------------------------------------------------


class TestHeaders:
    def test_a_gated_uncached_route_is_private(self, store) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login")
            response = client.get("/gated")
        assert response.headers["cache-control"] == "private"
        assert "Cookie" in response.headers["vary"]

    def test_a_rejection_is_private_too(self, store) -> None:
        with TestClient(_app(store)) as client:
            response = client.get("/gated")
        assert response.headers["cache-control"] == "private"

    def test_a_handler_s_no_store_gets_nothing_added(self, store) -> None:
        app = _app(store)

        @app.get("/sensitive")
        async def sensitive(session: SessionDep, response: Response) -> dict:
            response.headers["Cache-Control"] = "no-store"
            return {"user_id": session.get("user_id")}

        with TestClient(app) as client:
            client.post("/login")
            response = client.get("/sensitive")
        assert response.headers["cache-control"] == "no-store"
        assert "Cookie" in response.headers["vary"]

    def test_another_handler_directive_is_merged_with_private(self, store) -> None:
        app = _app(store)

        @app.get("/revalidate")
        async def revalidate(session: SessionDep, response: Response) -> dict:
            response.headers["Cache-Control"] = "max-age=0"
            return {"user_id": session.get("user_id")}

        with TestClient(app) as client:
            client.post("/login")
            response = client.get("/revalidate")
        assert response.headers["cache-control"] == "max-age=0, private"

    def test_always_save_writes_on_a_gated_request(self, store, monkeypatch) -> None:
        """S-1.5 accepts this cost; the test pins it so a change is deliberate."""
        monkeypatch.setenv("REDIS_SESSION_ALWAYS_SAVE", "true")
        get_settings.cache_clear()
        saved: list[str] = []
        original = store.save

        async def spy(session_id: str, record: Any) -> None:
            saved.append(session_id)
            await original(session_id, record)

        monkeypatch.setattr(store, "save", spy)
        with TestClient(_app(store)) as client:
            client.post("/login")
            client.get("/gated")
        assert saved, "the gated read of a non-empty session wrote nothing"


# ---------------------------------------------------------------------------
# S-1.6: the gate and cache()
# ---------------------------------------------------------------------------


def _cached_app(store: RedisSessionStore, fake_async_redis: Any) -> FastAPI:
    app = _app(store)
    FastAPIRedis(app).caching()

    async def _redis() -> Any:
        return fake_async_redis

    app.dependency_overrides[get_async_redis] = _redis

    async def require_user(session: SessionDep) -> None:
        if "user_id" not in session:
            raise HTTPException(401)

    @app.get(
        "/wrong-order",
        dependencies=[
            Depends(cache(ttl=300, vary_on_session=False)),
            Depends(valid_session()),
        ],
    )
    async def wrong_order() -> dict:
        return {"items": [1]}

    @app.get(
        "/right-order",
        dependencies=[
            Depends(valid_session()),
            Depends(cache(ttl=300, vary_on_session=False)),
        ],
    )
    async def right_order() -> dict:
        return {"items": [1]}

    @app.get(
        "/hand-written",
        dependencies=[
            Depends(require_user),
            Depends(cache(ttl=300, vary_on_session=False)),
        ],
    )
    async def hand_written() -> dict:
        return {"items": [1]}

    @app.get("/ungated", dependencies=[Depends(cache(ttl=300, vary_on_session=False))])
    async def ungated(session: SessionDep) -> dict:
        return {"basket": session.get("basket")}

    @app.get(
        "/undeclared",
        dependencies=[Depends(valid_session()), Depends(cache(ttl=300))],
    )
    async def undeclared() -> dict:
        return {"items": [1]}

    @app.get(
        "/recent-cached",
        dependencies=[
            Depends(valid_session(issued_within=600)),
            Depends(cache(ttl=300, vary_on_session=True)),
        ],
    )
    async def recent_cached(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    @app.get("/no-store", dependencies=[Depends(cache(ttl=300, no_store=True))])
    async def no_store() -> dict:
        return {"items": [1]}

    return app


async def _cache_keys(redis: Any) -> list[bytes]:
    return [key async for key in redis.scan_iter(match="*cache*")]


class TestTheGateAndCache:
    def test_the_wrong_order_raises_and_stores_nothing(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            with pytest.raises(SessionConfigurationError, match="must come before"):
                client.get("/wrong-order")
            with pytest.raises(SessionConfigurationError):
                client.get("/wrong-order")
        assert asyncio.run(_cache_keys(fake_async_redis)) == [], (
            "a misordered route stored a response a later hit could serve"
        )

    def test_the_right_order_is_a_private_miss_then_a_private_hit(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            first = client.get("/right-order")
            second = client.get("/right-order")
        assert first.headers["x-redis-cache"] == "MISS"
        assert second.headers["x-redis-cache"] == "HIT"
        assert first.headers["cache-control"] == "private, max-age=300"
        assert second.headers["cache-control"].startswith("private, max-age=")
        assert len(asyncio.run(_cache_keys(fake_async_redis))) == 1, (
            "Redis should hold one shared entry"
        )

    def test_a_rejected_caller_never_reaches_the_entry(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            client.get("/right-order")
            anonymous = client.get("/right-order", headers={"cookie": ""})
        assert anonymous.status_code == 401
        assert "x-redis-cache" not in anonymous.headers

    @pytest.mark.parametrize("path", ["/hand-written", "/ungated"])
    def test_other_gates_keep_their_shared_hits(
        self, store, fake_async_redis, path
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            client.get(path)
            second = client.get(path)
        assert second.headers["x-redis-cache"] == "HIT"
        assert "private" not in second.headers["cache-control"]

    def test_an_undeclared_gated_route_is_served_but_not_stored(
        self, store, fake_async_redis, caplog, monkeypatch
    ) -> None:
        # The warning names each route once per process; another test may
        # already have used this route name.
        import importlib

        cache_module = importlib.import_module("redis_fastapi.cache")
        monkeypatch.setattr(cache_module, "_WARNED_ROUTES", set())
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            with caplog.at_level("WARNING"):
                first = client.get("/undeclared")
                second = client.get("/undeclared")
        assert first.status_code == second.status_code == 200
        assert second.headers.get("x-redis-cache") != "HIT"
        assert "vary_on_session" in caplog.text

    def test_issued_within_makes_a_cached_route_no_store(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            client.post("/login")
            first = client.get("/recent-cached")
            second = client.get("/recent-cached")
        assert first.headers["x-redis-cache"] == "MISS"
        assert second.headers["x-redis-cache"] == "HIT"
        assert first.headers["cache-control"] == "no-store"
        assert second.headers["cache-control"] == "no-store"

    def test_cache_no_store_keeps_the_entry_but_says_no_store(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_cached_app(store, fake_async_redis)) as client:
            first = client.get("/no-store")
            second = client.get("/no-store")
        assert first.headers["x-redis-cache"] == "MISS"
        assert second.headers["x-redis-cache"] == "HIT"
        assert first.headers["cache-control"] == "no-store"
        assert second.headers["cache-control"] == "no-store"


# ---------------------------------------------------------------------------
# S-2: issued_within
# ---------------------------------------------------------------------------


def _recent(limit: int = 60) -> Any:
    return valid_session(issued_within=limit, on_reject=_reason_of)


async def _age_key(redis: Any, key: str, age: int, absolute: int = 600) -> None:
    """Make the session look *age* seconds old: that much gone from its TTL."""
    await redis.expire(key, absolute - age)


class TestIssuedWithin:
    def test_a_new_session_passes(self, store) -> None:
        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/basket")
            response = client.get("/gated")
        assert response.status_code == 200
        state = SessionState(
            data={}, session_id=client.cookies["session"], absolute_remaining=600
        )
        assert store.session_age(state) == 0

    def test_an_old_session_is_stale(self, store, fake_async_redis) -> None:
        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            asyncio.run(_age_key(fake_async_redis, key, age=120))
            response = client.get("/gated")
        assert response.json() == {"reason": "stale"}

    def test_reads_and_writes_do_not_reset_the_age(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            asyncio.run(_age_key(fake_async_redis, key, age=120))
            client.post("/basket")
            client.get("/gated")
            response = client.get("/gated")
        assert response.json() == {"reason": "stale"}

    @pytest.mark.parametrize("renewal", ["/reauth", "/login/7"])
    def test_a_new_id_makes_the_session_recent(
        self, store, fake_async_redis, renewal
    ) -> None:
        """``reauthenticate()`` and a principal change both issue a new ID."""
        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/login")
            old = client.cookies["session"]
            asyncio.run(_age_key(fake_async_redis, store.session_key(old), age=120))
            client.post(renewal)
            response = client.get("/gated")
        assert client.cookies["session"] != old
        assert response.status_code == 200

    def test_a_handler_rotation_makes_the_session_recent(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(
            _app(store, gate=_recent(), before=[Depends(_rotates)])
        ) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            asyncio.run(_age_key(fake_async_redis, key, age=120))
            response = client.get("/gated")
        assert response.status_code == 200

    def test_a_session_from_before_a_shorter_lifetime_is_stale(
        self, fake_async_redis
    ) -> None:
        """A negative age means the configuration changed: fail closed."""
        long_lived = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        with TestClient(_app(long_lived)) as client:
            client.post("/login")
            sid = client.cookies["session"]
        short_lived = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=300)
        with TestClient(_app(short_lived, gate=_recent(600))) as client:
            response = client.get("/gated", headers=_cookie(sid))
        assert response.json() == {"reason": "stale"}
        state = SessionState(data={}, session_id=sid, absolute_remaining=600)
        assert short_lived.session_age(state) is None

    def test_a_request_without_a_stored_session_has_no_age(self, store) -> None:
        assert store.session_age(SessionState(data={})) is None
        assert store.session_age(SessionState(data={}, session_id="x" * 30)) is None

    def test_the_application_clock_does_not_matter(self, store, monkeypatch) -> None:
        """Moving the library's own clock a century changes nothing.

        Only the library's clock is moved: ``fakeredis`` reads the global one
        to expire keys, and it stands in for the server here.
        """
        import types

        import redis_fastapi.session_backend as backend

        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/login")
            monkeypatch.setattr(
                backend, "time", types.SimpleNamespace(time=lambda: 4e9)
            )
            response = client.get("/gated")
        assert response.status_code == 200

    def test_no_session_gives_s_1_s_reason_first(self, store) -> None:
        with TestClient(_app(store, gate=_recent())) as client:
            response = client.get("/gated")
        assert response.json() == {"reason": "missing"}

    def test_the_default_rejection_says_what_is_needed(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_app(store, gate=valid_session(issued_within=60))) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            asyncio.run(_age_key(fake_async_redis, key, age=120))
            response = client.get("/gated")
        assert response.status_code == 401
        assert response.json() == {
            "detail": "A recently issued session is required",
            "error": "stale_session",
            "issued_within": 60,
        }
        assert response.headers["cache-control"] == "no-store"

    def test_a_passing_response_is_no_store(self, store) -> None:
        with TestClient(_app(store, gate=_recent())) as client:
            client.post("/login")
            response = client.get("/gated")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "no-store"

    def test_without_issued_within_there_is_no_stale_and_no_no_store(
        self, store, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            asyncio.run(_age_key(fake_async_redis, key, age=590))
            response = client.get("/gated")
        assert response.status_code == 200
        assert response.headers["cache-control"] == "private"

    def test_a_timedelta_is_accepted(self, store) -> None:
        from datetime import timedelta

        with TestClient(
            _app(store, gate=valid_session(issued_within=timedelta(minutes=1)))
        ) as client:
            client.post("/login")
            assert client.get("/gated").status_code == 200


class TestTypes:
    """The two overloads promise exact reason types (S-1, step 3)."""

    def test_the_overloads_narrow_the_reason_type(self, tmp_path) -> None:
        api = pytest.importorskip("mypy.api")
        source = tmp_path / "check.py"
        source.write_text(
            "from starlette.requests import Request\n"
            "from starlette.responses import Response\n"
            "from redis_fastapi import RecencyRejection, SessionRejection, valid_session\n"
            "def narrow(request: Request, reason: SessionRejection) -> Response:\n"
            "    raise NotImplementedError\n"
            "def wide(request: Request, reason: RecencyRejection) -> Response:\n"
            "    raise NotImplementedError\n"
            "valid_session(on_reject=narrow)\n"
            "valid_session(on_reject=wide)\n"
            "valid_session(issued_within=60, on_reject=wide)\n"
            "valid_session(issued_within=60, on_reject=narrow)  # E: stale unhandled\n"
        )
        stdout, _, status = api.run([str(source), "--no-incremental"])
        errors = [line for line in stdout.splitlines() if ": error:" in line]
        assert status == 1, stdout
        assert len(errors) == 1, stdout
        assert "check.py:11:" in errors[0], stdout
