"""End-to-end tests for the session middleware, against ``fakeredis``.

These drive a real FastAPI app through ``TestClient``, so they exercise the
eager load, the response rule, the cookie, and - most importantly - rotation
happening with **no call from the application**.
"""

from __future__ import annotations

from http.cookies import SimpleCookie

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, SessionStoreDep, get_session_store
from redis_fastapi.session_backend import RedisSessionStore
from redis_fastapi.sessions import add_redis_sessions


def _set_cookies(response) -> dict[str, SimpleCookie]:
    """Parse every ``Set-Cookie`` on a response, keyed by cookie name."""
    out: dict[str, SimpleCookie] = {}
    for raw in response.headers.get_list("set-cookie"):
        jar: SimpleCookie = SimpleCookie()
        jar.load(raw)
        for name in jar:
            out[name] = jar
    return out


@pytest.fixture()
def app(fake_async_redis, monkeypatch) -> FastAPI:
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", "60")
    monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", "600")
    get_settings.cache_clear()

    application = FastAPI()
    add_redis_sessions(application)

    store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)

    async def _store(request: Request) -> RedisSessionStore:
        return store

    application.dependency_overrides[get_session_store] = _store
    # The middleware resolves the store directly, not through DI, so point it
    # at the same fake instance.
    for mw in application.user_middleware:
        if "store_factory" in mw.kwargs:
            mw.kwargs["store_factory"] = _store

    @application.get("/read")
    async def read(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    @application.get("/untouched")
    async def untouched() -> dict:
        return {"ok": True}

    @application.post("/login")
    async def login(session: SessionDep) -> dict:
        session["user_id"] = 42
        return {"ok": True}

    @application.post("/login-fails")
    async def login_fails(session: SessionDep, response: Response) -> dict:
        session["user_id"] = 42
        response.status_code = 401
        return {"ok": False}

    @application.post("/write")
    async def write(session: SessionDep) -> dict:
        session["counter"] = session.get("counter", 0) + 1
        return {"counter": session["counter"]}

    @application.post("/logout")
    async def logout(session: SessionDep, store: SessionStoreDep) -> dict:
        await store.revoke(session)
        return {"ok": True}

    @application.post("/step-up")
    async def step_up(session: SessionDep, store: SessionStoreDep) -> dict:
        return {"sid": await store.rotate(session, subject="42")}

    @application.post("/promote")
    async def promote(session: SessionDep) -> dict:
        session["user_id"] = 99
        return {"ok": True}

    application.state._store = store
    return application


@pytest.fixture()
def client(app: FastAPI) -> TestClient:
    return TestClient(app)


class TestZeroCostPaths:
    def test_no_cookie_and_no_access_writes_nothing(self, client: TestClient) -> None:
        response = client.get("/untouched")
        assert response.status_code == 200
        assert "set-cookie" not in response.headers
        assert "vary" not in response.headers

    def test_reading_an_empty_session_sets_no_cookie(self, client: TestClient) -> None:
        """Reading changes nothing, so there is nothing to persist."""
        response = client.get("/read")
        assert response.json() == {"user_id": None}
        assert "set-cookie" not in response.headers


class TestVaryCookie:
    def test_vary_is_emitted_when_the_session_was_accessed(
        self, client: TestClient
    ) -> None:
        """Without it a shared cache can serve one user's page to another."""
        assert client.get("/read").headers["vary"] == "Cookie"

    def test_vary_is_absent_when_it_was_not(self, client: TestClient) -> None:
        assert "vary" not in client.get("/untouched").headers


class TestWriteAndRoundTrip:
    def test_a_write_sets_a_cookie_and_survives_the_next_request(
        self, client: TestClient
    ) -> None:
        first = client.post("/write")
        assert first.json() == {"counter": 1}
        assert "session" in _set_cookies(first)

        second = client.post("/write")
        assert second.json() == {"counter": 2}, "the session did not round-trip"

    def test_the_cookie_carries_no_session_data(self, client: TestClient) -> None:
        response = client.post("/login")
        value = _set_cookies(response)["session"]["session"].value
        assert "42" not in value
        assert "user_id" not in value

    def test_cookie_attributes_follow_the_settings(self, client: TestClient) -> None:
        morsel = _set_cookies(client.post("/write"))["session"]["session"]
        assert morsel["httponly"] is True
        assert morsel["samesite"] == "Lax"
        assert morsel["path"] == "/"
        # min(idle, absolute remaining) - the idle clock is the shorter one.
        assert int(morsel["max-age"]) == 60


class TestAutomaticRotation:
    """F-2: the application never calls rotation, and the middleware does."""

    def test_signing_in_rotates_with_no_call_from_the_handler(
        self, client: TestClient
    ) -> None:
        anonymous = client.post("/write")
        before = _set_cookies(anonymous)["session"]["session"].value

        after = _set_cookies(client.post("/login"))["session"]["session"].value
        assert after != before, (
            "the session ID did not change on sign-in - this is the session "
            "fixation defence and it is not optional"
        )

    async def test_the_old_key_is_gone_after_a_rotation(
        self, client: TestClient, app: FastAPI, fake_async_redis
    ) -> None:
        """The old key goes **first**, so an interrupted rotation signs out."""
        store = app.state._store
        before = _set_cookies(client.post("/write"))["session"]["session"].value
        client.post("/login")
        assert await fake_async_redis.exists(store.session_key(before)) == 0

    def test_a_privilege_change_rotates_too(self, client: TestClient) -> None:
        first = _set_cookies(client.post("/login"))["session"]["session"].value
        second = _set_cookies(client.post("/promote"))["session"]["session"].value
        assert second != first

    def test_an_ordinary_write_does_not_rotate(self, client: TestClient) -> None:
        """Rotating when we need not is harmless; doing it constantly is not.

        The failure asymmetry runs the right way, but a design that rotated on
        every write would churn the keyspace and break "this device" listings,
        so the stable case is asserted too.
        """
        client.post("/login")
        first = _set_cookies(client.post("/write"))["session"]["session"].value
        second = _set_cookies(client.post("/write"))["session"]["session"].value
        assert first == second

    def test_a_failed_sign_in_persists_nothing(
        self, client: TestClient, app: FastAPI, fake_async_redis
    ) -> None:
        """Section 4.3's one exception.

        Persisting the data while skipping the rotation would store the new
        identity against the old, unrotated ID - the exact fixation this
        design prevents, arrived at by being helpful.
        """
        response = client.post("/login-fails")
        assert response.status_code == 401
        assert "set-cookie" not in response.headers

        # And nothing was written, so a later read finds no user.
        assert client.get("/read").json() == {"user_id": None}


class TestCookieValidation:
    def test_a_malformed_cookie_yields_a_new_session(self, client: TestClient) -> None:
        client.cookies.set("session", "not a valid id")
        assert client.get("/read").json() == {"user_id": None}

    def test_an_injection_attempt_never_reaches_a_header(
        self, client: TestClient
    ) -> None:
        client.cookies.set("session", "abc\r\nSet-Cookie: evil=1")
        response = client.get("/read")
        assert response.status_code == 200
        assert "evil" not in str(response.headers)


class TestSkip:
    def test_a_skipped_request_costs_no_redis_call(
        self, fake_async_redis, monkeypatch
    ) -> None:
        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        get_settings.cache_clear()

        calls: list[str] = []
        store = RedisSessionStore(fake_async_redis)

        async def _store(request: Request) -> RedisSessionStore:
            calls.append("built")
            return store

        application = FastAPI()
        add_redis_sessions(application, skip=lambda request: True)
        for mw in application.user_middleware:
            if "store_factory" in mw.kwargs:
                mw.kwargs["store_factory"] = _store

        @application.get("/x")
        async def x(session: SessionDep) -> dict:
            return {"empty": len(session) == 0}

        with TestClient(application) as c:
            assert c.get("/x").json() == {"empty": True}
        assert calls == [], "a skipped request must not even build the store"


class TestRevokeClearsTheCookie:
    """The header that actually signs a user out of their browser.

    ``store.revoke()`` removing the key is only half of it: OWASP requires the
    session be invalidated on **both** sides, and the client half is this
    ``Set-Cookie``.  It was previously untested.
    """

    def test_logout_emits_a_clearing_cookie(self, client: TestClient) -> None:
        client.post("/login")
        response = client.post("/logout")
        morsel = _set_cookies(response)["session"]["session"]
        assert morsel.value == ""
        assert morsel["max-age"] == "0"

    def test_the_clearing_cookie_repeats_the_scoping_attributes(
        self, client: TestClient
    ) -> None:
        """A browser keeps the original unless every scoping attribute matches."""
        client.post("/login")
        morsel = _set_cookies(client.post("/logout"))["session"]["session"]
        assert morsel["path"] == "/"
        assert morsel["samesite"] == "Lax"
        assert morsel["httponly"] is True

    def test_the_session_is_gone_afterwards(self, client: TestClient) -> None:
        client.post("/login")
        client.post("/logout")
        assert client.get("/read").json() == {"user_id": None}

    def test_revoking_an_untouched_session_is_safe(self, client: TestClient) -> None:
        response = client.post("/logout")
        assert response.status_code == 200


class TestCookieOnlyMode:
    """Both clocks disabled: the browser decides, so there is no ``Max-Age``."""

    def test_no_max_age_is_emitted(self, fake_async_redis, monkeypatch) -> None:
        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", "0")
        monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", "0")
        get_settings.cache_clear()

        application = FastAPI()
        add_redis_sessions(application)
        store = RedisSessionStore(fake_async_redis, idle_ttl=0, absolute_ttl=0)

        async def _store(request: Request) -> RedisSessionStore:
            return store

        for mw in application.user_middleware:
            if "store_factory" in mw.kwargs:
                mw.kwargs["store_factory"] = _store

        @application.post("/w")
        async def w(session: SessionDep) -> dict:
            session["n"] = 1
            return {}

        with TestClient(application) as c:
            morsel = _set_cookies(c.post("/w"))["session"]["session"]
        assert morsel["max-age"] == "", "cookie-only mode must not pin a lifetime"
        get_settings.cache_clear()


class TestSecureAndDomainAttributes:
    def test_secure_and_domain_are_emitted_when_configured(
        self, fake_async_redis, monkeypatch
    ) -> None:
        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "true")
        monkeypatch.setenv("REDIS_SESSION_COOKIE_DOMAIN", "example.com")
        get_settings.cache_clear()

        application = FastAPI()
        add_redis_sessions(application)
        store = RedisSessionStore(fake_async_redis)

        async def _store(request: Request) -> RedisSessionStore:
            return store

        for mw in application.user_middleware:
            if "store_factory" in mw.kwargs:
                mw.kwargs["store_factory"] = _store

        @application.post("/w")
        async def w(session: SessionDep) -> dict:
            session["n"] = 1
            return {}

        with TestClient(application, base_url="https://example.com") as c:
            raw = c.post("/w").headers["set-cookie"]
        assert "Secure" in raw
        assert "Domain=example.com" in raw
        assert "HttpOnly" in raw
        get_settings.cache_clear()


class TestAnExplicitRotateIsNotRepeated:
    """``store.rotate()`` does the work; the middleware must not redo it.

    ``rotate()`` used to leave the pending-rotation flag set, so the
    middleware rotated a second time at response start.  That deleted the key
    ``rotate()`` had just written and made the ID it returned to the caller a
    stale value - a handler that returned it, or revoked it, was operating on
    a session that no longer existed.
    """

    def test_the_returned_id_is_the_one_in_the_cookie(self, client: TestClient) -> None:
        client.post("/login")
        response = client.post("/step-up")
        returned = response.json()["sid"]
        in_cookie = _set_cookies(response)["session"]["session"].value
        assert returned == in_cookie

    async def test_the_returned_id_names_a_live_session(
        self, client: TestClient, app: FastAPI, fake_async_redis
    ) -> None:
        store = app.state._store
        client.post("/login")
        returned = client.post("/step-up").json()["sid"]
        assert await fake_async_redis.exists(store.session_key(returned)) == 1

    def test_the_session_survives_an_explicit_rotation(
        self, client: TestClient
    ) -> None:
        client.post("/login")
        client.post("/step-up")
        assert client.get("/read").json() == {"user_id": 42}
