"""Tests for the session setup surface: the builder, DI, and the sync facade."""

from __future__ import annotations

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import (
    SessionDep,
    SessionStoreDep,
    SyncSessionStoreDep,
    get_session_store,
    get_sync_session_store,
)
from redis_fastapi.session_backend import RedisSessionStore, SyncSessionStore
from redis_fastapi.sessions import (
    SessionConfigurationError,
    SessionMiddleware,
    add_redis_sessions,
)
from redis_fastapi.setup import FastAPIRedis


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestTheBuilder:
    def test_sessions_registers_the_middleware(self) -> None:
        app = FastAPI()
        FastAPIRedis(app).sessions()
        assert any(m.cls is SessionMiddleware for m in app.user_middleware)

    def test_calling_it_twice_is_a_no_op(self) -> None:
        """Two middlewares would load and save the session twice per request."""
        app = FastAPI()
        FastAPIRedis(app).sessions().sessions()
        count = sum(1 for m in app.user_middleware if m.cls is SessionMiddleware)
        assert count == 1

    def test_it_chains(self) -> None:
        app = FastAPI()
        assert isinstance(FastAPIRedis(app).sessions(), FastAPIRedis)

    def test_principal_keys_reach_the_middleware(self) -> None:
        app = FastAPI()
        FastAPIRedis(app).sessions(principal_keys=["user_id", "role"])
        middleware = next(m for m in app.user_middleware if m.cls is SessionMiddleware)
        assert middleware.kwargs["principal_of"] is not None


class TestConfigurationIsChecked:
    def test_samesite_none_without_secure_is_refused(self, monkeypatch) -> None:
        """Browsers reject such a cookie outright.

        Left unchecked the session would simply never be stored, which
        presents as "login does nothing" with no error anywhere.
        """
        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_SAME_SITE", "none")
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        get_settings.cache_clear()
        with pytest.raises(SessionConfigurationError, match="Secure"):
            add_redis_sessions(FastAPI())


class TestSessionOf:
    def test_a_missing_middleware_says_so(self) -> None:
        """Otherwise this surfaces as a KeyError deep inside a handler."""
        app = FastAPI()

        @app.get("/x")
        async def x(session: SessionDep) -> dict:
            return {}

        with (
            TestClient(app) as client,
            pytest.raises(SessionConfigurationError, match="No session"),
        ):
            client.get("/x")


class TestDependencies:
    async def test_get_session_store_reuses_the_pool_capability_cache(
        self, fake_async_redis
    ) -> None:
        """Probing once per process, not once per request."""
        from redis_fastapi.deps import _get_pool_state

        app = FastAPI()
        state = _get_pool_state(app)
        state.async_pool = fake_async_redis.connection_pool

        class _Req:
            def __init__(self, application: FastAPI) -> None:
                self.app = application

        first = await get_session_store(_Req(app))  # type: ignore[arg-type]
        second = await get_session_store(_Req(app))  # type: ignore[arg-type]
        assert isinstance(first, RedisSessionStore)
        assert first._caps is second._caps

    async def test_get_sync_session_store_wraps_the_async_one(
        self, fake_async_redis
    ) -> None:
        from redis_fastapi.deps import _get_pool_state

        app = FastAPI()
        _get_pool_state(app).async_pool = fake_async_redis.connection_pool

        class _Req:
            def __init__(self, application: FastAPI) -> None:
                self.app = application

        store = await get_sync_session_store(_Req(app))  # type: ignore[arg-type]
        assert isinstance(store, SyncSessionStore)


class TestAnnotationsResolveUnderPep563:
    """A regression test for a defect this file's own suite exposed.

    FastAPI resolves an endpoint's annotations with ``get_type_hints``, which
    evaluates the forward reference inside ``Annotated[...]`` against
    ``deps.py``'s namespace.  While the session types were imported there only
    under ``TYPE_CHECKING``, that raised ``NameError`` and FastAPI silently
    demoted the parameter to a query parameter - so any endpoint in a module
    using ``from __future__ import annotations`` answered 422 instead of
    receiving its session.  Nothing type-checks this; only a running endpoint
    catches it.
    """

    @pytest.mark.parametrize(
        "alias", [SessionDep, SessionStoreDep, SyncSessionStoreDep]
    )
    def test_the_inner_type_is_a_class_not_a_string(self, alias) -> None:
        import typing

        inner = typing.get_args(alias)[0]
        assert isinstance(inner, type), (
            f"{inner!r} is still a forward reference; FastAPI will not be able "
            "to resolve it and will treat the parameter as a query parameter"
        )


class TestSyncEndpoint:
    def test_a_def_endpoint_can_use_the_store(self, fake_async_redis) -> None:
        """The anyio bridge only works from a FastAPI worker thread.

        Driving it through a real ``def`` endpoint is the only way to test it
        honestly - calling it directly from the main thread raises.
        """
        app = FastAPI()
        add_redis_sessions(app)
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)

        async def _async_store(request: Request) -> RedisSessionStore:
            return store

        async def _sync_store(request: Request) -> SyncSessionStore:
            return SyncSessionStore(store)

        for mw in app.user_middleware:
            if "store_factory" in mw.kwargs:
                mw.kwargs["store_factory"] = _async_store
        app.dependency_overrides[get_sync_session_store] = _sync_store

        @app.post("/login")
        def login(session: SessionDep) -> dict:
            session["user_id"] = 42
            return {"ok": True}

        @app.get("/count")
        def count(store: SyncSessionStoreDep) -> dict:
            return {"n": store.count_for_subject("42")}

        @app.get("/devices")
        def devices(session: SessionDep, store: SyncSessionStoreDep) -> dict:
            return {
                "n": len(store.list_for_subject("42")),
                "current": store.session_id(session) is not None,
            }

        @app.post("/rotate")
        def rotate(session: SessionDep, store: SyncSessionStoreDep) -> dict:
            return {"sid": store.rotate(session, subject="42")}

        @app.post("/step-up")
        def step_up(session: SessionDep, store: SyncSessionStoreDep) -> dict:
            store.reauthenticate(session)
            return {"ok": True}

        @app.post("/revoke-one")
        def revoke_one(sid: str, store: SyncSessionStoreDep) -> dict:
            return {"ended": store.revoke_id(sid, subject="42")}

        @app.post("/revoke-all")
        def revoke_all(store: SyncSessionStoreDep) -> dict:
            return {"n": store.revoke_all("42")}

        @app.post("/logout")
        def logout(session: SessionDep, store: SyncSessionStoreDep) -> dict:
            store.revoke(session)
            return {"ok": True}

        with TestClient(app) as client:
            assert client.post("/login").json() == {"ok": True}
            assert client.get("/count").json() == {"n": 1}
            assert client.get("/devices").json() == {"n": 1, "current": True}

            # An explicit rotate must not be followed by a second rotation
            # from the middleware, or the ID returned here is already stale.
            rotated = client.post("/rotate").json()["sid"]
            assert client.post("/revoke-one", params={"sid": rotated}).json() == {
                "ended": True
            }, "store.rotate() returned an ID that was no longer current"

            client.post("/login")
            assert client.post("/step-up").json() == {"ok": True}
            assert client.post("/logout").json() == {"ok": True}

            client.post("/login")
            assert client.post("/revoke-all").json()["n"] >= 1
