"""Tests for the ``searchable`` declaration: what setup refuses, and what a
save refuses.

The Query Engine removes a whole session from an index when one indexed
field holds a value it cannot read, and the write that caused it succeeds.
So the store checks each declared attribute before it writes, and it writes
nothing when a check fails.

Design sections 4, 5.1 and 5.2; tests 32, 33 and 33b.
"""

from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, get_session_store
from redis_fastapi.exceptions import (
    SessionAttributeTypeError,
    SessionConfigurationError,
    SessionError,
    SessionStoreError,
)
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
)
from redis_fastapi.sessions import add_redis_sessions
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import about

SEARCHABLE = {
    "device": "tag",
    "cart.total": "numeric",
    "name": "text",
    "location": "geo",
}
GOOD = {
    "device": "d-91",
    "cart": {"total": 5, "items": [1]},
    "name": "Ana",
    "location": [23.32, 42.69],
    "other": 1,
}

# One attribute of GOOD replaced by a value of another type.
WRONG_TYPES = {
    "numeric-string": {"cart": {"total": "5"}},
    "numeric-bool": {"cart": {"total": True}},
    "numeric-none": {"cart": {"total": None}},
    "tag-int": {"device": 7},
    "tag-float": {"device": 7.5},
    "tag-none": {"device": None},
    "text-int": {"name": 7},
    "text-list": {"name": ["Ana"]},
    "text-none": {"name": None},
    "geo-one-number": {"location": [23.32]},
    "geo-three-numbers": {"location": [23.32, 42.69, 5]},
    "geo-strings": {"location": ["23.32", "42.69"]},
    "geo-string": {"location": "23.32,42.69"},
    "geo-none": {"location": None},
}
RIGHT_TYPES = {
    "tag-bool": {"device": True},
    "numeric-float": {"cart": {"total": 5.5}},
    "numeric-zero": {"cart": {"total": 0}},
    "geo-integers": {"location": [23, 42]},
    "empty-text": {"name": ""},
}


class Cipher:
    def encrypt(self, data: bytes) -> bytes:
        return data[::-1]

    def decrypt(self, data: bytes) -> bytes:
        return data[::-1]


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    return RedisSessionStore(
        fake_async_redis,
        capabilities=_StoreCapabilities(supports_search=False, supports_hsetex=True),
        idle_ttl=60,
        absolute_ttl=600,
        searchable=SEARCHABLE,
    )


async def _snapshot(redis) -> dict[bytes, tuple[dict, bool, list[bool]]]:
    """Each key, with its fields, and which of them have a TTL.

    Not the number of seconds: a TTL read twice can differ by one.
    """
    state = {}
    for key in sorted(await redis.keys("*")):
        fields = await redis.hgetall(key)
        ttls = await redis.execute_command("HTTL", key, "FIELDS", len(fields), *fields)
        state[key] = (fields, await redis.ttl(key) > 0, [ttl > 0 for ttl in ttls])
    return state


class TestSetup:
    """Test 33, and the type names."""

    def test_the_store_refuses_searchable_with_an_encryptor(
        self, fake_async_redis
    ) -> None:
        """The engine cannot search an encrypted value."""
        with pytest.raises(SessionConfigurationError, match="encryptor"):
            RedisSessionStore(
                fake_async_redis, searchable={"device": "tag"}, encryptor=Cipher()
            )

    def test_add_redis_sessions_refuses_it_at_setup(self) -> None:
        """At setup, not at the first request: the store is built for each
        request, and a declaration that cannot work must stop the start."""
        app = FastAPI()
        with pytest.raises(SessionConfigurationError, match="encryptor"):
            add_redis_sessions(app, searchable={"device": "tag"}, encryptor=Cipher())
        assert app.user_middleware == []

    def test_the_builder_refuses_it_at_setup(self) -> None:
        app = FastAPI()
        with pytest.raises(SessionConfigurationError, match="encryptor"):
            FastAPIRedis(app).sessions(searchable={"device": "tag"}, encryptor=Cipher())

    def test_an_encryptor_with_no_declaration_is_accepted(
        self, fake_async_redis
    ) -> None:
        """The built-in searches work: the system fields are plaintext."""
        RedisSessionStore(fake_async_redis, encryptor=Cipher())
        RedisSessionStore(fake_async_redis, encryptor=Cipher(), searchable={})
        add_redis_sessions(FastAPI(), encryptor=Cipher(), searchable={})

    @pytest.mark.parametrize(
        "declaration",
        [{"device": "vector"}, {"device": "string"}, {"device": None}, {"": "tag"}],
    )
    def test_an_unknown_type_is_refused_at_setup(
        self, fake_async_redis, declaration
    ) -> None:
        with pytest.raises(SessionConfigurationError, match="must be one of"):
            RedisSessionStore(fake_async_redis, searchable=declaration)
        with pytest.raises(SessionConfigurationError, match="must be one of"):
            add_redis_sessions(FastAPI(), searchable=declaration)

    def test_a_type_in_another_case_is_accepted(self, fake_async_redis) -> None:
        lower = RedisSessionStore(fake_async_redis, searchable={"device": "tag"})
        upper = RedisSessionStore(fake_async_redis, searchable={"device": "TAG"})
        assert upper._custom_index == lower._custom_index

    def test_the_declaration_reaches_the_store_of_each_request(self) -> None:
        """Through ``add_redis_sessions`` and through the builder."""
        for register in (
            lambda app: add_redis_sessions(app, searchable=SEARCHABLE),
            lambda app: FastAPIRedis(app).sessions(searchable=SEARCHABLE),
        ):
            app = FastAPI()
            register(app)
            options = app.state._redis_session_options
            assert options.kwargs["searchable"] == SEARCHABLE

    def test_no_declaration_means_no_custom_index(self, fake_async_redis) -> None:
        assert RedisSessionStore(fake_async_redis)._custom_index is None
        assert RedisSessionStore(fake_async_redis, searchable={})._custom_index is None

    def test_the_new_errors_are_exported(self) -> None:
        import redis_fastapi

        for name in (
            "SessionAttributeTypeError",
            "SessionSearchError",
            "SessionSearchUnavailableError",
            "SessionSearchTimeoutError",
        ):
            assert name in redis_fastapi.__all__
            assert issubclass(getattr(redis_fastapi, name), SessionError)

    def test_a_type_error_is_not_a_store_error(self) -> None:
        """A handler that retries on ``SessionStoreError`` must not retry a
        value that can never be written."""
        assert issubclass(SessionAttributeTypeError, TypeError)
        assert not issubclass(SessionAttributeTypeError, SessionStoreError)


class TestASaveWithAWrongType:
    """Test 32, through the store."""

    @pytest.mark.parametrize("case", list(WRONG_TYPES))
    async def test_a_save_raises_and_the_stored_session_does_not_change(
        self, store: RedisSessionStore, fake_async_redis, case: str
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record(GOOD), subject="42")
        before = await _snapshot(fake_async_redis)

        with pytest.raises(SessionAttributeTypeError, match="is declared as"):
            await store.save(
                sid,
                store.new_record({**GOOD, **WRONG_TYPES[case], "other": 2}),
                baseline=fields,
                subject="42",
            )

        assert await _snapshot(fake_async_redis) == before
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == GOOD

    @pytest.mark.parametrize("case", list(WRONG_TYPES))
    async def test_a_create_raises_and_writes_nothing(
        self, store: RedisSessionStore, fake_async_redis, case: str
    ) -> None:
        with pytest.raises(SessionAttributeTypeError):
            await store.create(
                store.new_id(), store.new_record({**GOOD, **WRONG_TYPES[case]})
            )
        assert await fake_async_redis.keys("*") == []

    @pytest.mark.parametrize("case", list(RIGHT_TYPES))
    async def test_a_fitting_value_is_saved(
        self, store: RedisSessionStore, case: str
    ) -> None:
        sid = store.new_id()
        data = {**GOOD, **RIGHT_TYPES[case]}
        await store.create(sid, store.new_record(data))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == data

    async def test_an_absent_attribute_raises_nothing(
        self, store: RedisSessionStore
    ) -> None:
        """A declared attribute can be absent from a session."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"other": 1}))
        await store.save(sid, store.new_record({"cart": {}}), baseline=fields)
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"cart": {}}

    async def test_a_store_with_no_declaration_checks_nothing(
        self, fake_async_redis
    ) -> None:
        store = RedisSessionStore(fake_async_redis)
        sid = store.new_id()
        await store.create(sid, store.new_record({"device": 7, "name": None}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"device": 7, "name": None}

    async def test_a_refused_rotation_does_not_sign_the_user_out(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The check runs before the old key goes."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record(GOOD), subject="42")
        before = await _snapshot(fake_async_redis)
        state = SessionState(
            data={**GOOD, "device": 7}, session_id=sid, subject="42", fields=fields
        )

        with pytest.raises(SessionAttributeTypeError):
            await store.rotate(state)

        assert await _snapshot(fake_async_redis) == before
        assert state.session_id == sid

    async def test_the_geo_attribute_is_stored_in_the_form_the_engine_reads(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record(GOOD))
        stored = await fake_async_redis.hgetall(store.session_key(sid))
        assert stored[b"a:location"] == b"23.32,42.69"
        assert stored[b"a:cart.total"] == b"5"
        assert stored[b"a:device"] == b'"d-91"'
        assert stored[b"a:name"] == b'"Ana"'
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == GOOD


class TestATtlOnASearchableAttribute:
    """Test 33b.  When such a field expired, the session would lack an
    indexed field in normal operation."""

    @pytest.mark.parametrize(
        ("name", "value"),
        [
            pytest.param("device", "d-92", id="the-attribute"),
            pytest.param("cart", {"total": 6}, id="holds-a-searchable-path"),
            pytest.param("location", [1.5, 2.5], id="geo"),
        ],
    )
    async def test_a_save_raises_and_the_stored_session_does_not_change(
        self, store: RedisSessionStore, fake_async_redis, name: str, value: Any
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record(GOOD))
        before = await _snapshot(fake_async_redis)

        with pytest.raises(SessionConfigurationError, match="cannot have a TTL"):
            await store.save(
                sid,
                store.new_record({**GOOD, name: value, "other": 2}),
                baseline=fields,
                ttls={name: 60},
            )

        assert await _snapshot(fake_async_redis) == before

    async def test_a_create_raises_and_writes_nothing(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with pytest.raises(SessionConfigurationError, match="cannot have a TTL"):
            await store.create(
                store.new_id(), store.new_record(GOOD), ttls={"device": 60}
            )
        assert await fake_async_redis.keys("*") == []

    async def test_a_rotation_raises_and_keeps_the_old_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record(GOOD))
        before = await _snapshot(fake_async_redis)
        state = SessionState(data=dict(GOOD), session_id=sid, fields=fields)

        with pytest.raises(SessionConfigurationError, match="cannot have a TTL"):
            await store.rotate(state, ttls={"cart": 60})

        assert await _snapshot(fake_async_redis) == before

    async def test_another_attribute_can_have_a_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``cart_total`` is not ``cart.total``."""
        sid = store.new_id()
        await store.create(
            sid,
            store.new_record({**GOOD, "cart_total": 5, "flash": "x"}),
            ttls={"cart_total": 60, "flash": 30, "other": 20},
        )
        reply = await fake_async_redis.execute_command(
            "HTTL",
            store.session_key(sid),
            "FIELDS",
            5,
            "a:cart_total",
            "a:flash",
            "a:other",
            "a:cart.total",
            "a:device",
        )
        assert about(reply[0], 60)
        assert about(reply[1], 30)
        assert about(reply[2], 20)
        assert reply[3:] == [-1, -1]

    async def test_persist_of_a_searchable_attribute_is_allowed(
        self, store: RedisSessionStore
    ) -> None:
        """It removes a TTL.  It cannot make the field expire."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record(GOOD))
        await store.save(
            sid, store.new_record(GOOD), baseline=fields, ttls={"device": None}
        )

    async def test_the_error_reaches_the_application_from_a_request(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``Session`` has no access to the declaration, so the call is not
        refused.  The save is, and the stored session does not change."""
        app = FastAPI()
        add_redis_sessions(app)

        async def _factory(request: Request) -> RedisSessionStore:
            return store

        app.dependency_overrides[get_session_store] = _factory

        @app.post("/good")
        async def good(session: SessionDep) -> dict:
            session.update(GOOD)
            return {}

        @app.post("/ttl")
        async def ttl(session: SessionDep) -> dict:
            session.set("device", "d-92", ttl=60)
            return {}

        @app.post("/type")
        async def wrong_type(session: SessionDep) -> dict:
            session["device"] = 7
            return {}

        @app.get("/read")
        async def read(session: SessionDep) -> dict:
            return dict(session)

        with TestClient(app) as client:
            client.post("/good")
            before = await _snapshot(fake_async_redis)

            with pytest.raises(SessionConfigurationError, match="cannot have a TTL"):
                client.post("/ttl")
            with pytest.raises(SessionAttributeTypeError, match="declared as tag"):
                client.post("/type")

            assert await _snapshot(fake_async_redis) == before
            assert client.get("/read").json() == GOOD

    async def test_a_refused_first_write_leaves_no_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """A refused create must not leave an identifier that names nothing."""
        app = FastAPI()
        add_redis_sessions(app)

        async def _factory(request: Request) -> RedisSessionStore:
            return store

        app.dependency_overrides[get_session_store] = _factory

        @app.post("/type")
        async def wrong_type(session: SessionDep) -> dict:
            session["device"] = 7
            return {}

        with TestClient(app) as client:
            with pytest.raises(SessionAttributeTypeError):
                client.post("/type")
            assert "session" not in client.cookies
        assert await fake_async_redis.keys("*") == []
