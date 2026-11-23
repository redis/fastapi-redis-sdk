"""Tests for the two clocks of a session, and for what a load may write.

Redis enforces both clocks.  The idle timeout is the TTL of the key, and
the absolute timeout is the TTL of the field ``s:created``.  No test here
waits for a TTL: it reads the TTL the store set, and it forces the expiry
with ``aexpire_now``.

Design section 3; tests 8 to 11a, 13, 13a and 45.
"""

from __future__ import annotations

import itertools
import time

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, get_session_store
from redis_fastapi.exceptions import SessionStoreError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
)
from redis_fastapi.session_layout import FIELD_CREATED, FIELD_SEEN
from redis_fastapi.sessions import add_redis_sessions
from tests.conftest import about, aexpire_now
from tests.unit.session_stubs import names, record_commands

IDLE = 60
ABSOLUTE = 600

# Each kind of server.  ``None`` is a capability check with no answer yet.
SERVERS = {
    "search+hsetex": (True, True),
    "search-only": (True, False),
    "hsetex-only": (False, True),
    "neither": (False, False),
}
RECORDS_ACCESS = {"search+hsetex"}


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def _store(redis, server: str = "search+hsetex", **kwargs) -> RedisSessionStore:
    search, hsetex = SERVERS[server]
    caps = _StoreCapabilities(supports_search=search, supports_hsetex=hsetex)
    return RedisSessionStore(
        redis, capabilities=caps, idle_ttl=IDLE, absolute_ttl=ABSOLUTE, **kwargs
    )


@pytest.fixture(params=list(SERVERS))
def server(request) -> str:
    return request.param


@pytest.fixture()
def store(fake_async_redis, server: str) -> RedisSessionStore:
    """The store, once for each kind of server."""
    return _store(fake_async_redis, server)


def _app(store: RedisSessionStore) -> FastAPI:
    get_settings.cache_clear()
    app = FastAPI()
    add_redis_sessions(app)

    async def _factory(request: Request) -> RedisSessionStore:
        return store

    app.dependency_overrides[get_session_store] = _factory

    @app.post("/login/{uid}")
    async def login(uid: int, session: SessionDep) -> dict:
        session["user_id"] = uid
        return {}

    @app.post("/write")
    async def write(session: SessionDep) -> dict:
        session["counter"] = session.get("counter", 0) + 1
        return {"counter": session["counter"]}

    @app.get("/me")
    async def me(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    @app.get("/untouched")
    async def untouched() -> dict:
        return {}

    return app


async def _httl(redis, key: str, field: str) -> int:
    reply = await redis.execute_command("HTTL", key, "FIELDS", 1, field)
    return int(reply[0])


async def _age(redis, key: str, left: int) -> None:
    """Leave *left* seconds on the absolute clock."""
    await redis.execute_command("HEXPIRE", key, left, "FIELDS", 1, FIELD_CREATED)


class TestTheIdleTimeout:
    """Test 8.  The TTL of the key."""

    async def test_a_read_only_request_restarts_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            assert about(await fake_async_redis.ttl(key), IDLE)
            await fake_async_redis.expire(key, 5)

            assert client.get("/me").json() == {"user_id": 42}

            assert about(await fake_async_redis.ttl(key), IDLE)

    async def test_a_save_restarts_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.expire(key, 5)

            client.post("/write")

            assert about(await fake_async_redis.ttl(key), IDLE)

    async def test_after_it_ends_a_load_returns_no_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            sid = client.cookies["session"]
            await aexpire_now(fake_async_redis, store.session_key(sid))

            assert client.get("/me").json() == {"user_id": None}

        assert await store.load(sid) is None
        assert await fake_async_redis.keys("*") == []


class TestTheAbsoluteTimeout:
    """Tests 9 and 10.  The TTL of ``s:created``."""

    async def test_a_create_sets_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
        assert about(await _httl(fake_async_redis, key, FIELD_CREATED), ABSOLUTE)

    async def test_requests_and_saves_do_not_raise_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await _age(fake_async_redis, key, 100)

            for _ in range(3):
                client.get("/me")
                client.get("/untouched")
                client.post("/write")

            assert about(await _httl(fake_async_redis, key, FIELD_CREATED), 100), (
                "a request moved, removed or dropped the absolute deadline"
            )
            assert client.get("/me").json() == {"user_id": 42}

    async def test_after_many_saves_the_deadline_still_has_a_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 10.  ``HSET`` removes the TTL of a field, so one write of
        ``s:created`` would make the session immortal."""
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            created = await fake_async_redis.hget(key, FIELD_CREATED)
            await _age(fake_async_redis, key, 100)

            for expected in range(1, 21):
                assert client.post("/write").json() == {"counter": expected}

            assert about(await _httl(fake_async_redis, key, FIELD_CREATED), 100)
            assert await fake_async_redis.hget(key, FIELD_CREATED) == created

    async def test_the_cookie_carries_the_time_left(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``HTTL`` on ``s:created`` sizes ``Max-Age``, so the cookie does
        not outlive the session."""
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await _age(fake_async_redis, key, 100)

            header = client.post("/write").headers["set-cookie"]

        max_age = int(header.lower().split("max-age=")[1].split(";")[0])
        assert about(max_age, 100)

    async def test_after_it_ends_a_load_returns_no_session_and_deletes_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            sid = client.cookies["session"]
            key = store.session_key(sid)
            await aexpire_now(fake_async_redis, key, FIELD_CREATED)
            assert await fake_async_redis.exists(key) == 1, (
                "the idle clock has not ended: the key is still there"
            )

            assert client.get("/me").json() == {"user_id": None}

        assert await fake_async_redis.exists(key) == 0
        assert await store.load(sid) is None

    async def test_a_save_after_it_ended_does_not_create_a_live_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 10.  A request can load a live session and save it after the
        deadline.  If that save set a deadline, an active session would
        never end."""
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        loaded = await store.load(sid)
        assert loaded is not None
        await aexpire_now(fake_async_redis, key, FIELD_CREATED)

        await store.save(
            sid,
            store.new_record({"user_id": 42, "n": 1}, created=1.0),
            baseline=loaded.fields,
        )

        assert await _httl(fake_async_redis, key, FIELD_CREATED) == -2, (
            "the save wrote s:created"
        )
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_a_save_after_the_key_is_gone_leaves_a_hash_that_expires(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The save recreates the hash with no ``s:created``.  The key TTL
        still ends it, and the next load deletes it."""
        sid = store.new_id()
        key = store.session_key(sid)
        fields = await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.delete(key)

        await store.save(sid, store.new_record({"user_id": 43}), baseline=fields)

        assert await fake_async_redis.hexists(key, FIELD_CREATED) == 0
        assert about(await fake_async_redis.ttl(key), IDLE), (
            "a hash with no TTL would stay for ever"
        )
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0


class TestRotation:
    """Tests 11 and 11a."""

    async def test_a_rotation_starts_a_new_absolute_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """A rotation follows a sign-in or a change of privilege, so the
        deadline runs from that moment."""
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            old = client.cookies["session"]
            old_key = store.session_key(old)
            await _age(fake_async_redis, old_key, 100)
            await fake_async_redis.hset(old_key, FIELD_SEEN, "1.0")
            old_created = float(await fake_async_redis.hget(old_key, FIELD_CREATED))

            before = time.time()
            client.post("/login/7")
            new = client.cookies["session"]

        assert new != old
        new_key = store.session_key(new)
        assert await fake_async_redis.exists(old_key) == 0
        assert about(await _httl(fake_async_redis, new_key, FIELD_CREATED), ABSOLUTE)
        new_created = float(await fake_async_redis.hget(new_key, FIELD_CREATED))
        assert new_created >= before > old_created
        assert about(await fake_async_redis.ttl(new_key), IDLE)
        assert await fake_async_redis.keys("*") == [new_key.encode()]

    async def test_the_rotated_session_reports_the_full_time_left(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"user_id": 42}))
        await _age(fake_async_redis, store.session_key(sid), 100)
        state = SessionState(
            data={"user_id": 42},
            session_id=sid,
            absolute_remaining=100,
            fields=fields,
        )

        new_id = await store.rotate(state, subject="42")

        assert state.session_id == new_id
        assert state.absolute_remaining == ABSOLUTE
        assert state.subject == "42"
        loaded = await store.load(new_id)
        assert loaded is not None
        assert about(loaded.absolute_remaining, ABSOLUTE)
        assert loaded.fields == state.fields
        assert loaded.fields["s:sub"] == '"42"'

    async def test_the_old_key_goes_before_the_new_one_is_written(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The order is the security control.  Two session IDs must never
        both work."""
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"user_id": 42}))
        state = SessionState(data={"user_id": 42}, session_id=sid, fields=fields)
        sent = record_commands(fake_async_redis)

        await store.rotate(state)

        order = names(sent)
        assert order.index("DEL") < order.index("HSET")
        assert sent[order.index("DEL")] == ("DEL", store.session_key(sid))

    async def test_a_rotation_that_fails_after_the_delete_leaves_no_session(
        self, fake_async_redis, server: str
    ) -> None:
        """Test 11a.  Signed out is recoverable.  Two live IDs are not."""
        ids = itertools.chain(["a" * 43, "b" * 43], itertools.repeat("c" * 43))

        class _CreateFails(RedisSessionStore):
            fail = False

            async def _create(self, *args, **kwargs) -> None:
                if self.fail:
                    raise RedisConnectionError("the connection dropped")
                await super()._create(*args, **kwargs)

        search, hsetex = SERVERS[server]
        store = _CreateFails(
            fake_async_redis,
            capabilities=_StoreCapabilities(
                supports_search=search, supports_hsetex=hsetex
            ),
            idle_ttl=IDLE,
            absolute_ttl=ABSOLUTE,
            id_factory=lambda: next(ids),
        )
        old = store.new_id()
        fields = await store.create(old, store.new_record({"user_id": 42}))
        state = SessionState(data={"user_id": 42}, session_id=old, fields=fields)
        store.fail = True

        with pytest.raises(SessionStoreError, match="Could not save session"):
            await store.rotate(state, subject="42")

        assert await fake_async_redis.keys("*") == []
        assert await store.load(old) is None
        assert await store.load("b" * 43) is None

    async def test_a_failed_delete_stops_the_rotation(self, fake_async_redis) -> None:
        """With the old key still there, the store writes no new one."""

        class _RetireFails(RedisSessionStore):
            async def _retire(self, session_id, fields):
                raise RedisConnectionError("the connection dropped")

        store = _RetireFails(fake_async_redis, idle_ttl=IDLE, absolute_ttl=ABSOLUTE)
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"user_id": 42}))
        state = SessionState(data={"user_id": 42}, session_id=sid, fields=fields)

        with pytest.raises(SessionStoreError, match="Could not delete session"):
            await store.rotate(state)

        assert await fake_async_redis.keys("*") == [store.session_key(sid).encode()]
        assert state.session_id == sid

    async def test_a_rotation_of_a_session_never_written_creates_one(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        state = SessionState(data={"user_id": 42})
        new_id = await store.rotate(state, subject="42")
        assert await fake_async_redis.keys("*") == [store.session_key(new_id).encode()]
        loaded = await store.load(new_id)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}


class TestACookieThatNamesNoSession:
    """Test 13.  A scanner sends many such cookies.  None may leave a key."""

    async def test_a_request_leaves_no_key_and_writes_nothing(
        self, store: RedisSessionStore, fake_async_redis, server: str
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        sent = record_commands(fake_async_redis)

        with TestClient(_app(store)) as client:
            response = client.get("/me", headers={"cookie": f"session={sid}"})
        sent = list(sent)

        assert response.json() == {"user_id": None}
        assert await fake_async_redis.keys("*") == []
        expected = [("HGETALL", key), ("HTTL", key, "FIELDS", "1", FIELD_CREATED)]
        if server in RECORDS_ACCESS:
            # Writes only to a field that exists, so it cannot create a key.
            assert sent[2][:6] == ("HSETEX", key, "FXX", "FIELDS", "1", FIELD_SEEN)
            assert len(sent[2]) == 7
            del sent[2]
        expected.append(("EXPIRE", key, str(IDLE)))
        assert sent == expected

    async def test_no_request_sends_a_command_that_can_create_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Each path of a request that changes nothing: a read, no use of
        the session, and a read with ``session_refresh_on_load=False``."""
        sid = store.new_id()
        sent = record_commands(fake_async_redis)
        cookie = {"cookie": f"session={sid}"}

        with TestClient(_app(store)) as client:
            client.get("/me", headers=cookie)
            client.get("/untouched", headers=cookie)
        sent = list(sent)

        assert await fake_async_redis.keys("*") == []
        for command in sent:
            assert command[0] in {"HGETALL", "HTTL", "EXPIRE", "HSETEX"}, command
            if command[0] == "HSETEX":
                assert command[2] == "FXX", "a plain HSETEX creates the key"

    async def test_with_refresh_on_load_off(
        self, store: RedisSessionStore, fake_async_redis, monkeypatch
    ) -> None:
        monkeypatch.setenv("REDIS_SESSION_REFRESH_ON_LOAD", "false")
        sid = store.new_id()
        key = store.session_key(sid)
        sent = record_commands(fake_async_redis)

        with TestClient(_app(store)) as client:
            response = client.get("/me", headers={"cookie": f"session={sid}"})
        sent = list(sent)

        assert response.json() == {"user_id": None}
        assert await fake_async_redis.keys("*") == []
        assert sent == [("HGETALL", key), ("HTTL", key, "FIELDS", "1", FIELD_CREATED)]

    async def test_while_the_capability_check_has_no_answer(
        self, fake_async_redis, monkeypatch
    ) -> None:
        """The load does not write ``s:seen`` on a guess."""
        import redis_fastapi.session_backend as backend

        async def no_answer(client):
            return None

        monkeypatch.setattr(backend, "probe_search_support", no_answer)
        monkeypatch.setattr(backend, "probe_hsetex_support", no_answer)
        store = RedisSessionStore(
            fake_async_redis, idle_ttl=IDLE, absolute_ttl=ABSOLUTE
        )
        sid = store.new_id()
        sent = record_commands(fake_async_redis)

        assert await store.load(sid) is None
        await store.touch(sid)
        sent = list(sent)

        assert await fake_async_redis.keys("*") == []
        assert names(sent) == ["HGETALL", "HTTL", "EXPIRE", "EXPIRE"]

    async def test_the_store_asks_for_hsetex_once_and_keeps_the_answer(
        self, fake_async_redis, monkeypatch
    ) -> None:
        """One capabilities object serves each store of a pool."""
        import redis_fastapi.session_backend as backend

        asked: list[str] = []

        async def yes(client):
            asked.append("hsetex")
            return True

        monkeypatch.setattr(backend, "probe_hsetex_support", yes)
        caps = _StoreCapabilities(supports_search=True)
        sid = "a" * 43
        sent = record_commands(fake_async_redis)
        for _ in range(3):
            store = RedisSessionStore(fake_async_redis, capabilities=caps)
            assert await store.load(sid) is None
        sent = list(sent)

        assert asked == ["hsetex"]
        assert caps.supports_hsetex is True
        assert names(sent).count("HSETEX") == 3
        assert await fake_async_redis.keys("*") == []

    async def test_a_dead_hash_is_deleted_and_not_written(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """A hash with no ``s:created``: the load must not give it a last
        access time and a new idle clock, and then leave it."""
        sid = store.new_id()
        key = store.session_key(sid)
        await fake_async_redis.hset(key, "a:user_id", "42")

        with TestClient(_app(store)) as client:
            response = client.get("/me", headers={"cookie": f"session={sid}"})

        assert response.json() == {"user_id": None}
        assert await fake_async_redis.keys("*") == []


class TestLastAccess:
    """Test 13a.  Only a search reads ``s:seen``, so a load writes it only
    where a search can run, and only with ``HSETEX ... FXX``."""

    async def test_a_read_only_request_writes_it_only_where_a_search_can_run(
        self, store: RedisSessionStore, fake_async_redis, server: str
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.hset(key, FIELD_SEEN, "1.0")
            before = time.time()

            assert client.get("/me").json() == {"user_id": 42}

            seen = float(await fake_async_redis.hget(key, FIELD_SEEN))
        if server in RECORDS_ACCESS:
            assert before <= seen <= time.time()
        else:
            assert seen == 1.0, "a load wrote s:seen on a server with no use for it"

    async def test_a_save_writes_it_on_each_server(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The field is then present if the server gains the Query Engine."""
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.hset(key, FIELD_SEEN, "1.0")
            before = time.time()

            client.post("/write")

            seen = float(await fake_async_redis.hget(key, FIELD_SEEN))
        assert before <= seen <= time.time()

    async def test_a_create_writes_it_on_each_server(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        before = time.time()
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
        seen = float(await fake_async_redis.hget(key, FIELD_SEEN))
        assert before <= seen <= time.time()

    async def test_the_write_on_load_leaves_the_clocks_alone(
        self, fake_async_redis
    ) -> None:
        """``HSETEX`` sets a TTL when it is asked to.  The store asks for
        none: ``s:seen`` has no TTL, and ``s:created`` keeps its own."""
        store = _store(fake_async_redis, "search+hsetex")
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await _age(fake_async_redis, key, 100)

        loaded = await store.load(sid)

        assert loaded is not None
        assert about(loaded.absolute_remaining, 100)
        assert await _httl(fake_async_redis, key, FIELD_SEEN) == -1
        assert about(await _httl(fake_async_redis, key, FIELD_CREATED), 100)

    async def test_the_load_returns_the_fields_as_they_were_before_its_write(
        self, fake_async_redis
    ) -> None:
        """The next save compares the session with these.  It writes
        ``s:seen`` again in any case, so a stale value costs nothing."""
        store = _store(fake_async_redis, "search+hsetex")
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.hset(store.session_key(sid), FIELD_SEEN, "1.0")

        loaded = await store.load(sid)

        assert loaded is not None
        assert loaded.record.metadata.last_access == 1.0


class TestRefreshOnLoadOff:
    """Test 45.  ``session_refresh_on_load=False``: a load is a plain read,
    and the response step restarts the idle clock."""

    @pytest.fixture(autouse=True)
    def _off(self, monkeypatch, _plain_http):
        monkeypatch.setenv("REDIS_SESSION_REFRESH_ON_LOAD", "false")
        get_settings.cache_clear()

    async def test_a_load_does_not_change_the_ttl_of_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.expire(key, 5)
            await fake_async_redis.hset(key, FIELD_SEEN, "1.0")
            sent = record_commands(fake_async_redis)

            client.get("/untouched")
            sent = list(sent)

            assert about(await fake_async_redis.ttl(key), 5), (
                "a request that never used the session counted as activity"
            )
            assert await fake_async_redis.hget(key, FIELD_SEEN) == b"1.0"
        assert sent == [("HGETALL", key), ("HTTL", key, "FIELDS", "1", FIELD_CREATED)]

    async def test_a_request_that_used_the_session_restarts_the_idle_clock(
        self, store: RedisSessionStore, fake_async_redis, server: str
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.expire(key, 5)
            await fake_async_redis.hset(key, FIELD_SEEN, "1.0")
            await _age(fake_async_redis, key, 100)
            sent = record_commands(fake_async_redis)
            before = time.time()

            assert client.get("/me").json() == {"user_id": 42}
            sent = list(sent)

            assert about(await fake_async_redis.ttl(key), IDLE), (
                "the idle clock never advanced"
            )
            assert about(await _httl(fake_async_redis, key, FIELD_CREATED), 100)
            seen = float(await fake_async_redis.hget(key, FIELD_SEEN))

        assert names(sent)[:2] == ["HGETALL", "HTTL"]
        if server in RECORDS_ACCESS:
            assert names(sent)[2:] == ["HSETEX", "EXPIRE"]
            assert before <= seen <= time.time()
        else:
            assert names(sent)[2:] == ["EXPIRE"]
            assert seen == 1.0

    async def test_a_save_restarts_the_idle_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login/42")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.expire(key, 5)

            client.post("/write")

            assert about(await fake_async_redis.ttl(key), IDLE)
