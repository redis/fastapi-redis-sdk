"""Tests for the TTL of one session attribute.

``request.session.set(name, value, ttl=N)`` gives the attribute a TTL, and
``persist(name)`` removes it.  The TTL is on each hash field of the
attribute, and Redis enforces it.  No test here waits for a TTL.

Each test runs with and without an ``Encryptor`` (test 7b).

Design sections 3 and 5.2; tests 12 to 12g, 44 and 33c.
"""

from __future__ import annotations

import itertools
from collections.abc import Callable
from typing import Any

import pytest
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, get_session_store
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
)
from redis_fastapi.sessions import Session, add_redis_sessions
from tests.conftest import about, aexpire_now
from tests.unit.session_stubs import names, record_commands

NO_TTL = -1
NO_FIELD = -2
FLASH = {"text": "Saved", "tags": ["a", "b"]}
FLASH_FIELDS = ["a:flash.text", "a:flash.tags[0]", "a:flash.tags[1]"]


class Invert:
    """A cipher that starts with a nonce, so a second encryption of one
    value gives another ciphertext."""

    def __init__(self) -> None:
        self._nonce = itertools.count()

    def encrypt(self, data: bytes) -> bytes:
        return bytes([next(self._nonce) % 256]) + bytes(b ^ 0xFF for b in data)

    def decrypt(self, data: bytes) -> bytes:
        return bytes(b ^ 0xFF for b in data[1:])


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


@pytest.fixture(params=[None, Invert], ids=["plain", "encrypted"])
def store(request, fake_async_redis) -> RedisSessionStore:
    return RedisSessionStore(
        fake_async_redis,
        capabilities=_StoreCapabilities(supports_search=False, supports_hsetex=True),
        encryptor=request.param() if request.param else None,
        idle_ttl=60,
        absolute_ttl=600,
    )


class _Client(TestClient):
    """A client whose requests each run one function on the session."""

    def run(self, change: Callable[[Session], Any]) -> None:
        self.app.state.change = change  # type: ignore[attr-defined]
        assert self.post("/run").status_code == 200

    def session(self) -> dict:
        return self.get("/read").json()

    @property
    def sid(self) -> str:
        return self.cookies["session"]


def _client(store: RedisSessionStore) -> _Client:
    get_settings.cache_clear()
    app = FastAPI()
    add_redis_sessions(app)

    async def _factory(request: Request) -> RedisSessionStore:
        return store

    app.dependency_overrides[get_session_store] = _factory

    @app.post("/run")
    async def run(request: Request, session: SessionDep) -> dict:
        request.app.state.change(session)
        return {}

    @app.get("/read")
    async def read(session: SessionDep) -> dict:
        return dict(session)

    return _Client(app)


async def _httl(redis, key: str, *fields: str) -> list[int]:
    reply = await redis.execute_command("HTTL", key, "FIELDS", len(fields), *fields)
    return [int(item) for item in reply]


async def _age(redis, key: str, left: int, *fields: str) -> None:
    """Leave *left* seconds on the TTL of each of *fields*."""
    await redis.execute_command("HEXPIRE", key, left, "FIELDS", len(fields), *fields)


def _each_is(ttls: list[int], expected: int) -> bool:
    return all(about(ttl, expected) for ttl in ttls)


def _carried(ttl: int, expected: int) -> bool:
    """Whether *ttl* is *expected* after a rotation carried it over.

    The store reads the time left in whole seconds and sets it again, and
    the test reads it once more, so up to two seconds can go.
    """
    return expected - 2 <= ttl <= expected


def _with_flash(ttl: int = 300) -> Callable[[Session], None]:
    def change(session: Session) -> None:
        session["user"] = "ana"
        session.set("flash", {"text": "Saved", "tags": ["a", "b"]}, ttl=ttl)

    return change


class TestAWriteWithATtl:
    """Test 12."""

    async def test_each_field_of_the_attribute_gets_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)

            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 300)
            assert await _httl(fake_async_redis, key, "a:user", "s:seen") == [
                NO_TTL,
                NO_TTL,
            ], "the TTL is on the attribute, not on the session"
            assert client.session() == {"user": "ana", "flash": FLASH}

    async def test_it_works_on_a_session_that_exists(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The first test sets the TTL in a create.  This one, in a save."""
        with _client(store) as client:
            client.run(lambda s: s.__setitem__("user", "ana"))
            key = store.session_key(client.sid)

            client.run(lambda s: s.set("flash", {"text": "Saved"}, ttl=45))

            assert _each_is(await _httl(fake_async_redis, key, "a:flash.text"), 45)
            assert await _httl(fake_async_redis, key, "a:user") == [NO_TTL]

    async def test_requests_and_saves_of_other_attributes_do_not_raise_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The TTL does not slide."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await _age(fake_async_redis, key, 200, *FLASH_FIELDS)

            for n in range(3):
                assert client.session()["flash"] == FLASH
                client.run(lambda s, n=n: s.__setitem__("counter", n))
                client.run(lambda s: s.__setitem__("user", "ana"))

            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 200)

    async def test_after_the_ttl_the_attribute_is_absent_and_the_rest_loads(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)

            await aexpire_now(fake_async_redis, key, *FLASH_FIELDS)

            assert client.session() == {"user": "ana"}
            client.run(lambda s: s.__setitem__("counter", 1))
            assert client.session() == {"user": "ana", "counter": 1}

    async def test_an_attribute_that_expired_can_be_written_again(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The save compares with the load, where the attribute was absent,
        so it writes each field."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await aexpire_now(fake_async_redis, key, *FLASH_FIELDS)

            client.run(lambda s: s.__setitem__("flash", dict(FLASH)))

            assert client.session()["flash"] == FLASH
            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_TTL] * 3

    async def test_two_attributes_with_two_ttls(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def change(session: Session) -> None:
            session.set("flash", "Saved", ttl=30)
            session.set("otp", {"code": "1234", "tries": 0}, ttl=120)
            session.set("hint", "x", ttl=30)

        with _client(store) as client:
            client.run(change)
            key = store.session_key(client.sid)

            ttls = await _httl(
                fake_async_redis, key, "a:flash", "a:hint", "a:otp.code", "a:otp.tries"
            )
            assert _each_is(ttls[:2], 30)
            assert _each_is(ttls[2:], 120)

    async def test_a_name_with_a_path_character(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The TTL is for the attribute ``a.b``, not for ``a``."""

        def change(session: Session) -> None:
            session["a"] = {"b": 1}
            session.set("a.b", {"c": 2}, ttl=30)

        with _client(store) as client:
            client.run(change)
            key = store.session_key(client.sid)

            assert _each_is(await _httl(fake_async_redis, key, "a:a%2Eb.c"), 30)
            assert await _httl(fake_async_redis, key, "a:a.b") == [NO_TTL]


class TestALaterWrite:
    async def test_a_new_value_with_no_ttl_removes_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12a.  ``HSET`` removes the TTL, and the store does not carry
        it over."""
        with _client(store) as client:
            client.run(lambda s: s.set("flash", "Settings saved.", ttl=300))
            key = store.session_key(client.sid)

            client.run(lambda s: s.set("flash", "Profile saved."))

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]
            assert client.session() == {"flash": "Profile saved."}

    async def test_a_new_value_through_the_dict_removes_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with _client(store) as client:
            client.run(lambda s: s.set("flash", "Settings saved.", ttl=300))
            key = store.session_key(client.sid)

            client.run(lambda s: s.__setitem__("flash", "Profile saved."))

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]

    async def test_a_new_write_with_a_ttl_sets_that_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12b."""
        with _client(store) as client:
            client.run(lambda s: s.set("flash", "Settings saved.", ttl=300))
            key = store.session_key(client.sid)

            client.run(lambda s: s.set("flash", "Profile saved.", ttl=40))

            assert _each_is(await _httl(fake_async_redis, key, "a:flash"), 40)
            assert client.session() == {"flash": "Profile saved."}

    async def test_an_equal_value_with_a_ttl_sets_that_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """A write with a TTL sets it on each field, changed or not.  This is
        how the application extends an expiry."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            sent = record_commands(fake_async_redis)

            client.run(lambda s: s.set("flash", dict(FLASH), ttl=40))
            sent = list(sent)

            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 40)
        hset = next(command for command in sent if command[0] == "HSET")
        assert hset[2::2] == ("s:seen",), "an equal value was written again"

    async def test_one_changed_leaf_with_a_ttl_sets_it_on_each_field(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 44."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await _age(fake_async_redis, key, 200, *FLASH_FIELDS)

            client.run(
                lambda s: s.set(
                    "flash", {"text": "Changed", "tags": ["a", "b"]}, ttl=50
                )
            )

            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 50)
            assert client.session()["flash"] == {"text": "Changed", "tags": ["a", "b"]}

    async def test_one_changed_leaf_with_no_ttl_loses_its_ttl_only(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12c.  The store does not keep the fields of one attribute in
        step: the application is responsible for that."""

        def change(session: Session) -> None:
            session["flash"]["text"] = "Changed"
            session.mark_modified()

        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await _age(fake_async_redis, key, 200, *FLASH_FIELDS)

            client.run(change)

            text, first, second = await _httl(fake_async_redis, key, *FLASH_FIELDS)
            assert text == NO_TTL
            assert _each_is([first, second], 200)
            assert client.session()["flash"] == {"text": "Changed", "tags": ["a", "b"]}

    async def test_an_equal_value_with_no_ttl_keeps_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12f.  The save sees no difference and sends no command for
        the attribute."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await _age(fake_async_redis, key, 200, *FLASH_FIELDS)
            sent = record_commands(fake_async_redis)

            client.run(lambda s: s.__setitem__("flash", dict(FLASH)))
            client.run(lambda s: s.set("flash", dict(FLASH)))
            sent = list(sent)

            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 200)
        for command in sent:
            assert not any("a:flash" in part for part in command[2:]), command

    async def test_a_removed_attribute_written_again_has_no_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)

            client.run(lambda s: s.pop("flash"))
            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_FIELD] * 3
            client.run(lambda s: s.__setitem__("flash", dict(FLASH)))

            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_TTL] * 3
            assert client.session() == {"user": "ana", "flash": FLASH}


# In one request, after ``set("flash", "old", ttl=300)``: each other change
# of the name.  The save must then carry no TTL for it.
_CHANGES = {
    "setitem": lambda s: s.__setitem__("flash", "new"),
    "set-no-ttl": lambda s: s.set("flash", "new"),
    "pop-then-write": lambda s: (s.pop("flash"), s.__setitem__("flash", "new")),
    "del-then-setdefault": lambda s: (
        s.__delitem__("flash"),
        s.setdefault("flash", "new"),
    ),
    "update": lambda s: s.update({"flash": "new"}),
    "update-keyword": lambda s: s.update(flash="new"),
    "ior": lambda s: s.__ior__({"flash": "new"}),
    "clear-then-write": lambda s: (s.clear(), s.__setitem__("flash", "new")),
}


class TestTheLastCallForANameDecides:
    """Test 33c, from the request to the stored TTL."""

    @pytest.mark.parametrize("change", list(_CHANGES))
    async def test_a_later_change_gives_a_save_with_no_ttl(
        self, store: RedisSessionStore, fake_async_redis, change: str
    ) -> None:
        def handler(session: Session) -> None:
            session.set("flash", "old", ttl=300)
            _CHANGES[change](session)

        with _client(store) as client:
            client.run(handler)
            key = store.session_key(client.sid)

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]
            assert client.session() == {"flash": "new"}

    @pytest.mark.parametrize("change", list(_CHANGES))
    async def test_a_later_change_of_a_stored_attribute_removes_its_ttl(
        self, store: RedisSessionStore, fake_async_redis, change: str
    ) -> None:
        """The same, when the attribute has a TTL in Redis already."""
        with _client(store) as client:
            client.run(lambda s: s.set("flash", "old", ttl=300))
            key = store.session_key(client.sid)

            def handler(session: Session) -> None:
                session.set("flash", "newer", ttl=200)
                _CHANGES[change](session)

            client.run(handler)

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]

    async def test_set_with_a_ttl_after_persist_gives_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session.persist("flash")
            session.set("flash", "new", ttl=40)

        with _client(store) as client:
            client.run(lambda s: s.set("flash", "old", ttl=300))
            key = store.session_key(client.sid)

            client.run(handler)

            assert _each_is(await _httl(fake_async_redis, key, "a:flash"), 40)

    async def test_persist_after_set_with_a_ttl_gives_no_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session.set("flash", "new", ttl=40)
            session.persist("flash")

        with _client(store) as client:
            client.run(lambda s: s.set("flash", "old", ttl=300))
            key = store.session_key(client.sid)

            client.run(handler)

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]
            assert client.session() == {"flash": "new"}

    async def test_a_ttl_is_for_one_save(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The store reads the request once.  A later request that changes
        the value with no ``ttl`` is a plain write."""
        with _client(store) as client:
            client.run(lambda s: s.set("flash", "old", ttl=300))
            key = store.session_key(client.sid)
            client.run(lambda s: s.__setitem__("other", 1))
            client.run(lambda s: s.__setitem__("flash", "new"))

            assert await _httl(fake_async_redis, key, "a:flash") == [NO_TTL]


class TestPersist:
    async def test_it_removes_the_ttl_and_keeps_the_value(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12e."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            before = await fake_async_redis.hgetall(key)

            client.run(lambda s: s.persist("flash"))

            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_TTL] * 3
            after = await fake_async_redis.hgetall(key)
            for name in FLASH_FIELDS:
                assert after[name.encode()] == before[name.encode()]
            assert client.session() == {"user": "ana", "flash": FLASH}

    async def test_the_save_sends_hpersist_and_no_command_for_the_value(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12e.  The store does not read the value, and does not write
        it."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            sent = record_commands(fake_async_redis)

            client.run(lambda s: s.persist("flash"))
            sent = list(sent)

        load, save = sent[:3], sent[3:]
        assert names(load) == ["HGETALL", "HTTL", "EXPIRE"]
        assert names(save) == ["HSET", "HPERSIST", "EXPIRE"]
        assert save[0][2::2] == ("s:seen",)
        assert save[1][:4] == ("HPERSIST", key, "FIELDS", "3")
        assert sorted(save[1][4:]) == sorted(FLASH_FIELDS)

    async def test_persist_and_a_ttl_for_another_attribute_in_one_request(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session.persist("flash")
            session.set("otp", "1234", ttl=40)

        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)

            client.run(handler)

            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_TTL] * 3
            assert _each_is(await _httl(fake_async_redis, key, "a:otp"), 40)

    async def test_for_an_absent_attribute_it_changes_nothing(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12g.  The request writes nothing: the session is not
        modified."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            before = await fake_async_redis.hgetall(key)
            sent = record_commands(fake_async_redis)

            client.run(lambda s: s.persist("absent"))
            sent = list(sent)

            assert names(sent) == ["HGETALL", "HTTL", "EXPIRE"], "a load, and no save"
            assert await fake_async_redis.hgetall(key) == before
            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 300)

    async def test_for_an_attribute_with_no_ttl_it_changes_nothing(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Test 12g.  A field with no TTL is not an error."""
        with _client(store) as client:
            client.run(_with_flash(300))
            key = store.session_key(client.sid)
            await _age(fake_async_redis, key, 200, *FLASH_FIELDS)

            client.run(lambda s: s.persist("user"))

            assert await _httl(fake_async_redis, key, "a:user") == [NO_TTL]
            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 200)
            assert client.session() == {"user": "ana", "flash": FLASH}

    async def test_for_an_attribute_that_expired_since_the_load_it_is_no_error(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """A field that does not exist is not an error, and the save does
        not bring the attribute back."""
        sid = store.new_id()
        key = store.session_key(sid)
        fields = await store.create(
            sid, store.new_record({"flash": "x", "n": 1}), ttls={"flash": 300}
        )
        await aexpire_now(fake_async_redis, key, "a:flash")

        await store.save(
            sid,
            store.new_record({"flash": "x", "n": 1}),
            baseline=fields,
            ttls={"flash": None},
        )

        assert await _httl(fake_async_redis, key, "a:flash") == [NO_FIELD]
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"n": 1}


class TestRotation:
    """Test 12d.  A rotation is not a write by the application."""

    async def test_a_rotation_keeps_the_time_left(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with _client(store) as client:
            client.run(_with_flash(300))
            old = client.sid
            await _age(fake_async_redis, store.session_key(old), 200, *FLASH_FIELDS)

            # ``user_id`` is the principal: a change of it rotates the session.
            client.run(lambda s: s.__setitem__("user_id", 7))

            assert client.sid != old
            key = store.session_key(client.sid)
            ttls = await _httl(fake_async_redis, key, *FLASH_FIELDS)
            assert all(_carried(ttl, 200) for ttl in ttls), ttls
            assert await _httl(fake_async_redis, key, "a:user", "a:user_id") == [
                NO_TTL,
                NO_TTL,
            ]
            assert client.session() == {"user": "ana", "flash": FLASH, "user_id": 7}

    async def test_a_ttl_given_in_the_request_that_rotates_is_used(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session["user_id"] = 7
            session.set("flash", dict(FLASH), ttl=40)
            session.set("otp", "1234", ttl=90)

        with _client(store) as client:
            client.run(_with_flash(300))
            old = client.sid

            client.run(handler)

            assert client.sid != old
            key = store.session_key(client.sid)
            assert _each_is(await _httl(fake_async_redis, key, *FLASH_FIELDS), 40)
            assert _each_is(await _httl(fake_async_redis, key, "a:otp"), 90)

    async def test_persist_in_the_request_that_rotates_removes_the_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session["user_id"] = 7
            session.persist("flash")

        with _client(store) as client:
            client.run(_with_flash(300))
            old = client.sid

            client.run(handler)

            assert client.sid != old
            key = store.session_key(client.sid)
            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_TTL] * 3

    async def test_an_attribute_removed_in_the_request_that_rotates_is_gone(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        def handler(session: Session) -> None:
            session["user_id"] = 7
            del session["flash"]

        with _client(store) as client:
            client.run(_with_flash(300))
            client.run(handler)
            key = store.session_key(client.sid)

            assert await _httl(fake_async_redis, key, *FLASH_FIELDS) == [NO_FIELD] * 3
            assert client.session() == {"user": "ana", "user_id": 7}

    async def test_a_handler_rotation_keeps_the_time_left(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``store.rotate(state)`` with the state of the request."""
        sid = store.new_id()
        data = {"user": "ana", "flash": dict(FLASH)}
        await store.create(sid, store.new_record(data), ttls={"flash": 300})
        await _age(fake_async_redis, store.session_key(sid), 200, "a:flash.text")
        loaded = await store.load(sid)
        assert loaded is not None
        state = SessionState(data=data, session_id=sid, fields=loaded.fields)

        new_id = await store.rotate(state)

        key = store.session_key(new_id)
        text, first, second, user = await _httl(
            fake_async_redis, key, *FLASH_FIELDS, "a:user"
        )
        assert _carried(text, 200)
        assert _carried(first, 300)
        assert _carried(second, 300)
        assert user == NO_TTL

    async def test_a_field_that_expired_before_the_rotation_does_not_come_back(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The rotation writes the session in memory.  The middleware loaded
        it without the attribute, so the new session has none."""
        with _client(store) as client:
            client.run(_with_flash(300))
            await aexpire_now(
                fake_async_redis, store.session_key(client.sid), *FLASH_FIELDS
            )

            client.run(lambda s: s.__setitem__("user_id", 7))

            assert client.session() == {"user": "ana", "user_id": 7}
