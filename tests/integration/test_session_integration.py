"""Integration tests for the session store against a real Redis server.

These prove what ``fakeredis`` cannot: that the server accepts each command
the store sends, that the server gives the fields and the TTLs the design
states, and that the capability probes read a real answer.

No test here needs the Query Engine.  A test for a server without a
capability passes ``_StoreCapabilities`` to the store, so it runs on each
Redis version.  The searches are in ``test_session_search_integration.py``.
"""

import time

import pytest
import redis.asyncio as async_redis
from redis.exceptions import ResponseError

from redis_fastapi.exceptions import SessionSearchUnavailableError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
    probe_hsetex_support,
    probe_search_support,
)
from redis_fastapi.session_layout import (
    FIELD_CREATED,
    FIELD_DESCRIPTOR,
    FIELD_SEEN,
    FIELD_SUBJECT,
)
from tests.conftest import about, aexpire_now, requires_redis

pytestmark = [pytest.mark.integration, requires_redis, pytest.mark.asyncio]

IDLE = 60
ABSOLUTE = 600

# What Redis answers to HTTL for a field with no TTL, and for a missing field.
NO_TTL = -1
NO_FIELD = -2

# A last access far in the past, so a write of the present time shows.
LONG_AGO = "1000.0"


def _store(redis, prefix: str, **kwargs) -> RedisSessionStore:
    kwargs.setdefault("idle_ttl", IDLE)
    kwargs.setdefault("absolute_ttl", ABSOLUTE)
    return RedisSessionStore(redis, key_prefix=prefix, **kwargs)


async def _httl(redis, key: str, field: str) -> int:
    reply = await redis.execute_command("HTTL", key, "FIELDS", 1, field)
    return int(reply[0])


async def _calls(redis, command: str) -> int:
    """How many times the server ran *command* since it started."""
    stats = await redis.info("commandstats")
    for name, value in stats.items():
        if name.lower() == f"cmdstat_{command.lower()}":
            return int(value["calls"])
    return 0


async def _server_has(redis, *command: str) -> bool:
    """Whether the server knows *command*, asked by sending it.

    The probes ask ``COMMAND INFO``.  This asks in another way, so a test
    that compares the two can fail.
    """
    try:
        await redis.execute_command(*command)
    except ResponseError as exc:
        if "unknown command" in str(exc).lower():
            return False
    return True


async def _records_access(redis) -> bool:
    """Whether a load on this server should write the last access."""
    return bool(await probe_hsetex_support(redis) and await probe_search_support(redis))


class TestTheProbesMatchTheServer:
    """The answer, not merely that there is one.

    ``in {True, False}`` cannot fail for anything that returns a bool,
    including ``return False`` for each server.  ``fakeredis`` cannot tell
    either: this file is the only place where a real server gives the answer.
    """

    async def test_the_hsetex_probe(self, real_async_redis: async_redis.Redis) -> None:
        expected = await _server_has(real_async_redis, "HSETEX")
        assert await probe_hsetex_support(real_async_redis) is expected

    async def test_the_hsetex_probe_follows_the_version(
        self, real_async_redis: async_redis.Redis
    ) -> None:
        info = await real_async_redis.info("server")
        raw = str(info["redis_version"])
        major, minor = (int(part) for part in raw.split(".")[:2])
        assert await probe_hsetex_support(real_async_redis) is (
            (major, minor) >= (8, 0)
        )

    async def test_the_search_probe(self, real_async_redis: async_redis.Redis) -> None:
        expected = await _server_has(real_async_redis, "FT._LIST")
        assert await probe_search_support(real_async_redis) is expected


@pytest.fixture(params=[True, False], ids=["hsetex", "no-hsetex"])
async def tier_store(
    request: pytest.FixtureRequest,
    real_async_redis: async_redis.Redis,
    test_prefix: str,
) -> RedisSessionStore:
    """A store with each value of ``supports_hsetex`` injected.

    ``supports_search`` is left to the probe.  The load sends ``HSETEX`` only
    where both are true, so ``supports_hsetex=True`` on a server without the
    command is a case with no ``HSETEX`` unless that server has the Query
    Engine.  That one case is skipped.
    """
    supports_hsetex: bool = request.param
    if (
        supports_hsetex
        and await probe_search_support(real_async_redis)
        and not await probe_hsetex_support(real_async_redis)
    ):
        pytest.skip("the server has the Query Engine and no HSETEX")
    return _store(
        real_async_redis,
        test_prefix,
        capabilities=_StoreCapabilities(supports_hsetex=supports_hsetex),
    )


SESSION = {
    "user_id": 42,
    "name": "Ana",
    "cart": {"total": 12.5, "items": ["road", "gravel"]},
    "tags": [],
    "flash": "Saved.",
}

SESSION_FIELDS = {
    "a:user_id": "42",
    "a:name": '"Ana"',
    "a:cart.total": "12.5",
    "a:cart.items[0]": '"road"',
    "a:cart.items[1]": '"gravel"',
    "a:tags": "[]",
    "a:flash": '"Saved."',
}


class TestTheCommandsOfEachOperation:
    """Design test 46: the fields and TTLs of section 3, on a real server.

    ``fakeredis`` reads command arguments in any order, so it accepts an
    option order that a real server rejects.  Each test runs with each value
    of ``supports_hsetex``.
    """

    async def _create(self, store: RedisSessionStore, redis) -> tuple[str, str, dict]:
        sid = store.new_id()
        record = store.new_record(dict(SESSION))
        fields = await store.create(
            sid,
            record,
            subject="u1",
            descriptor={"device": "laptop"},
            ttls={"flash": 30},
        )
        return sid, store.session_key(sid), fields

    async def test_create_writes_one_field_for_each_leaf(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        _, key, returned = await self._create(tier_store, real_async_redis)

        stored = await real_async_redis.hgetall(key)
        assert stored == returned, "create did not return what it stored"
        attributes = {k: v for k, v in stored.items() if k.startswith("a:")}
        assert attributes == SESSION_FIELDS
        assert stored[FIELD_SUBJECT] == '"u1"'
        assert stored[FIELD_DESCRIPTOR] == '{"device": "laptop"}'
        assert float(stored[FIELD_CREATED]) == pytest.approx(time.time(), abs=5)
        assert stored[FIELD_SEEN] == stored[FIELD_CREATED]
        assert set(stored) == {
            *SESSION_FIELDS,
            FIELD_SUBJECT,
            FIELD_DESCRIPTOR,
            FIELD_CREATED,
            FIELD_SEEN,
        }

    async def test_create_starts_each_clock(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        _, key, stored = await self._create(tier_store, real_async_redis)

        assert about(await real_async_redis.ttl(key), IDLE)
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), ABSOLUTE)
        assert about(await _httl(real_async_redis, key, "a:flash"), 30)
        for name in set(stored) - {FIELD_CREATED, "a:flash"}:
            assert await _httl(real_async_redis, key, name) == NO_TTL, name

    async def test_load_returns_the_session_and_the_time_left(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid, key, stored = await self._create(tier_store, real_async_redis)
        await real_async_redis.hexpire(key, 100, FIELD_CREATED)

        loaded = await tier_store.load(sid)

        assert loaded is not None
        assert loaded.record.data == SESSION
        assert loaded.fields == stored
        assert loaded.record.metadata.created == float(stored[FIELD_CREATED])
        assert about(loaded.absolute_remaining, 100)

    async def test_load_restarts_the_idle_clock_only(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid, key, _ = await self._create(tier_store, real_async_redis)
        await real_async_redis.expire(key, 10)
        await real_async_redis.hexpire(key, 100, FIELD_CREATED)
        await real_async_redis.hexpire(key, 20, "a:flash")

        assert await tier_store.load(sid) is not None

        assert about(await real_async_redis.ttl(key), IDLE)
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), 100)
        assert about(await _httl(real_async_redis, key, "a:flash"), 20)

    async def test_load_writes_the_last_access_only_where_a_search_can_run(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """Design tests 13a and 22a.

        Only a search reads ``s:seen``, and only ``HSETEX ... FXX`` can write
        it with no risk of a new key.  With either one missing the load
        leaves the field as it is.
        """
        sid, key, _ = await self._create(tier_store, real_async_redis)
        await real_async_redis.hset(key, FIELD_SEEN, LONG_AGO)
        writes = tier_store._caps.supports_hsetex and await probe_search_support(
            real_async_redis
        )

        assert await tier_store.load(sid) is not None

        seen = await real_async_redis.hget(key, FIELD_SEEN)
        if writes:
            assert float(seen) == pytest.approx(time.time(), abs=5)
            assert await _httl(real_async_redis, key, FIELD_SEEN) == NO_TTL
        else:
            assert seen == LONG_AGO

    async def test_a_plain_read_moves_no_clock(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """``refresh=False`` is the load of ``session_refresh_on_load=False``."""
        sid, key, _ = await self._create(tier_store, real_async_redis)
        await real_async_redis.expire(key, 10)
        await real_async_redis.hset(key, FIELD_SEEN, LONG_AGO)

        assert await tier_store.load(sid, refresh=False) is not None

        assert about(await real_async_redis.ttl(key), 10)
        assert await real_async_redis.hget(key, FIELD_SEEN) == LONG_AGO

    async def test_touch_restarts_the_idle_clock_only(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid, key, _ = await self._create(tier_store, real_async_redis)
        await real_async_redis.expire(key, 10)
        await real_async_redis.hexpire(key, 100, FIELD_CREATED)

        await tier_store.touch(sid)

        assert about(await real_async_redis.ttl(key), IDLE)
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), 100)

    async def test_save_writes_the_difference(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """A changed field is set, a removed field is deleted, and a field
        that did not change is not written.

        ``HSET`` removes the TTL of a field.  So the TTL of ``a:flash`` and
        of ``s:created`` after the save shows that the save did not write
        them.
        """
        sid, key, baseline = await self._create(tier_store, real_async_redis)
        await real_async_redis.expire(key, 10)
        await real_async_redis.hexpire(key, 100, FIELD_CREATED)
        await real_async_redis.hexpire(key, 20, "a:flash")

        changed = dict(SESSION, name="Boris", cart={"total": 12.5, "items": ["road"]})
        del changed["tags"]
        record = tier_store.new_record(changed, created=float(baseline[FIELD_CREATED]))
        returned = await tier_store.save(
            sid,
            record,
            baseline=baseline,
            subject="u1",
            descriptor={"device": "laptop"},
        )

        stored = await real_async_redis.hgetall(key)
        assert stored == returned, "save did not return what the hash holds"
        assert stored["a:name"] == '"Boris"'
        assert "a:cart.items[1]" not in stored
        assert "a:tags" not in stored
        assert stored[FIELD_CREATED] == baseline[FIELD_CREATED]
        assert stored[FIELD_SEEN] == repr(record.metadata.last_access)
        assert about(await real_async_redis.ttl(key), IDLE)
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), 100)
        assert about(await _httl(real_async_redis, key, "a:flash"), 20)

        loaded = await tier_store.load(sid)
        assert loaded is not None
        assert loaded.record.data == changed

    async def test_save_sets_and_removes_an_attribute_ttl(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """``HEXPIRE`` for a write with a TTL, ``HPERSIST`` for ``persist``.

        The TTL covers each field of the attribute, changed or not.
        """
        sid, key, baseline = await self._create(tier_store, real_async_redis)
        record = tier_store.new_record(dict(SESSION))

        baseline = await tier_store.save(
            sid, record, baseline=baseline, subject="u1", ttls={"cart": 45}
        )
        for name in ("a:cart.total", "a:cart.items[0]", "a:cart.items[1]"):
            assert about(await _httl(real_async_redis, key, name), 45), name

        await tier_store.save(
            sid,
            record,
            baseline=baseline,
            subject="u1",
            ttls={"cart": None, "flash": None, "absent": None},
        )
        for name in ("a:cart.total", "a:cart.items[0]", "a:cart.items[1]", "a:flash"):
            assert await _httl(real_async_redis, key, name) == NO_TTL, name
        assert await real_async_redis.hget(key, "a:flash") == '"Saved."'
        assert await _httl(real_async_redis, key, "a:absent") == NO_FIELD
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), ABSOLUTE)

    async def test_rotate_starts_a_new_absolute_clock_and_carries_attribute_ttls(
        self, tier_store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid, old_key, fields = await self._create(tier_store, real_async_redis)
        await real_async_redis.expire(old_key, 10)
        await real_async_redis.hexpire(old_key, 100, FIELD_CREATED)
        await real_async_redis.hexpire(old_key, 20, "a:flash")
        state = SessionState(
            data=dict(SESSION), session_id=sid, subject="u1", fields=fields
        )

        new_id = await tier_store.rotate(state, descriptor={"device": "laptop"})

        new_key = tier_store.session_key(new_id)
        assert new_id != sid and state.session_id == new_id
        assert await real_async_redis.exists(old_key) == 0
        stored = await real_async_redis.hgetall(new_key)
        assert {k: v for k, v in stored.items() if k.startswith("a:")} == SESSION_FIELDS
        assert stored[FIELD_SUBJECT] == '"u1"'
        assert stored[FIELD_DESCRIPTOR] == '{"device": "laptop"}'
        assert about(await real_async_redis.ttl(new_key), IDLE)
        assert about(await _httl(real_async_redis, new_key, FIELD_CREATED), ABSOLUTE)
        # Whole seconds, read once by the store and once here.
        assert 18 <= await _httl(real_async_redis, new_key, "a:flash") <= 20
        assert await _httl(real_async_redis, new_key, "a:name") == NO_TTL
        assert state.absolute_remaining == ABSOLUTE


class TestRedisEnforcesTheClocks:
    """The server ends the session; nothing in the store counts down."""

    async def test_the_idle_clock_is_the_ttl_of_the_key(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = _store(real_async_redis, test_prefix)
        key = store.session_key(sid := store.new_id())
        await store.create(sid, store.new_record({"user_id": 42}))
        assert await store.load(sid) is not None
        assert about(await real_async_redis.ttl(key), IDLE)

        await aexpire_now(real_async_redis, key)

        assert await store.load(sid) is None, "the idle clock was not enforced"

    async def test_activity_does_not_move_the_absolute_clock(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """The guarantee the absolute clock exists for.

        The session is loaded and saved many times.  Each one restarts the
        idle clock, and none may move the deadline.
        """
        store = _store(real_async_redis, test_prefix)
        key = store.session_key(sid := store.new_id())
        baseline = await store.create(sid, store.new_record({"user_id": 42, "n": 0}))
        assert about(await _httl(real_async_redis, key, FIELD_CREATED), ABSOLUTE)
        await real_async_redis.hexpire(key, 100, FIELD_CREATED)

        for n in range(1, 6):
            loaded = await store.load(sid)
            assert loaded is not None
            baseline = await store.save(
                sid,
                store.new_record({"user_id": 42, "n": n}),
                baseline=loaded.fields,
            )
            await store.touch(sid)
            assert about(await _httl(real_async_redis, key, FIELD_CREATED), 100), (
                "activity moved the absolute deadline"
            )
        assert baseline["a:n"] == "5"

    async def test_the_deadline_ends_the_session_while_the_key_lives(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """Only ``s:created`` expires at the deadline.  The key has its idle
        TTL still, so the load must see the missing field, return no session,
        and delete the key."""
        store = _store(real_async_redis, test_prefix)
        key = store.session_key(sid := store.new_id())
        await store.create(sid, store.new_record({"user_id": 42}))

        await aexpire_now(real_async_redis, key, FIELD_CREATED)
        assert await real_async_redis.hget(key, "a:user_id") == "42"

        assert await store.load(sid) is None
        assert await real_async_redis.exists(key) == 0

    @pytest.mark.parametrize("ended_by", ["deadline", "revocation"])
    async def test_a_save_after_the_session_ended_does_not_revive_it(
        self, real_async_redis: async_redis.Redis, test_prefix: str, ended_by: str
    ) -> None:
        """A request that loaded a live session and saved it after it ended.

        The save never writes ``s:created``, so what it leaves is not a
        session.  The key has the idle TTL, so it cannot stay for ever, and
        the next load deletes it.
        """
        store = _store(real_async_redis, test_prefix)
        key = store.session_key(sid := store.new_id())
        await store.create(sid, store.new_record({"user_id": 42}))
        loaded = await store.load(sid)
        assert loaded is not None

        if ended_by == "deadline":
            await aexpire_now(real_async_redis, key, FIELD_CREATED)
        else:
            await store.delete(sid)

        for n in range(3):
            await store.save(
                sid,
                store.new_record({"user_id": 42, "n": n}),
                baseline=loaded.fields,
            )
        assert await real_async_redis.hexists(key, FIELD_CREATED) is False
        assert about(await real_async_redis.ttl(key), IDLE), (
            "the save left a key that never expires"
        )

        assert await store.load(sid) is None
        assert await real_async_redis.exists(key) == 0

    async def test_a_hash_in_the_earlier_layout_is_not_a_session(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """An upgrade signs each user out once, and leaves no old key."""
        store = _store(real_async_redis, test_prefix)
        key = store.session_key(sid := store.new_id())
        await real_async_redis.hset(key, "d", '{"m": {}, "d": {"user_id": 42}}')
        await real_async_redis.expire(key, ABSOLUTE)

        assert await store.load(sid) is None
        assert await real_async_redis.exists(key) == 0


class TestRotation:
    async def test_the_old_key_is_gone_and_the_new_one_exists(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = _store(real_async_redis, test_prefix)
        state = SessionState(data={"user_id": 42}, session_id=store.new_id())
        await store.create(state.session_id, store.new_record(dict(state.data)))
        old = state.session_id

        new = await store.rotate(state, subject="42")

        assert new != old
        assert await real_async_redis.exists(store.session_key(old)) == 0
        assert await real_async_redis.exists(store.session_key(new)) == 1
        assert await store.load(old) is None
        assert (await store.load(new)).record.data == {"user_id": 42}

    async def test_without_a_subject_it_keeps_the_current_one(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """A handler's step-up ``rotate(state)`` must stay with its user."""
        store = _store(real_async_redis, test_prefix)
        state = SessionState(
            data={"user_id": 42}, session_id=store.new_id(), subject="42"
        )
        await store.create(
            state.session_id, store.new_record(dict(state.data)), subject="42"
        )

        new = await store.rotate(state)

        assert state.subject == "42"
        key = store.session_key(new)
        assert await real_async_redis.hget(key, FIELD_SUBJECT) == '"42"'

    async def test_to_no_subject_it_writes_no_subject(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """``subject=None`` is how the middleware says the session is
        anonymous.  The new hash must not name the user who left."""
        store = _store(real_async_redis, test_prefix)
        state = SessionState(
            data={"user_id": None}, session_id=store.new_id(), subject="42"
        )
        await store.create(
            state.session_id, store.new_record(dict(state.data)), subject="42"
        )

        new = await store.rotate(state, subject=None)

        assert state.subject is None
        key = store.session_key(new)
        assert await real_async_redis.hexists(key, FIELD_SUBJECT) is False
        assert await store.revoke_id(new, subject="42") is False


@pytest.mark.parametrize("supports_search", [True, False], ids=["search", "no-search"])
@pytest.mark.parametrize("supports_hsetex", [True, False], ids=["hsetex", "no-hsetex"])
class TestACookieThatNamesNoSession:
    """Design test 13: no key, and no command that can create one.

    A request can carry any well-formed ID.  If the load created a key for
    it, a client could fill the server with one request for each ID.
    """

    @pytest.fixture()
    async def store(
        self,
        real_async_redis: async_redis.Redis,
        test_prefix: str,
        supports_search: bool,
        supports_hsetex: bool,
    ) -> RedisSessionStore:
        if supports_search and supports_hsetex:
            # The one case that sends HSETEX, so the server must have it.
            if not await probe_hsetex_support(real_async_redis):
                pytest.skip("the server has no HSETEX")
        return _store(
            real_async_redis,
            test_prefix,
            capabilities=_StoreCapabilities(
                supports_hsetex=supports_hsetex, supports_search=supports_search
            ),
        )

    async def test_a_load_leaves_no_key(
        self,
        store: RedisSessionStore,
        real_async_redis: async_redis.Redis,
        supports_search: bool,
        supports_hsetex: bool,
    ) -> None:
        assert await real_async_redis.dbsize() == 0
        hset = await _calls(real_async_redis, "hset")
        hsetex = await _calls(real_async_redis, "hsetex")

        assert await store.load(store.new_id()) is None

        assert await real_async_redis.dbsize() == 0
        assert await _calls(real_async_redis, "hset") == hset, "the load sent HSET"
        # The FXX write is the only write of the load. It ran, and made no key.
        expected = 1 if supports_search and supports_hsetex else 0
        assert await _calls(real_async_redis, "hsetex") == hsetex + expected

    async def test_a_plain_read_and_a_touch_leave_no_key(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """The two halves of a request under ``session_refresh_on_load=False``."""
        sid = store.new_id()
        hset = await _calls(real_async_redis, "hset")

        assert await store.load(sid, refresh=False) is None
        await store.touch(sid)

        assert await real_async_redis.dbsize() == 0
        assert await _calls(real_async_redis, "hset") == hset


class TestAServerWithoutTheQueryEngine:
    """Design test 35, with ``supports_search=False`` injected.

    Requirement 8: a session loads and saves by ID on a server that cannot
    search.  Each method that reads an index says so by name, and sends
    nothing.
    """

    @pytest.fixture()
    def store(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> RedisSessionStore:
        return _store(
            real_async_redis,
            test_prefix,
            searchable={"device_id": "tag", "total": "numeric"},
            capabilities=_StoreCapabilities(supports_search=False),
        )

    async def test_the_lifecycle_by_id_works(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid = store.new_id()
        data = {"user_id": "u1", "device_id": "d-1", "total": 5}
        fields = await store.create(sid, store.new_record(dict(data)), subject="u1")

        loaded = await store.load(sid)
        assert loaded is not None and loaded.record.data == data

        data["total"] = 6
        fields = await store.save(
            sid, store.new_record(dict(data)), baseline=fields, subject="u1"
        )
        assert (await store.load(sid)).record.data == data

        state = SessionState(
            data=dict(data), session_id=sid, subject="u1", fields=fields
        )
        rotated = await store.rotate(state)
        assert await store.load(sid) is None
        assert (await store.load(rotated)).record.data == data

        assert await store.revoke_id(rotated, subject="someone-else") is False
        assert await store.load(rotated) is not None
        assert await store.revoke_id(rotated, subject="u1") is True
        assert await store.load(rotated) is None

        other = store.new_id()
        await store.create(other, store.new_record(dict(data)), subject="u1")
        state = SessionState(data=dict(data), session_id=other, subject="u1")
        await store.revoke(state)
        assert state.revoked and state.session_id is None
        assert await real_async_redis.dbsize() == 0

    async def test_a_load_does_not_write_the_last_access(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """Only a search reads ``s:seen``, and this server has none."""
        key = store.session_key(sid := store.new_id())
        await store.create(sid, store.new_record({"user_id": "u1"}), subject="u1")
        await real_async_redis.hset(key, FIELD_SEEN, LONG_AGO)

        assert await store.load(sid) is not None

        assert await real_async_redis.hget(key, FIELD_SEEN) == LONG_AGO

    @pytest.mark.parametrize(
        "call",
        [
            lambda store: store.list_for_subject("u1"),
            lambda store: store.count_for_subject("u1"),
            lambda store: store.revoke_all("u1"),
            lambda store: store.list_active(60),
            lambda store: store.revoke_idle(60),
            lambda store: store.find("device_id", equals="d-1"),
            lambda store: store.count("total", between=(1, 10)),
        ],
        ids=[
            "list_for_subject",
            "count_for_subject",
            "revoke_all",
            "list_active",
            "revoke_idle",
            "find",
            "count",
        ],
    )
    async def test_each_search_raises_and_sends_nothing(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis, call
    ) -> None:
        sid = store.new_id()
        await store.create(
            sid,
            store.new_record({"user_id": "u1", "device_id": "d-1", "total": 5}),
            subject="u1",
        )
        sent = {
            name: await _calls(real_async_redis, name)
            for name in ("FT.SEARCH", "FT.CREATE", "FT._LIST", "del")
        }

        with pytest.raises(SessionSearchUnavailableError):
            await call(store)

        for name, before in sent.items():
            assert await _calls(real_async_redis, name) == before, name
        assert await store.load(sid) is not None, "a refused revoke ended a session"

    async def test_the_ensure_step_creates_no_index(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        created = await _calls(real_async_redis, "FT.CREATE")

        await store.ensure_indexes()

        assert await _calls(real_async_redis, "FT.CREATE") == created
