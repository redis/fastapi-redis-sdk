"""Session searches against a real Redis with the Query Engine.

``fakeredis`` has no ``FT.*`` command, so this file is the only place where a
query the store builds meets the engine that runs it.  Each test skips itself
on a server without the Query Engine.

Two rules keep these tests exact:

* A test that needs an old last access writes ``s:seen`` to the hash itself.
  No test depends on the clock for it.
* The engine indexes a write to a hash before the command returns.  It fills
  an index in the background only after a create over keys that exist.  A
  test that creates an index over such keys waits for ``FT.INFO``.
"""

import asyncio
import logging
import re
import time
from collections.abc import AsyncIterator

import pytest
import redis.asyncio as async_redis

from redis_fastapi.exceptions import SessionSearchTimeoutError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionInfo,
    SessionState,
    _StoreCapabilities,
    probe_hsetex_support,
)
from redis_fastapi.session_layout import FIELD_CREATED, FIELD_SEEN, FIELD_SUBJECT
from redis_fastapi.session_search import LIST_LIMIT
from tests.conftest import REDIS_URL, aexpire_now, requires_query_engine, requires_redis

pytestmark = [
    pytest.mark.integration,
    requires_redis,
    requires_query_engine,
    pytest.mark.asyncio,
]

IDLE = 60
ABSOLUTE = 600

# More than one batch of a bulk revoke, and more than a list returns.
MANY = LIST_LIMIT + 100

# Subjects and TAG values that the query syntax could misread.
AWKWARD = [
    "plain",
    "with,comma",
    "Upper",
    "upper",
    'with"quote',
    "with\\backslash",
    "with space",
    "with-dash",
    "curly{brace}",
    "pipe|pipe",
    "ünïcode-日本",
]


class _Reversing:
    """An ``Encryptor`` for tests: not secret, but no value stays readable."""

    def encrypt(self, data: bytes) -> bytes:
        return b"enc:" + data[::-1]

    def decrypt(self, data: bytes) -> bytes:
        assert data.startswith(b"enc:")
        return data[4:][::-1]


def _store(redis, prefix: str, **kwargs) -> RedisSessionStore:
    kwargs.setdefault("idle_ttl", IDLE)
    kwargs.setdefault("absolute_ttl", ABSOLUTE)
    return RedisSessionStore(redis, key_prefix=prefix, **kwargs)


async def _ready(redis, prefix: str, **kwargs) -> RedisSessionStore:
    """A store whose indexes exist, as after the startup of an app.

    The indexes must exist before the first write.  An index that the first
    search creates fills in the background, and that search sees a part of
    the sessions.
    """
    store = _store(redis, prefix, **kwargs)
    await store.ensure_indexes()
    return store


async def _new(
    store: RedisSessionStore,
    redis,
    data: dict | None = None,
    *,
    subject: str | None = None,
    descriptor: dict | None = None,
    seen: float | None = None,
) -> str:
    """Create one session through the store.  *seen* sets its last access."""
    sid = store.new_id()
    await store.create(
        sid, store.new_record(dict(data or {})), subject=subject, descriptor=descriptor
    )
    if seen is not None:
        await redis.hset(store.session_key(sid), FIELD_SEEN, repr(seen))
    return sid


async def _new_many(
    store: RedisSessionStore, redis, count: int, *, subject: str, base: float
) -> list[str]:
    """Create *count* sessions of *subject*; session ``i`` has ``s:seen``
    ``base + i``, so the last one has the newest access."""
    ids = []
    for _ in range(count):
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": subject}), subject=subject)
        ids.append(sid)
    pipe = redis.pipeline(transaction=False)
    for i, sid in enumerate(ids):
        pipe.hset(store.session_key(sid), FIELD_SEEN, repr(base + i))
    await pipe.execute()
    return ids


def _ids(infos: list[SessionInfo]) -> list[str]:
    return [info.session_id for info in infos]


async def _indexes(redis, prefix: str) -> set[str]:
    return {
        name
        for name in await redis.execute_command("FT._LIST")
        if name.startswith(f"{prefix}:")
    }


async def _indexing_ended(redis, index: str) -> None:
    """Wait until the engine has read each key that existed at the create."""
    for _ in range(500):
        info = await redis.ft(index).info()
        if int(info["indexing"]) == 0:
            return
        await asyncio.sleep(0.01)
    raise AssertionError(f"{index} did not finish indexing")


async def _calls(redis, command: str) -> int:
    """How many times the server ran *command* since it started."""
    stats = await redis.info("commandstats")
    for name, value in stats.items():
        if name.lower() == f"cmdstat_{command.lower()}":
            return int(value["calls"])
    return 0


@pytest.fixture(params=[None, _Reversing()], ids=["plain", "encrypted"])
async def store(
    request: pytest.FixtureRequest,
    real_async_redis: async_redis.Redis,
    test_prefix: str,
) -> RedisSessionStore:
    """The store of the built-in searches, with and without an ``Encryptor``.

    Design test 24: the system fields are plaintext with an ``Encryptor``
    too, so each built-in search must work the same.
    """
    return await _ready(real_async_redis, test_prefix, encryptor=request.param)


class TestListForSubject:
    """Design test 14."""

    async def test_it_returns_the_sessions_of_the_subject_only(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        now = time.time()
        old = await _new(
            store,
            real_async_redis,
            {"user_id": "u1", "secret": "s3"},
            subject="u1",
            descriptor={"device": "phone"},
            seen=now - 300,
        )
        new = await _new(
            store,
            real_async_redis,
            {"user_id": "u1"},
            subject="u1",
            descriptor={"device": "laptop", "ip": "10.0.0.1"},
            seen=now - 5,
        )
        await _new(store, real_async_redis, {"user_id": "u2"}, subject="u2")
        await _new(store, real_async_redis, {"cart": ["x"]})

        infos = await store.list_for_subject("u1")

        assert _ids(infos) == [new, old], "not newest access first"
        assert infos[0].descriptor == {"device": "laptop", "ip": "10.0.0.1"}
        assert infos[1].descriptor == {"device": "phone"}
        assert infos[0].last_access == pytest.approx(now - 5, abs=0.01)
        assert infos[1].last_access == pytest.approx(now - 300, abs=0.01)
        assert infos[0].created == pytest.approx(now, abs=5)

    async def test_a_session_with_no_descriptor_has_an_empty_one(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid = await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")

        assert await store.list_for_subject("u1") == [
            SessionInfo(
                session_id=sid,
                created=pytest.approx(time.time(), abs=5),
                last_access=pytest.approx(time.time(), abs=5),
                descriptor={},
            )
        ]

    async def test_an_unknown_subject_has_no_session(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")

        assert await store.list_for_subject("nobody") == []
        assert await store.count_for_subject("nobody") == 0

    async def test_it_returns_the_thousand_with_the_newest_access(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """The list has a fixed limit.  The count has none, and it is the
        answer to "how many"."""
        ids = await _new_many(
            store, real_async_redis, MANY, subject="u1", base=time.time() - MANY
        )

        infos = await store.list_for_subject("u1")

        assert len(infos) == LIST_LIMIT
        assert _ids(infos) == list(reversed(ids))[:LIST_LIMIT]
        assert await store.count_for_subject("u1") == MANY


class TestCountForSubject:
    """Design test 15."""

    async def test_it_matches_the_number_of_live_sessions(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        ids = [
            await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")
            for _ in range(5)
        ]
        await _new(store, real_async_redis, {"user_id": "u2"}, subject="u2")
        await _new(store, real_async_redis, {})
        assert await store.count_for_subject("u1") == 5

        await store.delete(ids[0])
        assert await store.count_for_subject("u1") == 4

        await aexpire_now(real_async_redis, store.session_key(ids[1]))
        assert await store.count_for_subject("u1") == 3

        assert await store.revoke_id(ids[2], subject="u1") is True
        assert await store.count_for_subject("u1") == 2
        assert await store.count_for_subject("u2") == 1

    async def test_a_rotation_moves_the_session_and_adds_none(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"user_id": "u1"}), subject="u1"
        )
        state = SessionState(
            data={"user_id": "u1"}, session_id=sid, subject="u1", fields=fields
        )

        new = await store.rotate(state)

        assert _ids(await store.list_for_subject("u1")) == [new]
        assert await store.count_for_subject("u1") == 1

    async def test_a_rotation_to_no_subject_leaves_the_subject_with_none(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """The anonymous session must not be listed under the user who left."""
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"user_id": "u1"}), subject="u1"
        )
        state = SessionState(
            data={"user_id": None}, session_id=sid, subject="u1", fields=fields
        )

        new = await store.rotate(state, subject=None)

        assert await store.load(new) is not None
        assert await store.list_for_subject("u1") == []
        assert await store.count_for_subject("u1") == 0

    async def test_a_save_that_changes_the_subject_moves_the_session(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"user_id": "u1"}), subject="u1"
        )

        await store.save(
            sid, store.new_record({"user_id": "u2"}), baseline=fields, subject="u2"
        )

        assert await store.count_for_subject("u1") == 0
        assert _ids(await store.list_for_subject("u2")) == [sid]


class TestRevokeAll:
    """Design test 16."""

    async def test_it_ends_each_session_of_the_subject_only(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        mine = [
            await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")
            for _ in range(3)
        ]
        other = await _new(store, real_async_redis, {"user_id": "u2"}, subject="u2")
        anonymous = await _new(store, real_async_redis, {"cart": 1})

        assert await store.revoke_all("u1") == 3

        for sid in mine:
            assert await store.load(sid) is None
            assert await real_async_redis.exists(store.session_key(sid)) == 0
        assert await store.load(other) is not None
        assert await store.load(anonymous) is not None
        assert await store.count_for_subject("u1") == 0
        assert await store.revoke_all("u1") == 0

    async def test_it_ends_more_sessions_than_one_batch_holds(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        await _new_many(store, real_async_redis, MANY, subject="u1", base=1000.0)
        other = await _new(store, real_async_redis, {"user_id": "u2"}, subject="u2")

        assert await store.revoke_all("u1") == MANY

        assert await store.count_for_subject("u1") == 0
        assert await real_async_redis.dbsize() == 1
        assert await store.load(other) is not None


class TestAnAwkwardSubject:
    """Design test 18.

    The subject is a TAG.  A comma would split it with the default
    separator, a TAG folds case by default, and a quote or a backslash could
    end the value in the query.
    """

    async def test_each_subject_finds_its_own_session_only(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        owner = {
            subject: await _new(
                store, real_async_redis, {"user_id": subject}, subject=subject
            )
            for subject in AWKWARD
        }
        # Parts of the subjects above. None is a subject of its own.
        for part in ("with", "comma", "brace", "pipe", "日本", "UPPER"):
            assert await store.list_for_subject(part) == [], part

        for subject, sid in owner.items():
            assert _ids(await store.list_for_subject(subject)) == [sid], subject
            assert await store.count_for_subject(subject) == 1, subject

    async def test_revoke_all_ends_that_subject_only(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        owner = {
            subject: await _new(
                store, real_async_redis, {"user_id": subject}, subject=subject
            )
            for subject in AWKWARD
        }

        for subject, sid in owner.items():
            assert await store.revoke_all(subject) == 1, subject
            assert await store.load(sid) is None
            owner_left = len(AWKWARD) - 1 - AWKWARD.index(subject)
            assert await real_async_redis.dbsize() == owner_left, subject


class TestASessionPastItsDeadline:
    """Design test 20.

    At the deadline only ``s:created`` expires.  The key stays until the next
    load or until the idle TTL, and no search may show it in that time.
    """

    async def test_it_is_in_no_list_and_in_no_count(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        live = await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")
        dead = await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")
        assert await store.count_for_subject("u1") == 2

        key = store.session_key(dead)
        await aexpire_now(real_async_redis, key, FIELD_CREATED)
        assert await real_async_redis.exists(key) == 1
        assert await real_async_redis.hexists(key, FIELD_SUBJECT) is True

        assert _ids(await store.list_for_subject("u1")) == [live]
        assert await store.count_for_subject("u1") == 1
        assert _ids(await store.list_active(3600)) == [live]

    async def test_a_hash_that_a_late_save_left_is_in_no_list(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """A save after a revocation writes a hash with a subject and no
        ``s:created``.  It is not a session, so no search may count it."""
        sid = store.new_id()
        fields = await store.create(
            sid, store.new_record({"user_id": "u1"}), subject="u1"
        )
        await store.delete(sid)

        await store.save(
            sid, store.new_record({"user_id": "u1", "n": 1}), baseline=fields
        )
        key = store.session_key(sid)
        await real_async_redis.hset(key, FIELD_SUBJECT, '"u1"')

        assert await real_async_redis.exists(key) == 1
        assert await store.list_for_subject("u1") == []
        assert await store.count_for_subject("u1") == 0
        assert await store.list_active(3600) == []


class TestListActive:
    """Design test 22, with ``s:seen`` written by the test."""

    async def test_it_returns_the_sessions_seen_in_the_window(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        now = time.time()
        recent = await _new(store, real_async_redis, {"n": 1}, seen=now - 10)
        older = await _new(
            store, real_async_redis, {"n": 2}, subject="u1", seen=now - 100
        )
        await _new(store, real_async_redis, {"n": 3}, subject="u1", seen=now - 1000)

        assert _ids(await store.list_active(50)) == [recent]
        assert _ids(await store.list_active(500)) == [recent, older]
        assert await store.list_active(5) == []

    async def test_limit_and_offset_page_through_the_result(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        now = time.time()
        ids = [
            await _new(store, real_async_redis, {"n": n}, seen=now - 10 - n)
            for n in range(5)
        ]
        await _new(store, real_async_redis, {"n": 99}, seen=now - 5000)

        assert _ids(await store.list_active(600, limit=2)) == ids[0:2]
        assert _ids(await store.list_active(600, limit=2, offset=2)) == ids[2:4]
        assert _ids(await store.list_active(600, limit=2, offset=4)) == ids[4:5]
        assert await store.list_active(600, limit=2, offset=6) == []

    async def test_a_load_makes_the_session_active(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """Design test 22a: a read-only request records the last access.

        The load needs ``HSETEX ... FXX`` for this.  A server with the Query
        Engine and no ``HSETEX`` records the last save only.
        """
        if not await probe_hsetex_support(real_async_redis):
            pytest.skip("the server has no HSETEX")
        sid = await _new(store, real_async_redis, {"n": 1}, seen=1000.0)
        assert await store.list_active(600) == []

        assert await store.load(sid) is not None

        key = store.session_key(sid)
        seen = float(await real_async_redis.hget(key, FIELD_SEEN))
        assert seen == pytest.approx(time.time(), abs=5)
        infos = await store.list_active(600)
        assert _ids(infos) == [sid]
        assert infos[0].last_access == pytest.approx(seen, abs=0.01)

    async def test_a_plain_read_then_a_touch_makes_the_session_active(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        """The same under ``session_refresh_on_load=False``: the read writes
        nothing, and the response step records the access."""
        if not await probe_hsetex_support(real_async_redis):
            pytest.skip("the server has no HSETEX")
        sid = await _new(store, real_async_redis, {"n": 1}, seen=1000.0)

        assert await store.load(sid, refresh=False) is not None
        assert await store.list_active(600) == []

        await store.touch(sid)
        assert _ids(await store.list_active(600)) == [sid]


class TestRevokeIdle:
    """Design test 23, with ``s:seen`` written by the test."""

    async def test_it_ends_the_sessions_not_seen_for_that_long(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        now = time.time()
        recent = await _new(store, real_async_redis, {"n": 1}, seen=now - 10)
        older = await _new(
            store, real_async_redis, {"n": 2}, subject="u1", seen=now - 100
        )
        oldest = await _new(
            store, real_async_redis, {"n": 3}, subject="u1", seen=now - 1000
        )
        ancient = await _new(store, real_async_redis, {"n": 4}, seen=now - 9000)

        assert await store.revoke_idle(500) == 2

        assert await real_async_redis.exists(store.session_key(oldest)) == 0
        assert await real_async_redis.exists(store.session_key(ancient)) == 0
        assert (await store.load(recent)).record.data == {"n": 1}
        assert (await store.load(older)).record.data == {"n": 2}
        assert await store.revoke_idle(500) == 0

    async def test_it_ends_more_sessions_than_one_batch_holds(
        self, store: RedisSessionStore, real_async_redis: async_redis.Redis
    ) -> None:
        await _new_many(store, real_async_redis, MANY, subject="u1", base=1000.0)
        recent = await _new(store, real_async_redis, {"n": 1}, subject="u1")

        assert await store.revoke_idle(3600) == MANY

        assert _ids(await store.list_for_subject("u1")) == [recent]


class TestTheReplyOfEachProtocol:
    """redis-py returns a different type for a search on RESP2 and on RESP3.

    The fixtures of the suite use RESP2.  This is the one place where a real
    RESP3 reply reaches the store.
    """

    @pytest.fixture()
    async def resp3(self) -> AsyncIterator[async_redis.Redis]:
        client = async_redis.Redis.from_url(
            REDIS_URL, decode_responses=True, protocol=3
        )
        yield client
        await client.aclose()

    async def test_resp3_gives_the_same_answers_as_resp2(
        self,
        real_async_redis: async_redis.Redis,
        resp3: async_redis.Redis,
        test_prefix: str,
    ) -> None:
        searchable = {"device_id": "tag", "total": "numeric"}
        store2 = await _ready(real_async_redis, test_prefix, searchable=searchable)
        store3 = _store(resp3, test_prefix, searchable=searchable)
        now = time.time()
        for n in range(3):
            await _new(
                store2,
                real_async_redis,
                {"user_id": "u1", "device_id": f"d-{n}", "total": n},
                subject="u1",
                descriptor={"n": n},
                seen=now - n,
            )
        await _new(store2, real_async_redis, {"user_id": "u2"}, subject="u2")

        listed = await store3.list_for_subject("u1")
        assert len(listed) == 3
        assert listed == await store2.list_for_subject("u1")
        assert await store3.count_for_subject("u1") == 3
        assert await store3.find("total", between=(1, None)) == await store2.find(
            "total", between=(1, None)
        )
        assert await store3.count("device_id", equals="d-1") == 1
        with pytest.raises(ValueError):
            await store3.find("total", between=(1, None), limit=0)
        assert await store3.revoke_all("u1") == 3
        assert await store2.count_for_subject("u1") == 0
        assert await store2.count_for_subject("u2") == 1


# -- custom search -------------------------------------------------------------


class TestFindByTag:
    """Design test 25."""

    async def test_a_string_matches_that_exact_value_only(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(
            real_async_redis, test_prefix, searchable={"device_id": "tag"}
        )
        owner = {
            value: await _new(store, real_async_redis, {"device_id": value})
            for value in AWKWARD
        }
        for part in ("with", "comma", "brace", "pipe", "日本", "UPPER", "*"):
            assert await store.find("device_id", equals=part) == [], part

        for value, sid in owner.items():
            assert _ids(await store.find("device_id", equals=value)) == [sid], value
            assert await store.count("device_id", equals=value) == 1, value

    async def test_a_boolean_matches_that_boolean_only(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """``True`` is stored as ``true``, and the string ``"true"`` as
        ``"true"`` with its quotes.  Neither may match the other."""
        store = await _ready(real_async_redis, test_prefix, searchable={"beta": "tag"})
        yes = await _new(store, real_async_redis, {"beta": True})
        no = await _new(store, real_async_redis, {"beta": False})
        word = await _new(store, real_async_redis, {"beta": "true"})
        await _new(store, real_async_redis, {"other": 1})

        assert _ids(await store.find("beta", equals=True)) == [yes]
        assert _ids(await store.find("beta", equals=False)) == [no]
        assert _ids(await store.find("beta", equals="true")) == [word]
        assert (await store.load(yes)).record.data == {"beta": True}


class TestFindByNumber:
    """Design test 26."""

    async def test_between_returns_the_sessions_in_the_range(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(
            real_async_redis, test_prefix, searchable={"total": "numeric"}
        )
        owner = {
            total: await _new(store, real_async_redis, {"total": total})
            for total in (-3, 5, 50, 50.5, 500)
        }
        await _new(store, real_async_redis, {"other": 50})

        async def totals(between) -> set:
            found = await store.find("total", between=between)
            by_id = {sid: total for total, sid in owner.items()}
            return {by_id[info.session_id] for info in found}

        assert await totals((10, 100)) == {50, 50.5}
        assert await totals((50, 50)) == {50}
        assert await totals((100, None)) == {500}
        assert await totals((None, 5)) == {-3, 5}
        assert await totals((None, None)) == {-3, 5, 50, 50.5, 500}
        assert await totals((1000, 2000)) == set()


NO_NAME = "(a session with no name)"


class TestFindByText:
    """Design tests 27 and 27a.  The text is Redis query syntax."""

    @pytest.fixture()
    async def names(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> tuple[RedisSessionStore, dict[str, str]]:
        store = await _ready(real_async_redis, test_prefix, searchable={"name": "text"})
        owner = {
            name: await _new(store, real_async_redis, {"name": name})
            for name in ("Ana Petrova", "Petar Ivanov", "Jean-Luc Picard")
        }
        # No ``name``. The word is in an attribute that is not declared.
        owner[NO_NAME] = await _new(store, real_async_redis, {"nickname": "petrova"})
        return store, owner

    @pytest.mark.parametrize(
        ("text", "expected"),
        [
            ("petrova", {"Ana Petrova"}),
            ("ana petrova", {"Ana Petrova"}),
            ("pet*", {"Ana Petrova", "Petar Ivanov"}),
            ("petrova|picard", {"Ana Petrova", "Jean-Luc Picard"}),
            ("%petrva%", {"Ana Petrova"}),
            # Each session without the word, one with no name included.
            ("-petrova", {"Petar Ivanov", "Jean-Luc Picard", NO_NAME}),
            ("jean luc", {"Jean-Luc Picard"}),
            ("nobody", set()),
        ],
    )
    async def test_the_text_selects_by_word(
        self, names: tuple[RedisSessionStore, dict[str, str]], text: str, expected: set
    ) -> None:
        store, owner = names
        by_id = {sid: name for name, sid in owner.items()}

        found = await store.find("name", text=text)

        assert {by_id[info.session_id] for info in found} == expected
        assert await store.count("name", text=text) == len(expected)

    @pytest.mark.parametrize("text", ["(", "pet* |", '"open'])
    async def test_a_syntax_error_raises_value_error(
        self, names: tuple[RedisSessionStore, dict[str, str]], text: str
    ) -> None:
        store, _ = names

        with pytest.raises(ValueError):
            await store.find("name", text=text)
        with pytest.raises(ValueError):
            await store.count("name", text=text)


class TestFindByPlace:
    """Design test 28."""

    async def test_near_returns_the_sessions_inside_the_radius(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(
            real_async_redis, test_prefix, searchable={"location": "geo"}
        )
        sofia = await _new(store, real_async_redis, {"location": [23.32, 42.69]})
        plovdiv = await _new(store, real_async_redis, {"location": [24.75, 42.14]})
        await _new(store, real_async_redis, {"other": [23.32, 42.69]})

        assert _ids(await store.find("location", near=(23.32, 42.69, 5000))) == [sofia]
        assert _ids(await store.find("location", near=(24.75, 42.14, 5000))) == [
            plovdiv
        ]
        both = await store.find("location", near=(23.32, 42.69, 200_000))
        assert set(_ids(both)) == {sofia, plovdiv}
        assert await store.count("location", near=(23.32, 42.69, 200_000)) == 2
        assert await store.find("location", near=(0.0, 0.0, 5000)) == []

    async def test_the_attribute_loads_as_a_pair(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(
            real_async_redis, test_prefix, searchable={"location": "geo"}
        )
        sid = await _new(store, real_async_redis, {"location": [23.32, 42.69]})

        key = store.session_key(sid)
        assert await real_async_redis.hget(key, "a:location") == "23.32,42.69"
        assert (await store.load(sid)).record.data == {"location": [23.32, 42.69]}


class TestANestedPath:
    """Design test 29."""

    async def test_two_paths_that_differ_in_one_character_do_not_mix(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """A query escapes each character of the field name.  With an alias
        made of letters only, ``cart.total`` and ``cart_total`` would give
        one name."""
        store = await _ready(
            real_async_redis,
            test_prefix,
            searchable={"cart.total": "numeric", "cart_total": "numeric"},
        )
        nested = await _new(
            store, real_async_redis, {"cart": {"total": 10}, "cart_total": 999}
        )
        flat = await _new(
            store, real_async_redis, {"cart": {"total": 999}, "cart_total": 10}
        )

        assert _ids(await store.find("cart.total", between=(0, 100))) == [nested]
        assert _ids(await store.find("cart_total", between=(0, 100))) == [flat]
        assert await store.count("cart.total", between=(0, 100)) == 1

    async def test_a_deep_path_is_searchable(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(
            real_async_redis, test_prefix, searchable={"user.address.city": "tag"}
        )
        sofia = await _new(
            store, real_async_redis, {"user": {"address": {"city": "Sofia"}}}
        )
        await _new(store, real_async_redis, {"user": {"address": {"city": "Varna"}}})

        assert _ids(await store.find("user.address.city", equals="Sofia")) == [sofia]


class TestASessionTheCustomIndexCannotHold:
    """Design tests 30 and 30a.

    Each index judges a hash on its own schema.  So what the application
    writes can hide a session from ``find``, and never from the methods that
    end sessions.
    """

    SEARCHABLE = {"device_id": "tag", "location": "geo", "total": "numeric"}

    async def test_a_session_without_the_attribute_is_found_by_subject(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(real_async_redis, test_prefix, searchable=self.SEARCHABLE)
        sid = await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")

        assert _ids(await store.list_for_subject("u1")) == [sid]
        assert await store.count_for_subject("u1") == 1
        assert await store.find("device_id", equals="d-1") == []

    @pytest.mark.parametrize(
        "data",
        [{"location": [10, 89]}, {"total": float("nan")}],
        ids=["geo-out-of-range", "numeric-nan"],
    )
    async def test_a_value_the_engine_cannot_index_hides_it_from_find_only(
        self, real_async_redis: async_redis.Redis, test_prefix: str, data: dict
    ) -> None:
        store = await _ready(real_async_redis, test_prefix, searchable=self.SEARCHABLE)
        sid = await _new(
            store,
            real_async_redis,
            {"user_id": "u1", "device_id": "d-1", **data},
            subject="u1",
        )
        good = await _new(
            store, real_async_redis, {"user_id": "u2", "device_id": "d-1"}, subject="u2"
        )

        assert _ids(await store.find("device_id", equals="d-1")) == [good]
        assert _ids(await store.list_for_subject("u1")) == [sid]
        assert await store.count_for_subject("u1") == 1
        assert await store.revoke_all("u1") == 1
        assert await store.load(sid) is None
        assert await store.load(good) is not None


class TestCountAndPages:
    """Design test 31."""

    async def test_count_matches_find_and_pages_have_no_repeat_and_no_gap(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(real_async_redis, test_prefix, searchable={"plan": "tag"})
        now = time.time()
        # A different last access for each, so the order of the pages is exact.
        pro = [
            await _new(store, real_async_redis, {"plan": "pro"}, seen=now - n)
            for n in range(25)
        ]
        for n in range(4):
            await _new(store, real_async_redis, {"plan": "free"}, seen=now - n)

        assert await store.count("plan", equals="pro") == 25
        assert len(await store.find("plan", equals="pro")) == 25
        assert await store.count("plan", equals="free") == 4
        assert await store.count("plan", equals="none") == 0

        pages = [
            _ids(await store.find("plan", equals="pro", limit=10, offset=offset))
            for offset in (0, 10, 20, 30)
        ]
        assert [len(page) for page in pages] == [10, 10, 5, 0]
        assert [sid for page in pages for sid in page] == pro

    async def test_a_session_past_its_deadline_is_not_found(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """Design test 20, for the custom index."""
        store = await _ready(real_async_redis, test_prefix, searchable={"plan": "tag"})
        live = await _new(store, real_async_redis, {"plan": "pro"})
        dead = await _new(store, real_async_redis, {"plan": "pro"})

        await aexpire_now(real_async_redis, store.session_key(dead), FIELD_CREATED)

        assert await real_async_redis.exists(store.session_key(dead)) == 1
        assert _ids(await store.find("plan", equals="pro")) == [live]
        assert await store.count("plan", equals="pro") == 1

    async def test_a_save_changes_what_find_returns(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = await _ready(real_async_redis, test_prefix, searchable={"plan": "tag"})
        sid = store.new_id()
        fields = await store.create(sid, store.new_record({"plan": "free"}))

        fields = await store.save(
            sid, store.new_record({"plan": "pro"}), baseline=fields
        )
        assert _ids(await store.find("plan", equals="pro")) == [sid]
        assert await store.find("plan", equals="free") == []

        await store.save(sid, store.new_record({}), baseline=fields)
        assert await store.find("plan", equals="pro") == []


# -- the indexes ---------------------------------------------------------------


def _fresh(redis, prefix: str, searchable: dict | None = None) -> RedisSessionStore:
    """A store as a new process builds it: nothing known about the server."""
    return _store(
        redis, prefix, searchable=searchable, capabilities=_StoreCapabilities()
    )


class TestTheIndexLifecycle:
    """Design tests 37 and 37a."""

    async def test_each_index_is_created_when_it_is_missing(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        assert await _indexes(real_async_redis, test_prefix) == set()

        await _fresh(real_async_redis, test_prefix, {"plan": "tag"}).ensure_indexes()

        names = await _indexes(real_async_redis, test_prefix)
        assert f"{test_prefix}:si" in names
        custom = names - {f"{test_prefix}:si"}
        assert len(custom) == 1
        assert re.fullmatch(rf"{test_prefix}:sa:[0-9a-f]{{8}}", custom.pop())

    async def test_the_first_search_creates_a_missing_index(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """An app whose Redis was down at startup still gets its indexes."""
        store = _fresh(real_async_redis, test_prefix, {"plan": "tag"})
        assert await _indexes(real_async_redis, test_prefix) == set()

        assert await store.count_for_subject("u1") == 0
        assert await store.find("plan", equals="pro") == []

        assert len(await _indexes(real_async_redis, test_prefix)) == 2

    async def test_a_search_creates_again_an_index_that_went_away(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """A failover to an empty node loses the index and keeps the process."""
        store = _fresh(real_async_redis, test_prefix)
        sid = await _new(store, real_async_redis, {"user_id": "u1"}, subject="u1")
        assert await store.count_for_subject("u1") == 1
        await real_async_redis.ft(f"{test_prefix}:si").dropindex()

        assert await store.count_for_subject("u1") in (0, 1)

        await _indexing_ended(real_async_redis, f"{test_prefix}:si")
        assert _ids(await store.list_for_subject("u1")) == [sid]

    async def test_a_second_start_with_the_same_declaration_changes_nothing(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        declaration = {"plan": "tag", "total": "numeric"}
        first = _fresh(real_async_redis, test_prefix, declaration)
        await first.ensure_indexes()
        sid = await _new(
            first, real_async_redis, {"plan": "pro", "total": 5}, subject="u1"
        )
        names = await _indexes(real_async_redis, test_prefix)
        dropped = await _calls(real_async_redis, "FT.DROPINDEX")

        # The same declaration in another order: one name for both stores.
        second = _fresh(
            real_async_redis, test_prefix, {"total": "numeric", **declaration}
        )
        await second.ensure_indexes()
        await second.ensure_indexes()

        assert await _indexes(real_async_redis, test_prefix) == names
        assert await _calls(real_async_redis, "FT.DROPINDEX") == dropped
        # No wait for indexing: an index that was created again could miss it.
        assert _ids(await second.find("plan", equals="pro")) == [sid]
        assert _ids(await second.list_for_subject("u1")) == [sid]

    async def test_with_no_searchable_there_is_no_custom_index(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        store = _fresh(real_async_redis, test_prefix)

        await store.ensure_indexes()

        assert await _indexes(real_async_redis, test_prefix) == {f"{test_prefix}:si"}
        with pytest.raises(ValueError):
            await store.find("plan", equals="pro")

    async def test_removing_the_declaration_drops_the_custom_index(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        await _fresh(real_async_redis, test_prefix, {"plan": "tag"}).ensure_indexes()
        assert len(await _indexes(real_async_redis, test_prefix)) == 2

        await _fresh(real_async_redis, test_prefix).ensure_indexes()

        assert await _indexes(real_async_redis, test_prefix) == {f"{test_prefix}:si"}

    async def test_a_changed_declaration_creates_a_new_index_and_drops_the_old_one(
        self,
        real_async_redis: async_redis.Redis,
        test_prefix: str,
        caplog: pytest.LogCaptureFixture,
    ) -> None:
        before = _fresh(real_async_redis, test_prefix, {"device_id": "tag"})
        await before.ensure_indexes()
        (old,) = await _indexes(real_async_redis, test_prefix) - {f"{test_prefix}:si"}
        written = [
            await _new(
                before,
                real_async_redis,
                {"device_id": f"d-{n}", "plan": "pro" if n % 2 else "free"},
                subject="u1",
            )
            for n in range(20)
        ]
        dropped = await _calls(real_async_redis, "FT.DROPINDEX")

        after = _fresh(real_async_redis, test_prefix, {"plan": "tag"})
        with caplog.at_level(logging.WARNING, logger="redis_fastapi.session_backend"):
            await after.ensure_indexes()

        names = await _indexes(real_async_redis, test_prefix)
        assert old not in names
        (new,) = names - {f"{test_prefix}:si"}
        assert new != old
        assert [r for r in caplog.records if old in r.getMessage()], (
            "the drop of the old index was not logged"
        )
        # One drop, and the old custom index is the one that went: so the
        # built-in index was not dropped.
        assert await _calls(real_async_redis, "FT.DROPINDEX") == dropped + 1
        # The drop has no DD. The sessions are still there.
        assert await real_async_redis.dbsize() == 20

        await _indexing_ended(real_async_redis, new)
        found = await after.find("plan", equals="pro")
        assert set(_ids(found)) == {sid for n, sid in enumerate(written) if n % 2}
        with pytest.raises(ValueError):
            await after.find("device_id", equals="d-1")

    async def test_a_changed_declaration_keeps_the_built_in_index(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """Design test 37a.

        ``revoke_all`` runs with no wait.  An index that was dropped and
        created again fills in the background, so it would miss sessions.
        """
        before = _fresh(real_async_redis, test_prefix, {"device_id": "tag"})
        await before.ensure_indexes()
        await _new_many(before, real_async_redis, 500, subject="u1", base=1000.0)
        other = await _new(before, real_async_redis, {"user_id": "u2"}, subject="u2")

        after = _fresh(real_async_redis, test_prefix, {"plan": "tag"})
        await after.ensure_indexes()

        assert await after.revoke_all("u1") == 500
        assert await real_async_redis.dbsize() == 1
        assert await after.load(other) is not None

    async def test_two_ensure_steps_at_once_both_succeed(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """Design test 37c: each worker of a deployment starts at once.

        Each round has a race for each step: the built-in index is missing,
        the custom index is missing, and an older custom index is there to
        drop.
        """
        builtin = f"{test_prefix}:si"
        for round_ in range(20):
            stale = _fresh(real_async_redis, test_prefix, {f"old{round_}": "tag"})
            await stale.ensure_indexes()
            await real_async_redis.ft(builtin).dropindex()
            assert len(await _indexes(real_async_redis, test_prefix)) == 1

            declaration = {f"new{round_}": "tag"}
            first = _fresh(real_async_redis, test_prefix, declaration)
            second = _fresh(real_async_redis, test_prefix, declaration)
            await asyncio.gather(first.ensure_indexes(), second.ensure_indexes())

            names = await _indexes(real_async_redis, test_prefix)
            assert builtin in names, round_
            assert len(names) == 2, (round_, names)
            assert await first.count(f"new{round_}", equals="x") == 0
            assert await second.count_for_subject("u1") == 0


@pytest.mark.slow
@pytest.mark.timeout(180)
class TestARealTimeout:
    """Design test 36a."""

    SESSIONS = 50_000

    async def test_a_list_that_times_out_raises(
        self, real_async_redis: async_redis.Redis, test_prefix: str
    ) -> None:
        """A sorted list of 50,000 matches cannot end in one millisecond.

        The test uses a list, because a count does not time out.  With the
        policy ``fail`` the server answers with an error, and the store must
        raise its own.  A partial list would read as "these are your
        devices".
        """
        store = _fresh(real_async_redis, test_prefix)
        await store.ensure_indexes()
        now = time.time()
        for start in range(0, self.SESSIONS, 5000):
            pipe = real_async_redis.pipeline(transaction=False)
            for n in range(start, start + 5000):
                key = store.session_key(store.new_id())
                pipe.hset(
                    key,
                    mapping={
                        FIELD_SUBJECT: '"u1"',
                        FIELD_CREATED: repr(now),
                        FIELD_SEEN: repr(now - n),
                    },
                )
                pipe.hexpire(key, ABSOLUTE, FIELD_CREATED)
                pipe.expire(key, IDLE)
            await pipe.execute()

        saved = {
            name: (await real_async_redis.config_get(name))[name]
            for name in ("search-on-timeout", "search-timeout")
        }
        try:
            await real_async_redis.config_set("search-on-timeout", "fail")
            assert await store.count_for_subject("u1") == self.SESSIONS
            await real_async_redis.config_set("search-timeout", 1)

            with pytest.raises(SessionSearchTimeoutError):
                await store.list_for_subject("u1")
        finally:
            for name, value in saved.items():
                await real_async_redis.config_set(name, value)
