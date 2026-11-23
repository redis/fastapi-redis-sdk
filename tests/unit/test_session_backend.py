"""Unit tests for :class:`RedisSessionStore`, against ``fakeredis``.

These run the real key schema, the real TTL commands and the real two-clock
layout - not a substitute - which is the whole reason the design refuses an
in-memory store.
"""

from __future__ import annotations

import pytest

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import SessionConfigurationError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    _StoreCapabilities,
)
from redis_fastapi.session_layout import FIELD_CREATED, FIELD_SEEN
from tests.conftest import about, aexpire_now

# What the server can do decides one thing only: whether a load writes
# ``s:seen``.  Each assertion in this file must hold in all three cases.
CAPABILITIES = {
    "search+hsetex": {"supports_search": True, "supports_hsetex": True},
    "no-search": {"supports_search": False, "supports_hsetex": True},
    "no-hsetex": {"supports_search": True, "supports_hsetex": False},
}


@pytest.fixture(params=list(CAPABILITIES), ids=list(CAPABILITIES))
def store(request, fake_async_redis) -> RedisSessionStore:
    """Every store test runs on each kind of server."""
    get_settings.cache_clear()
    caps = _StoreCapabilities(**CAPABILITIES[request.param])
    return RedisSessionStore(
        fake_async_redis, capabilities=caps, idle_ttl=60, absolute_ttl=600
    )


async def _httl(redis, key: str, field: str) -> int:
    reply = await redis.execute_command("HTTL", key, "FIELDS", 1, field)
    return int(reply[0])


async def _deadline(redis, key: str) -> int:
    """The absolute clock: the TTL of ``s:created``."""
    return await _httl(redis, key, FIELD_CREATED)


class TestKeySchema:
    def test_a_session_key_is_the_prefix_the_segment_and_the_id(
        self, store: RedisSessionStore
    ) -> None:
        assert store.session_key("abc") == "rf:s:abc"

    def test_no_index_name_is_under_the_session_prefix(self, fake_async_redis) -> None:
        """An index covers each hash under ``rf:s:``.

        An index name is not a key, but a name under that prefix would read
        as a session to anyone who scans the keyspace.
        """
        store = RedisSessionStore(fake_async_redis, searchable={"role": "tag"})
        assert store._builtin_index == "rf:si"
        assert store._custom_index is not None
        assert store._custom_index.startswith("rf:sa:")
        for name in (store._builtin_index, store._custom_index):
            assert not name.startswith(store._session_prefix)

    def test_the_prefix_follows_key_prefix(self, fake_async_redis) -> None:
        store = RedisSessionStore(
            fake_async_redis, key_prefix="app", searchable={"role": "tag"}
        )
        assert store.session_key("abc") == "app:s:abc"
        assert store._builtin_index == "app:si"
        assert store._custom_index is not None
        assert store._custom_index.startswith("app:sa:")

    def test_keys_are_flat_with_no_hash_tag(self, store: RedisSessionStore) -> None:
        # A hash tag would send every session of one deployment to one slot.
        assert "{" not in store.session_key("abc")
        assert "}" not in store.session_key("abc")

    async def test_a_session_is_one_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """No second key for the subject: the ``rf:si:<subject>`` hash is gone."""
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}), subject="42")
        assert await fake_async_redis.keys("*") == [store.session_key(sid).encode()]


class TestIdentifiers:
    def test_generated_ids_are_valid_and_unique(self, store: RedisSessionStore) -> None:
        ids = {store.new_id() for _ in range(50)}
        assert len(ids) == 50
        assert all(store.is_valid_id(i) for i in ids)

    @pytest.mark.parametrize(
        "bad",
        ["short", "by-subject:42", "has space", "has/slash", "", "a" * 21],
    )
    def test_invalid_ids_are_rejected(self, store: RedisSessionStore, bad: str) -> None:
        assert store.is_valid_id(bad) is False

    def test_a_bad_id_factory_raises_rather_than_writing(
        self, fake_async_redis
    ) -> None:
        store = RedisSessionStore(fake_async_redis, id_factory=lambda: "by-subject:42")
        with pytest.raises(SessionConfigurationError, match="unusable session ID"):
            store.new_id()

    def test_a_good_id_factory_is_accepted(self, fake_async_redis) -> None:
        store = RedisSessionStore(fake_async_redis, id_factory=lambda: "a" * 30)
        assert store.new_id() == "a" * 30


class TestTwoClocks:
    async def test_create_sets_the_idle_clock_and_the_deadline(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))

        key = store.session_key(sid)
        assert about(await fake_async_redis.ttl(key), 60)
        assert about(await _deadline(fake_async_redis, key), 600)
        assert await _httl(fake_async_redis, key, FIELD_SEEN) == -1, (
            "no system field other than s:created has a TTL"
        )
        assert await _httl(fake_async_redis, key, "a:user_id") == -1

    async def test_a_save_never_moves_the_deadline(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """N-6, asserted directly rather than inferred.

        This is the guarantee the two-clock layout exists for, so it is checked
        by watching the TTL of ``s:created`` rather than by reasoning about
        which command was sent.
        """
        sid = store.new_id()
        key = store.session_key(sid)
        fields = await store.create(sid, store.new_record({"n": -1}))

        # Age the absolute clock, then write the session many times over.
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_CREATED
        )
        await fake_async_redis.expire(key, 5)
        for n in range(5):
            fields = await store.save(sid, store.new_record({"n": n}), baseline=fields)

        assert about(await _deadline(fake_async_redis, key), 100), (
            "the TTL of s:created moved on a save - refreshed, shortened or "
            "removed; the absolute deadline is no longer absolute"
        )
        assert about(await fake_async_redis.ttl(key), 60), (
            "the save should have restarted the idle clock"
        )

    async def test_a_save_with_no_baseline_does_not_write_the_deadline(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """With no baseline the save writes each field, and still not this one."""
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"n": 1}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_CREATED
        )

        after = await store.save(sid, store.new_record({"n": 2}))

        assert FIELD_CREATED not in after
        assert about(await _deadline(fake_async_redis, key), 100)

    async def test_zero_ttls_fall_back_to_gc_ttl(self, fake_async_redis) -> None:
        """Cookie-only mode: both clocks still expire eventually."""
        store = RedisSessionStore(
            fake_async_redis, idle_ttl=0, absolute_ttl=0, gc_ttl=1234
        )
        sid = store.new_id()
        await store.create(sid, store.new_record({}))
        key = store.session_key(sid)
        assert about(await _deadline(fake_async_redis, key), 1234)
        assert about(await fake_async_redis.ttl(key), 1234)

    async def test_zero_idle_ttl_disables_the_idle_clock(
        self, fake_async_redis
    ) -> None:
        """idle_ttl=0 gives the key the absolute length, not gc_ttl.

        With gc_ttl shorter than absolute_ttl, a gc_ttl on the key would end a
        session the idle clock was told to leave alone.
        """
        store = RedisSessionStore(
            fake_async_redis, idle_ttl=0, absolute_ttl=5000, gc_ttl=1234
        )
        sid = store.new_id()
        await store.create(sid, store.new_record({}))
        key = store.session_key(sid)
        assert about(await _deadline(fake_async_redis, key), 5000)
        assert about(await fake_async_redis.ttl(key), 5000)

        assert await store.load(sid) is not None
        assert about(await fake_async_redis.ttl(key), 5000), (
            "the load's idle refresh fell back to gc_ttl"
        )


class TestLoadStateTable:
    """Every state a load can find."""

    async def test_alive(self, store: RedisSessionStore) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}
        assert about(loaded.absolute_remaining, 600)

    async def test_the_load_returns_the_stored_fields(
        self, store: RedisSessionStore
    ) -> None:
        """The next save compares the session with these."""
        sid = store.new_id()
        created = await store.create(
            sid, store.new_record({"user_id": 42}), subject="42"
        )
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.fields == created
        assert loaded.fields["a:user_id"] == "42"
        assert loaded.fields["s:sub"] == '"42"'

    async def test_the_absolute_deadline_ends_a_fresh_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``s:created`` expires, however fresh the idle clock is."""
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await aexpire_now(fake_async_redis, key, FIELD_CREATED)
        assert await fake_async_redis.exists(key) == 1, "the key outlives the field"

        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_a_deadline_with_no_ttl_is_deleted(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """``s:created`` with no TTL is not a deadline.

        A create always sets the TTL, so something else wrote this field.  A
        session that could never end is not one the store will serve.
        """
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.execute_command(
            "HPERSIST", key, "FIELDS", 1, FIELD_CREATED
        )
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_no_such_session(self, store: RedisSessionStore) -> None:
        assert await store.load(store.new_id()) is None

    async def test_idle_expired_is_no_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        assert about(await fake_async_redis.ttl(key), 60)
        await aexpire_now(fake_async_redis, key)
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_an_absolute_limit_of_zero_still_loads(
        self, fake_async_redis
    ) -> None:
        """The bug that made every such session dead on arrival.

        An earlier design wrote no deadline at all when the limit was
        disabled, and read the missing deadline as "passed", so every session
        in such a deployment was unreadable the moment it was created.
        """
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=0)
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 7}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 7}

    async def test_an_invalid_id_never_reaches_redis(
        self, store: RedisSessionStore, monkeypatch
    ) -> None:
        async def _unreachable(*args, **kwargs):
            raise AssertionError("an invalid ID reached Redis")

        monkeypatch.setattr(store, "_read", _unreachable)
        assert await store.load("not a valid id") is None


class TestIdleRefresh:
    async def test_load_restarts_the_idle_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.expire(key, 5)
        await store.load(sid)
        assert about(await fake_async_redis.ttl(key), 60)

    async def test_refresh_false_leaves_the_idle_clock_alone(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.expire(key, 5)
        await store.load(sid, refresh=False)
        assert about(await fake_async_redis.ttl(key), 5), (
            "a plain read moved the idle clock"
        )

    async def test_touch_advances_the_idle_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Under ``refresh_on_load=False`` this is the only thing that does.

        Omitting this branch froze the idle clock and made every session
        immortal until its absolute deadline.
        """
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.expire(key, 5)
        await store.touch(sid)
        assert about(await fake_async_redis.ttl(key), 60)

    async def test_no_read_extends_the_absolute_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_CREATED
        )
        await store.touch(sid)
        await store.load(sid)
        assert about(await _deadline(fake_async_redis, key), 100), (
            "a touch or a load moved the absolute clock"
        )

    async def test_touch_of_a_missing_session_creates_no_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        await store.touch(store.new_id())
        assert await fake_async_redis.keys("*") == []


class TestMetadata:
    async def test_metadata_never_reaches_the_payload(
        self, store: RedisSessionStore
    ) -> None:
        """Some libraries put it inside the session. We do not."""
        sid = store.new_id()
        await store.create(
            sid, store.new_record({"user_id": 42}), subject="42", descriptor={"a": 1}
        )
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}

    async def test_round_trip_preserves_the_creation_time(
        self, store: RedisSessionStore
    ) -> None:
        sid = store.new_id()
        record = store.new_record({"a": 1}, created=1.5)
        await store.create(sid, record)
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.metadata.created == 1.5


class TestDelete:
    async def test_delete_removes_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({}))
        await store.delete(sid)
        assert await fake_async_redis.exists(store.session_key(sid)) == 0
        assert await store.load(sid) is None


class TestConfigurationIsChecked:
    @pytest.mark.parametrize("kwargs", [{"idle_ttl": -1}, {"absolute_ttl": -5}])
    def test_negative_ttls_are_refused(self, fake_async_redis, kwargs) -> None:
        with pytest.raises(SessionConfigurationError, match="must not be negative"):
            RedisSessionStore(fake_async_redis, **kwargs)

    def test_gc_ttl_must_be_positive(self, fake_async_redis) -> None:
        with pytest.raises(SessionConfigurationError, match="must be positive"):
            RedisSessionStore(fake_async_redis, gc_ttl=0)

    def test_the_coder_is_accepted_and_not_used(self, fake_async_redis) -> None:
        """A caller written for the earlier store still builds one."""

        class Refuses:
            @classmethod
            def encode(cls, value):
                raise AssertionError("the coder encoded a session")

            @classmethod
            def decode(cls, value):
                raise AssertionError("the coder decoded a session")

        RedisSessionStore(fake_async_redis, coder=Refuses)
