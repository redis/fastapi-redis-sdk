"""Unit tests for :class:`RedisSessionStore`, against ``fakeredis``.

These run the real key schema, the real TTL commands and the real two-field
layout - not a substitute - which is the whole reason the design refuses an
in-memory store.
"""

from __future__ import annotations

import pytest

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    SessionConfigurationError,
    SessionStoreError,
)
from redis_fastapi.session_backend import (
    FIELD_ABSOLUTE,
    FIELD_DATA,
    RedisSessionStore,
    SessionMetadata,
    SessionRecord,
    _StoreCapabilities,
)
from tests.conftest import about


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)


async def _httl(redis, key: str, field: str) -> int:
    reply = await redis.execute_command("HTTL", key, "FIELDS", 1, field)
    return int(reply[0])


class TestKeySchema:
    def test_the_two_prefixes_cannot_collide(self, store: RedisSessionStore) -> None:
        """Structural, not conventional.

        The strings diverge before either key's variable part begins, so no
        session ID can produce the index key - not even the ``by-subject:42``
        that broke the earlier nested layout.
        """
        assert store.session_key("by-subject:42") != store.index_key("42")
        assert not store.session_key("x").startswith(store._index_prefix)
        assert not store.index_key("x").startswith(store._session_prefix)

    def test_keys_are_flat_with_no_hash_tag(self, store: RedisSessionStore) -> None:
        # A hash tag would send every session of one deployment to one slot.
        assert "{" not in store.session_key("abc")
        assert "}" not in store.index_key("42")


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


class TestTwoFieldsTwoClocks:
    async def test_save_writes_both_fields_with_their_own_ttls(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))

        key = store.session_key(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 600)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 60)

    async def test_writing_the_payload_never_extends_the_deadline(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """N-6, asserted directly rather than inferred.

        This is the guarantee the whole two-field layout exists for, so it is
        checked by watching field ``a``'s TTL rather than by reasoning about
        which command was sent.
        """
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"n": 1}))

        # Age the absolute clock, then write the payload many times over.
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_ABSOLUTE
        )
        for n in range(5):
            await store.save(sid, store.new_record({"n": n}))

        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 100), (
            "field 'a' moved on a payload write - refreshed, shortened or "
            "deleted; the absolute deadline is no longer absolute"
        )
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 60), (
            "field 'd' should have been refreshed by the write"
        )

    async def test_field_a_always_has_a_ttl(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The ``-1`` row of the state table must be unreachable."""
        sid = store.new_id()
        await store.create(sid, store.new_record({}))
        assert await _httl(fake_async_redis, store.session_key(sid), FIELD_ABSOLUTE) > 0

    async def test_zero_ttls_fall_back_to_gc_ttl(self, fake_async_redis) -> None:
        """Cookie-only mode: both fields still expire eventually."""
        store = RedisSessionStore(
            fake_async_redis, idle_ttl=0, absolute_ttl=0, gc_ttl=1234
        )
        sid = store.new_id()
        await store.create(sid, store.new_record({}))
        key = store.session_key(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 1234)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 1234)


class TestLoadStateTable:
    """All five rows of the table in Section 4.1."""

    async def test_alive(self, store: RedisSessionStore) -> None:
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}
        assert 0 < loaded.absolute_remaining <= 600

    async def test_absolute_deadline_passed_deletes_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.execute_command(
            "HDEL", key, FIELD_ABSOLUTE
        )  # simulate 'a' expiring
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0, (
            "a half-dead key must be removed so the index entry can follow"
        )

    async def test_no_such_session(self, store: RedisSessionStore) -> None:
        assert await store.load(store.new_id()) is None

    async def test_idle_expired_deletes_the_key(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.execute_command("HDEL", key, FIELD_DATA)
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_an_absolute_limit_of_zero_still_loads(
        self, fake_async_redis
    ) -> None:
        """The bug that made every such session dead on arrival.

        An earlier design read ``HTTL`` returning ``-2`` as "the absolute
        deadline passed" and wrote no field ``a`` at all when the limit was
        disabled, so every session in such a deployment was unreadable the
        moment it was created.
        """
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=0)
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 7}))
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 7}

    async def test_an_invalid_id_never_reaches_redis(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.load("not a valid id") is None


class TestIdleRefresh:
    async def test_load_restarts_the_idle_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 5, "FIELDS", 1, FIELD_DATA
        )
        await store.load(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 60)

    async def test_refresh_false_leaves_the_idle_clock_alone(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 5, "FIELDS", 1, FIELD_DATA
        )
        await store.load(sid, refresh=False)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 5), (
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
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 5, "FIELDS", 1, FIELD_DATA
        )
        await store.touch(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 60)

    async def test_touch_does_not_extend_the_absolute_clock(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_ABSOLUTE
        )
        await store.touch(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 100), (
            "touch moved the absolute clock"
        )


class TestSevenFourFallback:
    """The 7.4 path writes the same fields with the same expirations.

    Section 13.1: the fallback costs an extra command and never changes
    behaviour, so every assertion above must hold here too.
    """

    @pytest.fixture()
    def old_store(self, fake_async_redis) -> RedisSessionStore:
        caps = _StoreCapabilities(supports_hsetex=False)
        return RedisSessionStore(
            fake_async_redis, capabilities=caps, idle_ttl=60, absolute_ttl=600
        )

    async def test_round_trip(
        self, old_store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = old_store.new_id()
        await old_store.create(sid, old_store.new_record({"user_id": 42}))
        loaded = await old_store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}

    async def test_same_ttls_as_the_modern_path(
        self, old_store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = old_store.new_id()
        await old_store.create(sid, old_store.new_record({}))
        key = old_store.session_key(sid)
        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 600)
        assert about(await _httl(fake_async_redis, key, FIELD_DATA), 60)

    async def test_repeated_writes_still_never_extend_the_deadline(
        self, old_store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = old_store.new_id()
        key = old_store.session_key(sid)
        await old_store.create(sid, old_store.new_record({}))
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 100, "FIELDS", 1, FIELD_ABSOLUTE
        )
        await old_store.save(sid, old_store.new_record({"n": 2}))
        assert about(await _httl(fake_async_redis, key, FIELD_ABSOLUTE), 100), (
            "the 7.4 write path moved the absolute clock"
        )


class TestEnvelope:
    def test_metadata_never_reaches_the_payload(self, store: RedisSessionStore) -> None:
        """``starsessions`` puts it inside the session. We do not."""
        record = store.new_record({"user_id": 42})
        decoded = store.decode(store.encode(record))
        assert decoded.data == {"user_id": 42}
        assert "__metadata__" not in decoded.data
        assert "m" not in decoded.data

    def test_round_trip_preserves_metadata(self, store: RedisSessionStore) -> None:
        record = SessionRecord(
            data={"a": 1},
            metadata=SessionMetadata(created=1.5, last_access=2.5, lifetime=600),
        )
        decoded = store.decode(store.encode(record))
        assert decoded.metadata == record.metadata

    def test_a_corrupt_record_raises_rather_than_looking_empty(
        self, store: RedisSessionStore
    ) -> None:
        with pytest.raises(SessionStoreError, match="Unreadable session record"):
            store.decode("{not json")


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
