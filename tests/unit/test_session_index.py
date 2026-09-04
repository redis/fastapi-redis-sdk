"""Tests for the subject index: listing, counting and bulk revocation.

The index answers "which sessions has this user got?", which the cookie
cannot.  Its defining property is that it is an **upper bound**: an entry can
outlive the session it names, so every read verifies before reporting.
"""

from __future__ import annotations

import pytest

from redis_fastapi.config import get_settings
from redis_fastapi.session_backend import (
    FIELD_ABSOLUTE,
    FIELD_DATA,
    RedisSessionStore,
)


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)


async def _make(store: RedisSessionStore, subject: str, **data) -> str:
    """Create a session and index it under *subject*."""
    sid = store.new_id()
    record = store.new_record({"user_id": subject, **data})
    await store.create(sid, record)
    await store.index(subject, sid, record, absolute_remaining=600)
    return sid


class TestListing:
    async def test_lists_every_live_session(self, store: RedisSessionStore) -> None:
        first = await _make(store, "42")
        second = await _make(store, "42")
        listed = {info.session_id for info in await store.list_for_subject("42")}
        assert listed == {first, second}

    async def test_a_subject_sees_only_their_own(
        self, store: RedisSessionStore
    ) -> None:
        mine = await _make(store, "42")
        await _make(store, "99")
        listed = {info.session_id for info in await store.list_for_subject("42")}
        assert listed == {mine}

    async def test_an_unknown_subject_lists_nothing(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.list_for_subject("nobody") == []

    async def test_a_listing_carries_timestamps_without_reading_the_record(
        self, store: RedisSessionStore
    ) -> None:
        await _make(store, "42")
        info = (await store.list_for_subject("42"))[0]
        assert info.created > 0
        assert info.last_access > 0


class TestTheIndexIsAnUpperBound:
    async def test_an_idle_dead_session_is_not_reported(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The failure the index exists to avoid.

        Most sessions die of idleness long before their absolute deadline, and
        the index entry's TTL *is* the absolute deadline - so the entry
        routinely outlives the session.  Reporting it would show a user a
        device they are not signed in on, with a sign-out button that does
        nothing.
        """
        live = await _make(store, "42")
        idle_dead = await _make(store, "42")
        await fake_async_redis.execute_command(
            "HDEL", store.session_key(idle_dead), FIELD_DATA
        )

        listed = {info.session_id for info in await store.list_for_subject("42")}
        assert listed == {live}

    async def test_a_session_past_its_absolute_deadline_is_not_reported(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Why liveness checks both clocks, not just the idle one."""
        live = await _make(store, "42")
        absolute_dead = await _make(store, "42")
        await fake_async_redis.execute_command(
            "HDEL", store.session_key(absolute_dead), FIELD_ABSOLUTE
        )

        listed = {info.session_id for info in await store.list_for_subject("42")}
        assert listed == {live}

    async def test_reading_prunes_the_dead_entry(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        dead = await _make(store, "42")
        await fake_async_redis.execute_command(
            "HDEL", store.session_key(dead), FIELD_DATA
        )
        await store.list_for_subject("42")
        remaining = await fake_async_redis.hgetall(store.index_key("42"))
        assert dead.encode() not in remaining


class TestCounting:
    async def test_count_is_o1_and_unverified_by_default(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        await _make(store, "42")
        dead = await _make(store, "42")
        await fake_async_redis.execute_command(
            "HDEL", store.session_key(dead), FIELD_DATA
        )
        # The fast answer counts the entry that is about to be pruned.
        assert await store.count_for_subject("42") == 2

    async def test_a_limit_makes_the_count_exact_when_it_matters(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The verification round trip is paid only by the request refused."""
        await _make(store, "42")
        dead = await _make(store, "42")
        await fake_async_redis.execute_command(
            "HDEL", store.session_key(dead), FIELD_DATA
        )
        assert await store.count_for_subject("42", limit=2) == 1

    async def test_below_the_limit_takes_the_fast_path(
        self, store: RedisSessionStore
    ) -> None:
        await _make(store, "42")
        assert await store.count_for_subject("42", limit=5) == 1


class TestRevokeById:
    async def test_revokes_a_session_of_the_right_subject(
        self, store: RedisSessionStore
    ) -> None:
        sid = await _make(store, "42")
        assert await store.revoke_id(sid, subject="42") is True
        assert await store.load(sid) is None

    async def test_refuses_a_session_belonging_to_someone_else(
        self, store: RedisSessionStore
    ) -> None:
        """Without the subject check this is a cross-user revocation primitive.

        Any handler taking an ID from a request could end a stranger's
        session, so the subject is required and verified against the index.
        """
        theirs = await _make(store, "99")
        assert await store.revoke_id(theirs, subject="42") is False
        assert await store.load(theirs) is not None, "a stranger's session was ended"

    async def test_an_unknown_id_is_refused_rather_than_erroring(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.revoke_id(store.new_id(), subject="42") is False

    async def test_a_malformed_id_never_reaches_redis(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.revoke_id("not valid", subject="42") is False


class TestRevokeAll:
    async def test_ends_every_session_of_a_subject(
        self, store: RedisSessionStore
    ) -> None:
        first = await _make(store, "42")
        second = await _make(store, "42")
        assert await store.revoke_all("42") == 2
        assert await store.load(first) is None
        assert await store.load(second) is None

    async def test_leaves_other_subjects_alone(self, store: RedisSessionStore) -> None:
        mine = await _make(store, "42")
        theirs = await _make(store, "99")
        await store.revoke_all("42")
        assert await store.load(mine) is None
        assert await store.load(theirs) is not None

    async def test_clears_the_index_too(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        await _make(store, "42")
        await store.revoke_all("42")
        assert await store.list_for_subject("42") == []

    async def test_an_unknown_subject_revokes_nothing(
        self, store: RedisSessionStore
    ) -> None:
        assert await store.revoke_all("nobody") == 0


class TestIndexEntryLifetime:
    async def test_the_entry_expires_with_the_session(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = await _make(store, "42")
        reply = await fake_async_redis.execute_command(
            "HTTL", store.index_key("42"), "FIELDS", 1, sid
        )
        assert 0 < int(reply[0]) <= 600

    async def test_re_assertion_never_extends_the_entry(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """The bug no end-state assertion would catch.

        Writing the entry with a relative full lifetime restarts its clock on
        every request, so the index outlives the session it points at.  The
        assertion has to be that the TTL **decreased**.
        """
        sid = await _make(store, "42")
        key = store.index_key("42")
        await fake_async_redis.execute_command("HEXPIRE", key, 100, "FIELDS", 1, sid)

        record = store.new_record({"user_id": "42"})
        await store.index(
            "42", sid, record, absolute_remaining=90
        )  # the remainder, not 600

        reply = await fake_async_redis.execute_command("HTTL", key, "FIELDS", 1, sid)
        assert int(reply[0]) <= 100, (
            "the index entry's clock was restarted - it can now outlive the "
            "session it names"
        )

    async def test_an_expired_remainder_writes_no_entry(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        record = store.new_record({})
        await store.index("42", sid, record, absolute_remaining=0)
        assert await fake_async_redis.hgetall(store.index_key("42")) == {}
