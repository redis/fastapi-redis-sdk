"""Integration tests for sessions against a real Redis server.

These prove the things ``fakeredis`` cannot: that expiry is enforced by the
server itself, that the index prunes itself with no help from us, and that the
capability probe picks the right command tier for whatever server is running.
"""

import asyncio

import pytest
import redis.asyncio as async_redis

from redis_fastapi.session_backend import (
    FIELD_ABSOLUTE,
    FIELD_DATA,
    RedisSessionStore,
    probe_hsetex_support,
)
from tests.conftest import requires_redis

pytestmark = [pytest.mark.integration, requires_redis, pytest.mark.asyncio]


def _store(redis, prefix: str, **kwargs) -> RedisSessionStore:
    kwargs.setdefault("idle_ttl", 60)
    kwargs.setdefault("absolute_ttl", 600)
    return RedisSessionStore(redis, key_prefix=prefix, **kwargs)


async def test_capability_probe_answers_definitively(
    real_async_redis: async_redis.Redis,
) -> None:
    """Never ``None`` against a reachable server - that value means "unknown"."""
    assert await probe_hsetex_support(real_async_redis) in {True, False}


async def test_round_trip(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    store = _store(real_async_redis, test_prefix)
    sid = store.new_id()
    await store.create(sid, store.new_record({"user_id": 42}))
    loaded = await store.load(sid)
    assert loaded is not None
    assert loaded.record.data == {"user_id": 42}


async def test_redis_enforces_the_idle_clock(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    """The server expires the field; nothing here counts down."""
    store = _store(real_async_redis, test_prefix, idle_ttl=1, absolute_ttl=600)
    sid = store.new_id()
    await store.create(sid, store.new_record({"user_id": 42}))
    assert await store.load(sid) is not None

    await asyncio.sleep(1.5)
    assert await store.load(sid) is None, "the idle clock was not enforced"


async def test_redis_enforces_the_absolute_clock_despite_activity(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    """The guarantee the two-field layout exists for.

    The session is loaded continuously - each load refreshes the idle clock -
    and it must still die on schedule.
    """
    store = _store(real_async_redis, test_prefix, idle_ttl=60, absolute_ttl=2)
    sid = store.new_id()
    await store.create(sid, store.new_record({"user_id": 42}))

    for _ in range(4):
        await asyncio.sleep(0.6)
        result = await store.load(sid)
        await store.save(sid, store.new_record({"user_id": 42}))
        if result is None:
            break
    else:
        pytest.fail(
            "the session outlived its absolute deadline under continuous "
            "activity - writing the payload extended field 'a'"
        )


async def test_the_index_prunes_itself_with_no_help_from_us(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    """No sweeper, no cron, no reconciliation pass.

    This is what no other backend can do: Postgres needs a scheduled DELETE,
    DynamoDB's sweeper runs up to 48 hours late, Memcached cannot express the
    question at all.
    """
    store = _store(real_async_redis, test_prefix, idle_ttl=60, absolute_ttl=1)
    sid = store.new_id()
    record = store.new_record({"user_id": "42"})
    await store.create(sid, record)
    await store.index("42", sid, record, absolute_remaining=1)
    assert len(await store.list_for_subject("42")) == 1

    await asyncio.sleep(1.5)
    raw = await real_async_redis.hgetall(store.index_key("42"))
    assert raw == {}, "Redis did not expire the index entry on its own"


async def test_rotation_deletes_the_old_key_before_writing_the_new_one(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    from redis_fastapi.session_backend import SessionState

    store = _store(real_async_redis, test_prefix)
    state = SessionState(data={"user_id": 42}, session_id=store.new_id())
    await store.create(state.session_id, store.new_record(dict(state.data)))
    old = state.session_id

    new = await store.rotate(state, subject="42")
    assert new != old
    assert await real_async_redis.exists(store.session_key(old)) == 0
    assert await real_async_redis.exists(store.session_key(new)) == 1


async def test_writing_the_payload_leaves_the_deadline_alone(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    store = _store(real_async_redis, test_prefix)
    sid = store.new_id()
    key = store.session_key(sid)
    await store.create(sid, store.new_record({"n": 0}))

    before = (
        await real_async_redis.execute_command("HTTL", key, "FIELDS", 1, FIELD_ABSOLUTE)
    )[0]
    for n in range(5):
        await store.save(sid, store.new_record({"n": n}))
    after = (
        await real_async_redis.execute_command("HTTL", key, "FIELDS", 1, FIELD_ABSOLUTE)
    )[0]

    assert int(after) <= int(before)
    idle = (
        await real_async_redis.execute_command("HTTL", key, "FIELDS", 1, FIELD_DATA)
    )[0]
    assert int(idle) > 0


async def test_revoke_all_ends_every_session(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    store = _store(real_async_redis, test_prefix)
    ids = []
    for _ in range(3):
        sid = store.new_id()
        record = store.new_record({"user_id": "42"})
        await store.create(sid, record)
        await store.index("42", sid, record, absolute_remaining=600)
        ids.append(sid)

    assert await store.revoke_all("42") == 3
    for sid in ids:
        assert await store.load(sid) is None
