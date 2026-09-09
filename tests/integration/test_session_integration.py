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
    _StoreCapabilities,
    probe_hsetex_support,
)
from redis_fastapi.session_events import REQUIRED_CONFIG, SessionEvents
from tests.conftest import about, requires_redis

pytestmark = [pytest.mark.integration, requires_redis, pytest.mark.asyncio]


def _store(redis, prefix: str, **kwargs) -> RedisSessionStore:
    kwargs.setdefault("idle_ttl", 60)
    kwargs.setdefault("absolute_ttl", 600)
    return RedisSessionStore(redis, key_prefix=prefix, **kwargs)


async def _httl(redis, key: str, field: str) -> int:
    reply = await redis.execute_command("HTTL", key, "FIELDS", 1, field)
    return int(reply[0])


async def test_capability_probe_answers_definitively(
    real_async_redis: async_redis.Redis,
) -> None:
    """Never ``None`` against a reachable server - that value means "unknown"."""
    assert await probe_hsetex_support(real_async_redis) in {True, False}


async def test_the_probe_matches_the_server(
    real_async_redis: async_redis.Redis,
) -> None:
    """The answer, not merely that there is one.

    ``in {True, False}`` cannot fail for anything that returns a bool -
    including ``return False`` unconditionally, which is the answer that
    routes every operation down the 7.4 fallback. Nothing else in either
    suite would have noticed: ``fakeredis`` implements both tiers, so the
    unit suite is green either way, and this file is the only place a real
    server's answer is involved.
    """
    info = await real_async_redis.info("server")
    raw = str(info["redis_version"])
    major, minor = (int(part) for part in raw.split(".")[:2])
    expected = (major, minor) >= (8, 0)
    assert await probe_hsetex_support(real_async_redis) is expected, (
        f"the probe disagrees with the server it is probing ({raw})"
    )


@pytest.mark.parametrize("supports_hsetex", [True, False])
async def test_both_command_tiers_write_the_same_fields(
    real_async_redis: async_redis.Redis, test_prefix: str, supports_hsetex: bool
) -> None:
    """Both tiers, against a real server, on the same assertions.

    The two paths are not the same commands. The 8.0 path sends
    ``HSETEX key FNX EX n FIELDS 1 a 1``; the 7.4 path sends ``HSETNX`` then
    ``HEXPIRE key n NX FIELDS 1 a``. Argument order, the ``FNX``/``NX``
    semantics and the ``FIELDS numfields`` framing all differ, and
    ``fakeredis``'s argument parser is order-insensitive - so it accepts an
    option order a real server rejects.

    Forcing the fallback on an 8.x server is legitimate: the 7.4 commands
    still work there, and it is the only way to exercise that tier without
    provisioning a 7.4 instance.
    """
    if supports_hsetex and not await probe_hsetex_support(real_async_redis):
        pytest.skip("server has no HSETEX; the 8.0 tier cannot be forced on")

    store = _store(
        real_async_redis,
        test_prefix,
        capabilities=_StoreCapabilities(supports_hsetex=supports_hsetex),
    )
    key = store.session_key(sid := store.new_id())

    await store.create(sid, store.new_record({"user_id": 42}))
    assert about(await _httl(real_async_redis, key, FIELD_ABSOLUTE), 600)
    assert about(await _httl(real_async_redis, key, FIELD_DATA), 60)

    loaded = await store.load(sid)
    assert loaded is not None
    assert loaded.record.data == {"user_id": 42}
    assert 0 < loaded.absolute_remaining <= 600

    # Age the absolute clock, then update: the deadline must not move, and
    # the idle clock must be reapplied. Both are properties of the tier.
    await real_async_redis.execute_command(
        "HEXPIRE", key, 100, "FIELDS", 1, FIELD_ABSOLUTE
    )
    await store.save(sid, store.new_record({"user_id": 42, "n": 1}))
    assert about(await _httl(real_async_redis, key, FIELD_ABSOLUTE), 100), (
        "an update moved the absolute deadline"
    )
    assert about(await _httl(real_async_redis, key, FIELD_DATA), 60), (
        "an update did not reapply the idle clock"
    )

    await store.touch(sid)
    assert about(await _httl(real_async_redis, key, FIELD_ABSOLUTE), 100)
    assert about(await _httl(real_async_redis, key, FIELD_DATA), 60)


@pytest.mark.parametrize("supports_hsetex", [True, False])
async def test_both_index_tiers_expire_the_entry(
    real_async_redis: async_redis.Redis, test_prefix: str, supports_hsetex: bool
) -> None:
    """``_index_add``'s fallback had no test in either suite.

    Unlike the session write it uses no ``NX``, so nothing about it followed
    from the session-path tests. The entry carries the *remainder* of the
    absolute clock, never the full lifetime - a full lifetime would restart
    the entry's clock on every write and let the index outlive the session it
    names, which is unrevocable rather than merely untidy.
    """
    if supports_hsetex and not await probe_hsetex_support(real_async_redis):
        pytest.skip("server has no HSETEX; the 8.0 tier cannot be forced on")

    store = _store(
        real_async_redis,
        test_prefix,
        capabilities=_StoreCapabilities(supports_hsetex=supports_hsetex),
    )
    sid = store.new_id()
    record = store.new_record({"user_id": "42"})
    await store.create(sid, record)
    await store.index("42", sid, record, absolute_remaining=90)

    index_key = store.index_key("42")
    assert about(await _httl(real_async_redis, index_key, sid), 90)

    # Re-assert with a smaller remainder, as a later write would.
    await store.index("42", sid, record, absolute_remaining=80)
    assert about(await _httl(real_async_redis, index_key, sid), 80), (
        "the entry's clock is not the remainder it was re-asserted with"
    )

    assert [info.session_id for info in await store.list_for_subject("42")] == [sid]
    assert await store.count_for_subject("42") == 1


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

    # A band, not a ceiling: HTTL returns -2 for a deleted field and -1 for
    # one with no expiry, and both satisfy ``after <= before``. Five real
    # round trips can cross a second boundary, so allow a little slack below.
    assert int(after) > 0, "the absolute deadline was deleted or unset"
    assert int(before) - 5 <= int(after) <= int(before), (
        "field 'a' moved on a payload write"
    )
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


async def test_a_field_expiry_reaches_a_handler(
    real_async_redis: async_redis.Redis, test_prefix: str
) -> None:
    """The one thing a scripted Pub/Sub cannot prove: the channel is real.

    ``test_session_events.py`` drives ``_run`` deterministically against a
    fake, which covers the subscribe, the frame filtering and the dispatch.
    What it cannot check is that ``__subkeyevent@<db>__:hexpired`` is the name
    a real server publishes on, or that the payload really arrives in the
    documented length-prefixed shape.

    **Needs Redis 8.8.** Subkey notifications arrived there, and earlier
    servers reject the ``T`` flag outright - 8.7 answers ``CONFIG SET`` with
    "Invalid event class character. Use 'Ag$lshzxeKEtmdnocr'". So this
    configures the server itself and skips when that fails, rather than
    requiring a CI service-container flag: passing ``--notify-keyspace-events
    Th`` to the 7.4 leg of the matrix would stop that container booting at
    all.

    ``notify-keyspace-events`` is server-wide, so the original value is
    restored afterwards. The library never sets it; a test on a throwaway
    server may.
    """
    original = (await real_async_redis.config_get("notify-keyspace-events")).get(
        "notify-keyspace-events", ""
    )
    try:
        try:
            await real_async_redis.config_set("notify-keyspace-events", REQUIRED_CONFIG)
        except async_redis.RedisError as exc:
            pytest.skip(f"server will not take {REQUIRED_CONFIG!r}: {exc}")

        events = SessionEvents(real_async_redis, key_prefix=test_prefix)
        if await events.probe() == "none":
            pytest.skip("server cannot supply subkey notifications")

        delivered: asyncio.Queue = asyncio.Queue()

        @events.on_session_end
        async def _(session_id: str, cause: str) -> None:
            await delivered.put((session_id, cause))

        await events.start()
        try:
            # A one-second idle clock, so the idle field expires on its own.
            store = _store(real_async_redis, test_prefix, idle_ttl=1, absolute_ttl=600)
            sid = store.new_id()
            await store.create(sid, store.new_record({"user_id": 42}))

            # Expiry notifications fire when Redis removes the field, which
            # for a field nobody touches waits on the active-expiry cycle. A
            # read after the deadline forces the lazy path, so this does not
            # depend on that cycle's timing.
            await asyncio.sleep(1.5)
            assert await store.load(sid) is None

            session_id, cause = await asyncio.wait_for(delivered.get(), timeout=10)
        finally:
            await events.stop()

        assert session_id == sid
        assert cause == "idle"
    finally:
        await real_async_redis.config_set("notify-keyspace-events", original)
