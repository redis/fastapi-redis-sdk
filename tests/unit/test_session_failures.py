"""Tests for what happens when Redis is unreachable.

Section 7 of the design makes the read/write policy deliberately asymmetric,
and the asymmetry is a security control rather than a convenience:

* a failed **read** yields an empty session, so the caller looks anonymous and
  the application's own authorization rejects them;
* a failed **write** always raises, because losing a login or a rotation is
  the worst outcome in this design and must never be silent.

A search is a read that does not fail open.  An empty list, a count of zero
and "nothing to revoke" are each an answer, so a failed search raises.

Every ``except (RedisError, OSError)`` arm in the store is exercised here.
Before this file existed the whole policy was unexecuted code.
"""

from __future__ import annotations

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import SessionSearchError, SessionStoreError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionState,
    _StoreCapabilities,
)


class _BrokenPipeline:
    """Queues any command, and fails when it is sent."""

    def __init__(self, error: BaseException | None = None) -> None:
        self._error = error or RedisConnectionError("Redis is down")

    def __getattr__(self, name: str) -> object:
        return lambda *args, **kwargs: None

    async def execute(self) -> object:
        raise self._error


class _BrokenIndex:
    async def create_index(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("Redis is down")

    async def dropindex(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("Redis is down")

    async def search(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("Redis is down")


class _BrokenRedis:
    """A client whose every command fails, as an unreachable server does."""

    def pipeline(self, transaction: bool = True) -> _BrokenPipeline:
        return _BrokenPipeline()

    def ft(self, name: str) -> _BrokenIndex:
        return _BrokenIndex()

    async def execute_command(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("Redis is down")

    async def delete(self, *keys: str) -> int:
        raise RedisConnectionError("Redis is down")

    async def hget(self, key: str, field: str) -> bytes:
        raise RedisConnectionError("Redis is down")

    async def config_get(self, pattern: str) -> dict:
        raise RedisConnectionError("Redis is down")


@pytest.fixture()
def broken() -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(_BrokenRedis(), idle_ttl=60, absolute_ttl=600)


@pytest.fixture()
def broken_search() -> RedisSessionStore:
    """A server that had the Query Engine when the pool asked, and is down
    now."""
    get_settings.cache_clear()
    return RedisSessionStore(
        _BrokenRedis(),
        capabilities=_StoreCapabilities(supports_search=True, supports_hsetex=True),
        idle_ttl=60,
        absolute_ttl=600,
        searchable={"device": "tag"},
    )


@pytest.fixture()
def fail_closed(monkeypatch) -> RedisSessionStore:
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_FAIL_CLOSED", "true")
    get_settings.cache_clear()
    store = RedisSessionStore(_BrokenRedis(), idle_ttl=60, absolute_ttl=600)
    yield store
    get_settings.cache_clear()


class TestReadsFailOpen:
    async def test_a_failed_load_yields_no_session(
        self, broken: RedisSessionStore
    ) -> None:
        """The user looks anonymous; a protected route stays protected.

        It never depended on this read succeeding - the application's own
        authorization dependency finds no user and returns a login page.
        """
        assert await broken.load(broken.new_id()) is None

    async def test_a_failed_load_logs_a_warning(
        self, broken: RedisSessionStore, caplog
    ) -> None:
        with caplog.at_level("WARNING"):
            await broken.load(broken.new_id())
        assert "Session load failed" in caplog.text

    async def test_a_load_with_no_capability_answer_still_fails_open(
        self, broken: RedisSessionStore
    ) -> None:
        """The capability check fails too.  It must not turn the failed read
        into an error of its own."""
        assert broken._caps.supports_search is None
        assert await broken.load_with_status(broken.new_id()) == (None, True)
        assert broken._caps.supports_search is None, "no answer was cached as one"


SEARCHES = {
    "list_for_subject": lambda store: store.list_for_subject("42"),
    "count_for_subject": lambda store: store.count_for_subject("42"),
    "revoke_all": lambda store: store.revoke_all("42"),
    "list_active": lambda store: store.list_active(60),
    "revoke_idle": lambda store: store.revoke_idle(60),
    "find": lambda store: store.find("device", equals="d-91"),
    "count": lambda store: store.count("device", equals="d-91"),
}


class TestSearchesDoNotFailOpen:
    """An empty answer from a failed search is the permissive one.

    A count of zero lets each sign-in past a cap on concurrent sessions, and
    "nothing to revoke" reports a sign-out everywhere that did not happen.
    """

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_a_failed_search_raises(
        self, broken_search: RedisSessionStore, method: str
    ) -> None:
        with pytest.raises(SessionSearchError):
            await SEARCHES[method](broken_search)

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_a_failed_query_on_a_ready_index_raises(
        self, broken_search: RedisSessionStore, method: str
    ) -> None:
        """The same, when the indexes were ensured before the outage."""
        broken_search._caps.ready_indexes.update(
            {broken_search._builtin_index, broken_search._custom_index}
        )
        with pytest.raises(SessionSearchError, match="Could not search sessions"):
            await SEARCHES[method](broken_search)

    @pytest.mark.parametrize("method", list(SEARCHES))
    async def test_a_search_with_no_capability_answer_raises(self, method: str) -> None:
        """The server is down before the pool learned what it can do."""
        get_settings.cache_clear()
        store = RedisSessionStore(_BrokenRedis(), searchable={"device": "tag"})
        with pytest.raises(SessionStoreError):
            await SEARCHES[method](store)

    async def test_a_search_raises_with_reads_failing_open_or_closed(
        self, broken_search: RedisSessionStore, monkeypatch
    ) -> None:
        assert get_settings().session_fail_closed is False
        with pytest.raises(SessionSearchError):
            await broken_search.list_for_subject("42")
        monkeypatch.setenv("REDIS_SESSION_FAIL_CLOSED", "true")
        get_settings.cache_clear()
        with pytest.raises(SessionSearchError):
            await broken_search.list_for_subject("42")

    async def test_the_index_setup_raises(
        self, broken_search: RedisSessionStore
    ) -> None:
        with pytest.raises(SessionSearchError, match="Could not create"):
            await broken_search.ensure_indexes()


class TestFailClosedFlipsReadsOnly:
    async def test_a_failed_load_raises(self, fail_closed: RedisSessionStore) -> None:
        """For a deployment that prefers a 503 to an anonymous page."""
        with pytest.raises(SessionStoreError, match="Could not load session"):
            await fail_closed.load(fail_closed.new_id())

    async def test_the_setting_is_read_per_call_not_per_store(
        self, broken: RedisSessionStore, monkeypatch
    ) -> None:
        assert await broken.load(broken.new_id()) is None
        monkeypatch.setenv("REDIS_SESSION_FAIL_CLOSED", "true")
        get_settings.cache_clear()
        with pytest.raises(SessionStoreError):
            await broken.load(broken.new_id())
        get_settings.cache_clear()


class TestWritesAlwaysRaise:
    """Whatever ``session_fail_closed`` says.  Section 7's asymmetry."""

    async def test_save_raises(self, broken: RedisSessionStore) -> None:
        with pytest.raises(SessionStoreError, match="Could not save session"):
            await broken.save(broken.new_id(), broken.new_record({"user_id": 42}))

    async def test_touch_raises(self, broken: RedisSessionStore) -> None:
        with pytest.raises(SessionStoreError, match="Could not refresh session"):
            await broken.touch(broken.new_id())

    async def test_delete_raises(self, broken: RedisSessionStore) -> None:
        with pytest.raises(SessionStoreError, match="Could not delete session"):
            await broken.delete(broken.new_id())

    async def test_create_raises(self, broken: RedisSessionStore) -> None:
        record = broken.new_record({"user_id": "42"})
        with pytest.raises(SessionStoreError, match="Could not save session"):
            await broken.create(broken.new_id(), record, subject="42")

    async def test_revoke_all_raises(self, broken_search: RedisSessionStore) -> None:
        with pytest.raises(SessionStoreError):
            await broken_search.revoke_all("42")

    async def test_revoke_id_raises_rather_than_reporting_success(
        self, broken: RedisSessionStore
    ) -> None:
        """Returning False here would read as "not your session".

        The caller cannot distinguish that from a genuine refusal, so a store
        failure has to surface as one.
        """
        with pytest.raises(SessionStoreError, match="Could not read the session"):
            await broken.revoke_id(broken.new_id(), subject="42")

    async def test_a_write_still_raises_under_fail_closed(
        self, fail_closed: RedisSessionStore
    ) -> None:
        """The setting changes reads only; a write raises either way."""
        assert get_settings().session_fail_closed is True
        with pytest.raises(SessionStoreError, match="Could not save session"):
            await fail_closed.save(fail_closed.new_id(), fail_closed.new_record({}))


class TestTidyUpNeverFailsARequest:
    async def test_a_failed_cleanup_delete_is_swallowed(
        self, broken: RedisSessionStore, caplog
    ) -> None:
        """This runs on the tidy-up path of a read.

        Failing the request because we could not remove a session that is
        already gone turns a cosmetic problem into an outage.
        """
        with caplog.at_level("WARNING"):
            await broken._safe_delete(broken.new_id())
        assert "Could not remove a dead session key" in caplog.text

    async def test_a_dead_hash_that_cannot_be_deleted_is_still_no_session(
        self, fake_async_redis, caplog
    ) -> None:
        """The load found a hash with no ``s:created``, and the delete failed."""

        class _DeleteFails(RedisSessionStore):
            async def _delete(self, session_id: str) -> None:
                raise RedisConnectionError("Redis is down")

        get_settings.cache_clear()
        store = _DeleteFails(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        sid = store.new_id()
        await fake_async_redis.hset(store.session_key(sid), "a:user_id", "42")

        with caplog.at_level("WARNING"):
            assert await store.load_with_status(sid) == (None, False)
        assert "Could not remove a dead session key" in caplog.text


class TestUnexpectedStates:
    @staticmethod
    async def _corrupt_session(redis) -> tuple[RedisSessionStore, str]:
        get_settings.cache_clear()
        store = RedisSessionStore(redis, idle_ttl=60, absolute_ttl=600)
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        # One attribute field that is not JSON text. A plain ``HSET`` of that
        # field leaves the deadline on ``s:created`` alone.
        await redis.hset(store.session_key(sid), "a:user_id", "{not json")
        return store, sid

    async def test_a_corrupt_record_reads_as_a_failed_load_and_is_kept(
        self, fake_async_redis
    ) -> None:
        """Kept, so a mistaken encryption key can be fixed without losing every
        session; the failure flag lets the gate say "unavailable"."""
        store, sid = await self._corrupt_session(fake_async_redis)

        assert await store.load_with_status(sid) == (None, True)
        assert await fake_async_redis.exists(store.session_key(sid)) == 1

    async def test_a_corrupt_record_raises_when_reads_fail_closed(
        self, fake_async_redis, monkeypatch
    ) -> None:
        monkeypatch.setenv("REDIS_SESSION_FAIL_CLOSED", "true")
        store, sid = await self._corrupt_session(fake_async_redis)
        try:
            with pytest.raises(SessionStoreError, match="Unreadable session field"):
                await store.load(sid)
        finally:
            get_settings.cache_clear()

    async def test_a_creation_time_that_is_not_a_number_is_a_failed_load(
        self, fake_async_redis
    ) -> None:
        """A system field can be corrupt too.  The deadline is its TTL, not
        its value, so the hash is still a session and it is kept."""
        get_settings.cache_clear()
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.hset(key, "s:created", "yesterday")
        await fake_async_redis.execute_command(
            "HEXPIRE", key, 600, "FIELDS", 1, "s:created"
        )

        assert await store.load_with_status(sid) == (None, True)
        assert await fake_async_redis.exists(key) == 1


class TestOsErrorsCountToo:
    async def test_an_oserror_is_handled_like_a_redis_error(self) -> None:
        """A dropped socket surfaces as OSError, not RedisError."""

        class _SocketDied(_BrokenRedis):
            def pipeline(self, transaction: bool = True) -> _BrokenPipeline:
                return _BrokenPipeline(OSError("connection reset by peer"))

        get_settings.cache_clear()
        store = RedisSessionStore(_SocketDied(), idle_ttl=60, absolute_ttl=600)
        assert await store.load(store.new_id()) is None
        with pytest.raises(SessionStoreError):
            await store.save(store.new_id(), store.new_record({}))


class TestRevokeOnABrokenStore:
    async def test_revoke_surfaces_the_failure(self, broken: RedisSessionStore) -> None:
        """Revocation that quietly does nothing is the worst kind."""
        state = SessionState(data={"user_id": 42}, session_id="a" * 30)
        with pytest.raises(SessionStoreError):
            await broken.revoke(state)

    async def test_rotate_surfaces_the_failure(self, broken: RedisSessionStore) -> None:
        state = SessionState(data={"user_id": 42}, session_id="a" * 30)
        with pytest.raises(SessionStoreError):
            await broken.rotate(state, subject="42")
