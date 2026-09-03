"""Tests for what happens when Redis is unreachable.

Section 7 of the design makes the read/write policy deliberately asymmetric,
and the asymmetry is a security control rather than a convenience:

* a failed **read** yields an empty session, so the caller looks anonymous and
  the application's own authorization rejects them;
* a failed **write** always raises, because losing a login or a rotation is
  the worst outcome in this design and must never be silent.

Every ``except (RedisError, OSError)`` arm in the store is exercised here.
Before this file existed the whole policy was unexecuted code.
"""

from __future__ import annotations

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.config import get_settings
from redis_fastapi.session_backend import RedisSessionStore
from redis_fastapi.sessions import Session, SessionStoreError


class _BrokenPipeline:
    def execute_command(self, *args: object, **kwargs: object) -> None:
        return None

    async def execute(self) -> object:
        raise RedisConnectionError("Redis is down")


class _BrokenRedis:
    """A client whose every command fails, as an unreachable server does."""

    def pipeline(self, transaction: bool = True) -> _BrokenPipeline:
        return _BrokenPipeline()

    async def execute_command(self, *args: object, **kwargs: object) -> object:
        raise RedisConnectionError("Redis is down")

    async def delete(self, *keys: str) -> int:
        raise RedisConnectionError("Redis is down")

    async def hdel(self, key: str, *fields: str) -> int:
        raise RedisConnectionError("Redis is down")

    async def hgetall(self, key: str) -> dict:
        raise RedisConnectionError("Redis is down")

    async def info(self, section: str) -> dict:
        raise RedisConnectionError("Redis is down")


@pytest.fixture()
def broken() -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(_BrokenRedis(), idle_ttl=60, absolute_ttl=600)


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

    async def test_a_failed_listing_returns_empty(
        self, broken: RedisSessionStore
    ) -> None:
        assert await broken.list_for_subject("42") == []

    async def test_a_failed_count_returns_zero(self, broken: RedisSessionStore) -> None:
        assert await broken.count_for_subject("42") == 0


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

    async def test_indexing_raises(self, broken: RedisSessionStore) -> None:
        record = broken.new_record({"user_id": "42"})
        with pytest.raises(SessionStoreError, match="Could not index"):
            await broken.index("42", broken.new_id(), record, absolute_remaining=600)

    async def test_revoke_all_raises(self, broken: RedisSessionStore) -> None:
        with pytest.raises(SessionStoreError, match="Could not revoke"):
            await broken.revoke_all("42")

    async def test_revoke_id_raises_rather_than_reporting_success(
        self, broken: RedisSessionStore
    ) -> None:
        """Returning False here would read as "not your session".

        The caller cannot distinguish that from a genuine refusal, so a store
        failure has to surface as one.
        """
        with pytest.raises(SessionStoreError, match="Could not read the session index"):
            await broken.revoke_id(broken.new_id(), subject="42")

    async def test_a_write_still_raises_under_fail_open(
        self, broken: RedisSessionStore
    ) -> None:
        assert get_settings().session_fail_closed is False
        with pytest.raises(SessionStoreError):
            await broken.save(broken.new_id(), broken.new_record({}))


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

    async def test_a_failed_liveness_check_reports_nothing_alive(
        self, broken: RedisSessionStore
    ) -> None:
        assert await broken._verify(["a" * 30]) == set()


class TestUnreachableStates:
    async def test_a_field_with_no_expiry_is_treated_as_absent(
        self, fake_async_redis, caplog
    ) -> None:
        """The ``-1`` row of the state table, which must be unreachable.

        Reaching it means something outside this store wrote the key, so the
        store says so loudly and refuses the session rather than guessing.
        """
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        sid = store.new_id()
        key = store.session_key(sid)
        await fake_async_redis.hset(key, mapping={"a": "1", "d": "{}"})

        with caplog.at_level("ERROR"):
            assert await store.load(sid) is None
        assert "no expiry" in caplog.text
        assert await fake_async_redis.exists(key) == 0

    async def test_a_corrupt_record_raises_rather_than_signing_the_user_out(
        self, fake_async_redis
    ) -> None:
        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        sid = store.new_id()
        await store.save(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.execute_command(
            "HSETEX", store.session_key(sid), "KEEPTTL", "FIELDS", 1, "d", "{not json"
        )
        with pytest.raises(SessionStoreError, match="Unreadable session record"):
            await store.load(sid)


class TestOsErrorsCountToo:
    async def test_an_oserror_is_handled_like_a_redis_error(self) -> None:
        """A dropped socket surfaces as OSError, not RedisError."""

        class _SocketDied(_BrokenRedis):
            def pipeline(self, transaction: bool = True) -> object:
                class _P:
                    def execute_command(self, *a: object, **k: object) -> None:
                        return None

                    async def execute(self) -> object:
                        raise OSError("connection reset by peer")

                return _P()

        get_settings.cache_clear()
        store = RedisSessionStore(_SocketDied(), idle_ttl=60, absolute_ttl=600)
        assert await store.load(store.new_id()) is None
        with pytest.raises(SessionStoreError):
            await store.save(store.new_id(), store.new_record({}))


class TestRevokeOnABrokenStore:
    async def test_revoke_surfaces_the_failure(self, broken: RedisSessionStore) -> None:
        """Revocation that quietly does nothing is the worst kind."""
        session = Session({"user_id": 42})
        session.sid = "a" * 30
        with pytest.raises(SessionStoreError):
            await broken.revoke(session)

    async def test_rotate_surfaces_the_failure(self, broken: RedisSessionStore) -> None:
        session = Session({"user_id": 42})
        session.sid = "a" * 30
        with pytest.raises(SessionStoreError):
            await broken.rotate(session, subject="42")
