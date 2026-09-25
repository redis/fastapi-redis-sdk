"""Regression tests for the defects a review panel found in the first cut.

Each test here fails against the implementation as it was before the fix, so
none may be dropped as redundant.  The design's §11 calls this out as a rule:
a refuted claim earns a permanent test.
"""

from __future__ import annotations

import time
from http.cookies import SimpleCookie

import pytest
from fastapi import FastAPI, Request, Response
from fastapi.testclient import TestClient
from redis.exceptions import ConnectionError as RedisConnectionError

from redis_fastapi.config import get_settings
from redis_fastapi.deps import (
    SessionDep,
    SessionStateDep,
    SessionStoreDep,
    get_session_store,
)
from redis_fastapi.exceptions import SessionStoreError
from redis_fastapi.session_backend import (
    RedisSessionStore,
    SessionStoreProtocol,
)
from redis_fastapi.sessions import add_redis_sessions


def _cookie(response) -> SimpleCookie:
    jar: SimpleCookie = SimpleCookie()
    jar.load(response.headers["set-cookie"])
    return jar


@pytest.fixture()
def store(fake_async_redis) -> RedisSessionStore:
    get_settings.cache_clear()
    return RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)


def _app(store: RedisSessionStore, **kwargs) -> FastAPI:
    get_settings.cache_clear()
    app = FastAPI()
    add_redis_sessions(app, **kwargs)

    async def _factory(request: Request) -> RedisSessionStore:
        return store

    app.dependency_overrides[get_session_store] = _factory

    @app.post("/login")
    async def login(session: SessionDep) -> dict:
        session["user_id"] = 42
        return {}

    @app.post("/logout-clear")
    async def logout_clear(session: SessionDep) -> dict:
        session.clear()
        return {}

    @app.post("/logout-revoke")
    async def logout_revoke(state: SessionStateDep, st: SessionStoreDep) -> dict:
        await st.revoke(state)
        return {}

    @app.post("/login-and-rotate")
    async def login_and_rotate(
        session: SessionDep, state: SessionStateDep, st: SessionStoreDep
    ) -> dict:
        session["user_id"] = 42
        return {"sid": await st.rotate(state, subject="42")}

    @app.get("/me")
    async def me(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    return app


@pytest.fixture(autouse=True)
def _plain_http(monkeypatch):
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


class TestTheAbsoluteDeadlineCannotBeResurrected:
    """N-6: a save that lands after the deadline must not restore it.

    A request can load a live session and write it back after the key
    expired.  If that write set a deadline, it would set a full fresh
    lifetime; the window is one request long and recurs every cycle, so an
    actively used session would never die.  An expired key is an absent key,
    so the case is reproduced with ``DEL``.
    """

    async def test_a_save_after_the_deadline_lapsed_does_not_restore_it(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))

        await fake_async_redis.delete(key)
        await store.save(sid, store.new_record({"user_id": 42}))

        assert await fake_async_redis.ttl(key) == -1, (
            "the absolute deadline was recreated"
        )

    async def test_such_a_session_is_dead_on_the_next_load(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        sid = store.new_id()
        key = store.session_key(sid)
        await store.create(sid, store.new_record({"user_id": 42}))
        await fake_async_redis.delete(key)
        await store.save(sid, store.new_record({"user_id": 42}))
        assert await store.load(sid) is None
        assert await fake_async_redis.exists(key) == 0

    async def test_save_has_no_argument_that_could_write_the_deadline(self) -> None:
        """The guarantee is structural, not a runtime check."""
        import inspect

        params = set(inspect.signature(RedisSessionStore.save).parameters)
        assert params == {"self", "session_id", "record"}


class TestAFailedVerifyDoesNotDestroyTheIndex:
    """N-8. One transient pipeline error used to prune every entry."""

    @pytest.fixture()
    def flaky(self, fake_async_redis) -> RedisSessionStore:
        class _Flaky(RedisSessionStore):
            async def _alive(self, session_ids: list[str]) -> set[str]:
                raise RedisConnectionError("verify pipeline failed")

        return _Flaky(fake_async_redis, idle_ttl=60, absolute_ttl=600)

    async def _seed(self, store: RedisSessionStore, n: int) -> list[str]:
        ids = []
        for _ in range(n):
            sid = store.new_id()
            record = store.new_record({"user_id": "42"})
            await store.create(sid, record)
            await store.index("42", sid, record, absolute_remaining=600)
            ids.append(sid)
        return ids

    async def test_unknown_liveness_reports_everything_and_prunes_nothing(
        self, flaky: RedisSessionStore, fake_async_redis
    ) -> None:
        await self._seed(flaky, 3)
        listed = await flaky.list_for_subject("42")
        assert len(listed) == 3
        assert len(await fake_async_redis.hgetall(flaky.index_key("42"))) == 3

    async def test_the_sessions_stay_revocable(
        self, flaky: RedisSessionStore, fake_async_redis
    ) -> None:
        """The consequence that made this blocking, asserted end to end."""
        await self._seed(flaky, 3)
        await flaky.list_for_subject("42")

        healthy = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        assert await healthy.revoke_all("42") == 3


class TestLogout:
    def test_clearing_the_session_signs_the_user_out(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """§4.2 row 4.

        Emptying a session changes the principal, so before the fix it read as
        a privilege change and minted the user a brand-new valid session.
        """
        with TestClient(_app(store)) as client:
            client.post("/login")
            morsel = _cookie(client.post("/logout-clear"))["session"]
            assert morsel.value == ""
            assert morsel["max-age"] == "0"
            assert client.get("/me").json() == {"user_id": None}

    async def test_clearing_removes_the_key_and_the_index_entry(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            client.post("/login")
            client.post("/logout-clear")
        assert await fake_async_redis.keys(f"{store._session_prefix}*") == []
        assert await fake_async_redis.hgetall(store.index_key("42")) == {}

    async def test_revoke_removes_the_index_entry(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        """Otherwise ``count_for_subject`` over-counts until the deadline and a
        concurrent-session cap refuses a legitimate login."""
        with TestClient(_app(store)) as client:
            client.post("/login")
            client.post("/logout-revoke")
        assert await fake_async_redis.hgetall(store.index_key("42")) == {}
        assert await store.count_for_subject("42") == 0

    def test_the_clearing_cookie_goes_through_the_builder(
        self, store: RedisSessionStore
    ) -> None:
        """The seam exists for ``Partitioned`` and ``__Host-``.

        A browser deletes a cookie only when the clearing header repeats every
        scoping attribute, so a builder that applies to the set and not the
        clear leaves the cookie in place - defeating the seam's whole purpose.
        """
        from redis_fastapi.sessions import build_cookie

        def custom(spec) -> str:
            return build_cookie(spec) + "; Partitioned"

        with TestClient(_app(store, cookie_builder=custom)) as client:
            client.post("/login")
            header = client.post("/logout-revoke").headers["set-cookie"]
        assert "Partitioned" in header
        assert "Max-Age=0" in header


class TestAnExplicitRotateIsNeverRepeated:
    """The branch-ordering fix, on the case the first attempt missed.

    ``rotate()`` clears the pending-rotation flag but cannot clear the
    principal change, so the ordinary sign-in-then-rotate idiom still rotated
    twice and returned an identifier naming a deleted key.
    """

    def test_login_then_rotate_rotates_once(self, store: RedisSessionStore) -> None:
        calls: list[int] = []
        original = store.rotate

        async def counting(session, **kwargs):
            calls.append(1)
            return await original(session, **kwargs)

        store.rotate = counting  # type: ignore[method-assign]
        with TestClient(_app(store)) as client:
            client.post("/login-and-rotate")
        assert calls == [1]

    async def test_the_returned_id_is_the_live_one(
        self, store: RedisSessionStore, fake_async_redis
    ) -> None:
        with TestClient(_app(store)) as client:
            response = client.post("/login-and-rotate")
            returned = response.json()["sid"]
            assert returned == _cookie(response)["session"].value
        assert await fake_async_redis.exists(store.session_key(returned)) == 1


class TestMetadataSurvivesAWrite:
    async def test_created_is_not_restamped(self, store: RedisSessionStore) -> None:
        """Otherwise a device listing reports every session as seconds old."""
        sid = store.new_id()
        first = store.new_record({"n": 1})
        await store.create(sid, first)
        time.sleep(0.01)

        loaded = await store.load(sid)
        assert loaded is not None
        second = store.new_record({"n": 2}, created=loaded.record.metadata.created)
        await store.save(sid, second)

        again = await store.load(sid)
        assert again is not None
        assert again.record.metadata.created == pytest.approx(
            first.metadata.created, abs=1e-6
        )
        assert again.record.metadata.last_access > first.metadata.last_access

    def test_lifetime_records_the_deadline_actually_in_force(
        self, fake_async_redis
    ) -> None:
        store = RedisSessionStore(fake_async_redis, absolute_ttl=0, gc_ttl=1234)
        assert store.new_record({}).metadata.lifetime == 1234


class TestDescriptor:
    async def test_the_seam_populates_a_listing(self, store: RedisSessionStore) -> None:
        """F-7. Before the fix the only channel was an undocumented magic key
        inside the payload, so ``SessionInfo.descriptor`` was always empty."""
        app = _app(store, descriptor_of=lambda request, session: {"ua": "pytest"})
        with TestClient(app) as client:
            client.post("/login")
        info = (await store.list_for_subject("42"))[0]
        assert info.descriptor == {"ua": "pytest"}

    async def test_it_never_reaches_the_payload(self, store: RedisSessionStore) -> None:
        app = _app(store, descriptor_of=lambda request, session: {"ua": "pytest"})
        with TestClient(app) as client:
            client.post("/login")
            assert client.get("/me").json() == {"user_id": 42}
        sid = (await store.list_for_subject("42"))[0].session_id
        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}


class TestRevokeAllCostAndCount:
    async def test_it_counts_keys_removed_not_index_entries(
        self, store: RedisSessionStore
    ) -> None:
        live = store.new_id()
        record = store.new_record({"user_id": "7"})
        await store.create(live, record)
        await store.index("7", live, record, absolute_remaining=600)
        # An entry whose session has already died.
        await store.index("7", store.new_id(), record, absolute_remaining=600)

        assert await store.revoke_all("7") == 1

    async def test_it_is_two_round_trips_whatever_the_count(
        self, store: RedisSessionStore
    ) -> None:
        """N-2. It was 1+2N sequential awaits."""
        for _ in range(5):
            sid = store.new_id()
            record = store.new_record({"user_id": "9"})
            await store.create(sid, record)
            await store.index("9", sid, record, absolute_remaining=600)

        trips = 0
        original_members = store._index_members
        original_many = store._delete_many
        original_clear = store._index_clear

        async def c1(subject):
            nonlocal trips
            trips += 1
            return await original_members(subject)

        async def c2(ids):
            nonlocal trips
            trips += 1
            return await original_many(ids)

        async def c3(subject):
            nonlocal trips
            trips += 1
            return await original_clear(subject)

        store._index_members = c1  # type: ignore[method-assign]
        store._delete_many = c2  # type: ignore[method-assign]
        store._index_clear = c3  # type: ignore[method-assign]

        assert await store.revoke_all("9") == 5
        assert trips == 3, "read the index, delete the keys, drop the index"


class TestCountDoesNotFailOpen:
    async def test_an_uncountable_index_raises(self, fake_async_redis) -> None:
        class _Broken(RedisSessionStore):
            async def _index_size(self, subject: str) -> int:
                raise RedisConnectionError("down")

        store = _Broken(fake_async_redis)
        with pytest.raises(SessionStoreError, match="Could not count sessions"):
            await store.count_for_subject("42")

    async def test_an_unreadable_index_at_the_limit_raises(
        self, fake_async_redis
    ) -> None:
        """The members read only happens at the limit, and it must raise too."""

        class _Broken(RedisSessionStore):
            async def _index_members(self, subject: str) -> dict[str, bytes | str]:
                raise RedisConnectionError("down")

        store = _Broken(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        record = store.new_record({"user_id": "42"})
        for _ in range(2):
            await store.index("42", store.new_id(), record, absolute_remaining=600)
        with pytest.raises(SessionStoreError, match="Could not count sessions"):
            await store.count_for_subject("42", limit=2)

    async def test_an_unverifiable_count_at_the_limit_raises(
        self, fake_async_redis
    ) -> None:
        class _Flaky(RedisSessionStore):
            async def _alive(self, session_ids: list[str]) -> set[str]:
                raise RedisConnectionError("verify failed")

        store = _Flaky(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        record = store.new_record({"user_id": "42"})
        for _ in range(2):
            await store.index("42", store.new_id(), record, absolute_remaining=600)
        with pytest.raises(SessionStoreError, match="refusing to answer"):
            await store.count_for_subject("42", limit=2)


class TestClusterErrorsAreHandled:
    """``RedisClusterException`` is not a ``RedisError``.

    ``SlotNotCoveredError`` is raised on the ordinary command path during a
    resharding, so catching only ``RedisError`` let the commonest cluster
    failure escape every policy in the store.
    """

    def test_the_boundary_covers_the_cluster_hierarchy(self) -> None:
        from redis.exceptions import RedisError, SlotNotCoveredError

        from redis_fastapi.session_backend import STORE_ERRORS

        assert not issubclass(SlotNotCoveredError, RedisError)
        assert issubclass(SlotNotCoveredError, STORE_ERRORS)

    async def test_a_slot_error_fails_the_read_open(self, fake_async_redis) -> None:
        from redis.exceptions import SlotNotCoveredError

        class _Resharding(RedisSessionStore):
            async def _read(self, session_id: str, *, refresh_idle: int | None):
                raise SlotNotCoveredError('Slot "42" is not covered')

        store = _Resharding(fake_async_redis)
        assert await store.load("a" * 30) is None

    async def test_a_slot_error_on_a_write_raises_session_store_error(
        self, fake_async_redis
    ) -> None:
        from redis.exceptions import SlotNotCoveredError

        class _Resharding(RedisSessionStore):
            async def _write(self, session_id, payload, *, idle, absolute):
                raise SlotNotCoveredError('Slot "42" is not covered')

        store = _Resharding(fake_async_redis)
        with pytest.raises(SessionStoreError):
            await store.create("a" * 30, store.new_record({}))


class TestTheProtocolExists:
    def test_the_store_satisfies_it(self, store: RedisSessionStore) -> None:
        """The docstring advertised it to users before it existed."""
        assert isinstance(store, SessionStoreProtocol)

    def test_it_is_exported(self) -> None:
        import redis_fastapi

        assert "SessionStoreProtocol" in redis_fastapi.__all__


class TestCookieAttributesAreValidated:
    @pytest.mark.parametrize("name", ["sess\r\nX-Evil: 1", "", "a;b", "a b", "a=b"])
    def test_a_dangerous_cookie_name_is_refused(self, name: str) -> None:
        from pydantic import ValidationError

        from redis_fastapi.config import RedisSettings

        with pytest.raises(ValidationError):
            RedisSettings(session_cookie_name=name)

    @pytest.mark.parametrize("path", ["/x\r\nEvil: 1", "no-leading-slash", "/a;b"])
    def test_a_dangerous_cookie_path_is_refused(self, path: str) -> None:
        from pydantic import ValidationError

        from redis_fastapi.config import RedisSettings

        with pytest.raises(ValidationError):
            RedisSettings(session_cookie_path=path)

    def test_a_dangerous_cookie_domain_is_refused(self) -> None:
        from pydantic import ValidationError

        from redis_fastapi.config import RedisSettings

        with pytest.raises(ValidationError):
            RedisSettings(session_cookie_domain="ex.com\r\nEvil: 1")

    def test_ordinary_values_are_accepted(self) -> None:
        from redis_fastapi.config import RedisSettings

        settings = RedisSettings(
            session_cookie_name="my_session",
            session_cookie_path="/admin",
            session_cookie_domain="example.com",
        )
        assert settings.session_cookie_name == "my_session"


class TestEncryptionSeam:
    async def test_a_supplied_encryptor_wraps_the_payload(
        self, fake_async_redis
    ) -> None:
        """F-15. Encryption wraps serialization; the coder never sees ciphertext."""

        class Rot13:
            def encrypt(self, data: bytes) -> bytes:
                return bytes(b ^ 0x5A for b in data)

            def decrypt(self, data: bytes) -> bytes:
                return bytes(b ^ 0x5A for b in data)

        store = RedisSessionStore(
            fake_async_redis, encryptor=Rot13(), idle_ttl=60, absolute_ttl=600
        )
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))

        stored = await fake_async_redis.hget(store.session_key(sid), "d")
        assert b"user_id" not in stored, "the payload reached Redis in the clear"

        loaded = await store.load(sid)
        assert loaded is not None
        assert loaded.record.data == {"user_id": 42}

    async def test_a_record_that_will_not_decrypt_is_unreadable_not_empty(
        self, fake_async_redis
    ) -> None:
        class Exploding:
            def encrypt(self, data: bytes) -> bytes:
                return data

            def decrypt(self, data: bytes) -> bytes:
                raise ValueError("bad tag")

        store = RedisSessionStore(fake_async_redis, encryptor=Exploding())
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        with pytest.raises(SessionStoreError, match="Unreadable session record"):
            await store.load(sid)


class TestCookieMaxAgeInEveryBranch:
    """§11 requires the cookie's ``max-age`` asserted in every branch.

    Only ``idle < absolute`` was covered, so replacing the whole computation
    with ``settings.session_idle_ttl`` kept the suite green.

    The value is the absolute remainder, never the idle clock.  The idle clock
    slides on every request, but read-only responses send no cookie, so a
    cookie sized by it expired while the record was alive (§6 of
    ``session-design.md``).
    """

    @pytest.mark.parametrize(
        ("idle", "absolute", "expected"),
        [
            (60, 600, "600"),  # the idle clock is ignored, even when nearer
            (1800, 60, "60"),  # the absolute remainder
            (0, 600, "600"),  # no idle clock
            (0, 0, ""),  # cookie-only: the browser decides
        ],
    )
    def test_max_age_follows_the_absolute_deadline(
        self, fake_async_redis, monkeypatch, idle, absolute, expected
    ) -> None:
        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", str(idle))
        monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", str(absolute))
        get_settings.cache_clear()

        store = RedisSessionStore(
            fake_async_redis, idle_ttl=idle, absolute_ttl=absolute
        )
        with TestClient(_app(store)) as client:
            morsel = _cookie(client.post("/login"))["session"]
        assert morsel["max-age"] == expected
        get_settings.cache_clear()


class TestFailedResponsesPersistOrdinaryData:
    """§4.3 has two halves and only one was tested.

    Dropping the ``changed and`` from the guard left the suite green and
    silently stopped every failed-login counter from incrementing.
    """

    def test_an_unchanged_principal_still_writes_on_a_4xx(
        self, store: RedisSessionStore
    ) -> None:
        app = _app(store)

        @app.post("/count-failure")
        async def count_failure(session: SessionDep, response: Response) -> dict:
            session["failed_attempts"] = session.get("failed_attempts", 0) + 1
            response.status_code = 401
            return {"n": session["failed_attempts"]}

        with TestClient(app) as client:
            assert client.post("/count-failure").json()["n"] == 1
            assert client.post("/count-failure").json()["n"] == 2, (
                "the counter did not persist across a failed response, so "
                "lockout silently stops working"
            )

    def test_a_changed_principal_writes_nothing_on_a_4xx(
        self, store: RedisSessionStore
    ) -> None:
        app = _app(store)

        @app.post("/failed-login")
        async def failed_login(session: SessionDep, response: Response) -> dict:
            session["user_id"] = 42
            response.status_code = 401
            return {}

        with TestClient(app) as client:
            response = client.post("/failed-login")
            assert "set-cookie" not in response.headers
            assert client.get("/me").json() == {"user_id": None}


class TestPrincipalKeysActuallyDrivesRotation:
    """Asserting the resolver is not ``None`` proved nothing.

    A resolver hard-wired to return ``None`` passed the old assertion while
    disabling rotation entirely.
    """

    def _app_with_role(self, store: RedisSessionStore) -> FastAPI:
        app = _app(store, principal_keys=["user_id", "role"])

        @app.post("/set-role/{role}")
        async def set_role(role: str, session: SessionDep) -> dict:
            session["user_id"] = 42
            session["role"] = role
            return {}

        return app

    def test_a_privilege_change_rotates(self, store: RedisSessionStore) -> None:
        with TestClient(self._app_with_role(store)) as client:
            first = _cookie(client.post("/set-role/user"))["session"].value
            second = _cookie(client.post("/set-role/admin"))["session"].value
        assert second != first

    def test_a_de_escalation_rotates_too(self, store: RedisSessionStore) -> None:
        """The case that is easiest to forget: dropping back down."""
        with TestClient(self._app_with_role(store)) as client:
            client.post("/set-role/user")
            up = _cookie(client.post("/set-role/admin"))["session"].value
            down = _cookie(client.post("/set-role/user"))["session"].value
        assert down != up

    def test_an_unchanged_role_does_not_rotate(self, store: RedisSessionStore) -> None:
        with TestClient(self._app_with_role(store)) as client:
            first = _cookie(client.post("/set-role/user"))["session"].value
            second = _cookie(client.post("/set-role/user"))["session"].value
        assert first == second


class TestRefreshOnLoadFalseThroughTheMiddleware:
    """§11 names this branch because an earlier draft omitted it.

    Deleting the response-time ``touch`` left the suite green and froze the
    idle clock, making every session immortal until its absolute deadline.
    """

    async def test_the_middleware_advances_the_idle_clock(
        self, fake_async_redis, monkeypatch
    ) -> None:
        from redis_fastapi.session_backend import FIELD_DATA

        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        monkeypatch.setenv("REDIS_SESSION_REFRESH_ON_LOAD", "false")
        get_settings.cache_clear()

        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        with TestClient(_app(store)) as client:
            client.post("/login")
            sid = client.cookies["session"]
            key = store.session_key(sid)
            await fake_async_redis.execute_command(
                "HEXPIRE", key, 5, "FIELDS", 1, FIELD_DATA
            )
            client.get("/me")
            reply = await fake_async_redis.execute_command(
                "HTTL", key, "FIELDS", 1, FIELD_DATA
            )
        assert int(reply[0]) > 5, "the idle clock never advanced"
        get_settings.cache_clear()

    async def test_an_untouched_request_does_not_advance_it(
        self, fake_async_redis, monkeypatch
    ) -> None:
        from redis_fastapi.session_backend import FIELD_DATA

        get_settings.cache_clear()
        monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
        monkeypatch.setenv("REDIS_SESSION_REFRESH_ON_LOAD", "false")
        get_settings.cache_clear()

        store = RedisSessionStore(fake_async_redis, idle_ttl=60, absolute_ttl=600)
        app = _app(store)

        @app.get("/untouched")
        async def untouched() -> dict:
            return {}

        with TestClient(app) as client:
            client.post("/login")
            key = store.session_key(client.cookies["session"])
            await fake_async_redis.execute_command(
                "HEXPIRE", key, 5, "FIELDS", 1, FIELD_DATA
            )
            client.get("/untouched")
            reply = await fake_async_redis.execute_command(
                "HTTL", key, "FIELDS", 1, FIELD_DATA
            )
        assert int(reply[0]) <= 5, (
            "a request that never used the session counted as activity"
        )
        get_settings.cache_clear()


class TestTelemetryCarriesNoIdentifiers:
    """§10 states a test asserts this. None did.

    Adding ``session_id`` to the metric labels kept the suite green.
    """

    def test_no_session_id_or_subject_reaches_a_metric_label(self, monkeypatch) -> None:
        from redis_fastapi import telemetry

        recorded: list[dict] = []

        class _Instrument:
            def add(self, amount, attributes=None):
                recorded.append(dict(attributes or {}))

            def record(self, value, attributes=None):
                recorded.append(dict(attributes or {}))

        monkeypatch.setattr(telemetry._state, "enabled", True)
        monkeypatch.setattr(telemetry._state, "session_operations", _Instrument())
        monkeypatch.setattr(telemetry._state, "session_latency", _Instrument())
        monkeypatch.setattr(telemetry._state, "session_events", _Instrument())

        telemetry.record_session_operation(operation="load", result="hit")
        telemetry.record_session_latency(duration=0.01, operation="load")
        telemetry.record_session_event(cause="idle", result="delivered")

        allowed = {"operation", "result", "cause"}
        for attributes in recorded:
            assert set(attributes) <= allowed, f"unexpected label: {attributes}"

    async def test_the_store_never_passes_an_id_to_a_span(
        self, store: RedisSessionStore, monkeypatch
    ) -> None:
        from redis_fastapi import telemetry

        seen: list[tuple[str, dict]] = []
        real = telemetry.session_span

        import contextlib

        @contextlib.contextmanager
        def spy(name, attributes=None):
            seen.append((name, dict(attributes or {})))
            with real(name, attributes):
                yield None

        monkeypatch.setattr("redis_fastapi.session_backend.session_span", spy)
        sid = store.new_id()
        await store.create(sid, store.new_record({"user_id": 42}))
        await store.load(sid)

        assert seen, "no span was opened"
        for name, attributes in seen:
            assert attributes == {}, f"{name} carried {attributes}"
