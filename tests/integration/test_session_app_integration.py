"""The session features end to end: an app, a client, and a real Redis.

Each test drives the app through ``TestClient`` and then reads the session
hash from Redis.  The unit suite checks each part against ``fakeredis``.
These tests check that the parts meet: that what a handler does to
``request.session`` reaches Redis as the fields and the TTLs the design
states, and that a handler can search with the store it is given.

The tests of a search skip on a server without the Query Engine.  The other
tests run on each server, with the same app: ``searchable`` must not stop a
session from working where no search can run.
"""

from collections.abc import Iterator

import pytest
import redis as sync_redis
from fastapi import FastAPI, Request
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, SessionStateDep, SessionStoreDep
from redis_fastapi.session_layout import (
    FIELD_CREATED,
    FIELD_DESCRIPTOR,
    FIELD_SEEN,
    FIELD_SUBJECT,
)
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import (
    about,
    expire_now,
    has_query_engine,
    requires_query_engine,
    requires_redis,
)

pytestmark = [pytest.mark.integration, requires_redis]

IDLE = 60
ABSOLUTE = 600
NO_TTL = -1


def _descriptor_of(request: Request, session) -> dict:
    return {"device": request.headers.get("x-device", "unknown")}


@pytest.fixture()
def app(
    real_redis: sync_redis.Redis, test_prefix: str, monkeypatch
) -> Iterator[FastAPI]:
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    monkeypatch.setenv("REDIS_PREFIX", test_prefix)
    monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", str(IDLE))
    monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", str(ABSOLUTE))
    get_settings.cache_clear()

    application = FastAPI()
    FastAPIRedis(application).lifespan().sessions(
        descriptor_of=_descriptor_of,
        searchable={"device_id": "tag", "cart.total": "numeric"},
    )

    @application.post("/login/{user_id}")
    async def login(user_id: str, request: Request, session: SessionDep) -> dict:
        session["user_id"] = user_id
        session["device_id"] = request.headers.get("x-device", "unknown")
        return {}

    @application.post("/cart/{total}")
    async def cart(total: int, session: SessionDep) -> dict:
        session["cart"] = {"total": total, "items": ["road", "gravel"]}
        return {}

    @application.post("/flash")
    async def flash(request: Request) -> dict:
        request.session.set("flash", "x", ttl=60)
        return {}

    @application.post("/persist")
    async def persist(request: Request) -> dict:
        request.session.persist("flash")
        return {}

    @application.get("/read")
    async def read(session: SessionDep) -> dict:
        return dict(session)

    @application.get("/devices")
    async def devices(
        session: SessionDep, state: SessionStateDep, store: SessionStoreDep
    ) -> list[dict]:
        infos = await store.list_for_subject(session["user_id"])
        return [
            {
                "id": info.session_id,
                "descriptor": info.descriptor,
                "this_device": info.session_id == store.session_id(state),
            }
            for info in infos
        ]

    @application.get("/count")
    async def count(session: SessionDep, store: SessionStoreDep) -> dict:
        return {"count": await store.count_for_subject(session["user_id"])}

    @application.get("/find/{device_id}")
    async def find(device_id: str, store: SessionStoreDep) -> list[str]:
        infos = await store.find("device_id", equals=device_id)
        return [info.session_id for info in infos]

    @application.get("/carts-above/{total}")
    async def carts_above(total: int, store: SessionStoreDep) -> dict:
        infos = await store.find("cart.total", between=(total, None))
        return {
            "ids": [info.session_id for info in infos],
            "count": await store.count("cart.total", between=(total, None)),
        }

    @application.post("/logout-everywhere")
    async def logout_everywhere(session: SessionDep, store: SessionStoreDep) -> dict:
        return {"ended": await store.revoke_all(session["user_id"])}

    yield application
    get_settings.cache_clear()


@pytest.fixture()
def client(app: FastAPI) -> Iterator[TestClient]:
    with TestClient(app) as test_client:
        yield test_client


def _key(redis: sync_redis.Redis, prefix: str, session_id: str) -> str:
    return f"{prefix}:s:{session_id}"


def _httl(redis: sync_redis.Redis, key: str, field: str) -> int:
    return int(redis.execute_command("HTTL", key, "FIELDS", 1, field)[0])


def _sign_in(client: TestClient, user_id: str, device: str) -> str:
    """Sign in on a new device and return its session ID.

    The client keeps the cookie of the last sign-in.  A test that acts as an
    earlier device calls :func:`_use`.
    """
    client.cookies.clear()
    response = client.post(f"/login/{user_id}", headers={"x-device": device})
    assert response.status_code == 200
    session_id = client.cookies.get("session")
    assert session_id
    return session_id


def _use(client: TestClient, session_id: str) -> None:
    client.cookies.clear()
    client.cookies.set("session", session_id)


class TestALogin:
    def test_it_creates_the_hash_with_the_subject(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        session_id = _sign_in(client, "u1", "laptop")

        assert real_redis.keys(f"{test_prefix}:*") == [
            _key(real_redis, test_prefix, session_id)
        ], "a session is one hash and no other key"
        stored = real_redis.hgetall(_key(real_redis, test_prefix, session_id))
        assert stored[FIELD_SUBJECT] == '"u1"'
        assert stored[FIELD_DESCRIPTOR] == '{"device": "laptop"}'
        assert stored["a:user_id"] == '"u1"'
        assert stored["a:device_id"] == '"laptop"'
        assert set(stored) == {
            FIELD_SUBJECT,
            FIELD_DESCRIPTOR,
            FIELD_CREATED,
            FIELD_SEEN,
            "a:user_id",
            "a:device_id",
        }

    def test_it_starts_both_clocks(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))

        assert about(real_redis.ttl(key), IDLE)
        assert about(_httl(real_redis, key, FIELD_CREATED), ABSOLUTE)
        assert _httl(real_redis, key, FIELD_SUBJECT) == NO_TTL
        assert _httl(real_redis, key, "a:user_id") == NO_TTL

    def test_a_nested_attribute_is_one_field_for_each_leaf(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))

        client.post("/cart/120")

        stored = real_redis.hgetall(key)
        assert stored["a:cart.total"] == "120"
        assert stored["a:cart.items[0]"] == '"road"'
        assert stored["a:cart.items[1]"] == '"gravel"'
        assert client.get("/read").json() == {
            "user_id": "u1",
            "device_id": "laptop",
            "cart": {"total": 120, "items": ["road", "gravel"]},
        }

    def test_a_later_write_does_not_move_the_deadline(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))
        real_redis.hexpire(key, 100, FIELD_CREATED)
        real_redis.expire(key, 10)

        client.post("/cart/120")
        client.get("/read")

        assert about(_httl(real_redis, key, FIELD_CREATED), 100)
        assert about(real_redis.ttl(key), IDLE)


class TestACookieThatNamesNoSession:
    """Design test 13, through the app and on the capabilities of the server."""

    UNKNOWN = "A" * 43

    def test_a_read_leaves_no_key(
        self, client: TestClient, real_redis: sync_redis.Redis
    ) -> None:
        _use(client, self.UNKNOWN)

        response = client.get("/read")

        assert response.request.headers.get("cookie") == f"session={self.UNKNOWN}"
        assert response.json() == {}
        assert real_redis.dbsize() == 0

    def test_a_session_that_expired_leaves_no_key(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        session_id = _sign_in(client, "u1", "laptop")
        expire_now(real_redis, _key(real_redis, test_prefix, session_id))

        for _ in range(3):
            assert client.get("/read").json() == {}

        assert real_redis.dbsize() == 0


class TestAnAttributeTtl:
    """``request.session.set(name, value, ttl=...)`` and ``persist``."""

    def test_set_with_a_ttl_gives_the_field_a_ttl(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))

        client.post("/flash")

        assert real_redis.hget(key, "a:flash") == '"x"'
        assert about(_httl(real_redis, key, "a:flash"), 60)
        assert _httl(real_redis, key, "a:user_id") == NO_TTL
        assert about(_httl(real_redis, key, FIELD_CREATED), ABSOLUTE)

    def test_requests_do_not_refresh_the_ttl(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """An attribute TTL does not slide.  A read does not write the field,
        and a save of another attribute does not either."""
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))
        client.post("/flash")
        real_redis.hexpire(key, 20, "a:flash")

        assert client.get("/read").json()["flash"] == "x"
        client.post("/cart/5")

        assert about(_httl(real_redis, key, "a:flash"), 20)

    def test_the_attribute_goes_when_its_ttl_ends(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))
        client.post("/flash")
        assert client.get("/read").json()["flash"] == "x"

        expire_now(real_redis, key, "a:flash")

        assert client.get("/read").json() == {"user_id": "u1", "device_id": "laptop"}

    def test_persist_removes_the_ttl_and_keeps_the_value(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        key = _key(real_redis, test_prefix, _sign_in(client, "u1", "laptop"))
        client.post("/flash")
        assert about(_httl(real_redis, key, "a:flash"), 60)

        client.post("/persist")

        assert _httl(real_redis, key, "a:flash") == NO_TTL
        assert real_redis.hget(key, "a:flash") == '"x"'
        assert client.get("/read").json()["flash"] == "x"

    def test_a_sign_in_keeps_the_time_left_on_the_ttl(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """A sign-in rotates the session.  The rotation is not a write by the
        application, so the attribute keeps the time it had left."""
        client.post("/flash")
        anonymous = client.cookies.get("session")
        old_key = _key(real_redis, test_prefix, anonymous)
        real_redis.hexpire(old_key, 20, "a:flash")

        client.post("/login/u1", headers={"x-device": "laptop"})

        signed_in = client.cookies.get("session")
        assert signed_in != anonymous, "the sign-in did not rotate the ID"
        assert real_redis.exists(old_key) == 0
        new_key = _key(real_redis, test_prefix, signed_in)
        assert real_redis.hget(new_key, "a:flash") == '"x"'
        assert 18 <= _httl(real_redis, new_key, "a:flash") <= 20
        assert about(_httl(real_redis, new_key, FIELD_CREATED), ABSOLUTE)


@requires_query_engine
class TestADeviceList:
    """``SessionStoreDep.list_for_subject`` in a handler."""

    def test_it_returns_the_descriptor_of_each_session(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        laptop = _sign_in(client, "u1", "laptop")
        phone = _sign_in(client, "u1", "phone")
        _sign_in(client, "u2", "tablet")
        # The phone was used last. The test writes the last access itself.
        real_redis.hset(_key(real_redis, test_prefix, laptop), FIELD_SEEN, "1000.0")
        real_redis.hset(_key(real_redis, test_prefix, phone), FIELD_SEEN, "2000.0")
        _use(client, phone)

        listed = client.get("/devices").json()

        assert [row["descriptor"] for row in listed] == [
            {"device": "phone"},
            {"device": "laptop"},
        ]
        assert [row["id"] for row in listed] == [phone, laptop]
        assert [row["this_device"] for row in listed] == [True, False]
        assert client.get("/count").json() == {"count": 2}

    def test_a_request_makes_its_session_the_newest(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """A read-only request records the last access, where the server has
        ``HSETEX``.  The descriptor is not part of ``request.session``."""
        if int(str(real_redis.info("server")["redis_version"]).split(".")[0]) < 8:
            pytest.skip("the server has no HSETEX")
        laptop = _sign_in(client, "u1", "laptop")
        phone = _sign_in(client, "u1", "phone")
        for session_id in (laptop, phone):
            real_redis.hset(
                _key(real_redis, test_prefix, session_id), FIELD_SEEN, "1000.0"
            )

        _use(client, laptop)
        assert client.get("/read").json() == {"user_id": "u1", "device_id": "laptop"}

        assert [row["id"] for row in client.get("/devices").json()] == [laptop, phone]


@requires_query_engine
class TestFindInAHandler:
    """``sessions(searchable={...})`` then ``store.find(...)``."""

    def test_find_by_a_tag(self, client: TestClient) -> None:
        laptop = _sign_in(client, "u1", "laptop")
        _sign_in(client, "u1", "phone")
        other = _sign_in(client, "u2", "laptop")

        assert set(client.get("/find/laptop").json()) == {laptop, other}
        assert client.get("/find/tablet").json() == []

    def test_find_by_a_nested_number(self, client: TestClient) -> None:
        small = _sign_in(client, "u1", "laptop")
        client.post("/cart/20")
        large = _sign_in(client, "u2", "phone")
        client.post("/cart/200")
        _sign_in(client, "u3", "tablet")

        assert client.get("/carts-above/100").json() == {"ids": [large], "count": 1}
        assert set(client.get("/carts-above/10").json()["ids"]) == {small, large}

    def test_a_change_of_the_attribute_changes_the_result(
        self, client: TestClient
    ) -> None:
        session_id = _sign_in(client, "u1", "laptop")
        client.post("/cart/20")
        assert client.get("/carts-above/100").json()["ids"] == []

        client.post("/cart/200")

        assert client.get("/carts-above/100").json()["ids"] == [session_id]


@requires_query_engine
class TestLogOutEverywhere:
    """``revoke_all`` in a handler."""

    def test_it_ends_each_session_of_the_user_and_no_other(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        laptop = _sign_in(client, "u1", "laptop")
        phone = _sign_in(client, "u1", "phone")
        other = _sign_in(client, "u2", "tablet")
        _use(client, phone)

        assert client.post("/logout-everywhere").json() == {"ended": 2}

        for session_id in (laptop, phone):
            assert real_redis.exists(_key(real_redis, test_prefix, session_id)) == 0
            _use(client, session_id)
            assert client.get("/read").json() == {}
        _use(client, other)
        assert client.get("/read").json() == {"user_id": "u2", "device_id": "tablet"}
        assert real_redis.dbsize() == 1, "the sign-out left or made a key"


class TestTheIndexesOfAnApp:
    def test_the_startup_creates_them_where_a_search_can_run(
        self, client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """Before the first request, so the first sessions are indexed as the
        store writes them."""
        if not has_query_engine(real_redis):
            with pytest.raises(sync_redis.ResponseError):
                real_redis.execute_command("FT._LIST")
            return
        names = {
            name
            for name in real_redis.execute_command("FT._LIST")
            if name.startswith(f"{test_prefix}:")
        }
        assert f"{test_prefix}:si" in names
        assert len(names) == 2
