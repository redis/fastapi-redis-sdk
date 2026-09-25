"""The cookie must not expire before its record, against a real server.

Research §6 of ``session-di-factory-research.md``: the load refreshes the idle
clock in Redis on every request, but the cookie is sent only on writes. When
the cookie's ``Max-Age`` followed the idle clock, a user who only read lost the
cookie while the record was alive. ``Max-Age`` now follows the absolute clock,
which never slides, and Redis alone enforces the idle clock.

``TestClient`` keeps cookies in an ``http.cookiejar`` jar, which drops a cookie
once its ``Max-Age`` has passed on the real clock - as a browser does. So these
tests use short TTLs and real sleeps rather than a controlled clock.
"""

import time

import pytest
import redis as sync_redis
from fastapi import FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep
from redis_fastapi.session_backend import FIELD_ABSOLUTE, FIELD_DATA
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

pytestmark = [pytest.mark.integration, requires_redis]

IDLE = 6
ABSOLUTE = 600


@pytest.fixture()
def app(real_redis: sync_redis.Redis, test_prefix: str, monkeypatch) -> FastAPI:
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    monkeypatch.setenv("REDIS_PREFIX", test_prefix)
    monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", str(IDLE))
    monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", str(ABSOLUTE))
    get_settings.cache_clear()

    application = FastAPI()
    FastAPIRedis(application).lifespan().sessions()

    @application.post("/login")
    async def login(session: SessionDep) -> dict:
        session["user_id"] = 42
        return {}

    @application.get("/read")
    async def read(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    @application.post("/write")
    async def write(session: SessionDep) -> dict:
        session["n"] = session.get("n", 0) + 1
        return {"user_id": session.get("user_id")}

    yield application
    get_settings.cache_clear()


def _session_key(redis: sync_redis.Redis, session_id: str) -> str:
    keys = list(redis.scan_iter(match=f"*session:{session_id}"))
    assert len(keys) == 1, keys
    return keys[0]


def _httl(redis: sync_redis.Redis, key: str, field: str) -> int:
    return int(redis.execute_command("HTTL", key, "FIELDS", 1, field)[0])


def _max_age(response) -> int:
    for part in response.headers["set-cookie"].split(";"):
        name, _, value = part.strip().partition("=")
        if name.lower() == "max-age":
            return int(value)
    raise AssertionError("no Max-Age on the cookie")


def _sign_in(client: TestClient) -> str:
    response = client.post("/login")
    # Rotation starts a new absolute clock, so the cookie gets all of it.
    assert _max_age(response) == ABSOLUTE
    session_id = client.cookies.get("session")
    assert session_id
    return session_id


def test_a_reading_user_stays_signed_in(
    app: FastAPI, real_redis: sync_redis.Redis
) -> None:
    """Read-only requests, each well inside the idle window.

    Every request arrives less than ``IDLE`` seconds after the previous one,
    so the user was never idle and must still be signed in at the end.
    """
    with TestClient(app) as client:
        session_id = _sign_in(client)
        key = _session_key(real_redis, session_id)

        for _ in range(4):
            time.sleep(1)
            response = client.get("/read")
            assert response.json() == {"user_id": 42}
            assert "set-cookie" not in response.headers
            # The load reset the idle clock in Redis.
            assert _httl(real_redis, key, FIELD_DATA) >= IDLE - 1

        # Last request was 4 s after sign-in; wait 3 s more. The user has been
        # idle for 3 s, inside the 6 s window, but 7 s have passed since the
        # only Set-Cookie.
        time.sleep(3)

        # The record is alive in Redis ...
        assert _httl(real_redis, key, FIELD_DATA) > 0
        assert _httl(real_redis, key, FIELD_ABSOLUTE) > 0

        response = client.get("/read")
        sent_cookie = response.request.headers.get("cookie")

        # ... and the same ID still works when sent by hand ...
        by_hand = client.get("/read", headers={"cookie": f"session={session_id}"})
        assert by_hand.json() == {"user_id": 42}, "the session itself is alive"

        # ... so the user must still be signed in.
        assert sent_cookie is not None, (
            "the client dropped the cookie while the record was alive"
        )
        assert response.json() == {"user_id": 42}


def test_control_a_writing_user_stays_signed_in(
    app: FastAPI, real_redis: sync_redis.Redis
) -> None:
    """The same schedule with writes: each write resends the cookie.

    Shows that the harness honors ``Max-Age`` correctly, so a failure of the
    test above comes from the middleware and not from the client.
    """
    with TestClient(app) as client:
        _sign_in(client)
        for _ in range(4):
            time.sleep(1)
            response = client.post("/write")
            # The absolute remainder: never refreshed, so it only shrinks.
            assert ABSOLUTE - 10 <= _max_age(response) <= ABSOLUTE
        time.sleep(3)
        response = client.get("/read")
        assert response.request.headers.get("cookie") is not None
        assert response.json() == {"user_id": 42}


def test_an_idle_user_is_signed_out_although_the_cookie_lives(
    app: FastAPI, real_redis: sync_redis.Redis
) -> None:
    """The cookie now outlives the idle clock, so Redis must enforce it.

    After ``IDLE`` seconds with no request the client still holds the cookie
    and sends it, but the record's idle field has expired: the session is
    empty, and the load deletes the half-dead key.
    """
    with TestClient(app) as client:
        session_id = _sign_in(client)
        key = _session_key(real_redis, session_id)

        time.sleep(IDLE + 1)
        assert _httl(real_redis, key, FIELD_DATA) == -2
        assert _httl(real_redis, key, FIELD_ABSOLUTE) > 0

        response = client.get("/read")
        assert response.request.headers.get("cookie") == f"session={session_id}"
        assert response.json() == {"user_id": None}
        assert real_redis.exists(key) == 0, "the half-dead key was not deleted"
