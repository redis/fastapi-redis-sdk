"""``valid_session()`` against a real Redis server.

The unit suite drives every branch against ``fakeredis``. These tests prove
what only a real server can: that the reasons follow Redis's own expiry, and
that ``issued_within`` measures the server's countdown.

No test waits for a TTL. Each one checks the TTL the store set, then moves
that TTL in Redis.
"""

from __future__ import annotations

from collections.abc import Iterator

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient

from redis_fastapi import valid_session
from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, SessionStateDep, SessionStoreDep
from redis_fastapi.session_layout import FIELD_CREATED
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import about, expire_now, requires_redis

pytestmark = [pytest.mark.integration, requires_redis]

IDLE = 60
ABSOLUTE = 600


def _session_key(redis: sync_redis.Redis, client: TestClient) -> str:
    (key,) = redis.scan_iter(match=f"*:s:{client.cookies.get('session')}")
    return key


def _httl(redis: sync_redis.Redis, key: str) -> int:
    return int(redis.execute_command("HTTL", key, "FIELDS", 1, FIELD_CREATED)[0])


def _reason_of(request: Request, reason: str) -> Response:
    return JSONResponse({"reason": reason}, status_code=401)


def _build(monkeypatch, prefix: str, *, idle: int, absolute: int) -> FastAPI:
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    monkeypatch.setenv("REDIS_PREFIX", prefix)
    monkeypatch.setenv("REDIS_SESSION_IDLE_TTL", str(idle))
    monkeypatch.setenv("REDIS_SESSION_ABSOLUTE_TTL", str(absolute))
    get_settings.cache_clear()

    app = FastAPI()
    FastAPIRedis(app).lifespan().sessions()

    @app.post("/login")
    async def login(session: SessionDep) -> dict:
        session["user_id"] = 42
        return {}

    @app.post("/reauth")
    async def reauth(state: SessionStateDep, store: SessionStoreDep) -> dict:
        await store.reauthenticate(state)
        return {}

    @app.get("/gated", dependencies=[Depends(valid_session(on_reject=_reason_of))])
    async def gated() -> dict:
        return {"ok": True}

    @app.get(
        "/recent",
        dependencies=[Depends(valid_session(issued_within=1, on_reject=_reason_of))],
    )
    async def recent() -> dict:
        return {"ok": True}

    return app


@pytest.fixture()
def client(real_redis, test_prefix: str, monkeypatch) -> Iterator[TestClient]:
    app = _build(monkeypatch, test_prefix, idle=IDLE, absolute=ABSOLUTE)
    with TestClient(app) as test_client:
        yield test_client
    get_settings.cache_clear()


def test_a_live_session_passes(client: TestClient) -> None:
    client.post("/login")
    response = client.get("/gated")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private"


def test_the_server_s_idle_expiry_reads_as_expired(
    client: TestClient, real_redis: sync_redis.Redis
) -> None:
    """Redis removed the key on its own; the cookie still names it."""
    client.post("/login")
    key = _session_key(real_redis, client)
    assert about(real_redis.ttl(key), IDLE)

    expire_now(real_redis, key)

    assert client.get("/gated").json() == {"reason": "expired"}


def test_the_server_s_deadline_reads_as_expired(
    client: TestClient, real_redis: sync_redis.Redis
) -> None:
    """Only ``s:created`` expired. The key is there, and it is not a session."""
    client.post("/login")
    key = _session_key(real_redis, client)
    assert about(_httl(real_redis, key), ABSOLUTE)

    expire_now(real_redis, key, FIELD_CREATED)
    assert real_redis.exists(key) == 1

    assert client.get("/gated").json() == {"reason": "expired"}
    assert real_redis.exists(key) == 0


def test_the_server_s_countdown_makes_a_session_stale(
    client: TestClient, real_redis: sync_redis.Redis
) -> None:
    """``issued_within`` reads the ``HTTL`` of ``s:created``, not the
    application clock.

    The test takes five seconds off the deadline in Redis. That ages the ID
    by five seconds and does not end the session.
    """
    client.post("/login")
    assert client.get("/recent").status_code == 200
    key = _session_key(real_redis, client)
    assert about(_httl(real_redis, key), ABSOLUTE)

    real_redis.hexpire(key, ABSOLUTE - 5, FIELD_CREATED)

    stale = client.get("/recent")
    assert stale.json() == {"reason": "stale"}
    assert stale.headers["cache-control"] == "no-store"
    assert client.get("/gated").status_code == 200, "the session itself is alive"

    client.post("/reauth")
    assert client.get("/recent").status_code == 200, (
        "reauthenticate() issues a new ID, so the session is recent again"
    )
    assert about(_httl(real_redis, _session_key(real_redis, client)), ABSOLUTE)
