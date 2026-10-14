"""``valid_session()`` against a real Redis server.

The unit suite drives every branch against ``fakeredis``. These tests prove
what only a real server can: that the reasons follow Redis's own expiry, and
that ``issued_within`` measures the server's countdown.
"""

from __future__ import annotations

import time
from collections.abc import Iterator

import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient

from redis_fastapi import valid_session
from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, SessionStateDep, SessionStoreDep
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

pytestmark = [pytest.mark.integration, requires_redis]


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
    app = _build(monkeypatch, test_prefix, idle=2, absolute=600)
    with TestClient(app) as test_client:
        yield test_client
    get_settings.cache_clear()


def test_a_live_session_passes(client: TestClient) -> None:
    client.post("/login")
    response = client.get("/gated")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private"


def test_the_server_s_idle_expiry_reads_as_expired(client: TestClient) -> None:
    """Redis removed the payload on its own; the cookie still names it."""
    client.post("/login")
    time.sleep(3)
    assert client.get("/gated").json() == {"reason": "expired"}


def test_the_server_s_countdown_makes_a_session_stale(
    real_redis, test_prefix: str, monkeypatch
) -> None:
    """``issued_within`` reads the key's real TTL, not the application clock.

    A long idle clock, so the wait below ages the ID without ending the
    session.
    """
    app = _build(monkeypatch, test_prefix, idle=60, absolute=600)
    with TestClient(app) as client:
        client.post("/login")
        assert client.get("/recent").status_code == 200
        time.sleep(2.2)
        stale = client.get("/recent")
        assert stale.json() == {"reason": "stale"}
        assert stale.headers["cache-control"] == "no-store"

        client.post("/reauth")
        assert client.get("/recent").status_code == 200, (
            "reauthenticate() issues a new ID, so the session is recent again"
        )
    get_settings.cache_clear()
