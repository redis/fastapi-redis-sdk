"""``valid_session()`` against a real Redis server.

The unit suite drives every branch against ``fakeredis``. These tests prove
what only a real server can: that the reasons follow Redis's own expiry, that
``issued_within`` measures the server's countdown, and that the gate and
``cache()`` agree about what reaches Redis.
"""

from __future__ import annotations

import time
from collections.abc import Iterator
from typing import Any

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response
from fastapi.testclient import TestClient

from redis_fastapi import cache, valid_session
from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep, SessionStateDep, SessionStoreDep
from redis_fastapi.exceptions import SessionConfigurationError
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
    FastAPIRedis(app).lifespan().caching().sessions()

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

    @app.get(
        "/right-order",
        dependencies=[
            Depends(valid_session()),
            Depends(cache(ttl=300, vary_on_session=False)),
        ],
    )
    async def right_order() -> dict:
        return {"items": [1]}

    @app.get(
        "/wrong-order",
        dependencies=[
            Depends(cache(ttl=300, vary_on_session=False)),
            Depends(valid_session()),
        ],
    )
    async def wrong_order() -> dict:
        return {"items": [1]}

    return app


@pytest.fixture()
def client(real_redis, test_prefix: str, monkeypatch) -> Iterator[TestClient]:
    app = _build(monkeypatch, test_prefix, idle=2, absolute=600)
    with TestClient(app) as test_client:
        yield test_client
    get_settings.cache_clear()


def _cache_keys(redis: sync_redis.Redis, prefix: str) -> list[Any]:
    return list(redis.scan_iter(match=f"{prefix}:cache*"))


def test_a_live_session_passes(client: TestClient) -> None:
    client.post("/login")
    response = client.get("/gated")
    assert response.status_code == 200
    assert response.headers["cache-control"] == "private"


def test_no_cookie_is_missing(client: TestClient) -> None:
    assert client.get("/gated").json() == {"reason": "missing"}


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


def test_the_right_order_keeps_one_shared_entry_behind_private(
    client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
) -> None:
    client.post("/login")
    first = client.get("/right-order")
    second = client.get("/right-order")
    assert first.headers["x-redis-cache"] == "MISS"
    assert second.headers["x-redis-cache"] == "HIT"
    assert first.headers["cache-control"].startswith("private")
    assert second.headers["cache-control"].startswith("private")
    assert len(_cache_keys(real_redis, test_prefix)) == 1

    client.cookies.clear()
    anonymous = client.get("/right-order")
    assert anonymous.status_code == 401
    assert "x-redis-cache" not in anonymous.headers


def test_the_wrong_order_never_reaches_redis(
    client: TestClient, real_redis: sync_redis.Redis, test_prefix: str
) -> None:
    """The misordered route never succeeds, so no entry exists for a hit."""
    client.post("/login")
    for _ in range(2):
        with pytest.raises(SessionConfigurationError, match="must come before"):
            client.get("/wrong-order")
    assert _cache_keys(real_redis, test_prefix) == []
