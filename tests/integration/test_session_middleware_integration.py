"""What the middleware leaves in a real Redis after a request.

Two defects that only show as keys in the server: a sign-out after idle expiry
that minted a new empty session, and a rotation to an anonymous session that
kept the subject of the user who left.
"""

import pytest
import redis as sync_redis
from fastapi import FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep
from redis_fastapi.session_layout import FIELD_SUBJECT
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import about, expire_now, requires_redis

pytestmark = [pytest.mark.integration, requires_redis]

IDLE = 60
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
        session["user_id"] = "u1"
        return {}

    @application.post("/logout")
    async def logout(session: SessionDep) -> dict:
        session.clear()
        return {}

    @application.post("/anonymous")
    async def anonymous(session: SessionDep) -> dict:
        session["user_id"] = None
        return {}

    yield application
    get_settings.cache_clear()


def _session_keys(redis: sync_redis.Redis, prefix: str) -> list[str]:
    return list(redis.scan_iter(match=f"{prefix}*:s:*"))


class TestSigningOutWithNoStoredSession:
    """``session.clear()`` is the documented sign-out; it must not sign in."""

    def test_with_no_cookie_nothing_is_written(
        self, app: FastAPI, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        with TestClient(app) as client:
            response = client.post("/logout")
        assert "set-cookie" not in response.headers
        assert _session_keys(real_redis, test_prefix) == []

    def test_after_idle_expiry_nothing_is_written(
        self, app: FastAPI, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        with TestClient(app) as client:
            client.post("/login")
            assert client.cookies.get("session")
            (key,) = _session_keys(real_redis, test_prefix)
            assert about(real_redis.ttl(key), IDLE)
            expire_now(real_redis, key)

            response = client.post("/logout")
            assert response.request.headers.get("cookie") is not None
        # The stale cookie is cleared, and no new session is created.
        assert "max-age=0" in response.headers["set-cookie"].lower()
        assert _session_keys(real_redis, test_prefix) == []


class TestRotatingToAnAnonymousSession:
    def test_the_new_session_does_not_name_the_old_subject(
        self, app: FastAPI, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        """The subject is the field ``s:sub`` of the session's own hash.

        A search by subject reads that field, so an anonymous session that
        kept it would be listed, and ended, as a session of the user who
        left.
        """
        with TestClient(app) as client:
            client.post("/login")
            signed_in = client.cookies.get("session")
            (key,) = _session_keys(real_redis, test_prefix)
            assert real_redis.hget(key, FIELD_SUBJECT) == '"u1"'

            client.post("/anonymous")
            anonymous = client.cookies.get("session")

        assert anonymous and anonymous != signed_in, "the ID did not rotate"
        keys = _session_keys(real_redis, test_prefix)
        assert len(keys) == 1 and keys[0].endswith(anonymous), keys
        assert real_redis.hexists(keys[0], FIELD_SUBJECT) is False, (
            "the anonymous session kept the subject of the user who left it"
        )
        assert real_redis.hget(keys[0], "a:user_id") == "null"
