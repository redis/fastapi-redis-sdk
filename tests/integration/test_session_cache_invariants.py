"""Integration tests for the caching / session cross-section, against real Redis.

Four cases, one per row of the declaration table, each asserting **both**
halves: what this library stores, and what the response tells everything
downstream it may store. The two must agree, which is the whole point.

The invariants under test:

* **N-18** - the emitted directives are never more permissive than the caching
  this library performs for that response.
* **N-10 (revised)** - a response whose content *depends* on the session is not
  stored by any shared cache, ours or downstream.
* **F-10 (revised)** - ``Vary: Cookie`` is emitted whenever the response may
  vary by cookie; not whenever the session merely happened to be read.
* **No cross-user leak** - the property all three exist to protect.

These are integration rather than unit tests because the leak is only
observable end to end: it needs a real store, two real clients, two real
cookies and a real second request.
"""

import pytest
import redis as sync_redis
from fastapi import Depends, FastAPI
from fastapi.testclient import TestClient

from redis_fastapi.cache import cache
from redis_fastapi.config import get_settings
from redis_fastapi.deps import SessionDep
from redis_fastapi.setup import FastAPIRedis
from tests.conftest import requires_redis

pytestmark = [pytest.mark.integration, requires_redis]


@pytest.fixture()
def app(real_redis: sync_redis.Redis, test_prefix: str, monkeypatch):
    """A real app on a real pool.

    The pool is built by the real lifespan rather than injected, because
    ``TestClient`` runs its own event loop and an async client created on the
    fixture's loop cannot be shared with it.
    """
    get_settings.cache_clear()
    monkeypatch.setenv("REDIS_SESSION_COOKIE_HTTPS_ONLY", "false")
    monkeypatch.setenv("REDIS_PREFIX", test_prefix)
    get_settings.cache_clear()

    application = FastAPI()
    FastAPIRedis(application).lifespan().caching().sessions()

    async def require_user(session: SessionDep) -> int | None:
        """An auth guard: reads the session, contributes nothing to the body."""
        return session.get("user_id")

    @application.post("/login/{uid}")
    async def login(uid: int, session: SessionDep) -> dict:
        session["user_id"] = uid
        return {"ok": True}

    # Row 1 - the body depends on who is asking.
    @application.get(
        "/me",
        dependencies=[Depends(cache(ttl=300, vary_on_session=True))],
    )
    async def me(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    # Row 2 - reads the session, body identical for everyone.
    @application.get(
        "/catalogue",
        dependencies=[
            Depends(require_user),
            Depends(cache(ttl=300, vary_on_session=False)),
        ],
    )
    async def catalogue() -> dict:
        return {"products": ["a", "b"]}

    # Row 3 - never touches the session.
    @application.get("/status", dependencies=[Depends(cache(ttl=300))])
    async def status() -> dict:
        return {"ok": True}

    # Row 4 - reads the session, declares nothing.
    @application.get("/undeclared", dependencies=[Depends(cache(ttl=300))])
    async def undeclared(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    # No cache() at all, but the session is read.
    @application.get("/profile")
    async def profile(session: SessionDep) -> dict:
        return {"user_id": session.get("user_id")}

    yield application
    get_settings.cache_clear()


class _User:
    """One signed-in user, sharing a single ``TestClient``.

    Two clients would mean two lifespans on one app, each replacing the
    other's connection pool - and the pool is bound to the loop that created
    it. So the users are cookie jars, not clients.
    """

    def __init__(self, client: TestClient, cookies: dict[str, str]) -> None:
        self._client = client
        self._cookies = cookies

    def get(self, path: str):
        # Set on the client rather than per request: httpx deprecated the
        # per-request form because its persistence behaviour is ambiguous.
        self._client.cookies.clear()
        self._client.cookies.update(self._cookies)
        return self._client.get(path)


@pytest.fixture()
def client(app: FastAPI):
    with TestClient(app) as test_client:
        yield test_client


def _sign_in(client: TestClient, uid: int) -> _User:
    client.cookies.clear()
    client.post(f"/login/{uid}")
    cookies = {"session": client.cookies["session"]}
    client.cookies.clear()
    return _User(client, cookies)


@pytest.fixture()
def alice(client: TestClient) -> _User:
    return _sign_in(client, 1)


@pytest.fixture()
def bob(client: TestClient) -> _User:
    return _sign_in(client, 2)


def _directives(response) -> set[str]:
    raw = response.headers.get("cache-control", "")
    return {part.strip() for part in raw.split(",") if part.strip()}


class TestRow1PerUser:
    """`vary_on_session=True` - the body depends on who is asking."""

    def test_each_user_gets_their_own_body(self, alice, bob) -> None:
        assert alice.get("/me").json() == {"user_id": 1}
        assert bob.get("/me").json() == {"user_id": 2}

    def test_each_user_still_gets_a_cache_hit(self, alice, bob) -> None:
        """Per-user keying must not mean per-user cache misses forever."""
        alice.get("/me")
        bob.get("/me")
        assert alice.get("/me").headers["x-redis-cache"] == "HIT"
        assert bob.get("/me").headers["x-redis-cache"] == "HIT"
        assert alice.get("/me").json() == {"user_id": 1}

    def test_n18_the_directive_is_no_more_permissive_than_our_key(self, alice) -> None:
        """We store per subject, so a shared cache must not store at all.

        Without `private` a CDN keeps one copy for everyone and recreates the
        leak the per-user key just closed, one hop further out.
        """
        for response in (alice.get("/me"), alice.get("/me")):  # miss, then hit
            assert "private" in _directives(response)
            assert "max-age=300" in _directives(response)

    def test_n18_holds_on_the_hit_path_too(self, alice) -> None:
        """The hit path is where a session-emitted header could not reach.

        A hit short-circuits before the endpoint runs, so the session
        middleware contributes nothing; the directive has to come from the
        route's own declaration.
        """
        alice.get("/me")
        hit = alice.get("/me")
        assert hit.headers["x-redis-cache"] == "HIT"
        assert "private" in _directives(hit)

    def test_f10_vary_is_emitted(self, alice) -> None:
        """`private` stops shared caches; `Vary` stops the browser reusing a
        previous user's copy across a sign-out and sign-in."""
        assert "Cookie" in alice.get("/me").headers.get("vary", "")


class TestRow2SharedButSessionReading:
    """`vary_on_session=False` - reads the session, body identical to all."""

    def test_one_shared_entry_serves_everyone(self, alice, bob) -> None:
        first = alice.get("/catalogue")
        second = bob.get("/catalogue")
        assert first.headers["x-redis-cache"] == "MISS"
        assert second.headers["x-redis-cache"] == "HIT"
        assert first.json() == second.json()

    def test_n18_public_is_exactly_as_permissive_as_our_own_entry(self, alice) -> None:
        """The invariant must not push us to `private` here.

        We keep one shared entry, so a shared cache keeping one is an equal
        permission, not a greater one. A blanket "sessions imply private" rule
        would fail this row and cost hit rate for no safety gain.
        """
        directives = _directives(alice.get("/catalogue"))
        assert "private" not in directives
        assert "no-store" not in directives
        assert "max-age=300" in directives

    def test_f10_vary_is_suppressed(self, alice) -> None:
        """The session was read, so the middleware would add `Vary: Cookie`.

        Keeping it would force a shared cache to store one copy per user of a
        payload identical to all of them - which is exactly the cost this
        declaration exists to avoid.
        """
        assert "Cookie" not in alice.get("/catalogue").headers.get("vary", "")

    def test_n10_revised_this_row_is_deliberately_out_of_scope(
        self, alice, bob
    ) -> None:
        """A session-*bearing* response that is not session-*dependent*.

        Under N-10's original wording this row was forbidden. The revision is
        what allows it, and this is the test that pins the distinction.
        """
        assert alice.get("/catalogue").json() == bob.get("/catalogue").json()
        assert "no-store" not in _directives(bob.get("/catalogue"))


class TestRow3NoSessionAtAll:
    def test_shared_and_public_as_before(self, alice, bob) -> None:
        assert alice.get("/status").headers["x-redis-cache"] == "MISS"
        assert bob.get("/status").headers["x-redis-cache"] == "HIT"

    def test_no_vary_and_no_private(self, alice) -> None:
        """Nothing accessed the session, so nothing adds either header."""
        response = alice.get("/status")
        assert "Cookie" not in response.headers.get("vary", "")
        assert "private" not in _directives(response)

    def test_declaring_nothing_is_the_same_declaration_as_row_four(self, alice) -> None:
        """Rows 3 and 4 omit the parameter; the runtime tells them apart.

        The difference is not configuration, it is whether the endpoint turned
        out to read the session.
        """
        assert alice.get("/status").headers["x-redis-cache"] == "MISS"
        assert "x-redis-cache" not in alice.get("/undeclared").headers


class TestRow4UndeclaredIsNotStored:
    """The safety net. The only behaviour change to existing code."""

    def test_no_cross_user_leak(self, alice, bob) -> None:
        """The property the whole design exists for.

        Before the guard, Bob received Alice's body from our own Redis for the
        full TTL.
        """
        assert alice.get("/undeclared").json() == {"user_id": 1}
        assert bob.get("/undeclared").json() == {"user_id": 2}

    def test_nothing_is_stored(self, alice) -> None:
        alice.get("/undeclared")
        second = alice.get("/undeclared")
        assert second.headers.get("x-redis-cache") != "HIT"
        assert "etag" not in second.headers

    def test_the_entry_never_reaches_redis(
        self, alice, real_redis: sync_redis.Redis, test_prefix: str
    ) -> None:
        alice.get("/undeclared")
        assert real_redis.keys(f"*{test_prefix}*undeclared*") == []

    def test_n18_the_directive_matches_our_refusal_to_store(self, alice) -> None:
        """We store nothing, so nothing downstream may store it either.

        Emitting `max-age=300` here would say "too dangerous for me to cache,
        but you go ahead" - the starkest possible violation of N-18.
        """
        directives = _directives(alice.get("/undeclared"))
        assert "no-store" in directives
        assert "private" in directives
        assert not any(d.startswith("max-age") for d in directives)

    def test_f10_vary_is_kept(self, alice) -> None:
        """We do not know whether the body varies, so we say it might."""
        assert "Cookie" in alice.get("/undeclared").headers.get("vary", "")

    def test_it_warns_once_naming_the_route(self, alice, caplog) -> None:
        from redis_fastapi.cache import _WARNED_ROUTES

        _WARNED_ROUTES.clear()
        with caplog.at_level("WARNING"):
            alice.get("/undeclared")
            alice.get("/undeclared")
        warnings = [r for r in caplog.records if "vary_on_session" in r.message]
        assert len(warnings) == 1
        assert "/undeclared" in warnings[0].getMessage()


class TestASessionRouteWithNoCacheAtAll:
    def test_the_middleware_emits_private_itself(self, alice) -> None:
        """Nothing else will, on a route `cache()` does not own."""
        response = alice.get("/profile")
        assert "private" in _directives(response)
        assert "Cookie" in response.headers.get("vary", "")

    def test_only_one_cache_control_header_is_emitted(self, alice) -> None:
        """Two writers produced `max-age=300, private, no-store` in one
        response. There must be exactly one."""
        for path in ("/profile", "/me", "/catalogue", "/undeclared"):
            response = alice.get(path)
            assert len(response.headers.get_list("cache-control")) <= 1, path

    def test_vary_is_merged_not_duplicated(self, alice) -> None:
        for path in ("/profile", "/me"):
            assert len(alice.get(path).headers.get_list("vary")) == 1, path
