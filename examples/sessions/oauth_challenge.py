"""Sign in with Authlib, and tell an OAuth-aware client why a session was refused.

The app signs users in through an OAuth provider with Authlib and keeps the
result in a server-side session (the "backend for frontend" pattern).  A
refused request answers 401, and a client library can only react well if it
knows *why*:

* no session, or an expired one: run the sign-in flow again
  (RFC 6750 ``error="invalid_token"``);
* a session too old for a sensitive route: sign in again *now*, with
  ``max_age`` (RFC 9470, step-up authentication).

``valid_session()`` passes the reason to ``challenge``, which turns it into the
``WWW-Authenticate`` header such a client reads.

There is no real identity provider.  ``/mock-idp/authorize`` stands in for its
sign-in page, and an ``httpx.MockTransport`` answers the token and user-info
calls Authlib makes.  Everything else - the ``state`` check, the code exchange,
the session - is the real thing.

Run it (plain HTTP, so the cookie must not be ``Secure``)::

    export REDIS_URL=redis://localhost:6379/0 REDIS_SESSION_COOKIE_HTTPS_ONLY=false
    uv run --with "fastapi[standard]" --with authlib fastapi dev examples/sessions/oauth_challenge.py

    curl -i localhost:8000/me
    # 401, www-authenticate: Bearer realm="demo"
    curl -L -c jar -b jar localhost:8000/login    # provider -> /callback
    curl -b jar localhost:8000/settings           # 200 for 10 s, then:
    # 401, www-authenticate: Bearer realm="demo",
    #      error="insufficient_user_authentication", max_age=10
    curl -L -c jar -b jar "localhost:8000/login?max_age=10"   # step up
    curl -b jar localhost:8000/settings           # 200 again
"""

from __future__ import annotations

import httpx
from authlib.integrations.starlette_client import OAuth, OAuthError
from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import RedirectResponse

from redis_fastapi import FastAPIRedis, SessionDep, SessionStoreDep, valid_session
from redis_fastapi.deps import SessionStateDep

RECENT = 10  # seconds; short so the step-up is easy to see


def challenge(request: Request, reason: str) -> str | None:
    if reason == "unavailable":
        return None  # Redis is down: retry later, do not send users to sign in
    if reason == "stale":
        return (
            'Bearer realm="demo", error="insufficient_user_authentication", '
            f"max_age={RECENT}"
        )
    if reason == "expired":
        return 'Bearer realm="demo", error="invalid_token"'
    return 'Bearer realm="demo"'  # "missing": no credentials, so no error code


def mock_provider(request: httpx.Request) -> httpx.Response:
    """The provider's back channel: the token and user-info endpoints."""
    if request.url.path == "/token":
        return httpx.Response(200, json={"access_token": "t", "token_type": "Bearer"})
    return httpx.Response(200, json={"sub": "alice"})  # /userinfo


oauth = OAuth()
oauth.register(
    "idp",
    client_id="demo",
    client_secret="demo",
    authorize_url="/mock-idp/authorize",
    access_token_url="https://idp.example/token",
    userinfo_endpoint="https://idp.example/userinfo",
    client_kwargs={"transport": httpx.MockTransport(mock_provider)},
)

app = FastAPI()
FastAPIRedis(app).lifespan().sessions(challenge=challenge)


@app.get("/login")
async def login(request: Request, max_age: int | None = None) -> RedirectResponse:
    # Authlib keeps the OAuth state in request.session - that is, in Redis.
    extra = {"max_age": max_age} if max_age is not None else {}
    redirect_uri = request.url_for("callback")
    return await oauth.idp.authorize_redirect(request, redirect_uri, **extra)


@app.get("/mock-idp/authorize")
async def mock_idp_authorize(redirect_uri: str, state: str) -> RedirectResponse:
    # A real provider would ask the user to sign in, again if max_age demands it.
    return RedirectResponse(f"{redirect_uri}?code=demo&state={state}")


@app.get("/callback")
async def callback(
    request: Request,
    session: SessionDep,
    state: SessionStateDep,
    store: SessionStoreDep,
) -> RedirectResponse:
    try:
        token = await oauth.idp.authorize_access_token(request)  # checks state
    except OAuthError as exc:  # a forged or replayed callback
        raise HTTPException(400, exc.error) from exc
    userinfo = await oauth.idp.userinfo(token=token)
    session["user_id"] = userinfo["sub"]
    # Every completed sign-in is a fresh authentication, even when the user is
    # the same one - which is what makes a step-up satisfy issued_within.
    await store.reauthenticate(state)
    return RedirectResponse("/me")


@app.get("/me", dependencies=[Depends(valid_session())])
async def me(session: SessionDep) -> dict[str, str]:
    return {"user_id": session["user_id"]}


@app.get("/settings", dependencies=[Depends(valid_session(issued_within=RECENT))])
async def settings(session: SessionDep) -> dict[str, str | bool]:
    return {"user_id": session["user_id"], "can_change_password": True}
