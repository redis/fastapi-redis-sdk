"""CSRF protection for a cookie session: the synchronizer token pattern.

The browser attaches the session cookie to every request to this origin,
including one a form on somebody else's page submits.  ``SameSite=Lax`` (the
default) withholds it on a cross-site ``POST``; the token is the defence in
depth for the browsers and cases ``Lax`` does not cover.

* ``GET /form`` hands out the token kept in the session.
* ``POST /transfer`` refuses a request whose token does not match.
* ``POST /login`` drops the token, so a sign-in gets a fresh one.  Rotation
  carries the session's data to the new ID, the token included.

Never change state in a ``GET``: ``Lax`` sends the cookie on a cross-site
top-level ``GET``, and no token check runs there.

Run it (plain HTTP, so the cookie must not be ``Secure``)::

    export REDIS_URL=redis://localhost:6379/0 REDIS_SESSION_COOKIE_HTTPS_ONLY=false
    uv run --with "fastapi[standard]" --with python-multipart fastapi dev examples/sessions/csrf.py

    curl -c jar -b jar -X POST localhost:8000/login
    TOKEN=$(curl -s -b jar localhost:8000/form | python -c 'import json,sys;print(json.load(sys.stdin)["csrf_token"])')
    curl -b jar -X POST -d amount=10 localhost:8000/transfer                     # 403
    curl -b jar -X POST -d amount=10 -d csrf_token=$TOKEN localhost:8000/transfer # 200
"""

from __future__ import annotations

import secrets

from fastapi import FastAPI, Form, HTTPException

from redis_fastapi import FastAPIRedis, SessionDep

app = FastAPI()
FastAPIRedis(app).lifespan().sessions()


@app.post("/login")
async def login(session: SessionDep) -> dict[str, bool]:
    session["user_id"] = "alice"
    session.pop("csrf", None)  # the next /form mints a token for this sign-in
    return {"signed_in": True}


@app.get("/form")
async def form(session: SessionDep) -> dict[str, str]:
    token: str = session.setdefault("csrf", secrets.token_urlsafe(32))
    return {"csrf_token": token}


@app.post("/transfer")
async def transfer(
    session: SessionDep, amount: int = Form(), csrf_token: str = Form("")
) -> dict[str, int]:
    expected = session.get("csrf")
    if not expected or not secrets.compare_digest(csrf_token, expected):
        raise HTTPException(403, "CSRF token mismatch")
    return {"transferred": amount}
