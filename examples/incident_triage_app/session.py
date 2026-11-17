"""Sign in, sign out, and the session subject.

The subject - the handle an engineer signs in with - is the identity for two
other features: it names the run scope, which is part of every stream's name,
so an engineer reaches only their own streams, and it is the rate-limit
identifier for the run quota.
``subject_of`` is the access check on every stream endpoint.
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel, Field

from examples.incident_triage_app.health import require_sessions
from redis_fastapi import CannotIdentifyClient, SessionDep, SessionStoreDep
from redis_fastapi.sessions import session_of

router = APIRouter(prefix="/api", tags=["sessions"])


class SignIn(BaseModel):
    handle: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,31}$")


def subject_of(session: SessionDep) -> str:
    """The signed-in subject, or a 401."""
    subject = session.get("user_id")
    if not subject:
        raise HTTPException(401, "Not signed in")
    return str(subject)


def by_subject(request: Request) -> str:
    """Rate-limit identifier: the subject, never the IP.

    ``ip_identifier`` ignores ``X-Forwarded-For``, so behind a proxy every
    engineer would share one counter.
    """
    subject = session_of(request).get("user_id")
    if not subject:
        raise CannotIdentifyClient("no signed-in subject")
    return str(subject)


@router.post("/session", dependencies=[Depends(require_sessions)])
async def sign_in(body: SignIn, session: SessionDep) -> dict[str, str]:
    """No password: this is a demo.  Setting ``user_id`` rotates the session ID."""
    session["user_id"] = body.handle
    return {"subject": body.handle}


@router.delete("/session")
async def sign_out(session: SessionDep) -> dict[str, bool]:
    """Sign this device out: the page does it when a window closes."""
    session.clear()
    return {"signed_out": True}


@router.delete("/sessions")
async def sign_out_everywhere(
    session: SessionDep, store: SessionStoreDep
) -> dict[str, int]:
    subject = subject_of(session)
    revoked = await store.revoke_all(subject)
    session.clear()  # this device too: the middleware clears its cookie
    return {"revoked": revoked}
