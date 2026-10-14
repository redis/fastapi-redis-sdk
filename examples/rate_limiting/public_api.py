r"""A public API: fair use for everyone, lockouts for guessers, quotas for payers.

Three limits a real API needs, and why each takes a different tool:

* ``GET /search`` and ``GET /search/suggest`` share one budget per client -
  autocomplete must not be a way round the search limit.  One
  ``rate_limit()`` object with a ``scope`` names one counter.  Internal
  callers (``X-Internal: 1``) are exempt, and a refusal gets a JSON body
  saying when to retry.
* ``POST /login`` locks a username after 5 failed attempts in 15 minutes.  A
  dependency would count every request before the handler knows whether the
  password was right, so the handler uses ``RateLimitBackend``: ``peek`` to
  enforce the lockout, ``hit`` on a failure only, ``reset`` on success.
* ``POST /complete`` meters an LLM-style endpoint by tokens, not requests: each
  call costs what its prompt costs, against a daily budget per API key.

Run it::

    export REDIS_URL=redis://localhost:6379/0
    uv run --with "fastapi[standard]" fastapi dev examples/rate_limiting/public_api.py

    for i in $(seq 6); do curl -s -o /dev/null -w '%{http_code} ' "localhost:8000/search?q=tea"; done
    curl "localhost:8000/search/suggest?q=te"     # 429 too: the budget is shared
    curl -H 'X-Internal: 1' "localhost:8000/search?q=tea"   # 200: exempt

    curl -X POST localhost:8000/login -H 'content-type: application/json' \
         -d '{"username": "alice", "password": "wrong"}'   # 401, 4 attempts left
    curl -X POST localhost:8000/complete -H 'content-type: application/json' \
         -d '{"api_key": "k1", "prompt": "Write a haiku about Redis"}'
"""

from __future__ import annotations

from fastapi import APIRouter, Depends, FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from redis_fastapi import (
    FastAPIRedis,
    RateLimitBackendDep,
    RateLimitResult,
    rate_limit,
)

router = APIRouter()


def _internal(request: Request) -> bool:
    return request.headers.get("X-Internal") == "1"


def _retry_later(request: Request, result: RateLimitResult) -> JSONResponse:
    return JSONResponse(
        {"error": "rate limit exceeded", "retry_after": result.retry_after},
        status_code=429,
    )


# One object, one scope, one counter: both routes draw from the same budget.
search_budget = rate_limit(
    "5/minute", scope="search", skip_when=_internal, on_limit_exceeded=_retry_later
)


@router.get("/search", dependencies=[Depends(search_budget)])
async def search(q: str) -> dict[str, object]:
    return {"query": q, "results": [f"{q} result {n}" for n in range(3)]}


@router.get("/search/suggest", dependencies=[Depends(search_budget)])
async def suggest(q: str) -> dict[str, object]:
    return {"query": q, "suggestions": [f"{q}a", f"{q}pot"]}


class Credentials(BaseModel):
    username: str
    password: str


@router.post("/login")
async def login(body: Credentials, limiter: RateLimitBackendDep) -> dict[str, str]:
    key = f"login:{body.username}"
    if (await limiter.peek(key, limit=5, window=900)).remaining == 0:
        raise HTTPException(429, "account temporarily locked")
    if body.password != "correct-horse":  # noqa: S105 - stand-in for a real check
        result = await limiter.hit(key, limit=5, window=900)
        raise HTTPException(401, f"invalid credentials ({result.remaining} left)")
    await limiter.reset(key)
    return {"status": "signed in"}


class Completion(BaseModel):
    api_key: str
    prompt: str


@router.post("/complete")
async def complete(body: Completion, limiter: RateLimitBackendDep) -> dict[str, int]:
    tokens = max(1, len(body.prompt) // 4)  # a rough estimate: 4 characters each
    result = await limiter.hit(
        f"tokens:{body.api_key}", limit=100, window=86_400, cost=tokens
    )
    if not result.allowed:
        raise HTTPException(
            429, f"daily budget used up; retry in {result.retry_after}s"
        )
    return {"charged": tokens, "remaining": result.remaining}


app = FastAPI(title="Public API")
app.include_router(router)
FastAPIRedis(app).lifespan().rate_limiting()
