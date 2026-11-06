"""Template routes through the cache middleware under ``TestClient``.

Starlette's ``TemplateResponse`` sends ``http.response.debug`` before
``http.response.start`` whenever the server advertises the extension, and
``TestClient`` always does.  These tests use the real ``Jinja2Templates`` so
they break if Starlette changes that order, not only if the middleware does.
See https://github.com/redis/fastapi-redis-sdk/issues/92.
"""

import fakeredis.aioredis
import jinja2
import pytest
from fastapi import Depends, FastAPI, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi.testclient import TestClient

from redis_fastapi.cache import cache
from redis_fastapi.deps import get_async_redis
from redis_fastapi.setup import FastAPIRedis

_templates = Jinja2Templates(
    env=jinja2.Environment(
        loader=jinja2.DictLoader({"page.html": "<p>{{ value }}</p>"}),
        autoescape=True,
    )
)


def _make_app(
    fake: fakeredis.aioredis.FakeRedis,
) -> tuple[FastAPI, list[int]]:
    app = FastAPI()
    FastAPIRedis(app).caching()
    counts: list[int] = [0]

    def _render(request: Request) -> HTMLResponse:
        counts[0] += 1
        return _templates.TemplateResponse(request, "page.html", {"value": counts[0]})

    @app.get("/plain", response_class=HTMLResponse)
    async def plain(request: Request) -> HTMLResponse:
        return _render(request)

    @app.get(
        "/cached",
        response_class=HTMLResponse,
        dependencies=[Depends(cache(ttl=300))],
    )
    async def cached(request: Request) -> HTMLResponse:
        return _render(request)

    async def _fake() -> fakeredis.aioredis.FakeRedis:
        return fake

    app.dependency_overrides[get_async_redis] = _fake
    return app, counts


@pytest.mark.unit
class TestTemplateResponses:
    def test_uncached_route_is_served(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Caching is on but this route does not use it: no second start."""
        app, _ = _make_app(fake_async_redis)
        with TestClient(app) as c:
            r = c.get("/plain")

        assert r.status_code == 200
        assert r.headers["content-type"].startswith("text/html")
        assert r.text == "<p>1</p>"
        assert r.template.name == "page.html"  # type: ignore[attr-defined]
        assert "X-Redis-Cache" not in r.headers

    def test_cached_route_miss_then_hit(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        app, counts = _make_app(fake_async_redis)
        with TestClient(app) as c:
            r1 = c.get("/cached")
            r2 = c.get("/cached")

        assert r1.headers["X-Redis-Cache"] == "MISS"
        assert r1.headers["content-type"].startswith("text/html")
        assert r1.text == "<p>1</p>"
        assert r1.template.name == "page.html"  # type: ignore[attr-defined]
        assert r1.context["value"] == 1  # type: ignore[attr-defined]

        assert r2.headers["X-Redis-Cache"] == "HIT"
        assert r2.headers["content-type"].startswith("text/html")
        assert r2.text == r1.text
        assert counts[0] == 1

    def test_hit_has_no_template(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """A hit renders no template, so TestClient has none to report."""
        app, _ = _make_app(fake_async_redis)
        with TestClient(app) as c:
            c.get("/cached")
            r = c.get("/cached")

        assert r.headers["X-Redis-Cache"] == "HIT"
        assert not hasattr(r, "template")
