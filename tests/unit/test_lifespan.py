"""Unit tests for redis_lifespan startup failures."""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI

from redis_fastapi.config import get_settings
from redis_fastapi.deps import _PoolState
from redis_fastapi.lifespan import redis_lifespan

# Each startup step that awaits Redis, with the app flag that turns it on.
_STARTUP_STEPS = [
    ("_probe_rate_limit_capabilities", "_redis_rate_limiting"),
    ("_check_cache_eviction_safety", "_redis_caching"),
    ("_start_streams", None),
]


@pytest.mark.unit
@pytest.mark.parametrize(("step", "flag"), _STARTUP_STEPS)
@pytest.mark.parametrize("failure", [RuntimeError("startup"), asyncio.CancelledError()])
async def test_startup_failure_closes_sdk_pool(
    step: str, flag: str | None, failure: BaseException, monkeypatch: pytest.MonkeyPatch
) -> None:
    get_settings.cache_clear()
    pool = AsyncMock()
    monkeypatch.setattr(_PoolState, "build_async_pool", lambda: pool)
    monkeypatch.setattr(
        f"redis_fastapi.lifespan.{step}", AsyncMock(side_effect=failure)
    )
    app = FastAPI()
    if flag is not None:
        setattr(app.state, flag, True)
    try:
        with pytest.raises(type(failure)):
            async with redis_lifespan(app):
                pytest.fail("startup should fail")
        pool.aclose.assert_awaited_once()
        assert app.state._redis.async_pool is None
    finally:
        get_settings.cache_clear()
