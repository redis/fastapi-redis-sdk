"""Tests for Redis Pub/Sub helpers."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import fakeredis.aioredis
import pytest
from fastapi import FastAPI, WebSocket
from fastapi.testclient import TestClient

from redis_fastapi import RedisChannelManagerDep
from redis_fastapi.deps import get_async_redis
from redis_fastapi.pubsub import RedisChannelManager


@pytest.mark.unit
class TestRedisChannelManager:
    async def test_publish_and_subscribe_round_trip(self) -> None:
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)
        channels = RedisChannelManager(redis)

        async with channels.subscribe("room:42") as messages:
            subscribers = await channels.publish("room:42", "hello")
            assert subscribers == 1
            assert await anext(messages) == "hello"

        await redis.aclose()

    async def test_subscription_closes_on_exit(self) -> None:
        pubsub = MagicMock()
        pubsub.subscribe = AsyncMock()
        pubsub.aclose = AsyncMock()
        redis = MagicMock()
        redis.pubsub.return_value = pubsub

        channels = RedisChannelManager(redis)
        async with channels.subscribe("room:42"):
            pass

        pubsub.subscribe.assert_awaited_once_with("room:42")
        pubsub.aclose.assert_awaited_once_with()

    async def test_channel_manager_dependency(self) -> None:
        app = FastAPI()
        redis = fakeredis.aioredis.FakeRedis(decode_responses=True)

        async def override() -> fakeredis.aioredis.FakeRedis:
            return redis

        app.dependency_overrides[get_async_redis] = override

        @app.websocket("/ws")
        async def websocket_endpoint(
            websocket: WebSocket,
            channels: RedisChannelManagerDep,
        ) -> None:
            await websocket.accept()
            await websocket.send_json(
                {"subscribers": await channels.publish("notifications", "ready")}
            )

        with TestClient(app) as client:
            with client.websocket_connect("/ws") as websocket:
                assert websocket.receive_json() == {"subscribers": 0}

        await redis.aclose()

    async def test_subscription_closes_when_subscribe_fails(self) -> None:
        pubsub = MagicMock()
        pubsub.subscribe = AsyncMock(side_effect=ConnectionError("down"))
        pubsub.aclose = AsyncMock()
        redis = MagicMock()
        redis.pubsub.return_value = pubsub

        channels = RedisChannelManager(redis)
        with pytest.raises(ConnectionError, match="down"):
            async with channels.subscribe("room:42"):
                pass

        pubsub.aclose.assert_awaited_once_with()
