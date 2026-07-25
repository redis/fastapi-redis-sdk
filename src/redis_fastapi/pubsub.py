"""Transient Redis Pub/Sub helpers for FastAPI WebSocket endpoints."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import TypeAlias

from redis.asyncio.client import PubSub

from redis_fastapi.deps import AsyncClient

RedisChannel: TypeAlias = str | bytes
RedisChannelMessage: TypeAlias = str | bytes


class RedisChannelManager:
    """Publish and subscribe using the lifespan-managed Redis client."""

    def __init__(self, redis: AsyncClient) -> None:
        self._redis = redis

    async def publish(
        self,
        channel: RedisChannel,
        message: RedisChannelMessage,
    ) -> int:
        """Publish a transient message and return the subscriber count."""
        return await self._redis.publish(channel, message)

    @asynccontextmanager
    async def subscribe(
        self,
        channel: RedisChannel,
    ) -> AsyncIterator[AsyncIterator[RedisChannelMessage]]:
        """Yield channel payloads and close the Pub/Sub connection on exit."""
        pubsub = self._redis.pubsub(ignore_subscribe_messages=True)
        try:
            await pubsub.subscribe(channel)
            yield self._messages(pubsub)
        finally:
            await pubsub.aclose()  # type: ignore[no-untyped-call]

    @staticmethod
    async def _messages(pubsub: PubSub) -> AsyncIterator[RedisChannelMessage]:
        async for message in pubsub.listen():
            yield message["data"]
