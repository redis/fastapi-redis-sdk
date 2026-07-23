"""Redis Pub/Sub manager for real-time messaging.

Provides a :class:`PubSubManager` that wraps Redis Pub/Sub operations
for use in WebSocket endpoints and background tasks.  Each instance
uses a dedicated PubSub connection from the shared async pool.

Usage with dependency injection (HTTP)::

    from redis_fastapi import PubSubManagerDep

    @app.get("/publish")
    async def publish(pubsub: PubSubManagerDep):
        await pubsub.publish("channel", "hello")

Usage in WebSocket endpoints::

    from redis_fastapi import PubSubManager
    from redis_fastapi.deps import _get_pool_state

    @app.websocket("/ws/{room}")
    async def chat(websocket: WebSocket):
        redis = _get_pool_state(websocket.app).get_async_client()
        pubsub = PubSubManager(redis)
        await pubsub.subscribe(f"room:{room}")
        async for message in pubsub.listen():
            await websocket.send_text(message["data"])
"""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator
from typing import Any

from redis.exceptions import RedisError

from redis_fastapi.deps import AsyncClient
from redis_fastapi.telemetry import cache_span, record_pubsub_publish

logger = logging.getLogger(__name__)


class PubSubManager:
    """Manages Redis Pub/Sub connections for real-time messaging.

    Each instance wraps a dedicated PubSub connection obtained from
    the shared pool.  Call :meth:`subscribe` to join channels, then
    iterate :meth:`listen` to receive messages.

    Args:
        redis: An async Redis client (standalone or cluster).
    """

    def __init__(self, redis: AsyncClient) -> None:
        self._redis = redis
        self._pubsub: Any = None

    async def subscribe(self, *channels: str) -> None:
        """Subscribe to one or more Redis channels.

        Creates the underlying PubSub connection on first call.
        Subsequent calls add channels to the existing subscription.

        Args:
            *channels: One or more channel names to subscribe to.
        """
        ps = await self._get_or_create_pubsub()
        await ps.subscribe(*channels)

    async def unsubscribe(self, *channels: str) -> None:
        """Unsubscribe from one or more Redis channels.

        Safe to call before :meth:`subscribe`; no-op when the
        PubSub connection has not been created.

        Args:
            *channels: One or more channel names to unsubscribe from.
        """
        if self._pubsub is None:
            return
        await self._pubsub.unsubscribe(*channels)

    async def publish(self, channel: str, message: str) -> int:
        """Publish a message to a Redis channel.

        Args:
            channel: The target channel name.
            message: The message string to publish.

        Returns:
            Number of subscribers that received the message, or ``0``
            on error.
        """
        with cache_span(
            "pubsub.publish",
            attributes={"pubsub.channel": channel},
        ):
            try:
                count = await self._redis.publish(channel, message)
                record_pubsub_publish(channel=channel, subscribers=count)
                return count
            except (RedisError, OSError):
                logger.warning(
                    "Error publishing to channel '%s'", channel, exc_info=True
                )
                return 0

    async def listen(self) -> AsyncIterator[dict[str, Any]]:
        """Yield messages from all subscribed channels.

        Each yielded dict follows redis-py's PubSub message format::

            {
                "type": "message",
                "pattern": None,
                "channel": b"channel_name",
                "data": b"message_content",
            }

        Only ``message``-type events are yielded; subscription
        confirmation and other system events are filtered out.

        Yields:
            Raw PubSub message dicts.
        """
        ps = await self._get_or_create_pubsub()
        async for message in ps.listen():
            if message["type"] == "message":
                yield message

    async def close(self) -> None:
        """Unsubscribe all channels and close the PubSub connection.

        Safe to call multiple times.  No-op when already closed or
        never connected.
        """
        if self._pubsub is None:
            return
        try:
            await self._pubsub.unsubscribe()
            await self._pubsub.aclose()
        except (RedisError, OSError):
            logger.warning("Error closing PubSub connection", exc_info=True)
        finally:
            self._pubsub = None

    async def _get_or_create_pubsub(self) -> Any:
        """Return the PubSub connection, creating it lazily."""
        if self._pubsub is None:
            self._pubsub = self._redis.pubsub()
        return self._pubsub
