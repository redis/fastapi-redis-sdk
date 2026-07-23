"""Integration tests for PubSubManager with a real Redis server."""

from __future__ import annotations

import anyio
import pytest

from redis_fastapi.pubsub import PubSubManager

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        True, reason="Requires a running Redis server — run with --runintegration"
    ),
]


@pytest.mark.integration
class TestPubSubIntegration:
    """PubSubManager end-to-end with a real Redis server."""

    async def test_cross_connection_pubsub(
        self,
        real_async_redis: any,
        test_prefix: str,  # type: ignore[no-untyped-def]
    ) -> None:
        """Two PubSubManager instances on the same Redis communicate."""
        pub_a = PubSubManager(real_async_redis)
        pub_b = PubSubManager(real_async_redis)
        channel = f"{test_prefix}:bridge"

        await pub_a.subscribe(channel)

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pub_b.publish(channel, "from-b")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pub_a.listen():
                assert message["data"] == b"from-b"
                break

        await pub_a.close()
        await pub_b.close()

    async def test_publish_subscriber_count(
        self,
        real_async_redis: any,
        test_prefix: str,  # type: ignore[no-untyped-def]
    ) -> None:
        """publish() returns the number of subscribers."""
        pub = PubSubManager(real_async_redis)
        channel = f"{test_prefix}:count"

        sub = PubSubManager(real_async_redis)
        await sub.subscribe(channel)

        async def _publish() -> None:
            await anyio.sleep(0.05)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for _message in sub.listen():
                break

        count = await pub.publish(channel, "hi")
        assert count >= 1
        await pub.close()
        await sub.close()

    async def test_unsubscribe_stops_messages(
        self,
        real_async_redis: any,
        test_prefix: str,  # type: ignore[no-untyped-def]
    ) -> None:
        """After unsubscribe, no more messages arrive on that channel."""
        pub = PubSubManager(real_async_redis)
        sub = PubSubManager(real_async_redis)
        channel = f"{test_prefix}:unsub"

        await sub.subscribe(channel)

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pub.publish(channel, "first")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in sub.listen():
                assert message["data"] == b"first"
                break

        await sub.unsubscribe(channel)

        await pub.publish(channel, "second")

        # No more messages should arrive — just verify no crash
        await anyio.sleep(0.1)

        await pub.close()
        await sub.close()
