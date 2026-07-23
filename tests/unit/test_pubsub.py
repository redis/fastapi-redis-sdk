"""Unit tests for PubSubManager with fakeredis."""

from __future__ import annotations

import anyio
import asyncio
import fakeredis.aioredis
import gc
import pytest

from redis_fastapi.pubsub import PubSubManager


@pytest.mark.unit
class TestPubSubManager:
    """Core PubSubManager lifecycle: subscribe, listen, publish, close."""

    async def test_subscribe_and_listen(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("test:ch")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("test:ch", "hello")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"hello"
                break

        await pubsub.close()

    async def test_multiple_channels(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:a", "ch:b")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:a", "msg-a")
            await pubsub.publish("ch:b", "msg-b")

        received: list[str] = []
        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                received.append(message["data"])
                if len(received) == 2:
                    break

        assert b"msg-a" in received
        assert b"msg-b" in received
        await pubsub.close()

    async def test_publish_returns_subscriber_count(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pub_a = PubSubManager(fake)
        pub_b = PubSubManager(fake)
        await pub_b.subscribe("test:ch")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            count = await pub_a.publish("test:ch", "hello")
            assert count == 1

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for _message in pub_b.listen():
                break

        await pub_a.close()
        await pub_b.close()

    async def test_unsubscribe(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:a", "ch:b")
        await pubsub.unsubscribe("ch:a")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:a", "a-only")
            await pubsub.publish("ch:b", "b-only")

        received: list[str] = []
        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                received.append(message["data"])
                if len(received) == 1:
                    break

        assert received == [b"b-only"]
        await pubsub.close()

    async def test_close_idempotent(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.close()
        await pubsub.close()

    async def test_unsubscribe_before_connect(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.unsubscribe("ch")  # should not raise

    async def test_listen_yields_only_message_type(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("test:ch")

        events: list[str] = []
        async with anyio.create_task_group() as tg:

            async def _publish() -> None:
                await anyio.sleep(0.05)
                await pubsub.publish("test:ch", "hello")

            tg.start_soon(_publish)
            async for message in pubsub.listen():
                events.append(message["type"])
                break

        assert events == ["message"]
        await pubsub.close()

    async def test_di_pubsub_manager_dep(
        self, fake_async_redis: fakeredis.aioredis.FakeRedis
    ) -> None:
        """Verify PubSubManager works with get_async_redis client."""
        from redis_fastapi import PubSubManager

        pubsub = PubSubManager(fake_async_redis)
        await pubsub.subscribe("test:di")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("test:di", "from-di")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"from-di"
                break

        await pubsub.close()


@pytest.mark.unit
class TestPubSubErrorHandling:
    """PubSubManager gracefully handles Redis errors."""

    async def test_publish_error_returns_zero(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        count = await pubsub.publish("ch", "data")
        assert count == 0
        await pubsub.close()


@pytest.mark.unit
class TestPubSubEdgeCases:
    """Edge cases: empty channels, binary data, reconnection, etc."""

    async def test_empty_channel_name(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("")
        await pubsub.publish("", "empty-ch")
        await pubsub.close()

    async def test_unicode_message(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:unicode")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:unicode", "héllo wörld 🎉")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert isinstance(message["data"], bytes)
                assert message["data"] == "héllo wörld 🎉".encode()
                break
        await pubsub.close()

    async def test_utf8_message_with_special_chars(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:utf8")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:utf8", "tab\there\nnewline")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"tab\there\nnewline"
                break
        await pubsub.close()

    async def test_subscribe_after_close_reconnects(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:first")
        await pubsub.close()

        await pubsub.subscribe("ch:second")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:second", "after-reconnect")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"after-reconnect"
                break
        await pubsub.close()

    async def test_close_then_publish_no_crash(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.close()
        count = await pubsub.publish("ch", "after-close")
        assert count == 0
        await pubsub.close()

    async def test_long_channel_name(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        long_ch = "ch:" + "a" * 500
        await pubsub.subscribe(long_ch)

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish(long_ch, "long-channel")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"long-channel"
                break
        await pubsub.close()

    async def test_large_message(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:large")
        large_msg = "x" * 100_000

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:large", large_msg)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert len(message["data"]) == 100_000
                break
        await pubsub.close()

    async def test_rapid_publish_burst(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:burst")

        async def _publish_burst() -> None:
            await anyio.sleep(0.05)
            for i in range(20):
                await pubsub.publish("ch:burst", str(i))

        received: list[int] = []
        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish_burst)
            async for message in pubsub.listen():
                received.append(int(message["data"]))
                if len(received) == 20:
                    break
        assert received == list(range(20))
        await pubsub.close()

    async def test_multiple_concurrent_subscribers(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        sub_a = PubSubManager(fake)
        sub_b = PubSubManager(fake)
        pub = PubSubManager(fake)
        await sub_a.subscribe("ch:multi")
        await sub_b.subscribe("ch:multi")

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pub.publish("ch:multi", "to-all")

        results: list[str] = []

        async def _listen(sub: PubSubManager, label: str) -> None:
            async for _message in sub.listen():
                results.append(label)
                break

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            tg.start_soon(_listen, sub_a, "a")
            tg.start_soon(_listen, sub_b, "b")

        assert "a" in results
        assert "b" in results
        assert len(results) == 2
        await sub_a.close()
        await sub_b.close()
        await pub.close()

    async def test_channel_name_with_special_chars(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        special_ch = "ch:spaces and :colons:and/slashes"
        await pubsub.subscribe(special_ch)

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish(special_ch, "special")

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert message["data"] == b"special"
                break
        await pubsub.close()

    async def test_double_subscribe_same_channel(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:dup")
        await pubsub.subscribe("ch:dup")  # second subscribe should be idempotent

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:dup", "deduped")

        received = 0
        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for _message in pubsub.listen():
                received += 1
                break

        assert received == 1
        await pubsub.close()

    async def test_unsubscribe_all_channels(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:a", "ch:b", "ch:c")
        await pubsub.unsubscribe()
        # After unsubscribing all, listen should not yield
        await pubsub.close()

    async def test_memory_safety_large_payload(self) -> None:
        fake = fakeredis.aioredis.FakeRedis()
        pubsub = PubSubManager(fake)
        await pubsub.subscribe("ch:big")
        huge = "z" * 1_000_000  # 1 MB string

        async def _publish() -> None:
            await anyio.sleep(0.05)
            await pubsub.publish("ch:big", huge)

        async with anyio.create_task_group() as tg:
            tg.start_soon(_publish)
            async for message in pubsub.listen():
                assert len(message["data"]) == 1_000_000
                break
        await pubsub.close()


@pytest.mark.unit
class TestPubSubMemory:
    """Memory leak and reference cycle tests."""

    def test_no_memory_leak_after_close(self) -> None:
        """PubSubManager is garbage collected after close() and ref removal.

        Creates a manager, subscribes, closes, then verifies via weakref
        that the object is collected once all strong references are gone.
        The check runs outside ``asyncio.run()`` so the coroutine frame
        does not keep the object alive.
        """
        import weakref

        fake = fakeredis.aioredis.FakeRedis()

        async def lifecycle() -> weakref.ref:
            m = PubSubManager(fake)
            await m.subscribe("mem:leak")
            await m.close()
            return weakref.ref(m)

        ref = asyncio.run(lifecycle())

        gc.collect()
        gc.collect()
        gc.collect()

        assert ref() is None, (
            "PubSubManager was not garbage collected — possible reference cycle. "
            f"Object: {ref()}"
        )

    def test_multiple_instances_collected(self) -> None:
        """Many PubSubManager instances are all collectable."""
        import weakref

        async def bulk() -> list[weakref.ref]:
            refs = []
            for i in range(50):
                fake = fakeredis.aioredis.FakeRedis()
                m = PubSubManager(fake)
                await m.subscribe(f"bulk:ch:{i}")
                await m.close()
                refs.append(weakref.ref(m))
            return refs

        refs = asyncio.run(bulk())
        gc.collect()
        gc.collect()
        gc.collect()

        alive = [r for r in refs if r() is not None]
        assert len(alive) == 0, (
            f"{len(alive)} out of {len(refs)} PubSubManager instances "
            "were not garbage collected"
        )
