"""Shared bounded readers and separate catch-up, adapted from the SDK spike."""

from __future__ import annotations

import asyncio
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from redis.exceptions import RedisError

from redis_fastapi.stream_backend import (
    StreamCapacityExceeded,
    StreamError,
    StreamEvent,
    StreamSlowConsumer,
    _normalize_xread,
    _StreamStore,
    parse_cursor,
)
from redis_fastapi.telemetry import record_stream


@dataclass(eq=False)
class _Subscription:
    relay: _StreamRelay
    stream: str
    after: str
    boundary: str
    consumed: bool
    queue: asyncio.Queue[StreamEvent]
    queued_bytes: int = 0
    failure: StreamError | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)

    def fail(self, error: StreamError) -> None:
        if self.failure is None:
            record_stream("disconnects", reason=error.code)
        self.failure = error
        record_stream("queue_bytes", -self.queued_bytes)
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queued_bytes = 0
        self.ready.set()

    def offer(self, event: StreamEvent) -> None:
        if self.failure or parse_cursor(event.id) <= parse_cursor(self.boundary):
            return
        if (
            self.queue.full()
            or self.queued_bytes + event._size()
            > self.relay.settings.stream_queue_bytes
        ):
            self.fail(StreamSlowConsumer())
            return
        self.queue.put_nowait(event)
        self.queued_bytes += event._size()
        record_stream("queue_bytes", event._size())
        self.ready.set()

    async def events(self) -> AsyncIterator[StreamEvent]:
        if self.consumed:
            return
        position = self.after
        while parse_cursor(position) < parse_cursor(self.boundary):
            if self.failure:
                raise self.failure
            async with self.relay.replay_slots:
                try:
                    record_stream("replay_batches")
                    batch = await self.relay.store.read(
                        self.stream, position, self.relay.replay_count
                    )
                except RedisError as error:
                    raise StreamError() from error
            selected = [
                e
                for e in batch.events
                if parse_cursor(e.id) <= parse_cursor(self.boundary)
            ]
            if not selected:
                raise StreamError("Replay did not reach its boundary")
            for event in selected:
                if self.failure:
                    raise self.failure
                position = event.id
                yield event
                if event.final:
                    return
        while True:
            await self.ready.wait()
            if self.failure:
                raise self.failure
            while not self.queue.empty():
                event = self.queue.get_nowait()
                self.queued_bytes -= event._size()
                record_stream("queue_bytes", -event._size())
                if parse_cursor(event.id) > parse_cursor(position):
                    position = event.id
                    yield event
                    if event.final:
                        return
            # fail() can run while a yielded event is being processed. It
            # clears the queue and sets ready; never erase that notification.
            if self.failure:
                raise self.failure
            self.ready.clear()


@dataclass
class _LiveStream:
    stream: str
    position: str
    subscribers: set[_Subscription] = field(default_factory=set)
    final: bool = False


class _StreamRelay:
    def __init__(self, store: _StreamStore, client: Any):
        self.store = store
        self.client = client
        self.settings = store.settings
        self.lock = asyncio.Lock()
        self.streams: dict[str, _LiveStream] = {}
        self.changed = [asyncio.Event() for _ in range(store.buckets)]
        self.tasks: list[asyncio.Task[None]] = []
        self.closed = False
        self.replay_slots = asyncio.Semaphore(self.settings.stream_replay_concurrency)
        # Count bounds reserve a worst-case byte budget for each replay page.
        self.replay_count = min(
            self.settings.stream_read_count,
            max(
                1,
                self.settings.stream_replay_bytes
                // (self.settings.stream_max_event_bytes + 512),
            ),
        )
        self.batches = 0
        self.subscribers = 0

    async def start(self) -> None:
        self.tasks = [
            asyncio.create_task(self.run(i), name=f"redis-stream-relay-{i}")
            for i in range(self.store.buckets)
        ]

    async def stop(self) -> None:
        async with self.lock:
            self.closed = True
            for state in self.streams.values():
                for subscriber in state.subscribers:
                    subscriber.fail(StreamError("Stream runtime is shutting down"))
            record_stream("active_streams", -len(self.streams))
            self.streams.clear()
        # A client await may return concurrently with cancellation on older
        # Python versions. Wake idle loops and make closed state authoritative.
        for changed in self.changed:
            changed.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            done, pending = await asyncio.wait(
                self.tasks, timeout=self.settings.stream_prepare_timeout
            )
            for task in done:
                if not task.cancelled():
                    task.result()
            if pending:
                for task in pending:
                    task.cancel()
                raise StreamError("Relay shutdown exceeded its grace period")
        self.tasks.clear()

    @asynccontextmanager
    async def subscribe(
        self, stream: str, *, after: str
    ) -> AsyncIterator[_Subscription]:
        parse_cursor(after)
        bucket = self.store.bucket(stream)
        # Redis reads happen outside the global dispatch lock. Replay supplies
        # any events delivered locally between this read and registration.
        async with self.replay_slots:
            try:
                initial = await self.store.read(stream, after, 1)
            except RedisError as error:
                raise StreamError() from error
        async with self.lock:
            if self.closed:
                raise StreamError("Stream runtime is stopped")
            if self.subscribers >= self.settings.stream_max_subscribers or (
                stream not in self.streams
                and len(self.streams) >= self.settings.stream_max_active_streams
            ):
                record_stream("rejections", reason="capacity")
                raise StreamCapacityExceeded()
            state = self.streams.get(stream)
            if state is None:
                state = _LiveStream(stream, initial.tail, final=initial.sealed)
                self.streams[stream] = state
                record_stream("active_streams")
            boundary = max((state.position, initial.tail, after), key=parse_cursor)
            subscriber = _Subscription(
                self,
                stream,
                after,
                boundary,
                initial.sealed and parse_cursor(after) == parse_cursor(initial.tail),
                asyncio.Queue(self.settings.stream_queue_events),
            )
            state.subscribers.add(subscriber)
            self.subscribers += 1
            record_stream("subscribers")
            self.changed[bucket].set()
        try:
            yield subscriber
        finally:
            async with self.lock:
                current = self.streams.get(stream)
                if current is state:
                    current.subscribers.discard(subscriber)
                    if not current.subscribers:
                        del self.streams[stream]
                        record_stream("active_streams", -1)
                self.subscribers -= 1
                record_stream("subscribers", -1)
                self.changed[bucket].set()
                subscriber.fail(StreamError("Subscription closed"))

    async def run(self, bucket: int) -> None:
        rotation = 0
        while not self.closed:
            async with self.lock:
                if self.closed:
                    return
                states = [
                    s
                    for s in self.streams.values()
                    if self.store.bucket(s.stream) == bucket
                    and not s.final
                    and any(c.failure is None for c in s.subscribers)
                ]
                self.changed[bucket].clear()
                positions = {s.stream: s.position for s in states}
            if not states:
                await self.changed[bucket].wait()
                continue
            offset = rotation % len(states)
            states = states[offset:] + states[:offset]
            limits: list[Any] = []
            if bucket in self.store.native_read_limits:
                # Redis 8.10 bounds the aggregate response itself. Rotate the
                # first key so a busy stream cannot monopolize a native batch.
                count = min(
                    self.settings.stream_read_count, self.settings.stream_read_maxcount
                )
                limits = [
                    "MAXCOUNT",
                    self.settings.stream_read_maxcount,
                    "MAXSIZE",
                    self.settings.stream_read_maxbytes,
                ]
                rotation += 1
            else:
                # Redis 8.6 COUNT is per key. Reserve reply/key framing first,
                # then admit only a group whose worst-case entries fit the budget.
                entry_bytes = self.settings.stream_max_event_bytes + 512
                remaining = self.settings.stream_read_maxbytes - 64
                selected: list[_LiveStream] = []
                key_bytes = 0
                for state in states:
                    overhead = len(self.store.keys(state.stream)[0].encode()) + 64
                    if (
                        len(selected) >= self.settings.stream_read_maxcount
                        or key_bytes + overhead + (len(selected) + 1) * entry_bytes
                        > remaining
                    ):
                        break
                    selected.append(state)
                    key_bytes += overhead
                if not selected:
                    raise StreamError("Live read budget no longer fits a stream")
                states = selected
                count = min(
                    self.settings.stream_read_count,
                    self.settings.stream_read_maxcount // len(states),
                    (remaining - key_bytes) // (entry_bytes * len(states)),
                )
                rotation += len(states)
            keys = [self.store.keys(s.stream)[0] for s in states]
            ids = [max((positions[s.stream], "0-1"), key=parse_cursor) for s in states]
            try:
                # The relay client is independently owned and may not have a
                # slot map yet. Route native syntax directly to the bucket node.
                from redis.asyncio import RedisCluster

                route = {}
                if limits and isinstance(self.client, RedisCluster):
                    await self.client.initialize()
                    route = {"target_nodes": self.client.get_node_from_key(keys[0])}
                started = time.monotonic()
                record_stream("blocked_readers", bucket=bucket)
                try:
                    response = await self.client.execute_command(
                        "XREAD",
                        "COUNT",
                        count,
                        *limits,
                        "BLOCK",
                        self.settings.stream_block_ms,
                        "STREAMS",
                        *keys,
                        *ids,
                        **route,
                    )
                finally:
                    record_stream("blocked_readers", -1, bucket=bucket)
                    record_stream(
                        "read_duration", time.monotonic() - started, bucket=bucket
                    )
                record_stream("read_batches", bucket=bucket)
                self.batches += 1
                returned = _normalize_xread(response)
                for state, key in zip(states, keys, strict=True):
                    try:
                        batch = await self.store.read(
                            state.stream,
                            positions[state.stream],
                            min(len(returned[key]), self.settings.stream_read_count)
                            if key in returned
                            else 1,
                        )
                    except StreamError as error:
                        async with self.lock:
                            for subscriber in state.subscribers:
                                subscriber.fail(error)
                        continue
                    if key not in returned:
                        continue
                    observed = returned[key][-1][0]
                    events = [
                        e
                        for e in batch.events
                        if parse_cursor(e.id) <= parse_cursor(observed)
                    ]
                    async with self.lock:
                        if self.streams.get(state.stream) is not state:
                            continue
                        for event in events:
                            for subscriber in state.subscribers:
                                subscriber.offer(event)
                            record_stream("deliveries", bucket=bucket)
                            record_stream(
                                "delivery_lag",
                                max(0, time.time() - parse_cursor(event.id)[0] / 1000),
                                bucket=bucket,
                            )
                            state.position = event.id
                            state.final = event.final
            except asyncio.CancelledError:
                raise
            except (RedisError, StreamError):
                async with self.lock:
                    for state in states:
                        for subscriber in state.subscribers:
                            subscriber.fail(StreamError())
                # Failed subscribers are removed from subsequent reads. New
                # connections revalidate storage and trigger a fresh read.
