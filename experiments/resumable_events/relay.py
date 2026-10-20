"""Bounded blocking relay experiment, intentionally independent of HTTP."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any

from store import Event, Store, StreamError, cursor


class SlowConsumer(StreamError):
    pass


class RelayClosed(StreamError):
    pass


@dataclass(eq=False)
class Subscriber:
    stream: str
    after: str
    boundary: str
    queue: asyncio.Queue[Event]
    max_bytes: int
    queued_bytes: int = 0
    failure: BaseException | None = None
    ready: asyncio.Event = field(default_factory=asyncio.Event)

    @staticmethod
    def size(event: Event) -> int:
        return len(json.dumps(event.__dict__, ensure_ascii=False).encode())

    def fail(self, error: BaseException) -> None:
        self.failure = error
        # Release retained payload immediately; caller reconnects from its cursor.
        while not self.queue.empty():
            self.queue.get_nowait()
        self.queued_bytes = 0
        self.ready.set()

    def offer(self, event: Event) -> None:
        if self.failure is not None or cursor(event.id) <= cursor(self.boundary):
            return
        size = self.size(event)
        if self.queue.full() or self.queued_bytes + size > self.max_bytes:
            self.fail(SlowConsumer())
            return
        self.queue.put_nowait(event)
        self.queued_bytes += size
        self.ready.set()


@dataclass
class LiveStream:
    stream: str
    position: str
    subscribers: set[Subscriber] = field(default_factory=set)
    final: bool = False


class Relay:
    def __init__(
        self,
        store: Store,
        blocking_client: Any,
        *,
        block_ms: int = 200,
        maxcount: int = 64,
        maxsize: int = 262144,
        queue_events: int = 256,
        queue_bytes: int = 1048576,
    ):
        self.store = store
        self.client = blocking_client
        self.block_ms = block_ms
        self.maxcount = maxcount
        self.maxsize = maxsize
        self.queue_events = queue_events
        self.queue_bytes = queue_bytes
        self.lock = asyncio.Lock()
        self.streams: dict[str, LiveStream] = {}
        self.changed = [asyncio.Event() for _ in range(store.buckets)]
        self.tasks: list[asyncio.Task] = []
        self.closed = False
        self.replay_slots = asyncio.Semaphore(8)
        self.batches = 0

    async def __aenter__(self) -> Relay:
        self.tasks = [
            asyncio.create_task(self.run(bucket))
            for bucket in range(self.store.buckets)
        ]
        return self

    async def __aexit__(self, *args: Any) -> None:
        self.closed = True
        for state in self.streams.values():
            for subscriber in state.subscribers:
                subscriber.fail(RelayClosed())
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        self.streams.clear()

    @asynccontextmanager
    async def subscribe(self, stream: str, *, after: str = "0-0"):
        cursor(after)
        bucket = self.store.bucket(stream)
        async with self.lock:
            if self.closed:
                raise RelayClosed()
            # Holding the dispatch lock makes registration and boundary capture
            # indivisible relative to local live delivery, not Redis publication.
            initial = await self.store.read(stream, after, count=1)
            state = self.streams.get(stream)
            if state is None:
                state = LiveStream(stream, initial.tail, final=initial.sealed)
                self.streams[stream] = state
            boundary = max((state.position, after), key=cursor)
            subscriber = Subscriber(
                stream,
                after,
                boundary,
                asyncio.Queue(self.queue_events),
                self.queue_bytes,
            )
            state.subscribers.add(subscriber)
            self.changed[bucket].set()
        try:
            yield self.events(subscriber, initial.sealed and after == initial.tail)
        finally:
            async with self.lock:
                current = self.streams.get(stream)
                if current is state:
                    current.subscribers.discard(subscriber)
                    if not current.subscribers:
                        del self.streams[stream]
                self.changed[bucket].set()
                subscriber.fail(RelayClosed())

    async def events(
        self, subscriber: Subscriber, consumed: bool
    ) -> AsyncIterator[Event]:
        if consumed:
            return
        position = subscriber.after
        while cursor(position) < cursor(subscriber.boundary):
            if subscriber.failure:
                raise subscriber.failure
            async with self.replay_slots:
                batch = await self.store.read(subscriber.stream, position)
            selected = [
                e for e in batch.events if cursor(e.id) <= cursor(subscriber.boundary)
            ]
            if not selected:
                raise StreamError("Replay did not reach the captured boundary")
            for event in selected:
                position = event.id
                yield event
                if event.final:
                    return
        while True:
            await subscriber.ready.wait()
            if subscriber.failure:
                raise subscriber.failure
            while not subscriber.queue.empty():
                event = subscriber.queue.get_nowait()
                subscriber.queued_bytes -= subscriber.size(event)
                if cursor(event.id) > cursor(position):
                    position = event.id
                    yield event
                    if event.final:
                        return
            subscriber.ready.clear()

    async def run(self, bucket: int) -> None:
        rotation = 0
        while True:
            async with self.lock:
                states = [
                    state
                    for state in self.streams.values()
                    if self.store.bucket(state.stream) == bucket
                    and not state.final
                    and any(s.failure is None for s in state.subscribers)
                ]
                self.changed[bucket].clear()
                positions = {s.stream: s.position for s in states}
            if not states:
                await self.changed[bucket].wait()
                continue
            offset = rotation % len(states)
            states = states[offset:] + states[:offset]
            rotation += 1
            keys = [self.store.keys(s.stream)[0] for s in states]
            # Bootstrap is never a client event, nor a reason to busy-loop.
            ids = [max((positions[s.stream], "0-1"), key=cursor) for s in states]
            try:
                response = await self.client.execute_command(
                    "XREAD",
                    "COUNT",
                    min(16, self.maxcount),
                    "MAXCOUNT",
                    self.maxcount,
                    "MAXSIZE",
                    self.maxsize,
                    "BLOCK",
                    self.block_ms,
                    "STREAMS",
                    *keys,
                    *ids,
                )
                self.batches += 1
                returned = dict(response or [])
                for state, key in zip(states, keys, strict=True):
                    try:
                        if key not in returned:
                            # Also detect TTL loss during idle blocking reads.
                            await self.store.read(
                                state.stream, positions[state.stream], count=1
                            )
                            continue
                        observed = returned[key][-1][0]
                        batch = await self.store.read(
                            state.stream,
                            positions[state.stream],
                            count=len(returned[key]),
                        )
                    except StreamError as error:
                        # A stream-local expiry or retention failure must not
                        # terminate unrelated streams in this shared bucket.
                        async with self.lock:
                            for subscriber in state.subscribers:
                                subscriber.fail(error)
                        continue
                    events = [
                        e for e in batch.events if cursor(e.id) <= cursor(observed)
                    ]
                    async with self.lock:
                        if self.streams.get(state.stream) is not state:
                            continue
                        for event in events:
                            for subscriber in state.subscribers:
                                subscriber.offer(event)
                            state.position = event.id
                            state.final = event.final
            except asyncio.CancelledError:
                raise
            except Exception as error:
                async with self.lock:
                    for state in states:
                        for subscriber in state.subscribers:
                            subscriber.fail(error)
