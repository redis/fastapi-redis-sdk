"""Experimental storage protocol; not a public redis_fastapi implementation."""

from __future__ import annotations

import base64
import hashlib
import json
import re
from dataclasses import dataclass
from typing import Any

from redis.exceptions import ResponseError, WatchError


class StreamError(Exception):
    pass


class Missing(StreamError):
    pass


class Expired(StreamError):
    pass


class Conflict(StreamError):
    pass


class Sealed(StreamError):
    pass


class HistoryUnavailable(StreamError):
    pass


class Inconsistent(StreamError):
    pass


def cursor(value: str) -> tuple[int, int]:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{1,20}-[0-9]{1,20}", value
    ):
        raise ValueError("Invalid stream cursor")
    parts = tuple(map(int, value.split("-")))
    if any(part > 2**64 - 1 for part in parts):
        raise ValueError("Cursor exceeds Redis ID range")
    return parts


@dataclass(frozen=True)
class Event:
    id: str
    event: str
    data: Any
    producer: str
    idempotency_key: str
    final: bool

    @classmethod
    def parse(cls, entry: tuple[str, dict[str, str]]) -> Event:
        event_id, fields = entry
        return cls(
            event_id,
            fields["event"],
            json.loads(fields["data"]),
            fields["producer"],
            fields["iid"],
            fields["final"] == "1",
        )


@dataclass
class Batch:
    events: list[Event]
    tail: str
    sealed: bool


class Store:
    """WATCH/MULTI candidate with finality derived from the last stream event.

    Clients must decode responses. Uses only SDK-owned keys in a unique namespace.
    No automatic retry of EXEC after an ambiguous connection failure is attempted.
    """

    def __init__(self, client: Any, prefix: str, *, buckets: int = 4):
        if not prefix or any(c in prefix for c in "{}") or buckets < 1:
            raise ValueError("Invalid namespace or bucket count")
        self.client = client
        self.prefix = prefix
        self.buckets = buckets
        self.keys_used: set[str] = set()

    def bucket(self, stream: str) -> int:
        if not isinstance(stream, str) or not 1 <= len(stream.encode()) <= 512:
            raise ValueError("Stream must contain 1–512 UTF-8 bytes")
        return (
            int.from_bytes(hashlib.sha256(stream.encode()).digest(), "big")
            % self.buckets
        )

    def keys(self, stream: str) -> tuple[str, str]:
        bucket = self.bucket(stream)
        encoded = base64.urlsafe_b64encode(stream.encode()).decode().rstrip("=")
        base = f"{self.prefix}:{{streams:v1:bucket:{bucket}}}:{encoded}"
        keys = (f"{base}:events", f"{base}:meta")
        self.keys_used.update(keys)
        return keys

    async def initialize(self) -> None:
        """Reject mismatched placement, and probe required XREAD options."""
        manifest = f"{self.prefix}:layout"
        self.keys_used.add(manifest)
        layout = json.dumps({"version": 1, "hash": "sha256", "buckets": self.buckets})
        await self.client.set(manifest, layout, nx=True, ex=3600)
        if await self.client.get(manifest) != layout:
            raise Conflict("Namespace placement differs")
        key, _ = self.keys("_capability_probe")
        # No BLOCK and no mutation of the probe stream.
        await self.client.execute_command(
            "XREAD", "COUNT", 1, "MAXCOUNT", 1, "MAXSIZE", 1024, "STREAMS", key, "0-0"
        )

    @staticmethod
    def results(values: list[Any]) -> list[Any]:
        if any(isinstance(value, ResponseError) for value in values):
            raise Inconsistent("EXEC contained an error; no rollback is assumed")
        return values

    async def create(self, stream: str, *, ttl: int) -> None:
        if type(ttl) is not int or ttl <= 0:
            raise ValueError("TTL must be a positive integer")
        key, meta = self.keys(stream)
        for _ in range(64):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    state = await pipe.hgetall(meta)
                    kind = await pipe.type(key)
                    if state:
                        await self.validate(state, kind)
                        if int(state["ttl"]) != ttl:
                            raise Conflict("Creation configuration differs")
                        return
                    if kind != "none":
                        raise Inconsistent("Partial initialization")
                    seconds, micros = await self.client.time()
                    deadline = seconds * 1000 + micros // 1000 + ttl * 1000
                    pipe.multi()
                    pipe.xadd(key, {"bootstrap": "1"}, id="0-1")
                    # Expire immediately after key creation, before allocating metadata.
                    pipe.pexpireat(key, deadline)
                    pipe.execute_command(
                        "XCFGSET", key, "IDMP-DURATION", 86400, "IDMP-MAXSIZE", 10000
                    )
                    pipe.hset(
                        meta,
                        mapping={"ttl": ttl, "deadline": deadline, "floor": "0-0"},
                    )
                    pipe.pexpireat(meta, deadline + 86400000)
                    self.results(await pipe.execute(raise_on_error=False))
                    return
            except WatchError:
                continue
        raise Conflict("Creation contention limit")

    @staticmethod
    def check(state: dict[str, str], kind: str) -> None:
        if not state:
            if kind != "none":
                raise Inconsistent("Missing metadata")
            raise Missing()
        if not {"ttl", "deadline", "floor"} <= state.keys():
            raise Inconsistent("Invalid stored state")
        if kind == "none":
            # Caller uses server time below to distinguish expiry from key loss.
            return
        if kind != "stream":
            raise Inconsistent("Invalid stored state")

    async def validate(self, state: dict[str, str], kind: str) -> None:
        self.check(state, kind)
        seconds, micros = await self.client.time()
        if seconds * 1000 + micros // 1000 >= int(state["deadline"]):
            raise Expired()
        if kind != "stream":
            raise Inconsistent("Payload disappeared before expiry")

    async def append(
        self,
        stream: str,
        *,
        event: str,
        data: Any,
        producer: str,
        idempotency_key: str,
        final: bool = False,
    ) -> str:
        if not event or any(c in event for c in "\r\n\0"):
            raise ValueError("Invalid event name")
        if not producer or not idempotency_key:
            raise ValueError("Stable producer and event identities are required")
        encoded = json.dumps(data, allow_nan=False, separators=(",", ":"))
        fields = {
            "event": event,
            "data": encoded,
            "producer": producer,
            "iid": idempotency_key,
            "final": str(int(final)),
        }
        if sum(len(k.encode()) + len(v.encode()) for k, v in fields.items()) > 65536:
            raise ValueError("Event exceeds 64 KiB")
        key, meta = self.keys(stream)
        for _ in range(64):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    state = await pipe.hgetall(meta)
                    await self.validate(state, await pipe.type(key))
                    last = await pipe.xrevrange(key, count=1)
                    if last and last[0][1].get("final") == "1":
                        previous = Event.parse(last[0])
                        if final and (previous.producer, previous.idempotency_key) == (
                            producer,
                            idempotency_key,
                        ):
                            return previous.id
                        raise Sealed()
                    pipe.multi()
                    pipe.xadd(
                        key, fields, nomkstream=True, idmp=(producer, idempotency_key)
                    )
                    result = self.results(await pipe.execute(raise_on_error=False))[0]
                    if result is None:
                        raise Inconsistent("Append did not reach the stream")
                    if final:
                        stored = await self.client.xrange(key, min=result, max=result)
                        if not stored or stored[0][1].get("final") != "1":
                            raise Conflict("Finish identity was already used by append")
                    return result
            except WatchError:
                continue
        raise Conflict("Append contention limit")

    async def finish(self, stream: str, **kwargs: Any) -> str:
        return await self.append(stream, final=True, **kwargs)

    async def read(self, stream: str, after: str = "0-0", count: int = 32) -> Batch:
        position = cursor(after)
        key, meta = self.keys(stream)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.hgetall(meta)
            pipe.type(key)
            pipe.xrange(key, min=f"({after}", count=count + 1)
            pipe.xrevrange(key, count=1)
            state, kind, rows, last = self.results(
                await pipe.execute(raise_on_error=False)
            )
        await self.validate(state, kind)
        if position < cursor(state["floor"]):
            raise HistoryUnavailable()
        tail = last[0][0] if last and "bootstrap" not in last[0][1] else state["floor"]
        if position > cursor(tail):
            raise Conflict("Cursor ahead of tail")
        events = [Event.parse(row) for row in rows if "bootstrap" not in row[1]][:count]
        return Batch(events, tail, bool(last and last[0][1].get("final") == "1"))

    async def trim_through(self, stream: str, through: str) -> None:
        """Explicit trim for the experiment; always retain a sealed log's final event."""
        timestamp, sequence = cursor(through)
        if sequence == 2**64 - 1:
            raise ValueError("Unsupported trim boundary")
        key, meta = self.keys(stream)
        for _ in range(64):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    state = await pipe.hgetall(meta)
                    await self.validate(state, await pipe.type(key))
                    removed = await pipe.xrevrange(key, max=through, count=1)
                    if not removed or "bootstrap" in removed[0][1]:
                        return
                    if removed[0][1].get("final") == "1":
                        raise Conflict("Cannot trim final marker")
                    floor = max(cursor(state["floor"]), cursor(removed[0][0]))
                    pipe.multi()
                    pipe.xtrim(
                        key, minid=f"{timestamp}-{sequence + 1}", approximate=False
                    )
                    pipe.hset(meta, "floor", f"{floor[0]}-{floor[1]}")
                    self.results(await pipe.execute(raise_on_error=False))
                    return
            except WatchError:
                continue
        raise Conflict("Trim contention limit")

    async def cleanup(self) -> None:
        for key in self.keys_used:
            await self.client.delete(key)
