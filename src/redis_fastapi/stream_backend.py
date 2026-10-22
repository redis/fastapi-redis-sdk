"""Generic, finite Redis event logs. No FastAPI request or identity dependencies.

The transaction and retention patterns originate in Agent Mesh d94c269 and
fastapi-redis-sdk's resumable-events spike; see docs/guide/streams.md.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import time
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import Any
from uuid import uuid4

from redis.exceptions import RedisError, ResponseError, WatchError

from redis_fastapi.config import RedisSettings, StreamSettings
from redis_fastapi.telemetry import record_stream


class StreamError(Exception):
    """A safe, stable stream error; never includes Redis keys or payloads."""

    code = "stream_unavailable"
    status_code = 503
    action = "reconnect"


class StreamNotFound(StreamError):
    code = "stream_not_found"
    status_code = 404
    action = "reload"


class StreamExpired(StreamError):
    code = "stream_expired"
    status_code = 410
    action = "reload"


class StreamConflict(StreamError):
    code = "stream_conflict"
    status_code = 409
    action = "reload"


class StreamHistoryUnavailable(StreamConflict):
    code = "history_unavailable"


class StreamCursorAhead(StreamConflict):
    code = "cursor_ahead"


class InvalidStreamRequest(StreamError, ValueError):
    code = "invalid_stream_request"
    status_code = 422
    action = "reload"


class StreamCapacityExceeded(StreamError):
    code = "stream_capacity_exceeded"


class StreamSlowConsumer(StreamError):
    code = "slow_consumer"


def parse_cursor(value: str) -> tuple[int, int]:
    if not isinstance(value, str) or not re.fullmatch(
        r"[0-9]{1,20}-[0-9]{1,20}", value
    ):
        raise InvalidStreamRequest("Expected a full Redis entry ID")
    ms, seq = map(int, value.split("-"))
    if max(ms, seq) > 2**64 - 1:
        raise InvalidStreamRequest("Cursor exceeds Redis ID range")
    return ms, seq


def _normalize(value: Any) -> Any:
    """Support both binary and decoded redis-py clients, RESP2 and RESP3."""
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeError as error:
            raise StreamError("Invalid stored UTF-8") from error
    if isinstance(value, dict):
        return {_normalize(k): _normalize(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    return value


@dataclass(frozen=True)
class StreamEvent:
    id: str
    event: str
    data: Any
    final: bool = False

    @classmethod
    def _parse(cls, row: Any) -> StreamEvent:
        event_id, fields = row
        try:
            parse_cursor(event_id)
            if fields["final"] not in ("0", "1"):
                raise ValueError
            return cls(
                event_id,
                fields["event"],
                json.loads(fields["data"]),
                fields["final"] == "1",
            )
        except (KeyError, TypeError, ValueError) as error:
            raise StreamError("Invalid stored event") from error

    def _size(self) -> int:
        return (
            len(json.dumps(self.data, ensure_ascii=False).encode())
            + len(self.event.encode())
            + len(self.id)
            + 64
        )


@dataclass
class _Batch:
    events: list[StreamEvent]
    tail: str
    sealed: bool


def _minimum_live_read_bytes(settings: StreamSettings) -> int:
    # Worst-case UTF-8/base64 identity, bucket digits, returned key and RESP
    # framing, plus one validated entry. Prefix bytes are not part of event size.
    key = f"{settings.pattern_prefix('streams')}:{{streams:v1:bucket:{settings.stream_bucket_count - 1}}}:"
    return (
        settings.stream_max_event_bytes
        + 512
        + len(key.encode())
        + 684
        + len(":events")
        + 128
    )


class _StreamStore:
    def __init__(self, client: Any, settings: StreamSettings):
        self.client = client
        self.settings = settings
        self.prefix = settings.pattern_prefix("streams")
        self.buckets = settings.stream_bucket_count
        self.native_read_limits: set[int] = set()
        if not self.prefix or any(c in self.prefix for c in "{}"):
            raise InvalidStreamRequest(
                "Stream prefix must be nonempty and contain no braces"
            )

    def bucket(self, stream: str) -> int:
        try:
            valid = isinstance(stream, str) and 1 <= len(stream.encode()) <= 512
        except UnicodeError:
            valid = False
        if not valid:
            raise InvalidStreamRequest("Stream identity must contain 1–512 UTF-8 bytes")
        return (
            int.from_bytes(hashlib.sha256(stream.encode()).digest(), "big")
            % self.buckets
        )

    def keys(self, stream: str) -> tuple[str, str]:
        bucket = self.bucket(stream)
        encoded = base64.urlsafe_b64encode(stream.encode()).decode().rstrip("=")
        base = f"{self.prefix}:{{streams:v1:bucket:{bucket}}}:{encoded}"
        return f"{base}:events", f"{base}:meta"

    async def initialize(self) -> None:
        # Check every placement bucket, including primaries not serving bucket 0.
        for bucket in range(self.buckets):
            key = f"{self.prefix}:{{streams:v1:bucket:{bucket}}}:probe:{uuid4().hex}"
            try:
                await self.client.execute_command(
                    "XREAD",
                    "COUNT",
                    1,
                    "STREAMS",
                    key,
                    "0-0",
                )
                await self.client.execute_command(
                    "XADD",
                    key,
                    "NOMKSTREAM",
                    "IDMP",
                    "probe",
                    "probe",
                    "*",
                    "probe",
                    "1",
                )
                try:
                    await self.client.execute_command(
                        "XCFGSET", key, "IDMP-DURATION", 1
                    )
                except ResponseError as error:
                    if "no such key" not in str(error).lower():
                        raise
            except RedisError as error:
                raise StreamError(
                    "Streaming requires Redis 8.6 with native IDMP and standard XREAD support"
                ) from error
            # Managed endpoints can report a cluster version different from the
            # command dialect. Probe each placement bucket rather than trusting
            # a version string or requiring INFO permission. Transport errors
            # remain startup failures; unsupported optional syntax selects 8.6.
            from redis.asyncio import RedisCluster

            route = (
                {"target_nodes": self.client.get_node_from_key(key)}
                if isinstance(self.client, RedisCluster)
                else {}
            )
            try:
                await self.client.execute_command(
                    "XREAD",
                    "COUNT",
                    1,
                    "MAXCOUNT",
                    1,
                    "MAXSIZE",
                    1024,
                    "STREAMS",
                    key,
                    "0-0",
                    **route,
                )
            except ResponseError:
                self.native_read_limits.discard(bucket)
            else:
                self.native_read_limits.add(bucket)
        # This manifest intentionally has no TTL: placement must survive restarts.
        layout = json.dumps(
            {"version": 1, "hash": "sha256", "buckets": self.buckets}, sort_keys=True
        )
        manifest = f"{self.prefix}:layout"
        await self.client.set(manifest, layout, nx=True)
        if _normalize(await self.client.get(manifest)) != layout:
            raise StreamConflict("Stream namespace layout differs; use a new namespace")

    @staticmethod
    def results(values: Any) -> Any:
        if any(isinstance(value, RedisError) for value in values):
            raise StreamError("Transaction command failed; no rollback is assumed")
        return _normalize(values)

    def _configuration(self, ttl: int, deadline: int) -> dict[str, str]:
        return {
            "version": "1",
            "ttl": str(ttl),
            "deadline": str(deadline),
            "floor": "0-0",
            "ready": "0",
            "idmp_duration": str(self.settings.stream_idmp_duration),
            "idmp_maxsize": str(self.settings.stream_idmp_maxsize),
        }

    def _validate(
        self,
        state: Any,
        kind: str,
        now: Any,
        payload_expiry: int,
        meta_expiry: int,
        *,
        initializing: bool = False,
    ) -> None:
        if not state:
            if kind != "none":
                raise StreamError("Missing stream metadata")
            raise StreamNotFound()
        try:
            deadline = int(state["deadline"])
            parse_cursor(state["floor"])
            valid = (
                state["version"] == "1"
                and int(state["ttl"]) > 0
                and int(state["idmp_duration"]) > 0
                and int(state["idmp_maxsize"]) > 0
                and state["ready"] in (("0", "1") if initializing else ("1",))
            )
        except (KeyError, TypeError, ValueError) as error:
            raise StreamError("Invalid stream metadata") from error
        if not valid:
            raise StreamError("Incomplete stream initialization")
        if meta_expiry > 0 and now >= 0 and meta_expiry - now >= deadline:
            raise StreamExpired()
        if kind != "stream" or payload_expiry != deadline or meta_expiry <= deadline:
            raise StreamError("Missing or inconsistent stream storage")

    async def _state(
        self, pipe: Any, key: str, meta: str, *, initializing: bool = False
    ) -> dict[str, str]:
        state = _normalize(await pipe.hgetall(meta))
        self._validate(
            state,
            _normalize(await pipe.type(key)),
            await pipe.pttl(meta),
            await pipe.pexpiretime(key),
            await pipe.pexpiretime(meta),
            initializing=initializing,
        )
        return state  # type: ignore[no-any-return]

    async def create(self, stream: str, *, ttl: int) -> None:
        if type(ttl) is not int or not 1 <= ttl <= 2**31 - 1:
            raise InvalidStreamRequest(
                "TTL must be a positive integer no greater than 2147483647"
            )
        key, meta = self.keys(stream)
        for _ in range(32):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    state = _normalize(await pipe.hgetall(meta))
                    if state:
                        state = await self._state(pipe, key, meta, initializing=True)
                        if (
                            int(state["ttl"]),
                            int(state["idmp_duration"]),
                            int(state["idmp_maxsize"]),
                        ) != (
                            ttl,
                            self.settings.stream_idmp_duration,
                            self.settings.stream_idmp_maxsize,
                        ):
                            raise StreamConflict("Creation configuration differs")
                        info = _normalize(await pipe.xinfo_stream(key))
                        if int(info.get("idmp-duration", -1)) != int(
                            state["idmp_duration"]
                        ) or int(info.get("idmp-maxsize", -1)) != int(
                            state["idmp_maxsize"]
                        ):
                            raise StreamError(
                                "Stream idempotency configuration differs"
                            )
                        if state["ready"] == "1":
                            return
                        pipe.multi()
                        pipe.hset(meta, "ready", "1")
                        self.results(await pipe.execute(raise_on_error=False))
                        return
                    if _normalize(await pipe.type(key)) != "none":
                        raise StreamError("Partial stream initialization")
                    from redis.asyncio import RedisCluster

                    if isinstance(self.client, RedisCluster):
                        seconds, micros = await self.client.time(
                            target_nodes=self.client.get_node_from_key(key)
                        )
                    else:
                        seconds, micros = await self.client.time()
                    deadline = seconds * 1000 + micros // 1000 + ttl * 1000
                    pipe.multi()
                    pipe.xadd(key, {"bootstrap": "1"}, id="0-1")
                    pipe.pexpireat(key, deadline)
                    pipe.execute_command(
                        "XCFGSET",
                        key,
                        "IDMP-DURATION",
                        self.settings.stream_idmp_duration,
                        "IDMP-MAXSIZE",
                        self.settings.stream_idmp_maxsize,
                    )
                    pipe.hset(meta, mapping=self._configuration(ttl, deadline))
                    pipe.pexpireat(
                        meta, deadline + self.settings.stream_expiry_recognition * 1000
                    )
                    self.results(await pipe.execute(raise_on_error=False))
                    # Validate committed configuration before publishing readiness.
            except WatchError:
                continue
        raise StreamError("Stream creation contention limit reached")

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
        try:
            if isinstance(event, str):
                event.encode()
            for value in (producer, idempotency_key):
                if isinstance(value, str):
                    value.encode()
        except UnicodeError as error:
            raise InvalidStreamRequest(
                "Event identifiers must be valid UTF-8"
            ) from error
        if (
            not isinstance(event, str)
            or not 1 <= len(event.encode()) <= 256
            or any(c in event for c in "\r\n\0")
            or event.startswith("redis.stream.")
        ):
            raise InvalidStreamRequest("Invalid or reserved event name")
        for identity in (producer, idempotency_key):
            if not isinstance(identity, str) or not 1 <= len(identity.encode()) <= 512:
                raise InvalidStreamRequest(
                    "Producer and idempotency key must contain 1–512 UTF-8 bytes"
                )
        try:
            encoded = json.dumps(
                data, allow_nan=False, ensure_ascii=False, separators=(",", ":")
            )
            encoded.encode("utf-8")
        except (TypeError, ValueError, RecursionError) as error:
            raise InvalidStreamRequest(
                "Event data must be JSON serializable"
            ) from error
        fields = {
            "event": event,
            "data": encoded,
            "producer": producer,
            "iid": idempotency_key,
            "final": str(int(final)),
        }
        if (
            sum(len(k.encode()) + len(v.encode()) for k, v in fields.items())
            > self.settings.stream_max_event_bytes
        ):
            raise InvalidStreamRequest("Event exceeds configured size limit")
        key, meta = self.keys(stream)
        for _ in range(32):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    await self._state(pipe, key, meta)
                    last = _normalize(await pipe.xrevrange(key, count=1))
                    if last and last[0][1].get("final") == "1":
                        if final and (last[0][1]["producer"], last[0][1]["iid"]) == (
                            producer,
                            idempotency_key,
                        ):
                            return str(last[0][0])
                        raise StreamConflict("Stream is sealed")
                    pipe.multi()
                    pipe.execute_command(
                        "XADD",
                        key,
                        "NOMKSTREAM",
                        "IDMP",
                        producer,
                        idempotency_key,
                        "*",
                        *[v for pair in fields.items() for v in pair],
                    )
                    result = self.results(await pipe.execute(raise_on_error=False))[0]
                    if result is None:
                        raise StreamExpired()
                    if final:
                        stored = _normalize(
                            await self.client.xrange(key, min=result, max=result)
                        )
                        if not stored or stored[0][1].get("final") != "1":
                            raise StreamConflict(
                                "Finish identity was already used by an ordinary event"
                            )
                    await self._trim(stream)
                    return str(result)
            except WatchError:
                continue
        raise StreamError("Stream append contention limit reached")

    async def read(self, stream: str, after: str = "0-0", count: int = 32) -> _Batch:
        position = parse_cursor(after)
        key, meta = self.keys(stream)
        async with self.client.pipeline(transaction=True) as pipe:
            pipe.hgetall(meta)
            pipe.type(key)
            pipe.pttl(meta)
            pipe.pexpiretime(key)
            pipe.pexpiretime(meta)
            pipe.xrange(
                key, min=f"({after}", count=count + (1 if position < (0, 1) else 0)
            )
            pipe.xrevrange(key, count=1)
            pipe.xinfo_stream(key)
            state, kind, now, payload_expiry, meta_expiry, rows, last, info = (
                _normalize(await pipe.execute(raise_on_error=False))
            )
        self._validate(state, kind, now, payload_expiry, meta_expiry)
        self.results([rows, last, info])
        # XDEL records its highest removed ID in the Stream itself, atomically
        # with deletion. It survives a failed metadata update in the same EXEC.
        deleted = info.get("max-deleted-entry-id", "0-0")
        floor = max(parse_cursor(state["floor"]), parse_cursor(deleted))
        if position < floor:
            raise StreamHistoryUnavailable()
        tail = last[0][0] if last and "bootstrap" not in last[0][1] else state["floor"]
        if position > parse_cursor(tail):
            raise StreamCursorAhead()
        events = [StreamEvent._parse(row) for row in rows if "bootstrap" not in row[1]][
            :count
        ]
        return _Batch(events, tail, bool(last and last[0][1].get("final") == "1"))

    async def _trim(self, stream: str) -> None:
        key, meta = self.keys(stream)
        for _ in range(32):
            try:
                async with self.client.pipeline(transaction=True) as pipe:
                    await pipe.watch(key, meta)
                    state = await self._state(pipe, key, meta)
                    # Keep the bootstrap entry and at least one application event
                    # (therefore the authoritative final marker). Delete bounded
                    # prefixes; XDEL updates max-deleted-entry-id in the same
                    # command, providing a durable floor even if HSET fails.
                    length = await pipe.xlen(key)
                    excess = length - self.settings.stream_max_events - 1
                    if excess <= 0:
                        return
                    rows = _normalize(
                        await pipe.xrange(key, min="(0-1", count=min(excess, 32))
                    )
                    if not rows or any(row[1].get("final") == "1" for row in rows):
                        raise StreamError("Invalid retention boundary")
                    floor = max(parse_cursor(state["floor"]), parse_cursor(rows[-1][0]))
                    pipe.multi()
                    pipe.xdel(key, *[row[0] for row in rows])
                    pipe.hset(meta, "floor", f"{floor[0]}-{floor[1]}")
                    self.results(await pipe.execute(raise_on_error=False))
                    if excess <= len(rows):
                        return
            except WatchError:
                continue
        raise StreamError("Stream retention contention limit reached")


class StreamBackend:
    """Async finite event logs using caller-owned Redis clients.

    Enter as a context manager before subscribing. Publishing-only workers can
    call ``initialize()`` without starting a relay. Caller-owned clients are
    never closed by this backend; separate blocking resources are required for
    subscriptions.
    """

    def __init__(
        self,
        client: Any,
        *,
        settings: StreamSettings | None = None,
        blocking_client: Any = None,
    ):
        # Project the environment adapter onto the explicit model. No environment
        # discovery occurs here, including when settings are omitted.
        if isinstance(settings, RedisSettings):
            settings = StreamSettings.model_validate(
                {name: getattr(settings, name) for name in StreamSettings.model_fields}
            )
        self.settings = settings if settings is not None else StreamSettings()
        if (
            self.settings.stream_replay_bytes
            < self.settings.stream_max_event_bytes + 512
        ):
            raise InvalidStreamRequest(
                "Replay budget must accommodate one maximum event"
            )
        if self.settings.stream_read_maxbytes < _minimum_live_read_bytes(self.settings):
            raise InvalidStreamRequest(
                "Live read budget must accommodate one maximum event, key and framing"
            )
        self._store = _StreamStore(client, self.settings)
        self._blocking_client = blocking_client
        self._relay: Any = None
        self._initialized = False
        self._runtime_active = False

    async def initialize(self) -> None:
        if self._initialized:
            return
        try:
            await self._store.initialize()
        except RedisError as error:
            raise StreamError() from error
        self._initialized = True

    async def __aenter__(self) -> StreamBackend:
        if self._runtime_active:
            raise RuntimeError("StreamBackend runtime is already started")
        command_pool = getattr(self._store.client, "connection_pool", None)
        if (
            self._blocking_client is None
            or self._blocking_client is self._store.client
            or (
                command_pool is not None
                and command_pool
                is getattr(self._blocking_client, "connection_pool", None)
            )
        ):
            raise ValueError("Subscriptions require separate blocking Redis resources")
        self._runtime_active = True
        try:
            await self.initialize()
            from redis_fastapi._stream_relay import _StreamRelay

            self._relay = _StreamRelay(self._store, self._blocking_client)
            await self._relay.start()
        except BaseException:
            await self.__aexit__()
            raise
        return self

    async def __aexit__(self, *args: Any) -> None:
        import anyio

        try:
            if self._relay is not None:
                with anyio.CancelScope(shield=True):
                    await self._relay.stop()
        finally:
            self._relay = None
            self._runtime_active = False

    async def create(self, stream: str, *, ttl: int) -> None:
        await self.initialize()
        try:
            await self._store.create(stream, ttl=ttl)
        except RedisError as error:
            raise StreamError() from error

    async def append(
        self, stream: str, *, event: str, data: Any, producer: str, idempotency_key: str
    ) -> str:
        return await self._append(
            stream,
            event=event,
            data=data,
            producer=producer,
            idempotency_key=idempotency_key,
        )

    async def finish(
        self, stream: str, *, event: str, data: Any, producer: str, idempotency_key: str
    ) -> str:
        return await self._append(
            stream,
            event=event,
            data=data,
            producer=producer,
            idempotency_key=idempotency_key,
            final=True,
        )

    async def _append(self, stream: str, **kwargs: Any) -> str:
        started = time.monotonic()
        operation = "finish" if kwargs.get("final") else "append"
        result = "success"
        try:
            await self.initialize()
            return await self._store.append(stream, **kwargs)
        except RedisError as error:
            result = "error"
            raise StreamError() from error
        except StreamError:
            result = "error"
            raise
        finally:
            record_stream("operations", operation=operation, result=result)
            record_stream(
                "operation_duration", time.monotonic() - started, operation=operation
            )

    @asynccontextmanager
    async def subscribe(
        self, stream: str, *, after: str | None = None
    ) -> AsyncIterator[AsyncIterator[StreamEvent]]:
        if self._relay is None:
            raise StreamError("Stream runtime is not running")
        async with self._relay.subscribe(
            stream, after=after if after is not None else "0-0"
        ) as subscription:
            yield subscription.events()
