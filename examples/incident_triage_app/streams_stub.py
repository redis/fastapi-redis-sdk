"""An in-memory stand-in for the SDK's resumable streams feature.

The streams feature (``specs/resumable-streams-design.md``) does not exist
yet.  This module copies its public API - ``StreamBackend``, ``StreamWriter``,
``StreamEvent``, ``StreamBackendDep`` and the exception family - so the demo's
endpoints are written against the real names.  When the feature lands, the
imports change and the endpoints stay as they are.

What it keeps from the design:

* entry IDs in Redis's ``<ms>-<seq>`` form, and resume from any of them;
* one active run per scope, and a retried start that attaches to it;
* names derived from the scope: a stream is found by (scope, ID), so an ID
  from another scope gets the same 404 as one that does not exist;
* the connection deadline: ``expires_at`` (for example the session's) and a
  cap on connection lifetime;
* one end marker: the final entry, written by ``finish()`` or by ``spawn()``
  as ``stream.end``, ends every reader, and a reconnect past it gets a 204;
* the ``alive`` heartbeat; when it runs out, the reader ends the stream
  with ``stream.end`` and reason ``producer_lost``, so reconnects get a 204;
* TTLs, ``410`` for an expired stream, and a cap on live readers;
* support detection, as the SDK's other features do it: ``COMMAND INFO
  XCFGSET DELEX`` once at startup; without them the feature turns off, every
  entry point raises ``StreamUnsupported`` (503), and the app keeps running;
* a send timeout: a reader whose client stops reading is closed, so it frees
  its reader slot;
* the cursor rules: handler value, then ``Last-Event-ID``, then ``?after=``;
  numeric comparison; no cursor means ``0-0``; duplicates and malformed values
  get ``422``, and a cursor past the stream's last ID gets ``409``.

What it does not do: store anything in Redis.  The state lives in this
process, so it does not survive a restart and is not shared between replicas.
``inspect()`` reports it under the key names the real feature will use, so the
demo's Redis lens shows the same rows.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import re
import time
from collections.abc import AsyncIterator, Awaitable, Callable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Annotated, Any

from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import RedisError
from starlette.types import Message, Receive, Scope, Send

from redis_fastapi import SessionConfigurationError, get_settings
from redis_fastapi.sessions import session_state_of

logger = logging.getLogger(__name__)

RESERVED = "stream."  # the SDK's own event types
_CURSOR = re.compile(r"^\d+-\d+$")
# No braces, so no value can break out of the hash tag; no ":" in an ID, so
# "<scope>:<id>" splits in exactly one way.
_SCOPE = re.compile(r"^[A-Za-z0-9_.:-]{1,200}$")
_ID = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


# -- Settings ---------------------------------------------------------------


@dataclass(frozen=True)
class StreamSettings:
    """The design's ``stream_*`` settings (section 10)."""

    ttl_active: int = 600
    ttl_done: int = 120
    maxlen: int = 10_000
    heartbeat_seconds: float = 15
    alive_ttl_seconds: float = 45
    max_readers: int = 256
    max_connection_seconds: float = 300
    send_timeout_seconds: float = 10


# -- Exceptions -------------------------------------------------------------


class StreamError(Exception):
    """Base class for stream errors."""


class StreamNotFound(StreamError):
    """No stream under this (scope, ID).  Answered with 404."""


class StreamExpired(StreamError):
    """The stream, or the entries after a cursor, are gone.  Answered with 410."""


class StreamBusy(StreamError):
    """The scope already has an active run.  Answered with 409."""

    def __init__(self, stream_id: str) -> None:
        super().__init__(f"scope busy with {stream_id}")
        self.stream_id = stream_id


class StreamUnsupported(StreamError):
    """The server lacks the commands this feature needs.  Answered with 503."""


class StreamFinished(StreamError):
    """An append after the final entry.  The writer refuses it."""


class _SlowConsumer(Exception):
    """A send to the client took longer than ``send_timeout_seconds``."""


class _SSEResponse(StreamingResponse):
    """``StreamingResponse`` that holds a reader slot and limits every send.

    The slot is taken when the response runs, before any header goes out, and
    released in a ``finally``.  A response that is built but never sent - an
    error in middleware, a client gone before the first byte - so never takes
    a slot, and one that runs releases it on every exit path.  Over the limit
    the response is a 503 instead, still before the first byte.

    A client that stops reading fills the socket buffer, and ``send()`` then
    waits.  Past the limit the response closes with no event - none could get
    through a blocked socket - and a live client reconnects with
    ``Last-Event-ID``.  This frees the reader slot.  It does not catch a dead
    client on an idle stream, whose small pings never fill the buffer; the
    connection cap bounds that case.
    """

    def __init__(
        self,
        content: AsyncIterator[str],
        *,
        send_timeout: float,
        on_timeout: Callable[[], None],
        reserve: Callable[[], bool],
        release: Callable[[], None],
        **kwargs: Any,
    ) -> None:
        super().__init__(content, **kwargs)
        self._reserve = reserve
        self._release = release
        self._send_timeout = send_timeout
        self._on_timeout = on_timeout

    async def stream_response(self, send: Send) -> None:
        async def timed(message: Message) -> None:
            try:
                await asyncio.wait_for(send(message), self._send_timeout)
            except asyncio.TimeoutError:
                raise _SlowConsumer from None

        try:
            await super().stream_response(timed)
        finally:
            # An async for that stops early does not close its generator, and
            # the reader slot is released in the generator's finally.
            aclose = getattr(self.body_iterator, "aclose", None)
            if aclose is not None:
                await aclose()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._reserve():
            busy = _error(503, "Too many live readers", **{"Retry-After": "5"})
            # The body was never started; close it so nothing it holds lingers.
            aclose = getattr(self.body_iterator, "aclose", None)
            if aclose is not None:
                await aclose()
            await busy(scope, receive, send)
            return
        try:
            await super().__call__(scope, receive, send)
        except _SlowConsumer:
            self._on_timeout()  # return early; the server closes the connection
        finally:
            self._release()


class CredentialExpired(StreamError):
    """``expires_at`` has already passed.  Answered with 401."""


class StreamInvalidCursor(StreamError, ValueError):
    """A malformed, out-of-range or repeated cursor.  Answered with 422."""


class StreamCursorAhead(StreamError):
    """A cursor past the stream's last generated ID.  Answered with 409."""


# -- Data -------------------------------------------------------------------


@dataclass(frozen=True)
class StreamEvent:
    id: str | None  # Redis entry ID; the cursor.  None for a reader's own event.
    type: str
    data: Any
    final: bool = False  # True on the stream's final entry


def _key(entry_id: str) -> tuple[int, int]:
    """An entry ID as numbers.  As strings, "999-0" would sort after "1000-0"."""
    ms, seq = entry_id.split("-")
    return int(ms), int(seq)


_MAX_PART = 2**64 - 1


def _parse_cursor(value: str) -> tuple[int, int]:
    """Validate a client cursor.  ``$``, ``+``, ``-`` and bare ``<ms>`` fail."""
    if not _CURSOR.match(value):
        raise StreamInvalidCursor(value)
    key = _key(value)
    if key[0] > _MAX_PART or key[1] > _MAX_PART:
        raise StreamInvalidCursor(value)
    return key


def select_cursor(request: Request, after: str | None = None) -> str | None:
    """The cursor for *request*: handler value, then Last-Event-ID, then ?after=.

    EventSource sends Last-Event-ID on its own reconnects but keeps the query
    cursor it was built with, so the header is the newer of the two.  Every
    value present is validated, and a repeated header or query is refused.
    """
    headers = request.headers.getlist("last-event-id")
    queries = request.query_params.getlist("after")
    if len(headers) > 1 or len(queries) > 1:
        raise StreamInvalidCursor("more than one cursor")
    for value in (after, *headers, *queries):
        if value is not None:
            _parse_cursor(value)
    if after is not None:
        return after
    if headers:
        return headers[0]
    return queries[0] if queries else None


@dataclass
class _Stream:
    id: str
    scope: str
    exclusive: bool = True
    entries: list[StreamEvent] = field(default_factory=list)
    status: str = "running"
    cancel: bool = False
    expires_at: float = 0.0
    alive_until: float | None = None
    trimmed_up_to: str | None = None
    last_generated: str | None = None  # XINFO STREAM last-generated-id
    task: asyncio.Task[None] | None = None
    changed: asyncio.Condition = field(default_factory=asyncio.Condition)

    final_id: str | None = None  # meta field ``final``

    @property
    def finished(self) -> bool:
        return self.final_id is not None

    def producer_alive(self, now: float) -> bool:
        return self.alive_until is not None and now < self.alive_until


# -- Writer -----------------------------------------------------------------


class StreamWriter:
    """Appends to one stream.  Made by :meth:`StreamBackend.open`."""

    def __init__(self, backend: StreamBackend, stream: _Stream) -> None:
        self._backend = backend
        self._stream = stream
        self.stream_id = stream.id

    async def append(self, type: str, data: Any) -> str:
        """Add one entry and return its ID.

        Raises ``asyncio.CancelledError`` when the run was cancelled, which is
        how the real feature turns the cancel flag into task cancellation.
        """
        _check_type(type)
        if self._stream.finished:
            raise StreamFinished(self.stream_id)
        if self._stream.cancel:
            raise asyncio.CancelledError
        return await self._backend._append(self._stream, type, data)

    async def finish(self, type: str, data: Any = None) -> str:
        """Write the final entry, of the application's type, and seal the writer."""
        _check_type(type)
        return await self._end(type, data)

    async def _end(self, type: str, data: Any) -> str:
        # Every final entry shares one IDMP pair, so a second end - from a
        # slow producer after a reader ended its stream - returns the first.
        return await self._backend._finish(self._stream, type, data)


def _check_name(scope: str, stream_id: str | None) -> None:
    """Reject a scope or ID that could break the key's hash tag."""
    if not _SCOPE.match(scope):
        raise ValueError(f"invalid scope {scope!r}")
    if stream_id is not None and not _ID.match(stream_id):
        raise ValueError(f"invalid stream ID {stream_id!r}")


_MAX_TYPE = 128


def _check_type(type: str) -> None:
    """Reject a type that could break an SSE frame, or that the SDK reserves.

    The type goes into the ``event:`` line as it is, so a line break would
    end the frame and could start a forged one, with its own ``id:``.
    """
    if not type or len(type) > _MAX_TYPE or any(c in type for c in "\r\n\0"):
        raise ValueError(f"invalid event type {type!r}")
    if type.startswith(RESERVED):
        raise ValueError(f"{type!r}: types starting with {RESERVED!r} are the SDK's")


def _sse_frame(type: str, data: str, id: str | None = None) -> str:
    """One SSE frame.  Multi-line data becomes several ``data:`` lines.

    JSON never holds a raw line break, but a custom coder might; the browser
    joins the ``data:`` lines back with ``\n``.
    """
    head = f"id: {id}\n" if id else ""
    lines = data.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    body = "".join(f"data: {line}\n" for line in lines)
    return f"{head}event: {type}\n{body}\n"


async def probe_streams_support(
    client: AsyncRedis | AsyncRedisCluster,
) -> bool | None:
    """Ask the server whether it has ``XCFGSET`` and ``DELEX``.

    ``XCFGSET`` arrived in Redis 8.6 with ``XADD ... IDMP``, and ``DELEX`` with
    ``SET ... IFEQ`` in 8.4, so both present means the commands this feature
    uses exist.  Like the SDK's ``INCREX`` and ``HSETEX`` probes it asks
    ``COMMAND INFO`` rather than reading an error, because on Cluster
    redis-py rejects an unknown command client-side.  ``None`` means the
    question could not be answered.  The probe cannot tell 8.6.0 or 8.6.1,
    with their IDMP defects, from 8.6.2.
    """
    try:
        if isinstance(client, AsyncRedisCluster):
            reply = await client.execute_command(
                "COMMAND",
                "INFO",
                "XCFGSET",
                "DELEX",
                target_nodes=AsyncRedisCluster.RANDOM,
            )
        else:
            reply = await client.execute_command(  # type: ignore[no-untyped-call]
                "COMMAND", "INFO", "XCFGSET", "DELEX"
            )
    except TypeError:
        # redis-py indexes into a nil entry for a command the server lacks.
        return False
    except (RedisError, OSError):
        return None
    if isinstance(reply, dict):
        entries = list(reply.values())
    elif isinstance(reply, (list, tuple)):
        entries = list(reply)
    else:
        return False
    return len(entries) == 2 and all(entry for entry in entries)


# -- Backend ----------------------------------------------------------------


class StreamBackend:
    """The design's ``StreamBackend`` (section 8), kept in memory."""

    def __init__(self, settings: StreamSettings | None = None) -> None:
        self.settings = settings or StreamSettings()
        # Keyed by (scope, ID), as the real keys are: stream:{<scope>:<id>}.
        self._streams: dict[tuple[str, str], _Stream] = {}
        self._active: dict[str, str] = {}
        self._expired: set[tuple[str, str]] = set()
        self._tasks: set[asyncio.Task[None]] = set()
        self._readers = 0
        self.slow_consumer_disconnects = 0  # the design's telemetry counter
        self.supported: bool | None = None  # None: not yet known
        self._last_ms = 0
        self._seq = 0

    # -- writer side ----------------------------------------------------------

    async def detect(self, client: AsyncRedis | AsyncRedisCluster) -> bool | None:
        """Detect server support once, as the rate limiter and sessions do.

        A result of ``None`` (Redis unreachable) is not cached, so a later call
        asks again.  A missing command turns the feature off with one warning.
        """
        if self.supported is not None:
            return self.supported
        result = await probe_streams_support(client)
        if result is False:
            logger.warning(
                "Resumable streams are off: this Redis has no XCFGSET / DELEX; "
                "they need Redis 8.6.2 or later"
            )
        self.supported = result
        return result

    def _require_support(self) -> None:
        if self.supported is False:
            raise StreamUnsupported("resumable streams need Redis 8.6.2 or later")

    async def open(
        self, *, scope: str, stream_id: str | None = None, exclusive: bool = True
    ) -> StreamWriter:
        """Start a stream, or attach to the same one on a retried start.

        With ``exclusive`` the run takes the scope's active slot.
        """
        self._require_support()
        _check_name(scope, stream_id)
        now = time.monotonic()
        if exclusive:
            current = self._active_stream(scope, now)
            if current is not None:
                if current.id == stream_id:
                    return StreamWriter(self, current)
                raise StreamBusy(current.id)
        stream_id = stream_id or f"ru_{int(time.time() * 1000):x}"
        if (scope, stream_id) in self._streams:
            raise StreamBusy(stream_id)
        stream = _Stream(
            id=stream_id,
            scope=scope,
            exclusive=exclusive,
            expires_at=now + self.settings.ttl_active,
            alive_until=now + self.settings.alive_ttl_seconds,
        )
        self._streams[(scope, stream_id)] = stream
        if exclusive:
            self._active[scope] = stream_id
        return StreamWriter(self, stream)

    def spawn(
        self, writer: StreamWriter, fn: Callable[[StreamWriter], Awaitable[None]]
    ) -> asyncio.Task[None]:
        """Run *fn* as a task the backend owns.

        If *fn* has not called ``finish()``, write a ``stream.end`` final entry
        whose data gives the reason, so no reader waits for a run that is over.
        """
        stream = writer._stream

        async def end(data: dict[str, Any]) -> None:
            if not stream.finished:
                await writer._end("stream.end", data)

        async def run() -> None:
            beat = asyncio.create_task(self._keepalive(stream))
            try:
                await fn(writer)
            except asyncio.CancelledError:
                await end({"reason": "cancelled"})
                return
            except Exception as exc:  # the run failed; readers must see why
                code = getattr(exc, "code", "producer_error")
                await end({"reason": "error", "code": code})
                return
            finally:
                beat.cancel()
            await end({"reason": "done"})

        task = asyncio.create_task(run())
        stream.task = task
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def _keepalive(self, stream: _Stream) -> None:
        """Refresh ``alive`` and turn the cancel flag into cancellation."""
        while True:
            await asyncio.sleep(self.settings.heartbeat_seconds)
            stream.alive_until = time.monotonic() + self.settings.alive_ttl_seconds
            if stream.cancel and stream.task is not None:
                stream.task.cancel()

    def _next_id(self) -> str:
        ms = int(time.time() * 1000)
        if ms <= self._last_ms:
            ms, self._seq = self._last_ms, self._seq + 1
        else:
            self._seq = 0
        self._last_ms = ms
        return f"{ms}-{self._seq}"

    async def _append(self, stream: _Stream, type: str, data: Any) -> str:
        now = time.monotonic()
        if stream.status != "running" or now >= stream.expires_at:
            raise StreamExpired(stream.id)
        event = StreamEvent(self._next_id(), type, data)
        stream.entries.append(event)
        stream.last_generated = event.id
        if len(stream.entries) > self.settings.maxlen:
            dropped = stream.entries.pop(0)
            stream.trimmed_up_to = dropped.id
        stream.expires_at = now + self.settings.ttl_active
        stream.alive_until = now + self.settings.alive_ttl_seconds
        async with stream.changed:
            stream.changed.notify_all()
        return event.id  # type: ignore[return-value]

    async def _finish(self, stream: _Stream, type: str, data: Any) -> str:
        """The final entry, ``status=finished`` and ``final=<id>`` in one step.

        A second call returns the first final entry's ID, as the fixed
        ``IDMP end final`` pair does in Redis, so a stream has one final entry.
        """
        if stream.final_id is not None:
            return stream.final_id
        event = StreamEvent(self._next_id(), type, data, final=True)
        stream.entries.append(event)
        stream.last_generated = event.id
        stream.final_id = event.id
        stream.status = "finished"
        stream.expires_at = time.monotonic() + self.settings.ttl_done
        stream.alive_until = None
        if stream.exclusive and self._active.get(stream.scope) == stream.id:
            del self._active[stream.scope]  # DELEX ... IFEQ <id>
        async with stream.changed:
            stream.changed.notify_all()
        return event.id  # type: ignore[return-value]

    # -- reader side ----------------------------------------------------------

    def _get(self, stream_id: str, scope: str) -> _Stream:
        """The stream named by (scope, ID).  An ID from another scope names none."""
        _check_name(scope, None)
        if not _ID.match(stream_id):
            raise StreamNotFound(stream_id)  # a malformed ID names no stream
        stream = self._streams.get((scope, stream_id))
        if stream is not None and time.monotonic() >= stream.expires_at:
            self._forget(stream)
            stream = None
        if stream is None:
            if (scope, stream_id) in self._expired:
                raise StreamExpired(stream_id)
            raise StreamNotFound(stream_id)
        return stream

    def _forget(self, stream: _Stream) -> None:
        self._streams.pop((stream.scope, stream.id), None)
        if stream.exclusive and self._active.get(stream.scope) == stream.id:
            del self._active[stream.scope]
        self._expired.add((stream.scope, stream.id))
        if len(self._expired) > 10_000:
            self._expired.clear()

    def _check_cursor(
        self, stream: _Stream, after: str | None
    ) -> tuple[int, int] | None:
        """The cursor as numbers, or ``None`` when the reader has read the end.

        In the design's order: ahead of the stream (409), then at or past the
        final entry (204), then a gap (410).  No cursor means ``0-0``, the
        whole stream, so it fails after any trim.
        """
        cursor = _parse_cursor(after) if after is not None else (0, 0)
        last = stream.last_generated
        if cursor != (0, 0) and (last is None or cursor > _key(last)):
            raise StreamCursorAhead(stream.id)  # e.g. writes lost in a failover
        final = stream.final_id
        if final is not None and cursor >= _key(final):
            return None
        trimmed = stream.trimmed_up_to
        if trimmed is not None and cursor < _key(trimmed):
            raise StreamExpired(stream.id)  # an entry after the cursor was trimmed
        return cursor

    async def follow(
        self, stream_id: str, *, scope: str, after: str | None = None
    ) -> AsyncIterator[StreamEvent | None]:
        """Replay the entries after *after*, then follow the live stream.

        Yields ``None`` as a keepalive after each quiet heartbeat.
        """
        self._require_support()
        stream = self._get(stream_id, scope)
        cursor = self._check_cursor(stream, after)
        if cursor is None:
            return  # already read the end
        while True:
            # Scan under the lock, so an append between the scan and the wait
            # cannot be missed; yield outside it, so a slow reader blocks no one.
            async with stream.changed:
                fresh = [e for e in stream.entries if _key(e.id) > cursor]  # type: ignore[arg-type]
                if not fresh:
                    try:
                        await asyncio.wait_for(
                            stream.changed.wait(), self.settings.heartbeat_seconds
                        )
                        continue
                    except asyncio.TimeoutError:
                        pass
            for event in fresh:
                cursor = _key(event.id)  # type: ignore[arg-type]
                yield event
                if event.final:
                    return
            if fresh:
                continue
            if not stream.finished and not stream.producer_alive(time.monotonic()):
                # The stream would never get its end marker, so the reader
                # writes it.  The next pass yields it like any final entry.
                await self._finish(stream, "stream.end", {"reason": "producer_lost"})
                continue
            yield None

    def sse(
        self,
        stream_id: str,
        *,
        request: Request,
        scope: str,
        after: str | None = None,
        expires_at: datetime | None = None,
    ) -> Response:
        """An SSE response.  Errors before the first byte raise, as in the design.

        The response reads ``Last-Event-ID`` and ``?after=`` itself.  *after* is
        only for a cursor the application knows better, and it wins over both.

        The response ends at *expires_at* with a ``credential_expired`` error,
        and after ``max_connection_seconds`` with no event, so the browser
        reconnects through the application's access check.  Both are checked
        at each entry and heartbeat, so either can be up to one heartbeat late.
        """
        self._require_support()
        after = select_cursor(request, after)
        stream = self._get(stream_id, scope)
        if self._check_cursor(stream, after) is None:
            return Response(status_code=204)  # EventSource stops for good
        now = time.monotonic()
        credential_end = None
        if expires_at is not None:
            remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
            if remaining <= 0:
                raise CredentialExpired(stream_id)
            credential_end = now + remaining
        lifetime_end = now + self.settings.max_connection_seconds

        async def body() -> AsyncIterator[str]:
            # The reader slot is held by _SSEResponse, not here.
            yield f"retry: {int(self.settings.heartbeat_seconds * 1000)}\n\n"
            async for event in self.follow(stream_id, scope=scope, after=after):
                if event is None:
                    yield ": ping\n\n"
                else:
                    yield _sse_frame(event.type, json.dumps(event.data), event.id)
                now = time.monotonic()
                if credential_end is not None and now >= credential_end:
                    data = json.dumps({"code": "credential_expired"})
                    yield _sse_frame("stream.error", data)
                    return
                if now >= lifetime_end:
                    return

        def slow() -> None:
            self.slow_consumer_disconnects += 1

        def reserve() -> bool:
            if self._readers >= self.settings.max_readers:
                return False
            self._readers += 1
            return True

        def release() -> None:
            self._readers -= 1

        return _SSEResponse(
            body(),
            send_timeout=self.settings.send_timeout_seconds,
            on_timeout=slow,
            reserve=reserve,
            release=release,
            media_type="text/event-stream",
            headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
        )

    async def cancel(self, stream_id: str, *, scope: str) -> bool:
        self._require_support()
        stream = self._get(stream_id, scope)
        if stream.finished:
            return False
        stream.cancel = True
        if stream.task is not None:
            stream.task.cancel()
        return True

    async def active(self, scope: str) -> str | None:
        self._require_support()
        stream = self._active_stream(scope, time.monotonic())
        return stream.id if stream is not None else None

    def _active_stream(self, scope: str, now: float) -> _Stream | None:
        stream_id = self._active.get(scope)
        stream = self._streams.get((scope, stream_id)) if stream_id else None
        if stream is None or now >= stream.expires_at:
            if stream is not None:
                self._forget(stream)
            self._active.pop(scope, None)
            return None
        if not stream.producer_alive(now):
            # A dead producer cannot hold the slot, or one crash would block
            # its scope until the stream TTL ran out.
            del self._active[scope]
            return None
        return stream

    def inspect(self, stream_id: str, *, scope: str) -> dict[str, Any] | None:
        """The rows the Redis lens shows, under the real feature's key names."""
        try:
            stream = self._get(stream_id, scope)
        except StreamError:
            return None
        now = time.monotonic()
        ttl = max(0, int(stream.expires_at - now))
        alive_until = stream.alive_until
        alive_ms = (
            int((alive_until - now) * 1000)
            if alive_until is not None and now < alive_until
            else None
        )
        prefix = get_settings().prefix  # REDIS_PREFIX, as for every feature
        tag = f"{{{stream.scope}:{stream.id}}}"
        keys: dict[str, Any] = {
            f"{prefix}:stream:{tag}": {
                "XLEN": len(stream.entries),
                "last ID": stream.entries[-1].id if stream.entries else None,
                "TTL": ttl,
            },
            f"{prefix}:stream-meta:{tag}": {
                "status": stream.status,
                "cancel": int(stream.cancel),
                "final": stream.final_id,
                "TTL": ttl,
            },
            f"{prefix}:stream-alive:{tag}": (
                {"PTTL": alive_ms} if alive_ms is not None else None
            ),
        }
        if stream.exclusive and self._active.get(stream.scope) == stream.id:
            keys[f"{prefix}:stream-active:{stream.scope}"] = {
                "value": stream.id,
                "TTL": ttl,
            }
        return keys

    async def shutdown(self) -> None:
        """Cancel the remaining producers; each writes a ``stream.end`` entry."""
        for task in list(self._tasks):
            task.cancel()
        for task in list(self._tasks):
            with contextlib.suppress(asyncio.CancelledError):
                await task


# -- App wiring -------------------------------------------------------------


def _error(status: int, detail: str, **headers: str) -> JSONResponse:
    return JSONResponse({"detail": detail}, status_code=status, headers=headers or None)


def install(app: FastAPI, settings: StreamSettings | None = None) -> StreamBackend:
    """What ``FastAPIRedis(app).streams()`` will do: a backend and its handlers."""
    backend = StreamBackend(settings)
    app.state.streams = backend

    async def not_found(request: Request, exc: Exception) -> Response:
        return _error(404, "Not Found")

    async def expired(request: Request, exc: Exception) -> Response:
        return _error(410, "Stream expired")

    async def busy(request: Request, exc: Exception) -> Response:
        return _error(409, "A run is already active")

    async def bad_cursor(request: Request, exc: Exception) -> Response:
        return _error(422, "Invalid cursor")

    async def cursor_ahead(request: Request, exc: Exception) -> Response:
        return _error(409, "cursor_ahead")

    async def credential_expired(request: Request, exc: Exception) -> Response:
        return _error(401, "Credential expired")

    app.add_exception_handler(StreamNotFound, not_found)
    app.add_exception_handler(StreamExpired, expired)
    app.add_exception_handler(StreamBusy, busy)
    app.add_exception_handler(StreamInvalidCursor, bad_cursor)
    app.add_exception_handler(StreamCursorAhead, cursor_ahead)
    app.add_exception_handler(CredentialExpired, credential_expired)

    async def unsupported(request: Request, exc: Exception) -> Response:
        return _error(503, f"Resumable streams are off: {exc}")

    app.add_exception_handler(StreamUnsupported, unsupported)
    return backend


def get_stream_backend(request: Request) -> StreamBackend:
    backend: StreamBackend = request.app.state.streams
    return backend


StreamBackendDep = Annotated[StreamBackend, Depends(get_stream_backend)]


def session_expiry(request: Request) -> datetime | None:
    """The absolute deadline of the request's SDK session, if it has one.

    Redis counts the deadline down; ``absolute_remaining`` is its ``TTL``.
    """
    try:
        remaining = session_state_of(request).absolute_remaining
    except SessionConfigurationError:  # no session middleware on this app
        return None
    if remaining is None or remaining <= 0:
        return None
    return datetime.fromtimestamp(time.time() + remaining, tz=timezone.utc)
