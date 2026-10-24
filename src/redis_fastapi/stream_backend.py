"""Resumable streams: a Redis-backed event log a client can resume.

One Redis Stream holds one run - one agent turn, for example.  The entry ID is
the cursor and the SSE ``id:``, so a client that reconnects with
``Last-Event-ID`` gets exactly the entries after the last one it saw.  The run
is decoupled from the HTTP request: a disconnect never stops it.

Keys, all built from the SDK's ``prefix`` setting::

    <prefix>:stream:{<scope>:<id>}        Stream   entries
    <prefix>:stream-meta:{<scope>:<id>}   Hash     status, cancel, final, writer, started
    <prefix>:stream-alive:{<scope>:<id>}  String   producer heartbeat
    <prefix>:stream-active:<scope>        String   the scope's active run

The scope is part of every stream key, so a stream is reachable only through
the scope the application authorized, and the ``{<scope>:<id>}`` hash tag keeps
a stream's three keys in one Cluster slot.

Entry fields: ``t`` (type), ``d`` (payload), ``q`` (writer sequence number) and
``f`` (``1`` on the final entry only).

The HTTP side - the SSE response, cursor selection and the exception
handlers - lives in :mod:`redis_fastapi.streams`.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import re
import secrets
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Any, TypeAlias

from redis.asyncio import Redis as AsyncRedis
from redis.asyncio.cluster import RedisCluster as AsyncRedisCluster
from redis.exceptions import ConnectionError as RedisConnectionError
from redis.exceptions import NoPermissionError, RedisClusterException, RedisError
from redis.exceptions import TimeoutError as RedisTimeoutError

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    StreamBusy,
    StreamCursorAhead,
    StreamError,
    StreamExpired,
    StreamFenced,
    StreamFinished,
    StreamInvalidCursor,
    StreamNotFound,
    StreamStoreError,
    StreamUnsupported,
)
from redis_fastapi.telemetry import (
    add_stream_readers,
    record_stream_operation,
    stream_span,
    timed_stream,
)
from redis_fastapi.types import Coder, JsonCoder

if TYPE_CHECKING:
    from starlette.requests import Request
    from starlette.responses import Response

logger = logging.getLogger(__name__)

AsyncClient: TypeAlias = AsyncRedis | AsyncRedisCluster

# Every driver failure the feature interprets.  ``RedisClusterException`` is
# not a ``RedisError``; see the same tuple in ``session_backend``.
STORE_ERRORS: tuple[type[BaseException], ...] = (
    RedisError,
    RedisClusterException,
    OSError,
)

# Failures after which Redis may or may not have applied a write.  Every write
# here is safe to repeat (IDMP, EXPIRE, HSET, SET), so these are retried.
_AMBIGUOUS_ERRORS: tuple[type[BaseException], ...] = (
    RedisConnectionError,
    RedisTimeoutError,
    OSError,
)
_WRITE_ATTEMPTS = 3

# What ending a stream from ``spawn()`` may fail with, logged and not raised.
_END_ERRORS: tuple[type[BaseException], ...] = (StreamError, *STORE_ERRORS)

# Entry fields.
F_TYPE = "t"
F_DATA = "d"
F_SEQ = "q"
F_FINAL = "f"

# Meta fields.
M_STATUS = "status"
M_CANCEL = "cancel"
M_FINAL = "final"
M_WRITER = "writer"
M_STARTED = "started"

STATUS_RUNNING = "running"
STATUS_FINISHED = "finished"

# Types starting with this prefix belong to the SDK.
RESERVED_PREFIX = "stream."
END_TYPE = "stream.end"
GAP_TYPE = "stream.gap"
ERROR_TYPE = "stream.error"

# The one IDMP pair every final entry shares, so a stream holds at most one
# final entry even when two writers, or a writer and a reader, end it at once.
_END_PRODUCER = "end"
_END_ID = "final"

# No braces, so no value can break out of the hash tag.  No ":" in an ID, so
# "<scope>:<id>" splits in exactly one way and two pairs never share a key.
_SCOPE_RE = re.compile(r"[A-Za-z0-9_.:-]{1,200}")
_ID_RE = re.compile(r"[A-Za-z0-9_-]{1,64}")
_CURSOR_RE = re.compile(r"\d+-\d+")
_MAX_PART = 2**64 - 1
MAX_TYPE_LENGTH = 128

Cursor: TypeAlias = tuple[int, int]
_START: Cursor = (0, 0)


# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------


def _text(value: Any) -> str | None:
    """A reply value as text, whatever ``decode_responses`` is set to."""
    if value is None:
        return None
    if isinstance(value, bytes):
        return value.decode()
    return str(value)


def parse_cursor(value: str) -> Cursor:
    """Parse an entry ID into a pair of integers.

    Cursors compare as numbers: as strings ``"999-0"`` would sort after
    ``"1000-0"``.  The special IDs ``$``, ``+`` and ``-`` and a bare ``<ms>``
    are refused, as is a part above 2**64 - 1.

    Raises:
        StreamInvalidCursor: If *value* is not a valid entry ID.
    """
    if not _CURSOR_RE.fullmatch(value):
        raise StreamInvalidCursor(f"invalid cursor {value!r}")
    ms_text, seq_text = value.split("-")
    ms, seq = int(ms_text), int(seq_text)
    if ms > _MAX_PART or seq > _MAX_PART:
        raise StreamInvalidCursor(f"cursor out of range: {value!r}")
    return ms, seq


def _format(cursor: Cursor) -> str:
    return f"{cursor[0]}-{cursor[1]}"


def check_name(scope: str, stream_id: str | None = None) -> None:
    """Reject a scope or stream ID that could break a key's hash tag.

    Raises:
        ValueError: If *scope* or *stream_id* has characters outside the
            allowed set, or is too long.
    """
    if not isinstance(scope, str) or not _SCOPE_RE.fullmatch(scope):
        raise ValueError(f"invalid scope {scope!r}: use 1-200 of A-Z a-z 0-9 _ . : -")
    if stream_id is not None and (
        not isinstance(stream_id, str) or not _ID_RE.fullmatch(stream_id)
    ):
        raise ValueError(
            f"invalid stream ID {stream_id!r}: use 1-64 of A-Z a-z 0-9 _ -"
        )


def check_event_type(event_type: str) -> None:
    """Reject a type that could break an SSE frame, or that the SDK reserves.

    The type goes into the SSE ``event:`` line as it is, so a line break would
    end the frame and could start a forged one with its own ``id:``.

    Raises:
        ValueError: If the type is empty, longer than 128 characters, contains
            CR, LF or NUL, or starts with ``stream.``.
    """
    if (
        not isinstance(event_type, str)
        or not event_type
        or len(event_type) > MAX_TYPE_LENGTH
        or any(c in event_type for c in "\r\n\0")
    ):
        raise ValueError(f"invalid event type {event_type!r}")
    if event_type.startswith(RESERVED_PREFIX):
        raise ValueError(
            f"invalid event type {event_type!r}: types starting with "
            f"{RESERVED_PREFIX!r} are reserved"
        )


def _seconds(value: int | float | timedelta) -> int:
    if isinstance(value, timedelta):
        return max(1, int(value.total_seconds()))
    return max(1, int(value))


@dataclass(frozen=True)
class _Keys:
    stream: str
    meta: str
    alive: str
    active: str


def _active_key(scope: str) -> str:
    return f"{get_settings().prefix}:stream-active:{scope}"


def _keys(scope: str, stream_id: str) -> _Keys:
    prefix = get_settings().prefix
    tag = f"{{{scope}:{stream_id}}}"
    return _Keys(
        stream=f"{prefix}:stream:{tag}",
        meta=f"{prefix}:stream-meta:{tag}",
        alive=f"{prefix}:stream-alive:{tag}",
        active=_active_key(scope),
    )


def _xread_entries(reply: Any) -> list[tuple[Any, dict[Any, Any]]]:
    """Normalize an ``XREAD`` reply for one stream, RESP2 or RESP3."""
    if not reply:
        return []
    if isinstance(reply, dict):  # RESP3: {name: [entries]}
        streams = list(reply.values())
        if not streams:
            return []
        first = streams[0]
        if first and isinstance(first[0], list):
            return list(first[0])
        return list(first)
    return list(reply[0][1])  # RESP2: [[name, entries]]


# ---------------------------------------------------------------------------
# Support detection
# ---------------------------------------------------------------------------


@dataclass
class _StreamCapabilities:
    """Process-lifetime cache of server support, shared on the pool state.

    ``None`` means "not yet known" and leaves detection to a later attempt
    rather than caching a guess, as the rate limiter and the session store do.
    """

    supported: bool | None = None


async def probe_streams_support(client: AsyncClient) -> bool | None:
    """Ask the server whether it has ``XCFGSET`` and ``DELEX``.

    ``XCFGSET`` arrived in Redis 8.6 together with ``XADD ... IDMP``, and
    ``DELEX`` (with ``SET ... IFEQ``) in 8.4, so both present means the server
    has the commands this feature uses.  Like
    :func:`~redis_fastapi.ratelimit_backend.probe_increx_support` this asks
    ``COMMAND INFO`` rather than sending the commands and reading an error: on
    a cluster, redis-py rejects an unknown command client-side.

    The probe cannot tell Redis 8.6.0 or 8.6.1, which have the commands but
    carry IDMP defects, from 8.6.2.  The feature requires 8.6.2.

    Returns:
        ``True`` when both commands exist, ``False`` when either is missing,
        and ``None`` when the question could not be answered.
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
        # redis-py parses the COMMAND reply eagerly and indexes into a nil
        # entry for a command the server does not know: "unsupported".
        return False
    except STORE_ERRORS:
        logger.debug("Stream capability probe failed; will retry", exc_info=True)
        return None
    if isinstance(reply, dict):
        entries = list(reply.values())
    elif isinstance(reply, (list, tuple)):
        entries = list(reply)
    else:
        return False
    return len(entries) == 2 and all(entries)


# ---------------------------------------------------------------------------
# Data
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class StreamEvent:
    """One event read from a stream.

    Attributes:
        id: The Redis entry ID, which is the cursor.  ``None`` for the
            reader's own events (``stream.gap``, ``stream.error``).
        type: The event type the writer chose, or a ``stream.`` type.
        data: The decoded payload.
        final: ``True`` on the stream's final entry.
    """

    id: str | None
    type: str
    data: Any
    final: bool = False
    # The payload as stored, so the SSE response sends it without re-encoding.
    _raw: str | None = field(default=None, repr=False, compare=False)


@dataclass(frozen=True)
class AppendResult:
    """What :meth:`StreamWriter.append` returns.

    Attributes:
        id: The entry ID Redis assigned.
        cancelled: Whether a stop was requested with
            :meth:`StreamBackend.cancel`.  A producer started by
            :meth:`StreamBackend.spawn` is cancelled for you.
    """

    id: str
    cancelled: bool


# ---------------------------------------------------------------------------
# Writer
# ---------------------------------------------------------------------------


class StreamWriter:
    """Appends to one stream.  Made by :meth:`StreamBackend.open`.

    Attributes:
        stream_id: The stream's ID within its scope.
        scope: The scope the stream belongs to.
        created: ``True`` when :meth:`~StreamBackend.open` created the run,
            ``False`` when it found the same run already started - a retried
            start request.  Start a producer only when this is ``True``.
    """

    def __init__(
        self,
        backend: StreamBackend,
        *,
        scope: str,
        stream_id: str,
        writer_id: str,
        exclusive: bool,
        created: bool,
        seq: int = 0,
        started: bool = False,
    ) -> None:
        self._backend = backend
        self.scope = scope
        self.stream_id = stream_id
        self.created = created
        self._keys = _keys(scope, stream_id)
        self._writer_id = writer_id
        self._exclusive = exclusive
        self._seq = seq
        self._started = started
        self._final_id: str | None = None
        self._cancelled = False
        self._fenced = False
        # A handle for a run that already exists (a retried start) writes
        # nothing: the run's own producer does.
        self._read_only = False
        self._task: asyncio.Task[None] | None = None

    @property
    def cancelled(self) -> bool:
        """Whether a stop was requested, as last seen by an append or heartbeat."""
        return self._cancelled

    @property
    def finished(self) -> bool:
        """Whether this writer has written the final entry."""
        return self._final_id is not None

    async def append(self, type: str, data: Any) -> AppendResult:
        """Append one event and return its entry ID.

        One transaction writes the entry, refreshes the stream's TTL and the
        producer heartbeat, and reads the cancel flag.  A timeout or a dropped
        connection is retried with the same idempotency key, so a retried
        append never stores the event twice.

        Raises:
            ValueError: If *type* is invalid or reserved, or the encoded event
                is larger than ``stream_max_event_bytes``.
            StreamFinished: If the final entry was already written.
            StreamExpired: If the stream expired, for example after a pause
                longer than its TTL.
            StreamFenced: If :meth:`StreamBackend.attach` handed the stream to
                another writer.
            StreamStoreError: If Redis cannot be reached.
        """
        check_event_type(type)
        self._check_writable()
        if self._final_id is not None:
            raise StreamFinished(f"stream {self.stream_id} already ended")
        payload = self._backend._encode(data)
        with stream_span("redis_fastapi.streams.append"), timed_stream("append"):
            entry_id, cancelled = await self._write(
                type, payload, self._seq + 1, final=False
            )
        record_stream_operation(operation="append", result="ok")
        if cancelled:
            self._cancelled = True
            task = self._task
            if task is not None:
                is_current = asyncio.current_task() is task
                if is_current:
                    task.cancel()
                    await asyncio.sleep(0)  # raises CancelledError here
        return AppendResult(id=entry_id, cancelled=cancelled)

    async def finish(self, type: str, data: Any = None) -> str:
        """Write the final event and end the stream.

        The final event is an ordinary event of your chosen type, marked as
        the end.  Readers stop after it, and a reconnect after it gets 204.
        Every final entry shares one idempotency key, so a stream has at most
        one: if another writer ended it first, this returns that entry's ID.

        Returns:
            The final entry's ID.
        """
        check_event_type(type)
        return await self._end(type, data)

    async def suspend(self, ttl: int | timedelta) -> None:
        """Keep the stream, its heartbeat and its active slot for *ttl*.

        For a run that waits on a slow tool, or that another process will
        continue with :meth:`StreamBackend.attach`.
        """
        seconds = _seconds(ttl)
        window = get_settings().stream_expiry_window
        client = self._backend._client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.expire(self._keys.stream, seconds)
            pipe.expire(self._keys.meta, seconds + window)
            pipe.set(self._keys.alive, 1, ex=seconds)
            await pipe.execute()
            if self._exclusive:
                await self._backend._refresh_slot(
                    self._keys.active, self.stream_id, seconds
                )
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not suspend the stream") from exc

    def keepalive(self) -> AbstractAsyncContextManager[None]:
        """Refresh the producer heartbeat while a block runs.

        Every ``stream_heartbeat_seconds`` it refreshes the heartbeat key and
        the active slot, and reads the cancel flag.  It covers long tool calls
        during which no append happens.  :meth:`StreamBackend.spawn` runs it
        for you.
        """
        return self._keepalive()

    @asynccontextmanager
    async def _keepalive(self) -> AsyncIterator[None]:
        beat = asyncio.create_task(self._beat())
        try:
            yield
        finally:
            beat.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await beat

    async def _beat(self) -> None:
        settings = get_settings()
        while True:
            await asyncio.sleep(settings.stream_heartbeat_seconds)
            client = self._backend._client()
            try:
                pipe = client.pipeline(transaction=True)
                pipe.set(
                    self._keys.alive,
                    1,
                    px=int(settings.stream_alive_ttl_seconds * 1000),
                )
                pipe.hmget(self._keys.meta, [M_CANCEL, M_WRITER])
                replies = await pipe.execute()
                if self._exclusive:
                    await self._backend._refresh_slot(
                        self._keys.active,
                        self.stream_id,
                        settings.stream_ttl_active,
                    )
            except STORE_ERRORS:
                logger.debug("Stream heartbeat failed; will retry", exc_info=True)
                continue
            cancel, writer = (_text(v) for v in replies[-1])
            if writer is not None and writer != self._writer_id:
                logger.info("Stream %s was handed to another writer", self.stream_id)
                self._fenced = True
                if self._task is not None:
                    self._task.cancel()
                return
            if cancel == "1":
                self._cancelled = True
                if self._task is not None:
                    self._task.cancel()
                return

    def _check_writable(self) -> None:
        if self._read_only or self._fenced:
            raise StreamFenced(
                f"this writer does not own stream {self.stream_id}; "
                "its own producer writes to it"
            )

    async def _end(self, type: str, data: Any) -> str:
        self._check_writable()
        if self._final_id is not None:
            return self._final_id
        payload = self._backend._encode(data)
        with stream_span("redis_fastapi.streams.finish"), timed_stream("finish"):
            entry_id, _ = await self._write(type, payload, self._seq + 1, final=True)
            self._final_id = entry_id
            await self._backend._after_final(
                self._keys, self.stream_id, entry_id, release=self._exclusive
            )
        record_stream_operation(operation="finish", result="ok")
        return entry_id

    async def _write(
        self, type: str, payload: str, seq: int, *, final: bool
    ) -> tuple[str, bool]:
        settings = get_settings()
        keys = self._keys
        ttl = settings.stream_ttl_done if final else settings.stream_ttl_active
        args: list[Any] = ["XADD", keys.stream]
        if self._started:
            args.append("NOMKSTREAM")
        if final:
            args += ["IDMP", _END_PRODUCER, _END_ID]
        else:
            args += ["IDMP", self._writer_id, str(seq)]
        args += ["MAXLEN", "~", str(settings.stream_maxlen), "*"]
        args += [F_TYPE, type, F_DATA, payload, F_SEQ, str(seq)]
        if final:
            args += [F_FINAL, "1"]

        replies = await self._backend._transaction(
            lambda pipe: _build_write(pipe, args, keys, ttl, final, self._started)
        )
        entry_id = _text(replies[0])
        if entry_id is None:
            record_stream_operation(operation="append", result="expired")
            raise StreamExpired(f"stream {self.stream_id} has expired")
        cancel, writer = (_text(v) for v in replies[-1])
        self._seq = seq
        self._started = True
        if writer is None:
            raise StreamExpired(f"stream {self.stream_id} has expired")
        if writer != self._writer_id and not final:
            self._fenced = True
            record_stream_operation(operation="append", result="fenced")
            raise StreamFenced(f"stream {self.stream_id} was handed to another writer")
        return entry_id, cancel == "1"


def _build_write(
    pipe: Any, args: list[Any], keys: _Keys, ttl: int, final: bool, started: bool
) -> None:
    settings = get_settings()
    window = settings.stream_expiry_window
    pipe.execute_command(*args)
    if not started:
        pipe.hset(keys.meta, M_STARTED, "1")
    if final:
        pipe.hset(keys.meta, M_STATUS, STATUS_FINISHED)
    pipe.expire(keys.stream, ttl)
    pipe.expire(keys.meta, ttl + window)
    if final:
        pipe.delete(keys.alive)
    else:
        pipe.set(keys.alive, 1, px=int(settings.stream_alive_ttl_seconds * 1000))
    pipe.hmget(keys.meta, [M_CANCEL, M_WRITER])


# ---------------------------------------------------------------------------
# Reader plan
# ---------------------------------------------------------------------------


@dataclass
class _ReadPlan:
    """What the reader learned before the first byte."""

    keys: _Keys
    scope: str
    stream_id: str
    cursor: Cursor
    last_seq: int | None
    first: list[tuple[Any, dict[Any, Any]]] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Backend
# ---------------------------------------------------------------------------


class StreamBackend:
    """Resumable streams backed by Redis.

    There is one instance per process.  ``FastAPIRedis(app).streams()``
    creates it, and :data:`~redis_fastapi.deps.StreamBackendDep` returns it.
    It holds state for the whole process: the tasks started by :meth:`spawn`,
    the live reader count and the server-support result.

    Args:
        client: A callable returning the async Redis client, so the backend
            follows the application's pool.
        coder: Serializer for event payloads.  Defaults to :class:`JsonCoder`.
        capabilities: Shared server-support cache.
    """

    def __init__(
        self,
        client: Callable[[], AsyncClient],
        *,
        coder: type[Coder] | None = None,
        capabilities: _StreamCapabilities | None = None,
    ) -> None:
        self._client = client
        self._coder: type[Coder] = coder or JsonCoder
        self._caps = capabilities if capabilities is not None else _StreamCapabilities()
        self._tasks: set[asyncio.Task[None]] = set()
        self._spawned: dict[tuple[str, str], asyncio.Task[None]] = {}
        self._readers = 0
        self._closing = False

    # -- support -------------------------------------------------------------

    @property
    def supported(self) -> bool | None:
        """Whether the server supports the feature; ``None`` if not yet known."""
        return self._caps.supported

    async def detect(self) -> bool | None:
        """Detect server support once, and log one warning when it is missing."""
        if self._caps.supported is not None:
            return self._caps.supported
        try:
            result = await probe_streams_support(self._client())
        except RuntimeError:
            return None  # no pool yet
        if result is False:
            logger.warning(
                "Resumable streams are off: this Redis has no XCFGSET / DELEX.  "
                "They need Redis 8.6.2 or later; the rest of the application "
                "is unaffected."
            )
        elif result is True:
            logger.info("Resumable streams: server support detected")
        self._caps.supported = result
        return result

    async def _require_support(self) -> None:
        if self._caps.supported is None:
            await self.detect()
        if self._caps.supported is False:
            raise StreamUnsupported("resumable streams need Redis 8.6.2 or later")

    # -- plumbing ------------------------------------------------------------

    def _encode(self, data: Any) -> str:
        payload = self._coder.encode(data)
        limit = get_settings().stream_max_event_bytes
        if len(payload.encode()) > limit:
            raise ValueError(
                f"event of {len(payload.encode())} bytes exceeds "
                f"stream_max_event_bytes ({limit})"
            )
        return payload

    def _decode(self, raw: Any) -> Any:
        text = _text(raw)
        if text is None:
            return None
        try:
            return self._coder.decode(text)
        except Exception:
            logger.warning("Undecodable stream payload; passing it as text")
            return text

    async def _transaction(self, build: Callable[[Any], None]) -> list[Any]:
        """Run a transaction, retrying the ambiguous failures.

        Every command the feature sends in a transaction is safe to repeat, so
        a timeout or a dropped connection, after which Redis may or may not
        have applied the writes, is retried with backoff.
        """
        for attempt in range(_WRITE_ATTEMPTS):
            pipe = self._client().pipeline(transaction=True)
            build(pipe)
            try:
                return list(await pipe.execute())
            except _AMBIGUOUS_ERRORS as exc:
                if attempt == _WRITE_ATTEMPTS - 1:
                    raise StreamStoreError("Redis did not answer") from exc
                await asyncio.sleep(0.05 * 2**attempt)
            except STORE_ERRORS as exc:
                raise StreamStoreError(str(exc) or "Redis error") from exc
        raise StreamStoreError("Redis did not answer")  # pragma: no cover

    async def _set_meta(self, keys: _Keys, mapping: dict[Any, Any]) -> None:
        """Write meta fields without ever leaving meta without a TTL.

        If meta expired a moment ago, ``HSET`` would create it again with no
        expiry.  ``EXPIRE ... NX`` sets a TTL only on a key that has none, so
        it repairs exactly that case and leaves a live meta's TTL alone.
        """
        settings = get_settings()
        pipe = self._client().pipeline(transaction=True)
        pipe.hset(keys.meta, mapping=mapping)
        pipe.expire(
            keys.meta,
            settings.stream_ttl_done + settings.stream_expiry_window,
            nx=True,
        )
        await pipe.execute()

    async def _refresh_slot(self, active: str, stream_id: str, ttl: int) -> None:
        """Extend the active slot only while it still names *stream_id*."""
        await self._client().execute_command(
            "SET", active, stream_id, "IFEQ", stream_id, "EX", ttl
        )

    async def _release_slot(self, active: str, stream_id: str) -> None:
        """Free the active slot only if it still names *stream_id*.

        A stale writer must not free the slot of a newer run.
        """
        try:
            await self._client().execute_command("DELEX", active, "IFEQ", stream_id)
        except STORE_ERRORS:
            logger.warning("Could not release the active slot; it expires by itself")

    async def _after_final(
        self, keys: _Keys, stream_id: str, final_id: str, *, release: bool
    ) -> None:
        """Record the final entry's ID in meta, and free the active slot."""
        try:
            await self._set_meta(keys, {M_FINAL: final_id})
        except STORE_ERRORS:
            logger.warning("Could not record the final entry; readers still stop at it")
        if release:
            await self._release_slot(keys.active, stream_id)

    # -- writer side ---------------------------------------------------------

    async def open(
        self,
        *,
        scope: str,
        stream_id: str | None = None,
        exclusive: bool = True,
    ) -> StreamWriter:
        """Start a run, or return the same run on a retried start request.

        With *exclusive* (the default) the run takes the scope's active slot,
        so a scope holds one active run at a time.  A client-supplied
        *stream_id* - for example an idempotency key the browser generated -
        makes a retried ``POST`` safe: the second call finds the same run and
        returns a writer whose ``created`` is ``False``.

        Raises:
            ValueError: If *scope* or *stream_id* is invalid.
            StreamBusy: If the scope's slot names another run.
            StreamUnsupported: If the server lacks support.
            StreamStoreError: If Redis cannot be reached.
        """
        await self._require_support()
        check_name(scope, stream_id)
        sid = stream_id or secrets.token_urlsafe(12)
        keys = _keys(scope, sid)
        settings = get_settings()
        client = self._client()
        with stream_span("redis_fastapi.streams.open"), timed_stream("open"):
            try:
                if exclusive:
                    took = await client.set(
                        keys.active, sid, nx=True, ex=settings.stream_ttl_active
                    )
                    if not took:
                        holder = _text(await client.get(keys.active))
                        if holder != sid:
                            record_stream_operation(operation="open", result="busy")
                            raise StreamBusy(holder or "unknown")
                        if await client.exists(keys.meta):
                            record_stream_operation(operation="open", result="retry")
                            return self._handle(scope, sid, exclusive)
                elif await client.exists(keys.meta):
                    record_stream_operation(operation="open", result="retry")
                    return self._handle(scope, sid, exclusive)
                writer_id = _new_writer_id()
                pipe = client.pipeline(transaction=True)
                pipe.hset(
                    keys.meta,
                    mapping={M_STATUS: STATUS_RUNNING, M_WRITER: writer_id},
                )
                pipe.expire(
                    keys.meta,
                    settings.stream_ttl_active + settings.stream_expiry_window,
                )
                pipe.set(
                    keys.alive, 1, px=int(settings.stream_alive_ttl_seconds * 1000)
                )
                await pipe.execute()
            except STORE_ERRORS as exc:
                record_stream_operation(operation="open", result="error")
                raise StreamStoreError("could not open the stream") from exc
        record_stream_operation(operation="open", result="created")
        return StreamWriter(
            self,
            scope=scope,
            stream_id=sid,
            writer_id=writer_id,
            exclusive=exclusive,
            created=True,
        )

    def _handle(self, scope: str, stream_id: str, exclusive: bool) -> StreamWriter:
        """A writer for a run that already exists, returned on a retried start.

        It holds no writer ID of its own, so an append through it is fenced:
        the run's own producer keeps writing.
        """
        handle = StreamWriter(
            self,
            scope=scope,
            stream_id=stream_id,
            writer_id=_new_writer_id(),
            exclusive=exclusive,
            created=False,
            started=True,
        )
        handle._read_only = True
        return handle

    async def attach(self, stream_id: str, *, scope: str) -> StreamWriter:
        """Continue a run from another process of the same application.

        The new writer continues the sequence numbers and takes over: the old
        writer is fenced, and stops at its next append or heartbeat.

        Raises:
            StreamNotFound: If there is no such stream.
            StreamFinished: If the stream already has its final entry.
        """
        await self._require_support()
        check_name(scope, stream_id)
        keys = _keys(scope, stream_id)
        client = self._client()
        try:
            pipe = client.pipeline(transaction=True)
            pipe.hgetall(keys.meta)
            pipe.xrevrange(keys.stream, count=1)
            meta_raw, last = await pipe.execute()
            meta = {_text(k): _text(v) for k, v in (meta_raw or {}).items()}
            if not meta:
                raise StreamNotFound(f"no stream {stream_id} in this scope")
            if meta.get(M_STATUS) == STATUS_FINISHED:
                raise StreamFinished(f"stream {stream_id} already ended")
            seq = 0
            if last:
                fields = {_text(k): _text(v) for k, v in last[0][1].items()}
                seq = int(fields.get(F_SEQ) or 0)
            writer_id = _new_writer_id()
            await self._set_meta(keys, {M_WRITER: writer_id})
            holder = _text(await client.get(keys.active))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not attach to the stream") from exc
        record_stream_operation(operation="attach", result="ok")
        return StreamWriter(
            self,
            scope=scope,
            stream_id=stream_id,
            writer_id=writer_id,
            exclusive=holder == stream_id,
            created=False,
            seq=seq,
            started=bool(last) or meta.get(M_STARTED) == "1",
        )

    def spawn(
        self,
        writer: StreamWriter,
        fn: Callable[[StreamWriter], Awaitable[None]],
    ) -> asyncio.Task[None]:
        """Run *fn* as a task the backend owns, outside the request.

        A disconnect never stops it.  When *fn* returns, raises or is
        cancelled without having called :meth:`StreamWriter.finish`, the task
        writes a final ``stream.end`` event whose data gives the reason:
        ``{"reason": "done"}``, ``{"reason": "error", "code": ...}`` or
        ``{"reason": "cancelled"}``.  The heartbeat runs around *fn*.  On
        application shutdown the remaining tasks are cancelled.
        """

        async def end(data: dict[str, Any]) -> None:
            if writer.finished:
                return
            try:
                await writer._end(END_TYPE, data)
            except _END_ERRORS:
                logger.warning(
                    "Could not end stream %s; readers will see it as lost",
                    writer.stream_id,
                    exc_info=True,
                )

        async def run() -> None:
            try:
                async with writer.keepalive():
                    await fn(writer)
            except asyncio.CancelledError:
                if not writer._fenced:
                    await end({"reason": "cancelled"})
                return
            except StreamFenced:
                return  # another writer owns the stream now
            except Exception as exc:
                logger.exception("Stream producer for %s failed", writer.stream_id)
                code = getattr(exc, "code", None)
                await end(
                    {
                        "reason": "error",
                        "code": code if isinstance(code, str) else "producer_error",
                    }
                )
                return
            await end({"reason": "done"})

        task = asyncio.create_task(run())
        writer._task = task
        key = (writer.scope, writer.stream_id)
        self._tasks.add(task)
        self._spawned[key] = task

        def forget(done: asyncio.Task[None]) -> None:
            self._tasks.discard(done)
            if self._spawned.get(key) is done:
                del self._spawned[key]

        task.add_done_callback(forget)
        return task

    async def cancel(self, stream_id: str, *, scope: str) -> bool:
        """Ask the run to stop.

        Sets the cancel flag.  The producer sees it on its next append or
        heartbeat; a producer started by :meth:`spawn` in this process is
        cancelled at once.

        Returns:
            ``True`` if the run was still going, ``False`` if it had ended.

        Raises:
            StreamNotFound: If there is no such stream in this scope.
        """
        await self._require_support()
        check_name(scope)
        if not _ID_RE.fullmatch(stream_id):
            raise StreamNotFound(f"no stream {stream_id!r} in this scope")
        keys = _keys(scope, stream_id)
        try:
            status = _text(await self._client().hget(keys.meta, M_STATUS))
            if status is None:
                raise StreamNotFound(f"no stream {stream_id} in this scope")
            if status == STATUS_FINISHED:
                return False
            await self._set_meta(keys, {M_CANCEL: "1"})
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not cancel the stream") from exc
        task = self._spawned.get((scope, stream_id))
        if task is not None and not task.done():
            task.cancel()
        record_stream_operation(operation="cancel", result="ok")
        return True

    async def active(self, scope: str) -> str | None:
        """Return the ID of the scope's active run, or ``None``."""
        await self._require_support()
        check_name(scope)
        try:
            return _text(await self._client().get(_active_key(scope)))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not read the active run") from exc

    # -- reader side ---------------------------------------------------------

    def follow(
        self,
        stream_id: str,
        *,
        scope: str,
        after: str | None = None,
    ) -> AsyncIterator[StreamEvent]:
        """Replay the events after *after*, then follow the live stream.

        The transport-neutral core: :meth:`sse` wraps it for HTTP.  With no
        *after* it reads the whole stream, and fails if anything was trimmed.
        It stops after the final event.  Wrap it in
        ``contextlib.aclosing()`` if you may leave the loop early.

        Raises:
            StreamNotFound: Unknown stream, or one from another scope.
            StreamExpired: The stream expired, or entries after *after* were
                trimmed.
            StreamCursorAhead: *after* is past the stream's last entry.
            StreamInvalidCursor: *after* is not a valid entry ID.
        """
        return self._follow(stream_id, scope=scope, after=after)

    async def _follow(
        self, stream_id: str, *, scope: str, after: str | None
    ) -> AsyncGenerator[StreamEvent, None]:
        await self._require_support()
        plan = await self._prepare(stream_id, scope=scope, after=after)
        if plan is None:
            return
        async for event in self._events(plan):
            if event is not None:
                yield event

    async def sse(
        self,
        stream_id: str,
        *,
        request: Request,
        scope: str,
        after: str | None = None,
        expires_at: datetime | None = None,
    ) -> Response:
        """An SSE response for the stream.  See :func:`redis_fastapi.streams.sse`."""
        from redis_fastapi.streams import sse

        return await sse(
            self,
            stream_id,
            request=request,
            scope=scope,
            after=after,
            expires_at=expires_at,
        )

    async def _prepare(
        self, stream_id: str, *, scope: str, after: str | None
    ) -> _ReadPlan | None:
        """Check the stream and the cursor, and read the first page.

        Returns ``None`` when the cursor is at or past the final entry.  Runs
        as one transaction, so the checks and the first page are one snapshot
        and no trim can fall between them.
        """
        check_name(scope)
        if not isinstance(stream_id, str) or not _ID_RE.fullmatch(stream_id):
            record_stream_operation(operation="read", result="not_found")
            raise StreamNotFound(f"no stream {stream_id!r} in this scope")
        cursor = parse_cursor(after) if after is not None else _START
        keys = _keys(scope, stream_id)
        page = get_settings().stream_replay_page

        def build(pipe: Any) -> None:
            pipe.hgetall(keys.meta)
            pipe.exists(keys.stream)
            pipe.xinfo_stream(keys.stream)
            if cursor != _START:
                pipe.xrange(keys.stream, _format(cursor), _format(cursor))
            pipe.xread({keys.stream: _format(cursor)}, count=page)

        try:
            pipe = self._client().pipeline(transaction=True)
            build(pipe)
            replies = list(await pipe.execute(raise_on_error=False))
        except STORE_ERRORS as exc:
            raise StreamStoreError("could not read the stream") from exc
        for reply in (replies[0], replies[1]):
            if isinstance(reply, Exception):
                raise StreamStoreError("could not read the stream") from reply

        meta = {_text(k): _text(v) for k, v in (replies[0] or {}).items()}
        if not meta:
            record_stream_operation(operation="read", result="not_found")
            raise StreamNotFound(f"no stream {stream_id} in this scope")
        exists = bool(replies[1])
        if meta.get(M_STARTED) == "1" and not exists:
            record_stream_operation(operation="read", result="expired")
            raise StreamExpired(f"stream {stream_id} has expired")

        info = replies[2] if not isinstance(replies[2], Exception) else None
        last_generated = _info_id(info, "last-generated-id")
        if cursor != _START and (last_generated is None or cursor > last_generated):
            record_stream_operation(operation="read", result="ahead")
            raise StreamCursorAhead(f"cursor {_format(cursor)} is ahead of the stream")
        final = meta.get(M_FINAL)
        if final is not None and cursor >= parse_cursor(final):
            record_stream_operation(operation="read", result="finished")
            return None
        own = replies[3] if cursor != _START else None
        own_present = bool(own) and not isinstance(own, Exception)
        if _history_lost(info, cursor, own_present=own_present):
            record_stream_operation(operation="read", result="expired")
            raise StreamExpired(
                "entries after the cursor were removed; reload from the source"
            )

        last_seq: int | None = 0 if cursor == _START else None
        if own_present and own is not None:
            fields = {_text(k): _text(v) for k, v in own[0][1].items()}
            seq_text = fields.get(F_SEQ)
            last_seq = int(seq_text) if seq_text else None
        first_reply = replies[-1]
        first = (
            [] if isinstance(first_reply, Exception) else _xread_entries(first_reply)
        )
        record_stream_operation(
            operation="read", result="started" if cursor == _START else "resumed"
        )
        return _ReadPlan(
            keys=keys,
            scope=scope,
            stream_id=stream_id,
            cursor=cursor,
            last_seq=last_seq,
            first=first,
        )

    async def _events(
        self, plan: _ReadPlan
    ) -> AsyncGenerator[StreamEvent | None, None]:
        """The read loop.  Yields ``None`` as a keepalive on a quiet heartbeat."""
        settings = get_settings()
        cursor = plan.cursor
        last_seq = plan.last_seq
        pending = plan.first
        failing_since: float | None = None
        while True:
            for raw_id, raw_fields in pending:
                event, seq = self._to_event(raw_id, raw_fields)
                if seq is not None:
                    if last_seq is not None and seq > last_seq + 1:
                        missing = seq - last_seq - 1
                        record_stream_operation(operation="gap", result="ok")
                        yield StreamEvent(None, GAP_TYPE, {"missing": missing})
                    last_seq = seq
                cursor = parse_cursor(event.id or "0-0")
                yield event
                if event.final:
                    return
            pending = []
            if self._closing:
                return  # shutting down: close, and the client reconnects
            try:
                reply = await self._client().xread(
                    {plan.keys.stream: _format(cursor)},
                    count=settings.stream_replay_page,
                    block=int(settings.stream_heartbeat_seconds * 1000),
                )
                failing_since = None
            except STORE_ERRORS:
                now = time.monotonic()
                failing_since = failing_since or now
                if now - failing_since > settings.stream_read_retry_seconds:
                    raise StreamStoreError("Redis stayed unreachable") from None
                logger.debug("Stream read failed; retrying", exc_info=True)
                await asyncio.sleep(min(1.0, settings.stream_heartbeat_seconds))
                yield None
                continue
            pending = _xread_entries(reply)
            if pending:
                continue
            state = await self._check_producer(plan)
            if state == "gone":
                return
            if state == "lost_read_only":
                yield StreamEvent(None, ERROR_TYPE, {"code": "producer_lost"})
                return
            if state == "ended":
                continue  # the next read returns the final entry
            yield None

    def _to_event(
        self, raw_id: Any, raw_fields: dict[Any, Any]
    ) -> tuple[StreamEvent, int | None]:
        fields = {_text(k): v for k, v in raw_fields.items()}
        seq_text = _text(fields.get(F_SEQ))
        raw = _text(fields.get(F_DATA))
        event = StreamEvent(
            id=_text(raw_id),
            type=_text(fields.get(F_TYPE)) or "",
            data=self._decode(raw),
            final=_text(fields.get(F_FINAL)) == "1",
            _raw=raw,
        )
        return event, int(seq_text) if seq_text else None

    async def _check_producer(self, plan: _ReadPlan) -> str:
        """On a quiet heartbeat: is the producer alive, and does the stream exist?

        A producer whose heartbeat lapsed with no final entry will never end
        the stream, so the reader writes the final entry it never wrote.
        """
        keys = plan.keys
        try:
            pipe = self._client().pipeline(transaction=True)
            pipe.exists(keys.alive)
            pipe.hget(keys.meta, M_STATUS)
            alive, status = await pipe.execute()
        except STORE_ERRORS:
            return "quiet"
        status = _text(status)
        if status is None:
            return "gone"  # meta expired or evicted mid-read
        if status == STATUS_FINISHED or alive:
            return "quiet"
        try:
            await self._end_lost(plan)
        except NoPermissionError:
            return "lost_read_only"
        except STORE_ERRORS:
            logger.debug("Could not end a lost producer's stream", exc_info=True)
            return "quiet"
        return "ended"

    async def _end_lost(self, plan: _ReadPlan) -> None:
        settings = get_settings()
        keys = plan.keys
        payload = self._encode({"reason": "producer_lost"})
        client = self._client()
        exists = await client.exists(keys.stream)
        args: list[Any] = ["XADD", keys.stream]
        if exists:
            args.append("NOMKSTREAM")
        args += ["IDMP", _END_PRODUCER, _END_ID, "MAXLEN", "~"]
        args += [str(settings.stream_maxlen), "*"]
        args += [F_TYPE, END_TYPE, F_DATA, payload, F_FINAL, "1"]
        pipe = client.pipeline(transaction=True)
        pipe.execute_command(*args)
        pipe.hset(keys.meta, mapping={M_STATUS: STATUS_FINISHED, M_STARTED: "1"})
        pipe.expire(keys.stream, settings.stream_ttl_done)
        pipe.expire(keys.meta, settings.stream_ttl_done + settings.stream_expiry_window)
        replies = await pipe.execute()
        final_id = _text(replies[0])
        if final_id is None:
            return
        record_stream_operation(operation="producer_lost", result="ok")
        holder = _text(await client.get(keys.active))
        await self._after_final(
            keys, plan.stream_id, final_id, release=holder == plan.stream_id
        )

    # -- readers and lifecycle -----------------------------------------------

    def _start(self, capabilities: _StreamCapabilities) -> None:
        """Bind to a freshly built pool.  Called by the lifespan at startup."""
        self._caps = capabilities
        self._closing = False

    def _reserve_reader(self) -> bool:
        if self._closing or self._readers >= get_settings().stream_max_readers:
            return False
        self._readers += 1
        add_stream_readers(1)
        return True

    def _release_reader(self) -> None:
        self._readers -= 1
        add_stream_readers(-1)

    async def aclose(self) -> None:
        """Stop the feature: close live readers and cancel spawned producers.

        Each cancelled producer writes a ``stream.end`` with reason
        ``cancelled``, so its readers do not hang.  Called by the lifespan
        before the connection pool closes.
        """
        self._closing = True
        tasks = list(self._tasks)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


def _new_writer_id() -> str:
    return secrets.token_urlsafe(9)


def _history_lost(info: Any, cursor: Cursor, *, own_present: bool) -> bool:
    """Whether an entry after *cursor* was removed, from ``XINFO STREAM``.

    ``XDEL`` raises ``max-deleted-entry-id``, so a deleted entry newer than
    the cursor is exact to detect.  Trimming does not raise that field: it
    shows only as ``entries-added`` exceeding ``length``, with the oldest
    entries gone.  So once anything was removed, a cursor whose own entry is
    gone and that is older than the first retained entry counts as a gap.
    That is conservative: a client whose last entry was exactly the newest
    one trimmed also gets 410, although it missed nothing.  And with no
    cursor (``0-0``, the whole stream) any removal is a gap.
    """
    if not isinstance(info, dict):
        return False  # no stream yet, so nothing was removed
    max_deleted = _info_id(info, "max-deleted-entry-id")
    if max_deleted is not None and cursor < max_deleted:
        return True
    try:
        removed = int(_text(info.get("entries-added")) or 0) - int(
            _text(info.get("length")) or 0
        )
    except ValueError:
        return False
    if removed <= 0:
        return False
    if cursor == _START:
        return True
    if own_present:
        return False  # trimming removes the oldest entries, and this one stayed
    first = info.get("first-entry")
    if first:
        first_id = _text(first[0])
        return first_id is not None and cursor < parse_cursor(first_id)
    last = _info_id(info, "last-generated-id")
    return last is not None and cursor < last


def _info_id(info: Any, name: str) -> Cursor | None:
    if not isinstance(info, dict):
        return None
    value = _text(info.get(name))
    if value is None:
        return None
    try:
        return parse_cursor(value)
    except StreamInvalidCursor:
        return None
