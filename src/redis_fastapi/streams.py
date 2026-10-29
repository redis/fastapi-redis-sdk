"""Resumable streams over HTTP: the SSE response, cursor rules and handlers.

Setup::

    from redis_fastapi import FastAPIRedis

    app = FastAPI()
    FastAPIRedis(app).lifespan().streams()

Then inject :data:`~redis_fastapi.deps.StreamBackendDep`, start a run with
``open()`` and ``spawn()``, and return ``await streams.sse(...)`` from the
route a browser's ``EventSource`` connects to.  The storage side lives in
:mod:`redis_fastapi.stream_backend`.
"""

from __future__ import annotations

import asyncio
import json
import logging
import secrets
import time
from collections.abc import AsyncIterator
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from starlette.responses import JSONResponse, Response, StreamingResponse

from redis_fastapi.config import get_settings
from redis_fastapi.exceptions import (
    SessionConfigurationError,
    StreamConfigurationError,
    StreamCredentialExpired,
    StreamError,
    StreamInvalidCursor,
    StreamStoreError,
)
from redis_fastapi.stream_backend import (
    ERROR_TYPE,
    IDMP_WINDOW,
    StreamBackend,
    _Entry,
    parse_cursor,
)
from redis_fastapi.telemetry import record_stream_operation
from redis_fastapi.types import Coder

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.requests import Request
    from starlette.types import Message, Receive, Scope, Send

logger = logging.getLogger(__name__)

_RETRY_AFTER = "5"
_HEADERS = {"Cache-Control": "no-store", "X-Accel-Buffering": "no"}

# The SSE "retry:" hint, in milliseconds: how long EventSource waits before
# it reconnects after the server closes.  Short, because a planned close (the
# connection cap, a slow client, a deploy) happens while the run goes on.  A
# random value per connection spreads the reconnects of many clients that a
# deploy closed at once.
_RETRY_MIN_MS = 1000
_RETRY_SPREAD_MS = 2000


def _retry_ms() -> int:
    return _RETRY_MIN_MS + secrets.randbelow(_RETRY_SPREAD_MS + 1)


# ---------------------------------------------------------------------------
# Cursor and credential helpers
# ---------------------------------------------------------------------------


def select_cursor(request: Request, after: str | None = None) -> str | None:
    """The cursor for *request*: *after*, then ``Last-Event-ID``, then ``?after=``.

    A value the handler passes wins: the application uses it when it knows
    better, for example when it loaded state together with the matching
    cursor.  ``Last-Event-ID`` beats the query because ``EventSource`` keeps
    the URL it was built with but sends the newer header on every reconnect.

    Every cursor present is validated, even one that loses, and a repeated
    header or query parameter is refused rather than guessed between.

    Raises:
        StreamInvalidCursor: If a cursor is malformed or repeated.
    """
    headers = request.headers.getlist("last-event-id")
    queries = request.query_params.getlist("after")
    if len(headers) > 1 or len(queries) > 1:
        raise StreamInvalidCursor("more than one cursor in the request")
    for value in (after, *headers, *queries):
        if value is not None:
            parse_cursor(value)
    if after is not None:
        return after
    if headers:
        return headers[0]
    return queries[0] if queries else None


def session_expiry(request: Request) -> datetime | None:
    """The absolute deadline of the request's SDK session, if it has one.

    Pass it as ``expires_at`` to :meth:`StreamBackend.sse`, so an open stream
    ends when the session does.  Returns ``None`` when the application has no
    session middleware, or the session has no absolute deadline.
    """
    from redis_fastapi.sessions import session_state_of

    try:
        remaining = session_state_of(request).absolute_remaining
    except SessionConfigurationError:
        return None
    if remaining is None or remaining <= 0:
        return None
    return datetime.fromtimestamp(time.time() + remaining, tz=timezone.utc)


def encode_frame(event_type: str, data: str, event_id: str | None = None) -> str:
    """One SSE frame.  Data with line breaks becomes several ``data:`` lines.

    JSON never holds a raw line break, but a custom coder might; the browser
    joins the ``data:`` lines back with ``\\n``.
    """
    head = f"id: {event_id}\n" if event_id else ""
    lines = data.replace("\r\n", "\n").replace("\r", "\n").split("\n")
    body = "".join(f"data: {line}\n" for line in lines)
    return f"{head}event: {event_type}\n{body}\n"


def _frame(entry: _Entry) -> str:
    # The payload as stored: the SSE path never decodes it.
    return encode_frame(
        entry.type, entry.raw if entry.raw is not None else "null", entry.id
    )


# ---------------------------------------------------------------------------
# The SSE response
# ---------------------------------------------------------------------------


class _SlowConsumer(Exception):
    """A send to the client took longer than ``stream_send_timeout``."""


class _SSEResponse(StreamingResponse):
    """A streaming response that holds a reader slot and limits every send.

    The slot is taken when the response runs, before any header goes out, and
    released in a ``finally``.  A response that is built but never sent - an
    error in middleware, a client gone before the first byte - so never takes
    a slot, and one that runs releases it on every exit path.  Over
    ``stream_max_readers`` it answers 503 instead, still before the first byte.

    Every chunk is sent under a time limit.  A client that stops reading fills
    the socket buffer and ``send()`` waits; past the limit the response closes
    with no event, which no blocked socket could receive anyway, and a live
    client reconnects with ``Last-Event-ID``.
    """

    def __init__(
        self,
        content: AsyncIterator[str],
        *,
        backend: StreamBackend,
        send_timeout: int,
        **kwargs: Any,
    ) -> None:
        super().__init__(content, **kwargs)
        self._backend = backend
        self._send_timeout = send_timeout

    async def stream_response(self, send: Send) -> None:
        async def timed(message: Message) -> None:
            try:
                await asyncio.wait_for(send(message), self._send_timeout)
            except asyncio.TimeoutError:
                raise _SlowConsumer from None

        try:
            await super().stream_response(timed)
        finally:
            await self._close_body()

    async def _close_body(self) -> None:
        # An async for that stops early does not close its generator.
        aclose = getattr(self.body_iterator, "aclose", None)
        if aclose is not None:
            await aclose()

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if not self._backend._reserve_reader():
            await self._close_body()
            busy = JSONResponse(
                {
                    "detail": "too many live readers; retry shortly",
                    "code": "stream_capacity_exceeded",
                },
                status_code=503,
                headers={"Retry-After": _RETRY_AFTER},
            )
            await busy(scope, receive, send)
            return
        try:
            await super().__call__(scope, receive, send)
        except _SlowConsumer:
            record_stream_operation(operation="slow_consumer", result="ok")
        finally:
            self._backend._release_reader()


async def sse(
    backend: StreamBackend,
    stream_id: str,
    *,
    request: Request,
    scope: str,
    after: str | None = None,
    expires_at: datetime | None = None,
) -> Response:
    """Prepare an SSE response for a stream.

    Everything that decides the status code happens here, before the first
    byte: the cursor (``after``, then ``Last-Event-ID``, then ``?after=``), the
    stream's existence (404), expiry or trimmed history (410), a cursor ahead
    of the stream (409), a cursor at or past the final entry (204, which stops
    ``EventSource`` for good) and a credential that already expired (401).

    The response then replays the entries after the cursor and follows the
    live stream.  It ends after the final entry, at *expires_at* with a
    ``stream.error`` event whose code is ``credential_expired``, or after
    ``stream_max_connection_duration`` with no event, so the browser reconnects
    through the application's access check.

    Do not copy the ``?after=`` query into *after*: the response reads it
    itself, and a value passed here beats a newer ``Last-Event-ID``.
    """
    await backend._require_support()
    cursor = select_cursor(request, after)
    settings = get_settings()

    credential_left: float | None = None
    if expires_at is not None:
        if expires_at.tzinfo is None:
            expires_at = expires_at.replace(tzinfo=timezone.utc)
        credential_left = (expires_at - datetime.now(timezone.utc)).total_seconds()
        if credential_left <= 0:
            raise StreamCredentialExpired("the credential has expired")

    plan = await backend._prepare(stream_id, scope=scope, after=cursor)
    if plan is None:
        return Response(status_code=204)

    async def body() -> AsyncIterator[str]:
        started = time.monotonic()
        lifetime_end = started + settings.stream_max_connection_duration
        credential_end = (
            started + credential_left if credential_left is not None else None
        )
        deadline = (
            lifetime_end
            if credential_end is None
            else min(lifetime_end, credential_end)
        )
        pages = backend._pages(plan, deadline=deadline)
        try:
            yield f"retry: {_retry_ms()}\n\n"
            async for page in pages:
                if page:
                    # One chunk per read: one send, and one send timeout, for
                    # a whole replay page.
                    yield "".join(_frame(entry) for entry in page)
                now = time.monotonic()
                if credential_end is not None and now >= credential_end:
                    yield encode_frame(
                        ERROR_TYPE, json.dumps({"code": "credential_expired"})
                    )
                    return
                if now >= lifetime_end:
                    return
                if not page:
                    yield ": ping\n\n"
        except StreamStoreError:
            return  # Redis stayed down: close, and the client reconnects
        finally:
            await pages.aclose()

    return _SSEResponse(
        body(),
        backend=backend,
        send_timeout=settings.stream_send_timeout,
        media_type="text/event-stream",
        headers=_HEADERS,
    )


# ---------------------------------------------------------------------------
# Setup and handlers
# ---------------------------------------------------------------------------


async def stream_error_handler(request: Request, exc: Exception) -> Response:
    """Answer a stream error raised before the first byte.

    The status and the stable ``code`` come from the exception's class:
    ``{"detail": ..., "code": ...}``, with ``Retry-After`` on a 503.
    """
    status = exc.status_code if isinstance(exc, StreamError) else 500
    code = exc.code if isinstance(exc, StreamError) else StreamError.code
    headers = {"Retry-After": _RETRY_AFTER} if status == 503 else None
    return JSONResponse(
        {"detail": str(exc), "code": code}, status_code=status, headers=headers
    )


def check_stream_settings() -> None:
    """Reject settings the feature cannot work with.

    Checked when the feature is enabled, not by ``RedisSettings`` itself, so
    an application that does not use streams is never affected.

    Raises:
        StreamConfigurationError: If a setting cannot work.
    """
    settings = get_settings()
    if "{" in settings.prefix or "}" in settings.prefix:
        raise StreamConfigurationError(
            "The key prefix must not contain '{' or '}': Redis Cluster hashes "
            "on the first braces in a key, which would put every stream in "
            "one slot."
        )
    block = settings.stream_heartbeat_interval
    if settings.stream_silence_timeout <= 2 * block:
        raise StreamConfigurationError(
            "stream_silence_timeout must be more than twice "
            "stream_heartbeat_interval, or a live producer looks dead."
        )
    # Redis keeps IDMP history at most a day, and it must cover a run's life.
    if settings.stream_max_run > IDMP_WINDOW:
        raise StreamConfigurationError(
            f"stream_max_run must be at most {IDMP_WINDOW} s: Redis remembers a "
            "stream's idempotency keys for at most a day."
        )
    if settings.socket_timeout is not None and settings.socket_timeout <= block:
        raise StreamConfigurationError(
            "socket_timeout must be longer than stream_heartbeat_interval, which "
            "bounds each blocking stream read."
        )


def add_redis_streams(
    app: FastAPI,
    *,
    coder: type[Coder] | None = None,
) -> StreamBackend:
    """Enable resumable streams on *app*.

    Creates the application's one :class:`StreamBackend`, registers the
    handler that turns stream errors into status codes, and marks the app so
    the lifespan detects server support at startup and stops spawned
    producers at shutdown.  Calling it twice returns the same backend.

    Raises:
        StreamConfigurationError: If a setting cannot work.
    """
    existing: StreamBackend | None = getattr(app.state, "_redis_streams", None)
    if existing is not None:
        return existing
    check_stream_settings()

    from redis_fastapi.deps import _get_pool_state

    backend = StreamBackend(
        lambda: _get_pool_state(app).get_async_client(), coder=coder
    )
    app.state._redis_streams = backend
    # One handler for the whole family.  Starlette picks the most specific
    # class, so an application's handler for a subclass still wins, and one
    # the application already set for StreamError is left in place.
    if StreamError not in app.exception_handlers:
        app.add_exception_handler(StreamError, stream_error_handler)
    return backend
