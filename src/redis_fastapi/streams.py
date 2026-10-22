"""FastAPI setup and prepared SSE responses for generic event logs."""

from __future__ import annotations

import asyncio
import contextlib
import json
import math
from collections.abc import AsyncGenerator, AsyncIterator
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any

import anyio
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response
from redis.asyncio import Redis, RedisCluster

from redis_fastapi.config import RedisSettings, get_settings
from redis_fastapi.stream_backend import (
    InvalidStreamRequest,
    StreamBackend,
    StreamError,
    _minimum_live_read_bytes,
    parse_cursor,
)


def add_redis_streams(
    app: FastAPI,
    *,
    backend: StreamBackend | None = None,
    manage_lifespan: bool = True,
    **options: Any,
) -> None:
    """Register streaming and HTTP errors for this application.

    With an explicit backend, never discover settings, create Redis clients, or
    initialize telemetry. By default wrap the existing lifespan to manage backend
    relay resources. Set manage_lifespan=False when the caller enters/exits the
    backend. Both modes leave the backend's Redis clients caller-owned.

    Without a backend, stream_* options use RedisSettings and require the normal
    Redis lifespan. Identical registration is a no-op; conflicts fail.
    """
    try:
        from fastapi.sse import EventSourceResponse  # noqa: F401
    except ImportError as error:
        raise RuntimeError(
            "Install fastapi-redis-sdk[streams] for native SSE support"
        ) from error
    if backend is not None and options:
        raise ValueError(
            "Configure explicit backends with StreamSettings, not overrides"
        )
    if backend is None and not manage_lifespan:
        raise ValueError("manage_lifespan=False requires an explicit backend")
    registration = (backend, manage_lifespan, dict(options))
    previous = getattr(app.state, "_redis_stream_registration", None)
    if previous is not None:
        if previous != registration:
            raise ValueError("Streaming was already configured differently")
        return

    if backend is None:
        allowed = {
            name for name in RedisSettings.model_fields if name.startswith("stream_")
        }
        if any(name not in allowed for name in options):
            raise ValueError(
                "Unknown streaming option; use stream_* RedisSettings fields"
            )
        settings = get_settings().model_dump(exclude_unset=True)
        settings.update(options)
        validated = RedisSettings.model_validate(settings)
        if validated.stream_replay_bytes < validated.stream_max_event_bytes + 512:
            raise ValueError("Replay byte budget must accommodate one maximum event")
        if validated.stream_read_maxbytes < _minimum_live_read_bytes(validated):
            raise ValueError(
                "Live read budget must accommodate one maximum event, key and framing"
            )
        app.state._redis_stream_settings = validated
    elif manage_lifespan:
        existing = app.router.lifespan_context

        @asynccontextmanager
        async def explicit_runtime(app: FastAPI) -> AsyncIterator[Any]:
            async with backend:
                app.state._redis_stream_backend = backend
                try:
                    async with existing(app) as state:
                        yield state
                finally:
                    del app.state._redis_stream_backend

        app.router.lifespan_context = explicit_runtime
    else:
        app.state._redis_stream_backend = backend

    async def stream_error(request: Request, error: StreamError) -> JSONResponse:
        return JSONResponse(
            {"detail": {"code": error.code}},
            status_code=error.status_code,
            headers={"Retry-After": "2"} if error.status_code == 503 else None,
        )

    app.add_exception_handler(StreamError, stream_error)  # type: ignore[arg-type]
    app.state._redis_stream_registration = registration


def _blocking_client(settings: RedisSettings) -> Any:
    """Build owned relay resources, translating pool-only URL/TLS options."""
    from redis.asyncio.connection import SSLConnection, parse_url

    options = settings.connection_kwargs()
    url = options.pop("url", None)
    if url:
        options.update(parse_url(url))
    connection_class = options.pop("connection_class", None)
    if connection_class is SSLConnection:
        options["ssl"] = True
    elif connection_class is not None:
        # Redis.from_url also supports local Unix sockets. Cluster does not.
        if settings.cluster:
            raise ValueError("Streaming Cluster requires TCP endpoints")
        options["unix_socket_path"] = options.pop("path")
    options["max_connections"] = settings.stream_bucket_count
    options.setdefault("socket_connect_timeout", 2)
    options.setdefault("socket_timeout", max(5, settings.stream_block_ms / 1000 + 2))
    timeout = options["socket_timeout"]
    if timeout is not None and timeout <= settings.stream_block_ms / 1000:
        raise ValueError("Socket timeout must exceed the streaming block timeout")
    factory = RedisCluster if settings.cluster else Redis
    return factory(**options)


@asynccontextmanager
async def _stream_runtime(app: FastAPI, command_client: Any) -> AsyncIterator[None]:
    settings = app.state._redis_stream_settings
    if settings.sentinel:
        raise ValueError("Streaming Sentinel failover is not qualified")
    if (
        settings.socket_timeout is not None
        and settings.socket_timeout <= settings.stream_block_ms / 1000
    ):
        raise ValueError("REDIS_SOCKET_TIMEOUT must exceed the streaming block timeout")
    blocking = _blocking_client(settings)
    backend = StreamBackend(command_client, settings=settings, blocking_client=blocking)
    try:
        async with backend:
            app.state._redis_stream_backend = backend
            try:
                yield
            finally:
                del app.state._redis_stream_backend
    finally:
        await blocking.aclose()


def _cursor(request: Request, explicit: str | None) -> str:
    headers = request.headers.getlist("last-event-id")
    queries = request.query_params.getlist("after")
    if len(headers) > 1 or len(queries) > 1:
        raise InvalidStreamRequest("Duplicate resume cursor")
    for value in [*headers, *queries, *([explicit] if explicit is not None else [])]:
        parse_cursor(value)
    value = (
        explicit
        if explicit is not None
        else (headers[0] if headers else (queries[0] if queries else "0-0"))
    )
    ms, seq = parse_cursor(value)
    return f"{ms}-{seq}"


def _frame(*, event: str, data: Any, event_id: str | None = None) -> bytes:
    fields = ([f"id: {event_id}"] if event_id is not None else []) + [
        f"event: {event}",
        "data: "
        + json.dumps(data, ensure_ascii=False, allow_nan=False, separators=(",", ":")),
    ]
    return ("\n".join(fields) + "\n\n").encode()


async def sse_response(
    request: Request,
    *,
    backend: StreamBackend,
    stream: str,
    after: str | None = None,
    expires_at: datetime | None = None,
) -> Response:
    """Prepare an authorized stream before headers; returning does not await completion.

    ``expires_at`` must be a timezone-aware expiry from verified credentials.
    The application runs its access dependency before calling this helper.
    """
    from fastapi.sse import EventSourceResponse

    cursor = _cursor(request, after)
    settings = backend.settings
    lifetime = settings.stream_max_duration
    credential_limited = False
    if expires_at is not None:
        if expires_at.tzinfo is None or not math.isfinite(expires_at.timestamp()):
            raise InvalidStreamRequest("Credential expiry must be timezone aware")
        remaining = (expires_at - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            return JSONResponse(
                {"detail": {"code": "credential_expired"}}, status_code=401
            )
        credential_limited = remaining <= lifetime
        lifetime = min(lifetime, remaining)
    if backend._relay is None:
        raise StreamError("Stream runtime is not running")
    loop = asyncio.get_running_loop()
    deadline = loop.time() + lifetime
    cm = backend._relay.subscribe(stream, after=cursor)
    # Preparation has a finite bound even with application-owned Redis clients.
    preparing = asyncio.create_task(cm.__aenter__())
    try:
        subscription = await asyncio.wait_for(
            asyncio.shield(preparing), settings.stream_prepare_timeout
        )
    except BaseException as error:
        preparing.cancel()
        with anyio.CancelScope(shield=True):
            result = await asyncio.gather(preparing, return_exceptions=True)
            # Cancellation can race successful __aenter__. Release the acquired
            # subscription even when the awaiting caller never received it.
            if not isinstance(result[0], BaseException):
                await cm.__aexit__(None, None, None)
        if isinstance(error, (TimeoutError, asyncio.TimeoutError)):
            raise StreamError("Subscription preparation timed out") from error
        raise
    if subscription.consumed:
        await cm.__aexit__(None, None, None)
        return Response(status_code=204, headers={"Cache-Control": "no-store"})

    class PreparedResponse(EventSourceResponse):
        def __init__(self) -> None:
            # EventSourceResponse is a media-type marker; returning an instance
            # bypasses FastAPI's generator encoding, so frames are encoded here.
            self.frame_iterator = self.frames()
            super().__init__(
                self.frame_iterator,
                headers={"Cache-Control": "no-store", "X-Accel-Buffering": "no"},
            )
            self.closed = False
            self.started = False
            self.cleanup_task: asyncio.Task[None] | None = None
            self.watchdog = loop.call_later(
                settings.stream_prepare_timeout, self.abandon
            )

        def abandon(self) -> None:
            if not self.started:
                self.cleanup_task = asyncio.create_task(self.aclose())

        async def aclose(self) -> None:
            if not self.closed:
                self.closed = True
                self.watchdog.cancel()
                await cm.__aexit__(None, None, None)

        async def frames(self) -> AsyncGenerator[bytes, None]:
            iterator = subscription.events()
            pending: asyncio.Task[Any] | None = None
            try:
                while True:
                    if pending is None:
                        pending = asyncio.create_task(anext(iterator))
                    remaining = deadline - loop.time()
                    if remaining <= 0:
                        code = (
                            "credential_expired"
                            if credential_limited
                            else "connection_expired"
                        )
                        yield _frame(
                            event="redis.stream.control",
                            data={
                                "code": code,
                                "action": "reauthenticate"
                                if credential_limited
                                else "reconnect",
                            },
                        )
                        return
                    done, _ = await asyncio.wait(
                        [pending], timeout=min(settings.stream_keepalive, remaining)
                    )
                    if not done:
                        if loop.time() < deadline:
                            yield b": keepalive\n\n"
                        continue
                    try:
                        event = pending.result()
                    except StopAsyncIteration:
                        return
                    except StreamError as error:
                        yield _frame(
                            event="redis.stream.control",
                            data={"code": error.code, "action": error.action},
                        )
                        return
                    pending = None
                    yield _frame(event=event.event, data=event.data, event_id=event.id)
            finally:
                if pending is not None:
                    pending.cancel()
                    with contextlib.suppress(
                        asyncio.CancelledError, StopAsyncIteration, StreamError
                    ):
                        await pending
                await iterator.aclose()

        async def __call__(self, scope: Any, receive: Any, send: Any) -> None:
            if self.closed:
                raise StreamError("Prepared subscription expired before response start")
            self.started = True
            self.watchdog.cancel()

            async def bounded_send(message: Any) -> None:
                await asyncio.wait_for(send(message), settings.stream_send_timeout)

            try:
                await super().__call__(scope, receive, bounded_send)
            finally:
                # Shield cleanup from Starlette's disconnect cancellation scope.
                with anyio.CancelScope(shield=True):
                    try:
                        await self.frame_iterator.aclose()
                    finally:
                        await self.aclose()

    return PreparedResponse()
