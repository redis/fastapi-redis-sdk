"""Exercise the installed wheel outside the source checkout against owned Redis.

Run with a pinned Redis 8.10.2 server path as the sole argument. The caller must
install the wheel's streams extra and httpx in an isolated environment.
"""

from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import httpx
import redis
import redis.asyncio as async_redis
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

import redis_fastapi
from redis_fastapi import (
    FastAPIRedis,
    StreamBackend,
    StreamBackendDep,
    StreamSettings,
    add_redis_streams,
    sse_response,
)


async def exercise(port: int) -> None:
    os.environ["REDIS_URL"] = f"redis://127.0.0.1:{port}"
    os.environ["REDIS_PREFIX"] = "installed-stream-smoke"
    redis_fastapi.get_settings.cache_clear()
    app = FastAPI()
    FastAPIRedis(app).lifespan().streams(stream_bucket_count=2)

    @app.get("/events", response_class=StreamingResponse)
    async def events(request: Request, streams: StreamBackendDep):
        return await sse_response(request, backend=streams, stream="chat")

    async with app.router.lifespan_context(app):
        backend = app.state._redis_stream_backend
        await backend.create("chat", ttl=60)
        first = await backend.append(
            "chat",
            event="message",
            data={"text": "hello"},
            producer="worker",
            idempotency_key="1",
        )
        final = await backend.finish(
            "chat", event="done", data={}, producer="worker", idempotency_key="2"
        )
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app), base_url="http://test"
        ) as client:
            response = await client.get("/events", headers={"Last-Event-ID": first})
            assert response.status_code == 200 and f"id: {final}" in response.text
            assert f"id: {first}" not in response.text
            assert (
                await client.get("/events", headers={"Last-Event-ID": final})
            ).status_code == 204
    print(f"Installed stream API passed: {redis_fastapi.__file__}")


async def exercise_explicit(port: int) -> None:
    # An accidental global lookup must fail, even after the previous smoke test.
    os.environ["REDIS_URL"] = "redis://unreachable.invalid:1"
    os.environ["REDIS_STREAM_BUCKET_COUNT"] = "invalid"
    redis_fastapi.get_settings.cache_clear()
    async with (
        async_redis.Redis(port=port, max_connections=8) as commands,
        async_redis.Redis(port=port, max_connections=2) as relays,
    ):
        backend = StreamBackend(
            commands,
            blocking_client=relays,
            settings=StreamSettings(prefix="installed-explicit", stream_bucket_count=2),
        )
        app = FastAPI()
        add_redis_streams(app, backend=backend)

        @app.get("/events", response_class=StreamingResponse)
        async def events(request: Request, streams: StreamBackendDep):
            return await sse_response(request, backend=streams, stream="chat")

        async with app.router.lifespan_context(app):
            await backend.create("chat", ttl=60)
            final = await backend.finish(
                "chat", event="done", data={}, producer="worker", idempotency_key="1"
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://test"
            ) as client:
                response = await client.get("/events")
                assert response.status_code == 200 and f"id: {final}" in response.text
                assert (
                    await client.get("/events", headers={"Last-Event-ID": final})
                ).status_code == 204
        assert await commands.ping() and await relays.ping()
        assert not hasattr(app.state, "_redis_stream_backend")
    print("Installed explicit backend API passed")


def main() -> None:
    checkout = Path(__file__).resolve().parents[1]
    assert not Path(redis_fastapi.__file__).resolve().is_relative_to(checkout)
    binary = Path(sys.argv[1]).resolve()
    version = subprocess.check_output([str(binary), "--version"], text=True, timeout=5)
    assert "v=8.10.2 " in version
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    with tempfile.TemporaryDirectory(prefix="installed-streams-") as directory:
        with (Path(directory) / "server.log").open("w") as log:
            process = subprocess.Popen(
                [
                    str(binary),
                    "--bind",
                    "127.0.0.1",
                    "--port",
                    str(port),
                    "--dir",
                    directory,
                    "--save",
                    "",
                    "--appendonly",
                    "no",
                ],
                stdout=log,
                stderr=log,
            )
            try:
                deadline = time.monotonic() + 10
                with redis.Redis(port=port, socket_timeout=1) as client:
                    while True:
                        try:
                            if client.ping():
                                break
                        except redis.RedisError:
                            pass
                        assert process.poll() is None
                        if time.monotonic() >= deadline:
                            raise RuntimeError(
                                "Owned Redis server did not become ready"
                            )
                        time.sleep(0.01)
                asyncio.run(exercise(port))
                asyncio.run(exercise_explicit(port))
            finally:
                process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


if __name__ == "__main__":
    main()
