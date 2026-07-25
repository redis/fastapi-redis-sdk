# WebSocket Pub/Sub

`RedisChannelManagerDep` provides transient Redis Pub/Sub broadcasts for
FastAPI WebSocket endpoints. It reuses the async Redis client created by
`FastAPIRedis(app).lifespan()`, so HTTP publishers and WebSocket subscribers
work across processes and workers without opening application-managed Redis
clients.

## Setup

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan()
```

No additional builder method or middleware is required.

## Chat room

WebSocket reads and Redis messages must run concurrently. AnyIO is already a
runtime dependency of FastAPI and this SDK:

```python
from anyio import create_task_group
from fastapi import WebSocket, WebSocketDisconnect
from redis_fastapi import RedisChannelManagerDep


@app.websocket("/rooms/{room_id}")
async def room(
    websocket: WebSocket,
    room_id: str,
    channels: RedisChannelManagerDep,
) -> None:
    await websocket.accept()
    channel = f"room:{room_id}"

    async with channels.subscribe(channel) as messages:
        async with create_task_group() as tasks:
            async def receive() -> None:
                try:
                    while True:
                        message = await websocket.receive_text()
                        await channels.publish(channel, message)
                except WebSocketDisconnect:
                    tasks.cancel_scope.cancel()

            async def send() -> None:
                async for message in messages:
                    text = message.decode() if isinstance(message, bytes) else message
                    await websocket.send_text(text)

            tasks.start_soon(receive)
            tasks.start_soon(send)
```

Each worker subscribes through Redis, so a message received by one worker is
delivered to clients connected to the other workers.

## Publish from an HTTP endpoint

The same dependency works in HTTP endpoints:

```python
from redis_fastapi import RedisChannelManagerDep


@app.post("/users/{user_id}/notifications")
async def notify(
    user_id: str,
    message: str,
    channels: RedisChannelManagerDep,
) -> dict[str, int]:
    subscribers = await channels.publish(f"user:{user_id}", message)
    return {"subscribers": subscribers}
```

A WebSocket can consume that user channel:

```python
@app.websocket("/users/{user_id}/notifications")
async def notifications(
    websocket: WebSocket,
    user_id: str,
    channels: RedisChannelManagerDep,
) -> None:
    await websocket.accept()
    async with channels.subscribe(f"user:{user_id}") as messages:
        async for message in messages:
            if isinstance(message, bytes):
                await websocket.send_bytes(message)
            else:
                await websocket.send_text(message)
```

## Payloads and delivery

`publish()` accepts `str` or `bytes` and returns the number of subscribers
reported by Redis. Received payloads are `bytes` by default, or `str` when the
Redis client is configured with response decoding.

Redis Pub/Sub is at-most-once and does not persist messages. A disconnected
subscriber misses messages sent while it is offline. Use Redis Streams when
you need history, replay, acknowledgements, or stronger delivery guarantees.

## Connections and cleanup

Every active `subscribe()` context holds one connection from the shared Redis
pool. Size `REDIS_MAX_CONNECTIONS` for the expected number of simultaneous
WebSocket subscriptions plus ordinary Redis commands.

Leaving the context for any reason, including a WebSocket disconnect,
cancellation, or subscription error, closes the Pub/Sub object and returns its
connection to the pool.

## Testing

Override the provider with FastAPI's standard dependency override:

```python
from redis_fastapi import get_redis_channel_manager

app.dependency_overrides[get_redis_channel_manager] = fake_channel_manager
```

You can also override `get_async_redis` with `fakeredis` to exercise the real
manager without a Redis server.

