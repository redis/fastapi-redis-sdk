# Pub/Sub

Redis [Pub/Sub](https://redis.io/docs/latest/develop/interact/pubsub/) enables
real-time messaging between application instances.  fastapi-redis-sdk provides
a `PubSubManager` that wraps Redis Pub/Sub for use in WebSocket endpoints and
background tasks, reusing the shared async pool.

## When to use Pub/Sub

| Scenario | Example |
|----------|---------|
| Cross-worker chat rooms | Multiple server instances subscribe to the same channel |
| Real-time notifications | Broadcast events to connected WebSocket clients |
| Live updates | Push data changes to browsers without polling |
| Broadcasts | Send the same message to all subscribers |

## Setup

`PubSubManager` only needs a Redis connection pool; no additional
middleware or configuration is required:

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan()  # pool is set up; Pub/Sub is ready
```

## Publishing messages

Use `PubSubManagerDep` from any HTTP endpoint to publish messages:

```python
from fastapi import Depends
from redis_fastapi import PubSubManagerDep

@app.post("/notify")
async def notify(message: str, pubsub: PubSubManagerDep):
    count = await pubsub.publish("notifications", message)
    return {"subscribers": count}
```

The returned integer is the number of subscribers that received the message
(across all connected Redis clients).

## WebSocket subscriptions

``PubSubManagerDep`` works in WebSocket handlers directly because
``get_async_redis`` accepts ``HTTPConnection``, the common base type
of both ``Request`` and ``WebSocket``:

```python
from fastapi import WebSocket
from redis_fastapi import PubSubManagerDep

@app.websocket("/ws/chat/{room}")
async def chat(websocket: WebSocket, room: str, pubsub: PubSubManagerDep):
    await websocket.accept()

    channel = f"chat:{room}"
    await pubsub.subscribe(channel)

    async def forward_to_ws():
        async for message in pubsub.listen():
            await websocket.send_text(message["data"].decode())

    async def forward_to_redis():
        async for data in websocket.iter_text():
            await pubsub.publish(channel, data)

    try:
        async with anyio.create_task_group() as tg:
            tg.start_soon(forward_to_ws)
            tg.start_soon(forward_to_redis)
    except Exception:
        pass
    finally:
        await pubsub.close()
```

The `listen()` method yields only `message`-type events; system messages
(subscription confirmations, etc.) are filtered out automatically.

## Broadcasting

Publish to a channel without subscribing:

```python
@app.post("/broadcast")
async def broadcast(message: str, pubsub: PubSubManagerDep):
    count = await pubsub.publish("broadcasts", message)
    return {"sent_to": count}
```

## Managing subscriptions

Subscribe to multiple channels at once:

```python
await pubsub.subscribe("channel:a", "channel:b", "channel:c")
```

Unsubscribe selectively:

```python
await pubsub.unsubscribe("channel:a")
```

Unsubscribe all and close:

```python
await pubsub.close()
```

`close()` is safe to call multiple times and handles cleanup gracefully
even if the underlying connection was never established.

## Testing

Unit tests can use `fakeredis` to simulate Pub/Sub without a real Redis
server:

```python
import fakeredis.aioredis
from redis_fastapi import PubSubManager

async def test_pubsub():
    fake = fakeredis.aioredis.FakeRedis()
    pubsub = PubSubManager(fake)

    # Subscribe and publish in sequence
    await pubsub.subscribe("test:ch")

    # Listen for the published message
    async def publish_later():
        import anyio
        await anyio.sleep(0.05)
        await pubsub.publish("test:ch", "hello")

    async with anyio.create_task_group() as tg:
        tg.start_soon(publish_later)
        async for message in pubsub.listen():
            assert message["data"] == b"hello"
            break

    await pubsub.close()
```

## Limitations

Redis Pub/Sub is **transient and at-most-once**:

- Messages are **not persisted**; if a subscriber disconnects, it misses
  messages sent while offline.
- There is **no message acknowledgement**; if a subscriber crashes after
  receiving a message, that message is lost.
- Delivery is **at-most-once**; a message is delivered zero or one times.

For persistent message queues with consumer groups, see
[Redis Streams](https://redis.io/docs/latest/develop/data-types/streams/).

## API Reference

| Method | Description |
|--------|-------------|
| `subscribe(*channels)` | Subscribe to one or more Redis channels |
| `unsubscribe(*channels)` | Unsubscribe from Redis channels |
| `publish(channel, message)` | Publish a message, returns subscriber count |
| `listen()` | Async generator yielding `message`-type PubSub events |
| `close()` | Unsubscribe all + close the PubSub connection |
