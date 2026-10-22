# Resumable chat event example

Use a local Redis 8.10 server. This example keeps chat ownership in memory and
uses one bearer token; replace both with application identity and database state.

```bash
export REDIS_URL=redis://localhost:6379
export CHAT_DEMO_TOKEN=local-demo-token
uv run --extra streams --with uvicorn uvicorn examples.streams.app:app
```

Create a chat, copying its returned ID into the commands below:

```bash
curl -H 'Authorization: Bearer local-demo-token' -X POST localhost:8000/chats
curl -H 'Authorization: Bearer local-demo-token' -H 'Content-Type: application/json' \
  -d '{"id":"message-1","text":"Hello"}' localhost:8000/chats/CHAT_ID/messages
curl -N -H 'Authorization: Bearer local-demo-token' localhost:8000/chats/CHAT_ID/events
```

Stop curl, publish another message with a different stable message ID, and resume
using the last applied event cursor:

```bash
curl -N -H 'Authorization: Bearer local-demo-token' -H 'Last-Event-ID: CURSOR' \
  localhost:8000/chats/CHAT_ID/events
curl -H 'Authorization: Bearer local-demo-token' -X POST localhost:8000/chats/CHAT_ID/close
```

The close event seals the log. Reconnecting at its final cursor returns 204.
Stopping the HTTP client does not stop publication. Use Ctrl-C to stop uvicorn.
A bearer-authenticated browser must use a fetch-based SSE client; native
EventSource cannot set an Authorization header. See the
[streams guide](../../docs/guide/streams.md) for retention and recovery semantics.
