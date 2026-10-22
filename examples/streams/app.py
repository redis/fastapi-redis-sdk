"""Local bearer-authenticated example. Application state here is in memory."""

from __future__ import annotations

import os
import secrets
from dataclasses import dataclass
from uuid import uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import BaseModel

from redis_fastapi import FastAPIRedis, StreamBackendDep, sse_response

app = FastAPI()
FastAPIRedis(app).lifespan().streams()
security = HTTPBearer()


@dataclass
class Chat:
    id: str
    event_stream_id: str
    owner: str


chats: dict[str, Chat] = {}


def current_user(credentials: HTTPAuthorizationCredentials = Depends(security)) -> str:
    expected = os.environ.get("CHAT_DEMO_TOKEN")
    if not expected:
        raise HTTPException(
            503, "Set CHAT_DEMO_TOKEN before running this local example"
        )
    if not secrets.compare_digest(credentials.credentials, expected):
        raise HTTPException(
            401, "Invalid credentials", headers={"WWW-Authenticate": "Bearer"}
        )
    return "local-demo-user"


def require_chat_read_access(chat_id: str, user: str = Depends(current_user)) -> Chat:
    chat = chats.get(chat_id)
    if chat is None or chat.owner != user:
        raise HTTPException(404, "Chat not found")
    return chat


@app.post("/chats")
async def create_chat(
    streams: StreamBackendDep, user: str = Depends(current_user)
) -> dict[str, str]:
    chat = Chat(uuid4().hex, uuid4().hex, user)
    await streams.create(chat.event_stream_id, ttl=3600)
    chats[chat.id] = chat
    return {"chat_id": chat.id}


class Message(BaseModel):
    id: str
    text: str


@app.post("/chats/{chat_id}/messages")
async def publish_message(
    message: Message,
    streams: StreamBackendDep,
    chat: Chat = Depends(require_chat_read_access),
) -> dict[str, str]:
    cursor = await streams.append(
        chat.event_stream_id,
        event="message",
        data=message.model_dump(),
        producer="demo",
        idempotency_key=message.id,
    )
    return {"cursor": cursor}


@app.post("/chats/{chat_id}/close")
async def close_chat(
    streams: StreamBackendDep, chat: Chat = Depends(require_chat_read_access)
) -> dict[str, str]:
    cursor = await streams.finish(
        chat.event_stream_id,
        event="chat.closed",
        data={"chat_id": chat.id},
        producer="demo",
        idempotency_key=f"{chat.id}:close",
    )
    return {"cursor": cursor}


@app.get(
    "/chats/{chat_id}/events",
    response_class=StreamingResponse,
    responses={200: {"content": {"text/event-stream": {}}}},
)
async def chat_events(
    request: Request,
    streams: StreamBackendDep,
    chat: Chat = Depends(require_chat_read_access),
):
    return await sse_response(request, backend=streams, stream=chat.event_stream_id)
