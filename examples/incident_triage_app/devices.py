"""Several devices in one browser, for the demo only.

The story needs a laptop, a phone and a second engineer, each with its own
session.  One browser has one cookie jar, so this middleware gives each device
its own cookie: a request with ``?device=phone`` sends ``session_phone`` to the
SDK as ``session``, and a ``Set-Cookie: session=...`` goes back as
``session_phone=...``.  The SDK sees an ordinary session cookie.

A real app does not need this: each device is its own browser.
"""

from __future__ import annotations

from http.cookies import SimpleCookie
from urllib.parse import parse_qs

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from redis_fastapi import get_settings

DEVICES = ("laptop", "phone", "sam")


class DeviceCookies:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        query = parse_qs(scope.get("query_string", b"").decode())
        device = query.get("device", ["laptop"])[0]
        if device not in DEVICES:
            device = "laptop"
        name = get_settings().session_cookie_name
        own = f"{name}_{device}"

        headers = []
        for key, value in scope["headers"]:
            if key == b"cookie":
                jar = SimpleCookie()
                jar.load(value.decode("latin-1"))
                if own in jar:
                    headers.append((b"cookie", f"{name}={jar[own].value}".encode()))
                continue  # drop every other device's cookie
            headers.append((key, value))
        scope = {**scope, "headers": headers}

        prefix = f"{name}=".encode()

        async def send_renamed(message: Message) -> None:
            if message["type"] == "http.response.start":
                message["headers"] = [
                    (k, own.encode() + v[len(name) :])
                    if k == b"set-cookie" and v.startswith(prefix)
                    else (k, v)
                    for k, v in message["headers"]
                ]
            await send(message)

        await self.app(scope, receive, send_renamed)
