"""Encrypt the session at rest with AES-GCM.

Anyone with ``redis-cli`` access can read a plain session.  With an
``Encryptor`` the store encrypts the value of each attribute, so Redis holds
ciphertext for every value.  The names of the attributes stay readable, so
do not put sensitive data in a name.

The package ships the seam, not the cipher: the ten-line class below is yours
to audit, and a key rotation is your decision.  A record that will not decrypt
- after a key change, say - reads as no session and is kept, so restoring the
old key brings its users back.

Run it (plain HTTP, so the cookie must not be ``Secure``)::

    export REDIS_URL=redis://localhost:6379/0 REDIS_SESSION_COOKIE_HTTPS_ONLY=false
    export SESSION_KEY=$(python -c 'import os; print(os.urandom(32).hex())')
    uv run --with "fastapi[standard]" --with cryptography fastapi dev examples/sessions/encryption.py

    curl -c jar -b jar -X POST localhost:8000/login
    curl -b jar localhost:8000/me                 # {"user_id":"alice","tax_id":"..."}
    SID=$(awk '$6 == "session" {print $7}' jar)
    redis-cli HGET rf:s:$SID a:tax_id   # ciphertext, not the tax ID
"""

from __future__ import annotations

import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM
from fastapi import FastAPI

from redis_fastapi import FastAPIRedis, SessionDep


class AesGcmEncryptor:
    def __init__(self, key: bytes) -> None:
        self._aes = AESGCM(key)  # 16, 24 or 32 bytes

    def encrypt(self, data: bytes) -> bytes:
        nonce = os.urandom(12)  # never reuse a nonce with the same key
        return nonce + self._aes.encrypt(nonce, data, None)

    def decrypt(self, data: bytes) -> bytes:
        return self._aes.decrypt(data[:12], data[12:], None)


app = FastAPI()
FastAPIRedis(app).lifespan().sessions(
    encryptor=AesGcmEncryptor(bytes.fromhex(os.environ["SESSION_KEY"]))
)


@app.post("/login")
async def login(session: SessionDep) -> dict[str, bool]:
    session["user_id"] = "alice"
    session["tax_id"] = "123-45-6789"  # the kind of value worth encrypting
    return {"signed_in": True}


@app.get("/me")
async def me(session: SessionDep) -> dict[str, str | None]:
    return {"user_id": session.get("user_id"), "tax_id": session.get("tax_id")}
