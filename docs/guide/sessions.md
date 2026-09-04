# Sessions

Server-side sessions for FastAPI. The cookie carries an opaque identifier and
nothing else; the record lives in Redis, where the server enforces both of its
deadlines.

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis, SessionDep

app = FastAPI()
FastAPIRedis(app).lifespan().sessions()

@app.post("/login")
async def login(session: SessionDep) -> dict:
    session["user_id"] = 42       # this rotates the session ID
    return {"ok": True}

@app.get("/me")
async def me(session: SessionDep) -> dict:
    return {"user_id": session.get("user_id")}
```

`request.session` works too, so code written against Starlette's signed-cookie
middleware runs unchanged.

---

## Rotation is automatic

Writing the identity rotates the session ID. There is no `rotate()` to call and
therefore none to forget, which matters because forgetting it is the one
mistake in this API that is a vulnerability — session fixation.

The middleware evaluates a **principal** twice per request, once before the
application runs and once at `http.response.start`. If the two differ and the
response status is below 400, it issues a new ID and deletes the old key first.

Watch more than the user ID to get OWASP's privilege-change rotation:

```python
FastAPIRedis(app).lifespan().sessions(principal_keys=["user_id", "role"])
```

For anything a list of keys cannot express, supply a function:

```python
FastAPIRedis(app).lifespan().sessions(
    # Identity exists, but authentication is not finished at MFA step one.
    principal_of=lambda s: s.get("user_id") if s.get("mfa_ok") else None,
)
```

`principal_of` must be **pure, cheap and deterministic** — it runs twice per
request and the two results are compared by value — and it must return a
*verified* identity, never something the client set.

!!! warning "A misconfigured resolver is silent"
    If your application writes `uid` but the resolver reads `user_id`, nothing
    rotates and nothing complains. Assert it once:

    ```python
    def test_login_rotates(client):
        before = client.post("/write").cookies["session"]
        after = client.post("/login").cookies["session"]
        assert after != before
    ```

    The `operation="rotate"` counter is the runtime backstop: sign-ins with no
    rotations is a visible anomaly on a dashboard.

---

## Two clocks, both enforced by Redis

| Setting | Default | What it bounds |
|---|---|---|
| `session_idle_ttl` | 30 min | Time since the last request carrying the cookie |
| `session_absolute_ttl` | 8 h | Time since the session was created, however active the user |

They live on two separate hash fields with their own expirations, so **Redis
enforces both and this library computes neither**. Writing the payload touches
one field and never the other, so no number of writes can extend the absolute
deadline.

The absolute clock is the one that matters against a stolen session. An idle
timeout cannot expire a session an attacker is actively using, because the
attacker's own requests keep refreshing it — OWASP says so directly. The
absolute deadline is the only clock that fires on a live compromise.

Both accept an `int` number of seconds. The store constructor also accepts a
`timedelta`; the settings and their environment variables take seconds. Setting either to `0` disables
that clock; setting both gives a cookie-only session that the browser drops
when it closes.

The cookie's `max-age` is `min(idle, absolute remaining)`, and both numbers come
from Redis rather than from your process — so a container with a skewed clock
cannot produce a cookie that outlives its record and signs a user out with no
explanation.

---

## Signing out, everywhere

```python
from redis_fastapi import SessionStateDep, SessionStoreDep

@app.post("/logout")
async def logout(state: SessionStateDep, store: SessionStoreDep) -> dict:
    await store.revoke(state)
    return {"ok": True}

@app.post("/logout-everywhere")
async def logout_all(session: SessionDep, store: SessionStoreDep) -> dict:
    return {"ended": await store.revoke_all(str(session["user_id"]))}

@app.get("/devices")
async def devices(
    session: SessionDep, state: SessionStateDep, store: SessionStoreDep
) -> list[dict]:
    current = store.session_id(state)
    return [
        {"id": info.session_id, "this_device": info.session_id == current,
         "last_seen": info.last_access}
        for info in await store.list_for_subject(str(session["user_id"]))
    ]
```

`SessionDep` is the data — a `dict` you read and write. `SessionStateDep` is
the handle: the identifier, the subject and the timestamps. Operations that
act on the *session* rather than its contents take the handle.

Simply emptying the session signs the user out too, and is often all you need:

```python
@app.post("/logout")
async def logout(session: SessionDep) -> dict:
    session.clear()          # key deleted, index entry dropped, cookie cleared
    return {"ok": True}
```

`revoke_id` requires the subject and refuses an ID that is not indexed under it,
so a handler taking an ID from a request cannot end a stranger's session.

A listing is **verified before it is returned**. The index that answers "which
sessions has this user got?" is an upper bound: entries expire on the absolute
clock while most sessions die of idleness long before, so an entry routinely
outlives the session it names. Reporting one would show a user a device they
are not signed in on and a sign-out button that does nothing.

---

## When Redis is unreachable

The default is asymmetric, and the asymmetry is the point.

- **A failed read yields an empty session.** The user looks anonymous, your own
  authorization dependency finds no user, and a protected route stays protected
  because it never depended on the read succeeding.
- **A failed write raises `SessionStoreError`.** Losing a login or a rotation is
  the worst outcome here and must never be silent.

Set `session_fail_closed=True` to turn the failed read into an error too, for a
deployment that prefers a 503 to an anonymous page. Writes raise either way.

---

## Nested changes are invisible

```python
session["prefs"]["theme"] = "dark"     # NOT saved
session["prefs"] = {**session["prefs"], "theme": "dark"}   # saved
```

No `dict` subclass in any language can see a change inside a value it holds.
Reassign the top-level key, or set `session_always_save=True` to write on every
request that touched the session.

---

## Real-time session events (optional, Redis 8.8+)

Close a WebSocket the moment a session ends, instead of finding out on the next
HTTP request:

```python
from redis_fastapi import SessionEvents

events = SessionEvents(redis, key_prefix="redis:fastapi")

@events.on_session_end
async def _(session_id: str, cause: str) -> None:   # "idle" or "absolute"
    await close_sockets_for(session_id)

await events.start()
```

This needs Redis 8.8 for hash subkey notifications, and it needs the server
configured for them:

```
CONFIG SET notify-keyspace-events Th
```

The subkey flags `S`, `T`, `I`, `V` are **independent of `K` and `E`** — setting
`KEA` enables every standard keyspace event and still delivers none of these.
This library will never set the option for you: it is server-wide and affects
every other application on the instance.

!!! danger "The callback is best-effort, and silence is a possible outcome"
    On a server below 8.8, one without the flags, or one where `CONFIG GET` is
    unavailable — which is common on managed Redis — `events.tier` is `"none"`,
    one warning is logged at startup, and **your handlers never run**. Startup
    still succeeds and every request still works.

    A revocation handler that never fires looks exactly like one that works. If
    prompt closure matters, check `events.tier` and add a periodic sweep as
    well. Redis Pub/Sub is fire-and-forget: events sent while no subscriber is
    connected are lost, and an expiry event fires when Redis removes the field
    rather than when the deadline passed.

    Nothing else depends on this. Expiry, revocation and the index all work
    identically with events switched off.

On a cluster, keyspace events are node-local and are not broadcast, so seeing
every event needs one subscriber per node.

---

## Caching an endpoint that uses the session

A cached response is stored once and served many times. A session-dependent
response is different for every user. Put those two together without saying
which you meant and the first user's body is served to the second — so
`cache()` asks you to say.

One parameter, three answers:

```python
cache(ttl=300, vary_on_session=True)    # depends on who is asking
cache(ttl=300, vary_on_session=False)   # reads the session, body is the same
cache(ttl=300)                          # you have not said
```

### `vary_on_session=True` — the body differs per user

Use it when the response contains the user's own data.

```python
@app.get(
    "/me",
    dependencies=[Depends(cache(ttl=300, vary_on_session=True))],
)
async def me(session: SessionDep) -> dict:
    return {"user_id": session["user_id"], "cart": session.get("cart", [])}
```

Each user gets their own cache entry and their own hits — Alice's second
request is a `HIT` on Alice's copy. The key is built from the **subject**, not
the session ID, so it survives a rotation (a privilege change does not throw
the entry away) and is shared across that user's devices.

The response carries `Cache-Control: private, max-age=300`. The `private` is
not optional and is added for you: our entry is per user, so a CDN told it may
keep one copy for everyone would recreate the leak one hop further out.

### `vary_on_session=False` — reads the session, same body for everyone

Use it when an auth dependency reads the session but the payload does not
depend on who asked.

```python
async def require_user(session: SessionDep) -> int:
    if "user_id" not in session:
        raise HTTPException(401)
    return session["user_id"]

@app.get(
    "/catalogue",
    dependencies=[
        Depends(require_user),                          # reads the session
        Depends(cache(ttl=300, vary_on_session=False)), # body does not
    ],
)
async def catalogue() -> list[dict]:
    return await load_products()
```

One shared entry serves every signed-in user, and the `Vary: Cookie` the
session middleware would otherwise add is suppressed — keeping it would force a
CDN to store one identical copy per user and destroy the hit rate this
declaration exists to protect.

**This is an assertion, and the library takes your word for it.** If the body
does depend on the session, you have re-enabled the leak deliberately.

### Saying nothing

If the endpoint never touches the session, say nothing — there is nothing to
declare and caching behaves exactly as it always has.

If it *does* touch the session and you have not declared anything, the response
is served normally but **not stored**, and the route is named once in a log
line:

```
WARNING  GET /me read the session but is cached without vary_on_session set;
         the response was not stored. Pass vary_on_session=True to cache it
         per user, or False if the response does not depend on the session.
```

Losing caching is a visible, recoverable problem. Serving Alice's account page
to Bob is not, which is why the default errs this way.

Note that these last two are the *same* declaration — you write nothing in both
cases. The library tells them apart by whether the endpoint actually read the
session, which it can only know after the endpoint has run. That is also why
the choice cannot be inferred for you: the cache key is needed *before* the
endpoint runs, and reading the session does not tell us whether the response
depends on it.

### At a glance

| Your endpoint | Declaration | Cached as | Response headers |
|---|---|---|---|
| Body depends on the user | `vary_on_session=True` | one entry per user | `private, max-age=N` + `Vary: Cookie` |
| Reads the session, body identical | `vary_on_session=False` | one shared entry | `max-age=N`, no `Vary` |
| Never touches the session | *(nothing)* | one shared entry | `max-age=N` |
| Touches it, nothing declared | *(nothing)* | **not cached** | `private, no-store` + `Vary: Cookie` |

The rule underneath all four rows: **what a response tells other caches they
may do is never more permissive than what this library does itself.** If we
key per user, we say `private`. If we refuse to store, we say `no-store`.

### A session route with no caching on it

Reading the session on an uncached route emits `Cache-Control: private` and
`Vary: Cookie` on its own, since nothing else will. Where `cache()` is present
it owns the header outright — one writer, so you never see two contradictory
`Cache-Control` values on one response.

---

## Limitation: WebSockets have no session in this release

The middleware handles `http` scopes only. In a WebSocket handler,
`websocket.session` raises `AssertionError`, and `SessionDep` /
`SessionStateDep` raise `SessionConfigurationError`.

This is a deliberate boundary for the first release, not an oversight, and it
is a difference from Starlette's own `SessionMiddleware`, which handles both
scopes. If you are migrating from it and read the session inside a WebSocket
handler, that code needs changing.

**Why it is not simply switched on.** The read half is easy; the write half has
nowhere to go. A WebSocket has no `http.response.start`, so there is no point
at which a cookie can be set — which means no rotation, no save, and no
idle-clock refresh for the life of the connection. Half a session object,
silently read-only, invites exactly the bug the rest of this design works to
prevent: an application writes to it, sees no error, and loses the write.

**What to do instead.** Authenticate during the HTTP handshake, where the
cookie *is* available, and pass what the socket needs into the handler:

```python
@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    # The handshake is an HTTP request, so the cookie arrives with it.
    raw = websocket.cookies.get("session")
    store = await get_session_store(websocket)      # type: ignore[arg-type]
    loaded = await store.load(raw) if raw and store.is_valid_id(raw) else None
    if loaded is None:
        await websocket.close(code=1008)
        return

    user_id = loaded.record.data.get("user_id")
    await websocket.accept()
    ...
```

Load once at accept time and hold the identity for the connection. If you need
the socket to close when the session ends, pair it with
[real-time session events](#real-time-session-events-optional-redis-88) — that
is the case those exist for.

---

## CSRF: what a cookie session reintroduces

A cookie is attached by the browser to **every** request to your origin,
including one triggered by a form on somebody else's page. That is what makes
a session cookie convenient and it is also the whole of CSRF: an attacker
cannot read your session, but they can make the browser spend it.

A bearer token in an `Authorization` header does not have this problem, because
nothing attaches it automatically. Moving to cookies gets you revocation,
rotation and server-side expiry, and it hands this back.

**`SameSite=Lax` is the default here and it is most of the remedy.** The browser
withholds the cookie on cross-site `POST`, `PUT`, `PATCH` and `DELETE`. It does
*not* withhold it on a cross-site top-level `GET`, so:

- **Never change state in a `GET`.** A `GET /account/delete` is exploitable
  under `Lax` and no cookie attribute will save it.
- **Add a CSRF token for anything a browser form can reach.** `SameSite` is a
  defence in depth, not a substitute — it is unenforced on some older browsers,
  and `Lax` has a two-minute exemption window for top-level POSTs in some
  Chromium versions.
- **`SameSite=Strict`** closes the top-level `GET` hole too, at the cost of the
  cookie being withheld when a user follows a link into your site from
  anywhere else — including their own email.

The session is the natural place to keep the token:

```python
import secrets
from fastapi import HTTPException

@app.get("/form")
async def form(session: SessionDep) -> dict:
    token = session.setdefault("csrf", secrets.token_urlsafe(32))
    return {"csrf_token": token}

@app.post("/transfer")
async def transfer(session: SessionDep, csrf_token: str = Form(...)) -> dict:
    expected = session.get("csrf")
    if not expected or not secrets.compare_digest(csrf_token, expected):
        raise HTTPException(403, "CSRF token mismatch")
    ...
```

`secrets.compare_digest` rather than `==`, and the token rotates with the
session — a sign-in issues a new session ID, so the next `setdefault` mints a
fresh token.

---

## Encrypting the payload at rest

Anyone with `redis-cli` access can read a session. That is usually acceptable —
the guidance is to keep identifiers in the session and entities outside it —
but if you must store something sensitive, supply an `Encryptor`:

```python
import os
from cryptography.hazmat.primitives.ciphers.aead import AESGCM

class AesGcmEncryptor:
    """Ten lines, and yours to audit."""

    def __init__(self, key: bytes) -> None:
        self._aes = AESGCM(key)              # 16, 24 or 32 bytes

    def encrypt(self, data: bytes) -> bytes:
        nonce = os.urandom(12)
        return nonce + self._aes.encrypt(nonce, data, None)

    def decrypt(self, data: bytes) -> bytes:
        return self._aes.decrypt(data[:12], data[12:], None)
```

Encryption **wraps** serialization, never the reverse: the coder turns your
value into text, and only then does the encryptor see it. A coder is never
handed ciphertext.

This package ships the seam and not the implementation, deliberately. AES-GCM
in your codebase is a smaller liability for everyone than a cryptographic
primitive maintained here — and it means a key rotation is your decision, on
your schedule.

Two things to know before you turn it on. A record that will not decrypt raises
`SessionStoreError` rather than looking like an empty session, so rotating a key
without a re-encryption pass signs everybody out loudly rather than silently.
And the index descriptor is **not** encrypted — it holds only what your
`descriptor_of` returns, so do not put anything sensitive there.

---

## Migrating from something else

Sessions do not survive the switch, by decision: existing cookies reference
records this store cannot read, so every user signs in again once. Deploy
accordingly.

### From Starlette's `SessionMiddleware`

```python
# before
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key="…")

# after
FastAPIRedis(app).lifespan().sessions()
```

| Their behaviour | Here |
|---|---|
| `request.session["user_id"] = 42` | unchanged — and it now rotates the ID |
| `request.session.clear()` | unchanged — and it now deletes the record too |
| payload capped at ~4 KB by the cookie | no cap; the cookie carries an ID |
| data signed but readable by the client | never leaves the server |
| `max_age` | `session_idle_ttl` plus `session_absolute_ttl` |
| no revocation | `revoke`, `revoke_id`, `revoke_all` |

Handler code does not change. `secret_key` has no equivalent because nothing is
signed: the cookie is an opaque 256-bit identifier.

### From `starsessions`

| Theirs | Here |
|---|---|
| `lifetime=N, rolling=True` | `session_idle_ttl=N` |
| `lifetime=N, rolling=False` | `session_absolute_ttl=N` |
| both behaviours at once | not expressible for them; set both settings here |
| `load_session(request)` | nothing — the load is automatic and eager |
| `regenerate_session_id(request)` | delete the call; writing the identity rotates |
| `get_session_metadata(request)` | `store.list_for_subject()` for the fields |
| `RedisStore(...)` | `FastAPIRedis(app).lifespan().sessions()` |

Their `lifetime` accepts a `timedelta`; so does this store's constructor.

### From `fastapi-users`' `RedisStrategy`

`RedisStrategy` is a token store, not a session store: it maps an opaque token
to a user ID and nothing else. Keep `fastapi-users` for registration, password
reset and OAuth linking — this package does not replace it. Swap the strategy
for the session and read the identity from `request.session` instead of from
the strategy's token.

### From an in-process store

A dict keyed by session ID, or a `TTLCache`. The behaviour you gain is that it
survives a restart and is shared across workers; the behaviour you lose is
none. Delete the store and call `.sessions()`.

---

## Running it in production

**A session store is not a cache, and `maxmemory-policy` must say so.** Under
`allkeys-lru`, `allkeys-lfu` or `allkeys-random`, Redis will evict live
sessions to make room, and every evicted session is a user signed out mid-task
with nothing in any log to explain it. Use `volatile-ttl` or `noeviction`, or
give sessions their own instance or logical database.

This is the single most likely production incident with this feature.

A large tenant's index key is a single key that every login and logout writes,
which makes it a candidate hot spot. `HOTKEYS START METRICS 2 CPU NET SAMPLE 100`
finds it; the `subject_of` seam is what shards it.

---

## Sync endpoints

```python
from redis_fastapi import SyncSessionStoreDep

@app.post("/logout")
def logout(session: SessionDep, store: SyncSessionStoreDep) -> dict:
    store.revoke(session)
    return {"ok": True}
```

---

## Replacing the pieces

Every seam is a keyword argument to `.sessions()`:

```python
FastAPIRedis(app).lifespan().sessions(
    coder=pydantic_model_coder(MySession),   # serialization
    encryptor=AesGcmEncryptor(key),          # encryption at rest
    id_factory=my_id_factory,                # identifier generation (validated)
    key_prefix="myapp",                      # key namespace
    idle_ttl=timedelta(minutes=15),          # the two clocks
    absolute_ttl=timedelta(hours=8),
    descriptor_of=lambda req, s: {"ip": req.client.host},
    principal_keys=["user_id", "role"],      # what rotates the ID
    subject_of=lambda s: s.get("tenant_id"), # what the index is keyed on
    cookie_builder=my_cookie_builder,        # Set-Cookie rendering
    skip=lambda req: req.url.path.startswith("/health"),
)
```

To supply a whole store — another backend, or a test double — pass `store` or
`store_factory`:

```python
FastAPIRedis(app).lifespan().sessions(store=MyPostgresSessionStore(pool))
```

In tests, `dependency_overrides` reaches the middleware as well as your
handlers, so one override covers the whole request:

```python
app.dependency_overrides[get_session_store] = lambda request: fake_store
```

---

## Settings

Every setting is an environment variable prefixed `REDIS_`, so
`session_idle_ttl` is `REDIS_SESSION_IDLE_TTL`.

| Setting | Default | Notes |
|---|---|---|
| `session_cookie_name` | `session` | Matches Starlette and `starsessions` |
| `session_cookie_https_only` | `True` | Adds `Secure`. Turn it off for local HTTP only |
| `session_cookie_same_site` | `lax` | `none` requires `https_only=True` |
| `session_idle_ttl` | `1800` | `0` disables the idle clock |
| `session_absolute_ttl` | `28800` | `0` disables the absolute clock |
| `session_gc_ttl` | `2592000` | Backstop so Redis can always collect an abandoned key |
| `session_refresh_on_load` | `True` | `False`: only a request that *used* the session counts as activity |
| `session_fail_closed` | `False` | Read behaviour when Redis is down |
| `session_always_save` | `False` | Escape route for nested mutation |
| `session_principal_keys` | `["user_id"]` | What a change to rotates the ID |
| `session_events_enabled` | `False` | Opt in to real-time events |
