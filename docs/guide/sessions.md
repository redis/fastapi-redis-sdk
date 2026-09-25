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
[`SessionMiddleware`](https://starlette.dev/middleware/#sessionmiddleware) runs
unchanged.

---

## Rotation is automatic

Writing the identity rotates the session ID. There is no `rotate()` to call and
therefore none to forget, which matters because forgetting it is the one
mistake in this API that is a vulnerability -
[session fixation](https://owasp.org/www-community/attacks/Session_fixation).

The middleware evaluates a **principal** twice per request, once before the
application runs and once at `http.response.start`. If the two differ and the
response status is below 400, it issues a new ID and deletes the old key first.

Watch more than the user ID to get OWASP's
[privilege-change rotation](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#renew-the-session-id-after-any-privilege-level-change):

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

`principal_of` must be **pure, cheap and deterministic** - it runs twice per
request and the two results are compared by value - and it must return a
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

The idle clock is the [expiration](https://redis.io/docs/latest/commands/hexpire/)
of the hash field that holds the payload, and the absolute clock is the
[expiration](https://redis.io/docs/latest/commands/expire/) of the session key
itself. So **Redis enforces both and this library computes neither**. At the
absolute deadline Redis deletes the whole key, however recently the payload
was refreshed. Writing the payload never touches the key's expiration, so no
number of writes can extend the absolute deadline.

The absolute clock is the one that matters against a stolen session. An
[idle timeout](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#idle-timeout)
cannot expire a session an attacker is actively using, because the attacker's
own requests keep refreshing it - OWASP says so directly under
[absolute timeout](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#absolute-timeout).
The absolute deadline is the only clock that fires on a live compromise.

Both accept an `int` number of seconds. The store constructor also accepts a
[`timedelta`](https://docs.python.org/3/library/datetime.html#datetime.timedelta);
the settings and their environment variables take seconds. Setting either to
`0` disables that clock; setting both gives a cookie-only session that the
browser drops when it closes.

The cookie's
[`Max-Age`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#max-agenumber)
is the time left on the absolute clock, and the number comes from Redis rather
than from your process. It does not follow the idle clock: a read-only request
sends no cookie, so an idle-sized cookie would expire while the user is still
active. After an idle timeout the browser keeps a cookie that no longer names a
session, and the next request with it gets an empty session.

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

`SessionDep` is the data - a `dict` you read and write. `SessionStateDep` is
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

## Requiring a valid session

```python
from fastapi import Depends
from redis_fastapi import valid_session

@app.post("/checkout", dependencies=[Depends(valid_session())])
async def checkout(session: SessionDep) -> dict: ...
```

`valid_session()` rejects a request that did not arrive with a session this
application created in an earlier response. A value a client makes up finds no
record in Redis, so it fails, whatever its shape.

It checks nothing else. It does not ask who the session belongs to: an
anonymous session that holds a basket passes. Identity and roles stay with your
own authentication.

The default rejection is a `401`. To build your own, pass `on_reject`. It
receives the request and a reason, and returns the response:

```python
from fastapi import Request
from fastapi.responses import JSONResponse, RedirectResponse, Response
from redis_fastapi import SessionRejection

def start_over(request: Request, reason: SessionRejection) -> Response:
    if reason == "unavailable":
        return JSONResponse({"detail": "Try again shortly"}, status_code=503)
    return RedirectResponse("/?expired=1" if reason == "expired" else "/", status_code=303)

@app.get("/basket", dependencies=[Depends(valid_session(on_reject=start_over))])
async def basket(session: SessionDep) -> dict: ...
```

| Reason | What happened |
|---|---|
| `"missing"` | No usable session: no cookie, a malformed one, or a session that an earlier dependency revoked or emptied |
| `"expired"` | A well-formed cookie that names no live session: expired, revoked on another device, evicted or forged. The server cannot tell these apart. |
| `"unavailable"` | Redis could not be read, and the request continued without a session. Answer `503`, not a sign-in page. |

`on_reject` may be sync or async. It **returns** the response instead of
raising it, so a callback that forgets to return fails with a `500` instead of
letting the request through.

The gate also:

- **marks the session as read**, so the response carries `Vary: Cookie` and
  `Cache-Control: private`;
- **counts malformed cookies**, on every request, gated or not, as the session
  operation metric with `result="malformed"`. Every identifier the store issues
  passes the format check, so a malformed value was never ours. The middleware
  neither logs it nor clears it: the cookie may belong to another application
  on the same domain;
- **refuses a route excluded by `skip`** with `SessionConfigurationError`,
  because such a route can never pass.

### The `WWW-Authenticate` header

By default a rejection sends none, as most frameworks do for cookie sessions:
the browser shows the body, and no password dialog. To send one, set
`challenge` once for the application:

```python
from fastapi.security import APIKeyCookie

# The security scheme your application already declares.
FastAPIRedis(app).lifespan().sessions(challenge=APIKeyCookie(name="session"))

# A fixed value.
FastAPIRedis(app).lifespan().sessions(
    challenge='Cookie realm="shop" form-action="/login" cookie-name=session',
)

# A value that depends on the request or the reason; None sends no header.
FastAPIRedis(app).lifespan().sessions(
    challenge=lambda request, reason: None if reason == "unavailable" else "APIKey",
)
```

`Basic` is refused: every browser answers it with a password dialog, on every
gated page. The header goes on the default rejection only; a response from
`on_reject` is your own.

### Requiring a recently issued session

```python
@app.post(
    "/account/email",
    dependencies=[
        Depends(current_user),                      # your authentication
        Depends(valid_session(issued_within=600)),  # the session ID is at most 10 minutes old
    ],
)
async def change_email() -> dict: ...

@app.post("/confirm-password")
async def confirm(
    form: PasswordForm, state: SessionStateDep, store: SessionStoreDep
) -> dict:
    if not await verify(form.password):
        raise HTTPException(401)
    await store.reauthenticate(state)   # issues a new session ID
    return {"ok": True}
```

`issued_within` asks one more question: was this session's ID issued within the
last N seconds? A session gets a new ID when it is created, when the principal
changes - a sign-in, a role change - and when you call `store.rotate()` or
`store.reauthenticate()`. Reads and writes keep the ID. The age comes from the
session key's TTL in Redis, so a container with a wrong clock cannot make an old
session look recent.

This is the building block for a step-up before a sensitive action, as
[OWASP ASVS V7.5](https://github.com/OWASP/ASVS) asks: your route checks the
password and calls `reauthenticate()`, and for the next `issued_within` seconds
the sensitive routes pass.

!!! warning "`issued_within` is not an authentication check"
    - **A new anonymous session is recent.** Pair it with your own
      authentication dependency, as in the example.
    - **Every new ID counts.** If your principal includes something a user can
      change without a password - an active tenant, say - changing it makes the
      session recent.
    - **A handler that calls `store.rotate()` makes the session recent too.**

A session that is too old gets the reason `"stale"`. The default response is:

```json
401
{"detail": "A recently issued session is required",
 "error": "stale_session",
 "issued_within": 600}
```

Branch on `error` in the client: open a password prompt, then retry. Every
response from such a route, passed or rejected, says `Cache-Control: no-store`,
so no cache keeps it, the browser's included. For a "confirm your password to
continue" banner, `store.session_age(state)` returns the age in seconds.

Two side effects to know:

- **A step-up changes the cookie**, because `reauthenticate()` rotates the
  session. A form open in another tab, carrying a CSRF token from the old
  session, fails after it.
- **Lowering `session_absolute_ttl` makes older sessions stale** until they
  expire. The age is the configured lifetime minus the time left, and a session
  created under the longer lifetime has more time left than the new one allows.
  It is reported as `"stale"`, never as recent.

---

## When Redis is unreachable

The default is asymmetric by design:

- **A failed read yields an empty session.** The user looks anonymous, your own
  authorization dependency finds no user, and a protected route stays protected
  because it never depended on the read succeeding.
- **A failed write raises `SessionStoreError`.** Losing a login or a rotation is
  the worst outcome here and must never be silent.

Set `session_fail_closed=True` to turn the failed read into an error too, for a
deployment that prefers a
[503](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Status/503)
to an anonymous page. Writes raise either way.

---

## Nested changes are invisible

```python
session["prefs"]["theme"] = "dark"     # NOT saved
session["prefs"] = {**session["prefs"], "theme": "dark"}   # saved
```

No `dict` subclass in any language can see a change inside a value it holds.
Reassign the top-level key, or set `session_always_save=True` to write on every
request that touched the session.

!!! warning "`session_always_save` writes on every request that *read* the session"
    "Touched" includes a plain read - `session.get("user_id")` is enough - so
    with the setting on, a route that only inspects the session writes it back
    on every request. Prefer reassigning the top-level key, and reach for the
    setting only where you cannot.

    An **empty** session is exempt: reading one does not create it. Without
    that exemption every anonymous visitor to a session-touching route - every
    crawler, health check and preflight - would be minted an identifier, a
    Redis key and a cookie. Nothing the setting exists for is lost, because a
    nested mutation needs a top-level key already holding the nested value.

---

## Real-time session events (optional)

Close a WebSocket the moment a session ends, instead of finding out on the next
HTTP request:

```python
from redis_fastapi import Cause, SessionEvents

events = SessionEvents(redis, key_prefix="redis:fastapi")

@events.on_session_end
async def _(session_id: str, cause: Cause) -> None:   # "idle" or "absolute"
    await close_sockets_for(session_id)

await events.start()
```

`Cause` has exactly those two members, so an exhaustive `match` over it stays
exhaustive. A revocation is not among them: it is a `DEL`, and Redis publishes
the same `del` event when an idle expiry empties the key, so the two cannot be
told apart. Sign a user out through `revoke()` and you already know it
happened - the event stream is for the deaths nobody asked for. `Handler` is
exported too, for annotating the callable you register.

This uses
[keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/),
which work on every supported Redis version, and it needs the server
configured for them with
[`CONFIG SET`](https://redis.io/docs/latest/commands/config-set/):

```
CONFIG SET notify-keyspace-events Ehx
```

`E` enables the `__keyevent@` channels, `h` the hash events and `x` the
expiry events. An idle timeout arrives as `hexpired` - the payload field
expired - and an absolute timeout as `expired` - the key expired. `A` covers
both `h` and `x`, so a server already set to `KEA` needs nothing more.
This library will never set the option for you: it is server-wide and affects
every other application on the instance.

!!! danger "The callback is best-effort, and silence is a possible outcome"
    On a server without the flags, or one where
    [`CONFIG GET`](https://redis.io/docs/latest/commands/config-get/) is
    unavailable - which is common on managed Redis - `events.tier` is `"none"`,
    one warning is logged at startup, and **your handlers never run**. Startup
    still succeeds and every request still works.

    A revocation handler that never fires looks exactly like one that works. If
    prompt closure matters, check `events.tier` and add a periodic sweep as
    well. [Redis Pub/Sub](https://redis.io/docs/latest/develop/pubsub/) is
    fire-and-forget: events sent while no subscriber is connected are lost, and
    an [expiry event](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/#timing-of-expired-events)
    fires when Redis removes the field or the key rather than when the
    deadline passed.

    Nothing else depends on this. Expiry, revocation and the index all work
    identically with events switched off.

On a [cluster](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/#events-in-a-cluster),
keyspace events are node-local and are not broadcast, so seeing every event
needs one subscriber per node.

---

## Caching an endpoint that uses the session

A cached response is stored once and served many times. A session-dependent
response is different for every user. Without handling this in a special way
the first user's body could end up being served to the second.

Using the `vary_on_session` parameter you can control three distinct situations:

```python
cache(ttl=300, vary_on_session=True)    # depends on who is asking
cache(ttl=300, vary_on_session=False)   # reads the session but the body is the same for everyone
cache(ttl=300)                          # unknown, reading the session is potentially dangerous
```

### `vary_on_session=True` - cache per user

Use it when the response contains the user's own data.

```python
@app.get(
    "/me",
    dependencies=[Depends(cache(ttl=300, vary_on_session=True))],
)
async def me(session: SessionDep) -> dict:
    return {"user_id": session["user_id"], "cart": session.get("cart", [])}
```

Each user gets their own cache entry and their own hits - Alice's second
request is a `HIT` on Alice's copy. The key is built from the **subject**, not
the session ID, so it survives a rotation (a privilege change does not throw
the entry away) and is shared across that user's devices.

The response carries
[`Cache-Control: private, max-age=300`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control#private).
The `private` is not optional and is added for you: our entry is per user, so a
CDN told it may keep one copy for everyone would recreate the leak one hop
further out.

### `vary_on_session=False` - reads the session, but cache is shared

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

One shared entry serves every signed-in user, and the
[`Vary: Cookie`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Vary)
the session middleware would otherwise add is suppressed - keeping it would
force a CDN to store one identical copy per user and destroy the hit rate this
declaration exists to protect.

**This is an assertion, and the library takes your word for it.** If the body
does depend on the session, you have re-enabled the leak deliberately.

With [`valid_session()`](#requiring-a-valid-session) as the gate, two things
change:

```python
@app.get(
    "/members/catalogue",
    dependencies=[
        Depends(valid_session()),                       # first
        Depends(cache(ttl=300, vary_on_session=False)), # then the cache
    ],
)
async def members_catalogue() -> list[dict]:
    return await load_products()
```

- **The gate goes first.** FastAPI resolves `dependencies=[...]` in order, and
  a cache hit ends the resolution, so in the other order a hit would be served
  before the gate runs - to anyone. `valid_session()` detects that order and
  raises `SessionConfigurationError` on the first request, before anything is
  stored.
- **The response says `private`.** Redis still keeps one shared entry, because
  the gate runs before it on every request. A CDN cannot check a session, so it
  would serve that entry to anyone; `private` keeps it out.

A hand-written gate like `require_user` gets neither. List it before `cache()`
yourself, and pass `cache(..., private=True)` when the route sits behind a CDN
or another shared cache.

### Saying nothing

If the endpoint never touches the session, say nothing - there is nothing to
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

Note that these last two are the *same* declaration - you write nothing in both
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
| Gated by `valid_session()`, body identical | `vary_on_session=False` | one shared entry | `private, max-age=N`, no `Vary` |
| Gated by `valid_session(issued_within=...)` | any | as declared | `no-store` |
| Never touches the session | *(nothing)* | one shared entry | `max-age=N` |
| Touches it, nothing declared | *(nothing)* | **not cached** | `private, no-store` + `Vary: Cookie` |

The rule underneath every row: **what a response tells other caches they may do
is never more permissive than what this library does itself.** If we key per
user, or serve our entry only to callers who pass the gate, we say
[`private`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching#private_caches).
If we refuse to store, we say
[`no-store`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control#no-store).

### A session route with no caching on it

Reading the session on an uncached route emits `Cache-Control: private` and
`Vary: Cookie` on its own, since nothing else will. Where `cache()` is present
it owns the header outright - one writer, so you never see two contradictory
`Cache-Control` values on one response.

For a sensitive page - an order history, a bank statement - set
`Cache-Control: no-store` in the handler. The middleware then adds nothing, so
the browser does not keep the page and cannot show it again from history after
a sign-out:

```python
@app.get("/orders")
async def orders(session: SessionDep, response: Response) -> dict:
    response.headers["Cache-Control"] = "no-store"
    return {"orders": await db.orders_for(session["user_id"])}
```

On a cached route, pass `cache(..., no_store=True)` instead: the entry is still
kept in Redis, and every miss and hit says `no-store`.

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
at which a cookie can be set - which means no rotation, no save, and no
idle-clock refresh for the life of the connection. Half a session object,
silently read-only, invites exactly the bug the rest of this design works to
prevent: an application writes to it, sees no error, and loses the write.

**What to do instead.** Authenticate during the
[HTTP handshake](https://fastapi.tiangolo.com/advanced/websockets/), where the
cookie *is* available, and pass what the socket needs into the handler:

```python
@app.websocket("/ws")
async def ws(websocket: WebSocket) -> None:
    # The handshake is an HTTP request, so the cookie arrives with it.
    raw = websocket.cookies.get("session")
    store = await get_session_store(websocket)      # type: ignore[arg-type]
    loaded = await store.load(raw) if raw and store.is_valid_id(raw) else None
    if loaded is None:
        await websocket.close(code=1008)   # policy violation, RFC 6455 §7.4.1
        return

    user_id = loaded.record.data.get("user_id")
    await websocket.accept()
    ...
```

Load once at accept time and hold the identity for the connection. If you need
the socket to close when the session ends, pair it with
[real-time session events](#real-time-session-events-optional-redis-88) - that
is the case those exist for.

---

## CSRF: what a cookie session reintroduces

A [cookie](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Cookies) is
attached by the browser to **every** request to your origin, including one
triggered by a form on somebody else's page. That is what makes a session cookie
convenient and it is also the whole of
[CSRF](https://owasp.org/www-community/attacks/csrf): an attacker cannot read
your session, but they can make the browser spend it.

A bearer token in an
[`Authorization`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Authorization)
header does not have this problem, because nothing attaches it automatically.
Moving to cookies gets you revocation, rotation and server-side expiry, and it
hands this back.

**[`SameSite=Lax`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#samesitesamesite-value)
is the default here and it is most of the remedy.** The browser withholds the
cookie on cross-site `POST`, `PUT`, `PATCH` and `DELETE`. It does *not* withhold
it on a cross-site top-level `GET`, so:

- **Never change state in a `GET`.** A `GET /account/delete` is exploitable
  under `Lax` and no cookie attribute will save it.
- **Add a CSRF token for anything a browser form can reach.** `SameSite` is a
  [defence in depth, not a substitute](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#limitations-of-samesite)
  - it is unenforced on some older browsers, and `Lax` has a
  [two-minute exemption window](https://www.chromium.org/updates/same-site/faq/)
  for top-level POSTs in some Chromium versions.
- **`SameSite=Strict`** closes the top-level `GET` hole too, at the cost of the
  cookie being withheld when a user follows a link into your site from
  anywhere else - including their own email.

The session is the natural place to keep the token - this is OWASP's
[synchronizer token pattern](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html#synchronizer-token-pattern):

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

[`secrets.compare_digest`](https://docs.python.org/3/library/secrets.html#secrets.compare_digest)
rather than `==`, and
[`secrets.token_urlsafe`](https://docs.python.org/3/library/secrets.html#secrets.token_urlsafe)
for the token itself. The token rotates with the session - a sign-in issues a
new session ID, so the next `setdefault` mints a fresh token.

---

## Encrypting the payload at rest

Anyone with [`redis-cli`](https://redis.io/docs/latest/develop/tools/cli/)
access can read a session. That is usually acceptable - the guidance is to keep
identifiers in the session and entities outside it - but if you must store
something sensitive, supply an `Encryptor`:

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

This package ships the seam and not the implementation, deliberately.
[AES-GCM](https://csrc.nist.gov/pubs/sp/800/38/d/final), via
[`AESGCM`](https://cryptography.io/en/latest/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM),
in your codebase is a smaller liability for everyone than a cryptographic
primitive maintained here - and it means a key rotation is your decision, on
your schedule.

Two things to know before you turn it on. A record that will not decrypt raises
`SessionStoreError` rather than looking like an empty session, so rotating a key
without a re-encryption pass signs everybody out loudly rather than silently.
And the index descriptor is **not** encrypted - it holds only what your
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
| `request.session["user_id"] = 42` | unchanged - and it now rotates the ID |
| `request.session.clear()` | unchanged - and it now deletes the record too |
| payload capped at ~4 KB by the cookie | no cap; the cookie carries an ID |
| data signed but readable by the client | never leaves the server |
| `max_age` | `session_idle_ttl` plus `session_absolute_ttl` |
| no revocation | `revoke`, `revoke_id`, `revoke_all` |

Handler code does not change. `secret_key` has no equivalent because nothing is
signed: the cookie is an opaque 256-bit identifier, well past OWASP's
[64-bit entropy floor](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#session-id-entropy).

### From [`starsessions`](https://github.com/alex-oleshkevich/starsessions)

| Theirs | Here |
|---|---|
| `lifetime=N, rolling=True` | `session_idle_ttl=N` |
| `lifetime=N, rolling=False` | `session_absolute_ttl=N` |
| both behaviours at once | not expressible for them; set both settings here |
| `load_session(request)` | nothing - the load is automatic and eager |
| `regenerate_session_id(request)` | delete the call; writing the identity rotates |
| `get_session_metadata(request)` | `store.list_for_subject()` for the fields |
| `RedisStore(...)` | `FastAPIRedis(app).lifespan().sessions()` |

Their `lifetime` accepts a `timedelta`; so does this store's constructor.

### From `fastapi-users`' `RedisStrategy`

[`RedisStrategy`](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/strategies/redis/)
is a token store, not a session store: it maps an opaque token
to a user ID and nothing else. Keep `fastapi-users` for registration, password
reset and OAuth linking - this package does not replace it. Swap the strategy
for the session and read the identity from `request.session` instead of from
the strategy's token.

### From an in-process store

A dict keyed by session ID, or a
[`TTLCache`](https://cachetools.readthedocs.io/en/latest/#cachetools.TTLCache). The behaviour you gain is that it
survives a restart and is shared across workers; the behaviour you lose is
none. Delete the store and call `.sessions()`.

---

## Running it in production

**A session store is not a cache, and
[`maxmemory-policy`](https://redis.io/docs/latest/develop/reference/eviction/#eviction-policies)
must say so.** Under `allkeys-lru`, `allkeys-lfu` or `allkeys-random`, Redis
will evict live sessions to make room, and every evicted session is a user
signed out mid-task with nothing in any log to explain it. Use `volatile-ttl` or
`noeviction`, or give sessions their own instance or
[logical database](https://redis.io/docs/latest/commands/select/).

This is the single most likely production incident with this feature.

A large tenant's index key is a single key that every login and logout writes,
which makes it a candidate hot spot.
[`HOTKEYS START METRICS 2 CPU NET SAMPLE 100`](https://redis.io/docs/latest/commands/hotkeys-start/)
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

To supply the store object yourself - one you built and configured, or a test
double - pass `store` or `store_factory`:

```python
store = RedisSessionStore(redis, key_prefix="tenant-a", absolute_ttl=3600)
FastAPIRedis(app).lifespan().sessions(store=store)
```

In tests,
[`dependency_overrides`](https://fastapi.tiangolo.com/advanced/testing-dependencies/)
reaches the middleware as well as your handlers, so one override covers the whole request:

```python
app.dependency_overrides[get_session_store] = lambda request: fake_store
```

---

## Settings

Every setting is an environment variable prefixed `REDIS_`, so
`session_idle_ttl` is `REDIS_SESSION_IDLE_TTL`. See
[Configuration](configuration.md) for how these are loaded.

| Setting | Default | Notes |
|---|---|---|
| `session_cookie_name` | `session` | Matches Starlette and `starsessions` |
| `session_cookie_https_only` | `True` | Adds [`Secure`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#secure). Turn it off for local HTTP only |
| `session_cookie_same_site` | `lax` | [`none`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#samesitesamesite-value) requires `https_only=True` |
| `session_idle_ttl` | `1800` | `0` disables the idle clock |
| `session_absolute_ttl` | `28800` | `0` disables the absolute clock |
| `session_gc_ttl` | `2592000` | Backstop so Redis can always collect an abandoned key |
| `session_refresh_on_load` | `True` | `False`: only a request that *used* the session counts as activity |
| `session_fail_closed` | `False` | Read behaviour when Redis is down |
| `session_always_save` | `False` | Escape route for nested mutation. Writes on every request that **read** the session; an empty session is exempt |
| `session_principal_keys` | `["user_id"]` | What a change to rotates the ID. Comma-separated in the environment: `REDIS_SESSION_PRINCIPAL_KEYS=user_id,role` |
| `session_events_enabled` | `False` | Opt in to real-time events |

---

## References

The standards and specifications this design follows:

- [OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html)
  - identifier entropy, rotation on privilege change, and the two timeouts
- [OWASP Cross-Site Request Forgery Prevention Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Cross-Site_Request_Forgery_Prevention_Cheat_Sheet.html)
  - the token patterns and the limits of `SameSite`
- [OWASP ASVS, chapter 3: Session Management](https://owasp.org/www-project-application-security-verification-standard/)
  - the verifiable requirements behind the cheat sheets
- [RFC 6265](https://datatracker.ietf.org/doc/html/rfc6265) and
  [RFC 6265bis](https://datatracker.ietf.org/doc/html/draft-ietf-httpbis-rfc6265bis)
  - HTTP cookies, and the draft that defines `SameSite`
- [MDN: HTTP caching](https://developer.mozilla.org/en-US/docs/Web/HTTP/Guides/Caching)
  and [`Cache-Control`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control)
  - what `private`, `no-store` and `Vary` mean to a shared cache
- [Redis key eviction](https://redis.io/docs/latest/develop/reference/eviction/)
  - why `maxmemory-policy` decides whether sessions survive
- [Redis keyspace notifications](https://redis.io/docs/latest/develop/pubsub/keyspace-notifications/)
  - the delivery guarantees behind `SessionEvents`
- [Redis key expiration](https://redis.io/docs/latest/commands/expire/) and
  [hash field expiration](https://redis.io/docs/latest/commands/hexpire/)
  - the mechanisms the two clocks are built on
