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

Both settings accept an `int` or a `timedelta`. Setting either to `0` disables
that clock; setting both gives a cookie-only session that the browser drops
when it closes.

The cookie's `max-age` is `min(idle, absolute remaining)`, and both numbers come
from Redis rather than from your process — so a container with a skewed clock
cannot produce a cookie that outlives its record and signs a user out with no
explanation.

---

## Signing out, everywhere

```python
from redis_fastapi import SessionStoreDep

@app.post("/logout")
async def logout(session: SessionDep, store: SessionStoreDep) -> dict:
    await store.revoke(session)
    return {"ok": True}

@app.post("/logout-everywhere")
async def logout_all(session: SessionDep, store: SessionStoreDep) -> dict:
    return {"ended": await store.revoke_all(str(session["user_id"]))}

@app.get("/devices")
async def devices(session: SessionDep, store: SessionStoreDep) -> list[dict]:
    current = store.session_id(session)
    return [
        {"id": info.session_id, "this_device": info.session_id == current,
         "last_seen": info.last_access}
        for info in await store.list_for_subject(str(session["user_id"]))
    ]
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
