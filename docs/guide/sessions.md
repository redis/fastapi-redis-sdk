# Sessions

The SDK also provides server-side sessions for FastAPI. The cookie carries an 
opaque identifier and nothing else; the session lives in Redis, where the server
enforces both of its deadlines.

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis, SessionDep

app = FastAPI()
FastAPIRedis(app).lifespan().sessions()

@app.post("/login")
async def login(session: SessionDep) -> dict:
    session["user_id"] = 42  # rotates the session ID after the endpoint completes
    return {"ok": True}

@app.get("/me")
async def me(session: SessionDep) -> dict:
    return {"user_id": session.get("user_id")}
```

`request.session` works too, so code written against Starlette's signed-cookie
[`SessionMiddleware`](https://starlette.dev/middleware/#sessionmiddleware) runs
unchanged.

### Keys in Redis

A session is one Redis hash. The SDK writes no other key for it:

| Stored for  | Key                                                 |
|-------------|-----------------------------------------------------|
| One session | `rf:s:Qm3xLk9vT2pB7wRzYhN1dAe8Uc5JfG0sXoKi4VnMb6Z`  |

The session key ends in the session ID, the same opaque value the cookie
carries. A default ID is 43 characters: 32 random bytes, URL-safe base64.
Pass `key_prefix=` to replace `rf` for sessions alone - see
[Replacing the pieces](#replacing-the-pieces).

### Fields in the hash

The hash holds system fields, which start with `s:`, and one `a:` field for
each value in the session:

| Field       | Holds                                                                        |
|-------------|------------------------------------------------------------------------------|
| `s:created` | When the session was created, in Unix seconds. Its TTL is the absolute clock |
| `s:seen`    | The last access, in Unix seconds                                             |
| `s:sub`     | The subject, by default the session's `user_id`. Absent for an anonymous session |
| `s:desc`    | The [descriptor](#the-descriptor). Absent without `descriptor_of`            |
| `a:<path>`  | One leaf value of the session, such as `42` or `"Sofia"`. Never the whole session |

The store splits a nested value into one field for each leaf. The field name
is the path to the leaf:

```python
session["user_id"] = 42
session["user"] = {"address": {"city": "Sofia"}}
session["bikes"] = ["road", "gravel"]
```

```
a:user_id             42
a:user.address.city   "Sofia"
a:bikes[0]            "road"
a:bikes[1]            "gravel"
s:sub                 "42"
s:created             1767225600.0
s:seen                1767225660.0
```

- There is no field that holds the entire session.
- Each field holds one value, written as JSON text: a number as `42`, a
  string as `"Sofia"` with its quotes, `true`, `false`, or `null`. So `7` and
  `"7"` are stored differently and each comes back as it went in. A value
  with no JSON form fails the save with `SessionStoreError`.
- With an `Encryptor`, each `a:` field holds its value encrypted, not as
  readable JSON text. See
  [Encrypting the session at rest](#encrypting-the-session-at-rest).
- A tuple comes back as a list. A dict key that is not a string comes back as
  a string, as in JSON.
- An empty list or an empty dict is one field: `a:tags` holds `[]`.
- In a dict key, the store writes `%`, `.`, `[` and `]` as `%25`, `%2E`, `%5B`
  and `%5D`, and reverses this on the load.
- A save writes only the fields that changed.
- System fields never appear in `request.session`.

---

## Session change

Writing to the session dictionary remains an in-memory change until the endpoint 
completes its work. The middleware loads the session before your endpoint runs.
It saves, rotates or deletes the session after the endpoint returns, just before
it sends the response headers. While your endpoint runs:

- Reads in the same request see your writes.
- Redis still holds the old session, and `store.session_id(state)` returns the
  old ID, or `None` for a new visitor.
- A response with status 400 or more that changes the principal saves nothing.
  A sign-in followed by `raise HTTPException(403)` does not sign the user in.
- If the save fails, the client gets a 500. Your endpoint's own side effects,
  such as a database commit, have already happened.

To change the session now, call the store. Each `SessionStoreDep` call goes to
Redis at once:

```python
@app.post("/login")
async def login(
    session: SessionDep, state: SessionStateDep, store: SessionStoreDep
) -> dict:
    session["user_id"] = 42
    new_id = await store.rotate(state, subject="42")  # rotates now
    return {"ok": True}
```

The middleware sees the new ID and only sets the cookie. It does not rotate a
second time. You give up one protection: an error response after `rotate()`
no longer cancels the sign-in.

Work that must wait until the session is saved belongs in a
[background task](https://fastapi.tiangolo.com/tutorial/background-tasks/).
It runs after the response is sent.

The [session request lifecycle](architecture.md#session-request-lifecycle)
shows the full sequence.

---

## Automatic rotation

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

## Session clocks

| Setting                | Default | What it bounds                                              |
|------------------------|---------|-------------------------------------------------------------|
| `session_idle_ttl`     | 30 min  | Time since the last request carrying the cookie             |
| `session_absolute_ttl` | 8 h     | Time since the session was created, however active the user |

The idle clock is the [TTL](https://redis.io/docs/latest/commands/expire/) of
the session key. Each load restarts it, and so does each write. The absolute
clock is the [TTL](https://redis.io/docs/latest/commands/hexpire/) of the field
`s:created`. The store writes that field once, when it creates the session,
and never again, so no number of writes can extend the absolute deadline.

The library sets each TTL, and **Redis enforces both**: no code in this
library reads a timestamp to decide whether a session has expired. At the
absolute deadline Redis removes `s:created`. A hash without `s:created` is not
a session: the next load returns no session and deletes the key.

A rotation creates a new session key, so it starts a new absolute clock. A
rotation follows a sign-in or a privilege change, and the deadline runs from
that moment.

!!! warning "An upgrade signs every user out once"
    Version 0.10.x of the SDK introduced a breaking change on the way sessions
    are  stored. Older versions do not store an `s:created` field, so the
    session does not load: the first request after the upgrade could get an empty
    session, and the user would need to sign in again.

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

## Attribute TTL

One attribute of a session can expire before the session does. Pass `ttl`, in
seconds, to `set()`:

```python
from fastapi import Request

@app.post("/settings")
async def save_settings(request: Request) -> dict:
    request.session.set("flash", "Settings saved.", ttl=60)
    return {"ok": True}
```

After 60 seconds Redis removes the attribute, and the next request does not
see it. `SessionDep` is the same object, so `session.set(...)` works too.

```python
session.set("flash", "Settings saved.", ttl=60)   # TTL of 60 seconds
session.set("flash", "Profile saved.")            # no TTL
session["flash"] = "Profile saved."               # no TTL
session.persist("flash")                          # remove the TTL, keep the value
```

The rules:

- **The TTL starts at the save**, at the end of the request, not at the call.
- **The TTL does not slide.** A read does not extend it, and neither does a
  request.
- **A changed value loses the TTL.** A write of a different value without
  `ttl` removes the TTL. The store does not carry it over. To change the value
  and keep the expiry, pass the time left as `ttl`; the application must keep
  the deadline itself to know it.
- **An equal value keeps the TTL.** A write of the value the session already
  holds changes nothing in Redis, so the TTL stays. A write of an equal value
  with `ttl` starts the TTL again.
- **`persist(name)` removes the TTL and keeps the value.** It does nothing for
  an attribute that is absent or has no TTL.
- **The last call for a name decides.** `set()` with `ttl` and then
  `session[name] = ...` in one request leaves no TTL.
- **A rotation keeps the time left** for an attribute that the request did
  not change. Redis reports the time left in whole seconds and rounds up, so
  a rotation can move the expiry later by up to one second. An attribute
  whose value changed in the same request loses its TTL, as in any save.
- **`ttl` must be a positive `int`.** Any other value raises `ValueError` at
  the call.
- **A searchable attribute cannot have a TTL.** The save raises
  `SessionConfigurationError` and writes nothing. See
  [Searchable attributes](#searchable-attributes).

!!! warning "A nested attribute can expire in parts"
    The TTL is set on each field of the attribute, and a nested attribute has
    one field for each leaf. A change to one leaf without `ttl` removes the
    TTL from that field only:

    ```python
    session.set("draft", {"title": "Hello", "body": "..."}, ttl=600)
    # a later request
    session["draft"] = {**session["draft"], "title": "Hi"}
    ```

    Now `draft.title` has no TTL and `draft.body` expires on time, which
    leaves `{"title": "Hi"}`. The store does not keep the fields of one
    attribute in step. The application is responsible for the consistency of
    an attribute that has a TTL: write it with `ttl` each time, or keep such
    an attribute flat.

---

## Signing out

```python
from redis_fastapi import SessionStateDep, SessionStoreDep

@app.post("/logout")
async def logout(state: SessionStateDep, store: SessionStoreDep) -> dict:
    await store.revoke(state)
    return {"ok": True}

@app.post("/logout-everywhere")
async def logout_all(state: SessionStateDep, store: SessionStoreDep) -> dict:
    if state.subject is None:              # an anonymous session
        return {"ended": 0}
    return {"ended": await store.revoke_all(state.subject)}

@app.get("/devices")
async def devices(state: SessionStateDep, store: SessionStoreDep) -> list[dict]:
    if state.subject is None:
        return []
    current = store.session_id(state)
    return [
        {"id": info.session_id, "this_device": info.session_id == current,
         "last_seen": info.last_access}
        for info in await store.list_for_subject(state.subject)
    ]
```

`SessionDep` is the data - a `dict` you read and write. `SessionStateDep` is
the handle: the identifier, the subject and the creation time. Operations that
act on the *session* rather than its contents take the handle.

Take the subject from `state.subject`, not from the session's data. It is
the value the session is stored under: what `subject_of` returned when the
session was loaded, or the first principal key by default. A handler that
builds it by hand, such as `str(session["user_id"])`, asks about the wrong
subject as soon as the application sets its own `subject_of`.
`state.subject` is `None` for an anonymous session.

Simply emptying the session signs the user out too, and is often all you need:

```python
@app.post("/logout")
async def logout(session: SessionDep) -> dict:
    session.clear()          # key deleted, cookie cleared
    return {"ok": True}
```

`revoke_id` requires the subject and refuses an ID whose session belongs to
another subject, so a handler taking an ID from a request cannot end a
stranger's session. It reads the subject from the session's own hash.

`revoke_all` and `list_for_subject` are searches. They need the Redis Query
Engine - see [Searching sessions](#searching-sessions).

### The descriptor

A device listing needs something to show for each session: an IP address, a
user agent, a device name. Supply it with `descriptor_of`:

```python
FastAPIRedis(app).lifespan().sessions(
    descriptor_of=lambda req, s: {"ip": req.client.host,
                                  "agent": req.headers.get("user-agent")},
)
```

The store keeps the result in the session hash, in the field `s:desc`. It is
not part of `request.session`. Each list method returns it as
`SessionInfo.descriptor`. The middleware calls `descriptor_of` at each save,
and the store writes the field only when the value changed.

The descriptor is stored as plaintext, with or without an `Encryptor`. Do not
put sensitive data in it.

---

## Searching sessions

The store can find sessions by subject, by last access, and by an attribute
that you declare. Each search reads an index that the
[Redis Query Engine](https://redis.io/docs/latest/develop/ai/search-and-query/)
keeps over the session hashes.

!!! warning "Search needs the Query Engine"
    The Query Engine is part of Redis 8.0 and later. An earlier server has it
    only as an added module, as in Redis Stack. On Redis 7.4 without the Query
    Engine, `list_for_subject`, `count_for_subject` and `revoke_all` do not
    work, and neither does any other search method: each raises
    `SessionSearchUnavailableError`.

    The store logs one warning when it finds no Query Engine. Loading,
    saving, rotating, `revoke` and `revoke_id` work on every supported server.

### Built-in searches

These need no setup. They read an index that holds system fields only, so no
value your application writes can hide a session from them.

| Method                                         | Returns                                                                          |
|------------------------------------------------|----------------------------------------------------------------------------------|
| `list_for_subject(subject)`                    | The sessions of one subject, newest access first. At most 1,000                  |
| `count_for_subject(subject)`                   | How many sessions one subject has                                                |
| `revoke_all(subject)`                          | Ends every session of one subject. Returns how many it ended                     |
| `list_active(within, *, limit=100, offset=0)`  | One page of the sessions used in the last `within` seconds, newest access first  |
| `revoke_idle(older_than)`                      | Ends every session not used for `older_than` seconds. Returns how many it ended  |

`list_for_subject` returns at most 1,000 sessions. For a subject with more,
`count_for_subject` gives the full number.

`count_for_subject` is the check for a cap on concurrent sessions:

```python
from fastapi import HTTPException

MAX_SESSIONS = 5

@app.post("/login")
async def login(session: SessionDep, store: SessionStoreDep) -> dict:
    user_id = 42
    if await store.count_for_subject(str(user_id)) >= MAX_SESSIONS:
        raise HTTPException(409, "Too many sessions")
    session["user_id"] = user_id
    return {"ok": True}
```

This is the one place that builds the subject by hand. The user is not signed
in yet, so the session has no subject to read from `state.subject`. The value
must be the one `subject_of` will return for this user: `str(user_id)` with
the default, or whatever your own `subject_of` gives.

It never returns a number it could not establish. When the count fails it
raises, because `0` would let every sign-in through.

The idle timeout already ends an unused session. `revoke_idle` is for a cutoff
shorter than that:

```python
ended = await store.revoke_idle(15 * 60)       # not used for 15 minutes
recent = await store.list_active(5 * 60)       # used in the last 5 minutes
```

An anonymous session has no subject, so no search by subject finds it.
`list_active` and `revoke_idle` cover every session.

### Searchable attributes

To search by an attribute of your own, declare it at setup. `searchable` maps
the path of a leaf to a type:

```python
FastAPIRedis(app).lifespan().sessions(
    searchable={
        "device_id": "tag",
        "country": "tag",
        "size": "numeric",
        "name": "text",
        "location": "geo",
    },
)
```

A path names a leaf as the [hash](#fields-in-the-hash) does: `cart.total` is
`session["cart"]["total"]`.

| Type      | The attribute must hold                  | Filter                               | Matches                                         |
|-----------|------------------------------------------|--------------------------------------|-------------------------------------------------|
| `tag`     | `str` or `bool`                          | `equals=value`                       | The exact value. Case matters                   |
| `numeric` | `int` or `float`, not `bool`             | `between=(low, high)`                | Both limits included. `None` leaves a side open |
| `text`    | `str`                                    | `text="..."`                         | Words, in [Redis query syntax](#text-search)    |
| `geo`     | `[lon, lat]`, two numbers                | `near=(lon, lat, radius_in_meters)`  | Points within the radius                        |

`store.find` returns one page of sessions, newest access first, and
`store.count` returns how many match:

```python
await store.find("device_id", equals="d-91")
await store.find("size", between=(100_000, None))
await store.find("name", text="petrova")
await store.find("location", near=(23.32, 42.69, 5000))   # lon, lat, meters
await store.count("device_id", equals="d-91")
```

Each call takes one attribute and one filter. The store has no query over two
attributes.

The store checks each declared attribute at the save:

- A declared attribute can be absent from a session.
- A value of another type, `None` included, raises
  `SessionAttributeTypeError`. The store writes nothing, and the request fails.
- A declared attribute cannot have a [TTL](#attribute-ttl).
- `searchable` cannot be combined with an `Encryptor`. Setup raises
  `SessionConfigurationError`.

A session written before you declared an attribute is not checked until its
next save. If it holds a value the index cannot read, `find` and `count` miss
it. The built-in searches still find it.

!!! note "Changing the declaration"
    The store names the index after the declaration. A changed declaration
    creates a new index at startup and drops the old one. Redis fills the new
    index in the background, and until it finishes `find` and `count` see
    only a part of the sessions. Deploy a changed declaration to every worker
    at once: workers with different declarations drop each other's index.

#### Recipes

**A device ID.** Find the sessions of one device, and end them:

```python
session["device_id"] = "d-91"                      # at sign-in

for info in await store.find("device_id", equals="d-91"):
    await store.delete(info.session_id)
```

**A session size.** The store does not measure a session. Compute the size in
the application and store it as a number:

```python
import json

session["cart"] = cart
session["size"] = len(json.dumps(session.raw()))   # characters of JSON

large = await store.count("size", between=(100_000, None))
```

**A country code.** A `tag` matches the exact value, so store one form:

```python
session["country"] = "BG"

await store.count("country", equals="BG")
```

**A location.** Store the point as `[lon, lat]`, longitude first:

```python
session["location"] = [23.32, 42.69]

nearby = await store.find("location", near=(23.32, 42.69, 5000))
```

**A display name.** A `text` attribute matches words, not the whole value:

```python
session["name"] = "Ana Petrova"

await store.find("name", text="petrova")
```

### Text search

**The text of a `text` filter is Redis query syntax.** The store sends it to
Redis unchanged. It does not escape, split or limit it.

| Text              | Matches                                                     |
|-------------------|-------------------------------------------------------------|
| `petrova`         | The word                                                    |
| `ana petrova`     | Both words                                                  |
| `pet*`            | A word that starts with `pet`                               |
| `%petrva%`        | A word within one edit of `petrva`                          |
| <code>petrova&#124;picard</code> | Either word                                           |
| `-petrova`        | Every session without the word                              |
| `jean-luc`        | `jean` and not `luc`. To find "Jean-Luc", send `jean luc`   |

The full syntax is in the
[Redis query syntax reference](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/query_syntax/).

!!! warning "Sanitize text that comes from an end user"
    Text from an end user can change the meaning of the query. An operator
    such as `-`, `|`, `*` or `%` changes which sessions match. Text that
    closes the parenthesis can add a condition on another attribute. The
    application must sanitize the input and limit it to what its use case
    needs.

This sanitizer keeps letters, digits and spaces, and nothing else:

```python
def plain_words(text: str, max_length: int = 64) -> str:
    kept = "".join(ch if ch.isalnum() else " " for ch in text[:max_length])
    return " ".join(kept.split())

@app.get("/admin/sessions")
async def by_name(q: str, store: SessionStoreDep) -> list[str]:
    words = plain_words(q)                 # "Jean-Luc" becomes "Jean Luc"
    if not words:
        return []
    found = await store.find("name", text=words, limit=20)
    return [info.session_id for info in found]
```

Text that Redis rejects for its syntax raises `ValueError`. Empty text is one
such case.

The `equals`, `between` and `near` filters are not query syntax. The store
builds them from typed values, so they need no sanitizer. `between` and `near`
accept finite numbers only.

### Pages and counts

| Method                           | Rows                                                             |
|----------------------------------|------------------------------------------------------------------|
| `list_for_subject`               | At most 1,000. Does not accept a custom `limit`                  |
| `list_active`, `find`            | One page: `limit` (default 100) and `offset` (default 0)         |
| `count_for_subject`, `count`     | None. The query sorts nothing                                    |
| `revoke_all`, `revoke_idle`      | None. The store ends the matches in batches of 1,000             |

- `offset + limit` can be at most 10,000. A larger page raises `ValueError`,
  and so does a `limit` below 1 or an `offset` below 0. There is no cursor.
- **A sorted page costs about as much as sorting all the matches.** A page
  deep in a large result is expensive.
- **"How many" is a count.** Call `count_for_subject` or `count`. Do not take
  the length of a list: a list stops at its limit, and it costs more.

### What a search result proves

A search result does not prove that a session is valid. Use it to list and to
count. Before you act on one session as if it were live, load it:

```python
loaded = await store.load(info.session_id, refresh=False)
if loaded is None:
    ...   # the session has ended
```

[Versions and limits](#versions-and-limits) lists the cases where a search and
a load can differ.

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
session in Redis, so it fails, whatever its shape.

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

| Reason          | What happened                                                                                                                               |
|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `"missing"`     | No usable session: no cookie, a malformed one, or a session that an earlier dependency revoked or emptied                                   |
| `"expired"`     | A well-formed cookie that names no live session: expired, revoked on another device, evicted or forged. The server cannot tell these apart. |
| `"unavailable"` | Redis could not be read, or the session could not be decoded, and the request continued without a session. Answer `503`, not a sign-in page. |

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
time left on the absolute clock in Redis, so a container with a wrong clock
cannot make an old session look recent.

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

A session that cannot be decoded - corrupted, or encrypted with another key - is
handled like a failed read. The session is kept, so fixing a wrong key brings
the sessions back. Signing out still works: `store.revoke()` or emptying the
session deletes the key and clears the cookie.

A search does not fail open. Each search method raises - see [Errors](#errors).

!!! warning "With `session_fail_closed=True`, users cannot sign out of an unreadable session"
    The request fails before your handler runs, so a sign-out route fails too.
    Every request that carries the cookie fails until the session reaches a
    deadline, or the user deletes the cookie. One corrupted session
    locks out one user; a wrong encryption key locks out everyone. To recover,
    fix the key, or delete the affected keys in Redis.

---

## Errors

```
SessionError
├── SessionConfigurationError
├── SessionAttributeTypeError            also a TypeError
└── SessionStoreError
    └── SessionSearchError
        ├── SessionSearchUnavailableError
        └── SessionSearchTimeoutError
```

| Error                           | Raised when                                                                                                                              |
|---------------------------------|------------------------------------------------------------------------------------------------------------------------------------------|
| `SessionConfigurationError`     | A setting is invalid, or `searchable` is combined with an `Encryptor`. Also at the save, when a searchable attribute was given a TTL     |
| `SessionAttributeTypeError`     | At the save, when a searchable attribute holds a value of another type. The store writes nothing                                         |
| `SessionStoreError`             | A write failed, or a read failed with `session_fail_closed=True`                                                                         |
| `SessionSearchUnavailableError` | A search method runs on a server without the Query Engine                                                                                |
| `SessionSearchTimeoutError`     | The server reported a timeout for a search                                                                                               |
| `SessionSearchError`            | A search failed for another reason, such as an index that could not be created                                                           |
| `ValueError`                    | A call is wrong: a `ttl` that is not a positive `int`, a name that is not searchable, a filter of the wrong type, invalid search text, or a page out of range |

`except SessionStoreError` also catches the search errors. `except
SessionError` catches every row but `ValueError`.

The save runs after your endpoint returns, so an error at the save cannot be
caught in the endpoint. The client gets a 500.

### Search timeouts

The store raises `SessionSearchTimeoutError` and returns no partial result. A
bulk revoke that ends a part of the matches and reports success would be a
security defect. After a timeout in `revoke_all` or `revoke_idle`, the
sessions found before the timeout are ended; call the method again.

The store can raise only when the server tells it about the timeout. By
default Redis returns a partial result instead
([`search-on-timeout return`](https://redis.io/docs/latest/develop/ai/search-and-query/administration/configuration/)),
and only a RESP3 connection tells the store that the result is partial. The
store logs one warning at startup when it reads such a policy. Set the policy
to `fail`:

```
CONFIG SET search-on-timeout fail
```

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
[`Cache-Control: private, max-age=300`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Cache-Control#private)
and `Vary: Cookie`, on the hit as well as the miss.
The `private` is not optional and is added for you: our entry is per user, so a
CDN told it may keep one copy for everyone would recreate the leak one hop
further out. The `Vary` stops the browser showing one user's copy to the next
person who signs in on it.

A caller with no session at all has no subject either. Every such caller
shares one entry, computed with no user - which gives nothing away. Put
[`valid_session()`](#requiring-a-valid-session) in front, first in
`dependencies=[...]`, to refuse them instead.

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

### Not setting the `vary_on_session` at all

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

| Your endpoint                                        | Declaration             | Cached as          | A caller with no session                      | Response headers                         |
|------------------------------------------------------|-------------------------|--------------------|-----------------------------------------------|------------------------------------------|
| Body depends on the user                             | `vary_on_session=True`  | one entry per user | shares one entry with every other such caller | `private, max-age=N` + `Vary: Cookie`    |
| Gated by `valid_session()`, body depends on the user | `vary_on_session=True`  | one entry per user | 401                                           | `private, max-age=N` + `Vary: Cookie`    |
| Reads the session, body identical                    | `vary_on_session=False` | one shared entry   | the shared entry                              | `max-age=N`, no `Vary`                   |
| Gated by `valid_session()`, body identical           | `vary_on_session=False` | one shared entry   | 401                                           | `private, max-age=N`, no `Vary`          |
| Gated by `valid_session(issued_within=...)`          | either                  | as declared        | 401                                           | `no-store`, + `Vary: Cookie` when `True` |
| Never touches the session                            | *(nothing)*             | one shared entry   | the shared entry                              | `max-age=N`                              |
| Touches it, nothing declared                         | *(nothing)*             | **not cached**     | not cached                                    | `private, no-store` + `Vary: Cookie`     |

Reading the table:

- **A hit sends the same headers as the miss.** The only difference is
  `max-age`, which on a hit is the time left on the entry. The browser keeps
  whichever response it saw last, so the two must agree.
- **A gate changes who reaches the entry, not how it is keyed.** Each gated
  row behaves like the row above it, except that a caller with no session gets
  a 401. The 401 says `private` (`no-store` under `issued_within`) and
  `Vary: Cookie`, so no cache replays it after the user signs in.
- **"Per user" means per subject.** An anonymous session that holds data - a
  basket, say - has no subject, so its entry is keyed by the session ID.
  `valid_session()` lets it through: the gate checks that the session is live,
  not who it belongs to.

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

Load once at accept time and hold the identity for the connection. Nothing
closes the socket when the session ends - by sign-out, revocation or expiry -
so a socket that must not outlive its session has to check it again itself,
for example with `store.load(raw, refresh=False)` on a timer.

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
from fastapi import Form, HTTPException    # Form needs python-multipart

@app.post("/login")
async def login(session: SessionDep) -> dict:
    session["user_id"] = 42
    session.pop("csrf", None)    # a fresh token for this sign-in
    return {}

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
for the token itself. A sign-in rotates the session ID but carries the data
across, the token included, so drop the token when you sign the user in: the
next `setdefault` then mints a fresh one.

A runnable version is in [`examples/sessions/csrf.py`](https://github.com/redis/fastapi-redis-sdk/blob/main/examples/sessions/csrf.py).

---

## Encrypting the session at rest

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

The store encrypts the value of each attribute on its own. The layout does not
change: a session is still one hash with one field for each leaf. The store
keeps each encrypted value as Base64.

What an `Encryptor` hides, and what it does not:

- **It hides each value.**
- **It does not hide the names of the attributes, their number, or the length
  of each value.** Field names stay in plaintext.
- **Do not put sensitive data in an attribute name or in a dict key.** A dict
  key becomes part of a field name: `session["invites"] = {"ana": True}`
  writes the field `a:invites.ana`.
- **The system fields are plaintext**: the subject, the two timestamps and the
  [descriptor](#the-descriptor). Do not put anything sensitive in what
  `descriptor_of` returns.

Three more things to know:

- **`searchable` cannot be combined with an `Encryptor`.** Redis cannot search
  an encrypted value, so setup raises `SessionConfigurationError`. The
  [built-in searches](#built-in-searches) work, because they read system
  fields only.
- **An encrypted session uses more memory.** Encryption makes each value
  longer. With the AES-GCM class above, a value of more than about 20
  characters is stored as more than 64 bytes, and one such value moves the
  session to Redis's larger hash form. See
  [Versions and limits](#versions-and-limits).
- **The `coder` option is not used for sessions.** Each value is JSON text,
  with or without an `Encryptor`. `coder` is still accepted, and has no effect.

A runnable version is in [`examples/sessions/encryption.py`](https://github.com/redis/fastapi-redis-sdk/blob/main/examples/sessions/encryption.py).

This package ships the seam and not the implementation, deliberately.
[AES-GCM](https://csrc.nist.gov/pubs/sp/800/38/d/final), via
[`AESGCM`](https://cryptography.io/en/latest/hazmat/primitives/aead/#cryptography.hazmat.primitives.ciphers.aead.AESGCM),
in your codebase is a smaller liability for everyone than a cryptographic
primitive maintained here - and it means a key rotation is your decision, on
your schedule.

One thing to know before you turn it on. A session that will not decrypt is
handled like a failed read and kept, not deleted: rotating a key without a
re-encryption pass signs everybody out, and restoring the old key brings them
back. By default that happens quietly, with a warning in the log; set
`session_fail_closed=True` to make it fail every request instead, sign-outs
included (see [When Redis is unreachable](#when-redis-is-unreachable)).

---

## Migrating from something else

Sessions do not survive the switch, by decision: existing cookies reference
sessions this store cannot read, so every user signs in again once. Deploy
accordingly. The same holds for an upgrade from a release with the earlier
session layout - see [Session clocks](#session-clocks).

### From Starlette's `SessionMiddleware`

```python
# before
from starlette.middleware.sessions import SessionMiddleware
app.add_middleware(SessionMiddleware, secret_key="…")

# after
FastAPIRedis(app).lifespan().sessions()
```

| Their behaviour                        | Here                                           |
|----------------------------------------|------------------------------------------------|
| `request.session["user_id"] = 42`      | unchanged - and it now rotates the ID          |
| `request.session.clear()`              | unchanged - and it now deletes the session too |
| payload capped at ~4 KB by the cookie  | no cap; the cookie carries an ID               |
| data signed but readable by the client | never leaves the server                        |
| `max_age`                              | `session_idle_ttl` plus `session_absolute_ttl` |
| no revocation                          | `revoke`, `revoke_id`, `revoke_all`            |

Handler code does not change. `secret_key` has no equivalent because nothing is
signed: the cookie is an opaque 256-bit identifier, well past OWASP's
[64-bit entropy floor](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html#session-id-entropy).

### From other session libraries

Many Starlette-compatible libraries offer `lifetime` and `rolling` options and
a `load_session` call. The table maps those to this package.

| Theirs                           | Here                                                                                                |
|----------------------------------|-----------------------------------------------------------------------------------------------------|
| `lifetime=N, rolling=True`       | `session_idle_ttl=N`                                                                                |
| `lifetime=N, rolling=False`      | `session_absolute_ttl=N`                                                                            |
| both behaviours at once          | not expressible for them; set both settings here                                                    |
| `load_session(request)`          | nothing - the load is automatic and eager                                                           |
| `regenerate_session_id(request)` | delete the call; writing the identity rotates                                                       |
| `get_session_metadata(request)`  | `created` from `SessionStateDep`; `last_access` from `store.list_for_subject()`, which needs the Query Engine; `lifetime` is `store.absolute_seconds` |
| `RedisStore(...)`                | `FastAPIRedis(app).lifespan().sessions()`                                                           |

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

**Set `search-on-timeout fail` if you search sessions.** With the default
policy a search that times out can return a partial result - see
[Search timeouts](#search-timeouts).

---

## Versions and limits

| Server                                                | What works                                                                                   |
|-------------------------------------------------------|----------------------------------------------------------------------------------------------|
| Redis 7.4, no Query Engine                            | Sessions, both clocks, attribute TTL, `revoke`, `revoke_id`. No search method                |
| Redis 7.4 with the Query Engine, such as Redis Stack  | The same, plus search. The last access shows the last save                                   |
| Redis 8.0 and later                                   | Everything                                                                                   |

**Before Redis 8, a search can return a session that has ended.** Each query
leaves out a session whose `s:created` has expired. Before Redis 8 a query can
still match an expired field, so a search can return a session past its
absolute deadline. The index can also hold a session that has expired.

**A search result does not prove that a session is valid**, on any version.
Load the session before you act on it as live - see
[What a search result proves](#what-a-search-result-proves).

**On Redis 8.0 to 8.8, each request re-indexes the session.** A load records
the last access in `s:seen`, and `s:seen` is an indexed field. Redis updates
the index entries of the session on each request that carries the cookie.

**With the Query Engine but without `HSETEX`, the last access shows the last
save.** `HSETEX` arrived in Redis 8.0. On an earlier server a load does not
write `s:seen`; only a save does. `SessionInfo.last_access`, the order of each
list, `list_active` and `revoke_idle` then go by the last save.
`revoke_idle` ends a session that was read, but not written, since the cutoff.

**A long value costs memory.** Redis keeps a small hash in a compact form.
A value or a field name longer than 64 bytes moves the whole hash to the
larger form
([`hash-max-listpack-value`](https://redis.io/docs/latest/operate/oss_and_stack/management/optimization/memory-optimization/)).
A long string does this, and so does a deep path, and so does an encrypted
value of more than about 20 characters. A hash with more than 512 fields
(`hash-max-listpack-entries`) does it too.

---

## Sync endpoints

```python
from redis_fastapi import SyncSessionStoreDep

@app.post("/logout")
def logout(state: SessionStateDep, store: SyncSessionStoreDep) -> dict:
    store.revoke(state)
    return {"ok": True}
```

`SyncSessionStoreDep` has the methods a handler calls: `rotate`,
`reauthenticate`, `revoke`, `revoke_id`, `revoke_all`, `list_for_subject`,
`count_for_subject`, `list_active`, `revoke_idle`, `find` and `count`.

---

## Replacing the pieces

Every seam is a keyword argument to `.sessions()`:

```python
FastAPIRedis(app).lifespan().sessions(
    encryptor=AesGcmEncryptor(key),          # encryption at rest
    id_factory=my_id_factory,                # identifier generation (validated)
    key_prefix="myapp",                      # key namespace
    idle_ttl=timedelta(minutes=15),          # idle clock
    absolute_ttl=timedelta(hours=8),         # absolute clock
    # what a device listing shows for the session (stored as plaintext)
    descriptor_of=lambda req, s: {"ip": req.client.host},
    principal_keys=["user_id", "role"],      # what rotates the ID
    subject_of=lambda s: s.get("tenant_id"), # the subject a search by subject uses
    # searchable={"device_id": "tag"},       # attributes store.find() can search;
    #                                        # not together with an encryptor
    cookie_builder=my_cookie_builder,        # Set-Cookie rendering
    # requests that get no session and make no Redis calls
    skip=lambda req: req.url.path.startswith("/health"),
)
```

`coder` is still accepted, and is not used for sessions: each value is stored
as JSON text.

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
from fastapi import Request

def use_fake_store(request: Request) -> SessionStore:
    return fake_store

app.dependency_overrides[get_session_store] = use_fake_store
```

Annotate the parameter as `Request`. FastAPI reads the signature of an
override, and takes a parameter with no annotation, as in
`lambda request: fake_store`, for a query parameter. Each handler that uses
`SessionStoreDep` would then answer `422`.

---

## Settings

Every setting is an environment variable prefixed `REDIS_`, so
`session_idle_ttl` is `REDIS_SESSION_IDLE_TTL`. See
[Configuration](configuration.md) for how these are loaded.

| Setting                     | Default       | Notes                                                                                                                                      |
|-----------------------------|---------------|--------------------------------------------------------------------------------------------------------------------------------------------|
| `session_cookie_name`       | `session`     | Matches Starlette and other libraries                                                                                                      |
| `session_cookie_https_only` | `True`        | Adds [`Secure`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#secure). Turn it off for local HTTP only    |
| `session_cookie_same_site`  | `lax`         | [`none`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Reference/Headers/Set-Cookie#samesitesamesite-value) requires `https_only=True` |
| `session_idle_ttl`          | `1800`        | `0` disables the idle clock                                                                                                                |
| `session_absolute_ttl`      | `28800`       | `0` disables the absolute clock                                                                                                            |
| `session_gc_ttl`            | `2592000`     | Backstop so Redis can always collect an abandoned key                                                                                      |
| `session_refresh_on_load`   | `True`        | `False`: only a request that *used* the session counts as activity                                                                         |
| `session_fail_closed`       | `False`       | Read behaviour when Redis is down or a session is unreadable                                                                               |
| `session_always_save`       | `False`       | Escape route for nested mutation. Writes on every request that **read** the session; an empty session is exempt                            |
| `session_principal_keys`    | `["user_id"]` | What a change to rotates the ID. Comma-separated in the environment: `REDIS_SESSION_PRINCIPAL_KEYS=user_id,role`                           |

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
- [Redis key expiration](https://redis.io/docs/latest/commands/expire/) and
  [hash field expiration](https://redis.io/docs/latest/commands/hexpire/)
  - the mechanisms the two clocks and the attribute TTL are built on
- [Redis Query Engine](https://redis.io/docs/latest/develop/ai/search-and-query/)
  and its [query syntax](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/query_syntax/)
  - what the session searches run on
