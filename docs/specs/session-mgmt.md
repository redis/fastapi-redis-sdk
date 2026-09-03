# Session management for Starlette and FastAPI: research and recommendation

## Context

`fastapi-redis-sdk` is the official Redis integration for FastAPI. The package
already gives you four things:

- connection management
- cache operations that use dependency injection (`cache`, `cache_evict`,
  `cache_put`, and `CacheBackend`)
- rate limit operations (`rate_limit` and `RateLimitBackend`)
- OpenTelemetry instrumentation

You configure all four with one chain of methods:
`FastAPIRedis(app).lifespan().caching().rate_limiting().otel()`.

The FastAPI framework has no standard session solution and this document answers the question if server-side sessions should be the next feature.

---

## 1. What the core maintainers decided

Both upstream projects refused to own session backends. It is a decision about project scope, and the maintainers confirmed it across seven years.

| Source | Date | Outcome |
|---|---|---|
| [fastapi#754 "First-class session support"](https://github.com/fastapi/fastapi/issues/754) | Opened Nov 2019. Closed Feb 2020. | tiangolo: *"It's already in place. More or less like the rest of the security tools… It's just not properly documented yet."* He pointed to `APIKeyCookie` and a JWT in a cookie. He closed the issue as answered. |
| [encode/starlette#499 "Add pluggable session backends"](https://github.com/encode/starlette/pull/499) | Opened May 2019. Closed **unmerged** Feb 2022. | Tom Christie: *"let's continue with the 'as a third party packages' approach… it looks to me like we oughta scope Starlette as 'feature complete' and just let folks build other stuff on top of it."* |
| [starlette#1801 "Why are Starlette sessions so basic?"](https://github.com/Kludex/starlette/discussions/1801) | Aug 2022 | adriangb gave the accepted answer. Starlette is *"a **minimal** and **composable** web toolkit, not a complete web framework with all of the batteries included"*. Also: *"if it can be implemented as an external package, we'd prefer that to bringing it into Starlette itself."* The original poster then proposed to **remove** sessions from the core and to point users to `starsessions`. adriangb replied *"I don't disagree with you"*. Kludex replied *"That's a good point. 👍"* |
| [starlette#2256 (with PR #2255) JWT sessions](https://github.com/Kludex/starlette/discussions/2256) | Aug 2023 | The maintainers rejected it: *"can very well be an independent package… If it's a feature that's popular… it will be maintained by the community."* Nobody answered the discussion. The author released an alpha third-party package instead. |
| [fastapi#10370 Roadmap](https://github.com/fastapi/fastapi/issues/10370) | current | The roadmap has no work item for sessions or authentication. The items are Pydantic v2, Starlette upgrades, and the removal of Python versions at end of life. |

**One argument on the record has no answer.** In #754, the requester made an
important point. A JWT in a cookie works only while the session data is small
enough. The limit is 4096 bytes, from
[RFC 6265](https://tools.ietf.org/html/rfc6265#section-6.1). The requester wanted
server-side backends that you can exchange, in the style of Django and PHP.

dmontagu was then a core maintainer. He agreed that FastAPI must add only
dependency injection and OpenAPI support. Starlette must own the backends. But
Starlette then refused to own them. Neither project took the feature, and the
situation did not change.

**A note about project governance.** Kludex moved Starlette and Uvicorn from the
`encode` organisation to a personal account. Starlette then released version 1.0,
and the current release is **1.6.0 (Aug 2026)**. The maintainers call the project
feature complete. A 1.0 release with that scope makes adoption into the core
**less** probable, not more probable.

### 1a. One change that helps us

Kludex wrote [starlette#3166](https://github.com/Kludex/starlette/pull/3166) and
merged it on **2026-03-01**. Before this change, `scope["session"]` held a plain
`dict`. Now it holds a `Session` subclass. The subclass records two flags, in the
same way as Django and Flask:

- `accessed` becomes true when code reads the session. The
  `HTTPConnection.session` property sets this flag.
- `modified` becomes true when code calls `__setitem__`, `__delitem__`, `clear`,
  or `update`. The `pop` and `setdefault` methods set the flag only if the value
  changes.

The middleware now sends `Set-Cookie` **only if `modified` is true**. It adds
`Vary: Cookie` if `accessed` is true. This corrects
[race condition #2019](https://github.com/Kludex/starlette/issues/2019). In that
condition, a slow response that only read the session replaced a newer session
cookie.

The core still has **no backend parameter**. It supports only cookies that
itsdangerous signs. But a server-side store needs exactly the `accessed` and
`modified` flags. The flags let the store avoid a Redis request on every HTTP
request. Starlette built this connection point for its own purpose.

This is the most important technical result of the research. The flags did not
exist when the authors designed the current session libraries.

Section 5a explains what we do with this result. We copy the design of the flags
into our own class, and we do not import the Starlette class. That decision keeps
the minimum version of FastAPI where it is, and it lets us correct two faults in
the upstream flags without a wait.

---

## 2. What exists today, and how much people use it

The table shows PyPI downloads for the last 30 days, from `pypistats`. It also
shows the condition of each repository.

| Package | Downloads each month | Stars | Status |
|---|---|---|---|
| `starlette` | 665M | — | 1.6.0, Aug 2026 |
| `fastapi` | 603M | — | active |
| **`fastapi-users`** | **1.53M** | 6.2k | active (Aug 2026) |
| `starsessions` | 429k | 123 | active, v2.3.0a1 Mar 2026. The Starlette documentation points to this package. |
| `fastapi-sessions` | 70k | 109 | **ARCHIVED**. Last change Jul 2023. |
| `authx` | 69k | 1.2k | active |
| `starlette-session` | 41k | 37 | not maintained. Last change Feb 2023. |
| `starlette-authlib` | 2.6k | — | small user base |
| `fastsession` | 224 | — | almost no users |
| for comparison: `pyjwt` | 705M | — | — |
| for comparison: `authlib` | 154M | — | — |

Two conclusions are possible. Both are correct.

- **The session libraries together get approximately 610k downloads each month.
  Starlette gets 665M. The session libraries are therefore less than 0.1% of
  that.** No package became the standard. The package that the Starlette
  documentation recommends has 123 stars. An **archived** package gets 70k
  downloads each month. This is a supply chain risk, not a healthy market.
- **`fastapi-users` alone gets 2.5 times the downloads of all session libraries
  together.** Its
  [`RedisStrategy`](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/strategies/redis/)
  *is* a server-side session store, but it has a different name. Redis holds an
  opaque token that maps to a user ID. The strategy reads the token on each
  request. It **deletes the token when the user logs out**, which gives correct
  revocation. This is the server-side session implementation with the most users,
  and its name does not include the word session.

**Conclusion: the demand is real, but users satisfy it in other ways.** They use
three methods. First, the Redis strategy in `fastapi-users`. Second, a pattern
that they write themselves: `secrets.token_urlsafe(32)`, then `SETEX`, then a
cookie. Nearly every tutorial and vendor guide teaches this pattern. Third,
hosted identity providers such as WorkOS, Auth0, and PropelAuth. Download counts
for session libraries are therefore the wrong measurement.

### 2a. How applications use the Starlette session today

Most applications do **not** use `SessionMiddleware` for user sessions. They use
it to hold OAuth state for Authlib. Authlib puts the `state` parameter and the
PKCE code verifier into `request.session`, and it requires the middleware. This
explains why the simple signed cookie was sufficient for many years. The data is
small, and it exists for a short time.

This also explains the most frequent session fault that users report:
`mismatching_state` and `MismatchingStateError`. Three conditions cause this
fault. First, an incorrect `SameSite` or `Secure` setting. Second, a change of the
secret, which makes the old cookies invalid. Third, a signed cookie that becomes
larger than 4KB after somebody adds a token or a user profile.

The guidance for all three conditions is the same:
*"keep the session payload small; store tokens server-side"*. No package supplies
a recommended solution for that guidance.

---

## 3. Is session management part of security, and can you avoid it?

It is part of security. You can avoid it only for machine-to-machine interfaces.
The requirements below come from the
[OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html):

- **Opaque IDs and server state.** *"The session ID content (or value) must be
  meaningless to prevent information disclosure attacks"*. Business data
  *"must be stored on the server side, and specifically, in session objects or in a
  session management database or repository."* An ID needs at least 64 bits of
  entropy from a cryptographic random number generator. OWASP recommends at least
  128 bits for a custom ID.
- **Rotation.** *"The session ID must be renewed or regenerated… after any privilege
  level change"*. Also: *"regeneration is mandatory to prevent session fixation
  attacks."* Four events start a rotation: authentication, a password change, a
  permission change, and an increase of the user role.
- **Timeouts.** *"All sessions should implement an idle or inactivity timeout"*
  **and** *"an absolute timeout, regardless of session activity"*. Also:
  *"expiration must be enforced server-side."* OWASP rejects enforcement on the
  client. A **renewal timeout** is optional. It rotates the ID while the session
  continues.
- **Termination.** When a session expires, the application *"must take active
  actions to invalidate the session on both sides, client and server. The latter is
  the most relevant."*
- **Concurrency.** A user must be able to see the active sessions and to receive an
  alert about a new login. The user must also be able to **terminate a session from
  a different device**.
- **Detection of a stolen session.** OWASP writes: *"highly recommended to
  bind the session ID to other user or client properties, such as the client IP
  address, User-Agent"*. Use this for detection only. OWASP gives a clear warning.
  Network address translation, proxy servers, and spoofing mean that this binding
  *"cannot be used… to trustingly defend."*
- **Implementation.** *"It is recommended to use these built-in frameworks versus
  building a home made one from scratch."*
- Never put a token in `localStorage` or in `sessionStorage`.

The next table compares the two ways to carry a session against these
requirements. The symbol ✓ means that the method meets the requirement. The
symbol ✗ means that it does not meet the requirement.

| Requirement | Signed cookie or JWT | Server-side store |
|---|---|---|
| Immediate logout and revocation | ✗ valid until it expires | ✓ delete the key |
| The server enforces an idle timeout **and** an absolute timeout | ✗ | ✓ two clocks |
| Rotation of the ID after a privilege change | partial. You issue a new token, but the old token stays valid. | ✓ |
| Show and terminate the sessions of one user | ✗ | ✓ |
| A payload larger than 4KB | ✗ | ✓ |
| Keep the data away from the client | ✗ a signature is not encryption | ✓ |
| No infrastructure and no lookup for each request | ✓ | ✗ |

Stateless tokens are correct for service-to-service calls and for access tokens
with a short life. But some requirements above need shared state. These are the
requirements to revoke, to rotate, to limit, and to list a session.

Many sources recommend a hybrid method. You put a session ID into the JWT. Then
you compare that ID against a denylist in Redis. This method is a session store,
but its properties are worse. You do the lookup, and you also keep the size and
the staleness of the token. If you must revoke a session, you have already
accepted the cost of shared state.

The last OWASP point has two effects. It tells you to prefer framework code that
many people examined. Therefore it is an argument against one more small package.
It is also an argument for one implementation that the authors build and review
with care. Faults in session fixation, rotation, and CSRF protection become
security vulnerabilities, not simple defects.

---

## 4. Will the need come as FastAPI grows? Yes, for three reasons

**(a) Every mature ecosystem added this feature.** Django supplies session
backends that you can exchange, and one of them uses the cache or Redis. PHP has
`SessionHandler`. Rails has a store abstraction. Express uses `express-session`
together with `connect-redis`.

The closest example is **Spring Session Data Redis**. You put
`spring-session-data-redis` on the classpath. Spring then configures a
Redis-backed `HttpSession` automatically, and you change *no application code*.
Redis replaces the in-memory sessions. Every application instance can then serve
every request, so you do not need sticky sessions, and a session survives a
restart of a node.

A vendor maintains that integration for this exact problem, and we must follow the
same model. FastAPI is the only large modern web framework without an equivalent.

**(b) Most practitioners now reject JWTs for sessions.** Guidance from 2026
usually starts with the statement "JWTs can't be revoked natively". It then
recommends a denylist in Redis or a hybrid session record. The technical argument
for the stateless default in FastAPI is therefore much weaker than before.

**(c) The new reason: AI agents and MCP backends.** Most developers build these
services with FastAPI. The **Model Context Protocol (MCP) revision of 2026-07-28
is stateless by design**. It moves session behaviour to the application on
purpose. The server gives the client an identifier, and the client returns it.

In FastMCP, the session state is in memory by default. FastMCP needs Redis, or
another shared store, as soon as you run more than one server instance. Its event
store for resumability already uses Redis.

This group of FastAPI applications grows quickly. Each one needs the same thing: a
shared key-value store, with a time to live (TTL) for each session, plus rotation
and cleanup. Redis already supplies a similar product,
`redis/agent-memory-server`. That product holds session memory and working memory
with a TTL, and it can promote the data to long-term memory.

One more group grows, and it must use cookies: FastAPI applications that render
pages on the server with Jinja and HTMX. This is the original use case in #754.
These applications cannot use bearer tokens easily.

**Arguments against the feature:**

- Upstream will not adopt it. It stays a third-party package.
- We compete with `starsessions`, which the Starlette documentation recommends. We
  also compete with the `RedisStrategy` in `fastapi-users`, which has the most
  users today. We must show the differences clearly. Compatibility is more
  important than the number of features.
- The SDK becomes part of the security path. This raises the standard for review,
  documentation, and vulnerability disclosure. `SECURITY.md` exists, but it must
  then have more content.

---

## 5. Recommendation

**Build the feature as `FastAPIRedis(app).sessions()`.** Present it as the
Redis-native session store for FastAPI that meets the OWASP requirements. Give it
good compatibility with the existing `request.session` interface. Do not invent a
new interface. The gap is real. The existing packages are either small or have a
different name. Redis is the correct vendor for this feature, in the same way that
Spring Session Data Redis is correct for Spring.

Two conditions make this the right time. Starlette 1.x added the `accessed` and
`modified` flags. The agent and MCP applications also create new demand.

### The four things that existing packages do not do

1. **Use `Session.accessed` and `Session.modified` (Starlette 1.x and later,
   #3166).** Write to Redis only when the data changes. Refresh the idle TTL when
   code reads the session. Let the core send `Vary: Cookie`. The user does not have
   to call `load_session()`. `starsessions` solves the same problem with an
   explicit load call, and it raises `SessionNotLoaded` if you forget the call.
   That is a mistake which is easy to make, and we can prevent it. No library
   designed before March 2026 can use this method.
2. **Supply the OWASP operations as an API, not as documentation.** Give the user
   rotation that defends against session fixation at login and after a privilege
   change — and make it **automatic**, so no application call can be omitted. The
   middleware compares a *principal* before and after each request and rotates when
   it changes. Section 5.1 of [`session-design.md`](session-design.md) gives the
   mechanism and the sequence. Give **separate idle and absolute TTLs**. Give
   `revoke()`.
   Give `list_sessions(user_id)` and `revoke_all(user_id)` to control concurrent
   sessions. A Redis SET for each user holds the session IDs. A cookie library
   cannot supply that last capability, and this is the clearest reason to use
   Redis.
3. **Make the session visible in OpenAPI with `APIKeyCookie`.** This answers the
   original request in #754 directly. It uses the primitive that tiangolo
   recommends, so the session becomes a declared security scheme.
4. **Use compatibility to get adoption.** Stay compatible with `request.session`.
   Existing code and **Authlib OAuth flows then work without any change**. This
   also corrects two classes of `mismatching_state` fault: the 4KB overflow, and
   the `Set-Cookie` race condition. Document how to migrate from the
   `RedisStrategy` in `fastapi-users`, and from `starsessions`.

### 5a. Starlette already supports cookies. Why do we not use that support?

We keep the cookie. We change the content of the cookie.

|                 | The cookie holds                | Where the data is                                   |
|-----------------|---------------------------------|-----------------------------------------------------|
| Starlette today | `sign(b64(json(session_data)))` | the cookie **is** the database                      |
| This design     | `sign(opaque_id)`               | the cookie is a **pointer**. Redis is the database. |

That one change gives us every row of the table in Section 3: revocation, an idle
timeout and an absolute timeout that the server enforces, a payload larger than 4KB,
data that stays away from the client, and `revoke_all`. None of them are possible
while the payload is in the cookie, because the server then keeps no copy of anything.

**We cannot extend the middleware that exists.** It has no connection point. We
verified this in `starlette/middleware/sessions.py` on `main`:

- The constructor takes `app`, `secret_key`, `session_cookie`, `max_age`, `path`,
  `same_site`, `https_only`, and `domain`. It takes **no `backend`, no `store`, and no
  `serializer`.**
- The read path is inside `__call__`: `signer.unsign`, then `b64decode`, then
  `json.loads`, then `Session(...)`.
- The write path is inside the `send_wrapper` **closure**: `json.dumps`, then
  `b64encode`, then `signer.sign`, then `Set-Cookie`.

The encode and decode operations are in a closure inside `__call__`. A subclass
therefore has nothing to override except `__call__` itself, which means that it
rewrites the whole method. The missing constructor parameter is exactly the change in
[starlette#499](https://github.com/encode/starlette/pull/499). The table in Section 1
records that the maintainers declined it. The parameter is absent on purpose.

**Two other methods do not work. Do not propose them again.**

- **Put our layer on top of the Starlette middleware**, and let the Starlette cookie
  hold only `{"sid": ...}`. This fails difference 4 above. `request.session` then
  becomes the *cookie* dictionary, so Authlib writes the OAuth state into the cookie
  and not into Redis. We lose the correction for the 4KB limit, and we lose the
  drop-in property. Two `max_age` values also then compete.
- **Use dependency injection with no middleware.** This is not possible. The
  application must add `Set-Cookie` before it sends `http.response.start`. A
  dependency that is a context manager stays open until the background tasks finish,
  which is much later than the headers. sm-Fifteen recorded this limit in
  [fastapi#754](https://github.com/fastapi/fastapi/issues/754), and it is the reason
  why a wrapper around `send` is necessary.

**What we reuse. The replacement is approximately 80 lines, not a fork.**

- The `scope["session"]` contract. This is what keeps Authlib and every existing call
  to `request.session` correct with no change.
- The construction of the cookie flags (`httponly; samesite=…; secure`), the
  `add_vary_header("Cookie")` call, and the method to clear a cookie (the value
  `null`, with an `expires` date in 1970).
- `itsdangerous.TimestampSigner`. We still sign, but we sign the ID and not the
  payload.
- `MutableHeaders`, `HTTPConnection`, and `Secret`.

**Starlette permits this. It is a connection point, not a workaround.** The `session`
property in `starlette/requests.py` contains:

```python
session: Session = self.scope["session"]
# We keep the hasattr in case people actually use their own `SessionMiddleware` implementation.
if hasattr(session, "mark_accessed"):  # pragma: no branch
    session.mark_accessed()
```

The core supports a third-party session middleware that puts its own object into
`scope["session"]`, and a comment in the source says so.

**Therefore: write our own `Session` class. Do not import the Starlette class.** We
write the middleware in any case, so we control the object in `scope["session"]`. This
gives four results.

1. **The minimum version does not change.** The design works with Starlette 0.4x and
   with 1.x. The declaration `fastapi>=0.115.0` stays correct.
2. **We correct the faults in the upstream class ourselves, now.** Section 6.1 lists
   them: `popitem()` and `|=` set no flag, and `pop()` sets `modified` without
   `accessed`. For a server-side store, a missed flag is a lost write with no error,
   which is the worst fault in this design. We do not wait for a pull request from
   another author.
3. **We depend on no change in Starlette.** We import nothing from
   `starlette.middleware.sessions`, so no proposal of ours must succeed before we
   ship.
4. With Starlette 1.0 and later, the property above still calls `mark_accessed()` for
   us. With earlier versions the property only returns the dictionary, so we use a
   safe default: treat the session as accessed, and always send `Vary: Cookie`.

The cost: we own a class of approximately 40 lines, and we must read the upstream
`Session` class when it changes. That cost is smaller than a minimum version that we
cannot lower again.

### Structure, which follows the existing conventions of the SDK

Use the same division as the existing cache and rate limit code:

| New file                               | Follows                                    | Contents                                                                                                                                                                                                                                                                                            |
|----------------------------------------|--------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `src/redis_fastapi/sessions.py`        | `cache.py`, `ratelimit.py`                 | `SessionMiddleware`, the `session()` factory for dependency injection, and `add_redis_sessions()`                                                                                                                                                                                                   |
| `src/redis_fastapi/session_backend.py` | `cache_backend.py`, `ratelimit_backend.py` | `SessionStore`, an abstract base class (ABC) that owns the lifecycle. Also `RedisSessionBackend` and `SyncSessionBackend`, with these methods: `load`, `save`, `delete`, `rotate`, `touch`, `list_for_user`, and `revoke_all`. Section 7 gives the reason for an ABC instead of a plain `Protocol`. |

Extend the existing files. Do not write the same code again.

**Serialize with the existing `Coder`.** The repository already has a serialization
interface: the `Coder` protocol and the `JsonCoder` implementation, in
`src/redis_fastapi/types.py:15`. Use them for the session payload. Do not write a
second interface. `starsessions` has a separate `Serializer` and `JsonSerializer` for
this purpose, but we do not need them. This choice also gives us
`pydantic_model_coder()` at no cost, so a user can hold a typed model in a session.

- `src/redis_fastapi/setup.py`: add `.sessions()` to the `FastAPIRedis` chain.
- `src/redis_fastapi/deps.py`: add `SessionDep`, `SessionBackendDep`, and
  `get_session_backend`. Put them beside the existing `get_cache_backend` and
  `get_rate_limit_backend`. Keep the `dependency_overrides` behaviour, because
  tests need it.
- `src/redis_fastapi/config.py`: add `REDIS_SESSION_*` settings to
  `RedisSettings`. Section 8.3 compares this list against `starsessions`, and every
  entry below has an equivalent there. The settings control:
    - the cookie name, and its `domain` and `path`
    - the `SameSite` value and the `https_only` flag
    - the idle TTL and the absolute TTL. **Both accept an `int` or a
      `timedelta`**, as `cache()` already does in this repository.
    - the session-only mode, where the cookie carries no `max-age` and the browser
      deletes it when it closes
    - `gc_ttl`, the TTL for a Redis key in that mode, where no exact expiry exists
    - the key prefix, as a string **or a callable**
    - the rotation policy, as `principal_keys`: the session keys whose value triggers a
      rotation when it changes. The default watches the identity; add a role or a scope
      list to rotate on a privilege change as well.
    - the encryption, which is off by default. See Section 8.3.1.
- `src/redis_fastapi/telemetry.py`: add `session_*` instruments. Follow the
  existing pattern in that file exactly. Add new fields to `_OTelState`. Add a
  `session_span()` function beside `cache_span` and `ratelimit_span`. Add
  `record_session_*()` helper functions with the same guard conditions. Add a
  `timed_session()` context manager. Use these three instruments:
    - `redis_fastapi.sessions.operations`, a counter. The `operation` attribute is
      `load`, `save`, `rotate`, or `revoke`. The `result` attribute is `hit`,
      `miss`, or `expired`.
    - `redis_fastapi.sessions.latency`, a histogram.
    - `redis_fastapi.sessions.active`, an up-down counter or a gauge.

    Keep two existing behaviours: each helper does nothing if the import failed,
    and `disable_telemetry()` resets the state.

    Note the pattern in `telemetry.py`. It uses a module-global `_state`, and the
    backend methods call free functions. This pattern is one reason why Section 7
    chooses an ABC. The instrumentation calls belong in the base class. Each store
    then emits the telemetry once, and no vendor can omit them.
- `src/redis_fastapi/__init__.py`: add the new public names to `__all__`.
- Documentation: write `docs/guide/sessions.md` and add it to the mkdocs
  navigation. Add a section to `docs/guide/observability.md` for the new metrics.
  Write an application in `examples/` that shows a login, a privilege change, a
  logout, and `revoke_all()` — with the first two rotating on their own, so the example
  demonstrates that no rotation call exists. Write a second example for an Authlib OAuth flow.

### Security requirements. Treat these as acceptance criteria.

- **Write our own `Session` class. Do not import the one from Starlette, and do not
  raise the minimum version.** Section 5a gives the complete reasoning. In summary:
  Starlette added `Session` in version 1.0.0 (2026-03-22). But `pyproject.toml`
  declares no direct dependency on Starlette, and `fastapi>=0.115.0` permits a 0.4x
  version, which has no `Session` class. A floor of `starlette>=1.0.0` therefore
  raises the true FastAPI minimum by approximately twenty minor versions, and it
  contradicts the row "FastAPI 0.115+" in the requirements table of `README.md`. We
  write our own middleware in any case, so we can put our own object into
  `scope["session"]`.
- Make session IDs with `secrets.token_urlsafe(32)`, which gives at least 128
  bits. Keep the IDs opaque. Never put data into an ID.
- **Validate the session ID from the cookie before any other use of it.** Accept
  only the characters that are safe in a cookie value, and treat every other value
  as no session at all. Without this test, a value from the client can inject a
  header when the code writes the ID back into `Set-Cookie`. `starsessions` has this
  control, and Section 8.3 records it.
- **Rotate after authentication and after a privilege change, without an application
  call.** The middleware detects the change and rotates; the old key is deleted before
  the new one is written. A control that must be invoked is a control that can be
  omitted, and omitting this one is session fixation. Section 5.1 of
  [`session-design.md`](session-design.md) gives the detector and its four safety
  rules.
- Enforce an idle TTL **and** an absolute TTL. Redis must enforce both of them.
- Use strict cookie defaults: `HttpOnly`, `SameSite=Lax`, and `Secure`. Supply a
  documented method to disable `Secure` during development. `starsessions` also
  uses strict defaults, and that decision is correct.
- Send `Cache-Control: no-store` with each response that carries a session.
  Support `Clear-Site-Data` at logout.
- Write a salted hash of the session ID to the log. Never write the ID itself.
  This rule also applies to telemetry: **a session ID must never become a span
  attribute or a metric label.**
- Document CSRF clearly. A cookie session has CSRF exposure, but a bearer token
  does not. Point the user to `fastapi-csrf-protect`, which gets 138k downloads
  each month, or write your own guidance.
- Make one feature optional and disable it by default: a check of the IP address
  and the User-Agent, to *detect* a stolen session. Document the OWASP warning
  with this feature. No user must think that it is a control.

### Verification

- Write unit tests in `tests/unit/`. Use the same structure as the existing cache
  and rate limit tests. Test these conditions:
    - Rotation keeps the data and makes the old key invalid.
    - **Rotation happens with no application call**, when the identity is written.
    - A key declared privilege-bearing rotates on a change in either direction.
    - The idle timeout and the absolute timeout work independently.
    - `revoke_all` removes every session of one user.
    - The store writes nothing to Redis if no code touched the session. Assert on
      the `modified` flag.
    - The response has a `Vary: Cookie` header if code read the session.
- **Do not trust the `modified` flag.** Write one test for each row of the table in
  Section 6.1: `popitem()`, `|=`, a `pop()` that must also set `accessed`, and a
  change inside a nested object such as `session["a"]["b"] = 1`. In every case the
  store must still hold the data afterwards. The last row passes through the explicit
  `save()` method, because no subclass of `dict` can detect that change. These tests
  must never depend on the release schedule of Starlette.
- Write integration tests in `tests/integration/` against a real Redis server.
  Test that two application instances share one session through one Redis server.
  Test the TTL behaviour. Test the concurrent requests that replaced a cookie
  before #3166.
- Write a compatibility test. An Authlib OAuth flow must complete against the
  Redis store without any change. Also test a payload larger than 4KB, which a
  signed cookie cannot hold.
- Test the items that Section 8.3 added:
    - A session ID with an unsafe character gives a new session, and nothing from
      that value reaches a response header.
    - With encryption on, the value in Redis is not readable, and a round trip
      returns the same data. With encryption off, no warning appears for each
      request.
    - The session-only mode sends no `max-age`, and the Redis key still gets a TTL
      from `gc_ttl`.
    - The idle TTL and the absolute TTL each move the `max-age` of the cookie and
      the TTL of the key **together**. Section 8.3.2 explains why one test must
      cover both.
    - The key prefix works as a string and as a callable.
    - Every extension point in Section 9 accepts a substitute, and the middleware
      then uses it.
- Write a telemetry test. Follow the existing pattern. Assert that the instruments
  record the data. Assert that `disable_telemetry()` gives a clean `_OTelState`.
  Assert that no attribute contains a session ID.
- Run `nox`. It runs the lint, mypy, bandit, and coverage steps, and the
  repository requires all four.
- Do a manual test. Run the login application in `examples/`. Confirm the
  `HttpOnly`, `Secure`, and `SameSite` flags. Look at the keys in Redis. Confirm
  that a logout deletes them.

### Suggested order of work

Section 8.5 gives the complete contents of version 1. Inside that release, build
the OWASP core first: `session_backend.py`, `sessions.py`, the dependency
injection, the configuration, the strict cookie defaults, `rotate`, and `revoke`.

**Write the binding to a user and the index for each user in the same release**,
even if `list_sessions` and `revoke_all` appear later. Section 8.4 gives the
reason: an index that arrives after the first sessions exist reports a wrong
answer, and it reports it silently.

Give `list_sessions`, `revoke_all`, and the Authlib compatibility example their own
release note. They are the clearest differences from the other packages.

---

## 6. What we implement better, and what we send upstream

### 6.1 What our `Session` class must do better than the upstream one

Section 5a decides that we write our own class. This is the list of faults in the
upstream class that ours must not repeat. We verified each one in
`starlette/middleware/sessions.py`.

Difference 1 in Section 5 is the rule to write to Redis only when the data changes.
That rule is only as good as the `modified` flag. The upstream class overrides
`__setitem__`, `__delitem__`, `clear`, `pop`, `setdefault`, and `update`, and it
misses the cases below.

**The consequence is worse for us than for the author of that class.** When the flag
fails for a signed cookie, one `Set-Cookie` header does not go out, and the next
request repairs the damage. When the flag fails for a server-side store, the write to
Redis never happens, no error appears, and the data is gone.

| Fault                                                                          | Reason                                                                                                                                                                                                                                                                                            | Our class                                                                                                            |
|--------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------|
| `popitem()` sets no flag                                                       | The class does not override the method.                                                                                                                                                                                                                                                           | Override it.                                                                                                         |
| `\|=` sets no flag                                                             | `dict.__ior__` changes the dictionary in C code, so it never reaches the `update()` override.                                                                                                                                                                                                     | Override `__ior__`.                                                                                                  |
| `pop()` sets `modified` but not `accessed`                                     | It runs `self.modified = self.modified or key in self` instead of calling `mark_modified()`, which sets both flags. A request that only calls `pop()` therefore sends `Set-Cookie` with no `Vary: Cookie`. Issue [#2019](https://github.com/Kludex/starlette/issues/2019) covers the same header. | Set both flags.                                                                                                      |
| A change inside a nested object, such as `session["a"]["b"] = 1`, sets no flag | **No subclass of `dict` can detect this.** The change happens inside the value, and the dictionary never sees a method call.                                                                                                                                                                      | We cannot fix it either. The store must therefore supply an explicit `save()` method, and a mode that always writes. |

The first three faults are ours to correct, and the tests in Section 5 must prove
each one. The fourth is different in kind: it is a limit of the language, so no
release of Starlette will remove it, and our answer must be an escape route rather
than a correction.

### 6.2 Why the abstraction stays in this package

Someone will ask whether we should give the vendor-neutral part to Starlette or to
FastAPI, so that other vendors can build on it. The answer is no, for two reasons.

**The maintainers already refused it, twice.** That is what
[starlette#499](https://github.com/encode/starlette/pull/499) proposed: session
backends that a user can exchange, with no vendor code in the diff. It stayed open
for approximately three years and closed with no merge. Discussion
[#2256](https://github.com/Kludex/starlette/discussions/2256) got the same answer in
2023. Nobody can say that the neutral version lacked a proposal.

**An upstream interface follows an upstream release schedule.** Faults in session
code are security faults, because they concern fixation, rotation, and cookie flags.
If our store depends on an upstream protocol, every correction to that protocol waits
for a Starlette release, and we carry `hasattr` tests for the older releases in the
meantime. Control of the interface is worth more than the neutrality we would buy.

Section 4, item (a), gives the same conclusion from other ecosystems: the store
abstraction sits between the framework and the vendor, not inside the framework.

### 6.3 Optional contributions to Starlette

**None of these blocks the release.** Section 5a removed that condition: we write our
own class, so no fault upstream reaches our store. Do this work because it is right
for the ecosystem, and because a Redis maintainer in these discussions earns the
goodwill that the documentation PRs in Section 8.7 will need.

- **Support [starlette#3436](https://github.com/Kludex/starlette/pull/3436).** An
  external contributor opened it on 2026-08-10 to correct the `popitem()` and `|=`
  faults in the upstream class, and it is still open. Review it. **Do not write a
  second PR for the same problem.**
- **Open a PR for the `pop()` fault** in the table above. It needs two lines and one
  test, and #3436 does not cover it.
- **Send the documentation PRs after the release**, as Section 8.7 describes.

---

## 7. Store contract: an ABC for the lifecycle, a Protocol at the interface

For the goal "let other vendors extend this later", a `typing.Protocol` looks
correct. A Postgres store or a Valkey store can then match the type without a
dependency on this package. For most interfaces this is the right choice. **For
this interface it is the wrong choice.** The reason is security, not style.

**A Protocol cannot enforce a rule.** The type checker uses the Protocol. Python
does not use it at run time. A store can satisfy `mypy` and still be unsafe.

For example, it can make session IDs with `random.random()`. It can also write the
new key in `rotate()` but not delete the old key. This second fault causes session
fixation. Section 3 shows that rotation is the defence against that attack.

**A vendor is most likely to omit the dangerous methods.** The
`revoke_all(user_id)` and `list_for_user(user_id)` methods need an index for each
user, such as a Redis SET. A vendor that finds this difficult will write `pass` or
`return []`. The type checker accepts that code, but the security control then
does nothing. An empty list tells the application that the user has no other
sessions. This result is worse than a method that raises an error, because the
application cannot see the difference from a correct answer.

**A Protocol also contradicts the OWASP guidance** in Section 3:
*"recommended to use these built-in frameworks versus building a home made one
from scratch"*. A Protocol gives one implementation of fixation defence for each
vendor. It also gives one implementation of the two-clock TTL calculation for each
vendor. A base class gives one implementation in total, and the authors write and
review it one time.

**The code in `telemetry.py` gives the final reason.** That module keeps its state
in a global object, `_state`. Its helper functions are free functions, and the
backend methods call them. For example, `CacheBackend.get()` contains calls to
`cache_span`, `record_cache_request`, and `timed_operation`. The telemetry is
inside the method, not around it.

With a plain Protocol, a store that does not use Redis has two options. It can
import `redis_fastapi.telemetry`, but then it depends on this package again, and
the Protocol has no purpose. Or it can emit nothing, and then
`redis_fastapi.sessions.operations{operation="rotate"}` reports no data. For a
security metric, no data looks the same as no rotations.

### Decision

- **`SessionStore(ABC)` contains the lifecycle code.** It makes IDs with
  `secrets.token_urlsafe(32)`. Its `rotate()` method is a template method. That
  method keeps a fixed order:
    1. Write the new key.
    2. Move the data to the new key.
    3. Delete the old key.
    4. Update the index of the user.

    The class also calculates the idle TTL and the absolute TTL. It contains the
    telemetry calls, so every store gets the instrumentation. The rule against a
    raw session ID applies at one set of calls. No vendor can change it.
- **Declare only the storage methods abstract:** `_read`, `_write`, `_delete`,
  `_expire`, `_index_add`, and `_index_members`. The interface for a vendor stays
  simple on purpose.
- **Use capability flags. Do not accept empty methods.** Add
  `supports_user_index` and similar flags. The middleware then refuses to offer a
  control that the store cannot supply. It does not return an answer that misleads
  the application.
- **Also declare a small `SessionStoreProtocol`.** It describes only the methods
  that the middleware in `sessions.py` and the dependency injection actually call.
  Tests and other developers can then supply an object without inheritance, and
  the middleware never uses the concrete Redis class as a type. This is the part
  that is truly vendor-neutral. The `dependency_overrides` behaviour works at the
  dependency injection level, so this decision does not change it.

**The existing code in this repository supports the same division.** The
repository has one Protocol today: `Coder`, in
`src/redis_fastapi/types.py:15`. It has two methods, it holds no state, and it
only converts a value. Its default implementation, `JsonCoder`, does *not* inherit
from it. The classes that contain behaviour are plain classes: `CacheBackend` and
`RateLimitBackend`. So the repository uses a Protocol for a simple interface, and
a normal class for behaviour.

A session store contains behaviour, and that behaviour is part of security. The
repository has no ABC today, so an ABC is a new pattern here, and the
specification must say so. But the cache code and the rate limit code hold no rule
whose failure becomes a security vulnerability.

### Result: compatibility needs an adapter, not a smaller contract

Difference 4 in Section 5 uses compatibility to get adoption. That goal suggests
one more step: give our contract the same shape as the store interface in
`starsessions`. Their `SessionStore` abstract base class has three methods:

```python
async def read(self, session_id: str, lifetime: int) -> bytes
async def write(self, session_id: str, data: bytes, lifetime: int, ttl: int) -> str
async def remove(self, session_id: str) -> None
```

**The benefit is real.** A vendor writes one class, and that class then works with
both packages. Structural typing needs no inheritance, so an existing
`starsessions` store for Postgres or for Memcached would satisfy our Protocol
without any change. Today a vendor must choose one package or write two classes.
Most vendors write for the larger ecosystem, and that is not us.

**But do not copy the contract.** There are two reasons. The first is the shape of
the contract itself: **a store that is not Redis dictates it.** The `lifetime`
parameter exists for `CookieStore`, which needs it as the `max_age` of the signer.
Their own Redis store ignores that argument in `read()`. If we copy the contract, we
accept a limit that a cookie store created, inside a package that integrates Redis.
Section 8.3.4 records this.

The second reason: the contract cannot express three operations that Section 3
requires.

- **The contract has no `rotate()`.** The caller must build the defence against
  fixation from three calls: `read` the old key, `write` the new key, then
  `remove` the old key. Those calls are not atomic. Between the write and the
  remove, two IDs give access to the same authenticated session. If the process
  stops, or the caller ignores an error from `remove`, the old ID stays valid.

    **Our `rotate()` needs no transaction either, and it is still correct.** A
    session write touches more than one key, and on Redis Cluster those keys sit in
    different slots, so no Lua script and no `MULTI` can hold them together. We order
    the operations instead: **delete the old key before writing the new one.** A
    process that stops in the middle then signs the user out, which is safe, and no
    interruption can leave two IDs valid at once. The order gives the property that a
    transaction would have given, and it gives it on a cluster too. Section 5 of
    [`session-design.md`](session-design.md) gives the sequence.

    `starsessions` shows the risk in its own code. Its `regenerate_id()` method
    keeps the old ID in `_remove_data_for_session` and deletes it only at the next
    `save()`. If `save()` never runs, the old ID survives until the absolute
    timeout. In the Redis store, that timeout becomes `gc_ttl` when `lifetime` is
    zero, and the default value of `gc_ttl` is 30 days.
- **The two time parameters describe one clock.** `lifetime` is the total session
  duration. `ttl` is the time that remains under that same duration, because the
  caller computes it as `(created + lifetime) - now`. Neither value is an idle
  timer, and the contract has no `touch` method. Their Redis store also ignores
  the `lifetime` argument to `read()`, so a read refreshes nothing. To add an idle
  timeout you must keep `last_access` in the payload and test it after you decode
  the payload. Redis still holds the key, and only Python refuses the session.
  This result contradicts the OWASP requirement,
  *"expiration must be enforced server-side."*
- **The contract has no index for each user.** Every method takes only a
  `session_id`. Nothing records that a session belongs to a user, so `revoke_all`
  is not weaker. It is impossible. Section 5 calls that capability the clearest
  reason to use Redis.

The correct position is an adapter, in one direction only. Keep our own contract.
Write a small adapter that maps their three methods onto our storage primitives:

- `read` to `_read`
- `write` to `_write`
- `remove` to `_delete`

Our `rotate()` then still deletes the old key before it returns. For the
primitives that they cannot supply, set the capability flags to `False`:
`supports_idle_ttl` and `supports_user_index`. `revoke_all()` then raises a clear
error. It does not return an empty list that misleads the application.

Their stores therefore work with our middleware. Our stores do not work with
`starsessions`, and that is the cost of this decision. It is the smaller cost.
Document how to migrate.

---

## 8. The scope of version 1

Version 1 must be a **minimum solution that works**. It must also be complete
enough to replace the packages that people use today. This section says what that
requirement means in practice, after we read the source of both packages.

### 8.1 We do not replace `fastapi-users`. Do not claim that we do.

`fastapi-users` is a framework for user management. It supplies registration,
password hashing, password reset, email verification, links to OAuth accounts,
adapters for several databases, and a user manager. We replace exactly one layer of
it: `RedisStrategy` and `CookieTransport`.

State this limit in the documentation. If we claim more, we invite a comparison
against features that nobody expects from a session store, and we lose it.

### 8.2 What `RedisStrategy` is, in 33 lines

We read the complete file. The strategy stores **only** `str(user.id)`, under an
opaque key from `secrets.token_urlsafe()`, with `ex=lifetime_seconds`. The
consequences:

- It holds **no session payload**. There is nowhere to put a shopping cart, a
  wizard step, or OAuth state.
- It never refreshes the key when it reads it, so there is **no idle timeout**. The
  `ex` argument gives an absolute timeout only.
- It has **no rotation** for a change of privilege inside a session.
- It has **no index for each user**, so `revoke_all` is not possible.
- `lifetime_seconds` defaults to `None`. With the default, **the token never
  expires**.

Its `CookieTransport` is correct in the parts that matter: it uses `APIKeyCookie`,
so the scheme reaches OpenAPI, and its defaults are `secure=True`,
`httponly=True`, and `samesite="lax"`.

Parity with this strategy is therefore simple. We are better on every point above.
We are worse on one point only: we do not return a user object, because we do not
own the user model.

### 8.3 A complete comparison against `starsessions`

We read the full source: `middleware.py`, `session.py`, `serializers.py`,
`encryptors.py`, `exceptions.py`, `types.py`, and the four stores. An earlier draft
of this section read only `__init__.py`, and it therefore missed most of the list
below. The package root does not export the encryptors at all.

**The comparison covers the features of a Redis backend, and no other kind.** This
package integrates Redis. A feature that belongs to a different backend is not a gap
for us, and we must not treat it as one. The correct goal is different: a user must
be able to move **to** Redis with very little work. Section 8.3.4 lists the items
that are outside our scope, with the reason for each one.

In the tables: **✓** means that the specification covers it. **~** means that the
specification covers it in part, and the text needs a correction. **✗** means that
the specification does not cover it, and that this is a gap we must close.

#### Parameters of `SessionMiddleware`

| `starsessions` | Us | Note |
|---|---|---|
| `store` | ✓ | The `SessionStore` ABC in Sections 5 and 7. |
| `lifetime` (`int` or `timedelta`) | ~ | We have an absolute TTL. **It must also accept a `timedelta`.** Their parameter does, and `cache()` in this repository does. |
| `lifetime=0`, a session-only cookie | ✗ | **Gap.** No `max-age`, so the browser deletes the cookie when it closes. This also needs a `gc_ttl` value for the Redis key. See 8.3.1. |
| `rolling` | ~ | Our idle TTL gives a similar result, but not the same one. See 8.3.2. |
| `cookie_name` | ✓ | |
| `cookie_same_site` | ✓ | |
| `cookie_https_only` | ✓ | Both packages set `Secure` by default. |
| `cookie_domain` | ✗ | **Gap.** Section 5 does not list it. |
| `cookie_path` | ✗ | **Gap.** Section 5 does not list it. They also limit the deletion of a cookie to that path. |
| `serializer` | ✓ | Section 5 resolves this: use `Coder`, at `src/redis_fastapi/types.py:15`. |
| `encryptor` | ✗ | **Gap. This is a complete subsystem.** See 8.3.1. |

#### Behaviour

| `starsessions` | Us | Note |
|---|---|---|
| Reject an unsafe cookie value before use (`_SAFE_COOKIE_VALUE_RE`) | ✗ | **Gap, and it is a security control.** It prevents header injection when the code writes the ID into `Set-Cookie`. The cost is one regular expression. Copy it. |
| Validate `cookie_name` in the constructor | ✗ | Small. Copy it. |
| Delete the cookie **and** the record when a session becomes empty | ~ | Our `revoke()` implies this. Write it down. |
| Write nothing when an empty session stays empty (`initially_empty`) | ✓ | Our rule to write only when the data changes is stronger. |
| `SessionAutoloadMiddleware`, with paths and regular expressions | ✓ | We need no equivalent. Our design loads the session when code touches it, which is difference 1 in Section 5. That is better than a flag plus a `SessionNotLoaded` exception. |
| `LoadGuard` and `SessionNotLoaded` | ✓ | Absent on purpose, for the same reason. |

#### Public functions

| `starsessions` | Us | Note |
|---|---|---|
| `generate_session_id()`, `token_hex(16)`, 128 bits | ✓ | We use `token_urlsafe(32)`, which gives 256 bits. |
| `regenerate_session_id()` | ✓ | Our `rotate()`. Ours is atomic; Section 7 shows that theirs is not. |
| `get_session_id()` | ~ | Name it in the specification. |
| `load_session()` and `is_loaded()` | n/a | Our load is automatic. |
| `get_session_metadata()`, with `lifetime`, `created`, `last_access` | ~ | **Use the same three field names.** A migration is then a rename and not a redesign. |
| `get_session_remaining_seconds()` | ~ | The same. |
| `get_session_handler()` | n/a | Its own docstring says "private API, no backward compatibility guarantee". Ignore it. |

#### Stores, serializers, encryptors, and exceptions

| `starsessions` | Us | Note |
|---|---|---|
| The `SessionStore` ABC: `read`, `write`, `remove` | ✓ | Section 7, with the adapter. |
| `RedisStore(connection=…)` | ✓ | Ours uses the connection pool of the SDK. |
| `prefix`, a string **or a callable** | ~ | Section 5 says "key prefix". **It must also accept a callable.** Their documentation advertises this. |
| `gc_ttl` | ✗ | **Gap.** Necessary when `lifetime` is zero. |
| `InMemoryStore` | n/a | Outside our scope. See 8.3.3 and 8.3.4. |
| `CookieStore` | n/a | Outside our scope. See 8.3.4. |
| `Serializer` and `JsonSerializer(json_encoder, json_decoder)` | ✓ | `Coder` replaces both. Section 9 gives the translation. |
| **`Encryptor`, `NoopEncryptor`, `FernetEncryptor`, `AESGCMEncryptor`** | ✗ | **The largest gap.** See 8.3.1. |
| `SessionError`, `SessionNotLoaded`, `ImproperlyConfigured` | ✗ | **Gap.** The specification defines no exceptions. Section 9 defines them. |

#### 8.3.1 Encryption of the data at rest

`starsessions` accepts an `encryptor`, and it supplies Fernet and AES-GCM. Our
specification says nothing about encryption.

The row "Keep the data away from the client" in the Section 3 table is not the same
statement. A server-side store keeps the data away from the browser, but the data is
then plaintext in Redis. It also reaches the RDB file, the AOF file, every replica,
and every backup or snapshot that a managed service makes. For a session that holds
personal data or an OAuth token, under a rule such as the GDPR, that difference is
the whole point.

**Decision: supply the connection point, and document the implementation. Write no
cryptographic code in version 1.**

Define an `Encryptor` protocol with two methods, `encrypt(bytes)` and
`decrypt(bytes)`. That is approximately five lines. Then write a recipe in the
documentation that implements it with AES-GCM from the `cryptography` package, in
approximately ten lines. Ship no implementation, and add no `cryptography` extra.

The reason: cryptographic code that we ship is cryptographic code that we own, that
we must review, and whose vulnerabilities we must track and announce. Ten lines in
the documentation give the user the same result and keep that duty where the
`cryptography` project already discharges it. **The cost is real and we must state
it: a user who wants encryption writes ten lines instead of setting one flag.** If
users ask for a shipped implementation, promote the recipe into code and add the
extra then.

Two details from their code that the recipe must respect:

- **Do not copy `NoopEncryptor`.** It calls `warnings.warn()` inside `encrypt()`, so
  it warns on every request. A warning at that rate gets filtered, and then nobody
  reads it. Use `None` as the default value instead.
- **Use AES-GCM, not Fernet.** AES-GCM gives authenticated encryption in one
  operation. Their Fernet path is AES-128-CBC with a separate HMAC. The protocol
  stays public, so a user who prefers Fernet can still supply it.

#### 8.3.2 "Rolling" and "idle" are two different behaviours

Their `rolling=True` extends **both** the `max-age` of the cookie **and** the TTL of
the record by the complete `lifetime`, on every response. Their `rolling=False`
keeps the original expiry time and sends the seconds that **remain** as `max-age`.

Our idle TTL refreshes the key in Redis. The specification does not say what happens
to the `max-age` of the cookie.

**These two clocks must agree.** If they do not, the browser deletes a cookie while
the record in Redis is still alive. The user then sees a logout with no cause.

Write both clocks against both carriers, and give the translation: their `rolling=True`
becomes our idle TTL, and their `rolling=False` becomes our absolute TTL.

#### 8.3.3 Do not ship an `InMemoryStore`. Use `fakeredis` in tests.

An earlier draft argued for a store of this kind, because a `starsessions` user runs
`InMemoryStore` in the tests, and because such a store lets `pytest` run with no
Redis container. **The second reason is already false in this repository**, and the
first reason then disappears with it.

This repository solves the same problem, and it solves it better.
`tests/conftest.py:16` imports `fakeredis`, and `noxfile.py:102` describes the
`tests_unit` session as "the fakeredis-backed unit suite. Needs no Redis server."

`fakeredis` is the better answer for a session store, not only an equal one. It runs
our **real** code: the real key schema, the real TTL commands, and the real index built
on hash field expiration. `fakeredis` supports every one of those commands, which we
verified before we settled the design. An in-memory session store runs none of that, so a test suite that passes
against it proves less than it appears to prove. Two ways to reach an empty test
database is one way too many, and the weaker way is the one that hides faults.

**Decision: ship no in-memory store.** Instead write a recipe that shows a test with
`fakeredis`, and point to `tests/conftest.py` in this repository as the example that
we ourselves use.

#### 8.3.4 What is outside our scope, and why

This package integrates Redis. The items below belong to a different backend. They
are **not** gaps, and no later release must close them.

| Item | Why it is not ours |
|---|---|
| `InMemoryStore` | It is not a Redis feature. `fakeredis` covers the test case, and it covers it better. See 8.3.3. |
| `CookieStore` | It is not a Redis feature. The Starlette middleware already is a cookie store, and Section 5a explains that it is competent for that one job. A user who wants a cookie store must keep it. |
| The `lifetime` and `ttl` pair in their `write()` | Section 7 already refuses this contract. Here is the sharper reason: the shape exists **because of `CookieStore`**, which needs `lifetime` for the `max_age` of the signer. Their own Redis store ignores the `lifetime` argument to `read()` completely. If we copy the contract, we accept a limit that a store which is not Redis created. |

Our `SessionStoreProtocol` must still be wide enough to accept a store of any of
these kinds from a user. Section 9 lists it as a connection point. We do not write
one, but we do not prevent one.

### 8.4 Move the index for each user into version 1

Section 5 puts `list_sessions` and `revoke_all` in a later release. **Move the
binding to a user, and the index, into version 1.** The two methods can still
appear later.

The reason is the data, not the code. If we add the index afterwards, every session
from before that release has no entry in it. `revoke_all` then reports success and
removes nothing, and `list_sessions` hides a live session. Section 7 rejects
exactly this failure: an empty answer that the application cannot distinguish from
a correct one. Here we would cause it ourselves.

The work in Redis is small. Use a **hash with a TTL on each field**: one field for each
session, and the TTL of that field is the absolute deadline of the session. Redis then
deletes the entry when the session dies.

**Do not use a plain set.** An earlier draft did. Most sessions end because their TTL
runs out, and Redis calls nobody when a key expires, so a set keeps a member for every
session that ever timed out. It grows without limit, and `list_sessions` then reports
sessions that do not exist.

A second draft used a sorted set scored by expiry, which is correct but which still asks
us to prune. **Hash field expiration, which Redis added in 7.4, removes even that.** This
package already requires Redis 7.4, so we may use it. `HGETALL` returns the live sessions
and nothing else, `HLEN` counts them for a limit on concurrent sessions, and the value of
each field can hold a descriptor, so the "your active sessions" screen costs one round
trip. Section 3.3 of [`session-design.md`](session-design.md) gives the complete design,
and Section 13 explains why no other backend can copy it.

Do this while the key schema is still free.

### 8.5 The contents of version 1

Divide the work by the answer to one question: can a user add this later, without
our help?

- **Core.** No, the user cannot. It must be in the middleware or in the store.
- **A connection point.** Yes, but only if we expose a seam. Each seam is a few
  lines, so every seam belongs in version 1. A feature that we do not write must
  never become a feature that nobody can write.
- **A recipe.** Yes, with the seams that already exist. It needs no code from us,
  only documentation. Therefore it also belongs in version 1.

#### Core, P0. The release means nothing without these.

- `SessionStore` (ABC) and `RedisSessionStore`
- our `SessionMiddleware`, with a signed opaque ID in the cookie
- our own `Session` class, which tracks `popitem()` and `|=`
- a load that happens with no call from the user, and only when the request carries a
  session cookie. **An earlier draft said "only when code touches the session". No
  implementation can do that**, because `HTTPConnection.session` is a synchronous
  property and cannot await a Redis read. Section 1.1 of
  [`session-design.md`](session-design.md) gives the evidence and the corrected rule.
  The user still calls nothing, which is the promise in difference 1 above.
- a write that happens only when the data changes
- an idle TTL **and** an absolute TTL, with the clock of the cookie and the clock of
  Redis in agreement. Section 8.3.2 explains the failure if they disagree.
- automatic rotation when the principal changes, with the ordering guarantee beneath it
- `revoke()`
- strict cookie defaults
- the validation of the session ID that arrives in the cookie
- `SessionDep`, `SessionStoreDep`, and `get_session_store`
- the `REDIS_SESSION_*` settings, and `.sessions()` on the builder

#### Core, P1. Necessary to replace the packages that people use today.

- the binding to a user **and** the index for each user (Section 8.4)
- the settings for the cookie domain and the cookie path
- the session-only mode, with `gc_ttl` for the Redis key
- the OpenAPI scheme through `APIKeyCookie`
- the accessors for the metadata, with the field names of `starsessions`
- serialization through the existing `Coder`
- the telemetry from Section 5
- the exception hierarchy from Section 9.2
- compatibility with Authlib, which needs no work. It follows from the
  `scope["session"]` contract.
- **support for a `def` endpoint as well as an `async def` one.** Reading and writing a
  session already needs no bridge, because the session is a dictionary and the
  middleware does the input and output. The imperative store operations need
  `SyncSessionStore` and `SyncSessionStoreDep`, which follow `SyncCacheBackend` and
  `SyncRateLimitBackend` exactly. See the "Sync endpoints" part of Section 9 in
  [`session-design.md`](session-design.md).

#### Connection points. All of them, because each one is small.

Section 9.1 gives the complete table: `Coder`, `Encryptor`, `SessionStoreProtocol`,
the key prefix as a string or a callable, the factory for a session ID, the
identifier for the index, and the builder for the cookie.

#### Recipes. Documentation only, and no code from us.

Section 9.6 gives the complete list. It includes an encryptor with AES-GCM, tests
with `fakeredis`, the four migrations, `Partitioned` and `__Host-` cookies, CSRF,
the check of the IP address and the User-Agent, and `Clear-Site-Data` at logout.

**The last two arrived here from the excluded list.** An earlier draft excluded both
from version 1. That was wrong. The first compares two values that the session
payload already holds. The second sets one response header. Neither needs code that
we are not writing anyway, so neither has a reason to wait.

#### Excluded from version 1

Every item below is excluded because it needs code that we choose not to write now,
and not because it is small:

- the carrier for agents and MCP
- the adapter for `starsessions` stores, from Section 7
- the reader that migrates a live `starsessions` session, from Section 9.3.3

**`SyncSessionStore` was on this list, and it should not have been.** The reason given
was that the middleware is asynchronous, so a synchronous store would serve only
imperative calls inside synchronous endpoints. That argument assumed such calls were
rare. They are not: Section 9 of [`session-design.md`](session-design.md) makes
imperative store calls the way an application signs a user in, signs them out, lists
their devices and caps their sessions. FastAPI serves a `def` endpoint as readily as an
`async def` one, so excluding the facade excluded every one of those operations from
half of the framework's users.

The exclusion also contradicted itself. It noted that the absence was "visible, because
`SyncCacheBackend` and `SyncRateLimitBackend` exist" — that is an argument for building
it, not for deferring it. Two of three features shipping a synchronous facade and the
third not is exactly the kind of inconsistency this specification tries to avoid.

Section 8.3.4 lists what is outside our scope permanently. Do not confuse that list
with this one.

### 8.6 The answer to the open question

**Build the web sessions first, and keep the store independent of the transport.**

Section 7 already makes this possible. The base class holds the complete lifecycle,
and no part of it touches the transport:

- the session IDs
- the rotation
- the idle TTL and the absolute TTL
- the index for each user
- the telemetry

Only the cookie carrier belongs to the web, and it lives in `sessions.py` and not
in the store. Support for agents and MCP is then a second carrier that reads the
session ID from a header or from an argument. It is not a rewrite.

One question stays open, and it is not a technical question. Session state for
agents overlaps with `redis/agent-memory-server`. The product managers must decide
where that feature belongs.

### 8.7 Next actions

1. Write the code for version 1, as Section 8.5 defines it. Nothing upstream blocks
   this work.
2. Make the two optional contributions to Starlette, as Section 6.3 lists them:
   support #3436, and open the PR for the `pop()` fault. Neither one blocks us,
   because we write our own `Session` class.
3. After the release, add the SDK to the third-party pages of Starlette and FastAPI.
   One task there has more value than the listing itself: those pages still send
   approximately 70k downloads each month to the **archived** `fastapi-sessions`
   package, as Section 2 shows. That is a supply chain risk, and nobody will argue
   against a correction.

---

## 9. Extension points, exceptions, and how to change from another package

Section 8 sets two goals. A user of another package must be able to change to this
one with very little work. And if we do not supply a feature, the user must be able
to supply it, with a callback or an extension, and never with a fork.

This section is the contract for both goals.

### 9.1 The extension points

Each row is a connection point that we make public and that we keep. If you need to
add a row later, that is a sign that the design is too closed.

| Connection point | Type | It lets a user replace |
|---|---|---|
| `Coder` (`src/redis_fastapi/types.py:15`) | Protocol | the serialization: a custom JSON encoder or decoder, msgpack, or a compressed format |
| `Encryptor` | Protocol | the encryption at rest: Fernet, a key from a KMS, or the rotation of a key |
| `SessionStore` (ABC) and `SessionStoreProtocol` | ABC and Protocol | the complete storage: Postgres, Valkey, or Memcached. Section 7 explains the two types. |
| the key prefix | `str` or `Callable[[str], str]` | the names of the keys: one space for each tenant, or a hash tag for OSS Cluster |
| the factory for a session ID | `Callable[[], str]` | the format of an ID, if a company standard demands one |
| the builder for the cookie | a callable, or every attribute passed through | **any cookie attribute that we did not plan** |
| the identifier for the index | `Callable[[Session], str \| None]` | the subject of the index: a tenant, a device, or an API client, and not only a user |
| the trigger for a rotation | `principal_keys: list[str]`, or a callable | **what counts as a privilege change.** Defaults to the identity alone; add a role or a scope list and an escalation rotates by itself. Section 5.1 of [`session-design.md`](session-design.md). |

**The builder for the cookie is more important than it appears.** Section 2a records
that the Starlette middleware cannot send `Partitioned`, and that it cannot use a
`__Host-` prefix. Those two limits are a common reason to stop using it. A
connection point here means that we never repeat that fault. A user adds the
attribute; the user does not wait for our next release.

### 9.2 The exceptions

`starsessions` defines three exceptions, and this specification defined none. Define
these, so that a caller can catch every fault from this feature as one group:

- `SessionError`, the base class for every exception below.
- `SessionConfigurationError`, for a setting that is absent or wrong. This is the
  equivalent of their `ImproperlyConfigured`.
- `SessionStoreError`, for a store that fails. Wrap the error from the driver;
  do not let a `redis.RedisError` reach the application code directly.

We need no equivalent of their `SessionNotLoaded`. Our session always loads when
code touches it, which is difference 1 in Section 5.

**Decide the behaviour when the store is unavailable, and write it down.** The rate
limit code in this repository already has a `fail_closed` setting for the same
question. Sessions must make the same choice explicit: if Redis is unreachable,
does the request continue with an empty session, or does it fail? Do not leave this
to an exception that escapes by accident.

### 9.3 Where users arrive from

Order the migration documentation by the value to the user, and not by the name of
the competitor. Four sources matter, and the first one matters most.

**One rule applies to all four: the sessions that exist do not survive the change.**
Our key format, the position of the metadata, and the index for each user differ
from every source below. Every signed-in user signs in again. State this at the top
of each guide. A team must not find it in production. Recommend a release at a quiet
time.

#### 9.3.1 From a session store that holds data in one process

**This is the most important guide, and the specification did not have it.**

The user has sessions in the memory of the process. That covers the Starlette
middleware with small payloads, `starsessions` with `InMemoryStore`, and a dictionary
that somebody wrote by hand. The application works, and it works until the day the
team starts a second worker or a second pod. Then a user signs in on one instance and
the next request reaches the other one, which has never heard of that session.

That day is the reason this feature exists. Section 4 gives the same history for
Spring Session Data Redis. Write the guide for the person who is having that day:

- Name the symptom first: a user signs out at random, and the rate of it grows with
  the number of instances. That is what the person will search for.
- Show that the change removes the need for sticky sessions at the load balancer, so
  the balancer returns to a plain round robin.
- Show that a session then survives a restart and a deployment.
- Note the one new duty: Redis becomes a dependency of the request path. Point to the
  decision in Section 9.2 about the behaviour when the store is unavailable.

#### 9.3.2 From the Starlette signed cookie

This is the largest group of users, and most of them arrived through Authlib.

For them the change is almost free. Section 5a explains why: we keep the
`scope["session"]` contract, so `request.session` behaves as before and Authlib needs
no change at all. The user replaces one middleware.

Name the two faults that disappear at the same time, because Section 2a shows that
these are what people actually suffer from:

- The limit of 4096 bytes disappears. A payload larger than that no longer breaks
  the login without an error message.
- The race condition on `Set-Cookie` disappears, and with it one cause of
  `mismatching_state`.

#### 9.3.3 From `starsessions` with `RedisStore`

The user already has the correct architecture. Only the package changes. The table in
Section 9.4 translates every setting.

A reader for their format is possible, and it would remove the one interruption. It
would read their key, migrate the session at the first request, and write it again in
our format. It costs approximately 30 lines. Section 8.5 excludes it from version 1,
because it is code that we choose not to write yet. Add it if users ask for a
migration with no sign-out.

#### 9.3.4 From the `RedisStrategy` of `fastapi-users`

See Section 9.5.

### 9.4 Translation of the settings from `starsessions`

This table is the most valuable part of that migration, and it costs us nothing.

| `starsessions` | This SDK | Note |
|---|---|---|
| `store=RedisStore(connection=…)` | `.sessions()` | We use the connection pool of the SDK. |
| `lifetime=N` | the absolute TTL | Both accept an `int` or a `timedelta`. |
| `lifetime=0` | the session-only mode | Set `gc_ttl` for the Redis key. |
| `rolling=True` | the idle TTL | Section 8.3.2 explains the two clocks. |
| `rolling=False` | the absolute TTL alone | |
| `cookie_name` | the cookie name | The same meaning. |
| `cookie_same_site` | the `SameSite` value | The same meaning. |
| `cookie_https_only` | the `https_only` flag | Both default to on. |
| `cookie_domain`, `cookie_path` | the cookie domain, the cookie path | The same meaning. |
| `serializer=JsonSerializer(...)` | `Coder` | A custom `json_encoder` becomes a custom `Coder`. |
| `encryptor=FernetEncryptor(key)` | the `Encryptor` connection point | Use the AES-GCM recipe, or supply their Fernet class. Section 8.3.1. |
| `prefix="x."` or a callable | the key prefix | We accept both forms. |
| `gc_ttl` | `gc_ttl` | The same meaning. |
| `regenerate_session_id()` | `rotate()` | Ours is atomic. Section 7 gives the difference. |
| `load_session()`, `is_loaded()` | nothing | Our load is automatic. Delete these calls. |
| `get_session_metadata()` | the accessor for the metadata | The same three field names. |
| `get_session_remaining_seconds()` | the same name | |
| `InMemoryStore` | `fakeredis` in the tests | Section 8.3.3. Outside our scope in production. |
| `CookieStore` | nothing | Outside our scope. Keep the Starlette middleware for this case. |
| `SessionAutoloadMiddleware` | nothing | Delete it. Our load is automatic. |

### 9.5 Changing from the `RedisStrategy` of `fastapi-users`

Section 8.1 states the limit: we replace `RedisStrategy` and `CookieTransport`, and
we replace nothing else. The user keeps `fastapi-users` for registration, for
passwords, and for OAuth.

Their record maps a token to a user ID and holds nothing else. So the migration has
one direction that is simple: our session holds the user ID in the same way, and it
can also hold everything else. As above, the sessions that exist do not survive the
change.

### 9.6 The recipes for version 1

A recipe is documentation, and it needs no code from us. Each one uses a connection
point from Section 9.1. Section 8.5 puts all of them in version 1 for that reason.

| Recipe | It uses | Approximate size |
|---|---|---|
| An encryptor with AES-GCM | the `Encryptor` connection point and the `cryptography` package | 10 lines. Section 8.3.1. |
| Tests with `fakeredis` | `dependency_overrides`, which this package already supports | Point to `tests/conftest.py`. Section 8.3.3. |
| The four migrations | — | Section 9.3. |
| `Partitioned` and `__Host-` cookies | the builder for the cookie | 5 lines. It corrects the limit in Section 2a. |
| CSRF for a cookie session | `fastapi-csrf-protect` | Section 5 already requires this text. |
| A check of the IP address and the User-Agent | two values in the session payload | 10 lines. Repeat the OWASP warning from Section 3: it detects, and it does not defend. |
| `Clear-Site-Data` at logout | one response header | 2 lines. |
| One key space for each tenant | the key prefix as a callable | 3 lines. |
| A hash tag for OSS Cluster | the key prefix as a callable | 3 lines. |
| An index by device instead of by user | the identifier for the index | 5 lines. |

**Write the recipes as tested code.** Put each one in `examples/`, or in a test that
`nox` runs. A recipe that nobody runs stops working at the first release that changes
a name, and then it damages the user more than an absent feature does.
