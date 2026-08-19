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
   `rotate()`, which defends against session fixation at login and after a
   privilege change. Give **separate idle and absolute TTLs**. Give `revoke()`.
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

### Structure, which follows the existing conventions of the SDK

Use the same division as the existing cache and rate limit code:

| New file | Follows | Contents |
|---|---|---|
| `src/redis_fastapi/sessions.py` | `cache.py`, `ratelimit.py` | `SessionMiddleware`, the `session()` factory for dependency injection, and `add_redis_sessions()` |
| `src/redis_fastapi/session_backend.py` | `cache_backend.py`, `ratelimit_backend.py` | `SessionStore`, an abstract base class (ABC) that owns the lifecycle. Also `RedisSessionBackend` and `SyncSessionBackend`, with these methods: `load`, `save`, `delete`, `rotate`, `touch`, `list_for_user`, and `revoke_all`. Section 7 gives the reason for an ABC instead of a plain `Protocol`. |

Extend the existing files. Do not write the same code again.

- `src/redis_fastapi/setup.py`: add `.sessions()` to the `FastAPIRedis` chain.
- `src/redis_fastapi/deps.py`: add `SessionDep`, `SessionBackendDep`, and
  `get_session_backend`. Put them beside the existing `get_cache_backend` and
  `get_rate_limit_backend`. Keep the `dependency_overrides` behaviour, because
  tests need it.
- `src/redis_fastapi/config.py`: add `REDIS_SESSION_*` settings to
  `RedisSettings`. These settings control the cookie name, the idle TTL, the
  absolute TTL, the `SameSite` value, the `https_only` flag, the key prefix, and
  the rotation policy.
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
  Write an application in `examples/` that shows a login, then `rotate()`, then a
  logout, then `revoke_all()`. Write a second example for an Authlib OAuth flow.

### Security requirements. Treat these as acceptance criteria.

- **Declare a direct dependency on `starlette>=1.0.0`.** The design uses
  `Session.accessed` and `Session.modified`. Starlette added these flags in
  **version 1.0.0 (2026-03-22)**, because #3166 went into that release. Today
  `pyproject.toml` declares no direct dependency on Starlette. Starlette arrives
  through `fastapi>=0.115.0`, and `uv.lock` selects version 1.3.1. The current
  specification therefore permits a 0.3x version of Starlette, which has no
  `Session` class. Without a minimum version, the store writes nothing when the
  data changes.
- Make session IDs with `secrets.token_urlsafe(32)`, which gives at least 128
  bits. Keep the IDs opaque. Never put data into an ID.
- Call `rotate()` after authentication and after a privilege change. Delete the
  old key and move the data to the new key.
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
    - `rotate()` keeps the data and makes the old key invalid.
    - The idle timeout and the absolute timeout work independently.
    - `revoke_all` removes every session of one user.
    - The store writes nothing to Redis if no code touched the session. Assert on
      the `modified` flag.
    - The response has a `Vary: Cookie` header if code read the session.
- **Do not trust the `modified` flag.** Section 6.1 explains that the flag does not
  report every change today. The tests must show that the store keeps the data
  after each of these three operations:
    - `popitem()`
    - `|=`
    - a change inside a nested object, such as `session["a"]["b"] = 1`

    No `dict` subclass can detect the nested change, so nobody can correct it
    upstream. The store therefore needs an explicit `save()` method, and an
    optional mode that always writes. These tests must pass even if the
    maintainers never merge #3436.
- Write integration tests in `tests/integration/` against a real Redis server.
  Test that two application instances share one session through one Redis server.
  Test the TTL behaviour. Test the concurrent requests that replaced a cookie
  before #3166.
- Write a compatibility test. An Authlib OAuth flow must complete against the
  Redis store without any change. Also test a payload larger than 4KB, which a
  signed cookie cannot hold.
- Write a telemetry test. Follow the existing pattern. Assert that the instruments
  record the data. Assert that `disable_telemetry()` gives a clean `_OTelState`.
  Assert that no attribute contains a session ID.
- Run `nox`. It runs the lint, mypy, bandit, and coverage steps, and the
  repository requires all four.
- Do a manual test. Run the login application in `examples/`. Confirm the
  `HttpOnly`, `Secure`, and `SameSite` flags. Look at the keys in Redis. Confirm
  that a logout deletes them.

### Suggested order of work

Release these parts first, because they are the OWASP core:
`session_backend.py`, `sessions.py`, the dependency injection, the configuration,
the strict cookie defaults, `rotate`, and `revoke`. Then release
`list_sessions` and `revoke_all`, which control concurrent sessions, and the
Authlib compatibility example. These parts are the differences from other
packages, so give them their own release note.

---

## 6. Upstream strategy: what we propose, and what we keep

A second question came after the research above. Must we propose the
vendor-neutral part to Starlette or to FastAPI as common code, so that other
vendors can extend it later?

**Make it vendor-neutral, but keep it in this package.** The table below divides
the feature into layers, and it shows where the correct division falls.

| Layer | Vendor-neutral? | Can upstream accept it? |
|---|---|---|
| L1 The `Session` dict with the `accessed` and `modified` flags | already upstream | already present, but incomplete. See Section 6.1. |
| L2 A `Session` import that does not need the cookie middleware | a refactor only | a small PR is possible. See Section 6.3. |
| L3 A cookie carrier with a store as a parameter | neutral | **this is the diff of PR #499** |
| L4 A `SessionStore` protocol with `load`, `save`, `delete`, and `touch` | neutral. **This is the proposal.** | **refused two times** |
| L5 The OWASP lifecycle: `rotate`, two TTLs, `revoke_all`, and an index for each user | the interface is neutral | outside the scope of a feature-complete toolkit |
| L6 The Redis implementation | specific to the vendor | ours |

Layers L3 and L4 together are the proposal. They are also exactly the content of
[starlette#499](https://github.com/encode/starlette/pull/499): session backends
that you can exchange, with no vendor code in the diff. That PR stayed open for
approximately three years, and the maintainers closed it without a merge. Nobody
can say that the neutral version has no proposal. The maintainers refused the
neutral version, and they gave the same answer to
[#2256](https://github.com/Kludex/starlette/discussions/2256) in 2023.

We verified the current state in the source code, not only in the issue tracker.
Today `starlette/middleware/sessions.py` on `master` still has **no parameter for
a store, a backend, or a serializer**. The code always uses JSON, then base64,
then `TimestampSigner`. Nothing changed at layers L3 and L4.

There is a second reason to keep the interface. **An upstream interface follows
the upstream release schedule.** Faults in session code become security
vulnerabilities, because they involve fixation, rotation, and cookie flags. If our
store uses an upstream protocol, then every correction to that protocol waits for
a Starlette release. We must also support one or two older releases with
`hasattr` tests. Control of the interface is therefore an advantage.

The history of other ecosystems gives the same answer. In each ecosystem in
Section 4, item (a), the store abstraction sits between the framework and the
vendor.
`express-session` is a third-party package, and it defines the `Store` base class.
`connect-redis` implements that class. The Node core owns neither of them.

Spring puts `SessionRepository` in Spring Session, not in the Servlet
specification. Therefore make the abstraction vendor-neutral, and keep it here.

The parts that we must send upstream are much smaller. One of them is urgent.

### 6.1 Correct the `accessed` and `modified` flags that we depend on

Difference 1 in Section 5 is the rule to write to Redis only when the data
changes. That rule is correct only if the `modified` flag reports every change.
**Today it does not.** We verified this in
`starlette/middleware/sessions.py`.

The `Session` class overrides `__setitem__`, `__delitem__`, `clear`, `pop`,
`setdefault`, and `update`. It does **not** override `popitem()` and it does not
override `|=`. The `dict.__ior__` method updates the dictionary in C code, so it
does not use the `update()` override. For a signed cookie, the result is one lost
`Set-Cookie` header. For a server-side store, the result is a lost write, and the
store gives no error. The second result is much worse.

[starlette#3436](https://github.com/Kludex/starlette/pull/3436) already corrects
this. An external contributor opened it on 2026-08-10, and it is still open.
**Review that PR and support it. Do not write a second PR for the same problem.**
An open PR changes the exact behaviour that our design uses. A comment from a
Redis maintainer in that discussion has more value than a proposal that the
maintainers will refuse.

### 6.2 A new PR: `pop()` sets `modified` but never sets `accessed`

The `pop()` method runs `self.modified = self.modified or key in self`. The
`mark_modified()` method sets *both* flags. A request that only calls `pop()`
therefore sends `Set-Cookie` without `Vary: Cookie`. This result is different from
every other method that changes the data. The `Vary` header is also the subject of
[#2019](https://github.com/Kludex/starlette/issues/2019). The correction needs two
lines and one test, and it is outside the scope of #3436.

### 6.3 A proposal: a neutral import path for `Session`

Propose `starlette.datastructures.Session`, or a new `starlette.sessions` module.
Keep the existing name as an alias. Give this argument: a third-party store must
import from `starlette.middleware.sessions` today. That import loads the
itsdangerous cookie middleware, and the store needs only the type. The core code
shows that the type is already public in practice.
`starlette/requests.py:169-175` imports `Session` under `TYPE_CHECKING`, and then
it tests the object with `hasattr(session, "mark_accessed")`.

This proposal has a moderate chance and a low cost. It also **blocks nothing**. If
the maintainers refuse it, we import from the middleware module, or we test the
object in the same way as the core code.

### 6.4 Documentation PRs after the release

Add the SDK to the third-party pages of Starlette and FastAPI after we release it.
One task has more value. Some pages still send approximately 70k downloads each
month to the **archived** `fastapi-sessions` package, as Section 2 shows. That is
a supply chain risk. Nobody will argue against a correction.

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

**But do not copy the contract.** It cannot express three operations that
Section 3 requires.

- **The contract has no `rotate()`.** The caller must build the defence against
  fixation from three calls: `read` the old key, `write` the new key, then
  `remove` the old key. Those calls are not atomic. Between the write and the
  remove, two IDs give access to the same authenticated session. If the process
  stops, or the caller ignores an error from `remove`, the old ID stays valid.

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

## The open question

Must version 1 support only **web sessions**? That scope means a cookie and the
OWASP lifecycle, and it competes with `starsessions` and `fastapi-users`. Or must
version 1 also support **session state for agents and MCP**? That scope means a
session ID in a header or in an argument, no cookie, and working memory with a
TTL. Section 4, item (c), describes this group of users. Both scopes use the same
backend,
but the transport is different and the users are different.

My opinion: build the web sessions first, and make the backend independent of the
transport. The support for agents is then a second adapter, not new work. But this
scope also overlaps with `redis/agent-memory-server`. The decision is therefore
about the Redis product range as much as about the technical design.

Section 7 answers part of the technical question. The ABC with a small Protocol is
the reason that a transport-independent backend is possible. The base class holds
the complete lifecycle, and no part of it depends on the transport:

- the session IDs
- the rotation
- the idle TTL and the absolute TTL
- the index for each user
- the telemetry

Only the cookie carrier is specific to the web, and it lives in `sessions.py`, not
in the store. An adapter for agents and MCP then supplies a different carrier.
That carrier reads the session ID from a header or from an argument, and it uses
the same base class. The remaining question is about the product range.

### Next actions

1. Review and support
   [starlette#3436](https://github.com/Kludex/starlette/pull/3436), as Section 6.1
   explains. This action is urgent. The PR is open now, and it changes the
   behaviour that this design uses.
2. Open the PR for `pop()` and the `accessed` flag, as Section 6.2 explains.
3. Add a minimum version of `starlette>=1.0.0` to `pyproject.toml` when the
   implementation starts.
4. Propose the neutral import path for `Session`, as Section 6.3 explains.
   Continue with the work whatever the answer is.
5. Send the documentation PRs after the release, as Section 6.4 explains.
