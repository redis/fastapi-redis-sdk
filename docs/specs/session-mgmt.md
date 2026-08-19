# Session management in the Starlette/FastAPI ecosystem — research dossier & recommendation

## Context

`fastapi-redis-sdk` is the official Redis integration for FastAPI. It already ships
connection management, DI-based caching (`cache`/`cache_evict`/`cache_put`,
`CacheBackend`), rate limiting (`rate_limit`, `RateLimitBackend`), and OTel
instrumentation, all behind the fluent `FastAPIRedis(app).lifespan().caching().rate_limiting().otel()`
surface. The open question is whether **server-side sessions** are the right next
primitive: is the absence of a canonical FastAPI session solution a sign the need
doesn't exist, or a genuine gap the ecosystem would adopt?

This document records what the research found (primary sources, adoption numbers,
maintainer commitments) and what I recommend building as a result.

---

## 1. What core maintainers have committed to — it is settled, and it is "no"

Both upstream projects have explicitly and repeatedly declined to own session
backends. This is not neglect; it is a stated scope decision, reaffirmed over
seven years.

| Source | Date | Outcome |
|---|---|---|
| [fastapi#754 "First-class session support"](https://github.com/fastapi/fastapi/issues/754) | Nov 2019 → closed Feb 2020 | tiangolo: *"It's already in place. More or less like the rest of the security tools… It's just not properly documented yet."* — pointed at `APIKeyCookie` + JWT-in-cookie. Closed as answered. |
| [encode/starlette#499 "Add pluggable session backends"](https://github.com/encode/starlette/pull/499) | May 2019 → closed **unmerged** Feb 2022 | Tom Christie: *"let's continue with the 'as a third party packages' approach… it looks to me like we oughta scope Starlette as 'feature complete' and just let folks build other stuff on top of it."* |
| [starlette#1801 "Why are Starlette sessions so basic?"](https://github.com/Kludex/starlette/discussions/1801) | Aug 2022 | adriangb (accepted answer): Starlette is *"a **minimal** and **composable** web toolkit, not a complete web framework with all of the batteries included"*; *"if it can be implemented as an external package, we'd prefer that to bringing it into Starlette itself."* When the OP proposed **removing** sessions from core entirely and pointing at `starsessions`: adriangb *"I don't disagree with you"*, Kludex *"That's a good point. 👍"* |
| [starlette#2256 (+ PR #2255) JWT sessions](https://github.com/Kludex/starlette/discussions/2256) | Aug 2023 | Rejected. Response: *"can very well be an independent package… If it's a feature that's popular… it will be maintained by the community."* Discussion left unanswered; author shipped an alpha third-party package instead. |
| [fastapi#10370 Roadmap](https://github.com/fastapi/fastapi/issues/10370) | current | No session or auth workstream. Items are Pydantic v2, Starlette upgrades, dropping EOL Pythons. |

**Key rebuttal on record, still unanswered:** in #754, the requester pointed out
that JWT-in-a-cookie only works while session data fits in
[RFC 6265's 4096-byte cookie limit](https://tools.ietf.org/html/rfc6265#section-6.1),
and that the real ask was Django/PHP-style swappable server-side backends. dmontagu
(then core) agreed FastAPI should only add DI + OpenAPI polish over *Starlette-owned*
backends — and Starlette then declined to own them. The feature fell down the gap
between the two projects and has stayed there.

**Governance note:** Starlette and Uvicorn moved from `encode` to Kludex's personal
handle; Starlette shipped 1.0 and is at **1.6.0 (Aug 2026)**. A 1.0 release under a
"feature complete" scope makes core adoption *less* likely, not more.

### 1a. The one thing that did change — and it matters to us

[starlette#3166](https://github.com/Kludex/starlette/pull/3166), authored by **Kludex**
and merged **2026-03-01**, replaces the plain `dict` at `scope["session"]` with a
`Session` subclass that tracks two flags, explicitly following Django and Flask
convention:

- `accessed` — set when the session is read (via the `HTTPConnection.session` property)
- `modified` — set on `__setitem__`/`__delitem__`/`clear`/`update`; `pop`/`setdefault`
  mark modified only when the value actually changes

The middleware now emits `Set-Cookie` **only when `modified`**, and adds
`Vary: Cookie` when `accessed`. This closes the long-standing
[#2019 race condition](https://github.com/Kludex/starlette/issues/2019) where a slow
read-only response clobbered a newer session cookie.

Core still has **no backend parameter** — it remains itsdangerous-signed cookies only.
But `accessed`/`modified` is precisely the primitive a server-side store needs to avoid
a Redis round-trip on every request. Starlette has, incidentally, just built the hook.
This is the single most important technical finding: it did not exist when every
current session library was designed.

---

## 2. What exists today, and how much it is actually used

PyPI downloads, last 30 days (`pypistats`), with repo health:

| Package | Downloads/mo | Stars | Status |
|---|---|---|---|
| `starlette` | 665M | — | 1.6.0, Aug 2026 |
| `fastapi` | 603M | — | active |
| **`fastapi-users`** | **1.53M** | 6.2k | active (Aug 2026) |
| `starsessions` | 429k | 123 | active, v2.3.0a1 Mar 2026 — **the one Starlette docs point to** |
| `fastapi-sessions` | 70k | 109 | **ARCHIVED**, last push Jul 2023 |
| `authx` | 69k | 1.2k | active |
| `starlette-session` | 41k | 37 | stale, last push Feb 2023 |
| `starlette-authlib` | 2.6k | — | niche |
| `fastsession` | 224 | — | negligible |
| *for contrast:* `pyjwt` | 705M | — | — |
| *for contrast:* `authlib` | 154M | — | — |

Two readings, both true:

- **All dedicated session libraries combined ≈ 610k/mo against 665M Starlette
  installs — under 0.1%.** No incumbent has won. The Starlette-endorsed option has
  123 stars. 70k downloads/month flow to an **archived** package, which is a live
  supply-chain problem, not a healthy market.
- **`fastapi-users` alone is 2.5× all session libraries combined**, and its
  [`RedisStrategy`](https://fastapi-users.github.io/fastapi-users/latest/configuration/authentication/strategies/redis/)
  *is* a server-side session store in all but name: an opaque token is stored in Redis
  mapped to a user id, looked up per request, and **deleted on logout** for true
  revocation. It is the most-used server-side session implementation in the ecosystem
  and it is not labelled "session".

**Conclusion: demand is real but rerouted.** It is expressed as (a) `fastapi-users`
Redis strategy, (b) the hand-rolled `secrets.token_urlsafe(32)` + `SETEX` + cookie
pattern that every tutorial and vendor guide teaches, and (c) hosted IdPs
(WorkOS/Auth0/PropelAuth). "Session library downloads" is the wrong metric.

### 2a. What Starlette's built-in session is actually used for

The dominant real-world use of `SessionMiddleware` is **not** user sessions — it is
**OAuth handshake state for Authlib**, which stores the `state` parameter and PKCE code
verifier in `request.session` and hard-requires the middleware. That explains why the
basic signed-cookie version was "good enough" for so long: the payload is tiny and
short-lived.

It also explains the ecosystem's most-reported session bug class: `mismatching_state` /
`MismatchingStateError`, caused by SameSite/Secure misconfiguration, secret rotation
invalidating old cookies, or the signed cookie silently exceeding 4KB once anyone puts
a token or profile in it. Practitioner guidance for all of these converges on
*"keep the session payload small; store tokens server-side"* — i.e. the thing nobody
ships a blessed solution for.

---

## 3. Is session management tied to security/auth — and can you avoid it?

Yes it is tied, and largely no you cannot avoid it, once requirements go past
machine-to-machine APIs. From the
[OWASP Session Management Cheat Sheet](https://cheatsheetseries.owasp.org/cheatsheets/Session_Management_Cheat_Sheet.html):

- **Opaque IDs + server state:** *"The session ID content (or value) must be meaningless
  to prevent information disclosure attacks"*; business logic *"must be stored on the
  server side, and specifically, in session objects or in a session management database
  or repository."* ≥64 bits entropy from a CSPRNG (≥128 recommended for custom IDs).
- **Rotation:** *"The session ID must be renewed or regenerated… after any privilege
  level change"*; *"regeneration is mandatory to prevent session fixation attacks."*
  Triggers: authentication, password change, permission change, role elevation.
- **Timeouts:** *"All sessions should implement an idle or inactivity timeout"* **and**
  *"an absolute timeout, regardless of session activity"*, and
  *"expiration must be enforced server-side."* Client-side enforcement is rejected
  outright. Plus an optional **renewal timeout** rotating the ID mid-session.
- **Termination:** on expiry the app *"must take active actions to invalidate the session
  on both sides, client and server. The latter is the most relevant."*
- **Concurrency:** users should be able to view active sessions, get concurrent-logon
  alerts, and **remotely terminate** sessions.
- **Hijack detection:** *"highly recommended to bind the session ID to other user or
  client properties, such as the client IP address, User-Agent"* — for detection, with
  the explicit caveat that NAT/proxies/spoofing mean it *"cannot be used… to trustingly
  defend."*
- **Implementation:** *"It is recommended to use these built-in frameworks versus
  building a home made one from scratch."*
- Tokens must never go in `localStorage`/`sessionStorage`.

Mapping that against carrier choice:

| Requirement | Signed cookie / JWT | Server-side store |
|---|---|---|
| Immediate logout / revocation | ✗ (valid until exp) | ✓ delete the key |
| Server-enforced idle **and** absolute timeout | ✗ | ✓ two clocks |
| ID rotation on privilege change | partial (reissue, old still valid) | ✓ |
| List / remotely kill a user's sessions | ✗ | ✓ |
| Payload > 4KB | ✗ | ✓ |
| Keep data off the client | ✗ (signed ≠ encrypted) | ✓ |
| Zero-infrastructure, no per-request lookup | ✓ | ✗ |

**So:** stateless tokens are legitimately correct for service-to-service calls and
short-lived access tokens. But every OWASP requirement above that involves *revoking,
rotating, bounding, or enumerating* a session needs shared state. The widely-recommended
"hybrid" fix — embed a session id in the JWT and check it against a Redis denylist —
is a session store with strictly worse properties: you pay the lookup anyway, and keep
the token's size and staleness. Once revocation is a requirement, the stateless
argument has already been conceded.

Note the double edge of OWASP's last point: *prefer well-scrutinised framework code
over home-made*. It argues against another weekend package, and for one seriously
implemented and reviewed. Session fixation, rotation and CSRF handling are CVE
territory, not bug territory.

---

## 4. Would the need arrive anyway as FastAPI grows? Yes — three converging drivers

**(a) Every mature ecosystem converged here.** Django ships swappable session backends
including cache/Redis; PHP has `SessionHandler`; Rails has its store abstraction;
Express pairs `express-session` with `connect-redis`. The closest precedent is
**Spring Session Data Redis**: putting `spring-session-data-redis` on the classpath
auto-configures Redis-backed `HttpSession` with *no application code change*, replacing
in-memory sessions so any instance serves any request, removing sticky sessions, and
surviving node restarts. That is a vendor-maintained integration for exactly this
problem, and it is the model to emulate. FastAPI is the only major modern web framework
without an equivalent.

**(b) The JWT-for-sessions backlash is now mainstream.** 2026 practitioner guidance
routinely leads with "JWTs can't be revoked natively" and lands on Redis denylists or
hybrid session records. The intellectual argument that carried FastAPI's stateless
default has largely turned over.

**(c) The new one — AI agent and MCP backends.** FastAPI is the default substrate here,
and the **MCP 2026-07-28 revision is stateless by design**, deliberately pushing session
semantics *up to the application*: the server hands the client an identifier and the
client passes it back. FastMCP's session state is in-memory by default and needs Redis
(or another shared store) once more than one server instance is involved; its
resumability event store is Redis-backed. This is a fast-growing population of FastAPI
apps whose core need is a shared, TTL'd, per-session key-value store with rotation and
cleanup. Redis already occupies the adjacent space with `redis/agent-memory-server`
(session/working memory with TTL, promoted to long-term memory).

Also growing and structurally cookie-bound: server-rendered FastAPI (Jinja + HTMX),
which is the original #754 use case and cannot use bearer tokens cleanly.

**Honest counter-arguments:**
- Upstream will not bless it. This stays a third-party package indefinitely.
- We would compete with `starsessions` (Starlette-docs-endorsed) and with
  `fastapi-users`' `RedisStrategy` (the actual incumbent by usage). Differentiation
  must be explicit, and interop matters more than feature count.
- It puts the SDK on the security-critical path. That raises the review, docs, and
  disclosure bar (`SECURITY.md` already exists; it would need to mean more).

---

## 5. Recommendation

**Build it — as `FastAPIRedis(app).sessions()`, positioned as the OWASP-complete,
Redis-native session store for FastAPI, with first-class interop rather than a new
`request.session` dialect.** The gap is real, the incumbents are thin or mislabelled,
and Redis is the natural vendor for it, exactly as Spring Session Data Redis is for
Spring. Two things make now the right moment: Starlette 1.x just landed the
`accessed`/`modified` primitive, and the agent/MCP wave is creating net-new demand.

### Differentiation — the four things existing options don't do

1. **Ride `Session.accessed`/`modified` (Starlette ≥1.x, #3166).** Write to Redis only
   on mutation; refresh idle TTL on access; let core emit `Vary: Cookie`. No
   `load_session()` call for users to forget — `starsessions` solves the same problem
   with an explicit load that raises `SessionNotLoaded` if skipped, which is a footgun
   we can now simply not have. This is genuinely unavailable to any library designed
   before March 2026.
2. **OWASP operations as API, not as documentation.** `rotate()` (fixation defence on
   login/privilege change), **separate idle and absolute TTLs**, `revoke()`, and
   `list_sessions(user_id)` / `revoke_all(user_id)` for concurrent-session control —
   backed by a per-user Redis SET of session ids. That last capability is structurally
   impossible for a cookie library and is the clearest "why Redis" argument.
3. **OpenAPI visibility via `APIKeyCookie`.** This answers #754's original complaint
   directly, using tiangolo's own recommended primitive, so sessions appear as a
   declared security scheme.
4. **Interop as the adoption hook.** Stay drop-in compatible with `request.session` so
   existing code and **Authlib OAuth flows work unchanged** — which incidentally fixes
   the 4KB-overflow and `Set-Cookie`-race classes of `mismatching_state` bugs for free.
   Document a migration path from `fastapi-users`' `RedisStrategy` and from
   `starsessions`.

### Shape, following existing SDK conventions

Mirror the caching/rate-limiting split already in the tree:

| New file | Mirrors | Contents |
|---|---|---|
| `src/redis_fastapi/sessions.py` | `cache.py`, `ratelimit.py` | `SessionMiddleware`, `session()` DI factory, `add_redis_sessions()` |
| `src/redis_fastapi/session_backend.py` | `cache_backend.py`, `ratelimit_backend.py` | `SessionStore` (ABC, owns the lifecycle) + `RedisSessionBackend` / `SyncSessionBackend`: `load`/`save`/`delete`/`rotate`/`touch`/`list_for_user`/`revoke_all`. See §7 for why this one is an ABC and not a bare `Protocol`. |

Extend, don't duplicate:
- `src/redis_fastapi/setup.py` — add `.sessions()` to the `FastAPIRedis` chain.
- `src/redis_fastapi/deps.py` — add `SessionDep` / `SessionBackendDep` +
  `get_session_backend`, alongside the existing `get_cache_backend` /
  `get_rate_limit_backend`, preserving `dependency_overrides` testability.
- `src/redis_fastapi/config.py` — `REDIS_SESSION_*` settings on `RedisSettings`
  (cookie name, idle TTL, absolute TTL, samesite, https_only, key prefix, rotation
  policy).
- `src/redis_fastapi/telemetry.py` — add `session_*` instruments following the file's
  established pattern exactly: new fields on `_OTelState`, a `session_span()` alongside
  `cache_span`/`ratelimit_span`, `record_session_*()` guarded helpers, and a
  `timed_session()` context manager. Suggested instruments:
  `redis_fastapi.sessions.operations` (counter, by `operation` = load/save/rotate/revoke
  and `result` = hit/miss/expired), `redis_fastapi.sessions.latency` (histogram),
  `redis_fastapi.sessions.active` (up-down counter or gauge). Keep the
  import-guarded no-op discipline and `disable_telemetry()` reset behaviour.
  Note that `telemetry.py`'s pattern — module-global `_state` plus free functions
  called *inside* backend methods — is one of the reasons §7 lands on an ABC: the
  instrumentation call sites belong in the base class, emitted once, rather than being
  re-implemented (or silently skipped) per store.
- `src/redis_fastapi/__init__.py` — export the new public names into `__all__`.
- Docs: `docs/guide/sessions.md` + mkdocs nav; a section in
  `docs/guide/observability.md` for the new metrics; an `examples/` app showing
  login → `rotate()` → logout → `revoke_all()`, plus an Authlib OAuth example.

### Security requirements to treat as acceptance criteria, not nice-to-haves

- **Declare a direct `starlette>=1.0.0` dependency.** The design rides
  `Session.accessed`/`modified`, which shipped in **Starlette 1.0.0 (2026-03-22)** — that
  release is exactly where #3166 landed. Today `pyproject.toml` declares no direct
  Starlette dependency at all (it arrives transitively via `fastapi>=0.115.0`, and
  `uv.lock` resolves 1.3.1), so the current spec permits a 0.3x Starlette with no
  `Session` class whatsoever. Without an explicit floor the "write only on mutation"
  optimisation degrades into a silent no-write.
- Session IDs from `secrets.token_urlsafe(32)` (≥128 bits); IDs opaque, never carrying data.
- `rotate()` mandatory on authentication and privilege change; old key deleted, data migrated.
- Idle **and** absolute TTL, both enforced server-side in Redis.
- Cookie defaults strict: `HttpOnly`, `SameSite=Lax`, `Secure` on by default (with a
  documented dev escape hatch) — note `starsessions` chose strict-by-default and it is
  the right call.
- `Cache-Control: no-store` on session-bearing responses; support `Clear-Site-Data` on logout.
- Log a salted hash of the session ID, never the ID itself — applies to telemetry
  attributes too: **session IDs must never become span attributes or metric labels.**
- Document CSRF explicitly: cookie sessions reintroduce CSRF exposure that bearer tokens
  avoid. Reference `fastapi-csrf-protect` (138k/mo) or provide guidance.
- Optional, off by default: bind to IP/User-Agent for hijack *detection*, with OWASP's
  caveat documented so nobody treats it as a control.

### Verification

- Unit tests under `tests/unit/` mirroring the existing cache/ratelimit test layout:
  rotation preserves data and invalidates the old key; idle vs absolute expiry are
  independent; `revoke_all` kills every session for a user; no Redis write when the
  session is untouched (assert on `modified`); `Vary: Cookie` present when accessed.
- **Do not trust `modified` blindly** (see §6.1 — it currently under-reports upstream).
  Tests must assert that a `popitem()`, a `|=`, and a *nested* mutation
  (`session["a"]["b"] = 1`) are still persisted. Nested mutation is undetectable by any
  `dict` subclass and therefore unfixable upstream, so the store needs an explicit
  `save()` escape hatch and an opt-in always-write mode. These tests must pass whether or
  not upstream #3436 merges.
- Integration tests under `tests/integration/` against a real Redis: cross-worker
  session sharing (two app instances, one Redis), TTL behaviour, concurrent-request
  race that previously clobbered cookies.
- Interop test: an Authlib OAuth flow completing against the Redis-backed store
  unmodified, plus a >4KB payload that would break the signed-cookie path.
- Telemetry test following the existing pattern: assert instruments record and that
  `disable_telemetry()` restores a clean `_OTelState`; assert no session ID appears in
  any attribute.
- Run `nox` (lint, mypy, bandit, coverage) — the repo gates on all four.
- Manual: run the `examples/` login app, confirm `HttpOnly`/`Secure`/`SameSite` flags,
  inspect keys in Redis, confirm logout deletes them.

### Sequencing suggestion

Ship `session_backend.py` + `sessions.py` + DI + config + strict cookie defaults +
`rotate`/`revoke` first (the OWASP core). Add `list_sessions`/`revoke_all`
(concurrent-session control) and the Authlib interop example as a close follow-up —
they are the differentiators worth their own release note.

---

## 6. Upstream strategy — what we propose, and what we own

A follow-up question on this dossier: should the vendor-agnostic part be proposed to
Starlette or FastAPI as common code, so other vendors could extend it later?

**Vendor-agnostic yes; upstream-owned no.** Decomposing the feature by neutrality shows
where the seam actually falls:

| Layer | Vendor-neutral? | Upstream-viable? |
|---|---|---|
| L1 `Session` dict + `accessed`/`modified` tracking | already upstream | already there — but incomplete, see §6.1 |
| L2 `Session` importable without the cookie middleware | pure refactor | plausible small PR (§6.3) |
| L3 Cookie carrier parameterised by a store | neutral | **this is PR #499's diff** |
| L4 `SessionStore` protocol (`load`/`save`/`delete`/`touch`) | neutral — *this is the ask* | **declined twice** |
| L5 OWASP lifecycle (`rotate`, dual TTL, `revoke_all`, per-user index) | neutral interface | out of scope for a "feature complete" toolkit |
| L6 Redis implementation | vendor-specific | ours |

L3+L4 is precisely what a "submit it upstream" proposal would be — and precisely what
[starlette#499](https://github.com/encode/starlette/pull/499) already *was*: pluggable
session backends, no vendor in the diff. It sat open roughly three years and closed
unmerged. The evidence is not "nobody proposed the neutral version"; the neutral version
is the one that was refused, and [#2256](https://github.com/Kludex/starlette/discussions/2256)
got the same answer in 2023.

Verified against source rather than the issue tracker: `starlette/middleware/sessions.py`
on `master` today still has **no store, backend, or serializer parameter** — state is
hardcoded to JSON + base64 + `TimestampSigner`. Nothing has moved on L3/L4.

There is also an independent reason not to *want* it upstream even if it were offered:
**release-cadence coupling.** Session handling is CVE-adjacent (fixation, rotation, cookie
flags). If our store implements an upstream-owned protocol, every interface fix ships on
Starlette's timeline and we carry `hasattr` shims across N-1/N-2. Owning the interface
locally is a feature, not a compromise.

Precedent points the same way. In every ecosystem §4a cites, the store abstraction lived
one layer *below* the framework and one *above* the vendor: `express-session` (itself a
third-party package) defines the `Store` base and `connect-redis` implements it — Node core
owns neither. Spring's `SessionRepository` lives in Spring Session, not in the Servlet
spec. So the abstraction should be vendor-agnostic, and it should be ours.

What *is* worth sending upstream is much smaller, and one item is time-sensitive.

### 6.1 Harden the `accessed`/`modified` contract we depend on — highest value

Differentiator #1 above is "write to Redis only on mutation." That is only *sound* if
`modified` never under-reports. **Today it does.** Verified in
`starlette/middleware/sessions.py`:

`Session` overrides `__setitem__`, `__delitem__`, `clear`, `pop`, `setdefault` and
`update` — but **not `popitem()` and not `|=`**. `dict.__ior__` updates at C level, which
bypasses the Python-level `update()` override entirely. For a signed cookie that costs a
`Set-Cookie`; for a server-side store it is a **silently lost write**, which is strictly
worse.

This is already covered by [starlette#3436](https://github.com/Kludex/starlette/pull/3436)
(opened 2026-08-10 by an outside contributor, still open and unmerged as of this writing).
**Review and co-sign it — do not duplicate it.** There is an open PR modifying the exact
primitive we intend to build on; putting a Redis-maintainer voice in that thread before
the contract settles is worth more than proposing an abstraction that will be declined.

### 6.2 New PR — `pop()` sets `modified` but never `accessed`

`pop()` does `self.modified = self.modified or key in self`, while `mark_modified()` sets
*both* flags. A pop-only request therefore emits `Set-Cookie` without `Vary: Cookie` —
inconsistent with every other mutating path, and the `Vary` header is what
[#2019](https://github.com/Kludex/starlette/issues/2019) was about. Two lines plus a test,
and outside #3436's stated scope.

### 6.3 Proposal — a neutral import path for `Session`

Propose `starlette.datastructures.Session` (or a `starlette.sessions` module) with the
existing name kept as an alias. Argument: a third-party store must currently import from
`starlette.middleware.sessions` — pulling in the itsdangerous cookie middleware — just to
reference the type contract. Supporting evidence that the contract is already de-facto
public and duck-typed: `starlette/requests.py:169-175` type-imports `Session` under
`TYPE_CHECKING` and then guards the call with `hasattr(session, "mark_accessed")`.

Moderate odds, low cost, and **zero blocker if declined** — we import from the middleware
module or duck-type, exactly as core itself does.

### 6.4 Docs PRs, post-launch — the lever upstream reliably accepts

List the SDK on Starlette's and FastAPI's third-party pages once shipped. Higher value:
correct pointers that still route ~70k downloads/month to the **archived**
`fastapi-sessions` (§2) — a live supply-chain problem, and a contribution nobody has to
argue about.

---

## 7. Store contract — an ABC for the lifecycle, a narrow Protocol at the seam

The natural instinct for "let other vendors extend this later" is a `typing.Protocol`, so
a Postgres or Valkey store could conform structurally with no dependency on this package.
For most seams that is the right call. **For this one it is not**, and the reason is
security rather than style.

**A Protocol cannot hold an invariant.** It is a type-checker artefact that evaporates at
runtime. A conforming store passes `mypy` while seeding IDs from `random.random()`, or
implementing `rotate()` as write-new-without-deleting-old — which is session fixation, the
exact attack §3 lists rotation as the defence against.

**The dangerous methods are the ones most likely to be stubbed.** `revoke_all(user_id)`
and `list_for_user(user_id)` require a per-user index (a Redis SET). A vendor for whom
that is awkward will write `pass` / `return []`. That structurally conforms *and* turns a
security control into a silent no-op — an empty list reads to the calling application as
"this user has no other sessions." That is worse than an unimplemented method, because it
is indistinguishable from a correct answer.

**It also inverts OWASP's own guidance** quoted in §3 — *"recommended to use these
built-in frameworks versus building a home made one from scratch"*. A Protocol means N
implementations of fixation defence and dual-TTL clock arithmetic; a base class means one,
written and reviewed once.

**And `telemetry.py` settles it.** That module's state is a global (`_state`) and its
helpers are free functions invoked *inside* backend methods — `cache_span`,
`record_cache_request` and `timed_operation` are woven into `CacheBackend.get()`, not
layered over it. So under a bare Protocol a non-Redis store either imports
`redis_fastapi.telemetry` (reintroducing the dependency edge that was the whole point) or
emits nothing at all, and `redis_fastapi.sessions.operations{operation="rotate"}` goes
silent. For a security metric, silence is indistinguishable from "no rotations are
happening."

### Decision

- **`SessionStore(ABC)` owns the lifecycle concretely:** `new_id()` via
  `secrets.token_urlsafe(32)`; `rotate()` as a template method with fixed ordering (write
  new → migrate data → delete old → update user index); idle/absolute TTL arithmetic; and
  the telemetry call sites, so every store inherits instrumentation and the
  "never log a raw session ID" rule is enforced at one set of call sites instead of being
  re-litigated per vendor.
- **Abstract only the storage primitives:** `_read`, `_write`, `_delete`, `_expire`,
  `_index_add`, `_index_members`. The vendor seam is deliberately boring.
- **Capability flags, not silent stubs:** `supports_user_index` and friends, so the
  middleware refuses to expose a control the store cannot deliver rather than returning a
  misleading answer.
- **Keep a narrow `SessionStoreProtocol`** describing only what `sessions.py` middleware
  and DI actually consume — so tests and integrators can substitute without inheriting,
  and the middleware never types against the concrete Redis class. This is the honest
  vendor-agnostic piece. (`dependency_overrides` testability is DI-level and unaffected
  either way.)

**Repo idiom supports exactly this split.** The one existing Protocol, `Coder`
(`src/redis_fastapi/types.py:15`), is a stateless two-method value-conversion seam whose
default implementation `JsonCoder` deliberately does *not* inherit it. The classes that own
*behaviour* — `CacheBackend`, `RateLimitBackend` — are plain concrete classes. Protocol for
consequence-free seams; owned code for behaviour. A session store is behaviour, and
security behaviour at that. No ABC exists in the tree yet, so this is a new idiom and
worth flagging as one — but neither caching nor rate limiting carries invariants whose
violation is a CVE.

### Corollary — interop is an adapter, not a contract concession

Differentiator #4 makes interop the adoption hook, which invites shaping our contract to
match `starsessions`' store interface (roughly `read`/`write`/`remove`/`exists`, with one
TTL on `write`). **Do not.** That alignment costs precisely the operations §3 requires:

- **No `rotate()` in the contract** → fixation defence becomes caller-side read +
  write-new + remove-old: three round trips, non-atomic, and a crash mid-sequence leaves
  two valid sessions.
- **One TTL parameter cannot express two clocks** → absolute expiry has to be smuggled
  into the payload and checked after decode, so expiry is enforced by our Python rather
  than by Redis — directly against *"expiration must be enforced server-side."*
- **No per-user index** → `revoke_all` becomes impossible, deleting the one capability
  §5 calls structurally impossible for a cookie library.

Correct position: **adapter in, not contract out.** Ship a thin adapter so a
`starsessions` store can be *used* by our middleware in declared-degraded mode (missing
capabilities reported `False`), and document the migration path — without bending our own
contract to four methods.

---

## Open question for you

Whether to scope v1 at **web sessions** (cookie + OWASP lifecycle, competing with
`starsessions`/`fastapi-users`) or to also cover **agent/MCP session state** (header or
argument-carried session id, no cookie, TTL'd working memory — the driver from §4c).
They share a backend but differ in transport and in who the audience is. My inclination
is web sessions first with the backend deliberately transport-agnostic, so the agent
case is a thin second adapter rather than a rewrite — but this overlaps with
`redis/agent-memory-server`, so it is a portfolio question as much as a technical one.

§7 partly answers the technical half: the ABC-plus-narrow-Protocol split is *what makes*
"transport-agnostic backend" real rather than aspirational. The lifecycle (IDs, rotation,
dual TTL, per-user index, telemetry) lives in the base and is transport-free; only the
cookie carrier is web-specific and it lives in `sessions.py`, not in the store. An
agent/MCP adapter then supplies a different carrier — header or argument-carried session
id — against the same base. The remaining question is genuinely a portfolio one.

### Immediate next actions

1. Review and co-sign [starlette#3436](https://github.com/Kludex/starlette/pull/3436)
   (§6.1) — time-sensitive, it is open now and touches the primitive this design rides.
2. Open the `pop()`/`accessed` PR (§6.2).
3. Add a direct `starlette>=1.0.0` floor to `pyproject.toml` when implementation starts.
4. Float the neutral `Session` import path (§6.3); proceed regardless of the answer.
5. Docs PRs after launch (§6.4).
