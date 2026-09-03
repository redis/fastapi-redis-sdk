# Session management: implementation design

Companion to [`session-mgmt.md`](session-mgmt.md) which answers *why* we build
this and *what* goes into version 1 while this document answers the *how*.

Research date: 2026-08-24. Starlette 1.6.0, FastAPI 0.138.2 in `uv.lock`.

---

## 0. Requirements

Everything after this section explains *how* a functional or non-functional requirement is met, or *why* an excluded
item is excluded. Each row carries an ID so a commit, a test or a review comment can cite it.

### 0.1 Functional requirements

| ID   | Category       | Requirement                                                                                                                                                                                                                                                |
|------|----------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| F-1  | Lifecycle      | Creating or destroying the session does not require an explicit API call, loaded in the middleware before any dependency or endpoint reads it, written only when its data changed.                                                                         |
| F-2  | Lifecycle      | **Rotation is automatic.** The middleware issues a new ID, deleting the old key first, whenever the session's principal changes on a successful response.                                                                                                  |
| F-3  | Lifecycle      | The rotation trigger is the **principal** — the identity plus whatever the application declares privilege-bearing — while the index key is the **subject**, which stays stable per user; rotation is executed both in case of escalation and de-escalation |
| F-4  | Lifecycle      | Revocation in three forms: the current session, one session by ID scoped to its subject, and every session of a subject.                                                                                                                                   |
| F-5  | Expiry         | Two independent clocks, idle and absolute, both enforced by Redis. Writing the payload never extends the absolute deadline.                                                                                                                                |
| F-6  | Expiry         | Cookie `max-age` derives from the same server-side numbers as the record. Redis can eventually reclaim what a closed browser abandoned.                                                                                                                    |
| F-7  | Reverse lookup | A session may be bound to a subject, which need not be a user. List and count a subject's live sessions, each with a descriptor, which keeps all the needed data, without having to consult the straight-record.                                           |
| F-8  | Reverse lookup | A listing never reports a session that has already died. Reads to the session remove dead entries; writes re-assert lost ones.                                                                                                                             |
| F-9  | Transport      | The cookie carries a signed opaque identifier and no session data. An invalid value is rejected and yields a new session.                                                                                                                                  |
| F-10 | Transport      | Cookie name, `Domain`, `Path`, `SameSite`, `Secure` and `HttpOnly` are configurable. `Vary: Cookie` is emitted whenever the session was accessed.                                                                                                          |
| F-11 | API            | One call enables the feature. A dict-like dependency needs no load or save, and `request.session` behaves as before, so existing code and Authlib run unchanged.                                                                                           |
| F-12 | API            | The store exposes rotate, revoke, revoke-by-ID, revoke-all, list and count. Both dependencies resolve through `Depends`, so `dependency_overrides` works.                                                                                                  |
| F-13 | API            | Every part of the feature works from a `def` endpoint as well as an `async def` one, with no second pattern to learn.                                                                                                                                      |
| F-14 | Data           | `created`, `last_access` and `lifetime` are readable under those names(to preserve compatibility withother frameworks), stored alongside your data rather than mixed into it.                                                                              |
| F-15 | Data           | The payload is serialized through a replaceable coder, and optionally encrypted                                                                                                                                                                            |
| F-16 | Data           | Mutation is detected for `popitem()`, `\|=` and `pop()`. Nested mutation, which no `dict` subclass can see, has a documented escape route.                                                                                                                 |
| F-17 | Extensibility  | Coder, encryptor, store, key prefix, ID factory, subject resolver and cookie builder are all replaceable. A supplied ID factory is validated on every call.                                                                                                |
| F-18 | Observability  | Spans and metrics for every store operation, following the pattern the package already uses.                                                                                                                                                               |
| F-19 | Errors         | One exception base, with configuration and store errors beneath it; no driver error reaches the caller. A failed read yields an empty session by default, and a failed write always raises.                                                                |
| F-20 | Migration      | A documented path from each of four starting points: an in-process store, the Starlette signed cookie, `starsessions` with Redis, and the `fastapi-users` Redis strategy.                                                                                  |
| F-21 | Events         | **Opt-in real-time session events.** When the server supports and is configured for them, Redis notifications drive application callbacks on session death. When it does not, the feature turns itself off and the application is unaffected. Section 13.4.                                    |

### 0.2 Non-functional requirements

| ID   | Category        | Requirement                                                                                                                                                                                 |
|------|-----------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| N-1  | Performance     | A request with no session cookie costs zero Redis calls. A read-only request with one costs a single pipelined round trip. An unchanged payload is never re-serialized or re-written.       |
| N-2  | Performance     | List and revoke-all cost two round trips regardless of session count. A count below the configured limit is `O(1)` with no verification.                                                    |
| N-3  | Compatibility   | Redis 7.4 and later — the floor this package already declares — with no rise in the FastAPI floor.                                                                                          |
| N-4  | Compatibility   | Correct on standalone and on Cluster, with no behavioural difference between them.                                                                                                          |
| N-5  | Compatibility   | No new required runtime dependency. A newer server lowers cost with no code change and no configuration.                                                                                    |
| N-6  | Correctness     | Outliving the absolute deadline is unreachable by construction, not merely avoided by arithmetic.                                                                                           |
| N-7  | Correctness     | Multi-key sequences are correct by ordering rather than by transaction, because Cluster forbids one across them. An interrupted rotation signs the user out and never leaves two valid IDs. |
| N-8  | Correctness     | No interleaving or partial failure leaves a session that revoke-all cannot find. Where a guarantee cannot be given, its limit is documented rather than implied away.                       |
| N-9  | Security        | Identifiers are opaque, carry no data, and come from a CSPRNG with at least 128 bits. `HttpOnly`, `SameSite=Lax` and `Secure` are on unless deliberately relaxed.                           |
| N-10 | Security        | No session ID or subject appears in any log line, span attribute or metric label. Session-bearing responses are not stored by shared caches.                                                |
| N-11 | Security        | The CSRF exposure that a cookie session reintroduces is documented with a remedy.                                                                                                           |
| N-12 | Operability     | Documented guidance for running it: eviction policy, key legibility during an incident, and the per-subject key as a contention point with the seam that shards it.                         |
| N-13 | Testability     | The unit suite runs against `fakeredis` with no Redis process, exercising the real key schema, TTL commands and index rather than a substitute.                                             |
| N-14 | Testability     | Every refuted claim has a test that fails against the earlier design. Documented recipes are executable code under test.                                                                    |
| N-15 | Maintainability | Nothing ships gated on an unmerged upstream change.                                                                                                                                         |
| N-16 | Maintainability | File split, dependency injection, settings and telemetry follow the existing cache and rate-limit code. An abstract base owns the lifecycle; a protocol bounds what callers touch.          |
| N-17 | Correctness     | No correctness claim rests on a notification. Every guarantee holds with events switched off, because Pub/Sub delivery can be dropped and an expiry event can lag the deadline it reports. |

### 0.3 Out of scope

| ID   | Category              | Excluded                                                                                                                                                                   | Why                                                                                                                                                                                                                                                                             |
|------|-----------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| X-1  | Not a session concern | Querying inside session payloads, and holding carts, orders or other domain entities there.                                                                                | A session is not a queryable store. Keep identifiers in it and entities outside it.                                                                                                                                                                                             |
| X-2  | Not a session concern | User management: registration, password reset, verification, OAuth account linking.                                                                                        | We replace one layer of `fastapi-users`, not the framework.                                                                                                                                                                                                                     |
| X-3  | Other backends        | An in-memory store, or a signed-cookie store.                                                                                                                              | `fakeredis` covers the test case and covers it better; Starlette's own middleware already is a cookie store.                                                                                                                                                                    |
| X-4  | Other backends        | Adopting `starsessions`' `read`/`write`/`remove` contract.                                                                                                                 | A cookie store dictates that shape, and it cannot express rotation, two clocks or a subject index.                                                                                                                                                                              |
| X-5  | Deferred              | A carrier for MCP and agent sessions.                                                                                                                                      | Web sessions first. The store is already transport-agnostic, so this is a second carrier and not a rewrite.                                                                                                                                                                     |
| X-6  | Deferred              | An adapter for third-party `starsessions` stores, and reading an existing `starsessions` record in place.                                                                  | Sessions do not survive the switch, by decision. Add the reader if users ask for a migration with no sign-out.                                                                                                                                                                  |
| X-7  | Deferred              | Concurrent-write detection: compare-and-set on the session payload.                                                                                                        | **`SET IFEQ`/`IFDEQ` and `DELEX` cannot express this at all** — they are string commands and the session record is a hash. Section 13.5 gives the correction and names the mechanism that would work. Deferred, so two concurrent writes remain last-write-wins.                |
| X-8  | Deferred              | Client-side caching with `CLIENT TRACKING`.                                                                                                                                | Structurally inapplicable to the read path, because our read is a write command.                                                                                                                                                                                                |
| X-9  | Deferred              | A query-engine index in place of the reverse lookup.                                                                                                                       | Raises the floor to 8.0 and can leave a session silently un-indexed, which revoke-all would then miss.                                                                                                                                                                          |
| X-10 | Deferred              | Re-rotating a live session on a schedule (OWASP's renewal timeout).                                                                                                        | No timer in v1. Rotation on authentication and privilege change is automatic (Section 5.1); only the time-based variety is absent. A genuine gap rather than a boundary.                                                                                                        |
| X-11 | Recipe, not code      | A shipped AES-GCM encryptor.                                                                                                                                               | Ship the seam and document the ten lines, rather than owning cryptographic code and its vulnerabilities.                                                                                                                                                                        |
| X-12 | Recipe, not code      | Hijack detection by IP and User-Agent; `Clear-Site-Data`, `Partitioned` and `__Host-` cookies; a Stream-backed audit log; per-tenant key namespaces and Cluster hash tags. | Each is reachable through a seam that already exists, so none needs code from us.                                                                                                                                                                                               |
| X-13 | Deferred              | The `HIMPORT` family (Redis 8.10) for writing session keys.                                                                                                                | `HIMPORT SET` takes no expiration option and overwrites the key, so it would destroy field `a` and its absolute deadline on every write — the one thing N-6 forbids. What it saves is field names on the wire, and ours are `a` and `d`. Section 13.5 gives the full reckoning. |

## 1. Three corrections to `session-mgmt.md`

### 1.1 The load is eager, not lazy

`session-mgmt.md` once required "a load that happens only when code touches the session".
**No implementation can do that.** `HTTPConnection.session` is a **synchronous** property
in `starlette/requests.py`. A synchronous property cannot await a Redis `GET`.

This is not our limitation. It is why `starsessions` ships `load_session()` and a
`LoadGuard` that raises `SessionNotLoaded`: the same wall, and they chose to hand the
problem to the user.

We chose the other side of that trade. Section 5 difference 4 of `session-mgmt.md` keeps
the `scope["session"]` contract, which is what makes Authlib work with no change. That
choice puts the load in the middleware, which is the last point in the ASGI chain that can
still `await` before a dependency or an endpoint reads the session. Section 4 draws the
chain.

**The rule: load when a session cookie is present.** No cookie means no Redis call, so
anonymous traffic costs nothing. An optional `skip` predicate excludes hot paths, which
is the same control `SessionAutoloadMiddleware` gives, inverted into an opt-out.

The saving we do keep is on the write side, and it is the larger one: **we never
serialize or write a payload that did not change.** Section 4 gives the rule.


### 1.2 Redis Cluster removes atomicity, so ordering replaces it

`src/redis_fastapi/ratelimit_backend.py:192` explains that rate-limit keys are flat, with
no hash tag, because that feature only ever touches one key at a time.

**Sessions touch several.** A write touches the session key and the index key. A rotation
touches two session keys. On Redis Cluster those hash to different slots, so no Lua
script and no `MULTI` can hold them together.

We cannot force them into one slot either. The lookup key is the session ID, and we do
not know the subject until after we read the record, so no hash tag can co-locate the two
without putting the subject in the cookie — which Section 3 of `session-mgmt.md` forbids,
because the ID must carry no meaning.

**So we order the operations instead.** Section 5 gives the rotation order and the
argument for it. One good consequence: this store needs none of the Lua scripts or
capability probes that `ratelimit_backend.py` carries. Plain commands and pipelines.

---

## 2. Modules

Two new files, following the split that `cache.py` / `cache_backend.py` and
`ratelimit.py` / `ratelimit_backend.py` already use.

| File                                   | Contents                                                                                                                                          |
|----------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------|
| `src/redis_fastapi/session_backend.py` | `SessionStore` (ABC, owns the lifecycle), `RedisSessionStore`, **`SyncSessionStore`**, `SessionStoreProtocol`, `SessionMetadata`, `SessionRecord` |
| `src/redis_fastapi/session_events.py`  | `SessionEvents`, the tier probe, and the per-node subscriber task. Separate because it owns a background task and a Pub/Sub connection, which neither of the other two files does. Section 13.4 |
| `src/redis_fastapi/sessions.py`        | `Session`, `SessionMiddleware`, the `session()` dependency factory, `add_redis_sessions()`, the cookie builder, the exceptions                    |

Changes to some of the existing files include :

| File                              | Addition                                                                                                                                           |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| `src/redis_fastapi/setup.py`      | `.sessions()` on `FastAPIRedis`, guarded by the existing `_has_middleware` helper                                                                  |
| `src/redis_fastapi/deps.py`       | `get_session_store`, `SessionStoreDep`, `SessionDep`, plus `get_sync_session_store` and `SyncSessionStoreDep`, beside the matching rate-limit pair |
| `src/redis_fastapi/config.py`     | `session_*` fields on `RedisSettings`, beside the `rate_limit_*` fields                                                                            |
| `src/redis_fastapi/telemetry.py`  | `session_*` instruments, following the existing pattern exactly                                                                                    |
| `src/redis_fastapi/lifespan.py`   | Start and stop the `SessionEvents` subscriber, and run its tier probe once per process, beside `probe_increx_support`                              |
| `src/redis_fastapi/__init__.py`   | the new public names in `__all__`                                                                                                                  |

Reused, not rewritten:

- **`Coder` and `JsonCoder`**, `src/redis_fastapi/types.py:15` is the existing serialization
  interface we can reuse without change. `pydantic_model_coder()` then works for a typed
  session payload at no cost.
- **`settings.pattern_prefix()`**, `src/redis_fastapi/config.py:260`.
- **`get_async_redis` and `_get_pool_state`**, `src/redis_fastapi/deps.py`.
- **The `send_wrapper` pattern** in `RateLimitMiddleware.__call__`,
  `src/redis_fastapi/ratelimit.py:538`.

---

## 3. Redis keys

### 3.1 The schema

Two keys, and they answer two different questions.

```
# "Given this session ID from the cookie, what is the session?"
redis:fastapi:session:<sid>                    HASH
    field "a"  =  "1"          TTL = absolute       the deadline marker
    field "d"  =  <record>     TTL = idle           the payload

# "Given this user, which sessions do they have open?"   <-- the reverse direction
redis:fastapi:sessions-of:<subject>            HASH
    field <sid> = <descriptor> TTL = absolute       one field for each session
```

The first key is the one every request uses. The second exists **only** because three
things in Section 3 of `session-mgmt.md` need to go the other way — from a user to their
sessions — and the cookie cannot answer that. Section 3.3 explains it.

Both are hashes with a TTL on each field, from
`settings.pattern_prefix("session")` and `settings.pattern_prefix("sessions-of")`.

**Field `a` is always written, and always carries a TTL.** Section 3.2 explains why: a
missing `a` must mean one thing only.

#### The two keys cannot collide, and not only by convention

They live under **different prefixes**, so no session ID can produce the index key. The
strings `redis:fastapi:session:` and `redis:fastapi:sessions-of:` first differ at the
character after `session`, before either key's variable part begins, so equality is
impossible whatever the session ID contains.

This is deliberate. An earlier draft nested the index under the session prefix and rested
the argument on `secrets.token_urlsafe` never emitting a `:`. That is true of the default
generator — but Section 9 exposes `id_factory` as a supported seam, and a factory
returning the literal `"by-subject:42"` produced a key byte-identical to the index key.
**An argument that holds only for the default is not a structural guarantee**, so the
structure now carries it instead.

Section 9 additionally requires that a generated ID be validated, which catches the same
class of fault at its source rather than only in its consequence.

Both keys are flat, with no hash tag, for the reason `ratelimit_backend.py:192` already
gives: a hash tag would send every session of one deployment to one slot and create a hot
shard. Section 5 explains how correctness survives without co-location.

**Redis 7.4 is the floor this package already declares** (`README.md:39`,
`docs/getting-started/installation.md:29`), and hash-field expiration arrived in 7.4. So
every design below sits inside the support range we already promise.

### 3.2 Two fields, two clocks, no arithmetic

Section 3 of `session-mgmt.md` requires an idle timeout **and** an absolute timeout, and
requires that *"expiration must be enforced server-side"*. Two fields with two TTLs do
exactly that:

| Field | TTL | Refreshed? |
|---|---|---|
| `a` | the absolute lifetime, set once at creation | **never** |
| `d` | the idle timeout | on every access |

When `d` expires the session went idle. When `a` expires the absolute deadline passed,
whatever the user was doing. **Redis enforces both, and we compute neither.**

#### Field `a` is always written, and always has a TTL

Not an implementation detail — a correctness requirement, and getting it wrong disables
the whole feature for one supported configuration.

`HTTL` answers with `-2` when a field is absent **or** its key is absent, and with `-1`
when the field exists with no expiry. An earlier draft read `-2` as "the absolute deadline
passed" and did not write `a` at all when `absolute_ttl` was `0`. Section 3.2 permits that
setting, and Section 9.4 of `session-mgmt.md` maps `starsessions`' `lifetime=0` onto it —
so for those deployments `HTTL` answered `-2` on every load and **every session was read
as expired the moment it was created.**

Two rules remove the ambiguity:

1. **Write `a` on every session creation, without exception.** This also keeps every
   session key to one schema, which Section 13.2 depends on.
2. **Give `a` a TTL even when no absolute limit is configured.** With `absolute_ttl = 0`
   it gets `gc_ttl`. Never leave it unexpiring: if `d` later expires and `a` does not, the
   key survives with nobody to collect it. So `-1` never occurs, and `-2` carries exactly
   one meaning.

Section 4.1 reads the resulting state as a two-by-two, not as a single sentinel.

This matters more than the round trip it saves. If we instead kept one key and set its
TTL to `min(absolute_remaining, idle)`, then the absolute deadline would be a number our
code recalculates on every write, and one arithmetic bug would let a session outlive its
absolute limit without any test noticing. With two fields that outcome is not a bug we
must avoid — it is unreachable. For a security control, the difference is the whole
point.

Both settings accept an `int` or a `timedelta`, as `cache()` already does in this
package. With both at zero the session is cookie-only: no `max-age` on the cookie, so the
browser drops it when it closes, and **both** fields get `gc_ttl` so Redis eventually
collects what the browser abandoned.

### 3.3 The subject index: from a user back to their sessions

**This is not a search index, and it does not look inside session data.** It answers one
question, in the opposite direction to everything else in this design: *given a user,
which sessions do they currently have open?*

A cookie carries a session ID, so the session key answers "who is this request?". Nothing
answers the reverse. Three controls that Section 3 of `session-mgmt.md` requires all need
that reverse direction:

| The user does this | We need | Without the index |
|---|---|---|
| changes their password, or an administrator forces a sign-out | `revoke_all(user)` — end every session they have | impossible |
| opens the "signed in on these devices" screen | `list_sessions(user)` | impossible |
| signs in when a limit on concurrent sessions applies | a live count | impossible |

"Impossible" is the accurate word. Redis has no query over key contents, so without a
second key the only way to find one user's sessions would be to `SCAN` the whole keyspace
and deserialize every session in the deployment to look at its payload. That is `O(all
sessions)` for one user's logout, and `SCAN` gives no consistent snapshot, so it could
still miss one. A reverse key turns all three into `O(1)` lookups.

#### A worked example

User 42 is signed in on a laptop and a phone. Three keys exist:

```
redis:fastapi:session:7Kp2...aQ     ->  a: "1"          d: {"user_id": 42, ...}
redis:fastapi:session:mB9x...Lz     ->  a: "1"          d: {"user_id": 42, ...}

redis:fastapi:sessions-of:42        ->  7Kp2...aQ: {"ip": "10.0.0.4", "ua": "Firefox/Mac"}
                                        mB9x...Lz: {"ip": "77.1.2.3", "ua": "Safari/iOS"}
```

A request arrives with cookie `7Kp2...aQ`, and only the first key is touched. The third
key is never read on the request path at all — it exists for the three controls above.
"Sign out everywhere" is then `DEL redis:fastapi:session:7Kp2...aQ`,
`DEL redis:fastapi:session:mB9x...Lz`, `DEL redis:fastapi:sessions-of:42`, with the
session IDs supplied by one `HGETALL` of the third key.

**The subject need not be a user.** The `subject_of` seam in Section 9 chooses it, so it
can be a tenant, a device, or an API client. Returning `None` means no index entry, which
is the right answer for an anonymous session: no subject, nothing to revoke in bulk.

#### The operations

**The TTL on each field is that session's absolute deadline**, so Redis bounds the entry
and eventually removes it.

| Operation          | Command                                                                      | Note                                                                            |
|--------------------|------------------------------------------------------------------------------|---------------------------------------------------------------------------------|
| add                | `HSETEX sessions-of:<u> EX <absolute remaining> FIELDS 1 <sid> <descriptor>` | 7.4: `HSET` then `HEXPIRE`. **Remaining, never the full lifetime** — see below. |
| list               | `HGETALL sessions-of:<u>`, then verify                                       | see "the index is an upper bound"                                               |
| count              | `HLEN sessions-of:<u>`                                                       | an **upper bound**, in `O(1)`                                                   |
| revoke one         | `HDEL sessions-of:<u> <sid>`                                                 |                                                                                 |
| revoke all         | `DEL sessions-of:<u>`                                                        |                                                                                 |
| sweep dead entries | **none scheduled**                                                           | expiry does the bulk, reads repair the rest                                     |

#### The index is an upper bound, and a read must verify it

This is the part an earlier draft got wrong, and the error is worth keeping visible so
nobody re-simplifies it away. That draft claimed `HGETALL` was "already free of expired
sessions" and that `HLEN` gave "a live count".

**Both are false, because the entry's TTL is the absolute deadline and most sessions die
of idleness long before that.** With `idle = 30 min` and `absolute = 8 h`, a user who
closes their laptop at 10:00 has a dead session at 10:30 and an index entry until 18:00 —
so the "your devices" screen shows a phantom for seven and a half hours, and a cap on
concurrent sessions counts it.

The entry cannot simply carry the idle TTL instead. The idle clock moves forward on every
request, so tracking it would mean writing the index key on every request, which would
destroy the single-round-trip read in Section 4.2.

**So the index is authoritative about which sessions *might* be alive, and never about
which are.** `list_for_subject` and `revoke_all` both:

1. `HGETALL` the index — one round trip, giving candidates and their descriptors.
2. Pipeline one `HTTL <session key> FIELDS 1 d` for each candidate — a second round trip,
   whatever the number of candidates.
3. Drop the candidates whose `d` is gone, and `HDEL` them from the index.

That is two round trips instead of one, on two operations that a user triggers by hand —
opening a screen, or changing a password. The request path is untouched. In exchange the
answer is correct, and step 3 makes every read repair the index, so dead entries never
accumulate even when nothing expires them.

**`HLEN` stays useful precisely because it over-counts.** An upper bound below the limit
is a definitive answer, so a cap on concurrent sessions checks `HLEN` first and only pays
for verification when the bound is at or over the limit. The common case stays `O(1)`.

Three properties survive, and neither a set nor a sorted set gives all three:

1. **There is no scheduled prune.** Field expiry removes most entries with no code at all,
   and the verification above removes the rest as a side effect of reading. No periodic
   job, and no keyspace notification — which would need server configuration and can be
   dropped anyway.
2. **The key bounds itself.** When the last field expires, Redis deletes the hash. A user
   who never returns leaves nothing behind, and there is no TTL to maintain on the key.
3. **The value carries a descriptor**, so the listing needs no read of the session records
   themselves — only the cheap `HTTL` liveness check. Put the creation time, the client
   address and a device label in it, and the "your active sessions" screen that Section 3
   of `session-mgmt.md` asks for costs two round trips regardless of session count.

#### Add and re-assert with the *remaining* absolute time

The `add` row says `EX <absolute remaining>`, not `EX <absolute>`, and Section 5.3
re-asserts the entry on every session write. Those two facts interact.

A relative `EX <absolute>` restarts the entry's clock on every re-assertion: measured,
an entry at 98 seconds of a 100-second lifetime went back to 100 on re-assert. An actively
used session would therefore hold an index entry that never expires and outlives the
session it describes — reintroducing the phantom this section exists to prevent.

Use the remaining absolute time. Section 4.1 already reads it, as `HTTL` on field `a`, so
it costs no extra command. `EXAT` against the stored deadline is equivalent; what must not
happen is a fresh full lifetime.

#### Why not the query engine instead?

A reverse lookup is a kind of search, so the obvious question is why we maintain a second
key at all rather than declaring an index and letting Redis do it:

```
FT.CREATE  idx:sessions  ON HASH  PREFIX 1 redis:fastapi:session:
           SCHEMA subject TAG
FT.SEARCH  idx:sessions  '@subject:{42}'  NOCONTENT
```

**Two of the arguments for it are correct, and should be recorded as such.** It is a
search. And we do pay a write we would not otherwise pay — although less than it appears,
because that write is pipelined with the session write, so it costs bytes and server CPU
rather than a round trip. It would also remove the hot-key problem in Section 13.3, since
there would be no per-tenant key for every login to contend on, and it would answer
questions our hash cannot: every session from one IP, every session created before a
given time, aggregations across tenants.

It is not the choice for version 1, for four reasons, in order of weight.

**1. A session can become silently un-revocable.** The index only contains documents whose
shape matches the schema. A session written without the indexed field — a bug, a partial
rollout, a custom `Coder` that nests the value differently — is simply absent from every
result. `revoke_all` then reports success and misses it, and the only signal is the
`hash_indexing_failures` counter in `FT.INFO`, which nothing in the request path reads.
Our hash has no such state: if `HSETEX` returned OK, the field is in the index, and if it
did not, the write failed loudly. Section 7 of `session-mgmt.md` rejects exactly this
class of failure — an answer the caller cannot distinguish from a correct one — and here
it would apply to the security control itself.

**2. The floor.** The query engine became part of Redis Open Source in **8.0**. This
package supports **7.4**, where it is available only through Redis Stack. Building
`revoke_all` on it makes a security control conditional on the deployment, and a control
that is sometimes absent is worse than one that is always present. It is also absent from
several Redis-compatible services that users of this SDK do run.

**3. On 7.4 to 7.x the index does not account for field expiration.** Redis 8 filters
logically-expired documents at query time, which is exactly right. Below 8, expired data
can still surface in `FT.SEARCH`, so `list_sessions` would over-report — the same failure
as reason 1, in the version range we support. Our design has no equivalent gap, because
the entry *is* the TTL.

**4. `revoke_all` would need pagination.** `FT.SEARCH` caps `OFFSET + LIMIT`, so revoking
every session of a large tenant means cursor iteration. That is a loop, with partial
progress and a failure mode in the middle of a security operation, in place of one `DEL`.

There is also a memory question we have not measured: an inverted index over millions of
session documents against one small hash per active subject. It would need `NOOFFSETS`,
`NOHL`, `NOFIELDS` and `NOFREQS` to trim what a session index never uses, and then a
benchmark. That is work version 1 does not need to do.

**None of this closes the door, and the design already holds it open.** Section 7 of
`session-mgmt.md` splits the store into a concrete lifecycle and three abstract storage
primitives — `_index_add`, `_index_remove`, `_index_members`. A query-engine index is a
different implementation of those three and nothing else. So this is a store variant for
a later release, gated on Redis 8, and not a decision we are making permanently now. The
right moment to revisit is when the floor moves to 8.0, because reasons 2 and 3 disappear
at that point and reason 1 becomes a matter of validating writes.

### 3.4 Searching inside session data: we do not, and neither does anyone else

A separate question from the reverse lookup: can the application ask *"which sessions hold
a cart containing product X?"* — a query over the **contents** of the payload, not over an
identifier.

**Not in this design.** The payload lives in field `d` as one value, produced by the
configured `Coder` and optionally by an `Encryptor`. Redis cannot look inside an opaque
string, and an encrypted payload could not be indexed even in principle. The only index
here is subject to sessions.

**And not in the alternatives.** This is not a gap we alone have:

|                                       | Can it search session contents?                                                                                                     |
|---------------------------------------|-------------------------------------------------------------------------------------------------------------------------------------|
| `starsessions`                        | No. `SessionStore.read(session_id, lifetime)` is the only accessor; there is no method that takes anything but an ID.               |
| `fastapi-users` `RedisStrategy`       | No. The record holds `str(user.id)` and nothing else, so there is no payload to search.                                             |
| Starlette `SessionMiddleware`         | No. The data is in the client's cookie; the server keeps no copy.                                                                   |
| Django, Rails, PHP, `express-session` | No. All are keyed by session ID. Django additionally base64-encodes the record, so even a SQL `LIKE` over the column finds nothing. |
| **Spring Session**                    | **Partly** — see below.                                                                                                             |

**Spring Session is the one that goes furthest**, and the shape of what it offers is
instructive. `FindByIndexNameSessionRepository.findByIndexNameAndIndexValue(name, value)`
returns every session whose declared index has that value, and a custom `IndexResolver`
chooses which attributes get indexed at save time. That is an equality index over a
**scalar attribute**, maintained as one set per value — the same structure as our
`sessions-of` key, generalised from one index to several. It still cannot answer "the cart
contains product X", because that is a containment query over a collection inside the
session, and no session framework in any ecosystem does that.

Our `subject_of` seam is the singular form of the same idea. Generalising it to several
named indexes is a reasonable future request, and it is a small change: the store already
abstracts `_index_add`, `_index_remove` and `_index_members`.

#### Why this is the right boundary, not a missing feature

The cart example is the one that shows it. **A cart in a session is a design smell, and
the wish to search it is what exposes the smell.**

- **A session expires; a cart should not.** Both clocks in Section 3.2 will delete the
  session when the user goes idle or hits the absolute deadline. If the business wants to
  know who has product X in their basket — for inventory, for an abandoned-basket email,
  for merchandising — that answer must not depend on whether someone's laptop went to
  sleep.
- **`rotate()` moves the data.** Every sign-in gives the session a new key, so any index
  over its contents must follow the rotation.
- **It rules out encryption at rest.** Section 8.3.1 of `session-mgmt.md` offers an
  `Encryptor`. Indexed contents cannot be encrypted, so a deployment would have to choose
  between the two.
- **It rules out the compact-hash win.** Section 13.2 depends on every session key sharing
  one schema. Per-session indexed fields break the template.
- **A basket is often anonymous**, so there is no subject to hang it from anyway.

**The shape that works** is a pointer. The basket is a domain object with its own key,
its own lifetime and its own index; the session holds only its identifier:

```
redis:fastapi:session:<sid>   ->  d: {"cart_id": "c_8fA2"}      small, uniform, encryptable
cart:c_8fA2                   ->  a hash, or JSON, that you own and index as you like
```

Now the basket survives the session, follows the user to another device when they sign in,
and can be queried with the full query engine — which is what that engine is for. Meanwhile
the session stays small, keeps one schema, and can be encrypted and rotated freely.

Put this in the guide as a short rule, because it is the most common way people misuse a
session store: **keep identifiers in the session, keep entities outside it.**

### 3.5 The record, and where metadata lives

`starsessions` stores its metadata under a `__metadata__` key **inside** the session
payload, so it appears in the application's own `request.session`. Do not copy that.

Field `d` holds an envelope:

```json
{
  "m": {"created": 1756000000.0, "last_access": 1756000042.0, "lifetime": 3600},
  "d": {"user_id": 42, "cart_id": "c_8fA2", "flash": "Settings saved."}
}
```

`request.session` then holds the inner `d` alone, and contains only what the application
put there. The three field names in `m` are the three that `starsessions` uses, so its
`get_session_metadata()` accessors port across as a rename and not a redesign.

The envelope is serialized by the configured `Coder`, then passed through the configured
`Encryptor` if one is set. Encryption wraps serialization, never the reverse: the `Coder`
must never see ciphertext.

---

## 4. What runs on each request

Everything below happens **inside one HTTP request**, every time. "Before the
application" and "after the application" are positions in the ASGI chain, not events in
the server's life:

```
request arrives
└─ SessionMiddleware              ← §4.1 runs here: read the cookie, load from Redis
   └─ the rest of the app         ← "the application": router, dependencies, endpoint
      └─ your endpoint            ← reads request.session / SessionDep, already populated
   ↑ SessionMiddleware            ← §4.2 runs here, at http.response.start
response leaves
```

So "loaded before the application runs" means only this: **by the time any dependency or
endpoint touches the session, the Redis read has already happened.** It has to, because
`HTTPConnection.session` is a synchronous property and cannot await — Section 1.1. The
middleware is the last place in the chain that can still perform an `await`.

### 4.1 Before the application

1. Read the cookie named by `session_cookie_name`.
2. **Validate its value.** Accept only `[A-Za-z0-9_-]`, the alphabet of
   `token_urlsafe`. Anything else is treated as no session at all. This is not
   cosmetic: the value is written back into a `Set-Cookie` header, so an unvalidated
   value is a header-injection vector.
3. No cookie, or a `skip` predicate that returns true: put an empty `Session` into
   `scope["session"]` and call the application. **No Redis call.**
4. Otherwise read and refresh in **one** round trip, pipelined — both commands address
   the same key, so they share a slot and the pipeline is safe on a cluster:

   ```
   HGETEX <key> EX <idle> FIELDS 1 d      -> the payload, and the idle clock restarts
   HTTL   <key> FIELDS 1 a                -> what remains of the absolute deadline
   ```

   `HGETEX` reads a field **and** sets its expiration in one command, so the load *is*
   the idle refresh. There is no second command at response time and nothing to
   optimise away.

5. **Read the two answers as a pair, never one as a sentinel.** Section 3.2 guarantees
   that `a` is always written and always carries a TTL, which is what makes this table
   total:

   | `d`     | `HTTL a` | Meaning                                                                                                  | Action                                      |
   |---------|----------|----------------------------------------------------------------------------------------------------------|---------------------------------------------|
   | present | `> 0`    | alive                                                                                                    | serve it; absolute remaining is that number |
   | present | `-2`     | the absolute deadline passed                                                                             | empty `Session`; `DEL` the key              |
   | absent  | `-2`     | no such session — never existed, expired outright, or revoked                                            | empty `Session`                             |
   | absent  | `> 0`    | the idle clock ran out, absolute has time left                                                           | empty `Session`; `DEL` the key              |
   | any     | `-1`     | **cannot happen** — Section 3.2 forbids an unexpiring `a`. Treat as a bug: log and handle as no session. |                                             |

   An earlier draft collapsed this into "`HTTL` returning `-2` means the absolute deadline
   passed". `-2` is also what Redis answers for a field that was never written and for a
   key that does not exist, so that reading broke every deployment with no absolute limit.
   The pair disambiguates; a single value cannot.

   Deleting the key on rows two and four matters: it lets the index entry follow, rather
   than leaving a candidate that every later verification has to reject.

6. **Take the principal snapshot.** Evaluate `principal_of(session)` and keep the result
   for the response. Section 5.1 explains what it is for; here it costs one call of a pure
   function and no I/O.

The application never sees the difference. An expired session, a revoked one and an absent
one are the same thing to a caller.

The cookie `max-age` for the response is `min(idle, remaining a)`, and both numbers came
from Redis rather than from our own clock.

### 4.2 After the application, at `http.response.start`

Wrap `send`, as `RateLimitMiddleware` does at `ratelimit.py:551`. The two flags on
`Session` decide everything.

| State                  | Redis (`refresh_on_load` on, the default)             | Redis (`refresh_on_load` off) | Cookie                      |
|------------------------|-------------------------------------------------------|-------------------------------|-----------------------------|
| not accessed           | nothing                                               | nothing                       | nothing                     |
| accessed, not modified | **nothing** — step 4 already refreshed the idle clock | `HEXPIRE d <idle>`            | nothing                     |
| modified, non-empty    | `HSETEX` field `d`, **then** `HSETEX` the index entry | same                          | `Set-Cookie`                |
| modified, now empty    | `DEL` the key, then `HDEL` the index entry            | same                          | `Set-Cookie` that clears it |

**Before any of those rows, take the second principal snapshot.** If it differs from the
one taken at step 6 of Section 4.1 **and** the status is below 400, the rotation sequence in
Section 5.2 replaces the row above and emits the new cookie. A changed principal on a
response of 400 or more persists nothing at all — Section 4.3 gives the reason.

**The second row has two answers, and an earlier draft printed only the first.** It read
"nothing — step 4 already refreshed the idle clock", which is true only under the default.
With `refresh_on_load=False` the load is a plain `HGET`, nothing was refreshed, and this
row is the only place left to do it — so omitting the branch made that setting silently
stop the idle clock from ever advancing.

The index write on row three is a re-assertion, not a create: it repeats on every write,
not only at login. `HSETEX` is idempotent and the command is already in the same pipeline,
so it costs nothing, and it repairs an index entry that a partial failure lost. Two
constraints on it, both from Section 3.3: the session key is written **before** the index
entry, and the entry takes the **remaining** absolute time — never a fresh full lifetime,
which would let the entry outlive the session. Section 4.1 has already read that remainder
from `HTTL a`.

Add `Vary: Cookie` whenever `accessed` is true, so a cache never serves one user's page to
another.

**The second row is where `HGETEX` pays.** A read-only request costs exactly one
pipelined round trip for the whole request, and no `refresh_threshold` is needed because
there is no extra command to suppress. An earlier draft carried that setting and defaulted
it to `0.1`, which traded up to ten per cent of idle-timeout precision for a saving that
`HGETEX` gives for free. The setting is gone.

Writing field `d` never disturbs field `a`, so an active session keeps counting down to
its absolute deadline no matter how often it is written.

**One semantic to state plainly.** Because the refresh happens at load, the idle clock
restarts for any request that arrives with a session cookie, whether or not the
application touched `request.session`. That matches PHP, Django with
`SESSION_SAVE_EVERY_REQUEST`, and `express-session` with `rolling`. An application that
wants the stricter reading — only a request that *used* the session counts as activity —
sets `refresh_on_load=False` and takes the second round trip.

### 4.3 What a failed response persists

The intuitive rule — a request that failed writes nothing — is wrong, and three ordinary
patterns break under it:

- **A failed-login counter.** `session["failed_attempts"] += 1` then `raise 401`. Discard
  the write and the counter never increments, so lockout silently stops working.
- **A flash message on error.** `session["flash"] = "Check the form"` then redirect. That
  is the commonest use a flash message has.
- **A CSRF token minted, whose validation then failed.** The retry needs the token kept.

So ordinary session data persists whatever the status. But nothing should hand out an
authenticated session on a request the client saw fail. Two rules:

1. **Session data is written regardless of the response status.**
2. **Except when the principal changed and the status is 400 or more — then nothing is
   written at all.**

The exception is narrow and it exists to close one hole. Persisting the data while skipping
the rotation would leave `session["user_id"] = 42` stored against the *old*, unrotated ID:
an authenticated session with an identifier the client already had. That is precisely the
fixation this design exists to prevent, arrived at by being helpful.

---

## 5. Rotation, and correctness without atomicity

Rotation is the defence against session fixation, and Section 3 of `session-mgmt.md`
makes it mandatory after authentication and after any change of privilege.

### 5.1 What triggers it: the principal changed

**The application never calls rotation.** The middleware detects it.

The middleware evaluates a pure function of the session, the **principal**, twice: once
before the application runs and once at `http.response.start`. If the two differ and the
response status is below 400, it rotates.

This is the design's central safety property. Rotation-forgotten is the only mistake in
this API that is a vulnerability, and there is no call to forget. It follows that:

- An application signs a user in by writing the identity. Nothing else.
- Code we did not write gets the same protection. An application migrating off Starlette's
  signed cookie keeps its existing `request.session["user_id"] = …` and acquires fixation
  defence without touching the handler — and Section 9.3.2 of `session-mgmt.md` makes that
  the largest population of adopters.
- The failure asymmetry runs the right way. Rotating when we need not is harmless — a new
  cookie carrying the same data. Not rotating when we should is the vulnerability. A
  detector biased toward firing is therefore the safe bias.

#### Principal and subject are two different questions

They pull in opposite directions, so they are two functions:

| | Answers | Must be |
|---|---|---|
| `subject_of(session)` | which key indexes this session | **stable** per user, or `revoke_all` breaks across a role change |
| `principal_of(session)` | what must not change without rotation | **sensitive** to privilege |

`principal_of` defaults to `subject_of`, so an application that only cares about sign-in
configures nothing. One that wants OWASP's privilege-change rotation declares what counts:

```python
FastAPIRedis(app).sessions(principal_keys=["user_id", "role"])
```

Now `session["role"] = "admin"` rotates on its own — and so does dropping back to `"user"`,
which OWASP also requires and which an explicit call is especially easy to forget on the
way down. A callable is the escape hatch when a list of keys will not do:

```python
FastAPIRedis(app).sessions(
    principal_of=lambda s: (s.get("user_id"), s.get("role"), s.get("tenant")),
)
```

Prefer the list. It is greppable, it is reviewable — "what does this application consider
privilege?" is answerable without reading code — and it cannot accidentally perform I/O.

An explicit `reauthenticate()` remains for the one case this cannot see: a privilege change
with no trace in session state, such as re-entering a password before a sensitive action.
That genuinely is an event rather than a state change, and reads correctly as a call.

#### The sequence

```
Client          SessionMiddleware       principal_of     SessionStore        Redis
  │                     │                    │                │               │
  ├─ POST /login ──────▶│                    │                │               │
  │  Cookie: sess=OLD   │                    │                │               │
  │                     ├─ validate charset  │                │               │
  │                     ├────────────────────┼────────────────┼──────────────▶│
  │                     │   pipeline:  HGETEX sess:OLD … d   /  HTTL … a      │
  │                     │◀───────────────────┼────────────────┼───────────────┤
  │                     ├─ build Session(d)  │                │               │
  │                     ├───────────────────▶│                │               │
  │                     │    before = None ◀─┤   snapshot BEFORE the app      │
  │            ┌────────▼───────────────────────────────┐     │               │
  │            │  the app: router → deps → endpoint     │     │               │
  │            │     user = authenticate(...)           │     │               │
  │            │     session["user_id"] = 42            │     │               │
  │            └────────┬───────────────────────────────┘     │               │
  │                     │  200               │                │               │
  │                     ├───────────────────▶│                │               │
  │                     │    after = "42"  ◀─┤   snapshot at response.start   │
  │                     │                    │                │               │
  │                     ├── changed, and status < 400 ───────▶│               │
  │                     │                    │   rotate()     ├─ DEL sess:OLD▶│
  │                     │                    │                ├─ HDEL index ─▶│
  │                     │                    │                ├─ HSETEX a ───▶│
  │                     │                    │                ├─ HSETEX d ───▶│
  │                     │                    │                ├─ HSETEX ─────▶│
  │                     │                    │                │  sessions-of:42
  │◀── 200 ─────────────┤                    │                │               │
  │  Set-Cookie: NEW    │                    │                │               │
```

`principal_of` is called **exactly twice** per request. When the two snapshots match — every
ordinary request — the middleware skips this entirely and falls through to the write rule
in Section 4.2.

#### What the detector must guarantee

`principal_of` is user-supplied and a security control depends on it, so four rules are
not optional.

| Situation | Rule |
|---|---|
| It raises at the response snapshot | **Fail the response.** We cannot tell whether a privilege transition happened, and both guesses are unsafe — rotating with no subject leaves an un-indexed, un-revocable session. A loud 500 beats either. |
| It is impure, does I/O, or is non-deterministic | Rejected by contract. Document that it must be pure and cheap; the two results are compared by value. |
| It returns unverified input, such as an email the client set | It must return a **verified** identity. The blast radius is bounded — the index grants nothing, so the worst case is a polluted device listing rather than an escalation — but say so. |
| Identity exists but authentication is incomplete, as at MFA step one | Not a defect, and `principal_of` is the right place to express it: `lambda s: s.get("user_id") if s.get("mfa_ok") else None`. |

**And a misconfiguration is silent**, which is the one genuine cost of detecting rather
than being told. If an application stores `uid` while the resolver reads `user_id`, nothing
rotates and nothing complains. Two mitigations, and the first is a deliverable:

- **Ship a test helper.** An assertion that a given login flow changes the cookie value
  turns a configuration risk into a property the application proves once. Section 11 lists
  it.
- The `operation="rotate"` counter in Section 10 is the runtime backstop: sign-ins with no
  rotations is a visible anomaly on a dashboard.

Note that the alternative is not safer here. Any design where the application calls
rotation must get that call right on **every** path that establishes identity — password,
OAuth callback, magic link, SAML, MFA step two, impersonation. Detection concentrates the
risk in one place that is configured once and tested once.

### 5.2 How it runs: ordering, not atomicity

We already hold the payload in memory, so no read is needed. The order is:

```
1. DEL     redis:fastapi:session:<old_sid>
2. HDEL    redis:fastapi:sessions-of:<subject>  <old_sid>
3. HSETEX  redis:fastapi:session:<new_sid>  EX <absolute>  FIELDS 1 a 1
4. HSETEX  redis:fastapi:session:<new_sid>  EX <idle>      FIELDS 1 d <record>
5. HSETEX  redis:fastapi:sessions-of:<subject>  EX <absolute>  FIELDS 1 <new_sid> <descriptor>
6. Set-Cookie with <new_sid>
```

Step 3 restarts the absolute clock, which is correct: rotation follows authentication or
a change of privilege, so a new session begins. Step 5 is therefore the one place where
the index entry legitimately takes the **full** absolute lifetime rather than a remainder
— the session it describes was created in step 3, one command earlier. Every other write
of that entry uses the remainder, for the reason in Section 3.3.

When `absolute_ttl` is `0`, step 3 still runs and `a` takes `gc_ttl`, per Section 3.2.
There is no branch in which `a` goes unwritten.

**Delete before write, and the order is the security control.** A crash between steps 1
and 3 signs the user out, and they sign in again. A crash in the other order would leave
two session IDs valid for the same authenticated session, which is the fixation window
that Section 7 of `session-mgmt.md` criticises in `starsessions`, whose `regenerate_id()`
keeps the old ID until a later `save()` that may never run.

So we do not need a transaction. Ordering gives the property that a transaction would
have given, and it gives it on a cluster too, where a transaction is not available.

Steps 1 and 2 go in one pipeline, and steps 3 and 4 in a second. Two pipelines and not
one, because the boundary between them is the ordering guarantee. `transaction=False`:
on a cluster, redis-py splits a pipeline across nodes by slot, and these keys are on
different slots by design.

`revoke()` is steps 1, 2 and 5 alone. `revoke_all(subject)` prunes, lists, deletes every
member in one pipeline, then deletes the index key.

### 5.3 The reverse lookup is not atomic either, and does not need to be

**A pipeline batches; it does not isolate.** Redis executes each command atomically, but
another client's commands can land between ours. On a cluster it is worse than that: the
session key and the index key are on different slots, so redis-py sends them to different
nodes and they are genuinely concurrent. The window is a network round trip, not the
microseconds between two back-to-back commands on a single-threaded server.

So the question is what an interleaving can actually produce. There are only three
outcomes, and only one of them matters.

| Outcome                                                               | How                                                                               | Harm                                                                                                           |
|-----------------------------------------------------------------------|-----------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------|
| index entry exists, session gone                                      | `revoke()` interleaved between `DEL` and `HDEL`; or a session that expired by TTL | **none.** The field expires on its own, and a `revoke_all` that acts on it deletes a key that is already gone. |
| session exists, created after a concurrent `revoke_all` read its list | login racing a password change                                                    | **bounded.** The session is fully consistent and revocable next time. See the semantics below.                 |
| **session exists, index entry does not**                              | the index write failed after the session write succeeded                          | **this is the one.** The session is invisible to `revoke_all` until its absolute deadline.                     |

**The third row is the only failure worth engineering against**, because it is the same
fault this design refuses in Section 3.3 when rejecting the query engine: a session that
`revoke_all` silently misses.

**Atomicity would not fix the second row.** Even if create were one atomic unit, the
interleaving could be: the revoker reads the index and sees no new session, we atomically
write both keys, the revoker deletes what its stale list contained. The race is between
the revoker's snapshot and our write, not inside our write, so a transaction around our
half changes nothing. This is worth stating because it is the intuitive fix and it does
not work.

Three measures instead, none of them a transaction:

**1. Write the session key before the index entry.** The current order is not arbitrary.
Reversed, a concurrent `revoke_all` landing between the two deletes the index entry we
just wrote and then our session write lands — producing the third row, an orphaned and
un-revocable session. In the current order the same interleaving produces the second row,
which is consistent and revocable. **The ordering converts the dangerous outcome into the
harmless one**, exactly as it does for rotation above.

**2. Re-assert the index entry on every session write, not only at creation.** `HSETEX` is
idempotent, and the write is already in the pipeline that Section 4.2 issues for a
modified session, so this costs no round trip and no extra command in the common case.
It turns "un-revocable until the absolute deadline" into "un-revocable until this user's
next request that changes the session". A partial failure then repairs itself instead of
persisting for hours.

**Re-assert with the remaining absolute time, not a fresh lifetime.** Section 3.3 shows
what a relative `EX <absolute>` does here: it restarts the entry's clock on every write,
so a session in constant use holds an entry that never expires. Section 4.1 has already
read the remainder from `HTTL a`, so the correct value is in hand at no cost. This is the
easiest of the three measures to get subtly wrong, because the wrong version looks
identical and fails only on long-lived sessions.

**3. Bound it regardless.** The index entry's TTL is the session's absolute deadline, so
even an orphan that is never repaired dies with the session it describes. Nothing leaks
past the absolute clock — provided measure 2 uses the remainder, which is the other reason
that detail matters.

**A note on the reverse direction.** These measures make an entry that is *missing* rare
and self-healing. They do nothing about an entry that is *present but stale*, because the
index tracks the absolute clock while sessions usually die of idleness. That is not a race
at all but a structural property, and Section 3.3 handles it by verifying candidates on
read rather than trusting the membership.

**State `revoke_all`'s semantics rather than implying stronger ones.** It ends every
session that existed when it read the index. A session created concurrently may survive,
and that is not only unavoidable but arguably right: it did not exist when the caller
asked. The documentation must say this, because "sign out everywhere" reads like a
guarantee about the future and is not one. Where the guarantee genuinely matters —
credential compromise — the caller must change the credential first and revoke second, so
that a racing login cannot succeed.

**The alternative we are not taking.** A generation counter per subject would give strict
semantics: `revoke_all` becomes one atomic `INCR`, every session records the generation it
was born under, and any session older than the current value is dead. No enumeration, no
race. The cost is that every request must compare the two values, and the subject is
inside the session record, so learning it requires the read we have just done — a second
round trip on the request path, doubling its Redis cost. That is too much to pay for a
race this narrow, but it is the right answer for a deployment that needs revocation to be
strictly linearizable, and the store's abstract primitives leave room to add it.

---

## 6. The two clocks

Section 3.2 puts each clock on its own hash field, so **Redis enforces both and the store
computes neither**. What remains here is the cookie, which Redis cannot enforce.

The cookie `max-age` must agree with whichever clock will fire first:

```
max_age = min(idle_ttl, HTTL(key, "a"))
```

Both numbers come from Redis — the configured idle window, and the absolute remainder
that the server itself is counting down. Nothing is derived from the application's own
clock, so a container with a skewed clock cannot produce a cookie that disagrees with the
record.

**If the cookie and the record ever disagree, the browser deletes a cookie whose session
is still alive, and the user is signed out with no cause and no log line.** Section 8.3.2
of `session-mgmt.md` records that failure. Deriving both from the same two server-side
numbers is what prevents it.

In cookie-only mode the cookie carries no `max-age` at all and the browser decides.

### Translation from `starsessions`

Their `rolling=True` extends the cookie and the record by the full lifetime on every
response: that is our idle clock, field `d`. Their `rolling=False` keeps the original
expiry: that is our absolute clock, field `a`. We can express both, and we can run the
two together, which their single clock cannot.

---

## 7. When Redis is unreachable

`session_fail_closed` mirrors the existing `rate_limit_fail_closed`. The default is
**asymmetric**, and the asymmetry is the point.

**A read that fails yields an empty session.** The request continues, and the user looks
anonymous. Nothing is silently permitted: the application's own authorization dependency
still runs, finds no user, and returns a login page or a 401. A protected route stays
protected, because it never depended on the session load succeeding. Log at `warning` and
record a `result="error"` metric.

**A write that fails raises `SessionStoreError`.** Losing a login, or losing a rotation,
is the worst outcome in this design, and it must never be silent. A rotation that half
fails has already deleted the old key, so the user is signed out — safe, and Section 5
explains why that ordering was chosen.

Setting `session_fail_closed=True` turns the failed read into a `SessionStoreError` too,
for a deployment that would rather return 503 than serve an anonymous page.

### Exceptions

```
SessionError                     base, so a caller can catch the whole feature
├── SessionConfigurationError    a missing or invalid setting
└── SessionStoreError            the store failed; wraps the driver error
```

Never let a `redis.RedisError` reach application code. There is no equivalent of
`SessionNotLoaded`: our load is automatic.

---

## 8. The `Session` class

A `dict` subclass with `accessed` and `modified`, and `mark_accessed()` under exactly
that name, so the `hasattr` hook in Starlette 1.0 and later finds it and calls it for us.
Section 5a of `session-mgmt.md` gives that evidence.

It overrides what the upstream class overrides — `__setitem__`, `__delitem__`, `clear`,
`update`, `setdefault` — and then corrects the four faults from Section 6.1 of
`session-mgmt.md`:

| Fault upstream                                                                    | Ours                                                                                                             |
|-----------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------|
| `popitem()` sets no flag                                                          | override it                                                                                                      |
| `\|=` sets no flag, because `dict.__ior__` runs in C and never reaches `update()` | override `__ior__`                                                                                               |
| `pop()` sets `modified` but not `accessed`                                        | call `mark_modified()`, which sets both                                                                          |
| a nested change, `session["a"]["b"] = 1`, sets no flag                            | **unfixable in any `dict` subclass.** The store exposes `save()`, and a setting forces a write on every request. |

The fourth row is a limit of the language and not a defect we inherited, so no release of
Starlette will remove it. The escape route is the answer.

---

## 9. Public API

### The store

`SessionStore` is an abstract base class, not a bare protocol. Section 7 of
`session-mgmt.md` gives the reason: the lifecycle is a security control, and a base class
implements it once rather than once for each vendor.

**Concrete on the base class** — written once, including the TTL rules and every telemetry
call:

| Method                               | Signature                           | Purpose                                                                                                                              |
|--------------------------------------|-------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------|
| `rotate` | `(session, *, subject=None) -> str` | New ID, old key deleted first, cookie updated. **Normally driven by the middleware** when the principal changes (Section 5.1); public for the rare handler that must force one. |
| `reauthenticate` | `(session) -> None` | Force a rotation on the way out, for a privilege change that leaves no trace in session state. |
| `revoke`                             | `(session) -> None`                 | End the session in hand and clear its cookie.                                                                                        |
| `revoke_id`                          | `(session_id, *, subject) -> bool`  | End one session by ID. **Requires the subject** and refuses an ID not indexed under it, so a caller cannot end a stranger's session. |
| `revoke_all`                         | `(subject) -> int`                  | End every session for a subject. Returns the number ended.                                                                           |
| `list_for_subject`                   | `(subject) -> list[SessionInfo]`    | Live sessions with their descriptors. Verifies liveness before returning (Section 3.3).                                              |
| `count_for_subject`                  | `(subject) -> int`                  | Live count. `HLEN` fast path, verified only when it reaches a limit.                                                                 |
| `session_id`                         | `(session) -> str \| None`          | The current ID, for marking "this device" in a listing.                                                                              |
| `save` / `touch` / `delete` / `load` |                                     | The lifecycle the middleware calls; rarely needed directly.                                                                          |
| `new_id`                             | `() -> str`                         | Generates and **validates** an ID.                                                                                                   |

`SessionInfo` is a frozen dataclass — `session_id`, `created`, `last_access`, `descriptor`
— so a listing needs no read of the session records themselves.

**Abstract, and this is the whole surface a new backend implements:** `_read`, `_write`,
`_expire`, `_delete`, `_index_add`, `_index_remove`, `_index_members`.

`SessionStoreProtocol` describes only what the middleware and the dependencies call, so a
test or another package can supply an object with no inheritance.

#### Sync endpoints

FastAPI serves `def` handlers as readily as `async def` ones, so the feature must work in
both with one pattern to learn, not two.

**Reading and writing a session already needs nothing.** `SessionDep` is a dict in the ASGI
scope and the middleware performs all the I/O, so a `def` handler uses it unchanged and
awaits nothing:

```python
@app.post("/settings")
def save(theme: str, session: SessionDep) -> dict:
    session["flash"] = "Saved."          # no await anywhere
    return {"ok": True}
```

**The imperative store operations get a facade.** `SyncSessionStore` mirrors
`SyncCacheBackend` (`cache_backend.py:367`) and `SyncRateLimitBackend`
(`ratelimit_backend.py:459`): each method delegates through `anyio.from_thread.run`, and
`SyncSessionStoreDep` injects it. Copy the warning those two carry — it **only** works from
a FastAPI-managed worker thread, and raises `RuntimeError` elsewhere. State the remedy
explicitly for this feature, because "expire old sessions from a scheduled job" is a more
tempting misuse here than its equivalent is for a cache: outside a request, use the async
store.

Two notes the implementation must not lose.

**Thread safety is fine, and a reviewer will ask why.** A `def` handler mutates
`scope["session"]` on a worker thread while the middleware reads `accessed` and `modified`
on the event-loop thread afterwards. There is no race: Starlette runs the handler through
`await anyio.to_thread.run_sync(...)`, so the thread's completion orders every write before
the middleware's read.

**A blocking call costs a worker thread for a whole round trip**, and anyio's default pool
is **40** threads. That is affordable for `revoke_all` or a device listing, which happen
once in a while. It would not be affordable for something on the sign-in path of every
request, which is one reason the sign-in shape is still open.

### Setup and injection

One line enables it, beside the calls this package already has:

```python
from fastapi import FastAPI
from redis_fastapi import FastAPIRedis

app = FastAPI()
FastAPIRedis(app).lifespan().sessions()
```

`SessionDep` gives the `Session` for the current request. `SessionStoreDep` gives the
store for the operations that reach beyond this request — `revoke_id()`,
`list_for_subject()`, `count_for_subject()` and `revoke_all()`. Both resolve
through `Depends`, so `dependency_overrides` works, which this package already
advertises.

The cookie appears in OpenAPI through `fastapi.security.APIKeyCookie`, which answers the
original complaint in [fastapi#754](https://github.com/fastapi/fastapi/issues/754) using
the primitive tiangolo recommended there.

#### Reading and writing a session

`SessionDep` is a `dict`. Nothing is loaded by hand and nothing is saved by hand — the
middleware writes at response time, and only if the data changed (Section 4.2).

The two things below are what a session is *for*: **state that should cease to exist when
the session does.** Where the user was heading before being asked to sign in, and a message
to show them once on the next page. If either is lost because the session expired, that is
the correct outcome and not data loss.

```python
from typing import Annotated
from fastapi import Depends, Request
from fastapi.responses import RedirectResponse
from redis_fastapi import SessionDep


@app.get("/admin")
async def admin(session: SessionDep):
    if "user_id" not in session:
        session["next"] = "/admin"          # remember the destination, then ask them in
        return RedirectResponse("/login")
    return {"panel": "..."}


@app.post("/settings")
async def save_settings(theme: str, session: SessionDep) -> dict:
    await db.save_theme(session["user_id"], theme)
    session["flash"] = "Settings saved."    # to be shown exactly once
    return {"ok": True}


@app.get("/dashboard")
async def dashboard(session: SessionDep) -> dict:
    return {
        "flash": session.pop("flash", None),   # reading it also clears it
        "user_id": session.get("user_id"),
    }
```

Three details worth naming:

- **Assignment is the trigger.** `session["next"] = …` marks the session modified, and the
  middleware writes it at `http.response.start`. There is no `save()` to forget.
- **`pop()` counts as a change**, so consuming the flash message persists its removal. This
  is the method whose upstream version forgets to mark the session accessed, which is why
  Section 8 overrides it.
- **Reading alone writes nothing.** The `/dashboard` handler above does write, because
  `pop()` mutated. A handler that only called `session.get(...)` would issue no Redis
  command at response time at all.

For what does **not** belong in here — a shopping cart's contents, an order, any entity
with a life of its own — see Section 3.4. The rule there is to keep identifiers in the
session and entities outside it.

**`request.session` keeps working**, which is what makes Authlib and any existing code
run unchanged (Section 5a of `session-mgmt.md`). Use whichever fits:

```python
@app.get("/whoami")
async def whoami(request: Request) -> dict:
    return {"user_id": request.session.get("user_id")}
```

#### A dependency that requires a signed-in user

The common shape. Note that it needs no knowledge of Redis, and that it is what makes the
fail-open read in Section 7 safe: a failed load yields an empty session, so this rejects.

```python
from fastapi import HTTPException, status


async def current_user(session: SessionDep) -> int:
    user_id = session.get("user_id")
    if user_id is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Not signed in")
    return user_id


CurrentUser = Annotated[int, Depends(current_user)]


@app.get("/orders")
async def my_orders(user_id: CurrentUser) -> list[dict]:
    return await db.orders_for(user_id)
```

### The store, used directly

`SessionStoreDep` is for the operations a session dict cannot express. These four cover
almost every real use.

#### Recipe 1 — sign in, sign out, and change privilege

**There is no sign-in call.** Writing the identity *is* signing in; the middleware sees the
principal change and rotates. Section 5.1 gives the mechanism.

```python
from redis_fastapi import SessionDep


@app.post("/login")
async def login(form: LoginForm, session: SessionDep) -> dict:
    user = await authenticate(form.username, form.password)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bad credentials")

    session["user_id"] = user.id      # principal changed → rotated on the way out
    return {"ok": True}


@app.post("/logout")
async def logout(session: SessionDep) -> dict:
    session.clear()                   # emptied → key deleted, cookie cleared
    return {"ok": True}
```

Three things are absent on purpose: no store dependency, no `await`, and no rotation call
to forget. The handler is identical as a `def`, because nothing in it touches Redis.

**Privilege changes rotate too, once declared.** With `principal_keys=["user_id", "role"]`
from Section 5.1, a role change is a principal change:

```python
@app.post("/elevate")
async def become_admin(session: SessionDep, user_id: CurrentUser) -> dict:
    await audit.record(user_id, "elevated")
    session["role"] = "admin"         # principal changed → rotated
    return {"ok": True}
```

The `raise` in the first handler needs no undo. A principal that changes on a response of
400 or more persists nothing at all — Section 4.3.

For a privilege change that leaves no trace in the session — re-entering a password before
a sensitive action, say — there is an explicit `reauthenticate()`. It is the exception, not
the pattern.

#### Recipe 2 — sign out everywhere

For a password change, an administrator forcing a sign-out, or a user who thinks a device
was stolen. This is the reverse lookup in Section 3.3 earning its keep.

```python
@app.post("/password")
async def change_password(
    new_password: str,
    user_id: CurrentUser,
    store: SessionStoreDep,
) -> dict:
    await db.set_password(user_id, new_password)     # credential first
    ended = await store.revoke_all(subject=str(user_id))
    return {"sessions_ended": ended}
```

**Change the credential before revoking, as above.** Section 5.3 explains why: a session
created between the read and the delete may survive, so the new password must already be
in force for a racing sign-in to be harmless.

To end only the current session, `revoke()` takes no subject:

```python
@app.post("/logout")
async def logout(session: SessionDep, store: SessionStoreDep) -> dict:
    await store.revoke(session)          # deletes the key, clears the cookie
    return {"ok": True}
```

#### Recipe 3 — "you are signed in on these devices"

`list_for_subject()` returns the live sessions with the descriptor each one carries, so no
session record has to be read. It verifies liveness before returning, per Section 3.3, so
an idle-dead session never appears.

```python
@app.get("/account/sessions")
async def my_sessions(
    user_id: CurrentUser,
    session: SessionDep,
    store: SessionStoreDep,
) -> list[dict]:
    current = store.session_id(session)
    return [
        {
            "created": s.created,
            "last_seen": s.last_access,
            "ip": s.descriptor.get("ip"),
            "device": s.descriptor.get("ua"),
            "current": s.session_id == current,
        }
        for s in await store.list_for_subject(str(user_id))
    ]


@app.delete("/account/sessions/{session_id}")
async def end_one(
    session_id: str,
    user_id: CurrentUser,
    store: SessionStoreDep,
) -> dict:
    # Scope the delete to this user, or one user could end another's session.
    await store.revoke_id(session_id, subject=str(user_id))
    return {"ok": True}
```

**Never take the session ID from the client without scoping it to the caller.**
`revoke_id` requires the subject for that reason: it refuses an ID that is not indexed
under that subject.

#### Recipe 4 — cap concurrent sessions

Section 3 of `session-mgmt.md` lists this as a control. `count_for_subject()` is the
`HLEN` fast path from Section 3.3 — an upper bound, so a count under the limit needs no
verification, and only a count at the limit pays for it.

```python
MAX_SESSIONS = 3


@app.post("/login")
async def login_capped(
    form: LoginForm,
    session: SessionDep,
    store: SessionStoreDep,
) -> dict:
    user = await authenticate(form.username, form.password)
    if user is None:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Bad credentials")

    subject = str(user.id)
    if await store.count_for_subject(subject) >= MAX_SESSIONS:
        # Either refuse the new sign-in...
        raise HTTPException(status.HTTP_409_CONFLICT, "Too many active sessions")
        # ...or evict the oldest, which is usually the better product decision:
        # oldest = min(await store.list_for_subject(subject), key=lambda s: s.created)
        # await store.revoke_id(oldest.session_id, subject=subject)

    session["user_id"] = user.id      # principal changed → rotated on the way out
    return {"ok": True}
```

This is the case that mixes both styles, and the mix is the point. The **check** reads
Redis, so it is awaited; the **sign-in** is still just a write. And the `raise` needs no
undo, because a principal that changed on a 409 persists nothing (Section 4.3).

#### Testing against the store

`dependency_overrides` works because both dependencies resolve through `Depends`. Unit
tests need no override at all — point the pool at `fakeredis`, exactly as
`tests/conftest.py:113` already does, and the real store runs against the real commands
(Section 8.3.3 of `session-mgmt.md`).

### Extension points

Every seam from Section 9.1 of `session-mgmt.md`, as a signature:

| Seam              | Signature                                                            |
|-------------------|----------------------------------------------------------------------|
| serialization     | `Coder`, from `types.py:15`                                          |
| encryption        | `Encryptor`: `encrypt(bytes) -> bytes`, `decrypt(bytes) -> bytes`    |
| storage           | `SessionStore` subclass, or `SessionStoreProtocol`                   |
| key names         | `key_prefix: str \| Callable[[str], str]`                            |
| ID format         | `id_factory: Callable[[], str]` — **validated on output**, see below |
| index subject     | `subject_of: Callable[[Session], str \| None]`                       |
| rotation trigger | `principal_keys: list[str]`, or `principal_of: Callable[[Session], Hashable]`; defaults to `subject_of`. See Section 5.1. |
| cookie attributes | `cookie_builder: Callable[[CookieSpec], str]`                        |

**The cookie builder earns its place.** Section 2a of `session-mgmt.md` records that the
Starlette middleware cannot emit `Partitioned` and cannot use a `__Host-` prefix, and
that this is a common reason people abandon it. A seam here means a user adds the
attribute instead of waiting for our release.

`subject_of` returning `None` disables the index for that session, which is correct for
an anonymous one: no subject, no index entry, and `revoke_all` has nothing to promise.

**Validate what `id_factory` returns, on every call.** Apply the same character rule that
Section 4.1 applies to an incoming cookie — `[A-Za-z0-9_-]` — plus a minimum length, and
raise `SessionConfigurationError` on a violation rather than writing the key.

A seam that supplies a security-critical value has to be checked, not trusted. Section 3.1
records what a custom factory can otherwise do: returning `"by-subject:42"` built a key
that collided with the index key under the old layout. That particular collision is now
structurally impossible, but the general point stands — an ID containing a separator, or
one that is far too short, is a defect this package should refuse rather than store. The
check is one regular expression on a path that runs once per session.

### Settings

Fields on `RedisSettings`, beside the existing `rate_limit_*` ones and following the same
`Field(default=…, description=…)` style. The env prefix is `REDIS_`
(`config.py:207`), so `session_idle_ttl` is set by `REDIS_SESSION_IDLE_TTL`.

| Setting                     | Type                          | Default             | Description                                                                                                                                                                                                                                                                                                                                                     |
|-----------------------------|-------------------------------|---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `session_cookie_name`       | `str`                         | `"session"`         | Name of the session cookie. Matches Starlette's and `starsessions`' default so a migration keeps existing cookie names. Validated at construction: letters, digits, `-` and `_` only.                                                                                                                                                                           |
| `session_cookie_domain`     | `str \| None`                 | `None`              | `Domain` attribute. `None` scopes the cookie to the exact host that set it. Setting it also exposes the cookie to subdomains.                                                                                                                                                                                                                                   |
| `session_cookie_path`       | `str`                         | `"/"`               | `Path` attribute. Narrow it (e.g. `"/admin"`) and other paths neither send nor receive the cookie.                                                                                                                                                                                                                                                              |
| `session_cookie_same_site`  | `"lax" \| "strict" \| "none"` | `"lax"`             | `SameSite` attribute. `"none"` requires `session_cookie_https_only=True`; the two are checked together and a contradiction raises `SessionConfigurationError`.                                                                                                                                                                                                  |
| `session_cookie_https_only` | `bool`                        | `True`              | Adds `Secure`, so the browser sends the cookie over HTTPS only. **On by default**; turn it off for local development over plain HTTP and nowhere else.                                                                                                                                                                                                          |
| `session_idle_ttl`          | `int \| timedelta`            | `1800` (30 min)     | The idle clock. The session dies this long after the last request that carried its cookie. Stored as the TTL of field `d`. `0` disables the idle clock.                                                                                                                                                                                                         |
| `session_absolute_ttl`      | `int \| timedelta`            | `28800` (8 h)       | The absolute clock. The session dies this long after creation however active the user is. Stored as the TTL of field `a`. `0` disables it, in which case `a` takes `session_gc_ttl` — see Section 3.2, which explains why `a` is never left unexpiring.                                                                                                         |
| `session_gc_ttl`            | `int \| timedelta`            | `2592000` (30 days) | Backstop TTL for a key whose real deadline is unknown: cookie-only mode, or `session_absolute_ttl=0`. Never reached in normal operation; it exists so Redis can always collect an abandoned key.                                                                                                                                                                |
| `session_refresh_on_load`   | `bool`                        | `True`              | `True`: the load uses `HGETEX`, so any request carrying the cookie restarts the idle clock in the same round trip. `False`: the load uses `HGET` and only a request that touched `request.session` refreshes it, at the cost of a second round trip. Section 4.2 gives both branches.                                                                           |
| `session_fail_closed`       | `bool`                        | `False`             | Behaviour when Redis is unreachable **on read**. `False` yields an empty session, so the caller looks anonymous and the application's own authorization rejects them. `True` raises `SessionStoreError` instead, for a deployment that prefers a 503 to an anonymous page. **Writes always raise, whatever this is set to** — Section 7 explains the asymmetry. |
| `session_always_save`       | `bool`                        | `False`             | Write the payload on every request that touched the session, even when no mutation was detected. The escape route for the one fault no `dict` subclass can see: a change inside a nested value, `session["a"]["b"] = 1` (Section 8). Costs a write per request; prefer reassigning the top-level key.                                                           |
| `session_principal_keys` | `list[str]` | `["user_id"]` | Session keys the rotation trigger watches. A change to any of them on a successful response rotates the ID. Add `"role"` or `"scopes"` for OWASP's privilege-change rotation. Section 5.1; use `principal_of` when a list of keys cannot express it. |
| `session_events_enabled`    | `bool`                        | `False`             | Subscribe to Redis notifications and call registered handlers when a session ends (Section 13.4, F-21). **Best-effort.** On a server below 8.8, or one where `notify-keyspace-events` lacks the subkey flags, or where `CONFIG GET` is unavailable, the store logs one warning at startup and the handlers never fire. Never enable the server setting on the operator's behalf.                                              |
| `session_key_prefix`        | `str \| None`                 | `None`              | Overrides the key namespace. `None` uses `settings.pattern_prefix()`, giving `redis:fastapi:session:` and `redis:fastapi:sessions-of:`. A callable prefix is a constructor argument rather than a setting, since an environment variable cannot carry one (Section 9, extension points).                                                                        |

#### Three things the §5 list in `session-mgmt.md` names that are deliberately not settings

- **Cookie-only mode** is not a flag. It is what you get with `session_idle_ttl=0` **and**
  `session_absolute_ttl=0`: no `max-age` on the cookie, so the browser drops it when it
  closes, and `session_gc_ttl` on both fields so Redis can still collect the key
  (Section 3.2). A separate flag would be a second way to say the same thing, and the two
  could disagree.
- **Encryption** is configured by passing an `Encryptor`, not by an environment variable.
  A key is a secret with a lifecycle, and the seam takes an object so it can come from a
  KMS or rotate without a restart. Off unless one is supplied — Section 8.3.1 of
  `session-mgmt.md`.
- **A renewal timeout** — OWASP's optional re-rotation of a live session on a schedule —
  has no setting, because v1 has no such timer. Note that this is *not* the same as the
  rotation policy, which `session_principal_keys` does express: rotation on authentication
  and on privilege change is automatic (Section 5.1). What is missing is only the
  time-based variety. Record it as a gap rather than implying the knob exists.

Two notes on the defaults.

**Cookie defaults are strict** — `HttpOnly` always, `SameSite=Lax`, `Secure` on.
`starsessions` also defaults to strict and that is the right call: a permissive default is
a vulnerability that nobody reads the documentation to discover.

**The two TTL defaults are deliberately different from each other**, because they are
different controls. 30 minutes of idle and 8 hours absolute is the shape OWASP describes
for an application someone uses through a working day: inactivity signs you out quickly,
and no session survives past the day regardless. Both accept a `timedelta`, as `cache()`
already does in this package.

An earlier draft also carried `refresh_threshold`, to suppress a second round trip that
only existed because the idle refresh was a separate `EXPIRE`. `HGETEX` folds that refresh
into the load, so the setting has nothing left to save and is gone. Section 4.2 gives the
reasoning.

---

## 10. Telemetry

Follow `telemetry.py` exactly: fields on `_OTelState`, a `session_span()` beside
`cache_span` and `ratelimit_span`, guarded `record_session_*()` free functions, and a
`timed_session()` context manager. Keep both existing behaviours — every helper does
nothing when the import failed, and `disable_telemetry()` resets the state.

| Instrument                          | Type      | Attributes                                                                                |
|-------------------------------------|-----------|-------------------------------------------------------------------------------------------|
| `redis_fastapi.sessions.operations` | counter   | `operation` = load/save/touch/rotate/revoke/revoke_all; `result` = hit/miss/expired/error |
| `redis_fastapi.sessions.latency`    | histogram | `operation`                                                                               |
| `redis_fastapi.sessions.events`     | counter   | `cause` = idle/absolute/revoked; `result` = delivered/dropped                             |

**No session ID may become a span attribute or a metric label.** Neither may a subject
identifier, which is usually a user ID. Section 5 of `session-mgmt.md` makes this an
acceptance criterion, and a test asserts it. Cardinality is the lesser reason; the real
one is that traces and metrics reach dashboards and third-party vendors that the session
store's threat model never considered.

---

## 11. Tests

Unit tests in `tests/unit/`, against the existing `fake_async_redis` fixture at
`tests/conftest.py:113`. Integration tests in `tests/integration/`, behind the existing
`@requires_redis` marker. This is the same split the cache and rate-limit suites use, and
`noxfile.py:102` already runs the unit suite with no Redis server.

**`fakeredis 2.36.2` supports every command this design uses** — `HSETEX`, `HGETEX`,
`HEXPIRE`, `HTTL`, `HGETDEL` and `GETEX` all behave correctly against it, including field
expiry and the empty-key deletion in Section 3.3. That was verified before the design was
settled. Section 13.5 refuses `IFEQ` and `DELEX` on a stronger ground than tooling — they
are string commands and cannot address a hash field at all — but note that `fakeredis` does
not support them either, so they could not have been covered here in any case.

**Unit**

- Each row of the `Session` table in Section 8: `popitem()`, `|=`, `pop()` setting both
  flags, and a nested change surviving through `save()`.
- The response rule in Section 4.2: no command when untouched, **no command when read**
  because the load already refreshed, `HSETEX` when modified.
- **Writing field `d` leaves the TTL of field `a` alone.** This is the guarantee that
  keeps the absolute deadline absolute, so assert it directly rather than inferring it.
- Both clocks, independently: idle expiry while the absolute clock still has time, and
  absolute expiry despite continuous activity.
- Cookie `max-age` equal to `min(idle, HTTL(a))`, in every branch.
- Session-only mode: no `max-age`, and `gc_ttl` on **both** fields.
- `refresh_on_load=False` restoring the second round trip and refreshing only on access.
  **Assert that the idle clock actually advances under this setting** — an earlier draft
  of Section 4.2 omitted the branch, which would have left the clock frozen and every
  session immortal until its absolute deadline.

Four tests exist because adversarial review found the corresponding claims false. Each one
fails against the earlier design and passes against this one, so none may be dropped as
redundant:

- **`absolute_ttl = 0` produces a usable session.** Create one, load it, and assert the
  application receives the data. Under the earlier design `HTTL a` answered `-2`, which was
  read as "absolute deadline passed", so every such session was dead on arrival.
- **All five rows of the Section 4.1 state table**, including the `-1` row, which must be
  unreachable — assert that a session is never written with an unexpiring `a`.
- **The index does not report an idle-dead session.** Set idle far below absolute, let the
  idle clock lapse, then assert `list_for_subject` omits the session and that the entry has
  been removed from the index by the read.
- **Re-assertion never extends the index entry.** Record the entry's TTL, wait, modify the
  session so the response writes, and assert the TTL has **decreased**. The buggy version
  restores it to the full lifetime, which no end-state assertion would catch.
- **A custom `id_factory` returning an ID with a separator is rejected** with
  `SessionConfigurationError`, and no key is written.
- A session ID with an unsafe character yields a new session, and no part of that value
  reaches a response header.
- Read fail-open gives an empty session; write fail-closed raises `SessionStoreError`.
- No session ID and no subject in any telemetry attribute.
- **The tier probe answers `none` when it cannot ask.** Make `CONFIG GET` raise, and assert
  the store starts, logs exactly one warning, and reports `events.tier == "none"`. A probe
  that raises instead of degrading would take down every deployment on managed Redis.
- **Registering a handler at tier `none` never calls it**, and never raises. This is the
  documented silent fallback, so assert it rather than leave it to chance.

**Integration**

- **Rotation deletes the old key before writing the new one.** Assert the order, not only
  the end state — the order is the security control in Section 5.2.
- **Rotation fires with no application call.** Write the identity in a handler and assert
  the cookie value changed. This is F-2 and the reason the design detects rather than
  waits to be told.
- **A declared privilege key rotates too.** With `principal_keys=["user_id", "role"]`,
  changing the role rotates — and so does changing it back, which OWASP requires and an
  explicit call is easiest to forget.
- **A changed principal on a 4xx persists nothing**, while an unchanged principal on a 4xx
  persists normally. Both halves of Section 4.3, because dropping either one breaks a real
  pattern: the second is how a failed-login counter works.
- **`principal_of` raising at the response snapshot fails the response**, and writes
  nothing.
- **Ship the wiring assertion as a test helper**, not only as a test. Section 5.1 makes it
  the answer to a misconfigured resolver being silent; it belongs in the public surface so
  applications can prove their own configuration.
- **The session key is written before its index entry**, for the reason in Section 5.3.
  Assert the order here too: reversing it turns a harmless interleaving into an
  un-revocable session.
- **An orphaned session repairs itself.** Delete the index entry behind the store's back,
  then make one request that modifies the session, and confirm the entry is back and
  `revoke_all` finds it.
- **`revoke_all` misses a session created after it read the index**, and the session is
  still revocable by a second call. Pin this as the documented semantics rather than
  leaving it to be discovered.
- Two application instances share one session through one Redis.
- **The index prunes itself with no help from us**: add a session, let its absolute TTL
  pass, then confirm `HGETALL` omits it and `HLEN` has dropped — without any prune call.
  Then remove the last field and confirm Redis deleted the key.
- `list_for_subject` answers in one round trip and returns the descriptor for each
  session.
- `revoke_all` ends every session for one subject and leaves another subject untouched.
- A payload over 4KB, which a signed cookie cannot carry.
- An Authlib OAuth flow completing with no change to the application code.
- **The same scenarios again from `def` endpoints**, following
  `tests/integration/test_ratelimit_sync.py`. Cover both halves: a sync handler reading and
  writing `SessionDep` with no bridge, and a sync handler driving `SyncSessionStore`.
  Assert that a sync and an async handler leave Redis in the same state, since F-13
  promises one pattern rather than two.
- **Against a real 7.4 server**, the `HSET` + `HEXPIRE` fallback in Section 13.1 produces
  the same observable behaviour as the 8.0 path.

**Recipes are tests too.** Section 9.6 of `session-mgmt.md` puts ten recipes in version 1.
Each one lives in `examples/` or in a test that `nox` runs. A recipe nobody runs stops
working at the first rename, and a broken recipe costs the reader more than a missing
one.

Run `nox`: lint, mypy, bandit and coverage all gate.

---

## 12. Order of work

1. `Session`, and the tests for its four rows. It has no dependencies and it pins the
   contract everything else uses.
2. `SessionStore` and `RedisSessionStore`: keys, the two fields, the envelope,
   `load`/`save`/`touch`/`delete`.
3. `SessionMiddleware`: the eager load, the response rule, the cookie.
4. Settings, `deps.py`, `.sessions()` — the feature is usable at the end of this step.
   Add `SyncSessionStore` and `SyncSessionStoreDep` here too, alongside the async pair, so
   the two never drift apart. It is a mechanical delegation over an existing pattern; the
   cost of adding it later is that every method written in between needs a second author.
5. The principal snapshots and automatic rotation, with `rotate` and `revoke` beneath them,
   plus the ordering test. **The OWASP core is complete here.**
6. The subject index: `HSETEX`/`HDEL`, `list_for_subject`, `revoke_all`.
7. Telemetry, the exceptions, the extension points.
8. `SessionEvents`: the tier probe, the subscriber, the silent fallback. **Last, and
   separable.** It is the only step whose omission changes nothing else — N-17 says every
   guarantee holds without it — so it is the first thing to cut if the release is tight.
9. Documentation and the recipes.

Steps 1 to 5 are P0 in Section 8.5 of `session-mgmt.md`. Step 6 is P1, but its **key
schema must be settled in step 2**: Section 8.4 of `session-mgmt.md` explains that an
index added after sessions exist reports a wrong answer for every session that predates
it.

---

## 13. What Redis gives us that no other backend can

This package is the Redis integration, so the design should be one that a Postgres or a
Memcached store could not copy.

Three features carry that weight and all three sit inside the 7.4 floor this package
already declares, so version 1 depends on nothing newer. Section 13.2 then lists what a
later server adds for free, Section 13.3 gives the operational advice that comes with
running the server rather than only calling it, Section 13.4 designs the one opt-in feature
that a newer server unlocks, and Section 13.5 records what we looked at and left out.

### 13.1 What we use, at the 7.4 floor

| Feature                                                           | Since     | Where                         | What it replaces                                             |
|-------------------------------------------------------------------|-----------|-------------------------------|--------------------------------------------------------------|
| **TTL on a hash field** (`HEXPIRE`, `HSETEX`)                     | 7.4 / 8.0 | the index, and the two clocks | a prune job, or a sorted set with score filtering, or a scan |
| **`HGETEX`** — read a field and set its expiration in one command | 8.0       | the load path                 | `GET` then `EXPIRE`, and the whole `refresh_threshold` idea  |
| **A key that deletes itself when its last field expires**         | 7.4       | the index                     | a TTL on the index key, refreshed on every write             |

The index is the clearest case. **Every other store must answer the question "which
sessions has this user got?" by keeping a collection and then reconciling it**, because
no store tells you when a row quietly expired. Postgres needs a `DELETE ... WHERE
expires_at < now()` on a timer. DynamoDB's TTL sweeper runs on its own schedule, up to
48 hours late, so a read must filter anyway. Memcached cannot express the question at
all. Redis expires the fields itself, `HGETALL` returns exactly the live sessions, and
`HLEN` counts them in `O(1)`.

The two clocks are the second case. An idle timeout and an absolute timeout are two
independent deadlines on one piece of state, and Section 3.2 explains why holding them as
two field TTLs makes "a session outlives its absolute deadline" unreachable rather than
merely unlikely.

A note on version range. `HEXPIRE` is 7.4, which is our floor, but `HSETEX` and `HGETEX`
are 8.0. Against a 7.4 to 7.x server, fall back to `HSET` + `HEXPIRE` and to `HGET` +
`HEXPIRE`, pipelined. Follow the probe-once-per-process shape that
`probe_increx_support` already uses in `ratelimit_backend.py:67`. This fallback is
**only** an extra round trip, never a change in behaviour, which makes it far simpler
than the INCREX case: there is no correctness cliff to guard.

### 13.2 What a newer server adds, with no code from us

The design targets 7.4, and everything above works there. But later releases improve it
without a line of our code. One of them may also reward the two-field shape we chose for an
unrelated reason, and the paragraph below says plainly why we do not yet know. Say this in
the guide: **the same application gets cheaper and faster by upgrading the server.**

| Release | Feature                                                            | What it gives a session store                                      |
|---------|--------------------------------------------------------------------|--------------------------------------------------------------------|
| 8.6     | hash memory footprint down up to 16.7%, hash latency down up to 7% | every session key and every index key, for free                    |
| 8.8     | `HGETALL` up to 25% faster on hashes with 1K+ fields               | `list_for_subject` for a tenant with many live sessions            |
| 8.8     | **hash subkey notifications**                                      | a new capability, not only a speed-up. See below.                  |
| 8.10    | **compact hashes**                                                 | a large memory win **if** field expiry does not disqualify us. Open. See below. |
| 8.10    | wide `HSET` on a fresh hash batched into one listpack append       | session creation                                                   |

**Compact hashes (8.10) suit the shape of our record, and may still exclude it.** The
encoding stores field names **once** across every key that shares a schema. A session store
looks like the ideal case: a million session keys, each a hash with exactly the fields `a`
and `d`, identical in every one, so the names are held once for the deployment instead of
once per session. Section 3.2 chose two fields to make the absolute deadline structurally
unbreakable, which is a security argument; the uniform schema is a by-product. If the
encoding does reward it, say plainly in the guide that this was luck and not foresight.

**An earlier draft stopped there and called it "the largest memory win, aimed squarely at
us". That was asserted, not checked.** Redis offers two ways into the encoding, and each
has a problem for us.

The first is automatic conversion, driven by `hash-min-template-entries`, and the
documentation excludes us by name: *"A hash is not converted if it uses field expiration,
even when its field count meets the minimum."* Every session key here uses field expiration
on both fields. That is Section 3.2 and it is not negotiable, so on this path our keys are
ineligible whatever their schema.

The second is `HIMPORT`, which hints Redis to store the new key as a compact hash at
creation, before any `HEXPIRE` runs. Section 13.5 explains why we will not write session
keys with it. And whether a key created that way survives a later `HEXPIRE` on its fields is
undocumented in both directions: the hashes page says a converted key "never reverts to a
plain hash", which suggests it would, while the exclusion above suggests it would not.
Suggests is not knows.

**So this row is a question, not a benefit, until someone measures it.** The measurement is
small. Against an 8.10 server, write ten thousand session keys the way Section 4.2 writes
them, then read `hash_templates` and `hash_template_keys` from `INFO STATS` and
`used_memory_hash_templates` from `INFO MEMORY`. A zero settles it. Section 12 should carry
that as an integration check, and the guide should claim nothing until it passes.

Two consequences for the implementation hold either way, because both are free: keep the
field names short, and **keep them identical in every session**. Never write an optional
field into some sessions and not others. A divergent schema forfeits the template if we
ever qualify for one, and a short name is fewer bytes on the wire meanwhile.

**Hash subkey notifications (8.8) are a new capability, and the one row here that does
need code from us.** Redis 7.4 gave fields a TTL, but key-level notifications carry no
field name, so nothing could say *which* field expired. Redis 8.8 adds field-level events
across four channel types. Section 13.4 designs the feature that consumes them.

### 13.3 Operational guidance that only a Redis vendor will write

**A session store is not a cache, and `maxmemory-policy` must say so.** Under
`allkeys-lru`, `allkeys-lfu` or `allkeys-random`, Redis will evict live sessions to make
room, and every evicted session is a user signed out mid-task with nothing in any log to
explain it. Use `volatile-ttl` or `noeviction`, or give sessions their own instance or
logical database. This is the single most likely production incident with this feature,
and no competing package documents it, because none of them is written by people who
support the server.

Redis 8.6 adds `volatile-lrm` and `allkeys-lrm`, which track the least recently
**modified** key rather than the least recently used one. Worth one line in the guide, and
worth a caution with it: `HGETEX` is a write command, so our idle refresh counts as a
modification and LRM behaves much like LRU here. Neither is the answer. **Do not evict
sessions at all.**

**Sessions are small, and Redis stores small hashes as a listpack.** Below
`hash-max-listpack-entries` and `hash-max-listpack-value` a hash is a flat array, not a
hash table, so a two-field session and a short index cost far less than the per-key
overhead suggests. Note the thresholds so an operator sizing a deployment finds which side
of them a typical session falls.

**One index key can go hot, and 8.6 can find it.** Session keys spread across the keyspace
by session ID, but a tenant's index key is a single key that every login and every logout
writes. For a large tenant that key is a candidate hot spot, and slot migration will not
help, because moving one key only moves the problem. `HOTKEYS START METRICS 2 CPU NET
SAMPLE 100` identifies it by CPU and by network cost. Put this in the guide beside the
`subject_of` seam, because sharding a large tenant's index across several keys is exactly
what that seam is for.

This is the one place where the query engine would be strictly better, since it has no
per-subject key to contend on. Section 3.3 records why version 1 does not use it, and what
would change that.

### 13.4 Real-time session events

A session store knows when a session dies. Every competing store discovers it on the next
request, because a row that expired quietly tells nobody. Redis can tell us, and F-21 turns
that into an opt-in feature: **the store subscribes to Redis notifications and calls the
application back when a session ends.** Closing a WebSocket the moment a user is signed out
is the case that pays for it.

The feature is off by default, degrades to nothing on a server that cannot supply it, and
carries no guarantee. The rest of this section says exactly what that means.

#### The two-field design pushes this to 8.8

Our session key has **no key-level TTL**. It dies as a side effect of its last field
expiring, which is the whole of Section 3.2. That has a consequence for notifications that
is easy to miss:

| Tier    | Needs                                    | Channel                                  | Names the clock that fired? |
|---------|------------------------------------------|------------------------------------------|-----------------------------|
| `none`  | —                                        | —                                        | —                           |
| `key`   | 7.4, plus `Eghx` in `notify-keyspace-events` | `__keyevent@<db>__:del`               | **No**                      |
| `field` | **8.8**, plus `h` and one of `S`/`T`/`I`/`V` | `__subkeyevent@<db>__:hexpired`, whose payload names the field | **Yes** — `d` is idle, `a` is absolute |

At the `key` tier a subscriber learns that a session key went away and nothing else. It
cannot separate an idle death from an absolute one, and it cannot separate either from a
revocation. That is most of what a caller wants to know, so **the useful ladder is two
rungs, not three: `field` or `none`.** Implement the `key` tier only if a concrete recipe
needs it; do not add it speculatively.

One detail to confirm against a real 8.8 server before the guide claims it: whether field
expiry that empties a hash also emits a key-level `del`. The `field` tier does not depend
on the answer, which is another reason to build that tier and not the other.

#### Probe, and never configure

Two different questions, and the code must ask both:

1. **Can the server do it?** Read `redis_version` from `INFO server`.
2. **Is it switched on?** Read `notify-keyspace-events` with `CONFIG GET` and look for `h`
   together with one of `S`, `T`, `I`, `V`.

**The four subkey flags are independent of `K` and `E`.** Enabling standard keyspace
notifications does not enable subkey notifications, and the reverse holds too. This will
be the commonest support question; say it in the guide in those words.

**Both probes can fail, and failure is an answer.** Managed Redis often restricts, renames
or forbids `CONFIG`, and an ACL that omits `@admin` does the same. Treat any failure as
tier `none`. Follow the shape `probe_increx_support` already uses at
`ratelimit_backend.py:67`: probe once per process from the lifespan, return `None` when the
question could not be answered, and never cache a guess.

**Never call `CONFIG SET`.** `notify-keyspace-events` is server-wide. Setting it changes
behaviour for every other application on that instance and costs CPU on every write. A
library must not make that decision for an operator. Document the flag string, and put it
beside the `maxmemory-policy` advice in Section 13.3, which has the same shape.

#### The fallback is silence, and the guide must say so plainly

When the tier is `none`, the store registers the handlers, logs **one** warning at startup,
and the handlers never run. Startup still succeeds and every request still works.

This is the deliberate choice, and it has a sharp edge worth naming rather than hiding: a
revocation handler that never fires looks exactly like one that works. An application that
closes WebSockets on this signal and nothing else will hold them open after a sign-out, on
a server where the feature is unavailable. **So the guide must state that the callback is
best-effort, and that any application relying on prompt closure needs its own periodic
check as well.** One warning line at startup is the only thing the library will do about
it.

#### What a subscriber costs

- **On Cluster, keyspace events are node-local and are not broadcast.** A subscriber must
  connect to every node to see every event. This is a per-node fan-out, not one connection,
  and it is the largest implementation cost in the feature.
- **Every worker receives every event.** A deployment with eight uvicorn workers gets eight
  deliveries of each session death. That is correct for closing a WebSocket, because only
  the worker holding the socket acts. It is wrong for anything that writes, so an audit log
  driven this way needs deduplication or a single designated subscriber.
- **Pub/Sub is fire-and-forget.** Events sent while no subscriber is connected are lost, and
  the connection has to be re-established after a disconnect with no replay.
- **`hexpired` fires when Redis removes the field, not when the TTL reaches zero.** With
  many keys carrying a TTL, the lag can be significant.

#### The rule that does not move

**N-17: no correctness claim rests on a notification.** The index keeps pruning itself
through field expiry (Section 3.3), and the load-time state table (Section 4.1) stays the
authority on whether a session is alive. Notifications are a reaction channel and strictly
additive. Nothing in Sections 3 to 8 changes because this feature exists, and every test in
Section 11 must pass with it switched off.

#### Shape

A `SessionEvents` object built in the lifespan, holding one subscriber task per node:

```python
events = store.events()                       # tier probed once, at startup

@events.on_session_end
async def _(sid: str, cause: Literal["idle", "absolute", "revoked"]) -> None:
    await close_sockets_for(sid)

print(events.tier)        # "field" or "none"
```

`cause` is what the `field` tier buys and the `key` tier cannot give. At tier `none` the
handler is held and never called.

### 13.5 Considered, and not in version 1

**Compare-and-set on the session payload.** There is a real gap here and it should be
named: two requests from the same browser can load a session, both modify it, and the
second write silently discards the first. This is the server-side twin of the cookie race
in [starlette#2019](https://github.com/Kludex/starlette/issues/2019).

**An earlier draft answered it with `SET ... IFEQ` / `IFDEQ` and `DELEX ... IFEQ` (Redis
8.4). That was wrong, and wrong in a way worth recording, because the mistake is easy to
repeat.** Those are **string** commands. Our session record is a hash, so they cannot
address field `d` at all. `HSETEX` offers only `FNX` and `FXX` — field existence, not value
comparison — and through Redis 8.10 there is no `HDIGEST`, no `HDELEX`, and no hash-field
compare-and-swap of any kind. The draft also called `IFDEQ` an `O(1)` digest comparison; the
`DELEX` documentation gives `O(1)` for `IFEQ`/`IFNE` and **`O(N)` for `IFDEQ`/`IFDNE`**.
`IFDEQ` saves bytes on the wire against `IFEQ`. It does not save server time.

Reaching those commands would mean splitting the record: the payload into a string key, the
two clocks into a hash. That is a second key, a `{sid}` hash tag to keep Cluster in one
slot, and a schema change — to buy a command that is no cheaper than the alternative below.

**The mechanism that would work is Lua, and it works at the 7.4 floor.** A script reads
field `d`, compares `redis.sha1hex` of the stored bytes against a digest the client computed
from what it loaded, and writes only on a match. It touches field `d` and never field `a`,
so N-6 survives untouched.

The cost is the part worth recording, because it is the question that gets asked:

| Path | Today                                        | With a Lua compare-and-set |
|------|----------------------------------------------|----------------------------|
| Load | 1 round trip, 2 commands (`HGETEX d`, `HTTL a`) | **unchanged** — the client hashes bytes it already received |
| Save | 1 round trip, 2 commands (`HSETEX d`, index `HSETEX`) | 1 round trip, 2 commands — `EVALSHA` replaces the first |

**Identical round trips and identical command counts.** The new cost is CPU: one SHA-1 over
the payload in Python on load, one in Lua on save. Nor is the tooling an obstacle — the repo
already registers a script at `ratelimit_backend.py:348`, already falls back when `EVAL` is
unavailable at `cache_backend.py:332`, and already depends on `fakeredis[lua]`
(`pyproject.toml:83`), so the unit suite could cover it.

**It still waits, for two reasons that survive all of the above.** First, compare-and-set
*detects* a conflict; it cannot resolve one. A session dict has no merge function, so the
store's only honest choices are to raise or to count the conflict and overwrite anyway —
and neither is obviously right for every application. Second, the digest must be taken over
the bytes as stored, not over a re-serialized value, so a coder that is not byte-stable
would fail every write; that constraint belongs in the `Coder` contract before it belongs in
a security control.

Document the last-write-wins behaviour in the guide. When this returns, it returns as a Lua
script behind a setting, not as `IFDEQ`.

**The `HIMPORT` family for session writes** (Redis 8.10,
[redis-py #4205](https://github.com/redis/redis-py/pull/4205)). Declare an ordered list of
field names once per connection, then create hashes by sending only their values. It exists
to cut the field names off the wire during a bulk import, and it hints Redis to store the
result as a compact hash — which is the only reason we looked at it. Section 13.2 gives
that reason.

**One objection ends it.** `HIMPORT SET key fieldset-name value [value ...]` takes no
expiration option of any kind, and its documentation states that an existing key is
overwritten. Field `a` and its absolute deadline would be destroyed on every save.
Rebuilding them costs `HIMPORT SET`, then `HEXPIRE a`, then `HEXPIRE d` — three commands
where Section 4.2 issues one `HSETEX`, and between them a window where the session carries
no expiry at all. N-6 asks that outliving the absolute deadline be unreachable by
construction. This design makes it reachable by a crash.

Four more, each sufficient on its own:

- **There is nothing to save.** What HIMPORT saves is the field names, and ours are `a` and
  `d`. Redis measured 11% on a pipelined import of a million three-field records named
  `_uid`, `score` and `tag`. Our write path is one two-field write per HTTP request.
- **The index cannot use it at all.** `sessions-of:<subject>` carries session IDs as field
  names — unique per key, unknown until write time. A fieldset is fixed and shared by
  definition.
- **The three objections that X-7 already makes.** 8.10 against a 7.4 floor, `experimental`
  in redis-py, and absent from `fakeredis 2.36.2`, so the suite in Section 12 could not
  cover it.
- **It is unavailable where our users run.** Both command pages mark Redis Software and
  Redis Cloud unsupported, Standard and Active-Active alike, and redis-py raises
  `DataError` for every HIMPORT method on a multi-database client. On Cluster, redis-py
  re-prepares lazily per connection and a discard does not reach every server session at
  once — a standalone-versus-Cluster difference of exactly the kind N-4 forbids.

Revisit if a later server gives `HIMPORT SET` a `KEEPTTL`, and then only to ask again
whether two field names are worth sending.

**Client-side caching with RESP3 invalidation** (`CLIENT TRACKING`). Redis pushes an
invalidation when a key changes, so a worker could serve a repeated read from local memory
and still observe a revocation. Something Postgres and Memcached cannot offer.

**It does not apply to the session read at all, and the reason is our own design.** The
load path in Section 4.1 is `HGETEX`, which is a *write* command — it changes a TTL — and
redis-py refuses to cache writes. Verified against `redis.cache.DefaultCache.is_cachable`
in redis-py 8.0.1:

| Command                                 | Cacheable |
|-----------------------------------------|-----------|
| `GET`, `HGET`, `HGETALL`, `HLEN`        | yes       |
| **`HGETEX`, `HSETEX`, `GETEX`, `HTTL`** | **no**    |

So the authentication path is never served from a client cache, whatever the
configuration. The refresh-on-read that Section 4.2 buys with `HGETEX` costs us the
ability to cache the read — and for a session lookup that is the right trade.

**Where it could apply is the index**, because `HGETALL` and `HLEN` are cacheable. That
is worth a future look for `list_for_subject`, which a "your active sessions" screen may
call repeatedly.

**On the objection itself, an earlier draft of this section was wrong.** It implied that
invalidations might not arrive. They do, and redis-py handles them carefully: before
serving a hit it drains pending pushes on the connection that cached the entry and
re-checks whether the entry survived (`redis/connection.py:1733-1754`), and it flushes the
whole cache on disconnect (`redis/connection.py:1700-1702`). Lost delivery is not the
problem.

The real residual is **propagation delay, not reliability**. Redis sends the invalidation
after the write commits, and the drain above is non-blocking, so an invalidation still on
the wire is not seen and a hit is served from stale data. The window is a network
round trip, and no client can close it, because a client cannot know an invalidation is
coming until it arrives. Caching therefore makes a read *eventually* consistent with a
revocation rather than immediately consistent with it.

That is harmless for a UI listing and unacceptable for an authorization decision, which
gives the rule to record now, before anyone enables caching later:

> **`revoke_all()` must read the index uncached.** Acting on a stale member list means
> failing to kill a session that the caller was told had been killed.

Redis 8.10 also fixed an ACL key-name leak in `BCAST` invalidations, a further reason to
let the feature settle before this package leans on it.

**A Stream for the session audit log.** Section 3 of `session-mgmt.md` carries OWASP's
requirement to log the session lifecycle, using a salted hash of the session ID. `XADD`
with `MAXLEN` gives a bounded, ordered, replica-safe log that any instance can read, and
8.6's idempotent production (`XADD ... IDMP`) means a producer that retries after a crash
cannot double-write an audit entry. Pair it with the subkey notifications in Section 13.2:
the notification is the trigger, the stream is the record. This belongs in the recipe list
in Section 9.6 of `session-mgmt.md` rather than in the store.

---

## 14. Migration

One table per use case. Each names the three packages people arrive from and then ours.
Section 10 of `session-mgmt.md` gives the reasoning; this is the code.
**Existing sessions do not survive the change** — every user signs in again.

### 14.1 Turning sessions on

| From | Code |
|---|---|
| Starlette | <pre>app.add_middleware(SessionMiddleware, secret_key=SECRET, max_age=1209600)</pre> |
| `starsessions` | <pre>app.add_middleware(SessionAutoloadMiddleware)<br>app.add_middleware(SessionMiddleware, store=RedisStore(connection=redis),<br>                   lifetime=3600, rolling=True)</pre> |
| `fastapi-users` | <pre>auth_backend = AuthenticationBackend(<br>    name="redis", transport=CookieTransport(cookie_max_age=3600),<br>    get_strategy=lambda: RedisStrategy(redis, lifetime_seconds=3600),<br>)<br>app.include_router(fastapi_users.get_auth_router(auth_backend), prefix="/auth/cookie")</pre> |
| **This SDK** | <pre>FastAPIRedis(app).lifespan().sessions()</pre> |

No store to construct: we use the SDK's pool. No autoload middleware: the load is eager
(Section 1.1).

### 14.2 Sign in, sign out

| From | Code |
|---|---|
| Starlette | <pre>request.session["user_id"] = user.id<br>request.session.clear()</pre> |
| `starsessions` | <pre>await load_session(request)<br>request.session["user_id"] = user.id<br>regenerate_session_id(request)</pre> |
| `fastapi-users` | <pre># the generated /auth/cookie/login route; the record holds the user ID alone</pre> |
| **This SDK** | <pre>async def login(session: SessionDep):<br>    session["user_id"] = user.id   # signing in is writing the identity<br><br>async def logout(session: SessionDep):<br>    session.clear()                # emptied, so the key and the cookie go</pre> |

Nothing to load and nothing to save. Writing the identity is what the middleware watches
(Section 5.1); `def` handlers work unchanged (Section 9).

### 14.3 Rotation

| From | Code |
|---|---|
| Starlette | <pre># not possible: the cookie is the store, so there is no ID to rotate</pre> |
| `starsessions` | <pre>regenerate_session_id(request)   # at every sign-in and privilege change</pre> |
| `fastapi-users` | <pre># none</pre> |
| **This SDK** | <pre>FastAPIRedis(app).lifespan().sessions(principal_keys=["user_id", "role"])<br># then nothing in the handler: session["role"] = "admin" rotates, and so does<br># the way back down. store.reauthenticate(session) for a change state cannot see.</pre> |

Rotation is detected, not called, so it cannot be forgotten — the one mistake here that is
a vulnerability (Section 5.1). A change on a 4xx persists nothing (Section 4.3).

### 14.4 Expiry

| From | Code |
|---|---|
| Starlette | <pre>max_age=1209600        # cookie only; the server enforces nothing</pre> |
| `starsessions` | <pre>lifetime=3600, rolling=True    # one clock, refreshed or not</pre> |
| `fastapi-users` | <pre>lifetime_seconds=3600  # absolute only; None means it never expires</pre> |
| **This SDK** | <pre>REDIS_SESSION_IDLE_TTL=1800        # field d, refreshed on access<br>REDIS_SESSION_ABSOLUTE_TTL=28800   # field a, never refreshed</pre> |

Two clocks, both enforced by Redis, neither computed by us (Section 3.2). Set them equal to
keep the single-clock behaviour of the row above. Section 9.4 of `session-mgmt.md` maps
every `starsessions` setting.

### 14.5 Reverse lookup

| From | Code |
|---|---|
| Starlette | <pre># impossible: nothing on the server knows the session exists</pre> |
| `starsessions` | <pre># none; hand-rolled, and its members outlive the sessions they name<br>await redis.sadd(f"sessions-of:{user_id}", sid)</pre> |
| `fastapi-users` | <pre># none</pre> |
| **This SDK** | <pre>await store.revoke_all(str(user_id))        # password change, forced sign-out<br>await store.revoke_id(sid, subject=str(user_id))   # one device<br>await store.list_for_subject(str(user_id))  # the "your devices" screen<br>await store.count_for_subject(str(user_id)) # a cap on concurrent sessions</pre> |

The index prunes itself and reads verify liveness, so a dead session is never listed
(Section 3.3). Change the credential before revoking (Section 5.3).

### 14.6 Error handling

| From | Code |
|---|---|
| Starlette | <pre># no store to fail; a bad signature silently yields an empty session</pre> |
| `starsessions` | <pre>except RedisError:   # the driver error reaches your handler</pre> |
| `fastapi-users` | <pre># the driver error reaches the route</pre> |
| **This SDK** | <pre># read fails  -> empty session, so the auth dependency returns 401<br># write fails -> SessionStoreError, never silent<br>@app.exception_handler(SessionStoreError)<br>async def store_down(request: Request, exc: SessionStoreError):<br>    return JSONResponse({"detail": "Session unavailable"}, status_code=503)<br># REDIS_SESSION_FAIL_CLOSED=true  ->  a failed read raises too</pre> |

The asymmetry is deliberate (Section 7). No `redis.RedisError` reaches application code:
catch `SessionError`, or `SessionConfigurationError` and `SessionStoreError` beneath it.
