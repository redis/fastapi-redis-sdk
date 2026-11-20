# Session attribute search: research

Research for [issue #108](https://github.com/redis/fastapi-redis-sdk/issues/108),
"Do not serialize session attributes as JSON". The issue asks for two things:
keep each session attribute in its own hash field, and "provide primitives for
searching and expiring individual attributes". It does not say what search
means. This document records how other ecosystems search inside session
attributes, so that the design can rest on evidence.

Sections 1 to 7 are research. Section 8 lists what the findings imply and
the questions a design must answer. Sections 6.5 and 8 also record the
decisions made on 2026-11-18 and 2026-11-19.

## How this was produced

Seven research agents ran in parallel on 2026-11-17: Java, .NET and C/C++,
Python, JavaScript and TypeScript, Go (with Rust), PHP (with Ruby and Elixir),
and the Redis Query Engine. Each read source code, official documentation, and
issue trackers, and cited a URL for each claim. Vote, reaction, and view counts
are as of that date.

The claims here come from those reports. They were cross-checked against each
other but not re-verified one by one. Section 9 lists what no agent could
confirm.

Tests on 2026-11-19 against Redis 7.4, 8.8, and 8.10 in local containers
checked some of the Redis behavior. The text marks each result as tested.

## 1. Where we are today

```
rf:s:<sid>         hash, key TTL = absolute timeout
                                     field "d" = <envelope>, field TTL = idle timeout
rf:si:<subject>    hash, field <sid> = <descriptor>
```

This document calls the second hash `sessions-of`.

- All attributes sit in one JSON string in field `d`. The string is encrypted
  when an `Encryptor` is set.
- The only lookup other than by session ID is by subject, through the
  hand-kept `sessions-of` hash: `list_for_subject`, `count_for_subject`,
  `revoke_id`, `revoke_all`.
- That hash is an upper bound. Most sessions end by idle timeout, so the store
  verifies each entry against the live keys and prunes dead ones on read.

So the store already answers the most common question, "which sessions does
this user have". It cannot answer any question about another attribute.

## 2. Short answers

### Do they support simple lexicographic search, or also numeric, date, geo, and so on?

Almost all session stores support no search at all. The session is one opaque
value, and the store contract is get, set, and delete by session ID.

Where search exists, it is exact match on a string, and the string is nearly
always the user ID. A second kind appears in stores that keep typed metadata:
a range on a timestamp (created, last activity, expiry). Dates are always an
integer or timestamp column; no store has a separate date search.

Three kinds of system go further:

| System | Search types | Notes |
|---|---|---|
| `redis-sessions-java` (Redis field engineering) | tag exact match, full text, numeric equality and range, geo radius, sort | The only session store found with all of these. Not a supported product. |
| Stores where the user owns the schema (Yii2 `DbSession`, Lucia, better-auth, Rails 8, Phoenix, Django with an `AbstractBaseSession` subclass) | whatever the database allows on the columns the user added | The library gives no query API; the user writes SQL or ORM queries. |
| Identity servers (Zitadel, Duende, Casdoor, Keycloak, Ory Kratos) | exact match on fixed fields; date ranges (Zitadel); substring (Duende, Casdoor) | Fixed schema. None searches custom attributes. |

No session store found offers geo or full-text search except
`redis-sessions-java`. No tracker in any ecosystem holds a request with real
support for geo, full-text, or numeric-range search over session attributes.

### Are indexes predefined and created at startup, or can users define their own?

Four models exist.

| Model | Examples | What happens on change |
|---|---|---|
| No index | Most stores | Nothing to change. |
| One predefined index, hand-kept on write | Spring Session Redis (a Set per principal), AdonisJS (`session_tag:<userId>` Set), better-auth (`active-sessions-<userId>` JSON list), lua-resty-session (`sessions_meta`), this SDK (`sessions-of`) | No definition exists. Entries go stale when sessions expire. |
| Predefined columns, created by migration or at startup | Laravel `sessions` table, Drupal, Duende, Kratos, Flask-Session (table and TTL index at startup), Spring Session Hazelcast (HASH index at startup) | Normal schema migration. No store backfills old rows. |
| User-declared fields | `redis-sessions-java` (builder in code), Spring Session Geode (`indexableSessionAttributes`), reactive Spring Session Redis (`IndexResolver`), Yii2 (`writeCallback`), connect-dynamodb (`specialKeys`), better-auth (`additionalFields`), Redis OM | See below. |

For user-declared fields, the handling of a changed definition differs:

- `redis-sessions-java` drops and recreates the index at every start. Queries
  during the rebuild see partial results, and instances that start together
  race. It also runs `FUNCTION FLUSH`, which removes every function library on
  the server.
- Redis OM Python and Node store a hash of the schema in a side key and drop
  and recreate the index when the hash differs. Redis OM .NET compares
  `FT.INFO` with the wanted schema and leaves the decision to the caller.
- RedisVL keeps the old index when one exists and logs a message. A changed
  schema has no effect unless the caller passes `overwrite=True`.
- better-auth checks the schema on first use and fails requests on a mismatch.
- SQL stores write the new column from the next session write. Old rows keep
  `NULL`.
- Spring Session's reactive Redis indexer does not re-index. A session gets a
  new index entry only when it is next saved.

No library found does a zero-downtime index swap by default.

### What control does the user have over the type of data and the type of search?

In most stores, none. The user can choose the serializer for the whole
session (gob or JSON in Go, pickle or JSON or msgpack in Python, JDK
serialization or JSON in Java). The store sees bytes.

The stores that give control share one design: the user names an attribute
and declares its type, the store keeps that attribute as a plain typed value,
and everything else stays opaque.

| System | How the user declares | Types |
|---|---|---|
| `redis-sessions-java` | `IndexedField.tag("x")`, `.numeric("age").javaType(Long.class)`, `.geo("loc")`, `.text("d")`, with `converter(...)` and `sortable(...)` | tag, text, numeric, geo |
| Redis OM (all languages) | model field types or annotations | `str` → TAG, opt-in TEXT, number → NUMERIC, `datetime` → NUMERIC, `bool` → TAG, geo → GEO |
| better-auth | `session.additionalFields` | string, number, boolean, enum |
| connect-dynamodb | `specialKeys: [{name, type}]` | DynamoDB scalar types |
| Yii2 | `writeCallback` returns extra columns | the SQL column type the user created |
| Lucia v3 | typed columns plus `getSessionAttributes()` | SQL column types |
| Django | subclass `AbstractBaseSession`, add a model field | any model field |

`redis-sessions-java` rejects a value of the wrong type at write time. This
matters on Redis: section 7 shows that a wrong-typed value removes the whole
session from the index without an error.

### What are the most requested features?

The same list appears in every ecosystem, in nearly the same order. Section 6
gives the evidence.

1. End all sessions of a user: "log out everywhere", force-logout by an admin,
   logout on password or role change.
2. List a user's sessions with device details (IP, user agent, last activity),
   and end one of them.
3. List or count all active sessions: "who is online".
4. Indexes that stay correct when sessions expire. This is a demand for
   correctness, and it is the loudest signal in the Spring Session tracker.
5. Limit the number of sessions per user.
6. Promote a chosen attribute to a typed, searchable field (tenant, device,
   role).
7. Change one attribute in all of a user's sessions (a role or plan change).
8. Filter by created or expiry time.

Search by an arbitrary attribute has little recorded demand. When users ask
for it, the attribute is almost always the user ID.

## 3. Findings by ecosystem

### Java and the JVM

**Spring Session.** The API is `FindByIndexNameSessionRepository`, with
`findByIndexNameAndIndexValue(String, String)` and `findByPrincipalName`. Both
arguments are strings, so the contract is exact match
([source](https://github.com/spring-projects/spring-session/blob/main/spring-session-core/src/main/java/org/springframework/session/FindByIndexNameSessionRepository.java)).
`IndexResolver` looks like a hook for custom indexes, but most back ends
ignore every index except the principal name:

| Repository | Custom indexes | Index storage |
|---|---|---|
| `RedisSessionRepository` (Spring Boot default) | no indexes at all | none |
| `RedisIndexedSessionRepository` (servlet) | ignored; returns an empty map for any other name | a Redis Set per principal value |
| `ReactiveRedisIndexedSessionRepository` | honored, string equality | a Set per `name:value`, plus a reverse Set per session |
| `JdbcIndexedSessionRepository` | ignored | `PRINCIPAL_NAME` column with a database index |
| Hazelcast | ignored | a HASH index on `principalName`, created at startup |
| MongoDB with the Jackson converter | any top-level attribute, unindexed | TTL index only |
| Geode | declared list (`indexableSessionAttributes`) | OQL, string equality |

Spring Session Redis stores one hash field per attribute (`sessionAttr:<name>`),
which is the layout issue #108 proposes. The values are JDK-serialized by
default, so they cannot be searched. Spring Session JDBC lets the user replace
each SQL statement and store attributes as PostgreSQL `JSONB`; search is then
possible, but only through the user's own SQL
([docs](https://docs.spring.io/spring-session/reference/configuration/jdbc.html)).

The hand-kept Sets leak. Cleanup depends on keyspace notifications, and on
Redis Cluster the repository subscribes to one random node, so some entries
are never removed
([docs](https://docs.spring.io/spring-session/reference/configuration/redis.html)).
Issues: [#3183](https://github.com/spring-projects/spring-session/issues/3183)
(+7, open),
[#1715](https://github.com/spring-projects/spring-session/issues/1715),
[#3453](https://github.com/spring-projects/spring-session/issues/3453), and
several more. A proposal to index with RediSearch has been open since 2022
with no design
([#2133](https://github.com/spring-projects/spring-session/issues/2133)).

**`redis-sessions-java`.** This is the closest prior art
([repository](https://github.com/redis-field-engineering/redis-sessions-java)).
It is a Redis field-engineering project at version 0.1.13, last changed in
March 2025. Its README lists "flexible indexing capabilities, allowing
searchable sessions" and "bulk session operations" among "the most common asks
we see from our users".

- Storage: one hash per session, one field per attribute, plus reserved fields
  `createdAt`, `sessionSize`, `maxInactiveInterval`, `lastAccessedTime`,
  `lastModifiedTime`.
- Declared attributes are written as plain text after a type check. Undeclared
  attributes go through the JDK or JSON serializer as opaque bytes.
- Index: `FT.CREATE ... PREFIX <prefix>:session:` built from the declared
  fields, dropped and recreated at every start.
- Query API: `QueryBuilder` with `equals`, `textMatch`, `between`,
  `greaterThan`, `lessThan`, `geoRadius`, joined by `and()` and `or()`, and a
  `RawFilter`.
- Operations on a filter: `findSessions(filter, limit)`, with a sorted
  variant; `deleteSessions(filter, limit)`;
  `updateSessions(filter, limit, field, value)`; and the statistics
  `largestSessions(topK)`, `uniqueSessions()`, `sessionSizeQuantiles(...)`.

Its weak points, read from source: it formats values into the query string
without escaping, so an email or a UUID breaks a tag query and allows query
injection; it has only `limit`, no paging; it has no date or vector type.

**Others.** Redisson's Tomcat manager, Jetty's session data stores, WildFly,
Hazelcast web sessions, Micronaut, Vert.x, and Ktor offer no attribute search.
Keycloak offers fixed queries (by user, by client, counts) with paging only.

### .NET and C/C++

No store searches inside session attributes.

- ASP.NET Core `ISession` writes one `byte[]` per session to
  `IDistributedCache`, which has no key enumeration
  ([source](https://github.com/dotnet/aspnetcore/blob/main/src/Middleware/Session/src/DistributedSession.cs)).
  `ITicketStore` has four methods, all by key.
- `Microsoft.Web.RedisSessionStateProvider` stored each session item as a hash
  field through version 4. It never offered search, because the field values
  were serializer output. Version 5 (June 2023) went back to one value per
  session "for necessary security improvements" and removed the pluggable
  serializer. That removal drew the largest reaction in the repository
  ([#185](https://github.com/Azure/aspnet-redis-providers/issues/185), 49
  comments).
- NCache and ScaleOut have query engines for cached objects, but not for
  sessions. A ScaleOut staff answer says its index attribute "is only honored
  by our NamedCache API".
- Duende IdentityServer server-side sessions are the richest model
  ([source](https://github.com/DuendeSoftware/products/tree/main/identity-server/src)).
  The record has fixed fields: `SubjectId`, `SessionId`, `DisplayName`,
  `Created`, `Renewed`, `Expires`, and an encrypted `Ticket`. Get and delete
  use exact match on subject or session ID. The admin query uses substring
  match on subject, session ID, and display name. Paging uses a cursor, and
  total counts are optional so that stores without a cheap count can omit
  them. Custom metadata goes inside the encrypted ticket and cannot be
  filtered. Display name is off by default "due to potential PII concerns".
- ABP offers fixed filters: user, device, client.
- lua-resty-session (OpenResty) has an opt-in side index of audience, subject,
  and session ID, with an option to hash the subject.
- Drogon, Crow, CppCMS, and Apache `mod_session_dbd` offer no search.

### Python

No session library searches inside attributes. Each stores one value per
session.

- Django: `session_data` is a signed, encoded text column. The documented way
  to find a user's sessions is to subclass `AbstractBaseSession` and add an
  indexed `account_id` column
  ([docs](https://docs.djangoproject.com/en/5.2/topics/http/sessions/)).
  Django refused built-in support four times, most recently in 2024
  ([#35806](https://code.djangoproject.com/ticket/35806)), because other back
  ends could not support it and because IP and device data are personal data.
  `django-user-sessions` (720 stars) and `django-qsessions` add `user`,
  `user_agent`, `ip`, and `last_activity` columns.
- Flask-Session: one Redis string per session; no list or query. Issue
  [#267](https://github.com/pallets-eco/flask-session/issues/267) (open, no
  reactions) proposes exactly what issue #108 proposes: hashes, hash-field
  expiry, and Redis search. A contributor's feedback was to keep it optional:
  "we don't reinvent redis-py within flask-session".
- Starlette's `SessionMiddleware` is a signed cookie. FastAPI has no session
  store ([#754](https://github.com/fastapi/fastapi/issues/754), 75 thumbs-up,
  closed in 2026). `starsessions`, Litestar, `aiohttp-session`, Beaker, and
  `pyramid_redis_sessions` have no list or search.
- Nearby Redis libraries, which are not session stores:
  - `redis-om-python` is the best model for typed, user-declared indexes. It
    stores a SHA-1 of the schema and drops and recreates the index on a
    mismatch. It had to add separate data migrations when the stored format of
    `datetime` changed.
  - RedisVL message history and `langgraph-redis` use fixed schemas, and users
    cannot add filter fields.

### JavaScript and TypeScript

No session library searches inside attributes.

- The express-session store contract has an optional `all()` and no query
  method. `connect-redis` implements `all()` with `SCAN` and `MGET`; its
  maintainer calls it a debugging aid. A pull request to store sessions with
  RedisJSON so that users could "create indexes and find sessions" was closed
  unmerged ([#421](https://github.com/tj/connect-redis/pull/421)).
- `connect-mongo` with `stringify: false` and `connect-pg-simple` (a `json`
  column) allow queries through the raw database driver. Neither has a query
  API.
- Promotion of chosen attributes: `connect-session-sequelize`
  `extendDefaultFields` and `connect-dynamodb` `specialKeys`. The Sequelize
  columns are written only when the session is created, so a user ID set at
  login never reaches the column.
- AdonisJS added `SessionCollection` in 2025: `tag(sessionId, userId)`,
  `tagged(userId)`, `destroy(sessionId)`, and `supportsTagging()`, which
  throws a named error for stores that cannot tag. The Redis store keeps a Set
  per user and removes dead IDs on read.
- better-auth keeps a JSON list per user in Redis (`active-sessions-<userId>`).
  That hand-kept index produced a series of defects: updates that are not
  atomic ([#11022](https://github.com/better-auth/better-auth/issues/11022)),
  entries left after sign-out
  ([#3819](https://github.com/better-auth/better-auth/issues/3819)),
  duplicates, and a security advisory for sessions that survived user
  deletion. Its `additionalFields` give typed columns, but the library queries
  sessions by user ID only.
- Cookie-only sessions (iron-session, `nuxt-auth-utils`, h3,
  `@fastify/secure-session`) keep no server state, so they can neither search
  nor revoke.
- Redis OM for Node is not a session library. It maps `string` and `boolean`
  to TAG, `number` and `date` to NUMERIC, `point` to GEO, and `text` to TEXT.
  It rebuilds an index only when the schema hash changed.

### Go and Rust

No session library searches inside attributes, and none uses a secondary
index.

- gorilla/sessions has `Get`, `New`, and `Save`, all bound to a request. Its
  Redis stores write one gob or JSON value per session. A `redistore`
  maintainer wrote in 2026 that the library "operates at the session level,
  not the user level".
- alexedwards/scs is the clearest account of the demand. A user asked for
  "sessions belonging to a specific user"
  ([#86](https://github.com/alexedwards/scs/issues/86)). The store could not
  do it, because `Commit` receives bytes that are already serialized. The
  maintainer declined search by "arbitrary parameters" as out of scope and
  later added `Iterate()`, which loads each session and filters in Go. The
  Redis stores implement it with `KEYS` or `SCAN`.
- Iris v12 stores one Redis hash per session with one field per attribute. It
  adopted that layout after `SCAN` over three million keys drove Redis to 50%
  CPU ([#1610](https://github.com/kataras/iris/issues/1610)). It still has no
  search across sessions.
- Fiber refuses enumeration "by design". Beego's count returns a fixed 0 on
  Redis.
- Identity servers show which filters operators want:
  - Zitadel's session search grew one filter per request: session IDs, user
    ID, creation date, creator, user-agent fingerprint, expiry date. Dates
    take five comparison methods. Filters combine with AND. The free-form
    `metadata` map of a session is not searchable.
  - Ory Kratos: by identity, plus an `active` flag; bulk disable or delete by
    identity list, session list, or all. Its session management request has 23
    thumbs-up ([#655](https://github.com/ory/kratos/issues/655)).
  - Casdoor: exact match on owner, user, and application; substring match on
    one column; session limits per user.
- Rust: tower-sessions and actix-session have load, save, and delete by ID.

### PHP, Ruby, and Elixir

No framework queries arbitrary session attributes.

- Native PHP passes one serialized string per session to
  `SessionHandlerInterface`. The phpredis handler has settings for locking and
  compression, none for lookup.
- Laravel gives the strongest demand signal for Redis. The `sessions` table of
  the database driver has fixed columns `user_id` and `last_activity`, both
  indexed, plus `ip_address`, `user_agent`, and `payload`
  ([migration](https://github.com/laravel/laravel/blob/12.x/database/migrations/0001_01_01_000000_create_users_table.php)).
  Jetstream's "Browser Sessions" feature runs SQL on that table and returns an
  empty list on any other driver. The Redis driver is a plain cache get and
  put. Users have asked for the feature on Redis since 2020
  ([jetstream#350](https://github.com/laravel/jetstream/issues/350); the
  maintainer: "The only option is the database driver atm"). A 2024 pull
  request that kept a list of session IDs per user in Redis was closed
  unmerged.
- `Auth::logoutOtherDevices` does not search. It rehashes the password, and a
  middleware then rejects each session whose stored hash no longer matches.
  MediaWiki does the same with a per-user token. This works on any store, but
  it can neither list sessions nor end one chosen session.
- Yii2 `DbSession` has the only real custom promotion in PHP: `writeCallback`
  returns extra columns, such as `user_id`, on each write. The class comment
  states the aim: "to query all active sessions for a particular user or
  terminate them at will".
- WordPress keeps all of a user's sessions in one user meta value, with
  `expiration`, `ip`, `ua`, and `login` for each. The
  `attach_session_information` filter lets plugins add fields. It has "Log Out
  Everywhere Else".
- Symfony, CodeIgniter, CakePHP, and Laminas: one value per session, no query.
- Magento's Redis handler uses a hash per session, but the fields hold
  bookkeeping (`data`, `lock`, `writes`); the attributes stay in one field.
- Rails 8's authentication generator and Phoenix `phx.gen.auth` do not use the
  session store for this. They generate a `sessions` or `users_tokens` table
  that the application owns, so the user can add typed columns and indexes.

## 4. Comparison across ecosystems

| Ecosystem | Typical store | Best search in a session store | Custom searchable attributes |
|---|---|---|---|
| Java | Spring Session | string equality on principal; any declared attribute in Geode and reactive Redis | `redis-sessions-java` only: typed tag, text, numeric, geo |
| .NET | `ISession` over `IDistributedCache` | none; Duende has exact and substring match on fixed fields | none |
| C/C++ | framework-local | lua-resty-session: by audience and subject | none |
| Python | Django, Flask-Session | Django: ORM queries on columns the user adds | by subclassing the Django model; else none |
| JS/TS | express-session stores | AdonisJS: by user tag; better-auth: by user ID | typed columns in better-auth and Lucia, queried by the user's own SQL |
| Go | gorilla, scs | scs: full scan with a predicate in Go | none |
| PHP | native handlers, Laravel | Laravel database driver: `user_id`, `last_activity` | Yii2 `writeCallback` |
| Ruby, Elixir | Rails stores, Phoenix | by user, with time ranges, on an application-owned table | application-owned table |

## 5. Patterns that recur

1. **The opaque value is the root cause.** Each "find the sessions of user X"
   thread ends at the same point: the store sees bytes. One hash field per
   attribute removes that obstacle.
2. **A field per attribute is not enough.** Spring Session Redis, the ASP.NET
   Redis provider through version 4, Iris, and Magento all use a hash with
   several fields, and none can search, because the field values are
   serializer output. A searchable field must hold a plain, typed value.
3. **Each design that works splits the session in two.** A few named, typed
   fields are searchable. The rest stays opaque.
4. **Hand-kept indexes go stale.** Spring Session's Sets, better-auth's list,
   and the Jetstream pull request all met the same defect: the index outlives
   the sessions it points to. This SDK's `sessions-of` hash has the same
   property and handles it by verifying on read.
5. **Scans do not scale.** `KEYS` and `SCAN` are the fallback where no index
   exists (scs, connect-redis, ServiceStack). Iris measured the cost.
6. **Search and bulk action come together.** Duende, Kratos, ABP, Casdoor, and
   `redis-sessions-java` each put "delete by filter" next to "query by
   filter". That pair is what "log out everywhere" needs.
7. **Stored metadata is the same everywhere.** User ID, created time, last
   activity, expiry, IP address, and user agent.
8. **Personal data.** Django refused IP and device fields in core for this
   reason. Duende leaves display name off by default. lua-resty-session can
   hash the subject. WordPress, Drupal, and Rodauth store a hash of the
   session ID, so that a query result cannot be used to take over a session.
9. **Nobody handles a changed index definition well.** The choices seen are:
   drop and recreate at each start, drop and recreate on a hash mismatch, keep
   the old index without warning, or fail.

## 6. Demand, by the field searched

This section orders the evidence by the field a query filters on. It is meant
to answer two questions:

- How much of the demand can a **static index** meet? A static index covers a
  field that the store itself writes, with a type the store fixes: subject,
  created time, last activity, device.
- How likely is a user to declare an index on a **custom field**, one that the
  application defines?

"Shipped in" lists products that built the filter. A shipped filter is
stronger evidence than an issue count, because someone paid to build it.

### 6.1 The table

| # | Field searched | Index type | Use cases | Requests from users | Shipped in | Kind |
|---|---|---|---|---|---|---|
| 1 | None (all sessions) | any index; `*` and a count | List active sessions; count online users | Tomcat ([27 votes, 71.7k views](https://stackoverflow.com/questions/4069444)); ASP.NET ([20 votes, 40k views](https://stackoverflow.com/q/8854176)); PHP ([51 votes, 48k views](https://stackoverflow.com/questions/675913)); Django ([39 votes, 33k views](https://stackoverflow.com/questions/2723052)); [scs#92](https://github.com/alexedwards/scs/issues/92); [fiber#3606](https://github.com/gofiber/fiber/issues/3606); [aspnetcore#54331](https://github.com/dotnet/aspnetcore/issues/54331) | Keycloak session counts; Kratos admin list; Beego count; scs `Iterate` | Static |
| 2 | Subject (principal) | TAG | End all sessions of a user; list a user's sessions and end one; limit sessions per user; change one attribute in all of a user's sessions | Django force logout ([87 votes, 54k views](https://stackoverflow.com/questions/953879)); CodeIgniter ([78 votes, 72k views](https://stackoverflow.com/questions/9563675)); Django lookup by user ([64 votes, 47.5k views](https://stackoverflow.com/questions/235950)); Spring with Redis ([15 votes, 21.5k views](https://stackoverflow.com/questions/43090679)); [express-session#865](https://github.com/expressjs/session/issues/865) (18 comments); [laravel/framework#2444](https://github.com/laravel/framework/issues/2444) (25 comments); [jetstream#350](https://github.com/laravel/jetstream/issues/350); [kratos#655](https://github.com/ory/kratos/issues/655) (23 thumbs-up); [devise#5262](https://github.com/heartcombo/devise/issues/5262); [redistore#56](https://github.com/boj/redistore/issues/56); [scs#86](https://github.com/alexedwards/scs/issues/86); [gin-contrib/sessions#53](https://github.com/gin-contrib/sessions/issues/53); limit per user: Laravel on Redis ([28.5k views](https://stackoverflow.com/questions/27938186)), [better-auth#12342](https://github.com/better-auth/better-auth/issues/12342); change an attribute: [spring-session#398](https://github.com/spring-projects/spring-session/issues/398) (+3) | Spring Session; Laravel database driver; Duende; Kratos; Zitadel; Keycloak; AdonisJS; better-auth; WordPress; Nextcloud; SuperTokens; this SDK (`sessions-of`) | Static |
| 3 | Created time | NUMERIC | Sessions created before a time; end old sessions; sort a list | Zitadel [#6746](https://github.com/zitadel/zitadel/pull/6746) | Zitadel (five comparison methods); Phoenix (validity is a date range on `inserted_at`); Rodauth (lifetime deadline) | Static |
| 4 | Last activity | NUMERIC | Who was active in the last N minutes; sort a device list; end idle sessions | Devise "who's online" ([44 votes, 22.5k views](https://stackoverflow.com/questions/5504130)); online status ([29 votes](https://stackoverflow.com/questions/51931038)); [authelia#1388](https://github.com/authelia/authelia/issues/1388) (last-seen time; open since 2020) | Laravel (`last_activity`, indexed, sorts the Jetstream list); Drupal (`timestamp`, indexed); Nextcloud (`invalidateLastUsedBefore`); Rodauth (`last_use`); `django-user-sessions`; ABP | Static |
| 5 | Expiry time | NUMERIC | Leave expired sessions out of a list | Zitadel [#10384](https://github.com/zitadel/zitadel/pull/10384) (expired sessions appeared in the list) | Zitadel; Duende (index on `Expires`); Kratos (`active` flag) | Static. In Redis the key TTL does this, so the field is rarely needed. |
| 6 | Device or user agent | TAG | Sessions on this device; one session per device; "this device" in a list | Zitadel [#8301](https://github.com/zitadel/zitadel/issues/8301); ServiceStack one session per device ([Stack Overflow](https://stackoverflow.com/q/48752226)); [connect-pg-simple#195](https://github.com/voxpelli/node-connect-pg-simple/issues/195) (logout per device) | Zitadel (user-agent fingerprint ID); ABP (`device`); `authie` (`browser_id`, indexed); better-auth multi-session plugin | Static, if the store defines a device ID. A user-agent string is not an ID. |
| 7 | IP address | TAG | All sessions from one address, for incident response; display in a device list | None found as a search. Requests to store it for display: [kratos#2091](https://github.com/ory/kratos/issues/2091) (5 thumbs-up), [connect-pg-simple#195](https://github.com/voxpelli/node-connect-pg-simple/issues/195) | Casdoor (substring match on a column). Stored but not filterable in Laravel, Drupal, WordPress, Kratos, Keycloak, better-auth | Static, opt-in. It is personal data: Django refused it in core ([#35806](https://code.djangoproject.com/ticket/35806)). |
| 8 | Session size and other statistics | NUMERIC | Largest sessions; size quantiles; counts for metrics | [spring-session#1495](https://github.com/spring-projects/spring-session/issues/1495) (+10) | `redis-sessions-java` (`sessionSize`, always indexed; `largestSessions`, `sessionSizeQuantiles`); Vert.x `size()` | Static |
| 9 | Client or application | TAG | Sessions of one client application; counts per client | None found in a session library | Keycloak (by client, with counts); ABP (`clientId`); Casdoor (`application`); Ory Hydra (`client`); Duende (`ClientIds` on removal) | Static in an identity server, which knows its clients. Custom in a session library. |
| 10 | Tenant or organization | TAG | End all sessions of a tenant; count sessions per tenant | None found in a session library | SuperTokens (`tenant_id` on session lookups); Keycloak (logout-all per realm) | Custom. The subject can carry a tenant, but a session has one subject. |
| 11 | Location | GEO or TAG | Limit logins by location; geo queries | [next-auth#583](https://github.com/nextauthjs/next-auth/issues/583) (2 thumbs-up); named as a possible use in [flask-session#267](https://github.com/pallets-eco/flask-session/issues/267) (0 reactions) | `redis-sessions-java` (geo radius). `authie` stores a country, for display. Duende's blog filters the IP city in application code. | Custom |
| 12 | Display name | TEXT | Find a session by a person's name in an admin screen | None found | Duende (substring match; off by default as personal data) | Custom |
| 13 | Any attribute the application defines | declared by the user | Find or end sessions by a value the application chose | [spring-session#2133](https://github.com/spring-projects/spring-session/issues/2133) (+1, open since 2022); [#1549](https://github.com/spring-projects/spring-session/issues/1549) (+1); [#376](https://github.com/spring-projects/spring-session/issues/376); [#3254](https://github.com/spring-projects/spring-session/issues/3254); [flask-session#267](https://github.com/pallets-eco/flask-session/issues/267) (0 reactions); a comment on [connect-redis#421](https://github.com/tj/connect-redis/pull/421) (1 thumbs-up); a [ScaleOut forum thread](https://community.scaleoutsoftware.com/threads/out-of-context-access-to-the-session-store.741/); Yii2 extra attributes ([5.6k views](https://stackoverflow.com/questions/37966453)) | `redis-sessions-java`; Spring Session Geode (`indexableSessionAttributes`); reactive Spring Session Redis; Yii2 `writeCallback`; connect-dynamodb `specialKeys` | Custom |
| 14 | Application data inside the session, such as cart contents | — | Find sessions that hold a given item | None found in any ecosystem | None | Out of scope. The earlier session design called this a misuse of a session. |

### 6.2 How much do static indexes cover?

Count the rows of the table by kind:

| Kind | Rows | Share of the 14 rows |
|---|---|---|
| Static: a field the store writes, with a fixed type | 1 to 8 | 8 |
| Custom: a field the application must declare | 9 to 13 | 5 |
| Out of scope | 14 | 1 |

The row count understates the result, because the evidence is not spread
evenly:

- **Each request with strong support is in a static row.** Rows 1, 2, and 4
  hold every Stack Overflow question above 20k views. Row 2 holds every issue
  with more than ten comments or reactions. Rows 3, 4, and 6 hold the filters
  that products added in answer to a request.
- **Four static fields meet nearly all of it:** subject, created time, last
  activity, and a device ID. Add the count over all sessions, which needs no
  field.
- **The custom rows hold nine requests in total.** The largest is a Stack
  Overflow question with 5.6k views; no issue among them has more than two
  reactions. In rows 9, 10, and 12 no user of a session library asked at all;
  the evidence is that identity servers ship the filter.

The items in section 2 that are not searches fit the same picture. "Limit
sessions per user" and "change an attribute in all of a user's sessions" both
filter on the subject. "Indexes that stay correct on expiry" is a property of
the index engine, not a field.

### 6.3 How likely is a user to declare an index on a custom field?

Unlikely. The evidence allows no percentage, but four signals point the same
way.

1. **Few users ask.** Row 13 lists every request found in seven ecosystems:
   eight threads. No issue among them has more than two reactions, and the
   RediSearch proposal for Spring Session has been open since 2022 with no
   design.
2. **Where the feature exists, little shows that it is used.**
   - The servlet Redis repository of Spring Session accepts a custom
     `IndexResolver` and then ignores each index but the principal. The one
     issue about this has a single reaction
     ([#1549](https://github.com/spring-projects/spring-session/issues/1549)).
   - better-auth lets users add typed session fields but offers no query on
     them. No request to query them was found.
   - Zitadel stores a free-form `metadata` map on each session and offers no
     filter on it. No request for one was found.
   - Duende puts custom metadata inside the encrypted ticket, where the store
     cannot filter it. Its own blog loads the sessions and filters in
     application code.
3. **When users do promote a field, it is one of the static fields.** The
   examples in the libraries' own documentation are the user ID
   (connect-session-sequelize, Django, Yii2), the IP address (Yii2), and the
   IP country (Lucia). Yii2's `is_trusted` flag is the only application value
   among them.
4. **The standard advice sends this need elsewhere.** Maintainers of scs,
   gorilla, ServiceStack, ScaleOut, and Django tell users to keep such data in
   their own table. Rails 8, Phoenix, Lucia, and better-auth made that the
   design.

One signal points the other way. The README of `redis-sessions-java` calls
searchable sessions one of "the most common asks we see from our users". That
is a vendor's statement about its customers. The project has one star and no
visible issue traffic, so the size of that demand cannot be checked.

Two cautions apply to this conclusion:

- Signal 4 means that some demand never appears as a feature request. The need
  is met outside the session store.
- No agent searched commercial support forums, where requests about tenants
  and compliance would appear. Rows 9 and 10 are the most likely to be
  understated.

### 6.4 Related demands that are not searches

- Readable, stable serialization: Spring Session
  [#529](https://github.com/spring-projects/spring-session/issues/529) (+28),
  [#348](https://github.com/spring-projects/spring-session/issues/348) (+12),
  and the ASP.NET Redis provider issue #185.
- A request in flight that writes a revoked session back:
  [starsessions#58](https://github.com/alex-oleshkevich/starsessions/issues/58),
  [flask-session#287](https://github.com/pallets-eco/flask-session/issues/287).
  Writes to single fields make this easier to cause.
- Indexes that stay correct when sessions expire:
  [spring-session#3183](https://github.com/spring-projects/spring-session/issues/3183)
  (+7) and about ten related issues; better-auth
  [#11022](https://github.com/better-auth/better-auth/issues/11022),
  [#3819](https://github.com/better-auth/better-auth/issues/3819); "Redis,
  session expiration, and reverse lookup"
  ([22 votes, 22k views](https://stackoverflow.com/questions/16741476)).

### 6.5 What the first version supports

Decided on 2026-11-19. This section answers questions 1 and 8 of section 8.
The row numbers are those of the table in 6.1.

| Rows | Field | Support | What the application must do |
|---|---|---|---|
| 2 | Subject | Built-in | Nothing |
| 4 | Last activity | Built-in | Nothing |
| 6 | Device or user agent | Custom | Declare the attribute and write it |
| 8 | Session size and other statistics | Custom | Declare the attribute and write it |
| 11 | Location | Custom | Declare the attribute and write it |
| 12 | Display name | Custom | Declare the attribute and write it |

Rows 3, 5, 7, 9, 10, and 13 are not in the first version as named use cases.
An application can meet rows 7, 9, 10, and 13 with a custom attribute.

#### Built-in: rows 2 and 4

- **The index.** The store defines one index over the session hashes. It
  holds the subject as TAG and the last activity as NUMERIC. When the session
  feature starts, the store creates the index if the server does not have
  it.
- **The API.** Each use case has its own method. The application writes no
  query.

| Row | Use case | Method |
|---|---|---|
| 2 | End all sessions of a user | `revoke_all` (exists) |
| 2 | List the sessions of a user | `list_for_subject` (exists) |
| 2 | End one session of a user | `revoke_id` (exists) |
| 2 | Limit the sessions of a user | `count_for_subject` (exists) |
| 2 | Change one attribute in all sessions of a user | New. No method does this today. |
| 4 | Who was active in the last N seconds | New |
| 4 | Sort a device list by last activity | `list_for_subject` (exists; it sorts by last access) |
| 4 | End the sessions that were idle for more than N seconds | New |

The existing methods keep their names and results. They read the index in
place of the `sessions-of` hash (decision 6).

**Row 4 needs a change to `last_access`.** Today the store sets it on a
write only, so it shows the last write, not the last activity. For row 4 the
store must set it on each load. With the clocks of section 8, that is one
more `HSET` in the pipeline of the load, and no added round trip. The write
changes an indexed value, so each load re-indexes the session (see "The
re-index cost" in section 8).

#### Custom: rows 6, 8, 11, and 12

The application enables a custom search in three steps:

1. **Declare the attribute.** At setup, the application names the session
   attribute and gives its type: TAG, NUMERIC, TEXT, or GEO. The store adds
   the attribute to the index.
2. **Write the attribute.** The application sets the value in
   `request.session`, as it sets any other value.
3. **Fetch by the attribute.** The application calls one advanced method
   with the attribute name and one filter. The method returns the sessions
   that match.

The feature accepts any attribute of the four types. Rows 6, 8, 11, and 12
are the recipes that the documentation shows:

| Row | Recipe | Type | Filter |
|---|---|---|---|
| 6 | Sessions on one device | TAG | Exact match on a device ID |
| 8 | Largest sessions | NUMERIC | Range on a size that the application computes |
| 11 | Sessions near a place | GEO | Radius around a point |
| 11 | Sessions from one country | TAG | Exact match on a country code |
| 12 | Find a session by a person's name | TEXT | Words of the name |

**One attribute for each query.** The first version filters on one attribute
at a time. It has no AND, no OR, and no query that joins two attributes. An
application that needs two filters fetches by one and filters the result in
its own code.

| Type | The one filter |
|---|---|
| TAG | Exact match on one value |
| NUMERIC | A range, with a lower limit, an upper limit, or both |
| TEXT | Words |
| GEO | A radius around a point |

#### Limits from other decisions

- **Encryption.** With an `Encryptor` set, the built-in searches work and the
  custom searches do not (decision 2).
- **No Query Engine.** Neither kind of search works. The store logs one
  warning (decision 6).
- **A TTL on a searchable attribute.** See "An attribute with a TTL and an
  index" in section 8.
- **A search result does not prove that a session is valid** (decision 5).

#### Open

1. **GEO and TEXT with JSON text.** The store writes each value as JSON
   text. The engine reads a GEO value as `lon,lat` with no quotes, so a GEO
   attribute needs a different stored form. TEXT is not tested.
2. **Row 8.** The store, not the application, knows the size of a session.
   As a custom attribute, the application must compute the size itself.
3. **A changed declaration.** The store creates the index only if it is
   missing. The design must say what the store does when the index exists
   and the declared attributes differ from it.
4. **Row 1.** The store has no method today that lists or counts all
   sessions. See the note below.

**Row 1 is not solved today.** The store lists and counts the sessions of
one subject only. Without an index, a list or a count of all sessions needs
a `SCAN` over each session key. The index of this section makes row 1
possible: a query for `*` returns each session, and `LIMIT 0 0` returns the
count. Row 1 is not in the scope above.

## 7. What Redis offers, and its limits

### Field types over a hash

| Type | Stored value | Queries |
|---|---|---|
| TAG | string, split on a separator (default `,`), lowercased unless `CASESENSITIVE` | exact, OR, prefix, suffix, infix |
| NUMERIC | a number as a string | range, and comparison operators in dialect 2 |
| TEXT | any string, tokenized and stemmed | full text, phrase, prefix, fuzzy |
| GEO | `"lon,lat"` | radius |
| GEOSHAPE | WKT | within, contains, intersects, disjoint |
| VECTOR | binary | nearest neighbor, range |

- Redis has no date type and, for hashes, no boolean type. Store a date as a
  Unix timestamp in NUMERIC, and a boolean as `true` or `false` in TAG.
- A hash has no nesting. A dict or list attribute cannot be indexed unless the
  library flattens it or stores a derived scalar field next to it.
- JSON documents support nested paths and arrays, but JSON paths have no
  field TTL. Expiry of single attributes therefore requires a hash.
- **A wrong-typed value removes the whole session from the index (tested on
  Redis 8.8).** The write succeeds and returns no error. A session that was
  in the index leaves it; no search finds it, on any field. An empty string
  in a NUMERIC field counts as wrong ([FAQ](https://redis.io/faq/doc/1dho8668iu/results-of-a-search-operation-are-less-than-expected-why)).
  `FT.INFO` reports the count in `hash_indexing_failures`.
- With the default separator, the TAG value `"a,b"` becomes two tags.

### Index lifecycle

- `FT.CREATE` on an existing index returns "Index already exists". Redis has
  no atomic "create if absent", so workers that start together race.
- `FT.ALTER` can only add fields. A type change or a removal needs a new
  index. Redis advises a versioned index name with `FT.ALIASUPDATE`
  ([docs](https://redis.io/docs/latest/develop/ai/search-and-query/best-practices/index-mgmt-best-practices/)).
- A new index scans existing keys in the background. Queries during the scan
  return partial results. `FT.INFO` shows `indexing` and `percent_indexed`.
- The index definition persists in RDB and AOF; the index data is rebuilt on
  load. With persistence off, or after a failover to an empty node, the
  definition is lost. The library must therefore ensure the index at startup.
- `TEMPORARY` and `FT.DROPINDEX ... DD` delete the indexed hashes. Neither is
  safe for sessions.

### Expiry

- From Redis 8, a query returns only documents and fields that were unexpired
  when it started. Before Redis 8, a query could match on an expired field or
  return a `nil` document
  ([docs](https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/expiration/)).
- When a key expires, Redis removes it from the index. This solves the
  stale-index defect of pattern 4.
- **A refresh of a TTL re-indexes the whole document on Redis 8.0 to 8.8
  (read from source; tested on 8.8.0).** `HGETEX ... EX`, `HEXPIRE`, and
  `EXPIRE` on the key take the same re-index path as `HSET`, and `HSETEX`
  takes it twice. Redis 8.10.3 refreshes the TTL without a re-index. This
  store refreshes the idle TTL on each load, so on the earlier versions each
  request to an indexed session costs a full re-index. No release note
  states the version of the change. "The re-index cost" in section 8 gives
  the detail.

### Availability

- The Query Engine is part of Redis from 8.0, but a source build needs
  `BUILD_WITH_MODULES=yes`. Detect it; do not assume it.
- Missing or partial: Azure Managed Redis with the OSS cluster policy; Redis
  Software with the OSS Cluster API (lightly verified); ElastiCache with
  Valkey 8.2 (vector only); Memorystore for Valkey (no TEXT).
- valkey-search 1.2 has NUMERIC, TAG, TEXT, and VECTOR. It has no GEO, no
  `FT.ALTER`, and no aliases, so an alias swap does not port to Valkey.
- In a cluster, `FT.SEARCH` fails inside `MULTI` or Lua, and results can be
  partial or duplicated during slot migration.
- redis-py routes `FT.*` commands by policy in a cluster from version 7.1.0.
  This project requires `redis>=6.0.0`.

### Query safety

- The default is still dialect 1. Send `DIALECT 2` with each query.
- The query string is a language, so unescaped user input can add clauses. The
  simplest safe form for a tag is `@field:{"value"}` in dialect 2, where only
  `"` needs an escape. redis-py has no escape helper.
- A field name cannot be a parameter. Accept only names from the declared
  schema.
- On a timeout, the default policy (`search-on-timeout RETURN`) returns
  partial results without an error. For "end all sessions of tenant X", a
  silently short result is a security defect.
- The default `LIMIT` is 10. Paging without `SORTBY` can repeat or skip
  results.
- `LIMIT 0 0` returns only a count. `FT.AGGREGATE ... GROUPBY` gives counts by
  group, such as sessions per tenant.

### Encryption

The engine indexes the bytes in the field, so an encrypted attribute cannot be
searched. Two options exist:

- Leave declared attributes in plaintext and encrypt the rest.
- A blind index: store an HMAC of the value in a companion TAG field and hash
  the search term the same way. It gives exact match only, and it shows which
  sessions share a value.

## 8. What this implies for the design

These are conclusions from the evidence, not decisions.

1. **Recorded demand is narrow.** Lookup by user, listing, counting, and bulk
   revoke cover nearly all of it, and the store already offers most of that
   through `sessions-of`. Search over custom attributes would set this SDK
   apart; users do not expect it as a baseline. Exact match (TAG) and ranges
   on timestamps (NUMERIC) cover each use case found. TEXT and GEO have no
   recorded demand.
2. **Storing each attribute in its own field does not by itself enable
   search.** The values must also be plain and typed. This is the main lesson
   from Spring Session and the ASP.NET provider.
3. **Split attributes into declared and undeclared.** The user declares an
   attribute as searchable and gives its type. The store writes it as a plain
   typed value and checks the type on write. The rest stays opaque.
   `redis-sessions-java`, Redis OM, better-auth, and Yii2 all take this shape.
4. **Do not infer types from values.** One value of the wrong type removes the
   session from the index without an error. The store must coerce and
   validate before it writes.
5. **Let the Query Engine keep the index.** It follows key expiry, which
   removes the defect that Spring Session and better-auth users report most.
6. **Keep search optional.** Loading and saving by session ID must not depend
   on the index. The Query Engine is absent on several managed services, so
   the store needs capability detection, a named error, and probably the
   present `sessions-of` path as the fallback for lookup by subject.
7. **Choose an index lifecycle and document it.** The evidence favors: ensure
   the index at startup, treat "already exists" as success, and compare
   `FT.INFO` with the wanted schema. For a changed schema, the choices are
   `FT.ALTER` for additions and a versioned index with an alias for other
   changes. Do not drop and recreate at each start.
8. **Pair query with bulk operations**, and offer count and deterministic
   paging.
9. **Build queries from typed helpers** that escape values, and offer a raw
   query only as a clearly marked option.
10. **Sliding idle timeout against re-index cost.** The store refreshes a
    field TTL on each load. If the source-read finding holds, the design must
    choose among a minimum Redis version, less frequent refresh, and accepted
    write cost.

The decisions below differ from these conclusions in six places:

| Conclusion | What the decisions do |
|---|---|
| 1. TEXT and GEO have no recorded demand | Section 6.5 supports both for custom attributes |
| 3. Declared attributes are plain; the rest stays opaque | Decision 3 gives each attribute its own field, as JSON text. No attribute stays opaque. |
| 6. Keep `sessions-of` as the fallback | Decision 6 removes it. The store has no fallback. |
| 7. Compare `FT.INFO` with the wanted schema; alias swap for a change | Section 6.5 creates the index only if it is missing. A changed declaration is open. |
| 9. Offer a raw query as a marked option | Section 6.5 offers one filter on one attribute, and no raw query |
| 10. The finding on the re-index cost needs a test | A test confirmed it. The refresh is now `EXPIRE` on the key, with the same cost. |

### Questions the design must answer

1. **Scope.** Is the first version "lookup by subject through the index, plus
   declared TAG and NUMERIC attributes", or the full set of tag, numeric,
   text, and geo, as in `redis-sessions-java`?
2. **Encryption.** The store can encrypt the whole session today. With an
   `Encryptor` set, are declared attributes plaintext, blind-indexed, or
   refused?
3. **Nested values.** A session attribute can be a dict or a list. Does an
   undeclared attribute stay as a JSON string in its own field? Can a declared
   attribute be only a scalar?
4. **The idle clock.** Today the field TTL on `d` is the idle clock. With many
   fields, which field carries it, and what does "expire a single attribute"
   mean next to it?
5. **Minimum versions.** Redis 8 for correct search over expiring data; 8.10
   or later if the re-index finding holds; redis-py 7.1.0 for cluster routing.
6. **`sessions-of`.** Keep it as the fallback, or replace it where the Query
   Engine is present?
7. **Schema change.** What happens to live sessions when a declared attribute
   changes type? A session written before the change keeps the old stored
   form until its next write.
8. **Built-in metadata.** Which fields does the store index by default
   (subject, created, last access), and are IP address and user agent opt-in
   because they are personal data?
9. **Valkey.** Is valkey-search a target? It has no aliases, no `FT.ALTER`,
   and no GEO.

### Decisions

Decided on 2026-11-18. The numbers match the questions above. Decisions 3
and 4 were revised on 2026-11-19. Section 6.5 answers questions 1 and 8.

Two terms apply from here on:

- A **system attribute** is one the store writes for itself: the subject,
  the created time, the last access time, the descriptor, and the deadline
  field.
- A **custom attribute** is one the application puts in `request.session`.

2. **Encryption: no search over encrypted custom attributes.** With an
   `Encryptor` set, the store encrypts custom attributes and cannot search
   them. The first version has no blind index. System attributes stay in
   plaintext in each store, so lookup by subject works with or without an
   `Encryptor`.
3. **Nested values: flatten them; each field holds JSON text.** The store
   writes a nested value as one field for each leaf, with the path as the
   field name: `bikes[0]` for a list item and `user.address` for a dict
   member. Each field holds its value as JSON text. The store must do both
   directions: a list or a dict that goes in comes out equal. See "How the
   store encodes values".
4. **The clocks: swapped.** The key TTL is the idle timeout. The TTL of one
   system field is the absolute timeout. The design adds a third, separate
   TTL that the application sets on a single attribute. This TTL is fixed
   and does not slide. It has no link to the two clocks. When either clock
   ends the session, the attribute ends with it. See "The two clocks".
5. **Minimum versions: document, do not enforce.** The documentation must say
   that on a version before Redis 8 the index can keep entries for expired
   sessions. A search result never proves that a session is valid.
6. **`sessions-of`: removed.** Lookup by subject goes through the index. The
   store has no fallback.
   - **No Query Engine.** The store adds a check for the Query Engine to its
     capability checks (`_StoreCapabilities`, next to `supports_hsetex`). If
     the server lacks it, the store logs one warning that says session
     indexing is unavailable. Load and save by session ID still work.
   - **A session that lacks an indexed field** is a data consistency error.
     The store does not try to find or repair it.
   - **A query timeout** must reach the caller. The store must not return a
     partial result as if it were complete.
   - **The descriptor** moves into the session hash, as JSON in one field.
     See "The descriptor" below.
7. **Schema change: no guarantee.** Sessions are short-lived, and the design
   does not handle rolling updates. A type that changes during one run of
   the application is an application error. The store reports it where it
   can detect it, and promises nothing more.
9. **Valkey: not a target.**

### The two clocks

Decided on 2026-11-19. With one field for each attribute, the two clocks
swap places:

```
rf:s:<sid>   hash, key TTL = idle timeout
             deadline field (system), field TTL = absolute timeout
             one field for each attribute, with no TTL unless the
             application sets one
```

- **Idle timeout: the key TTL.** Each load restarts it with
  `EXPIRE <key> <idle>`.
- **Absolute timeout: the TTL of one system field,** the deadline field. The
  store sets it once, when it creates the session, and never changes it.
  When the field is gone, the session is past its deadline.
- **The TTL of a single attribute: a field TTL.** It has no link to either
  clock. No refresh goes near it.

Redis enforces both clocks. The store sends no timestamp and compares none.

| Operation | Commands | Notes |
|---|---|---|
| Create | `HSET` the fields; `HEXPIRE <key> <absolute> FIELDS 1 <deadline field>`; `EXPIRE <key> <idle>` | In one `MULTI` |
| Load | `HGETALL`; `HTTL` on the deadline field; `EXPIRE <key> <idle>`; `HSET` the last access time | One pipeline, one round trip. `HTTL` gives the time left to the deadline. Section 6.5 adds the `HSET`. |
| Save | `HSET` the changed fields; `HDEL` the removed ones | Never writes the deadline field |
| Rotate | Create the new key, with the time left from `HTTL` as the TTL of its deadline field | The deadline does not move |

Rules that follow:

- **A hash without the deadline field is not a session.** The load returns
  no session and deletes the key. This covers a session past its deadline.
  It also covers a write that lands after the session ended and creates the
  key again. Today the store finds that case as a key without a TTL.
- **A save must not write the deadline field.** `HSET` on a field removes
  the TTL of that field.
- **The time left to the deadline comes from `HTTL`,** not from `TTL` on the
  key. The `Max-Age` of the cookie and the session age that
  `valid_session(issued_within=N)` checks (`issued_ago`) both use it.

What this gives:

- At the idle timeout, Redis deletes the whole hash, and the session leaves
  the index.
- A read-only request still costs one round trip.
- No field name is needed for the refresh, so flattened names such as
  `bikes[3]` cause no problem.
- The clocks need only `EXPIRE`, `HEXPIRE`, and `HTTL`, which Redis 7.4 has.

What it costs:

- **A session past its deadline can stay for up to one idle timeout.** The
  deadline field expires, and the rest of the hash stays until the next load
  deletes it or the key TTL ends. No load returns it as a session.
- **The index holds that session for the same time.** The built-in index of
  section 6.5 does not hold the deadline field, so a search cannot leave
  such a session out. Decision 5 already says that a search result does not
  prove that a session is valid.
- **Each load re-indexes the session.** On Redis 8.0 to 8.8, `EXPIRE` on the
  key of an indexed hash takes the re-index path. On each version, the
  `HSET` of the last access time changes an indexed value. See "The
  re-index cost".

Checked on Redis 7.4 on 2026-11-19: after `HEXPIREAT` on one field and
`EXPIRE` on the key, the field expires, the other fields stay, and the key
TTL does not change.

### How the store encodes values

Decided on 2026-11-19. Each hash field holds its value as JSON text. The
store writes a value with `json.dumps` and reads it with `json.loads`, as
the store does today for the whole session.

| Python value | Stored field value |
|---|---|
| `7` | `7` |
| `"7"` | `"7"` |
| `129.5` | `129.5` |
| `True` | `true` |
| `None` | `null` |
| `[]` | `[]` |
| `{}` | `{}` |

The JSON text carries the type of each value. The number `7` and the string
`"7"` have different stored forms, so each comes back as it went in. The
store needs no model to restore a type, and a plain `request.session` dict
works. `None`, an empty list, and an empty dict each have a stored form.

Effect on the Query Engine, tested on Redis 8.8 on 2026-11-19:

| Stored value | Field type | Result |
|---|---|---|
| `129.5` | NUMERIC | Works. A range query finds it. |
| `true` | TAG | Works. `@admin:{true}` finds it. |
| `"admin"` | TAG | The quotes are part of the tag. `@role:{admin}` finds nothing. `@role:{\"admin\"}` finds it. |
| `null` | NUMERIC | The session leaves the index. No search finds it. |
| `"a,b"` | TAG | The default separator splits it into `"a` and `b"`. |

Rules that follow:

- **The store builds each query.** It encodes a search term as it encodes a
  value, so the term `admin` becomes `"admin"`.
- **A searchable numeric attribute must not hold `null`.** The write
  succeeds, and the session leaves the index. The store must refuse the
  value or leave the field out. The design must choose.
- **A searchable string attribute needs a separator** that its values cannot
  contain.

A GEO attribute cannot use JSON text (section 6.5, open point 1).

Not tested: TEXT, `[]` and `{}` in a searchable field, and a string with
characters outside ASCII. `json.dumps` writes such a character as `\uXXXX`
by default, which can break a search for the word.

### An attribute with a TTL and an index

An attribute that is both indexed and has its own TTL causes these
problems:

- When the field expires, the session lacks an indexed field. Decision 6
  calls that state a data consistency error. Here the store would cause it
  in normal operation.
- Before Redis 8, a search can match on the expired value.
- Each time the application sets the TTL, the engine can re-index the
  session.

The design must choose between two rules:

| Rule | Effect |
|---|---|
| Forbid | The store raises a configuration error at setup when one attribute has an index and a TTL. Both are declared, so the store can check before the first request. |
| Advise against | The documentation warns. The store accepts the combination. |

A system attribute never has its own TTL.

### The descriptor

The descriptor is what a device list shows for a session: an IP address, a
user agent, a device name. The store does not collect any of these by
default. The first session design made this choice (exclusion X-12 in
`docs/specs/session-design.md`, deleted in commit `1bcdf78`). Its research
quoted OWASP: a check of the IP address and user agent detects a stolen
session, and does not defend against one.

The application enables the descriptor with `descriptor_of`:

```python
FastAPIRedis(app).lifespan().sessions(
    descriptor_of=lambda req, s: {
        "ip": req.client.host,
        "ua": req.headers.get("user-agent"),
    },
)
```

- With no `descriptor_of`, the store writes no descriptor.
- The store calls `descriptor_of` on each write of the session, with the
  request that caused the write. The descriptor therefore shows the last
  request that changed the session, not the request that created it.
- Today the descriptor is the value in the `sessions-of` hash. After
  decision 6 it is one system field in the session hash, with the dict
  serialized as JSON. It never appears in `request.session`.
- A search that lists sessions returns the field with each result
  (`RETURN`), so a device list still reads no session record.
- The field is plaintext, as the descriptor is today. Do not put a secret in
  it.
- The engine cannot search inside the JSON. To find sessions by IP address,
  user agent, or device, the application declares a custom attribute and
  writes the value to it (section 6.5). The store has no system attribute
  for these.

### The re-index cost

The Query Engine learns of a change to a hash from the keyspace event that
the command raises. On Redis 8.0 to 8.8 the events for `HEXPIRE` and for
`HGETEX ... EX` take the same path as the event for `HSET`: the engine
removes the document from the index and adds each indexed field again. The
values did not change; only a TTL did. `HSETEX` raises two events, so it
takes the path twice. Redis 8.10 updates the TTL and leaves the index
alone.

This matters because the store restarts the idle TTL on each load:

- **Each request becomes an index write.** A request that only reads the
  session still makes the engine remove and add one document.
- **The cost grows with the number of indexed fields** of the session, not
  with the size of the session. A session with the subject, the last access
  time, and a few declared attributes costs a few index updates on each
  request.
- **Each removal leaves a dead entry in the index** until the garbage
  collector of the engine clears it.
- **The key TTL has the same cost.** `EXPIRE` on the key of an indexed hash
  re-indexes the document, as a TTL change on a field does.
- **The last access time adds a cost on each version.** Section 6.5 makes
  the store write it on each load, and it is an indexed field. That write
  changes a value, so it re-indexes the session on Redis 8.10 too. The
  answers below remove the cost of the TTL refresh only.

A test on 2026-11-19 confirmed the finding. On Redis 8.8.0, `EXPIRE`,
`HEXPIRE`, and `HGETEX ... EX` each re-index the document, and `HSETEX`
re-indexes it twice. On Redis 8.10.3, the first three do not re-index, and
`HSETEX` re-indexes once. The test counted `max_doc_id` in `FT.INFO`. Nobody
has measured the size of the cost. Three answers exist:

| Answer | Cost |
|---|---|
| Accept the cost and document it | Extra load on Redis 8.0 to 8.8. This matches decision 5. |
| Refresh less often, for example only when less than half of the idle timeout remains | One more read on each load. The real idle timeout becomes a range from half the setting to the setting. |
| Require the version with the fix | Contradicts decision 5 |

Measure before the design chooses: the same load against 8.8 and against
8.10, with and without an index.

### What the decisions leave open

1. **The store with no Query Engine.** Decision 6 gives the warning. It does
   not say what `revoke_all`, `list_for_subject`, and `count_for_subject` do
   on such a server. A named error at the call is the likely answer.
2. **How the store sees a timeout.** With the default policy
   (`search-on-timeout RETURN`) the server returns a partial result without
   an error. The store needs a signal: the server set to
   `search-on-timeout FAIL`, or the warning that a RESP3 reply carries.
3. **`null` in a searchable numeric attribute.** The store must refuse the
   value or leave the field out. See "How the store encodes values".
4. **A key that contains `.` or `[`.** It reads as a path.
5. **Custom attributes under encryption.** Either the store encrypts each
   flattened field, or it keeps the custom attributes in one encrypted
   field as today. The second is simpler, but an attribute then cannot have
   its own TTL.
6. **Names of system fields.** A system field and a flattened custom
   attribute share one hash. The system fields need names that no custom
   attribute can take.
7. **The cost of last access.** Section 6.5 makes the store write the last
   access time on each load. Each request then re-indexes the session, on
   each Redis version. The design must accept that cost or write the value
   less often, for example once in each minute.

## 9. Not verified

- The first version that has the changed `hexpire` re-index path. A test
  found the old path on 8.8.0 and the new path on 8.10.3; no release note
  states the version.
- Whether `PARAMS` works inside a TAG clause.
- How `FT.CREATE` propagates across shards in a Redis Open Source cluster, and
  what `MODULE LIST` shows for the built-in Query Engine on Redis 8.
- That Redis Software with the OSS Cluster API lacks search (from a search
  snippet; the page did not load).
- That `findByPrincipalName` in `redis-sessions-java` returns nothing without
  a declared field (read from source, not run).
- That no Redis-published Spring Session module uses RediSearch other than
  `redis-sessions-java`, and that no Go session library uses a secondary
  index. Both are absences found by search.
- That a RESP3 `FT.SEARCH` reply carries a warning when the query timed out.
- The size of the re-index cost. Nobody has measured it.
- GitHub rate limits cut short some tracker sweeps for .NET, Python, and
  Rust, so the demand counts there are lower bounds.
