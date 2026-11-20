# Session attribute search: design

Design for [issue #108](https://github.com/redis/fastapi-redis-sdk/issues/108).
The research and the decisions are in `session-management-search.md`.

## 1. Requirements

1. The store keeps each session attribute in its own hash field.
2. A session that the application writes comes back equal, with or without a
   Pydantic model.
3. The idle timeout and the absolute timeout work as today. Redis enforces
   both.
4. The application can give a TTL to a single attribute when it writes the
   attribute, and can remove the TTL without a write of the value.
5. The store finds sessions by subject and by last activity through a
   built-in index. Each use case has a method. The built-in index holds
   system fields only, so no value from the application can affect it.
6. The application can declare an attribute as searchable, with the type TAG,
   NUMERIC, TEXT, or GEO, and fetch sessions by that attribute. Declared
   attributes are in a second, custom index.
7. A search filters on one attribute. The first version has no query that
   involves more than one attribute.
8. Load and save by session ID work on a server without the Query Engine.
9. The `sessions-of` hash (`rf:si:<subject>`) is removed. There is no
   fallback.

## 2. Storage

One hash for each session. No other key.

```
rf:s:<sid>     hash, key TTL = idle timeout

  s:sub        <subject>             JSON text; absent for an anonymous session
  s:created    <unix seconds>        field TTL = absolute timeout
  s:seen       <unix seconds>        last access
  s:desc       <descriptor>          JSON text; absent with no descriptor_of

  a:<path>     <value>               one field for each leaf of request.session
```

- A field that starts with `s:` is a system field. A field that starts with
  `a:` is a custom attribute. The prefixes prevent a name collision.
- System fields are never encrypted and never appear in `request.session`.
- No system field other than `s:created` has a TTL.

### 2.1 Paths

The store flattens `request.session` into one field for each leaf:

| Value                                      | Fields                     |
|--------------------------------------------|----------------------------|
| `{"role": "admin"}`                        | `a:role`                   |
| `{"user": {"address": {"city": "Sofia"}}}` | `a:user.address.city`      |
| `{"bikes": ["road", "gravel"]}`            | `a:bikes[0]`, `a:bikes[1]` |
| `{"tags": []}`                             | `a:tags` = `[]`            |
| `{"meta": {}}`                             | `a:meta` = `{}`            |

- A dict key that is not a string becomes a string, as JSON does today.
- In a key, the store writes `%` as `%25`, `.` as `%2E`, `[` as `%5B`, and
  `]` as `%5D`. The load reverses this.
- An empty list and an empty dict are leaves.
- A GEO attribute is a leaf. The store does not flatten it.

### 2.2 Values

Each `a:` field holds JSON text: `json.dumps(value, ensure_ascii=False)` on
write and `json.loads` on read.

| Python value | Stored |
|---|---|
| `7` | `7` |
| `"7"` | `"7"` |
| `True` | `true` |
| `None` | `null` |

One exception: a GEO attribute holds `<lon>,<lat>` with no quotes. The
application writes it as `[lon, lat]`, and the load returns `[lon, lat]`.

### 2.3 With an `Encryptor`

The layout does not change. The store flattens the session as in section
2.1, with or without an `Encryptor`.

- **Each `a:` field holds its value encrypted.** The store takes the JSON
  text of section 2.2, encrypts it with the `Encryptor`, and writes the
  result as Base64. The load reverses this for each field.
- **Field names stay in plaintext,** and so do the system fields. Redis
  shows which attributes a session has, how many, and the length of each
  value. It does not show a value.
- **The save compares plaintext.** The store decrypts the fields at load and
  compares the session with that state. A new encryption of an equal value
  gives a different result, so the store does not compare ciphertext.
- **An attribute TTL works,** because each attribute still has its own
  fields.
- **A searchable attribute together with an `Encryptor` raises
  `SessionConfigurationError` at setup.** The engine cannot search an
  encrypted value. The built-in searches work, because the system fields are
  plaintext.
- **A field that does not decrypt** makes the record unreadable. The store
  handles it as a failed read, as it does today.

**Documentation duty.** The session guide must state:

- that an `Encryptor` hides each value and does not hide the names of the
  attributes, their number, or the length of each value;
- that the application must not put sensitive data in an attribute name or
  in a dict key, because a dict key becomes part of a field name;
- that an encrypted value is longer than 64 bytes in most cases, so a
  session with an `Encryptor` uses the larger hash form of Redis.

The store does not use the `coder` option for sessions. Each value is JSON
text, with or without an `Encryptor`.

## 3. Clocks and commands

| Clock | Mechanism |
|---|---|
| Idle timeout | Key TTL. Each pipeline that writes to the session key ends with `EXPIRE <key> <idle>`. |
| Absolute timeout | TTL of `s:created`. The store writes the field once, at creation, and never writes it again. |
| Attribute TTL | TTL of the `a:` fields of that attribute. Set in the save that writes the attribute, and only if the application gave a TTL with that write. Never refreshed. |

| Operation | Commands, in one pipeline | Rules |
|---|---|---|
| Create | `MULTI`; `HSET` all fields; `HEXPIRE <key> <absolute> FIELDS 1 s:created`; `HEXPIRE` for each attribute that the application wrote with a TTL; `EXPIRE <key> <idle>`; `EXEC` | |
| Load | `HGETALL`; `HTTL <key> FIELDS 1 s:created`; write `s:seen`, only where a search can run (see below); `EXPIRE <key> <idle>` | A hash without `s:created` is not a session: return no session, and `DEL` the key. `HTTL` gives the time left to the deadline, for the cookie `Max-Age` and `issued_ago`. |
| Save | `HSET` the changed fields; `HDEL` the removed fields; `HEXPIRE` for each attribute that the application wrote with a TTL in this request; `HPERSIST` for each attribute that the application called `persist` for; `EXPIRE <key> <idle>` | The store compares the flattened session with the flattened state at load, and writes only the difference. It never writes `s:created`: `HSET` removes a field TTL, so a write would remove the absolute timeout. For the same reason it never writes an unchanged field. The same rule applies to `s:sub` and `s:desc`: the save writes each one only when its value differs from the value at load. |
| Rotate | First trip: `HTTL` for each `a:` field that the load returned; `DEL` the old key. The store does not track which attributes have a TTL, so it asks for each field. Second trip: Create (the row above) under the new ID, from the session in memory, with the attribute TTLs that `HTTL` returned. | The old key goes first. The new session starts a new absolute clock. See "Rotation" below. |
| Revoke | `DEL <key>` | |

The write of `s:seen` on load:

- **Only a search reads `s:seen`:** `list_active`, `revoke_idle`, the sort
  of each list, and `SessionInfo.last_access`. So the load writes it only on
  a server where a search can run.
- **With the Query Engine and `HSETEX`** (Redis 8.0 and later): the load
  sends `HSETEX <key> FXX FIELDS 1 s:seen <now>`. `FXX` writes only if the
  field exists, so a cookie that names no session creates no key.
- **On any other server:** the load does not write `s:seen`. The pipeline is
  `HGETALL`, `HTTL`, and `EXPIRE`, and no command in it can create a key.
  This includes a server with the Query Engine and without `HSETEX`, such as
  Redis Stack on 7.4. There, `s:seen` shows the last save, not the last
  request, and the documentation must say so.
- **While the capability check has no answer,** the load does not write
  `s:seen`.
- **Create and save write `s:seen` on each server.** The field is then
  present if the server gains the Query Engine later.

The load has no plain `HSET`. A plain `HSET` creates the key when it is
missing.

Attribute TTL in a save:

- **An attribute that did not change keeps its TTL.** The save does not write
  its fields. This includes a write of a value equal to the stored value:
  the save sees no difference and sends no command for the attribute.
- **`persist` removes the TTL and keeps the value.** The save sends
  `HPERSIST <key> FIELDS <n> <each field of the attribute>`. The store does
  not read the value. A field with no TTL, or a field that does not exist,
  is not an error.
- **A field that changed loses its TTL,** unless the application gave a TTL
  with the new write. `HSET` removes the old TTL; the store does not read it
  or carry it over.
- **A write with a TTL sets that TTL on each field of the attribute,**
  changed or not.
- **The store does not keep the fields of one attribute in step.** A change
  to one nested value, without a TTL, removes the TTL from that field only.
  The other fields of the attribute keep theirs, so the attribute can expire
  in parts. The application is responsible for the consistency of an
  attribute that has a TTL.
- **A rotation is not a write by the application.** It copies each field with
  the time left on its TTL. `HTTL` gives whole seconds and rounds up, so a
  rotation can move an attribute TTL later by up to one second.

Rotation, as in the present store:

- **The old key goes first.** The store deletes the old key, then creates the
  new one. A rotation that fails between the two signs the user out. It
  never leaves two session IDs that both work.
- **The new session starts a new absolute clock.** The create writes a new
  `s:created`, with the present time as its value and the full absolute
  timeout as its TTL. A rotation follows a sign-in or a change of privilege,
  so the deadline runs from that moment. The store does not read or copy the
  time left on the old `s:created`.
- **The idle timeout starts again,** because the create ends with
  `EXPIRE <key> <idle>`.

With `session_refresh_on_load=False`, the load sends `HGETALL` and `HTTL`
only. When the request used the session, the response step sends `EXPIRE`,
and the write of `s:seen` under the rule above.

## 4. Indexes

Two indexes over the same session hashes. Each index judges a hash on its
own schema, so a value that one index rejects does not remove the session
from the other.

| Index | Name | Holds | Used by |
|---|---|---|---|
| Built-in | `<prefix>:s-idx` | System fields only: `s:sub`, `s:seen`, `s:created` | The built-in methods (section 5.3) |
| Custom | `<prefix>:s-idx-a:<hash>` | The declared attributes, `s:seen` for the sort, and `s:created` for the live filter | `find` and `count` (section 5.4) |

**The built-in index holds no custom attribute.** Its schema is fixed. The
store writes each value in it, so no value from the application can remove a
session from it. `revoke_all`, `list_for_subject`, and `count_for_subject`
do not depend on a custom attribute or on the declaration.

```
FT.CREATE <prefix>:s-idx ON HASH PREFIX 1 <prefix>:s: SCHEMA
    s:sub      TAG CASESENSITIVE SEPARATOR "\x1f"
    s:seen     NUMERIC SORTABLE
    s:created  NUMERIC

FT.CREATE <prefix>:s-idx-a:<hash> ON HASH PREFIX 1 <prefix>:s: SCHEMA
    s:seen     NUMERIC SORTABLE
    s:created  NUMERIC
    <one line for each declared attribute>
```

`<hash>` is the first 8 hexadecimal characters of the SHA-256 of the schema
lines of the custom index, in sorted order. Two stores with the same
declaration give the same name. A changed declaration gives a new name, so
the store never compares an index with its declaration.

The store ensures the indexes when the session feature starts:

1. If the Query Engine is absent, skip (section 6).
2. Built-in index: `FT.CREATE`. Treat "Index already exists" as success. The
   store never drops this index.
3. Custom index, only if `searchable` is not empty: `FT.CREATE` under the
   name with the hash. Treat "Index already exists" as success.
4. `FT._LIST`. For each other index whose name starts with
   `<prefix>:s-idx-a:`, log a warning and `FT.DROPINDEX` (without `DD`).
   Treat "index not found" as success: another worker dropped it first.
   With an empty `searchable`, this step drops each custom index.

The ensure step sends no `FT.INFO`.

No field has an alias (`AS`). The schema names each field as the hash names
it. Declared attributes:

| Type | Schema line | Value the store accepts on write |
|---|---|---|
| TAG | `a:<path> TAG CASESENSITIVE SEPARATOR "\x1f"` | `str` or `bool` |
| NUMERIC | `a:<path> NUMERIC` | `int` or `float`, not `bool` |
| TEXT | `a:<path> TEXT` | `str` |
| GEO | `a:<path> GEO` | `[lon, lat]`, two numbers |

- A declared attribute can be absent from a session. The session stays in the
  custom index.
- A declared attribute with a value of another type, `None` included, raises
  `SessionAttributeTypeError` on save. The store writes nothing.
- A value that the engine cannot index removes the session from the custom
  index only. `find` and `count` then miss the session. The built-in methods
  still find it.

### 4.1 Queries

The store builds each query. It sends `DIALECT 2` with each one. A built-in
method queries the built-in index. `find` and `count` query the custom
index.

**The driver helpers.** The store builds each index command and each search
command with the search helpers of redis-py, not with `execute_command`:

- `TagField`, `NumericField`, `TextField`, `GeoField`, and `IndexDefinition`
  for `FT.CREATE`;
- `ft(<index>).create_index`, `.info`, and `.dropindex` for the index
  lifecycle;
- `Query`, with `sort_by`, `paging`, `return_fields`, `no_content`, and
  `dialect`, and `ft(<index>).search` for each search.

The store still writes the query string that `Query` takes, with the field
names and the values in the forms of this section. `ft().search` returns a
different type on RESP2 and on RESP3, so the store reads both.

**Field names in a query.** A query names a field as `@<f>`, where `<f>` is
the field name with a backslash before each character that is not a letter,
a digit, or `_`. The field `a:cart.total` is `@a\:cart\.total`, and `s:sub`
is `@s\:sub`. `SORTBY` and `RETURN` take the field name with no escape.
Two declared paths never give one name, so `cart.total` and `cart_total` can
both be searchable.

| Filter | Query |
|---|---|
| TAG equals `v` | `@<f>:{"<t>"}`, where `<t>` is the JSON text of `v` with `\` and `"` escaped |
| NUMERIC between `lo` and `hi` | `@<f>:[<lo> <hi>]`; `-inf` or `+inf` for a missing limit |
| TEXT | `@<f>:(<text>)`. The store passes `<text>` unchanged (section 4.2). |
| GEO near | `@<f>:[<lon> <lat> <radius> m]` |

- **The live filter.** The store adds the clause `@s\:created:[-inf +inf]` to
  each query, on both indexes. The clause matches a hash that has
  `s:created`. It leaves out a session past its deadline, whose `s:created`
  expired, and a hash that a late save created with no `s:created`. Counts,
  pages, and bulk revokes therefore see live sessions only. Example:
  `@s\:sub:{"<t>"} @s\:created:[-inf +inf]`.
- The live filter is internal. It is not a second filter of the application
  (requirement 7).
- Each query that returns sessions has `SORTBY s:seen DESC`,
  `RETURN 3 s:created s:seen s:desc`, and `LIMIT <offset> <limit>`. Section
  5.5 gives the limit of each method. The store sends an explicit `LIMIT`
  with each query; the default of Redis is 10 rows.
- A count is the same query with `LIMIT 0 0`.
- A bulk revoke repeats `LIMIT 0 1000` with `NOCONTENT`, and `DEL` for each
  key, until the query returns nothing.

### 4.2 TEXT search and the query syntax

The store does not escape, split, or limit the text of a TEXT filter. The
text is Redis query syntax. A limit in the SDK would make some use cases
impossible, such as a prefix search or a fuzzy search.

Requirement 7 still applies. The operators in the text work on the one
attribute that the call names. A query that involves a second attribute is
not a feature of the first version, and the store gives no support for it.

| Text | Meaning |
|---|---|
| `petrova` | The word |
| `ana petrova` | Both words |
| `pet*` | A word that starts with `pet` |
| `%petrva%` | A word within one edit of `petrva` |
| `petrova\|picard` | Either word |
| `-petrova` | Each session without the word |
| `jean-luc` | `jean` and not `luc`. To find "Jean-Luc", send `jean luc`. |

**The application is responsible for the text.** It must sanitize the input
and limit it to what its use case needs. Text from an end user can change the
meaning of the query:

- An operator such as `-`, `|`, `*`, or `%` changes which sessions match.
- Text that closes the parenthesis can add a clause on another declared
  attribute, such as `x) | @a\:role:{...}`. This breaks requirement 7. The
  application must not let such text through.
- Text with a syntax error makes Redis reject the query.

The store's part:

- A query that Redis rejects for its syntax raises `ValueError`, with the
  message from Redis.
- The live filter of section 4.1 stays outside the parenthesis, so the text
  cannot remove it from a well-formed query.

**Documentation duty.** The session guide must:

- state that the text of a TEXT filter is Redis query syntax, with the table
  above;
- link to the reference:
  <https://redis.io/docs/latest/develop/ai/search-and-query/advanced-concepts/query_syntax/>;
- state that the application must sanitize and limit input from an end
  user, with one example of a sanitizer that keeps letters, digits, and
  spaces only.

The TAG, NUMERIC, and GEO filters are not query syntax. The store builds
them from typed values, as section 4.1 shows. A NUMERIC or GEO filter
accepts finite numbers only; `nan` or `inf` raises `ValueError`.

## 5. API

### 5.1 Setup

```python
FastAPIRedis(app).lifespan().sessions(
    searchable={"device_id": "tag", "cart.total": "numeric",
                "name": "text", "location": "geo"},
)
```

- `searchable`: path of a leaf to its type.

Setup has no option for an attribute TTL.

### 5.2 Attribute TTL

```python
request.session.set("flash", "Settings saved.", ttl=60)   # TTL of 60 seconds
request.session.set("flash", "Profile saved.")            # no TTL
request.session["flash"] = "Profile saved."               # no TTL
request.session.persist("flash")                          # remove the TTL, keep the value
```

- `Session.set(name, value, *, ttl=None)` writes a top-level attribute.
  `ttl` is in seconds. The TTL applies to each field of the attribute.
- The TTL starts when the store saves the session, at the end of the
  request.
- To change the value and keep the expiry, the application computes the time
  left and passes it as `ttl`. The store does not carry a TTL over.
- `Session.persist(name)` removes the TTL of a top-level attribute and
  keeps its value. The application does not need the value to call it. It
  is the way to remove a TTL: a write of an equal value changes nothing, so
  it does not remove the TTL.
- `persist` for an attribute that is absent or has no TTL does nothing.
- The documentation must state the rules of section 3 for an attribute with
  a TTL: a change without `ttl` removes the TTL from the changed fields
  only, and the application is responsible for the consistency of the
  attribute.
- `ttl` that is not a positive integer raises `ValueError` at the call. This
  is the only check that `Session` makes. `Session` has no access to the
  configuration of the store.
- `ttl` for a name that is in `searchable`, or that holds a path in
  `searchable`, raises `SessionConfigurationError` at the save, beside the
  type check of section 4. The store writes nothing.

How `Session` carries a TTL:

- `Session` gains one slot. It maps the name of an attribute to a pending
  TTL, or to a mark for `persist`.
- `set` with `ttl` and `persist` write the entry for the name.
- Each other change of that name removes the entry: `[...] =`, `set` with
  no `ttl`, `del`, `pop`, `update`, `|=`, `setdefault`, and `clear`. So the
  last call for a name decides, with no rule for each pair of calls.
- The save reads the slot. The slot is not part of `raw()`, and the store
  does not write it to Redis.

### 5.3 Built-in methods

| Method | Query | Change |
|---|---|---|
| `list_for_subject(subject)` | `s:sub` equals, `LIMIT 0 1000` | Reads the built-in index. Returns at most 1,000 sessions. |
| `count_for_subject(subject, *, limit=None)` | `s:sub` equals, count | Reads the built-in index |
| `revoke_all(subject)` | `s:sub` equals, bulk revoke | Reads the built-in index |
| `revoke_id(session_id, *, subject)` | None. `HGET s:sub`, compare, `DEL`. | No index |
| `list_active(within, *, limit=100, offset=0) -> list[SessionInfo]` | `s:seen` in `[<now - within> +inf]`, `LIMIT <offset> <limit>` | New |
| `revoke_idle(older_than) -> int` | `s:seen` in `[-inf <now - older_than>]`, bulk revoke | New |

`within` and `older_than` are seconds. `SessionInfo` keeps its fields:
`session_id`, `created`, `last_access`, `descriptor`.

### 5.4 Custom search

```python
await store.find("device_id", equals="d-91")
await store.find("cart.total", between=(100, None))
await store.find("name", text="petrova")                  # Redis query syntax, section 4.2
await store.find("location", near=(23.32, 42.69, 5000))   # lon, lat, meters
await store.count("device_id", equals="d-91")
```

- `find(name, *, equals | between | text | near, limit=100, offset=0) ->
  list[SessionInfo]`.
- `count(name, *, equals | between | text | near) -> int`.
- The call takes one filter, and the filter must match the declared type.
  Any other call raises `ValueError`.
- A name that is not in `searchable` raises `ValueError`.

### 5.5 Result limits

| Method | Rows returned | Rule |
|---|---|---|
| `count_for_subject`, `count` | None | `LIMIT 0 0`. The query does not sort. |
| `revoke_all`, `revoke_idle` | None | The store ends each match, in batches of 1,000, with no sort. The method returns the number ended. |
| `list_for_subject` | At most 1,000, newest access first | A fixed limit. The method has no `limit` parameter. `count_for_subject` gives the full number. |
| `list_active`, `find` | One page | `limit` (default 100) and `offset` (default 0). `offset + limit` above 10,000 raises `ValueError`. |

- The two numbers, 1,000 and 10,000, are constants of the store, not
  settings.
- `limit` below 1 or `offset` below 0 raises `ValueError`.
- The first version has no cursor. A caller that needs each match of a large
  result reads pages, up to the limit of 10,000 rows.

**Documentation duty.** The session guide must state:

- the limit of `list_for_subject`, and that a subject with more sessions
  needs `count_for_subject` for the full number;
- that a sorted page costs about as much as a sort of all the matches, so a
  page deep in a large result is expensive;
- that "how many" is a count, not the length of a list.

### 5.6 Errors

Each new error is in the present hierarchy of `exceptions.py`, and each name
ends in `Error`, as the present names do.

```
SessionError                              (exists)
├── SessionConfigurationError             (exists)
├── SessionAttributeTypeError             (new; also a TypeError)
└── SessionStoreError                     (exists)
    └── SessionSearchError                (new)
        ├── SessionSearchUnavailableError (new)
        └── SessionSearchTimeoutError     (new)
```

| Error | When |
|---|---|
| `SessionConfigurationError` | A setup rule in section 2.3 fails. A save holds a TTL for a searchable attribute (section 5.2). |
| `ValueError` | A call rule in sections 4.2, 5.2, 5.4, or 5.5 fails |
| `SessionAttributeTypeError` | A save holds a declared attribute with a value of another type |
| `SessionSearchUnavailableError` | A method that reads an index runs on a server without the Query Engine |
| `SessionSearchTimeoutError` | The server reports a timeout for a query |

A handler that catches `SessionStoreError` also catches the two search
errors. A handler that catches `SessionError` catches each error of this
table but `ValueError`.

## 6. Capability and failure

- **Query Engine.** `_StoreCapabilities` gains `supports_search`, from
  `COMMAND INFO FT.SEARCH`. The probe follows the present pattern: its own
  function, `probe_search_support`, beside `probe_hsetex_support` in
  `session_backend.py`, with the same three answers (supported, not
  supported, no answer). The design adds no shared probe. If it is false, the store logs one warning:
  session indexing is unavailable. Load, save, rotate, `revoke`, and
  `revoke_id` work. Each method that reads the index raises
  `SessionSearchUnavailableError`.
- **Timeout.** The store raises `SessionSearchTimeoutError` when the server
  returns a timeout error, or when the reply has a timeout entry in
  `warning` (RESP3). At startup the store reads
  `CONFIG GET search-on-timeout`. If the value is not `fail`, it logs one
  warning: a timeout can return a partial result.
- **Versions.** The documentation states: before Redis 8 a query can match
  an expired field, so the live filter can let a session past its deadline
  through; before Redis 8 the index can hold
  expired sessions; a search result does not prove that a session is valid;
  on Redis 8.0 to 8.8 each request re-indexes the session.
- **A session that lacks an indexed field.** The store does not find or
  repair it.

## 7. What changes in the present code

- Remove the `sessions-of` hash, its key segment `si`, and `_index_add`,
  `_index_remove`, `_index_members`, `_verify`.
- Remove the envelope (`m`, `d`) and field `d`.
- `absolute_remaining` comes from `HTTL` on `s:created`, not from `TTL` on the
  key.
- `descriptor_of` gives the value of `s:desc`, and `subject_of` gives the
  value of `s:sub`. The store calls both on each save. It writes a field
  only when the value differs from the value at load. The present store
  writes its index entry again on each save, to repair an entry in the
  separate `rf:si` hash; that hash is gone, so the repair has no purpose.
- A session stored in the present layout has no `s:created`. The load treats it as
  no session, so an upgrade signs each user out once.

### 7.1 Structure of the new code

**The storage seam.** One module with no Redis in it holds the layout:

| Function | Does |
|---|---|
| `flatten(session) -> dict[str, str]` | Sections 2.1 and 2.2: paths, escapes, JSON text, the GEO form |
| `unflatten(fields) -> dict` | The reverse |
| `diff(before, after) -> (to_set, to_delete)` | The difference that a save writes |

- Encryption of a value is one optional step in `flatten` and `unflatten`.
  There is one layout (section 2.3).
- Save and rotate call these functions and share one write path.
- The store keeps the flattened fields of the load on the session state, so
  the save has the state to compare with.
- Tests 1 to 7a run on these functions with no Redis.

**The search path.** One registry and three operations serve each search
method:

- The registry maps each searchable field to its type and its index. The
  store adds `s:sub` and `s:seen` to it for the built-in index, and each
  declared attribute for the custom index.
- `_search(filter, limit, offset)`, `_count(filter)`, and
  `_revoke_matching(filter)` build the query from one filter, add the live
  filter, and read the reply.
- Each public method is a short call to one of the three:
  `list_for_subject` and `find` to `_search`; `count_for_subject` and
  `count` to `_count`; `revoke_all` and `revoke_idle` to
  `_revoke_matching`.
- The check for the Query Engine, the check for a timeout, and the reading
  of the two reply forms are in these three operations only.

## 8. Choices this design makes

The research left these open. Each row is a proposal.

| Point | Choice |
|---|---|
| Names of system fields | Prefix `s:`; custom attributes take prefix `a:` |
| A key with `.` or `[` | Percent-escape in the path |
| `None` in a searchable attribute | Refuse on save |
| Custom attributes under encryption | One layout. Each field value is encrypted; field names are plaintext. No custom search. |
| An attribute with a TTL and an index | Refuse at the call that gives the TTL |
| GEO stored form | Plain `lon,lat` |
| A changed declaration | The custom index has a hash of its schema in its name. A new declaration creates a new index and drops the old ones. The built-in index is never dropped. |
| No Query Engine | Named error at the call |
| Timeout | Named error; warning at startup if the policy is not `fail` |
| Cost of last access | Write on each load; accept the re-index |
| The `coder` option | Not used for sessions; each value is JSON text |
| Live sessions at upgrade | Not migrated; each user signs in again |
| Change one attribute in all sessions of a user | Not in the first version. The application calls `revoke_all`, and the user signs in again. |
| Session size (research row 8) | The application computes it; documentation recipe only |

## 9. Functional tests

### 9.1 How the tests run

Each test has a level:

| Level | Runs on | In CI |
|---|---|---|
| U: unit | `fakeredis`, or a stub of the client | The unit job, on each system and Python version |
| I: integration | A real Redis | The integration job, on Redis 8.8 and on 7.4 |
| S: search | A real Redis with the Query Engine | The integration job on Redis 8.8. The test skips itself when `COMMAND INFO FT.SEARCH` shows no Query Engine. |

Rules for each test:

- **The lowest level that can fail.** `fakeredis` has `HEXPIRE`, `HTTL`, and
  `HSETEX ... FXX`, so the storage and clock tests are unit tests. It has no
  `FT.*` command, so a test that runs a search is level S.
- **Search code has unit cover too.** The query builder and the reply
  parser are functions with no Redis in them. Unit tests give them input and
  check the output, so the unit job covers the search code on each system
  and Python version (tests 38 to 40).
- **No test waits for a TTL.** A test checks the TTL that the store set, with
  `TTL` or `HTTL`. To see what happens after the expiry, it forces the
  expiry: `PEXPIRE <key> 1` or `HPEXPIRE <key> 1 FIELDS ...`. The helpers
  `expire_now` and `aexpire_now` do this. They are not in `tests/conftest.py`
  on this branch; this work adds them.
- **No test depends on the clock for `s:seen`.** A test that needs an old
  last access time writes `s:seen` to the hash itself.
- **A missing capability is injected.** A test for a server without the Query
  Engine or without `HSETEX` passes
  `_StoreCapabilities(supports_search=False)` or
  `_StoreCapabilities(supports_hsetex=False)` to the store, as
  `tests/integration/test_session_integration.py` does today. It then runs
  on each Redis version.
- **Each test must be able to fail.** A test does not check a property that
  holds when the code under test is absent, and it does not check Redis
  alone.

### 9.2 Storage and round trip

| # | Level | Test |
|---|---|---|
| 1 | U | A session with a string, an integer, a float, a boolean, `None`, an empty list, an empty dict, a nested dict, and a list of dicts loads equal to what the application wrote. |
| 2 | U | The values `7` and `"7"` in two attributes load with their types. |
| 3 | U | A key that contains `.`, `[`, `]`, or `%` loads unchanged. A dict key that is not a string loads as a string. |
| 4 | U | Each attribute is in its own `a:` field, with the field name of section 2.1. No field `d` exists. |
| 5 | U | A custom attribute named `s:sub` or `created` does not change a system field. |
| 6 | U | A save that changes one attribute writes that field only. A save that shortens a list removes the fields of the removed items. |
| 7 | U | With an `Encryptor`, the hash has the same fields as without one. No `a:` field holds its plaintext value, and the session loads equal. |
| 7a | U | With an `Encryptor`, a save that changes one attribute writes that field only, and a save with no change writes no `a:` field. |
| 7b | U | With an `Encryptor`, tests 12 to 12g pass. |
| 41 | U | A session that the application wrote from `Model.model_dump(mode="json")` loads equal, and `Model.model_validate` accepts the loaded value. |
| 42 | U | An anonymous session has no `s:sub`. It loads and saves. |
| 43 | U | A hash in the present layout (field `d`, no `s:created`) loads as no session, and the load deletes the key. |

### 9.3 Clocks

| # | Level | Test |
|---|---|---|
| 8 | U | After a read-only request, `TTL` on the key is the idle timeout. After `expire_now` on the key, a load returns no session. |
| 9 | U | After a create, `HTTL` on `s:created` is the absolute timeout. Requests and saves do not raise it. After `expire_now` on `s:created`, a load returns no session and deletes the key. |
| 10 | U | A save after `expire_now` on `s:created` does not create a live session. After many saves, `s:created` still has a TTL. |
| 11 | U | A rotation starts a new absolute clock: the new session has a new `s:created`, and its `HTTL` is the absolute timeout. The old key does not exist after the rotation. |
| 11a | U | A rotation that fails after it deleted the old key leaves no session. Neither session ID loads. |
| 12 | U | After a save of an attribute written with `ttl=N`, `HTTL` on each field of the attribute is N. Requests, and saves of other attributes, do not raise it. After `expire_now` on those fields, the attribute is absent and the rest of the session loads. |
| 12a | U | A new write of the attribute with a different value and without `ttl` removes the TTL: `HTTL` on the field shows no TTL. |
| 12b | U | A new write of the attribute with `ttl=M` sets `HTTL` to M. |
| 12c | U | A change to one nested value of an attribute that has a TTL, without `ttl`, removes the TTL from that field. `HTTL` on the other fields of the attribute does not change. |
| 12d | U | A rotation keeps the time left on an attribute TTL, to the second. |
| 12e | U | `persist` removes the TTL and keeps the value: `HTTL` on each field shows no TTL, and the request sent no command that reads or writes the value. |
| 12f | U | A write of a value equal to the stored value, without `ttl`, keeps the TTL. |
| 12g | U | `persist` for an attribute that is absent or has no TTL changes nothing. |
| 44 | U | A write with `ttl=N` of a nested attribute, where one leaf changed, sets `HTTL` to N on each field of the attribute, changed or not. |
| 13 | U and I | A request with a cookie that names no session leaves no key in Redis and sends no command that writes. Run with each value of `supports_search` and `supports_hsetex`. |
| 13a | U | With `supports_search=False`, or with `supports_hsetex=False`, a read-only request does not change `s:seen`. A save does. |
| 45 | U | With `session_refresh_on_load=False`: a load does not change `TTL` on the key. A request that used the session sets it to the idle timeout. |
| 46 | I | The commands of section 3 give the same fields and the same TTLs on a real Redis as on `fakeredis`, for create, load, save, and rotate. Run with each value of `supports_hsetex`. |

### 9.4 Built-in search

| # | Level | Test |
|---|---|---|
| 14 | S | `list_for_subject` returns each live session of the subject, newest access first, with the descriptor. It returns no session of another subject and no anonymous session. For a subject with more than 1,000 sessions it returns the 1,000 with the newest access, and `count_for_subject` returns the full number. |
| 15 | S | `count_for_subject` matches the number of live sessions. |
| 16 | S | `revoke_all` ends each session of the subject, more than 1,000 included, and no session of another subject. It returns the number ended. |
| 17 | U | `revoke_id` ends the session for the right subject and refuses for another subject. It runs no search. |
| 18 | S | A subject with a comma, an uppercase letter, a quote, a backslash, or a character outside ASCII is found, and matches no other subject. |
| 20 | S | After `expire_now` on `s:created`, with the key still present, the session is in no list and in no count. |
| 22 | S | With `s:seen` written by the test: `list_active(within=N)` returns the sessions with `s:seen` in the last N seconds and no other session. With more matches than `limit`, it returns `limit` sessions, and `offset` gives the next page. |
| 22a | S | On a server with the Query Engine and `HSETEX`, a read-only request sets `s:seen` to the present time. |
| 23 | S | With `s:seen` written by the test: `revoke_idle(older_than=N)` ends the sessions with `s:seen` older than N seconds and no other session. It returns the number ended. |
| 24 | S | With an `Encryptor`, tests 14 to 16 pass. |

### 9.5 Custom search

| # | Level | Test |
|---|---|---|
| 25 | S | TAG: `find(..., equals=v)` returns the sessions with that exact value, for a boolean and for a string with a comma, a quote, a backslash, and a character outside ASCII. It matches no other value. |
| 26 | S | NUMERIC: `find(..., between=(lo, hi))` returns the sessions in the range, with one limit and with both. |
| 27 | S | TEXT: `find(..., text="word")` returns the sessions whose value has the word. `text="pet*"` returns the sessions with a word that starts with `pet`. `text="a\|b"` returns the sessions with either word. |
| 27a | S | TEXT with a syntax error, such as `text="("`, raises `ValueError`. |
| 27b | U | `between` or `near` with `nan` or `inf` raises `ValueError`. |
| 28 | S | GEO: `find(..., near=(lon, lat, r))` returns the sessions inside the radius and none outside. The attribute loads as `[lon, lat]`. |
| 29 | S | A nested path such as `cart.total` is searchable. With `cart.total` and `cart_total` both declared, a search on one does not match the other. |
| 30 | S | A session without the declared attribute is still found by subject. |
| 30a | S | A session with a value that the engine cannot index (GEO `[10, 89]`, NUMERIC `NaN`) is still in `list_for_subject` and `count_for_subject`, and `revoke_all` ends it. |
| 31 | S | `count` matches `find`. `limit` and `offset` page through a result with no repeat and no gap. |

### 9.6 Errors and capability

| # | Level | Test |
|---|---|---|
| 32 | U | Each type rule of section 4. A save raises `SessionAttributeTypeError`, and the stored session does not change, for: a string or a `bool` in NUMERIC; a number in TAG; a value that is not a string in TEXT; a GEO value that is not two numbers; `None` in each of the four types. A `bool` in TAG and an absent attribute raise nothing. |
| 33 | U | Setup raises `SessionConfigurationError` for `searchable` with an `Encryptor`. |
| 33a | U | `Session.set(..., ttl=N)` raises `ValueError` at the call for a `ttl` that is not a positive integer. |
| 33b | U | A save that holds a TTL for a searchable attribute, or for an attribute that holds a searchable path, raises `SessionConfigurationError`, and the stored session does not change. |
| 33c | U | After `set(name, v, ttl=N)`, each of `[...] =`, `set` with no `ttl`, `pop` then a new write, `update`, and `\|=` for that name gives a save with no TTL for it. After `persist(name)`, a later `set(name, v, ttl=N)` gives a save with the TTL. |
| 34 | U | `find` with a name that is not declared, with two filters, or with a filter of the wrong type raises `ValueError`. |
| 34a | U | `find` and `list_active` raise `ValueError` for `limit` below 1, for `offset` below 0, and for `offset + limit` above 10,000. |
| 35 | U and I | With `supports_search=False`: load, save, rotate, `revoke`, and `revoke_id` work. Each of `list_for_subject`, `count_for_subject`, `revoke_all`, `list_active`, `revoke_idle`, `find`, and `count` raises `SessionSearchUnavailableError` and sends no command. |
| 35a | U | With `supports_search=False`, the store logs the warning once, for many stores and many requests. |
| 36 | U | With a stub of the client: a reply that is a server timeout error raises `SessionSearchTimeoutError`. A RESP3 reply with a timeout entry in `warning` raises `SessionSearchTimeoutError`. In each case the method returns no session, and `revoke_all` ends none. |
| 36a | S, `slow` | On a real Redis: the test sets `search-on-timeout fail` and `search-timeout 1`, loads 50,000 sessions of one subject, and calls `list_for_subject`. The call raises `SessionSearchTimeoutError`. The test uses a list, because a count does not time out. It restores the two settings when it ends. |
| 36b | U | At startup, with a stub that reports a policy other than `fail`, the store logs the warning once. With `fail`, it logs none. |
| 37 | S | Each index is created when it is missing. A second start with the same declaration leaves both indexes as they are and sends no `FT.DROPINDEX`. The name of the custom index holds the hash of its schema, and two stores with the same declaration use one name. With no `searchable`, the custom index does not exist. A start with a changed declaration creates a custom index under a new name, logs the warning, and drops the old one, and, when `FT.INFO` shows that the indexing ended, a search on the new attribute finds sessions written before the change. |
| 37a | S | A start with a changed declaration does not drop the built-in index. `revoke_all` straight after that start ends each session of the subject. |
| 37b | U | With a stub: `FT.CREATE` that fails with "Index already exists" counts as success, and `FT.DROPINDEX` that fails with "index not found" counts as success. |
| 37c | S | Two ensure steps that run at the same time, with `asyncio.gather`, both succeed and leave one index of each kind. The test repeats this 20 times. |

### 9.7 Search code with no Redis

| # | Level | Test |
|---|---|---|
| 38 | U | The query builder gives the query strings of section 4.1 for each filter type, with the field name escaped and the live filter added. |
| 39 | U | The query builder escapes a TAG value with a quote, a backslash, a comma, braces, a pipe, and a character outside ASCII as section 4.1 states. |
| 40 | U | The reply parser gives the same `SessionInfo` list from a RESP2 reply and from a RESP3 reply of the same search, and the same number from the two forms of a count. |
