# Resumable streams: review panel results

Date: 2026-10-24; revised 2026-10-26.
Scope: branch `feature/resumable-streams` against `origin/main` (merge base
`da33a94`).
Method: ten parallel read-only reviewers (security, API stewardship,
correctness, performance, test design, platform fitness, reuse, complexity,
problem fit, principal developer), then one independent skeptic per blocking
candidate.

This revision keeps only the findings that are still open. They were
checked against commit `83a0359` plus the uncommitted changes after it.
Fixed findings were removed, and the line numbers were updated to match the
current code.

## Change brief

Implement the first release of resumable streams from
[`resumable-streams-design.md`](resumable-streams-design.md): the SDK feature,
a full unit and integration suite, a guide page, an architecture diagram, a
README section and API docs. Then: correct the design's deviations, move the
demo onto the real feature, make the 7.4 coverage run an expectation, and fix
the Mermaid diagram.

Constraints: FastAPI apps are the only writers and readers; the key prefix
follows the SDK's setting; no key versioning; no mention of phases or the
design file; keep the design simple.

Non-goals: WebSocket, durable queues, application-supplied idempotency keys,
workers outside FastAPI, a sync writer, relay buckets.

## Platform profile

| Platform | Evidence and baseline |
|---|---|
| FastAPI / Starlette | `fastapi>=0.115.0`. Starlette 1.3.1 installed; the 0.115 floor pins Starlette 0.37–0.38 |
| Redis | 8.6.2+ for streams, 7.4+ otherwise. CI runs 8.8 and 7.4 |
| redis-py | `>=6.2.0`; 8.0.1 installed |
| Others | Pydantic Settings v2, browser SSE / `EventSource`, Python 3.10–3.14, MkDocs Material with Mermaid, nox, coverage.py |

## Summary

No blocker is open. The most important open items, in order:

1. **Shutdown under uvicorn.** uvicorn runs the lifespan shutdown only after
   open responses end, so `aclose()` cannot close live readers. A deploy can
   wait up to `stream_max_connection_seconds` for SSE responses. The guide
   promises a prompt close, and gives two different outcomes for a deploy.
   No test runs a real server.
2. **The Starlette floor.** On FastAPI 0.115 (Starlette 0.37–0.38) the
   slow-consumer exit arrives inside an `ExceptionGroup` and escapes as an
   unhandled ASGI error. CI never installs the floors.
3. **Untested behaviour.** `exclusive=False`, the read-only reader fallback,
   the session-revoked reconnect, a reconnect storm, a foreign-scope
   `attach`, the slow-consumer counter and all stream telemetry have no test.
   Several tests still prove behaviour through private internals.
4. **The storage/HTTP split.** `streams.py` runs on the backend's private
   methods, and `StreamBackend.sse` imports `streams` lazily.
5. **Reader capacity and data at rest.** One client can take every reader
   slot. Payloads are stored as plaintext, and the docs do not say so.
6. **Stale or overstated docs.** The examples README still mentions a stub,
   the demo README overstates what survives a redeploy, the sessions guide
   lacks the changes design §11 lists, and the guide does not say what to do
   after a `StreamStoreError`.

Smaller items: three copies of the support probe, an unread
`_PoolState.stream_capabilities`, minor copied code, a demo lens that
rebuilds the key layout, `XINFO STREAM` on every connect, a missing
replay-bytes counter, and content outside the brief.

## Decision-relevant

### Correctness and robustness

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `lifespan.py:238-246`; `stream_backend.py:1503, 1633-1663`; `docs/guide/streams.md:122, 405-407` | Under uvicorn, readers do not close at shutdown as the docs promise, and a deploy can wait on them. | uvicorn waits for open responses before it runs the lifespan shutdown. Readers stop on `_closing`, which only `aclose()` sets, so they end on their own, up to `stream_max_connection_seconds` (300 s). Producers can die without a final entry if the process is killed first. The guide says `spawn()` cancels producers at shutdown (reason `cancelled`), and also that readers get `producer_lost` on a deploy; it does not say when each applies. No test runs a real server. Raised by three reviewers. | Design §7.2 and test 22 |
| `streams.py:176-212` | On the FastAPI 0.115 floor (Starlette 0.37–0.38), the slow-consumer exit escapes as an unhandled ASGI error. | Those versions run `stream_response` in a plain anyio task group, so `_SlowConsumer` arrives wrapped in an `ExceptionGroup`, which `except _SlowConsumer` does not catch. uvicorn reports `spec_version` 2.3, so this is the production path. The slot is still released. CI never installs the floors. | Platform baseline |

### API and structure

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `stream_backend.py:1359-1378`; `streams.py:195, 212, 253, 268` | The storage/HTTP split is not a real boundary. | `StreamBackend.sse` lazily imports `streams`, a hidden cycle. `streams.py` runs on `_prepare`, `_pages`, `_reserve_reader` and `_release_reader`. Raised by two reviewers. | `exceptions.py` docstring on layering |
| `noxfile.py:113-138`; `test_streams_integration.py:46-60` | The support probe exists in three copies, and the noxfile comment says it runs the SDK's own check, which it does not. | The error handling differs: a `ResponseError` after a good ping fails the nox session. Raised by four reviewers. | `probe_streams_support` |
| `deps.py:84, 165`; `lifespan.py:180-183` | `_PoolState.stream_capabilities` is written but never read. | The backend is a per-process singleton, so the shared dataclass and `_start()` earn nothing. | Design §11; simplicity |
| `stream_backend.py:85-89, 232-236, 626-636`; `telemetry.py:425-427` | Minor copied or unused code. | A copy of `STORE_ERRORS`; `_ms`, a near-copy of the session `_seconds` that silently raises any duration under 1 ms to 1 ms; a one-use `build` closure in `_write`; an `attributes` parameter on `stream_span` that no caller passes. | Existing `session_backend` helpers |
| `examples/incident_triage_app/lens.py:56-101`; `planner.py:33` | The demo rebuilds SDK internals. | The lens rebuilds the key layout and makes about 9 Redis round trips per 2-second poll; streams have no key accessor, while sessions expose `session_key()`. `planner.py` keeps its own copy of the name pattern, which the 422 for `StreamInvalidName` makes unnecessary. | `SessionStore.session_key` |

### Performance

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `stream_backend.py:1397` | `XINFO STREAM` returns the full first and last entries on every connect. | Up to about 128 KB per reconnect, of which a few fields are used. | Reconnect storms |

### Security

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `stream_backend.py:1622-1627`; demo `main.py:48` | One client can take every reader slot. | The cap is per process with no per-client share. In the public demo the cap is 8 and sign-in needs no password. | Connection-cap design |
| `stream_backend.py:893-901` | Stream payloads sit in Redis as plaintext, and the docs say nothing about it. | Sessions offer an `Encryptor`. | Session precedent (evidence gap) |

### Tests

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `tests/**` | Several behaviours in design §13 still have no test. | The read-only reader fallback (#3, `NoPermissionError` to `producer_lost`); a foreign-scope `attach` (#5); a reconnect storm (#10); the session-revoked reconnect (#13); the slow-consumer counter (#23); `exclusive=False`. The stream telemetry is untested, unlike the `test_otel_*` files for the other features. | Brief ("full suite"); design §13 |
| `test_streams_integration.py:935, 969, 1129, 1186, 1216, 1241, 1257, 1305` | Some behaviours are proven only through private internals. | The gap event, the read retry, the reader count and the 204 cases go through `_prepare`, `_pages`, `_event` and `_readers`. | Observable behaviour |
| `tests/unit/test_streams.py:1-8` | The unit tests use mocks instead of fakeredis. | Fakeredis lacks the commands, which the module docstring gives as the reason, but SKILL.md says unit tests use fakeredis. | SKILL.md testing conventions |
| `test_streams_integration.py:78, 530`; `test_streams.py:74` | Some timing is fragile, and a fixture is copied. | A bare `time.sleep(0.15)` before shutdown with no sync point. The `settings_env` fixture is copied in both test files. | `conftest.about()` |

### Docs and scope

| Where | Claim | Evidence | Anchor |
|---|---|---|---|
| `examples/README.md:29` | One line still says the demo's streams "come from a stub". | A doc line to fix, not a blocker. | Demo migration |
| `examples/incident_triage_app/README.md:11` | "A redeploy never loses the answer" overstates the feature. | True only for finished answers. A doc fix, not a blocker. | Design §2 non-goal |
| `docs/guide/streams.md:134-140` | The guide does not say what to do after an append raises `StreamStoreError`. | The writer keeps its sequence number, so retrying the same event is safe, and sending a different one drops it. Say to retry the same event or stop the run. | Append retry trade-off |
| `docs/guide/sessions.md` | The sessions-guide changes listed in design §11 are missing. | Neither the stream routes as an access example nor `session_expiry()` appears there. | Design §11 |
| `telemetry.py` | The replay-bytes counter listed in §11 is missing. | No byte counter in either stream module. | Design §11 |
| `specs/**`; `.gitignore:18`; `noxfile.py:72-82`; `examples/incident_triage_app/deploy/**` | The change carries content outside the requests. | Several thousand lines of specs, including a WebSocket spec for a non-goal; `/.mcp.json` in `.gitignore`; a bandit reflow mixed into the coverage change; new deploy tooling. | Brief scope |
| `examples/incident_triage_app/README.md:13`; `specs/resumable-streams-demo.md:480` | It is unclear whether the "no phases or design file" rule covers the examples. | The demo README links the demo spec, and that spec says "phase 1". | Brief constraint |

## Decisions needed

Most open items are plain fixes. The ones below need a decision first,
because they change the public API, a version floor, the design or the
PR's scope. A recommended option is marked.

The items that need no decision:

- the test gaps;
- the probe copies (import `probe_streams_support`);
- the unread `stream_capabilities`;
- the minor copied code;
- the fragile sleep and the copied fixture;
- the stub line, the redeploy claim and the `StreamStoreError` advice;
- the sessions-guide changes.

### D1. Readers at shutdown under uvicorn

Question: how should open SSE responses end on a deploy, when the lifespan
shutdown runs only after they end?

- **A (recommended):** Document it. Tell users to set uvicorn's
  `--timeout-graceful-shutdown`, and give the outcome for each path: a
  graceful stop within the grace ends with `cancelled`, a killed process
  with `producer_lost`. Optionally lower the `stream_max_connection_seconds`
  default.
- **B:** Detect the server's shutdown before the lifespan runs, for example
  with an SDK signal handler. Readers close at once. This is not portable
  across servers, and it can conflict with uvicorn's own handlers.
- **C:** Accept that readers end at `stream_max_connection_seconds` and say
  so. This needs the least work, but a deploy can wait up to 300 s.

Also: which outcome should the guide promise for a deploy, `cancelled` or
`producer_lost`?

### D2. The FastAPI / Starlette floor

Question: support Starlette 0.37–0.38, where the slow-consumer exit escapes
inside an `ExceptionGroup`?

- **A (recommended):** Raise the floor to the first FastAPI that requires a
  Starlette whose `StreamingResponse` collapses the group. Starlette 1.3.1
  uses `create_collapsing_task_group()`; the first such version still has
  to be found in the changelog.
- **B:** Keep the floor and unwrap the group in `_SSEResponse`. Python 3.10
  then needs the `exceptiongroup` backport.
- **C:** Either A or B, plus a CI job that installs the lowest allowed
  versions.

Also: should the floor change apply to the whole SDK, or should the stream
feature refuse to start on older Starlette?

### D3. The storage/HTTP split

Question: should `streams.py` stay a separate layer?

- **A (recommended):** Make the split real. Give the backend a small
  internal interface for the HTTP side (prepare, pages, reserve and release
  a reader slot), and keep `StreamBackend.sse` as a thin forwarder.
- **B:** Merge the HTTP code into `stream_backend.py` and drop the claim
  that they are separate layers.
- **C:** Remove `StreamBackend.sse` and expose only `streams.sse(backend,
  ...)`. This changes the public API.

### D4. A public key accessor

Question: should streams expose their Redis key names, as sessions do with
`session_key()`?

- **A:** Add `StreamBackend.keys(stream_id, scope=...)`, and have the demo
  lens use it. This makes the key layout public, which goes against the
  guide's "the key layout may change".
- **B (recommended):** Keep the layout private. Mark the lens as a demo that
  copies an internal layout on purpose, and cut its round trips with one
  pipeline.
- **C:** Remove the key view from the lens.

### D5. `XINFO STREAM` on every connect

Question: is up to about 128 KB per reconnect acceptable?

- **A (recommended):** Accept it and document the cost. Only `XINFO STREAM`
  gives `entries-added` and `max-deleted-entry-id`, which the gap check
  needs.
- **B:** Lower the `stream_max_event_bytes` default, which bounds the first
  and last entries the reply carries.
- **C:** Redesign the gap check so it needs no `XINFO STREAM`.

### D6. One client taking every reader slot

Question: should the SDK limit readers per client?

- **A (recommended):** No new mechanism. Document that the cap is per
  process, and show the SDK's rate limiter on the SSE route. Give the
  public demo a rate limit too.
- **B:** Add a per-key reader cap, with a key the application supplies,
  such as the scope or the user.
- **C:** Leave it as it is.

### D7. Plaintext payloads

Question: should stream payloads support encryption, as sessions do?

- **A (recommended):** For this release, add a note to the guide: payloads
  are stored as plaintext, so encrypt them in the coder if needed.
- **B:** Accept an `Encryptor`, as sessions do.

### D8. The replay-bytes counter

Question: design §11 lists it, and the code lacks it.

- **A:** Add it to `telemetry.py`.
- **B (recommended if no one needs it now):** Remove it from the design.

### D9. The unit-test convention

Question: SKILL.md says unit tests use fakeredis, but fakeredis lacks
`XADD ... IDMP`, `DELEX` and `SET ... IFEQ`.

- **A (recommended):** Amend SKILL.md to allow mocks when fakeredis lacks
  the commands, and keep Redis behaviour in the integration suite.
- **B:** Keep the rule, and move every mock-based test to the integration
  suite.

Also: should tests that go through private internals get public seams, or
stay as they are?

### D10. The PR's scope

Question: what should the PR carry beyond the brief?

- The specs, including the WebSocket spec for a non-goal: keep them in the
  PR, move them to a separate PR, or leave them out of the repository.
- `/.mcp.json` in `.gitignore`: keep it, or move it to a separate change.
- The bandit reflow in `noxfile.py:72-82`: keep it, or split it out.
- The deploy tooling in `examples/incident_triage_app/deploy/`: keep it,
  or move it to a separate PR.

Recommendation: split out everything that is not the feature, its tests,
its docs and the demo.

### D11. The "no phases or design file" rule

Question: does the rule cover the examples and the specs?

- **A (recommended):** Yes for the examples. Drop the spec link from the
  demo README, and replace "phase 1" in the demo spec.
- **B:** The rule covers only the SDK and its docs. The examples may link
  the specs.
