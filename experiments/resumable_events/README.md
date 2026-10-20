# Resumable events implementation spike

Executable experiments for the storage and shared relay requirements in the
[design proposal](../../docs/specs/resumable-events.md). This code is outside
`src/redis_fastapi`, is not exported by the SDK, and is not a supported backend.
The proposed FastAPI API remains unchanged.

## Result

On 2026-10-20, the same 18 scenarios passed against each local topology:

| Deployment | Result | Configuration |
| --- | --- | --- |
| Redis OSS standalone 8.10.2 | 18 passed | AOF enabled, `appendfsync always` |
| Redis OSS Cluster 8.10.2 | 18 passed | Three primaries, no replicas; bucket keys span multiple primaries |
| Redis Enterprise sharded endpoint | Unverified: 18 skipped | No endpoint supplied; no compatibility claim |

Host: macOS arm64, Python 3.14.5, redis-py 8.0.1. Redis was built from the
official `8.10.2` Git tag, commit
`498ecd0d6d007db11ddb3aea9428552598a78622`. These are behavioral experiments,
not throughput, latency, failover, or release qualification results.

The connection-count scenario measures named blocked clients using `CLIENT LIST`
on every local node. Increasing from four subscribers on four streams to **84
subscribers on eight streams keeps four blocking XREAD connections**, with four
configured buckets. Publishing succeeds while those readers block. Shutting down
the relay leaves zero blocked readers.

That establishes the blocking-connection mechanism for one process and this
workload. It does not establish a four-socket bound for the whole application:
ordinary commands, replay, topology discovery, and idle pool connections are
additional. redis-py Cluster pool limits apply per node.

## What the experiments exercise

- Concurrent create retries preserve the original absolute expiry.
- Concurrent native IDMP retries append one event.
- Concurrent append/finish and repeated finish preserve a final entry as the
  last event; reusing an ordinary append identity for finish fails.
- Disconnect before `EXEC` leaves the queued write unapplied. Dropping a
  committed finish reply and retrying returns the original final cursor.
- An injected transaction command error demonstrates that another command in
  that transaction still executes; `EXEC` does not provide rollback.
- Replay starts strictly after the cursor, including across a fresh gateway
  registry. Events published between registration and replay are delivered
  without gaps or duplicates in the tested interleaving.
- Exact trimming advances the history floor. An older cursor fails explicitly;
  a cursor at the floor remains usable, even when all open history was removed.
- Expiry is distinguished from payload loss before the deadline. An expired
  stream does not disconnect a healthy stream in the same bucket.
- Subscriber queues enforce event-count and byte limits. Overflow releases the
  queue and requires replay from the client's cursor.
- Rotating stream order delivers a quiet stream while a hot stream still has
  backlog, even with an aggregate one-entry XREAD budget.
- Same-bucket keys work with Cluster transactions and multi-stream XREAD.
  Mismatched bucket layouts are rejected at initialization.

## Design consequences

**Use the final entry as the authoritative seal.** The candidate in `store.py`
uses `WATCH` on the stream and metadata, checks the last entry, then executes one
`XADD IDMP` carrying the final marker. A separate metadata seal could succeed
when its accompanying append fails. Keeping finality in the append avoids that
specific split outcome. Finish retries inspect the retained final entry; trim
refuses to remove it. This is a proposed refinement to the design's metadata
seal, not a claim that the entire lifecycle protocol is qualified.

**Keep relay and ordinary command pools separate.** There is one blocking task
per bucket. Registration captures a replay boundary under the dispatch lock;
subsequent live events queue while that subscriber replays. Slow consumers fail
individually. Stream-local expiry and retention failures affect only that stream.

**Keep the Redis version gate.** The experiment uses Redis 8.10 XREAD aggregate
caps. redis-py 8.0.1 accepts native XADD IDMP, but its typed `xread()` method does
not expose `MAXCOUNT` or `MAXSIZE`; the relay uses `execute_command`.
`MAXSIZE` may return one oversized entry, so it is not sufficient without the
separate event-size limit.

## Run it

From the repository root, with `redis-server` and `redis-cli` from Redis 8.10.2
in the same directory:

```bash
uv sync --all-groups --frozen
uv run --frozen pytest \
  -c experiments/resumable_events/pytest.ini \
  experiments/resumable_events \
  --redis-server /absolute/path/to/redis-8.10.2/src/redis-server \
  --topology standalone --topology cluster --topology enterprise \
  --junitxml=/tmp/resumable-events-spike.xml
```

The suite creates disposable local processes on dynamically assigned loopback
ports and terminates them afterward. Each test uses a unique namespace and
deletes only its recorded keys. It never calls `FLUSHDB` or `FLUSHALL`.
Readiness and asynchronous assertions use bounded observed-state checks.

For a separately approved Enterprise environment, supply the database endpoint
through `STREAM_SPIKE_ENTERPRISE_URL` and set
`STREAM_SPIKE_ALLOW_EXTERNAL=1`. Credentials stay in the environment.
The endpoint must be a sharded database with the required commands. Connection
introspection and direct-node socket fault injection still require separate
operator-level verification; those scenarios explicitly skip through the
Enterprise endpoint. No external service is provisioned by this suite.

## Remaining implementation gates

This spike is deliberately incomplete:

- No FastAPI builder/dependency, SSE framing, Last-Event-ID handling, HTTP error
  mapping, application authorization example, or lifespan integration.
- No Enterprise sharded verification, replica promotion, resharding, Redis
  process crash/restart, network partition, or sustained load qualification.
  The socket test covers disconnects around one transaction, not server crashes.
- Creation and trimming still need injected failure coverage at every command
  boundary, including OOM and partial metadata updates. Checking `EXEC` results
  alone does not prove that later callers cannot accept partial initialization.
- The namespace manifest expires after one hour for experiment cleanup.
  Production placement configuration must remain valid for the namespace's
  lifetime and survive process restarts and migration.
- No automatic reconnect/backoff after shared Redis failures, global subscriber
  admission limit, bounded stream-membership batches, metrics, or full input
  validation. Registration currently holds a dispatch lock during a Redis read.
- Replay uses count-bounded pages and the event-size limit; the implementation
  still needs explicit aggregate replay byte budgeting and tuning.
- No 24-hour or 10,000-ID boundary test for native IDMP retention. Retry identity
  deduplication is tested inside those bounds, not indefinitely.
- Only Python 3.14 was exercised. The eventual SDK feature must run through the
  supported Python/FastAPI compatibility and installed-package CI matrices.

The evidence supports continuing with the generic SDK API and bounded relay
approach. It does not justify advertising the full resumable-event feature as
implemented or ready to merge.
