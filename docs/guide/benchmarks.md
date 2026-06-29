# Benchmarks

## Summary

fastapi-redis-sdk is benchmarked against popular caching libraries
on identical workloads. All numbers are **median latency in microseconds (µs)**
- lower is better.

**Caching** - compared with fastapi-cache2 and cashews:

- On **small payloads** (~550 B), all three libraries perform within 10% of each
  other (~650–700 µs per HIT). There is no clear winner.
- On **large payloads** (~50 KB), **fastapi-redis-sdk is 5.6× faster** than
  fastapi-cache2 on cache HITs (1,165 µs vs 6,530 µs), thanks to its
  raw-bytes approach that skips re-serialization entirely.
- fastapi-cache2 is **2.8× faster** on write-through because its decorator
  writes the return value directly, avoiding fastapi-redis-sdk's response-capture
  middleware overhead.
- cashews is **5.5× slower** than fastapi-redis-sdk on eviction-group eviction due to
  Python-side `SCAN` round-trips vs fastapi-redis-sdk's server-side Lua script.

**Environment:** Python 3.14, Redis 8, macOS Apple Silicon, localhost.
Full methodology is described at the bottom of this page.

---

## Caching

Compared libraries: **fastapi-redis-sdk** (dev), **fastapi-cache2** 0.2.2,
**cashews** 7.5.0.

### Small payload (~550 bytes)

| Scenario                    | fastapi-redis-sdk | fastapi-cache2 |      cashews |
|-----------------------------|------------------:|---------------:|-------------:|
| Cache HIT                   |      **643 µs**   |        662 µs  |     696 µs   |
| Cache MISS                  |        **994 µs** |       1,008 µs |     1,188 µs |
| ETag / 304                  |            703 µs |     **672 µs** |        *n/a* |
| Write-through               |            700 µs |     **254 µs** |       619 µs |
| Single-key eviction         |          1,867 µs |       1,367 µs | **1,310 µs** |
| Group eviction (1 000 keys) |          2,250 µs |     **728 µs** |    12,319 µs |

### Large payload (~50 KB)

| Scenario   | fastapi-redis-sdk | fastapi-cache2 |      cashews |
|------------|------------------:|---------------:|-------------:|
| Cache HIT  |    **1,165 µs**   |     6,530 µs   |    1,909 µs  |
| Cache MISS |          3,982 µs |       7,129 µs | **2,802 µs** |

### Standout results

#### 🏆 Large-payload HIT - fastapi-redis-sdk is 5.6× faster than fastapi-cache2

```
fastapi-redis-sdk       1,165 µs
cashews                 1,909 µs   (1.6× slower)
fastapi-cache2          6,530 µs   (5.6× slower)
```

fastapi-cache2's `JsonCoder.decode` runs `json.loads()` with a custom
`object_hook` that inspects every dict for `_spec_type` markers. On 200
product objects that hook fires 1,000+ times. cashews uses pickle - faster
than JSON but still slower than raw bytes.

fastapi-redis-sdk stores the JSON body as raw bytes and returns it **without
re-parsing or re-serializing** on HIT, making coder overhead effectively
zero regardless of payload size.

#### ⚠️ Write-through - fastapi-cache2 is 2.8× faster

```
fastapi-cache2          254 µs
cashews                 619 µs   (2.4× slower)
fastapi-redis-sdk       700 µs   (2.8× slower)
```

fastapi-cache2's `@cache` decorator caches the return value directly - one
function call, one Redis write. fastapi-redis-sdk's `cache_put()` goes through
the full DI pipeline and response-capture middleware (buffer the ASGI
response, serialize, write to Redis).

#### ⚠️ Group eviction - cashews is 5.5× slower

```
fastapi-cache2           728 µs
fastapi-redis-sdk      2,250 µs   (3.1× slower)
cashews               12,319 µs   (5.5× slower than fastapi-redis-sdk)
```

fastapi-cache2 uses a Lua script with `KEYS` (fast but
[discouraged in production](https://redis.io/commands/keys/)). fastapi-redis-sdk
uses a Lua script with `SCAN` + `UNLINK` (production-safe, slightly slower).
cashews uses Python-side `scan_iter` + `delete_match` - multiple network
round-trips per SCAN batch.

### Default coders

| Library            | Default coder | Approach                                                          |
|--------------------|---------------|-------------------------------------------------------------------|
| fastapi-redis-sdk  | raw bytes     | Stores JSON body as-is; no decode on HIT                          |
| fastapi-cache2     | `JsonCoder`   | `json.dumps` + `jsonable_encoder` / `json.loads(object_hook=...)` |
| cashews            | pickle        | `pickle.dumps()` / `pickle.loads()`                               |

---

## What happens when latency increases

The localhost benchmarks above are dominated by client-side Python overhead.
In production, Redis is typically on a separate host with **1–5 ms network
round-trip time (RTT)**. This section shows how caching
performance changes under realistic network conditions.

All latency-scaled benchmarks use a TCP proxy
(`performance/redis_latency_proxy.py`) that injects configurable round-trip
delay between the benchmark app and Redis.

### Caching - fastapi-redis-sdk HIT/MISS by added RTT

| Added RTT            | Cache HIT | Cache MISS |
|----------------------|----------:|-----------:|
| **0 ms**  (loopback) |  690 µs   |  1,101 µs  |
| **+1 ms**            |  2,496 µs |   5,217 µs |
| **+2 ms**            |  3,987 µs |   7,502 µs |

Each millisecond of network RTT adds **~1,650 µs to HIT** and **~3,200 µs
to MISS**. The HIT path performs a pipelined `GET` + `TTL` (one round-trip)
plus middleware overhead. The MISS path adds a second round-trip for the
`SET` and involves the response-capture middleware buffering.

!!! note "Decorator-based libraries scale similarly"
    fastapi-cache2 and cashews also make Redis round-trips on every HIT/MISS
    and would see proportional latency increases. The absolute numbers differ
    based on how many Redis commands each library issues per request, but the
    **linear relationship between RTT and response time** applies equally to
    all Redis-backed caching libraries.

### Key takeaway

Each additional millisecond of RTT adds roughly **1.5–3 ms per
request** depending on the cache path (HIT vs MISS). With a 5 ms RTT, even
a cache HIT takes ~9 ms - making local response caching or edge caching
worth considering for latency-sensitive endpoints.

---

## Methodology

| Parameter            | Value                                 |
|----------------------|---------------------------------------|
| Python               | 3.14.3                                |
| Redis                | 8.0+ (Docker)                         |
| OS                   | macOS (Apple Silicon, M3 Max)         |
| Benchmark tool       | pytest-benchmark 5.2.3, pedantic mode |
| Rounds               | 500 (50 for eviction-group eviction)  |
| Iterations per round | 20                                    |
| Warmup rounds        | 20                                    |

All benchmarks use `TestClient` (synchronous), which runs the ASGI app
in-process. This measures the **full request-response cycle** including
middleware, dependency injection, Redis I/O, and serialization - not just
the caching logic in isolation.

Latency-scaled benchmarks use a Python asyncio TCP proxy that adds half the
specified delay in each direction (client→proxy→Redis and back), ensuring
identical added latency for all libraries under test.

### Running the benchmarks

```bash
# Caching benchmarks
pytest performance/test_cache_*.py performance/test_large_payload_*.py \
  performance/test_etag_304.py performance/test_evict.py \
  performance/test_group_evict.py performance/test_write_through.py \
  -v --benchmark-columns=min,mean,median,stddev,ops

# Latency-scaled benchmarks (via TCP proxy)
python performance/redis_latency_proxy.py --latency-ms 2 --listen-port 6380 &
REDIS_PORT=6380 pytest performance/test_cache_*.py -v

# Or run all latency levels automatically
./performance/run_latency_benchmarks.sh
```

**Prerequisites:** Redis on `localhost:6379`.
