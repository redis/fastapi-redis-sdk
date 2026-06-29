# Observability demo (Docker)

A one-command, self-contained showcase of fastapi-redis-sdk's OpenTelemetry
instrumentation. It runs three containers:

| Container | Role |
|-----------|------|
| `app` | The demo FastAPI app ([`app.py`](app.py)), instrumented across all three OTel layers |
| `redis` | Redis 8 backing cache + rate limiting |
| `lgtm` | [`grafana/otel-lgtm`](https://github.com/grafana/docker-otel-lgtm) — OTel Collector + Tempo (traces) + Prometheus (metrics) + Grafana (UI), pre-wired |

See the [Observability guide](../../docs/guide/observability.md) for what each
span and metric means.

## 1. Start everything

From this directory:

```bash
docker compose up --build
```

Wait for `app` to log `Uvicorn running on http://0.0.0.0:8000`.

## 2. Generate some traffic

```bash
# A cache miss, then hits (same key, 30s TTL)
curl localhost:8000/products/42
curl localhost:8000/products/42
curl localhost:8000/products/42

# Trip the rate limiter: 5 allowed, then 429s (5 per 10s)
for i in $(seq 1 8); do curl -s -o /dev/null -w "%{http_code}\n" localhost:8000/limited; done
```

You should see three `200`s, then `429`s.

## 3. Explore in Grafana

Open **http://localhost:3000** (anonymous access is enabled; if prompted, use
`admin` / `admin`).

**Traces** — *Explore* → data source **Tempo** → *Search* → service
`fastapi-redis-observability-demo`. Open a `/products/{product_id}` request and
expand the nested trace (Layer 1 HTTP span with the Layer 2 cache span beneath):

```
GET /products/{product_id}          ← Layer 1 (FastAPI)
 └── cache.get   (cache.hit=true)   ← Layer 2 (fastapi-redis-sdk)
```

Open a `/limited` request to see the `ratelimit.hit` span and its
`ratelimit.backend` attribute (`increx` on Redis 8.8+, otherwise `lua`).

**Metrics** — *Explore* → data source **Prometheus** → query, e.g.:

```promql
redis_fastapi_cache_requests_total
redis_fastapi_ratelimit_requests_total
histogram_quantile(0.95, rate(redis_fastapi_ratelimit_latency_seconds_bucket[5m]))

# Layer 3 — redis-py native (OTel db.client.* semantics)
db_client_connection_count
```

The rate-limit counter is broken down by `result` (`allowed` / `limited` /
`bypass`); the cache counter by `result` (`hit` / `miss` / `bypass`).

> **Note on Layer 3:** redis-py's native OTel (`REDIS_OTEL_REDIS_ENABLED`)
> defaults to **metrics only** — connection/operation metrics under the OTel
> `db.client.*` convention (e.g. `db_client_connection_count`), not per-command
> spans. So the reproducible trace above shows Layers 1–2; Layer 3 shows up in
> Prometheus, not Tempo. (Command spans are available by enabling tracing in
> redis-py's own OTel config.)

## 4. Tear down

```bash
docker compose down
```

## How the three layers are enabled

- **Layer 1 (HTTP spans)** — the container runs under `opentelemetry-instrument`,
  which configures the OTel SDK + OTLP export from the `OTEL_*` env vars and
  installs the FastAPI instrumentation.
- **Layer 2 (cache + rate-limit spans & metrics)** — `.otel()` in
  [`app.py`](app.py), reusing the SDK providers set up by Layer 1.
- **Layer 3 (Redis command spans)** — `REDIS_OTEL_REDIS_ENABLED=true` in
  [`docker-compose.yml`](docker-compose.yml).

All OTLP data is sent to the `lgtm` container at `http://lgtm:4317`.
