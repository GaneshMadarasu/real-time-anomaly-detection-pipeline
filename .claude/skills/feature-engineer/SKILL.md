---
name: feature-engineer
description: Reference for the feature-engineer service (port 8002): Faust app setup, Redis sorted-set sliding window pattern, feature computation table, Kafka I/O config, and known gotchas. Load when working on anomaly-detection/services/feature-engineer/main.py.
allowed-tools: Read, Grep, Glob
---

# Feature Engineer Service

## Purpose

Consumes raw transaction events from the `raw-transactions` Kafka topic via a Faust (faust-streaming)
streaming agent, computes seven engineered features per event using per-user 60-second sliding
windows stored in Redis sorted sets, and publishes enriched events to `enriched-features`.
This is the only service in the pipeline that uses Faust async streaming; all other services
use synchronous kafka-python consumers. The Faust web server on port 8002 serves both the
`/health` and `/metrics` endpoints natively.

---

## Faust App Pattern

### App initialization
```python
broker_url = f"kafka://{KAFKA_BOOTSTRAP_SERVERS}"
app = faust.App(
    "feature-engineer",
    broker=broker_url,
    web_port=8002,
    web_bind="0.0.0.0",
    broker_consumer_auto_offset_reset="latest",
    topic_allow_declare=True,
    topic_disable_leader=True,
    producer_linger_ms=10,
)

raw_topic      = app.topic("raw-transactions",  value_serializer="raw")
enriched_topic = app.topic("enriched-features", value_serializer="raw")
```

`value_serializer="raw"` means Faust passes bytes directly without its own codec layer;
JSON encoding/decoding is done manually in the agent body.

### Agent structure
```python
@app.agent(raw_topic)
async def process_transactions(stream):
    async for payload in stream:
        try:
            if isinstance(payload, bytes):
                event = json.loads(payload.decode("utf-8"))
            else:
                event = json.loads(payload)

            enriched = compute_features(event)
            await enriched_topic.send(
                value=json.dumps(enriched).encode("utf-8")
            )
        except Exception as exc:
            logger.error(f"Error processing event: {exc}", extra={"extra": {"error": str(exc)}})
```

Exceptions are caught and logged per-event; the agent continues processing. No dead-letter queue.

### Startup command
Dockerfile CMD: `["python", "main.py", "worker", "--loglevel=info"]`

This passes the `worker` argument to `python main.py` which calls `app.main()` at the bottom
of `main.py`. Faust intercepts `sys.argv` and starts the worker mode. The `if __name__ == "__main__": app.main()` call at module bottom handles direct `python main.py worker ...` invocation.

### Web endpoints (Faust native, not http.server)
```python
@app.page("/health")
async def health_page(web, request):
    return web.json({"status": "ok", "service": "feature-engineer"})

@app.page("/metrics")
async def metrics_page(web, request):
    output = generate_latest()
    if isinstance(output, str):
        output = output.encode("utf-8")
    return Response(body=output, content_type=CONTENT_TYPE_LATEST)
```

---

## Redis Sliding Window Pattern

### Key naming format
```
window:amount:{user_id}
```
Example: `window:amount:42`

One sorted set per user. Only `amount` values are windowed; all other features are computed
from the raw event fields directly.

### Pipeline pattern (non-transactional for performance)
```python
WINDOW_TTL = 60  # seconds

def window_get_stats(user_id: int, timestamp: float, amount: float) -> tuple:
    key    = f"window:amount:{user_id}"
    member = f"{amount}:{uuid.uuid4()}"   # UUID suffix prevents dedup on equal amounts

    pipe = redis_client.pipeline(transaction=False)
    pipe.zadd(key, {member: timestamp})                     # add current (score=timestamp)
    pipe.zremrangebyscore(key, 0, timestamp - WINDOW_TTL)  # evict older than 60s
    pipe.zrange(key, 0, -1)                                 # fetch all remaining members
    pipe.expire(key, WINDOW_TTL * 2)                        # set TTL = 120s (2x window)
    results = pipe.execute()

    raw_members = results[2]   # zrange result
    values = []
    for m in raw_members:
        try:
            val_str = m.decode() if isinstance(m, bytes) else m
            values.append(float(val_str.split(":")[0]))    # parse amount before the colon
        except (ValueError, IndexError):
            pass

    if not values:
        return 0.0, 0.0

    n    = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0   # cannot compute std with fewer than 2 points

    variance = sum((v - mean) ** 2 for v in values) / (n - 1)  # sample variance (Bessel's)
    std = math.sqrt(variance)
    return mean, std
```

### Edge case handling
- `n < 2`: returns `(mean, 0.0)` — std is 0, z-score will be computed against `max(0.0, 1.0) = 1.0`.
- Empty window (all values evicted): returns `(0.0, 0.0)`.
- UUID suffix in member key: prevents Redis ZADD from treating two identical amounts as the
  same member (sorted sets deduplicate on member string, not score).
- `transaction=False`: pipeline runs as a batch without MULTI/EXEC — safe because operations
  are idempotent and we don't need atomicity.

---

## Feature Computation Pattern

All feature computation happens in `compute_features(event: dict) -> dict`:

```python
# Floor std at 1.0 to avoid division by zero in z-score
effective_std  = max(amount_std_60s, 1.0)
amount_zscore  = round((amount - amount_mean_60s) / effective_std, 4)

# Velocity: normalize transactions_last_hour by expected-max of 4
tx_velocity_ratio = round(transactions_last_hour / 4.0, 4)

# Binary risk flags (int, not bool)
is_high_risk_merchant = 1 if merchant_category in HIGH_RISK_MERCHANTS else 0
is_odd_hour           = 1 if hour_of_day in ODD_HOURS else 0

# Composite risk score (additive, max = 5)
risk_score_raw = 0.0
if not location_match:      risk_score_raw += 1
if not device_known:        risk_score_raw += 1
if is_high_risk_merchant:   risk_score_raw += 1
if is_odd_hour:             risk_score_raw += 1
if amount_zscore > 3.0:     risk_score_raw += 1
```

Constants:
```python
HIGH_RISK_MERCHANTS = {"crypto", "wire_transfer"}
ODD_HOURS           = {0, 1, 2, 3, 4, 5}
```

### Complete feature table

| Feature | Redis key | Formula / Source |
|---------|-----------|-----------------|
| `amount_mean_60s` | `window:amount:{user_id}` | 60s sliding sample mean |
| `amount_std_60s` | `window:amount:{user_id}` | 60s sliding sample std (Bessel's) |
| `amount_zscore` | derived | `(amount - mean) / max(std, 1.0)`, rounded to 4 dp |
| `tx_velocity_ratio` | none | `transactions_last_hour / 4.0`, rounded to 4 dp |
| `is_high_risk_merchant` | none | 1 if merchant in {crypto, wire_transfer}, else 0 |
| `is_odd_hour` | none | 1 if hour in {0,1,2,3,4,5}, else 0 |
| `risk_score_raw` | none | sum of 5 boolean risk factors, range [0.0, 5.0] |

All original event fields are passed through via `{**event, ...}`.

---

## Kafka Input/Output

- **Input topic:** `raw-transactions`, `value_serializer="raw"` (bytes)
- **Output topic:** `enriched-features`, `value_serializer="raw"` (bytes)
- **Serialization:** manual `json.loads(payload.decode("utf-8"))` in, `json.dumps(enriched).encode("utf-8")` out
- **Consumer group:** managed internally by Faust under app ID `"feature-engineer"`
- **Auto offset reset:** `broker_consumer_auto_offset_reset="latest"` (set on app)

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | Buckets |
|--------|------|--------|---------|
| `events_enriched_total` | Counter | (none) | n/a |
| `enrichment_latency_seconds` | Histogram | (none) | [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0] |

Latency measured from `start = time.time()` at top of `compute_features()` to
`elapsed = time.time() - start` just before return.

---

## Gaps

- **No SIGTERM handler:** Faust manages its own worker lifecycle. There is no `shutdown_event`,
  no `signal.signal()` call, and no `finally:` cleanup block. Faust handles SIGTERM internally.
- **No Redis disconnect on shutdown:** The synchronous `redis_client` connection pool is not
  explicitly closed. This is safe because pool connections are returned to the pool automatically
  and the process will terminate cleanly.

---

## Gotchas

- `numpy==1.26.0` is listed in `requirements.txt` but is **never imported** in `main.py`.
  It is a dead dependency. Do not add numpy-based computations without reconsidering whether
  this is intentional (it was likely a leftover from an earlier implementation).

- Redis pipeline uses `transaction=False` (non-atomic MULTI/EXEC). This means a crash between
  ZADD and ZREMRANGEBYSCORE will leave stale entries in the sorted set. The `expire(key, 120)`
  ensures stale keys are cleaned up within 2 minutes even if the eviction step is skipped.

- The Redis client is **synchronous** (`redis.Redis`), called from inside an **async** Faust
  agent. This blocks the event loop during Redis I/O. For high throughput, consider `aioredis`.
  Current implementation works because throughput is limited by REPLAY_SPEED.

- `amount_vs_user_avg` in the input event is computed by the **producer** (not here). The
  feature engineer re-computes per-user stats only for the sliding-window features.

- Member key format `f"{amount}:{uuid.uuid4()}"` means ZRANGE returns bytes like
  `b"150.5:f47ac10b-58cc-4372-a567-0e02b2c3d479"`. The parser splits on `:` and takes index 0.
  If amount happens to contain a colon (impossible for floats), parsing would silently fail.
