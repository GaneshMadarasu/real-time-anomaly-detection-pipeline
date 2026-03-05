---
name: retraining-worker
description: Reference for the retraining-worker service (port 8005): Celery app setup, subprocess worker pattern, retrain_model task, shadow accuracy testing, model version promotion to Redis, and known gotchas including the scored_events gap. Load when working on anomaly-detection/services/retraining-worker/main.py.
allowed-tools: Read, Grep, Glob
---

# Retraining Worker Service

## Purpose

Listens for retraining trigger messages on the `retraining-triggers` Kafka topic and dispatches
Celery tasks to retrain River models on recent TimescaleDB data. Implements a dual-process
architecture: the main process runs the Kafka consumer loop while a Celery worker subprocess
(launched via `subprocess.Popen`) executes the actual retraining. After training on up to
50,000 recent scored events, performs a shadow accuracy test against the current live model
on 200 samples. If the new model achieves equal or better accuracy, it is promoted by writing
to Redis keys `active_model` and `model_version`, which ml-inference hot-swaps on the next
500-event check. Exposes `/health` and `/metrics` on port 8005.

---

## Celery App Pattern

### App initialization
```python
celery_app = Celery("retraining", broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.task_serializer = "json"
celery_app.conf.accept_content  = ["json"]
celery_app.conf.result_expires  = 3600       # task results expire after 1 hour
```

Broker and backend both point to Redis. `task_serializer="json"` means Celery task arguments
must be JSON-serializable (dicts, not custom objects).

### Celery worker subprocess (from `__main__` block)
```python
celery_proc = subprocess.Popen(
    [sys.executable, "-m", "celery", "-A", "main", "worker",
     "--loglevel=info", "--concurrency=2"],
    env=os.environ.copy(),
)
```

`-A main`: Celery imports `main.py` as module `main` (not `__main__`), so the
`if __name__ == "__main__":` guard does not execute. No subprocess recursion.

Subprocess termination at shutdown:
```python
finally:
    celery_proc.terminate()
    try:
        celery_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        celery_proc.kill()
```

### Kafka trigger → Celery dispatch (non-blocking thread per trigger)
```python
for msg in messages:
    trigger_info = msg.value
    t = threading.Thread(
        target=lambda ti=trigger_info: retrain_model.delay(ti),
        daemon=True,
    )
    t.start()
```

`retrain_model.delay(ti)` enqueues the task to Redis without blocking the poll loop.
The `lambda ti=trigger_info:` captures `ti` by value (avoids late-binding closure bug).

---

## Retraining Task Pattern

```python
@celery_app.task(name="retrain_model", bind=True, max_retries=3)
def retrain_model(self, trigger_info: dict) -> dict:
```

### Step 1: Fetch training data from TimescaleDB
```python
cur.execute(
    """SELECT timestamp, amount, anomaly_score, is_anomaly, anomaly_label,
              amount_vs_user_avg, features_json
       FROM scored_events
       ORDER BY timestamp DESC
       LIMIT 50000"""
)
rows = cur.fetchall()
```

On DB failure → retry: `raise self.retry(exc=exc, countdown=30)` (max 3 retries, 30s apart).

### Step 2: Minimum sample guard
```python
if len(rows) < 500:
    logger.info(f"Not enough data for retraining ({len(rows)} rows), skipping")
    return {"promoted": False, "reason": "insufficient_data", "n_samples": len(rows)}
```

Guard is `< 500`, not `< 50000`. With fewer than 500 rows, training is skipped entirely.

### Step 3: Train new models on all fetched rows
```python
new_hst = river_anomaly.HalfSpaceTrees(n_trees=25, height=15, window_size=250, seed=42)
new_sad = StandardAbsoluteDeviation()

for row in rows:
    features_json = row[6]
    if features_json:
        features = {k: safe_float(v) for k, v in features_json.items()}
    else:
        # Fallback for rows without features_json (uses default neutral values)
        features = {
            "amount": safe_float(row[1]), "amount_zscore": 0.0,
            "tx_velocity_ratio": 0.0, "amount_vs_user_avg": safe_float(row[5]),
            "is_high_risk_merchant": 0.0, "is_odd_hour": 0.0,
            "location_match": 0.5, "device_known": 0.5,
            "risk_score_raw": 0.0, "hour_of_day": 12.0, "day_of_week": 3.0,
        }
    new_hst.learn_one(features)
    new_sad.learn_one(features)
```

Note: training data is sorted `DESC` (newest first), so models learn from newest to oldest.
This is an online learning quirk — River's HalfSpaceTrees uses a sliding window anyway.

---

## Model Version Management

Version is stored as integer in Redis key `model_version`:

```python
# On promotion:
new_version = int(redis_client.get("model_version") or 0) + 1
payload = pickle.dumps({"hst": new_hst, "sad": new_sad})
redis_client.set("active_model", payload)
redis_client.set("model_version", new_version)
model_promotions_total.inc()
```

DB record for audit trail:
```python
cur.execute(
    """INSERT INTO model_versions
       (version, trained_at, trigger_reason, precision_score, n_samples)
       VALUES (%s, %s, %s, %s, %s)""",
    (
        new_version,
        datetime.now(timezone.utc),
        trigger_info.get("reason", "unknown"),
        round(new_precision, 4),
        len(rows),
    ),
)
conn.commit()
```

DB write failure is caught and logged as a warning (non-fatal) — model is already promoted
in Redis at this point.

---

## Shadow Testing Pattern

### Setup
```python
shadow_rows = rows[:200]    # most recent 200 events (rows sorted newest-first)
threshold   = 0.7           # HARDCODED — does not use ml-inference's dynamic threshold
```

### Load old model for comparison
```python
try:
    raw_old = redis_client.get("active_model")
    if raw_old:
        old_payload = pickle.loads(raw_old)
        old_hst = old_payload["hst"]
        old_sad = old_payload["sad"]
    else:
        old_hst = new_hst   # fallback: compare new against itself → guaranteed promotion
        old_sad = new_sad
except Exception:
    old_hst = new_hst       # same fallback on any error
    old_sad = new_sad
```

### Comparison loop (same ensemble formula as ml-inference)
```python
for row in shadow_rows:
    anomaly_label = int(row[4]) if row[4] is not None else 0
    ...
    old_score = 0.7 * float(old_hst.score_one(features)) + 0.3 * float(old_sad.score_one(features))
    new_score = 0.7 * float(new_hst.score_one(features)) + 0.3 * float(new_sad.score_one(features))

    old_pred = 1 if old_score > threshold else 0
    new_pred = 1 if new_score > threshold else 0

    if old_pred == anomaly_label: old_correct += 1
    if new_pred == anomaly_label: new_correct += 1

old_precision = old_correct / n_shadow
new_precision = new_correct / n_shadow
```

Metric called "precision" in variable names is actually **accuracy** (fraction correct, not
precision in the strict ML sense — no TP/(TP+FP) calculation).

### Promotion criterion
```python
if new_precision >= old_precision:   # tie goes to new model
    # promote
else:
    return {"promoted": False, "reason": "lower_precision", ...}
```

---

## StandardAbsoluteDeviation (identical to ml-inference)

The `StandardAbsoluteDeviation` class is defined again here, identically to ml-inference.
Module-level `safe_float` is defined as a standalone function (unlike ml-inference where it
is a closure inside `build_feature_vector`):

```python
def safe_float(v, default=0.0):
    try:
        val = float(v)
        return 0.0 if (math.isnan(val) or math.isinf(val)) else val
    except (TypeError, ValueError):
        return default
```

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | Buckets |
|--------|------|--------|---------|
| `retraining_runs_total` | Counter | (none) | n/a |
| `retraining_duration_seconds` | Histogram | (none) | [5, 10, 30, 60, 120, 300, 600] |
| `model_promotions_total` | Counter | (none) | n/a |
| `last_retraining_timestamp` | Gauge | (none) | n/a |

`last_retraining_timestamp` is set to `time.time()` after shadow test completes, regardless
of whether promotion occurred.

---

## Entry Point — Module-Level Signal Handler

Unlike all other services, `retraining-worker` does NOT use `def main(): ...`.
Signal handler and `shutdown_event` are at **module level** (required because
`kafka_consumer_loop()` references `shutdown_event` from its module-level scope):

```python
shutdown_event = threading.Event()

def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()

if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    celery_proc = subprocess.Popen(...)
    start_http_server(8005)

    try:
        kafka_consumer_loop()
    finally:
        celery_proc.terminate()
        ...
```

---

## Gotchas

- **`scored_events` table is never populated** (as of current codebase). The retrain task
  fetches from `scored_events` but ml-inference does not write to it. The `< 500` sample guard
  will always trigger, returning `{"promoted": False, "reason": "insufficient_data"}`. To fix:
  either ml-inference must write to `scored_events`, or the retrain task must read from a
  different source.

- **Shadow test uses hardcoded threshold 0.7**, not the dynamic threshold that ml-inference
  may have auto-adjusted to (e.g., 0.85). Shadow test results may not reflect production model
  behavior at the actual operating threshold.

- **When Redis has no `active_model`** (first run), `old_hst = new_hst` and
  `old_sad = new_sad`, so `old_precision == new_precision` → guaranteed promotion. This is
  the intended bootstrap behavior.

- **Celery worker is spawned as a subprocess**, not a thread. The `/metrics` endpoint on port
  8005 exposes the main process's Prometheus metrics only, not the Celery worker's. If you add
  Prometheus counters inside Celery tasks, they increment in the worker subprocess's memory
  (not shared with the main process). The `retraining_runs_total` counter in `retrain_model`
  task will NOT appear in `/metrics`. This is a known limitation.

- **`max_retries=3` on Celery task with `countdown=30`** means DB failures can delay the task
  up to 90 seconds (3 × 30s). During that time, the trigger message is acknowledged from Kafka.
  If the container dies, the task is lost from the Celery queue.

- **`flower==2.0.1`** in requirements.txt enables the `celery-flower` service in docker-compose.
  The celery-flower service re-uses the retraining-worker Dockerfile with a custom CMD:
  `celery -A main flower --port=5555 --address=0.0.0.0`.
