# SKILL.md — Drift Detector Service

## Purpose

Consumes scored events from `anomaly-scores` and monitors for two types of distribution shift:
(1) **data drift** — changes in the statistical distribution of `amount`, `amount_zscore`, and
`anomaly_score` features, detected using ADWIN (Adaptive Windowing); and (2) **concept drift** —
degradation in model prediction accuracy relative to ground-truth `anomaly_label`, detected using
DDM (Drift Detection Method). When drift is detected, publishes to `drift-events` (observability)
and conditionally to `retraining-triggers` (action), subject to a 300-second Redis TTL cooldown.
Drift events are batch-written to TimescaleDB `drift_events` table. Exposes `/health` and
`/metrics` on port 8004.

---

## ADWIN Pattern

### Initialization (one detector per monitored feature)
```python
adwin_detectors = {
    "amount":        river_drift.ADWIN(),
    "amount_zscore": river_drift.ADWIN(),
    "anomaly_score": river_drift.ADWIN(),
}
```

### Per-event update and drift check
```python
feature_vals = {
    "amount":        float(event.get("amount", 0.0)),
    "amount_zscore": float(event.get("amount_zscore", 0.0)),
    "anomaly_score": float(event.get("anomaly_score", 0.0)),
}

for feature, detector in adwin_detectors.items():
    detector.update(feature_vals[feature])
    if detector.drift_detected:
        drift_record = {
            "timestamp":  now_iso,
            "drift_type": "data",
            "feature":    feature,
            "detector":   "ADWIN",
        }
        producer.send("drift-events", drift_record)
        db_writer.add(drift_record)
        drift_events_total.labels(type="data", feature=feature).inc()
        last_drift_time = time.time()
        time_since_last_drift.set(0)
        maybe_trigger_retrain(producer, "data")
```

Monitored features and rationale:
- `amount`: captures distribution shift in transaction values (Normal→Drift: mean 150→400)
- `amount_zscore`: captures shift in per-user deviation patterns even if absolute amounts shift together
- `anomaly_score`: captures upstream model behavior change (indirect indicator of concept drift)

ADWIN is initialized with default parameters (delta=0.002). No custom delta is set.

---

## DDM Pattern

### Correct import (River 0.21.0)
```python
from river.drift.binary import DDM as RiverDDM   # NOT: from river import drift; drift.DDM
```

### Initialization and update
```python
ddm = RiverDDM()

# Per event — compute binary error from ground truth vs prediction:
anomaly_label = int(event.get("anomaly_label", 0))   # ground truth (0 or 1)
is_anomaly    = bool(event.get("is_anomaly", False))  # model prediction
prediction    = 1 if is_anomaly else 0
error         = 0 if prediction == anomaly_label else 1  # 1 = misclassification

ddm.update(error)
if ddm.drift_detected:
    drift_record = {
        "timestamp":  now_iso,
        "drift_type": "concept",
        "feature":    "error_rate",
        "detector":   "DDM",
    }
    producer.send("drift-events", drift_record)
    db_writer.add(drift_record)
    drift_events_total.labels(type="concept", feature="error_rate").inc()
    maybe_trigger_retrain(producer, "concept")
```

DDM requires binary error input (exactly 0 or 1), not a probability. The `error_rate`
feature label is a convention, not an actual event field.

Concept drift urgency is `"high"` vs data drift `"medium"`:
```python
urgency = "high" if drift_type == "concept" else "medium"
```

---

## Cooldown Pattern

### Redis key and TTL
```python
RETRAIN_COOLDOWN = 300  # seconds (hardcoded, not configurable via env var)
cooldown_key     = "last_retrain_trigger"   # single global key, not per-feature
```

### Check and set pattern
```python
def maybe_trigger_retrain(producer: KafkaProducer, drift_type: str) -> None:
    cooldown_key = "last_retrain_trigger"
    if redis_client.get(cooldown_key):
        logger.info("Retrain trigger suppressed (cooldown active)")
        return

    urgency = "high" if drift_type == "concept" else "medium"
    trigger = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reason":    drift_type,
        "urgency":   urgency,
    }
    producer.send("retraining-triggers", trigger)
    redis_client.setex(cooldown_key, RETRAIN_COOLDOWN, "1")
    logger.info("Retraining trigger sent", extra={"extra": trigger})
```

`setex(key, time_seconds, value)`: atomically sets key with 300s TTL. Because the TTL
key is global (not per feature or detector), any drift detection restarts the same cooldown
window — a DDM trigger will suppress an ADWIN trigger fired 299 seconds later.

---

## Drift Event Publishing

### To `drift-events` topic (observability)
```python
{
    "timestamp":  "2024-01-01T00:00:00+00:00",   # timezone-aware ISO8601
    "drift_type": "data",                          # "data" | "concept"
    "feature":    "amount",                        # "amount" | "amount_zscore" | "anomaly_score" | "error_rate"
    "detector":   "ADWIN",                         # "ADWIN" | "DDM"
}
```

### To `retraining-triggers` topic (action, subject to cooldown)
```python
{
    "timestamp": "2024-01-01T00:00:00+00:00",     # timezone-aware ISO8601
    "reason":    "data",                           # "data" | "concept"
    "urgency":   "medium",                         # "high" (concept) | "medium" (data)
}
```

Note: timestamps use `datetime.now(timezone.utc).isoformat()` (timezone-aware), unlike the
producer and ml-inference which use `datetime.utcnow().isoformat()` (naive UTC). Inconsistency.

---

## TimescaleDB Write Pattern

`BatchWriter` class — flush on 100 rows OR 5s elapsed, whichever comes first.
Background thread checks every 1s.

```python
class BatchWriter:
    def __init__(self, dsn: str, table: str, flush_interval: float = 5.0, batch_size: int = 100):
        ...
        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

    def add(self, row: dict) -> None:
        with self._lock:
            self._buffer.append(row)
            if len(self._buffer) >= self.batch_size:
                self._flush_unlocked()

    def _flush_loop(self) -> None:
        while True:
            time.sleep(1.0)
            with self._lock:
                if time.time() - self._last_flush >= self.flush_interval and self._buffer:
                    self._flush_unlocked()
```

INSERT statement for `drift_events`:
```python
cur.execute(
    """INSERT INTO drift_events (timestamp, drift_type, feature, detector)
       VALUES (%s, %s, %s, %s)""",
    (row["timestamp"], row["drift_type"], row["feature"], row["detector"]),
)
self._conn.commit()
```

On DB error: rollback, reconnect (up to 10 attempts), do NOT retry the failed batch.
The batch is silently dropped — no dead-letter queue.

```python
except Exception as exc:
    logger.error(f"DB write error: {exc}")
    try:
        self._conn.rollback()
        self._connect()
    except Exception:
        pass
```

### At shutdown
```python
db_writer.flush_and_close()   # flushes buffer, closes psycopg2 connection
```

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | What it measures |
|--------|------|--------|-----------------|
| `drift_events_total` | Counter | `type` (data/concept), `feature` | Drift events by type and feature |
| `time_since_last_drift_seconds` | Gauge | (none) | Wall-clock seconds since most recent drift event |

`time_since_last_drift_seconds` is updated on every Kafka poll iteration:
```python
time_since_last_drift.set(time.time() - last_drift_time)
```

---

## Gotchas

- **ADWIN fires frequently after phase transitions.** Normal→Drift and Drift→Attack transitions
  cause immediate ADWIN triggers on `amount` (distribution shift). This is expected behavior.
  The 300s cooldown prevents retraining spam but does not suppress `drift-events` publications.
  Expect many `drift-events` messages during the first few minutes after each phase transition.

- **DDM cold start.** If the model has high error rate on the first few hundred events
  (before it has learned the distribution), DDM may fire spuriously. Not mitigated in current code.

- **One cooldown key covers all detectors.** If ADWIN fires on `amount`, the 300s cooldown
  also suppresses a DDM-triggered retrain event. The cooldown is per-service-instance, not per
  detector or feature.

- **Drift events still written to DB even during cooldown.** `db_writer.add(drift_record)` is
  called before `maybe_trigger_retrain()`. DB writes are unconditional; only the Kafka
  `retraining-triggers` message is gated by cooldown.

- **Timestamp format inconsistency.** `datetime.now(timezone.utc).isoformat()` produces
  timezone-aware strings (e.g., `"2024-01-01T00:00:00+00:00"`), while producer and ml-inference
  use `datetime.utcnow().isoformat()` (naive, e.g., `"2024-01-01T00:00:00"`). When comparing
  timestamps across topics, be aware of this format difference.

- **`ml-inference` does not write to `scored_events`.** The retraining worker's `retrain_model`
  task fetches from `scored_events` (`SELECT ... FROM scored_events LIMIT 50000`) but nothing
  populates this table. The query will return 0 rows and the task will skip retraining with
  `"insufficient_data"`. This is a gap in the pipeline that would prevent any actual model
  retraining from occurring.
