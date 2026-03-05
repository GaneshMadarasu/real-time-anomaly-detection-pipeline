---
name: ml-inference
description: Reference for the ml-inference service (port 8003): River HalfSpaceTrees + StandardAbsoluteDeviation ensemble, score-before-learn rule, dynamic threshold auto-adjustment, Redis model persistence and hot-swap, and known gotchas. Load when working on anomaly-detection/services/ml-inference/main.py.
allowed-tools: Read, Grep, Glob
---

# ML Inference Service

## Purpose

Consumes enriched feature events from `enriched-features`, scores each event using a weighted
ensemble of River `HalfSpaceTrees` (unsupervised anomaly detection) and a custom
`StandardAbsoluteDeviation` scorer (z-score on amount), applies a dynamically self-adjusting
threshold to classify events as anomalies, and publishes scored events to `anomaly-scores`.
Models are pickled to Redis key `active_model` every 1,000 events. Promoted models from the
retraining worker are detected by comparing Redis `model_version` every 500 events and hot-swapped
without restart. Exposes `/health` and `/metrics` on port 8003.

---

## River Model Pattern

### Model initialization
```python
def create_fresh_models() -> tuple:
    hst = river_anomaly.HalfSpaceTrees(
        n_trees=25, height=15, window_size=250, seed=42
    )
    sad = StandardAbsoluteDeviation()
    return hst, sad
```

HalfSpaceTrees parameters:
- `n_trees=25`: 25 independent half-space trees (ensemble size)
- `height=15`: depth of each tree (2^15 = 32,768 leaves)
- `window_size=250`: sliding window of 250 observations per tree
- `seed=42`: reproducible random partition boundaries

### Score BEFORE learn (mandatory River convention)
```python
# 1. Score first (uses current model state)
hst_score = float(hst.score_one(features))
sad_score = float(sad.score_one(features))
ensemble_score = round(0.7 * hst_score + 0.3 * sad_score, 6)

# 2. Learn second (update model with this observation)
hst.learn_one(features)
sad.learn_one(features)
```

Reversing this order would cause the model to score using knowledge it just learned from
the same event — data leakage in online learning.

### NaN/Inf handling (build_feature_vector)
```python
def build_feature_vector(event: dict) -> dict:
    def safe_float(v, default=0.0):
        try:
            val = float(v)
            return 0.0 if (math.isnan(val) or math.isinf(val)) else val
        except (TypeError, ValueError):
            return default

    return {
        "amount":               safe_float(event.get("amount")),
        "amount_zscore":        safe_float(event.get("amount_zscore")),
        "tx_velocity_ratio":    safe_float(event.get("tx_velocity_ratio")),
        "amount_vs_user_avg":   safe_float(event.get("amount_vs_user_avg")),
        "is_high_risk_merchant":safe_float(event.get("is_high_risk_merchant")),
        "is_odd_hour":          safe_float(event.get("is_odd_hour")),
        "location_match":       safe_float(1.0 if event.get("location_match") else 0.0),
        "device_known":         safe_float(1.0 if event.get("device_known") else 0.0),
        "risk_score_raw":       safe_float(event.get("risk_score_raw")),
        "hour_of_day":          safe_float(event.get("hour_of_day")),
        "day_of_week":          safe_float(event.get("day_of_week")),
    }
```

`safe_float` is a local closure (nested function) in ml-inference, unlike retraining-worker
where it is a module-level function. Both have identical behavior.

---

## Custom StandardAbsoluteDeviation Class

This class is **NOT** a River built-in. It must be kept byte-for-byte identical in both
`ml-inference/main.py` and `retraining-worker/main.py` — pickle deserialization requires
the class definition to match at load time.

```python
class StandardAbsoluteDeviation:
    """Online anomaly scorer based on z-score of a single feature."""

    def __init__(self):
        self._mean = 0.0
        self._m2   = 0.0
        self._n    = 0

    def score_one(self, x: dict) -> float:
        val = float(x.get("amount", 0.0))
        if self._n < 2:
            return 0.0
        variance = self._m2 / (self._n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std == 0:
            return 0.0
        z = abs((val - self._mean) / std)
        return min(1.0, z / 10.0)   # normalize: z=10 maps to score=1.0

    def learn_one(self, x: dict) -> "StandardAbsoluteDeviation":
        val = float(x.get("amount", 0.0))
        self._n    += 1
        delta       = val - self._mean
        self._mean += delta / self._n
        delta2      = val - self._mean
        self._m2   += delta * delta2   # Welford's online variance
        return self
```

Uses Welford's online variance (numerically stable). Score is normalized by dividing
z-score by 10 (clamped to [0, 1]).

---

## Ensemble Pattern

```python
ensemble_score = round(0.7 * hst_score + 0.3 * sad_score, 6)
```

- HST weight: 0.7 (primary detector, multi-dimensional)
- SAD weight: 0.3 (secondary, univariate amount-only z-score)
- Both scores are in [0.0, 1.0]
- Rounded to 6 decimal places
- This same formula is used in retraining-worker shadow testing (must stay in sync)

---

## Dynamic Threshold Pattern

### Rolling window of binary decisions
```python
threshold     = ANOMALY_THRESHOLD      # starts at 0.7 (from env var)
recent_scores: list = []              # rolling list of 0/1 decisions

# Per event:
recent_scores.append(1 if ensemble_score > threshold else 0)
if len(recent_scores) > 1000:
    recent_scores.pop(0)
```

### Adjustment logic (runs every event after 100 samples)
```python
if len(recent_scores) >= 100:
    rate = sum(recent_scores) / len(recent_scores)
    rolling_anomaly_rate_gauge.set(rate)
    if rate > 0.05:                          # anomaly rate > 5%: threshold too low → raise it
        threshold = min(0.95, threshold + 0.01)
    elif rate < 0.001:                       # anomaly rate < 0.1%: threshold too high → lower it
        threshold = max(0.50, threshold - 0.01)
    current_threshold_gauge.set(threshold)
```

Clamp range: `[0.50, 0.95]`. Step size: `±0.01`. Target band: `0.1% – 5%` anomaly rate.
Adjustment happens on every event (not on a timer).

---

## Model Persistence Pattern

### Redis keys
- `active_model`: raw pickle bytes of `{"hst": hst_obj, "sad": sad_obj}`
- `model_version`: integer stored as string (e.g., `b"3"`)

### Save (every 1,000 events)
```python
def save_models(hst, sad, version: int) -> None:
    try:
        payload = pickle.dumps({"hst": hst, "sad": sad})
        redis_client.set("active_model", payload)
        redis_client.set("model_version", version)
        logger.info(f"Saved model version {version}")
    except Exception as exc:
        logger.error(f"Failed to save model: {exc}")

# Trigger in main loop:
events_since_save += 1
if events_since_save % 1000 == 0:
    save_models(hst, sad, model_version)

# Also save in finally block at shutdown:
finally:
    save_models(hst, sad, model_version)
```

No compression — raw pickle. For large HST models, Redis memory usage can grow.

### Load at startup (5 retries, 5s delay)
```python
def load_models(max_retries: int = 5) -> tuple:
    for attempt in range(1, max_retries + 1):
        try:
            raw = redis_client.get("active_model")
            if raw:
                payload = pickle.loads(raw)
                hst = payload["hst"]
                sad = payload["sad"]
                version = int(redis_client.get("model_version") or 1)
                return hst, sad, version
            break
        except Exception as exc:
            logger.warning(f"Model load attempt {attempt}/{max_retries}: {exc}")
            time.sleep(5)
    logger.info("Initializing fresh models")
    hst, sad = create_fresh_models()
    return hst, sad, 1
```

### Hot-swap promoted model (check every 500 events)
```python
if events_since_save > 0 and events_since_save % 500 == 0:
    try:
        raw = redis_client.get("active_model")
        stored_version = int(redis_client.get("model_version") or 1)
        if raw and stored_version > model_version:    # only forward promotion
            payload = pickle.loads(raw)
            hst = payload["hst"]
            sad = payload["sad"]
            model_version = stored_version
            model_version_gauge.set(model_version)
            logger.info(f"Loaded promoted model version {model_version}")
    except Exception as exc:
        logger.warning(f"Model reload check failed: {exc}")
```

---

## Kafka Input/Output

- **Input:** `enriched-features`, group `inference-group`, `auto_offset_reset="latest"`
- **Output:** `anomaly-scores`
- **Deserializer:** `lambda v: json.loads(v.decode("utf-8"))`
- **Serializer (producer):** `lambda v: json.dumps(v).encode("utf-8")`
- **Poll params:** `timeout_ms=500, max_records=100`

Fields added to output message:
```python
output = {
    **event,                                      # all enriched-features fields pass through
    "anomaly_score":   ensemble_score,            # float, 6 dp
    "is_anomaly":      is_anomaly,                # bool
    "threshold_used":  round(threshold, 4),       # current threshold at time of scoring
    "model_version":   model_version,             # int
}
```

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | Buckets |
|--------|------|--------|---------|
| `events_processed_total` | Counter | (none) | n/a |
| `anomalies_detected_total` | Counter | (none) | n/a |
| `anomaly_score` | Histogram | (none) | [0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0] |
| `processing_latency_seconds` | Histogram | (none) | [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5] |
| `current_threshold` | Gauge | (none) | n/a |
| `model_version` | Gauge | (none) | n/a |
| `rolling_anomaly_rate` | Gauge | (none) | n/a |

Latency measured from before `build_feature_vector()` to after `producer.send()`.

---

## Gotchas

- **No compression on pickle.** HalfSpaceTrees with `n_trees=25, height=15` can be several
  megabytes in memory. Monitor Redis `used_memory` if Redis runs low.

- **Forward-only model promotion.** The check `stored_version > model_version` means rollbacks
  require manual Redis manipulation (`redis-cli set model_version <old_version>`).

- **`StandardAbsoluteDeviation` must match across services.** If you change the class
  definition in one file, update the other. Pickle will silently load an incompatible object
  if class fields differ, leading to subtle scoring bugs.

- **`safe_float` defined as closure.** It is a local function inside `build_feature_vector`,
  not accessible elsewhere. In retraining-worker, it is module-level. Keep them synchronized.

- **Threshold not persisted.** The dynamic threshold is in-memory only. On restart, it resets
  to the `ANOMALY_THRESHOLD` env var value (default 0.7). It will re-adapt within ~100 events.

- **`ml-inference` does not write to TimescaleDB directly.** The `scored_events` table
  in `db/init.sql` exists but nothing in the current codebase populates it. This appears
  to be an incomplete feature — the retraining worker fetches from `scored_events` but no
  service inserts into it.
