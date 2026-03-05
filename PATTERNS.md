# PATTERNS.md — Pattern Index

Quick-reference index of all patterns documented in this project's SKILL.md files.
All paths are relative to `anomaly-detection/`.

---

## Where To Find Each Pattern

| Pattern | File | Section |
|---------|------|---------|
| Kafka consumer with exponential backoff | `services/ml-inference/SKILL.md` | Kafka Input/Output |
| Kafka consumer reusable factory | `services/alert-router/SKILL.md` | Dual Consumer Pattern |
| Kafka producer initialization | `services/producer/SKILL.md` | Kafka Producer Pattern |
| Redis sliding window (ZADD/ZREMRANGEBYSCORE) | `services/feature-engineer/SKILL.md` | Redis Sliding Window Pattern |
| Redis connection pool | `SKILL.md` (root) | Service Startup Pattern |
| Redis deduplication (TTL setex) | `services/alert-router/SKILL.md` | Redis Deduplication Pattern |
| River model persistence (pickle) | `services/ml-inference/SKILL.md` | Model Persistence Pattern |
| River score-before-learn convention | `services/ml-inference/SKILL.md` | River Model Pattern |
| NaN/Inf safe_float guard | `services/ml-inference/SKILL.md` | River Model Pattern |
| HalfSpaceTrees initialization parameters | `services/ml-inference/SKILL.md` | River Model Pattern |
| StandardAbsoluteDeviation custom class | `services/ml-inference/SKILL.md` | Custom StandardAbsoluteDeviation Class |
| Ensemble score formula (0.7 HST + 0.3 SAD) | `services/ml-inference/SKILL.md` | Ensemble Pattern |
| Dynamic threshold adjustment | `services/ml-inference/SKILL.md` | Dynamic Threshold Pattern |
| Model hot-swap from Redis | `services/ml-inference/SKILL.md` | Model Persistence Pattern |
| ADWIN data drift detection | `services/drift-detector/SKILL.md` | ADWIN Pattern |
| DDM concept drift detection | `services/drift-detector/SKILL.md` | DDM Pattern |
| Drift detection cooldown (Redis setex) | `services/drift-detector/SKILL.md` | Cooldown Pattern |
| Drift event schema | `services/drift-detector/SKILL.md` | Drift Event Publishing |
| Alert severity bucketing | `services/alert-router/SKILL.md` | Severity Bucketing Pattern |
| Alert deduplication (user+severity key) | `services/alert-router/SKILL.md` | Redis Deduplication Pattern |
| Slack webhook integration | `services/alert-router/SKILL.md` | Slack Webhook Pattern |
| Dual consumer threading model | `services/alert-router/SKILL.md` | Dual Consumer Pattern |
| SIGTERM shutdown pattern | `SKILL.md` (root) | SIGTERM Handling Pattern |
| Service startup retry loop (Kafka) | `SKILL.md` (root) | Service Startup Pattern |
| Service startup retry loop (TimescaleDB) | `SKILL.md` (root) | Service Startup Pattern |
| JSONFormatter structured logging | `SKILL.md` (root) | Logging Convention |
| Prometheus metric naming | `SKILL.md` (root) | (see each service SKILL.md) |
| TimescaleDB BatchWriter (flush 100 rows or 5s) | `services/drift-detector/SKILL.md` | TimescaleDB Write Pattern |
| TimescaleDB AlertBatchWriter | `services/alert-router/SKILL.md` | TimescaleDB Write Pattern |
| Shadow model testing | `services/retraining-worker/SKILL.md` | Shadow Testing Pattern |
| Model version management | `services/retraining-worker/SKILL.md` | Model Version Management |
| Celery + Kafka hybrid architecture | `services/retraining-worker/SKILL.md` | Celery App Pattern |
| Celery subprocess launch pattern | `services/retraining-worker/SKILL.md` | Celery App Pattern |
| Synthetic data phase management | `services/producer/SKILL.md` | Synthetic Data Generation Pattern |
| Per-user Welford online average | `services/producer/SKILL.md` | Synthetic Data Generation Pattern |
| Anomaly label logic | `services/producer/SKILL.md` | Anomaly Label Logic |
| Phase transition publishing | `services/producer/SKILL.md` | Phase Transition Handling |
| Faust app + agent structure | `services/feature-engineer/SKILL.md` | Faust App Pattern |
| Redis pipeline (non-transactional) | `services/feature-engineer/SKILL.md` | Redis Sliding Window Pattern |
| Healthcheck endpoint (/health + /metrics) | `SKILL.md` (root) | Healthcheck Endpoint Pattern |
| Faust web endpoint alternative | `services/feature-engineer/SKILL.md` | Faust App Pattern |
| Docker Compose healthcheck formats | `infrastructure/SKILL.md` | Docker Compose Patterns |
| depends_on with service_healthy | `infrastructure/SKILL.md` | Docker Compose Patterns |
| Kafka listener configuration | `infrastructure/SKILL.md` | Kafka Configuration |
| TimescaleDB hypertable creation | `infrastructure/SKILL.md` | TimescaleDB Configuration |
| TimescaleDB compound index rule | `infrastructure/SKILL.md` | TimescaleDB Configuration |
| Prometheus scrape config | `infrastructure/SKILL.md` | Prometheus Configuration |
| Grafana datasource UID convention | `infrastructure/SKILL.md` | Grafana Provisioning |
| env_file vs environment block pattern | `infrastructure/SKILL.md` | Environment Variable Passing |

---

## Anti-Patterns To Avoid

These patterns would break the system. Do not introduce them:

| Anti-Pattern | Why It Breaks | Correct Pattern |
|-------------|--------------|-----------------|
| `from kafka.errors import KafkaException` | Not exported in kafka-python 2.0.2 | `from kafka.errors import KafkaError as KafkaException` |
| `from river import drift; drift.DDM` | DDM moved in River 0.21.0 | `from river.drift.binary import DDM` |
| `aiokafka>=0.11.0` in feature-engineer | Removed `api_version` param faust-streaming passes | Pin `aiokafka==0.10.0` |
| `app.on_start.connect(...)` in Faust | Not available in faust-streaming 0.11.3 | Removed — no equivalent available |
| `UNIQUE INDEX ON scored_events(event_id)` | Illegal on TimescaleDB hypertable without partition key | `INDEX ON scored_events(event_id, timestamp DESC)` |
| `nc -z localhost 2181` for Kafka healthcheck | Wrong target — Kafka is on 29092 | `kafka-broker-api-versions --bootstrap-server localhost:29092` |
| `ruok` in Zookeeper healthcheck | Not whitelisted in cp-zookeeper:7.4.0 | `nc -z localhost 2181` |
| `hst.learn_one(features)` before `score_one` | River online learning data leakage | Always `score_one()` then `learn_one()` |
| Changing `StandardAbsoluteDeviation` in one service only | Pickle deserialization failure on cross-service load | Keep the class byte-for-byte identical in ml-inference and retraining-worker |
| `if __name__ == "__main__": main()` in retraining-worker | Triggers Celery subprocess recursion | Module-level entry point with `if __name__ == "__main__":` guard preventing celery re-execution |

---

## Inconsistencies Found

These are real inconsistencies between services that exist in the current codebase.
The "Preferred" column shows what new code should follow.

### 1. Kafka producer retry strategy

| Service | Retry Strategy | Start Delay | Max Delay |
|---------|---------------|-------------|-----------|
| producer | Fixed | 5.0s | 5.0s |
| ml-inference | Exponential backoff | 1.0s | 60.0s |
| drift-detector | Exponential backoff | 1.0s | 60.0s |
| alert-router | Exponential backoff | 1.0s | 60.0s |
| retraining-worker | Exponential backoff | 1.0s | 60.0s |

**Preferred:** Exponential backoff (`delay *= 2`, capped at `min(delay, 60.0)`)

### 2. Kafka producer reliability settings

| Service | `retries` | `acks` |
|---------|-----------|--------|
| producer | `retries=5` | `acks="all"` |
| ml-inference | not set (default) | not set (default) |
| drift-detector | not set (default) | not set (default) |
| alert-router | not set (default) | not set (default) |

**Preferred:** Set `retries=5, acks="all"` for all producer instances (producer's pattern).

### 3. BatchWriter duplication

`BatchWriter` in `drift-detector/main.py` and `AlertBatchWriter` in `alert-router/main.py`
are structurally identical (same `__init__`, `add`, `_flush_loop`, `_flush_unlocked`,
`_flush_loop`, `_connect`, `flush_and_close` methods) but with different class names and
different INSERT statements. They should share a common base class.

**Preferred:** Extract a `BaseBatchWriter(dsn, flush_interval, batch_size)` with a
`_do_insert(cur, row)` abstract method, placed in a shared `common.py` module.

### 4. `safe_float` scope

| Service | `safe_float` scope |
|---------|-------------------|
| ml-inference | Nested closure inside `build_feature_vector()` |
| retraining-worker | Module-level function |

Both have identical implementation. No behavioral difference.
**Preferred:** Module-level function (retraining-worker pattern) — easier to test and reuse.

### 5. Signal handler placement

| Service | `shutdown_event` location | Signal registration location |
|---------|--------------------------|------------------------------|
| producer, ml-inference, drift-detector, alert-router | Inside `main()` | Inside `main()` |
| retraining-worker | Module level | Inside `if __name__ == "__main__":` |

Retraining-worker differs because `kafka_consumer_loop()` references `shutdown_event` at
module scope, requiring it to be defined before `kafka_consumer_loop` is defined.
**This inconsistency is required and should not be "fixed".**

### 6. Timestamp format

| Service(s) | Timestamp format | Timezone |
|------------|-----------------|----------|
| producer, ml-inference | `datetime.utcnow().isoformat()` | Naive UTC |
| drift-detector, retraining-worker, alert-router | `datetime.now(timezone.utc).isoformat()` | Aware (+00:00) |

**Preferred:** `datetime.now(timezone.utc).isoformat()` (drift-detector / alert-router pattern).
Naive UTC timestamps can cause issues when comparing with timezone-aware timestamps in TimescaleDB.

### 7. Dead dependency

`feature-engineer/requirements.txt` includes `numpy==1.26.0`. It is not imported anywhere
in `main.py`. **Remove it** when next updating dependencies.

### 8. `scored_events` table never populated

`db/init.sql` creates the `scored_events` hypertable. The retraining worker reads from it.
But no service writes to it — ml-inference publishes to the `anomaly-scores` Kafka topic
without persisting to TimescaleDB. This breaks the retraining pipeline: `retrain_model` will
always return `{"promoted": False, "reason": "insufficient_data"}`.

**Fix:** ml-inference must write scored events to TimescaleDB (via a BatchWriter), or
the retraining worker must consume `anomaly-scores` directly rather than reading from DB.

### 9. Shadow test threshold mismatch

Retraining worker uses hardcoded `threshold = 0.7` for shadow testing. ml-inference uses a
dynamic threshold that auto-adjusts between 0.50 and 0.95. If ml-inference has drifted to
e.g. `threshold=0.85`, shadow test results at 0.7 will not reflect production accuracy.

**Fix:** Store the current dynamic threshold in Redis so retraining-worker can read it.
