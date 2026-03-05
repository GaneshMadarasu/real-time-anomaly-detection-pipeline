# SKILL.md — Producer Service

## Purpose

Generates synthetic financial transaction events at a configurable rate (default: 100 events/sec)
and publishes them to the `raw-transactions` Kafka topic. Cycles through three behavioral phases
(Normal → Drift → Attack) every 250,000 events to simulate real-world distribution shift and
concept drift. Maintains per-user in-process state (running average amount, transaction count)
to compute realistic `amount_vs_user_avg` values. Publishes phase change notifications to
`phase-transitions`. Exposes `/health` and `/metrics` on port 8001.

---

## Synthetic Data Generation Pattern

### Phase selection (cyclic, modulo 250,000)
```python
PHASE_SEQUENCE = [("normal", 100_000), ("drift", 200_000), ("attack", 250_000)]
PHASE_NUMBER   = {"normal": 0, "drift": 1, "attack": 2}

def get_phase(event_number: int) -> str:
    pos = event_number % 250_000
    if pos < 100_000:
        return "normal"
    elif pos < 200_000:
        return "drift"
    else:
        return "attack"
```

### Phase configuration (PHASES dict)
| Parameter | normal | drift | attack |
|-----------|--------|-------|--------|
| `amount_mean` | 150 | 400 | 2000 |
| `amount_std` | 50 | 120 | 800 |
| `amount_min` | 1.0 | 1.0 | 100.0 |
| `tx_range` | (1, 4) | (3, 10) | (10, 30) |
| `location_prob` | 0.95 | 0.70 | 0.20 |
| `device_prob` | 0.90 | 0.60 | 0.15 |
| `hour_weights` | weighted (daytime peak) | weighted (night shift) | None (uniform) |
| Top merchants | grocery(35%), restaurant(25%) | crypto(30%), wire_transfer(25%) | wire_transfer(50%), crypto(30%) |

`hour_weights=None` in attack phase means `random.randint(0, 23)`.

### Per-user state tracking (in-memory dict, lost on restart)
```python
user_state: dict = {}

def get_user_state(user_id: int) -> dict:
    if user_id not in user_state:
        user_state[user_id] = {
            "avg_amount": 150.0,
            "count": 0,
            "tx_count_last_hour": 0,
            "last_hour_reset": time.time(),
            "last_tx_time": time.time(),
        }
    state = user_state[user_id]
    now = time.time()
    if now - state["last_hour_reset"] >= 60:
        state["tx_count_last_hour"] = 0
        state["last_hour_reset"] = now
    return state
```

### Running average update (Welford's online algorithm)
```python
def update_user_state(user_id: int, amount: float) -> None:
    state = user_state[user_id]
    state["count"] += 1
    n = state["count"]
    state["avg_amount"] += (amount - state["avg_amount"]) / n
    state["tx_count_last_hour"] += 1
    state["last_tx_time"] = time.time()
```

---

## Anomaly Label Logic

Applied inside `generate_event()` after computing all fields. Conditions are checked in order;
first match wins (early return via reassignment):

```python
anomaly_label = 0
if amount > user_avg * 5.0:
    anomaly_label = 1
elif transactions_last_hour > 10:
    anomaly_label = 1
elif not location_match and not device_known:
    anomaly_label = 1
elif merchant_category in ("crypto", "wire_transfer") and amount > 1000:
    anomaly_label = 1
elif hour_of_day in (1, 2, 3, 4) and amount > 500:
    anomaly_label = 1
elif phase == "attack" and random.random() < 0.15:
    anomaly_label = 1
```

`anomaly_label` is ground truth used by drift-detector for DDM concept drift detection.

---

## Kafka Producer Pattern

### Initialization (fixed 5s delay retry — differs from other services which use exponential backoff)
```python
def create_kafka_producer(max_retries: int = 20, delay: float = 5.0) -> KafkaProducer:
    for attempt in range(1, max_retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
                acks="all",
                linger_ms=10,
            )
            logger.info("Kafka producer connected")
            return producer
        except Exception as exc:
            logger.warning(f"Kafka not ready (attempt {attempt}/{max_retries}): {exc}")
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after max retries")
```

Note: `retries=5` and `acks="all"` are set only in producer service. Other services do not
set these parameters (they use defaults).

### Send pattern
```python
producer.send("raw-transactions", event)    # fire and forget (no callback)
events_produced_total.inc()

# Error handling in main loop:
except KafkaException as exc:
    logger.error(f"Kafka error: {exc}")
    time.sleep(1.0)
    continue

# At shutdown:
producer.flush(timeout=10)
producer.close()
```

### Topic creation at startup
```python
def ensure_topics(max_retries: int = 20) -> None:
    topic_names = [
        "raw-transactions", "enriched-features", "anomaly-scores",
        "drift-events", "alerts", "retraining-triggers", "phase-transitions",
    ]
    # Creates only missing topics with num_partitions=3, replication_factor=1
    new_topics = [
        NewTopic(name=t, num_partitions=3, replication_factor=1)
        for t in topic_names if t not in existing
    ]
```

---

## Phase Transition Handling

Transition is detected by comparing current event's phase against `current_phase_name`
(a local variable in `main()`):

```python
if phase != current_phase_name:
    transition = {
        "phase": phase,
        "event_number": event_number,
        "timestamp": time.time(),
    }
    producer.send("phase-transitions", transition)
    logger.info(
        f"Phase transition: {current_phase_name} -> {phase}",
        extra={"extra": transition},
    )
    current_phase_name = phase
    current_phase_gauge.set(PHASE_NUMBER[phase])
```

Phase transitions are logged every 100,000 events. Transition message uses Unix float
timestamp (not ISO8601), consistent with `ingestion_timestamp` field in events.

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | What it measures |
|--------|------|--------|-----------------|
| `events_produced_total` | Counter | (none) | Total events sent to `raw-transactions` |
| `current_phase` | Gauge | (none) | 0=normal, 1=drift, 2=attack |
| `anomaly_label_rate` | Gauge | (none) | Rolling anomaly rate over last 1000 produced events |

`anomaly_label_rate` is maintained with a sliding list:
```python
recent_labels: list = []
recent_labels.append(event["anomaly_label"])
if len(recent_labels) > 1000:
    recent_labels.pop(0)
anomaly_label_rate.set(sum(recent_labels) / len(recent_labels))
```

Progress logged every 5,000 events.

---

## Gotchas

- `user_state` is in-process memory. On container restart, all users reset to `avg_amount=150.0`
  regardless of what phase we restart into. Drift-phase events will still compute correct
  `amount_vs_user_avg` after enough events warm up the state.

- `tx_count_last_hour` resets every 60 **real-world seconds**, not 60 simulated event-time
  seconds. At REPLAY_SPEED=100, 60 seconds of wall time = 6,000 events. At high replay speeds
  this counter barely fluctuates and is largely irrelevant.

- `sleep_time = 1.0 / REPLAY_SPEED`. At REPLAY_SPEED=1000, sleep_time=0.001s. Python
  `time.sleep()` resolution is ~1ms on Linux, so actual throughput may be lower than specified.

- Retry loop uses **fixed** `delay=5.0` (not exponential backoff like other services).
  Pattern inconsistency: if adding a new retry loop in this service, decide whether to match
  the producer's 5s fixed delay or the standard exponential backoff used everywhere else.

- `drift_injected` field in event is `True` for both "drift" and "attack" phases:
  `drift_injected = phase in ("drift", "attack")`. Not used downstream for routing logic,
  only for observability.
