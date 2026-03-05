---
name: alert-router
description: Reference for the alert-router service (port 8006): severity bucketing thresholds, Redis dedup TTL pattern, Slack webhook integration, dual-consumer threading model (Consumer A + B), AlertBatchWriter, and known gotchas. Load when working on anomaly-detection/services/alert-router/main.py.
allowed-tools: Read, Grep, Glob
---

# Alert Router Service

## Purpose

Filters anomalous events from `anomaly-scores`, applies severity bucketing based on score
thresholds, deduplicates alerts per user+severity using Redis TTL keys, writes de-duplicated
alerts to TimescaleDB `alerts` table, and optionally posts to a Slack webhook. Implements a
two-stage Kafka consumer pipeline: Consumer A reads `anomaly-scores` and re-publishes anomalous
events to the `alerts` topic; Consumer B reads `alerts` and performs the dedup/DB/Slack fanout.
Consumer A runs in a background daemon thread; Consumer B runs in the main thread. Exposes
`/health` and `/metrics` on port 8006.

---

## Severity Bucketing Pattern

```python
def get_severity(score: float) -> str:
    if score >= 0.95:
        return "CRITICAL"
    elif score >= 0.90:
        return "HIGH"
    elif score >= 0.80:
        return "MEDIUM"
    else:
        return "LOW"
```

Score thresholds: CRITICAL ≥ 0.95, HIGH ≥ 0.90, MEDIUM ≥ 0.80, LOW < 0.80.
Applied in Consumer B after reading from `alerts` topic.

---

## Redis Deduplication Pattern

### Key format
```
alert:{user_id}:{severity}
```
Examples: `alert:42:HIGH`, `alert:1000:CRITICAL`

### TTL per severity
```python
DEDUP_TTL = {"LOW": 600, "MEDIUM": 300, "HIGH": 120, "CRITICAL": 60}
```

Higher severity → shorter TTL → more frequent alerts for critical events.

### Check-and-set pattern (in Consumer B loop)
```python
dedup_key = f"alert:{user_id}:{severity}"
if redis_client.get(dedup_key):
    alerts_deduplicated_total.inc()
    continue              # skip this alert entirely

ttl = DEDUP_TTL.get(severity, 300)
redis_client.setex(dedup_key, ttl, "1")
# proceed with DB write + Slack
```

No SETNX or Lua script — there is a small race window between GET and SETEX where two
Consumer B instances could both pass the dedup check. In the single-instance setup
(one container), this is safe.

---

## Slack Webhook Pattern

### Configuration
```python
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
MIN_SLACK_SEVERITY = os.getenv("MIN_SLACK_SEVERITY", "HIGH").strip().upper()

SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}
SLACK_MIN_IDX  = SEVERITY_ORDER.get(MIN_SLACK_SEVERITY, 2)   # default: index 2 = HIGH
```

### Notification function
```python
def send_slack(alert: dict, severity: str) -> None:
    if not SLACK_WEBHOOK_URL:
        return
    if SEVERITY_ORDER.get(severity, 0) < SLACK_MIN_IDX:
        return

    emoji = {"LOW": "⚠️", "MEDIUM": "🔶", "HIGH": "🚨", "CRITICAL": "🆘"}.get(severity, "🚨")
    text = (
        f"{emoji} *{severity}* | Score: `{alert.get('anomaly_score', 0):.3f}` | "
        f"Amount: `${alert.get('amount', 0):.2f}` | "
        f"User: `{alert.get('user_id')}` | "
        f"Phase: `{alert.get('phase', 'unknown')}` | "
        f"`{alert.get('ingestion_timestamp', '')}`"
    )
    try:
        resp = requests.post(SLACK_WEBHOOK_URL, json={"text": text}, timeout=5)
        resp.raise_for_status()
        slack_notifications_sent_total.inc()
    except Exception as exc:
        slack_notification_errors_total.inc()
        logger.warning(f"Slack notification failed: {exc}")
```

### When Slack is disabled (SLACK_WEBHOOK_URL empty)
Alert is logged via structured logging instead:
```python
logger.info(
    f"ALERT {severity}",
    extra={"extra": {
        "score": round(score, 4),
        "amount": alert.get("amount"),
        "user_id": user_id,
        "phase": alert.get("phase"),
        "event_id": alert.get("event_id"),
    }},
)
```

---

## TimescaleDB Write Pattern

`AlertBatchWriter` — structurally identical to `BatchWriter` in drift-detector, but with
a different class name and different INSERT statement. Flush on 100 rows OR 5s.

```python
cur.execute(
    """INSERT INTO alerts
       (timestamp, event_id, user_id, anomaly_score,
        severity, amount, phase, features_json)
       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
       ON CONFLICT DO NOTHING""",
    (
        row["timestamp"],
        row.get("event_id"),
        row.get("user_id"),
        row.get("anomaly_score"),
        row["severity"],
        row.get("amount"),
        row.get("phase"),
        json.dumps({
            k: row.get(k) for k in [
                "amount_zscore", "risk_score_raw",
                "is_high_risk_merchant", "is_odd_hour",
                "tx_velocity_ratio", "amount_vs_user_avg",
            ]
        }),
    ),
)
self._conn.commit()
```

`ON CONFLICT DO NOTHING` — the `alerts` table has no unique constraint, so this clause
never actually fires. It is a defensive guard that does nothing in practice.

`features_json` stores a subset of 6 feature values as JSONB, not the full event payload.

---

## Dual Consumer Pattern

### Threading model
```python
def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_http_server(8006)
    db_writer = AlertBatchWriter(dsn=TIMESCALE_URL)
    producer  = make_producer()

    # Consumer A in background daemon thread
    t_a = threading.Thread(target=consumer_a_loop, args=(producer,), daemon=True)
    t_a.start()

    # Consumer B blocks main thread
    try:
        consumer_b_loop(db_writer)
    finally:
        db_writer.flush_and_close()
        producer.flush(timeout=10)
        producer.close()
        logger.info("Alert router shutdown complete")
```

### Consumer A — filter and re-publish
```python
def consumer_a_loop(producer: KafkaProducer) -> None:
    consumer = make_consumer("anomaly-scores", "alert-group-a")
    while not shutdown_event.is_set():
        records = consumer.poll(timeout_ms=500, max_records=100)
        for tp, messages in records.items():
            for msg in messages:
                event = msg.value
                if not event.get("is_anomaly", False):
                    continue                    # skip non-anomalous events
                producer.send("alerts", event)  # re-publish anomalous events
```

Consumer group: `alert-group-a`, topic: `anomaly-scores`

### Consumer B — dedup + DB + Slack
```python
def consumer_b_loop(db_writer: AlertBatchWriter) -> None:
    consumer = make_consumer("alerts", "alert-group-b")
    while not shutdown_event.is_set():
        records = consumer.poll(timeout_ms=500, max_records=100)
        for tp, messages in records.items():
            for msg in messages:
                alert    = msg.value
                score    = float(alert.get("anomaly_score", 0.0))
                severity = get_severity(score)
                user_id  = alert.get("user_id", 0)
                # dedup check → DB write → Prometheus → Slack
```

Consumer group: `alert-group-b`, topic: `alerts`

### Kafka consumer factory (reusable `make_consumer`)
Unlike other services that have separate `create_consumer` functions per topic, alert-router
uses a single parameterized factory:

```python
def make_consumer(topic: str, group_id: str, max_retries: int = 20) -> KafkaConsumer:
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            c = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id=group_id,
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                max_poll_records=100,
                session_timeout_ms=30000,
            )
            return c
        except Exception as exc:
            logger.warning(f"Consumer connect attempt {attempt}: {exc}")
            time.sleep(min(delay, 60.0))
            delay *= 2
```

---

## Prometheus Metrics in This Service

| Metric | Type | Labels | What it measures |
|--------|------|--------|-----------------|
| `alerts_fired_total` | Counter | `severity` | De-duplicated alerts written to DB |
| `alerts_deduplicated_total` | Counter | (none) | Alerts suppressed by Redis TTL dedup |
| `slack_notifications_sent_total` | Counter | (none) | Successful Slack webhook POSTs |
| `slack_notification_errors_total` | Counter | (none) | Failed Slack webhook POSTs |

`alerts_fired_total` has label `severity` with values: LOW, MEDIUM, HIGH, CRITICAL.
The other counters have no labels.

---

## Gotchas

- **Consumer A is a daemon thread.** When `consumer_b_loop()` returns (shutdown), the main
  thread enters the `finally:` block immediately. Consumer A may still be in mid-poll. There
  is no join or graceful shutdown of Consumer A — it terminates when the process exits.
  In-flight messages from Consumer A may not be flushed to `alerts` topic at shutdown.

- **`alerts` topic is internal to alert-router.** Consumer A writes to `alerts`; Consumer B
  reads from `alerts`. Both use `auto_offset_reset="latest"`. If Consumer B starts after Consumer A
  has already published some messages, those messages will be missed. In practice both consumers
  start within the same `main()` call, so race window is minimal.

- **Slack failures are non-fatal.** `send_slack()` catches all exceptions and increments
  `slack_notification_errors_total`. DB write always proceeds regardless of Slack outcome.

- **`ON CONFLICT DO NOTHING` is a no-op.** The `alerts` hypertable has no unique constraint
  defined in `db/init.sql`, so this SQL clause never prevents any insert. Duplicate alerts CAN
  be written to the DB if the Redis dedup key expires and the same user+severity alert fires again
  within the same session (by design — that's what TTL-based dedup is for).

- **Severity applied in Consumer B, not Consumer A.** Consumer A publishes the raw scored event
  to `alerts` without assigning severity. Consumer B assigns severity when reading from `alerts`.
  This means the `alerts` topic messages do NOT have a `severity` field.

- **`make_consumer` vs `create_consumer`.** This service uses `make_consumer(topic, group_id)`
  as a reusable factory (the only service to do this). All other services define separate
  `create_consumer()` functions per service. The `make_consumer` pattern is preferred when
  a service needs multiple consumers.
