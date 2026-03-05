---
name: pipeline-conventions
description: Project-wide coding conventions, absolute rules, logging patterns, service startup patterns, SIGTERM handling, and env var conventions for this anomaly detection pipeline. Load when writing or reviewing any service code.
allowed-tools: Read, Grep, Glob
---

# Pipeline Conventions — Real-Time Anomaly Detection Pipeline

## Project Overview

A real-time financial transaction anomaly detection system built as six Python 3.11 microservices
communicating over Apache Kafka. Raw synthetic transactions flow through a pipeline:
Producer → Feature Engineer (Faust) → ML Inference (River HalfSpaceTrees ensemble) →
Drift Detector (ADWIN + DDM) → Retraining Worker (Celery) → Alert Router (Slack / TimescaleDB).
Redis serves triple duty: sliding-window feature store, model persistence backend (pickle),
and Celery broker. TimescaleDB (PostgreSQL + TimescaleDB extension) stores scored events,
drift events, alerts, and model version history. Prometheus scrapes all six services at 15s
intervals; Grafana provides dashboards with both Prometheus and TimescaleDB SQL panels.

---

## Absolute Rules

- **ALWAYS** import KafkaError aliased: `from kafka.errors import KafkaError as KafkaException`
  — kafka-python 2.0.2 does not export `KafkaException` directly.
- **ALWAYS** import DDM from the correct path: `from river.drift.binary import DDM`
  — In River 0.21.0, `river.drift.DDM` does not exist.
- **ALWAYS** use a `JSONFormatter` class for all logging (see Logging Convention below).
- **ALWAYS** register both SIGTERM and SIGINT handlers pointing to `shutdown_event.set()`.
- **ALWAYS** expose `/health` (JSON) and `/metrics` (Prometheus) on the service port.
- **ALWAYS** use Redis connection pool: `redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)`.
- **ALWAYS** pin `aiokafka==0.10.0` in faust-streaming services — aiokafka 0.11+ removed the
  `api_version` parameter that faust-streaming 0.11.3 passes to AIOKafkaProducer.
- **ALWAYS** place SIGTERM cleanup in a `finally:` block that flushes DB, closes consumer,
  then flushes and closes producer (in that order).
- **NEVER** use `UNIQUE INDEX` on a non-partition-key column alone on a TimescaleDB hypertable.
  Use a regular `INDEX` with `(column, timestamp DESC)`.
- **NEVER** call `app.on_start.connect` or `app.on_stop.connect` in faust-streaming 0.11.3 —
  those signal hooks are not available.
- **NEVER** use the Zookeeper `ruok` four-letter command in healthchecks with
  `confluentinc/cp-zookeeper:7.4.0` — it is not whitelisted. Use `nc -z localhost 2181` instead.
- **NEVER** score after learning in River — always call `score_one()` before `learn_one()`.
- **NEVER** use module-level `shutdown_event` except in retraining-worker where it is required
  (kafka_consumer_loop references it at module scope). All other services define it inside main().

---

## Python Code Style

### Import ordering (observed consistently across all six services)
```
stdlib    →  json, math, os, pickle, signal, subprocess, sys, threading, time, logging, uuid
             datetime, http.server
third-party →  redis, faust, psycopg2, celery, kafka, prometheus_client, river, requests, aiohttp
local       →  (none — no local modules exist)
```

### Type hints
- Used on function signatures: `def create_consumer(max_retries: int = 20) -> KafkaConsumer:`
- Not used inside function bodies.
- Return type `None` omitted on void functions.

### Docstrings
- Rare — only one function in feature-engineer has a docstring (`window_get_stats`, `compute_features`).
- Do not add docstrings to functions you did not write.

### Other conventions
- f-strings for all string interpolation.
- `snake_case` for all variables, functions, and module-level constants spelled in UPPER_CASE.
- `PascalCase` for all classes.
- No enforced line-length limit observed.
- `round(value, N)` used consistently: 4 decimal places for feature values, 6 for ensemble score.

---

## Logging Convention

Every service defines an identical `JSONFormatter` class. The `service` field is hardcoded
to the service name (not read from an env var).

```python
class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "service-name",        # hardcoded per service
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("service-name")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)
```

**Usage patterns:**
```python
# Structured extra data
logger.info("Phase transition", extra={"extra": {"phase": phase, "event_number": n}})

# Plain message (no extra)
logger.info("Kafka consumer connected")

# Error with exception string
logger.error(f"Kafka poll error: {exc}")
logger.warning(f"Consumer connect attempt {attempt}: {exc}")
```

The `extra` field on the log record must be a dict nested under the key `"extra"`.
`getattr(record, "extra", {})` defaults to `{}` when no extra is passed.

Library used: stdlib `logging` — no structlog, loguru, or third-party logging libraries.

---

## Environment Variables

```python
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL               = os.getenv("REDIS_URL",               "redis://redis:6379")
TIMESCALE_URL           = os.getenv("TIMESCALE_URL",           "postgresql://anomaly:anomaly@timescaledb:5432/anomalydb")
ANOMALY_THRESHOLD       = float(os.getenv("ANOMALY_THRESHOLD", "0.7"))
REPLAY_SPEED            = float(os.getenv("REPLAY_SPEED",      "100"))
SLACK_WEBHOOK_URL       = os.getenv("SLACK_WEBHOOK_URL",       "").strip()
MIN_SLACK_SEVERITY      = os.getenv("MIN_SLACK_SEVERITY",      "HIGH").strip().upper()
```

**Notes:**
- `DRIFT_AFTER_N_EVENTS=100000` exists in `.env` but is not read by any service — unused.
- All env vars are read at module level (not inside functions).
- Type casting applied immediately: `float(os.getenv(...))`.
- `SLACK_WEBHOOK_URL` uses `.strip()` to handle trailing newlines in `.env`.
- Only `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_URL` are used by all services.
- `TIMESCALE_URL` used only by: drift-detector, retraining-worker, alert-router.
- `ANOMALY_THRESHOLD` used only by: ml-inference.
- `REPLAY_SPEED` used only by: producer.
- `SLACK_WEBHOOK_URL`, `MIN_SLACK_SEVERITY` used only by: alert-router.

---

## Service Startup Pattern

### Kafka consumer (exponential backoff — standard pattern for 5 of 6 services)
```python
def create_consumer(max_retries: int = 20) -> KafkaConsumer:
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            c = KafkaConsumer(
                "topic-name",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="service-group",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                max_poll_records=100,
                session_timeout_ms=30000,
            )
            logger.info("Kafka consumer connected")
            return c
        except Exception as exc:
            logger.warning(f"Consumer connect attempt {attempt}: {exc}")
            time.sleep(min(delay, 60.0))
            delay *= 2
    raise RuntimeError("Could not connect to Kafka consumer")
```

### TimescaleDB (BatchWriter._connect — 10 retries, fixed 5s delay)
```python
def _connect(self) -> None:
    for attempt in range(1, 11):
        try:
            self._conn = psycopg2.connect(self.dsn)
            self._conn.autocommit = False
            logger.info(f"TimescaleDB connected for {self.table}")
            return
        except Exception as exc:
            logger.warning(f"DB connect attempt {attempt}/10: {exc}")
            time.sleep(5)
    raise RuntimeError("Could not connect to TimescaleDB")
```

### Redis model load (5 retries, 5s delay)
```python
def load_models(max_retries: int = 5) -> tuple:
    for attempt in range(1, max_retries + 1):
        try:
            raw = redis_client.get("active_model")
            if raw:
                payload = pickle.loads(raw)
                ...
                return hst, sad, version
            break
        except Exception as exc:
            logger.warning(f"Model load attempt {attempt}/{max_retries}: {exc}")
            time.sleep(5)
    logger.info("Initializing fresh models")
    hst, sad = create_fresh_models()
    return hst, sad, 1
```

---

## SIGTERM Handling Pattern

```python
shutdown_event = threading.Event()     # inside main() for most services


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    ...

    try:
        while not shutdown_event.is_set():
            try:
                records = consumer.poll(timeout_ms=500, max_records=100)
            except KafkaException as exc:
                logger.error(f"Kafka poll error: {exc}")
                time.sleep(2.0)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if shutdown_event.is_set():   # inner break check
                        break
                    ...
    finally:
        db_writer.flush_and_close()     # 1. Flush DB (if BatchWriter used)
        consumer.close()                # 2. Close consumer
        producer.flush(timeout=10)      # 3. Flush producer
        producer.close()                # 4. Close producer
        logger.info("Service shutdown complete")
```

**Exception:** `feature-engineer` uses Faust which manages its own lifecycle — no explicit
SIGTERM handler or `shutdown_event` is implemented.

**Exception:** `retraining-worker` defines `shutdown_event` at module level (required because
`kafka_consumer_loop()` references it) and additionally terminates the Celery subprocess:
```python
finally:
    celery_proc.terminate()
    try:
        celery_proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        celery_proc.kill()
```

---

## Healthcheck Endpoint Pattern

### Standard pattern (producer, ml-inference, drift-detector, retraining-worker, alert-router)
```python
class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass  # suppress default HTTP access logging

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "service-name"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            output = generate_latest()
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(output)
        else:
            self.send_response(404)
            self.end_headers()


def start_http_server(port: int = PORT) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"HTTP server listening on port {port}")
```

### Faust pattern (feature-engineer only)
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

## Known Issues and Workarounds

| Issue | Root Cause | Fix Applied |
|-------|-----------|-------------|
| `KafkaException` not exported | kafka-python 2.0.2 API change | `from kafka.errors import KafkaError as KafkaException` |
| `river.drift.DDM` missing | River 0.21.0 moved DDM to sub-module | `from river.drift.binary import DDM as RiverDDM` |
| Faust `AIOKafkaProducer` error | aiokafka 0.11+ removed `api_version` param | Pin `aiokafka==0.10.0` |
| Faust signal hooks | `on_start.connect`/`on_stop.connect` removed in 0.11.3 | Removed those hooks |
| Zookeeper healthcheck | `ruok` not whitelisted by default | `nc -z localhost 2181` (TCP probe) |
| TimescaleDB unique index | UNIQUE on `event_id` alone illegal on hypertable | Regular `INDEX ON (event_id, timestamp DESC)` |
