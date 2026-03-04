import json
import os
import signal
import threading
import time
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
import psycopg2.pool
import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError as KafkaException
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST
from river import drift as river_drift
from river.drift.binary import DDM as RiverDDM

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "drift-detector",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("drift-detector")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TIMESCALE_URL = os.getenv("TIMESCALE_URL", "postgresql://anomaly:anomaly@timescaledb:5432/anomalydb")
RETRAIN_COOLDOWN = 300  # seconds

# ── Redis ────────────────────────────────────────────────────────────────────

redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)
redis_client = redis.Redis(connection_pool=redis_pool)

# ── Prometheus metrics ───────────────────────────────────────────────────────

drift_events_total = Counter(
    "drift_events_total", "Total drift events detected",
    ["type", "feature"],
)
time_since_last_drift = Gauge("time_since_last_drift_seconds", "Seconds since last drift event")

# ── TimescaleDB batch writer ──────────────────────────────────────────────────

class BatchWriter:
    def __init__(self, dsn: str, table: str, flush_interval: float = 5.0, batch_size: int = 100):
        self.dsn = dsn
        self.table = table
        self.flush_interval = flush_interval
        self.batch_size = batch_size
        self._buffer: list = []
        self._lock = threading.Lock()
        self._last_flush = time.time()
        self._conn = None
        self._connect()

        t = threading.Thread(target=self._flush_loop, daemon=True)
        t.start()

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

    def _flush_unlocked(self) -> None:
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer = []
        self._last_flush = time.time()
        self._write_batch(batch)

    def _write_batch(self, rows: list) -> None:
        if not rows:
            return
        try:
            with self._conn.cursor() as cur:
                for row in rows:
                    cur.execute(
                        """INSERT INTO drift_events (timestamp, drift_type, feature, detector)
                           VALUES (%s, %s, %s, %s)""",
                        (row["timestamp"], row["drift_type"], row["feature"], row["detector"]),
                    )
            self._conn.commit()
        except Exception as exc:
            logger.error(f"DB write error: {exc}")
            try:
                self._conn.rollback()
                self._connect()
            except Exception:
                pass

    def flush_and_close(self) -> None:
        with self._lock:
            self._flush_unlocked()
        if self._conn:
            self._conn.close()


# ── HTTP server ──────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "drift-detector"}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(body)
        elif self.path == "/metrics":
            self.send_response(200)
            self.send_header("Content-Type", CONTENT_TYPE_LATEST)
            self.end_headers()
            self.wfile.write(generate_latest())
        else:
            self.send_response(404)
            self.end_headers()


def start_http_server(port: int = 8004) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"HTTP server on port {port}")


# ── Kafka helpers ─────────────────────────────────────────────────────────────

def create_consumer(max_retries: int = 20) -> KafkaConsumer:
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            c = KafkaConsumer(
                "anomaly-scores",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="drift-group",
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


def create_kafka_producer(max_retries: int = 20) -> KafkaProducer:
    delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            p = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                linger_ms=10,
            )
            logger.info("Kafka producer connected")
            return p
        except Exception as exc:
            logger.warning(f"Producer connect attempt {attempt}: {exc}")
            time.sleep(min(delay, 60.0))
            delay *= 2
    raise RuntimeError("Could not connect to Kafka producer")


# ── Main ──────────────────────────────────────────────────────────────────────

shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


def maybe_trigger_retrain(producer: KafkaProducer, drift_type: str) -> None:
    cooldown_key = "last_retrain_trigger"
    if redis_client.get(cooldown_key):
        logger.info("Retrain trigger suppressed (cooldown active)")
        return

    urgency = "high" if drift_type == "concept" else "medium"
    trigger = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "reason": drift_type,
        "urgency": urgency,
    }
    producer.send("retraining-triggers", trigger)
    redis_client.setex(cooldown_key, RETRAIN_COOLDOWN, "1")
    logger.info(
        f"Retraining trigger sent",
        extra={"extra": trigger},
    )


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_http_server(8004)

    db_writer = BatchWriter(dsn=TIMESCALE_URL, table="drift_events")
    consumer = create_consumer()
    producer = create_kafka_producer()

    # One ADWIN per monitored feature
    adwin_detectors = {
        "amount": river_drift.ADWIN(),
        "amount_zscore": river_drift.ADWIN(),
        "anomaly_score": river_drift.ADWIN(),
    }
    ddm = RiverDDM()

    last_drift_time = time.time()
    time_since_last_drift.set(0)

    logger.info("Drift detector started")

    try:
        while not shutdown_event.is_set():
            # Update time-since-last-drift gauge
            time_since_last_drift.set(time.time() - last_drift_time)

            try:
                records = consumer.poll(timeout_ms=500, max_records=100)
            except KafkaException as exc:
                logger.error(f"Kafka poll error: {exc}")
                time.sleep(2.0)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if shutdown_event.is_set():
                        break
                    event = msg.value
                    now_iso = datetime.now(timezone.utc).isoformat()

                    # ── ADWIN drift detection on features ─────────────────
                    feature_vals = {
                        "amount": float(event.get("amount", 0.0)),
                        "amount_zscore": float(event.get("amount_zscore", 0.0)),
                        "anomaly_score": float(event.get("anomaly_score", 0.0)),
                    }
                    for feature, detector in adwin_detectors.items():
                        detector.update(feature_vals[feature])
                        if detector.drift_detected:
                            drift_record = {
                                "timestamp": now_iso,
                                "drift_type": "data",
                                "feature": feature,
                                "detector": "ADWIN",
                            }
                            producer.send("drift-events", drift_record)
                            db_writer.add(drift_record)
                            drift_events_total.labels(
                                type="data", feature=feature
                            ).inc()
                            last_drift_time = time.time()
                            time_since_last_drift.set(0)
                            logger.info(
                                f"ADWIN drift on {feature}",
                                extra={"extra": drift_record},
                            )
                            maybe_trigger_retrain(producer, "data")

                    # ── DDM concept drift detection ───────────────────────
                    anomaly_label = int(event.get("anomaly_label", 0))
                    is_anomaly = bool(event.get("is_anomaly", False))
                    prediction = 1 if is_anomaly else 0
                    error = 0 if prediction == anomaly_label else 1

                    ddm.update(error)
                    if ddm.drift_detected:
                        drift_record = {
                            "timestamp": now_iso,
                            "drift_type": "concept",
                            "feature": "error_rate",
                            "detector": "DDM",
                        }
                        producer.send("drift-events", drift_record)
                        db_writer.add(drift_record)
                        drift_events_total.labels(
                            type="concept", feature="error_rate"
                        ).inc()
                        last_drift_time = time.time()
                        time_since_last_drift.set(0)
                        logger.info(
                            "DDM concept drift detected",
                            extra={"extra": drift_record},
                        )
                        maybe_trigger_retrain(producer, "concept")

    finally:
        db_writer.flush_and_close()
        consumer.close()
        producer.flush(timeout=10)
        producer.close()
        logger.info("Drift detector shutdown complete")


if __name__ == "__main__":
    main()
