import json
import os
import signal
import threading
import time
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
import redis
import requests
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError as KafkaException
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "alert-router",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("alert-router")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TIMESCALE_URL = os.getenv("TIMESCALE_URL", "postgresql://anomaly:anomaly@timescaledb:5432/anomalydb")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "").strip()
MIN_SLACK_SEVERITY = os.getenv("MIN_SLACK_SEVERITY", "HIGH").strip().upper()

SEVERITY_ORDER = {"LOW": 0, "MEDIUM": 1, "HIGH": 2, "CRITICAL": 3}

# Dedup TTLs per severity (seconds)
DEDUP_TTL = {"LOW": 600, "MEDIUM": 300, "HIGH": 120, "CRITICAL": 60}

# Slack threshold severity
SLACK_MIN_IDX = SEVERITY_ORDER.get(MIN_SLACK_SEVERITY, 2)

# ── Redis ────────────────────────────────────────────────────────────────────

redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)
redis_client = redis.Redis(connection_pool=redis_pool)

# ── Prometheus metrics ───────────────────────────────────────────────────────

alerts_fired_total = Counter(
    "alerts_fired_total", "Total alerts fired", ["severity"]
)
alerts_deduplicated_total = Counter(
    "alerts_deduplicated_total", "Total alerts deduplicated"
)
slack_notifications_sent_total = Counter(
    "slack_notifications_sent_total", "Total Slack notifications sent"
)
slack_notification_errors_total = Counter(
    "slack_notification_errors_total", "Total Slack notification errors"
)

# ── Severity bucketing ────────────────────────────────────────────────────────

def get_severity(score: float) -> str:
    if score >= 0.95:
        return "CRITICAL"
    elif score >= 0.90:
        return "HIGH"
    elif score >= 0.80:
        return "MEDIUM"
    else:
        return "LOW"


# ── TimescaleDB batch writer ──────────────────────────────────────────────────

class AlertBatchWriter:
    def __init__(self, dsn: str, flush_interval: float = 5.0, batch_size: int = 100):
        self.dsn = dsn
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
                logger.info("TimescaleDB connected for alerts")
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
                        """INSERT INTO alerts
                           (timestamp, event_id, user_id, anomaly_score,
                            severity, amount, phase, features_json)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                           ON CONFLICT DO NOTHING""",
                        (
                            row["timestamp"], row.get("event_id"),
                            row.get("user_id"), row.get("anomaly_score"),
                            row["severity"], row.get("amount"),
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


# ── Slack notification ────────────────────────────────────────────────────────

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
        resp = requests.post(
            SLACK_WEBHOOK_URL,
            json={"text": text},
            timeout=5,
        )
        resp.raise_for_status()
        slack_notifications_sent_total.inc()
        logger.info(f"Slack notification sent for {severity} alert")
    except Exception as exc:
        slack_notification_errors_total.inc()
        logger.warning(f"Slack notification failed: {exc}")


# ── HTTP server ──────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "alert-router"}).encode()
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


def start_http_server(port: int = 8006) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"HTTP server on port {port}")


# ── Kafka helpers ─────────────────────────────────────────────────────────────

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
            logger.info(f"Kafka consumer connected: {topic} / {group_id}")
            return c
        except Exception as exc:
            logger.warning(f"Consumer connect attempt {attempt}: {exc}")
            time.sleep(min(delay, 60.0))
            delay *= 2
    raise RuntimeError(f"Could not connect to Kafka consumer for {topic}")


def make_producer(max_retries: int = 20) -> KafkaProducer:
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


# ── Consumer A: anomaly-scores → alerts ──────────────────────────────────────

def consumer_a_loop(producer: KafkaProducer) -> None:
    """Filter is_anomaly=True events and publish to alerts topic."""
    consumer = make_consumer("anomaly-scores", "alert-group-a")
    logger.info("Consumer A started (anomaly-scores -> alerts)")
    try:
        while not shutdown_event.is_set():
            try:
                records = consumer.poll(timeout_ms=500, max_records=100)
            except KafkaException as exc:
                logger.error(f"Consumer A poll error: {exc}")
                time.sleep(2.0)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if shutdown_event.is_set():
                        break
                    event = msg.value
                    if not event.get("is_anomaly", False):
                        continue
                    # publish raw scored event to alerts topic for Consumer B
                    producer.send("alerts", event)
    finally:
        consumer.close()
        logger.info("Consumer A stopped")


# ── Consumer B: alerts → fanout (dedup + DB + Slack) ─────────────────────────

def consumer_b_loop(db_writer: AlertBatchWriter) -> None:
    """Consume alerts topic, dedup, write to DB, optionally notify Slack."""
    consumer = make_consumer("alerts", "alert-group-b")
    logger.info("Consumer B started (alerts fanout)")
    try:
        while not shutdown_event.is_set():
            try:
                records = consumer.poll(timeout_ms=500, max_records=100)
            except KafkaException as exc:
                logger.error(f"Consumer B poll error: {exc}")
                time.sleep(2.0)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    if shutdown_event.is_set():
                        break
                    alert = msg.value
                    score = float(alert.get("anomaly_score", 0.0))
                    severity = get_severity(score)
                    user_id = alert.get("user_id", 0)

                    # Deduplication via Redis TTL
                    dedup_key = f"alert:{user_id}:{severity}"
                    if redis_client.get(dedup_key):
                        alerts_deduplicated_total.inc()
                        continue

                    ttl = DEDUP_TTL.get(severity, 300)
                    redis_client.setex(dedup_key, ttl, "1")

                    now = datetime.now(timezone.utc).isoformat()
                    alert_record = {**alert, "timestamp": now, "severity": severity}

                    # Write to TimescaleDB
                    db_writer.add(alert_record)

                    # Prometheus
                    alerts_fired_total.labels(severity=severity).inc()

                    # Slack
                    if SLACK_WEBHOOK_URL:
                        send_slack(alert, severity)
                    else:
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
    finally:
        consumer.close()
        logger.info("Consumer B stopped")


# ── Main ──────────────────────────────────────────────────────────────────────

shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_http_server(8006)
    db_writer = AlertBatchWriter(dsn=TIMESCALE_URL)
    producer = make_producer()

    # Consumer A in a background thread
    t_a = threading.Thread(target=consumer_a_loop, args=(producer,), daemon=True)
    t_a.start()

    # Consumer B in the main thread (blocks until shutdown)
    try:
        consumer_b_loop(db_writer)
    finally:
        db_writer.flush_and_close()
        producer.flush(timeout=10)
        producer.close()
        logger.info("Alert router shutdown complete")


if __name__ == "__main__":
    main()
