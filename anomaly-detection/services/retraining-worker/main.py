import json
import math
import os
import pickle
import signal
import subprocess
import sys
import threading
import time
import logging
from datetime import datetime, timezone
from http.server import HTTPServer, BaseHTTPRequestHandler

import psycopg2
import redis
from celery import Celery
from kafka import KafkaConsumer
from kafka.errors import KafkaError as KafkaException
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from river import anomaly as river_anomaly

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "retraining-worker",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("retraining-worker")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
TIMESCALE_URL = os.getenv("TIMESCALE_URL", "postgresql://anomaly:anomaly@timescaledb:5432/anomalydb")

# ── Redis ────────────────────────────────────────────────────────────────────

redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)
redis_client = redis.Redis(connection_pool=redis_pool)

# ── Celery app ────────────────────────────────────────────────────────────────

celery_app = Celery("retraining", broker=REDIS_URL, backend=REDIS_URL)
celery_app.conf.task_serializer = "json"
celery_app.conf.accept_content = ["json"]
celery_app.conf.result_expires = 3600

# ── Prometheus metrics ───────────────────────────────────────────────────────

retraining_runs_total = Counter("retraining_runs_total", "Total retraining runs")
retraining_duration_seconds = Histogram(
    "retraining_duration_seconds", "Retraining duration in seconds",
    buckets=[5, 10, 30, 60, 120, 300, 600],
)
model_promotions_total = Counter("model_promotions_total", "Total model promotions")
last_retraining_timestamp = Gauge("last_retraining_timestamp", "Unix timestamp of last retraining")

# ── Custom SAD (must match ml-inference definition) ───────────────────────────

class StandardAbsoluteDeviation:
    def __init__(self):
        self._mean = 0.0
        self._m2 = 0.0
        self._n = 0

    def score_one(self, x: dict) -> float:
        val = float(x.get("amount", 0.0))
        if self._n < 2:
            return 0.0
        variance = self._m2 / (self._n - 1)
        std = math.sqrt(variance) if variance > 0 else 0.0
        if std == 0:
            return 0.0
        z = abs((val - self._mean) / std)
        return min(1.0, z / 10.0)

    def learn_one(self, x: dict) -> "StandardAbsoluteDeviation":
        val = float(x.get("amount", 0.0))
        self._n += 1
        delta = val - self._mean
        self._mean += delta / self._n
        delta2 = val - self._mean
        self._m2 += delta * delta2
        return self


def safe_float(v, default=0.0):
    try:
        val = float(v)
        return 0.0 if (math.isnan(val) or math.isinf(val)) else val
    except (TypeError, ValueError):
        return default


def build_feature_vector(event: dict) -> dict:
    return {
        "amount": safe_float(event.get("amount")),
        "amount_zscore": safe_float(event.get("amount_zscore")),
        "tx_velocity_ratio": safe_float(event.get("tx_velocity_ratio")),
        "amount_vs_user_avg": safe_float(event.get("amount_vs_user_avg")),
        "is_high_risk_merchant": safe_float(event.get("is_high_risk_merchant")),
        "is_odd_hour": safe_float(event.get("is_odd_hour")),
        "location_match": safe_float(1.0 if event.get("location_match") else 0.0),
        "device_known": safe_float(1.0 if event.get("device_known") else 0.0),
        "risk_score_raw": safe_float(event.get("risk_score_raw")),
        "hour_of_day": safe_float(event.get("hour_of_day")),
        "day_of_week": safe_float(event.get("day_of_week")),
    }


# ── Celery task: retrain_model ────────────────────────────────────────────────

@celery_app.task(name="retrain_model", bind=True, max_retries=3)
def retrain_model(self, trigger_info: dict) -> dict:
    start_time = time.time()
    retraining_runs_total.inc()
    logger.info("Starting retraining", extra={"extra": trigger_info})

    try:
        conn = psycopg2.connect(TIMESCALE_URL)
        cur = conn.cursor()
        cur.execute(
            """SELECT timestamp, amount, anomaly_score, is_anomaly, anomaly_label,
                      amount_vs_user_avg, features_json
               FROM scored_events
               ORDER BY timestamp DESC
               LIMIT 50000"""
        )
        rows = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as exc:
        logger.error(f"DB fetch failed: {exc}")
        raise self.retry(exc=exc, countdown=30)

    if len(rows) < 500:
        logger.info(f"Not enough data for retraining ({len(rows)} rows), skipping")
        return {"promoted": False, "reason": "insufficient_data", "n_samples": len(rows)}

    # Train new model
    new_hst = river_anomaly.HalfSpaceTrees(
        n_trees=25, height=15, window_size=250, seed=42
    )
    new_sad = StandardAbsoluteDeviation()

    for row in rows:
        features_json = row[6]
        if features_json:
            features = {k: safe_float(v) for k, v in features_json.items()}
        else:
            features = {
                "amount": safe_float(row[1]),
                "amount_zscore": 0.0,
                "tx_velocity_ratio": 0.0,
                "amount_vs_user_avg": safe_float(row[5]),
                "is_high_risk_merchant": 0.0,
                "is_odd_hour": 0.0,
                "location_match": 0.5,
                "device_known": 0.5,
                "risk_score_raw": 0.0,
                "hour_of_day": 12.0,
                "day_of_week": 3.0,
            }
        new_hst.learn_one(features)
        new_sad.learn_one(features)

    # Shadow test: compare old vs new on last 200 events
    shadow_rows = rows[:200]
    threshold = 0.7

    old_correct = 0
    new_correct = 0

    # Load old model
    try:
        raw_old = redis_client.get("active_model")
        if raw_old:
            old_payload = pickle.loads(raw_old)
            old_hst = old_payload["hst"]
            old_sad = old_payload["sad"]
        else:
            old_hst = new_hst
            old_sad = new_sad
    except Exception:
        old_hst = new_hst
        old_sad = new_sad

    for row in shadow_rows:
        anomaly_label = int(row[4]) if row[4] is not None else 0
        features_json = row[6]
        if features_json:
            features = {k: safe_float(v) for k, v in features_json.items()}
        else:
            features = {
                "amount": safe_float(row[1]),
                "amount_zscore": 0.0,
                "tx_velocity_ratio": 0.0,
                "amount_vs_user_avg": safe_float(row[5]),
                "is_high_risk_merchant": 0.0,
                "is_odd_hour": 0.0,
                "location_match": 0.5,
                "device_known": 0.5,
                "risk_score_raw": 0.0,
                "hour_of_day": 12.0,
                "day_of_week": 3.0,
            }

        old_score = 0.7 * float(old_hst.score_one(features)) + 0.3 * float(old_sad.score_one(features))
        new_score = 0.7 * float(new_hst.score_one(features)) + 0.3 * float(new_sad.score_one(features))

        old_pred = 1 if old_score > threshold else 0
        new_pred = 1 if new_score > threshold else 0

        if old_pred == anomaly_label:
            old_correct += 1
        if new_pred == anomaly_label:
            new_correct += 1

    n_shadow = len(shadow_rows)
    old_precision = old_correct / n_shadow if n_shadow > 0 else 0.0
    new_precision = new_correct / n_shadow if n_shadow > 0 else 0.0

    duration = time.time() - start_time
    retraining_duration_seconds.observe(duration)
    last_retraining_timestamp.set(time.time())

    if new_precision >= old_precision:
        # Promote new model
        try:
            new_version = int(redis_client.get("model_version") or 0) + 1
            payload = pickle.dumps({"hst": new_hst, "sad": new_sad})
            redis_client.set("active_model", payload)
            redis_client.set("model_version", new_version)
            model_promotions_total.inc()

            # Log to DB
            try:
                conn = psycopg2.connect(TIMESCALE_URL)
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO model_versions
                       (version, trained_at, trigger_reason, precision_score, n_samples)
                       VALUES (%s, %s, %s, %s, %s)""",
                    (new_version, datetime.now(timezone.utc),
                     trigger_info.get("reason", "unknown"),
                     round(new_precision, 4), len(rows)),
                )
                conn.commit()
                cur.close()
                conn.close()
            except Exception as db_exc:
                logger.warning(f"model_versions DB write failed: {db_exc}")

            logger.info(
                f"Model promoted to version {new_version}",
                extra={"extra": {
                    "old_precision": round(old_precision, 4),
                    "new_precision": round(new_precision, 4),
                    "n_samples": len(rows),
                }},
            )
            return {"promoted": True, "version": new_version, "precision": new_precision}
        except Exception as exc:
            logger.error(f"Model promotion failed: {exc}")
            raise
    else:
        logger.info(
            "New model not better, keeping old model",
            extra={"extra": {
                "old_precision": round(old_precision, 4),
                "new_precision": round(new_precision, 4),
            }},
        )
        return {"promoted": False, "reason": "lower_precision",
                "old_precision": old_precision, "new_precision": new_precision}


# ── HTTP server ──────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "retraining-worker"}).encode()
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


def start_http_server(port: int = 8005) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"HTTP server on port {port}")


# ── Kafka consumer loop (enqueues Celery tasks) ───────────────────────────────

def kafka_consumer_loop() -> None:
    delay = 1.0
    consumer = None

    for attempt in range(1, 21):
        try:
            consumer = KafkaConsumer(
                "retraining-triggers",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="retraining-group",
                auto_offset_reset="latest",
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                session_timeout_ms=30000,
            )
            logger.info("Retraining Kafka consumer connected")
            break
        except Exception as exc:
            logger.warning(f"Consumer connect attempt {attempt}: {exc}")
            time.sleep(min(delay, 60.0))
            delay *= 2

    if consumer is None:
        logger.error("Could not connect to Kafka consumer for retraining triggers")
        return

    try:
        while not shutdown_event.is_set():
            try:
                records = consumer.poll(timeout_ms=1000, max_records=10)
            except KafkaException as exc:
                logger.error(f"Kafka poll error: {exc}")
                time.sleep(2.0)
                continue

            for tp, messages in records.items():
                for msg in messages:
                    trigger_info = msg.value
                    logger.info(
                        "Received retraining trigger, enqueuing Celery task",
                        extra={"extra": trigger_info},
                    )
                    # Enqueue without blocking consumer poll loop
                    t = threading.Thread(
                        target=lambda ti=trigger_info: retrain_model.delay(ti),
                        daemon=True,
                    )
                    t.start()
    finally:
        consumer.close()


# ── Main ──────────────────────────────────────────────────────────────────────

shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


if __name__ == "__main__":
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    # Start Celery worker subprocess (imports this module as 'main', not '__main__')
    celery_proc = subprocess.Popen(
        [sys.executable, "-m", "celery", "-A", "main", "worker",
         "--loglevel=info", "--concurrency=2"],
        env=os.environ.copy(),
    )
    logger.info(f"Celery worker subprocess started (pid={celery_proc.pid})")

    start_http_server(8005)

    try:
        kafka_consumer_loop()
    finally:
        logger.info("Terminating Celery worker subprocess")
        celery_proc.terminate()
        try:
            celery_proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            celery_proc.kill()
        logger.info("Retraining worker shutdown complete")
