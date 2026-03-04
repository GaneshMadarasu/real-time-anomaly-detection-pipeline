import json
import math
import os
import pickle
import signal
import threading
import time
import logging
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

import redis
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import KafkaError as KafkaException
from prometheus_client import Counter, Gauge, Histogram, generate_latest, CONTENT_TYPE_LATEST
from river import anomaly as river_anomaly

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "ml-inference",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("ml-inference")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")
ANOMALY_THRESHOLD = float(os.getenv("ANOMALY_THRESHOLD", "0.7"))

# ── Redis ────────────────────────────────────────────────────────────────────

redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)
redis_client = redis.Redis(connection_pool=redis_pool)

# ── Prometheus metrics ───────────────────────────────────────────────────────

events_processed_total = Counter("events_processed_total", "Total events scored")
anomalies_detected_total = Counter("anomalies_detected_total", "Total anomalies detected")
anomaly_score_hist = Histogram(
    "anomaly_score", "Anomaly score distribution",
    buckets=[0.0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0],
)
processing_latency_seconds = Histogram(
    "processing_latency_seconds", "ML inference latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5],
)
current_threshold_gauge = Gauge("current_threshold", "Current anomaly threshold")
model_version_gauge = Gauge("model_version", "Active model version")
rolling_anomaly_rate_gauge = Gauge("rolling_anomaly_rate", "Rolling anomaly rate last 1000 events")

# ── Custom SAD anomaly scorer ─────────────────────────────────────────────────

class StandardAbsoluteDeviation:
    """Online anomaly scorer based on z-score of a single feature."""

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


# ── Model management ─────────────────────────────────────────────────────────

def create_fresh_models() -> tuple:
    hst = river_anomaly.HalfSpaceTrees(
        n_trees=25, height=15, window_size=250, seed=42
    )
    sad = StandardAbsoluteDeviation()
    return hst, sad


def save_models(hst, sad, version: int) -> None:
    try:
        payload = pickle.dumps({"hst": hst, "sad": sad})
        redis_client.set("active_model", payload)
        redis_client.set("model_version", version)
        logger.info(f"Saved model version {version}")
    except Exception as exc:
        logger.error(f"Failed to save model: {exc}")


def load_models(max_retries: int = 5) -> tuple:
    for attempt in range(1, max_retries + 1):
        try:
            raw = redis_client.get("active_model")
            if raw:
                payload = pickle.loads(raw)
                hst = payload["hst"]
                sad = payload["sad"]
                version = int(redis_client.get("model_version") or 1)
                logger.info(f"Loaded model version {version} from Redis")
                return hst, sad, version
            break
        except Exception as exc:
            logger.warning(f"Model load attempt {attempt}/{max_retries}: {exc}")
            time.sleep(5)
    logger.info("Initializing fresh models")
    hst, sad = create_fresh_models()
    return hst, sad, 1


def build_feature_vector(event: dict) -> dict:
    def safe_float(v, default=0.0):
        try:
            val = float(v)
            return 0.0 if (math.isnan(val) or math.isinf(val)) else val
        except (TypeError, ValueError):
            return default

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


# ── HTTP server ──────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "ml-inference"}).encode()
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


def start_http_server(port: int = 8003) -> None:
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
                "enriched-features",
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
                group_id="inference-group",
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


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_http_server(8003)
    hst, sad, model_version = load_models()
    consumer = create_consumer()
    producer = create_kafka_producer()

    threshold = ANOMALY_THRESHOLD
    recent_scores: list = []
    events_since_save = 0

    current_threshold_gauge.set(threshold)
    model_version_gauge.set(model_version)

    logger.info("ML inference started")

    try:
        while not shutdown_event.is_set():
            # Check for updated model from retraining worker
            if events_since_save > 0 and events_since_save % 500 == 0:
                try:
                    raw = redis_client.get("active_model")
                    stored_version = int(redis_client.get("model_version") or 1)
                    if raw and stored_version > model_version:
                        payload = pickle.loads(raw)
                        hst = payload["hst"]
                        sad = payload["sad"]
                        model_version = stored_version
                        model_version_gauge.set(model_version)
                        logger.info(f"Loaded promoted model version {model_version}")
                except Exception as exc:
                    logger.warning(f"Model reload check failed: {exc}")

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
                    start = time.time()

                    features = build_feature_vector(event)

                    hst_score = float(hst.score_one(features))
                    sad_score = float(sad.score_one(features))
                    ensemble_score = round(0.7 * hst_score + 0.3 * sad_score, 6)

                    hst.learn_one(features)
                    sad.learn_one(features)

                    # Dynamic threshold
                    recent_scores.append(1 if ensemble_score > threshold else 0)
                    if len(recent_scores) > 1000:
                        recent_scores.pop(0)

                    if len(recent_scores) >= 100:
                        rate = sum(recent_scores) / len(recent_scores)
                        rolling_anomaly_rate_gauge.set(rate)
                        if rate > 0.05:
                            threshold = min(0.95, threshold + 0.01)
                        elif rate < 0.001:
                            threshold = max(0.50, threshold - 0.01)
                        current_threshold_gauge.set(threshold)

                    is_anomaly = ensemble_score > threshold

                    # Prometheus
                    anomaly_score_hist.observe(ensemble_score)
                    events_processed_total.inc()
                    if is_anomaly:
                        anomalies_detected_total.inc()

                    elapsed = time.time() - start
                    processing_latency_seconds.observe(elapsed)

                    output = {
                        **event,
                        "anomaly_score": ensemble_score,
                        "is_anomaly": is_anomaly,
                        "threshold_used": round(threshold, 4),
                        "model_version": model_version,
                    }
                    producer.send("anomaly-scores", output)

                    events_since_save += 1
                    if events_since_save % 1000 == 0:
                        save_models(hst, sad, model_version)

    finally:
        save_models(hst, sad, model_version)
        consumer.close()
        producer.flush(timeout=10)
        producer.close()
        logger.info("ML inference shutdown complete")


if __name__ == "__main__":
    main()
