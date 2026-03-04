import json
import random
import time
import signal
import threading
import uuid
import logging
import os
from datetime import datetime
from http.server import HTTPServer, BaseHTTPRequestHandler

from kafka import KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import KafkaError as KafkaException
from prometheus_client import Counter, Gauge, generate_latest, CONTENT_TYPE_LATEST

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "producer",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("producer")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REPLAY_SPEED = float(os.getenv("REPLAY_SPEED", "100"))

# ── Prometheus metrics ───────────────────────────────────────────────────────

events_produced_total = Counter("events_produced_total", "Total events produced")
current_phase_gauge = Gauge("current_phase", "Current phase: 0=normal 1=drift 2=attack")
anomaly_label_rate = Gauge("anomaly_label_rate", "Rolling anomaly rate over last 1000 events")

# ── Phase configuration ──────────────────────────────────────────────────────

PHASES = {
    "normal": {
        "amount_mean": 150, "amount_std": 50, "amount_min": 1.0,
        "tx_range": (1, 4),
        "hour_weights": [
            0.01, 0.01, 0.01, 0.01, 0.01, 0.02, 0.04, 0.07, 0.08, 0.08,
            0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.07, 0.06, 0.06, 0.05,
            0.04, 0.03, 0.02, 0.01,
        ],
        "merchants": {"grocery": 0.35, "restaurant": 0.25, "gas": 0.15, "retail": 0.20, "other": 0.05},
        "location_prob": 0.95,
        "device_prob": 0.90,
    },
    "drift": {
        "amount_mean": 400, "amount_std": 120, "amount_min": 1.0,
        "tx_range": (3, 10),
        "hour_weights": [
            0.05, 0.05, 0.06, 0.06, 0.05, 0.04, 0.03, 0.03, 0.03, 0.03,
            0.04, 0.04, 0.04, 0.04, 0.04, 0.04, 0.04, 0.04, 0.04, 0.04,
            0.05, 0.06, 0.07, 0.06,
        ],
        "merchants": {"crypto": 0.30, "wire_transfer": 0.25, "grocery": 0.10, "restaurant": 0.10,
                      "gas": 0.05, "retail": 0.10, "other": 0.10},
        "location_prob": 0.70,
        "device_prob": 0.60,
    },
    "attack": {
        "amount_mean": 2000, "amount_std": 800, "amount_min": 100.0,
        "tx_range": (10, 30),
        "hour_weights": None,  # uniform
        "merchants": {"wire_transfer": 0.50, "crypto": 0.30, "grocery": 0.04,
                      "restaurant": 0.04, "gas": 0.04, "retail": 0.04, "other": 0.04},
        "location_prob": 0.20,
        "device_prob": 0.15,
    },
}

PHASE_SEQUENCE = [("normal", 100_000), ("drift", 200_000), ("attack", 250_000)]
PHASE_NUMBER = {"normal": 0, "drift": 1, "attack": 2}

# ── Per-user state ───────────────────────────────────────────────────────────

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


def update_user_state(user_id: int, amount: float) -> None:
    state = user_state[user_id]
    state["count"] += 1
    n = state["count"]
    state["avg_amount"] += (amount - state["avg_amount"]) / n
    state["tx_count_last_hour"] += 1
    state["last_tx_time"] = time.time()


def get_phase(event_number: int) -> str:
    pos = event_number % 250_000
    if pos < 100_000:
        return "normal"
    elif pos < 200_000:
        return "drift"
    else:
        return "attack"


def generate_event(event_number: int) -> dict:
    phase = get_phase(event_number)
    cfg = PHASES[phase]

    user_id = random.randint(1, 1000)
    state = get_user_state(user_id)

    amount = random.gauss(cfg["amount_mean"], cfg["amount_std"])
    amount = max(amount, cfg["amount_min"])
    amount = round(amount, 2)

    if cfg["hour_weights"] is None:
        hour_of_day = random.randint(0, 23)
    else:
        hour_of_day = random.choices(range(24), weights=cfg["hour_weights"])[0]

    day_of_week = random.randint(0, 6)

    merchants = list(cfg["merchants"].keys())
    weights = list(cfg["merchants"].values())
    merchant_category = random.choices(merchants, weights=weights)[0]

    location_match = random.random() < cfg["location_prob"]
    device_known = random.random() < cfg["device_prob"]
    transactions_last_hour = random.randint(*cfg["tx_range"])

    user_avg = state["avg_amount"]
    amount_vs_user_avg = round(amount / user_avg if user_avg > 0 else 1.0, 4)
    drift_injected = phase in ("drift", "attack")

    # Anomaly label logic
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

    update_user_state(user_id, amount)

    return {
        "event_id": str(uuid.uuid4()),
        "user_id": user_id,
        "ingestion_timestamp": time.time(),
        "amount": amount,
        "hour_of_day": hour_of_day,
        "day_of_week": day_of_week,
        "merchant_category": merchant_category,
        "location_match": location_match,
        "device_known": device_known,
        "transactions_last_hour": transactions_last_hour,
        "amount_vs_user_avg": amount_vs_user_avg,
        "phase": phase,
        "drift_injected": drift_injected,
        "anomaly_label": anomaly_label,
    }


# ── Kafka helpers ────────────────────────────────────────────────────────────

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
            logger.warning(
                f"Kafka not ready (attempt {attempt}/{max_retries}): {exc}"
            )
            time.sleep(delay)
    raise RuntimeError("Could not connect to Kafka after max retries")


def ensure_topics(max_retries: int = 20) -> None:
    topic_names = [
        "raw-transactions", "enriched-features", "anomaly-scores",
        "drift-events", "alerts", "retraining-triggers", "phase-transitions",
    ]
    delay = 5.0
    for attempt in range(1, max_retries + 1):
        try:
            admin = KafkaAdminClient(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(",")
            )
            existing = set(admin.list_topics())
            new_topics = [
                NewTopic(name=t, num_partitions=3, replication_factor=1)
                for t in topic_names
                if t not in existing
            ]
            if new_topics:
                admin.create_topics(new_topics=new_topics, validate_only=False)
                logger.info(
                    "Created topics",
                    extra={"extra": {"topics": [t.name for t in new_topics]}}
                )
            admin.close()
            return
        except Exception as exc:
            logger.warning(
                f"Topic setup attempt {attempt}/{max_retries}: {exc}"
            )
            time.sleep(delay)


# ── HTTP server ──────────────────────────────────────────────────────────────

class HealthHandler(BaseHTTPRequestHandler):
    def log_message(self, format, *args):  # noqa: A002
        pass

    def do_GET(self):
        if self.path == "/health":
            body = json.dumps({"status": "ok", "service": "producer"}).encode()
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


def start_http_server(port: int = 8001) -> None:
    server = HTTPServer(("0.0.0.0", port), HealthHandler)
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    logger.info(f"HTTP server listening on port {port}")


# ── Main loop ────────────────────────────────────────────────────────────────

shutdown_event = threading.Event()


def signal_handler(signum, frame):
    logger.info("Shutdown signal received")
    shutdown_event.set()


def main():
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    start_http_server(8001)
    producer = create_kafka_producer()
    ensure_topics()

    event_number = 0
    current_phase_name = "normal"
    sleep_time = 1.0 / REPLAY_SPEED
    recent_labels: list = []

    current_phase_gauge.set(0)

    try:
        while not shutdown_event.is_set():
            try:
                event = generate_event(event_number)
                phase = event["phase"]

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

                producer.send("raw-transactions", event)
                events_produced_total.inc()

                recent_labels.append(event["anomaly_label"])
                if len(recent_labels) > 1000:
                    recent_labels.pop(0)
                if recent_labels:
                    anomaly_label_rate.set(
                        sum(recent_labels) / len(recent_labels)
                    )

                event_number += 1

                if event_number % 5000 == 0:
                    logger.info(
                        f"Produced {event_number} events",
                        extra={"extra": {
                            "phase": phase,
                            "anomaly_rate": round(
                                sum(recent_labels) / len(recent_labels), 4
                            ) if recent_labels else 0,
                        }},
                    )

            except KafkaException as exc:
                logger.error(f"Kafka error: {exc}")
                time.sleep(1.0)
                continue

            time.sleep(sleep_time)

    finally:
        logger.info("Flushing and closing Kafka producer")
        producer.flush(timeout=10)
        producer.close()


if __name__ == "__main__":
    main()
