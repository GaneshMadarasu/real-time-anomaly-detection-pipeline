import json
import time
import math
import uuid
import logging
import os
from datetime import datetime

import redis
import faust
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from aiohttp.web import Response

# ── Structured JSON logging ──────────────────────────────────────────────────

class JSONFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps({
            "timestamp": datetime.utcnow().isoformat(),
            "service": "feature-engineer",
            "level": record.levelname,
            "message": record.getMessage(),
            "extra": getattr(record, "extra", {}),
        })

logger = logging.getLogger("feature-engineer")
handler = logging.StreamHandler()
handler.setFormatter(JSONFormatter())
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# ── Config ───────────────────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379")

HIGH_RISK_MERCHANTS = {"crypto", "wire_transfer"}
ODD_HOURS = {0, 1, 2, 3, 4, 5}

# ── Redis connection pool (sync) ─────────────────────────────────────────────

redis_pool = redis.ConnectionPool.from_url(REDIS_URL, max_connections=10)
redis_client = redis.Redis(connection_pool=redis_pool)

# ── Prometheus metrics ───────────────────────────────────────────────────────

events_enriched_total = Counter(
    "events_enriched_total", "Total events enriched"
)
enrichment_latency_seconds = Histogram(
    "enrichment_latency_seconds",
    "Enrichment processing latency",
    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0],
)

# ── Faust app ────────────────────────────────────────────────────────────────

broker_url = f"kafka://{KAFKA_BOOTSTRAP_SERVERS}"
app = faust.App(
    "feature-engineer",
    broker=broker_url,
    web_port=8002,
    web_bind="0.0.0.0",
    broker_consumer_auto_offset_reset="latest",
    topic_allow_declare=True,
    topic_disable_leader=True,
    producer_linger_ms=10,
)

raw_topic = app.topic("raw-transactions", value_serializer="raw")
enriched_topic = app.topic("enriched-features", value_serializer="raw")

# ── Sliding window helpers (sync Redis) ─────────────────────────────────────

WINDOW_TTL = 60  # seconds


def window_get_stats(user_id: int, timestamp: float, amount: float) -> tuple:
    """Add value to Redis sorted-set window and return (mean, std)."""
    key = f"window:amount:{user_id}"
    member = f"{amount}:{uuid.uuid4()}"

    pipe = redis_client.pipeline(transaction=False)
    pipe.zadd(key, {member: timestamp})
    pipe.zremrangebyscore(key, 0, timestamp - WINDOW_TTL)
    pipe.zrange(key, 0, -1)
    pipe.expire(key, WINDOW_TTL * 2)
    results = pipe.execute()

    raw_members = results[2]
    values = []
    for m in raw_members:
        try:
            val_str = m.decode() if isinstance(m, bytes) else m
            values.append(float(val_str.split(":")[0]))
        except (ValueError, IndexError):
            pass

    if not values:
        return 0.0, 0.0

    n = len(values)
    mean = sum(values) / n
    if n < 2:
        return mean, 0.0

    variance = sum((v - mean) ** 2 for v in values) / (n - 1)
    std = math.sqrt(variance)
    return mean, std


def compute_features(event: dict) -> dict:
    """Compute engineered features and return enriched event dict."""
    start = time.time()

    user_id = event["user_id"]
    amount = float(event.get("amount", 0.0))
    timestamp = float(event.get("ingestion_timestamp", time.time()))
    transactions_last_hour = int(event.get("transactions_last_hour", 1))
    merchant_category = event.get("merchant_category", "other")
    hour_of_day = int(event.get("hour_of_day", 0))
    location_match = bool(event.get("location_match", True))
    device_known = bool(event.get("device_known", True))

    amount_mean_60s, amount_std_60s = window_get_stats(user_id, timestamp, amount)

    effective_std = max(amount_std_60s, 1.0)
    amount_zscore = round((amount - amount_mean_60s) / effective_std, 4)

    tx_velocity_ratio = round(transactions_last_hour / 4.0, 4)
    is_high_risk_merchant = 1 if merchant_category in HIGH_RISK_MERCHANTS else 0
    is_odd_hour = 1 if hour_of_day in ODD_HOURS else 0

    risk_score_raw = 0.0
    if not location_match:
        risk_score_raw += 1
    if not device_known:
        risk_score_raw += 1
    if is_high_risk_merchant:
        risk_score_raw += 1
    if is_odd_hour:
        risk_score_raw += 1
    if amount_zscore > 3.0:
        risk_score_raw += 1

    enriched = {
        **event,
        "amount_mean_60s": round(amount_mean_60s, 4),
        "amount_std_60s": round(amount_std_60s, 4),
        "amount_zscore": amount_zscore,
        "tx_velocity_ratio": tx_velocity_ratio,
        "is_high_risk_merchant": is_high_risk_merchant,
        "is_odd_hour": is_odd_hour,
        "risk_score_raw": risk_score_raw,
    }

    elapsed = time.time() - start
    enrichment_latency_seconds.observe(elapsed)
    events_enriched_total.inc()

    return enriched


# ── Faust agent ──────────────────────────────────────────────────────────────

@app.agent(raw_topic)
async def process_transactions(stream):
    async for payload in stream:
        try:
            if isinstance(payload, bytes):
                event = json.loads(payload.decode("utf-8"))
            else:
                event = json.loads(payload)

            enriched = compute_features(event)
            await enriched_topic.send(
                value=json.dumps(enriched).encode("utf-8")
            )
        except Exception as exc:
            logger.error(f"Error processing event: {exc}", extra={"extra": {"error": str(exc)}})


# ── Faust web endpoints ──────────────────────────────────────────────────────

@app.page("/health")
async def health_page(web, request):
    return web.json({"status": "ok", "service": "feature-engineer"})


@app.page("/metrics")
async def metrics_page(web, request):
    output = generate_latest()
    if isinstance(output, str):
        output = output.encode("utf-8")
    return Response(body=output, content_type=CONTENT_TYPE_LATEST)



if __name__ == "__main__":
    app.main()
