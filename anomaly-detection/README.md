# Real-Time Anomaly Detection Pipeline

A production-grade streaming ML system that detects financial transaction fraud in real time. It generates synthetic transactions, engineers features on a live sliding window, scores them with an online learning model, detects when the model drifts, automatically retrains it, and fires severity-bucketed alerts — all wired together with Kafka and launched with a single command.

---

## What This Project Demonstrates

| Concept | How it's implemented here |
|---|---|
| **Stream processing** | Kafka topics carry events between 6 independent microservices |
| **Online machine learning** | River `HalfSpaceTrees` updates its model on *every* event — no batch retraining cycle |
| **Feature engineering at scale** | Faust (async Kafka consumer) computes per-user sliding-window Z-scores and velocity ratios via Redis |
| **Drift detection** | ADWIN monitors feature distributions; DDM monitors prediction error rate |
| **Auto-retraining** | When drift fires, a Celery task fetches recent data, trains a challenger model, shadow-tests it, and promotes if it wins |
| **Alert routing** | Anomaly scores are severity-bucketed (LOW → CRITICAL), deduplicated via Redis TTLs, and fanned out to Slack |
| **Observability** | Every service exposes `/metrics` (Prometheus) and `/health` (JSON); Grafana provides pre-built dashboards |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────┐
│                        Kafka  (port 29092)                           │
│   raw-transactions  ·  enriched-features  ·  anomaly-scores          │
│   drift-events  ·  retraining-triggers  ·  alerts  ·  phase-transitions │
└──────────────────────────────────────────────────────────────────────┘
        ▲                   ▲                    ▲
        │                   │                    │
  ┌─────┴──────┐    ┌────────┴────────┐   ┌──────┴──────────┐
  │  Producer  │───▶│ Feature Engineer│──▶│  ML Inference   │
  │  :8001     │    │  (Faust) :8002  │   │  (River) :8003  │
  │ 100 evt/s  │    │  Redis windows  │   │ HalfSpaceTrees  │
  └────────────┘    └─────────────────┘   └────────┬────────┘
                                                    │ anomaly-scores
                                    ┌───────────────┼───────────────┐
                                    ▼               │               ▼
                           ┌────────────────┐       │    ┌──────────────────┐
                           │ Drift Detector │       │    │  Alert Router    │
                           │ ADWIN+DDM :8004│       │    │  :8006           │
                           └───────┬────────┘       │    │  Dedup · Slack   │
                                   │ retraining-    │    └────────┬─────────┘
                                   │ triggers       │             │ alerts
                                   ▼                │             ▼
                           ┌────────────────┐       │    ┌──────────────────┐
                           │  Retraining    │       │    │   TimescaleDB    │
                           │  Worker :8005  │       │    │   (5432)         │
                           │  Celery+River  │       │    │   scored_events  │
                           └───────┬────────┘       │    │   drift_events   │
                                   │ promotes       │    │   alerts         │
                                   ▼                │    │   model_versions │
                                 Redis ◀────────────┘    └──────────────────┘
                              active_model                        ▲
                              model_version                       │
                                                       ┌──────────┴──────────┐
                                                       │  Prometheus  :9090  │
                                                       │  Grafana     :3000  │
                                                       └─────────────────────┘
```

### The 6 Microservices

| Service | Port | Technology | Role |
|---|---|---|---|
| **Producer** | 8001 | kafka-python | Generates synthetic financial transactions at a configurable rate and cycles through Normal → Drift → Attack phases |
| **Feature Engineer** | 8002 | Faust (async Kafka) | Computes sliding-window features per user in real time: amount Z-score, transaction velocity, user spend deviation, device and location signals |
| **ML Inference** | 8003 | River HalfSpaceTrees | Scores every event with an online ensemble model; self-adjusts its anomaly threshold to target a 0.1–5% anomaly rate; persists the model to Redis every 1,000 events |
| **Drift Detector** | 8004 | River ADWIN + DDM | ADWIN watches feature distributions for data drift; DDM watches prediction error rate for concept drift; emits retraining triggers when either fires |
| **Retraining Worker** | 8005 | Celery + River | Receives triggers, fetches the last 50k scored events from TimescaleDB, trains a challenger model, shadow-tests it against the incumbent on the 200 most recent events, and promotes it atomically in Redis if it performs better |
| **Alert Router** | 8006 | kafka-python | Classifies anomaly scores into LOW / MEDIUM / HIGH / CRITICAL, deduplicates with per-severity Redis TTLs, writes to TimescaleDB, and forwards high-severity events to Slack |

### Infrastructure

| Component | Port | Purpose |
|---|---|---|
| Kafka (Confluent 7.4) | 9092 (ext) / 29092 (int) | Message bus — 7 topics, 3 partitions each |
| Redis 7 | 6379 | Sliding-window state, model storage, Celery broker, alert dedup TTLs |
| TimescaleDB (pg15) | 5432 | Time-series storage for scored events, drift events, alerts, model versions |
| Prometheus | 9090 | Scrapes `/metrics` from all 6 services |
| Grafana | 3000 | Pre-provisioned dashboard with Prometheus + TimescaleDB datasources |
| Kafka UI | 8080 | Browse topics and consumer offsets |
| Schema Registry | 8081 | Confluent schema management |
| Celery Flower | 5555 | Monitor Celery retraining tasks |

---

## Quick Start

**Requirements:** Docker Engine 24+, Docker Compose v2, 8 GB RAM (16 GB recommended), 4 CPU cores

```bash
git clone https://github.com/GaneshMadarasu/real-time-anomaly-detection-pipeline.git
cd real-time-anomaly-detection-pipeline/anomaly-detection
docker compose up --build
```

Wait ~60–90 seconds for Kafka and TimescaleDB to fully initialize, then open:

| Dashboard | URL | Login |
|---|---|---|
| Grafana | http://localhost:3000 | admin / admin |
| Kafka UI | http://localhost:8080 | — |
| Prometheus | http://localhost:9090 | — |
| Celery Flower | http://localhost:5555 | — |

---

## How the Simulation Works

The Producer cycles through three phases automatically — no manual intervention needed:

| Phase | Event range | Anomaly rate | What happens |
|---|---|---|---|
| **Normal** | 0 – 99,999 | ~1–2% | Baseline transactions. Model learns typical behaviour. |
| **Drift** | 100,000 – 199,999 | ~5–8% | Amount distribution shifts. ADWIN fires ~1–2 min in. Retraining Celery task starts. New model version promoted. |
| **Attack** | 200,000 – 249,999 | ~15–20% | Fraud burst. HIGH/CRITICAL alerts spike. DDM fires on rising error rate. Cycle repeats. |

At 100 events/sec (default), the drift phase is reached in ~16 minutes and the attack phase in ~33 minutes. Use `REPLAY_SPEED=500` to reach them in ~3 min and ~7 min respectively.

---

## Verifying It's Working

1. **Kafka UI** → Topics → `raw-transactions` → messages should be flowing within 30 seconds
2. **Grafana** → "Real-Time Anomaly Detection" dashboard → stat panels should show non-zero values within 2 minutes
3. **Prometheus** → query `events_processed_total` → value should be increasing
4. **TimescaleDB** → connect to `localhost:5432` (user: `anomaly`, pass: `anomaly`, db: `anomalydb`) → `SELECT COUNT(*) FROM scored_events` should grow
5. **Celery Flower** → a `retrain_model` task should appear ~1–2 minutes after the drift phase starts (~t=17min at default speed)

---

## Configuration

All settings live in `anomaly-detection/.env`:

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
REDIS_URL=redis://redis:6379
TIMESCALE_URL=postgresql://anomaly:anomaly@timescaledb:5432/anomalydb

ANOMALY_THRESHOLD=0.7      # Starting threshold — auto-adjusted at runtime (0.50–0.95)
REPLAY_SPEED=100           # Synthetic events per second
DRIFT_AFTER_N_EVENTS=100000

SLACK_WEBHOOK_URL=         # Optional — leave empty to disable Slack
MIN_SLACK_SEVERITY=HIGH    # Only forward HIGH and CRITICAL to Slack
```

**Tuning tips:**

- `ANOMALY_THRESHOLD=0.65` → more sensitive (lower threshold, more alerts, more false positives)
- `ANOMALY_THRESHOLD=0.80` → less sensitive (fewer alerts, may miss borderline anomalies)
- `REPLAY_SPEED=500` → 5× speed; reaches drift phase in ~3 min (may strain CPU)

---

## Key Design Decisions

**Why online learning?**
`HalfSpaceTrees` from the River library updates on every single event. There is no batch training cycle, no cold-start latency, and no stale model sitting idle between training jobs. The model adapts continuously as transaction patterns evolve.

**Why a dynamic threshold?**
A fixed threshold breaks the moment traffic volume or fraud rate changes. The inference service watches its own rolling anomaly rate and nudges the threshold ±0.01 every evaluation cycle to stay in the 0.1–5% target band.

**Why shadow-test before promotion?**
A newly trained model might perform worse if drift was transient. Before promoting, the Retraining Worker runs the challenger and incumbent side-by-side on the 200 most recent scored events. Only if the challenger's precision is equal or better does it get written to Redis.

**Why batch-write to TimescaleDB?**
Per-event DB inserts at 100 events/sec would create enormous write amplification. A `BatchWriter` background thread flushes every 100 rows *or* every 5 seconds — whichever comes first — reducing round-trips by ~100×.

**Why Redis for model storage?**
Pickle-serialising the model to a Redis key (`active_model`) gives the Retraining Worker a single atomic write path. It also means the Inference service survives container restarts without losing the latest trained weights.

---

## Prometheus Metrics

Each service exposes `/health` (JSON) and `/metrics` (Prometheus text) on its own port.

| Service | Key Metrics |
|---|---|
| Producer | `events_produced_total`, `current_phase`, `anomaly_label_rate` |
| Feature Engineer | `events_enriched_total`, `enrichment_latency_seconds` |
| ML Inference | `events_processed_total`, `anomalies_detected_total`, `anomaly_score`, `current_threshold`, `model_version`, `rolling_anomaly_rate` |
| Drift Detector | `drift_events_total{type, feature}`, `time_since_last_drift_seconds` |
| Retraining Worker | `retraining_runs_total`, `retraining_duration_seconds`, `model_promotions_total`, `last_retraining_timestamp` |
| Alert Router | `alerts_fired_total{severity}`, `alerts_deduplicated_total` |

---

## Kafka Topics

| Topic | Producer | Consumer(s) |
|---|---|---|
| `raw-transactions` | Producer | Feature Engineer |
| `enriched-features` | Feature Engineer | ML Inference |
| `anomaly-scores` | ML Inference | Drift Detector, Alert Router |
| `drift-events` | Drift Detector | — (observability) |
| `retraining-triggers` | Drift Detector | Retraining Worker |
| `alerts` | Alert Router | — (downstream sinks) |
| `phase-transitions` | Producer | — (observability) |

---

## Stopping

```bash
docker compose stop          # pause — containers keep their state; restarts instantly
docker compose down          # remove containers — TimescaleDB volume is preserved
docker compose down -v       # full reset — removes containers and all data volumes
```

After `docker compose down` (without `-v`), a subsequent `docker compose up` (no `--build`) starts in seconds since all images are already cached.
