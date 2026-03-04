# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Quick Start

```bash
cd anomaly-detection
docker compose up --build   # starts all 15 services
docker compose down -v      # stop and remove volumes
```

## Architecture

All production code lives in `anomaly-detection/`. It is a 6-service Kafka streaming pipeline:

```
Producer → Feature Engineer (Faust) → ML Inference (River) → anomaly-scores topic
                                                                    ├→ Drift Detector → retraining-triggers
                                                                    │       └→ Retraining Worker (Celery)
                                                                    └→ Alert Router → TimescaleDB / Slack
```

### Services and Ports

| Service            | Port | Tech                | Responsibility                            |
|--------------------|------|---------------------|-------------------------------------------|
| producer           | 8001 | kafka-python        | Synthetic financial event generation       |
| feature-engineer   | 8002 | Faust (faust-streaming) | Sliding-window feature engineering via Redis |
| ml-inference       | 8003 | River (HalfSpaceTrees) | Online scoring, dynamic threshold, model persistence |
| drift-detector     | 8004 | River ADWIN + DDM   | Data drift & concept drift detection       |
| retraining-worker  | 8005 | Celery + River      | Auto-retrain on drift, shadow-test & promote |
| alert-router       | 8006 | kafka-python        | Severity bucketing, dedup, Slack fanout    |

### Infrastructure

- **Kafka** (29092 internal / 9092 external) — 7 topics, 3 partitions each
- **Redis** (6379) — sliding windows, model persistence, dedup TTLs, Celery broker
- **TimescaleDB** (5432) — `scored_events`, `drift_events`, `alerts`, `model_versions`
- **Prometheus** (9090) → **Grafana** (3000) — metrics from all 6 services

### Key Design Decisions

- **Online learning**: River models (`HalfSpaceTrees` + custom SAD) update on every event; no batch training needed at startup.
- **Model persistence**: ml-inference serializes models to Redis key `active_model` every 1000 events; retraining-worker overwrites it on promotion.
- **Retraining subprocess**: `retraining-worker/main.py` spawns a `celery worker` subprocess; the Kafka consumer loop runs in the main process. No subprocess recursion since `celery -A main worker` imports `main` as a module (not `__main__`).
- **Feature-engineer Faust startup**: Dockerfile CMD is `["python", "main.py", "worker", "--loglevel=info"]` — Faust takes the `worker` argument via `app.main()`.
- **Batched DB writes**: All TimescaleDB writes flush every 100 rows OR every 5s via a background thread `BatchWriter` class.
- **Dynamic threshold**: ml-inference auto-adjusts threshold ±0.01 based on rolling anomaly rate (target 0.1%–5%).

### Phase Simulation

Producer cycles: Normal (0–99,999) → Drift (100,000–199,999) → Attack (200,000–249,999) → repeat.
Drift detection typically fires ~1–2 min after phase transition. ADWIN monitors `amount`, `amount_zscore`, `anomaly_score`; DDM monitors prediction error rate.

## Environment Variables (.env)

```
KAFKA_BOOTSTRAP_SERVERS=kafka:29092
REDIS_URL=redis://redis:6379
TIMESCALE_URL=postgresql://anomaly:anomaly@timescaledb:5432/anomalydb
ANOMALY_THRESHOLD=0.7      # starting threshold; auto-adjusted at runtime
REPLAY_SPEED=100           # events/sec from producer
DRIFT_AFTER_N_EVENTS=100000
SLACK_WEBHOOK_URL=         # optional
MIN_SLACK_SEVERITY=HIGH
```

## Kafka Topics

`raw-transactions`, `enriched-features`, `anomaly-scores`, `drift-events`, `alerts`, `retraining-triggers`, `phase-transitions`

## Prometheus Metrics per Service

Each service exposes `/health` (JSON) and `/metrics` (Prometheus) on its port. Key metrics:
- `events_produced_total`, `current_phase`, `anomaly_label_rate` (producer)
- `events_enriched_total`, `enrichment_latency_seconds` (feature-engineer)
- `events_processed_total`, `anomalies_detected_total`, `anomaly_score`, `current_threshold`, `model_version`, `rolling_anomaly_rate` (ml-inference)
- `drift_events_total{type,feature}`, `time_since_last_drift_seconds` (drift-detector)
- `retraining_runs_total`, `retraining_duration_seconds`, `model_promotions_total` (retraining-worker)
- `alerts_fired_total{severity}`, `alerts_deduplicated_total` (alert-router)
