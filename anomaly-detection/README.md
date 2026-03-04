# Real-Time Anomaly Detection Pipeline

A streaming ML anomaly detection system for financial transactions.
Generates synthetic data, engineers features in real-time, scores with online learning,
detects drift, auto-retrains, and fires severity-bucketed alerts — all with one command.

## System Requirements

- Docker Engine 24+
- Docker Compose v2 (`docker compose` not `docker-compose`)
- 8 GB RAM minimum (16 GB recommended)
- 4 CPU cores recommended

## Quick Start

```bash
git clone <repo>
cd real-time-anomaly-detection-pipeline/anomaly-detection
docker compose up --build
```

Wait ~60–90 seconds for all services to initialize, then open the dashboards below.

## Service URLs

| Service        | URL                          | Credentials  |
|----------------|------------------------------|--------------|
| Grafana        | http://localhost:3000        | admin / admin |
| Prometheus     | http://localhost:9090        | —            |
| Kafka UI       | http://localhost:8080        | —            |
| Celery Flower  | http://localhost:5555        | —            |
| Schema Registry| http://localhost:8081        | —            |

## Architecture

```
                          ┌─────────────────────────────────────────────────┐
                          │                 Kafka (29092)                   │
                          │  raw-transactions    enriched-features          │
                          │  anomaly-scores      drift-events               │
                          │  alerts              retraining-triggers        │
                          │  phase-transitions                              │
                          └────────────┬────────────────────────────────────┘
                                       │
          ┌────────────────────────────┼────────────────────────────────────┐
          │                            │                                    │
    ┌─────▼──────┐            ┌────────▼──────────┐              ┌─────────▼──────┐
    │  Producer  │            │ Feature Engineer  │              │  ML Inference  │
    │  :8001     │──raw──────▶│  (Faust)  :8002   │──enriched──▶│  (River) :8003 │
    │ 100 evt/s  │            │  Redis windows    │              │  HalfSpaceTrees│
    └────────────┘            └───────────────────┘              └────────┬───────┘
          │                                                               │
          │ phase-transitions                                     anomaly-scores
          │                                                               │
          │                              ┌────────────────────┐          │
          │                              │  Drift Detector    │◀─────────┤
          │                              │  ADWIN + DDM :8004 │          │
          │                              └────────────────────┘          │
          │                                      │                       │
          │                              retraining-triggers     anomaly-scores
          │                                      │                       │
          │                      ┌───────────────▼──────┐      ┌────────▼───────┐
          │                      │  Retraining Worker   │      │  Alert Router  │
          │                      │  Celery + River:8005 │      │  :8006         │
          │                      │  TimescaleDB fetch   │      │  Dedup + Slack │
          │                      └──────────────────────┘      └────────────────┘
          │                                                              │
          └─────────────────────────────────────────────────────────────┘
                                                                         │
                          ┌──────────────────────────────────────────────▼──┐
                          │          TimescaleDB  (5432)                    │
                          │  scored_events  drift_events  alerts            │
                          │  model_versions                                 │
                          └─────────────────────────────────────────────────┘
                                         ▲
                          ┌──────────────┴────────────┐
                          │  Prometheus (9090)         │
                          │  Grafana    (3000)         │
                          └───────────────────────────┘
```

## Phase Timeline

The producer cycles through three phases automatically:

| Time      | Events        | Phase   | Anomaly Rate | What to Expect                          |
|-----------|---------------|---------|--------------|------------------------------------------|
| t=0       | 0             | —       | —            | Services start, Kafka topics created     |
| t=30s     | ~3,000        | Normal  | ~1–2%        | Pipeline flowing at 100 evt/s            |
| t=2min    | ~12,000       | Normal  | ~1–2%        | Scores visible, rare anomaly alerts      |
| t=16min   | 100,000       | **Drift** | ~5–8%      | ADWIN fires on `amount`, DDM on errors   |
| t=17min   | ~107,000      | Drift   | ~5–8%        | Retraining trigger sent, Celery job starts|
| t=20min   | ~120,000      | Drift   | ~5–8%        | New model version live in Grafana        |
| t=33min   | 200,000       | **Attack** | ~15–20%  | HIGH/CRITICAL alerts spike               |
| t=41min   | 250,000       | Normal  | ~1–2%        | Loop resets, downward drift detected     |

## Verifying the Pipeline Is Working

1. **Kafka UI** (`:8080`) → Topics → `raw-transactions` should show messages flowing
2. **Grafana** (`:3000`) → "Real-Time Anomaly Detection" dashboard → stat panels should show non-zero values within 2 minutes
3. **Prometheus** (`:9090`) → query `events_processed_total` should be increasing
4. **Celery Flower** (`:5555`) → after ~16 minutes a `retrain_model` task should appear
5. **TimescaleDB** → connect with any Postgres client to `localhost:5432` (user: `anomaly`, pass: `anomaly`, db: `anomalydb`) and query `SELECT COUNT(*) FROM scored_events`

## Tuning

### ANOMALY_THRESHOLD

Controls the starting score threshold (0–1). The system auto-adjusts dynamically
between 0.50 and 0.95 based on rolling anomaly rate.

```bash
# Edit .env before starting:
ANOMALY_THRESHOLD=0.65   # more sensitive (catches more, more false positives)
ANOMALY_THRESHOLD=0.80   # less sensitive (fewer alerts, may miss anomalies)
```

### REPLAY_SPEED

Events generated per second. Higher speed reaches phase transitions faster.

```bash
REPLAY_SPEED=50    # slower, more realistic
REPLAY_SPEED=200   # 2x speed, reaches drift phase in ~8 minutes
REPLAY_SPEED=500   # 5x speed, drift in ~3 minutes (may strain CPU)
```

### Slack Alerts

```bash
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/YOUR/WEBHOOK/URL
MIN_SLACK_SEVERITY=HIGH   # Only send HIGH and CRITICAL to Slack
```

## Stopping

```bash
docker compose down          # stops containers, keeps TimescaleDB data
docker compose down -v       # stops containers AND removes all data volumes
```
