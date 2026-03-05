---
name: infrastructure
description: Docker Compose patterns, Kafka config, TimescaleDB schema and hypertable rules, Prometheus scrape config, Grafana provisioning, Dockerfile patterns, and requirements.txt pinning for this anomaly detection pipeline. Load when modifying infrastructure, docker-compose.yml, db/init.sql, or adding new services.
allowed-tools: Read, Grep, Glob
---

# Infrastructure — Real-Time Anomaly Detection Pipeline

## Docker Compose Patterns

All infrastructure is defined in `anomaly-detection/docker-compose.yml` (version: 3.8).
All services share one bridge network: `anomaly-net`. Only one named volume: `timescale-data`.

### Healthcheck format by service type

**Zookeeper** — TCP port probe (NOT `ruok` four-letter command, which is not whitelisted):
```yaml
healthcheck:
  test: ["CMD", "bash", "-c", "nc -z localhost 2181"]
  interval: 10s
  timeout: 5s
  retries: 5
```

**Kafka** — Kafka API version call (proves broker is responsive):
```yaml
healthcheck:
  test: ["CMD", "kafka-broker-api-versions", "--bootstrap-server", "localhost:29092"]
  interval: 10s
  timeout: 10s
  retries: 10    # more retries — Kafka takes longer to start than Redis
```

**Redis** — Redis PING:
```yaml
healthcheck:
  test: ["CMD", "redis-cli", "ping"]
  interval: 5s
  timeout: 3s
  retries: 5
```

**TimescaleDB** — pg_isready:
```yaml
healthcheck:
  test: ["CMD", "pg_isready", "-U", "anomaly"]
  interval: 5s
  timeout: 3s
  retries: 10    # more retries — DB init script runs on first start
```

**Application services** — Dockerfile-level HEALTHCHECK via curl (not in docker-compose.yml):
```dockerfile
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:<PORT>/health || exit 1
```
`curl` is installed in every service Dockerfile: `apt-get install -y --no-install-recommends curl`.

### depends_on with condition: service_healthy
```yaml
depends_on:
  kafka:
    condition: service_healthy
  redis:
    condition: service_healthy
  timescaledb:
    condition: service_healthy
```

Dependency matrix per service:
| Service | kafka | redis | timescaledb |
|---------|-------|-------|-------------|
| producer | ✓ | ✓ | |
| feature-engineer | ✓ | ✓ | |
| ml-inference | ✓ | ✓ | |
| drift-detector | ✓ | | ✓ |
| retraining-worker | ✓ | ✓ | ✓ |
| alert-router | ✓ | ✓ | ✓ |

Note: `drift-detector` does NOT declare a `redis` dependency in docker-compose, even though
it uses Redis for the retrain cooldown key. Redis is implicitly available when Kafka is up
(they share `anomaly-net`), but there is no health guarantee.

### Environment variable injection
All 6 application services use `env_file: .env` — no inline `environment:` blocks for
application config. Infrastructure services (Kafka, Zookeeper, TimescaleDB, Grafana) use
inline `environment:` blocks only.

### Volume naming convention
- `timescale-data`: named volume for TimescaleDB data persistence (persists across `docker-compose down`; removed with `docker-compose down -v`)
- No named volumes for Redis (ephemeral — all state is lost on restart), Kafka, or application services.

### Port mapping convention
| Service | Internal | External |
|---------|----------|----------|
| Kafka | 29092 | 9092 |
| Schema Registry | 8081 | 8081 |
| Kafka UI | 8080 | 8080 |
| Redis | 6379 | 6379 |
| TimescaleDB | 5432 | 5432 |
| producer | 8001 | 8001 |
| feature-engineer | 8002 | 8002 |
| ml-inference | 8003 | 8003 |
| drift-detector | 8004 | 8004 |
| retraining-worker | 8005 | 8005 |
| alert-router | 8006 | 8006 |
| Prometheus | 9090 | 9090 |
| Grafana | 3000 | 3000 |
| Celery Flower | 5555 | 5555 |

---

## Kafka Configuration

```yaml
KAFKA_BROKER_ID:                       1
KAFKA_ZOOKEEPER_CONNECT:               zookeeper:2181
KAFKA_ADVERTISED_LISTENERS:            PLAINTEXT://kafka:29092,PLAINTEXT_HOST://localhost:9092
KAFKA_LISTENER_SECURITY_PROTOCOL_MAP:  PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
KAFKA_INTER_BROKER_LISTENER_NAME:      PLAINTEXT
KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
KAFKA_AUTO_CREATE_TOPICS_ENABLE:       "true"
KAFKA_NUM_PARTITIONS:                  3
KAFKA_LOG_RETENTION_HOURS:             2
```

- **Two listeners**: `kafka:29092` (internal, used by all services in `anomaly-net`) and
  `localhost:9092` (external, for connecting from the host machine during development).
- **Auto topic creation**: enabled as fallback; the producer service also calls `ensure_topics()`
  at startup to explicitly create all 7 topics with `num_partitions=3, replication_factor=1`.
- **Log retention**: 2 hours (development setting — not suitable for production).
- **Replication factor**: 1 (single broker — no fault tolerance).

---

## TimescaleDB Configuration

### Connection string format
```
postgresql://anomaly:anomaly@timescaledb:5432/anomalydb
```

Used identically in: `TIMESCALE_URL` env var → `psycopg2.connect(TIMESCALE_URL)`.

### init.sql mounting
```yaml
volumes:
  - ./db/init.sql:/docker-entrypoint-initdb.d/init.sql
```

Executed once on first container start (when data volume is empty). Subsequent starts skip it.

### Table structure and hypertable pattern
```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- All tables follow this pattern:
CREATE TABLE IF NOT EXISTS <table_name> (..., timestamp TIMESTAMPTZ NOT NULL, ...);
SELECT create_hypertable('<table_name>', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_<table>_<col> ON <table> (<col>, timestamp DESC);
```

Rule: all indexes on hypertables include `timestamp DESC` as compound key — a UNIQUE INDEX
on any column alone is illegal on a hypertable partitioned by `timestamp`.

### Tables
| Table | Hypertable | Key indexes |
|-------|-----------|-------------|
| `scored_events` | ✓ (by timestamp) | `(user_id, timestamp DESC)`, `(is_anomaly, timestamp DESC)`, `(event_id, timestamp DESC)` |
| `drift_events` | ✓ (by timestamp) | none extra |
| `alerts` | ✓ (by timestamp) | `(severity, timestamp DESC)`, `(user_id, timestamp DESC)` |
| `model_versions` | ✓ (by trained_at) | none extra |

---

## Prometheus Configuration

File: `prometheus/prometheus.yml`, mounted at `/etc/prometheus/prometheus.yml`.

```yaml
global:
  scrape_interval:     15s
  evaluation_interval: 15s

scrape_configs:
  - job_name: 'producer'
    static_configs:
      - targets: ['producer:8001']

  - job_name: 'feature-engineer'
    static_configs:
      - targets: ['feature-engineer:8002']

  - job_name: 'ml-inference'
    static_configs:
      - targets: ['ml-inference:8003']

  - job_name: 'drift-detector'
    static_configs:
      - targets: ['drift-detector:8004']

  - job_name: 'retraining-worker'
    static_configs:
      - targets: ['retraining-worker:8005']

  - job_name: 'alert-router'
    static_configs:
      - targets: ['alert-router:8006']
```

Service discovery: static configs using Docker service names (DNS resolves within `anomaly-net`).
Metrics path: `/metrics` (Prometheus default, no custom path needed).
Retention: `--storage.tsdb.retention.time=7d` (7 days).

To add a new service: add a new `scrape_configs` entry with `job_name` matching the
Docker service name and `targets` matching `service-name:PORT`.

---

## Grafana Provisioning

### Datasources (`grafana/provisioning/datasources/datasources.yml`)
UIDs must be stable — they are referenced in dashboard JSON panel definitions:

```yaml
apiVersion: 1

datasources:
  - name: Prometheus
    type: prometheus
    uid: prometheus          # used in dashboard JSON: "datasource": {"uid": "prometheus"}
    url: http://prometheus:9090
    isDefault: true
    access: proxy
    editable: false
    jsonData:
      httpMethod: POST
      prometheusType: Prometheus

  - name: TimescaleDB
    type: postgres
    uid: timescaledb         # used in dashboard JSON: "datasource": {"uid": "timescaledb"}
    url: timescaledb:5432
    database: anomalydb
    user: anomaly
    secureJsonData:
      password: anomaly
    access: proxy
    editable: false
    jsonData:
      sslmode: disable
      timescaledb: true
```

### Dashboard provider (`grafana/provisioning/dashboards/dashboards.yml`)
```yaml
apiVersion: 1
providers:
  - name: Anomaly Detection
    folder: Anomaly
    type: file
    disableDeletion: false
    updateIntervalSeconds: 30
    allowUiUpdates: true
    options:
      path: /var/lib/grafana/dashboards
```

Dashboard JSON files in `grafana/dashboards/` are mounted to `/var/lib/grafana/dashboards/`.
Grafana polls for changes every 30 seconds.

### How to add a new dashboard
1. Create `grafana/dashboards/my_dashboard.json`
2. In JSON, reference datasources by UID: `"datasource": {"type": "prometheus", "uid": "prometheus"}`
3. Grafana picks it up within 30 seconds (no restart needed)

---

## Dockerfile Pattern

All 6 service Dockerfiles are identical except for port number and CMD:

```dockerfile
FROM python:3.11-slim
WORKDIR /app
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY main.py .
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
  CMD curl -f http://localhost:<PORT>/health || exit 1
CMD ["python", "main.py"]
```

Exception: `feature-engineer` CMD:
```dockerfile
CMD ["python", "main.py", "worker", "--loglevel=info"]
```

Layer order: `apt install` → `requirements.txt` → `main.py`. This ensures pip dependencies
are cached across `main.py` changes.

---

## requirements.txt Pattern

Each service pins all dependencies to exact versions. No version ranges or minimum-version
constraints are used. Order within files is not alphabetical — dependencies appear to be
listed in rough dependency order (framework first, then infrastructure clients, then tooling).

| Package | Version | Services |
|---------|---------|---------|
| `kafka-python` | 2.0.2 | all 6 |
| `prometheus-client` | 0.17.1 | all 6 |
| `redis` | 5.0.1 | feature-engineer, ml-inference, drift-detector, retraining-worker, alert-router |
| `river` | 0.21.0 | ml-inference, drift-detector, retraining-worker |
| `psycopg2-binary` | 2.9.9 | drift-detector, retraining-worker, alert-router |
| `faust-streaming` | 0.11.3 | feature-engineer only |
| `aiokafka` | 0.10.0 | feature-engineer only (pinned for faust compatibility) |
| `celery` | 5.3.4 | retraining-worker only |
| `flower` | 2.0.1 | retraining-worker only |
| `requests` | 2.31.0 | alert-router only |
| `aiohttp` | 3.9.1 | feature-engineer only |
| `numpy` | 1.26.0 | feature-engineer only (unused — dead dependency) |
