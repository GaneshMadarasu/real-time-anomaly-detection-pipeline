-- Enable TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS scored_events (
  timestamp             TIMESTAMPTZ NOT NULL,
  event_id              TEXT,
  user_id               INTEGER,
  amount                FLOAT,
  anomaly_score         FLOAT,
  is_anomaly            BOOLEAN,
  anomaly_label         INTEGER,
  model_version         INTEGER,
  phase                 TEXT,
  drift_injected        BOOLEAN,
  threshold_used        FLOAT,
  risk_score_raw        FLOAT,
  features_json         JSONB
);
SELECT create_hypertable('scored_events', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_scored_events_user_id ON scored_events (user_id, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_scored_events_is_anomaly ON scored_events (is_anomaly, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_scored_events_event_id ON scored_events (event_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS drift_events (
  timestamp   TIMESTAMPTZ NOT NULL,
  drift_type  TEXT,
  feature     TEXT,
  detector    TEXT
);
SELECT create_hypertable('drift_events', 'timestamp', if_not_exists => TRUE);

CREATE TABLE IF NOT EXISTS alerts (
  timestamp     TIMESTAMPTZ NOT NULL,
  event_id      TEXT,
  user_id       INTEGER,
  anomaly_score FLOAT,
  severity      TEXT,
  amount        FLOAT,
  phase         TEXT,
  features_json JSONB
);
SELECT create_hypertable('alerts', 'timestamp', if_not_exists => TRUE);
CREATE INDEX IF NOT EXISTS idx_alerts_severity ON alerts (severity, timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_user_id ON alerts (user_id, timestamp DESC);

CREATE TABLE IF NOT EXISTS model_versions (
  version         INTEGER,
  trained_at      TIMESTAMPTZ NOT NULL,
  trigger_reason  TEXT,
  precision_score FLOAT,
  n_samples       INTEGER
);
SELECT create_hypertable('model_versions', 'trained_at', if_not_exists => TRUE);
