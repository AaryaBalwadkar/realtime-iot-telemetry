-- =====================================================================
-- IoT Sensor Data Processing - PostgreSQL Schema Initialization
-- =====================================================================
-- This script is auto-executed by the official postgres image on first
-- container startup (mounted at /docker-entrypoint-initdb.d/init.sql).
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS iot;

SET search_path TO iot, public;

-- ---------------------------------------------------------------------
-- Table: anomaly_events
-- Stores individual threshold-breach anomaly events detected by the
-- Spark Structured Streaming job (foreachBatch sink).
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.anomaly_events (
    id                  BIGSERIAL PRIMARY KEY,
    device_id           VARCHAR(64)      NOT NULL,
    event_timestamp     TIMESTAMPTZ      NOT NULL,
    temperature         DOUBLE PRECISION,
    humidity            DOUBLE PRECISION,
    pressure            DOUBLE PRECISION,
    vibration           DOUBLE PRECISION,
    anomaly_type        VARCHAR(64)      NOT NULL DEFAULT 'ANOMALY_DETECTED',
    temp_threshold      DOUBLE PRECISION,
    vibration_threshold DOUBLE PRECISION,
    severity            VARCHAR(16)      NOT NULL DEFAULT 'HIGH',
    batch_id            BIGINT,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_device_id
    ON iot.anomaly_events (device_id);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_event_timestamp
    ON iot.anomaly_events (event_timestamp DESC);

CREATE INDEX IF NOT EXISTS idx_anomaly_events_anomaly_type
    ON iot.anomaly_events (anomaly_type);

-- ---------------------------------------------------------------------
-- Table: aggregated_metrics
-- Stores windowed (30s / 1m) rolling aggregations of temperature and
-- vibration per device, computed via Spark Structured Streaming
-- window() aggregations.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.aggregated_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    device_id           VARCHAR(64)      NOT NULL,
    window_start        TIMESTAMPTZ      NOT NULL,
    window_end          TIMESTAMPTZ      NOT NULL,
    window_duration     VARCHAR(16)      NOT NULL,
    avg_temperature     DOUBLE PRECISION,
    max_temperature     DOUBLE PRECISION,
    min_temperature     DOUBLE PRECISION,
    avg_humidity        DOUBLE PRECISION,
    avg_pressure        DOUBLE PRECISION,
    avg_vibration       DOUBLE PRECISION,
    max_vibration       DOUBLE PRECISION,
    reading_count       BIGINT           NOT NULL DEFAULT 0,
    batch_id            BIGINT,
    ingested_at         TIMESTAMPTZ      NOT NULL DEFAULT now(),
    CONSTRAINT uq_aggregated_metrics_window UNIQUE (device_id, window_start, window_end, window_duration)
);

CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_device_id
    ON iot.aggregated_metrics (device_id);

CREATE INDEX IF NOT EXISTS idx_aggregated_metrics_window_start
    ON iot.aggregated_metrics (window_start DESC);

-- ---------------------------------------------------------------------
-- Table: device_registry
-- Optional metadata table describing simulated devices; useful for
-- Grafana joins / dashboard variable population.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.device_registry (
    device_id       VARCHAR(64) PRIMARY KEY,
    device_type     VARCHAR(64) NOT NULL DEFAULT 'generic-sensor',
    location        VARCHAR(128),
    first_seen      TIMESTAMPTZ NOT NULL DEFAULT now(),
    last_seen       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- Table: raw_batch_manifest
-- Tracks each raw telemetry batch archived to MinIO (S3) so the
-- object-store archive can be cross-referenced from Postgres/Grafana.
-- ---------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS iot.raw_batch_manifest (
    id              BIGSERIAL PRIMARY KEY,
    batch_id        BIGINT           NOT NULL,
    s3_bucket       VARCHAR(128)     NOT NULL,
    s3_key          VARCHAR(512)     NOT NULL,
    record_count    BIGINT           NOT NULL DEFAULT 0,
    file_format     VARCHAR(16)      NOT NULL DEFAULT 'json',
    written_at      TIMESTAMPTZ      NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_raw_batch_manifest_batch_id
    ON iot.raw_batch_manifest (batch_id);

-- ---------------------------------------------------------------------
-- Convenience view: recent anomalies (last 24h) for dashboards
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW iot.recent_anomalies AS
SELECT
    device_id,
    event_timestamp,
    temperature,
    vibration,
    anomaly_type,
    severity
FROM iot.anomaly_events
WHERE event_timestamp >= (now() - INTERVAL '24 hours')
ORDER BY event_timestamp DESC;

-- ---------------------------------------------------------------------
-- Convenience view: per-device anomaly counts
-- ---------------------------------------------------------------------
CREATE OR REPLACE VIEW iot.anomaly_counts_by_device AS
SELECT
    device_id,
    COUNT(*) AS anomaly_count,
    MAX(event_timestamp) AS last_anomaly_at
FROM iot.anomaly_events
GROUP BY device_id
ORDER BY anomaly_count DESC;

-- ---------------------------------------------------------------------
-- Grants (in case a dedicated read-only Grafana role is added later)
-- ---------------------------------------------------------------------
GRANT USAGE ON SCHEMA iot TO iot_user;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA iot TO iot_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA iot TO iot_user;

ALTER ROLE iot_user SET search_path TO iot, public;
