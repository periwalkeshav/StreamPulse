-- ============================================================================
--  StreamPulse - PostgreSQL schema
--  Applied automatically on first boot (mounted into /docker-entrypoint-initdb.d)
--  and idempotently by `python sql/migrate.py`.
-- ============================================================================

-- ---------------------------------------------------------------- raw events
-- Append-only landing table. The Spark job upserts with ON CONFLICT DO NOTHING
-- so replaying a micro-batch after a failure cannot create duplicates.
CREATE TABLE IF NOT EXISTS raw_events (
    event_id            UUID PRIMARY KEY,
    user_id             TEXT        NOT NULL,
    event_type          TEXT        NOT NULL,
    event_time          TIMESTAMPTZ NOT NULL,
    ingested_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    value               NUMERIC(14, 4),
    currency            TEXT,
    country             TEXT,
    device              TEXT,
    channel             TEXT,
    session_id          TEXT,
    product_id          TEXT,
    latency_ms          NUMERIC(10, 3),
    status              TEXT,
    is_revenue_event    BOOLEAN     NOT NULL DEFAULT FALSE,
    value_bucket        TEXT,
    ingest_lag_seconds  NUMERIC(12, 3),
    kafka_partition     INTEGER,
    kafka_offset        BIGINT
);

CREATE INDEX IF NOT EXISTS idx_raw_events_event_time  ON raw_events (event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_event_type  ON raw_events (event_type);
CREATE INDEX IF NOT EXISTS idx_raw_events_type_time   ON raw_events (event_type, event_time DESC);
CREATE INDEX IF NOT EXISTS idx_raw_events_user        ON raw_events (user_id);
CREATE INDEX IF NOT EXISTS idx_raw_events_ingested_at ON raw_events (ingested_at DESC);

-- ------------------------------------------------------- 5-minute rollups
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    window_start    TIMESTAMPTZ NOT NULL,
    window_end      TIMESTAMPTZ NOT NULL,
    event_type      TEXT        NOT NULL,
    total_events    BIGINT      NOT NULL,
    unique_users    BIGINT      NOT NULL DEFAULT 0,
    avg_value       NUMERIC(14, 4),
    total_value     NUMERIC(18, 4),
    p95_latency_ms  NUMERIC(10, 3),
    max_latency_ms  NUMERIC(10, 3),
    error_count     BIGINT      NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (window_start, window_end, event_type)
);

CREATE INDEX IF NOT EXISTS idx_agg_window_start ON aggregated_metrics (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_agg_event_type   ON aggregated_metrics (event_type);

-- --------------------------------------------------------------- alerts
CREATE TABLE IF NOT EXISTS alerts (
    alert_id     BIGSERIAL PRIMARY KEY,
    event_id     UUID        NOT NULL,
    alert_type   TEXT        NOT NULL,
    severity     TEXT        NOT NULL CHECK (severity IN ('warning', 'critical')),
    event_type   TEXT        NOT NULL,
    user_id      TEXT,
    event_time   TIMESTAMPTZ,
    value        NUMERIC(14, 4),
    z_score      NUMERIC(10, 4),
    message      TEXT,
    detected_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    acknowledged BOOLEAN     NOT NULL DEFAULT FALSE,
    UNIQUE (event_id, alert_type)
);

CREATE INDEX IF NOT EXISTS idx_alerts_detected_at ON alerts (detected_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_severity    ON alerts (severity);
CREATE INDEX IF NOT EXISTS idx_alerts_event_type  ON alerts (event_type);

-- ------------------------------------------------------------ user sessions
CREATE TABLE IF NOT EXISTS user_sessions (
    user_id           TEXT        NOT NULL,
    session_start     TIMESTAMPTZ NOT NULL,
    session_end       TIMESTAMPTZ NOT NULL,
    duration_seconds  BIGINT      NOT NULL,
    event_count       BIGINT      NOT NULL,
    order_count       BIGINT      NOT NULL DEFAULT 0,
    revenue           NUMERIC(18, 4) NOT NULL DEFAULT 0,
    distinct_products BIGINT      NOT NULL DEFAULT 0,
    device            TEXT,
    country           TEXT,
    converted         BOOLEAN     NOT NULL DEFAULT FALSE,
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (user_id, session_start)
);

CREATE INDEX IF NOT EXISTS idx_sessions_start ON user_sessions (session_start DESC);

-- ---------------------------------------------------------- dead letter queue
CREATE TABLE IF NOT EXISTS dlq_events (
    dlq_id           BIGSERIAL PRIMARY KEY,
    error_type       TEXT        NOT NULL,
    original_message TEXT,
    source_partition INTEGER     NOT NULL,
    source_offset    BIGINT      NOT NULL,
    failed_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (source_partition, source_offset)
);

CREATE INDEX IF NOT EXISTS idx_dlq_failed_at  ON dlq_events (failed_at DESC);
CREATE INDEX IF NOT EXISTS idx_dlq_error_type ON dlq_events (error_type);

-- Rolling snapshots written by dlq_monitor.py (one row per error type/window).
CREATE TABLE IF NOT EXISTS dlq_stats (
    window_start  TIMESTAMPTZ NOT NULL,
    window_end    TIMESTAMPTZ NOT NULL,
    error_type    TEXT        NOT NULL,
    message_count BIGINT      NOT NULL,
    PRIMARY KEY (window_start, error_type)
);

-- ------------------------------------------------ rolling anomaly baseline
-- Sufficient statistics (n, sum, sum of squares) per event type. Mean and
-- stddev are derived on read, which makes every micro-batch update a single
-- commutative UPSERT - safe to replay.
CREATE TABLE IF NOT EXISTS baseline_stats (
    event_type   TEXT PRIMARY KEY,
    observations BIGINT           NOT NULL DEFAULT 0,
    sum_value    DOUBLE PRECISION NOT NULL DEFAULT 0,
    sum_sq_value DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at   TIMESTAMPTZ      NOT NULL DEFAULT now()
);

-- ------------------------------------------------------- streaming batch log
CREATE TABLE IF NOT EXISTS batch_log (
    query_name     TEXT        NOT NULL,
    batch_id       BIGINT      NOT NULL,
    rows_valid     BIGINT      NOT NULL DEFAULT 0,
    rows_invalid   BIGINT      NOT NULL DEFAULT 0,
    rows_anomalous BIGINT      NOT NULL DEFAULT 0,
    duration_ms    BIGINT      NOT NULL DEFAULT 0,
    processed_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

CREATE INDEX IF NOT EXISTS idx_batch_log_processed_at ON batch_log (processed_at DESC);

-- --------------------------------------------- Airflow: daily batch outputs
-- `daily_summary` used to be declared here and populated by inline SQL in the
-- daily_aggregation DAG. It is now `analytics.fct_event_metrics_daily`, owned
-- and created by dbt (see dbt/models/marts/). It is deliberately NOT dropped
-- here: an existing deployment keeps its history until an operator removes it.

CREATE TABLE IF NOT EXISTS data_quality_results (
    run_id       BIGSERIAL PRIMARY KEY,
    dag_run_id   TEXT,
    check_name   TEXT        NOT NULL,
    table_name   TEXT        NOT NULL,
    status       TEXT        NOT NULL CHECK (status IN ('pass', 'warn', 'fail')),
    observed     DOUBLE PRECISION,
    threshold    DOUBLE PRECISION,
    details      TEXT,
    checked_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dq_checked_at ON data_quality_results (checked_at DESC);
CREATE INDEX IF NOT EXISTS idx_dq_status     ON data_quality_results (status);

-- ------------------------------------------------------------------- views
-- Throughput per minute, used by the "Events / sec" Grafana panel.
CREATE OR REPLACE VIEW v_events_per_minute AS
SELECT date_trunc('minute', ingested_at) AS minute,
       event_type,
       count(*)              AS events,
       count(*) / 60.0       AS events_per_second
FROM raw_events
GROUP BY 1, 2;

-- Rolling error rate across the last hour of rollups.
CREATE OR REPLACE VIEW v_error_rate AS
SELECT window_start,
       sum(error_count)::numeric / NULLIF(sum(total_events), 0) AS error_rate,
       sum(total_events) AS total_events,
       sum(error_count)  AS error_events
FROM aggregated_metrics
GROUP BY window_start;

-- Pipeline health at a glance - one row, consumed by the Airflow SLA check.
CREATE OR REPLACE VIEW v_pipeline_health AS
SELECT (SELECT count(*) FROM raw_events)                                    AS raw_event_count,
       (SELECT max(ingested_at) FROM raw_events)                            AS last_event_at,
       (SELECT count(*) FROM aggregated_metrics)                            AS rollup_count,
       (SELECT count(*) FROM alerts WHERE detected_at > now() - interval '1 hour') AS alerts_last_hour,
       (SELECT count(*) FROM dlq_events WHERE failed_at > now() - interval '1 hour') AS dlq_last_hour,
       (SELECT count(*) FROM user_sessions)                                 AS session_count;
