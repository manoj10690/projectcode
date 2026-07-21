-- ============================================================================
-- Metadata Store schema (Cloud SQL for PostgreSQL)
-- Maps to Chapter 4.2 "Metadata Database Schema Design"
-- Run this once against the ingest_metadata database created by Terraform:
--   psql "host=<PUBLIC_IP> dbname=ingest_metadata user=ingest_app" -f 01_schema.sql
-- ============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto"; -- for gen_random_uuid()

-- ----------------------------------------------------------------------------
-- Table A: pipelines
-- The control panel: one row = one configured ingestion pipeline.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipelines (
    pipeline_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name            VARCHAR(100) NOT NULL,
    description     TEXT,
    source_type     VARCHAR(30)  NOT NULL,   -- POSTGRES | API | FILE | KAFKA | PUBSUB
    source_config   JSONB        NOT NULL,   -- connection string / bucket / endpoint
    sink_config     JSONB        NOT NULL,   -- target BQ dataset.table etc.
    evolution_policy VARCHAR(20) NOT NULL DEFAULT 'ALLOW_ADDITIONS'
                        CHECK (evolution_policy IN ('ALLOW_ADDITIONS', 'STRICT', 'IGNORE')),
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_by      VARCHAR(100),
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- Table B: schema_ledger
-- Versioned schema definitions per pipeline -- supports schema drift tracking.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS schema_ledger (
    schema_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    version_number  INT  NOT NULL,
    schema_json     JSONB NOT NULL,   -- {"field": "TYPE", ...}
    active_status   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (pipeline_id, version_number)
);

CREATE INDEX IF NOT EXISTS idx_schema_ledger_active
    ON schema_ledger (pipeline_id) WHERE active_status = TRUE;

-- ----------------------------------------------------------------------------
-- Table C: dq_rules
-- Declarative, column-level data quality constraints tied to a schema version.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_rules (
    rule_id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    schema_id       UUID NOT NULL REFERENCES schema_ledger(schema_id) ON DELETE CASCADE,
    target_column   VARCHAR(50) NOT NULL,
    constraint_type VARCHAR(20) NOT NULL
                        CHECK (constraint_type IN ('NOT_NULL', 'UNIQUE', 'REGEX_MATCH',
                                                    'VALUE_MIN', 'VALUE_MAX', 'ALLOWED_VALUES')),
    constraint_value TEXT,             -- regex pattern / numeric bound / comma list
    threshold_pct   NUMERIC(5,2) NOT NULL DEFAULT 0.00,  -- max % of rows allowed to fail
    severity        VARCHAR(10) NOT NULL DEFAULT 'FAIL' CHECK (severity IN ('WARN', 'FAIL')),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- ----------------------------------------------------------------------------
-- pipeline_runs -- one row per Dataflow / Beam execution
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    pipeline_id     UUID NOT NULL REFERENCES pipelines(pipeline_id) ON DELETE CASCADE,
    schema_id       UUID REFERENCES schema_ledger(schema_id),
    dataflow_job_id VARCHAR(100),
    status          VARCHAR(20) NOT NULL DEFAULT 'RUNNING'
                        CHECK (status IN ('RUNNING', 'SUCCEEDED', 'FAILED', 'QUARANTINED_PARTIAL')),
    rows_read       BIGINT DEFAULT 0,
    rows_written    BIGINT DEFAULT 0,
    rows_quarantined BIGINT DEFAULT 0,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ
);

-- ----------------------------------------------------------------------------
-- dq_alerts -- individual rule-violation / anomaly events raised during a run
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS dq_alerts (
    alert_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_id          UUID NOT NULL REFERENCES pipeline_runs(run_id) ON DELETE CASCADE,
    rule_id         UUID REFERENCES dq_rules(rule_id),
    alert_source    VARCHAR(30) NOT NULL DEFAULT 'DETERMINISTIC_GATE'
                        CHECK (alert_source IN ('DETERMINISTIC_GATE', 'VERTEX_AI_ANOMALY_MODEL',
                                                 'INFRASTRUCTURE_FAIL_SAFE')),
    failed_row_count BIGINT NOT NULL DEFAULT 0,
    sample_payload  JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_runs_pipeline ON pipeline_runs (pipeline_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_alerts_run ON dq_alerts (run_id);
