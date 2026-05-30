-- Migration 002: Row-level traceability, audit history, and observability metrics.
-- Runs automatically on first `make up` if the data volume is fresh.
-- For an existing volume: apply manually with:
--   docker compose exec postgres psql -U rico -d rico -f /docker-entrypoint-initdb.d/002_traceability.sql

\c rico

-- ────────────────────────────────────────────────────────────────────────────
-- 1. pipeline_runs — one row per DAG run; every destination row points here.
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id      TEXT NOT NULL UNIQUE,          -- Airflow's run_id string
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'running'
                        CHECK (status IN ('running', 'succeeded', 'failed', 'paused-by-audit')),
    limit_param     INTEGER NOT NULL,              -- LIMIT value used for this run
    git_sha         TEXT,                          -- commit hash of the running code
    clip_version    TEXT,                          -- e.g. open-clip-ViT-B-32-laion2b_s34b_b79k
    sbert_version   TEXT,                          -- e.g. sentence-transformers/all-MiniLM-L6-v2
    llm_model       TEXT,                          -- e.g. qwen2.5:3b
    prompt_version  TEXT,                          -- e.g. v1
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 2. audit_results — one row per audit check per run (queryable history).
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS audit_results (
    id          BIGSERIAL PRIMARY KEY,
    run_id      UUID NOT NULL REFERENCES pipeline_runs(run_id),
    audit_name  TEXT NOT NULL,
    passed      BOOLEAN NOT NULL,
    details     JSONB,                             -- duplicate keys, counts, etc.
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- ────────────────────────────────────────────────────────────────────────────
-- 3. pipeline_metrics — one row per metric per run (plottable across runs).
-- ────────────────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS pipeline_metrics (
    id           BIGSERIAL PRIMARY KEY,
    run_id       UUID NOT NULL REFERENCES pipeline_runs(run_id),
    metric_name  TEXT NOT NULL,
    metric_value DOUBLE PRECISION,                 -- scalar value for easy querying/plotting
    metric_json  JSONB,                            -- richer payload when a scalar isn't enough
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_pipeline_metrics_run_id
    ON pipeline_metrics(run_id);

-- ────────────────────────────────────────────────────────────────────────────
-- 4. Extend existing tables with run_id + source_fingerprint.
--    ADD COLUMN IF NOT EXISTS is idempotent — safe to re-run.
-- ────────────────────────────────────────────────────────────────────────────

-- screens_metadata
ALTER TABLE screens_metadata
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;   -- SHA-256 of the PNG bytes

-- screens_embeddings
ALTER TABLE screens_embeddings
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;   -- SHA-256 of the input fed to the embedder

-- screens_review_queue
ALTER TABLE screens_review_queue
    ADD COLUMN IF NOT EXISTS run_id             UUID REFERENCES pipeline_runs(run_id),
    ADD COLUMN IF NOT EXISTS source_fingerprint TEXT;

-- screens_eval (lightweight; just tag which run produced the eval row)
ALTER TABLE screens_eval
    ADD COLUMN IF NOT EXISTS run_id UUID REFERENCES pipeline_runs(run_id);

-- ────────────────────────────────────────────────────────────────────────────
-- 5. Indexes that make the traceability queries fast.
-- ────────────────────────────────────────────────────────────────────────────
CREATE INDEX IF NOT EXISTS idx_screens_metadata_run_id
    ON screens_metadata(run_id);

CREATE INDEX IF NOT EXISTS idx_screens_embeddings_run_id
    ON screens_embeddings(run_id);

CREATE INDEX IF NOT EXISTS idx_screens_review_queue_run_id
    ON screens_review_queue(run_id);

CREATE INDEX IF NOT EXISTS idx_audit_results_run_id
    ON audit_results(run_id);
