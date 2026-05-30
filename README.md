# RICO Pipeline — From Notebook to Production

A scheduled, idempotent, auditable Airflow DAG that re-implements the RICO multimodal pipeline from the lab notebook, adding row-level traceability, duplicate-detection auditing, observability metrics, and Slack notifications.

---

## Prerequisites

- Docker Desktop (or Docker Engine + Compose)
- `conda` (for the local Python environment used in development)
- ~5 GB free disk space (Ollama model + CLIP weights + Docker images)

---

## Quick Start

```bash
# 1. Start Postgres, MinIO, Ollama
make up

# 2. Start Airflow (first run takes ~2 min to pull the image)
make airflow-up

# 3. Open the Airflow UI
open http://localhost:8080   # admin / admin

# 4. Trigger a dev run (2 screens, fast)
docker compose exec airflow-scheduler \
  airflow dags trigger rico_pipeline --conf '{"LIMIT": 2}'

# 5. Trigger a full demo run (50 screens)
docker compose exec airflow-scheduler \
  airflow dags trigger rico_pipeline --conf '{"LIMIT": 50}'
```

Or trigger from the UI: click **rico_pipeline → ▶ Trigger DAG w/ config** and enter `{"LIMIT": 5}`.

---

## Architecture

```
ingest → parse → embed_image ─┐
                 embed_text  ──┼→ load → audit → eval
                 extract     ─┘
```

| Stage | Module | What it does |
|---|---|---|
| `ingest` | `rico/ingest.py` | Streams screens from HuggingFace, PUTs PNG + JSON to MinIO, upserts `screens_metadata` |
| `parse` | `rico/parse.py` | Fetches hierarchy JSON from MinIO, extracts text representation via DFS |
| `embed_image` | `rico/embed_image.py` | CLIP (ViT-B-32) image embeddings → `screens_embeddings` |
| `embed_text` | `rico/embed_text.py` | SBERT (all-MiniLM-L6-v2) text embeddings → `screens_embeddings` |
| `extract` | `rico/extract.py` | Ollama (qwen2.5:3b) structured JSON extraction → `screens_metadata` |
| `load` | `rico/load.py` | Verifies expected row counts; raises if any upstream task missed rows |
| `audit` | `rico/audit.py` | Duplicate-detection circuit breaker — halts the run if duplicates found |
| `eval` | `rico/eval.py` | recall@5 self-test + metrics collection + Slack notification |

The three middle tasks (`embed_image`, `embed_text`, `extract`) run in parallel.

---

## Idempotency

Re-triggering the DAG with the same `LIMIT` produces no new rows:

- `screens_metadata`: `INSERT ... ON CONFLICT (screen_id) DO UPDATE SET updated_at = NOW()`
- `screens_embeddings`: `INSERT ... ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING`
- `pipeline_runs`: `INSERT ... ON CONFLICT (dag_run_id) DO UPDATE SET status = ...`
- MinIO PUTs: same key = same bytes, safe to overwrite

Verify after re-triggering:
```sql
SELECT COUNT(*) FROM screens_metadata;     -- same as before
SELECT COUNT(*) FROM screens_embeddings;   -- same as before
SELECT COUNT(*) FROM pipeline_runs;        -- one row per dag_run_id
```

---

## Traceability

Every destination row carries:

- `run_id` — UUID foreign key to `pipeline_runs`
- `source_fingerprint` — SHA-256 of the bytes fed to that stage

**Given a `run_id`, find everything it produced:**
```sql
-- All screens from a run
SELECT screen_id, app_package, source_fingerprint
FROM screens_metadata WHERE run_id = '<run_id>';

-- All embeddings from a run
SELECT screen_id, model_name, model_version, embedding_kind
FROM screens_embeddings WHERE run_id = '<run_id>';

-- Which model versions were used
SELECT clip_version, sbert_version, llm_model, prompt_version, git_sha
FROM pipeline_runs WHERE run_id = '<run_id>';
```

**Given a row, find its run:**
```sql
SELECT r.run_id, r.started_at, r.status, r.git_sha
FROM screens_metadata m
JOIN pipeline_runs r ON r.run_id = m.run_id
WHERE m.screen_id = 2;
```

---

## Audit

The `audit` task is a **circuit breaker**: if it finds duplicate `(screen_id, model_name, model_version, embedding_kind)` combinations in `screens_embeddings`, or duplicate `screen_id` in `screens_metadata`, it:

1. Logs all duplicate keys in full
2. Writes a failing row to `audit_results`
3. Posts a Slack alert
4. Raises `AirflowException` — the `eval` task never runs

**View audit history:**
```sql
SELECT run_id, audit_name, passed, details, created_at
FROM audit_results
ORDER BY created_at DESC;
```

**Deliberately trigger the circuit breaker (for testing):**
```sql
-- Drop the PK temporarily and insert a duplicate
ALTER TABLE screens_embeddings DROP CONSTRAINT screens_embeddings_pkey;
INSERT INTO screens_embeddings (screen_id, model_name, model_version, embedding_kind, vector, run_id)
SELECT screen_id, model_name, model_version, embedding_kind, vector, run_id
FROM screens_embeddings LIMIT 1;

-- Re-trigger the DAG — audit task will fail, eval will be skipped
-- Restore the constraint afterwards:
ALTER TABLE screens_embeddings
  ADD PRIMARY KEY (screen_id, model_name, model_version, embedding_kind);
```

**Interpreting an audit failure:**
- The `audit` task log shows the exact duplicate keys
- `audit_results.details` JSONB has the full list — query it with `SELECT details FROM audit_results WHERE passed = false ORDER BY created_at DESC LIMIT 1`
- The Slack alert includes the duplicate keys and a link to the task log
- `pipeline_runs.status` is set to `paused-by-audit`
- The `eval` task is skipped — bad data is contained

---

## Observability Metrics

At the end of every successful run, a one-screen summary is logged to the `eval` task and persisted to `pipeline_metrics`.

**View metrics for a run:**
```sql
SELECT metric_name, metric_value, metric_json
FROM pipeline_metrics
WHERE run_id = '<run_id>'
ORDER BY metric_name;
```

**Plot a metric across runs:**
```sql
SELECT r.started_at, m.metric_value
FROM pipeline_metrics m
JOIN pipeline_runs r ON r.run_id = m.run_id
WHERE m.metric_name = 'recall_at_5'
ORDER BY r.started_at;
```

### What each metric means

| Metric | Meaning | Healthy value |
|---|---|---|
| `meta_total` | Rows written to `screens_metadata` | Equals `LIMIT` |
| `pct_extraction_non_null` | Fraction of screens with a non-null LLM payload | Close to 1.0 |
| `pct_confidence_gte_05` | Fraction of screens where the LLM was confident | > 0.5 |
| `review_queue_count` | Screens flagged for human review (low confidence) | Low; investigate if high |
| `pct_in_review_queue` | Same as above as a fraction | < 0.2 is fine |
| `distinct_app_packages` | Distinct apps in this run | Should be > 1 for real runs |
| `distinct_categories` | Distinct categories | Should be > 1 for real runs |
| `embedding_breakdown` | Count + avg dims per model/kind | Dims must be constant (512 CLIP, 384 SBERT) |
| `pct_zero_norm_vectors` | Fraction of embeddings with L2 norm = 0 | Must be 0.0 — any nonzero is an embedder bug |
| `recall_at_5` | Self-test retrieval recall | 1.0 for small runs (tautology); use as a regression check |

---

## Slack Notifications

Set the webhook URL before starting Airflow:
```bash
export SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
make airflow-up
```

Or add it to a `.env` file (never commit this file — it is in `.gitignore`):
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

Three messages are posted per run: run started, audit failed (only if audit fails), run finished. A failure to post to Slack does not fail the pipeline.

---

## Resetting State

```bash
# Wipe all pipeline data (keeps Ollama model + Docker volumes intact)
make reset

# Full reset including Docker volumes (re-pulls Ollama model on next start)
make clean && make up && make airflow-up
```

---

## Project Layout

```
dags/
  rico_pipeline.py        # Thin DAG — orchestration only, no business logic
src/rico/
  db.py                   # Postgres connection, pipeline_runs lifecycle
  minio_client.py         # MinIO / S3 helpers
  fingerprint.py          # SHA-256 fingerprinting
  notify.py               # Slack notifications (never raises)
  ingest.py               # Stage 1: HuggingFace → MinIO + screens_metadata
  parse.py                # Stage 2: hierarchy JSON → text representation
  embed_image.py          # Stage 3a: CLIP image embeddings
  embed_text.py           # Stage 3b: SBERT text embeddings
  extract.py              # Stage 3c: Ollama LLM extraction
  load.py                 # Stage 4: row count verification
  audit.py                # Stage 5: duplicate-detection circuit breaker
  eval.py                 # Stage 6: recall@5 + screens_eval row
  metrics.py              # Observability: collect, persist, log summary
migrations/
  001_init.sql            # Base schema
  002_traceability.sql    # pipeline_runs, audit_results, pipeline_metrics + new columns
prompts/
  v1.txt                  # Versioned LLM prompt (tracked in git)
```
