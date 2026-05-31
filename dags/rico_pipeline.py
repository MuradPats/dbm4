"""RICO multimodal pipeline DAG.

Shape: ingest → parse → [embed_image, embed_text, extract] → load → audit → eval

The DAG file is intentionally thin — no business logic here.
All logic lives in src/rico/*.py.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from airflow.decorators import dag, task
from airflow.models.param import Param

log = logging.getLogger(__name__)

# Model version constants (single source of truth for the DAG).
CLIP_VERSION = "open-clip-ViT-B-32-laion2b_s34b_b79k"
SBERT_VERSION = "sentence-transformers/all-MiniLM-L6-v2"
PROMPT_VERSION = "v1"


@dag(
    dag_id="rico_pipeline",
    description="RICO multimodal pipeline: ingest → embed → extract → audit → eval",
    schedule="@daily",
    start_date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    catchup=False,
    max_active_runs=1,
    default_args={
        "retries": 1,
        "retry_delay": timedelta(minutes=2),
        "owner": "rico",
    },
    params={
        "LIMIT": Param(5, type="integer", minimum=1, description="Number of screens to process"),
    },
    tags=["rico", "multimodal"],
)
def rico_pipeline():

    @task
    def ingest(**context) -> list[int]:
        import os
        from rico.db import register_run
        from rico.ingest import ingest_screens
        from rico.notify import notify_run_started

        limit = context["params"]["LIMIT"]
        dag_run_id = context["run_id"]
        ollama_model = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")

        run_id = register_run(
            dag_run_id=dag_run_id,
            limit_param=limit,
            clip_version=CLIP_VERSION,
            sbert_version=SBERT_VERSION,
            llm_model=ollama_model,
            prompt_version=PROMPT_VERSION,
        )
        # Stash run_id as string in XCom (UUID isn't XCom-serializable directly).
        context["task_instance"].xcom_push(key="run_id", value=str(run_id))

        trigger = context.get("dag_run").run_type or "manual"
        notify_run_started(str(run_id), dag_run_id, limit, trigger)

        screen_ids = ingest_screens(run_id, limit=limit)
        return screen_ids

    @task
    def parse(screen_ids: list[int], **context) -> dict:
        from uuid import UUID
        from rico.parse import parse_screens

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        text_reps = parse_screens(run_id, screen_ids)
        # XCom can only hold JSON-serialisable values; dict[int, str] needs int keys as str.
        return {str(k): v for k, v in text_reps.items()}

    @task
    def embed_image(screen_ids: list[int], **context) -> None:
        from uuid import UUID
        from rico.embed_image import embed_images

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        embed_images(run_id, screen_ids)

    @task
    def embed_text(text_reps_str: dict, **context) -> None:
        from uuid import UUID
        from rico.embed_text import embed_texts

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        # Restore int keys.
        text_reps = {int(k): v for k, v in text_reps_str.items()}
        embed_texts(run_id, text_reps)

    @task
    def extract(text_reps_str: dict, **context) -> None:
        from uuid import UUID
        from rico.extract import extract_screens

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        text_reps = {int(k): v for k, v in text_reps_str.items()}
        extract_screens(run_id, text_reps)

    @task
    def load(screen_ids: list[int], **context) -> None:
        from uuid import UUID
        from rico.load import load_verify

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        load_verify(run_id, screen_ids)

    @task
    def audit(**context) -> None:
        from uuid import UUID
        from rico.audit import run_duplicate_audit
        from rico.db import close_run
        from rico.notify import notify_audit_failed
        from rico.db import get_conn

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        dag_run_id = context["run_id"]
        try:
            run_duplicate_audit(run_id)
        except Exception as exc:
            # Extract duplicate list from exception message for Slack.
            close_run(run_id, "paused-by-audit")
            notify_audit_failed(str(run_id), dag_run_id, [str(exc)])
            
            # Persist health metrics for the failed/paused run
            try:
                with get_conn() as conn, conn.cursor() as cur:
                    cur.execute(
                        "SELECT EXTRACT(EPOCH FROM (NOW() - started_at)) FROM pipeline_runs WHERE run_id = %s",
                        (run_id,),
                    )
                    duration_s = float(cur.fetchone()[0] or 0)
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                        VALUES (%s, 'run_total_duration', %s)
                        """,
                        (run_id, duration_s),
                    )
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_json)
                        VALUES (%s, 'run_status', 0.0, '{"status": "paused-by-audit"}'::jsonb)
                        """,
                        (run_id,),
                    )
            except Exception as metric_exc:
                log.warning("Failed to persist failed run health metrics (non-fatal): %s", metric_exc)

            raise

    @task
    def eval(**context) -> None:
        from uuid import UUID
        from rico.eval import run_eval
        from rico.metrics import collect_and_persist
        from rico.db import close_run, get_conn
        from rico.notify import notify_run_finished
        import time

        run_id = UUID(context["task_instance"].xcom_pull(key="run_id", task_ids="ingest"))
        dag_run_id = context["run_id"]

        recall = run_eval(run_id)
        metrics = collect_and_persist(run_id)
        close_run(run_id, "succeeded")

        summary = (
            f"recall@5={recall:.3f} | "
            f"screens={metrics.get('meta_total', '?')} | "
            f"extracted={metrics.get('pct_extraction_non_null', 0)*100:.0f}% | "
            f"review_queue={metrics.get('review_queue_count', '?')}"
        )
        
        # Collect per-task durations, retries, and statuses from Airflow's execution context.
        try:
            dag_run = context["dag_run"]
            task_instances = dag_run.get_task_instances()
            
            with get_conn() as conn, conn.cursor() as cur:
                # Get total run duration
                cur.execute(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - started_at)) FROM pipeline_runs WHERE run_id = %s",
                    (run_id,),
                )
                duration_s = float(cur.fetchone()[0] or 0)

                # Persist total run duration and final status
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                    VALUES (%s, 'run_total_duration', %s)
                    """,
                    (run_id, duration_s),
                )
                cur.execute(
                    """
                    INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_json)
                    VALUES (%s, 'run_status', 1.0, '{"status": "succeeded"}'::jsonb)
                    """,
                    (run_id,),
                )

                # Persist per-task durations and retries
                for ti in task_instances:
                    ti_duration = ti.duration if ti.duration is not None else 0.0
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                        VALUES (%s, %s, %s)
                        """,
                        (run_id, f"task_duration_{ti.task_id}", float(ti_duration)),
                    )
                    
                    ti_retries = float(max(0, ti.try_number - 1))
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                        VALUES (%s, %s, %s)
                        """,
                        (run_id, f"task_retries_{ti.task_id}", ti_retries),
                    )

                # Persist per-task row counts in/out flows
                meta_total = float(metrics.get("meta_total", 0))
                row_flows = {
                    "ingest": (0.0, meta_total),
                    "parse": (meta_total, meta_total),
                    "embed_image": (meta_total, meta_total),
                    "embed_text": (meta_total, meta_total),
                    "extract": (meta_total, meta_total),
                    "load": (meta_total, meta_total),
                    "audit": (meta_total, meta_total),
                    "eval": (meta_total, 1.0),
                }
                
                for task_id, (rows_in, rows_out) in row_flows.items():
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                        VALUES (%s, %s, %s)
                        """,
                        (run_id, f"task_rows_in_{task_id}", rows_in),
                    )
                    cur.execute(
                        """
                        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
                        VALUES (%s, %s, %s)
                        """,
                        (run_id, f"task_rows_out_{task_id}", rows_out),
                    )
        except Exception as health_exc:
            log.warning("Failed to persist successful run health metrics (non-fatal): %s", health_exc)

        # Get total run duration again if needed, or use duration_s from block
        try:
            with get_conn() as conn, conn.cursor() as cur:
                cur.execute(
                    "SELECT EXTRACT(EPOCH FROM (NOW() - started_at)) FROM pipeline_runs WHERE run_id = %s",
                    (run_id,),
                )
                duration_s = float(cur.fetchone()[0] or 0)
        except Exception:
            duration_s = 0.0

        notify_run_finished(str(run_id), dag_run_id, "succeeded", duration_s, summary)

    # ── Wire the DAG ──────────────────────────────────────────────────────
    screen_ids = ingest()
    text_reps = parse(screen_ids)

    img_task = embed_image(screen_ids)
    txt_task = embed_text(text_reps)
    ext_task = extract(text_reps)

    load_task = load(screen_ids)
    [img_task, txt_task, ext_task] >> load_task

    audit_task = audit()
    load_task >> audit_task

    eval_task = eval()
    audit_task >> eval_task


rico_pipeline()
