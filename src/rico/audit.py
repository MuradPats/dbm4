"""Stage 5 — Audit: duplicate-detection circuit breaker.

If duplicates are found, writes a failing audit_results row, notifies Slack,
and raises AirflowException (or RuntimeError outside Airflow) to halt the run.
Eval never runs if this task fails.
"""
import json
import logging
import time
from uuid import UUID

from rico.db import get_conn

log = logging.getLogger(__name__)

AUDIT_NAME = "duplicate_detection"


def run_duplicate_audit(run_id: UUID) -> None:
    """Check for duplicate rows in screens_metadata and screens_embeddings for this run.

    Passes silently if clean.
    Raises RuntimeError (caught by Airflow as task failure) if duplicates found.
    Always writes a row to audit_results.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s audit starting audit_name=%s", run_id, AUDIT_NAME)

    duplicates: list[str] = []

    with get_conn() as conn, conn.cursor() as cur:
        # --- Check 1: duplicate screen_id in screens_metadata for this run ---
        cur.execute(
            """
            SELECT screen_id, COUNT(*) AS cnt
            FROM screens_metadata
            WHERE run_id = %s
            GROUP BY screen_id
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        for screen_id, cnt in cur.fetchall():
            msg = f"screens_metadata: screen_id={screen_id} appears {cnt}x"
            duplicates.append(msg)
            log.error("run_id=%s DUPLICATE %s", run_id, msg)

        # --- Check 2: duplicate (screen_id, model_name, model_version, embedding_kind) ---
        cur.execute(
            """
            SELECT screen_id, model_name, model_version, embedding_kind, COUNT(*) AS cnt
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY screen_id, model_name, model_version, embedding_kind
            HAVING COUNT(*) > 1
            """,
            (run_id,),
        )
        for screen_id, model_name, model_version, kind, cnt in cur.fetchall():
            msg = (
                f"screens_embeddings: screen_id={screen_id} "
                f"model={model_name}/{model_version} kind={kind} appears {cnt}x"
            )
            duplicates.append(msg)
            log.error("run_id=%s DUPLICATE %s", run_id, msg)

    passed = len(duplicates) == 0
    details = {"duplicates": duplicates, "duplicate_count": len(duplicates)}
    elapsed = time.perf_counter() - t0

    # Always persist the audit result.
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_results (run_id, audit_name, passed, details)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (run_id, AUDIT_NAME, passed, json.dumps(details)),
        )

    if passed:
        log.info("run_id=%s audit PASSED elapsed=%.1fs", run_id, elapsed)
    else:
        log.error(
            "run_id=%s audit FAILED duplicate_count=%d duplicates=%s elapsed=%.1fs",
            run_id, len(duplicates), duplicates, elapsed,
        )
        # Import here to avoid hard dependency on Airflow outside the DAG context.
        try:
            from airflow.exceptions import AirflowException
            raise AirflowException(
                f"Audit FAILED: {len(duplicates)} duplicate(s) found. "
                f"Details: {duplicates}"
            )
        except ImportError:
            raise RuntimeError(
                f"Audit FAILED: {len(duplicates)} duplicate(s) found. "
                f"Details: {duplicates}"
            )
