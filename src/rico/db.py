"""Postgres connection and pipeline_runs lifecycle helpers."""
import logging
import os
import subprocess
from datetime import datetime, timezone
from typing import Optional
from uuid import UUID

import psycopg
from pgvector.psycopg import register_vector

log = logging.getLogger(__name__)

POSTGRES_DSN = os.environ["POSTGRES_DSN"]


def get_conn() -> psycopg.Connection:
    """Open and return a psycopg3 connection with pgvector registered."""
    conn = psycopg.connect(POSTGRES_DSN)
    register_vector(conn)
    return conn


def _git_sha() -> Optional[str]:
    try:
        return subprocess.check_output(
            ["git", "rev-parse", "--short", "HEAD"],
            stderr=subprocess.DEVNULL,
        ).decode().strip()
    except Exception:
        return None


def register_run(
    dag_run_id: str,
    limit_param: int,
    clip_version: str,
    sbert_version: str,
    llm_model: str,
    prompt_version: str,
) -> UUID:
    """Insert a pipeline_runs row (or return existing run_id for this dag_run_id).

    Idempotent: if dag_run_id already exists, returns the existing run_id.
    """
    git_sha = _git_sha()
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO pipeline_runs
                (dag_run_id, limit_param, git_sha,
                 clip_version, sbert_version, llm_model, prompt_version)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_run_id) DO UPDATE
                SET status = EXCLUDED.status   -- no-op update to return the row
            RETURNING run_id
            """,
            (dag_run_id, limit_param, git_sha,
             clip_version, sbert_version, llm_model, prompt_version),
        )
        run_id: UUID = cur.fetchone()[0]
    log.info("run_id=%s dag_run_id=%s registered", run_id, dag_run_id)
    return run_id


def close_run(run_id: UUID, status: str) -> None:
    """Set ended_at and status on a pipeline_runs row."""
    assert status in ("succeeded", "failed", "paused-by-audit"), f"bad status: {status}"
    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE pipeline_runs SET ended_at = %s, status = %s WHERE run_id = %s",
            (datetime.now(timezone.utc), status, run_id),
        )
    log.info("run_id=%s closed with status=%s", run_id, status)
