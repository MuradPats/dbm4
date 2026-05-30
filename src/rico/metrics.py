"""Observability: collect pipeline health + data quality metrics, persist, and log summary."""
import json
import logging
import time
from uuid import UUID

from rico.db import get_conn

log = logging.getLogger(__name__)


def _insert_metric(cur, run_id: UUID, name: str, value: float, extra: dict = None) -> None:
    cur.execute(
        """
        INSERT INTO pipeline_metrics (run_id, metric_name, metric_value, metric_json)
        VALUES (%s, %s, %s, %s::jsonb)
        """,
        (run_id, name, value, json.dumps(extra) if extra else None),
    )


def collect_and_persist(run_id: UUID) -> dict:
    """Query all destination tables, compute health + quality metrics, persist to pipeline_metrics.

    Returns the metrics dict (also logged as a one-screen summary).
    """
    t0 = time.perf_counter()
    log.info("run_id=%s metrics collection starting", run_id)

    metrics: dict = {}

    with get_conn() as conn, conn.cursor() as cur:

        # ── screens_metadata ───────────────────────────────────────────────
        cur.execute(
            "SELECT COUNT(*) FROM screens_metadata WHERE run_id = %s", (run_id,)
        )
        meta_total = cur.fetchone()[0]
        metrics["meta_total"] = meta_total

        cur.execute(
            """
            SELECT
                COUNT(*) FILTER (WHERE extraction_payload IS NOT NULL)::float / NULLIF(COUNT(*), 0),
                COUNT(*) FILTER (WHERE confidence >= 0.5)::float / NULLIF(COUNT(*), 0)
            FROM screens_metadata WHERE run_id = %s
            """,
            (run_id,),
        )
        pct_extracted, pct_confident = cur.fetchone()
        metrics["pct_extraction_non_null"] = round(pct_extracted or 0.0, 4)
        metrics["pct_confidence_gte_05"] = round(pct_confident or 0.0, 4)

        cur.execute(
            "SELECT COUNT(*) FROM screens_review_queue WHERE run_id = %s", (run_id,)
        )
        review_count = cur.fetchone()[0]
        metrics["review_queue_count"] = review_count
        metrics["pct_in_review_queue"] = round(review_count / meta_total, 4) if meta_total else 0.0

        cur.execute(
            "SELECT COUNT(DISTINCT app_package), COUNT(DISTINCT category) FROM screens_metadata WHERE run_id = %s",
            (run_id,),
        )
        distinct_packages, distinct_categories = cur.fetchone()
        metrics["distinct_app_packages"] = distinct_packages
        metrics["distinct_categories"] = distinct_categories

        # ── screens_embeddings ─────────────────────────────────────────────
        cur.execute(
            """
            SELECT model_version, embedding_kind, COUNT(*), AVG(vector_dims(vector))
            FROM screens_embeddings
            WHERE run_id = %s
            GROUP BY model_version, embedding_kind
            """,
            (run_id,),
        )
        emb_breakdown = []
        for model_version, kind, cnt, avg_dims in cur.fetchall():
            emb_breakdown.append({
                "model_version": model_version,
                "embedding_kind": kind,
                "count": int(cnt),
                "avg_dims": round(float(avg_dims or 0), 1),
            })
        metrics["embedding_breakdown"] = emb_breakdown

        # Zero-norm vectors are a silent embedder bug.
        cur.execute(
            """
            SELECT COUNT(*) FILTER (WHERE vector_norm(vector) = 0)::float / NULLIF(COUNT(*), 0)
            FROM screens_embeddings WHERE run_id = %s
            """,
            (run_id,),
        )
        pct_zero_norm = cur.fetchone()[0] or 0.0
        metrics["pct_zero_norm_vectors"] = round(pct_zero_norm, 4)

        # ── persist scalars ────────────────────────────────────────────────
        _insert_metric(cur, run_id, "meta_total", meta_total)
        _insert_metric(cur, run_id, "pct_extraction_non_null", metrics["pct_extraction_non_null"])
        _insert_metric(cur, run_id, "pct_confidence_gte_05", metrics["pct_confidence_gte_05"])
        _insert_metric(cur, run_id, "review_queue_count", review_count)
        _insert_metric(cur, run_id, "pct_in_review_queue", metrics["pct_in_review_queue"])
        _insert_metric(cur, run_id, "distinct_app_packages", distinct_packages)
        _insert_metric(cur, run_id, "distinct_categories", distinct_categories)
        _insert_metric(cur, run_id, "pct_zero_norm_vectors", pct_zero_norm)
        _insert_metric(cur, run_id, "embedding_breakdown", None, {"breakdown": emb_breakdown})

    elapsed = time.perf_counter() - t0
    metrics["metrics_collection_elapsed_s"] = round(elapsed, 2)

    _log_summary(run_id, metrics)
    return metrics


def _log_summary(run_id: UUID, m: dict) -> None:
    """Print a one-screen summary readable in ~10 seconds from Airflow logs."""
    breakdown_str = ", ".join(
        f"{e['model_version'].split('/')[-1]}:{e['embedding_kind']}={e['count']}"
        for e in m.get("embedding_breakdown", [])
    )
    log.info(
        "\n"
        "╔══════════════════════════════════════════════════════════╗\n"
        "║  RICO PIPELINE RUN SUMMARY  run_id=%-26s║\n"
        "╠══════════════════════════════════════════════════════════╣\n"
        "║  screens_metadata rows      : %-28s║\n"
        "║  extraction non-null %%      : %-28s║\n"
        "║  confidence >= 0.5 %%        : %-28s║\n"
        "║  in review queue            : %-28s║\n"
        "║  distinct packages          : %-28s║\n"
        "║  distinct categories        : %-28s║\n"
        "║  embeddings                 : %-28s║\n"
        "║  zero-norm vectors %%        : %-28s║\n"
        "╚══════════════════════════════════════════════════════════╝",
        str(run_id)[:26],
        m.get("meta_total", "?"),
        f"{m.get('pct_extraction_non_null', 0)*100:.1f}%",
        f"{m.get('pct_confidence_gte_05', 0)*100:.1f}%",
        f"{m.get('review_queue_count', 0)} ({m.get('pct_in_review_queue', 0)*100:.1f}%)",
        m.get("distinct_app_packages", "?"),
        m.get("distinct_categories", "?"),
        breakdown_str[:28] if breakdown_str else "none",
        f"{m.get('pct_zero_norm_vectors', 0)*100:.2f}%",
    )
