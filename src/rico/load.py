"""Stage 4 — Load: verify row counts and confirm the run is ready for audit."""
import logging
import time
from uuid import UUID

from rico.db import get_conn

log = logging.getLogger(__name__)


def load_verify(run_id: UUID, screen_ids: list[int]) -> dict:
    """Verify expected row counts in all destination tables for this run.

    Raises if any table has fewer rows than expected (indicates a task failure upstream).
    Returns a summary dict that is pushed to XCom for the audit task.
    """
    t0 = time.perf_counter()
    expected = len(screen_ids)
    log.info("run_id=%s load_verify starting expected_screens=%d", run_id, expected)

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT COUNT(*) FROM screens_metadata WHERE run_id = %s", (run_id,)
        )
        metadata_count = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM screens_embeddings WHERE run_id = %s AND embedding_kind = 'image'",
            (run_id,),
        )
        image_emb_count = cur.fetchone()[0]

        cur.execute(
            "SELECT COUNT(*) FROM screens_embeddings WHERE run_id = %s AND embedding_kind = 'text'",
            (run_id,),
        )
        text_emb_count = cur.fetchone()[0]

    issues = []
    if metadata_count < expected:
        issues.append(f"screens_metadata: {metadata_count} < {expected}")
    if image_emb_count < expected:
        issues.append(f"image embeddings: {image_emb_count} < {expected}")
    if text_emb_count < expected:
        issues.append(f"text embeddings: {text_emb_count} < {expected}")

    if issues:
        raise RuntimeError(f"run_id={run_id} load_verify FAILED: {issues}")

    elapsed = time.perf_counter() - t0
    summary = {
        "metadata_count": metadata_count,
        "image_emb_count": image_emb_count,
        "text_emb_count": text_emb_count,
    }
    log.info("run_id=%s load_verify OK %s elapsed=%.1fs", run_id, summary, elapsed)
    return summary
