"""Stage 6 — Eval: recall@5 self-test using SBERT text embeddings."""
import logging
import time
from uuid import UUID

import numpy as np

from rico.db import get_conn

log = logging.getLogger(__name__)

SBERT_MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"
SBERT_MODEL_NAME = "sbert"


def run_eval(run_id: UUID) -> float:
    """Compute recall@5 using a self-test: each screen queries with its own vector.

    A self-test is a tautology (the answer is always in the index) but confirms
    that vectors are stored correctly and retrieval works end-to-end.

    Returns recall@5 and writes a row to screens_eval + pipeline_metrics.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s eval starting", run_id)

    with get_conn() as conn, conn.cursor() as cur:
        # Load all text vectors for this run.
        cur.execute(
            """
            SELECT screen_id, vector
            FROM screens_embeddings
            WHERE run_id = %s
              AND model_name = %s
              AND model_version = %s
              AND embedding_kind = 'text'
            """,
            (run_id, SBERT_MODEL_NAME, SBERT_MODEL_VERSION),
        )
        rows = cur.fetchall()

    if not rows:
        log.warning("run_id=%s eval no text vectors found — skipping", run_id)
        return 0.0

    screen_ids = [r[0] for r in rows]
    # pgvector returns vectors as lists; convert to numpy array.
    vecs = np.array([list(r[1]) for r in rows], dtype="float32")
    n = len(screen_ids)
    k = min(5, n)

    hits = 0
    for i, (sid, qvec) in enumerate(zip(screen_ids, vecs)):
        # Cosine similarity = dot product (vectors are L2-normalised).
        scores = vecs @ qvec
        top_k = np.argsort(scores)[::-1][:k]
        if i in top_k:
            hits += 1

    recall_at_5 = hits / n
    elapsed = time.perf_counter() - t0
    log.info(
        "run_id=%s eval done recall@5=%.3f n=%d elapsed=%.1fs",
        run_id, recall_at_5, n, elapsed,
    )

    with get_conn() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO screens_eval
                (embedding_model_version, n_queries, recall_at_5, run_id)
            VALUES (%s, %s, %s, %s)
            """,
            (SBERT_MODEL_VERSION, n, recall_at_5, run_id),
        )
        cur.execute(
            """
            INSERT INTO pipeline_metrics (run_id, metric_name, metric_value)
            VALUES (%s, 'recall_at_5', %s)
            """,
            (run_id, recall_at_5),
        )

    return recall_at_5
