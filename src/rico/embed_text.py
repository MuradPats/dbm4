"""Stage 3b — Embed text: SBERT text embeddings → screens_embeddings."""
import logging
import time
from uuid import UUID

import numpy as np
from sentence_transformers import SentenceTransformer

from rico.db import get_conn
from rico.fingerprint import sha256_text

log = logging.getLogger(__name__)

MODEL_NAME = "sbert"
MODEL_VERSION = "sentence-transformers/all-MiniLM-L6-v2"


def embed_texts(run_id: UUID, text_reps: dict[int, str]) -> None:
    """Embed text representations with SBERT and upsert into screens_embeddings.

    Idempotent: ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s embed_text starting screens=%d model=%s", run_id, len(text_reps), MODEL_VERSION)

    sbert = SentenceTransformer(MODEL_VERSION)

    screen_ids = list(text_reps.keys())
    corpus = [text_reps[sid] for sid in screen_ids]

    vecs: np.ndarray = sbert.encode(corpus, normalize_embeddings=True).astype("float32")

    rows_written = 0
    for sid, vec in zip(screen_ids, vecs):
        fingerprint = sha256_text(text_reps[sid])
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO screens_embeddings
                    (screen_id, model_name, model_version, embedding_kind, vector,
                     run_id, source_fingerprint)
                VALUES (%s, %s, %s, 'text', %s, %s, %s)
                ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING
                """,
                (sid, MODEL_NAME, MODEL_VERSION, vec.tolist(), run_id, fingerprint),
            )
        rows_written += 1
        log.info("run_id=%s embed_text screen_id=%d dims=%d", run_id, sid, vec.shape[0])

    elapsed = time.perf_counter() - t0
    log.info("run_id=%s embed_text done rows_out=%d elapsed=%.1fs", run_id, rows_written, elapsed)
