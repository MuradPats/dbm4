"""Stage 1 — Ingest: stream RICO screens from HuggingFace, PUT to MinIO, upsert screens_metadata."""
import io
import itertools
import logging
import time
from uuid import UUID

from datasets import load_dataset

from rico.db import get_conn
from rico.fingerprint import sha256_bytes
from rico.minio_client import get_s3, put_object

log = logging.getLogger(__name__)

DATASET_NAME = "rootsautomation/RICO-Screen2Words"
CLIP_VERSION = "open-clip-ViT-B-32-laion2b_s34b_b79k"
SBERT_VERSION = "sentence-transformers/all-MiniLM-L6-v2"


def ingest_screens(run_id: UUID, limit: int) -> list[int]:
    """Stream `limit` screens from HuggingFace, store in MinIO and screens_metadata.

    Returns the list of screen_ids ingested (for downstream tasks to consume via XCom).
    Idempotent: uses INSERT ... ON CONFLICT DO UPDATE so re-runs don't duplicate rows.
    MinIO PUTs are idempotent by key — same bytes overwrite cleanly.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s ingest starting limit=%d", run_id, limit)

    s3 = get_s3()

    ds = load_dataset(DATASET_NAME, split="train", streaming=True, trust_remote_code=True)

    ingested_ids: list[int] = []
    rows_written = 0

    for row in itertools.islice(ds, limit):
        screen_id = int(row["screenId"])
        app_package = row.get("app_package_name", "")
        category = row.get("category", "")

        # --- PNG ---
        img = row["image"]
        buf = io.BytesIO()
        img.save(buf, format="PNG")
        png_bytes = buf.getvalue()

        png_key = f"screens/{screen_id}.png"
        put_object(s3, png_key, png_bytes, content_type="image/png")
        fingerprint = sha256_bytes(png_bytes)

        # --- Hierarchy JSON ---
        hierarchy_raw: str = row.get("ui_metadata", "{}")
        if not isinstance(hierarchy_raw, str):
            import json
            hierarchy_raw = json.dumps(hierarchy_raw)
        json_key = f"screens/{screen_id}.json"
        put_object(s3, json_key, hierarchy_raw.encode("utf-8"), content_type="application/json")

        # --- Postgres upsert ---
        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO screens_metadata
                    (screen_id, app_package, category, png_path, hierarchy_json_path,
                     run_id, source_fingerprint)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (screen_id) DO UPDATE SET
                    run_id             = EXCLUDED.run_id,
                    source_fingerprint = EXCLUDED.source_fingerprint,
                    updated_at         = NOW()
                """,
                (screen_id, app_package, category, png_key, json_key,
                 run_id, fingerprint),
            )
        ingested_ids.append(screen_id)
        rows_written += 1
        log.info("run_id=%s ingest screen_id=%d fingerprint=%s", run_id, screen_id, fingerprint[:12])

    elapsed = time.perf_counter() - t0
    log.info("run_id=%s ingest done rows_out=%d elapsed=%.1fs", run_id, rows_written, elapsed)
    return ingested_ids
