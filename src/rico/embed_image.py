"""Stage 3a — Embed image: CLIP image embeddings → screens_embeddings."""
import io
import logging
import time
from uuid import UUID

import numpy as np
import open_clip
import torch
from PIL import Image

from rico.db import get_conn
from rico.fingerprint import sha256_bytes
from rico.minio_client import get_s3, get_object

log = logging.getLogger(__name__)

CLIP_ARCH = "ViT-B-32"
CLIP_PRETRAINED = "laion2b_s34b_b79k"
MODEL_NAME = "open-clip"
MODEL_VERSION = f"open-clip-{CLIP_ARCH}-{CLIP_PRETRAINED.replace('_', '-')}"


def embed_images(run_id: UUID, screen_ids: list[int]) -> None:
    """Embed each screen PNG with CLIP and upsert into screens_embeddings.

    Idempotent: ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO NOTHING.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s embed_image starting screens=%d model=%s", run_id, len(screen_ids), MODEL_VERSION)

    model, _, preprocess = open_clip.create_model_and_transforms(CLIP_ARCH, pretrained=CLIP_PRETRAINED)
    model.eval()

    s3 = get_s3()
    rows_written = 0

    for sid in screen_ids:
        png_bytes = get_object(s3, f"screens/{sid}.png")
        fingerprint = sha256_bytes(png_bytes)

        img = Image.open(io.BytesIO(png_bytes)).convert("RGB")
        tensor = preprocess(img).unsqueeze(0)  # (1, C, H, W)

        with torch.no_grad():
            vec = model.encode_image(tensor)
            vec = vec / vec.norm(dim=-1, keepdim=True)  # L2 normalise

        vec_np: np.ndarray = vec.squeeze(0).cpu().numpy().astype("float32")

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO screens_embeddings
                    (screen_id, model_name, model_version, embedding_kind, vector,
                     run_id, source_fingerprint)
                VALUES (%s, %s, %s, 'image', %s, %s, %s)
                ON CONFLICT (screen_id, model_name, model_version, embedding_kind) DO UPDATE SET
                    run_id             = EXCLUDED.run_id,
                    source_fingerprint = EXCLUDED.source_fingerprint
                """,
                (sid, MODEL_NAME, MODEL_VERSION, vec_np.tolist(), run_id, fingerprint),
            )
        rows_written += 1
        log.info("run_id=%s embed_image screen_id=%d dims=%d", run_id, sid, vec_np.shape[0])

    elapsed = time.perf_counter() - t0
    log.info("run_id=%s embed_image done rows_out=%d elapsed=%.1fs", run_id, rows_written, elapsed)
