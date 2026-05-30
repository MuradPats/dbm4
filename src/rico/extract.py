"""Stage 3c — Extract: LLM structured JSON extraction via Ollama → screens_metadata."""
import json
import logging
import os
import time
from pathlib import Path
from uuid import UUID

import requests

from rico.db import get_conn

log = logging.getLogger(__name__)

OLLAMA_URL = os.environ.get("OLLAMA_URL", "http://localhost:11434")
OLLAMA_MODEL = os.environ.get("OLLAMA_MODEL", "qwen2.5:3b")
PROMPT_VERSION = "v1"

# Load the versioned prompt template from the prompts/ directory.
_PROMPT_PATH = Path(__file__).parent.parent.parent / "prompts" / f"{PROMPT_VERSION}.txt"


def _load_prompt() -> str:
    if _PROMPT_PATH.exists():
        return _PROMPT_PATH.read_text()
    # Fallback inline prompt (identical to v1.txt) so tests pass without the file.
    return """\
You are a UI structure extractor for Android app screenshots.

Given the visible text from one screen's view hierarchy, return a single
JSON object with these fields:

- "title": a short string naming the screen (e.g. "Login", "Settings")
- "primary_action": the main action available (e.g. "Submit", "Search", "None")
- "element_count": integer count of meaningful UI elements visible
- "confidence": float 0–1 reflecting how confident you are in the extraction

Return ONLY the JSON object, no explanation, no markdown fences.
"""


def extract_one(text_rep: str) -> dict:
    """Call Ollama to extract structured JSON from a single screen's text representation."""
    prompt = _load_prompt()
    payload = {
        "model": OLLAMA_MODEL,
        "prompt": f"{prompt}\n\nScreen text:\n{text_rep}",
        "stream": False,
    }
    try:
        resp = requests.post(f"{OLLAMA_URL}/api/generate", json=payload, timeout=120)
        resp.raise_for_status()
        raw = resp.json().get("response", "")
        # Strip markdown fences if the model wraps in ```json ... ```
        raw = raw.strip().removeprefix("```json").removeprefix("```").removesuffix("```").strip()
        return json.loads(raw)
    except (json.JSONDecodeError, requests.RequestException) as exc:
        log.warning("extract_one failed: %s — returning empty payload", exc)
        return {"title": "", "primary_action": "", "element_count": 0, "confidence": 0.0}


def extract_screens(run_id: UUID, text_reps: dict[int, str]) -> None:
    """Run LLM extraction on each screen and UPDATE screens_metadata.

    Idempotent: UPDATE always overwrites — running twice with the same model just re-writes
    the same payload (deterministic for the same input + model).
    Screens with low confidence (<0.5) are added to screens_review_queue.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s extract starting screens=%d model=%s", run_id, len(text_reps), OLLAMA_MODEL)

    rows_updated = 0
    rows_queued = 0

    for sid, text_rep in text_reps.items():
        started = time.perf_counter()
        payload = extract_one(text_rep)
        elapsed_one = time.perf_counter() - started

        confidence = float(payload.get("confidence", 0.0))
        log.info(
            "run_id=%s extract screen_id=%d confidence=%.2f elapsed=%.1fs",
            run_id, sid, confidence, elapsed_one,
        )

        with get_conn() as conn, conn.cursor() as cur:
            cur.execute(
                """
                UPDATE screens_metadata
                SET extraction_payload = %s::jsonb,
                    prompt_version     = %s,
                    confidence         = %s,
                    updated_at         = NOW()
                WHERE screen_id = %s
                """,
                (json.dumps(payload), PROMPT_VERSION, confidence, sid),
            )
            rows_updated += 1

            if confidence < 0.5:
                cur.execute(
                    """
                    INSERT INTO screens_review_queue (screen_id, reason, raw_output, run_id)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (sid, f"low_confidence:{confidence:.2f}", json.dumps(payload), run_id),
                )
                rows_queued += 1
                log.warning("run_id=%s extract screen_id=%d queued for review", run_id, sid)

    elapsed = time.perf_counter() - t0
    log.info(
        "run_id=%s extract done rows_updated=%d rows_queued=%d elapsed=%.1fs",
        run_id, rows_updated, rows_queued, elapsed,
    )
