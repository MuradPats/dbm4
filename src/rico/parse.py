"""Stage 2 — Parse: read hierarchy JSON from MinIO, extract text representations."""
import json
import logging
import time
from uuid import UUID

from rico.minio_client import get_s3, get_object

log = logging.getLogger(__name__)


def parse_hierarchy(raw_json: str) -> list[tuple[str, str, tuple]]:
    """Iterative DFS over RICO view hierarchy.

    Returns list of (element_type, text, bounds) for nodes with non-empty text or class.
    RICO wraps the real tree in {"activity": {"root": ...}}; unwrap if present.
    """
    tree = json.loads(raw_json)
    root = tree.get("activity", {}).get("root", tree) if isinstance(tree, dict) else None
    if root is None:
        return []

    elements: list[tuple[str, str, tuple]] = []
    stack = [root]
    while stack:
        node = stack.pop()
        if not isinstance(node, dict):
            continue
        text = (node.get("text") or "").strip()
        cls = (node.get("class") or "").strip()
        bounds = tuple(node.get("bounds", []))
        if text or cls:
            elements.append((cls, text, bounds))
        for child in reversed(node.get("children", []) or []):
            stack.append(child)
    return elements


def text_representation(elements: list[tuple[str, str, tuple]]) -> str:
    """Concatenate element texts in reading order into a single string."""
    parts = []
    for cls, text, _ in elements:
        if text:
            parts.append(text)
        elif cls:
            parts.append(f"[{cls.split('.')[-1]}]")
    return " | ".join(parts)


def parse_screens(run_id: UUID, screen_ids: list[int]) -> dict[int, str]:
    """Fetch each screen's hierarchy JSON from MinIO and return {screen_id: text_repr}.

    The text_repr is what gets fed to SBERT and the LLM.
    """
    t0 = time.perf_counter()
    log.info("run_id=%s parse starting screens=%d", run_id, len(screen_ids))

    s3 = get_s3()
    text_reps: dict[int, str] = {}

    for sid in screen_ids:
        raw = get_object(s3, f"screens/{sid}.json").decode("utf-8")
        elements = parse_hierarchy(raw)
        text_reps[sid] = text_representation(elements)
        log.debug("run_id=%s parse screen_id=%d elements=%d", run_id, sid, len(elements))

    elapsed = time.perf_counter() - t0
    log.info("run_id=%s parse done screens=%d elapsed=%.1fs", run_id, len(text_reps), elapsed)
    return text_reps
