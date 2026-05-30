"""SHA-256 fingerprinting for source traceability."""
import hashlib


def sha256_bytes(data: bytes) -> str:
    """Return hex SHA-256 of raw bytes (e.g. PNG file)."""
    return hashlib.sha256(data).hexdigest()


def sha256_text(text: str) -> str:
    """Return hex SHA-256 of a UTF-8 string (e.g. text fed to an embedder)."""
    return hashlib.sha256(text.encode("utf-8")).hexdigest()
