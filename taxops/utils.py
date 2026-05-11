from __future__ import annotations

import hashlib
import os
import re
from dataclasses import dataclass
from datetime import datetime, timezone


def parse_iso_datetime(value: str | None) -> datetime | None:
    """
    Parse ISO-8601 timestamps stored from now() / email ingest.
    Returns None if the string is missing or unparsable.
    """
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def hash_file(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def safe_str(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def get_return_documents_path(return_id: int) -> str:
    from config import DOCUMENTS_BASE_PATH
    folder = os.path.join(DOCUMENTS_BASE_PATH, "returns", str(return_id))
    os.makedirs(folder, exist_ok=True)
    return folder


def sanitize_filename(filename: str) -> str:
    name, ext = os.path.splitext(filename)
    name = re.sub(r"[^a-zA-Z0-9]", "_", name)
    return f"{name[:60]}{ext.lower()}"


def _scrub_ssn_from_string(value: str) -> str:
    if not isinstance(value, str):
        return value
    # Formatted: 123-45-6789
    value = re.sub(r"\b\d{3}-\d{2}-\d{4}\b", "[REDACTED]", value)
    # Spaced: 123 45 6789
    value = re.sub(r"\b\d{3}\s\d{2}\s\d{4}\b", "[REDACTED]", value)
    # Unformatted 9-digit: 123456789 — only redact if standalone
    value = re.sub(r"\b\d{9}\b", "[REDACTED]", value)
    # Already-masked SSN — e.g. XXX-XX-7527 (common on W-2 PDFs)
    value = re.sub(r"\bXXX-XX-\d{4}\b", "[REDACTED]", value, flags=re.I)
    # Asterisk-masked SSN — e.g. ***-**-7527
    value = re.sub(r"\*{3}-\*{2}-\d{4}\b", "[REDACTED]", value)
    # Hash-masked SSN — e.g. ###-##-7527
    value = re.sub(r"#{3}-#{2}-\d{4}\b", "[REDACTED]", value)
    return value


_SSN_FIELD_NAMES = {
    "ssn", "ssn_last4", "social_security", "social_security_number",
    "taxpayer_id", "tin", "ein", "itin", "identification_number",
    "id_number", "tax_id", "primary_ssn", "spouse_ssn",
    "dependent_ssn", "social", "ssn_full",
}


def scrub_ssn_from_dict(data: dict) -> dict:
    """
    PRIVACY ENFORCEMENT — SSN SCRUBBING (rules.md §14)

    This function MUST be called on every dict returned by any LLM
    document extraction or OCR processing path before that data is:
      - returned to the frontend
      - stored in any database column
      - written to any log
      - included in any API response

    Failure to call this function on LLM-extracted document data
    is a privacy violation. There are no exceptions.

    Enforced in: ai_routes.py on all /ai/documents/* extract routes
    Also enforced in: mail_watcher.py (DOC-3) on email attachment processing
    """
    if not isinstance(data, dict):
        return {}
    result = {}
    for key, value in data.items():
        if key.lower().strip() in _SSN_FIELD_NAMES:
            continue
        if isinstance(value, str):
            value = _scrub_ssn_from_string(value)
        elif isinstance(value, dict):
            value = scrub_ssn_from_dict(value)
        elif isinstance(value, list):
            value = [
                _scrub_ssn_from_string(v) if isinstance(v, str)
                else scrub_ssn_from_dict(v) if isinstance(v, dict)
                else v
                for v in value
            ]
        result[key] = value
    return result


def _enqueue_extraction(doc_id: int, return_id: int) -> None:
    """Queue a newly saved document for background extraction."""
    import logging

    log = logging.getLogger(__name__)
    try:
        from db import get_connection

        cq = get_connection()
        try:
            if cq.execute(
                "SELECT 1 FROM extraction_queue WHERE doc_id = ? LIMIT 1",
                (doc_id,),
            ).fetchone():
                return
            cq.execute(
                """
                INSERT INTO extraction_queue
                    (doc_id, return_id, status, attempts, created_at)
                VALUES (?, ?, 'pending', 0, ?)
                """,
                (doc_id, return_id, now()),
            )
            cq.commit()
        finally:
            cq.close()
    except Exception as e:
        log.error("Failed to enqueue doc %s: %s", doc_id, e)


@dataclass
class ImportStats:
    row_count: int = 0
    success_count: int = 0
    error_count: int = 0
    review_count: int = 0
    created_clients: int = 0
    updated_clients: int = 0
    created_returns: int = 0
    updated_returns: int = 0
    events_created: int = 0
    notes_created: int = 0
