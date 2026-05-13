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


def normalize_staff_question_key(question: str) -> str:
    """Normalize for answer-cache keys (lowercase, collapse whitespace, light contractions)."""
    q = (question or "").lower().strip()
    q = q.replace("how's", "how is")
    q = q.replace("what's", "what is")
    q = q.replace("who's", "who is")
    q = q.replace("don't", "do not")
    q = q.replace("haven't", "have not")
    q = q.replace("hasn't", "has not")
    q = re.sub(r"\s+", " ", q)
    return q


def get_return_documents_path(return_id: int) -> str:
    from config import DOCUMENTS_BASE_PATH
    folder = os.path.join(DOCUMENTS_BASE_PATH, "returns", str(return_id))
    os.makedirs(folder, exist_ok=True)
    return folder


def get_drake_documents_path(
    return_id: int, client_last_name: str, tax_year: str
) -> str | None:
    """
    Get or create the Drake-compatible folder path for a return's documents.
    Returns None if DRAKE_DOCUMENTS_BASE is not configured or
    DRAKE_FOLDER_STRUCTURE_ENABLED is False.

    Drake organizes by: TaxYear / LastName_ReturnID /
    This mirrors that structure so future sync is trivial.

    Never includes SSN, EIN, or identification numbers in path.
    """
    from config import DRAKE_DOCUMENTS_BASE, DRAKE_FOLDER_STRUCTURE_ENABLED

    if not DRAKE_FOLDER_STRUCTURE_ENABLED or not DRAKE_DOCUMENTS_BASE:
        return None

    # Sanitize last name for folder name — no special chars
    safe_last = re.sub(r"[^A-Za-z0-9]", "_", (client_last_name or "UNKNOWN").upper())
    safe_last = safe_last[:30]

    yr = str(tax_year).strip() if tax_year not in (None, "") else "unknown_year"
    base = os.path.abspath(DRAKE_DOCUMENTS_BASE)
    folder = os.path.join(base, yr, f"{safe_last}_{return_id}")
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
