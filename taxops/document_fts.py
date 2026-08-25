"""FTS5 indexing helpers for return_documents extracted text."""
from __future__ import annotations

import json
import logging
import sqlite3
from typing import Any

logger = logging.getLogger(__name__)


def flatten_extracted_fields(fields: dict[str, Any] | None) -> str:
    """Turn extracted_fields JSON into searchable plain text."""
    if not fields:
        return ""
    parts: list[str] = []

    def _walk(obj: Any) -> None:
        if obj is None:
            return
        if isinstance(obj, dict):
            for k, v in obj.items():
                if str(k).startswith("_"):
                    continue
                _walk(v)
        elif isinstance(obj, (list, tuple)):
            for v in obj:
                _walk(v)
        else:
            s = str(obj).strip()
            if s and s.lower() not in ("none", "null", "unknown"):
                parts.append(s)

    _walk(fields)
    return " ".join(parts)


def index_document_text(
    conn: sqlite3.Connection,
    doc_id: int,
    fields: dict[str, Any] | None = None,
    *,
    fields_json: str | None = None,
) -> bool:
    """Insert/replace FTS row for doc_id and set ocr_text_indexed=1.

    Returns True when non-empty text was indexed.
    """
    payload = fields
    if payload is None and fields_json:
        try:
            parsed = json.loads(fields_json)
            payload = parsed if isinstance(parsed, dict) else None
        except (TypeError, json.JSONDecodeError):
            payload = None

    text = flatten_extracted_fields(payload)
    # FTS5 external-content style with content='': delete then insert by rowid=doc_id
    conn.execute("DELETE FROM return_documents_fts WHERE rowid = ?", (doc_id,))
    if text:
        conn.execute(
            "INSERT INTO return_documents_fts(rowid, doc_text) VALUES (?, ?)",
            (doc_id, text),
        )
        conn.execute(
            "UPDATE return_documents SET ocr_text_indexed = 1 WHERE id = ?",
            (doc_id,),
        )
        return True

    conn.execute(
        "UPDATE return_documents SET ocr_text_indexed = 0 WHERE id = ?",
        (doc_id,),
    )
    return False


def search_documents_fts(
    conn: sqlite3.Connection,
    query: str,
    *,
    return_id: int | None = None,
    limit: int = 50,
) -> list[int]:
    """Return matching return_documents ids (rowids) for an FTS query."""
    q = (query or "").strip()
    if not q:
        return []
    try:
        if return_id is not None:
            rows = conn.execute(
                """
                SELECT rd.id
                FROM return_documents_fts
                JOIN return_documents rd ON rd.id = return_documents_fts.rowid
                WHERE return_documents_fts MATCH ?
                  AND rd.return_id = ?
                  AND rd.is_deleted = 0
                  AND rd.ocr_text_indexed = 1
                LIMIT ?
                """,
                (q, return_id, limit),
            ).fetchall()
        else:
            rows = conn.execute(
                """
                SELECT rd.id
                FROM return_documents_fts
                JOIN return_documents rd ON rd.id = return_documents_fts.rowid
                WHERE return_documents_fts MATCH ?
                  AND rd.is_deleted = 0
                  AND rd.ocr_text_indexed = 1
                LIMIT ?
                """,
                (q, limit),
            ).fetchall()
    except sqlite3.OperationalError as exc:
        logger.warning("FTS search failed: %s", exc)
        return []
    return [int(r["id"] if hasattr(r, "keys") else r[0]) for r in rows]
