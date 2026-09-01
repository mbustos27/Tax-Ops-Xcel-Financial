"""
email_inbox_link.py
~~~~~~~~~~~~~~~~~~~
When a client's taxpayer_email is set (intake, profile, or Drake promote),
assign matching unassigned email_inbox attachments to the client's return —
same outcome as staff assigning from /email-inbox.

Taxpayer email only (not spouse_email). Never stores email body.
"""
from __future__ import annotations

import logging
import os
import shutil
import sqlite3
from dataclasses import dataclass, field
from typing import Optional

from utils import _enqueue_extraction, get_return_documents_path, now, sanitize_filename

_log = logging.getLogger(__name__)

MATCH_METHOD = "email_client_address"


@dataclass
class InboxLinkStats:
    assigned: int = 0
    skipped_no_return: int = 0
    errors: int = 0
    item_ids: list[int] = field(default_factory=list)


def _most_recent_return(conn: sqlite3.Connection, client_id: int) -> Optional[int]:
    row = conn.execute(
        "SELECT id FROM returns WHERE client_id=? ORDER BY tax_year DESC, id DESC LIMIT 1",
        (client_id,),
    ).fetchone()
    return int(row["id"]) if row else None


def invalidate_suggestions_for_email(conn: sqlite3.Connection, email: str) -> int:
    """Clear cached inbox suggestions so exact-match can re-run."""
    email = (email or "").strip()
    if not email:
        return 0
    cur = conn.execute(
        """
        UPDATE email_inbox
        SET suggested_return_id = NULL,
            suggestion_method = NULL,
            suggestion_score = NULL
        WHERE is_assigned = 0 AND is_deleted = 0
          AND lower(trim(sender_email)) = lower(?)
        """,
        (email,),
    )
    return cur.rowcount


def assign_inbox_item_to_return(
    conn: sqlite3.Connection,
    *,
    inbox_row: sqlite3.Row,
    return_id: int,
    assigned_by: str,
    match_method: str = MATCH_METHOD,
    match_score: Optional[int] = None,
) -> int:
    """
    Copy inbox attachment to return folder and insert return_documents row.
    Returns new return_documents.id.
    """
    dest_dir = get_return_documents_path(return_id)
    os.makedirs(dest_dir, exist_ok=True)

    src_path = inbox_row["file_path"]
    orig_name = inbox_row["original_filename"] or inbox_row["filename"]
    sanitized = sanitize_filename(orig_name)
    dest_path = os.path.join(dest_dir, sanitized)
    if os.path.exists(dest_path):
        stem, ext_part = os.path.splitext(sanitized)
        n = 1
        while os.path.exists(dest_path):
            dest_path = os.path.join(dest_dir, f"{stem}_{n}{ext_part}")
            n += 1

    shutil.copy2(src_path, dest_path)
    final_filename = os.path.basename(dest_path)
    ts = now()

    cur = conn.execute(
        """
        INSERT INTO return_documents
          (return_id, filename, original_filename, doc_type, source,
           file_path, file_size_bytes, uploaded_by, uploaded_at,
           match_confirmed, match_score, match_method)
        VALUES (?, ?, ?, 'unknown', 'email_inbox', ?, ?, ?, ?, 1, ?, ?)
        """,
        (
            return_id,
            final_filename,
            orig_name,
            dest_path,
            inbox_row["file_size_bytes"],
            assigned_by,
            ts,
            match_score,
            match_method,
        ),
    )
    new_doc_id = int(cur.lastrowid)

    conn.execute(
        """
        UPDATE email_inbox
        SET is_assigned = 1, assigned_return_id = ?, assigned_by = ?, assigned_at = ?
        WHERE id = ?
        """,
        (return_id, assigned_by, ts, inbox_row["id"]),
    )
    return new_doc_id


def link_inbox_for_client_email(
    conn: sqlite3.Connection,
    client_id: int,
    *,
    return_id: Optional[int] = None,
    assigned_by: str = "system",
    taxpayer_email: Optional[str] = None,
) -> InboxLinkStats:
    """
    Assign all unassigned inbox rows whose sender_email matches the client's
    taxpayer_email (exact, case-insensitive). Uses return_id when given, else
    the client's most recent return.
    """
    stats = InboxLinkStats()
    if taxpayer_email is None:
        row = conn.execute(
            "SELECT taxpayer_email FROM clients WHERE id=?", (client_id,)
        ).fetchone()
        taxpayer_email = (row["taxpayer_email"] if row else "") or ""
    email = taxpayer_email.strip()
    if not email:
        return stats

    target_return = return_id or _most_recent_return(conn, client_id)
    if not target_return:
        stats.skipped_no_return = 1
        return stats

    invalidate_suggestions_for_email(conn, email)

    items = conn.execute(
        """
        SELECT * FROM email_inbox
        WHERE is_assigned = 0 AND is_deleted = 0
          AND lower(trim(sender_email)) = lower(?)
        ORDER BY received_at ASC, id ASC
        """,
        (email,),
    ).fetchall()

    for item in items:
        try:
            doc_id = assign_inbox_item_to_return(
                conn,
                inbox_row=item,
                return_id=int(target_return),
                assigned_by=assigned_by,
            )
            stats.assigned += 1
            stats.item_ids.append(int(item["id"]))
            _enqueue_extraction(doc_id, int(target_return))
        except Exception:
            _log.exception(
                "Failed to link email_inbox id=%s to return_id=%s client_id=%s",
                item["id"],
                target_return,
                client_id,
            )
            stats.errors += 1

    return stats
