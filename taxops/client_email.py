"""Client outbound-email helpers — recipient resolution for mass campaigns (Prompt J)."""

from __future__ import annotations

import sqlite3
from typing import Optional


def resolve_client_email(conn: sqlite3.Connection, client_id: int) -> Optional[str]:
    """Return the sendable address for *client_id* per MFJ policy.

    taxpayer_email when present and do_not_email=0; else spouse_email under the
    same opt-out gate; else None. Never raises for missing client — returns None.
    """
    row = conn.execute(
        """
        SELECT taxpayer_email, spouse_email, do_not_email
        FROM clients
        WHERE id = ?
        """,
        (client_id,),
    ).fetchone()
    if row is None:
        return None
    if int(row["do_not_email"] or 0):
        return None
    taxpayer = (row["taxpayer_email"] or "").strip()
    if taxpayer:
        return taxpayer
    spouse = (row["spouse_email"] or "").strip()
    if spouse:
        return spouse
    return None
