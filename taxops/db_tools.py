"""
Read-only DB query functions for LLM tool routing.

Rules:
- SELECT only — no INSERT, UPDATE, or DELETE anywhere in this file
- No ssn_last4 in any return value from any function
- Follows the exact SQL patterns from app.py (_SELECT join structure,
  dict(row) conversion, COALESCE balance logic, name LIKE search)
- No logging of query results
"""
from __future__ import annotations

import sqlite3


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _rows(db: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return results as a list of plain dicts."""
    return [dict(r) for r in db.execute(sql, params).fetchall()]


# Season clause shared across functions that filter by intake year.
# Matches the logic in app.py query_returns():
#   - return belongs to season Y if intake_date is in calendar year Y, OR
#   - intake_date is NULL and tax_year = Y-1 (Drake-imported records)
_SEASON_CLAUSE = (
    "(strftime('%Y', r.intake_date) = ? OR "
    "(r.intake_date IS NULL AND r.tax_year = ?))"
)


# ---------------------------------------------------------------------------
# Public functions
# ---------------------------------------------------------------------------

def get_returns_by_status(db: sqlite3.Connection, status: str, year: int) -> list[dict]:
    """All returns matching *status* in the filing season for *year*.

    Returns: id, client_status, log_number, processor, intake_date, display_name
    """
    sql = """
        SELECT
            r.id,
            r.client_status,
            r.log_number,
            r.processor,
            r.intake_date,
            COALESCE(c.display_name, c.last_name || CASE WHEN c.first_name IS NOT NULL AND c.first_name != '' THEN ', ' || c.first_name ELSE '' END) AS display_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.client_status = ?
          AND """ + _SEASON_CLAUSE + """
        ORDER BY CAST(r.log_number AS INTEGER), r.id
    """
    return _rows(db, sql, (status, str(year), year - 1))


def get_returns_by_processor(db: sqlite3.Connection, processor: str, year: int) -> list[dict]:
    """All returns assigned to *processor* in the filing season for *year*.

    Processor name is matched case-insensitively.
    Returns: id, client_status, log_number, processor, intake_date, display_name
    """
    sql = """
        SELECT
            r.id,
            r.client_status,
            r.log_number,
            r.processor,
            r.intake_date,
            COALESCE(c.display_name, c.last_name || CASE WHEN c.first_name IS NOT NULL AND c.first_name != '' THEN ', ' || c.first_name ELSE '' END) AS display_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE lower(r.processor) = lower(?)
          AND """ + _SEASON_CLAUSE + """
        ORDER BY CAST(r.log_number AS INTEGER), r.id
    """
    return _rows(db, sql, (processor, str(year), year - 1))


def get_client_returns(db: sqlite3.Connection, client_id: int) -> list[dict]:
    """All returns for *client_id* across all tax years.

    Returns: id, tax_year, client_status, log_number, intake_date, pickup_date
    """
    sql = """
        SELECT
            r.id,
            r.tax_year,
            r.client_status,
            r.log_number,
            r.intake_date,
            r.pickup_date
        FROM returns r
        WHERE r.client_id = ?
        ORDER BY r.tax_year DESC, r.id DESC
    """
    return _rows(db, sql, (client_id,))


def get_balance_due_returns(db: sqlite3.Connection, year: int) -> list[dict]:
    """Returns in the filing season for *year* where a balance is still owed.

    Uses the same balance condition as app.py query_returns() balance_due filter:
      p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee

    Returns: id, display_name, log_number, total_fee, fee_paid
    """
    sql = """
        SELECT
            r.id,
            r.log_number,
            COALESCE(c.display_name, c.last_name || CASE WHEN c.first_name IS NOT NULL AND c.first_name != '' THEN ', ' || c.first_name ELSE '' END) AS display_name,
            p.total_fee,
            p.fee_paid
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE (p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)
          AND """ + _SEASON_CLAUSE + """
        ORDER BY CAST(r.log_number AS INTEGER), r.id
    """
    return _rows(db, sql, (str(year), year - 1))


def get_missing_docs(db: sqlite3.Connection, return_id: int) -> list[dict]:
    """Unresolved missing-document rows for *return_id*.

    Columns verified against db.py init_db():
      id, return_id, item_text, is_resolved, created_at, resolved_at

    Only returns rows where is_resolved = 0.
    """
    sql = """
        SELECT
            id,
            return_id,
            item_text,
            is_resolved,
            created_at,
            resolved_at
        FROM missing_docs
        WHERE return_id = ?
          AND is_resolved = 0
        ORDER BY created_at
    """
    return _rows(db, sql, (return_id,))


def search_clients(db: sqlite3.Connection, query: str) -> list[dict]:
    """Search clients by name, replicating the name-search logic in app.py query_returns().

    Matches lower(last_name), lower(first_name), or lower(display_name) with LIKE.
    Returns: id, display_name, last_name, first_name
    No ssn_last4 returned.
    """
    q = query.strip()
    if not q:
        return []

    qp = f"%{q.lower()}%"
    sql = """
        SELECT
            id,
            COALESCE(display_name, last_name || CASE WHEN first_name IS NOT NULL AND first_name != '' THEN ', ' || first_name ELSE '' END) AS display_name,
            last_name,
            first_name
        FROM clients
        WHERE lower(last_name) LIKE ?
           OR lower(first_name) LIKE ?
           OR lower(COALESCE(display_name, '')) LIKE ?
        ORDER BY last_name, first_name
        LIMIT 20
    """
    return _rows(db, sql, (qp, qp, qp))
