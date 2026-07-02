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
from typing import Any


# ---------------------------------------------------------------------------
# Static IRS rejection code reference table
# ---------------------------------------------------------------------------
# Authoritative local lookup — checked before any LLM call.
# No PII, no ssn_last4 anywhere in this structure.

IRS_REJECTION_CODES: dict[str, dict] = {
    "IND-031-04": {
        "frequency": "very_high",
        "category": "Identity / PIN",
        "explanation": "Prior-year AGI or self-select PIN doesn't match IRS records.",
        "action": "Enter the exact AGI from line 11 of the prior-year 1040. If filed late or return was adjusted, use $0.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-031-04"
    },
    "IND-032-04": {
        "frequency": "very_high",
        "category": "Identity / PIN",
        "explanation": "Spouse's prior-year AGI or self-select PIN doesn't match IRS records.",
        "action": "Verify the spouse's exact AGI from line 11 of their prior-year 1040. Use $0 if they did not file.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-032-04"
    },
    "R0000-902-01": {
        "frequency": "very_high",
        "category": "Duplicate",
        "explanation": "A return with this SSN was already accepted by the IRS for this tax year.",
        "action": "Verify no duplicate filing was submitted. If identity theft is suspected, call the IRS Identity Theft Hotline at 800-908-4490.",
        "irs_reference": "IRS e-file Error Code Reference, Rule R0000-902-01"
    },
    "IND-507": {
        "frequency": "very_high",
        "category": "Dependent",
        "explanation": "A dependent on this return was already claimed on another accepted return.",
        "action": "Confirm who has legal right to claim the dependent. If client does, the return must be filed by mail.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-507"
    },
    "R0000-500-01": {
        "frequency": "very_high",
        "category": "Name / SSN",
        "explanation": "The primary taxpayer's name or SSN does not match IRS and SSA records.",
        "action": "Verify the spelling matches the Social Security card exactly, including any recent name changes.",
        "irs_reference": "IRS e-file Error Code Reference, Rule R0000-500-01"
    },
    "IND-157": {
        "frequency": "high",
        "category": "Name / SSN",
        "explanation": "The primary taxpayer's name control does not match SSA records.",
        "action": "Use the first four letters of the last name exactly as shown on the Social Security card.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-157"
    },
    "R0000-503-02": {
        "frequency": "high",
        "category": "Name / SSN",
        "explanation": "The spouse's SSN and name control do not match the IRS e-file database.",
        "action": "Verify the spouse's SSN and confirm the name matches their Social Security card exactly.",
        "irs_reference": "IRS e-file Error Code Reference, Rule R0000-503-02"
    },
    "R0000-504-02": {
        "frequency": "high",
        "category": "Dependent",
        "explanation": "A dependent's SSN and name control do not match the IRS e-file database.",
        "action": "Verify each dependent's SSN and name spelling against their Social Security card.",
        "irs_reference": "IRS e-file Error Code Reference, Rule R0000-504-02"
    },
    "IND-524": {
        "frequency": "high",
        "category": "Identity / PIN",
        "explanation": "The date of birth on the return does not match IRS and SSA records.",
        "action": "Verify the exact birth date against the taxpayer's Social Security card or government-issued ID.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-524"
    },
    "IND-180-01": {
        "frequency": "high",
        "category": "Identity / PIN",
        "explanation": "The primary taxpayer's Identity Protection PIN is missing or incorrect.",
        "action": "Retrieve the current-year IP PIN at IRS.gov/ippin — it changes every year and last year's PIN will not work.",
        "irs_reference": "IRS Identity Protection PIN Program"
    },
    "IND-183-01": {
        "frequency": "medium",
        "category": "Identity / PIN",
        "explanation": "The spouse's Identity Protection PIN is missing or incorrect.",
        "action": "Retrieve the spouse's current-year IP PIN at IRS.gov/ippin.",
        "irs_reference": "IRS Identity Protection PIN Program"
    },
    "FW2-502": {
        "frequency": "medium",
        "category": "Employer / EIN",
        "explanation": "The W-2 employer EIN or first four characters of the employer name do not match IRS records.",
        "action": "Verify the EIN against the original W-2. If correct, the client must contact their employer for a corrected W-2.",
        "irs_reference": "IRS e-file Error Code Reference, Rule FW2-502"
    },
    "F8962-070": {
        "frequency": "medium",
        "category": "ACA / Credits",
        "explanation": "IRS records show the taxpayer or a dependent had Marketplace health insurance but Form 8962 is missing from the return.",
        "action": "Add Form 8962 using the 1095-A received from the Health Insurance Marketplace.",
        "irs_reference": "IRS Form 8962 Instructions"
    },
    "SEIC-F1040-521-02": {
        "frequency": "medium",
        "category": "EIC / Credits",
        "explanation": "A qualifying child's SSN on Schedule EIC matches a child already claimed on another accepted return.",
        "action": "Determine who has legal right to claim the child. The other party must file an amended return before this return can be e-filed.",
        "irs_reference": "IRS Schedule EIC Instructions"
    },
    "SEIC-F1040-535-02": {
        "frequency": "medium",
        "category": "EIC / Credits",
        "explanation": "A child's SSN or birth year on Schedule EIC does not match the IRS e-file database.",
        "action": "Verify the child's exact SSN and date of birth against their Social Security card.",
        "irs_reference": "IRS Schedule EIC Instructions"
    },
    "IND-046": {
        "frequency": "medium",
        "category": "EIC / Credits",
        "explanation": "The IRS database shows this taxpayer is not eligible to claim the Earned Income Credit this year.",
        "action": "If the client received an IRS letter reinstating EIC eligibility, attach Form 8862 and retransmit.",
        "irs_reference": "IRS Form 8862 Instructions"
    },
    "IND-452": {
        "frequency": "medium",
        "category": "Duplicate",
        "explanation": "The primary SSN was already used on a previously accepted return for this tax period.",
        "action": "Confirm no duplicate was filed. If identity theft is suspected, contact the IRS Identity Theft Hotline at 800-908-4490.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-452"
    },
    "IND-510-02": {
        "frequency": "medium",
        "category": "Duplicate",
        "explanation": "The spouse's SSN matches the primary SSN on another accepted return for this tax year.",
        "action": "Verify all SSNs are entered correctly. This may indicate identity theft — contact the IRS if confirmed correct.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-510-02"
    },
    "F1099R-502-02": {
        "frequency": "medium",
        "category": "Employer / EIN",
        "explanation": "The 1099-R payer EIN does not match IRS records.",
        "action": "Verify the EIN against the original 1099-R document. If correct, the client must contact the payer for a corrected form.",
        "irs_reference": "IRS e-file Error Code Reference, Rule F1099R-502-02"
    },
    "IND-516-02": {
        "frequency": "low",
        "category": "Dependent",
        "explanation": "The primary taxpayer's SSN matches a dependent SSN on another return where the primary is not marked as a dependent.",
        "action": "Verify the primary SSN is correct and confirm the client is not being claimed as a dependent on another return.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-516-02"
    },
    "IND-517-01": {
        "frequency": "low",
        "category": "Dependent",
        "explanation": "A dependent's SSN matches the primary or spouse SSN on another return.",
        "action": "Verify the dependent's SSN is correct and check for a data entry error.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-517-01"
    },
    "F2441-010": {
        "frequency": "low",
        "category": "Credits",
        "explanation": "A qualifying person's SSN on Form 2441 matches a qualifying person on another accepted return.",
        "action": "Confirm which taxpayer has the right to claim this qualifying person for the child and dependent care credit.",
        "irs_reference": "IRS Form 2441 Instructions"
    },
    "IND-513-01": {
        "frequency": "low",
        "category": "Duplicate",
        "explanation": "The spouse's SSN matches the spouse SSN on another accepted return for this tax year.",
        "action": "Verify the spouse's SSN is entered correctly. This may indicate a duplicate filing or identity theft.",
        "irs_reference": "IRS e-file Error Code Reference, Rule IND-513-01"
    },
    "FW2G-502": {
        "frequency": "low",
        "category": "Employer / EIN",
        "explanation": "The W-2G payer EIN does not match IRS records.",
        "action": "Verify the EIN on the original W-2G from the gambling or lottery payer. Contact the payer if the mismatch persists.",
        "irs_reference": "IRS e-file Error Code Reference, Rule FW2G-502"
    },
    "F1040-068-02": {
        "frequency": "low",
        "category": "EIC / Credits",
        "explanation": "EIC was claimed but the taxpayer's age is outside the 25-64 range and no qualifying child is listed.",
        "action": "Verify the taxpayer's date of birth. EIC without a qualifying child requires the taxpayer to be between 25 and 64.",
        "irs_reference": "IRS Schedule EIC Instructions"
    },
}


def lookup_rejection_code(normalized_code: str) -> dict | None:
    """Return the static reference entry for a known IRS rejection code, or None.

    Never includes ssn_last4 or any PII.
    """
    return IRS_REJECTION_CODES.get(normalized_code)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _rows(db: sqlite3.Connection, sql: str, params: tuple = ()) -> list[dict]:
    """Execute a SELECT and return results as a list of plain dicts."""
    return [dict(r) for r in db.execute(sql, params).fetchall()]


def _safe_fetchone(db: sqlite3.Connection, sql: str, params: tuple) -> Any | None:
    """SELECT single row — returns None on sqlite errors (missing table/columns, etc.)."""
    try:
        return db.execute(sql, params).fetchone()
    except sqlite3.Error:
        return None


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

_USELESS_REJECTION_REASONS: frozenset[str] = frozenset(
    {
        "",
        "test",
        "n/a",
        "na",
        "none",
        "tbd",
        "unknown",
        "?",
        "-",
        ".",
        "no reason",
        "no reason provided",
    },
)


def _rejection_reason_useful_for_chat(reason: str) -> bool:
    s = " ".join((reason or "").split()).strip()
    if len(s) < 10:
        return False
    return s.casefold() not in _USELESS_REJECTION_REASONS


def summarize_season_rejections_for_chat(db: sqlite3.Connection, year: int) -> dict:
    """Roll up ``REJECTED`` returns for season *year* by latest IRS rejection code per return.

    No client names or SSN fields. Used when staff ask both how many rejects exist and why.
    """
    ys, yt = str(year), year - 1
    total_row = db.execute(
        f"""
        SELECT COUNT(*) AS c FROM returns r
        WHERE r.client_status = 'REJECTED'
          AND {_SEASON_CLAUSE}
        """,
        (ys, yt),
    ).fetchone()
    total = int(total_row["c"] if total_row else 0)

    no_hist_row = db.execute(
        f"""
        SELECT COUNT(*) AS c FROM returns r
        WHERE r.client_status = 'REJECTED'
          AND {_SEASON_CLAUSE}
          AND NOT EXISTS (
            SELECT 1 FROM efile_batch_items e WHERE e.return_id = r.id
          )
        """,
        (ys, yt),
    ).fetchone()
    no_batch_history_count = int(no_hist_row["c"] if no_hist_row else 0)

    code_rows = _rows(
        db,
        f"""
        WITH rej AS (
          SELECT r.id AS return_id
          FROM returns r
          WHERE r.client_status = 'REJECTED' AND {_SEASON_CLAUSE}
        ),
        latest_coded AS (
          SELECT
            i.return_id,
            TRIM(COALESCE(i.rejection_code, '')) AS code_raw,
            ROW_NUMBER() OVER (
              PARTITION BY i.return_id ORDER BY i.id DESC
            ) AS rn
          FROM efile_batch_items i
          INNER JOIN rej ON rej.return_id = i.return_id
          WHERE NULLIF(TRIM(COALESCE(i.rejection_code, '')), '') IS NOT NULL
        )
        SELECT code_raw AS code, COUNT(*) AS cnt
        FROM latest_coded
        WHERE rn = 1
        GROUP BY code_raw
        ORDER BY cnt DESC, code_raw ASC
        """,
        (ys, yt),
    )

    def _explain(code: str) -> str | None:
        raw = (code or "").strip()
        if not raw:
            return None
        meta = lookup_rejection_code(raw) or lookup_rejection_code(raw.upper())
        if not isinstance(meta, dict):
            return None
        expl = meta.get("explanation")
        return expl if isinstance(expl, str) and expl.strip() else None

    no_code_msg = (
        "No non-empty `rejection_code` was saved on any `efile_batch_items` row for these returns. "
        "Use the return's e-file batch / IRS ack UI in TaxOps for the message, or enter the IRS code on the rejection row."
    )

    by_code: list[dict] = []
    for row in code_rows:
        code_clean = str(row.get("code") or "").strip()
        cnt = int(row.get("cnt") or 0)
        if not code_clean:
            continue
        by_code.append(
            {
                "code": code_clean,
                "count": cnt,
                "explanation": _explain(code_clean),
            }
        )

    orphan_row = db.execute(
        f"""
        SELECT COUNT(*) AS c FROM returns r
        WHERE r.client_status = 'REJECTED'
          AND {_SEASON_CLAUSE}
          AND EXISTS (SELECT 1 FROM efile_batch_items e WHERE e.return_id = r.id)
          AND NOT EXISTS (
              SELECT 1 FROM efile_batch_items e2
              WHERE e2.return_id = r.id
                AND NULLIF(TRIM(COALESCE(e2.rejection_code, '')), '') IS NOT NULL
          )
        """,
        (ys, yt),
    ).fetchone()
    orphan_ct = int(orphan_row["c"] if orphan_row else 0)
    fallback_no_detail = orphan_ct

    if orphan_ct > 0:
        rr_rows = _rows(
            db,
            f"""
            WITH orphans AS (
              SELECT r.id AS return_id
              FROM returns r
              WHERE r.client_status = 'REJECTED'
                AND {_SEASON_CLAUSE}
                AND EXISTS (SELECT 1 FROM efile_batch_items e WHERE e.return_id = r.id)
                AND NOT EXISTS (
                    SELECT 1 FROM efile_batch_items e2
                    WHERE e2.return_id = r.id
                      AND NULLIF(TRIM(COALESCE(e2.rejection_code, '')), '') IS NOT NULL
                )
            ),
            latest_reason AS (
              SELECT
                i.return_id,
                TRIM(COALESCE(i.rejection_reason, '')) AS rr,
                ROW_NUMBER() OVER (
                  PARTITION BY i.return_id ORDER BY i.id DESC
                ) AS rn
              FROM efile_batch_items i
              INNER JOIN orphans ON orphans.return_id = i.return_id
            )
            SELECT rr AS reason_raw, COUNT(*) AS cnt
            FROM latest_reason
            WHERE rn = 1
            GROUP BY rr
            ORDER BY cnt DESC
            """,
            (ys, yt),
        )
        for row in rr_rows:
            rr_raw = str(row.get("reason_raw") or "")
            rr_one = " ".join(rr_raw.split()).strip()
            cnt = int(row.get("cnt") or 0)
            if cnt <= 0:
                continue
            if _rejection_reason_useful_for_chat(rr_one):
                fallback_no_detail -= cnt
                snippet = rr_one[:120] + ("…" if len(rr_one) > 120 else "")
                sane = rr_one.replace("**", "").replace("\n", " ")
                sane = " ".join(sane.split()).strip()[:500]
                label = f"(batch notes — no IRS code) {snippet}"
                by_code.append(
                    {
                        "code": label,
                        "count": cnt,
                        "explanation": (
                            "Recorded on the latest e-file batch item (no structured IRS code saved): "
                            f"{sane}"
                        ),
                    }
                )

    if fallback_no_detail > 0:
        by_code.append(
            {
                "code": "(no IRS rejection code on any e-file batch row)",
                "count": fallback_no_detail,
                "explanation": no_code_msg,
            }
        )

    by_code.sort(key=lambda item: (-int(item.get("count") or 0), str(item.get("code") or "")))

    return {
        "total": total,
        "no_batch_history_count": no_batch_history_count,
        "by_code": by_code,
    }


def get_returns_by_status(db: sqlite3.Connection, status: str, year: int) -> list[dict]:
    """All returns matching *status* in the filing season for *year*.

    Returns: id, client_status, log_number, processor, intake_date, pickup_date,
    logout_date, ack_date, display_name
    """
    sql = """
        SELECT
            r.id,
            r.client_status,
            r.log_number,
            r.processor,
            r.intake_date,
            r.pickup_date,
            r.logout_date,
            r.ack_date,
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
    Returns: id, client_status, log_number, processor, intake_date, pickup_date,
    logout_date, ack_date, display_name
    """
    sql = """
        SELECT
            r.id,
            r.client_status,
            r.log_number,
            r.processor,
            r.intake_date,
            r.pickup_date,
            r.logout_date,
            r.ack_date,
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


def count_returns_with_positive_refund_in_season(db: sqlite3.Connection, year: int) -> int:
    """Count distinct returns (intake-season *year*) with ``payments.refund_amount`` > 0.

    Mirrors :data:`_SEASON_CLAUSE`; uses Drake/import payment rows — no taxpayer PII in result.
    """
    sql = f"""
        SELECT COUNT(DISTINCT r.id) AS n
        FROM returns r
        INNER JOIN payments p ON p.return_id = r.id
        WHERE {_SEASON_CLAUSE}
          AND p.refund_amount IS NOT NULL
          AND CAST(p.refund_amount AS REAL) > 0
    """
    row = db.execute(sql, (str(year), year - 1)).fetchone()
    if row is None or row["n"] is None:
        return 0
    return int(row["n"])


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


def get_system_context(db: sqlite3.Connection, year: int) -> dict:
    """Aggregate office snapshot for LLM system prompts (POST /ai/chat).

    Only aggregates — no client names, return identifiers, log numbers, or ssn_last4.

    ``processor_return_counts`` keys are **preparer/staff workload labels** from
    ``returns.processor`` (who is assigned to prep a return), not taxpayer client display names.

    Returned keys:

    - ``today_local_iso``: server local calendar date ``YYYY-MM-DD``
    - ``season_year``: filing-season selector (calendar year; matches helpers using
      :data:`_SEASON_CLAUSE`)
    - ``status_counts``: workflow status labels to counts for that season
    - ``processor_return_counts``: canonical preparer name -> assignment count this season
    - ``balance_due_season_total``: in-season returns with unpaid balance rows
      (aligned with :func:`get_balance_due_returns`).
    - ``dataplane_compact``: token-sized slice of office-wide KPIs (see :func:`compact_llm_dataplane`)
      for LLM routers; counts and rolled-up money fields only — no client identifiers.
    """
    from datetime import date

    cnt_sql = f"""
        SELECT r.client_status AS status, COUNT(*) AS n
        FROM returns r
        WHERE {_SEASON_CLAUSE}
        GROUP BY r.client_status
        ORDER BY r.client_status
    """
    count_rows = _rows(db, cnt_sql, (str(year), year - 1))
    status_counts: dict[str, int] = {}
    for row in count_rows:
        st_raw = row.get("status")
        st_label = "(none)" if st_raw is None or str(st_raw).strip() == "" else str(st_raw)
        status_counts[st_label] = int(row.get("n") or 0)

    proc_agg_sql = f"""
        SELECT LOWER(TRIM(r.processor)) AS proc_key,
               MIN(TRIM(r.processor)) AS proc_label,
               COUNT(*) AS n
        FROM returns r
        WHERE {_SEASON_CLAUSE}
          AND TRIM(COALESCE(r.processor, '')) != ''
          AND LOWER(TRIM(r.processor)) != '(unassigned)'
        GROUP BY proc_key
        ORDER BY proc_label COLLATE NOCASE
    """
    proc_rows = _rows(db, proc_agg_sql, (str(year), year - 1))
    processor_return_counts: dict[str, int] = {}
    for row in proc_rows:
        label = row.get("proc_label")
        if not isinstance(label, str):
            label = str(label or "").strip()
        lbl = label.strip()
        if not lbl:
            continue
        processor_return_counts[lbl] = int(row.get("n") or 0)

    bal_sql = f"""
        SELECT COUNT(*) AS n
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE (p.total_fee IS NOT NULL AND COALESCE(p.fee_paid, 0) < p.total_fee)
          AND {_SEASON_CLAUSE}
    """
    bal_row = db.execute(bal_sql, (str(year), year - 1)).fetchone()
    balance_due_season_total = int(bal_row["n"]) if bal_row else 0

    return {
        "today_local_iso": date.today().isoformat(),
        "season_year": int(year),
        "status_counts": status_counts,
        "processor_return_counts": processor_return_counts,
        "balance_due_season_total": balance_due_season_total,
        "dataplane_compact": compact_llm_dataplane(gather_season_dataplane(db, year)),
    }


def gather_season_dataplane(db: sqlite3.Connection, year: int) -> dict[str, Any]:
    """Season-linked office aggregates (counts only; suitable for synthetic chat answers).

    Uses the same intake season rule as :data:`_SEASON_CLAUSE` on ``returns``.
    Includes calendar-year slices (imports / e-file batch creation / email classifier)
    keyed to ``year`` for parallelism with the season selector.

    Missing tables/columns on older SQLite files degrade gracefully (empty dicts/lists
    instead of exceptions) thanks to defensive fetch helpers.
    """
    ys, yt = str(year), year - 1
    params = (ys, yt)

    def _qf(sql: str, pr: tuple) -> list[dict]:
        try:
            return _rows(db, sql, pr)
        except sqlite3.Error:
            return []

    forms_row = _safe_fetchone(
        db,
        f"""
        SELECT
          COUNT(*) AS returns_in_season,
          SUM(CASE WHEN rf.id IS NULL THEN 1 ELSE 0 END) AS no_form_profile_row,
          SUM(CASE WHEN COALESCE(rf.form_1040, 0) <> 0 THEN 1 ELSE 0 END) AS ct_1040,
          SUM(CASE WHEN COALESCE(rf.sched_a_d, 0) <> 0 THEN 1 ELSE 0 END) AS ct_sched_ad,
          SUM(CASE WHEN COALESCE(rf.sched_c, 0) <> 0 THEN 1 ELSE 0 END) AS ct_sched_c,
          SUM(CASE WHEN COALESCE(rf.sched_e, 0) <> 0 THEN 1 ELSE 0 END) AS ct_sched_e,
          SUM(CASE WHEN COALESCE(rf.form_1120, 0) <> 0 THEN 1 ELSE 0 END) AS ct_1120,
          SUM(CASE WHEN COALESCE(rf.form_1120s, 0) <> 0 THEN 1 ELSE 0 END) AS ct_1120s,
          SUM(CASE WHEN COALESCE(rf.form_1065_llc, 0) <> 0 THEN 1 ELSE 0 END) AS ct_1065,
          SUM(CASE WHEN COALESCE(rf.form_990_1041, 0) <> 0 THEN 1 ELSE 0 END) AS ct_9901041,
          SUM(CASE WHEN COALESCE(rf.corp_officer, 0) <> 0 THEN 1 ELSE 0 END) AS ct_corp_officer,
          SUM(CASE WHEN COALESCE(rf.business_owner, 0) <> 0 THEN 1 ELSE 0 END) AS ct_business_owner
        FROM returns r
        LEFT JOIN return_forms rf ON rf.return_id = r.id
        WHERE {_SEASON_CLAUSE}
        """,
        params,
    )
    forms_dict = dict(forms_row) if forms_row else {}

    flags_row = _safe_fetchone(
        db,
        f"""
        SELECT
          SUM(CASE WHEN COALESCE(r.is_extension, 0) <> 0 THEN 1 ELSE 0 END) AS ext,
          SUM(CASE WHEN COALESCE(r.has_w7, 0) <> 0 THEN 1 ELSE 0 END) AS w7,
          SUM(CASE WHEN COALESCE(r.is_amended, 0) <> 0 THEN 1 ELSE 0 END) AS amd
        FROM returns r WHERE {_SEASON_CLAUSE}
        """,
        params,
    )
    flags_dict = dict(flags_row) if flags_row else {}

    drake_histogram = _qf(
        f"""
        SELECT COALESCE(NULLIF(TRIM(r.drake_status_raw), ''), '(blank)') AS label, COUNT(*) AS n
        FROM returns r
        WHERE {_SEASON_CLAUSE}
        GROUP BY label
        ORDER BY n DESC
        LIMIT 20
        """,
        params,
    )

    md_open = _safe_fetchone(
        db,
        f"""
        SELECT COUNT(*) AS n_items,
               COUNT(DISTINCT md.return_id) AS n_returns
        FROM missing_docs md
        INNER JOIN returns r ON r.id = md.return_id
        WHERE COALESCE(md.is_resolved, 0) = 0
          AND {_SEASON_CLAUSE}
        """,
        params,
    )
    missing_dict = dict(md_open) if md_open else {"n_items": 0, "n_returns": 0}

    extraction_by_status = _qf(
        f"""
        SELECT COALESCE(TRIM(eq.status), '(unknown)') AS status_label, COUNT(*) AS n
        FROM extraction_queue eq
        INNER JOIN returns r ON r.id = eq.return_id
        WHERE {_SEASON_CLAUSE}
        GROUP BY status_label
        ORDER BY n DESC
        LIMIT 40
        """,
        params,
    )

    payment_methods = _qf(
        f"""
        SELECT
          CASE
            WHEN TRIM(COALESCE(p.payment_method, '')) = '' THEN '(not recorded)'
            ELSE TRIM(p.payment_method)
          END AS payment_method_label,
          COUNT(*) AS n
        FROM payments p
        INNER JOIN returns r ON r.id = p.return_id
        WHERE {_SEASON_CLAUSE}
        GROUP BY payment_method_label
        ORDER BY n DESC
        LIMIT 30
        """,
        params,
    )

    fee_row = _safe_fetchone(
        db,
        f"""
        SELECT
          COALESCE(SUM(p.total_fee), 0.0) AS sum_total_fee,
          COALESCE(SUM(p.fee_paid), 0.0) AS sum_fee_paid,
          COALESCE(SUM(p.refund_amount), 0.0) AS sum_refund,
          COALESCE(SUM(p.bank_deposit), 0.0) AS sum_deposit,
          COALESCE(SUM(p.accounting_fee), 0.0) AS sum_acct_fee,
          COALESCE(SUM(p.down_payment), 0.0) AS sum_down,
          COALESCE(
              SUM(COALESCE(p.discount_amount, 0) + COALESCE(p.special_discount, 0)),
              0.0
          ) AS sum_discount_like
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE {_SEASON_CLAUSE}
        """,
        params,
    )
    fee_dict = {}
    if fee_row:
        fee_dict = {k: float(v or 0) for k, v in dict(fee_row).items()}

    status_transitions = _qf(
        f"""
        SELECT
          (COALESCE(NULLIF(TRIM(se.old_status), ''), '-') || ' \u2192 '
           || COALESCE(NULLIF(TRIM(se.new_status), ''), '-')) AS edge,
          COUNT(*) AS n
        FROM status_events se
        INNER JOIN returns r ON r.id = se.return_id
        WHERE {_SEASON_CLAUSE}
        GROUP BY edge
        ORDER BY n DESC
        LIMIT 14
        """,
        params,
    )

    se_ct = _safe_fetchone(
        db,
        f"""
        SELECT COUNT(*) AS n_events
        FROM status_events se
        INNER JOIN returns r ON r.id = se.return_id
        WHERE {_SEASON_CLAUSE}
        """,
        params,
    )
    status_evt_total = int(se_ct["n_events"] if se_ct else 0)

    efile_items_by_ack_status = _qf(
        f"""
        SELECT COALESCE(NULLIF(TRIM(i.ack_status), ''), '(unknown)') AS ack_status_label,
               COUNT(*) AS n
        FROM efile_batch_items i
        INNER JOIN returns r ON r.id = i.return_id
        WHERE {_SEASON_CLAUSE}
        GROUP BY ack_status_label
        ORDER BY n DESC
        LIMIT 20
        """,
        params,
    )

    im_row = _safe_fetchone(
        db,
        """
        SELECT
          COUNT(*) AS batches_y,
          COALESCE(SUM(row_count), 0) AS rows_y,
          COALESCE(SUM(error_count), 0) AS errs_y,
          COALESCE(SUM(success_count), 0) AS ok_y,
          COALESCE(SUM(review_count), 0) AS reviews_y
        FROM import_batches
        WHERE strftime('%Y', imported_at) = ?
        """,
        (ys,),
    )
    imports_dict = dict(im_row) if im_row else {}

    efile_batches_by_status = _qf(
        """
        SELECT COALESCE(TRIM(status), '(unknown)') AS batch_status_label, COUNT(*) AS n
        FROM efile_batches
        WHERE strftime('%Y', created_at) = ?
        GROUP BY batch_status_label
        ORDER BY n DESC
        LIMIT 12
        """,
        (ys,),
    )

    return {
        "season_year": year,
        "forms": forms_dict,
        "flags": flags_dict,
        "drake_histogram": drake_histogram,
        "missing_docs": missing_dict,
        "extraction_by_status": extraction_by_status,
        "payment_methods": payment_methods,
        "fee_rollups": fee_dict,
        "status_transitions": status_transitions,
        "status_event_count": status_evt_total,
        "efile_items_by_ack_status": efile_items_by_ack_status,
        "efile_batches_by_status": efile_batches_by_status,
        "import_batches_calendar_year": imports_dict,
    }


def compact_llm_dataplane(
    dp: dict[str, Any] | None,
    *,
    hist_cap: int = 6,
    form_flag_cap: int = 12,
) -> dict[str, Any]:
    """Trim :func:`gather_season_dataplane` into a JSON-friendly blob for LLM prompts.

    Counts and money rollups only (no names, return ids, or log numbers). Histograms
    are truncated to ``hist_cap`` rows; non-positive form-profile flags are omitted.
    """
    if not isinstance(dp, dict) or not dp:
        return {}

    def _thin(rows: object, label_key: str, n_key: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if not isinstance(rows, list):
            return out
        for row in rows[:hist_cap]:
            if not isinstance(row, dict):
                continue
            lbl = row.get(label_key)
            out.append(
                {
                    "l": (str(lbl)[:120] if lbl is not None else ""),
                    "n": int(row.get(n_key) or row.get("n") or 0),
                }
            )
        return out

    fm = dp.get("forms") if isinstance(dp.get("forms"), dict) else {}
    form_pairs: list[tuple[str, int]] = []
    for col, short in (
        ("ct_1040", "1040"),
        ("ct_sched_ad", "sched_ad"),
        ("ct_sched_c", "sch_c"),
        ("ct_sched_e", "sch_e"),
        ("ct_1120", "1120"),
        ("ct_1120s", "1120s"),
        ("ct_1065", "1065"),
        ("ct_9901041", "990_1041"),
        ("ct_corp_officer", "corp_officer"),
        ("ct_business_owner", "biz_owner"),
    ):
        n = int(fm.get(col) or 0)
        if n > 0:
            form_pairs.append((short, n))
    form_pairs.sort(key=lambda x: -x[1])

    flags = dp.get("flags") if isinstance(dp.get("flags"), dict) else {}
    fr = dp.get("fee_rollups") if isinstance(dp.get("fee_rollups"), dict) else {}
    fees_out: dict[str, float] = {}
    if fr:
        for k, raw in fr.items():
            kk = str(k)
            try:
                fees_out[kk] = round(float(raw or 0), 2)
            except (TypeError, ValueError):
                fees_out[kk] = 0.0

    imb = dp.get("import_batches_calendar_year")
    imb_out: dict[str, int] = {}
    if isinstance(imb, dict):
        for k in ("batches_y", "rows_y", "errs_y", "ok_y", "reviews_y"):
            try:
                imb_out[k] = int(float(imb.get(k) or 0))
            except (TypeError, ValueError):
                imb_out[k] = 0

    md = dp.get("missing_docs") if isinstance(dp.get("missing_docs"), dict) else {}

    slim: dict[str, Any] = {
        "y": int(dp.get("season_year") or 0),
        "forms": {
            "n": int(fm.get("returns_in_season") or 0),
            "no_rf": int(fm.get("no_form_profile_row") or 0),
            "top": [{"k": a, "n": b} for a, b in form_pairs[:form_flag_cap]],
        },
        "flags": {
            "ext": int(flags.get("ext") or 0),
            "w7": int(flags.get("w7") or 0),
            "amd": int(flags.get("amd") or 0),
        },
        "miss": {"items": int(md.get("n_items") or 0), "rets": int(md.get("n_returns") or 0)},
        "drake": _thin(dp.get("drake_histogram"), "label", "n"),
        "extr": _thin(dp.get("extraction_by_status"), "status_label", "n"),
        "paym": _thin(dp.get("payment_methods"), "payment_method_label", "n"),
        "fees": fees_out,
        "st_ev": int(dp.get("status_event_count") or 0),
        "st_tr": _thin(dp.get("status_transitions"), "edge", "n"),
        "ef_ack": _thin(dp.get("efile_items_by_ack_status"), "ack_status_label", "n"),
        "ef_bat": _thin(dp.get("efile_batches_by_status"), "batch_status_label", "n"),
        "imp_cy": imb_out,
    }

    return slim
