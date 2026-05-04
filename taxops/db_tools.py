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
