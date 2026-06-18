"""
csv_analyzer.py
---------------
Smart CSV analyzer that auto-detects header rows, merges split headers,
and fuzzy-maps columns to known database fields with confidence scores.
No external API required — pure pattern matching.
"""
from __future__ import annotations

import csv
import io
import re
from dataclasses import dataclass, field
from typing import Any


# ── Known field definitions ────────────────────────────────────────────────────
# Each entry: (db_table, db_field, field_type, keywords_that_identify_it)
# field_type: str | int | bool | date | currency | status

KNOWN_FIELDS: list[tuple[str, str, str, list[str]]] = [
    # ── Client identity ────────────────────────────────────────────────────────
    ("clients",      "last_name",           "str",      ["last name", "taxpayer last name", "taxpayer last", "surname", "last"]),
    ("clients",      "first_name",          "str",      ["first name", "taxpayer first name", "taxpayer first", "given name", "first"]),
    ("clients",      "display_name",        "str",      ["taxpayer name", "client name", "tax payer name"]),
    # ── Log numbers ───────────────────────────────────────────────────────────
    # Current-year log: any "LOG YYYY" where YYYY is 2024-2027 (or explicit keywords)
    ("returns",      "log_number",          "str",      ["log 2025", "log 2026", "log#", "log #",
                                                          "log number", "2025", "2026"]),
    ("clients",      "prior_year_log",      "str",      ["log 2024", "log 2023", "prior log",
                                                          "prev log", "2024", "2023"]),
    # ── Return metadata ───────────────────────────────────────────────────────
    ("returns",      "tax_year",            "int",      ["tax year", "year", "yr"]),
    # "Return Type" / "Filing Status" from TAXOPS are handled by the exact map; these
    # keywords target only our own log columns.
    ("returns",      "is_extension",        "bool",     ["extension", "ext flag", "is extension"]),
    ("returns",      "is_amended",          "bool",     ["1040x", "amended", "amend"]),
    ("returns",      "has_w7",              "bool",     ["w7", "w-7"]),
    # ── Workflow ──────────────────────────────────────────────────────────────
    ("returns",      "processor",           "str",      ["processor", "preparer", "proc", "changed by"]),
    ("returns",      "verified",            "bool",     ["verified", "verify", "e-filed", "efiled", "e filed"]),
    ("returns",      "client_status",       "status",   ["client status", "workflow status"]),
    # ── Dates ─────────────────────────────────────────────────────────────────
    ("returns",      "intake_date",         "date",     ["int'd", "intake date", "date logged", "interview date",
                                                          "date in", "date started", "started"]),
    ("returns",      "logout_date",         "date",     ["log out", "logout", "log-out", "date completed",
                                                          "completed date"]),
    ("returns",      "updated_date",        "date",     ["updated date", "update date", "date changed",
                                                          "last change", "last changed"]),
    # "email_marker" must come BEFORE "date_emailed" so that a bare "EMAIL" column
    # is claimed by email_marker (exact 100-pt match) before date_emailed can steal it
    # via the "email" substring of "date emailed".
    ("returns",      "email_marker",        "str",      ["email", "e-mail", "email flag", "email marker"]),
    ("returns",      "date_emailed",        "date",     ["date emailed", "email date", "emailed date"]),
    ("returns",      "pickup_date",         "date",     ["pick up", "pickup", "pick-up"]),
    ("returns",      "efile_date",          "date",     ["efile date", "e-file date", "date efiled", "date e-filed"]),
    ("returns",      "ack_date",            "date",     ["ack date", "fed ack date", "ack", "acknowledgment date",
                                                          "acceptance date", "irs ack"]),
    # ── Payments ──────────────────────────────────────────────────────────────
    # "total_fee" keywords are now specific — avoid matching "total" alone which hits
    # summary/balance rows.
    ("payments",     "total_fee",           "currency", ["total fee", "ttl fee", "bal due - bill", "bill amount",
                                                          "our fee", "amount billed"]),
    ("payments",     "fee_paid",            "currency", ["fee paid", "ttl paid", "amount paid", "client payments"]),
    ("payments",     "refund_amount",       "currency", ["refund", "refund amount", "irs refund"]),
    ("payments",     "receipt_number",      "str",      ["receipt #", "receipt", "rcpt"]),
    ("payments",     "cc_fee",              "currency", ["cc fee", "credit card fee"]),
    # cash_or_qpay_ref before zelle_or_check_ref so a combined "Cash, Q Pay Zelle or CK #"
    # column goes to cash_or_qpay_ref (cash/qpay listed first in the header)
    ("payments",     "cash_or_qpay_ref",    "str",      ["cash", "q pay", "qpay", "cash or q"]),
    ("payments",     "zelle_or_check_ref",  "str",      ["zelle", "check", "ck #", "zelle or ck"]),
    # ── Forms ─────────────────────────────────────────────────────────────────
    ("return_forms", "form_1040",           "bool",     ["1040"]),
    ("return_forms", "sched_a_d",           "bool",     ["sch a", "sched a", "a/d", "a&d", "sch a & d"]),
    ("return_forms", "sched_c",             "bool",     ["sched c", "sch c", "schedule c"]),
    ("return_forms", "sched_e",             "bool",     ["sched e", "sch e", "schedule e"]),
    ("return_forms", "form_1120",           "bool",     ["1120"]),
    ("return_forms", "form_1120s",          "bool",     ["1120s"]),
    ("return_forms", "form_1065_llc",       "bool",     ["1065", "llc"]),
    # "corp officer" — removed short "co" keyword that was a false-positive magnet
    ("return_forms", "corp_officer",        "bool",     ["corp officer", "corporate officer"]),
    ("return_forms", "business_owner",      "bool",     ["bus owner", "business owner"]),
    ("return_forms", "form_990_1041",       "bool",     ["990", "1041", "990/1041"]),
    # ── Transfer / misc flags ─────────────────────────────────────────────────
    ("returns",      "transfer_2025_flag",  "bool",     ["25 transf", "2025 transf", "transfer 25", "xfer 25"]),
    ("returns",      "transfer_2026_flag",  "bool",     ["26 transf", "2026 transf", "transfer 26", "xfer 26"]),
    ("returns",      "transfer_flag",       "bool",     ["transfer", "xfer"]),
    # (email_marker defined earlier with ordering-sensitive placement)
    # "referral_flag" — removed short "ref" keyword; "refund" was a false-positive hit
    ("clients",      "referral_flag",       "bool",     ["referral flag", "referral", "referred"]),
    ("clients",      "referred_by",         "str",      ["referred by", "referrer"]),
    ("notes",        "note_text",           "str",      ["notes", "note", "comments"]),
]

# ---------------------------------------------------------------------------
# Hard-coded exact maps for known Drake export formats
# These bypass fuzzy scoring entirely — column names are fixed by Drake itself.
# skip=True → column is intentionally not imported (IRS-only fields, etc.)
# ---------------------------------------------------------------------------

# (table, field, field_type, skip, note_for_ui)
_TAXOPS_EXACT: dict[str, tuple[str, str, str, bool, str]] = {
    "TAXPAYER LAST NAME":  ("clients",  "last_name",     "str",      False, ""),
    "TAXPAYER FIRST NAME": ("clients",  "first_name",    "str",      False, ""),
    # "Return Type" carries the IRS form code (1040, 1040NR, EXT, 4868…).
    # We convert it to an is_extension flag in drake_importer; show it mapped there.
    "RETURN TYPE":         ("returns",  "is_extension",  "str",      False, "EXT/4868→extension flag"),
    # "Filing Status" is the IRS filing code (1=Single, 2=MFJ, …) — not our workflow status.
    "FILING STATUS":       ("",         "",              "",         True,  "IRS filing code — skipped"),
    "DATE STARTED":        ("returns",  "intake_date",   "date",     False, ""),
    "DATE COMPLETED":      ("returns",  "logout_date",   "date",     False, ""),
    "DATE CHANGED":        ("returns",  "updated_date",  "date",     False, ""),
    "E-FILED":             ("returns",  "verified",      "bool",     False, "Yes/No → e-filed flag"),
    "FED ACK DATE":        ("returns",  "ack_date",      "date",     False, ""),
    # "Balance Due" = amount the client owes the IRS (stored alongside refund_amount).
    "BALANCE DUE":         ("payments", "balance_due",   "currency", False, "IRS balance owed by client"),
    "REFUND":              ("payments", "refund_amount", "currency", False, ""),
    # "Bal Due - BILL" = what we charge the client.
    "BAL DUE - BILL":      ("payments", "total_fee",     "currency", False, "Our billing amount"),
}

_CSM_EXACT: dict[str, tuple[str, str, str, bool, str]] = {
    "CLIENT NAME":      ("clients",  "display_name",  "str",      False, "LAST, FIRST format"),
    "ID (LAST 4)":      ("clients",  "ssn_last4",     "str",      False, "SSN last 4 digits"),
    "TYPE":             ("returns",  "is_extension",  "str",      False, "Form type"),
    "PREPARER":         ("returns",  "processor",     "str",      False, ""),
    "STATUS":           ("returns",  "client_status", "status",   False, "Drake status → mapped"),
    "STARTED":          ("returns",  "intake_date",   "date",     False, ""),
    "COMPLETED":        ("returns",  "logout_date",   "date",     False, ""),
    "LAST CHANGE":      ("returns",  "updated_date",  "date",     False, ""),
    "CHANGED BY":       ("returns",  "processor",     "str",      True,  "Stored as note"),
    "REFUND":           ("payments", "refund_amount", "currency", False, ""),
    "TOTAL BILL":       ("payments", "total_fee",     "currency", False, ""),
    "BANK DEPOSITS":    ("payments", "bank_deposit",  "currency", False, ""),
    "CLIENT PAYMENTS":  ("payments", "fee_paid",      "currency", False, ""),
    "AMOUNT OWED":      ("",         "",              "",         True,  "Derived — skipped"),
}

_TAXOPS_SIGNATURE = frozenset({"TAXPAYER LAST NAME", "DATE STARTED", "E-FILED"})
_CSM_SIGNATURE    = frozenset({"CLIENT NAME", "PREPARER", "STARTED", "COMPLETED"})


def _apply_exact_map(
    merged: list[str],
    exact: dict[str, tuple[str, str, str, bool, str]],
) -> list[ColumnMapping]:
    """Build a ColumnMapping list from a hardcoded exact header→field table."""
    cols: list[ColumnMapping] = []
    for ci, hdr in enumerate(merged):
        key = hdr.strip().upper()
        if key in exact:
            tbl, fld, ftype, skip, note = exact[key]
            cols.append(ColumnMapping(ci, hdr, tbl, fld, ftype, 100,
                                      skip=skip, skip_reason=note if skip else ""))
        else:
            cols.append(ColumnMapping(ci, hdr, "", "", "", 0, skip=True))
    return cols


# Status value normalization
STATUS_NORMALIZE: dict[str, str] = {
    "PRIOR HOLD":   "PROCESSING",
    "PRIOR PROC":   "PROCESSING",
    "PRIOR PROCESSING": "PROCESSING",
    "LOGOUT":       "LOG OUT",
    "LOG-OUT":      "LOG OUT",
    "LOGGED OUT":   "LOG OUT",
    "LOGGEDIN":     "LOG IN",
    "LOGGED IN":    "LOG IN",
    "LOG-IN":       "LOG IN",
    "EFILED":       "EFILE",
    "E-FILED":      "EFILE",
    "E FILED":      "EFILE",
    "EFILE READY":  "EFILE READY",
    "READY":        "EFILE READY",
    "FINALIZED":    "FINALIZE",
    "PICK UP":      "PICKUP",
    "PICK-UP":      "PICKUP",
}

VALID_STATUSES = {
    "LOG IN", "PENDING INTAKE", "PROCESSING", "FINALIZE", "PICKUP",
    "EFILE READY", "EFILE", "LOG OUT",
}

# Rows that look like metadata/totals (skip them)
_SKIP_PATTERNS = [
    re.compile(r"^\s*\$[\d,]+", re.I),   # starts with a dollar amount
    re.compile(r"2025 TAX LOG", re.I),
    re.compile(r"TTL FEE", re.I),
]


@dataclass
class ColumnMapping:
    col_index: int
    raw_header: str
    table: str
    field: str
    field_type: str
    confidence: int        # 0-100
    skip: bool = False     # True = ignore this column
    skip_reason: str = ""  # Human-readable note shown in the UI when skip=True


@dataclass
class AnalysisResult:
    header_row_index: int          # 0-based row where data headers were found
    data_start_index: int          # 0-based row where actual data begins
    columns: list[ColumnMapping]
    sample_rows: list[list[str]]   # first 10 data rows (cleaned)
    total_rows: int
    warnings: list[str] = field(default_factory=list)
    merged_headers: list[str] = field(default_factory=list)  # one label per column (multi-row headers merged)


def _clean(val: str) -> str:
    return " ".join(val.split()).strip()


def _score(header: str, keywords: list[str]) -> int:
    """Return 0-100 match score between a header string and a keyword list.

    Short keywords (≤ 3 chars) are only matched as whole words to prevent
    false-positive substring hits (e.g. "ty" inside "return type", "co" inside
    "completed", "ref" inside "refund").
    """
    h = header.lower().strip()
    if not h:
        return 0
    h_words = set(h.split())
    for kw in keywords:
        k = kw.lower()
        if h == k:
            return 100
        if len(k) <= 3:
            # Short keyword: only award a match if it appears as a whole word
            if k in h_words:
                return 80
            continue
        if k in h or h in k:
            return 80
        # word overlap
        k_words = set(k.split())
        overlap = h_words & k_words
        if overlap:
            score = int(70 * len(overlap) / max(len(h_words), len(k_words)))
            if score >= 50:
                return score
    return 0


def _is_skip_row(row: list[str]) -> bool:
    joined = ",".join(row)
    for pat in _SKIP_PATTERNS:
        if pat.search(joined):
            return True
    # completely empty row
    if not any(c.strip() for c in row):
        return True
    return False


def _is_header_row(row: list[str]) -> bool:
    """Heuristic: a header row has several recognizable field-name tokens."""
    text = " ".join(c.lower().strip() for c in row)
    hits = 0
    triggers = [
        "log", "taxpayer", "processor", "status", "fee", "verified",
        "receipt", "1040", "last", "first", "yr", "year", "transfer",
    ]
    for t in triggers:
        if t in text:
            hits += 1
    return hits >= 4


def _best_field(header: str) -> tuple[str, str, str, int] | None:
    """Return (table, field, field_type, confidence) for the best matching field."""
    best_score = 0
    best = None
    for table, db_field, ftype, keywords in KNOWN_FIELDS:
        s = _score(header, keywords)
        if s > best_score:
            best_score = s
            best = (table, db_field, ftype, s)
    if best and best_score >= 50:
        return best
    return None


def analyze(file_bytes: bytes, filename: str = "") -> AnalysisResult:
    """
    Analyze a CSV file and return a detected column mapping with confidence scores.
    Handles multi-row headers, metadata rows, and irregular spacing.
    """
    text = file_bytes.decode("utf-8-sig", errors="replace")
    rows: list[list[str]] = list(csv.reader(io.StringIO(text)))

    warnings: list[str] = []

    # ── Find header rows ──────────────────────────────────────────────────────
    header_indices: list[int] = []
    for i, row in enumerate(rows[:10]):
        if _is_header_row(row):
            header_indices.append(i)
        if len(header_indices) >= 2:
            break

    if not header_indices:
        # Fall back: first non-skip row
        for i, row in enumerate(rows):
            if not _is_skip_row(row):
                header_indices = [i]
                break
        warnings.append("Could not confidently detect header row — using best guess.")

    # ── Merge multi-row headers ───────────────────────────────────────────────
    max_cols = max(len(r) for r in rows[:10]) if rows else 0
    merged: list[str] = [""] * max_cols

    for hi in header_indices:
        for ci, val in enumerate(rows[hi]):
            cleaned = _clean(val)
            if cleaned and not merged[ci]:
                merged[ci] = cleaned
            elif cleaned and merged[ci] and cleaned.lower() != merged[ci].lower():
                merged[ci] = f"{merged[ci]} {cleaned}"

    # ── Find data start ───────────────────────────────────────────────────────
    data_start = (header_indices[-1] + 1) if header_indices else 0
    # Skip blank / metadata rows immediately after headers
    while data_start < len(rows) and _is_skip_row(rows[data_start]):
        data_start += 1

    # ── Map columns ───────────────────────────────────────────────────────────
    # Check for known Drake export formats first — these have fixed column names
    # so we use exact lookup instead of fuzzy scoring.
    upper_headers = {h.strip().upper() for h in merged if h.strip()}
    if _TAXOPS_SIGNATURE.issubset(upper_headers):
        columns = _apply_exact_map(merged, _TAXOPS_EXACT)
        warnings.append("Detected Drake Tax Ops CSV Export — using exact column map.")
    elif _CSM_SIGNATURE.issubset(upper_headers):
        columns = _apply_exact_map(merged, _CSM_EXACT)
        warnings.append("Detected Drake CSM Data Export — using exact column map.")
    else:
        columns = []
        seen_fields: set[str] = set()

        for ci, header in enumerate(merged):
            match = _best_field(header)
            if match:
                table, db_field, ftype, conf = match
                # Avoid duplicate field assignments (keep highest confidence)
                key = f"{table}.{db_field}"
                if key in seen_fields:
                    columns.append(ColumnMapping(ci, header, "", "", "", 0, skip=True))
                    continue
                seen_fields.add(key)
                columns.append(ColumnMapping(ci, header, table, db_field, ftype, conf))
            else:
                columns.append(ColumnMapping(ci, header, "", "", "", 0, skip=True))

    # ── Sample rows ───────────────────────────────────────────────────────────
    sample: list[list[str]] = []
    for row in rows[data_start:data_start + 10]:
        if not _is_skip_row(row):
            # pad to max_cols
            padded = row + [""] * (max_cols - len(row))
            sample.append([_clean(v) for v in padded])

    total_data = sum(
        1 for row in rows[data_start:]
        if not _is_skip_row(row) and any(c.strip() for c in row)
    )

    return AnalysisResult(
        header_row_index=header_indices[0] if header_indices else 0,
        data_start_index=data_start,
        columns=columns,
        sample_rows=sample,
        total_rows=total_data,
        warnings=warnings,
        merged_headers=list(merged),
    )


def normalize_status(raw: str) -> str:
    """Normalize a raw status string to a valid internal status."""
    cleaned = " ".join(raw.upper().split())
    if cleaned in VALID_STATUSES:
        return cleaned
    return STATUS_NORMALIZE.get(cleaned, "PROCESSING")


def iter_data_rows(
    file_bytes: bytes,
    result: AnalysisResult,
) -> list[dict[str, Any]]:
    """
    Given an analysis result, iterate over data rows and return
    a list of dicts keyed by 'table.field'.
    """
    text = file_bytes.decode("utf-8-sig", errors="replace")
    rows = list(csv.reader(io.StringIO(text)))

    mapped_cols = [c for c in result.columns if not c.skip and c.field]
    output = []

    for row in rows[result.data_start_index:]:
        if _is_skip_row(row):
            continue
        if not any(c.strip() for c in row):
            continue
        padded = row + [""] * (len(result.columns) - len(row))
        record: dict[str, Any] = {}
        for cm in mapped_cols:
            raw_val = _clean(padded[cm.col_index]) if cm.col_index < len(padded) else ""
            record[f"{cm.table}.{cm.field}"] = raw_val
        output.append(record)

    return output
