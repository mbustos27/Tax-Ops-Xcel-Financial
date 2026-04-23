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
    ("returns",      "log_number",         "str",      ["log 2025", "log#", "log #", "log number", "2025"]),
    ("clients",      "prior_year_log",      "str",      ["log 2024", "prior log", "log 2023", "prev log"]),
    ("clients",      "last_name",           "str",      ["last", "last name", "taxpayer last", "surname"]),
    ("clients",      "first_name",          "str",      ["first", "first name", "taxpayer first", "given"]),
    ("clients",      "display_name",        "str",      ["taxpayer name", "name", "tax payer name"]),
    ("returns",      "tax_year",            "int",      ["yr", "year", "tax year", "ty"]),
    ("returns",      "processor",           "str",      ["processor", "proc", "preparer", "by"]),
    ("returns",      "verified",            "bool",     ["verified", "ver", "verify"]),
    ("returns",      "client_status",       "status",   ["client status", "status"]),
    ("returns",      "intake_date",         "date",     ["int'd", "intake", "date logged", "interview date", "date in", "int"]),
    ("returns",      "transfer_2025_flag",  "bool",     ["25 transf", "2025 transf", "transfer 25", "xfer 25"]),
    ("returns",      "transfer_2026_flag",  "bool",     ["26 transf", "2026 transf", "transfer 26", "xfer 26"]),
    ("returns",      "email_marker",        "str",      ["email", "e-mail", "email marker"]),
    ("returns",      "date_emailed",        "date",     ["date emailed", "emailed", "email date"]),
    ("returns",      "pickup_date",         "date",     ["pick up", "pickup", "pick-up"]),
    ("returns",      "logout_date",         "date",     ["log out", "logout", "log-out", "date out"]),
    ("payments",     "total_fee",           "currency", ["total fee", "ttl fee", "total", "fee", "paid"]),
    ("payments",     "receipt_number",      "str",      ["receipt #", "receipt", "rcpt"]),
    ("payments",     "fee_paid",            "currency", ["fee paid", "ttl paid", "amount paid"]),
    ("payments",     "cc_fee",              "currency", ["cc fee", "cc", "credit card"]),
    ("payments",     "zelle_or_check_ref",  "str",      ["zelle", "check", "ck #", "zelle or ck"]),
    ("payments",     "cash_or_qpay_ref",    "str",      ["cash", "q pay", "qpay", "cash or q"]),
    ("return_forms", "form_1040",           "bool",     ["1040"]),
    ("return_forms", "sched_a_d",           "bool",     ["sch a", "sched a", "a/d", "a&d", "sch a & d"]),
    ("return_forms", "sched_c",             "bool",     ["sched c", "sch c", "schedule c"]),
    ("return_forms", "sched_e",             "bool",     ["sched e", "sch e", "schedule e"]),
    ("return_forms", "form_1120",           "bool",     ["1120"]),
    ("return_forms", "form_1120s",          "bool",     ["1120s"]),
    ("return_forms", "form_1065_llc",       "bool",     ["1065", "llc"]),
    ("return_forms", "corp_officer",        "bool",     ["corp officer", "co"]),
    ("return_forms", "business_owner",      "bool",     ["bus owner", "bu", "business"]),
    ("returns",      "is_amended",          "bool",     ["1040x", "amended", "amend"]),
    ("returns",      "has_w7",              "bool",     ["w7", "w-7"]),
    ("return_forms", "form_990_1041",       "bool",     ["990", "1041", "990/1041"]),
    ("returns",      "is_extension",        "bool",     ["ext", "extension", "xt"]),
    ("returns",      "transfer_flag",       "bool",     ["transfer", "xfer"]),
    ("returns",      "updated_date",        "date",     ["updated", "update date"]),
    ("clients",      "referral_flag",       "bool",     ["referral", "ref"]),
    ("clients",      "referred_by",         "str",      ["referred by", "referrer"]),
    ("notes",        "note_text",           "str",      ["notes", "note", "comments"]),
]

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
    "LOG IN", "PROCESSING", "FINALIZE", "PICKUP",
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


@dataclass
class AnalysisResult:
    header_row_index: int          # 0-based row where data headers were found
    data_start_index: int          # 0-based row where actual data begins
    columns: list[ColumnMapping]
    sample_rows: list[list[str]]   # first 10 data rows (cleaned)
    total_rows: int
    warnings: list[str] = field(default_factory=list)


def _clean(val: str) -> str:
    return " ".join(val.split()).strip()


def _score(header: str, keywords: list[str]) -> int:
    """Return 0-100 match score between a header string and a keyword list."""
    h = header.lower().strip()
    if not h:
        return 0
    for kw in keywords:
        k = kw.lower()
        if h == k:
            return 100
        if k in h or h in k:
            return 80
        # word overlap
        h_words = set(h.split())
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
        "receipt", "1040", "last", "first", "yr", "transfer",
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
    columns: list[ColumnMapping] = []
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
