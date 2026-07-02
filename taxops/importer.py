from __future__ import annotations

import csv
import io
import json
import re
import sqlite3
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List

from config import MANUAL_LOG_SOURCE
from csv_analyzer import _is_skip_row, analyze
from csv_analyzer import _clean as _clean_header_cell
from events import create_status_events
from preparer import normalize_preparer
from normalizer import (
    build_header_lookup,
    canonical_header,
    get_value,
    get_value_any,
    normalize_bool_flag,
    normalize_currency,
    normalize_date,
    normalize_status,
    normalize_string,
    normalize_tax_year,
    is_locked_status,
)
from name_matcher import (
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    score_client_names_pair,
    spouse_parts_from_display_line,
    split_joint_first_column,
    split_spouse_name_chunk,
)
from utils import ImportStats, ImportResult, now

# ---------------------------------------------------------------------------
# Canonical tax-log CSV field mapping
# Read from actual "TAX LOG 2025 Live.csv" / "TAXOPS.csv" headers.
# ---------------------------------------------------------------------------

TAX_LOG_FIELD_MAP: Dict[str, str] = {
    # Tax-log column header → TaxOps DB field
    "LOG 2025":              "log_number",       # Intake season sequence number
    "LOG 2024":              "log_number",        # Prior-season reference
    "LAST":                  "last_name",
    "FIRST":                 "first_name",
    "TAX PAYER NAME (S)":    "display_name",
    "YR":                    "tax_year",
    "PROCESSOR":             "processor",
    "VERIFIED":              "verified",
    "CLIENT STATUS":         "client_status",
    "INT'D":                 "intake_date",
    "25 TRANSF":             "transfer_2025_flag",
    "26 TRANSF":             "transfer_2026_flag",
    "EMAIL":                 "email_marker",
    "DATE EMAILED":          "date_emailed",
    "PICK UP":               "pickup_date",
    "LOG OUT":               "logout_date",
    "TOTAL FEE":             "total_fee",
    "RECEIPT #":             "receipt_number",
    "FEE PAID":              "fee_paid",
    "CC FEE":                "cc_fee",
    "ZELLE/CHECK":           "zelle_or_check_ref",
    "CASH/QPAY":             "cash_or_qpay_ref",
    "Referral":              "referral_flag",
    "Referred By":           "referred_by",
}

_LOG_COL_RE = re.compile(r"^LOG (20\d{2})$")

# Canonical header synonyms for tax year column (Excel often exports "Year" instead of "YR").
_TAX_YEAR_SYNONYMS: tuple[str, ...] = ("YR", "YEAR", "TAX YEAR", "TY", "TAX YR")

# Gap between fuzzy name scores when two plausible DB rows collide (same tax year scope).
FUZZY_AMBIGUOUS_MARGIN = 8


def resolve_manual_log_column_key(
    header_lookup: Dict[str, str], tax_year_hint: int | None
) -> tuple[str | None, str | None]:
    """
    Pick which ``LOG yyyy`` column carries the office-assigned log number.

    - Prefer exact ``LOG <tax_year_hint>`` when Source compare passes a year and that column exists.
    - Otherwise prefer the newest ``LOG yyyy`` with ``yyyy <= tax_year_hint`` (e.g. file has only
      LOG 2024/2025 but you are comparing DB TY 2026 → use LOG 2025).
    - If all ``LOG yyyy`` are strictly after the hint, use the oldest such column with a note.

    Returns ``(canonical_key_or_none, user_facing_note_when_not_exact)``.
    """
    keys = [k for k in header_lookup if _LOG_COL_RE.match(k)]
    if not keys:
        return None, None

    year_key: list[tuple[int, str]] = []
    for k in keys:
        m = _LOG_COL_RE.match(k)
        if m:
            year_key.append((int(m.group(1)), k))
    year_key.sort(key=lambda x: x[0])

    if tax_year_hint is None:
        best_k = max(year_key, key=lambda x: x[0])[1]
        return best_k, None

    exact_canon = canonical_header(f"LOG {tax_year_hint}")
    if exact_canon in header_lookup:
        return exact_canon, None

    le = [(y, k) for y, k in year_key if y <= tax_year_hint]
    if le:
        picked_y, picked_k = max(le, key=lambda x: x[0])
        return picked_k, (
            f"Using LOG {picked_y} as file default when no LOG {tax_year_hint} column exists. "
            "Rows still read log numbers from LOG matching each row's YR when that column is present."
        )

    picked_y, picked_k = min(year_key, key=lambda x: x[0])
    return picked_k, (
        f"Using LOG {picked_y} for log numbers (only \"LOG\" columns after {tax_year_hint} were found)."
    )


def effective_log_canonical_key(
    header_lookup: Dict[str, str],
    row_tax_year: int | None,
    file_fallback_log_key: str,
    row: Dict[str, str] | None = None,
) -> str:
    """
    Return the best LOG yyyy column key for this row.

    Priority:
    1. File-level intake log key (e.g. LOG 2025 for a 2025 intake log) — if non-empty in this row.
       This is the definitive intake-season sequence number and takes precedence over the tax-year
       column.  A client filing a 2024 or 2023 return during the 2025 season gets a LOG 2025 number
       that IS their primary log number for this season; LOG 2024 is their prior-year reference.
    2. Exact ``LOG {row_tax_year}`` — only used when the file-level key is absent/empty for this row.
    3. ``resolve_manual_log_column_key`` fallback (nearest LOG column).
    """
    # 1. Prefer the file's own intake-season log key.
    if file_fallback_log_key:
        raw_key = header_lookup.get(file_fallback_log_key, file_fallback_log_key)
        if row is None or (row.get(raw_key) or "").strip():
            return file_fallback_log_key

    # 2. Fall back to the row's own tax-year log column.
    if row_tax_year is not None:
        exact_canon = canonical_header(f"LOG {row_tax_year}")
        if exact_canon in header_lookup:
            raw_key = header_lookup[exact_canon]
            if row is None or (row.get(raw_key) or "").strip():
                return exact_canon

    # 3. Last resort: nearest LOG column from resolve helper.
    resolved, _ = resolve_manual_log_column_key(header_lookup, row_tax_year)
    if resolved:
        raw_key = header_lookup.get(resolved, resolved)
        if row is None or (row.get(raw_key) or "").strip():
            return resolved

    return file_fallback_log_key


def resolve_tax_year_column_key(header_lookup: Dict[str, str]) -> str | None:
    """First matching tax-year column (YR, Year, Tax year, …); ``header_lookup`` keys are canonical."""
    for syn in _TAX_YEAR_SYNONYMS:
        c = canonical_header(syn)
        if c in header_lookup:
            return c
    return None


def validate_manual_log_headers(
    header_lookup: Dict[str, str], tax_year_hint: int | None
) -> tuple[str | None, str | None, str | None, str | None]:
    """
    Returns ``(resolved_LOG_yyyy_canonical_key, resolved_tax_year_canonical_key, error_or_none, fallback_note)``.
    ``fallback_note`` is set when ``LOG <season>`` needed a different ``LOG yyyy`` column.
    """
    yr_key = resolve_tax_year_column_key(header_lookup)
    if not yr_key:
        return (
            None,
            None,
            "CSV missing a tax-year column (looks for "
            + ", ".join(_TAX_YEAR_SYNONYMS)
            + "). If Excel put the title rows above columns, Save As CSV again or use the importer after headers are detected.",
            None,
        )

    last_ok = canonical_header("LAST") in header_lookup or canonical_header("TAX PAYER NAME (S) LAST") in header_lookup
    if not last_ok:
        return None, None, "CSV missing Last name column: need LAST or TAX PAYER NAME (S) LAST", None

    if canonical_header("FIRST") not in header_lookup:
        return None, None, "CSV missing required column: FIRST", None

    log_key, fallback_note = resolve_manual_log_column_key(header_lookup, tax_year_hint)
    if not log_key:
        log_like = [k for k in header_lookup if "LOG" in k]
        log_like.sort()
        extra = ", ".join(log_like[:12]) if log_like else "none"
        return (
            None,
            None,
            f"CSV has no LOG yyyy column for assigned log numbers (expected something like LOG 2025). Found: {extra}",
            None,
        )

    return log_key, yr_key, None, fallback_note


@dataclass
class PreparedManualCsv:
    """Result of scanning a manual office log once."""

    path: str
    fieldnames: List[str]
    all_rows: List[List[str]]
    data_start: int
    header_lookup: Dict[str, str]
    log_key: str  # file-level fallback when choosing among LOG yyyy columns; rows prefer LOG matching YR
    yr_key: str
    fallback_note: str | None


def prepare_manual_csv(csv_path: str, tax_year_hint: int | None) -> PreparedManualCsv:
    """
    Detect headers (including Excel title rows), validate columns, resolve LOG yyyy + tax-year keys.
    """
    fieldnames, all_rows, data_start, _ = _manual_csv_layout(csv_path)
    header_lookup = build_header_lookup(fieldnames)
    log_key, yr_key, req_err, fb = validate_manual_log_headers(header_lookup, tax_year_hint)
    if req_err or not log_key or not yr_key:
        raise ValueError(req_err or "manual log validation failed")
    return PreparedManualCsv(csv_path, fieldnames, all_rows, data_start, header_lookup, log_key, yr_key, fb)


def iterate_manual_prep(prep: PreparedManualCsv):
    """Yield ``(row_number, normalized | None, warnings, error_or None)`` for a prepared manual CSV."""
    for row_number, row_cells in enumerate(prep.all_rows[prep.data_start :], start=prep.data_start + 1):
        if _is_skip_row(row_cells):
            continue
        if not any(c.strip() for c in row_cells):
            continue
        padded = row_cells + [""] * (len(prep.fieldnames) - len(row_cells))
        row = {prep.fieldnames[i]: padded[i] for i in range(len(prep.fieldnames))}
        try:
            normalized, warnings = _normalize_row(row, prep.header_lookup, prep.log_key, prep.yr_key)
            if not normalized["returns"]["log_number"] or normalized["returns"]["tax_year"] is None:
                yield row_number, None, [], "Missing required values: LOG yyyy column and/or tax year column"
            elif not normalized["clients"]["last_name"] or not normalized["clients"]["first_name"]:
                yield row_number, None, [], "Missing required values: LAST (or TAX PAYER NAME (S) LAST) and/or FIRST"
            else:
                yield row_number, normalized, warnings, None
        except Exception as exc:  # noqa: BLE001
            yield row_number, None, [], str(exc)


def _stable_fieldnames_merged(merged_headers: List[str]) -> List[str]:
    """Unique non-empty-ish headers for DictReader-style row mapping (blank cells get placeholder names)."""
    out: List[str] = []
    for i, raw in enumerate(merged_headers):
        t = _clean_header_cell(raw)
        out.append(t if t else f"_BLANK_{i}")
    return out


def _manual_csv_layout(csv_path: str):
    """
    Read an office export with the same header detection as csv_analyzer (title rows, merged headers).
    Returns (fieldnames, all_rows, data_start_index, analysis_result).
    """
    raw = Path(csv_path).read_bytes()
    result = analyze(raw, Path(csv_path).name)
    merged = result.merged_headers
    if not merged:
        raise ValueError("CSV has no header row (try re-saving from Excel or check the file is not empty).")
    names = _stable_fieldnames_merged(merged)
    text = raw.decode("utf-8-sig", errors="replace")
    all_rows = list(csv.reader(io.StringIO(text)))
    return names, all_rows, result.data_start_index, result

def process_csv(conn: sqlite3.Connection, csv_path: str, batch_id: int, source_file: str) -> ImportResult:
    stats = ImportResult(source=MANUAL_LOG_SOURCE, filename=source_file)
    _t0 = time.monotonic()
    prep = prepare_manual_csv(csv_path, tax_year_hint=None)
    returns_cache: Dict[int, List[sqlite3.Row]] = {}

    for idx, row_cells in enumerate(prep.all_rows[prep.data_start :], start=prep.data_start + 1):
        if _is_skip_row(row_cells):
            continue
        if not any(c.strip() for c in row_cells):
            continue
        stats.row_count += 1
        row_number = idx
        padded = row_cells + [""] * (len(prep.fieldnames) - len(row_cells))
        row = {prep.fieldnames[i]: padded[i] for i in range(len(prep.fieldnames))}
        # Skip truly blank rows (no name and no year) — these are spacer rows in Excel
        last_raw = (row.get(prep.header_lookup.get("taxpayer name s last", "TAX PAYER NAME (S) LAST"), "")
                    or row.get(prep.header_lookup.get("last name", "LAST"), "")
                    or row.get(prep.header_lookup.get("last", "LAST"), "")).strip()
        yr_raw  = (row.get(prep.header_lookup.get(prep.yr_key, prep.yr_key), "") if prep.yr_key else "").strip()
        if not last_raw and not yr_raw:
            continue
        try:
            normalized, warnings = _normalize_row(row, prep.header_lookup, prep.log_key, prep.yr_key)
            # If year is missing but the row has a name, default to the log's own year (intake year)
            if normalized["returns"]["tax_year"] is None and normalized["clients"]["last_name"]:
                normalized["returns"]["tax_year"] = getattr(prep, "tax_year_hint", None) or 2025
            if not normalized["returns"]["log_number"] or normalized["returns"]["tax_year"] is None:
                raise ValueError(
                    "Missing required values: office log #(LOG yyyy column) and/or tax year column"
                )
            # Business/entity returns have no first name — only last_name (the entity name) is required.
            if not normalized["clients"]["last_name"]:
                raise ValueError("Missing required values: LAST (or TAX PAYER NAME (S) LAST) and/or FIRST")

            ty = normalized["returns"]["tax_year"]
            prefetch = returns_cache.setdefault(ty, fetch_returns_clients_for_tax_year(conn, ty))
            match = _match_return(conn, normalized, prefetch)

            if match["ambiguous"]:
                _insert_review_row(conn, batch_id, row_number, row, "AMBIGUOUS_MATCH")
                _insert_import_row(conn, batch_id, row_number, row, "REVIEW", "; ".join(warnings) if warnings else None)
                stats.review_count += 1
                continue

            if match["needs_review"]:
                reason = match.get("review_reason") or "REVIEW"
                _insert_review_row(conn, batch_id, row_number, row, f"MANUAL_{reason}")
                _insert_import_row(conn, batch_id, row_number, row, "REVIEW", "; ".join(warnings) if warnings else None)
                stats.review_count += 1
                continue

            client_id, created_client, updated_client = _upsert_client(conn, normalized["clients"], match["client_id"])
            return_id, created_return, updated_return, before_row, after_row = _upsert_return(
                conn, client_id, normalized["returns"], match["return_id"], apply_manual_log_number=True
            )
            _upsert_forms(conn, return_id, normalized["return_forms"])
            _upsert_payment(conn, return_id, normalized["payments"])

            if _insert_note_if_new(conn, return_id, normalized["notes"]["note_text"]):
                stats.notes_created += 1

            stats.events_created += create_status_events(
                conn=conn,
                return_id=return_id,
                before=before_row,
                after=after_row,
                import_time=now(),
                source_file=source_file,
            )
            stats.created_clients += int(created_client)
            stats.updated_clients += int(updated_client)
            stats.created_returns += int(created_return)
            stats.updated_returns += int(updated_return)
            stats.success_count += 1

            action = "CREATED" if created_return else "UPDATED"
            _insert_import_row(conn, batch_id, row_number, row, action, "; ".join(warnings) if warnings else None)
        except Exception as exc:
            _insert_import_row(conn, batch_id, row_number, row, "ERROR", str(exc))
            stats.error_count += 1
            stats.errors.append(f"Row {row_number}: {exc}")
    stats.duration_seconds = time.monotonic() - _t0
    return stats



def fetch_returns_clients_for_tax_year(conn: sqlite3.Connection, tax_year: int) -> List[sqlite3.Row]:
    """All DB returns (+ client names) for a tax season — prefetch for manual matching."""
    return conn.execute(
        """
        SELECT
          r.id AS return_id,
          r.client_id,
          r.log_number AS return_log_number,
          c.last_name,
          c.first_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.tax_year = ?
        """,
        (tax_year,),
    ).fetchall()


def _augment_clients_joint_spouse(clients: Dict[str, Any]) -> None:
    """
    Derive spouse fields from LAST/FIRST/display when logs encode joint households
    (e.g. ``JOHN & JANE`` in FIRST or comma+ampersand lines in ``TAX PAYER NAME (S)``).
    """
    disp = (clients.get("display_name") or "").strip()
    fn = (clients.get("first_name") or "").strip()
    primary_fn, spouse_first_chunk = split_joint_first_column(fn)
    disp_spouse = spouse_parts_from_display_line(disp)
    if disp_spouse:
        sf, sl = disp_spouse
        if sf:
            clients["spouse_first_name"] = sf.strip()
        if sl:
            clients["spouse_last_name"] = sl.strip()
        return

    if spouse_first_chunk:
        clients["first_name"] = primary_fn
        sf2, sl2 = split_spouse_name_chunk(spouse_first_chunk)
        if sf2:
            clients["spouse_first_name"] = sf2
        if sl2:
            clients["spouse_last_name"] = sl2


def _fuzzy_pick_for_manual(
    csv_last: str,
    csv_first: str,
    year_rows: List[sqlite3.Row],
    *,
    excluded_return_ids: set[int],
    fuzzy_ambiguous_margin: int = FUZZY_AMBIGUOUS_MARGIN,
) -> Dict[str, Any]:
    """Best DB return (same tax year) by fuzzy name excluding recycled LOG misses."""
    scored: List[tuple[int, int, int]] = []
    for row in year_rows:
        rid = int(row["return_id"])
        if rid in excluded_return_ids:
            continue
        s = score_client_names_pair(
            csv_last,
            csv_first,
            row["last_name"],
            row["first_name"],
        )
        scored.append((s, rid, int(row["client_id"])))
    if not scored:
        return _empty_manual_match_payload()
    scored.sort(key=lambda x: (-x[0], x[1]))
    best_s, best_rid, best_cid = scored[0]
    second_s = scored[1][0] if len(scored) > 1 else -1

    ambiguous = False
    if len(scored) > 1 and second_s >= REVIEW_THRESHOLD and (best_s - second_s < fuzzy_ambiguous_margin):
        ambiguous = True

    if ambiguous:
        return {
            "return_id": None,
            "client_id": None,
            "ambiguous": True,
            "needs_review": False,
            "review_reason": None,
        }

    if best_s < REVIEW_THRESHOLD:
        return _empty_manual_match_payload()

    if ACCEPT_THRESHOLD <= best_s:
        return {
            "return_id": best_rid,
            "client_id": best_cid,
            "ambiguous": False,
            "needs_review": False,
            "review_reason": None,
        }

    return {
        "return_id": None,
        "client_id": None,
        "ambiguous": False,
        "needs_review": True,
        "review_reason": "FUZZY_NAME_REVIEW",
    }


def _empty_manual_match_payload() -> Dict[str, Any]:
    return {
        "return_id": None,
        "client_id": None,
        "ambiguous": False,
        "needs_review": False,
        "review_reason": None,
    }


def _normalize_row(
    row: Dict[str, str],
    header_lookup: Dict[str, str],
    file_fallback_log_key: str,
    tax_year_canon_key: str,
) -> tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []
    tk = header_lookup.get(tax_year_canon_key)
    tax_year_val = normalize_string(row.get(tk)) if tk else None
    tax_year = normalize_tax_year(tax_year_val)

    log_ck = effective_log_canonical_key(header_lookup, tax_year, file_fallback_log_key, row)

    intake_raw = get_value_any(row, header_lookup, ("INT'D", "DATE INT'D"))
    intake_date, warn = normalize_date(intake_raw)
    if warn:
        warnings.append(warn)
    date_emailed, warn = normalize_date(get_value(row, header_lookup, "DATE EMAILED"))
    if warn:
        warnings.append(warn)
    pickup_date, warn = normalize_date(get_value(row, header_lookup, "PICK UP"))
    if warn:
        warnings.append(warn)
    logout_date, warn = normalize_date(get_value(row, header_lookup, "LOG OUT"))
    if warn:
        warnings.append(warn)
    updated_date, warn = normalize_date(get_value(row, header_lookup, "UPDATED"))
    if warn:
        warnings.append(warn)

    payload: Dict[str, Any] = {
            "clients": {
                "last_name": normalize_string(
                    get_value_any(row, header_lookup, ("LAST", "TAX PAYER NAME (S) LAST"))
                ),
                "first_name": normalize_string(get_value(row, header_lookup, "FIRST")),
                "display_name": normalize_string(get_value(row, header_lookup, "TAX PAYER NAME (S)")),
                "referral_flag": normalize_bool_flag(get_value(row, header_lookup, "Referral")),
                "referred_by": normalize_string(get_value(row, header_lookup, "Referred By")),
            },
            "returns": {
                "log_number": normalize_string(get_value(row, header_lookup, log_ck)),
                "tax_year": tax_year,
                "processor": normalize_preparer(
                    normalize_string(get_value(row, header_lookup, "PROCESSOR"))
                ),
                "verified": normalize_bool_flag(get_value(row, header_lookup, "VERIFIED")),
                "client_status": normalize_status(get_value(row, header_lookup, "CLIENT STATUS")),
                "intake_date": intake_date,
                "transfer_2025_flag": normalize_bool_flag(get_value(row, header_lookup, "25 TRANSF")),
                "transfer_2026_flag": normalize_bool_flag(get_value(row, header_lookup, "26 TRANSF")),
                "email_marker": normalize_string(get_value(row, header_lookup, "EMAIL")),
                "date_emailed": date_emailed,
                "pickup_date": pickup_date,
                "logout_date": logout_date,
                "updated_date": updated_date,
                "is_amended": normalize_bool_flag(get_value(row, header_lookup, "1040X")),
                "has_w7": normalize_bool_flag(get_value(row, header_lookup, "W7")),
                "is_extension": normalize_bool_flag(get_value(row, header_lookup, "EXT")),
                "transfer_flag": normalize_bool_flag(get_value(row, header_lookup, "TRANSFER")),
            },
            "return_forms": {
                "form_1040": normalize_bool_flag(get_value(row, header_lookup, "1040")),
                "sched_a_d": normalize_bool_flag(get_value(row, header_lookup, "SCH A & D")),
                "sched_c": normalize_bool_flag(get_value(row, header_lookup, "SCHED C")),
                "sched_e": normalize_bool_flag(get_value(row, header_lookup, "SCHED E")),
                "form_1120": normalize_bool_flag(get_value(row, header_lookup, "1120")),
                "form_1120s": normalize_bool_flag(get_value(row, header_lookup, "1120S")),
                "form_1065_llc": normalize_bool_flag(get_value(row, header_lookup, "1065/LLC")),
                "corp_officer": normalize_bool_flag(get_value(row, header_lookup, "Corp Officer")),
                "business_owner": normalize_bool_flag(get_value(row, header_lookup, "Bus Owner")),
                "form_990_1041": normalize_bool_flag(get_value(row, header_lookup, "990/1041")),
            },
            "payments": {
                "total_fee": normalize_currency(get_value(row, header_lookup, "TOTAL FEE")),
                "receipt_number": normalize_string(get_value(row, header_lookup, "RECEIPT #")),
                "fee_paid": normalize_currency(get_value(row, header_lookup, "FEE PAID")),
                "cc_fee": normalize_currency(get_value(row, header_lookup, "CC Fee")),
                "zelle_or_check_ref": normalize_string(get_value(row, header_lookup, "Zelle or CK #")),
                "cash_or_qpay_ref": normalize_string(get_value(row, header_lookup, "Cash, Q Pay")),
            },
            "notes": {"note_text": normalize_string(get_value(row, header_lookup, "NOTES"))},
        }
    _augment_clients_joint_spouse(payload["clients"])
    return (payload, warnings)


def _match_return(
    conn: sqlite3.Connection,
    normalized: Dict[str, Any],
    prefetch_returns_for_year: List[sqlite3.Row] | None = None,
) -> Dict[str, Any]:
    """
    Manual office log: LOG + tax year pick a candidate row, then **verify** taxpayer names fuzzily —
    reused log slots are routed through same-year fuzzy search rather than overwriting unrelated clients.
    """
    ret = normalized["returns"]
    cli = normalized["clients"]
    ty = ret["tax_year"]
    csv_ln = (cli["last_name"] or "").strip()
    csv_fn = (cli["first_name"] or "").strip()

    if ty is None:
        return _empty_manual_match_payload()

    rows = prefetch_returns_for_year
    if rows is None:
        rows = fetch_returns_clients_for_tax_year(conn, ty)

    log_needle = normalize_string(ret.get("log_number") or "")
    same_log_rows = [
        r
        for r in rows
        if normalize_string(str(r["return_log_number"] if r["return_log_number"] is not None else "")) == log_needle
    ]

    excluded: set[int] = set()

    if len(same_log_rows) > 1:
        return {
            "return_id": None,
            "client_id": None,
            "ambiguous": True,
            "needs_review": False,
            "review_reason": None,
        }

    if len(same_log_rows) == 1:
        lone = same_log_rows[0]
        lone_rid = int(lone["return_id"])
        lone_cid = int(lone["client_id"])
        nm_score = score_client_names_pair(csv_ln, csv_fn, lone["last_name"], lone["first_name"])

        if nm_score >= ACCEPT_THRESHOLD:
            return {
                "return_id": lone_rid,
                "client_id": lone_cid,
                "ambiguous": False,
                "needs_review": False,
                "review_reason": None,
            }
        if nm_score >= REVIEW_THRESHOLD:
            return {
                "return_id": None,
                "client_id": None,
                "ambiguous": False,
                "needs_review": True,
                "review_reason": "LOG_VS_NAME_MEDIUM",
            }
        excluded.add(lone_rid)

    return _fuzzy_pick_for_manual(csv_ln, csv_fn, rows, excluded_return_ids=excluded)


def _upsert_client(conn: sqlite3.Connection, data: Dict[str, Any], forced_client_id: int | None) -> tuple[int, bool, bool]:
    existing = None
    if forced_client_id is not None:
        existing = dict(conn.execute("SELECT * FROM clients WHERE id = ?", (forced_client_id,)).fetchone() or {})
    if existing is None:
        fn = data.get("first_name")
        if fn:
            row = conn.execute(
                "SELECT * FROM clients WHERE lower(last_name)=lower(?) AND lower(first_name)=lower(?) LIMIT 1",
                (data["last_name"], fn),
            ).fetchone()
        else:
            # Business / no-first-name records: NULL != NULL in SQL, so must use IS NULL
            row = conn.execute(
                "SELECT * FROM clients WHERE lower(last_name)=lower(?) AND (first_name IS NULL OR first_name='') LIMIT 1",
                (data["last_name"],),
            ).fetchone()
        existing = dict(row) if row else None

    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO clients (
              last_name, first_name, display_name,
              spouse_first_name, spouse_last_name,
              referral_flag, referred_by, created_at, updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["last_name"],
                data["first_name"],
                data["display_name"],
                data.get("spouse_first_name"),
                data.get("spouse_last_name"),
                _bool_to_int(data["referral_flag"]),
                data["referred_by"],
                now(),
                now(),
            ),
        )
        return int(cur.lastrowid), True, False

    changed = False
    payload: Dict[str, Any] = {
        "display_name": None,
        "referral_flag": None,
        "referred_by": None,
        "spouse_first_name": None,
        "spouse_last_name": None,
    }
    for key in ("display_name", "referral_flag", "referred_by", "spouse_first_name", "spouse_last_name"):
        incoming = data.get(key)
        if incoming is None:
            continue
        if key == "referral_flag":
            incoming = _bool_to_int(incoming)
        if existing.get(key) != incoming:
            payload[key] = incoming
            changed = True
    if changed:
        conn.execute(
            """
            UPDATE clients SET
              display_name = COALESCE(?, display_name),
              referral_flag = COALESCE(?, referral_flag),
              referred_by = COALESCE(?, referred_by),
              spouse_first_name = COALESCE(?, spouse_first_name),
              spouse_last_name = COALESCE(?, spouse_last_name),
              updated_at = ?
            WHERE id = ?
            """,
            (
                payload["display_name"],
                payload["referral_flag"],
                payload["referred_by"],
                payload["spouse_first_name"],
                payload["spouse_last_name"],
                now(),
                int(existing["id"]),
            ),
        )
    return int(existing["id"]), False, changed


def _upsert_return(
    conn: sqlite3.Connection,
    client_id: int,
    data: Dict[str, Any],
    forced_return_id: int | None,
    *,
    apply_manual_log_number: bool = False,
) -> tuple[int, bool, bool, Dict[str, Any], Dict[str, Any]]:
    existing = None
    if forced_return_id is not None:
        existing = dict(conn.execute("SELECT * FROM returns WHERE id = ?", (forced_return_id,)).fetchone() or {})
    if existing is None:
        row = conn.execute(
            "SELECT * FROM returns WHERE log_number = ? AND tax_year = ? LIMIT 1",
            (data["log_number"], data["tax_year"]),
        ).fetchone()
        existing = dict(row) if row else None

    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO returns (
              client_id, log_number, tax_year, processor, verified, client_status, intake_date,
              transfer_2025_flag, transfer_2026_flag, email_marker, date_emailed, pickup_date, logout_date,
              updated_date, is_amended, has_w7, is_extension, transfer_flag, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                client_id,
                data["log_number"],
                data["tax_year"],
                data["processor"],
                _bool_to_int(data["verified"]),
                data["client_status"],
                data["intake_date"],
                _bool_to_int(data["transfer_2025_flag"]),
                _bool_to_int(data["transfer_2026_flag"]),
                data["email_marker"],
                data["date_emailed"],
                data["pickup_date"],
                data["logout_date"],
                data["updated_date"],
                _bool_to_int(data["is_amended"]),
                _bool_to_int(data["has_w7"]),
                _bool_to_int(data["is_extension"]),
                _bool_to_int(data["transfer_flag"]),
                now(),
                now(),
            ),
        )
        after = dict(data)
        after["verified"] = _bool_to_int(after["verified"])
        return int(cur.lastrowid), True, False, {}, after

    before = dict(existing)
    changed = False
    payload: Dict[str, Any] = {}
    bool_fields = {"verified", "transfer_2025_flag", "transfer_2026_flag", "is_amended", "has_w7", "is_extension", "transfer_flag"}
    existing_status = existing.get("client_status")
    status_is_locked = is_locked_status(existing_status)
    for key, incoming in data.items():
        if key in {"log_number", "tax_year"}:
            continue
        if incoming is None:
            continue
        # CANCELLED is a terminal status — no import source may overwrite it.
        if key == "client_status" and status_is_locked:
            continue
        db_value = _bool_to_int(incoming) if key in bool_fields else incoming
        if existing[key] != db_value:
            payload[key] = db_value
            changed = True
    if existing["client_id"] != client_id:
        payload["client_id"] = client_id
        changed = True

    if apply_manual_log_number:
        inc_ln = normalize_string(str(data.get("log_number") or ""))
        ex_ln = normalize_string(str(existing["log_number"] or ""))
        if inc_ln and inc_ln != ex_ln:
            payload["log_number"] = inc_ln
            changed = True

    if changed:
        conn.execute(
            """
            UPDATE returns SET
              log_number = COALESCE(?, log_number),
              client_id = COALESCE(?, client_id),
              processor = COALESCE(?, processor),
              verified = COALESCE(?, verified),
              client_status = COALESCE(?, client_status),
              intake_date = COALESCE(?, intake_date),
              transfer_2025_flag = COALESCE(?, transfer_2025_flag),
              transfer_2026_flag = COALESCE(?, transfer_2026_flag),
              email_marker = COALESCE(?, email_marker),
              date_emailed = COALESCE(?, date_emailed),
              pickup_date = COALESCE(?, pickup_date),
              logout_date = COALESCE(?, logout_date),
              updated_date = COALESCE(?, updated_date),
              is_amended = COALESCE(?, is_amended),
              has_w7 = COALESCE(?, has_w7),
              is_extension = COALESCE(?, is_extension),
              transfer_flag = COALESCE(?, transfer_flag),
              updated_at = ?
            WHERE id = ?
            """,
            (
                payload.get("log_number"),
                payload.get("client_id"),
                payload.get("processor"),
                payload.get("verified"),
                payload.get("client_status"),
                payload.get("intake_date"),
                payload.get("transfer_2025_flag"),
                payload.get("transfer_2026_flag"),
                payload.get("email_marker"),
                payload.get("date_emailed"),
                payload.get("pickup_date"),
                payload.get("logout_date"),
                payload.get("updated_date"),
                payload.get("is_amended"),
                payload.get("has_w7"),
                payload.get("is_extension"),
                payload.get("transfer_flag"),
                now(),
                int(existing["id"]),
            ),
        )
    fresh = conn.execute("SELECT * FROM returns WHERE id = ?", (int(existing["id"]),)).fetchone()
    return int(existing["id"]), False, changed, before, dict(fresh)


def _upsert_forms(conn: sqlite3.Connection, return_id: int, forms: Dict[str, Any]) -> None:
    payload = {k: _bool_to_int(v) for k, v in forms.items()}
    row = conn.execute("SELECT id FROM return_forms WHERE return_id = ? LIMIT 1", (return_id,)).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO return_forms
            (return_id, form_1040, sched_a_d, sched_c, sched_e, form_1120, form_1120s, form_1065_llc, corp_officer, business_owner, form_990_1041)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                return_id,
                payload["form_1040"],
                payload["sched_a_d"],
                payload["sched_c"],
                payload["sched_e"],
                payload["form_1120"],
                payload["form_1120s"],
                payload["form_1065_llc"],
                payload["corp_officer"],
                payload["business_owner"],
                payload["form_990_1041"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE return_forms SET
              form_1040 = COALESCE(?, form_1040),
              sched_a_d = COALESCE(?, sched_a_d),
              sched_c = COALESCE(?, sched_c),
              sched_e = COALESCE(?, sched_e),
              form_1120 = COALESCE(?, form_1120),
              form_1120s = COALESCE(?, form_1120s),
              form_1065_llc = COALESCE(?, form_1065_llc),
              corp_officer = COALESCE(?, corp_officer),
              business_owner = COALESCE(?, business_owner),
              form_990_1041 = COALESCE(?, form_990_1041)
            WHERE return_id = ?
            """,
            (
                payload["form_1040"],
                payload["sched_a_d"],
                payload["sched_c"],
                payload["sched_e"],
                payload["form_1120"],
                payload["form_1120s"],
                payload["form_1065_llc"],
                payload["corp_officer"],
                payload["business_owner"],
                payload["form_990_1041"],
                return_id,
            ),
        )


def _upsert_payment(conn: sqlite3.Connection, return_id: int, payment: Dict[str, Any]) -> None:
    row = conn.execute("SELECT id FROM payments WHERE return_id = ? LIMIT 1", (return_id,)).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO payments
            (return_id, total_fee, receipt_number, fee_paid, cc_fee, zelle_or_check_ref, cash_or_qpay_ref)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                return_id,
                payment["total_fee"],
                payment["receipt_number"],
                payment["fee_paid"],
                payment["cc_fee"],
                payment["zelle_or_check_ref"],
                payment["cash_or_qpay_ref"],
            ),
        )
    else:
        conn.execute(
            """
            UPDATE payments SET
              total_fee = COALESCE(?, total_fee),
              receipt_number = COALESCE(?, receipt_number),
              fee_paid = COALESCE(?, fee_paid),
              cc_fee = COALESCE(?, cc_fee),
              zelle_or_check_ref = COALESCE(?, zelle_or_check_ref),
              cash_or_qpay_ref = COALESCE(?, cash_or_qpay_ref)
            WHERE return_id = ?
            """,
            (
                payment["total_fee"],
                payment["receipt_number"],
                payment["fee_paid"],
                payment["cc_fee"],
                payment["zelle_or_check_ref"],
                payment["cash_or_qpay_ref"],
                return_id,
            ),
        )


def _insert_note_if_new(conn: sqlite3.Connection, return_id: int, note_text: str | None) -> bool:
    note = normalize_string(note_text)
    if note is None:
        return False
    rows = conn.execute("SELECT note_text FROM notes WHERE return_id = ?", (return_id,)).fetchall()
    if note in {normalize_string(r["note_text"]) for r in rows}:
        return False
    conn.execute(
        "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?, ?, ?, ?)",
        (return_id, note, MANUAL_LOG_SOURCE, now()),
    )
    return True


def _insert_review_row(conn: sqlite3.Connection, batch_id: int, row_number: int, row: Dict[str, str], reason: str) -> None:
    conn.execute(
        "INSERT INTO review_queue (batch_id, row_number, reason, raw_json, created_at) VALUES (?, ?, ?, ?, ?)",
        (batch_id, row_number, reason, json.dumps(row, ensure_ascii=True), now()),
    )


def _insert_import_row(
    conn: sqlite3.Connection,
    batch_id: int,
    row_number: int,
    row: Dict[str, str],
    action: str,
    error: str | None,
) -> None:
    conn.execute(
        "INSERT INTO import_rows (batch_id, row_number, raw_json, action, error) VALUES (?, ?, ?, ?, ?)",
        (batch_id, row_number, json.dumps(row, ensure_ascii=True), action, error),
    )


def _bool_to_int(value: Any) -> int | None:
    if value is None:
        return None
    return 1 if bool(value) else 0


def iter_manual_csv_rows(csv_path: str, tax_year_hint: int | None = None):
    """
    Parse a manual log CSV without touching the database.

    ``tax_year_hint`` informs which ``LOG yyyy`` column to prefer when the file lacks an exact ``LOG <year>``.

    Yields: (row_number, normalized | None, warnings, error | None)
    """
    prep = prepare_manual_csv(csv_path, tax_year_hint)
    yield from iterate_manual_prep(prep)

