"""
drake_importer.py
-----------------
Processes Drake Tax CSV exports into the taxops SQLite database.

Supports two Drake export formats, detected automatically from headers:

  CSM_DATA  — "Client Status Manager Data" report (CSMDATA.csv)
    Columns: ID (Last 4), Client Name, Type, Preparer, Status,
             Started, Completed, Last Change, Changed By,
             Refund, BalDue, Total Bill, Bank Deposits,
             Client Payments, Amount Owed

  TAX_OPS   — "Tax Ops CSV Export" (drake_YYYY.csv)
    Columns: Taxpayer Last Name, Taxpayer First Name, Return Type,
             Filing Status, Date Started, Date Completed, Date Changed,
             E-Filed, Fed Ack Date, Balance Due, Refund, Bal Due - BILL

Both files begin with 2 metadata header rows before the real column row.
The last data row is a "Totals" summary line that is automatically skipped.

PII notes
---------
* SSN (ID Last 4) is stored ONLY as a 4-digit disambiguation key.
  It must be masked ("••••") in any web-facing display.
* Refund / balance amounts are stored for internal workflow use only.
  They should never be exposed in public-facing or unauthenticated views.
"""

from __future__ import annotations

import csv
import io
import json
import sqlite3
from typing import Any, Dict, List, Optional

from config import CSMDATA_SOURCE, DRAKE_SOURCE, DRAKE_STATUS_MAP, DRAKE_TYPE_FORMS
from events import create_status_events
from normalizer import normalize_currency, normalize_date, normalize_string
from utils import ImportStats, now

# ---------------------------------------------------------------------------
# Format signatures — keys are canonical (upper-cased) header names
# ---------------------------------------------------------------------------

_CSM_SIGNATURE  = {"CLIENT NAME", "PREPARER", "STARTED", "COMPLETED"}
_TAXOPS_SIGNATURE = {"TAXPAYER LAST NAME", "DATE STARTED", "E-FILED"}


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def process_drake_csv(
    conn: sqlite3.Connection,
    csv_path: str,
    batch_id: int,
    source_file: str,
    tax_year: int,
) -> ImportStats:
    reader, fmt = _open_and_detect(csv_path)
    if fmt == "UNKNOWN":
        raise ValueError(
            "Unrecognised Drake CSV format. "
            "Expected CSM Data or Tax Ops Export columns."
        )

    source_label = CSMDATA_SOURCE if fmt == "CSM_DATA" else DRAKE_SOURCE
    stats = ImportStats()

    for row_number, row in enumerate(reader, start=2):
        if _is_totals_row(row):
            continue

        stats.row_count += 1
        try:
            if fmt == "CSM_DATA":
                normalized, warnings = _normalize_csm(row, tax_year)
            else:
                normalized, warnings = _normalize_taxops(row, tax_year)

            last = normalized["clients"].get("last_name")
            if not last:
                raise ValueError("Row has empty client name / Taxpayer Last Name.")

            match = _match_return(conn, normalized)
            if match["ambiguous"]:
                _insert_review_row(conn, batch_id, row_number, row, "AMBIGUOUS_MATCH")
                _insert_import_row(conn, batch_id, row_number, row, "REVIEW",
                                   "; ".join(warnings) if warnings else None)
                stats.review_count += 1
                continue

            client_id, created_c, updated_c = _upsert_client(
                conn, normalized["clients"], match["client_id"]
            )
            return_id, created_r, updated_r, before, after = _upsert_return(
                conn, client_id, normalized["returns"], match["return_id"]
            )
            _upsert_forms(conn, return_id, normalized["return_forms"])
            _upsert_payment(conn, return_id, normalized["payments"])

            note = normalized.get("note")
            if note and _insert_note_if_new(conn, return_id, note, source_label):
                stats.notes_created += 1

            stats.events_created += create_status_events(
                conn=conn,
                return_id=return_id,
                before=before,
                after=after,
                import_time=now(),
                source_file=source_file,
            )
            stats.created_clients += int(created_c)
            stats.updated_clients += int(updated_c)
            stats.created_returns  += int(created_r)
            stats.updated_returns  += int(updated_r)
            stats.success_count += 1

            action = "CREATED" if created_r else "UPDATED"
            _insert_import_row(conn, batch_id, row_number, row, action,
                               "; ".join(warnings) if warnings else None)

        except Exception as exc:
            _insert_import_row(conn, batch_id, row_number, row, "ERROR", str(exc))
            stats.error_count += 1

    return stats


# ---------------------------------------------------------------------------
# CSV opening + format detection
# ---------------------------------------------------------------------------

def _open_and_detect(path: str) -> tuple[csv.DictReader, str]:
    """
    Open a Drake CSV, skip metadata rows, return (DictReader, format_name).
    Both Drake export types begin with 2 plain-text metadata rows before the
    actual column header line.
    """
    with open(path, "r", encoding="utf-8-sig", newline="") as f:
        raw = f.read()

    lines = raw.splitlines(keepends=True)

    # Find the header line: first line that contains known column keywords
    keywords = {
        "TAXPAYER LAST NAME", "CLIENT NAME", "LAST NAME",
        "TAXPAYER FIRST NAME", "PREPARER", "RETURN TYPE",
    }
    header_idx = 0
    for i, line in enumerate(lines):
        upper = line.strip().upper()
        if any(kw in upper for kw in keywords):
            header_idx = i
            break

    content = "".join(lines[header_idx:])
    reader  = csv.DictReader(io.StringIO(content))

    # Peek at field names to determine format
    if not reader.fieldnames:
        return reader, "UNKNOWN"
    canonical = {f.strip().upper() for f in reader.fieldnames if f}

    if _CSM_SIGNATURE.issubset(canonical):
        fmt = "CSM_DATA"
    elif _TAXOPS_SIGNATURE.issubset(canonical):
        fmt = "TAX_OPS"
    else:
        fmt = "UNKNOWN"

    return reader, fmt


def _is_totals_row(row: Dict[str, str]) -> bool:
    """Skip the summary 'Totals (N)' row Drake appends at the end."""
    for val in row.values():
        v = (val or "").strip().upper()
        if v.startswith("TOTALS"):
            return True
    return False


# ---------------------------------------------------------------------------
# CSM Data format normalizer
# ---------------------------------------------------------------------------

def _normalize_csm(row: Dict[str, str], tax_year: int) -> tuple[Dict[str, Any], List[str]]:
    """
    Normalise a row from a "Client Status Manager Data" export.

    Key columns:
      ID (Last 4)  — "XXXXX1234"  → ssn_last4 = "1234"  (4-digit key only)
      Client Name  — "LAST, FIRST" or "BUSINESS NAME"
      Preparer     — preparer/processor name
      Status       — Drake workflow status → DRAKE_STATUS_MAP
      Started      — intake date
      Completed    — completion/logout date
      Last Change  — last updated (may include time: "MM/DD/YYYY HH:MM:SS")
      Changed By   — stored as a note
      Refund       — client's tax refund amount (internal reference)
      Total Bill   — our billing amount
      Bank Deposits— bank deposit portion of payment
      Client Payments — direct client payment
    """
    warnings: List[str] = []

    def _d(col: str) -> Optional[str]:
        val, warn = normalize_date(_col(row, col))
        if warn:
            warnings.append(warn)
        return val

    raw_status    = normalize_string(_col(row, "Status")) or ""
    client_status = DRAKE_STATUS_MAP.get(raw_status.upper(), "PROCESSING") if raw_status else None

    last_name, first_name = _split_client_name(normalize_string(_col(row, "Client Name")) or "")
    ssn_last4 = _extract_ssn_last4(_col(row, "ID (Last 4)"))
    changed_by = normalize_string(_col(row, "Changed By"))
    note = f"Last changed by: {changed_by}" if changed_by else None

    return (
        {
            "clients": {
                "last_name":  last_name,
                "first_name": first_name,
                "ssn_last4":  ssn_last4,
            },
            "returns": {
                "tax_year":         tax_year,
                "processor":        normalize_string(_col(row, "Preparer")),
                "client_status":    client_status,
                "intake_date":      _d("Started"),
                "logout_date":      _d("Completed"),
                "updated_date":     _d("Last Change"),
                "drake_status_raw": raw_status or None,
            },
            "return_forms": _type_to_forms(normalize_string(_col(row, "Type")) or ""),
            "payments": {
                "total_fee":     normalize_currency(_col(row, "Total Bill")),
                "fee_paid":      normalize_currency(_col(row, "Client Payments")),
                "bank_deposit":  normalize_currency(_col(row, "Bank Deposits")),
                "refund_amount": normalize_currency(_col(row, "Refund")),
            },
            "note": note,
        },
        warnings,
    )


# ---------------------------------------------------------------------------
# Tax Ops Export format normalizer
# ---------------------------------------------------------------------------

def _normalize_taxops(row: Dict[str, str], tax_year: int) -> tuple[Dict[str, Any], List[str]]:
    """
    Normalise a row from a "Tax Ops CSV Export" (drake_YYYY.csv).

    Status is inferred because there is no workflow Status column:
      E-Filed = Yes              → EFILE
      E-Filed = No + Completed   → LOG OUT
      Otherwise                  → PROCESSING

    Billing column: "Bal Due - BILL" = our fee charge (→ total_fee)
    "Balance Due" = client's IRS balance (skip — not our concern)
    "Filing Status" = 1-5 IRS filing code (skip — not workflow status)
    """
    warnings: List[str] = []

    def _d(col: str) -> Optional[str]:
        val, warn = normalize_date(_col(row, col))
        if warn:
            warnings.append(warn)
        return val

    date_completed = _d("Date Completed")
    ack_date       = _d("Fed Ack Date")
    e_filed_raw    = (_col(row, "E-Filed") or "").strip().upper()
    e_filed        = e_filed_raw in ("YES", "Y", "TRUE", "1", "X")

    if ack_date:
        client_status = "LOG OUT"      # acknowledged/accepted → case closed
    elif e_filed:
        client_status = "EFILE READY"  # transmitted but no ack yet — stays in EFILE READY
    elif date_completed:
        client_status = "LOG OUT"
    else:
        client_status = "PROCESSING"

    is_ext_raw = normalize_string(_col(row, "Return Type")) or ""
    is_extension = 1 if is_ext_raw.upper() in ("EXT", "4868") else None

    return (
        {
            "clients": {
                "last_name":  normalize_string(_col(row, "Taxpayer Last Name")),
                "first_name": normalize_string(_col(row, "Taxpayer First Name")),
                "ssn_last4":  None,
            },
            "returns": {
                "tax_year":         tax_year,
                "client_status":    client_status,
                "intake_date":      _d("Date Started"),
                "logout_date":      date_completed,
                "updated_date":     _d("Date Changed"),
                "efile_date":       None,
                "ack_date":         ack_date,
                "is_extension":     is_extension,
                "drake_status_raw": f"E-Filed: {e_filed_raw}" if e_filed_raw else None,
            },
            "return_forms": _type_to_forms(is_ext_raw),
            "payments": {
                "total_fee":     normalize_currency(_col(row, "Bal Due - BILL")),
                "fee_paid":      None,
                "bank_deposit":  None,
                "refund_amount": normalize_currency(_col(row, "Refund")),
            },
            "note": None,
        },
        warnings,
    )


# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

def _col(row: Dict[str, str], name: str) -> Optional[str]:
    """Case-insensitive column lookup."""
    name_upper = name.upper()
    for k, v in row.items():
        if (k or "").strip().upper() == name_upper:
            return v or None
    return None


def _split_client_name(client_name: str) -> tuple[Optional[str], Optional[str]]:
    """
    Split "LAST, FIRST" into (last, first).
    Business names without a comma return (name, None).
    """
    name = client_name.strip()
    if not name:
        return None, None
    if "," in name:
        idx = name.index(",")
        last  = name[:idx].strip() or None
        first = name[idx + 1:].strip() or None
        return last, first
    return name, None


def _extract_ssn_last4(id_value: Optional[str]) -> Optional[str]:
    """
    Extract the 4-digit SSN suffix from Drake's masked format ("XXXXX1234").
    Returns None if the value is absent or has fewer than 4 digits.
    IMPORTANT: Store only the 4 digits; mask as '••••' in any UI display.
    """
    if not id_value:
        return None
    digits = "".join(c for c in id_value if c.isdigit())
    return digits[-4:] if len(digits) >= 4 else None


def _type_to_forms(return_type: str) -> Dict[str, int]:
    """Convert a Drake return type code to a return_forms dict."""
    key = return_type.strip().upper().replace(" ", "").replace("-", "")
    base: Dict[str, int] = {
        "form_1040": 0, "sched_a_d": 0, "sched_c": 0, "sched_e": 0,
        "form_1120": 0, "form_1120s": 0, "form_1065_llc": 0,
        "corp_officer": 0, "business_owner": 0, "form_990_1041": 0,
    }
    for type_key, flags in DRAKE_TYPE_FORMS.items():
        if key == type_key.upper().replace(" ", "").replace("-", ""):
            base.update(flags)
            break
    return base


def _map_status(raw: str) -> Optional[str]:
    if not raw:
        return None
    return DRAKE_STATUS_MAP.get(raw.strip().upper(), "PROCESSING")


# ---------------------------------------------------------------------------
# Match / upsert helpers
# ---------------------------------------------------------------------------

def _match_return(conn: sqlite3.Connection, normalized: Dict[str, Any]) -> Dict[str, Any]:
    cli      = normalized["clients"]
    tax_year = normalized["returns"]["tax_year"]
    last     = cli.get("last_name") or ""
    first    = cli.get("first_name")

    if first:
        rows = conn.execute(
            """
            SELECT r.id, r.client_id
            FROM returns r JOIN clients c ON c.id = r.client_id
            WHERE lower(c.last_name)=lower(?) AND lower(c.first_name)=lower(?)
              AND r.tax_year=?
            """,
            (last, first, tax_year),
        ).fetchall()
    else:
        # Business returns — match by full business name, no first name
        rows = conn.execute(
            """
            SELECT r.id, r.client_id
            FROM returns r JOIN clients c ON c.id = r.client_id
            WHERE lower(c.last_name)=lower(?)
              AND (c.first_name IS NULL OR c.first_name='')
              AND r.tax_year=?
            """,
            (last, tax_year),
        ).fetchall()

    if len(rows) == 1:
        return {"return_id": int(rows[0]["id"]), "client_id": int(rows[0]["client_id"]), "ambiguous": False}

    if len(rows) > 1:
        ssn = cli.get("ssn_last4")
        if ssn:
            narrowed = [
                r for r in rows
                if conn.execute(
                    "SELECT ssn_last4 FROM clients WHERE id=?", (r["client_id"],)
                ).fetchone()["ssn_last4"] == ssn
            ]
            if len(narrowed) == 1:
                return {
                    "return_id": int(narrowed[0]["id"]),
                    "client_id": int(narrowed[0]["client_id"]),
                    "ambiguous": False,
                }
        return {"return_id": None, "client_id": None, "ambiguous": True}

    return {"return_id": None, "client_id": None, "ambiguous": False}


def _upsert_client(
    conn: sqlite3.Connection,
    data: Dict[str, Any],
    forced_client_id: Optional[int],
) -> tuple[int, bool, bool]:
    existing = None
    if forced_client_id is not None:
        existing = conn.execute("SELECT * FROM clients WHERE id=?", (forced_client_id,)).fetchone()

    if existing is None:
        last  = data.get("last_name") or ""
        first = data.get("first_name")
        if first:
            existing = conn.execute(
                "SELECT * FROM clients WHERE lower(last_name)=lower(?) AND lower(first_name)=lower(?) LIMIT 1",
                (last, first),
            ).fetchone()
        else:
            existing = conn.execute(
                "SELECT * FROM clients WHERE lower(last_name)=lower(?) AND (first_name IS NULL OR first_name='') LIMIT 1",
                (last,),
            ).fetchone()

    if existing is None:
        cur = conn.execute(
            "INSERT INTO clients (last_name, first_name, ssn_last4, created_at, updated_at) VALUES (?,?,?,?,?)",
            (data.get("last_name"), data.get("first_name"), data.get("ssn_last4"), now(), now()),
        )
        return int(cur.lastrowid), True, False

    changed = False
    updates: Dict[str, Any] = {}
    for key in ("ssn_last4",):
        incoming = data.get(key)
        if incoming is not None and existing[key] != incoming:
            updates[key] = incoming
            changed = True
    if changed:
        conn.execute(
            "UPDATE clients SET ssn_last4=COALESCE(?,ssn_last4), updated_at=? WHERE id=?",
            (updates.get("ssn_last4"), now(), int(existing["id"])),
        )
    return int(existing["id"]), False, changed


def _upsert_return(
    conn: sqlite3.Connection,
    client_id: int,
    data: Dict[str, Any],
    forced_return_id: Optional[int],
) -> tuple[int, bool, bool, Dict[str, Any], Dict[str, Any]]:
    existing = None
    if forced_return_id is not None:
        existing = conn.execute("SELECT * FROM returns WHERE id=?", (forced_return_id,)).fetchone()
    if existing is None:
        existing = conn.execute(
            "SELECT * FROM returns WHERE client_id=? AND tax_year=? LIMIT 1",
            (client_id, data["tax_year"]),
        ).fetchone()

    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO returns (
              client_id, tax_year, processor, client_status,
              intake_date, logout_date, updated_date,
              efile_date, ack_date, is_extension, drake_status_raw,
              created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                client_id,
                data["tax_year"],
                data.get("processor"),
                data.get("client_status"),
                data.get("intake_date"),
                data.get("logout_date"),
                data.get("updated_date"),
                data.get("efile_date"),
                data.get("ack_date"),
                data.get("is_extension"),
                data.get("drake_status_raw"),
                now(), now(),
            ),
        )
        return int(cur.lastrowid), True, False, {}, dict(data)

    before = dict(existing)
    payload: Dict[str, Any] = {}
    changed = False
    update_keys = (
        "processor", "client_status", "intake_date", "logout_date",
        "updated_date", "efile_date", "ack_date", "is_extension", "drake_status_raw",
    )
    for key in update_keys:
        val = data.get(key)
        if val is None:
            continue
        if existing[key] != val:
            payload[key] = val
            changed = True
    if existing["client_id"] != client_id:
        payload["client_id"] = client_id
        changed = True

    if changed:
        conn.execute(
            """
            UPDATE returns SET
              client_id        = COALESCE(?, client_id),
              processor        = COALESCE(?, processor),
              client_status    = COALESCE(?, client_status),
              intake_date      = COALESCE(?, intake_date),
              logout_date      = COALESCE(?, logout_date),
              updated_date     = COALESCE(?, updated_date),
              efile_date       = COALESCE(?, efile_date),
              ack_date         = COALESCE(?, ack_date),
              is_extension     = COALESCE(?, is_extension),
              drake_status_raw = COALESCE(?, drake_status_raw),
              updated_at       = ?
            WHERE id = ?
            """,
            (
                payload.get("client_id"),
                payload.get("processor"),
                payload.get("client_status"),
                payload.get("intake_date"),
                payload.get("logout_date"),
                payload.get("updated_date"),
                payload.get("efile_date"),
                payload.get("ack_date"),
                payload.get("is_extension"),
                payload.get("drake_status_raw"),
                now(),
                int(existing["id"]),
            ),
        )
    fresh = conn.execute("SELECT * FROM returns WHERE id=?", (int(existing["id"]),)).fetchone()
    return int(existing["id"]), False, changed, before, dict(fresh)


def _upsert_forms(conn: sqlite3.Connection, return_id: int, forms: Dict[str, Any]) -> None:
    row = conn.execute("SELECT id FROM return_forms WHERE return_id=? LIMIT 1", (return_id,)).fetchone()
    if row is None:
        conn.execute(
            """
            INSERT INTO return_forms
            (return_id, form_1040, sched_a_d, sched_c, sched_e,
             form_1120, form_1120s, form_1065_llc, corp_officer, business_owner, form_990_1041)
            VALUES (?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                return_id,
                forms.get("form_1040"), forms.get("sched_a_d"), forms.get("sched_c"),
                forms.get("sched_e"), forms.get("form_1120"), forms.get("form_1120s"),
                forms.get("form_1065_llc"), forms.get("corp_officer"),
                forms.get("business_owner"), forms.get("form_990_1041"),
            ),
        )
    else:
        conn.execute(
            """
            UPDATE return_forms SET
              form_1040     = COALESCE(?, form_1040),
              sched_a_d     = COALESCE(?, sched_a_d),
              sched_c       = COALESCE(?, sched_c),
              sched_e       = COALESCE(?, sched_e),
              form_1120     = COALESCE(?, form_1120),
              form_1120s    = COALESCE(?, form_1120s),
              form_1065_llc = COALESCE(?, form_1065_llc),
              corp_officer  = COALESCE(?, corp_officer),
              business_owner = COALESCE(?, business_owner),
              form_990_1041 = COALESCE(?, form_990_1041)
            WHERE return_id = ?
            """,
            (
                forms.get("form_1040"), forms.get("sched_a_d"), forms.get("sched_c"),
                forms.get("sched_e"), forms.get("form_1120"), forms.get("form_1120s"),
                forms.get("form_1065_llc"), forms.get("corp_officer"),
                forms.get("business_owner"), forms.get("form_990_1041"),
                return_id,
            ),
        )


def _upsert_payment(conn: sqlite3.Connection, return_id: int, payment: Dict[str, Any]) -> None:
    if not any(v is not None for v in payment.values()):
        return
    row = conn.execute("SELECT id FROM payments WHERE return_id=? LIMIT 1", (return_id,)).fetchone()
    if row is None:
        conn.execute(
            "INSERT INTO payments (return_id, total_fee, fee_paid, bank_deposit, refund_amount) VALUES (?,?,?,?,?)",
            (return_id, payment.get("total_fee"), payment.get("fee_paid"),
             payment.get("bank_deposit"), payment.get("refund_amount")),
        )
    else:
        conn.execute(
            """
            UPDATE payments SET
              total_fee     = COALESCE(?, total_fee),
              fee_paid      = COALESCE(?, fee_paid),
              bank_deposit  = COALESCE(?, bank_deposit),
              refund_amount = COALESCE(?, refund_amount)
            WHERE return_id = ?
            """,
            (payment.get("total_fee"), payment.get("fee_paid"),
             payment.get("bank_deposit"), payment.get("refund_amount"), return_id),
        )


def _insert_note_if_new(
    conn: sqlite3.Connection, return_id: int, note: str, source: str
) -> bool:
    clean = normalize_string(note)
    if not clean:
        return False
    existing = {
        normalize_string(r["note_text"])
        for r in conn.execute("SELECT note_text FROM notes WHERE return_id=?", (return_id,)).fetchall()
    }
    if clean in existing:
        return False
    conn.execute(
        "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,?,?)",
        (return_id, clean, source, now()),
    )
    return True


def _insert_review_row(
    conn: sqlite3.Connection, batch_id: int, row_number: int,
    row: Dict[str, str], reason: str,
) -> None:
    conn.execute(
        "INSERT INTO review_queue (batch_id, row_number, reason, raw_json, created_at) VALUES (?,?,?,?,?)",
        (batch_id, row_number, reason, json.dumps(row, ensure_ascii=True), now()),
    )


def _insert_import_row(
    conn: sqlite3.Connection, batch_id: int, row_number: int,
    row: Dict[str, str], action: str, error: Optional[str],
) -> None:
    conn.execute(
        "INSERT INTO import_rows (batch_id, row_number, raw_json, action, error) VALUES (?,?,?,?,?)",
        (batch_id, row_number, json.dumps(row, ensure_ascii=True), action, error),
    )
