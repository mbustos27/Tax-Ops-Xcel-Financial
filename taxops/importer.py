from __future__ import annotations

import csv
import json
import sqlite3
from dataclasses import dataclass
from typing import Any, Dict, List

from config import MANUAL_LOG_SOURCE
from events import create_status_events
from normalizer import (
    build_header_lookup,
    get_value,
    normalize_bool_flag,
    normalize_currency,
    normalize_date,
    normalize_status,
    normalize_string,
)
from utils import now

REQUIRED_COLUMNS = ["LOG 2025", "LAST", "FIRST", "YR"]


@dataclass
class ImportStats:
    row_count: int = 0
    success_count: int = 0
    error_count: int = 0
    review_count: int = 0
    created_clients: int = 0
    updated_clients: int = 0
    created_returns: int = 0
    updated_returns: int = 0
    events_created: int = 0
    notes_created: int = 0


def process_csv(conn: sqlite3.Connection, csv_path: str, batch_id: int, source_file: str) -> ImportStats:
    stats = ImportStats()
    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV is missing header row.")

        header_lookup = build_header_lookup(reader.fieldnames)
        missing = [name for name in REQUIRED_COLUMNS if name.upper() not in header_lookup]
        if missing:
            raise ValueError(f"CSV missing required columns: {', '.join(missing)}")

        for row_number, row in enumerate(reader, start=2):
            stats.row_count += 1
            try:
                normalized, warnings = _normalize_row(row, header_lookup)
                if not normalized["returns"]["log_number"] or normalized["returns"]["tax_year"] is None:
                    raise ValueError("Missing required values: LOG 2025 and/or YR")
                if not normalized["clients"]["last_name"] or not normalized["clients"]["first_name"]:
                    raise ValueError("Missing required values: LAST and/or FIRST")

                match = _match_return(conn, normalized)
                if match["ambiguous"]:
                    _insert_review_row(conn, batch_id, row_number, row, "AMBIGUOUS_MATCH")
                    _insert_import_row(conn, batch_id, row_number, row, "REVIEW", "; ".join(warnings) if warnings else None)
                    stats.review_count += 1
                    continue

                client_id, created_client, updated_client = _upsert_client(conn, normalized["clients"], match["client_id"])
                return_id, created_return, updated_return, before_row, after_row = _upsert_return(
                    conn, client_id, normalized["returns"], match["return_id"]
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
    return stats


def _normalize_row(row: Dict[str, str], header_lookup: Dict[str, str]) -> tuple[Dict[str, Any], List[str]]:
    warnings: List[str] = []
    tax_year_val = normalize_string(get_value(row, header_lookup, "YR"))
    tax_year = int(tax_year_val) if tax_year_val and tax_year_val.isdigit() else None

    intake_date, warn = normalize_date(get_value(row, header_lookup, "INT'D"))
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

    return (
        {
            "clients": {
                "last_name": normalize_string(get_value(row, header_lookup, "LAST")),
                "first_name": normalize_string(get_value(row, header_lookup, "FIRST")),
                "display_name": normalize_string(get_value(row, header_lookup, "TAX PAYER NAME (S)")),
                "referral_flag": normalize_bool_flag(get_value(row, header_lookup, "Referral")),
                "referred_by": normalize_string(get_value(row, header_lookup, "Referred By")),
            },
            "returns": {
                "log_number": normalize_string(get_value(row, header_lookup, "LOG 2025")),
                "tax_year": tax_year,
                "processor": normalize_string(get_value(row, header_lookup, "PROCESSOR")),
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
        },
        warnings,
    )


def _match_return(conn: sqlite3.Connection, normalized: Dict[str, Any]) -> Dict[str, Any]:
    ret = normalized["returns"]
    cli = normalized["clients"]
    matches = conn.execute(
        "SELECT id, client_id FROM returns WHERE log_number = ? AND tax_year = ?",
        (ret["log_number"], ret["tax_year"]),
    ).fetchall()
    if len(matches) == 1:
        return {"return_id": int(matches[0]["id"]), "client_id": int(matches[0]["client_id"]), "ambiguous": False}
    if len(matches) > 1:
        return {"return_id": None, "client_id": None, "ambiguous": True}

    fallback = conn.execute(
        """
        SELECT r.id, r.client_id
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE lower(c.last_name) = lower(?) AND lower(c.first_name) = lower(?) AND r.tax_year = ?
        """,
        (cli["last_name"], cli["first_name"], ret["tax_year"]),
    ).fetchall()
    if len(fallback) == 1:
        return {"return_id": int(fallback[0]["id"]), "client_id": int(fallback[0]["client_id"]), "ambiguous": False}
    if len(fallback) > 1:
        return {"return_id": None, "client_id": None, "ambiguous": True}
    return {"return_id": None, "client_id": None, "ambiguous": False}


def _upsert_client(conn: sqlite3.Connection, data: Dict[str, Any], forced_client_id: int | None) -> tuple[int, bool, bool]:
    existing = None
    if forced_client_id is not None:
        existing = conn.execute("SELECT * FROM clients WHERE id = ?", (forced_client_id,)).fetchone()
    if existing is None:
        existing = conn.execute(
            "SELECT * FROM clients WHERE lower(last_name)=lower(?) AND lower(first_name)=lower(?) LIMIT 1",
            (data["last_name"], data["first_name"]),
        ).fetchone()

    if existing is None:
        cur = conn.execute(
            """
            INSERT INTO clients (last_name, first_name, display_name, referral_flag, referred_by, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                data["last_name"],
                data["first_name"],
                data["display_name"],
                _bool_to_int(data["referral_flag"]),
                data["referred_by"],
                now(),
                now(),
            ),
        )
        return int(cur.lastrowid), True, False

    changed = False
    payload = {"display_name": None, "referral_flag": None, "referred_by": None}
    for key in ("display_name", "referral_flag", "referred_by"):
        incoming = data.get(key)
        if incoming is None:
            continue
        if key == "referral_flag":
            incoming = _bool_to_int(incoming)
        if existing[key] != incoming:
            payload[key] = incoming
            changed = True
    if changed:
        conn.execute(
            """
            UPDATE clients SET
              display_name = COALESCE(?, display_name),
              referral_flag = COALESCE(?, referral_flag),
              referred_by = COALESCE(?, referred_by),
              updated_at = ?
            WHERE id = ?
            """,
            (payload["display_name"], payload["referral_flag"], payload["referred_by"], now(), int(existing["id"])),
        )
    return int(existing["id"]), False, changed


def _upsert_return(
    conn: sqlite3.Connection,
    client_id: int,
    data: Dict[str, Any],
    forced_return_id: int | None,
) -> tuple[int, bool, bool, Dict[str, Any], Dict[str, Any]]:
    existing = None
    if forced_return_id is not None:
        existing = conn.execute("SELECT * FROM returns WHERE id = ?", (forced_return_id,)).fetchone()
    if existing is None:
        existing = conn.execute(
            "SELECT * FROM returns WHERE log_number = ? AND tax_year = ? LIMIT 1",
            (data["log_number"], data["tax_year"]),
        ).fetchone()

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
    for key, incoming in data.items():
        if key in {"log_number", "tax_year"} or incoming is None:
            continue
        db_value = _bool_to_int(incoming) if key in bool_fields else incoming
        if existing[key] != db_value:
            payload[key] = db_value
            changed = True
    if existing["client_id"] != client_id:
        payload["client_id"] = client_id
        changed = True
    if changed:
        conn.execute(
            """
            UPDATE returns SET
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
