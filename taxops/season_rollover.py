"""
Next-season rollover (Epic #88 — ROLLOVER-2…6).

Creates one new return per eligible client at ``target_tax_year`` with status
``PENDING INTAKE``, optionally copying preparer and other configured fields from
each client's newest return at ``source_tax_year``.
"""

from __future__ import annotations

import csv
import io
from dataclasses import dataclass
from sqlite3 import Connection
from typing import Any

from preparer import normalize_preparer

NEW_ROLLOVER_STATUS = "PENDING INTAKE"
PRIOR_CLOSED_STATUSES: frozenset[str] = frozenset({"LOG OUT"})

RETURN_FORM_COLUMNS: tuple[str, ...] = (
    "form_1040", "sched_a_d", "sched_c", "sched_e",
    "form_1120", "form_1120s", "form_1065_llc",
    "corp_officer", "business_owner", "form_990_1041",
)


@dataclass
class RolloverCarryOptions:
    carry_processor: bool = True
    carry_filing_status: bool = True
    carry_return_forms: bool = False
    carry_intake_fields: bool = False
    carry_prior_year_log_on_client: bool = True
    require_prior_logged_out: bool = False


def carry_options_from_dict(raw: dict[str, Any] | None) -> RolloverCarryOptions:
    if not isinstance(raw, dict):
        return RolloverCarryOptions()
    return RolloverCarryOptions(
        carry_processor=bool(raw.get("carry_processor", True)),
        carry_filing_status=bool(raw.get("carry_filing_status", True)),
        carry_return_forms=bool(raw.get("carry_return_forms", False)),
        carry_intake_fields=bool(raw.get("carry_intake_fields", False)),
        carry_prior_year_log_on_client=bool(raw.get("carry_prior_year_log_on_client", True)),
        require_prior_logged_out=bool(raw.get("require_prior_logged_out", False)),
    )


def validate_years(source: int, target: int) -> str | None:
    if source < 1990 or source > 2100 or target < 1990 or target > 2100:
        return "Source and target years must be between 1990 and 2100."
    if target <= source:
        return "Target tax year must be greater than the source tax year."
    return None


def fetch_latest_returns_by_client(conn: Connection, tax_year: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        SELECT r.*
          FROM returns r
          INNER JOIN (
            SELECT client_id, MAX(id) AS mx
              FROM returns
             WHERE tax_year = ?
             GROUP BY client_id
          ) q ON q.client_id = r.client_id AND q.mx = r.id
         ORDER BY r.client_id
        """,
        (tax_year,),
    ).fetchall()
    return [dict(x) for x in rows]


def clients_with_return_in_year(conn: Connection, tax_year: int) -> frozenset[int]:
    rs = conn.execute(
        "SELECT DISTINCT client_id FROM returns WHERE tax_year = ?",
        (tax_year,),
    ).fetchall()
    return frozenset(int(r["client_id"]) for r in rs if r["client_id"] is not None)


def rollover_preview_json(
    conn: Connection,
    *,
    source_year: int,
    target_year: int,
    options: RolloverCarryOptions,
) -> dict[str, Any]:
    msg = validate_years(source_year, target_year)
    if msg:
        return {"ok": False, "error": msg}

    sources = fetch_latest_returns_by_client(conn, source_year)
    already = clients_with_return_in_year(conn, target_year)

    would_create: list[dict[str, Any]] = []
    skipped: list[dict[str, Any]] = []

    for src in sources:
        cid = int(src["client_id"])
        sid = int(src["id"])

        if cid in already:
            skipped.append({
                "client_id": cid,
                "source_return_id": sid,
                "reason": "already_has_target_year_return",
            })
            continue

        prior_status = (src.get("client_status") or "").strip()
        if options.require_prior_logged_out and prior_status not in PRIOR_CLOSED_STATUSES:
            skipped.append({
                "client_id": cid,
                "source_return_id": sid,
                "reason": "prior_year_not_logged_out",
            })
            continue

        preview = {"client_id": cid, "source_return_id": sid}
        if options.carry_processor and src.get("processor"):
            preview["processor_from_prior"] = str(src["processor"]).strip()

        would_create.append(preview)

    return {
        "ok": True,
        "new_status": NEW_ROLLOVER_STATUS,
        "source_year": source_year,
        "target_year": target_year,
        "carry": {
            "carry_processor": options.carry_processor,
            "carry_filing_status": options.carry_filing_status,
            "carry_return_forms": options.carry_return_forms,
            "carry_intake_fields": options.carry_intake_fields,
            "carry_prior_year_log_on_client": options.carry_prior_year_log_on_client,
            "require_prior_logged_out": options.require_prior_logged_out,
        },
        "totals": {
            "eligible": len(would_create),
            "skipped": len(skipped),
            "prior_year_returns": len(sources),
            "distinct_clients_in_source_year": len(sources),
        },
        "would_create_sample": would_create[:200],
        "skipped_sample": skipped[:200],
    }


def rollover_commit(
    conn: Connection,
    *,
    source_year: int,
    target_year: int,
    options: RolloverCarryOptions,
    actor: str | None,
    ts: str,
) -> dict[str, Any]:
    msg = validate_years(source_year, target_year)
    if msg:
        return {"ok": False, "error": msg}

    sources = fetch_latest_returns_by_client(conn, source_year)
    already = clients_with_return_in_year(conn, target_year)

    created_rows: list[dict[str, Any]] = []
    skipped_rows: list[dict[str, Any]] = []

    conn.execute("BEGIN IMMEDIATE")

    try:
        for src in sources:
            cid = int(src["client_id"])
            sid = int(src["id"])

            if cid in already:
                skipped_rows.append({
                    "outcome": "skipped",
                    "client_id": cid,
                    "source_return_id": sid,
                    "new_return_id": None,
                    "reason": "already_has_target_year_return",
                })
                continue

            prior_status = (src.get("client_status") or "").strip()
            if options.require_prior_logged_out and prior_status not in PRIOR_CLOSED_STATUSES:
                skipped_rows.append({
                    "outcome": "skipped",
                    "client_id": cid,
                    "source_return_id": sid,
                    "new_return_id": None,
                    "reason": "prior_year_not_logged_out",
                })
                continue

            proc = normalize_preparer(str(src["processor"])) if options.carry_processor and src.get("processor") else None
            filing = (
                str(src["filing_status"]).strip()
                if options.carry_filing_status and src.get("filing_status")
                else None
            )
            interview_by = promise = delivered_by = notes = None
            if options.carry_intake_fields:
                interview_by = src.get("interview_by") or None
                promise = src.get("promise_date") or None
                delivered_by = src.get("delivered_by") or None
                notes = src.get("notes_intake") or None

            conn.execute(
                """
                INSERT INTO returns (
                  client_id, log_number, tax_year, client_status,
                  processor, verified, filing_status,
                  intake_date, interview_by, promise_date, delivered_by, notes_intake,
                  pickup_date, logout_date, efile_date, ack_date,
                  date_emailed, updated_date,
                  transfer_flag, transfer_2025_flag, transfer_2026_flag,
                  email_marker,
                  created_at, updated_at
                ) VALUES (
                  ?, NULL, ?, ?,
                  ?, 0, ?,
                  NULL, ?, ?, ?, ?,
                  NULL, NULL, NULL, NULL,
                  NULL, NULL,
                  NULL, NULL, NULL,
                  NULL,
                  ?, ?
                )
                """,
                (
                    cid, target_year, NEW_ROLLOVER_STATUS,
                    proc, filing,
                    interview_by, promise, delivered_by, notes,
                    ts, ts,
                ),
            )
            new_rid = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])

            if options.carry_prior_year_log_on_client and src.get("log_number"):
                conn.execute(
                    "UPDATE clients SET prior_year_log = ?, updated_at = ? WHERE id = ?",
                    (str(src["log_number"]).strip(), ts, cid),
                )

            if options.carry_return_forms:
                rf_src = conn.execute(
                    "SELECT * FROM return_forms WHERE return_id = ? LIMIT 1",
                    (sid,),
                ).fetchone()
                if rf_src:
                    rfd = dict(rf_src)
                    form_vals = [rfd.get(c) for c in RETURN_FORM_COLUMNS]
                else:
                    form_vals = [None] * len(RETURN_FORM_COLUMNS)
            else:
                form_vals = [None] * len(RETURN_FORM_COLUMNS)

            ph_f = ",".join("?" * (len(RETURN_FORM_COLUMNS) + 1))
            fc = ", ".join(RETURN_FORM_COLUMNS)
            conn.execute(
                f"INSERT INTO return_forms (return_id, {fc}) VALUES ({ph_f})",
                [new_rid] + form_vals,
            )

            conn.execute(
                """
                INSERT INTO status_events (
                  return_id, event_type, old_status, new_status,
                  event_timestamp, source_file, note
                ) VALUES (
                  ?, 'ROLLOVER', NULL, ?, ?, 'season_rollover', ?
                )
                """,
                (
                    new_rid, NEW_ROLLOVER_STATUS, ts,
                    f"Seeded from TY{source_year} return #{sid}; actor={(actor or '').strip() or '?'}",
                ),
            )

            created_rows.append({
                "outcome": "created",
                "client_id": cid,
                "source_return_id": sid,
                "new_return_id": new_rid,
                "reason": "",
            })

        conn.commit()
    except Exception:
        conn.rollback()
        raise

    return {
        "ok": True,
        "report": {"created": created_rows, "skipped": skipped_rows},
    }


def build_rollover_report_csv(report: dict[str, Any]) -> str:
    created = report.get("created") or []
    skipped = report.get("skipped") or []
    rows = list(created) + list(skipped)
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["outcome", "client_id", "source_return_id", "new_return_id", "reason"])
    for r in rows:
        w.writerow([
            r.get("outcome", ""),
            r.get("client_id", ""),
            r.get("source_return_id", ""),
            r.get("new_return_id", ""),
            (r.get("reason") or "").replace("\n", " ").strip(),
        ])
    return buf.getvalue()