"""M2 — ingest Drake, Tax Log sheets, and TaxOps snapshot into audit staging."""

from __future__ import annotations

import re
import sqlite3
from pathlib import Path
from typing import Optional

import openpyxl

from audit import config
from audit.db import connect_audit, connect_taxops_readonly
from audit.util import dumps


def normalize_yr(raw) -> Optional[int]:
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # Excel may give int 25 or 2025
    if re.fullmatch(r"\d{2}", s):
        return 2000 + int(s)
    if re.fullmatch(r"\d{4}", s):
        return int(s)
    # float-like
    try:
        n = int(float(s))
        if 0 <= n <= 99:
            return 2000 + n
        if 1900 <= n <= 2100:
            return n
    except ValueError:
        return None
    return None


def _resolve_sheet(wb, wanted: str) -> Optional[str]:
    """Match sheet names allowing trailing spaces / smart quotes drift."""
    if wanted in wb.sheetnames:
        return wanted
    w = wanted.strip().casefold()
    for name in wb.sheetnames:
        if name.strip().casefold() == w:
            return name
        # EXT 1120S variant
        if w.replace("'", "'") == name.strip().casefold().replace("'", "'"):
            return name
    return None


def ingest_drake(conn: sqlite3.Connection, run_id: int, path: Path) -> dict:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    kept = dropped = 0
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        if not any(v is not None and str(v).strip() for v in vals):
            conn.execute(
                "INSERT INTO audit_dropped (run_id, source, source_row, reason) VALUES (?,?,?,?)",
                (run_id, "drake", i, "empty_row"),
            )
            dropped += 1
            continue
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name:
            conn.execute(
                "INSERT INTO audit_dropped (run_id, source, source_row, reason) VALUES (?,?,?,?)",
                (run_id, "drake", i, "empty_client_name"),
            )
            dropped += 1
            continue
        if name.upper().startswith("TOTAL"):
            conn.execute(
                "INSERT INTO audit_dropped (run_id, source, source_row, reason) VALUES (?,?,?,?)",
                (run_id, "drake", i, "totals_row"),
            )
            dropped += 1
            continue
        id_raw = str(vals[0] or "").strip() if vals else ""
        digits = "".join(c for c in id_raw if c.isdigit())
        last4 = digits[-4:] if len(digits) >= 4 else None
        rtype = str(vals[2] or "").strip().upper() if len(vals) > 2 else ""
        is_entity = 1 if rtype in config.ENTITY_DRAKE_TYPES else 0
        conn.execute(
            """
            INSERT INTO stage_drake (
              run_id, source_row, id_last4, client_name_raw, return_type,
              preparer, status, is_entity, dropped, drop_reason
            ) VALUES (?,?,?,?,?,?,?,?,0,NULL)
            """,
            (
                run_id,
                i,
                last4,
                name,
                rtype or None,
                str(vals[3] or "").strip() if len(vals) > 3 else None,
                str(vals[4] or "").strip() if len(vals) > 4 else None,
                is_entity,
            ),
        )
        kept += 1
    wb.close()
    conn.commit()
    return {"kept": kept, "dropped": dropped}


def ingest_tax_log(conn: sqlite3.Connection, run_id: int, path: Path) -> dict:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    stats = {"sheets": {}}

    # Individuals
    sheet = _resolve_sheet(wb, config.SHEET_INDIVIDUALS)
    if not sheet:
        raise RuntimeError(f"Sheet {config.SHEET_INDIVIDUALS!r} NOT FOUND in {wb.sheetnames}")
    kept = dropped = 0
    ws = wb[sheet]
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i < config.LOG_DATA_START_ROW:
            continue
        vals = list(row)
        last = str(vals[config.LOG_LAST_COL] or "").strip() if len(vals) > config.LOG_LAST_COL else ""
        first = (
            str(vals[config.LOG_FIRST_COL] or "").strip()
            if len(vals) > config.LOG_FIRST_COL
            else ""
        )
        if not last and not first:
            conn.execute(
                "INSERT INTO audit_dropped (run_id, source, source_row, reason) VALUES (?,?,?,?)",
                (run_id, f"log:{sheet}", i, "empty_name"),
            )
            dropped += 1
            # Still keep a dropped staging marker? Spec: keep every source row —
            # insert with dropped=1
            conn.execute(
                """
                INSERT INTO stage_log (
                  run_id, sheet_name, source_row, last_raw, first_raw, yr_raw, yr_norm,
                  processor, client_status, log_2025, is_entity_sheet, dropped, drop_reason
                ) VALUES (?,?,?,?,?,?,?,?,?,?,0,1,?)
                """,
                (run_id, sheet, i, last or None, first or None, None, None,
                 None, None, None, "empty_name"),
            )
            continue
        yr_raw = vals[config.LOG_YR_COL] if len(vals) > config.LOG_YR_COL else None
        yr_norm = normalize_yr(yr_raw)
        # LOG 2025 is col index 1 (B) per row3 header
        log_2025 = str(vals[1] or "").strip() if len(vals) > 1 else None
        processor = str(vals[5] or "").strip() if len(vals) > 5 else None
        status = str(vals[7] or "").strip() if len(vals) > 7 else None
        conn.execute(
            """
            INSERT INTO stage_log (
              run_id, sheet_name, source_row, last_raw, first_raw, yr_raw, yr_norm,
              processor, client_status, log_2025, is_entity_sheet, dropped, drop_reason
            ) VALUES (?,?,?,?,?,?,?,?,?,?,0,0,NULL)
            """,
            (
                run_id, sheet, i, last or None, first or None,
                str(yr_raw).strip() if yr_raw is not None else None,
                yr_norm, processor or None, status or None, log_2025 or None,
            ),
        )
        kept += 1
    stats["sheets"][sheet] = {"kept": kept, "dropped": dropped}

    # Business sheets — preserve all non-empty rows; name column heuristics
    for wanted in config.SHEET_BUSINESS:
        resolved = _resolve_sheet(wb, wanted)
        if not resolved:
            stats["sheets"][wanted] = {"error": "NOT FOUND", "searched": list(wb.sheetnames)}
            continue
        ws = wb[resolved]
        kept = dropped = 0
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            vals = list(row)
            if not any(v is not None and str(v).strip() for v in vals):
                conn.execute(
                    "INSERT INTO audit_dropped (run_id, source, source_row, reason) VALUES (?,?,?,?)",
                    (run_id, f"log:{resolved}", i, "empty_row"),
                )
                dropped += 1
                continue
                # Prefer first non-header-looking text cell as entity name
            name_blob = ""
            for v in vals[:8]:
                if v is None:
                    continue
                s = str(v).strip()
                if not s:
                    continue
                if s.upper() in ("NAME", "EIN", "CLIENT", "TYPE", "STATUS"):
                    continue
                # skip pure numbers / dates for the name field
                if s.replace(".", "").isdigit():
                    continue
                name_blob = s
                break
            if not name_blob:
                chunks = [str(v).strip() for v in vals[:4] if v is not None and str(v).strip()]
                name_blob = chunks[0] if chunks else ""
            conn.execute(
                """
                INSERT INTO stage_log (
                  run_id, sheet_name, source_row, last_raw, first_raw, yr_raw, yr_norm,
                  processor, client_status, log_2025, is_entity_sheet, dropped, drop_reason
                ) VALUES (?,?,?,?,NULL,NULL,NULL,NULL,NULL,NULL,1,0,NULL)
                """,
                (run_id, resolved, i, name_blob or None),
            )
            kept += 1
        stats["sheets"][resolved] = {"kept": kept, "dropped": dropped}

    wb.close()
    conn.commit()
    return stats


def ingest_taxops_snapshot(conn: sqlite3.Connection, run_id: int, snapshot: Path) -> dict:
    src = connect_taxops_readonly(snapshot)
    try:
        clients = src.execute(
            """
            SELECT id, last_name, first_name, display_name, ssn_last4,
                   spouse_last_name, spouse_first_name
            FROM clients
            """
        ).fetchall()
        for r in clients:
            conn.execute(
                """
                INSERT INTO stage_taxops_client (
                  run_id, client_id, last_name, first_name, display_name, ssn_last4,
                  spouse_last_name, spouse_first_name
                ) VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    run_id, int(r["id"]), r["last_name"], r["first_name"], r["display_name"],
                    r["ssn_last4"], r["spouse_last_name"], r["spouse_first_name"],
                ),
            )

        # import_batch_id may not exist on returns — probe columns
        cols = {row[1] for row in src.execute("PRAGMA table_info(returns)").fetchall()}
        batch_expr = "NULL"
        # No import_batch_id on returns typically; leave null. status_events/source elsewhere.
        returns = src.execute(
            """
            SELECT id, client_id, log_number, tax_year, client_status, processor, created_at
            FROM returns
            """
        ).fetchall()
        for r in returns:
            conn.execute(
                """
                INSERT INTO stage_taxops_return (
                  run_id, return_id, client_id, log_number, tax_year, client_status,
                  processor, created_at, import_batch_id
                ) VALUES (?,?,?,?,?,?,?,?,NULL)
                """,
                (
                    run_id, int(r["id"]), int(r["client_id"]), r["log_number"], r["tax_year"],
                    r["client_status"], r["processor"], r["created_at"],
                ),
            )

        spouses = src.execute(
            """
            SELECT id, client_id, last_name, first_name, date_of_birth, needs_review
            FROM spouses
            """
        ).fetchall()
        for r in spouses:
            conn.execute(
                """
                INSERT INTO stage_taxops_spouse (
                  run_id, spouse_row_id, client_id, last_name, first_name,
                  date_of_birth, needs_review
                ) VALUES (?,?,?,?,?,?,?)
                """,
                (
                    run_id, int(r["id"]), int(r["client_id"]), r["last_name"], r["first_name"],
                    r["date_of_birth"], r["needs_review"],
                ),
            )
        conn.commit()
        return {
            "clients": len(clients),
            "returns": len(returns),
            "spouses": len(spouses),
            "returns_cols_seen": sorted(cols),
        }
    finally:
        src.close()


def run_ingest(audit_db: Path, run_id: int, drake: Path, tax_log: Path, snapshot: Path) -> dict:
    conn = connect_audit(audit_db)
    try:
        out = {
            "drake": ingest_drake(conn, run_id, drake),
            "tax_log": ingest_tax_log(conn, run_id, tax_log),
            "taxops": ingest_taxops_snapshot(conn, run_id, snapshot),
        }
        # Reconcile to M0 expected named totals
        d_kept = out["drake"]["kept"]
        log_kept = out["tax_log"]["sheets"].get(config.SHEET_INDIVIDUALS, {}).get("kept")
        out["reconcile"] = {
            "drake_kept_vs_expected": {"kept": d_kept, "expected": config.EXPECTED_DRAKE_ROWS},
            "log_kept_vs_expected": {"kept": log_kept, "expected": config.EXPECTED_LOG_NAMED_ROWS},
        }
        return out
    finally:
        conn.close()
