"""Task 2 — Tax Log churn: April import_rows batch 1 vs current XCEL 2025."""

from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

import openpyxl

from audit import config
from audit.db import connect_taxops_readonly
from audit.ingest import normalize_yr
from audit.normalizer import (
    keys_with_transposition,
    normalize_drake_client_name,
    normalize_entity,
    normalize_log_row,
    normalize_person,
)
from audit.util import dumps


def _resolve_sheet(wb, wanted: str) -> Optional[str]:
    w = wanted.strip().casefold()
    for name in wb.sheetnames:
        if name.strip().casefold() == w:
            return name
    return None


def _person_key(last: str, first: str) -> tuple[str, str]:
    n = normalize_log_row(last, first)
    if n.match_keys:
        return n.match_keys[0]
    return (n.surname_full or "", n.first_key or "")


def _all_person_keys(last: str, first: str) -> set[tuple[str, str]]:
    return set(keys_with_transposition(last, first))


def _entity_keys(name: str) -> set[str]:
    return set(normalize_entity(name).keys)


def load_current_log(tax_log: Path) -> dict[str, Any]:
    wb = openpyxl.load_workbook(tax_log, read_only=True, data_only=True)
    # XCEL person index: set of person keys + list of rows
    xcel_keys: set[tuple[str, str]] = set()
    xcel_entity_keys: set[str] = set()
    xcel_rows = []
    sheet = _resolve_sheet(wb, config.SHEET_INDIVIDUALS)
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
            continue
        yr = vals[config.LOG_YR_COL] if len(vals) > config.LOG_YR_COL else None
        pkeys = _all_person_keys(last, first)
        xcel_keys |= pkeys
        if last and not first:
            xcel_entity_keys |= _entity_keys(last)
        xcel_rows.append(
            {
                "source_row": i,
                "last": last,
                "first": first,
                "yr_norm": normalize_yr(yr),
                "keys": pkeys,
                "entity_keys": _entity_keys(last) if last and not first else set(),
                "primary": _person_key(last, first),
            }
        )

    other_sheets = [
        "Other Office Svcs",
        "LOG OUT",
        "1120 CORP LIST",
        "1120 S LIST",
        "1065 & LLC LIST",
        "EXT 1120",
        "EXT 1120S, 1065'S",
    ]
    other_person: set[tuple[str, str]] = set()
    other_entity: set[str] = set()
    other_sheet_hits: dict[str, int] = Counter()

    for wanted in other_sheets:
        resolved = _resolve_sheet(wb, wanted)
        if not resolved:
            continue
        ws = wb[resolved]
        for row in ws.iter_rows(values_only=True):
            vals = list(row)
            texts = [str(v).strip() for v in vals[:8] if v is not None and str(v).strip()]
            if not texts:
                continue
            # Try first text cell as name blob / LAST
            blob = texts[0]
            ekeys = _entity_keys(blob)
            if ekeys:
                other_entity |= ekeys
                other_sheet_hits[resolved] += 1
            if "," in blob:
                n = normalize_drake_client_name(blob)
                other_person |= set(n.match_keys)
            # also LAST/FIRST style if 2+ cols look like names
            if len(texts) >= 2 and wanted.startswith("LOG"):
                other_person |= _all_person_keys(texts[0], texts[1])

    wb.close()
    return {
        "xcel_keys": xcel_keys,
        "xcel_entity_keys": xcel_entity_keys,
        "xcel_rows": xcel_rows,
        "other_person": other_person,
        "other_entity": other_entity,
        "other_sheet_hits": dict(other_sheet_hits),
    }


def load_batch1(snapshot: Path) -> list[dict]:
    conn = connect_taxops_readonly(snapshot)
    rows = []
    try:
        for r in conn.execute(
            "SELECT id, row_number, raw_json FROM import_rows WHERE batch_id=1 ORDER BY row_number"
        ):
            raw = json.loads(r["raw_json"])
            last = str(raw.get("TAX PAYER NAME (S) LAST") or "").strip()
            first = str(raw.get("FIRST") or "").strip()
            if not last and not first:
                continue
            yr = normalize_yr(raw.get("YR"))
            rows.append(
                {
                    "import_row_id": int(r["id"]),
                    "row_number": int(r["row_number"]),
                    "last": last,
                    "first": first,
                    "yr_norm": yr,
                    "log_2025": str(raw.get("LOG 2025") or "").strip(),
                    "status": str(raw.get("CLIENT STATUS") or "").strip(),
                    "keys": _all_person_keys(last, first),
                    "primary": _person_key(last, first),
                    "entity_keys": _entity_keys(last) if last and not first else set(),
                    "norm_primary": normalize_log_row(last, first),
                }
            )
    finally:
        conn.close()
    return rows


def classify_row(b: dict, cur: dict) -> tuple[str, Optional[str]]:
    """Return (class, relocated_sheet_hint)."""
    # IDENTICAL: any person key or entity key hits XCEL
    if b["keys"] & cur["xcel_keys"]:
        # Check if primary form matches any current primary for NAME_CHANGED vs IDENTICAL
        # Build map primary -> exists
        hit_exact_primary = any(
            b["primary"] == xr["primary"] for xr in cur["xcel_rows"] if xr["keys"] & b["keys"]
        )
        # Also compare full normalized string
        bn = b["norm_primary"]
        from audit.normalizer import full_normalized_string

        bfull = full_normalized_string(bn)
        identical = False
        name_changed = False
        for xr in cur["xcel_rows"]:
            if not (xr["keys"] & b["keys"]):
                continue
            cn = normalize_log_row(xr["last"], xr["first"])
            cfull = full_normalized_string(cn)
            if bfull == cfull and b["primary"] == xr["primary"]:
                identical = True
                break
            name_changed = True
        if identical:
            return "IDENTICAL", None
        if name_changed:
            return "NAME_CHANGED", None
        return "IDENTICAL", None  # key overlap without full string — treat identical-ish

    if b["entity_keys"] & cur["xcel_entity_keys"]:
        return "IDENTICAL", None

    # ABSENT from XCEL — check other sheets
    if b["keys"] & cur["other_person"] or b["entity_keys"] & cur["other_entity"]:
        return "ABSENT", "relocated_other_sheet"
    return "ABSENT", "not_found"


def load_unmatched_169(audit_db: Path, run_id: int, snapshot: Path) -> set[int]:
    """
    Return set of TaxOps return_ids for unmatched TY2025 that look like the
    169 bulk-import cohort (created 2026-04-29), plus all unmatched if needed.

    User asked: 'originating rows of the 169 unmatched TY2025 returns'.
    From M9.3: 169 created in two seconds on 2026-04-29. Use that cohort.
    """
    from audit.db import connect_audit

    aconn = connect_audit(audit_db)
    try:
        # Prefer gap rows with created_at on 2026-04-29
        ids = []
        for r in aconn.execute(
            """
            SELECT return_id, client_id, created_at, has_log_number, detail_json
            FROM audit_gap_ty2025 WHERE run_id=?
            """,
            (run_id,),
        ):
            ca = str(r["created_at"] or "")
            if ca.startswith("2026-04-29"):
                ids.append(int(r["return_id"]))
        return set(ids)
    finally:
        aconn.close()


def map_returns_to_batch1_keys(
    snapshot: Path, return_ids: set[int]
) -> dict[int, dict]:
    """Map unmatched returns → client name keys for joining to batch1."""
    conn = connect_taxops_readonly(snapshot)
    out = {}
    try:
        for rid in return_ids:
            row = conn.execute(
                """
                SELECT r.id, r.client_id, r.log_number, c.last_name, c.first_name
                FROM returns r JOIN clients c ON c.id = r.client_id
                WHERE r.id=?
                """,
                (rid,),
            ).fetchone()
            if not row:
                continue
            last = row["last_name"] or ""
            first = row["first_name"] or ""
            out[rid] = {
                "client_id": int(row["client_id"]),
                "log_number": str(row["log_number"] or "").strip(),
                "keys": _all_person_keys(last, first),
                "primary": _person_key(last, first),
                "norm": normalize_person(last, first),
            }
    finally:
        conn.close()
    return out


def run_task2(
    *,
    snapshot: Path,
    tax_log: Path,
    audit_db: Path,
    run_id: int,
) -> dict:
    batch1 = load_batch1(snapshot)
    current = load_current_log(tax_log)

    classes = Counter()
    absent_where = Counter()
    details_for_workbook = []

    for b in batch1:
        cls, where = classify_row(b, current)
        classes[cls] += 1
        if cls == "ABSENT":
            absent_where[where or "not_found"] += 1
        details_for_workbook.append(
            {
                "row_number": b["row_number"],
                "class": cls,
                "relocated": where,
                "yr_norm": b["yr_norm"],
                "has_first": bool(b["first"]),
                "norm_surname": b["norm_primary"].surname_full,
                "norm_first_key": b["norm_primary"].first_key,
            }
        )

    total = len(batch1)
    changed = classes["NAME_CHANGED"] + classes["ABSENT"]
    # ~3 months: 2026-04-29 → 2026-07-31 ≈ 93 days ≈ 3.06 months
    days = (datetime(2026, 7, 31, tzinfo=timezone.utc) - datetime(2026, 4, 29, tzinfo=timezone.utc)).days
    months = days / 30.44
    change_rate = changed / total if total else 0
    monthly = change_rate / months if months else 0

    # Part B — self-match failures among 169 bulk unmatched
    unmatched_ids = load_unmatched_169(audit_db, run_id, snapshot)
    ret_map = map_returns_to_batch1_keys(snapshot, unmatched_ids)

    # Index batch1 by keys
    b1_by_key: dict[tuple[str, str], list] = {}
    for b in batch1:
        for k in b["keys"]:
            b1_by_key.setdefault(k, []).append(b)

    self_match_failures = []
    cohort_class = Counter()
    for rid, info in ret_map.items():
        # Find originating batch1 row(s)
        origins = []
        seen = set()
        for k in info["keys"]:
            for b in b1_by_key.get(k, []):
                if b["import_row_id"] not in seen:
                    seen.add(b["import_row_id"])
                    origins.append(b)
        if not origins:
            cohort_class["NO_BATCH1_ORIGIN"] += 1
            continue
        # Classify each origin; if any IDENTICAL → self-match failure
        for b in origins:
            cls, where = classify_row(b, current)
            cohort_class[cls] += 1
            if cls == "IDENTICAL":
                # Find the current row's normalized form
                cur_forms = []
                for xr in current["xcel_rows"]:
                    if xr["keys"] & b["keys"]:
                        cn = normalize_log_row(xr["last"], xr["first"])
                        cur_forms.append(
                            {
                                "surname": cn.surname_full,
                                "first_key": cn.first_key,
                                "variants": cn.surname_variants,
                            }
                        )
                bn = b["norm_primary"]
                self_match_failures.append(
                    {
                        "return_id": rid,
                        "batch1_row_number": b["row_number"],
                        "batch1_norm_surname": bn.surname_full,
                        "batch1_norm_first_key": bn.first_key,
                        "batch1_variants": bn.surname_variants,
                        "taxops_norm_surname": info["norm"].surname_full,
                        "taxops_norm_first_key": info["norm"].first_key,
                        "taxops_variants": info["norm"].surname_variants,
                        "current_xcel_forms": cur_forms[:3],
                        "keys_overlap_taxops_batch1": bool(info["keys"] & b["keys"]),
                    }
                )

    return {
        "part_a": {
            "batch1_named_rows": total,
            "classes": dict(classes),
            "absent_breakdown": dict(absent_where),
            "changed": changed,
            "change_rate": round(change_rate, 4),
            "window_days": days,
            "window_months": round(months, 2),
            "implied_monthly_rate": round(monthly, 4),
            "implied_monthly_row_count": round(changed / months, 1) if months else 0,
        },
        "part_b": {
            "unmatched_bulk_cohort_size": len(unmatched_ids),
            "cohort_origin_class_counts": dict(cohort_class),
            "self_match_failure_count": len(self_match_failures),
            "self_match_failures": self_match_failures,  # workbook only
        },
        "workbook_rows": details_for_workbook,
    }


def append_workbook_tab(xlsx_path: Path, result: dict) -> Path:
    """Add Task2 tabs to a copy of the M9 workbook (new file)."""
    from openpyxl.styles import Alignment, Border, Font, PatternFill, Side
    from openpyxl.utils import get_column_letter

    # Prefer latest M9 workbook; write new timestamped file
    out_dir = Path(r"T:\audit\output")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = out_dir / f"client_audit_findings_TASK2_{stamp}.xlsx"

    if xlsx_path.is_file():
        wb = openpyxl.load_workbook(xlsx_path)
    else:
        wb = openpyxl.Workbook()

    HEADER_FILL = PatternFill("solid", fgColor="1E293B")
    HEADER_FONT = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    DATA_FONT = Font(name="Calibri", size=11)
    THIN = Border(bottom=Side(style="thin", color="E2E8F0"))

    def hdr(ws, cols):
        for i, (lab, w) in enumerate(cols, 1):
            c = ws.cell(1, i, lab)
            c.font = HEADER_FONT
            c.fill = HEADER_FILL
            c.alignment = Alignment(horizontal="center")
            ws.column_dimensions[get_column_letter(i)].width = w

    # Summary tab
    if "Task2_churn" in wb.sheetnames:
        del wb["Task2_churn"]
    ws = wb.create_sheet("Task2_churn", 0)
    hdr(ws, [("Metric", 40), ("Value", 60)])
    pa = result["part_a"]
    pb = result["part_b"]
    rows = [
        ("batch1_named_rows", pa["batch1_named_rows"]),
        ("IDENTICAL", pa["classes"].get("IDENTICAL", 0)),
        ("NAME_CHANGED", pa["classes"].get("NAME_CHANGED", 0)),
        ("ABSENT", pa["classes"].get("ABSENT", 0)),
        ("absent_relocated_other_sheet", pa["absent_breakdown"].get("relocated_other_sheet", 0)),
        ("absent_not_found", pa["absent_breakdown"].get("not_found", 0)),
        ("change_rate", pa["change_rate"]),
        ("window_months", pa["window_months"]),
        ("implied_monthly_rate", pa["implied_monthly_rate"]),
        ("implied_monthly_row_count", pa["implied_monthly_row_count"]),
        ("part_b_cohort_size", pb["unmatched_bulk_cohort_size"]),
        ("part_b_self_match_failures", pb["self_match_failure_count"]),
    ]
    for i, (k, v) in enumerate(rows, 2):
        ws.cell(i, 1, k).font = DATA_FONT
        ws.cell(i, 2, v).font = DATA_FONT
        ws.cell(i, 1).border = THIN
        ws.cell(i, 2).border = THIN

    # Self-match failures detail
    if "Task2_self_match" in wb.sheetnames:
        del wb["Task2_self_match"]
    ws2 = wb.create_sheet("Task2_self_match")
    hdr(
        ws2,
        [
            ("return_id", 12),
            ("batch1_row", 12),
            ("b1_surname", 28),
            ("b1_first_key", 16),
            ("taxops_surname", 28),
            ("taxops_first_key", 16),
            ("xcel_surname", 28),
            ("xcel_first_key", 16),
            ("keys_overlap", 12),
        ],
    )
    for i, f in enumerate(pb["self_match_failures"], 2):
        xcel = (f.get("current_xcel_forms") or [{}])[0]
        vals = [
            f["return_id"],
            f["batch1_row_number"],
            f["batch1_norm_surname"],
            f["batch1_norm_first_key"],
            f["taxops_norm_surname"],
            f["taxops_norm_first_key"],
            xcel.get("surname"),
            xcel.get("first_key"),
            f["keys_overlap_taxops_batch1"],
        ]
        for ci, v in enumerate(vals, 1):
            cell = ws2.cell(i, ci, v)
            cell.font = DATA_FONT
            cell.border = THIN

    wb.save(dest)
    return dest


if __name__ == "__main__":
    from audit.db import connect_audit

    snap = Path(r"T:\audit\snapshots\taxops_snapshot_20260731.sqlite")
    tax_log = Path(
        r"C:\Users\Windows 10\OneDrive - Xcel Financial Services LLC"
        r"\Shared\Logs\TAX LOG 2025 Live.xlsx"
    )
    dbs = sorted(Path(r"T:\audit").glob("audit_*.sqlite"), key=lambda p: p.stat().st_mtime)
    db = dbs[-1]
    conn = connect_audit(db)
    run_id = int(conn.execute("SELECT MAX(id) FROM audit_run").fetchone()[0])
    conn.close()

    result = run_task2(snapshot=snap, tax_log=tax_log, audit_db=db, run_id=run_id)
    Path(r"T:\audit\output\task2_log_churn.json").write_text(
        json.dumps(
            {
                "part_a": result["part_a"],
                "part_b": {
                    **{k: v for k, v in result["part_b"].items() if k != "self_match_failures"},
                    "self_match_failure_count": result["part_b"]["self_match_failure_count"],
                    # keep failures for workbook writer
                    "self_match_failures": result["part_b"]["self_match_failures"],
                },
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )

    # Console: no names
    console = {
        "part_a": result["part_a"],
        "part_b": {
            "unmatched_bulk_cohort_size": result["part_b"]["unmatched_bulk_cohort_size"],
            "cohort_origin_class_counts": result["part_b"]["cohort_origin_class_counts"],
            "self_match_failure_count": result["part_b"]["self_match_failure_count"],
        },
    }
    print(json.dumps(console, indent=2))

    m9s = sorted(Path(r"T:\audit\output").glob("client_audit_findings_M9_*.xlsx"))
    src = m9s[-1] if m9s else Path(r"T:\audit\output\missing.xlsx")
    xlsx = append_workbook_tab(src, result)
    print(json.dumps({"workbook": str(xlsx)}, indent=2))
