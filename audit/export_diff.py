"""M9.1 — Diff two Drake CSM CLIENTS.xlsx exports (identity keys, both directions)."""

from __future__ import annotations

import sqlite3
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import openpyxl

from audit.db import connect_audit, connect_taxops_readonly
from audit.normalizer import normalize_drake_client_name
from audit.util import dumps


# Column order matching Drake CSM export
CSM_COLUMNS = [
    "ID (Last 4)",
    "Client Name",
    "Type",
    "Preparer",
    "Status",
    "Started",
    "Completed",
    "Last Change",
    "Changed By",
    "Refund",
    "BalDue",
    "Total Bill",
    "Bank Deposits",
    "Client Payments",
    "Amount Owed",
]


@dataclass
class CsmRow:
    source_row: int
    cells: dict[str, Any]
    id_last4: str
    name_raw: str
    norm_key: str  # normalized surname|first + last4

    def identity(self) -> tuple[str, str]:
        return (self.norm_key, self.id_last4)


def _last4(raw: Any) -> str:
    s = str(raw or "")
    digits = "".join(c for c in s if c.isdigit())
    return digits[-4:] if len(digits) >= 4 else ""


def _norm_name_key(client_name: str) -> str:
    n = normalize_drake_client_name(client_name)
    return f"{n.surname_full}|{n.first_key}"


def load_csm(path: Path) -> list[CsmRow]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    headers: list[str] = []
    rows: list[CsmRow] = []
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        vals = list(row)
        if i == 1:
            headers = [str(c or "").strip() for c in vals]
            continue
        if not any(v is not None and str(v).strip() for v in vals):
            continue
        cells = {}
        for hi, h in enumerate(headers):
            if not h:
                continue
            cells[h] = vals[hi] if hi < len(vals) else None
        name = str(cells.get("Client Name") or "").strip()
        if not name or name.upper().startswith("TOTAL"):
            continue
        last4 = _last4(cells.get("ID (Last 4)"))
        rows.append(
            CsmRow(
                source_row=i,
                cells=cells,
                id_last4=last4,
                name_raw=name,
                norm_key=_norm_name_key(name),
            )
        )
    wb.close()
    return rows


def _index(rows: list[CsmRow]) -> dict[tuple[str, str], list[CsmRow]]:
    idx: dict[tuple[str, str], list[CsmRow]] = defaultdict(list)
    for r in rows:
        idx[r.identity()].append(r)
    return idx


def cells_public(row: CsmRow) -> dict[str, Any]:
    """Full CSM columns for a row (contains names — for workbook/gitignored only)."""
    out = {}
    for col in CSM_COLUMNS:
        v = row.cells.get(col)
        if hasattr(v, "isoformat"):
            out[col] = v.isoformat()
        else:
            out[col] = v
    out["_source_row"] = row.source_row
    out["_norm_key"] = row.norm_key
    out["_id_last4"] = row.id_last4
    return out


def cross_ref_presence(
    row: CsmRow,
    *,
    taxops_conn: Optional[sqlite3.Connection],
    log_keys: set[tuple[str, str]],
    taxops_keys: set[tuple[str, str]],
    taxops_ty2025_keys: set[tuple[str, str]],
) -> dict[str, bool]:
    """Presence flags without echoing names to caller logs."""
    n = normalize_drake_client_name(row.name_raw)
    person_key = (n.surname_full, n.first_key)
    in_log = person_key in log_keys or any(
        (sv, n.first_key) in log_keys for sv in n.surname_variants
    )
    in_taxops = person_key in taxops_keys or any(
        (sv, n.first_key) in taxops_keys for sv in n.surname_variants
    )
    in_ty2025 = person_key in taxops_ty2025_keys or any(
        (sv, n.first_key) in taxops_ty2025_keys for sv in n.surname_variants
    )
    # Also try last4 on taxops if available
    if taxops_conn and row.id_last4 and not in_taxops:
        hit = taxops_conn.execute(
            "SELECT 1 FROM clients WHERE ssn_last4=? LIMIT 1", (row.id_last4,)
        ).fetchone()
        if hit:
            in_taxops = True
    return {
        "in_tax_log": in_log,
        "in_taxops_client": in_taxops,
        "in_taxops_ty2025": in_ty2025,
    }


def shared_attribute_analysis(removed: list[CsmRow]) -> dict[str, Any]:
    if not removed:
        return {"empty": True}
    preparers = Counter(str(r.cells.get("Preparer") or "").strip() for r in removed)
    statuses = Counter(str(r.cells.get("Status") or "").strip() for r in removed)
    types = Counter(str(r.cells.get("Type") or "").strip() for r in removed)
    last4s = sorted(r.id_last4 for r in removed if r.id_last4)
    sequential = False
    if len(last4s) >= 2:
        nums = []
        for x in last4s:
            try:
                nums.append(int(x))
            except ValueError:
                pass
        nums.sort()
        sequential = any(nums[i + 1] - nums[i] == 1 for i in range(len(nums) - 1))

    changes = []
    for r in removed:
        v = r.cells.get("Last Change")
        if v is not None:
            changes.append(str(v))
    changes_sorted = sorted(changes)

    adjacent = False
    # crude: same calendar day on Last Change
    days = Counter(c[:10] for c in changes_sorted if len(c) >= 10)
    adjacent = any(c >= 2 for c in days.values())

    return {
        "count": len(removed),
        "preparers": dict(preparers),
        "statuses": dict(statuses),
        "types": dict(types),
        "last4_sequential_pair": sequential,
        "last_change_same_day_cluster": adjacent,
        "last_change_day_counts": dict(days),
        "unique_preparers": len([k for k in preparers if k]),
        "unique_statuses": len([k for k in statuses if k]),
    }


def build_log_keys(tax_log_path: Path) -> set[tuple[str, str]]:
    from audit import config
    from audit.normalizer import keys_with_transposition

    wb = openpyxl.load_workbook(tax_log_path, read_only=True, data_only=True)
    ws = wb[config.SHEET_INDIVIDUALS]
    keys: set[tuple[str, str]] = set()
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
        for k in keys_with_transposition(last, first):
            keys.add(k)
    wb.close()
    return keys


def build_taxops_keys(snapshot: Path) -> tuple[set[tuple[str, str]], set[tuple[str, str]]]:
    from audit.normalizer import normalize_person

    conn = connect_taxops_readonly(snapshot)
    try:
        all_keys: set[tuple[str, str]] = set()
        ty_keys: set[tuple[str, str]] = set()
        clients = {
            int(r["id"]): r
            for r in conn.execute(
                "SELECT id, last_name, first_name FROM clients"
            )
        }
        for r in clients.values():
            n = normalize_person(r["last_name"] or "", r["first_name"] or "")
            for k in n.match_keys:
                all_keys.add(k)
        for r in conn.execute(
            "SELECT client_id FROM returns WHERE tax_year=2025"
        ):
            c = clients.get(int(r["client_id"]))
            if not c:
                continue
            n = normalize_person(c["last_name"] or "", c["first_name"] or "")
            for k in n.match_keys:
                ty_keys.add(k)
        return all_keys, ty_keys
    finally:
        conn.close()


def run_export_diff(
    older_path: Path,
    newer_path: Path,
    *,
    tax_log_path: Path,
    taxops_snapshot: Path,
    audit_db: Optional[Path] = None,
    run_id: Optional[int] = None,
) -> dict[str, Any]:
    """
    Diff older (expected baseline) vs newer (audit input).

    removals = in older, not in newer
    additions = in newer, not in older
    """
    older = load_csm(older_path)
    newer = load_csm(newer_path)
    oi, ni = _index(older), _index(newer)

    removed_keys = set(oi) - set(ni)
    added_keys = set(ni) - set(oi)

    log_keys = build_log_keys(tax_log_path)
    taxops_keys, ty_keys = build_taxops_keys(taxops_snapshot)
    tconn = connect_taxops_readonly(taxops_snapshot)

    removals = []
    try:
        for k in sorted(removed_keys, key=lambda x: x[0]):
            for row in oi[k]:
                presence = cross_ref_presence(
                    row,
                    taxops_conn=tconn,
                    log_keys=log_keys,
                    taxops_keys=taxops_keys,
                    taxops_ty2025_keys=ty_keys,
                )
                removals.append(
                    {
                        "direction": "removed_from_newer",
                        "presence": presence,
                        "row": cells_public(row),
                    }
                )
        additions = []
        for k in sorted(added_keys, key=lambda x: x[0]):
            for row in ni[k]:
                presence = cross_ref_presence(
                    row,
                    taxops_conn=tconn,
                    log_keys=log_keys,
                    taxops_keys=taxops_keys,
                    taxops_ty2025_keys=ty_keys,
                )
                additions.append(
                    {
                        "direction": "added_in_newer",
                        "presence": presence,
                        "row": cells_public(row),
                    }
                )
    finally:
        tconn.close()

    removed_rows = [oi[k][0] for k in removed_keys]
    shared = shared_attribute_analysis(removed_rows)

    # Presence pattern summary (no names)
    presence_patterns = Counter()
    for item in removals:
        p = item["presence"]
        tag = (
            f"log={p['in_tax_log']}|taxops={p['in_taxops_client']}|"
            f"ty2025={p['in_taxops_ty2025']}"
        )
        presence_patterns[tag] += 1

    # Recommendation (do not apply)
    recommendation = {
        "action": None,
        "rationale": None,
        "proposed_config": {
            "EXPECTED_DRAKE_ROWS": len(newer),
            "EXPECTED_DRAKE_TYPES": dict(
                Counter(str(r.cells.get("Type") or "").strip().upper() for r in newer)
            ),
        },
        "keep_acknowledge_baseline_drift": True,
    }
    # Explain if all removals share a clear presence pattern
    if len(removals) == 4 and len(additions) == 0:
        recommendation["action"] = "re_lock_baselines"
        recommendation["keep_acknowledge_baseline_drift"] = False
        recommendation["rationale"] = (
            "Net −4 with zero additions; removals enumerated with cross-source presence. "
            "Re-lock EXPECTED_* to the audit input file once operator confirms the "
            "removals are intentional CSM state (not a filtered export)."
        )
    elif additions and removals:
        recommendation["action"] = "keep_flag"
        recommendation["rationale"] = (
            "Both removals and additions present — not a simple −4 vanishing; "
            "investigate before dropping --acknowledge-baseline-drift."
        )
    elif not removals and additions:
        recommendation["action"] = "re_lock_baselines"
        recommendation["keep_acknowledge_baseline_drift"] = False
        recommendation["rationale"] = (
            "Audit input is a subset/superset mismatch favoring additions only; "
            "re-lock to the file you will use going forward."
        )
    else:
        recommendation["action"] = "keep_flag"
        recommendation["rationale"] = "Unexpected empty or asymmetric diff — keep the flag."

    # File mtime note for operator
    result = {
        "older_path": str(older_path),
        "newer_path": str(newer_path),
        "older_count": len(older),
        "newer_count": len(newer),
        "older_mtime": older_path.stat().st_mtime,
        "newer_mtime": newer_path.stat().st_mtime,
        "removals_count": len(removals),
        "additions_count": len(additions),
        "removals": removals,  # names — for workbook only
        "additions": additions,
        "shared_attributes": shared,
        "presence_patterns": dict(presence_patterns),
        "recommendation": recommendation,
    }

    if audit_db and run_id is not None:
        conn = connect_audit(audit_db)
        try:
            # Store summary without full name payload in a compact notes table via finding
            conn.execute(
                """
                INSERT INTO audit_finding (
                  run_id, finding_type, subtype, severity, subject_kind, subject_id,
                  tax_year, detail_json, source_refs
                ) VALUES (?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    "FIELD_MISMATCH",
                    "csm_export_diff",
                    20,
                    "export",
                    None,
                    2025,
                    dumps(
                        {
                            "removals_count": len(removals),
                            "additions_count": len(additions),
                            "shared_attributes": shared,
                            "presence_patterns": dict(presence_patterns),
                            "recommendation": {
                                k: v
                                for k, v in recommendation.items()
                                if k != "proposed_config"
                            }
                            | {
                                "proposed_EXPECTED_DRAKE_ROWS": recommendation[
                                    "proposed_config"
                                ]["EXPECTED_DRAKE_ROWS"],
                                "proposed_EXPECTED_DRAKE_TYPES": recommendation[
                                    "proposed_config"
                                ]["EXPECTED_DRAKE_TYPES"],
                            },
                            "older_count": len(older),
                            "newer_count": len(newer),
                        }
                    ),
                    dumps(
                        {
                            "older": str(older_path),
                            "newer": str(newer_path),
                        }
                    ),
                ),
            )
            conn.commit()
        finally:
            conn.close()

    return result


def console_summary(diff: dict[str, Any]) -> dict[str, Any]:
    """PII-free console view."""
    return {
        "older_count": diff["older_count"],
        "newer_count": diff["newer_count"],
        "removals_count": diff["removals_count"],
        "additions_count": diff["additions_count"],
        "shared_attributes": diff["shared_attributes"],
        "presence_patterns": diff["presence_patterns"],
        "recommendation": diff["recommendation"],
        "mtime_note": {
            "older_newer_by_mtime": diff["older_mtime"] > diff["newer_mtime"],
            "note": (
                "If older_path has a newer mtime than newer_path, chronological "
                "file dates disagree with the baseline/audit labeling — review carefully."
            ),
        },
        # Per-removal: status/preparer/type/presence only (no names)
        "removals_trace": [
            {
                "type": r["row"].get("Type"),
                "status": r["row"].get("Status"),
                "preparer": r["row"].get("Preparer"),
                "started": str(r["row"].get("Started")),
                "completed": str(r["row"].get("Completed")),
                "last_change": str(r["row"].get("Last Change")),
                "changed_by": r["row"].get("Changed By"),
                "total_bill": r["row"].get("Total Bill"),
                "presence": r["presence"],
                "id_last4_present": bool(r["row"].get("_id_last4")),
            }
            for r in diff["removals"]
        ],
        "additions_trace": [
            {
                "type": r["row"].get("Type"),
                "status": r["row"].get("Status"),
                "preparer": r["row"].get("Preparer"),
                "last_change": str(r["row"].get("Last Change")),
                "presence": r["presence"],
            }
            for r in diff["additions"]
        ],
    }
