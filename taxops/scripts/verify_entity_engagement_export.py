"""Bidirectional entity verification: export <-> TaxOps using production matchers."""

from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from client_export_sync import normalize_client_export_row
from db import get_connection
from drake_importer import _match_return
from engagement_status_rules import (
    ENGAGEMENT_EFILE_ACCEPTED,
    ENGAGEMENT_EXTENSION_EFILE_ACCEPTED,
    engagement_to_drake_status_raw,
)
from name_matcher import ACCEPT_THRESHOLD, normalize_name, fuzz

CSV = Path(r"C:\Users\Windows 10\Downloads\client-export-2026-08-27_180922.csv")
TAX_YEAR = 2025
ENTITY_TYPES = frozenset(
    {"Corporate", "Business 1120-S", "Partnership", "Exempt", "Fiduciary"}
)
OUT = _TAXOPS / "data" / "processed" / "entity_engagement_verify_v2.json"


def _load_export_entities() -> list[dict]:
    rows: list[dict] = []
    with CSV.open(newline="", encoding="utf-8-sig") as fh:
        for raw in csv.DictReader(fh):
            ctype = (raw.get("Client Type") or "").strip()
            if ctype not in ENTITY_TYPES:
                continue
            norm = normalize_client_export_row(raw, tax_year=TAX_YEAR)
            if not norm:
                continue
            rows.append(
                {
                    "raw": raw,
                    "norm": norm,
                    "display": (raw.get("Display Name") or "").strip(),
                    "client_type": ctype,
                    "engagement": (norm.get("engagement_status") or "").strip(),
                    "tin4": norm["clients"].get("ssn_last4"),
                }
            )
    return rows


def _taxops_entities(conn) -> list[dict]:
    rows = conn.execute(
        """
        SELECT r.id, r.client_id, cl.last_name, cl.first_name, cl.ssn_last4,
               r.client_status, r.drake_status_raw,
               rf.form_1120, rf.form_1120s, rf.form_1065_llc, rf.form_990_1041
        FROM returns r
        JOIN clients cl ON cl.id = r.client_id
        JOIN return_forms rf ON rf.return_id = r.id
        WHERE r.tax_year = ?
          AND (
            COALESCE(rf.form_1120,0)=1 OR COALESCE(rf.form_1120s,0)=1
            OR COALESCE(rf.form_1065_llc,0)=1 OR COALESCE(rf.form_990_1041,0)=1
          )
        ORDER BY cl.last_name
        """,
        (TAX_YEAR,),
    ).fetchall()
    return [dict(r) for r in rows]


def _status_issues(engagement: str, expected_drake: str | None, status: str, drake: str) -> list[str]:
    issues: list[str] = []
    if engagement == ENGAGEMENT_EFILE_ACCEPTED:
        if status != "LOG OUT":
            issues.append("EFILE_NOT_LOGOUT")
        if drake != "EF Accepted":
            issues.append("DRAKE_NOT_EF_ACCEPTED")
    elif engagement == ENGAGEMENT_EXTENSION_EFILE_ACCEPTED:
        if status == "LOG OUT":
            issues.append("EXT_PREMATURE_LOGOUT")
        if drake != "EF Ext Accepted":
            issues.append("DRAKE_NOT_EF_EXT")
    elif expected_drake and drake and drake != expected_drake:
        issues.append("DRAKE_STATUS_MISMATCH")
    return issues


def main() -> None:
    conn = get_connection()
    export_rows = _load_export_entities()
    taxops_rows = _taxops_entities(conn)

    # ── Forward: export -> TaxOps via _match_return ──
    forward: list[dict] = []
    export_rid_map: dict[int, list[str]] = defaultdict(list)
    counts = Counter()

    for ex in export_rows:
        counts["export_entity"] += 1
        norm = ex["norm"]
        engagement = ex["engagement"]
        expected_drake = engagement_to_drake_status_raw(engagement)
        match = _match_return(conn, norm)
        rid = match.get("return_id")

        row = {
            "direction": "export_to_taxops",
            "export_name": ex["display"],
            "export_type": ex["client_type"],
            "export_tin4": ex["tin4"],
            "engagement": engagement,
            "expected_drake": expected_drake,
            "match_return_id": rid,
            "ambiguous": bool(match.get("ambiguous")),
        }

        if not rid or match.get("ambiguous"):
            counts["export_no_match"] += 1
            row["ok"] = False
            row["issue"] = "AMBIGUOUS" if match.get("ambiguous") else "NO_MATCH"
            forward.append(row)
            continue

        rid = int(rid)
        export_rid_map[rid].append(ex["display"])
        db = conn.execute(
            "SELECT client_status, drake_status_raw FROM returns WHERE id=?",
            (rid,),
        ).fetchone()
        status = (db["client_status"] or "").strip()
        drake = (db["drake_status_raw"] or "").strip()
        issues = _status_issues(engagement, expected_drake, status, drake)

        row.update(
            {
                "client_status": status,
                "drake_status_raw": drake,
                "issues": issues,
                "ok": not issues,
                "issue": ",".join(issues) if issues else "OK",
            }
        )
        counts["export_matched"] += 1
        counts["ok" if not issues else "status_mismatch"] += 1
        forward.append(row)

    # Duplicate export targets (same return_id from multiple export rows — shouldn't happen)
    dup_targets = {rid: names for rid, names in export_rid_map.items() if len(names) > 1}

    # ── Reverse: TaxOps -> export via fuzzy name + TIN ──
    reverse: list[dict] = []
    matched_taxops_rids = set(export_rid_map.keys())

    for t in taxops_rows:
        rid = t["id"]
        last = t["last_name"] or ""
        tin4 = t.get("ssn_last4")
        best_ex: dict | None = None
        best_score = 0

        for ex in export_rows:
            score = fuzz.token_sort_ratio(normalize_name(last), normalize_name(ex["display"]))
            if tin4 and ex["tin4"] and tin4 == ex["tin4"]:
                score = max(score, 98)
            if score > best_score:
                best_score = score
                best_ex = ex

        rev = {
            "direction": "taxops_to_export",
            "return_id": rid,
            "taxops_name": last,
            "taxops_tin4": tin4,
            "taxops_status": t["client_status"],
            "taxops_drake": t["drake_status_raw"],
            "reverse_score": best_score,
            "export_name": best_ex["display"] if best_ex else None,
            "export_engagement": best_ex["engagement"] if best_ex else None,
            "matched_by_forward": rid in matched_taxops_rids,
        }

        if best_score >= ACCEPT_THRESHOLD and best_ex:
            expected_drake = engagement_to_drake_status_raw(best_ex["engagement"])
            issues = _status_issues(
                best_ex["engagement"],
                expected_drake,
                (t["client_status"] or "").strip(),
                (t["drake_status_raw"] or "").strip(),
            )
            rev["ok"] = not issues
            rev["issues"] = issues
            if rid not in matched_taxops_rids:
                counts["taxops_duplicate_or_stale"] += 1
                rev["issue"] = "DUPLICATE_STALE_TAXOPS"
            elif issues:
                rev["issue"] = ",".join(issues)
            else:
                rev["issue"] = "OK"
        else:
            rev["ok"] = None  # truly not in export
            rev["issue"] = "NOT_IN_EXPORT"
            counts["taxops_not_in_export"] += 1

        reverse.append(rev)

    eerie = [r for r in forward + reverse if r.get("export_name") and "EERIE" in str(r.get("export_name", "")).upper()]
    eerie += [r for r in reverse if r.get("taxops_name") and "EERIE" in str(r.get("taxops_name", "")).upper()]

    payload = {
        "csv": str(CSV),
        "tax_year": TAX_YEAR,
        "matcher": f"_match_return (forward) + find_client/fuzz.token_sort_ratio>={ACCEPT_THRESHOLD} (reverse)",
        "counts": dict(counts),
        "forward_mismatches": [r for r in forward if not r.get("ok")],
        "reverse_duplicates_stale": [
            r for r in reverse if r.get("issue") == "DUPLICATE_STALE_TAXOPS"
        ],
        "reverse_not_in_export": [r for r in reverse if r.get("issue") == "NOT_IN_EXPORT"],
        "reverse_status_mismatches": [
            r for r in reverse if r.get("issues")
        ],
        "eerie_lane": eerie,
        "dup_forward_targets": dup_targets,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"Export entities: {counts['export_entity']}")
    print(f"Forward matched: {counts['export_matched']}  status OK: {counts['ok']}  mismatches: {counts['status_mismatch']}")
    print(f"Export no match: {counts['export_no_match']}")
    print(f"TaxOps duplicate/stale (in export but different return): {counts['taxops_duplicate_or_stale']}")
    print(f"TaxOps truly not in export: {counts['taxops_not_in_export']}")
    print(f"\nEERIE LANE detail:")
    for e in eerie:
        print(f"  {e}")
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
