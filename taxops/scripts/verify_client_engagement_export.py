"""Verify ALL client-export rows vs TaxOps (bidirectional, production matchers).

Forward: export Display Name → `_match_return` (exact + TIN + find_client fuzzy).
Reverse: TaxOps TY return → best export via fuzz.token_sort_ratio (+ TIN boost).
Rules: E-File Accepted → LOG OUT + EF Accepted;
       Extension E-File Accepted → not LOG OUT + EF Ext Accepted.
"""

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
from name_matcher import ACCEPT_THRESHOLD, fuzz, normalize_name

CSV = Path(r"C:\Users\Windows 10\Downloads\client-export-2026-08-27_180922.csv")
TAX_YEAR = 2025
OUT = _TAXOPS / "data" / "processed" / "client_engagement_verify_all.json"


def _status_issues(
    engagement: str, expected_drake: str | None, status: str, drake: str
) -> list[str]:
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
        # Only flag when we have a mapped engagement → drake_raw expectation
        # and TaxOps already has a non-empty raw that disagrees.
        issues.append("DRAKE_STATUS_MISMATCH")
    return issues


def _display_key(last: str | None, first: str | None) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if first:
        return f"{last}, {first}"
    return last


def main() -> None:
    conn = get_connection()
    export_rows: list[dict] = []

    with CSV.open(newline="", encoding="utf-8-sig") as fh:
        for raw in csv.DictReader(fh):
            norm = normalize_client_export_row(raw, tax_year=TAX_YEAR)
            if not norm:
                continue
            export_rows.append(
                {
                    "display": (raw.get("Display Name") or "").strip(),
                    "client_type": (raw.get("Client Type") or "").strip(),
                    "engagement": (norm.get("engagement_status") or "").strip(),
                    "tin4": norm["clients"].get("ssn_last4"),
                    "norm": norm,
                }
            )

    taxops_rows = [
        dict(r)
        for r in conn.execute(
            """
            SELECT r.id, r.client_id, cl.last_name, cl.first_name, cl.ssn_last4,
                   r.client_status, r.drake_status_raw
            FROM returns r
            JOIN clients cl ON cl.id = r.client_id
            WHERE r.tax_year = ?
            ORDER BY cl.last_name, cl.first_name
            """,
            (TAX_YEAR,),
        ).fetchall()
    ]

    counts: Counter = Counter()
    forward: list[dict] = []
    export_rid_map: dict[int, list[str]] = defaultdict(list)

    # ── Forward: every export row → TaxOps ──
    for ex in export_rows:
        counts["export_total"] += 1
        counts[f"type:{ex['client_type']}"] += 1
        eng = ex["engagement"] or "(blank)"
        counts[f"eng:{eng}"] += 1

        expected_drake = engagement_to_drake_status_raw(ex["engagement"])
        match = _match_return(conn, ex["norm"])
        rid = match.get("return_id")
        ambiguous = bool(match.get("ambiguous"))

        row: dict = {
            "export_name": ex["display"],
            "client_type": ex["client_type"],
            "tin4": ex["tin4"],
            "engagement": ex["engagement"],
            "expected_drake": expected_drake,
            "return_id": rid,
            "ambiguous": ambiguous,
        }

        if ambiguous:
            counts["ambiguous"] += 1
            row.update(ok=False, issue="AMBIGUOUS")
            forward.append(row)
            continue

        if not rid:
            counts["no_match"] += 1
            # Client matched but no TY return is still a gap for e-file statuses
            if match.get("client_id") and ex["engagement"] in {
                ENGAGEMENT_EFILE_ACCEPTED,
                ENGAGEMENT_EXTENSION_EFILE_ACCEPTED,
            }:
                counts["client_no_ty_return"] += 1
                row["client_id"] = match["client_id"]
                row["issue"] = "CLIENT_NO_TY_RETURN"
            else:
                row["issue"] = "NO_MATCH"
            row["ok"] = False
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
        issues = _status_issues(ex["engagement"], expected_drake, status, drake)

        row.update(
            client_status=status,
            drake_status_raw=drake,
            issues=issues,
            ok=not issues,
            issue=",".join(issues) if issues else "OK",
        )
        counts["matched"] += 1
        if issues:
            counts["status_mismatch"] += 1
            for i in issues:
                counts[i] += 1
        else:
            counts["ok"] += 1
        forward.append(row)

    # ── Reverse: TaxOps → export (catch stale duplicates / orphans) ──
    reverse: list[dict] = []
    matched_rids = set(export_rid_map.keys())

    for t in taxops_rows:
        counts["taxops_ty"] += 1
        last = t["last_name"] or ""
        first = t["first_name"]
        tin4 = t.get("ssn_last4")
        display = _display_key(last, first)
        best_ex = None
        best_score = 0.0

        for ex in export_rows:
            # Compare using same shape as export Display Name
            if first:
                # Individual: export is "LAST, FIRST"
                ex_cmp = normalize_name(ex["display"].replace(",", " "))
                tax_cmp = normalize_name(f"{last} {first}")
            else:
                ex_cmp = normalize_name(ex["display"])
                tax_cmp = normalize_name(last)
            score = float(fuzz.token_sort_ratio(tax_cmp, ex_cmp))
            if tin4 and ex["tin4"] and tin4 == ex["tin4"]:
                score = max(score, 98.0)
            if score > best_score:
                best_score = score
                best_ex = ex

        rev: dict = {
            "return_id": t["id"],
            "taxops_name": display,
            "tin4": tin4,
            "status": t["client_status"],
            "drake": t["drake_status_raw"],
            "reverse_score": round(best_score, 1),
            "export_name": best_ex["display"] if best_ex else None,
            "export_engagement": best_ex["engagement"] if best_ex else None,
            "forward_matched": t["id"] in matched_rids,
        }

        if best_score >= ACCEPT_THRESHOLD and best_ex:
            if t["id"] not in matched_rids:
                # Export exists but forward matched a different return → duplicate/stale
                counts["stale_duplicate"] += 1
                expected = engagement_to_drake_status_raw(best_ex["engagement"])
                issues = _status_issues(
                    best_ex["engagement"],
                    expected,
                    (t["client_status"] or "").strip(),
                    (t["drake_status_raw"] or "").strip(),
                )
                rev["issues"] = issues
                rev["issue"] = "STALE_DUPLICATE"
                rev["ok"] = False
            else:
                rev["issue"] = "OK"
                rev["ok"] = True
        else:
            counts["taxops_not_in_export"] += 1
            rev["issue"] = "NOT_IN_EXPORT"
            rev["ok"] = None

        reverse.append(rev)

    # Summaries
    efile = [r for r in forward if r.get("engagement") == ENGAGEMENT_EFILE_ACCEPTED]
    ext = [
        r
        for r in forward
        if r.get("engagement") == ENGAGEMENT_EXTENSION_EFILE_ACCEPTED
    ]
    mismatches = [r for r in forward if r.get("ok") is False and r.get("issue") not in {
        "NO_MATCH", "AMBIGUOUS", "CLIENT_NO_TY_RETURN"
    }]
    # Keep status mismatches separate from no-match
    status_bad = [r for r in forward if not r.get("ok") and r.get("issues")]
    no_match = [r for r in forward if r.get("issue") in {"NO_MATCH", "AMBIGUOUS", "CLIENT_NO_TY_RETURN"}]
    stale = [r for r in reverse if r.get("issue") == "STALE_DUPLICATE"]
    not_in_export = [r for r in reverse if r.get("issue") == "NOT_IN_EXPORT"]

    # Spot checks
    dental = next((r for r in forward if "D P DENTAL" in (r.get("export_name") or "")), None)
    eerie = [r for r in forward if "EERIE" in (r.get("export_name") or "").upper()]
    eerie += [r for r in reverse if "EERIE" in (r.get("taxops_name") or "").upper()]

    payload = {
        "csv": str(CSV),
        "tax_year": TAX_YEAR,
        "matcher": (
            f"_match_return forward; fuzz.token_sort_ratio>={ACCEPT_THRESHOLD} reverse"
        ),
        "counts": dict(counts),
        "efile_accepted": {
            "total": len(efile),
            "ok": sum(1 for r in efile if r.get("ok")),
            "bad": [r for r in efile if not r.get("ok")],
        },
        "ext_accepted": {
            "total": len(ext),
            "ok": sum(1 for r in ext if r.get("ok")),
            "bad": [r for r in ext if not r.get("ok")],
        },
        "status_mismatches": status_bad,
        "no_match_sample": no_match[:40],
        "no_match_count": len(no_match),
        "stale_duplicates": stale,
        "not_in_export_count": len(not_in_export),
        "not_in_export_sample": not_in_export[:40],
        "spot_checks": {"dental": dental, "eerie": eerie},
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    print(f"CSV: {CSV.name}  TY{TAX_YEAR}")
    print(f"Export rows: {counts['export_total']}  TaxOps TY returns: {counts['taxops_ty']}")
    print(
        f"Forward matched: {counts['matched']}  "
        f"OK: {counts['ok']}  status_mismatch: {counts['status_mismatch']}"
    )
    print(
        f"No match: {counts['no_match']}  ambiguous: {counts['ambiguous']}  "
        f"client_no_ty_return: {counts['client_no_ty_return']}"
    )
    print(
        f"E-File Accepted: {payload['efile_accepted']['ok']}/"
        f"{payload['efile_accepted']['total']} OK"
    )
    print(
        f"Extension E-File Accepted: {payload['ext_accepted']['ok']}/"
        f"{payload['ext_accepted']['total']} OK"
    )
    print(f"Stale TaxOps duplicates: {counts['stale_duplicate']}")
    print(f"TaxOps not in export: {counts['taxops_not_in_export']}")
    print(f"D P DENTAL: {dental}")
    if status_bad:
        print(f"\n--- STATUS MISMATCHES ({len(status_bad)}) ---")
        for m in status_bad[:50]:
            print(
                f"  [{m['issue']}] {m['export_name']} ({m['client_type']}) "
                f"eng={m['engagement']} -> status={m.get('client_status')} "
                f"drake={m.get('drake_status_raw')} rid={m.get('return_id')}"
            )
        if len(status_bad) > 50:
            print(f"  ... +{len(status_bad) - 50} more")
    print(f"\nWrote {OUT}")


if __name__ == "__main__":
    main()
