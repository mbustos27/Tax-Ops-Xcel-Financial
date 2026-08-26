"""
investigate_no_export_spouses.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only DOB enrichment pass for NO_EXPORT spouse rows from the latest
verify_spouses_vs_export report.

Splits rows into DOB_CORROBORATED, STILL_NO_EXPORT, and edge-case buckets.

Usage:
    python scripts/investigate_no_export_spouses.py --db T:\\taxops\\taxops.db
    python scripts/investigate_no_export_spouses.py --verify-csv reports\\spouse_export_verify_....csv
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from db import get_connection  # noqa: E402
from name_matcher import ACCEPT_THRESHOLD, score_client_names_pair  # noqa: E402
from verify_spouses_vs_export import DEFAULT_EXPORTS, _norm_dob, load_exports  # noqa: E402


def _latest_verify_csv(reports: Path) -> Path:
    matches = sorted(reports.glob("spouse_export_verify_*.csv"))
    if not matches:
        raise SystemExit("Run verify_spouses_vs_export.py first.")
    return matches[-1]


def _dob_index(export_rows: list[dict]) -> dict[str, list[dict]]:
    by_dob: dict[str, list[dict]] = defaultdict(list)
    for e in export_rows:
        if e["primary_dob"]:
            by_dob[e["primary_dob"]].append(e)
        if e["spouse_dob"]:
            by_dob[e["spouse_dob"]].append(e)
    return by_dob


def classify_no_export_row(
    row: dict,
    *,
    client_dob: str,
    spouse_dob: str,
    by_dob: dict[str, list[dict]],
) -> tuple[str, str]:
    cd = _norm_dob(client_dob)
    sd = _norm_dob(spouse_dob or row.get("spouse_dob") or "")

    if not cd and not sd:
        return "STILL_NO_EXPORT", "no_usable_dob"

    client_ln = (row.get("client_name") or "").split(",")[0].strip()
    client_fn = ""
    if "," in (row.get("client_name") or ""):
        client_fn = row["client_name"].split(",", 1)[1].strip()
    sp_parts = (row.get("spouse_name") or "").strip().split()
    sp_first = sp_parts[0] if sp_parts else ""
    sp_last = sp_parts[-1] if len(sp_parts) > 1 else ""

    # Try client DOB -> export primary
    if cd and cd in by_dob:
        for h in by_dob[cd]:
            if h["primary_dob"] != cd:
                continue  # hit via spouse_dob index
            score = score_client_names_pair(
                client_ln, client_fn, h["primary_last"], h["primary_first"]
            )
            exp_sf0 = (h["spouse_first"] or "").split()[0].upper()
            got_sf0 = sp_first.upper()
            if score >= ACCEPT_THRESHOLD:
                if got_sf0 and exp_sf0 == got_sf0:
                    return (
                        "DOB_CORROBORATED",
                        f"client DOB + name + spouse first: {h['taxpayer_name']}",
                    )
                if got_sf0 and exp_sf0 != got_sf0:
                    return (
                        "EDGE_DOB_NAME_MATCH_SPOUSE_MISMATCH",
                        f"client DOB+name match but export spouse "
                        f"{h['spouse_first']} {h['spouse_last']}",
                    )
                return (
                    "DOB_CORROBORATED",
                    f"client DOB + name match (spouse first-only stored): {h['taxpayer_name']}",
                )
            return (
                "EDGE_DOB_ONLY",
                f"client DOB matches export row but name score<{ACCEPT_THRESHOLD}: "
                f"{h['taxpayer_name']}",
            )

    # Try spouse DOB -> export spouse line
    if sd and sd in by_dob:
        for h in by_dob[sd]:
            if h["spouse_dob"] != sd:
                continue
            if score_client_names_pair(sp_last, sp_first, h["spouse_last"], h["spouse_first"]) >= 90:
                return (
                    "DOB_CORROBORATED",
                    f"spouse DOB + name: {h['taxpayer_name']}",
                )

    if cd or sd:
        return "STILL_NO_EXPORT", "dob_present_no_export_match"
    return "STILL_NO_EXPORT", "no_usable_dob"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None)
    ap.add_argument("--verify-csv", default=None)
    args = ap.parse_args()

    verify_csv = (
        Path(args.verify_csv)
        if args.verify_csv
        else _latest_verify_csv(Path(_TAXOPS) / "reports")
    )
    verify_rows = list(csv.DictReader(verify_csv.open(encoding="utf-8")))
    no_export = [r for r in verify_rows if r.get("verdict") == "NO_EXPORT"]
    print(f"Verify CSV: {verify_csv}")
    print(f"NO_EXPORT rows: {len(no_export)}")

    conn = get_connection(args.db)
    export_rows, _ = load_exports(list(DEFAULT_EXPORTS))
    by_dob = _dob_index(export_rows)

    with_dob = 0
    out_rows = []
    for r in no_export:
        cid = int(r["client_id"])
        c = conn.execute(
            "SELECT taxpayer_dob FROM clients WHERE id=?", (cid,)
        ).fetchone()
        client_dob = (c["taxpayer_dob"] if c else "") or r.get("client_dob") or ""
        spouse_dob = r.get("spouse_dob") or ""
        cd = _norm_dob(client_dob)
        sd = _norm_dob(spouse_dob)
        if cd or sd:
            with_dob += 1

        bucket, note = classify_no_export_row(
            r,
            client_dob=client_dob,
            spouse_dob=spouse_dob,
            by_dob=by_dob,
        )
        out_rows.append(
            {
                **r,
                "client_dob_resolved": client_dob,
                "client_dob_norm": cd,
                "spouse_dob_norm": sd,
                "dob_bucket": bucket,
                "dob_match_note": note,
            }
        )

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(_TAXOPS) / "reports" / f"no_export_dob_investigation_{ts}.csv"
    out_path.parent.mkdir(exist_ok=True)
    base_fields = list(out_rows[0].keys()) if out_rows else []
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=base_fields)
        w.writeheader()
        w.writerows(out_rows)

    counts: dict[str, int] = defaultdict(int)
    for row in out_rows:
        counts[row["dob_bucket"]] += 1

    print(f"\nDOB coverage: {with_dob}/{len(no_export)} rows have client and/or spouse DOB")
    print(f"Wrote {out_path}")
    print("\nBucket counts:")
    for k, v in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {k}: {v}")
    total = sum(counts.values())
    print(f"\nReconcile: bucket sum={total} vs NO_EXPORT={len(no_export)}")
    if total != len(no_export):
        print("WARNING: counts do not reconcile")


if __name__ == "__main__":
    main()
