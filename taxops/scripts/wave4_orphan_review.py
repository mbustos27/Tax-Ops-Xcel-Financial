"""
wave4_orphan_review.py
~~~~~~~~~~~~~~~~~~~~~~
Human-review CSV for Wave4 orphan spouse candidates from reject_wave4_orphans
logic. Read-only — never deletes rows.

Uses Drake exports (CSVFILES) for cross-reference triage.

Usage:
    python scripts/wave4_orphan_review.py --db T:\\taxops\\taxops.db
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
from import_spouse_info import _load_csv_rows, _resolve_derived_last  # noqa: E402
from name_matcher import ACCEPT_THRESHOLD, normalize_name, score_client_names_pair  # noqa: E402
from reject_wave4_orphans import _drake_spouse_keys, find_orphans  # noqa: E402
from verify_spouses_vs_export import DEFAULT_EXPORTS, _norm_dob, load_exports  # noqa: E402

SIGNAL_ORDER = {
    "NO_SIGNAL": 0,
    "PARTIAL_SPOUSE_FIRST": 1,
    "PARTIAL_CLIENT_DOB": 2,
    "PARTIAL_CLIENT_NAME": 3,
}


def _build_export_dob_index(export_rows: list[dict]) -> dict[str, list[dict]]:
    by_dob: dict[str, list[dict]] = defaultdict(list)
    for e in export_rows:
        if e["primary_dob"]:
            by_dob[e["primary_dob"]].append(e)
    return by_dob


def _spouse_first_tokens(export_rows: list[dict]) -> set[str]:
    out: set[str] = set()
    for e in export_rows:
        sf = normalize_name(e["spouse_first"]).split()
        if sf:
            out.add(sf[0])
    return out


def triage_orphan(
    *,
    client_ln: str,
    client_fn: str,
    client_dob: str,
    sp_first: str,
    sp_last: str,
    export_rows: list[dict],
    by_dob: dict[str, list[dict]],
    spouse_first_tokens: set[str],
) -> tuple[str, str]:
    """Return (signal_tier, export_reference_note)."""
    cd = _norm_dob(client_dob)
    sf0 = normalize_name(sp_first).split()[0] if sp_first else ""

    # Client DOB matches export primary
    if cd and cd in by_dob:
        hits = by_dob[cd]
        for h in hits:
            score = score_client_names_pair(
                client_ln, client_fn, h["primary_last"], h["primary_first"]
            )
            exp_sf0 = normalize_name(h["spouse_first"]).split()[0]
            if score >= ACCEPT_THRESHOLD and sf0 and exp_sf0 == sf0:
                return (
                    "PARTIAL_CLIENT_DOB",
                    f"DOB+name match but spouse key absent from Drake index: "
                    f"{h['taxpayer_name']}",
                )
            if score >= ACCEPT_THRESHOLD:
                return (
                    "PARTIAL_CLIENT_DOB",
                    f"Client DOB+name match export primary; export spouse="
                    f"{h['spouse_first']} {h['spouse_last']} vs stored {sp_first} {sp_last}",
                )

    # Client name matches some export primary (no DOB required)
    for h in export_rows:
        if score_client_names_pair(
            client_ln, client_fn, h["primary_last"], h["primary_first"]
        ) >= ACCEPT_THRESHOLD:
            return (
                "PARTIAL_CLIENT_NAME",
                f"Name match export: {h['taxpayer_name']}",
            )

    # Spouse first token appears somewhere in exports
    if sf0 and sf0 in spouse_first_tokens:
        sample = next(
            (
                e
                for e in export_rows
                if normalize_name(e["spouse_first"]).split()[0] == sf0
            ),
            None,
        )
        note = (
            f"Spouse first token {sf0!r} on export line {sample['taxpayer_name']}"
            if sample
            else f"Spouse first token {sf0!r} in exports"
        )
        return "PARTIAL_SPOUSE_FIRST", note

    return "NO_SIGNAL", ""


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None)
    args = ap.parse_args()

    conn = get_connection(args.db)
    before_c = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
    ).fetchone()[0]
    before_p = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=1"
    ).fetchone()[0]

    # Drake spouse keys from live spouse.csv + CSVFILES exports
    drake_keys: set[str] = set()
    spouse_csv = r"f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv"
    if os.path.isfile(spouse_csv):
        drake_keys |= _drake_spouse_keys(spouse_csv, False)
    for path, combined in DEFAULT_EXPORTS:
        if os.path.isfile(path):
            drake_keys |= _drake_spouse_keys(path, combined)

    export_rows, _ = load_exports(list(DEFAULT_EXPORTS))
    by_dob = _build_export_dob_index(export_rows)
    spouse_first_tokens = _spouse_first_tokens(export_rows)

    orphans = find_orphans(conn, drake_keys)
    out_rows = []

    for r in orphans:
        cid = int(r["client_id"])
        c = conn.execute(
            """
            SELECT last_name, first_name, taxpayer_dob
            FROM clients WHERE id=?
            """,
            (cid,),
        ).fetchone()
        sp_first = (r["first_name"] or "").strip()
        sp_last = (r["last_name"] or r["derived_last_name"] or "").strip()
        client_ln = c["last_name"] if c else ""
        client_fn = c["first_name"] if c else ""
        client_dob = c["taxpayer_dob"] if c else ""

        tier, export_note = triage_orphan(
            client_ln=client_ln or "",
            client_fn=client_fn or "",
            client_dob=client_dob or "",
            sp_first=sp_first,
            sp_last=sp_last,
            export_rows=export_rows,
            by_dob=by_dob,
            spouse_first_tokens=spouse_first_tokens,
        )
        conf = r["match_confidence"]
        out_rows.append(
            {
                "sort_key": SIGNAL_ORDER[tier],
                "signal_tier": tier,
                "spouse_row_id": r["id"],
                "client_id": cid,
                "client_last_name": client_ln or "",
                "client_first_name": client_fn or "",
                "client_dob": client_dob or "",
                "spouse_first": sp_first,
                "spouse_last": sp_last,
                "match_confidence": conf if conf is not None else "",
                "source": r["source"] or "",
                "taxpayer_name": r["taxpayer_name"] or "",
                "export_reference": export_note,
            }
        )

    out_rows.sort(key=lambda x: (x["sort_key"], x["client_id"]))

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(_TAXOPS) / "reports" / f"wave4_orphan_review_{ts}.csv"
    out_path.parent.mkdir(exist_ok=True)
    fields = [k for k in out_rows[0].keys() if k != "sort_key"] if out_rows else []
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields, extrasaction="ignore")
        w.writeheader()
        w.writerows(out_rows)

    counts: dict[str, int] = defaultdict(int)
    for row in out_rows:
        counts[row["signal_tier"]] += 1

    after_c = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
    ).fetchone()[0]
    after_p = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=1"
    ).fetchone()[0]

    print(f"Wrote {out_path}")
    print(f"Total Wave4 orphan candidates: {len(out_rows)}")
    print("\nSignal tiers (sorted clearest orphans first):")
    for tier in sorted(SIGNAL_ORDER, key=SIGNAL_ORDER.get):
        print(f"  {tier}: {counts[tier]}")
    zero = counts["NO_SIGNAL"]
    partial = len(out_rows) - zero
    print(f"\nSummary: {zero} with zero corroborating signal; {partial} with partial signal")
    print(
        f"Spouses count unchanged: confirmed {before_c}->{after_c} "
        f"pending {before_p}->{after_p}"
    )


if __name__ == "__main__":
    main()
