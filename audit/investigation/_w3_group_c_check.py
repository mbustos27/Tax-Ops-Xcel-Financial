"""Wave 3 Group C ITIN check + Wave 4 spouse store counts."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

DB = Path(r"T:\taxops\taxops.db")
OUT = Path(r"T:\audit\investigation\W3-group-c-itin-check.json")

PAIRS = [
    (659, 2275, "HERNANDEZ|ISMAEL"),
    (731, 2276, "NUNO|JUAN"),
    (1215, 2272, "ALVARADO|OSCAR"),
    (1278, 2393, "LUNA|ESTEBAN"),
    (1344, 2404, "VALDEZ|SANDRA"),
    (1440, 2273, "HERNANDEZ|ABEL"),
    (1506, 2329, "TASHAYOD|ALEX"),
    (1709, 2429, "SOLOMON|LAUREN"),
]


def main() -> None:
    conn = sqlite3.connect(str(DB))
    conn.row_factory = sqlite3.Row
    results = []
    for older_id, newer_id, key in PAIRS:
        older = conn.execute(
            "SELECT id, last_name, first_name, ssn_last4, id_type, created_at "
            "FROM clients WHERE id=?",
            (older_id,),
        ).fetchone()
        newer = conn.execute(
            "SELECT id, last_name, first_name, ssn_last4, id_type, created_at "
            "FROM clients WHERE id=?",
            (newer_id,),
        ).fetchone()
        oy = [
            dict(r)
            for r in conn.execute(
                "SELECT id, tax_year, log_number, client_status FROM returns "
                "WHERE client_id=? ORDER BY tax_year",
                (older_id,),
            )
        ]
        ny = [
            dict(r)
            for r in conn.execute(
                "SELECT id, tax_year, log_number, client_status FROM returns "
                "WHERE client_id=? ORDER BY tax_year",
                (newer_id,),
            )
        ]
        older_years = {r["tax_year"] for r in oy if r["tax_year"] is not None}
        newer_years = {r["tax_year"] for r in ny if r["tax_year"] is not None}
        overlap = sorted(older_years & newer_years)
        # ITIN→SSN transition signal: contiguous handoff (older max year < newer min)
        handoff = False
        if older_years and newer_years:
            handoff = max(older_years) < min(newer_years)
        contiguous_or_gap = None
        if older_years and newer_years:
            contiguous_or_gap = min(newer_years) - max(older_years)
        verdict = "distinct_people"
        if handoff and older and newer:
            # still different last4 — could be ITIN→SSN; flag for human
            verdict = "possible_itin_ssn_handoff"
        if overlap:
            verdict = "same_year_overlap_distinct"
        results.append(
            {
                "key": key,
                "older_id": older_id,
                "newer_id": newer_id,
                "older": dict(older) if older else None,
                "newer": dict(newer) if newer else None,
                "older_returns": oy,
                "newer_returns": ny,
                "older_years": sorted(older_years),
                "newer_years": sorted(newer_years),
                "overlap_years": overlap,
                "year_gap": contiguous_or_gap,
                "verdict": verdict,
            }
        )
        print(
            f"{key}: older={older_id} yrs={sorted(older_years)} "
            f"newer={newer_id} yrs={sorted(newer_years)} "
            f"overlap={overlap} gap={contiguous_or_gap} -> {verdict}"
        )

    # spouse store
    spouse = {
        "clients_spouse_last": conn.execute(
            "SELECT COUNT(*) FROM clients WHERE spouse_last_name IS NOT NULL "
            "AND TRIM(spouse_last_name)!=''"
        ).fetchone()[0],
        "clients_spouse_first": conn.execute(
            "SELECT COUNT(*) FROM clients WHERE spouse_first_name IS NOT NULL "
            "AND TRIM(spouse_first_name)!=''"
        ).fetchone()[0],
    }
    for col in ("spouse_dob", "spouse_cell", "spouse_work_phone", "spouse_email"):
        spouse[col] = conn.execute(
            f"SELECT COUNT(*) FROM clients WHERE {col} IS NOT NULL AND TRIM({col})!=''"
        ).fetchone()[0]
    spouse["spouses_rows"] = conn.execute("SELECT COUNT(*) FROM spouses").fetchone()[0]
    spouse["clients_only_spouse_names"] = conn.execute(
        """
        SELECT COUNT(*) FROM clients c
        WHERE (c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name)!='')
          AND NOT EXISTS (SELECT 1 FROM spouses s WHERE s.client_id=c.id)
        """
    ).fetchone()[0]
    print("spouse_store", spouse)

    OUT.write_text(
        json.dumps(
            {
                "group_c": results,
                "itin_handoff_candidates": [
                    r["key"] for r in results if r["verdict"] == "possible_itin_ssn_handoff"
                ],
                "false_positive_safe": [
                    r["key"]
                    for r in results
                    if r["verdict"] in ("distinct_people", "same_year_overlap_distinct")
                ],
                "spouse_store": spouse,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print("wrote", OUT)
    conn.close()


if __name__ == "__main__":
    main()
