"""
cleanup_wrong_spouse_attaches.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Delete confirmed spouse rows that ``verify_spouses_vs_export.py`` classified
as WRONG_ATTACH or WRONG_SINGLE_HAS_SPOUSE (export-backed).

Default is dry-run. Pass --apply only after reviewing the printed list.

Never deletes rows with confirmed_at_intake=1.

Usage:
    python scripts/cleanup_wrong_spouse_attaches.py --db T:\\taxops\\taxops.db
    python scripts/cleanup_wrong_spouse_attaches.py --db T:\\taxops\\taxops.db --apply
    python scripts/cleanup_wrong_spouse_attaches.py --verify-csv reports\\spouse_export_verify_....csv
    python scripts/cleanup_wrong_spouse_attaches.py --db T:\\taxops\\taxops.db --only-test
    python scripts/cleanup_wrong_spouse_attaches.py --db T:\\taxops\\taxops.db --only-test --apply
"""
from __future__ import annotations

import argparse
import csv
import os
import sys
from pathlib import Path
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from db import get_connection  # noqa: E402

BAD_VERDICTS = frozenset({"WRONG_ATTACH", "WRONG_SINGLE_HAS_SPOUSE"})
TEST_VERDICT = "SPOUSE_IS_OTHER_CLIENT_NO_EXPORT"


def _is_test_junk_row(verify_row: dict) -> bool:
    """True only when verify row is confirmed TEST junk (not ID range alone)."""
    if verify_row.get("verdict") != TEST_VERDICT:
        return False
    spouse = (verify_row.get("spouse_name") or "").strip().upper()
    client = (verify_row.get("client_name") or "").strip().upper()
    if spouse != "UNKNOWN TEST":
        return False
    if "TEST" not in client:
        return False
    return True


def _is_test_junk_db_row(row) -> bool:
    sp_first = (row["first_name"] or "").strip().upper()
    sp_last = (row["last_name"] or row["derived_last_name"] or "").strip().upper()
    client_ln = (row["client_ln"] or "").strip().upper()
    client_fn = (row["client_fn"] or "").strip().upper()
    if sp_first != "UNKNOWN" or sp_last != "TEST":
        return False
    if "TEST" not in client_ln and "TEST" not in client_fn:
        return False
    return True


def _latest_verify_csv(reports: Path) -> Path:
    matches = sorted(reports.glob("spouse_export_verify_*.csv"))
    if not matches:
        raise SystemExit(
            f"No spouse_export_verify_*.csv under {reports}. "
            "Run scripts/verify_spouses_vs_export.py first."
        )
    return matches[-1]


def load_targets(verify_csv: Path, *, only_test: bool = False) -> list[dict]:
    rows = list(csv.DictReader(verify_csv.open(encoding="utf-8")))
    if only_test:
        return [r for r in rows if _is_test_junk_row(r)]
    return [r for r in rows if r.get("verdict") in BAD_VERDICTS]


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None)
    ap.add_argument(
        "--verify-csv",
        default=None,
        help="Path to spouse_export_verify_*.csv (default: latest under reports/)",
    )
    ap.add_argument(
        "--only-test",
        action="store_true",
        help="Delete TEST junk rows (UNKNOWN TEST spouse + TEST client name) from verify CSV",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Actually DELETE the spouse rows (default: dry-run)",
    )
    args = ap.parse_args()

    verify_csv = (
        Path(args.verify_csv)
        if args.verify_csv
        else _latest_verify_csv(Path(_TAXOPS) / "reports")
    )
    targets = load_targets(verify_csv, only_test=args.only_test)
    mode = "TEST junk" if args.only_test else "export-backed bad verdicts"
    print(f"Verify CSV: {verify_csv}")
    print(f"Mode: {mode}")
    print(f"Rows to remove: {len(targets)}")

    conn = get_connection(args.db)
    before_c = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
    ).fetchone()[0]
    before_p = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=1"
    ).fetchone()[0]
    print(f"DB spouses before: confirmed={before_c} pending={before_p}")

    to_delete: list[dict] = []
    skipped: list[str] = []

    for t in targets:
        sid = int(t["spouse_row_id"])
        row = conn.execute(
            """
            SELECT s.id, s.client_id, s.first_name, s.last_name, s.derived_last_name,
                   s.source, s.confirmed_at_intake, s.taxpayer_name, s.needs_review,
                   c.last_name AS client_ln, c.first_name AS client_fn
            FROM spouses s
            JOIN clients c ON c.id = s.client_id
            WHERE s.id=?
            """,
            (sid,),
        ).fetchone()
        if row is None:
            skipped.append(f"missing spouse id={sid} (client {t['client_id']})")
            continue
        if args.only_test and not _is_test_junk_db_row(row):
            skipped.append(
                f"not TEST junk in DB spouse id={sid} client={row['client_id']} "
                f"(verify CSV said TEST — skipped for safety)"
            )
            continue
        if int(row["confirmed_at_intake"] or 0) == 1:
            skipped.append(
                f"protected confirmed_at_intake spouse id={sid} "
                f"client={row['client_id']}"
            )
            continue
        to_delete.append(
            {
                "spouse_id": sid,
                "client_id": row["client_id"],
                "spouse_name": (
                    f"{(row['first_name'] or '').strip()} "
                    f"{(row['last_name'] or row['derived_last_name'] or '').strip()}"
                ).strip(),
                "source": row["source"] or "",
                "verdict": t.get("verdict") or ("TEST_JUNK" if args.only_test else ""),
                "export_line": t.get("export_line") or "",
                "client_name": t.get("client_name") or "",
            }
        )

    print(f"\nCandidates for DELETE: {len(to_delete)}")
    for d in to_delete:
        print(
            f"  [{d['verdict']}] spouses.id={d['spouse_id']} "
            f"client #{d['client_id']} {d['client_name']} "
            f"| spouse={d['spouse_name']} | src={d['source']} "
            f"| export={d['export_line']!r}"
        )
    if skipped:
        print(f"\nSkipped: {len(skipped)}")
        for s in skipped:
            print(f"  {s}")

    if not args.apply:
        print(
            f"\nDRY-RUN — would delete {len(to_delete)} rows "
            f"(confirmed {before_c} -> {before_c - len(to_delete)}). "
            "Re-run with --apply to execute."
        )
        return

    if not to_delete:
        print("Nothing to delete.")
        return

    ids = [d["spouse_id"] for d in to_delete]
    placeholders = ",".join("?" * len(ids))
    conn.execute(f"DELETE FROM spouses WHERE id IN ({placeholders})", ids)
    conn.commit()

    after_c = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
    ).fetchone()[0]
    after_p = conn.execute(
        "SELECT COUNT(*) FROM spouses WHERE needs_review=1"
    ).fetchone()[0]
    print(
        f"\nAPPLIED — deleted {len(ids)} rows. "
        f"confirmed {before_c} -> {after_c}; pending {before_p} -> {after_p}"
    )


if __name__ == "__main__":
    main()
