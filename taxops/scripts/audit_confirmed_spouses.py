"""
audit_confirmed_spouses.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only audit of confirmed (needs_review=0) spouses rows vs Drake MFJ
primary-taxpayer matching (same logic as import_spouse_info.py).

Never writes to the database. Writes a timestamped CSV under reports/
(gitignored — may contain client names).

Usage (workstation):

    python scripts/audit_confirmed_spouses.py --db T:\\taxops\\taxops.db
    python scripts/audit_confirmed_spouses.py --db T:\\taxops\\taxops.db \\
        --csv "f:\\DRAKE25\\DT\\0\\B879F122\\Documents\\spouse.csv"
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from db import get_connection  # noqa: E402
from import_spouse_info import (  # noqa: E402
    DEFAULT_CSV,
    _client_is_spouse_name,
    _load_csv_rows,
    _primary_attachment_score,
    _resolve_derived_last,
)
from name_matcher import (  # noqa: E402
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    normalize_name,
    parse_mfj_primary_taxpayer,
)


def _spouse_key(first: str, last: str) -> str:
    return f"{normalize_name(first)}|{normalize_name(last)}"


def _build_drake_indexes(csv_path: str, combined_format: bool) -> tuple[dict, dict]:
    """
    Returns:
      by_taxpayer: normalized Taxpayer Name → row
      by_spouse: spouse key → list of (taxpayer_name, sp_first, sp_last)
    """
    by_taxpayer: dict[str, dict] = {}
    by_spouse: dict[str, list] = {}
    for row in _load_csv_rows(csv_path, combined_format):
        tp = (row.get("Taxpayer Name") or "").strip()
        if not tp or tp.upper().startswith("TOTALS"):
            continue
        sp_first = (row.get("Spouse First Name") or "").strip()
        sp_last = (row.get("Spouse Last Name") or "").strip()
        derived, _ = _resolve_derived_last(sp_last, tp)
        sp_ln = sp_last or derived
        by_taxpayer[normalize_name(tp)] = {
            "taxpayer_name": tp,
            "sp_first": sp_first,
            "sp_last": sp_ln,
        }
        if sp_first:
            key = _spouse_key(sp_first, sp_ln)
            by_spouse.setdefault(key, []).append(tp)
    return by_taxpayer, by_spouse


def _classify(
    client_ln: str,
    client_fn: str,
    sp_first: str,
    sp_last: str,
    taxpayer_name: Optional[str],
) -> tuple[str, str, str, int, str]:
    """
    Returns (primary_last, primary_first, match_score_str, score_int, flag).
    """
    from name_matcher import _first_token

    if not (taxpayer_name or "").strip():
        return "", "", "", 0, "UNCERTAIN"

    tp_last, tp_first = parse_mfj_primary_taxpayer(taxpayer_name)
    score = _primary_attachment_score(client_ln, client_fn, taxpayer_name)
    self_spouse = _client_is_spouse_name(
        client_ln, client_fn, sp_first, sp_last or "", ""
    )
    # Wrong-household pattern from original bug: client IS the spouse name,
    # not the Drake primary.
    if self_spouse and score < ACCEPT_THRESHOLD:
        flag = "MISMATCH"
    elif score >= ACCEPT_THRESHOLD:
        flag = "MATCH"
    elif score < REVIEW_THRESHOLD:
        flag = "MISMATCH"
    elif _first_token(client_fn or "") != _first_token(tp_first or ""):
        # Same-surname band but different first name (CARDONA/HECTOR vs PEDRO)
        flag = "MISMATCH"
    else:
        flag = "UNCERTAIN"

    return tp_last or "", tp_first or "", str(score), score, flag


def _table_fingerprint(conn) -> tuple[int, str]:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n,
               COALESCE(SUM(id), 0) AS id_sum,
               COALESCE(SUM(client_id), 0) AS cid_sum,
               COALESCE(SUM(needs_review), 0) AS nr_sum
        FROM spouses
        """
    ).fetchone()
    payload = f"{row['n']}|{row['id_sum']}|{row['cid_sum']}|{row['nr_sum']}"
    return int(row["n"]), hashlib.sha256(payload.encode()).hexdigest()[:16]


def run_audit(
    *,
    db_path: Optional[str],
    csv_path: str,
    combined_format: bool,
    out_dir: Path,
) -> Path:
    conn = get_connection(db_path)
    before_n, before_fp = _table_fingerprint(conn)

    by_taxpayer, by_spouse = _build_drake_indexes(csv_path, combined_format)

    rows = conn.execute(
        """
        SELECT s.id, s.client_id, s.taxpayer_name, s.first_name, s.last_name,
               s.derived_last_name, s.match_confidence, s.source,
               c.last_name AS cln, c.first_name AS cfn
        FROM spouses s
        JOIN clients c ON c.id = s.client_id
        WHERE s.needs_review = 0
        ORDER BY c.last_name, c.first_name
        """
    ).fetchall()

    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = out_dir / f"spouse_audit_{stamp}.csv"

    counts = {"MATCH": 0, "MISMATCH": 0, "UNCERTAIN": 0}
    fieldnames = [
        "client_id",
        "stored_spouse_first",
        "stored_spouse_last",
        "drake_parsed_primary_last",
        "drake_parsed_primary_first",
        "match_score",
        "flag",
        # Extra context for operators (not required by acceptance; helpful)
        "client_last",
        "client_first",
        "taxpayer_name_used",
        "source",
    ]

    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=fieldnames)
        writer.writeheader()

        for r in rows:
            sp_first = (r["first_name"] or "").strip()
            sp_last = (r["last_name"] or r["derived_last_name"] or "").strip()
            tp = (r["taxpayer_name"] or "").strip() or None

            if not tp:
                # Resolve Drake line via spouse name when taxpayer_name blank
                hits = by_spouse.get(_spouse_key(sp_first, sp_last), [])
                if len(hits) == 1:
                    tp = hits[0]
                elif len(hits) > 1:
                    # Ambiguous spouse name across multiple MFJ lines
                    counts["UNCERTAIN"] += 1
                    writer.writerow(
                        {
                            "client_id": r["client_id"],
                            "stored_spouse_first": sp_first,
                            "stored_spouse_last": sp_last,
                            "drake_parsed_primary_last": "",
                            "drake_parsed_primary_first": "",
                            "match_score": "",
                            "flag": "UNCERTAIN",
                            "client_last": r["cln"],
                            "client_first": r["cfn"],
                            "taxpayer_name_used": "",
                            "source": r["source"],
                        }
                    )
                    continue

            pl, pf, score_s, _score, flag = _classify(
                r["cln"] or "", r["cfn"] or "", sp_first, sp_last, tp
            )
            counts[flag] += 1
            writer.writerow(
                {
                    "client_id": r["client_id"],
                    "stored_spouse_first": sp_first,
                    "stored_spouse_last": sp_last,
                    "drake_parsed_primary_last": pl,
                    "drake_parsed_primary_first": pf,
                    "match_score": score_s,
                    "flag": flag,
                    "client_last": r["cln"],
                    "client_first": r["cfn"],
                    "taxpayer_name_used": tp or "",
                    "source": r["source"],
                }
            )

    after_n, after_fp = _table_fingerprint(conn)
    conn.close()

    if before_n != after_n or before_fp != after_fp:
        raise RuntimeError(
            f"spouses table changed during audit "
            f"(before n={before_n} fp={before_fp}; after n={after_n} fp={after_fp})"
        )

    total = sum(counts.values())
    print("=== Confirmed spouse primary-match audit (READ-ONLY) ===")
    print(f"Confirmed rows:  {total}")
    print(f"  MATCH:         {counts['MATCH']}")
    print(f"  MISMATCH:      {counts['MISMATCH']}")
    print(f"  UNCERTAIN:     {counts['UNCERTAIN']}")
    print(f"CSV:             {out_path}")
    print(f"spouses checksum unchanged: n={before_n} fp={before_fp}")
    return out_path


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Read-only audit of confirmed spouses vs Drake primary"
    )
    parser.add_argument("--db", default=None, help="SQLite path (workstation: T:\\taxops\\taxops.db)")
    parser.add_argument("--csv", default=DEFAULT_CSV, help="Drake purple-sheet spouse CSV")
    parser.add_argument(
        "--combined-format",
        action="store_true",
        help="TY2024-style Taxpayer Name + Spouse Name columns",
    )
    parser.add_argument(
        "--out-dir",
        default=os.path.join(_TAXOPS, "reports"),
        help="Directory for timestamped CSV (default: taxops/reports)",
    )
    args = parser.parse_args()

    if not os.path.exists(args.csv):
        print(f"ERROR: Drake CSV not found: {args.csv}", file=sys.stderr)
        sys.exit(1)

    run_audit(
        db_path=args.db,
        csv_path=args.csv,
        combined_format=args.combined_format,
        out_dir=Path(args.out_dir),
    )


if __name__ == "__main__":
    main()
