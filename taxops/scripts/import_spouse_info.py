"""
import_spouse_info.py
~~~~~~~~~~~~~~~~~~~~~
Import Drake TY2025 MFJ spouse data into the TaxLog ``spouses`` table.

**Sanctioned path only.** Matches the **primary taxpayer** (name before ``&``
in the MFJ line) to a TaxLog client, then stores spouse name/DOB on that
client's ``spouses`` row for intake prefill.

Deprecated (do not revive): matching the Drake *spouse-side* name via
``find_client()`` and attaching the row to that client — that mis-scored
same-surname households (see ``docs/runbooks/spouse-import.md``).

Usage (from T:\\taxops on the workstation / C:\\TaxOps\\taxops on the server):

    python scripts/import_spouse_info.py [PATH_TO_CSV] [--dry-run]
    python scripts/import_spouse_info.py CSVFILES\\TY2024Spouses.csv --combined-format

Outputs:
  scripts/spouse_review.csv   — unmatched / review-band rows
  scripts/spouse_import.log   — audit trail
"""
from __future__ import annotations

import argparse
import csv
import logging
import os
import re
import sqlite3
import sys
from datetime import datetime
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from config import DB_PATH  # noqa: E402
from db import get_connection  # noqa: E402

from name_matcher import (  # noqa: E402
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    _all_clients_cache,
    find_client,
    is_business,
    normalize_name,
    parse_mfj_primary_taxpayer,
    parse_name,
    score_client_names_pair,
)

DEFAULT_CSV = r"f:\DRAKE25\DT\0\B879F122\Documents\spouse.csv"
REVIEW_CSV = os.path.join(_HERE, "spouse_review.csv")
LOG_FILE = os.path.join(_HERE, "spouse_import.log")
CSV_SKIP = 2
SOURCE_TAG = "TY2025 Drake import"
_SUFFIXES = {"JR", "SR", "II", "III", "IV", "ESQ", "MD", "PHD"}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("import_spouse_info")


def _fmt_date(raw: str) -> Optional[str]:
    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def _resolve_derived_last(spouse_last: str, taxpayer_name: str) -> tuple[str, bool]:
    if spouse_last.strip():
        return spouse_last.strip(), False

    name = taxpayer_name.strip()
    if not name:
        return "", True

    words = name.split()
    non_suffix = [w for w in words if w.upper() not in _SUFFIXES]
    if not non_suffix:
        return "", True

    derived = non_suffix[-1]
    ambiguous = False
    if "&" in name:
        after = name[name.index("&") + 1 :].strip().split()
        if len(after) >= 3:
            ambiguous = True
    return derived, ambiguous


def _match_primary_taxpayer(
    name_raw: str,
    conn: sqlite3.Connection,
    cache: list[dict],
) -> Optional[dict]:
    """Match Drake MFJ line to TaxLog client via primary filer name only.

    Do not replace this with spouse-side ``find_client(spouse_last, spouse_first)``
    — that approach is deprecated (wrong-household 90% scores). See runbook.
    """
    tp_last, tp_first = parse_mfj_primary_taxpayer(name_raw)
    if is_business(tp_last, tp_first):
        return None
    if not tp_last and not tp_first:
        return None
    return find_client(conn, tp_last, tp_first or "", cache=cache)


def _client_is_spouse_name(
    client_ln: str,
    client_fn: str,
    sp_first: str,
    sp_last: str,
    derived_last: str,
) -> bool:
    """True when the matched client record is the spouse, not the primary taxpayer."""
    sp_ln = (sp_last or derived_last or "").strip()
    if not sp_first and not sp_ln:
        return False
    return score_client_names_pair(client_ln, client_fn, sp_ln, sp_first) >= ACCEPT_THRESHOLD


def _primary_attachment_score(
    client_ln: str,
    client_fn: str,
    taxpayer_name: str,
) -> int:
    tp_last, tp_first = parse_mfj_primary_taxpayer(taxpayer_name)
    return score_client_names_pair(client_ln, client_fn, tp_last, tp_first)


def _parse_combined_spouse_name(spouse_name: str) -> tuple[str, str, str]:
    """``Spouse Name`` column: ``MARIA HERNANDEZ`` -> first, last, mid."""
    raw = (spouse_name or "").strip()
    if not raw:
        return "", "", ""
    tokens = raw.split()
    if len(tokens) == 1:
        return tokens[0], "", ""
    if len(tokens) == 2:
        return tokens[0], tokens[1], ""
    # First token first name; rest is last (compound surnames)
    return tokens[0], " ".join(tokens[1:]), ""


def _load_csv_rows(csv_path: str, combined_format: bool) -> list[dict]:
    with open(csv_path, newline="", encoding="utf-8-sig") as fh:
        for _ in range(CSV_SKIP):
            fh.readline()
        reader = csv.DictReader(fh)
        raw_rows = list(reader)

    normalized: list[dict] = []
    for row in raw_rows:
        if combined_format:
            tp_name = (row.get("Taxpayer Name") or "").strip()
            sp_name = (row.get("Spouse Name") or "").strip()
            sp_first, sp_last, _ = _parse_combined_spouse_name(sp_name)
            normalized.append(
                {
                    "Taxpayer Name": tp_name,
                    "Spouse ID": "",
                    "Spouse Last Name": sp_last,
                    "Spouse Middle Initial": "",
                    "Spouse First Name": sp_first,
                    "Spouse Date of Birth": (row.get("Spouse Date of Birth") or "").strip(),
                }
            )
        else:
            normalized.append(dict(row))
    return normalized


def run_import(
    csv_path: str,
    *,
    combined_format: bool = False,
    dry_run: bool = False,
    db_path: Optional[str] = None,
) -> None:
    if dry_run:
        log.info("DRY RUN — no database writes")
    log.info("Starting spouse import from %s", csv_path)
    log.info("Database: %s", db_path or DB_PATH)

    if not os.path.exists(csv_path):
        log.error("CSV not found: %s", csv_path)
        sys.exit(1)

    rows = _load_csv_rows(csv_path, combined_format)
    log.info("Loaded %d CSV rows", len(rows))

    conn = get_connection(db_path)

    cache = _all_clients_cache(conn)
    id_to_name = {c["id"]: f"{c['ln']}, {c['fn']}".strip(", ") for c in cache}
    log.info("Loaded %d TaxLog clients for matching", len(cache))

    exact_matches = fuzzy_matches = review_count = derived_used = derived_ambig = 0
    inserted = updated = skipped_self_spouse = 0
    review_rows: list[dict] = []

    for row in rows:
        tp_name_raw = (row.get("Taxpayer Name") or "").strip()
        if not tp_name_raw:
            continue
        if re.match(r"^Totals\s*\(", tp_name_raw, re.IGNORECASE):
            continue

        sp_id_raw = (row.get("Spouse ID") or "").strip()
        sp_last = (row.get("Spouse Last Name") or "").strip()
        sp_mid = (row.get("Spouse Middle Initial") or "").strip()
        sp_first = (row.get("Spouse First Name") or "").strip()
        sp_dob_raw = (row.get("Spouse Date of Birth") or "").strip()

        if not sp_first:
            review_rows.append(
                {
                    "taxpayer_name": tp_name_raw,
                    "spouse_id": sp_id_raw,
                    "sp_last": sp_last,
                    "sp_mid": sp_mid,
                    "sp_first": sp_first,
                    "sp_dob": sp_dob_raw,
                    "matched_client": "",
                    "score": 0,
                    "reason": "no_spouse_first_name",
                }
            )
            review_count += 1
            continue

        sp_dob = _fmt_date(sp_dob_raw)
        derived_last, ambiguous = _resolve_derived_last(sp_last, tp_name_raw)
        if not sp_last.strip():
            derived_used += 1
        if ambiguous:
            derived_ambig += 1

        match = _match_primary_taxpayer(tp_name_raw, conn, cache)
        client_id: Optional[int] = None
        confidence = 0.0
        method = "no_match"
        needs_rev = 1

        if match and match["score"] >= REVIEW_THRESHOLD:
            client_id = match["client_id"]
            confidence = round(match["score"] / 100.0, 3)
            method = "exact" if match["score"] == 100 else "fuzzy"
            c = next(x for x in cache if x["id"] == client_id)
            client_ln, client_fn = c["ln"], c["fn"]

            if _client_is_spouse_name(client_ln, client_fn, sp_first, sp_last, derived_last):
                log.info(
                    "SKIP self-spouse | %s -> client %s (spouse=%s %s)",
                    tp_name_raw,
                    id_to_name.get(client_id, client_id),
                    sp_first,
                    sp_last or derived_last,
                )
                review_rows.append(
                    {
                        "taxpayer_name": tp_name_raw,
                        "spouse_id": sp_id_raw,
                        "sp_last": sp_last,
                        "sp_mid": sp_mid,
                        "sp_first": sp_first,
                        "sp_dob": sp_dob_raw,
                        "matched_client": id_to_name.get(client_id, ""),
                        "score": match["score"],
                        "reason": "self_spouse_match",
                    }
                )
                review_count += 1
                skipped_self_spouse += 1
                continue

            primary_score = _primary_attachment_score(client_ln, client_fn, tp_name_raw)
            if primary_score < ACCEPT_THRESHOLD:
                needs_rev = 1
                method = "review_band"
            elif ambiguous or match["needs_review"]:
                needs_rev = 1
            else:
                needs_rev = 0

            if match["score"] == 100:
                exact_matches += 1
            else:
                fuzzy_matches += 1

        if client_id is None:
            reason = method if method != "no_match" else (
                "below_threshold" if match else "no_match"
            )
            review_rows.append(
                {
                    "taxpayer_name": tp_name_raw,
                    "spouse_id": sp_id_raw,
                    "sp_last": sp_last,
                    "sp_mid": sp_mid,
                    "sp_first": sp_first,
                    "sp_dob": sp_dob_raw,
                    "matched_client": id_to_name.get(match["client_id"], "") if match else "",
                    "score": match["score"] if match else 0,
                    "reason": reason,
                }
            )
            review_count += 1
            log.info(
                "REVIEW %s | %s | score=%.0f | %s",
                reason,
                tp_name_raw,
                match["score"] if match else 0,
                id_to_name.get(match["client_id"], "--") if match else "--",
            )
            continue

        log.info(
            "%s %s | client_id=%-5d | score=%.0f | review=%d | %s -> %s %s [%s]",
            "AMB" if ambiguous else "   ",
            method.upper().ljust(7),
            client_id,
            confidence * 100,
            needs_rev,
            tp_name_raw,
            sp_first,
            derived_last,
            "derived" if not sp_last.strip() else "explicit",
        )

        if dry_run:
            continue

        existing = conn.execute(
            "SELECT id, confirmed_at_intake FROM spouses WHERE client_id=?",
            (client_id,),
        ).fetchone()

        if existing:
            if not existing["confirmed_at_intake"]:
                conn.execute(
                    """
                    UPDATE spouses
                    SET drake_spouse_id=?, taxpayer_name=?, last_name=?, first_name=?,
                        middle_initial=?, date_of_birth=?, derived_last_name=?,
                        match_confidence=?, needs_review=?, source=?
                    WHERE id=?
                    """,
                    (
                        sp_id_raw or None,
                        tp_name_raw,
                        sp_last or None,
                        sp_first,
                        sp_mid or None,
                        sp_dob,
                        derived_last or None,
                        confidence,
                        needs_rev,
                        SOURCE_TAG,
                        existing["id"],
                    ),
                )
                updated += 1
        else:
            conn.execute(
                """
                INSERT INTO spouses
                  (client_id, drake_spouse_id, taxpayer_name, last_name, first_name,
                   middle_initial, date_of_birth, derived_last_name,
                   source, match_confidence, needs_review)
                VALUES (?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    client_id,
                    sp_id_raw or None,
                    tp_name_raw,
                    sp_last or None,
                    sp_first,
                    sp_mid or None,
                    sp_dob,
                    derived_last or None,
                    SOURCE_TAG,
                    confidence,
                    needs_rev,
                ),
            )
            inserted += 1

    if review_rows:
        with open(REVIEW_CSV, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh,
                fieldnames=[
                    "taxpayer_name",
                    "spouse_id",
                    "sp_last",
                    "sp_mid",
                    "sp_first",
                    "sp_dob",
                    "matched_client",
                    "score",
                    "reason",
                ],
            )
            writer.writeheader()
            writer.writerows(review_rows)

    conn.close()

    sep = "=" * 56
    print()
    print(sep)
    print(f"  Spouse import {'(DRY RUN) ' if dry_run else ''}complete")
    print(sep)
    print(f"  Exact matches:             {exact_matches:>5}")
    print(f"  Fuzzy auto-matched:        {fuzzy_matches:>5}")
    print(f"  Self-spouse skipped:       {skipped_self_spouse:>5}")
    print(f"  Review / no match:         {review_count:>5}  (see spouse_review.csv)")
    print(f"  {'-' * 45}")
    total = exact_matches + fuzzy_matches
    print(f"  Total matched:             {total:>5}")
    print(f"  Derived last name used:    {derived_used:>5}")
    print(f"  Ambiguous derivations:     {derived_ambig:>5}")
    if not dry_run:
        print(f"  Rows inserted:             {inserted:>5}")
        print(f"  Rows updated:              {updated:>5}")
    print(sep)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Import Drake MFJ spouse data into TaxLog spouses table"
    )
    parser.add_argument("csv", nargs="?", default=DEFAULT_CSV)
    parser.add_argument(
        "--combined-format",
        action="store_true",
        help="TY2024-style CSV with Taxpayer Name + Spouse Name columns",
    )
    parser.add_argument(
        "--db",
        default=None,
        help="SQLite path (default: TAXOPS_DB env or taxops/taxops.db)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Simulate without writing to the database",
    )
    args = parser.parse_args()
    run_import(
        args.csv,
        combined_format=args.combined_format,
        dry_run=args.dry_run,
        db_path=args.db,
    )
