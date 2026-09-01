"""Populate client_dependents from TY2024S.csv (Desktop spouse/dependent export).

Uses the same primary-taxpayer matching as enrich_clients_from_ty2024s.py
(parse_mfj_primary_taxpayer + find_client). Dedupes by synthetic
drake_dependent_id = ty2024s:<FIRST>:<LAST> so re-runs are idempotent.
"""

from __future__ import annotations

import argparse
import csv
import json
import re
import sys
from collections import Counter
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from db import get_connection
from name_matcher import (
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    _all_clients_cache,
    find_client,
    is_business,
    normalize_name,
    parse_mfj_primary_taxpayer,
    score_client_names_pair,
)
from utils import now

DEFAULT_CSV = Path(r"C:\Users\Windows 10\Desktop\TY2024S.csv")
SOURCE = "TY2024S.csv dependent enrich"
OUT = _TAXOPS / "data" / "processed" / "ty2024s_dependent_enrich.json"


def _dep_key(first: str, last: str | None) -> str:
    f = normalize_name(first or "")
    l = normalize_name(last or "")
    return f"ty2024s:{f}:{l}"


def _parse_spouse_name(spouse_name: str) -> tuple[str | None, str | None]:
    tokens = (spouse_name or "").strip().split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return tokens[0], None
    return tokens[0], " ".join(tokens[1:])


def _load_dependent_rows(csv_path: Path) -> list[dict]:
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        fh.readline()
        fh.readline()
        reader = csv.DictReader(fh)
        out: list[dict] = []
        seen: set[tuple[str, str, str]] = set()
        for row in reader:
            tp = (row.get("Taxpayer Name") or "").strip()
            df = (row.get("Dependent First Name") or "").strip()
            dl = (row.get("Dependent Last Name") or "").strip() or None
            if not tp or not df:
                continue
            if re.match(r"^Totals\s*\(", tp, re.I):
                continue
            key = (tp.upper(), df.upper(), (dl or "").upper())
            if key in seen:
                continue
            seen.add(key)
            out.append(
                {
                    "taxpayer_name": tp,
                    "dep_first": df,
                    "dep_last": dl,
                    "spouse_name": (row.get("Spouse Name") or "").strip() or None,
                }
            )
    return out


def _match_client(conn, cache, taxpayer_name: str) -> dict:
    tp_last, tp_first = parse_mfj_primary_taxpayer(taxpayer_name)
    if is_business(tp_last, tp_first):
        return {"skip": "business"}
    if not tp_last and not tp_first:
        return {"skip": "empty_name"}
    match = find_client(conn, tp_last, tp_first or "", cache=cache)
    if not match or match["score"] < REVIEW_THRESHOLD:
        return {"skip": "no_match", "score": match["score"] if match else 0}
    return {
        "client_id": match["client_id"],
        "score": match["score"],
        "needs_review": bool(match["needs_review"] or match["score"] < ACCEPT_THRESHOLD),
        "tp_last": tp_last,
        "tp_first": tp_first,
    }


def _is_self_or_spouse(dep_first: str, dep_last: str | None, row: dict, m: dict, crow: dict) -> str | None:
    """Skip dependents that are clearly the taxpayer or spouse repeating."""
    # Taxpayer first-name self-row (e.g. BENIGNA / BENIGNA)
    if score_client_names_pair(
        crow["ln"], crow["fn"], dep_last or m.get("tp_last") or "", dep_first
    ) >= ACCEPT_THRESHOLD:
        return "self_taxpayer"
    sp_first, sp_last = _parse_spouse_name(row.get("spouse_name") or "")
    if sp_first and score_client_names_pair(
        sp_last or m.get("tp_last") or "", sp_first, dep_last or sp_last or "", dep_first
    ) >= ACCEPT_THRESHOLD:
        return "self_spouse"
    return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    deps = _load_dependent_rows(args.csv)
    conn = get_connection()
    cache = _all_clients_cache(conn)
    counts: Counter = Counter()
    samples: list[dict] = []
    no_match: list[dict] = []

    try:
        if args.apply:
            conn.execute("BEGIN")

        for row in deps:
            counts["csv_deps"] += 1
            m = _match_client(conn, cache, row["taxpayer_name"])
            if m.get("skip"):
                counts[f"skip_{m['skip']}"] += 1
                if m["skip"] == "no_match" and len(no_match) < 30:
                    no_match.append({"taxpayer": row["taxpayer_name"], "dep": row["dep_first"]})
                continue

            client_id = int(m["client_id"])
            crow = next(x for x in cache if x["id"] == client_id)
            reason = _is_self_or_spouse(row["dep_first"], row["dep_last"], row, m, crow)
            if reason:
                counts[f"skip_{reason}"] += 1
                continue

            counts["matched"] += 1
            key = _dep_key(row["dep_first"], row["dep_last"])
            # Default last name to taxpayer last when blank (common in this CSV)
            dep_last = row["dep_last"] or m.get("tp_last")
            needs_rev = 1 if m.get("needs_review") else 0

            existing = conn.execute(
                """
                SELECT id FROM client_dependents
                WHERE client_id=? AND drake_dependent_id=?
                """,
                (client_id, key),
            ).fetchone()
            if existing is None:
                # Also catch prior rows without synthetic id (same names)
                existing = conn.execute(
                    """
                    SELECT id FROM client_dependents
                    WHERE client_id=?
                      AND upper(first_name)=upper(?)
                      AND upper(COALESCE(last_name,''))=upper(COALESCE(?,''))
                      AND removed_for_ty2026=0
                    LIMIT 1
                    """,
                    (client_id, row["dep_first"], dep_last or ""),
                ).fetchone()

            if existing:
                if args.apply:
                    conn.execute(
                        """
                        UPDATE client_dependents SET
                          last_name = COALESCE(NULLIF(last_name,''), ?),
                          first_name = ?,
                          drake_dependent_id = COALESCE(drake_dependent_id, ?),
                          source = ?,
                          taxpayer_name = COALESCE(NULLIF(taxpayer_name,''), ?),
                          match_confidence = ?,
                          needs_review = CASE WHEN needs_review=1 THEN 1 ELSE ? END
                        WHERE id=?
                        """,
                        (
                            dep_last,
                            row["dep_first"],
                            key,
                            SOURCE,
                            row["taxpayer_name"],
                            round(m["score"] / 100.0, 3),
                            needs_rev,
                            existing["id"],
                        ),
                    )
                counts["updated"] += 1
                action = "updated"
            else:
                if args.apply:
                    conn.execute(
                        """
                        INSERT INTO client_dependents (
                          client_id, drake_dependent_id, taxpayer_name,
                          last_name, first_name, source, match_confidence,
                          needs_review, confirmed_at_intake, removed_for_ty2026,
                          created_at, is_claimed_dependent
                        ) VALUES (?,?,?,?,?,?,?,?,0,0,?,1)
                        """,
                        (
                            client_id,
                            key,
                            row["taxpayer_name"],
                            dep_last,
                            row["dep_first"],
                            SOURCE,
                            round(m["score"] / 100.0, 3),
                            needs_rev,
                            now(),
                        ),
                    )
                counts["inserted"] += 1
                action = "inserted"

            if len(samples) < 12:
                samples.append(
                    {
                        "taxpayer": row["taxpayer_name"],
                        "client_id": client_id,
                        "dep": f"{row['dep_first']} {dep_last or ''}".strip(),
                        "score": m["score"],
                        "action": action,
                    }
                )

        if args.apply:
            conn.commit()

        payload = {
            "csv": str(args.csv),
            "apply": args.apply,
            "counts": dict(counts),
            "samples": samples,
            "no_match_sample": no_match,
        }
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(json.dumps(payload["counts"], indent=2))
        print(f"Wrote {OUT}")
        if not args.apply:
            print("Re-run with --apply to commit.")
    except Exception:
        if args.apply:
            conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    main()
