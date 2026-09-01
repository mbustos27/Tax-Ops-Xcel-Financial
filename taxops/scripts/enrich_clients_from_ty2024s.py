"""Enrich TaxOps clients from TY2024 spouse-info CSV (Desktop TY2024S.csv).

Same pattern as Rudy & Claire Sandoval:
  - Match primary taxpayer (name before &) via find_client / parse_mfj_primary_taxpayer
  - Fill blank client fields: taxpayer_dob, taxpayer_cell, spouse_* 
  - Upsert spouses row when Spouse Name is present

Does NOT use deprecated spouse-side matching.
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
    parse_mfj_primary_taxpayer,
    score_client_names_pair,
)
from utils import now

DEFAULT_CSV = Path(r"C:\Users\Windows 10\Desktop\TY2024S.csv")
SOURCE = "TY2024S.csv client enrich"
OUT = _TAXOPS / "data" / "processed" / "ty2024s_client_enrich.json"


def _fmt_date(raw: str) -> str | None:
    from datetime import datetime

    raw = (raw or "").strip()
    if not raw:
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def _digits_phone(raw: str) -> str | None:
    d = "".join(c for c in (raw or "") if c.isdigit())
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return d if len(d) == 10 else (d or None)


def _parse_spouse_name(spouse_name: str) -> tuple[str | None, str | None]:
    tokens = (spouse_name or "").strip().split()
    if not tokens:
        return None, None
    if len(tokens) == 1:
        return tokens[0], None
    return tokens[0], " ".join(tokens[1:])


def _load_unique_taxpayers(csv_path: Path) -> list[dict]:
    """One row per Taxpayer Name (file repeats for dependents)."""
    with csv_path.open(newline="", encoding="utf-8-sig") as fh:
        fh.readline()  # title
        fh.readline()  # as-of
        reader = csv.DictReader(fh)
        by_name: dict[str, dict] = {}
        for row in reader:
            name = (row.get("Taxpayer Name") or "").strip()
            if not name or re.match(r"^Totals\s*\(", name, re.I):
                continue
            incoming = {
                "taxpayer_name": name,
                "taxpayer_dob": _fmt_date(row.get("Taxpayer Date of Birth") or ""),
                "taxpayer_cell": _digits_phone(row.get("Taxpayer Cell Phone") or ""),
                "spouse_name": (row.get("Spouse Name") or "").strip() or None,
                "spouse_cell": _digits_phone(row.get("Spouse Daytime Phone") or ""),
                "spouse_dob": _fmt_date(row.get("Spouse Date of Birth") or ""),
            }
            if name not in by_name:
                by_name[name] = incoming
                continue
            cur = by_name[name]
            for k, v in incoming.items():
                if k == "taxpayer_name":
                    continue
                if not cur.get(k) and v:
                    cur[k] = v
    return list(by_name.values())


def _match_client(conn, cache, taxpayer_name: str) -> dict | None:
    tp_last, tp_first = parse_mfj_primary_taxpayer(taxpayer_name)
    if is_business(tp_last, tp_first):
        return {"skip": "business", "tp_last": tp_last, "tp_first": tp_first}
    if not tp_last and not tp_first:
        return {"skip": "empty_name"}
    match = find_client(conn, tp_last, tp_first or "", cache=cache)
    if not match or match["score"] < REVIEW_THRESHOLD:
        return {
            "skip": "no_match",
            "tp_last": tp_last,
            "tp_first": tp_first,
            "score": match["score"] if match else 0,
        }
    return {
        "client_id": match["client_id"],
        "score": match["score"],
        "needs_review": match["needs_review"] or match["score"] < ACCEPT_THRESHOLD,
        "tp_last": tp_last,
        "tp_first": tp_first,
        "method": match["method"],
    }


def _fill(conn, client_id: int, updates: dict) -> list[str]:
    """Apply only non-empty incoming values onto blank client columns. Returns changed keys."""
    row = conn.execute("SELECT * FROM clients WHERE id=?", (client_id,)).fetchone()
    if not row:
        return []
    existing = dict(row)
    changed: list[str] = []
    sets: list[str] = []
    vals: list = []
    for col, val in updates.items():
        if val is None or val == "":
            continue
        cur = existing.get(col)
        if cur is None or str(cur).strip() == "":
            sets.append(f"{col}=?")
            vals.append(val)
            changed.append(col)
    if not changed:
        return []
    sets.append("updated_at=?")
    vals.append(now())
    vals.append(client_id)
    conn.execute(f"UPDATE clients SET {', '.join(sets)} WHERE id=?", vals)
    return changed


def _upsert_spouse(
    conn,
    client_id: int,
    *,
    first: str,
    last: str | None,
    dob: str | None,
    taxpayer_name: str,
) -> str:
    # Schema enforces one spouses row per client_id.
    existing = conn.execute(
        "SELECT id, first_name, last_name, date_of_birth FROM spouses WHERE client_id=? LIMIT 1",
        (client_id,),
    ).fetchone()
    if existing:
        conn.execute(
            """
            UPDATE spouses SET
              first_name = COALESCE(NULLIF(first_name,''), ?),
              last_name = COALESCE(NULLIF(last_name,''), ?),
              date_of_birth = COALESCE(NULLIF(date_of_birth,''), ?),
              source = ?,
              needs_review = 0,
              taxpayer_name = COALESCE(NULLIF(taxpayer_name,''), ?)
            WHERE id=?
            """,
            (first, last, dob, SOURCE, taxpayer_name, existing["id"]),
        )
        return "updated"
    conn.execute(
        """
        INSERT INTO spouses (
          client_id, last_name, first_name, date_of_birth,
          source, match_confidence, needs_review, confirmed_at_intake,
          created_at, taxpayer_name
        ) VALUES (?,?,?,?,?,1.0,0,0,?,?)
        """,
        (client_id, last, first, dob, SOURCE, now(), taxpayer_name),
    )
    return "inserted"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", type=Path, default=DEFAULT_CSV)
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--accept-only", action="store_true", help="Only score>=88 matches")
    args = ap.parse_args()

    taxpayers = _load_unique_taxpayers(args.csv)
    conn = get_connection()
    cache = _all_clients_cache(conn)
    counts: Counter = Counter()
    samples: list[dict] = []
    mismatches: list[dict] = []

    try:
        if args.apply:
            conn.execute("BEGIN")

        for row in taxpayers:
            counts["csv_taxpayers"] += 1
            m = _match_client(conn, cache, row["taxpayer_name"])
            if m.get("skip"):
                counts[f"skip_{m['skip']}"] += 1
                if m["skip"] in {"no_match"} and len(mismatches) < 40:
                    mismatches.append({"name": row["taxpayer_name"], **m})
                continue

            if args.accept_only and m.get("needs_review"):
                counts["skip_review_band"] += 1
                continue

            client_id = int(m["client_id"])
            counts["matched"] += 1
            if m["score"] == 100:
                counts["exact"] += 1
            else:
                counts["fuzzy"] += 1

            sp_first, sp_last = _parse_spouse_name(row.get("spouse_name") or "")
            # Guard: don't treat matched client as the spouse
            crow = next(x for x in cache if x["id"] == client_id)
            if sp_first and score_client_names_pair(
                crow["ln"], crow["fn"], sp_last or "", sp_first
            ) >= ACCEPT_THRESHOLD:
                counts["skip_self_spouse"] += 1
                sp_first, sp_last = None, None

            updates = {
                "taxpayer_dob": row.get("taxpayer_dob"),
                "taxpayer_cell": row.get("taxpayer_cell"),
            }
            if sp_first:
                updates["spouse_first_name"] = sp_first
                updates["spouse_last_name"] = sp_last or m.get("tp_last")
                updates["spouse_dob"] = row.get("spouse_dob")
                updates["spouse_cell"] = row.get("spouse_cell")

            if args.apply:
                changed = _fill(conn, client_id, updates)
                if changed:
                    counts["clients_updated"] += 1
                    counts["fields_filled"] += len(changed)
                else:
                    counts["clients_noop"] += 1

                if sp_first:
                    action = _upsert_spouse(
                        conn,
                        client_id,
                        first=sp_first,
                        last=sp_last or m.get("tp_last"),
                        dob=row.get("spouse_dob"),
                        taxpayer_name=row["taxpayer_name"],
                    )
                    counts[f"spouse_{action}"] += 1
            else:
                # dry-run: estimate fills
                existing = dict(
                    conn.execute(
                        """
                        SELECT taxpayer_dob, taxpayer_cell,
                               spouse_first_name, spouse_last_name, spouse_dob, spouse_cell
                        FROM clients WHERE id=?
                        """,
                        (client_id,),
                    ).fetchone()
                )
                would = []
                for col, val in updates.items():
                    if not val:
                        continue
                    existing_val = existing.get(col)
                    if existing_val is None or str(existing_val).strip() == "":
                        would.append(col)
                if would:
                    counts["clients_would_update"] += 1
                    counts["fields_would_fill"] += len(would)
                else:
                    counts["clients_noop"] += 1
                if sp_first:
                    counts["spouse_would_upsert"] += 1

            if len(samples) < 15 and (sp_first or row.get("taxpayer_dob")):
                samples.append(
                    {
                        "csv": row["taxpayer_name"],
                        "client_id": client_id,
                        "score": m["score"],
                        "dob": row.get("taxpayer_dob"),
                        "spouse": row.get("spouse_name"),
                    }
                )

        if args.apply:
            conn.commit()

        payload = {
            "csv": str(args.csv),
            "apply": args.apply,
            "counts": dict(counts),
            "samples": samples,
            "no_match_sample": mismatches,
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
