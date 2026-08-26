"""
verify_spouses_vs_export.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~
Read-only: every confirmed spouses row vs Drake spouse exports, and check
whether the stored spouse identity is actually another TaxLog *client*
(primary profile) rather than a legitimate MFJ spouse on this client.

Never writes the DB. Writes CSV under reports/ (gitignored).

Usage:
    python scripts/verify_spouses_vs_export.py --db T:\\taxops\\taxops.db
"""
from __future__ import annotations

import argparse
import csv
import os
import re
import sqlite3
import sys
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Optional

_HERE = os.path.dirname(os.path.abspath(__file__))
_TAXOPS = os.path.dirname(_HERE)
sys.path.insert(0, _TAXOPS)

from db import get_connection  # noqa: E402
from import_spouse_info import _load_csv_rows, _resolve_derived_last  # noqa: E402
from name_matcher import (  # noqa: E402
    ACCEPT_THRESHOLD,
    normalize_name,
    parse_mfj_primary_taxpayer,
    score_client_names_pair,
    _all_clients_cache,
)

DEFAULT_EXPORTS = [
    (r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv", False),
    (r"T:\taxops\CSVFILES\TY2024Spouses.csv", True),
]


def _norm_dob(s: Optional[str]) -> str:
    if not s:
        return ""
    s = s.strip().replace(".", "-").replace("/", "-")
    parts = s.split("-")
    if len(parts) != 3:
        return s
    if len(parts[0]) == 4:
        return f"{parts[0]}-{parts[1].zfill(2)}-{parts[2].zfill(2)}"
    return f"{parts[2]}-{parts[0].zfill(2)}-{parts[1].zfill(2)}"


def _name_key(first: str, last: str) -> str:
    return f"{normalize_name(first)}|{normalize_name(last)}"


def _split_spouse_display(spouse_name: str) -> tuple[str, str]:
    """'GUADALUPE DURAN' / 'NOSHEEN KHOKHAR' -> (first, last)."""
    parts = (spouse_name or "").strip().split()
    if not parts:
        return "", ""
    if len(parts) == 1:
        return parts[0], ""
    return " ".join(parts[:-1]), parts[-1]


def load_exports(paths: list[tuple[str, bool]]) -> tuple[list[dict], dict]:
    """
    Return flat export rows + index by spouse key.

    Each row: taxpayer_name, primary_last, primary_first, primary_dob,
              spouse_first, spouse_last, spouse_dob, source_file
    """
    rows: list[dict] = []
    by_spouse: dict[str, list[dict]] = defaultdict(list)

    for path, combined in paths:
        if not os.path.isfile(path):
            print(f"WARN skip missing export: {path}")
            continue
        for raw in _load_csv_rows(path, combined):
            # Combined (TY2024): Taxpayer Name + Spouse First/Last
            # Split (TY2025): Taxpayer First/Last + Spouse Name (display)
            tp = (raw.get("Taxpayer Name") or "").strip()
            if not tp:
                tf = (raw.get("Taxpayer First Name") or "").strip()
                tl = (raw.get("Taxpayer Last Name") or "").strip()
                if not tl or tl.upper().startswith("TOTAL"):
                    continue
                tp = f"{tf} {tl}".strip()
                pl, pf = tl, tf
            else:
                if tp.upper().startswith("TOTAL"):
                    continue
                pl, pf = parse_mfj_primary_taxpayer(tp)

            sp_first = (raw.get("Spouse First Name") or "").strip()
            sp_last = (raw.get("Spouse Last Name") or "").strip()
            if not sp_first and raw.get("Spouse Name"):
                sp_first, sp_last = _split_spouse_display(raw["Spouse Name"])
            derived, _ = _resolve_derived_last(sp_last, tp)
            sp_ln = sp_last or derived
            if not sp_first:
                continue
            # Prefer MFJ combined line when present; for split build synthetic
            if "&" not in tp and sp_first:
                display_tp = f"{pf} {pl} & {sp_first} {sp_ln}".strip()
            else:
                display_tp = tp
            rec = {
                "taxpayer_name": display_tp,
                "primary_last": pl,
                "primary_first": pf,
                "primary_dob": _norm_dob(
                    raw.get("Taxpayer Date of Birth")
                    or raw.get("Taxpayer DOB")
                    or ""
                ),
                "spouse_first": sp_first,
                "spouse_last": sp_ln,
                "spouse_dob": _norm_dob(
                    raw.get("Spouse Date of Birth") or ""
                ),
                "source_file": os.path.basename(path),
            }
            rows.append(rec)
            by_spouse[_name_key(sp_first, sp_ln)].append(rec)
    return rows, by_spouse


def find_export_pair(
    by_spouse: dict,
    export_rows: list[dict],
    *,
    spouse_first: str,
    spouse_last: str,
    client_ln: str,
    client_fn: str,
    taxpayer_name: str,
    client_dob: str,
) -> tuple[Optional[dict], str]:
    """Best export MFJ line for this spouses row."""
    tp_norm = normalize_name(taxpayer_name) if taxpayer_name else ""

    # 1) Exact Drake MFJ line if stored
    if tp_norm:
        for e in export_rows:
            if normalize_name(e["taxpayer_name"]) == tp_norm:
                return e, "exact_taxpayer_name"

    # 2) Spouse key + primary matches this client
    hits = by_spouse.get(_name_key(spouse_first, spouse_last), [])
    cd = _norm_dob(client_dob)
    scored: list[tuple[int, dict, str]] = []
    for e in hits:
        score = score_client_names_pair(
            client_ln, client_fn, e["primary_last"], e["primary_first"]
        )
        why = "spouse_key+primary_name"
        if cd and e["primary_dob"] and cd == e["primary_dob"]:
            score = max(score, 100)
            why = "spouse_key+primary_dob"
        scored.append((score, e, why))
    scored.sort(key=lambda x: -x[0])
    if scored and scored[0][0] >= ACCEPT_THRESHOLD:
        return scored[0][1], scored[0][2]
    if scored:
        return scored[0][1], f"weak_spouse_key_{scored[0][0]}"

    # 3) Client DOB + spouse first token (wave4 folds often store spouse as "PAMELA A")
    cd = _norm_dob(client_dob)
    sf0 = normalize_name(spouse_first).split()[0] if spouse_first else ""
    if cd and sf0:
        for e in export_rows:
            if e["primary_dob"] != cd:
                continue
            if score_client_names_pair(
                client_ln, client_fn, e["primary_last"], e["primary_first"]
            ) < ACCEPT_THRESHOLD:
                continue
            exp_sf0 = normalize_name(e["spouse_first"]).split()[0]
            if exp_sf0 == sf0:
                return e, "client_dob+spouse_first"

    return None, "no_export_hit"


def other_clients_matching_spouse(
    cache: list,
    *,
    spouse_first: str,
    spouse_last: str,
    exclude_client_id: int,
) -> list[dict]:
    """TaxLog clients whose *primary* name matches the spouse identity."""
    out = []
    for c in cache:
        cid = c["id"]
        ln = c["ln"] or ""
        fn = c["fn"] or ""
        if cid == exclude_client_id:
            continue
        if score_client_names_pair(spouse_last, spouse_first, ln, fn) >= ACCEPT_THRESHOLD:
            out.append({"id": cid, "last_name": ln, "first_name": fn})
    return out


def _same_household_hint(
    client_ln: str,
    client_fn: str,
    export_hit: dict,
) -> bool:
    """
    True when profile looks like the same MFJ household as the export line
    (compound last / joint first), even if find_client primary parse differs.
    """
    hay = normalize_name(f"{client_fn} {client_ln}")
    pf = normalize_name(export_hit["primary_first"])
    pl = normalize_name(export_hit["primary_last"])
    sf = normalize_name(export_hit["spouse_first"])
    sl = normalize_name(export_hit["spouse_last"] or "")
    if not pf or not sf:
        return False
    # Primary first + spouse first both appear on the client name
    if pf.split()[0] in hay and sf.split()[0] in hay:
        return True
    # Client last contains export primary last tokens (PADILLA GARCIA / PADILLA)
    if pl and pl in normalize_name(client_ln):
        if pf.split()[0] in hay:
            return True
    # DE LA CRUZ vs CRUZ
    if pl and normalize_name(client_ln).endswith(pl) and pf.split()[0] in hay:
        return True
    if sl and sl in hay and pf.split()[0] in hay and sf.split()[0] in hay:
        return True
    return False


def classify_row(
    *,
    client_id: int,
    client_ln: str,
    client_fn: str,
    client_dob: str,
    spouse_first: str,
    spouse_last: str,
    taxpayer_name: str,
    source: str,
    export_hit: Optional[dict],
    export_how: str,
    other_as_clients: list[dict],
) -> str:
    """
    Verdict taxonomy:

    OK_EXPORT — export MFJ primary matches this client.
    OK_EXPORT_SPOUSE_ALSO_CLIENT — same, and spouse also has a client profile.
    OK_EXPORT_NAME_VARIANT — export MFJ line is this household; client name
        is a joint/compound variant of the same couple (not a different person).
    WRONG_ATTACH — export MFJ primary is a different person than this client.
    SPOUSE_IS_OTHER_CLIENT_NO_EXPORT — spouse name = another client's primary,
        and no export MFJ pair backs this attach.
    NO_EXPORT — cannot verify.
    """
    if not spouse_first:
        return "EMPTY_SPOUSE"

    if export_hit:
        prim_score = score_client_names_pair(
            client_ln,
            client_fn,
            export_hit["primary_last"],
            export_hit["primary_first"],
        )
        cd = _norm_dob(client_dob)
        dob_ok = bool(cd and export_hit["primary_dob"] and cd == export_hit["primary_dob"])
        dob_bad = bool(cd and export_hit["primary_dob"] and cd != export_hit["primary_dob"])

        same_hh = _same_household_hint(client_ln, client_fn, export_hit)

        if dob_bad and not same_hh and prim_score < ACCEPT_THRESHOLD:
            return "WRONG_ATTACH"
        if prim_score >= ACCEPT_THRESHOLD or dob_ok:
            if other_as_clients:
                return "OK_EXPORT_SPOUSE_ALSO_CLIENT"
            return "OK_EXPORT"
        if same_hh and not dob_bad:
            return "OK_EXPORT_NAME_VARIANT"
        if export_how.startswith("weak") or prim_score < ACCEPT_THRESHOLD:
            return "WRONG_ATTACH"

    if other_as_clients:
        return "SPOUSE_IS_OTHER_CLIENT_NO_EXPORT"
    return "NO_EXPORT"


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--db", default=None)
    ap.add_argument(
        "--export",
        action="append",
        nargs=2,
        metavar=("PATH", "combined|split"),
        help="Export path + format (combined or split). Repeatable.",
    )
    args = ap.parse_args()

    exports: list[tuple[str, bool]] = []
    if args.export:
        for path, fmt in args.export:
            exports.append((path, fmt.lower().startswith("comb")))
    else:
        exports = list(DEFAULT_EXPORTS)

    conn = get_connection(args.db) if args.db else get_connection()
    conn.row_factory = sqlite3.Row
    cache = _all_clients_cache(conn)

    export_rows, by_spouse = load_exports(exports)
    print(f"Export MFJ-with-spouse rows: {len(export_rows)}")

    spouses = conn.execute(
        """
        SELECT s.id AS spouse_row_id, s.client_id, s.first_name, s.last_name,
               s.derived_last_name, s.date_of_birth, s.taxpayer_name, s.source,
               c.last_name AS client_ln, c.first_name AS client_fn,
               c.taxpayer_dob AS client_dob
        FROM spouses s
        JOIN clients c ON c.id = s.client_id
        WHERE s.needs_review = 0
        ORDER BY s.client_id
        """
    ).fetchall()
    print(f"Confirmed spouses rows: {len(spouses)}")

    out_rows = []
    counts: dict[str, int] = defaultdict(int)

    for s in spouses:
        sp_first = (s["first_name"] or "").strip()
        sp_last = (s["last_name"] or s["derived_last_name"] or "").strip()
        if not sp_last and s["taxpayer_name"]:
            der, _amb = _resolve_derived_last("", s["taxpayer_name"])
            sp_last = der

        export_hit, how = find_export_pair(
            by_spouse,
            export_rows,
            spouse_first=sp_first,
            spouse_last=sp_last,
            client_ln=s["client_ln"] or "",
            client_fn=s["client_fn"] or "",
            taxpayer_name=s["taxpayer_name"] or "",
            client_dob=s["client_dob"] or "",
        )

        others = other_clients_matching_spouse(
            cache,
            spouse_first=sp_first,
            spouse_last=sp_last,
            exclude_client_id=int(s["client_id"]),
        )

        verdict = classify_row(
            client_id=int(s["client_id"]),
            client_ln=s["client_ln"] or "",
            client_fn=s["client_fn"] or "",
            client_dob=s["client_dob"] or "",
            spouse_first=sp_first,
            spouse_last=sp_last,
            taxpayer_name=s["taxpayer_name"] or "",
            source=s["source"] or "",
            export_hit=export_hit,
            export_how=how,
            other_as_clients=others,
        )
        counts[verdict] += 1

        # Extra: export says this client DOB is a single filer (no spouse) —
        # catch wave4 wrong folds even without going through spouse-key index
        if verdict.startswith("OK") or verdict == "NO_EXPORT":
            cd = _norm_dob(s["client_dob"])
            if cd:
                singles = [
                    e
                    for e in export_rows
                    if e["primary_dob"] == cd
                    and score_client_names_pair(
                        s["client_ln"] or "",
                        s["client_fn"] or "",
                        e["primary_last"],
                        e["primary_first"],
                    )
                    >= ACCEPT_THRESHOLD
                ]
                # Also check split export singles: loaded only MFJ-with-spouse.
                # Single filers aren't in export_rows. Spot-check via raw later.
                pass

        out_rows.append(
            {
                "spouse_row_id": s["spouse_row_id"],
                "client_id": s["client_id"],
                "client_name": f"{s['client_ln']}, {s['client_fn']}",
                "client_dob": s["client_dob"] or "",
                "spouse_name": f"{sp_first} {sp_last}".strip(),
                "spouse_dob": s["date_of_birth"] or "",
                "taxpayer_name": s["taxpayer_name"] or "",
                "source": s["source"] or "",
                "export_how": how,
                "export_line": export_hit["taxpayer_name"] if export_hit else "",
                "export_primary": (
                    f"{export_hit['primary_last']}, {export_hit['primary_first']}"
                    if export_hit
                    else ""
                ),
                "export_primary_dob": export_hit["primary_dob"] if export_hit else "",
                "export_spouse": (
                    f"{export_hit['spouse_first']} {export_hit['spouse_last']}"
                    if export_hit
                    else ""
                ),
                "other_client_ids": ";".join(str(o["id"]) for o in others[:8]),
                "other_client_names": "; ".join(
                    f"{o['last_name']}, {o['first_name']}" for o in others[:5]
                ),
                "verdict": verdict,
            }
        )

    # Second pass: single-filer export (no spouse) but profile has spouse
    # Re-load split export including empty-spouse rows
    singles_by_dob: dict[str, list] = defaultdict(list)
    split_path = DEFAULT_EXPORTS[0][0]
    if os.path.isfile(split_path):
        with open(split_path, newline="", encoding="utf-8-sig") as f:
            f.readline()
            f.readline()
            for r in csv.DictReader(f):
                last = (r.get("Taxpayer Last Name") or "").strip()
                first = (r.get("Taxpayer First Name") or "").strip()
                if not last or last.upper().startswith("TOTAL"):
                    continue
                spouse = (r.get("Spouse Name") or "").strip()
                dob = _norm_dob(r.get("Taxpayer Date of Birth") or "")
                if spouse:
                    continue
                if dob:
                    singles_by_dob[dob].append({"first": first, "last": last})

    for row in out_rows:
        cd = _norm_dob(row["client_dob"])
        if not cd or cd not in singles_by_dob:
            continue
        for sing in singles_by_dob[cd]:
            if (
                score_client_names_pair(
                    row["client_name"].split(",")[0].strip(),
                    row["client_name"].split(",", 1)[1].strip()
                    if "," in row["client_name"]
                    else "",
                    sing["last"],
                    sing["first"],
                )
                >= ACCEPT_THRESHOLD
            ):
                if row["verdict"].startswith("OK") or row["verdict"] == "NO_EXPORT":
                    counts[row["verdict"]] -= 1
                    row["verdict"] = "WRONG_SINGLE_HAS_SPOUSE"
                    counts["WRONG_SINGLE_HAS_SPOUSE"] += 1
                    row["export_how"] = "single_filer_export"
                    row["export_line"] = f"{sing['first']} {sing['last']} (no spouse)"
                break

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    out_path = Path(_TAXOPS) / "reports" / f"spouse_export_verify_{ts}.csv"
    out_path.parent.mkdir(exist_ok=True)
    with out_path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=list(out_rows[0].keys()))
        w.writeheader()
        w.writerows(out_rows)

    print(f"\nWrote {out_path}")
    print("\nVerdict counts:")
    for k, v in sorted(counts.items(), key=lambda x: (-x[1], x[0])):
        print(f"  {k}: {v}")

    problems = [
        r
        for r in out_rows
        if r["verdict"]
        in (
            "WRONG_ATTACH",
            "WRONG_SINGLE_HAS_SPOUSE",
            "SPOUSE_IS_OTHER_CLIENT_NO_EXPORT",
        )
    ]
    print(f"\nProblem rows: {len(problems)}")
    for r in problems[:50]:
        print(
            f"  [{r['verdict']}] client #{r['client_id']} {r['client_name']} "
            f"| spouse={r['spouse_name']} | export={r['export_line']!r} "
            f"| other_clients={r['other_client_ids'] or '-'}"
        )
    if len(problems) > 50:
        print(f"  ... +{len(problems) - 50} more")

    also = sum(1 for r in out_rows if r["verdict"] == "OK_EXPORT_SPOUSE_ALSO_CLIENT")
    variant = sum(1 for r in out_rows if r["verdict"] == "OK_EXPORT_NAME_VARIANT")
    print(
        f"\nSpouse also exists as another client (export confirms MFJ — OK): {also}"
    )
    print(f"Same household, compound/joint name variant (export OK): {variant}")


if __name__ == "__main__":
    main()
