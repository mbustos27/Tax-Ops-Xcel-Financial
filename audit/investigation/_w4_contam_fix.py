"""Fix Drake-import spouse mis-attachments (personnel contamination).

For each spouses row where taxpayer_name is disjoint from the owner client name:
  - If clients.spouse_* has a real name → replace spouses last/first from clients cols
  - Else if Drake TY2025 spouse export has a spouse for this taxpayer → use Drake
  - Else leave for human (do not invent)

Always dry-run unless --apply. W5 contamination trio is included.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TAXOPS = Path(r"T:\taxops\taxops.db")
SPOUSE_CSV = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
OUT = Path(r"T:\audit\investigation\W4-contam-fix.json")
OUT_MD = Path(r"T:\audit\investigation\W4-contam-fix.md")

W5_TRIO = {610, 819, 145}  # MARTINEZ, QUINTANA, MUNGUIA


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(s: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", _norm(s)) if len(t) > 1}


def _bag(last: str, first: str) -> set[str]:
    return _tokens(f"{last} {first}")


def _parse_spouse_name(raw: str) -> tuple[str, str]:
    raw = (raw or "").strip()
    if not raw:
        return "", ""
    if "," in raw:
        last, first = [x.strip() for x in raw.split(",", 1)]
        return last, first
    parts = raw.split()
    if len(parts) == 1:
        return parts[0], ""
    return parts[-1], " ".join(parts[:-1])


def load_drake() -> dict[str, list[dict]]:
    with SPOUSE_CSV.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    header = rows[2]
    idx = {h: i for i, h in enumerate(header)}

    def cell(row, col):
        i = idx.get(col)
        if i is None or i >= len(row):
            return ""
        return (row[i] or "").strip()

    by_tp: dict[str, list[dict]] = {}
    for row in rows[3:]:
        if not row or not any(row):
            continue
        t_first, t_last = cell(row, "Taxpayer First Name"), cell(row, "Taxpayer Last Name")
        s_name = cell(row, "Spouse Name")
        if not (t_last or t_first):
            continue
        s_last, s_first = _parse_spouse_name(s_name)
        key = f"{_norm(t_last)}|{_norm(t_first)}"
        by_tp.setdefault(key, []).append(
            {
                "spouse_raw": s_name,
                "spouse_last": s_last,
                "spouse_first": s_first,
                "has_spouse": bool(s_name),
            }
        )
    return by_tp


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--w5-only", action="store_true", help="Only fix W5 trio ids")
    args = ap.parse_args()

    drake = load_drake()
    conn = sqlite3.connect(str(TAXOPS) if args.apply else f"file:{TAXOPS}?mode=ro", uri=not args.apply)
    if args.apply:
        conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row

    cols = {r[1] for r in conn.execute("PRAGMA table_info(spouses)")}
    has_tp = "taxpayer_name" in cols
    sql = (
        "SELECT s.id, s.client_id, s.last_name, s.first_name, s.source"
        + (", s.taxpayer_name" if has_tp else ", NULL AS taxpayer_name")
        + ", c.last_name AS cl, c.first_name AS cf, c.spouse_last_name, c.spouse_first_name, c.ssn_last4 "
        "FROM spouses s JOIN clients c ON c.id=s.client_id "
        "WHERE COALESCE(c.is_test,0)=0"
    )
    rows = list(conn.execute(sql))

    plans = []
    for r in rows:
        if args.w5_only and r["client_id"] not in W5_TRIO:
            continue
        tp = r["taxpayer_name"]
        if not tp or not str(tp).strip():
            continue
        owner_last_tok = _tokens(r["cl"] or "")
        tbag = _tokens(str(tp))
        if not owner_last_tok or not owner_last_tok.isdisjoint(tbag):
            continue  # not a mismatch

        col_last = (r["spouse_last_name"] or "").strip()
        col_first = (r["spouse_first_name"] or "").strip()
        # Prefer clients.spouse_* when present and not sentinel / not single-letter junk
        replacement = None
        source = None
        col_ok = bool(col_last or col_first) and _norm(col_last) not in (
            "UNKNOWN",
            "NONE",
            "TAXPAYER",
        ) and _norm(col_first) not in ("SPOUSE", "UNKNOWN", "NONE") and len(_norm(col_last)) > 1
        if col_ok:
            replacement = (col_last, col_first)
            source = "clients.spouse_*"

        # Drake lookup (also used to override junk clients cols for W5-class cases)
        key = f"{_norm(r['cl'])}|{_norm(r['cf'])}"
        hits = [h for h in (drake.get(key) or []) if h["has_spouse"]]
        if not hits and "&" in (r["cf"] or ""):
            # joint first: try each side + surname
            for part in re.split(r"\s*&\s*", r["cf"] or ""):
                part = part.strip()
                if not part:
                    continue
                key2 = f"{_norm(r['cl'])}|{_norm(part)}"
                hits = [h for h in (drake.get(key2) or []) if h["has_spouse"]]
                if hits:
                    break
            # also strip JR/SR from last
            last_core = re.sub(r"\b(JR|SR|II|III|IV)\b", "", r["cl"] or "", flags=re.I).strip()
            if not hits and last_core != (r["cl"] or ""):
                for part in re.split(r"\s*&\s*", r["cf"] or ""):
                    key2 = f"{_norm(last_core)}|{_norm(part.strip())}"
                    hits = [h for h in (drake.get(key2) or []) if h["has_spouse"]]
                    if hits:
                        break
        drake_rep = None
        if len(hits) == 1 or (hits and len({h["spouse_raw"] for h in hits}) == 1):
            drake_rep = (hits[0]["spouse_last"], hits[0]["spouse_first"])

        if drake_rep and (not col_ok or r["client_id"] in W5_TRIO):
            replacement = drake_rep
            source = "drake_spouse_export"
        elif replacement is None and drake_rep:
            replacement = drake_rep
            source = "drake_spouse_export"

        # Skip if replacement equals current (already correct name somehow)
        plan = {
            "spouse_row_id": r["id"],
            "client_id": r["client_id"],
            "owner": f"{r['cl']}, {r['cf']}",
            "owner_last4": r["ssn_last4"],
            "wrong_spouse": f"{r['last_name']}, {r['first_name']}",
            "taxpayer_name": tp,
            "source_row": r["source"],
            "replacement": (
                f"{replacement[0]}, {replacement[1]}" if replacement else None
            ),
            "replacement_source": source,
            "action": "REPLACE" if replacement else "NEEDS_HUMAN",
            "w5_trio": r["client_id"] in W5_TRIO,
        }
        plans.append(plan)

    applied = []
    if args.apply:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for p in plans:
            if p["action"] != "REPLACE":
                continue
            last, first = p["replacement"].split(",", 1)
            last, first = last.strip(), first.strip()
            conn.execute(
                """
                UPDATE spouses
                   SET last_name=?, first_name=?,
                       source=?,
                       taxpayer_name=NULL
                 WHERE id=?
                """,
                (
                    last,
                    first,
                    f"contam_fix:{p['replacement_source']}:{now[:10]}",
                    p["spouse_row_id"],
                ),
            )
            applied.append(p)
        conn.commit()

    conn.close()
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied": bool(args.apply),
        "w5_only": bool(args.w5_only),
        "n_mismatch": len(plans),
        "n_replace": sum(1 for p in plans if p["action"] == "REPLACE"),
        "n_needs_human": sum(1 for p in plans if p["action"] == "NEEDS_HUMAN"),
        "n_applied": len(applied),
        "w5_trio_plans": [p for p in plans if p["w5_trio"]],
        "plans": plans,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# W4 — contamination fix",
        "",
        f"_Generated: {payload['generated_at']} · apply={args.apply} · w5_only={args.w5_only}_",
        "",
        f"| Metric | n |",
        f"|---|---:|",
        f"| taxpayer_name mismatches scanned | {payload['n_mismatch']} |",
        f"| REPLACE (have clients.* or Drake) | {payload['n_replace']} |",
        f"| NEEDS_HUMAN | {payload['n_needs_human']} |",
        f"| Applied | {payload['n_applied']} |",
        "",
        "## W5 trio",
        "",
    ]
    for p in payload["w5_trio_plans"]:
        lines.append(
            f"- **{p['owner']}** (`{p['client_id']}`): `{p['wrong_spouse']}` ← `{p['taxpayer_name']}` "
            f"→ `{p['replacement'] or '—'}` ({p['action']} via {p['replacement_source']})"
        )
    lines += ["", f"Machine: `{OUT}`"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        "mismatch",
        payload["n_mismatch"],
        "replace",
        payload["n_replace"],
        "human",
        payload["n_needs_human"],
        "applied",
        payload["n_applied"],
    )
    for p in payload["w5_trio_plans"]:
        print("W5", p["client_id"], p["action"], p["wrong_spouse"], "->", p["replacement"])


if __name__ == "__main__":
    main()
