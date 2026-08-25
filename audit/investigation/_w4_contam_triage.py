"""Triage remaining contamination NEEDS_HUMAN — enhance Drake lookup, classify.

Safe auto actions beyond REPLACE:
- JOINT_FROM_FIRST: clients.first_name has 'A & B' -> spouse = (owner last, B)
- DETACH_DRAKE_BLANK: owner exact-match in spouse export with blank Spouse Name
  (wrong person must go; Drake says no spouse)
- DETACH_BUSINESS: LLC/INC owner with no usable clients.spouse_* (wrong person on entity)

Residual NEEDS_HUMAN: not in export, non-joint, non-biz.
"""
from __future__ import annotations

import argparse
import csv
import json
import re
import sqlite3
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

TAXOPS = Path(r"T:\taxops\taxops.db")
SPOUSE_CSV = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
OUT = Path(r"T:\audit\investigation\W4-contam-triage.json")
OUT_MD = Path(r"T:\audit\investigation\W4-contam-triage.md")

BIZ = re.compile(r"\b(LLC|INC|CORP|CO|COMPANY|SVC|SERVICES|LP|LLP|PC|PLLC)\b", re.I)


def _norm(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def _tokens(s: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", _norm(s)) if len(t) > 1}


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


def _joint_spouse(cl: str, cf: str) -> tuple[str, str] | None:
    """From 'WILLIAM J & MARIA D' -> (cl, 'MARIA D'); handles 'A & LAST, FIRST'."""
    if "&" not in (cf or ""):
        return None
    right = re.split(r"\s*&\s*", cf, maxsplit=1)[1].strip()
    if not right:
        return None
    if "," in right:
        # BRAVO GARCIA, MARTHA
        last, first = [x.strip() for x in right.split(",", 1)]
        return last, first
    # Prefer owner last name + right-hand given names
    return (cl or "").strip(), right


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
        t_first = cell(row, "Taxpayer First Name")
        t_last = cell(row, "Taxpayer Last Name")
        s_name = cell(row, "Spouse Name")
        if not (t_last or t_first):
            continue
        s_last, s_first = _parse_spouse_name(s_name)
        rec = {
            "spouse_raw": s_name,
            "spouse_last": s_last,
            "spouse_first": s_first,
            "has_spouse": bool(s_name),
            "t_last": t_last,
            "t_first": t_first,
        }
        key = f"{_norm(t_last)}|{_norm(t_first)}"
        by_tp.setdefault(key, []).append(rec)
    return by_tp


def lookup_owner(by_tp: dict, cl: str, cf: str) -> list[dict]:
    """All Drake spouse-export rows for this owner (including blank Spouse Name)."""
    keys = [f"{_norm(cl)}|{_norm(cf)}"]
    last_core = re.sub(r"\b(JR|SR|II|III|IV)\b", "", cl or "", flags=re.I).strip()
    if last_core != (cl or ""):
        keys.append(f"{_norm(last_core)}|{_norm(cf)}")
    # primary side of joint first only for export presence
    if "&" in (cf or ""):
        left = re.split(r"\s*&\s*", cf)[0].strip()
        keys.append(f"{_norm(cl)}|{_norm(left)}")
        if last_core != (cl or ""):
            keys.append(f"{_norm(last_core)}|{_norm(left)}")
    out = []
    seen = set()
    for k in keys:
        for h in by_tp.get(k) or []:
            sig = (h["t_last"], h["t_first"], h["spouse_raw"])
            if sig in seen:
                continue
            seen.add(sig)
            out.append(h)
    return out


def col_ok(col_last, col_first) -> bool:
    if not (col_last or col_first):
        return False
    if _norm(col_last) in ("UNKNOWN", "NONE", "TAXPAYER"):
        return False
    if _norm(col_first) in ("SPOUSE", "UNKNOWN", "NONE"):
        return False
    if len(_norm(col_last)) <= 1:
        return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    by_tp = load_drake()
    print("drake keys", len(by_tp))

    conn = sqlite3.connect(str(TAXOPS) if args.apply else f"file:{TAXOPS}?mode=ro", uri=not args.apply)
    if args.apply:
        conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row

    rows = list(
        conn.execute(
            """
            SELECT s.id, s.client_id, s.last_name, s.first_name, s.source, s.taxpayer_name,
                   c.last_name AS cl, c.first_name AS cf,
                   c.spouse_last_name, c.spouse_first_name, c.ssn_last4
              FROM spouses s JOIN clients c ON c.id=s.client_id
             WHERE COALESCE(c.is_test,0)=0
               AND s.taxpayer_name IS NOT NULL AND trim(s.taxpayer_name) != ''
            """
        )
    )

    plans = []
    for r in rows:
        owner_last_tok = _tokens(r["cl"] or "")
        tbag = _tokens(str(r["taxpayer_name"]))
        if not owner_last_tok or not owner_last_tok.isdisjoint(tbag):
            continue

        owner_rows = lookup_owner(by_tp, r["cl"], r["cf"])
        with_spouse = [h for h in owner_rows if h["has_spouse"]]
        unique_spouses = {h["spouse_raw"] for h in with_spouse}
        drake_rep = None
        if len(unique_spouses) == 1 and with_spouse:
            drake_rep = (with_spouse[0]["spouse_last"], with_spouse[0]["spouse_first"])
        drake_blank = bool(owner_rows) and not with_spouse

        c_ok = col_ok(r["spouse_last_name"], r["spouse_first_name"])
        joint = _joint_spouse(r["cl"] or "", r["cf"] or "")
        biz = bool(BIZ.search(r["cl"] or "") or BIZ.search(r["cf"] or ""))

        replacement = None
        source = None
        action = "NEEDS_HUMAN"
        reason = "NO_DRAKE_HIT"

        if c_ok:
            replacement = (r["spouse_last_name"].strip(), (r["spouse_first_name"] or "").strip())
            source = "clients.spouse_*"
            action = "REPLACE"
            reason = None
        if drake_rep and (not c_ok or action != "REPLACE"):
            replacement = drake_rep
            source = "drake_spouse_export"
            action = "REPLACE"
            reason = None
        elif joint and not c_ok and not drake_rep:
            # joint name encodes spouse; prefer over leaving wrong person
            replacement = joint
            source = "joint_first_name"
            action = "REPLACE"
            reason = None
        elif not replacement and drake_blank and not c_ok:
            action = "DETACH"
            reason = "DRAKE_BLANK_SPOUSE"
            source = "drake_blank"
        elif not replacement and biz and not c_ok:
            action = "DETACH"
            reason = "BUSINESS_OWNER"
            source = "business_detach"
        elif not replacement and len(unique_spouses) > 1:
            reason = "AMBIGUOUS_DRAKE"
        elif not replacement and not owner_rows:
            reason = "NOT_IN_SPOUSE_EXPORT"
        elif not replacement:
            reason = "EMPTY_BOTH"

        plans.append(
            {
                "spouse_row_id": r["id"],
                "client_id": r["client_id"],
                "owner": f"{r['cl']}, {r['cf']}",
                "owner_last4": r["ssn_last4"],
                "wrong_spouse": f"{r['last_name']}, {r['first_name']}",
                "taxpayer_name": r["taxpayer_name"],
                "source_row": r["source"],
                "replacement": (
                    f"{replacement[0]}, {replacement[1]}" if replacement else None
                ),
                "replacement_source": source,
                "action": action,
                "reason": reason,
                "n_owner_export_rows": len(owner_rows),
                "drake_blank": drake_blank,
                "business": biz,
            }
        )

    applied = []
    if args.apply:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for p in plans:
            if p["action"] == "REPLACE":
                last, first = p["replacement"].split(",", 1)
                last, first = last.strip(), first.strip()
                conn.execute(
                    """
                    UPDATE spouses
                       SET last_name=?, first_name=?, source=?, taxpayer_name=NULL
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
            elif p["action"] == "DETACH":
                conn.execute("DELETE FROM spouses WHERE id=?", (p["spouse_row_id"],))
                applied.append(p)
        conn.commit()
    conn.close()

    reasons = Counter(p["reason"] for p in plans if p["action"] == "NEEDS_HUMAN")
    actions = Counter(p["action"] for p in plans)
    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied": bool(args.apply),
        "n_mismatch": len(plans),
        "n_replace": actions.get("REPLACE", 0),
        "n_detach": actions.get("DETACH", 0),
        "n_needs_human": actions.get("NEEDS_HUMAN", 0),
        "n_applied": len(applied),
        "actions": dict(actions),
        "human_reasons": dict(reasons),
        "replace_plans": [p for p in plans if p["action"] == "REPLACE"],
        "detach_plans": [p for p in plans if p["action"] == "DETACH"],
        "human_plans": [p for p in plans if p["action"] == "NEEDS_HUMAN"],
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# W4 — contamination triage (DETACH + joint)",
        "",
        f"_Generated: {payload['generated_at']} · apply={args.apply}_",
        "",
        f"| Metric | n |",
        f"|---|---:|",
        f"| mismatches still open | {payload['n_mismatch']} |",
        f"| REPLACE | {payload['n_replace']} |",
        f"| DETACH | {payload['n_detach']} |",
        f"| NEEDS_HUMAN | {payload['n_needs_human']} |",
        f"| Applied | {payload['n_applied']} |",
        "",
        "## Human reasons",
        "",
    ]
    for k, v in sorted(reasons.items(), key=lambda x: -x[1]):
        lines.append(f"- `{k}`: {v}")
    lines += ["", "## REPLACE sample", ""]
    for p in payload["replace_plans"][:15]:
        lines.append(
            f"- `{p['client_id']}` {p['owner']}: `{p['wrong_spouse']}` -> `{p['replacement']}` "
            f"({p['replacement_source']})"
        )
    lines += ["", "## DETACH sample", ""]
    for p in payload["detach_plans"][:15]:
        lines.append(
            f"- `{p['client_id']}` {p['owner']}: drop `{p['wrong_spouse']}` ({p['reason']})"
        )
    lines += ["", f"Machine: `{OUT}`"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        "mismatch", payload["n_mismatch"],
        "replace", payload["n_replace"],
        "detach", payload["n_detach"],
        "human", payload["n_needs_human"],
        "reasons", dict(reasons),
        "applied", payload["n_applied"],
    )


if __name__ == "__main__":
    main()
