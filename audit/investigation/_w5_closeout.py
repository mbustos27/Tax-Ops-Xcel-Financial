"""Wave 5 close-out helper — Drake spouse export diff + disposition plan.

Identifies:
  - resolvable rows (fold/spouse agrees with Drake export, or clear single)
  - EXPORT_GAP trio (ROBLEDO / BURGOS GARCIA / RIOS TRINY) -> ACKED + structured note
    (EXPORT_GAP is not in the enum; do not use WONTFIX)
  - disagreement rows for staff (target ~32)

Dry-run by default. Pass --apply to write dispositions.
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
from typing import Any

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

QUEUE = Path(r"T:\audit\investigation\W5-needs-human-queue.json")
SPOUSE_CSV = Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv")
TAXOPS = Path(r"T:\taxops\taxops.db")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT_JSON = Path(r"T:\audit\investigation\W5-closeout.json")
OUT_MD = Path(r"T:\audit\investigation\W5-closeout.md")
OUT_DIFF = Path(r"T:\audit\investigation\W5-staff-diff.md")

EXPORT_GAP_KEYS = {
    "spouse_unrecovered|5336|ROBLEDO|PEDRO",
    "spouse_unrecovered|9799|BURGOS GARCIA|JESUS",
    "spouse_unrecovered|4032|RIOS|TRINY",
}
EXPORT_GAP_NOTE = (
    "ACKED as export-gap bucket (Wave 5): spouse missing from Drake TY2025 spouse "
    "export / unrecovered after fold — not WONTFIX; re-open when export catches up. "
    "Structured tag: EXPORT_GAP."
)


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


def load_drake_spouses() -> dict[str, list[dict]]:
    """Index by taxpayer last|first norm key."""
    with SPOUSE_CSV.open(encoding="utf-8-sig", newline="") as f:
        rows = list(csv.reader(f))
    # title, as-of, header
    header = rows[2]
    idx = {h: i for i, h in enumerate(header)}
    by_tp: dict[str, list[dict]] = {}
    def cell(row: list[str], col: str) -> str:
        i = idx.get(col)
        if i is None or i >= len(row):
            return ""
        return (row[i] or "").strip()

    for row in rows[3:]:
        if not row or not any(row):
            continue
        t_first = cell(row, "Taxpayer First Name")
        t_last = cell(row, "Taxpayer Last Name")
        s_name = cell(row, "Spouse Name")
        if not (t_last or t_first):
            continue
        s_last, s_first = _parse_spouse_name(s_name)
        key = f"{_norm(t_last)}|{_norm(t_first)}"
        by_tp.setdefault(key, []).append(
            {
                "taxpayer": f"{t_last}, {t_first}".strip(", "),
                "spouse_raw": s_name,
                "spouse_last": s_last,
                "spouse_first": s_first,
                "has_spouse": bool(s_name.strip()),
            }
        )
    return by_tp


def spouse_agree(a_last: str, a_first: str, b_last: str, b_first: str) -> bool:
    if not (_norm(a_last) or _norm(a_first)):
        return False
    if not (_norm(b_last) or _norm(b_first)):
        return False
    ta = _tokens(f"{a_last} {a_first}")
    tb = _tokens(f"{b_last} {b_first}")
    # Order-invariant: same two-or-more name tokens (handles LAST,FIRST vs FIRST LAST swaps)
    if len(ta) >= 2 and len(tb) >= 2 and ta == tb:
        return True
    if len(ta & tb) >= 2 and (not ta - tb or not tb - ta or len(ta & tb) >= min(len(ta), len(tb))):
        return True
    la, lb = _tokens(a_last), _tokens(b_last)
    if not la or not lb or la.isdisjoint(lb):
        return False
    fa, fb = _tokens(a_first), _tokens(b_first)
    if fa and fb and fa.isdisjoint(fb):
        return False
    return True


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    queue = json.loads(QUEUE.read_text(encoding="utf-8"))["queue"]
    drake = load_drake_spouses()

    tax = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    tax.row_factory = sqlite3.Row

    buckets = {
        "export_gap": [],
        "resolvable_agree": [],
        "resolvable_drake_single": [],
        "disagreement": [],
        "no_drake_hit": [],
        "ambiguous_drake": [],
    }

    for item in queue:
        ek = item["entity_key"]
        # entity: spouse_unrecovered|LAST4|LAST|FIRST
        parts = ek.split("|")
        last4 = parts[1] if len(parts) > 1 else item.get("last4")
        tp_last = parts[2] if len(parts) > 2 else item.get("name_hint") or ""
        tp_first = parts[3] if len(parts) > 3 else ""

        if ek in EXPORT_GAP_KEYS:
            buckets["export_gap"].append({**item, "plan": "ACKED_EXPORT_GAP"})
            continue

        # live spouses row(s) for matched clients
        cids = item.get("matched_client_ids") or []
        spouse_rows = []
        for cid in cids:
            for r in tax.execute(
                "SELECT first_name, last_name, source FROM spouses WHERE client_id=?",
                (cid,),
            ):
                spouse_rows.append(dict(r))
            # also clients.spouse_* read-through
            cr = tax.execute(
                "SELECT spouse_first_name, spouse_last_name, last_name, first_name "
                "FROM clients WHERE id=?",
                (cid,),
            ).fetchone()
            if cr:
                item.setdefault("_client_name", f"{cr['last_name']}, {cr['first_name']}")

        # Drake lookup by last4 via TaxOps return/client, and by name
        drake_hits: list[dict] = []
        # name key
        name_key = f"{_norm(tp_last)}|{_norm(tp_first)}"
        drake_hits.extend(drake.get(name_key) or [])
        # also try matched client names
        for cid in cids:
            cr = tax.execute(
                "SELECT last_name, first_name FROM clients WHERE id=?", (cid,)
            ).fetchone()
            if not cr:
                continue
            k = f"{_norm(cr['last_name'])}|{_norm(cr['first_name'])}"
            for h in drake.get(k) or []:
                if h not in drake_hits:
                    drake_hits.append(h)

        # Dedup drake hits by spouse_raw
        seen = set()
        uniq = []
        for h in drake_hits:
            k = h["spouse_raw"]
            if k in seen:
                continue
            seen.add(k)
            uniq.append(h)
        drake_hits = uniq

        rec: dict[str, Any] = {
            **item,
            "tp_last": tp_last,
            "tp_first": tp_first,
            "spouses_rows": spouse_rows,
            "drake_hits": drake_hits,
        }

        if not drake_hits:
            buckets["no_drake_hit"].append({**rec, "plan": "STAFF_NO_DRAKE"})
            continue

        with_spouse = [h for h in drake_hits if h["has_spouse"]]
        without = [h for h in drake_hits if not h["has_spouse"]]

        if not with_spouse and without:
            # Drake says single — resolvable as FALSE_POSITIVE / RESOLVED single
            buckets["resolvable_drake_single"].append(
                {**rec, "plan": "RESOLVED_DRAKE_SINGLE"}
            )
            continue

        if len(with_spouse) > 1:
            # multiple distinct spouse strings
            spouses_set = {_norm(h["spouse_raw"]) for h in with_spouse}
            if len(spouses_set) > 1:
                buckets["ambiguous_drake"].append({**rec, "plan": "STAFF_AMBIGUOUS"})
                continue

        d = with_spouse[0]
        if spouse_rows:
            # compare fold to Drake
            agrees = any(
                spouse_agree(
                    s.get("last_name") or "",
                    s.get("first_name") or "",
                    d["spouse_last"],
                    d["spouse_first"],
                )
                for s in spouse_rows
            )
            if agrees:
                buckets["resolvable_agree"].append(
                    {**rec, "plan": "RESOLVED_FOLD_MATCHES_DRAKE", "drake_spouse": d["spouse_raw"]}
                )
            else:
                buckets["disagreement"].append(
                    {
                        **rec,
                        "plan": "STAFF_DISAGREE",
                        "drake_spouse": d["spouse_raw"],
                        "taxops_spouse": [
                            f"{s.get('last_name')}, {s.get('first_name')}" for s in spouse_rows
                        ],
                    }
                )
        else:
            # no spouses row but Drake has spouse — staff / intake
            buckets["disagreement"].append(
                {
                    **rec,
                    "plan": "STAFF_MISSING_SPOUSE_ROW",
                    "drake_spouse": d["spouse_raw"],
                    "taxops_spouse": [],
                }
            )

    tax.close()

    # Prefer exactly 7 resolvable if we have more agree+single — take singles first then agrees
    resolvable = buckets["resolvable_drake_single"] + buckets["resolvable_agree"]
    staff = (
        buckets["disagreement"]
        + buckets["no_drake_hit"]
        + buckets["ambiguous_drake"]
    )

    summary = {
        "export_gap_n": len(buckets["export_gap"]),
        "resolvable_agree_n": len(buckets["resolvable_agree"]),
        "resolvable_drake_single_n": len(buckets["resolvable_drake_single"]),
        "resolvable_total": len(resolvable),
        "disagreement_n": len(buckets["disagreement"]),
        "no_drake_hit_n": len(buckets["no_drake_hit"]),
        "ambiguous_drake_n": len(buckets["ambiguous_drake"]),
        "staff_total": len(staff),
        "queue_total": len(queue),
        "math_check": f"{len(resolvable)} resolvable + {len(buckets['export_gap'])} export_gap + {len(staff)} staff = {len(resolvable)+len(buckets['export_gap'])+len(staff)}",
    }

    applied = []
    if args.apply:
        dconn = sqlite3.connect(str(DISP))
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for item in buckets["export_gap"]:
            dconn.execute(
                """
                UPDATE audit_disposition
                   SET status='ACKED', resolved_by='wave5_export_gap', resolved_at=?,
                       note=?
                 WHERE finding_id=?
                """,
                (now, EXPORT_GAP_NOTE, item["finding_id"]),
            )
            applied.append({"finding_id": item["finding_id"], "status": "ACKED", "tag": "EXPORT_GAP"})
        for item in resolvable:
            note = (
                "Wave 5: Drake spouse export agrees with folded spouses row — RESOLVED."
                if item["plan"] == "RESOLVED_FOLD_MATCHES_DRAKE"
                else "Wave 5: Drake TY2025 spouse export has blank spouse — RESOLVED as single/no spouse."
            )
            dconn.execute(
                """
                UPDATE audit_disposition
                   SET status='RESOLVED', resolved_by='wave5', resolved_at=?,
                       note=?
                 WHERE finding_id=?
                """,
                (now, note, item["finding_id"]),
            )
            applied.append(
                {"finding_id": item["finding_id"], "status": "RESOLVED", "plan": item["plan"]}
            )
        dconn.commit()
        dconn.close()

    def slim(items: list[dict]) -> list[dict]:
        out = []
        for x in items:
            out.append(
                {
                    "finding_id": x.get("finding_id"),
                    "entity_key": x.get("entity_key"),
                    "plan": x.get("plan"),
                    "has_spouses_row_now": x.get("has_spouses_row_now"),
                    "drake_spouse": x.get("drake_spouse"),
                    "taxops_spouse": x.get("taxops_spouse"),
                    "spouses_rows": x.get("spouses_rows"),
                    "drake_hits": x.get("drake_hits"),
                }
            )
        return out

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied": bool(args.apply),
        "summary": summary,
        "export_gap": slim(buckets["export_gap"]),
        "resolvable": slim(resolvable),
        "staff_diff": slim(staff),
        "applied_rows": applied,
    }
    OUT_JSON.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    lines = [
        "# W5 — close-out",
        "",
        f"_Generated: {payload['generated_at']} · apply={args.apply}_",
        "",
        "## Summary",
        "",
        f"| Bucket | n |",
        f"|---|---:|",
        f"| Resolvable (fold↔Drake agree / Drake single) | {summary['resolvable_total']} |",
        f"| Export-gap → `ACKED` + EXPORT_GAP note | {summary['export_gap_n']} |",
        f"| Staff diff (disagreements / no Drake / ambiguous) | {summary['staff_total']} |",
        f"| Queue total | {summary['queue_total']} |",
        "",
        f"Math: {summary['math_check']}",
        "",
        "Enum note: `EXPORT_GAP` is **not** a status — use `ACKED` with structured note tag "
        "`EXPORT_GAP`. Do not use `WONTFIX` for ROBLEDO / BURGOS GARCIA / RIOS TRINY.",
        "",
        "## Export-gap trio",
        "",
    ]
    for x in buckets["export_gap"]:
        lines.append(f"- `{x['entity_key']}` → ACKED / EXPORT_GAP")
    lines += ["", "## Resolvable", ""]
    for x in resolvable:
        lines.append(
            f"- `{x['entity_key']}` → `{x['plan']}`"
            + (f" (Drake spouse: {x.get('drake_spouse')})" if x.get("drake_spouse") else "")
        )
    lines += ["", f"Staff queue detail: `{OUT_DIFF}`", f"Machine: `{OUT_JSON}`"]
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    diff_lines = [
        "# W5 — staff diff (disagreements only)",
        "",
        f"_n={len(staff)}. Folded TaxOps spouse vs Drake TY2025 spouse export._",
        "",
        "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
        "|---|---|---|---|",
    ]
    for x in staff:
        tops = ", ".join(x.get("taxops_spouse") or []) or (
            ", ".join(
                f"{s.get('last_name')}, {s.get('first_name')}" for s in (x.get("spouses_rows") or [])
            )
            or "—"
        )
        drake_s = x.get("drake_spouse") or (
            "; ".join(h.get("spouse_raw") or "(blank)" for h in (x.get("drake_hits") or [])) or "—"
        )
        diff_lines.append(
            f"| `{x['entity_key']}` | {tops} | {drake_s} | `{x.get('plan')}` |"
        )
    OUT_DIFF.write_text("\n".join(diff_lines) + "\n", encoding="utf-8")

    print("summary", json.dumps(summary, indent=2))
    print("wrote", OUT_MD)
    print("wrote", OUT_DIFF)
    print("wrote", OUT_JSON)
    if args.apply:
        print("applied", len(applied))


if __name__ == "__main__":
    main()
