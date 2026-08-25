"""Refresh W5 queue docs from disposition DB after close-out."""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

QUEUE = Path(r"T:\audit\investigation\W5-needs-human-queue.json")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT_MD = Path(r"T:\audit\investigation\W5-needs-human-queue.md")
OUT_DIFF = Path(r"T:\audit\investigation\W5-staff-diff.md")
CLOSE = Path(r"T:\audit\investigation\W5-closeout.json")

q = json.loads(QUEUE.read_text(encoding="utf-8"))
close = json.loads(CLOSE.read_text(encoding="utf-8"))
by_id = {x["finding_id"]: x for x in q["queue"]}

conn = sqlite3.connect(f"file:{DISP}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
rows = list(
    conn.execute(
        "SELECT finding_id, entity_key, status, resolved_by, note "
        "FROM audit_disposition WHERE finding_type='NEEDS_HUMAN' "
        "AND entity_key LIKE 'spouse_unrecovered|%'"
    )
)
conn.close()

open_rows = [r for r in rows if r["status"] == "OPEN"]
acked = [r for r in rows if r["status"] == "ACKED"]
resolved = [r for r in rows if r["status"] == "RESOLVED"]

# Enrich open from closeout staff_diff
staff_by_id = {x["finding_id"]: x for x in close.get("staff_diff") or []}

lines = [
    "# W5 — NEEDS_HUMAN review queue",
    "",
    "_Post close-out 2026-08-11. Export-gap trio ACKED (not WONTFIX). Fold↔Drake agrees RESOLVED._",
    "",
    f"- **Still OPEN:** {len(open_rows)} (staff diff)",
    f"- **RESOLVED (wave5 fold↔Drake agree):** {len(resolved)}",
    f"- **ACKED EXPORT_GAP:** {len(acked)} (ROBLEDO / BURGOS GARCIA / RIOS TRINY)",
    "",
    "## Staff diff (OPEN only)",
    "",
    "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
    "|---|---|---|---|",
]
diff_lines = list(lines[-4:])  # table header only for diff file — rebuild below

diff_md = [
    "# W5 — staff diff (OPEN disagreements only)",
    "",
    f"_n={len(open_rows)}. Remaining after Wave 5 close-out._",
    "",
    "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
    "|---|---|---|---|",
]

for r in sorted(open_rows, key=lambda x: x["entity_key"]):
    s = staff_by_id.get(r["finding_id"])
    if s is None:
        raise RuntimeError(
            f"W5 refresh: OPEN finding {r['finding_id']} ({r['entity_key']}) "
            "has no staff_diff entry in W5-closeout.json — refusing to render '—'"
        )
    tops = ", ".join(s.get("taxops_spouse") or []) or (
        ", ".join(
            f"{x.get('last_name')}, {x.get('first_name')}" for x in (s.get("spouses_rows") or [])
        )
        or None
    )
    drake_s = s.get("drake_spouse") or (
        "; ".join(h.get("spouse_raw") or "(blank)" for h in (s.get("drake_hits") or [])) or None
    )
    if not tops and not drake_s:
        raise RuntimeError(
            f"W5 refresh: staff_diff entry for {r['entity_key']} has no taxops/drake spouse signal"
        )
    tops = tops or "—"
    drake_s = drake_s or "—"
    plan = s.get("plan") or "STAFF"
    row = f"| `{r['entity_key']}` | {tops} | {drake_s} | `{plan}` |"
    lines.append(row)
    diff_md.append(row)

lines += [
    "",
    "## ACKED export-gap (not WONTFIX)",
    "",
]
for r in acked:
    lines.append(f"- `{r['entity_key']}` — {r['resolved_by']}")

lines += ["", f"Close-out machine: `W5-closeout.json`", f"Staff-only: `W5-staff-diff.md`"]
OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
OUT_DIFF.write_text("\n".join(diff_md) + "\n", encoding="utf-8")

# update queue json statuses
for item in q["queue"]:
    for r in rows:
        if r["finding_id"] == item["finding_id"]:
            item["status"] = r["status"]
            item["resolved_by"] = r["resolved_by"]
            break
q["needs_human_open"] = len(open_rows)
q["wave5_resolved"] = len(resolved)
q["wave5_export_gap_acked"] = len(acked)
QUEUE.write_text(json.dumps(q, indent=2), encoding="utf-8")
print("OPEN", len(open_rows), "RESOLVED", len(resolved), "ACKED", len(acked))
print("wrote", OUT_MD)
print("wrote", OUT_DIFF)
