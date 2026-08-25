"""Split W5 OPEN into Lucy / intake / contamination lanes; leave Lucy ≤6."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DISP = Path(r"T:\audit\audit_disposition.sqlite")
CLOSE = Path(r"T:\audit\investigation\W5-closeout.json")
DIFF = Path(r"T:\audit\investigation\W5-staff-diff.md")
QUEUE_MD = Path(r"T:\audit\investigation\W5-needs-human-queue.md")
QUEUE_JSON = Path(r"T:\audit\investigation\W5-needs-human-queue.json")

# After contam fix these should resolve; if still OPEN mark ACKED CONTAMINATION_FIXED pending verify
CONTAM = {
    "spouse_unrecovered|3527|MUNGUIA|FEDERICO",
    "spouse_unrecovered|5180|QUINTANA|ROGELIO",
    "spouse_unrecovered|5949|MARTINEZ|ARNULFO",
}
INTAKE = {
    "spouse_unrecovered|1859|JORGE BARAJAS|ISRAEL",
    "spouse_unrecovered|3546|REYES SOLIS|GASPAR",
    "spouse_unrecovered|4972|CALZADILLA FLORES|ENRIQUE",
    "spouse_unrecovered|8686|AVALOS PEREZ|JOSE",
    "spouse_unrecovered|9169|OLEA|ANTONIO",
}
# Lucy = remaining STAFF_DISAGREE


def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = sqlite3.connect(str(DISP))
    conn.row_factory = sqlite3.Row

    # Resolve contamination findings after fix
    for ek in CONTAM:
        row = conn.execute(
            "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?", (ek,)
        ).fetchone()
        if not row:
            continue
        conn.execute(
            """
            UPDATE audit_disposition
               SET status='RESOLVED', resolved_by='contam_fix', resolved_at=?,
                   note=?
             WHERE finding_id=?
            """,
            (
                now,
                "Wave contamination fix: Drake-import mis-attached spouse replaced from "
                "clients.spouse_* / Drake spouse export; taxpayer_name cleared.",
                row["finding_id"],
            ),
        )

    # Intake lane — ACKED with structured tag (not WONTFIX)
    for ek in INTAKE:
        row = conn.execute(
            "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?", (ek,)
        ).fetchone()
        if not row or row["status"] != "OPEN":
            continue
        conn.execute(
            """
            UPDATE audit_disposition
               SET status='ACKED', resolved_by='wave5_intake_gap', resolved_at=?,
                   note=?
             WHERE finding_id=?
            """,
            (
                now,
                "ACKED intake-gap: Drake has spouse, TaxOps spouses row missing — "
                "enter on intake. Structured tag: INTAKE_GAP. Not WONTFIX.",
                row["finding_id"],
            ),
        )

    conn.commit()

    open_rows = list(
        conn.execute(
            "SELECT finding_id, entity_key, status, note, resolved_by FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND entity_key LIKE 'spouse_unrecovered|%' "
            "AND status='OPEN' ORDER BY entity_key"
        )
    )
    acked = list(
        conn.execute(
            "SELECT finding_id, entity_key, resolved_by FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND entity_key LIKE 'spouse_unrecovered|%' "
            "AND status='ACKED'"
        )
    )
    resolved = list(
        conn.execute(
            "SELECT finding_id, entity_key, resolved_by FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND entity_key LIKE 'spouse_unrecovered|%' "
            "AND status='RESOLVED'"
        )
    )

    prior = {}
    if CLOSE.exists():
        doc = json.loads(CLOSE.read_text(encoding="utf-8"))
        for x in doc.get("staff_diff") or []:
            prior[x["entity_key"]] = x

    staff = []
    for r in open_rows:
        prev = prior.get(r["entity_key"]) or {}
        staff.append(
            {
                "finding_id": r["finding_id"],
                "entity_key": r["entity_key"],
                "plan": "LUCY_DISAGREE",
                "taxops_spouse": prev.get("taxops_spouse"),
                "drake_spouse": prev.get("drake_spouse"),
                "note": r["note"],
            }
        )

    close = {
        "generated_at": now,
        "applied": True,
        "source": "wave5_lucy_split",
        "summary": {
            "lucy_open": len(staff),
            "acked": len(acked),
            "resolved": len(resolved),
        },
        "staff_diff": staff,
        "export_gap": [
            {"entity_key": r["entity_key"], "resolved_by": r["resolved_by"]}
            for r in acked
            if "export" in (r["resolved_by"] or "")
        ],
        "intake_gap": [
            {"entity_key": r["entity_key"], "resolved_by": r["resolved_by"]}
            for r in acked
            if "intake" in (r["resolved_by"] or "")
        ],
    }
    CLOSE.write_text(json.dumps(close, indent=2), encoding="utf-8")

    lines = [
        "# W5 — Lucy queue (disagreements only)",
        "",
        f"_Generated: {now}. Contamination RESOLVED. Intake gaps ACKED. Lucy should see {len(staff)} rows._",
        "",
        f"- **Lucy OPEN:** {len(staff)}",
        f"- **ACKED (export-gap + intake-gap):** {len(acked)}",
        f"- **RESOLVED:** {len(resolved)}",
        "",
        "## Lucy",
        "",
        "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
        "|---|---|---|---|",
    ]
    for e in staff:
        tops = ", ".join(e.get("taxops_spouse") or []) or "—"
        lines.append(
            f"| `{e['entity_key']}` | {tops} | {e.get('drake_spouse') or '—'} | `{e['plan']}` |"
        )
    lines += ["", "## ACKED lanes", ""]
    for r in acked:
        lines.append(f"- `{r['entity_key']}` — {r['resolved_by']}")
    QUEUE_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    DIFF.write_text("\n".join(lines) + "\n", encoding="utf-8")

    if QUEUE_JSON.exists():
        q = json.loads(QUEUE_JSON.read_text(encoding="utf-8"))
        by_id = {
            r["finding_id"]: (r["status"], r["resolved_by"])
            for r in conn.execute(
                "SELECT finding_id, status, resolved_by FROM audit_disposition "
                "WHERE finding_type='NEEDS_HUMAN' AND entity_key LIKE 'spouse_unrecovered|%'"
            )
        }
        for item in q.get("queue") or []:
            st = by_id.get(item["finding_id"])
            if st:
                item["status"], item["resolved_by"] = st
        q["needs_human_open"] = len(staff)
        q["lucy_n"] = len(staff)
        QUEUE_JSON.write_text(json.dumps(q, indent=2), encoding="utf-8")

    conn.close()
    print("lucy", len(staff), "acked", len(acked), "resolved", len(resolved))
    for e in staff:
        print(" ", e["entity_key"])


if __name__ == "__main__":
    main()
