"""W5 hygiene pass — reopen, apply, truncation batch, regenerate closeout from DB.

- Raise in refresh when OPEN finding lacks staff_diff entry
- Reopen 0852 (LILI ERIVES) and 7316 (NUBIA MEDRANO) with dry-run evidence
- Apply 2374 → MELISSA MOLINA; 3574 → NATALIIA BARANETSKA
- Batch truncation-artifact rows (Drake wins): name-order / trunc pairs
- Regenerate W5-closeout.json from applied disposition state (applied=true)
"""
from __future__ import annotations

import json
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path("T:/")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DISP = Path(r"T:\audit\audit_disposition.sqlite")
TAXOPS = Path(r"T:\taxops\taxops.db")
QUEUE = Path(r"T:\audit\investigation\W5-needs-human-queue.json")
CLOSE = Path(r"T:\audit\investigation\W5-closeout.json")
DIFF = Path(r"T:\audit\investigation\W5-staff-diff.md")
OUT_MD = Path(r"T:\audit\investigation\W5-needs-human-queue.md")
HYGIENE = Path(r"T:\audit\investigation\W5-hygiene.json")

REOPEN = {
    "spouse_unrecovered|0852|VILLANUEVA VAZQUEZ|JUAN": {
        "taxops": "LILI, ERIVES",
        "drake": "LILI ERIVES",
        "note": "Wave5 hygiene reopen: order-swap LILI ERIVES — dry-run agreed; was dropped from staff_diff.",
    },
    "spouse_unrecovered|7316|RAMIREZ HUERTA|ISMAEL": {
        "taxops": "NUBIA, MEDRANO",
        "drake": "NUBIA MEDRANO",
        "note": "Wave5 hygiene reopen: order-swap NUBIA MEDRANO — dry-run agreed; was dropped from staff_diff.",
    },
}

# Resolutions: entity_key → (status, note, spouse_fix optional)
APPLY_RESOLVE = {
    "spouse_unrecovered|2374|MOLINA|JOSE": (
        "RESOLVED",
        "Wave5 hygiene: spouse=MELISSA MOLINA (1896 took GREGORIA; strip erroneous JR).",
    ),
    "spouse_unrecovered|3574|SANDOVAL|EFRAIN": (
        "RESOLVED",
        "Wave5 hygiene: spouse=NATALIIA BARANETSKA (suffix-tolerant match on SANDOVAL JR).",
    ),
}

# Truncation / order artifacts — Drake wins (same class as applied 0377/2416/8316/9270)
TRUNCATION_DRAKE_WINS = [
    "spouse_unrecovered|0852|VILLANUEVA VAZQUEZ|JUAN",  # after reopen evidence, resolve as agree
    "spouse_unrecovered|7316|RAMIREZ HUERTA|ISMAEL",
    "spouse_unrecovered|1495|GRAJEDA|ARNULFO",  # None,SUSANA vs SUSANA GRAJEDA
    "spouse_unrecovered|2332|VELASQUEZ|MAYNOR",  # F JOSEF,OROZCO vs JOSEFA OROZCO F
    "spouse_unrecovered|2472|TEXCOCANO|FEDERICO",  # MARGARI,PINEDA vs MARGARITA D PINEDA
    "spouse_unrecovered|3263|MURGUIA MENDEZ|ANA",  # JIMENE,OCTAVIANO vs OCTAVIANO JIMENEZ ZAMORA
    "spouse_unrecovered|3582|ASFOUR|EMAD",  # K,RANDA vs RANDA ASFOUR
    "spouse_unrecovered|3962|MORALES ITZEP|JUAN",  # E MORALE,CLAUDIA vs CLAUDIA MORALES
    "spouse_unrecovered|5628|FLORES SALAMANCA|JOSUE",  # E,BEATRICE vs BEATRICE FLORES
]


def _tokens(s: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", (s or "").upper()) if len(t) > 1}


def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    q = json.loads(QUEUE.read_text(encoding="utf-8"))
    by_ek = {item["entity_key"]: item for item in q["queue"]}

    dconn = sqlite3.connect(str(DISP))
    dconn.row_factory = sqlite3.Row
    actions = []

    # 1) Reopen 0852 / 7316 if currently missing from meaningful staff state
    for ek, meta in REOPEN.items():
        row = dconn.execute(
            "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?",
            (ek,),
        ).fetchone()
        if not row:
            actions.append({"entity_key": ek, "action": "missing_finding"})
            continue
        # Attach evidence note; ensure OPEN for trunc batch to pick up
        dconn.execute(
            """
            UPDATE audit_disposition
               SET status='OPEN', resolved_by=NULL, resolved_at=NULL,
                   note=?
             WHERE finding_id=?
            """,
            (meta["note"] + f" taxops={meta['taxops']} drake={meta['drake']}", row["finding_id"]),
        )
        actions.append({"entity_key": ek, "action": "reopened", "finding_id": row["finding_id"]})

    # 2) Apply 2374 / 3574
    for ek, (status, note) in APPLY_RESOLVE.items():
        row = dconn.execute(
            "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?",
            (ek,),
        ).fetchone()
        if not row:
            actions.append({"entity_key": ek, "action": "missing_finding"})
            continue
        dconn.execute(
            """
            UPDATE audit_disposition
               SET status=?, resolved_by='wave5_hygiene', resolved_at=?, note=?
             WHERE finding_id=?
            """,
            (status, now, note, row["finding_id"]),
        )
        actions.append({"entity_key": ek, "action": f"set_{status}", "finding_id": row["finding_id"]})

    # 3) Truncation batch — Drake wins → RESOLVED
    for ek in TRUNCATION_DRAKE_WINS:
        row = dconn.execute(
            "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?",
            (ek,),
        ).fetchone()
        if not row:
            continue
        if row["status"] in ("RESOLVED", "FALSE_POSITIVE", "WONTFIX"):
            # still allow reopen targets to resolve
            if ek not in REOPEN:
                continue
        dconn.execute(
            """
            UPDATE audit_disposition
               SET status='RESOLVED', resolved_by='wave5_hygiene', resolved_at=?,
                   note=?
             WHERE finding_id=?
            """,
            (
                now,
                "Wave5 hygiene: truncation/order artifact — Drake spouse wins "
                "(same class as 0377/2416/8316/9270).",
                row["finding_id"],
            ),
        )
        actions.append({"entity_key": ek, "action": "trunc_drake_wins", "finding_id": row["finding_id"]})

    dconn.commit()

    # 4) Build staff_diff from remaining OPEN + raise if any lack enrichment
    open_rows = list(
        dconn.execute(
            "SELECT finding_id, entity_key, status, note FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND status='OPEN' "
            "AND entity_key LIKE 'spouse_unrecovered|%' ORDER BY entity_key"
        )
    )
    acked = list(
        dconn.execute(
            "SELECT finding_id, entity_key, resolved_by FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND status='ACKED' "
            "AND entity_key LIKE 'spouse_unrecovered|%'"
        )
    )
    resolved = list(
        dconn.execute(
            "SELECT finding_id, entity_key, resolved_by FROM audit_disposition "
            "WHERE finding_type='NEEDS_HUMAN' AND status='RESOLVED' "
            "AND entity_key LIKE 'spouse_unrecovered|%'"
        )
    )

    # Enrich open rows from prior closeout staff_diff + live spouses
    prior = {}
    if CLOSE.exists():
        try:
            prior_doc = json.loads(CLOSE.read_text(encoding="utf-8"))
            for block in ("staff_diff", "resolvable", "export_gap"):
                for x in prior_doc.get(block) or []:
                    prior[x.get("entity_key")] = x
        except json.JSONDecodeError:
            pass

    tax = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
    tax.row_factory = sqlite3.Row
    staff_diff = []
    missing_enrichment = []
    for r in open_rows:
        ek = r["entity_key"]
        item = by_ek.get(ek) or {}
        prev = prior.get(ek) or {}
        cids = item.get("matched_client_ids") or []
        spouses = []
        for cid in cids:
            for s in tax.execute(
                "SELECT last_name, first_name, source FROM spouses WHERE client_id=?",
                (cid,),
            ):
                spouses.append(f"{s['last_name']}, {s['first_name']}")
        entry = {
            "finding_id": r["finding_id"],
            "entity_key": ek,
            "plan": prev.get("plan") or "STAFF",
            "taxops_spouse": spouses or prev.get("taxops_spouse"),
            "drake_spouse": prev.get("drake_spouse"),
            "drake_hits": prev.get("drake_hits"),
            "note": r["note"],
        }
        # Require at least one of taxops/drake signal for non-export staff rows
        if not entry["taxops_spouse"] and not entry["drake_spouse"] and not entry.get("drake_hits"):
            missing_enrichment.append(ek)
        staff_diff.append(entry)
    tax.close()

    if missing_enrichment:
        dconn.close()
        raise RuntimeError(
            "W5 refresh: OPEN findings lack staff_diff enrichment (refusing to render —): "
            + ", ".join(missing_enrichment)
        )

    # Contamination flag on remaining
    contam_last4 = {"5949", "5180", "3527"}
    for e in staff_diff:
        parts = e["entity_key"].split("|")
        if len(parts) > 1 and parts[1] in contam_last4:
            e["plan"] = "CONTAMINATION_DRAKE_MISATTACH"
            e["note"] = (
                (e.get("note") or "")
                + " Personnel contamination: spouses.taxpayer_name belongs to another household."
            )

    close_doc = {
        "generated_at": now,
        "applied": True,
        "source": "wave5_hygiene from disposition DB",
        "summary": {
            "open_staff": len(staff_diff),
            "acked_export_gap": len(acked),
            "resolved": len(resolved),
            "actions_n": len(actions),
        },
        "export_gap": [
            {"finding_id": r["finding_id"], "entity_key": r["entity_key"], "plan": "ACKED_EXPORT_GAP"}
            for r in acked
        ],
        "resolvable": [
            {"finding_id": r["finding_id"], "entity_key": r["entity_key"], "resolved_by": r["resolved_by"]}
            for r in resolved
        ],
        "staff_diff": staff_diff,
        "hygiene_actions": actions,
    }
    CLOSE.write_text(json.dumps(close_doc, indent=2), encoding="utf-8")

    # Queue + diff markdown
    lines = [
        "# W5 — NEEDS_HUMAN review queue (post-hygiene)",
        "",
        f"_Generated: {now}. Export-gap ACKED. Hygiene applied. Staff should see ~{len(staff_diff)} rows._",
        "",
        f"- **OPEN (staff):** {len(staff_diff)}",
        f"- **RESOLVED:** {len(resolved)}",
        f"- **ACKED EXPORT_GAP:** {len(acked)}",
        "",
        "## Staff diff (OPEN only)",
        "",
        "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
        "|---|---|---|---|",
    ]
    diff_lines = [
        "# W5 — staff diff (OPEN only, post-hygiene)",
        "",
        f"_n={len(staff_diff)}. Contamination trio flagged. Truncation artifacts resolved._",
        "",
        "| Entity | TaxOps spouse(s) | Drake spouse | Plan |",
        "|---|---|---|---|",
    ]
    for e in staff_diff:
        tops = ", ".join(e.get("taxops_spouse") or []) or "—"
        drake = e.get("drake_spouse") or (
            "; ".join(
                (h.get("spouse_raw") or "(blank)") for h in (e.get("drake_hits") or [])
            )
            or "—"
        )
        row = f"| `{e['entity_key']}` | {tops} | {drake} | `{e.get('plan')}` |"
        lines.append(row)
        diff_lines.append(row)
    lines += ["", "## ACKED export-gap", ""]
    for r in acked:
        lines.append(f"- `{r['entity_key']}` — {r['resolved_by']}")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    DIFF.write_text("\n".join(diff_lines) + "\n", encoding="utf-8")

    # Update queue json statuses
    status_by_id = {}
    for r in dconn.execute(
        "SELECT finding_id, status, resolved_by FROM audit_disposition "
        "WHERE finding_type='NEEDS_HUMAN' AND entity_key LIKE 'spouse_unrecovered|%'"
    ):
        status_by_id[r["finding_id"]] = (r["status"], r["resolved_by"])
    for item in q["queue"]:
        st = status_by_id.get(item["finding_id"])
        if st:
            item["status"], item["resolved_by"] = st
    q["needs_human_open"] = len(staff_diff)
    q["wave5_resolved"] = len(resolved)
    q["wave5_export_gap_acked"] = len(acked)
    QUEUE.write_text(json.dumps(q, indent=2), encoding="utf-8")

    HYGIENE.write_text(
        json.dumps({"generated_at": now, "actions": actions, "open_n": len(staff_diff)}, indent=2),
        encoding="utf-8",
    )
    dconn.close()
    print("open", len(staff_diff), "resolved", len(resolved), "acked", len(acked))
    print("actions", len(actions))
    for e in staff_diff:
        print(" OPEN", e["entity_key"], e.get("plan"))


if __name__ == "__main__":
    main()
