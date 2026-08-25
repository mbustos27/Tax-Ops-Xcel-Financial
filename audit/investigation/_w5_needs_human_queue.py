"""Wave 5 — inventory NEEDS_HUMAN + post-wave disposition rollup."""
from __future__ import annotations

import json
import sqlite3
from collections import Counter, defaultdict
from pathlib import Path

DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT_MD = Path(r"T:\audit\investigation\W5-needs-human-queue.md")
OUT_JSON = Path(r"T:\audit\investigation\W5-needs-human-queue.json")


def main() -> None:
    conn = sqlite3.connect(str(DISP))
    conn.row_factory = sqlite3.Row

    rollup = [
        dict(r)
        for r in conn.execute(
            """
            SELECT finding_type, status, COUNT(*) AS n
            FROM audit_disposition
            GROUP BY finding_type, status
            ORDER BY finding_type, status
            """
        )
    ]

    nh = [
        dict(r)
        for r in conn.execute(
            """
            SELECT finding_id, entity_key, status, sample_detail, note,
                   first_seen_run, last_seen_run
            FROM audit_disposition
            WHERE finding_type='NEEDS_HUMAN' AND status IN ('OPEN','ACKED')
            ORDER BY entity_key
            """
        )
    ]
    amb = [
        dict(r)
        for r in conn.execute(
            """
            SELECT finding_id, entity_key, status, sample_detail
            FROM audit_disposition
            WHERE finding_type='SPOUSE_AMBIGUOUS' AND status IN ('OPEN','ACKED')
            ORDER BY entity_key
            """
        )
    ]

    by_sub: dict[str, list] = defaultdict(list)
    for r in nh:
        ek = r["entity_key"] or ""
        # entity_key = subtype|' '|stable subject
        if "|" in ek:
            subtype = ek.split("|", 1)[0].strip()
        elif " " in ek:
            subtype = ek.split(" ", 1)[0].strip()
        else:
            subtype = ek or "(blank)"
        detail = {}
        try:
            detail = json.loads(r["sample_detail"] or "{}")
        except json.JSONDecodeError:
            detail = {"raw": (r["sample_detail"] or "")[:200]}
        # scrub long digit runs from display
        item = {
            "finding_id": r["finding_id"],
            "entity_key": ek,
            "subtype": subtype,
            "status": r["status"],
            "detail_keys": sorted(detail.keys()) if isinstance(detail, dict) else [],
            "salient": detail.get("salient") or detail.get("reason") or detail.get("subtype"),
            "note": r["note"],
        }
        by_sub[subtype].append(item)

    lines = [
        "# W5 — NEEDS_HUMAN review queue",
        "",
        "_Generated for Wave 5. Findings-only; no TaxOps writes. Human adjudication required._",
        "",
        f"**Open/ACKED `NEEDS_HUMAN`:** {len(nh)}",
        f"**Open/ACKED `SPOUSE_AMBIGUOUS` (also human):** {len(amb)}",
        "",
        "## By subtype",
        "",
        "| Subtype | n |",
        "|---|---:|",
    ]
    for sub, items in sorted(by_sub.items(), key=lambda x: (-len(x[1]), x[0])):
        lines.append(f"| `{sub}` | {len(items)} |")

    lines.extend(["", "## Queue (batched)", ""])
    for sub, items in sorted(by_sub.items(), key=lambda x: (-len(x[1]), x[0])):
        lines.append(f"### `{sub}` ({len(items)})")
        lines.append("")
        lines.append("| Entity key | Status | Note |")
        lines.append("|---|---|---|")
        for it in items:
            note = (it.get("note") or it.get("salient") or "").replace("|", "/")
            lines.append(f"| `{it['entity_key']}` | {it['status']} | {note} |")
        lines.append("")

    lines.extend(
        [
            "## How to clear",
            "",
            "1. Staff who know the return (preparer / Lucy) reviews each subtype batch.",
            "2. Disposition each finding in `audit_disposition` → `RESOLVED` / `WONTFIX` / `FALSE_POSITIVE` with a short note.",
            "3. Do **not** mass-resolve — subtype batches exist so distinct reasons stay separate.",
            "4. Time-box; Wave 0 re-export already landed — counts here are post-Wave-0/2/3/4.",
            "",
            f"Machine: `{OUT_JSON}`",
        ]
    )
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "needs_human_n": len(nh),
                "spouse_ambiguous_n": len(amb),
                "by_subtype": {k: len(v) for k, v in by_sub.items()},
                "queue": by_sub,
                "disposition_rollup": rollup,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print("NEEDS_HUMAN", len(nh), "subtypes", {k: len(v) for k, v in by_sub.items()})
    print("SPOUSE_AMBIGUOUS", len(amb))
    print("wrote", OUT_MD)
    conn.close()


if __name__ == "__main__":
    main()
