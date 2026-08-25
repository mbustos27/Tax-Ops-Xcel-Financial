"""DETACH residual 27 contam: wrong person, no clients.spouse_*, not in spouse export.

Leaving another household's name on the record is worse than an empty spouses row;
intake can re-add the correct spouse. Always dry-run unless --apply.
"""
from __future__ import annotations

import argparse
import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")
TRIAGE = Path(r"T:\audit\investigation\W4-contam-triage.json")
OUT = Path(r"T:\audit\investigation\W4-contam-detach27.json")
OUT_MD = Path(r"T:\audit\investigation\W4-contam-detach27.md")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    args = ap.parse_args()

    doc = json.loads(TRIAGE.read_text(encoding="utf-8"))
    plans = list(doc.get("human_plans") or [])
    if len(plans) != 27:
        # re-derive from live DB if triage stale
        print("warn: triage human_plans=", len(plans), "- using as-is")

    conn = sqlite3.connect(str(TAXOPS) if args.apply else f"file:{TAXOPS}?mode=ro", uri=not args.apply)
    if args.apply:
        conn.execute("PRAGMA busy_timeout=60000")
    conn.row_factory = sqlite3.Row

    verified = []
    for p in plans:
        row = conn.execute(
            """
            SELECT s.id, s.last_name, s.first_name, s.taxpayer_name,
                   c.spouse_last_name, c.spouse_first_name
              FROM spouses s JOIN clients c ON c.id=s.client_id
             WHERE s.id=? AND c.id=?
            """,
            (p["spouse_row_id"], p["client_id"]),
        ).fetchone()
        if not row:
            verified.append({**p, "verify": "MISSING"})
            continue
        if row["spouse_last_name"] or row["spouse_first_name"]:
            verified.append({**p, "verify": "HAS_CLIENTS_COLS_SKIP"})
            continue
        if not row["taxpayer_name"]:
            verified.append({**p, "verify": "NO_TP_NAME_SKIP"})
            continue
        verified.append({**p, "verify": "OK_DETACH"})

    detachable = [p for p in verified if p["verify"] == "OK_DETACH"]
    applied = []
    if args.apply:
        now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        for p in detachable:
            conn.execute("DELETE FROM spouses WHERE id=?", (p["spouse_row_id"],))
            applied.append(p)
        conn.commit()
        note = f"contam_detach27:{now}"
    else:
        note = None

    # remasure
    n_sp = conn.execute("SELECT count(*) FROM spouses").fetchone()[0]
    n_tp = conn.execute(
        "SELECT count(*) FROM spouses WHERE taxpayer_name IS NOT NULL AND trim(taxpayer_name)!=''"
    ).fetchone()[0]
    conn.close()

    payload = {
        "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "applied": bool(args.apply),
        "n_input": len(plans),
        "n_detachable": len(detachable),
        "n_skipped": len(verified) - len(detachable),
        "n_applied": len(applied),
        "post_spouses": n_sp,
        "post_taxpayer_name_set": n_tp,
        "note": note,
        "verified": verified,
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    lines = [
        "# W4 — detach residual 27 (NOT_IN_SPOUSE_EXPORT)",
        "",
        f"_Generated: {payload['generated_at']} · apply={args.apply}_",
        "",
        f"| Metric | n |",
        f"|---|---:|",
        f"| Input residual | {payload['n_input']} |",
        f"| DETACH | {payload['n_detachable']} |",
        f"| Skipped | {payload['n_skipped']} |",
        f"| Applied | {payload['n_applied']} |",
        f"| spouses after | {n_sp} |",
        f"| taxpayer_name still set | {n_tp} |",
        "",
        "Policy: wrong-person row, empty `clients.spouse_*`, owner absent from TY2025 spouse export → delete spouses row.",
        "",
    ]
    for p in detachable[:30]:
        lines.append(f"- `{p['client_id']}` {p['owner']}: drop `{p['wrong_spouse']}`")
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(
        "detachable", len(detachable),
        "skipped", payload["n_skipped"],
        "applied", len(applied),
        "spouses", n_sp,
        "tp_set", n_tp,
    )


if __name__ == "__main__":
    main()
