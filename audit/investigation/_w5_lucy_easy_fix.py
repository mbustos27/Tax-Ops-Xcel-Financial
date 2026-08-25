"""Auto-fix remaining Lucy OPEN easy cases; mark MONTANEZ false positive.

1. HUERTA 495 — Drake-import contam (EVELYN from JESUS&EVELYN) -> BRIDGET HUERTA
2. RAMON 139 — fold junk last 'B' -> MARIA MORENO from Drake
3. MONTANEZ entity_key last4 6789 — matched LEVINE/SAMPLE, not real MONTANEZ 670
   (already has JOANNA) -> FALSE_POSITIVE wrong fingerprint match
"""
from __future__ import annotations

import json
import sqlite3
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT = Path(r"T:\audit\investigation\W5-lucy-easy-fix.json")


def main() -> None:
    apply = "--apply" in sys.argv
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    tax = sqlite3.connect(str(TAXOPS) if apply else f"file:{TAXOPS}?mode=ro", uri=not apply)
    tax.row_factory = sqlite3.Row
    if apply:
        tax.execute("PRAGMA busy_timeout=60000")

    plans = []

    # HUERTA: spouse row 93 on client 495
    h = tax.execute(
        "SELECT s.id, s.last_name, s.first_name, s.taxpayer_name, s.source, "
        "c.id AS cid, c.last_name, c.first_name, c.spouse_last_name, c.spouse_first_name "
        "FROM spouses s JOIN clients c ON c.id=s.client_id WHERE c.id=495"
    ).fetchone()
    plans.append(
        {
            "case": "HUERTA",
            "client_id": 495,
            "spouse_id": h["id"] if h else None,
            "before": f"{h['last_name']}, {h['first_name']}" if h else None,
            "taxpayer_name": h["taxpayer_name"] if h else None,
            "after": "HUERTA, BRIDGET",
            "action": "REPLACE_CONTAM",
            "entity_key": "spouse_unrecovered|1313|HUERTA|DANNY",
        }
    )

    # RAMON: spouse row on client 139
    r = tax.execute(
        "SELECT s.id, s.last_name, s.first_name, s.taxpayer_name, s.source "
        "FROM spouses s WHERE s.client_id=139"
    ).fetchone()
    plans.append(
        {
            "case": "RAMON",
            "client_id": 139,
            "spouse_id": r["id"] if r else None,
            "before": f"{r['last_name']}, {r['first_name']}" if r else None,
            "taxpayer_name": r["taxpayer_name"] if r else None,
            "after": "MORENO, MARIA",
            "action": "REPLACE_TRUNC",
            "entity_key": "spouse_unrecovered|1697|RAMON RODRIGUEZ|CRESCENCIANO",
        }
    )

    # MONTANEZ fingerprint: verify real client + wrong matches
    real = tax.execute(
        "SELECT id, last_name, first_name, ssn_last4, spouse_last_name, spouse_first_name "
        "FROM clients WHERE id=670"
    ).fetchone()
    real_sp = tax.execute(
        "SELECT id, last_name, first_name, source FROM spouses WHERE client_id=670"
    ).fetchone()
    wrong = list(
        tax.execute(
            "SELECT id, last_name, first_name, ssn_last4 FROM clients WHERE ssn_last4='6789'"
        )
    )
    plans.append(
        {
            "case": "MONTANEZ",
            "real_client": dict(real) if real else None,
            "real_spouse": dict(real_sp) if real_sp else None,
            "last4_6789_clients": [dict(x) for x in wrong],
            "action": "FALSE_POSITIVE_WRONG_LAST4",
            "entity_key": "spouse_unrecovered|6789|MONTANEZ|PABLO",
        }
    )

    applied = []
    if apply:
        # HUERTA replace
        if h and h["id"]:
            tax.execute(
                """
                UPDATE spouses
                   SET last_name=?, first_name=?, source=?, taxpayer_name=NULL
                 WHERE id=?
                """,
                ("HUERTA", "BRIDGET", f"contam_fix:drake_win:{now[:10]}", h["id"]),
            )
            applied.append("HUERTA")
        # RAMON replace
        if r and r["id"]:
            tax.execute(
                """
                UPDATE spouses
                   SET last_name=?, first_name=?, source=?
                 WHERE id=?
                """,
                ("MORENO", "MARIA", f"trunc_fix:drake_win:{now[:10]}", r["id"]),
            )
            applied.append("RAMON")
        tax.commit()

        disp = sqlite3.connect(str(DISP))
        for p in plans:
            ek = p["entity_key"]
            row = disp.execute(
                "SELECT finding_id, status FROM audit_disposition WHERE entity_key=?",
                (ek,),
            ).fetchone()
            if not row:
                continue
            if p["action"] == "FALSE_POSITIVE_WRONG_LAST4":
                disp.execute(
                    """
                    UPDATE audit_disposition
                       SET status='FALSE_POSITIVE', resolved_by='wrong_last4_match',
                           resolved_at=?, note=?
                     WHERE finding_id=?
                    """,
                    (
                        now,
                        "Entity last4 6789 matched LEVINE/SAMPLE (incl. TAXPAYER,SPOUSE "
                        "sentinel), not MONTANEZ PABLO client 670 (last4 6693) who already "
                        "has JOANNA on spouses. Fingerprint collision FP.",
                        row[0],
                    ),
                )
            elif p["action"] == "REPLACE_CONTAM":
                disp.execute(
                    """
                    UPDATE audit_disposition
                       SET status='RESOLVED', resolved_by='contam_fix',
                           resolved_at=?, note=?
                     WHERE finding_id=?
                    """,
                    (
                        now,
                        "Lucy easy: Drake-import mis-attach EVELYN (JESUS&EVELYN) replaced "
                        "with BRIDGET HUERTA; taxpayer_name cleared.",
                        row[0],
                    ),
                )
            elif p["action"] == "REPLACE_TRUNC":
                disp.execute(
                    """
                    UPDATE audit_disposition
                       SET status='RESOLVED', resolved_by='trunc_fix',
                           resolved_at=?, note=?
                     WHERE finding_id=?
                    """,
                    (
                        now,
                        "Lucy easy: fold junk last 'B' + MARIA replaced with Drake "
                        "MARIA MORENO.",
                        row[0],
                    ),
                )
        disp.commit()
        disp.close()

    tax.close()
    payload = {
        "generated_at": now,
        "applied": apply,
        "plans": plans,
        "applied_cases": applied,
    }
    OUT.write_text(json.dumps(payload, indent=2, default=str), encoding="utf-8")
    print("applied" if apply else "dry-run", json.dumps({"n": len(plans), "cases": applied}, indent=2))
    for p in plans:
        print(p["case"], p["action"], p.get("before"), "->", p.get("after"))


if __name__ == "__main__":
    main()
