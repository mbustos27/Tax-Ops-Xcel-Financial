"""Wave 3 — mark Group C DUPLICATE_CLIENT + PREFILL_STUB_BURST as FALSE_POSITIVE."""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DISP = Path(r"T:\audit\audit_disposition.sqlite")
NOTE = (
    "Wave 3: Group C different last4 + newer stub has no returns — distinct people "
    "(not ITIN→SSN handoff). Prefill burst correctly declined merge."
)


def main() -> None:
    conn = sqlite3.connect(str(DISP))
    conn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    updated = []

    # PREFILL_STUB_BURST cohort
    rows = list(
        conn.execute(
            "SELECT finding_id, finding_type, entity_key, status FROM audit_disposition "
            "WHERE finding_type='PREFILL_STUB_BURST' AND status IN ('OPEN','ACKED')"
        )
    )
    for r in rows:
        conn.execute(
            """
            UPDATE audit_disposition
               SET status='FALSE_POSITIVE', resolved_by='wave3', resolved_at=?,
                   note=?
             WHERE finding_id=?
            """,
            (now, NOTE, r["finding_id"]),
        )
        updated.append({"finding_id": r["finding_id"], "type": r["finding_type"], "entity": r["entity_key"]})

    # Group C duplicate keys — match by entity_key containing both names / last4 pairs
    group_c_tokens = [
        "HERNANDEZ",
        "ISMAEL",
        "NUNO",
        "JUAN",
        "ALVARADO",
        "OSCAR",
        "LUNA",
        "ESTEBAN",
        "VALDEZ",
        "SANDRA",
        "ABEL",
        "TASHAYOD",
        "ALEX",
        "SOLOMON",
        "LAUREN",
    ]
    # Safer: mark DUPLICATE_CLIENT OPEN rows whose sample_detail mentions both ids
    pair_ids = [
        (659, 2275),
        (731, 2276),
        (1215, 2272),
        (1278, 2393),
        (1344, 2404),
        (1440, 2273),
        (1506, 2329),
        (1709, 2429),
    ]
    dups = list(
        conn.execute(
            "SELECT finding_id, finding_type, entity_key, status, sample_detail "
            "FROM audit_disposition WHERE finding_type='DUPLICATE_CLIENT' "
            "AND status IN ('OPEN','ACKED')"
        )
    )
    for r in dups:
        blob = f"{r['entity_key'] or ''}\n{r['sample_detail'] or ''}"
        hit = False
        for a, b in pair_ids:
            if str(a) in blob and str(b) in blob:
                hit = True
                break
            # name-key style without ids
        if not hit:
            # entity keys like HERNANDEZ|ISMAEL
            ek = (r["entity_key"] or "").upper()
            for key in (
                "HERNANDEZ|ISMAEL",
                "NUNO|JUAN",
                "ALVARADO|OSCAR",
                "LUNA|ESTEBAN",
                "VALDEZ|SANDRA",
                "HERNANDEZ|ABEL",
                "TASHAYOD|ALEX",
                "SOLOMON|LAUREN",
            ):
                if key.replace("|", "") in ek.replace("|", "").replace(" ", "") or key in ek:
                    hit = True
                    break
                parts = key.split("|")
                if all(p in ek for p in parts):
                    hit = True
                    break
        if hit:
            conn.execute(
                """
                UPDATE audit_disposition
                   SET status='FALSE_POSITIVE', resolved_by='wave3', resolved_at=?,
                       note=?
                 WHERE finding_id=?
                """,
                (now, NOTE, r["finding_id"]),
            )
            updated.append(
                {"finding_id": r["finding_id"], "type": r["finding_type"], "entity": r["entity_key"]}
            )

    conn.commit()
    out = Path(r"T:\audit\investigation\W3-disposition.json")
    out.write_text(json.dumps({"updated": updated, "n": len(updated)}, indent=2), encoding="utf-8")
    print(f"FALSE_POSITIVE marked: {len(updated)}")
    for u in updated:
        print(" ", u["type"], u["entity"])
    print("wrote", out)
    conn.close()


if __name__ == "__main__":
    main()
