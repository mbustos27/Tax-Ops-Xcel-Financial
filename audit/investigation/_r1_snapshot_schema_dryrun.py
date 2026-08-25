"""Phase 2C — client_profile_backfill_history schema + throwaway reconstruct dry-run.

Does NOT write to live taxops.db. Creates throwaway DB, applies proposed DDL,
simulates one COALESCE write with snapshot, verifies reconstruct.
"""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(r"T:\audit\investigation\R1-phase2c-snapshot-dryrun.md")

DDL = """
CREATE TABLE IF NOT EXISTS client_profile_backfill_history (
  id                    INTEGER PRIMARY KEY AUTOINCREMENT,
  client_id             INTEGER NOT NULL,
  run_label             TEXT NOT NULL,
  applied_at            TEXT NOT NULL,
  bare_log_number       INTEGER NOT NULL,
  invoice_number        TEXT NOT NULL,
  source_export_sha256  TEXT,
  fields_written_json   TEXT NOT NULL,
  before_json           TEXT NOT NULL,
  after_json            TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cpbh_client
  ON client_profile_backfill_history(client_id, applied_at);
CREATE INDEX IF NOT EXISTS idx_cpbh_run
  ON client_profile_backfill_history(run_label);
"""

# Fields Phase 3 would COALESCE (address cols deferred until Phase 0.1)
FIELDS = (
    "taxpayer_email",
    "spouse_email",
    "taxpayer_cell",
    "spouse_cell",
    "taxpayer_dob",
    "spouse_dob",
)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="r1_snapshot_")) / "throwaway.sqlite"
    conn = sqlite3.connect(str(tmp))
    conn.executescript(
        """
        CREATE TABLE clients (
          id INTEGER PRIMARY KEY,
          taxpayer_email TEXT,
          spouse_email TEXT,
          taxpayer_cell TEXT,
          spouse_cell TEXT,
          taxpayer_dob TEXT,
          spouse_dob TEXT
        );
        """
    )
    conn.executescript(DDL)

    # Seed: empty email, has cell (COALESCE must not overwrite cell)
    conn.execute(
        "INSERT INTO clients VALUES (42, NULL, NULL, '555-0100', NULL, NULL, NULL)"
    )
    conn.commit()

    before = dict(
        conn.execute(
            "SELECT taxpayer_email, spouse_email, taxpayer_cell, spouse_cell, "
            "taxpayer_dob, spouse_dob FROM clients WHERE id=42"
        ).fetchone()
        and zip(
            FIELDS,
            conn.execute(
                "SELECT taxpayer_email, spouse_email, taxpayer_cell, spouse_cell, "
                "taxpayer_dob, spouse_dob FROM clients WHERE id=42"
            ).fetchone(),
        )
    )
    # clearer
    row = conn.execute(
        "SELECT taxpayer_email, spouse_email, taxpayer_cell, spouse_cell, "
        "taxpayer_dob, spouse_dob FROM clients WHERE id=42"
    ).fetchone()
    before = dict(zip(FIELDS, row))

    # Simulated Drake payload for this client
    drake = {
        "taxpayer_email": "new@example.com",
        "spouse_email": "spouse@example.com",
        "taxpayer_cell": "999-9999",  # must NOT overwrite existing 555-0100
        "spouse_cell": "555-0200",
        "taxpayer_dob": "1990-01-01",
        "spouse_dob": None,
    }

    written = {}
    for f in FIELDS:
        cur = before[f]
        incoming = drake.get(f)
        if (cur is None or str(cur).strip() == "") and incoming:
            written[f] = incoming

    sets = ", ".join(f"{k}=?" for k in written)
    conn.execute(
        f"UPDATE clients SET {sets} WHERE id=42",
        list(written.values()),
    )
    after_row = conn.execute(
        "SELECT taxpayer_email, spouse_email, taxpayer_cell, spouse_cell, "
        "taxpayer_dob, spouse_dob FROM clients WHERE id=42"
    ).fetchone()
    after = dict(zip(FIELDS, after_row))

    applied_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO client_profile_backfill_history (
          client_id, run_label, applied_at, bare_log_number, invoice_number,
          source_export_sha256, fields_written_json, before_json, after_json
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            42,
            "r1-dryrun-2026-04-13",
            applied_at,
            464,
            "250464",
            "024cc87af2275246682ca4e4498ddf708b151ab293051cddb36743829e87b415",
            json.dumps(written, sort_keys=True),
            json.dumps(before, sort_keys=True),
            json.dumps(after, sort_keys=True),
        ),
    )
    conn.commit()

    # Reconstruct: apply before_json over current row
    hist = conn.execute(
        "SELECT before_json, fields_written_json FROM client_profile_backfill_history "
        "WHERE client_id=42 ORDER BY id DESC LIMIT 1"
    ).fetchone()
    snap_before = json.loads(hist[0])
    fields_written = json.loads(hist[1])
    restore_sets = ", ".join(f"{k}=?" for k in snap_before)
    conn.execute(
        f"UPDATE clients SET {restore_sets} WHERE id=42",
        [snap_before[k] for k in snap_before],
    )
    restored = dict(
        zip(
            FIELDS,
            conn.execute(
                "SELECT taxpayer_email, spouse_email, taxpayer_cell, spouse_cell, "
                "taxpayer_dob, spouse_dob FROM clients WHERE id=42"
            ).fetchone(),
        )
    )
    ok = restored == before
    cell_preserved = after["taxpayer_cell"] == "555-0100"
    wrote_email = after["taxpayer_email"] == "new@example.com"
    wrote_spouse_cell = after["spouse_cell"] == "555-0200"
    did_not_overwrite_cell = "taxpayer_cell" not in fields_written

    lines = [
        "# R1 Phase 2C — snapshot schema dry-run",
        "",
        f"Throwaway DB: `{tmp}`",
        "",
        "## Proposed DDL (schema v28 candidate)",
        "",
        "```sql",
        DDL.strip(),
        "```",
        "",
        "## Simulated COALESCE",
        "",
        f"- before: `{json.dumps(before)}`",
        f"- drake: `{json.dumps(drake)}`",
        f"- fields_written: `{json.dumps(written)}`",
        f"- after: `{json.dumps(after)}`",
        "",
        "## Acceptance checks",
        "",
        f"| Check | Result |",
        f"|---|---|",
        f"| COALESCE skipped filled taxpayer_cell | {'PASS' if did_not_overwrite_cell and cell_preserved else 'FAIL'} |",
        f"| COALESCE filled empty taxpayer_email | {'PASS' if wrote_email else 'FAIL'} |",
        f"| COALESCE filled empty spouse_cell | {'PASS' if wrote_spouse_cell else 'FAIL'} |",
        f"| Reconstruct from before_json restores pre-state | {'PASS' if ok else 'FAIL'} |",
        "",
        f"**Overall:** {'PASS' if ok and cell_preserved and wrote_email and did_not_overwrite_cell else 'FAIL'}",
        "",
        "## Live taxops notes",
        "",
        "- Do **not** apply this DDL to live until Phase 3 is approved.",
        "- Mirror pattern: `client_merge_history` (schema v25) in `taxops/db.py`.",
        "- `CURRENT_SCHEMA_VERSION` is currently **27**; this would be **v28**.",
        "- Address structured columns still blocked on Phase 0.1 City/State/ZIP re-export.",
        "",
    ]
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print(
        "PASS" if ok and cell_preserved and wrote_email else "FAIL",
        "written",
        written,
        "->",
        OUT,
    )
    conn.close()


if __name__ == "__main__":
    main()
