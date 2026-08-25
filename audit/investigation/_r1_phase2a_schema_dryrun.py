"""Phase 2A+2C — address columns + backfill history on throwaway DB. No live writes."""
from __future__ import annotations

import json
import sqlite3
import tempfile
from datetime import datetime, timezone
from pathlib import Path

OUT = Path(r"T:\audit\investigation\R1-phase2a-address-schema-dryrun.md")

ADDRESS_COLS = """
ALTER TABLE clients ADD COLUMN address_street TEXT;
ALTER TABLE clients ADD COLUMN address_city TEXT;
ALTER TABLE clients ADD COLUMN address_state TEXT;
ALTER TABLE clients ADD COLUMN address_zip TEXT;
ALTER TABLE clients ADD COLUMN address_county TEXT;
ALTER TABLE clients ADD COLUMN address_source TEXT;
ALTER TABLE clients ADD COLUMN address_verified_at TEXT;
"""

# Also returns.filing_status_drake (Phase 3 field map) — additive, schema-only here
RETURNS_COL = "ALTER TABLE returns ADD COLUMN filing_status_drake TEXT;"

HISTORY_DDL = """
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

PROFILE_FIELDS = (
    "taxpayer_email",
    "taxpayer_cell",
    "taxpayer_dob",
    "address_street",
    "address_city",
    "address_state",
    "address_zip",
    "address_county",
    "address_source",
    "spouse_dob",
    "spouse_cell",
)


def main() -> None:
    tmp = Path(tempfile.mkdtemp(prefix="r1_2a_")) / "throwaway.sqlite"
    conn = sqlite3.connect(str(tmp))
    conn.executescript(
        """
        CREATE TABLE clients (
          id INTEGER PRIMARY KEY,
          taxpayer_email TEXT,
          taxpayer_cell TEXT,
          taxpayer_dob TEXT,
          address TEXT,
          spouse_dob TEXT,
          spouse_cell TEXT
        );
        CREATE TABLE returns (
          id INTEGER PRIMARY KEY,
          client_id INTEGER,
          tax_year INTEGER,
          log_number TEXT,
          filing_status TEXT
        );
        """
    )
    # Simulate existing live shape: legacy address filled, structured empty after ALTER
    conn.execute(
        "INSERT INTO clients (id, taxpayer_email, taxpayer_cell, address) "
        "VALUES (7, NULL, '555-0100', '100 OLD ST')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, tax_year, log_number, filing_status) "
        "VALUES (1, 7, 2025, '250464', NULL)"
    )
    conn.commit()

    # Apply proposed migrations
    for stmt in ADDRESS_COLS.strip().split(";"):
        s = stmt.strip()
        if s:
            conn.execute(s)
    conn.execute(RETURNS_COL)
    conn.executescript(HISTORY_DDL)

    cols = [r[1] for r in conn.execute("PRAGMA table_info(clients)")]
    rcols = [r[1] for r in conn.execute("PRAGMA table_info(returns)")]
    needed = [
        "address_street",
        "address_city",
        "address_state",
        "address_zip",
        "address_county",
        "address_source",
        "address_verified_at",
    ]
    cols_ok = all(c in cols for c in needed)
    fs_ok = "filing_status_drake" in rcols
    legacy_intact = "address" in cols

    row = conn.execute(
        "SELECT taxpayer_email, taxpayer_cell, taxpayer_dob, address, "
        "address_street, address_city, address_state, address_zip, address_county, "
        "address_source, spouse_dob, spouse_cell FROM clients WHERE id=7"
    ).fetchone()
    keys = [
        "taxpayer_email",
        "taxpayer_cell",
        "taxpayer_dob",
        "address",
        "address_street",
        "address_city",
        "address_state",
        "address_zip",
        "address_county",
        "address_source",
        "spouse_dob",
        "spouse_cell",
    ]
    before = dict(zip(keys, row))

    drake = {
        "taxpayer_email": "a@example.com",
        "taxpayer_cell": "999-9999",  # must NOT overwrite
        "taxpayer_dob": "1990-01-01",
        "address_street": "2900 N EASTERN AVENUE",
        "address_city": "LOS ANGELES",
        "address_state": "CA",
        "address_zip": "90032",
        "address_county": "LOS ANGELES",
        "address_source": "drake_taxpayer_csv",
        "spouse_dob": "1992-02-02",
        "spouse_cell": "555-0200",
    }

    written = {}
    for f in PROFILE_FIELDS:
        cur = before.get(f)
        incoming = drake.get(f)
        if (cur is None or str(cur).strip() == "") and incoming:
            written[f] = incoming

    # legacy address left untouched (COALESCE into structured only)
    sets = ", ".join(f"{k}=?" for k in written)
    conn.execute(f"UPDATE clients SET {sets} WHERE id=7", list(written.values()))
    conn.execute(
        "UPDATE returns SET filing_status_drake=? WHERE id=1", ("2",)
    )

    after_row = conn.execute(
        "SELECT taxpayer_email, taxpayer_cell, taxpayer_dob, address, "
        "address_street, address_city, address_state, address_zip, address_county, "
        "address_source, spouse_dob, spouse_cell FROM clients WHERE id=7"
    ).fetchone()
    after = dict(zip(keys, after_row))

    applied_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn.execute(
        """
        INSERT INTO client_profile_backfill_history (
          client_id, run_label, applied_at, bare_log_number, invoice_number,
          source_export_sha256, fields_written_json, before_json, after_json
        ) VALUES (?,?,?,?,?,?,?,?,?)
        """,
        (
            7,
            "r1-2a-dryrun",
            applied_at,
            464,
            "250464",
            "33bcda025ef8a4db1e7b189da13dbebbd7c7ea170d2f9c5f5220a6ca6cf529c8",
            json.dumps(written, sort_keys=True),
            json.dumps(before, sort_keys=True),
            json.dumps(after, sort_keys=True),
        ),
    )
    conn.commit()

    snap = json.loads(
        conn.execute(
            "SELECT before_json FROM client_profile_backfill_history WHERE client_id=7"
        ).fetchone()[0]
    )
    restore_cols = [k for k in snap if k in keys]
    conn.execute(
        f"UPDATE clients SET {', '.join(f'{k}=?' for k in restore_cols)} WHERE id=7",
        [snap[k] for k in restore_cols],
    )
    restored = dict(
        zip(
            keys,
            conn.execute(
                "SELECT taxpayer_email, taxpayer_cell, taxpayer_dob, address, "
                "address_street, address_city, address_state, address_zip, "
                "address_county, address_source, spouse_dob, spouse_cell "
                "FROM clients WHERE id=7"
            ).fetchone(),
        )
    )

    checks = {
        "address_cols_added": cols_ok,
        "filing_status_drake_added": fs_ok,
        "legacy_address_intact": legacy_intact and after["address"] == "100 OLD ST",
        "coalesce_skip_filled_cell": after["taxpayer_cell"] == "555-0100"
        and "taxpayer_cell" not in written,
        "coalesce_fill_email": after["taxpayer_email"] == "a@example.com",
        "coalesce_fill_street": after["address_street"] == "2900 N EASTERN AVENUE",
        "coalesce_fill_zip": after["address_zip"] == "90032",
        "reconstruct": restored == before,
    }
    overall = all(checks.values())

    lines = [
        "# R1 Phase 2A + 2C — address schema dry-run",
        "",
        f"Throwaway: `{tmp}`",
        "",
        "## Proposed DDL (schema v28 candidate)",
        "",
        "```sql",
        ADDRESS_COLS.strip(),
        RETURNS_COL,
        HISTORY_DDL.strip(),
        "```",
        "",
        "## Simulated COALESCE (structured address; legacy `address` untouched)",
        "",
        f"- before: `{json.dumps(before)}`",
        f"- written: `{json.dumps(written)}`",
        f"- after: `{json.dumps(after)}`",
        "",
        "## Acceptance",
        "",
        "| Check | Result |",
        "|---|---|",
    ]
    for k, v in checks.items():
        lines.append(f"| `{k}` | {'PASS' if v else 'FAIL'} |")
    lines += [
        "",
        f"**Overall:** {'PASS' if overall else 'FAIL'}",
        "",
        "## Live notes",
        "",
        "- Do **not** apply to `taxops.db` until Phase 3 approved.",
        "- `CURRENT_SCHEMA_VERSION` is **27**; this bundle is **v28** "
        "(address cols + `filing_status_drake` + `client_profile_backfill_history`).",
        "- Wire via `_migrate_existing_tables()` with `ALTER TABLE … ADD COLUMN` defaults.",
        "- Phase 0.1 gate cleared by `TAXPAYER.csv` sha `33bcda02…`.",
        "",
    ]
    OUT.write_text("\n".join(lines), encoding="utf-8")
    print("PASS" if overall else "FAIL", checks, "->", OUT)
    conn.close()


if __name__ == "__main__":
    main()
