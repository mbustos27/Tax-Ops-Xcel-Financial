"""COMPLIANCE-0 M0: rollback path for the Compliance Tracker schema.

Drops the tables added for the Compliance Tracker (compliance_clients,
compliance_credentials, compliance_accounts, compliance_filing_periods,
compliance_correspondence_log) and their indexes. Does NOT touch any other
TaxOps table — in particular the existing `clients`/`auth_users` tables are
completely untouched, since compliance_clients is a deliberately separate
table (see db.py's COMPLIANCE-0 comment) that only *references* auth_users
(FK), never modifies it.

WARNING: compliance_credentials.encrypted_password rows are the ONLY copy of
those portal passwords TaxOps holds (there is no plaintext backup by
design). Dropping this table is equivalent to permanently losing every
migrated/entered CDTFA and city-portal login. Export/back up the DB file
first if there is any chance you'll want this data back.

This does NOT restore CURRENT_SCHEMA_VERSION in db.py — that's a code
change, not a data change; only run this if you are also reverting the
code that added these tables.

Usage (from taxops/ directory, ideally against a DB COPY first):

    python scripts\\rollback_compliance_schema.py            # dry run (default)
    python scripts\\rollback_compliance_schema.py --confirm   # actually drop
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, ".")

from db import get_connection  # noqa: E402

# Child-first order: filing_periods/correspondence reference accounts/clients,
# accounts reference clients/credentials, so drop in this order even though
# every FK is already ON DELETE CASCADE from the parent side.
_TABLES = (
    "compliance_filing_periods",
    "compliance_correspondence_log",
    "compliance_accounts",
    "compliance_credentials",
    "compliance_clients",
)
_INDEXES = (
    "idx_compliance_clients_name",
    "idx_compliance_clients_active",
    "idx_compliance_credentials_shared",
    "idx_compliance_accounts_client",
    "idx_compliance_accounts_type",
    "idx_compliance_accounts_credential",
    "ux_compliance_filing_periods_account_label",
    "idx_compliance_filing_periods_status",
    "idx_compliance_filing_periods_due",
    "idx_compliance_correspondence_client_month",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", action="store_true", help="Actually drop the tables (default: dry run)")
    args = parser.parse_args()

    conn = get_connection()
    try:
        placeholders = ",".join("?" * len(_TABLES))
        existing = {
            row["name"]
            for row in conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
                _TABLES,
            ).fetchall()
        }
        counts = {}
        for t in _TABLES:
            if t in existing:
                counts[t] = conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"]

        print("Compliance Tracker tables found:")
        for t in _TABLES:
            status = f"{counts[t]} row(s)" if t in existing else "(not present)"
            print(f"  {t}: {status}")

        cred_rows = counts.get("compliance_credentials", 0)
        if cred_rows:
            print(
                f"\n*** WARNING: compliance_credentials has {cred_rows} row(s). "
                "Dropping it permanently loses those encrypted portal "
                "passwords — there is no other copy. ***"
            )

        if not args.confirm:
            print("\nDry run only — pass --confirm to actually drop these tables.")
            return 0

        for t in _TABLES:  # already child-first order
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        for idx in _INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
        conn.commit()
        print(
            "\nDropped compliance_clients, compliance_credentials, "
            "compliance_accounts, compliance_filing_periods, "
            "compliance_correspondence_log and their indexes."
        )
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
