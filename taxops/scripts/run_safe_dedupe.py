"""Resume safe client dedupe after preintake seed (backup first)."""

from __future__ import annotations

import shutil
from datetime import datetime
from pathlib import Path

from db import get_connection, get_active_intake_tax_year, _deduplicate_existing_records


def main() -> None:
    db_path = Path(__import__("os").environ.get("TAXOPS_DB") or "taxops.db")
    backup = db_path.with_name(
        db_path.name + ".backup." + datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    shutil.copy2(db_path, backup)
    print("backup", backup)

    conn = get_connection(str(db_path))
    active = get_active_intake_tax_year(conn)
    print("active", active)
    print(
        "pending_before",
        conn.execute(
            "SELECT COUNT(*) c FROM returns WHERE tax_year=? AND client_status='PENDING INTAKE'",
            (active,),
        ).fetchone()["c"],
    )

    conn.execute("BEGIN")
    removed = _deduplicate_existing_records(conn)
    conn.commit()
    print("safe_dedupe_removed", removed)

    exact = conn.execute(
        """
        SELECT COUNT(*) c FROM (
          SELECT lower(trim(last_name)), lower(trim(first_name))
          FROM clients
          WHERE last_name IS NOT NULL AND first_name IS NOT NULL
          GROUP BY 1,2 HAVING COUNT(*)>1
        )
        """
    ).fetchone()["c"]
    print("exact_name_dup_groups_remaining", exact)
    print(
        "clients_total",
        conn.execute("SELECT COUNT(*) c FROM clients").fetchone()["c"],
    )
    # conflicting SSN exact groups remaining
    conflict = conn.execute(
        """
        SELECT COUNT(*) c FROM (
          SELECT 1 FROM clients
          WHERE last_name IS NOT NULL AND first_name IS NOT NULL
          GROUP BY lower(trim(last_name)), lower(trim(first_name))
          HAVING COUNT(*)>1
             AND COUNT(DISTINCT CASE WHEN ssn_last4 IS NOT NULL AND trim(ssn_last4)!='' THEN ssn_last4 END) > 1
        )
        """
    ).fetchone()["c"]
    print("exact_name_ssn_conflict_groups", conflict)
    conn.close()


if __name__ == "__main__":
    main()
