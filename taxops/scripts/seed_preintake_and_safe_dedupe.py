"""Ops one-shot: backup DB, seed active-year preintake, run safe name+SSN dedupe."""

from __future__ import annotations

import shutil
import sys
from datetime import datetime
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from db import get_connection, get_active_intake_tax_year, _deduplicate_existing_records
from season_rollover import RolloverCarryOptions, seed_preintake_commit, seed_preintake_preview
from utils import now


def main() -> None:
    db_path = Path(__import__("os").environ.get("TAXOPS_DB") or "taxops.db")
    if not db_path.is_file():
        raise SystemExit(f"DB not found: {db_path}")

    backup = db_path.with_name(
        db_path.name + ".backup." + datetime.now().strftime("%Y%m%d_%H%M%S")
    )
    shutil.copy2(db_path, backup)
    print("backup", backup)

    conn = get_connection(str(db_path))
    active = get_active_intake_tax_year(conn)
    print("active_intake_tax_year", active)

    prev = seed_preintake_preview(conn, target_year=active)
    print("preintake_preview", prev.get("totals"))
    out = seed_preintake_commit(
        conn,
        target_year=active,
        options=RolloverCarryOptions(),
        actor="ops_script",
        ts=now(),
    )
    print("preintake_created", out.get("totals"))

    conn.execute("BEGIN")
    removed = _deduplicate_existing_records(conn)
    conn.commit()
    print("safe_dedupe_removed", removed)

    pending = conn.execute(
        "SELECT COUNT(*) c FROM returns WHERE tax_year=? AND client_status='PENDING INTAKE'",
        (active,),
    ).fetchone()["c"]
    print("pending_intake_ty", active, pending)

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
    conn.close()


if __name__ == "__main__":
    main()
