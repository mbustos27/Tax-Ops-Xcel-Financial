"""Wave 2A acceptance: dry-run merge on a throwaway copy of taxops.db (never production)."""
from __future__ import annotations

import json
import shutil
import sys
import tempfile
from pathlib import Path

ROOT = Path(r"T:\taxops")
sys.path.insert(0, str(ROOT))

from db import CURRENT_SCHEMA_VERSION, get_connection, init_db  # noqa: E402
from merge_ops import merge_client_into  # noqa: E402
from utils import now  # noqa: E402


def main() -> None:
    src = ROOT / "taxops.db"
    tmpdir = Path(tempfile.mkdtemp(prefix="w2a_merge_"))
    dst = tmpdir / "taxops_copy.db"
    shutil.copy2(src, dst)
    for suf in ("-wal", "-shm"):
        p = Path(str(src) + suf)
        if p.exists():
            shutil.copy2(p, Path(str(dst) + suf))

    conn = get_connection(str(dst))
    init_db(conn)
    ver = conn.execute(
        "SELECT value FROM app_settings WHERE key='schema_version'"
    ).fetchone()
    print("schema_version", ver[0] if ver else None, "expected", CURRENT_SCHEMA_VERSION)
    has = conn.execute(
        "SELECT name FROM sqlite_master WHERE name='client_merge_history'"
    ).fetchone()
    print("table", has[0] if has else None)

    ts = now()
    conn.execute(
        "INSERT INTO clients (last_name, first_name, ssn_last4, created_at, updated_at) "
        "VALUES (?,?,?,?,?)",
        ("W2AKEEP", "Trail", "9991", ts, ts),
    )
    keep_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        "INSERT INTO clients (last_name, first_name, taxpayer_phone, address, "
        "created_at, updated_at) VALUES (?,?,?,?,?,?)",
        ("W2ADROP", "Trail", "555-9999", "99 Test Ln", ts, ts),
    )
    discard_id = int(conn.execute("SELECT last_insert_rowid()").fetchone()[0])
    conn.execute(
        "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
        "created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (keep_id, "W2A1", 2025, "LOG OUT", ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
        "created_at, updated_at) VALUES (?,?,?,?,?,?)",
        (discard_id, "W2A2", 2024, "PROCESSING", ts, ts),
    )
    conn.commit()

    hist_id = merge_client_into(
        conn,
        keep_id,
        discard_id,
        ts,
        operator="wave2a-dry",
        reason_code="throwaway_copy",
        note="acceptance",
    )
    conn.commit()
    row = dict(
        conn.execute(
            "SELECT * FROM client_merge_history WHERE id=?", (hist_id,)
        ).fetchone()
    )
    snap = json.loads(row["discard_client_json"])
    assert snap["last_name"] == "W2ADROP"
    assert snap["taxpayer_phone"] == "555-9999"
    assert (
        conn.execute(
            "SELECT COUNT(*) FROM clients WHERE id=?", (discard_id,)
        ).fetchone()[0]
        == 0
    )
    print(
        "DRY_RUN_OK",
        "history_id",
        hist_id,
        "discard_reconstructed",
        snap["last_name"],
        snap["first_name"],
        "phone",
        snap["taxpayer_phone"],
    )
    print("copy_db", dst)
    conn.close()


if __name__ == "__main__":
    main()
