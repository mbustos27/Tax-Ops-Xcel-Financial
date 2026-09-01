"""Rebuild taxops.db by copying readable rows into a fresh database."""

from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

SRC = Path("T:/taxops/taxops.db")
DST = Path("T:/taxops/taxops_rebuilt.db")
BACKUP = Path(f"T:/taxops/taxops.db.pre_rebuild_{datetime.now():%Y%m%d_%H%M%S}")


def main() -> None:
    if DST.exists():
        DST.unlink()

    src = sqlite3.connect(SRC)
    dst = sqlite3.connect(DST)
    src.row_factory = sqlite3.Row

    schema = src.execute(
        "SELECT type, name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY rowid"
    ).fetchall()
    for typ, name, ddl in schema:
        if name.startswith("sqlite_") or "_fts" in name:
            continue
        try:
            dst.execute(ddl)
        except sqlite3.Error as exc:
            print(f"SKIP schema {typ} {name}: {exc}")
    dst.commit()

    tables = [
        t
        for (t,) in src.execute(
            """
            SELECT name FROM sqlite_master
            WHERE type='table' AND name NOT LIKE 'sqlite_%'
            ORDER BY name
            """
        ).fetchall()
        if not t.endswith("_fts") and "_fts_" not in t
    ]

    for table in tables:
        try:
            rows = src.execute(f"SELECT * FROM [{table}]").fetchall()
        except sqlite3.Error as exc:
            print(f"SKIP read {table}: {exc}")
            continue
        if not rows:
            print(f"  {table}: 0")
            continue
        cols = rows[0].keys()
        placeholders = ", ".join("?" for _ in cols)
        col_list = ", ".join(f"[{c}]" for c in cols)
        dst.executemany(
            f"INSERT INTO [{table}] ({col_list}) VALUES ({placeholders})",
            [tuple(row[c] for c in cols) for row in rows],
        )
        print(f"  {table}: {len(rows)}")

    dst.commit()
    ic = dst.execute("PRAGMA integrity_check").fetchone()[0]
    print("integrity:", ic)
    print("returns:", dst.execute("SELECT COUNT(*) FROM returns").fetchone()[0])
    dst.row_factory = sqlite3.Row
    dental = dst.execute(
        """
        SELECT r.id, r.client_status, r.drake_status_raw
        FROM returns r JOIN clients cl ON cl.id = r.client_id
        WHERE cl.last_name LIKE '%D P DENTAL%' AND r.tax_year = 2025
        """
    ).fetchone()
    print("dental:", dict(dental) if dental else None)

    src.close()
    dst.close()

    if ic != "ok":
        raise SystemExit("Rebuild failed integrity check")

    shutil.copy2(SRC, BACKUP)
    shutil.copy2(DST, SRC)
    print(f"Replaced {SRC} (backup at {BACKUP})")


if __name__ == "__main__":
    main()
