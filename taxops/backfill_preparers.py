"""
One-time: normalize existing returns.processor values in the DB (LY / MB / aliases).
Run from taxops/:  python backfill_preparers.py
"""
from __future__ import annotations

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from db import get_connection, init_db
from preparer import normalize_preparer
from utils import now


def main() -> None:
    conn = get_connection()
    init_db(conn)
    rows = conn.execute(
        "SELECT id, processor FROM returns WHERE processor IS NOT NULL AND TRIM(processor) != ''"
    ).fetchall()
    n = 0
    for r in rows:
        old = (r["processor"] or "").strip()
        new = normalize_preparer(old) or old
        if new != (r["processor"] or ""):
            conn.execute(
                "UPDATE returns SET processor = ?, updated_at = ? WHERE id = ?",
                (new, now(), r["id"]),
            )
            n += 1
            print(f"  id={r['id']}: {r['processor']!r} -> {new!r}")
    conn.commit()
    conn.close()
    print(f"Done. Updated {n} of {len(rows)} non-empty rows.")


if __name__ == "__main__":
    main()
