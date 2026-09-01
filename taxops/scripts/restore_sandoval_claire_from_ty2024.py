"""Enrich SANDOVAL, RUDY from TY2024 spouse-info CSV and add CLAIRE as spouse."""

from __future__ import annotations

import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from db import get_connection
from utils import now

# From Desktop\TY2024S.csv:
# RUDY & CLAIRE SANDOVAL,03/02/1967,3237753567,CLAIRE SANDOVAL,3239979126,06/22/1965
CLIENT_ID = 2460
RETURN_ID = 2871
SOURCE = "TY2024S.csv spouse-info"


def _iso_dob(mmddyyyy: str) -> str:
    m, d, y = mmddyyyy.strip().split("/")
    return f"{int(y):04d}-{int(m):02d}-{int(d):02d}"


def main(*, apply: bool) -> None:
    rudy_dob = _iso_dob("03/02/1967")
    claire_dob = _iso_dob("06/22/1965")
    claire_cell = "3239979126"

    conn = get_connection()
    try:
        before = conn.execute("SELECT * FROM clients WHERE id=?", (CLIENT_ID,)).fetchone()
        if not before:
            raise SystemExit(f"client {CLIENT_ID} not found")
        print("before:", dict(before))

        if not apply:
            print(
                f"DRY-RUN would set taxpayer_dob={rudy_dob}, "
                f"spouse CLAIRE SANDOVAL dob={claire_dob} cell={claire_cell}"
            )
            return

        conn.execute(
            """
            UPDATE clients SET
              taxpayer_dob = COALESCE(NULLIF(taxpayer_dob,''), ?),
              spouse_first_name = COALESCE(NULLIF(spouse_first_name,''), 'CLAIRE'),
              spouse_last_name  = COALESCE(NULLIF(spouse_last_name,''), 'SANDOVAL'),
              spouse_dob = COALESCE(NULLIF(spouse_dob,''), ?),
              spouse_cell = COALESCE(NULLIF(spouse_cell,''), ?),
              taxpayer_cell = COALESCE(NULLIF(taxpayer_cell,''), '3237753567'),
              updated_at = ?
            WHERE id = ?
            """,
            (rudy_dob, claire_dob, claire_cell, now(), CLIENT_ID),
        )

        existing = conn.execute(
            "SELECT id FROM spouses WHERE client_id=? AND upper(first_name)='CLAIRE'",
            (CLIENT_ID,),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE spouses SET
                  last_name = 'SANDOVAL',
                  date_of_birth = ?,
                  source = ?,
                  needs_review = 0
                WHERE id = ?
                """,
                (claire_dob, SOURCE, existing["id"]),
            )
            print(f"updated spouses.id={existing['id']}")
        else:
            cur = conn.execute(
                """
                INSERT INTO spouses (
                  client_id, last_name, first_name, date_of_birth,
                  source, match_confidence, needs_review, confirmed_at_intake,
                  created_at, taxpayer_name
                ) VALUES (?, 'SANDOVAL', 'CLAIRE', ?, ?, 1.0, 0, 0, ?, ?)
                """,
                (
                    CLIENT_ID,
                    claire_dob,
                    SOURCE,
                    now(),
                    "RUDY & CLAIRE SANDOVAL",
                ),
            )
            print(f"inserted spouses.id={cur.lastrowid}")

        conn.execute(
            "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,?,?)",
            (
                RETURN_ID,
                f"Spouse CLAIRE SANDOVAL restored from {SOURCE} "
                f"(Claire DOB {claire_dob}, cell {claire_cell}; Rudy DOB {rudy_dob}).",
                SOURCE,
                now(),
            ),
        )
        conn.commit()
        after = conn.execute("SELECT * FROM clients WHERE id=?", (CLIENT_ID,)).fetchone()
        print("after:", {k: after[k] for k in after.keys() if after[k] is not None})
        sp = conn.execute("SELECT * FROM spouses WHERE client_id=?", (CLIENT_ID,)).fetchall()
        print("spouses:", [dict(s) for s in sp])
    finally:
        conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
