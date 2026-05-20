"""Print latest w2_records row(s) for spot-checking box columns (run: python taxops/scripts/verify_w2.py)."""
from __future__ import annotations

import json
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from config import DB_PATH
from db import get_connection

BOX_KEYS = [
    "employer_name",
    "tax_year",
    "box1_wages_tips_other",
    "box2_federal_income_tax_withheld",
    "box3_social_security_wages",
    "box4_social_security_tax_withheld",
    "box5_medicare_wages_tips",
    "box6_medicare_tax_withheld",
    "box15_state",
    "box16_state_wages",
    "box17_state_income_tax",
]


def main() -> None:
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, return_id, doc_id, source, is_deleted,
                   employer_name, tax_year,
                   box1_wages_tips_other, box2_federal_income_tax_withheld,
                   box3_social_security_wages, box4_social_security_tax_withheld,
                   box5_medicare_wages_tips, box6_medicare_tax_withheld,
                   box15_state, box16_state_wages, box17_state_income_tax
            FROM w2_records
            WHERE COALESCE(is_deleted, 0) = 0
            ORDER BY id DESC
            LIMIT 5
            """
        ).fetchall()
        if not rows:
            print("No w2_records rows found.")
            return
        print(f"Showing up to {len(rows)} newest w2_records (non-deleted):\n")
        for r in rows:
            d = dict(r)
            rid = d.pop("id", None)
            summary = {k: d.get(k) for k in BOX_KEYS}
            nonempty = sum(1 for k in BOX_KEYS if d.get(k) not in (None, ""))
            print(f"id={rid} return_id={d.get('return_id')} doc_id={d.get('doc_id')} source={d.get('source')}")
            print(f"  populated_box_fields={nonempty}/{len(BOX_KEYS)}")
            print(f"  {json.dumps(summary, indent=4, default=str)}")
            print()
    finally:
        conn.close()


if __name__ == "__main__":
    main()
