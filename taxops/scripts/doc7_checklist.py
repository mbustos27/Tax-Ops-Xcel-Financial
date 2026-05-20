"""One-off DOC-7 schema verification (run from repo root: python taxops/scripts/doc7_checklist.py)."""
import sqlite3
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(_ROOT))
from config import DB_PATH

def main() -> None:
    print("DB_PATH", DB_PATH)
    db = sqlite3.connect(DB_PATH)
    db.row_factory = sqlite3.Row

    tables = [
        r[0]
        for r in db.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    ]

    required_tables = [
        "w2_records",
        "f1099_nec_records",
        "f1099_misc_records",
        "f1099_int_records",
        "f1099_div_records",
    ]
    for t in required_tables:
        ok = t in tables
        print(f"{t}: {'PASS' if ok else 'FAIL — TABLE MISSING'}")

    checks = {
        "w2_records": [
            "box1_wages_tips_other",
            "box2_federal_income_tax_withheld",
            "box15_state",
            "box16_state_wages",
            "box17_state_income_tax",
            "employer_name",
            "tax_year",
            "source",
            "is_deleted",
        ],
        "f1099_nec_records": [
            "box1_nonemployee_compensation",
            "box4_federal_income_tax_withheld",
            "box5_state_tax_withheld",
            "box6_state",
            "box7_state_income",
            "payer_name",
            "tax_year",
            "source",
            "is_deleted",
        ],
        "f1099_misc_records": [
            "box1_rents",
            "box2_royalties",
            "box3_other_income",
            "box4_federal_income_tax_withheld",
            "box16_state_tax_withheld",
            "payer_name",
            "tax_year",
            "source",
            "is_deleted",
        ],
        "f1099_int_records": [
            "box1_interest_income",
            "box4_federal_income_tax_withheld",
            "box8_tax_exempt_interest",
            "box17_state_tax_withheld",
            "payer_name",
            "tax_year",
            "source",
            "is_deleted",
        ],
        "f1099_div_records": [
            "box1a_total_ordinary_dividends",
            "box1b_qualified_dividends",
            "box2a_total_capital_gain",
            "box4_federal_income_tax_withheld",
            "box16_state_tax_withheld",
            "payer_name",
            "tax_year",
            "source",
            "is_deleted",
        ],
    }

    for table, required_cols in checks.items():
        if table not in tables:
            print(f"  {table}: SKIP (table missing)")
            continue
        cols = [
            r[1] for r in db.execute(f"PRAGMA table_info({table})").fetchall()
        ]
        for col in required_cols:
            status = "PASS" if col in cols else "FAIL — COLUMN MISSING"
            print(f"  {table}.{col}: {status}")
        if "ssn" in cols:
            raise AssertionError(f"SSN column found in {table}")
        if "ein" in cols:
            raise AssertionError(f"EIN column found in {table}")
        print(f"  {table} SSN/EIN check: PASS")

    db.close()


if __name__ == "__main__":
    main()
