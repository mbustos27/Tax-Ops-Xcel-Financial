"""
IRS-oriented form tables — INSERT column lists, INTEGER identifiers, ALTER specs.

Keeps DOC-7 DB migrations and SAVE paths aligned without circular imports.

Legacy columns kept for older rows / DOC-4 field names — never renamed or dropped.
"""

from __future__ import annotations

# Columns allowed for INSERT/UPDATE JSON body (excluding id/return_id/doc_id/source/metadata)
FORM_TABLE_INSERT_COLUMNS: dict[str, list[str]] = {
    "w2_records": [
        "employer_name",
        "employer_address",
        "box1_wages_tips_other",
        "box2_federal_income_tax_withheld",
        "box3_social_security_wages",
        "box4_social_security_tax_withheld",
        "box5_medicare_wages_tips",
        "box6_medicare_tax_withheld",
        "box7_social_security_tips",
        "box8_allocated_tips",
        "box10_dependent_care_benefits",
        "box11_nonqualified_plans",
        "box12a_code",
        "box12a_amount",
        "box12b_code",
        "box12b_amount",
        "box12c_code",
        "box12c_amount",
        "box12d_code",
        "box12d_amount",
        "box13_statutory_employee",
        "box13_retirement_plan",
        "box13_third_party_sick_pay",
        "box14_other",
        "box15_state",
        "box16_state_wages",
        "box17_state_income_tax",
        "box18_local_wages",
        "box19_local_income_tax",
        "box20_locality_name",
        "box15b_state",
        "box16b_state_wages",
        "box17b_state_income_tax",
        "box18b_local_wages",
        "box19b_local_income_tax",
        "box20b_locality_name",
        "tax_year",
    ],
    "f1099_nec_records": [
        "payer_name",
        "payer_address",
        "box1_nonemployee_compensation",
        "box2_direct_sales_indicator",
        "box4_federal_income_tax_withheld",
        "box5_state_tax_withheld",
        "box6_state",
        "box7_state_income",
        "tax_year",
        # legacy
        "nonemployee_compensation",
        "federal_income_tax_withheld",
    ],
    "f1099_misc_records": [
        "payer_name",
        "payer_address",
        "box1_rents",
        "box2_royalties",
        "box3_other_income",
        "box4_federal_income_tax_withheld",
        "box5_fishing_boat_proceeds",
        "box6_medical_health_care_payments",
        "box7_direct_sales_indicator",
        "box8_substitute_payments",
        "box9_crop_insurance_proceeds",
        "box10_gross_proceeds_attorney",
        "box11_fish_purchased_resale",
        "box12_section_409a_deferrals",
        "box14_gross_proceeds_attorney",
        "box15_section_409a_income",
        "box16_state_tax_withheld",
        "box17_state",
        "box18_state_income",
        "tax_year",
        # legacy
        "rents",
        "royalties",
        "other_income",
        "federal_income_tax_withheld",
    ],
    "f1099_int_records": [
        "payer_name",
        "payer_address",
        "box1_interest_income",
        "box2_early_withdrawal_penalty",
        "box3_us_savings_bond_treasury_interest",
        "box4_federal_income_tax_withheld",
        "box5_investment_expenses",
        "box6_foreign_tax_paid",
        "box7_foreign_country",
        "box8_tax_exempt_interest",
        "box9_specified_private_activity_bond_interest",
        "box10_market_discount",
        "box11_bond_premium",
        "box12_bond_premium_treasury_obligations",
        "box13_bond_premium_tax_exempt_bond",
        "box14_tax_exempt_bond_cusip",
        "box15_state",
        "box16_state_identification",
        "box17_state_tax_withheld",
        "tax_year",
        # legacy
        "interest_income",
        "early_withdrawal_penalty",
        "us_savings_bond_interest",
        "federal_income_tax_withheld",
    ],
    "f1099_div_records": [
        "payer_name",
        "payer_address",
        "box1a_total_ordinary_dividends",
        "box1b_qualified_dividends",
        "box2a_total_capital_gain",
        "box2b_unrecap_sec1250_gain",
        "box2c_section_1202_gain",
        "box2d_collectibles_gain",
        "box2e_section_897_ordinary_dividends",
        "box2f_section_897_capital_gain",
        "box3_nondividend_distributions",
        "box4_federal_income_tax_withheld",
        "box5_section_199a_dividends",
        "box6_investment_expenses",
        "box7_foreign_tax_paid",
        "box8_foreign_country",
        "box9_cash_liquidation_distributions",
        "box10_noncash_liquidation_distributions",
        "box11_fatca_filing_requirement",
        "box12_exempt_interest_dividends",
        "box13_specified_private_activity_bond",
        "box14_state",
        "box15_state_identification",
        "box16_state_tax_withheld",
        "tax_year",
        # legacy
        "total_ordinary_dividends",
        "qualified_dividends",
        "total_capital_gain",
        "federal_income_tax_withheld",
    ],
}

FORM_INTEGER_COLUMNS: frozenset[str] = frozenset(
    {
        "box13_statutory_employee",
        "box13_retirement_plan",
        "box13_third_party_sick_pay",
        "box2_direct_sales_indicator",
        "box7_direct_sales_indicator",
        "box11_fatca_filing_requirement",
    }
)

FORM_TABLE_INSERT_COLUMN_SETS: dict[str, frozenset[str]] = {
    t: frozenset(cols) for t, cols in FORM_TABLE_INSERT_COLUMNS.items()
}

# DDL fragments for ALTER ADD — every business column vs empty DB bootstrap
CREATE_TABLE_FRAGMENTS_DOC7: dict[str, str] = {
    "w2_records": """CREATE TABLE IF NOT EXISTS w2_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          employer_name TEXT,
          employer_address TEXT,
          box1_wages_tips_other TEXT,
          box2_federal_income_tax_withheld TEXT,
          box3_social_security_wages TEXT,
          box4_social_security_tax_withheld TEXT,
          box5_medicare_wages_tips TEXT,
          box6_medicare_tax_withheld TEXT,
          box7_social_security_tips TEXT,
          box8_allocated_tips TEXT,
          box10_dependent_care_benefits TEXT,
          box11_nonqualified_plans TEXT,
          box12a_code TEXT,
          box12a_amount TEXT,
          box12b_code TEXT,
          box12b_amount TEXT,
          box12c_code TEXT,
          box12c_amount TEXT,
          box12d_code TEXT,
          box12d_amount TEXT,
          box13_statutory_employee INTEGER DEFAULT 0,
          box13_retirement_plan INTEGER DEFAULT 0,
          box13_third_party_sick_pay INTEGER DEFAULT 0,
          box14_other TEXT,
          box15_state TEXT,
          box16_state_wages TEXT,
          box17_state_income_tax TEXT,
          box18_local_wages TEXT,
          box19_local_income_tax TEXT,
          box20_locality_name TEXT,
          box15b_state TEXT,
          box16b_state_wages TEXT,
          box17b_state_income_tax TEXT,
          box18b_local_wages TEXT,
          box19b_local_income_tax TEXT,
          box20b_locality_name TEXT,
          wages_tips_other TEXT,
          federal_income_tax_withheld TEXT,
          state_wages TEXT,
          state_income_tax TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        );""",
    "f1099_nec_records": """CREATE TABLE IF NOT EXISTS f1099_nec_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          payer_address TEXT,
          box1_nonemployee_compensation TEXT,
          box2_direct_sales_indicator INTEGER DEFAULT 0,
          box4_federal_income_tax_withheld TEXT,
          box5_state_tax_withheld TEXT,
          box6_state TEXT,
          box7_state_income TEXT,
          nonemployee_compensation TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        );""",
    "f1099_misc_records": """CREATE TABLE IF NOT EXISTS f1099_misc_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          payer_address TEXT,
          box1_rents TEXT,
          box2_royalties TEXT,
          box3_other_income TEXT,
          box4_federal_income_tax_withheld TEXT,
          box5_fishing_boat_proceeds TEXT,
          box6_medical_health_care_payments TEXT,
          box7_direct_sales_indicator INTEGER DEFAULT 0,
          box8_substitute_payments TEXT,
          box9_crop_insurance_proceeds TEXT,
          box10_gross_proceeds_attorney TEXT,
          box11_fish_purchased_resale TEXT,
          box12_section_409a_deferrals TEXT,
          box14_gross_proceeds_attorney TEXT,
          box15_section_409a_income TEXT,
          box16_state_tax_withheld TEXT,
          box17_state TEXT,
          box18_state_income TEXT,
          rents TEXT,
          royalties TEXT,
          other_income TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        );""",
    "f1099_int_records": """CREATE TABLE IF NOT EXISTS f1099_int_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          payer_address TEXT,
          box1_interest_income TEXT,
          box2_early_withdrawal_penalty TEXT,
          box3_us_savings_bond_treasury_interest TEXT,
          box4_federal_income_tax_withheld TEXT,
          box5_investment_expenses TEXT,
          box6_foreign_tax_paid TEXT,
          box7_foreign_country TEXT,
          box8_tax_exempt_interest TEXT,
          box9_specified_private_activity_bond_interest TEXT,
          box10_market_discount TEXT,
          box11_bond_premium TEXT,
          box12_bond_premium_treasury_obligations TEXT,
          box13_bond_premium_tax_exempt_bond TEXT,
          box14_tax_exempt_bond_cusip TEXT,
          box15_state TEXT,
          box16_state_identification TEXT,
          box17_state_tax_withheld TEXT,
          interest_income TEXT,
          early_withdrawal_penalty TEXT,
          us_savings_bond_interest TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        );""",
    "f1099_div_records": """CREATE TABLE IF NOT EXISTS f1099_div_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          payer_address TEXT,
          box1a_total_ordinary_dividends TEXT,
          box1b_qualified_dividends TEXT,
          box2a_total_capital_gain TEXT,
          box2b_unrecap_sec1250_gain TEXT,
          box2c_section_1202_gain TEXT,
          box2d_collectibles_gain TEXT,
          box2e_section_897_ordinary_dividends TEXT,
          box2f_section_897_capital_gain TEXT,
          box3_nondividend_distributions TEXT,
          box4_federal_income_tax_withheld TEXT,
          box5_section_199a_dividends TEXT,
          box6_investment_expenses TEXT,
          box7_foreign_tax_paid TEXT,
          box8_foreign_country TEXT,
          box9_cash_liquidation_distributions TEXT,
          box10_noncash_liquidation_distributions TEXT,
          box11_fatca_filing_requirement INTEGER DEFAULT 0,
          box12_exempt_interest_dividends TEXT,
          box13_specified_private_activity_bond TEXT,
          box14_state TEXT,
          box15_state_identification TEXT,
          box16_state_tax_withheld TEXT,
          total_ordinary_dividends TEXT,
          qualified_dividends TEXT,
          total_capital_gain TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        );""",
}


def split_sql_columns(inner: str) -> list[str]:
    parts: list[str] = []
    buf: list[str] = []
    depth = 0
    for ch in inner:
        if ch == "(":
            depth += 1
            buf.append(ch)
        elif ch == ")":
            depth -= 1
            buf.append(ch)
        elif ch == "," and depth == 0:
            s = "".join(buf).strip()
            if s:
                parts.append(s)
            buf = []
        else:
            buf.append(ch)
    tail = "".join(buf).strip()
    if tail:
        parts.append(tail)
    return parts


def get_form_alter_columns_by_table() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    for tbl, stmt in CREATE_TABLE_FRAGMENTS_DOC7.items():
        inner = stmt[stmt.index("(") + 1 : stmt.rindex(")")]
        raw_parts = split_sql_columns(inner)
        cols = []
        for p in raw_parts:
            pname = p.split(None, 1)[0].lower()
            if pname in ("id", "return_id", "doc_id"):
                continue
            cols.append(p)
        out[tbl] = cols
    return out
