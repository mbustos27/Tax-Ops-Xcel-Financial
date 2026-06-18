"""FORMS-3 — extraction/save paths must leave legacy mirror DDL columns NULL.

Legacy columns remain on SQLite tables per migration discipline; FORM_TABLE_INSERT_COLUMNS
(and thus _save_form_data) uses only canonical IRS box_* names."""

from __future__ import annotations

from flask import Flask

from ai_routes import _save_form_data
from db import get_connection, init_db
from form_schema import FORM_LEGACY_MIRROR_COLUMNS


def _seed_parent_rows(conn, *, return_id: int, client_id: int) -> int:
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (?, 'Leg', 'Mirror', 'Leg, Mirror');
        """,
        (client_id,),
    )
    conn.execute(
        """
        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor, verified, client_status,
            intake_date
        ) VALUES (
            ?, ?, 'lm79', 2025, '', 0, 'PROCESSING', '2026-06-02'
        );
        """,
        (return_id, client_id),
    )
    cur = conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, uploaded_by, uploaded_at, is_deleted
        ) VALUES (?, 'l.pdf', 'l.pdf', 'unknown', 'test', '/tmp/l', 1, 't', '2026-06-02', 0);
        """,
        (return_id,),
    )
    conn.commit()
    return int(cur.lastrowid)


def _legacy_nonempty(row, table: str) -> list[str]:
    bad: list[str] = []
    for col in FORM_LEGACY_MIRROR_COLUMNS[table]:
        v = row[col]
        if v is None:
            continue
        if isinstance(v, str) and v.strip() == "":
            continue
        if isinstance(v, (int, float)) and int(v) == 0:
            continue
        bad.append(col)
    return bad


def _assert_canonical_save_no_legacy_mirror(
    taxops_db_path: str, table_name: str, rid: int, cid: int, payload: dict
) -> None:
    conn = get_connection(taxops_db_path)
    init_db(conn)
    doc_id = _seed_parent_rows(conn, return_id=rid, client_id=cid)

    flask_app = Flask(__name__)
    with flask_app.app_context():
        ok = _save_form_data(conn, table_name, rid, doc_id, payload)
    assert ok, table_name

    conn.commit()
    row = conn.execute(
        f"SELECT * FROM {table_name} WHERE return_id = ? ORDER BY id DESC LIMIT 1",
        (rid,),
    ).fetchone()
    conn.close()

    assert row is not None
    violated = _legacy_nonempty(row, table_name)
    assert violated == [], (
        f"{table_name}: legacy mirrors empty; unexpected {violated}; "
        "FORMS-3 DDL mirrors must not be written by _save_form_data."
    )
    assert row["tax_year"] == payload["tax_year"]


def test_legacy_mirror_w2_records(taxops_db_path):
    _assert_canonical_save_no_legacy_mirror(
        taxops_db_path,
        "w2_records",
        80101,
        80100,
        {
            "employer_name": "ACME LEG",
            "tax_year": "2024",
            "box1_wages_tips_other": "93000",
            "box2_federal_income_tax_withheld": "12000",
        },
    )


def test_legacy_mirror_f1099_nec(taxops_db_path):
    _assert_canonical_save_no_legacy_mirror(
        taxops_db_path,
        "f1099_nec_records",
        80201,
        80200,
        {
            "payer_name": "PAYER NEC",
            "tax_year": "2024",
            "box1_nonemployee_compensation": "4500",
        },
    )


def test_legacy_mirror_f1099_misc(taxops_db_path):
    _assert_canonical_save_no_legacy_mirror(
        taxops_db_path,
        "f1099_misc_records",
        80301,
        80300,
        {
            "payer_name": "PAYER MISC",
            "tax_year": "2024",
            "box1_rents": "100",
            "box2_royalties": "200",
        },
    )


def test_legacy_mirror_f1099_int(taxops_db_path):
    _assert_canonical_save_no_legacy_mirror(
        taxops_db_path,
        "f1099_int_records",
        80401,
        80400,
        {
            "payer_name": "PAYER INT",
            "tax_year": "2024",
            "box1_interest_income": "305",
            "box2_early_withdrawal_penalty": "0",
        },
    )


def test_legacy_mirror_f1099_div(taxops_db_path):
    _assert_canonical_save_no_legacy_mirror(
        taxops_db_path,
        "f1099_div_records",
        80501,
        80500,
        {
            "payer_name": "PAYER DIV",
            "tax_year": "2024",
            "box1a_total_ordinary_dividends": "900",
            "box1b_qualified_dividends": "700",
        },
    )
