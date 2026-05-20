"""Multi-year comparison API (#124–#129)."""

from __future__ import annotations

import io

from multiyear_comparison import DOC_TYPES_COMPARE, parse_years_param, render_year_comparison_pdf


def test_parse_years_param_accepts_three():
    yrs, err = parse_years_param("2024, 2023,2022")
    assert err is None
    assert yrs == [2022, 2023, 2024]


def test_parse_years_param_rejects_one():
    yrs, err = parse_years_param("2024")
    assert yrs is None
    assert err is not None


def test_compare_json_two_years(client_logged_in, taxops_db_path: str):  # noqa: ARG001
    import db as db_mod

    conn = db_mod.get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (90001, 'Doe', 'Jane')")
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (91001, 90001, '101', 2023, 'LOG OUT'), (91002, 90001, '102', 2024, 'LOG OUT')
        """,
    )
    conn.execute(
        """INSERT INTO payments (return_id, refund_amount, balance_due)
           VALUES (91001, 1000.0, 50.0), (91002, 2000.0, 700.0)""",
    )
    conn.commit()
    conn.close()

    rv = client_logged_in.get("/api/clients/90001/years?years=2024,2023")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["client_id"] == 90001
    assert data["years"] == [2024, 2023]
    cols = {c["tax_year"]: c for c in data["columns"]}
    assert cols[2024]["refund_amount"] == 2000.0
    assert cols[2023]["refund_amount"] == 1000.0
    assert cols[2024]["field_highlights"]["refund_amount"] is True
    assert cols[2024]["field_highlights"]["balance_due"] is True


def test_compare_pdf_returns_bytes():
    blob = io.BytesIO(
        render_year_comparison_pdf(
            {
                "client_id": 1,
                "client_display": "Test",
                "columns": [
                    {
                        "tax_year": 2024,
                        "has_return": False,
                        "field_highlights": {},
                    },
                    {
                        "tax_year": 2023,
                        "has_return": True,
                        "return_url": "/return/9",
                        "adjusted_gross_income": 50000,
                        "filing_status": "MFJ",
                        "refund_amount": 900,
                        "balance_due": None,
                        "preparer_display": "A",
                        "documents_received": {dt: dt == "W-2" for dt in DOC_TYPES_COMPARE},
                        "missing_doc_items": [],
                        "field_highlights": {
                            k: False
                            for k in (
                                "adjusted_gross_income",
                                "filing_status",
                                "refund_amount",
                                "balance_due",
                                "preparer",
                            )
                        },
                    },
                ],
                "rows": [{"field": "refund_amount", "label": "Refund"}],
                "document_types": [{"doc_type": "W-2", "label": "W-2"}],
                "thresholds": {},
            },
        ),
    )
    assert blob.read()[:5] == b"%PDF-"
