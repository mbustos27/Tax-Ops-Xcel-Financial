"""Dashboard export and upcoming-deadline print routes."""

from __future__ import annotations

from datetime import date, timedelta


def test_export_includes_deadline_columns(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (9401, 'DEAD', 'LINE')"
        )
        conn.execute(
            """
            INSERT INTO returns (
              id, client_id, tax_year, log_number, client_status,
              extension_due_date, created_at
            ) VALUES (94010, 9401, 2025, '9401', 'PROCESSING', ?, datetime('now'))
            """,
            ((date.today() + timedelta(days=10)).isoformat(),),
        )
        conn.execute(
            "INSERT INTO return_forms (return_id, form_1040) VALUES (94010, 1)"
        )
        conn.commit()
    finally:
        conn.close()

    year = date.today().year
    rv = client_logged_in.get(f"/export?year={year}&upcoming_deadline=1")
    assert rv.status_code == 200
    assert "spreadsheetml" in (rv.content_type or "")
    assert rv.data[:2] == b"PK"


def test_upcoming_deadlines_print_lists_rows_alphabetical(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (9402, 'ZULU', 'AAA')"
        )
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (9403, 'ALPHA', 'BBB')"
        )
        due = (date.today() + timedelta(days=12)).isoformat()
        intake = f"{date.today().year}-01-15"
        for cid, rid, log, last in (
            (9402, 94020, "9402", "ZULU"),
            (9403, 94030, "9403", "ALPHA"),
        ):
            conn.execute(
                """
                INSERT INTO returns (
                  id, client_id, tax_year, log_number, client_status,
                  extension_due_date, created_at, intake_date
                ) VALUES (?, ?, 2025, ?, 'PROCESSING', ?, datetime('now'), ?)
                """,
                (rid, cid, log, due, intake),
            )
            conn.execute(
                "INSERT INTO return_forms (return_id, form_1120s) VALUES (?, 1)",
                (rid,),
            )
        conn.commit()
    finally:
        conn.close()

    year = date.today().year
    rv = client_logged_in.get(f"/export/upcoming-deadlines/print?year={year}")
    assert rv.status_code == 200
    html = rv.get_data(as_text=True)
    assert "UPCOMING DEADLINES" in html
    assert html.index("ALPHA") < html.index("ZULU")


def test_export_csv_fallback_without_openpyxl(client_logged_in, monkeypatch):
    """Export must not 500 when openpyxl is missing — CSV fallback."""
    import builtins
    import sys

    real_import = builtins.__import__

    def _no_openpyxl(name, *args, **kwargs):
        if name == "openpyxl" or name.startswith("openpyxl."):
            raise ImportError("simulated missing openpyxl")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", _no_openpyxl)
    sys.modules.pop("openpyxl", None)

    year = date.today().year
    rv = client_logged_in.get(f"/export?year={year}")
    assert rv.status_code == 200
    assert "csv" in (rv.content_type or "")
    body = rv.get_data(as_text=True)
    assert "Log #" in body
    assert "Last Name" in body


def test_export_filename_suffix():
    from app import _export_filename_suffix

    assert "upcoming-deadlines" in _export_filename_suffix({"upcoming_deadline": "1"})
    assert _export_filename_suffix({}) == "ALL"
