"""Missing document print checklist + aggregate report."""
from __future__ import annotations


def test_missing_docs_print_open_items(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('MISS','DOC',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, intake_date, created_at) "
        "VALUES (?,?, '901', '2026-01-15', datetime('now'))",
        (cid, 2025),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
        "VALUES (?, 'W-2', 0, datetime('now'))",
        (rid,),
    )
    conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at, resolved_at) "
        "VALUES (?, '1099-INT (Interest)', 1, datetime('now'), datetime('now'))",
        (rid,),
    )
    conn.execute(
        "INSERT INTO notes (return_id, note_text, source, created_at) "
        "VALUES (?, 'Client will bring W-2 from second job next week.', 'INTAKE', datetime('now'))",
        (rid,),
    )
    conn.execute(
        "INSERT INTO notes (return_id, note_text, source, created_at) "
        "VALUES (?, 'Called client — left voicemail.', 'APP', datetime('now'))",
        (rid,),
    )
    conn.commit()
    conn.close()

    page = client_logged_in.get(f"/return/{rid}/missing-docs/print")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "MISSING DOCS" in html
    assert "#901" in html
    assert "W-2" in html
    assert "1099-INT" not in html
    assert "Supporting notes" in html
    assert "second job" in html
    assert "left voicemail" in html
    assert "INTAKE" in html

    all_page = client_logged_in.get(f"/return/{rid}/missing-docs/print?all=1")
    assert all_page.status_code == 200
    all_html = all_page.get_data(as_text=True)
    assert "W-2" in all_html
    assert "1099-INT" in all_html


def test_missing_docs_aggregate_report(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('AGG','ONE',datetime('now'),datetime('now'))"
    )
    cid1 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '1001', datetime('now'))",
        (cid1, 2025),
    )
    rid1 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('AGG','TWO',datetime('now'),datetime('now'))"
    )
    cid2 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, created_at) "
        "VALUES (?,?, '1002', datetime('now'))",
        (cid2, 2025),
    )
    rid2 = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
        "VALUES (?, 'W-2', 0, datetime('now'))",
        (rid1,),
    )
    conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) "
        "VALUES (?, 'w-2', 0, datetime('now'))",
        (rid2,),
    )
    conn.commit()
    conn.close()

    page = client_logged_in.get("/reports/missing-docs?year=2025")
    assert page.status_code == 200
    html = page.get_data(as_text=True)
    assert "Missing Documents Report" in html
    assert "W-2" in html or "w-2" in html

    api = client_logged_in.get("/api/reports/missing-docs/data?year=2025&status=open")
    assert api.status_code == 200
    data = api.get_json()
    assert data["summary"]["open_items"] == 2
    assert data["summary"]["returns_with_open"] == 2
    assert len(data["items"]) == 1
    assert data["items"][0]["open_count"] == 2

    norm_key = data["items"][0]["norm_key"]
    drill = client_logged_in.get(
        f"/api/reports/missing-docs/returns?year=2025&norm_key={norm_key}"
    )
    assert drill.status_code == 200
    rows = drill.get_json()["returns"]
    assert len(rows) == 2
