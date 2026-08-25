"""Re-intake must complete a PENDING INTAKE shell instead of double-inserting."""

from __future__ import annotations


def test_intake_reuses_pending_intake_shell(client_logged_in, taxops_db_path, monkeypatch):
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (501, 'BARRERA', 'ADRIAN', '2025-01-01', '2025-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (6001, 501, NULL, 2025, 'PENDING INTAKE', '2025-01-01', '2025-01-01')"
    )
    conn.execute(
        "INSERT INTO return_forms (return_id, form_1040) VALUES (6001, NULL)"
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "client_id": "501",
            "last_name": "BARRERA",
            "first_name": "ADRIAN",
            "form_1040": "1",
            "intake_date": "2026-08-07",
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303)
    assert "/return/6001" in (resp.headers.get("Location") or "")

    conn = get_connection(taxops_db_path)
    rows = conn.execute(
        "SELECT id, log_number, client_status FROM returns WHERE client_id=501 AND tax_year=2025"
    ).fetchall()
    assert len(rows) == 1
    assert rows[0]["id"] == 6001
    assert rows[0]["client_status"] == "PROCESSING"
    assert rows[0]["log_number"] is not None
    forms = conn.execute(
        "SELECT form_1040 FROM return_forms WHERE return_id=6001"
    ).fetchone()
    assert forms["form_1040"] == 1
    evt = conn.execute(
        """
        SELECT old_status, new_status, note FROM status_events
        WHERE return_id=6001 ORDER BY id DESC LIMIT 1
        """
    ).fetchone()
    assert evt["old_status"] == "PENDING INTAKE"
    assert evt["new_status"] == "PROCESSING"
    conn.close()


def test_intake_blocks_existing_non_pending_return(client_logged_in, taxops_db_path, monkeypatch):
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (502, 'SMITH', 'JANE', '2025-01-01', '2025-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (6002, 502, '1200', 2025, 'PROCESSING', '2025-01-01', '2025-01-01')"
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "client_id": "502",
            "last_name": "SMITH",
            "first_name": "JANE",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 409
    body = resp.get_data(as_text=True)
    assert "already has a 2025 return" in body
    assert "LOG #1200" in body
    assert "PROCESSING" in body

    conn = get_connection(taxops_db_path)
    n = conn.execute(
        "SELECT COUNT(*) AS c FROM returns WHERE client_id=502 AND tax_year=2025"
    ).fetchone()["c"]
    assert n == 1
    conn.close()
