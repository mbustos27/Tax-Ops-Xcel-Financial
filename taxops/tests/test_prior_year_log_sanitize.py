"""Old log #s must not be treated as years or block this-year intake."""
from __future__ import annotations


def test_sanitize_prior_year_log_undoes_year_expansion():
    import app as app_mod

    assert app_mod._sanitize_prior_year_log("1936", known_log_numbers=["36", "1271"]) == "36"
    assert app_mod._sanitize_prior_year_log("36", known_log_numbers=["36"]) == "36"
    assert app_mod._sanitize_prior_year_log("1271", known_log_numbers=["36", "1271"]) == "1271"
    assert app_mod._sanitize_prior_year_log("1936", known_log_numbers=["99"]) == "1936"
    assert app_mod._sanitize_prior_year_log("", known_log_numbers=["36"]) is None


def test_intake_this_year_despite_old_log_and_corrupted_prior_year_log(
    client_logged_in, taxops_db_path, monkeypatch
):
    """Jarmi-class: TY2023 log #36 + prior_year_log 1936 must still log TY2025."""
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, prior_year_log, created_at, updated_at) "
        "VALUES (530, 'LOPEZ', 'JARMI', '1936', '2024-04-04', '2024-04-04')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, "
        "intake_date, created_at, updated_at) "
        "VALUES (6301, 530, '36', 2023, 'PROCESSING', '2024-04-04', '2024-04-04', '2024-04-04')"
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "client_id": "530",
            "last_name": "LOPEZ",
            "first_name": "JARMI",
            "tax_year": "2025",
            "prior_year_log": "1936",
            "intake_date": "2026-08-18",
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303), resp.get_data(as_text=True)[:500]
    loc = resp.headers.get("Location") or ""
    assert "/return/" in loc
    assert "6301" not in loc  # must be a NEW return, not the TY2023 row

    conn = get_connection(taxops_db_path)
    try:
        years = {
            int(r["tax_year"]): dict(r)
            for r in conn.execute(
                "SELECT id, log_number, tax_year, client_status FROM returns "
                "WHERE client_id=530 AND COALESCE(client_status,'') != 'CANCELLED'"
            )
        }
        assert 2023 in years
        assert years[2023]["log_number"] == "36"
        assert 2025 in years
        assert years[2025]["client_status"] == "PROCESSING"
        assert years[2025]["log_number"] not in (None, "", "36", "1936")
        cli = conn.execute(
            "SELECT prior_year_log FROM clients WHERE id=530"
        ).fetchone()
        assert cli["prior_year_log"] == "36"
    finally:
        conn.close()


def test_reintake_unexpands_prior_year_log(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, prior_year_log) "
        "VALUES (531, 'LOPEZ', 'JARMI', '1936')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status) "
        "VALUES (6302, 531, '36', 2023, 'PROCESSING')"
    )
    conn.commit()
    conn.close()

    r = client_logged_in.get("/api/clients/531/reintake?tax_year=2025")
    assert r.status_code == 200
    body = r.get_json()
    assert body["client"]["prior_year_log"] == "36"


def test_pending_shell_gets_fresh_log_not_old_year_number(
    client_logged_in, taxops_db_path, monkeypatch
):
    """Season PENDING INTAKE for 2025 must not reuse TY2023's log #36."""
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, prior_year_log, created_at, updated_at) "
        "VALUES (532, 'LOPEZ', 'JARMI', '36', '2024-04-04', '2024-04-04')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, "
        "intake_date, created_at, updated_at) "
        "VALUES (6303, 532, '36', 2023, 'PROCESSING', '2024-04-04', '2024-04-04', '2024-04-04')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (6304, 532, NULL, 2025, 'PENDING INTAKE', '2025-01-01', '2025-01-01')"
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "client_id": "532",
            "last_name": "LOPEZ",
            "first_name": "JARMI",
            "tax_year": "2025",
            "prior_year_log": "36",
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303), resp.get_data(as_text=True)[:500]

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT log_number, client_status, tax_year FROM returns WHERE id=6304"
        ).fetchone()
        assert row["client_status"] == "PROCESSING"
        assert row["tax_year"] == 2025
        assert row["log_number"] not in (None, "", "36", "1936")
        old = conn.execute(
            "SELECT log_number, tax_year FROM returns WHERE id=6303"
        ).fetchone()
        assert old["log_number"] == "36"
        assert old["tax_year"] == 2023
    finally:
        conn.close()


def test_old_year_processing_relogs_onto_this_season_book(
    client_logged_in, taxops_db_path, monkeypatch
):
    """POST tax_year=2023 with last year's LOG #36 keeps TY2023 on the 2025 book."""
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (540, 'LOPEZ', 'JARMI', '2024-04-04', '2024-04-04')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, "
        "intake_date, created_at, updated_at) "
        "VALUES (6401, 540, '36', 2023, 'PROCESSING', '2024-04-04', '2024-04-04', '2024-04-04')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, "
        "intake_date, created_at, updated_at) "
        "VALUES (6402, 540, '100', 2025, 'PROCESSING', '2026-08-18', '2026-08-18', '2026-08-18')"
    )
    conn.commit()
    conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "client_id": "540",
            "last_name": "LOPEZ",
            "first_name": "JARMI",
            "tax_year": "2023",
            "prior_year_log": "36",
            "intake_date": "2026-08-18",
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303), resp.get_data(as_text=True)[:800]
    loc = resp.headers.get("Location") or ""
    assert "/return/6401" in loc

    conn = get_connection(taxops_db_path)
    try:
        rows = list(
            conn.execute(
                "SELECT id, tax_year, log_number, client_status, intake_date FROM returns "
                "WHERE client_id=540 ORDER BY tax_year"
            )
        )
        by_year = {int(r["tax_year"]): dict(r) for r in rows}
        assert by_year[2023]["id"] == 6401
        assert by_year[2023]["client_status"] == "PROCESSING"
        assert by_year[2023]["intake_date"] == "2026-08-18"
        assert by_year[2023]["log_number"] not in (None, "", "36")
        assert int(by_year[2023]["log_number"]) >= 101
        assert by_year[2025]["id"] == 6402
        assert by_year[2025]["log_number"] == "100"
    finally:
        conn.close()
