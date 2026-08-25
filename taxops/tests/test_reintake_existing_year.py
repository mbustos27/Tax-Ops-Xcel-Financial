"""Reintake always uses the same Drake/profile pull — even when a year exists."""
from __future__ import annotations


def test_reintake_pulls_when_client_already_has_active_year(client_logged_in, taxops_db_path):
    """Client with TY2025 on file still gets Drake + last-return prefill for TY2024."""
    from db import get_connection, init_db, set_active_intake_tax_year
    from utils import now as utc_now

    conn = get_connection(taxops_db_path)
    init_db(conn)
    set_active_intake_tax_year(conn, 2025)
    ts = utc_now()
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, taxpayer_cell) "
        "VALUES (920, 'ALREADY', 'HASYEAR', '555-0100')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status,
                             filing_status, created_at, updated_at)
        VALUES (9201, 920, '777', 2025, 'PROCESSING', 'SINGLE', ?, ?)
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT INTO drake_prefill_links (
          tax_year, csm_ssn_last4, csm_name_raw, csm_name_norm, client_id,
          prefill_status, purple_name, match_tier, created_at, updated_at
        ) VALUES (
          2024, '1111', 'ALREADY, HASYEAR', 'ALREADY, HASYEAR', 920,
          'PRIOR_YEAR_FORMS_AVAILABLE', 'HASYEAR ALREADY', 'deterministic', ?, ?
        )
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT INTO drake_form_prefill (
          link_id, tax_year, form_counts, return_type, source_files, created_at
        )
        SELECT id, 2024, '{"Schedule C": 2}', '1040', '[]', ?
        FROM drake_prefill_links WHERE client_id = 920
        """,
        (ts,),
    )
    conn.commit()
    conn.close()

    # No tax_year param — same mechanism as classic reintake
    r = client_logged_in.get("/api/clients/920/reintake")
    assert r.status_code == 200
    body = r.get_json()
    assert body["client"]["last_name"] == "ALREADY"
    assert body["client"]["taxpayer_cell"] == "555-0100"
    assert body["last_return"]["tax_year"] == 2025
    assert body["last_return"]["log_number"] == "777"
    assert body["last_return"].get("sched_c") == 1

    # Opening a prior TY still uses the same pull; source return is TY2025
    r2 = client_logged_in.get("/api/clients/920/reintake?tax_year=2024")
    assert r2.status_code == 200
    body2 = r2.get_json()
    assert body2["client"]["last_name"] == "ALREADY"
    assert body2["target_tax_year"] == 2024
    assert body2["source_tax_year"] == 2025
    assert body2["last_return"]["tax_year"] == 2025
    assert body2["client"].get("prior_year_log") == "777"
    assert body2["last_return"].get("sched_c") == 1


def test_pick_reintake_source_prefers_older_than_target(taxops_db_path, app):
    from db import get_connection
    import app as app_mod

    conn = get_connection(taxops_db_path)
    try:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name) VALUES (921, 'SRC', 'PICK')"
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, log_number, tax_year, client_status) VALUES "
            "(9211, 921, '1', 2023, 'LOG OUT'),"
            "(9212, 921, '2', 2025, 'PROCESSING')"
        )
        conn.commit()
        row = app_mod._pick_reintake_source_return(conn, 921, 2024)
        assert row is not None
        assert int(row["tax_year"]) == 2023
        assert str(row["log_number"]) == "1"
    finally:
        conn.close()
