"""Staff may log prior tax years within the season window at intake."""
from __future__ import annotations

import pytest


def test_allowed_intake_tax_years_includes_active_and_five_prior(taxops_db_path):
    from db import (
        allowed_intake_tax_years,
        get_connection,
        set_active_intake_tax_year,
    )

    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.commit()
        assert allowed_intake_tax_years(conn) == [2025, 2024, 2023, 2022, 2021, 2020]
    finally:
        conn.close()


def test_resolve_intake_tax_year_defaults_and_rejects(taxops_db_path):
    from db import get_connection, resolve_intake_tax_year, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.commit()
        assert resolve_intake_tax_year(conn, None) == 2025
        assert resolve_intake_tax_year(conn, "") == 2025
        assert resolve_intake_tax_year(conn, "2023") == 2023
        with pytest.raises(ValueError):
            resolve_intake_tax_year(conn, "2018")
        with pytest.raises(ValueError):
            resolve_intake_tax_year(conn, "2026")
        with pytest.raises(ValueError):
            resolve_intake_tax_year(conn, "abc")
    finally:
        conn.close()


def test_intake_creates_prior_year_return(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.commit()
    finally:
        conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={"last_name": "PriorYear", "first_name": "Test", "tax_year": "2023"},
        follow_redirects=False,
    )
    assert resp.status_code == 302

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT r.tax_year, r.log_number
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE c.last_name = 'PRIORYEAR'
            """
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["tax_year"] == 2023
    assert row["log_number"]


def test_next_season_log_number_follows_active_book(taxops_db_path):
    from db import get_connection, next_season_log_number, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
            "VALUES (710, 'BOOK', 'A', '2026-01-01', '2026-01-01')"
        )
        conn.execute(
            "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (710, '50', 2025, 'PROCESSING', '2026-03-01', '2026-03-01', '2026-03-01')"
        )
        conn.execute(
            "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (710, '12', 2023, 'PROCESSING', '2024-04-01', '2024-04-01', '2024-04-01')"
        )
        conn.commit()
        assert next_season_log_number(conn, intake_date="2026-08-19") == "51"
        conn.execute(
            "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (710, '80', 2019, 'PROCESSING', '2026-06-02', '2026-06-02', '2026-06-02')"
        )
        conn.commit()
        assert next_season_log_number(conn, intake_date="2026-08-19") == "81"
    finally:
        conn.close()


def test_intake_prior_year_uses_season_log_book(client_logged_in, taxops_db_path, monkeypatch):
    from db import get_connection, set_active_intake_tax_year

    monkeypatch.setattr(
        "filetrack.labels.dispatch.try_print_log_label",
        lambda *a, **k: None,
    )
    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
            "VALUES (711, 'SEASON', 'LOG', '2026-01-01', '2026-01-01')"
        )
        conn.execute(
            "INSERT INTO returns (client_id, log_number, tax_year, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (711, '90', 2025, 'PROCESSING', '2026-03-01', '2026-03-01', '2026-03-01')"
        )
        conn.commit()
    finally:
        conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={
            "last_name": "Catchup",
            "first_name": "Prior",
            "tax_year": "2023",
            "intake_date": "2026-08-19",
        },
        follow_redirects=False,
    )
    assert resp.status_code in (302, 303), resp.get_data(as_text=True)[:500]

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT r.tax_year, r.log_number
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE c.last_name = 'CATCHUP'
            """
        ).fetchone()
    finally:
        conn.close()
    assert row is not None
    assert row["tax_year"] == 2023
    assert int(row["log_number"]) >= 91


def test_intake_rejects_out_of_range_tax_year(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.commit()
    finally:
        conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={"last_name": "BadYear", "tax_year": "2010"},
        follow_redirects=False,
    )
    assert resp.status_code == 409
    assert b"outside the allowed range" in resp.data or b"2010" in resp.data
