"""Season list keeps prior-TY intakes with the current season; multi-TY per client."""
from __future__ import annotations

from datetime import date


def test_season_list_includes_same_client_multiple_tax_years(app, taxops_db_path):
    """TY2024 + TY2025 for one person both appear on this season's dashboard list."""
    from db import get_connection, set_active_intake_tax_year
    import app as app_mod

    season = date.today().year
    today = date.today().isoformat()
    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
            "VALUES (901, 'MULTITY', 'SAM', ?, ?)",
            (today, today),
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, "
            "intake_date, created_at, updated_at) VALUES "
            "(9011, 901, '100', 2025, 'PROCESSING', ?, ?, ?),"
            "(9012, 901, '50', 2024, 'PROCESSING', ?, ?, ?)",
            (today, today, today, today, today, today),
        )
        conn.commit()
    finally:
        conn.close()

    with app.test_request_context():
        rows = app_mod.query_returns({"year": season})
    mine = [r for r in rows if r.get("client_id") == 901 or r.get("last_name") == "MULTITY"]
    years = sorted({r["tax_year"] for r in mine})
    assert years == [2024, 2025], years
    assert len(mine) == 2


def test_season_year_for_return_uses_intake_not_tax_year():
    import app as app_mod

    assert app_mod._season_year_for_return("2026-08-18") == 2026
    assert app_mod._season_year_for_return(None) == date.today().year


def test_prior_year_intake_stays_on_current_season_list(client_logged_in, app, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year
    import app as app_mod

    season = date.today().year
    conn = get_connection(taxops_db_path)
    try:
        set_active_intake_tax_year(conn, 2025)
        conn.commit()
    finally:
        conn.close()

    resp = client_logged_in.post(
        "/intake",
        data={"last_name": "SeasonStay", "first_name": "Pat", "tax_year": "2023"},
        follow_redirects=False,
    )
    assert resp.status_code == 302
    # Must open the new return — never bounce to a tax-year-as-season URL
    assert "/return/" in (resp.headers.get("Location") or "")
    assert "year=2023" not in (resp.headers.get("Location") or "")

    with app.test_request_context():
        rows = app_mod.query_returns({"year": season})
    mine = [r for r in rows if (r.get("last_name") or "") == "SEASONSTAY"]
    assert len(mine) == 1
    assert mine[0]["tax_year"] == 2023
