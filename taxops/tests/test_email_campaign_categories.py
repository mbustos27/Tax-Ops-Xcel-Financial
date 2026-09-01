"""Campaign category filters (personal vs business return types)."""

from __future__ import annotations


def _seed_return_with_form(taxops_db_path, client_id: int, form_col: str, log: str) -> int:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, taxpayer_email, do_not_email) "
        "VALUES (?, 'Cat', 'Test', 'cat@example.com', 0)",
        (client_id,),
    )
    rid = client_id * 10
    conn.execute(
        """
        INSERT INTO returns (id, client_id, tax_year, log_number, client_status, created_at)
        VALUES (?, ?, 2026, ?, 'PROCESSING', datetime('now'))
        """,
        (rid, client_id, log),
    )
    conn.execute(f"INSERT INTO return_forms (return_id, {form_col}) VALUES (?, 1)", (rid,))
    conn.commit()
    conn.close()
    return rid


def test_sanitize_expands_campaign_category_to_forms():
    from email_campaigns import sanitize_audience_definition

    aud = sanitize_audience_definition({"campaign_category": "business_1120"})
    assert aud["campaign_category"] == "business_1120"
    assert aud["forms"] == ["form_1120"]


def test_resolve_audience_filters_personal_vs_business(taxops_db_path):
    from db import get_connection
    from email_campaigns import resolve_audience

    _seed_return_with_form(taxops_db_path, 9201, "form_1040", "101")
    _seed_return_with_form(taxops_db_path, 9202, "form_1120", "102")

    conn = get_connection(taxops_db_path)
    personal, pstats = resolve_audience(
        conn, {"tax_year": 2026, "campaign_category": "personal"}
    )
    business, bstats = resolve_audience(
        conn, {"tax_year": 2026, "campaign_category": "business_1120"}
    )
    conn.close()

    assert pstats.resolved_recipients == 1
    assert personal[0].client_id == 9201
    assert bstats.resolved_recipients == 1
    assert business[0].client_id == 9202


def test_business_all_includes_1120s_and_1065(taxops_db_path):
    from db import get_connection
    from email_campaigns import resolve_audience

    _seed_return_with_form(taxops_db_path, 9210, "form_1120s", "201")
    _seed_return_with_form(taxops_db_path, 9211, "form_1065_llc", "202")

    conn = get_connection(taxops_db_path)
    recipients, stats = resolve_audience(
        conn, {"tax_year": 2026, "campaign_category": "business_all"}
    )
    conn.close()

    assert stats.resolved_recipients == 2
    ids = {r.client_id for r in recipients}
    assert ids == {9210, 9211}


def test_categories_api(client_logged_in):
    resp = client_logged_in.get("/api/email-campaigns/categories")
    assert resp.status_code == 200
    data = resp.get_json()
    ids = {c["id"] for c in data["categories"]}
    assert "personal" in ids
    assert "business_1120" in ids
    assert "business_partnership" in ids
