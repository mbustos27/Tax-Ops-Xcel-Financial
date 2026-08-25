"""Dashboard quick filter q must search the full year set (server-side)."""
from __future__ import annotations

from db import get_connection


def _seed(taxops_db_path: str) -> None:
    conn = get_connection(taxops_db_path)
    rows = [
        ("Alpha", "Ann", "1001"),
        ("Beta", "Bob", "2002"),
        ("Smith", "John", "3003"),
        ("Garcia", "Maria", "4004"),
    ]
    for i, (ln, fn, log) in enumerate(rows, start=1):
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, display_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, '2026-01-01', '2026-01-01')",
            (i, ln, fn, f"{ln}, {fn}"),
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, tax_year, log_number, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (?, ?, 2025, ?, 'PROCESSING', '2026-02-01', '2026-01-01', '2026-01-01')",
            (i, i, log),
        )
    conn.commit()
    conn.close()


def _api(client, q: str, *, per_page: int = 50, page: int = 1) -> dict:
    resp = client.get(
        f"/api/dashboard/returns?year=2026&q={q}&per_page={per_page}&page={page}"
    )
    assert resp.status_code == 200
    return resp.get_json()


def test_q_matches_across_full_set_not_just_name_contiguous(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    data = _api(client_logged_in, "john%20smith")
    assert data["total_count"] == 1
    assert data["returns"][0]["log_number"] == "3003"

    data = _api(client_logged_in, "garcia")
    assert [r["log_number"] for r in data["returns"]] == ["4004"]


def test_q_digit_log_lookup(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    data = _api(client_logged_in, "3003")
    assert [r["log_number"] for r in data["returns"]] == ["3003"]

    data = _api(client_logged_in, "30")
    assert [r["log_number"] for r in data["returns"]] == ["3003"]


def test_api_dashboard_returns_respects_q(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    data = _api(client_logged_in, "smith")
    assert data["total_count"] == 1
    assert data["returns"][0]["log_number"] == "3003"


def test_paginated_q_count_matches_full_filter(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    page = _api(client_logged_in, "a", per_page=2, page=1)
    full = _api(client_logged_in, "a", per_page=50, page=1)
    assert page["total_count"] == full["total_count"]
    assert len(page["returns"]) <= 2
    assert page["total_count"] == full["total_count"]
