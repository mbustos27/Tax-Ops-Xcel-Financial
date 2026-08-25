"""Dashboard server-side sort covers the full filtered year set."""
from __future__ import annotations

from db import get_connection


def _seed(taxops_db_path: str) -> None:
    conn = get_connection(taxops_db_path)
    # Deliberately misaligned: log order ≠ name order ≠ fee/balance order
    rows = [
        # rid, last, first, log, tax_year, fee, paid → balance
        (1, "Zebra", "Ann", "100", 2024, 500.0, 0.0),    # bal 500
        (2, "Alpha", "Bob", "300", 2025, 100.0, 50.0),   # bal 50
        (3, "Mike", "Carol", "200", 2023, 300.0, 300.0), # bal 0
    ]
    for rid, ln, fn, log, ty, fee, paid in rows:
        conn.execute(
            "INSERT INTO clients (id, last_name, first_name, display_name, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, '2026-01-01', '2026-01-01')",
            (rid, ln, fn, f"{ln}, {fn}"),
        )
        conn.execute(
            "INSERT INTO returns (id, client_id, tax_year, log_number, client_status, "
            "intake_date, created_at, updated_at) "
            "VALUES (?, ?, ?, ?, 'PROCESSING', '2026-02-01', '2026-01-01', '2026-01-01')",
            (rid, rid, ty, log),
        )
        conn.execute(
            "INSERT INTO payments (return_id, total_fee, fee_paid) VALUES (?, ?, ?)",
            (rid, fee, paid),
        )
    conn.commit()
    conn.close()


def _logs(client, sort: str) -> list[str]:
    resp = client.get(f"/api/dashboard/returns?year=2026&sort={sort}&per_page=50")
    assert resp.status_code == 200
    return [r["log_number"] for r in resp.get_json()["returns"]]


def test_sort_by_name_full_set(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    # Alpha(300), Mike(200), Zebra(100)
    assert _logs(client_logged_in, "name_asc") == ["300", "200", "100"]
    assert _logs(client_logged_in, "name_desc") == ["100", "200", "300"]


def test_sort_by_fee_full_set(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    # fees: 300→100, 200→300, 100→500
    assert _logs(client_logged_in, "fee_asc") == ["300", "200", "100"]
    assert _logs(client_logged_in, "fee_desc") == ["100", "200", "300"]


def test_sort_by_balance_full_set(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    # balances: 200→0, 300→50, 100→500
    assert _logs(client_logged_in, "balance_asc") == ["200", "300", "100"]


def test_sort_by_log_still_works(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    assert _logs(client_logged_in, "log_asc") == ["100", "200", "300"]
    assert _logs(client_logged_in, "log_desc") == ["300", "200", "100"]


def test_sort_unknown_falls_back_to_log(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    assert _logs(client_logged_in, "not_a_real_sort") == ["100", "200", "300"]


def test_filter_plus_sort_still_full_corpus(client_logged_in, taxops_db_path):
    _seed(taxops_db_path)
    resp = client_logged_in.get(
        "/api/dashboard/returns?year=2026&q=alpha&sort=fee_desc&per_page=50"
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["total_count"] == 1
    assert data["returns"][0]["log_number"] == "300"
