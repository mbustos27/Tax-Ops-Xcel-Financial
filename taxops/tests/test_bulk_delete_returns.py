"""DEL-3: admin mass-delete returns from dashboard selection."""
from __future__ import annotations

from werkzeug.security import generate_password_hash

from db import get_connection


def _seed_return(conn, rid: int, client_id: int, log: str = None):
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, created_at, updated_at)
        VALUES (?, 'BulkDel', 'Test', datetime('now'), datetime('now'))
        """,
        (client_id,),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, log_number, tax_year, client_status, created_at, updated_at
        ) VALUES (?, ?, ?, 2025, 'PROCESSING', datetime('now'), datetime('now'))
        """,
        (rid, client_id, log if log is not None else str(9000 + rid)),
    )
    conn.execute(
        "INSERT INTO notes (return_id, note_text, created_at) VALUES (?, 'n', datetime('now'))",
        (rid,),
    )
    conn.commit()


def _seed_user(taxops_db_path: str, username: str, role: str) -> None:
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users
            (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pw12345"), username.title(), role),
    )
    conn.commit()
    conn.close()


def _login_as(client, username: str) -> None:
    client.post("/login", data={"username": username, "password": "pw12345"}, follow_redirects=True)


def test_admin_bulk_delete_happy_path(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 901, 801)
    _seed_return(conn, 902, 802)
    conn.close()

    bad = client_logged_in.post(
        "/api/returns/bulk-delete",
        json={"return_ids": [901, 902], "confirm": "delete"},
    )
    assert bad.status_code == 400

    ok = client_logged_in.post(
        "/api/returns/bulk-delete",
        json={"return_ids": [901, 902], "confirm": "DELETE"},
    )
    assert ok.status_code == 200
    body = ok.get_json()
    assert body["success"] is True
    assert body["deleted"] == 2
    assert set(body["deleted_return_ids"]) == {901, 902}

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT COUNT(*) c FROM returns WHERE id IN (901,902)").fetchone()["c"] == 0
    assert conn.execute("SELECT COUNT(*) c FROM notes WHERE return_id IN (901,902)").fetchone()["c"] == 0
    # Clients remain
    assert conn.execute("SELECT COUNT(*) c FROM clients WHERE id IN (801,802)").fetchone()["c"] == 2
    conn.close()


def test_bulk_delete_rolls_back_if_any_missing(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _seed_return(conn, 903, 803)
    conn.close()

    rv = client_logged_in.post(
        "/api/returns/bulk-delete",
        json={"return_ids": [903, 999991], "confirm": "DELETE"},
    )
    assert rv.status_code == 409
    body = rv.get_json()
    assert body.get("success") is False

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM returns WHERE id=903").fetchone() is not None
    assert conn.execute("SELECT COUNT(*) c FROM notes WHERE return_id=903").fetchone()["c"] == 1
    conn.close()


def test_preparer_cannot_bulk_delete(app, taxops_db_path):
    _seed_user(taxops_db_path, "__bulkdel_prep__", "preparer")
    prep = app.test_client()
    _login_as(prep, "__bulkdel_prep__")

    conn = get_connection(taxops_db_path)
    _seed_return(conn, 904, 804)
    conn.close()

    rv = prep.post(
        "/api/returns/bulk-delete",
        json={"return_ids": [904], "confirm": "DELETE"},
    )
    assert rv.status_code == 403

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM returns WHERE id=904").fetchone() is not None
    conn.close()


def test_dashboard_shows_bulk_delete_for_admin(client_logged_in):
    rv = client_logged_in.get("/?year=2026")
    assert rv.status_code == 200
    assert b"btn-bulk-delete-open" in rv.data
    assert b"bulk-delete-modal" in rv.data
