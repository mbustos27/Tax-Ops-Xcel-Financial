"""DEL-1 / DEL-2: hard-delete returns and clients (admin-only)."""
from __future__ import annotations

from werkzeug.security import generate_password_hash

from db import get_connection


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


def _seed_client_with_returns(taxops_db_path: str, *, with_log: bool = True) -> tuple[int, list[int]]:
    conn = get_connection(taxops_db_path)
    cur = conn.execute(
        "INSERT INTO clients (last_name, first_name, display_name, created_at, updated_at) "
        "VALUES ('Delete', 'Me', 'Delete, Me', '2026-01-01', '2026-01-01')"
    )
    client_id = int(cur.lastrowid)
    ret_ids: list[int] = []
    for year, log in ((2024, "9001" if with_log else None), (2025, "9002" if with_log else None)):
        cur = conn.execute(
            "INSERT INTO returns (client_id, tax_year, log_number, client_status, created_at, updated_at) "
            "VALUES (?, ?, ?, 'PROCESSING', '2026-01-01', '2026-01-01')",
            (client_id, year, log),
        )
        rid = int(cur.lastrowid)
        ret_ids.append(rid)
        conn.execute(
            "INSERT INTO notes (return_id, note_text, created_at) VALUES (?, 'note', '2026-01-01')",
            (rid,),
        )
        conn.execute(
            "INSERT INTO return_documents (return_id, filename, original_filename, "
            "doc_type, file_path, uploaded_at) VALUES (?, 'a.pdf', 'a.pdf', 'W2', "
            "'/tmp/a.pdf', '2026-01-01')",
            (rid,),
        )
    conn.execute(
        "INSERT INTO spouses (client_id, last_name, first_name) "
        "VALUES (?, 'Spouse', 'Test')",
        (client_id,),
    )
    # Soft-linked work order (must survive client delete with NULL client_id)
    conn.execute(
        """
        INSERT INTO work_orders
            (work_order_number, client_id, client_name, date_created, total_fee,
             status, created_by_user_id, created_at, updated_at)
        VALUES ('WO-DEL01', ?, 'Delete, Me', '2026-01-15', 50, 'open', NULL,
                '2026-01-01', '2026-01-01')
        """,
        (client_id,),
    )
    conn.commit()
    conn.close()
    return client_id, ret_ids


def test_admin_delete_return_with_log_number(client_logged_in, taxops_db_path):
    _client_id, ret_ids = _seed_client_with_returns(taxops_db_path, with_log=True)
    rid = ret_ids[0]

    wrong = client_logged_in.post(
        f"/api/return/{rid}/delete",
        json={"confirm_log_number": "WRONG"},
    )
    assert wrong.status_code == 400

    ok = client_logged_in.post(
        f"/api/return/{rid}/delete",
        json={"confirm_log_number": "9001"},
    )
    assert ok.status_code == 200
    assert ok.get_json()["deleted_return_id"] == rid

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM returns WHERE id=?", (rid,)).fetchone() is None
    assert conn.execute("SELECT COUNT(*) c FROM notes WHERE return_id=?", (rid,)).fetchone()["c"] == 0
    assert conn.execute(
        "SELECT COUNT(*) c FROM return_documents WHERE return_id=?", (rid,)
    ).fetchone()["c"] == 0
    # Sibling return + client remain
    assert conn.execute("SELECT 1 FROM returns WHERE id=?", (ret_ids[1],)).fetchone() is not None
    assert conn.execute("SELECT 1 FROM clients WHERE id=?", (_client_id,)).fetchone() is not None
    conn.close()


def test_admin_delete_return_without_log_uses_return_id(client_logged_in, taxops_db_path):
    _client_id, ret_ids = _seed_client_with_returns(taxops_db_path, with_log=False)
    rid = ret_ids[0]

    # Wrong path: log confirm when there is no log
    bad = client_logged_in.post(
        f"/api/return/{rid}/delete",
        json={"confirm_log_number": str(rid)},
    )
    assert bad.status_code == 400

    ok = client_logged_in.post(
        f"/api/return/{rid}/delete",
        json={"confirm_return_id": str(rid)},
    )
    assert ok.status_code == 200


def test_preparer_cannot_delete_return(app, taxops_db_path):
    _seed_user(taxops_db_path, "__del_prep__", "preparer")
    prep = app.test_client()
    _login_as(prep, "__del_prep__")
    _cid, ret_ids = _seed_client_with_returns(taxops_db_path, with_log=True)
    rid = ret_ids[0]

    resp = prep.post(f"/api/return/{rid}/delete", json={"confirm_log_number": "9001"})
    assert resp.status_code == 403

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM returns WHERE id=?", (rid,)).fetchone() is not None
    conn.close()


def test_admin_delete_client_cascades_returns(client_logged_in, taxops_db_path):
    client_id, ret_ids = _seed_client_with_returns(taxops_db_path, with_log=True)

    wrong = client_logged_in.post(
        f"/api/clients/{client_id}/delete",
        json={"confirm_client_id": "999999"},
    )
    assert wrong.status_code == 400

    ok = client_logged_in.post(
        f"/api/clients/{client_id}/delete",
        json={"confirm_client_id": str(client_id)},
    )
    assert ok.status_code == 200
    body = ok.get_json()
    assert body["deleted_client_id"] == client_id
    assert body["deleted_return_count"] == 2
    assert set(body["deleted_return_ids"]) == set(ret_ids)

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM clients WHERE id=?", (client_id,)).fetchone() is None
    for rid in ret_ids:
        assert conn.execute("SELECT 1 FROM returns WHERE id=?", (rid,)).fetchone() is None
        assert conn.execute("SELECT COUNT(*) c FROM notes WHERE return_id=?", (rid,)).fetchone()["c"] == 0
    # Work order preserved; client link cleared
    wo = conn.execute(
        "SELECT client_id FROM work_orders WHERE work_order_number='WO-DEL01'"
    ).fetchone()
    assert wo is not None
    assert wo["client_id"] is None
    conn.close()


def test_preparer_cannot_delete_client(app, taxops_db_path):
    _seed_user(taxops_db_path, "__del_prep2__", "preparer")
    prep = app.test_client()
    _login_as(prep, "__del_prep2__")
    client_id, _ret_ids = _seed_client_with_returns(taxops_db_path, with_log=True)

    resp = prep.post(
        f"/api/clients/{client_id}/delete",
        json={"confirm_client_id": str(client_id)},
    )
    assert resp.status_code == 403

    conn = get_connection(taxops_db_path)
    assert conn.execute("SELECT 1 FROM clients WHERE id=?", (client_id,)).fetchone() is not None
    conn.close()


def test_delete_missing_client_404(client_logged_in):
    resp = client_logged_in.post(
        "/api/clients/999999/delete",
        json={"confirm_client_id": "999999"},
    )
    assert resp.status_code == 404
