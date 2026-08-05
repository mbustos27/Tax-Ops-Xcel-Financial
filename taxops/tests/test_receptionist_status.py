"""Reception status-change RBAC (Feature 1)."""

from __future__ import annotations

from werkzeug.security import generate_password_hash

from app import (
    receptionist_allowed_statuses,
    receptionist_may_set_status,
)


def _seed_user(taxops_db_path: str, username: str, role: str) -> None:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR REPLACE INTO auth_users
          (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pass"), username, role),
    )
    conn.commit()
    conn.close()


def _seed_return(taxops_db_path: str, *, status: str = "PROCESSING") -> int:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (801, 'Recv', 'Status')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (8101, 801, '8101', 2025, ?)
        """,
        (status,),
    )
    conn.commit()
    conn.close()
    return 8101


def _login_as(client, username: str) -> None:
    client.post(
        "/login",
        data={"username": username, "password": "pass"},
        follow_redirects=True,
    )


def test_receptionist_allowed_statuses_safe_pack():
    assert receptionist_allowed_statuses("PROCESSING") == [
        "PROCESSING", "HOLD", "PICKUP",
    ]
    assert receptionist_allowed_statuses("PICKUP") == [
        "PROCESSING", "HOLD", "PICKUP", "EFILE READY",
    ]
    assert "FINALIZE" not in receptionist_allowed_statuses("PROCESSING")
    assert "LOG OUT" not in receptionist_allowed_statuses("PICKUP")
    assert "REJECTED" not in receptionist_allowed_statuses("HOLD")
    assert receptionist_may_set_status("PICKUP", "EFILE READY") is True
    assert receptionist_may_set_status("PROCESSING", "EFILE READY") is False
    assert receptionist_may_set_status("PROCESSING", "LOG OUT") is False


def test_receptionist_can_set_desk_safe_statuses(client, taxops_db_path):
    _seed_user(taxops_db_path, "__recv_status__", "receptionist")
    rid = _seed_return(taxops_db_path, status="PROCESSING")
    _login_as(client, "__recv_status__")

    for target in ("HOLD", "PICKUP", "PROCESSING"):
        rv = client.post(f"/api/return/{rid}/status", json={"status": target})
        assert rv.status_code == 200, (target, rv.get_json())
        assert rv.get_json()["success"] is True


def test_receptionist_pickup_to_efile_ready(client, taxops_db_path):
    _seed_user(taxops_db_path, "__recv_efile__", "receptionist")
    rid = _seed_return(taxops_db_path, status="PICKUP")
    _login_as(client, "__recv_efile__")

    rv = client.post(f"/api/return/{rid}/status", json={"status": "EFILE READY"})
    assert rv.status_code == 200, rv.get_json()


def test_receptionist_forbidden_sensitive_statuses(client, taxops_db_path):
    _seed_user(taxops_db_path, "__recv_forbid__", "receptionist")
    rid = _seed_return(taxops_db_path, status="PROCESSING")
    _login_as(client, "__recv_forbid__")

    for bad in ("FINALIZE", "LOG OUT", "REJECTED", "PENDING INTAKE", "EFILE READY"):
        rv = client.post(f"/api/return/{rid}/status", json={"status": bad})
        assert rv.status_code == 403, (bad, rv.status_code, rv.get_json())


def test_preparer_still_can_set_finalize(client, taxops_db_path):
    _seed_user(taxops_db_path, "__prep_status__", "preparer")
    rid = _seed_return(taxops_db_path, status="PROCESSING")
    _login_as(client, "__prep_status__")

    rv = client.post(f"/api/return/{rid}/status", json={"status": "FINALIZE"})
    assert rv.status_code == 200, rv.get_json()
