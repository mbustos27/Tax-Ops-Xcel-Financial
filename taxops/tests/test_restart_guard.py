"""Restart-safety presence + assessment."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db import get_connection, init_db
from restart_guard import assess_restart, touch_presence
from utils import now


def test_required_role_restart_paths():
    from auth import required_role_for_path

    assert required_role_for_path("/ops/restart-check") == "admin"
    assert required_role_for_path("/api/ops/restart-safe") == "admin"


def test_assess_safe_on_empty_db(taxops_db_path, monkeypatch):
    monkeypatch.setenv("TAXOPS_DB", taxops_db_path)
    conn = get_connection(taxops_db_path)
    init_db(conn)
    a = assess_restart(conn, db_path=taxops_db_path, idle_minutes=5, write_minutes=3)
    conn.close()
    assert a.verdict == "SAFE"
    assert a.safe_to_restart is True
    assert a.recent_writes == []
    assert a.active_staff == []


def test_presence_and_recent_write_wait(taxops_db_path):
    conn = get_connection(taxops_db_path)
    init_db(conn)
    touch_presence(
        conn,
        username="maria",
        path="/pickup/12",
        method="POST",
        ip="192.168.1.10",
        force=True,
    )
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, created_at, updated_at)
        VALUES (1, 'Test', 'Client', ?, ?)
        """,
        (now(), now()),
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, updated_at)
        VALUES (1, 1, '1', 2025, 'PICKUP', ?)
        """,
        (now(),),
    )
    conn.execute(
        """
        INSERT INTO status_events (return_id, event_type, old_status, new_status, event_timestamp)
        VALUES (1, 'status', 'FINALIZE', 'PICKUP', ?)
        """,
        (now(),),
    )
    conn.commit()
    a = assess_restart(conn, db_path=taxops_db_path, idle_minutes=5, write_minutes=3)
    conn.close()
    assert a.verdict == "WAIT"
    assert a.safe_to_restart is False
    assert any(s["username"] == "maria" for s in a.active_staff)
    assert any(w["key"] == "status_changes" for w in a.recent_writes)


def test_browse_only_is_caution(taxops_db_path):
    conn = get_connection(taxops_db_path)
    init_db(conn)
    touch_presence(
        conn,
        username="front",
        path="/",
        method="GET",
        force=True,
    )
    # Age the write stamp into the past so only browsing remains.
    old = (datetime.now(timezone.utc) - timedelta(minutes=30)).isoformat(timespec="seconds")
    conn.execute(
        "UPDATE staff_presence SET last_write_at = ? WHERE username = ?",
        (old, "front"),
    )
    conn.commit()
    a = assess_restart(conn, db_path=taxops_db_path, idle_minutes=5, write_minutes=3)
    conn.close()
    assert a.verdict == "CAUTION"
    assert a.safe_to_restart is True


def test_api_restart_safe_admin_only(client, monkeypatch):
    monkeypatch.setenv("TAXOPS_USER", "boss")
    monkeypatch.setenv("TAXOPS_PASS", "secret")
    monkeypatch.setenv("TAXOPS_USERS", "boss:secret:admin;front:desk:staff")
    # staff blocked
    client.post("/login", data={"username": "front", "password": "desk"})
    r = client.get("/api/ops/restart-safe")
    assert r.status_code in (403, 302)
    client.get("/logout")
    # admin ok
    client.post("/login", data={"username": "boss", "password": "secret"})
    r = client.get("/api/ops/restart-safe")
    assert r.status_code == 200
    body = r.get_json()
    assert body["verdict"] in ("SAFE", "CAUTION", "WAIT")
    assert "summary" in body
