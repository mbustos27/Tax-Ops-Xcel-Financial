"""Restart-safety presence + assessment (reception branch)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db import get_connection, init_db
from restart_guard import assess_restart, touch_presence
from utils import now


def test_assess_safe_on_empty_db(taxops_db_path):
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
    assert any(d.get("kind") == "status" and d.get("return_id") == 1 for d in a.write_details)
    assert a.next_steps
    assert "ok" in a.db_lock


def test_junk_and_future_timestamps_do_not_block(taxops_db_path):
    """Office DB has bad status_events rows; string >= cutoff wrongly counted them."""
    from restart_guard import _is_recent

    assert not _is_recent("database is locked", 3)
    assert not _is_recent("2027-03-16", 3)
    assert not _is_recent("2026-09-14T17:38:58+00:00", 3)

    conn = get_connection(taxops_db_path)
    init_db(conn)
    old = "2026-01-01T00:00:00+00:00"
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, created_at, updated_at)
        VALUES (1, 'Test', 'Client', ?, ?)
        """,
        (old, old),
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, updated_at)
        VALUES (1, 1, '1', 2025, 'PICKUP', ?)
        """,
        (old,),
    )
    conn.execute(
        """
        INSERT INTO status_events (return_id, event_type, old_status, new_status, event_timestamp, source_file)
        VALUES
          (1, 'STATUS_CHANGED', 'A', 'B', 'database is locked', '4'),
          (1, 'LOGGED_OUT', NULL, NULL, '2027-03-16', 'CSMDATA.csv'),
          (1, 'STATUS_CHANGED', 'X', 'Y', '2026-09-14T17:38:58+00:00', 'old')
        """
    )
    conn.commit()
    a = assess_restart(conn, db_path=taxops_db_path, idle_minutes=5, write_minutes=3)
    conn.close()
    assert not any(w["key"] == "status_changes" for w in a.recent_writes)
    assert not any(d.get("kind") == "status" for d in a.write_details)
    assert a.verdict in ("SAFE", "CAUTION")


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


def test_api_restart_safe_admin_ok(client_logged_in):
    r = client_logged_in.get("/api/ops/restart-safe")
    assert r.status_code == 200
    body = r.get_json()
    assert body["verdict"] in ("SAFE", "CAUTION", "WAIT")
    assert "summary" in body


def test_api_restart_safe_staff_forbidden(client, taxops_db_path):
    from db import get_connection
    from werkzeug.security import generate_password_hash

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'receptionist', 1, '2025-01-01T00:00:00Z')
        """,
        ("front", generate_password_hash("desk"), "Front Desk"),
    )
    conn.commit()
    conn.close()
    client.post("/login", data={"username": "front", "password": "desk"})
    r = client.get("/api/ops/restart-safe")
    assert r.status_code in (403, 302)
