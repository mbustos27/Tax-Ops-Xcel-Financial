"""Now Serving M2–M5 — kiosk (public), staff board RBAC, print hook."""
from __future__ import annotations

from werkzeug.security import generate_password_hash

from db import get_connection
from now_serving import issue_ticket, render_now_serving_zpl, reset_day
from routes.now_serving import is_now_serving_public_path


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


def _login(client, username: str) -> None:
    client.post(
        "/login",
        data={"username": username, "password": "pw12345"},
        follow_redirects=True,
    )


def test_public_path_helper():
    assert is_now_serving_public_path("/now-serving/kiosk")
    assert is_now_serving_public_path("/now-serving/kiosk/take")
    assert is_now_serving_public_path("/now-serving/display")
    assert is_now_serving_public_path("/now-serving/api/snapshot")
    assert is_now_serving_public_path("/now-serving/api/events")
    assert not is_now_serving_public_path("/now-serving/board")
    assert not is_now_serving_public_path("/now-serving/api/call-next")


def test_lobby_display_reachable_without_login(client, taxops_db_path):
    reset_day(taxops_db_path)
    resp = client.get("/now-serving/display")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Ahora sirviendo" in html
    assert "speechSynthesis" in html
    assert "/now-serving/api/events" in html


def test_kiosk_reachable_without_login(client, taxops_db_path):
    reset_day(taxops_db_path)
    resp = client.get("/now-serving/kiosk")
    assert resp.status_code == 200
    html = resp.get_data(as_text=True)
    assert "Take a Number" in html or "take-btn" in html
    assert "apple-mobile-web-app-capable" in html


def test_kiosk_take_issues_ticket_without_login(client, taxops_db_path, monkeypatch):
    reset_day(taxops_db_path)
    printed: list[tuple] = []
    monkeypatch.setattr(
        "routes.now_serving.try_print_now_serving_ticket",
        lambda label, window: printed.append((label, window)) or True,
    )
    resp = client.post("/now-serving/kiosk/take", json={})
    assert resp.status_code == 200, resp.get_data(as_text=True)
    data = resp.get_json()
    assert data["success"] is True
    assert data["label"] == "1"
    assert data["window"] == 1
    assert data["printed"] is True
    assert printed == [("1", 1)]


def test_snapshot_and_events_public(client, taxops_db_path):
    reset_day(taxops_db_path)
    issue_ticket(taxops_db_path)
    snap = client.get("/now-serving/api/snapshot")
    assert snap.status_code == 200
    body = snap.get_json()
    assert "revision" in body
    assert "windows" in body

    # Read a short SSE burst — first event should be a data: line.
    ev = client.get("/now-serving/api/events", buffered=False)
    assert ev.status_code == 200
    assert ev.mimetype == "text/event-stream"
    # Consume a small chunk then abandon (generator has 1h cap; client closes).
    chunk = next(ev.response)
    text = chunk.decode("utf-8") if isinstance(chunk, (bytes, bytearray)) else str(chunk)
    assert text.startswith("data:") or text.startswith(":")


def test_board_requires_login_allows_receptionist(client, taxops_db_path):
    from now_serving import reset_day
    reset_day(taxops_db_path)

    anon = client.get("/now-serving/board", follow_redirects=False)
    assert anon.status_code in (302, 401)

    _seed_user(taxops_db_path, "__ns_recep__", "receptionist")
    _login(client, "__ns_recep__")
    # Legacy board URL redirects home with modal query — reception is allowed.
    ok = client.get("/now-serving/board", follow_redirects=False)
    assert ok.status_code == 302
    assert "open_now_serving=1" in (ok.headers.get("Location") or "")

    call = client.post("/now-serving/api/call-next", json={"window": 1})
    # Empty queue → 409, but not 403
    assert call.status_code in (200, 409)
    assert call.status_code != 403


def test_board_requires_preparer(client, taxops_db_path):
    # Kept name for compatibility — reception is now allowed; verify preparer still works.
    from now_serving import reset_day, issue_ticket
    reset_day(taxops_db_path)
    issue_ticket(taxops_db_path)

    _seed_user(taxops_db_path, "__ns_prep__", "preparer")
    _login(client, "__ns_prep__")
    ok = client.get("/now-serving/board", follow_redirects=False)
    assert ok.status_code == 302
    r = client.post("/now-serving/api/call-next", json={"window": 1})
    assert r.status_code == 200


def test_preparer_call_next_and_transfer(client, taxops_db_path):
    reset_day(taxops_db_path)
    issue_ticket(taxops_db_path)  # 1 W1
    issue_ticket(taxops_db_path)  # 2 W2
    issue_ticket(taxops_db_path)  # 3 W1

    _seed_user(taxops_db_path, "__ns_prep2__", "preparer")
    _login(client, "__ns_prep2__")

    r1 = client.post("/now-serving/api/call-next", json={"window": 1})
    assert r1.status_code == 200
    t = r1.get_json()["ticket"]
    assert t["label"] == "1"

    xfer = client.post("/now-serving/api/transfer", json={"ticket_id": t["id"]})
    assert xfer.status_code == 200
    moved = xfer.get_json()["ticket"]
    assert moved["window"] == 2
    assert moved["status"] == "waiting"


def test_reset_admin_only(client, taxops_db_path):
    reset_day(taxops_db_path)
    issue_ticket(taxops_db_path)

    _seed_user(taxops_db_path, "__ns_prep3__", "preparer")
    _login(client, "__ns_prep3__")
    forbidden = client.post("/now-serving/api/reset", json={"confirm": True})
    assert forbidden.status_code == 403

    client.get("/logout", follow_redirects=True)
    _seed_user(taxops_db_path, "__ns_admin__", "admin")
    _login(client, "__ns_admin__")
    ok = client.post("/now-serving/api/reset", json={"confirm": True})
    assert ok.status_code == 200
    assert ok.get_json()["success"] is True
    snap = ok.get_json()["snapshot"]
    assert snap["state"]["next_number"] == 1


def test_render_now_serving_zpl_matches_log_geometry_no_window():
    zpl = render_now_serving_zpl(label="14", window=2)
    assert "^XA" in zpl and "^XZ" in zpl
    assert "14" in zpl
    assert "^PW532" in zpl
    assert "^LL203" in zpl
    assert "Window" not in zpl
    assert "Xcel Financial Services, LLC" in zpl
    assert "Now Serving" not in zpl


def test_call_next_writes_started_audit_and_completes_prior(client, taxops_db_path, monkeypatch):
    """Each Call Next starts a fresh ticket audit trail; prior ticket is closed."""
    import audit_service
    from now_serving import reset_day, issue_ticket

    reset_day(taxops_db_path)
    issue_ticket(taxops_db_path)  # 1 W1
    issue_ticket(taxops_db_path)  # 2 W2
    issue_ticket(taxops_db_path)  # 3 W1

    writes = []
    orig = audit_service._enqueue_write

    def _spy(**kwargs):
        writes.append(kwargs)
        return orig(**kwargs)

    monkeypatch.setattr(audit_service, "_enqueue_write", _spy)

    _seed_user(taxops_db_path, "__ns_audit__", "preparer")
    _login(client, "__ns_audit__")

    r1 = client.post("/now-serving/api/call-next", json={"window": 1})
    assert r1.status_code == 200
    t1 = r1.get_json()["ticket"]
    assert t1["label"] == "1"
    assert t1.get("completed") is None

    started = [w for w in writes if w.get("action") == "NOW_SERVING_STARTED"]
    assert len(started) == 1
    assert started[0]["entity_type"] == "now_serving_ticket"
    assert started[0]["entity_id"] == str(t1["id"])

    r2 = client.post("/now-serving/api/call-next", json={"window": 1})
    assert r2.status_code == 200
    t2 = r2.get_json()["ticket"]
    assert t2["label"] == "3"
    assert t2["completed"]["id"] == t1["id"]

    completed = [w for w in writes if w.get("action") == "NOW_SERVING_COMPLETED"]
    assert any(w["entity_id"] == str(t1["id"]) for w in completed)
    started2 = [w for w in writes if w.get("action") == "NOW_SERVING_STARTED" and w["entity_id"] == str(t2["id"])]
    assert len(started2) == 1


def test_issue_triggers_print_from_kiosk_route(client, taxops_db_path, monkeypatch):
    """M4 acceptance: issuing a ticket triggers print with right number/window."""
    reset_day(taxops_db_path)
    calls = []

    def _fake(label, window):
        calls.append({"label": label, "window": window})
        return True

    monkeypatch.setattr("routes.now_serving.try_print_now_serving_ticket", _fake)
    resp = client.post("/now-serving/kiosk/take", json={})
    assert resp.status_code == 200
    assert calls == [{"label": "1", "window": 1}]
