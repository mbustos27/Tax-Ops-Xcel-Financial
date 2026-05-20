"""TOUR-1..4: Staff onboarding tooltip tour — API routes and state persistence."""
from __future__ import annotations


# ── TOUR-3: /api/tour/status and /api/tour/complete ───────────────────────────

def test_tour_status_unauthenticated(client):
    """GET /api/tour/status returns 401 for unauthenticated users."""
    r = client.get("/api/tour/status")
    assert r.status_code == 401


def test_tour_status_not_completed(client_logged_in):
    """GET /api/tour/status returns completed=false before any completion."""
    r = client_logged_in.get("/api/tour/status")
    assert r.status_code == 200
    data = r.get_json()
    assert data["completed"] is False


def test_tour_complete_marks_done(client_logged_in, taxops_db_path):
    """POST /api/tour/complete writes to app_settings and status returns completed=true."""
    r = client_logged_in.post("/api/tour/complete")
    assert r.status_code == 200
    assert r.get_json()["success"] is True

    r2 = client_logged_in.get("/api/tour/status")
    assert r2.get_json()["completed"] is True


def test_tour_complete_idempotent(client_logged_in):
    """POST /api/tour/complete can be called multiple times without error."""
    client_logged_in.post("/api/tour/complete")
    r = client_logged_in.post("/api/tour/complete")
    assert r.status_code == 200
    assert r.get_json()["success"] is True


def test_tour_status_returns_false_after_reset(client_logged_in, taxops_db_path):
    """After admin resets tour, status returns completed=false again."""
    client_logged_in.post("/api/tour/complete")
    assert client_logged_in.get("/api/tour/status").get_json()["completed"] is True

    # Get user id from DB
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute(
        "SELECT id FROM auth_users WHERE username='__test_user__'"
    ).fetchone()["id"]
    conn.close()

    r = client_logged_in.post(f"/api/admin/users/{uid}/reset-tour")
    assert r.status_code == 200
    assert r.get_json()["success"] is True

    assert client_logged_in.get("/api/tour/status").get_json()["completed"] is False


# ── TOUR-4: admin reset endpoint ──────────────────────────────────────────────

def test_reset_tour_404_for_unknown_user(client_logged_in):
    """POST reset-tour returns 404 for a nonexistent user id."""
    r = client_logged_in.post("/api/admin/users/99999/reset-tour")
    assert r.status_code == 404


def test_reset_tour_requires_login(client):
    """POST reset-tour returns 401 without a session."""
    r = client.post("/api/admin/users/1/reset-tour")
    assert r.status_code == 401


def test_tour_complete_requires_login(client):
    """POST /api/tour/complete returns 401 without a session."""
    r = client.post("/api/tour/complete")
    assert r.status_code == 401


# ── TOUR-3: app_settings storage check ───────────────────────────────────────

def test_tour_reset_self_serve(client_logged_in):
    """POST /api/tour/reset clears completion so status returns false again."""
    client_logged_in.post("/api/tour/complete")
    assert client_logged_in.get("/api/tour/status").get_json()["completed"] is True

    r = client_logged_in.post("/api/tour/reset")
    assert r.status_code == 200
    assert r.get_json()["success"] is True
    assert client_logged_in.get("/api/tour/status").get_json()["completed"] is False


def test_tour_reset_requires_login(client):
    """POST /api/tour/reset returns 401 without a session."""
    r = client.post("/api/tour/reset")
    assert r.status_code == 401


def test_tour_complete_writes_app_settings_key(client_logged_in, taxops_db_path):
    """Completing the tour writes a tour_completed_<id> key to app_settings."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute(
        "SELECT id FROM auth_users WHERE username='__test_user__'"
    ).fetchone()["id"]
    conn.close()

    client_logged_in.post("/api/tour/complete")

    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT value FROM app_settings WHERE key=?", (f"tour_completed_{uid}",)
    ).fetchone()
    conn.close()
    assert row is not None
    assert row["value"]  # non-empty ISO timestamp
