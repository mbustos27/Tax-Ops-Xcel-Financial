"""ONBOARD-1..4: Staff onboarding — user management, forced password change, nav, orientation."""
from __future__ import annotations

import pytest
from werkzeug.security import generate_password_hash, check_password_hash


# ── Helpers ───────────────────────────────────────────────────────────────────

def _seed_user(db_path, username="staff1", password="temp1234", role="staff",
               must_change=0, has_seen_orientation=0, is_active=1):
    from db import get_connection
    from utils import now
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users
            (username, password_hash, display_name, role, is_active, created_at,
             must_change_password, has_seen_orientation)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (username, generate_password_hash(password),
         username.capitalize(), role, is_active, now(),
         must_change, has_seen_orientation),
    )
    conn.commit()
    conn.close()


# ── ONBOARD-1: schema migration ───────────────────────────────────────────────

def test_must_change_password_column_exists(taxops_db_path):
    """must_change_password column is present after init_db migration."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA table_info(auth_users)").fetchall()
    col_names = {r["name"] for r in row}
    conn.close()
    assert "must_change_password" in col_names


def test_has_seen_orientation_column_exists(taxops_db_path):
    """has_seen_orientation column is present after init_db migration."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA table_info(auth_users)").fetchall()
    col_names = {r["name"] for r in row}
    conn.close()
    assert "has_seen_orientation" in col_names


# ── ONBOARD-1: /admin/users page ──────────────────────────────────────────────

def test_admin_users_page_requires_login(client):
    """GET /admin/users redirects unauthenticated users to /login."""
    r = client.get("/admin/users")
    assert r.status_code in (302, 301)
    assert "login" in r.headers["Location"].lower()


def test_admin_users_page_accessible_as_admin(client_logged_in):
    """GET /admin/users renders for an admin user."""
    r = client_logged_in.get("/admin/users")
    assert r.status_code == 200
    assert b"Staff Accounts" in r.data


# ── ONBOARD-1: create user API ────────────────────────────────────────────────

def test_create_user_success(client_logged_in, taxops_db_path):
    """POST /api/admin/users creates a new user with must_change_password=1."""
    r = client_logged_in.post(
        "/api/admin/users",
        json={"username": "newstaff", "display_name": "New Staff",
              "role": "staff", "temporary_password": "temp1234"},
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["success"] is True
    assert "user_id" in data

    from db import get_connection
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT * FROM auth_users WHERE username = 'newstaff'").fetchone()
    conn.close()
    assert row is not None
    assert row["must_change_password"] == 1
    assert "password" not in (row["password_hash"] or "").lower()  # never plaintext


def test_create_user_duplicate_username(client_logged_in, taxops_db_path):
    """POST /api/admin/users returns 409 for duplicate username."""
    _seed_user(taxops_db_path, username="dupuser")
    r = client_logged_in.post(
        "/api/admin/users",
        json={"username": "dupuser", "role": "staff", "temporary_password": "temp1234"},
    )
    assert r.status_code == 409


def test_create_user_short_password(client_logged_in):
    """POST /api/admin/users rejects passwords shorter than 8 chars."""
    r = client_logged_in.post(
        "/api/admin/users",
        json={"username": "x", "role": "staff", "temporary_password": "short"},
    )
    assert r.status_code == 400


def test_create_user_invalid_role(client_logged_in):
    """POST /api/admin/users rejects invalid role values."""
    r = client_logged_in.post(
        "/api/admin/users",
        json={"username": "y", "role": "superadmin", "temporary_password": "temp1234"},
    )
    assert r.status_code == 400


# ── ONBOARD-1: deactivate / reactivate ───────────────────────────────────────

def test_deactivate_user(client_logged_in, taxops_db_path):
    """POST /api/admin/users/<id>/deactivate sets is_active=0."""
    _seed_user(taxops_db_path, username="todeactivate")
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute("SELECT id FROM auth_users WHERE username='todeactivate'").fetchone()["id"]
    conn.close()
    r = client_logged_in.post(f"/api/admin/users/{uid}/deactivate")
    assert r.status_code == 200
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT is_active FROM auth_users WHERE id=?", (uid,)).fetchone()
    conn.close()
    assert row["is_active"] == 0


def test_cannot_deactivate_self(client_logged_in, taxops_db_path):
    """Admin cannot deactivate their own account."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute(
        "SELECT id FROM auth_users WHERE username='__test_user__'"
    ).fetchone()["id"]
    conn.close()
    r = client_logged_in.post(f"/api/admin/users/{uid}/deactivate")
    assert r.status_code == 400


def test_reactivate_user(client_logged_in, taxops_db_path):
    """POST /api/admin/users/<id>/reactivate sets is_active=1."""
    _seed_user(taxops_db_path, username="inactive1", is_active=0)
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute("SELECT id FROM auth_users WHERE username='inactive1'").fetchone()["id"]
    conn.close()
    r = client_logged_in.post(f"/api/admin/users/{uid}/reactivate")
    assert r.status_code == 200
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT is_active FROM auth_users WHERE id=?", (uid,)).fetchone()
    conn.close()
    assert row["is_active"] == 1


# ── ONBOARD-1: reset password ─────────────────────────────────────────────────

def test_reset_password_sets_must_change(client_logged_in, taxops_db_path):
    """POST reset-password updates hash and sets must_change_password=1."""
    _seed_user(taxops_db_path, username="resetme", must_change=0)
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute("SELECT id FROM auth_users WHERE username='resetme'").fetchone()["id"]
    conn.close()
    r = client_logged_in.post(
        f"/api/admin/users/{uid}/reset-password",
        json={"temporary_password": "newtemp99"},
    )
    assert r.status_code == 200
    data = r.get_json()
    assert data["success"] is True
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT must_change_password, password_hash FROM auth_users WHERE id=?", (uid,)).fetchone()
    conn.close()
    assert row["must_change_password"] == 1
    assert check_password_hash(row["password_hash"], "newtemp99")


def test_reset_password_never_returns_hash(client_logged_in, taxops_db_path):
    """Reset password API response must not contain password_hash."""
    _seed_user(taxops_db_path, username="hashcheck")
    from db import get_connection
    conn = get_connection(taxops_db_path)
    uid = conn.execute("SELECT id FROM auth_users WHERE username='hashcheck'").fetchone()["id"]
    conn.close()
    r = client_logged_in.post(
        f"/api/admin/users/{uid}/reset-password",
        json={"temporary_password": "newtemp99"},
    )
    assert b"password_hash" not in r.data


# ── ONBOARD-2: forced password change ────────────────────────────────────────

def test_login_with_must_change_redirects_to_change_password(client, taxops_db_path):
    """Login with must_change_password=1 redirects to /change-password."""
    _seed_user(taxops_db_path, username="forcedchange", password="temp1234", must_change=1)
    r = client.post(
        "/login",
        data={"username": "forcedchange", "password": "temp1234"},
        follow_redirects=False,
    )
    assert r.status_code == 302
    assert "change-password" in r.headers["Location"]


def test_change_password_accessible_after_login(client, taxops_db_path):
    """GET /change-password returns 200 when user is logged in."""
    _seed_user(taxops_db_path, username="changepw2", password="temp1234", must_change=1)
    client.post("/login", data={"username": "changepw2", "password": "temp1234"})
    r = client.get("/change-password")
    assert r.status_code == 200
    assert b"Set Your Password" in r.data


def test_change_password_success_clears_flag(client, taxops_db_path):
    """POST /change-password clears must_change_password flag on success."""
    _seed_user(taxops_db_path, username="changepw3", password="temp1234",
               must_change=1, has_seen_orientation=1)
    client.post("/login", data={"username": "changepw3", "password": "temp1234"})
    r = client.post(
        "/change-password",
        data={"new_password": "newpassword1", "confirm_password": "newpassword1"},
        follow_redirects=False,
    )
    assert r.status_code == 302
    from db import get_connection
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT must_change_password FROM auth_users WHERE username='changepw3'").fetchone()
    conn.close()
    assert row["must_change_password"] == 0


def test_change_password_rejects_short(client, taxops_db_path):
    """POST /change-password rejects passwords < 8 chars."""
    _seed_user(taxops_db_path, username="shortpw", password="temp1234", must_change=1)
    client.post("/login", data={"username": "shortpw", "password": "temp1234"})
    r = client.post(
        "/change-password",
        data={"new_password": "abc", "confirm_password": "abc"},
    )
    assert r.status_code == 200
    assert b"8 characters" in r.data


def test_change_password_rejects_mismatch(client, taxops_db_path):
    """POST /change-password rejects non-matching confirm password."""
    _seed_user(taxops_db_path, username="mismatchpw", password="temp1234", must_change=1)
    client.post("/login", data={"username": "mismatchpw", "password": "temp1234"})
    r = client.post(
        "/change-password",
        data={"new_password": "newpassword1", "confirm_password": "different123"},
    )
    assert r.status_code == 200
    assert b"do not match" in r.data


def test_change_password_rejects_same_as_temp(client, taxops_db_path):
    """POST /change-password rejects same password as the current one."""
    _seed_user(taxops_db_path, username="samepw", password="temp1234", must_change=1)
    client.post("/login", data={"username": "samepw", "password": "temp1234"})
    r = client.post(
        "/change-password",
        data={"new_password": "temp1234", "confirm_password": "temp1234"},
    )
    assert r.status_code == 200
    assert b"different from the temporary" in r.data


def test_must_change_password_guard_blocks_other_routes(client, taxops_db_path):
    """Accessing /review while must_change_password=True redirects to /change-password."""
    _seed_user(taxops_db_path, username="guardtest", password="temp1234", must_change=1)
    client.post("/login", data={"username": "guardtest", "password": "temp1234"})
    r = client.get("/review", follow_redirects=False)
    assert r.status_code == 302
    assert "change-password" in r.headers["Location"]


# ── ONBOARD-4: orientation screen ────────────────────────────────────────────

def test_orientation_shown_after_first_password_change(client, taxops_db_path):
    """After password change with has_seen_orientation=0 user is redirected to /orientation."""
    _seed_user(taxops_db_path, username="oritest", password="temp1234",
               must_change=1, has_seen_orientation=0)
    client.post("/login", data={"username": "oritest", "password": "temp1234"})
    r = client.post(
        "/change-password",
        data={"new_password": "newpassword1", "confirm_password": "newpassword1"},
        follow_redirects=False,
    )
    assert r.status_code == 302
    assert "orientation" in r.headers["Location"]


def test_orientation_dismissed_sets_flag(client, taxops_db_path):
    """POST /orientation/dismiss sets has_seen_orientation=1."""
    _seed_user(taxops_db_path, username="oritest2", password="pass1234",
               must_change=0, has_seen_orientation=0)
    client.post("/login", data={"username": "oritest2", "password": "pass1234"})
    r = client.post("/orientation/dismiss", follow_redirects=False)
    assert r.status_code == 302
    from db import get_connection
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT has_seen_orientation FROM auth_users WHERE username='oritest2'"
    ).fetchone()
    conn.close()
    assert row["has_seen_orientation"] == 1


def test_orientation_skipped_for_returning_user(client, taxops_db_path):
    """GET /orientation redirects to dashboard if has_seen_orientation=1."""
    _seed_user(taxops_db_path, username="oritest3", password="pass1234",
               must_change=0, has_seen_orientation=1)
    client.post("/login", data={"username": "oritest3", "password": "pass1234"})
    r = client.get("/orientation", follow_redirects=False)
    assert r.status_code == 302
    assert r.headers["Location"].endswith("/") or "dashboard" in r.headers["Location"]
