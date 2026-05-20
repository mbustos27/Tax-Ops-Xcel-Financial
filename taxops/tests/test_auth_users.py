"""SEC-2: per-user accounts with hashed passwords tests."""
from __future__ import annotations

import pytest
from werkzeug.security import generate_password_hash


# ── helpers ──────────────────────────────────────────────────────────────────

def _insert_user(conn, username, password, *, role="staff", is_active=1):
    conn.execute(
        """
        INSERT INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, ?, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash(password), username, role, is_active),
    )
    conn.commit()


# ── schema ────────────────────────────────────────────────────────────────────

def test_auth_users_table_exists_after_init(taxops_db_path):
    """SEC-2: auth_users table is created by init_db with all required columns."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    cols = {r["name"] for r in conn.execute("PRAGMA table_info(auth_users)").fetchall()}
    conn.close()

    required = {"id", "username", "password_hash", "role", "is_active", "created_at",
                "last_login_at", "failed_attempts", "locked_until"}
    assert required <= cols, f"Missing columns: {required - cols}"


# ── bootstrap ─────────────────────────────────────────────────────────────────

def test_bootstrap_seeds_user_from_env(taxops_db_path, monkeypatch):
    """SEC-2: bootstrap_auth_user seeds one admin from TAXOPS_USER/TAXOPS_PASS when table is empty."""
    monkeypatch.setenv("TAXOPS_USER", "boot_admin")
    monkeypatch.setenv("TAXOPS_PASS", "s3cret!")
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    seeded = mod.bootstrap_auth_user(conn)
    conn.close()

    assert seeded is True

    conn2 = get_connection(taxops_db_path)
    row = conn2.execute("SELECT * FROM auth_users WHERE username = 'boot_admin'").fetchone()
    conn2.close()
    assert row is not None
    assert row["role"] == "admin"
    assert row["is_active"] == 1


def test_bootstrap_is_noop_when_users_exist(taxops_db_path, monkeypatch):
    """SEC-2: bootstrap_auth_user is a no-op if the table already has at least one user."""
    monkeypatch.setenv("TAXOPS_USER", "boot_admin")
    monkeypatch.setenv("TAXOPS_PASS", "s3cret!")
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "existing_user", "pw123")
    seeded = mod.bootstrap_auth_user(conn)
    conn.close()

    assert seeded is False


# ── _authenticate_user ────────────────────────────────────────────────────────

def test_authenticate_correct_password(taxops_db_path):
    """SEC-2: _authenticate_user returns user dict on correct password."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "alice", "correct_pw")
    conn.close()

    result = mod._authenticate_user("alice", "correct_pw")
    assert result is not None
    assert result["username"] == "alice"


def test_authenticate_wrong_password_returns_none(taxops_db_path):
    """SEC-2: _authenticate_user returns None on wrong password and increments failed_attempts."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "bob", "correct_pw")
    conn.close()

    result = mod._authenticate_user("bob", "wrong_pw")
    assert result is None

    conn2 = get_connection(taxops_db_path)
    row = conn2.execute("SELECT failed_attempts FROM auth_users WHERE username='bob'").fetchone()
    conn2.close()
    assert row["failed_attempts"] == 1


def test_authenticate_inactive_user_returns_none(taxops_db_path):
    """SEC-2: _authenticate_user rejects users with is_active = 0."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "carol", "pw", is_active=0)
    conn.close()

    result = mod._authenticate_user("carol", "pw")
    assert result is None


def test_authenticate_unknown_user_returns_none(taxops_db_path):
    """SEC-2: _authenticate_user returns None for a username not in the table."""
    import app as mod

    result = mod._authenticate_user("ghost_user", "whatever")
    assert result is None


def test_authenticate_updates_last_login_at(taxops_db_path):
    """SEC-2: successful auth updates last_login_at timestamp in auth_users."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "diana", "pw456")
    conn.close()

    mod._authenticate_user("diana", "pw456")

    conn2 = get_connection(taxops_db_path)
    row = conn2.execute("SELECT last_login_at FROM auth_users WHERE username='diana'").fetchone()
    conn2.close()
    assert row["last_login_at"] is not None


# ── login route ───────────────────────────────────────────────────────────────

def test_login_route_accepts_hashed_user(client, taxops_db_path):
    """SEC-2: POST /login with correct credentials stored in auth_users succeeds (redirects to dashboard)."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "frank", "fw_pass", role="staff")
    conn.close()

    rv = client.post(
        "/login",
        data={"username": "frank", "password": "fw_pass"},
        follow_redirects=False,
    )
    assert rv.status_code in (301, 302), (
        f"Expected redirect after valid login but got {rv.status_code}"
    )


def test_login_route_rejects_wrong_password(client, taxops_db_path):
    """SEC-2: POST /login with wrong password re-renders the login page without redirecting."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "grace", "real_pass")
    conn.close()

    rv = client.post(
        "/login",
        data={"username": "grace", "password": "bad_pass"},
        follow_redirects=False,
    )
    assert rv.status_code == 200, (
        f"Expected 200 (re-render login) on bad password but got {rv.status_code}"
    )
    assert b"Invalid username or password" in rv.data


def test_plaintext_compare_never_fires_when_users_exist(taxops_db_path, monkeypatch):
    """SEC-2: when auth_users has rows, env-var plaintext fallback does NOT authenticate."""
    from db import get_connection
    import app as mod

    monkeypatch.setattr(mod, "_LOGIN_USER", "env_user")
    monkeypatch.setattr(mod, "_LOGIN_PASS", "env_pass")

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "real_user", "real_pass")  # table now has rows
    conn.close()

    # env_user is NOT in auth_users; even with correct env-var password, must reject
    result = mod._authenticate_user("env_user", "env_pass")
    assert result is None, (
        "Plaintext env-var fallback fired even though auth_users has rows — SEC-2 violation."
    )
