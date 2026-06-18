"""SEC-7: login rate limiting and account lockout tests."""
from __future__ import annotations

import time

import pytest
from werkzeug.security import generate_password_hash


def _insert_user(conn, username, password, *, failed_attempts=0, locked_until=None):
    conn.execute(
        """INSERT INTO auth_users
           (username, password_hash, display_name, role, is_active, created_at,
            failed_attempts, locked_until)
           VALUES (?, ?, ?, 'staff', 1, '2025-01-01T00:00:00Z', ?, ?)""",
        (username, generate_password_hash(password), username,
         failed_attempts, locked_until),
    )
    conn.commit()


# ── lockout threshold config ──────────────────────────────────────────────────

def test_login_max_attempts_default(app):
    """SEC-7: _LOGIN_MAX_ATTEMPTS defaults to 5 when env var is unset."""
    import app as mod
    assert mod._LOGIN_MAX_ATTEMPTS == 5


def test_login_lockout_minutes_default(app):
    """SEC-7: _LOGIN_LOCKOUT_MINUTES defaults to 15 when env var is unset."""
    import app as mod
    assert mod._LOGIN_LOCKOUT_MINUTES == 15


# ── failed_attempts increments ────────────────────────────────────────────────

def test_failed_attempts_increments_on_bad_password(taxops_db_path):
    """SEC-7: failed_attempts counter increments on each wrong password."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "user_sec7a", "correct")
    conn.close()

    mod._authenticate_user("user_sec7a", "wrong1")
    mod._authenticate_user("user_sec7a", "wrong2")

    conn2 = get_connection(taxops_db_path)
    row = conn2.execute(
        "SELECT failed_attempts FROM auth_users WHERE username='user_sec7a'"
    ).fetchone()
    conn2.close()
    assert row["failed_attempts"] == 2


def test_failed_attempts_resets_on_success(taxops_db_path):
    """SEC-7: failed_attempts resets to 0 on a successful login."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "user_sec7b", "correct", failed_attempts=3)
    conn.close()

    result = mod._authenticate_user("user_sec7b", "correct")
    assert result is not None and result is not mod._AUTH_LOCKED

    conn2 = get_connection(taxops_db_path)
    row = conn2.execute(
        "SELECT failed_attempts FROM auth_users WHERE username='user_sec7b'"
    ).fetchone()
    conn2.close()
    assert row["failed_attempts"] == 0


# ── lockout enforcement ────────────────────────────────────────────────────────

def test_account_locks_after_max_attempts(taxops_db_path, monkeypatch):
    """SEC-7: after _LOGIN_MAX_ATTEMPTS consecutive failures, _AUTH_LOCKED is returned."""
    from db import get_connection
    import app as mod

    monkeypatch.setattr(mod, "_LOGIN_MAX_ATTEMPTS", 3)
    monkeypatch.setattr(mod, "_LOGIN_LOCKOUT_MINUTES", 15)

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "user_sec7c", "correct")
    conn.close()

    # Two bad attempts — should not yet be locked
    assert mod._authenticate_user("user_sec7c", "bad") is None
    assert mod._authenticate_user("user_sec7c", "bad") is None
    # Third failure — triggers lockout
    result = mod._authenticate_user("user_sec7c", "bad")
    # After exactly max_attempts failures the row is now locked; next call returns _AUTH_LOCKED
    next_result = mod._authenticate_user("user_sec7c", "bad")
    assert next_result is mod._AUTH_LOCKED, (
        "Expected _AUTH_LOCKED after exceeding max attempts"
    )


def test_locked_account_rejects_correct_password(taxops_db_path):
    """SEC-7: a locked account returns _AUTH_LOCKED even with the correct password."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    # Set locked_until to 1 hour in the future
    import datetime as dt
    locked_until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _insert_user(conn, "user_sec7d", "correct", locked_until=locked_until)
    conn.close()

    result = mod._authenticate_user("user_sec7d", "correct")
    assert result is mod._AUTH_LOCKED, (
        "Locked account must be rejected even with correct password"
    )


def test_expired_lockout_allows_login(taxops_db_path):
    """SEC-7: a lockout whose locked_until timestamp has passed allows login again."""
    from db import get_connection
    import app as mod

    conn = get_connection(taxops_db_path)
    # Set locked_until in the past
    import datetime as dt
    locked_until = (dt.datetime.now(dt.timezone.utc) - dt.timedelta(minutes=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _insert_user(conn, "user_sec7e", "correct", locked_until=locked_until, failed_attempts=5)
    conn.close()

    result = mod._authenticate_user("user_sec7e", "correct")
    assert result is not None and result is not mod._AUTH_LOCKED, (
        "Expired lockout should allow a successful login"
    )


# ── login route UX ────────────────────────────────────────────────────────────

def test_login_route_returns_same_error_for_locked_and_bad_credentials(client, taxops_db_path):
    """SEC-7: locked account and wrong-password both return HTTP 200 with the same error string."""
    from db import get_connection
    import datetime as dt

    conn = get_connection(taxops_db_path)
    locked_until = (dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")
    _insert_user(conn, "locked_user", "pw", locked_until=locked_until)
    _insert_user(conn, "active_user", "correct_pw")
    conn.close()

    rv_locked = client.post(
        "/login",
        data={"username": "locked_user", "password": "pw"},
        follow_redirects=False,
    )
    rv_bad = client.post(
        "/login",
        data={"username": "active_user", "password": "wrong"},
        follow_redirects=False,
    )

    assert rv_locked.status_code == 200
    assert rv_bad.status_code == 200
    assert b"Invalid username or password" in rv_locked.data
    assert b"Invalid username or password" in rv_bad.data


def test_sixth_rapid_failure_triggers_lockout(taxops_db_path, monkeypatch):
    """SEC-7: sixth consecutive failure triggers lockout (max_attempts=5)."""
    from db import get_connection
    import app as mod

    monkeypatch.setattr(mod, "_LOGIN_MAX_ATTEMPTS", 5)
    monkeypatch.setattr(mod, "_LOGIN_LOCKOUT_MINUTES", 15)

    conn = get_connection(taxops_db_path)
    _insert_user(conn, "user_sec7f", "correct")
    conn.close()

    # Five bad attempts
    for _ in range(5):
        mod._authenticate_user("user_sec7f", "bad")

    # Sixth attempt — account is now locked
    result = mod._authenticate_user("user_sec7f", "bad")
    assert result is mod._AUTH_LOCKED, (
        "Account must be locked after the 5th failed attempt (6th call should return _AUTH_LOCKED)"
    )
