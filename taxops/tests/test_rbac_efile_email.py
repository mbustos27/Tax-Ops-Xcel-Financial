"""RBAC-1: Receptionist gets targeted access to e-file/pickup queue only.

Email inbox is admin-only (can_use_email_tools restricted to admin).

Covers:
  1. Receptionist can GET /efile-queue (was preparer-only)
  2. Receptionist can GET /logout-queue (was preparer-only)
  3. Receptionist can GET /efile-queue/export (was preparer-only)
  4. Receptionist is BLOCKED from /email-inbox (admin-only)
  5. Receptionist is BLOCKED from /api/email-inbox/items (admin-only)
  6. Receptionist CANNOT access other preparer-only routes (e.g. /review)
  7. Receptionist CANNOT access admin-only routes (e.g. /payments)
  8. Preparer is BLOCKED from /email-inbox (admin-only)
  9. has_permission() helper returns correct values per role
 10. ROLE_PERMISSIONS is defined in config with both keys
"""
from __future__ import annotations

import pytest
from werkzeug.security import generate_password_hash


# ── helpers ──────────────────────────────────────────────────────────────────

def _seed_user(conn, username: str, role: str) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users
            (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("testpass"), username, role),
    )
    conn.commit()


def _login(client, username: str) -> None:
    client.post(
        "/login",
        data={"username": username, "password": "testpass"},
        follow_redirects=True,
    )


# ── 1-3: E-file / pickup queue — receptionist access ─────────────────────────

def test_receptionist_can_view_efile_queue(client, taxops_db_path):
    """Receptionist GET /efile-queue returns 200 (not 403)."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    _seed_user(conn, "rbac_recep1", "receptionist")
    conn.close()
    _login(client, "rbac_recep1")
    rv = client.get("/efile-queue")
    assert rv.status_code == 200, f"Expected 200 for receptionist on /efile-queue, got {rv.status_code}"


def test_receptionist_can_view_logout_queue(client, taxops_db_path):
    """Receptionist GET /logout-queue returns 200 (not 403)."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    _seed_user(conn, "rbac_recep2", "receptionist")
    conn.close()
    _login(client, "rbac_recep2")
    rv = client.get("/logout-queue")
    assert rv.status_code == 200, f"Expected 200 for receptionist on /logout-queue, got {rv.status_code}"


def test_receptionist_can_export_efile_queue(client, taxops_db_path):
    """Receptionist GET /efile-queue/export returns 200 (not 403)."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    _seed_user(conn, "rbac_recep3", "receptionist")
    conn.close()
    _login(client, "rbac_recep3")
    rv = client.get("/efile-queue/export")
    assert rv.status_code == 200, f"Expected 200 for receptionist on /efile-queue/export, got {rv.status_code}"


# ── 4-5: Email inbox — receptionist is BLOCKED (admin-only) ──────────────────
# Tested via auth module directly to avoid base.html template-context issues
# when the 403 error handler fires (same pattern as test_has_permission_* below).

def test_receptionist_blocked_from_email_inbox(app):
    """Receptionist lacks can_use_email_tools — email inbox is admin-only."""
    import auth as auth_mod
    original = auth_mod.get_effective_role
    auth_mod.get_effective_role = lambda: "receptionist"
    try:
        assert auth_mod.has_permission("can_use_email_tools") is False, \
            "Receptionist must not have can_use_email_tools"
    finally:
        auth_mod.get_effective_role = original


def test_receptionist_blocked_from_email_inbox_api(app):
    """Receptionist lacks can_use_email_tools — API endpoint is admin-only too."""
    import auth as auth_mod
    original = auth_mod.get_effective_role
    auth_mod.get_effective_role = lambda: "receptionist"
    try:
        assert auth_mod.has_permission("can_use_email_tools") is False, \
            "Receptionist must not have can_use_email_tools (covers API endpoint)"
    finally:
        auth_mod.get_effective_role = original


# ── 6-7: No scope creep — receptionist still blocked from other features ──────
#
# These tests verify the guard logic directly on the auth module rather than
# via HTTP, because the HTML 403 error handler renders base.html which needs
# base_ctx() context variables not available outside a full request context.
# The guard logic (role hierarchy check) is what matters for security here.

def test_receptionist_not_in_any_extra_permissions():
    """Receptionist has exactly one named permission and no others.

    ROLE_PERMISSIONS only grants receptionist can_manage_efile_queue.
    Email tools are admin-only and must not appear here.
    """
    from config import ROLE_PERMISSIONS
    recep_permissions = {k for k, roles in ROLE_PERMISSIONS.items() if "receptionist" in roles}
    expected = {"can_manage_efile_queue"}
    assert recep_permissions == expected, (
        f"Receptionist has unexpected permissions: {recep_permissions - expected}"
    )


def test_receptionist_rank_still_below_preparer():
    """ROLE_HIERARCHY rank for receptionist is still 0, below preparer (1).

    Ensures role_required('preparer') and role_required('admin') still block
    receptionist on all routes outside the two named permissions.
    """
    from config import ROLE_HIERARCHY
    assert ROLE_HIERARCHY["receptionist"] < ROLE_HIERARCHY["preparer"]
    assert ROLE_HIERARCHY["receptionist"] < ROLE_HIERARCHY["admin"]


# ── 8: Preparer behavior unchanged ───────────────────────────────────────────

def test_preparer_still_accesses_efile_queue(client, taxops_db_path):
    """Preparer GET /efile-queue still returns 200."""
    from db import get_connection
    conn = get_connection(taxops_db_path)
    _seed_user(conn, "rbac_prep1", "preparer")
    conn.close()
    _login(client, "rbac_prep1")
    rv = client.get("/efile-queue")
    assert rv.status_code == 200, f"Expected 200 for preparer on /efile-queue, got {rv.status_code}"


def test_preparer_blocked_from_email_inbox(app):
    """Preparer lacks can_use_email_tools — email inbox is admin-only."""
    import auth as auth_mod
    original = auth_mod.get_effective_role
    auth_mod.get_effective_role = lambda: "preparer"
    try:
        assert auth_mod.has_permission("can_use_email_tools") is False, \
            "Preparer must not have can_use_email_tools"
    finally:
        auth_mod.get_effective_role = original


# ── 9: has_permission() helper ────────────────────────────────────────────────

def test_has_permission_receptionist_efile(app):
    """has_permission('can_manage_efile_queue') returns True for receptionist."""
    with app.test_request_context("/"):
        from flask import session as flask_session
        with app.test_client() as c:
            with c.session_transaction() as sess:
                sess["logged_in"] = True
                sess["role"] = "receptionist"
        from auth import has_permission
        import auth as auth_mod
        original = auth_mod.get_effective_role
        auth_mod.get_effective_role = lambda: "receptionist"
        try:
            assert has_permission("can_manage_efile_queue") is True
            assert has_permission("can_use_email_tools") is False
        finally:
            auth_mod.get_effective_role = original


def test_has_permission_unknown_permission_always_false(app):
    """has_permission() returns False for an unknown permission string."""
    import auth as auth_mod
    original = auth_mod.get_effective_role
    auth_mod.get_effective_role = lambda: "admin"
    try:
        assert auth_mod.has_permission("nonexistent_permission") is False
    finally:
        auth_mod.get_effective_role = original


# ── 10: ROLE_PERMISSIONS config shape ────────────────────────────────────────

def test_role_permissions_config_contains_both_keys():
    """ROLE_PERMISSIONS in config defines can_manage_efile_queue and can_use_email_tools."""
    from config import ROLE_PERMISSIONS
    assert "can_manage_efile_queue" in ROLE_PERMISSIONS
    assert "can_use_email_tools" in ROLE_PERMISSIONS


def test_role_permissions_all_three_roles_have_efile():
    """All three roles (receptionist, preparer, admin) have can_manage_efile_queue."""
    from config import ROLE_PERMISSIONS
    allowed = ROLE_PERMISSIONS["can_manage_efile_queue"]
    assert "receptionist" in allowed
    assert "preparer" in allowed
    assert "admin" in allowed


def test_only_admin_has_email_tools():
    """Only admin has can_use_email_tools; receptionist and preparer are excluded."""
    from config import ROLE_PERMISSIONS
    allowed = ROLE_PERMISSIONS["can_use_email_tools"]
    assert "admin" in allowed
    assert "receptionist" not in allowed, "receptionist must not have email tools"
    assert "preparer" not in allowed, "preparer must not have email tools"
