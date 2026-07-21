"""WO-6: Admin CRUD for Work Order quick picks (the standardized service
list), including compound quick picks that carry extra fee components
(e.g. "Statement of Information" $125 + a "Filing Fee" $25 component).

Covers:
  - seeded "Statement of Information" example ships with its Filing Fee component
  - admin create/update/deactivate/reactivate a quick pick with components
  - RBAC: non-admin cannot manage quick picks
  - the Work Order create/edit form's embedded JSON includes components, so
    the browser-side auto-add-extra-lines behavior has data to work with
  - deactivated quick picks disappear from the Work Order form's picker
"""
from __future__ import annotations

from werkzeug.security import generate_password_hash

from db import get_connection


def _seed_user(taxops_db_path: str, username: str, role: str) -> None:
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pw12345"), username.title(), role),
    )
    conn.commit()
    conn.close()


def _login_as(client, username: str) -> None:
    client.post("/login", data={"username": username, "password": "pw12345"}, follow_redirects=True)


# ── Seed data ─────────────────────────────────────────────────────────────────

def test_statement_of_information_seeded_with_filing_fee_component(taxops_db_path):
    conn = get_connection(taxops_db_path)
    try:
        pick = conn.execute(
            "SELECT id, default_fee, active FROM work_order_quick_picks WHERE label='Statement of Information'"
        ).fetchone()
        assert pick is not None
        assert float(pick["default_fee"]) == 125.00
        assert pick["active"] == 1

        components = conn.execute(
            "SELECT label, fee FROM work_order_quick_pick_components WHERE quick_pick_id=?",
            (pick["id"],),
        ).fetchall()
        assert len(components) == 1
        assert components[0]["label"] == "Filing Fee"
        assert float(components[0]["fee"]) == 25.00
    finally:
        conn.close()


# ── Admin page + RBAC ─────────────────────────────────────────────────────────

def test_admin_can_view_quick_picks_page(client_logged_in):
    resp = client_logged_in.get("/admin/work-order-quick-picks")
    assert resp.status_code == 200
    assert b"Statement of Information" in resp.data


def test_non_admin_cannot_view_quick_picks_page(app, taxops_db_path):
    _seed_user(taxops_db_path, "__qp_recept__", "receptionist")
    client = app.test_client()
    _login_as(client, "__qp_recept__")
    resp = client.get("/admin/work-order-quick-picks")
    assert resp.status_code == 403


def test_non_admin_cannot_create_quick_pick(app, taxops_db_path):
    _seed_user(taxops_db_path, "__qp_prep__", "preparer")
    client = app.test_client()
    _login_as(client, "__qp_prep__")
    resp = client.post(
        "/api/admin/work-order-quick-picks",
        json={"label": "Sneaky", "default_fee": 1},
    )
    assert resp.status_code == 403


# ── Create / update with components ──────────────────────────────────────────

def test_admin_creates_quick_pick_with_multiple_components(client_logged_in, taxops_db_path):
    resp = client_logged_in.post(
        "/api/admin/work-order-quick-picks",
        json={
            "label": "LLC Formation",
            "default_fee": 300.00,
            "components": [
                {"label": "State Filing Fee", "fee": 70.00},
                {"label": "Registered Agent (1yr)", "fee": 49.00},
            ],
        },
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    pick_id = data["id"]

    conn = get_connection(taxops_db_path)
    try:
        pick = conn.execute("SELECT label, default_fee, active FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()
        assert pick["label"] == "LLC Formation"
        assert float(pick["default_fee"]) == 300.00
        assert pick["active"] == 1

        comps = conn.execute(
            "SELECT label, fee FROM work_order_quick_pick_components WHERE quick_pick_id=? ORDER BY sort_order",
            (pick_id,),
        ).fetchall()
        assert [dict(c) for c in comps] == [
            {"label": "State Filing Fee", "fee": 70.00},
            {"label": "Registered Agent (1yr)", "fee": 49.00},
        ]
    finally:
        conn.close()


def test_create_quick_pick_requires_label(client_logged_in):
    resp = client_logged_in.post("/api/admin/work-order-quick-picks", json={"label": "", "default_fee": 5})
    assert resp.status_code == 400


def test_create_quick_pick_rejects_negative_fee(client_logged_in):
    resp = client_logged_in.post("/api/admin/work-order-quick-picks", json={"label": "Bad Fee", "default_fee": -5})
    assert resp.status_code == 400


def test_update_quick_pick_replaces_components(client_logged_in, taxops_db_path):
    create_resp = client_logged_in.post(
        "/api/admin/work-order-quick-picks",
        json={"label": "Amendment", "default_fee": 100.00, "components": [{"label": "Old Comp", "fee": 10.00}]},
    )
    pick_id = create_resp.get_json()["id"]

    update_resp = client_logged_in.post(
        f"/api/admin/work-order-quick-picks/{pick_id}",
        json={
            "label": "Amendment (Updated)",
            "default_fee": 150.00,
            "components": [{"label": "New Comp A", "fee": 20.00}, {"label": "New Comp B", "fee": 5.00}],
        },
    )
    assert update_resp.status_code == 200

    conn = get_connection(taxops_db_path)
    try:
        pick = conn.execute("SELECT label, default_fee FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()
        assert pick["label"] == "Amendment (Updated)"
        assert float(pick["default_fee"]) == 150.00

        comps = conn.execute(
            "SELECT label, fee FROM work_order_quick_pick_components WHERE quick_pick_id=? ORDER BY sort_order",
            (pick_id,),
        ).fetchall()
        assert [dict(c) for c in comps] == [
            {"label": "New Comp A", "fee": 20.00},
            {"label": "New Comp B", "fee": 5.00},
        ]
        # old component gone, not just appended
        assert not any(c["label"] == "Old Comp" for c in comps)
    finally:
        conn.close()


def test_update_nonexistent_quick_pick_404s(client_logged_in):
    resp = client_logged_in.post("/api/admin/work-order-quick-picks/999999", json={"label": "x", "default_fee": 1})
    assert resp.status_code == 404


# ── Deactivate / reactivate ───────────────────────────────────────────────────

def test_deactivate_and_reactivate_quick_pick(client_logged_in, taxops_db_path):
    create_resp = client_logged_in.post(
        "/api/admin/work-order-quick-picks", json={"label": "Seasonal Special", "default_fee": 50.00}
    )
    pick_id = create_resp.get_json()["id"]

    deactivate_resp = client_logged_in.post(f"/api/admin/work-order-quick-picks/{pick_id}/deactivate")
    assert deactivate_resp.status_code == 200
    conn = get_connection(taxops_db_path)
    try:
        assert conn.execute("SELECT active FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()["active"] == 0
    finally:
        conn.close()

    reactivate_resp = client_logged_in.post(f"/api/admin/work-order-quick-picks/{pick_id}/reactivate")
    assert reactivate_resp.status_code == 200
    conn = get_connection(taxops_db_path)
    try:
        assert conn.execute("SELECT active FROM work_order_quick_picks WHERE id=?", (pick_id,)).fetchone()["active"] == 1
    finally:
        conn.close()


def test_deactivated_quick_pick_disappears_from_work_order_form(client_logged_in):
    create_resp = client_logged_in.post(
        "/api/admin/work-order-quick-picks", json={"label": "Temp Offer", "default_fee": 20.00}
    )
    pick_id = create_resp.get_json()["id"]

    before = client_logged_in.get("/work-orders/new")
    assert b"Temp Offer" in before.data

    client_logged_in.post(f"/api/admin/work-order-quick-picks/{pick_id}/deactivate")

    after = client_logged_in.get("/work-orders/new")
    assert b"Temp Offer" not in after.data


# ── Work Order form embeds components for the browser-side auto-add ─────────

def test_work_order_new_form_embeds_quick_pick_components_json(client_logged_in):
    resp = client_logged_in.get("/work-orders/new")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert '"label": "Filing Fee"' in html or '"label":"Filing Fee"' in html
    assert '"label": "Statement of Information"' in html or '"label":"Statement of Information"' in html
