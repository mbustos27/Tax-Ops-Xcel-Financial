"""Compliance Tracker (COMPLIANCE-0) test suite.

Covers:
  - M0 schema migration (tables created, idempotent re-run)
  - M1 RBAC (admin-only CRUD/credentials; login_required view access)
  - M1 security requirements: ssn_last4 never in JSON, credential password
    never serialized, reveal decrypts correctly and is audit-logged
  - M2 roll-forward correctness + idempotency
  - Filing-period status permission (preparer/admin yes, receptionist no)
"""
from __future__ import annotations

import pytest
from werkzeug.security import generate_password_hash

import audit_service
from db import get_connection, init_db


def _create_user(taxops_db_path: str, username: str, role: str) -> None:
    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at) "
        "VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')",
        (username, generate_password_hash("pw12345"), username, role),
    )
    conn.commit()
    conn.close()


def _login(client, username: str):
    return client.post("/login", data={"username": username, "password": "pw12345"}, follow_redirects=True)


def _logout(client):
    client.get("/logout")


# ── M0: schema ───────────────────────────────────────────────────────────────

def test_schema_creates_all_compliance_tables(taxops_db_path):
    conn = get_connection(taxops_db_path)
    tables = {
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'compliance_%'"
        ).fetchall()
    }
    conn.close()
    assert tables == {
        "compliance_clients",
        "compliance_credentials",
        "compliance_accounts",
        "compliance_filing_periods",
        "compliance_correspondence_log",
    }


def test_schema_migration_is_idempotent(taxops_db_path):
    conn = get_connection(taxops_db_path)
    init_db(conn)  # taxops_db_path fixture already ran this once; a second run must not raise
    integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]
    conn.close()
    assert integrity == "ok"


def test_filing_period_unique_index_prevents_duplicate_period_per_account(taxops_db_path):
    import sqlite3

    conn = get_connection(taxops_db_path)
    ts = "2026-01-01T00:00:00Z"
    conn.execute(
        "INSERT INTO compliance_clients (name, client_type, active, created_at, updated_at) VALUES ('X', 'business', 1, ?, ?)",
        (ts, ts),
    )
    client_id = conn.execute("SELECT id FROM compliance_clients WHERE name='X'").fetchone()["id"]
    conn.execute(
        "INSERT INTO compliance_accounts (compliance_client_id, account_type, frequency, active, created_at, updated_at) "
        "VALUES (?, 'cdtfa_sales_tax', 'quarterly', 1, ?, ?)",
        (client_id, ts, ts),
    )
    account_id = conn.execute("SELECT id FROM compliance_accounts WHERE compliance_client_id=?", (client_id,)).fetchone()["id"]
    conn.execute(
        "INSERT INTO compliance_filing_periods (compliance_account_id, period_type, period_label, status, created_at, updated_at) "
        "VALUES (?, 'quarterly', '1ST QTR 2026', 'needs_sales_data', ?, ?)",
        (account_id, ts, ts),
    )
    conn.commit()
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(
            "INSERT INTO compliance_filing_periods (compliance_account_id, period_type, period_label, status, created_at, updated_at) "
            "VALUES (?, 'quarterly', '1ST QTR 2026', 'needs_sales_data', ?, ?)",
            (account_id, ts, ts),
        )
    conn.close()


# ── crypto ───────────────────────────────────────────────────────────────────

def test_crypto_roundtrip_and_never_stores_plaintext():
    from compliance.crypto import decrypt_password, encrypt_password

    ciphertext = encrypt_password("S3cretPortalPass!")
    assert isinstance(ciphertext, bytes)
    assert b"S3cretPortalPass!" not in ciphertext
    assert decrypt_password(ciphertext) == "S3cretPortalPass!"


def test_mask_username():
    from compliance.crypto import mask_username

    assert mask_username("xcelfin92") == "xce****92"
    assert mask_username("") == ""


# ── RBAC: viewing vs. CRUD vs. filing-work permission ───────────────────────

def test_dashboard_and_periods_board_viewable_by_all_roles(client, app, taxops_db_path):
    for role in ("receptionist", "preparer", "admin"):
        username = f"__view_{role}__"
        _create_user(taxops_db_path, username, role)
        _login(client, username)
        assert client.get("/compliance").status_code == 200
        assert client.get("/compliance/periods").status_code == 200
        _logout(client)


def test_credentials_admin_page_is_admin_only(client, app, taxops_db_path):
    _create_user(taxops_db_path, "__cred_recep__", "receptionist")
    _login(client, "__cred_recep__")
    assert client.get("/compliance/credentials").status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__cred_prep__", "preparer")
    _login(client, "__cred_prep__")
    assert client.get("/compliance/credentials").status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__cred_admin__", "admin")
    _login(client, "__cred_admin__")
    assert client.get("/compliance/credentials").status_code == 200
    _logout(client)


def test_client_account_credential_crud_is_admin_only(client, app, taxops_db_path):
    _create_user(taxops_db_path, "__crud_prep__", "preparer")
    _login(client, "__crud_prep__")
    assert client.post("/api/compliance/clients", json={"name": "Blocked Co"}).status_code == 403
    assert client.post("/api/compliance/credentials", json={"login_username": "u", "password": "p"}).status_code == 403
    assert client.post(
        "/api/compliance/accounts", json={"compliance_client_id": 1, "account_type": "misc", "frequency": "none"}
    ).status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__crud_recep__", "receptionist")
    _login(client, "__crud_recep__")
    assert client.post("/api/compliance/clients", json={"name": "Blocked Co"}).status_code == 403
    _logout(client)


@pytest.fixture
def seeded_period(client, app, taxops_db_path):
    """Create one client/account/filing_period as admin, then log out —
    returns (client_id, account_id, period_id, credential_id)."""
    _create_user(taxops_db_path, "__seed_admin__", "admin")
    _login(client, "__seed_admin__")

    cred_resp = client.post(
        "/api/compliance/credentials",
        json={"login_username": "seeduser", "password": "SeedPW1!", "shared_login": False},
    )
    credential_id = cred_resp.get_json()["id"]

    client_resp = client.post(
        "/api/compliance/clients",
        json={"name": "Seeded Co", "client_type": "business", "city": "Fresno", "ssn_last4": "9999"},
    )
    client_id = client_resp.get_json()["id"]

    account_resp = client.post(
        "/api/compliance/accounts",
        json={
            "compliance_client_id": client_id, "account_type": "cdtfa_sales_tax",
            "account_number": "SR-SEED", "frequency": "quarterly", "fee": 50, "credential_id": credential_id,
        },
    )
    account_id = account_resp.get_json()["id"]

    conn = get_connection(taxops_db_path)
    ts = "2026-01-01T00:00:00Z"
    conn.execute(
        "INSERT INTO compliance_filing_periods (compliance_account_id, period_type, period_label, fee, status, created_at, updated_at) "
        "VALUES (?, 'quarterly', '1ST QTR 2026', 50, 'needs_sales_data', ?, ?)",
        (account_id, ts, ts),
    )
    conn.commit()
    period_id = conn.execute(
        "SELECT id FROM compliance_filing_periods WHERE compliance_account_id=?", (account_id,)
    ).fetchone()["id"]
    conn.close()

    _logout(client)
    return {"client_id": client_id, "account_id": account_id, "period_id": period_id, "credential_id": credential_id}


def test_filing_status_update_allowed_for_preparer_not_receptionist(client, app, taxops_db_path, seeded_period):
    period_id = seeded_period["period_id"]

    _create_user(taxops_db_path, "__status_recep__", "receptionist")
    _login(client, "__status_recep__")
    assert client.post(f"/api/compliance/periods/{period_id}/status", json={"status": "filed"}).status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__status_prep__", "preparer")
    _login(client, "__status_prep__")
    resp = client.post(f"/api/compliance/periods/{period_id}/status", json={"status": "filed"})
    assert resp.status_code == 200
    assert resp.get_json()["status"] == "filed"
    _logout(client)


def test_correspondence_note_requires_filing_permission(client, app, taxops_db_path, seeded_period):
    client_id = seeded_period["client_id"]

    _create_user(taxops_db_path, "__note_recep__", "receptionist")
    _login(client, "__note_recep__")
    resp = client.post(
        "/api/compliance/correspondence",
        json={"compliance_client_id": client_id, "month": "2026-02", "note_type": "general", "note": "blocked"},
    )
    assert resp.status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__note_prep__", "preparer")
    _login(client, "__note_prep__")
    resp = client.post(
        "/api/compliance/correspondence",
        json={"compliance_client_id": client_id, "month": "2026-02", "note_type": "general", "note": "allowed"},
    )
    assert resp.status_code == 200
    _logout(client)


# ── Security requirement: ssn_last4 never in JSON, credential fields masked ─

def test_ssn_last4_never_appears_in_client_create_response(client_logged_in):
    resp = client_logged_in.post(
        "/api/compliance/clients",
        json={"name": "SSN Test Client", "client_type": "individual", "ssn_last4": "1234"},
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "1234" not in body
    assert "ssn_last4" not in resp.get_json()


def test_ssn_last4_never_appears_in_client_update_response(client_logged_in, seeded_period):
    client_id = seeded_period["client_id"]
    resp = client_logged_in.post(
        f"/api/compliance/clients/{client_id}",
        json={"name": "Seeded Co", "client_type": "individual", "ssn_last4": "5678"},
    )
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "5678" not in body


def test_ssn_last4_hidden_on_client_detail_page_for_unassigned_receptionist(client, app, taxops_db_path, seeded_period):
    client_id = seeded_period["client_id"]
    # seeded_period's client has ssn_last4='9999' and no assigned preparer.
    _create_user(taxops_db_path, "__ssn_recep__", "receptionist")
    _login(client, "__ssn_recep__")
    resp = client.get(f"/compliance/clients/{client_id}")
    assert resp.status_code == 200
    assert "9999" not in resp.get_data(as_text=True)
    _logout(client)


def test_ssn_last4_visible_on_client_detail_page_for_admin(client_logged_in, seeded_period):
    client_id = seeded_period["client_id"]
    resp = client_logged_in.get(f"/compliance/clients/{client_id}")
    assert resp.status_code == 200
    assert "9999" in resp.get_data(as_text=True)


def test_credential_list_never_serializes_password_or_ciphertext(client_logged_in, seeded_period):
    resp = client_logged_in.get("/compliance/credentials")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "SeedPW1!" not in body


def test_client_detail_never_serializes_password_or_ciphertext(client_logged_in, seeded_period):
    client_id = seeded_period["client_id"]
    resp = client_logged_in.get(f"/compliance/clients/{client_id}")
    assert resp.status_code == 200
    assert "SeedPW1!" not in resp.get_data(as_text=True)


# ── Reveal + audit (security requirement #2) ────────────────────────────────

def test_reveal_credential_decrypts_correctly_and_is_admin_only(client, app, taxops_db_path, seeded_period):
    credential_id = seeded_period["credential_id"]

    _create_user(taxops_db_path, "__reveal_prep__", "preparer")
    _login(client, "__reveal_prep__")
    assert client.post(f"/api/compliance/credentials/{credential_id}/reveal").status_code == 403
    _logout(client)

    _create_user(taxops_db_path, "__reveal_admin__", "admin")
    _login(client, "__reveal_admin__")
    resp = client.post(f"/api/compliance/credentials/{credential_id}/reveal")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["password"] == "SeedPW1!"
    assert data["login_username"] == "seeduser"
    _logout(client)


def test_reveal_credential_writes_audit_log_entry(client_logged_in, seeded_period, monkeypatch):
    credential_id = seeded_period["credential_id"]
    captured = []
    orig = audit_service._enqueue_write

    def _spy(**kwargs):
        captured.append(kwargs)
        orig(**kwargs)

    monkeypatch.setattr(audit_service, "_enqueue_write", _spy)
    monkeypatch.setattr("routes.compliance.audit_service", audit_service, raising=False)

    resp = client_logged_in.post(f"/api/compliance/credentials/{credential_id}/reveal")
    assert resp.status_code == 200

    reveal_events = [e for e in captured if e.get("action") == "COMPLIANCE_CREDENTIAL_REVEALED"]
    assert len(reveal_events) == 1
    assert reveal_events[0]["entity_type"] == "compliance_credential"
    assert reveal_events[0]["entity_id"] == str(credential_id)
    # Never write the password itself into the audit payload.
    assert "SeedPW1!" not in str(reveal_events[0].get("after"))
    assert "SeedPW1!" not in str(reveal_events[0].get("before"))


def test_reveal_credential_with_no_password_returns_400(client_logged_in):
    resp = client_logged_in.post(
        "/api/compliance/credentials", json={"login_username": "nopassworduser", "password": None}
    )
    cred_id = resp.get_json()["id"]
    resp2 = client_logged_in.post(f"/api/compliance/credentials/{cred_id}/reveal")
    assert resp2.status_code == 400


# ── Credential rotation clears needs_rotation ───────────────────────────────

def test_rotating_password_clears_needs_rotation_flag(client_logged_in, taxops_db_path):
    resp = client_logged_in.post(
        "/api/compliance/credentials", json={"login_username": "rotateuser", "password": "OldPW1!"}
    )
    cred_id = resp.get_json()["id"]

    conn = get_connection(taxops_db_path)
    conn.execute("UPDATE compliance_credentials SET needs_rotation=1 WHERE id=?", (cred_id,))
    conn.commit()
    conn.close()

    resp2 = client_logged_in.post(
        f"/api/compliance/credentials/{cred_id}", json={"login_username": "rotateuser", "password": "NewPW2!"}
    )
    assert resp2.status_code == 200

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT needs_rotation, last_rotated_at FROM compliance_credentials WHERE id=?", (cred_id,)).fetchone()
    conn.close()
    assert row["needs_rotation"] == 0
    assert row["last_rotated_at"] is not None

    from compliance.crypto import decrypt_password
    conn = get_connection(taxops_db_path)
    enc = conn.execute("SELECT encrypted_password FROM compliance_credentials WHERE id=?", (cred_id,)).fetchone()["encrypted_password"]
    conn.close()
    assert decrypt_password(enc) == "NewPW2!"


# ── M2: roll-forward correctness + idempotency ──────────────────────────────

def test_roll_forward_creates_period_for_every_account_in_source_period(client_logged_in, seeded_period):
    resp = client_logged_in.post(
        "/api/compliance/periods/roll-forward",
        json={"source_period_label": "1ST QTR 2026", "target_period_label": "2ND QTR 2026"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["created"] == 1
    assert data["skipped_existing"] == 0

    account_id = seeded_period["account_id"]
    resp2 = client_logged_in.get("/compliance/periods?period_label=2ND%20QTR%202026")
    assert resp2.status_code == 200
    body = resp2.get_data(as_text=True)
    assert "2ND QTR 2026" in body
    assert "Seeded Co" in body


def test_roll_forward_is_idempotent_on_second_call(client_logged_in, seeded_period):
    payload = {"source_period_label": "1ST QTR 2026", "target_period_label": "2ND QTR 2026"}
    first = client_logged_in.post("/api/compliance/periods/roll-forward", json=payload).get_json()
    second = client_logged_in.post("/api/compliance/periods/roll-forward", json=payload).get_json()
    assert first["created"] == 1
    assert second["created"] == 0
    assert second["skipped_existing"] == 1


def test_roll_forward_requires_distinct_labels(client_logged_in, seeded_period):
    resp = client_logged_in.post(
        "/api/compliance/periods/roll-forward",
        json={"source_period_label": "1ST QTR 2026", "target_period_label": "1ST QTR 2026"},
    )
    assert resp.status_code == 400


def test_roll_forward_404_for_unknown_source_label(client_logged_in, seeded_period):
    resp = client_logged_in.post(
        "/api/compliance/periods/roll-forward",
        json={"source_period_label": "NOPE 2099", "target_period_label": "SOMETHING"},
    )
    assert resp.status_code == 404


# ── Data Download export (M5) never includes SSN/credentials ───────────────

def test_data_download_export_never_includes_ssn_or_credentials(client_logged_in, seeded_period):
    resp = client_logged_in.get("/api/compliance/export/data-download.csv?year=2026")
    assert resp.status_code == 200
    body = resp.get_data(as_text=True)
    assert "9999" not in body
    assert "SeedPW1!" not in body
    assert "Seeded Co" in body
