"""Hardening: auth, CSRF/LAN cookies, roles, status aliases, review payload."""
from __future__ import annotations

import os
import sqlite3

import pytest

from auth import (
    authenticate_user,
    required_role_for_path,
    safe_next_url,
)
from config import default_tax_year, parse_taxops_users
from normalizer import canonical_status, is_locked_status
from review_payload import extract_review_identity, to_import_row


def test_no_hardcoded_office_password():
    app_path = os.path.join(os.path.dirname(__file__), "..", "app.py")
    src = open(app_path, encoding="utf-8").read()
    assert "2703Tax" not in src


def test_parse_users_and_authenticate(monkeypatch):
    monkeypatch.delenv("TAXOPS_USERS", raising=False)
    monkeypatch.delenv("TAXOPS_PASS", raising=False)
    assert parse_taxops_users() == {}
    assert authenticate_user("info", "2703Tax") is None

    monkeypatch.setenv("TAXOPS_USER", "info")
    monkeypatch.setenv("TAXOPS_PASS", "office-secret")
    rec = authenticate_user("info", "office-secret")
    assert rec == {"username": "info", "role": "admin"}
    assert authenticate_user("info", "wrong") is None


def test_taxops_users_roles(monkeypatch):
    monkeypatch.setenv("TAXOPS_USERS", "front:desk:staff;maria:p:preparer;boss:x:admin")
    monkeypatch.delenv("TAXOPS_PASS", raising=False)
    assert authenticate_user("front", "desk")["role"] == "staff"
    assert authenticate_user("maria", "p")["role"] == "preparer"
    assert authenticate_user("boss", "x")["role"] == "admin"


def test_cookie_flags_are_lan_safe(app):
    assert app.config["SESSION_COOKIE_HTTPONLY"] is True
    assert app.config["SESSION_COOKIE_SAMESITE"] == "Lax"
    assert app.config["SESSION_COOKIE_SECURE"] is False
    with app.app_context():
        assert safe_next_url("https://evil.example/phish") == "/"
        assert safe_next_url("//evil.example") == "/"
        assert safe_next_url("/intake") == "/intake"
        assert safe_next_url("/efile-queue?year=2026") == "/efile-queue?year=2026"


def test_required_role_paths():
    assert required_role_for_path("/upload") == "admin"
    assert required_role_for_path("/upload/confirm") == "admin"
    assert required_role_for_path("/export") == "admin"
    assert required_role_for_path("/merge-clients") == "admin"
    assert required_role_for_path("/efile-queue") == "preparer"
    assert required_role_for_path("/efile-queue/export") == "preparer"
    assert required_role_for_path("/ai/chat") == "preparer"
    assert required_role_for_path("/email-review") == "preparer"
    assert required_role_for_path("/") is None
    assert required_role_for_path("/intake") is None
    assert required_role_for_path("/payments") is None


def test_canonical_status_aliases():
    assert canonical_status("EFILE") == "EFILE READY"
    assert canonical_status("PICK UP") == "PICKUP"
    assert canonical_status("OLD PICKUP") == "PICKUP"
    assert canonical_status("PRIOR HOLD") == "HOLD"
    assert canonical_status("PRIOR PROC") == "PROCESSING"
    assert canonical_status("COMPLETE") == "LOG OUT"
    assert is_locked_status("CANCELLED")
    assert is_locked_status("canceled")


def test_legacy_status_rewrite_on_init():
    from db import init_db

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) VALUES ('A','B',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """INSERT INTO returns (client_id, tax_year, log_number, client_status, created_at, updated_at)
           VALUES (?,?,?,?,datetime('now'),datetime('now'))""",
        (cid, 2025, "1", "EFILE"),
    )
    init_db(conn)
    st = conn.execute("SELECT client_status FROM returns WHERE log_number='1'").fetchone()[0]
    assert st == "EFILE READY"
    conn.close()


def test_review_payload_drake_csm():
    raw = {
        "Taxpayer Last Name": "SANCHEZ",
        "Taxpayer First Name": "JORGE",
        "Date Started": "02/25/2026",
    }
    ident = extract_review_identity(raw)
    assert ident["csv_last"] == "SANCHEZ"
    assert ident["csv_first"] == "JORGE"
    row = to_import_row(raw, 2025)
    assert row["clients.last_name"] == "SANCHEZ"
    assert row["returns.tax_year"] == "2025"


def test_default_tax_year_is_season_minus_one():
    assert default_tax_year(2026) == 2025


def _login(client, monkeypatch, users: str, username: str, password: str):
    monkeypatch.setenv("TAXOPS_USERS", users)
    monkeypatch.delenv("TAXOPS_PASS", raising=False)
    monkeypatch.delenv("TAXOPS_USER", raising=False)
    r = client.post("/login", data={"username": username, "password": password})
    assert r.status_code in (302, 200)


def test_staff_forbidden_from_upload(client, monkeypatch):
    _login(client, monkeypatch, "front:desk:staff;boss:x:admin", "front", "desk")
    r = client.get("/upload")
    assert r.status_code == 403
    r = client.get("/")
    assert r.status_code == 200
    body = r.get_data(as_text=True)
    assert "Import CSV" not in body
    assert "E-File Queue" not in body


def test_preparer_can_open_efile_not_upload(client, monkeypatch):
    _login(client, monkeypatch, "maria:p:preparer;boss:x:admin", "maria", "p")
    assert client.get("/efile-queue").status_code == 200
    assert client.get("/upload").status_code == 403


def test_admin_can_open_upload(client, monkeypatch):
    _login(client, monkeypatch, "boss:x:admin", "boss", "x")
    assert client.get("/upload").status_code == 200


def test_cancelled_status_locked(client_logged_in, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) VALUES ('LOCK','TEST',datetime('now'),datetime('now'))"
    )
    cid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, client_status, created_at, updated_at) VALUES (?,?,?,?,datetime('now'),datetime('now'))",
        (cid, 2025, "9001", "CANCELLED"),
    )
    rid = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.commit()
    conn.close()
    r = client_logged_in.post(
        f"/api/return/{rid}/status",
        json={"status": "PROCESSING"},
    )
    assert r.status_code == 409


def test_upload_confirm_ignores_client_tmp_path(client_logged_in):
    r = client_logged_in.post(
        "/upload/confirm",
        json={"tmp_path": "/etc/passwd", "overrides": {}, "tax_year": 2025},
    )
    assert r.status_code == 400
    assert "expired" in (r.get_json() or {}).get("error", "").lower()


def test_payments_uses_season_filter():
    import inspect
    import app as app_mod

    src = inspect.getsource(app_mod.payments)
    assert "strftime('%Y', r.intake_date)" in src
    assert "WHERE r.tax_year=?" not in src
