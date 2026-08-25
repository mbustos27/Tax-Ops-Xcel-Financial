"""Compliance Tracker — CDTFA sales/use tax filings, city business license
renewals, SBE/CDTFA monthly prepayment deposits, and misc individual
filings. Replaces the hand-maintained ACCOUNTING_LOG_2026.xlsx workbook.

Data model (see db.py's COMPLIANCE-0 migration block):
  compliance_clients            — business/individual roster (NOT the same
                                   table as `clients`, which is tax-return
                                   intake — see db.py comment for why).
  compliance_credentials        — CDTFA/city portal logins. encrypted_password
                                   is Fernet ciphertext (compliance/crypto.py),
                                   NEVER plaintext.
  compliance_accounts           — one persistent row per (client, filing
                                   obligation), e.g. "Acme Corp CDTFA sales
                                   tax account".
  compliance_filing_periods     — the Kanban card: one row per
                                   quarter/month/year per account. `status`
                                   replaces the old workbook's ad hoc column
                                   flags with a single enum + timestamps.
  compliance_correspondence_log — notes, replacing the NOTES sheet grid.

RBAC (security requirements, non-negotiable):
  - Client/account/credential CRUD is role_required("admin") ONLY — never
    loosened via ROLE_PERMISSIONS (see config.py's comment on
    can_manage_compliance_filings, which is deliberately NOT used for this).
  - Filing-period status updates / roll-forward / correspondence notes use
    permission_required("can_manage_compliance_filings") — preparer + admin
    (the actual filing work), not receptionist.
  - Viewing (dashboard, client detail, filing board) is login_required only
    — all three roles can see operational status; sensitive fields are
    gated per-field, not per-page:
      * ssn_last4: never in ANY JSON response (matches the app-wide
        invariant for the existing `clients` table — see
        .cursor/rules/taxops-invariants.mdc "Privacy" section); shown in
        the server-rendered client detail template ONLY to admin or that
        client's assigned_preparer_user_id.
      * Credential passwords: NEVER decrypted for list/detail views, ever
        — only api_reveal_credential() decrypts, admin-only, and every
        call is audit-logged (who/when/which credential) via
        audit_service._enqueue_write, matching routes/work_orders.py's
        _enqueue_audit pattern.

Routes:
  GET  /compliance                                   — roster + due-soon-licenses widget (M3)
  GET  /compliance/clients/<id>                      — client detail (accounts, masked credentials, correspondence)
  GET  /compliance/credentials                       — admin-only global credential list
  GET  /compliance/periods                           — filing period board (M2), filterable
  POST /api/compliance/clients                       — create (admin)
  POST /api/compliance/clients/<id>                  — update (admin)
  POST /api/compliance/clients/<id>/deactivate        — (admin)
  POST /api/compliance/clients/<id>/reactivate        — (admin)
  POST /api/compliance/accounts                       — create (admin)
  POST /api/compliance/accounts/<id>                   — update (admin)
  POST /api/compliance/accounts/<id>/deactivate         — (admin)
  POST /api/compliance/credentials                       — create (admin)
  POST /api/compliance/credentials/<id>                   — update (admin) — rotate password/username/notes
  POST /api/compliance/credentials/<id>/reveal             — decrypt + audit (admin) — plaintext returned ONCE
  POST /api/compliance/periods/<id>/status                  — update status (preparer/admin)
  POST /api/compliance/periods/roll-forward                   — bulk-create next period's rows (preparer/admin)
  POST /api/compliance/correspondence                           — create a note (preparer/admin)
  GET  /api/compliance/export/data-download.csv                  — CSV export mirroring the old DATA DOWNLOAD sheet (M5)
"""
from __future__ import annotations

import csv
import io
import logging

from flask import Blueprint, Response, jsonify, render_template, request

from auth import get_effective_role, login_required, permission_required, role_required
from db import get_connection
from utils import now as _now

logger = logging.getLogger("compliance")

compliance_bp = Blueprint("compliance", __name__)

ACCOUNT_TYPES = ("cdtfa_sales_tax", "city_license", "sbe_deposit", "misc")
FREQUENCIES = ("monthly", "quarterly", "annual", "fiscal", "q_mo_dep", "none")
PERIOD_STATUSES = ("needs_sales_data", "sales_in", "filed", "tp_filed", "done", "ready")
NOTE_TYPES = ("password_correspondence", "missing_password", "general")


# ── Audit helper (matches routes/work_orders.py's _enqueue_audit) ──────────

def _enqueue_audit(*, user_id, action: str, entity_type: str, entity_id, before=None, after=None) -> None:
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id=user_id, action=action, entity_type=entity_type, entity_id=str(entity_id),
            before=before, after=after, ip_address=request.remote_addr, http_status=200,
        )
    except Exception:
        logger.exception("compliance: failed to enqueue audit action=%s entity=%s/%s", action, entity_type, entity_id)


def _current_user_id() -> int | None:
    from flask import session
    return session.get("user_id")


# ── Masking helpers ──────────────────────────────────────────────────────────

def _client_can_see_ssn(client_row, role: str, user_id) -> bool:
    if role == "admin":
        return True
    return user_id is not None and client_row["assigned_preparer_user_id"] == user_id


def _client_json_safe(row) -> dict:
    """JSON-response dict for a compliance_clients row — NEVER includes
    ssn_last4, matching the app-wide invariant that this field is never
    returned in any API response (see module docstring)."""
    d = dict(row)
    d.pop("ssn_last4", None)
    return d


def _credential_masked_dict(row) -> dict:
    from compliance.crypto import MASKED_PASSWORD_DISPLAY, mask_username

    d = dict(row)
    d.pop("encrypted_password", None)  # never serialize ciphertext either
    d["login_username_masked"] = mask_username(d.get("login_username") or "")
    d["password_display"] = MASKED_PASSWORD_DISPLAY if d.get("has_password") else "—"
    return d


# ── Dashboard / roster (M0 roster + M3 due-soon widget) ─────────────────────

@compliance_bp.get("/compliance")
@login_required
def compliance_dashboard():
    from app import base_ctx

    q = (request.args.get("q") or "").strip()
    show_inactive = request.args.get("show_inactive") == "1"

    conn = get_connection()
    try:
        sql = "SELECT * FROM compliance_clients WHERE 1=1"
        params: list = []
        if not show_inactive:
            sql += " AND active=1"
        if q:
            sql += " AND name LIKE ?"
            params.append(f"%{q}%")
        sql += " ORDER BY name"
        clients = [dict(r) for r in conn.execute(sql, params).fetchall()]

        # Account/period counts per client, for the roster table.
        account_counts = {
            r["compliance_client_id"]: r["c"]
            for r in conn.execute(
                "SELECT compliance_client_id, COUNT(*) c FROM compliance_accounts WHERE active=1 GROUP BY compliance_client_id"
            ).fetchall()
        }
        for c in clients:
            c["account_count"] = account_counts.get(c["id"], 0)
            c.pop("ssn_last4", None)  # roster is a list view — never show SSN here even to admin

        # M3 — city licenses due within 30 days (dashboard widget).
        due_soon = conn.execute(
            """
            SELECT fp.id, fp.period_due_date, fp.status, ca.city_name, ca.account_number,
                   cc.id AS client_id, cc.name AS client_name
            FROM compliance_filing_periods fp
            JOIN compliance_accounts ca ON ca.id = fp.compliance_account_id
            JOIN compliance_clients cc ON cc.id = ca.compliance_client_id
            WHERE ca.account_type = 'city_license'
              AND fp.status != 'done'
              AND fp.period_due_date IS NOT NULL
              AND date(fp.period_due_date) <= date('now', '+30 days')
            ORDER BY fp.period_due_date ASC
            """
        ).fetchall()
        due_soon = [dict(r) for r in due_soon]
        for r in due_soon:
            try:
                from datetime import date
                days_left = (date.fromisoformat(str(r["period_due_date"])[:10]) - date.today()).days
            except Exception:
                days_left = None
            r["days_left"] = days_left
    finally:
        conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="compliance",
        clients=clients,
        q=q,
        show_inactive=show_inactive,
        due_soon=due_soon,
        is_admin=get_effective_role() == "admin",
    )
    return render_template("compliance_dashboard.html", **ctx)


# ── Client detail ────────────────────────────────────────────────────────────

@compliance_bp.get("/compliance/clients/<int:client_id>")
@login_required
def compliance_client_detail(client_id: int):
    from app import base_ctx

    role = get_effective_role()
    user_id = _current_user_id()

    conn = get_connection()
    try:
        client = conn.execute("SELECT * FROM compliance_clients WHERE id=?", (client_id,)).fetchone()
        if not client:
            return render_template("compliance_not_found.html", **base_ctx()), 404

        can_see_ssn = _client_can_see_ssn(client, role, user_id)
        client_dict = dict(client)
        if not can_see_ssn:
            client_dict["ssn_last4"] = None

        accounts = conn.execute(
            """
            SELECT ca.*, cr.login_username, cr.shared_login, cr.needs_rotation,
                   (cr.encrypted_password IS NOT NULL) AS has_password
            FROM compliance_accounts ca
            LEFT JOIN compliance_credentials cr ON cr.id = ca.credential_id
            WHERE ca.compliance_client_id = ?
            ORDER BY ca.account_type, ca.id
            """,
            (client_id,),
        ).fetchall()
        accounts = [_credential_masked_dict(r) if r["login_username"] is not None else dict(r) for r in accounts]

        periods_by_account = {}
        if accounts:
            account_ids = [a["id"] for a in accounts]
            placeholders = ",".join("?" * len(account_ids))
            for r in conn.execute(
                f"SELECT * FROM compliance_filing_periods WHERE compliance_account_id IN ({placeholders}) "
                "ORDER BY period_due_date IS NULL, period_due_date, id",
                account_ids,
            ).fetchall():
                periods_by_account.setdefault(r["compliance_account_id"], []).append(dict(r))

        notes = conn.execute(
            """
            SELECT n.*, u.display_name, u.username
            FROM compliance_correspondence_log n
            LEFT JOIN auth_users u ON u.id = n.created_by_user_id
            WHERE n.compliance_client_id = ?
            ORDER BY n.created_at DESC
            """,
            (client_id,),
        ).fetchall()
        notes = [dict(r) for r in notes]

        preparers = conn.execute(
            "SELECT id, username, display_name FROM auth_users WHERE is_active=1 AND role IN ('preparer','admin') ORDER BY display_name, username"
        ).fetchall()
        preparers = [dict(r) for r in preparers]

        credentials = conn.execute(
            "SELECT id, login_username, shared_login, needs_rotation, (encrypted_password IS NOT NULL) AS has_password FROM compliance_credentials ORDER BY login_username"
        ).fetchall()
        credentials = [_credential_masked_dict(r) for r in credentials]
    finally:
        conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="compliance",
        client=client_dict,
        can_see_ssn=can_see_ssn,
        accounts=accounts,
        periods_by_account=periods_by_account,
        notes=notes,
        preparers=preparers,
        all_credentials=credentials,
        account_types=ACCOUNT_TYPES,
        frequencies=FREQUENCIES,
        note_types=NOTE_TYPES,
        is_admin=role == "admin",
        can_manage_filings=role in ("preparer", "admin"),
    )
    return render_template("compliance_client_detail.html", **ctx)


# ── Client CRUD (admin only) ─────────────────────────────────────────────────

@compliance_bp.post("/api/compliance/clients")
@role_required("admin")
def api_create_client():
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    client_type = data.get("client_type") if data.get("client_type") in ("business", "individual") else "business"

    assigned = data.get("assigned_preparer_user_id")
    try:
        assigned = int(assigned) if assigned else None
    except (TypeError, ValueError):
        return jsonify({"error": "assigned_preparer_user_id must be an integer"}), 400

    ts = _now()
    conn = get_connection()
    try:
        if assigned is not None:
            row = conn.execute("SELECT id FROM auth_users WHERE id=? AND is_active=1", (assigned,)).fetchone()
            if not row:
                return jsonify({"error": "assigned_preparer_user_id is not an active user"}), 400

        cur = conn.execute(
            """
            INSERT INTO compliance_clients
                (name, client_type, corp_number, fein, address, city, zip, phone, ssn_last4,
                 assigned_preparer_user_id, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                name, client_type, data.get("corp_number"), data.get("fein"), data.get("address"),
                data.get("city"), data.get("zip"), data.get("phone"), data.get("ssn_last4"),
                assigned, ts, ts,
            ),
        )
        client_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_create_client failed")
        return jsonify({"error": "Could not create client"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_CLIENT_CREATED",
        entity_type="compliance_client", entity_id=client_id,
        before=None, after={"name": name, "client_type": client_type},
    )
    return jsonify({"success": True, "id": client_id})


@compliance_bp.post("/api/compliance/clients/<int:client_id>")
@role_required("admin")
def api_update_client(client_id: int):
    data = request.get_json(silent=True) or {}
    name = (data.get("name") or "").strip()
    if not name:
        return jsonify({"error": "name is required"}), 400
    client_type = data.get("client_type") if data.get("client_type") in ("business", "individual") else "business"

    assigned = data.get("assigned_preparer_user_id")
    try:
        assigned = int(assigned) if assigned else None
    except (TypeError, ValueError):
        return jsonify({"error": "assigned_preparer_user_id must be an integer"}), 400

    conn = get_connection()
    try:
        before = conn.execute("SELECT * FROM compliance_clients WHERE id=?", (client_id,)).fetchone()
        if not before:
            return jsonify({"error": "Client not found"}), 404
        if assigned is not None:
            row = conn.execute("SELECT id FROM auth_users WHERE id=? AND is_active=1", (assigned,)).fetchone()
            if not row:
                return jsonify({"error": "assigned_preparer_user_id is not an active user"}), 400

        conn.execute(
            """
            UPDATE compliance_clients
            SET name=?, client_type=?, corp_number=?, fein=?, address=?, city=?, zip=?, phone=?,
                ssn_last4=?, assigned_preparer_user_id=?, updated_at=?
            WHERE id=?
            """,
            (
                name, client_type, data.get("corp_number"), data.get("fein"), data.get("address"),
                data.get("city"), data.get("zip"), data.get("phone"), data.get("ssn_last4"),
                assigned, _now(), client_id,
            ),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_update_client failed for id=%s", client_id)
        return jsonify({"error": "Could not update client"}), 500
    finally:
        conn.close()

    before_dict = dict(before)
    before_dict.pop("ssn_last4", None)  # never persist SSN into the audit log
    after_dict = {"name": name, "client_type": client_type}
    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_CLIENT_UPDATED",
        entity_type="compliance_client", entity_id=client_id, before=before_dict, after=after_dict,
    )
    return jsonify({"success": True})


def _set_client_active(client_id: int, active: int):
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM compliance_clients WHERE id=?", (client_id,)).fetchone()
        if not row:
            return jsonify({"error": "Client not found"}), 404
        conn.execute("UPDATE compliance_clients SET active=?, updated_at=? WHERE id=?", (active, _now(), client_id))
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("_set_client_active failed")
        return jsonify({"error": "Could not update client"}), 500
    finally:
        conn.close()
    _enqueue_audit(
        user_id=_current_user_id(),
        action="COMPLIANCE_CLIENT_ACTIVATED" if active else "COMPLIANCE_CLIENT_DEACTIVATED",
        entity_type="compliance_client", entity_id=client_id,
    )
    return jsonify({"success": True})


@compliance_bp.post("/api/compliance/clients/<int:client_id>/deactivate")
@role_required("admin")
def api_deactivate_client(client_id: int):
    return _set_client_active(client_id, 0)


@compliance_bp.post("/api/compliance/clients/<int:client_id>/reactivate")
@role_required("admin")
def api_reactivate_client(client_id: int):
    return _set_client_active(client_id, 1)


# ── Account CRUD (admin only) ────────────────────────────────────────────────

@compliance_bp.post("/api/compliance/accounts")
@role_required("admin")
def api_create_account():
    data = request.get_json(silent=True) or {}
    try:
        client_id = int(data.get("compliance_client_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "compliance_client_id is required"}), 400
    account_type = data.get("account_type")
    if account_type not in ACCOUNT_TYPES:
        return jsonify({"error": f"account_type must be one of {ACCOUNT_TYPES}"}), 400
    frequency = data.get("frequency") or "quarterly"
    if frequency not in FREQUENCIES:
        return jsonify({"error": f"frequency must be one of {FREQUENCIES}"}), 400

    credential_id = data.get("credential_id")
    try:
        credential_id = int(credential_id) if credential_id else None
    except (TypeError, ValueError):
        return jsonify({"error": "credential_id must be an integer"}), 400

    try:
        fee = float(data.get("fee")) if data.get("fee") not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"error": "fee must be a number"}), 400

    ts = _now()
    conn = get_connection()
    try:
        client_row = conn.execute("SELECT id FROM compliance_clients WHERE id=?", (client_id,)).fetchone()
        if not client_row:
            return jsonify({"error": "Client not found"}), 404
        if credential_id is not None:
            cred_row = conn.execute("SELECT id FROM compliance_credentials WHERE id=?", (credential_id,)).fetchone()
            if not cred_row:
                return jsonify({"error": "Credential not found"}), 404

        cur = conn.execute(
            """
            INSERT INTO compliance_accounts
                (compliance_client_id, account_type, account_number, city_name, frequency,
                 credential_id, fee, active, created_at, updated_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
            """,
            (
                client_id, account_type, data.get("account_number"), data.get("city_name"),
                frequency, credential_id, fee, ts, ts,
            ),
        )
        account_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_create_account failed")
        return jsonify({"error": "Could not create account"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_ACCOUNT_CREATED",
        entity_type="compliance_account", entity_id=account_id,
        after={"compliance_client_id": client_id, "account_type": account_type},
    )
    return jsonify({"success": True, "id": account_id})


@compliance_bp.post("/api/compliance/accounts/<int:account_id>")
@role_required("admin")
def api_update_account(account_id: int):
    data = request.get_json(silent=True) or {}
    account_type = data.get("account_type")
    if account_type not in ACCOUNT_TYPES:
        return jsonify({"error": f"account_type must be one of {ACCOUNT_TYPES}"}), 400
    frequency = data.get("frequency") or "quarterly"
    if frequency not in FREQUENCIES:
        return jsonify({"error": f"frequency must be one of {FREQUENCIES}"}), 400

    credential_id = data.get("credential_id")
    try:
        credential_id = int(credential_id) if credential_id else None
    except (TypeError, ValueError):
        return jsonify({"error": "credential_id must be an integer"}), 400
    try:
        fee = float(data.get("fee")) if data.get("fee") not in (None, "") else None
    except (TypeError, ValueError):
        return jsonify({"error": "fee must be a number"}), 400

    conn = get_connection()
    try:
        before = conn.execute("SELECT * FROM compliance_accounts WHERE id=?", (account_id,)).fetchone()
        if not before:
            return jsonify({"error": "Account not found"}), 404
        if credential_id is not None:
            cred_row = conn.execute("SELECT id FROM compliance_credentials WHERE id=?", (credential_id,)).fetchone()
            if not cred_row:
                return jsonify({"error": "Credential not found"}), 404

        conn.execute(
            """
            UPDATE compliance_accounts
            SET account_type=?, account_number=?, city_name=?, frequency=?, credential_id=?, fee=?, updated_at=?
            WHERE id=?
            """,
            (account_type, data.get("account_number"), data.get("city_name"), frequency, credential_id, fee, _now(), account_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_update_account failed for id=%s", account_id)
        return jsonify({"error": "Could not update account"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_ACCOUNT_UPDATED",
        entity_type="compliance_account", entity_id=account_id,
        before=dict(before), after={"account_type": account_type, "frequency": frequency},
    )
    return jsonify({"success": True})


@compliance_bp.post("/api/compliance/accounts/<int:account_id>/deactivate")
@role_required("admin")
def api_deactivate_account(account_id: int):
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM compliance_accounts WHERE id=?", (account_id,)).fetchone()
        if not row:
            return jsonify({"error": "Account not found"}), 404
        conn.execute("UPDATE compliance_accounts SET active=0, updated_at=? WHERE id=?", (_now(), account_id))
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_deactivate_account failed")
        return jsonify({"error": "Could not deactivate account"}), 500
    finally:
        conn.close()
    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_ACCOUNT_DEACTIVATED",
        entity_type="compliance_account", entity_id=account_id,
    )
    return jsonify({"success": True})


# ── Credential admin (M1 security requirements) ──────────────────────────────

@compliance_bp.get("/compliance/credentials")
@role_required("admin")
def compliance_credentials_admin():
    from app import base_ctx

    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT cr.*, (cr.encrypted_password IS NOT NULL) AS has_password,
                   (SELECT COUNT(*) FROM compliance_accounts a WHERE a.credential_id = cr.id) AS account_count
            FROM compliance_credentials cr
            ORDER BY cr.needs_rotation DESC, cr.login_username
            """
        ).fetchall()
        credentials = [_credential_masked_dict(r) for r in rows]
    finally:
        conn.close()

    ctx = base_ctx()
    ctx.update(active_page="compliance_credentials", credentials=credentials)
    return render_template("compliance_credentials_admin.html", **ctx)


@compliance_bp.post("/api/compliance/credentials")
@role_required("admin")
def api_create_credential():
    data = request.get_json(silent=True) or {}
    login_username = (data.get("login_username") or "").strip()
    password = data.get("password") or None
    shared_login = bool(data.get("shared_login"))
    notes = data.get("notes")

    if not login_username and not password:
        return jsonify({"error": "login_username or password is required"}), 400

    encrypted = None
    if password:
        from compliance.crypto import encrypt_password
        encrypted = encrypt_password(password)

    ts = _now()
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO compliance_credentials
                (login_username, encrypted_password, encryption_key_ref, shared_login,
                 last_rotated_at, needs_rotation, notes, created_at, updated_at)
            VALUES (?, ?, 'default', ?, ?, 0, ?, ?, ?)
            """,
            (login_username, encrypted, int(shared_login), ts if password else None, notes, ts, ts),
        )
        cred_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_create_credential failed")
        return jsonify({"error": "Could not create credential"}), 500
    finally:
        conn.close()

    # Never include the password itself in the audit before/after payload.
    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_CREDENTIAL_CREATED",
        entity_type="compliance_credential", entity_id=cred_id,
        after={"login_username": login_username, "shared_login": shared_login, "password_set": bool(password)},
    )
    return jsonify({"success": True, "id": cred_id})


@compliance_bp.post("/api/compliance/credentials/<int:credential_id>")
@role_required("admin")
def api_update_credential(credential_id: int):
    data = request.get_json(silent=True) or {}
    login_username = (data.get("login_username") or "").strip()
    new_password = data.get("password")  # None/omitted = leave password unchanged
    shared_login = bool(data.get("shared_login"))
    notes = data.get("notes")

    conn = get_connection()
    try:
        before = conn.execute("SELECT id FROM compliance_credentials WHERE id=?", (credential_id,)).fetchone()
        if not before:
            return jsonify({"error": "Credential not found"}), 404

        ts = _now()
        if new_password:
            from compliance.crypto import encrypt_password
            encrypted = encrypt_password(new_password)
            conn.execute(
                """
                UPDATE compliance_credentials
                SET login_username=?, encrypted_password=?, shared_login=?, notes=?,
                    last_rotated_at=?, needs_rotation=0, updated_at=?
                WHERE id=?
                """,
                (login_username, encrypted, int(shared_login), notes, ts, ts, credential_id),
            )
        else:
            conn.execute(
                """
                UPDATE compliance_credentials
                SET login_username=?, shared_login=?, notes=?, updated_at=?
                WHERE id=?
                """,
                (login_username, int(shared_login), notes, ts, credential_id),
            )
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_update_credential failed for id=%s", credential_id)
        return jsonify({"error": "Could not update credential"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_CREDENTIAL_UPDATED",
        entity_type="compliance_credential", entity_id=credential_id,
        after={"login_username": login_username, "shared_login": shared_login, "password_rotated": bool(new_password)},
    )
    return jsonify({"success": True})


@compliance_bp.post("/api/compliance/credentials/<int:credential_id>/reveal")
@role_required("admin")
def api_reveal_credential(credential_id: int):
    """Decrypt and return a credential's password ONCE. Security requirement
    #2 — admin-only (enforced by role_required above) AND every call is
    audit-logged (who/when/which credential), regardless of outcome."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT login_username, encrypted_password FROM compliance_credentials WHERE id=?",
            (credential_id,),
        ).fetchone()
    finally:
        conn.close()

    if not row:
        return jsonify({"error": "Credential not found"}), 404

    # Audit the reveal attempt BEFORE returning the value, so a decrypt
    # failure still leaves a record that someone tried.
    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_CREDENTIAL_REVEALED",
        entity_type="compliance_credential", entity_id=credential_id,
        after={"login_username": row["login_username"]},  # never the password
    )

    if not row["encrypted_password"]:
        return jsonify({"error": "No password is stored for this credential"}), 400

    from compliance.crypto import ComplianceCryptoError, decrypt_password
    try:
        plaintext = decrypt_password(row["encrypted_password"])
    except ComplianceCryptoError as exc:
        logger.error("compliance: reveal failed for credential_id=%s: %s", credential_id, exc)
        return jsonify({"error": str(exc)}), 500

    return jsonify({"success": True, "login_username": row["login_username"], "password": plaintext})


# ── Filing period board (M2) ────────────────────────────────────────────────

@compliance_bp.get("/compliance/periods")
@login_required
def compliance_periods_board():
    from app import base_ctx

    period_label = (request.args.get("period_label") or "").strip()
    account_type = request.args.get("account_type") or ""
    frequency = request.args.get("frequency") or ""
    city = (request.args.get("city") or "").strip()
    status = request.args.get("status") or ""
    assigned_preparer_user_id = request.args.get("assigned_preparer_user_id") or ""

    sql = """
        SELECT fp.*, ca.account_type, ca.account_number, ca.city_name, ca.frequency,
               cc.id AS client_id, cc.name AS client_name, cc.assigned_preparer_user_id,
               u.display_name AS assigned_preparer_name, u.username AS assigned_preparer_username
        FROM compliance_filing_periods fp
        JOIN compliance_accounts ca ON ca.id = fp.compliance_account_id
        JOIN compliance_clients cc ON cc.id = ca.compliance_client_id
        LEFT JOIN auth_users u ON u.id = cc.assigned_preparer_user_id
        WHERE ca.active = 1
    """
    params: list = []
    if period_label:
        sql += " AND fp.period_label = ?"
        params.append(period_label)
    if account_type:
        sql += " AND ca.account_type = ?"
        params.append(account_type)
    if frequency:
        sql += " AND ca.frequency = ?"
        params.append(frequency)
    if city:
        sql += " AND ca.city_name LIKE ?"
        params.append(f"%{city}%")
    if status:
        sql += " AND fp.status = ?"
        params.append(status)
    if assigned_preparer_user_id:
        sql += " AND cc.assigned_preparer_user_id = ?"
        params.append(assigned_preparer_user_id)
    sql += " ORDER BY fp.period_due_date IS NULL, fp.period_due_date, cc.name"

    conn = get_connection()
    try:
        periods = [dict(r) for r in conn.execute(sql, params).fetchall()]
        period_labels = [
            r["period_label"] for r in conn.execute(
                "SELECT DISTINCT period_label FROM compliance_filing_periods ORDER BY period_label DESC"
            ).fetchall()
        ]
        preparers = [
            dict(r) for r in conn.execute(
                "SELECT id, username, display_name FROM auth_users WHERE is_active=1 AND role IN ('preparer','admin') ORDER BY display_name, username"
            ).fetchall()
        ]
    finally:
        conn.close()

    role = get_effective_role()
    ctx = base_ctx()
    ctx.update(
        active_page="compliance_periods",
        periods=periods,
        period_labels=period_labels,
        preparers=preparers,
        account_types=ACCOUNT_TYPES,
        frequencies=FREQUENCIES,
        statuses=PERIOD_STATUSES,
        filters={
            "period_label": period_label, "account_type": account_type, "frequency": frequency,
            "city": city, "status": status, "assigned_preparer_user_id": assigned_preparer_user_id,
        },
        can_manage_filings=role in ("preparer", "admin"),
    )
    return render_template("compliance_periods_board.html", **ctx)


@compliance_bp.post("/api/compliance/periods/<int:period_id>/status")
@permission_required("can_manage_compliance_filings")
def api_update_period_status(period_id: int):
    data = request.get_json(silent=True) or {}
    status = data.get("status")
    if status not in PERIOD_STATUSES:
        return jsonify({"error": f"status must be one of {PERIOD_STATUSES}"}), 400

    conn = get_connection()
    try:
        before = conn.execute("SELECT * FROM compliance_filing_periods WHERE id=?", (period_id,)).fetchone()
        if not before:
            return jsonify({"error": "Filing period not found"}), 404

        ts = _now()
        set_clauses = ["status=?", "updated_at=?"]
        params: list = [status, ts]
        if status == "sales_in" and not before["sales_data_received_at"]:
            set_clauses.append("sales_data_received_at=?")
            params.append(ts)
        if status == "filed" and not before["filed_at"]:
            set_clauses.append("filed_at=?")
            params.append(ts)
        if status == "done":
            set_clauses += ["done_at=?", "done_by_user_id=?"]
            params += [ts, _current_user_id()]
        params.append(period_id)

        conn.execute(f"UPDATE compliance_filing_periods SET {', '.join(set_clauses)} WHERE id=?", params)
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_update_period_status failed for id=%s", period_id)
        return jsonify({"error": "Could not update status"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_PERIOD_STATUS_UPDATED",
        entity_type="compliance_filing_period", entity_id=period_id,
        before={"status": before["status"]}, after={"status": status},
    )
    return jsonify({"success": True, "status": status})


@compliance_bp.post("/api/compliance/periods/roll-forward")
@permission_required("can_manage_compliance_filings")
def api_roll_forward_periods():
    """Bulk 'roll forward' — every active account that has a filing_period
    for `source_period_label` gets a new filing_period for
    `target_period_label` (skipped if one already exists), copying
    period_type/fee. This reproduces the account list from the source
    period without manual re-entry (M2 acceptance criteria) — matched by
    account, not by any date-math on the free-text period_label, so it
    works the same for quarterly/monthly/annual cadences."""
    data = request.get_json(silent=True) or {}
    source_label = (data.get("source_period_label") or "").strip()
    target_label = (data.get("target_period_label") or "").strip()
    if not source_label or not target_label:
        return jsonify({"error": "source_period_label and target_period_label are required"}), 400
    if source_label == target_label:
        return jsonify({"error": "source and target period labels must differ"}), 400

    conn = get_connection()
    try:
        source_rows = conn.execute(
            """
            SELECT fp.compliance_account_id, fp.period_type, fp.fee
            FROM compliance_filing_periods fp
            JOIN compliance_accounts ca ON ca.id = fp.compliance_account_id
            WHERE fp.period_label = ? AND ca.active = 1
            """,
            (source_label,),
        ).fetchall()

        if not source_rows:
            return jsonify({"error": f"No filing periods found for period_label={source_label!r}"}), 404

        created = 0
        skipped_existing = 0
        ts = _now()
        for r in source_rows:
            existing = conn.execute(
                "SELECT id FROM compliance_filing_periods WHERE compliance_account_id=? AND period_label=?",
                (r["compliance_account_id"], target_label),
            ).fetchone()
            if existing:
                skipped_existing += 1
                continue
            conn.execute(
                """
                INSERT INTO compliance_filing_periods
                    (compliance_account_id, period_type, period_label, fee, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, 'needs_sales_data', ?, ?)
                """,
                (r["compliance_account_id"], r["period_type"], target_label, r["fee"], ts, ts),
            )
            created += 1
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_roll_forward_periods failed")
        return jsonify({"error": "Could not roll forward periods"}), 500
    finally:
        conn.close()

    _enqueue_audit(
        user_id=_current_user_id(), action="COMPLIANCE_PERIODS_ROLLED_FORWARD",
        entity_type="compliance_filing_period", entity_id=target_label,
        after={"source_period_label": source_label, "target_period_label": target_label, "created": created},
    )
    return jsonify({"success": True, "created": created, "skipped_existing": skipped_existing})


# ── Correspondence log (M5) ──────────────────────────────────────────────────

@compliance_bp.post("/api/compliance/correspondence")
@permission_required("can_manage_compliance_filings")
def api_create_correspondence_note():
    data = request.get_json(silent=True) or {}
    try:
        client_id = int(data.get("compliance_client_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "compliance_client_id is required"}), 400
    month = (data.get("month") or "").strip()
    note = (data.get("note") or "").strip()
    note_type = data.get("note_type") or "general"
    if note_type not in NOTE_TYPES:
        return jsonify({"error": f"note_type must be one of {NOTE_TYPES}"}), 400
    if not month or not note:
        return jsonify({"error": "month and note are required"}), 400

    conn = get_connection()
    try:
        client_row = conn.execute("SELECT id FROM compliance_clients WHERE id=?", (client_id,)).fetchone()
        if not client_row:
            return jsonify({"error": "Client not found"}), 404
        cur = conn.execute(
            """
            INSERT INTO compliance_correspondence_log
                (compliance_client_id, month, note_type, note, created_by_user_id, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (client_id, month, note_type, note, _current_user_id(), _now()),
        )
        note_id = cur.lastrowid
        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("api_create_correspondence_note failed")
        return jsonify({"error": "Could not create note"}), 500
    finally:
        conn.close()

    return jsonify({"success": True, "id": note_id})


# ── Data Download export (M5) ────────────────────────────────────────────────

@compliance_bp.get("/api/compliance/export/data-download.csv")
@login_required
def api_export_data_download():
    """CSV export aggregating status across all periods for a given year —
    structurally mirrors the old workbook's DATA DOWNLOAD sheet. Never
    includes ssn_last4 or any credential field (privacy invariant)."""
    year = (request.args.get("year") or "").strip()

    sql = """
        SELECT cc.name AS client_name, cc.client_type, ca.account_type, ca.account_number,
               ca.city_name, ca.frequency, fp.period_type, fp.period_label, fp.status,
               fp.fee, fp.period_due_date, fp.sales_data_received_at, fp.filed_at, fp.done_at
        FROM compliance_filing_periods fp
        JOIN compliance_accounts ca ON ca.id = fp.compliance_account_id
        JOIN compliance_clients cc ON cc.id = ca.compliance_client_id
        WHERE 1=1
    """
    params: list = []
    if year:
        sql += " AND (fp.period_label LIKE ? OR fp.period_due_date LIKE ?)"
        params += [f"%{year}%", f"{year}%"]
    sql += " ORDER BY cc.name, ca.account_type, fp.period_label"

    conn = get_connection()
    try:
        rows = conn.execute(sql, params).fetchall()
    finally:
        conn.close()

    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow([
        "Client Name", "Client Type", "Account Type", "Account Number", "City", "Frequency",
        "Period Type", "Period Label", "Status", "Fee", "Due Date", "Sales Data Received",
        "Filed At", "Done At",
    ])
    for r in rows:
        writer.writerow([
            r["client_name"], r["client_type"], r["account_type"], r["account_number"], r["city_name"],
            r["frequency"], r["period_type"], r["period_label"], r["status"], r["fee"], r["period_due_date"],
            r["sales_data_received_at"], r["filed_at"], r["done_at"],
        ])

    filename = f"compliance_data_download_{year or 'all'}.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )
