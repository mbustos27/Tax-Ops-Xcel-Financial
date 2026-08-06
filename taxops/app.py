from __future__ import annotations

import contextlib
import functools
import os
import sys
from datetime import date, datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import (
    Flask, abort, current_app, flash, g, jsonify, redirect, render_template,
    request, Response, send_file, session, url_for,
)

import json
import io
import logging
import math
import secrets
import shutil
import sqlite3
import tempfile
import time
from urllib.parse import urlencode

import re as _re

from config import (
    APP_ENV,
    APP_NAME,
    DB_PATH,
    DRAKE_FOLDER_STRUCTURE_ENABLED,
    EMAIL_INBOX_DIR,
    INTAKE_AUTO_DISCOUNT,
    INTERVIEWERS,
    KNOWN_PROMOTIONAL_DOMAINS,
    MASS_MAILING_PREFIXES,
    MULTIYEAR_AGI_PERCENT_THRESHOLD,
    MULTIYEAR_BALANCE_ABS_THRESHOLD,
    MULTIYEAR_REFUND_ABS_THRESHOLD,
    SCAN_AGENT_TOKEN,
    SCAN_AGENT_URL,
    taxops_asset_cache_version,
    taxops_release_version,
)
from logging_config import configure_logging

configure_logging()

from env_validation import validate_taxops_environment_and_exit

validate_taxops_environment_and_exit()

from csv_analyzer import analyze, iter_data_rows, normalize_status
from db import (
    CURRENT_SCHEMA_VERSION,
    get_active_intake_tax_year,
    get_connection,
    get_schema_version,
    init_db,
    set_active_intake_tax_year,
)
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS
from merge_ops import merge_client_into
from bulk_returns import bulk_apply_processor_changes, bulk_apply_status_changes
from name_matcher import find_client as fuzzy_find_client, is_business, parse_name, _all_clients_cache
from normalizer import normalize_date, normalize_currency, normalize_string, canonical_status, is_locked_status
from preparer import (
    normalize_preparer,
    preparer_dropdown_options,
    preparer_filter_match_values,
    preparer_list_label,
)
import multiyear_comparison
import season_rollover
from source_compare import (
    discover_default_paths,
    list_csv_basenames,
    run_compare,
    safe_resolve_csv,
)
from utils import (
    get_drake_documents_path,
    get_return_documents_path,
    now,
    parse_iso_datetime,
    sanitize_filename,
    scrub_ssn_from_dict,
    _enqueue_extraction,
)
from mail_watcher import start_mail_watcher
from extractor import start_extraction_worker
from drake_documents_sync import sync_to_drake

_APP_START_MONOTONIC = time.monotonic()

app = Flask(__name__)
# CACHE / #142: never rely on intermediary caches honoring long TTL for send_file-backed responses.
app.config["SEND_FILE_MAX_AGE_DEFAULT"] = 0
# SEC-3: only reload templates in debug/dev mode — avoids unnecessary disk I/O in production.
app.config["TEMPLATES_AUTO_RELOAD"] = app.debug
# CACHE / #143: ``?v=`` on static URLs — resolved via taxops_asset_cache_version() (+ optional TAXOPS_APP_VERSION).
app.config["APP_VERSION"] = taxops_asset_cache_version()

# SEC-3: session cookie hardening + lifetime.
# SESSION_COOKIE_SECURE is gated on TAXOPS_HTTPS_ENABLED because the office LAN
# currently uses plain HTTP.  Set TAXOPS_HTTPS_ENABLED=true once a reverse proxy
# or load balancer terminates TLS in front of this server.
app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    SESSION_COOKIE_SECURE=os.environ.get("TAXOPS_HTTPS_ENABLED", "false").lower() == "true",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
)

# SEC-5: cap incoming request bodies so a large upload cannot exhaust disk or
# tie up Waitress threads.  50 MB covers the largest realistic tax document
# bundles (multi-page PDF + photos).  Overridable via TAXOPS_MAX_UPLOAD_MB.
_max_mb = max(1, min(int(os.environ.get("TAXOPS_MAX_UPLOAD_MB", "50")), 500))
app.config["MAX_CONTENT_LENGTH"] = _max_mb * 1024 * 1024

# SEC-1: CSRF protection via Flask-WTF.
# WTF_CSRF_SECRET_KEY defaults to Flask's secret_key when not set separately — that is intentional here.
# TESTING mode disables enforcement automatically (set app.config["WTF_CSRF_ENABLED"] = False in tests).
from flask_wtf.csrf import CSRFProtect, CSRFError
_csrf = CSRFProtect(app)

# ── I18N-1: Flask-Babel ───────────────────────────────────────────────────────
from flask_babel import Babel, gettext as _t

_SUPPORTED_LOCALES = ("en", "es_MX")


def _get_locale() -> str:
    locale = session.get("locale", "en")
    return locale if locale in _SUPPORTED_LOCALES else "en"


babel = Babel(app, locale_selector=_get_locale)

app.config["BABEL_DEFAULT_LOCALE"] = "en"
app.config["BABEL_TRANSLATION_DIRECTORIES"] = "translations"

from audit_service import (
    audit_queue_depth,
    fetch_audit_entry,
    format_json_diff_styled_chunks,
    get_audit_retention_years,
    purge_audit_logs_older_than,
    query_audit_logs,
    register_audit_hooks,
    sanitize_filename_audit,
    set_audit_retention_years,
    start_audit_writer,
    write_audit_export_csv,
)

register_audit_hooks(app)
start_audit_writer()   # REL-2: single long-lived writer thread

# DEBT-1: register extracted blueprints.
from routes.documents import documents_bp
from routes.accounting import accounting_bp
from routes.users import users_bp
from routes.reports import reports_bp
from routes.sender_rules import sender_rules_bp
from routes.email_health import email_health_bp
from routes.filetrack import api_filetrack_status, filetrack_bp
from routes.work_orders import work_orders_bp
from routes.work_order_quick_picks import wo_quick_picks_bp
from routes.notifications import notifications_bp
from routes.compliance import compliance_bp
from routes.reception_agents import reception_agents_bp
app.register_blueprint(documents_bp)
app.register_blueprint(accounting_bp)
app.register_blueprint(users_bp)
app.register_blueprint(reports_bp)
app.register_blueprint(sender_rules_bp)
app.register_blueprint(email_health_bp)
app.register_blueprint(filetrack_bp)
app.register_blueprint(reception_agents_bp)
# POST /filetrack/status is hit by the headless filetrack.listener process
# (M3), which has no browser session and therefore no CSRF token to send —
# it authenticates instead via its own X-Filetrack-Token header (see
# routes/filetrack.py's module docstring: "Auth model (deliberately NOT
# session/RBAC ...)"). Without this exemption, Flask-WTF's global
# CSRFProtect(app) above 400s every real scan with "CSRF token missing or
# invalid" — confirmed in production on 2026-07-22 (first live listener run).
# Exempting the specific view function, not the whole blueprint, so the
# admin-only GET routes in this blueprint keep normal CSRF behavior (GET is
# CSRF-exempt by default anyway) and any *future* POST route added to this
# blueprint still requires a deliberate, separate exemption decision.
_csrf.exempt(api_filetrack_status)
app.register_blueprint(work_orders_bp)
app.register_blueprint(wo_quick_picks_bp)
app.register_blueprint(notifications_bp)
app.register_blueprint(compliance_bp)


# REL-4: Flask g-based DB helper — lets routes use get_db() and have the connection
# closed automatically at teardown, as a safer alternative to manual try/finally.
def get_db():
    """Return a per-request SQLite connection stored on Flask g (auto-closed at teardown)."""
    if not hasattr(g, "db"):
        g.db = get_connection()
    return g.db


@app.teardown_appcontext
def _close_db(exc):  # noqa: ARG001
    db = g.pop("db", None)
    if db is not None:
        db.close()


app.jinja_env.globals["preparer_list_label"] = preparer_list_label

# Secret key for signing session cookies.
# Set TAXOPS_SECRET env-var in production; a random fallback is fine for dev.
_secret = os.environ.get("TAXOPS_SECRET")
if not _secret:
    _secret_file = os.path.join(os.path.dirname(__file__), ".secret_key")
    if os.path.exists(_secret_file):
        _secret = open(_secret_file, "rb").read()
    else:
        _secret = os.urandom(32)
        with open(_secret_file, "wb") as _f:
            _f.write(_secret)
app.secret_key = _secret

# SEC-2: Legacy env-var credentials kept only for the bootstrap seed and test fixtures.
# Production auth now goes through auth_users (check_password_hash).
# These are intentionally preserved so that an existing deployment that hasn't yet
# been bootstrapped can still log in via the fallback path below.
_LOGIN_USER = os.environ.get("TAXOPS_USER", "info")
_LOGIN_PASS = os.environ.get("TAXOPS_PASS", "2703Tax")


# ── SEC-2: per-user auth helpers ─────────────────────────────────────────────

from werkzeug.security import check_password_hash, generate_password_hash


def _auth_users_exist(conn) -> bool:
    """Return True if the auth_users table has at least one active user row."""
    row = conn.execute("SELECT 1 FROM auth_users WHERE is_active = 1 LIMIT 1").fetchone()
    return row is not None


def bootstrap_auth_user(conn) -> bool:
    """SEC-2: If auth_users is empty, seed one admin from TAXOPS_USER/TAXOPS_PASS env vars.

    Returns True if a new row was created, False if the table already had users.
    Safe to call every startup — is a no-op once any row exists.
    """
    if _auth_users_exist(conn):
        return False
    username = (os.environ.get("TAXOPS_USER") or "").strip()
    password = os.environ.get("TAXOPS_PASS") or ""
    if not username or not password:
        return False
    hashed = generate_password_hash(password)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'admin', 1, ?)
        """,
        (username, hashed, username, time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())),
    )
    conn.commit()
    return True


# SEC-7: max consecutive failures before account is locked.
# Set TAXOPS_LOGIN_MAX_ATTEMPTS in the environment to change the threshold.
# Lockout duration is TAXOPS_LOGIN_LOCKOUT_MINUTES (default 15).
_LOGIN_MAX_ATTEMPTS: int = max(1, int(os.environ.get("TAXOPS_LOGIN_MAX_ATTEMPTS", "5")))
_LOGIN_LOCKOUT_MINUTES: int = max(1, int(os.environ.get("TAXOPS_LOGIN_LOCKOUT_MINUTES", "15")))

# Sentinel returned by _authenticate_user to distinguish lockout from bad credentials.
_AUTH_LOCKED = object()


def _authenticate_user(username: str, password: str):
    """SEC-2/SEC-7: Look up username in auth_users, enforce lockout, verify hash.

    Returns:
      - dict with 'username'/'display_name'/'role' on success
      - _AUTH_LOCKED sentinel when the account is currently locked
      - None on bad credentials or unknown user

    Falls back to env-var plaintext comparison ONLY when the table has no active
    users yet (bootstrap not yet run), so existing deployments aren't locked out.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM auth_users WHERE username = ? AND is_active = 1 LIMIT 1",
            (username,),
        ).fetchone()
        if row:
            now_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

            # SEC-7: check existing lockout before verifying the password.
            if row["locked_until"] and row["locked_until"] > now_utc:
                return _AUTH_LOCKED

            if check_password_hash(row["password_hash"], password):
                conn.execute(
                    "UPDATE auth_users SET last_login_at = ?, failed_attempts = 0, locked_until = NULL WHERE id = ?",
                    (now_utc, row["id"]),
                )
                conn.commit()
                return {
                    "username": row["username"],
                    "display_name": row["display_name"] or row["username"],
                    "role": row["role"],
                    "must_change_password": int(row["must_change_password"] or 0),
                }
            else:
                new_attempts = (row["failed_attempts"] or 0) + 1
                locked_until = None
                if new_attempts >= _LOGIN_MAX_ATTEMPTS:
                    import datetime as _dt
                    locked_until = (
                        _dt.datetime.now(_dt.timezone.utc) + _dt.timedelta(minutes=_LOGIN_LOCKOUT_MINUTES)
                    ).strftime("%Y-%m-%dT%H:%M:%SZ")
                conn.execute(
                    "UPDATE auth_users SET failed_attempts = ?, locked_until = ? WHERE id = ?",
                    (new_attempts, locked_until, row["id"]),
                )
                conn.commit()
                return None

        # Fallback: no users in table yet — accept TAXOPS_USERS_MAP credentials.
        if not _auth_users_exist(conn):
            import hmac as _hmac
            from config import TAXOPS_USERS_MAP
            entry = TAXOPS_USERS_MAP.get(username)
            if entry and _hmac.compare_digest(entry["password"], password):
                return {"username": username, "display_name": username, "role": entry["role"]}
        return None
    finally:
        conn.close()


def privacy_mode_enabled() -> bool:
    return bool(session.get("privacy_mode"))


def _mask_value(value):
    if value is None:
        return None
    if isinstance(value, str):
        return "XXXXX" if value.strip() else value
    return "XXXXX"


def _mask_return_payload(payload: dict) -> dict:
    masked = dict(payload)
    for key in (
        "last_name", "first_name", "display_name", "name_full",
        "referred_by", "ssn_last4", "zelle_or_check_ref",
        "cash_or_qpay_ref", "receipt_number",
    ):
        if key in masked:
            masked[key] = _mask_value(masked.get(key))
    return masked


def _mask_client_payload(payload: dict) -> dict:
    masked = dict(payload)
    for key in (
        "last_name", "first_name", "display_name", "ssn_last4",
        "spouse_last_name", "spouse_first_name",
        "taxpayer_phone", "taxpayer_cell", "taxpayer_work_phone",
        "spouse_cell", "spouse_work_phone",
        "taxpayer_email", "spouse_email",
        "address", "referred_by",
    ):
        if key in masked:
            masked[key] = _mask_value(masked.get(key))
    return masked


# DEBT-1: auth helpers live in auth.py to avoid circular imports with blueprints.
from auth import login_required, role_required, view_only_for, get_effective_role, permission_required, has_permission  # noqa: E402 (import after path setup)
app.jinja_env.globals["get_effective_role"] = get_effective_role
app.jinja_env.globals["has_permission"] = has_permission


def _compact_currency(v):
    """Jinja2 filter: formats large dollar amounts as $417k or $1.2M.
    Values below $10,000 are shown as-is: $4,250."""
    v = float(v or 0)
    if v >= 1_000_000:
        return f"${v / 1_000_000:.1f}M"
    if v >= 10_000:
        return f"${v / 1_000:.0f}k"
    return f"${v:,.0f}"


app.jinja_env.filters["compact_currency"] = _compact_currency


@app.after_request
def _security_headers(response):
    """SEC-3 / DEBT-5: security response headers — this app is internal-only (LAN).

    Cache-Control strategy (DEBT-5):
    - Versioned /static/ assets (?v=... query param added by taxops_asset_cache_version)
      get "public, max-age=31536000, immutable" so browsers re-use them across sessions.
    - All other responses (HTML pages, API JSON) stay "no-store" to prevent sensitive data
      from being served from browser cache.

    CSP notes:
    - 'unsafe-inline' for script/style covers inline <script> blocks in templates.
      A future hardening pass (nonce-based CSP) can eliminate it — tracked in DEBT-2.
    - object-src 'none', base-uri 'self', form-action 'self' and
      frame-ancestors 'none' provide the highest-value protections even with
      'unsafe-inline' present.
    - img-src includes data: and blob: for document upload previews.
    """
    response.headers["X-Frame-Options"]        = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"]        = "same-origin"

    # DEBT-5: versioned static assets get a long-lived immutable cache.
    # Flask's test_request_context may not have request available, guard safely.
    try:
        is_static = request.path.startswith("/static/") and "v=" in request.query_string.decode("ascii", errors="replace")
    except RuntimeError:
        is_static = False

    if is_static:
        response.headers["Cache-Control"] = "public, max-age=31536000, immutable"
    else:
        response.headers["Cache-Control"] = "no-store"

    response.headers["Content-Security-Policy"] = (
        "default-src 'self'; "
        "script-src 'self' 'unsafe-inline'; "
        "style-src 'self' 'unsafe-inline'; "
        "img-src 'self' data: blob:; "
        "font-src 'self'; "
        "connect-src 'self'; "
        "object-src 'none'; "
        "base-uri 'self'; "
        "form-action 'self'; "
        "frame-ancestors 'none';"
    )
    return response


@app.errorhandler(403)
def _forbidden(e):
    # WO-1 QA: 403.html extends base.html, which needs the full base_ctx()
    # dict (status_counts, nav badge counts, etc.) — passing only `role`
    # crashed with a Jinja UndefinedError on every plain-HTML abort(403) in
    # the app (discovered via the Work Order Creator's RBAC tests, but this
    # affected every existing abort(403) path, not just the new one).
    ctx = base_ctx()
    ctx["role"] = get_effective_role()
    return render_template("403.html", **ctx), 403


@app.errorhandler(CSRFError)
def _csrf_error(e: CSRFError):
    """SEC-1: return a clean JSON/HTML error instead of Werkzeug 400 page."""
    p = request.path or ""
    if p.startswith("/api/") or p.startswith("/ai/"):
        return jsonify({"error": _t("CSRF token missing or invalid. Reload the page and try again.")}), 400
    return "<h1>400 Bad Request</h1><p>CSRF token missing or invalid. Please go back and try again.</p>", 400


from werkzeug.exceptions import RequestEntityTooLarge


# ── Preview-as-role feature ──────────────────────────────────────────────────

def admin_only(f):
    """Decorator: require the user's TRUE role (session["role"]) to be admin.

    Distinct from role_required("admin") which reads the *effective* role and
    would therefore block an admin while they are in preview mode.
    """
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login"))
        if session.get("role") != "admin":
            abort(403)
        return f(*args, **kwargs)
    return wrapper


@app.route("/admin/preview-role", methods=["POST"])
@admin_only
def start_role_preview():
    if session.get("preview_role"):
        return redirect(url_for("dashboard"))
    requested_role = request.form.get("role", "").strip().lower()
    if requested_role not in ("receptionist", "preparer"):
        abort(400)
    session["true_role"] = session["role"]
    session["preview_role"] = requested_role
    flash(f"Previewing as {requested_role}. Your changes are real — be careful.", "warning")
    return redirect(url_for("dashboard"))


@app.route("/admin/exit-preview", methods=["POST"])
def exit_role_preview():
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    if not session.get("preview_role"):
        return redirect(url_for("dashboard"))
    session.pop("preview_role", None)
    session.pop("true_role", None)
    flash("Back to admin view.", "success")
    return redirect(url_for("dashboard"))


@app.errorhandler(RequestEntityTooLarge)
def _handle_too_large(_e: RequestEntityTooLarge):
    """SEC-5: return a readable JSON 413 instead of Werkzeug's HTML page."""
    limit_mb = app.config.get("MAX_CONTENT_LENGTH", 0) // (1024 * 1024)
    return jsonify({"error": _t("File too large. Maximum upload size is %(mb)s MB.", mb=limit_mb)}), 413


# ── Workflow constants ────────────────────────────────────────────────────────

STATUS_FLOW = ["PENDING INTAKE", "PROCESSING", "HOLD", "FINALIZE", "PICKUP", "EFILE READY", "LOG OUT", "REJECTED"]

# Desk-safe targets Reception may always set (from any current status).
# Never includes FINALIZE / LOG OUT / REJECTED / PENDING INTAKE / unrestricted EFILE READY.
RECEPTIONIST_STATUS_ALWAYS: frozenset[str] = frozenset({"HOLD", "PROCESSING", "PICKUP"})
# Extra targets allowed only when the file is already in that status (desk handoff).
RECEPTIONIST_STATUS_FROM: dict[str, frozenset[str]] = {
    "PICKUP": frozenset({"EFILE READY"}),
}


def receptionist_allowed_statuses(current_status: str | None) -> list[str]:
    """STATUS_FLOW-ordered statuses Reception may set from ``current_status``.

    Always includes the current status (so the UI can show it selected). Destination
    set is RECEPTIONIST_STATUS_ALWAYS plus any RECEPTIONIST_STATUS_FROM extras.
    """
    cur = (current_status or "").strip().upper()
    allowed: set[str] = set(RECEPTIONIST_STATUS_ALWAYS)
    allowed |= RECEPTIONIST_STATUS_FROM.get(cur, frozenset())
    if cur in STATUS_FLOW:
        allowed.add(cur)
    return [s for s in STATUS_FLOW if s in allowed]


def receptionist_may_set_status(current_status: str | None, new_status: str) -> bool:
    """True when Reception is allowed to change current → new (including no-op)."""
    new = (new_status or "").strip().upper()
    if new not in STATUS_FLOW:
        return False
    return new in receptionist_allowed_statuses(current_status)


app.jinja_env.globals["receptionist_allowed_statuses"] = receptionist_allowed_statuses


def _get_json_safe() -> dict | None:
    """SEC-1: safe JSON body parser.

    Accepts requests whose Content-Type contains 'application/json' OR whose body
    looks like a JSON object/array (for legacy curl / integration callers that omit the
    Content-Type header). Returns the parsed dict/list, or None if parsing fails.
    Callers that need to reject a missing body entirely should check the return value.
    """
    ct = (request.content_type or "").lower()
    if "application/json" in ct:
        return request.get_json(silent=True)
    # Fallback: try parsing if there is a body (tolerates missing Content-Type header).
    if request.data:
        return request.get_json(force=True, silent=True)
    return None


# Rejected-return client contact tracking (stored on returns; privacy: no SSN fields)
CONTACT_STATUS_VALUES = ("not_contacted", "contacted", "follow_up_needed", "resolved")
CONTACT_LABELS = {
    "not_contacted":    "Not contacted",
    "contacted":        "Contacted",
    "follow_up_needed": "Follow-up needed",
    "resolved":         "Resolved (contact)",
}

STATUS_BADGE = {
    "PENDING INTAKE": "sb-pending",
    "PROCESSING":     "sb-processing",
    "HOLD":           "sb-hold",
    "FINALIZE":       "sb-finalize",
    "PICKUP":         "sb-pickup",
    "EFILE READY":    "sb-efile",
    "LOG OUT":        "sb-logout",
    "REJECTED":       "sb-rejected",
}

STATUS_DOT = {
    "PENDING INTAKE": "dot-violet",
    "PROCESSING":  "dot-amber",
    "HOLD":        "dot-hold",
    "FINALIZE":    "dot-orange",
    "PICKUP":      "dot-teal",
    "EFILE READY": "dot-indigo",
    "LOG OUT":     "dot-slate",
    "REJECTED":    "dot-red",
}

# When advancing to these statuses, auto-stamp the corresponding date field
# only if it hasn't been set yet.
STATUS_DATE_STAMP = {
    "PICKUP":  "pickup_date",   # client called in to sign
    "LOG OUT": "logout_date",   # accepted, case closed
}

# Operational risk thresholds
LATE_INTAKE_MONTH = 4
LATE_INTAKE_DAY = 1
SLOW_CYCLE_DAYS = 21

# Fields that live in the returns table and may be edited via /api/return/<id>/field
RETURN_EDITABLE = {
    "processor", "verified", "email_marker", "tax_year",
    "intake_date", "date_emailed", "pickup_date", "logout_date", "updated_date",
    "efile_date", "ack_date",
    "is_amended", "has_w7", "is_extension",
    "extension_requested", "extension_filed_date",
    "extension_ack_status", "extension_ack_date", "extension_due_date",
    "transfer_flag", "transfer_2025_flag", "transfer_2026_flag",
    "signatures_given", "signatures_received",
    "signatures_given_method", "signatures_received_method",
    "filing_status",
    # Processing fields (filled after intake — editable on return detail page)
    "promise_date", "delivered_by", "date_signatures_emailed", "date_reports_emailed",
}

# Fields that live in the clients table
CLIENT_EDITABLE = {
    "display_name",
    "referred_by",
    "referral_flag",
    "last_name",
    "first_name",
    "address",
    "taxpayer_phone",
    "taxpayer_cell",
    "taxpayer_work_phone",
    "spouse_cell",
    "spouse_work_phone",
    "taxpayer_email",
    "spouse_email",
    "taxpayer_dob",
    "spouse_dob",
    "spouse_last_name",
    "spouse_first_name",
    "taxpayer_occupation",
    "spouse_occupation",
}

# Fields that live in the payments table
PAYMENT_EDITABLE = {
    "total_fee", "fee_paid", "receipt_number",
    "cc_fee", "zelle_or_check_ref", "cash_or_qpay_ref",
    "bank_deposit", "refund_amount", "payment_method", "check_number",
}

CARD_FEE_RATE = 0.03   # 3 % card processing surcharge

# Return document uploads (DOC-2)
_ALLOWED_RETURN_DOC_TYPES = frozenset(
    {
        "W-2",
        "1099",
        "paystub",
        "prior_return",
        "government_id",
        "misc",
        "unknown",
    }
)
_ALLOWED_RETURN_DOC_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".pdf"})

# ── SQL fragment shared by all queries ───────────────────────────────────────

_SELECT = """
SELECT
    r.id, r.log_number, r.tax_year, r.client_status, r.processor,
    r.verified, r.intake_date, r.date_emailed, r.pickup_date,
    r.logout_date, r.updated_date, r.email_marker,
    r.is_amended, r.has_w7, r.is_extension,
    r.transfer_flag, r.transfer_2025_flag, r.transfer_2026_flag,
    r.efile_date, r.ack_date, r.drake_status_raw,
    r.contact_status, r.last_contacted_date,
    r.filing_status,
    r.adjusted_gross_income,
    r.created_at, r.updated_at,
    c.id   AS client_id,
    c.last_name, c.first_name, c.display_name,
    c.spouse_first_name, c.spouse_last_name,
    c.referral_flag, c.referred_by, c.ssn_last4,
    p.id   AS payment_id,
    p.total_fee, p.fee_paid, p.receipt_number,
    p.cc_fee, p.zelle_or_check_ref, p.cash_or_qpay_ref,
    p.refund_amount, p.bank_deposit, p.payment_method, p.check_number,
    r.signatures_given, r.signatures_received,
    r.signatures_given_method, r.signatures_received_method,
    r.promise_date, r.delivered_by, r.date_signatures_emailed, r.date_reports_emailed,
    rf.form_1040, rf.sched_a_d, rf.sched_c, rf.sched_e,
    rf.form_1120, rf.form_1120s, rf.form_1065_llc,
    rf.corp_officer, rf.business_owner, rf.form_990_1041
FROM returns r
JOIN clients c ON c.id = r.client_id
LEFT JOIN payments p ON p.return_id = r.id
LEFT JOIN return_forms rf ON rf.return_id = r.id
"""

# ── DB helpers ────────────────────────────────────────────────────────────────

def _to_float(v) -> float:
    """Coerce DB value (float, int, str, or None) to float safely."""
    try:
        return float(v) if v is not None and v != "" else 0.0
    except (TypeError, ValueError):
        return 0.0


def _build_name_full(first: str, last: str, display_name: str,
                     spouse_first: str = "", spouse_last: str = "") -> str:
    """Drake-style display name: LAST, FIRST & SPOUSE_FIRST (if MFJ)."""
    if display_name:
        return display_name
    base = f"{last}, {first}".strip(", ") if first else last
    sp_first = (spouse_first or "").strip()
    if sp_first:
        return f"{base} & {sp_first}"
    return base


def _enrich(r: dict) -> dict:
    total = _to_float(r.get("total_fee"))
    paid  = _to_float(r.get("fee_paid"))
    r["balance"]      = round(total - paid, 2) if total else None
    r["paid_in_full"] = bool(total and paid >= total)
    r["color"]        = STATUS_DOT.get(r.get("client_status") or "", "dot-slate")
    r["badge_class"]  = STATUS_BADGE.get(r.get("client_status") or "", "sb-default")
    first = r.get("first_name") or ""
    last  = r.get("last_name")  or ""
    r["name_full"] = _build_name_full(
        first, last, r.get("display_name") or "",
        r.get("spouse_first_name") or "", r.get("spouse_last_name") or "",
    )
    r["forms"]         = _form_badges(r)
    # Pre-compute preparer display label so the AJAX row renderer doesn't need a server roundtrip.
    r["processor_label"] = preparer_list_label(r.get("processor") or "")
    intake_dt = _parse_iso_date(r.get("intake_date"))
    completion_dt = _parse_iso_date(r.get("logout_date")) or _parse_iso_date(r.get("ack_date"))
    r["cycle_days"] = (
        (completion_dt - intake_dt).days
        if intake_dt and completion_dt and completion_dt >= intake_dt
        else None
    )
    r["late_intake_flag"] = (
        bool(intake_dt) and
        (intake_dt.month > LATE_INTAKE_MONTH or (intake_dt.month == LATE_INTAKE_MONTH and intake_dt.day >= LATE_INTAKE_DAY))
    )
    r["slow_cycle_flag"] = bool(r["cycle_days"] is not None and r["cycle_days"] >= SLOW_CYCLE_DAYS)
    r["risk_flags"] = []
    if r["late_intake_flag"]:
        r["risk_flags"].append("LATE INTAKE")
    if r["slow_cycle_flag"]:
        r["risk_flags"].append("SLOW CYCLE")
    cs = r.get("contact_status") or ""
    if r.get("client_status") == "REJECTED" and cs in ("", "not_contacted", "follow_up_needed"):
        r["risk_flags"].append("CLIENT CONTACT")
    if privacy_mode_enabled():
        r = _mask_return_payload(r)
    return r


_NEEDS_ATTENTION_DRAKE_DONE = (
    "EF Accepted",
    "EF Ext Accepted",
    "E-Filed: YES",
    "Printed",
)
_NEEDS_ATTENTION_REASON_PRIORITY = {
    "ef_rejected": 0,
    "client_contact": 1,
    "stale_processing": 2,
}
_NEEDS_ATTENTION_LIST_CAP = 25


def _needs_attention_reason(row) -> str | None:
    """Return reason code for a return row, or None if it does not need attention."""
    get = row.get if isinstance(row, dict) else lambda k, default=None: (
        row[k] if k in row.keys() else default
    )
    drake = (get("drake_status_raw") or "").strip()
    if drake == "EF Rejected":
        return "ef_rejected"
    status = (get("client_status") or "").strip().upper()
    contact = (get("contact_status") or "").strip()
    if status == "REJECTED" and contact in ("", "not_contacted", "follow_up_needed"):
        return "client_contact"
    if status == "PROCESSING":
        intake = get("intake_date")
        display = get("display_name")
        if display and intake:
            intake_dt = _parse_iso_date(intake)
            if intake_dt and intake_dt < (date.today() - timedelta(days=60)):
                if drake not in _NEEDS_ATTENTION_DRAKE_DONE:
                    return "stale_processing"
    return None


def fetch_needs_attention(
    conn,
    tax_year: int,
    *,
    limit: int | None = _NEEDS_ATTENTION_LIST_CAP,
) -> tuple[list[dict], int]:
    """Active-year returns that need cross-staff attention.

    Reasons (deduped; highest priority wins): EF Rejected, REJECTED needing
    client contact, stale PROCESSING (named client, intake older than 60 days,
    Drake not accepted/printed).

    Returns ``(items, total_count)``. Items are capped by ``limit`` (None = uncapped).
    Never includes file_path / SSN fields.
    """
    from flask import has_request_context

    cutoff = (date.today() - timedelta(days=60)).isoformat()
    drake_done = tuple(_NEEDS_ATTENTION_DRAKE_DONE)
    placeholders = ",".join("?" * len(drake_done))
    rows = conn.execute(
        f"""
        SELECT
            r.id AS return_id,
            r.log_number,
            r.tax_year,
            r.client_status,
            r.processor,
            r.intake_date,
            r.drake_status_raw,
            r.contact_status,
            c.id AS client_id,
            c.display_name,
            c.last_name,
            c.first_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        WHERE r.tax_year = ?
          AND (
            r.drake_status_raw = 'EF Rejected'
            OR (
              r.client_status = 'REJECTED'
              AND (
                r.contact_status IS NULL
                OR r.contact_status = ''
                OR r.contact_status IN ('not_contacted', 'follow_up_needed')
              )
            )
            OR (
              r.client_status = 'PROCESSING'
              AND c.display_name IS NOT NULL
              AND r.intake_date IS NOT NULL
              AND r.intake_date < ?
              AND (
                r.drake_status_raw IS NULL
                OR r.drake_status_raw NOT IN ({placeholders})
              )
            )
          )
        """,
        (tax_year, cutoff, *drake_done),
    ).fetchall()

    by_id: dict[int, dict] = {}
    for row in rows:
        d = dict(row)
        reason = _needs_attention_reason(d)
        if not reason:
            continue
        rid = int(d["return_id"])
        existing = by_id.get(rid)
        if existing is None or (
            _NEEDS_ATTENTION_REASON_PRIORITY[reason]
            < _NEEDS_ATTENTION_REASON_PRIORITY[existing["reason"]]
        ):
            item = {
                "return_id": d["return_id"],
                "log_number": d["log_number"],
                "tax_year": d["tax_year"],
                "client_status": d["client_status"],
                "processor": d["processor"],
                "intake_date": d["intake_date"],
                "client_id": d["client_id"],
                "display_name": d["display_name"],
                "last_name": d["last_name"],
                "first_name": d["first_name"],
                "reason": reason,
            }
            if has_request_context() and privacy_mode_enabled():
                item = _mask_return_payload(item)
            by_id[rid] = item

    ordered = sorted(
        by_id.values(),
        key=lambda it: (
            _NEEDS_ATTENTION_REASON_PRIORITY.get(it["reason"], 99),
            it.get("intake_date") or "9999-99-99",
            it.get("return_id") or 0,
        ),
    )
    total = len(ordered)
    if limit is not None:
        ordered = ordered[: max(0, int(limit))]
    return ordered, total
def _form_badges(r: dict) -> list[str]:
    mapping = [
        ("form_1040",    "1040"),
        ("sched_a_d",    "A&D"),
        ("sched_c",      "SCH C"),
        ("sched_e",      "SCH E"),
        ("form_1120",    "1120"),
        ("form_1120s",   "1120S"),
        ("form_1065_llc","1065"),
        ("corp_officer", "CORP"),
        ("business_owner","BUS"),
        ("form_990_1041","990"),
    ]
    badges = [label for field, label in mapping if r.get(field)]
    if r.get("is_amended"):   badges.append("1040X")
    if r.get("has_w7"):       badges.append("W7")
    if r.get("is_extension"): badges.append("EXT")
    if r.get("transfer_flag") or r.get("transfer_2025_flag") or r.get("transfer_2026_flag"):
        badges.append("XFER")
    return badges


def _parse_iso_date(value: str | None):
    if not value:
        return None
    try:
        return datetime.strptime(value[:10], "%Y-%m-%d").date()
    except ValueError:
        return None


def query_returns(filters: dict | None = None) -> list[dict]:
    conn = get_connection()  # REL-4: closed in finally below
    f = filters or {}
    clauses: list[str] = []
    params:  list      = []

    # "year" here is the INTAKE/SEASON year (e.g. 2026 = the 2025-2026 filing season).
    # A return belongs to season Y if it was brought in during calendar year Y,
    # OR if it has no intake date but its tax_year = Y-1 (Drake-imported TY2025 records).
    year = f.get("year") or date.today().year
    clauses.append(
        "(strftime('%Y', r.intake_date) = ? OR "
        "(r.intake_date IS NULL AND r.tax_year = ?))"
    )
    params.append(str(year))
    params.append(year - 1)

    if f.get("status"):
        statuses = f["status"] if isinstance(f["status"], list) else [f["status"]]
        statuses = [s for s in statuses if s]
        if statuses:
            clauses.append(f"r.client_status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)

    if f.get("processor"):
        pvals = preparer_filter_match_values(f["processor"])
        if pvals:
            clauses.append("r.processor IN (" + ",".join("?" for _ in pvals) + ")")
            params.extend(pvals)

    if f.get("balance_due"):
        clauses.append(
            "(p.total_fee IS NOT NULL AND COALESCE(p.fee_paid,0) < p.total_fee)"
        )
    if f.get("late_intake"):
        clauses.append(
            "(r.intake_date IS NOT NULL AND ("
            "CAST(substr(r.intake_date,6,2) AS INTEGER) > 4 OR "
            "(CAST(substr(r.intake_date,6,2) AS INTEGER) = 4 AND CAST(substr(r.intake_date,9,2) AS INTEGER) >= 1)"
            "))"
        )
    if f.get("slow_cycle"):
        clauses.append(
            "(r.intake_date IS NOT NULL AND "
            "(r.logout_date IS NOT NULL OR r.ack_date IS NOT NULL) AND "
            "(julianday(COALESCE(r.logout_date, r.ack_date)) - julianday(r.intake_date)) >= ?)"
        )
        params.append(SLOW_CYCLE_DAYS)

    if f.get("form"):
        form_col = f["form"]
        allowed = {
            "form_1040", "sched_a_d", "sched_c", "sched_e",
            "form_1120", "form_1120s", "form_1065_llc",
            "corp_officer", "business_owner", "form_990_1041",
            "is_amended", "has_w7", "is_extension",
        }
        if form_col in allowed:
            # is_amended/has_w7/is_extension live on returns; forms live on return_forms
            if form_col in ("is_amended", "has_w7", "is_extension"):
                clauses.append(f"r.{form_col} = 1")
            else:
                clauses.append(f"rf.{form_col} = 1")

    if f.get("reject_contact"):
        st_raw = f.get("status")
        st_list = st_raw if isinstance(st_raw, list) else ([st_raw] if st_raw else [])
        if st_list and "REJECTED" not in st_list:
            pass
        else:
            rc = (f["reject_contact"] or "").strip().lower()
            clauses.append("r.client_status = 'REJECTED'")
            if rc == "needs_followup":
                clauses.append(
                    "(r.contact_status IS NULL OR r.contact_status = '' OR "
                    "r.contact_status IN ('not_contacted','follow_up_needed'))"
                )
            elif rc in CONTACT_STATUS_VALUES:
                if rc == "not_contacted":
                    clauses.append(
                        "(r.contact_status IS NULL OR r.contact_status = '' OR r.contact_status = 'not_contacted')"
                    )
                else:
                    clauses.append("r.contact_status = ?")
                    params.append(rc)

    if f.get("scan_deferred"):
        clauses.append("COALESCE(r.scan_deferred, 0) = 1")

    if f.get("q"):
        q = f["q"].strip()
        if q.isdigit():
            clauses.append("r.log_number = ?")
            params.append(q)
        else:
            qp = f"%{q.lower()}%"
            clauses.append(
                "(lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ? OR lower(COALESCE(c.display_name,'')) LIKE ?)"
            )
            params.extend([qp, qp, qp])

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""
    order = "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END, CAST(r.log_number AS INTEGER), r.id"
    sql   = f"{_SELECT} {where} {order}"

    try:
        rows = conn.execute(sql, params).fetchall()
        return [_enrich(dict(r)) for r in rows]
    finally:
        conn.close()


def query_returns_paginated(filters: dict | None = None, *, page: int = 1, per_page: int = 50) -> tuple[list[dict], int]:
    """Return (rows_for_page, total_count) for dashboard pagination.

    Runs two queries: a COUNT and a paginated SELECT.  Both share the same
    WHERE clause built from *filters*, so the count always reflects the full
    matching set regardless of the current page.
    """
    conn = get_connection()
    f = filters or {}
    clauses: list[str] = []
    params:  list      = []

    year = f.get("year") or date.today().year
    clauses.append(
        "(strftime('%Y', r.intake_date) = ? OR "
        "(r.intake_date IS NULL AND r.tax_year = ?))"
    )
    params.append(str(year))
    params.append(year - 1)

    if f.get("status"):
        statuses = f["status"] if isinstance(f["status"], list) else [f["status"]]
        statuses = [s for s in statuses if s]
        if statuses:
            clauses.append(f"r.client_status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)

    if f.get("processor"):
        pvals = preparer_filter_match_values(f["processor"])
        if pvals:
            clauses.append("r.processor IN (" + ",".join("?" for _ in pvals) + ")")
            params.extend(pvals)

    if f.get("balance_due"):
        clauses.append(
            "(p.total_fee IS NOT NULL AND COALESCE(p.fee_paid,0) < p.total_fee)"
        )
    if f.get("late_intake"):
        clauses.append(
            "(r.intake_date IS NOT NULL AND ("
            "CAST(substr(r.intake_date,6,2) AS INTEGER) > 4 OR "
            "(CAST(substr(r.intake_date,6,2) AS INTEGER) = 4 AND CAST(substr(r.intake_date,9,2) AS INTEGER) >= 1)"
            "))"
        )
    if f.get("slow_cycle"):
        clauses.append(
            "(r.intake_date IS NOT NULL AND "
            "(r.logout_date IS NOT NULL OR r.ack_date IS NOT NULL) AND "
            "(julianday(COALESCE(r.logout_date, r.ack_date)) - julianday(r.intake_date)) >= ?)"
        )
        params.append(SLOW_CYCLE_DAYS)

    if f.get("form"):
        form_col = f["form"]
        allowed = {
            "form_1040", "sched_a_d", "sched_c", "sched_e",
            "form_1120", "form_1120s", "form_1065_llc",
            "corp_officer", "business_owner", "form_990_1041",
            "is_amended", "has_w7", "is_extension",
        }
        if form_col in allowed:
            if form_col in ("is_amended", "has_w7", "is_extension"):
                clauses.append(f"r.{form_col} = 1")
            else:
                clauses.append(f"rf.{form_col} = 1")

    if f.get("reject_contact"):
        st_raw = f.get("status")
        st_list = st_raw if isinstance(st_raw, list) else ([st_raw] if st_raw else [])
        if not (st_list and "REJECTED" not in st_list):
            rc = (f["reject_contact"] or "").strip().lower()
            clauses.append("r.client_status = 'REJECTED'")
            if rc == "needs_followup":
                clauses.append(
                    "(r.contact_status IS NULL OR r.contact_status = '' OR "
                    "r.contact_status IN ('not_contacted','follow_up_needed'))"
                )
            elif rc in CONTACT_STATUS_VALUES:
                if rc == "not_contacted":
                    clauses.append(
                        "(r.contact_status IS NULL OR r.contact_status = '' OR r.contact_status = 'not_contacted')"
                    )
                else:
                    clauses.append("r.contact_status = ?")
                    params.append(rc)

    if f.get("scan_deferred"):
        clauses.append("COALESCE(r.scan_deferred, 0) = 1")

    if f.get("q"):
        q = f["q"].strip()
        if q.isdigit():
            clauses.append("r.log_number = ?")
            params.append(q)
        else:
            qp = f"%{q.lower()}%"
            clauses.append(
                "(lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ? OR lower(COALESCE(c.display_name,'')) LIKE ?)"
            )
            params.extend([qp, qp, qp])

    where = ("WHERE " + " AND ".join(clauses)) if clauses else ""

    _sort = f.get("sort", "log_asc")
    if _sort == "log_desc":
        order = (
            "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END,"
            " CAST(r.log_number AS INTEGER) DESC, r.id DESC"
        )
    else:  # log_asc (default)
        order = (
            "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END,"
            " CAST(r.log_number AS INTEGER) ASC, r.id ASC"
        )

    per_page = min(100, max(1, int(per_page)))
    page     = max(1, int(page))
    offset   = (page - 1) * per_page

    # COUNT query — same WHERE, no ORDER/LIMIT
    count_sql = (
        "SELECT COUNT(*) n "
        "FROM returns r "
        "JOIN clients c ON c.id = r.client_id "
        "LEFT JOIN payments p ON p.return_id = r.id "
        "LEFT JOIN return_forms rf ON rf.return_id = r.id "
        f"{where}"
    )

    paginated_sql = f"{_SELECT} {where} {order} LIMIT ? OFFSET ?"

    try:
        total_count = conn.execute(count_sql, params).fetchone()["n"]
        rows = conn.execute(paginated_sql, params + [per_page, offset]).fetchall()
        return [_enrich(dict(r)) for r in rows], total_count
    finally:
        conn.close()


def get_one(return_id: int) -> dict | None:
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(f"{_SELECT} WHERE r.id = ?", (return_id,)).fetchone()
        return _enrich(dict(row)) if row else None


def batch_fetch_returns(return_ids: list[int]) -> dict[int, dict]:
    """DEBT-4: fetch multiple returns in ONE query keyed by return_id.

    Eliminates the N+1 pattern where callers loop over return_ids calling get_one()
    individually.  Returns a mapping {return_id: enriched_dict}; missing ids are absent.
    """
    if not return_ids:
        return {}
    placeholders = ",".join("?" * len(return_ids))
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            f"{_SELECT} WHERE r.id IN ({placeholders})", list(return_ids)
        ).fetchall()
    return {r["id"]: _enrich(dict(r)) for r in rows}


def _fetch_returns_for_client(conn: sqlite3.Connection, client_id: int) -> list[dict]:
    rows = conn.execute(
        f"{_SELECT} WHERE r.client_id = ? ORDER BY r.tax_year DESC, r.id DESC",
        (client_id,),
    ).fetchall()
    return [_enrich(dict(r)) for r in rows]


def _fetch_client_documents(conn: sqlite3.Connection, client_id: int) -> list[dict]:
    rows = conn.execute(
        """
        SELECT rd.id, rd.return_id, rd.filename, rd.original_filename,
               rd.doc_type, rd.uploaded_at, rd.uploaded_by,
               r.tax_year, r.log_number
          FROM return_documents rd
          JOIN returns r ON r.id = rd.return_id
         WHERE r.client_id = ?
           AND IFNULL(rd.is_deleted, 0) = 0
         ORDER BY rd.uploaded_at IS NULL ASC, rd.uploaded_at DESC, rd.id DESC
        """,
        (client_id,),
    ).fetchall()
    return [dict(r) for r in rows]


def _fetch_client_activity(conn: sqlite3.Connection, client_id: int, limit: int = 150) -> list[dict]:
    notes = conn.execute(
        """
        SELECT n.note_text, n.source, n.created_at, n.return_id,
               r.tax_year, r.log_number
          FROM notes n
          JOIN returns r ON r.id = n.return_id
         WHERE r.client_id = ?
        """,
        (client_id,),
    ).fetchall()
    events = conn.execute(
        """
        SELECT e.event_type, e.old_status, e.new_status, e.event_timestamp,
               e.source_file, e.note, e.return_id,
               r.tax_year, r.log_number
          FROM status_events e
          JOIN returns r ON r.id = e.return_id
         WHERE r.client_id = ?
        """,
        (client_id,),
    ).fetchall()
    paired: list[tuple[str, str, dict]] = []
    for n in notes:
        paired.append(((n["created_at"] or ""), "note", dict(n)))
    for e in events:
        paired.append(((e["event_timestamp"] or ""), "event", dict(e)))
    paired.sort(key=lambda x: x[0], reverse=True)

    out: list[dict] = []
    for ts, kind, row in paired[:limit]:
        if kind == "note":
            txt = row.get("note_text") or ""
            if privacy_mode_enabled():
                txt = _mask_value(txt)
            ln = row.get("log_number")
            ty = row.get("tax_year")
            lbl = "Note · LOG " + (str(ln) if ln else "—")
            if ty is not None:
                lbl += f" · TY{ty}"
            out.append({
                "kind":      "note",
                "at":        ts or "—",
                "title":     lbl,
                "body":      txt,
                "return_id": row.get("return_id"),
                "source":    row.get("source"),
            })
        else:
            et = row.get("event_type") or "event"
            old_s, new_s = row.get("old_status"), row.get("new_status")
            if et == "STATUS_CHANGED":
                summary = (
                    ("Status · " + (str(old_s) if old_s else "—") + " → " + str(new_s))
                    if new_s or old_s
                    else et
                )
            else:
                fragment = ": " + (row.get("note") or "") if row.get("note") else ""
                summary = et + fragment
            ln = row.get("log_number")
            ty = row.get("tax_year")
            subtitle = "LOG " + (str(ln) if ln else "—")
            if ty is not None:
                subtitle += f" · TY{ty}"
            out.append({
                "kind":       "event",
                "at":         ts or "—",
                "title":      summary,
                "body":       (row.get("note") or row.get("source_file") or "") or None,
                "return_id":  row.get("return_id"),
                "subtitle":   subtitle,
                "new_status": new_s,
            })
    return out


def get_status_counts(year: int) -> dict[str, int]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT client_status, COUNT(*) n FROM returns "
        "WHERE (strftime('%Y', intake_date) = ? OR (intake_date IS NULL AND tax_year = ?)) "
        "GROUP BY client_status",
        (str(year), year - 1),
    ).fetchall()
    conn.close()
    return {r["client_status"]: r["n"] for r in rows if r["client_status"]}


def get_totals(year: int) -> dict:
    conn = get_connection()
    row = conn.execute(
        """
        SELECT
            COALESCE(SUM(p.total_fee),0) billed,
            COALESCE(SUM(p.fee_paid),0)  collected
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?))
          AND UPPER(COALESCE(r.client_status,'')) != 'CANCELLED'
        """,
        (str(year), year - 1),
    ).fetchone()
    conn.close()
    billed    = row["billed"]    if row else 0
    collected = row["collected"] if row else 0
    return {"billed": billed, "collected": collected, "outstanding": round(billed - collected, 2)}


def get_processors(year: int) -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT processor FROM returns "
        "WHERE (strftime('%Y', intake_date) = ? OR (intake_date IS NULL AND tax_year = ?)) "
        "AND processor IS NOT NULL ORDER BY processor",
        (str(year), year - 1),
    ).fetchall()
    conn.close()
    return [r["processor"] for r in rows]


def _session_username() -> str | None:
    u = (session.get("username") or "").strip()
    return u or None


def _resolve_current_role() -> str:
    """Return the current user's role.

    Prefer the session value (set on login with the new code). Fall back to
    a DB lookup so that sessions created before ONBOARD-3 still get the
    correct role without requiring a log-out / log-in cycle.  Env-var
    fallback accounts are treated as admin.
    """
    if session.get("role"):
        return session["role"]
    username = _session_username()
    if not username:
        return "staff"
    # Env-var fallback user has no DB row — treat as admin.
    _tmp_conn = get_connection()
    _no_db_users = not _auth_users_exist(_tmp_conn)
    _tmp_conn.close()
    if username == (_LOGIN_USER or "").strip().lower() and _no_db_users:
        return "admin"
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT role FROM auth_users WHERE username = ? AND is_active = 1 LIMIT 1",
                (username,),
            ).fetchone()
        finally:
            conn.close()
        role = (row["role"] if row else None) or "staff"
        session["role"] = role  # cache for subsequent requests
        return role
    except Exception:
        return "staff"


def _season_rollover_admins() -> frozenset[str]:
    raw = (os.environ.get("TAXOPS_ROLLOVER_ADMINS") or "").strip().lower()
    if raw:
        return frozenset(p.strip().lower() for p in raw.split(",") if p.strip())
    lu = (_LOGIN_USER or "").strip().lower()
    return frozenset({lu}) if lu else frozenset()


def can_run_season_rollover() -> bool:
    u = (_session_username() or "").lower()
    return bool(u) and u in _season_rollover_admins()


def base_ctx(year: int | None = None) -> dict:
    today_year = date.today().year
    # Never let the season picker go backwards to a tax year.
    # Callers should pass the intake/season year, not the tax year.
    y = year if (year and year >= today_year) else today_year
    conn = get_connection()
    pending_review = conn.execute(
        "SELECT COUNT(*) n FROM review_queue WHERE status='pending'"
    ).fetchone()["n"]
    # DOC-HARD-3: count permanently failed (dead-letter) extractions for the nav badge.
    try:
        failed_doc_count = conn.execute(
            "SELECT COUNT(*) n FROM extraction_queue WHERE status = 'failed'"
        ).fetchone()["n"]
    except Exception:
        failed_doc_count = 0
    # ACCOUNTING-9: count receipts awaiting staff review for the nav badge.
    try:
        receipt_review_count = conn.execute(
            "SELECT COUNT(*) n FROM receipt_queue WHERE status = 'review'"
        ).fetchone()["n"]
    except Exception:
        receipt_review_count = 0
    # Email inbox — count unassigned attachments for the nav badge.
    try:
        unassigned_email_count = conn.execute(
            "SELECT COUNT(*) n FROM email_inbox WHERE is_assigned=0 AND is_deleted=0"
        ).fetchone()["n"]
    except Exception:
        unassigned_email_count = 0
    # Intake scan deferred — returns where staff skipped "scan now".
    try:
        scan_deferred_count = conn.execute(
            "SELECT COUNT(*) n FROM returns WHERE scan_deferred = 1"
        ).fetchone()["n"]
    except Exception:
        scan_deferred_count = 0
    # DEP-IMPORT: count dependents pending match review for the nav badge.
    try:
        dep_review_count = conn.execute(
            "SELECT COUNT(*) n FROM client_dependents WHERE needs_review=1 AND removed_for_ty2026=0"
        ).fetchone()["n"]
    except Exception:
        dep_review_count = 0
    # SPOUSE-IMPORT: count spouses pending review for the nav badge.
    try:
        spouse_review_count = conn.execute(
            "SELECT COUNT(*) n FROM spouses WHERE needs_review=1"
        ).fetchone()["n"]
    except Exception:
        spouse_review_count = 0
    try:
        recovered_client_count = conn.execute(
            "SELECT COUNT(*) n FROM clients WHERE last_name='[RECOVERED]'"
        ).fetchone()["n"]
    except Exception:
        recovered_client_count = 0
    try:
        audit_alert_count = conn.execute(
            """SELECT COUNT(*) n FROM returns r
               WHERE r.drake_status_raw = 'EF Rejected'
                  OR (r.notes_intake LIKE '%MISLINKED AUDIT FLAG%')
                  OR (r.client_status = 'PROCESSING'
                      AND r.intake_date IS NOT NULL
                      AND r.intake_date < date('now', '-60 days')
                      AND (r.drake_status_raw IS NULL
                           OR r.drake_status_raw NOT IN (
                              'EF Accepted','EF Ext Accepted','E-Filed: YES','Printed')))"""
        ).fetchone()["n"]
    except Exception:
        audit_alert_count = 0
    # WO-7: per-user unread in-app notification count for the nav bell (e.g.
    # "you've been assigned a new work order"). Unlike every other badge
    # above, this is scoped to session["username"], not a global queue size.
    try:
        _current_username = session.get("username")
        if _current_username:
            my_unread_notification_count = conn.execute(
                """
                SELECT COUNT(*) n FROM notifications nf
                JOIN auth_users u ON u.id = nf.user_id
                WHERE u.username = ? AND nf.is_read = 0
                """,
                (_current_username,),
            ).fetchone()["n"]
        else:
            my_unread_notification_count = 0
    except Exception:
        my_unread_notification_count = 0
    # Rejected returns — always pulled regardless of season filter
    rejected_rows = conn.execute(
        f"{_SELECT} WHERE r.client_status = 'REJECTED' ORDER BY r.updated_at DESC"
    ).fetchall()
    try:
        needs_attention_items, needs_attention_count = fetch_needs_attention(conn, y)
    except Exception:
        needs_attention_items, needs_attention_count = [], 0
    conn.close()
    rejected = [_enrich(dict(r)) for r in rejected_rows]
    return {
        "current_year":         y,
        "status_flow":          STATUS_FLOW,
        "status_badge":         STATUS_BADGE,
        "status_dot":           STATUS_DOT,
        "receptionist_status_always": [s for s in STATUS_FLOW if s in RECEPTIONIST_STATUS_ALWAYS],
        "receptionist_status_from": {
            k: [s for s in STATUS_FLOW if s in v]
            for k, v in RECEPTIONIST_STATUS_FROM.items()
        },
        "status_counts":        get_status_counts(y),
        "totals":               get_totals(y),
        "processors":           preparer_dropdown_options(get_processors(y)),
        "app_env":              APP_ENV,
        "privacy_mode":         privacy_mode_enabled(),
        "pending_review_count": pending_review,
        "rejected_returns":     rejected,
        "rejected_count":       len(rejected),
        "needs_attention_items": needs_attention_items,
        "needs_attention_count": needs_attention_count,
        "can_run_season_rollover": can_run_season_rollover(),
        "failed_doc_count":         failed_doc_count,
        "receipt_review_count":     receipt_review_count,
        "unassigned_email_count":   unassigned_email_count,
        "scan_deferred_count":      scan_deferred_count,
        "dep_review_count":         dep_review_count,
        "spouse_review_count":      spouse_review_count,
        "recovered_client_count":   recovered_client_count,
        "audit_alert_count":        audit_alert_count,
        "my_unread_notification_count": my_unread_notification_count,
        # ONBOARD-3: current user info for nav display.
        # Fall back to DB lookup so sessions created before role was stored still work.
        "current_user_name":    session.get("display_name") or session.get("username"),
        "current_user_role":    get_effective_role(),
        "app_name":             APP_NAME,
        "dev_console":          app.debug,
        "interviewers":         INTERVIEWERS,
    }


def build_client_habit_profile(conn, client_id: int, target_year: int | None = None) -> dict:
    """
    Build planning reminders from prior-year filing behavior.
    This is intentionally heuristic so staff can anticipate complexity
    while still re-confirming items that tend to change year to year.
    """
    rows = conn.execute(
        """
        SELECT
            r.id, r.tax_year, r.intake_date, r.logout_date, r.is_extension, r.has_w7,
            rf.sched_c, rf.sched_e, rf.form_1120, rf.form_1120s,
            rf.form_1065_llc, rf.business_owner, rf.corp_officer,
            r.insurance_type
        FROM returns r
        LEFT JOIN return_forms rf ON rf.return_id = r.id
        WHERE r.client_id = ?
        ORDER BY r.tax_year DESC, r.id DESC
        """,
        (client_id,),
    ).fetchall()

    if not rows:
        return {
            "target_year": target_year or date.today().year,
            "risk_level": "standard",
            "late_filer": False,
            "late_years": [],
            "recurring_forms": [],
            "ask_again": [],
            "reminders": [],
        }

    effective_target_year = target_year or date.today().year
    prior_rows = [r for r in rows if (r["tax_year"] or 0) < effective_target_year] or rows

    def _is_late_intake(intake_date: str | None) -> bool:
        if not intake_date or len(intake_date) < 7:
            return False
        try:
            month = int(intake_date[5:7])
            day = int(intake_date[8:10]) if len(intake_date) >= 10 else 1
            return month >= 4 or (month == 3 and day >= 25)
        except ValueError:
            return False

    late_years = sorted(
        [r["tax_year"] for r in prior_rows if r["tax_year"] and _is_late_intake(r["intake_date"])],
        reverse=True,
    )
    slow_cycle_years: list[int] = []
    for r in prior_rows:
        start = _parse_iso_date(r["intake_date"])
        end = _parse_iso_date(r["logout_date"])
        if start and end and end >= start and (end - start).days >= SLOW_CYCLE_DAYS and r["tax_year"]:
            slow_cycle_years.append(r["tax_year"])
    slow_cycle_years = sorted(set(slow_cycle_years), reverse=True)

    recurring_forms: list[str] = []
    form_rules = [
        ("sched_c", "Schedule C"),
        ("sched_e", "Schedule E"),
        ("form_1065_llc", "K-1/1065 partnership"),
        ("form_1120", "1120 corporate"),
        ("form_1120s", "1120S"),
        ("business_owner", "Business owner"),
        ("corp_officer", "Corporate officer"),
        ("is_extension", "Extension filing"),
        ("has_w7", "W-7 / ITIN"),
    ]
    for key, label in form_rules:
        if any(r[key] for r in prior_rows):
            recurring_forms.append(label)

    ask_again: list[str] = []
    had_marketplace = any(
        (r["insurance_type"] or "").strip().lower().startswith("marketplace")
        for r in prior_rows
    )
    if had_marketplace:
        ask_again.append("Confirm current 1095-A / Marketplace coverage for this year")
    if any((r["insurance_type"] or "").strip().lower() in {"medi-cal", "medicare"} for r in prior_rows):
        ask_again.append("Reconfirm current Medi-Cal/Medicare status (can change year to year)")

    reminders: list[str] = []
    if late_years:
        years = ", ".join(str(y) for y in late_years[:3])
        reminders.append(
            f"Historically filed late ({years}). Trigger early outreach before March."
        )
    if recurring_forms:
        reminders.append(
            "Prior complexity detected: " + ", ".join(recurring_forms[:5]) +
            (", ..." if len(recurring_forms) > 5 else "")
        )
    if slow_cycle_years:
        years = ", ".join(str(y) for y in slow_cycle_years[:3])
        reminders.append(
            f"Historically long turnaround ({years}). Ask for missing docs at intake to avoid delays."
        )
    reminders.extend(ask_again)

    risk_level = "high" if (len(late_years) >= 2 or len(recurring_forms) >= 3 or len(slow_cycle_years) >= 2) else "watch"
    if not late_years and len(recurring_forms) <= 1:
        risk_level = "standard"

    return {
        "target_year": effective_target_year,
        "risk_level": risk_level,
        "late_filer": bool(late_years),
        "late_years": late_years,
        "slow_cycle_years": slow_cycle_years,
        "recurring_forms": recurring_forms,
        "ask_again": ask_again,
        "reminders": reminders,
    }


# ── Auth routes ───────────────────────────────────────────────────────────────

@app.route("/login", methods=["GET", "POST"])
def login():
    error = None
    if request.method == "POST":
        username = (request.form.get("username") or "").strip()
        password = request.form.get("password") or ""
        user = _authenticate_user(username, password)
        if user is _AUTH_LOCKED:
            # SEC-7: constant-time failure — sleep before returning so a timing oracle
            # cannot distinguish a lockout response from a bcrypt verification.
            time.sleep(0.2)
            error = "Invalid username or password."
        elif user:
            session.permanent = True  # SEC-3: enforce PERMANENT_SESSION_LIFETIME (12 h)
            session["logged_in"]     = True
            session["username"]      = user["username"]
            session["role"]          = user.get("role", "staff")
            session["display_name"]  = user.get("display_name") or user["username"]
            # ONBOARD-2: check if user must change their temporary password
            _mcp = user.get("must_change_password", 0)
            if _mcp:
                session["must_change_password"] = True
                return redirect(url_for("change_password"))
            session.pop("must_change_password", None)
            next_url = request.args.get("next") or url_for("dashboard")
            return redirect(next_url)
        else:
            # SEC-7: constant-time failure — sleep before returning so a timing oracle
            # cannot distinguish an unknown-user response from a bcrypt verification.
            time.sleep(0.2)
            error = "Invalid username or password."
    return render_template("login.html", error=error, app_name=APP_NAME)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── ONBOARD-2: Forced password change ────────────────────────────────────────

@app.route("/change-password", methods=["GET", "POST"])
def change_password():
    """ONBOARD-2: staff with must_change_password=1 are redirected here after login."""
    if not session.get("logged_in"):
        return redirect(url_for("login"))
    if request.method == "GET":
        return render_template("change_password.html")
    new_pw = request.form.get("new_password") or ""
    confirm_pw = request.form.get("confirm_password") or ""
    if len(new_pw) < 8:
        return render_template("change_password.html", error=_t("Password must be at least 8 characters."))
    if new_pw != confirm_pw:
        return render_template("change_password.html", error=_t("Passwords do not match."))
    username = session.get("username")
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, password_hash FROM auth_users WHERE username = ? AND is_active = 1",
            (username,),
        ).fetchone()
        if not row:
            return render_template("change_password.html", error=_t("Account not found."))
        from werkzeug.security import check_password_hash as _chk, generate_password_hash as _gen
        if _chk(row["password_hash"], new_pw):
            return render_template("change_password.html", error=_t("New password must be different from the temporary password."))
        conn.execute(
            "UPDATE auth_users SET password_hash = ?, must_change_password = 0, failed_attempts = 0 WHERE id = ?",
            (_gen(new_pw), row["id"]),
        )
        conn.commit()
    finally:
        conn.close()
    session.pop("must_change_password", None)
    # ONBOARD-4: route to orientation if staff hasn't seen it yet
    conn2 = get_connection()
    try:
        orow = conn2.execute(
            "SELECT has_seen_orientation FROM auth_users WHERE username = ?", (username,)
        ).fetchone()
        if orow and not orow["has_seen_orientation"]:
            return redirect(url_for("orientation"))
    finally:
        conn2.close()
    return redirect(url_for("dashboard"))


# ── ONBOARD-4: First-login orientation ───────────────────────────────────────

@app.route("/orientation")
@login_required
def orientation():
    username = session.get("username")
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT has_seen_orientation FROM auth_users WHERE username = ?", (username,)
        ).fetchone()
        if row and row["has_seen_orientation"]:
            return redirect(url_for("dashboard"))
    finally:
        conn.close()
    return render_template("orientation.html")


@app.post("/orientation/dismiss")
@login_required
def orientation_dismiss():
    username = session.get("username")
    conn = get_connection()
    try:
        conn.execute(
            "UPDATE auth_users SET has_seen_orientation = 1 WHERE username = ?", (username,)
        )
        conn.commit()
    finally:
        conn.close()
    return redirect(url_for("dashboard"))


# ── I18N-1: locale context processor + language toggle ───────────────────────

@app.context_processor
def _inject_locale():
    """Make current_locale available in every template."""
    return {"current_locale": _get_locale()}


@app.post("/set-language")
@login_required
def set_language():
    """I18N-1: switch the session locale. Accepts JSON {locale: 'es'|'en'}."""
    data = request.get_json(silent=True) or {}
    raw = (data.get("locale") or "").strip()
    # Normalize to canonical form (accept es_mx or es_MX)
    _LOCALE_ALIAS = {"en": "en", "es_mx": "es_MX", "es_MX": "es_MX"}
    locale = _LOCALE_ALIAS.get(raw, "")
    if not locale:
        return jsonify({"error": "Unsupported locale. Supported: en, es_MX"}), 400
    session["locale"] = locale
    return jsonify({"success": True, "locale": locale})


# ── TOUR-3: Tour state API ────────────────────────────────────────────────────

def _tour_key_for_user(username: str) -> str | None:
    """Return the app_settings key for the user's tour completion, or None if user not found."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM auth_users WHERE username = ? LIMIT 1", (username,)
        ).fetchone()
        if row:
            return f"tour_completed_{row['id']}"
        # env-var fallback user has no DB row — use username directly
        return f"tour_completed_env_{username}"
    finally:
        conn.close()


@app.get("/api/tour/status")
@login_required
def api_tour_status():
    """TOUR-3: return {completed: bool} for the current user."""
    key = _tour_key_for_user(session.get("username") or "")
    if not key:
        return jsonify({"completed": False})
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?", (key,)
        ).fetchone()
        return jsonify({"completed": bool(row)})
    finally:
        conn.close()


@app.post("/api/tour/complete")
@login_required
def api_tour_complete():
    """TOUR-3: mark the tour as completed for the current user."""
    key = _tour_key_for_user(session.get("username") or "")
    if not key:
        return jsonify({"success": False, "error": "user not found"}), 400
    conn = get_connection()
    try:
        conn.execute(
            "INSERT OR REPLACE INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)",
            (key, now(), now()),
        )
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.post("/api/tour/reset")
@login_required
def api_tour_reset():
    """TOUR-3: self-serve reset — lets any user replay their own tour."""
    key = _tour_key_for_user(session.get("username") or "")
    if not key:
        return jsonify({"success": False, "error": "user not found"}), 400
    conn = get_connection()
    try:
        conn.execute("DELETE FROM app_settings WHERE key = ?", (key,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.get("/api/tour/first-return")
@login_required
def api_tour_first_return():
    """TOUR-4: Return the URL of the most-recently-updated return for tour navigation.
    Used by the JS tour so it can navigate to a real return without scraping the DOM."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM returns ORDER BY updated_at DESC, id DESC LIMIT 1"
        ).fetchone()
        if row:
            return jsonify({"url": f"/return/{row['id']}"})
        return jsonify({"url": None})
    finally:
        conn.close()


@app.get("/api/translations")
def api_translations():
    """I18N-4: JS-side translatable strings for the current session locale.

    Not gated by @login_required so the page can fetch it before session checks.
    Returns a JSON object keyed by stable English keys.
    """
    strings = {
        "loading":              _t("Loading..."),
        "saving":               _t("Saving..."),
        "uploading":            _t("Uploading..."),
        "confirm_delete":       _t("Are you sure you want to delete this?"),
        "no_results":           _t("No results"),
        "error_generic":        _t("Something went wrong. Please try again."),
        "upload_success":       _t("Document uploaded successfully."),
        "upload_error":         _t("Upload failed. Please try again."),
        "doc_deleted":          _t("Document deleted."),
        "classification_saved": _t("Classification saved."),
        "needs_manual_tag":     _t("Needs manual tag"),
        "session_expired":      _t("Your session has expired. Please sign in again."),
        # Tour step titles and bodies
        # UI chrome strings used in modal buttons
        "tour_label":           _t("Guided Tour"),
        "tour_skip":            _t("Skip tour"),
        "tour_begin":           _t("Start tour"),
        "tour_present":         _t("Present"),
        "tour_back":            _t("Back"),
        "tour_next":            _t("Next"),
        # Step 0 — welcome modal
        "tour_modal_title":     _t("Welcome to TaxOps"),
        "tour_modal_subtitle":  _t("A quick tour for your whole team"),
        "tour_modal_b1":        _t("Find any client in under 2 seconds"),
        "tour_modal_b2":        _t("Track every return from intake to pickup"),
        "tour_modal_b3":        _t("Manage documents, fees, missing items, and e-file"),
        "tour_modal_b4":        _t("Power features: bulk actions, saved views, receipt tracking"),
        # Step 1 — workflow overview modal
        "tour_wf_title":        _t("How a return flows through TaxOps"),
        "tour_wf_subtitle":     _t("One client visit, start to finish"),
        "tour_wf_s1_label":     _t("Intake"),
        "tour_wf_s1_body":      _t("Client walks in. Fill the intake form — personal info, documents, fees, and any missing items."),
        "tour_wf_s2_label":     _t("Collect documents"),
        "tour_wf_s2_body":      _t("Upload W-2s, 1099s, and everything else directly to the return. Tag each file so it stays findable."),
        "tour_wf_s3_label":     _t("Process"),
        "tour_wf_s3_body":      _t("Preparer opens Drake and works the return. Status stays PROCESSING until complete."),
        "tour_wf_s4_label":     _t("Finalize"),
        "tour_wf_s4_body":      _t("Done in Drake. Mark FINALIZE — signals ready for review or client signature."),
        "tour_wf_s5_label":     _t("Deliver"),
        "tour_wf_s5_body":      _t("Counter pickup → mark PICKUP. Electronic filing → mark EFILE READY to queue for transmission."),
        "tour_wf_s6_label":     _t("Close"),
        "tour_wf_s6_body":      _t("Client picks up their copy or the batch gets transmitted. Return is done."),
        # Steps 2–3 — dashboard search + tabs
        "tour_s1_title":        _t("Find any client instantly"),
        "tour_s1_body":         _t("Type a name or return number here. Results appear as you type. This is the fastest way to get to any client or return."),
        "tour_s2_title":        _t("Track where every return stands"),
        "tour_s2_body":         _t("These tabs filter by workflow status. PROCESSING means actively being worked. PICKUP means ready for the client. Click any tab to see only those returns."),
        # Steps 3–4 — dashboard unlabeled controls
        "tour_s_reject_bell_title": _t("IRS rejection alerts"),
        "tour_s_reject_bell_body":  _t("This bell lights up when any e-filed return has been rejected by the IRS. Open it to see which returns need correction — don't let a rejection sit."),
        "tour_s_alert_pills_title": _t("Problem filters"),
        "tour_s_alert_pills_body":  _t("These flag buttons filter for returns with active issues — balance due, late intake, or slow processing cycle. One click shows only those returns."),
        # Steps 5–6 — intake
        "tour_s_intake_title":  _t("Start a new engagement"),
        "tour_s_intake_body":   _t("The intake form is your TaxOps version of the paper client worksheet — personal info, document checklist, fees, and missing items, all in one place before Drake is ever opened."),
        "tour_s_reintake_title": _t("Re-intake a returning client"),
        "tour_s_reintake_body":  _t("For a returning client, search their name here before you fill anything in. TaxOps pre-fills what it already knows from last year — no re-entering the same info every season."),
        # Steps 5–7 — client profile
        "tour_s_client_link_title": _t("The client record"),
        "tour_s_client_link_body":  _t("Click here from any return to see the full client record — contact info, every return at a glance, and multi-year comparison."),
        "tour_s_client_edit_title": _t("Edit contact info"),
        "tour_s_client_edit_body":  _t("Update address, phone, email, and filing status here — changes apply to every return for this client, not just the latest year."),
        "tour_s_client_multi_title": _t("Multi-year comparison"),
        "tour_s_client_multi_body":  _t("Pick 2–3 tax years to compare income, deductions, and fees side-by-side. Useful for spotting year-over-year changes or walking a client through their history."),
        # Steps 8–12 — return detail
        "tour_s3_title":        _t("Every document in one place"),
        "tour_s3_body":         _t("W-2s, 1099s, and anything the client emails gets saved here automatically. You can also upload documents directly. Click any file to view it."),
        "tour_s_upload_title":  _t("Add documents to this return"),
        "tour_s_upload_body":   _t("Upload directly here instead of a shared drive folder — the file stays attached to this return. Use bulk upload to send a whole client packet at once."),
        "tour_s_missing_title": _t("Track what the client still owes you"),
        "tour_s_missing_body":  _t("Add missing items here instead of keeping a sticky note or mental list. Your whole team sees the same checklist and can check things off as the client brings them in."),
        "tour_s4_title":        _t("Change the return status"),
        "tour_s4_body":         _t("Use this control to move the return through the workflow — from PROCESSING to FINALIZE to PICKUP — as you work it."),
        "tour_s5_title":        _t("Keep your team in sync"),
        "tour_s5_body":         _t("Add notes visible to everyone on the team. Record what was discussed, what's outstanding, or anything the next person needs to know."),
        # Step 15 — return action sidebar
        "tour_s_action_nav_title": _t("Return shortcuts"),
        "tour_s_action_nav_body":  _t("Print a routing sticker, download the intake sheet as a PDF, or cancel the return from this panel. When a return reaches PICKUP or EFILE READY a direct action button also appears here."),
        # Steps 16–18 — dashboard power features
        "tour_s_bulk_title":    _t("Update many returns at once"),
        "tour_s_bulk_body":     _t("Check this box to select every visible return, then use the bulk bar that appears to change status or assign a preparer across all of them — no one-by-one clicking."),
        "tour_s_filters_title": _t("Save your daily view"),
        "tour_s_filters_body":  _t("If you filter the dashboard the same way every shift, save it here. Your saved views appear as quick-pick chips so you don't rebuild the same filters every time."),
        # Steps 12–13 — queues
        "tour_s_pickup_title":  _t("Pickup queue"),
        "tour_s_pickup_body":   _t("Everything flagged PICKUP lives here — your TaxOps version of the paper pickup log. Filter by name or log number to find a client fast."),
        "tour_s_efile_title":   _t("E-file queue"),
        "tour_s_efile_body":    _t("Returns ready to transmit show up here — the electronic version of pulling the e-file pile. Select returns and create a batch to send them all at once."),
        # Step 14 — receipts
        "tour_s_receipts_title": _t("Receipt tracking"),
        "tour_s_receipts_body":  _t("Upload receipts here instead of a separate spreadsheet. TaxOps reads them with OCR, suggests a category, and queues them for QuickBooks export once a staff member approves."),
        # Step 18 — export
        "tour_s_export_title":  _t("Export to Excel"),
        "tour_s_export_body":   _t("Downloads the full filtered set — whatever is visible in the table right now, including any active status or preparer filters. Good for end-of-day reports or handing a list to someone."),
        # Steps 22–23 — global UI
        "tour_s_privacy_title": _t("Hide sensitive info in one click"),
        "tour_s_privacy_body":  _t("Client at your desk or screen visible in the office? Toggle Privacy to mask names and numbers instantly — one click to hide, one more to restore."),
        "tour_s_year_picker_title": _t("Change the filing season"),
        "tour_s_year_picker_body":  _t("Switch tax years here and every return, stat, and export updates to match. Use it to check prior seasons or confirm nothing was left open."),
        # Step 24 — wrap-up / unused s6
        "tour_s6_title":        _t("Incoming client documents"),
        "tour_s6_body":         _t("When a client emails their documents they appear here. Review and confirm to attach them to the right return. The system matches clients automatically — you just verify."),
        "tour_s7_title":        _t("You are ready"),
        "tour_s7_body":         _t("That covers the essentials. You can relaunch this tour anytime from the help icon in the top navigation. If you have questions check the runbook or ask your admin."),
    }
    resp = jsonify(strings)
    # Short-lived cache is OK — locale rarely changes mid-session
    resp.headers["Cache-Control"] = "private, max-age=60"
    return resp


@app.get("/health")
def health():
    """PROD-3 — liveness/readiness probe: JSON status, SQLite check, process uptime, version."""
    db_ok = True
    db_detail: dict = {}
    t0 = time.perf_counter()
    try:
        conn = get_connection()
        try:
            conn.execute("SELECT 1").fetchone()
        finally:
            conn.close()
        db_detail = {"ok": True, "latency_ms": round((time.perf_counter() - t0) * 1000, 3)}
    except sqlite3.Error as ex:
        db_ok = False
        db_detail = {
            "ok": False,
            "latency_ms": round((time.perf_counter() - t0) * 1000, 3),
            "error": str(ex),
        }

    schema_ver: int | None = None
    fts5: dict | None = None
    if db_ok:
        try:
            _sv_conn = get_connection()
            try:
                schema_ver = get_schema_version(_sv_conn)
                fts_enabled = bool(
                    _sv_conn.execute(
                        "SELECT sqlite_compileoption_used('ENABLE_FTS5')"
                    ).fetchone()[0]
                )
                fts_table = (
                    _sv_conn.execute(
                        "SELECT 1 FROM sqlite_master WHERE type='table' "
                        "AND name='return_documents_fts'"
                    ).fetchone()
                    is not None
                )
                fts5 = {
                    "compile_option": bool(fts_enabled),
                    "return_documents_fts": fts_table,
                }
            finally:
                _sv_conn.close()
        except Exception:
            pass

    # DOC-HARD-1: include extraction queue depth so ops / monitoring can see pending docs.
    extraction_q: dict | None = None
    if db_ok:
        try:
            from extractor import extraction_queue_status_for_api
            extraction_q = extraction_queue_status_for_api()
        except Exception:
            pass

    # HEALTH-2/3: worker thread liveness
    workers: dict = {}
    try:
        from extractor import extraction_worker_status
        workers["extraction"] = extraction_worker_status()
    except Exception:
        workers["extraction"] = {"started": False, "running": False}
    try:
        from mail_watcher import mail_watcher_status
        workers["mail_watcher"] = mail_watcher_status()
    except Exception:
        workers["mail_watcher"] = {"started": False, "running": False}
    try:
        from accounting_worker import accounting_worker_status
        workers["accounting"] = accounting_worker_status()
    except Exception:
        pass  # accounting worker is optional

    body = {
        "status": "ok" if db_ok else "degraded",
        "db": db_detail,
        "uptime_seconds": round(time.monotonic() - _APP_START_MONOTONIC, 3),
        "version": taxops_release_version(),
        "audit_queue_depth": audit_queue_depth(),
        "schema_version": schema_ver,
        "schema_version_expected": CURRENT_SCHEMA_VERSION,
        "fts5": fts5,
        "extraction_queue": extraction_q,
        "workers": workers,
    }
    return jsonify(body), (200 if db_ok else 503)


@app.get("/api/dashboard/returns")
@login_required
def api_dashboard_returns():
    """Paginated dashboard returns for AJAX navigation (Section 3 — dashboard pagination).

    Accepts the same filter query-string parameters as GET / plus `page` and `per_page`.
    Returns JSON with the paginated row list and total-count metadata so the client
    can update the tbody and pagination controls without a full page reload.
    """
    year     = int(request.args.get("year", date.today().year))
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 50))))
    _api_sort = request.args.get("sort", "log_asc")
    if _api_sort not in ("log_asc", "log_desc"):
        _api_sort = "log_asc"
    filters  = {
        "year":           year,
        "status":         request.args.getlist("status") or None,
        "processor":      request.args.get("processor"),
        "balance_due":    request.args.get("balance_due"),
        "late_intake":    request.args.get("late_intake"),
        "slow_cycle":     request.args.get("slow_cycle"),
        "form":           request.args.get("form"),
        "reject_contact": request.args.get("reject_contact"),
        "q":              request.args.get("q"),
        "sort":           _api_sort,
        "scan_deferred":  (
            "1"
            if (request.args.get("filter") or "").strip().lower() == "scan_deferred"
            or request.args.get("scan_deferred")
            else None
        ),
    }
    rows, total_count = query_returns_paginated(filters, page=page, per_page=per_page)
    # Privacy invariant #7: ssn_last4 must not appear in list-endpoint JSON responses.
    # Strip at the data layer — client-side hiding is not sufficient (audit finding M7).
    for r in rows:
        r.pop("ssn_last4", None)
    total_pages = max(1, math.ceil(total_count / per_page))
    return jsonify({
        "returns":     rows,
        "total_count": total_count,
        "page":        page,
        "per_page":    per_page,
        "total_pages": total_pages,
        "has_next":    page < total_pages,
        "has_prev":    page > 1,
    })


@app.get("/api/dashboard/status-counts")
@login_required
def api_dashboard_status_counts():
    """Live poll target for base.html's status summary bar (the row of
    per-status counts + total shown under the nav on every page).

    Added 2026-07-22 alongside M3 going live: scans now update
    returns.client_status directly from the scan station, so these counts
    can change without anyone navigating/reloading a page — this endpoint
    lets the nav bar poll for that instead of only reflecting whatever was
    true at the last full page render.

    Cheap by construction: get_status_counts() is the exact same query
    base_ctx() already runs on every single page load (benchmarked at
    ~1.3ms against the live returns table), and this endpoint does nothing
    else — no template render, no other base_ctx() nav-badge queries -
    intentionally so this can be polled every few seconds without adding
    meaningful load. `year` defaults to the current calendar year, same
    default dashboard() uses; pass the page's own current_year explicitly
    to stay in sync with whatever season the caller is actually viewing.
    """
    year = int(request.args.get("year", date.today().year))
    counts = get_status_counts(year)
    return jsonify({"status_counts": counts, "total": sum(counts.values())})


@app.get("/api/notifications/unread-documents")
@login_required
def api_unread_documents():
    """Document arrival badge — counts unassigned email_inbox items.

    Queries email_inbox directly (audit H3 fix): the badge represents items
    waiting to be assigned to a return, which is email_inbox.is_assigned,
    not return_documents.source.  The old source='email' query always returned
    0 because assignments write source='email_inbox'.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT COUNT(*) n FROM email_inbox WHERE is_assigned=0 AND is_deleted=0"
        ).fetchone()
        return jsonify({"count": row["n"]})
    finally:
        conn.close()


# ── Saved dashboard filters (Epic #85, FILTER-1…FILTER-6) ─────────────────────


def _shared_saved_filter_admins() -> frozenset[str]:
    raw = (os.environ.get("TAXOPS_SHARED_FILTER_ADMINS") or "").strip().lower()
    if raw:
        return frozenset(p.strip().lower() for p in raw.split(",") if p.strip())
    lu = (_LOGIN_USER or "").strip().lower()
    return frozenset({lu}) if lu else frozenset()


def can_publish_shared_dashboard_filters() -> bool:
    u = (_session_username() or "").lower()
    return bool(u) and u in _shared_saved_filter_admins()


def _dashboard_request_has_explicit_filters() -> bool:
    if len(request.args.getlist("status")) > 0:
        return True
    for key in ("processor", "balance_due", "late_intake", "slow_cycle", "form", "reject_contact", "q", "scan_deferred"):
        v = request.args.get(key)
        if v is not None and str(v).strip():
            return True
    filt = (request.args.get("filter") or "").strip().lower()
    if filt == "scan_deferred":
        return True
    return False


_ALLOWED_DASH_SAVE_FORM_FIELDS = frozenset(
    [
        "form_1040", "sched_a_d", "sched_c", "sched_e",
        "form_1120", "form_1120s", "form_1065_llc",
        "corp_officer", "business_owner", "form_990_1041",
        "is_amended", "has_w7", "is_extension",
    ]
)


def _sanitize_dashboard_filter_payload(raw: object) -> dict:
    """Whitelist keys to match query_returns dashboard filters."""
    if not isinstance(raw, dict):
        return {}
    out: dict = {}
    sf = frozenset(s.upper() for s in STATUS_FLOW)
    statuses = raw.get("status")
    filtered_status: list[str] = []
    if isinstance(statuses, list):
        for s in statuses:
            ss = str(s).strip().upper()
            if ss in sf:
                filtered_status.append(ss)
    elif isinstance(statuses, str) and statuses.strip():
        ss = statuses.strip().upper()
        if ss in sf:
            filtered_status.append(ss)
    if filtered_status:
        out["status"] = filtered_status

    processor = raw.get("processor")
    if processor is not None and str(processor).strip():
        out["processor"] = str(processor).strip()

    for flag in ("balance_due", "late_intake", "slow_cycle"):
        val = raw.get(flag)
        if val in ("1", 1, True, "true", "yes", "on"):
            out[flag] = "1"

    form_col = raw.get("form")
    if isinstance(form_col, str) and form_col.strip():
        fk = form_col.strip()
        if fk in _ALLOWED_DASH_SAVE_FORM_FIELDS:
            out["form"] = fk

    rc = raw.get("reject_contact")
    if isinstance(rc, str) and rc.strip():
        rcv = rc.strip().lower()
        if rcv == "needs_followup" or rcv in CONTACT_STATUS_VALUES:
            out["reject_contact"] = rcv

    qq = raw.get("q")
    if isinstance(qq, str) and qq.strip():
        out["q"] = qq.strip()[:500]

    return out


def _dashboard_saved_filter_meaningful(fd: dict) -> bool:
    d = _sanitize_dashboard_filter_payload(fd)
    return bool(d)


def _dashboard_filter_query_string(filter_data: dict, year: int) -> str:
    d = _sanitize_dashboard_filter_payload(filter_data)
    pairs: list[tuple[str, str]] = [("year", str(int(year)))]
    for s in d.get("status") or []:
        pairs.append(("status", s))
    proc = d.get("processor")
    if proc:
        pairs.append(("processor", str(proc)))
    for flag in ("balance_due", "late_intake", "slow_cycle"):
        if d.get(flag) == "1":
            pairs.append((flag, "1"))
    if d.get("form"):
        pairs.append(("form", d["form"]))
    if d.get("reject_contact"):
        pairs.append(("reject_contact", d["reject_contact"]))
    if d.get("q"):
        pairs.append(("q", d["q"]))
    return urlencode(pairs, doseq=True)


def _dashboard_filter_snapshot_from_current_request(year: int) -> dict:
    st = request.args.getlist("status")
    fd: dict = {}
    sf = frozenset(s.upper() for s in STATUS_FLOW)
    st_clean = [str(x).strip().upper() for x in st if str(x).strip().upper() in sf]
    if st_clean:
        fd["status"] = st_clean
    p = request.args.get("processor")
    if p and str(p).strip():
        fd["processor"] = str(p).strip()
    if request.args.get("balance_due"):
        fd["balance_due"] = "1"
    if request.args.get("late_intake"):
        fd["late_intake"] = "1"
    if request.args.get("slow_cycle"):
        fd["slow_cycle"] = "1"
    form = request.args.get("form")
    if form and form.strip() in _ALLOWED_DASH_SAVE_FORM_FIELDS:
        fd["form"] = form.strip()
    rj = request.args.get("reject_contact")
    if rj:
        rv = str(rj).strip().lower()
        if rv == "needs_followup" or rv in CONTACT_STATUS_VALUES:
            fd["reject_contact"] = rv
    qq = request.args.get("q")
    if qq and str(qq).strip():
        fd["q"] = str(qq).strip()[:500]
    return fd


def _saved_dashboard_filters_payload(username: str | None, year: int) -> list[dict]:
    if not username:
        return []
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, user_id, name, filter_json, is_default, is_shared
              FROM dashboard_saved_filters
             WHERE is_shared = 1 OR user_id = ?
             ORDER BY is_shared ASC, is_default DESC, lower(name), id
            """,
            (username,),
        ).fetchall()
    finally:
        conn.close()

    admins = _shared_saved_filter_admins()
    uid_l = username.lower()
    payload: list[dict] = []
    for r in rows:
        try:
            merged = json.loads(r["filter_json"])
        except (json.JSONDecodeError, TypeError, ValueError):
            merged = {}
        fd = _sanitize_dashboard_filter_payload(merged)
        is_shared = bool(r["is_shared"])
        qs = _dashboard_filter_query_string(fd, year)
        owns_personal = (not is_shared) and (r["user_id"] == username)
        payload.append({
            "id":            r["id"],
            "name":          r["name"],
            "is_default":    bool(r["is_default"]) and owns_personal,
            "is_shared":     is_shared,
            "query_string": qs,
            "filter":        fd,
            "can_delete":    (is_shared and uid_l in admins) or owns_personal,
            "can_set_default": owns_personal and not is_shared,
        })
    return payload


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
    year = int(request.args.get("year", date.today().year))
    uname = _session_username()

    # FILTER-5: load user's default preset only on a \"clean\" dashboard query (season only).
    if uname and not _dashboard_request_has_explicit_filters():
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT filter_json FROM dashboard_saved_filters "
                "WHERE user_id = ? AND is_shared = 0 AND is_default = 1 LIMIT 1",
                (uname,),
            ).fetchone()
        finally:
            conn.close()
        if row:
            try:
                sj = json.loads(row["filter_json"])
            except (json.JSONDecodeError, TypeError, ValueError):
                sj = {}
            fd_clean = _sanitize_dashboard_filter_payload(sj)
            if _dashboard_saved_filter_meaningful(fd_clean):
                return redirect("/?" + _dashboard_filter_query_string(fd_clean, year))

    # Dashboard pagination — cap per_page at 100, default 50.
    page     = max(1, int(request.args.get("page", 1)))
    per_page = min(100, max(1, int(request.args.get("per_page", 50))))

    _sort_arg = request.args.get("sort", "log_asc")
    if _sort_arg not in ("log_asc", "log_desc"):
        _sort_arg = "log_asc"
    filters = {
        "year":        year,
        "status":      request.args.getlist("status") or None,
        "processor":   request.args.get("processor"),
        "balance_due": request.args.get("balance_due"),
        "late_intake": request.args.get("late_intake"),
        "slow_cycle":  request.args.get("slow_cycle"),
        "form":        request.args.get("form"),
        "reject_contact": request.args.get("reject_contact"),
        "q":           request.args.get("q"),
        "sort":        _sort_arg,
        "scan_deferred": (
            "1"
            if (request.args.get("filter") or "").strip().lower() == "scan_deferred"
            or request.args.get("scan_deferred")
            else None
        ),
    }
    returns, total_count = query_returns_paginated(filters, page=page, per_page=per_page)
    total_pages = max(1, math.ceil(total_count / per_page))
    ctx = base_ctx(year)
    snap = _dashboard_filter_snapshot_from_current_request(year)
    ctx.update({
        "active_page":                         "dashboard",
        "returns":                             returns,
        "filters":                             filters,
        "total_count":                         total_count,
        "page":                                page,
        "per_page":                            per_page,
        "total_pages":                         total_pages,
        "has_next":                            page < total_pages,
        "has_prev":                            page > 1,
        "sort":                                _sort_arg,
        "saved_dashboard_filters":             _saved_dashboard_filters_payload(uname, year),
        "can_publish_shared_dashboard_filters": can_publish_shared_dashboard_filters(),
        "dashboard_current_filter_snapshot":   snap,
        "dashboard_snapshot_has_meaningful":    _dashboard_saved_filter_meaningful(snap),
    })
    return render_template("dashboard.html", **ctx)


@app.route("/return/<int:return_id>")
@login_required
@view_only_for("receptionist")
def return_detail(return_id: int):
    ret = get_one(return_id)
    if not ret:
        abort(404)
    conn   = get_connection()
    notes  = conn.execute(
        "SELECT * FROM notes WHERE return_id=? ORDER BY created_at DESC", (return_id,)
    ).fetchall()
    events = conn.execute(
        "SELECT * FROM status_events WHERE return_id=? ORDER BY event_timestamp DESC", (return_id,)
    ).fetchall()
    missing_docs = conn.execute(
        "SELECT * FROM missing_docs WHERE return_id=? ORDER BY is_resolved, created_at",
        (return_id,)
    ).fetchall()
    # DEP-1/DEP-3: load active dependents for return detail
    dependents = conn.execute(
        "SELECT * FROM dependents WHERE return_id=? AND is_deleted=0 ORDER BY id",
        (return_id,)
    ).fetchall()
    conn.close()
    notes_payload = [dict(n) for n in notes]
    if privacy_mode_enabled():
        for note in notes_payload:
            note["note_text"] = _mask_value(note.get("note_text"))

    # Always use current calendar year for the season picker — never the return's tax year.
    ctx = base_ctx(date.today().year)
    ctx.update({
        "active_page":    "dashboard",
        "ret":            ret,
        "notes":          notes_payload,
        "events":         [dict(e) for e in events],
        "missing_docs":   [dict(d) for d in missing_docs],
        "dependents":     [dict(d) for d in dependents],
        "contact_labels": CONTACT_LABELS,
        "drake_enabled":  bool(DRAKE_FOLDER_STRUCTURE_ENABLED),
        "view_only":      g.get("view_only", False),
        "scan_agent_url": SCAN_AGENT_URL,
        "scan_agent_token": SCAN_AGENT_TOKEN,
        "can_scan_intake_docs": has_permission("can_scan_intake_docs"),
        "prompt_scan":    request.args.get("scan") == "1",
    })
    return render_template("return_detail.html", **ctx)


@app.post("/api/return/<int:return_id>/scan-deferred")
@permission_required("can_scan_intake_docs")
def api_return_scan_deferred(return_id: int):
    """Mark or clear scan_deferred after intake 'Skip' / successful scan."""
    data = request.get_json(silent=True) or {}
    deferred = 1 if data.get("deferred", True) else 0
    conn = get_connection()
    try:
        row = conn.execute("SELECT id FROM returns WHERE id = ?", (return_id,)).fetchone()
        if not row:
            return jsonify({"error": "Return not found"}), 404
        conn.execute(
            "UPDATE returns SET scan_deferred = ? WHERE id = ?",
            (deferred, return_id),
        )
        conn.commit()
        return jsonify({"success": True, "scan_deferred": deferred})
    finally:
        conn.close()


FILING_STATUS_OPTIONS = ("SINGLE", "MFJ", "MFS", "HH", "DEPENDENT", "QUAL NON DEP")


def profile_title_for_client(cli_d: dict, client_id: int, *, privacy: bool) -> str:
    if privacy:
        return f"Client {client_id}"
    fm = _build_name_full(
        cli_d.get("first_name") or "",
        cli_d.get("last_name")  or "",
        cli_d.get("display_name") or "",
        cli_d.get("spouse_first_name") or "",
        cli_d.get("spouse_last_name")  or "",
    )
    return (fm.strip() or f"Client {client_id}")


@app.route("/clients/<int:client_id>")
@login_required
def client_profile(client_id: int):
    conn = get_connection()
    try:
        cli = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
        if not cli:
            abort(404)
        client_row = dict(cli)
        if privacy_mode_enabled():
            client_disp = _mask_client_payload(client_row)
        else:
            client_disp = client_row

        returns = _fetch_returns_for_client(conn, client_id)
        documents = _fetch_client_documents(conn, client_id)
        activity = _fetch_client_activity(conn, client_id, limit=200)
        doc_year_options = sorted(
            {d["tax_year"] for d in documents if d.get("tax_year") is not None},
            reverse=True,
        )
    finally:
        conn.close()

    anchor_return_id = returns[0]["id"] if returns else None

    nm = profile_title_for_client(client_row, client_id, privacy=privacy_mode_enabled())

    yr = date.today().year
    filing_for_form = ""
    if returns:
        filing_for_form = (returns[0].get("filing_status") or "").strip()

    ctx = base_ctx(yr)
    ctx.update({
        "active_page":           "dashboard",
        "client_id":             client_id,
        "client":                client_disp,
        "client_anchor_return_id": anchor_return_id,
        "client_returns":        returns,
        "client_documents":      documents,
        "client_activity":       activity,
        "doc_year_options":      doc_year_options,
        "doc_type_options":      sorted(_ALLOWED_RETURN_DOC_TYPES),
        "filing_status_options": FILING_STATUS_OPTIONS,
        "filing_status_anchor": filing_for_form,
        "profile_title_name": nm,
        "comparison_year_choices": sorted(
            {r["tax_year"] for r in returns if r.get("tax_year") is not None},
            reverse=True,
        ),
    })
    return render_template("client_profile.html", **ctx)


# DEBT-1: document routes moved to routes/documents.py (Blueprint).

def _serialize_form_row(row: sqlite3.Row) -> dict:
    d = dict(row)
    d.pop("is_deleted", None)
    return d


@app.route("/return/<int:return_id>/form-data")
@login_required
def return_form_data_get(return_id: int):
    conn = get_connection()
    try:
        exists = conn.execute("SELECT 1 FROM returns WHERE id = ?", (return_id,)).fetchone()
        if not exists:
            return jsonify({"error": "Not found"}), 404

        out: dict[str, list] = {
            "w2": [],
            "f1099_nec": [],
            "f1099_misc": [],
            "f1099_int": [],
            "f1099_div": [],
        }

        tbl_map = (
            ("w2_records", "w2"),
            ("f1099_nec_records", "f1099_nec"),
            ("f1099_misc_records", "f1099_misc"),
            ("f1099_int_records", "f1099_int"),
            ("f1099_div_records", "f1099_div"),
        )
        for sql_table, resp_key in tbl_map:
            rows = conn.execute(
                f"""
                SELECT *
                FROM {sql_table}
                WHERE return_id = ? AND is_deleted = 0
                ORDER BY id ASC
                """,
                (return_id,),
            ).fetchall()
            out[resp_key] = [_serialize_form_row(r) for r in rows]

        return jsonify(out)
    finally:
        conn.close()


@app.route(
    "/return/<int:return_id>/form-data/<string:table>/<int:record_id>/update",
    methods=["POST"],
)
@login_required
def return_form_data_update(return_id: int, table: str, record_id: int):
    if table not in _FORM_DATA_SQL_TABLES:
        return jsonify({"error": "Invalid table"}), 400

    payload = request.get_json(silent=True) or {}
    field = payload.get("field")
    if not isinstance(field, str) or not field.strip():
        return jsonify({"error": "Missing or invalid field"}), 400
    field = field.strip()

    allowed = _FORM_DATA_UPDATE_FIELDS.get(table)
    if not allowed or field not in allowed:
        return jsonify({"error": "Invalid field"}), 400

    raw_val = payload.get("value")
    val = _parse_form_update_value(field, raw_val)

    conn = get_connection()
    try:
        cur = conn.execute(
            f"""
            UPDATE {table}
            SET {field} = ?, source = ?, updated_at = ?
            WHERE id = ? AND return_id = ? AND is_deleted = 0
            """,
            (val, "manual", now(), record_id, return_id),
        )
        if cur.rowcount == 0:
            return jsonify({"error": "Not found"}), 404
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.route(
    "/return/<int:return_id>/form-data/<string:table>/<int:record_id>",
    methods=["DELETE"],
)
@login_required
def return_form_data_soft_delete(return_id: int, table: str, record_id: int):
    if table not in _FORM_DATA_SQL_TABLES:
        return jsonify({"error": "Invalid table"}), 400

    conn = get_connection()
    try:
        cur = conn.execute(
            f"""
            UPDATE {table}
            SET is_deleted = 1, updated_at = ?
            WHERE id = ? AND return_id = ? AND is_deleted = 0
            """,
            (now(), record_id, return_id),
        )
        if cur.rowcount == 0:
            return jsonify({"error": "Not found"}), 404
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


# ── Email Inbox ───────────────────────────────────────────────────────────────


# Phase 3.2: filter chips on /email-inbox and /api/email-inbox/items. Each
# maps to a predicate over an already-enriched item dict (needs_manual_tagging
# and age_bucket/suggestion fields must already be populated). Server-side
# filtering — chips are plain links/query params, never client-side hiding.
_INBOX_FILTERS = {
    "needs_manual_tagging": lambda d: d["needs_manual_tagging"],
    "has_suggestion": lambda d: d.get("suggested_return_id") is not None,
    "older_than_7d": lambda d: d["age_bucket"] == "red",
}


def _build_inbox_items(conn, *, filter_key: str | None = None) -> tuple[list[dict], dict]:
    """Fetch, enrich (manual-tag flag, age bucket, suggestions), and
    optionally filter unassigned/non-deleted email_inbox rows.

    Returns (items, summary) where summary has unassigned_total and
    older_than_7d_total computed over the *unfiltered* set (so the header
    summary line/count badge always reflects the whole queue, independent of
    which filter chip is active). Default sort is oldest-first.
    """
    from utils import needs_manual_tagging, age_bucket
    from email_suggest import enrich_items_with_suggestions

    rows = conn.execute(
        "SELECT * FROM email_inbox WHERE is_assigned=0 AND is_deleted=0 "
        "ORDER BY received_at ASC"
    ).fetchall()
    items = [dict(r) for r in rows]
    for d in items:
        d["needs_manual_tagging"] = needs_manual_tagging(d.get("filename") or "")
        d["age_bucket"] = age_bucket(d.get("received_at"))
    # Phase 3.1: non-binding suggestions, computed here (never in the IMAP
    # poll cycle) and cached on the row. Never adds SSN/EIN/TIN/file_path.
    enrich_items_with_suggestions(conn, items)

    summary = {
        "unassigned_total": len(items),
        "older_than_7d_total": sum(1 for d in items if d["age_bucket"] == "red"),
    }

    if filter_key and filter_key in _INBOX_FILTERS:
        items = [d for d in items if _INBOX_FILTERS[filter_key](d)]

    return items, summary


# Phase 3.5: Admin-only "Deleted items" view for soft-delete recovery. Kept
# separate from _build_inbox_items/_INBOX_FILTERS (which are can_use_email_tools
# scoped and only ever query is_deleted=0) so a non-admin can never reach
# is_deleted=1 rows through the normal triage filters.
_DELETED_INBOX_ITEM_PUBLIC_FIELDS = (
    "id", "sender_email", "sender_domain", "subject_snippet", "filename",
    "original_filename", "file_size_bytes", "received_at", "age_bucket",
)


def _build_deleted_inbox_items(conn) -> list[dict]:
    from utils import age_bucket

    rows = conn.execute(
        "SELECT * FROM email_inbox WHERE is_deleted=1 ORDER BY received_at DESC"
    ).fetchall()
    items = [dict(r) for r in rows]
    for d in items:
        d["age_bucket"] = age_bucket(d.get("received_at"))
    return items


@app.route("/email-inbox")
@permission_required("can_use_email_tools")
def email_inbox_page():
    filter_key = request.args.get("filter") or ""
    is_admin = get_effective_role() == "admin"
    conn = get_connection()
    try:
        if filter_key == "deleted" and is_admin:
            inbox_items = _build_deleted_inbox_items(conn)
            _, summary = _build_inbox_items(conn)
        else:
            inbox_items, summary = _build_inbox_items(conn, filter_key=filter_key)
    finally:
        conn.close()
    active_filter = filter_key if (filter_key in _INBOX_FILTERS or (filter_key == "deleted" and is_admin)) else ""
    ctx = base_ctx()
    ctx.update(
        inbox_items=inbox_items,
        unassigned_count=summary["unassigned_total"],
        older_than_7d_count=summary["older_than_7d_total"],
        active_filter=active_filter,
        show_deleted_filter=is_admin,
        active_page="email_inbox",
    )
    return render_template("email_inbox.html", **ctx)


# Explicit allowlist for the JSON API response — deliberately narrower than
# the full email_inbox row (which _build_inbox_items uses internally for
# filtering/enrichment). file_path, sender_name, is_assigned/is_deleted, and
# assigned_* bookkeeping columns never leave this process as JSON.
_INBOX_ITEM_PUBLIC_FIELDS = (
    "id", "sender_email", "sender_domain", "subject_snippet", "filename",
    "original_filename", "file_size_bytes", "received_at",
    "needs_manual_tagging", "age_bucket",
    "suggested_return_id", "suggestion_method", "suggestion_score",
    "suggested_client_name", "suggested_log_number",
)


@app.route("/api/email-inbox/items")
@permission_required("can_use_email_tools")
def api_email_inbox_items():
    filter_key = request.args.get("filter") or ""
    conn = get_connection()
    try:
        if filter_key == "deleted":
            # Phase 3.5: admin-only, even though can_use_email_tools got this
            # far — deleted items are a distinct, more sensitive view.
            if get_effective_role() != "admin":
                return jsonify({"error": "forbidden"}), 403
            deleted_items = _build_deleted_inbox_items(conn)
            _, summary = _build_inbox_items(conn)
            response_items = [
                {k: d.get(k) for k in _DELETED_INBOX_ITEM_PUBLIC_FIELDS} for d in deleted_items
            ]
        else:
            items, summary = _build_inbox_items(conn, filter_key=filter_key)
            response_items = [
                {k: d.get(k) for k in _INBOX_ITEM_PUBLIC_FIELDS} for d in items
            ]
        return jsonify({
            "items": response_items,
            "unassigned_total": summary["unassigned_total"],
            "older_than_7d_total": summary["older_than_7d_total"],
        })
    finally:
        conn.close()


@app.route("/api/email-inbox/<int:item_id>/file")
@permission_required("can_use_email_tools")
def api_email_inbox_file(item_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT file_path, filename FROM email_inbox WHERE id=? AND is_deleted=0",
            (item_id,),
        ).fetchone()
    finally:
        conn.close()
    if row is None:
        abort(404)
    fp = row["file_path"]
    # Path confinement — ensure the stored path cannot escape EMAIL_INBOX_DIR
    # (audit finding H5: a compromised DB value must not allow arbitrary file reads).
    from config import EMAIL_INBOX_DIR
    allowed_root = os.path.realpath(EMAIL_INBOX_DIR)
    requested    = os.path.realpath(fp)
    if not requested.startswith(allowed_root + os.sep):
        app.logger.warning("Path confinement violation blocked for email_inbox item %d: %s", item_id, requested)
        abort(403)
    if not os.path.exists(requested):
        abort(404)
    return send_file(requested, as_attachment=False, download_name=row["filename"])


@app.route("/api/email-inbox/<int:item_id>/assign", methods=["POST"])
@permission_required("can_use_email_tools")
def api_email_inbox_assign(item_id: int):
    data = request.get_json(silent=True) or {}
    return_id = data.get("return_id")
    if not return_id:
        return jsonify({"error": "return_id required"}), 400

    conn = get_connection()
    try:
        inbox_row = conn.execute(
            "SELECT * FROM email_inbox WHERE id=? AND is_assigned=0 AND is_deleted=0",
            (item_id,),
        ).fetchone()
        if inbox_row is None:
            return jsonify({"error": "Item not found or already assigned"}), 404

        ret = conn.execute("SELECT id FROM returns WHERE id=?", (return_id,)).fetchone()
        if ret is None:
            return jsonify({"error": "Return not found"}), 404

        dest_dir = get_return_documents_path(return_id)
        os.makedirs(dest_dir, exist_ok=True)

        src_path = inbox_row["file_path"]
        orig_name = inbox_row["original_filename"] or inbox_row["filename"]
        sanitized = sanitize_filename(orig_name)
        dest_path = os.path.join(dest_dir, sanitized)
        # Avoid collisions
        if os.path.exists(dest_path):
            stem, ext_part = os.path.splitext(sanitized)
            n = 1
            while os.path.exists(dest_path):
                dest_path = os.path.join(dest_dir, f"{stem}_{n}{ext_part}")
                n += 1
        shutil.copy2(src_path, dest_path)

        final_filename = os.path.basename(dest_path)
        ts = now()
        # Phase 3.1: if this return_id matches the item's cached suggestion,
        # this assign was "via suggestion" — recorded for suggestion-quality
        # auditing only; it is still a fully human-confirmed assignment like
        # every other path (match_confirmed=1 either way).
        suggested_return_id = inbox_row["suggested_return_id"] if "suggested_return_id" in inbox_row.keys() else None
        if suggested_return_id is not None and int(suggested_return_id) == int(return_id):
            match_method = "email_suggested"
            match_score = inbox_row["suggestion_score"]
        else:
            match_method = "email_manual"
            match_score = None
        # Invariant (taxops-invariants.mdc): staff picking the return via this
        # endpoint IS the human confirmation. Set match fields explicitly —
        # never rely on the column default, which is what caused this to
        # silently drift to match_method=NULL for every email-assigned doc.
        cur = conn.execute(
            "INSERT INTO return_documents "
            "(return_id, filename, original_filename, doc_type, source, "
            " file_path, file_size_bytes, uploaded_by, uploaded_at, "
            " match_confirmed, match_score, match_method) "
            "VALUES (?, ?, ?, 'unknown', 'email_inbox', ?, ?, ?, ?, 1, ?, ?)",
            (
                return_id, final_filename, orig_name,
                dest_path, inbox_row["file_size_bytes"],
                session.get("username"), ts,
                match_score, match_method,
            ),
        )
        new_doc_id = cur.lastrowid

        conn.execute(
            "UPDATE email_inbox SET is_assigned=1, assigned_return_id=?, "
            "assigned_by=?, assigned_at=? WHERE id=?",
            (return_id, session.get("username"), ts, item_id),
        )
        conn.commit()
        _enqueue_extraction(new_doc_id, return_id)
        return jsonify({"success": True, "doc_id": new_doc_id})
    except Exception as exc:
        conn.rollback()
        logging.error("email_inbox assign error: %s", exc)
        return jsonify({"error": "Assignment failed"}), 500
    finally:
        conn.close()


@app.route("/api/email-inbox/<int:item_id>/delete", methods=["POST"])
@permission_required("can_use_email_tools")
def api_email_inbox_delete(item_id: int):
    conn = get_connection()
    try:
        # Phase 3.5 invariant: flag-only, never unlink from disk — this is
        # what makes admin restore possible. Do not add a file delete here.
        conn.execute(
            "UPDATE email_inbox SET is_deleted=1 WHERE id=?", (item_id,)
        )
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.route("/api/email-inbox/<int:item_id>/restore", methods=["POST"])
@login_required
@role_required("admin")
def api_email_inbox_restore(item_id: int):
    """Phase 3.5: Admin-only recovery from the soft-delete flag. The item
    reappears in the default unassigned list with its original received_at
    (its age badge will likely show red — intentional, per spec)."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, is_deleted FROM email_inbox WHERE id=?", (item_id,)
        ).fetchone()
        if row is None:
            return jsonify({"error": "Item not found"}), 404
        if not row["is_deleted"]:
            return jsonify({"error": "Item is not deleted"}), 400
        conn.execute("UPDATE email_inbox SET is_deleted=0 WHERE id=?", (item_id,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.route("/api/clients/<int:client_id>/returns")
@login_required
def api_client_returns(client_id: int):
    """Lightweight return list for a client — used by the email-inbox assignment panel."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT r.id, r.log_number, r.tax_year, r.client_status "
            "FROM returns r WHERE r.client_id=? ORDER BY r.tax_year DESC, r.id DESC "
            "LIMIT 10",
            (client_id,),
        ).fetchall()
        return jsonify({
            "returns": [
                {
                    "id":            r["id"],
                    "log_number":    r["log_number"],
                    "tax_year":      r["tax_year"],
                    "client_status": r["client_status"],
                }
                for r in rows
            ]
        })
    finally:
        conn.close()


@app.route("/logout-queue")
@permission_required("can_manage_efile_queue")
def logout_queue():
    year = int(request.args.get("year", date.today().year))
    conn = get_connection()
    rows = conn.execute(
        f"{_SELECT} WHERE (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?)) "
        "AND r.client_status = 'PICKUP' ORDER BY CAST(r.log_number AS INTEGER)",
        (str(year), year - 1),
    ).fetchall()
    conn.close()
    saved = request.args.get("saved")
    success = request.args.get("msg", "Saved.") if saved else None
    ctx = base_ctx(year)
    ctx.update({
        "active_page": "logout",
        "returns":     [_enrich(dict(r)) for r in rows],
        "today":       date.today().isoformat(),
        "success":     success,
    })
    return render_template("logout_queue.html", **ctx)


@app.route("/efile-queue")
@permission_required("can_manage_efile_queue")
def efile_queue():
    year  = int(request.args.get("year", date.today().year))
    sort  = request.args.get("sort", "log")   # "log" or "name"
    conn  = get_connection()
    order = (
        "ORDER BY c.last_name, c.first_name" if sort == "name"
        else "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END, CAST(r.log_number AS INTEGER)"
    )
    rows = conn.execute(
        f"{_SELECT} WHERE r.client_status = 'EFILE READY' "
        f"AND (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?)) "
        f"{order}",
        (str(year), year - 1),
    ).fetchall()

    # Build a map of return_id → active batch info so the queue can show
    # which returns are already in an open or transmitted batch.
    active_batch_rows = conn.execute(
        """SELECT bi.return_id, b.id AS batch_id, b.status AS batch_status
           FROM efile_batch_items bi
           JOIN efile_batches b ON b.id = bi.batch_id
           WHERE b.status NOT IN ('closed')"""
    ).fetchall()
    in_active_batch = {r["return_id"]: dict(r) for r in active_batch_rows}

    conn.close()
    enriched = []
    for r in rows:
        rd = _enrich(dict(r))
        ab = in_active_batch.get(r["id"])
        if ab:
            rd["in_batch_id"]     = ab["batch_id"]
            rd["in_batch_status"] = ab["batch_status"]
        enriched.append(rd)

    ctx = base_ctx(year)
    ctx.update({
        "active_page": "efile",
        "returns":     enriched,
        "sort":        sort,
        "today":       date.today().isoformat(),
    })
    return render_template("efile_queue.html", **ctx)


@app.route("/efile-queue/export")
@permission_required("can_manage_efile_queue")
def efile_queue_export():
    import csv, io
    year  = int(request.args.get("year", date.today().year))
    sort  = request.args.get("sort", "log")
    conn  = get_connection()
    order = (
        "ORDER BY c.last_name, c.first_name" if sort == "name"
        else "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END, CAST(r.log_number AS INTEGER)"
    )
    rows = conn.execute(
        f"{_SELECT} WHERE r.client_status = 'EFILE READY' "
        f"AND (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?)) "
        f"{order}",
        (str(year), year - 1),
    ).fetchall()
    conn.close()

    buf = io.StringIO()
    w   = csv.writer(buf)
    w.writerow(["Log #", "Last Name", "First Name", "SSN Last 4", "Tax Year",
                "Preparer", "Pickup Date", "Fee Paid", "Receipt #"])
    for r in rows:
        w.writerow([
            r["log_number"] or "",
            r["last_name"]  or "",
            r["first_name"] or "",
            r["ssn_last4"]  or "",
            r["tax_year"]   or "",
            r["processor"]  or "",
            r["pickup_date"] or "",
            r["fee_paid"]   or "",
            r["receipt_number"] or "",
        ])

    from flask import Response
    filename = f"efile_ready_{year}_{date.today().isoformat()}.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/pickup/<int:return_id>", methods=["GET", "POST"])
@login_required
def pickup_workflow(return_id: int):
    conn = get_connection()
    ret = get_one(return_id)
    if not ret:
        conn.close()
        abort(404)

    # Returns that have moved past PICKUP have no business in the pickup workflow.
    if ret.get("client_status") not in ("PICKUP",):
        conn.close()
        flash("This return is no longer at PICKUP status.", "error")
        return redirect(url_for("return_detail", return_id=return_id))

    error = None
    success = None

    if request.method == "POST":
        f = request.form

        # Signatures — stored both as bool (for checklist) and method (for display)
        sigs_given_method    = normalize_string(f.get("signatures_given_method")) or None
        sigs_received_method = normalize_string(f.get("signatures_received_method")) or None
        sigs_given    = 1 if sigs_given_method else 0
        sigs_received = 1 if sigs_received_method else 0

        method        = f.get("payment_method", "").strip()
        is_qb         = (method == "QB Billing")
        base_fee_raw  = f.get("total_fee", "").strip()
        base_fee      = _to_float(base_fee_raw) if base_fee_raw else None
        cc_fee        = round((base_fee or 0) * CARD_FEE_RATE, 2) if method == "Card/Visa" else 0.0
        # QB billing: fee is invoiced externally — record invoice amount, fee_paid stays 0
        fee_paid      = 0.0 if is_qb else (round((base_fee or 0) + cc_fee, 2) if base_fee is not None else None)
        # QB Billing: default receipt to "QB" so the checklist/status-advance pass
        # without requiring a numeric receipt number that may not exist yet
        receipt       = normalize_string(f.get("receipt_number")) or ("QB" if is_qb else None)
        check_number  = normalize_string(f.get("check_number")) or None
        pickup_date   = normalize_string(f.get("pickup_date")) or None
        notes         = normalize_string(f.get("notes")) or None

        # QB Billing clients are "paid" for advancement purposes even with fee_paid=0
        payment_ok = (fee_paid is not None and fee_paid > 0) or is_qb
        ready = sigs_received and payment_ok and receipt
        new_status = "EFILE READY" if ready else ret.get("client_status")
        if is_locked_status(ret.get("client_status")):
            new_status = ret.get("client_status")

        conn.execute(
            "UPDATE returns SET signatures_given=?, signatures_received=?, "
            "signatures_given_method=?, signatures_received_method=?, "
            "pickup_date=COALESCE(?,pickup_date), client_status=?, updated_at=? WHERE id=?",
            (sigs_given, sigs_received,
             sigs_given_method, sigs_received_method,
             pickup_date, new_status, datetime.now().isoformat(), return_id),
        )

        # Use COALESCE so a re-submit without fee values doesn't wipe previously saved amounts.
        # fee_paid can legitimately be 0 (QB Billing) — don't coerce 0 → None.
        _fee_db      = base_fee               # None if field left blank
        _cc_db       = cc_fee if cc_fee else None
        _paid_db     = fee_paid               # 0.0 is valid for QB; None when fee field blank
        pay_row = conn.execute("SELECT id FROM payments WHERE return_id=?", (return_id,)).fetchone()
        if pay_row:
            conn.execute(
                "UPDATE payments SET "
                "total_fee=COALESCE(?,total_fee), "
                "cc_fee=COALESCE(?,cc_fee), "
                "fee_paid=COALESCE(?,fee_paid), "
                "payment_method=COALESCE(?,payment_method), "
                "check_number=COALESCE(?,check_number), "
                "receipt_number=COALESCE(?,receipt_number) "
                "WHERE return_id=?",
                (_fee_db, _cc_db, _paid_db,
                 method or None, check_number, receipt, return_id),
            )
        else:
            conn.execute(
                "INSERT INTO payments "
                "(return_id, total_fee, cc_fee, fee_paid, payment_method, check_number, receipt_number) "
                "VALUES (?,?,?,?,?,?,?)",
                (return_id, _fee_db, _cc_db, _paid_db,
                 method or None, check_number, receipt),
            )

        if notes:
            conn.execute(
                "INSERT INTO notes (return_id, note_text, created_at) VALUES (?,?,?)",
                (return_id, notes, datetime.now().isoformat()),
            )

        conn.commit()
        conn.close()
        success_msg = "Saved."
        if new_status == "EFILE READY":
            success_msg = "Pickup complete — status moved to EFILE READY."
            intake = ret.get("intake_date") or ""
            try:
                year_for_queue = int(intake[:4]) if len(intake) >= 4 else date.today().year
            except (ValueError, TypeError):
                year_for_queue = date.today().year
            return redirect(
                url_for(
                    "logout_queue",
                    year=year_for_queue,
                    saved=1,
                    msg=success_msg,
                )
            )
        return redirect(f"/pickup/{return_id}?saved=1&msg={success_msg}")

    saved = request.args.get("saved")
    if saved:
        success = request.args.get("msg", "Saved.")

    conn.close()
    ctx = base_ctx()
    ctx.update({
        "active_page": "logout",
        "ret":         ret,
        "success":     success,
        "card_fee_rate": CARD_FEE_RATE,
    })
    return render_template("pickup_workflow.html", **ctx)


@app.route("/payments")
@role_required("admin")
def payments():
    year         = int(request.args.get("year", date.today().year))
    balance_only = request.args.get("balance_only")
    where = "WHERE r.tax_year=?"
    if balance_only:
        where += " AND p.total_fee IS NOT NULL AND COALESCE(p.fee_paid,0) < p.total_fee"
    conn = get_connection()
    rows = conn.execute(
        f"{_SELECT} {where} ORDER BY CAST(r.log_number AS INTEGER)", (year,)
    ).fetchall()
    conn.close()
    ctx = base_ctx(year)
    ctx.update({
        "active_page":  "payments",
        "returns":      [_enrich(dict(r)) for r in rows],
        "balance_only": balance_only,
    })
    return render_template("payments.html", **ctx)


# ── Intake form ───────────────────────────────────────────────────────────────

@app.route("/intake", methods=["GET", "POST"])
@login_required
def intake():
    # Tax Year is no longer a per-intake choice — it's a single admin-set
    # value, changed only at season rollover (see /admin/season-rollover),
    # never per walk-in. This is what actually fixed the "goes to 1233
    # instead of 1270s" bug: staff could never accidentally leave a stale
    # default selected, because there's no longer a selectable default.
    _tay_conn = get_connection()
    try:
        active_intake_tax_year = get_active_intake_tax_year(_tay_conn)
    finally:
        _tay_conn.close()

    if request.method == "GET":
        from config import INTAKE_SUGGESTED_UPCHARGE_PCT as _upc
        ctx = base_ctx()
        ctx.update({
            "active_page": "intake",
            "today": date.today().isoformat(),
            "error": None,
            "habit_profile": None,
            "intake_suggested_upcharge_pct": _upc,
            "active_intake_tax_year": active_intake_tax_year,
        })
        return render_template("intake.html", **ctx)

    # POST — create records
    f = request.form
    ts = now()
    today_iso = date.today().isoformat()

    last_name  = (f.get("last_name") or "").strip().upper()
    first_name = (f.get("first_name") or "").strip().upper()
    if not last_name:
        from config import INTAKE_SUGGESTED_UPCHARGE_PCT as _upc
        ctx = base_ctx()
        ctx.update({"active_page": "intake", "today": today_iso, "error": "Last name is required.",
                    "prefill": {}, "intake_suggested_upcharge_pct": _upc,
                    "active_intake_tax_year": active_intake_tax_year})
        return render_template("intake.html", **ctx), 400

    def _v(key):
        val = f.get(key, "").strip()
        return val or None

    def _n(key):
        val = f.get(key, "").strip()
        try:
            return float(val) if val else None
        except ValueError:
            return None

    def _i(key):
        val = f.get(key, "").strip()
        return int(val) if val.isdigit() else None

    def _ssn_last4_from_full(field_name: str) -> str | None:
        """Extract last 4 digits from a full SSN field (XXX-XX-XXXX). Never logged."""
        raw = f.get(field_name, "").strip()
        digits = _re.sub(r"\D", "", raw)
        if len(digits) >= 4:
            return digits[-4:]
        return digits if digits else None

    conn = get_connection()

    # RACE-1: the log-number read+insert below can race two concurrent
    # intakes into reading the same MAX(log_number) before either commits,
    # producing duplicate log numbers in the same tax_year (see
    # AUDIT_INTAKE.md, Link 3). _run_intake_write() is the entire original
    # intake body, unchanged below except for the added BEGIN IMMEDIATE;
    # wrapping it in a function (rather than reindenting it under a new
    # loop) lets the retry loop below call it without touching a single
    # line of existing field-handling logic.
    def _run_intake_write():
        # Server-authoritative: the intake form no longer sends a real choice
        # here (Tax Year is a locked display on the form, not a dropdown —
        # see intake.html). Any client-submitted "tax_year" is ignored on
        # purpose, so this can never again be silently wrong per-intake; the
        # only way it changes is an admin running /admin/season-rollover.
        tax_year = get_active_intake_tax_year(conn)

        # RACE-1: acquire the write lock *before* the MAX read below (not on
        # the first INSERT, as sqlite3's default deferred-transaction mode
        # would) so a second concurrent intake can't read the same MAX before
        # this one commits. WAL readers elsewhere are unaffected; only other
        # writers queue behind this lock until commit()/rollback() below.
        conn.execute("BEGIN IMMEDIATE")

        # ── Auto log number (max + 1 for this tax year) ───────────────────────
        row = conn.execute(
            "SELECT MAX(CAST(log_number AS INTEGER)) AS mx FROM returns WHERE tax_year = ?",
            (tax_year,),
        ).fetchone()
        log_number = str((row["mx"] or 0) + 1)

        # ── Client — insert new or update existing (re-intake) ────────────────
        existing_client_id = _i("client_id")
        # INTAKE-2: derive ssn_last4 from full SSN field (never stored in full)
        taxpayer_ssn_last4 = _ssn_last4_from_full("ssn_full")
        spouse_ssn_last4   = _ssn_last4_from_full("spouse_ssn_full")

        if existing_client_id:
            conn.execute(
                """
                UPDATE clients SET
                    last_name=?, first_name=?, ssn_last4=?,
                    spouse_last_name=?, spouse_first_name=?,
                    taxpayer_dob=?, spouse_dob=?,
                    taxpayer_occupation=?, spouse_occupation=?,
                    taxpayer_phone=COALESCE(?, taxpayer_phone),
                    taxpayer_cell=COALESCE(?, taxpayer_cell),
                    taxpayer_work_phone=COALESCE(?, taxpayer_work_phone),
                    spouse_cell=COALESCE(?, spouse_cell),
                    spouse_work_phone=COALESCE(?, spouse_work_phone),
                    taxpayer_email=COALESCE(?, taxpayer_email),
                    spouse_email=COALESCE(?, spouse_email),
                    address=?, referral_flag=?, referred_by=?,
                    is_new_client=0, prior_year_log=?, updated_at=?
                WHERE id=?
                """,
                (
                    last_name, first_name, taxpayer_ssn_last4,
                    (_v("spouse_last_name") or "").upper() or None,
                    (_v("spouse_first_name") or "").upper() or None,
                    _v("taxpayer_dob"), _v("spouse_dob"),
                    _v("taxpayer_occupation"), _v("spouse_occupation"),
                    _v("taxpayer_phone"), _v("taxpayer_cell"), _v("taxpayer_work_phone"),
                    _v("spouse_cell"), _v("spouse_work_phone"),
                    _v("taxpayer_email"), _v("spouse_email"),
                    _v("address"),
                    1 if f.get("referral_flag") else 0,
                    _v("referred_by"),
                    _v("prior_year_log"),
                    ts, existing_client_id,
                ),
            )
            client_id = existing_client_id
        else:
            conn.execute(
                """
                INSERT INTO clients (
                    last_name, first_name, ssn_last4,
                    spouse_last_name, spouse_first_name,
                    taxpayer_dob, spouse_dob,
                    taxpayer_occupation, spouse_occupation,
                    taxpayer_phone, taxpayer_cell, taxpayer_work_phone,
                    spouse_cell, spouse_work_phone,
                    taxpayer_email, spouse_email,
                    address, referral_flag, referred_by,
                    is_new_client, prior_year_log,
                    created_at, updated_at
                ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    last_name, first_name, taxpayer_ssn_last4,
                    (_v("spouse_last_name") or "").upper() or None,
                    (_v("spouse_first_name") or "").upper() or None,
                    _v("taxpayer_dob"), _v("spouse_dob"),
                    _v("taxpayer_occupation"), _v("spouse_occupation"),
                    _v("taxpayer_phone"), _v("taxpayer_cell"), _v("taxpayer_work_phone"),
                    _v("spouse_cell"), _v("spouse_work_phone"),
                    _v("taxpayer_email"), _v("spouse_email"),
                    _v("address"),
                    1 if f.get("referral_flag") else 0,
                    _v("referred_by"),
                    int(f.get("is_new_client", "0")),
                    _v("prior_year_log"),
                    ts, ts,
                ),
            )
            client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # ── Return ────────────────────────────────────────────────────────────
        conn.execute(
            """
            INSERT INTO returns (
                client_id, log_number, tax_year, client_status,
                processor, verified, intake_date, interview_by,
                filing_status, promise_date, delivered_by,
                date_signatures_emailed, date_reports_emailed,
                overtime_flag, insurance_type, digital_assets,
                bank_name, bank_routing, bank_account, bank_account_type,
                notes_intake,
                is_amended, has_w7, is_extension,
                estimate_irs, estimate_state, final_irs, final_state,
                created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                client_id,
                log_number,
                tax_year,
                "PROCESSING",
                normalize_preparer(_v("processor")),
                1 if f.get("verified") else 0,
                _v("intake_date") or today_iso,
                _v("interview_by"),
                _v("filing_status"),
                _v("promise_date"),
                _v("delivered_by"),
                _v("date_signatures_emailed"),
                _v("date_reports_emailed"),
                int(f.get("overtime_flag", "0")),
                _v("insurance_type"),
                int(f.get("digital_assets", "0")),
                _v("bank_name"), _v("bank_routing"), _v("bank_account"), _v("bank_account_type"),
                _v("notes_intake"),
                1 if f.get("is_amended") else None,
                1 if f.get("has_w7") else None,
                1 if f.get("is_extension") else None,
                _n("estimate_irs"), _n("estimate_state"),
                _n("final_irs"), _n("final_state"),
                ts, ts,
            ),
        )
        return_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

        # ── Return forms ──────────────────────────────────────────────────────
        form_fields = [
            "form_1040", "sched_a_d", "sched_c", "sched_e",
            "form_1120", "form_1120s", "form_1065_llc",
            "corp_officer", "business_owner", "form_990_1041",
        ]
        form_vals = {field: (1 if f.get(field) else None) for field in form_fields}
        conn.execute(
            f"""INSERT INTO return_forms (return_id, {', '.join(form_fields)})
                VALUES (?, {', '.join('?' for _ in form_fields)})""",
            [return_id] + [form_vals[k] for k in form_fields],
        )

        # ── Payment ───────────────────────────────────────────────────────────
        # INTAKE-8: apply auto-discount for new clients (no prior return)
        discount_val = _n("discount_amount")
        if existing_client_id is None and discount_val is None:
            discount_val = float(INTAKE_AUTO_DISCOUNT) if INTAKE_AUTO_DISCOUNT else None

        conn.execute(
            """
            INSERT INTO payments (
                return_id, total_fee, fee_paid, receipt_number, receipt2_number,
                accounting_fee, w7_fee, form_1099_fee, license_fee,
                reprocess_fee, discount_amount, special_discount, down_payment
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                return_id,
                _n("total_fee"), _n("fee_paid"),
                _v("receipt_number"), _v("receipt2_number"),
                _n("accounting_fee"), _n("w7_fee"), _n("form_1099_fee"), _n("license_fee"),
                _n("reprocess_fee"), discount_val, _n("special_discount"),
                _n("down_payment"),
            ),
        )

        # ── Dependents ────────────────────────────────────────────────────────
        dep_count = int(f.get("dep_count", "6"))
        for i in range(1, dep_count + 1):
            name = (f.get(f"dep_name_{i}") or "").strip().upper()
            if not name:
                continue
            conn.execute(
                """
                INSERT INTO dependents
                  (return_id, full_name, ssn_last4, relationship, date_of_birth, medi_cal, on_medicare, created_at)
                VALUES (?,?,?,?,?,?,?,?)
                """,
                (
                    return_id, name,
                    _v(f"dep_ssn_{i}"),
                    _v(f"dep_rel_{i}"),
                    _v(f"dep_dob_{i}"),
                    1 if f.get(f"dep_medicaid_{i}") else 0,
                    1 if f.get(f"dep_medicare_{i}") else 0,
                    ts,
                ),
            )

        # ── Missing documents ─────────────────────────────────────────────────
        missing_items = request.form.getlist("missing_doc")
        custom_raw    = (f.get("missing_doc_custom") or "").strip()
        for line in custom_raw.splitlines():
            line = line.strip()
            if line:
                missing_items.append(line)
        for item in missing_items:
            item = item.strip()
            if item:
                conn.execute(
                    "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at)"
                    " VALUES (?,?,0,?)",
                    (return_id, item, ts),
                )

        # ── Status event ──────────────────────────────────────────────────────
        conn.execute(
            """
            INSERT INTO status_events
              (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
            VALUES (?, 'STATUS_CHANGED', NULL, 'PROCESSING', ?, 'INTAKE', 'Created via intake form')
            """,
            (return_id, ts),
        )

        # ── Notes ─────────────────────────────────────────────────────────────
        if _v("notes_intake"):
            conn.execute(
                "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,'INTAKE',?)",
                (return_id, _v("notes_intake"), ts),
            )

        conn.commit()

        # M3: print a physical file label for the new log_number, behind a
        # feature flag. This is a convenience, not a hard invariant like the
        # DB writes above — any failure here (module missing, printer off,
        # pywin32 not installed, relay unreachable) is logged and swallowed,
        # never breaks intake.
        #
        # FILETRACK_PRINT_MODE picks how the label actually gets to the
        # printer: "local" calls win32print directly (only works when the
        # printer is a real local Windows queue on THIS process's machine —
        # never true for a Windows *service*'s Session 0 if the printer is
        # only attached to a different workstation). "relay" instead POSTs
        # the job to filetrack.relay.server running on the machine the
        # printer is actually attached to. See filetrack/DEPLOYMENT.md.
        from filetrack.config import FILETRACK_ENABLED
        if FILETRACK_ENABLED:
            try:
                from filetrack.config import FILETRACK_PRINT_MODE
                _label_kwargs = dict(log_in_date=_v("intake_date") or today_iso)
                if FILETRACK_PRINT_MODE == "relay":
                    from filetrack.labels.relay_client import print_label_via_relay
                    print_label_via_relay(log_number, **_label_kwargs)
                else:
                    from filetrack.labels.print_label import print_label as _filetrack_print_label
                    _filetrack_print_label(log_number, **_label_kwargs)
            except Exception as _print_exc:
                # RelayError's message is already the actionable, human-
                # readable summary (see relay_client._classify_network_error)
                # — surface it as its own WARNING line, ahead of the full
                # exc_info traceback below, so whoever is skimming
                # taxops_stderr.log doesn't have to parse a stack trace to
                # find out what to actually go check on the print station.
                from filetrack.labels.relay_client import RelayError
                if isinstance(_print_exc, RelayError):
                    logging.getLogger("filetrack").warning(
                        "filetrack: label print failed for log_number=%s — %s",
                        log_number, _print_exc,
                    )
                logging.getLogger("filetrack").warning(
                    "filetrack: label print failed for log_number=%s", log_number, exc_info=True,
                )

        if has_permission("can_scan_intake_docs"):
            return redirect(f"/return/{return_id}?scan=1")
        return redirect(f"/return/{return_id}")

    def _is_log_number_conflict(exc: sqlite3.IntegrityError) -> bool:
        # RACE-1: matches the exact ux_returns_log_year UNIQUE index message
        # (see db.py) — "UNIQUE constraint failed: returns.log_number,
        # returns.tax_year" — so unrelated IntegrityErrors (bad FK, some other
        # constraint) are never mistaken for the log-number race and retried.
        msg = str(exc)
        return "returns.log_number" in msg and "returns.tax_year" in msg

    try:
        _LOG_NUMBER_MAX_ATTEMPTS = 3
        for _log_number_attempt in range(1, _LOG_NUMBER_MAX_ATTEMPTS + 1):
            try:
                return _run_intake_write()
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                # RACE-1: belt-and-suspenders — with BEGIN IMMEDIATE above
                # holding the write lock across the read+insert, this should
                # not fire in normal operation. If it ever does, retry with a
                # fresh MAX read under a new lock rather than a raw 500;
                # anything that isn't this specific conflict behaves exactly
                # as before (single attempt, generic error).
                if _is_log_number_conflict(exc) and _log_number_attempt < _LOG_NUMBER_MAX_ATTEMPTS:
                    continue
                ctx = base_ctx()
                if _is_log_number_conflict(exc):
                    err_msg, status = "Could not assign a log number, please retry.", 409
                else:
                    err_msg, status = str(exc), 500
                ctx.update({"active_page": "intake", "today": today_iso, "error": err_msg,
                            "active_intake_tax_year": active_intake_tax_year})
                return render_template("intake.html", **ctx), status
    except Exception as exc:
        conn.rollback()
        ctx = base_ctx()
        ctx.update({"active_page": "intake", "today": today_iso, "error": str(exc),
                    "active_intake_tax_year": active_intake_tax_year})
        return render_template("intake.html", **ctx), 500
    finally:
        conn.close()


# ── CSV Upload / Analyze ──────────────────────────────────────────────────────

@app.route("/upload", methods=["GET"])
@role_required("admin")
def upload_get():
    ctx = base_ctx()
    ctx.update({"active_page": "upload", "error": None})
    return render_template("upload.html", **ctx)


@app.route("/upload/preview", methods=["POST"])
@role_required("admin")
def upload_preview():
    f = request.files.get("csv_file")
    if not f or not f.filename:
        return jsonify({"error": "No file uploaded"}), 400

    file_bytes = f.read()
    result = analyze(file_bytes, f.filename)

    # Store file bytes in a temp file keyed by a token for the confirm step
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv", prefix="taxops_upload_")
    tmp.write(file_bytes)
    tmp.close()

    cols = [
        {
            "index":       c.col_index,
            "raw_header":  c.raw_header,
            "table":       c.table,
            "field":       c.field,
            "field_type":  c.field_type,
            "confidence":  c.confidence,
            "skip":        c.skip,
            "skip_reason": c.skip_reason,
        }
        for c in result.columns
    ]

    return jsonify({
        "tmp_path":    tmp.name,
        "filename":    f.filename,
        "total_rows":  result.total_rows,
        "warnings":    result.warnings,
        "columns":     cols,
        "sample_rows": result.sample_rows[:8],
        "data_start":  result.data_start_index,
        "header_row":  result.header_row_index,
    })


@app.route("/upload/confirm", methods=["POST"])
@role_required("admin")
def upload_confirm():
    """Execute import using the analysis result confirmed by staff."""
    data       = _get_json_safe()
    tmp_path   = data.get("tmp_path", "")
    overrides  = data.get("overrides", {})   # {str(col_index): "table.field" | "skip"}
    tax_year   = int(data.get("tax_year", date.today().year))

    if not tmp_path or not os.path.exists(tmp_path):
        return jsonify({"error": "Upload session expired — please re-upload."}), 400

    with open(tmp_path, "rb") as fh:
        file_bytes = fh.read()

    result    = analyze(file_bytes)
    ts        = now()
    today_iso = date.today().isoformat()

    # Apply overrides to column mappings
    for c in result.columns:
        key = str(c.col_index)
        if key in overrides:
            ov = overrides[key]
            if ov == "skip":
                c.skip = True
                c.table = c.field = ""
            elif "." in ov:
                parts = ov.split(".", 1)
                c.table, c.field = parts[0], parts[1]
                c.skip = False

    rows = iter_data_rows(file_bytes, result)

    conn = get_connection()
    stats = {"created": 0, "updated": 0, "skipped": 0, "review": 0, "errors": []}

    try:
        # Build client cache once so fuzzy matching doesn't hammer the DB per row
        client_cache = _all_clients_cache(conn)
        for row_data in rows:
            try:
                _import_row(conn, row_data, tax_year, ts, today_iso, stats, _client_cache=client_cache)
            except Exception as exc:
                stats["errors"].append(str(exc))
                if len(stats["errors"]) > 20:
                    break
        conn.commit()
    finally:
        conn.close()
        try:
            os.unlink(tmp_path)
        except OSError:
            pass

    return jsonify(stats)


def _import_row_forced(conn, row_data: dict, tax_year: int, ts: str, today_iso: str,
                       stats: dict, *, client_id: int | None):
    """Run _import_row but skip fuzzy matching — use the supplied client_id directly,
    or create a new client if client_id is None."""
    # Temporarily patch the row so _import_row's name-parse produces something
    # that will definitely match (or not) based on client_id override.
    # Easiest: delegate to _import_row with a single-entry cache that forces the match.
    if client_id is not None:
        forced_cache = [{"id": client_id, "ln": "\x00FORCED\x00", "fn": ""}]
        # Pre-seed row name with the sentinel so exact match fires
        patched = dict(row_data)
        patched["clients.last_name"]  = "\x00FORCED\x00"
        patched["clients.first_name"] = ""
        _import_row(conn, patched, tax_year, ts, today_iso, stats, _client_cache=forced_cache)
    else:
        # No client_id → force new client by using an empty cache
        _import_row(conn, row_data, tax_year, ts, today_iso, stats, _client_cache=[])


def _import_row(conn, row_data: dict, tax_year: int, ts: str, today_iso: str, stats: dict,
                _client_cache: list | None = None):
    """Import a single analyzed row into the database."""
    def g(table, field):
        return row_data.get(f"{table}.{field}", "") or ""

    raw_last  = normalize_string(g("clients", "last_name"))
    raw_first = normalize_string(g("clients", "first_name"))
    display   = normalize_string(g("clients", "display_name"))

    if not raw_last and display:
        raw_last = display

    log_number = normalize_string(g("returns", "log_number"))

    # Placeholder row: reserved log slot with no client data — skip silently
    if not raw_last:
        stats["skipped"] += 1
        return

    if not log_number:
        stats["skipped"] += 1
        return

    # ── Parse name via name_matcher ───────────────────────────────────────────
    last_name, first_name = parse_name(raw_last)
    # If the CSV already split first/last, prefer that
    if raw_first:
        first_name = raw_first.upper().strip() or None
    last_name = last_name.upper().strip()
    first_name = (first_name or "").upper().strip() or None

    # ── Match or create client ────────────────────────────────────────────────
    match = fuzzy_find_client(conn, last_name, first_name, cache=_client_cache)

    if match and not match["needs_review"]:
        # Confident match — upsert against existing client
        client_id = match["client_id"]
        conn.execute("UPDATE clients SET updated_at=? WHERE id=?", (ts, client_id))
        stats["updated"] = stats.get("updated", 0) + 1
        match_method = match["method"]
    elif match and match["needs_review"]:
        # Low-confidence — park in review queue for human decision, don't process yet
        stats["review"] = stats.get("review", 0) + 1
        raw_yr = g("returns", "tax_year")
        ret_year_q = int(raw_yr) if raw_yr.isdigit() else tax_year
        conn.execute(
            """INSERT INTO review_queue
               (status, csv_last, csv_first, csv_log, csv_year,
                proposed_client_id, match_score, match_method,
                raw_json, reason, created_at)
               VALUES ('pending',?,?,?,?,?,?,?,?,?,?)""",
            (
                last_name, first_name, log_number, ret_year_q,
                match["client_id"], match["score"], match["method"],
                json.dumps(row_data),
                f"Fuzzy score={match['score']} method={match['method']}",
                ts,
            ),
        )
        return  # do not create return — wait for staff to resolve
    else:
        # Fuzzy returned nothing — try an exact SQL lookup before inserting a new row.
        # This prevents duplicates for business names (NULL first_name) and handles
        # cases where the in-memory cache was not populated (e.g. first row of a session).
        if first_name:
            _exact = conn.execute(
                "SELECT id FROM clients WHERE lower(last_name)=lower(?) AND lower(first_name)=lower(?) LIMIT 1",
                (last_name, first_name),
            ).fetchone()
        else:
            _exact = conn.execute(
                "SELECT id FROM clients WHERE lower(last_name)=lower(?) AND (first_name IS NULL OR first_name='') LIMIT 1",
                (last_name,),
            ).fetchone()

        if _exact:
            client_id = _exact[0]
            conn.execute("UPDATE clients SET updated_at=? WHERE id=?", (ts, client_id))
            if _client_cache is not None and not any(c["id"] == client_id for c in _client_cache):
                _client_cache.append({"id": client_id, "ln": last_name.upper(), "fn": (first_name or "").upper()})
            stats["updated"] = stats.get("updated", 0) + 1
            match_method = "exact_fallback"
        else:
            # Genuinely new client
            conn.execute(
                """INSERT INTO clients (last_name, first_name, referral_flag, referred_by,
                                        prior_year_log, is_new_client, created_at, updated_at)
                   VALUES (?,?,?,?,?,1,?,?)""",
                (
                    last_name, first_name,
                    1 if g("clients", "referral_flag") else 0,
                    normalize_string(g("clients", "referred_by")),
                    normalize_string(g("clients", "prior_year_log")),
                    ts, ts,
                ),
            )
            client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
            # Add to cache so subsequent rows for the same new client match
            if _client_cache is not None:
                _client_cache.append({"id": client_id, "ln": last_name.upper(), "fn": (first_name or "").upper()})
            stats["created"] = stats.get("created", 0) + 1
            match_method = "new"

    # ── Match or create return ────────────────────────────────────────────────
    ret_year = tax_year
    raw_yr = g("returns", "tax_year")
    if raw_yr.isdigit() and 2000 <= int(raw_yr) <= 2030:
        ret_year = int(raw_yr)

    # Match by client + year only — log_number may be absent on Drake-imported
    # returns and will be written onto the record if the CSV supplies it.
    existing_ret = conn.execute(
        "SELECT id, log_number FROM returns WHERE client_id=? AND tax_year=?",
        (client_id, ret_year),
    ).fetchone()

    raw_status  = g("returns", "client_status")
    norm_status = normalize_status(raw_status) if raw_status else "PROCESSING"

    intake_dt,  _ = normalize_date(g("returns", "intake_date"))
    pickup_dt,  _ = normalize_date(g("returns", "pickup_date"))
    logout_dt,  _ = normalize_date(g("returns", "logout_date"))
    emailed_dt, _ = normalize_date(g("returns", "date_emailed"))
    updated_dt, _ = normalize_date(g("returns", "updated_date"))

    def flag(table, field):
        v = g(table, field).strip()
        return 1 if v and v not in ("0", "", " ") else None

    ret_fields = dict(
        client_id=client_id,
        log_number=log_number,
        tax_year=ret_year,
        client_status=norm_status,
        processor=normalize_preparer(normalize_string(g("returns", "processor"))),
        verified=flag("returns", "verified"),
        intake_date=intake_dt or today_iso,
        date_emailed=emailed_dt,
        pickup_date=pickup_dt,
        logout_date=logout_dt,
        updated_date=updated_dt,
        is_amended=flag("returns", "is_amended"),
        has_w7=flag("returns", "has_w7"),
        is_extension=flag("returns", "is_extension"),
        transfer_flag=flag("returns", "transfer_flag"),
        transfer_2025_flag=flag("returns", "transfer_2025_flag"),
        transfer_2026_flag=flag("returns", "transfer_2026_flag"),
    )

    if existing_ret:
        ret_id = existing_ret["id"]
        # Stamp log_number from CSV onto the return if it didn't have one yet
        # (Drake imports don't carry log numbers; the manual log is the source).
        existing_log = existing_ret["log_number"]
        new_log = log_number if log_number else existing_log
        conn.execute(
            """UPDATE returns
               SET log_number=?,
                   client_status=?, processor=?, verified=?,
                   intake_date=COALESCE(intake_date,?),
                   pickup_date=COALESCE(pickup_date,?),
                   logout_date=COALESCE(logout_date,?),
                   updated_at=?
               WHERE id=?""",
            (new_log, norm_status, ret_fields["processor"], ret_fields["verified"],
             ret_fields["intake_date"], pickup_dt, logout_dt, ts, ret_id),
        )
    else:
        cols = ", ".join(ret_fields.keys()) + ", created_at, updated_at"
        vals = ", ".join("?" for _ in ret_fields) + ", ?, ?"
        conn.execute(
            f"INSERT INTO returns ({cols}) VALUES ({vals})",
            list(ret_fields.values()) + [ts, ts],
        )
        ret_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]

    # ── Forms ─────────────────────────────────────────────────────────────────
    form_fields = {
        "form_1040": flag("return_forms", "form_1040"),
        "sched_a_d": flag("return_forms", "sched_a_d"),
        "sched_c":   flag("return_forms", "sched_c"),
        "sched_e":   flag("return_forms", "sched_e"),
        "form_1120": flag("return_forms", "form_1120"),
        "form_1120s":flag("return_forms", "form_1120s"),
        "form_1065_llc": flag("return_forms", "form_1065_llc"),
        "corp_officer":  flag("return_forms", "corp_officer"),
        "business_owner":flag("return_forms", "business_owner"),
        "form_990_1041": flag("return_forms", "form_990_1041"),
    }
    existing_forms = conn.execute("SELECT id FROM return_forms WHERE return_id=?", (ret_id,)).fetchone()
    if existing_forms:
        sets = ", ".join(f"{k}=?" for k in form_fields)
        conn.execute(f"UPDATE return_forms SET {sets} WHERE return_id=?",
                     list(form_fields.values()) + [ret_id])
    else:
        fcols = "return_id, " + ", ".join(form_fields.keys())
        fvals = "?, " + ", ".join("?" for _ in form_fields)
        conn.execute(f"INSERT INTO return_forms ({fcols}) VALUES ({fvals})",
                     [ret_id] + list(form_fields.values()))

    # ── Payment ───────────────────────────────────────────────────────────────
    total_fee = normalize_currency(g("payments", "total_fee"))
    fee_paid  = normalize_currency(g("payments", "fee_paid"))
    cc_fee    = normalize_currency(g("payments", "cc_fee"))
    receipt   = normalize_string(g("payments", "receipt_number"))
    zelle     = normalize_string(g("payments", "zelle_or_check_ref"))
    cash      = normalize_string(g("payments", "cash_or_qpay_ref"))

    if any(v is not None for v in [total_fee, fee_paid, cc_fee, receipt]):
        existing_pay = conn.execute("SELECT id FROM payments WHERE return_id=?", (ret_id,)).fetchone()
        if existing_pay:
            conn.execute(
                """UPDATE payments SET
                   total_fee=COALESCE(?,total_fee), fee_paid=COALESCE(?,fee_paid),
                   cc_fee=COALESCE(?,cc_fee), receipt_number=COALESCE(?,receipt_number),
                   zelle_or_check_ref=COALESCE(?,zelle_or_check_ref),
                   cash_or_qpay_ref=COALESCE(?,cash_or_qpay_ref)
                   WHERE return_id=?""",
                (total_fee, fee_paid, cc_fee, receipt, zelle, cash, ret_id),
            )
        else:
            conn.execute(
                """INSERT INTO payments
                   (return_id, total_fee, fee_paid, cc_fee, receipt_number,
                    zelle_or_check_ref, cash_or_qpay_ref)
                   VALUES (?,?,?,?,?,?,?)""",
                (ret_id, total_fee, fee_paid, cc_fee, receipt, zelle, cash),
            )

    # ── Note ──────────────────────────────────────────────────────────────────
    note_text = normalize_string(g("notes", "note_text"))
    if note_text:
        dup = conn.execute(
            "SELECT id FROM notes WHERE return_id=? AND lower(note_text)=lower(?)",
            (ret_id, note_text),
        ).fetchone()
        if not dup:
            conn.execute(
                "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,'CSV_UPLOAD',?)",
                (ret_id, note_text, ts),
            )


# ── Export ────────────────────────────────────────────────────────────────────

@app.route("/export")
@role_required("admin")
def export_excel():
    """Export the current filtered view as an .xlsx file."""
    import io
    import openpyxl
    from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
    from openpyxl.utils import get_column_letter

    year = int(request.args.get("year", date.today().year))
    filters = {
        "year":        year,
        "status":      request.args.getlist("status") or None,
        "processor":   request.args.get("processor"),
        "balance_due": request.args.get("balance_due"),
        "late_intake": request.args.get("late_intake"),
        "slow_cycle":  request.args.get("slow_cycle"),
        "form":        request.args.get("form"),
        "reject_contact": request.args.get("reject_contact"),
        "q":           request.args.get("q"),
        "scan_deferred": (
            "1"
            if (request.args.get("filter") or "").strip().lower() == "scan_deferred"
            or request.args.get("scan_deferred")
            else None
        ),
    }
    rows = query_returns(filters)

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = f"TaxOps {year}"

    # ── Styles ────────────────────────────────────────────────────────────────
    HEADER_FILL  = PatternFill("solid", fgColor="1E293B")
    HEADER_FONT  = Font(name="Calibri", bold=True, color="FFFFFF", size=11)
    DATA_FONT    = Font(name="Calibri", size=11)
    BOLD_FONT    = Font(name="Calibri", bold=True, size=11)
    CENTER       = Alignment(horizontal="center", vertical="center")
    LEFT         = Alignment(horizontal="left",   vertical="center", wrap_text=False)
    MONEY        = '#,##0.00'
    thin         = Side(style="thin", color="E2E8F0")
    BORDER       = Border(bottom=thin)

    STATUS_COLORS = {
        "PROCESSING":  "E0F2FE",
        "FINALIZE":    "FEF9C3", "PICKUP":      "CCFBF1",
        "EFILE READY": "E0E7FF", "EFILE":       "EDE9FE",
        "LOG OUT":     "F1F5F9",
    }

    # ── Header row ────────────────────────────────────────────────────────────
    COLUMNS = [
        ("Log #",        9),  ("Last Name",    22), ("First Name",   18),
        ("Year",         7),  ("Status",       14), ("Preparer",     12),
        ("Forms",        18), ("Intake Date",  13), ("Pickup Date",  13),
        ("Logout Date",  13), ("Total Fee",    12), ("Fee Paid",     12),
        ("Balance",      12), ("Receipt #",    13), ("✓",             5),
    ]
    for col_idx, (label, width) in enumerate(COLUMNS, start=1):
        cell = ws.cell(row=1, column=col_idx, value=label)
        cell.font      = HEADER_FONT
        cell.fill      = HEADER_FILL
        cell.alignment = CENTER
        ws.column_dimensions[get_column_letter(col_idx)].width = width

    ws.row_dimensions[1].height = 22
    ws.freeze_panes = "A2"

    # ── Data rows ─────────────────────────────────────────────────────────────
    for row_idx, r in enumerate(rows, start=2):
        status  = r.get("client_status") or ""
        fill_hex = STATUS_COLORS.get(status, "FFFFFF")
        row_fill = PatternFill("solid", fgColor=fill_hex)

        forms_str = "  ".join(r.get("forms") or [])
        balance   = r.get("balance") or 0
        total_fee = r.get("total_fee") or 0
        fee_paid  = r.get("fee_paid") or 0

        values = [
            r.get("log_number") or "",
            r.get("last_name")  or "",
            r.get("first_name") or "",
            r.get("tax_year")   or "",
            status,
            r.get("processor")  or "",
            forms_str,
            r.get("intake_date")  or "",
            r.get("pickup_date")  or "",
            r.get("logout_date")  or "",
            total_fee,
            fee_paid,
            balance,
            r.get("receipt_number") or "",
            "✓" if r.get("verified") else "",
        ]

        for col_idx, value in enumerate(values, start=1):
            cell = ws.cell(row=row_idx, column=col_idx, value=value)
            cell.fill   = row_fill
            cell.border = BORDER
            cell.font   = DATA_FONT
            # Money columns
            if col_idx in (11, 12, 13) and isinstance(value, (int, float)) and value:
                cell.number_format = MONEY
                cell.alignment     = Alignment(horizontal="right", vertical="center")
                if col_idx == 13 and balance > 0:
                    cell.font = Font(name="Calibri", size=11, bold=True, color="DC2626")
            elif col_idx == 1:
                cell.font      = Font(name="Calibri", bold=True, size=11)
                cell.alignment = CENTER
            elif col_idx in (4, 15):
                cell.alignment = CENTER
            else:
                cell.alignment = LEFT

        ws.row_dimensions[row_idx].height = 18

    # ── Auto-filter ───────────────────────────────────────────────────────────
    ws.auto_filter.ref = f"A1:{get_column_letter(len(COLUMNS))}1"

    # ── Footer summary ────────────────────────────────────────────────────────
    footer_row = len(rows) + 2
    ws.cell(row=footer_row, column=10, value="TOTALS").font = BOLD_FONT
    total_fee_sum = sum(r.get("total_fee") or 0 for r in rows)
    fee_paid_sum  = sum(r.get("fee_paid")  or 0 for r in rows)
    balance_sum   = sum(r.get("balance")   or 0 for r in rows)
    for col_idx, val in [(11, total_fee_sum), (12, fee_paid_sum), (13, balance_sum)]:
        c = ws.cell(row=footer_row, column=col_idx, value=val)
        c.font         = BOLD_FONT
        c.number_format = MONEY
        c.alignment    = Alignment(horizontal="right", vertical="center")
        if col_idx == 13 and val > 0:
            c.font = Font(name="Calibri", bold=True, size=11, color="DC2626")

    # ── Stream to response ────────────────────────────────────────────────────
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    status_label = (filters["status"][0] if filters["status"] else "ALL").replace(" ", "-")
    filename = f"TaxOps_{year}_{status_label}.xlsx"

    from flask import send_file
    return send_file(
        buf,
        as_attachment=True,
        download_name=filename,
        mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


# ── Source compare (database vs office log + Drake files on disk) ─────────────

@app.route("/source-compare")
@role_required("admin")
def source_compare_page():
    year = int(request.args.get("year", date.today().year))
    # only=miss (default) | all — so "show all returns" is stable after form submit
    only_mismatch = (request.args.get("only", "miss") or "miss") != "all"
    m_arg = (request.args.get("manual") or "").strip()
    d_arg = (request.args.get("drake") or "").strip()

    m_path, d_path = discover_default_paths(year)
    req_err: str | None = None
    if m_arg:
        p = safe_resolve_csv(m_arg)
        if p is None:
            req_err = f"Unknown manual file {m_arg!r} (put it in data/incoming or data/processed)"
        else:
            m_path = p
    if d_arg:
        p = safe_resolve_csv(d_arg)
        if p is None:
            extra = f"Unknown Drake file {d_arg!r} (put it in data/incoming or data/processed)"
            req_err = f"{req_err} · {extra}" if req_err else extra
        else:
            d_path = p

    conn = get_connection()
    if req_err:
        n = conn.execute(
            "SELECT COUNT(*) c FROM returns WHERE tax_year=?", (year,)
        ).fetchone()["c"]
        conn.close()
        rep = {
            "year": year,
            "error": req_err,
            "file_note": None,
            "rows": [],
            "manual_file": m_arg,
            "drake_file": d_arg,
            "db_count": n,
            "summary": {
                "db_returns": n,
                "manual_matched": 0,
                "manual_rows_matched_other_years": 0,
                "drake_matched": 0,
                "manual_orphan_rows": 0,
                "manual_orphan_parse": 0,
                "manual_orphan_ambiguous": 0,
                "manual_orphan_no_db": 0,
                "manual_review_pending_rows": 0,
                "manual_orphans_other_ty_rows": 0,
                "manual_other_ty_ambiguous": 0,
                "manual_other_ty_no_db": 0,
                "drake_orphan_rows": 0,
            },
            "manual_orphans": [],
            "manual_orphans_other_ty": [],
            "drake_orphans": [],
        }
    else:
        rep = run_compare(
            conn, year, m_path, d_path, only_mismatch=only_mismatch
        )
        conn.close()

    if privacy_mode_enabled() and not req_err:
        for r in rep.get("rows") or []:
            r["name"] = "XXXXX"
    ctx = base_ctx(year=year)
    ctx.update(
        {
            "active_page": "source_compare",
            "cmp": rep,
            "only_mismatch": only_mismatch,
            "csv_list": list_csv_basenames(),
            "manual_param": m_arg,
            "drake_param": d_arg,
        }
    )
    return render_template("source_compare.html", **ctx)


# ── Source compare: apply a source's values to the database ──────────────────
# Which compare fields map to which table / column, and their types.
_APPLY_FIELDS: Dict[str, Dict[str, str]] = {
    # returns table
    "client_status": {"table": "returns", "col": "client_status", "type": "status"},
    "processor":     {"table": "returns", "col": "processor",     "type": "str"},
    "intake_date":   {"table": "returns", "col": "intake_date",   "type": "date"},
    "logout_date":   {"table": "returns", "col": "logout_date",   "type": "date"},
    "updated_date":  {"table": "returns", "col": "updated_date",  "type": "date"},
    "date_emailed":  {"table": "returns", "col": "date_emailed",  "type": "date"},
    "pickup_date":   {"table": "returns", "col": "pickup_date",   "type": "date"},
    "efile_date":    {"table": "returns", "col": "efile_date",    "type": "date"},
    "ack_date":      {"table": "returns", "col": "ack_date",      "type": "date"},
    "drake_status_raw": {"table": "returns", "col": "drake_status_raw", "type": "str"},
    "verified":      {"table": "returns", "col": "verified",      "type": "bool"},
    "is_extension":  {"table": "returns", "col": "is_extension",  "type": "bool"},
    # payments table
    "total_fee":     {"table": "payments", "col": "total_fee",    "type": "currency"},
    "fee_paid":      {"table": "payments", "col": "fee_paid",     "type": "currency"},
    "refund_amount": {"table": "payments", "col": "refund_amount","type": "currency"},
    "balance_due":   {"table": "payments", "col": "balance_due",  "type": "currency"},
}
# Read-only in compare — matched by, or client-level; not writable here
_READONLY_COMPARE_FIELDS = {"log_number", "last_name", "first_name"}
# These fields may only be applied from the manual log, never from Drake
_MANUAL_ONLY_APPLY_FIELDS = {"client_status", "drake_status_raw"}


def _coerce_apply_value(raw: str, ftype: str):
    """Convert a string value from the compare table into a DB-ready Python value."""
    from normalizer import canonical_status
    s = (raw or "").strip()
    if not s or s == "—":
        return None
    if ftype == "status":
        return canonical_status(s)
    if ftype == "date":
        val, _ = normalize_date(s)
        return val
    if ftype == "currency":
        try:
            return float(s.replace("$", "").replace(",", ""))
        except ValueError:
            return None
    if ftype == "bool":
        return 1 if s in {"1", "true", "True", "yes", "Yes", "YES"} else 0
    return s or None


@app.route("/api/source-compare/apply", methods=["POST"])
@role_required("admin")
def source_compare_apply():
    """Apply selected source values (manual or drake) for a single return to the DB."""
    body = request.get_json(silent=True) or {}
    return_id = body.get("return_id")
    # source: 'manual' | 'drake' — lets endpoint enforce manual-only restrictions
    source: str = (body.get("source") or "manual").strip().lower()
    # fields: {field_key: value_string}  — only the fields the user chose to apply
    fields: Dict[str, str] = body.get("fields") or {}

    if not return_id:
        return jsonify({"error": "return_id required"}), 400
    if not fields:
        return jsonify({"error": "No fields to apply"}), 400

    conn = get_connection()
    ret = conn.execute("SELECT id FROM returns WHERE id=?", (int(return_id),)).fetchone()
    if not ret:
        conn.close()
        return jsonify({"error": "Return not found"}), 404

    # Check whether the current DB status is locked (CANCELLED).
    current_status_row = conn.execute(
        "SELECT client_status FROM returns WHERE id=?", (int(return_id),)
    ).fetchone()
    db_status_locked = is_locked_status(
        current_status_row["client_status"] if current_status_row else None
    )

    returns_updates: Dict[str, Any] = {}
    payments_updates: Dict[str, Any] = {}
    skipped: List[str] = []

    for field, raw_val in fields.items():
        if field in _READONLY_COMPARE_FIELDS:
            skipped.append(field)
            continue
        # Status fields are manual-log only — Drake's formatting is non-standard
        if source == "drake" and field in _MANUAL_ONLY_APPLY_FIELDS:
            skipped.append(field)
            continue
        # CANCELLED is terminal — block any attempt to overwrite it via the UI.
        # Staff must edit the return directly to reverse a cancellation.
        if field == "client_status" and db_status_locked:
            conn.close()
            return jsonify({
                "error": "This return is CANCELLED — status cannot be changed via source compare. "
                         "Open the return record to manually reverse the cancellation."
            }), 409
        meta = _APPLY_FIELDS.get(field)
        if not meta:
            skipped.append(field)
            continue
        coerced = _coerce_apply_value(str(raw_val), meta["type"])
        if coerced is None:
            skipped.append(field)
            continue
        if meta["table"] == "returns":
            returns_updates[meta["col"]] = coerced
        else:
            payments_updates[meta["col"]] = coerced

    ts = now()
    if returns_updates:
        set_clause = ", ".join(f"{col}=?" for col in returns_updates)
        vals = list(returns_updates.values()) + [ts, int(return_id)]
        conn.execute(f"UPDATE returns SET {set_clause}, updated_at=? WHERE id=?", vals)

    if payments_updates:
        existing_pay = conn.execute(
            "SELECT id FROM payments WHERE return_id=?", (int(return_id),)
        ).fetchone()
        if existing_pay:
            set_clause = ", ".join(f"{col}=?" for col in payments_updates)
            vals = list(payments_updates.values()) + [int(return_id)]
            conn.execute(f"UPDATE payments SET {set_clause} WHERE return_id=?", vals)
        else:
            cols = ", ".join(["return_id"] + list(payments_updates))
            placeholders = ", ".join(["?"] * (1 + len(payments_updates)))
            vals = [int(return_id)] + list(payments_updates.values())
            conn.execute(f"INSERT INTO payments ({cols}) VALUES ({placeholders})", vals)

    conn.commit()
    conn.close()

    return jsonify({
        "ok": True,
        "applied": list(returns_updates) + list(payments_updates),
        "skipped": skipped,
    })


# ── Intake log (chronological register) ───────────────────────────────────────

@app.route("/review")
@role_required("admin")
def review_queue_page():
    conn = get_connection()
    items = conn.execute(
        """SELECT rq.*,
                  c.last_name  AS db_last,
                  c.first_name AS db_first
           FROM review_queue rq
           LEFT JOIN clients c ON c.id = rq.proposed_client_id
           WHERE rq.status = 'pending'
           ORDER BY rq.id ASC"""
    ).fetchall()
    conn.close()
    ctx = base_ctx()
    ctx.update({"active_page": "review", "items": [dict(i) for i in items]})
    return render_template("review_queue.html", **ctx)


@app.route("/review/resolve", methods=["POST"])
@role_required("admin")
def review_resolve():
    """Staff resolves a review_queue item.

    JSON body:
      queue_id  : int
      action    : 'confirm' | 'new' | 'link'
      client_id : int  (required for 'link'; ignored otherwise)
    """
    data     = _get_json_safe()
    queue_id = int(data.get("queue_id", 0))
    action   = data.get("action", "")   # confirm | new | link
    override_client_id = data.get("client_id")  # for 'link'

    conn = get_connection()
    item = conn.execute(
        "SELECT * FROM review_queue WHERE id=? AND status='pending'", (queue_id,)
    ).fetchone()

    if not item:
        conn.close()
        return jsonify({"error": "Item not found or already resolved"}), 404

    row_data  = json.loads(item["raw_json"])
    ts        = now()
    today_iso = date.today().isoformat()

    try:
        if action == "new":
            # Force-create a brand-new client by wiping the cache entry
            forced_cache: list = []
            stats = {"created": 0, "updated": 0, "skipped": 0, "review": 0, "errors": []}
            _import_row_forced(conn, row_data, item["csv_year"] or date.today().year,
                               ts, today_iso, stats, client_id=None)

        elif action in ("confirm", "link"):
            cid = override_client_id if action == "link" else item["proposed_client_id"]
            stats = {"created": 0, "updated": 0, "skipped": 0, "review": 0, "errors": []}
            _import_row_forced(conn, row_data, item["csv_year"] or date.today().year,
                               ts, today_iso, stats, client_id=int(cid))
        else:
            conn.close()
            return jsonify({"error": f"Unknown action '{action}'"}), 400

        # Mark resolved
        resolved_cid = override_client_id if action == "link" else (
            item["proposed_client_id"] if action == "confirm" else None
        )
        conn.execute(
            "UPDATE review_queue SET status=?, resolved_client_id=?, resolved_at=? WHERE id=?",
            (action, resolved_cid, ts, queue_id),
        )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        conn.close()
        return jsonify({"error": str(exc)}), 500

    # Return remaining pending count
    remaining = conn.execute(
        "SELECT COUNT(*) n FROM review_queue WHERE status='pending'"
    ).fetchone()["n"]
    conn.close()
    return jsonify({"ok": True, "remaining": remaining, "stats": stats})


@app.route("/intake-log")
@login_required
def intake_log():
    year = int(request.args.get("year", date.today().year))
    conn = get_connection()
    rows = conn.execute(
        f"""
        {_SELECT}
        WHERE (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?))
        ORDER BY CAST(r.log_number AS INTEGER) ASC
        """,
        (str(year), year - 1),
    ).fetchall()
    conn.close()

    enriched = [_enrich(dict(r)) for r in rows]

    # Group by intake date
    from collections import defaultdict
    groups: dict[str, list] = defaultdict(list)
    for r in enriched:
        key = r.get("intake_date") or "No Date"
        groups[key].append(r)

    sorted_groups = sorted(groups.items(), key=lambda x: x[0])

    ctx = base_ctx(year)
    ctx.update({
        "active_page":  "intake_log",
        "groups":       sorted_groups,
        "total":        len(enriched),
    })
    return render_template("intake_log.html", **ctx)



# ── JSON API ──────────────────────────────────────────────────────────────────


@app.post("/api/client-error")
@login_required
def api_client_error():
    """PROD-6: log frontend (vanilla JS) errors without taking down the Flask process."""
    data = request.get_json(silent=True)
    if data is None or not isinstance(data, dict):
        return jsonify({"ok": False, "error": "expected_json_object"}), 400

    kind = str(data.get("kind") or "unknown")[:64]
    message = str(data.get("message") or "")[:2000]
    page_url = str(data.get("page_url") or "")[:2000]
    filename = str(data.get("filename") or "")[:500]
    stack = str(data.get("stack") or "")[:8000]
    lineno = data.get("lineno")
    colno = data.get("colno")

    frontend_log = logging.getLogger("taxops.frontend")
    frontend_log.warning(
        "CLIENT_JS[%s]: %s | url=%r file=%r line=%s col=%s\n%s",
        kind,
        message.replace("\r", " ").replace("\n", " ")[:480],
        page_url[:400],
        filename,
        lineno,
        colno,
        stack.replace("\r", "").strip()[:2500],
    )
    return jsonify({"ok": True})


@app.get("/api/clients/search")
@login_required
def api_client_search():
    """Search existing clients by name for re-intake prefill."""
    q = (request.args.get("q") or "").strip()
    if len(q) < 2:
        return jsonify([])
    qp = f"%{q.lower()}%"
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT c.id, c.last_name, c.first_name, c.display_name,
               c.spouse_first_name, c.spouse_last_name,
               MAX(r.tax_year) AS last_year
        FROM clients c
        LEFT JOIN returns r ON r.client_id = c.id
        WHERE lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ?
           OR lower(COALESCE(c.display_name,'')) LIKE ?
           OR lower(COALESCE(c.spouse_first_name,'')) LIKE ?
           OR lower(COALESCE(c.spouse_last_name,'')) LIKE ?
        GROUP BY c.id
        ORDER BY c.last_name, c.first_name
        LIMIT 12
        """,
        (qp, qp, qp, qp, qp),
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        name = _build_name_full(
            r["first_name"] or "", r["last_name"] or "",
            r["display_name"] or "",
            r["spouse_first_name"] or "", r["spouse_last_name"] or "",
        )
        if privacy_mode_enabled():
            name = f"XXXXX #{r['id']}"
        results.append({
            "id":        r["id"],
            "name":      name,
            "last_year": r["last_year"],
        })
    return jsonify(results)


@app.get("/api/clients/<int:client_id>/reintake")
@login_required
def api_client_reintake(client_id: int):
    """
    Return everything needed to pre-populate the re-intake form for a
    returning client: client fields + most recent return's data + dependents.
    SSN fields are intentionally excluded.
    """
    conn = get_connection()
    client = conn.execute("SELECT * FROM clients WHERE id=?", (client_id,)).fetchone()
    if not client:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    # Most recent return for this client
    ret = conn.execute(
        """
        SELECT r.*, rf.form_1040, rf.sched_a_d, rf.sched_c, rf.sched_e,
               rf.form_1120, rf.form_1120s, rf.form_1065_llc,
               rf.corp_officer, rf.business_owner, rf.form_990_1041
        FROM returns r
        LEFT JOIN return_forms rf ON rf.return_id = r.id
        WHERE r.client_id = ?
        ORDER BY r.tax_year DESC, r.id DESC
        LIMIT 1
        """,
        (client_id,),
    ).fetchone()

    # Dependents from that return
    deps = []
    if ret:
        deps = [dict(d) for d in conn.execute(
            "SELECT * FROM dependents WHERE return_id = ? ORDER BY id",
            (ret["id"],),
        ).fetchall()]
        # strip SSN from dependents too
        for d in deps:
            d.pop("ssn_last4", None)
            if privacy_mode_enabled():
                d["full_name"] = _mask_value(d.get("full_name"))
                d["relationship"] = _mask_value(d.get("relationship"))

    # DEP-IMPORT: if no return-level dependents, fall back to Drake-imported
    # client_dependents so TY2026 intake pre-populates from TY2025 data.
    drake_deps = []
    if not deps:
        cd_rows = conn.execute(
            """
            SELECT id, first_name, last_name, date_of_birth, relationship,
                   is_claimed_dependent, hoh_qualifier_only, source
            FROM client_dependents
            WHERE client_id = ?
              AND is_claimed_dependent = 1
              AND removed_for_ty2026 = 0
              AND needs_review = 0
            ORDER BY id
            """,
            (client_id,),
        ).fetchall()
        for cd in cd_rows:
            full = " ".join(filter(None, [cd["first_name"], cd["last_name"]]))
            entry = {
                "full_name":       _mask_value(full) if privacy_mode_enabled() else full,
                "relationship":    _mask_value(cd["relationship"]) if privacy_mode_enabled() else cd["relationship"],
                "date_of_birth":   cd["date_of_birth"],
                "medi_cal":        0,
                "on_medicare":     0,
                "from_drake":      True,
                "source":          cd["source"],
            }
            drake_deps.append(entry)

    # SPOUSES: fall back to Drake-imported spouses table for any fields missing
    # from the clients row (needs_review=0 only).
    # – If no spouse name at all: pull name + DOB from spouses table.
    # – If spouse name exists but DOB is blank: pull DOB from spouses table.
    drake_spouse = None
    client_has_spouse = bool(
        (client["spouse_first_name"] or "").strip()
        or (client["spouse_last_name"] or "").strip()
    )
    client_has_spouse_dob = bool((client["spouse_dob"] or "").strip())

    # Always fetch spouse row — needed for id_type flags regardless of prefill
    _sp_row_full = conn.execute(
        """
        SELECT first_name, last_name, derived_last_name,
               middle_initial, date_of_birth, source, id_type
        FROM spouses
        WHERE client_id = ? AND needs_review = 0
        """,
        (client_id,),
    ).fetchone()

    if not client_has_spouse or not client_has_spouse_dob:
        sp_row = _sp_row_full
        if sp_row:
            if not client_has_spouse:
                # No spouse at all — provide full name + DOB
                sp_first = sp_row["first_name"] or ""
                sp_last  = sp_row["derived_last_name"] or sp_row["last_name"] or ""
                sp_mid   = sp_row["middle_initial"] or ""
                sp_dob   = sp_row["date_of_birth"] or ""
                drake_spouse = {
                    "spouse_first_name":    _mask_value(sp_first) if privacy_mode_enabled() else sp_first,
                    "spouse_last_name":     _mask_value(sp_last)  if privacy_mode_enabled() else sp_last,
                    "spouse_middle_initial": sp_mid,
                    "spouse_dob":           sp_dob,
                    "source":               sp_row["source"],
                }
            else:
                # Spouse name already in clients table — only fill missing DOB
                sp_dob = sp_row["date_of_birth"] or ""
                if sp_dob:
                    drake_spouse = {
                        "spouse_dob": sp_dob,
                        "source":     sp_row["source"],
                    }

    habit_profile = build_client_habit_profile(conn, client_id)
    conn.close()

    data = dict(client)
    data.pop("ssn_last4", None)
    if privacy_mode_enabled():
        data = _mask_client_payload(data)

    last_return = {}
    if ret:
        last_return = dict(ret)
        last_return.pop("ssn_last4", None)
        if privacy_mode_enabled():
            last_return = _mask_return_payload(last_return)

    # ID-type flags for intake warnings
    spouse_id_type  = _sp_row_full["id_type"]  if _sp_row_full else None
    spouse_first_flag = (
        _sp_row_full["first_name"] or ""
        if _sp_row_full else (client["spouse_first_name"] or "")
    ).strip() or None

    return jsonify({
        "client":      data,
        "last_return": last_return,
        "dependents":  deps if deps else drake_deps,
        "drake_deps_prefilled": bool(drake_deps) and not bool(deps),
        "drake_spouse": drake_spouse,
        "habit_profile": habit_profile,
        "has_spouse_row":  bool(_sp_row_full),
        "spouse_id_type":  spouse_id_type,
        "spouse_first_for_flag": spouse_first_flag,
    })


@app.get("/api/clients/<int:client_id>/years")
@login_required
def api_client_year_comparison(client_id: int):
    """MULTIYEAR-6 — normalized 2–3 year comparison payload for the client."""
    raw = request.args.get("years") or ""
    years_asc, err = multiyear_comparison.parse_years_param(raw)
    if err:
        return jsonify({"error": err}), 400

    conn = get_connection()
    try:
        cli = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
        if not cli:
            return jsonify({"error": "Not found"}), 404
        cli_d = dict(cli)
        display = profile_title_for_client(cli_d, client_id, privacy=privacy_mode_enabled())
        thresholds = {
            "agi_percent": float(MULTIYEAR_AGI_PERCENT_THRESHOLD),
            "refund_abs": float(MULTIYEAR_REFUND_ABS_THRESHOLD),
            "balance_abs": float(MULTIYEAR_BALANCE_ABS_THRESHOLD),
        }
        payload = multiyear_comparison.build_client_year_comparison_payload(
            conn,
            client_id=client_id,
            years_asc=years_asc or [],
            thresholds=thresholds,
            client_display_name=display,
            privacy_mode=privacy_mode_enabled(),
        )
        return jsonify(payload)
    finally:
        conn.close()


@app.get("/clients/<int:client_id>/years")
@login_required
def legacy_client_comparison_years(client_id: int):
    """MULTIYEAR-6 — epic path aliases the JSON API."""
    return api_client_year_comparison(client_id)


@app.get("/api/clients/<int:client_id>/years.pdf")
@login_required
def api_client_year_comparison_pdf(client_id: int):
    """MULTIYEAR-5 — same data as JSON route, formatted for preparer/client summary."""
    raw = request.args.get("years") or ""
    years_asc, err = multiyear_comparison.parse_years_param(raw)
    if err:
        return jsonify({"error": err}), 400

    conn = get_connection()
    try:
        cli = conn.execute("SELECT * FROM clients WHERE id = ?", (client_id,)).fetchone()
        if not cli:
            return jsonify({"error": "Not found"}), 404
        cli_d = dict(cli)
        display = profile_title_for_client(cli_d, client_id, privacy=privacy_mode_enabled())
        thresholds = {
            "agi_percent": float(MULTIYEAR_AGI_PERCENT_THRESHOLD),
            "refund_abs": float(MULTIYEAR_REFUND_ABS_THRESHOLD),
            "balance_abs": float(MULTIYEAR_BALANCE_ABS_THRESHOLD),
        }
        payload = multiyear_comparison.build_client_year_comparison_payload(
            conn,
            client_id=client_id,
            years_asc=years_asc or [],
            thresholds=thresholds,
            client_display_name=display,
            privacy_mode=privacy_mode_enabled(),
        )
    finally:
        conn.close()

    pdf_bytes = multiyear_comparison.render_year_comparison_pdf(payload)
    fname = f"client_{client_id}_year_comparison.pdf"
    return Response(
        pdf_bytes,
        mimetype="application/pdf",
        headers={"Content-Disposition": f'inline; filename="{fname}"'},
    )


@app.get("/api/search")
@login_required
def api_search():
    q    = request.args.get("q", "").strip()
    year = int(request.args.get("year", date.today().year))
    if not q:
        return jsonify([])
    results = query_returns({"year": year, "q": q})
    deduped: list[dict] = []
    seen: set[int] = set()
    for r in results:
        cid = r.get("client_id")
        if cid is None or cid in seen:
            continue
        seen.add(int(cid))
        deduped.append(r)
        if len(deduped) >= 12:
            break
    priv = privacy_mode_enabled()
    return jsonify([
        {
            "id":          r["id"],
            "client_id":   r["client_id"],
            "log_number":  r["log_number"],
            "name":        (f"XXXXX #{r['client_id']}" if priv else r["name_full"]),
            "status":      r["client_status"],
            "badge":       r["badge_class"],
            "tax_year":    r["tax_year"],
        }
        for r in deduped
    ])


@app.post("/api/privacy-mode")
@login_required
def api_privacy_mode():
    data = _get_json_safe() if request.data else {}
    enabled = data.get("enabled")
    session["privacy_mode"] = bool(enabled)
    return jsonify({"success": True, "privacy_mode": bool(session.get("privacy_mode"))})


@app.get("/api/filters")
@login_required
def api_dashboard_filters_list():
    uname = _session_username()
    if not uname:
        return jsonify({"error": "No user in session"}), 400
    year = int(request.args.get("year", date.today().year))
    rows = _saved_dashboard_filters_payload(uname, year)
    return jsonify({"filters": rows, "year": year})


@app.post("/api/filters")
@login_required
def api_dashboard_filters_create():
    uname = _session_username()
    if not uname:
        return jsonify({"error": "No user in session"}), 400
    data = _get_json_safe() if request.data else {}
    name = str(data.get("name") or "").strip()[:120]
    if not name:
        return jsonify({"error": "Name is required"}), 400

    fd = _sanitize_dashboard_filter_payload(data.get("filter") or {})
    if not _dashboard_saved_filter_meaningful(fd):
        return jsonify({"error": "Filter has no criteria — pick status, preparer, or another filter first"}), 400

    wants_shared = bool(data.get("is_shared"))
    if wants_shared and not can_publish_shared_dashboard_filters():
        return jsonify({"error": "Not allowed to publish shared filters"}), 403

    set_default = bool(data.get("set_default")) and not wants_shared
    ts = now()
    year_hint = int(data.get("year", date.today().year))

    new_id = None
    conn = get_connection()
    try:
        cur = conn.execute(
            """
            INSERT INTO dashboard_saved_filters (user_id, name, filter_json, is_default, is_shared, created_at)
            VALUES (?, ?, ?, 0, ?, ?)
            """,
            (
                uname,
                name,
                json.dumps(fd, separators=(",", ":"), sort_keys=True),
                1 if wants_shared else 0,
                ts,
            ),
        )
        new_id = cur.lastrowid
        if set_default and new_id:
            conn.execute(
                "UPDATE dashboard_saved_filters SET is_default = 0 WHERE user_id = ? AND is_shared = 0",
                (uname,),
            )
            conn.execute(
                "UPDATE dashboard_saved_filters SET is_default = 1 WHERE id = ?",
                (new_id,),
            )
        conn.commit()
    finally:
        conn.close()

    return jsonify({
        "success":      True,
        "id":           new_id,
        "query_string": _dashboard_filter_query_string(fd, year_hint),
        "filter":       fd,
        "set_default":  set_default,
    })


@app.delete("/api/filters/<int:fid>")
@login_required
def api_dashboard_filters_delete(fid: int):
    uname = _session_username()
    if not uname:
        return jsonify({"error": "No user in session"}), 400
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT user_id, is_shared FROM dashboard_saved_filters WHERE id = ?",
            (fid,),
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        is_shared = bool(row["is_shared"])
        uid = row["user_id"]
        if is_shared:
            if not can_publish_shared_dashboard_filters():
                return jsonify({"error": "Forbidden"}), 403
        elif uid != uname:
            return jsonify({"error": "Forbidden"}), 403
        conn.execute("DELETE FROM dashboard_saved_filters WHERE id = ?", (fid,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.post("/api/filters/<int:fid>/default")
@login_required
def api_dashboard_filters_set_default(fid: int):
    uname = _session_username()
    if not uname:
        return jsonify({"error": "No user in session"}), 400
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT user_id, is_shared FROM dashboard_saved_filters WHERE id = ?",
            (fid,),
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        if bool(row["is_shared"]) or row["user_id"] != uname:
            return jsonify({"error": "Forbidden"}), 403
        conn.execute(
            "UPDATE dashboard_saved_filters SET is_default = 0 WHERE user_id = ? AND is_shared = 0",
            (uname,),
        )
        conn.execute(
            "UPDATE dashboard_saved_filters SET is_default = 1 "
            "WHERE id = ? AND user_id = ? AND is_shared = 0",
            (fid, uname),
        )
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@app.post("/api/return/<int:return_id>/sync-to-drake")
@login_required
def api_return_sync_to_drake(return_id: int):
    """
    DOC-6 — Push one return's documents into DRAKE_DOCUMENTS_PATH staging (copy only).

    Existing TaxOps ``return_documents.file_path`` files are unchanged; no Drake API calls.
    """
    import config as _cfg

    if not (_cfg.DRAKE_DOCUMENTS_PATH or "").strip():
        return jsonify(
            {
                "error": "DRAKE_DOCUMENTS_PATH is not configured.",
                "hint": "Set the DRAKE_DOCUMENTS_PATH env var to your Drake Documents data root.",
            }
        ), 400
    outcome = sync_to_drake(return_id)
    status = 200 if outcome.get("success") else 400
    if outcome.get("error") == "return_not_found":
        status = 404
    return jsonify(outcome), status


@app.post("/api/return/<int:return_id>/status")
@login_required
def api_status(return_id: int):
    data       = _get_json_safe()
    new_status = (data.get("status") or "").upper().strip()
    if new_status not in STATUS_FLOW:
        return jsonify({"error": "Invalid status"}), 400

    conn = get_connection()
    row  = conn.execute("SELECT * FROM returns WHERE id=?", (return_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    old_status  = row["client_status"]

    # Receptionist may only make desk-safe transitions; enforce server-side.
    if get_effective_role() == "receptionist":
        if not receptionist_may_set_status(old_status, new_status):
            conn.close()
            return jsonify({"error": "forbidden", "required_role": "preparer"}), 403
    timestamp   = now()
    today_iso   = date.today().isoformat()
    date_field  = STATUS_DATE_STAMP.get(new_status)

    if date_field and not row[date_field]:
        conn.execute(
            f"UPDATE returns SET client_status=?, {date_field}=?, updated_at=? WHERE id=?",
            (new_status, today_iso, timestamp, return_id),
        )
    else:
        conn.execute(
            "UPDATE returns SET client_status=?, updated_at=? WHERE id=?",
            (new_status, timestamp, return_id),
        )

    # Deduplicated status event
    exists = conn.execute(
        "SELECT id FROM status_events WHERE return_id=? AND event_type='STATUS_CHANGED' AND event_timestamp=?",
        (return_id, timestamp),
    ).fetchone()
    if not exists:
        conn.execute(
            """
            INSERT INTO status_events
              (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
            VALUES (?, 'STATUS_CHANGED', ?, ?, ?, 'APP', 'Updated via app')
            """,
            (return_id, old_status, new_status, timestamp),
        )

    if new_status == "REJECTED":
        conn.execute(
            "UPDATE returns SET contact_status='not_contacted', last_contacted_date=NULL, updated_at=? WHERE id=?",
            (timestamp, return_id),
        )
    elif old_status == "REJECTED" and new_status != "REJECTED":
        conn.execute(
            "UPDATE returns SET contact_status=NULL, last_contacted_date=NULL, updated_at=? WHERE id=?",
            (timestamp, return_id),
        )

    conn.commit()
    conn.close()
    return jsonify({
        "success":       True,
        "client_status": new_status,
        "badge_class":   STATUS_BADGE.get(new_status, "sb-default"),
    })


BULK_RETURN_IDS_CAP = 500


@app.post("/api/returns/bulk-status")
@role_required("preparer")
def api_returns_bulk_status():
    payload    = _get_json_safe() or {}
    raw_ids    = payload.get("return_ids")
    new_status = (payload.get("status") or "").strip().upper()
    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"error": "return_ids required (non-empty list)"}), 400
    if len(raw_ids) > BULK_RETURN_IDS_CAP:
        return jsonify({"error": f"Too many returns (max {BULK_RETURN_IDS_CAP})"}), 400
    try:
        parsed_ids = [int(x) for x in raw_ids]
    except (TypeError, ValueError):
        return jsonify({"error": "return_ids must be integers"}), 400
    if new_status not in STATUS_FLOW:
        return jsonify({"error": "Invalid status"}), 400
    actor = (session.get("username") or "").strip() or "?"
    conn  = get_connection()
    try:
        conn.execute("BEGIN")
        errors = bulk_apply_status_changes(
            conn,
            return_ids=parsed_ids,
            new_status=new_status,
            status_flow=tuple(STATUS_FLOW),
            status_date_stamp=STATUS_DATE_STAMP,
            actor_username=actor,
        )
        if errors:
            conn.rollback()
            return jsonify({"success": False, "errors": errors, "changed": 0}), 409
        conn.commit()
        return jsonify(
            {"success": True, "errors": [], "changed": len({i for i in parsed_ids})},
        )
    finally:
        conn.close()


@app.post("/api/returns/bulk-processor")
@role_required("preparer")
def api_returns_bulk_processor():
    payload       = _get_json_safe() or {}
    raw_ids       = payload.get("return_ids")
    processor_raw = payload.get("processor")
    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"error": "return_ids required (non-empty list)"}), 400
    if len(raw_ids) > BULK_RETURN_IDS_CAP:
        return jsonify({"error": f"Too many returns (max {BULK_RETURN_IDS_CAP})"}), 400
    try:
        parsed_ids = [int(x) for x in raw_ids]
    except (TypeError, ValueError):
        return jsonify({"error": "return_ids must be integers"}), 400
    actor = (session.get("username") or "").strip() or "?"
    conn  = get_connection()
    try:
        conn.execute("BEGIN")
        errors, rows_updated = bulk_apply_processor_changes(
            conn,
            return_ids=parsed_ids,
            new_processor_raw=processor_raw,
            actor_username=actor,
        )
        if errors:
            conn.rollback()
            return jsonify({"success": False, "errors": errors, "changed": 0}), 409
        conn.commit()
        return jsonify(
            {"success": True, "errors": [], "changed": rows_updated},
        )
    finally:
        conn.close()


@app.post("/api/returns/bulk-update")
@role_required("preparer")
def api_returns_bulk_update():
    """Unified bulk update: set status and/or processor in one transaction.

    Accepts JSON: {return_ids: [1,2,3], status: "PICKUP", processor: "Alice"}
    At least one of status or processor must be supplied.
    Maximum BULK_RETURN_IDS_CAP IDs per request.
    Each changed return is audited individually.
    Never returns ssn or identification fields.
    """
    payload     = _get_json_safe() or {}
    raw_ids     = payload.get("return_ids")
    new_status  = (payload.get("status") or "").strip().upper() or None
    processor   = payload.get("processor")  # may be None to leave unchanged

    if not isinstance(raw_ids, list) or not raw_ids:
        return jsonify({"error": "return_ids required (non-empty list)"}), 400
    if len(raw_ids) > BULK_RETURN_IDS_CAP:
        return jsonify({"error": f"Too many returns (max {BULK_RETURN_IDS_CAP})"}), 400
    try:
        parsed_ids = [int(x) for x in raw_ids]
    except (TypeError, ValueError):
        return jsonify({"error": "return_ids must be integers"}), 400

    if new_status is None and processor is None:
        return jsonify({"error": "At least one of status or processor must be provided"}), 400
    if new_status is not None and new_status not in STATUS_FLOW:
        return jsonify({"error": f"Invalid status. Allowed: {STATUS_FLOW}"}), 400

    actor = (session.get("username") or "").strip() or "?"
    conn  = get_connection()
    try:
        conn.execute("BEGIN")
        errors: list[str] = []
        updated_count = 0

        if new_status is not None:
            errs = bulk_apply_status_changes(
                conn,
                return_ids=parsed_ids,
                new_status=new_status,
                status_flow=tuple(STATUS_FLOW),
                status_date_stamp=STATUS_DATE_STAMP,
                actor_username=actor,
            )
            if errs:
                conn.rollback()
                return jsonify({"success": False, "errors": errs, "updated_count": 0}), 409
            updated_count = len(parsed_ids)

        if processor is not None:
            perrs, rows_updated = bulk_apply_processor_changes(
                conn,
                return_ids=parsed_ids,
                new_processor_raw=processor,
                actor_username=actor,
            )
            if perrs:
                conn.rollback()
                return jsonify({"success": False, "errors": perrs, "updated_count": 0}), 409
            updated_count = max(updated_count, rows_updated)

        conn.commit()
        return jsonify({"success": True, "updated_count": updated_count})
    finally:
        conn.close()


def _validate_field(field: str, value) -> tuple[bool, str]:
    """REL-6: coerce and validate a value for a known editable field.

    Returns (ok, coerced_value_or_error_message).  Validators are intentionally
    lenient about None/empty-string so clearing a field always works.
    """
    import re as _re

    def _is_empty(v) -> bool:
        return v is None or str(v).strip() == ""

    # INTEGER boolean flags (0/1 only; None = clear)
    _BOOL_FIELDS = {
        "verified", "is_amended", "has_w7", "is_extension",
        "transfer_flag", "transfer_2025_flag", "transfer_2026_flag",
        "signatures_given", "signatures_received", "referral_flag",
    }
    # ISO-8601 date fields (YYYY-MM-DD or empty)
    _DATE_FIELDS = {
        "intake_date", "date_emailed", "pickup_date", "logout_date",
        "updated_date", "efile_date", "ack_date", "taxpayer_dob", "spouse_dob",
        "promise_date", "date_signatures_emailed", "date_reports_emailed",
    }
    # 4-digit tax-year integer
    _YEAR_FIELDS = {"tax_year"}
    # Money (REAL >= 0)
    _MONEY_FIELDS = {"total_fee", "fee_paid", "cc_fee", "refund_amount", "bank_deposit"}

    if field in _BOOL_FIELDS:
        if _is_empty(value):
            return True, None
        try:
            v = int(value)
        except (TypeError, ValueError):
            return False, f"Field '{field}' must be 0 or 1, got {value!r}"
        if v not in (0, 1):
            return False, f"Field '{field}' must be 0 or 1, got {v!r}"
        return True, v

    if field in _DATE_FIELDS:
        if _is_empty(value):
            return True, None
        s = str(value).strip()
        if not _re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
            return False, f"Field '{field}' must be YYYY-MM-DD, got {s!r}"
        return True, s

    if field in _YEAR_FIELDS:
        if _is_empty(value):
            return True, None
        try:
            v = int(value)
        except (TypeError, ValueError):
            return False, f"Field '{field}' must be a 4-digit year, got {value!r}"
        if not (1990 <= v <= 2100):
            return False, f"Field '{field}' year {v} out of range 1990–2100"
        return True, v

    if field in _MONEY_FIELDS:
        if _is_empty(value):
            return True, None
        try:
            v = float(value)
        except (TypeError, ValueError):
            return False, f"Field '{field}' must be a number, got {value!r}"
        if v < 0:
            return False, f"Field '{field}' cannot be negative, got {v!r}"
        return True, round(v, 2)

    # Everything else (TEXT fields) — accept as-is; strip leading/trailing whitespace.
    if value is None:
        return True, None
    return True, str(value).strip() or None


@app.post("/api/return/<int:return_id>/field")
@role_required("preparer")
def api_field(return_id: int):
    data  = _get_json_safe()
    field = (data.get("field") or "").strip()
    value = data.get("value")
    if field == "processor":
        value = normalize_preparer(value) if (value is not None and str(value).strip() != "") else None
    elif field in (RETURN_EDITABLE | CLIENT_EDITABLE | PAYMENT_EDITABLE):
        ok, coerced = _validate_field(field, value)
        if not ok:
            return jsonify({"error": coerced}), 400
        value = coerced

    conn = get_connection()
    try:
        if field in RETURN_EDITABLE:
            conn.execute(
                f"UPDATE returns SET {field}=?, updated_at=? WHERE id=?",
                (value, now(), return_id),
            )
            # Auto-advance to LOG OUT when a completion date is recorded.
            # logout_date = physically logged out; ack_date = IRS accepted.
            # Either one means the engagement is closed.
            auto_logout = (
                (field == "ack_date"    and value) or
                (field == "logout_date" and value)
            )
            if auto_logout:
                cur = conn.execute(
                    "SELECT client_status FROM returns WHERE id=?", (return_id,)
                ).fetchone()
                if cur and cur["client_status"] != "LOG OUT":
                    old_status = cur["client_status"]
                    ts = now()
                    note = "Auto-advanced: ack date set" if field == "ack_date" else "Auto-advanced: logout date set"
                    conn.execute(
                        "UPDATE returns SET client_status='LOG OUT', updated_at=? WHERE id=?",
                        (ts, return_id),
                    )
                    conn.execute(
                        """INSERT INTO status_events
                           (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
                           VALUES (?, 'STATUS_CHANGED', ?, 'LOG OUT', ?, 'APP', ?)""",
                        (return_id, old_status, ts, note),
                    )
        elif field in CLIENT_EDITABLE:
            cid = conn.execute("SELECT client_id FROM returns WHERE id=?", (return_id,)).fetchone()
            if cid:
                conn.execute(
                    f"UPDATE clients SET {field}=?, updated_at=? WHERE id=?",
                    (value, now(), cid["client_id"]),
                )
        elif field in PAYMENT_EDITABLE:
            prow = conn.execute(
                "SELECT id FROM payments WHERE return_id=?", (return_id,)
            ).fetchone()
            if prow:
                conn.execute(
                    f"UPDATE payments SET {field}=? WHERE return_id=?", (value, return_id)
                )
            else:
                conn.execute(
                    f"INSERT INTO payments (return_id, {field}) VALUES (?,?)", (return_id, value)
                )
        else:
            return jsonify({"error": "Field not editable"}), 400

        conn.commit()
        return jsonify({"success": True, "field": field, "value": value})
    finally:
        conn.close()


@app.post("/api/return/<int:return_id>/note")
@role_required("preparer")
def api_note(return_id: int):
    data = _get_json_safe()
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "Empty note"}), 400

    conn = get_connection()
    dup  = conn.execute(
        "SELECT id FROM notes WHERE return_id=? AND lower(note_text)=lower(?)",
        (return_id, text),
    ).fetchone()
    if dup:
        conn.close()
        return jsonify({"error": "Duplicate note"}), 409

    ts = now()
    conn.execute(
        "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,'APP',?)",
        (return_id, text, ts),
    )
    conn.commit()
    conn.close()
    visible_text = _mask_value(text) if privacy_mode_enabled() else text
    return jsonify({"success": True, "text": visible_text, "created_at": ts})


@app.post("/api/return/<int:return_id>/contact")
@role_required("preparer")
def api_return_contact(return_id: int):
    """Update client-contact follow-up fields for REJECTED returns."""
    data  = _get_json_safe() or {}
    cs_in = (data.get("contact_status") or "").strip().lower()
    if cs_in not in CONTACT_STATUS_VALUES:
        return jsonify({"error": "Invalid contact_status"}), 400
    lcd_raw = (data.get("last_contacted_date") or "").strip() or None

    conn = get_connection()
    row  = conn.execute("SELECT client_status FROM returns WHERE id=?", (return_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    if row["client_status"] != "REJECTED":
        conn.close()
        return jsonify({"error": "Contact tracking is only for REJECTED returns"}), 400

    today_iso = date.today().isoformat()
    if cs_in == "not_contacted":
        lcd_val = None
    else:
        lcd_val = lcd_raw if lcd_raw else today_iso

    ts = now()
    conn.execute(
        "UPDATE returns SET contact_status=?, last_contacted_date=?, updated_at=? WHERE id=?",
        (cs_in, lcd_val, ts, return_id),
    )
    conn.commit()
    conn.close()
    return jsonify({
        "success": True,
        "contact_status": cs_in,
        "last_contacted_date": lcd_val,
    })


# ── Missing documents tracker ────────────────────────────────────────────────

@app.post("/api/return/<int:return_id>/missing-doc")
@role_required("preparer")
def api_missing_doc_add(return_id: int):
    data = _get_json_safe()
    text = (data.get("text") or "").strip()
    if not text:
        return jsonify({"error": "Empty item"}), 400
    ts = now()
    conn = get_connection()
    cur = conn.execute(
        "INSERT INTO missing_docs (return_id, item_text, is_resolved, created_at) VALUES (?,?,0,?)",
        (return_id, text, ts),
    )
    doc_id = cur.lastrowid
    conn.commit()
    conn.close()
    return jsonify({"success": True, "id": doc_id, "text": text, "is_resolved": 0, "created_at": ts})


@app.post("/api/return/<int:return_id>/missing-doc/<int:doc_id>/toggle")
@role_required("preparer")
def api_missing_doc_toggle(return_id: int, doc_id: int):
    conn = get_connection()
    row = conn.execute(
        "SELECT is_resolved FROM missing_docs WHERE id=? AND return_id=?", (doc_id, return_id)
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404
    new_val = 0 if row["is_resolved"] else 1
    ts = now() if new_val else None
    conn.execute(
        "UPDATE missing_docs SET is_resolved=?, resolved_at=? WHERE id=?",
        (new_val, ts, doc_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True, "is_resolved": new_val})


@app.delete("/api/return/<int:return_id>/missing-doc/<int:doc_id>")
@login_required
def api_missing_doc_delete(return_id: int, doc_id: int):
    conn = get_connection()
    conn.execute("DELETE FROM missing_docs WHERE id=? AND return_id=?", (doc_id, return_id))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# ── DEP-1: Remove dependent from return ──────────────────────────────────────

@app.delete("/api/return/<int:return_id>/dependents/<int:dep_id>")
@login_required
def api_dependent_delete(return_id: int, dep_id: int):
    user   = session.get("username")
    ip     = request.remote_addr
    ts     = now()
    conn   = get_connection()
    try:
        row = conn.execute(
            "SELECT id, full_name FROM dependents WHERE id=? AND return_id=? AND is_deleted=0",
            (dep_id, return_id),
        ).fetchone()
        if not row:
            return jsonify({"error": "Dependent not found"}), 404
        before = dict(row)
        conn.execute(
            "UPDATE dependents SET is_deleted=1 WHERE id=? AND return_id=?",
            (dep_id, return_id),
        )
        conn.commit()
    finally:
        conn.close()
    from audit_service import _enqueue_write
    _enqueue_write(
        user_id=user,
        action="DEPENDENT_REMOVED",
        entity_type="dependent",
        entity_id=str(dep_id),
        before=before,
        after={"is_deleted": 1, "return_id": return_id},
        ip_address=ip,
        http_status=200,
    )
    return jsonify({"success": True})


# ── LIFE-1: Cancel / uncancel return ─────────────────────────────────────────

@app.post("/api/return/<int:return_id>/cancel")
@role_required("preparer")
def api_cancel_return(return_id: int):
    data   = _get_json_safe()
    reason = (data.get("reason") or "").strip()
    if not reason:
        return jsonify({"error": "reason is required"}), 400
    user = session.get("username")
    ip   = request.remote_addr
    ts   = now()
    conn = get_connection()
    try:
        ret = conn.execute("SELECT client_status FROM returns WHERE id=?", (return_id,)).fetchone()
        if not ret:
            return jsonify({"error": "Return not found"}), 404
        if (ret["client_status"] or "").upper() == "CANCELLED":
            return jsonify({"error": "Already cancelled"}), 409
        pmt = conn.execute(
            "SELECT id, total_fee, fee_paid FROM payments WHERE return_id=?", (return_id,)
        ).fetchone()
        original_fee = float(pmt["total_fee"] or 0) if pmt else 0.0
        if pmt:
            conn.execute(
                "UPDATE payments SET cancelled_fee=?, total_fee=0, fee_paid=0 WHERE return_id=?",
                (original_fee, return_id),
            )
        conn.execute(
            "UPDATE returns SET client_status='CANCELLED', cancelled_fee=?, "
            "cancelled_reason=?, cancelled_at=?, updated_at=? WHERE id=?",
            (original_fee, reason, ts, ts, return_id),
        )
        note_text = f"Return cancelled: {reason} — original fee was ${original_fee:,.2f}"
        conn.execute(
            "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,'CANCEL',?)",
            (return_id, note_text, ts),
        )
        conn.execute(
            "INSERT INTO status_events (return_id, event_type, old_status, new_status, "
            "event_timestamp, source_file, note) VALUES (?, 'STATUS_CHANGED', ?, 'CANCELLED', ?, 'APP', ?)",
            (return_id, ret["client_status"], ts, reason),
        )
        conn.commit()
    finally:
        conn.close()
    from audit_service import _enqueue_write
    _enqueue_write(
        user_id=user, action="RETURN_CANCELLED", entity_type="return",
        entity_id=str(return_id), before={"client_status": ret["client_status"], "total_fee": original_fee},
        after={"client_status": "CANCELLED", "total_fee": 0, "reason": reason},
        ip_address=ip, http_status=200,
    )
    return jsonify({"success": True, "original_fee": original_fee})


@app.post("/api/return/<int:return_id>/uncancel")
@role_required("preparer")
def api_uncancel_return(return_id: int):
    user = session.get("username")
    ip   = request.remote_addr
    ts   = now()
    conn = get_connection()
    try:
        ret = conn.execute(
            "SELECT client_status, cancelled_fee FROM returns WHERE id=?", (return_id,)
        ).fetchone()
        if not ret:
            return jsonify({"error": "Return not found"}), 404
        if (ret["client_status"] or "").upper() != "CANCELLED":
            return jsonify({"error": "Return is not cancelled"}), 409
        restored_fee = ret["cancelled_fee"] or 0.0
        conn.execute(
            "UPDATE returns SET client_status='PROCESSING', cancelled_fee=NULL, "
            "cancelled_reason=NULL, cancelled_at=NULL, updated_at=? WHERE id=?",
            (ts, return_id),
        )
        conn.execute(
            "UPDATE payments SET total_fee=?, cancelled_fee=NULL WHERE return_id=?",
            (restored_fee, return_id),
        )
        conn.execute(
            "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?,?,'UNCANCEL',?)",
            (return_id, f"Cancellation reversed — fee restored to ${restored_fee:,.2f}", ts),
        )
        conn.execute(
            "INSERT INTO status_events (return_id, event_type, old_status, new_status, "
            "event_timestamp, source_file, note) VALUES (?, 'STATUS_CHANGED', 'CANCELLED', 'PROCESSING', ?, 'APP', ?)",
            (return_id, ts, "Cancellation reversed"),
        )
        conn.commit()
    finally:
        conn.close()
    from audit_service import _enqueue_write
    _enqueue_write(
        user_id=user, action="RETURN_UNCANCELLED", entity_type="return",
        entity_id=str(return_id), before={"client_status": "CANCELLED"},
        after={"client_status": "PROCESSING", "total_fee": restored_fee},
        ip_address=ip, http_status=200,
    )
    return jsonify({"success": True, "restored_fee": restored_fee})


# ── DEL-1: Hard-delete a return (admin-only, irreversible) ───────────────────
# Cancel/uncancel (LIFE-1, above) is the normal, reversible day-to-day action.
# This is a separate, stricter action for permanently removing rows that
# should never have existed at all (print/import test data, duplicate
# intakes, etc.) rather than for closing out a real client's return.

@app.post("/api/return/<int:return_id>/delete")
@role_required("admin")
def api_delete_return(return_id: int):
    data = _get_json_safe() or {}
    user = session.get("username")
    ip   = request.remote_addr
    conn = get_connection()
    try:
        ret = conn.execute(
            """SELECT r.id, r.log_number, r.tax_year, r.client_status, r.client_id,
                      c.last_name, c.first_name
               FROM returns r JOIN clients c ON c.id = r.client_id
               WHERE r.id = ?""",
            (return_id,),
        ).fetchone()
        if not ret:
            return jsonify({"error": "Return not found"}), 404

        # Require the caller to echo back the log_number as an explicit,
        # hard-to-fat-finger confirmation of *which* return is being destroyed
        # (mirrors the "type to confirm" pattern used for other destructive
        # admin actions in this app).
        confirm = str(data.get("confirm_log_number") or "").strip()
        if confirm != str(ret["log_number"] or ""):
            return jsonify({
                "error": "confirm_log_number must match this return's log number",
                "log_number": ret["log_number"],
            }), 400

        before = dict(ret)
        from db import _delete_return_children
        conn.execute("BEGIN IMMEDIATE")
        _delete_return_children(conn, return_id)
        conn.execute("DELETE FROM returns WHERE id = ?", (return_id,))
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    from audit_service import _enqueue_write
    _enqueue_write(
        user_id=user, action="RETURN_DELETED", entity_type="return",
        entity_id=str(return_id), before=before, after=None,
        ip_address=ip, http_status=200,
    )
    logging.getLogger("taxops").warning(
        "RETURN_DELETED: return_id=%s log_number=%s tax_year=%s client=%s, %s by user=%s",
        return_id, before["log_number"], before["tax_year"],
        before["last_name"], before["first_name"], user,
    )
    return jsonify({"success": True, "deleted_return_id": return_id})


# ── BANK-1: Routing number lookup (local JSON only) ───────────────────────────

import json as _json_mod

_ROUTING_DB: dict[str, str] | None = None

def _load_routing_db() -> dict[str, str]:
    global _ROUTING_DB
    if _ROUTING_DB is None:
        path = os.path.join(os.path.dirname(__file__), "data", "routing_numbers.json")
        try:
            with open(path, encoding="utf-8") as f:
                _ROUTING_DB = _json_mod.load(f)
        except Exception:
            _ROUTING_DB = {}
    return _ROUTING_DB


@app.get("/api/routing-number/<string:routing_number>")
@login_required
def api_routing_number(routing_number: str):
    if not routing_number.isdigit() or len(routing_number) != 9:
        return jsonify({"error": "Routing number must be exactly 9 digits"}), 400
    db = _load_routing_db()
    bank_name = db.get(routing_number)
    if bank_name:
        return jsonify({"found": True, "bank_name": bank_name})
    return jsonify({"found": False})


# ── LIFE-2: Prior year fee for returning client intake ────────────────────────

@app.get("/api/clients/<int:client_id>/prior-fee")
@login_required
def api_client_prior_fee(client_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT r.tax_year, p.total_fee
            FROM returns r
            LEFT JOIN payments p ON p.return_id = r.id
            WHERE r.client_id = ?
              AND r.tax_year = (
                  SELECT MAX(tax_year) FROM returns
                  WHERE client_id = ? AND UPPER(COALESCE(client_status,'')) != 'CANCELLED'
              )
              AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
            LIMIT 1
            """,
            (client_id, client_id),
        ).fetchone()
    finally:
        conn.close()
    if not row or row["total_fee"] is None:
        return jsonify({"found": False})
    return jsonify({
        "found": True,
        "tax_year": row["tax_year"],
        "total_fee": float(row["total_fee"]),
    })


# ── LIFE-3: Intake sheet PDF ──────────────────────────────────────────────────

@app.get("/api/return/<int:return_id>/intake-sheet")
@login_required
def api_intake_sheet(return_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT r.*, c.last_name, c.first_name, c.display_name,
                   c.taxpayer_phone, c.taxpayer_cell, c.address,
                   c.ssn_last4, c.taxpayer_email,
                   c.spouse_last_name, c.spouse_first_name,
                   c.taxpayer_occupation, c.spouse_occupation
            FROM returns r JOIN clients c ON c.id = r.client_id
            WHERE r.id = ?
            """,
            (return_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "Return not found"}), 404
        r = dict(row)

        pmt = conn.execute(
            """SELECT total_fee, discount_amount, special_discount,
                      fee_paid, down_payment, accounting_fee,
                      w7_fee, form_1099_fee, license_fee, reprocess_fee,
                      receipt_number, receipt2_number, payment_method
               FROM payments WHERE return_id=?""",
            (return_id,),
        ).fetchone()

        deps = conn.execute(
            """SELECT full_name, ssn_last4, relationship, date_of_birth, on_medicare
               FROM dependents WHERE return_id=? AND is_deleted=0 ORDER BY id""",
            (return_id,),
        ).fetchall()
    finally:
        conn.close()

    # Privacy: never expose full account number
    acct_raw    = (r.get("bank_account") or "")
    acct_masked = ("*" * (len(acct_raw) - 4) + acct_raw[-4:]) if len(acct_raw) > 4 else acct_raw

    client_name = (
        r.get("display_name")
        or f"{r.get('last_name','')}, {r.get('first_name','')}".strip(", ")
    )
    spouse_name = ""
    if r.get("spouse_last_name") or r.get("spouse_first_name"):
        spouse_name = f"{r.get('spouse_last_name','')}, {r.get('spouse_first_name','')}".strip(", ")

    total_fee = float((pmt["total_fee"] or 0)) if pmt else 0.0
    discount  = float((pmt["discount_amount"] or 0)) if pmt else 0.0
    sp_disc   = float((pmt["special_discount"] or 0)) if pmt else 0.0
    net_fee   = max(0.0, total_fee - discount - sp_disc)
    fee_paid  = float((pmt["fee_paid"] or 0)) if pmt else 0.0

    return render_template(
        "intake_print.html",
        r=r,
        pmt=dict(pmt) if pmt else {},
        deps=[dict(d) for d in deps],
        client_name=client_name,
        spouse_name=spouse_name,
        acct_masked=acct_masked,
        preparer_label=preparer_list_label(r.get("processor") or ""),
        total_fee=total_fee,
        discount=discount + sp_disc,
        net_fee=net_fee,
        fee_paid=fee_paid,
        return_id=return_id,
    )


# ── Duplicate client detection & merge ───────────────────────────────────────

# For joint clients, "CARLOS G & MARIA" → compare only the primary ("CARLOS G").
def _first_primary_for_compare(first_name: str) -> str:
    s = (first_name or "").upper().strip()
    if " & " in s:
        s = s.split(" & ")[0].strip()
    return s


def _name_tokens(s: str) -> list[str]:
    return _first_primary_for_compare(s).split()


# Extra tokens 1–2 letters (O, A) or Jr/Sr/II, etc. — treat as middle / suffix, same person
_MIDDLE_LIKE: frozenset = frozenset(
    {
        "JR", "SR", "II", "III", "IV", "V",
    }
)


def _token_is_initial_or_suffix(tok: str) -> bool:
    t = tok.strip(".,'").upper()
    if t in _MIDDLE_LIKE:
        return True
    if not t or not t.isalpha():
        return False
    if len(t) == 1:
        return True
    if len(t) == 2 and t.isupper():
        return True
    return False


def _first_names_likely_same_middles(a_first: str, b_first: str) -> bool:
    """
    Same person when the only first-name difference is missing vs middle initial
    (e.g. BRYAN vs BRYAN O, JOSE vs JOSE A, CARLOS vs CARLOS G for one-char 'G').

    If the shorter token list is a prefix of the longer, and every extra token is
    1–2 letter initial/suffix, treat as a duplicate.
    """
    ta, tb = _name_tokens(a_first), _name_tokens(b_first)
    if not ta or not tb:
        return False
    if ta == tb:
        return True
    if len(ta) > len(tb):
        ta, tb = tb, ta
    # ta is shorter
    if len(ta) > len(tb) or not tb:
        return False
    if ta != tb[: len(ta)]:
        return False
    rest = tb[len(ta) :]
    return all(_token_is_initial_or_suffix(x) for x in rest)


def _first_names_likely_duplicate(fa: str, fb: str) -> bool:
    fa_st = (fa or "").upper().strip()
    fb_st = (fb or "").upper().strip()
    if not fa_st or not fb_st:
        return False
    # String containment / one side extends the other (incl. joint "X" in "X & Y")
    if (
        fa_st.startswith(fb_st) or fb_st.startswith(fa_st) or
        fa_st in fb_st or fb_st in fa_st
    ):
        return True
    if _first_names_likely_same_middles(fa, fb):
        return True
    return False


def _find_duplicate_pairs() -> list[dict]:
    """
    Find likely duplicate client records with the same last_name and either:
    - overlapping / contained first_name strings, or
    - first names that differ only by middle initials / extra 1–2 char tokens
      (BRYAN vs BRYAN O) using the primary name before " & " for joint filers.
    """
    conn = get_connection()
    clients = conn.execute(
        """
        SELECT c.id, c.last_name, c.first_name, c.display_name,
               COUNT(r.id)                         AS return_count,
               MAX(r.log_number)                   AS best_log,
               GROUP_CONCAT(r.id)                  AS return_ids,
               GROUP_CONCAT(COALESCE(r.log_number,''))  AS log_numbers,
               GROUP_CONCAT(r.tax_year)            AS tax_years,
               GROUP_CONCAT(r.client_status)       AS statuses
        FROM clients c
        LEFT JOIN returns r ON r.client_id = c.id
        GROUP BY c.id
        ORDER BY c.last_name, c.first_name
        """
    ).fetchall()
    conn.close()

    # Group by last_name
    by_last: dict[str, list] = {}
    for row in clients:
        key = (row["last_name"] or "").upper().strip()
        by_last.setdefault(key, []).append(dict(row))

    pairs = []
    seen = set()
    for last, group in by_last.items():
        if len(group) < 2:
            continue
        for i, a in enumerate(group):
            for b in group[i + 1:]:
                if not (a.get("first_name") or "").strip() or not (b.get("first_name") or "").strip():
                    continue
                if not _first_names_likely_duplicate(
                    a["first_name"] or "", b["first_name"] or ""
                ):
                    continue
                key = tuple(sorted([a["id"], b["id"]]))
                if key in seen:
                    continue
                seen.add(key)
                # Prefer keeping the one with a log number / more returns
                a_score = (1 if a["best_log"] else 0) + (a["return_count"] or 0)
                b_score = (1 if b["best_log"] else 0) + (b["return_count"] or 0)
                keep, discard = (a, b) if a_score >= b_score else (b, a)
                pairs.append({
                    "keep":    keep,
                    "discard": discard,
                })

    return sorted(pairs, key=lambda p: (p["keep"]["last_name"] or ""))


def _merge_pair_key(ka: int, kb: int) -> str:
    return f"{min(ka, kb)}-{max(ka, kb)}"


def _merge_pairs_for_session() -> list[dict]:
    skipped = set(session.get("merge_skipped", []))
    out: list[dict] = []
    for p in _find_duplicate_pairs():
        k, d = int(p["keep"]["id"]), int(p["discard"]["id"])
        if _merge_pair_key(k, d) in skipped:
            continue
        out.append(p)
    return out


@app.get("/merge-clients")
@role_required("admin")
def merge_clients_page():
    pairs = _merge_pairs_for_session()
    ctx = base_ctx(date.today().year)
    ctx.update({"active_page": "merge", "pairs": pairs})
    return render_template("merge_clients.html", **ctx)


@app.post("/api/merge-clients")
@role_required("admin")
def api_merge_clients():
    """
    Merge 'discard' client into 'keep' client.
    Moves all returns (and review_queue refs) from discard → keep, then deletes discard.
    """
    data       = _get_json_safe()
    keep_id    = int(data.get("keep_id", 0))
    discard_id = int(data.get("discard_id", 0))
    if not keep_id or not discard_id or keep_id == discard_id:
        return jsonify({"error": "Invalid IDs"}), 400

    conn = get_connection()
    try:
        keep    = conn.execute("SELECT * FROM clients WHERE id=?", (keep_id,)).fetchone()
        discard = conn.execute("SELECT * FROM clients WHERE id=?", (discard_id,)).fetchone()
        if not keep or not discard:
            return jsonify({"error": "Client not found"}), 404

        ts = now()
        merge_client_into(conn, keep_id, discard_id, ts)
        conn.commit()

        keep_name = _build_name_full(
            keep["first_name"] or "", keep["last_name"] or "",
            keep["display_name"] or "",
            keep.get("spouse_first_name") or "", keep.get("spouse_last_name") or "",
        )
        return jsonify({"success": True, "kept": keep_name})
    except Exception as e:
        conn.rollback()
        return jsonify({"error": str(e)}), 500
    finally:
        conn.close()


@app.post("/api/merge-clients/bulk")
@login_required
def api_merge_clients_bulk():
    """
    Auto-merge all duplicate pairs (same as Merge Dupes list, respecting skipped pairs in session).
    `dry_run: true` returns a count and sample; false runs the merge in separate transactions.
    """
    data    = request.get_json(silent=True) or {}
    dry_run = bool(data.get("dry_run"))
    limit   = int(data.get("limit", 2000))
    if limit < 1 or limit > 5000:
        limit = 2000

    pairs = _merge_pairs_for_session()[:limit]
    if dry_run:
        return jsonify({
            "success":  True,
            "dry_run":  True,
            "count":    len(pairs),
            "previews": [
                {
                    "keep_id":    int(p["keep"]["id"]),
                    "discard_id": int(p["discard"]["id"]),
                    "name": (p["keep"]["last_name"] or "")
                    + ", " + (p["keep"].get("first_name") or ""),
                }
                for p in pairs[:50]
            ],
        })

    merged = 0
    errors: list[dict] = []
    for p in pairs:
        k = int(p["keep"]["id"])
        d = int(p["discard"]["id"])
        conn = get_connection()
        try:
            merge_client_into(conn, k, d, now())
            conn.commit()
            merged += 1
        except Exception as e:
            conn.rollback()
            errors.append({
                "keep_id":    k, "discard_id": d, "error": str(e),
            })
        finally:
            conn.close()

    return jsonify({"success": True, "merged": merged, "errors": errors})


@app.post("/api/merge-clients/skip")
@login_required
def api_merge_skip():
    """Mark a pair as 'not duplicates' by storing a skip record (simple session list)."""
    data = _get_json_safe()
    skipped = session.get("merge_skipped", [])
    pair_key = f"{min(data['keep_id'], data['discard_id'])}-{max(data['keep_id'], data['discard_id'])}"
    if pair_key not in skipped:
        skipped.append(pair_key)
    session["merge_skipped"] = skipped
    return jsonify({"success": True})


# ── E-file Batches ────────────────────────────────────────────────────────────

@app.post("/efile-batch/create")
@role_required("admin")
def efile_batch_create():
    """Create a new e-file batch from a list of EFILE READY return IDs."""
    return_ids = request.form.getlist("return_ids")
    if not return_ids:
        flash("No returns selected.", "error")
        return redirect(url_for("efile_queue"))

    transmission_date = request.form.get("transmission_date") or date.today().isoformat()
    notes = request.form.get("notes", "").strip()
    ts = now()

    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO efile_batches (transmission_date, notes, status, created_at) VALUES (?,?,?,?)",
            (transmission_date, notes or None, "open", ts),
        )
        batch_id = cur.lastrowid

        added = 0
        skipped_batched = 0
        for rid in return_ids:
            try:
                rid = int(rid)
            except ValueError:
                continue

            # Server-side guard: reject returns already in an open/transmitted batch
            already = conn.execute(
                """SELECT b.id FROM efile_batch_items bi
                   JOIN efile_batches b ON b.id = bi.batch_id
                   WHERE bi.return_id=? AND b.status NOT IN ('closed')""",
                (rid,),
            ).fetchone()
            if already:
                skipped_batched += 1
                continue

            # Pull autofill data from the return + payment rows
            row = conn.execute(
                f"{_SELECT} WHERE r.id=?", (rid,)
            ).fetchone()
            if not row:
                continue
            r = _enrich(dict(row))

            client_name = r.get("last_name", "")
            if r.get("first_name"):
                client_name += f", {r['first_name']}"

            conn.execute(
                """INSERT OR IGNORE INTO efile_batch_items
                   (batch_id, return_id, log_number, client_name,
                    tax_year, receipt_number, fee_paid, cc_fee, pickup_date,
                    transmission_date, ack_status, created_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    batch_id,
                    rid,
                    r.get("log_number") or None,
                    client_name or None,
                    r.get("tax_year") or None,
                    r.get("receipt_number") or None,
                    r.get("fee_paid") or None,
                    r.get("cc_fee") or None,
                    r.get("pickup_date") or None,
                    transmission_date,
                    "pending",
                    ts,
                ),
            )
            added += 1

        conn.commit()
        msg = f"Batch #{batch_id} created with {added} return(s)."
        if skipped_batched:
            msg += f" ({skipped_batched} skipped — already in another active batch.)"
        flash(msg, "success")
        return redirect(url_for("efile_batch_detail", batch_id=batch_id))
    except Exception as e:
        conn.rollback()
        flash(f"Error creating batch: {e}", "error")
        return redirect(url_for("efile_queue"))
    finally:
        conn.close()


@app.route("/efile-batch/<int:batch_id>")
@role_required("admin")
def efile_batch_detail(batch_id: int):
    conn = get_connection()
    batch = conn.execute(
        "SELECT * FROM efile_batches WHERE id=?", (batch_id,)
    ).fetchone()
    if not batch:
        conn.close()
        abort(404)

    sort = request.args.get("sort", "name_desc")
    if sort == "name":
        order = "ORDER BY i.client_name ASC"
    elif sort == "log":
        order = "ORDER BY CASE WHEN i.log_number IS NULL OR i.log_number='' THEN 1 ELSE 0 END, CAST(i.log_number AS INTEGER)"
    else:  # name_desc (default)
        order = "ORDER BY i.client_name DESC"
    items = conn.execute(
        f"SELECT i.*, r.client_status FROM efile_batch_items i "
        f"LEFT JOIN returns r ON r.id = i.return_id "
        f"WHERE i.batch_id=? {order}",
        (batch_id,),
    ).fetchall()
    items = [dict(i) for i in items]

    # Summary counts
    counts = {s: 0 for s in ("pending", "accepted", "rejected", "needs_calculation", "ready")}
    for item in items:
        ack = item.get("ack_status", "pending")
        counts[ack] = counts.get(ack, 0) + 1
        if item.get("needs_calculation"):
            counts["needs_calculation"] += 1
        elif ack == "pending":
            # "ready" = pending ACK and not flagged as needing calculation
            counts["ready"] += 1

    # All batches for the sidebar list
    all_batches = [dict(b) for b in conn.execute(
        "SELECT id, transmission_date, status, created_at, "
        "(SELECT COUNT(*) FROM efile_batch_items WHERE batch_id=efile_batches.id) AS item_count "
        "FROM efile_batches ORDER BY created_at DESC"
    ).fetchall()]
    conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="efile",
        batch=dict(batch),
        items=items,
        counts=counts,
        sort=sort,
        all_batches=all_batches,
    )
    return render_template("efile_batch.html", **ctx)


@app.route("/efile-batch")
@role_required("admin")
def efile_batch_list():
    """List all e-file batches."""
    conn = get_connection()
    batches = [dict(b) for b in conn.execute(
        "SELECT b.id, b.transmission_date, b.transmitted_at, b.status, b.notes, b.created_at, "
        "COUNT(i.id) AS item_count, "
        "SUM(CASE WHEN i.ack_status='accepted' THEN 1 ELSE 0 END) AS accepted_count, "
        "SUM(CASE WHEN i.ack_status='rejected' THEN 1 ELSE 0 END) AS rejected_count "
        "FROM efile_batches b "
        "LEFT JOIN efile_batch_items i ON i.batch_id=b.id "
        "GROUP BY b.id ORDER BY b.created_at DESC"
    ).fetchall()]
    conn.close()
    ctx = base_ctx()
    ctx.update(active_page="efile", batches=batches)
    return render_template("efile_batch_list.html", **ctx)


@app.post("/api/efile-batch/<int:batch_id>/transmit")
@role_required("admin")
def efile_batch_transmit(batch_id: int):
    """Mark batch as transmitted (sent to IRS via Drake)."""
    conn = get_connection()
    batch = conn.execute("SELECT * FROM efile_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        return jsonify({"success": False, "error": "Batch not found"}), 404
    if batch["transmitted_at"]:
        conn.close()
        return jsonify({"success": False, "error": "Batch already transmitted"}), 409
    ts = now()
    transmission_date = batch["transmission_date"]
    conn.execute(
        "UPDATE efile_batches SET transmitted_at=?, status='transmitted' WHERE id=?",
        (ts, batch_id),
    )
    # Stamp efile_date on every return in this batch (only if not already set).
    # This records when the return was sent to IRS, independent of when ACK arrives.
    conn.execute(
        """UPDATE returns
           SET efile_date  = COALESCE(efile_date, ?),
               updated_at  = ?
           WHERE id IN (SELECT return_id FROM efile_batch_items WHERE batch_id=?)""",
        (transmission_date, ts, batch_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True, "transmitted_at": ts})


@app.post("/api/efile-batch/<int:batch_id>/item/<int:item_id>/ack")
@login_required
def efile_batch_item_ack(batch_id: int, item_id: int):
    """Update ACK status on a single batch item."""
    data       = _get_json_safe()
    ack_status = data.get("ack_status", "").lower()
    if ack_status not in ("pending", "accepted", "rejected"):
        return jsonify({"success": False, "error": "Invalid ack_status"}), 400

    if ack_status == "rejected":
        if not (data.get("rejection_code") or "").strip():
            return jsonify({"success": False, "error": "Rejection code is required."}), 400
        if not (data.get("rejection_reason") or "").strip():
            return jsonify({"success": False, "error": "Rejection reason is required."}), 400

    conn = get_connection()
    conn.execute(
        """UPDATE efile_batch_items
           SET ack_status=?, ack_date=?, rejection_code=?, rejection_reason=?
           WHERE id=? AND batch_id=?""",
        (
            ack_status,
            data.get("ack_date") or (date.today().isoformat() if ack_status != "pending" else None),
            data.get("rejection_code") or None,
            data.get("rejection_reason") or None,
            item_id,
            batch_id,
        ),
    )

    # Fetch return_id + snapshot fields for status updates below
    item_row = conn.execute(
        "SELECT return_id, transmission_date FROM efile_batch_items WHERE id=?", (item_id,)
    ).fetchone()

    if item_row:
        return_id       = item_row["return_id"]
        transmission_dt = item_row["transmission_date"]
        current = conn.execute(
            "SELECT client_status FROM returns WHERE id=?", (return_id,)
        ).fetchone()
        current_status = current["client_status"] if current else None
        ack_date_val   = data.get("ack_date") or date.today().isoformat()

        if ack_status == "accepted":
            if not is_locked_status(current_status) and current_status not in ("LOG OUT",):
                conn.execute(
                    "UPDATE returns SET client_status='LOG OUT', "
                    "logout_date=COALESCE(logout_date,?), "
                    "efile_date=COALESCE(efile_date,?), "
                    "ack_date=COALESCE(ack_date,?), "
                    "updated_at=? WHERE id=?",
                    (date.today().isoformat(), transmission_dt, ack_date_val, now(), return_id),
                )
            else:
                # Return already in LOG OUT or locked — still sync efile_date and ack_date
                conn.execute(
                    "UPDATE returns SET "
                    "efile_date=COALESCE(efile_date,?), "
                    "ack_date=COALESCE(ack_date,?), "
                    "updated_at=? WHERE id=?",
                    (transmission_dt, ack_date_val, now(), return_id),
                )

        elif ack_status == "rejected":
            if not is_locked_status(current_status):
                conn.execute(
                    "UPDATE returns SET client_status='REJECTED', "
                    "contact_status='not_contacted', last_contacted_date=NULL, "
                    "ack_date=COALESCE(ack_date,?), "
                    "updated_at=? WHERE id=?",
                    (ack_date_val, now(), return_id),
                )

        elif ack_status == "pending":
            # Revert return to EFILE READY if it was moved to LOG OUT or REJECTED by this batch
            if current_status in ("LOG OUT", "REJECTED") and not is_locked_status(current_status):
                if current_status == "REJECTED":
                    conn.execute(
                        "UPDATE returns SET client_status='EFILE READY', "
                        "contact_status=NULL, last_contacted_date=NULL, ack_date=NULL, "
                        "updated_at=? WHERE id=?",
                        (now(), return_id),
                    )
                else:
                    conn.execute(
                        "UPDATE returns SET client_status='EFILE READY', updated_at=? WHERE id=?",
                        (now(), return_id),
                    )
            # Clear stale ack_date on the return regardless of whether status reverted
            # Only safe when return is in a state this batch would have set it
            if current_status in ("LOG OUT", "REJECTED"):
                conn.execute(
                    "UPDATE returns SET ack_date=NULL WHERE id=? "
                    "AND client_status IN ('EFILE READY','REJECTED','LOG OUT')",
                    (return_id,)
                )

    # Auto-close batch if all items are resolved — never overwrite 'transmitted'
    unresolved = conn.execute(
        "SELECT COUNT(*) FROM efile_batch_items WHERE batch_id=? AND ack_status='pending'",
        (batch_id,),
    ).fetchone()[0]
    if unresolved == 0:
        conn.execute(
            "UPDATE efile_batches SET status='closed' WHERE id=? AND status NOT IN ('transmitted','closed')",
            (batch_id,)
        )

    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.post("/api/efile-batch/<int:batch_id>/item/<int:item_id>/flag")
@login_required
def efile_batch_item_flag(batch_id: int, item_id: int):
    """Toggle 'needs calculation' flag on a batch item."""
    data = _get_json_safe()
    conn = get_connection()
    conn.execute(
        "UPDATE efile_batch_items SET needs_calculation=? WHERE id=? AND batch_id=?",
        (1 if data.get("needs_calculation") else 0, item_id, batch_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.post("/api/efile-batch/<int:batch_id>/item/<int:item_id>/logout")
@login_required
def efile_batch_item_logout(batch_id: int, item_id: int):
    """Manually move an accepted return to LOG OUT (fallback for edge cases)."""
    conn = get_connection()
    item_row = conn.execute(
        "SELECT return_id, ack_status, transmission_date, ack_date FROM efile_batch_items "
        "WHERE id=? AND batch_id=?",
        (item_id, batch_id),
    ).fetchone()

    if not item_row:
        conn.close()
        return jsonify({"success": False, "error": "Item not found"}), 404
    if item_row["ack_status"] != "accepted":
        conn.close()
        return jsonify({"success": False, "error": "Only accepted items can be moved to LOG OUT"}), 400

    return_id = item_row["return_id"]
    current   = conn.execute(
        "SELECT client_status FROM returns WHERE id=?", (return_id,)
    ).fetchone()
    current_status = current["client_status"] if current else None

    if is_locked_status(current_status):
        conn.close()
        return jsonify({"success": False, "error": f"Return status '{current_status}' is locked and cannot be changed"}), 400

    today = date.today().isoformat()
    conn.execute(
        "UPDATE returns SET client_status='LOG OUT', "
        "logout_date=COALESCE(logout_date,?), "
        "efile_date=COALESCE(efile_date,?), "
        "ack_date=COALESCE(ack_date,?), "
        "updated_at=? WHERE id=?",
        (today, item_row["transmission_date"], item_row["ack_date"], now(), return_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True})


@app.route("/efile-batch/<int:batch_id>/export")
@role_required("admin")
def efile_batch_export(batch_id: int):
    """Download batch as CSV.
    ?filter=all (default) | accepted | rejected
    """
    import csv, io as _io
    from flask import Response

    report = request.args.get("filter", "all").lower()
    if report not in ("all", "accepted", "rejected"):
        report = "all"

    conn = get_connection()
    batch = conn.execute("SELECT * FROM efile_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        abort(404)

    base_query = (
        "SELECT * FROM efile_batch_items WHERE batch_id=? "
        "{where}"
        "ORDER BY CASE WHEN log_number IS NULL OR log_number='' THEN 1 ELSE 0 END, CAST(log_number AS INTEGER)"
    )
    where_clause = ""
    params = [batch_id]
    if report == "accepted":
        where_clause = "AND ack_status='accepted' "
    elif report == "rejected":
        where_clause = "AND ack_status='rejected' "

    items = conn.execute(base_query.format(where=where_clause), params).fetchall()
    conn.close()

    buf = _io.StringIO()
    w = csv.writer(buf)

    if report == "rejected":
        w.writerow(["Log #", "Client Name", "Tax Year",
                    "Receipt #", "Fee Paid", "CC Fee", "Pickup Date", "Transmission Date",
                    "ACK Date", "Rejection Code", "Rejection Reason"])
        for i in items:
            w.writerow([
                i["log_number"] or "",
                i["client_name"] or "",
                i["tax_year"] or "",
                i["receipt_number"] or "",
                i["fee_paid"] or "",
                i["cc_fee"] or "",
                i["pickup_date"] or "",
                i["transmission_date"] or "",
                i["ack_date"] or "",
                i["rejection_code"] or "",
                i["rejection_reason"] or "",
            ])
    elif report == "accepted":
        w.writerow(["Log #", "Client Name", "Tax Year",
                    "Receipt #", "Fee Paid", "CC Fee", "Pickup Date", "Transmission Date",
                    "ACK Date"])
        for i in items:
            w.writerow([
                i["log_number"] or "",
                i["client_name"] or "",
                i["tax_year"] or "",
                i["receipt_number"] or "",
                i["fee_paid"] or "",
                i["cc_fee"] or "",
                i["pickup_date"] or "",
                i["transmission_date"] or "",
                i["ack_date"] or "",
            ])
    else:
        w.writerow(["Log #", "Client Name", "Tax Year",
                    "Receipt #", "Fee Paid", "CC Fee", "Pickup Date", "Transmission Date",
                    "ACK Status", "ACK Date", "Rejection Code", "Rejection Reason",
                    "Needs Calculation"])
        for i in items:
            w.writerow([
                i["log_number"] or "",
                i["client_name"] or "",
                i["tax_year"] or "",
                i["receipt_number"] or "",
                i["fee_paid"] or "",
                i["cc_fee"] or "",
                i["pickup_date"] or "",
                i["transmission_date"] or "",
                i["ack_status"] or "",
                i["ack_date"] or "",
                i["rejection_code"] or "",
                i["rejection_reason"] or "",
                "Yes" if i["needs_calculation"] else "",
            ])

    tdate = dict(batch).get("transmission_date") or "nodateset"
    label = {"all": "full", "accepted": "accepted", "rejected": "rejected"}[report]
    fname = f"efile_batch_{batch_id}_{tdate}_{label}.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ── Extension Feature ─────────────────────────────────────────────────────────

@app.route("/extension-queue")
@permission_required("can_manage_extension_queue")
def extension_queue():
    year = int(request.args.get("year", date.today().year))
    sort = request.args.get("sort", "log")
    conn = get_connection()
    order = (
        "ORDER BY c.last_name, c.first_name" if sort == "name"
        else "ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END, CAST(r.log_number AS INTEGER)"
    )
    rows = conn.execute(
        f"{_SELECT} WHERE r.extension_requested = 1 "
        f"AND (r.extension_ack_status IS NULL OR r.extension_ack_status != 'accepted') "
        f"AND (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?)) "
        f"{order}",
        (str(year), year - 1),
    ).fetchall()
    conn.close()
    ctx = base_ctx(year)
    ctx.update(
        active_page="extension",
        returns=[_enrich(dict(r)) for r in rows],
        sort=sort,
        today=date.today().isoformat(),
    )
    return render_template("extension_queue.html", **ctx)


@app.post("/extension-batch/create")
@role_required("admin")
def extension_batch_create():
    return_ids = request.form.getlist("return_ids")
    if not return_ids:
        flash("No returns selected.", "error")
        return redirect(url_for("extension_queue"))

    filing_date = request.form.get("filing_date") or date.today().isoformat()
    notes = request.form.get("notes", "").strip()
    ts = now()

    conn = get_connection()
    try:
        cur = conn.execute(
            "INSERT INTO extension_batches (filing_date, notes, status, created_at) VALUES (?,?,?,?)",
            (filing_date, notes or None, "open", ts),
        )
        batch_id = cur.lastrowid

        added = 0
        for rid in return_ids:
            try:
                rid = int(rid)
            except ValueError:
                continue
            row = conn.execute(f"{_SELECT} WHERE r.id=?", (rid,)).fetchone()
            if not row:
                continue
            r = _enrich(dict(row))
            client_name = r.get("last_name", "")
            if r.get("first_name"):
                client_name += f", {r['first_name']}"
            conn.execute(
                """INSERT OR IGNORE INTO extension_batch_items
                   (batch_id, return_id, log_number, client_name, tax_year,
                    filing_date, ack_status, created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (batch_id, rid, r.get("log_number") or None, client_name or None,
                 r.get("tax_year") or None, filing_date, "pending", ts),
            )
            added += 1

        conn.commit()
        flash(f"Extension batch #{batch_id} created with {added} return(s).", "success")
        return redirect(url_for("extension_batch_detail", batch_id=batch_id))
    except Exception as e:
        conn.rollback()
        flash(f"Error creating extension batch: {e}", "error")
        return redirect(url_for("extension_queue"))
    finally:
        conn.close()


@app.route("/extension-batch/<int:batch_id>")
@role_required("admin")
def extension_batch_detail(batch_id: int):
    conn = get_connection()
    batch = conn.execute("SELECT * FROM extension_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        abort(404)

    sort = request.args.get("sort", "name_desc")
    if sort == "name":
        order = "ORDER BY i.client_name ASC"
    elif sort == "log":
        order = "ORDER BY CASE WHEN i.log_number IS NULL OR i.log_number='' THEN 1 ELSE 0 END, CAST(i.log_number AS INTEGER)"
    else:
        order = "ORDER BY i.client_name DESC"

    items = conn.execute(
        f"SELECT i.*, r.client_status FROM extension_batch_items i "
        f"JOIN returns r ON r.id = i.return_id "
        f"WHERE i.batch_id=? {order}",
        (batch_id,),
    ).fetchall()
    items = [dict(i) for i in items]

    counts = {s: 0 for s in ("pending", "accepted", "rejected")}
    for item in items:
        ack = item.get("ack_status", "pending")
        counts[ack] = counts.get(ack, 0) + 1

    all_batches = [dict(b) for b in conn.execute(
        "SELECT id, filing_date, status, created_at, "
        "(SELECT COUNT(*) FROM extension_batch_items WHERE batch_id=extension_batches.id) AS item_count "
        "FROM extension_batches ORDER BY created_at DESC"
    ).fetchall()]
    conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="extension",
        batch=dict(batch),
        items=items,
        counts=counts,
        sort=sort,
        all_batches=all_batches,
    )
    return render_template("extension_batch.html", **ctx)


@app.route("/extension-batch")
@role_required("admin")
def extension_batch_list():
    conn = get_connection()
    batches = [dict(b) for b in conn.execute(
        "SELECT b.id, b.filing_date, b.transmitted_at, b.status, b.notes, b.created_at, "
        "COUNT(i.id) AS item_count, "
        "SUM(CASE WHEN i.ack_status='accepted' THEN 1 ELSE 0 END) AS accepted_count, "
        "SUM(CASE WHEN i.ack_status='rejected' THEN 1 ELSE 0 END) AS rejected_count "
        "FROM extension_batches b "
        "LEFT JOIN extension_batch_items i ON i.batch_id=b.id "
        "GROUP BY b.id ORDER BY b.created_at DESC"
    ).fetchall()]
    conn.close()
    ctx = base_ctx()
    ctx.update(active_page="extension", batches=batches)
    return render_template("extension_batch_list.html", **ctx)


@app.post("/api/extension-batch/<int:batch_id>/transmit")
@login_required
def extension_batch_transmit(batch_id: int):
    conn = get_connection()
    batch = conn.execute("SELECT * FROM extension_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        return jsonify({"success": False, "error": "Batch not found"}), 404
    ts = now()
    conn.execute(
        "UPDATE extension_batches SET transmitted_at=?, status='transmitted' WHERE id=?",
        (ts, batch_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True, "transmitted_at": ts})


@app.post("/api/extension-batch/<int:batch_id>/item/<int:item_id>/ack")
@login_required
def extension_batch_item_ack(batch_id: int, item_id: int):
    data = _get_json_safe()
    ack_status = (data.get("ack_status") or "").lower()
    if ack_status not in ("pending", "accepted", "rejected"):
        return jsonify({"success": False, "error": "Invalid ack_status"}), 400
    if ack_status == "rejected" and not (data.get("rejection_reason") or "").strip():
        return jsonify({"success": False, "error": "Rejection reason is required."}), 400

    conn = get_connection()
    item = conn.execute(
        "SELECT * FROM extension_batch_items WHERE id=? AND batch_id=?",
        (item_id, batch_id),
    ).fetchone()
    if not item:
        conn.close()
        return jsonify({"success": False, "error": "Item not found"}), 404

    ack_date = data.get("ack_date") or None
    rejection_reason = (data.get("rejection_reason") or "").strip() or None
    ts = now()

    try:
        conn.execute(
            "UPDATE extension_batch_items SET ack_status=?, ack_date=?, rejection_reason=? WHERE id=?",
            (ack_status, ack_date, rejection_reason, item_id),
        )
        return_id = item["return_id"]
        filing_date = item["filing_date"]

        if ack_status == "accepted":
            # Stamp extension dates; auto-fill Oct 15 as default extended due date
            tax_year = item["tax_year"] or date.today().year
            default_due = f"{tax_year + 1}-10-15"
            conn.execute(
                """UPDATE returns SET
                     extension_ack_status='accepted',
                     extension_ack_date=?,
                     extension_filed_date=COALESCE(extension_filed_date, ?),
                     extension_due_date=COALESCE(extension_due_date, ?),
                     updated_at=?
                   WHERE id=?""",
                (ack_date, filing_date, default_due, ts, return_id),
            )
        elif ack_status == "rejected":
            conn.execute(
                "UPDATE returns SET extension_ack_status='rejected', extension_ack_date=?, updated_at=? WHERE id=?",
                (ack_date, ts, return_id),
            )
        else:  # pending — reset
            conn.execute(
                "UPDATE returns SET extension_ack_status=NULL, extension_ack_date=NULL, updated_at=? WHERE id=?",
                (ts, return_id),
            )

        # Auto-close batch when all items resolved
        all_items = conn.execute(
            "SELECT ack_status FROM extension_batch_items WHERE batch_id=?", (batch_id,)
        ).fetchall()
        if all(i["ack_status"] in ("accepted", "rejected") for i in all_items):
            conn.execute(
                "UPDATE extension_batches SET status='closed' WHERE id=?", (batch_id,)
            )

        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


@app.route("/extension-batch/<int:batch_id>/export")
@role_required("admin")
def extension_batch_export(batch_id: int):
    """Download extension batch as CSV."""
    import csv
    import io
    conn = get_connection()
    batch = conn.execute("SELECT * FROM extension_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        abort(404)
    items = [dict(i) for i in conn.execute(
        "SELECT i.* FROM extension_batch_items i WHERE i.batch_id=? ORDER BY i.client_name",
        (batch_id,),
    ).fetchall()]
    conn.close()

    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["Log #", "Client Name", "Tax Year", "Filing Date",
                "ACK Status", "ACK Date", "Rejection Reason"])
    for i in items:
        w.writerow([
            i["log_number"] or "",
            i["client_name"] or "",
            i["tax_year"] or "",
            i["filing_date"] or "",
            i["ack_status"] or "",
            i["ack_date"] or "",
            i["rejection_reason"] or "",
        ])

    tdate = batch["filing_date"] or "unknown"
    fname = f"extension_batch_{batch_id}_{tdate}.csv"
    return Response(
        buf.getvalue(),
        mimetype="text/csv",
        headers={"Content-Disposition": f"attachment; filename={fname}"},
    )


# ── Import Audit ──────────────────────────────────────────────────────────────

@app.route("/import-audit")
@role_required("admin")
def import_audit():
    """Diagnostic page showing orphaned / unmatched records and import health."""
    conn = get_connection()

    # --- Import batch summary ---
    batches = [dict(r) for r in conn.execute(
        "SELECT * FROM import_batches ORDER BY imported_at DESC"
    ).fetchall()]

    # --- Parse errors from import_rows ---
    parse_errors = [dict(r) for r in conn.execute(
        """SELECT ir.row_number, ir.action, ir.error, ir.raw_json,
                  ib.filename
           FROM import_rows ir
           JOIN import_batches ib ON ib.id = ir.batch_id
           WHERE ir.error IS NOT NULL AND ir.error != ''
           ORDER BY ib.filename, ir.row_number"""
    ).fetchall()]
    import json as _json
    for e in parse_errors:
        try:
            raw = _json.loads(e["raw_json"] or "{}")
            e["name"]     = f"{raw.get('last_name','?')}, {raw.get('first_name','?')}"
            e["log"]      = raw.get("log_number", "")
            e["tax_year"] = raw.get("tax_year", "")
        except Exception:
            e["name"] = "?"

    # --- Ambiguous / review queue ---
    review_items = [dict(r) for r in conn.execute(
        """SELECT rq.*, ib.filename
           FROM review_queue rq
           LEFT JOIN import_batches ib ON ib.id = rq.batch_id
           WHERE rq.status = 'pending'
           ORDER BY ib.filename, rq.row_number"""
    ).fetchall()]
    for item in review_items:
        try:
            raw = _json.loads(item["raw_json"] or "{}")
            item["csv_last"]  = item["csv_last"]  or raw.get("Taxpayer Last Name")  or raw.get("last_name")  or raw.get("TAX PAYER NAME (S) LAST") or "?"
            item["csv_first"] = item["csv_first"] or raw.get("Taxpayer First Name") or raw.get("first_name") or raw.get("FIRST") or "?"
        except Exception:
            pass

    # --- No-log returns: returns that came from Drake but have no log_number ---
    # Grouped into: (a) exact/near-exact name match with a logged client, (b) genuinely unmatched
    no_log_rows = conn.execute(
        """SELECT c.id as cid, c.last_name, c.first_name, r.id as rid,
                  r.tax_year, r.client_status, r.intake_date, r.processor
           FROM returns r JOIN clients c ON c.id = r.client_id
           WHERE (r.log_number IS NULL OR r.log_number = '')
             AND r.tax_year >= 2024
           ORDER BY r.tax_year DESC, c.last_name"""
    ).fetchall()

    logged_clients = conn.execute(
        """SELECT DISTINCT c.id, upper(trim(c.last_name)) as ln,
                  upper(trim(COALESCE(c.first_name,''))) as fn
           FROM clients c
           JOIN returns r ON r.client_id = c.id
           WHERE r.log_number IS NOT NULL AND r.log_number != ''"""
    ).fetchall()
    logged_cache = [{"id": r["id"], "ln": r["ln"], "fn": r["fn"]} for r in logged_clients]

    from name_matcher import find_client, ACCEPT_THRESHOLD, REVIEW_THRESHOLD

    close_matches   = []  # score between REVIEW_THRESHOLD and ACCEPT_THRESHOLD (needs review)
    unmatched       = []  # score < REVIEW_THRESHOLD (genuinely unmatched)

    for row in no_log_rows:
        result = find_client(conn, row["last_name"] or "", row["first_name"], cache=logged_cache)
        entry = {
            "cid":    row["cid"],
            "rid":    row["rid"],
            "name":   f"{row['last_name']}, {row['first_name'] or ''}".strip(", "),
            "status": row["client_status"],
            "year":   row["tax_year"],
            "intake": row["intake_date"],
        }
        if result and result["score"] >= ACCEPT_THRESHOLD:
            # High confidence match but not yet merged — surface as fixable
            match_client = conn.execute(
                "SELECT last_name, first_name FROM clients WHERE id=?", (result["client_id"],)
            ).fetchone()
            log = conn.execute(
                "SELECT log_number FROM returns WHERE client_id=? AND log_number IS NOT NULL AND log_number!='' ORDER BY CAST(log_number AS INTEGER) LIMIT 1",
                (result["client_id"],)
            ).fetchone()
            entry["match_name"]  = f"{match_client['last_name']}, {match_client['first_name'] or ''}".strip(", ") if match_client else "?"
            entry["match_log"]   = log["log_number"] if log else ""
            entry["match_score"] = result["score"]
            entry["match_cid"]   = result["client_id"]
            close_matches.append(entry)
        elif result and result["score"] >= REVIEW_THRESHOLD:
            match_client = conn.execute(
                "SELECT last_name, first_name FROM clients WHERE id=?", (result["client_id"],)
            ).fetchone()
            log = conn.execute(
                "SELECT log_number FROM returns WHERE client_id=? AND log_number IS NOT NULL AND log_number!='' ORDER BY CAST(log_number AS INTEGER) LIMIT 1",
                (result["client_id"],)
            ).fetchone()
            entry["match_name"]  = f"{match_client['last_name']}, {match_client['first_name'] or ''}".strip(", ") if match_client else "?"
            entry["match_log"]   = log["log_number"] if log else ""
            entry["match_score"] = result["score"]
            entry["match_cid"]   = result["client_id"]
            close_matches.append(entry)
        else:
            unmatched.append(entry)

    conn.close()

    ctx = base_ctx()
    ctx.update(
        active_page="import_audit",
        batches=batches,
        parse_errors=parse_errors,
        review_items=review_items,
        close_matches=close_matches,
        unmatched=unmatched,
        accept_threshold=ACCEPT_THRESHOLD,
        review_threshold=REVIEW_THRESHOLD,
    )
    return render_template("import_audit.html", **ctx)


@app.post("/api/audit/merge-client")
@login_required
def api_audit_merge_client():
    """Merge an unlogged client into a logged one (from the audit page)."""
    data       = _get_json_safe()
    discard_id = int(data["discard_id"])
    keep_id    = int(data["keep_id"])
    conn = get_connection()
    try:
        merge_client_into(conn, keep_id=keep_id, discard_id=discard_id, updated_ts=now())
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        conn.rollback()
        return jsonify({"success": False, "error": str(e)}), 500
    finally:
        conn.close()


# ── Season rollover (Epic #88 — ROLLOVER-1…6) ─────────────────────────────────


# ── OPS-5: serve RUNBOOK.md in the admin UI ──────────────────────────────────

@app.route("/admin/runbook")
@login_required
def admin_runbook():
    """OPS-5: render RUNBOOK.md as a readable HTML page accessible from the footer."""
    runbook_path = os.path.join(os.path.dirname(__file__), "docs", "RUNBOOK.md")
    try:
        with open(runbook_path, encoding="utf-8") as fh:
            raw_md = fh.read()
    except OSError:
        raw_md = "# RUNBOOK.md not found\n\nFile expected at `docs/RUNBOOK.md`."
    return render_template(
        "runbook.html",
        raw_md=raw_md,
        active_page="runbook",
    )


@app.route("/admin/runbook/raw")
@login_required
def admin_runbook_raw():
    """Serve raw RUNBOOK.md as plain text (for download / copy-paste)."""
    runbook_path = os.path.join(os.path.dirname(__file__), "docs", "RUNBOOK.md")
    try:
        with open(runbook_path, encoding="utf-8") as fh:
            content = fh.read()
    except OSError:
        content = "# RUNBOOK.md not found"
    from flask import Response
    return Response(content, mimetype="text/plain; charset=utf-8")


# ── DOC-HARD-3: Failed document (dead-letter) admin ──────────────────────────

@app.route("/admin/failed-docs")
@login_required
def failed_docs_admin():
    """DOC-HARD-3: list extraction dead-letter items; staff can retry or dismiss."""
    from extractor import MAX_ATTEMPTS
    ctx = base_ctx()
    ctx["active_page"] = "failed_docs"
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT
                eq.id AS eq_id, eq.doc_id, eq.attempts, eq.error_message,
                eq.created_at, eq.processed_at,
                rd.filename, rd.original_filename, rd.doc_type, rd.return_id,
                r.log_number, c.last_name, c.first_name, r.tax_year
            FROM extraction_queue eq
            JOIN return_documents rd ON eq.doc_id = rd.id AND rd.is_deleted = 0
            JOIN returns r ON eq.return_id = r.id
            JOIN clients c ON c.id = r.client_id
            WHERE eq.status = 'failed'
            ORDER BY eq.processed_at DESC
            LIMIT 200
            """
        ).fetchall()
    ctx["failed_docs"] = [dict(r) for r in rows]
    ctx["max_attempts"] = MAX_ATTEMPTS
    return render_template("failed_docs_admin.html", **ctx)


@app.post("/api/admin/documents/<int:doc_id>/retry")
@login_required
def api_admin_document_retry(doc_id: int):
    """DOC-HARD-3: reset a dead-letter extraction item back to pending for retry."""
    reviewer = session.get("username") or "staff"
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT id, return_id FROM extraction_queue WHERE doc_id = ? AND status = 'failed' "
            "ORDER BY id DESC LIMIT 1",
            (doc_id,),
        ).fetchone()
        if not row:
            return jsonify({"error": "No failed extraction found for this document"}), 404
        conn.execute(
            """
            UPDATE extraction_queue
            SET status = 'pending', attempts = 0, error_message = ?,
                processed_at = NULL
            WHERE id = ?
            """,
            (f"Manually retried by {reviewer}", row["id"]),
        )
        conn.commit()
        from extractor import _notify_extraction_worker
        _notify_extraction_worker()
    return jsonify({"success": True, "doc_id": doc_id})


# ── DEP-IMPORT: dependent match review ───────────────────────────────────────

@app.route("/admin/dependents-review")
@login_required
@role_required("admin")
def dependents_review_admin():
    """Review client_dependents rows flagged needs_review=1 from the Drake import."""
    ctx = base_ctx()
    ctx["active_page"] = "dependents_review"
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT
                cd.id, cd.client_id, cd.drake_dependent_id,
                cd.taxpayer_name,
                cd.first_name, cd.last_name, cd.date_of_birth, cd.relationship,
                cd.is_claimed_dependent, cd.hoh_qualifier_only,
                cd.match_confidence, cd.source,
                c.last_name  AS client_last,
                c.first_name AS client_first
            FROM client_dependents cd
            JOIN clients c ON c.id = cd.client_id
            WHERE cd.needs_review = 1
              AND cd.removed_for_ty2026 = 0
            ORDER BY cd.taxpayer_name, cd.client_id, cd.id
            """
        ).fetchall()
        total_clean = conn.execute(
            "SELECT COUNT(*) FROM client_dependents WHERE needs_review=0 AND removed_for_ty2026=0"
        ).fetchone()[0]
        total_review = len(rows)

    # Group by (taxpayer_name, client_id) so each Drake name → TaxLog client pair
    # is reviewed once rather than once per dependent.
    from collections import OrderedDict
    groups = OrderedDict()
    for r in rows:
        key = (r["taxpayer_name"] or "", r["client_id"])
        if key not in groups:
            groups[key] = {
                "taxpayer_name": r["taxpayer_name"] or "",
                "client_id":     r["client_id"],
                "client_last":   r["client_last"],
                "client_first":  r["client_first"],
                "match_confidence": r["match_confidence"],
                "dep_ids":       [],
                "dependents":    [],
            }
        groups[key]["dep_ids"].append(r["id"])
        groups[key]["dependents"].append({
            "id":           r["id"],
            "first_name":   r["first_name"],
            "last_name":    r["last_name"],
            "date_of_birth": r["date_of_birth"],
            "relationship": r["relationship"],
            "is_claimed_dependent": r["is_claimed_dependent"],
        })

    ctx["groups"] = list(groups.values())
    ctx["total_clean"] = total_clean
    ctx["total_review"] = total_review
    return render_template("dependents_review_admin.html", **ctx)


@app.post("/api/admin/dependents/<int:dep_id>/confirm")
@login_required
@role_required("admin")
def api_admin_dependent_confirm(dep_id: int):
    """Mark a client_dependent as confirmed (needs_review=0)."""
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT id FROM client_dependents WHERE id=?", (dep_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute(
            "UPDATE client_dependents SET needs_review=0 WHERE id=?", (dep_id,)
        )
        conn.commit()
    return jsonify({"ok": True})


@app.post("/api/admin/dependents/<int:dep_id>/remove")
@login_required
@role_required("admin")
def api_admin_dependent_remove(dep_id: int):
    """Flag a client_dependent as removed (removed_for_ty2026=1)."""
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT id FROM client_dependents WHERE id=?", (dep_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute(
            "UPDATE client_dependents SET removed_for_ty2026=1 WHERE id=?", (dep_id,)
        )
        conn.commit()
    return jsonify({"ok": True})


@app.post("/api/admin/dependents/confirm-all")
@login_required
@role_required("admin")
def api_admin_dependents_confirm_all():
    """Confirm all remaining needs_review=1 dependents at once."""
    with contextlib.closing(get_connection()) as conn:
        conn.execute(
            "UPDATE client_dependents SET needs_review=0 WHERE needs_review=1 AND removed_for_ty2026=0"
        )
        count = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
    return jsonify({"ok": True, "confirmed": count})


@app.post("/api/admin/dependents/confirm-all-ids")
@login_required
@role_required("admin")
def api_admin_dependents_confirm_ids():
    """Confirm a specific list of client_dependent ids (one taxpayer group)."""
    ids = (request.json or {}).get("ids", [])
    if not ids:
        return jsonify({"error": "No ids provided"}), 400
    placeholders = ",".join("?" * len(ids))
    with contextlib.closing(get_connection()) as conn:
        conn.execute(
            f"UPDATE client_dependents SET needs_review=0 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
    return jsonify({"ok": True})


@app.post("/api/admin/dependents/remove-ids")
@login_required
@role_required("admin")
def api_admin_dependents_remove_ids():
    """Remove (flag removed_for_ty2026=1) a specific list of client_dependent ids."""
    ids = (request.json or {}).get("ids", [])
    if not ids:
        return jsonify({"error": "No ids provided"}), 400
    placeholders = ",".join("?" * len(ids))
    with contextlib.closing(get_connection()) as conn:
        conn.execute(
            f"UPDATE client_dependents SET removed_for_ty2026=1 WHERE id IN ({placeholders})",
            ids,
        )
        conn.commit()
    return jsonify({"ok": True})


# ── SPOUSE-IMPORT: review page (mirrors dependents review — same confirm/reject pattern) ──

@app.route("/admin/spouses-review")
@login_required
@role_required("admin")
def spouses_review_admin():
    """Review spouses rows flagged needs_review=1 from the Drake import.
    Identical workflow to /admin/dependents-review: confirm clears the flag,
    confirmed rows prefill intake automatically — no separate apply step."""
    ctx = base_ctx()
    ctx["active_page"] = "spouses_review"
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT s.id, s.client_id, s.taxpayer_name,
                   s.first_name, s.last_name,
                   s.derived_last_name, s.middle_initial, s.date_of_birth,
                   s.match_confidence, s.source,
                   c.last_name  AS client_last,
                   c.first_name AS client_first
            FROM spouses s
            JOIN clients c ON c.id = s.client_id
            WHERE s.needs_review = 1
            ORDER BY s.match_confidence DESC, c.last_name, c.first_name
            """
        ).fetchall()
        total_clean = conn.execute(
            "SELECT COUNT(*) FROM spouses WHERE needs_review=0"
        ).fetchone()[0]

    ctx["rows"]         = [dict(r) for r in rows]
    ctx["total_review"] = len(rows)
    ctx["total_clean"]  = total_clean
    return render_template("spouses_review_admin.html", **ctx)


@app.post("/api/admin/spouses/<int:spouse_id>/confirm")
@login_required
@role_required("admin")
def api_admin_spouse_confirm(spouse_id: int):
    """Mark a spouses row as confirmed (needs_review=0) so it prefills at intake."""
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute("SELECT id FROM spouses WHERE id=?", (spouse_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute("UPDATE spouses SET needs_review=0 WHERE id=?", (spouse_id,))
        conn.commit()
    return jsonify({"ok": True})


@app.route("/admin/recovered-clients")
@login_required
@role_required("admin")
def recovered_clients_admin():
    """List [RECOVERED] placeholder clients for manual name entry."""
    ctx = base_ctx()
    ctx["active_page"] = "recovered_clients"
    with contextlib.closing(get_connection()) as conn:
        rows = conn.execute(
            """
            SELECT c.id AS client_id,
                   c.last_name, c.first_name,
                   r.intake_date, r.processor, r.client_status, r.tax_year
            FROM clients c
            LEFT JOIN returns r ON r.client_id = c.id
            WHERE c.last_name = '[RECOVERED]'
            ORDER BY r.intake_date DESC, c.id
            """
        ).fetchall()
    ctx["rows"] = [dict(r) | {"last_input": "", "first_input": ""} for r in rows]
    return render_template("recovered_clients_admin.html", **ctx)


@app.post("/api/admin/recovered-clients/<int:client_id>/rename")
@login_required
@role_required("admin")
def api_admin_recovered_client_rename(client_id: int):
    """Save a real name for a [RECOVERED] placeholder client."""
    data       = _get_json_safe() or {}
    last_name  = (data.get("last_name") or "").strip()
    first_name = (data.get("first_name") or "").strip() or None
    if not last_name:
        return jsonify({"error": "last_name is required"}), 400
    display    = f"{last_name}, {first_name}" if first_name else last_name
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute(
            "SELECT id FROM clients WHERE id=? AND last_name='[RECOVERED]'", (client_id,)
        ).fetchone()
        if not row:
            return jsonify({"error": "Client not found or already renamed"}), 404
        conn.execute(
            "UPDATE clients SET last_name=?, first_name=?, display_name=?, updated_at=? WHERE id=?",
            (last_name, first_name, display, now(), client_id),
        )
        conn.commit()
    return jsonify({"ok": True})


@app.route("/admin/audit-alerts")
@login_required
@role_required("admin")
def audit_alerts_admin():
    """Surface EF-Rejected, mislinked, and stale PROCESSING returns for staff action."""
    ctx = base_ctx()
    ctx["active_page"] = "audit_alerts"
    with contextlib.closing(get_connection()) as conn:
        ef_rejected = conn.execute(
            """SELECT c.id AS client_id, c.display_name,
                      r.id AS return_id, r.log_number, r.tax_year,
                      r.client_status, r.intake_date, r.processor,
                      r.drake_status_raw, r.notes_intake
               FROM returns r
               JOIN clients c ON c.id = r.client_id
               WHERE r.drake_status_raw = 'EF Rejected'
               ORDER BY r.intake_date DESC"""
        ).fetchall()

        mislinked = conn.execute(
            """SELECT c.id AS client_id, c.display_name,
                      r.id AS return_id, r.log_number, r.tax_year,
                      r.client_status, r.intake_date, r.processor,
                      r.notes_intake
               FROM returns r
               JOIN clients c ON c.id = r.client_id
               WHERE r.notes_intake LIKE '%MISLINKED AUDIT FLAG%'
               ORDER BY c.last_name"""
        ).fetchall()

        _stale_base = """
               SELECT c.id AS client_id, c.display_name,
                      r.id AS return_id, r.log_number, r.tax_year,
                      r.client_status, r.intake_date, r.processor,
                      r.drake_status_raw, r.notes_intake
               FROM returns r
               JOIN clients c ON c.id = r.client_id
               WHERE r.client_status = 'PROCESSING'
                 AND r.intake_date IS NOT NULL
                 AND r.intake_date < date('now', '-60 days')
                 AND (r.drake_status_raw IS NULL
                      OR r.drake_status_raw NOT IN (
                         'EF Accepted','EF Ext Accepted','E-Filed: YES','Printed'))"""
        # Current-season named returns (the real action items)
        stale_processing = conn.execute(
            _stale_base +
            " AND r.tax_year >= 2024"
            " AND c.display_name IS NOT NULL"
            " ORDER BY r.intake_date ASC"
        ).fetchall()
        # Prior-year or unnamed recovery artifacts — shown collapsed
        stale_processing_old = conn.execute(
            _stale_base +
            " AND (r.tax_year < 2024 OR c.display_name IS NULL)"
            " ORDER BY r.intake_date ASC"
        ).fetchall()

    ctx["ef_rejected"]           = [dict(r) for r in ef_rejected]
    ctx["mislinked"]             = [dict(r) for r in mislinked]
    ctx["stale_processing"]      = [dict(r) for r in stale_processing]
    ctx["stale_processing_old"]  = [dict(r) for r in stale_processing_old]
    return render_template("audit_alerts_admin.html", **ctx)


@app.post("/api/admin/audit-alerts/mislinked/<int:return_id>/dismiss")
@login_required
@role_required("admin")
def api_audit_alert_mislinked_dismiss(return_id: int):
    """Clear the MISLINKED AUDIT FLAG from a return's intake notes."""
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute("SELECT id, notes_intake FROM returns WHERE id=?", (return_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        import re
        cleaned = re.sub(r"\s*MISLINKED AUDIT FLAG[^\n]*", "", row["notes_intake"] or "").strip()
        conn.execute("UPDATE returns SET notes_intake=? WHERE id=?", (cleaned or None, return_id))
        conn.commit()
    return jsonify({"ok": True})


@app.post("/api/admin/spouses/<int:spouse_id>/reject")
@login_required
@role_required("admin")
def api_admin_spouse_reject(spouse_id: int):
    """Delete a wrong-match spouses row."""
    with contextlib.closing(get_connection()) as conn:
        row = conn.execute("SELECT id FROM spouses WHERE id=?", (spouse_id,)).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute("DELETE FROM spouses WHERE id=?", (spouse_id,))
        conn.commit()
    return jsonify({"ok": True})


@app.post("/api/admin/spouses/confirm-all")
@login_required
@role_required("admin")
def api_admin_spouses_confirm_all():
    """Confirm all pending needs_review=1 spouses rows at once."""
    with contextlib.closing(get_connection()) as conn:
        conn.execute("UPDATE spouses SET needs_review=0 WHERE needs_review=1")
        count = conn.execute("SELECT changes()").fetchone()[0]
        conn.commit()
    return jsonify({"ok": True, "confirmed": count})


# ── BACKUP-5: on-demand backup admin ─────────────────────────────────────────

@app.route("/admin/backup")
@login_required
def backup_admin():
    """BACKUP-5: admin page with manual backup trigger button."""
    ctx = base_ctx()
    ctx["active_page"] = "backup_admin"
    return render_template("backup_admin.html", **ctx)


@app.post("/api/admin/backup/run")
@login_required
def api_admin_backup_run():
    """BACKUP-5: trigger an on-demand backup; returns JSON result."""
    from backup import run_backup
    try:
        result = run_backup()
        return jsonify({
            "success": result.success,
            "message": result.message,
            "backup_file": result.backup_file,
            "size_bytes": result.size_bytes,
        }), (200 if result.success else 500)
    except Exception as exc:
        current_app.logger.exception("Manual backup failed")
        return jsonify({"success": False, "message": str(exc), "backup_file": None, "size_bytes": None}), 500


@app.route("/admin/season-rollover")
@login_required
def season_rollover_admin():
    if not can_run_season_rollover():
        abort(403)
    ctx = base_ctx()
    yr = date.today().year
    tay_conn = get_connection()
    try:
        active_intake_tax_year = get_active_intake_tax_year(tay_conn)
    finally:
        tay_conn.close()
    ctx.update({
        "active_page":               "season_rollover",
        "rollover_status":           season_rollover.NEW_ROLLOVER_STATUS,
        "default_source_tax_year":   yr - 1,
        "default_target_tax_year":    yr,
        "active_intake_tax_year":    active_intake_tax_year,
    })
    return render_template("season_rollover.html", **ctx)


@app.post("/api/admin/active-intake-tax-year")
@login_required
def api_admin_set_active_intake_tax_year():
    """The one place that changes what tax_year new /intake walk-ins get.

    Deliberately separate from the rollover preview/run endpoints below —
    changing the active intake year is a one-line settings write, not a
    data-copying operation, and an admin may need to set/correct it even
    if they never run (or haven't yet run) a rollover. Gated behind the
    same RBAC as season rollover since both are "turn of the season" admin
    actions that should happen together, once a year.
    """
    if not can_run_season_rollover():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    try:
        year = int(data["year"])
        confirm_year = int(data["confirm_year"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "year and confirm_year must be integers"}), 400
    if confirm_year != year:
        return jsonify({"error": "Confirmation failed: enter the new tax year to confirm."}), 400
    if year < 2000 or year > date.today().year + 1:
        return jsonify({"error": "That doesn't look like a plausible tax year."}), 400

    actor = (_session_username() or "").strip() or None
    ip = request.remote_addr
    conn = get_connection()
    try:
        before_year = get_active_intake_tax_year(conn)
        conn.execute("BEGIN IMMEDIATE")
        set_active_intake_tax_year(conn, year)
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    from audit_service import _enqueue_write
    _enqueue_write(
        user_id=actor, action="ACTIVE_INTAKE_TAX_YEAR_CHANGED", entity_type="app_settings",
        entity_id="active_intake_tax_year", before={"year": before_year}, after={"year": year},
        ip_address=ip, http_status=200,
    )
    logging.getLogger("taxops").warning(
        "ACTIVE_INTAKE_TAX_YEAR_CHANGED: %s -> %s by user=%s", before_year, year, actor,
    )
    return jsonify({"success": True, "active_intake_tax_year": year})


@app.post("/api/admin/season-rollover/preview")
@login_required
def api_admin_season_rollover_preview():
    if not can_run_season_rollover():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    try:
        source_year = int(data["source_year"])
        target_year = int(data["target_year"])
    except (KeyError, TypeError, ValueError):
        return jsonify({"error": "source_year and target_year are required integers"}), 400
    carry_raw = data.get("carry") if isinstance(data.get("carry"), dict) else data
    opts = season_rollover.carry_options_from_dict(carry_raw)

    conn = get_connection()
    try:
        out = season_rollover.rollover_preview_json(
            conn, source_year=source_year, target_year=target_year, options=opts
        )
        return jsonify(out)
    finally:
        conn.close()


@app.post("/api/admin/season-rollover/run")
@login_required
def api_admin_season_rollover_run():
    if not can_run_season_rollover():
        return jsonify({"error": "Forbidden"}), 403
    data = request.get_json(silent=True) or {}
    try:
        source_year = int(data["source_year"])
        target_year = int(data["target_year"])
        confirmation_year = int(data["confirmation_year"])
    except (KeyError, TypeError, ValueError):
        return jsonify(
            {"error": "source_year, target_year, and confirmation_year must be integers"},
        ), 400

    if confirmation_year != target_year:
        return jsonify({"error": "Confirmation failed: enter the target tax year to confirm."}), 400

    carry_raw = data.get("carry") if isinstance(data.get("carry"), dict) else data
    opts = season_rollover.carry_options_from_dict(carry_raw)

    actor = (_session_username() or "").strip() or None
    ts = now()

    conn = get_connection()
    try:
        result = season_rollover.rollover_commit(
            conn,
            source_year=source_year,
            target_year=target_year,
            options=opts,
            actor=actor,
            ts=ts,
        )
        if not result.get("ok"):
            return jsonify({"ok": False, "error": result.get("error", "Rollover failed")}), 400

        report = result.get("report") or {}
        csv_body = season_rollover.build_rollover_report_csv(report)
        session["season_rollover_export_csv"] = csv_body
        session["season_rollover_export_filename"] = f"season_rollover_ty{target_year}_{ts[:10]}.csv"

        created = report.get("created") or []
        skipped = report.get("skipped") or []
        noop_msg = None
        if not created and skipped:
            noop_msg = (
                "No new returns created — likely an idempotent repeat (clients already "
                f"have a TY{target_year} return)."
            )
        return jsonify({
            "ok": True,
            "created_count":   len(created),
            "skipped_count": len(skipped),
            "noop_message":    noop_msg,
            "export_ready": True,
        })
    finally:
        conn.close()


@app.get("/admin/season-rollover/export.csv")
@login_required
def download_season_rollover_csv():
    if not can_run_season_rollover():
        abort(403)
    csv_text = session.get("season_rollover_export_csv")
    if csv_text is None:
        abort(404)
    fname = session.get("season_rollover_export_filename") or "season_rollover_report.csv"
    return Response(
        "\ufeff" + csv_text,
        mimetype="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{fname}"'},
    )


# ── AUDIT admin (AUDIT-3…AUDIT-7) ───────────────────────────────────────────


def _audit_date_range_filters():
    df = (request.args.get("date_from") or "").strip() or None
    dt = (request.args.get("date_to") or "").strip() or None
    date_from_iso = None
    date_to_excl = None
    try:
        if df:
            date_from_iso = f"{date.fromisoformat(df).isoformat()}T00:00:00"
        if dt:
            end_day = date.fromisoformat(dt) + timedelta(days=1)
            date_to_excl = f"{end_day.isoformat()}T00:00:00"
    except ValueError:
        pass
    return date_from_iso, date_to_excl


@app.route("/admin/audit-log")
@login_required
def audit_log_admin():
    user_f = (request.args.get("user_id") or "").strip() or None
    action_f = (request.args.get("action") or "").strip() or None
    entity_type_f = (request.args.get("entity_type") or "").strip() or None
    entity_id_f = (request.args.get("entity_id") or "").strip() or None
    date_from_iso, date_to_excl = _audit_date_range_filters()

    try:
        page = max(1, int(request.args.get("page") or "1"))
    except ValueError:
        page = 1
    per_page = 75
    offset = (page - 1) * per_page

    conn = get_connection()
    try:
        retention_years = get_audit_retention_years(conn)
        users_rows = conn.execute(
            """SELECT DISTINCT user_id FROM audit_log
               WHERE user_id IS NOT NULL AND TRIM(user_id) != ''
               ORDER BY user_id LIMIT 400"""
        ).fetchall()
        users_list = [r["user_id"] for r in users_rows]

        cnt, rows = query_audit_logs(
            conn,
            user_id=user_f,
            action_contains=action_f,
            entity_type=entity_type_f,
            entity_id=entity_id_f,
            date_from=date_from_iso,
            date_to=date_to_excl,
            limit=per_page,
            offset=offset,
        )
    finally:
        conn.close()

    total_pages = max(1, (cnt + per_page - 1) // per_page) if cnt else 1

    ctx = base_ctx()
    ctx.update(
        active_page="audit_log",
        rows=rows,
        total=cnt,
        page=page,
        total_pages=total_pages,
        per_page=per_page,
        users_list=users_list,
        retention_years=retention_years,
        filters={
            "user_id": user_f or "",
            "action": action_f or "",
            "entity_type": entity_type_f or "",
            "entity_id": entity_id_f or "",
            "date_from": (request.args.get("date_from") or "").strip(),
            "date_to": (request.args.get("date_to") or "").strip(),
        },
    )
    return render_template("audit_log.html", **ctx)


@app.route("/admin/audit-log/<int:entry_id>")
@login_required
def audit_log_detail(entry_id: int):
    conn = get_connection()
    try:
        row = fetch_audit_entry(conn, entry_id)
    finally:
        conn.close()
    if not row:
        abort(404)
    rowd = dict(row)
    diff_chunks = format_json_diff_styled_chunks(rowd.get("before_json"), rowd.get("after_json"))
    ctx = base_ctx()
    ctx.update(active_page="audit_log", entry=rowd, diff_chunks=diff_chunks)
    return render_template("audit_log_detail.html", **ctx)


@app.get("/admin/audit-log/export.csv")
@login_required
def audit_log_export_csv():
    user_f = (request.args.get("user_id") or "").strip() or None
    action_f = (request.args.get("action") or "").strip() or None
    entity_type_f = (request.args.get("entity_type") or "").strip() or None
    entity_id_f = (request.args.get("entity_id") or "").strip() or None
    date_from_iso, date_to_excl = _audit_date_range_filters()

    conn = get_connection()
    try:
        buf = write_audit_export_csv(
            conn,
            filters={
                "user_id": user_f,
                "action_contains": action_f,
                "entity_type": entity_type_f,
                "entity_id": entity_id_f,
                "date_from": date_from_iso,
                "date_to": date_to_excl,
            },
        )
    finally:
        conn.close()

    name = sanitize_filename_audit(
        f"audit_export_{date.today().isoformat()}_{secrets.token_hex(4)}.csv"
    )
    return send_file(
        io.BytesIO(buf.getvalue().encode("utf-8")),
        as_attachment=True,
        download_name=name,
        mimetype="text/csv; charset=utf-8",
    )


@app.post("/admin/audit-log/retention")
@login_required
def audit_log_retention_update():
    try:
        years = int((request.form.get("retention_years") or "").strip())
    except ValueError:
        flash("Retention years must be a whole number.", "error")
        return redirect(url_for("audit_log_admin"))

    conn = get_connection()
    try:
        set_audit_retention_years(conn, years)
        n = purge_audit_logs_older_than(conn, years)
        conn.commit()
        flash(f"Retention saved ({years} yr). Immediate purge removed {n} row(s).", "success")
    finally:
        conn.close()
    return redirect(url_for("audit_log_admin"))


# ── Startup ───────────────────────────────────────────────────────────────────

def register_workers(flask_app) -> None:
    """REL-3: initialise DB, seed users, and start all background daemons.

    Call this from *any* entry point (python app.py, waitress-serve, tests that
    need background workers) rather than relying on __main__ guard.

    Supported single entry point remains: ``python taxops/app.py``
    (``waitress-serve taxops.app:app`` skips DB migration; run register_workers
    explicitly or use the python app.py entry point instead).
    """
    _log = logging.getLogger(__name__)

    conn = get_connection()
    init_db(conn)
    seeded = bootstrap_auth_user(conn)
    if seeded:
        _log.info(
            "SEC-2: auth_users bootstrapped from TAXOPS_USER env var. "
            "Consider unsetting TAXOPS_PASS after verifying login works."
        )
    conn.close()

    _taxops_env = os.environ.get("TAXOPS_ENV", "").lower()
    if _taxops_env == "production" and not flask_app.config.get("SESSION_COOKIE_SECURE"):
        _log.warning(
            "SEC-3: SESSION_COOKIE_SECURE is False in a production environment. "
            "Session cookies will be sent over plain HTTP. "
            "Set app.config['SESSION_COOKIE_SECURE'] = True once TLS terminates in front of this server."
        )

    start_mail_watcher(flask_app)
    start_extraction_worker(flask_app)
    try:
        from accounting_worker import start_accounting_worker
        start_accounting_worker(flask_app)
    except Exception as ex:
        _log.warning("Accounting worker startup skipped: %s", ex)
    # Note: the fasttext sender-classifier (`classifier.py`) and the AI chat
    # assistant's `chat_cache` module were both removed in the holding-area
    # email rewrite (commit e188a1a) — do not reintroduce startup threads
    # that import them here. See taxops-invariants.mdc.


@app.post("/api/admin/reset-and-reimport")
@login_required
def api_admin_reset_and_reimport():
    """
    Controlled DB deduplication + forced re-import of every CSV in data/incoming/.

    Requirements:
      - Admin role only
      - Exact confirmation string "RESET AND REIMPORT" in request body
      - Creates a timestamped DB backup before touching anything
      - Runs _deduplicate_existing_records
      - Re-runs every CSV through the upsert importer (no new files moved; upsert is safe)
      - Audit-logs the action
    """
    if _resolve_current_role() != "admin":
        return jsonify({"error": "Forbidden — admin only"}), 403

    data = _get_json_safe() or {}
    if data.get("confirm") != "RESET AND REIMPORT":
        return jsonify({"error": "Confirmation required: send {\"confirm\": \"RESET AND REIMPORT\"}"}), 400

    import shutil as _shutil
    from datetime import datetime as _dt
    from pathlib import Path as _Path
    from db import _deduplicate_existing_records
    import main as _main_mod
    from config import INCOMING_DIR, DB_PATH as _DB_PATH

    actor = _session_username() or "unknown"

    # 1. Timestamped backup
    backup_path = _DB_PATH + ".backup." + _dt.now().strftime("%Y%m%d_%H%M%S")
    try:
        _shutil.copy2(_DB_PATH, backup_path)
        current_app.logger.info("reset-and-reimport: DB backup created: %s", backup_path)
    except Exception as exc:
        return jsonify({"error": f"Backup failed: {exc}"}), 500

    result_summary: dict = {
        "backup_file": backup_path,
        "dedup_removed": 0,
        "files_processed": [],
        "totals": {
            "clients_created": 0,
            "clients_updated": 0,
            "returns_created": 0,
            "returns_updated": 0,
            "rows_skipped": 0,
            "errors": [],
        },
    }

    conn = get_connection()
    try:
        # 2. Dedup existing records
        conn.execute("BEGIN")
        removed = _deduplicate_existing_records(conn)
        conn.commit()
        result_summary["dedup_removed"] = removed

        # 3. Re-run every CSV in data/incoming/
        from utils import ImportResult
        from datetime import date as _date

        for csv_file in sorted(_Path(INCOMING_DIR).glob("*.csv")):
            from main import _drake_year as _dy
            from drake_importer import process_drake_csv, _open_and_detect
            from importer import process_csv

            try:
                from utils import hash_file
                file_hash = hash_file(str(csv_file))
                batch_id = _main_mod.create_batch(conn, csv_file.name, file_hash + "_reimport_" + actor)
            except Exception:
                batch_id = _main_mod.create_batch(conn, csv_file.name, str(csv_file.stat().st_mtime))

            stats: ImportResult | None = None
            try:
                conn.execute("BEGIN")
                drake_year = _dy(csv_file.name)
                if drake_year is not None:
                    stats = process_drake_csv(conn, str(csv_file), batch_id, csv_file.name, drake_year)
                else:
                    # Try Drake CSM detection first; fall back to manual-log importer
                    _, fmt = _open_and_detect(str(csv_file))
                    if fmt != "UNKNOWN":
                        # CSM or TAX_OPS format — use current year - 1 as tax year
                        ty = _date.today().year - 1
                        stats = process_drake_csv(conn, str(csv_file), batch_id, csv_file.name, ty)
                    else:
                        stats = process_csv(conn, str(csv_file), batch_id, csv_file.name)
                conn.commit()
            except Exception as exc:
                conn.rollback()
                current_app.logger.exception("reset-and-reimport: error processing %s", csv_file.name)
                result_summary["files_processed"].append({
                    "filename": csv_file.name,
                    "error": str(exc),
                })
                result_summary["totals"]["errors"].append(f"{csv_file.name}: {exc}")
                continue

            if stats:
                result_summary["files_processed"].append({
                    "filename":        csv_file.name,
                    "source":          stats.source,
                    "rows":            stats.row_count,
                    "clients_created": stats.created_clients,
                    "clients_updated": stats.updated_clients,
                    "returns_created": stats.created_returns,
                    "returns_updated": stats.updated_returns,
                    "errors":          stats.errors,
                    "duration_s":      round(stats.duration_seconds, 2),
                })
                t = result_summary["totals"]
                t["clients_created"] += stats.created_clients
                t["clients_updated"] += stats.updated_clients
                t["returns_created"] += stats.created_returns
                t["returns_updated"] += stats.updated_returns
                t["errors"].extend(stats.errors)

        # 4. Audit the action
        try:
            from audit_service import _enqueue_write
            _enqueue_write(
                user_id=actor,
                action="ADMIN_RESET_AND_REIMPORT",
                entity_type="system",
                entity_id=None,
                before_data=None,
                after_data=result_summary,
            )
        except Exception:
            pass

    finally:
        conn.close()

    current_app.logger.info(
        "reset-and-reimport by %s: dedup_removed=%s files=%s clients_created=%s returns_created=%s",
        actor,
        result_summary["dedup_removed"],
        len(result_summary["files_processed"]),
        result_summary["totals"]["clients_created"],
        result_summary["totals"]["returns_created"],
    )
    return jsonify({"success": True, **result_summary}), 200


if __name__ == "__main__":
    register_workers(app)

    host = (os.environ.get("WAITRESS_HOST") or os.environ.get("HOST") or "0.0.0.0").strip() or "0.0.0.0"
    port_raw = (
        os.environ.get("WAITRESS_PORT")
        or os.environ.get("PORT")
        or "5000"
    )
    try:
        port = int(str(port_raw).strip())
    except ValueError:
        port = 5000

    print(f"TaxOps running at http://localhost:{port}", flush=True)
    debug_mode = os.environ.get("FLASK_DEBUG", "0") == "1"

    # WSGI-2 (#138): Production/offices use Waitress; keep FLASK_DEBUG=1 only for interactive dev + reloader.
    if debug_mode:
        app.run(host=host, port=port, debug=True, use_reloader=True)
    else:
        from waitress import serve

        try:
            threads = int(os.environ.get("WAITRESS_THREADS", "8"))
        except ValueError:
            threads = 8
        threads = max(1, min(threads, 64))

        print(
            f"Waitress listening on http://{host}:{port}/ (threads={threads})",
            flush=True,
        )
        serve(app, host=host, port=port, threads=threads)
