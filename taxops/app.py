from __future__ import annotations

import functools
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import (
    Flask, abort, flash, jsonify, redirect, render_template,
    request, session, url_for,
)

import json
import tempfile

from config import APP_ENV
from csv_analyzer import analyze, iter_data_rows, normalize_status
from db import get_connection, init_db
from merge_ops import merge_client_into
from name_matcher import find_client as fuzzy_find_client, is_business, parse_name, _all_clients_cache
from normalizer import normalize_date, normalize_currency, normalize_string, canonical_status, is_locked_status
from preparer import (
    normalize_preparer,
    preparer_dropdown_options,
    preparer_filter_match_values,
    preparer_list_label,
)
from source_compare import (
    discover_default_paths,
    list_csv_basenames,
    run_compare,
    safe_resolve_csv,
)
from utils import now

app = Flask(__name__)

from ai_routes import ai as ai_blueprint
app.register_blueprint(ai_blueprint)

app.jinja_env.globals["preparer_list_label"] = preparer_list_label

# Secret key for signing session cookies.
# Set TAXOPS_SECRET env-var in production; a random fallback is fine for dev.
app.secret_key = os.environ.get("TAXOPS_SECRET", os.urandom(24))

# Login credentials — override via environment variables.
_LOGIN_USER = os.environ.get("TAXOPS_USER", "info")
_LOGIN_PASS = os.environ.get("TAXOPS_PASS", "2703Tax")


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


def login_required(f):
    @functools.wraps(f)
    def wrapper(*args, **kwargs):
        if not session.get("logged_in"):
            return redirect(url_for("login", next=request.path))
        return f(*args, **kwargs)
    return wrapper


@app.after_request
def _security_headers(response):
    """Add basic security headers — this app is internal-only."""
    response.headers["X-Frame-Options"]        = "DENY"
    response.headers["X-Content-Type-Options"] = "nosniff"
    response.headers["Referrer-Policy"]        = "same-origin"
    response.headers["Cache-Control"]          = "no-store"
    return response

# ── Workflow constants ────────────────────────────────────────────────────────

STATUS_FLOW = ["PROCESSING", "HOLD", "FINALIZE", "PICKUP", "EFILE READY", "LOG OUT", "REJECTED"]

# Rejected-return client contact tracking (stored on returns; privacy: no SSN fields)
CONTACT_STATUS_VALUES = ("not_contacted", "contacted", "follow_up_needed", "resolved")
CONTACT_LABELS = {
    "not_contacted":    "Not contacted",
    "contacted":        "Contacted",
    "follow_up_needed": "Follow-up needed",
    "resolved":         "Resolved (contact)",
}

STATUS_BADGE = {
    "PROCESSING":  "bg-sky-50 text-sky-700 border-sky-200",
    "HOLD":        "bg-orange-50 text-orange-700 border-orange-200",
    "FINALIZE":    "bg-yellow-50 text-yellow-700 border-yellow-200",
    "PICKUP":      "bg-teal-50 text-teal-700 border-teal-200",
    "EFILE READY": "bg-indigo-50 text-indigo-700 border-indigo-200",
    "LOG OUT":     "bg-slate-100 text-slate-500 border-slate-200",
    "REJECTED":    "bg-red-50 text-red-700 border-red-200",
}

STATUS_DOT = {
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
    "processor", "verified", "email_marker",
    "intake_date", "date_emailed", "pickup_date", "logout_date", "updated_date",
    "efile_date", "ack_date",
    "is_amended", "has_w7", "is_extension",
    "transfer_flag", "transfer_2025_flag", "transfer_2026_flag",
    "signatures_given", "signatures_received",
}

# Fields that live in the clients table
CLIENT_EDITABLE = {"display_name", "referred_by", "referral_flag"}

# Fields that live in the payments table
PAYMENT_EDITABLE = {
    "total_fee", "fee_paid", "receipt_number",
    "cc_fee", "zelle_or_check_ref", "cash_or_qpay_ref",
    "bank_deposit", "refund_amount", "payment_method",
}

CARD_FEE_RATE = 0.03   # 3 % card processing surcharge

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
    r.created_at, r.updated_at,
    c.id   AS client_id,
    c.last_name, c.first_name, c.display_name,
    c.referral_flag, c.referred_by, c.ssn_last4,
    p.id   AS payment_id,
    p.total_fee, p.fee_paid, p.receipt_number,
    p.cc_fee, p.zelle_or_check_ref, p.cash_or_qpay_ref,
    p.refund_amount, p.bank_deposit, p.payment_method,
    r.signatures_given, r.signatures_received,
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


def _enrich(r: dict) -> dict:
    total = _to_float(r.get("total_fee"))
    paid  = _to_float(r.get("fee_paid"))
    r["balance"]      = round(total - paid, 2) if total else None
    r["paid_in_full"] = bool(total and paid >= total)
    r["color"]        = STATUS_DOT.get(r.get("client_status") or "", "dot-slate")
    r["badge_class"]  = STATUS_BADGE.get(r.get("client_status") or "", "bg-slate-100 text-slate-500 border-slate-200")
    first = r.get("first_name") or ""
    last  = r.get("last_name")  or ""
    r["name_full"] = r.get("display_name") or (f"{last}, {first}".strip(", ") if first else last)
    r["forms"]        = _form_badges(r)
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
    conn = get_connection()
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
    sql   = f"{_SELECT} {where} ORDER BY CASE WHEN r.log_number IS NULL OR r.log_number='' THEN 1 ELSE 0 END, CAST(r.log_number AS INTEGER), r.id"

    rows = conn.execute(sql, params).fetchall()
    conn.close()
    return [_enrich(dict(r)) for r in rows]


def get_one(return_id: int) -> dict | None:
    conn = get_connection()
    row  = conn.execute(f"{_SELECT} WHERE r.id = ?", (return_id,)).fetchone()
    conn.close()
    return _enrich(dict(row)) if row else None


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


def base_ctx(year: int | None = None) -> dict:
    today_year = date.today().year
    # Never let the season picker go backwards to a tax year.
    # Callers should pass the intake/season year, not the tax year.
    y = year if (year and year >= today_year) else today_year
    conn = get_connection()
    pending_review = conn.execute(
        "SELECT COUNT(*) n FROM review_queue WHERE status='pending'"
    ).fetchone()["n"]
    # Rejected returns — always pulled regardless of season filter
    rejected_rows = conn.execute(
        f"{_SELECT} WHERE r.client_status = 'REJECTED' ORDER BY r.updated_at DESC"
    ).fetchall()
    conn.close()
    rejected = [_enrich(dict(r)) for r in rejected_rows]
    return {
        "current_year":         y,
        "status_flow":          STATUS_FLOW,
        "status_badge":         STATUS_BADGE,
        "status_dot":           STATUS_DOT,
        "status_counts":        get_status_counts(y),
        "totals":               get_totals(y),
        "processors":           preparer_dropdown_options(get_processors(y)),
        "app_env":              APP_ENV,
        "privacy_mode":         privacy_mode_enabled(),
        "pending_review_count": pending_review,
        "rejected_returns":     rejected,
        "rejected_count":       len(rejected),
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
            r.id, r.tax_year, r.intake_date, r.is_extension, r.has_w7,
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
        if username == _LOGIN_USER and password == _LOGIN_PASS:
            session["logged_in"] = True
            session["username"]  = username
            next_url = request.args.get("next") or url_for("dashboard")
            return redirect(next_url)
        error = "Invalid username or password."
    return render_template("login.html", error=error)


@app.route("/logout")
def logout():
    session.clear()
    return redirect(url_for("login"))


# ── Page routes ───────────────────────────────────────────────────────────────

@app.route("/")
@login_required
def dashboard():
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
    }
    returns = query_returns(filters)
    ctx = base_ctx(year)
    ctx.update({"active_page": "dashboard", "returns": returns, "filters": filters})
    return render_template("dashboard.html", **ctx)


@app.route("/return/<int:return_id>")
@login_required
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
    conn.close()
    notes_payload = [dict(n) for n in notes]
    if privacy_mode_enabled():
        for note in notes_payload:
            note["note_text"] = _mask_value(note.get("note_text"))

    # Always use current calendar year for the season picker — never the return's tax year.
    ctx = base_ctx(date.today().year)
    ctx.update({
        "active_page":   "dashboard",
        "ret":           ret,
        "notes":         notes_payload,
        "events":        [dict(e) for e in events],
        "missing_docs":  [dict(d) for d in missing_docs],
        "contact_labels": CONTACT_LABELS,
    })
    return render_template("return_detail.html", **ctx)


@app.route("/logout-queue")
@login_required
def logout_queue():
    year = int(request.args.get("year", date.today().year))
    conn = get_connection()
    rows = conn.execute(
        f"{_SELECT} WHERE (strftime('%Y', r.intake_date) = ? OR (r.intake_date IS NULL AND r.tax_year = ?)) "
        "AND r.client_status IN ('PICKUP','EFILE READY') ORDER BY CAST(r.log_number AS INTEGER)",
        (str(year), year - 1),
    ).fetchall()
    conn.close()
    ctx = base_ctx(year)
    ctx.update({
        "active_page": "logout",
        "returns":     [_enrich(dict(r)) for r in rows],
        "today":       date.today().isoformat(),
    })
    return render_template("logout_queue.html", **ctx)


@app.route("/efile-queue")
@login_required
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
    conn.close()
    ctx = base_ctx(year)
    ctx.update({
        "active_page": "efile",
        "returns":     [_enrich(dict(r)) for r in rows],
        "sort":        sort,
        "today":       date.today().isoformat(),
    })
    return render_template("efile_queue.html", **ctx)


@app.route("/efile-queue/export")
@login_required
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

    error = None
    success = None

    if request.method == "POST":
        f = request.form
        sigs_given    = 1 if f.get("signatures_given") else 0
        sigs_received = 1 if f.get("signatures_received") else 0
        method        = f.get("payment_method", "").strip()
        base_fee      = _to_float(f.get("total_fee"))
        cc_fee        = round(base_fee * CARD_FEE_RATE, 2) if method == "Card/Visa" else 0.0
        fee_paid      = round(base_fee + cc_fee, 2)
        receipt       = normalize_string(f.get("receipt_number")) or None
        pickup_date   = normalize_string(f.get("pickup_date")) or None
        notes         = normalize_string(f.get("notes")) or None

        ready = sigs_received and fee_paid > 0 and receipt
        new_status = "EFILE READY" if ready else ret.get("client_status")
        if is_locked_status(ret.get("client_status")):
            new_status = ret.get("client_status")

        conn.execute(
            "UPDATE returns SET signatures_given=?, signatures_received=?, "
            "pickup_date=COALESCE(?,pickup_date), client_status=?, updated_at=? WHERE id=?",
            (sigs_given, sigs_received, pickup_date, new_status, datetime.now().isoformat(), return_id),
        )

        pay_row = conn.execute("SELECT id FROM payments WHERE return_id=?", (return_id,)).fetchone()
        if pay_row:
            conn.execute(
                "UPDATE payments SET total_fee=?, cc_fee=?, fee_paid=?, "
                "payment_method=?, receipt_number=COALESCE(?,receipt_number) WHERE return_id=?",
                (base_fee or None, cc_fee or None, fee_paid or None,
                 method or None, receipt, return_id),
            )
        else:
            conn.execute(
                "INSERT INTO payments (return_id, total_fee, cc_fee, fee_paid, payment_method, receipt_number) "
                "VALUES (?,?,?,?,?,?)",
                (return_id, base_fee or None, cc_fee or None, fee_paid or None, method or None, receipt),
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
@login_required
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
    if request.method == "GET":
        ctx = base_ctx()
        ctx.update({
            "active_page": "intake",
            "today": date.today().isoformat(),
            "error": None,
            "habit_profile": None,
        })
        return render_template("intake.html", **ctx)

    # POST — create records
    f = request.form
    ts = now()
    today_iso = date.today().isoformat()

    last_name  = (f.get("last_name") or "").strip().upper()
    first_name = (f.get("first_name") or "").strip().upper()
    if not last_name:
        ctx = base_ctx()
        ctx.update({"active_page": "intake", "today": today_iso, "error": "Last name is required.", "prefill": {}})
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

    conn = get_connection()
    try:
        tax_year = _i("tax_year") or date.today().year

        # ── Auto log number (max + 1 for this tax year) ───────────────────────
        row = conn.execute(
            "SELECT MAX(CAST(log_number AS INTEGER)) AS mx FROM returns WHERE tax_year = ?",
            (tax_year,),
        ).fetchone()
        log_number = str((row["mx"] or 0) + 1)

        # ── Client — insert new or update existing (re-intake) ────────────────
        existing_client_id = _i("client_id")
        if existing_client_id:
            conn.execute(
                """
                UPDATE clients SET
                    last_name=?, first_name=?, ssn_last4=?,
                    spouse_last_name=?, spouse_first_name=?,
                    taxpayer_dob=?, spouse_dob=?,
                    taxpayer_occupation=?, spouse_occupation=?,
                    taxpayer_phone=?, taxpayer_cell=?, taxpayer_work_phone=?,
                    spouse_cell=?, spouse_work_phone=?,
                    taxpayer_email=?, spouse_email=?,
                    address=?, referral_flag=?, referred_by=?,
                    is_new_client=0, prior_year_log=?, updated_at=?
                WHERE id=?
                """,
                (
                    last_name, first_name, _v("ssn_last4"),
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
                    last_name, first_name, _v("ssn_last4"),
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
                _n("reprocess_fee"), _n("discount_amount"), _n("special_discount"),
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
                  (return_id, full_name, ssn_last4, relationship, date_of_birth, medi_cal, created_at)
                VALUES (?,?,?,?,?,?,?)
                """,
                (
                    return_id, name,
                    _v(f"dep_ssn_{i}"),
                    _v(f"dep_rel_{i}"),
                    _v(f"dep_dob_{i}"),
                    1 if f.get(f"dep_medicaid_{i}") else 0,
                    ts,
                ),
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
        return redirect(f"/return/{return_id}")

    except Exception as exc:
        conn.rollback()
        ctx = base_ctx()
        ctx.update({"active_page": "intake", "today": today_iso, "error": str(exc)})
        return render_template("intake.html", **ctx), 500
    finally:
        conn.close()


# ── CSV Upload / Analyze ──────────────────────────────────────────────────────

@app.route("/upload", methods=["GET"])
@login_required
def upload_get():
    ctx = base_ctx()
    ctx.update({"active_page": "upload", "error": None})
    return render_template("upload.html", **ctx)


@app.route("/upload/preview", methods=["POST"])
@login_required
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
@login_required
def upload_confirm():
    """Execute import using the analysis result confirmed by staff."""
    data       = request.get_json(force=True)
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
        # No match — create new client
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
@login_required
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
@login_required
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
@login_required
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
@login_required
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
@login_required
def review_resolve():
    """Staff resolves a review_queue item.

    JSON body:
      queue_id  : int
      action    : 'confirm' | 'new' | 'link'
      client_id : int  (required for 'link'; ignored otherwise)
    """
    data     = request.get_json(force=True)
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
               MAX(r.tax_year) AS last_year
        FROM clients c
        LEFT JOIN returns r ON r.client_id = c.id
        WHERE lower(c.last_name) LIKE ? OR lower(c.first_name) LIKE ?
           OR lower(COALESCE(c.display_name,'')) LIKE ?
        GROUP BY c.id
        ORDER BY c.last_name, c.first_name
        LIMIT 12
        """,
        (qp, qp, qp),
    ).fetchall()
    conn.close()
    results = []
    for r in rows:
        first = r["first_name"] or ""
        last  = r["last_name"]  or ""
        name  = r["display_name"] or (f"{last}, {first}".strip(", ") if first else last)
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

    return jsonify({
        "client":      data,
        "last_return": last_return,
        "dependents":  deps,
        "habit_profile": habit_profile,
    })


@app.get("/api/search")
@login_required
def api_search():
    q    = request.args.get("q", "").strip()
    year = int(request.args.get("year", date.today().year))
    if not q:
        return jsonify([])
    results = query_returns({"year": year, "q": q})
    return jsonify([
        {
            "id":         r["id"],
            "log_number": r["log_number"],
            "name":       (f"XXXXX #{r['id']}" if privacy_mode_enabled() else r["name_full"]),
            "status":     r["client_status"],
            "badge":      r["badge_class"],
            "tax_year":   r["tax_year"],
        }
        for r in results[:12]
    ])


@app.post("/api/privacy-mode")
@login_required
def api_privacy_mode():
    data = request.get_json(force=True) if request.data else {}
    enabled = data.get("enabled")
    session["privacy_mode"] = bool(enabled)
    return jsonify({"success": True, "privacy_mode": bool(session.get("privacy_mode"))})


@app.post("/api/return/<int:return_id>/status")
@login_required
def api_status(return_id: int):
    data       = request.get_json(force=True)
    new_status = (data.get("status") or "").upper().strip()
    if new_status not in STATUS_FLOW:
        return jsonify({"error": "Invalid status"}), 400

    conn = get_connection()
    row  = conn.execute("SELECT * FROM returns WHERE id=?", (return_id,)).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Not found"}), 404

    old_status  = row["client_status"]
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
        "badge_class":   STATUS_BADGE.get(new_status, "bg-slate-100 text-slate-500 border-slate-200"),
    })


@app.post("/api/return/<int:return_id>/field")
@login_required
def api_field(return_id: int):
    data  = request.get_json(force=True)
    field = (data.get("field") or "").strip()
    value = data.get("value")
    if field == "processor":
        value = normalize_preparer(value) if (value is not None and str(value).strip() != "") else None

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
@login_required
def api_note(return_id: int):
    data = request.get_json(force=True)
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
@login_required
def api_return_contact(return_id: int):
    """Update client-contact follow-up fields for REJECTED returns."""
    data  = request.get_json(force=True) or {}
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
@login_required
def api_missing_doc_add(return_id: int):
    data = request.get_json(force=True)
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
@login_required
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
@login_required
def merge_clients_page():
    pairs = _merge_pairs_for_session()
    ctx = base_ctx(date.today().year)
    ctx.update({"active_page": "merge", "pairs": pairs})
    return render_template("merge_clients.html", **ctx)


@app.post("/api/merge-clients")
@login_required
def api_merge_clients():
    """
    Merge 'discard' client into 'keep' client.
    Moves all returns (and review_queue refs) from discard → keep, then deletes discard.
    """
    data       = request.get_json(force=True)
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

        keep_name = (keep["display_name"] or
                     f"{keep['last_name']}, {keep['first_name']}".strip(", "))
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
    data = request.get_json(force=True)
    skipped = session.get("merge_skipped", [])
    pair_key = f"{min(data['keep_id'], data['discard_id'])}-{max(data['keep_id'], data['discard_id'])}"
    if pair_key not in skipped:
        skipped.append(pair_key)
    session["merge_skipped"] = skipped
    return jsonify({"success": True})


# ── E-file Batches ────────────────────────────────────────────────────────────

@app.post("/efile-batch/create")
@login_required
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
        for rid in return_ids:
            try:
                rid = int(rid)
            except ValueError:
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
        flash(f"Batch #{batch_id} created with {added} return(s).", "success")
        return redirect(url_for("efile_batch_detail", batch_id=batch_id))
    except Exception as e:
        conn.rollback()
        flash(f"Error creating batch: {e}", "error")
        return redirect(url_for("efile_queue"))
    finally:
        conn.close()


@app.route("/efile-batch/<int:batch_id>")
@login_required
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
        f"JOIN returns r ON r.id = i.return_id "
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
@login_required
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
@login_required
def efile_batch_transmit(batch_id: int):
    """Mark batch as transmitted (sent to IRS via Drake)."""
    conn = get_connection()
    batch = conn.execute("SELECT * FROM efile_batches WHERE id=?", (batch_id,)).fetchone()
    if not batch:
        conn.close()
        return jsonify({"success": False, "error": "Batch not found"}), 404
    ts = now()
    conn.execute(
        "UPDATE efile_batches SET transmitted_at=?, status='transmitted' WHERE id=?",
        (ts, batch_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"success": True, "transmitted_at": ts})


@app.post("/api/efile-batch/<int:batch_id>/item/<int:item_id>/ack")
@login_required
def efile_batch_item_ack(batch_id: int, item_id: int):
    """Update ACK status on a single batch item."""
    data       = request.get_json(force=True)
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
    data = request.get_json(force=True)
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
@login_required
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


# ── Import Audit ──────────────────────────────────────────────────────────────

@app.route("/import-audit")
@login_required
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
    data       = request.get_json(force=True)
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


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_connection()
    init_db(conn)
    conn.close()
    print("TaxOps running at http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
