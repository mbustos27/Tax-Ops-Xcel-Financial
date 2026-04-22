from __future__ import annotations

import functools
import os
import sys
from datetime import date, datetime

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import (
    Flask, abort, jsonify, redirect, render_template,
    request, session, url_for,
)

from config import APP_ENV
from db import get_connection, init_db
from utils import now

app = Flask(__name__)

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

STATUS_FLOW = ["LOG IN", "PROCESSING", "FINALIZE", "PICKUP", "EFILE READY", "EFILE", "LOG OUT"]

STATUS_BADGE = {
    "LOG IN":      "bg-blue-50 text-blue-700 border-blue-200",
    "PROCESSING":  "bg-amber-50 text-amber-700 border-amber-200",
    "FINALIZE":    "bg-orange-50 text-orange-700 border-orange-200",
    "PICKUP":      "bg-teal-50 text-teal-700 border-teal-200",
    "EFILE READY": "bg-indigo-50 text-indigo-700 border-indigo-200",
    "EFILE":       "bg-violet-50 text-violet-700 border-violet-200",
    "LOG OUT":     "bg-slate-100 text-slate-500 border-slate-200",
}

STATUS_DOT = {
    "LOG IN":      "dot-blue",
    "PROCESSING":  "dot-amber",
    "FINALIZE":    "dot-orange",
    "PICKUP":      "dot-teal",
    "EFILE READY": "dot-indigo",
    "EFILE":       "dot-violet",
    "LOG OUT":     "dot-slate",
}

# When advancing to these statuses, auto-stamp the corresponding date field
# only if it hasn't been set yet.
STATUS_DATE_STAMP = {
    "LOG IN":      "intake_date",
    "PICKUP":      "pickup_date",      # client called in to sign
    "LOG OUT":     "logout_date",      # accepted, case closed
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
}

# Fields that live in the clients table
CLIENT_EDITABLE = {"display_name", "referred_by", "referral_flag"}

# Fields that live in the payments table
PAYMENT_EDITABLE = {
    "total_fee", "fee_paid", "receipt_number",
    "cc_fee", "zelle_or_check_ref", "cash_or_qpay_ref",
    "bank_deposit", "refund_amount",
}

# ── SQL fragment shared by all queries ───────────────────────────────────────

_SELECT = """
SELECT
    r.id, r.log_number, r.tax_year, r.client_status, r.processor,
    r.verified, r.intake_date, r.date_emailed, r.pickup_date,
    r.logout_date, r.updated_date, r.email_marker,
    r.is_amended, r.has_w7, r.is_extension,
    r.transfer_flag, r.transfer_2025_flag, r.transfer_2026_flag,
    r.efile_date, r.ack_date, r.drake_status_raw,
    r.created_at, r.updated_at,
    c.id   AS client_id,
    c.last_name, c.first_name, c.display_name,
    c.referral_flag, c.referred_by,
    p.id   AS payment_id,
    p.total_fee, p.fee_paid, p.receipt_number,
    p.cc_fee, p.zelle_or_check_ref, p.cash_or_qpay_ref,
    p.refund_amount, p.bank_deposit,
    rf.form_1040, rf.sched_a_d, rf.sched_c, rf.sched_e,
    rf.form_1120, rf.form_1120s, rf.form_1065_llc,
    rf.corp_officer, rf.business_owner, rf.form_990_1041
FROM returns r
JOIN clients c ON c.id = r.client_id
LEFT JOIN payments p ON p.return_id = r.id
LEFT JOIN return_forms rf ON rf.return_id = r.id
"""

# ── DB helpers ────────────────────────────────────────────────────────────────

def _enrich(r: dict) -> dict:
    total = r.get("total_fee") or 0
    paid  = r.get("fee_paid")  or 0
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

    year = f.get("year") or date.today().year
    clauses.append("r.tax_year = ?")
    params.append(year)

    if f.get("status"):
        statuses = f["status"] if isinstance(f["status"], list) else [f["status"]]
        statuses = [s for s in statuses if s]
        if statuses:
            clauses.append(f"r.client_status IN ({','.join('?' for _ in statuses)})")
            params.extend(statuses)

    if f.get("processor"):
        clauses.append("r.processor = ?")
        params.append(f["processor"])

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
    sql   = f"{_SELECT} {where} ORDER BY CAST(r.log_number AS INTEGER), r.id"

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
        "SELECT client_status, COUNT(*) n FROM returns WHERE tax_year=? GROUP BY client_status",
        (year,),
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
        WHERE r.tax_year = ?
        """,
        (year,),
    ).fetchone()
    conn.close()
    billed    = row["billed"]    if row else 0
    collected = row["collected"] if row else 0
    return {"billed": billed, "collected": collected, "outstanding": round(billed - collected, 2)}


def get_processors(year: int) -> list[str]:
    conn = get_connection()
    rows = conn.execute(
        "SELECT DISTINCT processor FROM returns WHERE tax_year=? AND processor IS NOT NULL ORDER BY processor",
        (year,),
    ).fetchall()
    conn.close()
    return [r["processor"] for r in rows]


def base_ctx(year: int | None = None) -> dict:
    y = year or date.today().year
    return {
        "current_year":  y,
        "status_flow":   STATUS_FLOW,
        "status_badge":  STATUS_BADGE,
        "status_dot":    STATUS_DOT,
        "status_counts": get_status_counts(y),
        "totals":        get_totals(y),
        "processors":    get_processors(y),
        "app_env":       APP_ENV,
        "privacy_mode":  privacy_mode_enabled(),
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
    conn.close()
    notes_payload = [dict(n) for n in notes]
    if privacy_mode_enabled():
        for note in notes_payload:
            note["note_text"] = _mask_value(note.get("note_text"))

    ctx = base_ctx(ret.get("tax_year"))
    ctx.update({
        "active_page": "dashboard",
        "ret":    ret,
        "notes":  notes_payload,
        "events": [dict(e) for e in events],
    })
    return render_template("return_detail.html", **ctx)


@app.route("/logout-queue")
@login_required
def logout_queue():
    year = int(request.args.get("year", date.today().year))
    conn = get_connection()
    rows = conn.execute(
        f"{_SELECT} WHERE r.tax_year=? AND r.client_status = 'PICKUP' "
        "ORDER BY CAST(r.log_number AS INTEGER)",
        (year,),
    ).fetchall()
    conn.close()
    ctx = base_ctx(year)
    ctx.update({
        "active_page": "logout",
        "returns":     [_enrich(dict(r)) for r in rows],
        "today":       date.today().isoformat(),
    })
    return render_template("logout_queue.html", **ctx)


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
                "LOG IN",
                _v("processor"),
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
            VALUES (?, 'STATUS_CHANGED', NULL, 'LOG IN', ?, 'INTAKE', 'Created via intake form')
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

    conn = get_connection()
    try:
        if field in RETURN_EDITABLE:
            conn.execute(
                f"UPDATE returns SET {field}=?, updated_at=? WHERE id=?",
                (value, now(), return_id),
            )
            # When ack_date is set on a transmitted return, auto-advance to LOG OUT
            if field == "ack_date" and value:
                cur = conn.execute(
                    "SELECT client_status FROM returns WHERE id=?", (return_id,)
                ).fetchone()
                if cur and cur["client_status"] == "EFILE":
                    ts = now()
                    conn.execute(
                        "UPDATE returns SET client_status='LOG OUT', logout_date=COALESCE(logout_date,?), updated_at=? WHERE id=?",
                        (date.today().isoformat(), ts, return_id),
                    )
                    conn.execute(
                        """INSERT INTO status_events
                           (return_id, event_type, old_status, new_status, event_timestamp, source_file, note)
                           VALUES (?, 'STATUS_CHANGED', 'EFILE', 'LOG OUT', ?, 'APP', 'Auto-advanced: ack date set')""",
                        (return_id, ts),
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


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_connection()
    init_db(conn)
    conn.close()
    print("TaxOps running at http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
