from __future__ import annotations

import functools
import os
import sys
from datetime import date

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from flask import (
    Flask, abort, jsonify, redirect, render_template,
    request, session, url_for,
)

from db import get_connection, init_db
from utils import now

app = Flask(__name__)

# Secret key for signing session cookies.
# Set TAXOPS_SECRET env-var in production; a random fallback is fine for dev.
app.secret_key = os.environ.get("TAXOPS_SECRET", os.urandom(24))

# Login credentials — override via environment variables.
_LOGIN_USER = os.environ.get("TAXOPS_USER", "info")
_LOGIN_PASS = os.environ.get("TAXOPS_PASS", "2703Tax")


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

STATUS_FLOW = ["LOG IN", "PROCESSING", "FINALIZE", "EFILE", "PICKUP", "LOG OUT"]

STATUS_BADGE = {
    "LOG IN":     "bg-blue-50 text-blue-700 border-blue-200",
    "PROCESSING": "bg-amber-50 text-amber-700 border-amber-200",
    "FINALIZE":   "bg-orange-50 text-orange-700 border-orange-200",
    "EFILE":      "bg-violet-50 text-violet-700 border-violet-200",
    "PICKUP":     "bg-teal-50 text-teal-700 border-teal-200",
    "LOG OUT":    "bg-slate-100 text-slate-500 border-slate-200",
}

STATUS_DOT = {
    "LOG IN":     "dot-blue",
    "PROCESSING": "dot-amber",
    "FINALIZE":   "dot-orange",
    "EFILE":      "dot-violet",
    "PICKUP":     "dot-teal",
    "LOG OUT":    "dot-slate",
}

# When advancing to these statuses, auto-stamp the corresponding date field
# only if it hasn't been set yet.
STATUS_DATE_STAMP = {
    "LOG IN":  "intake_date",
    "PICKUP":  "pickup_date",
    "LOG OUT": "logout_date",
}

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
    ctx = base_ctx(ret.get("tax_year"))
    ctx.update({
        "active_page": "dashboard",
        "ret":    ret,
        "notes":  [dict(n) for n in notes],
        "events": [dict(e) for e in events],
    })
    return render_template("return_detail.html", **ctx)


@app.route("/logout-queue")
@login_required
def logout_queue():
    year = int(request.args.get("year", date.today().year))
    conn = get_connection()
    rows = conn.execute(
        f"{_SELECT} WHERE r.tax_year=? AND r.client_status IN ('PICKUP','LOG OUT') "
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


# ── JSON API ──────────────────────────────────────────────────────────────────

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
            "name":       r["name_full"],
            "status":     r["client_status"],
            "badge":      r["badge_class"],
            "tax_year":   r["tax_year"],
        }
        for r in results[:12]
    ])


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
    return jsonify({"success": True, "text": text, "created_at": ts})


# ── Startup ───────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    conn = get_connection()
    init_db(conn)
    conn.close()
    print("TaxOps running at http://localhost:5000")
    app.run(debug=True, host="0.0.0.0", port=5000)
