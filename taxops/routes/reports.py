"""Reports Blueprint — season summary, preparer performance, return mix.

Routes
------
GET  /reports             — HTML reports page (cache-first)
GET  /api/reports/data    — JSON report data for ?year=N (no reload)
"""
from __future__ import annotations

import logging
from datetime import date

from flask import Blueprint, jsonify, render_template, request

from auth import login_required
from db import get_connection

logger = logging.getLogger(__name__)

reports_bp = Blueprint("reports", __name__)


# ---------------------------------------------------------------------------
# Helpers — all queries JOIN payments; never expose ssn / ssn_last4
# ---------------------------------------------------------------------------

def _season_summary(conn, year: int) -> dict:
    """Section A: total billed, collected, outstanding, rates."""
    row = conn.execute(
        """
        SELECT
            COUNT(r.id) AS return_count,
            COALESCE(SUM(p.total_fee), 0)                                   AS total_billed,
            COALESCE(SUM(p.fee_paid),  0)                                   AS total_collected,
            COALESCE(SUM(
                CASE WHEN COALESCE(p.total_fee,0) > COALESCE(p.fee_paid,0)
                     THEN COALESCE(p.total_fee,0) - COALESCE(p.fee_paid,0)
                     ELSE 0 END
            ), 0)                                                            AS total_outstanding,
            COUNT(CASE WHEN COALESCE(p.fee_paid,0) >= COALESCE(p.total_fee,0)
                            AND COALESCE(p.total_fee,0) > 0
                       THEN 1 END)                                           AS fully_paid_count,
            COUNT(CASE WHEN COALESCE(p.fee_paid,0) < COALESCE(p.total_fee,0)
                            AND COALESCE(p.total_fee,0) > 0
                       THEN 1 END)                                           AS balance_due_count,
            ROUND(AVG(CASE WHEN COALESCE(p.total_fee,0) > 0
                           THEN p.total_fee END), 2)                         AS avg_fee
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        """,
        (year,),
    ).fetchone()
    if not row:
        return {}
    d = dict(row)
    billed = d.get("total_billed") or 0
    collected = d.get("total_collected") or 0
    d["collection_rate"] = round(collected / billed * 100, 1) if billed else 0.0
    return d


def _season_summary_yoy(conn, year: int) -> dict:
    """Section A with YoY deltas for billed and avg_fee."""
    curr = _season_summary(conn, year)
    prev = _season_summary(conn, year - 1)

    def _delta(key: str) -> float | None:
        c = curr.get(key) or 0.0
        p = prev.get(key) or 0.0
        if not p:
            return None
        return round((c - p) / p * 100, 1)

    curr["yoy_billed_pct"] = _delta("total_billed")
    curr["yoy_avg_fee_pct"] = _delta("avg_fee")
    curr["prev_year"] = year - 1
    curr["prev_total_billed"] = prev.get("total_billed", 0)
    return curr


def _preparer_performance(conn, year: int) -> list[dict]:
    """Section B: per-preparer stats."""
    rows = conn.execute(
        """
        SELECT
            r.processor,
            COUNT(r.id)                                                         AS return_count,
            ROUND(AVG(CASE WHEN COALESCE(p.total_fee,0) > 0
                           THEN p.total_fee END), 2)                            AS avg_fee,
            COALESCE(SUM(p.total_fee), 0)                                       AS total_billed,
            COALESCE(SUM(p.fee_paid),  0)                                       AS total_collected,
            COUNT(CASE WHEN r.client_status = 'LOG OUT'                  THEN 1 END) AS completed_count,
            COUNT(CASE WHEN r.client_status IN ('PROCESSING','HOLD','FINALIZE') THEN 1 END) AS in_progress_count
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
          AND r.processor IS NOT NULL
        GROUP BY r.processor
        ORDER BY return_count DESC
        """,
        (year,),
    ).fetchall()
    result = [dict(r) for r in rows]
    total = sum(r["return_count"] for r in result)
    for r in result:
        r["share_pct"] = round(r["return_count"] / total * 100, 1) if total else 0.0
    return result


def _missing_docs_summary(conn, year: int) -> dict:
    """Headline counts for missing-docs aggregate report."""
    season = conn.execute(
        """
        SELECT COUNT(*) AS n
        FROM returns r
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        """,
        (year,),
    ).fetchone()
    season_returns = int(season["n"] or 0) if season else 0

    open_row = conn.execute(
        """
        SELECT
            COUNT(*) AS open_items,
            COUNT(DISTINCT md.return_id) AS returns_with_open
        FROM missing_docs md
        JOIN returns r ON r.id = md.return_id
        WHERE r.tax_year = ?
          AND md.is_resolved = 0
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        """,
        (year,),
    ).fetchone()
    open_items = int(open_row["open_items"] or 0) if open_row else 0
    returns_with_open = int(open_row["returns_with_open"] or 0) if open_row else 0
    pct = round(returns_with_open / season_returns * 100, 1) if season_returns else 0.0
    return {
        "season_returns": season_returns,
        "open_items": open_items,
        "returns_with_open": returns_with_open,
        "pct_returns_with_open": pct,
    }


def _missing_docs_frequency(conn, year: int, *, open_only: bool) -> list[dict]:
    """Rank missing-doc labels by frequency (normalized grouping)."""
    resolved_clause = "AND md.is_resolved = 0" if open_only else ""
    rows = conn.execute(
        f"""
        SELECT
            LOWER(TRIM(md.item_text)) AS norm_key,
            MIN(md.item_text) AS display_text,
            COUNT(*) AS total_count,
            SUM(CASE WHEN md.is_resolved = 0 THEN 1 ELSE 0 END) AS open_count,
            COUNT(DISTINCT CASE WHEN md.is_resolved = 0 THEN md.return_id END) AS returns_with_open
        FROM missing_docs md
        JOIN returns r ON r.id = md.return_id
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
          {resolved_clause}
        GROUP BY LOWER(TRIM(md.item_text))
        ORDER BY open_count DESC, total_count DESC, display_text ASC
        """,
        (year,),
    ).fetchall()
    return [dict(r) for r in rows]


def _missing_docs_returns_for_item(conn, year: int, norm_key: str) -> list[dict]:
    """Returns with an open missing-doc line matching norm_key."""
    key = (norm_key or "").strip().lower()
    if not key:
        return []
    rows = conn.execute(
        """
        SELECT
            r.id AS return_id,
            r.log_number,
            c.last_name,
            c.first_name,
            r.client_status,
            r.intake_date,
            md.item_text
        FROM missing_docs md
        JOIN returns r ON r.id = md.return_id
        JOIN clients c ON c.id = r.client_id
        WHERE r.tax_year = ?
          AND md.is_resolved = 0
          AND LOWER(TRIM(md.item_text)) = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        ORDER BY r.log_number, c.last_name, c.first_name
        """,
        (year, key),
    ).fetchall()
    return [dict(r) for r in rows]


def _return_mix(conn, year: int) -> dict:
    """Section C: status breakdown, fee ranges, YoY table."""
    # -- by status --
    by_status_rows = conn.execute(
        """
        SELECT r.client_status, COUNT(*) AS count
        FROM returns r
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        GROUP BY r.client_status
        ORDER BY count DESC
        """,
        (year,),
    ).fetchall()

    # -- by fee range --
    by_fee_rows = conn.execute(
        """
        SELECT
            CASE
                WHEN COALESCE(p.total_fee, 0) = 0   THEN 'No fee'
                WHEN p.total_fee < 200              THEN 'Under $200'
                WHEN p.total_fee < 400              THEN '$200 – $399'
                WHEN p.total_fee < 600              THEN '$400 – $599'
                WHEN p.total_fee < 1000             THEN '$600 – $999'
                ELSE '$1,000+'
            END AS fee_range,
            COUNT(*) AS count,
            ROUND(AVG(CASE WHEN COALESCE(p.total_fee,0) > 0 THEN p.total_fee END), 2) AS avg_fee,
            MIN(COALESCE(p.total_fee, 0)) AS min_fee
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE r.tax_year = ?
          AND UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        GROUP BY fee_range
        ORDER BY min_fee
        """,
        (year,),
    ).fetchall()

    # -- YoY (last 5 seasons) --
    yoy_rows = conn.execute(
        """
        SELECT
            r.tax_year,
            COUNT(*) AS count,
            ROUND(AVG(CASE WHEN COALESCE(p.total_fee,0) > 0 THEN p.total_fee END), 2) AS avg_fee
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE UPPER(COALESCE(r.client_status, '')) != 'CANCELLED'
        GROUP BY r.tax_year
        ORDER BY r.tax_year DESC
        LIMIT 5
        """,
    ).fetchall()

    status_list = [dict(r) for r in by_status_rows]
    total_status = sum(r["count"] for r in status_list)
    for r in status_list:
        r["pct"] = round(r["count"] / total_status * 100, 1) if total_status else 0.0

    fee_list = [dict(r) for r in by_fee_rows]
    total_fee_count = sum(r["count"] for r in fee_list)
    for r in fee_list:
        r["pct"] = round(r["count"] / total_fee_count * 100, 1) if total_fee_count else 0.0

    return {
        "by_status": status_list,
        "by_fee_range": fee_list,
        "yoy": [dict(r) for r in yoy_rows],
    }


def _available_years(conn) -> list[int]:
    rows = conn.execute(
        "SELECT DISTINCT tax_year FROM returns WHERE tax_year IS NOT NULL ORDER BY tax_year DESC LIMIT 10"
    ).fetchall()
    return [r["tax_year"] for r in rows]


def _build_report_data(year: int) -> dict:
    """Assemble full report dict for the given year, straight from the DB.

    Never exposes ssn, ssn_last4, or identification numbers.

    Note: this used to consult a `chat_cache` module for a "cache-first"
    financial-stats shortcut, but `chat_cache` was removed along with the AI
    chat assistant subsystem (commit e188a1a) and this route was added after
    that removal — the import was always dead (ModuleNotFoundError on every
    call). Neither `reports.html` nor its AJAX refresh actually reads the
    `cache_warm`/`cached_financial` fields, so they're dropped rather than
    reimplemented.
    """
    conn = get_connection()
    try:
        summary = _season_summary_yoy(conn, year)
        preparers = _preparer_performance(conn, year)
        mix = _return_mix(conn, year)
        years = _available_years(conn)
    finally:
        conn.close()

    return {
        "year": year,
        "available_years": years,
        "summary": summary,
        "preparers": preparers,
        "mix": mix,
    }


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------

@reports_bp.route("/reports")
@login_required
def reports():
    year = int(request.args.get("year", date.today().year))
    data = _build_report_data(year)
    from app import base_ctx  # lazy — avoids circular import at module level
    ctx = base_ctx(year)
    ctx.update({
        "active_page":  "reports",
        "report":       data,
        "report_year":  year,
        "as_of_date":   date.today().strftime("%B %Y"),
    })
    return render_template("reports.html", **ctx)


@reports_bp.route("/api/reports/data")
@login_required
def api_reports_data():
    """JSON endpoint for year-picker AJAX refresh. Never returns ssn fields."""
    year = int(request.args.get("year", date.today().year))
    return jsonify(_build_report_data(year))


@reports_bp.route("/reports/missing-docs")
@login_required
def missing_docs_report():
    """Aggregate missing-document frequency for a tax year."""
    year = int(request.args.get("year", date.today().year))
    status_filter = (request.args.get("status") or "open").strip().lower()
    open_only = status_filter != "all"
    conn = get_connection()
    try:
        summary = _missing_docs_summary(conn, year)
        items = _missing_docs_frequency(conn, year, open_only=open_only)
        years = _available_years(conn)
    finally:
        conn.close()

    from app import base_ctx  # lazy — avoids circular import at module level
    ctx = base_ctx(year)
    ctx.update({
        "active_page": "reports",
        "report_year": year,
        "status_filter": "all" if not open_only else "open",
        "summary": summary,
        "items": items,
        "available_years": years,
    })
    return render_template("missing_docs_report.html", **ctx)


@reports_bp.route("/api/reports/missing-docs/data")
@login_required
def api_missing_docs_report_data():
    year = int(request.args.get("year", date.today().year))
    status_filter = (request.args.get("status") or "open").strip().lower()
    open_only = status_filter != "all"
    conn = get_connection()
    try:
        summary = _missing_docs_summary(conn, year)
        items = _missing_docs_frequency(conn, year, open_only=open_only)
    finally:
        conn.close()
    return jsonify({
        "year": year,
        "status_filter": "all" if not open_only else "open",
        "summary": summary,
        "items": items,
    })


@reports_bp.route("/api/reports/missing-docs/returns")
@login_required
def api_missing_docs_returns():
    year = int(request.args.get("year", date.today().year))
    norm_key = (request.args.get("norm_key") or "").strip()
    conn = get_connection()
    try:
        returns = _missing_docs_returns_for_item(conn, year, norm_key)
    finally:
        conn.close()
    return jsonify({"year": year, "norm_key": norm_key, "returns": returns})
