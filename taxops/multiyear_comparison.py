"""
Multi-year comparison for a single client (GitHub MULTIYEAR-1..6).

Builds a normalized JSON payload and optional PDF export for 2-3 tax years.
"""

from __future__ import annotations

import sqlite3
from typing import Any

from preparer import preparer_list_label

DOC_TYPES_COMPARE: tuple[str, ...] = (
    "W-2",
    "1099",
    "paystub",
    "prior_return",
    "government_id",
    "misc",
)

COMPARISON_ROW_DEFS: tuple[tuple[str, str], ...] = (
    ("adjusted_gross_income", "Adjusted gross income (AGI)"),
    ("filing_status", "Filing status"),
    ("refund_amount", "Refund (client, $)"),
    ("balance_due", "IRS balance due ($)"),
    ("preparer", "Preparer"),
)


def parse_years_param(raw: str | None, *, minimum: int = 2, maximum: int = 3) -> tuple[list[int] | None, str | None]:
    """Return (years ascending unique, error_message)."""
    if raw is None or not str(raw).strip():
        return None, "Query parameter years is required (comma-separated tax years, e.g. 2023,2024)."
    yrs: list[int] = []
    for seg in str(raw).strip().replace(" ", "").split(","):
        if not seg:
            continue
        try:
            y = int(seg)
        except ValueError:
            return None, f"Invalid year: {seg!r}."
        if y < 1980 or y > 2100:
            return None, f"Year out of range: {y}."
        yrs.append(y)
    uniq = sorted(set(yrs))
    if len(uniq) < minimum:
        return None, f"Select at least {minimum} distinct tax years."
    if len(uniq) > maximum:
        return None, f"At most {maximum} tax years allowed."
    return uniq, None


def _strnorm(v: Any) -> str:
    return ("" if v is None else str(v)).strip()


def _fmoney(v: Any) -> float | None:
    if v is None or v == "":
        return None
    try:
        return round(float(v), 2)
    except (TypeError, ValueError):
        return None


def _agi_changed_sig(prev: float | None, curr: float | None, pct_threshold: float) -> bool:
    if prev is None or curr is None:
        return False
    try:
        p, c = float(prev), float(curr)
    except (TypeError, ValueError):
        return False
    if abs(p) < 1e-6:
        return abs(c - p) >= max(5000.0, pct_threshold)
    pct = abs((c - p) / p * 100.0)
    return pct >= pct_threshold


def _money_delta_sig(prev: float | None, curr: float | None, thresh: float) -> bool:
    if prev is None and curr is None:
        return False
    pv = _fmoney(prev)
    cv = _fmoney(curr)
    if pv is None and cv is None:
        return False
    if pv is None:
        return abs(cv or 0) >= thresh
    if cv is None:
        return abs(pv or 0) >= thresh
    return abs((cv or 0) - (pv or 0)) >= thresh


def _fetch_return_financials(conn: sqlite3.Connection, client_id: int, tax_year: int) -> sqlite3.Row | None:
    return conn.execute(
        """
        SELECT
            r.id AS return_id,
            r.tax_year,
            r.processor,
            r.filing_status,
            r.adjusted_gross_income,
            p.refund_amount,
            p.balance_due
        FROM returns r
        LEFT JOIN payments p ON p.return_id = r.id
        WHERE r.client_id = ? AND r.tax_year = ?
        ORDER BY r.id DESC
        LIMIT 1
        """,
        (client_id, tax_year),
    ).fetchone()


def _documents_received_map(conn: sqlite3.Connection, return_id: int) -> dict[str, bool]:
    rows = conn.execute(
        """
        SELECT COALESCE(doc_type, 'unknown') AS t, COUNT(*) AS n
          FROM return_documents
         WHERE return_id = ?
           AND IFNULL(is_deleted, 0) = 0
         GROUP BY t
        """,
        (return_id,),
    ).fetchall()
    seen = {_strnorm(r["t"]).lower() for r in rows if int(r["n"] or 0) > 0}
    return {canonical: canonical.lower() in seen for canonical in DOC_TYPES_COMPARE}


def _missing_docs_titles(conn: sqlite3.Connection, return_id: int) -> list[str]:
    rs = conn.execute(
        """
        SELECT item_text
          FROM missing_docs
         WHERE return_id = ?
           AND IFNULL(is_resolved, 0) = 0
         ORDER BY id
        """,
        (return_id,),
    ).fetchall()
    return [_strnorm(r["item_text"]) for r in rs if _strnorm(r["item_text"])]


def build_client_year_comparison_payload(
    conn: sqlite3.Connection,
    *,
    client_id: int,
    years_asc: list[int],
    thresholds: dict[str, float],
    client_display_name: str,
    privacy_mode: bool,
) -> dict[str, Any]:
    cols_by_year: dict[int, dict[str, Any]] = {}
    desc = sorted(years_asc, reverse=True)

    for ty in years_asc:
        row = _fetch_return_financials(conn, client_id, ty)
        empty_hl = {k for k, _ in COMPARISON_ROW_DEFS}
        empty_hl_map = dict.fromkeys(empty_hl, False)
        if not row:
            cols_by_year[ty] = {
                "tax_year": ty,
                "return_id": None,
                "has_return": False,
                "return_url": None,
                "adjusted_gross_income": None,
                "filing_status": None,
                "refund_amount": None,
                "balance_due": None,
                "processor_raw": None,
                "preparer_display": None,
                "documents_received": {k: False for k in DOC_TYPES_COMPARE},
                "missing_doc_items": [],
                "field_highlights": empty_hl_map,
            }
            continue

        rid = int(row["return_id"])
        proc = row["processor"]
        prep_label = "XXXXX" if privacy_mode else preparer_list_label(proc)

        cols_by_year[ty] = {
            "tax_year": ty,
            "return_id": rid,
            "has_return": True,
            "return_url": f"/return/{rid}",
            "adjusted_gross_income": _fmoney(row["adjusted_gross_income"]),
            "filing_status": _strnorm(row["filing_status"]) or None,
            "refund_amount": _fmoney(row["refund_amount"]),
            "balance_due": _fmoney(row["balance_due"]),
            "processor_raw": proc,
            "preparer_display": prep_label,
            "documents_received": _documents_received_map(conn, rid),
            "missing_doc_items": _missing_docs_titles(conn, rid),
            "field_highlights": dict(empty_hl_map),
        }

    agi_pct = float(thresholds.get("agi_percent", 10.0))
    ref_thr = float(thresholds.get("refund_abs", 500.0))
    bal_thr = float(thresholds.get("balance_abs", 500.0))

    for i in range(1, len(years_asc)):
        yp, yc = years_asc[i - 1], years_asc[i]
        pv, cv = cols_by_year[yp], cols_by_year[yc]
        if not cv.get("has_return"):
            continue
        fh = cv["field_highlights"]
        if pv.get("has_return"):
            if _agi_changed_sig(pv.get("adjusted_gross_income"), cv.get("adjusted_gross_income"), agi_pct):
                fh["adjusted_gross_income"] = True
            fs_p, fs_c = _strnorm(pv.get("filing_status")), _strnorm(cv.get("filing_status"))
            if fs_p != fs_c and (fs_p or fs_c):
                fh["filing_status"] = True
            if _money_delta_sig(pv.get("refund_amount"), cv.get("refund_amount"), ref_thr):
                fh["refund_amount"] = True
            if _money_delta_sig(pv.get("balance_due"), cv.get("balance_due"), bal_thr):
                fh["balance_due"] = True
            pp, pc = _strnorm(pv.get("processor_raw")), _strnorm(cv.get("processor_raw"))
            if pp != pc and (pp or pc):
                fh["preparer"] = True

    columns = [cols_by_year[ty] for ty in desc]

    doc_rows = [{"doc_type": dt, "label": dt.replace("_", " ")} for dt in DOC_TYPES_COMPARE]

    return {
        "client_id": client_id,
        "client_display": client_display_name,
        "years": desc,
        "years_ordered_asc": list(years_asc),
        "thresholds": {
            "agi_percent": agi_pct,
            "refund_abs": ref_thr,
            "balance_abs": bal_thr,
        },
        "rows": [{"field": k, "label": lab} for k, lab in COMPARISON_ROW_DEFS],
        "document_types": doc_rows,
        "columns": columns,
    }


def _ascii_safe(s: str) -> str:
    return s.encode("ascii", "replace").decode("ascii")


def _pdf_text(s: str) -> str:
    return _ascii_safe(s or "")


def _fmt_money(v: float | None) -> str:
    if v is None:
        return "-"
    return f"{v:,.2f}"


def render_year_comparison_pdf(payload: dict[str, Any]) -> bytes:
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos

    pdf = FPDF(orientation="L", unit="mm", format="Letter")
    pdf.set_auto_page_break(auto=True, margin=14)
    pdf.add_page()
    pdf.set_font("Helvetica", "B", 13)
    pdf.cell(
        0,
        8,
        _pdf_text(f"Multi-year comparison - Client #{payload.get('client_id')}"),
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(
        0,
        6,
        _pdf_text(str(payload.get("client_display") or "")),
        new_x=XPos.LMARGIN,
        new_y=YPos.NEXT,
    )
    thr = payload.get("thresholds") or {}
    pdf.set_font("Helvetica", "I", 8)
    pdf.multi_cell(
        0,
        4,
        _pdf_text(
            "YoY highlight rules vs prior older year in this selection: filing status/preparer changes; "
            f"AGI relative change >= {thr.get('agi_percent', 10)}%; refund delta >= {thr.get('refund_abs', 500)}; "
            f"IRS balance delta >= {thr.get('balance_abs', 500)}."
        ),
    )
    pdf.ln(1)

    columns = list(payload.get("columns") or [])
    years = [str(c.get("tax_year")) for c in columns]
    label_w = 54.0
    inner_w = pdf.w - pdf.l_margin - pdf.r_margin
    col_w = (inner_w - label_w) / max(1, len(years))
    h = 7.0

    def cell_hdr(txt: str, w: float) -> None:
        pdf.set_font("Helvetica", "B", 9)
        pdf.cell(w, h, _pdf_text(txt)[:36], border=1, align="C")

    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(label_w, h, "Field", border=1)
    for y in years:
        cell_hdr(f"TY {y}", col_w)
    pdf.ln(h)

    def val_for(col: dict[str, Any], field: str) -> str:
        if not col.get("has_return"):
            return "(no return)"
        fhi = col.get("field_highlights") or {}
        hl = "* " if fhi.get(field) else ""
        if field == "adjusted_gross_income":
            return hl + _fmt_money(col.get("adjusted_gross_income"))
        if field == "filing_status":
            return hl + _pdf_text(col.get("filing_status") or "-")
        if field == "refund_amount":
            return hl + _fmt_money(col.get("refund_amount"))
        if field == "balance_due":
            return hl + _fmt_money(col.get("balance_due"))
        if field == "preparer":
            return hl + _pdf_text(col.get("preparer_display") or "-")
        return "-"

    for field_key, lab in COMPARISON_ROW_DEFS:
        pdf.set_font("Helvetica", "", 9)
        pdf.cell(label_w, h, _pdf_text(lab)[:44], border=1)
        align = "R"
        if field_key in ("preparer", "filing_status"):
            align = "L"
        for c in columns:
            pdf.set_font("Helvetica", "B" if (c.get("field_highlights") or {}).get(field_key) else "", 9)
            pdf.cell(col_w, h, _pdf_text(val_for(c, field_key))[:48], border=1, align=align)
        pdf.ln(h)

    pdf.ln(2)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(0, h, "Documents on file (by return)", new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(label_w, h, "Type", border=1)
    for y in years:
        pdf.cell(col_w, h, _pdf_text(f"TY {y}"), border=1, align="C")
    pdf.ln(h)
    pdf.set_font("Helvetica", "", 9)
    for dt in DOC_TYPES_COMPARE:
        pdf.cell(label_w, h, _pdf_text(dt), border=1)
        for c in columns:
            if not c.get("has_return"):
                sym = "-"
            else:
                got = (c.get("documents_received") or {}).get(dt)
                sym = "Received" if got else "Missing"
            pdf.cell(col_w, h, sym, border=1, align="C")
        pdf.ln(h)

    pdf.ln(1)
    pdf.set_font("Helvetica", "B", 9)
    pdf.cell(label_w, h, "Open missing-doc items", border=1)
    pdf.set_font("Helvetica", "", 8)
    for c in columns:
        items = list(c.get("missing_doc_items") or [])
        if not c.get("has_return"):
            txt = "-"
        elif not items:
            txt = "None listed"
        else:
            txt = "; ".join(_pdf_text(x)[:120] for x in items[:4])
            if len(items) > 4:
                txt += "; ..."
        pdf.cell(col_w, h, txt[:120], border=1)
    pdf.ln(h)

    out = pdf.output()
    if isinstance(out, str):
        return out.encode("latin-1")
    return bytes(out)
