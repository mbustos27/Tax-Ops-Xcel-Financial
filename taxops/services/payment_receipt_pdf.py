"""Single-page letter (8.5×11) payment receipt PDF for silent relay print.

Uses the office burgundy palette (#6B2233 / #3D1019). Exactly one page —
``set_auto_page_break(False)`` and a single ``add_page()``.
"""
from __future__ import annotations

from typing import Any


_BURGUNDY = (107, 34, 51)       # #6B2233
_BURGUNDY_DEEP = (61, 16, 25)   # #3D1019
_CREAM = (244, 239, 233)        # #F4EFE9


def _txt(value: Any) -> str:
    """fpdf Helvetica is Latin-1; strip/replace anything outside that."""
    s = "" if value is None else str(value)
    return s.encode("latin-1", "replace").decode("latin-1")


def _money(v: float) -> str:
    return f"${v:,.2f}"


def render_payment_receipt_pdf(
    *,
    client_name: str,
    log_number: str | None,
    tax_year: Any,
    receipt_date: str,
    receipt_number: str | None,
    payment_method: str | None,
    check_number: str | None = None,
    total_fee: float = 0.0,
    cc_fee: float = 0.0,
    amount_display: float = 0.0,
    balance: float = 0.0,
    is_qb: bool = False,
) -> bytes:
    """Return PDF bytes for one Letter portrait page."""
    from fpdf import FPDF
    from fpdf.enums import XPos, YPos

    pdf = FPDF(orientation="P", unit="in", format="Letter")
    pdf.set_auto_page_break(auto=False, margin=0.5)
    pdf.set_margins(0.6, 0.55, 0.6)
    pdf.add_page()

    # Header
    pdf.set_text_color(*_BURGUNDY_DEEP)
    pdf.set_font("Helvetica", "B", 16)
    pdf.cell(0, 0.28, _txt("Xcel Financial Services, LLC"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.set_text_color(*_BURGUNDY)
    pdf.set_font("Helvetica", "", 10)
    pdf.cell(0, 0.2, _txt("Payment receipt  |  Tax preparation"), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    y = pdf.get_y() + 0.08
    pdf.set_draw_color(*_BURGUNDY)
    pdf.set_line_width(0.04)
    pdf.line(0.6, y, 8.5 - 0.6, y)
    pdf.set_y(y + 0.12)

    pdf.set_font("Helvetica", "B", 14)
    pdf.set_text_color(*_BURGUNDY)
    title = "PAYMENT RECEIPT"
    if log_number:
        title = f"PAYMENT RECEIPT    #{log_number}"
    pdf.cell(0, 0.28, _txt(title), new_x=XPos.LMARGIN, new_y=YPos.NEXT)
    pdf.ln(0.08)

    # Meta block
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(0, 0, 0)
    rows = [
        ("Client", client_name or "—"),
        ("Date", receipt_date or "—"),
        ("Tax year", str(tax_year) if tax_year is not None else "—"),
        ("Receipt #", receipt_number or "—"),
        ("Payment method", payment_method or "—"),
    ]
    if check_number:
        rows.append(("Check #", check_number))

    label_w = 1.6
    for lab, val in rows:
        pdf.set_font("Helvetica", "B", 9)
        pdf.set_text_color(*_BURGUNDY)
        pdf.cell(label_w, 0.22, _txt(lab.upper()))
        pdf.set_font("Helvetica", "", 11)
        pdf.set_text_color(26, 26, 26)
        pdf.cell(0, 0.22, _txt(val), new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    pdf.ln(0.12)

    # Line items table header
    col_desc = 5.5
    col_amt = 1.8
    row_h = 0.28
    pdf.set_fill_color(*_BURGUNDY)
    pdf.set_text_color(255, 255, 255)
    pdf.set_font("Helvetica", "B", 10)
    pdf.cell(col_desc, row_h, " Description", border=0, fill=True)
    pdf.cell(col_amt, row_h, "Amount ", border=0, fill=True, align="R", new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    def line_row(desc: str, amt: str, *, fill_cream: bool = False, bold: bool = False) -> None:
        if fill_cream:
            pdf.set_fill_color(*_CREAM)
        else:
            pdf.set_fill_color(255, 255, 255)
        pdf.set_text_color(26, 26, 26)
        pdf.set_font("Helvetica", "B" if bold else "", 10)
        pdf.cell(col_desc, row_h, _txt(f" {desc}"), border="B", fill=True)
        pdf.cell(col_amt, row_h, _txt(f"{amt} "), border="B", fill=True, align="R",
                 new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    ty = tax_year if tax_year is not None else "—"
    line_row(f"Tax preparation fee (TY {ty})", _money(float(total_fee or 0)), fill_cream=False)
    if float(cc_fee or 0) > 0.005:
        line_row("Card processing fee", _money(float(cc_fee)), fill_cream=True)
    paid_label = "Amount billed" if is_qb else "Amount paid"
    line_row(paid_label, _money(float(amount_display or 0)), fill_cream=False, bold=True)

    pdf.ln(0.18)

    # Amount callout box
    box_y = pdf.get_y()
    box_h = 0.55
    pdf.set_fill_color(*_CREAM)
    pdf.set_draw_color(*_BURGUNDY)
    pdf.set_line_width(0.02)
    pdf.rect(0.6, box_y, 7.3, box_h, style="DF")
    pdf.set_xy(0.75, box_y + 0.1)
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*_BURGUNDY)
    pdf.cell(3.5, 0.18, _txt(paid_label.upper()))
    pdf.set_xy(0.75, box_y + 0.28)
    pdf.set_font("Helvetica", "B", 16)
    pdf.set_text_color(*_BURGUNDY_DEEP)
    pdf.cell(0, 0.22, _txt(_money(float(amount_display or 0))))
    pdf.set_y(box_y + box_h + 0.15)

    # Status
    pdf.set_font("Helvetica", "B", 10)
    pdf.set_text_color(*_BURGUNDY_DEEP)
    if is_qb:
        note = f"QuickBooks billing — invoice / receipt # recorded as {receipt_number or 'QB'}."
    elif float(balance or 0) > 0.005:
        note = f"Balance remaining: {_money(float(balance))}"
        pdf.set_text_color(146, 64, 14)
    else:
        note = "Paid in full. Thank you."
    pdf.multi_cell(0, 0.2, _txt(note))

    pdf.ln(0.15)
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(68, 68, 68)
    pdf.cell(0, 0.2, _txt("Thank you for choosing Xcel Financial Services."),
             new_x=XPos.LMARGIN, new_y=YPos.NEXT)

    # End line near bottom of page 1 only
    pdf.set_y(10.2)
    pdf.set_draw_color(*_BURGUNDY)
    pdf.set_line_width(0.015)
    pdf.line(0.6, pdf.get_y(), 8.5 - 0.6, pdf.get_y())
    pdf.set_y(pdf.get_y() + 0.08)
    pdf.set_font("Helvetica", "B", 8)
    pdf.set_text_color(153, 153, 153)
    pdf.cell(0, 0.18, _txt("END OF PAYMENT RECEIPT  ·  ONE PAGE  ·  8.5 x 11"), align="C")

    if pdf.page != 1:
        raise ValueError(
            f"Payment receipt PDF must be exactly 1 page; got {pdf.page}. "
            "Refusing to print a multi-page job."
        )

    out = pdf.output()
    if isinstance(out, (bytes, bytearray)):
        return bytes(out)
    return bytes(out)
