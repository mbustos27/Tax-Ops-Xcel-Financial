"""Letter-size payment receipt — PDF + relay path (never hits a real printer)."""
from __future__ import annotations

from datetime import date
from unittest.mock import patch

from db import get_connection


def _seed_pickup_return(db_path: str) -> int:
    conn = get_connection(db_path)
    try:
        cur = conn.execute(
            "INSERT INTO clients (last_name, first_name, created_at) VALUES (?,?,?)",
            ("ReceiptClient", "Ada", "2026-01-01T00:00:00"),
        )
        cid = cur.lastrowid
        cur = conn.execute(
            """
            INSERT INTO returns (
              client_id, tax_year, log_number, client_status,
              intake_date, pickup_date, created_at, updated_at
            ) VALUES (?,?,?,?,?,?,?,?)
            """,
            (
                cid, 2025, "9001", "PICKUP",
                "2026-01-10", "2026-03-15",
                "2026-01-10T00:00:00", "2026-01-10T00:00:00",
            ),
        )
        rid = cur.lastrowid
        conn.execute(
            """
            INSERT INTO payments (
              return_id, total_fee, cc_fee, fee_paid,
              payment_method, receipt_number
            ) VALUES (?,?,?,?,?,?)
            """,
            (rid, 250.0, 7.5, 257.5, "Card/Visa", "QB-1234"),
        )
        conn.commit()
        return rid
    finally:
        conn.close()


def test_payment_receipt_pdf_is_exactly_one_letter_page():
    from services.payment_receipt_pdf import render_payment_receipt_pdf

    pdf = render_payment_receipt_pdf(
        client_name="ReceiptClient, Ada",
        log_number="9001",
        tax_year=2025,
        receipt_date="2026-03-15",
        receipt_number="QB-1234",
        payment_method="Card/Visa",
        total_fee=250.0,
        cc_fee=7.5,
        amount_display=257.5,
        balance=0.0,
        is_qb=False,
    )
    assert pdf.startswith(b"%PDF")
    # Letter 8.5×11 in points (612×792) and exactly one page.
    assert b"/MediaBox [0 0 612.00 792.00]" in pdf or b"/MediaBox [0 0 612 792]" in pdf
    assert b"/Count 1" in pdf
    leaf = pdf.count(b"/Type /Page") - pdf.count(b"/Type /Pages")
    assert leaf == 1



def test_payment_receipt_preview_burgundy_no_window_print(client_logged_in, taxops_db_path):
    rid = _seed_pickup_return(taxops_db_path)
    resp = client_logged_in.get(f"/return/{rid}/payment-receipt")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")
    assert "PAYMENT RECEIPT" in html
    assert "#6B2233" in html
    assert "window.print()" not in html
    assert "autoprint" not in html
    assert "/payment-receipt/print" in html


def test_api_payment_receipt_print_uses_relay_helper(client_logged_in, taxops_db_path):
    rid = _seed_pickup_return(taxops_db_path)
    with patch(
        "services.payment_receipt_pdf.render_payment_receipt_pdf",
        return_value=b"%PDF-1.4 one-page",
    ), patch(
        "services.letter_print.try_print_letter_pdf",
        return_value=(True, "sent to print relay"),
    ) as mock_send:
        resp = client_logged_in.post(f"/api/return/{rid}/payment-receipt/print")
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True
    mock_send.assert_called_once()
    assert mock_send.call_args[0][0].startswith(b"%PDF")


def test_pickup_complete_prints_via_relay_not_browser(client_logged_in, taxops_db_path):
    rid = _seed_pickup_return(taxops_db_path)
    with patch("app._try_print_payment_receipt", return_value=(True, "sent to print relay")) as mock_print:
        resp = client_logged_in.post(
            f"/pickup/{rid}",
            data={
                "signatures_given_method": "in_person",
                "signatures_received_method": "in_person",
                "payment_method": "Card/Visa",
                "total_fee": "250.00",
                "receipt_number": "QB-9999",
                "pickup_date": date.today().isoformat(),
            },
            follow_redirects=False,
        )
    assert resp.status_code == 302
    loc = resp.headers.get("Location") or ""
    assert "/logout-queue" in loc
    assert "autoprint" not in loc
    assert "payment-receipt" not in loc
    mock_print.assert_called_once_with(rid)


def test_relay_accepts_letter_pdf_payload():
    from unittest.mock import patch
    from filetrack.relay.server import handle_print_job
    import base64

    pdf = b"%PDF-1.4\n1 0 obj\n<< /Type /Page >>\nendobj\n"
    # One /Type /Page, zero /Type /Pages → leaf count 1
    b64 = base64.b64encode(pdf).decode("ascii")
    with patch("filetrack.labels.printer.send_pdf") as mock_send:
        status, body = handle_print_job(
            {"pdf_base64": b64, "doc_name": "payment-receipt-1"},
            token_header="",
            expected_token="",
            remote_addr="127.0.0.1",
        )
    assert status == 200
    assert body["mode"] == "letter_pdf"
    mock_send.assert_called_once()


def test_relay_rejects_multipage_pdf():
    from filetrack.relay.server import handle_print_job
    import base64

    # Two page leaf markers
    pdf = b"%PDF-1.4 /Type /Page /Type /Page"
    b64 = base64.b64encode(pdf).decode("ascii")
    with patch("filetrack.labels.printer.send_pdf") as mock_send:
        status, body = handle_print_job(
            {"pdf_base64": b64},
            token_header="",
            expected_token="",
            remote_addr="127.0.0.1",
        )
    assert status == 400
    assert "1 page" in body["error"]
    mock_send.assert_not_called()
