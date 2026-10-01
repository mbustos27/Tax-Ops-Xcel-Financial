"""Letter-size (8.5×11) payment receipt — HTML/CSS only, never hits a printer."""
from __future__ import annotations

from datetime import date

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


def test_payment_receipt_requires_login(client, taxops_db_path):
    rid = _seed_pickup_return(taxops_db_path)
    resp = client.get(f"/return/{rid}/payment-receipt")
    assert resp.status_code in (302, 401)


def test_payment_receipt_is_single_letter_page_html(client_logged_in, taxops_db_path):
    """Flask test client only — no browser, no window.print, no spooler."""
    rid = _seed_pickup_return(taxops_db_path)
    resp = client_logged_in.get(f"/return/{rid}/payment-receipt")
    assert resp.status_code == 200
    html = resp.data.decode("utf-8")

    assert "PAYMENT RECEIPT" in html
    assert "ReceiptClient" in html
    assert "QB-1234" in html
    assert "257.50" in html
    # Brand burgundy — same palette as work_order / missing_docs letter prints.
    assert "#6B2233" in html
    assert "#3D1019" in html
    assert "#F4EFE9" in html
    assert "#1e3a5f" not in html  # no navy substitute
    assert "size: letter portrait" in html
    assert "max-height: 10in" in html
    assert "page-break-inside: avoid" in html
    # Must never auto-fire the print dialog.
    assert "window.print()" in html  # button onclick only
    assert "addEventListener('load'" not in html
    assert html.count("window.print()") == 1


def test_pickup_complete_redirects_to_receipt_not_autoprint(client_logged_in, taxops_db_path):
    rid = _seed_pickup_return(taxops_db_path)
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
    assert f"/return/{rid}/payment-receipt" in loc
    assert "autoprint" not in loc

    # Follow once — still HTML only, no printer.
    page = client_logged_in.get(loc)
    assert page.status_code == 200
    body = page.data.decode("utf-8")
    assert "PAYMENT RECEIPT" in body
    assert "#6B2233" in body
    assert body.count("window.print()") == 1


def test_work_order_and_intake_print_css_cap_one_page(client_logged_in):
    """Regression: letter print templates advertise a hard 1-page cap."""
    from pathlib import Path

    root = Path(__file__).resolve().parents[1] / "templates"
    for name in ("work_order_print.html", "intake_print.html", "payment_receipt_print.html"):
        text = (root / name).read_text(encoding="utf-8")
        assert "size: letter portrait" in text
        assert "max-height: 10in" in text
        assert "page-break-inside: avoid" in text
