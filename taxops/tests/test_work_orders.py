"""WO-1 M8: QA pass for the Work Order Creator.

Covers:
  - sequential WO-##### numbering (happy path)
  - concurrent creates never produce duplicate work_order_number (M1 proof,
    same technique as test_intake_log_number_race.py)
  - full workflow: create -> edit line items -> mark processed -> edit now
    blocked for non-admin, allowed for admin
  - RBAC denial paths: receptionist can't edit another user's work order,
    preparer can't delete, non-preparer can't mark processed
  - print view reachability
"""
from __future__ import annotations

import re
import sqlite3
import threading
import time

from werkzeug.security import generate_password_hash

from db import get_connection


def _wo_id_from_redirect(resp) -> int:
    location = resp.headers.get("Location") or ""
    match = re.search(r"/work-orders/(\d+)$", location)
    assert match, f"expected a /work-orders/<id> redirect, got {location!r}"
    return int(match.group(1))


def _wo_number(taxops_db_path: str, wo_id: int) -> str:
    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute("SELECT work_order_number FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    finally:
        conn.close()
    assert row is not None
    return row["work_order_number"]


def _create_payload(client_name="Test Client", desc="Consultation", fee="175.00"):
    return {
        "client_name": client_name,
        "date_created": "2026-01-15",
        "due_by": "",
        "item_description[]": [desc],
        "item_fee[]": [fee],
        "item_is_quick_pick[]": ["0"],
    }


def _seed_user(taxops_db_path: str, username: str, role: str) -> None:
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, ?, 1, '2025-01-01T00:00:00Z')
        """,
        (username, generate_password_hash("pw12345"), username.title(), role),
    )
    conn.commit()
    conn.close()


def _login_as(client, username: str) -> None:
    client.post("/login", data={"username": username, "password": "pw12345"}, follow_redirects=True)


# ── M1: numbering ──────────────────────────────────────────────────────────────

def test_sequential_work_order_numbers_increment(client_logged_in, taxops_db_path):
    resp1 = client_logged_in.post("/work-orders", data=_create_payload("Alpha"), follow_redirects=False)
    resp2 = client_logged_in.post("/work-orders", data=_create_payload("Beta"), follow_redirects=False)

    assert resp1.status_code == 302
    assert resp2.status_code == 302

    num1 = _wo_number(taxops_db_path, _wo_id_from_redirect(resp1))
    num2 = _wo_number(taxops_db_path, _wo_id_from_redirect(resp2))

    n1 = int(num1.split("-")[1])
    n2 = int(num2.split("-")[1])
    assert num1.startswith("WO-") and num2.startswith("WO-")
    assert n2 == n1 + 1


def test_concurrent_creates_get_distinct_work_order_numbers(app, taxops_db_path, monkeypatch):
    """Same proof technique as test_intake_log_number_race.py: widen the gap
    between the MAX read and the next write so the race triggers reliably."""

    class _SlowConnection(sqlite3.Connection):
        def execute(self, sql, *args, **kwargs):
            cur = super().execute(sql, *args, **kwargs)
            if isinstance(sql, str) and "FROM work_orders WHERE work_order_number LIKE" in sql:
                time.sleep(0.2)
            return cur

    _orig_connect = sqlite3.connect

    def _connect_slow(*args, **kwargs):
        kwargs.setdefault("factory", _SlowConnection)
        return _orig_connect(*args, **kwargs)

    monkeypatch.setattr(sqlite3, "connect", _connect_slow)

    n_racers = 6
    _seed_user(taxops_db_path, "__wo_racer__", "preparer")

    barrier = threading.Barrier(n_racers)
    results: dict[str, object] = {}
    errors: list[BaseException] = []

    def _race(name: str) -> None:
        try:
            racer_client = app.test_client()
            _login_as(racer_client, "__wo_racer__")
            barrier.wait(timeout=10)
            resp = racer_client.post("/work-orders", data=_create_payload(name), follow_redirects=False)
            results[name] = resp.status_code
            if resp.status_code == 302:
                results[f"{name}_id"] = _wo_id_from_redirect(resp)
        except BaseException as exc:
            errors.append(exc)

    names = [f"Racer{i}" for i in range(n_racers)]
    threads = [threading.Thread(target=_race, args=(n,)) for n in names]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)

    assert not errors, f"racer thread(s) raised: {errors}"
    for name in names:
        assert results.get(name) == 302, f"{name} got status {results.get(name)!r}"

    numbers = [_wo_number(taxops_db_path, results[f"{name}_id"]) for name in names]
    assert len(set(numbers)) == n_racers, f"expected {n_racers} distinct numbers, got duplicates: {numbers}"


# ── M2/M8: full workflow ───────────────────────────────────────────────────────

def test_full_workflow_create_edit_process_then_edit_blocked_for_non_admin(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_preparer__", "preparer")
    _seed_user(taxops_db_path, "__wo_admin__", "admin")

    preparer_client = app.test_client()
    _login_as(preparer_client, "__wo_preparer__")

    # Create
    resp = preparer_client.post("/work-orders", data=_create_payload("Workflow Client"), follow_redirects=False)
    assert resp.status_code == 302
    wo_id = _wo_id_from_redirect(resp)

    # Server computed the total from line items, never trusting a client total
    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT total_fee, status FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert float(row["total_fee"]) == 175.00
    assert row["status"] == "open"

    # Edit line items -> total recomputed server-side
    edit_resp = preparer_client.post(
        f"/work-orders/{wo_id}",
        data={
            "client_name": "Workflow Client",
            "date_created": "2026-01-15",
            "due_by": "",
            "item_description[]": ["Consultation", "Corporate Book"],
            "item_fee[]": ["175.00", "475.00"],
            "item_is_quick_pick[]": ["1", "1"],
        },
        follow_redirects=False,
    )
    assert edit_resp.status_code == 302
    conn = get_connection(taxops_db_path)
    total_after_edit = conn.execute("SELECT total_fee FROM work_orders WHERE id=?", (wo_id,)).fetchone()["total_fee"]
    conn.close()
    assert float(total_after_edit) == 650.00

    # Mark processed (self-attest)
    process_resp = preparer_client.post(f"/work-orders/{wo_id}/process")
    assert process_resp.status_code == 200
    assert process_resp.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    processed_row = conn.execute(
        "SELECT status, processed_by_user_id FROM work_orders WHERE id=?", (wo_id,)
    ).fetchone()
    preparer_id = conn.execute("SELECT id FROM auth_users WHERE username=?", ("__wo_preparer__",)).fetchone()["id"]
    conn.close()
    assert processed_row["status"] == "processed"
    assert processed_row["processed_by_user_id"] == preparer_id

    # Editing a processed WO is now blocked for the preparer (non-admin)
    blocked_edit = preparer_client.get(f"/work-orders/{wo_id}/edit")
    assert blocked_edit.status_code == 403

    blocked_update = preparer_client.post(
        f"/work-orders/{wo_id}",
        data={"client_name": "Should Not Save", "date_created": "2026-01-15",
              "item_description[]": ["x"], "item_fee[]": ["1"], "item_is_quick_pick[]": ["0"]},
    )
    assert blocked_update.status_code == 403

    # ...but an admin CAN still edit a processed work order
    admin_client = app.test_client()
    _login_as(admin_client, "__wo_admin__")
    admin_edit_page = admin_client.get(f"/work-orders/{wo_id}/edit")
    assert admin_edit_page.status_code == 200


def test_create_with_no_line_items_is_rejected_with_no_partial_write(client_logged_in, taxops_db_path):
    payload = {
        "client_name": "No Items Client",
        "date_created": "2026-01-15",
        "due_by": "",
        "item_description[]": [""],
        "item_fee[]": ["0"],
        "item_is_quick_pick[]": ["0"],
    }
    resp = client_logged_in.post("/work-orders", data=payload)
    assert resp.status_code == 400

    conn = get_connection(taxops_db_path)
    count = conn.execute(
        "SELECT COUNT(*) c FROM work_orders WHERE client_name='No Items Client'"
    ).fetchone()["c"]
    conn.close()
    assert count == 0


# ── M5: RBAC denial paths ──────────────────────────────────────────────────────

def test_receptionist_cannot_edit_another_users_open_work_order(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_recep_a__", "receptionist")
    _seed_user(taxops_db_path, "__wo_recep_b__", "receptionist")

    client_a = app.test_client()
    _login_as(client_a, "__wo_recep_a__")
    create_resp = client_a.post("/work-orders", data=_create_payload("Recep A's Client"), follow_redirects=False)
    assert create_resp.status_code == 302
    wo_id = _wo_id_from_redirect(create_resp)

    # Owner (receptionist A) can view/edit their own open work order
    assert client_a.get(f"/work-orders/{wo_id}").status_code == 200
    assert client_a.get(f"/work-orders/{wo_id}/edit").status_code == 200

    # A different receptionist cannot view or edit it — explicit 403, not silent
    client_b = app.test_client()
    _login_as(client_b, "__wo_recep_b__")
    assert client_b.get(f"/work-orders/{wo_id}").status_code == 403
    assert client_b.get(f"/work-orders/{wo_id}/edit").status_code == 403


def test_preparer_cannot_delete_work_order(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_preparer2__", "preparer")
    preparer_client = app.test_client()
    _login_as(preparer_client, "__wo_preparer2__")

    create_resp = preparer_client.post("/work-orders", data=_create_payload("Delete Me Not"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)
    wo_number = _wo_number(taxops_db_path, wo_id)

    resp = preparer_client.post(
        f"/work-orders/{wo_id}/delete",
        json={"confirm_work_order_number": wo_number},
    )
    assert resp.status_code == 403

    conn = get_connection(taxops_db_path)
    still_there = conn.execute("SELECT 1 FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    conn.close()
    assert still_there is not None


def test_receptionist_cannot_mark_processed(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_recep_c__", "receptionist")
    client_c = app.test_client()
    _login_as(client_c, "__wo_recep_c__")

    create_resp = client_c.post("/work-orders", data=_create_payload("Recep C's Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)

    resp = client_c.post(f"/work-orders/{wo_id}/process")
    assert resp.status_code == 403


def test_admin_can_delete_with_correct_confirmation(client_logged_in, taxops_db_path):
    create_resp = client_logged_in.post("/work-orders", data=_create_payload("Admin Delete Target"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)
    wo_number = _wo_number(taxops_db_path, wo_id)

    wrong = client_logged_in.post(f"/work-orders/{wo_id}/delete", json={"confirm_work_order_number": "WO-99999"})
    assert wrong.status_code == 400

    ok = client_logged_in.post(f"/work-orders/{wo_id}/delete", json={"confirm_work_order_number": wo_number})
    assert ok.status_code == 200

    conn = get_connection(taxops_db_path)
    gone = conn.execute("SELECT 1 FROM work_orders WHERE id=?", (wo_id,)).fetchone()
    items_gone = conn.execute("SELECT COUNT(*) c FROM work_order_items WHERE work_order_id=?", (wo_id,)).fetchone()["c"]
    conn.close()
    assert gone is None
    assert items_gone == 0  # ON DELETE CASCADE


# ── M4: print view ─────────────────────────────────────────────────────────────

def test_print_view_reachable_from_detail_and_directly(client_logged_in, taxops_db_path):
    create_resp = client_logged_in.post("/work-orders", data=_create_payload("Print Me"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(create_resp)

    detail = client_logged_in.get(f"/work-orders/{wo_id}")
    assert detail.status_code == 200
    assert f"/work-orders/{wo_id}/print".encode() in detail.data

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    assert b"WORK ORDER" in print_resp.data
    assert b"Print Me" in print_resp.data


# ── WO-2: attached billing request ──────────────────────────────────────────────

def _billing_request(taxops_db_path: str, wo_id: int) -> sqlite3.Row | None:
    conn = get_connection(taxops_db_path)
    try:
        return conn.execute("SELECT * FROM billing_requests WHERE work_order_id=?", (wo_id,)).fetchone()
    finally:
        conn.close()


def test_billing_request_created_with_work_order_fee(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Billing Client", fee="175.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    br = _billing_request(taxops_db_path, wo_id)
    assert br is not None
    assert float(br["amount"]) == 175.00
    assert br["status"] == "pending"
    assert br["client_name"] == "Billing Client"


def test_billing_request_amount_syncs_while_pending(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Sync Client", fee="175.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)
    assert float(_billing_request(taxops_db_path, wo_id)["amount"]) == 175.00

    client_logged_in.post(
        f"/work-orders/{wo_id}",
        data={
            "client_name": "Sync Client",
            "date_created": "2026-01-15",
            "due_by": "",
            "item_description[]": ["Consultation", "Corporate Book"],
            "item_fee[]": ["175.00", "475.00"],
            "item_is_quick_pick[]": ["1", "1"],
        },
        follow_redirects=False,
    )
    assert float(_billing_request(taxops_db_path, wo_id)["amount"]) == 650.00


def test_billing_request_amount_frozen_once_billed(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_bill_preparer__", "preparer")
    preparer_client = app.test_client()
    _login_as(preparer_client, "__wo_bill_preparer__")

    resp = preparer_client.post("/work-orders", data=_create_payload("Frozen Client", fee="175.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    mark_resp = preparer_client.post(f"/work-orders/{wo_id}/billing/mark", json={"status": "billed"})
    assert mark_resp.status_code == 200
    assert _billing_request(taxops_db_path, wo_id)["status"] == "billed"

    # Editing the work order after billing must NOT silently change the
    # amount accounting already invoiced against.
    preparer_client.post(
        f"/work-orders/{wo_id}",
        data={
            "client_name": "Frozen Client",
            "date_created": "2026-01-15",
            "due_by": "",
            "item_description[]": ["Consultation"],
            "item_fee[]": ["999.00"],
            "item_is_quick_pick[]": ["0"],
        },
        follow_redirects=False,
    )
    br = _billing_request(taxops_db_path, wo_id)
    assert float(br["amount"]) == 175.00
    assert br["status"] == "billed"

    # ...and it can still advance to paid from there.
    paid_resp = preparer_client.post(f"/work-orders/{wo_id}/billing/mark", json={"status": "paid"})
    assert paid_resp.status_code == 200
    assert _billing_request(taxops_db_path, wo_id)["status"] == "paid"


def test_billing_mark_rejects_unknown_status(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Bad Status Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    bad = client_logged_in.post(f"/work-orders/{wo_id}/billing/mark", json={"status": "cancelled"})
    assert bad.status_code == 400
    assert _billing_request(taxops_db_path, wo_id)["status"] == "pending"


def test_receptionist_cannot_mark_billing_request(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_bill_recep__", "receptionist")
    recep_client = app.test_client()
    _login_as(recep_client, "__wo_bill_recep__")

    resp = recep_client.post("/work-orders", data=_create_payload("Recep Billing Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    forbidden = recep_client.post(f"/work-orders/{wo_id}/billing/mark", json={"status": "billed"})
    assert forbidden.status_code == 403
    assert _billing_request(taxops_db_path, wo_id)["status"] == "pending"


def test_deleting_work_order_cascades_billing_request(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Delete With Billing"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)
    wo_number = _wo_number(taxops_db_path, wo_id)
    assert _billing_request(taxops_db_path, wo_id) is not None

    ok = client_logged_in.post(f"/work-orders/{wo_id}/delete", json={"confirm_work_order_number": wo_number})
    assert ok.status_code == 200
    assert _billing_request(taxops_db_path, wo_id) is None  # ON DELETE CASCADE


def test_delete_button_onclick_is_well_formed_html(client_logged_in, taxops_db_path):
    """Regression: woDelete({{ wo.id }}, {{ wo.work_order_number | tojson }}) sat
    inside a double-quoted onclick="..." attribute, but tojson wraps the string
    work_order_number in double quotes too — closing the attribute early and
    leaving an unclosed-paren onclick handler. Browsers threw "Uncaught
    SyntaxError: Unexpected end of input" trying to compile it, which is
    exactly the "deleting work orders causes an error" report. Fixed with a
    single-quoted onclick attribute (tojson's output only ever contains
    double quotes internally, so it's safe there)."""
    resp = client_logged_in.post("/work-orders", data=_create_payload("Onclick Check Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)
    wo_number = _wo_number(taxops_db_path, wo_id)

    detail = client_logged_in.get(f"/work-orders/{wo_id}")
    assert detail.status_code == 200
    html = detail.data.decode("utf-8")

    assert f"onclick='woDelete({wo_id}, \"{wo_number}\")'" in html
    assert f'onclick="woDelete({wo_id}, "' not in html


def test_billing_request_visible_on_detail_page(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Visible Billing Client", fee="175.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    detail = client_logged_in.get(f"/work-orders/{wo_id}")
    assert detail.status_code == 200
    assert b"Billing Request" in detail.data
    assert b"175.00" in detail.data


# ── WO-3: billing request folded into the one work order print-out ─────────────

def test_print_view_includes_billing_amount_and_requester(client_logged_in, taxops_db_path):
    resp = client_logged_in.post("/work-orders", data=_create_payload("Print Billing Client", fee="175.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    html = print_resp.data.decode("utf-8")
    assert "Billing Request" in html
    assert "175.00" in html
    # requester: client_logged_in creates the work order as __test_user__/"Test User"
    assert "Test User" in html
    # analog hand-off: no separate print page — same sheet gets a checkbox
    # row (not a blank name field) for whoever bills it to mark themselves.
    assert "Billed By" in html
    assert "Date Billed" in html


def test_print_view_billing_block_repeats_client_and_item_info(client_logged_in):
    """The billing block is meant to be cut off and handed to billing as its
    own slip, so it must be self-contained: work order #, client name, and
    the itemized work being billed — not just a bare dollar amount."""
    resp = client_logged_in.post(
        "/work-orders",
        data=_create_payload("Standalone Slip Client", desc="Corporate Book", fee="475.00"),
        follow_redirects=False,
    )
    wo_id = _wo_id_from_redirect(resp)

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    html = print_resp.data.decode("utf-8")

    body_start = html.index("<body")
    billing_idx = html.index("Billing Request", body_start)
    billing_html = html[billing_idx:]

    assert "Work Order #" in billing_html
    assert "Standalone Slip Client" in billing_html
    assert "Corporate Book" in billing_html
    assert "475.00" in billing_html


def test_print_view_shows_billed_by_checkbox_for_each_staff_member(client_logged_in, taxops_db_path):
    for username in ("lucy", "lorena", "moises", "marlin", "armida", "sandra", "other_staff"):
        _seed_user(taxops_db_path, username, "receptionist")

    resp = client_logged_in.post("/work-orders", data=_create_payload("Checkbox Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    html = print_resp.data.decode("utf-8")
    # Abbreviated, per the ask ("same peoples names ... abbreviated to fit")
    for abbrev in ("Lucy", "Lore", "Mois", "Marl", "Armi", "Sand", "Other"):
        assert abbrev in html
    assert html.count('class="chk-box"') >= 7


def test_print_view_has_closing_dashed_line_after_billing_block(client_logged_in):
    """Short work orders leave blank space at the bottom of the letter-size
    sheet — a closing dashed line after the Billing Request block caps the
    form so that blank space doesn't read as a cut-off/incomplete printout."""
    resp = client_logged_in.post("/work-orders", data=_create_payload("End Line Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    html = print_resp.data.decode("utf-8")

    body_start = html.index("<body")
    billing_idx = html.index("Billing Request", body_start)
    end_line_idx = html.index("form-end-line", body_start)
    assert end_line_idx > billing_idx


def test_only_one_print_route_exists_for_billing(client_logged_in, taxops_db_path):
    """The old standalone billing print page is gone — everything is on the
    single /work-orders/<id>/print sheet now, so staff never print twice."""
    resp = client_logged_in.post("/work-orders", data=_create_payload("No Second Page Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    assert client_logged_in.get(f"/work-orders/{wo_id}/billing/print").status_code == 404


def test_receptionist_can_print_own_work_order_but_not_anothers(app, taxops_db_path):
    _seed_user(taxops_db_path, "__wo_print_a__", "receptionist")
    _seed_user(taxops_db_path, "__wo_print_b__", "receptionist")

    client_a = app.test_client()
    _login_as(client_a, "__wo_print_a__")
    resp = client_a.post("/work-orders", data=_create_payload("Recep Print Client"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    assert client_a.get(f"/work-orders/{wo_id}/print").status_code == 200

    client_b = app.test_client()
    _login_as(client_b, "__wo_print_b__")
    assert client_b.get(f"/work-orders/{wo_id}/print").status_code == 403


def test_print_view_backfills_for_pre_existing_work_order_without_billing_row(client_logged_in, taxops_db_path):
    """Any work order created before billing_requests existed still needs a
    printable billing section — the print route must backfill on the fly,
    same as the /billing/mark endpoint does."""
    resp = client_logged_in.post("/work-orders", data=_create_payload("Legacy WO Client", fee="250.00"), follow_redirects=False)
    wo_id = _wo_id_from_redirect(resp)

    conn = get_connection(taxops_db_path)
    conn.execute("DELETE FROM billing_requests WHERE work_order_id=?", (wo_id,))
    conn.commit()
    conn.close()

    print_resp = client_logged_in.get(f"/work-orders/{wo_id}/print")
    assert print_resp.status_code == 200
    assert b"250.00" in print_resp.data

    conn = get_connection(taxops_db_path)
    br = conn.execute("SELECT * FROM billing_requests WHERE work_order_id=?", (wo_id,)).fetchone()
    conn.close()
    assert br is not None
    assert float(br["amount"]) == 250.00
