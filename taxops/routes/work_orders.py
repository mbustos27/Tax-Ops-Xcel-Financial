"""WO-1: Work Order Creator — form-based module for staff to create, fill
out, and print Work Orders (letter-size, normal printer; separate concept
from returns / LOG:##### file-tracking barcodes).

Sequential WO-##### numbering uses the exact same BEGIN IMMEDIATE +
UNIQUE-index + retry-on-conflict pattern as the intake route's log_number
race fix (see AUDIT_INTAKE.md Link 3, and app.py's
_run_intake_write / _is_log_number_conflict) — a separate counter, not
reusing the LOG sequence.

RBAC (WO-5):
  Reception: create; view/edit/print only work orders THEY created, and
             only while status='open'.
  Preparer:  create; view/edit/print ALL open work orders; mark processed
             (self-attest only — see _process, no "on behalf of" mode).
  Admin:     full CRUD, including delete and editing already-processed
             work orders.

Routes
------
GET  /work-orders                 — list / search / filter (M6)
GET  /work-orders/new             — create form
POST /work-orders                 — create
GET  /work-orders/<id>            — detail view
GET  /work-orders/<id>/edit       — edit form (open only, or admin)
POST /work-orders/<id>            — update
POST /work-orders/<id>/process    — mark processed (self-attest)
POST /work-orders/<id>/delete     — admin-only hard delete, confirm step
GET  /work-orders/<id>/print      — printable view (M4)
POST /work-orders/<id>/billing/mark — advance the attached billing request's
                                       status (pending -> billed -> paid)
POST /work-orders/<id>/assign     — assign/reassign to a staff member (WO-7);
                                     notifies the new assignee (in-app, see
                                     _notify_work_order_assigned)

WO-7 (assign to an employee):
  A work order can be assigned to any active staff member (not just the
  curated front-desk "Received By" list — assignment is about who does the
  work). Assigning/reassigning (create form, edit form, or the detail page's
  quick /assign action) writes a row to the generic `notifications` table
  for the new assignee — TaxOps has no outbound email today, so this is an
  in-app bell/badge (see base.html's notification bell + base_ctx's
  my_unread_notification_count), not an email. Re-assigning to the SAME
  person, or clearing the assignment, does not re-notify.

WO-2 (billing requests):
  Every work order gets exactly one attached `billing_requests` row, created
  in the same transaction as the work order itself, carrying a snapshot of
  the fee (`total_fee`). While the billing request is still 'pending', an
  edit to the work order's line items keeps the billing request's `amount`
  in sync; once accounting has acted on it (status moved to 'billed' or
  'paid') further edits no longer silently change the amount someone may
  have already invoiced/collected — see _sync_billing_request().
"""
from __future__ import annotations

import logging
import sqlite3
from datetime import date

from flask import Blueprint, abort, jsonify, redirect, render_template, request, session, url_for

from auth import get_effective_role, login_required, role_required
from db import get_connection

logger = logging.getLogger("work_orders")

work_orders_bp = Blueprint("work_orders", __name__)

_WO_PREFIX = "WO-"
_WO_WIDTH = 5
_WO_MAX_ATTEMPTS = 3


# ── Shared helpers ────────────────────────────────────────────────────────────

def _next_work_order_number(conn: sqlite3.Connection) -> str:
    """Caller must already hold the write lock (BEGIN IMMEDIATE) before calling."""
    row = conn.execute(
        "SELECT MAX(CAST(SUBSTR(work_order_number, ?) AS INTEGER)) AS mx "
        "FROM work_orders WHERE work_order_number LIKE ?",
        (len(_WO_PREFIX) + 1, f"{_WO_PREFIX}%"),
    ).fetchone()
    return f"{_WO_PREFIX}{(row['mx'] or 0) + 1:0{_WO_WIDTH}d}"


def _is_wo_number_conflict(exc: sqlite3.IntegrityError) -> bool:
    # Matches ux_work_orders_number's UNIQUE index message exactly, so any
    # unrelated IntegrityError (bad FK, etc.) is never mistaken for the
    # number race and silently retried.
    return "work_orders.work_order_number" in str(exc)


# WO-1 curation: "Received By" is deliberately NOT "every active auth_users
# row" (that would include admin/test/verify accounts that never take a
# work order at the front desk) — it's this exact named list, in this exact
# order, per the explicit ask. "other_staff" is a placeholder account (not a
# real login) that backs the paper form's generic "Other" bucket so the FK
# always has a valid target. Add/remove usernames here to change the list;
# each username must exist in auth_users (see routes/users.py / admin panel).
_RECEIVED_BY_USERNAMES = ["lucy", "lorena", "moises", "marlin", "armida", "sandra", "other_staff"]


def _active_staff(conn: sqlite3.Connection):
    placeholders = ",".join("?" * len(_RECEIVED_BY_USERNAMES))
    rows = conn.execute(
        f"SELECT id, username, display_name FROM auth_users "
        f"WHERE is_active=1 AND username IN ({placeholders})",
        _RECEIVED_BY_USERNAMES,
    ).fetchall()
    by_username = {r["username"]: r for r in rows}
    # Preserve _RECEIVED_BY_USERNAMES' order (Lucy, Lorena, Moises, Marlin,
    # Armida, Sandra, Other) rather than an alphabetical/display_name sort —
    # missing/deactivated usernames are silently skipped rather than erroring,
    # so this stays safe if an account is later deactivated.
    ordered = [by_username[u] for u in _RECEIVED_BY_USERNAMES if u in by_username]
    return ordered


# WO-3: the printed "Billed By" checklist reuses the exact same named staff
# as _RECEIVED_BY_USERNAMES (per the ask: "the same peoples names we used
# earlier"), just abbreviated to fit a row of checkboxes on a letter-size
# sheet. Keyed by username so it stays in lockstep if that list ever changes.
_RECEIVED_BY_ABBREV = {
    "lucy": "Lucy",
    "lorena": "Lore",
    "moises": "Mois",
    "marlin": "Marl",
    "armida": "Armi",
    "sandra": "Sand",
    "other_staff": "Other",
}


def _billed_by_checklist(conn: sqlite3.Connection) -> list[dict]:
    return [
        {"username": s["username"], "abbrev": _RECEIVED_BY_ABBREV.get(s["username"], s["display_name"])}
        for s in _active_staff(conn)
    ]


def _current_user_id(conn: sqlite3.Connection) -> int | None:
    username = session.get("username")
    if not username:
        return None
    row = conn.execute("SELECT id FROM auth_users WHERE username=?", (username,)).fetchone()
    return row["id"] if row else None


# WO-7: "assign to an employee" — deliberately broader than _active_staff's
# curated front-desk list (_RECEIVED_BY_USERNAMES). Assignment is about who
# does the work (any active staff member, any role), not who greeted the
# client at intake, so this is every active login rather than a fixed list.
def _assignable_staff(conn: sqlite3.Connection):
    return conn.execute(
        "SELECT id, username, display_name FROM auth_users WHERE is_active=1 "
        "ORDER BY COALESCE(display_name, username)"
    ).fetchall()


def _notify_user(
    conn: sqlite3.Connection, *, user_id: int, title: str, body: str | None,
    link_url: str | None, entity_type: str | None, entity_id: int | None, ts: str,
) -> None:
    """Generic per-user in-app notification (WO-7). TaxOps has no outbound
    email today (mail_watcher.py is inbound IMAP only) — this is the bell/
    badge in base.html, not an email, per the current infra."""
    conn.execute(
        "INSERT INTO notifications (user_id, title, body, link_url, entity_type, entity_id, is_read, created_at) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?)",
        (user_id, title, body, link_url, entity_type, entity_id, ts),
    )


def _notify_work_order_assigned(
    conn: sqlite3.Connection, *, wo_id: int, wo_number: str, client_name: str,
    assignee_user_id: int, assigned_by_display: str, ts: str,
) -> None:
    _notify_user(
        conn, user_id=assignee_user_id,
        title="New Work Order Assigned",
        body=f"{wo_number} — {client_name} (assigned by {assigned_by_display})",
        link_url=f"/work-orders/{wo_id}",
        entity_type="work_order", entity_id=wo_id, ts=ts,
    )


def _current_user_display(conn: sqlite3.Connection) -> str:
    username = session.get("username") or ""
    row = conn.execute("SELECT display_name FROM auth_users WHERE username=?", (username,)).fetchone()
    return (row["display_name"] if row and row["display_name"] else username) or "a staff member"


def _parse_assigned_to(form) -> int | None:
    raw = (form.get("assigned_to_user_id") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _compute_total(items: list[dict]) -> float:
    return round(sum(float(i.get("fee") or 0) for i in items), 2)


# WO-6: quick picks can carry extra fee components (see db.py's
# work_order_quick_pick_components) — e.g. "Statement of Information" ($125)
# plus a "Filing Fee" ($25) component. This is the one place that assembles
# active picks + their components for the create/edit form, so all four call
# sites (new/create-error/edit/update-error) stay in lockstep.
def _active_quick_picks(conn: sqlite3.Connection) -> list[dict]:
    picks = conn.execute(
        "SELECT id, label, default_fee FROM work_order_quick_picks WHERE active=1 ORDER BY sort_order"
    ).fetchall()
    components = conn.execute(
        """
        SELECT c.quick_pick_id, c.label, c.fee
        FROM work_order_quick_pick_components c
        JOIN work_order_quick_picks p ON p.id = c.quick_pick_id
        WHERE p.active = 1
        ORDER BY c.quick_pick_id, c.sort_order, c.id
        """
    ).fetchall()
    by_pick: dict[int, list[dict]] = {}
    for c in components:
        by_pick.setdefault(c["quick_pick_id"], []).append({"label": c["label"], "fee": c["fee"]})
    return [
        {"id": p["id"], "label": p["label"], "default_fee": p["default_fee"], "components": by_pick.get(p["id"], [])}
        for p in picks
    ]


def _get_work_order_or_404(conn: sqlite3.Connection, wo_id: int) -> sqlite3.Row:
    row = conn.execute(
        """
        SELECT wo.*,
               ru.display_name AS received_by_name, ru.username AS received_by_username,
               pu.display_name AS processed_by_name, pu.username AS processed_by_username,
               cu.display_name AS created_by_name, cu.username AS created_by_username,
               au.display_name AS assigned_to_name, au.username AS assigned_to_username,
               abu.display_name AS assigned_by_name, abu.username AS assigned_by_username
        FROM work_orders wo
        LEFT JOIN auth_users ru ON ru.id = wo.received_by_user_id
        LEFT JOIN auth_users pu ON pu.id = wo.processed_by_user_id
        LEFT JOIN auth_users cu ON cu.id = wo.created_by_user_id
        LEFT JOIN auth_users au ON au.id = wo.assigned_to_user_id
        LEFT JOIN auth_users abu ON abu.id = wo.assigned_by_user_id
        WHERE wo.id = ?
        """,
        (wo_id,),
    ).fetchone()
    if not row:
        abort(404)
    return row


def _can_view(conn: sqlite3.Connection, wo_row: sqlite3.Row) -> bool:
    role = get_effective_role()
    if role in ("preparer", "admin"):
        return True
    # receptionist: own-created only
    return wo_row["created_by_user_id"] == _current_user_id(conn)


def _can_edit(conn: sqlite3.Connection, wo_row: sqlite3.Row) -> bool:
    role = get_effective_role()
    if role == "admin":
        return True
    if (wo_row["status"] or "open") == "processed":
        return False  # only admin may touch a processed work order
    if role == "preparer":
        return True
    return wo_row["created_by_user_id"] == _current_user_id(conn)


# WO-2: billing request lifecycle. "pending" is the only state an edit is
# allowed to silently overwrite the amount for — once accounting has acted
# on it (billed/paid) it's frozen against further work-order edits.
_BILLING_STATUSES = ("pending", "billed", "paid")


def _get_billing_request(conn: sqlite3.Connection, wo_id: int) -> sqlite3.Row | None:
    return conn.execute(
        "SELECT * FROM billing_requests WHERE work_order_id=?", (wo_id,)
    ).fetchone()


def _get_billing_request_with_requester(conn: sqlite3.Connection, wo_id: int) -> sqlite3.Row | None:
    """Same as _get_billing_request but joined to the requester's name, for the
    printable billing request (WO-3) — "who requested the billing" is whoever
    created the work order, since the billing request is generated alongside it.
    """
    return conn.execute(
        """
        SELECT br.*, u.display_name AS requested_by_name, u.username AS requested_by_username
        FROM billing_requests br
        LEFT JOIN auth_users u ON u.id = br.created_by_user_id
        WHERE br.work_order_id = ?
        """,
        (wo_id,),
    ).fetchone()


def _create_billing_request(
    conn: sqlite3.Connection, *, wo_id: int, client_id, client_name: str,
    amount: float, created_by_user_id, ts: str,
) -> None:
    conn.execute(
        "INSERT INTO billing_requests "
        "(work_order_id, client_id, client_name, amount, status, created_by_user_id, created_at, updated_at) "
        "VALUES (?, ?, ?, ?, 'pending', ?, ?, ?)",
        (wo_id, client_id, client_name, amount, created_by_user_id, ts, ts),
    )


def _sync_billing_request(
    conn: sqlite3.Connection, *, wo_id: int, client_id, client_name: str, amount: float, ts: str,
) -> None:
    """Keep the attached billing request's amount/client_name current on
    work-order edits — but only while it's still 'pending'. Once accounting
    has moved it to 'billed' or 'paid', a later line-item edit must not
    quietly change a figure that may already be on an invoice or receipt.
    """
    existing = _get_billing_request(conn, wo_id)
    if existing is None:
        # Backfill for any work order created before this table existed.
        _create_billing_request(
            conn, wo_id=wo_id, client_id=client_id, client_name=client_name,
            amount=amount, created_by_user_id=None, ts=ts,
        )
        return
    if existing["status"] != "pending":
        return
    conn.execute(
        "UPDATE billing_requests SET client_id=?, client_name=?, amount=?, updated_at=? WHERE work_order_id=?",
        (client_id, client_name, amount, ts, wo_id),
    )


def _forbidden():
    # All routes that call this are plain HTML views/form-posts (never fetch/JSON
    # endpoints — /process and /delete are JSON but use role_required directly,
    # never this helper), so the app's registered 403 error page is always right.
    abort(403)


def _enqueue_audit(*, user_id, action, entity_id, before=None, after=None) -> None:
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id=user_id, action=action, entity_type="work_order", entity_id=str(entity_id),
            before=before, after=after, ip_address=request.remote_addr, http_status=200,
        )
    except Exception as exc:
        logger.warning("work_orders audit write failed: %s", exc)


def _parse_items_from_form(form) -> list[dict]:
    """Line items posted as parallel arrays: description[], fee[], is_quick_pick[]."""
    descriptions = form.getlist("item_description[]")
    fees = form.getlist("item_fee[]")
    quick_flags = form.getlist("item_is_quick_pick[]")
    items = []
    for i, desc in enumerate(descriptions):
        desc = (desc or "").strip()
        fee_raw = (fees[i] if i < len(fees) else "") or "0"
        try:
            fee = float(fee_raw)
        except ValueError:
            fee = 0.0
        if not desc:
            continue
        items.append({
            "description": desc,
            "fee": max(0.0, fee),
            "sort_order": i,
            "is_quick_pick": 1 if (i < len(quick_flags) and quick_flags[i] == "1") else 0,
        })
    return items


# ── List / search / filter (M6) ───────────────────────────────────────────────

@work_orders_bp.route("/work-orders")
@login_required
def work_orders_list():
    status_filter = request.args.get("status", "")
    date_from = request.args.get("date_from", "")
    date_to = request.args.get("date_to", "")
    q = (request.args.get("q") or "").strip()
    assigned_to_me = request.args.get("assigned_to_me") == "1"

    clauses = []
    params: list = []
    role = get_effective_role()
    conn = get_connection()
    try:
        uid = _current_user_id(conn)
        if role == "receptionist":
            clauses.append("wo.created_by_user_id = ?")
            params.append(uid)
        if assigned_to_me:
            clauses.append("wo.assigned_to_user_id = ?")
            params.append(uid)
        if status_filter in ("open", "processed"):
            clauses.append("wo.status = ?")
            params.append(status_filter)
        if date_from:
            clauses.append("wo.date_created >= ?")
            params.append(date_from)
        if date_to:
            clauses.append("wo.date_created <= ?")
            params.append(date_to)
        if q:
            clauses.append("wo.client_name LIKE ?")
            params.append(f"%{q}%")

        where = f"WHERE {' AND '.join(clauses)}" if clauses else ""
        rows = conn.execute(
            f"""
            SELECT wo.id, wo.work_order_number, wo.client_name, wo.date_created, wo.due_by,
                   wo.status, wo.total_fee, ru.display_name AS received_by_name,
                   ru.username AS received_by_username, br.status AS billing_status,
                   au.display_name AS assigned_to_name, au.username AS assigned_to_username
            FROM work_orders wo
            LEFT JOIN auth_users ru ON ru.id = wo.received_by_user_id
            LEFT JOIN auth_users au ON au.id = wo.assigned_to_user_id
            LEFT JOIN billing_requests br ON br.work_order_id = wo.id
            {where}
            ORDER BY (wo.status = 'processed') ASC, wo.date_created DESC, wo.id DESC
            LIMIT 500
            """,
            params,
        ).fetchall()
    finally:
        conn.close()

    from app import base_ctx
    ctx = base_ctx()
    ctx.update(
        active_page="work_orders",
        work_orders=[dict(r) for r in rows],
        status_filter=status_filter,
        date_from=date_from,
        date_to=date_to,
        q=q,
        assigned_to_me=assigned_to_me,
    )
    return render_template("work_orders_list.html", **ctx)


# ── Create ────────────────────────────────────────────────────────────────────

@work_orders_bp.route("/work-orders/new")
@login_required
def work_order_new():
    conn = get_connection()
    try:
        staff = _active_staff(conn)
        assignable_staff = _assignable_staff(conn)
        quick_picks = _active_quick_picks(conn)
    finally:
        conn.close()

    from app import base_ctx
    ctx = base_ctx()
    ctx.update(
        active_page="work_orders",
        staff=[dict(s) for s in staff],
        assignable_staff=[dict(s) for s in assignable_staff],
        quick_picks=quick_picks,
        today=date.today().isoformat(),
        work_order=None,
        items=[],
        form_action=url_for("work_orders.work_order_create"),
        error=None,
    )
    return render_template("work_order_form.html", **ctx)


@work_orders_bp.post("/work-orders")
@login_required
def work_order_create():
    f = request.form
    client_name = (f.get("client_name") or "").strip()
    date_created = (f.get("date_created") or date.today().isoformat()).strip()
    due_by = (f.get("due_by") or "").strip() or None
    received_by_user_id = f.get("received_by_user_id") or None
    client_id = f.get("client_id") or None
    assigned_to_user_id = _parse_assigned_to(f)
    items = _parse_items_from_form(f)

    def _render_error(msg: str, status: int = 400):
        conn2 = get_connection()
        try:
            staff = _active_staff(conn2)
            assignable_staff = _assignable_staff(conn2)
            quick_picks = _active_quick_picks(conn2)
        finally:
            conn2.close()
        from app import base_ctx
        ctx = base_ctx()
        ctx.update(
            active_page="work_orders", staff=[dict(s) for s in staff],
            assignable_staff=[dict(s) for s in assignable_staff],
            quick_picks=quick_picks, today=date_created,
            work_order={
                "client_name": client_name, "date_created": date_created, "due_by": due_by,
                "assigned_to_user_id": assigned_to_user_id,
            },
            items=items, form_action=url_for("work_orders.work_order_create"), error=msg,
        )
        return render_template("work_order_form.html", **ctx), status

    if not client_name:
        return _render_error("Client name is required.")
    if not items:
        return _render_error("At least one line item is required.")
    if any(i["fee"] < 0 for i in items):
        return _render_error("Fees must be zero or greater.")

    total_fee = _compute_total(items)
    ts = __import__("utils").now()
    user = session.get("username")

    conn = get_connection()
    try:
        created_by_user_id = _current_user_id(conn)
        if assigned_to_user_id is not None:
            valid_assignee = conn.execute(
                "SELECT 1 FROM auth_users WHERE id=? AND is_active=1", (assigned_to_user_id,)
            ).fetchone()
            if not valid_assignee:
                return _render_error("Selected assignee is not a valid, active staff member.")
        for attempt in range(1, _WO_MAX_ATTEMPTS + 1):
            try:
                conn.execute("BEGIN IMMEDIATE")
                wo_number = _next_work_order_number(conn)
                cur = conn.execute(
                    """
                    INSERT INTO work_orders (
                        work_order_number, client_id, client_name, date_created, due_by,
                        received_by_user_id, created_by_user_id, status, total_fee,
                        created_at, updated_at, assigned_to_user_id, assigned_by_user_id, assigned_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, 'open', ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        wo_number, client_id, client_name, date_created, due_by,
                        received_by_user_id, created_by_user_id, total_fee, ts, ts,
                        assigned_to_user_id,
                        created_by_user_id if assigned_to_user_id else None,
                        ts if assigned_to_user_id else None,
                    ),
                )
                wo_id = cur.lastrowid
                for item in items:
                    conn.execute(
                        "INSERT INTO work_order_items (work_order_id, description, fee, sort_order, is_quick_pick) "
                        "VALUES (?, ?, ?, ?, ?)",
                        (wo_id, item["description"], item["fee"], item["sort_order"], item["is_quick_pick"]),
                    )
                _create_billing_request(
                    conn, wo_id=wo_id, client_id=client_id, client_name=client_name,
                    amount=total_fee, created_by_user_id=created_by_user_id, ts=ts,
                )
                if assigned_to_user_id:
                    _notify_work_order_assigned(
                        conn, wo_id=wo_id, wo_number=wo_number, client_name=client_name,
                        assignee_user_id=assigned_to_user_id,
                        assigned_by_display=_current_user_display(conn), ts=ts,
                    )
                conn.commit()
                break
            except sqlite3.IntegrityError as exc:
                conn.rollback()
                if _is_wo_number_conflict(exc) and attempt < _WO_MAX_ATTEMPTS:
                    continue
                if _is_wo_number_conflict(exc):
                    return _render_error("Could not assign a work order number, please retry.", 409)
                return _render_error(str(exc), 500)
    finally:
        conn.close()

    _enqueue_audit(
        user_id=user, action="WORK_ORDER_CREATED", entity_id=wo_id,
        after={"work_order_number": wo_number, "client_name": client_name, "total_fee": total_fee},
    )
    return redirect(url_for("work_orders.work_order_detail", wo_id=wo_id))


# ── Detail ────────────────────────────────────────────────────────────────────

@work_orders_bp.route("/work-orders/<int:wo_id>")
@login_required
def work_order_detail(wo_id: int):
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if not _can_view(conn, wo):
            return _forbidden()
        items = conn.execute(
            "SELECT * FROM work_order_items WHERE work_order_id=? ORDER BY sort_order, id",
            (wo_id,),
        ).fetchall()
        billing_request = _get_billing_request(conn, wo_id)
        role = get_effective_role()
        can_edit = _can_edit(conn, wo)
        assignable_staff = _assignable_staff(conn) if can_edit else []
    finally:
        conn.close()

    from app import base_ctx
    ctx = base_ctx()
    ctx.update(
        active_page="work_orders",
        wo=dict(wo),
        items=[dict(i) for i in items],
        can_edit=can_edit,
        assignable_staff=[dict(s) for s in assignable_staff],
        billing_request=dict(billing_request) if billing_request else None,
        can_manage_billing=role in ("preparer", "admin"),
    )
    return render_template("work_order_detail.html", **ctx)


# ── Edit ──────────────────────────────────────────────────────────────────────

@work_orders_bp.route("/work-orders/<int:wo_id>/edit")
@login_required
def work_order_edit(wo_id: int):
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if not _can_edit(conn, wo):
            return _forbidden()
        staff = _active_staff(conn)
        assignable_staff = _assignable_staff(conn)
        quick_picks = _active_quick_picks(conn)
        items = conn.execute(
            "SELECT * FROM work_order_items WHERE work_order_id=? ORDER BY sort_order, id",
            (wo_id,),
        ).fetchall()
    finally:
        conn.close()

    from app import base_ctx
    ctx = base_ctx()
    ctx.update(
        active_page="work_orders",
        staff=[dict(s) for s in staff],
        assignable_staff=[dict(s) for s in assignable_staff],
        quick_picks=quick_picks,
        today=wo["date_created"],
        work_order=dict(wo),
        items=[dict(i) for i in items],
        form_action=url_for("work_orders.work_order_update", wo_id=wo_id),
        error=None,
    )
    return render_template("work_order_form.html", **ctx)


@work_orders_bp.post("/work-orders/<int:wo_id>")
@login_required
def work_order_update(wo_id: int):
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if not _can_edit(conn, wo):
            return _forbidden()

        f = request.form
        client_name = (f.get("client_name") or "").strip()
        date_created = (f.get("date_created") or wo["date_created"]).strip()
        due_by = (f.get("due_by") or "").strip() or None
        received_by_user_id = f.get("received_by_user_id") or None
        client_id = f.get("client_id") or None
        assigned_to_user_id = _parse_assigned_to(f)
        items = _parse_items_from_form(f)

        def _render_edit_error(msg: str):
            staff = _active_staff(conn)
            assignable_staff = _assignable_staff(conn)
            quick_picks = _active_quick_picks(conn)
            from app import base_ctx
            ctx = base_ctx()
            ctx.update(
                active_page="work_orders", staff=[dict(s) for s in staff],
                assignable_staff=[dict(s) for s in assignable_staff],
                quick_picks=quick_picks, today=date_created,
                work_order={
                    **dict(wo), "client_name": client_name, "date_created": date_created, "due_by": due_by,
                    "assigned_to_user_id": assigned_to_user_id,
                },
                items=items, form_action=url_for("work_orders.work_order_update", wo_id=wo_id),
                error=msg,
            )
            return render_template("work_order_form.html", **ctx), 400

        if not client_name or not items or any(i["fee"] < 0 for i in items):
            return _render_edit_error("Client name and at least one valid line item (fee >= 0) are required.")

        if assigned_to_user_id is not None:
            valid_assignee = conn.execute(
                "SELECT 1 FROM auth_users WHERE id=? AND is_active=1", (assigned_to_user_id,)
            ).fetchone()
            if not valid_assignee:
                return _render_edit_error("Selected assignee is not a valid, active staff member.")

        total_fee = _compute_total(items)
        ts = __import__("utils").now()
        before = dict(wo)

        prior_assignee = wo["assigned_to_user_id"]
        reassigning_to_new_person = (
            assigned_to_user_id is not None and assigned_to_user_id != prior_assignee
        )

        conn.execute(
            "UPDATE work_orders SET client_id=?, client_name=?, date_created=?, due_by=?, "
            "received_by_user_id=?, total_fee=?, updated_at=?, assigned_to_user_id=?, "
            "assigned_by_user_id=CASE WHEN ? THEN ? ELSE assigned_by_user_id END, "
            "assigned_at=CASE WHEN ? THEN ? ELSE assigned_at END "
            "WHERE id=?",
            (
                client_id, client_name, date_created, due_by, received_by_user_id, total_fee, ts,
                assigned_to_user_id,
                reassigning_to_new_person, _current_user_id(conn),
                reassigning_to_new_person, ts,
                wo_id,
            ),
        )
        conn.execute("DELETE FROM work_order_items WHERE work_order_id=?", (wo_id,))
        for item in items:
            conn.execute(
                "INSERT INTO work_order_items (work_order_id, description, fee, sort_order, is_quick_pick) "
                "VALUES (?, ?, ?, ?, ?)",
                (wo_id, item["description"], item["fee"], item["sort_order"], item["is_quick_pick"]),
            )
        _sync_billing_request(
            conn, wo_id=wo_id, client_id=client_id, client_name=client_name, amount=total_fee, ts=ts,
        )
        if reassigning_to_new_person:
            _notify_work_order_assigned(
                conn, wo_id=wo_id, wo_number=wo["work_order_number"], client_name=client_name,
                assignee_user_id=assigned_to_user_id,
                assigned_by_display=_current_user_display(conn), ts=ts,
            )
        conn.commit()
    finally:
        conn.close()

    _enqueue_audit(
        user_id=session.get("username"), action="WORK_ORDER_UPDATED", entity_id=wo_id,
        before={"client_name": before["client_name"], "total_fee": before["total_fee"]},
        after={"client_name": client_name, "total_fee": total_fee},
    )
    return redirect(url_for("work_orders.work_order_detail", wo_id=wo_id))


# ── Assign / reassign (WO-7) ──────────────────────────────────────────────────
# Quick-action for the detail page — same RBAC as editing the work order
# itself (_can_edit), so this doesn't open a new permission hole; it's just
# a faster path than the full edit form for the one field people change most
# often day-to-day ("send this one to Marlin").

@work_orders_bp.post("/work-orders/<int:wo_id>/assign")
@login_required
def work_order_assign(wo_id: int):
    data = request.get_json(silent=True) or {}
    raw = data.get("assigned_to_user_id")
    new_assignee_id = int(raw) if raw not in (None, "", "null") else None

    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if not _can_edit(conn, wo):
            # Unlike _forbidden() (used by the plain HTML views), this is a
            # fetch()-based JSON endpoint — the JS always does resp.json(),
            # so a 403 here has to stay JSON, not the HTML error page.
            return jsonify({"error": "You don't have permission to reassign this work order."}), 403

        if new_assignee_id is not None:
            valid = conn.execute(
                "SELECT 1 FROM auth_users WHERE id=? AND is_active=1", (new_assignee_id,)
            ).fetchone()
            if not valid:
                return jsonify({"error": "Selected assignee is not a valid, active staff member."}), 400

        prior_assignee = wo["assigned_to_user_id"]
        ts = __import__("utils").now()
        current_uid = _current_user_id(conn)

        if new_assignee_id is None:
            conn.execute(
                "UPDATE work_orders SET assigned_to_user_id=NULL WHERE id=?", (wo_id,)
            )
        else:
            conn.execute(
                "UPDATE work_orders SET assigned_to_user_id=?, assigned_by_user_id=?, assigned_at=? WHERE id=?",
                (new_assignee_id, current_uid, ts, wo_id),
            )
            if new_assignee_id != prior_assignee:
                _notify_work_order_assigned(
                    conn, wo_id=wo_id, wo_number=wo["work_order_number"], client_name=wo["client_name"],
                    assignee_user_id=new_assignee_id, assigned_by_display=_current_user_display(conn), ts=ts,
                )
        conn.commit()
    finally:
        conn.close()

    _enqueue_audit(
        user_id=session.get("username"), action="WORK_ORDER_ASSIGNED", entity_id=wo_id,
        before={"assigned_to_user_id": prior_assignee}, after={"assigned_to_user_id": new_assignee_id},
    )
    return jsonify({"success": True})


# ── Mark processed (self-attest only) ────────────────────────────────────────

@work_orders_bp.post("/work-orders/<int:wo_id>/process")
@role_required("preparer")
def work_order_process(wo_id: int):
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if (wo["status"] or "open") == "processed":
            return jsonify({"error": "Already processed"}), 409
        uid = _current_user_id(conn)
        if not uid:
            return jsonify({"error": "Could not identify current user"}), 400
        ts = __import__("utils").now()
        conn.execute(
            "UPDATE work_orders SET status='processed', processed_by_user_id=?, updated_at=? WHERE id=?",
            (uid, ts, wo_id),
        )
        conn.commit()
    finally:
        conn.close()

    _enqueue_audit(
        user_id=session.get("username"), action="WORK_ORDER_PROCESSED", entity_id=wo_id,
        before={"status": "open"}, after={"status": "processed", "processed_by": session.get("username")},
    )
    return jsonify({"success": True})


# ── Billing request status (WO-2) ─────────────────────────────────────────────

@work_orders_bp.post("/work-orders/<int:wo_id>/billing/mark")
@role_required("preparer")
def work_order_billing_mark(wo_id: int):
    data = request.get_json(silent=True) or {}
    new_status = str(data.get("status") or "").strip().lower()
    if new_status not in _BILLING_STATUSES:
        return jsonify({"error": f"status must be one of {_BILLING_STATUSES}"}), 400

    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)  # 404s if the work order itself is gone
        br = _get_billing_request(conn, wo_id)
        if br is None:
            # Backfill for a work order created before billing_requests existed.
            ts = __import__("utils").now()
            _create_billing_request(
                conn, wo_id=wo_id, client_id=wo["client_id"], client_name=wo["client_name"],
                amount=wo["total_fee"], created_by_user_id=None, ts=ts,
            )
            br = _get_billing_request(conn, wo_id)
        before_status = br["status"]
        ts = __import__("utils").now()
        conn.execute(
            "UPDATE billing_requests SET status=?, updated_at=? WHERE work_order_id=?",
            (new_status, ts, wo_id),
        )
        conn.commit()
    finally:
        conn.close()

    _enqueue_audit(
        user_id=session.get("username"), action="BILLING_REQUEST_STATUS_CHANGED", entity_id=wo_id,
        before={"status": before_status}, after={"status": new_status},
    )
    return jsonify({"success": True, "status": new_status})


# ── Delete (admin-only, confirm step) ─────────────────────────────────────────

@work_orders_bp.post("/work-orders/<int:wo_id>/delete")
@role_required("admin")
def work_order_delete(wo_id: int):
    data = request.get_json(silent=True) or {}
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        confirm = str(data.get("confirm_work_order_number") or "").strip()
        if confirm != str(wo["work_order_number"] or ""):
            return jsonify({
                "error": "confirm_work_order_number must match this work order's number",
                "work_order_number": wo["work_order_number"],
            }), 400
        before = dict(wo)
        conn.execute("DELETE FROM work_orders WHERE id=?", (wo_id,))  # cascades work_order_items
        conn.commit()
    finally:
        conn.close()

    _enqueue_audit(user_id=session.get("username"), action="WORK_ORDER_DELETED", entity_id=wo_id, before=before)
    return jsonify({"success": True})


# ── Printable view (M4 + WO-3 billing request) ────────────────────────────────
# WO-3: the billing request is folded into this SAME print-out rather than a
# separate page — one physical sheet per work order, not two, so the front
# desk isn't burning an extra page every time (see _BILLED_BY_CHECKLIST for
# the analog "who billed this" checkbox row this adds).

@work_orders_bp.route("/work-orders/<int:wo_id>/print")
@login_required
def work_order_print(wo_id: int):
    conn = get_connection()
    try:
        wo = _get_work_order_or_404(conn, wo_id)
        if not _can_view(conn, wo):
            return _forbidden()
        items = conn.execute(
            "SELECT * FROM work_order_items WHERE work_order_id=? ORDER BY sort_order, id",
            (wo_id,),
        ).fetchall()
        billing_request = _get_billing_request_with_requester(conn, wo_id)
        if billing_request is None:
            # Backfill for a work order created before billing_requests existed.
            ts = __import__("utils").now()
            _create_billing_request(
                conn, wo_id=wo_id, client_id=wo["client_id"], client_name=wo["client_name"],
                amount=wo["total_fee"], created_by_user_id=wo["created_by_user_id"], ts=ts,
            )
            conn.commit()
            billing_request = _get_billing_request_with_requester(conn, wo_id)
        billed_by_checklist = _billed_by_checklist(conn)
    finally:
        conn.close()

    return render_template(
        "work_order_print.html",
        wo=dict(wo),
        items=[dict(i) for i in items],
        br=dict(billing_request),
        billed_by_checklist=billed_by_checklist,
    )
