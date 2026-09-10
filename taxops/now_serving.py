"""Now Serving — single-queue, two-window take-a-number (data layer).

Ticket display is a plain day number (1, 2, 14, …). Numbers are integers in DB;
formatting is presentation-only.

Transfers to the other window are penalized: the ticket is inserted as
second-to-last in that window's waiting line (last if the line is empty/one).
After a transfer loads one window, auto-balance pulls never-transferred waiters
from the heavier line toward the lighter (no ping-pong). Printed stubs omit
window and use Filetrack LOG label geometry (2.625x1 @ 203dpi).

All mutating helpers use BEGIN IMMEDIATE so concurrent kiosk taps cannot
collide on next_number / assign_turn, and Call Next / Transfer stay consistent.
"""
from __future__ import annotations

import sqlite3
import threading
import time
from typing import Any, Optional

from db import get_connection
from utils import now

WINDOWS = (1, 2)
STATUSES = frozenset({"waiting", "serving", "done"})

# Match staff Now Serving modal poll interval.
REBALANCE_POLL_INTERVAL_SEC = 4.0
_last_rebalance_mono = 0.0
_rebalance_gate = threading.Lock()


class NowServingError(Exception):
    """Domain error for Now Serving operations (empty queue, bad window, etc.)."""


def format_ticket_label(number: int) -> str:
    """Public ticket stub label — plain day number (no A- prefix)."""
    return str(int(number))


def other_window(window: int) -> int:
    w = int(window)
    if w not in WINDOWS:
        raise NowServingError(f"Invalid window {window!r}")
    return 3 - w


def _ensure_state(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT OR IGNORE INTO now_serving_state
          (id, next_number, assign_turn, transfer_counter, event_seq)
        VALUES (1, 1, 1, 0, 0)
        """
    )


def _bump_event_seq(conn: sqlite3.Connection) -> int:
    """Caller must hold BEGIN IMMEDIATE. Returns new event_seq."""
    conn.execute(
        "UPDATE now_serving_state SET event_seq = event_seq + 1 WHERE id = 1"
    )
    row = conn.execute(
        "SELECT event_seq FROM now_serving_state WHERE id = 1"
    ).fetchone()
    return int(row["event_seq"] if row else 0)


def _row_to_ticket(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    d = dict(row)
    d["label"] = format_ticket_label(int(d["number"]))
    if "transferred" in d:
        d["transferred"] = int(d.get("transferred") or 0)
    return d


def _people_ahead(conn: sqlite3.Connection, *, window: int, queue_pos: int, ticket_id: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n
          FROM now_serving_tickets
         WHERE window = ?
           AND status = 'waiting'
           AND id != ?
           AND (
                 queue_pos < ?
              OR (queue_pos = ? AND id < ?)
           )
        """,
        (window, ticket_id, queue_pos, queue_pos, ticket_id),
    ).fetchone()
    return int(row["n"] if row else 0)


def _waiting_count(conn: sqlite3.Connection, window: int) -> int:
    row = conn.execute(
        """
        SELECT COUNT(*) AS n FROM now_serving_tickets
         WHERE window = ? AND status = 'waiting'
        """,
        (window,),
    ).fetchone()
    return int(row["n"] if row else 0)


def _append_queue_pos(conn: sqlite3.Connection, dest_window: int) -> int:
    row = conn.execute(
        """
        SELECT COALESCE(MAX(queue_pos), 0) AS mx
          FROM now_serving_tickets
         WHERE window = ? AND status = 'waiting'
        """,
        (dest_window,),
    ).fetchone()
    return int(row["mx"] if row else 0) + 1


def _rebalance_waiting_locked(conn: sqlite3.Connection) -> int:
    """Move never-transferred waiters from the heavier line toward the lighter.

    Used after a manual transfer (dest got heavier) and on the poll path when
    lines are uneven. Tickets with transferred=1 stay put — no ping-pong.
    Never auto-move someone who is still ahead of a transferred guest on the
    heavy line (that would unfairly promote the transfer to the front).
    Caller must hold BEGIN IMMEDIATE.
    """
    n1 = _waiting_count(conn, 1)
    n2 = _waiting_count(conn, 2)
    if n1 == n2:
        return 0
    if n1 > n2:
        heavy_w, light_w, n_heavy, n_light = 1, 2, n1, n2
    else:
        heavy_w, light_w, n_heavy, n_light = 2, 1, n2, n1

    to_move = (n_heavy - n_light) // 2
    if n_light == 0 and to_move == 0 and n_heavy >= 1:
        to_move = 1
    if to_move <= 0:
        return 0

    anchor = conn.execute(
        """
        SELECT COALESCE(MIN(queue_pos), -1) AS mn
          FROM now_serving_tickets
         WHERE window = ?
           AND status = 'waiting'
           AND COALESCE(transferred, 0) = 1
        """,
        (heavy_w,),
    ).fetchone()
    # If any transferred guests are in line, only pull from at/behind the
    # earliest of them — never steal people still ahead of a transfer.
    min_xfer_pos = int(anchor["mn"] if anchor else -1)

    if min_xfer_pos >= 0:
        eligible = conn.execute(
            """
            SELECT id, queue_pos
              FROM now_serving_tickets
             WHERE window = ?
               AND status = 'waiting'
               AND COALESCE(transferred, 0) = 0
               AND queue_pos >= ?
             ORDER BY queue_pos ASC, id ASC
            """,
            (heavy_w, min_xfer_pos),
        ).fetchall()
    else:
        eligible = conn.execute(
            """
            SELECT id, queue_pos
              FROM now_serving_tickets
             WHERE window = ?
               AND status = 'waiting'
               AND COALESCE(transferred, 0) = 0
             ORDER BY queue_pos ASC, id ASC
            """,
            (heavy_w,),
        ).fetchall()
    if not eligible:
        return 0

    to_move = min(to_move, len(eligible))
    # Prefer rear of the eligible list so people nearer Call Next stay.
    movers = eligible[-to_move:]
    moved = 0
    for row in movers:
        new_pos = _append_queue_pos(conn, light_w)
        conn.execute(
            """
            UPDATE now_serving_tickets
               SET window = ?, queue_pos = ?, transferred = 1
             WHERE id = ? AND status = 'waiting'
            """,
            (light_w, new_pos, int(row["id"])),
        )
        moved += 1
    if moved:
        _bump_event_seq(conn)
    return moved


def rebalance_queues(db_path: Optional[str] = None) -> int:
    """Run waiting-line auto-balance. Returns tickets moved."""
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_state(conn)
        moved = _rebalance_waiting_locked(conn)
        conn.commit()
        return moved
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def maybe_rebalance_queues(db_path: Optional[str] = None) -> int:
    """Rebalance at most once per staff-modal poll interval (4s)."""
    global _last_rebalance_mono
    now_m = time.monotonic()
    with _rebalance_gate:
        if (now_m - _last_rebalance_mono) < REBALANCE_POLL_INTERVAL_SEC:
            return 0
        _last_rebalance_mono = now_m
    return rebalance_queues(db_path)


def issue_ticket(db_path: Optional[str] = None) -> dict[str, Any]:
    """Atomically issue the next ticket and assign the next window (1-2-1-2).

    Returns ticket dict including label, window, people_ahead, and board snapshot
    fields needed by the kiosk stub confirmation.
    """
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_state(conn)
        state = conn.execute(
            "SELECT next_number, assign_turn FROM now_serving_state WHERE id = 1"
        ).fetchone()
        assert state is not None
        number = int(state["next_number"])
        window = int(state["assign_turn"])
        if window not in WINDOWS:
            window = 1
        created = now()
        # Normal tickets: queue_pos == number keeps FIFO among waiting for a window.
        queue_pos = number
        cur = conn.execute(
            """
            INSERT INTO now_serving_tickets (number, window, status, queue_pos, created_at)
            VALUES (?, ?, 'waiting', ?, ?)
            """,
            (number, window, queue_pos, created),
        )
        ticket_id = int(cur.lastrowid)
        next_turn = other_window(window)
        conn.execute(
            """
            UPDATE now_serving_state
               SET next_number = ?, assign_turn = ?
             WHERE id = 1
            """,
            (number + 1, next_turn),
        )
        _bump_event_seq(conn)
        ahead = _people_ahead(conn, window=window, queue_pos=queue_pos, ticket_id=ticket_id)
        serving = _serving_labels(conn)
        conn.commit()
        return {
            "id": ticket_id,
            "number": number,
            "label": format_ticket_label(number),
            "window": window,
            "status": "waiting",
            "queue_pos": queue_pos,
            "created_at": created,
            "people_ahead": ahead,
            "now_serving": serving,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _serving_labels(conn: sqlite3.Connection) -> dict[str, str | None]:
    out: dict[str, str | None] = {"1": None, "2": None}
    for w in WINDOWS:
        row = conn.execute(
            """
            SELECT number FROM now_serving_tickets
             WHERE window = ? AND status = 'serving'
             ORDER BY id DESC LIMIT 1
            """,
            (w,),
        ).fetchone()
        out[str(w)] = format_ticket_label(int(row["number"])) if row else None
    return out


def call_next(window: int, db_path: Optional[str] = None) -> dict[str, Any]:
    """Mark current serving ticket for *window* done; promote lowest queue_pos waiting.

    Returns the new serving ticket. Includes ``completed`` (prior ticket marked
    done, or None) so callers can close that ticket's audit trail and open a
    fresh one for the newly called client.

    Raises NowServingError if the waiting line for that window is empty.
    """
    w = int(window)
    if w not in WINDOWS:
        raise NowServingError(f"Invalid window {window!r}")

    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        prev = conn.execute(
            """
            SELECT id, number, window, status, queue_pos, created_at,
                   COALESCE(transferred, 0) AS transferred
              FROM now_serving_tickets
             WHERE window = ? AND status = 'serving'
             ORDER BY id DESC LIMIT 1
            """,
            (w,),
        ).fetchone()
        # Complete whoever is currently being served at this window.
        conn.execute(
            """
            UPDATE now_serving_tickets
               SET status = 'done'
             WHERE window = ? AND status = 'serving'
            """,
            (w,),
        )
        nxt = conn.execute(
            """
            SELECT id, number, window, status, queue_pos, created_at,
                   COALESCE(transferred, 0) AS transferred
              FROM now_serving_tickets
             WHERE window = ? AND status = 'waiting'
             ORDER BY queue_pos ASC, id ASC
             LIMIT 1
            """,
            (w,),
        ).fetchone()
        if nxt is None:
            _bump_event_seq(conn)
            conn.commit()
            raise NowServingError(f"No waiting tickets for window {w}")
        conn.execute(
            "UPDATE now_serving_tickets SET status = 'serving' WHERE id = ?",
            (nxt["id"],),
        )
        _bump_event_seq(conn)
        ticket = _row_to_ticket(nxt)
        assert ticket is not None
        ticket["status"] = "serving"
        ticket["completed"] = _row_to_ticket(prev)
        if ticket["completed"] is not None:
            ticket["completed"]["status"] = "done"
        ticket["now_serving"] = _serving_labels(conn)
        # Recompute after status flip so serving reflects this ticket.
        ticket["now_serving"][str(w)] = ticket["label"]
        waiting = conn.execute(
            """
            SELECT COUNT(*) AS n FROM now_serving_tickets
             WHERE window = ? AND status = 'waiting'
            """,
            (w,),
        ).fetchone()
        ticket["waiting_count"] = int(waiting["n"] if waiting else 0)
        conn.commit()
        return ticket
    except NowServingError:
        # Empty queue still commits the "done" flip for the prior serving ticket.
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def _waiting_rows(conn: sqlite3.Connection, window: int) -> list:
    return conn.execute(
        """
        SELECT id, number, queue_pos
          FROM now_serving_tickets
         WHERE window = ? AND status = 'waiting'
         ORDER BY queue_pos ASC, id ASC
        """,
        (window,),
    ).fetchall()


def _queue_pos_second_to_last(conn: sqlite3.Connection, dest_window: int) -> int:
    """Penalize transfers: insert as second-to-last in the destination waiting line.

    Front → back order. With 0 waiting → alone at end. With 1 waiting → go last
    (behind them). With 2+ → sit just before the current last person.
    """
    waiting = _waiting_rows(conn, dest_window)
    if not waiting:
        row = conn.execute(
            """
            SELECT COALESCE(MAX(queue_pos), 0) AS mx
              FROM now_serving_tickets
             WHERE window = ?
            """,
            (dest_window,),
        ).fetchone()
        return int(row["mx"] if row else 0) + 1

    if len(waiting) == 1:
        return int(waiting[0]["queue_pos"]) + 1

    second_last = waiting[-2]
    last = waiting[-1]
    a = int(second_last["queue_pos"])
    b = int(last["queue_pos"])
    if b > a + 1:
        return a + 1
    # No integer gap — push everyone at/after last back by 1, then take a+1.
    conn.execute(
        """
        UPDATE now_serving_tickets
           SET queue_pos = queue_pos + 1
         WHERE window = ?
           AND status = 'waiting'
           AND queue_pos >= ?
        """,
        (dest_window, b),
    )
    return a + 1


def transfer_to_other_window(ticket_id: int, db_path: Optional[str] = None) -> dict[str, Any]:
    """Move a serving ticket onto the other window as second-to-last (penalty).

    Preferring another window is not rewarded — they skip only the very last
    person in that line (or go last if the line has 0–1 waiters).
    """
    tid = int(ticket_id)
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        _ensure_state(conn)
        row = conn.execute(
            """
            SELECT id, number, window, status, queue_pos, created_at
              FROM now_serving_tickets WHERE id = ?
            """,
            (tid,),
        ).fetchone()
        if row is None:
            raise NowServingError("Ticket not found")
        if row["status"] != "serving":
            raise NowServingError("Only a currently serving ticket can be transferred")
        src_window = int(row["window"])
        dest = other_window(src_window)
        new_pos = _queue_pos_second_to_last(conn, dest)
        conn.execute(
            """
            UPDATE now_serving_tickets
               SET window = ?, status = 'waiting', queue_pos = ?, transferred = 1
             WHERE id = ?
            """,
            (dest, new_pos, tid),
        )
        # Dest just got heavier — pull never-transferred waiters toward the lighter side.
        _rebalance_waiting_locked(conn)
        _bump_event_seq(conn)
        updated = conn.execute(
            """
            SELECT id, number, window, status, queue_pos, created_at
              FROM now_serving_tickets WHERE id = ?
            """,
            (tid,),
        ).fetchone()
        ticket = _row_to_ticket(updated)
        assert ticket is not None
        ticket["now_serving"] = _serving_labels(conn)
        conn.commit()
        return ticket
    except NowServingError:
        conn.rollback()
        raise
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def reset_day(db_path: Optional[str] = None) -> dict[str, Any]:
    """Admin end-of-day: clear all tickets; numbering restarts at 1, turn at window 1."""
    conn = get_connection(db_path)
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("DELETE FROM now_serving_tickets")
        conn.execute(
            """
            INSERT INTO now_serving_state (id, next_number, assign_turn, transfer_counter, event_seq)
            VALUES (1, 1, 1, 0, 0)
            ON CONFLICT(id) DO UPDATE SET
              next_number = 1,
              assign_turn = 1,
              transfer_counter = 0,
              event_seq = now_serving_state.event_seq + 1
            """
        )
        conn.commit()
        return {"success": True, "next_number": 1, "assign_turn": 1}
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def board_snapshot(db_path: Optional[str] = None) -> dict[str, Any]:
    """Read-only snapshot for kiosk Now Serving strip + staff board."""
    conn = get_connection(db_path)
    try:
        _ensure_state(conn)
        conn.commit()
        windows: dict[str, Any] = {}
        for w in WINDOWS:
            serving = conn.execute(
                """
                SELECT id, number, window, status, queue_pos, created_at,
                       COALESCE(transferred, 0) AS transferred
                  FROM now_serving_tickets
                 WHERE window = ? AND status = 'serving'
                 ORDER BY id DESC LIMIT 1
                """,
                (w,),
            ).fetchone()
            waiting_n = conn.execute(
                """
                SELECT COUNT(*) AS n FROM now_serving_tickets
                 WHERE window = ? AND status = 'waiting'
                """,
                (w,),
            ).fetchone()
            waiting_rows = conn.execute(
                """
                SELECT id, number, window, status, queue_pos, created_at,
                       COALESCE(transferred, 0) AS transferred
                  FROM now_serving_tickets
                 WHERE window = ? AND status = 'waiting'
                 ORDER BY queue_pos ASC, id ASC
                """,
                (w,),
            ).fetchall()
            windows[str(w)] = {
                "window": w,
                "serving": _row_to_ticket(serving),
                "waiting_count": int(waiting_n["n"] if waiting_n else 0),
                "waiting": [_row_to_ticket(r) for r in waiting_rows],
            }
        state = conn.execute(
            "SELECT next_number, assign_turn, transfer_counter, event_seq "
            "FROM now_serving_state WHERE id = 1"
        ).fetchone()
        return {
            "windows": windows,
            "now_serving": {
                "1": windows["1"]["serving"]["label"] if windows["1"]["serving"] else None,
                "2": windows["2"]["serving"]["label"] if windows["2"]["serving"] else None,
            },
            "revision": int(state["event_seq"]) if state and state["event_seq"] is not None else 0,
            "state": {
                "next_number": int(state["next_number"]) if state else 1,
                "assign_turn": int(state["assign_turn"]) if state else 1,
                "transfer_counter": int(state["transfer_counter"]) if state else 0,
                "event_seq": int(state["event_seq"]) if state and state["event_seq"] is not None else 0,
            },
        }
    finally:
        conn.close()


def render_now_serving_zpl(*, label: str, window: int | None = None) -> str:
    """Take-a-number stub on the same 2.625x1 @ 203dpi stock as Filetrack LOG labels.

    Window is intentionally omitted — transfers mean the window is only known
    when the guest is called. ``window`` is accepted for call-site compat only.
    """
    from filetrack.labels.geometry import enforce_label_geometry

    safe_label = "".join(c for c in str(label) if c.isalnum() or c in "-_")[:16]
    # Same office name as mass-email templates; keep ZPL-safe (no ^).
    company = "Xcel Financial Services, LLC"
    raw = (
        "^XA\n"
        "^FX Take-a-number stub — same geometry as LOG labels (2.625x1 @ 203dpi).^FS\n"
        "^CI28\n"
        "^FWN\n"
        "^PW532\n"
        "^LL203\n"
        "^LS0\n"
        "^LH0,0\n"
        "^LT0\n"
        "^MMT\n"
        "^PQ1,0,1,Y\n"
        "\n"
        "^CF0,20\n"
        f"^FO24,18^FD{company}^FS\n"
        "^FO24,48^GB484,2,2^FS\n"
        "^CF0,72\n"
        f"^FO24,70^FD{safe_label}^FS\n"
        "^XZ\n"
    )
    return enforce_label_geometry(raw)


def try_print_now_serving_ticket(label: str, window: int | None = None) -> bool:
    """Print ticket stub via the same Filetrack print path (relay or local).

    Never raises — print failure must not block ticket issuance. Confirm with
    Moises before pointing at a live-traffic printer. Window is not printed.
    """
    import logging

    log = logging.getLogger("now_serving")
    zpl = render_now_serving_zpl(label=label, window=window)
    try:
        from filetrack.config import FILETRACK_ENABLED, FILETRACK_PRINT_MODE

        if not FILETRACK_ENABLED:
            log.info("now_serving print skipped — FILETRACK_ENABLED is false")
            return False

        mode = (FILETRACK_PRINT_MODE or "local").strip().lower()
        if mode == "relay":
            return _print_zpl_via_relay(zpl)
        from filetrack.labels.printer import send_zpl

        send_zpl(zpl)
        return True
    except Exception as exc:
        log.warning("now_serving print failed for %s window %s: %s", label, window, exc)
        return False


def _print_zpl_via_relay(zpl: str) -> bool:
    """Reuse Filetrack relay URL/token; POST raw ZPL (supported by relay server)."""
    import requests
    from filetrack.config import (
        FILETRACK_RELAY_TIMEOUT_SEC,
        FILETRACK_RELAY_TOKEN,
        FILETRACK_RELAY_URL,
    )
    from filetrack.labels.relay_client import RelayError, _classify_network_error

    url = FILETRACK_RELAY_URL
    headers = {"Content-Type": "application/json"}
    if FILETRACK_RELAY_TOKEN:
        headers["X-Filetrack-Token"] = FILETRACK_RELAY_TOKEN
    try:
        resp = requests.post(
            url,
            json={"zpl": zpl},
            headers=headers,
            timeout=FILETRACK_RELAY_TIMEOUT_SEC,
        )
    except Exception as exc:
        raise RelayError(_classify_network_error(exc, url)) from exc
    if resp.status_code != 200:
        raise RelayError(
            f"print relay at {url!r} rejected job (status={resp.status_code}): {resp.text[:200]}"
        )
    return True
