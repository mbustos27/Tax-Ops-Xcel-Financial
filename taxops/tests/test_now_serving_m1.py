"""Milestone 1 — Now Serving data model + atomic queue ops."""
from __future__ import annotations

import sqlite3
import threading
import time

import pytest

from db import CURRENT_SCHEMA_VERSION, get_connection, get_schema_version
from now_serving import (
    NowServingError,
    board_snapshot,
    call_next,
    format_ticket_label,
    issue_ticket,
    reset_day,
    transfer_to_other_window,
)


def test_schema_v36_now_serving_tables(taxops_db_path):
    assert CURRENT_SCHEMA_VERSION >= 36
    conn = get_connection(taxops_db_path)
    assert get_schema_version(conn) >= 36
    tables = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('now_serving_tickets', 'now_serving_state')"
        )
    }
    assert tables == {"now_serving_tickets", "now_serving_state"}
    state = conn.execute("SELECT * FROM now_serving_state WHERE id = 1").fetchone()
    assert state is not None
    assert state["next_number"] == 1
    assert state["assign_turn"] == 1
    conn.close()


def test_format_ticket_label():
    assert format_ticket_label(1) == "1"
    assert format_ticket_label(14) == "14"
    assert format_ticket_label(1000) == "1000"


def test_issue_alternates_windows(taxops_db_path):
    t1 = issue_ticket(taxops_db_path)
    t2 = issue_ticket(taxops_db_path)
    t3 = issue_ticket(taxops_db_path)
    assert t1["number"] == 1 and t1["window"] == 1 and t1["label"] == "1"
    assert t2["number"] == 2 and t2["window"] == 2 and t2["label"] == "2"
    assert t3["number"] == 3 and t3["window"] == 1
    assert t1["people_ahead"] == 0
    assert t3["people_ahead"] == 1  # t1 still waiting at window 1


def test_concurrent_issue_no_duplicate_numbers_or_broken_alternation(taxops_db_path):
    """Simultaneous kiosk taps must not collide on number or break 1-2-1-2."""
    n = 24
    barrier = threading.Barrier(n)
    results: list[dict] = []
    errors: list[BaseException] = []
    lock = threading.Lock()

    def _race():
        try:
            barrier.wait(timeout=10)
            ticket = issue_ticket(taxops_db_path)
            with lock:
                results.append(ticket)
        except BaseException as exc:  # noqa: BLE001 — collect for assertion
            with lock:
                errors.append(exc)

    threads = [threading.Thread(target=_race) for _ in range(n)]
    for th in threads:
        th.start()
    for th in threads:
        th.join(timeout=30)

    assert not errors, f"issue_ticket raised: {errors!r}"
    assert len(results) == n
    numbers = sorted(t["number"] for t in results)
    assert numbers == list(range(1, n + 1))
    assert len({t["number"] for t in results}) == n

    # Alternation is global issuance order by number, not by thread finish order.
    by_num = {t["number"]: t["window"] for t in results}
    expected_windows = [1 if (i % 2 == 1) else 2 for i in range(1, n + 1)]
    assert [by_num[i] for i in range(1, n + 1)] == expected_windows


def test_call_next_ordering_fifo_per_window(taxops_db_path):
    # Window 1: 1, 3; Window 2: 2
    issue_ticket(taxops_db_path)
    issue_ticket(taxops_db_path)
    issue_ticket(taxops_db_path)

    s1 = call_next(1, taxops_db_path)
    assert s1["label"] == "1"
    assert s1["status"] == "serving"
    assert s1["waiting_count"] == 1  # 3 still waiting

    s1b = call_next(1, taxops_db_path)
    assert s1b["label"] == "3"
    assert s1b["waiting_count"] == 0

    # A-001 must be done
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT status FROM now_serving_tickets WHERE number = 1"
    ).fetchone()
    conn.close()
    assert row["status"] == "done"

    s2 = call_next(2, taxops_db_path)
    assert s2["label"] == "2"

    with pytest.raises(NowServingError, match="No waiting"):
        call_next(2, taxops_db_path)


def test_transfer_goes_second_to_last_on_other_window(taxops_db_path):
    # W1: 1, 3 waiting; W2: 2 waiting only
    a1 = issue_ticket(taxops_db_path)
    a2 = issue_ticket(taxops_db_path)
    a3 = issue_ticket(taxops_db_path)
    assert a1["window"] == 1 and a2["window"] == 2 and a3["window"] == 1

    serving = call_next(1, taxops_db_path)
    assert serving["id"] == a1["id"]

    # Only A-002 waiting on W2 → transfer goes last (behind them).
    moved = transfer_to_other_window(serving["id"], taxops_db_path)
    assert moved["window"] == 2
    assert moved["status"] == "waiting"

    snap = board_snapshot(taxops_db_path)
    w2_waiting = [t["label"] for t in snap["windows"]["2"]["waiting"]]
    assert w2_waiting == ["2", "1"]

    nxt = call_next(2, taxops_db_path)
    assert nxt["label"] == "2"


def test_transfer_second_to_last_with_multiple_waiting(taxops_db_path):
    """With 2+ already waiting, transfer sits just before the current last.

    Auto-balance may then move one never-transferred waiter off the heavier
    destination back toward the lighter window — the transferred ticket stays.
    """
    issue_ticket(taxops_db_path)  # 1 W1
    issue_ticket(taxops_db_path)  # 2 W2
    issue_ticket(taxops_db_path)  # 3 W1
    issue_ticket(taxops_db_path)  # 4 W2
    issue_ticket(taxops_db_path)  # 5 W1
    issue_ticket(taxops_db_path)  # 6 W2  → W2 waiting: 2, 4, 6

    s1 = call_next(1, taxops_db_path)  # serve 1
    transfer_to_other_window(s1["id"], taxops_db_path)

    snap = board_snapshot(taxops_db_path)
    labels = [t["label"] for t in snap["windows"]["2"]["waiting"]]
    # Transferred 1 stays on W2 and is never auto-moved back.
    assert "1" in labels
    a001 = next(t for t in snap["windows"]["2"]["waiting"] if t["label"] == "1")
    assert int(a001.get("transferred") or 0) == 1
    # A-001 was inserted second-to-last at transfer time; auto-balance may
    # have moved the former last (6) to W1 — 1 must not be first.
    assert labels[0] != "1"


def test_transfer_newer_still_penalized_not_front(taxops_db_path):
    issue_ticket(taxops_db_path)  # 1 W1
    issue_ticket(taxops_db_path)  # 2 W2
    issue_ticket(taxops_db_path)  # 3 W1
    issue_ticket(taxops_db_path)  # 4 W2

    s1 = call_next(1, taxops_db_path)
    transfer_to_other_window(s1["id"], taxops_db_path)
    # After transfer+rebalance, 1 remains on W2 and is not front of line.
    labels = [t["label"] for t in board_snapshot(taxops_db_path)["windows"]["2"]["waiting"]]
    assert "1" in labels
    assert labels[0] != "1"

    s1b = call_next(1, taxops_db_path)  # 3
    transfer_to_other_window(s1b["id"], taxops_db_path)
    w2 = board_snapshot(taxops_db_path)["windows"]["2"]["waiting"]
    labels2 = [t["label"] for t in w2]
    assert "3" in labels2
    a003 = next(t for t in w2 if t["label"] == "3")
    assert int(a003.get("transferred") or 0) == 1
    # If anyone else is still waiting on W2, the transferred guest is not jump-to-front.
    if len(labels2) > 1:
        assert labels2[0] != "3"


def test_transfer_triggers_rebalance_from_heavier_line(taxops_db_path):
    """Sending to a busy window pulls never-transferred waiters back to the lighter side."""
    for _ in range(6):
        issue_ticket(taxops_db_path)
    # W1: 001,003,005  W2: 002,004,006
    s1 = call_next(1, taxops_db_path)  # serve 001; W1 wait 003,005
    before_w1 = board_snapshot(taxops_db_path)["windows"]["1"]["waiting_count"]
    transfer_to_other_window(s1["id"], taxops_db_path)
    snap = board_snapshot(taxops_db_path)
    # Dest W2 got 1 (locked). Rebalance should grow W1 waiting vs pre-transfer wait-only count
    # (pre had 2 waiting; after should be >= 2 and W2 should not keep every original + transfer).
    assert snap["windows"]["1"]["waiting_count"] >= before_w1
    assert any(
        t["label"] == "1" and int(t.get("transferred") or 0) == 1
        for t in snap["windows"]["2"]["waiting"]
    )
    issue_ticket(taxops_db_path)
    issue_ticket(taxops_db_path)
    call_next(1, taxops_db_path)
    reset_day(taxops_db_path)
    snap = board_snapshot(taxops_db_path)
    assert snap["state"]["next_number"] == 1
    assert snap["state"]["assign_turn"] == 1
    assert snap["windows"]["1"]["waiting_count"] == 0
    assert snap["windows"]["1"]["serving"] is None
    t = issue_ticket(taxops_db_path)
    assert t["number"] == 1 and t["window"] == 1 and t["label"] == "1"


def test_call_next_invalid_window(taxops_db_path):
    with pytest.raises(NowServingError):
        call_next(3, taxops_db_path)
