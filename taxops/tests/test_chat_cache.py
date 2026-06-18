"""Unit tests for chat_cache indexes + answer cache (no LLM, no external DB paths)."""

from __future__ import annotations

import sqlite3

import pytest

import chat_cache as cc
from db import init_db
from utils import normalize_staff_question_key


@pytest.fixture(autouse=True)
def _cc_patch_get_connection_for_disk_cache(monkeypatch):
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    cc._DISK_CACHE_CONN_SKIP_CLOSE_IDS.add(id(conn))
    monkeypatch.setattr(cc, "get_connection", lambda db_path=None: conn)
    yield
    cc._DISK_CACHE_CONN_SKIP_CLOSE_IDS.discard(id(conn))
    sqlite3.Connection.close(conn)


@pytest.fixture(autouse=True)
def _reset_chat_snapshot_between_tests():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    cc.refresh_chat_cache(conn=conn, year=3000)
    conn.close()
    cc.ANSWER_CACHE.clear()
    yield


def _seed_demo(conn: sqlite3.Connection) -> None:
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (1, 'Doe', 'Jane', NULL), (2, 'Martinez', 'Jose', 'Martinez, Jose');

        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor,
            verified, client_status, intake_date
        ) VALUES
            (101, 1, '1', 2025, 'Maria Garcia', 0,
             'PROCESSING', '2026-03-01'),
            (102, 1, '2', 2025, 'Maria Garcia', 0,
             'PROCESSING', '2026-03-05'),
            (201, 1, '3', 2025, 'Lucila Yanez', 0,
             'HOLD', '2026-04-01'),
            (302, 2, '4', 2025, 'Maria Garcia', 0,
             'PICKUP', '2026-05-01');

        INSERT INTO payments (return_id, total_fee, fee_paid)
        VALUES (201, 200, 150), (302, 100, 0);
        """
    )
    conn.commit()


def test_refresh_chat_cache_and_structured_build():
    conn = cc.get_connection()
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    cc.build_cache(None, 2026)
    assert cc.get_status_count("PROCESSING") == 2
    assert cc.get_status_count("HOLD") == 1
    fs = cc.get_financial_stats()
    assert fs.get("return_count", 0) >= 1
    assert "total_billed" in fs


def test_normalize_staff_question_key_contract():
    assert normalize_staff_question_key("  Hello THERE  ") == "hello there"
    assert "how is" in normalize_staff_question_key("How's workload for Maria?")


def test_memory_answer_cache_roundtrip():
    year = 2026
    q = normalize_staff_question_key("How many in PROCESSING?")
    body = {"answer": "2", "tool_used": "cache"}
    cc.set_cached_answer(q, year, body)
    hit = cc.get_cached_answer(q, year)
    assert hit and hit.get("answer") == "2"
