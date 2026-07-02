"""get_system_context aggregate helper (LLM briefing; no schema changes)."""

from __future__ import annotations

import sqlite3

import pytest

from db import init_db
import db_tools as dt


@pytest.fixture()
def seeded_conn():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (1, 'Smith', 'A', NULL),
               (2, 'Jones', 'B', NULL),
               (3, 'Lee',   'C', NULL);

        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor,
            verified, client_status, intake_date
        ) VALUES
            (101, 1, '1', 2025, 'Maria Garcia', 0, 'PROCESSING', '2026-03-01'),
            (102, 2, '2', 2025, 'Maria Garcia', 0, 'PROCESSING', '2026-03-05'),
            (103, 3, '3', 2025, 'Lee Tran',      0, 'HOLD',       '2026-04-01');

        INSERT INTO payments (return_id, total_fee, fee_paid)
        VALUES (103, 150, 75);
        """
    )
    conn.commit()
    yield conn
    conn.close()


def test_get_system_context_counts_and_balances(seeded_conn):
    ctx = dt.get_system_context(seeded_conn, year=2026)
    assert ctx["season_year"] == 2026
    sc = ctx["status_counts"]
    assert sc.get("PROCESSING") == 2
    assert sc.get("HOLD") == 1
    pmap = ctx["processor_return_counts"]
    assert pmap.get("Maria Garcia") == 2
    assert pmap.get("Lee Tran") == 1
    assert ctx["balance_due_season_total"] == 1
    assert "today_local_iso" in ctx


def test_system_context_only_aggregate_keys(seeded_conn):
    ctx = dt.get_system_context(seeded_conn, year=2026)
    allowed = frozenset(
        (
            "today_local_iso",
            "season_year",
            "status_counts",
            "processor_return_counts",
            "balance_due_season_total",
            "dataplane_compact",
        )
    )
    assert frozenset(ctx.keys()) <= allowed


def test_get_system_context_dataplane_compact_shape(seeded_conn):
    ctx = dt.get_system_context(seeded_conn, year=2026)
    dpc = ctx.get("dataplane_compact")
    assert isinstance(dpc, dict)
    assert dpc.get("y") == 2026
    assert "forms" in dpc
    assert int((dpc.get("forms") or {}).get("n") or 0) == 3


def test_gather_season_dataplane_survives_dropped_optional_tables(seeded_conn):
    seeded_conn.executescript(
        """
        DROP TABLE IF EXISTS extraction_queue;
        DROP TABLE IF EXISTS email_classifications;
        """
    )
    seeded_conn.commit()
    dp = dt.gather_season_dataplane(seeded_conn, 2026)
    assert dp["extraction_by_status"] == []
    assert dp["email_classifications_year"] == []
    assert int((dp.get("forms") or {}).get("returns_in_season") or 0) == 3


def test_compact_llm_dataplane_trims_histograms(seeded_conn):
    raw = dt.gather_season_dataplane(seeded_conn, 2026)
    slim = dt.compact_llm_dataplane(raw, hist_cap=2)
    assert isinstance(slim.get("drake"), list)
    assert len(slim["drake"]) <= 2


def test_count_returns_with_positive_refund_in_season(seeded_conn):
    seeded_conn.execute("UPDATE payments SET refund_amount = 200 WHERE return_id = 103")
    seeded_conn.commit()
    assert dt.count_returns_with_positive_refund_in_season(seeded_conn, 2026) == 1


def test_system_context_matches_season_clause_no_rows():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    ctx = dt.get_system_context(conn, year=2026)
    assert ctx["status_counts"] == {}
    assert ctx["processor_return_counts"] == {}
    assert ctx["balance_due_season_total"] == 0
    conn.close()
