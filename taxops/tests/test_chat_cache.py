"""Unit tests for chat_cache (no LLM, no external DB paths)."""

from __future__ import annotations

import hashlib
import sqlite3

import pytest

import chat_cache as cc
from db import init_db


@pytest.fixture(autouse=True)
def _cc_patch_get_connection_for_disk_cache(monkeypatch):
    """
    Patch chat_cache.get_connection() to ephemeral SQLite shared for the whole test,
    registering it in cc._DISK_CACHE_CONN_SKIP_CLOSE_IDS so disk cache code does not
    close it between calls.
    """
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
    """Avoid leaking in-memory aggregates between tests."""
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


def test_classify_intent_examples():
    intent, ents = cc.classify_intent("how many returns are in PROCESSING?")
    assert intent == "count_by_status"
    assert ents.get("status") == "PROCESSING"

    assert cc.classify_intent("who has an outstanding balance?")[0] == "balance_due"

    intent3, ents3 = cc.classify_intent("find the Martinez family")
    assert intent3 == "search_client"
    assert "martinez" in (ents3.get("query") or "").lower()

    intent4, ents4 = cc.classify_intent("what documents are missing for return 1149?")
    assert intent4 == "missing_docs"
    assert ents4.get("return_id") == 1149

    assert cc.classify_intent("how is the NASDAQ doing today?")[0] == "fallback"

def test_classify_season_totals_intent():
    assert cc.classify_intent("Total returns")[0] == "season_totals"
    assert cc.classify_intent("total returns ?")[0] == "season_totals"
    assert cc.classify_intent("how many returns")[0] == "season_totals"
    assert cc.classify_intent("how many returns are in PROCESSING?")[0] == "count_by_status"


def test_classify_returns_with_refund_volume():
    assert cc.classify_intent("Total returns with refunds")[0] == "returns_with_refund_count"
    assert cc.classify_intent("how many returns have a refund")[0] == "returns_with_refund_count"


def test_classify_intent_efiled_and_logged_out_maps_to_log_out_bucket():
    i, e = cc.classify_intent("How many returns have been fully efiled and logged out?")
    assert i == "count_by_status"
    assert e.get("status") == "LOG OUT"
    i2, e2 = cc.classify_intent("how many returns are logged out")
    assert i2 == "count_by_status"
    assert e2.get("status") == "LOG OUT"


def test_try_deterministic_efiled_logged_out_counts_log_out_status():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    i, ents = cc.classify_intent("How many returns have been fully efiled and logged out")
    resp = cc.try_deterministic_response(
        conn, i, ents, "How many returns have been fully efiled and logged out", 2026
    )
    assert resp is not None
    assert resp["tool_used"] is None
    assert resp["result_count"] == 0
    assert "LOG OUT" in resp["answer"]
    assert "EFILE READY" in resp["answer"]
    conn.close()


def test_try_deterministic_rejected_with_why_includes_code_breakdown():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (9, 'Doe', 'Jane', NULL);

        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor,
            verified, client_status, intake_date
        ) VALUES
            (901, 9, '99', 2025, 'Maria Garcia', 0,
             'REJECTED', '2026-03-10');

        INSERT INTO efile_batches (id, transmission_date, notes, status, created_at)
        VALUES (5501, '2026-03-11', '', 'closed', '2026-03-11T10:00:00');

        INSERT INTO efile_batch_items (
            id, batch_id, return_id, log_number, ack_status, created_at, rejection_code
        ) VALUES (
            8801, 5501, 901, '99', 'rejected', '2026-03-11T15:30:00', 'IND-031-04'
        );
        """
    )
    conn.commit()
    cc.refresh_chat_cache(conn=conn, year=2026)

    q = "how many rejects do we have currently and why are they rejected"
    assert cc.wants_rejection_reason_breakdown(q)
    i, ents = cc.classify_intent(q)
    assert i == "count_by_status"
    assert ents.get("status") == "REJECTED"

    resp = cc.try_deterministic_response(conn, i, ents, q, 2026)
    assert resp is not None
    assert resp["tool_used"] is None
    assert resp["result_count"] == 1
    assert "REJECTED" in resp["answer"]
    assert "IND-031-04" in resp["answer"]
    assert (
        "AGI" in resp["answer"]
        or "PIN" in resp["answer"]
        or "prior-year" in resp["answer"].lower()
    )
    conn.close()


def test_summarize_rejections_prefers_latest_row_that_has_rejection_code():
    """Newer e-file batch rows may clear `rejection_code`; rollup must not drop older codes."""
    import db_tools as dt

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (8, 'Roe', 'Pat', NULL);

        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor,
            verified, client_status, intake_date
        ) VALUES
            (902, 8, '98', 2025, 'Prep A', 0,
             'REJECTED', '2026-04-01');

        INSERT INTO efile_batches (id, transmission_date, notes, status, created_at)
        VALUES
            (6601, '2026-04-02', '', 'closed', '2026-04-02T09:00:00'),
            (6602, '2026-04-10', '', 'open', '2026-04-10T11:00:00');

        INSERT INTO efile_batch_items (
            id, batch_id, return_id, log_number, ack_status, created_at, rejection_code
        ) VALUES
            (7701, 6601, 902, '98', 'rejected', '2026-04-02T12:00:00', 'IND-031-04'),
            (7702, 6602, 902, '98', 'pending', '2026-04-10T12:00:00', '');
        """
    )
    conn.commit()

    summ = dt.summarize_season_rejections_for_chat(conn, 2026)
    assert summ["total"] == 1
    assert len(summ["by_code"]) >= 1
    assert summ["by_code"][0]["code"] == "IND-031-04"
    assert summ["by_code"][0]["count"] == 1
    conn.close()


def test_summarize_rejections_notes_when_no_irs_code_stored():
    import db_tools as dt

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    init_db(conn)
    conn.executescript(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (19, 'Noh', 'Code', NULL);

        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor,
            verified, client_status, intake_date
        ) VALUES
            (903, 19, '77', 2025, 'Prep B', 0,
             'REJECTED', '2026-04-12');

        INSERT INTO efile_batches (id, transmission_date, notes, status, created_at)
        VALUES (77001, '2026-04-12', '', 'closed', '2026-04-12T10:00:00');

        INSERT INTO efile_batch_items (
            id, batch_id, return_id, log_number, ack_status, created_at,
            rejection_code, rejection_reason
        ) VALUES (
            88099, 77001, 903, '77', 'rejected', '2026-04-12T16:00:00', '',
            'IRS ack: taxpayer prior-year Adjusted Gross Income mismatch on authentication fields — client must verify prior return.'
        );
        """
    )
    conn.commit()

    summ = dt.summarize_season_rejections_for_chat(conn, 2026)
    assert summ["total"] == 1
    assert any(int(x.get("count") or 0) == 1 for x in summ["by_code"])
    flat = "|".join(
        str(x.get("code", "")) + str(x.get("explanation", "")) for x in summ["by_code"]
    ).lower()
    assert "prior-year" in flat or "mismatch" in flat or "agi" in flat
    conn.close()


def test_classify_how_many_different_types_of_returns_maps_form_leader():
    i, ents = cc.classify_intent("How many different types of returns are in the system?")
    assert i == "dataplane_slice"
    assert ents.get("slice") == "form_leader"


def test_classify_how_many_different_returns_plain_is_season_totals_not_status_tool():
    i, ents = cc.classify_intent("How many different returns are in the system?")
    assert i == "season_totals"
    assert ents == {}


def test_classify_returns_in_taxops_is_season_totals():
    i, _ = cc.classify_intent("How many returns are there in TaxOps?")
    assert i == "season_totals"


def test_classify_returns_in_database_is_season_totals():
    i, _ = cc.classify_intent("What's the total number of returns in the database?")
    assert i == "season_totals"


def test_fallback_guard_drops_status_tool_when_question_never_names_that_status():
    t, args = cc.apply_row_tool_grounding_guard(
        "How many returns are in the database overall?",
        "fallback",
        {},
        "get_returns_by_status",
        {"status": "PROCESSING", "year": 2026},
    )
    assert t is None
    assert args["status"] == "PROCESSING"


def test_fallback_guard_keeps_status_when_question_names_it():
    t, args = cc.apply_row_tool_grounding_guard(
        "How many PROCESSING returns?",
        "fallback",
        {},
        "get_returns_by_status",
        {"status": "PROCESSING", "year": 2026},
    )
    assert t == "get_returns_by_status"
    assert args.get("status") == "PROCESSING"


def test_fallback_guard_disabled_when_intent_already_classified():
    t, _ = cc.apply_row_tool_grounding_guard(
        "How many returns are in PROCESSING?",
        "count_by_status",
        {"status": "PROCESSING"},
        "get_returns_by_status",
        {"status": "PROCESSING"},
    )
    assert t == "get_returns_by_status"


def test_fallback_guard_keeps_processor_when_name_in_question_even_if_regex_skips():
    t, _args = cc.apply_row_tool_grounding_guard(
        "how many returns does maria prepare",
        "fallback",
        {},
        "get_returns_by_processor",
        {"processor": "Maria", "year": 2026},
    )
    assert t == "get_returns_by_processor"


def test_snapshot_office_brief_digest_nonempty_after_refresh():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    text = cc.snapshot_office_brief_digest(2026, max_chars=90000)
    assert isinstance(text, str) and ("Season" in text or "season" in text.lower())


def test_normalize_chat_router_legacy_tool_pick():
    out = cc.normalize_chat_router_payload(
        {"tool": "search_clients", "args": {"query": "martinez"}}
    )
    assert out["router_mode"] == "need_rows"
    assert out["tool"] == "search_clients"
    assert out["router_confidence"] >= 0.5


def test_normalize_chat_router_clarify_prompt():
    out = cc.normalize_chat_router_payload(
        {"mode": "clarify", "confidence": 0.9, "tool": None, "clarify_prompt": "Which preparer?"}
    )
    assert out["router_mode"] == "clarify"
    assert "preparer" in out["clarify_prompt"].lower()


def test_classify_most_common_return_type_uses_form_dataplane_not_workflow_domination():
    intent, ents = cc.classify_intent("Most common type of return in the system")
    assert intent == "dataplane_slice"
    assert ents.get("slice") == "form_leader"


def test_classify_and_resolve_dominant_workflow_status():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    i, ents = cc.classify_intent("What's the most common workflow status right now")
    assert i == "dominant_status"
    resp = cc.try_deterministic_response(conn, i, ents, "most common", 2026)
    assert resp is not None
    assert resp["tool_used"] is None
    assert "PROCESSING" in resp["answer"]
    conn.close()


def test_try_deterministic_form_dataplane_slice():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    conn.execute(
        """
        INSERT INTO return_forms (
            return_id, form_1040, sched_c, form_1120, form_1120s,
            form_1065_llc, form_990_1041, corp_officer, business_owner
        ) VALUES
          (101, 1, 0, 0, 0, 0, 0, 0, 0),
          (102, 1, 1, 0, 0, 0, 0, 0, 0),
          (201, 0, 0, 0, 0, 0, 1, 0, 0);
        """
    )
    conn.commit()
    cc.refresh_chat_cache(conn=conn, year=2026)
    i, ents = cc.classify_intent("Most common kind of returns this season-wide")
    assert i == "dataplane_slice"
    assert ents.get("slice") == "form_leader"
    resp = cc.try_deterministic_response(conn, i, ents, "form mix", 2026)
    assert resp is not None
    assert "Form 1040" in resp["answer"] or "1040" in resp["answer"]
    conn.close()


def test_gather_season_dataplane_contains_sections():
    import db_tools as dt

    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    conn.commit()
    dp = dt.gather_season_dataplane(conn, 2026)
    for key in (
        "forms",
        "flags",
        "drake_histogram",
        "missing_docs",
        "fee_rollups",
        "payment_methods",
    ):
        assert key in dp
    conn.close()


def test_try_deterministic_returns_with_refund_count():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    conn.execute("UPDATE payments SET refund_amount = 950 WHERE return_id = 201")
    conn.commit()
    cc.refresh_chat_cache(conn=conn, year=2026)
    resp = cc.try_deterministic_response(
        conn,
        "returns_with_refund_count",
        {},
        "Total returns with refunds",
        2026,
    )
    assert resp is not None
    assert resp["result_count"] == 1
    assert resp["tool_used"] is None
    conn.close()


def test_classify_intent_balance_superlatives_route_to_balance_due_not_status():
    assert cc.balance_superlative_sort_mode("Highest Balance Client") == "high"
    assert cc.balance_superlative_sort_mode("Lowest Valance client") == "low"
    assert cc.classify_intent("Lowest Valance client")[0] == "balance_due"
    assert cc.classify_intent("Highest Balance Client")[0] == "balance_due"
    assert cc.classify_intent("Which client has the lowest balance still due?")[0] == "balance_due"
    assert cc.balance_superlative_sort_mode("Which client has the lowest balance still due?") == "low"
    intent_owes, _ = cc.classify_intent("Who owes the most right now")
    assert intent_owes == "balance_due"


def test_try_deterministic_season_totals_respects_live_aggregate():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    live = {
        "status_counts": {"PROCESSING": 50, "ZZZ": 3},
    }
    resp = cc.try_deterministic_response(
        conn,
        "season_totals",
        {},
        "Total returns",
        2026,
        office_ctx_live=live,
    )
    assert resp is not None
    assert resp["result_count"] == 53
    assert resp["tool_used"] is None
    assert "**53**" in resp["answer"]
    conn.close()


def test_try_deterministic_highest_unpaid_fee():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)

    resp = cc.try_deterministic_response(
        conn,
        "balance_due",
        {},
        "Highest balance client",
        2026,
    )
    assert resp is not None
    assert resp["tool_used"] == "get_balance_due_returns"
    assert resp["fast"] is True
    assert "Martinez" in resp["answer"] or "100" in resp["answer"].replace(",", "")
    assert "100.00" in resp["answer"] or "$100" in resp["answer"]

    conn.close()


def test_answer_cache_ttl_roundtrip():
    qn = cc.normalize_question("same question twice?")
    cc.set_cached_answer(
        qn,
        2025,
        {
            "answer": "instant answer from unit test",
            "tool_used": None,
            "args_used": None,
        },
    )
    got = cc.get_cached_answer(qn, 2025)
    assert got is not None
    assert got["cached"] is True
    assert got["answer"] == "instant answer from unit test"


def test_disk_cache_survives_memory_clear(monkeypatch):
    monkeypatch.setattr(cc, "AI_CHAT_DISK_CACHE_ENABLE", True)
    monkeypatch.setattr(cc, "AI_CHAT_DISK_CACHE_HOURS", 24.0)
    qn = cc.normalize_question("how many tests in PROCESSING for disk?")
    payload = {
        "answer": "There are 7 returns in PROCESSING.",
        "tool_used": None,
        "args_used": None,
    }
    cc.set_cached_answer(qn, 2025, payload)
    cc.ANSWER_CACHE.clear()
    got = cc.get_cached_answer(qn, 2025)
    assert got is not None
    assert got["cached"] is True
    assert got.get("persistent_hit") is True
    assert "7 returns" in got["answer"]


def test_disk_cache_skips_scope_fallback():
    qn = cc.normalize_question("off topic question for cache")
    cc.set_cached_answer(
        qn,
        2025,
        {
            "answer": (
                "I can only answer questions about returns, clients, balances, "
                "and missing documents in TaxOps."
            ),
            "tool_used": None,
            "args_used": None,
        },
    )
    cc.ANSWER_CACHE.clear()
    assert cc.get_cached_answer(qn, 2025) is None


def test_disk_cache_expired_row_deleted_on_read(monkeypatch):
    monkeypatch.setattr(cc, "AI_CHAT_DISK_CACHE_ENABLE", True)
    monkeypatch.setattr(cc, "AI_CHAT_DISK_CACHE_HOURS", 24.0)
    qn = cc.normalize_question("expired disk cache row")
    cc.set_cached_answer(
        qn,
        2025,
        {"answer": "old answer text here", "tool_used": "get_balance_due_returns"},
    )
    cc.ANSWER_CACHE.clear()
    k = cc.answer_cache_payload_key(qn, 2025)
    cx = cc.get_connection()
    cx.execute(
        "UPDATE ai_chat_common_answers SET expires_at = ? WHERE cache_key = ?",
        ("2000-01-01T00:00:00+00:00", k),
    )
    cx.commit()
    assert cc.get_cached_answer(qn, 2025) is None
    n = cx.execute(
        "SELECT COUNT(*) AS c FROM ai_chat_common_answers WHERE cache_key = ?",
        (k,),
    ).fetchone()["c"]
    assert n == 0


def test_refresh_chat_cache_counts_and_balance_total():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    conn.close()

    snap = cc._read_snapshot()
    assert snap["season_year"] == 2026
    assert snap["stats"]["status_counts"].get("PROCESSING") == 2
    assert snap["stats"]["status_counts"].get("HOLD") == 1
    assert snap["stats"]["processor_counts"].get("Maria Garcia") == 3
    assert snap["stats"]["balance_due_total"] >= 2


def test_try_deterministic_count_by_status():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)

    resp = cc.try_deterministic_response(
        conn,
        "count_by_status",
        {"status": "PROCESSING"},
        "how many returns are in PROCESSING?",
        2026,
    )
    assert resp is not None
    assert resp["result_count"] == 2
    assert resp["tool_used"] is None
    assert resp["args_used"] is None
    assert resp["fast"] is True
    conn.close()


def test_try_deterministic_status_count_prefers_live_aggregates():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)
    live = {"status_counts": {"PROCESSING": 99}}
    resp = cc.try_deterministic_response(
        conn,
        "count_by_status",
        {"status": "PROCESSING"},
        "how many returns are in PROCESSING?",
        2026,
        office_ctx_live=live,
    )
    assert resp is not None
    assert resp["result_count"] == 99
    assert resp["tool_used"] is None
    conn.close()


def test_try_deterministic_search_client():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON;")
    _seed_demo(conn)
    cc.refresh_chat_cache(conn=conn, year=2026)

    resp = cc.try_deterministic_response(
        conn,
        "search_client",
        {"query": "Martinez"},
        "find Martinez",
        2026,
    )
    assert resp is not None
    assert resp["tool_used"] == "search_clients"
    assert resp["result_count"] >= 1
    conn.close()


def test_normalize_question_stable():
    assert cc.normalize_question("  Hello THERE  ") == "hello there"


def test_answer_cache_key_versions_for_routing_invalidation():
    """Stale tool answers must miss after _CHAT_ANSWER_ROUTE_VERSION bumps."""
    norm = cc.normalize_question("Highest balance client")
    expected = hashlib.md5(
        f"{cc._CHAT_ANSWER_ROUTE_VERSION}:{norm}|2026".encode()
    ).hexdigest()
    assert cc.answer_cache_payload_key(norm, 2026) == expected


def test_wants_tool_row_aggregate_processing_stage_worded():
    q = "What returns do we have in the PROCESSING stage?"
    assert cc.wants_tool_row_aggregate(q)
    assert not cc.prefers_narrative_list_answer(q)
    assert not cc.use_narrative_return_rows(q)


def test_qualitative_who_earliest_skips_count_shortcircuit():
    q = "Who is the earliest log out in the system?"
    assert cc.wants_qualitative_return_answer(q)
    assert cc.use_narrative_return_rows(q)


def test_how_many_log_out_still_volume_not_narrative():
    q = "how many returns are in LOG OUT?"
    assert cc.wants_tool_row_aggregate(q)
    assert not cc.wants_qualitative_return_answer(q)
    assert not cc.use_narrative_return_rows(q)


def test_chronological_log_out_earliest():
    rows = [
        {"display_name": "Later", "log_number": "2", "logout_date": "2026-03-15T12:00:00", "ack_date": None},
        {"display_name": "Earlier", "log_number": "1", "logout_date": "2026-02-09", "ack_date": None},
    ]
    kw = {"status": "LOG OUT", "year": 2026}
    p = cc.chronological_superlative_chat_payload(
        "get_returns_by_status",
        kw,
        rows,
        "Who was the earliest log out?",
        2,
        2026,
    )
    assert p is not None
    assert "Earlier" in p["answer"]
    assert "2026-02-09" in p["answer"]
    assert p["fast"] is True


def test_chronological_log_out_prefers_logout_over_ack():
    rows = [
        {"display_name": "X", "log_number": "1", "logout_date": "2026-06-01", "ack_date": "2026-01-01"},
    ]
    kw = {"status": "LOG OUT", "year": 2026}
    p = cc.chronological_superlative_chat_payload(
        "get_returns_by_status",
        kw,
        rows,
        "latest log out by date?",
        1,
        2026,
    )
    assert p is not None
    assert "2026-06-01" in p["answer"]


def test_chronological_logs_out_when_no_dates():
    rows = [
        {"display_name": "A", "log_number": "1", "logout_date": None, "ack_date": None},
    ]
    kw = {"status": "LOG OUT", "year": 2026}
    p = cc.chronological_superlative_chat_payload(
        "get_returns_by_status",
        kw,
        rows,
        "Who is the earliest log out?",
        1,
        2026,
    )
    assert p is not None
    assert "cannot be determined" in p["answer"].lower()


def test_chronological_skips_non_terminal_status():
    assert (
        cc.chronological_superlative_chat_payload(
            "get_returns_by_status",
            {"status": "PROCESSING", "year": 2026},
            [{"display_name": "Z", "log_number": "1"}],
            "Who is the earliest?",
            1,
            2026,
        )
        is None
    )
