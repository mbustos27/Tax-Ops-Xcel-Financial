"""chat_training_log — optional JSONL capture for scope-classifier training."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from chat_training_log import append_staff_ai_chat_question


def test_append_writes_jsonl(tmp_path):
    p = tmp_path / "q.jsonl"
    append_staff_ai_chat_question(
        question="Who is earliest log out?",
        normalized="who is earliest log out?",
        year=2026,
        classified_intent="count_by_status",
        response_payload={
            "answer": "…",
            "tool_used": "get_returns_by_status",
            "fast": True,
        },
        scope_classifier_blocked=False,
        from_answer_cache_hit=False,
        log_enable=True,
        log_include_cache_hits=False,
        log_path=p,
    )
    raw = p.read_text(encoding="utf-8").strip()
    obj = json.loads(raw)
    assert obj["tool_used"] == "get_returns_by_status"
    assert obj["classified_intent"] == "count_by_status"
    assert obj["scope_classifier_blocked"] is False


def test_append_merges_telemetry_into_jsonl(tmp_path):
    p = tmp_path / "q.jsonl"
    append_staff_ai_chat_question(
        question="telemetry test",
        normalized="telemetry test",
        year=2026,
        classified_intent="fallback",
        response_payload={"answer": ".", "tool_used": None},
        scope_classifier_blocked=False,
        from_answer_cache_hit=False,
        log_enable=True,
        log_include_cache_hits=False,
        log_path=p,
        telemetry={"router_mode": "aggregates", "router_confidence": 0.9},
    )
    obj = json.loads(p.read_text(encoding="utf-8").strip())
    assert obj["router_mode"] == "aggregates"
    assert pytest.approx(float(obj["router_confidence"]), rel=1e-3) == 0.9


def test_skip_when_disabled(tmp_path):
    p = tmp_path / "q.jsonl"
    append_staff_ai_chat_question(
        question="x",
        normalized="x",
        year=2026,
        classified_intent="fallback",
        response_payload={},
        scope_classifier_blocked=False,
        from_answer_cache_hit=False,
        log_enable=False,
        log_include_cache_hits=False,
        log_path=p,
    )
    assert not Path(p).exists()


def test_skip_cache_hit_by_default(tmp_path):
    p = tmp_path / "q.jsonl"
    append_staff_ai_chat_question(
        question="same",
        normalized="same",
        year=2026,
        classified_intent="fallback",
        response_payload={"cached": True},
        scope_classifier_blocked=False,
        from_answer_cache_hit=True,
        log_enable=True,
        log_include_cache_hits=False,
        log_path=p,
    )
    assert not p.exists()


def test_writes_cache_hit_when_flag(tmp_path):
    p = tmp_path / "q.jsonl"
    append_staff_ai_chat_question(
        question="same",
        normalized="same",
        year=2026,
        classified_intent="fallback",
        response_payload={"cached": True, "tool_used": "x"},
        scope_classifier_blocked=False,
        from_answer_cache_hit=True,
        log_enable=True,
        log_include_cache_hits=True,
        log_path=p,
    )
    assert json.loads(p.read_text(encoding="utf-8"))["from_cache"] is True
