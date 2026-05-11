"""Tests for chat_training_io (merge / export helpers)."""

from __future__ import annotations

import json
from pathlib import Path

import chat_training_io as ctio


def test_read_jsonl_latest_collapses_same_question(tmp_path: Path) -> None:
    p = tmp_path / "l.jsonl"
    p.write_text(
        json.dumps(
            {
                "question": "Hello there?",
                "normalized": "ignore",
                "year": 2025,
                "tool_used": "get_returns_by_status",
            }
        )
        + "\n"
        + json.dumps(
            {
                "question": "HELLO there?",
                "normalized": "",
                "year": 2026,
                "scope_classifier_blocked": True,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    m = ctio.read_jsonl_latest_by_normalized_question(p)
    assert len(m) == 1
    row = next(iter(m.values()))
    assert row.get("scope_classifier_blocked") is True


def test_tsv_appends_skips_already_in_examples(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    log.write_text(
        json.dumps(
            {
                "question": "balance due?",
                "tool_used": "get_balance_due_returns",
                "scope_classifier_blocked": False,
            }
        )
        + "\n",
        encoding="utf-8",
    )
    tsv = tmp_path / "examples.tsv"
    tsv.write_text(
        "label\tquestion\nin_scope\tBalance due?\n",
        encoding="utf-8",
    )
    norms = ctio.load_existing_example_norms(tsv)
    high, amb = ctio.tsv_appends_for_log(log, set(norms))
    assert high == []
    assert amb == []


def test_infer_scope_training_labels_classifier_fallback_without_tool(tmp_path: Path) -> None:
    lab, note = ctio.infer_scope_training_label(
        {
            "classified_intent": "fallback",
            "tool_used": None,
            "scope_classifier_blocked": False,
        }
    )
    assert lab is None
    assert note == "classifier_fallback_no_tool"


def test_infer_scope_training_fallback_with_tool_remains_in_scope() -> None:
    lab, _note = ctio.infer_scope_training_label(
        {
            "classified_intent": "fallback",
            "tool_used": "search_clients",
            "scope_classifier_blocked": False,
        }
    )
    assert lab == "in_scope"


def test_tsv_appends_adds_gate_and_tools(tmp_path: Path) -> None:
    log = tmp_path / "log.jsonl"
    log.write_text(
        "\n".join(
            json.dumps(x)
            for x in (
                {"question": "stock market ?", "tool_used": None},
                {"question": "How many PROCESSING ?", "tool_used": "get_returns_by_status"},
            )
        )
        + "\n",
        encoding="utf-8",
    )
    high, amb = ctio.tsv_appends_for_log(log, set())
    assert any(lab == "in_scope" and "PROCESSING" in q for lab, q in high)
    assert amb and amb[0][0].startswith("#REVIEW:")
