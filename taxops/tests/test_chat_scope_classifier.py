"""Tests for chat scope gate (sklearn optional at runtime; dev dep for training)."""

from __future__ import annotations

import os

import pytest

import chat_scope_classifier as csc

pytest.importorskip("sklearn")


@pytest.fixture(autouse=True)
def reset_scope_model():
    csc._model = None
    yield
    csc._model = None


def test_no_pkl_allows_router(tmp_path, monkeypatch):
    monkeypatch.setattr(csc, "MODEL_PATH", str(tmp_path / "missing_chat_scope.pkl"))
    assert csc.should_block_tool_router_llm("test the llm") is False


def test_blankish_blocked_without_model(tmp_path, monkeypatch):
    monkeypatch.setattr(csc, "MODEL_PATH", str(tmp_path / "missing_chat_scope.pkl"))
    assert csc.should_block_tool_router_llm(" ") is True
    assert csc.should_block_tool_router_llm("a") is True


def test_login_analytics_blocked_without_pkl(tmp_path, monkeypatch):
    monkeypatch.setattr(csc, "MODEL_PATH", str(tmp_path / "missing_chat_scope.pkl"))
    assert csc.should_block_tool_router_llm(
        "How many people were logged in in march?"
    ) is True


def test_log_out_workflow_language_not_blocked_by_login_rule(tmp_path, monkeypatch):
    monkeypatch.setattr(csc, "MODEL_PATH", str(tmp_path / "missing_chat_scope.pkl"))
    assert csc.should_block_tool_router_llm(
        "how many returns are in LOG OUT status this season"
    ) is False


def test_train_and_gate_off_topic(tmp_path, monkeypatch):
    tsv = tmp_path / "examples.tsv"
    pkl = tmp_path / "scope.pkl"
    lines = ["label\tquestion"]
    for i in range(15):
        lines.append(f"out_of_scope\tgibberish test fluff {i} nonsense")
    for i in range(15):
        lines.append(
            f"in_scope\thow many returns in PROCESSING for season {i} headcount"
        )
    tsv.write_text("\n".join(lines), encoding="utf-8")
    monkeypatch.setattr(csc, "MODEL_PATH", str(pkl))
    monkeypatch.setenv("CHAT_SCOPE_BLOCK_MIN_PROBA", "0.55")
    n = csc.train_and_save_from_tsv(str(tsv), str(pkl))
    assert n == 30
    csc._model = None
    assert csc.should_block_tool_router_llm("gibberish test fluff 3") is True
    assert csc.should_block_tool_router_llm("how many returns in PROCESSING") is False


def test_shipped_examples_pkl_if_present():
    if not os.path.isfile(csc.MODEL_PATH):
        pytest.skip("sklearn_chat_scope.pkl not in tree (run train_chat_scope_model.py)")
    csc._model = None
    assert csc.should_block_tool_router_llm("test the llm") is True
    assert csc.should_block_tool_router_llm("how many returns are in PROCESSING") is False


def test_examples_tsv_readable():
    assert csc.EXAMPLES_PATH.endswith("chat_scope_examples.tsv")
    row_count = 0
    with open(csc.EXAMPLES_PATH, encoding="utf-8") as fh:
        for i, line in enumerate(fh):
            if i == 0:
                assert "label" in line and "question" in line
                continue
            if line.strip():
                row_count += 1
    assert row_count >= 8
