"""Duplicate pair classification: year-splits vs different people."""

from __future__ import annotations

from app import (
    _classify_duplicate_pair,
    _first_names_likely_duplicate,
)


def test_first_names_joint_vs_primary_match():
    assert _first_names_likely_duplicate("AHMED & HIND ABED", "AHMED")
    assert _first_names_likely_duplicate("JOSE & CLAUDIA", "JOSE")
    assert _first_names_likely_duplicate("BRYAN O", "BRYAN")


def test_first_names_rejects_substring_false_friends():
    # Old containment logic would match ANA inside DIANA / startswith junk
    assert not _first_names_likely_duplicate("DIANA", "ANA")
    assert not _first_names_likely_duplicate("TEST", "TEST2")
    assert not _first_names_likely_duplicate("JOSE", "JOSEPHINE")


def test_classify_year_split_is_high_confidence():
    keep = {
        "first_name": "MARIA",
        "ssn_last4": None,
        "year_logs": "2024:100",
    }
    discard = {
        "first_name": "MARIA",
        "ssn_last4": None,
        "year_logs": "2025:200",
    }
    meta = _classify_duplicate_pair(keep, discard)
    assert meta["confidence"] == "high"
    assert "year_split" in meta["flags"]
    assert meta["merge_allowed"] is True


def test_classify_ssn_conflict_blocked():
    keep = {"first_name": "JOHN", "ssn_last4": "1111", "year_logs": "2025:1"}
    discard = {"first_name": "JOHN", "ssn_last4": "2222", "year_logs": "2025:2"}
    meta = _classify_duplicate_pair(keep, discard)
    assert meta["confidence"] == "blocked"
    assert meta["merge_allowed"] is False


def test_classify_different_logs_same_year_caution():
    keep = {"first_name": "HUGO", "ssn_last4": None, "year_logs": "2025:10"}
    discard = {"first_name": "HUGO", "ssn_last4": None, "year_logs": "2025:99"}
    meta = _classify_duplicate_pair(keep, discard)
    assert meta["confidence"] == "caution"
    assert "different_logs_same_year" in meta["flags"]
