"""M2 — pure tests for filetrack.listener.parser.classify(). No I/O."""
from __future__ import annotations

from filetrack.listener.parser import (
    RECORD_KIND_LOG,
    RECORD_KIND_STATUS,
    RECORD_KIND_UNKNOWN,
    classify,
)


def test_classify_log_record():
    assert classify("LOG:00123") == (RECORD_KIND_LOG, "00123")


def test_classify_status_record():
    assert classify("STATUS:FINALIZE") == (RECORD_KIND_STATUS, "FINALIZE")


def test_classify_strips_confirmed_suffix():
    assert classify("LOG:00123\r\n", suffix="\r\n") == (RECORD_KIND_LOG, "00123")
    assert classify("LOG:00123\n", suffix="\n") == (RECORD_KIND_LOG, "00123")


def test_classify_tolerant_of_wrong_suffix_guess():
    # Caller guesses suffix="\n" but the scanner actually sent "\r\n" —
    # general whitespace stripping still recovers the correct value.
    assert classify("LOG:00123\r\n", suffix="\n") == (RECORD_KIND_LOG, "00123")


def test_classify_tolerant_of_surrounding_whitespace():
    assert classify("  LOG:00123  ") == (RECORD_KIND_LOG, "00123")
    assert classify("\tSTATUS:HOLD\t") == (RECORD_KIND_STATUS, "HOLD")


def test_classify_unknown_prefix():
    kind, value = classify("FOO:BAR")
    assert kind == RECORD_KIND_UNKNOWN
    assert value == "FOO:BAR"


def test_classify_no_delimiter_at_all():
    kind, value = classify("just some text")
    assert kind == RECORD_KIND_UNKNOWN
    assert value == "just some text"


def test_classify_empty_after_stripping():
    assert classify("\r\n") == (RECORD_KIND_UNKNOWN, "")
    assert classify("") == (RECORD_KIND_UNKNOWN, "")


def test_classify_none_input_never_raises():
    kind, value = classify(None)
    assert kind == RECORD_KIND_UNKNOWN


def test_classify_status_name_with_space_survives():
    # ALLOWED_STATUSES includes multi-word names like "PENDING INTAKE" —
    # classify() must not mangle the value on a space.
    assert classify("STATUS:PENDING INTAKE") == (RECORD_KIND_STATUS, "PENDING INTAKE")


def test_classify_log_value_with_extra_colon_or_hyphen():
    # Value is everything after the FIRST colon — a stress-test payload like
    # LOG:00123-A must not get truncated at a hyphen.
    assert classify("LOG:00123-A") == (RECORD_KIND_LOG, "00123-A")
