"""M2 — pure tests for filetrack.listener.state.StatusState (the sticky logic)."""
from __future__ import annotations

from filetrack.listener.parser import RECORD_KIND_LOG, RECORD_KIND_STATUS, RECORD_KIND_UNKNOWN
from filetrack.listener.state import Assignment, Rejection, StatusState


def test_starts_with_no_active_status():
    s = StatusState()
    assert s.active_status is None


def test_log_before_any_status_is_rejected_not_defaulted():
    s = StatusState()
    outcome = s.apply(RECORD_KIND_LOG, "00123")
    assert isinstance(outcome, Rejection)
    assert s.active_status is None  # never silently defaulted


def test_status_then_multiple_logs_is_sticky_batch():
    s = StatusState()
    assert s.apply(RECORD_KIND_STATUS, "FINALIZE") is None
    assert s.active_status == "FINALIZE"

    a1 = s.apply(RECORD_KIND_LOG, "00001")
    a2 = s.apply(RECORD_KIND_LOG, "00002")
    a3 = s.apply(RECORD_KIND_LOG, "00003")
    for a in (a1, a2, a3):
        assert isinstance(a, Assignment)
        assert a.status == "FINALIZE"
    assert [a.log_number for a in (a1, a2, a3)] == ["00001", "00002", "00003"]
    # Sticky — active_status persists, was never consumed by the assignments.
    assert s.active_status == "FINALIZE"


def test_status_change_mid_stream_switches_subsequent_assignments():
    s = StatusState()
    s.apply(RECORD_KIND_STATUS, "PROCESSING")
    a1 = s.apply(RECORD_KIND_LOG, "1")
    s.apply(RECORD_KIND_STATUS, "HOLD")
    a2 = s.apply(RECORD_KIND_LOG, "2")
    assert a1.status == "PROCESSING"
    assert a2.status == "HOLD"


def test_unknown_status_name_rejected_and_leaves_prior_state():
    s = StatusState()
    s.apply(RECORD_KIND_STATUS, "PROCESSING")
    outcome = s.apply(RECORD_KIND_STATUS, "NOT_A_REAL_STATUS")
    assert isinstance(outcome, Rejection)
    assert s.active_status == "PROCESSING"  # unchanged, not cleared


def test_unknown_prefix_rejected_never_crashes():
    s = StatusState()
    s.apply(RECORD_KIND_STATUS, "PROCESSING")
    outcome = s.apply(RECORD_KIND_UNKNOWN, "garbage")
    assert isinstance(outcome, Rejection)
    # Does not disturb active_status or crash the subsequent apply() call.
    a = s.apply(RECORD_KIND_LOG, "5")
    assert isinstance(a, Assignment)
    assert a.status == "PROCESSING"


def test_custom_allowed_statuses_override():
    s = StatusState(allowed_statuses=("CUSTOM_A", "CUSTOM_B"))
    assert isinstance(s.apply(RECORD_KIND_STATUS, "PROCESSING"), Rejection)  # not in custom set
    assert s.apply(RECORD_KIND_STATUS, "CUSTOM_A") is None
    assert s.active_status == "CUSTOM_A"
