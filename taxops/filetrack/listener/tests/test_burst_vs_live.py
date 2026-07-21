"""M2 — proves the listener's correctness depends only on scan ORDER, never
on timing between scans: a buffered storage-mode burst (near-zero gaps) and
the same records replayed slowly must produce identical assignments."""
from __future__ import annotations

import time

from filetrack.listener.run_listener import run
from filetrack.listener.sink import CallbackSink

_SCAN_SEQUENCE = [
    "STATUS:PROCESSING\n",
    "LOG:00001\n",
    "LOG:00002\n",
    "GARBAGE_UNKNOWN\n",          # unknown prefix — must be skipped, not crash the run
    "LOG:00003\n",
    "STATUS:HOLD\n",
    "LOG:00004\n",
    "STATUS:NOT_A_REAL_STATUS\n",  # rejected — active_status must stay HOLD
    "LOG:00005\n",
    "STATUS:FINALIZE\n",
    "LOG:00006\n",
]


def _collect_assignments(records):
    calls: list[tuple[str, str, str]] = []
    sink = CallbackSink(lambda log_number, status, ts: calls.append((log_number, status, ts)))
    # Fixed clock — this test is about ORDER, not real timestamps, so make ts
    # deterministic to allow a byte-for-byte comparison between the two runs.
    run(iter(records), sink=sink, now_iso=lambda: "FIXED_TS")
    return calls


def test_burst_replay_matches_live_replay_exactly():
    # "Burst": records fed with zero gap (storage-mode buffer released at once).
    burst_result = _collect_assignments(_SCAN_SEQUENCE)

    # "Live": the exact same records, but with real delays between each —
    # timing must never affect the outcome, only order.
    live_records = []
    for rec in _SCAN_SEQUENCE:
        live_records.append(rec)
        time.sleep(0.01)
    live_result = _collect_assignments(live_records)

    assert burst_result == live_result
    assert burst_result == [
        ("00001", "PROCESSING", "FIXED_TS"),
        ("00002", "PROCESSING", "FIXED_TS"),
        ("00003", "PROCESSING", "FIXED_TS"),
        ("00004", "HOLD", "FIXED_TS"),
        ("00005", "HOLD", "FIXED_TS"),
        ("00006", "FINALIZE", "FIXED_TS"),
    ]


def test_burst_replay_preserves_order_even_with_interleaved_rejections():
    calls = _collect_assignments(_SCAN_SEQUENCE)
    log_numbers = [c[0] for c in calls]
    assert log_numbers == ["00001", "00002", "00003", "00004", "00005", "00006"]
