"""M2 — tests for filetrack.listener.sink implementations."""
from __future__ import annotations

import json

from filetrack.listener.sink import CallbackSink, JsonlSink


def test_jsonl_sink_appends_one_json_line_per_submit(tmp_path):
    path = tmp_path / "assignments.jsonl"
    sink = JsonlSink(path)
    sink.submit("00001", "FINALIZE", "2026-01-01T00:00:00+00:00")
    sink.submit("00002", "HOLD", "2026-01-01T00:00:05+00:00")

    lines = path.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2
    r1 = json.loads(lines[0])
    r2 = json.loads(lines[1])
    assert r1 == {"log_number": "00001", "status": "FINALIZE", "ts": "2026-01-01T00:00:00+00:00"}
    assert r2["log_number"] == "00002"


def test_callback_sink_invokes_injected_function():
    calls = []
    sink = CallbackSink(lambda log_number, status, ts: calls.append((log_number, status, ts)))
    sink.submit("00099", "PICKUP", "2026-02-02T00:00:00+00:00")
    assert calls == [("00099", "PICKUP", "2026-02-02T00:00:00+00:00")]
