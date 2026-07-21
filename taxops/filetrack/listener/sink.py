"""filetrack.listener.sink — where confirmed Assignments go.

An AssignmentSink is deliberately the ONLY seam between filetrack.listener
and the outside world. M2 ships two implementations that keep the listener
fully testable standalone (JsonlSink) or pluggable (CallbackSink). M3 wires
a CallbackSink whose callback POSTs to TaxOps's /filetrack/status endpoint
— this module itself still never imports anything TaxOps-specific.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Callable, Protocol

logger = logging.getLogger(__name__)


class AssignmentSink(Protocol):
    def submit(self, log_number: str, status: str, ts: str) -> None:
        """Record one (log_number, status) assignment observed at `ts`
        (ISO-8601 string, supplied by the caller — sinks do no clock reads
        of their own, keeping them trivially testable with fixed timestamps)."""
        ...


class JsonlSink:
    """Append-only, dependency-free sink for standalone testing/dry-runs —
    one JSON object per line: {"log_number", "status", "ts"}."""

    def __init__(self, path: str | Path = "assignments.jsonl"):
        self.path = Path(path)

    def submit(self, log_number: str, status: str, ts: str) -> None:
        record = {"log_number": log_number, "status": status, "ts": ts}
        with self.path.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(record) + "\n")
        logger.info("JsonlSink: recorded %s", record)


class CallbackSink:
    """Wraps an injected `callback(log_number, status, ts)` function. M2's
    tests inject a plain list-appending callback; M3 injects a function that
    POSTs to TaxOps's /filetrack/status with retry/backoff (see
    filetrack.service or the M3 wiring module — never imported from here)."""

    def __init__(self, callback: Callable[[str, str, str], None]):
        self._callback = callback

    def submit(self, log_number: str, status: str, ts: str) -> None:
        self._callback(log_number, status, ts)
