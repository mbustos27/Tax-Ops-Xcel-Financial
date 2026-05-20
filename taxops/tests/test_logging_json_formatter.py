"""PROD-2: JSON line formatter parses as JSON and preserves message."""

from __future__ import annotations

import json
import logging

from logging_config import JsonLinesFormatter


def test_json_lines_formatter_round_trip():
    fmt = JsonLinesFormatter()
    r = logging.LogRecord(
        name="taxops.tests",
        level=logging.WARNING,
        pathname=__file__,
        lineno=42,
        msg="client %s not found",
        args=("123",),
        exc_info=None,
    )
    line = fmt.format(r)
    obj = json.loads(line)
    assert obj["level"] == "WARNING"
    assert obj["logger"] == "taxops.tests"
    assert obj["msg"] == "client 123 not found"
    assert obj["lineno"] == 42
    assert "ts" in obj
