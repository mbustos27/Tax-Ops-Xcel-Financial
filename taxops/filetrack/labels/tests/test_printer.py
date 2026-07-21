"""M1 — filetrack.labels.printer.list_printers() against the real win32print
API on this Windows dev machine (no mocking) — regression test for a real
bug: EnumPrinters(2) returns (flags, description, name, comment) tuples,
not dicts with a "pPrinterName" key. Only runs meaningfully on Windows with
pywin32 installed; skipped otherwise."""
from __future__ import annotations

import pytest

from filetrack.labels.printer import _HAVE_WIN32PRINT, list_printers

pytestmark = pytest.mark.skipif(not _HAVE_WIN32PRINT, reason="pywin32 not available on this platform")


def test_list_printers_returns_list_of_plain_strings():
    names = list_printers()
    assert isinstance(names, list)
    assert len(names) > 0
    for name in names:
        assert isinstance(name, str)
        assert name  # non-empty
