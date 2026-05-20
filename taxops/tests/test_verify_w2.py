"""FORMS-2 — verify_w2 regression harness must pass on clean DB."""

from __future__ import annotations

from pathlib import Path

import pytest

from verify_w2 import run_w2_verification


def test_verify_w2_harness_passes(tmp_path: Path):
    db = str(tmp_path / "verify_w2_test.db")
    ok, lines = run_w2_verification(db_path=db, dump_row=False)
    assert ok, "\n".join(lines)
