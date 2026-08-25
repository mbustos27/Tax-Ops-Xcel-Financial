"""Spouse import: primary-taxpayer matching guards."""
from __future__ import annotations

import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))

from import_spouse_info import (  # noqa: E402
    _client_is_spouse_name,
    _match_primary_taxpayer,
    _primary_attachment_score,
)
from name_matcher import ACCEPT_THRESHOLD, score_client_names_pair  # noqa: E402


def test_primary_match_not_spouse_side():
    conn = sqlite3.connect(":memory:")
    conn.row_factory = sqlite3.Row
    conn.execute(
        "CREATE TABLE clients (id INTEGER PRIMARY KEY, last_name TEXT, first_name TEXT)"
    )
    conn.execute("INSERT INTO clients VALUES (1, 'ORTIZ', 'ARGELIS')")
    conn.execute("INSERT INTO clients VALUES (2, 'CANIZALES', 'ROSA')")
    conn.execute("INSERT INTO clients VALUES (3, 'CANIZALES', 'SANDRA')")

    drake = "ARGELIS ORTIZ & SANDRA CANIZALES"
    match = _match_primary_taxpayer(drake, conn, None)
    assert match is not None
    assert match["client_id"] == 1
    assert match["score"] == 100

    assert _client_is_spouse_name("CANIZALES", "SANDRA", "SANDRA", "CANIZALES", "")
    assert not _client_is_spouse_name("ORTIZ", "ARGELIS", "SANDRA", "CANIZALES", "")

    assert _primary_attachment_score("CANIZALES", "ROSA", drake) < ACCEPT_THRESHOLD
    assert _primary_attachment_score("ORTIZ", "ARGELIS", drake) >= ACCEPT_THRESHOLD
    conn.close()


def test_self_spouse_score():
    assert score_client_names_pair("CANIZALES", "SANDRA", "CANIZALES", "SANDRA") >= ACCEPT_THRESHOLD
