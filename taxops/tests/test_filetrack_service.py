"""M3 — filetrack_service.apply_filetrack_status() against a real (temp) TaxOps DB."""
from __future__ import annotations

import pytest

from db import get_connection
from filetrack_service import FiletrackStatusError, apply_filetrack_status, get_filetrack_history


def _seed_client_and_return(conn, *, log_number: str, tax_year: int = 2026) -> int:
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (?, 'Test', 'Client', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        (900 + hash(log_number) % 90,),
    )
    client_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        "INSERT INTO returns (client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (?, ?, ?, 'PROCESSING', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')",
        (client_id, log_number, tax_year),
    )
    conn.commit()
    return conn.execute("SELECT last_insert_rowid()").fetchone()[0]


def test_unknown_status_raises_and_writes_nothing(taxops_db_path):
    with pytest.raises(FiletrackStatusError):
        apply_filetrack_status("00123", "NOT_A_REAL_STATUS")

    conn = get_connection(taxops_db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM filetrack_status_history").fetchone()["n"]
    finally:
        conn.close()
    assert count == 0


def test_matched_log_number_updates_return_and_history(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00123")
    conn.close()

    result = apply_filetrack_status("123", "FINALIZE", source="scanner")
    assert result == {"matched": True, "return_id": return_id, "old_status": None, "new_status": "FINALIZE"}

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT filetrack_status, filetrack_status_updated_at FROM returns WHERE id = ?", (return_id,)
        ).fetchone()
        assert row["filetrack_status"] == "FINALIZE"
        assert row["filetrack_status_updated_at"] is not None

        hist = conn.execute(
            "SELECT * FROM filetrack_status_history WHERE return_id = ?", (return_id,)
        ).fetchall()
        assert len(hist) == 1
        assert hist[0]["log_number"] == "00123"
        assert hist[0]["old_status"] is None
        assert hist[0]["new_status"] == "FINALIZE"
        assert hist[0]["source"] == "scanner"
    finally:
        conn.close()


def test_second_scan_records_old_status_correctly(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00456")
    conn.close()

    apply_filetrack_status("00456", "PROCESSING")
    result = apply_filetrack_status("00456", "HOLD")
    assert result["old_status"] == "PROCESSING"
    assert result["new_status"] == "HOLD"

    conn = get_connection(taxops_db_path)
    try:
        hist = conn.execute(
            "SELECT old_status, new_status FROM filetrack_status_history "
            "WHERE return_id = ? ORDER BY id", (return_id,),
        ).fetchall()
        assert [dict(h) for h in hist] == [
            {"old_status": None, "new_status": "PROCESSING"},
            {"old_status": "PROCESSING", "new_status": "HOLD"},
        ]
    finally:
        conn.close()


def test_unmatched_log_number_recorded_in_history_only_never_raises(taxops_db_path):
    result = apply_filetrack_status("00999", "PICKUP")
    assert result == {"matched": False, "return_id": None, "old_status": None, "new_status": "PICKUP"}

    conn = get_connection(taxops_db_path)
    try:
        hist = conn.execute(
            "SELECT return_id, log_number, new_status FROM filetrack_status_history WHERE log_number = '00999'"
        ).fetchall()
        assert len(hist) == 1
        assert hist[0]["return_id"] is None
        assert hist[0]["new_status"] == "PICKUP"
    finally:
        conn.close()


def test_matches_unpadded_log_number_already_stored(taxops_db_path):
    # Older rows may have "7" rather than "00007" — apply_filetrack_status
    # must still find them via the raw/un-padded fallback lookup.
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="7")
    conn.close()

    result = apply_filetrack_status("7", "EFILE READY")
    assert result["matched"] is True
    assert result["return_id"] == return_id


def test_case_insensitive_status_name_is_normalized(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00042")
    conn.close()

    result = apply_filetrack_status("00042", "hold")
    assert result["new_status"] == "HOLD"


def test_get_filetrack_history_orders_most_recent_first(taxops_db_path):
    apply_filetrack_status("00321", "PROCESSING")
    apply_filetrack_status("00321", "HOLD")
    apply_filetrack_status("00321", "FINALIZE")

    hist = get_filetrack_history("321")
    assert [h["new_status"] for h in hist] == ["FINALIZE", "HOLD", "PROCESSING"]
