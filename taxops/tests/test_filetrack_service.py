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
    assert result == {
        "matched": True,
        "return_id": return_id,
        "old_status": None,
        "new_status": "FINALIZE",
        "client_status_synced": True,
    }

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT filetrack_status, filetrack_status_updated_at, client_status FROM returns WHERE id = ?",
            (return_id,),
        ).fetchone()
        assert row["filetrack_status"] == "FINALIZE"
        assert row["filetrack_status_updated_at"] is not None
        # 2026-07-22: scans now also drive the real workflow status, not just
        # the filetrack_status shadow column — see filetrack_service.py's
        # apply_filetrack_status() docstring for why.
        assert row["client_status"] == "FINALIZE"

        hist = conn.execute(
            "SELECT * FROM filetrack_status_history WHERE return_id = ?", (return_id,)
        ).fetchall()
        assert len(hist) == 1
        assert hist[0]["log_number"] == "00123"
        assert hist[0]["old_status"] is None
        assert hist[0]["new_status"] == "FINALIZE"
        assert hist[0]["source"] == "scanner"

        events = conn.execute(
            "SELECT old_status, new_status, source_file FROM status_events WHERE return_id = ?", (return_id,)
        ).fetchall()
        assert len(events) == 1
        assert events[0]["old_status"] == "PROCESSING"  # seeded client_status
        assert events[0]["new_status"] == "FINALIZE"
        assert events[0]["source_file"] == "FILETRACK"
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
    assert result == {
        "matched": False,
        "return_id": None,
        "old_status": None,
        "new_status": "PICKUP",
        "client_status_synced": False,
    }

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


def test_matches_unpadded_multidigit_log_number_scanned_as_padded(taxops_db_path):
    """Regression test for the 2026-07-22 production incident: return 1282
    was stored in the DB as log_number="1282" (unpadded), but its printed
    barcode label always encodes the zero-padded form ("01282", via
    filetrack.config.format_log_number()) — so a real scan's log_number
    argument is ALWAYS already zero-padded, never the short raw form.
    That means the old "raw_log" fallback (which only helps when the
    CALLER's input itself is unpadded) never fires for real scanner
    traffic: formatted_log == raw_log == "01282" in that case, and neither
    matches the DB's unpadded "1282". Every one of 6 real scans of this
    return silently no-opped (return_id NULL in filetrack_status_history,
    client_status never touched) until the zero-STRIPPED fallback below was
    added. This test scans with the padded form, exactly like a real
    barcode would, against an unpadded-but-multidigit stored value."""
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="1282")
    conn.close()

    result = apply_filetrack_status("01282", "EFILE READY")  # what a real scan sends
    assert result["matched"] is True
    assert result["return_id"] == return_id
    assert result["client_status_synced"] is True

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute("SELECT client_status FROM returns WHERE id = ?", (return_id,)).fetchone()
        assert row["client_status"] == "EFILE READY"
    finally:
        conn.close()


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


def test_scan_sets_date_stamp_like_api_status(taxops_db_path):
    """PICKUP/LOG OUT set pickup_date/logout_date the first time that status
    is reached — mirrors app.py's STATUS_DATE_STAMP behavior in api_status
    exactly, so scan-driven and staff-driven transitions leave the same
    audit trail for reporting."""
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00700")
    conn.close()

    apply_filetrack_status("00700", "PICKUP")

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT client_status, pickup_date FROM returns WHERE id = ?", (return_id,)
        ).fetchone()
        assert row["client_status"] == "PICKUP"
        assert row["pickup_date"] is not None
    finally:
        conn.close()


def test_scan_does_not_overwrite_existing_date_stamp(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00701")
    conn.close()

    apply_filetrack_status("00701", "PICKUP")
    conn = get_connection(taxops_db_path)
    try:
        first_pickup_date = conn.execute(
            "SELECT pickup_date FROM returns WHERE id = ?", (return_id,)
        ).fetchone()["pickup_date"]
    finally:
        conn.close()

    apply_filetrack_status("00701", "PICKUP")  # scanned twice, e.g. re-scan
    conn = get_connection(taxops_db_path)
    try:
        second_pickup_date = conn.execute(
            "SELECT pickup_date FROM returns WHERE id = ?", (return_id,)
        ).fetchone()["pickup_date"]
    finally:
        conn.close()
    assert first_pickup_date == second_pickup_date


def test_scan_to_rejected_clears_contact_fields_like_api_status(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00702")
    conn.execute(
        "UPDATE returns SET contact_status='attempted', last_contacted_date='2026-01-01' WHERE id=?",
        (return_id,),
    )
    conn.commit()
    conn.close()

    apply_filetrack_status("00702", "REJECTED")

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT client_status, contact_status, last_contacted_date FROM returns WHERE id = ?",
            (return_id,),
        ).fetchone()
        assert row["client_status"] == "REJECTED"
        assert row["contact_status"] == "not_contacted"
        assert row["last_contacted_date"] is None
    finally:
        conn.close()


def test_scan_leaving_rejected_clears_contact_fields(taxops_db_path):
    conn = get_connection(taxops_db_path)
    return_id = _seed_client_and_return(conn, log_number="00703")
    conn.execute(
        "UPDATE returns SET client_status='REJECTED', contact_status='not_contacted' WHERE id=?",
        (return_id,),
    )
    conn.commit()
    conn.close()

    apply_filetrack_status("00703", "PROCESSING")

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT client_status, contact_status, last_contacted_date FROM returns WHERE id = ?",
            (return_id,),
        ).fetchone()
        assert row["client_status"] == "PROCESSING"
        assert row["contact_status"] is None
        assert row["last_contacted_date"] is None
    finally:
        conn.close()


def test_unmatched_scan_never_touches_client_status_or_status_events(taxops_db_path):
    """Nothing to sync when there's no return row — only the standalone
    filetrack_status_history row (return_id=NULL) should be written."""
    apply_filetrack_status("00888", "FINALIZE")

    conn = get_connection(taxops_db_path)
    try:
        count = conn.execute("SELECT COUNT(*) AS n FROM status_events").fetchone()["n"]
        assert count == 0
    finally:
        conn.close()
