"""Auto LOG OUT when Drake marks e-file accepted."""

from __future__ import annotations

from datetime import date

import pytest


def _seed_return(conn, *, rid: int, cid: int, status: str, drake: str | None = None, ack: str | None = None):
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, taxpayer_email)
        VALUES (?, 'TEST', 'A', 'auto@example.com')
        """,
        (cid,),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, tax_year, log_number, client_status,
          drake_status_raw, ack_date, created_at
        ) VALUES (?, ?, 2025, ?, ?, ?, ?, datetime('now'))
        """,
        (rid, cid, str(rid), status, drake, ack),
    )


def test_maybe_sync_efile_logout_updates_processing(taxops_db_path):
    from db import get_connection
    from efile_logout_sync import maybe_sync_efile_logout

    conn = get_connection(taxops_db_path)
    try:
        _seed_return(conn, rid=9101, cid=9101, status="PROCESSING", drake="EF Accepted")
        conn.commit()
        assert maybe_sync_efile_logout(conn, 9101) is True
        conn.commit()
        row = conn.execute(
            "SELECT client_status, logout_date FROM returns WHERE id=9101"
        ).fetchone()
        assert row["client_status"] == "LOG OUT"
        assert row["logout_date"] == date.today().isoformat()
        events = conn.execute(
            "SELECT COUNT(*) FROM status_events WHERE return_id=9101"
        ).fetchone()[0]
        assert events == 1
    finally:
        conn.close()


def test_maybe_sync_skips_extension_only(taxops_db_path):
    from db import get_connection
    from efile_logout_sync import maybe_sync_efile_logout

    conn = get_connection(taxops_db_path)
    try:
        _seed_return(conn, rid=9102, cid=9102, status="PROCESSING", drake="EF Ext Accepted")
        conn.commit()
        assert maybe_sync_efile_logout(conn, 9102) is False
        st = conn.execute("SELECT client_status FROM returns WHERE id=9102").fetchone()[0]
        assert st == "PROCESSING"
    finally:
        conn.close()


def test_sync_batch_dry_run_counts(taxops_db_path):
    from db import get_connection
    from efile_logout_sync import sync_efile_accepted_logouts

    conn = get_connection(taxops_db_path)
    try:
        _seed_return(conn, rid=9103, cid=9103, status="HOLD", drake="E-Filed: YES")
        _seed_return(conn, rid=9104, cid=9104, status="LOG OUT", drake="EF Accepted")
        conn.commit()
        result = sync_efile_accepted_logouts(conn, tax_year=2025, dry_run=True)
        assert result["candidate_count"] == 1
        assert result["updated"] == 1
    finally:
        conn.close()


def test_revert_premature_logout_extension_entity(taxops_db_path):
    from db import get_connection
    from efile_logout_sync import revert_premature_logouts

    conn = get_connection(taxops_db_path)
    try:
        _seed_return(conn, rid=9105, cid=9105, status="LOG OUT", drake="EF Ext Accepted")
        conn.execute(
            "INSERT INTO return_forms (return_id, form_1120s) VALUES (9105, 1)"
        )
        conn.commit()
        result = revert_premature_logouts(conn, tax_year=2025, entity_only=True, dry_run=False)
        assert result["updated"] == 1
        st = conn.execute("SELECT client_status FROM returns WHERE id=9105").fetchone()[0]
        assert st == "PROCESSING"
    finally:
        conn.close()
