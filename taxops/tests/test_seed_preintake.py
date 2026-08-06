"""Seed PENDING INTAKE for every client missing the target tax year."""

from __future__ import annotations

from season_rollover import (
    NEW_ROLLOVER_STATUS,
    RolloverCarryOptions,
    seed_preintake_commit,
    seed_preintake_preview,
)
from utils import now


def test_seed_preintake_from_prior_and_orphan(taxops_db_path: str):
    import db as db_mod

    conn = db_mod.get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (701, 'Prior', 'Has')")
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (702, 'Orphan', 'Only')")
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (703, 'Done', 'Already')")
    conn.executescript(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (7201, 701, '7201', 2024, 'LOG OUT', 'Pat');
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (7202, 703, '7202', 2025, 'PROCESSING', 'Pat');
        """
    )
    conn.commit()

    prev = seed_preintake_preview(conn, target_year=2025)
    assert prev["ok"] is True
    assert prev["totals"]["missing"] == 2
    assert prev["totals"]["with_prior_return"] == 1
    assert prev["totals"]["orphans_no_prior_return"] == 1

    out = seed_preintake_commit(
        conn,
        target_year=2025,
        options=RolloverCarryOptions(),
        actor="tester",
        ts=now(),
    )
    assert out["ok"] is True
    assert out["totals"]["created"] == 2

    rows = conn.execute(
        "SELECT client_id, client_status, processor FROM returns WHERE tax_year=2025 ORDER BY client_id"
    ).fetchall()
    by_cid = {int(r["client_id"]): dict(r) for r in rows}
    assert by_cid[701]["client_status"] == NEW_ROLLOVER_STATUS
    assert by_cid[701]["processor"] == "Pat"
    assert by_cid[702]["client_status"] == NEW_ROLLOVER_STATUS
    assert by_cid[703]["client_status"] == "PROCESSING"

    # Idempotent
    out2 = seed_preintake_commit(
        conn,
        target_year=2025,
        options=RolloverCarryOptions(),
        actor="tester",
        ts=now(),
    )
    assert out2["totals"]["created"] == 0
    conn.close()


def test_seed_preintake_after_import_uses_active_year(taxops_db_path: str, monkeypatch):
    """Post-import helper seeds the active intake year without raising."""
    import db as db_mod
    from season_rollover import NEW_ROLLOVER_STATUS, seed_preintake_after_import

    conn = db_mod.get_connection(taxops_db_path)
    monkeypatch.setattr(db_mod, "get_active_intake_tax_year", lambda _c: 2025)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (711, 'Import', 'New')")
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (7211, 711, '7211', 2024, 'LOG OUT')
        """
    )
    conn.commit()

    out = seed_preintake_after_import(conn, actor="test_import")
    assert out["ok"] is True
    assert out["target_year"] == 2025
    assert out["totals"]["created"] == 1

    row = conn.execute(
        "SELECT client_status FROM returns WHERE client_id=711 AND tax_year=2025"
    ).fetchone()
    assert row is not None
    assert row["client_status"] == NEW_ROLLOVER_STATUS

    # Idempotent / best-effort
    out2 = seed_preintake_after_import(conn, actor="test_import")
    assert out2["ok"] is True
    assert out2["totals"]["created"] == 0
    conn.close()
