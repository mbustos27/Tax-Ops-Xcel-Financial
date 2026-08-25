"""merge_client_into must be FK-safe, handle same-year log handoff, and write merge trail."""

from __future__ import annotations

import json

from merge_ops import merge_client_into
from utils import now


def _fresh(tmp_path):
    from db import get_connection, init_db

    path = str(tmp_path / "merge_test.db")
    conn = get_connection(path)
    init_db(conn)
    return conn


def test_merge_repoints_client_children_then_deletes(tmp_path):
    conn = _fresh(tmp_path)
    ts = now()
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, ssn_last4, created_at, updated_at) "
        "VALUES (1, 'ABOU ABDOU', 'AHMED & HIND', '1111', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (2, 'ABOU ABDOU', 'AHMED', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (10, 1, '574', 2025, 'LOG OUT', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (11, 2, NULL, 2025, 'LOG OUT', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO spouses (client_id, first_name, last_name, source, created_at) "
        "VALUES (1, 'HIND', 'ABED', 'drake', ?)",
        (ts,),
    )
    conn.execute(
        """
        INSERT INTO client_dependents (
          client_id, first_name, last_name, is_claimed_dependent,
          removed_for_ty2026, needs_review, source, created_at
        ) VALUES (1, 'KID', 'ABOU', 1, 0, 0, 'drake', ?)
        """,
        (ts,),
    )
    conn.execute(
        """
        INSERT INTO client_billing (client_id, balance_as_of, balance_due, source, created_at)
        VALUES (2, '2025-01-01', 50.0, 'drake', ?)
        """,
        (ts,),
    )
    conn.commit()

    hist_id = merge_client_into(conn, 1, 2, ts, operator="test", reason_code="unit")
    conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 1
    assert conn.execute("SELECT id FROM clients").fetchone()["id"] == 1
    assert conn.execute("SELECT COUNT(*) FROM returns WHERE client_id=1").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM spouses WHERE client_id=1").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM client_dependents WHERE client_id=1").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM client_billing WHERE client_id=1").fetchone()[0] == 1
    assert hist_id >= 1
    conn.close()


def test_merge_log_number_handoff_when_discard_return_wins(tmp_path):
    """Discard return is richer (has log); must reassign onto keep without UNIQUE clash."""
    conn = _fresh(tmp_path)
    ts = now()
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) VALUES (1,'A','One',?,?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) VALUES (2,'A','One X',?,?)",
        (ts, ts),
    )
    # Keep's return is sparse (no log); discard's is richer — discard wins score
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (10, 1, NULL, 2025, 'PENDING INTAKE', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (11, 2, '9001', 2025, 'PROCESSING', ?, ?)",
        (ts, ts),
    )
    conn.commit()

    merge_client_into(conn, 1, 2, ts)
    conn.commit()

    rows = conn.execute("SELECT id, log_number, client_id FROM returns WHERE tax_year=2025").fetchall()
    assert len(rows) == 1
    assert rows[0]["log_number"] == "9001"
    assert rows[0]["client_id"] == 1
    assert conn.execute("SELECT COUNT(*) FROM clients").fetchone()[0] == 1
    conn.close()


def test_merge_history_reconstructs_discarded_client(tmp_path):
    """Wave 2A acceptance: history row fully reconstructs discarded identity."""
    conn = _fresh(tmp_path)
    ts = now()
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, ssn_last4, taxpayer_email, "
        "created_at, updated_at) VALUES (1, 'KEEP', 'Ann', '1111', 'a@x.com', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, ssn_last4, taxpayer_phone, address, "
        "created_at, updated_at) VALUES (2, 'DROP', 'Bob', '2222', '555-0100', '1 Main', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (20, 1, '100', 2025, 'LOG OUT', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, log_number, tax_year, client_status, created_at, updated_at) "
        "VALUES (21, 2, '200', 2024, 'PROCESSING', ?, ?)",
        (ts, ts),
    )
    conn.execute(
        "INSERT INTO status_events (return_id, old_status, new_status, event_timestamp) "
        "VALUES (21, NULL, 'PROCESSING', ?)",
        (ts,),
    )
    conn.commit()

    hist_id = merge_client_into(
        conn,
        1,
        2,
        ts,
        operator="wave2a",
        reason_code="dry_run",
        note="reconstructability check",
    )
    conn.commit()

    assert conn.execute("SELECT COUNT(*) FROM clients WHERE id=2").fetchone()[0] == 0
    row = dict(
        conn.execute("SELECT * FROM client_merge_history WHERE id=?", (hist_id,)).fetchone()
    )
    assert row["keep_id"] == 1
    assert row["discard_id"] == 2
    assert row["operator"] == "wave2a"
    assert row["reason_code"] == "dry_run"
    assert row["note"] == "reconstructability check"

    snap = json.loads(row["discard_client_json"])
    assert snap["id"] == 2
    assert snap["last_name"] == "DROP"
    assert snap["first_name"] == "Bob"
    assert snap["ssn_last4"] == "2222"
    assert snap["taxpayer_phone"] == "555-0100"
    assert snap["address"] == "1 Main"

    rets = json.loads(row["discard_returns_json"])
    assert len(rets) == 1
    assert rets[0]["id"] == 21
    assert rets[0]["log_number"] == "200"
    assert rets[0]["tax_year"] == 2024

    actions = json.loads(row["returns_actions_json"])
    assert actions[0]["action"] == "repointed"
    assert actions[0]["return_id"] == 21

    events = json.loads(row["status_events_json"])
    assert len(events) == 1
    assert events[0]["return_id"] == 21

    # Return 21 should now live on keep
    assert conn.execute(
        "SELECT client_id FROM returns WHERE id=21"
    ).fetchone()["client_id"] == 1
    conn.close()
