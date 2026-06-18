"""Season rollover (Epic #88)."""

from __future__ import annotations

import json

import pytest


def test_rollover_preview_counts(taxops_db_path: str):  # noqa: ARG001
    import db as db_mod
    from season_rollover import RolloverCarryOptions, rollover_preview_json

    conn = db_mod.get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (501, 'A', 'One')")
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (502, 'B', 'Two')")
    conn.executescript(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (6001, 501, '9001', 2024, 'LOG OUT', 'Alice');
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (6002, 502, '9002', 2024, 'LOG OUT', 'Bob');
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (6003, 502, '9002', 2025, 'PENDING INTAKE', 'Bob');
        """
    )
    conn.commit()
    conn.close()

    conn = db_mod.get_connection(taxops_db_path)
    prev = rollover_preview_json(
        conn,
        source_year=2024,
        target_year=2025,
        options=RolloverCarryOptions(),
    )
    conn.close()
    assert prev["ok"] is True
    assert prev["totals"]["eligible"] == 1
    assert prev["totals"]["skipped"] == 1


def test_rollover_commit_idempotent(taxops_db_path: str):  # noqa: ARG001
    import db as db_mod
    from season_rollover import NEW_ROLLOVER_STATUS, RolloverCarryOptions, rollover_commit
    from utils import now

    conn = db_mod.get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (601, 'C', 'Three')")
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status, processor)
        VALUES (6101, 601, '9101', 2024, 'LOG OUT', 'Pat')
        """
    )
    conn.commit()
    conn.close()

    ts = now()
    opts = RolloverCarryOptions(carry_prior_year_log_on_client=True)
    conn = db_mod.get_connection(taxops_db_path)
    out1 = rollover_commit(
        conn,
        source_year=2024,
        target_year=2025,
        options=opts,
        actor="tester",
        ts=ts,
    )
    conn.close()
    assert out1["ok"] is True
    assert len(out1["report"]["created"]) == 1
    rid_new = out1["report"]["created"][0]["new_return_id"]
    assert rid_new

    conn = db_mod.get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT client_status, processor, tax_year FROM returns WHERE id = ?",
        (rid_new,),
    ).fetchone()
    cli = conn.execute("SELECT prior_year_log FROM clients WHERE id = 601").fetchone()
    conn.close()
    assert row["client_status"] == NEW_ROLLOVER_STATUS
    assert row["processor"] == "Pat"
    assert row["tax_year"] == 2025
    assert cli["prior_year_log"] == "9101"

    ts2 = now()
    conn = db_mod.get_connection(taxops_db_path)
    out2 = rollover_commit(
        conn,
        source_year=2024,
        target_year=2025,
        options=opts,
        actor="tester",
        ts=ts2,
    )
    conn.close()
    assert out2["ok"] is True
    assert len(out2["report"]["created"]) == 0
    assert len(out2["report"]["skipped"]) >= 1


def test_rollover_preview_api_forbidden(client_logged_in, monkeypatch: pytest.MonkeyPatch):
    import app as app_mod

    monkeypatch.setattr(app_mod, "can_run_season_rollover", lambda: False)
    rv = client_logged_in.post(
        "/api/admin/season-rollover/preview",
        data=json.dumps({"source_year": 2024, "target_year": 2025}),
        content_type="application/json",
    )
    assert rv.status_code == 403
