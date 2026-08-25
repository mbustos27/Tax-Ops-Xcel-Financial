"""Intake client search uses AND-token matching so Drake names are findable."""

from __future__ import annotations


def test_client_search_multi_token_finds_drake_name(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (901, 'ABDEL HADY', 'OMAR')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (9101, 901, '8001', 2024, 'LOG OUT')
        """
    )
    conn.commit()
    conn.close()

    # Whole-string LIKE on one field would miss this; token AND must hit.
    r = client_logged_in.get("/api/clients/search?q=omar%20abdel")
    assert r.status_code == 200
    data = r.get_json()
    assert any(x["id"] == 901 for x in data)
    hit = next(x for x in data if x["id"] == 901)
    assert hit["preintake"] is True  # no TY2025 return yet


def test_client_search_marks_pending_intake_as_preintake(client_logged_in, taxops_db_path):
    from db import get_connection, set_active_intake_tax_year

    conn = get_connection(taxops_db_path)
    set_active_intake_tax_year(conn, 2025)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (902, 'SMITH', 'JANE')"
    )
    conn.execute(
        """
        INSERT INTO returns (id, client_id, log_number, tax_year, client_status)
        VALUES (9102, 902, NULL, 2025, 'PENDING INTAKE')
        """
    )
    conn.commit()
    conn.close()

    r = client_logged_in.get("/api/clients/search?q=jane%20smith")
    assert r.status_code == 200
    hit = next(x for x in r.get_json() if x["id"] == 902)
    assert hit["preintake"] is True


def test_client_search_finds_joint_via_prefill_spouse(client_logged_in, taxops_db_path):
    """Joint CSM names linked to clients must be findable by spouse token."""
    from db import get_connection, init_db, set_active_intake_tax_year
    from utils import now as utc_now

    conn = get_connection(taxops_db_path)
    init_db(conn)
    set_active_intake_tax_year(conn, 2025)
    ts = utc_now()
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, spouse_first_name, spouse_last_name)
        VALUES (903, 'BARRERA', 'ADRIAN', 'BLANCA', 'BARRERA')
        """
    )
    conn.execute(
        """
        INSERT INTO drake_prefill_links (
          tax_year, csm_ssn_last4, csm_name_raw, csm_name_norm, client_id,
          prefill_status, purple_name, match_tier, created_at, updated_at
        ) VALUES (
          2024, '8676', 'BARRERA, ADRIAN & BLANCA', 'BARRERA, ADRIAN & BLANCA', 903,
          'PRIOR_YEAR_FORMS_AVAILABLE', 'ADRIAN & BLANCA BARRERA', 'deterministic', ?, ?
        )
        """,
        (ts, ts),
    )
    conn.commit()
    conn.close()

    r = client_logged_in.get("/api/clients/search?q=blanca%20barrera")
    assert r.status_code == 200
    data = r.get_json()
    hit = next((x for x in data if x["id"] == 903), None)
    assert hit is not None, data
    assert "BLANCA" in (hit["name"] or "").upper()
    assert hit.get("prior_year_forms") is True


def test_reintake_fills_name_from_prefill_when_client_blank(client_logged_in, taxops_db_path):
    """Preintake autofill must return CSM names even if the clients row is empty."""
    from db import get_connection, init_db, set_active_intake_tax_year
    from utils import now as utc_now

    conn = get_connection(taxops_db_path)
    init_db(conn)
    set_active_intake_tax_year(conn, 2025)
    ts = utc_now()
    # Thin client stub — no names (simulates a bad/partial row)
    conn.execute("INSERT INTO clients (id, last_name, first_name) VALUES (904, '', NULL)")
    conn.execute(
        """
        INSERT INTO drake_prefill_links (
          tax_year, csm_ssn_last4, csm_name_raw, csm_name_norm, client_id,
          prefill_status, purple_name, match_tier, created_at, updated_at
        ) VALUES (
          2024, '8676', 'BARRERA, ADRIAN & BLANCA', 'BARRERA, ADRIAN & BLANCA', 904,
          'PRIOR_YEAR_FORMS_AVAILABLE', 'ADRIAN & BLANCA BARRERA', 'deterministic', ?, ?
        )
        """,
        (ts, ts),
    )
    conn.execute(
        """
        INSERT INTO drake_form_prefill (
          link_id, tax_year, form_counts, return_type, source_files, created_at
        )
        SELECT id, 2024, '{"Schedule C": 1, "Schedule A": null}', '1040SR', '[]', ?
        FROM drake_prefill_links WHERE client_id = 904
        """,
        (ts,),
    )
    conn.commit()
    conn.close()

    r = client_logged_in.get("/api/clients/904/reintake")
    assert r.status_code == 200
    body = r.get_json()
    assert body["client"]["last_name"] == "BARRERA"
    assert body["client"]["first_name"] == "ADRIAN"
    assert body["drake_spouse"]["spouse_first_name"] == "BLANCA"
    assert body["last_return"].get("form_1040") == 1
    assert body["last_return"].get("sched_c") == 1
    # null Schedule A must not coerce to a checked box
    assert body["last_return"].get("sched_a_d") in (None, 0, False, "")
