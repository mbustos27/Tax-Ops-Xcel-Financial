"""Drake household email vs client email validation on prefill links."""
from __future__ import annotations


def _seed_link_with_household(
    conn,
    *,
    client_id: int,
    csm_name: str,
    drake_email: str,
    taxpayer_email: str | None,
    spouse_email: str | None = None,
    match_tier: str = "fuzzy",
) -> int:
    from utils import now as utc_now

    ts = utc_now()
    conn.execute(
        """
        INSERT INTO drake_prefill_links (
          tax_year, csm_ssn_last4, csm_name_raw, csm_name_norm, client_id,
          prefill_status, purple_name, match_tier, created_at, updated_at
        ) VALUES (2025, '1234', ?, ?, ?, 'PRIOR_YEAR_FORMS_AVAILABLE', 'PURPLE', ?, ?, ?)
        """,
        (csm_name, csm_name.upper(), client_id, match_tier, ts, ts),
    )
    link_id = conn.execute("SELECT last_insert_rowid()").fetchone()[0]
    conn.execute(
        """
        INSERT INTO drake_household_prefill (
          link_id, tax_year, taxpayer_email, dependents_json, created_at, updated_at
        ) VALUES (?, 2025, ?, '[]', ?, ?)
        """,
        (link_id, drake_email, ts, ts),
    )
    if taxpayer_email is not None or spouse_email is not None:
        conn.execute(
            """
            UPDATE clients
            SET taxpayer_email = ?, spouse_email = ?, updated_at = ?
            WHERE id = ?
            """,
            (taxpayer_email, spouse_email, ts, client_id),
        )
    conn.commit()
    return int(link_id)


def test_audit_clears_email_and_unlinks_when_mismatch_no_name(taxops_db_path):
    from db import get_connection, init_db
    from drake_prefill_importer import audit_drake_client_email_links

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (501, 'WRONG', 'PERSON', datetime('now'), datetime('now'))"
    )
    link_id = _seed_link_with_household(
        conn,
        client_id=501,
        csm_name="SMITH, JOHN",
        drake_email="john.smith@email.com",
        taxpayer_email="totally.wrong@other.com",
    )

    stats = audit_drake_client_email_links(conn, tax_year=2025, dry_run=False)
    conn.commit()

    assert stats.cleared_taxpayer_email == 1
    assert stats.links_unlinked == 1
    row = conn.execute(
        "SELECT taxpayer_email FROM clients WHERE id = 501"
    ).fetchone()
    assert row["taxpayer_email"] is None
    link = conn.execute(
        "SELECT client_id FROM drake_prefill_links WHERE id = ?", (link_id,)
    ).fetchone()
    assert link["client_id"] is None
    conn.close()


def test_audit_keeps_email_when_name_matches(taxops_db_path):
    from db import get_connection, init_db
    from drake_prefill_importer import audit_drake_client_email_links

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (502, 'SMITH', 'JOHN', datetime('now'), datetime('now'))"
    )
    link_id = _seed_link_with_household(
        conn,
        client_id=502,
        csm_name="SMITH, JOHN",
        drake_email="john.smith@email.com",
        taxpayer_email="alternate@personal.com",
    )

    stats = audit_drake_client_email_links(conn, tax_year=2025, dry_run=False)
    conn.commit()

    assert stats.kept_name_match == 1
    assert stats.cleared_taxpayer_email == 0
    assert stats.links_unlinked == 0
    row = conn.execute(
        "SELECT taxpayer_email FROM clients WHERE id = 502"
    ).fetchone()
    assert row["taxpayer_email"] == "alternate@personal.com"
    link = conn.execute(
        "SELECT client_id FROM drake_prefill_links WHERE id = ?", (link_id,)
    ).fetchone()
    assert link["client_id"] == 502
    conn.close()


def test_audit_keeps_when_client_email_matches_drake(taxops_db_path):
    from db import get_connection, init_db
    from drake_prefill_importer import audit_drake_client_email_links

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (503, 'OTHER', 'NAME', datetime('now'), datetime('now'))"
    )
    link_id = _seed_link_with_household(
        conn,
        client_id=503,
        csm_name="SMITH, JOHN",
        drake_email="john.smith@email.com",
        taxpayer_email="john.smith@email.com",
    )

    stats = audit_drake_client_email_links(conn, tax_year=2025, dry_run=False)
    conn.commit()

    assert stats.kept_email_match == 1
    assert stats.links_unlinked == 0
    link = conn.execute(
        "SELECT client_id FROM drake_prefill_links WHERE id = ?", (link_id,)
    ).fetchone()
    assert link["client_id"] == 503
    conn.close()


def test_link_prefill_runs_email_audit_on_existing_links(taxops_db_path):
    """link_prefill_clients ends with audit_drake_client_email_links on live DB."""
    from db import get_connection, init_db
    from drake_prefill_importer import link_prefill_clients

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (504, 'WRONG', 'PERSON', datetime('now'), datetime('now'))"
    )
    link_id = _seed_link_with_household(
        conn,
        client_id=504,
        csm_name="SMITH, JOHN",
        drake_email="john.smith@email.com",
        taxpayer_email="bad@wrong.com",
    )

    link_prefill_clients(conn, tax_year=2025, dry_run=False)
    link = conn.execute(
        "SELECT client_id FROM drake_prefill_links WHERE id = ?", (link_id,)
    ).fetchone()
    email = conn.execute(
        "SELECT taxpayer_email FROM clients WHERE id = 504"
    ).fetchone()
    assert link["client_id"] is None
    assert email["taxpayer_email"] is None
    conn.close()
