"""Tests for email_inbox_link and Drake email promote."""
from __future__ import annotations

from db import get_connection, init_db
from drake_prefill_importer import promote_drake_email_to_client
from email_inbox_link import link_inbox_for_client_email


def test_link_inbox_for_client_email_assigns_document(taxops_db_path, tmp_path):
    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('SMITH', 'JANE', '2026-01-01', '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, intake_date, "
        "client_status, created_at, updated_at) "
        "VALUES (1, 2025, '100', '2026-01-01', 'PROCESSING', '2026-01-01', '2026-01-01')"
    )
    src = tmp_path / "w2.pdf"
    src.write_bytes(b"%PDF-test")
    conn.execute(
        """
        INSERT INTO email_inbox
          (sender_email, sender_domain, filename, file_path, file_size_bytes,
           received_at, is_assigned, is_deleted)
        VALUES ('jane@example.com', 'example.com', 'w2.pdf', ?, 8,
                '2026-01-02', 0, 0)
        """,
        (str(src),),
    )
    conn.execute(
        "UPDATE clients SET taxpayer_email='jane@example.com', updated_at='2026-01-01' WHERE id=1"
    )
    conn.commit()

    stats = link_inbox_for_client_email(
        conn, 1, return_id=1, assigned_by="test", taxpayer_email="jane@example.com"
    )
    conn.commit()

    assert stats.assigned == 1
    assert conn.execute("SELECT is_assigned FROM email_inbox WHERE id=1").fetchone()[0] == 1
    docs = conn.execute(
        "SELECT return_id, match_method, match_confirmed FROM return_documents"
    ).fetchall()
    assert len(docs) == 1
    assert docs[0][0] == 1
    assert docs[0][1] == "email_client_address"
    assert docs[0][2] == 1
    conn.close()


def test_link_inbox_ignores_spouse_only_match(taxops_db_path, tmp_path):
    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (last_name, first_name, created_at, updated_at) "
        "VALUES ('SMITH', 'JANE', '2026-01-01', '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (client_id, tax_year, log_number, intake_date, "
        "client_status, created_at, updated_at) "
        "VALUES (1, 2025, '100', '2026-01-01', 'PROCESSING', '2026-01-01', '2026-01-01')"
    )
    src = tmp_path / "w2.pdf"
    src.write_bytes(b"%PDF-test")
    conn.execute(
        """
        INSERT INTO email_inbox
          (sender_email, sender_domain, filename, file_path, file_size_bytes,
           received_at, is_assigned, is_deleted)
        VALUES ('jane@example.com', 'example.com', 'w2.pdf', ?, 8,
                '2026-01-02', 0, 0)
        """,
        (str(src),),
    )
    conn.execute(
        "UPDATE clients SET spouse_email='jane@example.com', updated_at='2026-01-01' WHERE id=1"
    )
    conn.commit()

    stats = link_inbox_for_client_email(conn, 1, return_id=1, assigned_by="test")
    conn.commit()

    assert stats.assigned == 0
    conn.close()


def test_promote_drake_email_to_client(taxops_db_path):
    from tests.test_drake_client_email_audit import _seed_link_with_household

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, created_at, updated_at) "
        "VALUES (501, 'SMITH', 'JANE', '2026-01-01', '2026-01-01')"
    )
    _seed_link_with_household(
        conn,
        client_id=501,
        csm_name="SMITH, JANE",
        drake_email="jane@drake.com",
        taxpayer_email=None,
    )
    conn.commit()

    promoted = promote_drake_email_to_client(conn, 501)
    conn.commit()
    assert promoted == "jane@drake.com"
    row = conn.execute("SELECT taxpayer_email FROM clients WHERE id=501").fetchone()
    assert row[0] == "jane@drake.com"
    conn.close()
