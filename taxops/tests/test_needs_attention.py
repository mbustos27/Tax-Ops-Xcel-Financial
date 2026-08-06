"""Feature 2 — needs-attention derived query."""

from __future__ import annotations

from datetime import date, timedelta

from werkzeug.security import generate_password_hash


def _seed_client_return(
    taxops_db_path: str,
    *,
    client_id: int,
    return_id: int,
    log_number: str,
    tax_year: int = 2025,
    status: str = "PROCESSING",
    intake_date: str | None = None,
    drake_status_raw: str | None = None,
    contact_status: str | None = None,
    display_name: str | None = "Attention Client",
    last_name: str = "Attention",
    first_name: str = "Client",
) -> None:
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (?, ?, ?, ?)
        """,
        (client_id, last_name, first_name, display_name),
    )
    conn.execute(
        """
        INSERT INTO returns (
          id, client_id, log_number, tax_year, client_status,
          intake_date, drake_status_raw, contact_status
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            return_id,
            client_id,
            log_number,
            tax_year,
            status,
            intake_date,
            drake_status_raw,
            contact_status,
        ),
    )
    conn.commit()
    conn.close()


def test_fetch_includes_three_reasons(taxops_db_path):
    from app import fetch_needs_attention
    from db import get_connection

    stale_intake = (date.today() - timedelta(days=90)).isoformat()
    _seed_client_return(
        taxops_db_path,
        client_id=501,
        return_id=5101,
        log_number="5101",
        status="PROCESSING",
        intake_date=stale_intake,
        drake_status_raw=None,
    )
    _seed_client_return(
        taxops_db_path,
        client_id=502,
        return_id=5102,
        log_number="5102",
        status="REJECTED",
        contact_status="not_contacted",
        last_name="Reject",
        first_name="Need",
        display_name="Reject, Need",
    )
    _seed_client_return(
        taxops_db_path,
        client_id=503,
        return_id=5103,
        log_number="5103",
        status="PROCESSING",
        intake_date=date.today().isoformat(),
        drake_status_raw="EF Rejected",
        last_name="Ef",
        first_name="Reject",
        display_name="Ef, Reject",
    )

    conn = get_connection(taxops_db_path)
    items, total, counts = fetch_needs_attention(conn, 2025, limit=None)
    conn.close()

    reasons = {it["return_id"]: it["reason"] for it in items}
    assert total == 3
    assert reasons[5101] == "stale_processing"
    assert reasons[5102] == "client_contact"
    assert reasons[5103] == "ef_rejected"
    assert counts == {
        "ef_rejected": 1,
        "client_contact": 1,
        "stale_processing": 1,
    }


def test_fetch_excludes_resolved_and_fresh(taxops_db_path):
    from app import fetch_needs_attention
    from db import get_connection

    _seed_client_return(
        taxops_db_path,
        client_id=511,
        return_id=5111,
        log_number="5111",
        status="REJECTED",
        contact_status="resolved",
        display_name="Resolved Reject",
    )
    _seed_client_return(
        taxops_db_path,
        client_id=512,
        return_id=5112,
        log_number="5112",
        status="PROCESSING",
        intake_date=date.today().isoformat(),
        display_name="Fresh Processing",
        last_name="Fresh",
    )
    _seed_client_return(
        taxops_db_path,
        client_id=513,
        return_id=5113,
        log_number="5113",
        tax_year=2024,
        status="REJECTED",
        contact_status="not_contacted",
        display_name="Prior Year",
        last_name="Prior",
    )

    conn = get_connection(taxops_db_path)
    items, total, counts = fetch_needs_attention(conn, 2025, limit=None)
    conn.close()
    assert total == 0
    assert items == []
    assert counts == {
        "ef_rejected": 0,
        "client_contact": 0,
        "stale_processing": 0,
    }


def test_fetch_dedups_ef_over_contact(taxops_db_path):
    from app import fetch_needs_attention
    from db import get_connection

    _seed_client_return(
        taxops_db_path,
        client_id=521,
        return_id=5121,
        log_number="5121",
        status="REJECTED",
        contact_status="not_contacted",
        drake_status_raw="EF Rejected",
        display_name="Both Reasons",
    )

    conn = get_connection(taxops_db_path)
    items, total, counts = fetch_needs_attention(conn, 2025, limit=None)
    conn.close()
    assert total == 1
    assert items[0]["reason"] == "ef_rejected"
    assert counts["ef_rejected"] == 1
    assert counts["client_contact"] == 0
    assert counts["stale_processing"] == 0


def test_receptionist_dashboard_sees_attention(client, taxops_db_path):
    from db import get_connection

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR REPLACE INTO auth_users
          (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'receptionist', 1, '2025-01-01T00:00:00Z')
        """,
        ("__recv_attn__", generate_password_hash("pass"), "Recv Attn"),
    )
    conn.commit()
    conn.close()

    _seed_client_return(
        taxops_db_path,
        client_id=531,
        return_id=5131,
        log_number="5131",
        status="REJECTED",
        contact_status="follow_up_needed",
        display_name="Desk Visible",
    )

    client.post(
        "/login",
        data={"username": "__recv_attn__", "password": "pass"},
        follow_redirects=True,
    )
    rv = client.get("/?year=2025")
    assert rv.status_code == 200
    body = rv.get_data(as_text=True)
    assert "Needs attention" in body or "Requieren atención" in body
    assert "5131" in body
