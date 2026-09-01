"""Tests for Drake client-export engagement sync."""

from __future__ import annotations


def test_engagement_to_drake_status_raw():
    from engagement_status_rules import (
        ENGAGEMENT_EFILE_ACCEPTED,
        ENGAGEMENT_EXTENSION_EFILE_ACCEPTED,
        engagement_to_drake_status_raw,
    )

    assert engagement_to_drake_status_raw(ENGAGEMENT_EFILE_ACCEPTED) == "EF Accepted"
    assert engagement_to_drake_status_raw(ENGAGEMENT_EXTENSION_EFILE_ACCEPTED) == "EF Ext Accepted"
    assert engagement_to_drake_status_raw("") is None
    assert engagement_to_drake_status_raw("E-File Rejected") == "EF Rejected"


def test_normalize_client_export_entity_row():
    from client_export_sync import normalize_client_export_row

    norm = normalize_client_export_row(
        {
            "Display Name": "D P DENTAL & BILLER MANAGEMENT INC",
            "Client Type": "Business 1120-S",
            "Tax ID (Last 4)": "0189",
            "Engagement Status": "E-File Accepted",
            "Owner": "Moises Bustos",
        },
        tax_year=2025,
    )
    assert norm is not None
    assert norm["clients"]["last_name"] == "D P DENTAL & BILLER MANAGEMENT INC"
    assert norm["clients"]["ssn_last4"] == "0189"
    assert norm["returns"]["drake_status_raw"] == "EF Accepted"
    assert norm["return_forms"]["form_1120s"] == 1
