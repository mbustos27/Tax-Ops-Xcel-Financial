"""
Import deduplication tests — Parts 1-8 of the import-system fix.

Covers:
  - Re-import does not create duplicate clients or returns
  - Merge fills empty fields but does not overwrite manual data
  - _deduplicate_existing_records cleans historical duplicates correctly
  - Drake status normalization
  - ImportResult counts correctly
  - Business-name (NULL first_name) upsert does not create duplicates
"""

from __future__ import annotations

import sqlite3
import time
from pathlib import Path

import pytest

from db import get_connection, init_db, _deduplicate_existing_records
from utils import ImportResult, now


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fresh_conn(tmp_path: Path) -> sqlite3.Connection:
    path = str(tmp_path / "test.db")
    conn = get_connection(path)
    init_db(conn)
    return conn


def _insert_client(conn, last, first=None, ssn=None, phone=None, address=None) -> int:
    ts = now()
    cur = conn.execute(
        """INSERT INTO clients (last_name, first_name, ssn_last4, taxpayer_phone, address, created_at, updated_at)
           VALUES (?,?,?,?,?,?,?)""",
        (last, first, ssn, phone, address, ts, ts),
    )
    conn.commit()
    return cur.lastrowid


def _insert_return(conn, client_id, tax_year, status="PROCESSING", processor=None) -> int:
    ts = now()
    cur = conn.execute(
        """INSERT INTO returns (client_id, tax_year, client_status, processor, created_at, updated_at)
           VALUES (?,?,?,?,?,?)""",
        (client_id, tax_year, status, processor, ts, ts),
    )
    conn.commit()
    return cur.lastrowid


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestDrakeUpsertClient:
    """_upsert_client in drake_importer does not create duplicates on reimport."""

    def test_reimport_does_not_create_duplicate_client(self, tmp_path):
        """Importing the same client twice keeps exactly one row."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client

        data = {"last_name": "GARCIA", "first_name": "ANA", "ssn_last4": "1234"}
        # First import
        cid1, created1, _ = _upsert_client(conn, data, None)
        assert created1 is True

        # Second import — must match, not insert
        cid2, created2, _ = _upsert_client(conn, data, None)
        assert created2 is False
        assert cid1 == cid2

        total = conn.execute("SELECT COUNT(*) FROM clients WHERE last_name='GARCIA'").fetchone()[0]
        assert total == 1, "Re-import must not create a second client row"

    def test_reimport_does_not_create_duplicate_return(self, tmp_path):
        """Importing the same client+year combination twice keeps exactly one return."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client, _upsert_return

        data_c = {"last_name": "FLORES", "first_name": "PEDRO", "ssn_last4": "5678"}
        cid, _, _ = _upsert_client(conn, data_c, None)

        data_r = {"tax_year": 2025, "client_status": "PROCESSING", "processor": None,
                  "intake_date": None, "logout_date": None, "updated_date": None,
                  "efile_date": None, "ack_date": None, "is_extension": None, "drake_status_raw": None}
        rid1, created1, _, _, _ = _upsert_return(conn, cid, data_r, None)
        assert created1 is True

        rid2, created2, _, _, _ = _upsert_return(conn, cid, data_r, None)
        assert created2 is False
        assert rid1 == rid2

        total = conn.execute(
            "SELECT COUNT(*) FROM returns WHERE client_id=? AND tax_year=2025", (cid,)
        ).fetchone()[0]
        assert total == 1, "Re-import must not create a second return row"

    def test_business_name_null_first_name_no_duplicate(self, tmp_path):
        """Business clients with NULL first_name never create duplicates across imports."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client

        biz = {"last_name": "SANDOVAL AUTO SERVICE & TOW", "first_name": None, "ssn_last4": None}
        cid1, c1, _ = _upsert_client(conn, biz, None)
        assert c1 is True

        cid2, c2, _ = _upsert_client(conn, biz, None)
        assert c2 is False
        assert cid1 == cid2

        total = conn.execute(
            "SELECT COUNT(*) FROM clients WHERE last_name='SANDOVAL AUTO SERVICE & TOW'"
        ).fetchone()[0]
        assert total == 1


class TestManualLogUpsertClient:
    """_upsert_client in importer.py handles NULL first_name correctly."""

    def test_business_name_no_duplicate_via_manual_log_importer(self, tmp_path):
        """importer._upsert_client must not create a second row for business with NULL first_name."""
        conn = _fresh_conn(tmp_path)
        from importer import _upsert_client

        data = {
            "last_name": "TIBIS MISSION", "first_name": None,
            "display_name": None, "referral_flag": None,
            "referred_by": None, "spouse_first_name": None, "spouse_last_name": None,
        }
        cid1, c1, _ = _upsert_client(conn, data, None)
        assert c1 is True

        cid2, c2, _ = _upsert_client(conn, data, None)
        assert c2 is False
        assert cid1 == cid2

        total = conn.execute(
            "SELECT COUNT(*) FROM clients WHERE last_name='TIBIS MISSION'"
        ).fetchone()[0]
        assert total == 1


class TestMergeBehavior:
    """Import fills gaps; never overwrites manually entered data."""

    def test_merge_does_not_overwrite_manual_data(self, tmp_path):
        """When a return already has a processor set, reimport must not overwrite it."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client, _upsert_return

        data_c = {"last_name": "SMITH", "first_name": "JOHN", "ssn_last4": "9999"}
        cid, _, _ = _upsert_client(conn, data_c, None)

        # Create return manually with processor already set
        _insert_return(conn, cid, 2025, status="PROCESSING", processor="LUCILA YANEZ")

        # Import tries to fill processor — must not overwrite the existing manual value
        data_r = {
            "tax_year": 2025, "client_status": None,
            "processor": "NEW PREPARER",   # import value that must be ignored
            "intake_date": None, "logout_date": None, "updated_date": None,
            "efile_date": None, "ack_date": None, "is_extension": None, "drake_status_raw": None,
        }
        rid, created, _, _, _ = _upsert_return(conn, cid, data_r, None)
        assert created is False

        processor = conn.execute(
            "SELECT processor FROM returns WHERE id=?", (rid,)
        ).fetchone()["processor"]
        assert processor == "LUCILA YANEZ", "Manual processor must not be overwritten by import"

    def test_merge_fills_empty_fields(self, tmp_path):
        """Import fills NULL fields on an existing return."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client, _upsert_return

        data_c = {"last_name": "JOHNSON", "first_name": "MARY", "ssn_last4": "4321"}
        cid, _, _ = _upsert_client(conn, data_c, None)

        # Return without ack_date
        _insert_return(conn, cid, 2025)

        data_r = {
            "tax_year": 2025, "client_status": None, "processor": None,
            "intake_date": None, "logout_date": None,
            "updated_date": "2025-04-15",
            "efile_date": None, "ack_date": "2025-04-20",
            "is_extension": None, "drake_status_raw": None,
        }
        rid, created, _, _, after = _upsert_return(conn, cid, data_r, None)
        assert created is False
        assert after["ack_date"] == "2025-04-20", "Null ack_date must be filled from import"


class TestDeduplicateExistingRecords:
    """_deduplicate_existing_records cleans historical duplicates."""

    def test_dedup_cleanup_keeps_client_with_most_returns(self, tmp_path):
        """After dedup, only one client row remains; the one with more returns is kept."""
        conn = _fresh_conn(tmp_path)

        # Two duplicate clients with same natural key
        cid1 = _insert_client(conn, "REMG INC")
        cid2 = _insert_client(conn, "REMG INC")
        # cid1 has 2 returns, cid2 has 1 — dedup should keep cid1
        _insert_return(conn, cid1, 2023)
        _insert_return(conn, cid1, 2024)
        _insert_return(conn, cid2, 2022)

        removed = _deduplicate_existing_records(conn)
        conn.commit()

        assert removed == 1
        remaining = conn.execute(
            "SELECT id FROM clients WHERE last_name='REMG INC'"
        ).fetchall()
        assert len(remaining) == 1
        assert remaining[0]["id"] == cid1

    def test_dedup_cleanup_reassigns_non_conflicting_returns(self, tmp_path):
        """Returns from discarded client are moved to kept client when no year conflict."""
        conn = _fresh_conn(tmp_path)

        cid1 = _insert_client(conn, "LIONS FUMIGATION INC")
        cid2 = _insert_client(conn, "LIONS FUMIGATION INC")
        _insert_return(conn, cid1, 2025)
        _insert_return(conn, cid2, 2024)  # different year — should be reassigned to kept

        _deduplicate_existing_records(conn)
        conn.commit()

        total_clients = conn.execute(
            "SELECT COUNT(*) FROM clients WHERE last_name='LIONS FUMIGATION INC'"
        ).fetchone()[0]
        assert total_clients == 1

        # Find the surviving client (tie broken by highest id → cid2 survives)
        kept_row = conn.execute(
            "SELECT id FROM clients WHERE last_name='LIONS FUMIGATION INC'"
        ).fetchone()
        kept_id = kept_row["id"]

        ret_years = {
            r["tax_year"]
            for r in conn.execute(
                "SELECT tax_year FROM returns WHERE client_id=?", (kept_id,)
            ).fetchall()
        }
        assert len(ret_years) == 2, "Both returns must survive under the kept client"
        assert 2024 in ret_years, "2024 return must be reassigned to kept client"
        assert 2025 in ret_years, "2025 return must also be present on kept client"

    def test_dedup_cleanup_merges_non_null_fields(self, tmp_path):
        """Discarded client's non-null contact fields are merged into kept client."""
        conn = _fresh_conn(tmp_path)

        cid1 = _insert_client(conn, "WEST COAST LEGAL MANAGEMENT INC", phone="555-1234")
        cid2 = _insert_client(conn, "WEST COAST LEGAL MANAGEMENT INC", address="123 Main St")
        # Give cid1 more returns so it is kept
        _insert_return(conn, cid1, 2025)
        _insert_return(conn, cid1, 2024)
        _insert_return(conn, cid2, 2023)

        _deduplicate_existing_records(conn)
        conn.commit()

        kept = dict(conn.execute(
            "SELECT taxpayer_phone, address FROM clients WHERE last_name='WEST COAST LEGAL MANAGEMENT INC'"
        ).fetchone())
        assert kept["taxpayer_phone"] == "555-1234"
        assert kept["address"] == "123 Main St", "Address from discarded client must be merged"

    def test_dedup_is_idempotent(self, tmp_path):
        """Running dedup twice with no duplicates removes 0 rows and raises no error."""
        conn = _fresh_conn(tmp_path)
        _insert_client(conn, "ORMA SERVICES INC")

        r1 = _deduplicate_existing_records(conn)
        r2 = _deduplicate_existing_records(conn)
        assert r1 == 0
        assert r2 == 0


class TestDrakeStatusNormalization:
    """DRAKE_STATUS_MAP covers all status codes found in the real CSMDATA.csv."""

    @pytest.mark.parametrize("raw,expected", [
        ("EF Accepted",        "LOG OUT"),
        ("EF Ext Accepted",    "LOG OUT"),
        ("EF Pending",         "PROCESSING"),
        ("EF Rejected",        "PROCESSING"),
        ("In Progress",        "PROCESSING"),
        ("Printed",            "PICKUP"),
        ("Updated From 2024",  "PROCESSING"),
    ])
    def test_drake_status_normalization(self, raw, expected):
        """Every status value found in the real CSMDATA.csv maps to a known TaxOps status."""
        from config import DRAKE_STATUS_MAP
        result = DRAKE_STATUS_MAP.get(raw.upper(), "PROCESSING")
        assert result == expected, f"Status '{raw}' expected '{expected}', got '{result}'"


class TestImportResultCounts:
    """ImportResult tracks counts correctly."""

    def test_import_result_has_required_fields(self):
        """ImportResult carries all required metadata fields."""
        r = ImportResult(source="drake", filename="CSMDATA.csv")
        assert r.source == "drake"
        assert r.filename == "CSMDATA.csv"
        assert r.errors == []
        assert r.duration_seconds == 0.0
        assert r.created_clients == 0
        assert r.created_returns == 0

    def test_import_result_counts_correctly(self, tmp_path):
        """After importing 3 new clients and touching 1 existing, counts are accurate."""
        conn = _fresh_conn(tmp_path)
        from drake_importer import _upsert_client

        # Pre-seed one existing client
        _insert_client(conn, "EXISTING", "CLIENT", ssn="0001")

        result = ImportResult(source="drake", filename="test.csv")
        t0 = time.monotonic()

        new_clients = [
            {"last_name": "NEW1", "first_name": "A", "ssn_last4": "0010"},
            {"last_name": "NEW2", "first_name": "B", "ssn_last4": "0020"},
            {"last_name": "NEW3", "first_name": "C", "ssn_last4": "0030"},
        ]
        existing_again = {"last_name": "EXISTING", "first_name": "CLIENT", "ssn_last4": "0001"}

        for d in new_clients:
            _, created, _ = _upsert_client(conn, d, None)
            if created:
                result.created_clients += 1
            else:
                result.updated_clients += 1

        _, created, updated = _upsert_client(conn, existing_again, None)
        if created:
            result.created_clients += 1
        else:
            result.updated_clients += 1

        result.duration_seconds = time.monotonic() - t0

        assert result.created_clients == 3
        assert result.updated_clients == 1
        assert result.duration_seconds >= 0.0


class TestUniqueConstraint:
    """idx_returns_unique_client_year prevents duplicate returns on a fresh DB."""

    def test_unique_constraint_blocks_duplicate_return(self, tmp_path):
        """Inserting two returns for the same client+year raises IntegrityError."""
        conn = _fresh_conn(tmp_path)
        cid = _insert_client(conn, "DUPE TEST", "USER")
        _insert_return(conn, cid, 2025, status="PROCESSING")

        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO returns (client_id, tax_year, client_status, created_at, updated_at) VALUES (?,?,?,?,?)",
                (cid, 2025, "PROCESSING", now(), now()),
            )
            conn.commit()

    def test_unique_constraint_allows_cancelled_duplicate(self, tmp_path):
        """CANCELLED returns are excluded from the unique constraint (partial index)."""
        conn = _fresh_conn(tmp_path)
        cid = _insert_client(conn, "CANCELLED TEST", "USER")
        # Insert a CANCELLED return for 2025
        _insert_return(conn, cid, 2025, status="CANCELLED")

        # A second PROCESSING return for same year must succeed (CANCELLED is excluded)
        try:
            _insert_return(conn, cid, 2025, status="PROCESSING")
        except sqlite3.IntegrityError:
            pytest.fail(
                "CANCELLED return should not block a new PROCESSING return for the same year"
            )
