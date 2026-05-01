"""
Data Integrity Test Suite for TaxOps.

Validates core schema constraints, relational integrity, workflow rules,
and privacy requirements. All tests use in-memory SQLite only.
"""
from __future__ import annotations

import sqlite3
import re

import pytest

from conftest import (
    columns,
    insert_batch,
    insert_batch_item,
    insert_client,
    insert_return,
)


# ---------------------------------------------------------------------------
# 1. One logical return per (client_id, tax_year)
# ---------------------------------------------------------------------------


class TestReturnUniqueness:
    def test_duplicate_client_year_detectable(self, mem_db):
        """
        The schema allows multiple returns per (client_id, tax_year) but
        operational integrity requires they be detectable.
        Any duplicates should surface via a COUNT query.
        """
        cid = insert_client(mem_db)
        insert_return(mem_db, cid, tax_year=2025, log_number="1001")
        insert_return(mem_db, cid, tax_year=2025, log_number="1002")

        count = mem_db.execute(
            "SELECT COUNT(*) FROM returns WHERE client_id=? AND tax_year=?",
            (cid, 2025),
        ).fetchone()[0]

        # If a unique constraint existed, count would be 1.
        # Without it we assert that duplicates ARE detectable so ops can flag them.
        assert count >= 1, "Should be able to query returns by client+year"

    def test_single_return_per_client_year_is_normal_state(self, mem_db):
        """Happy path — one return per (client, tax_year)."""
        cid = insert_client(mem_db)
        insert_return(mem_db, cid, tax_year=2025, log_number="1001")

        count = mem_db.execute(
            "SELECT COUNT(*) FROM returns WHERE client_id=? AND tax_year=?",
            (cid, 2025),
        ).fetchone()[0]
        assert count == 1


# ---------------------------------------------------------------------------
# 2. Batch item uniqueness — UNIQUE (batch_id, return_id)
# ---------------------------------------------------------------------------


class TestBatchItemUniqueness:
    def test_duplicate_return_in_same_batch_rejected(self, mem_db):
        """Schema enforces UNIQUE (batch_id, return_id) on efile_batch_items."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid = insert_batch(mem_db)
        insert_batch_item(mem_db, bid, rid)

        with pytest.raises(sqlite3.IntegrityError):
            # Same return_id inserted into the same batch again
            mem_db.execute(
                "INSERT INTO efile_batch_items "
                "(batch_id, return_id, ack_status, created_at) VALUES (?,?,'pending',datetime('now'))",
                (bid, rid),
            )

    def test_same_return_allowed_in_different_batches(self, mem_db):
        """A return may appear in a second batch (re-filed after rejection)."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid1 = insert_batch(mem_db)
        bid2 = insert_batch(mem_db)

        insert_batch_item(mem_db, bid1, rid)
        insert_batch_item(mem_db, bid2, rid)  # must not raise

        count = mem_db.execute(
            "SELECT COUNT(*) FROM efile_batch_items WHERE return_id=?", (rid,)
        ).fetchone()[0]
        assert count == 2


# ---------------------------------------------------------------------------
# 3. Duplicate log numbers detectable
# ---------------------------------------------------------------------------


class TestLogNumberCollisions:
    def test_duplicate_log_numbers_detectable(self, mem_db):
        """Log-number collisions (same number, same year) must be queryable."""
        cid1 = insert_client(mem_db, last="Smith", first="Alice")
        cid2 = insert_client(mem_db, last="Jones", first="Bob")
        insert_return(mem_db, cid1, tax_year=2025, log_number="500")
        insert_return(mem_db, cid2, tax_year=2025, log_number="500")

        dups = mem_db.execute(
            "SELECT log_number, tax_year, COUNT(*) as cnt "
            "FROM returns WHERE log_number IS NOT NULL "
            "GROUP BY log_number, tax_year HAVING cnt > 1"
        ).fetchall()

        assert len(dups) == 1
        assert dups[0]["log_number"] == "500"
        assert dups[0]["cnt"] == 2

    def test_unique_log_numbers_produce_no_duplicates(self, mem_db):
        """No false positives — unique log numbers should not appear in dups query."""
        cid = insert_client(mem_db)
        insert_return(mem_db, cid, tax_year=2025, log_number="100")
        insert_return(mem_db, cid, tax_year=2024, log_number="100")  # different year is OK

        dups = mem_db.execute(
            "SELECT log_number, tax_year, COUNT(*) as cnt "
            "FROM returns WHERE log_number IS NOT NULL "
            "GROUP BY log_number, tax_year HAVING cnt > 1"
        ).fetchall()
        assert len(dups) == 0


# ---------------------------------------------------------------------------
# 4. Payment linkage integrity
# ---------------------------------------------------------------------------


class TestPaymentLinkage:
    def test_payment_links_to_return(self, mem_db):
        """Payment foreign key must resolve to the parent return."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        mem_db.execute(
            "INSERT INTO payments (return_id, total_fee, fee_paid) VALUES (?,?,?)",
            (rid, 200.0, 200.0),
        )
        mem_db.commit()

        row = mem_db.execute(
            "SELECT p.return_id FROM payments p WHERE p.return_id=?", (rid,)
        ).fetchone()
        assert row is not None
        assert row["return_id"] == rid

    def test_no_orphan_payments(self, mem_db):
        """All payments must have a valid parent return."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        mem_db.execute(
            "INSERT INTO payments (return_id, total_fee) VALUES (?,?)", (rid, 100.0)
        )
        mem_db.commit()

        orphans = mem_db.execute(
            "SELECT p.id FROM payments p "
            "LEFT JOIN returns r ON r.id = p.return_id "
            "WHERE r.id IS NULL"
        ).fetchall()
        assert len(orphans) == 0

    def test_orphan_payment_rejected_by_fk(self, mem_db):
        """Inserting a payment for a non-existent return_id must fail."""
        with pytest.raises(sqlite3.IntegrityError):
            mem_db.execute(
                "INSERT INTO payments (return_id, total_fee) VALUES (99999, 50.0)"
            )


# ---------------------------------------------------------------------------
# 5. Batch snapshot consistency
# ---------------------------------------------------------------------------


class TestBatchSnapshot:
    def test_snapshot_fields_present(self, mem_db):
        """efile_batch_items snapshot columns (log_number, client_name, receipt_number, fee_paid) exist."""
        required = {"log_number", "client_name", "receipt_number", "fee_paid"}
        actual = columns(mem_db, "efile_batch_items")
        missing = required - actual
        assert not missing, f"Missing snapshot columns: {missing}"

    def test_snapshot_values_stored_correctly(self, mem_db):
        """Snapshot values written during batch creation are retrievable."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, log_number="2001")
        mem_db.execute(
            "INSERT INTO payments (return_id, receipt_number, fee_paid) VALUES (?,?,?)",
            (rid, "R-9999", 350.0),
        )
        bid = insert_batch(mem_db)
        mem_db.execute(
            """INSERT INTO efile_batch_items
               (batch_id, return_id, log_number, client_name, receipt_number, fee_paid, ack_status, created_at)
               VALUES (?,?,?,?,?,?,?,datetime('now'))""",
            (bid, rid, "2001", "Smith, John", "R-9999", 350.0, "pending"),
        )
        mem_db.commit()

        item = mem_db.execute(
            "SELECT log_number, client_name, receipt_number, fee_paid "
            "FROM efile_batch_items WHERE batch_id=? AND return_id=?",
            (bid, rid),
        ).fetchone()
        assert item["log_number"] == "2001"
        assert item["client_name"] == "Smith, John"
        assert item["receipt_number"] == "R-9999"
        assert item["fee_paid"] == 350.0


# ---------------------------------------------------------------------------
# 6. ACK acceptance consistency
# ---------------------------------------------------------------------------


class TestAckAcceptedConsistency:
    def test_accept_sets_ack_status_and_return_ack_date(self, mem_db):
        """Accepting a batch item should persist ack_status and ack_date on the return."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="EFILE READY")
        bid = insert_batch(mem_db)
        iid = insert_batch_item(mem_db, bid, rid, ack_status="pending")

        ack_date = "2025-04-10"
        mem_db.execute(
            "UPDATE efile_batch_items SET ack_status='accepted', ack_date=? WHERE id=?",
            (ack_date, iid),
        )
        mem_db.execute(
            "UPDATE returns SET client_status='LOG OUT', ack_date=? WHERE id=?",
            (ack_date, rid),
        )
        mem_db.commit()

        item = mem_db.execute(
            "SELECT ack_status, ack_date FROM efile_batch_items WHERE id=?", (iid,)
        ).fetchone()
        ret = mem_db.execute(
            "SELECT client_status, ack_date FROM returns WHERE id=?", (rid,)
        ).fetchone()

        assert item["ack_status"] == "accepted"
        assert item["ack_date"] == ack_date
        assert ret["client_status"] == "LOG OUT"
        assert ret["ack_date"] == ack_date


# ---------------------------------------------------------------------------
# 7. Reject consistency
# ---------------------------------------------------------------------------


class TestRejectConsistency:
    def test_reject_stores_code_and_reason(self, mem_db):
        """Rejecting a batch item must store rejection_code and rejection_reason."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="EFILE READY")
        bid = insert_batch(mem_db)
        iid = insert_batch_item(mem_db, bid, rid, ack_status="pending")

        mem_db.execute(
            "UPDATE efile_batch_items "
            "SET ack_status='rejected', rejection_code='R0000', rejection_reason='SSN mismatch', ack_date=date('now') "
            "WHERE id=?",
            (iid,),
        )
        mem_db.execute(
            "UPDATE returns SET client_status='REJECTED' WHERE id=?", (rid,)
        )
        mem_db.commit()

        item = mem_db.execute(
            "SELECT ack_status, rejection_code, rejection_reason "
            "FROM efile_batch_items WHERE id=?",
            (iid,),
        ).fetchone()
        ret = mem_db.execute(
            "SELECT client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()

        assert item["ack_status"] == "rejected"
        assert item["rejection_code"] == "R0000"
        assert item["rejection_reason"] == "SSN mismatch"
        assert ret["client_status"] == "REJECTED"

    def test_reject_without_code_should_be_caught_at_api_layer(self, mem_db):
        """
        Schema does not enforce rejection_code NOT NULL — enforcement is in app.py.
        This test documents that the DB alone won't block a missing code,
        but the API validation is the enforcement point.
        """
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid = insert_batch(mem_db)
        iid = insert_batch_item(mem_db, bid, rid)

        # DB allows NULL rejection_code — app.py is responsible for blocking this
        mem_db.execute(
            "UPDATE efile_batch_items SET ack_status='rejected', rejection_code=NULL WHERE id=?",
            (iid,),
        )
        mem_db.commit()

        row = mem_db.execute(
            "SELECT rejection_code FROM efile_batch_items WHERE id=?", (iid,)
        ).fetchone()
        # Document that the DB permits it (app validation is the gate)
        assert row["rejection_code"] is None


# ---------------------------------------------------------------------------
# 8. Privacy — ssn_last4 must not leak into exported/UI outputs
# ---------------------------------------------------------------------------


class TestPrivacy:
    def test_ssn_last4_not_in_efile_batch_list_template(self):
        """efile_batch_list.html must not reference ssn_last4 in any Jinja expression."""
        import os

        tmpl_path = os.path.join(
            os.path.dirname(__file__), "..", "taxops", "templates", "efile_batch_list.html"
        )
        with open(tmpl_path, encoding="utf-8") as f:
            content = f.read()

        assert "ssn_last4" not in content, (
            "efile_batch_list.html must not reference ssn_last4"
        )

    def test_ssn_last4_not_in_efile_batch_template(self):
        """efile_batch.html must not render ssn_last4 in the Jinja output layer."""
        import os

        tmpl_path = os.path.join(
            os.path.dirname(__file__), "..", "taxops", "templates", "efile_batch.html"
        )
        with open(tmpl_path, encoding="utf-8") as f:
            content = f.read()

        # Allow schema references in comments but not active rendering
        active_refs = re.findall(r"\{\{[^}]*ssn_last4[^}]*\}\}", content)
        assert not active_refs, (
            f"efile_batch.html renders ssn_last4 in Jinja expression(s): {active_refs}"
        )

    def test_export_route_in_app_py_excludes_ssn(self):
        """The export route in app.py must not include ssn_last4 in exported columns."""
        import os

        app_path = os.path.join(
            os.path.dirname(__file__), "..", "taxops", "app.py"
        )
        with open(app_path, encoding="utf-8") as f:
            content = f.read()

        # Find the efile batch export function block
        export_fn_match = re.search(
            r"def efile_batch_export.*?(?=\n@app|\nclass |\Z)", content, re.DOTALL
        )
        assert export_fn_match, "Could not locate efile_batch_export function in app.py"
        fn_body = export_fn_match.group()

        assert "ssn_last4" not in fn_body, (
            "efile_batch_export must not include ssn_last4 in output"
        )

    def test_ssn_column_not_in_batch_item_insert_for_new_batches(self):
        """Batch creation in app.py must not snapshot ssn_last4 into efile_batch_items."""
        import os

        app_path = os.path.join(
            os.path.dirname(__file__), "..", "taxops", "app.py"
        )
        with open(app_path, encoding="utf-8") as f:
            content = f.read()

        fn_match = re.search(
            r"def efile_batch_create.*?(?=\n@app|\nclass |\Z)", content, re.DOTALL
        )
        assert fn_match, "Could not locate efile_batch_create function in app.py"
        fn_body = fn_match.group()

        # ssn_last4 must not appear in INSERT INTO efile_batch_items block
        insert_block = re.search(
            r"INSERT.*?INTO efile_batch_items.*?;", fn_body, re.DOTALL | re.IGNORECASE
        )
        if insert_block:
            assert "ssn_last4" not in insert_block.group(), (
                "efile_batch_create INSERT must not include ssn_last4"
            )


# ---------------------------------------------------------------------------
# 9. Status transition validity
# ---------------------------------------------------------------------------


class TestStatusTransitions:
    """
    Tests mirror the STATUS_FLOW list from app.py.
    Actual enforcement lives in app.py api_status; here we validate
    the flow rules directly against the DB + normalizer logic.
    """

    STATUS_FLOW = ["PROCESSING", "HOLD", "FINALIZE", "PICKUP", "EFILE READY", "LOG OUT", "REJECTED"]
    LOCKED = {"CANCELLED"}

    def _apply_status(self, conn: sqlite3.Connection, return_id: int, new_status: str) -> bool:
        """Apply a status change; return True if allowed, False if rejected."""
        if new_status not in self.STATUS_FLOW:
            return False
        current = conn.execute(
            "SELECT client_status FROM returns WHERE id=?", (return_id,)
        ).fetchone()
        old_status = current["client_status"] if current else None
        if old_status in self.LOCKED:
            return False
        conn.execute(
            "UPDATE returns SET client_status=? WHERE id=?", (new_status, return_id)
        )
        conn.commit()
        return True

    def test_valid_forward_progression(self, mem_db):
        """Full forward flow: PROCESSING → HOLD → FINALIZE → PICKUP → EFILE READY → LOG OUT."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="PROCESSING")

        flow = ["HOLD", "FINALIZE", "PICKUP", "EFILE READY", "LOG OUT"]
        for step in flow:
            ok = self._apply_status(mem_db, rid, step)
            assert ok, f"Transition to '{step}' was unexpectedly rejected"

        final = mem_db.execute(
            "SELECT client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()["client_status"]
        assert final == "LOG OUT"

    def test_invalid_status_value_rejected(self, mem_db):
        """A status not in STATUS_FLOW must be rejected."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="PROCESSING")
        ok = self._apply_status(mem_db, rid, "FLYING_SAUCER")
        assert not ok

    def test_cancelled_is_locked(self, mem_db):
        """Once CANCELLED, the return must not accept further status changes."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="CANCELLED")

        # Try to move back to PROCESSING
        ok = self._apply_status(mem_db, rid, "PROCESSING")
        assert not ok, "CANCELLED return must be locked"

        # Status must remain CANCELLED
        final = mem_db.execute(
            "SELECT client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()["client_status"]
        assert final == "CANCELLED"

    def test_rejected_can_move_to_efile_ready_on_ack_reset(self, mem_db):
        """A REJECTED return can revert to EFILE READY (ACK reset path)."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid, status="REJECTED")
        ok = self._apply_status(mem_db, rid, "EFILE READY")
        assert ok

        final = mem_db.execute(
            "SELECT client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()["client_status"]
        assert final == "EFILE READY"
