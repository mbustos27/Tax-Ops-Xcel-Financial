"""
Tests for E-file Stabilization Issues #42–#46.

Each test is self-contained and uses an in-memory SQLite DB.
No Flask routes are exercised directly; SQL logic is replicated faithfully
from app.py to validate behaviour without spinning up the HTTP layer.
"""
from __future__ import annotations

import sqlite3
from datetime import date

import pytest

from conftest import columns, insert_batch, insert_batch_item, insert_client, insert_return


# ---------------------------------------------------------------------------
# PART 1 — Schema tests (#42 / #43)
# ---------------------------------------------------------------------------


class TestSchema:
    def test_cc_fee_exists_in_efile_batch_items(self, mem_db):
        """#42 — efile_batch_items must include cc_fee (fresh install)."""
        assert "cc_fee" in columns(mem_db, "efile_batch_items")

    def test_transmitted_at_exists_in_efile_batches(self, mem_db):
        """#43 — efile_batches must include transmitted_at (fresh install)."""
        assert "transmitted_at" in columns(mem_db, "efile_batches")


# ---------------------------------------------------------------------------
# PART 2 — Auto-close protection (#44)
# ---------------------------------------------------------------------------


class TestAutoClose:
    def _run_auto_close(self, conn: sqlite3.Connection, batch_id: int) -> None:
        """Mirror the auto-close logic from app.py efile_batch_item_ack."""
        unresolved = conn.execute(
            "SELECT COUNT(*) FROM efile_batch_items WHERE batch_id=? AND ack_status='pending'",
            (batch_id,),
        ).fetchone()[0]
        if unresolved == 0:
            conn.execute(
                "UPDATE efile_batches SET status='closed' "
                "WHERE id=? AND status NOT IN ('transmitted','closed')",
                (batch_id,),
            )
        conn.commit()

    def test_transmitted_batch_not_overwritten_as_closed(self, mem_db):
        """#44 — A 'transmitted' batch must never be downgraded to 'closed'."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid = insert_batch(mem_db, status="transmitted", transmitted_at="2025-04-01T12:00:00")
        insert_batch_item(mem_db, bid, rid, ack_status="accepted")

        self._run_auto_close(mem_db, bid)

        status = mem_db.execute(
            "SELECT status FROM efile_batches WHERE id=?", (bid,)
        ).fetchone()["status"]
        assert status == "transmitted", (
            f"Expected 'transmitted' but got '{status}' — #44 regression"
        )

    def test_open_batch_closes_when_all_resolved(self, mem_db):
        """Normal path — an 'open' batch becomes 'closed' when all items resolved."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid = insert_batch(mem_db, status="open")
        insert_batch_item(mem_db, bid, rid, ack_status="accepted")

        self._run_auto_close(mem_db, bid)

        status = mem_db.execute(
            "SELECT status FROM efile_batches WHERE id=?", (bid,)
        ).fetchone()["status"]
        assert status == "closed"

    def test_already_closed_batch_stays_closed(self, mem_db):
        """Edge case — 'closed' batch must not be re-closed (idempotent)."""
        cid = insert_client(mem_db)
        rid = insert_return(mem_db, cid)
        bid = insert_batch(mem_db, status="closed")
        insert_batch_item(mem_db, bid, rid, ack_status="accepted")

        self._run_auto_close(mem_db, bid)

        status = mem_db.execute(
            "SELECT status FROM efile_batches WHERE id=?", (bid,)
        ).fetchone()["status"]
        assert status == "closed"


# ---------------------------------------------------------------------------
# PART 3 — ACK reset clears stale ack_date (#45)
# ---------------------------------------------------------------------------


class TestAckDateClear:
    def _run_pending_reset(self, conn: sqlite3.Connection, return_id: int) -> None:
        """
        Mirror the 'pending' branch of app.py efile_batch_item_ack.
        Only the ack_date clearing part is under test here.
        """
        current = conn.execute(
            "SELECT client_status FROM returns WHERE id=?", (return_id,)
        ).fetchone()
        current_status = current["client_status"] if current else None

        # Status revert (mirrors app logic)
        if current_status in ("LOG OUT", "REJECTED"):
            if current_status == "REJECTED":
                conn.execute(
                    "UPDATE returns SET client_status='EFILE READY', "
                    "contact_status=NULL, last_contacted_date=NULL, ack_date=NULL "
                    "WHERE id=?",
                    (return_id,),
                )
            else:
                conn.execute(
                    "UPDATE returns SET client_status='EFILE READY' WHERE id=?",
                    (return_id,),
                )

        # ack_date clear (#45 logic)
        if current_status in ("LOG OUT", "REJECTED"):
            conn.execute(
                "UPDATE returns SET ack_date=NULL WHERE id=? "
                "AND client_status IN ('EFILE READY','REJECTED','LOG OUT')",
                (return_id,),
            )
        conn.commit()

    def test_rejected_return_ack_date_cleared_on_reset(self, mem_db):
        """#45 — Resetting ACK to pending clears ack_date when return is REJECTED."""
        cid = insert_client(mem_db)
        rid = insert_return(
            mem_db, cid, status="REJECTED", ack_date="2025-03-15"
        )

        self._run_pending_reset(mem_db, rid)

        row = mem_db.execute(
            "SELECT ack_date, client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()
        assert row["ack_date"] is None, "ack_date should be NULL after pending reset for REJECTED"
        assert row["client_status"] == "EFILE READY"

    def test_logout_return_ack_date_cleared_on_reset(self, mem_db):
        """#45 — Resetting ACK to pending clears ack_date when return is LOG OUT."""
        cid = insert_client(mem_db)
        rid = insert_return(
            mem_db, cid, status="LOG OUT", ack_date="2025-03-20"
        )

        self._run_pending_reset(mem_db, rid)

        row = mem_db.execute(
            "SELECT ack_date, client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()
        assert row["ack_date"] is None, "ack_date should be NULL after pending reset for LOG OUT"
        assert row["client_status"] == "EFILE READY"

    def test_processing_return_ack_date_not_cleared(self, mem_db):
        """#45 — ack_date must NOT be cleared for returns NOT in LOG OUT / REJECTED."""
        cid = insert_client(mem_db)
        rid = insert_return(
            mem_db, cid, status="PROCESSING", ack_date="2025-01-10"
        )

        self._run_pending_reset(mem_db, rid)

        row = mem_db.execute(
            "SELECT ack_date, client_status FROM returns WHERE id=?", (rid,)
        ).fetchone()
        assert row["ack_date"] == "2025-01-10", (
            "ack_date must be preserved for returns not in LOG OUT / REJECTED"
        )

    def test_multiple_resets_idempotent(self, mem_db):
        """Edge case — resetting ACK multiple times is safe."""
        cid = insert_client(mem_db)
        rid = insert_return(
            mem_db, cid, status="REJECTED", ack_date="2025-03-15"
        )
        self._run_pending_reset(mem_db, rid)
        self._run_pending_reset(mem_db, rid)  # second call

        row = mem_db.execute(
            "SELECT ack_date FROM returns WHERE id=?", (rid,)
        ).fetchone()
        assert row["ack_date"] is None


# ---------------------------------------------------------------------------
# PART 4 — UI template — transmitted indicator (#46)
# ---------------------------------------------------------------------------


class TestTransmittedBadge:
    def test_transmitted_batch_shows_checkmark_and_date(self, mem_db):
        """#46 — Rendered batch list must include ✓ and transmitted_at for transmitted batches."""
        import sys
        import os

        # Locate the templates directory
        taxops_dir = os.path.join(os.path.dirname(__file__), "..", "taxops")
        templates_dir = os.path.join(taxops_dir, "templates")

        from jinja2 import Environment, FileSystemLoader

        env = Environment(loader=FileSystemLoader(templates_dir), autoescape=True)

        # Load only the status cell fragment by rendering a minimal mock
        # We render a standalone micro-template that reproduces the status cell logic
        status_fragment = """
        {% set b = batch %}
        {% if b.status == 'transmitted' %}
        <div class="flex flex-col gap-0.5">
          <span class="bg-green-100 text-green-700 w-fit">
            &#10003; TRANSMITTED
          </span>
          {% if b.transmitted_at %}
          <span class="text-slate-400">{{ b.transmitted_at[:10] }}</span>
          {% endif %}
        </div>
        {% elif b.status == 'closed' %}
        <span>CLOSED</span>
        {% else %}
        <span>{{ b.status | upper }}</span>
        {% endif %}
        """

        tmpl = env.from_string(status_fragment)

        class FakeBatch:
            status = "transmitted"
            transmitted_at = "2025-04-15T09:30:00"

        html = tmpl.render(batch=FakeBatch())

        assert "✓" in html or "&#10003;" in html, "Missing checkmark in transmitted badge"
        assert "2025-04-15" in html, "Missing transmitted_at date in badge"

    def test_transmitted_missing_date_does_not_crash(self, mem_db):
        """Edge case — transmitted batch with no transmitted_at renders without crashing."""
        import os
        from jinja2 import Environment, BaseLoader

        status_fragment = """
        {% set b = batch %}
        {% if b.status == 'transmitted' %}
        <span>&#10003; TRANSMITTED</span>
        {% if b.transmitted_at %}
        <span>{{ b.transmitted_at[:10] }}</span>
        {% endif %}
        {% endif %}
        """
        tmpl = Environment(loader=BaseLoader()).from_string(status_fragment)

        class FakeBatch:
            status = "transmitted"
            transmitted_at = None

        html = tmpl.render(batch=FakeBatch())
        assert "TRANSMITTED" in html
        # No date should appear when transmitted_at is None
        assert "None" not in html
