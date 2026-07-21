"""Phase 3.2 — inbox aging + triage view.

Covers:
  - age_bucket() boundaries: <2d green, 2-7d amber (inclusive both ends),
    >7d red
  - /api/email-inbox/items default sort is oldest-first
  - filter query params: needs_manual_tagging / has_suggestion / older_than_7d
  - unassigned_total / older_than_7d_total summary fields
  - /email-inbox page renders the summary line and filter chips
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

from db import get_connection
from utils import age_bucket, AGE_BUCKET_GREEN, AGE_BUCKET_AMBER, AGE_BUCKET_RED


_NOW = datetime(2026, 6, 24, 12, 0, 0, tzinfo=timezone.utc)


def _iso(days_ago: float) -> str:
    return (_NOW - timedelta(days=days_ago)).isoformat()


# ── age_bucket() boundary tests ──────────────────────────────────────────────

def test_age_bucket_just_under_2_days_is_green():
    assert age_bucket(_iso(1.99), now=_NOW) == AGE_BUCKET_GREEN


def test_age_bucket_exactly_2_days_is_amber():
    assert age_bucket(_iso(2.0), now=_NOW) == AGE_BUCKET_AMBER


def test_age_bucket_exactly_7_days_is_amber():
    assert age_bucket(_iso(7.0), now=_NOW) == AGE_BUCKET_AMBER


def test_age_bucket_just_over_7_days_is_red():
    assert age_bucket(_iso(7.01), now=_NOW) == AGE_BUCKET_RED


def test_age_bucket_zero_days_is_green():
    assert age_bucket(_iso(0), now=_NOW) == AGE_BUCKET_GREEN


def test_age_bucket_unparsable_defaults_to_green():
    assert age_bucket("not-a-date", now=_NOW) == AGE_BUCKET_GREEN
    assert age_bucket(None, now=_NOW) == AGE_BUCKET_GREEN


# ── /api/email-inbox/items — sort, filters, summary ──────────────────────────

def _seed_item(db_path, item_id, *, days_ago, filename="doc.pdf",
                suggested_return_id=None, suggestion_method=None):
    # Integration tests exercise the real app.py code path, which always
    # buckets against the real wall clock (utils.age_bucket() with no `now`
    # override) — so received_at must be relative to real "now", not the
    # fixed _NOW used only by the direct age_bucket() unit tests above.
    real_now = datetime.now(timezone.utc)
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT INTO email_inbox
          (id, sender_email, sender_domain, subject_snippet, filename,
           original_filename, file_path, file_size_bytes, received_at,
           is_assigned, is_deleted, suggested_return_id, suggestion_method)
        VALUES (?, 'x@nowhere-unmatched.example', 'nowhere-unmatched.example', 'docs',
                ?, ?, ?, 10, ?, 0, 0, ?, ?)
        """,
        (item_id, filename, filename, f"/tmp/{item_id}.pdf",
         (real_now - timedelta(days=days_ago)).isoformat(),
         suggested_return_id, suggestion_method),
    )
    conn.commit()
    conn.close()


def test_items_default_sort_is_oldest_first(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 1, days_ago=1)
    _seed_item(taxops_db_path, 2, days_ago=10)
    _seed_item(taxops_db_path, 3, days_ago=5)

    rv = client_logged_in.get("/api/email-inbox/items")
    ids_in_order = [i["id"] for i in rv.get_json()["items"]]
    assert ids_in_order == [2, 3, 1]


def test_items_includes_age_bucket_field(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 4, days_ago=10)
    rv = client_logged_in.get("/api/email-inbox/items")
    items = {i["id"]: i for i in rv.get_json()["items"]}
    assert items[4]["age_bucket"] == "red"


def test_items_summary_totals_reflect_unfiltered_set(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 5, days_ago=1)   # green
    _seed_item(taxops_db_path, 6, days_ago=10)  # red
    _seed_item(taxops_db_path, 7, days_ago=8)   # red

    rv = client_logged_in.get("/api/email-inbox/items")
    data = rv.get_json()
    assert data["unassigned_total"] == 3
    assert data["older_than_7d_total"] == 2
    # Applying a filter must not change the summary totals.
    rv2 = client_logged_in.get("/api/email-inbox/items?filter=older_than_7d")
    data2 = rv2.get_json()
    assert data2["unassigned_total"] == 3
    assert data2["older_than_7d_total"] == 2
    assert len(data2["items"]) == 2


def test_filter_older_than_7d(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 8, days_ago=1)
    _seed_item(taxops_db_path, 9, days_ago=9)

    rv = client_logged_in.get("/api/email-inbox/items?filter=older_than_7d")
    ids = {i["id"] for i in rv.get_json()["items"]}
    assert ids == {9}


def test_filter_needs_manual_tagging(client_logged_in, taxops_db_path, monkeypatch):
    import config as cfg
    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", False)
    _seed_item(taxops_db_path, 10, days_ago=1, filename="receipt.jpg")
    _seed_item(taxops_db_path, 11, days_ago=1, filename="w2.pdf")

    rv = client_logged_in.get("/api/email-inbox/items?filter=needs_manual_tagging")
    ids = {i["id"] for i in rv.get_json()["items"]}
    assert ids == {10}


def test_filter_has_suggestion(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    conn.execute("INSERT INTO clients (id, last_name, first_name, display_name) VALUES (999, 'X', 'Y', 'X, Y')")
    conn.execute("INSERT INTO returns (id, client_id, tax_year, client_status, log_number) VALUES (999, 999, 2025, 'PROCESSING', '1')")
    conn.commit()
    conn.close()

    _seed_item(taxops_db_path, 12, days_ago=1, suggested_return_id=None)
    _seed_item(taxops_db_path, 13, days_ago=1, suggested_return_id=999, suggestion_method="fuzzy")

    rv = client_logged_in.get("/api/email-inbox/items?filter=has_suggestion")
    ids = {i["id"] for i in rv.get_json()["items"]}
    assert ids == {13}


def test_filter_all_or_unknown_returns_everything(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 14, days_ago=1)
    _seed_item(taxops_db_path, 15, days_ago=10)

    rv_default = client_logged_in.get("/api/email-inbox/items")
    rv_bogus   = client_logged_in.get("/api/email-inbox/items?filter=not_a_real_filter")
    assert {i["id"] for i in rv_default.get_json()["items"]} == {14, 15}
    assert {i["id"] for i in rv_bogus.get_json()["items"]} == {14, 15}


# ── /email-inbox page ─────────────────────────────────────────────────────────

def test_page_renders_summary_line_and_filter_chips(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 16, days_ago=1)
    _seed_item(taxops_db_path, 17, days_ago=9)

    rv = client_logged_in.get("/email-inbox")
    assert rv.status_code == 200
    html = rv.data.decode("utf-8")
    assert "2" in html and "unassigned" in html
    assert "older than 7 days" in html
    assert "filter=needs_manual_tagging" in html
    assert "filter=has_suggestion" in html
    assert "filter=older_than_7d" in html


def test_page_applies_filter_query_param(client_logged_in, taxops_db_path):
    _seed_item(taxops_db_path, 18, days_ago=1, filename="w2.pdf")
    _seed_item(taxops_db_path, 19, days_ago=9, filename="w9.pdf")

    rv = client_logged_in.get("/email-inbox?filter=older_than_7d")
    html = rv.data.decode("utf-8")
    assert "w9.pdf" in html
    assert "w2.pdf" not in html
