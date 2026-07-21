"""Phase 3.1 — suggested client matching (non-binding).

Covers:
  - matching chain: exact_email > domain_hint > fuzzy (>=90), each in isolation
  - ambiguous exact/domain matches yield no suggestion
  - fuzzy threshold boundary (89 -> none, 91 -> suggested)
  - /api/email-inbox/items computes, caches, and exposes suggestion fields
    (client name + log number only — never file_path/SSN/EIN/TIN)
  - /assign sets match_method='email_suggested' + cached score only when the
    assigned return_id matches the cached suggestion; otherwise 'email_manual'
  - mail_watcher.py never imports email_suggest (poll cycle stays match-free)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import pytest

from db import get_connection
import email_suggest


def _make_client(conn, client_id, last_name, first_name, taxpayer_email=None, spouse_email=None):
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name, display_name, taxpayer_email, spouse_email) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        (client_id, last_name, first_name, f"{last_name}, {first_name}", taxpayer_email, spouse_email),
    )


def _make_return(conn, return_id, client_id, tax_year=2025, log_number=None):
    conn.execute(
        "INSERT INTO returns (id, client_id, tax_year, log_number, client_status) "
        "VALUES (?, ?, ?, ?, 'PROCESSING')",
        (return_id, client_id, tax_year, log_number),
    )


# ── compute_suggestion() — unit tests on the matching chain ─────────────────

def test_exact_email_match_suggests_most_recent_return(taxops_db_path):
    conn = get_connection(taxops_db_path)
    _make_client(conn, 1, "SMITH", "JOHN", taxpayer_email="john@client.com")
    _make_return(conn, 1, 1, tax_year=2024, log_number="50")
    _make_return(conn, 2, 1, tax_year=2025, log_number="150")
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="john@client.com", sender_domain="client.com", sender_name="John Smith"
    )
    conn.close()
    assert result == {"return_id": 2, "method": "exact_email", "score": None}


def test_exact_email_matches_spouse_email_too(taxops_db_path):
    conn = get_connection(taxops_db_path)
    _make_client(conn, 2, "GARCIA", "MARIA", taxpayer_email="maria@x.com", spouse_email="carlos@x.com")
    _make_return(conn, 3, 2, tax_year=2025, log_number="200")
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="CARLOS@X.COM", sender_domain="x.com", sender_name=""
    )
    conn.close()
    assert result == {"return_id": 3, "method": "exact_email", "score": None}


def test_exact_email_no_match_at_all_returns_none(taxops_db_path):
    conn = get_connection(taxops_db_path)
    _make_client(conn, 3, "LOPEZ", "ANA", taxpayer_email="ana@x.com")
    _make_return(conn, 4, 3, tax_year=2025)
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="stranger@nowhere.com", sender_domain="nowhere.com", sender_name="Totally Unrelated Person"
    )
    conn.close()
    assert result is None


def test_domain_hint_single_client_on_configured_domain(taxops_db_path, monkeypatch):
    monkeypatch.setattr(email_suggest, "CLIENT_HINT_DOMAINS", frozenset({"acmecorp.com"}))
    conn = get_connection(taxops_db_path)
    _make_client(conn, 4, "ACME CORP", None, taxpayer_email="billing@acmecorp.com")
    _make_return(conn, 5, 4, tax_year=2025, log_number="300")
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="random@acmecorp.com", sender_domain="acmecorp.com", sender_name=""
    )
    conn.close()
    assert result == {"return_id": 5, "method": "domain_hint", "score": None}


def test_domain_hint_multiple_clients_is_ambiguous_no_suggestion(taxops_db_path, monkeypatch):
    monkeypatch.setattr(email_suggest, "CLIENT_HINT_DOMAINS", frozenset({"shared.com"}))
    conn = get_connection(taxops_db_path)
    _make_client(conn, 5, "ONE", "CLIENT", taxpayer_email="a@shared.com")
    _make_client(conn, 6, "TWO", "CLIENT", taxpayer_email="b@shared.com")
    _make_return(conn, 6, 5, tax_year=2025)
    _make_return(conn, 7, 6, tax_year=2025)
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="new@shared.com", sender_domain="shared.com", sender_name="No Name Match Here"
    )
    conn.close()
    assert result is None


def test_domain_hint_ignored_when_domain_not_configured(taxops_db_path, monkeypatch):
    monkeypatch.setattr(email_suggest, "CLIENT_HINT_DOMAINS", frozenset())
    conn = get_connection(taxops_db_path)
    _make_client(conn, 7, "ONLY", "CLIENT", taxpayer_email="a@unconfigured.com")
    _make_return(conn, 8, 7, tax_year=2025)
    conn.commit()

    result = email_suggest.compute_suggestion(
        conn, sender_email="new@unconfigured.com", sender_domain="unconfigured.com", sender_name=""
    )
    conn.close()
    assert result is None


def test_fuzzy_below_90_yields_no_suggestion(taxops_db_path):
    """89 < 90 threshold -> no suggestion, even though find_client() itself
    would surface it as a below-accept-bar candidate for CSV-import review."""
    conn = get_connection(taxops_db_path)
    with patch.object(email_suggest, "find_client", return_value={
        "client_id": 9, "score": 89, "method": "fuzzy_full", "needs_review": True,
    }):
        result = email_suggest.compute_suggestion(
            conn, sender_email="", sender_domain="", sender_name="Almost Match Name"
        )
    conn.close()
    assert result is None


def test_fuzzy_at_91_yields_suggestion_with_score(taxops_db_path):
    conn = get_connection(taxops_db_path)
    _make_client(conn, 9, "RODRIGUEZ", "PEDRO")
    _make_return(conn, 10, 9, tax_year=2025, log_number="400")
    conn.commit()

    with patch.object(email_suggest, "find_client", return_value={
        "client_id": 9, "score": 91, "method": "fuzzy_full", "needs_review": False,
    }):
        result = email_suggest.compute_suggestion(
            conn, sender_email="", sender_domain="", sender_name="Pedro Rodriguez"
        )
    conn.close()
    assert result == {"return_id": 10, "method": "fuzzy", "score": 91}


def test_fuzzy_skipped_when_sender_name_blank(taxops_db_path):
    conn = get_connection(taxops_db_path)
    result = email_suggest.compute_suggestion(
        conn, sender_email="", sender_domain="", sender_name="   "
    )
    conn.close()
    assert result is None


def test_matching_chain_precedence_exact_beats_fuzzy(taxops_db_path):
    """Even if a fuzzy candidate scores higher, an exact email hit wins."""
    conn = get_connection(taxops_db_path)
    _make_client(conn, 10, "TORRES", "LUIS", taxpayer_email="luis@x.com")
    _make_return(conn, 11, 10, tax_year=2025, log_number="500")
    conn.commit()

    with patch.object(email_suggest, "find_client", return_value={
        "client_id": 999, "score": 100, "method": "exact", "needs_review": False,
    }):
        result = email_suggest.compute_suggestion(
            conn, sender_email="luis@x.com", sender_domain="x.com", sender_name="Luis Torres"
        )
    conn.close()
    assert result == {"return_id": 11, "method": "exact_email", "score": None}


# ── /api/email-inbox/items — computation, caching, privacy ──────────────────

def _seed_inbox_item(db_path, item_id, sender_email="", sender_domain="", sender_name="",
                      suggested_return_id=None, suggestion_method=None, suggestion_score=None):
    conn = get_connection(db_path)
    conn.execute(
        """
        INSERT INTO email_inbox
          (id, sender_email, sender_domain, sender_name, subject_snippet, filename,
           original_filename, file_path, file_size_bytes, received_at,
           is_assigned, is_deleted, suggested_return_id, suggestion_method, suggestion_score)
        VALUES (?, ?, ?, ?, 'docs', 'doc.pdf', 'doc.pdf', ?, 123,
                '2026-01-01T00:00:00+00:00', 0, 0, ?, ?, ?)
        """,
        (item_id, sender_email, sender_domain, sender_name, f"/tmp/doc{item_id}.pdf",
         suggested_return_id, suggestion_method, suggestion_score),
    )
    conn.commit()
    conn.close()


def test_items_api_computes_and_caches_suggestion(client_logged_in, taxops_db_path):
    conn = get_connection(taxops_db_path)
    _make_client(conn, 20, "MARTINEZ", "ELENA", taxpayer_email="elena@client.com")
    _make_return(conn, 20, 20, tax_year=2025, log_number="777")
    conn.commit()
    conn.close()
    _seed_inbox_item(taxops_db_path, 30, sender_email="elena@client.com", sender_domain="client.com")

    rv = client_logged_in.get("/api/email-inbox/items")
    items = {i["id"]: i for i in rv.get_json()["items"]}
    item = items[30]
    assert item["suggestion_method"] == "exact_email"
    assert item["suggested_return_id"] == 20
    assert item["suggested_client_name"] == "MARTINEZ, ELENA"
    assert item["suggested_log_number"] == "777"
    assert "file_path" not in item
    assert "sender_name" not in item

    # Cached on the row — a second read must not need to recompute (still correct).
    conn = get_connection(taxops_db_path)
    row = conn.execute(
        "SELECT suggested_return_id, suggestion_method FROM email_inbox WHERE id=30"
    ).fetchone()
    conn.close()
    assert row["suggested_return_id"] == 20
    assert row["suggestion_method"] == "exact_email"


def test_items_api_no_suggestion_fields_are_null(client_logged_in, taxops_db_path):
    _seed_inbox_item(taxops_db_path, 31, sender_email="nobody@nowhere.com", sender_domain="nowhere.com")
    rv = client_logged_in.get("/api/email-inbox/items")
    items = {i["id"]: i for i in rv.get_json()["items"]}
    item = items[31]
    assert item["suggested_return_id"] is None
    assert item["suggestion_method"] is None
    assert item["suggested_client_name"] is None
    assert item["suggested_log_number"] is None


def test_items_api_does_not_recompute_already_cached_suggestion(client_logged_in, taxops_db_path):
    """A pre-cached suggestion must be trusted as-is (recomputed only when
    suggested_return_id IS NULL) even if the underlying client data changed."""
    conn = get_connection(taxops_db_path)
    _make_client(conn, 21, "OLD", "MATCH")
    _make_return(conn, 21, 21, tax_year=2025, log_number="1")
    conn.commit()
    conn.close()
    _seed_inbox_item(
        taxops_db_path, 32, sender_email="whoever@x.com", sender_domain="x.com",
        suggested_return_id=21, suggestion_method="fuzzy", suggestion_score=95,
    )

    rv = client_logged_in.get("/api/email-inbox/items")
    items = {i["id"]: i for i in rv.get_json()["items"]}
    item = items[32]
    assert item["suggested_return_id"] == 21
    assert item["suggestion_method"] == "fuzzy"
    assert item["suggestion_score"] == 95


# ── /assign — email_suggested vs email_manual ────────────────────────────────

def _seed_client_and_return(db_path, client_id, return_id, log_number=None):
    # RACE-1: default to a log_number derived from return_id (rather than a
    # fixed "900") so two calls in the same test/tax_year never collide with
    # the ux_returns_log_year UNIQUE index (db.py) — each seeded return gets
    # its own distinct log_number unless a test explicitly overrides one.
    if log_number is None:
        log_number = str(return_id)
    conn = get_connection(db_path)
    conn.execute(
        "INSERT OR IGNORE INTO clients (id, last_name, first_name, display_name) "
        "VALUES (?, 'Test', 'Client', 'Test Client')",
        (client_id,),
    )
    conn.execute(
        "INSERT OR IGNORE INTO returns (id, client_id, tax_year, client_status, log_number) "
        "VALUES (?, ?, 2025, 'PROCESSING', ?)",
        (return_id, client_id, log_number),
    )
    conn.commit()
    conn.close()


@pytest.fixture
def inbox_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    import config as cfg
    root = tmp_path / "email_inbox"
    root.mkdir()
    monkeypatch.setattr(cfg, "EMAIL_INBOX_DIR", str(root))
    return root


def test_assign_via_matching_suggestion_writes_email_suggested_with_score(
    client_logged_in, taxops_db_path, inbox_dir
):
    _seed_client_and_return(taxops_db_path, client_id=200, return_id=200)
    src = inbox_dir / "w2.pdf"
    src.write_bytes(b"%PDF-1.4 x")
    _seed_inbox_item(taxops_db_path, 40, sender_domain="x.com")
    conn = get_connection(taxops_db_path)
    conn.execute(
        "UPDATE email_inbox SET file_path=?, suggested_return_id=200, "
        "suggestion_method='fuzzy', suggestion_score=93 WHERE id=40",
        (str(src),),
    )
    conn.commit()
    conn.close()

    dest_root = inbox_dir.parent / "returns_docs"
    with patch("app.get_return_documents_path", side_effect=lambda rid: str(dest_root / str(rid))):
        rv = client_logged_in.post("/api/email-inbox/40/assign", json={"return_id": 200})

    assert rv.status_code == 200, rv.data
    doc_id = rv.get_json()["doc_id"]

    conn = get_connection(taxops_db_path)
    doc = conn.execute(
        "SELECT match_confirmed, match_score, match_method FROM return_documents WHERE id=?",
        (doc_id,),
    ).fetchone()
    conn.close()
    assert doc["match_confirmed"] == 1
    assert doc["match_score"] == 93
    assert doc["match_method"] == "email_suggested"


def test_assign_to_different_return_than_suggestion_stays_email_manual(
    client_logged_in, taxops_db_path, inbox_dir
):
    """Staff overriding a suggestion (picking a different return) must record
    email_manual/NULL, never email_suggested."""
    _seed_client_and_return(taxops_db_path, client_id=201, return_id=201)
    _seed_client_and_return(taxops_db_path, client_id=202, return_id=202)
    src = inbox_dir / "w2.pdf"
    src.write_bytes(b"%PDF-1.4 x")
    _seed_inbox_item(taxops_db_path, 41)
    conn = get_connection(taxops_db_path)
    conn.execute(
        "UPDATE email_inbox SET file_path=?, suggested_return_id=201, "
        "suggestion_method='exact_email', suggestion_score=NULL WHERE id=41",
        (str(src),),
    )
    conn.commit()
    conn.close()

    dest_root = inbox_dir.parent / "returns_docs2"
    with patch("app.get_return_documents_path", side_effect=lambda rid: str(dest_root / str(rid))):
        rv = client_logged_in.post("/api/email-inbox/41/assign", json={"return_id": 202})

    assert rv.status_code == 200, rv.data
    doc_id = rv.get_json()["doc_id"]

    conn = get_connection(taxops_db_path)
    doc = conn.execute(
        "SELECT match_score, match_method FROM return_documents WHERE id=?", (doc_id,)
    ).fetchone()
    conn.close()
    assert doc["match_method"] == "email_manual"
    assert doc["match_score"] is None


def test_assign_with_no_cached_suggestion_stays_email_manual(client_logged_in, taxops_db_path, inbox_dir):
    _seed_client_and_return(taxops_db_path, client_id=203, return_id=203)
    src = inbox_dir / "w2.pdf"
    src.write_bytes(b"%PDF-1.4 x")
    _seed_inbox_item(taxops_db_path, 42)
    conn = get_connection(taxops_db_path)
    conn.execute("UPDATE email_inbox SET file_path=? WHERE id=42", (str(src),))
    conn.commit()
    conn.close()

    dest_root = inbox_dir.parent / "returns_docs3"
    with patch("app.get_return_documents_path", side_effect=lambda rid: str(dest_root / str(rid))):
        rv = client_logged_in.post("/api/email-inbox/42/assign", json={"return_id": 203})

    doc_id = rv.get_json()["doc_id"]
    conn = get_connection(taxops_db_path)
    doc = conn.execute(
        "SELECT match_score, match_method FROM return_documents WHERE id=?", (doc_id,)
    ).fetchone()
    conn.close()
    assert doc["match_method"] == "email_manual"
    assert doc["match_score"] is None


# ── Watcher isolation invariant ──────────────────────────────────────────────

def test_mail_watcher_never_imports_email_suggest():
    """mail_watcher.py must stay match-free — suggestions are only computed
    from /email-inbox and /api/email-inbox/items, never the IMAP poll cycle."""
    import pathlib
    src = pathlib.Path(__file__).resolve().parents[1] / "mail_watcher.py"
    text = src.read_text(encoding="utf-8")
    assert "email_suggest" not in text
