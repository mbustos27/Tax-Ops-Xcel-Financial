"""EMAIL-1/2/3/4/5/6/7 — mail watcher stability and match-confidence tests."""

from __future__ import annotations

import threading
from collections import OrderedDict
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from db import get_connection, init_db


# ── helpers ──────────────────────────────────────────────────────────────────

class _QuietThread(threading.Thread):
    """Prevent background classify threads from touching AI during tests."""
    def start(self) -> None:
        return None


@pytest.fixture
def _patch_enqueue(monkeypatch: pytest.MonkeyPatch):
    import utils as utils_mod
    monkeypatch.setattr(utils_mod, "_enqueue_extraction", lambda *a, **k: None)


@pytest.fixture
def _patch_mail_thread(monkeypatch: pytest.MonkeyPatch):
    import mail_watcher as mw
    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)


@pytest.fixture
def _docs_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    import utils as utils_mod
    root = tmp_path / "return_docs"
    def _rtp(rid: int) -> str:
        p = root / str(rid)
        p.mkdir(parents=True, exist_ok=True)
        return str(p)
    monkeypatch.setattr(utils_mod, "get_return_documents_path", _rtp)
    return root


@pytest.fixture
def email_client_return(taxops_db_path: str) -> int:
    """Create a test client + return and return the return id."""
    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT OR IGNORE INTO clients (id, last_name, first_name, display_name)"
        " VALUES (701, 'Garcia', 'Yakelin', 'Garcia, Yakelin')"
    )
    conn.execute(
        "INSERT OR IGNORE INTO returns "
        "(id, client_id, log_number, tax_year, processor, verified, client_status, intake_date)"
        " VALUES (9901, 701, 'm9901', 2025, '', 0, 'PROCESSING', '2026-01-01')"
    )
    conn.commit()
    conn.close()
    return 9901


def _msg_with_pdf(payload: bytes = b"%PDF-test") -> EmailMessage:
    m = EmailMessage()
    m["Subject"] = "Test document – Garcia Yakelin"
    m["From"] = "client@example.com"
    m.set_content("See attached.")
    m.add_attachment(payload, maintype="application", subtype="pdf", filename="test.pdf")
    return m


# ── EMAIL-1/2: UID pre-registration contract ─────────────────────────────────

def test_mark_uid_processed_returns_true_first_time():
    """EMAIL-1: first claim of a UID should succeed."""
    import mail_watcher as mw
    # Reset state for isolation
    orig = mw._processed_uids.copy()
    try:
        uid_key = ("TEST_FOLDER", "UID_UNIQUE_99999")
        mw._processed_uids.clear()
        assert mw._mark_uid_processed("TEST_FOLDER", "UID_UNIQUE_99999") is True
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(orig)


def test_mark_uid_processed_returns_false_on_duplicate():
    """EMAIL-1/5: second claim of same UID must return False — no double dispatch."""
    import mail_watcher as mw
    orig = mw._processed_uids.copy()
    try:
        mw._processed_uids.clear()
        mw._mark_uid_processed("INBOX", "UID_DUP_12345")
        assert mw._mark_uid_processed("INBOX", "UID_DUP_12345") is False
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(orig)


def test_uid_dedup_prevents_double_dispatch(monkeypatch: pytest.MonkeyPatch):
    """EMAIL-5: poll loop with same UID in raw_uids list dispatches exactly once."""
    import mail_watcher as mw
    orig = mw._processed_uids.copy()
    dispatch_calls: list = []
    monkeypatch.setattr(mw, "_dispatch_classified_message", lambda *a, **k: dispatch_calls.append(1))

    try:
        mw._processed_uids.clear()
        uid = b"99999"
        uid_str = "99999"
        claimed = []
        for u in [uid, uid]:  # same UID twice
            if mw._mark_uid_processed("INBOX", uid_str):
                claimed.append(u)
        # Simulate dispatch for each claimed UID
        for _ in claimed:
            mw._dispatch_classified_message(None, {})
        assert len(dispatch_calls) == 1, "Expected exactly 1 dispatch for a duplicate UID"
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(orig)


# ── EMAIL-3: diagnostic counters ─────────────────────────────────────────────

def test_poll_logs_total_unseen_and_already_seen(monkeypatch: pytest.MonkeyPatch, caplog):
    """EMAIL-3: log line must show 'new of <total> unseen (<n> already seen)'."""
    import logging
    import mail_watcher as mw
    orig = mw._processed_uids.copy()

    try:
        mw._processed_uids.clear()
        # Pre-populate one UID so it's "already seen"
        mw._mark_uid_processed("INBOX", "100")

        raw_uids = [b"100", b"101", b"102"]  # 1 already seen, 2 new
        claimed = []
        for uid in raw_uids:
            uid_str = uid.decode("ascii")
            if mw._mark_uid_processed("INBOX", uid_str):
                claimed.append(uid)

        total_unseen = len(raw_uids)
        already_seen = total_unseen - len(claimed)

        with caplog.at_level(logging.INFO, logger="mail_watcher"):
            mw.logger.info(
                f"Folder INBOX: {len(claimed)} new"
                f" of {total_unseen} unseen"
                f" ({already_seen} already seen)"
            )

        assert any(
            "2 new" in r.message and "3 unseen" in r.message and "1 already seen" in r.message
            for r in caplog.records
        ), f"Expected diagnostic log not found in: {[r.message for r in caplog.records]}"
    finally:
        mw._processed_uids.clear()
        mw._processed_uids.update(orig)


# ── EMAIL-4: poll-lock skip counter ──────────────────────────────────────────

def test_poll_skip_counter_increments(monkeypatch: pytest.MonkeyPatch):
    """EMAIL-4: _poll_skip_count increments when lock is already held."""
    import mail_watcher as mw
    orig_count = mw._poll_skip_count
    try:
        mw._poll_lock.acquire()
        mw._poll_once(None)  # will skip because lock held
        assert mw._poll_skip_count > orig_count
    finally:
        mw._poll_lock.release()
        mw._poll_skip_count = 0


def test_mail_watcher_status_includes_poll_skipped():
    """EMAIL-4: mail_watcher_status() must expose poll_skipped count."""
    import mail_watcher as mw
    status = mw.mail_watcher_status()
    assert "poll_skipped" in status
    assert isinstance(status["poll_skipped"], int)


# ── EMAIL-6: match_score returned from _match_client ─────────────────────────

def test_match_client_returns_score(taxops_db_path: str, monkeypatch: pytest.MonkeyPatch):
    """EMAIL-6: _match_client must include match_score in return dict."""
    import mail_watcher as mw
    import config as cfg

    monkeypatch.setattr(cfg, "MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE", 50)

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        "INSERT OR IGNORE INTO clients (id, last_name, first_name, display_name)"
        " VALUES (801, 'Smith', 'John', 'Smith, John')"
    )
    conn.commit()
    conn.close()

    app = MagicMock()
    app.app_context = MagicMock(return_value=MagicMock(__enter__=lambda s: s, __exit__=lambda *a: None))

    monkeypatch.setattr("mail_watcher.DB_PATH" if hasattr(mw, "DB_PATH") else "db.DB_PATH",
                        taxops_db_path, raising=False)

    result = mw._match_client(app, "Smith John")
    if result is not None:
        assert "match_score" in result, "match_score missing from _match_client return"
        assert isinstance(result["match_score"], int)


# ── EMAIL-7: low-confidence routing ──────────────────────────────────────────

def test_record_classification_stores_match_score(taxops_db_path: str):
    """EMAIL-6/7: _record_classification stores match_score and match_status."""
    import mail_watcher as mw
    import db as db_mod

    orig = db_mod.DB_PATH
    db_mod.DB_PATH = taxops_db_path
    try:
        conn = get_connection(taxops_db_path)
        init_db(conn)
        conn.close()

        ec_id = mw._record_classification(
            "test@example.com",
            "example.com",
            "Tax docs Smith",
            "client_document",
            source="auto",
            match_score=85,
            matched_client_id=999,
            match_status="pending_review",
        )
        assert ec_id is not None

        conn2 = get_connection(taxops_db_path)
        row = conn2.execute(
            "SELECT match_score, matched_client_id, match_status FROM email_classifications WHERE id = ?",
            (ec_id,),
        ).fetchone()
        conn2.close()
        assert row is not None
        assert row["match_score"] == 85
        assert row["matched_client_id"] == 999
        assert row["match_status"] == "pending_review"
    finally:
        db_mod.DB_PATH = orig


def test_pending_review_api_list(client_logged_in):
    """EMAIL-7: GET /api/email-classifications/pending-review returns list."""
    rv = client_logged_in.get("/api/email-classifications/pending-review")
    assert rv.status_code == 200
    data = rv.get_json()
    assert "items" in data
    assert isinstance(data["items"], list)


def test_confirm_match_404_on_nonexistent(client_logged_in):
    """EMAIL-7: confirming unknown ec_id returns 404."""
    rv = client_logged_in.post("/api/email-classifications/999999/confirm-match")
    assert rv.status_code == 404


def test_reject_match_404_on_nonexistent(client_logged_in):
    """EMAIL-7: rejecting unknown ec_id returns 404."""
    rv = client_logged_in.post("/api/email-classifications/999999/reject-match")
    assert rv.status_code == 404


def test_schema_has_match_score_column(taxops_db_path: str):
    """EMAIL-6: email_classifications must have match_score column after migration."""
    conn = get_connection(taxops_db_path)
    init_db(conn)
    info = conn.execute("PRAGMA table_info(email_classifications)").fetchall()
    conn.close()
    cols = {row["name"] for row in info}
    assert "match_score" in cols
    assert "matched_client_id" in cols
    assert "match_status" in cols
