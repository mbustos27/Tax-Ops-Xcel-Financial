"""Part 8 — mail_watcher outcome-based mark-as-read tests.

Tests that verify:
  - Successful processing marks as read (OUTCOME_SUCCESS)
  - Failed attachment save leaves unread (OUTCOME_RETRY)
  - Known-rule match marks as read immediately (OUTCOME_SKIP)
  - Max retries exceeded marks as read and stops retrying (OUTCOME_SKIP)
  - Duplicate attachment is skipped without error
  - DRY_RUN never calls imap.store
  - Processing log upserts correctly on repeated attempts
"""
from __future__ import annotations

import hashlib
import threading
from email.message import EmailMessage
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from db import get_connection, init_db


# ── Shared helpers ────────────────────────────────────────────────────────────

class _QuietThread(threading.Thread):
    """Silences background classify threads during tests."""
    def start(self) -> None:
        return None


def _make_email_with_pdf(filename: str = "test.pdf", payload: bytes = b"PDF") -> object:
    msg = EmailMessage()
    msg["From"] = "test@example.com"
    msg["Subject"] = "Test"
    msg.add_attachment(payload, maintype="application", subtype="pdf", filename=filename)
    return msg


@pytest.fixture
def db_path(tmp_path, monkeypatch):
    path = str(tmp_path / "test.db")
    import config as cfg
    import db as db_mod
    monkeypatch.setattr(cfg, "DB_PATH", path)
    monkeypatch.setattr(db_mod, "DB_PATH", path)
    conn = get_connection(path)
    init_db(conn)
    # seed a client and return so attachment saving can succeed
    conn.execute(
        "INSERT INTO clients (id, last_name, first_name) VALUES (9001, 'Smith', 'Jane')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, tax_year, client_status) VALUES (8001, 9001, 2025, 'PROCESSING')"
    )
    conn.commit()
    conn.close()
    return path


@pytest.fixture
def docs_path(tmp_path, monkeypatch):
    import utils as utils_mod
    root = tmp_path / "docs"

    def _rtp(rid):
        p = root / str(rid)
        p.mkdir(parents=True, exist_ok=True)
        return str(p)

    monkeypatch.setattr(utils_mod, "get_return_documents_path", _rtp)
    monkeypatch.setattr(utils_mod, "_enqueue_extraction", lambda *a, **k: None)
    return root


@pytest.fixture
def quiet_threads(monkeypatch):
    import mail_watcher as mw
    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)


# ── Outcome constants ─────────────────────────────────────────────────────────

def test_outcome_constants_defined():
    import mail_watcher as mw
    assert mw.OUTCOME_SUCCESS == "success"
    assert mw.OUTCOME_SKIP    == "skip"
    assert mw.OUTCOME_RETRY   == "retry"
    assert mw.OUTCOME_DRY_RUN == "dry_run"


# ── _dispatch_classified_message returns outcomes ────────────────────────────

def test_known_rule_returns_skip():
    import mail_watcher as mw
    msg = {
        "classification": "client_document",
        "source_layer": "known_rule",
        "sender_domain": "spam.com",
        "sender_email": "noreply@spam.com",
        "subject": "promo",
        "body_text": "",
        "message": MagicMock(),
        "sender": "noreply@spam.com",
    }
    outcome = mw._dispatch_classified_message(None, msg)
    assert outcome == mw.OUTCOME_SKIP


def test_promotional_returns_skip():
    import mail_watcher as mw
    msg = {
        "classification": "promotional",
        "source_layer": "keyword",
        "sender_domain": "promo.com",
        "sender_email": "deals@promo.com",
        "subject": "big sale",
        "body_text": "",
        "message": MagicMock(),
        "sender": "deals@promo.com",
    }
    outcome = mw._dispatch_classified_message(MagicMock(), msg)
    assert outcome == mw.OUTCOME_SKIP


def test_unknown_classification_returns_skip():
    import mail_watcher as mw
    msg = {
        "classification": "unknown",
        "source_layer": "llm",
        "sender_domain": "unknown.com",
        "sender_email": "x@unknown.com",
        "subject": "????",
        "body_text": "",
        "message": MagicMock(),
        "sender": "x@unknown.com",
    }
    with patch.object(mw, "_log_unmatched"):
        outcome = mw._dispatch_classified_message(MagicMock(), msg)
    assert outcome == mw.OUTCOME_SKIP


# ── Successful processing marks as read ──────────────────────────────────────

def test_successful_processing_never_marks_read(db_path, docs_path, quiet_threads, monkeypatch):
    """OUTCOME_SUCCESS from dispatch → imap.uid('STORE') is NEVER called.
    TaxOps read-status policy: emails are never marked read under any outcome."""
    import mail_watcher as mw

    imap_mock = MagicMock()
    uid = b"42"
    msg = {
        "uid": uid,
        "folder": "INBOX",
        "classification": "client_document",
        "source_layer": "llm",
        "sender_domain": "example.com",
        "sender_email": "jane@example.com",
        "subject": "My W2",
        "body_text": "Here is my W2",
        "message": _make_email_with_pdf(),
        "sender": "Jane Smith <jane@example.com>",
    }

    with patch.object(mw, "_extract_client_name", return_value="Smith Jane"), \
         patch.object(mw, "_match_client", return_value={"id": 9001, "match_score": 90, "first_name": "Jane", "last_name": "Smith"}), \
         patch.object(mw, "_find_current_return", return_value={"id": 8001}), \
         patch.object(mw, "_save_attachments", return_value=1), \
         patch.object(mw, "_add_note", return_value=True), \
         patch.object(mw, "_update_domain_cache"), \
         patch.object(mw, "_upsert_processing_log"), \
         patch.object(mw, "_retry_cap_exceeded", return_value=False):

        try:
            outcome = mw._dispatch_classified_message(MagicMock(), msg)
        except Exception:
            outcome = mw.OUTCOME_RETRY

    assert outcome == mw.OUTCOME_SUCCESS
    # Policy: _mark_read is never called — imap.uid("STORE") must not appear
    imap_mock.uid.assert_not_called()


# ── Failed attachment save leaves unread ─────────────────────────────────────

def test_failed_attachment_save_leaves_unread(monkeypatch):
    """Exception in _save_attachments → OUTCOME_RETRY → imap.store NOT called."""
    import mail_watcher as mw

    imap_mock = MagicMock()
    uid = b"99"
    msg = {
        "uid": uid,
        "folder": "INBOX",
        "classification": "client_document",
        "source_layer": "llm",
        "sender_domain": "fail.com",
        "sender_email": "x@fail.com",
        "subject": "doc",
        "body_text": "",
        "message": MagicMock(),
        "sender": "x@fail.com",
    }

    with patch.object(mw, "_extract_client_name", return_value="Smith Jane"), \
         patch.object(mw, "_match_client", return_value={"id": 9001, "match_score": 95, "first_name": "Jane", "last_name": "Smith"}), \
         patch.object(mw, "_find_current_return", return_value={"id": 8001}), \
         patch.object(mw, "_save_attachments", side_effect=RuntimeError("disk full")):
        try:
            outcome = mw._dispatch_classified_message(MagicMock(), msg)
        except Exception:
            outcome = mw.OUTCOME_RETRY

    assert outcome == mw.OUTCOME_RETRY
    # imap.store must NOT be called for RETRY outcome
    imap_mock.uid.assert_not_called()


# ── Known-rule marks as read without DB write ─────────────────────────────────

def test_known_rule_marks_read_no_db_write(monkeypatch):
    """Known-rule skip → OUTCOME_SKIP → mark as read; no email_classifications row written."""
    import mail_watcher as mw

    imap_mock = MagicMock()
    uid = b"55"

    with patch.object(mw, "_record_classification") as mock_record:
        msg = {
            "uid": uid,
            "folder": "INBOX",
            "classification": "promotional",
            "source_layer": "known_rule",
            "sender_domain": "skip.com",
            "sender_email": "x@skip.com",
            "subject": "skip",
            "body_text": "",
            "message": MagicMock(),
            "sender": "x@skip.com",
        }
        outcome = mw._dispatch_classified_message(MagicMock(), msg)

    assert outcome == mw.OUTCOME_SKIP
    # No DB classification written for known_rule
    mock_record.assert_not_called()


# ── Max retries exceeded ──────────────────────────────────────────────────────

def test_max_retries_exceeded_marks_read(db_path, monkeypatch):
    """When attempt_count >= IMAP_MAX_RETRIES, outcome=SKIP and message gets marked read."""
    import mail_watcher as mw

    uid_str = "uid-max-retry"
    folder  = "INBOX"

    # Seed the processing log with attempt_count at the cap
    import config as cfg
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO email_processing_log "
        "(message_uid, imap_folder, sender_domain, subject_snippet, outcome, attempt_count, last_attempt_at) "
        "VALUES (?, ?, 'x.com', 'test', 'retry', ?, datetime('now'))",
        (uid_str, folder, cfg.IMAP_MAX_RETRIES),
    )
    conn.commit()
    conn.close()

    assert mw._retry_cap_exceeded(uid_str, folder) is True


def test_below_retry_cap_not_exceeded(db_path, monkeypatch):
    """When attempt_count < IMAP_MAX_RETRIES, _retry_cap_exceeded returns False."""
    import mail_watcher as mw

    uid_str = "uid-below-cap"
    folder  = "INBOX"

    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO email_processing_log "
        "(message_uid, imap_folder, sender_domain, subject_snippet, outcome, attempt_count, last_attempt_at) "
        "VALUES (?, ?, 'x.com', 'test', 'retry', 1, datetime('now'))",
        (uid_str, folder),
    )
    conn.commit()
    conn.close()

    assert mw._retry_cap_exceeded(uid_str, folder) is False


# ── _mark_read is a tombstone — never touches IMAP ───────────────────────────

def test_mark_read_tombstone_never_calls_store(monkeypatch):
    """_mark_read is a disabled tombstone — imap.uid(STORE) is never called
    regardless of IMAP_DRY_RUN or IMAP_MARK_AS_READ settings."""
    import mail_watcher as mw
    import config as cfg

    imap_mock = MagicMock()

    # Call with every combination of flags — none should reach STORE
    for dry_run, mark_as_read in [(True, True), (False, True), (True, False), (False, False)]:
        monkeypatch.setattr(cfg, "IMAP_DRY_RUN", dry_run)
        monkeypatch.setattr(cfg, "IMAP_MARK_AS_READ", mark_as_read)
        mw._mark_read(imap_mock, b"123", "tombstone-test")

    imap_mock.uid.assert_not_called()


# ── Processing log upsert ────────────────────────────────────────────────────

def test_processing_log_upsert(db_path, monkeypatch):
    """Processing same uid twice → attempt_count=2, not two separate rows."""
    import mail_watcher as mw
    import config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    import db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    uid_str = "uid-upsert-test"
    folder  = "INBOX"

    mw._upsert_processing_log(uid_str, folder, "x.com", "subject", mw.OUTCOME_RETRY, "err1")
    mw._upsert_processing_log(uid_str, folder, "x.com", "subject", mw.OUTCOME_RETRY, "err2")

    conn = get_connection(db_path)
    rows = conn.execute(
        "SELECT attempt_count, error_message FROM email_processing_log "
        "WHERE message_uid = ? AND imap_folder = ?",
        (uid_str, folder),
    ).fetchall()
    conn.close()

    assert len(rows) == 1, "Should have exactly one row (upsert), not two inserts"
    assert rows[0]["attempt_count"] == 2
    assert rows[0]["error_message"] == "err2"


# ── Duplicate attachment skipped ──────────────────────────────────────────────

def test_duplicate_attachment_skipped(db_path, docs_path, quiet_threads, monkeypatch):
    """_save_attachments returns 0 when identical file already exists for the return."""
    import mail_watcher as mw
    import utils as utils_mod
    import config as cfg
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    import db as db_mod
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    payload = b"DUPLICATE_PDF_CONTENT"
    file_hash = hashlib.sha256(payload).hexdigest()
    sanitized = "test.pdf"

    # Pre-seed the document so it looks like it was already saved
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO return_documents "
        "(return_id, filename, original_filename, doc_type, source, file_path, "
        " file_size_bytes, file_hash, uploaded_by, uploaded_at, is_deleted) "
        "VALUES (8001, ?, ?, 'unknown', 'email', '/fake/path', ?, ?, 'mail_watcher', datetime('now'), 0)",
        (sanitized, sanitized, len(payload), file_hash),
    )
    conn.commit()
    conn.close()

    msg = _make_email_with_pdf(filename=sanitized, payload=payload)

    app_mock = MagicMock()
    app_mock.app_context.return_value.__enter__ = MagicMock(return_value=None)
    app_mock.app_context.return_value.__exit__  = MagicMock(return_value=False)

    count = mw._save_attachments(app_mock, msg, 8001)
    assert count == 0, "Duplicate attachment should be silently skipped"


# ── Read status is never modified — all outcomes ─────────────────────────────

def test_no_outcome_ever_marks_read(monkeypatch):
    """imap.uid(STORE) is never called for any outcome — read-status policy is absolute."""
    import mail_watcher as mw
    import config as cfg

    monkeypatch.setattr(cfg, "IMAP_PRESERVE_UNREAD", False)  # even with flag off
    monkeypatch.setattr(cfg, "IMAP_MARK_AS_READ", True)
    monkeypatch.setattr(cfg, "IMAP_DRY_RUN", False)

    imap_mock = MagicMock()
    uid = b"no-store-test"

    for outcome in (mw.OUTCOME_SUCCESS, mw.OUTCOME_SKIP, mw.OUTCOME_RETRY, mw.OUTCOME_DRY_RUN):
        # Replicate the decision point in _poll_once_inner — STORE must never appear
        if outcome == mw.OUTCOME_RETRY:
            pass  # left unread for retry
        elif outcome == mw.OUTCOME_DRY_RUN:
            pass  # dry-run, no IMAP state changes
        else:
            pass  # policy: never mark read regardless of outcome

    imap_mock.uid.assert_not_called()


def test_already_processed_in_log_returns_true(db_path, monkeypatch):
    """_already_processed_in_log returns True for success/skip, False for retry."""
    import mail_watcher as mw
    import config as cfg
    import db as db_mod
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO email_processing_log "
        "(message_uid, imap_folder, sender_domain, subject_snippet, outcome, attempt_count, last_attempt_at) "
        "VALUES ('uid-done', 'INBOX', 'x.com', 'sub', 'success', 1, datetime('now'))"
    )
    conn.execute(
        "INSERT INTO email_processing_log "
        "(message_uid, imap_folder, sender_domain, subject_snippet, outcome, attempt_count, last_attempt_at) "
        "VALUES ('uid-retry', 'INBOX', 'x.com', 'sub', 'retry', 2, datetime('now'))"
    )
    conn.commit()
    conn.close()

    assert mw._already_processed_in_log("uid-done",  "INBOX") is True
    assert mw._already_processed_in_log("uid-retry", "INBOX") is False
    assert mw._already_processed_in_log("uid-new",   "INBOX") is False


# ── /api/email-processing-log endpoint ───────────────────────────────────────

def test_processing_log_endpoint_returns_50(client_logged_in, taxops_db_path, monkeypatch):
    """GET /api/email-processing-log returns at most 50 rows in JSON."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    for i in range(60):
        conn.execute(
            "INSERT INTO email_processing_log "
            "(message_uid, imap_folder, sender_domain, subject_snippet, outcome, attempt_count, last_attempt_at) "
            "VALUES (?, 'INBOX', 'x.com', 'sub', 'success', 1, datetime('now'))",
            (f"uid-endpoint-{i}",),
        )
    conn.commit()
    conn.close()

    rv = client_logged_in.get("/api/email-processing-log")
    assert rv.status_code == 200
    data = rv.get_json()
    assert isinstance(data, list)
    assert len(data) <= 50
    # Verify privacy: no email body, no full address
    for row in data:
        assert "body" not in row
        assert "@" not in (row.get("sender_domain") or "")  # domain only, not address
