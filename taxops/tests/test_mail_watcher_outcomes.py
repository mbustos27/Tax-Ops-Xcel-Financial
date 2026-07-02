"""mail_watcher outcome model — holding-area poll cycle.

Covers the current architecture (see taxops-invariants.mdc / CODEBASE_OVERVIEW.md):
  - success / skip / no_attachment are terminal outcomes — never retried
  - retry leaves the UID unclaimed (memo AND email_processing_log) so the
    next poll cycle re-fetches it
  - IMAP_DRY_RUN performs zero writes of any kind (Phase 0.4 fix — this was
    previously dead config; _poll_once_inner now honors it)
  - the in-process memo is only populated *after* a confirmed terminal DB
    log write (claim-after-success — audit finding C1)
  - Gmail category-folder routing ('skip' folders never get fetched)
  - _is_promotional / _is_drive_share suppression, at both the unit level
    and end-to-end through a full poll cycle
  - _mark_read is a permanent tombstone regardless of outcome
"""
from __future__ import annotations

import email.message
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from db import get_connection, init_db


# ── Fake IMAP server ──────────────────────────────────────────────────────────

class _FakeImap:
    """Minimal stand-in for imaplib.IMAP4_SSL covering exactly what
    _poll_once_inner calls: login, list, select, uid(SEARCH/FETCH), logout."""

    def __init__(self, folders: dict[str, list[tuple[bytes, bytes]]], fail_fetch_uids: set[str] | None = None):
        # folders: {folder_name: [(uid_bytes, raw_message_bytes), ...]}
        self._folders = folders
        self._fail_fetch_uids = fail_fetch_uids or set()
        self.selected: str | None = None
        self.fetch_calls: list[str] = []

    def login(self, user, password):
        return ("OK", [b"Logged in"])

    def list(self):
        items = [f'(\\HasNoChildren) "/" "{name}"'.encode() for name in self._folders]
        return ("OK", items)

    def select(self, folder):
        name = folder.strip('"')
        if name in self._folders:
            self.selected = name
            return ("OK", [b"1"])
        return ("NO", [b"no such folder"])

    def uid(self, command, *args):
        if command == "SEARCH":
            uids = [u for u, _ in self._folders.get(self.selected, [])]
            if not uids:
                return ("OK", [b""])
            return ("OK", [b" ".join(uids)])
        if command == "FETCH":
            uid_arg = args[0]
            uid_str = uid_arg.decode("ascii") if isinstance(uid_arg, bytes) else str(uid_arg)
            self.fetch_calls.append(uid_str)
            if uid_str in self._fail_fetch_uids:
                return ("OK", [])
            for uid, raw in self._folders.get(self.selected, []):
                if uid.decode("ascii") == uid_str:
                    return ("OK", [(b"1 (BODY[])", raw)])
            return ("OK", [])
        raise AssertionError(f"Unexpected IMAP command: {command}")

    def logout(self):
        return ("OK", [b"bye"])


def _raw_message(sender: str = "client@example.com", subject: str = "docs",
                  attachment: bytes | None = b"%PDF-1.4 test") -> bytes:
    msg = email.message.EmailMessage()
    msg["From"] = sender
    msg["Subject"] = subject
    msg.set_content("body")
    if attachment is not None:
        msg.add_attachment(attachment, maintype="application", subtype="pdf", filename="doc.pdf")
    return msg.as_bytes()


class _QuietThread(threading.Thread):
    def start(self) -> None:
        return None


@pytest.fixture
def poll_env(taxops_db_path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """Configure mail_watcher for a single-folder, non-Gmail-category poll cycle
    against a temp DB and temp EMAIL_INBOX_DIR."""
    import config as cfg
    import mail_watcher as mw

    monkeypatch.setattr(cfg, "IMAP_HOST", "imap.example.com")
    monkeypatch.setattr(cfg, "IMAP_PORT", 993)
    monkeypatch.setattr(cfg, "IMAP_USER", "office@example.com")
    monkeypatch.setattr(cfg, "IMAP_PASS", "secret")
    monkeypatch.setattr(cfg, "IMAP_FOLDERS", ["INBOX"])
    monkeypatch.setattr(cfg, "USE_GMAIL_CATEGORIES", False)
    monkeypatch.setattr(cfg, "IMAP_DRY_RUN", False)
    inbox_dir = tmp_path / "email_inbox"
    inbox_dir.mkdir()
    monkeypatch.setattr(cfg, "EMAIL_INBOX_DIR", str(inbox_dir))
    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)
    mw._processed_uids.clear()
    mw._processed_uids_fifo.clear()
    return inbox_dir


def _install_fake_imap(monkeypatch: pytest.MonkeyPatch, fake: _FakeImap) -> _FakeImap:
    import mail_watcher as mw
    monkeypatch.setattr(mw.imaplib, "IMAP4_SSL", lambda host, port: fake)
    return fake


def _log_row(db_path: str, uid: str, folder: str = "INBOX"):
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT outcome, attempt_count FROM email_processing_log "
        "WHERE message_uid=? AND imap_folder=?",
        (uid, folder),
    ).fetchone()
    conn.close()
    return row


# ── Outcome constants (kept from prior suite) ────────────────────────────────

def test_outcome_constants_defined():
    import mail_watcher as mw
    assert mw.OUTCOME_SUCCESS == "success"
    assert mw.OUTCOME_SKIP == "skip"
    assert mw.OUTCOME_RETRY == "retry"
    assert mw.OUTCOME_DRY_RUN == "dry_run"
    assert mw.OUTCOME_NO_ATTACHMENT == "no_attachment"


# ── success / no_attachment / skip are terminal, never retried ──────────────

def test_success_outcome_is_terminal_and_saved_to_inbox(taxops_db_path, poll_env, monkeypatch):
    import mail_watcher as mw

    fake = _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"1", _raw_message())],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "1")
    assert row is not None and row["outcome"] == mw.OUTCOME_SUCCESS
    assert mw._already_processed_in_log("1", "INBOX") is True
    assert mw._uid_in_memo("INBOX", "1") is True

    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 1

    # Second poll cycle, same UID still "returned" by the server (as if
    # unseen) — must NOT be re-fetched or re-saved.
    fake.fetch_calls.clear()
    mw._poll_once_inner(MagicMock())
    assert fake.fetch_calls == [], "Terminal (success) UID must not be re-fetched on the next poll"
    conn = get_connection(taxops_db_path)
    n2 = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n2 == 1, "No duplicate email_inbox row from re-processing a terminal UID"


def test_no_attachment_outcome_is_terminal(taxops_db_path, poll_env, monkeypatch):
    import mail_watcher as mw

    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"2", _raw_message(attachment=None))],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "2")
    assert row is not None and row["outcome"] == mw.OUTCOME_NO_ATTACHMENT
    assert mw._already_processed_in_log("2", "INBOX") is True
    assert mw._uid_in_memo("INBOX", "2") is True


def test_promotional_sender_yields_skip_end_to_end(taxops_db_path, poll_env, monkeypatch):
    import config as cfg
    import mail_watcher as mw

    promo_domain = next(iter(cfg.KNOWN_PROMOTIONAL_DOMAINS))
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"3", _raw_message(sender=f"deals@{promo_domain}"))],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "3")
    assert row is not None and row["outcome"] == mw.OUTCOME_SKIP
    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 0, "Promotional sender must not reach the inbox holding area"


# ── retry leaves the UID unclaimed ───────────────────────────────────────────

def test_retry_leaves_uid_unclaimed_and_is_reprocessed(taxops_db_path, poll_env, monkeypatch):
    import mail_watcher as mw

    fake = _install_fake_imap(monkeypatch, _FakeImap(
        {"INBOX": [(b"4", _raw_message())]},
        fail_fetch_uids={"4"},
    ))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "4")
    assert row is not None and row["outcome"] == mw.OUTCOME_RETRY
    assert mw._already_processed_in_log("4", "INBOX") is False, "retry must not be terminal"
    assert mw._uid_in_memo("INBOX", "4") is False, "retry must leave the UID unclaimed in the memo"

    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 0

    # Fix the "server" (fetch now succeeds) and poll again — must be retried.
    fake._fail_fetch_uids.clear()
    mw._poll_once_inner(MagicMock())
    row2 = _log_row(taxops_db_path, "4")
    assert row2["outcome"] == mw.OUTCOME_SUCCESS
    assert row2["attempt_count"] == 2, "attempt_count increments across retry attempts (upsert)"


# ── IMAP_DRY_RUN performs zero writes ────────────────────────────────────────

def test_dry_run_writes_nothing(taxops_db_path, poll_env, monkeypatch):
    """IMAP_DRY_RUN=true: no disk write, no email_inbox row, no processing-log
    row, and the UID is left unclaimed so a real (non-dry-run) poll will still
    process it."""
    import config as cfg
    import mail_watcher as mw

    monkeypatch.setattr(cfg, "IMAP_DRY_RUN", True)
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"5", _raw_message())],
    }))

    mw._poll_once_inner(MagicMock())

    assert _log_row(taxops_db_path, "5") is None, "dry run must not write email_processing_log"
    assert mw._uid_in_memo("INBOX", "5") is False, "dry run must not claim the UID"

    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 0, "dry run must not write email_inbox"
    assert list(poll_env.iterdir()) == [], "dry run must not write any file to EMAIL_INBOX_DIR"


# ── memo-vs-DB dedup layering ─────────────────────────────────────────────────

def test_db_log_dedup_populates_memo_without_refetch(taxops_db_path, poll_env, monkeypatch):
    """A UID already terminal in email_processing_log (but not yet in the
    in-process memo, e.g. after a restart) is skipped without a re-fetch, and
    the memo is populated so subsequent polls skip it even faster."""
    import mail_watcher as mw

    mw._upsert_processing_log("6", "INBOX", "example.com", "old", mw.OUTCOME_SUCCESS)
    assert mw._uid_in_memo("INBOX", "6") is False  # simulates a fresh process, e.g. after restart

    fake = _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"6", _raw_message())],
    }))

    mw._poll_once_inner(MagicMock())

    assert fake.fetch_calls == [], "DB-terminal UID must be skipped via the log check, never fetched"
    assert mw._uid_in_memo("INBOX", "6") is True, "memo should be back-filled from the DB check"


def test_memo_claimed_only_after_log_write_succeeds(taxops_db_path, poll_env, monkeypatch):
    """Claim-after-success (audit finding C1): if the processing-log write
    itself fails, the UID must NOT be added to the memo."""
    import mail_watcher as mw

    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"7", _raw_message())],
    }))
    monkeypatch.setattr(mw, "_upsert_processing_log", MagicMock(side_effect=RuntimeError("db down")))

    mw._poll_once_inner(MagicMock())

    assert mw._uid_in_memo("INBOX", "7") is False, "a failed log write must never claim the UID"


# ── Gmail category-folder routing ────────────────────────────────────────────

def test_category_folder_skip_never_fetches(taxops_db_path, tmp_path, monkeypatch):
    """Folders mapped to 'skip' in GMAIL_CATEGORY_FOLDERS are auto-logged as
    OUTCOME_SKIP without ever calling FETCH."""
    import config as cfg
    import mail_watcher as mw

    monkeypatch.setattr(cfg, "IMAP_HOST", "imap.example.com")
    monkeypatch.setattr(cfg, "IMAP_USER", "office@example.com")
    monkeypatch.setattr(cfg, "IMAP_PASS", "secret")
    monkeypatch.setattr(cfg, "USE_GMAIL_CATEGORIES", True)
    monkeypatch.setattr(cfg, "IMAP_DRY_RUN", False)
    inbox_dir = tmp_path / "email_inbox"
    inbox_dir.mkdir()
    monkeypatch.setattr(cfg, "EMAIL_INBOX_DIR", str(inbox_dir))
    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)
    mw._processed_uids.clear()
    mw._processed_uids_fifo.clear()

    fake = _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"8", _raw_message())],
        "CATEGORY_PROMOTIONS": [(b"9", _raw_message())],
    }))

    mw._poll_once_inner(MagicMock())

    assert "9" not in fake.fetch_calls, "'skip'-handling folders must never be fetched"
    assert "8" in fake.fetch_calls, "'full_processing' folders must still be fetched"
    row9 = _log_row(taxops_db_path, "9", "CATEGORY_PROMOTIONS")
    assert row9 is not None and row9["outcome"] == mw.OUTCOME_SKIP


# ── _is_promotional / _is_drive_share unit-level suppression ────────────────

def test_is_promotional_known_domain_blocked():
    import config as cfg
    import mail_watcher as mw
    domain = next(iter(cfg.KNOWN_PROMOTIONAL_DOMAINS))
    assert mw._is_promotional(domain) is True


def test_is_promotional_personal_domain_never_blocked(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset({"gmail.com"}))
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset({"gmail.com"}))
    assert mw._is_promotional("gmail.com") is False, "personal domains are never suppressed, even if also listed"


def test_is_promotional_mass_mailing_prefix_blocked(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ("mail.",))
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    assert mw._is_promotional("mail.somevendor.com") is True


def test_is_drive_share_detects_share_notification():
    import mail_watcher as mw
    assert mw._is_drive_share("Jane has shared a file with you", "") is True
    assert mw._is_drive_share("My W2", "See attached please") is False


# ── _mark_read tombstone ──────────────────────────────────────────────────────

def test_mark_read_tombstone_never_calls_store():
    """_mark_read is a disabled tombstone (empty body) — calling it is a no-op
    and never touches the imap object, regardless of arguments."""
    import mail_watcher as mw

    imap_mock = MagicMock()
    mw._mark_read(imap_mock, b"123", "tombstone-test")
    imap_mock.uid.assert_not_called()
    imap_mock.store.assert_not_called()


def test_no_outcome_path_calls_imap_store(taxops_db_path, poll_env, monkeypatch):
    """End-to-end: across success, skip (promotional), and no_attachment
    outcomes in one poll cycle, imap.uid('STORE', ...) is never invoked —
    read-status policy is enforced at the dispatch loop, not just in the
    (now-unused) _mark_read tombstone."""
    import config as cfg
    import mail_watcher as mw

    promo_domain = next(iter(cfg.KNOWN_PROMOTIONAL_DOMAINS))
    fake = _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [
            (b"10", _raw_message(sender="client@example.com")),
            (b"11", _raw_message(sender=f"deals@{promo_domain}")),
            (b"12", _raw_message(sender="client@example.com", attachment=None)),
        ],
    }))
    orig_uid = fake.uid
    monkeypatch.setattr(fake, "uid", lambda cmd, *a: (_ for _ in ()).throw(AssertionError("STORE must never be called"))
                         if cmd == "STORE" else orig_uid(cmd, *a))

    mw._poll_once_inner(MagicMock())

    assert _log_row(taxops_db_path, "10")["outcome"] == mw.OUTCOME_SUCCESS
    assert _log_row(taxops_db_path, "11")["outcome"] == mw.OUTCOME_SKIP
    assert _log_row(taxops_db_path, "12")["outcome"] == mw.OUTCOME_NO_ATTACHMENT


# ── Processing log helpers (kept from prior suite) ───────────────────────────

def test_processing_log_upsert(taxops_db_path):
    import mail_watcher as mw

    mw._upsert_processing_log("uid-upsert-test", "INBOX", "x.com", "subject", mw.OUTCOME_RETRY, "err1")
    mw._upsert_processing_log("uid-upsert-test", "INBOX", "x.com", "subject", mw.OUTCOME_RETRY, "err2")

    conn = get_connection(taxops_db_path)
    rows = conn.execute(
        "SELECT attempt_count, error_message FROM email_processing_log "
        "WHERE message_uid = ? AND imap_folder = ?",
        ("uid-upsert-test", "INBOX"),
    ).fetchall()
    conn.close()

    assert len(rows) == 1, "Should have exactly one row (upsert), not two inserts"
    assert rows[0]["attempt_count"] == 2
    assert rows[0]["error_message"] == "err2"


def test_already_processed_in_log_returns_true(taxops_db_path):
    import mail_watcher as mw

    mw._upsert_processing_log("uid-done", "INBOX", "x.com", "sub", mw.OUTCOME_SUCCESS)
    mw._upsert_processing_log("uid-retry", "INBOX", "x.com", "sub", mw.OUTCOME_RETRY)

    assert mw._already_processed_in_log("uid-done", "INBOX") is True
    assert mw._already_processed_in_log("uid-retry", "INBOX") is False
    assert mw._already_processed_in_log("uid-new", "INBOX") is False


# ── Poll-lock skip counter (moved from the now-deleted test_email_stability.py) ──

def test_poll_skip_counter_increments():
    """_poll_skip_count increments when the poll lock is already held."""
    import mail_watcher as mw
    orig_count = mw._poll_skip_count
    try:
        mw._poll_lock.acquire()
        mw._poll_once(None)  # will skip because lock is held
        assert mw._poll_skip_count > orig_count
    finally:
        mw._poll_lock.release()
        mw._poll_skip_count = 0


def test_mail_watcher_status_includes_poll_skipped():
    """mail_watcher_status() must expose poll_skipped count."""
    import mail_watcher as mw
    status = mw.mail_watcher_status()
    assert "poll_skipped" in status
    assert isinstance(status["poll_skipped"], int)
