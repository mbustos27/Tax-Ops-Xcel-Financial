"""Attachment saving in mail_watcher — _save_to_inbox() holding-area writes.

Rewritten for the holding-area architecture: attachments are saved to
EMAIL_INBOX_DIR and a row is inserted into email_inbox with is_assigned=0 —
there is no client matching or return_documents write at this stage (that
only happens later, when staff assigns the item via /api/email-inbox/<id>/assign).
"""
from __future__ import annotations

import threading
from email.message import EmailMessage
from pathlib import Path

import pytest

from db import get_connection


class _QuietThread(threading.Thread):
    """Prevent any background thread from actually starting during tests."""

    def start(self) -> None:
        return None


@pytest.fixture
def _patch_mail_thread(monkeypatch: pytest.MonkeyPatch):
    import mail_watcher as mw

    monkeypatch.setattr(mw.threading, "Thread", _QuietThread)


@pytest.fixture
def inbox_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    import config as cfg

    root = tmp_path / "email_inbox"
    root.mkdir(parents=True, exist_ok=True)
    monkeypatch.setattr(cfg, "EMAIL_INBOX_DIR", str(root))
    return root


def _msg_with_attachment(
    filename: str,
    payload: bytes = b"%PDF-1.4 test",
    maintype: str = "application",
    subtype: str = "pdf",
) -> EmailMessage:
    m = EmailMessage()
    m["Subject"] = "docs"
    m["From"] = "client@example.com"
    m.set_content("See attached.")
    m.add_attachment(payload, maintype=maintype, subtype=subtype, filename=filename)
    return m


def test_save_to_inbox_writes_one_row_per_file(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """A single allowed attachment produces exactly one email_inbox row, unassigned."""
    import mail_watcher as mw

    payload = b"%PDF-1.4 unique-bytes-test"
    msg = _msg_with_attachment("w2.pdf", payload)

    count = mw._save_to_inbox(app, msg, "client@example.com", "example.com", "docs", "2026-01-01T00:00:00+00:00")
    assert count == 1

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            "SELECT filename, original_filename, file_size_bytes, is_assigned, is_deleted, "
            "sender_email, sender_domain FROM email_inbox"
        ).fetchone()
        assert row is not None
        assert row["original_filename"] == "w2.pdf"
        assert row["file_size_bytes"] == len(payload)
        assert row["is_assigned"] == 0
        assert row["is_deleted"] == 0
        assert row["sender_email"] == "client@example.com"
        assert row["sender_domain"] == "example.com"
    finally:
        conn.close()


def test_save_to_inbox_writes_file_to_disk_with_correct_bytes(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """The saved file on disk matches the attachment payload exactly."""
    import mail_watcher as mw

    payload = b"%PDF-1.4 exact-bytes-check"
    msg = _msg_with_attachment("statement.pdf", payload)

    mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")

    conn = get_connection(taxops_db_path)
    file_path = conn.execute("SELECT file_path FROM email_inbox").fetchone()["file_path"]
    conn.close()

    assert Path(file_path).read_bytes() == payload
    assert Path(file_path).parent == inbox_dir


@pytest.mark.parametrize("ext,ok", [
    (".pdf", True), (".jpg", True), (".jpeg", True), (".png", True),
    (".exe", False), (".docx", False), (".zip", False), (".txt", False),
])
def test_allowed_extensions_only(app, taxops_db_path, inbox_dir, _patch_mail_thread, ext, ok):
    """Only pdf/jpg/jpeg/png are saved; everything else is silently skipped."""
    import mail_watcher as mw

    msg = _msg_with_attachment(f"file{ext}", b"content", maintype="application", subtype="octet-stream")
    count = mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")

    assert count == (1 if ok else 0)
    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == (1 if ok else 0)


def test_filename_gets_timestamp_prefix_and_sanitized(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """Saved filename is `<UTC-timestamp>_<sanitized-original-name>`."""
    import mail_watcher as mw

    msg = _msg_with_attachment("My Report (final)!.pdf")
    mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT filename FROM email_inbox").fetchone()
    conn.close()

    # Sanitize replaces non-alnum with '_' and lowercases the extension.
    assert row["filename"].endswith("My_Report__final__.pdf") or "_My_Report__final__.pdf" in row["filename"]
    # Timestamp prefix: YYYYMMDD_HHMMSS_
    prefix = row["filename"].split("My_Report", 1)[0]
    assert len(prefix) == len("20260101_000000_")
    assert prefix[8] == "_" and prefix[-1] == "_"


def test_filename_collision_gets_counter_suffix(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """Two attachments that sanitize to the same on-disk name in the same second
    get a `_1`, `_2`, ... counter suffix instead of overwriting each other."""
    import mail_watcher as mw

    msg = EmailMessage()
    msg["Subject"] = "two files"
    msg["From"] = "client@example.com"
    msg.set_content("See attached.")
    msg.add_attachment(b"first", maintype="application", subtype="pdf", filename="doc.pdf")
    msg.add_attachment(b"second", maintype="application", subtype="pdf", filename="doc.pdf")

    count = mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")
    assert count == 2

    conn = get_connection(taxops_db_path)
    rows = conn.execute("SELECT filename, file_path FROM email_inbox ORDER BY id").fetchall()
    conn.close()

    assert len(rows) == 2
    assert rows[0]["filename"] != rows[1]["filename"], "Collision must not silently overwrite the first file"
    for r in rows:
        assert Path(r["file_path"]).exists()


def test_scrub_ssn_from_dict_applied_to_filename(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """A filename containing an SSN-shaped pattern is scrubbed before being stored."""
    import mail_watcher as mw

    msg = _msg_with_attachment("SSN 123-45-6789 W2.pdf")
    mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT original_filename FROM email_inbox").fetchone()
    conn.close()

    assert "123-45-6789" not in row["original_filename"]
    assert "REDACTED" in row["original_filename"]


def test_no_attachment_returns_zero(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """An email with no attachment parts saves nothing and returns count=0."""
    import mail_watcher as mw

    msg = EmailMessage()
    msg["Subject"] = "just text"
    msg["From"] = "client@example.com"
    msg.set_content("No files here.")

    count = mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")
    assert count == 0

    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 0


def test_multiple_valid_attachments_all_saved(app, taxops_db_path, inbox_dir, _patch_mail_thread):
    """Multiple distinct allowed attachments in one message all get saved."""
    import mail_watcher as mw

    msg = EmailMessage()
    msg["Subject"] = "multi"
    msg["From"] = "client@example.com"
    msg.set_content("See attached.")
    msg.add_attachment(b"pdf-bytes", maintype="application", subtype="pdf", filename="a.pdf")
    msg.add_attachment(b"jpg-bytes", maintype="image", subtype="jpeg", filename="b.jpg")
    msg.add_attachment(b"png-bytes", maintype="image", subtype="png", filename="c.png")

    count = mw._save_to_inbox(app, msg, "a@b.com", "b.com", "sub", "2026-01-01T00:00:00+00:00")
    assert count == 3

    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox WHERE is_assigned=0 AND is_deleted=0").fetchone()["c"]
    conn.close()
    assert n == 3
