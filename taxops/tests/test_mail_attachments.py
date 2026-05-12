"""Attachment saving in mail_watcher — dedupe + file_hash persistence."""

from __future__ import annotations

import hashlib
import logging
import threading
from email.message import EmailMessage
from pathlib import Path

import pytest

from db import get_connection, init_db


class _QuietThread(threading.Thread):
    """Prevent background classify threads from touching AI during tests."""

    def start(self) -> None:  # noqa: D401
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
def mail_test_return_id(taxops_db_path: str) -> int:
    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (601, 'Test', 'User', 'Test, User');
        """
    )
    conn.execute(
        """
        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor, verified, client_status,
            intake_date
        ) VALUES (
            8801, 601, 'm8801', 2025, '', 0, 'PROCESSING', '2026-01-01'
        );
        """
    )
    conn.commit()
    conn.close()
    return 8801


def _msg_with_pdf(filename: str, payload: bytes) -> EmailMessage:
    m = EmailMessage()
    m["Subject"] = "docs"
    m["From"] = "client@example.com"
    m.set_content("See attached.")
    m.add_attachment(
        payload,
        maintype="application",
        subtype="pdf",
        filename=filename,
    )
    return m


def test_save_attachments_writes_hash_and_counts(
    app,
    taxops_db_path,
    mail_test_return_id,
    _patch_enqueue,
    _patch_mail_thread,
    _docs_path,
):
    import mail_watcher as mw

    rid = mail_test_return_id
    payload = b"%PDF-1.4 unique-bytes-test"
    msg = _msg_with_pdf("w2.pdf", payload)

    count = mw._save_attachments(app, msg, rid)
    assert count == 1

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT filename, file_size_bytes, file_hash, is_deleted
            FROM return_documents WHERE return_id = ?
            """,
            (rid,),
        ).fetchone()
        assert row is not None
        assert row["file_hash"] == hashlib.sha256(payload).hexdigest()
        assert row["filename"] == "w2.pdf"
        assert row["file_size_bytes"] == len(payload)
        assert row["is_deleted"] == 0
    finally:
        conn.close()


def test_duplicate_two_identical_parts_skipped(
    caplog,
    app,
    mail_test_return_id,
    taxops_db_path,
    _patch_enqueue,
    _patch_mail_thread,
    _docs_path,
):
    import mail_watcher as mw

    rid = mail_test_return_id
    payload = b"%PDF-twice"
    hp = hashlib.sha256(payload).hexdigest()[:16]

    msg = EmailMessage()
    msg["Subject"] = "dup"
    msg["From"] = "c@example.com"
    msg.set_content("both same")
    msg.add_attachment(payload, maintype="application", subtype="pdf", filename="same.pdf")
    msg.add_attachment(payload, maintype="application", subtype="pdf", filename="same.pdf")

    with caplog.at_level(logging.INFO, logger="mail_watcher"):
        count = mw._save_attachments(app, msg, rid)

    assert count == 1
    conn = get_connection(taxops_db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM return_documents WHERE return_id = ? AND is_deleted = 0",
            (rid,),
        ).fetchone()["c"]
        assert n == 1
    finally:
        conn.close()

    skip_msgs = [
        r.message for r in caplog.records if "Skipping duplicate attachment" in r.message
    ]
    assert skip_msgs
    assert hashlib.sha256(payload).hexdigest() not in skip_msgs[0]
    assert hp in skip_msgs[0]


def test_strong_dedupe_different_filenames(
    caplog,
    app,
    mail_test_return_id,
    taxops_db_path,
    tmp_path: Path,
    _patch_enqueue,
    _patch_mail_thread,
    _docs_path,
):
    import mail_watcher as mw

    rid = mail_test_return_id
    payload = b"%PDF-rename-strong"
    h = hashlib.sha256(payload).hexdigest()

    fake_path = str(tmp_path / "existing.pdf")

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted
        )
        VALUES (?, ?, ?, 'unknown', 'email', ?, ?, ?, 'mail_watcher', '2026-01-02', NULL, 0)
        """,
        (
            rid,
            "first.pdf",
            "first.pdf",
            fake_path,
            len(payload),
            h,
        ),
    )
    conn.commit()
    conn.close()

    msg = _msg_with_pdf("different.pdf", payload)
    with caplog.at_level(logging.INFO, logger="mail_watcher"):
        count = mw._save_attachments(app, msg, rid)

    assert count == 0
    conn = get_connection(taxops_db_path)
    try:
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM return_documents WHERE return_id = ? AND is_deleted = 0",
            (rid,),
        ).fetchone()["c"]
        assert n == 1
    finally:
        conn.close()

    joined = "\n".join(r.message for r in caplog.records)
    assert "Skipping duplicate attachment" in joined


def test_cheap_dedupe_legacy_row_without_hash(
    caplog,
    app,
    mail_test_return_id,
    taxops_db_path,
    tmp_path: Path,
    _patch_enqueue,
    _patch_mail_thread,
    _docs_path,
):
    import mail_watcher as mw

    rid = mail_test_return_id
    sanitized = "legacy.pdf"
    payload = b"%PDF-legacy-null-hash"
    fake_path = str(tmp_path / "legacy.pdf")

    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted
        )
        VALUES (?, ?, ?, 'unknown', 'email', ?, ?, NULL, 'mail_watcher', '2026-01-02', NULL, 0)
        """,
        (rid, sanitized, sanitized, fake_path, len(payload)),
    )
    conn.commit()
    conn.close()

    msg = _msg_with_pdf(sanitized, payload)
    with caplog.at_level(logging.INFO, logger="mail_watcher"):
        count = mw._save_attachments(app, msg, rid)

    assert count == 0
    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT file_hash FROM return_documents
            WHERE return_id = ? AND filename = ?
            LIMIT 1
            """,
            (rid, sanitized),
        ).fetchone()
        assert row is not None
        assert row["file_hash"] is None
    finally:
        conn.close()

    joined = "\n".join(r.message for r in caplog.records)
    assert "Skipping duplicate attachment" in joined
