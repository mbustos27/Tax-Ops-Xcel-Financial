"""Phase 2.1 (email system revamp): email_sender_rules wiring + admin UI.

Covers:
  - _load_sender_rules() bucketing by (rule_scope, action) and graceful
    degrade-to-empty on any DB error.
  - _suppression_decision() precedence: allow > block > hardcoded
    promotional lists > default pass. Personal-domain block rules are
    ignored as a defensive backstop.
  - End-to-end through _poll_once_inner: a DB block rule suppresses mail
    that the hardcoded lists would have let through, and vice versa for
    an allow rule against a hardcoded promotional domain.
  - The suppression_reason column is populated for every skip outcome
    (sender_rule_block / known_promotional / drive_share).
  - Admin-gated CRUD routes: list page, create (with scope/action/personal
    -domain validation and duplicate rejection), delete.
"""
from __future__ import annotations

import email.message
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from db import get_connection


# ── Reuse the fake IMAP harness pattern from test_mail_watcher_outcomes ─────

class _FakeImap:
    def __init__(self, folders: dict[str, list[tuple[bytes, bytes]]]):
        self._folders = folders
        self.selected: str | None = None

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
            for uid, raw in self._folders.get(self.selected, []):
                if uid.decode("ascii") == uid_str:
                    return ("OK", [(b"1 (BODY[])", raw)])
            return ("OK", [])
        raise AssertionError(f"Unexpected IMAP command: {command}")

    def logout(self):
        return ("OK", [b"bye"])


def _raw_message(sender: str = "client@example.com", subject: str = "docs") -> bytes:
    msg = email.message.EmailMessage()
    msg["From"] = sender
    msg["Subject"] = subject
    msg.set_content("body")
    msg.add_attachment(b"%PDF-1.4 test", maintype="application", subtype="pdf", filename="doc.pdf")
    return msg.as_bytes()


class _QuietThread(threading.Thread):
    def start(self) -> None:
        return None


@pytest.fixture
def poll_env(taxops_db_path: str, tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
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


def _install_fake_imap(monkeypatch, fake):
    import mail_watcher as mw
    monkeypatch.setattr(mw.imaplib, "IMAP4_SSL", lambda host, port: fake)
    return fake


def _add_rule(db_path, domain, rule_scope="domain", action="block", note=None):
    conn = get_connection(db_path)
    conn.execute(
        "INSERT INTO email_sender_rules (domain, rule_scope, action, note, created_by, created_at) "
        "VALUES (?, ?, ?, ?, 'tester', '2026-01-01T00:00:00Z')",
        (domain, rule_scope, action, note),
    )
    conn.commit()
    conn.close()


def _log_row(db_path, uid, folder="INBOX"):
    conn = get_connection(db_path)
    row = conn.execute(
        "SELECT outcome, suppression_reason FROM email_processing_log "
        "WHERE message_uid=? AND imap_folder=?",
        (uid, folder),
    ).fetchone()
    conn.close()
    return row


# ── _load_sender_rules ───────────────────────────────────────────────────────

def test_load_sender_rules_buckets_by_scope_and_action(taxops_db_path):
    import mail_watcher as mw

    _add_rule(taxops_db_path, "goodvendor.com", "domain", "allow")
    _add_rule(taxops_db_path, "badvendor.com", "domain", "block")
    _add_rule(taxops_db_path, "vip@gmail.com", "address", "allow")
    _add_rule(taxops_db_path, "spammer@gmail.com", "address", "block")

    rules = mw._load_sender_rules()
    assert rules["allow_domains"] == frozenset({"goodvendor.com"})
    assert rules["block_domains"] == frozenset({"badvendor.com"})
    assert rules["allow_addresses"] == frozenset({"vip@gmail.com"})
    assert rules["block_addresses"] == frozenset({"spammer@gmail.com"})


def test_load_sender_rules_degrades_to_empty_on_db_error(taxops_db_path, monkeypatch):
    import mail_watcher as mw

    class _Boom:
        def execute(self, *a, **k):
            raise Exception("db unavailable")
        def close(self):
            pass

    monkeypatch.setattr(mw, "get_connection", lambda *a, **k: _Boom(), raising=False)
    import db as db_mod
    monkeypatch.setattr(db_mod, "get_connection", lambda *a, **k: _Boom())

    rules = mw._load_sender_rules()
    assert rules == {
        "allow_domains": frozenset(), "allow_addresses": frozenset(),
        "block_domains": frozenset(), "block_addresses": frozenset(),
    }


# ── _suppression_decision precedence ─────────────────────────────────────────

def test_allow_beats_hardcoded_block(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset({"vendor.com"}))
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    rules = {"allow_domains": frozenset({"vendor.com"}), "allow_addresses": frozenset(),
             "block_domains": frozenset(), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision("deals@vendor.com", "vendor.com", rules)
    assert suppressed is False
    assert reason is None


def test_block_rule_suppresses_with_reason(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    rules = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset({"badvendor.com"}), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision("x@badvendor.com", "badvendor.com", rules)
    assert suppressed is True
    assert reason == "sender_rule_block"


def test_address_block_rule_beats_default_pass(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset({"gmail.com"}))
    rules = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset(), "block_addresses": frozenset({"spammer@gmail.com"})}
    suppressed, reason = mw._suppression_decision("spammer@gmail.com", "gmail.com", rules)
    assert suppressed is True
    assert reason == "sender_rule_block"


def test_personal_domain_block_rule_ignored_defensively(monkeypatch):
    """Even if a domain-level block rule for a personal domain somehow exists
    in the DB, suppression must never honor it — only address-level blocks
    can suppress personal-domain senders."""
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset({"gmail.com"}))
    rules = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset({"gmail.com"}), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision("anyone@gmail.com", "gmail.com", rules)
    assert suppressed is False
    assert reason is None


def test_drive_share_beats_hardcoded_promotional(monkeypatch):
    """Precedence: drive-share suppression outranks the hardcoded promotional
    lists, so a drive-share notification from a domain that also happens to
    be in KNOWN_PROMOTIONAL_DOMAINS is still reported as 'drive_share'."""
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset({"google.com"}))
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    empty = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset(), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision(
        "notifications@google.com", "google.com", empty,
        subject="Someone has shared a file with you", body_text="",
    )
    assert suppressed is True
    assert reason == "drive_share"


def test_allow_rule_overrides_drive_share_suppression(monkeypatch):
    """Precedence: an explicit staff allow rule beats drive-share
    suppression too — allow is the highest-precedence layer."""
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    rules = {"allow_domains": frozenset({"google.com"}), "allow_addresses": frozenset(),
             "block_domains": frozenset(), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision(
        "notifications@google.com", "google.com", rules,
        subject="Someone has shared a file with you", body_text="",
    )
    assert suppressed is False
    assert reason is None


def test_block_rule_overrides_drive_share_suppression_reason(monkeypatch):
    """A staff block rule takes precedence over drive-share detection, so the
    logged reason is 'sender_rule_block', not 'drive_share'."""
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset())
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    rules = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset({"google.com"}), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision(
        "notifications@google.com", "google.com", rules,
        subject="Someone has shared a file with you", body_text="",
    )
    assert suppressed is True
    assert reason == "sender_rule_block"


def test_falls_through_to_hardcoded_promotional_when_no_rule_matches(monkeypatch):
    import config as cfg
    import mail_watcher as mw
    monkeypatch.setattr(cfg, "KNOWN_PROMOTIONAL_DOMAINS", frozenset({"vendor.com"}))
    monkeypatch.setattr(cfg, "MASS_MAILING_PREFIXES", ())
    monkeypatch.setattr(cfg, "PERSONAL_EMAIL_DOMAINS", frozenset())
    empty = {"allow_domains": frozenset(), "allow_addresses": frozenset(),
             "block_domains": frozenset(), "block_addresses": frozenset()}
    suppressed, reason = mw._suppression_decision("x@vendor.com", "vendor.com", empty)
    assert suppressed is True
    assert reason == "known_promotional"


# ── End-to-end through _poll_once_inner ──────────────────────────────────────

def test_end_to_end_block_rule_suppresses_non_hardcoded_domain(taxops_db_path, poll_env, monkeypatch):
    import mail_watcher as mw
    _add_rule(taxops_db_path, "unwantedvendor.com", "domain", "block")
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"20", _raw_message(sender="hello@unwantedvendor.com"))],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "20")
    assert row is not None and row["outcome"] == mw.OUTCOME_SKIP
    assert row["suppression_reason"] == "sender_rule_block"
    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 0


def test_end_to_end_allow_rule_overrides_hardcoded_promotional(taxops_db_path, poll_env, monkeypatch):
    import config as cfg
    import mail_watcher as mw

    promo_domain = next(iter(cfg.KNOWN_PROMOTIONAL_DOMAINS))
    _add_rule(taxops_db_path, promo_domain, "domain", "allow")
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"21", _raw_message(sender=f"deals@{promo_domain}"))],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "21")
    assert row is not None and row["outcome"] == mw.OUTCOME_SUCCESS
    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 1, "an explicit allow rule must override the hardcoded promotional list"


def test_end_to_end_known_promotional_reason_recorded(taxops_db_path, poll_env, monkeypatch):
    import config as cfg
    import mail_watcher as mw
    promo_domain = next(iter(cfg.KNOWN_PROMOTIONAL_DOMAINS))
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [(b"22", _raw_message(sender=f"deals@{promo_domain}"))],
    }))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "22")
    assert row["outcome"] == mw.OUTCOME_SKIP
    assert row["suppression_reason"] == "known_promotional"


def test_end_to_end_drive_share_reason_recorded(taxops_db_path, poll_env, monkeypatch):
    import mail_watcher as mw
    msg = email.message.EmailMessage()
    msg["From"] = "notifications@google.com"
    msg["Subject"] = "Someone has shared a file with you"
    msg.set_content("open it here")
    _install_fake_imap(monkeypatch, _FakeImap({"INBOX": [(b"23", msg.as_bytes())]}))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "23")
    assert row["outcome"] == mw.OUTCOME_SKIP
    assert row["suppression_reason"] == "drive_share"


def test_end_to_end_allow_rule_overrides_drive_share_suppression(taxops_db_path, poll_env, monkeypatch):
    """An explicit staff allow rule for the sender must let the message's
    attachments through even though the subject looks like a Drive share
    notification — allow is the top-precedence layer end-to-end."""
    import mail_watcher as mw
    _add_rule(taxops_db_path, "shares.com", "domain", "allow")
    msg = email.message.EmailMessage()
    msg["From"] = "notifications@shares.com"
    msg["Subject"] = "Someone has shared a file with you"
    msg.set_content("open it here")
    msg.add_attachment(b"%PDF-1.4 test", maintype="application", subtype="pdf", filename="doc.pdf")
    _install_fake_imap(monkeypatch, _FakeImap({"INBOX": [(b"24", msg.as_bytes())]}))

    mw._poll_once_inner(MagicMock())

    row = _log_row(taxops_db_path, "24")
    assert row is not None and row["outcome"] == mw.OUTCOME_SUCCESS
    conn = get_connection(taxops_db_path)
    n = conn.execute("SELECT COUNT(*) c FROM email_inbox").fetchone()["c"]
    conn.close()
    assert n == 1, "allow rule must override drive-share suppression end-to-end"


def test_one_rules_select_per_poll_cycle_not_per_message(taxops_db_path, poll_env, monkeypatch):
    """_load_sender_rules must be called exactly once per _poll_once_inner
    call regardless of how many messages are in the cycle."""
    import mail_watcher as mw
    calls = []
    orig = mw._load_sender_rules

    def _counted():
        calls.append(1)
        return orig()

    monkeypatch.setattr(mw, "_load_sender_rules", _counted)
    _install_fake_imap(monkeypatch, _FakeImap({
        "INBOX": [
            (b"30", _raw_message(sender="a@example.com")),
            (b"31", _raw_message(sender="b@example.com")),
            (b"32", _raw_message(sender="c@example.com")),
        ],
    }))

    mw._poll_once_inner(MagicMock())
    assert len(calls) == 1


# ── Admin UI routes ───────────────────────────────────────────────────────────

def test_sender_rules_page_requires_login(client):
    resp = client.get("/admin/sender-rules")
    assert resp.status_code in (302, 401, 403)


def test_sender_rules_page_loads_for_admin(client_logged_in, taxops_db_path):
    _add_rule(taxops_db_path, "vendor.com", "domain", "block", "test note")
    resp = client_logged_in.get("/admin/sender-rules")
    assert resp.status_code == 200
    assert b"vendor.com" in resp.data


def test_create_rule_success(client_logged_in, taxops_db_path):
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "newvendor.com", "rule_scope": "domain", "action": "block", "note": "spam"},
    )
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT * FROM email_sender_rules WHERE domain = 'newvendor.com'").fetchone()
    conn.close()
    assert row is not None
    assert row["rule_scope"] == "domain"
    assert row["action"] == "block"


def test_create_rule_rejects_personal_domain_block(client_logged_in):
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "gmail.com", "rule_scope": "domain", "action": "block"},
    )
    assert resp.status_code == 400
    assert "address level" in resp.get_json()["error"]


def test_create_rule_allows_personal_domain_address_block(client_logged_in, taxops_db_path):
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "spammer@gmail.com", "rule_scope": "address", "action": "block"},
    )
    assert resp.status_code == 200


def test_create_rule_rejects_bad_scope(client_logged_in):
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "vendor.com", "rule_scope": "planet", "action": "block"},
    )
    assert resp.status_code == 400


def test_create_rule_rejects_address_without_at_sign(client_logged_in):
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "notanaddress", "rule_scope": "address", "action": "block"},
    )
    assert resp.status_code == 400


def test_create_rule_rejects_duplicate(client_logged_in, taxops_db_path):
    _add_rule(taxops_db_path, "dupe.com", "domain", "block")
    resp = client_logged_in.post(
        "/api/admin/sender-rules",
        json={"domain": "dupe.com", "rule_scope": "domain", "action": "block"},
    )
    assert resp.status_code == 409


def test_delete_rule(client_logged_in, taxops_db_path):
    _add_rule(taxops_db_path, "removeme.com", "domain", "block")
    conn = get_connection(taxops_db_path)
    rule_id = conn.execute("SELECT id FROM email_sender_rules WHERE domain='removeme.com'").fetchone()["id"]
    conn.close()

    resp = client_logged_in.post(f"/api/admin/sender-rules/{rule_id}/delete")
    assert resp.status_code == 200
    assert resp.get_json()["success"] is True

    conn = get_connection(taxops_db_path)
    row = conn.execute("SELECT id FROM email_sender_rules WHERE id=?", (rule_id,)).fetchone()
    conn.close()
    assert row is None


def test_delete_nonexistent_rule_returns_404(client_logged_in):
    resp = client_logged_in.post("/api/admin/sender-rules/999999/delete")
    assert resp.status_code == 404
