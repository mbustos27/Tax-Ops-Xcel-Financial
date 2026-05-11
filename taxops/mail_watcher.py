"""
mail_watcher.py
---------------
Background IMAP poller — attaches client email attachments to the correct return.

DOC-3 / GitHub issue #60

Privacy rules enforced here:
- IMAP_PASS is never logged, printed, or stored anywhere
- Email body is never stored in any DB column
- SSN / identification numbers are never extracted or stored in filenames
- scrub_ssn_from_dict() is called on user-supplied strings before DB storage
- file_path is stored in DB for internal use only and never returned to callers
"""
from __future__ import annotations

import email
import email.header
import imaplib
import logging
import os
import threading
import time

logger = logging.getLogger(__name__)

_ALLOWED_ATTACHMENT_EXTS = frozenset({".pdf", ".jpg", ".jpeg", ".png"})

# Prevents two poll cycles from running concurrently when LLM calls are slow
_poll_lock = threading.Lock()

# Prevents start_mail_watcher from launching a second poll thread on Flask
# dev-server reloads (which re-execute module-level code in the child process).
_watcher_started = False

# UIDs processed this session — keyed by (folder, uid_str).
# Prevents the same IMAP message from being processed twice within a session
# without touching Gmail's read/unread state.  Lost on restart, but the
# _record_classification 24-hour dedup guard prevents duplicate DB rows.
_processed_uids: set = set()


# ── Public entry point ────────────────────────────────────────────────────────

def start_mail_watcher(app) -> None:
    """Start the background IMAP poll thread. No-op if IMAP_HOST is not configured.
    Safe to call multiple times — only the first call launches the thread."""
    global _watcher_started
    if _watcher_started:
        logger.info("Mail watcher already started — ignoring duplicate start call")
        return
    from config import IMAP_HOST
    if not IMAP_HOST:
        logger.info("IMAP_HOST not configured — mail watcher not started")
        return
    _watcher_started = True
    thread = threading.Thread(
        target=_poll_loop,
        args=(app,),
        daemon=True,
        name="mail-watcher",
    )
    thread.start()
    logger.info("Mail watcher started")


# ── Poll loop ─────────────────────────────────────────────────────────────────

def _poll_loop(app) -> None:
    """Run forever in the daemon thread. Never crashes — logs errors and sleeps.

    Windows console note: Quick Edit Mode pauses the process when you click or drag
    in the terminal window; outbound logs then block until you press Enter. That can
    look like Python is waiting for keyboard input even though no code calls input().
    Disable Quick Edit in cmd.exe / PowerShell properties if this bites you.
    """
    from config import IMAP_POLL_INTERVAL
    while True:
        try:
            with app.app_context():
                _poll_once(app)
        except Exception:
            logger.exception("Mail watcher poll error")
        time.sleep(IMAP_POLL_INTERVAL)


def _poll_once(app) -> None:
    """
    One full poll cycle:
    1. Connect + login (credentials never logged)
    2. Select folder
    3. Search UNSEEN
    4. Process each message — mark READ regardless of success
    5. Logout

    Skips entirely if a previous cycle is still running (LLM calls can be slow).
    """
    if not _poll_lock.acquire(blocking=False):
        logger.info("Previous poll cycle still running — skipping this cycle")
        return
    try:
        _poll_once_inner(app)
    finally:
        _poll_lock.release()


def _get_available_folders(imap) -> set:
    """
    Return the set of folder names available on this IMAP account.
    Used to silently skip category folders that don't exist (e.g. Gmail
    category tabs are absent on standard accounts).
    Never raises — returns empty set on error.
    """
    try:
        resp = imap.list()
        if not isinstance(resp, tuple) or len(resp) < 2:
            logger.error(f"Unexpected IMAP LIST response shape: {resp!r}")
            return set()
        typ = resp[0]
        folder_list = resp[1]
        if typ != "OK":
            return set()
        available = set()
        for item in folder_list:
            if isinstance(item, bytes):
                decoded = item.decode("utf-8", errors="replace")
            elif isinstance(item, str):
                decoded = item
            else:
                continue
            # Response format: (\Flags) "/" "Folder Name"  or  (\Flags) "/" Folder
            # Split on quotes to extract the name
            parts = decoded.split('"')
            if len(parts) >= 3:
                available.add(parts[-2])
            elif parts:
                # Unquoted folder name — take the last whitespace-delimited token
                last = decoded.rsplit(None, 1)
                if last:
                    available.add(last[-1])
        return available
    except Exception as e:
        logger.error(f"Failed to list IMAP folders: {e}")
        return set()


def _safe_select(imap, folder: str) -> bool:
    """
    Safely select an IMAP folder.
    Returns True if selected OK, False if folder does not exist or errors.
    Never raises.
    """
    try:
        resp = imap.select(folder)
        if not isinstance(resp, tuple) or len(resp) < 2:
            logger.info(f"Folder {folder!r} not selectable — malformed response: {resp!r}")
            return False
        typ = resp[0]
        data = resp[1]
        if typ == "OK":
            return True
        logger.info(f"Folder {folder!r} not selectable — server returned: {typ}")
        return False
    except Exception as e:
        logger.error(f"imap.select({folder!r}) raised: {e}")
        return False


def _mark_read(imap, uid, reason: str) -> None:
    """
    Mark a message as read via UID STORE.
    Respects IMAP_MARK_AS_READ — when False, leaves messages unread (still processed).
    Respects IMAP_DRY_RUN — logs only, does not touch Gmail when enabled.
    Never raises.
    """
    from config import IMAP_DRY_RUN, IMAP_MARK_AS_READ
    if not IMAP_MARK_AS_READ:
        return
    if IMAP_DRY_RUN:
        logger.info(f"DRY RUN — would mark read: uid={uid!r} reason={reason}")
        return
    try:
        imap.uid("STORE", uid, "+FLAGS", "\\Seen")
        logger.debug(f"Marked read: uid={uid!r} reason={reason}")
    except Exception as e:
        logger.error(f"Failed to mark read uid={uid!r}: {e}")


def _fetch_message_data(imap, uid) -> dict | None:
    """
    Fetch and parse one IMAP message using its stable UID.
    Returns a dict with all fields needed for classification and processing,
    or None if the message cannot be fetched.
    Body text is extracted here for LLM use — never stored beyond this function.
    """
    resp = imap.uid("FETCH", uid, "(RFC822)")
    if not isinstance(resp, tuple) or len(resp) < 2:
        logger.error(f"Unexpected IMAP UID FETCH response for uid={uid!r}: {resp!r}")
        return None
    status = resp[0]
    msg_data_raw = resp[1]
    if status != "OK" or not msg_data_raw:
        return None

    raw_bytes: bytes | None = None
    for item in msg_data_raw:
        if isinstance(item, tuple) and len(item) >= 2:
            raw_bytes = item[1]
            break
    if not raw_bytes:
        return None

    message = email.message_from_bytes(raw_bytes)
    sender       = _decode_header_value(message.get("From", ""))
    subject      = _decode_header_value(message.get("Subject", ""))
    body_text    = _extract_plain_text(message)  # used for LLM only — never stored
    sender_email = _extract_email_address(sender)
    sender_domain = _extract_domain(sender_email)

    return {
        "message":       message,
        "sender":        sender,
        "sender_email":  sender_email,
        "sender_domain": sender_domain,
        "subject":       subject,
        "body_text":     body_text,
    }


def _dispatch_classified_message(app, msg: dict) -> None:
    """
    Execute the appropriate action for a message whose classification is known.
    Handles: hard-skip, promotional, unknown, client_document/inquiry.
    source_layer controls whether the result is recorded in email_classifications.

    Does NOT touch IMAP state — Gmail read/unread is left unchanged.
    Re-processing within a session is prevented by _processed_uids in the
    poll loop, and across restarts by the 24-hour dedup guard in
    _record_classification.
    """
    classification = msg["classification"]
    source_layer   = msg["source_layer"]
    sender_domain  = msg["sender_domain"]
    sender_email   = msg["sender_email"]
    subject        = msg["subject"]
    body_text      = msg["body_text"]
    message        = msg["message"]
    sender         = msg["sender"]

    # Layer 1 hard skip — no DB writes at all
    if source_layer == "known_rule":
        logger.info(f"Hard skip: {sender_domain} — known sender rule")
        return

    # ── Recording policy ─────────────────────────────────────────────────────
    # Record in email_classifications for heuristic / ML / LLM outcomes staff may correct.
    # Cached-domain promotional stays silent (noise).
    should_record = source_layer in (
        "personal_llm",
        "domain_llm",
        "keyword",
        "llm",
        "ml",
    ) or (source_layer == "cache" and classification != "promotional")

    # ── Domain cache update policy ────────────────────────────────────────────
    # Only per-message LLM layers bump cache here; keyword/ml seed elsewhere if needed.
    should_update_cache = source_layer in ("domain_llm", "personal_llm", "llm")

    if classification == "promotional":
        logger.info(f"Promotional: {sender_domain} (layer={source_layer})")
        if should_record:
            _record_classification(
                sender_email, sender_domain, subject, "promotional", source="auto"
            )
        if should_update_cache:
            _update_domain_cache(sender_domain, "promotional")
        return

    if classification == "unknown":
        _log_unmatched(app, sender, subject)
        return

    # client_document or client_inquiry ──────────────────────────────────────
    ec_id: int | None = None
    if should_record:
        ec_id = _record_classification(
            sender_email, sender_domain, subject, classification, source="auto"
        )
    if should_update_cache:
        _update_domain_cache(sender_domain, classification)

    # Drive share — add a note, skip attachment saving
    if _is_drive_share(subject, body_text):
        logger.info(f"Drive share from {sender_domain} — adding note")
        name = _extract_client_name(subject, body_text)
        if name:
            client = _match_client(app, name)
            if client:
                ret = _find_current_return(app, client["id"])
                if ret:
                    if _add_note(app, ret["id"], sender, subject, 0, drive_share=True):
                        if ec_id is not None:
                            _mark_email_routed_ok(ec_id)
        return

    # Normal attachment + note flow
    name = _extract_client_name(subject, body_text)
    # body_text is discarded after this point — never passed further or stored
    if name:
        client = _match_client(app, name)
        if client:
            ret = _find_current_return(app, client["id"])
            if ret:
                count = _save_attachments(app, message, ret["id"])
                _add_note(app, ret["id"], sender, subject, count)
                if ec_id is not None and count > 0:
                    _mark_email_routed_ok(ec_id)
            else:
                _log_unmatched(app, sender, subject)
        else:
            _log_unmatched(app, sender, subject)
    else:
        _log_unmatched(app, sender, subject)


def _poll_once_inner(app) -> None:
    """
    Multi-folder poll cycle:

    Phase 0 — Connect once; loop over Gmail category folders.
              'skip' folders → mark all UNSEEN as seen immediately (zero LLM).
              'full_processing' folders → collect messages for batch classification.
    Phase 1 — Fetch and parse collected messages.
    Phase 2 — Classify in batch (layers 1-5 + fastText + optional LLM).
    Phase 3 — Dispatch each message.

    All IMAP operations use stable UIDs (not volatile sequence numbers).
    """
    from config import (
        IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASS,
        IMAP_FOLDER, GMAIL_CATEGORY_FOLDERS, USE_GMAIL_CATEGORIES,
    )

    folders_to_check = GMAIL_CATEGORY_FOLDERS if USE_GMAIL_CATEGORIES else {
        IMAP_FOLDER: "full_processing"
    }

    imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    try:
        imap.login(IMAP_USER, IMAP_PASS)  # credentials intentionally not logged

        # ── Phase 0: scan each folder ─────────────────────────────────────────
        messages: list[dict] = []

        # Discover which folders actually exist on this account before selecting.
        # Standard Gmail accounts lack CATEGORY_PROMOTIONS etc.; attempting to
        # select a non-existent folder causes an unpredictable response that can
        # crash the loop.
        available_folders = _get_available_folders(imap)
        logger.info(f"Available IMAP folders: {available_folders or '(could not list)'}")

        for folder, handling in folders_to_check.items():
            # INBOX is always attempted even if not in the listed set
            if folder != "INBOX" and available_folders and folder not in available_folders:
                logger.info(f"Skipping {folder!r} — not available on this account")
                continue

            if not _safe_select(imap, folder):
                continue

            resp = imap.uid("SEARCH", "UNSEEN")
            if not isinstance(resp, tuple) or len(resp) < 2:
                logger.error(
                    f"Unexpected IMAP UID SEARCH response in folder {folder!r}: {resp!r}"
                )
                continue
            status = resp[0]
            data = resp[1]
            if status != "OK" or not data or not data[0]:
                logger.info(f"No unseen messages in {folder!r}")
                continue

            raw_uids = data[0].split()
            if not raw_uids:
                continue

            # Filter UIDs already handled this session
            new_uids = [
                uid for uid in raw_uids
                if (folder, uid.decode("ascii", errors="replace"))
                not in _processed_uids
            ]

            logger.info(
                f"Folder {folder!r}: {len(new_uids)} new unseen "
                f"(handling={handling}, {len(raw_uids)-len(new_uids)} already seen this session)"
            )

            if not new_uids:
                continue

            if handling == "skip":
                for uid in new_uids:
                    _processed_uids.add((folder, uid.decode("ascii", errors="replace")))
                    _mark_read(imap, uid, f"folder={folder} auto-skip")
                logger.info(f"Auto-skipped {len(new_uids)} message(s) from {folder!r}")
            else:
                # full_processing — fetch and collect for classification
                for uid in new_uids:
                    uid_str = uid.decode("ascii", errors="replace")
                    try:
                        md = _fetch_message_data(imap, uid)
                        if md:
                            messages.append({
                                **md,
                                "uid":            uid,
                                "folder":         folder,
                                "classification": None,
                                "source_layer":   None,
                            })
                            _processed_uids.add((folder, uid_str))
                        else:
                            _processed_uids.add((folder, uid_str))
                    except Exception as e:
                        logger.error(f"Fetch failed uid={uid!r} in {folder!r}: {e}")
                        _mark_read(imap, uid, "fetch-error")

        if not messages:
            return

        # ── Phase 2: classify each message (clean 5-layer) ───────────────────
        for msg in messages:
            cls, layer = _classify_email(
                msg["sender_domain"],
                msg["subject"],
                msg.get("body_text", ""),
            )
            msg["classification"] = cls
            msg["source_layer"]   = layer

        # ── Phase 3: dispatch each message ────────────────────────────────────
        for msg in messages:
            try:
                _dispatch_classified_message(app, msg)
            except Exception as e:
                logger.error(
                    f"Dispatch failed for {msg.get('sender_domain')}: {e}"
                )

    finally:
        try:
            imap.logout()
        except Exception:
            pass


# ── Header / body helpers ─────────────────────────────────────────────────────

def _decode_header_value(raw: str) -> str:
    """Decode a possibly RFC2047-encoded email header into a plain string."""
    parts = email.header.decode_header(raw)
    decoded = []
    for bstr, charset in parts:
        if isinstance(bstr, bytes):
            decoded.append(bstr.decode(charset or "utf-8", errors="replace"))
        else:
            decoded.append(bstr)
    return "".join(decoded).strip()


def _extract_plain_text(message) -> str:
    """Return the first text/plain part of the message. Empty string if none."""
    for part in message.walk():
        if part.get_content_type() == "text/plain":
            payload = part.get_payload(decode=True)
            if payload:
                charset = part.get_content_charset() or "utf-8"
                return payload.decode(charset, errors="replace")
    return ""


# ── Sender address helpers ────────────────────────────────────────────────────

import re as _re

def _extract_email_address(from_header: str) -> str:
    """Extract bare email address from a From header like 'Name <addr@domain>'."""
    m = _re.search(r'<([^>@\s]+@[^>\s]+)>', from_header)
    if m:
        return m.group(1).strip().lower()
    stripped = from_header.strip()
    if '@' in stripped and ' ' not in stripped:
        return stripped.lower()
    return from_header.lower()


def _extract_domain(email_addr: str) -> str:
    """Extract domain portion from an email address. Returns empty string if none found."""
    if '@' in email_addr:
        return email_addr.split('@', 1)[1].lower()
    return ''


# ── Google Drive share detection ─────────────────────────────────────────────

_DRIVE_INDICATORS = (
    "has shared a file with you",
    "has shared a folder with you",
    "invited you to edit",
    "invited you to view",
    "docs.google.com",
    "drive.google.com",
)


def _is_drive_share(subject: str, body_text: str) -> bool:
    """
    Return True if the email is a Google Drive share notification rather than
    a direct attachment. These should receive a note but not be flagged as
    missed attachments — the client shared a link, not a file.
    """
    combined = (subject + " " + (body_text or "")).lower()
    return any(indicator in combined for indicator in _DRIVE_INDICATORS)


# ── Email classification (self-improving few-shot) ────────────────────────────

_ALLOWED_CLASSES = frozenset({"client_document", "client_inquiry", "promotional", "unknown"})

# rule_type values in email_sender_rules → classification
_RULE_TYPE_MAP = {
    "always_promotional": "promotional",
    "always_client":      "client_document",
}


def _check_known_sender_rule(sender_domain: str) -> str | None:
    """
    Check whether sender_domain has a row in email_sender_rules.
    Returns the mapped classification string if a rule exists, None otherwise.
    Never raises.

    Exact column name: rule_type  (see db.py email_sender_rules table)
    Exact rule_type values: 'always_promotional', 'always_client'
    """
    from db import get_connection
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT rule_type FROM email_sender_rules WHERE domain = ? COLLATE NOCASE",
                (sender_domain,),
            ).fetchone()
            if row:
                mapped = _RULE_TYPE_MAP.get(row["rule_type"])
                if mapped:
                    return mapped
                logger.warning(
                    f"Unknown rule_type '{row['rule_type']}' for domain {sender_domain} — ignoring"
                )
            return None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_check_known_sender_rule failed for {sender_domain}: {e}")
        return None


def _record_classification(
    sender_email: str,
    sender_domain: str,
    subject: str,
    classification: str,
    source: str = "auto",
) -> int | None:
    """
    Insert one row into email_classifications.
    source is 'auto' (LLM) or 'staff' (confirmed by staff).
    Never raises — logs errors; returns None on failure.
    Body is never stored here.

    Deduplication: if an identical (sender_email, subject_snippet) row already
    exists within the last 24 hours, skips insert and returns that row's id so
    dispatch can still mark email_routed_ok when attachments are saved on a
    replayed fetch.
    """
    from db import get_connection
    from utils import now
    try:
        conn = get_connection()
        try:
            subject_snippet = subject[:100]
            existing = conn.execute(
                """
                SELECT id FROM email_classifications
                WHERE sender_email = ?
                  AND subject_snippet = ?
                  AND created_at >= datetime('now', '-24 hours')
                LIMIT 1
                """,
                (sender_email, subject_snippet),
            ).fetchone()
            if existing:
                logger.debug(
                    f"Skipping duplicate classification for "
                    f"{sender_domain!r} / {subject_snippet!r}"
                )
                return int(existing["id"])

            conn.execute(
                """
                INSERT INTO email_classifications
                    (sender_email, sender_domain, subject_snippet, classification, source, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (sender_email, sender_domain, subject_snippet, classification, source, now()),
            )
            conn.commit()
            new_id_row = conn.execute("SELECT last_insert_rowid() AS id").fetchone()
            new_id = int(new_id_row["id"]) if new_id_row else None
            # Auto-trigger pattern analysis every 20 auto-classified emails
            if source == "auto":
                try:
                    n = conn.execute(
                        "SELECT COUNT(*) FROM email_classifications WHERE source = 'auto'"
                    ).fetchone()[0]
                    if n % 20 == 0:
                        import threading as _threading
                        from flask import current_app as _current_app
                        _threading.Thread(
                            target=_analyze_patterns,
                            args=(_current_app._get_current_object(),),
                            daemon=True,
                            name="pattern-analyzer",
                        ).start()
                        logger.info(f"Pattern analyzer auto-triggered at {n} auto-classifications")
                except Exception as _e:
                    logger.warning(f"Pattern analyzer auto-trigger failed: {_e}")
            return new_id
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_record_classification failed: {e}")
        return None


def _mark_email_routed_ok(ec_id: int) -> None:
    """Set email_routed_ok when attachments or a drive-share note was stored."""
    from db import get_connection
    try:
        conn = get_connection()
        try:
            conn.execute(
                "UPDATE email_classifications SET email_routed_ok = 1 WHERE id = ?",
                (ec_id,),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_mark_email_routed_ok failed for ec_id={ec_id}: {e}")


# ── Domain classification cache ───────────────────────────────────────────────

def _get_cached_domain(domain: str) -> dict | None:
    """
    Return the domain_classifications row for this domain, or None.
    Never raises.
    """
    from db import get_connection
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT * FROM domain_classifications WHERE domain = ? COLLATE NOCASE",
                (domain,),
            ).fetchone()
            return dict(row) if row else None
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_get_cached_domain failed for {domain}: {e}")
        return None


def _classify_by_subject(subject: str) -> str:
    """
    Keyword-based classifier for personal email domain senders.
    No LLM — runs in <1 ms, never wrong for tax-office context.

    Personal email senders (gmail, yahoo, icloud, etc.) are almost always
    real clients.  The only question is document vs inquiry; default is
    client_document because most tax-office emails are document submissions.
    """
    s = subject.lower()

    # Clearly promotional even from a personal address
    _PROMO = {
        "unsubscribe", "newsletter", "coupon", "offer", "deal", "sale",
        "subscription", "your order", "your receipt", "delivery",
        "tracking number", "order confirmation", "invoice from",
    }
    if any(k in s for k in _PROMO):
        return "promotional"

    # Inquiry signals
    _INQUIRY = {
        "question", "when will", "is my return", "status", "deadline",
        "can you", "please call", "appointment", "waiting", "how long",
        "need help", "ready", "pick up", "update on", "have you",
        "did you receive", "any news", "where is",
    }
    if any(k in s for k in _INQUIRY):
        return "client_inquiry"

    # Document signals — also the default
    return "client_document"


def _classify_domain_llm(domain: str, example_subjects: list[str]) -> str:
    """
    Ask the LLM to classify a single unknown corporate domain.
    Called at most once per domain — result is cached immediately.
    Returns one of: 'promotional', 'client_document', 'client_inquiry', 'unknown'.
    Never raises.
    """
    from config import MAIL_WATCHER_LLM_TIMEOUT
    from llm import extract_json

    subjects_text = "\n".join(f"  - {s[:80]}" for s in example_subjects[:4])
    prompt = (
        f"You are classifying email senders for a tax preparation office.\n\n"
        f"Sender domain: {domain!r}\n"
        f"Recent subject lines from this domain:\n{subjects_text}\n\n"
        "Classify this sender as ONE of:\n"
        "  promotional   — vendor, software, bank, newsletter, government bulletin,\n"
        "                  professional association, mass mailing, retail, or any\n"
        "                  non-individual automated sender\n"
        "  client_document — a real individual or small business sending tax\n"
        "                    documents, W-2s, bank statements, invoices, or\n"
        "                    IRS/government notices directly to the office\n"
        "  client_inquiry  — a real individual asking questions about their return\n\n"
        "Rules:\n"
        "  - When in doubt between promotional and client, choose promotional\n"
        "  - Government agencies and large companies = promotional\n"
        "  - Small businesses or individuals with business emails = check subjects\n\n"
        'Respond with JSON only: {"classification": "promotional"}'
    )
    try:
        result = extract_json(prompt, timeout=MAIL_WATCHER_LLM_TIMEOUT)
        cls = (result or {}).get("classification", "unknown")
        if cls in _ALLOWED_CLASSES:
            return cls
        return "unknown"
    except Exception as e:
        logger.error(f"LLM domain classify failed for {domain!r}: {e}")
        return "unknown"


def _classify_email(domain: str, subject: str, body_text: str = "") -> tuple:
    """
    Classify one email.  Returns (classification, source_layer).

    Layer 1 — known sender rule  (DB, instant)
    Layer 2 — known promotional config list  (in-memory, instant)
    Layer 3 — mass-mailing subdomain prefix  (in-memory, instant)
    Layer 4 — personal email domain → subject keyword match  (no LLM)
    Layer 5 — domain cache  (DB, instant — confidence >= 1)
    Layer 5.5 — sklearn classifier  (in-memory ML, fast)
    Layer 6 — LLM per-domain  (slow, result cached to domain_classifications)
    """
    from config import KNOWN_PROMOTIONAL_DOMAINS, MASS_MAILING_PREFIXES, PERSONAL_EMAIL_DOMAINS

    parts = domain.split(".")
    base  = ".".join(parts[-2:]) if len(parts) >= 2 else domain

    # Layer 1: staff-approved sender rule
    known = _check_known_sender_rule(domain)
    if known:
        return known, "known_rule"

    # Layer 2: config promotional list
    if base in KNOWN_PROMOTIONAL_DOMAINS:
        return "promotional", "config"

    # Layer 3: mass-mailing subdomain prefix
    if any(domain.startswith(p) for p in MASS_MAILING_PREFIXES):
        return "promotional", "prefix"

    # Layer 4: personal email domain → keyword match, never LLM
    if base in PERSONAL_EMAIL_DOMAINS:
        cls = _classify_by_subject(subject)
        return cls, "keyword"

    # Layer 5: domain_classifications cache (any confidence)
    cached = _get_cached_domain(domain)
    if cached:
        logger.info(
            f"Cache hit: {domain} → {cached['classification']} "
            f"(seen {cached['confidence_count']}x)"
        )
        return cached["classification"], "cache"

    # Layer 5.5: sklearn classifier (trained on confirmed staff data)
    try:
        from classifier import classify as _ml_classify
        ml_cls, _ml_conf = _ml_classify(subject, domain, body_text[:200])
        if ml_cls is not None:
            _update_domain_cache(domain, ml_cls)
            return ml_cls, "ml"
    except Exception as _ml_err:
        logger.debug(f"ML layer skipped for {domain}: {_ml_err}")

    # Layer 6: LLM — one call per unknown domain, cache result immediately
    logger.info(f"LLM classifying new domain: {domain!r}")
    cls = _classify_domain_llm(domain, [subject])
    _update_domain_cache(domain, cls)
    return cls, "llm"


# ─── Legacy wrapper kept so _get_domain_classification call sites in tests
# and external callers don't break.  Remove once callers are updated.
def _get_domain_classification(
    domain: str,
    subject: str = "",
    body_preview: str = "",
) -> tuple:
    """Legacy wrapper — delegates to _classify_email."""
    return _classify_email(domain, subject, body_preview)


def _update_domain_cache(domain: str, classification: str) -> None:
    """
    Upsert domain_classifications — increment confidence if same classification,
    reset to 1 if classification changed.
    After updating, check whether this domain qualifies for graduation.
    Never raises.
    """
    from db import get_connection
    from utils import now
    if not domain:
        return
    try:
        conn = get_connection()
        try:
            existing = conn.execute(
                "SELECT classification, confidence_count FROM domain_classifications "
                "WHERE domain = ? COLLATE NOCASE",
                (domain,),
            ).fetchone()

            if existing:
                if existing["classification"] == classification:
                    conn.execute(
                        "UPDATE domain_classifications "
                        "SET confidence_count = confidence_count + 1, last_seen = ? "
                        "WHERE domain = ? COLLATE NOCASE",
                        (now(), domain),
                    )
                else:
                    conn.execute(
                        "UPDATE domain_classifications "
                        "SET classification = ?, confidence_count = 1, last_seen = ? "
                        "WHERE domain = ? COLLATE NOCASE",
                        (classification, now(), domain),
                    )
            else:
                conn.execute(
                    "INSERT INTO domain_classifications "
                    "(domain, classification, confidence_count, last_seen) "
                    "VALUES (?, ?, 1, ?)",
                    (domain, classification, now()),
                )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_update_domain_cache failed for {domain}: {e}")
        return

    # Trigger graduation check in the same call (fast — just DB reads/writes)
    _check_graduation_trigger(None, domain)


def _check_graduation_trigger(app, domain: str) -> None:
    """
    Create a pending rule suggestion when a domain's confidence_count >= 3
    and no rule or pending suggestion exists yet.

    NEVER auto-applies a rule — always creates a pending suggestion for staff review.
    Personal email domains are excluded — rules for gmail.com etc. make no sense.
    Never raises.
    """
    import json as _json
    from config import PERSONAL_EMAIL_DOMAINS
    from db import get_connection
    from utils import now

    if not domain:
        return

    parts = domain.split(".")
    base = ".".join(parts[-2:]) if len(parts) >= 2 else domain
    if base in PERSONAL_EMAIL_DOMAINS:
        return

    try:
        conn = get_connection()
        try:
            cached = conn.execute(
                "SELECT classification, confidence_count, graduated "
                "FROM domain_classifications WHERE domain = ? COLLATE NOCASE",
                (domain,),
            ).fetchone()
            if not cached or cached["confidence_count"] < 3 or cached["graduated"]:
                return

            # No existing rule for this domain
            if conn.execute(
                "SELECT id FROM email_sender_rules WHERE domain = ? COLLATE NOCASE",
                (domain,),
            ).fetchone():
                return

            # No pending suggestion already
            if conn.execute(
                "SELECT id FROM rule_suggestions "
                "WHERE domain = ? AND status = 'pending' COLLATE NOCASE",
                (domain,),
            ).fetchone():
                return

            _CLASS_TO_RULE = {
                "promotional":     "always_promotional",
                "client_document": "always_client",
                "client_inquiry":  "always_client",
            }
            suggested_rule = _CLASS_TO_RULE.get(cached["classification"])
            if not suggested_rule:
                return

            # Pull up to 3 example subjects from email_classifications
            examples = conn.execute(
                "SELECT subject_snippet FROM email_classifications "
                "WHERE sender_domain = ? COLLATE NOCASE "
                "ORDER BY created_at DESC LIMIT 3",
                (domain,),
            ).fetchall()
            example_subjects = _json.dumps(
                [r["subject_snippet"] for r in examples if r["subject_snippet"]]
            )

            conn.execute(
                """
                INSERT INTO rule_suggestions
                    (domain, suggested_rule, confidence, occurrence_count,
                     example_subjects, suggested_at, suggested_by, status)
                VALUES (?, ?, 'high', ?, ?, ?, 'auto', 'pending')
                """,
                (domain, suggested_rule, cached["confidence_count"],
                 example_subjects, now()),
            )
            conn.commit()
            logger.info(
                f"Rule suggestion created (graduation): {domain} → {suggested_rule} "
                f"({cached['confidence_count']} occurrences)"
            )
        finally:
            conn.close()
    except Exception as e:
        logger.error(f"_check_graduation_trigger failed for {domain}: {e}")


# ── Batched LLM classification ────────────────────────────────────────────────

# _classify_domains_batch and _classify_personal_emails_batch removed.
# Classification is now done per-message via _classify_email().


def _analyze_patterns(app) -> int:
    """
    Analyze email_classifications for domains appearing 3+ times in the last 30 days
    with a consistent classification (source='auto'). For each qualifying domain not
    already covered by a known rule or pending suggestion, ask the LLM whether a
    permanent sender rule should be created.

    Inserts pending suggestions into rule_suggestions.
    Returns the number of new suggestions created. Never raises.

    Privacy rules:
    - Only bare domain strings and subject snippets (truncated to 80 chars) are sent to LLM
    - No sender_email addresses, no full subjects, no body content, no SSN data
    """
    import json as _json
    from db import get_connection
    from llm import extract_json
    from utils import now

    # Map from classification value to rule_type value in email_sender_rules
    _CLASS_TO_RULE = {
        "promotional":    "always_promotional",
        "client_document": "always_client",
        "client_inquiry":  "always_client",
    }

    created = 0
    try:
        with app.app_context():
            conn = get_connection()
            try:
                # Domains with 3+ consistent auto-classifications in the last 30 days
                candidates = conn.execute(
                    """
                    SELECT sender_domain, classification, COUNT(*) AS count,
                           GROUP_CONCAT(subject_snippet, '|||') AS subjects
                    FROM email_classifications
                    WHERE created_at > datetime('now', '-30 days')
                      AND source = 'auto'
                      AND sender_domain IS NOT NULL
                      AND sender_domain != ''
                    GROUP BY sender_domain, classification
                    HAVING count >= 3
                    ORDER BY count DESC
                    """
                ).fetchall()

                # Domains already covered by a known sender rule
                known_domains = {
                    r["domain"]
                    for r in conn.execute("SELECT domain FROM email_sender_rules").fetchall()
                }
                # Domains that already have a pending suggestion
                pending_domains = {
                    r["domain"]
                    for r in conn.execute(
                        "SELECT domain FROM rule_suggestions WHERE status = 'pending'"
                    ).fetchall()
                }

                from config import (
                    KNOWN_PROMOTIONAL_DOMAINS,
                    MAIL_WATCHER_LLM_TIMEOUT,
                    MASS_MAILING_PREFIXES,
                    PERSONAL_EMAIL_DOMAINS,
                )

                for row in candidates:
                    domain = row["sender_domain"]
                    classification = row["classification"]
                    count = row["count"]

                    if domain in known_domains or domain in pending_domains:
                        continue

                    # NEVER suggest rules for personal email domains (gmail, yahoo, etc.)
                    # A blanket "always_client" rule for gmail.com would be absurd.
                    parts = domain.split(".")
                    base = ".".join(parts[-2:]) if len(parts) >= 2 else domain
                    if base in PERSONAL_EMAIL_DOMAINS:
                        logger.info(
                            f"Skipping rule suggestion for personal domain: {domain}"
                        )
                        continue

                    # NEVER suggest rules for domains already in config promotional list
                    # or mass-mailing prefixes — they are already handled without LLM.
                    if base in KNOWN_PROMOTIONAL_DOMAINS or any(
                        domain.startswith(p) for p in MASS_MAILING_PREFIXES
                    ):
                        logger.info(
                            f"Skipping rule suggestion for config-covered domain: {domain}"
                        )
                        continue

                    suggested_rule = _CLASS_TO_RULE.get(classification)
                    if not suggested_rule:
                        continue

                    # Up to 3 example subjects — truncated to 80 chars each
                    raw_subjects = (row["subjects"] or "").split("|||")
                    example_subjects = [s.strip()[:80] for s in raw_subjects if s.strip()][:3]

                    prompt = (
                        f"A tax office email inbox received {count} emails from the domain "
                        f"'{domain}', all classified as '{classification}'.\n"
                        f"Example subjects: {example_subjects}\n\n"
                        "Should a permanent classification rule be created for this domain "
                        "(so future emails are skipped without calling the AI)?\n"
                        "Respond in JSON only — no explanation outside the JSON:\n"
                        '{"suggest_rule": true, "confidence": "high", "reason": "..."}'
                        "\nconfidence must be exactly: high, medium, or low"
                    )

                    try:
                        result = extract_json(prompt, timeout=MAIL_WATCHER_LLM_TIMEOUT)
                        if not isinstance(result, dict) or not result.get("suggest_rule"):
                            continue
                        confidence = str(result.get("confidence", "low")).lower()
                        if confidence not in ("high", "medium"):
                            continue

                        conn.execute(
                            """
                            INSERT INTO rule_suggestions
                                (domain, suggested_rule, confidence, occurrence_count,
                                 example_subjects, suggested_at, suggested_by, status)
                            VALUES (?, ?, ?, ?, ?, ?, 'llm', 'pending')
                            """,
                            (
                                domain,
                                suggested_rule,
                                confidence,
                                count,
                                _json.dumps(example_subjects),
                                now(),
                            ),
                        )
                        conn.commit()
                        created += 1
                        pending_domains.add(domain)
                        logger.info(
                            f"Rule suggestion created: {domain} → {suggested_rule} "
                            f"({confidence}, {count} occurrences)"
                        )
                    except Exception as e:
                        logger.warning(f"Pattern analysis failed for {domain}: {e}")
                        continue

            finally:
                conn.close()
    except Exception as e:
        logger.error(f"_analyze_patterns failed: {e}")

    return created


def _classify_email_legacy_fewshot(subject: str, body_text: str, sender_email: str) -> str:
    """
    LEGACY — per-message LLM + few-shot classifier (returns a single label).
    Not used by the mail watcher poll path; kept for reference / manual testing.

    Classify an email using pre-LLM heuristics + LLM + few-shot examples.
    Caller is responsible for checking known sender rules before calling this.

    Returns one of: client_document, client_inquiry, promotional, unknown
    Never raises — returns 'unknown' on failure.

    Privacy rules:
    - subject truncated to 100 chars — never full subject
    - body truncated to 200 chars for prompt context — never stored
    - sender_email stored in DB column; domain-only in prompt
    - No SSN, ssn_last4, or identification numbers appear anywhere here
    """
    from config import (
        KNOWN_PROMOTIONAL_DOMAINS,
        MAIL_WATCHER_LLM_TIMEOUT,
        MASS_MAILING_PREFIXES,
        PERSONAL_EMAIL_DOMAINS,
    )
    from db import get_connection
    from llm import chat

    domain = _extract_domain(sender_email)

    # Compute base domain once — used in multiple checks below.
    parts = domain.split(".")
    base_domain = ".".join(parts[-2:]) if len(parts) >= 2 else domain

    # ── Pre-LLM check 1: mass-mailing subdomain ──────────────────────────────
    # Subdomains like e., em., email., mail. always indicate marketing/automation.
    # This check applies even to subdomains of personal email providers.
    if any(domain.startswith(prefix) for prefix in MASS_MAILING_PREFIXES):
        logger.info(f"Mass mailing subdomain: {domain} — classifying as promotional (no LLM)")
        return "promotional"

    # ── Personal email domain flag ────────────────────────────────────────────
    # Personal domain senders are almost always real clients — skip the promotional
    # domain block for them and give the LLM a strong hint.
    is_personal = base_domain in PERSONAL_EMAIL_DOMAINS

    # ── Pre-LLM check 2: known promotional base domain ───────────────────────
    # Only applies if NOT a personal domain.
    if not is_personal and base_domain in KNOWN_PROMOTIONAL_DOMAINS:
        logger.info(f"Known promotional domain: {domain} — classifying as promotional (no LLM)")
        return "promotional"

    # ── LLM path ─────────────────────────────────────────────────────────────
    classification = "unknown"

    conn = get_connection()
    try:
        # Step A — Load recent staff-confirmed examples for few-shot context
        staff_rows = conn.execute(
            "SELECT subject_snippet, classification FROM email_classifications "
            "WHERE source = 'staff' ORDER BY confirmed_at DESC LIMIT 10"
        ).fetchall()
        examples = list(staff_rows)

        if len(examples) < 3:
            auto_rows = conn.execute(
                "SELECT subject_snippet, classification FROM email_classifications "
                "WHERE source = 'auto' ORDER BY created_at DESC LIMIT ?",
                (10 - len(examples),),
            ).fetchall()
            examples.extend(auto_rows)

        # Step B — Build few-shot block
        few_shot_examples = ""
        for ex in examples:
            few_shot_examples += (
                f"Subject: {ex['subject_snippet']}\n"
                f"Classification: {ex['classification']}\n\n"
            )

        # Step C — Build prompt (rewritten for tax-office specificity)
        # Personal domain senders get an additional hint that they are likely real clients.
        if is_personal:
            personal_note = (
                f"\nIMPORTANT: This email is from a personal email address ({domain}). "
                f"Personal email senders are almost always real tax clients, not vendors. "
                f"Unless the subject clearly indicates a newsletter, shopping receipt, or automated "
                f"notification, classify as client_document or client_inquiry."
            )
        else:
            personal_note = ""

        prompt = (
            f"You are classifying incoming emails for a tax preparation office called Xcel Financial.\n\n"
            f"CLASSIFICATION RULES — read carefully:\n\n"
            f"client_document: ONLY use this if a real individual tax client is sending their personal tax documents.\n"
            f"Examples: W-2, 1099, social security letter, IRS notice, government ID, prior year tax return.\n"
            f"The sender must be a person, not a company, vendor, newsletter, or automated system.\n"
            f"Vendor emails about tax products (Drake, ADP, Intuit, GruntWorx, etc.) are NOT client documents.\n"
            f"Government agency newsletters and bulletins are NOT client documents.\n"
            f"Notary association emails are NOT client documents.\n"
            f"School photo services, real estate listings, shopping sites are NOT client documents.\n\n"
            f"client_inquiry: A real individual tax client asking a question about their return, refund, appointment, or filing status.\n"
            f"The sender must be a person who is likely a client of this office.\n"
            f"Vendor webinar invites, newsletters, and marketing emails are NOT client inquiries.\n\n"
            f"promotional: Any of the following:\n"
            f"- Marketing, newsletters, advertisements, sales emails\n"
            f"- Vendor emails (software companies, office supply stores, professional associations)\n"
            f"- Government agency bulletins and newsletters not addressed to this office specifically\n"
            f"- Automated notifications from services (Adobe, Microsoft, Canva, Poshmark, ADP payroll marketing, etc.)\n"
            f"- Seminar and webinar invitations\n"
            f"- Reward program emails\n"
            f"- Any email from a mass mailing domain (em., e., email., mails., engage. subdomains)\n\n"
            f"unknown: Only use this if you genuinely cannot determine the category.\n\n"
            f"EMAIL TO CLASSIFY:\n"
            f"Sender domain: {domain}\n"
            f"Subject: {subject[:100]}\n"
            f"Body preview: {(body_text or '')[:200]}\n"
            f"{personal_note}\n"
            f"IMPORTANT: When in doubt between client_document and promotional, choose promotional.\n"
            f"Tax office vendors are promotional, not client documents.\n"
            f"Only a real human client sending their personal tax paperwork is client_document.\n\n"
            f"Few-shot examples from this office:\n"
            f"{few_shot_examples}"
            f"Respond with exactly one word. No explanation."
        )

        # Step D — Call LLM and validate
        try:
            result = chat(prompt, timeout=MAIL_WATCHER_LLM_TIMEOUT).strip().lower()
            classification = result if result in _ALLOWED_CLASSES else "unknown"
        except Exception as e:
            logger.error(f"Classification LLM error: {e}")
            classification = "unknown"

    except Exception as e:
        logger.error(f"Mail watcher: _classify_email_legacy_fewshot error: {e}")
    finally:
        conn.close()

    # Step E — Record LLM result (source='auto') — body never stored
    _record_classification(sender_email, domain, subject, classification, source="auto")

    return classification


# ── LLM name extraction ───────────────────────────────────────────────────────

def _extract_client_name(subject: str, body_text: str) -> str | None:
    """
    Ask the LLM to identify the client from the email subject and a short body excerpt.
    Body is truncated to 500 chars maximum — full body is never sent to the LLM.
    Returns None if the LLM cannot identify a name or if it fails.
    """
    from config import MAIL_WATCHER_LLM_TIMEOUT
    from llm import chat

    truncated_body = (body_text or "")[:500]
    prompt = (
        "You are helping a tax office identify which client sent this email.\n\n"
        f"Email subject: {subject}\n"
        f"Email body (first 500 characters): {truncated_body}\n\n"
        "Extract the full legal name of the tax client this email is about or sent by. "
        "Return only the name, nothing else. "
        "If you cannot determine a name with confidence, return exactly: UNKNOWN"
    )
    try:
        result = chat(prompt, timeout=MAIL_WATCHER_LLM_TIMEOUT).strip()
        if result.upper() == "UNKNOWN" or not result:
            return None
        return result
    except Exception as e:
        logger.error(f"LLM name extraction failed: {e}")
        return None


# ── Client matching ───────────────────────────────────────────────────────────

def _match_client(app, name: str) -> dict | None:
    """
    Fuzzy-match an extracted client name against the clients table.

    Tries multiple parses — comma forms via parse_name(), plus common unpunctuated
    Hispanic compound surnames ("Cesar L Salas Diaz" → last SALAS DIAZ / first CESAR L)
    and simple FIRST LAST reversal.

    Uses MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE (default 82), slightly below the
    importer ACCEPT_THRESHOLD (88), because emailed names often omit commas.

    Returns a minimal dict with 'id', 'last_name', 'first_name' or None.
    """
    from config import MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE
    from db import get_connection
    from name_matcher import ACCEPT_THRESHOLD, find_client, is_business, normalize_name, parse_name

    raw = (name or "").strip()
    if not raw:
        return None

    candidates: list[tuple[str, str | None]] = []

    ln, fn = parse_name(raw)
    if ln:
        candidates.append((ln, fn))

    norm = normalize_name(raw)
    if norm and not is_business(norm):
        toks = norm.split()
        if len(toks) >= 3:
            candidates.append((" ".join(toks[-2:]), " ".join(toks[:-2])))
        elif len(toks) == 2:
            candidates.append((toks[1], toks[0]))

    seen: set[tuple[str, str]] = set()
    uniq: list[tuple[str, str | None]] = []
    for ln2, fn2 in candidates:
        key = (ln2, (fn2 or ""))
        if key in seen:
            continue
        seen.add(key)
        uniq.append((ln2, fn2))

    conn = get_connection()
    try:
        best = None
        best_score = -1
        for ln2, fn2 in uniq:
            if not ln2:
                continue
            result = find_client(conn, ln2, fn2)
            if result is None:
                continue
            sc = int(result["score"])
            if sc > best_score:
                best_score = sc
                best = result

        if best is None or best_score < MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE:
            return None

        if best_score < ACCEPT_THRESHOLD:
            logger.info(
                "Mail watcher: borderline client match "
                f"score={best_score} (min={MAIL_WATCHER_CLIENT_MATCH_MIN_SCORE}) "
                f"for extracted name {raw!r}"
            )

        row = conn.execute(
            "SELECT id, last_name, first_name FROM clients WHERE id = ?",
            (best["client_id"],),
        ).fetchone()
        if row is None:
            return None
        return {"id": row["id"], "last_name": row["last_name"], "first_name": row["first_name"]}
    finally:
        conn.close()


# ── Return lookup ─────────────────────────────────────────────────────────────

def _find_current_return(app, client_id: int) -> dict | None:
    """
    Find the most recent return row for this client (by tax_year, then id).

    Clients often email amended docs, letters, or late forms after a return is
    marked LOG OUT — those still need to land on the correct profile.  Only
    CANCELLED returns are skipped so attachments never attach to voided cases.
    """
    from db import get_connection

    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, client_id, tax_year, client_status
            FROM returns
            WHERE client_id = ?
              AND client_status != 'CANCELLED'
            ORDER BY tax_year DESC, id DESC
            LIMIT 1
            """,
            (client_id,),
        ).fetchone()
        if row is None:
            return None
        return {"id": row["id"], "client_id": row["client_id"], "tax_year": row["tax_year"]}
    finally:
        conn.close()


# ── Attachment saving ─────────────────────────────────────────────────────────

def _save_attachments(app, message, return_id: int) -> int:
    """
    Walk MIME parts, save allowed attachments (pdf/jpg/jpeg/png) to disk,
    and record each in return_documents.

    Files are stored as-is — there is no OCR or automated text extraction on
    ingest (native PDFs remain PDFs; scanned images remain images).  Staff use
    the UI / separate tooling if extraction is needed.

    - source is always 'email'
    - doc_type is always 'unknown' — staff tags later
    - uploaded_by is always 'mail_watcher'
    - file_path is stored in DB but never returned to any caller
    - scrub_ssn_from_dict() is applied to user-supplied filename strings
    Returns count of successfully saved attachments.
    """
    from db import get_connection
    from utils import (
        get_return_documents_path,
        sanitize_filename,
        scrub_ssn_from_dict,
        now,
        _enqueue_extraction,
    )

    # Content types that may carry a real attachment without an 'attachment' disposition.
    # Gmail in particular sends application/octet-stream or the specific MIME type
    # with only a filename parameter and no explicit Content-Disposition header.
    _ATTACHMENT_CONTENT_TYPES = frozenset({
        "application/pdf",
        "image/jpeg",
        "image/jpg",
        "image/png",
        "application/octet-stream",
    })

    count = 0
    conn = get_connection()
    try:
        for part in message.walk():
            content_type        = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition") or "").lower()
            raw_filename        = part.get_filename() or ""

            # Per-part detail can flood stdout (Windows console Quick Edit pauses → blocks writes).
            logger.debug(
                f"MIME part: type={content_type!r} "
                f"disposition={content_disposition[:60]!r} "
                f"filename={raw_filename[:40]!r}"
            )

            # Accept if explicit attachment disposition OR if the content-type is a
            # known attachment type and a filename is present (handles Gmail inline PDFs).
            is_attachment = (
                "attachment" in content_disposition
                or (raw_filename and content_type in _ATTACHMENT_CONTENT_TYPES)
            )

            if not is_attachment or not raw_filename:
                continue

            raw_filename = _decode_header_value(raw_filename)
            ext = os.path.splitext(raw_filename)[1].lower()
            if ext not in _ALLOWED_ATTACHMENT_EXTS:
                logger.debug(f"Skipping unsupported extension: {ext!r} ({raw_filename!r})")
                continue

            try:
                folder = get_return_documents_path(return_id)
                sanitized = sanitize_filename(raw_filename)
                stem, ext_part = os.path.splitext(sanitized)
                candidate = sanitized
                counter = 1
                while os.path.exists(os.path.join(folder, candidate)):
                    candidate = f"{stem}_{counter}{ext_part}"
                    counter += 1

                full_path = os.path.abspath(os.path.join(folder, candidate))

                payload = part.get_payload(decode=True)
                if not payload:
                    logger.warning(f"Empty payload for attachment: {raw_filename!r}")
                    continue

                with open(full_path, "wb") as fh:
                    fh.write(payload)

                file_size_bytes = os.path.getsize(full_path)
                uploaded_at = now()

                # Scrub any SSN patterns that may be embedded in user-supplied strings
                safe = scrub_ssn_from_dict({
                    "filename": candidate,
                    "original_filename": raw_filename,
                })

                cur = conn.execute(
                    """
                    INSERT INTO return_documents (
                        return_id, filename, original_filename, doc_type, source,
                        file_path, file_size_bytes, uploaded_by, uploaded_at, notes, is_deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        return_id,
                        safe["filename"],
                        safe["original_filename"],
                        "unknown",
                        "email",
                        full_path,          # stored server-side only
                        file_size_bytes,
                        "mail_watcher",
                        uploaded_at,
                        None,
                    ),
                )
                new_doc_id = cur.lastrowid
                conn.commit()
                _enqueue_extraction(new_doc_id, return_id)

                def _bg_classify():
                    try:
                        with app.app_context():
                            from ai_routes import _classify_document

                            _classify_document(new_doc_id, only_if_still_unknown=True)
                    except Exception as e:
                        logger.error(
                            "Background classify failed for doc %s: %s",
                            new_doc_id,
                            e,
                        )

                threading.Thread(target=_bg_classify, daemon=True).start()
                count += 1

            except Exception as e:
                logger.error(
                    f"Mail watcher: failed to save attachment {raw_filename!r} "
                    f"for return {return_id}: {e}"
                )
                try:
                    conn.rollback()
                except Exception:
                    pass

        logger.info(
            f"Mail watcher: attachment scan for return {return_id} finished "
            f"({count} file(s) saved)"
        )

    finally:
        conn.close()

    return count


# ── Note creation ─────────────────────────────────────────────────────────────

def _add_note(
    app, return_id: int, sender: str, subject: str, attachment_count: int,
    drive_share: bool = False,
) -> bool:
    """
    Create a staff-visible note on the return.
    notes table columns (from db.py): id, return_id, note_text, source, created_at.
    Email body is never stored — only the sender display name, subject,
    and attachment count.
    drive_share=True adds a manual-download reminder instead of an attachment count.
    Returns True if the note was committed.
    """
    from db import get_connection
    from utils import now

    if drive_share:
        note_text = (
            f"Email received from {sender}. "
            f"Subject: {subject}. "
            "Client shared a Google Drive link — manual download required. "
            "Check email for link and save the file to Documents."
        )
    else:
        note_text = (
            f"Email received from {sender}. "
            f"Subject: {subject}. "
            f"{attachment_count} attachment(s) added to documents. "
            "Review and tag document types."
        )
    conn = get_connection()
    try:
        conn.execute(
            "INSERT INTO notes (return_id, note_text, source, created_at) VALUES (?, ?, ?, ?)",
            (return_id, note_text, "EMAIL", now()),
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Mail watcher: failed to add note for return {return_id}: {e}")
        try:
            conn.rollback()
        except Exception:
            pass
        return False
    finally:
        conn.close()


# ── Unmatched logging ─────────────────────────────────────────────────────────

def _log_unmatched(app, sender: str, subject: str) -> None:
    """Log a warning when no client match is found. Nothing is written to the DB."""
    logger.warning(
        f"Mail watcher: no client match for email from {sender} — subject: {subject}"
    )
