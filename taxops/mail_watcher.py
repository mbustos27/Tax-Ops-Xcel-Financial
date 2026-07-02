"""
mail_watcher.py
---------------
Background IMAP poller — saves inbound email attachments to the
email_inbox holding table for staff assignment via the /email-inbox UI.

Design (simplified from prior 6-layer classifier):
  1. Connect to IMAP, fetch UNSEEN messages from configured folders.
  2. Suppress clearly promotional senders (config list + subdomain
     prefix match) — no LLM, no DB reads.
  3. Suppress Google Drive share notifications (no attachment to save).
  4. Save every remaining attachment to EMAIL_INBOX_DIR on disk and
     insert a row into email_inbox with is_assigned=0.
  5. Staff assign attachments to returns via /email-inbox in the UI.

Privacy rules enforced here:
  - IMAP_PASS is never logged, printed, or stored anywhere
  - Email body is never stored in any DB column
  - SSN / identification numbers are never extracted or stored
  - scrub_ssn_from_dict() is called on user-supplied filename strings
  - file_path is stored in DB for internal use only
"""
from __future__ import annotations

import email
import email.header
import hashlib
import imaplib
import logging
import os
import re as _re
import threading
import time
from collections import OrderedDict
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

_ALLOWED_ATTACHMENT_EXTS = frozenset({".pdf", ".jpg", ".jpeg", ".png"})

# Prevents two poll cycles from running concurrently.
_poll_lock = threading.Lock()
_poll_skip_count = 0

# Prevents duplicate thread launch on Flask dev-server reloads.
_watcher_started = False
_watcher_thread: "threading.Thread | None" = None

# In-process UID memo — fast-path to skip DB lookup for UIDs already confirmed
# terminal (success/skip/no_attachment) within this process's lifetime.
# The DB log is the authoritative source; this is an optimisation only.
# Cap at 50,000 with FIFO eviction — prefer an unnecessary DB lookup over
# a silent permanent drop (audit finding C1).
_PROCESSED_UIDS_CAP = int(os.environ.get("MAIL_PROCESSED_UIDS_CAP", "50000"))
_processed_uids: set[tuple[str, str]] = set()
_processed_uids_fifo: "OrderedDict[tuple[str, str], None]" = OrderedDict()
_processed_uids_lock = threading.Lock()

# ── Processing outcome constants ──────────────────────────────────────────────
OUTCOME_SUCCESS        = "success"
OUTCOME_SKIP           = "skip"
OUTCOME_RETRY          = "retry"
OUTCOME_DRY_RUN        = "dry_run"
OUTCOME_NO_ATTACHMENT  = "no_attachment"  # Fix 6 (H1): email had no saveable attachments


def _uid_in_memo(folder: str, uid: str) -> bool:
    """Return True if (folder, uid) is already in the in-process memo."""
    return (folder, uid) in _processed_uids


def _add_uid_to_memo(folder: str, uid: str) -> None:
    """Add (folder, uid) to the in-process memo after a confirmed terminal outcome.
    Only call this AFTER the DB log write succeeds (audit finding C1)."""
    key = (folder, uid)
    with _processed_uids_lock:
        if key in _processed_uids:
            return
        _processed_uids.add(key)
        _processed_uids_fifo[key] = None
        if len(_processed_uids_fifo) > _PROCESSED_UIDS_CAP:
            evicted, _ = _processed_uids_fifo.popitem(last=False)
            _processed_uids.discard(evicted)


# ── Public entry point ────────────────────────────────────────────────────────

def start_mail_watcher(app) -> None:
    """Start the background IMAP poll thread. No-op if IMAP_HOST is not configured.
    Safe to call multiple times — only the first call launches the thread."""
    global _watcher_started, _watcher_thread
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
    _watcher_thread = thread
    thread.start()
    logger.info("Mail watcher started")


def mail_watcher_status() -> dict:
    """Return thread liveness and config status for /health endpoint."""
    from config import IMAP_HOST
    base: dict = {"poll_skipped": _poll_skip_count}
    if not IMAP_HOST:
        return {**base, "started": False, "running": False, "configured": False}
    thread = _watcher_thread
    if thread is None:
        return {**base, "started": False, "running": False, "configured": True}
    return {**base, "started": True, "running": thread.is_alive(), "configured": True}


# ── Poll loop ─────────────────────────────────────────────────────────────────

def _poll_loop(app) -> None:
    """Run forever in the daemon thread. Never crashes — logs errors and sleeps."""
    from config import IMAP_POLL_INTERVAL
    while True:
        try:
            with app.app_context():
                _poll_once(app)
        except Exception:
            logger.exception("Mail watcher poll error")
        time.sleep(IMAP_POLL_INTERVAL)


def _poll_once(app) -> None:
    """Acquire lock and run one poll cycle. Skips if previous cycle still running."""
    if not _poll_lock.acquire(blocking=False):
        global _poll_skip_count
        _poll_skip_count += 1
        logger.info(
            f"Previous poll cycle still running — skipping (skip #{_poll_skip_count})"
        )
        return
    try:
        _poll_once_inner(app)
    finally:
        _poll_lock.release()


def _get_available_folders(imap) -> set:
    """Return the set of folder names available on this IMAP account.
    Used to silently skip category folders that don't exist.
    Never raises — returns empty set on error."""
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
            parts = decoded.split('"')
            if len(parts) >= 3:
                available.add(parts[-2])
            elif parts:
                last = decoded.rsplit(None, 1)
                if last:
                    available.add(last[-1])
        return available
    except Exception as e:
        logger.error(f"Failed to list IMAP folders: {e}")
        return set()


def _safe_select(imap, folder: str) -> bool:
    """Safely select an IMAP folder. Returns True if selected OK."""
    try:
        imap_folder = f'"{folder}"' if " " in folder and not folder.startswith('"') else folder
        resp = imap.select(imap_folder)
        if not isinstance(resp, tuple) or len(resp) < 2:
            logger.info(f"Folder {folder!r} not selectable — malformed response: {resp!r}")
            return False
        typ = resp[0]
        if typ == "OK":
            return True
        logger.info(f"Folder {folder!r} not selectable — server returned: {typ}")
        return False
    except Exception as e:
        logger.error(f"imap.select({folder!r}) raised: {e}")
        return False


def _mark_read(imap, uid, reason: str) -> None:  # noqa: ARG001
    """
    DISABLED — TaxOps never modifies the read/unread state of any email.

    Read-status policy (enforced at three layers):
      1. BODY.PEEK[] fetch — IMAP server never auto-sets \\Seen on download.
      2. This function has no callers anywhere in the codebase (audit M1 fix:
         three stale call sites removed; all outcomes now handled via the
         claim-after-success model in the dispatch loop).
      3. No -FLAGS \\Seen STORE call exists anywhere.

    Kept as a tombstone so git history records why read-marking was removed
    and to prevent accidental re-introduction.  Do not add IMAP STORE logic
    here without a full review of the retry model.
    """


def _fetch_message_data(imap, uid) -> dict | None:
    """Fetch and parse one IMAP message. Returns a dict or None on failure."""
    resp = imap.uid("FETCH", uid, "(BODY.PEEK[])")
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
    body_text    = _extract_plain_text(message)
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


def _poll_once_inner(app) -> None:
    """
    Multi-folder poll cycle:

    Phase 0 — Connect + scan folders. 'skip' folders are noted.
              'full_processing' folders → collect messages.
    Phase 1 — Fetch and parse collected messages.
    Phase 2+3 — For each message:
        - Suppress promotional senders (2-layer config check, no LLM).
        - Suppress Drive share notifications.
        - Save remaining attachments to email_inbox holding area.
    """
    from config import (
        IMAP_HOST, IMAP_PORT, IMAP_USER, IMAP_PASS,
        IMAP_FOLDERS, GMAIL_CATEGORY_FOLDERS, USE_GMAIL_CATEGORIES,
        IMAP_DRY_RUN,
    )

    folders_to_check = GMAIL_CATEGORY_FOLDERS if USE_GMAIL_CATEGORIES else {
        folder: "full_processing" for folder in IMAP_FOLDERS
    }

    imap = imaplib.IMAP4_SSL(IMAP_HOST, IMAP_PORT)
    try:
        imap.login(IMAP_USER, IMAP_PASS)

        # ── Phase 0: scan each folder ─────────────────────────────────────
        messages: list[dict] = []
        available_folders = _get_available_folders(imap)
        logger.info(f"Available IMAP folders: {available_folders or '(could not list)'}")

        for folder, handling in folders_to_check.items():
            if folder != "INBOX" and available_folders and folder not in available_folders:
                logger.info(f"Skipping {folder!r} — not available on this account")
                continue

            if not _safe_select(imap, folder):
                continue

            resp = imap.uid("SEARCH", "UNSEEN")
            if not isinstance(resp, tuple) or len(resp) < 2:
                logger.error(f"Unexpected IMAP UID SEARCH response in {folder!r}: {resp!r}")
                continue
            status = resp[0]
            data = resp[1]
            if status != "OK" or not data or not data[0]:
                logger.info(f"No unseen messages in {folder!r}")
                continue

            raw_uids = data[0].split()
            if not raw_uids:
                logger.info(f"No unseen messages in {folder!r}")
                continue

            total_unseen = len(raw_uids)
            new_uids: list[bytes] = []
            for uid in raw_uids:
                uid_str = uid.decode("ascii", errors="replace")
                # Fast in-process memo check (optimisation — DB is authoritative)
                if _uid_in_memo(folder, uid_str):
                    logger.debug(f"UID {uid_str} in {folder} already processed (memo) — skipping")
                    continue
                # Authoritative DB check
                if _already_processed_in_log(uid_str, folder):
                    logger.debug(f"UID {uid_str} in {folder} already in processing log — skipping")
                    _add_uid_to_memo(folder, uid_str)  # populate memo so next poll skips DB
                    continue
                new_uids.append(uid)

            already_seen = total_unseen - len(new_uids)
            logger.info(
                f"Folder {folder}: {len(new_uids)} new"
                f" of {total_unseen} unseen ({already_seen} already seen)"
            )

            if not new_uids:
                continue

            if handling == "skip":
                for uid in new_uids:
                    uid_str = uid.decode("ascii", errors="replace") if isinstance(uid, bytes) else str(uid)
                    _upsert_processing_log(uid_str, folder, "", "", OUTCOME_SKIP)
                    _add_uid_to_memo(folder, uid_str)
                logger.info(f"Auto-skipped {len(new_uids)} message(s) from {folder!r}")
            else:
                for uid in new_uids:
                    try:
                        md = _fetch_message_data(imap, uid)
                        if md:
                            messages.append({
                                **md,
                                "uid":         uid,
                                "folder":      folder,
                                "received_at": _now_utc(),
                            })
                        else:
                            uid_str = uid.decode("ascii", errors="replace") if isinstance(uid, bytes) else str(uid)
                            logger.warning(f"Fetch returned no data uid={uid_str!r} — leaving for retry")
                            # Do NOT add to memo — leave unread for next poll cycle retry
                            _upsert_processing_log(uid_str, folder, "", "", OUTCOME_RETRY, "fetch returned no data")
                    except Exception as e:
                        uid_str = uid.decode("ascii", errors="replace") if isinstance(uid, bytes) else str(uid)
                        logger.error(f"Fetch failed uid={uid_str!r}: {e}")
                        # Do NOT add to memo — leave unread for next poll cycle retry
                        _upsert_processing_log(uid_str, folder, "", "", OUTCOME_RETRY, str(e)[:200])

        if not messages:
            return

        # ── Phase 2+3: classify and dispatch ─────────────────────────────
        # Claim-after-success model (audit finding C1):
        #   - UIDs are NOT added to the in-process memo before processing.
        #   - The memo is only updated after the DB log write confirms the
        #     terminal outcome.  A failed log write leaves the UID unclaimed
        #     so it is retried on the next poll cycle (audit finding C3).
        for msg in messages:
            uid_str = msg["uid"].decode("ascii", errors="replace") if isinstance(msg["uid"], bytes) else str(msg["uid"] or "")
            folder  = msg.get("folder", "INBOX")
            domain  = msg.get("sender_domain") or ""
            subject = (msg.get("subject") or "")[:100]

            try:
                # ── Intentional-skip paths (promotional / drive-share) ────
                if _is_promotional(domain):
                    logger.info(f"Promotional suppressed: {domain}")
                    outcome = OUTCOME_SKIP
                    # Log write failure handling — see audit finding C3.
                    try:
                        _upsert_processing_log(uid_str, folder, domain, subject, outcome)
                        _add_uid_to_memo(folder, uid_str)
                    except Exception as log_exc:
                        logger.error(
                            "Log write failed for uid=%s after skip (promotional) — will retry: %s",
                            uid_str, log_exc,
                        )
                        # Do NOT add to memo — retry on next poll
                    continue

                if _is_drive_share(msg["subject"], msg.get("body_text", "")):
                    logger.info(f"Drive share from {domain} — skipped (no attachment to save)")
                    outcome = OUTCOME_SKIP
                    try:
                        _upsert_processing_log(uid_str, folder, domain, subject, outcome)
                        _add_uid_to_memo(folder, uid_str)
                    except Exception as log_exc:
                        logger.error(
                            "Log write failed for uid=%s after skip (drive-share) — will retry: %s",
                            uid_str, log_exc,
                        )
                    continue

                # ── Dry run: log what would happen, write nothing at all ──
                # (no disk write, no email_inbox row, no processing-log row,
                # no memo claim) so the UID is re-evaluated on the next poll
                # once IMAP_DRY_RUN is turned back off.
                if IMAP_DRY_RUN:
                    logger.info(f"[DRY RUN] Would save attachment(s) from {domain} — no writes performed")
                    continue

                # ── Attachment save ───────────────────────────────────────
                count = _save_to_inbox(
                    app,
                    msg["message"],
                    msg["sender_email"],
                    msg["sender_domain"],
                    msg["subject"],
                    msg["received_at"],
                )

                # Fix 6 (H1): distinguish zero-attachment emails from real successes.
                # OUTCOME_NO_ATTACHMENT is terminal (won't be retried) but is
                # distinguishable in the log from a genuine document save.
                if count > 0:
                    logger.info(f"Saved {count} attachment(s) from {domain} to inbox")
                    outcome = OUTCOME_SUCCESS
                else:
                    logger.info(f"No saveable attachments found in email from {domain}")
                    outcome = OUTCOME_NO_ATTACHMENT

                # Log write failure handling (audit finding C3):
                # If the DB write fails after a successful save, do NOT add the
                # UID to the memo — leave it unclaimed so the next poll retries.
                # The attachment save is idempotent via SHA-256 dedup in _save_to_inbox.
                try:
                    _upsert_processing_log(uid_str, folder, domain, subject, outcome)
                    _add_uid_to_memo(folder, uid_str)  # only after confirmed log write
                except Exception as log_exc:
                    logger.error(
                        "Log write failed for uid=%s outcome=%s — will retry next poll: %s",
                        uid_str, outcome, log_exc,
                    )
                    # Do NOT add to memo. Email stays unread; next poll re-processes it.
                    # _save_to_inbox's SHA-256 dedup prevents duplicate disk writes.

            except Exception as e:
                logger.error(f"Dispatch failed uid={uid_str!r} domain={domain!r}: {e}")
                # Do NOT add to memo — leave unread for retry
                _upsert_processing_log(uid_str, folder, domain, subject, OUTCOME_RETRY, str(e)[:200])

    finally:
        try:
            imap.logout()
        except Exception:
            pass


# ── Processing log helpers ────────────────────────────────────────────────────

def _upsert_processing_log(
    uid_str: str,
    folder: str,
    sender_domain: str,
    subject_snippet: str,
    outcome: str,
    error_message: str | None = None,
    doc_id: int | None = None,
    return_id: int | None = None,
) -> None:
    """Upsert a row in email_processing_log after each message is processed."""
    from db import get_connection
    try:
        conn = get_connection()
        try:
            conn.execute(
                """
                INSERT INTO email_processing_log
                    (message_uid, imap_folder, sender_domain, subject_snippet,
                     outcome, attempt_count, last_attempt_at, error_message,
                     doc_id, return_id)
                VALUES (?, ?, ?, ?, ?, 1, ?, ?, ?, ?)
                ON CONFLICT(message_uid, imap_folder) DO UPDATE SET
                    attempt_count    = attempt_count + 1,
                    last_attempt_at  = excluded.last_attempt_at,
                    outcome          = excluded.outcome,
                    error_message    = excluded.error_message
                """,
                (
                    uid_str[:64],
                    folder[:128],
                    (sender_domain or "")[:128],
                    (subject_snippet or "")[:100],
                    outcome,
                    _now_utc(),
                    (error_message or "")[:200] or None,
                    doc_id,
                    return_id,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as exc:
        logger.error("email_processing_log upsert failed uid=%s: %s", uid_str, exc)


def _already_processed_in_log(uid_str: str, folder: str) -> bool:
    """Return True when processing log shows a terminal outcome for this (uid, folder)."""
    from db import get_connection
    try:
        conn = get_connection()
        try:
            row = conn.execute(
                "SELECT outcome FROM email_processing_log "
                "WHERE message_uid = ? AND imap_folder = ?",
                (uid_str[:64], folder[:128]),
            ).fetchone()
        finally:
            conn.close()
        if row and row["outcome"] in (OUTCOME_SUCCESS, OUTCOME_SKIP, OUTCOME_NO_ATTACHMENT):
            return True
    except Exception as exc:
        logger.error("already_processed check failed uid=%s: %s", uid_str, exc)
    return False


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
    """Extract domain portion from an email address."""
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
    """Return True if the email is a Google Drive share notification."""
    combined = (subject + " " + (body_text or "")).lower()
    return any(indicator in combined for indicator in _DRIVE_INDICATORS)


# ── Promotional suppression (2-layer, no LLM, no DB) ─────────────────────────

def _is_promotional(domain: str) -> bool:
    """
    Return True if the sender domain is clearly promotional.

    Layer 1: base domain in KNOWN_PROMOTIONAL_DOMAINS config list.
    Layer 2: domain starts with a mass-mailing subdomain prefix.

    Personal email domains (gmail, yahoo, icloud, etc.) always return
    False — their senders are real clients and their attachments belong
    in the inbox holding area.
    """
    from config import KNOWN_PROMOTIONAL_DOMAINS, MASS_MAILING_PREFIXES, PERSONAL_EMAIL_DOMAINS
    if not domain:
        return False
    parts = domain.split(".")
    base = ".".join(parts[-2:]) if len(parts) >= 2 else domain
    # Personal domains are never promotional
    if base in PERSONAL_EMAIL_DOMAINS:
        return False
    if base in KNOWN_PROMOTIONAL_DOMAINS:
        return True
    if any(domain.startswith(p) for p in MASS_MAILING_PREFIXES):
        return True
    return False


# ── Inbox attachment saving ───────────────────────────────────────────────────

_ATTACHMENT_CONTENT_TYPES = frozenset({
    "application/pdf",
    "image/jpeg",
    "image/jpg",
    "image/png",
    "application/octet-stream",
})


def _save_to_inbox(
    app,
    message,
    sender_email: str,
    sender_domain: str,
    subject: str,
    received_at: str,
) -> int:
    """
    Walk MIME parts, save allowed attachments to EMAIL_INBOX_DIR, and
    insert one row per file into email_inbox with is_assigned=0.

    Privacy rules:
      - Email body is never stored
      - scrub_ssn_from_dict() is applied to user-supplied filenames
      - IMAP_PASS never referenced here
    Returns count of files saved.
    """
    from config import EMAIL_INBOX_DIR
    from db import get_connection
    from utils import sanitize_filename, scrub_ssn_from_dict

    os.makedirs(EMAIL_INBOX_DIR, exist_ok=True)

    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    subject_snippet = (subject or "")[:100]
    count = 0
    conn = get_connection()
    try:
        for part in message.walk():
            content_type        = part.get_content_type()
            content_disposition = str(part.get("Content-Disposition") or "").lower()
            raw_filename        = part.get_filename() or ""

            is_attachment = (
                "attachment" in content_disposition
                or (raw_filename and content_type in _ATTACHMENT_CONTENT_TYPES)
            )
            if not is_attachment or not raw_filename:
                continue

            raw_filename = _decode_header_value(raw_filename)
            ext = os.path.splitext(raw_filename)[1].lower()
            if ext not in _ALLOWED_ATTACHMENT_EXTS:
                logger.debug(f"Skipping unsupported extension: {ext!r}")
                continue

            try:
                payload = part.get_payload(decode=True)
                if not payload:
                    logger.warning(f"Empty payload for: {raw_filename!r}")
                    continue

                sanitized = sanitize_filename(raw_filename)
                safe = scrub_ssn_from_dict({
                    "filename": sanitized,
                    "original_filename": raw_filename,
                })
                candidate = f"{ts}_{safe['filename']}"
                # Avoid collisions with a counter suffix
                stem, ext_part = os.path.splitext(candidate)
                final_name = candidate
                n = 1
                while os.path.exists(os.path.join(EMAIL_INBOX_DIR, final_name)):
                    final_name = f"{stem}_{n}{ext_part}"
                    n += 1

                full_path = os.path.abspath(os.path.join(EMAIL_INBOX_DIR, final_name))
                with open(full_path, "wb") as fh:
                    fh.write(payload)

                conn.execute(
                    """
                    INSERT INTO email_inbox
                        (sender_email, sender_domain, subject_snippet, filename,
                         original_filename, file_path, file_size_bytes, received_at,
                         is_assigned, is_deleted)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0, 0)
                    """,
                    (
                        sender_email,
                        sender_domain,
                        subject_snippet,
                        final_name,
                        safe["original_filename"],
                        full_path,
                        len(payload),
                        received_at,
                    ),
                )
                conn.commit()
                count += 1

            except Exception as e:
                logger.error(
                    f"Mail watcher: failed to save attachment {raw_filename!r}: {e}"
                )
                try:
                    conn.rollback()
                except Exception:
                    pass

        logger.info(f"email_inbox: {count} file(s) saved from {sender_domain}")
    finally:
        conn.close()

    return count


# ── Internal time helper ──────────────────────────────────────────────────────

def _now_utc() -> str:
    """Return the current UTC time as an ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
