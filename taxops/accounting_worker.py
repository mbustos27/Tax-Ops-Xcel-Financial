"""ACCOUNTING-7: Background OCR + COA categorisation worker for receipt_queue.

One long-lived daemon thread per process.  Polls ``receipt_queue`` for
``pending`` items, runs OCR via ``services.receipt_ocr``, then matches against
the Chart of Accounts via ``services.coa_matcher``.  On success the item moves
to ``review``; staff then approve/reject via the UI.

Threading model
---------------
  * ``_receipt_event`` (threading.Event) — set by upload routes to wake the
    worker immediately instead of waiting for ``POLL_INTERVAL``.
  * One item at a time — avoids Ollama congestion and simplifies error handling.
  * WAL SQLite: the worker holds its own connection; concurrent readers/writers
    are safe.

Retry logic
-----------
  attempts < ACCOUNTING_MAX_ATTEMPTS  → reset to 'pending', increment attempts
  attempts >= ACCOUNTING_MAX_ATTEMPTS → permanent 'failed', set error_message

Status flow: pending → processing → review (happy path)
                                 → pending  (retry)
                                 → failed   (dead-letter)
"""

from __future__ import annotations

import contextlib
import json
import logging
import threading
import time
from datetime import datetime, timezone

logger = logging.getLogger(__name__)

POLL_INTERVAL = 30  # seconds between idle polls

_receipt_event = threading.Event()


def _notify_receipt_worker() -> None:
    """Wake the receipt worker immediately (call from upload routes)."""
    _receipt_event.set()


def _now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _process_one(app) -> bool:
    """Pick one pending item and process it.  Returns True if an item was processed."""
    from db import get_connection
    import config as _cfg

    with app.app_context():
        with contextlib.closing(get_connection()) as conn:
            item = conn.execute(
                """
                SELECT * FROM receipt_queue
                WHERE status = 'pending'
                ORDER BY created_at
                LIMIT 1
                """
            ).fetchone()

        if not item:
            return False

        item_id = item["id"]
        image_path = item["image_path"]
        attempts = item["attempts"]

        logger.info("accounting_worker: processing receipt_queue id=%d (%s)", item_id, image_path)

        # Mark as processing
        with contextlib.closing(get_connection()) as conn:
            conn.execute(
                "UPDATE receipt_queue SET status='processing', attempts=attempts+1 WHERE id=?",
                (item_id,),
            )
            conn.commit()

        # ── OCR ──────────────────────────────────────────────────────────────
        ocr_result: dict = {}
        ocr_error: str | None = None
        try:
            from services.receipt_ocr import extract_receipt_data
            ocr_result = extract_receipt_data(image_path)
            ocr_error = ocr_result.get("_error")
        except Exception as exc:
            ocr_error = f"ocr_exception: {exc}"
            logger.error("accounting_worker: OCR exception for id=%d — %s", item_id, exc)

        if ocr_error:
            _handle_failure(item_id, attempts, ocr_error, _cfg.ACCOUNTING_MAX_ATTEMPTS)
            return True

        # ── COA match ─────────────────────────────────────────────────────────
        match_result: dict = {}
        try:
            from services.coa_matcher import get_matcher
            vendor = ocr_result.get("vendor") or ""
            # Build match text from vendor + line item descriptions
            item_descs = " ".join(
                li.get("description", "") for li in (ocr_result.get("line_items") or [])
            )
            match_text = f"{vendor} {item_descs}".strip() or vendor
            match_result = get_matcher().categorize(match_text)
        except Exception as exc:
            logger.warning("accounting_worker: COA match failed for id=%d — %s", item_id, exc)
            match_result = {
                "suggested_category": None,
                "suggested_account": None,
                "confidence": "low",
                "embedding_score": 0.0,
                "candidates": [],
            }

        # ── Persist results ────────────────────────────────────────────────────
        with contextlib.closing(get_connection()) as conn:
            conn.execute(
                """
                UPDATE receipt_queue
                SET status             = 'review',
                    ocr_raw            = ?,
                    vendor             = ?,
                    receipt_date       = ?,
                    total_amount       = ?,
                    payment_method     = ?,
                    line_items         = ?,
                    category_candidates= ?,
                    suggested_category = ?,
                    suggested_account  = ?,
                    confidence         = ?,
                    error_message      = NULL,
                    processed_at       = ?
                WHERE id = ?
                """,
                (
                    ocr_result.get("_raw", ""),
                    ocr_result.get("vendor"),
                    ocr_result.get("date"),
                    ocr_result.get("total_amount"),
                    ocr_result.get("payment_method"),
                    json.dumps(ocr_result.get("line_items") or []),
                    json.dumps(match_result.get("candidates") or []),
                    match_result.get("suggested_category"),
                    match_result.get("suggested_account"),
                    match_result.get("confidence"),
                    _now(),
                    item_id,
                ),
            )
            conn.commit()

        # ── ACCOUNTING-11: audit log ──────────────────────────────────────────
        _emit_audit(
            item_id=item_id,
            action="receipt_ocr_completed",
            after={
                "receipt_queue_id": item_id,
                "vendor": ocr_result.get("vendor"),
                "total_amount": ocr_result.get("total_amount"),
                "suggested_category": match_result.get("suggested_category"),
                "confidence": match_result.get("confidence"),
            },
        )

        logger.info(
            "accounting_worker: id=%d → review (vendor=%r, cat=%r, confidence=%s)",
            item_id,
            ocr_result.get("vendor"),
            match_result.get("suggested_category"),
            match_result.get("confidence"),
        )
        return True


def _handle_failure(item_id: int, current_attempts: int, error_msg: str, max_attempts: int) -> None:
    """Retry or permanently fail a receipt_queue item."""
    import contextlib
    from db import get_connection

    new_attempts = current_attempts + 1  # already incremented by the processing update
    if new_attempts < max_attempts:
        logger.info(
            "accounting_worker: id=%d failed (attempt %d/%d) — will retry: %s",
            item_id, new_attempts, max_attempts, error_msg,
        )
        with contextlib.closing(get_connection()) as conn:
            conn.execute(
                "UPDATE receipt_queue SET status='pending', error_message=? WHERE id=?",
                (f"Attempt {new_attempts}/{max_attempts}: {error_msg}", item_id),
            )
            conn.commit()
    else:
        logger.warning(
            "accounting_worker: id=%d permanently failed after %d attempts — %s",
            item_id, new_attempts, error_msg,
        )
        with contextlib.closing(get_connection()) as conn:
            conn.execute(
                """
                UPDATE receipt_queue
                SET status='failed', error_message=?, processed_at=?
                WHERE id=?
                """,
                (error_msg, _now(), item_id),
            )
            conn.commit()

        _emit_audit(
            item_id=item_id,
            action="receipt_ocr_failed",
            after={"receipt_queue_id": item_id, "error": error_msg, "attempts": new_attempts},
        )


def _emit_audit(*, item_id: int, action: str, after: dict) -> None:
    """ACCOUNTING-11: non-blocking audit write for receipt worker events."""
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id="accounting_worker",
            action=action,
            entity_type="receipt_queue",
            entity_id=str(item_id),
            before=None,
            after=after,
            ip_address=None,
            http_status=200,
        )
    except Exception as exc:
        logger.warning("accounting_worker: audit write failed — %s", exc)


def _worker_loop(app) -> None:
    """Main loop: process items until none remain, then wait for event or poll."""
    while True:
        try:
            processed = True
            while processed:
                processed = _process_one(app)
        except Exception as exc:
            logger.error("accounting_worker: unhandled exception in loop — %s", exc, exc_info=True)
        _receipt_event.wait(timeout=POLL_INTERVAL)
        _receipt_event.clear()


_accounting_thread: threading.Thread | None = None  # HEALTH: tracked for is_alive()


def start_accounting_worker(app) -> threading.Thread:
    """Start the background receipt processing thread.  Called from app startup."""
    global _accounting_thread
    t = threading.Thread(
        target=_worker_loop,
        args=(app,),
        name="accounting-worker",
        daemon=True,
    )
    _accounting_thread = t
    t.start()
    logger.info("accounting_worker: started daemon thread")
    return t


def accounting_worker_status() -> dict:
    """Return thread liveness for /health endpoint."""
    thread = _accounting_thread
    if thread is None:
        return {"started": False, "running": False}
    return {"started": True, "running": thread.is_alive()}
