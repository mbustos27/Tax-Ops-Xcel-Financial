"""
Background document extraction worker for TaxOps.
Polls extraction_queue, extracts fields, detects form types,
tags documents silently if confident, queues for review if not.

DEBT-8 — confidence scoring rationale
--------------------------------------
``_extract_fields(file_path, filename) -> (fields | None, method | None)``
  Returns the cleaned field dict and the extraction method ("text" or "vision"),
  or ``(None, None)`` on failure.  It does NOT return a confidence value because
  the LLM output alone is insufficient to judge quality — detected_type is also
  required.

``_compute_confidence(fields, detected_type) -> float [0.0 – 1.0]``
  Computes the final score AFTER the type is resolved:

  * **paystub**: weighted sum of employer/employee name (0.28), YTD gross/net (0.32),
    overtime indicators (0.35), plus a bonus 0.12 for explicit form_type label.
  * **W-2** (``w2_records``): fraction of [employer_name, tax_year,
    box1_wages_tips_other, box2_federal_income_tax_withheld] present, +0.1 if
    form_type is set.
  * **1099-NEC/MISC/INT/DIV**: similar fraction of their respective required fields.
  * **Unknown type** (no ``required_fields`` entry): returns 0.5 (neutral; routes
    to review queue for manual type assignment).
  * **No detected_type or empty fields**: returns 0.0.

  ``HIGH_CONFIDENCE = 0.85`` — documents at or above this threshold are
  auto-tagged and saved without human review.  Below this threshold the item is
  placed on the review queue (status = 'needs_review').

Vision fallback
---------------
Image-only documents (JPEG/PNG) and PDFs with no embedded text always go through
the vision model (``OLLAMA_EXTRACT_MODEL_VISION``).  Multi-page PDFs are rendered
to a single composite thumbnail by ``_pdf_to_image_b64`` before sending to the
vision model.  For very long documents this means later pages may be underweighted;
a future improvement can cap and send multiple thumbnails per page range.
"""

from __future__ import annotations

import json
import logging
import os
import re as _re
import threading
import time
from typing import Any

from config import (
    OLLAMA_EXTRACT_MODEL_TEXT,
    OLLAMA_EXTRACT_MODEL_VISION,
    OLLAMA_EXTRACT_TIMEOUT_TEXT,
    OLLAMA_EXTRACT_TIMEOUT_VISION,
)
from db import get_connection
from form_schema import FORM_INTEGER_COLUMNS

# LLM extraction allow-list — IRS box-* keys only (FORMS-3: no pre-DOC7 mirror names).
# Includes prompt-schema fields omitted from inline spec (full W‑2 sec. b local, DIV/INT/MISC parity).
_DOCUMENT_EXTRACT_ALLOWED_KEYS: frozenset[str] = frozenset(
    {
        "form_type",
        "tax_year",
        "employer_name",
        "employer_address",
        "payer_name",
        "payer_address",
        "box1_wages_tips_other",
        "box2_federal_income_tax_withheld",
        "box3_social_security_wages",
        "box4_social_security_tax_withheld",
        "box5_medicare_wages_tips",
        "box6_medicare_tax_withheld",
        "box7_social_security_tips",
        "box8_allocated_tips",
        "box10_dependent_care_benefits",
        "box11_nonqualified_plans",
        "box12a_code",
        "box12a_amount",
        "box12b_code",
        "box12b_amount",
        "box12c_code",
        "box12c_amount",
        "box12d_code",
        "box12d_amount",
        "box13_statutory_employee",
        "box13_retirement_plan",
        "box13_third_party_sick_pay",
        "box14_other",
        "box15_state",
        "box16_state_wages",
        "box17_state_income_tax",
        "box18_local_wages",
        "box19_local_income_tax",
        "box20_locality_name",
        "box15b_state",
        "box16b_state_wages",
        "box17b_state_income_tax",
        "box18b_local_wages",
        "box19b_local_income_tax",
        "box20b_locality_name",
        "box1_nonemployee_compensation",
        "box2_direct_sales_indicator",
        "box4_federal_income_tax_withheld",
        "box5_state_tax_withheld",
        "box6_state",
        "box7_state_income",
        "box1_rents",
        "box2_royalties",
        "box3_other_income",
        "box4_federal_income_tax_withheld",
        "box5_fishing_boat_proceeds",
        "box6_medical_health_care_payments",
        "box7_direct_sales_indicator",
        "box8_substitute_payments",
        "box9_crop_insurance_proceeds",
        "box10_gross_proceeds_attorney",
        "box11_fish_purchased_resale",
        "box12_section_409a_deferrals",
        "box14_gross_proceeds_attorney",
        "box15_section_409a_income",
        "box16_state_tax_withheld",
        "box17_state",
        "box18_state_income",
        "box1_interest_income",
        "box2_early_withdrawal_penalty",
        "box3_us_savings_bond_treasury_interest",
        "box4_federal_income_tax_withheld",
        "box5_investment_expenses",
        "box6_foreign_tax_paid",
        "box7_foreign_country",
        "box8_tax_exempt_interest",
        "box9_specified_private_activity_bond_interest",
        "box10_market_discount",
        "box11_bond_premium",
        "box12_bond_premium_treasury_obligations",
        "box13_bond_premium_tax_exempt_bond",
        "box14_tax_exempt_bond_cusip",
        "box15_state",
        "box16_state_identification",
        "box17_state_tax_withheld",
        "box1a_total_ordinary_dividends",
        "box1b_qualified_dividends",
        "box2a_total_capital_gain",
        "box2b_unrecap_sec1250_gain",
        "box2c_section_1202_gain",
        "box2d_collectibles_gain",
        "box2e_section_897_ordinary_dividends",
        "box2f_section_897_capital_gain",
        "box3_nondividend_distributions",
        "box4_federal_income_tax_withheld",
        "box5_section_199a_dividends",
        "box6_investment_expenses",
        "box7_foreign_tax_paid",
        "box8_foreign_country",
        "box9_cash_liquidation_distributions",
        "box10_noncash_liquidation_distributions",
        "box11_fatca_filing_requirement",
        "box12_exempt_interest_dividends",
        "box13_specified_private_activity_bond",
        "box14_state",
        "box15_state_identification",
        "box16_state_tax_withheld",
        # ── PAYSTUB / check stub (earnings statements with OT & YTD) ─────────────
        "employee_name",
        "pay_period_start",
        "pay_period_end",
        "pay_date",
        "gross_pay_this_period",
        "net_pay_this_period",
        "regular_hours",
        "regular_pay",
        "overtime_hours",
        "overtime_pay",
        "has_overtime",
        "ytd_gross",
        "ytd_net",
        "ytd_federal_tax",
        "ytd_state_tax",
        "ytd_social_security",
        "ytd_medicare",
    }
)

logger = logging.getLogger(__name__)

POLL_INTERVAL = 60          # seconds between queue checks (max wait when idle)
MAX_ATTEMPTS = 3           # max retries before permanently failing (dead letter)
HIGH_CONFIDENCE = 0.85     # threshold for silent auto-tagging
_SUPPORTED_EXTS = frozenset({".pdf", ".jpg", ".jpeg", ".png"})

_worker_started = False
_worker_lock = threading.Lock()
_worker_thread: threading.Thread | None = None  # HEALTH-3: tracked for is_alive() check

# REL-5: event signalled by _notify_extraction_worker() when a new row is enqueued.
# The worker uses event.wait(POLL_INTERVAL) so it wakes immediately rather than
# waiting up to 60 s after an upload.
_work_event = threading.Event()


def _notify_extraction_worker() -> None:
    """Signal the worker that a new queue row is available."""
    _work_event.set()


def _emit_extraction_audit(
    *,
    doc_id: int,
    return_id: int,
    status: str,
    confidence: float | None = None,
    detected_type: str | None = None,
    reason: str | None = None,
    method: str | None = None,
) -> None:
    """DOC-HARD-5: non-blocking audit log entry for extraction outcomes."""
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id="extractor",
            action=f"extraction_{status}",
            entity_type="return_document",
            entity_id=str(doc_id),
            before=None,
            after={
                "doc_id": doc_id,
                "return_id": return_id,
                "status": status,
                "confidence": confidence,
                "detected_type": detected_type,
                "extraction_method": method,
                "reason": reason,
            },
            ip_address=None,
            http_status=200,
        )
    except Exception as exc:
        logger.warning("extraction_audit write failed: %s", exc)


def start_extraction_worker(app) -> None:
    """Start the background extraction worker thread (daemon)."""
    global _worker_started, _worker_thread
    with _worker_lock:
        if _worker_started:
            logger.info("Extraction worker already running")
            return
        _worker_started = True
    thread = threading.Thread(
        target=_worker_loop,
        args=(app,),
        daemon=True,
        name="extraction-worker",
    )
    _worker_thread = thread
    thread.start()
    logger.info("Extraction worker started")


def extraction_worker_status() -> dict:
    """HEALTH-3: return thread liveness for /health endpoint."""
    thread = _worker_thread
    if thread is None:
        return {"started": False, "running": False}
    return {"started": True, "running": thread.is_alive()}


def _worker_loop(app):
    while True:
        # REL-5: block until woken by _notify_extraction_worker() or POLL_INTERVAL expires.
        _work_event.wait(timeout=POLL_INTERVAL)
        _work_event.clear()
        try:
            with app.app_context():
                _process_queue()
        except Exception as e:
            logger.error("Extraction worker error: %s", e)


def _process_queue():
    conn = get_connection()
    try:
        pending = conn.execute(
            """
            SELECT eq.*, rd.file_path, rd.filename, rd.doc_type
            FROM extraction_queue eq
            JOIN return_documents rd ON eq.doc_id = rd.id
            WHERE eq.status = 'pending'
              AND eq.attempts < ?
              AND rd.is_deleted = 0
            ORDER BY eq.created_at ASC
            LIMIT 10
            """,
            (MAX_ATTEMPTS,),
        ).fetchall()
        if not pending:
            return
        logger.info("Extraction worker: %s items to process", len(pending))
        for row in pending:
            _process_item(conn, dict(row))
    finally:
        conn.close()


def _table_from_form_type_hint(raw: str | None) -> str | None:
    if not raw or not str(raw).strip():
        return None
    u = str(raw).strip().upper()
    if u == "UNKNOWN":
        return None
    compact = u.replace("-", " ")
    if "1099" in compact:
        if "NEC" in compact:
            return "f1099_nec_records"
        if "MISC" in compact:
            return "f1099_misc_records"
        if "INT" in compact:
            return "f1099_int_records"
        if "DIV" in compact:
            return "f1099_div_records"
    if "W-2" in u or "W2" in u.replace(" ", "").replace("-", ""):
        return "w2_records"
    cup = compact.replace(" ", "")
    if compact in ("CHECK STUB", "PAY STUB") or cup in {"PAYSTUB", "CHECKSTUB"}:
        return "paystub"
    return None


def _resolve_detected_table(doc_type_db: str | None, fields: dict) -> str | None:
    from form_store import _detect_form_type

    hint_table = _table_from_form_type_hint(fields.get("form_type"))
    if hint_table:
        return hint_table
    return _detect_form_type(doc_type_db, fields)


def _process_item(conn, item: dict) -> None:
    from form_store import (
        _apply_extraction_doc_tag,
        _form_table_to_doc_type,
        _save_form_data,
    )
    from utils import now as get_now, scrub_ssn_from_dict

    item_id = item["id"]
    doc_id = item["doc_id"]
    return_id = item["return_id"]
    file_path = item["file_path"]
    filename = item["filename"]
    current_doc_type = item["doc_type"]

    if not file_path or not os.path.isfile(file_path):
        conn.execute(
            """
            UPDATE extraction_queue
            SET status = 'failed',
                error_message = 'File not found on disk',
                processed_at = ?
            WHERE id = ?
            """,
            (get_now(), item_id),
        )
        conn.commit()
        _emit_extraction_audit(doc_id=doc_id, return_id=return_id, status="failed",
                               reason="File not found on disk")
        return

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _SUPPORTED_EXTS:
        conn.execute(
            """
            UPDATE extraction_queue
            SET status = 'skipped',
                error_message = ?,
                processed_at = ?
            WHERE id = ?
            """,
            (f"Unsupported extension: {ext}", get_now(), item_id),
        )
        conn.commit()
        return

    conn.execute(
        """
        UPDATE extraction_queue
        SET status = 'processing', attempts = attempts + 1
        WHERE id = ?
        """,
        (item_id,),
    )
    conn.commit()

    try:
        fields, method = _extract_fields(file_path, filename)

        if method == "image_skipped":
            conn.execute(
                """
                UPDATE extraction_queue
                SET status = 'skipped',
                    extraction_method = 'image_skipped',
                    error_message = 'Image file — vision extraction disabled. Tag manually.',
                    processed_at = ?
                WHERE id = ?
                """,
                (get_now(), item_id),
            )
            conn.commit()
            logger.info(
                "Skipped vision extraction for %s — image files are tagged manually",
                filename,
            )
            return

        if not fields:
            # DOC-HARD-2: retry up to MAX_ATTEMPTS before permanently failing.
            new_attempts = item["attempts"] + 1  # DB already incremented
            if new_attempts < MAX_ATTEMPTS:
                logger.info(
                    "Doc %s: no fields extracted (attempt %d/%d) — will retry",
                    doc_id, new_attempts, MAX_ATTEMPTS,
                )
                conn.execute(
                    "UPDATE extraction_queue SET status = 'pending', error_message = ? WHERE id = ?",
                    (f"No fields extracted (attempt {new_attempts}/{MAX_ATTEMPTS})", item_id),
                )
            else:
                logger.warning(
                    "Doc %s: no fields extracted after %d attempts — dead letter",
                    doc_id, new_attempts,
                )
                conn.execute(
                    """
                    UPDATE extraction_queue
                    SET status = 'failed',
                        error_message = 'No fields extracted after %d attempts',
                        processed_at = ?
                    WHERE id = ?
                    """ % new_attempts,
                    (get_now(), item_id),
                )
            conn.commit()
            return

        detected_type = _resolve_detected_table(current_doc_type, fields)
        confidence = _compute_confidence(fields, detected_type)
        safe_fields = scrub_ssn_from_dict(dict(fields))
        fields_json = json.dumps(safe_fields)

        if confidence >= HIGH_CONFIDENCE and detected_type:
            if detected_type == "paystub":
                saved = True
                doc_tag = _form_table_to_doc_type("paystub")
            else:
                saved = _save_form_data(
                    conn, detected_type, return_id, doc_id, safe_fields
                )
                doc_tag = _form_table_to_doc_type(detected_type)
            if saved and doc_tag != "unknown":
                tagged = _apply_extraction_doc_tag(
                    conn, doc_id=doc_id, return_id=return_id, doc_tag=doc_tag
                )
                if tagged:
                    if detected_type == "paystub":
                        logger.info(
                            "Auto-tagged doc %s as paystub "
                            "(no typed SQL row; confidence %.2f)",
                            doc_id,
                            confidence,
                        )
                    else:
                        logger.info(
                            "Auto-tagged doc %s as %s after form save "
                            "(confidence %.2f, table %s)",
                            doc_id,
                            doc_tag,
                            confidence,
                            detected_type,
                        )
                conn.execute(
                    """
                    UPDATE extraction_queue
                    SET status = 'completed',
                        confidence = ?,
                        detected_form_type = ?,
                        extracted_fields = ?,
                        extraction_method = ?,
                        processed_at = ?
                    WHERE id = ?
                    """,
                    (
                        confidence,
                        detected_type,
                        fields_json,
                        method,
                        get_now(),
                        item_id,
                    ),
                )
                # DOC-HARD-5: emit audit trail for completed extraction.
                _emit_extraction_audit(
                    doc_id=doc_id, return_id=return_id, status="completed",
                    confidence=confidence, detected_type=detected_type, method=method,
                )
            else:
                logger.info(
                    "Doc %s high confidence %.2f but persist failed "
                    "or ambiguous tag — needs review (%s)",
                    doc_id,
                    confidence,
                    detected_type,
                )
                conn.execute(
                    """
                    UPDATE extraction_queue
                    SET status = 'needs_review',
                        confidence = ?,
                        detected_form_type = ?,
                        extracted_fields = ?,
                        extraction_method = ?,
                        processed_at = ?
                    WHERE id = ?
                    """,
                    (
                        confidence,
                        detected_type,
                        fields_json,
                        method,
                        get_now(),
                        item_id,
                    ),
                )
        else:
            logger.info(
                "Doc %s needs review (confidence %.2f, type=%s)",
                doc_id,
                confidence,
                detected_type,
            )
            conn.execute(
                """
                UPDATE extraction_queue
                SET status = 'needs_review',
                    confidence = ?,
                    detected_form_type = ?,
                    extracted_fields = ?,
                    extraction_method = ?,
                    processed_at = ?
                WHERE id = ?
                """,
                (
                    confidence,
                    detected_type,
                    fields_json,
                    method,
                    get_now(),
                    item_id,
                ),
            )

        conn.commit()

    except Exception as e:
        err = str(e)[:200]
        logger.error("Extraction failed for doc %s: %s", doc_id, e)
        # DOC-HARD-2: retry up to MAX_ATTEMPTS; permanently fail on exhaustion (dead letter).
        new_attempts = item["attempts"] + 1  # DB already incremented
        if new_attempts < MAX_ATTEMPTS:
            status = "pending"
            logger.info(
                "Doc %s: extraction error (attempt %d/%d) — will retry",
                doc_id, new_attempts, MAX_ATTEMPTS,
            )
        else:
            status = "failed"
            logger.warning(
                "Doc %s: extraction permanently failed after %d attempts",
                doc_id, new_attempts,
            )
        try:
            conn.execute(
                """
                UPDATE extraction_queue
                SET status = ?,
                    error_message = ?,
                    processed_at = CASE WHEN ? = 'failed' THEN ? ELSE processed_at END
                WHERE id = ?
                """,
                (status, err, status, get_now(), item_id),
            )
            conn.commit()
            if status == "failed":
                # DOC-HARD-5: audit trail for permanent (dead-letter) failure.
                _emit_extraction_audit(
                    doc_id=doc_id, return_id=return_id, status="failed", reason=err,
                )
        except Exception:
            conn.rollback()


def _irs_form_extraction_block() -> str:
    """Shared IRS box-level instructions for text and vision extractors."""
    return (
        "You are a tax document data extraction assistant for a US tax preparation office.\n"
        "Extract ALL available fields from the tax document.\n"
        "Return ONLY valid JSON — no markdown, no explanation.\n\n"
        "CRITICAL PRIVACY RULES:\n"
        "- NEVER extract Social Security Numbers (SSN) — not full, not partial\n"
        "- NEVER extract Employer Identification Numbers (EIN)\n"
        "- NEVER extract Taxpayer Identification Numbers (TIN)\n"
        "- If you see a 9-digit number formatted as XXX-XX-XXXX or XX-XXXXXXX — skip it\n"
        "- Vision runs only after mechanical PDF text proves unusable — it is never used to transcribe "
        "SSN/EIN/TIN from imagery into JSON (leave identification numbers blank)\n\n"
        "MONETARY VALUES: numbers only, no $ symbols, no commas (e.g. '52000' not '$52,000')\n"
        "EMPTY FIELDS: use empty string '' for fields not found — do not guess\n\n"
        "First identify the form type, then extract all applicable fields:\n\n"
        "FOR W-2 (Wage and Tax Statement):\n"
        '{"form_type": "W-2", "tax_year": "", '
        '"employer_name": "", "employer_address": "", '
        '"box1_wages_tips_other": "", '
        '"box2_federal_income_tax_withheld": "", '
        '"box3_social_security_wages": "", '
        '"box4_social_security_tax_withheld": "", '
        '"box5_medicare_wages_tips": "", '
        '"box6_medicare_tax_withheld": "", '
        '"box7_social_security_tips": "", '
        '"box8_allocated_tips": "", '
        '"box10_dependent_care_benefits": "", '
        '"box11_nonqualified_plans": "", '
        '"box12a_code": "", "box12a_amount": "", '
        '"box12b_code": "", "box12b_amount": "", '
        '"box12c_code": "", "box12c_amount": "", '
        '"box12d_code": "", "box12d_amount": "", '
        '"box13_statutory_employee": false, '
        '"box13_retirement_plan": false, '
        '"box13_third_party_sick_pay": false, '
        '"box14_other": "", '
        '"box15_state": "", "box16_state_wages": "", '
        '"box17_state_income_tax": "", '
        '"box18_local_wages": "", "box19_local_income_tax": "", '
        '"box20_locality_name": "", '
        '"box15b_state": "", "box16b_state_wages": "", '
        '"box17b_state_income_tax": "", '
        '"box18b_local_wages": "", "box19b_local_income_tax": "", '
        '"box20b_locality_name": ""}\n\n'
        "FOR 1099-NEC (Nonemployee Compensation):\n"
        '{"form_type": "1099-NEC", "tax_year": "", '
        '"payer_name": "", "payer_address": "", '
        '"box1_nonemployee_compensation": "", '
        '"box2_direct_sales_indicator": false, '
        '"box4_federal_income_tax_withheld": "", '
        '"box5_state_tax_withheld": "", '
        '"box6_state": "", "box7_state_income": ""}\n\n'
        "FOR 1099-MISC (Miscellaneous Income):\n"
        '{"form_type": "1099-MISC", "tax_year": "", '
        '"payer_name": "", "payer_address": "", '
        '"box1_rents": "", "box2_royalties": "", '
        '"box3_other_income": "", '
        '"box4_federal_income_tax_withheld": "", '
        '"box5_fishing_boat_proceeds": "", '
        '"box6_medical_health_care_payments": "", '
        '"box7_direct_sales_indicator": false, '
        '"box8_substitute_payments": "", '
        '"box9_crop_insurance_proceeds": "", '
        '"box10_gross_proceeds_attorney": "", '
        '"box11_fish_purchased_resale": "", '
        '"box12_section_409a_deferrals": "", '
        '"box14_gross_proceeds_attorney": "", '
        '"box15_section_409a_income": "", '
        '"box16_state_tax_withheld": "", '
        '"box17_state": "", "box18_state_income": ""}\n\n'
        "FOR 1099-INT (Interest Income):\n"
        '{"form_type": "1099-INT", "tax_year": "", '
        '"payer_name": "", "payer_address": "", '
        '"box1_interest_income": "", '
        '"box2_early_withdrawal_penalty": "", '
        '"box3_us_savings_bond_treasury_interest": "", '
        '"box4_federal_income_tax_withheld": "", '
        '"box5_investment_expenses": "", '
        '"box6_foreign_tax_paid": "", '
        '"box7_foreign_country": "", '
        '"box8_tax_exempt_interest": "", '
        '"box9_specified_private_activity_bond_interest": "", '
        '"box10_market_discount": "", '
        '"box11_bond_premium": "", '
        '"box12_bond_premium_treasury_obligations": "", '
        '"box13_bond_premium_tax_exempt_bond": "", '
        '"box14_tax_exempt_bond_cusip": "", '
        '"box15_state": "", '
        '"box16_state_identification": "", '
        '"box17_state_tax_withheld": ""}\n\n'
        "FOR 1099-DIV (Dividends and Distributions):\n"
        '{"form_type": "1099-DIV", "tax_year": "", '
        '"payer_name": "", "payer_address": "", '
        '"box1a_total_ordinary_dividends": "", '
        '"box1b_qualified_dividends": "", '
        '"box2a_total_capital_gain": "", '
        '"box2b_unrecap_sec1250_gain": "", '
        '"box2c_section_1202_gain": "", '
        '"box2d_collectibles_gain": "", '
        '"box2e_section_897_ordinary_dividends": "", '
        '"box2f_section_897_capital_gain": "", '
        '"box3_nondividend_distributions": "", '
        '"box4_federal_income_tax_withheld": "", '
        '"box5_section_199a_dividends": "", '
        '"box6_investment_expenses": "", '
        '"box7_foreign_tax_paid": "", '
        '"box8_foreign_country": "", '
        '"box9_cash_liquidation_distributions": "", '
        '"box10_noncash_liquidation_distributions": "", '
        '"box11_fatca_filing_requirement": false, '
        '"box12_exempt_interest_dividends": "", '
        '"box13_specified_private_activity_bond": "", '
        '"box14_state": "", '
        '"box15_state_identification": "", '
        '"box16_state_tax_withheld": ""}\n\n'
        "FOR PAYSTUB / CHECK-STUB (employee paystub — often REG vs OT/OVT rows and YEAR-TO-DATE / YTD totals):\n"
        "Identify as PAYSTUB rather than Form W-2 when you see net pay per check, "
        "pay-period dates/number, hours worked, overtime lines, or YTD columns tied to THIS pay date.\n"
        '{"form_type": "PAYSTUB", "tax_year": "", '
        '"employee_name": "", "employer_name": "", '
        '"pay_period_start": "", "pay_period_end": "", "pay_date": "", '
        '"gross_pay_this_period": "", "net_pay_this_period": "", '
        '"regular_hours": "", "regular_pay": "", '
        '"overtime_hours": "", "overtime_pay": "", '
        '"has_overtime": false, '
        '"ytd_gross": "", "ytd_net": "", '
        '"ytd_federal_tax": "", "ytd_state_tax": "", '
        '"ytd_social_security": "", "ytd_medicare": ""}\n\n'
    )


def _extract_json_retry_on_timeout(
    filename: str,
    *,
    prompt: str,
    model: str,
    image_b64: str | None,
    timeout: int,
):
    """Call ``extract_json``; retry once after backoff on read/connect timeouts (busy Ollama host)."""
    from requests.exceptions import ConnectTimeout, ReadTimeout

    from llm import extract_json

    last_exc: BaseException | None = None
    for attempt in range(2):
        if attempt > 0:
            time.sleep(8)
        try:
            return extract_json(
                prompt,
                model=model,
                image_b64=image_b64,
                timeout=timeout,
            )
        except (ReadTimeout, ConnectTimeout) as exc:
            last_exc = exc
            logger.warning(
                "Ollama extract timed out (%s/2) for %s — %s",
                attempt + 1,
                filename,
                exc,
            )
    assert last_exc is not None
    raise last_exc


def _extract_fields(
    file_path: str, filename: str
) -> tuple[dict[str, Any] | None, str | None]:
    """DEBT-8: returns (fields, method) only.  Confidence is computed separately
    by _compute_confidence once the detected_type is known.  Callers must not
    use the presence of fields as a proxy for confidence — always call
    _compute_confidence(fields, detected_type) explicitly.
    """
    from form_store import _extract_pdf_text, _image_to_b64, _pdf_to_image_b64
    from utils import scrub_ssn_from_dict

    try:
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".pdf":
            pdf_text = _extract_pdf_text(file_path)
            if pdf_text:
                # Step 1: native regex parse — no LLM, instant for standard IRS forms.
                native = _native_extract_fields(pdf_text, filename)
                if native:
                    method = "native"
                    raw = native
                else:
                    # Step 2: text LLM — slower but handles non-standard layouts.
                    method = "text"
                    prompt = _build_text_prompt(pdf_text, filename)
                    logger.info(
                        "Extraction: native parse insufficient for %s — using text LLM",
                        filename,
                    )
                    raw = _extract_json_retry_on_timeout(
                        filename,
                        prompt=prompt,
                        model=OLLAMA_EXTRACT_MODEL_TEXT,
                        image_b64=None,
                        timeout=OLLAMA_EXTRACT_TIMEOUT_TEXT,
                    )
            else:
                # Step 3: vision LLM — scanned / image-only PDF, no embedded text.
                method = "vision"
                image_b64 = _pdf_to_image_b64(file_path)
                prompt = _build_vision_prompt()
                logger.info(
                    "Extraction: vision fallback (no usable embedded PDF text) for %s",
                    filename,
                )
                raw = _extract_json_retry_on_timeout(
                    filename,
                    prompt=prompt,
                    model=OLLAMA_EXTRACT_MODEL_VISION,
                    image_b64=image_b64,
                    timeout=OLLAMA_EXTRACT_TIMEOUT_VISION,
                )

        elif ext in (".jpg", ".jpeg", ".png"):
            from config import EXTRACTOR_VISION_ENABLED
            if not EXTRACTOR_VISION_ENABLED:
                logger.info(
                    "Skipped vision extraction for %s — image files are tagged manually",
                    filename,
                )
                return None, "image_skipped"
            method = "vision"
            image_b64 = _image_to_b64(file_path)
            prompt = _build_vision_prompt()
            logger.info("Extraction: vision (raster/image input) for %s", filename)
            raw = _extract_json_retry_on_timeout(
                filename,
                prompt=prompt,
                model=OLLAMA_EXTRACT_MODEL_VISION,
                image_b64=image_b64,
                timeout=OLLAMA_EXTRACT_TIMEOUT_VISION,
            )
        else:
            logger.info("Unsupported extension for extraction: %s", ext)
            return None, None

        if raw is None or not isinstance(raw, dict):
            return None, None

        scrubbed = scrub_ssn_from_dict(raw)

        allowed = _DOCUMENT_EXTRACT_ALLOWED_KEYS
        clean: dict[str, Any] = {}
        for k, v in scrubbed.items():
            if k not in allowed:
                continue
            if v is None:
                continue
            if k in FORM_INTEGER_COLUMNS:
                if isinstance(v, bool):
                    clean[k] = v
                elif isinstance(v, (int, float)):
                    clean[k] = bool(int(v))
                else:
                    sraw = str(v).strip().lower()
                    clean[k] = sraw in ("1", "true", "yes", "y", "on")
                continue
            sval = str(v).strip()
            if sval:
                clean[k] = sval

        return clean, method

    except Exception as e:
        logger.error("Field extraction failed for %s: %s", filename, e)
        return None, None


def _build_text_prompt(pdf_text: str, filename: str = "") -> str:
    fn_note = ""
    if (filename or "").strip():
        fn_note = f"Original filename: {filename.strip()}\n\n"
    return (
        _irs_form_extraction_block()
        + "EXTRACTION MODE: The following text was copied mechanically from the PDF's embedded text layer "
        "(not vision). Image-only scans may yield incomplete text.\n\n"
        + "Identify which form type this document is and return ONLY the JSON "
        "structure for that form type. Do not return multiple structures.\n\n"
        + fn_note
        + "Document text:\n"
        + pdf_text
    )


def _build_vision_prompt() -> str:
    return (
        _irs_form_extraction_block()
        + "EXTRACTION MODE — VISION (final fallback): There was no usable selectable text from this PDF, "
        "or the upload is already a raster image. Read layout from pixels only; identification numbers "
        "stay blank under the CRITICAL PRIVACY RULES above.\n\n"
        + "Examine the document image carefully. Read every box label and value. "
        "Identify which form type this document is and return ONLY the JSON "
        "structure for that form type. Do not return multiple structures. "
        "Return JSON only — no other text."
    )


# ── Native PDF field extractor — no LLM required ─────────────────────────────
#
# Tries regex/pattern matching on machine-extracted text before touching Ollama.
# Works well for ADP / Paychex / IRS-generated PDFs that embed text.
# Falls through to the text-LLM when < _NATIVE_PARSE_MIN_FIELDS data fields found.
# Vision-LLM path is unchanged — still used when pdfplumber/fitz find no text.

# Minimum non-metadata fields required to accept native parse result.
_NATIVE_PARSE_MIN_FIELDS = 3

# Money: bare number with optional commas and up to 2 decimal places.
_MONEY_PAT = _re.compile(r'(?<!\d)([\d,]{1,12}(?:\.\d{1,2})?)(?!\d)')

# SSN (XXX-XX-XXXX) / EIN (XX-XXXXXXX) — never extract, never store.
_ID_SKIP = _re.compile(r'^\d{3}-\d{2}-\d{4}$|^\d{2}-\d{7}$')

# Valid US state / territory abbreviations for W-2 box 15 validation.
_US_STATE_ABBR = frozenset(
    "AL AK AZ AR CA CO CT DE FL GA HI ID IL IN IA KS KY LA ME MD MA MI MN "
    "MS MO MT NE NV NH NJ NM NY NC ND OH OK OR PA RI SC SD TN TX UT VT VA "
    "WA WV WI WY DC PR VI GU AS MP".split()
)


def _nv_money(text: str, label_re: str, window: int = 150) -> str | None:
    """Return the first plausible monetary amount in `window` chars after `label_re`.

    Uses finditer so a bare single-digit (e.g. the '2' in '2 Federal income tax')
    does not block the real value on the next line.
    """
    m = _re.search(label_re, text, _re.I | _re.S)
    if not m:
        return None
    snippet = text[m.end(): m.end() + window]
    for vm in _MONEY_PAT.finditer(snippet):
        raw = vm.group(1).replace(",", "")
        # Skip bare integers ≤ 4 digits with no decimal (years, box numbers, zip codes)
        if len(raw.replace(".", "")) <= 4 and "." not in raw:
            continue
        try:
            if float(raw) >= 1_000_000_000:
                continue
        except ValueError:
            continue
        return raw
    return None


def _nv_line(text: str, label_re: str) -> str | None:
    """Return the first useful non-blank line after `label_re`, skipping SSN/EIN."""
    m = _re.search(label_re + r"[^\n]*\n", text, _re.I)
    if not m:
        return None
    for line in text[m.end():].splitlines():
        line = line.strip()
        if not line:
            continue
        if _ID_SKIP.match(line):
            continue
        if len(line) >= 3:
            return line[:80]
    return None


def _nv_year(text: str) -> str | None:
    """Extract the most-common tax year (2015-2029) from text."""
    hits = _re.findall(r"(?:^|\s)(20[1-2]\d)(?:\s|$)", text, _re.M)
    if hits:
        from collections import Counter
        return Counter(hits).most_common(1)[0][0]
    m = _re.search(r"\b(20[1-2]\d)\b", text)
    return m.group(1) if m else None


def _native_parse_w2(text: str) -> dict:
    f: dict = {}
    # Employer name: after box "c" label
    for pat in (r"[cC]\s*Employer.{0,8}name", r"Employer.{0,8}name"):
        v = _nv_line(text, pat)
        if v:
            f["employer_name"] = v
            break
    v = _nv_money(text, r"1\s+Wages[,\s]+tips")
    if v:
        f["box1_wages_tips_other"] = v
    v = _nv_money(text, r"2\s+Federal\s+income\s+tax\s+withheld")
    if v:
        f["box2_federal_income_tax_withheld"] = v
    v = _nv_money(text, r"3\s+Social\s+security\s+wages")
    if v:
        f["box3_social_security_wages"] = v
    v = _nv_money(text, r"4\s+Social\s+security\s+tax\s+withheld")
    if v:
        f["box4_social_security_tax_withheld"] = v
    v = _nv_money(text, r"5\s+Medicare\s+wages")
    if v:
        f["box5_medicare_wages_tips"] = v
    v = _nv_money(text, r"6\s+Medicare\s+tax\s+withheld")
    if v:
        f["box6_medicare_tax_withheld"] = v
    v = _nv_money(text, r"16\s+State\s+wages")
    if v:
        f["box16_state_wages"] = v
    v = _nv_money(text, r"17\s+State\s+income\s+tax")
    if v:
        f["box17_state_income_tax"] = v
    # State abbreviation: look on the line AFTER "15 State" or inline after whitespace,
    # but only accept known US state abbreviations to avoid matching "ER" from "Employer".
    for sm in _re.finditer(r"\b([A-Z]{2})\b", text):
        candidate = sm.group(1).upper()
        if candidate in _US_STATE_ABBR:
            # Confirm it appears near a "15" / "State" label
            nearby_start = max(0, sm.start() - 120)
            nearby = text[nearby_start: sm.start()]
            if _re.search(r"15\s+State|State.*ID", nearby, _re.I):
                f["box15_state"] = candidate
                break

    # Cross-validate: federal tax withheld (box2) must be less than gross wages (box1).
    # If equal or greater, the side-by-side PDF layout confused the parser — return
    # without these fields so the caller falls through to the text LLM.
    b1 = f.get("box1_wages_tips_other")
    b2 = f.get("box2_federal_income_tax_withheld")
    if b1 and b2:
        try:
            if float(b2) >= float(b1):
                f.pop("box1_wages_tips_other", None)
                f.pop("box2_federal_income_tax_withheld", None)
                f.pop("box3_social_security_wages", None)
                f.pop("box4_social_security_tax_withheld", None)
                f.pop("box5_medicare_wages_tips", None)
                f.pop("box6_medicare_tax_withheld", None)
        except ValueError:
            pass

    return f


def _native_parse_1099nec(text: str) -> dict:
    f: dict = {}
    v = _nv_line(text, r"PAYER.{0,6}[Ss]?\s*name")
    if v:
        f["payer_name"] = v
    v = _nv_money(text, r"1\s+Nonemployee\s+comp")
    if v:
        f["box1_nonemployee_compensation"] = v
    v = _nv_money(text, r"4\s+Federal\s+income\s+tax\s+withheld")
    if v:
        f["box4_federal_income_tax_withheld"] = v
    v = _nv_money(text, r"5\s+State\s+tax\s+withheld")
    if v:
        f["box5_state_tax_withheld"] = v
    sm = _re.search(r"6\s+State[^\n]{0,20}([A-Z]{2})", text, _re.I)
    if sm:
        f["box6_state"] = sm.group(1).upper()
    return f


def _native_parse_1099misc(text: str) -> dict:
    f: dict = {}
    v = _nv_line(text, r"PAYER.{0,6}[Ss]?\s*name")
    if v:
        f["payer_name"] = v
    v = _nv_money(text, r"1\s+Rents")
    if v:
        f["box1_rents"] = v
    v = _nv_money(text, r"2\s+Royalties")
    if v:
        f["box2_royalties"] = v
    v = _nv_money(text, r"3\s+Other\s+income")
    if v:
        f["box3_other_income"] = v
    v = _nv_money(text, r"4\s+Federal\s+income\s+tax\s+withheld")
    if v:
        f["box4_federal_income_tax_withheld"] = v
    return f


def _native_parse_1099int(text: str) -> dict:
    f: dict = {}
    v = _nv_line(text, r"PAYER.{0,6}[Ss]?\s*name")
    if v:
        f["payer_name"] = v
    v = _nv_money(text, r"1\s+Interest\s+income")
    if v:
        f["box1_interest_income"] = v
    v = _nv_money(text, r"4\s+Federal\s+income\s+tax\s+withheld")
    if v:
        f["box4_federal_income_tax_withheld"] = v
    v = _nv_money(text, r"8\s+Tax.exempt\s+interest")
    if v:
        f["box8_tax_exempt_interest"] = v
    return f


def _native_parse_1099div(text: str) -> dict:
    f: dict = {}
    v = _nv_line(text, r"PAYER.{0,6}[Ss]?\s*name")
    if v:
        f["payer_name"] = v
    v = _nv_money(text, r"1a\s+Total\s+ordinary\s+dividends")
    if v:
        f["box1a_total_ordinary_dividends"] = v
    v = _nv_money(text, r"1b\s+Qualified\s+dividends")
    if v:
        f["box1b_qualified_dividends"] = v
    v = _nv_money(text, r"2a\s+Total\s+capital\s+gain")
    if v:
        f["box2a_total_capital_gain"] = v
    v = _nv_money(text, r"4\s+Federal\s+income\s+tax\s+withheld")
    if v:
        f["box4_federal_income_tax_withheld"] = v
    return f


def _native_parse_paystub(text: str) -> dict:
    f: dict = {}
    # Employee name
    for pat in (r"Employee\s*(?:Name)?[:\s]+([A-Za-z]+(?: [A-Za-z]+)+)",
                r"Pay\s+To[:\s]+([A-Za-z]+(?: [A-Za-z]+)+)"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["employee_name"] = m.group(1).strip()[:60]
            break
    # Employer name
    for pat in (r"Company\s*(?:Name)?[:\s]+([^\n]{3,60})",
                r"Employer[:\s]+([^\n]{3,60})"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["employer_name"] = m.group(1).strip()[:60]
            break
    # Gross/Net pay this period
    for pat in (r"(?:Current\s+)?Gross\s+Pay[:\s$]*([\d,]+(?:\.\d{2})?)",
                r"Gross\s+Earnings[:\s$]*([\d,]+(?:\.\d{2})?)"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["gross_pay_this_period"] = m.group(1).replace(",", "")
            break
    for pat in (r"Net\s+Pay[:\s$]*([\d,]+(?:\.\d{2})?)",
                r"Net\s+Amount[:\s$]*([\d,]+(?:\.\d{2})?)"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["net_pay_this_period"] = m.group(1).replace(",", "")
            break
    # YTD gross / net
    for pat in (r"(?:YTD|Year.to.Date)\s+Gross[:\s$]*([\d,]+(?:\.\d{2})?)",
                r"Gross\s+YTD[:\s$]*([\d,]+(?:\.\d{2})?)"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["ytd_gross"] = m.group(1).replace(",", "")
            break
    for pat in (r"(?:YTD|Year.to.Date)\s+Net[:\s$]*([\d,]+(?:\.\d{2})?)",
                r"Net\s+YTD[:\s$]*([\d,]+(?:\.\d{2})?)"):
        m = _re.search(pat, text, _re.I)
        if m:
            f["ytd_net"] = m.group(1).replace(",", "")
            break
    # Overtime
    if _re.search(r"\bovertime\b|\bOT\s+hours\b", text, _re.I):
        f["has_overtime"] = True
        m = _re.search(r"Overtime\s+Hours?[:\s$]*([\d,]+(?:\.\d{2})?)", text, _re.I)
        if m:
            f["overtime_hours"] = m.group(1).replace(",", "")
        m = _re.search(r"Overtime\s+(?:Pay|Amount)[:\s$]*([\d,]+(?:\.\d{2})?)", text, _re.I)
        if m:
            f["overtime_pay"] = m.group(1).replace(",", "")
    # Pay period dates
    pp = _re.search(
        r"(?:Pay\s+)?Period[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})\s*(?:[-–]|to|thru)\s*(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})",
        text, _re.I,
    )
    if pp:
        f["pay_period_start"] = pp.group(1)
        f["pay_period_end"] = pp.group(2)
    pd_m = _re.search(r"(?:Pay|Check)\s+Date[:\s]+(\d{1,2}[/\-]\d{1,2}[/\-]\d{2,4})", text, _re.I)
    if pd_m:
        f["pay_date"] = pd_m.group(1)
    return f


def _native_extract_fields(text: str, filename: str = "") -> dict | None:
    """Parse IRS tax form fields from machine-extracted PDF text — no LLM required.

    Returns a fields dict using the same keys as the LLM extractor, or None if
    fewer than _NATIVE_PARSE_MIN_FIELDS data fields are found (caller falls through
    to the text LLM).

    Privacy: SSN / EIN / TIN lines are never matched or stored.
    """
    tl = text.lower()
    fn = (filename or "").lower()

    is_w2      = bool(_re.search(r"\bw[-\u2011\u2013]?2\b|wage and tax statement", tl) or _re.search(r"\bw2\b|\bw-2\b", fn))
    is_1099nec  = bool(_re.search(r"1099[-\u2013]?nec\b|nonemployee comp", tl))
    is_1099misc = bool(_re.search(r"1099[-\u2013]?misc\b|miscellaneous\s+income", tl))
    is_1099int  = bool(_re.search(r"1099[-\u2013]?int\b|interest income", tl))
    is_1099div  = bool(_re.search(r"1099[-\u2013]?div\b|dividends and distributions", tl))
    is_paystub  = bool(_re.search(
        r"pay\s*(?:stub|period|check)|check\s*stub|ytd\s+gross|year[- ]to[- ]date|earnings\s+statement",
        tl,
    ))

    if not any([is_w2, is_1099nec, is_1099misc, is_1099int, is_1099div, is_paystub]):
        return None  # unrecognised form — let LLM decide

    fields: dict = {}
    year = _nv_year(text)
    if year:
        fields["tax_year"] = year

    if is_w2:
        fields["form_type"] = "W-2"
        fields.update(_native_parse_w2(text))
    elif is_1099nec:
        fields["form_type"] = "1099-NEC"
        fields.update(_native_parse_1099nec(text))
    elif is_1099misc:
        fields["form_type"] = "1099-MISC"
        fields.update(_native_parse_1099misc(text))
    elif is_1099int:
        fields["form_type"] = "1099-INT"
        fields.update(_native_parse_1099int(text))
    elif is_1099div:
        fields["form_type"] = "1099-DIV"
        fields.update(_native_parse_1099div(text))
    elif is_paystub:
        fields["form_type"] = "PAYSTUB"
        fields.update(_native_parse_paystub(text))

    meta = {"form_type", "tax_year"}
    data_fields = {k: v for k, v in fields.items() if k not in meta and v not in (None, "", False)}
    if len(data_fields) < _NATIVE_PARSE_MIN_FIELDS:
        logger.debug(
            "Native parse: only %d data field(s) for %s — falling through to text LLM",
            len(data_fields),
            filename or "(unknown)",
        )
        return None

    logger.info(
        "Native parse succeeded for %s: form=%s fields=%d (no LLM used)",
        filename or "(unknown)",
        fields.get("form_type"),
        len(data_fields),
    )
    return fields


def _field_truthy(fields: dict, key: str) -> bool:
    v = fields.get(key)
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return bool(str(v).strip())


def _compute_confidence(fields: dict, detected_type: str | None) -> float:
    if not fields or not detected_type:
        return 0.0

    if detected_type == "paystub":
        score = 0.0
        if _field_truthy(fields, "employer_name") or _field_truthy(fields, "employee_name"):
            score += 0.28
        if _field_truthy(fields, "ytd_gross") or _field_truthy(fields, "ytd_net"):
            score += 0.32
        ot_line = (
            fields.get("has_overtime") is True
            or _field_truthy(fields, "overtime_hours")
            or _field_truthy(fields, "overtime_pay")
        )
        if ot_line:
            score += 0.35
        ft = str(fields.get("form_type") or "").strip().upper()
        ft_c = ft.replace("-", "").replace(" ", "").replace("/", "")
        if "PAYSTUB" in ft_c or "CHECKSTUB" in ft_c or ft in ("PAY STUB", "CHECK STUB"):
            score = min(score + 0.12, 1.0)
        return max(min(score, 1.0), 0.0)

    required_fields = {
        "w2_records": [
            "employer_name",
            "tax_year",
            "box1_wages_tips_other",
            "box2_federal_income_tax_withheld",
        ],
        "f1099_nec_records": [
            "payer_name",
            "tax_year",
            "box1_nonemployee_compensation",
        ],
        "f1099_misc_records": ["payer_name", "tax_year"],
        "f1099_int_records": [
            "payer_name",
            "tax_year",
            "box1_interest_income",
        ],
        "f1099_div_records": [
            "payer_name",
            "tax_year",
            "box1a_total_ordinary_dividends",
        ],
    }

    required = required_fields.get(detected_type, [])
    if not required:
        return 0.5

    found = 0
    for field in required:
        if _field_truthy(fields, field):
            found += 1

    confidence = found / len(required)

    ft = fields.get("form_type")
    if ft and str(ft).strip().lower() not in {"", "unknown"}:
        confidence = min(confidence + 0.1, 1.0)

    return confidence


def extraction_queue_counts() -> dict[str, int]:
    """Aggregates for health endpoints (SQLite)."""
    from datetime import datetime, timezone

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        counts: dict[str, int] = {
            "pending": 0,
            "processing": 0,
            "needs_review": 0,
            "failed": 0,
            "completed_today": 0,
            "skipped": 0,
        }
        rows = conn.execute(
            """
            SELECT status, COUNT(*) AS n
            FROM extraction_queue
            GROUP BY status
            """
        ).fetchall()
        for r in rows:
            key = str(r["status"])
            counts[key] = int(r["n"])

        ct = conn.execute(
            """
            SELECT COUNT(*) AS n FROM extraction_queue
            WHERE status = 'completed'
              AND processed_at IS NOT NULL
              AND SUBSTR(processed_at, 1, 10) = ?
            """,
            (today,),
        ).fetchone()
        counts["completed_today"] = int(ct["n"] if ct else 0)

        return counts
    finally:
        conn.close()


def extraction_queue_status_for_api() -> dict[str, int]:
    raw = extraction_queue_counts()
    return {
        "pending": int(raw.get("pending", 0)),
        "processing": int(raw.get("processing", 0)),
        "completed_today": int(raw.get("completed_today", 0)),
        "needs_review": int(raw.get("needs_review", 0)),
        "failed": int(raw.get("failed", 0)),
    }
