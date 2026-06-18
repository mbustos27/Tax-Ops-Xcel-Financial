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
    from ai_routes import _detect_form_type

    hint_table = _table_from_form_type_hint(fields.get("form_type"))
    if hint_table:
        return hint_table
    return _detect_form_type(doc_type_db, fields)


def _process_item(conn, item: dict) -> None:
    from ai_routes import (
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
    from ai_routes import _extract_pdf_text, _image_to_b64, _pdf_to_image_b64
    from utils import scrub_ssn_from_dict

    try:
        ext = os.path.splitext(file_path)[1].lower()

        if ext == ".pdf":
            pdf_text = _extract_pdf_text(file_path)
            if pdf_text:
                method = "text"
                prompt = _build_text_prompt(pdf_text, filename)
                raw = _extract_json_retry_on_timeout(
                    filename,
                    prompt=prompt,
                    model=OLLAMA_EXTRACT_MODEL_TEXT,
                    image_b64=None,
                    timeout=OLLAMA_EXTRACT_TIMEOUT_TEXT,
                )
            else:
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
