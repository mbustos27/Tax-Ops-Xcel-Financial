"""ACCOUNTING-3: Ollama vision receipt OCR.

``extract_receipt_data(image_path)`` sends the receipt image to the configured
Ollama vision model and returns a structured dict.  On partial failure the dict
still contains whatever was extracted plus an ``_error`` key.

Supported image formats: JPEG, PNG, WEBP, BMP.
PDFs are rendered to a single-page thumbnail before sending (reuses the
``_pdf_to_image_b64`` helper from ai_routes).

Return shape
------------
{
    "vendor":         str | None,
    "date":           str | None,   # ISO-8601 preferred, whatever the model returns
    "total_amount":   float | None,
    "payment_method": str | None,   # "cash", "credit", "debit", "check", …
    "line_items": [                 # may be empty list on failure
        {"description": str, "amount": float | None}
    ],
    "_raw":   str,                  # full LLM response text (for debug / ocr_raw column)
    "_error": str | None,           # set on failure; partial data may still be present
}
"""

from __future__ import annotations

import base64
import json
import logging
import os
import re

log = logging.getLogger(__name__)

_PROMPT = (
    "You are a receipt-parsing assistant.  Extract the following from the receipt image "
    "and return ONLY valid JSON with no explanation or markdown fences:\n"
    '{"vendor": "<store/restaurant name>", '
    '"date": "<YYYY-MM-DD if visible>", '
    '"total_amount": <number or null>, '
    '"payment_method": "<cash|credit|debit|check|other or null>", '
    '"line_items": [{"description": "<item>", "amount": <number or null>}]}\n'
    "If a field is not visible write null.  Do not guess amounts."
)


def _image_to_b64(path: str) -> str:
    """Return base64-encoded bytes for an image file."""
    with open(path, "rb") as fh:
        return base64.b64encode(fh.read()).decode("ascii")


def _to_b64(image_path: str) -> str:
    """Convert any supported receipt file to a base64 image string."""
    ext = os.path.splitext(image_path)[1].lower()
    if ext == ".pdf":
        try:
            from ai_routes import _pdf_to_image_b64
            return _pdf_to_image_b64(image_path)
        except Exception as exc:
            log.warning("PDF→image conversion failed for %s: %s", image_path, exc)
            raise
    return _image_to_b64(image_path)


def _parse_response(raw: str) -> dict:
    """Strip markdown fences and parse the JSON response from the LLM."""
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip(), flags=re.MULTILINE)
    cleaned = re.sub(r"\s*```$", "", cleaned, flags=re.MULTILINE).strip()
    # Find first JSON object in case the model added prose
    m = re.search(r"\{.*\}", cleaned, re.DOTALL)
    if m:
        cleaned = m.group(0)
    return json.loads(cleaned)


def extract_receipt_data(
    image_path: str,
    *,
    model: str | None = None,
    timeout: int | None = None,
) -> dict:
    """OCR a receipt image using Ollama vision.

    Returns a dict with keys: vendor, date, total_amount, payment_method,
    line_items, _raw, _error.  Never raises — sets ``_error`` on failure.
    """
    import config as _cfg

    use_model = model or _cfg.ACCOUNTING_VISION_MODEL
    use_timeout = timeout if timeout is not None else _cfg.ACCOUNTING_OCR_TIMEOUT

    result: dict = {
        "vendor": None,
        "date": None,
        "total_amount": None,
        "payment_method": None,
        "line_items": [],
        "_raw": "",
        "_error": None,
    }

    try:
        image_b64 = _to_b64(image_path)
    except Exception as exc:
        result["_error"] = f"image_load_failed: {exc}"
        log.error("receipt_ocr: cannot load %s — %s", image_path, exc)
        return result

    try:
        from llm import chat
        raw = chat(_PROMPT, model=use_model, image_b64=image_b64, timeout=use_timeout)
        result["_raw"] = raw
    except Exception as exc:
        result["_error"] = f"ollama_call_failed: {exc}"
        log.error("receipt_ocr: Ollama call failed for %s — %s", image_path, exc)
        return result

    try:
        parsed = _parse_response(raw)
    except Exception as exc:
        result["_error"] = f"json_parse_failed: {exc!r} raw={raw[:200]!r}"
        log.warning("receipt_ocr: JSON parse failed for %s — %s", image_path, exc)
        return result

    # Normalise and coerce types defensively.
    result["vendor"] = parsed.get("vendor") or None
    result["date"] = parsed.get("date") or None
    result["payment_method"] = parsed.get("payment_method") or None

    raw_total = parsed.get("total_amount")
    if raw_total is not None:
        try:
            result["total_amount"] = float(raw_total)
        except (TypeError, ValueError):
            pass

    raw_items = parsed.get("line_items") or []
    items = []
    if isinstance(raw_items, list):
        for item in raw_items:
            if not isinstance(item, dict):
                continue
            amt = item.get("amount")
            try:
                amt = float(amt) if amt is not None else None
            except (TypeError, ValueError):
                amt = None
            items.append({"description": str(item.get("description") or ""), "amount": amt})
    result["line_items"] = items

    log.info(
        "receipt_ocr: extracted vendor=%r total=%s items=%d from %s",
        result["vendor"],
        result["total_amount"],
        len(result["line_items"]),
        os.path.basename(image_path),
    )
    return result
