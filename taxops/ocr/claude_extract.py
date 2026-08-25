"""Claude vision extraction for scan-agent documents (PunchBridge-style).

Cost control:
  - ``ocr_extraction_cache`` keyed on sha256(image_bytes) + system prompt hash
  - Model tiering: Haiku → Sonnet → Opus, escalate only below confidence thresholds

Only used when ``return_documents.source == 'scan_agent'``. Email / walk-in
keep the existing Ollama path in extractor.py.
"""
from __future__ import annotations

import hashlib
import json
import logging
import os
from typing import Any

logger = logging.getLogger(__name__)


def _sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _cache_key(image_sha256: str, system: str) -> str:
    """Composite key so a prompt change invalidates cached answers."""
    prompt_hash = hashlib.sha256(system.encode("utf-8")).hexdigest()[:16]
    return hashlib.sha256(f"{image_sha256}:{prompt_hash}".encode("utf-8")).hexdigest()


def _system_prompt() -> str:
    from extractor import _irs_form_extraction_block

    return (
        _irs_form_extraction_block()
        + "EXTRACTION MODE — CLAUDE VISION (scan agent):\n"
        + "Read the scanned tax document image carefully. "
        "Identify form type and return ONLY the JSON for that form type.\n"
        "Also include a top-level numeric field "
        '"_confidence" between 0.0 and 1.0 reflecting how sure you are '
        "that the extracted box values are correct "
        "(0.9+ = clear print; below 0.75 = blurry/handwritten/ambiguous).\n"
        "Return JSON only — no markdown fences, no commentary."
    )


def _file_to_page_image(file_path: str) -> tuple[bytes, str]:
    """Return (image_bytes, media_type) for the first page / raster file."""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in (".jpg", ".jpeg"):
        with open(file_path, "rb") as fh:
            return fh.read(), "image/jpeg"
    if ext == ".png":
        with open(file_path, "rb") as fh:
            return fh.read(), "image/png"
    if ext == ".pdf":
        try:
            import fitz  # pymupdf
        except ImportError as exc:
            raise RuntimeError("pymupdf required to rasterize scanned PDFs") from exc
        doc = fitz.open(file_path)
        try:
            if doc.page_count < 1:
                raise RuntimeError("PDF has no pages")
            page = doc.load_page(0)
            pix = page.get_pixmap(dpi=200)
            return pix.tobytes("png"), "image/png"
        finally:
            doc.close()
    raise RuntimeError(f"Unsupported extension for Claude OCR: {ext}")


def _cache_get(conn, key: str) -> dict | None:
    row = conn.execute(
        "SELECT fields_json, confidence, model FROM ocr_extraction_cache WHERE cache_key = ?",
        (key,),
    ).fetchone()
    if not row:
        return None
    try:
        fields = json.loads(row["fields_json"] or "{}")
    except json.JSONDecodeError:
        return None
    if not isinstance(fields, dict):
        return None
    return {
        "fields": fields,
        "confidence": row["confidence"],
        "model": row["model"],
    }


def _cache_put(
    conn,
    key: str,
    image_sha256: str,
    fields: dict,
    confidence: float | None,
    model: str,
) -> None:
    from utils import now as get_now

    conn.execute(
        """
        INSERT OR REPLACE INTO ocr_extraction_cache
          (cache_key, image_sha256, fields_json, confidence, model, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            key,
            image_sha256,
            json.dumps(fields, ensure_ascii=False),
            confidence,
            model,
            get_now(),
        ),
    )


def _confidence_of(payload: dict) -> float | None:
    raw = payload.get("_confidence")
    if raw is None:
        raw = payload.get("confidence")
    try:
        return float(raw)
    except (TypeError, ValueError):
        return None


def _strip_meta(payload: dict) -> dict:
    return {k: v for k, v in payload.items() if not str(k).startswith("_")}


def extract_scan_document_claude(
    file_path: str,
    conn,
    *,
    filename: str = "",
) -> tuple[dict[str, Any] | None, str, dict[str, Any]]:
    """Run Claude tiered vision OCR for a scan-agent document.

    Returns (fields, method, meta) where method is e.g. ``claude_sonnet``
    or ``claude_cache``, and meta includes cache_hit / model / confidence.
    """
    from config import (
        CLAUDE_OCR_ESCALATE_THRESHOLD,
        CLAUDE_OCR_HARD_REREAD_THRESHOLD,
        CLAUDE_OCR_MODEL_FAST,
        CLAUDE_OCR_MODEL_HARD,
        CLAUDE_OCR_MODEL_READ,
    )
    from ocr.vision_client import call_vision

    image_bytes, media_type = _file_to_page_image(file_path)
    image_sha = _sha256_bytes(image_bytes)
    system = _system_prompt()
    key = _cache_key(image_sha, system)

    meta: dict[str, Any] = {
        "image_sha256": image_sha,
        "cache_hit": False,
        "model": None,
        "confidence": None,
        "api_calls": 0,
    }

    cached = _cache_get(conn, key)
    if cached is not None:
        meta["cache_hit"] = True
        meta["model"] = cached["model"]
        meta["confidence"] = cached["confidence"]
        logger.info(
            "Claude OCR cache HIT for %s (model=%s)",
            filename or file_path,
            cached["model"],
        )
        return cached["fields"], "claude_cache", meta

    best_fields: dict | None = None
    best_conf: float | None = None
    best_model: str | None = None

    def _run(model: str) -> None:
        nonlocal best_fields, best_conf, best_model
        logger.info(
            "Claude OCR calling %s for %s (prev_conf=%s)",
            model,
            filename or file_path,
            best_conf,
        )
        payload = call_vision(
            image_bytes,
            model,
            system,
            media_type=media_type,
        )
        meta["api_calls"] = int(meta["api_calls"]) + 1
        if not isinstance(payload, dict):
            return
        conf = _confidence_of(payload)
        fields = _strip_meta(payload)
        best_fields = fields
        best_conf = conf
        best_model = model
        meta["model"] = model
        meta["confidence"] = conf

    try:
        _run(CLAUDE_OCR_MODEL_FAST)
    except Exception as exc:
        logger.warning("Claude OCR haiku failed for %s: %s", filename, exc)
        raise

    if best_conf is None or best_conf < CLAUDE_OCR_ESCALATE_THRESHOLD:
        try:
            _run(CLAUDE_OCR_MODEL_READ)
        except Exception as exc:
            logger.warning("Claude OCR sonnet failed for %s: %s", filename, exc)
            if best_fields is None:
                raise

    if best_conf is None or best_conf < CLAUDE_OCR_HARD_REREAD_THRESHOLD:
        # Skip if we already used HARD somehow; otherwise escalate from sonnet/haiku.
        if best_model != CLAUDE_OCR_MODEL_HARD:
            try:
                _run(CLAUDE_OCR_MODEL_HARD)
            except Exception as exc:
                logger.warning("Claude OCR opus failed for %s: %s", filename, exc)
                if best_fields is None:
                    raise

    if not best_fields:
        return None, "claude_failed", meta

    _cache_put(conn, key, image_sha, best_fields, best_conf, best_model or "")
    if best_model == CLAUDE_OCR_MODEL_FAST:
        method = "claude_haiku"
    elif best_model == CLAUDE_OCR_MODEL_READ:
        method = "claude_sonnet"
    elif best_model == CLAUDE_OCR_MODEL_HARD:
        method = "claude_opus"
    else:
        method = "claude"

    return best_fields, method, meta
