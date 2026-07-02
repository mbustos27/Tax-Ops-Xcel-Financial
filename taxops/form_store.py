"""form_store — helpers to persist extracted form data to typed SQLite tables.

Extracted from ai_routes.py when the AI chat feature was removed.
These utilities are used by the document extraction pipeline (extractor.py)
and the staff confirmation flow (routes/documents.py).
"""
from __future__ import annotations

import io
import logging
import os

from flask import current_app

from db import get_connection
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS
from utils import now

logger = logging.getLogger(__name__)

_ALLOWED_CLASSIFY_DOC_TYPES: frozenset[str] = frozenset(
    {"W-2", "1099", "prior_return", "government_id", "misc", "paystub", "unknown"}
)

_CLASSIFY_SUPPORTED_EXTS: frozenset[str] = frozenset({".pdf", ".jpg", ".jpeg", ".png"})

_ALLOWED_FORM_TABLES: frozenset[str] = frozenset(
    {
        "w2_records",
        "f1099_nec_records",
        "f1099_misc_records",
        "f1099_int_records",
        "f1099_div_records",
    }
)


def _form_table_to_doc_type(table_name: str | None) -> str:
    if not table_name:
        return "unknown"
    return {
        "w2_records": "W-2",
        "f1099_nec_records": "1099",
        "f1099_misc_records": "1099",
        "f1099_int_records": "1099",
        "f1099_div_records": "1099",
        "paystub": "paystub",
    }.get(table_name, "unknown")


def _apply_extraction_doc_tag(
    conn,
    *,
    doc_id: int,
    return_id: int,
    doc_tag: str,
) -> int:
    """After a successful typed form save — set doc_type only when unset or unknown.

    Returns the number of rows updated.
    """
    if doc_tag == "unknown":
        return 0
    cur = conn.execute(
        """
        UPDATE return_documents
        SET doc_type = ?
        WHERE id = ? AND return_id = ? AND is_deleted = 0
          AND (
               doc_type IS NULL
               OR TRIM(doc_type) = ''
               OR LOWER(TRIM(doc_type)) = 'unknown'
          )
        """,
        (doc_tag, doc_id, return_id),
    )
    return int(cur.rowcount or 0)


def _coerce_sql_integer_field(val) -> int:
    if isinstance(val, bool):
        return 1 if val else 0
    if isinstance(val, (int, float)):
        return 1 if int(val) != 0 else 0
    s = str(val or "").strip().lower()
    if s in ("1", "true", "yes", "y", "on"):
        return 1
    return 0


def _save_form_data(conn, table_name: str, return_id: int, doc_id: int, fields: dict) -> bool:
    """Save extracted form data to the correct typed table.

    Never stores SSN or identification-number fields.
    Text amounts stay TEXT; checkbox-style fields are INTEGER 0/1.
    Never raises — logs errors and returns False on failure.

    FORMS-3: INSERT uses only FORM_TABLE_INSERT_COLUMNS (canonical IRS box_* names).
    Legacy mirrored DDL columns on DOC-7 form tables remain NULL for newly extracted rows.
    """
    if table_name not in _ALLOWED_FORM_TABLES:
        return False

    ridok = conn.execute("SELECT 1 FROM returns WHERE id = ?", (return_id,)).fetchone()
    if not ridok:
        return False

    doc_row = conn.execute(
        """
        SELECT 1 FROM return_documents
        WHERE id = ? AND return_id = ? AND is_deleted = 0
        """,
        (doc_id, return_id),
    ).fetchone()
    if not doc_row:
        return False

    allowed_cols = FORM_TABLE_INSERT_COLUMNS.get(table_name, [])
    if not allowed_cols:
        return False

    fd = dict(fields)

    cols = ["return_id", "doc_id", "source", "created_at"]
    vals: list = [return_id, doc_id, "extracted", now()]

    for col in allowed_cols:
        if col not in fd:
            continue
        val = fd[col]
        if val is None:
            continue
        if col in FORM_INTEGER_COLUMNS:
            cols.append(col)
            vals.append(_coerce_sql_integer_field(val))
            continue
        sval = str(val).strip()
        if sval:
            cols.append(col)
            vals.append(sval)

    if len(cols) <= 4:
        return False

    placeholders = ", ".join(["?" for _ in vals])
    col_names = ", ".join(cols)

    try:
        conn.execute(
            f"INSERT INTO {table_name} ({col_names}) VALUES ({placeholders})",
            vals,
        )
        return True
    except Exception as e:
        current_app.logger.error("Form data save failed %s: %s", table_name, e)
        return False


# ---------------------------------------------------------------------------
# Document classification helpers (used by extractor.py and mail_watcher.py)
# ---------------------------------------------------------------------------

def _extract_field_truthy(fields: dict, key: str) -> bool:
    v = fields.get(key)
    if v is None:
        return False
    if isinstance(v, bool):
        return v
    return bool(str(v).strip())


def _likely_paystub_overtime_ytd(fields: dict) -> bool:
    """Heuristic: YTD + overtime on a check stub, without W-2 box fields filled."""
    if _extract_field_truthy(fields, "box1_wages_tips_other") or _extract_field_truthy(
        fields, "box3_social_security_wages"
    ):
        return False
    ytd = any(
        _extract_field_truthy(fields, k)
        for k in (
            "ytd_gross", "ytd_net", "ytd_federal_tax", "ytd_state_tax",
            "ytd_social_security", "ytd_medicare",
        )
    )
    ot = (
        fields.get("has_overtime") is True
        or _extract_field_truthy(fields, "overtime_hours")
        or _extract_field_truthy(fields, "overtime_pay")
    )
    who = _extract_field_truthy(fields, "employer_name") or _extract_field_truthy(
        fields, "employee_name"
    )
    stub_ctx = (
        _extract_field_truthy(fields, "pay_date")
        or _extract_field_truthy(fields, "pay_period_start")
        or _extract_field_truthy(fields, "gross_pay_this_period")
        or _extract_field_truthy(fields, "net_pay_this_period")
    )
    return bool(ytd and ot and who and stub_ctx)


def _detect_form_type(doc_type: str | None, fields: dict) -> str | None:
    """Determine which form table to save extracted data to.

    Returns table name string or None if form type is unknown.
    """
    dt = (doc_type or "").strip()
    tagged = frozenset({"prior_return", "government_id", "misc"})
    if dt in tagged:
        return None

    form_type_raw = str(fields.get("form_type", "") or "").upper().strip()
    ft_compact = form_type_raw.replace("-", "").replace(" ", "").replace("/", "")
    if ft_compact in ("PAYSTUB", "CHECKSTUB"):
        return "paystub"
    form_type_map = {
        "W-2": "w2_records", "W2": "w2_records",
        "1099-NEC": "f1099_nec_records", "1099NEC": "f1099_nec_records",
        "1099-MISC": "f1099_misc_records", "1099MISC": "f1099_misc_records",
        "1099-INT": "f1099_int_records", "1099INT": "f1099_int_records",
        "1099-DIV": "f1099_div_records", "1099DIV": "f1099_div_records",
    }
    if form_type_raw in form_type_map:
        return form_type_map[form_type_raw]

    doc_type_map = {"W-2": "w2_records", "1099": "f1099_nec_records", "paystub": "paystub"}
    if dt in doc_type_map:
        return doc_type_map[dt]

    if _likely_paystub_overtime_ytd(fields):
        return "paystub"

    if fields.get("box1_wages_tips_other") or fields.get("box3_social_security_wages"):
        return "w2_records"
    if fields.get("box1_nonemployee_compensation"):
        return "f1099_nec_records"
    if fields.get("box1_interest_income"):
        return "f1099_int_records"
    if fields.get("box1a_total_ordinary_dividends"):
        return "f1099_div_records"
    if fields.get("box1_rents") or fields.get("box2_royalties") or fields.get("box3_other_income"):
        return "f1099_misc_records"

    employer = str(fields.get("employer_name", "") or "").strip().lower()
    wages = str(fields.get("box1_wages_tips_other") or "").strip()
    if wages and employer:
        return "w2_records"

    payer = str(fields.get("payer_name", "") or "").strip()
    nonemployee = fields.get("box1_nonemployee_compensation", "")
    if payer and str(nonemployee or "").strip():
        return "f1099_nec_records"

    return None


def _classify_document_using_row(conn, row, *, only_if_still_unknown: bool) -> dict:
    """Run extraction + form-type detection and optionally update return_documents.doc_type.

    Never raises. Does not commit (caller must commit).
    """
    doc_id = row["id"]
    out: dict = {"doc_type": "unknown", "doc_id": doc_id}
    file_path = row["file_path"]
    filename = (row["filename"] or os.path.basename(file_path or "") or "document").strip()

    if not file_path or not isinstance(file_path, str):
        return out
    if not os.path.isfile(file_path):
        return out

    ext = os.path.splitext(file_path)[1].lower()
    if ext not in _CLASSIFY_SUPPORTED_EXTS:
        return out

    fields = None
    try:
        from extractor import _extract_fields
        tup = _extract_fields(file_path, filename)
        if tup and tup[0]:
            fields = tup[0]
    except Exception as e:
        logger.info("Classify extract skipped for doc %s: %s", doc_id, e)

    if not fields:
        return out

    doc_type_db = (row["doc_type"] or "unknown").strip()
    table = _detect_form_type(doc_type_db, fields)
    tag = _form_table_to_doc_type(table)
    if tag not in _ALLOWED_CLASSIFY_DOC_TYPES:
        tag = "unknown"
    out["doc_type"] = tag

    if tag == "unknown":
        return out

    if only_if_still_unknown:
        conn.execute(
            """
            UPDATE return_documents SET doc_type = ?
            WHERE id = ? AND is_deleted = 0
              AND (doc_type IS NULL OR TRIM(doc_type) = '' OR LOWER(TRIM(doc_type)) = 'unknown')
            """,
            (tag, doc_id),
        )
    else:
        conn.execute(
            "UPDATE return_documents SET doc_type = ? WHERE id = ? AND is_deleted = 0",
            (tag, doc_id),
        )
    return out


def _classify_document(doc_id: int, *, only_if_still_unknown: bool = True) -> dict:
    """DOC-5 — auto doc_type from LLM fields. Safe for background threads (own connection).

    Does not raise; returns {doc_type, doc_id}.
    Staff-tagged types are preserved when only_if_still_unknown is True.
    """
    conn = get_connection()
    try:
        row = conn.execute(
            """
            SELECT id, return_id, doc_type, file_path, filename
            FROM return_documents
            WHERE id = ? AND is_deleted = 0
            """,
            (doc_id,),
        ).fetchone()
        if not row:
            return {"doc_type": "unknown", "doc_id": doc_id}
        out = _classify_document_using_row(conn, row, only_if_still_unknown=only_if_still_unknown)
        conn.commit()
        return out
    except Exception as e:
        logger.error("Classify failed for doc %s: %s", doc_id, e)
        try:
            conn.rollback()
        except Exception:
            pass
        return {"doc_type": "unknown", "doc_id": doc_id}
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# PDF / image utilities (used by services/receipt_ocr.py)
# ---------------------------------------------------------------------------

def _pdf_to_image_b64(file_path: str) -> str:
    """Convert first page of PDF to base64 JPEG for vision model.

    Uses PyMuPDF (no Poppler/Ghostscript — works on Windows without extra PATH setup).
    Raises ValueError if conversion fails.
    """
    import base64

    try:
        import fitz  # PyMuPDF
    except ImportError:
        raise ValueError("PDF rasterizer not installed — run: pip install pymupdf") from None

    try:
        from PIL import Image
    except ImportError:
        raise ValueError("Pillow required — pip install Pillow") from None

    try:
        with fitz.open(file_path) as doc:
            if doc.page_count < 1:
                raise ValueError("PDF has no pages")
            page = doc.load_page(0)
            zoom = 200 / 72
            mat = fitz.Matrix(zoom, zoom)
            pix = page.get_pixmap(matrix=mat, alpha=False)
            mode = "RGB" if pix.n == 3 else "RGBA"
            img = Image.frombytes(mode, [pix.width, pix.height], pix.samples)
            buffer = io.BytesIO()
            if img.mode != "RGB":
                img = img.convert("RGB")
            img.save(buffer, format="JPEG", quality=85)
            buffer.seek(0)
            return base64.b64encode(buffer.read()).decode("utf-8")
    except ValueError:
        raise
    except Exception as e:
        raise ValueError(f"PDF conversion failed: {e}") from e


def _extract_pdf_text(file_path: str) -> str | None:
    """Extract text from a PDF for the text-first LLM path (DOC-4).

    Tries pdfplumber first, then PyMuPDF. Returns None when both fail or produce
    fewer than 50 characters, so callers fall back to the vision model.
    Never raises.
    """
    max_chars = 3000
    min_chars = 50
    candidates: list[str] = []

    try:
        import pdfplumber
        with pdfplumber.open(file_path) as pdf:
            text_parts: list[str] = []
            for page in pdf.pages[:3]:
                text = page.extract_text()
                if text:
                    text_parts.append(text.strip())
            joined = "\n".join(text_parts).strip()
            if joined:
                candidates.append(joined)
    except Exception as e:
        logger.info("pdfplumber PDF text extraction failed: %s", e)

    try:
        import fitz
        with fitz.open(file_path) as doc:
            text_parts = []
            for i in range(min(3, doc.page_count)):
                t = doc.load_page(i).get_text()
                if t:
                    text_parts.append(t.strip())
            joined = "\n".join(text_parts).strip()
            if joined:
                candidates.append(joined)
    except Exception as e:
        logger.info("PyMuPDF PDF text extraction failed: %s", e)

    if not candidates:
        return None
    best = max(candidates, key=len).strip()
    if len(best) < min_chars:
        return None
    return best[:max_chars]


def _image_to_b64(file_path: str) -> str:
    import base64
    with open(file_path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")
