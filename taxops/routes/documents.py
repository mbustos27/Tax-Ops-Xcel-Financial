"""DEBT-1: documents Blueprint — routes extracted from app.py.

Covers:
  POST   /return/<id>/documents/upload
  POST   /return/<id>/documents/bulk-upload          (DOC-HARD-7)
  GET    /return/<id>/documents
  GET    /return/<id>/documents/<doc_id>/view
  POST   /return/<id>/documents/<doc_id>/delete
  POST   /return/<id>/documents/<doc_id>/tag
  POST   /return/<id>/documents/<doc_id>/sync-drake
  POST   /return/<id>/documents/<doc_id>/confirm-extraction
"""
from __future__ import annotations

import contextlib
import hashlib
import json
import logging
import os
import shutil
import threading

from flask import Blueprint, abort, current_app, jsonify, request, send_file, session

import config as _config
from auth import login_required
from db import get_connection
from utils import (
    _enqueue_extraction,
    get_drake_documents_path,
    get_return_documents_path,
    now,
    sanitize_filename,
    scrub_ssn_from_dict,
)
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS

log = logging.getLogger(__name__)

documents_bp = Blueprint("documents", __name__)

_ALLOWED_RETURN_DOC_TYPES = frozenset(
    {"W-2", "1099", "paystub", "prior_return", "government_id", "misc", "receipt", "unknown"}
)
_ALLOWED_RETURN_DOC_EXTENSIONS = frozenset({".jpg", ".jpeg", ".png", ".pdf"})

_FORM_DATA_SQL_TABLES = frozenset(
    {"w2_records", "f1099_nec_records", "f1099_misc_records", "f1099_int_records", "f1099_div_records"}
)
_FORM_DATA_UPDATE_FIELDS: dict[str, frozenset[str]] = {
    tbl: frozenset(cols) for tbl, cols in FORM_TABLE_INSERT_COLUMNS.items()
}


def _sha256_file(path: str) -> str:
    """DOC-HARD-4: compute SHA-256 hex digest of a file on disk."""
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _enqueue_receipt(doc_id: int, image_path: str) -> None:
    """ACCOUNTING-10: auto-enqueue a return document with doc_type='receipt' for OCR."""
    from datetime import datetime, timezone

    now_ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    try:
        with contextlib.closing(get_connection()) as conn:
            conn.execute(
                """
                INSERT INTO receipt_queue
                  (return_document_id, image_path, original_filename, status, attempts, created_at)
                SELECT ?, ?, original_filename, 'pending', 0, ?
                FROM return_documents WHERE id = ?
                """,
                (doc_id, image_path, now_ts, doc_id),
            )
            conn.commit()
        from accounting_worker import _notify_receipt_worker
        _notify_receipt_worker()
    except Exception as exc:
        log.warning("_enqueue_receipt: failed for doc_id=%d — %s", doc_id, exc)


def _intake_audit(
    *,
    user_id: str | None,
    action: str,
    entity_id: str | None,
    after: dict,
    ip_address: str | None,
) -> None:
    """DOC-HARD-5: emit a non-blocking audit log entry for document intake events."""
    try:
        from audit_service import _enqueue_write
        _enqueue_write(
            user_id=user_id,
            action=action,
            entity_type="return_document",
            entity_id=entity_id,
            before=None,
            after=after,
            ip_address=ip_address,
            http_status=200,
        )
    except Exception as exc:
        log.warning("intake_audit write failed: %s", exc)


def _parse_form_update_value(field: str, raw_val) -> object:
    if field in FORM_INTEGER_COLUMNS:
        if isinstance(raw_val, bool):
            return 1 if raw_val else 0
        s = str(raw_val or "").strip().lower()
        return 1 if s in ("1", "true", "yes", "y", "on") else 0
    if raw_val is None:
        return ""
    return str(raw_val).strip()


@documents_bp.route("/return/<int:return_id>/documents/upload", methods=["POST"])
@login_required
def return_documents_upload(return_id: int):
    conn = get_connection()
    full_path: str | None = None
    try:
        exists = conn.execute("SELECT id FROM returns WHERE id = ?", (return_id,)).fetchone()
        if not exists:
            return jsonify({"error": "Return not found"}), 404

        if "document" not in request.files:
            return jsonify({"error": "No file uploaded"}), 400

        upload = request.files["document"]
        if not upload or not upload.filename:
            return jsonify({"error": "No file uploaded"}), 400

        original_filename = upload.filename
        ext = os.path.splitext(original_filename)[1].lower()
        if ext not in _ALLOWED_RETURN_DOC_EXTENSIONS:
            return jsonify({"error": "Unsupported file type. Use jpg, png, or pdf"}), 400

        folder = get_return_documents_path(return_id)
        sanitized = sanitize_filename(original_filename)
        stem, ext_part = os.path.splitext(sanitized)
        candidate = sanitized
        counter = 1
        while os.path.exists(os.path.join(folder, candidate)):
            candidate = f"{stem}_{counter}{ext_part}"
            counter += 1

        full_path = os.path.abspath(os.path.join(folder, candidate))

        raw_doc_type = (request.form.get("doc_type") or "unknown").strip()
        doc_type = raw_doc_type if raw_doc_type in _ALLOWED_RETURN_DOC_TYPES else "unknown"

        uploaded_at = now()
        uploaded_by = session.get("username")

        try:
            upload.save(full_path)
        except OSError:
            return jsonify({"error": "Failed to save file"}), 500

        file_size_bytes = os.path.getsize(full_path)

        # DOC-HARD-4: compute SHA-256 and check for duplicates on this return.
        file_hash: str | None = None
        duplicate_warning: str | None = None
        try:
            file_hash = _sha256_file(full_path)
            dup = conn.execute(
                """
                SELECT id, filename FROM return_documents
                WHERE return_id = ? AND file_hash = ? AND is_deleted = 0
                LIMIT 1
                """,
                (return_id, file_hash),
            ).fetchone()
            if dup:
                duplicate_warning = (
                    f"A file with identical content already exists on this return "
                    f"('{dup['filename']}', doc #{dup['id']}). Saved anyway."
                )
                log.info(
                    "Duplicate upload detected for return %d: new=%s matches existing doc %d (%s)",
                    return_id, candidate, dup["id"], dup["filename"],
                )
        except OSError as exc:
            log.warning("Could not compute file hash for %s: %s", full_path, exc)

        try:
            cur = conn.execute(
                """
                INSERT INTO return_documents (
                  return_id, filename, original_filename, doc_type, source,
                  file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                """,
                (
                    return_id, candidate, original_filename, doc_type, "walk_in",
                    full_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, None,
                ),
            )
            doc_id = cur.lastrowid
            conn.commit()
            _enqueue_extraction(doc_id, return_id)

            # ACCOUNTING-10: auto-enqueue receipts to receipt_queue (still requires staff review).
            if doc_type == "receipt":
                _enqueue_receipt(doc_id, dest_path)

            # DOC-HARD-5: emit intake audit trail entry.
            _intake_audit(
                user_id=uploaded_by,
                action="document_upload",
                entity_id=str(doc_id),
                after={
                    "doc_id": doc_id,
                    "return_id": return_id,
                    "filename": candidate,
                    "original_filename": original_filename,
                    "doc_type": doc_type,
                    "file_size_bytes": file_size_bytes,
                    "duplicate_warning": duplicate_warning,
                },
                ip_address=request.remote_addr,
            )

            app_obj = current_app._get_current_object()

            def _bg_classify():
                try:
                    with app_obj.app_context():
                        from ai_routes import _classify_document
                        _classify_document(doc_id, only_if_still_unknown=True)
                except Exception as exc:
                    log.error("Background classify failed for doc %s: %s", doc_id, exc)

            threading.Thread(target=_bg_classify, daemon=True).start()
        except Exception:
            conn.rollback()
            if full_path and os.path.isfile(full_path):
                try:
                    os.remove(full_path)
                except OSError:
                    pass
            return jsonify({"error": "Could not record document"}), 500

        resp = scrub_ssn_from_dict(
            {
                "success": True,
                "doc_id": doc_id,
                "filename": candidate,
                "doc_type": doc_type,
                "uploaded_at": uploaded_at,
            }
        )
        if duplicate_warning:
            resp["duplicate_warning"] = duplicate_warning
        return jsonify(resp)
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/bulk-upload", methods=["POST"])
@login_required
def return_documents_bulk_upload(return_id: int):
    """DOC-HARD-7: accept multiple files in one request and queue each for extraction.

    Expects multipart/form-data with any number of 'documents' file fields and
    an optional 'doc_type' text field (applied to all files in the batch).
    Returns ``{"results": [...]}`` where each item mirrors the single-upload response
    plus a ``filename_original`` key for client-side reconciliation.
    """
    conn = get_connection()
    try:
        exists = conn.execute("SELECT id FROM returns WHERE id = ?", (return_id,)).fetchone()
        if not exists:
            return jsonify({"error": "Return not found"}), 404

        uploads = request.files.getlist("documents")
        if not uploads:
            return jsonify({"error": "No files uploaded"}), 400

        raw_doc_type = (request.form.get("doc_type") or "unknown").strip()
        doc_type = raw_doc_type if raw_doc_type in _ALLOWED_RETURN_DOC_TYPES else "unknown"
        uploaded_by = session.get("username")
        folder = get_return_documents_path(return_id)
        app_obj = current_app._get_current_object()
        results = []

        for upload in uploads:
            if not upload or not upload.filename:
                results.append({"success": False, "error": "Empty file slot"})
                continue

            original_filename = upload.filename
            ext = os.path.splitext(original_filename)[1].lower()
            if ext not in _ALLOWED_RETURN_DOC_EXTENSIONS:
                results.append({
                    "success": False,
                    "filename_original": original_filename,
                    "error": f"Unsupported file type: {ext}",
                })
                continue

            sanitized = sanitize_filename(original_filename)
            stem, ext_part = os.path.splitext(sanitized)
            candidate = sanitized
            counter = 1
            while os.path.exists(os.path.join(folder, candidate)):
                candidate = f"{stem}_{counter}{ext_part}"
                counter += 1

            full_path = os.path.abspath(os.path.join(folder, candidate))
            uploaded_at = now()

            try:
                upload.save(full_path)
            except OSError as exc:
                results.append({
                    "success": False,
                    "filename_original": original_filename,
                    "error": f"Failed to save file: {exc}",
                })
                continue

            file_size_bytes = os.path.getsize(full_path)

            # DOC-HARD-4: hash each uploaded file.
            file_hash: str | None = None
            duplicate_warning: str | None = None
            try:
                file_hash = _sha256_file(full_path)
                dup = conn.execute(
                    "SELECT id, filename FROM return_documents "
                    "WHERE return_id = ? AND file_hash = ? AND is_deleted = 0 LIMIT 1",
                    (return_id, file_hash),
                ).fetchone()
                if dup:
                    duplicate_warning = (
                        f"Identical content already attached ('{dup['filename']}', "
                        f"doc #{dup['id']}). Saved anyway."
                    )
            except OSError:
                pass

            try:
                cur = conn.execute(
                    """
                    INSERT INTO return_documents (
                      return_id, filename, original_filename, doc_type, source,
                      file_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, notes, is_deleted
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                    """,
                    (
                        return_id, candidate, original_filename, doc_type, "walk_in",
                        full_path, file_size_bytes, file_hash, uploaded_by, uploaded_at, None,
                    ),
                )
                doc_id = cur.lastrowid
                conn.commit()
                _enqueue_extraction(doc_id, return_id)

                # ACCOUNTING-10: auto-enqueue receipts.
                if doc_type == "receipt":
                    _enqueue_receipt(doc_id, full_path)

                # DOC-HARD-5: audit trail entry per file.
                _intake_audit(
                    user_id=uploaded_by,
                    action="document_bulk_upload",
                    entity_id=str(doc_id),
                    after={
                        "doc_id": doc_id,
                        "return_id": return_id,
                        "filename": candidate,
                        "original_filename": original_filename,
                        "doc_type": doc_type,
                        "file_size_bytes": file_size_bytes,
                        "duplicate_warning": duplicate_warning,
                        "batch_size": len(uploads),
                    },
                    ip_address=request.remote_addr,
                )

                def _bg_classify(_doc_id=doc_id):
                    try:
                        with app_obj.app_context():
                            from ai_routes import _classify_document
                            _classify_document(_doc_id, only_if_still_unknown=True)
                    except Exception as exc:
                        log.error("Background classify failed for doc %s: %s", _doc_id, exc)

                threading.Thread(target=_bg_classify, daemon=True).start()

                item: dict = {
                    "success": True,
                    "doc_id": doc_id,
                    "filename": candidate,
                    "filename_original": original_filename,
                    "doc_type": doc_type,
                    "uploaded_at": uploaded_at,
                }
                if duplicate_warning:
                    item["duplicate_warning"] = duplicate_warning
                results.append(scrub_ssn_from_dict(item))

            except Exception as exc:
                conn.rollback()
                if os.path.isfile(full_path):
                    try:
                        os.remove(full_path)
                    except OSError:
                        pass
                results.append({
                    "success": False,
                    "filename_original": original_filename,
                    "error": "Could not record document",
                })

        return jsonify({"results": results})
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents")
@login_required
def return_documents_list(return_id: int):
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT
                rd.id, rd.filename, rd.original_filename, rd.doc_type, rd.source,
                rd.uploaded_by, rd.uploaded_at, rd.file_size_bytes,
                eq.status AS extraction_status,
                eq.confidence AS extraction_confidence,
                eq.detected_form_type AS extraction_detected_table,
                eq.extracted_fields AS extraction_fields_raw
            FROM return_documents rd
            LEFT JOIN (
                SELECT e.id, e.doc_id, e.status, e.confidence,
                       e.detected_form_type, e.extracted_fields
                FROM extraction_queue e
                INNER JOIN (
                    SELECT doc_id AS d2, MAX(id) AS mid
                    FROM extraction_queue GROUP BY doc_id
                ) latest ON e.doc_id = latest.d2 AND e.id = latest.mid
            ) eq ON eq.doc_id = rd.id
            WHERE rd.return_id = ? AND rd.is_deleted = 0
            ORDER BY rd.uploaded_at DESC
            """,
            (return_id,),
        ).fetchall()
        documents = []
        for r in rows:
            ext_stat = r["extraction_status"]
            try:
                extraction_confidence = float(r["extraction_confidence"]) if r["extraction_confidence"] is not None else None
            except (TypeError, ValueError):
                extraction_confidence = None
            extracted_fields_view = None
            if ext_stat == "needs_review":
                raw_j = r["extraction_fields_raw"]
                if raw_j:
                    try:
                        parsed = json.loads(raw_j)
                        extracted_fields_view = scrub_ssn_from_dict(parsed) if isinstance(parsed, dict) else {}
                    except json.JSONDecodeError:
                        extracted_fields_view = {}
            documents.append(
                scrub_ssn_from_dict(
                    {
                        "id": r["id"],
                        "filename": r["filename"],
                        "original_filename": r["original_filename"],
                        "doc_type": r["doc_type"],
                        "source": r["source"],
                        "uploaded_by": r["uploaded_by"],
                        "uploaded_at": r["uploaded_at"],
                        "file_size_bytes": r["file_size_bytes"],
                        "extraction_status": ext_stat,
                        "extraction_confidence": extraction_confidence,
                        "extracted_fields": extracted_fields_view,
                    }
                )
            )
        return jsonify({"documents": documents})
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/<int:doc_id>/view")
@login_required
def return_document_view(return_id: int, doc_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT file_path, filename FROM return_documents WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_id, return_id),
        ).fetchone()
        if not row:
            abort(404)
        disk_path = row["file_path"]
        if not disk_path or not os.path.isfile(disk_path):
            return jsonify({"error": "File not found on disk"}), 404
        fname = row["filename"] or ""
        view_ext = os.path.splitext(fname)[1].lower()
        mimetype = (
            "application/pdf" if view_ext == ".pdf"
            else "image/jpeg" if view_ext in (".jpg", ".jpeg")
            else "image/png" if view_ext == ".png"
            else None
        )
        return send_file(disk_path, as_attachment=False, mimetype=mimetype)
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/<int:doc_id>/delete", methods=["POST"])
@login_required
def return_document_delete(return_id: int, doc_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM return_documents WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_id, return_id),
        ).fetchone()
        if not row:
            return jsonify({"error": "Not found"}), 404
        conn.execute("UPDATE return_documents SET is_deleted = 1 WHERE id = ?", (doc_id,))
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/<int:doc_id>/tag", methods=["POST"])
@login_required
def return_document_tag(return_id: int, doc_id: int):
    payload = request.get_json(silent=True) or {}
    raw = payload.get("doc_type")
    if raw is None or not isinstance(raw, str):
        return jsonify({"error": "Missing or invalid doc_type"}), 400
    doc_type = raw.strip()
    if doc_type not in _ALLOWED_RETURN_DOC_TYPES:
        return jsonify({"error": "Invalid doc_type"}), 400
    conn = get_connection()
    try:
        cur = conn.execute(
            "UPDATE return_documents SET doc_type = ? WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_type, doc_id, return_id),
        )
        if cur.rowcount == 0:
            return jsonify({"error": "Not found"}), 404
        conn.commit()
        return jsonify({"success": True, "doc_type": doc_type})
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/<int:doc_id>/sync-drake", methods=["POST"])
@login_required
def return_document_sync_drake(return_id: int, doc_id: int):
    """DOC-6 — copy one document file into mirrored Drake folder layout (optional)."""
    conn = get_connection()
    try:
        doc = conn.execute(
            "SELECT id, filename, file_path FROM return_documents WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_id, return_id),
        ).fetchone()
        if not doc:
            return jsonify({"success": False, "reason": "Not found"})
        fp = doc["file_path"]
        if not fp or not os.path.isfile(fp):
            return jsonify({"success": False, "reason": "Copy failed"})

        meta = conn.execute(
            "SELECT r.tax_year, c.last_name FROM returns r JOIN clients c ON c.id = r.client_id WHERE r.id = ?",
            (return_id,),
        ).fetchone()
        if not meta:
            return jsonify({"success": False, "reason": "Copy failed"})

        drake_dir = get_drake_documents_path(
            return_id,
            str(meta["last_name"] or ""),
            str(meta["tax_year"] if meta["tax_year"] is not None else ""),
        )
        if not drake_dir:
            return jsonify({"success": False, "reason": "Drake folder structure not enabled"})

        base_abs = os.path.normpath(os.path.abspath(_config.DRAKE_DOCUMENTS_BASE))
        fname = sanitize_filename(doc["filename"] or os.path.basename(fp))
        stem, ext_part = os.path.splitext(fname)
        dest_name = fname
        counter = 1
        while os.path.exists(os.path.join(drake_dir, dest_name)):
            dest_name = f"{stem}_{counter}{ext_part}"
            counter += 1
        dest_path = os.path.join(drake_dir, dest_name)
        try:
            shutil.copy2(fp, dest_path)
        except OSError:
            return jsonify({"success": False, "reason": "Copy failed"})

        dest_abs = os.path.normpath(os.path.abspath(dest_path))
        try:
            rel_fwd = os.path.relpath(dest_abs, base_abs).replace(os.sep, "/")
        except ValueError:
            rel_fwd = dest_name.replace(os.sep, "/")
        return jsonify({"success": True, "drake_path_relative": rel_fwd})
    finally:
        conn.close()


@documents_bp.route("/return/<int:return_id>/documents/<int:doc_id>/confirm-extraction", methods=["POST"])
@login_required
def return_document_confirm_extraction(return_id: int, doc_id: int):
    """Staff confirms queued extraction marked needs_review."""
    from ai_routes import _form_table_to_doc_type, _save_form_data
    from extractor import _resolve_detected_table

    reviewer = session.get("username") or "staff"
    conn = get_connection()
    try:
        doc = conn.execute(
            "SELECT id, doc_type FROM return_documents WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_id, return_id),
        ).fetchone()
        if not doc:
            return jsonify({"error": "Not found"}), 404

        eq = conn.execute(
            """
            SELECT id, extracted_fields, detected_form_type
            FROM extraction_queue
            WHERE doc_id = ? AND return_id = ? AND status = 'needs_review'
            ORDER BY id DESC LIMIT 1
            """,
            (doc_id, return_id),
        ).fetchone()
        if not eq:
            return jsonify({"error": "No extraction pending review"}), 400

        raw_fields = eq["extracted_fields"] or "{}"
        try:
            fields = json.loads(raw_fields)
            if not isinstance(fields, dict):
                return jsonify({"error": "Invalid stored extraction"}), 400
        except json.JSONDecodeError:
            return jsonify({"error": "Invalid stored extraction"}), 400

        fields = scrub_ssn_from_dict(fields)
        table_name = eq["detected_form_type"]
        if not table_name or table_name not in _FORM_DATA_SQL_TABLES:
            table_name = _resolve_detected_table(doc["doc_type"], fields)

        if table_name == "paystub":
            doc_type_ui = _form_table_to_doc_type("paystub")
            if doc_type_ui == "unknown":
                return jsonify({"error": "Could not resolve document type"}), 400
            conn.execute(
                "UPDATE return_documents SET doc_type = ? WHERE id = ? AND return_id = ? AND is_deleted = 0",
                (doc_type_ui, doc_id, return_id),
            )
            ts = now()
            conn.execute(
                """
                UPDATE extraction_queue SET
                    status = 'completed', reviewed_by = ?, reviewed_at = ?,
                    processed_at = COALESCE(processed_at, ?),
                    extracted_fields = ?, detected_form_type = ?,
                    confidence = COALESCE(confidence, 1.0)
                WHERE id = ?
                """,
                (reviewer, ts, ts, json.dumps(fields), table_name, eq["id"]),
            )
            conn.commit()
            return jsonify({"success": True, "doc_type": doc_type_ui})

        if not table_name or table_name not in _FORM_DATA_SQL_TABLES:
            return jsonify({"error": "Could not resolve form type"}), 400

        if not _save_form_data(conn, table_name, return_id, doc_id, fields):
            return jsonify({"error": "Could not save form data"}), 500

        doc_type_ui = _form_table_to_doc_type(table_name)
        if doc_type_ui == "unknown":
            return jsonify({"error": "Could not resolve document type"}), 400

        conn.execute(
            "UPDATE return_documents SET doc_type = ? WHERE id = ? AND return_id = ? AND is_deleted = 0",
            (doc_type_ui, doc_id, return_id),
        )
        ts = now()
        conn.execute(
            """
            UPDATE extraction_queue SET
                status = 'completed', reviewed_by = ?, reviewed_at = ?,
                processed_at = COALESCE(processed_at, ?),
                extracted_fields = ?, detected_form_type = ?,
                confidence = COALESCE(confidence, 1.0)
            WHERE id = ?
            """,
            (reviewer, ts, ts, json.dumps(fields), table_name, eq["id"]),
        )
        conn.commit()
        return jsonify({"success": True, "doc_type": doc_type_ui})
    finally:
        conn.close()
