"""DEBT-1: email review Blueprint — routes extracted from app.py.

Covers:
  GET    /email-review                (page)
  GET    /api/email-classifications
  POST   /api/email-classifications/<id>/confirm
  POST   /api/email-classifications/train
  GET    /api/email-classifications/stats
  GET    /api/email-classifications/digest
  POST   /api/email-classifications/bulk-confirm
  GET    /api/email-classifications/today-confirmed
  POST   /api/email-classifications/<id>/mark-missed-reviewed
"""
from __future__ import annotations

import threading
from datetime import date, timedelta

from flask import Blueprint, current_app, jsonify, render_template, request, session

from auth import login_required, role_required
from config import DB_PATH, KNOWN_PROMOTIONAL_DOMAINS, MASS_MAILING_PREFIXES, OWN_EMAIL_DOMAINS
from db import get_connection
from utils import now, parse_iso_datetime, scrub_ssn_from_dict

email_review_bp = Blueprint("email_review", __name__)

_EC_ALLOWED = frozenset({"client_document", "client_inquiry", "promotional", "unknown"})


def _is_promotional_domain(domain: str) -> bool:
    """Return True when a sender domain is known-promotional or a mass-mailing subdomain."""
    if not domain:
        return False
    if any(domain.startswith(prefix) for prefix in MASS_MAILING_PREFIXES):
        return True
    parts = domain.split(".")
    base = ".".join(parts[-2:]) if len(parts) >= 2 else domain
    return base in KNOWN_PROMOTIONAL_DOMAINS


@email_review_bp.route("/email-review")
@role_required("preparer")
def email_review():
    from app import base_ctx  # lazy import avoids circular at module level
    ctx = base_ctx()
    ctx["active_page"] = "email_review"
    return render_template("email_review.html", **ctx)


@email_review_bp.route("/api/email-classifications")
@role_required("preparer")
def api_email_classifications_list():
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT ec.id, ec.sender_email, ec.sender_domain, ec.subject_snippet,
                   ec.classification, ec.source, ec.created_at,
                   (SELECT rd.return_id FROM return_documents rd
                    WHERE rd.source = 'email' AND rd.is_deleted = 0
                      AND rd.uploaded_at BETWEEN
                          datetime(ec.created_at, '-10 minutes') AND
                          datetime(ec.created_at, '+10 minutes')
                    LIMIT 1) AS linked_return_id
            FROM email_classifications ec
            WHERE ec.confirmed_by IS NULL
              AND ec.source != 'rule'
              AND (ec.sender_domain IS NULL OR ec.sender_domain NOT IN (
                  SELECT domain FROM known_sender_rules
              ))
            ORDER BY ec.created_at DESC
            LIMIT 200
            """
        ).fetchall()
        return jsonify({
            "classifications": [
                {
                    "id": r["id"],
                    "sender_email": r["sender_email"] or "",
                    "sender_domain": r["sender_domain"] or "",
                    "is_own": (r["sender_domain"] or "").lower() in OWN_EMAIL_DOMAINS
                              or (r["sender_email"] or "").lower() in OWN_EMAIL_DOMAINS,
                    "subject_snippet": r["subject_snippet"] or "",
                    "classification": r["classification"],
                    "source": r["source"] or "auto",
                    "created_at": r["created_at"],
                    "linked_return_id": r["linked_return_id"],
                }
                for r in rows
            ]
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/<int:classification_id>/confirm", methods=["POST"])
@role_required("preparer")
def api_email_classification_confirm(classification_id: int):
    data = request.get_json(silent=True) or {}
    confirm_current = (
        bool(data.get("confirm_current"))
        or str(data.get("confirm_current") or "").lower() in {"1", "true", "yes"}
    )
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id, sender_domain, classification FROM email_classifications WHERE id = ?",
            (classification_id,),
        ).fetchone()
        if row is None:
            return jsonify({"error": "Not found"}), 404

        if confirm_current:
            classification = row["classification"] if row["classification"] in _EC_ALLOWED else "unknown"
        else:
            classification = data.get("classification", "")
            if classification not in _EC_ALLOWED:
                return jsonify({"error": "Invalid classification.", "allowed": sorted(_EC_ALLOWED)}), 400

        conn.execute(
            """
            UPDATE email_classifications
            SET classification = ?, confirmed_by = ?, confirmed_at = ?, source = 'staff'
            WHERE id = ?
            """,
            (classification, session.get("username"), now(), classification_id),
        )

        domain = row["sender_domain"]
        if domain:
            conn.execute(
                """
                INSERT INTO domain_classifications
                    (domain, classification, confidence_count, last_seen,
                     last_confirmed_by, last_confirmed_at)
                VALUES (?, ?, 3, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    classification      = excluded.classification,
                    confidence_count    = MAX(confidence_count + 2, 3),
                    last_seen           = excluded.last_seen,
                    last_confirmed_by   = excluded.last_confirmed_by,
                    last_confirmed_at   = excluded.last_confirmed_at
                """,
                (domain, classification, now(), session.get("username"), now()),
            )

        conn.commit()

        if domain:
            from mail_watcher import _check_graduation_trigger as _cgt
            threading.Thread(
                target=_cgt,
                args=(current_app._get_current_object(), domain),
                daemon=True,
            ).start()

        from classifier import trigger_retrain_async
        trigger_retrain_async(DB_PATH)

        return jsonify({"success": True, "classification": classification})
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/train", methods=["POST"])
@role_required("preparer")
def api_email_classifications_train():
    data = request.get_json(silent=True) or {}
    confirmations = data.get("confirmations", [])
    if not isinstance(confirmations, list):
        return jsonify({"error": "confirmations must be a list"}), 400
    updated = 0
    conn = get_connection()
    try:
        for item in confirmations:
            if not isinstance(item, dict):
                continue
            cid = item.get("id")
            cls = item.get("classification", "")
            if not isinstance(cid, int) or cls not in _EC_ALLOWED:
                continue
            conn.execute(
                """
                UPDATE email_classifications
                SET classification = ?, confirmed_by = ?, confirmed_at = ?, source = 'staff'
                WHERE id = ?
                """,
                (cls, session.get("username"), now(), cid),
            )
            updated += 1
        conn.commit()
        return jsonify({"success": True, "updated": updated})
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/stats")
@role_required("preparer")
def api_email_classifications_stats():
    today = date.today().isoformat()
    conn = get_connection()
    try:
        total_today = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE created_at >= ?", (today,)
        ).fetchone()[0]
        total_confirmed = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE confirmed_by IS NOT NULL"
        ).fetchone()[0]
        total_promotional = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE classification = 'promotional'"
        ).fetchone()[0]
        total_pending = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE confirmed_by IS NULL"
        ).fetchone()[0]
        domains_cached = conn.execute(
            "SELECT COUNT(*) FROM domain_classifications"
        ).fetchone()[0]
        domains_near_graduation = conn.execute(
            "SELECT COUNT(*) FROM domain_classifications WHERE confidence_count >= 2 AND graduated = 0"
        ).fetchone()[0]
        domains_graduated = conn.execute(
            "SELECT COUNT(*) FROM domain_classifications WHERE graduated = 1"
        ).fetchone()[0]
        pending_suggestions = conn.execute(
            "SELECT COUNT(*) FROM rule_suggestions WHERE status = 'pending'"
        ).fetchone()[0]
        return jsonify({
            "total_today": total_today,
            "total_confirmed": total_confirmed,
            "total_promotional": total_promotional,
            "total_pending_review": total_pending,
            "domains_cached": domains_cached,
            "domains_near_graduation": domains_near_graduation,
            "domains_graduated": domains_graduated,
            "pending_suggestions": pending_suggestions,
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/digest")
@role_required("preparer")
def api_email_classifications_digest():
    today = date.today().isoformat()
    conn = get_connection()
    try:
        client_docs_today = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE classification = 'client_document' AND created_at >= ?",
            (today,),
        ).fetchone()[0]
        need_tagging = conn.execute(
            "SELECT COUNT(*) FROM return_documents WHERE source = 'email' AND is_deleted = 0 AND (doc_type IS NULL OR doc_type = 'unknown')"
        ).fetchone()[0]
        already_tagged = conn.execute(
            "SELECT COUNT(*) FROM return_documents WHERE source = 'email' AND is_deleted = 0 AND doc_type IS NOT NULL AND doc_type != 'unknown'"
        ).fetchone()[0]

        raw_missed_rows = conn.execute(
            """
            SELECT ec.id, ec.sender_email, ec.sender_domain, ec.subject_snippet, ec.created_at
            FROM email_classifications ec
            WHERE ec.classification = 'client_document'
              AND ec.created_at >= ?
              AND COALESCE(ec.reviewed_missed, 0) = 0
              AND COALESCE(ec.email_routed_ok, 0) = 0
            ORDER BY ec.created_at DESC
            """,
            (today,),
        ).fetchall()

        window_start = (date.fromisoformat(today) - timedelta(days=1)).isoformat()
        doc_rows = conn.execute(
            "SELECT uploaded_at FROM return_documents WHERE source = 'email' AND is_deleted = 0 AND substr(COALESCE(uploaded_at, ''), 1, 10) >= ?",
            (window_start,),
        ).fetchall()
        doc_times = [t for r in doc_rows if (t := parse_iso_datetime(r["uploaded_at"])) is not None]
        _miss_window_sec = 30 * 60

        def _no_email_upload_near(ec_row) -> bool:
            ec_t = parse_iso_datetime(ec_row["created_at"])
            if ec_t is None:
                return True
            return not any(abs((ec_t - d).total_seconds()) <= _miss_window_sec for d in doc_times)

        missed_rows = [
            r for r in raw_missed_rows
            if _no_email_upload_near(r) and not _is_promotional_domain(r["sender_domain"] or "")
        ]

        client_inquiries_today = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE classification = 'client_inquiry' AND created_at >= ?",
            (today,),
        ).fetchone()[0]
        promotional_unconfirmed = conn.execute(
            "SELECT COUNT(*) FROM email_classifications WHERE classification = 'promotional' AND confirmed_by IS NULL"
        ).fetchone()[0]
        need_attention = conn.execute(
            """
            SELECT COUNT(*) FROM email_classifications
            WHERE confirmed_by IS NULL AND source != 'rule'
              AND classification IN ('client_document', 'client_inquiry')
              AND (sender_domain IS NULL OR sender_domain NOT IN (SELECT domain FROM known_sender_rules))
            """
        ).fetchone()[0]

        untagged = conn.execute(
            """
            SELECT rd.id AS doc_id, rd.filename, rd.return_id, rd.uploaded_at,
                   c.display_name AS client_name, r.tax_year, r.log_number
            FROM return_documents rd
            JOIN returns r ON rd.return_id = r.id
            JOIN clients c ON r.client_id = c.id
            WHERE rd.source = 'email' AND rd.doc_type = 'unknown' AND rd.is_deleted = 0
            ORDER BY rd.uploaded_at DESC
            """
        ).fetchall()

        untagged_docs = [
            scrub_ssn_from_dict({
                "doc_id": r["doc_id"],
                "filename": r["filename"],
                "return_id": r["return_id"],
                "uploaded_at": r["uploaded_at"],
                "client_name": r["client_name"] or "",
                "tax_year": r["tax_year"],
                "log_number": r["log_number"] or "",
            })
            for r in untagged
        ]

        unconfirmed_matches = conn.execute(
            "SELECT COUNT(*) n FROM return_documents WHERE match_confirmed = 0 AND is_deleted = 0"
        ).fetchone()[0]

        return jsonify({
            "client_docs_today": client_docs_today,
            "need_tagging": need_tagging,
            "already_tagged": already_tagged,
            "possible_missed": len(missed_rows),
            "client_inquiries_today": client_inquiries_today,
            "promotional_unconfirmed": promotional_unconfirmed,
            "need_attention": need_attention,
            "unconfirmed_matches": unconfirmed_matches,
            "missed_items": [
                {"id": r["id"], "sender_email": r["sender_email"] or "",
                 "sender_domain": r["sender_domain"] or "",
                 "subject_snippet": r["subject_snippet"] or "", "created_at": r["created_at"]}
                for r in missed_rows
            ],
            "untagged_docs": untagged_docs,
            "untagged_count": len(untagged_docs),
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/bulk-confirm", methods=["POST"])
@role_required("preparer")
def api_email_classifications_bulk_confirm():
    data = request.get_json(silent=True) or {}
    classification = data.get("classification", "")
    if classification not in _EC_ALLOWED:
        return jsonify({"error": f"Invalid classification. Allowed: {', '.join(sorted(_EC_ALLOWED))}"}), 400
    conn = get_connection()
    try:
        affected_domains = [
            r["sender_domain"]
            for r in conn.execute(
                "SELECT DISTINCT sender_domain FROM email_classifications WHERE classification = ? AND confirmed_by IS NULL",
                (classification,),
            ).fetchall()
            if r["sender_domain"]
        ]
        result = conn.execute(
            "UPDATE email_classifications SET confirmed_by = ?, confirmed_at = ?, source = 'staff' WHERE classification = ? AND confirmed_by IS NULL",
            (session.get("username") or "staff", now(), classification),
        )
        for domain in affected_domains:
            conn.execute(
                """
                INSERT INTO domain_classifications
                    (domain, classification, confidence_count, last_seen, last_confirmed_by, last_confirmed_at)
                VALUES (?, ?, 3, ?, ?, ?)
                ON CONFLICT(domain) DO UPDATE SET
                    classification = excluded.classification,
                    confidence_count = MAX(confidence_count + 2, 3),
                    last_seen = excluded.last_seen,
                    last_confirmed_by = excluded.last_confirmed_by,
                    last_confirmed_at = excluded.last_confirmed_at
                """,
                (domain, classification, now(), session.get("username") or "staff", now()),
            )
        conn.commit()
        if affected_domains:
            from mail_watcher import _check_graduation_trigger as _cgt
            _app = current_app._get_current_object()
            for domain in affected_domains:
                threading.Thread(target=_cgt, args=(_app, domain), daemon=True).start()
        from classifier import trigger_retrain_async
        trigger_retrain_async(DB_PATH)
        return jsonify({"success": True, "confirmed_count": result.rowcount})
    except Exception:
        conn.rollback()
        return jsonify({"error": "Could not bulk confirm"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/today-confirmed")
@role_required("preparer")
def api_email_classifications_today_confirmed():
    today = date.today().isoformat()
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, sender_domain, subject_snippet, classification,
                   confirmed_by, confirmed_at, created_at
            FROM email_classifications
            WHERE confirmed_by IS NOT NULL AND created_at >= ?
            ORDER BY confirmed_at DESC LIMIT 200
            """,
            (today,),
        ).fetchall()
        return jsonify({
            "confirmed": [
                {
                    "id": r["id"],
                    "sender_domain": r["sender_domain"] or "",
                    "subject_snippet": r["subject_snippet"] or "",
                    "classification": r["classification"],
                    "confirmed_by": r["confirmed_by"] or "",
                    "confirmed_at": (r["confirmed_at"] or "").replace("T", " ")[:16],
                    "created_at": r["created_at"],
                }
                for r in rows
            ]
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/pending-review")
@role_required("preparer")
def api_email_pending_review_list():
    """EMAIL-7: return pending_review items so staff can confirm/reject."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT ec.id, ec.sender_email, ec.sender_domain, ec.subject_snippet,
                   ec.classification, ec.created_at, ec.match_score, ec.matched_client_id,
                   c.last_name, c.first_name,
                   (SELECT COUNT(*) FROM return_documents rd
                    WHERE rd.source = 'mail_pending_review'
                      AND rd.is_deleted = 0
                      AND EXISTS (
                          SELECT 1 FROM returns r WHERE r.id = rd.return_id
                          AND r.client_id = ec.matched_client_id
                      )) AS pending_doc_count
            FROM email_classifications ec
            LEFT JOIN clients c ON c.id = ec.matched_client_id
            WHERE ec.match_status = 'pending_review'
            ORDER BY ec.created_at DESC
            LIMIT 200
            """,
        ).fetchall()
        return jsonify({
            "items": [
                {
                    "id": r["id"],
                    "sender_email": r["sender_email"] or "",
                    "sender_domain": r["sender_domain"] or "",
                    "subject_snippet": r["subject_snippet"] or "",
                    "classification": r["classification"],
                    "created_at": r["created_at"],
                    "match_score": r["match_score"],
                    "matched_client_id": r["matched_client_id"],
                    "client_name": f"{r['first_name'] or ''} {r['last_name'] or ''}".strip(),
                    "pending_doc_count": r["pending_doc_count"] or 0,
                }
                for r in rows
            ]
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/<int:classification_id>/confirm-match", methods=["POST"])
@role_required("preparer")
def api_email_confirm_match(classification_id: int):
    """EMAIL-7: staff confirms a low-confidence match — promote docs from mail_pending_review → email."""
    user = session.get("user", "staff")
    conn = get_connection()
    try:
        ec = conn.execute(
            "SELECT * FROM email_classifications WHERE id = ? AND match_status = 'pending_review'",
            (classification_id,),
        ).fetchone()
        if not ec:
            return jsonify({"error": "Not found or already resolved"}), 404

        # Promote return_documents: mail_pending_review → email
        updated = conn.execute(
            """
            UPDATE return_documents
            SET source = 'email'
            WHERE source = 'mail_pending_review'
              AND is_deleted = 0
              AND return_id IN (
                  SELECT r.id FROM returns r WHERE r.client_id = ?
              )
            """,
            (ec["matched_client_id"],),
        ).rowcount

        conn.execute(
            """
            UPDATE email_classifications
            SET match_status = 'confirmed', confirmed_by = ?, confirmed_at = datetime('now')
            WHERE id = ?
            """,
            (user, classification_id),
        )
        conn.commit()
        return jsonify({"success": True, "docs_promoted": updated})
    except Exception:
        conn.rollback()
        return jsonify({"error": "Could not confirm match"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/<int:classification_id>/reject-match", methods=["POST"])
@role_required("preparer")
def api_email_reject_match(classification_id: int):
    """EMAIL-7: staff rejects a low-confidence match — soft-delete pending docs."""
    user = session.get("user", "staff")
    conn = get_connection()
    try:
        ec = conn.execute(
            "SELECT * FROM email_classifications WHERE id = ? AND match_status = 'pending_review'",
            (classification_id,),
        ).fetchone()
        if not ec:
            return jsonify({"error": "Not found or already resolved"}), 404

        # Soft-delete return_documents still in pending state
        deleted = conn.execute(
            """
            UPDATE return_documents
            SET is_deleted = 1
            WHERE source = 'mail_pending_review'
              AND is_deleted = 0
              AND return_id IN (
                  SELECT r.id FROM returns r WHERE r.client_id = ?
              )
            """,
            (ec["matched_client_id"],),
        ).rowcount

        conn.execute(
            """
            UPDATE email_classifications
            SET match_status = 'rejected', confirmed_by = ?, confirmed_at = datetime('now')
            WHERE id = ?
            """,
            (user, classification_id),
        )
        conn.commit()
        return jsonify({"success": True, "docs_removed": deleted})
    except Exception:
        conn.rollback()
        return jsonify({"error": "Could not reject match"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-classifications/<int:classification_id>/mark-missed-reviewed", methods=["POST"])
@role_required("preparer")
def api_email_classification_mark_missed_reviewed(classification_id: int):
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM email_classifications WHERE id = ?", (classification_id,)
        ).fetchone()
        if row is None:
            return jsonify({"error": "Not found"}), 404
        conn.execute(
            "UPDATE email_classifications SET reviewed_missed = 1 WHERE id = ?",
            (classification_id,),
        )
        conn.commit()
        return jsonify({"success": True})
    finally:
        conn.close()


@email_review_bp.route("/api/email-review/unconfirmed-matches")
@role_required("preparer")
def api_unconfirmed_matches():
    """Fix 3 — documents attached by email matching that staff have not yet verified.

    Returns per-document: doc_id, filename, doc_type, match_score (0.0–1.0),
    uploaded_at, return_id, client_display_name, tax_year, sender_domain,
    subject_snippet.

    Never returns file_path, ssn_last4, or identification numbers.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT rd.id          AS doc_id,
                   rd.filename,
                   rd.doc_type,
                   rd.match_score,
                   rd.uploaded_at,
                   rd.return_id,
                   c.display_name AS client_display_name,
                   r.tax_year,
                   (SELECT ec.sender_domain FROM email_classifications ec
                    WHERE ec.matched_client_id = c.id
                    ORDER BY ec.created_at DESC LIMIT 1) AS sender_domain,
                   (SELECT ec.subject_snippet FROM email_classifications ec
                    WHERE ec.matched_client_id = c.id
                    ORDER BY ec.created_at DESC LIMIT 1) AS subject_snippet
            FROM return_documents rd
            JOIN returns r  ON rd.return_id  = r.id
            JOIN clients c  ON r.client_id   = c.id
            WHERE rd.match_confirmed = 0 AND rd.is_deleted = 0
            ORDER BY rd.uploaded_at DESC
            """
        ).fetchall()
        return jsonify({
            "items": [
                scrub_ssn_from_dict({
                    "doc_id":              r["doc_id"],
                    "filename":            r["filename"],
                    "doc_type":            r["doc_type"] or "unknown",
                    "match_score":         r["match_score"],
                    "uploaded_at":         r["uploaded_at"],
                    "return_id":           r["return_id"],
                    "client_display_name": r["client_display_name"] or "",
                    "tax_year":            r["tax_year"],
                    "sender_domain":       r["sender_domain"] or "",
                    "subject_snippet":     r["subject_snippet"] or "",
                })
                for r in rows
            ]
        })
    finally:
        conn.close()


@email_review_bp.route("/api/email-review/unconfirmed-matches/<int:doc_id>/confirm", methods=["POST"])
@role_required("preparer")
def api_confirm_unconfirmed_match(doc_id: int):
    """Fix 3 — staff confirms a document is on the correct return."""
    user = session.get("username", "staff")
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT id FROM return_documents WHERE id = ? AND match_confirmed = 0 AND is_deleted = 0",
            (doc_id,),
        ).fetchone()
        if row is None:
            return jsonify({"error": "Not found or already confirmed"}), 404
        conn.execute(
            "UPDATE return_documents SET match_confirmed = 1 WHERE id = ?",
            (doc_id,),
        )
        conn.commit()
        try:
            from utils import _enqueue_write
            _enqueue_write(
                "return_document", doc_id, "match_confirmed",
                {"confirmed_by": user},
            )
        except Exception:
            pass
        return jsonify({"success": True})
    except Exception:
        conn.rollback()
        return jsonify({"error": "Could not confirm match"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-review/unconfirmed-matches/<int:doc_id>/reassign", methods=["POST"])
@role_required("preparer")
def api_reassign_unconfirmed_match(doc_id: int):
    """Fix 3 — move a document to the correct return and mark it confirmed."""
    import os
    import shutil
    from utils import get_return_documents_path

    user = session.get("username", "staff")
    data = request.get_json(silent=True) or {}
    new_return_id = data.get("return_id")
    if not isinstance(new_return_id, int):
        return jsonify({"error": "return_id (integer) is required"}), 400

    conn = get_connection()
    try:
        doc = conn.execute(
            "SELECT id, return_id, file_path, filename FROM return_documents WHERE id = ? AND is_deleted = 0",
            (doc_id,),
        ).fetchone()
        if doc is None:
            return jsonify({"error": "Document not found"}), 404

        if conn.execute("SELECT id FROM returns WHERE id = ?", (new_return_id,)).fetchone() is None:
            return jsonify({"error": "Target return not found"}), 404

        old_return_id  = doc["return_id"]
        old_file_path  = doc["file_path"]
        new_folder     = get_return_documents_path(new_return_id)
        os.makedirs(new_folder, exist_ok=True)

        new_file_path = os.path.join(new_folder, doc["filename"])
        if os.path.exists(new_file_path):
            stem, ext = os.path.splitext(doc["filename"])
            i = 1
            while os.path.exists(new_file_path):
                new_file_path = os.path.join(new_folder, f"{stem}_{i}{ext}")
                i += 1

        if old_file_path and os.path.isfile(old_file_path):
            shutil.move(old_file_path, new_file_path)

        conn.execute(
            """
            UPDATE return_documents
            SET return_id = ?, file_path = ?, match_confirmed = 1, match_method = 'manual'
            WHERE id = ?
            """,
            (new_return_id, new_file_path, doc_id),
        )
        conn.commit()
        try:
            from utils import _enqueue_write
            _enqueue_write(
                "return_document", doc_id, "reassigned",
                {"from_return": old_return_id, "to_return": new_return_id, "reassigned_by": user},
            )
        except Exception:
            pass
        return jsonify({"success": True})
    except Exception as exc:
        conn.rollback()
        return jsonify({"error": f"Could not reassign: {exc}"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-review/unconfirmed-matches/bulk-confirm", methods=["POST"])
@role_required("preparer")
def api_bulk_confirm_unconfirmed_matches():
    """Fix 5 — confirm all unconfirmed docs with match_score >= 0.90 in one click."""
    conn = get_connection()
    try:
        result = conn.execute(
            """
            UPDATE return_documents
            SET match_confirmed = 1
            WHERE match_confirmed = 0 AND match_score >= 0.90 AND is_deleted = 0
            """
        )
        conn.commit()
        return jsonify({"success": True, "confirmed_count": result.rowcount})
    except Exception:
        conn.rollback()
        return jsonify({"error": "Could not bulk confirm"}), 500
    finally:
        conn.close()


@email_review_bp.route("/api/email-processing-log")
@role_required("preparer")
def api_email_processing_log():
    """Part 7: return the last 50 rows from email_processing_log for the processing log panel.

    Privacy: returns only domain, subject_snippet, outcome, attempt_count,
    last_attempt_at, and error_message.  Never includes full email body,
    sender address, SSN, or identification numbers.
    """
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT id, sender_domain, subject_snippet, outcome,
                   attempt_count, last_attempt_at, error_message
            FROM email_processing_log
            ORDER BY last_attempt_at DESC
            LIMIT 50
            """
        ).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()
