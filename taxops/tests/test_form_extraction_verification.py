"""Form extraction verification — extractor worker + guarded doc_type tagging.

Synthetic W‑2 payloads (no LLM): exercise extractor._process_item → _save_form_data
→ _apply_extraction_doc_tag (unknown → W‑2 when confidence threshold met).

Manual UI smoke (production-like):
    Log in to TaxOps → open a return → Documents list.
    For a PDF marked unknown, either wait for the extraction worker (pending queue)
    or trigger extract from tooling that hits POST ``/ai/documents/<doc_id>/extract``.
    Confirm the row shows document type ``W‑2`` after thresholds are met; staff-set
    types (e.g. ``1099``) must not flip to ``W‑2`` from extraction alone.
"""

from __future__ import annotations

import pytest

from db import get_connection, init_db


SYNTH_W2_HIGH_CONFIDENCE = {
    "form_type": "W-2",
    "employer_name": "Acme Corp",
    "tax_year": "2024",
    "box1_wages_tips_other": "50000",
    "box2_federal_income_tax_withheld": "6000",
}

SYNTH_PAYSTUB_HIGH_CONFIDENCE = {
    "form_type": "PAYSTUB",
    "employer_name": "Acme Payroll",
    "employee_name": "Jane Worker",
    "pay_date": "2024-06-01",
    "has_overtime": True,
    "ytd_gross": "45200",
    "overtime_hours": "4",
}


def _seed_return_with_queued_pdf(
    taxops_db_path: str,
    tmp_path,
    *,
    doc_type: str = "unknown",
) -> dict:
    pdf = tmp_path / "stub_w2.pdf"
    pdf.write_bytes(b"%PDF-1.4 synthetic")

    conn = get_connection(taxops_db_path)
    init_db(conn)
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (7801, 'Syn', 'W2', 'Syn, W2');
        """
    )
    conn.execute(
        """
        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor, verified, client_status,
            intake_date
        ) VALUES (
            7802, 7801, 'ext99', 2025, '', 0, 'PROCESSING', '2026-05-01'
        );
        """
    )
    conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, uploaded_by, uploaded_at, is_deleted
        ) VALUES (?, 'stub_w2.pdf', 'stub_w2.pdf', ?, 'email', ?, 100, 'test', '2026-05-01', 0)
        """,
        (7802, doc_type, str(pdf)),
    )
    doc_row = conn.execute(
        "SELECT id FROM return_documents WHERE return_id = 7802 LIMIT 1"
    ).fetchone()
    doc_id = int(doc_row["id"])

    conn.execute(
        """
        INSERT INTO extraction_queue (doc_id, return_id, status, created_at)
        VALUES (?, ?, 'pending', '2026-05-01')
        """,
        (doc_id, 7802),
    )
    q_row = conn.execute(
        "SELECT id FROM extraction_queue WHERE doc_id = ? ORDER BY id DESC LIMIT 1",
        (doc_id,),
    ).fetchone()
    conn.commit()
    conn.close()

    return {
        "pdf": pdf,
        "return_id": 7802,
        "doc_id": doc_id,
        "queue_id": int(q_row["id"]),
    }


def test_worker_tags_unknown_paystub_without_sql_row(monkeypatch, taxops_db_path, tmp_path):
    """Paystub: doc_type tag only; no INSERT into w2_records / 1099 tables."""
    import extractor as ex

    env = _seed_return_with_queued_pdf(taxops_db_path, tmp_path)
    monkeypatch.setattr(
        ex,
        "_extract_fields",
        lambda fp, fn: (dict(SYNTH_PAYSTUB_HIGH_CONFIDENCE), "stub"),
    )

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT eq.*, rd.file_path, rd.filename, rd.doc_type
            FROM extraction_queue eq
            JOIN return_documents rd ON eq.doc_id = rd.id
            WHERE eq.id = ?
            """,
            (env["queue_id"],),
        ).fetchone()
        ex._process_item(conn, dict(row))
    finally:
        conn.close()

    conn = get_connection(taxops_db_path)
    try:
        dt = conn.execute(
            "SELECT doc_type FROM return_documents WHERE id = ?",
            (env["doc_id"],),
        ).fetchone()["doc_type"]
        assert dt == "paystub"

        st = conn.execute(
            "SELECT status FROM extraction_queue WHERE id = ?",
            (env["queue_id"],),
        ).fetchone()["status"]
        assert st == "completed"

        w2_n = conn.execute(
            "SELECT COUNT(*) AS c FROM w2_records WHERE doc_id = ?",
            (env["doc_id"],),
        ).fetchone()["c"]
        assert int(w2_n) == 0
    finally:
        conn.close()


def test_detect_paystub_from_ot_ytd_without_form_type_hint():
    import ai_routes

    fields = {
        "employer_name": "Retail Co",
        "pay_date": "05/15/2025",
        "ytd_gross": "24000",
        "overtime_pay": "120",
    }
    assert ai_routes._detect_form_type("unknown", fields) == "paystub"


def test_worker_tags_unknown_w2_after_save(monkeypatch, taxops_db_path, tmp_path):
    import extractor as ex

    env = _seed_return_with_queued_pdf(taxops_db_path, tmp_path)
    monkeypatch.setattr(
        ex,
        "_extract_fields",
        lambda fp, fn: (dict(SYNTH_W2_HIGH_CONFIDENCE), "stub"),
    )

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT eq.*, rd.file_path, rd.filename, rd.doc_type
            FROM extraction_queue eq
            JOIN return_documents rd ON eq.doc_id = rd.id
            WHERE eq.id = ?
            """,
            (env["queue_id"],),
        ).fetchone()
        ex._process_item(conn, dict(row))
    finally:
        conn.close()

    conn = get_connection(taxops_db_path)
    try:
        dt = conn.execute(
            "SELECT doc_type FROM return_documents WHERE id = ?",
            (env["doc_id"],),
        ).fetchone()["doc_type"]
        assert dt == "W-2"

        st = conn.execute(
            "SELECT status FROM extraction_queue WHERE id = ?",
            (env["queue_id"],),
        ).fetchone()["status"]
        assert st == "completed"

        inserted = conn.execute(
            "SELECT COUNT(*) AS c FROM w2_records WHERE doc_id = ?",
            (env["doc_id"],),
        ).fetchone()["c"]
        assert int(inserted) >= 1
    finally:
        conn.close()


def test_worker_does_not_overwrite_staff_doc_type(monkeypatch, taxops_db_path, tmp_path):
    import ai_routes
    import extractor as ex

    env = _seed_return_with_queued_pdf(taxops_db_path, tmp_path, doc_type="1099")
    monkeypatch.setattr(
        ex,
        "_extract_fields",
        lambda fp, fn: (dict(SYNTH_W2_HIGH_CONFIDENCE), "stub"),
    )

    monkeypatch.setattr(ai_routes, "_detect_form_type", lambda dt, fields: "w2_records")

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT eq.*, rd.file_path, rd.filename, rd.doc_type
            FROM extraction_queue eq
            JOIN return_documents rd ON eq.doc_id = rd.id
            WHERE eq.id = ?
            """,
            (env["queue_id"],),
        ).fetchone()
        ex._process_item(conn, dict(row))
    finally:
        conn.close()

    conn = get_connection(taxops_db_path)
    try:
        dt = conn.execute(
            "SELECT doc_type FROM return_documents WHERE id = ?",
            (env["doc_id"],),
        ).fetchone()["doc_type"]
        assert dt == "1099"
    finally:
        conn.close()


def test_manual_extract_route_tags_unknown(monkeypatch, client_logged_in, taxops_db_path, tmp_path):
    import extractor as ex

    env = _seed_return_with_queued_pdf(taxops_db_path, tmp_path)

    monkeypatch.setattr(
        ex,
        "_extract_fields",
        lambda fp, fn: (dict(SYNTH_W2_HIGH_CONFIDENCE), "text"),
    )

    r = client_logged_in.post(f"/ai/documents/{env['doc_id']}/extract")
    assert r.status_code == 200
    payload = r.get_json()
    assert payload.get("saved_to") == "w2_records"

    conn = get_connection(taxops_db_path)
    try:
        dt = conn.execute(
            "SELECT doc_type FROM return_documents WHERE id = ?",
            (env["doc_id"],),
        ).fetchone()["doc_type"]
        assert dt == "W-2"
    finally:
        conn.close()


def test_classify_shim_respects_only_unknown(monkeypatch, taxops_db_path, tmp_path):
    """_classify_document_using_row does not overwrite a non-unknown doc_type."""
    import ai_routes
    import extractor as ex

    env = _seed_return_with_queued_pdf(taxops_db_path, tmp_path, doc_type="1099")
    monkeypatch.setattr(
        ex,
        "_extract_fields",
        lambda fp, fn: (dict(SYNTH_W2_HIGH_CONFIDENCE), "stub"),
    )
    monkeypatch.setattr(ai_routes, "_detect_form_type", lambda dt, fields: "w2_records")

    conn = get_connection(taxops_db_path)
    try:
        row = conn.execute(
            """
            SELECT id, return_id, doc_type, file_path, filename
            FROM return_documents WHERE id = ?
            """,
            (env["doc_id"],),
        ).fetchone()
        out = ai_routes._classify_document_using_row(
            conn, row, only_if_still_unknown=True
        )
        assert out["doc_type"] == "W-2"
        conn.commit()
    finally:
        conn.close()

    conn = get_connection(taxops_db_path)
    try:
        dt = conn.execute(
            "SELECT doc_type FROM return_documents WHERE id = ?",
            (env["doc_id"],),
        ).fetchone()["doc_type"]
        assert dt == "1099"
    finally:
        conn.close()

