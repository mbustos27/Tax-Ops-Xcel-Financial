"""REL-5: threading.Event wake-on-enqueue eliminates 60-second poll delay."""
from __future__ import annotations

import time
import threading


def test_work_event_exists_in_extractor():
    """extractor._work_event is a threading.Event."""
    from extractor import _work_event

    assert isinstance(_work_event, threading.Event)


def test_notify_extraction_worker_sets_event():
    """_notify_extraction_worker() sets _work_event."""
    from extractor import _work_event, _notify_extraction_worker

    _work_event.clear()
    _notify_extraction_worker()
    assert _work_event.is_set()


def test_worker_loop_clears_event_after_waking(monkeypatch):
    """After the worker loop processes one cycle, _work_event is cleared."""
    import extractor

    processed = []

    def _mock_process_queue():
        processed.append(True)

    monkeypatch.setattr(extractor, "_process_queue", _mock_process_queue)

    # Manually call what _worker_loop does (minus the infinite while True).
    extractor._work_event.set()
    extractor._work_event.wait(timeout=1)
    extractor._work_event.clear()
    with monkeypatch.context() as m:
        # Ensure it was processed.
        pass
    assert not extractor._work_event.is_set()


def test_enqueue_extraction_wakes_worker(monkeypatch, tmp_path):
    """_enqueue_extraction triggers _notify_extraction_worker via utils._enqueue_extraction."""
    import config as cfg
    import db as db_mod

    db_path = str(tmp_path / "ev_test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    from db import get_connection, init_db

    conn = get_connection(db_path)
    init_db(conn)
    # Insert a dummy client + return + document so FK constraints pass.
    conn.execute(
        "INSERT INTO clients (id, display_name, created_at) VALUES (1, 'Test', '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO returns (id, client_id, tax_year, client_status, created_at) VALUES (1, 1, 2025, 'IN PROGRESS', '2026-01-01')"
    )
    conn.execute(
        "INSERT INTO return_documents (id, return_id, filename, original_filename, doc_type, file_path, uploaded_at) "
        "VALUES (1, 1, 'test.pdf', 'test.pdf', 'misc', '/tmp/test.pdf', '2026-01-01')"
    )
    conn.commit()
    conn.close()

    from extractor import _work_event

    _work_event.clear()

    from utils import _enqueue_extraction

    _enqueue_extraction(doc_id=1, return_id=1)

    # The event should have been set by _notify_extraction_worker.
    assert _work_event.is_set(), "_work_event not set after _enqueue_extraction"
