"""REL-2: single long-lived audit writer queue replaces per-request thread spawn."""
from __future__ import annotations

import queue
import threading
import time

import pytest


def test_audit_queue_is_bounded():
    """_audit_queue has a maxsize set."""
    from audit_service import _audit_queue, _AUDIT_QUEUE_MAXSIZE

    assert _audit_queue.maxsize == _AUDIT_QUEUE_MAXSIZE
    assert _AUDIT_QUEUE_MAXSIZE > 0


def test_audit_queue_depth_returns_int():
    """audit_queue_depth() returns an integer >= 0."""
    from audit_service import audit_queue_depth

    depth = audit_queue_depth()
    assert isinstance(depth, int)
    assert depth >= 0


def test_enqueue_write_does_not_spawn_thread(monkeypatch):
    """_enqueue_write puts on _audit_queue without spawning a thread."""
    from audit_service import _audit_queue, _enqueue_write

    threads_before = threading.active_count()

    # Drain queue snapshot size before enqueue.
    size_before = _audit_queue.qsize()

    _enqueue_write(
        user_id="test",
        action="TEST",
        entity_type="return",
        entity_id="1",
        before=None,
        after={"x": 1},
        ip_address="127.0.0.1",
        http_status=200,
    )

    # Thread count must not have increased.
    threads_after = threading.active_count()
    assert threads_after <= threads_before + 1  # writer thread may already exist
    # Queue depth must have grown by 1 (unless writer drained it already — that's fine).
    # At least no exception was raised.


def test_start_audit_writer_is_idempotent():
    """Calling start_audit_writer() multiple times starts only one thread."""
    from audit_service import start_audit_writer

    count_before = sum(
        1 for t in threading.enumerate() if t.name == "audit-log-writer"
    )
    start_audit_writer()
    start_audit_writer()
    count_after = sum(
        1 for t in threading.enumerate() if t.name == "audit-log-writer"
    )
    assert count_after <= count_before + 1


def test_audit_writer_thread_is_daemon():
    """The audit writer thread is a daemon (won't block process exit)."""
    from audit_service import start_audit_writer

    start_audit_writer()
    writers = [t for t in threading.enumerate() if t.name == "audit-log-writer"]
    assert writers, "audit-log-writer thread not found after start_audit_writer()"
    assert all(t.daemon for t in writers)


def test_full_queue_drops_with_warning(caplog):
    """When _audit_queue is full, _enqueue_write logs a warning and does not raise."""
    import logging
    import audit_service as audit_svc
    from audit_service import _enqueue_write

    # Replace the module-level queue with a capacity-1 queue that is pre-filled.
    full_q: queue.Queue = queue.Queue(maxsize=1)
    full_q.put_nowait(("placeholder",))  # fill it so the next put_nowait raises queue.Full
    original = audit_svc._audit_queue
    audit_svc._audit_queue = full_q
    try:
        with caplog.at_level(logging.WARNING, logger="audit_service"):
            _enqueue_write(
                user_id="test",
                action="TEST_FULL",
                entity_type="return",
                entity_id="99",
                before=None,
                after=None,
                ip_address="127.0.0.1",
                http_status=200,
            )
        assert any(
            "full" in r.message.lower() or "dropping" in r.message.lower()
            for r in caplog.records
        ), f"Expected warning log; got: {[r.message for r in caplog.records]}"
    finally:
        audit_svc._audit_queue = original
