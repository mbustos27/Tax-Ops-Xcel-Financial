"""SEC-4: WAL mode, busy_timeout, and PRAGMA configuration tests for db.get_connection()."""
from __future__ import annotations

import sqlite3
import threading
import time


def test_journal_mode_is_wal(taxops_db_path):
    """SEC-4: journal_mode returns 'wal' after get_connection() is called."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA journal_mode").fetchone()
    conn.close()
    assert row[0].lower() == "wal", f"Expected journal_mode=wal but got {row[0]!r}"


def test_synchronous_is_normal(taxops_db_path):
    """SEC-4: synchronous PRAGMA is 1 (NORMAL) — safe fsync behaviour with WAL."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA synchronous").fetchone()
    conn.close()
    # SQLite returns 1 for NORMAL
    assert row[0] == 1, f"Expected synchronous=1 (NORMAL) but got {row[0]!r}"


def test_busy_timeout_is_set(taxops_db_path):
    """SEC-4: busy_timeout is ≥ 10 000 ms so locked-DB errors don't surface immediately."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA busy_timeout").fetchone()
    conn.close()
    assert row[0] >= 10_000, f"Expected busy_timeout >= 10000 but got {row[0]}"


def test_temp_store_is_memory(taxops_db_path):
    """SEC-4: temp_store is 2 (MEMORY) — temp tables/indices stay in RAM."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA temp_store").fetchone()
    conn.close()
    # SQLite returns 2 for MEMORY
    assert row[0] == 2, f"Expected temp_store=2 (MEMORY) but got {row[0]!r}"


def test_foreign_keys_still_on(taxops_db_path):
    """SEC-4: foreign_keys enforcement remains ON after the new PRAGMAs are added."""
    from db import get_connection

    conn = get_connection(taxops_db_path)
    row = conn.execute("PRAGMA foreign_keys").fetchone()
    conn.close()
    assert row[0] == 1, "foreign_keys should still be ON"


def test_concurrent_readers_do_not_block_each_other(taxops_db_path):
    """SEC-4: WAL mode lets multiple readers proceed concurrently without blocking.

    Two threads each open their own connection and do a read inside the same
    second — under journal-delete mode one would block; under WAL both proceed.
    """
    from db import get_connection, init_db

    errors: list[str] = []
    times: list[float] = []

    def reader(i: int):
        try:
            conn = get_connection(taxops_db_path)
            t0 = time.monotonic()
            conn.execute("SELECT count(*) FROM returns").fetchone()
            times.append(time.monotonic() - t0)
            conn.close()
        except Exception as exc:
            errors.append(f"reader-{i}: {exc}")

    threads = [threading.Thread(target=reader, args=(i,)) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5)

    assert not errors, f"Concurrent readers raised errors: {errors}"
    assert len(times) == 4, "Not all reader threads completed"
    # All four reads should finish well within a second under WAL
    assert max(times) < 2.0, f"A reader took too long: {max(times):.2f}s"


def test_busy_timeout_allows_writer_to_finish(taxops_db_path):
    """SEC-4: when one connection holds a write lock, a second connection waits
    (busy_timeout) rather than raising OperationalError immediately.

    This is a behavioural smoke test: the writer holds a BEGIN EXCLUSIVE for
    ~300 ms; the second writer must not raise before busy_timeout (10 000 ms).
    """
    from db import get_connection, init_db

    barrier = threading.Event()
    release = threading.Event()
    errors: list[str] = []

    def slow_writer():
        conn = get_connection(taxops_db_path)
        try:
            conn.execute("BEGIN EXCLUSIVE")
            barrier.set()       # signal that the lock is held
            release.wait(timeout=0.4)   # hold for ~400 ms
            conn.execute("ROLLBACK")
        except Exception as exc:
            errors.append(f"slow_writer: {exc}")
        finally:
            conn.close()

    def fast_writer():
        barrier.wait(timeout=2)  # wait until slow_writer holds the lock
        conn = get_connection(taxops_db_path)
        try:
            # This must not raise immediately — busy_timeout keeps it waiting.
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("ROLLBACK")
        except sqlite3.OperationalError as exc:
            errors.append(f"fast_writer locked out immediately (busy_timeout not working): {exc}")
        except Exception as exc:
            errors.append(f"fast_writer unexpected error: {exc}")
        finally:
            release.set()
            conn.close()

    t1 = threading.Thread(target=slow_writer)
    t2 = threading.Thread(target=fast_writer)
    t1.start()
    t2.start()
    t1.join(timeout=6)
    t2.join(timeout=6)

    assert not errors, "\n".join(errors)
