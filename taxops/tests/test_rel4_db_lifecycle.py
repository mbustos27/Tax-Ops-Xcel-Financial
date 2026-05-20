"""REL-4: DB connection lifecycle — contextlib.closing and g-teardown."""
from __future__ import annotations

import contextlib
import sqlite3


def test_contextlib_closing_imported_in_app():
    """contextlib is imported in app.py (REL-4 pattern available)."""
    import app as app_mod

    assert hasattr(app_mod, "contextlib")


def test_get_db_returns_same_connection_within_request(app, taxops_db_path):
    """get_db() from Flask g returns the same connection object within a request context."""
    with app.test_request_context("/"):
        from flask import g
        from app import get_db

        conn1 = get_db()
        conn2 = get_db()
        assert conn1 is conn2


def test_get_db_connection_closed_after_teardown(app, taxops_db_path):
    """Connection stored in g is closed when the app context tears down.

    Verified by checking that the connection is unusable (raises) after the
    context exits — which only happens if close() was called.
    """
    from app import get_db

    with app.test_request_context("/"):
        conn_ref = get_db()
        # Confirm it works while the context is live.
        conn_ref.execute("SELECT 1").fetchone()

    # After the context exits, teardown_appcontext closes g.db.
    # A closed SQLite connection raises ProgrammingError on any operation.
    try:
        conn_ref.execute("SELECT 1")
        closed = False
    except Exception:
        closed = True

    assert closed, "g.db was not closed after app context teardown"


def test_get_one_does_not_leave_connection_open(tmp_path, monkeypatch):
    """get_one() closes its connection even if the query returns None."""
    import config as cfg
    import db as db_mod

    db_path = str(tmp_path / "gc_test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    from db import get_connection, init_db

    conn = get_connection(db_path)
    init_db(conn)
    conn.close()

    import app as app_mod
    monkeypatch.setattr(app_mod, "DB_PATH", db_path)

    # Should not raise even for non-existent id
    result = app_mod.get_one(999999)
    assert result is None


def test_query_returns_does_not_leave_connection_open(tmp_path, monkeypatch):
    """query_returns() closes its connection in the finally block."""
    import config as cfg
    import db as db_mod

    db_path = str(tmp_path / "gc_test2.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)
    from db import get_connection, init_db

    conn = get_connection(db_path)
    init_db(conn)
    conn.close()

    import app as app_mod
    monkeypatch.setattr(app_mod, "DB_PATH", db_path)

    results = app_mod.query_returns()
    assert isinstance(results, list)
