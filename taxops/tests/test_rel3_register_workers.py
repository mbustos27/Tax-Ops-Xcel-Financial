"""REL-3: register_workers() is importable and callable from any entry point."""
from __future__ import annotations

import inspect


def test_register_workers_is_importable():
    """register_workers can be imported from app module."""
    from app import register_workers

    assert callable(register_workers)


def test_register_workers_accepts_flask_app():
    """register_workers signature accepts a single positional flask_app argument."""
    from app import register_workers

    sig = inspect.signature(register_workers)
    params = list(sig.parameters)
    assert len(params) == 1
    assert params[0] == "flask_app"


def test_register_workers_calls_init_db(monkeypatch, tmp_path):
    """register_workers calls init_db (DB migration runs on any entry point)."""
    import config as cfg
    import db as db_mod
    import app as app_mod

    db_path = str(tmp_path / "rw_test.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    calls = []
    original_init_db = db_mod.init_db

    def _mock_init_db(conn):
        calls.append("init_db")
        return original_init_db(conn)

    monkeypatch.setattr(db_mod, "init_db", _mock_init_db)
    monkeypatch.setattr(app_mod, "init_db", _mock_init_db)

    # Prevent actual daemon threads from starting in tests.
    monkeypatch.setattr(app_mod, "start_mail_watcher", lambda _app: None)
    monkeypatch.setattr(app_mod, "start_extraction_worker", lambda _app: None)

    import threading
    monkeypatch.setattr(threading.Thread, "start", lambda self: None)

    from app import register_workers, app as flask_app

    register_workers(flask_app)

    assert "init_db" in calls, "register_workers did not call init_db"


def test_register_workers_calls_bootstrap_auth_user(monkeypatch, tmp_path):
    """register_workers calls bootstrap_auth_user to seed the first admin."""
    import config as cfg
    import db as db_mod
    import app as app_mod

    db_path = str(tmp_path / "rw_test2.db")
    monkeypatch.setattr(cfg, "DB_PATH", db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", db_path)

    calls = []

    def _mock_bootstrap(conn):
        calls.append("bootstrap")
        return False

    monkeypatch.setattr(app_mod, "bootstrap_auth_user", _mock_bootstrap)
    monkeypatch.setattr(app_mod, "start_mail_watcher", lambda _app: None)
    monkeypatch.setattr(app_mod, "start_extraction_worker", lambda _app: None)

    import threading
    monkeypatch.setattr(threading.Thread, "start", lambda self: None)

    from app import register_workers, app as flask_app

    register_workers(flask_app)

    assert "bootstrap" in calls
