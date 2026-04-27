"""Shared fixtures. Patch DB before importing ``app`` so tests use a temp database."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

# Application lives in parent of tests/ (flat modules: db, config, app, …)
_TAXOPS_ROOT = Path(__file__).resolve().parent.parent
if str(_TAXOPS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TAXOPS_ROOT))


@pytest.fixture
def taxops_db_path(tmp_path, monkeypatch: pytest.MonkeyPatch) -> str:
    path = str(tmp_path / "taxops_test.db")
    monkeypatch.setenv("TAXOPS_DB", path)
    import config as cfg
    import db as db_mod

    monkeypatch.setattr(cfg, "DB_PATH", path)
    monkeypatch.setattr(db_mod, "DB_PATH", path)
    from db import get_connection, init_db

    conn = get_connection(path)
    init_db(conn)
    conn.close()
    return path


@pytest.fixture
def app(taxops_db_path: str):
    import app as app_module

    app_module.app.config["TESTING"] = True
    return app_module.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def client_logged_in(client, app, monkeypatch: pytest.MonkeyPatch):
    # app reads login env-vars at import time; patch the module globals for tests.
    import app as mod

    monkeypatch.setattr(mod, "_LOGIN_USER", "__test_user__")
    monkeypatch.setattr(mod, "_LOGIN_PASS", "__test_pass__")
    client.post(
        "/login",
        data={"username": "__test_user__", "password": "__test_pass__"},
        follow_redirects=True,
    )
    return client
