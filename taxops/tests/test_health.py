"""PROD-3 / HEALTH-1..3: GET /health — anonymous JSON probes."""

from __future__ import annotations

import sqlite3
import threading

import pytest


def test_health_ok(client):
    rv = client.get("/health")
    assert rv.status_code == 200
    data = rv.get_json()
    assert data["status"] == "ok"
    assert data["db"]["ok"] is True
    assert isinstance(data["db"]["latency_ms"], (int, float))
    assert data["db"]["latency_ms"] >= 0
    assert isinstance(data["uptime_seconds"], (int, float))
    assert data["uptime_seconds"] >= 0
    assert "version" in data and isinstance(data["version"], str) and len(data["version"]) > 0


def test_health_includes_workers_field(client):
    """HEALTH-1: /health must include a workers dict."""
    rv = client.get("/health")
    data = rv.get_json()
    assert "workers" in data
    assert isinstance(data["workers"], dict)


def test_health_workers_has_extraction_key(client):
    """HEALTH-3: extraction worker status always present."""
    rv = client.get("/health")
    workers = rv.get_json()["workers"]
    assert "extraction" in workers
    ext = workers["extraction"]
    assert "started" in ext
    assert "running" in ext
    assert isinstance(ext["started"], bool)
    assert isinstance(ext["running"], bool)


def test_health_workers_has_mail_watcher_key(client):
    """HEALTH-2: mail_watcher status always present."""
    rv = client.get("/health")
    workers = rv.get_json()["workers"]
    assert "mail_watcher" in workers
    mw = workers["mail_watcher"]
    assert "started" in mw
    assert "running" in mw
    assert "configured" in mw


def test_health_worker_reports_live_thread(client, monkeypatch):
    """HEALTH-3: running=True when thread.is_alive() is True."""
    import extractor as ext_mod
    stop = threading.Event()
    fake_thread = threading.Thread(target=lambda: stop.wait(), daemon=True)
    fake_thread.start()
    try:
        monkeypatch.setattr(ext_mod, "_worker_thread", fake_thread)
        monkeypatch.setattr(ext_mod, "_worker_started", True)
        rv = client.get("/health")
        ext_status = rv.get_json()["workers"]["extraction"]
        assert ext_status["started"] is True
        assert ext_status["running"] is True
    finally:
        stop.set()
        fake_thread.join(timeout=2)


def test_health_worker_reports_dead_thread(client, monkeypatch):
    """HEALTH-3: running=False when thread has stopped."""
    import extractor as ext_mod
    dead_thread = threading.Thread(target=lambda: None, daemon=True)
    dead_thread.start()
    dead_thread.join(timeout=2)
    monkeypatch.setattr(ext_mod, "_worker_thread", dead_thread)
    monkeypatch.setattr(ext_mod, "_worker_started", True)

    rv = client.get("/health")
    ext_status = rv.get_json()["workers"]["extraction"]
    assert ext_status["started"] is True
    assert ext_status["running"] is False


def test_health_mail_watcher_unconfigured(client, monkeypatch):
    """HEALTH-2: configured=False when IMAP_HOST is unset."""
    import mail_watcher as mw_mod
    import config as cfg_mod
    monkeypatch.setattr(cfg_mod, "IMAP_HOST", "")
    monkeypatch.setattr(mw_mod, "_watcher_thread", None)
    monkeypatch.setattr(mw_mod, "_watcher_started", False)

    rv = client.get("/health")
    mw = rv.get_json()["workers"]["mail_watcher"]
    assert mw["configured"] is False
    assert mw["running"] is False


def test_health_no_login_required(client):
    rv = client.get("/health")
    assert rv.status_code == 200
    assert rv.get_json().get("error") != "login_required"


def test_health_version_from_env(monkeypatch: pytest.MonkeyPatch, client):
    import config as cfg

    monkeypatch.setenv("TAXOPS_VERSION", "v9.9.9-test")
    monkeypatch.setattr(cfg, "_RELEASE_VERSION_CACHED", None)

    rv = client.get("/health")
    assert rv.status_code == 200
    assert rv.get_json()["version"] == "v9.9.9-test"


def test_health_db_failure_returns_503(monkeypatch: pytest.MonkeyPatch, client):
    import app as app_mod

    def _bad(**_kw):
        raise sqlite3.OperationalError("simulated disconnect")

    monkeypatch.setattr(app_mod, "get_connection", lambda *a, **k: _bad(*a, **k))
    rv = client.get("/health")
    assert rv.status_code == 503
    data = rv.get_json()
    assert data["status"] == "degraded"
    assert data["db"]["ok"] is False
    assert "error" in data["db"]
