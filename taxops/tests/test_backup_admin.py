"""BACKUP-1..5: nightly_backup_db + backup.py wrapper + admin API endpoint."""
from __future__ import annotations

import importlib.util
import os
import sqlite3
import sys
import uuid
from pathlib import Path

import pytest


# ── helpers ─────────────────────────────────────────────────────────────────

def _script_path() -> Path:
    return Path(__file__).resolve().parents[1] / "scripts" / "nightly_backup_db.py"


def _load_script():
    script = _script_path()
    mod_name = "_nbackup_" + uuid.uuid4().hex
    spec = importlib.util.spec_from_file_location(mod_name, script)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture()
def bmod():
    return _load_script()


# ── BACKUP-1: datestamped filename ───────────────────────────────────────────

def test_backup_filename_uses_utc_stamp(bmod, tmp_path, monkeypatch):
    """Backup file name contains UTC ISO timestamp and correct suffix."""
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src)); cx.execute("CREATE TABLE t(x)"); cx.commit(); cx.close()

    bdir = tmp_path / "bk"; bdir.mkdir()
    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))
    mod = _load_script()
    # db_path passed explicitly (3.0 fix) — no reliance on TAXOPS_DB env timing
    # or config.DB_PATH's import-time binding.
    ok, out, msg = mod._run_once(db_path=str(src))

    assert ok, msg
    assert out is not None
    name = Path(out).name
    assert name.startswith("taxops_backup_")
    assert name.endswith(".sqlite")
    # UTC stamp portion: 8 digits 'T' 6 digits 'Z'
    import re
    assert re.search(r'\d{8}T\d{6}Z', name), f"No UTC stamp in {name!r}"


def test_backup_creates_readable_sqlite(bmod, tmp_path):
    """Backup file is a valid SQLite database with the source table."""
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src))
    cx.execute("CREATE TABLE clients(id INTEGER PRIMARY KEY)")
    cx.execute("INSERT INTO clients VALUES(1)")
    cx.commit(); cx.close()

    bdir = tmp_path / "bk"; bdir.mkdir()
    dest = bdir / "manual.sqlite"
    bmod.backup_sqlite_live(src, dest)

    c2 = sqlite3.connect(str(dest))
    row = c2.execute("SELECT id FROM clients").fetchone()
    c2.close()
    assert row == (1,)


# ── BACKUP-3: 30-day retention ────────────────────────────────────────────────

def test_retention_removes_stale_keeps_fresh(bmod, tmp_path):
    bk = tmp_path / "bk"; bk.mkdir()
    epoch = 1_700_000_000.0

    stale = bk / "taxops_backup_stale.sqlite"; stale.write_bytes(b"s")
    os.utime(stale, (epoch - 35 * 86400, epoch - 35 * 86400))

    fresh = bk / "taxops_backup_fresh.sqlite"; fresh.write_bytes(b"f")
    os.utime(fresh, (epoch - 5 * 86400, epoch - 5 * 86400))

    rem, errs = bmod.retention_cleanup(bk, retention_days=30, now=lambda: epoch)
    assert errs == []
    assert rem == 1
    assert not stale.exists()
    assert fresh.exists()


def test_retention_zero_removes_nothing_when_all_fresh(bmod, tmp_path):
    bk = tmp_path / "bk"; bk.mkdir()
    epoch = 1_700_000_000.0

    f = bk / "taxops_backup_recent.sqlite"; f.write_bytes(b"r")
    os.utime(f, (epoch - 1 * 86400, epoch - 1 * 86400))

    rem, _ = bmod.retention_cleanup(bk, retention_days=30, now=lambda: epoch)
    assert rem == 0
    assert f.exists()


# ── BACKUP-4: failure alert + error log ──────────────────────────────────────

def test_run_once_writes_error_log_on_missing_db(tmp_path, monkeypatch):
    """BACKUP-4: failure writes a line to backup_error.log."""
    import config as cfg
    bdir   = tmp_path / "bk"; bdir.mkdir()
    errlog = bdir / "backup_error.log"

    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "nonexistent.db"))
    monkeypatch.setenv("TAXOPS_BACKUP_DIR",       str(bdir))
    monkeypatch.setenv("TAXOPS_BACKUP_ERROR_LOG", str(errlog))

    mod = _load_script()
    ok, _, msg = mod._run_once()

    assert ok is False
    assert errlog.exists(), "backup_error.log should be created on failure"
    content = errlog.read_text(encoding="utf-8")
    assert "BACKUP FAILED" in content


def test_run_once_logs_warning_on_failure(tmp_path, monkeypatch, caplog):
    """BACKUP-4: _write_error_log is called (implicitly covers WARNING path) on missing-DB failure."""
    import logging
    import config as cfg
    bdir = tmp_path / "bk"; bdir.mkdir()
    errlog = bdir / "backup_error.log"

    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "gone.db"))
    monkeypatch.setenv("TAXOPS_BACKUP_DIR",       str(bdir))
    monkeypatch.setenv("TAXOPS_BACKUP_ERROR_LOG", str(errlog))

    mod = _load_script()
    ok, _, _ = mod._run_once()

    assert ok is False
    # Confirm warning path executed: error log was written
    assert errlog.exists()


def test_run_once_no_error_log_on_success(tmp_path, monkeypatch):
    """BACKUP-4: no error log written when backup succeeds."""
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src)); cx.execute("CREATE TABLE t(x)"); cx.commit(); cx.close()

    bdir   = tmp_path / "bk"; bdir.mkdir()
    errlog = bdir / "backup_error.log"

    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))
    monkeypatch.setenv("TAXOPS_BACKUP_ERROR_LOG", str(errlog))
    mod = _load_script()
    ok, _, msg = mod._run_once(db_path=str(src))

    assert ok, msg
    assert not errlog.exists(), "error log must NOT be written on success"


# ── Side-task 3.0: DB_PATH resolved at call time, not import time ───────────

def test_run_once_default_db_path_reads_config_at_call_time(tmp_path, monkeypatch):
    """Without an explicit db_path, _run_once() must use config.DB_PATH as it
    is *at call time* — proving the fix for the frozen `from config import
    DB_PATH` binding that made monkeypatching unreachable."""
    import config as cfg
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src)); cx.execute("CREATE TABLE t(x)"); cx.commit(); cx.close()
    bdir = tmp_path / "bk"; bdir.mkdir()

    monkeypatch.setattr(cfg, "DB_PATH", str(src))
    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))

    mod = _load_script()
    ok, out, msg = mod._run_once()

    assert ok, msg
    assert out is not None


def test_run_once_explicit_db_path_overrides_config(tmp_path, monkeypatch):
    """An explicit db_path always wins over config.DB_PATH, however stale."""
    import config as cfg
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src)); cx.execute("CREATE TABLE t(x)"); cx.commit(); cx.close()
    bdir = tmp_path / "bk"; bdir.mkdir()

    # config.DB_PATH deliberately points somewhere that doesn't exist.
    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "does_not_exist.db"))
    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))

    mod = _load_script()
    ok, out, msg = mod._run_once(db_path=str(src))

    assert ok, msg
    assert out is not None


def test_main_entry_point_signature_unchanged(bmod):
    """BACKUP entry point stays backward compatible: main() takes no args and
    calls _run_once() with defaults, matching the nightly Task Scheduler
    invocation (`python scripts/nightly_backup_db.py`)."""
    import inspect
    sig = inspect.signature(bmod.main)
    assert list(sig.parameters) == [], "main() must remain a no-arg entry point"


# ── BACKUP-5: backup.py wrapper ───────────────────────────────────────────────

def test_backup_module_importable():
    """backup.py is importable and exposes run_backup and BackupResult."""
    import backup as bk
    assert callable(bk.run_backup)
    assert bk.BackupResult


def test_run_backup_returns_backupresult(tmp_path, monkeypatch):
    """run_backup() returns a BackupResult with expected fields."""
    src = tmp_path / "live.db"
    cx = sqlite3.connect(str(src)); cx.execute("CREATE TABLE t(x)"); cx.commit(); cx.close()

    bdir = tmp_path / "bk"; bdir.mkdir()
    import backup as bk
    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))
    result = bk.run_backup(db_path=str(src))

    assert result.success is True
    assert result.backup_file is not None
    assert result.size_bytes is not None and result.size_bytes > 0
    assert "Backup OK" in result.message


def test_run_backup_failure_returns_false(tmp_path, monkeypatch):
    """run_backup() returns success=False for missing source DB."""
    import config as cfg
    import backup as bk
    bdir = tmp_path / "bk"; bdir.mkdir()

    monkeypatch.setattr(cfg, "DB_PATH", str(tmp_path / "no.db"))
    monkeypatch.setenv("TAXOPS_BACKUP_DIR", str(bdir))

    result = bk.run_backup()

    assert result.success is False
    assert result.backup_file is None


# ── BACKUP-5: admin API endpoint ─────────────────────────────────────────────

def test_backup_admin_page_requires_login(client):
    """GET /admin/backup redirects unauthenticated users to /login."""
    resp = client.get("/admin/backup")
    assert resp.status_code in (302, 401)


def test_backup_admin_page_renders_for_logged_in(client_logged_in):
    """GET /admin/backup returns 200 with 'Run backup now' button."""
    resp = client_logged_in.get("/admin/backup")
    assert resp.status_code == 200
    assert b"Run backup now" in resp.data
    assert b"backup_admin" in resp.data or b"Backup" in resp.data


def test_api_backup_run_requires_login(client):
    """POST /api/admin/backup/run returns 401 for unauthenticated callers."""
    resp = client.post("/api/admin/backup/run")
    assert resp.status_code in (302, 401)


def test_api_backup_run_returns_json(client_logged_in, tmp_path, monkeypatch):
    """POST /api/admin/backup/run returns JSON with success/message/backup_file."""
    import backup as bk
    from dataclasses import dataclass

    @dataclass
    class _FakeResult:
        success: bool = True
        message: str = "Backup OK (test.sqlite; 1024 bytes); removed_old_backups=0"
        backup_file: str = str(tmp_path / "test.sqlite")
        size_bytes: int = 1024

    monkeypatch.setattr(bk, "run_backup", lambda: _FakeResult())

    resp = client_logged_in.post("/api/admin/backup/run")
    assert resp.status_code == 200
    data = resp.get_json()
    assert data["success"] is True
    assert "message" in data
    assert "backup_file" in data
    assert data["size_bytes"] == 1024


def test_api_backup_run_returns_500_on_failure(client_logged_in, monkeypatch):
    """POST /api/admin/backup/run returns 500 JSON when backup fails."""
    import backup as bk
    from dataclasses import dataclass

    @dataclass
    class _FailResult:
        success: bool = False
        message: str = "Source database not found"
        backup_file: str = None
        size_bytes: int = None

    monkeypatch.setattr(bk, "run_backup", lambda: _FailResult())

    resp = client_logged_in.post("/api/admin/backup/run")
    assert resp.status_code == 500
    data = resp.get_json()
    assert data["success"] is False
    assert "not found" in data["message"].lower()


def test_backup_admin_route_registered(app):
    """backup_admin and api_admin_backup_run routes are registered."""
    rules = {r.rule for r in app.url_map.iter_rules()}
    assert "/admin/backup" in rules
    assert "/api/admin/backup/run" in rules
