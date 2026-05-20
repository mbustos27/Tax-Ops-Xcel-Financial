"""PROD-4 nightly backup (GitHub #92)."""



from __future__ import annotations



import importlib.util

import os

import sqlite3

import subprocess

import sys

import uuid

from pathlib import Path



import pytest



import config as taxops_config





def _backup_script_path() -> Path:

    return Path(__file__).resolve().parents[1] / "scripts" / "nightly_backup_db.py"





def load_nightly_backup_script() -> object:

    """Load scripts/nightly_backup_db.py fresh (isolates imports from other tests)."""

    script = _backup_script_path()

    mod_name = "_nbackup_test_" + uuid.uuid4().hex

    spec = importlib.util.spec_from_file_location(mod_name, script)

    assert spec and spec.loader

    mod = importlib.util.module_from_spec(spec)

    spec.loader.exec_module(mod)

    return mod





@pytest.fixture()

def backup_mod():

    return load_nightly_backup_script()





def test_retention_cleanup_deletes_old_backups(backup_mod, tmp_path):

    bk = tmp_path / "bk"

    bk.mkdir()

    cutoff_now = 1_000_000_000.0



    stale = bk / "taxops_backup_stale.sqlite"

    stale.write_bytes(b"x")

    os.utime(stale, (cutoff_now - 100 * 86400, cutoff_now - 100 * 86400))



    fresh = bk / "taxops_backup_fresh.sqlite"

    fresh.write_bytes(b"y")

    os.utime(fresh, (cutoff_now - 1 * 86400, cutoff_now - 1 * 86400))



    def fake_now():

        return cutoff_now



    rem, errs = backup_mod.retention_cleanup(bk, retention_days=30, now=fake_now)

    assert errs == []

    assert rem == 1

    assert stale.exists() is False

    assert fresh.exists()





def test_backup_sqlite_live_copies(backup_mod, tmp_path):

    src = tmp_path / "src.db"

    dst = tmp_path / "out" / "copy.sqlite"



    cx = sqlite3.connect(str(src))

    cx.execute("CREATE TABLE t(x INT)")

    cx.execute("INSERT INTO t VALUES (7)")

    cx.commit()

    cx.close()



    backup_mod.backup_sqlite_live(src, dst)

    c2 = sqlite3.connect(str(dst))

    rows = c2.execute("SELECT x FROM t").fetchall()

    c2.close()

    assert rows == [(7,)]





def test_run_once_success_subprocess(tmp_path):

    taxops_root = Path(__file__).resolve().parents[1]



    src = tmp_path / "live.db"

    bk = tmp_path / "bk"

    bk.mkdir()



    cx = sqlite3.connect(str(src))

    cx.execute("CREATE TABLE t(x TEXT)")

    cx.execute("INSERT INTO t VALUES ('ok')")

    cx.commit()

    cx.close()



    env = os.environ.copy()

    env["TAXOPS_DB"] = str(src.resolve())

    env["TAXOPS_BACKUP_DIR"] = str(bk.resolve())



    r = subprocess.run(

        [sys.executable, str(_backup_script_path())],

        cwd=str(taxops_root),

        env=env,

        capture_output=True,

        text=True,

        timeout=120,

        check=False,

    )

    assert r.returncode == 0, (r.stdout, r.stderr)



    backups = list(bk.glob("taxops_backup_*.sqlite"))

    assert len(backups) == 1

    chk = sqlite3.connect(str(backups[0])).execute("SELECT x FROM t").fetchone()

    assert chk == ("ok",)





@pytest.mark.parametrize("hook_set", [True, False])

def test_run_once_missing_db_alerts_optional_webhook(monkeypatch: pytest.MonkeyPatch, tmp_path, hook_set):

    bad = tmp_path / "nope.sqlite"

    assert not bad.exists()



    calls: list[tuple[str, str, str]] = []



    def fake_alert(url: str, subject: str, detail: str) -> None:

        calls.append((url, subject, detail))



    monkeypatch.setattr(taxops_config, "DB_PATH", str(bad))

    if hook_set:

        monkeypatch.setenv("TAXOPS_BACKUP_ALERT_WEBHOOK", "https://example.invalid/webhook")

    else:

        monkeypatch.delenv("TAXOPS_BACKUP_ALERT_WEBHOOK", raising=False)



    mod = load_nightly_backup_script()



    ok, out, msg = mod._run_once(alert_fn=fake_alert)



    assert ok is False and out is None

    assert "not found" in msg.lower()

    assert len(calls) == (1 if hook_set else 0)


