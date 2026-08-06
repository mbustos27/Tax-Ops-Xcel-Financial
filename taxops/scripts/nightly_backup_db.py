"""PROD-4 — nightly SQLite backup with retention and optional failure webhook (#92).

BACKUP-1: creates C:\\TaxOps\\backups\\taxops_backup_<stamp>.sqlite (set via TAXOPS_BACKUP_DIR).
BACKUP-3: removes files older than TAXOPS_BACKUP_RETENTION_DAYS (default 30) each run.
BACKUP-4: writes backup_error.log and logs WARNING on any failure; optional webhook alert.

Run from Scheduled Task / cron with WorkingDirectory = ``taxops/``::

    python scripts/nightly_backup_db.py

Linux example for ``cron`` (adjust paths and owning user permissions)::

    15 2 * * * cd /opt/TaxOps/taxops && .venv/bin/python scripts/nightly_backup_db.py >> /var/log/taxops-backup.log 2>&1

Uses SQLite online backup API (consistent snapshot while the app may hold the DB open).
Loads ``config`` so ``TAXOPS_DB``, repo-root ``.env``, and ``taxops/.env`` behave like the service.
"""

from __future__ import annotations

import json
import logging
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable
from urllib.error import URLError
from urllib.request import Request, urlopen

import sqlite3

_ROOT = Path(__file__).resolve().parents[1]
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config

log = logging.getLogger(__name__)


BACKUP_FILENAME_PREFIX = "taxops_backup_"
BACKUP_SUFFIX = ".sqlite"


def _backup_dir_default() -> Path:
    raw = os.environ.get("TAXOPS_BACKUP_DIR", "").strip()
    if raw:
        return Path(raw).expanduser()
    # Windows production default matches BACKUP-1 spec: C:\TaxOps\backups\
    win_default = Path(r"C:\TaxOps\backups")
    if sys.platform == "win32" and win_default.parent.exists():
        return win_default
    return _ROOT / "data" / "backups"


def _error_log_path() -> Path:
    """BACKUP-4: path for backup_error.log (next to backup dir, or in taxops/logs/)."""
    raw = os.environ.get("TAXOPS_BACKUP_ERROR_LOG", "").strip()
    if raw:
        return Path(raw).expanduser()
    bdir = _backup_dir_default()
    return bdir / "backup_error.log"


def _write_error_log(msg: str) -> None:
    """BACKUP-4: append a timestamped failure line to backup_error.log."""
    try:
        log_path = _error_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        stamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        line = f"[{stamp}] BACKUP FAILED: {msg}\n"
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(line)
    except OSError as exc:
        print(f"WARNING: could not write backup_error.log: {exc}", file=sys.stderr)


def _retention_days() -> int:
    try:
        d = int(os.environ.get("TAXOPS_BACKUP_RETENTION_DAYS", "30"))
    except ValueError:
        return 30
    return max(1, min(3660, d))


def backup_sqlite_live(src: Path, dest: Path) -> None:
    """Copy SQLite via ``sqlite3.Connection.backup`` (WAL-aware, hot-safe under normal workloads)."""
    dest.parent.mkdir(parents=True, exist_ok=True)
    if dest.exists():
        dest.unlink()

    dst_conn: sqlite3.Connection | None = None
    src_conn = sqlite3.connect(str(src), timeout=60.0)
    try:
        src_conn.execute("PRAGMA busy_timeout=60000;")
        dst_conn = sqlite3.connect(str(dest), timeout=60.0)
        dst_conn.execute("PRAGMA busy_timeout=60000;")
        src_conn.backup(dst_conn)
        dst_conn.commit()
    finally:
        if dst_conn is not None:
            dst_conn.close()
        src_conn.close()


def retention_cleanup(
    backup_dir: Path,
    *,
    retention_days: int,
    now: Callable[[], float] | None = None,
    globber: Callable[[Path, str], list[Path]] | None = None,
) -> tuple[int, list[str]]:
    """Remove backups older than retention window. Returns ``(removed_count, errors)``."""

    mono = now or time.time
    glob_pat = BACKUP_FILENAME_PREFIX + "*" + BACKUP_SUFFIX

    def _glob(p: Path, pattern: str) -> list[Path]:
        return sorted(p.glob(pattern))

    finder = globber or _glob

    cutoff = mono() - max(1, retention_days) * 86400
    removed = 0
    errors: list[str] = []
    for fp in finder(backup_dir, glob_pat):
        try:
            if not fp.is_file():
                continue
            try:
                mtime = fp.stat().st_mtime
            except OSError as exc:
                errors.append(f"{fp}: stat failed: {exc}")
                continue
            if mtime < cutoff:
                try:
                    fp.unlink()
                    removed += 1
                except OSError as exc:
                    errors.append(f"{fp}: unlink failed: {exc}")
        except OSError as exc:
            errors.append(f"{fp}: scan failed: {exc}")
    return removed, errors


def alert_failure_webhook(webhook_url: str, *, subject: str, detail: str) -> None:
    """POST JSON alert; Slack Incoming Webhooks accept ``{"text": "..."}``."""
    body: dict[str, Any] = {
        "text": f"{subject}\n{detail}",
        "event": "taxops_backup_failed",
        "detail": detail,
    }
    data = json.dumps(body, ensure_ascii=False).encode("utf-8")
    req = Request(
        webhook_url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    urlopen(req, timeout=30)


_AlertCaller = Callable[[str, str, str], None]


def _alert_default(webhook_url: str, subject: str, detail: str) -> None:
    alert_failure_webhook(webhook_url, subject=subject, detail=detail)


def _run_once(
    *,
    db_path: str | None = None,
    alert_fn: _AlertCaller | None = None,
) -> tuple[bool, Path | None, str]:
    """Returns ``(success, outfile, message)``. ``alert_fn`` injected for testing.

    ``db_path`` defaults to ``config.DB_PATH`` resolved at call time (not at
    module-import time) so callers — including tests — can override the
    source database without relying on environment-variable timing or
    module-reload tricks. Production callers (the nightly Task Scheduler
    invocation and the on-demand admin endpoint via backup.py) never pass
    this and get identical behavior to before: today's ``config.DB_PATH``.
    """
    src = Path(db_path if db_path is not None else config.DB_PATH).resolve()

    webhook = os.environ.get("TAXOPS_BACKUP_ALERT_WEBHOOK", "").strip()

    notify = alert_fn or _alert_default

    if not src.is_file():
        msg = f"Source database not found or not a file: {src}"
        log.warning("BACKUP-4: %s", msg)
        _write_error_log(msg)
        if webhook:
            try:
                notify(webhook, "[TaxOps] PROD-4 nightly DB backup FAILED", msg)
            except (URLError, OSError):
                print("WARNING: webhook alert POST failed after missing DB error", file=sys.stderr)
        return False, None, msg

    backup_root = _backup_dir_default().resolve()
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    dest = backup_root / f"{BACKUP_FILENAME_PREFIX}{stamp}{BACKUP_SUFFIX}"

    try:
        backup_sqlite_live(src, dest)
        size_b = dest.stat().st_size
    except sqlite3.Error as exc:
        msg = f"SQLite backup failed: {exc}"
        log.warning("BACKUP-4: %s", msg)
        _write_error_log(msg)
        if webhook:
            try:
                notify(webhook, "[TaxOps] PROD-4 nightly DB backup FAILED", msg)
            except (URLError, OSError):
                print("WARNING: webhook alert POST failed after backup SQLite error", file=sys.stderr)
        return False, None, msg
    except OSError as exc:
        msg = f"Backup I/O failed: {exc}"
        log.warning("BACKUP-4: %s", msg)
        _write_error_log(msg)
        if webhook:
            try:
                notify(webhook, "[TaxOps] PROD-4 nightly DB backup FAILED", msg)
            except (URLError, OSError):
                print("WARNING: webhook alert POST failed after backup I/O error", file=sys.stderr)
        return False, dest, msg

    try:
        rem, errs = retention_cleanup(backup_root, retention_days=_retention_days())
    except OSError as exc:
        msg = f"Retention scan failed after successful backup ({dest}; {size_b} bytes): {exc}"
        log.warning("BACKUP-4: %s", msg)
        _write_error_log(msg)
        if webhook:
            try:
                notify(webhook, "[TaxOps] PROD-4 nightly DB backup FAILED", msg)
            except (URLError, OSError):
                print("WARNING: webhook alert POST failed after retention scan error", file=sys.stderr)
        return False, dest, msg

    if errs:
        joined = "; ".join(errs)
        msg = (
            f"Backup OK ({dest}; {size_b} bytes); removed_old={rem}; retention_errors={joined}"
        )
        log.warning("BACKUP-4: %s", msg)
        _write_error_log(msg)
        if webhook:
            try:
                notify(webhook, "[TaxOps] PROD-4 nightly backup retention FAILED", msg)
            except (URLError, OSError):
                print("WARNING: webhook alert POST failed after retention errors", file=sys.stderr)
        return False, dest, msg

    return True, dest, f"Backup OK ({dest}; {size_b} bytes); removed_old_backups={rem}"


def main() -> int:
    ok, _out, msg = _run_once()
    print(msg)
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
