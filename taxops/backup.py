"""BACKUP-5: importable wrapper around scripts/nightly_backup_db.py for on-demand backup.

Exposes ``run_backup()`` so the Flask admin endpoint can trigger a backup without
forking a subprocess.  The nightly Task Scheduler job calls the script directly.
"""
from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from pathlib import Path

_SCRIPT = Path(__file__).parent / "scripts" / "nightly_backup_db.py"


def _load_backup_script():
    """Load scripts/nightly_backup_db.py as a module (isolates its sys.path surgery)."""
    spec = importlib.util.spec_from_file_location("_nightly_backup_mod", _SCRIPT)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load backup script: {_SCRIPT}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@dataclass
class BackupResult:
    success: bool
    message: str
    backup_file: str | None
    size_bytes: int | None


def run_backup(db_path: str | None = None) -> BackupResult:
    """Run a single backup cycle (backup + retention cleanup + error log).

    Safe to call from a request thread — the underlying SQLite backup() call
    uses WAL and is compatible with concurrent readers/writers.

    ``db_path`` is optional and defaults to ``config.DB_PATH`` resolved at
    call time inside ``nightly_backup_db._run_once()`` — the admin endpoint
    never passes it, so on-demand backups behave identically to before.
    """
    mod = _load_backup_script()
    ok, out_path, msg = mod._run_once(db_path=db_path)

    size: int | None = None
    if out_path is not None:
        try:
            size = Path(out_path).stat().st_size
        except OSError:
            pass

    return BackupResult(
        success=ok,
        message=msg,
        backup_file=str(out_path) if out_path else None,
        size_bytes=size,
    )
