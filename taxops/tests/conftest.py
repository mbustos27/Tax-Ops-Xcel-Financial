"""Shared fixtures. Patch DB before importing ``app`` so tests use a temp database."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

import pytest

from tests.proof_registry import PROOF_BY_TEST_NAME

# Default pytest to relaxed env validation unless TAXOPS_ENV is already pinned (NSSM-heavy CI imports).
os.environ.setdefault("TAXOPS_ENV", "test")

# Application lives in parent of tests/ (flat modules: db, config, app, …)
_TAXOPS_ROOT = Path(__file__).resolve().parent.parent
if str(_TAXOPS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TAXOPS_ROOT))

# Collect call-phase reports for disk logs (pytest_sessionfinish).
_call_reports: list = []


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
    # SEC-1: disable CSRF enforcement in the test client so existing integration
    # tests that POST without a token continue to work.  The CSRF test itself
    # temporarily re-enables enforcement via its own fixture.
    app_module.app.config["WTF_CSRF_ENABLED"] = False
    return app_module.app


@pytest.fixture
def client(app):
    return app.test_client()


@pytest.fixture
def client_logged_in(client, app, monkeypatch: pytest.MonkeyPatch, taxops_db_path: str):
    """Log in via the auth_users table (SEC-2 path) using a test-specific hashed credential."""
    import app as mod
    from db import get_connection
    from werkzeug.security import generate_password_hash

    # Keep legacy globals patched for any code that still inspects them (e.g. admin guards).
    monkeypatch.setattr(mod, "_LOGIN_USER", "__test_user__")
    monkeypatch.setattr(mod, "_LOGIN_PASS", "__test_pass__")

    # Seed a real hashed user so the auth_users path is exercised.
    conn = get_connection(taxops_db_path)
    conn.execute(
        """
        INSERT OR IGNORE INTO auth_users (username, password_hash, display_name, role, is_active, created_at)
        VALUES (?, ?, ?, 'admin', 1, '2025-01-01T00:00:00Z')
        """,
        ("__test_user__", generate_password_hash("__test_pass__"), "Test User"),
    )
    conn.commit()
    conn.close()

    client.post(
        "/login",
        data={"username": "__test_user__", "password": "__test_pass__"},
        follow_redirects=True,
    )
    return client


def _format_verification_lines(passed: list, failed: list, skipped: list) -> list[str]:
    """Shared between terminal output and test_logs/*.txt files."""
    lines: list[str] = []
    lines.append("")
    lines.append("=" * 72)
    lines.append("VERIFICATION - what passed tests proved (behavioral checks)")
    lines.append("=" * 72)

    seen_failed_names: set[str] = set()
    for rep in failed:
        nodeid = getattr(rep, "nodeid", "")
        name = nodeid.split("::")[-1] if "::" in nodeid else nodeid
        if name:
            seen_failed_names.add(name)

    if not passed and not failed:
        lines.append("(no tests executed)")
        lines.append("")
        return lines

    for rep in passed:
        nodeid = getattr(rep, "nodeid", "")
        fname = nodeid.split("::")[-1] if "::" in nodeid else nodeid
        bullets = PROOF_BY_TEST_NAME.get(fname)
        if not bullets:
            lines.append(f"[PASS] {fname}")
            lines.append("       (no proof bullets registered - add to proof_registry.py)")
            lines.append("")
            continue
        lines.append(f"[PASS] {fname}")
        for b in bullets:
            lines.append(f"       - {b}")
        lines.append("")

    for fname in sorted(seen_failed_names):
        lines.append(f"[FAIL] {fname} - see traceback above; assertions spell out expected behavior.")
        lines.append("")

    if skipped:
        lines.append(f"Skipped tests ({len(skipped)}): not counted as behavioral proof.")
    lines.append("=" * 72)
    return lines


def pytest_terminal_summary(terminalreporter, exitstatus, config):  # noqa: ARG001
    """Append a plain-language verification section after normal pytest output."""
    passed = terminalreporter.stats.get("passed", [])
    failed = terminalreporter.stats.get("failed", [])
    skipped = terminalreporter.stats.get("skipped", [])
    tw = terminalreporter._tw
    for line in _format_verification_lines(passed, failed, skipped):
        tw.line(line)


def pytest_sessionstart(session):  # noqa: ARG001
    global _call_reports
    _call_reports = []


def pytest_runtest_logreport(report: pytest.TestReport):
    if report.when != "call":
        return
    _call_reports.append(report)


def pytest_sessionfinish(session, exitstatus):
    """Always write taxops/test_logs/pytest_latest.txt plus a timestamped copy."""
    log_dir = _TAXOPS_ROOT / "test_logs"
    log_dir.mkdir(parents=True, exist_ok=True)

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    passed = [r for r in _call_reports if r.outcome == "passed"]
    failed = [r for r in _call_reports if r.outcome == "failed"]
    skipped = [r for r in _call_reports if r.outcome == "skipped"]

    summary_lines = [
        "TaxOps pytest session log (written automatically on every run)",
        f"Ended (local): {stamp}",
        f"Pytest exit status: {exitstatus}",
        f"Tests collected: {getattr(session, 'testscollected', len(_call_reports))}",
        f"Call outcomes: passed={len(passed)} failed={len(failed)} skipped={len(skipped)}",
        "",
    ]

    ver = _format_verification_lines(passed, failed, skipped)
    fail_detail: list[str] = []
    if failed:
        fail_detail.append("")
        fail_detail.append("FAILURE DETAILS")
        fail_detail.append("-" * 72)
        for rep in failed:
            fail_detail.append(getattr(rep, "nodeid", "?"))
            lr = getattr(rep, "longrepr", None)
            if lr is not None:
                fail_detail.append(str(lr))
            fail_detail.append("")

    full_text = "\n".join(summary_lines + ver + fail_detail)

    latest = log_dir / "pytest_latest.txt"
    latest.write_text(full_text + "\n", encoding="utf-8")

    stamped = log_dir / f"pytest_{stamp}.txt"
    stamped.write_text(full_text + "\n", encoding="utf-8")
