"""Unit tests for scan_agent.selftest (no hardware required)."""
from __future__ import annotations

import json
from unittest import mock

import pytest


def test_selftest_code_rev_constant_matches_server():
    from scan_agent import selftest, server

    assert selftest.REQUIRED_CODE_REV == server.REQUIRED_CODE_REV == "com_sta_v4"


def test_selftest_python_and_imports_report(monkeypatch):
    from scan_agent.selftest import SuiteReport, check_imports, check_python

    report = SuiteReport()
    check_python(report)
    assert any(c.id == "python" and c.ok for c in report.checks)
    check_imports(report)
    # flask + win32com may or may not exist in CI; just ensure checks were added
    ids = {c.id for c in report.checks}
    assert "import_flask" in ids
    assert "import_win32com" in ids


def test_selftest_code_on_disk_sees_com_sta():
    from scan_agent.selftest import SuiteReport, check_code_on_disk

    report = SuiteReport()
    check_code_on_disk(report)
    row = next(c for c in report.checks if c.id == "code_rev_disk")
    assert row.ok, row.detail


def test_selftest_http_health_detects_old_agent(monkeypatch):
    from scan_agent.selftest import SuiteReport, check_http_health
    import scan_agent.selftest as st

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps(
                {
                    "status": "ok",
                    "scanner_found": False,
                    "scanner_error": "CoInitialize has not been called.",
                    "tips": ["CoInitialize has not been called."],
                }
            ).encode()

    monkeypatch.setattr(st.urllib.request, "urlopen", lambda *a, **k: _Resp())
    report = SuiteReport()
    check_http_health(report, token="secret", port=8766)
    by_id = {c.id: c for c in report.checks}
    assert by_id["http_code_rev"].ok is False
    assert by_id["http_no_coinit_error"].ok is False


def test_selftest_http_health_accepts_com_sta_v4(monkeypatch):
    from scan_agent.selftest import SuiteReport, check_http_health
    import scan_agent.selftest as st

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps(
                {
                    "status": "ok",
                    "code_rev": "com_sta_v4",
                    "agent_ok": True,
                    "com_sta": True,
                    "scanner_found": True,
                    "scanner_names": "EPSON ES-500W II",
                }
            ).encode()

    monkeypatch.setattr(st.urllib.request, "urlopen", lambda *a, **k: _Resp())
    report = SuiteReport()
    check_http_health(report, token="secret", port=8766)
    by_id = {c.id: c for c in report.checks}
    assert by_id["http_code_rev"].ok is True
    assert by_id["http_scanner"].ok is True
    assert by_id["http_no_coinit_error"].ok is True


def test_selftest_http_health_rejects_stale_com_sta_v3(monkeypatch):
    from scan_agent.selftest import SuiteReport, check_http_health
    import scan_agent.selftest as st

    class _Resp:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return False

        def read(self):
            return json.dumps(
                {
                    "status": "ok",
                    "code_rev": "com_sta_v3",
                    "agent_ok": True,
                    "scanner_found": False,
                }
            ).encode()

    monkeypatch.setattr(st.urllib.request, "urlopen", lambda *a, **k: _Resp())
    report = SuiteReport()
    check_http_health(report, token="secret", port=8766)
    by_id = {c.id: c for c in report.checks}
    assert by_id["http_code_rev"].ok is False


def test_selftest_main_json_smoke(monkeypatch, capsys):
    from scan_agent import selftest

    monkeypatch.setattr(
        selftest,
        "run_suite",
        lambda **kwargs: selftest.SuiteReport(
            checks=[
                selftest.CheckResult(id="python", title="Python", ok=True),
            ]
        ),
    )
    rc = selftest.main(["--json", "--no-http"])
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["passed"] is True


def test_wizard_scripts_exist():
    from pathlib import Path

    # T:\taxops\tests\thisfile → parents[0]=tests, [1]=taxops, [2]=repo root (T:\)
    root = Path(__file__).resolve().parents[2]
    assert (root / "scan_agent_wizard.bat").is_file(), root
    assert (root / "scan_agent_wizard.ps1").is_file()
    assert (root / "GO_SCAN_AGENT.bat").is_file()
    assert (root / "restart_scan_agent.bat").is_file()
    assert "GO_SCAN_AGENT.bat" in (root / "restart_scan_agent.bat").read_text(
        encoding="utf-8", errors="replace"
    )
