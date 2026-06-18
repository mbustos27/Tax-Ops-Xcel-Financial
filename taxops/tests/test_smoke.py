"""Tests for smoke_test.py (SMOKE-1 through SMOKE-4)."""

from __future__ import annotations

import importlib.util
import os
import sys
import tempfile
import urllib.error
from pathlib import Path
from unittest.mock import patch

# Load smoke_test module without running main()
_SCRIPT = Path(__file__).resolve().parents[1] / "smoke_test.py"
_SPEC = importlib.util.spec_from_file_location("_taxops_smoke_test_mod", _SCRIPT)
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)

run_smoke = _MOD.run_smoke
ENDPOINTS = _MOD.ENDPOINTS


# ── Fake HTTP response helpers ────────────────────────────────────────────────

class _FakeResp:
    def __init__(self, code: int):
        self._code = code

    def __enter__(self):
        return self

    def __exit__(self, *_):
        return None

    @property
    def status(self):
        return self._code

    def getcode(self):
        return self._code

    def read(self, _n=-1):
        return b""


def _make_opener(mapping: dict[str, int]):
    """Return a urlopen side-effect that resolves paths → status codes."""
    def fake_open(req, timeout=20):  # noqa: ARG001
        url = getattr(req, "full_url", str(req))
        for path, code in mapping.items():
            if path in url:
                if code in (301, 302, 303):
                    raise urllib.error.HTTPError(url, code, "Redirect", {}, None)
                return _FakeResp(code)
        raise AssertionError(f"Unexpected URL in smoke test: {url!r}")
    return fake_open


# ── SMOKE-1: all required endpoints are probed ────────────────────────────────

def test_all_required_endpoints_covered():
    paths = {ep[0] for ep in ENDPOINTS}
    for required in ("/health", "/", "/login", "/review", "/payments", "/ai/status"):
        assert required in paths, f"{required!r} missing from ENDPOINTS"


# ── SMOKE-2: 200 accepted, non-zero exit on failure ──────────────────────────

def test_all_200_passes():
    mapping = {ep[0]: 200 for ep in ENDPOINTS}
    with patch.object(_MOD, "_probe", side_effect=lambda base, path, acc: None):
        errs = run_smoke("http://127.0.0.1:5000")
    assert errs == []


def test_unreachable_endpoint_fails():
    def bad_probe(base, path, accepted):
        if path == "/health":
            return f"GET {path} unreachable: connection refused"
        return None

    with patch.object(_MOD, "_probe", side_effect=bad_probe):
        errs = run_smoke("http://127.0.0.1:5000")
    assert len(errs) == 1
    assert "/health" in errs[0]


def test_wrong_status_fails():
    def probe_500(base, path, accepted):
        return f"GET {path} expected {accepted} got 500"

    with patch.object(_MOD, "_probe", side_effect=probe_500):
        errs = run_smoke("http://x")
    assert len(errs) == len(ENDPOINTS)


def test_main_returns_1_on_failure(tmp_path):
    with patch.object(_MOD, "run_smoke", return_value=["GET /health unreachable"]):
        with patch.object(_MOD, "_log"):
            rc = _MOD.main(["smoke_test.py", "http://x"])
    assert rc == 1


def test_main_returns_0_on_success():
    with patch.object(_MOD, "run_smoke", return_value=[]):
        with patch.object(_MOD, "_log"):
            rc = _MOD.main(["smoke_test.py", "http://x"])
    assert rc == 0


# ── SMOKE-2: 302 is accepted for auth-protected pages ─────────────────────────

def test_302_accepted_for_protected_pages():
    """Auth-protected HTML pages redirect to /login — that must be a pass."""
    def probe_302_for_protected(base, path, accepted):
        if path in ("/review", "/payments", "/"):
            if 302 not in accepted:
                return f"GET {path} expected {accepted} got 302"
        return None

    with patch.object(_MOD, "_probe", side_effect=probe_302_for_protected):
        errs = run_smoke("http://x")
    assert errs == []


def test_401_accepted_for_ai_status():
    """/ai/status is an API endpoint — returns 401 when unauthenticated."""
    def probe_401(base, path, accepted):
        if path == "/ai/status":
            if 401 not in accepted:
                return f"GET {path} expected {accepted} got 401"
        return None

    with patch.object(_MOD, "_probe", side_effect=probe_401):
        errs = run_smoke("http://x")
    assert errs == []


# ── SMOKE-4: log file written with timestamp ──────────────────────────────────

def test_log_creates_file_and_appends(tmp_path):
    log_file = tmp_path / "smoke.log"

    with patch.object(_MOD, "LOG_PATH", str(log_file)):
        _MOD._log(["SMOKE OK    target=http://127.0.0.1:5000  endpoints=6"])
        _MOD._log(["SMOKE FAIL  target=http://127.0.0.1:5000  failures=1", "  - GET /health unreachable"])

    lines = log_file.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    assert "SMOKE OK" in lines[0]
    assert "SMOKE FAIL" in lines[1]
    assert "/health" in lines[2]
    # Each line starts with a timestamp (YYYY-MM-DDTHH:MM:SS)
    import re
    for line in lines:
        assert re.match(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", line)


def test_log_missing_dir_does_not_raise(tmp_path):
    bad_path = str(tmp_path / "nonexistent" / "dir" / "smoke.log")
    with patch.object(_MOD, "LOG_PATH", bad_path):
        _MOD._log(["test"])  # must not raise even if makedirs fails in odd envs


def test_log_write_failure_does_not_raise(tmp_path):
    with patch("builtins.open", side_effect=OSError("disk full")):
        _MOD._log(["should not raise"])


# ── Integration: _probe accepts 302 without following redirect ────────────────

def test_probe_302_treated_as_pass():
    def fake_open(req, timeout=20):  # noqa: ARG001
        raise urllib.error.HTTPError(req.full_url, 302, "Found", {}, None)

    opener_mock = type("O", (), {"open": staticmethod(fake_open)})()
    with patch("urllib.request.build_opener", return_value=opener_mock):
        result = _MOD._probe("http://x", "/review", (200, 302))
    assert result is None


def test_probe_500_treated_as_fail():
    def fake_open(req, timeout=20):  # noqa: ARG001
        raise urllib.error.HTTPError(req.full_url, 500, "Server Error", {}, None)

    opener_mock = type("O", (), {"open": staticmethod(fake_open)})()
    with patch("urllib.request.build_opener", return_value=opener_mock):
        result = _MOD._probe("http://x", "/health", (200,))
    assert result is not None
    assert "500" in result
