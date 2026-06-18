"""PROD-8 smoke_deploy.py unit tests."""

from __future__ import annotations

import json
from io import BytesIO
from unittest.mock import patch

import urllib.error

import importlib.util
from pathlib import Path

_SCRIPT = Path(__file__).resolve().parents[1] / "scripts" / "smoke_deploy.py"
_SPEC = importlib.util.spec_from_file_location("_taxops_smoke_deploy_testmod", _SCRIPT)
assert _SPEC and _SPEC.loader
_MOD = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_MOD)

probe_health = _MOD.probe_health
run_checks = _MOD.run_checks


class _OkResp200:
    def __enter__(self):
        return self

    def __exit__(self, *args):
        return None

    status = 200

    def getcode(self):
        return 200

    def read(self):
        return json.dumps(
            {"status": "ok", "db": {"ok": True}, "uptime_seconds": 1, "version": "test"}
        ).encode()


def test_probe_health_ok():
    with patch("urllib.request.urlopen", return_value=_OkResp200()):
        assert probe_health("http://127.0.0.1:5000") == []


def test_probe_health_requires_ok_status():
    class Bad:
        def __enter__(self):
            return self

        def __exit__(self, *a):
            return None

        status = 200

        def getcode(self):
            return 200

        def read(self):
            return b'{"status":"degraded","db":{"ok":false}}'

    with patch("urllib.request.urlopen", return_value=Bad()):
        err = probe_health("http://h")
        assert len(err) >= 1
        assert any('not "ok"' in e for e in err)


def test_probe_health_503_reports_payload():
    fp = BytesIO(b'{"status":"degraded","db":{"ok":false}}')

    def opener(req, timeout=20):  # noqa: ARG001
        raise urllib.error.HTTPError(url=req.full_url, code=503, msg="n", hdrs={}, fp=fp)

    with patch("urllib.request.urlopen", side_effect=opener):
        errs = probe_health("http://h")
        assert any("503" in e for e in errs)


def test_run_checks_detects_bad_static():
    calls = []

    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        url = req.full_url
        calls.append(url)
        if "/health" in url:
            return _OkResp200()
        if "/login" in url:

            class R:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return None

                status = 200

                def getcode(self):
                    return 200

                def read(self):
                    return b"""<html><head>
<link href="/static/tw.min.css?v=abc123" rel="stylesheet"/>
<link href="/static/app.css?v=abc123" rel="stylesheet"/>
</head><body><form>sign in</form></body></html>"""

            return R()
        if "/static/app.js" in url:

            class Junk:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return None

                status = 200

                def getcode(self):
                    return 200

                def read(self):
                    return b"xxx " * 120

            return Junk()
        raise AssertionError(f"unexpected url {url!r}")

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        errs = run_checks("http://test.local")
        assert any("static/app.js" in e.lower() or "javascript" in e.lower() or "boilerplate" in e.lower() for e in errs)


def test_run_checks_requires_versioned_css_hrefs_on_login():
    def fake_urlopen(req, timeout=20):  # noqa: ARG001
        url = req.full_url
        if "/health" in url:
            return _OkResp200()
        if "/login" in url:

            class R:
                def __enter__(self):
                    return self

                def __exit__(self, *a):
                    return None

                status = 200

                def getcode(self):
                    return 200

                def read(self):
                    return b"<html><head><link href='/static/app.css'/></head><body><form>sign in</form></body></html>"

            return R()
        raise AssertionError(url)

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        errs = run_checks("http://test.local")
        assert any("cache-bust" in e.lower() for e in errs)