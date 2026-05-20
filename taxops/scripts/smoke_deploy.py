"""PROD-8 — post-deploy smoke checks (Epic #82 / GitHub #96).

Exits non-zero if any probe fails::

    cd path/to/taxops
    python scripts/smoke_deploy.py
    python scripts/smoke_deploy.py http://192.168.1.50:5000

NSSM/Task Scheduler typically runs against ``http://127.0.0.1:PORT`` after restart.
Also validates **cache-busted** static URLs on ``/login`` and fetches **versioned**
``/static/app.js?v=…`` (CACHE / GitHub #141–145).
Requires the server reachable from this machine — no Flask import (safe for ops-only hosts).
"""

from __future__ import annotations

import json
import re
import sys
import urllib.error
import urllib.request


def probe_health(base_url: str, timeout_sec: float = 20.0) -> list[str]:
    errs: list[str] = []
    url = base_url.rstrip("/") + "/health"
    try:
        req = urllib.request.Request(url, headers={"Accept": "application/json"})
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = getattr(resp, "status", resp.getcode())
            raw = resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        body = ""
        try:
            body = e.read().decode("utf-8", errors="replace")
        except OSError:
            pass
        if e.code == 503:
            errs.append("GET /health -> HTTP 503 (degraded / DB check failed)")
            if body.strip().startswith("{"):
                try:
                    data = json.loads(body)
                    errs.append(f"… payload: status={data.get('status')!r} db={data.get('db')}")
                except json.JSONDecodeError:
                    errs.append(f"… body: {body[:200]}")
        else:
            errs.append(f"GET /health -> HTTP {e.code} ({e.reason})")
        return errs
    except Exception as exc:
        return [f"GET /health unreachable ({exc!s})"]

    if code != 200:
        errs.append(f"GET /health expected 200 got {code}")
        return errs

    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        errs.append("/health returned non-JSON")
        return errs

    status = data.get("status")
    if status != "ok":
        errs.append(f'/health JSON "status" is {status!r} not "ok" (db check failed?)')
        if data.get("db"):
            errs.append(f"… db payload: {data.get('db')}")
    return errs


def probe_login_page(base_url: str, timeout_sec: float = 20.0) -> tuple[list[str], str | None]:
    errs: list[str] = []
    url = base_url.rstrip("/") + "/login"
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = getattr(resp, "status", resp.getcode())
            raw = resp.read().decode("utf-8", errors="replace")
            body_lower = raw.lower()
    except Exception as exc:
        return [f"GET /login unreachable ({exc!s})"], None

    if code != 200:
        errs.append(f"GET /login expected 200 got {code}")
        return errs, None
    if "sign in" not in body_lower and "<form" not in body_lower:
        errs.append("GET /login body missing expected sign-in markup")

    # CACHE — Jinja emits ``?v=`` on static assets; omission risks stale bundles after deploy.
    for label, pat in (
        ("tw.min.css cache-bust (?v=)", r"static/tw\.min\.css\?v=[^\s\"'&<>]+"),
        ("app.css cache-bust (?v=)", r"static/app\.css\?v=[^\s\"'&<>]+"),
    ):
        if not re.search(pat, raw, re.IGNORECASE):
            errs.append(f"GET /login HTML missing {label}")

    return errs, raw


def probe_versioned_static_app_js(base_url: str, login_html: str, timeout_sec: float = 20.0) -> list[str]:
    """Same ``v`` token as login page must successfully fetch ``/static/app.js``."""
    errs: list[str] = []
    m = re.search(r"static/app\.css\?v=([^\s\"'&<>]+)", login_html, re.IGNORECASE)
    if not m:
        errs.append("could not parse app.css ?v= token from /login HTML")
        return errs
    token = m.group(1)
    js_url = base_url.rstrip("/") + "/static/app.js?v=" + token
    try:
        req = urllib.request.Request(js_url)
        with urllib.request.urlopen(req, timeout=timeout_sec) as resp:
            code = getattr(resp, "status", resp.getcode())
            sniff = resp.read(500).decode("utf-8", errors="replace")
    except Exception as exc:
        return [f"GET versioned /static/app.js failed ({exc!s})"]

    if code != 200:
        errs.append(f"GET versioned /static/app.js expected 200 got {code}")
    elif not re.search(r"\bfunction\b|=>", sniff):
        errs.append("versioned /static/app.js missing expected JS boilerplate")

    return errs


def run_checks(base_url: str) -> list[str]:
    """Return aggregated error strings; empty → success."""
    out: list[str] = []
    out.extend(probe_health(base_url))
    if out:
        return out
    login_errs, login_html = probe_login_page(base_url)
    out.extend(login_errs)
    if out or not login_html:
        return out
    out.extend(probe_versioned_static_app_js(base_url, login_html))
    return out


def main(argv: list[str]) -> int:
    base = (argv[1] if len(argv) > 1 else "http://127.0.0.1:5000").strip()
    errs = run_checks(base)
    if errs:
        print(f"Smoke test FAILED targeting {base!r}")
        for line in errs:
            print(f"  - {line}")
        return 1
    print(f"Smoke test OK ({base})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
