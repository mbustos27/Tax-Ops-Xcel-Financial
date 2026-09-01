"""SMOKE-1/2/4 — TaxOps smoke test suite.

Probes every key endpoint after a restart and logs results with a timestamp.

Usage::

    cd taxops
    python smoke_test.py                          # targets http://127.0.0.1:5000
    python smoke_test.py http://192.168.1.173:5000

Exit codes:
    0  all probes passed
    1  one or more probes failed

Log output: C:\\TaxOps\\logs\\smoke.log  (SMOKE-4)
"""

from __future__ import annotations

import datetime
import os
import sys
import urllib.error
import urllib.request

# ── Configuration ─────────────────────────────────────────────────────────────

DEFAULT_BASE = "http://192.168.1.173:5000"
LOG_PATH = r"C:\TaxOps\logs\smoke.log"
TIMEOUT = 20.0

# Endpoints to probe.  accepted_codes = tuple of HTTP status codes considered
# a pass.  Auth-protected pages redirect to /login (302) when not logged in.
ENDPOINTS: list[tuple[str, tuple[int, ...]]] = [
    ("/health",    (200,)),
    ("/",          (200, 302)),
    ("/login",     (200,)),
    ("/review",    (200, 302)),
    ("/payments",  (200, 302)),
    ("/export",    (200, 302)),  # 302 login redirect; must not 500
]


# ── Probe helpers ─────────────────────────────────────────────────────────────

def _probe(base: str, path: str, accepted: tuple[int, ...]) -> str | None:
    """Return None on pass, error string on fail."""
    url = base.rstrip("/") + path
    try:
        # do NOT follow redirects — we want the raw status code
        req = urllib.request.Request(url)
        opener = urllib.request.build_opener(urllib.request.HTTPRedirectHandler())
        # Monkey-patch: disable redirect following by raising on 3xx
        class _NoRedirect(urllib.request.HTTPRedirectHandler):
            def redirect_request(self, *_a, **_kw):  # noqa: ANN002
                return None

        no_redir_opener = urllib.request.build_opener(_NoRedirect())
        try:
            with no_redir_opener.open(req, timeout=TIMEOUT) as resp:
                code = getattr(resp, "status", resp.getcode())
        except urllib.error.HTTPError as exc:
            code = exc.code
    except urllib.error.URLError as exc:
        return f"GET {path} unreachable: {exc.reason}"
    except Exception as exc:  # noqa: BLE001
        return f"GET {path} error: {exc}"

    if code not in accepted:
        return f"GET {path} expected {accepted} got {code}"
    return None


def run_smoke(base: str) -> list[str]:
    """Return list of error strings (empty = all passed)."""
    errors: list[str] = []
    for path, accepted in ENDPOINTS:
        err = _probe(base, path, accepted)
        if err:
            errors.append(err)
    return errors


# ── Logging (SMOKE-4) ─────────────────────────────────────────────────────────

def _log(lines: list[str]) -> None:
    """Append timestamped results to LOG_PATH, creating dirs as needed."""
    try:
        os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
        ts = datetime.datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
        with open(LOG_PATH, "a", encoding="utf-8") as fh:
            for line in lines:
                fh.write(f"{ts}  {line}\n")
    except OSError:
        pass  # log failure must never break the caller


# ── Entry point ───────────────────────────────────────────────────────────────

def main(argv: list[str]) -> int:
    base = (argv[1] if len(argv) > 1 else DEFAULT_BASE).strip()
    errors = run_smoke(base)

    if errors:
        summary = f"SMOKE FAIL  target={base}  failures={len(errors)}"
        print(summary)
        for e in errors:
            print(f"  - {e}")
        _log([summary] + [f"  - {e}" for e in errors])
        return 1

    summary = f"SMOKE OK    target={base}  endpoints={len(ENDPOINTS)}"
    print(summary)
    _log([summary])
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
