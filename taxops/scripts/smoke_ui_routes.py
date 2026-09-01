"""Smoke key TaxOps UI routes (logged-in) against the configured DB."""

from __future__ import annotations

import os
import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
os.environ.setdefault("TAXOPS_DB", str(_TAXOPS / "taxops.db"))
sys.path.insert(0, str(_TAXOPS))

import app as taxops_app  # noqa: E402
from db import get_connection  # noqa: E402
from werkzeug.security import generate_password_hash  # noqa: E402

taxops_app.app.config["WTF_CSRF_ENABLED"] = False
taxops_app.app.config["TESTING"] = True

ROUTES = [
    ("GET", "/", (200,)),
    ("GET", "/export?year=2025", (200,)),
    ("GET", "/export/upcoming-deadlines/print?year=2025", (200,)),
    ("GET", "/api/dashboard/status-counts?year=2025", (200,)),
    ("GET", "/review", (200, 302)),
    ("GET", "/payments", (200, 302)),
    ("GET", "/email-inbox", (200, 302)),
    ("GET", "/efile-queue", (200, 302)),
    ("GET", "/health", (200,)),
]


def main() -> int:
    conn = get_connection()
    try:
        conn.execute(
            """
            INSERT OR IGNORE INTO auth_users
              (username, password_hash, display_name, role, is_active, created_at)
            VALUES (?, ?, ?, 'admin', 1, '2025-01-01T00:00:00Z')
            """,
            ("__smoke_user__", generate_password_hash("__smoke_pass__"), "Smoke User"),
        )
        conn.commit()
    finally:
        conn.close()

    client = taxops_app.app.test_client()
    login = client.post(
        "/login",
        data={"username": "__smoke_user__", "password": "__smoke_pass__"},
        follow_redirects=True,
    )
    if login.status_code >= 400:
        print(f"LOGIN FAIL {login.status_code}")
        return 1

    fails = []
    for method, path, accepted in ROUTES:
        rv = client.open(path, method=method)
        ok = rv.status_code in accepted
        note = ""
        if path.startswith("/export?") and rv.status_code == 200:
            ct = rv.content_type or ""
            if "spreadsheetml" in ct or rv.data[:2] == b"PK":
                note = "xlsx"
            elif "csv" in ct:
                note = "csv-fallback"
            else:
                note = f"ct={ct!r}"
        mark = "OK" if ok else "FAIL"
        print(f"  [{mark}] {method} {path} -> {rv.status_code} {note}")
        if not ok:
            fails.append(f"{path} got {rv.status_code}")

    if fails:
        print("FAILURES:")
        for f in fails:
            print(" ", f)
        return 1
    print("ALL ROUTE SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
