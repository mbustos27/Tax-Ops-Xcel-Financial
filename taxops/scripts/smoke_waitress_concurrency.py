"""WSGI-3 / GitHub #139 — concurrent GET /health probes (office verification).

Run against a running TaxOps instance (Waitress or dev server)::

    cd path/to/taxops
    python scripts/smoke_waitress_concurrency.py
    python scripts/smoke_waitress_concurrency.py http://192.168.1.50:5000

Exits 0 if every request returns HTTP 200 JSON with status ok.
"""

from __future__ import annotations

import concurrent.futures
import json
import sys
import urllib.error
import urllib.request


def _probe(url: str, timeout: float) -> None:
    req = urllib.request.Request(url, headers={"Accept": "application/json"})
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        code = getattr(resp, "status", resp.getcode())
        raw = resp.read().decode("utf-8", errors="replace")
        if code != 200:
            raise RuntimeError(f"HTTP {code}: {raw[:200]}")
        data = json.loads(raw)
        if data.get("status") != "ok":
            raise RuntimeError(f'/health status not ok: {data.get("status")!r}')


def main() -> int:
    base = (sys.argv[1] if len(sys.argv) > 1 else "http://127.0.0.1:5000").rstrip("/")
    url = base + "/health"
    rounds = 40
    workers = 16
    timeout = 30.0

    errors: list[str] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
        futures = [pool.submit(_probe, url, timeout) for _ in range(rounds)]
        for i, fut in enumerate(concurrent.futures.as_completed(futures), start=1):
            try:
                fut.result()
            except Exception as exc:  # noqa: BLE001 — aggregate probe failures
                errors.append(str(exc))

    if errors:
        print(f"FAIL: {len(errors)}/{rounds} requests raised errors:")
        for e in errors[:10]:
            print(" ", e)
        if len(errors) > 10:
            print(f"  … plus {len(errors) - 10} more")
        return 1

    print(f"OK: {rounds} concurrent GET /health completed (workers={workers})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
