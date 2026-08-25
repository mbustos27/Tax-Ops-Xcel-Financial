"""Hashing and small helpers (no PII logged)."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def stamp_day() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%d")


def sha256_file(path: Path, chunk: int = 1 << 20) -> str:
    h = hashlib.sha256()
    with path.open("rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, default=str)
