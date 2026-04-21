from __future__ import annotations

import hashlib
from datetime import datetime, timezone


def hash_file(path: str) -> str:
    sha = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    return sha.hexdigest()


def safe_str(value) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text if text else None


def now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")
