"""PROD-2: optional structured JSON lines + rotating file; level via TAXOPS_LOG_LEVEL."""

from __future__ import annotations

import json
import logging
import sys
from datetime import datetime, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any

_configured = False


class JsonLinesFormatter(logging.Formatter):
    """One JSON object per line (newline-terminated), UTF-8."""

    def format(self, record: logging.LogRecord) -> str:
        ts = datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat()
        if ts.endswith("+00:00"):
            ts = ts[:-6] + "Z"
        payload: dict[str, Any] = {
            "ts": ts,
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
            "module": record.module,
            "lineno": record.lineno,
        }
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info).rstrip("\n")
        return json.dumps(payload, ensure_ascii=False)


def configure_logging(force: bool = False) -> None:
    """Attach root handlers once: optional rotating JSON file + stderr (plain line)."""
    global _configured
    if sys.modules.get("pytest") is not None:
        return
    if _configured and not force:
        return

    from config import (
        LOG_CONSOLE_ENABLED,
        LOG_JSON_BACKUP_COUNT,
        LOG_JSON_MAX_BYTES,
        LOG_JSON_PATH,
        LOG_LEVEL_INT,
        LOG_LEVEL_STR,
    )

    root = logging.getLogger()
    root.setLevel(LOG_LEVEL_INT)

    if not LOG_JSON_PATH and not LOG_CONSOLE_ENABLED:
        logging.basicConfig(
            level=LOG_LEVEL_INT,
            format="%(levelname)s %(name)s %(message)s",
            stream=sys.stderr,
            force=False,
        )
        _configured = True
        return

    root.handlers.clear()
    fmt_console = logging.Formatter("%(levelname)s %(name)s %(message)s")

    if LOG_JSON_PATH:
        Path(LOG_JSON_PATH).parent.mkdir(parents=True, exist_ok=True)
        fh = RotatingFileHandler(
            str(LOG_JSON_PATH),
            maxBytes=LOG_JSON_MAX_BYTES,
            backupCount=max(0, LOG_JSON_BACKUP_COUNT),
            encoding="utf-8",
            delay=True,
        )
        fh.setLevel(LOG_LEVEL_INT)
        fh.setFormatter(JsonLinesFormatter())
        root.addHandler(fh)

    if LOG_CONSOLE_ENABLED:
        ch = logging.StreamHandler(sys.stderr)
        ch.setLevel(LOG_LEVEL_INT)
        ch.setFormatter(fmt_console)
        root.addHandler(ch)

    logging.captureWarnings(True)
    logging.getLogger("werkzeug").setLevel(LOG_LEVEL_INT)
    logging.getLogger("flask.app").setLevel(LOG_LEVEL_INT)

    _configured = True
