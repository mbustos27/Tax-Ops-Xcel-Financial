"""filetrack.listener.parser — pure classification of one raw scan record.

No I/O. Strips the scanner's terminator (suffix) if present, then any
incidental whitespace/line-ending noise, then splits on the locked
`PREFIX:VALUE` scheme (filetrack.config.PREFIX_DELIMITER).
"""
from __future__ import annotations

from filetrack.config import LOG_PREFIX, PREFIX_DELIMITER, STATUS_PREFIX

RECORD_KIND_STATUS = "status"
RECORD_KIND_LOG = "log"
RECORD_KIND_UNKNOWN = "unknown"


def classify(raw: str, *, suffix: str = "") -> tuple[str, str]:
    """Classify one raw scan record.

    Returns (kind, value):
      ('status', NAME)   — a STATUS:<NAME> scan; NAME is returned verbatim
                            (whitespace-trimmed) — validity against the
                            allowed set is state.py's job, not this one's.
      ('log', NUMBER)    — a LOG:<number> scan; NUMBER is returned verbatim.
      ('unknown', text)  — anything else: no recognized prefix, or empty
                            after stripping. `text` is the cleaned-but-
                            unparsed record, for logging.

    `suffix` is the scanner's confirmed terminator (e.g. "\\r\\n") — see
    filetrack.config.DEFAULT_SCAN_SUFFIX, PENDING M0 confirmation. Only the
    exact trailing `suffix` is stripped; general whitespace/CR/LF/TAB is
    ALWAYS additionally stripped regardless of `suffix`, so a caller that
    gets the suffix slightly wrong (e.g. "\\n" when the scanner actually
    sends "\\r\\n") still parses correctly — this is the tolerance the M2
    spec asks for.
    """
    if raw is None:
        return (RECORD_KIND_UNKNOWN, "")

    text = raw
    if suffix and text.endswith(suffix):
        text = text[: -len(suffix)]
    text = text.strip("\r\n\t ")

    if not text:
        return (RECORD_KIND_UNKNOWN, "")

    if PREFIX_DELIMITER not in text:
        return (RECORD_KIND_UNKNOWN, text)

    prefix, _, value = text.partition(PREFIX_DELIMITER)
    prefix = prefix.strip()
    value = value.strip()

    if prefix == LOG_PREFIX:
        return (RECORD_KIND_LOG, value)
    if prefix == STATUS_PREFIX:
        return (RECORD_KIND_STATUS, value)
    return (RECORD_KIND_UNKNOWN, text)
