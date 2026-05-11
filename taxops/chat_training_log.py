"""
Append-only log of staff questions on POST /ai/chat for deriving scope-training examples.

Each row includes `classified_intent` from the deterministic chat router (regex + heuristics), and
merged optional planner telemetry from the `/ai/chat` JSON router (`router_mode`, `router_confidence`,
`confidence_abstain`, `row_tool_guard_dropped`, …)—use alongside `classified_intent` to mine ambiguity.

Questions that land in `fallback` are easy to prioritize for new intents / dataplane slices.

Enable with CHAT_TRAINING_LOG_ENABLE=true. Logs may contain client names — restrict file ACLs.

Export: python export_chat_training_tsv.py ; fallback mining: python export_chat_training_tsv.py --intent-fallback-summary
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from pathlib import Path

logger = logging.getLogger(__name__)


def _one_line(text: str, max_len: int) -> str:
    s = " ".join(str(text).split())
    return s[:max_len] if len(s) > max_len else s


def append_staff_ai_chat_question(
    *,
    question: str,
    normalized: str,
    year: int,
    response_payload: dict,
    classified_intent: str,
    scope_classifier_blocked: bool,
    from_answer_cache_hit: bool,
    log_enable: bool,
    log_include_cache_hits: bool,
    log_path: Path,
    telemetry: dict | None = None,
) -> None:
    """Best-effort JSONL append; never raises to callers."""
    if not log_enable:
        return
    if from_answer_cache_hit and not log_include_cache_hits:
        return
    tu = response_payload.get("tool_used")

    record = {
        "ts": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "question": _one_line(question, 900),
        "normalized": _one_line(normalized, 400),
        "year": year,
        "classified_intent": _one_line(classified_intent, 120),
        "from_cache": from_answer_cache_hit,
        "scope_classifier_blocked": scope_classifier_blocked,
        "tool_used": tu if tu is None else str(tu),
        "fast_path": response_payload.get("fast"),
        "cached_flag": response_payload.get("cached"),
    }
    extra = telemetry or {}
    for k_raw, val in extra.items():
        rk = _one_line(str(k_raw).strip(), 96)
        if rk:
            record[rk] = val
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with open(log_path, "a", encoding="utf-8") as fh:
            fh.write(json.dumps(record, ensure_ascii=False) + "\n")
    except OSError as e:
        logger.warning("AI chat training log write failed: %s", e)
