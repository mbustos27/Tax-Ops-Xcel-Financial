#!/usr/bin/env python3
"""
Read JSONL from CHAT_TRAINING_LOG_PATH (or --log) and print proposed TSV lines for
chat_scope_examples.tsv (label + tab + question).

Heuristics (review before merging):
  - scope_classifier_blocked  -> out_scope
  - tool_used truthy           -> in_scope
  - classified_intent=fallback AND no tool_used -> # REVIEW (classifier_fallback_no_tool); tune regex routing
  - else                       -> commented # REVIEW line

Dedupes by normalized question text (latest log row wins).

Usage:
  cd taxops
  python export_chat_training_tsv.py > proposed_scope_rows.tsv
  python export_chat_training_tsv.py --intent-fallback-summary   # mine fallback rows from classifier + log

For hands-off pipeline see: python automerge_chat_scope_from_log.py --apply
"""
from __future__ import annotations

import argparse
import json
import os
import sys
from collections import Counter
from pathlib import Path

_PKG = Path(__file__).resolve().parent
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

from utils import normalize_staff_question_key  # noqa: E402
from chat_training_io import (  # noqa: E402
    infer_scope_training_label,
    read_jsonl_latest_by_normalized_question,
    sanitized_question_text,
)
from config import CHAT_TRAINING_LOG_PATH  # noqa: E402


def _print_fallback_intent_summary(log_path: Path) -> None:
    """Grouped rows where classified_intent is fallback or unknown — newest wording per norm key."""
    occ: Counter[str] = Counter()
    last_q_by_norm: dict[str, str] = {}
    with log_path.open(encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            try:
                row = json.loads(raw)
            except json.JSONDecodeError:
                continue
            ci = str(row.get("classified_intent") or "").strip()
            if ci not in ("fallback", "unknown"):
                continue
            q = sanitized_question_text(row.get("question") or "")
            if not q:
                continue
            kn = normalize_staff_question_key(q)
            occ[kn] += 1
            last_q_by_norm[kn] = q
    lines = sorted(occ.items(), key=lambda kv: (-kv[1], kv[0].lower()))
    print("# classified_intent=fallback|unknown — tally / latest wording (prioritize repeats for new intents)\n")
    print("# count\tquestion\n")
    for _nk, cnt in lines:
        print(f"{cnt}\t{last_q_by_norm[_nk]}")
    if not lines:
        print("(no fallback-labeled rows in this log)")
    print(
        "\n# Add patterns / labels in dataplane prompts or NEEDS_LOOKUP flow based on repeats above.",
        file=sys.stderr,
    )


def main() -> None:
    p = argparse.ArgumentParser(description="Propose chat_scope_examples.tsv rows from staff chat JSONL.")
    p.add_argument(
        "--log",
        type=Path,
        default=CHAT_TRAINING_LOG_PATH,
        help="JSONL path (default: CHAT_TRAINING_LOG_PATH)",
    )
    p.add_argument(
        "--intent-fallback-summary",
        action="store_true",
        help=(
            "Print dedup questions where classified_intent was `fallback`, sorted by how often "
            "they appeared — use to grow deterministic routing."
        ),
    )
    args = p.parse_args()
    path: Path = args.log

    if not path.is_file():
        print(
            f"# No log file at {path} — set CHAT_TRAINING_LOG_ENABLE=true and ask questions in /ai/chat.",
            file=sys.stderr,
        )
        return

    if args.intent_fallback_summary:
        _print_fallback_intent_summary(path)
        return

    print("# Merge into chat_scope_examples.tsv after review (tabs = delimiter)\n")

    for _kn, row in sorted(read_jsonl_latest_by_normalized_question(path).items()):
        q = sanitized_question_text(row.get("question") or "")
        if not q:
            continue
        label, note = infer_scope_training_label(row)
        if label:
            print(f"{label}\t{q}")
        else:
            why = note or "unknown"
            print(f"# REVIEW ({why})\t{q}")

    print("\n# Or automate: python automerge_chat_scope_from_log.py --apply", file=sys.stderr)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    main()
