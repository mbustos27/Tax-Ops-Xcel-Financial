#!/usr/bin/env python3
"""
Auto-merge staff chat JSONL into chat_scope_examples.tsv and re-run train_chat_scope_model.py.

Uses only **high-confidence** labels from the log (scope_gate → out_scope, tool_used → in_scope).
Ambiguous router outcomes are never written to the TSV unless you classify them elsewhere.

Defaults to **dry-run** (preview). Use --apply to write.

Usage:
  cd taxops
  python automerge_chat_scope_from_log.py
  python automerge_chat_scope_from_log.py --apply --review-out scope_review_queue.txt

Nightly automation (Task Scheduler example):
  cd /d C:\\path\\to\\taxops && python automerge_chat_scope_from_log.py --apply
"""
from __future__ import annotations

import argparse
import os
import shutil
import sys
from pathlib import Path

_PKG = Path(__file__).resolve().parent
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

from chat_scope_classifier import (  # noqa: E402
    EXAMPLES_PATH,
    MODEL_PATH,
    train_and_save_from_tsv,
)
from chat_training_io import load_existing_example_norms, tsv_appends_for_log  # noqa: E402
from config import CHAT_TRAINING_LOG_PATH  # noqa: E402


def main() -> None:
    ap = argparse.ArgumentParser(
        description=(
            "Append high-confidence logged questions to chat_scope_examples.tsv "
            "and optionally retrain sklearn_chat_scope.pkl."
        )
    )
    ap.add_argument("--log", type=Path, default=CHAT_TRAINING_LOG_PATH, help="Staff JSONL")
    ap.add_argument(
        "--tsv",
        type=Path,
        default=Path(EXAMPLES_PATH),
        help="Training TSV path",
    )
    ap.add_argument(
        "--apply",
        action="store_true",
        help="Append rows + backup .tsv + train; omit for preview-only",
    )
    ap.add_argument(
        "--no-train",
        action="store_true",
        help="With --apply: merge TSV only, skip sklearn retrain",
    )
    ap.add_argument(
        "--review-out",
        type=Path,
        default=None,
        help="Write ambiguous questions as #comment lines for later manual labeling",
    )
    args = ap.parse_args()
    log: Path = args.log
    tsv: Path = args.tsv

    if not log.is_file():
        print(f"No log file: {log} (enable CHAT_TRAINING_LOG_ENABLE and use AI Assistant)", file=sys.stderr)
        sys.exit(1)

    existing = load_existing_example_norms(tsv)
    high, amb = tsv_appends_for_log(log, set(existing))

    print(f"log={log}\ntsv={tsv}\n  new high-confidence rows: {len(high)}\n  ambiguous (need review): {len(amb)}\n")

    if high:
        for lab, q in high[:15]:
            print(f"  + {lab}\t{q}")
        if len(high) > 15:
            print(f"  … ({len(high) - 15} more)")
    if amb:
        print("\nambiguous (examples):")
        for tag, q in amb[:10]:
            print(f"  {tag}\t{q}")
        if len(amb) > 10:
            print(f"  … ({len(amb) - 10} more)")

    if args.review_out and amb:
        body = "\n".join(f"{tag}\t{q}" for tag, q in amb) + "\n"
        args.review_out.parent.mkdir(parents=True, exist_ok=True)
        args.review_out.write_text(body, encoding="utf-8")
        print(f"Wrote {len(amb)} ambiguous rows → {args.review_out}", file=sys.stderr)

    if not args.apply:
        print("\nDry-run only — rerun with --apply to merge + train.", file=sys.stderr)
        return

    if not high:
        print("\nNothing to merge.", file=sys.stderr)
        return

    if not tsv.is_file():
        print(f"Training TSV missing: {tsv}", file=sys.stderr)
        sys.exit(1)

    backup = tsv.with_suffix(tsv.suffix + ".bak")
    shutil.copy2(tsv, backup)
    print(f"Backup: {backup}", file=sys.stderr)

    with tsv.open("a", encoding="utf-8") as fh:
        for lab, q in high:
            fh.write(f"{lab}\t{q}\n")

    print(f"Appended {len(high)} rows to {tsv}", file=sys.stderr)

    if args.no_train:
        return

    try:
        n = train_and_save_from_tsv(str(EXAMPLES_PATH), str(MODEL_PATH))
    except RuntimeError as e:
        print(f"Retrain skipped or failed: {e}", file=sys.stderr)
        sys.exit(1)
    kb = Path(MODEL_PATH).stat().st_size // 1024
    print(f"Retrained model on {n} rows → {MODEL_PATH} ({kb} KB)", file=sys.stderr)


if __name__ == "__main__":
    os.environ.setdefault("PYTHONIOENCODING", "utf-8")
    main()
