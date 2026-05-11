#!/usr/bin/env python3
"""
Train sklearn_chat_scope.pkl from chat_scope_examples.tsv.

Run from the taxops package directory (or anywhere with PYTHONPATH set):
    python train_chat_scope_model.py

Add or edit rows in chat_scope_examples.tsv, then re-run to refresh the gate.
"""
from __future__ import annotations

import os
import sys

# Allow running as `python taxops/train_chat_scope_model.py` from repo root
_PKG = os.path.dirname(os.path.abspath(__file__))
if _PKG not in sys.path:
    sys.path.insert(0, _PKG)

from chat_scope_classifier import EXAMPLES_PATH, MODEL_PATH, train_and_save_from_tsv


def main() -> None:
    n = train_and_save_from_tsv(EXAMPLES_PATH, MODEL_PATH)
    kb = os.path.getsize(MODEL_PATH) // 1024
    print(f"Trained chat scope classifier on {n} examples -> {MODEL_PATH} ({kb} KB)")


if __name__ == "__main__":
    main()
