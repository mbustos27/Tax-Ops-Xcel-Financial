"""Shared helpers: read staff chat JSONL → deduped rows → scope training labels."""

from __future__ import annotations

import json
from pathlib import Path

from chat_cache import normalize_question


def read_jsonl_latest_by_normalized_question(path: Path) -> dict[str, dict]:
    """Latest JSON row per `normalize(question)` — collapsing duplicate wording / years."""
    out: dict[str, dict] = {}
    if not path.is_file():
        return out
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except json.JSONDecodeError:
                continue
            q = sanitized_question_text(row.get("question") or "")
            if not q:
                continue
            out[normalize_question(q)] = row
    return out


def infer_scope_training_label(row: dict) -> tuple[str | None, str]:
    """Return (label_for_tsv, notes) — label None means needs human review."""
    if row.get("scope_classifier_blocked"):
        return "out_of_scope", ""
    ci = str(row.get("classified_intent") or "").strip()
    tu = row.get("tool_used")
    if ci == "fallback" and not (tu and str(tu).strip()):
        return None, "classifier_fallback_no_tool"
    if tu and str(tu).strip():
        return "in_scope", ""
    return None, "no_tool_router_result"


def sanitized_question_text(question: str) -> str:
    return " ".join(str(question).strip().split()).replace("\t", " ")


def load_existing_example_norms(tsv_path: Path) -> set[str]:
    """Normalized question strings already present in chat_scope_examples.tsv."""
    known: set[str] = set()
    if not tsv_path.is_file():
        return known
    with tsv_path.open(encoding="utf-8") as fh:
        for line in fh:
            s = line.strip()
            if not s or s.startswith("#"):
                continue
            if "\t" not in s:
                continue
            lab_raw, qrest = s.split("\t", 1)
            lab_lc = lab_raw.strip().lower()
            q_hint = qrest.strip().lower()
            if lab_lc == "label" and q_hint == "question":
                continue
            q = sanitized_question_text(qrest)
            if q:
                known.add(normalize_question(q))
    return known


def tsv_appends_for_log(
    path: Path,
    existing_norms: set[str],
) -> tuple[list[tuple[str, str]], list[tuple[str, str]]]:
    """
    Returns (high_confidence_rows, ambiguous_rows) as (label, question).

    ambiguous uses label string like "#REVIEW:reason" for sidecar files only —
    never append those to chat_scope_examples.tsv automatically.
    """
    high: list[tuple[str, str]] = []
    amb: list[tuple[str, str]] = []
    for _, row in sorted(
        read_jsonl_latest_by_normalized_question(path).items(),
        key=lambda kv: kv[0],
    ):
        q = sanitized_question_text(row.get("question") or "")
        if not q:
            continue
        nn = normalize_question(q)
        if nn in existing_norms:
            continue
        label, reason = infer_scope_training_label(row)
        if label:
            high.append((label, q))
            existing_norms.add(nn)
        else:
            amb.append((f"#REVIEW:{reason}", q))
    high.sort(key=lambda x: (x[0], x[1].lower()))
    amb.sort(key=lambda x: x[1].lower())
    return high, amb
