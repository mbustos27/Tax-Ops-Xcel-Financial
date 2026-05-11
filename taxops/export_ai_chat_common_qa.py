#!/usr/bin/env python3
"""
Export non-expired rows from ai_chat_common_answers (SQLite cache for POST /ai/chat).

Use as a lightweight living FAQ or seed for training docs. Optional: delete expired
rows first (suitable for nightly Task Scheduler).

Usage:
  cd taxops
  python export_ai_chat_common_qa.py > data/chat_faq_export.tsv
  python export_ai_chat_common_qa.py --prune-expired --format md -o data/chat_faq.md
"""
from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

_PKG = Path(__file__).resolve().parent
if str(_PKG) not in sys.path:
    sys.path.insert(0, str(_PKG))

from config import DB_PATH  # noqa: E402
from db import get_connection, init_db  # noqa: E402
from utils import parse_iso_datetime  # noqa: E402


def prune_expired_rows(conn) -> int:
    now_d = datetime.now(timezone.utc)
    rows = conn.execute(
        "SELECT cache_key, expires_at FROM ai_chat_common_answers"
    ).fetchall()
    deleted = 0
    for r in rows:
        exp = parse_iso_datetime(r["expires_at"])
        if exp is None:
            conn.execute(
                "DELETE FROM ai_chat_common_answers WHERE cache_key = ?",
                (r["cache_key"],),
            )
            deleted += 1
            continue
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp < now_d:
            conn.execute(
                "DELETE FROM ai_chat_common_answers WHERE cache_key = ?",
                (r["cache_key"],),
            )
            deleted += 1
    conn.commit()
    return deleted


def _active_rows(conn) -> list:
    now_d = datetime.now(timezone.utc)
    rows = conn.execute(
        """
        SELECT normalized_question, season_year, hit_count, tool_used,
               answer, expires_at
        FROM ai_chat_common_answers
        ORDER BY hit_count DESC, normalized_question COLLATE NOCASE
        """
    ).fetchall()
    out = []
    for r in rows:
        exp = parse_iso_datetime(r["expires_at"])
        if exp is None:
            continue
        if exp.tzinfo is None:
            exp = exp.replace(tzinfo=timezone.utc)
        if exp >= now_d:
            out.append(r)
    return out


def emit_tsv(rows, fp):
    fp.write(
        "hit_count\tseason_year\ttool_used\tnormalized_question\tanswer\n"
    )
    for r in rows:
        aq = (r["answer"] or "").replace("\t", " ").replace("\r", "").replace("\n", " ")
        nq = (r["normalized_question"] or "").replace("\t", " ")
        tu = r["tool_used"] or ""
        fp.write(
            f'{r["hit_count"]}\t{r["season_year"]}\t{tu}\t{nq}\t{aq}\n'
        )


def emit_md(rows, fp):
    for r in rows:
        fp.write(f"### {r['normalized_question']} (year {r['season_year']})\n")
        fp.write(f"- Hits: `{r['hit_count']}`  Tool: `{r['tool_used'] or '-'}`  Expires: `{r['expires_at']}`\n\n")
        fp.write(r["answer"] or "")
        fp.write("\n\n---\n\n")


def main() -> None:
    p = argparse.ArgumentParser(
        description="Export (and optionally prune) /ai/chat SQLite FAQ cache.",
    )
    p.add_argument(
        "--db",
        default=None,
        help="SQLite path (default: TAXOPS_DB or bundled taxops.db)",
    )
    p.add_argument(
        "--prune-expired",
        action="store_true",
        help="Delete expired or invalid rows before exporting",
    )
    p.add_argument(
        "--format",
        choices=("tsv", "md"),
        default="tsv",
    )
    p.add_argument(
        "-o",
        "--output",
        default="-",
        help="Output path, or '-' for stdout",
    )
    args = p.parse_args()

    conn = get_connection(args.db or DB_PATH)
    try:
        init_db(conn)
        if args.prune_expired:
            n = prune_expired_rows(conn)
            print(f"Pruned {n} expired or invalid ai_chat_common_answers row(s)", file=sys.stderr)
        rows = _active_rows(conn)
    finally:
        conn.close()

    out_fp = sys.stdout if args.output == "-" else open(args.output, "w", encoding="utf-8")
    try:
        if args.format == "tsv":
            emit_tsv(rows, out_fp)
        else:
            emit_md(rows, out_fp)
    finally:
        if out_fp is not sys.stdout:
            out_fp.close()


if __name__ == "__main__":
    main()
