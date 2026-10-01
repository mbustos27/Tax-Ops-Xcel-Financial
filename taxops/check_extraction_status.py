#!/usr/bin/env python3
"""CLI: extraction queue / worker health for the office PC.

Examples:
  python check_extraction_status.py
  python check_extraction_status.py --fail-missing
  python check_extraction_status.py --health http://127.0.0.1:5000/health
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config import DB_PATH  # noqa: E402
from db import get_connection, init_db  # noqa: E402
from extractor import MAX_ATTEMPTS  # noqa: E402
from utils import now  # noqa: E402


def _counts(conn) -> dict[str, int]:
    rows = conn.execute(
        "SELECT status, COUNT(*) AS n FROM extraction_queue GROUP BY status"
    ).fetchall()
    return {str(r["status"]): int(r["n"]) for r in rows}


def _fetch_health(url: str) -> dict:
    with urllib.request.urlopen(url, timeout=5) as resp:
        return json.loads(resp.read().decode("utf-8"))


def _list_open(conn, limit: int = 30) -> list[dict]:
    rows = conn.execute(
        """
        SELECT eq.id, eq.status, eq.attempts, eq.error_message, eq.created_at,
               eq.processed_at, eq.extraction_method, eq.detected_form_type,
               eq.return_id, eq.doc_id,
               rd.file_path, rd.filename, rd.source,
               r.log_number, c.last_name, c.first_name
        FROM extraction_queue eq
        LEFT JOIN return_documents rd ON rd.id = eq.doc_id
        LEFT JOIN returns r ON r.id = eq.return_id
        LEFT JOIN clients c ON c.id = r.client_id
        WHERE eq.status IN ('pending', 'processing', 'failed', 'needs_review')
        ORDER BY CASE eq.status
                   WHEN 'processing' THEN 0
                   WHEN 'pending' THEN 1
                   WHEN 'failed' THEN 2
                   ELSE 3
                 END,
                 eq.created_at ASC
        LIMIT ?
        """,
        (limit,),
    ).fetchall()
    out = []
    for r in rows:
        path = r["file_path"] or ""
        out.append(
            {
                "id": r["id"],
                "status": r["status"],
                "attempts": r["attempts"],
                "error": (r["error_message"] or "")[:160],
                "created_at": r["created_at"],
                "method": r["extraction_method"] or "",
                "form": r["detected_form_type"] or "",
                "return_id": r["return_id"],
                "doc_id": r["doc_id"],
                "log_number": r["log_number"],
                "client": f"{(r['last_name'] or '').strip()}, {(r['first_name'] or '').strip()}".strip(", "),
                "filename": r["filename"] or "",
                "source": r["source"] or "",
                "file_exists": bool(path and os.path.isfile(path)),
                "file_path": path,
            }
        )
    return out


def _fail_missing(conn) -> int:
    rows = conn.execute(
        """
        SELECT eq.id, rd.file_path
        FROM extraction_queue eq
        JOIN return_documents rd ON rd.id = eq.doc_id
        WHERE eq.status IN ('pending', 'processing')
        """
    ).fetchall()
    n = 0
    ts = now()
    for r in rows:
        path = r["file_path"] or ""
        if path and os.path.isfile(path):
            continue
        conn.execute(
            """
            UPDATE extraction_queue
            SET status = 'failed',
                error_message = ?,
                processed_at = ?
            WHERE id = ?
            """,
            (
                f"File missing on disk: {path or '(empty path)'}",
                ts,
                r["id"],
            ),
        )
        n += 1
    if n:
        conn.commit()
    return n


def main(argv: list[str] | None = None) -> int:
    ap = argparse.ArgumentParser(description="TaxOps extraction queue status")
    ap.add_argument("--db", default=os.environ.get("TAXOPS_DB") or DB_PATH)
    ap.add_argument(
        "--health",
        default=os.environ.get("TAXOPS_HEALTH_URL", "http://127.0.0.1:5000/health"),
        help="Health URL for worker liveness (empty to skip)",
    )
    ap.add_argument(
        "--fail-missing",
        action="store_true",
        help="Mark pending/processing items whose files are gone as failed",
    )
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args(argv)

    health = None
    if args.health:
        try:
            health = _fetch_health(args.health)
        except Exception as exc:
            health = {"error": str(exc)}

    conn = get_connection(args.db)
    try:
        init_db(conn)
        counts = extraction_queue_counts()
        # Recompute with this connection's DB_PATH override context — counts() uses
        # global DB_PATH; if --db differs, query locally.
        if os.path.abspath(args.db) != os.path.abspath(DB_PATH):
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM extraction_queue GROUP BY status"
            ).fetchall()
            counts = {str(r["status"]): int(r["n"]) for r in rows}
        items = _list_open(conn)
        failed_missing = 0
        if args.fail_missing:
            failed_missing = _fail_missing(conn)
            items = _list_open(conn)
            rows = conn.execute(
                "SELECT status, COUNT(*) AS n FROM extraction_queue GROUP BY status"
            ).fetchall()
            counts = {str(r["status"]): int(r["n"]) for r in rows}
    finally:
        conn.close()

    payload = {
        "db": args.db,
        "max_attempts": MAX_ATTEMPTS,
        "counts": counts,
        "health": health,
        "open_items": items,
        "failed_missing": failed_missing,
    }

    if args.json:
        print(json.dumps(payload, indent=2))
        return 0

    print("TaxOps extraction status")
    print(f"DB: {args.db}")
    print(f"Max attempts before dead-letter: {MAX_ATTEMPTS}")
    print("")
    print("Queue counts:")
    for k in (
        "pending",
        "processing",
        "needs_review",
        "failed",
        "completed",
        "completed_today",
        "skipped",
    ):
        if k in counts or k in ("pending", "processing", "failed", "needs_review"):
            print(f"  {k}: {counts.get(k, 0)}")
    for k, v in sorted(counts.items()):
        if k not in {
            "pending",
            "processing",
            "needs_review",
            "failed",
            "completed",
            "completed_today",
            "skipped",
        }:
            print(f"  {k}: {v}")

    print("")
    if health is None:
        print("Worker health: (skipped)")
    elif health.get("error"):
        print(f"Worker health: unreachable — {health['error']}")
        print("  (Is TaxOps running? Try after nssm start.)")
    else:
        workers = health.get("workers") or {}
        ext = workers.get("extraction") or {}
        print(
            f"Worker health: extraction started={ext.get('started')} "
            f"running={ext.get('running')}  app_status={health.get('status')}"
        )
        eq = health.get("extraction_queue") or {}
        if eq:
            print(f"  /health queue: {eq}")

    print("")
    print("Open / problem items:")
    if not items:
        print("  (none)")
    for it in items:
        log = it["log_number"] or it["return_id"]
        exists = "file OK" if it["file_exists"] else "FILE MISSING"
        print(
            f"  • [{it['status']}] id={it['id']} attempts={it['attempts']}/{MAX_ATTEMPTS}  "
            f"log {log}  {it['client'] or '(no client)'}  ({exists})"
        )
        print(f"      {it['filename']}  source={it['source'] or '—'}")
        if it["error"]:
            print(f"      error: {it['error']}")
        print(f"      created {it['created_at']}")

    if args.fail_missing:
        print("")
        print(f"Marked missing-file items failed: {failed_missing}")

    print("")
    print("Next:")
    print("  • Tools → Failed Docs — retry/dismiss dead-letter items")
    print("  • Restart TaxOps to wake the worker (pending items retry automatically)")
    print("  • python check_extraction_status.py --fail-missing  — clear gone files")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
