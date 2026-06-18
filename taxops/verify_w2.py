#!/usr/bin/env python3
"""
FORMS-2 / GitHub issue 77 — W-2 extraction & persistence regression harness.

Checks (no LLM):
  1) Every ``FORM_TABLE_INSERT_COLUMNS["w2_records"]`` key appears in
     ``extractor._DOCUMENT_EXTRACT_ALLOWED_KEYS`` (no silent post-LLM drop).
  2) Shared extractor prompt exposes each of those columns in the W-2 JSON snippet.
  3) ``ai_routes._save_form_data`` round-trip: canonical fixture → SQLite row;
     all enumerated box*/employer/tax_year columns match expected values.

Run from repo: ``python verify_w2.py`` (cwd = taxops/) or ``python -m verify_w2`` if on PYTHONPATH.
Exit 0 if all checks pass, 1 otherwise.
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path
from typing import Any

# taxops/ package root (this file lives next to app.py, db.py, …)
_TAXOPS_ROOT = Path(__file__).resolve().parent
if str(_TAXOPS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TAXOPS_ROOT))

from ai_routes import _coerce_sql_integer_field, _save_form_data  # noqa: E402
from db import init_db  # noqa: E402
from extractor import _DOCUMENT_EXTRACT_ALLOWED_KEYS, _irs_form_extraction_block  # noqa: E402
from form_schema import FORM_INTEGER_COLUMNS, FORM_TABLE_INSERT_COLUMNS  # noqa: E402


W2_COLUMNS = FORM_TABLE_INSERT_COLUMNS["w2_records"]


def canonical_w2_fixture() -> dict[str, Any]:
    """Distinct sentinel per column so truncation or swaps are visible in diffs."""
    d: dict[str, Any] = {}
    for col in W2_COLUMNS:
        if col in FORM_INTEGER_COLUMNS:
            d[col] = 1 if hash(col) % 2 else 0
        else:
            d[col] = f"w2sec_{col}"
    return d


def expected_stored_scalar(col: str, raw: Any) -> Any:
    if col in FORM_INTEGER_COLUMNS:
        return _coerce_sql_integer_field(raw)
    return "" if raw is None else str(raw).strip()


def check_whitelist(messages: list[str]) -> bool:
    allowed = _DOCUMENT_EXTRACT_ALLOWED_KEYS
    missing = [c for c in W2_COLUMNS if c not in allowed]
    if missing:
        messages.append("FAIL: INSERT columns missing from _DOCUMENT_EXTRACT_ALLOWED_KEYS:")
        messages.extend(f"       - {m}" for m in missing)
        return False
    messages.append(f"PASS: all {len(W2_COLUMNS)} W-2 INSERT cols are extraction allow-listed.")
    return True


def check_prompt(messages: list[str]) -> bool:
    block = _irs_form_extraction_block()
    missing = []
    for col in W2_COLUMNS:
        needle = f'"{col}"'
        if needle not in block:
            missing.append(col)
    if missing:
        messages.append("FAIL: W-2 prompt JSON template missing quoted keys:")
        messages.extend(f"       - {m}" for m in missing)
        return False
    messages.append(f"PASS: W-2 prompt snippet mentions all {len(W2_COLUMNS)} INSERT columns.")
    return True


def check_pragma_table(messages: list[str], conn: sqlite3.Connection) -> bool:
    info = conn.execute("PRAGMA table_info(w2_records)").fetchall()
    have = {str(r["name"]) for r in info}
    missing = [c for c in W2_COLUMNS if c not in have]
    if missing:
        messages.append("FAIL: w2_records table missing DDL columns:")
        messages.extend(f"       - {m}" for m in missing)
        return False
    messages.append("PASS: PRAGMA table_info(w2_records) covers all INSERT columns.")
    return True


def check_roundtrip(messages: list[str], conn: sqlite3.Connection) -> tuple[bool, dict[str, Any] | None]:
    fixture = canonical_w2_fixture()
    cur_doc = conn.execute(
        """
        INSERT INTO return_documents (
            return_id, filename, original_filename, doc_type, source,
            file_path, file_size_bytes, uploaded_by, uploaded_at, is_deleted
        ) VALUES (?, 'w2canon.pdf', 'w2canon.pdf', 'unknown', 'verify',
                  ?, 1, 'verify_w2', '2026-06-01', 0)
        """,
        (99001, str(_TAXOPS_ROOT / "_verify_w2_placeholder.pdf")),
    )
    doc_id = int(cur_doc.lastrowid)

    ok_save = _save_form_data(conn, "w2_records", 99001, doc_id, fixture)
    if not ok_save:
        messages.append("FAIL: _save_form_data returned False for canonical fixture.")
        return False, None

    row = conn.execute(
        """
        SELECT * FROM w2_records
        WHERE return_id = ? AND doc_id = ?
        ORDER BY id DESC LIMIT 1
        """,
        (99001, doc_id),
    ).fetchone()
    if row is None:
        messages.append("FAIL: no w2_records row inserted.")
        return False, None

    mismatches: list[str] = []
    snapshot: dict[str, Any] = {}
    for col in W2_COLUMNS:
        got = row[col]
        exp = expected_stored_scalar(col, fixture[col])
        snapshot[col] = got
        if col in FORM_INTEGER_COLUMNS:
            gi = int(got or 0)
            ei = int(exp)
            if gi != ei:
                mismatches.append(f"       {col!r}: expected INTEGER {ei!r}, got {gi!r}")
        else:
            gs = "" if got is None else str(got).strip()
            es = str(exp).strip()
            if gs != es:
                mismatches.append(f"       {col!r}: expected TEXT {es!r}, got {gs!r}")

    if mismatches:
        messages.append("FAIL: DB row differs from canonical fixture:")
        messages.extend(mismatches)
        return False, snapshot

    messages.append("PASS: canonical W-2 fixture round-trip matches all INSERT columns.")
    return True, snapshot


def _seed_parent_rows(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        INSERT INTO clients (id, last_name, first_name, display_name)
        VALUES (99000, 'Verify', 'Wtwo', 'Verify, Wtwo');
        """
    )
    conn.execute(
        """
        INSERT INTO returns (
            id, client_id, log_number, tax_year, processor, verified, client_status,
            intake_date
        ) VALUES (
            99001, 99000, 'w2chk', 2025, '', 0, 'PROCESSING', '2026-06-01'
        );
        """
    )
    conn.commit()


def run_w2_verification(
    *,
    db_path: str | None = None,
    dump_row: bool = False,
) -> tuple[bool, list[str]]:
    """Run alignment + round-trip checks. Returns (all_ok, message_lines)."""
    messages: list[str] = []

    ok_wl = check_whitelist(messages)
    messages.append("")
    ok_pr = check_prompt(messages)
    messages.append("")

    path = db_path
    tmp_fd: Any = None
    overall = False
    snapshot_out: dict[str, Any] | None = None
    if path is None:
        tmp_fd, path = tempfile.mkstemp(prefix="taxops_verify_w2_", suffix=".db")
    try:
        conn = sqlite3.connect(path)
        conn.row_factory = sqlite3.Row

        init_db(conn)
        _seed_parent_rows(conn)

        messages.append("")  # pragma / db section
        ok_tm = check_pragma_table(messages, conn)
        messages.append("")
        from flask import Flask

        flask_app = Flask(__name__)
        with flask_app.app_context():
            ok_rt, snapshot_out = check_roundtrip(messages, conn)
        conn.commit()
        conn.close()

        ok_db = ok_tm and ok_rt

        if dump_row and snapshot_out is not None:
            messages.append("")
            messages.append("-- persisted w2_records business columns (JSON):")
            slim = {k: snapshot_out[k] for k in W2_COLUMNS}
            messages.append(json.dumps(slim, indent=2, sort_keys=True))

        overall = ok_wl and ok_pr and ok_db
    finally:
        if tmp_fd is not None:
            try:
                import os

                os.close(tmp_fd)
            except OSError:
                pass
            try:
                Path(path).unlink(missing_ok=True)
            except OSError:
                pass

    messages.insert(
        0,
        "TaxOps verify_w2 (FORMS-2 / GH issue 77), db="
        + (repr(path) if path is not None else "<missing>"),
    )
    return overall, messages


def main(argv: list[str]) -> int:
    dump_row = "--dump-row" in argv or "-d" in argv
    quiet = "--quiet" in argv or "-q" in argv
    db_path: str | None = None
    for i, a in enumerate(argv):
        if a in ("--db",) and i + 1 < len(argv):
            db_path = argv[i + 1]

    ok, lines = run_w2_verification(db_path=db_path, dump_row=dump_row)
    text = "\n".join(lines)
    if quiet and not ok:
        print(text, file=sys.stderr)
    elif not quiet:
        print(text)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
