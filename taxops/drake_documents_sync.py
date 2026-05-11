"""
DOC-6 — Drake Documents folder groundwork (manual file copy only; no Drake API).

Drake organizes *Documents* in a client tree (Working / Archive Cabinets); filenames
alphabetically by client. Exact on-disk layouts vary by office options. TaxOps lays out
staging copies consistently for handoff::

    {DRAKE_DOCUMENTS_PATH}/{tax_year}/{client_key}/taxops_rid_{return_id}/
      ├─ <copied original filenames (deduped)>
      └─ taxops_drake_sync.json   # manifest / metadata export

Staff can map folders into Drake's cabinet as their workflow allows.
"""

from __future__ import annotations

import json
import os
import re
import shutil
from datetime import datetime, timezone

_DRAKE_SYNC_META = "taxops_drake_sync.json"

_WIN_ILLEGAL_RE = re.compile(r'[/\\:*?"<>|]')


def drake_safe_folder_fragment(value: object, max_len: int = 80) -> str:
    """One path segment safe on Windows/macOS/Linux."""
    s = str(value or "").strip().replace("\n", "_").replace("\r", "")
    s = _WIN_ILLEGAL_RE.sub("_", s)
    s = re.sub(r"[\x00-\x1f]", "_", s).strip(". _")
    return (s[:max_len] if s else "unknown")[:max_len]


def _client_folder_key(last_name: str | None, first_name: str | None, client_id: int) -> str:
    """Sort-friendly client folder (individual business rule: surname first elsewhere in office)."""
    ln = drake_safe_folder_fragment(last_name, 48).upper()
    fn = drake_safe_folder_fragment(first_name, 32)
    return f"{ln}_{fn}_c{int(client_id)}"


def _tax_year_segment(tax_year: int | None) -> str:
    if tax_year is None:
        return "unknown_tax_year"
    try:
        return str(int(tax_year))
    except (TypeError, ValueError):
        return "unknown_tax_year"


def _return_folder_name(log_number: object, return_id: int) -> str:
    ln = drake_safe_folder_fragment(log_number if log_number is not None else "nolog")
    return f"log{ln}_rid{int(return_id)}"


def _dest_leaf_name(original_name: object, doc_id: int) -> str:
    raw = os.path.basename(str(original_name).strip()) if original_name else ""
    if not raw:
        raw = f"document_{doc_id}"
    stem, ext = os.path.splitext(raw)
    stem_s = drake_safe_folder_fragment(stem, 180)
    ext_trim = ""
    if ext.strip():
        ext_trim = drake_safe_folder_fragment(ext.lstrip("."), 24)
    ext_part = f".{ext_trim}" if ext_trim else ""
    return f"{stem_s}{ext_part}"


def sync_to_drake(return_id: int) -> dict:
    """
    Copy all active ``return_documents`` files for ``return_id`` into the Drake staging tree.

    Source files remain untouched (read + copy only). Existing document storage unchanged.

    Requires ``DRAKE_DOCUMENTS_PATH`` (see ``config``). Idempotent overwrite on re-sync.
    """
    from config import DRAKE_DOCUMENTS_PATH
    from db import get_connection

    root = (DRAKE_DOCUMENTS_PATH or "").strip()
    if not root:
        return {
            "success": False,
            "error": "drake_documents_path_unset",
            "message": "Set DRAKE_DOCUMENTS_PATH in the environment to enable sync.",
        }

    dest_root_abs = os.path.abspath(os.path.expanduser(root))
    os.makedirs(dest_root_abs, exist_ok=True)

    conn = get_connection()
    try:
        bundle = conn.execute(
            """
            SELECT
              r.id AS return_id,
              r.client_id,
              r.tax_year,
              r.log_number,
              c.last_name,
              c.first_name,
              c.display_name
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.id = ?
            """,
            (int(return_id),),
        ).fetchone()
        if not bundle:
            return {
                "success": False,
                "error": "return_not_found",
                "message": f"No return found with id={return_id}.",
            }

        doc_rows = conn.execute(
            """
            SELECT id, filename, original_filename, file_path
            FROM return_documents
            WHERE return_id = ?
              AND (is_deleted = 0 OR is_deleted IS NULL)
            ORDER BY id
            """,
            (int(return_id),),
        ).fetchall()
    finally:
        conn.close()

    ty = _tax_year_segment(bundle["tax_year"])
    ck = _client_folder_key(bundle["last_name"], bundle["first_name"], bundle["client_id"])
    rdir = _return_folder_name(bundle["log_number"], bundle["return_id"])
    sync_dir = os.path.join(dest_root_abs, ty, ck, rdir)
    os.makedirs(sync_dir, exist_ok=True)

    copied: list[dict[str, object]] = []
    skipped: list[dict[str, object]] = []
    used_lower: set[str] = set()
    utc_now = datetime.now(timezone.utc).isoformat(timespec="seconds")

    for dr in doc_rows:
        fp = dr["file_path"]
        doc_id = int(dr["id"])
        fname = (
            dr["original_filename"]
            or dr["filename"]
            or (os.path.basename(fp) if fp else f"document_{doc_id}")
        )
        base_name = _dest_leaf_name(fname, doc_id)

        cand = base_name
        dup = 0
        cand_key = cand.lower()
        while cand_key in used_lower:
            dup += 1
            stem, ext = os.path.splitext(base_name)
            cand = f"{stem}_doc{doc_id}_{dup}{ext}"
            cand_key = cand.lower()
        used_lower.add(cand_key)

        dest_path = os.path.join(sync_dir, cand)
        src_path = os.path.abspath(str(fp).strip()) if fp else ""

        if not src_path or not os.path.isfile(src_path):
            skipped.append(
                {
                    "doc_id": doc_id,
                    "reason": "source_missing",
                    "file_path": fp,
                    "planned_dest": cand,
                }
            )
            continue

        shutil.copy2(src_path, dest_path)
        copied.append(
            {
                "doc_id": doc_id,
                "source_path": src_path,
                "dest_filename": cand,
                "dest_path": dest_path,
            }
        )

    manifest = {
        "schema_version": 1,
        "taxops_return_id": int(bundle["return_id"]),
        "taxops_client_id": int(bundle["client_id"]),
        "tax_year_bundle": ty,
        "log_number_raw": bundle["log_number"],
        "client_folder_key": ck,
        "synced_at_utc": utc_now,
        "documents_copied": len(copied),
        "documents_skipped": len(skipped),
        "entries": copied,
        "skipped": skipped,
        "staging_relative": os.path.relpath(sync_dir, dest_root_abs),
    }

    meta_path = os.path.join(sync_dir, _DRAKE_SYNC_META)
    with open(meta_path, "w", encoding="utf-8") as mh:
        json.dump(manifest, mh, indent=2)

    return {
        "success": True,
        "drake_documents_root": dest_root_abs,
        "staging_directory": sync_dir,
        "tax_year_segment": ty,
        "client_key": ck,
        "manifest_path": meta_path,
        "copied": copied,
        "skipped": skipped,
    }
