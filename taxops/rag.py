"""
RAG (Retrieval-Augmented Generation) for TaxOps /ai/chat.

ChromaDB (local disk) + sentence-transformers (local CPU). No external embedding APIs.
Never embed or store SSN, ssn_last4, EIN, or TIN in document text or Chroma metadata.
"""
from __future__ import annotations

import logging
import os
import re
import threading
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger(__name__)

from config import (  # noqa: E402
    RAG_DB_PATH,
    RAG_EMBEDDING_MODEL,
    RAG_ENABLED,
    RAG_REBUILD_INTERVAL,
    RAG_TOP_K,
)
from db import get_connection

_HERE = Path(__file__).resolve().parent

_rag_available = False
_rag_client: Any = None
_embedding_fn: Any = None
_collections: dict[str, Any] = {}
_collection_names = ("returns", "clients", "documents", "form_data")
_collections_lock = threading.Lock()
_initialize_lock = threading.Lock()
_last_index_finish: float = 0.0
_last_index_error: str | None = None

_SSNLIKE = re.compile(r"\b\d{3}\s*[-.]?\s*\d{2}\s*[-.]?\s*\d{4}\b")
_EINLIKE = re.compile(r"\b\d{2}\s*[-.]?\s*\d{7}\b")
_TIN_SUFFIX = re.compile(r"\bssn(?:_last4|\s*(?:tail|suffix))?\s*[:.]?\s*\d+", re.I)


def _scrub_identifiers(text: str | None) -> str:
    if not text or not isinstance(text, str):
        return ""
    s = text
    s = _SSNLIKE.sub("(redacted-id)", s)
    s = _EINLIKE.sub("(redacted-id)", s)
    s = _TIN_SUFFIX.sub("(redacted-id)", s)
    return s.strip()


def _safe_str(val: Any) -> str:
    if val is None:
        return ""
    return _scrub_identifiers(str(val))


def initialize_rag() -> bool:
    """
    Lazily attach Chroma persistent client + sentence-transformers embedding_fn.
    On failure logs and clears _rag_available (never raises).
    """
    global _rag_available, _rag_client, _embedding_fn, _collections

    if not RAG_ENABLED:
        return False

    with _initialize_lock:
        if _collections and _embedding_fn is not None and _rag_client is not None:
            _rag_available = True
            return True
        try:
            import chromadb
            from chromadb.utils.embedding_functions import SentenceTransformerEmbeddingFunction

            os.makedirs(RAG_DB_PATH, exist_ok=True)
            ef = SentenceTransformerEmbeddingFunction(model_name=RAG_EMBEDDING_MODEL)
            client = chromadb.PersistentClient(path=str(RAG_DB_PATH))

            cols: dict[str, Any] = {}
            for name in _collection_names:
                cols[name] = client.get_or_create_collection(name=name, embedding_function=ef)

            _embedding_fn = ef
            _rag_client = client
            _collections = cols
            _rag_available = True
            logger.info("RAG: Chroma initialized at %s model=%s", RAG_DB_PATH, RAG_EMBEDDING_MODEL)
            return True
        except Exception as exc:
            _rag_available = False
            _rag_client = None
            _embedding_fn = None
            _collections = {}
            logger.error("RAG: initialize failed (chat continues without retrieval): %s", exc)
            return False


def build_index(app) -> None:  # noqa: ARG001 — Flask pattern
    """Rebuild four Chroma collections from SQLite via upsert (batches of 100). Never raises."""
    global _last_index_finish, _last_index_error

    if not RAG_ENABLED:
        return
    try:
        if not initialize_rag():
            _last_index_error = "initialize_rag_failed"
            return
    except Exception as exc_init:
        logger.error("RAG build_index early exit: %s", exc_init)
        _last_index_error = str(exc_init)[:200]
        return

    conn = None
    t0 = time.time()
    try:
        conn = get_connection()

        _build_returns_collection(conn)
        _build_clients_collection(conn)
        _build_documents_collection(conn)
        _build_form_data_collection(conn)

        _last_index_finish = time.time()
        _last_index_error = None
        logger.info("RAG: full index rebuilt in %.1fs", _last_index_finish - t0)
    except Exception as exc:
        _last_index_error = str(exc)[:500]
        logger.exception("RAG: build_index failed: %s", exc)
    finally:
        try:
            if conn is not None:
                conn.close()
        except Exception:
            pass


def _upsert_batches(
    col: Any,
    ids: list[str],
    docs: list[str],
    metas: list[dict[str, Any]],
    batch_size: int = 100,
) -> None:
    total = len(ids)
    for i in range(0, total, batch_size):
        batch_ids = ids[i : i + batch_size]
        batch_docs = docs[i : i + batch_size]
        batch_meta = metas[i : i + batch_size]
        cleaned_meta: list[dict[str, str | int | float]] = []
        for m in batch_meta:
            nm: dict[str, str | int | float] = {}
            for k, v in m.items():
                if v is None:
                    continue
                if isinstance(v, (int, float, str, bool)):
                    nm[str(k)] = v if isinstance(v, (int, float, bool)) else str(v)[:499]
                else:
                    nm[str(k)] = str(v)[:499]
            cleaned_meta.append(nm)
        try:
            col.upsert(ids=batch_ids, documents=batch_docs, metadatas=cleaned_meta)
        except Exception:
            logger.exception("RAG: upsert batch failed at offset %s", i)
        if i and i % 500 == 0:
            logger.info("RAG: upserted %s/%s into %s", min(i + batch_size, total), total, col.name)


def _latest_note_preview(conn: Any, return_id: int, limit: int = 200) -> str:
    try:
        row = conn.execute(
            """
            SELECT note_text FROM notes
            WHERE return_id = ?
            ORDER BY datetime(COALESCE(created_at,'')) DESC, id DESC
            LIMIT 1
            """,
            (return_id,),
        ).fetchone()
        if not row or not row[0]:
            return ""
        txt = _scrub_identifiers(str(row[0]))
        return txt[:limit] + ("..." if len(txt) > limit else "")
    except Exception:
        return ""


def _payment_row(conn: Any, return_id: int) -> tuple[str, str]:
    try:
        row = conn.execute(
            """
            SELECT total_fee, fee_paid FROM payments
            WHERE return_id = ?
            ORDER BY rowid DESC
            LIMIT 1
            """,
            (return_id,),
        ).fetchone()
        if row:
            return str(row["total_fee"] or ""), str(row["fee_paid"] or "")
    except Exception:
        pass
    return "", ""


def _display_name_parts(crow: dict) -> str:
    dn = crow.get("display_name") if isinstance(crow, dict) else None
    if dn:
        return str(dn).strip()
    ln = crow.get("last_name") or ""
    fn = crow.get("first_name") or ""
    parts = []
    if str(ln).strip():
        parts.append(str(ln).strip())
    if str(fn).strip():
        parts.append(str(fn).strip())
    return ", ".join(parts) if parts else "Unknown"


def _build_returns_collection(conn: Any) -> None:
    col = _collections.get("returns")
    if col is None:
        return

    rows = conn.execute(
        """
        SELECT r.id AS id,
               r.client_status, r.tax_year, r.processor,
               r.intake_date, r.log_number,
               c.display_name AS display_name,
               c.last_name AS last_name,
               c.first_name AS first_name
        FROM returns r
        JOIN clients c ON c.id = r.client_id
        """
    ).fetchall()

    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, Any]] = []

    for row in rows:
        rd = dict(row)
        rid = int(rd["id"])
        display = _scrub_identifiers(_display_name_parts(rd))
        fee, paid = _payment_row(conn, rid)
        note_pre = _latest_note_preview(conn, rid)

        txt = (
            f"Return for: {display}\n"
            f"Status: {_safe_str(rd.get('client_status'))}\n"
            f"Tax year: {_safe_str(rd.get('tax_year'))}\n"
            f"Preparer: {_safe_str(rd.get('processor'))}\n"
            f"Intake date: {_safe_str(rd.get('intake_date'))}\n"
            f"Log number: {_safe_str(rd.get('log_number'))}\n"
            f"Fee: {fee or '—'} billed, {paid or '—'} paid\n"
            + (f"Notes summary: {note_pre}\n" if note_pre else "")
        )
        txt = _scrub_identifiers(txt)
        ids.append(f"return_{rid}")
        docs.append(txt)
        ty_raw = rd.get("tax_year")
        ty_meta = ""
        try:
            if ty_raw is not None and str(ty_raw).strip() != "":
                ty_meta = str(int(ty_raw))
        except (TypeError, ValueError):
            ty_meta = str(ty_raw).strip() if ty_raw is not None else ""

        metas.append(
            {
                "return_id": rid,
                "display_name": display[:498],
                "status": (_safe_str(rd.get("client_status")) or "")[:120],
                "tax_year": ty_meta[:12],
                "processor": (_safe_str(rd.get("processor")) or "")[:200],
                "intake_date": (_safe_str(rd.get("intake_date")) or "")[:40],
            }
        )

    _upsert_batches(col, ids, docs, metas)


def _build_clients_collection(conn: Any) -> None:
    col = _collections.get("clients")
    if col is None:
        return

    rows = conn.execute(
        """
        SELECT id, display_name, last_name, first_name, referred_by
        FROM clients
        """
    ).fetchall()

    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, Any]] = []

    for row in rows:
        rd = dict(row)
        cid = int(rd["id"])
        dn = _scrub_identifiers(_safe_str(rd.get("display_name")) or _display_name_parts(rd))

        referral = rd.get("referred_by")
        ref_txt = ""
        if referral:
            ref_txt = f"Referred by: {_safe_str(referral)}\n"

        txt = (
            f"Client: {dn}\n"
            f"Last name: {_safe_str(rd.get('last_name'))}\n"
            f"First name: {_safe_str(rd.get('first_name'))}\n"
            + ref_txt
        ).strip() + "\n"
        txt = _scrub_identifiers(txt)

        ids.append(f"client_{cid}")
        docs.append(txt)
        metas.append(
            {
                "client_id": cid,
                "display_name": dn[:498],
                "last_name": (_safe_str(rd.get("last_name")) or "")[:200],
                "first_name": (_safe_str(rd.get("first_name")) or "")[:200],
            }
        )

    _upsert_batches(col, ids, docs, metas)


def _build_documents_collection(conn: Any) -> None:
    col = _collections.get("documents")
    if col is None:
        return

    rows = conn.execute(
        """
        SELECT d.id AS doc_id, d.filename, d.doc_type, d.return_id, d.source, d.uploaded_at,
               c.display_name AS display_name, c.first_name AS first_name, c.last_name AS last_name
        FROM return_documents d
        JOIN returns r ON r.id = d.return_id
        JOIN clients c ON c.id = r.client_id
        WHERE COALESCE(d.is_deleted, 0) = 0
        """
    ).fetchall()

    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, Any]] = []

    for row in rows:
        rd = dict(row)
        did = int(rd["doc_id"])
        rn = rd.get("return_id") or ""
        dn = _scrub_identifiers(
            _safe_str(rd.get("display_name")) or _display_name_parts(rd),
        )

        txt = (
            f"Document: {_safe_str(rd.get('filename'))}\n"
            f"Type: {_safe_str(rd.get('doc_type'))}\n"
            f"For return: {rn}\n"
            f"Client: {dn}\n"
            f"Source: {_safe_str(rd.get('source'))}\n"
            f"Uploaded: {_safe_str(rd.get('uploaded_at'))}\n"
        )
        txt = _scrub_identifiers(txt)
        ids.append(f"document_{did}")
        docs.append(txt)
        rid_v = rn
        try:
            rid_meta = int(rid_v)
        except (TypeError, ValueError):
            rid_meta = -1

        metas.append(
            {
                "doc_id": did,
                "return_id": rid_meta if rid_meta > 0 else 0,
                "doc_type": (_safe_str(rd.get("doc_type")) or "")[:120],
                "source": (_safe_str(rd.get("source")) or "")[:120],
                "display_name": dn[:498],
            }
        )

    _upsert_batches(col, ids, docs, metas)


def _append_form_rows_generic(
    col: Any,
    table: str,
    label: str,
    fmt,
    *,
    conn: Any,
) -> tuple[list[str], list[str], list[dict]]:
    ids: list[str] = []
    docs: list[str] = []
    metas: list[dict[str, Any]] = []

    try:
        rows = conn.execute(
            f"""
            SELECT fr.*,
                   COALESCE(c.display_name,
                       TRIM(COALESCE(c.last_name,'') || CASE WHEN COALESCE(TRIM(c.first_name),'') != ''
                             THEN ', ' || c.first_name ELSE '' END)
                   ) AS client_display_name
            FROM {table} fr
            JOIN returns ret ON ret.id = fr.return_id
            JOIN clients c ON c.id = ret.client_id
            WHERE COALESCE(fr.is_deleted, 0) = 0
            """
        ).fetchall()
    except Exception:
        logger.warning("RAG: skip indexing table %s (missing or inaccessible)", table)
        return ids, docs, metas

    for row in rows:
        frd = dict(row)
        fid = frd.get("id")
        if fid is None:
            continue
        rid = frd.get("return_id")

        dn = _scrub_identifiers(_safe_str(frd.get("client_display_name")) or "Unknown")

        txt, meta_override = fmt(frd, dn, label)
        txt = _scrub_identifiers(txt)
        fid_i = int(fid)
        ids.append(f"{table}_{fid_i}")
        docs.append(txt)
        try:
            ridi = int(rid)
        except (TypeError, ValueError):
            ridi = 0
        ty = str(frd.get("tax_year") or "")[:40]
        m: dict[str, Any] = {
            "record_id": fid_i,
            "return_id": ridi,
            "form_type": label[:40],
            "display_name": dn[:498],
            "tax_year": ty,
        }
        m.update(meta_override or {})
        metas.append(m)

    return ids, docs, metas


def _fmt_w2(frd: dict, dn: str, label: str) -> tuple[str, dict]:
    wages = (
        frd.get("box1_wages_tips_other")
        or frd.get("wages_tips_other")
        or ""
    )
    withhold = (
        frd.get("box2_federal_income_tax_withheld")
        or frd.get("federal_income_tax_withheld")
        or ""
    )
    txt = (
        f"W-2 for return {frd.get('return_id')}\n"
        f"Client: {dn}\n"
        f"Employer: {_safe_str(frd.get('employer_name'))}\n"
        f"Wages: {_safe_str(wages)}\n"
        f"Federal tax withheld: {_safe_str(withhold)}\n"
        f"State: {_safe_str(frd.get('box15_state'))}\n"
        f"State wages: {_safe_str(frd.get('box16_state_wages'))}\n"
        f"State tax: {_safe_str(frd.get('box17_state_income_tax'))}\n"
        f"Tax year: {_safe_str(frd.get('tax_year'))}\n"
    )
    return txt, {"form_subtype": label}


def _fmt_1099_nec(frd: dict, dn: str, label: str) -> tuple[str, dict]:
    txt = (
        f"{label} for return {frd.get('return_id')}\n"
        f"Client: {dn}\n"
        f"Payer: {_safe_str(frd.get('payer_name'))}\n"
        f"Nonemployee compensation: {_safe_str(frd.get('box1_nonemployee_compensation') or frd.get('nonemployee_compensation'))}\n"
        f"Federal tax withheld: {_safe_str(frd.get('box4_federal_income_tax_withheld'))}\n"
        f"State: {_safe_str(frd.get('box6_state'))}\n"
        f"Tax year: {_safe_str(frd.get('tax_year'))}\n"
    )
    return txt, {}


def _fmt_1099_misc(frd: dict, dn: str, label: str) -> tuple[str, dict]:
    txt = (
        f"{label} for return {frd.get('return_id')}\n"
        f"Client: {dn}\n"
        f"Payer: {_safe_str(frd.get('payer_name'))}\n"
        f"Rents: {_safe_str(frd.get('box1_rents'))}\n"
        f"Other income: {_safe_str(frd.get('box3_other_income'))}\n"
        f"Royalties: {_safe_str(frd.get('box2_royalties'))}\n"
        f"Federal tax withheld: {_safe_str(frd.get('box4_federal_income_tax_withheld'))}\n"
        f"Tax year: {_safe_str(frd.get('tax_year'))}\n"
    )
    return txt, {}


def _fmt_1099_int(frd: dict, dn: str, label: str) -> tuple[str, dict]:
    txt = (
        f"{label} for return {frd.get('return_id')}\n"
        f"Client: {dn}\n"
        f"Payer: {_safe_str(frd.get('payer_name'))}\n"
        f"Interest income: {_safe_str(frd.get('box1_interest_income') or frd.get('interest_income'))}\n"
        f"Federal tax withheld: {_safe_str(frd.get('box4_federal_income_tax_withheld'))}\n"
        f"Tax year: {_safe_str(frd.get('tax_year'))}\n"
    )
    return txt, {}


def _fmt_1099_div(frd: dict, dn: str, label: str) -> tuple[str, dict]:
    txt = (
        f"{label} for return {frd.get('return_id')}\n"
        f"Client: {dn}\n"
        f"Payer: {_safe_str(frd.get('payer_name'))}\n"
        f"Ordinary dividends: {_safe_str(frd.get('box1a_total_ordinary_dividends'))}\n"
        f"Qualified dividends: {_safe_str(frd.get('box1b_qualified_dividends'))}\n"
        f"Federal tax withheld: {_safe_str(frd.get('box4_federal_income_tax_withheld'))}\n"
        f"Tax year: {_safe_str(frd.get('tax_year'))}\n"
    )
    return txt, {}


def _build_form_data_collection(conn: Any) -> None:
    col = _collections.get("form_data")
    if col is None:
        return

    all_ids: list[str] = []
    all_docs: list[str] = []
    all_metas: list[dict[str, Any]] = []

    for table, label, fmt in (
        ("w2_records", "W-2", _fmt_w2),
        ("f1099_nec_records", "1099-NEC", _fmt_1099_nec),
        ("f1099_misc_records", "1099-MISC", _fmt_1099_misc),
        ("f1099_int_records", "1099-INT", _fmt_1099_int),
        ("f1099_div_records", "1099-DIV", _fmt_1099_div),
    ):
        ia, db, mb = _append_form_rows_generic(col, table, label, fmt, conn=conn)
        all_ids.extend(ia)
        all_docs.extend(db)
        all_metas.extend(mb)

    if all_ids:
        _upsert_batches(col, all_ids, all_docs, all_metas)


def retrieve(
    query: str,
    collection_name: str = "returns",
    top_k: int | None = None,
    filters: dict | None = None,
) -> list[dict[str, Any]]:
    """Semantic search; returns [{'text','metadata','distance'}]. Never raises."""
    k = top_k if top_k is not None else RAG_TOP_K
    if not query or not str(query).strip():
        return []
    try:
        if not RAG_ENABLED or not initialize_rag():
            return []
        col = _collections.get(collection_name)
        if col is None:
            return []

        kw: dict[str, Any] = {"query_texts": [_scrub_identifiers(query.strip())], "n_results": k}

        try:
            cand_count = int(col.count())
        except Exception:
            cand_count = None

        if cand_count is not None and cand_count <= 0:
            return []

        kw["n_results"] = max(1, min(k, cand_count if cand_count is not None else k))

        wr: dict[str, Any] | None = None
        if filters:
            clauses = []
            for fk, fv in filters.items():
                if fv is None or str(fv).strip() == "":
                    continue
                clauses.append({str(fk): str(fv)})
            if clauses:
                if len(clauses) == 1:
                    wr = clauses[0]
                else:
                    wr = {"$and": clauses}

        if wr:
            kw["where"] = wr

        res = col.query(**kw)
        ids0 = (res.get("ids") or [[]])[0]
        docs0 = (res.get("documents") or [[]])[0]
        meta0 = (res.get("metadatas") or [[]])[0]
        dist0 = (res.get("distances") or [[]])[0]

        out: list[dict[str, Any]] = []
        for i, cid in enumerate(ids0):
            d_val = docs0[i] if i < len(docs0) else ""
            m_val = meta0[i] if i < len(meta0) else {}
            dit = None
            if dist0 is not None and i < len(dist0):
                try:
                    dit = float(dist0[i])
                except (TypeError, ValueError):
                    dit = None
            out.append(
                {
                    "text": _scrub_identifiers(str(d_val or "")),
                    "metadata": m_val if isinstance(m_val, dict) else {},
                    "distance": dit if dit is not None else 1.0,
                }
            )
        return out
    except Exception as exc:
        logger.warning("RAG retrieve failed (%s): %s", collection_name, exc)
        return []


def retrieve_for_question(question: str, year: int) -> list[dict[str, Any]]:
    q = question.lower()
    merged: list[dict[str, Any]] = []

    if any(t in q for t in ("find", "search", "who is", "family", "client")):
        merged += retrieve(question, "clients", top_k=5)
        merged += retrieve(
            question,
            "returns",
            top_k=5,
            filters={"tax_year": str(int(year))},
        )

    if any(t in q for t in ("document", "w-2", "1099", "filed", "uploaded")):
        merged += retrieve(question, "documents", top_k=5)
        merged += retrieve(question, "form_data", top_k=5)

    if not merged:
        merged += retrieve(
            question,
            "returns",
            top_k=RAG_TOP_K,
            filters={"tax_year": str(int(year))},
        )

    seen: set[Any] = set()
    deduped: list[dict[str, Any]] = []

    merged_sorted = sorted(merged, key=lambda x: float(x.get("distance") or 1.0))
    for r in merged_sorted:
        rid = None
        if isinstance(r.get("metadata"), dict):
            rid = r["metadata"].get("return_id")
        if rid is not None:
            rid_i = rid
            try:
                rid_i = int(rid_i)
            except (TypeError, ValueError):
                rid_i = rid
            if rid_i not in seen:
                seen.add(rid_i)
                deduped.append(r)
            continue
        deduped.append(r)

    return deduped[: RAG_TOP_K * 2]


def format_retrieved_context(results: list[dict[str, Any]]) -> str:
    """Format retrieved documents for the LLM prompt; never includes raw SSN/TIN strings."""
    if not results:
        return ""
    lines = ["--- RETRIEVED CONTEXT (semantically relevant records) ---"]
    for i, r in enumerate(results, 1):
        lines.append(f"{i}. {_safe_str(r.get('text'))}")
    lines.append("--- END RETRIEVED CONTEXT ---")
    return "\n".join(lines)


def start_rag_worker(app) -> None:
    """Background periodic rebuild inside application context."""

    def _loop() -> None:
        idle = float(RAG_REBUILD_INTERVAL)
        if idle < 60:
            idle = 60
        initial_delay = float(os.environ.get("RAG_STARTUP_DELAY_SEC", "8"))
        if initial_delay < 2:
            initial_delay = 2.0
        time.sleep(initial_delay)
        while True:
            try:
                with app.app_context():
                    build_index(app)
            except Exception:
                logger.exception("RAG worker loop tick failed")
            time.sleep(idle)

    if not RAG_ENABLED:
        return
    threading.Thread(target=_loop, daemon=True, name="rag-worker").start()


def get_rag_stats() -> dict[str, Any]:
    counts = {name: 0 for name in _collection_names}
    available = False
    try:
        available = initialize_rag() and bool(_collections)
        if available:
            for nm, col in _collections.items():
                try:
                    counts[nm] = int(col.count())
                except Exception:
                    counts[nm] = 0
    except Exception:
        available = False

    try:
        rel_db = os.path.relpath(str(RAG_DB_PATH), start=str(_HERE))
    except ValueError:
        rel_db = "chroma_db"

    stats: dict[str, Any] = {
        "available": bool(available),
        "collections": counts,
        "index_age_seconds": None,
        "embedding_model": RAG_EMBEDDING_MODEL if RAG_EMBEDDING_MODEL else "",
        "db_path": rel_db.replace("\\", "/"),
    }

    global _last_index_finish
    if _last_index_finish > 0:
        stats["index_age_seconds"] = round(time.time() - _last_index_finish, 2)
    return stats
