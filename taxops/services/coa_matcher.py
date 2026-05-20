"""ACCOUNTING-4: Chart-of-Accounts matcher.

Primary strategy: sentence-transformers all-MiniLM-L6-v2 cosine similarity.
Fallback (when sentence-transformers / numpy are not installed):
  rapidfuzz token_set_ratio string matching.

Usage
-----
    from services.coa_matcher import CoaMatcher
    matcher = CoaMatcher()          # loads COA CSV + embeddings (lazy)
    result  = matcher.categorize("Office supplies from Staples")
    # result = {
    #   "suggested_category": "Office Supplies",
    #   "suggested_account":  "6100",
    #   "confidence":         "high",
    #   "embedding_score":    0.912,
    #   "candidates": [
    #     {"account_code": "6100", "account_name": "Office Supplies", "score": 0.912},
    #     ...
    #   ],
    # }

COA CSV columns (any order; headers are normalised):
  Required:  account_name  (or "Account Name", "name", "Name")
  Optional:  account_code  (or "Account Code", "Account #", "number"),
             account_type  (or "Type"), description  (or "Description")

Embeddings cache: ``<taxops_dir>/data/coa_embeddings.npy``
                  ``<taxops_dir>/data/coa_labels.json``

Call ``matcher.rebuild()`` to force recompute (e.g., after updating the COA CSV).
"""

from __future__ import annotations

import csv
import json
import logging
import os
import threading
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_HERE = Path(__file__).parent.parent  # taxops/ directory
_EMBED_NPY = _HERE / "data" / "coa_embeddings.npy"
_LABELS_JSON = _HERE / "data" / "coa_labels.json"

_TOP_K = 5  # number of candidates to return


def _col(row: dict, *candidates: str) -> str:
    """Return the first matching key from a CSV row dict (case-insensitive)."""
    lower = {k.lower().strip(): v for k, v in row.items()}
    for c in candidates:
        v = lower.get(c.lower())
        if v is not None:
            return v.strip()
    return ""


def _load_coa_csv(path: str) -> list[dict]:
    """Load COA entries from CSV.  Returns list of dicts with account_code, account_name,
    account_type, description, embed_text (combined text for embedding).
    """
    entries = []
    with open(path, newline="", encoding="utf-8-sig") as fh:
        reader = csv.DictReader(fh)
        for row in reader:
            name = _col(row, "account_name", "Account Name", "name", "Name", "ACCOUNT NAME")
            if not name:
                continue
            code = _col(row, "account_code", "Account Code", "Account #", "number", "Number", "ACCOUNT CODE", "Acct #")
            atype = _col(row, "account_type", "Account Type", "Type", "type")
            desc = _col(row, "description", "Description", "desc")
            embed_text = " ".join(filter(None, [name, atype, desc]))
            entries.append({
                "account_code": code,
                "account_name": name,
                "account_type": atype,
                "description": desc,
                "embed_text": embed_text,
            })
    return entries


def _confidence_band(score: float) -> str:
    import config as _cfg
    if score >= _cfg.ACCOUNTING_CONFIDENCE_HIGH:
        return "high"
    if score >= _cfg.ACCOUNTING_CONFIDENCE_MEDIUM:
        return "medium"
    return "low"


class CoaMatcher:
    """Thread-safe COA matcher.  One instance per application (use module-level singleton)."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._entries: list[dict] = []
        self._embeddings: Any = None  # numpy array or None
        self._loaded = False
        self._use_embeddings = False

    # ── Public API ─────────────────────────────────────────────────────────────

    def load(self, coa_csv_path: str | None = None) -> bool:
        """Load COA entries.  Returns True if successful, False on error."""
        import config as _cfg
        path = coa_csv_path or _cfg.COA_CSV_PATH
        if not path or not os.path.isfile(path):
            log.warning("coa_matcher: COA_CSV_PATH not set or file missing (%r)", path)
            return False
        try:
            entries = _load_coa_csv(path)
        except Exception as exc:
            log.error("coa_matcher: failed to load COA CSV %r — %s", path, exc)
            return False
        with self._lock:
            self._entries = entries
            self._embeddings = self._load_or_build_embeddings(entries, path)
            self._loaded = True
        log.info("coa_matcher: loaded %d COA entries from %s", len(entries), path)
        return True

    def rebuild(self, coa_csv_path: str | None = None) -> bool:
        """Force recompute embeddings even if cache exists.  Used after COA CSV update."""
        import config as _cfg
        path = coa_csv_path or _cfg.COA_CSV_PATH
        if not path or not os.path.isfile(path):
            return False
        try:
            entries = _load_coa_csv(path)
            embs = self._compute_embeddings(entries)
        except Exception as exc:
            log.error("coa_matcher: rebuild failed — %s", exc)
            return False
        self._save_embeddings(embs, entries)
        with self._lock:
            self._entries = entries
            self._embeddings = embs
            self._loaded = True
        log.info("coa_matcher: rebuilt %d embeddings", len(entries))
        return True

    def categorize(self, text: str, *, top_k: int = _TOP_K) -> dict:
        """Match *text* (vendor + description) against the COA.

        Returns a dict with suggested_category, suggested_account, confidence,
        embedding_score, and candidates list.
        """
        if not self._loaded:
            self.load()

        if not self._entries:
            return {
                "suggested_category": None,
                "suggested_account": None,
                "confidence": "low",
                "embedding_score": 0.0,
                "candidates": [],
                "_error": "coa_not_loaded",
            }

        with self._lock:
            entries = self._entries
            embeddings = self._embeddings

        if embeddings is not None and self._use_embeddings:
            candidates = self._match_embeddings(text, entries, embeddings, top_k)
        else:
            candidates = self._match_rapidfuzz(text, entries, top_k)

        best = candidates[0] if candidates else None
        best_score = best["score"] if best else 0.0
        return {
            "suggested_category": best["account_name"] if best else None,
            "suggested_account":  best["account_code"] if best else None,
            "confidence":         _confidence_band(best_score),
            "embedding_score":    round(best_score, 4),
            "candidates":         candidates,
        }

    # ── Embedding methods ──────────────────────────────────────────────────────

    def _load_or_build_embeddings(self, entries: list[dict], csv_path: str) -> Any:
        """Load cached embeddings if they exist and are up-to-date; else compute."""
        if _EMBED_NPY.exists() and _LABELS_JSON.exists():
            try:
                import numpy as np
                csv_mtime = os.path.getmtime(csv_path)
                npy_mtime = _EMBED_NPY.stat().st_mtime
                if npy_mtime >= csv_mtime:
                    embs = np.load(str(_EMBED_NPY))
                    with open(_LABELS_JSON, encoding="utf-8") as fh:
                        labels = json.load(fh)
                    if len(embs) == len(labels) == len(entries):
                        self._use_embeddings = True
                        log.debug("coa_matcher: loaded %d embeddings from cache", len(embs))
                        return embs
            except Exception as exc:
                log.warning("coa_matcher: embedding cache load failed — %s", exc)

        try:
            embs = self._compute_embeddings(entries)
            self._save_embeddings(embs, entries)
            self._use_embeddings = embs is not None
            return embs
        except Exception as exc:
            log.warning("coa_matcher: embedding build failed — using rapidfuzz fallback: %s", exc)
            return None

    def _compute_embeddings(self, entries: list[dict]) -> Any:
        """Compute sentence-transformer embeddings for each COA entry."""
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        import numpy as np  # noqa: PLC0415
        texts = [e["embed_text"] for e in entries]
        model = SentenceTransformer("all-MiniLM-L6-v2")
        embs = model.encode(texts, show_progress_bar=False, convert_to_numpy=True)
        return embs.astype(np.float32)

    @staticmethod
    def _save_embeddings(embs: Any, entries: list[dict]) -> None:
        """Persist embeddings and label list to disk."""
        try:
            import numpy as np
            _EMBED_NPY.parent.mkdir(parents=True, exist_ok=True)
            np.save(str(_EMBED_NPY), embs)
            with open(_LABELS_JSON, "w", encoding="utf-8") as fh:
                json.dump([e["account_name"] for e in entries], fh)
            log.debug("coa_matcher: saved %d embeddings to %s", len(entries), _EMBED_NPY)
        except Exception as exc:
            log.warning("coa_matcher: could not save embeddings — %s", exc)

    def _match_embeddings(
        self, text: str, entries: list[dict], embeddings: Any, top_k: int
    ) -> list[dict]:
        """Cosine similarity match using pre-computed numpy embeddings."""
        from sentence_transformers import SentenceTransformer  # noqa: PLC0415
        import numpy as np  # noqa: PLC0415

        model = SentenceTransformer("all-MiniLM-L6-v2")
        query_emb = model.encode([text], show_progress_bar=False, convert_to_numpy=True)[0]
        query_emb = query_emb.astype(np.float32)

        # cosine similarity: (A · B) / (‖A‖ ‖B‖)
        norms = np.linalg.norm(embeddings, axis=1)
        norms[norms == 0] = 1e-9
        scores = (embeddings @ query_emb) / (norms * (np.linalg.norm(query_emb) + 1e-9))

        top_idx = np.argsort(scores)[::-1][:top_k]
        return [
            {
                "account_code": entries[i]["account_code"],
                "account_name": entries[i]["account_name"],
                "score":        float(scores[i]),
            }
            for i in top_idx
        ]

    @staticmethod
    def _match_rapidfuzz(text: str, entries: list[dict], top_k: int) -> list[dict]:
        """Fallback string-similarity match using rapidfuzz."""
        from rapidfuzz import fuzz  # noqa: PLC0415

        scored = []
        for e in entries:
            ratio = fuzz.token_set_ratio(text.lower(), e["embed_text"].lower()) / 100.0
            scored.append((ratio, e))
        scored.sort(key=lambda x: x[0], reverse=True)
        return [
            {
                "account_code": e["account_code"],
                "account_name": e["account_name"],
                "score":        round(s, 4),
            }
            for s, e in scored[:top_k]
        ]


# Module-level singleton — imported by routes and worker.
_matcher: CoaMatcher | None = None
_matcher_lock = threading.Lock()


def get_matcher() -> CoaMatcher:
    """Return the shared CoaMatcher singleton, initialising it on first call."""
    global _matcher
    with _matcher_lock:
        if _matcher is None:
            _matcher = CoaMatcher()
            _matcher.load()
    return _matcher


def reset_matcher() -> None:
    """Force singleton re-creation (used after COA CSV path change in settings)."""
    global _matcher
    with _matcher_lock:
        _matcher = None
