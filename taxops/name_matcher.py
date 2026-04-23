"""
name_matcher.py
~~~~~~~~~~~~~~~
Fuzzy client-name matching for CSV imports.

Strategy (in priority order):
  1. Exact match on upper(last_name) + upper(first_name)
  2. Exact match on last_name only when first_name is absent
  3. Fuzzy match using token_sort_ratio on the full normalized name
  4. Fuzzy match on last_name only (handles compound surnames)

A match is accepted when score >= ACCEPT_THRESHOLD.
Scores between REVIEW_THRESHOLD and ACCEPT_THRESHOLD are sent to review_queue.
Scores below REVIEW_THRESHOLD create a new client.
"""

import re
import sqlite3
from typing import Optional

from rapidfuzz import fuzz

# ── Thresholds ────────────────────────────────────────────────────────────────
ACCEPT_THRESHOLD = 88   # auto-match
REVIEW_THRESHOLD = 70   # flag for human review; below this → new client

# ── Name suffix / noise tokens ────────────────────────────────────────────────
_SUFFIXES = frozenset(["JR", "SR", "II", "III", "IV", "V", "ESQ", "CPA", "MD", "DDS"])

# ── Business keywords — treated as business returns, not individuals ──────────
_BIZ_TOKENS = frozenset([
    "INC", "LLC", "CORP", "CORPORATION", "LLP", "LP", "PC",
    "CO", "COMPANY", "ENTERPRISES", "GROUP", "SERVICES", "TRUST",
    "FOUNDATION", "ASSOCIATION", "PARTNERS", "PARTNERSHIP",
])


def _clean(name: str) -> str:
    """Uppercase, strip punctuation noise, collapse whitespace."""
    name = name.upper()
    name = re.sub(r"[.,&;]", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _strip_suffixes(tokens: list[str]) -> list[str]:
    return [t for t in tokens if t not in _SUFFIXES]


def normalize_name(raw: str) -> str:
    """Return a cleaned, suffix-stripped version of a raw name string."""
    return " ".join(_strip_suffixes(_clean(raw).split()))


def is_business(last_name: str, first_name: Optional[str] = None) -> bool:
    """Return True if the name looks like a business / organization."""
    combined = _clean(f"{last_name} {first_name or ''}")
    tokens = set(combined.split())
    return bool(tokens & _BIZ_TOKENS)


def parse_name(raw: str) -> tuple[str, Optional[str]]:
    """
    Parse a free-form name string into (last_name, first_name).

    Handles:
    - "LAST, FIRST"          → last=LAST, first=FIRST
    - "LAST FIRST"           → attempts to split on known patterns
    - "BOCANEGRA GALLEGOS, URIEL & ADRIANA" → last=BOCANEGRA GALLEGOS, first=URIEL
    - "CORNWELL IV, JOHN"    → strips suffix from last
    - Businesses passed through as-is with first=None
    """
    raw = _clean(raw)

    if not raw:
        return "", None

    # Business — return as-is
    if is_business(raw):
        return raw, None

    # "LAST, FIRST [& SPOUSE]" — comma-separated
    if "," in raw:
        parts = raw.split(",", 1)
        last_raw = parts[0].strip()
        first_raw = parts[1].strip() if len(parts) > 1 else ""

        # Strip "& SPOUSE_NAME" from first → keep only primary taxpayer
        first_raw = re.split(r"\s*&\s*", first_raw)[0].strip()

        # Strip suffixes from last name portion
        last_tokens = _strip_suffixes(last_raw.split())
        last = " ".join(last_tokens)

        first = first_raw if first_raw else None
        return last, first

    # No comma — return whole string as last_name (business or single-word)
    tokens = _strip_suffixes(raw.split())
    return " ".join(tokens), None


# ── Database matching ─────────────────────────────────────────────────────────

MatchResult = dict  # keys: client_id, score, method, last_name, first_name


def _all_clients_cache(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all clients once per import session for fast in-memory matching."""
    rows = conn.execute(
        "SELECT id, upper(trim(last_name)) as ln, upper(trim(COALESCE(first_name,''))) as fn FROM clients"
    ).fetchall()
    return [{"id": r["id"], "ln": r["ln"], "fn": r["fn"]} for r in rows]


def find_client(
    conn: sqlite3.Connection,
    last_name: str,
    first_name: Optional[str],
    *,
    cache: Optional[list] = None,
) -> Optional[MatchResult]:
    """
    Return the best matching client or None.

    Result dict:
        client_id : int
        score     : int   (0-100; 100 = exact)
        method    : str   ('exact', 'fuzzy_full', 'fuzzy_last')
        needs_review : bool
    """
    last_norm  = normalize_name(last_name)
    first_norm = normalize_name(first_name) if first_name else ""
    full_norm  = (last_norm + " " + first_norm).strip()

    if cache is None:
        cache = _all_clients_cache(conn)

    best_score  = 0
    best_client = None
    best_method = ""

    for c in cache:
        c_ln = normalize_name(c["ln"])
        c_fn = normalize_name(c["fn"])
        c_full = (c_ln + " " + c_fn).strip()

        # 1. Exact match
        if c_ln == last_norm:
            if not first_norm or c_fn == first_norm:
                return {
                    "client_id": c["id"],
                    "score": 100,
                    "method": "exact",
                    "needs_review": False,
                }

        # 2. Fuzzy full name
        score_full = fuzz.token_sort_ratio(full_norm, c_full)
        # 3. Fuzzy last-name only (helps with compound surnames)
        score_last = fuzz.token_sort_ratio(last_norm, c_ln)

        # Weight: full name match takes priority; last-only is a fallback
        score = max(score_full, int(score_last * 0.90))
        method = "fuzzy_full" if score_full >= score_last else "fuzzy_last"

        if score > best_score:
            best_score  = score
            best_client = c
            best_method = method

    if best_client is None or best_score < REVIEW_THRESHOLD:
        return None

    return {
        "client_id": best_client["id"],
        "score": best_score,
        "method": best_method,
        "needs_review": best_score < ACCEPT_THRESHOLD,
    }
