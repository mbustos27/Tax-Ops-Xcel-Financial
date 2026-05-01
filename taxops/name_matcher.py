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
    """Uppercase, normalize punctuation noise, collapse whitespace.
    Hyphens are treated as spaces so PEREZ-QUINTANA == PEREZ QUINTANA.
    """
    name = name.upper()
    name = re.sub(r"[-.,&;']", " ", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def _strip_suffixes(tokens: list[str]) -> list[str]:
    return [t for t in tokens if t not in _SUFFIXES]


def normalize_name(raw: str) -> str:
    """Return a cleaned, suffix-stripped version of a raw name string."""
    return " ".join(_strip_suffixes(_clean(raw).split()))


def strip_middle_initial(first: str) -> str:
    """
    Remove a single-letter middle initial from a first name string.
    'AMJAD G' → 'AMJAD', 'JOHN A' → 'JOHN', 'MARY ANN' → 'MARY ANN' (unchanged)
    """
    tokens = first.strip().split()
    if len(tokens) >= 2 and len(tokens[-1]) == 1 and tokens[-1].isalpha():
        return " ".join(tokens[:-1])
    return first


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


# ── Joint filers / manual-office log ───────────────────────────────────────────

_SPLIT_JOINT_FIRST = re.compile(r"\s*&\s*", re.I)


def score_client_names_pair(
    last_a: Optional[str],
    first_a: Optional[str],
    last_b: Optional[str],
    first_b: Optional[str],
) -> int:
    """
    Similarity score 0–100 between two ``(last, first)`` pairs (same heuristic as ``find_client``).
    Used to verify manual log names against an existing DB return keyed by LOG + tax season.
    """
    ln_a = normalize_name(last_a or "")
    fn_a = normalize_name(strip_spouse(first_a or ""))
    ln_b = normalize_name(last_b or "")
    fn_b = normalize_name(strip_spouse(first_b or ""))
    if not ln_a or not ln_b:
        return 0
    # If last names match and first names match after stripping middle initial → near-exact
    if ln_a == ln_b and fn_a and fn_b:
        if strip_middle_initial(fn_a) == strip_middle_initial(fn_b):
            return 97
    full_a = (ln_a + " " + fn_a).strip()
    full_b = (ln_b + " " + fn_b).strip()
    score_full = fuzz.token_sort_ratio(full_a, full_b)
    score_last = fuzz.token_sort_ratio(ln_a, ln_b)
    return int(max(score_full, int(score_last * 0.90)))


def split_joint_first_column(first_cell: str) -> tuple[str, Optional[str]]:
    """
    FIRST column values like ``JOHN & JANE`` split into taxpayer first vs spouse substring.
    """
    raw = (first_cell or "").strip()
    if not raw:
        return "", None
    if "&" not in raw.upper():
        return raw, None
    chunks = _SPLIT_JOINT_FIRST.split(raw)
    chunks = [c.strip() for c in chunks if c.strip()]
    if len(chunks) < 2:
        return raw, None
    primary = chunks[0]
    spouse_rest = chunks[1] if len(chunks) >= 2 else ""
    if len(chunks) > 2:
        spouse_rest = spouse_rest + " " + " ".join(chunks[2:])
    return primary, spouse_rest or None


def split_spouse_name_chunk(chunk: str) -> tuple[str, str]:
    """ spouse fragment ``JANE`` or ``DOE JANE`` → (first_tokens, rest_as_last_or_empty). """
    s = normalize_name(chunk)
    if not s:
        return "", ""
    toks = s.split()
    if len(toks) == 1:
        return toks[0], ""
    return toks[0], " ".join(toks[1:])


def spouse_parts_from_display_line(display_raw: Optional[str]) -> Optional[tuple[str, str]]:
    """``TAX PAYER NAME (S)``: ``LAST, JOHN & JANE`` → spouse first last parts when ``&`` after comma."""
    raw = (display_raw or "").strip()
    if not raw or "," not in raw or "&" not in raw.upper():
        return None
    _, rest = raw.split(",", 1)
    rest = rest.strip()
    chunks = [c.strip() for c in _SPLIT_JOINT_FIRST.split(rest) if c.strip()]
    if len(chunks) < 2:
        return None
    return split_spouse_name_chunk(chunks[1])


# ── Database matching ─────────────────────────────────────────────────────────

MatchResult = dict  # keys: client_id, score, method, last_name, first_name


def _all_clients_cache(conn: sqlite3.Connection) -> list[dict]:
    """Fetch all clients once per import session for fast in-memory matching."""
    rows = conn.execute(
        "SELECT id, upper(trim(last_name)) as ln, upper(trim(COALESCE(first_name,''))) as fn FROM clients"
    ).fetchall()
    return [{"id": r["id"], "ln": r["ln"], "fn": r["fn"]} for r in rows]


def strip_spouse(first: str) -> str:
    """Strip '& SPOUSE NAME' from a joint first-name string.
    'GANEM B & MONA N' → 'GANEM B', 'JOHN' → 'JOHN' (unchanged)
    """
    if "&" in first:
        return re.split(r"\s*&\s*", first, maxsplit=1)[0].strip()
    return first


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
    last_norm      = normalize_name(last_name)
    # Strip joint spouse part before matching — "GANEM B & MONA N" → "GANEM B"
    first_primary  = strip_spouse(first_name) if first_name else (first_name or "")
    first_norm     = normalize_name(first_primary) if first_primary else ""
    first_stripped = strip_middle_initial(first_norm)   # "AMJAD G" → "AMJAD"
    full_norm      = (last_norm + " " + first_norm).strip()

    if cache is None:
        cache = _all_clients_cache(conn)

    best_score  = 0
    best_client = None
    best_method = ""

    for c in cache:
        c_ln = normalize_name(c["ln"])
        c_fn = normalize_name(c["fn"])
        c_fn_stripped = strip_middle_initial(c_fn)
        c_full = (c_ln + " " + c_fn).strip()

        # 1. Exact match (with and without middle initial)
        if c_ln == last_norm:
            if not first_norm or c_fn == first_norm:
                return {
                    "client_id": c["id"],
                    "score": 100,
                    "method": "exact",
                    "needs_review": False,
                }
            # Middle-initial-stripped exact match — treat as near-exact (score 97)
            if first_stripped and c_fn_stripped == first_stripped:
                if 97 > best_score:
                    best_score  = 97
                    best_client = c
                    best_method = "exact_no_initial"

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
