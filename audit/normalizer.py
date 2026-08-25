"""
Audit-only name normalizer.

Do NOT import or modify taxops/name_matcher.py. This module is deliberately
separate so production import/email thresholds stay untouched.
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field
from typing import Iterable, Optional


_SUFFIXES = frozenset({"JR", "SR", "II", "III", "IV", "V", "ESQ", "CPA", "MD", "DDS"})

# Longest-first so "DE LOS" wins over "DE" / "LOS".
_PARTICLES: tuple[str, ...] = (
    "DE LOS",
    "DE LAS",
    "DE LA",
    "DEL",
    "DE",
    "LA",
    "LAS",
    "LOS",
)

_AMP_SPLIT = re.compile(r"\s*&\s*")
_NON_ALNUM = re.compile(r"[^A-Z0-9\s]")
_WS = re.compile(r"\s+")


def fold_accents(text: str) -> str:
    """NFKD → strip combining marks (ñ→n, é→e)."""
    if not text:
        return ""
    nk = unicodedata.normalize("NFKD", text)
    return "".join(ch for ch in nk if not unicodedata.combining(ch))


def clean_tokens(raw: str) -> list[str]:
    """Uppercase, fold accents, drop punctuation, split on whitespace."""
    s = fold_accents(raw or "").upper()
    s = s.replace("-", " ")
    s = _NON_ALNUM.sub(" ", s)
    s = _WS.sub(" ", s).strip()
    if not s:
        return []
    return [t for t in s.split(" ") if t]


def strip_suffixes(tokens: Iterable[str]) -> list[str]:
    return [t for t in tokens if t not in _SUFFIXES]


def is_single_letter(token: str) -> bool:
    return len(token) == 1 and token.isalpha()


def strip_trailing_initials(tokens: list[str]) -> list[str]:
    """Trailing single-letter tokens are initials, never surnames."""
    out = list(tokens)
    while out and is_single_letter(out[-1]):
        out.pop()
    return out


def particle_span_len(tokens: list[str], start: int) -> int:
    """Return how many tokens from `start` form a known particle (0 if none)."""
    upper = " ".join(tokens[start:])
    for part in _PARTICLES:
        plen = len(part.split())
        if upper.startswith(part) and (
            len(tokens) == start + plen
            or upper[len(part) : len(part) + 1] in ("", " ")
        ):
            # Exact token alignment
            if tokens[start : start + plen] == part.split():
                return plen
    return 0


def surname_variants(last_tokens: list[str]) -> list[str]:
    """
    Full surname plus paternal-only (drop trailing maternal block).

    Particles bind to the following surname token.
    Example: DE LA CRUZ GARCIA → full + DE LA CRUZ (paternal-only drop GARCIA).
    """
    toks = strip_trailing_initials(strip_suffixes(last_tokens))
    if not toks:
        return []

    # Build surname "units": optional particle(s) + head token.
    units: list[list[str]] = []
    i = 0
    while i < len(toks):
        plen = particle_span_len(toks, i)
        if plen and i + plen < len(toks):
            units.append(toks[i : i + plen + 1])
            i += plen + 1
        else:
            units.append([toks[i]])
            i += 1

    full = " ".join(t for u in units for t in u)
    variants = [full]
    if len(units) >= 2:
        paternal = " ".join(t for u in units[:-1] for t in u)
        if paternal and paternal != full:
            variants.append(paternal)
    return variants


def first_token(first_tokens: list[str]) -> str:
    toks = strip_trailing_initials(strip_suffixes(first_tokens))
    return toks[0] if toks else ""


@dataclass
class NormalizedName:
    raw: str
    last_raw: str = ""
    first_raw: str = ""
    last_tokens: list[str] = field(default_factory=list)
    first_tokens: list[str] = field(default_factory=list)
    surname_full: str = ""
    surname_variants: list[str] = field(default_factory=list)
    first_key: str = ""
    truncated: bool = False
    truncation_unreliable_tail: bool = False
    is_joint: bool = False
    spouse_chunk: str = ""

    @property
    def match_keys(self) -> list[tuple[str, str]]:
        """(surname_variant, first_token) keys for deterministic tiers."""
        keys: list[tuple[str, str]] = []
        seen: set[tuple[str, str]] = set()
        for sv in self.surname_variants:
            pair = (sv, self.first_key)
            if pair not in seen and sv and self.first_key:
                seen.add(pair)
                keys.append(pair)
        return keys


def split_drake_client_name(raw: str) -> tuple[str, str, str]:
    """
    Parse Drake ``LAST, FIRST [& SPOUSE]``.

    Returns (last, primary_first, spouse_chunk).
    Missing spaces around ``&`` are tolerated.
    `` Y `` (single letter) before ``&`` is a middle initial, not a conjunction.
    """
    text = (raw or "").strip()
    if not text:
        return "", "", ""

    # Normalize glued ampersands: MIGUEL& LOURDES → MIGUEL & LOURDES
    text = re.sub(r"\s*&\s*", " & ", text)
    text = _WS.sub(" ", text).strip()

    if "," in text:
        last, rest = text.split(",", 1)
        last, rest = last.strip(), rest.strip()
    else:
        last, rest = text, ""

    spouse = ""
    primary = rest
    if " & " in rest:
        # Do not treat a lone letter token immediately before & as conjunction —
        # split only on &. The letter stays in primary first (stripped later as MI).
        primary, spouse = rest.split(" & ", 1)
        primary, spouse = primary.strip(), spouse.strip()

    return last, primary, spouse


def split_log_names(last_raw: str, first_raw: str) -> tuple[str, str, str]:
    """Tax Log LAST/FIRST; FIRST may be ``TAXPAYER & SPOUSE``."""
    last = (last_raw or "").strip()
    first = (first_raw or "").strip()
    first = re.sub(r"\s*&\s*", " & ", first)
    first = _WS.sub(" ", first).strip()
    spouse = ""
    primary = first
    if " & " in first:
        primary, spouse = first.split(" & ", 1)
        primary, spouse = primary.strip(), spouse.strip()
    # Shared / dual surnames in LAST: keep raw; matching uses variants.
    return last, primary, spouse


def detect_truncation(raw: str, threshold: int = 39) -> tuple[bool, bool]:
    """Flag names at/near Drake's 40-char cap; mark final token unreliable."""
    s = (raw or "").rstrip()
    truncated = len(s) >= threshold
    unreliable = truncated
    return truncated, unreliable


def normalize_person(
    last_raw: str,
    first_raw: str,
    *,
    display_raw: Optional[str] = None,
) -> NormalizedName:
    last_toks = clean_tokens(last_raw)
    first_toks = clean_tokens(first_raw)
    # Trailing single letter on surname → initial (AGUILAR FLORES G)
    last_toks = strip_trailing_initials(last_toks)
    variants = surname_variants(last_toks)
    fk = first_token(first_toks)
    raw = display_raw or f"{last_raw}, {first_raw}".strip(", ")
    trunc, unreliable = detect_truncation(raw if display_raw else (last_raw or ""))
    # Drake cap applies to full Client Name; callers should pass display_raw.
    if display_raw:
        trunc, unreliable = detect_truncation(display_raw)
    return NormalizedName(
        raw=raw,
        last_raw=last_raw or "",
        first_raw=first_raw or "",
        last_tokens=last_toks,
        first_tokens=first_toks,
        surname_full=variants[0] if variants else "",
        surname_variants=variants,
        first_key=fk,
        truncated=trunc,
        truncation_unreliable_tail=unreliable,
    )


def normalize_drake_client_name(client_name: str) -> NormalizedName:
    last, first, spouse = split_drake_client_name(client_name)
    n = normalize_person(last, first, display_raw=client_name)
    n.is_joint = bool(spouse)
    n.spouse_chunk = spouse
    return n


def normalize_log_row(last_raw: str, first_raw: str) -> NormalizedName:
    last, first, spouse = split_log_names(last_raw, first_raw)
    n = normalize_person(last, first)
    n.is_joint = bool(spouse) or ("&" in (last_raw or ""))
    n.spouse_chunk = spouse
    return n


def transposed_interpretation(last_raw: str, first_raw: str) -> NormalizedName:
    """
    Column-swap tolerance: treat FIRST as surname material and LAST as given name.

    Used when log has surname sitting in FIRST (e.g. AGUIRRE | JIMENEZ).
    """
    return normalize_person(first_raw, last_raw)


def keys_with_transposition(last_raw: str, first_raw: str) -> list[tuple[str, str]]:
    """Match keys for normal + transposed interpretations."""
    keys = list(normalize_log_row(last_raw, first_raw).match_keys)
    for k in transposed_interpretation(last_raw, first_raw).match_keys:
        if k not in keys:
            keys.append(k)
    return keys


def full_normalized_string(n: NormalizedName) -> str:
    parts = []
    if n.surname_full:
        parts.append(n.surname_full)
    # first tokens without trailing initials for full compare
    ft = strip_trailing_initials(strip_suffixes(n.first_tokens))
    if ft:
        parts.append(" ".join(ft))
    return " ".join(parts)


# ── Entity normalizer (separate from person — & is never a spouse split) ─────

# Trailing legal-form suffixes (longest first). Mid-name CO is not stripped.
_ENTITY_SUFFIXES: tuple[str, ...] = (
    "INCORPORATED",
    "CORPORATION",
    "L L C",  # after punct→space: L.L.C.
    "PLLC",
    "LLC",
    "LLP",
    "INC",
    "CORP",
    "LTD",
    "LP",
    "PC",
    "CO",
)

_DBA_SPLIT = re.compile(r"\bD\s*/\s*B\s*/\s*A\b|\bDBA\b", re.IGNORECASE)


@dataclass
class NormalizedEntity:
    raw: str
    primary: str
    keys: list[str] = field(default_factory=list)
    keys_with_suffix: list[str] = field(default_factory=list)
    dba_names: list[str] = field(default_factory=list)
    stripped_suffix: str = ""
    notes: list[str] = field(default_factory=list)


def _entity_base_tokens(raw: str) -> list[str]:
    """Fold accents; keep digits; turn hyphens/punct into spaces; keep & as token."""
    s = fold_accents(raw or "").upper()
    s = s.replace("&", " & ")
    s = s.replace("-", " ")
    s = re.sub(r"[^\w\s&]", " ", s)
    s = _WS.sub(" ", s).strip()
    return [t for t in s.split(" ") if t]


def _strip_leading_the(tokens: list[str]) -> tuple[list[str], bool]:
    if tokens and tokens[0] == "THE":
        return tokens[1:], True
    return tokens, False


def _strip_trailing_legal_suffix(tokens: list[str]) -> tuple[list[str], str]:
    if not tokens:
        return tokens, ""
    for suf in _ENTITY_SUFFIXES:
        suf_toks = suf.split()
        n = len(suf_toks)
        if len(tokens) > n and tokens[-n:] == suf_toks:
            return tokens[:-n], suf
    return tokens, ""


def _zero_strip_variant(tokens: list[str]) -> Optional[str]:
    changed = False
    out = []
    for t in tokens:
        if t.isdigit() and len(t) > 1 and t.startswith("0"):
            out.append(t.lstrip("0") or "0")
            changed = True
        else:
            out.append(t)
    if not changed:
        return None
    return " ".join(out)


def _split_dba(raw: str) -> tuple[str, list[str]]:
    parts = _DBA_SPLIT.split(raw or "")
    parts = [p.strip() for p in parts if p and p.strip()]
    if len(parts) <= 1:
        return (raw or "").strip(), []
    return parts[0], parts[1:]


def normalize_entity(raw: str) -> NormalizedEntity:
    """
    Normalize a business / entity name.

    Deliberately separate from person normalization: ``&`` is part of the
    entity name (e.g. ``J & J FENCE``), never a spouse separator.
    """
    notes: list[str] = []
    primary_raw, dba_raws = _split_dba(raw or "")
    if dba_raws:
        notes.append("dba_split")

    def _one(name: str) -> tuple[str, str, list[str], list[str]]:
        toks_all = _entity_base_tokens(name)
        toks_all, had_the = _strip_leading_the(toks_all)
        if had_the:
            notes.append("stripped_the")
        with_suf = " ".join(toks_all)
        toks, suf = _strip_trailing_legal_suffix(toks_all)
        core = " ".join(toks)
        keys: list[str] = []
        keys_suf: list[str] = []
        if core:
            keys.append(core)
        if with_suf:
            keys_suf.append(with_suf)
        zs = _zero_strip_variant(toks)
        if zs and zs not in keys:
            keys.append(zs)
            notes.append("zero_strip_variant")
        return core, suf, keys, keys_suf

    core, suf, keys, keys_suf = _one(primary_raw)
    dba_names: list[str] = []
    for d in dba_raws:
        d_core, _, d_keys, d_suf = _one(d)
        if d_core:
            dba_names.append(d_core)
        for k in d_keys:
            if k not in keys:
                keys.append(k)
        for k in d_suf:
            if k not in keys_suf:
                keys_suf.append(k)

    def _dedupe(seq: list[str]) -> list[str]:
        seen: set[str] = set()
        uniq: list[str] = []
        for k in seq:
            if k and k not in seen:
                seen.add(k)
                uniq.append(k)
        return uniq

    return NormalizedEntity(
        raw=raw or "",
        primary=core,
        keys=_dedupe(keys),
        keys_with_suffix=_dedupe(keys_suf),
        dba_names=dba_names,
        stripped_suffix=suf,
        notes=notes,
    )


def entity_match_keys(raw: str) -> list[str]:
    return normalize_entity(raw).keys
