"""DEBT-3: name_matcher.py — parameterized unit tests for fuzzy-match thresholds and edge cases."""
from __future__ import annotations

import pytest
from name_matcher import (
    ACCEPT_THRESHOLD,
    REVIEW_THRESHOLD,
    _clean,
    normalize_name,
    parse_name,
    score_client_names_pair,
    strip_middle_initial,
    strip_spouse,
    is_business,
    split_joint_first_column,
)


# ── Threshold contract ────────────────────────────────────────────────────────

def test_accept_threshold_above_review():
    assert ACCEPT_THRESHOLD > REVIEW_THRESHOLD


def test_thresholds_in_reasonable_range():
    assert 0 < REVIEW_THRESHOLD < ACCEPT_THRESHOLD <= 100


# ── _clean() ─────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("garcia",                  "GARCIA"),
    # _clean uppercases (no unicode normalisation) and replaces hyphens with spaces.
    ("Pérez-Quintana",          "PÉREZ QUINTANA"),
    ("O'BRIEN",                 "O BRIEN"),
    # commas become spaces; consecutive spaces are collapsed by re.sub(\s+)
    ("Smith,  Jr.",             "SMITH JR"),
    ("  whitespace  ",          "WHITESPACE"),
])
def test_clean(raw, expected):
    result = _clean(raw)
    # strip trailing space before comparing (punctuation → space at end is fine)
    assert result.strip() == expected.strip()


# ── normalize_name() ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, expected", [
    ("SMITH JR",        "SMITH"),
    ("CORNWELL IV",     "CORNWELL"),
    ("GARCIA",          "GARCIA"),
    ("PEREZ-QUINTANA",  "PEREZ QUINTANA"),
    ("O'Brien Sr.",     "O BRIEN"),
])
def test_normalize_name(raw, expected):
    assert normalize_name(raw) == expected


# ── strip_middle_initial() ────────────────────────────────────────────────────

@pytest.mark.parametrize("first, expected", [
    ("AMJAD G",     "AMJAD"),
    ("JOHN A",      "JOHN"),
    ("MARY ANN",    "MARY ANN"),    # two-token, second not single char
    ("JOSE",        "JOSE"),        # single token unchanged
    ("LUIS ANGEL",  "LUIS ANGEL"),  # two-token, 5-char token unchanged
])
def test_strip_middle_initial(first, expected):
    assert strip_middle_initial(first) == expected


# ── parse_name() ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("raw, exp_last, exp_first", [
    ("GARCIA, MARIA",                   "GARCIA",           "MARIA"),
    ("BOCANEGRA GALLEGOS, URIEL",       "BOCANEGRA GALLEGOS","URIEL"),
    ("CORNWELL IV, JOHN",               "CORNWELL",         "JOHN"),
    ("GARCIA, JUAN & MARIA",            "GARCIA",           "JUAN"),   # strips spouse
    ("",                                "",                 None),
    ("SINGLE",                          "SINGLE",           None),
])
def test_parse_name(raw, exp_last, exp_first):
    last, first = parse_name(raw)
    assert last == exp_last
    assert first == exp_first


# ── is_business() ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("last, first, expected", [
    ("ACME LLC",    None,   True),
    ("SMITH CORP",  None,   True),
    ("GARCIA",      "JUAN", False),
    ("TAX SERVICES INC", None, True),
    ("PARTNERS GROUP", None, True),
])
def test_is_business(last, first, expected):
    assert is_business(last, first) == expected


# ── strip_spouse() ────────────────────────────────────────────────────────────

@pytest.mark.parametrize("first, expected", [
    ("GANEM B & MONA N",  "GANEM B"),
    ("JOHN",              "JOHN"),
    ("MARIA & JOSE",      "MARIA"),
])
def test_strip_spouse(first, expected):
    assert strip_spouse(first) == expected


# ── split_joint_first_column() ───────────────────────────────────────────────

@pytest.mark.parametrize("raw, exp_primary, exp_spouse", [
    ("JOHN & JANE",     "JOHN", "JANE"),
    ("JUAN",            "JUAN", None),
    ("",                "",     None),
    ("A & B & C",       "A",    "B C"),   # all non-primary parts joined
])
def test_split_joint_first_column(raw, exp_primary, exp_spouse):
    primary, spouse = split_joint_first_column(raw)
    assert primary == exp_primary
    assert spouse == exp_spouse


# ── score_client_names_pair() — threshold boundary tests ─────────────────────

@pytest.mark.parametrize("la, fa, lb, fb, min_score, note", [
    # Exact match → near-perfect
    ("GARCIA",  "JUAN",     "GARCIA",   "JUAN",     95, "exact match"),
    # Middle initial difference → still high
    ("GARCIA",  "JUAN A",   "GARCIA",   "JUAN",     90, "middle initial stripped"),
    # Last-name-only fuzzy match
    ("GARCIA",  None,       "GARCIA",   None,       85, "last-only match"),
    # Clearly different → below REVIEW_THRESHOLD (not necessarily 0)
    ("SMITH",   "JOHN",     "GARCIA",   "MARIA",    -1,  "different names"),
    # Hyphenated vs space variant
    ("PEREZ QUINTANA", "JOSE", "PEREZ-QUINTANA", "JOSE", 88, "hyphen vs space"),
    # Accented variant (normalised before scoring)
    ("PEREZ",   "ROSA",     "PÉREZ",    "ROSA",     88, "accented name"),
    # Empty last → 0
    ("",        "JUAN",     "",         "JUAN",     0,  "empty last → zero"),
])
def test_score_pair_boundaries(la, fa, lb, fb, min_score, note):
    score = score_client_names_pair(la, fa, lb, fb)
    if min_score < 0:
        # negative sentinel = "below REVIEW_THRESHOLD"
        assert score < REVIEW_THRESHOLD, f"{note}: expected < {REVIEW_THRESHOLD}, got {score}"
    elif min_score == 0:
        assert score == 0, f"{note}: expected 0, got {score}"
    else:
        assert score >= min_score, f"{note}: expected >= {min_score}, got {score}"


def test_score_pair_above_accept_threshold_for_exact():
    score = score_client_names_pair("GARCIA", "JUAN", "GARCIA", "JUAN")
    assert score >= ACCEPT_THRESHOLD


def test_score_pair_below_review_threshold_for_different():
    score = score_client_names_pair("SMITH", "ALICE", "JONES", "BOB")
    assert score < REVIEW_THRESHOLD


def test_score_pair_suffix_stripped_before_compare():
    """JR / SR in last names should not artificially lower the score."""
    score = score_client_names_pair("SMITH JR", "JOHN", "SMITH", "JOHN")
    assert score >= ACCEPT_THRESHOLD
