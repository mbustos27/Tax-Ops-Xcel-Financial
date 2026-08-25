"""Quick-filter token matching (mirrors static/app.js AND-token rules)."""
from __future__ import annotations


def parse_quick_filter_tokens(query: str) -> list[str]:
    return [t for t in (query or "").strip().lower().split() if t]


def row_matches_quick_filter(search_lcase: str, tokens: list[str]) -> bool:
    if not tokens:
        return True
    hay = search_lcase or ""
    return all(t in hay for t in tokens)


def test_empty_query_matches_all():
    assert row_matches_quick_filter("9001 smith john", parse_quick_filter_tokens(""))
    assert row_matches_quick_filter("9001 smith john", parse_quick_filter_tokens("   "))


def test_single_token_substring():
    hay = "9001 smith john"
    assert row_matches_quick_filter(hay, parse_quick_filter_tokens("smi"))
    assert not row_matches_quick_filter(hay, parse_quick_filter_tokens("jones"))


def test_and_tokens_cross_field_order_independent():
    # data-search order is log last first — contiguous "john smith" fails includes()
    hay = "9001 smith john"
    assert row_matches_quick_filter(hay, parse_quick_filter_tokens("john smith"))
    assert row_matches_quick_filter(hay, parse_quick_filter_tokens("smith 9001"))
    assert not row_matches_quick_filter(hay, parse_quick_filter_tokens("john jones"))


def test_case_insensitive():
    hay = "9001 smith john"
    assert row_matches_quick_filter(hay, parse_quick_filter_tokens("JOHN SMITH"))
