"""Manual log matching and joint-name parsing helpers."""

from name_matcher import (
    ACCEPT_THRESHOLD,
    score_client_names_pair,
    split_joint_first_column,
    spouse_parts_from_display_line,
)


def test_score_exact_match() -> None:
    s = score_client_names_pair(
        "SMITH",
        "JOHN",
        "SMITH",
        "JOHN",
    )
    assert s >= ACCEPT_THRESHOLD


def test_joint_split_first_column() -> None:
    primary, spouse = split_joint_first_column("JOHN & JANE")
    assert primary == "JOHN"
    assert spouse == "JANE"


def test_spouse_from_display() -> None:
    parts = spouse_parts_from_display_line("SMITH, JOHN & JANE DOE")
    assert parts is not None
    assert parts[0] == "JANE"
    assert parts[1] == "DOE"


def test_spouse_from_display_requires_comma_ampersand() -> None:
    assert spouse_parts_from_display_line("") is None
    assert spouse_parts_from_display_line("SMITH JOHN AND JANE") is None
