"""Unit checks for confirmed-spouse audit classifier (no DB)."""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from audit_confirmed_spouses import _classify  # noqa: E402


def test_wrong_household_canizales_rosa_is_mismatch():
    _pl, _pf, _sc, _score, flag = _classify(
        "CANIZALES",
        "ROSA",
        "SANDRA",
        "CANIZALES",
        "ARGELIS ORTIZ & SANDRA CANIZALES",
    )
    assert flag == "MISMATCH"


def test_correct_ortiz_argelis_is_match():
    _pl, _pf, _sc, _score, flag = _classify(
        "ORTIZ",
        "ARGELIS",
        "SANDRA",
        "CANIZALES",
        "ARGELIS ORTIZ & SANDRA CANIZALES",
    )
    assert flag == "MATCH"


def test_wrong_household_cardona_hector_is_mismatch():
    _pl, _pf, _sc, _score, flag = _classify(
        "CARDONA",
        "HECTOR",
        "MARIA",
        "CARDONA",
        "PEDRO & MARIA CARDONA",
    )
    assert flag == "MISMATCH"


def test_correct_cardona_pedro_is_match():
    _pl, _pf, _sc, _score, flag = _classify(
        "CARDONA",
        "PEDRO",
        "MARIA",
        "CARDONA",
        "PEDRO & MARIA CARDONA",
    )
    assert flag == "MATCH"
