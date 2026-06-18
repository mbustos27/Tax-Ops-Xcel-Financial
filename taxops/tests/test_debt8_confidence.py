"""DEBT-8: _compute_confidence — normalized scoring rationale, paystub + W-2 + 1099."""
from __future__ import annotations

import pytest


def test_module_docstring_documents_scoring():
    """extractor module docstring contains DEBT-8 scoring rationale."""
    import extractor
    doc = extractor.__doc__ or ""
    assert "DEBT-8" in doc
    assert "_compute_confidence" in doc


def test_empty_fields_returns_zero():
    from extractor import _compute_confidence
    assert _compute_confidence({}, "w2_records") == 0.0


def test_no_detected_type_returns_zero():
    from extractor import _compute_confidence
    assert _compute_confidence({"employer_name": "ACME"}, None) == 0.0


def test_unknown_type_returns_half():
    """Unknown detected_type (not in required_fields) returns 0.5 neutral."""
    from extractor import _compute_confidence
    score = _compute_confidence({"some_field": "val"}, "unknown_type")
    assert score == 0.5


# ── W-2 scoring ─────────────────────────────────────────────────────────────

def test_w2_all_required_fields_returns_high():
    from extractor import _compute_confidence, HIGH_CONFIDENCE
    fields = {
        "employer_name": "ACME CORP",
        "tax_year": "2025",
        "box1_wages_tips_other": "50000",
        "box2_federal_income_tax_withheld": "7000",
        "form_type": "W-2",
    }
    score = _compute_confidence(fields, "w2_records")
    assert score >= HIGH_CONFIDENCE


def test_w2_partial_fields_below_threshold():
    from extractor import _compute_confidence, HIGH_CONFIDENCE
    fields = {"employer_name": "ACME CORP"}
    score = _compute_confidence(fields, "w2_records")
    assert score < HIGH_CONFIDENCE


def test_w2_no_fields_returns_zero():
    from extractor import _compute_confidence
    assert _compute_confidence({}, "w2_records") == 0.0


# ── Paystub scoring ──────────────────────────────────────────────────────────

def test_paystub_with_ytd_and_employer_is_substantial():
    from extractor import _compute_confidence
    fields = {
        "employer_name": "ACME",
        "ytd_gross": "15000",
        "ytd_net": "11000",
    }
    score = _compute_confidence(fields, "paystub")
    assert score >= 0.55


def test_paystub_with_overtime_and_ytd_is_high():
    from extractor import _compute_confidence, HIGH_CONFIDENCE
    fields = {
        "employee_name": "JOHN DOE",
        "ytd_gross": "15000",
        "has_overtime": True,
        "overtime_hours": "8.0",
        "form_type": "PAYSTUB",
    }
    score = _compute_confidence(fields, "paystub")
    assert score >= HIGH_CONFIDENCE


def test_paystub_empty_returns_zero():
    from extractor import _compute_confidence
    assert _compute_confidence({}, "paystub") == 0.0


def test_paystub_score_capped_at_one():
    """Paystub confidence never exceeds 1.0 even with all signals."""
    from extractor import _compute_confidence
    fields = {
        "employer_name": "ACME",
        "employee_name": "JOHN",
        "ytd_gross": "50000",
        "ytd_net": "35000",
        "has_overtime": True,
        "overtime_hours": "10",
        "overtime_pay": "500",
        "form_type": "PAYSTUB",
    }
    score = _compute_confidence(fields, "paystub")
    assert 0.0 <= score <= 1.0


# ── 1099 scoring ─────────────────────────────────────────────────────────────

def test_1099_nec_full_fields_is_high():
    from extractor import _compute_confidence, HIGH_CONFIDENCE
    fields = {
        "payer_name": "CLIENT LLC",
        "tax_year": "2025",
        "box1_nonemployee_compensation": "12000",
        "form_type": "1099-NEC",
    }
    score = _compute_confidence(fields, "f1099_nec_records")
    assert score >= HIGH_CONFIDENCE


def test_high_confidence_constant_is_sane():
    """HIGH_CONFIDENCE is between 0.5 and 1.0."""
    from extractor import HIGH_CONFIDENCE
    assert 0.5 < HIGH_CONFIDENCE < 1.0


# ── batch_fetch_returns ────────────────────────────────────────────────────

def test_batch_fetch_returns_empty_list(taxops_db_path, monkeypatch):
    """batch_fetch_returns([]) returns an empty dict without DB round-trip."""
    import config as cfg
    import db as db_mod
    monkeypatch.setattr(cfg, "DB_PATH", taxops_db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", taxops_db_path)
    from app import batch_fetch_returns
    result = batch_fetch_returns([])
    assert result == {}


def test_batch_fetch_returns_missing_id(taxops_db_path, monkeypatch):
    """batch_fetch_returns with a non-existent id returns empty dict."""
    import config as cfg
    import db as db_mod
    monkeypatch.setattr(cfg, "DB_PATH", taxops_db_path)
    monkeypatch.setattr(db_mod, "DB_PATH", taxops_db_path)
    import app as app_mod
    monkeypatch.setattr(app_mod, "DB_PATH", taxops_db_path)
    from app import batch_fetch_returns
    result = batch_fetch_returns([999999])
    assert result == {}
