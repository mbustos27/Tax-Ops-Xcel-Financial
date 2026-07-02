"""Phase 2.3 (email system revamp): needs_manual_tagging() predicate.

Mirrors extractor.py's _extract_fields(), which returns (None, "image_skipped")
for raster image attachments whenever EXTRACTOR_VISION_ENABLED is false. This
predicate lets the email inbox view and the return documents list render a
"needs manual tag" signal without importing extractor.py.
"""
from __future__ import annotations

import config as cfg
from utils import needs_manual_tagging


def test_image_needs_manual_tagging_when_vision_disabled(monkeypatch):
    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", False)
    assert needs_manual_tagging("receipt.jpg") is True
    assert needs_manual_tagging("receipt.JPEG") is True
    assert needs_manual_tagging("scan.png") is True


def test_non_image_never_needs_manual_tagging(monkeypatch):
    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", False)
    assert needs_manual_tagging("w2.pdf") is False
    assert needs_manual_tagging("statement.docx") is False
    assert needs_manual_tagging("") is False
    assert needs_manual_tagging(None) is False


def test_image_never_needs_manual_tagging_when_vision_enabled(monkeypatch):
    monkeypatch.setattr(cfg, "EXTRACTOR_VISION_ENABLED", True)
    assert needs_manual_tagging("receipt.jpg") is False
    assert needs_manual_tagging("scan.png") is False
