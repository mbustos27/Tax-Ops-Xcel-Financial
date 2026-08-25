"""Claude OCR path for scan-agent docs: cache hit + Haiku→Sonnet→Opus escalate."""
from __future__ import annotations

import pytest


@pytest.fixture
def png_bytes():
    # Minimal valid 1x1 PNG
    return (
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01"
        b"\x08\x02\x00\x00\x00\x90wS\xde\x00\x00\x00\x0cIDATx\x9cc\xf8\x0f"
        b"\x00\x00\x01\x01\x00\x05\x18\xd8N\x00\x00\x00\x00IEND\xaeB`\x82"
    )


def test_schema_v23_ocr_cache(taxops_db_path):
    from db import CURRENT_SCHEMA_VERSION, get_connection, get_schema_version

    assert CURRENT_SCHEMA_VERSION == 23
    conn = get_connection(taxops_db_path)
    assert get_schema_version(conn) == 23
    row = conn.execute(
        "SELECT name FROM sqlite_master WHERE name='ocr_extraction_cache'"
    ).fetchone()
    assert row is not None
    conn.close()


def test_claude_cache_hit_skips_api(monkeypatch, taxops_db_path, tmp_path, png_bytes):
    from db import get_connection
    import ocr.claude_extract as ce
    import ocr.vision_client as vc

    calls: list[str] = []

    def fake_call(
        image_bytes, model, system, user_text="x", *, media_type="image/png", max_tokens=4096
    ):
        calls.append(model)
        return {
            "form_type": "W-2",
            "employer_name": "ACME",
            "box1_wages_tips_other": "50000",
            "_confidence": 0.95,
        }

    monkeypatch.setattr(vc, "call_vision", fake_call)

    img = tmp_path / "page.png"
    img.write_bytes(png_bytes)
    conn = get_connection(taxops_db_path)

    fields1, method1, meta1 = ce.extract_scan_document_claude(
        str(img), conn, filename="page.png"
    )
    conn.commit()
    assert fields1["employer_name"] == "ACME"
    assert method1 == "claude_haiku"
    assert meta1["api_calls"] == 1
    assert meta1["cache_hit"] is False

    fields2, method2, meta2 = ce.extract_scan_document_claude(
        str(img), conn, filename="page.png"
    )
    assert method2 == "claude_cache"
    assert meta2["cache_hit"] is True
    assert meta2["api_calls"] == 0
    assert fields2["employer_name"] == "ACME"
    assert len(calls) == 1
    conn.close()


def test_claude_escalates_haiku_to_sonnet(monkeypatch, taxops_db_path, tmp_path, png_bytes):
    from config import CLAUDE_OCR_MODEL_FAST, CLAUDE_OCR_MODEL_READ
    from db import get_connection
    import ocr.claude_extract as ce
    import ocr.vision_client as vc

    calls: list[str] = []

    def fake_call(
        image_bytes, model, system, user_text="x", *, media_type="image/png", max_tokens=4096
    ):
        calls.append(model)
        if model == CLAUDE_OCR_MODEL_FAST:
            return {
                "form_type": "W-2",
                "employer_name": "BLURRY CO",
                "_confidence": 0.50,
            }
        return {
            "form_type": "W-2",
            "employer_name": "CLEAR CO",
            "box1_wages_tips_other": "60000",
            "_confidence": 0.92,
        }

    monkeypatch.setattr(vc, "call_vision", fake_call)
    img = tmp_path / "blur.png"
    img.write_bytes(png_bytes)
    conn = get_connection(taxops_db_path)
    fields, method, meta = ce.extract_scan_document_claude(
        str(img), conn, filename="blur.png"
    )
    assert CLAUDE_OCR_MODEL_FAST in calls
    assert CLAUDE_OCR_MODEL_READ in calls
    assert method == "claude_sonnet"
    assert fields["employer_name"] == "CLEAR CO"
    assert meta["api_calls"] == 2
    conn.close()


def test_claude_escalates_to_opus_below_hard_threshold(
    monkeypatch, taxops_db_path, tmp_path, png_bytes
):
    from config import (
        CLAUDE_OCR_MODEL_FAST,
        CLAUDE_OCR_MODEL_HARD,
        CLAUDE_OCR_MODEL_READ,
    )
    from db import get_connection
    import ocr.claude_extract as ce
    import ocr.vision_client as vc

    calls: list[str] = []

    def fake_call(
        image_bytes, model, system, user_text="x", *, media_type="image/png", max_tokens=4096
    ):
        calls.append(model)
        if model == CLAUDE_OCR_MODEL_HARD:
            return {
                "form_type": "W-2",
                "employer_name": "OPUS CO",
                "_confidence": 0.88,
            }
        return {
            "form_type": "W-2",
            "employer_name": "WEAK",
            "_confidence": 0.40,
        }

    monkeypatch.setattr(vc, "call_vision", fake_call)
    img = tmp_path / "hard.png"
    img.write_bytes(png_bytes)
    conn = get_connection(taxops_db_path)
    fields, method, meta = ce.extract_scan_document_claude(
        str(img), conn, filename="hard.png"
    )
    assert calls == [
        CLAUDE_OCR_MODEL_FAST,
        CLAUDE_OCR_MODEL_READ,
        CLAUDE_OCR_MODEL_HARD,
    ]
    assert method == "claude_opus"
    assert fields["employer_name"] == "OPUS CO"
    assert meta["api_calls"] == 3
    conn.close()


def test_extract_fields_scan_agent_uses_claude(
    monkeypatch, taxops_db_path, tmp_path, png_bytes
):
    """_extract_fields(source=scan_agent) routes images through Claude, not Ollama."""
    import extractor
    from db import get_connection

    called = {"claude": False, "ollama": False}

    def fake_claude(file_path, filename, conn):
        called["claude"] = True
        return {"form_type": "W-2", "employer_name": "X"}, "claude_haiku"

    def boom_ollama(*a, **k):
        called["ollama"] = True
        raise AssertionError("Ollama should not run for scan_agent images")

    monkeypatch.setattr(extractor, "_extract_via_claude", fake_claude)
    monkeypatch.setattr(extractor, "_extract_json_retry_on_timeout", boom_ollama)

    img = tmp_path / "w2.png"
    img.write_bytes(png_bytes)
    conn = get_connection(taxops_db_path)
    fields, method = extractor._extract_fields(
        str(img), "w2.png", source="scan_agent", conn=conn
    )
    assert called["claude"] is True
    assert called["ollama"] is False
    assert method == "claude_haiku"
    assert fields["employer_name"] == "X"
    conn.close()
