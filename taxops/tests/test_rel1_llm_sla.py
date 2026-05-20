"""REL-1: LLM SLA — /ai/chat timeout is configured and documented."""
from __future__ import annotations

import pytest


def test_ollama_chat_answer_timeout_configured():
    """OLLAMA_CHAT_ANSWER_TIMEOUT_SEC is a positive integer."""
    from config import OLLAMA_CHAT_ANSWER_TIMEOUT_SEC

    assert isinstance(OLLAMA_CHAT_ANSWER_TIMEOUT_SEC, int)
    assert OLLAMA_CHAT_ANSWER_TIMEOUT_SEC > 0


def test_ai_chat_route_has_sla_docstring():
    """REL-1: ai_chat docstring documents the SLA (no single worker held beyond timeout)."""
    from ai_routes import ai_chat

    doc = (ai_chat.__doc__ or "").lower()
    assert "sla" in doc or "timeout" in doc or "ollama_chat_answer_timeout" in doc.lower()


def test_extraction_routes_do_not_call_ollama_synchronously(app):
    """Document LLM calls via extraction_queue, not inline in upload routes.

    DEBT-1: route moved to routes/documents.py Blueprint.
    """
    import inspect
    from routes.documents import return_documents_upload

    src = inspect.getsource(return_documents_upload)
    assert "requests.post" not in src, (
        "return_document_upload must not make synchronous Ollama calls; "
        "extraction goes through extraction_queue."
    )


def test_health_exposes_audit_queue_depth_type(client):
    """Health endpoint returns audit_queue_depth as an integer."""
    resp = client.get("/health")
    assert resp.status_code in (200, 503)
    data = resp.get_json()
    assert "audit_queue_depth" in data
    assert isinstance(data["audit_queue_depth"], int)
