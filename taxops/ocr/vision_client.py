"""
Shared Anthropic vision-call plumbing for TaxOps scan-agent OCR.

Ported from punchbridge/pipeline/vision_client.py — same shape, own copy,
so TaxOps does not take a cross-repo runtime dependency on punchbridge.
"""
from __future__ import annotations

import json
import logging
from base64 import standard_b64encode
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

_client = None


def get_client():
    """Lazy Anthropic client singleton (max_retries=6, matching PunchBridge)."""
    global _client
    if _client is None:
        from anthropic import Anthropic

        from config import ANTHROPIC_API_KEY

        if not ANTHROPIC_API_KEY:
            raise RuntimeError(
                "ANTHROPIC_API_KEY is not set — required for scan-agent Claude OCR"
            )
        # SDK retries 429/5xx (incl. 529 Overloaded) with exponential backoff.
        _client = Anthropic(api_key=ANTHROPIC_API_KEY, max_retries=6)
    return _client


def strip_json_fences(text: str) -> str:
    text = (text or "").strip()
    if text.startswith("```"):
        text = text.strip("`")
        if text[:4].lower() == "json":
            text = text[4:]
    return text.strip()


def call_vision(
    image_bytes: bytes,
    model: str,
    system: str,
    user_text: str = "Extract the tax form fields as JSON.",
    *,
    media_type: str = "image/png",
    max_tokens: int = 4096,
) -> dict:
    """Raw (uncached) Claude vision call → parsed JSON dict.

    Isolated so tests can monkeypatch a single choke point.
    """
    b64 = standard_b64encode(image_bytes).decode()
    msg = get_client().messages.create(
        model=model,
        max_tokens=max_tokens,
        system=system,
        messages=[
            {
                "role": "user",
                "content": [
                    {
                        "type": "image",
                        "source": {
                            "type": "base64",
                            "media_type": media_type,
                            "data": b64,
                        },
                    },
                    {"type": "text", "text": user_text},
                ],
            }
        ],
    )
    text = "".join(block.text for block in msg.content if block.type == "text")
    return json.loads(strip_json_fences(text))


def call_vision_path(
    image_path: Path,
    model: str,
    system: str,
    user_text: str = "Extract the tax form fields as JSON.",
) -> dict:
    data = Path(image_path).read_bytes()
    suffix = Path(image_path).suffix.lower()
    media = "image/jpeg" if suffix in (".jpg", ".jpeg") else "image/png"
    return call_vision(data, model, system, user_text, media_type=media)
