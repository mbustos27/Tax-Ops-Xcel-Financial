from __future__ import annotations

import json
import re

import requests

from config import OLLAMA_BASE_URL


def chat(prompt: str, model: str = "taxops", image_b64: str = None) -> str:
    """POST a prompt to Ollama and return the response text.

    Raises on any network or HTTP error — callers must handle exceptions.
    Prompt content is never logged (SSN safety rule).
    """
    payload: dict = {"model": model, "prompt": prompt, "stream": False}
    if image_b64 is not None:
        payload["images"] = [image_b64]

    response = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json=payload,
        timeout=60,
    )
    response.raise_for_status()
    return response.json()["response"]


def extract_json(prompt: str, model: str = "taxops", image_b64: str = None) -> dict:
    """Call chat() and parse the result as JSON.

    Strips markdown fences before parsing.
    Raises json.JSONDecodeError if the response is not valid JSON.
    """
    raw = chat(prompt, model=model, image_b64=image_b64)
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    return json.loads(cleaned)
