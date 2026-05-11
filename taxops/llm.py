from __future__ import annotations

import json
import re

import requests

from config import OLLAMA_BASE_URL, OLLAMA_MODEL


def chat(
    prompt: str,
    model: str | None = None,
    image_b64: str = None,
    timeout: int = 60,
) -> str:
    """POST a prompt to Ollama and return the response text.

    Raises on any network or HTTP error — callers must handle exceptions.
    Prompt content is never logged (SSN safety rule).
    """
    use_model = model if model is not None else OLLAMA_MODEL
    payload: dict = {"model": use_model, "prompt": prompt, "stream": False}
    if image_b64 is not None:
        payload["images"] = [image_b64]

    # Separate connect vs read — a dead/unreachable host must not hang forever on connect.
    connect_timeout = min(10.0, float(timeout))
    read_timeout = float(timeout)
    req_timeout = (connect_timeout, read_timeout)

    response = requests.post(
        f"{OLLAMA_BASE_URL}/api/generate",
        json=payload,
        timeout=req_timeout,
    )
    response.raise_for_status()
    return response.json()["response"]


def extract_json(
    prompt: str,
    model: str | None = None,
    image_b64: str = None,
    timeout: int = 60,
) -> dict:
    """Call chat() and parse the result as JSON.

    Strips markdown fences before parsing.
    Raises json.JSONDecodeError if the response is not valid JSON.
    """
    raw = chat(prompt, model=model, image_b64=image_b64, timeout=timeout)
    cleaned = re.sub(r"^```(?:json)?\s*", "", raw.strip())
    cleaned = re.sub(r"\s*```$", "", cleaned).strip()
    return json.loads(cleaned)
