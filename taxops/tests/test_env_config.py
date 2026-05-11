"""Env loading edge cases (NSSM / .env typos)."""

from __future__ import annotations

from config import (
    _migrate_leading_colon_process_env_inplace,
    _normalize_env_key,
)


def test_normalize_env_key_strips_nssm_colon_typo():
    assert _normalize_env_key(":OLLAMA_BASE_URL") == "OLLAMA_BASE_URL"
    assert _normalize_env_key("OLLAMA_BASE_URL") == "OLLAMA_BASE_URL"
    assert _normalize_env_key("\ufeffFOO") == "FOO"


def test_migrate_leading_colon_copies_when_canonical_missing():
    e: dict[str, str] = {":OLLAMA_BASE_URL": "http://x", ":IMAP_HOST": ""}
    _migrate_leading_colon_process_env_inplace(e)
    assert e["OLLAMA_BASE_URL"] == "http://x"
    assert e["IMAP_HOST"] == ""


def test_migrate_leading_colon_does_not_overwrite_existing():
    e: dict[str, str] = {"OLLAMA_BASE_URL": "ok", ":OLLAMA_BASE_URL": "bad"}
    _migrate_leading_colon_process_env_inplace(e)
    assert e["OLLAMA_BASE_URL"] == "ok"
