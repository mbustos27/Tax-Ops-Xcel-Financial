"""PROD-5 environment validation (GitHub #93)."""

from __future__ import annotations

import config as taxops_config
import pytest

from env_validation import validate_taxops_environment, validate_taxops_environment_and_exit


def test_relaxed_profile_empty_errors(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(taxops_config, "APP_ENV", "test")
    assert validate_taxops_environment() == []


def test_strict_requires_secret_user_pass(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(taxops_config, "APP_ENV", "production")
    monkeypatch.setattr(taxops_config, "IMAP_HOST", "")
    monkeypatch.delenv("TAXOPS_SKIP_ENV_VALIDATION", raising=False)
    monkeypatch.delenv("FLASK_DEBUG", raising=False)

    monkeypatch.delenv("TAXOPS_SECRET", raising=False)
    monkeypatch.delenv("TAXOPS_USER", raising=False)
    monkeypatch.delenv("TAXOPS_PASS", raising=False)

    errs = validate_taxops_environment()
    assert any("TAXOPS_SECRET" in e for e in errs)
    assert any("TAXOPS_USER" in e for e in errs)
    assert any("TAXOPS_PASS" in e for e in errs)


def test_strict_imap_requires_mailbox_secrets(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(taxops_config, "APP_ENV", "production")
    monkeypatch.setattr(taxops_config, "IMAP_HOST", "imap.example.com")
    monkeypatch.delenv("TAXOPS_SKIP_ENV_VALIDATION", raising=False)
    monkeypatch.delenv("FLASK_DEBUG", raising=False)

    monkeypatch.setenv("TAXOPS_SECRET", "x" * 20)
    monkeypatch.setenv("TAXOPS_USER", "staff")
    monkeypatch.setenv("TAXOPS_PASS", "LongEnough123")
    monkeypatch.setenv("IMAP_USER", "")
    monkeypatch.setenv("IMAP_PASS", "")

    errs = validate_taxops_environment()
    assert any("IMAP_USER" in e for e in errs)
    assert any("IMAP_PASS" in e for e in errs)


def test_skip_validation_flag(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(taxops_config, "APP_ENV", "production")
    monkeypatch.setenv("TAXOPS_SKIP_ENV_VALIDATION", "1")
    monkeypatch.delenv("TAXOPS_SECRET", raising=False)

    assert validate_taxops_environment() == []


def test_validate_and_exit_raises(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(taxops_config, "APP_ENV", "staging")
    monkeypatch.setattr(taxops_config, "IMAP_HOST", "")
    monkeypatch.delenv("TAXOPS_SKIP_ENV_VALIDATION", raising=False)
    monkeypatch.delenv("FLASK_DEBUG", raising=False)
    monkeypatch.delenv("TAXOPS_SECRET", raising=False)
    monkeypatch.delenv("TAXOPS_USER", raising=False)
    monkeypatch.delenv("TAXOPS_PASS", raising=False)

    with pytest.raises(SystemExit) as ex:
        validate_taxops_environment_and_exit()

    assert ex.value.code == 1
