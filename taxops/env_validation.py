"""PROD-5 — validate environment/secrets before serving (GitHub #93).

Loads after ``config`` (``.env`` applied). Strict profile when ``TAXOPS_ENV`` is not relaxed
(`demo`, `dev`, ``development``, ``local``, ``test``), ``FLASK_DEBUG`` is not ``1``, and
``TAXOPS_SKIP_ENV_VALIDATION`` is not ``1``.
"""

from __future__ import annotations

import logging
import os
import sys

import config as taxops_config

_RELAXED_APP_ENVS = frozenset({"demo", "dev", "development", "local", "test"})

_BUILTIN_LOGIN_PASS_FALLBACK = "2703Tax"
_WEAK_LOGIN_PASSWORD_NORMALIZED = frozenset(
    {
        "",
        "changeme",
        "password",
        "2703tax",
        "taxops",
        "info2703tax",
        "changeme!",
    },
)

_SECRET_MIN_LEN = 16


def _requires_strict_profile() -> bool:
    """Read ``APP_ENV`` from :mod:`config` each time so tests can monkeypatch."""
    if os.environ.get("TAXOPS_SKIP_ENV_VALIDATION", "0").strip() == "1":
        return False
    if taxops_config.APP_ENV in _RELAXED_APP_ENVS:
        return False
    if os.environ.get("FLASK_DEBUG", "0").strip() == "1":
        return False
    return True


def validate_taxops_environment() -> list[str]:
    """Return human-readable fatal configuration errors (empty list = OK)."""
    if not _requires_strict_profile():
        return []

    errs: list[str] = []

    secret_raw = os.environ.get("TAXOPS_SECRET")
    if secret_raw is None or not isinstance(secret_raw, str):
        errs.append(
            "TAXOPS_SECRET must be set in the process environment (.env beside taxops/, taxops/.env, or NSSM "
            "AppEnvironmentExtra). It signs login cookies — omitting it makes every restart invalidate sessions "
            "and weakens SSRF/session handling."
        )
    elif len(secret_raw.strip()) < _SECRET_MIN_LEN:
        errs.append(
            f"TAXOPS_SECRET must be at least {_SECRET_MIN_LEN} characters (after trim). Generate e.g. "
            "`python -c \"import secrets; print(secrets.token_urlsafe(32))\"` and paste once into NSSM or `.env`."
        )

    if "TAXOPS_USER" not in os.environ:
        errs.append(
            "TAXOPS_USER must be set explicitly (.env template default is insecure for production)."
            "Staff login username stays out of repo — commit only `.env.example` placeholders."
        )
    elif not os.environ["TAXOPS_USER"].strip():
        errs.append("TAXOPS_USER cannot be blank when running a strict profile.")

    if "TAXOPS_PASS" not in os.environ:
        errs.append(
            "TAXOPS_PASS must be set explicitly (.env/NSSM). The built-in dev fallback "
            f"'{_BUILTIN_LOGIN_PASS_FALLBACK}' is blocked in strict mode."
        )
    else:
        pw_raw = os.environ["TAXOPS_PASS"]
        if pw_raw is None or not pw_raw.strip():
            errs.append(
                "TAXOPS_PASS is empty — set a strong password (.env beside taxops/ or NSSM)."
            )
        else:
            if pw_raw == _BUILTIN_LOGIN_PASS_FALLBACK:
                errs.append(
                    "TAXOPS_PASS cannot equal the bundled dev fallback — pick a unique password "
                    "(rotate if this value was pasted from README/.env.template)."
                )
            norm = pw_raw.strip().casefold()
            if norm in _WEAK_LOGIN_PASSWORD_NORMALIZED:
                errs.append(
                    "TAXOPS_PASS matches a known-weak/demo value (`changeme`, `password`, …) — rotate before exposing the UI."
                )
            if len(pw_raw.strip()) < 10:
                errs.append(
                    "TAXOPS_PASS shorter than 10 characters — lengthen for production deployments."
                )

    host = getattr(taxops_config, "IMAP_HOST", "") or ""
    if host.strip():
        if not (os.getenv("IMAP_USER") or "").strip():
            errs.append(
                "IMAP_HOST is enabled but IMAP_USER is blank — mailbox login would fail;"
                " set IMAP_USER (and secrets) fully or blank IMAP_HOST to disable watcher."
            )
        if not (os.getenv("IMAP_PASS") or "").strip():
            errs.append(
                "IMAP_HOST is enabled but IMAP_PASS is blank — Gmail/app-password style secrets belong in NSSM /.env,"
                " not in git."
            )

    return errs


# SEC-7: Login lockout configuration.
# These are informational constants — the live values are read at runtime in app.py.
# Document here so staff know where to look.
_LOGIN_LOCKOUT_DEFAULTS = {
    "TAXOPS_LOGIN_MAX_ATTEMPTS":   "5   # consecutive failures before account locks",
    "TAXOPS_LOGIN_LOCKOUT_MINUTES": "15  # minutes the account stays locked",
}
"""Staff note: override via NSSM AppEnvironmentExtra or .env.
Example: TAXOPS_LOGIN_MAX_ATTEMPTS=3 to lock after 3 failures.
         TAXOPS_LOGIN_LOCKOUT_MINUTES=30 to extend the lockout window."""


def validate_taxops_environment_and_exit() -> None:
    """Log + stderr + ``SystemExit(1)`` on failure."""
    failed = validate_taxops_environment()
    if not failed:
        return

    sep = "\n  - "
    msg = (
        "TaxOps environment validation failed — refusing to boot (GitHub PROD-5)."
        "\nSecrets and auth knobs must ship via `.env`/NSSM env, not baked-in defaults.\nSee `.env.example`."
        + sep
        + sep.join(failed)
    )
    logging.critical(msg)
    print(msg, file=sys.stderr)
    raise SystemExit(1)
