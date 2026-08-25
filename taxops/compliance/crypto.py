"""Encryption for compliance_credentials.encrypted_password.

SECURITY (non-negotiable — Compliance Tracker spec, security requirement #1):
  - Portal login passwords (CDTFA, city license portals, etc.) are NEVER
    stored in plaintext, in this DB or anywhere else.
  - Uses Fernet (AES-128-CBC + HMAC-SHA256, via the `cryptography` package) —
    symmetric, authenticated encryption. `InvalidToken` on decrypt means
    "wrong key or tampered ciphertext", never a silent wrong answer.
  - The key lives OUTSIDE the SQLite DB: set `COMPLIANCE_ENCRYPTION_KEY` (a
    urlsafe-base64 Fernet key, e.g. `Fernet.generate_key().decode()`) as an
    environment variable — same place FILETRACK_TOKEN/IMAP_PASS live today
    (taxops/.env for dev, NSSM AppEnvironmentExtra or Windows DPAPI-backed
    secret storage for the real deployment). NEVER commit this value.

  Losing the key permanently loses every stored credential — there is no
  recovery path, by design (that's what "the key is the only thing that can
  decrypt it" means). Back it up like a master password. Rotating it
  requires decrypting every row with the OLD key and re-encrypting with the
  NEW one in one pass — never just swap the env var, or every existing
  credential becomes permanently unreadable garbage (see decrypt_password's
  InvalidToken message).

Dev/first-run fallback: if COMPLIANCE_ENCRYPTION_KEY is unset, a key is
generated once and persisted to <taxops repo root>/.compliance_key (added to
.gitignore) so a dev DB survives a process restart. This is loudly logged
every time it's used and is NOT a supported production posture — production
must set COMPLIANCE_ENCRYPTION_KEY explicitly.

No function in this module ever logs a plaintext password or raw
ciphertext — only counts, ids, and outcome (see routes/compliance.py for how
callers must also avoid this).
"""
from __future__ import annotations

import logging
import os
from pathlib import Path

logger = logging.getLogger("compliance")

_DEV_KEY_FILENAME = ".compliance_key"
_ENV_VAR = "COMPLIANCE_ENCRYPTION_KEY"

# Lazy singleton — built once per process from whichever key source resolves
# (env var, else dev fallback file). Not rebuilt mid-process; a running
# process keeps using the key it started with even if the env var changes
# underneath it (matches every other FILETRACK_*/IMAP_* "read once at import"
# convention in this codebase — see filetrack/DEPLOYMENT.md).
_cached_fernet = None


class ComplianceCryptoError(RuntimeError):
    """Raised for missing dependency, malformed key, or failed decrypt.
    Never includes the plaintext/ciphertext value itself in its message."""


def _dev_key_path() -> Path:
    # This file lives at <repo>/taxops/compliance/crypto.py, so repo root's
    # taxops/ dir is parent.parent.
    return Path(__file__).resolve().parent.parent / _DEV_KEY_FILENAME


def _load_or_create_dev_key() -> bytes:
    from cryptography.fernet import Fernet

    path = _dev_key_path()
    if path.exists():
        key = path.read_bytes().strip()
        logger.warning(
            "compliance: %s env var not set — using dev fallback key file at "
            "%s. This is NOT a supported production posture; set %s before "
            "handling real credentials, and back up whichever key is "
            "actually in use (losing it permanently loses every stored "
            "compliance credential).",
            _ENV_VAR, path, _ENV_VAR,
        )
        return key

    key = Fernet.generate_key()
    path.write_bytes(key)
    logger.warning(
        "compliance: generated a NEW dev fallback encryption key at %s "
        "because %s was unset. BACK THIS FILE UP NOW — if it is lost, every "
        "compliance credential encrypted with it becomes permanently "
        "unreadable. Set %s before deploying to production.",
        path, _ENV_VAR, _ENV_VAR,
    )
    return key


def _get_fernet():
    global _cached_fernet
    if _cached_fernet is not None:
        return _cached_fernet

    try:
        from cryptography.fernet import Fernet
    except ImportError as exc:  # pragma: no cover - dependency presence, not logic
        raise ComplianceCryptoError(
            "the 'cryptography' package is required for the Compliance "
            "Tracker module (pip install cryptography) — see requirements.txt"
        ) from exc

    raw_key = os.environ.get(_ENV_VAR, "").strip()
    if raw_key:
        key_bytes = raw_key.encode("ascii")
        key_source = "env"
    else:
        key_bytes = _load_or_create_dev_key()
        key_source = "dev_file"

    try:
        fernet = Fernet(key_bytes)
    except Exception as exc:
        raise ComplianceCryptoError(
            f"{_ENV_VAR} (source={key_source}) is not a valid Fernet key "
            f"(expected urlsafe-base64, 32 raw bytes): {exc}"
        ) from exc

    _cached_fernet = fernet
    return fernet


def reset_cached_key_for_tests() -> None:
    """Test-only: clear the process-level key singleton so a test can swap
    COMPLIANCE_ENCRYPTION_KEY / monkeypatch and get a fresh Fernet instance.
    Never call this from application code."""
    global _cached_fernet
    _cached_fernet = None


def encrypt_password(plaintext: str) -> bytes:
    """Encrypt a plaintext portal password for storage in
    compliance_credentials.encrypted_password. Returns Fernet ciphertext
    bytes (self-describing: version + timestamp + IV + ciphertext + HMAC).
    Never logs `plaintext`."""
    if plaintext is None or plaintext == "":
        raise ValueError("plaintext password is required")
    fernet = _get_fernet()
    return fernet.encrypt(plaintext.encode("utf-8"))


def decrypt_password(ciphertext: bytes) -> str:
    """Decrypt ciphertext back to the plaintext portal password.

    Callers MUST gate this behind admin-only RBAC and MUST audit-log every
    call (who/when/which credential) per Compliance Tracker security
    requirement #2 — this function itself performs no authorization check
    and does not write to the audit log; see routes/compliance.py's
    api_reveal_credential for the gate + audit write."""
    if not ciphertext:
        raise ValueError("ciphertext is required")
    from cryptography.fernet import InvalidToken

    fernet = _get_fernet()
    try:
        return fernet.decrypt(bytes(ciphertext)).decode("utf-8")
    except InvalidToken as exc:
        raise ComplianceCryptoError(
            "could not decrypt credential — either the wrong "
            f"{_ENV_VAR} is set for this process, or it changed since this "
            "credential was stored. Never re-key by swapping the env var "
            "alone; every already-encrypted password becomes permanently "
            "unreadable the moment the key changes without a decrypt-under-"
            "old/re-encrypt-under-new migration pass."
        ) from exc


def mask_username(username: str) -> str:
    """Cosmetic masking for a login *username* (not the password) in list
    views — e.g. 'xcelfin92' -> 'xce****92'. Usernames are not secret (they
    are frequently shared/known among staff already), this is purely to
    keep list views uncluttered; the full username is always shown on an
    authorized detail/edit screen."""
    if not username:
        return ""
    if len(username) <= 4:
        return username[0] + "*" * max(0, len(username) - 1)
    return username[:3] + "*" * (len(username) - 5) + username[-2:]


MASKED_PASSWORD_DISPLAY = "•" * 10
