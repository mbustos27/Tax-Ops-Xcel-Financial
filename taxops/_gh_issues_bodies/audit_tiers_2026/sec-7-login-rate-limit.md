## Parent epic
Security hardening epic

## Depends on
SEC-2 (per-user identities + hashed passwords recommended before lockout semantics)

## Problem
`/login` has no backoff; brute force bounded only by LAN access.

## Fix
SQLite-backed counters on `auth_users` (failed_attempts, locked_until) or companion table keyed by username+IP depending on posture. Always return identical error UX on failure where practical to reduce timing/leak signals.

Test: sixth rapid failure → 429 (or configurable lock).

## Definition of done
- Lockout thresholds documented in env_validation / README where staff can see values
- python -m pytest tests/ -v passes
