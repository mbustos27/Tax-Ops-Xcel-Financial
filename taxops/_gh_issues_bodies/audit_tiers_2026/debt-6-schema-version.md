## Parent epic
Technical debt epic

## Problem
Migrations additive-only but no authoritative schema version ledger for ordering/coordination.

## Note
`app_settings` table already backs audit retention (`audit_service.py`). Extend with controlled version key (`schema_version`) instead of duplicate DDL.

## Fix
Integer `CURRENT_SCHEMA_VERSION`, helpers `_get_schema_version`/`_set_schema_version`, guarded upgrade blocks inside `db.py`.

## Definition of done
- `/health` or admin-only endpoint surfaces schema_version (pick one documented contract)
- python -m pytest tests/ -v passes
