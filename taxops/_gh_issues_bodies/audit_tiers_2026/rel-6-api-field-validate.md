## Parent epic
Reliability hardening epic

## Problem
Dynamic field updates (`api_field`/similar) trust SQLite coercion — invalid types can persist.

## Fix
Build `RETURN_FIELD_VALIDATORS` (and siblings) keyed from `RETURN_EDITABLE` whitelist; coerce/bool/year/money/date with explicit validation + 400 on failure.

## Definition of done
- Representative bad payloads rejected (`verified='banana'`, bad tax_year strings)
- Legitimate payloads unchanged
- python -m pytest tests/ -v passes
