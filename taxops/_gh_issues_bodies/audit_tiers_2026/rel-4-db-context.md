## Parent epic
Reliability hardening epic

## Problem
Uneven guarantee that every `get_connection()` is closed — risk leaks / lock pressure.

## Fix
Standardize `with closing(get_connection()) as conn:` (or Flask `g` teardown) across high-churn routes. Inventory `get_connection()` call sites starting with `app.py`.

## Definition of done
- No known leak paths in audited modules
- python -m pytest tests/ -v passes
