## Parent epic
Technical debt epic

## Problem
Global `Cache-Control: no-store` in `_security_headers` hits `/static/` too — fights `taxops_asset_cache_version()` versioning.

## Fix
Special-case `/static/` for long-lived immutable cache headers; retain `no-store` for HTML/API.

## Definition of done
- DevTools confirms static hashed assets cached; HTML stays no-store
- python -m pytest tests/ -v passes
