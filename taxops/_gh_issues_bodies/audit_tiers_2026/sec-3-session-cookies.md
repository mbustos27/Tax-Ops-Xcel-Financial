## Parent epic
Security hardening epic

## Problem
No explicit Flask session cookie hardening (`SESSION_COOKIE_*`, `PERMANENT_SESSION_LIFETIME`). `_security_headers` sets global `Cache-Control: no-store` for all responses (~171–177) — see also DEBT-5 for static assets.

## Fix
After `app = Flask(__name__)`:
```python
from datetime import timedelta

app.config.update(
    SESSION_COOKIE_HTTPONLY=True,
    SESSION_COOKIE_SAMESITE="Lax",
    PERMANENT_SESSION_LIFETIME=timedelta(hours=12),
    # Enable once HTTPS terminates in front:
    # SESSION_COOKIE_SECURE=True,
)
```

Add CSP inside `_security_headers` (coordinate with DEBT-5 so static/cache policy stays coherent).

Gate `app.config["TEMPLATES_AUTO_RELOAD"]`: currently forced `True` at ~85 — set from `app.debug` instead.

Production: log warning when `SESSION_COOKIE_SECURE` ought to be True but HTTPS is off (env-gated).

## Definition of done
- HTTPONLY + SameSite=Lax + 12h lifetime
- CSP header added (tuned for Tailwind/script if needed)
- TEMPLATES_AUTO_RELOAD gated on debug
- python -m pytest tests/ -v passes
