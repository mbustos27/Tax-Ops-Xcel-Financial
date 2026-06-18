## Parent epic
Security hardening epic

## Problem
No CSRF protection exists anywhere. Every state-changing endpoint accepts POST based purely on the session cookie. request.get_json(force=True) is used in places (see app.py grep for `force=True`; also ai_routes.py), ignoring Content-Type. A malicious page on the same office LAN could cross-post to TaxOps.

Affected endpoints include: /api/return/<id>/field, /api/payment, /login, /api/clients/<id>/merge, document upload, bulk update, season rollover commit.

## Fix
Add Flask-WTF CSRF protection:

```bash
pip install flask-wtf
```

In app.py:
```python
from flask_wtf.csrf import CSRFProtect
csrf = CSRFProtect(app)
```

For JSON API endpoints use the X-CSRFToken header pattern:
```python
# In base.html meta tag:
<meta name='csrf-token' content='{{ csrf_token() }}'>

# In app.js fetch wrapper:
headers: {
  'Content-Type': 'application/json',
  'X-CSRFToken': document.querySelector('meta[name=csrf-token]').content
}
```

Replace all request.get_json(force=True) with request.get_json(silent=True) and explicit Content-Type validation.

## Definition of done
- Flask-WTF installed and CSRFProtect(app) active
- All POST/PUT/PATCH/DELETE endpoints protected
- All fetch() calls in app.js send X-CSRFToken header
- request.get_json(force=True) replaced throughout
- Test: verify cross-origin POST returns 400
- python -m pytest tests/ -v passes
