## Parent epic
Security hardening epic

## Problem
Single shared credential compared with `==` against form input (see `taxops/app.py` ~824). No hashing, no constant-time compare, no lockout tied to identities. Credentials default from env: `_LOGIN_USER` / `_LOGIN_PASS` (~113–114).

## Fix — reference (verify exact lines during implementation)

```python
_LOGIN_USER = os.environ.get("TAXOPS_USER", "info")
_LOGIN_PASS = os.environ.get("TAXOPS_PASS", "2703Tax")
# ...
if username == _LOGIN_USER and password == _LOGIN_PASS:
```

New table:
```sql
CREATE TABLE IF NOT EXISTS auth_users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    display_name TEXT,
    role TEXT NOT NULL DEFAULT 'staff',
    is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    last_login_at TEXT,
    failed_attempts INTEGER NOT NULL DEFAULT 0,
    locked_until TEXT
)
```

Password handling:
```python
from werkzeug.security import generate_password_hash, check_password_hash

user = conn.execute(
    'SELECT * FROM auth_users WHERE username = ?',
    (username,)
).fetchone()
if user and check_password_hash(user['password_hash'], password):
    ...
```

Migration path:
- First boot after deploy: if `auth_users` is empty AND `TAXOPS_USER`/`TAXOPS_PASS` are set, auto-create one admin from env; then deprecate env password auth.

Audit: session already stores username on successful login today — wire `audit_log.user_id`/equivalent consistently with real usernames once multi-user ships.

## Definition of done
- auth_users table migrated in db.py safely
- /login uses `check_password_hash` (and timing-safe compares where comparing secrets)
- Old plaintext `==` comparison removed for production auth path
- python -m pytest tests/ -v passes
