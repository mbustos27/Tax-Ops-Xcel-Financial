## Goal
TaxOps handles real client tax data including W-2s, 1099s, and financial records. Before any staff beyond the primary admin use this on the office network, these security issues must be resolved.

## Audit source
Professional codebase review identified these as Critical/High severity findings.

## Child issues
- SEC-1: CSRF protection on all state-changing endpoints
- SEC-2: Per-user accounts with hashed passwords
- SEC-3: Session cookie security flags and lifetime
- SEC-4: SQLite WAL mode and busy_timeout
- SEC-5: MAX_CONTENT_LENGTH and upload rate limiting
- SEC-6: Missing database indexes
- SEC-7: Login rate limiting and lockout

## What is already good
- SSN scrubbing at three layers (extractor allow-list, scrub_ssn_from_dict, mask_audit_payload)
- Whitelisted dynamic SQL (RETURN_EDITABLE etc.)
- FK enforcement on every connection
- Strict env validation refuses to boot with default secrets
- No secrets committed to git

## Definition of done
All SEC-1 through SEC-7 closed and verified on the live server.
