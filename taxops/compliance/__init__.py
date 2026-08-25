"""Compliance Tracker — CDTFA sales/use tax filings, city business license
renewals, SBE/CDTFA monthly prepayment deposits, and misc individual filings.

Replaces ACCOUNTING_LOG_2026.xlsx (11 hand-maintained sheets, shared/plaintext
portal credentials, unrestricted SSN cells) with an in-app module matching
TaxOps's existing RBAC/audit/i18n conventions.

Submodules:
  compliance.crypto  — Fernet encryption for stored portal credentials.
                        Never import plaintext passwords past this boundary
                        without going through encrypt_password()/decrypt_password().

Routes live in routes/compliance.py (matches routes/work_orders.py etc.);
schema lives in db.py's _migrate_existing_tables (matches every other
TaxOps feature table) — this package holds only the credential-security
logic that has no TaxOps-specific dependency.
"""
