# Cursor Coding Rules – TaxOps

## 1. Always Read Before Writing Code

Before making any change:

- Read `.cursor/project.md`
- Read `db.py` for schema
- Read relevant routes in `app.py`
- Read related templates in `/templates`

Do not assume structure — verify it.

---

## 2. Reuse Before Creating

- Reuse existing tables and columns whenever possible
- Do not create new tables if an existing one can be extended
- Do not duplicate data across tables unless explicitly required
- Check for existing fields before adding new ones

---

## 3. Database Safety Rules

- All schema changes must be added as safe migrations in `db.py`
- Never drop columns or tables
- Never rename existing columns unless explicitly instructed
- Prefer adding new nullable columns over modifying existing ones
- Maintain compatibility with existing data

---

## 4. Status System Rules

- Status is stored as `returns.client_status`
- Use existing status values (PROCESSING, HOLD, FINALIZE, PICKUP, EFILE READY, LOG OUT, REJECTED)
- Do not introduce new statuses without explicit instruction
- Do not bypass status transitions

---

## 5. Workflow Integrity

- All features must map to real-world office workflow
- Do not introduce abstract or generic systems
- UI must reflect how staff actually work

Examples:

- Pickup requires signatures + payment
- EFILE READY comes after pickup
- Rejected returns require rejection code + reason

---

## 6. UI Rules

- Prefer modifying existing pages over creating new ones
- Keep UI simple and functional
- Do not introduce complex styling systems
- Do not break existing dashboard or return detail page

---

## 7. E-file System Rules

- Do not integrate directly with Drake
- Do not automate IRS submission
- E-file batches are internal tracking only
- ACK results are manually entered

---

## 8. Payment System Rules

- Payments are tracked in the `payments` table
- Card payments include a 3% fee
- Receipt numbers come from QuickBooks and are manually entered
- Do not attempt QuickBooks integration

---

## 9. Import System Protection

- Do not break CSV import functionality
- Do not modify import logic without checking dependencies
- Imported data must remain compatible with dashboard and workflow

---

## 10. Code Style Rules

- Use existing patterns in the codebase
- Follow current Flask route structure
- Use snake_case for variables and fields
- Keep functions small and readable
- Avoid unnecessary abstraction

---

## 11. Change Scope Control

- Only implement what is requested in the issue
- Do not expand scope
- Do not add “nice to have” features
- If something is unclear, ask instead of guessing

---

## 12. Output Requirements

After implementing a feature:

- Explain what was added
- List any schema changes
- List modified files
- Explain how to test the feature

---

## 13. When Unsure

If any of the following occur:

- Missing fields
- Ambiguous workflow
- Conflicting logic

Stop and ask for clarification before proceeding.

## 14. Data Privacy & SSN Handling (CRITICAL)

- Full SSN must NEVER be stored, logged, displayed, or exported anywhere in the system
- Only last 4 digits (ssn_last4) may exist, and only for internal identification

### Export & Exposure Rules

- ssn_last4 must NEVER be included in:
  - CSV exports
  - reports
  - downloads
  - external integrations
  - logs or debug output
- ssn_last4 should NOT be displayed in:
  - dashboards
  - batch exports
  - any screen visible to non-essential staff

### Allowed Usage

- ssn_last4 may be used ONLY for:
  - internal matching (import, deduplication)
  - controlled display when absolutely necessary for identification

### UI Masking

- If displayed, ssn_last4 must be clearly labeled and minimized
- Prefer masking or avoiding display entirely when possible

### Developer Rules

- Do not add new SSN-related fields
- Do not expand SSN usage beyond existing fields
- Do not include SSN data in new features unless explicitly approved
- If a feature requires SSN usage, STOP and ask for clarification

### Logging Safety

- Never log SSN or ssn_last4 to console, files, or error output