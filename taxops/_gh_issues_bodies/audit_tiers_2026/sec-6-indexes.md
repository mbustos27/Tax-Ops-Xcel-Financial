## Parent epic
Security hardening epic

## Problem
Heavy join paths (`return_id`, dashboards, queues) may scan without indexes. Add `CREATE INDEX IF NOT EXISTS` in `init_db`/migration for hot columns (return_documents, payments, notes, missing_docs, dependents, extraction_queue, email_classifications, audit_log entity keys, returns filters).

Implement after `EXPLAIN QUERY PLAN` confirms table scans — align index list with actual table/column names in `db.py` (e.g. `f1099_nec_records` not `f1099_nec`).

## Definition of done
- Indexes migrated safely (no DROP)
- EXPLAIN shows SEARCH USING INDEX where expected on representative queries
- python -m pytest tests/ -v passes
