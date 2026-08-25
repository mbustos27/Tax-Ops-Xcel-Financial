-- M8 proposed migration ONLY — DO NOT EXECUTE against TaxOps.
-- Follows db.py _migrate_existing_tables() / CURRENT_SCHEMA_VERSION convention.
-- Hypothetical next schema version: 24
--
-- When implementing for real:
--   1. Add block inside _migrate_existing_tables() in taxops/db.py
--   2. Bump CURRENT_SCHEMA_VERSION to 24
--   3. Test on a DB copy first (WAL checkpoint + copy + integrity_check)

-- Spouse may later link to a promoted client row (nullable; unused until write-phase).
ALTER TABLE spouses ADD COLUMN promoted_client_id INTEGER REFERENCES clients(id);

-- Explicit OBSERVED vs INFERRED provenance for recovered spouse identity.
ALTER TABLE spouses ADD COLUMN spouse_provenance TEXT;

-- Optional linkage back to an external audit run/finding (audit DB is separate).
ALTER TABLE spouses ADD COLUMN audit_finding_ref TEXT;

-- Index for future join from returns/clients tooling (safe if column exists).
-- CREATE INDEX IF NOT EXISTS idx_spouses_promoted_client ON spouses(promoted_client_id);

-- Flag non-production / fixture clients. Exclude from audits, counts, and reports.
-- Do NOT delete: merge_client_into does not reassign spouses/dependents/billing, and
-- filetrack resolves via returns.log_number — removing returns breaks barcode lookups.
-- Default 0 keeps all existing rows production until staff marks them.
ALTER TABLE clients ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0;

-- CREATE INDEX IF NOT EXISTS idx_clients_is_test ON clients(is_test);
