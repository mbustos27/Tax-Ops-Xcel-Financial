# sqlite_master for snapshot/taxops.db (verbatim)

## index: idx_ai_chat_common_exp
```sql
CREATE INDEX idx_ai_chat_common_exp ON ai_chat_common_answers(expires_at)
```

## index: idx_audit_log_created
```sql
CREATE INDEX idx_audit_log_created ON audit_log(created_at)
```

## index: idx_audit_log_entity
```sql
CREATE INDEX idx_audit_log_entity ON audit_log(entity_type, entity_id)
```

## index: idx_audit_log_user
```sql
CREATE INDEX idx_audit_log_user       ON audit_log(user_id)
```

## index: idx_auth_users_username
```sql
CREATE INDEX idx_auth_users_username  ON auth_users(username)
```

## index: idx_billing_requests_status
```sql
CREATE INDEX idx_billing_requests_status ON billing_requests(status, created_at)
```

## index: idx_client_billing_snapshot
```sql
CREATE UNIQUE INDEX idx_client_billing_snapshot ON client_billing(client_id, balance_as_of)
```

## index: idx_client_deps_client
```sql
CREATE INDEX idx_client_deps_client ON client_dependents(client_id)
```

## index: idx_client_deps_drake_dedup
```sql
CREATE UNIQUE INDEX idx_client_deps_drake_dedup
        ON client_dependents(client_id, drake_dependent_id)
        WHERE drake_dependent_id IS NOT NULL
        
```

## index: idx_clients_name
```sql
CREATE INDEX idx_clients_name ON clients(last_name, first_name)
```

## index: idx_compliance_accounts_client
```sql
CREATE INDEX idx_compliance_accounts_client ON compliance_accounts(compliance_client_id)
```

## index: idx_compliance_accounts_credential
```sql
CREATE INDEX idx_compliance_accounts_credential ON compliance_accounts(credential_id)
```

## index: idx_compliance_accounts_type
```sql
CREATE INDEX idx_compliance_accounts_type ON compliance_accounts(account_type, active)
```

## index: idx_compliance_clients_active
```sql
CREATE INDEX idx_compliance_clients_active ON compliance_clients(active)
```

## index: idx_compliance_clients_name
```sql
CREATE INDEX idx_compliance_clients_name ON compliance_clients(name)
```

## index: idx_compliance_correspondence_client_month
```sql
CREATE INDEX idx_compliance_correspondence_client_month ON compliance_correspondence_log(compliance_client_id, month)
```

## index: idx_compliance_credentials_shared
```sql
CREATE INDEX idx_compliance_credentials_shared ON compliance_credentials(shared_login)
```

## index: idx_compliance_filing_periods_due
```sql
CREATE INDEX idx_compliance_filing_periods_due ON compliance_filing_periods(period_due_date)
```

## index: idx_compliance_filing_periods_status
```sql
CREATE INDEX idx_compliance_filing_periods_status ON compliance_filing_periods(status)
```

## index: idx_dash_saved_shared_default
```sql
CREATE INDEX idx_dash_saved_shared_default
        ON dashboard_saved_filters(user_id, is_default)
        
```

## index: idx_dash_saved_user
```sql
CREATE INDEX idx_dash_saved_user ON dashboard_saved_filters(user_id)
```

## index: idx_dependents_return
```sql
CREATE INDEX idx_dependents_return ON dependents(return_id)
```

## index: idx_drake_household_prefill_year
```sql
CREATE INDEX idx_drake_household_prefill_year ON drake_household_prefill(tax_year)
```

## index: idx_drake_prefill_links_client
```sql
CREATE INDEX idx_drake_prefill_links_client ON drake_prefill_links(client_id)
```

## index: idx_drake_prefill_links_status
```sql
CREATE INDEX idx_drake_prefill_links_status ON drake_prefill_links(tax_year, prefill_status, disposition_status)
```

## index: idx_efile_items_batch
```sql
CREATE INDEX idx_efile_items_batch   ON efile_batch_items(batch_id)
```

## index: idx_efile_items_return
```sql
CREATE INDEX idx_efile_items_return  ON efile_batch_items(return_id)
```

## index: idx_email_class_domain
```sql
CREATE INDEX idx_email_class_domain   ON "archive_email_classifications"(sender_domain)
```

## index: idx_email_class_email
```sql
CREATE INDEX idx_email_class_email    ON "archive_email_classifications"(sender_email)
```

## index: idx_email_inbox_unassigned
```sql
CREATE INDEX idx_email_inbox_unassigned ON email_inbox(is_assigned, is_deleted, received_at)
```

## index: idx_email_proc_log_outcome
```sql
CREATE INDEX idx_email_proc_log_outcome ON email_processing_log(outcome, last_attempt_at)
```

## index: idx_extraction_failed
```sql
CREATE INDEX idx_extraction_failed    ON extraction_queue(status, attempts)
```

## index: idx_extraction_return
```sql
CREATE INDEX idx_extraction_return    ON extraction_queue(return_id)
```

## index: idx_extraction_status
```sql
CREATE INDEX idx_extraction_status    ON extraction_queue(status, created_at)
```

## index: idx_filetrack_history_log
```sql
CREATE INDEX idx_filetrack_history_log ON filetrack_status_history(log_number, scanned_at)
```

## index: idx_filetrack_history_return
```sql
CREATE INDEX idx_filetrack_history_return ON filetrack_status_history(return_id, scanned_at)
```

## index: idx_import_rows_batch
```sql
CREATE INDEX idx_import_rows_batch ON import_rows(batch_id)
```

## index: idx_lb_coa_unique
```sql
CREATE UNIQUE INDEX idx_lb_coa_unique
            ON ledgerbridge_chart_of_accounts (client_id, qb_account_name)
```

## index: idx_lb_merchant_unique
```sql
CREATE UNIQUE INDEX idx_lb_merchant_unique
            ON ledgerbridge_merchant_memory (client_id, normalized_merchant)
```

## index: idx_missing_docs_open
```sql
CREATE INDEX idx_missing_docs_open    ON missing_docs(return_id, is_resolved)
```

## index: idx_missing_docs_return
```sql
CREATE INDEX idx_missing_docs_return  ON missing_docs(return_id)
```

## index: idx_notes_return
```sql
CREATE INDEX idx_notes_return         ON notes(return_id)
```

## index: idx_notifications_user_unread
```sql
CREATE INDEX idx_notifications_user_unread ON notifications(user_id, is_read, created_at)
```

## index: idx_ocr_extraction_cache_image
```sql
CREATE INDEX idx_ocr_extraction_cache_image ON ocr_extraction_cache(image_sha256)
```

## index: idx_payments_return
```sql
CREATE INDEX idx_payments_return      ON payments(return_id)
```

## index: idx_receipt_queue_doc
```sql
CREATE INDEX idx_receipt_queue_doc ON receipt_queue(return_document_id)
```

## index: idx_receipt_queue_status
```sql
CREATE INDEX idx_receipt_queue_status ON receipt_queue(status, created_at)
```

## index: idx_return_docs_hash
```sql
CREATE INDEX idx_return_docs_hash     ON return_documents(return_id, file_hash)
```

## index: idx_return_docs_return
```sql
CREATE INDEX idx_return_docs_return   ON return_documents(return_id)
```

## index: idx_return_docs_type
```sql
CREATE INDEX idx_return_docs_type     ON return_documents(return_id, doc_type)
```

## index: idx_returns_client_year
```sql
CREATE INDEX idx_returns_client_year ON returns(client_id, tax_year)
```

## index: idx_returns_proc_year
```sql
CREATE INDEX idx_returns_proc_year    ON returns(processor, tax_year)
```

## index: idx_returns_status_year
```sql
CREATE INDEX idx_returns_status_year  ON returns(client_status, tax_year)
```

## index: idx_returns_unique_client_year
```sql
CREATE UNIQUE INDEX idx_returns_unique_client_year
        ON returns(client_id, tax_year)
        WHERE client_status != 'CANCELLED'
        
```

## index: idx_returns_updated_at
```sql
CREATE INDEX idx_returns_updated_at   ON returns(updated_at)
```

## index: idx_spouse_import_client
```sql
CREATE INDEX idx_spouse_import_client ON client_spouse_import(client_id)
```

## index: idx_spouse_import_dedup
```sql
CREATE UNIQUE INDEX idx_spouse_import_dedup
        ON client_spouse_import(client_id)
        
```

## index: idx_spouses_one_per_client
```sql
CREATE UNIQUE INDEX idx_spouses_one_per_client ON spouses(client_id)
```

## index: idx_status_events_return
```sql
CREATE INDEX idx_status_events_return ON status_events(return_id)
```

## index: idx_wo_qp_components_pick
```sql
CREATE INDEX idx_wo_qp_components_pick ON work_order_quick_pick_components(quick_pick_id, sort_order)
```

## index: idx_work_order_items_wo
```sql
CREATE INDEX idx_work_order_items_wo ON work_order_items(work_order_id, sort_order)
```

## index: idx_work_orders_assigned
```sql
CREATE INDEX idx_work_orders_assigned ON work_orders(assigned_to_user_id)
```

## index: idx_work_orders_client
```sql
CREATE INDEX idx_work_orders_client ON work_orders(client_id)
```

## index: idx_work_orders_status_date
```sql
CREATE INDEX idx_work_orders_status_date ON work_orders(status, date_created)
```

## index: sqlite_autoindex_ai_chat_common_answers_1
```sql
-- NULL
```

## index: sqlite_autoindex_app_settings_1
```sql
-- NULL
```

## index: sqlite_autoindex_archive_domain_classifications_1
```sql
-- NULL
```

## index: sqlite_autoindex_auth_users_1
```sql
-- NULL
```

## index: sqlite_autoindex_drake_form_prefill_1
```sql
-- NULL
```

## index: sqlite_autoindex_drake_household_prefill_1
```sql
-- NULL
```

## index: sqlite_autoindex_drake_prefill_links_1
```sql
-- NULL
```

## index: sqlite_autoindex_efile_batch_items_1
```sql
-- NULL
```

## index: sqlite_autoindex_email_processing_log_1
```sql
-- NULL
```

## index: sqlite_autoindex_email_sender_rules_1
```sql
-- NULL
```

## index: sqlite_autoindex_extension_batch_items_1
```sql
-- NULL
```

## index: sqlite_autoindex_import_batches_1
```sql
-- NULL
```

## index: sqlite_autoindex_ocr_extraction_cache_1
```sql
-- NULL
```

## index: ux_billing_requests_wo
```sql
CREATE UNIQUE INDEX ux_billing_requests_wo ON billing_requests(work_order_id)
```

## index: ux_compliance_filing_periods_account_label
```sql
CREATE UNIQUE INDEX ux_compliance_filing_periods_account_label ON compliance_filing_periods(compliance_account_id, period_label)
```

## index: ux_returns_log_year
```sql
CREATE UNIQUE INDEX ux_returns_log_year ON returns(log_number, tax_year)
```

## index: ux_work_orders_number
```sql
CREATE UNIQUE INDEX ux_work_orders_number ON work_orders(work_order_number)
```

## table: ai_chat_common_answers
```sql
CREATE TABLE ai_chat_common_answers (
          cache_key             TEXT PRIMARY KEY,
          normalized_question   TEXT NOT NULL,
          season_year           INTEGER NOT NULL,
          answer                TEXT NOT NULL,
          tool_used             TEXT,
          payload_json          TEXT NOT NULL,
          created_at            TEXT NOT NULL,
          expires_at            TEXT NOT NULL,
          hit_count             INTEGER NOT NULL DEFAULT 0
        )
```

## table: app_settings
```sql
CREATE TABLE app_settings (
          key        TEXT PRIMARY KEY,
          value      TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
```

## table: archive_domain_classifications
```sql
CREATE TABLE "archive_domain_classifications" (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          domain              TEXT NOT NULL UNIQUE,
          classification      TEXT NOT NULL,
          confidence_count    INTEGER NOT NULL DEFAULT 1,
          last_seen           TEXT NOT NULL,
          last_confirmed_by   TEXT,
          last_confirmed_at   TEXT,
          graduated           INTEGER NOT NULL DEFAULT 0
        )
```

## table: archive_email_classifications
```sql
CREATE TABLE "archive_email_classifications" (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email    TEXT,
          sender_domain   TEXT,
          subject_snippet TEXT,
          classification  TEXT NOT NULL,
          confirmed_by    TEXT,
          confirmed_at    TEXT,
          created_at      TEXT NOT NULL,
          source          TEXT NOT NULL DEFAULT 'auto'
        , reviewed_missed INTEGER NOT NULL DEFAULT 0, email_routed_ok INTEGER NOT NULL DEFAULT 0, match_score INTEGER, matched_client_id INTEGER, match_status TEXT NOT NULL DEFAULT 'auto')
```

## table: audit_log
```sql
CREATE TABLE audit_log (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id      TEXT,
          action       TEXT NOT NULL,
          entity_type  TEXT NOT NULL,
          entity_id    TEXT,
          before_json  TEXT,
          after_json   TEXT,
          ip_address   TEXT,
          created_at   TEXT NOT NULL
        )
```

## table: auth_users
```sql
CREATE TABLE auth_users (
          id               INTEGER PRIMARY KEY AUTOINCREMENT,
          username         TEXT NOT NULL UNIQUE,
          password_hash    TEXT NOT NULL,
          display_name     TEXT,
          role             TEXT NOT NULL DEFAULT 'staff',
          is_active        INTEGER NOT NULL DEFAULT 1,
          created_at       TEXT NOT NULL,
          last_login_at    TEXT,
          failed_attempts  INTEGER NOT NULL DEFAULT 0,
          locked_until     TEXT
        , must_change_password INTEGER NOT NULL DEFAULT 0, has_seen_orientation INTEGER NOT NULL DEFAULT 0)
```

## table: billing_requests
```sql
CREATE TABLE billing_requests (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          work_order_id       INTEGER NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
          client_id           INTEGER REFERENCES clients(id),
          client_name         TEXT NOT NULL,
          amount              NUMERIC NOT NULL DEFAULT 0,
          status              TEXT NOT NULL DEFAULT 'pending',
          created_by_user_id  INTEGER REFERENCES auth_users(id),
          created_at          TEXT NOT NULL,
          updated_at          TEXT NOT NULL
        )
```

## table: client_billing
```sql
CREATE TABLE client_billing (
        id            INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id     INTEGER NOT NULL REFERENCES clients(id),
        balance_due   REAL,
        balance_as_of DATE,
        source        TEXT DEFAULT 'TY2025 Drake import',
        created_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
```

## table: client_dependents
```sql
CREATE TABLE client_dependents (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id             INTEGER NOT NULL REFERENCES clients(id),
          drake_dependent_id    TEXT,
          last_name             TEXT,
          first_name            TEXT NOT NULL,
          date_of_birth         TEXT,
          relationship          TEXT,
          is_claimed_dependent  INTEGER NOT NULL DEFAULT 1,
          hoh_qualifier_only    INTEGER NOT NULL DEFAULT 0,
          source                TEXT DEFAULT 'TY2025 Drake import',
          match_confidence      REAL,
          needs_review          INTEGER NOT NULL DEFAULT 0,
          confirmed_at_intake   INTEGER NOT NULL DEFAULT 0,
          removed_for_ty2026    INTEGER NOT NULL DEFAULT 0,
          created_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        , taxpayer_name TEXT)
```

## table: client_spouse_import
```sql
CREATE TABLE client_spouse_import (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id             INTEGER NOT NULL REFERENCES clients(id),
          taxpayer_name         TEXT,
          spouse_last_name      TEXT,
          spouse_first_name     TEXT NOT NULL,
          spouse_dob            TEXT,
          filing_status         TEXT,
          source                TEXT DEFAULT 'TY2025 Drake import',
          match_confidence      REAL,
          needs_review          INTEGER NOT NULL DEFAULT 0,
          applied_at            TEXT,
          created_at            TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        )
```

## table: clients
```sql
CREATE TABLE clients (
          id INTEGER PRIMARY KEY,
          last_name TEXT,
          first_name TEXT,
          display_name TEXT,
          ssn_last4 TEXT,
          referral_flag INTEGER,
          referred_by TEXT,
          created_at TEXT,
          updated_at TEXT
        , spouse_last_name TEXT, spouse_first_name TEXT, taxpayer_dob TEXT, spouse_dob TEXT, taxpayer_occupation TEXT, spouse_occupation TEXT, taxpayer_phone TEXT, taxpayer_cell TEXT, taxpayer_work_phone TEXT, spouse_cell TEXT, spouse_work_phone TEXT, taxpayer_email TEXT, spouse_email TEXT, address TEXT, is_new_client INTEGER DEFAULT 0, prior_year_log TEXT, id_type INTEGER)
```

## table: compliance_accounts
```sql
CREATE TABLE compliance_accounts (
          id                      INTEGER PRIMARY KEY AUTOINCREMENT,
          compliance_client_id    INTEGER NOT NULL REFERENCES compliance_clients(id) ON DELETE CASCADE,
          account_type            TEXT NOT NULL,
          account_number          TEXT,
          city_name               TEXT,
          frequency               TEXT NOT NULL DEFAULT 'quarterly',
          credential_id           INTEGER REFERENCES compliance_credentials(id),
          fee                     NUMERIC,
          active                  INTEGER NOT NULL DEFAULT 1,
          created_at              TEXT NOT NULL,
          updated_at              TEXT NOT NULL
        )
```

## table: compliance_clients
```sql
CREATE TABLE compliance_clients (
          id                          INTEGER PRIMARY KEY AUTOINCREMENT,
          name                        TEXT NOT NULL,
          client_type                 TEXT NOT NULL DEFAULT 'business',
          corp_number                 TEXT,
          fein                        TEXT,
          address                     TEXT,
          city                        TEXT,
          zip                         TEXT,
          phone                       TEXT,
          ssn_last4                   TEXT,
          assigned_preparer_user_id   INTEGER REFERENCES auth_users(id),
          active                      INTEGER NOT NULL DEFAULT 1,
          created_at                  TEXT NOT NULL,
          updated_at                  TEXT NOT NULL
        )
```

## table: compliance_correspondence_log
```sql
CREATE TABLE compliance_correspondence_log (
          id                      INTEGER PRIMARY KEY AUTOINCREMENT,
          compliance_client_id    INTEGER NOT NULL REFERENCES compliance_clients(id) ON DELETE CASCADE,
          month                    TEXT NOT NULL,
          note_type               TEXT NOT NULL DEFAULT 'general',
          note                     TEXT,
          created_by_user_id      INTEGER REFERENCES auth_users(id),
          created_at               TEXT NOT NULL
        )
```

## table: compliance_credentials
```sql
CREATE TABLE compliance_credentials (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          login_username        TEXT NOT NULL,
          encrypted_password    BLOB,
          encryption_key_ref    TEXT NOT NULL DEFAULT 'default',
          shared_login          INTEGER NOT NULL DEFAULT 0,
          last_rotated_at       TEXT,
          needs_rotation        INTEGER NOT NULL DEFAULT 0,
          notes                 TEXT,
          created_at            TEXT NOT NULL,
          updated_at            TEXT NOT NULL
        )
```

## table: compliance_filing_periods
```sql
CREATE TABLE compliance_filing_periods (
          id                        INTEGER PRIMARY KEY AUTOINCREMENT,
          compliance_account_id     INTEGER NOT NULL REFERENCES compliance_accounts(id) ON DELETE CASCADE,
          period_type               TEXT NOT NULL,
          period_label              TEXT NOT NULL,
          period_start              TEXT,
          period_due_date           TEXT,
          fee                       NUMERIC,
          status                    TEXT NOT NULL DEFAULT 'needs_sales_data',
          sales_data_received_at    TEXT,
          filed_at                  TEXT,
          done_at                   TEXT,
          done_by_user_id           INTEGER REFERENCES auth_users(id),
          notes                     TEXT,
          created_at                TEXT NOT NULL,
          updated_at                TEXT NOT NULL
        )
```

## table: dashboard_saved_filters
```sql
CREATE TABLE dashboard_saved_filters (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          name TEXT NOT NULL,
          filter_json TEXT NOT NULL,
          is_default INTEGER NOT NULL DEFAULT 0,
          is_shared INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
```

## table: dependents
```sql
CREATE TABLE dependents (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          full_name TEXT,
          ssn_last4 TEXT,
          relationship TEXT,
          date_of_birth TEXT,
          medi_cal INTEGER DEFAULT 0,
          created_at TEXT, is_deleted INTEGER NOT NULL DEFAULT 0, on_medicare INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: drake_form_prefill
```sql
CREATE TABLE drake_form_prefill (
          id            INTEGER PRIMARY KEY,
          link_id       INTEGER NOT NULL REFERENCES drake_prefill_links(id)
                        ON DELETE CASCADE,
          tax_year      INTEGER NOT NULL,
          form_counts   TEXT    NOT NULL,
          return_type   TEXT,
          source_files  TEXT,
          created_at    TEXT    NOT NULL,
          UNIQUE (link_id)
        )
```

## table: drake_household_prefill
```sql
CREATE TABLE drake_household_prefill (
          id                 INTEGER PRIMARY KEY,
          link_id            INTEGER NOT NULL REFERENCES drake_prefill_links(id)
                             ON DELETE CASCADE,
          tax_year           INTEGER NOT NULL,
          taxpayer_dob       TEXT,
          taxpayer_phone     TEXT,
          taxpayer_email     TEXT,
          spouse_name        TEXT,
          spouse_dob         TEXT,
          spouse_phone       TEXT,
          dependents_json    TEXT    NOT NULL DEFAULT '[]',
          source_file        TEXT,
          created_at         TEXT    NOT NULL,
          updated_at         TEXT    NOT NULL,
          UNIQUE (link_id)
        )
```

## table: drake_prefill_links
```sql
CREATE TABLE drake_prefill_links (
          id                   INTEGER PRIMARY KEY,
          tax_year             INTEGER NOT NULL,
          csm_ssn_last4        TEXT    NOT NULL,
          csm_name_raw         TEXT    NOT NULL,
          csm_name_norm        TEXT    NOT NULL,
          client_id            INTEGER REFERENCES clients(id),
          prefill_status       TEXT    NOT NULL
            CHECK (prefill_status IN (
              'PRIOR_YEAR_FORMS_AVAILABLE',
              'NO_PRIOR_FORM_DATA',
              'NEEDS_MANUAL_LINK',
              'LOW_CONFIDENCE_NO_MATCH'
            )),
          disposition_status   TEXT
            CHECK (disposition_status IS NULL OR disposition_status IN (
              'PY_FILED_ACCEPTED',
              'PY_REJECTED',
              'PY_EXTENDED',
              'PY_INCOMPLETE',
              'PY_ROLLOVER_ONLY',
              'PY_STATUS_UNKNOWN'
            )),
          csm_status_raw       TEXT,
          csm_status_as_of     TEXT,
          csm_anchor_changed   TEXT,
          purple_name          TEXT,
          match_tier           TEXT
            CHECK (match_tier IS NULL OR match_tier IN (
              'deterministic', 'fuzzy', 'manual'
            )),
          match_score          REAL,
          matched_variant      TEXT,
          resolved_by          TEXT,
          resolved_at          TEXT,
          import_batch_id      INTEGER REFERENCES import_batches(id),
          created_at           TEXT    NOT NULL,
          updated_at           TEXT    NOT NULL,
          UNIQUE (tax_year, csm_ssn_last4, csm_name_norm)
        )
```

## table: efile_batch_items
```sql
CREATE TABLE efile_batch_items (
          id               INTEGER PRIMARY KEY,
          batch_id         INTEGER NOT NULL,
          return_id        INTEGER NOT NULL,
          log_number       TEXT,
          client_name      TEXT,
          ssn_last4        TEXT,
          tax_year         INTEGER,
          receipt_number   TEXT,
          fee_paid         REAL,
          pickup_date      TEXT,
          transmission_date TEXT,
          ack_status       TEXT NOT NULL DEFAULT 'pending',
          ack_date         TEXT,
          rejection_code   TEXT,
          rejection_reason TEXT,
          needs_calculation INTEGER NOT NULL DEFAULT 0,
          created_at       TEXT NOT NULL, cc_fee REAL,
          FOREIGN KEY (batch_id)  REFERENCES efile_batches(id),
          FOREIGN KEY (return_id) REFERENCES returns(id),
          UNIQUE (batch_id, return_id)
        )
```

## table: efile_batches
```sql
CREATE TABLE efile_batches (
          id               INTEGER PRIMARY KEY,
          transmission_date TEXT NOT NULL,
          notes            TEXT,
          status           TEXT NOT NULL DEFAULT 'open',
          created_at       TEXT NOT NULL
        , transmitted_at TEXT)
```

## table: email_inbox
```sql
CREATE TABLE email_inbox (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email        TEXT,
          sender_domain       TEXT,
          subject_snippet     TEXT,
          filename            TEXT NOT NULL,
          original_filename   TEXT,
          file_path           TEXT NOT NULL,
          file_size_bytes     INTEGER,
          received_at         TEXT NOT NULL,
          assigned_return_id  INTEGER REFERENCES returns(id),
          assigned_by         TEXT,
          assigned_at         TEXT,
          is_assigned         INTEGER NOT NULL DEFAULT 0,
          is_deleted          INTEGER NOT NULL DEFAULT 0
        , sender_name TEXT, suggested_return_id INTEGER REFERENCES returns(id), suggestion_method TEXT, suggestion_score INTEGER)
```

## table: email_processing_log
```sql
CREATE TABLE email_processing_log (
          id               INTEGER PRIMARY KEY AUTOINCREMENT,
          message_uid      TEXT NOT NULL,
          imap_folder      TEXT NOT NULL,
          sender_domain    TEXT,
          subject_snippet  TEXT,
          outcome          TEXT NOT NULL,
          attempt_count    INTEGER NOT NULL DEFAULT 1,
          last_attempt_at  TEXT NOT NULL,
          error_message    TEXT,
          doc_id           INTEGER REFERENCES return_documents(id),
          return_id        INTEGER REFERENCES returns(id), suppression_reason TEXT,
          UNIQUE(message_uid, imap_folder)
        )
```

## table: email_sender_rules
```sql
CREATE TABLE email_sender_rules (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          domain      TEXT NOT NULL UNIQUE,
          rule_type   TEXT NOT NULL DEFAULT 'always_promotional',
          note        TEXT,
          created_by  TEXT,
          created_at  TEXT NOT NULL
        , rule_scope TEXT NOT NULL DEFAULT 'domain', action TEXT NOT NULL DEFAULT 'block')
```

## table: extension_batch_items
```sql
CREATE TABLE extension_batch_items (
          id               INTEGER PRIMARY KEY,
          batch_id         INTEGER NOT NULL,
          return_id        INTEGER NOT NULL,
          log_number       TEXT,
          client_name      TEXT,
          tax_year         INTEGER,
          filing_date      TEXT,
          ack_status       TEXT NOT NULL DEFAULT 'pending',
          ack_date         TEXT,
          rejection_reason TEXT,
          created_at       TEXT NOT NULL,
          FOREIGN KEY (batch_id)  REFERENCES extension_batches(id),
          FOREIGN KEY (return_id) REFERENCES returns(id),
          UNIQUE (batch_id, return_id)
        )
```

## table: extension_batches
```sql
CREATE TABLE extension_batches (
          id               INTEGER PRIMARY KEY,
          filing_date      TEXT NOT NULL,
          notes            TEXT,
          transmitted_at   TEXT,
          status           TEXT NOT NULL DEFAULT 'open',
          created_at       TEXT NOT NULL
        )
```

## table: extraction_queue
```sql
CREATE TABLE extraction_queue (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          doc_id INTEGER NOT NULL REFERENCES return_documents(id),
          return_id INTEGER NOT NULL REFERENCES returns(id),
          status TEXT NOT NULL DEFAULT 'pending',
          confidence REAL,
          detected_form_type TEXT,
          extracted_fields TEXT,
          extraction_method TEXT,
          error_message TEXT,
          attempts INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL,
          processed_at TEXT,
          reviewed_by TEXT,
          reviewed_at TEXT
        )
```

## table: f1099_div_records
```sql
CREATE TABLE f1099_div_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          total_ordinary_dividends TEXT,
          qualified_dividends TEXT,
          total_capital_gain TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        , payer_address TEXT, box1a_total_ordinary_dividends TEXT, box1b_qualified_dividends TEXT, box2a_total_capital_gain TEXT, box2b_unrecap_sec1250_gain TEXT, box2c_section_1202_gain TEXT, box2d_collectibles_gain TEXT, box2e_section_897_ordinary_dividends TEXT, box2f_section_897_capital_gain TEXT, box3_nondividend_distributions TEXT, box4_federal_income_tax_withheld TEXT, box5_section_199a_dividends TEXT, box6_investment_expenses TEXT, box7_foreign_tax_paid TEXT, box8_foreign_country TEXT, box9_cash_liquidation_distributions TEXT, box10_noncash_liquidation_distributions TEXT, box11_fatca_filing_requirement INTEGER DEFAULT 0, box12_exempt_interest_dividends TEXT, box13_specified_private_activity_bond TEXT, box14_state TEXT, box15_state_identification TEXT, box16_state_tax_withheld TEXT)
```

## table: f1099_int_records
```sql
CREATE TABLE f1099_int_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          interest_income TEXT,
          early_withdrawal_penalty TEXT,
          us_savings_bond_interest TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        , payer_address TEXT, box1_interest_income TEXT, box2_early_withdrawal_penalty TEXT, box3_us_savings_bond_treasury_interest TEXT, box4_federal_income_tax_withheld TEXT, box5_investment_expenses TEXT, box6_foreign_tax_paid TEXT, box7_foreign_country TEXT, box8_tax_exempt_interest TEXT, box9_specified_private_activity_bond_interest TEXT, box10_market_discount TEXT, box11_bond_premium TEXT, box12_bond_premium_treasury_obligations TEXT, box13_bond_premium_tax_exempt_bond TEXT, box14_tax_exempt_bond_cusip TEXT, box15_state TEXT, box16_state_identification TEXT, box17_state_tax_withheld TEXT)
```

## table: f1099_misc_records
```sql
CREATE TABLE f1099_misc_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          rents TEXT,
          royalties TEXT,
          other_income TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        , payer_address TEXT, box1_rents TEXT, box2_royalties TEXT, box3_other_income TEXT, box4_federal_income_tax_withheld TEXT, box5_fishing_boat_proceeds TEXT, box6_medical_health_care_payments TEXT, box7_direct_sales_indicator INTEGER DEFAULT 0, box8_substitute_payments TEXT, box9_crop_insurance_proceeds TEXT, box10_gross_proceeds_attorney TEXT, box11_fish_purchased_resale TEXT, box12_section_409a_deferrals TEXT, box14_gross_proceeds_attorney TEXT, box15_section_409a_income TEXT, box16_state_tax_withheld TEXT, box17_state TEXT, box18_state_income TEXT)
```

## table: f1099_nec_records
```sql
CREATE TABLE f1099_nec_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          payer_name TEXT,
          nonemployee_compensation TEXT,
          federal_income_tax_withheld TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        , payer_address TEXT, box1_nonemployee_compensation TEXT, box2_direct_sales_indicator INTEGER DEFAULT 0, box4_federal_income_tax_withheld TEXT, box5_state_tax_withheld TEXT, box6_state TEXT, box7_state_income TEXT)
```

## table: filetrack_status_history
```sql
CREATE TABLE filetrack_status_history (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id   INTEGER REFERENCES returns(id),
          log_number  TEXT NOT NULL,
          old_status  TEXT,
          new_status  TEXT NOT NULL,
          source      TEXT NOT NULL DEFAULT 'scanner',
          scanned_at  TEXT NOT NULL,
          recorded_at TEXT NOT NULL
        )
```

## table: import_batches
```sql
CREATE TABLE import_batches (
          id INTEGER PRIMARY KEY,
          filename TEXT NOT NULL,
          file_hash TEXT NOT NULL UNIQUE,
          imported_at TEXT NOT NULL,
          status TEXT NOT NULL,
          row_count INTEGER DEFAULT 0,
          success_count INTEGER DEFAULT 0,
          error_count INTEGER DEFAULT 0,
          review_count INTEGER DEFAULT 0,
          created_clients INTEGER DEFAULT 0,
          updated_clients INTEGER DEFAULT 0,
          created_returns INTEGER DEFAULT 0,
          updated_returns INTEGER DEFAULT 0,
          events_created INTEGER DEFAULT 0,
          notes_created INTEGER DEFAULT 0
        )
```

## table: import_rows
```sql
CREATE TABLE import_rows (
          id INTEGER PRIMARY KEY,
          batch_id INTEGER NOT NULL,
          row_number INTEGER NOT NULL,
          raw_json TEXT NOT NULL,
          action TEXT NOT NULL,
          error TEXT,
          FOREIGN KEY (batch_id) REFERENCES import_batches(id)
        )
```

## table: ledgerbridge_accounts
```sql
CREATE TABLE ledgerbridge_accounts (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id    INTEGER NOT NULL,
            display_name TEXT    NOT NULL,
            qb_account_name TEXT NOT NULL,
            institution  TEXT,
            last4        TEXT,
            active       INTEGER NOT NULL DEFAULT 1,
            created_at   TEXT    NOT NULL
        )
```

## table: ledgerbridge_audit_log
```sql
CREATE TABLE ledgerbridge_audit_log (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id    INTEGER,
            user_id     TEXT,
            action      TEXT    NOT NULL,
            detail_json TEXT,
            created_at  TEXT    NOT NULL
        )
```

## table: ledgerbridge_chart_of_accounts
```sql
CREATE TABLE ledgerbridge_chart_of_accounts (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id       INTEGER NOT NULL,
            qb_account_name TEXT    NOT NULL,
            account_type    TEXT    NOT NULL DEFAULT 'expense',
            active          INTEGER NOT NULL DEFAULT 1,
            created_at      TEXT    NOT NULL
        )
```

## table: ledgerbridge_clients
```sql
CREATE TABLE ledgerbridge_clients (
            id           INTEGER PRIMARY KEY,
            last_name    TEXT    NOT NULL,
            first_name   TEXT,
            display_name TEXT,
            created_at   TEXT    NOT NULL,
            created_by   TEXT
        )
```

## table: ledgerbridge_import_batches
```sql
CREATE TABLE ledgerbridge_import_batches (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id               INTEGER NOT NULL,
            account_id              INTEGER NOT NULL REFERENCES ledgerbridge_accounts(id),
            period_start            TEXT,
            period_end              TEXT,
            source_type             TEXT    NOT NULL DEFAULT 'csv',
            source_filename         TEXT,
            status                  TEXT    NOT NULL DEFAULT 'draft',
            stmt_beginning_balance  REAL,
            stmt_total_additions    REAL,
            stmt_total_subtractions REAL,
            stmt_ending_balance     REAL,
            created_by              TEXT    NOT NULL,
            created_at              TEXT    NOT NULL,
            exported_by             TEXT,
            exported_at             TEXT,
            reconcile_ok            INTEGER NOT NULL DEFAULT 0,
            reconcile_note          TEXT
        , iif_path TEXT, source_file_path TEXT, stmt_totals_source TEXT, totals_entered_by TEXT, totals_entered_at TEXT)
```

## table: ledgerbridge_merchant_memory
```sql
CREATE TABLE ledgerbridge_merchant_memory (
            id                  INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id           INTEGER NOT NULL,
            normalized_merchant TEXT    NOT NULL,
            qb_account_name     TEXT    NOT NULL,
            hit_count           INTEGER NOT NULL DEFAULT 1,
            last_used_at        TEXT    NOT NULL
        )
```

## table: ledgerbridge_transactions
```sql
CREATE TABLE ledgerbridge_transactions (
            id                      INTEGER PRIMARY KEY AUTOINCREMENT,
            batch_id                INTEGER NOT NULL REFERENCES ledgerbridge_import_batches(id),
            txn_date                TEXT    NOT NULL,
            raw_description         TEXT    NOT NULL,
            amount                  REAL    NOT NULL,
            check_number            TEXT,
            proposed_account        TEXT,
            approved_account        TEXT,
            memo                    TEXT,
            source                  TEXT    NOT NULL DEFAULT 'manual',
            confidence              REAL,
            claude_reasoning        TEXT,
            review_status           TEXT    NOT NULL DEFAULT 'pending',
            needs_payee_confirmation INTEGER NOT NULL DEFAULT 0,
            source_page             INTEGER,
            splits_json             TEXT
        , approved_by TEXT, approved_at TEXT, payee_ocr_raw TEXT)
```

## table: missing_docs
```sql
CREATE TABLE missing_docs (
          id          INTEGER PRIMARY KEY,
          return_id   INTEGER NOT NULL,
          item_text   TEXT    NOT NULL,
          is_resolved INTEGER DEFAULT 0,
          created_at  TEXT,
          resolved_at TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: notes
```sql
CREATE TABLE notes (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          note_text TEXT,
          source TEXT,
          created_at TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: notifications
```sql
CREATE TABLE notifications (
          id            INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id       INTEGER NOT NULL REFERENCES auth_users(id) ON DELETE CASCADE,
          title         TEXT NOT NULL,
          body          TEXT,
          link_url      TEXT,
          entity_type   TEXT,
          entity_id     INTEGER,
          is_read       INTEGER NOT NULL DEFAULT 0,
          created_at    TEXT NOT NULL
        )
```

## table: ocr_extraction_cache
```sql
CREATE TABLE ocr_extraction_cache (
          cache_key     TEXT PRIMARY KEY,
          image_sha256  TEXT NOT NULL,
          fields_json   TEXT,
          confidence    REAL,
          model         TEXT,
          created_at    TEXT NOT NULL
        )
```

## table: payments
```sql
CREATE TABLE payments (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          total_fee REAL,
          receipt_number TEXT,
          fee_paid REAL,
          cc_fee REAL,
          zelle_or_check_ref TEXT,
          cash_or_qpay_ref TEXT,
          refund_amount REAL,
          bank_deposit REAL, balance_due REAL, accounting_fee REAL, w7_fee REAL, form_1099_fee REAL, license_fee REAL, reprocess_fee REAL, discount_amount REAL, special_discount REAL, down_payment REAL, receipt2_number TEXT, payment_method TEXT, cancelled_fee REAL, check_number TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: receipt_queue
```sql
CREATE TABLE receipt_queue (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          return_document_id    INTEGER REFERENCES return_documents(id),
          image_path            TEXT NOT NULL,
          original_filename     TEXT,
          status                TEXT NOT NULL DEFAULT 'pending',
          ocr_raw               TEXT,
          vendor                TEXT,
          receipt_date          TEXT,
          total_amount          REAL,
          payment_method        TEXT,
          line_items            TEXT,
          category_candidates   TEXT,
          suggested_category    TEXT,
          suggested_account     TEXT,
          confidence            TEXT,
          approved_category     TEXT,
          approved_account      TEXT,
          reviewed_by           TEXT,
          reviewed_at           TEXT,
          review_notes          TEXT,
          exported_at           TEXT,
          export_file           TEXT,
          error_message         TEXT,
          attempts              INTEGER NOT NULL DEFAULT 0,
          created_at            TEXT NOT NULL,
          processed_at          TEXT
        )
```

## table: return_documents
```sql
CREATE TABLE return_documents (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id         INTEGER NOT NULL REFERENCES returns(id),
          filename          TEXT NOT NULL,
          original_filename TEXT,
          doc_type          TEXT,
          source            TEXT,
          file_path         TEXT NOT NULL,
          file_size_bytes   INTEGER,
          uploaded_by       TEXT,
          uploaded_at       TEXT,
          notes             TEXT,
          is_deleted        INTEGER NOT NULL DEFAULT 0
        , file_hash TEXT, match_confirmed INTEGER NOT NULL DEFAULT 1, match_score REAL, match_method TEXT, ocr_text_indexed INTEGER NOT NULL DEFAULT 0)
```

## table: return_documents_fts
```sql
CREATE VIRTUAL TABLE return_documents_fts USING fts5(
                doc_text,
                content='',
                tokenize='porter'
            )
```

## table: return_documents_fts_config
```sql
CREATE TABLE 'return_documents_fts_config'(k PRIMARY KEY, v) WITHOUT ROWID
```

## table: return_documents_fts_data
```sql
CREATE TABLE 'return_documents_fts_data'(id INTEGER PRIMARY KEY, block BLOB)
```

## table: return_documents_fts_docsize
```sql
CREATE TABLE 'return_documents_fts_docsize'(id INTEGER PRIMARY KEY, sz BLOB)
```

## table: return_documents_fts_idx
```sql
CREATE TABLE 'return_documents_fts_idx'(segid, term, pgno, PRIMARY KEY(segid, term)) WITHOUT ROWID
```

## table: return_forms
```sql
CREATE TABLE return_forms (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          form_1040 INTEGER,
          sched_a_d INTEGER,
          sched_c INTEGER,
          sched_e INTEGER,
          form_1120 INTEGER,
          form_1120s INTEGER,
          form_1065_llc INTEGER,
          corp_officer INTEGER,
          business_owner INTEGER,
          form_990_1041 INTEGER,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: returns
```sql
CREATE TABLE returns (
          id INTEGER PRIMARY KEY,
          client_id INTEGER NOT NULL,
          log_number TEXT,
          tax_year INTEGER,
          processor TEXT,
          verified INTEGER,
          client_status TEXT,
          intake_date TEXT,
          transfer_2025_flag INTEGER,
          transfer_2026_flag INTEGER,
          email_marker TEXT,
          date_emailed TEXT,
          pickup_date TEXT,
          logout_date TEXT,
          updated_date TEXT,
          is_amended INTEGER,
          has_w7 INTEGER,
          is_extension INTEGER,
          transfer_flag INTEGER,
          efile_date TEXT,
          ack_date TEXT,
          drake_status_raw TEXT,
          created_at TEXT,
          updated_at TEXT, filing_status TEXT, interview_by TEXT, promise_date TEXT, delivered_by TEXT, date_signatures_emailed TEXT, date_reports_emailed TEXT, overtime_flag INTEGER DEFAULT 0, insurance_type TEXT, digital_assets INTEGER DEFAULT 0, bank_name TEXT, bank_routing TEXT, bank_account TEXT, bank_account_type TEXT, notes_intake TEXT, estimate_irs REAL, estimate_state REAL, final_irs REAL, final_state REAL, signatures_given INTEGER DEFAULT 0, signatures_received INTEGER DEFAULT 0, contact_status TEXT, last_contacted_date TEXT, adjusted_gross_income REAL, cancelled_fee REAL, cancelled_reason TEXT, cancelled_at TEXT, signatures_given_method TEXT, signatures_received_method TEXT, extension_requested INTEGER NOT NULL DEFAULT 0, extension_filed_date TEXT, extension_ack_status TEXT, extension_ack_date TEXT, extension_due_date TEXT, filetrack_status TEXT, filetrack_status_updated_at TEXT, scan_deferred INTEGER NOT NULL DEFAULT 0,
          FOREIGN KEY (client_id) REFERENCES clients(id)
        )
```

## table: review_queue
```sql
CREATE TABLE review_queue (
          id          INTEGER PRIMARY KEY,
          batch_id    INTEGER,
          row_number  INTEGER,
          status      TEXT    DEFAULT 'pending',
          csv_last    TEXT,
          csv_first   TEXT,
          csv_log     TEXT,
          csv_year    INTEGER,
          proposed_client_id INTEGER,
          match_score INTEGER,
          match_method TEXT,
          raw_json    TEXT,
          resolved_client_id INTEGER,
          reason      TEXT,
          created_at  TEXT,
          resolved_at TEXT,
          FOREIGN KEY (proposed_client_id)  REFERENCES clients(id),
          FOREIGN KEY (resolved_client_id)  REFERENCES clients(id)
        )
```

## table: rule_suggestions
```sql
CREATE TABLE rule_suggestions (
          id               INTEGER PRIMARY KEY AUTOINCREMENT,
          domain           TEXT NOT NULL,
          suggested_rule   TEXT NOT NULL,
          confidence       TEXT NOT NULL,
          occurrence_count INTEGER NOT NULL DEFAULT 0,
          example_subjects TEXT,
          suggested_at     TEXT NOT NULL,
          suggested_by     TEXT NOT NULL DEFAULT 'llm',
          status           TEXT NOT NULL DEFAULT 'pending',
          reviewed_by      TEXT,
          reviewed_at      TEXT
        )
```

## table: spouses
```sql
CREATE TABLE spouses (
          id                   INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id            INTEGER NOT NULL REFERENCES clients(id),
          drake_spouse_id      TEXT,
          last_name            TEXT,
          first_name           TEXT NOT NULL,
          middle_initial       TEXT,
          date_of_birth        TEXT,
          derived_last_name    TEXT,
          source               TEXT DEFAULT 'TY2025 Drake import',
          match_confidence     REAL,
          needs_review         INTEGER NOT NULL DEFAULT 0,
          confirmed_at_intake  INTEGER NOT NULL DEFAULT 0,
          created_at           TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        , taxpayer_name TEXT, id_type INTEGER)
```

## table: sqlite_sequence
```sql
CREATE TABLE sqlite_sequence(name,seq)
```

## table: status_events
```sql
CREATE TABLE status_events (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          event_type TEXT,
          old_status TEXT,
          new_status TEXT,
          event_timestamp TEXT,
          source_file TEXT,
          note TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        )
```

## table: w2_records
```sql
CREATE TABLE w2_records (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id INTEGER NOT NULL REFERENCES returns(id),
          doc_id INTEGER REFERENCES return_documents(id),
          employer_name TEXT,
          wages_tips_other TEXT,
          federal_income_tax_withheld TEXT,
          state_wages TEXT,
          state_income_tax TEXT,
          tax_year TEXT,
          source TEXT NOT NULL DEFAULT 'extracted',
          created_at TEXT NOT NULL,
          updated_at TEXT,
          is_deleted INTEGER NOT NULL DEFAULT 0
        , employer_address TEXT, box1_wages_tips_other TEXT, box2_federal_income_tax_withheld TEXT, box3_social_security_wages TEXT, box4_social_security_tax_withheld TEXT, box5_medicare_wages_tips TEXT, box6_medicare_tax_withheld TEXT, box7_social_security_tips TEXT, box8_allocated_tips TEXT, box10_dependent_care_benefits TEXT, box11_nonqualified_plans TEXT, box12a_code TEXT, box12a_amount TEXT, box12b_code TEXT, box12b_amount TEXT, box12c_code TEXT, box12c_amount TEXT, box12d_code TEXT, box12d_amount TEXT, box13_statutory_employee INTEGER DEFAULT 0, box13_retirement_plan INTEGER DEFAULT 0, box13_third_party_sick_pay INTEGER DEFAULT 0, box14_other TEXT, box15_state TEXT, box16_state_wages TEXT, box17_state_income_tax TEXT, box18_local_wages TEXT, box19_local_income_tax TEXT, box20_locality_name TEXT, box15b_state TEXT, box16b_state_wages TEXT, box17b_state_income_tax TEXT, box18b_local_wages TEXT, box19b_local_income_tax TEXT, box20b_locality_name TEXT)
```

## table: watcher_heartbeat
```sql
CREATE TABLE watcher_heartbeat (
          id                        INTEGER PRIMARY KEY CHECK (id = 1),
          last_poll_at              TEXT,
          last_poll_outcome_counts  TEXT,
          last_error                TEXT
        )
```

## table: work_order_items
```sql
CREATE TABLE work_order_items (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          work_order_id   INTEGER NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
          description     TEXT NOT NULL,
          fee             NUMERIC NOT NULL DEFAULT 0,
          sort_order      INTEGER NOT NULL DEFAULT 0,
          is_quick_pick   INTEGER NOT NULL DEFAULT 0
        )
```

## table: work_order_quick_pick_components
```sql
CREATE TABLE work_order_quick_pick_components (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          quick_pick_id  INTEGER NOT NULL REFERENCES work_order_quick_picks(id) ON DELETE CASCADE,
          label          TEXT NOT NULL,
          fee            NUMERIC NOT NULL DEFAULT 0,
          sort_order     INTEGER NOT NULL DEFAULT 0
        )
```

## table: work_order_quick_picks
```sql
CREATE TABLE work_order_quick_picks (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          label          TEXT NOT NULL,
          default_fee    NUMERIC NOT NULL DEFAULT 0,
          active         INTEGER NOT NULL DEFAULT 1,
          sort_order     INTEGER NOT NULL DEFAULT 0
        )
```

## table: work_orders
```sql
CREATE TABLE work_orders (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          work_order_number     TEXT NOT NULL,
          client_id             INTEGER REFERENCES clients(id),
          client_name           TEXT NOT NULL,
          date_created          TEXT NOT NULL,
          due_by                TEXT,
          received_by_user_id   INTEGER REFERENCES auth_users(id),
          processed_by_user_id  INTEGER REFERENCES auth_users(id),
          created_by_user_id    INTEGER REFERENCES auth_users(id),
          status                TEXT NOT NULL DEFAULT 'open',
          total_fee             NUMERIC NOT NULL DEFAULT 0,
          created_at            TEXT NOT NULL,
          updated_at            TEXT NOT NULL
        , assigned_to_user_id INTEGER REFERENCES auth_users(id), assigned_by_user_id INTEGER REFERENCES auth_users(id), assigned_at TEXT)
```
