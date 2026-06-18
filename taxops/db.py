from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

from config import DB_PATH
from form_schema import CREATE_TABLE_FRAGMENTS_DOC7, get_form_alter_columns_by_table

# DEBT-6: increment this integer whenever a new migration block is added to
# _migrate_existing_tables.  The value is stored in app_settings and surfaced
# via /health so ops can confirm a deploy applied all migrations.
CURRENT_SCHEMA_VERSION = 5


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    # SEC-4: WAL mode allows concurrent readers alongside a single writer — critical
    # for the web process, audit writer, extractor worker, and mail watcher running
    # simultaneously.  WAL is sticky on the file after the first connection sets it;
    # subsequent connections still send the PRAGMA but it is a no-op.
    conn.execute("PRAGMA journal_mode=WAL;")
    # SEC-4: NORMAL is safe with WAL — it still fsync's the WAL checkpoint.
    conn.execute("PRAGMA synchronous=NORMAL;")
    # SEC-4: wait up to 10 s instead of raising OperationalError immediately when
    # the database is locked (e.g. audit writer holding a write transaction).
    conn.execute("PRAGMA busy_timeout=10000;")
    # SEC-4: keep temp tables and indices in memory — avoids temp-file I/O.
    conn.execute("PRAGMA temp_store=MEMORY;")
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def get_schema_version(conn: sqlite3.Connection) -> int:
    """DEBT-6: return the persisted schema version (0 if never set)."""
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = 'schema_version'"
        ).fetchone()
        return int(row["value"]) if row else 0
    except Exception:
        return 0


def set_schema_version(conn: sqlite3.Connection, version: int) -> None:
    """DEBT-6: upsert the schema_version in app_settings."""
    now_utc = __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime())
    conn.execute(
        """
        INSERT INTO app_settings (key, value, updated_at) VALUES ('schema_version', ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (str(version), now_utc),
    )


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS clients (
          id INTEGER PRIMARY KEY,
          last_name TEXT,
          first_name TEXT,
          display_name TEXT,
          ssn_last4 TEXT,
          referral_flag INTEGER,
          referred_by TEXT,
          created_at TEXT,
          updated_at TEXT
        );

        CREATE TABLE IF NOT EXISTS returns (
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
          contact_status TEXT,
          last_contacted_date TEXT,
          created_at TEXT,
          updated_at TEXT,
          FOREIGN KEY (client_id) REFERENCES clients(id)
        );

        CREATE TABLE IF NOT EXISTS return_forms (
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
        );

        CREATE TABLE IF NOT EXISTS payments (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          total_fee REAL,
          receipt_number TEXT,
          fee_paid REAL,
          cc_fee REAL,
          zelle_or_check_ref TEXT,
          cash_or_qpay_ref TEXT,
          refund_amount REAL,
          bank_deposit REAL,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        );

        CREATE TABLE IF NOT EXISTS notes (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          note_text TEXT,
          source TEXT,
          created_at TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        );

        CREATE TABLE IF NOT EXISTS missing_docs (
          id          INTEGER PRIMARY KEY,
          return_id   INTEGER NOT NULL,
          item_text   TEXT    NOT NULL,
          is_resolved INTEGER DEFAULT 0,
          created_at  TEXT,
          resolved_at TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        );

        CREATE TABLE IF NOT EXISTS status_events (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          event_type TEXT,
          old_status TEXT,
          new_status TEXT,
          event_timestamp TEXT,
          source_file TEXT,
          note TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        );

        CREATE TABLE IF NOT EXISTS import_batches (
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
        );

        CREATE TABLE IF NOT EXISTS import_rows (
          id INTEGER PRIMARY KEY,
          batch_id INTEGER NOT NULL,
          row_number INTEGER NOT NULL,
          raw_json TEXT NOT NULL,
          action TEXT NOT NULL,
          error TEXT,
          FOREIGN KEY (batch_id) REFERENCES import_batches(id)
        );

        CREATE TABLE IF NOT EXISTS review_queue (
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
        );

        CREATE TABLE IF NOT EXISTS dependents (
          id INTEGER PRIMARY KEY,
          return_id INTEGER NOT NULL,
          full_name TEXT,
          ssn_last4 TEXT,
          relationship TEXT,
          date_of_birth TEXT,
          medi_cal INTEGER DEFAULT 0,
          created_at TEXT,
          FOREIGN KEY (return_id) REFERENCES returns(id)
        );

        CREATE TABLE IF NOT EXISTS efile_batches (
          id               INTEGER PRIMARY KEY,
          transmission_date TEXT NOT NULL,
          notes            TEXT,
          transmitted_at   TEXT,
          status           TEXT NOT NULL DEFAULT 'open',
          created_at       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS efile_batch_items (
          id               INTEGER PRIMARY KEY,
          batch_id         INTEGER NOT NULL,
          return_id        INTEGER NOT NULL,
          log_number       TEXT,
          client_name      TEXT,
          ssn_last4        TEXT,
          tax_year         INTEGER,
          receipt_number   TEXT,
          fee_paid         REAL,
          cc_fee           REAL,
          pickup_date      TEXT,
          transmission_date TEXT,
          ack_status       TEXT NOT NULL DEFAULT 'pending',
          ack_date         TEXT,
          rejection_code   TEXT,
          rejection_reason TEXT,
          needs_calculation INTEGER NOT NULL DEFAULT 0,
          created_at       TEXT NOT NULL,
          FOREIGN KEY (batch_id)  REFERENCES efile_batches(id),
          FOREIGN KEY (return_id) REFERENCES returns(id),
          UNIQUE (batch_id, return_id)
        );

        CREATE TABLE IF NOT EXISTS return_documents (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id         INTEGER NOT NULL REFERENCES returns(id),
          filename          TEXT NOT NULL,
          original_filename TEXT,
          doc_type          TEXT,
          source            TEXT,
          file_path         TEXT NOT NULL,
          file_size_bytes   INTEGER,
          file_hash         TEXT,
          uploaded_by       TEXT,
          uploaded_at       TEXT,
          notes             TEXT,
          is_deleted        INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS email_classifications (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email    TEXT,
          sender_domain   TEXT,
          subject_snippet TEXT,
          classification  TEXT NOT NULL,
          confirmed_by    TEXT,
          confirmed_at    TEXT,
          created_at      TEXT NOT NULL,
          source          TEXT NOT NULL DEFAULT 'auto'
        );

        CREATE TABLE IF NOT EXISTS known_sender_rules (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          domain      TEXT NOT NULL UNIQUE,
          rule_type   TEXT NOT NULL DEFAULT 'always_promotional',
          note        TEXT,
          created_by  TEXT,
          created_at  TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS rule_suggestions (
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
        );

        CREATE TABLE IF NOT EXISTS domain_classifications (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          domain              TEXT NOT NULL UNIQUE,
          classification      TEXT NOT NULL,
          confidence_count    INTEGER NOT NULL DEFAULT 1,
          last_seen           TEXT NOT NULL,
          last_confirmed_by   TEXT,
          last_confirmed_at   TEXT,
          graduated           INTEGER NOT NULL DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS extraction_queue (
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
        );

        CREATE TABLE IF NOT EXISTS ai_chat_common_answers (
          cache_key             TEXT PRIMARY KEY,
          normalized_question   TEXT NOT NULL,
          season_year           INTEGER NOT NULL,
          answer                TEXT NOT NULL,
          tool_used             TEXT,
          payload_json          TEXT NOT NULL,
          created_at            TEXT NOT NULL,
          expires_at            TEXT NOT NULL,
          hit_count             INTEGER NOT NULL DEFAULT 0
        );

        -- AUDIT-1: unified app audit trail (JSON snapshots; user_id = login name until a users table exists)
        CREATE TABLE IF NOT EXISTS audit_log (
          id           INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id      TEXT,
          action       TEXT NOT NULL,
          entity_type  TEXT NOT NULL,
          entity_id    TEXT,
          before_json  TEXT,
          after_json   TEXT,
          ip_address   TEXT,
          created_at   TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS app_settings (
          key        TEXT PRIMARY KEY,
          value      TEXT NOT NULL,
          updated_at TEXT NOT NULL
        );

        -- ACCOUNTING-1: receipt OCR → QB categorization queue
        -- Statuses: pending → processing → review → approved → rejected → exported
        CREATE TABLE IF NOT EXISTS receipt_queue (
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
        );

        -- SEC-2: per-user accounts with hashed passwords.
        -- user_id TEXT in audit_log refers to username here.
        CREATE TABLE IF NOT EXISTS auth_users (
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
        );
        """
    )
    for _form_sql in CREATE_TABLE_FRAGMENTS_DOC7.values():
        conn.execute(_form_sql.strip())
    _migrate_existing_tables(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_returns_log_year ON returns(log_number, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_client_year ON returns(client_id, tax_year);
        CREATE INDEX IF NOT EXISTS idx_clients_name ON clients(last_name, first_name);
        CREATE INDEX IF NOT EXISTS idx_status_events_return ON status_events(return_id);
        CREATE INDEX IF NOT EXISTS idx_import_rows_batch   ON import_rows(batch_id);
        CREATE INDEX IF NOT EXISTS idx_dependents_return   ON dependents(return_id);
        CREATE INDEX IF NOT EXISTS idx_efile_items_batch   ON efile_batch_items(batch_id);
        CREATE INDEX IF NOT EXISTS idx_efile_items_return  ON efile_batch_items(return_id);
        CREATE INDEX IF NOT EXISTS idx_ai_chat_common_exp ON ai_chat_common_answers(expires_at);
        CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id);
        CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at);

        -- SEC-6: hot-path indexes confirmed missing via EXPLAIN QUERY PLAN
        CREATE INDEX IF NOT EXISTS idx_return_docs_return   ON return_documents(return_id);
        CREATE INDEX IF NOT EXISTS idx_return_docs_type     ON return_documents(return_id, doc_type);
        CREATE INDEX IF NOT EXISTS idx_payments_return      ON payments(return_id);
        CREATE INDEX IF NOT EXISTS idx_notes_return         ON notes(return_id);
        CREATE INDEX IF NOT EXISTS idx_missing_docs_return  ON missing_docs(return_id);
        CREATE INDEX IF NOT EXISTS idx_missing_docs_open    ON missing_docs(return_id, is_resolved);
        CREATE INDEX IF NOT EXISTS idx_extraction_status    ON extraction_queue(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_extraction_return    ON extraction_queue(return_id);
        -- DOC-HARD-2/3: fast lookup of permanently failed (dead-letter) items.
        CREATE INDEX IF NOT EXISTS idx_extraction_failed    ON extraction_queue(status, attempts);
        -- DOC-HARD-4: duplicate-detection lookup by content hash within a return.
        CREATE INDEX IF NOT EXISTS idx_return_docs_hash     ON return_documents(return_id, file_hash);
        -- ACCOUNTING-1: fast queue status scans.
        CREATE INDEX IF NOT EXISTS idx_receipt_queue_status ON receipt_queue(status, created_at);
        CREATE INDEX IF NOT EXISTS idx_receipt_queue_doc    ON receipt_queue(return_document_id);
        CREATE INDEX IF NOT EXISTS idx_email_class_email    ON email_classifications(sender_email);
        CREATE INDEX IF NOT EXISTS idx_email_class_domain   ON email_classifications(sender_domain);
        CREATE INDEX IF NOT EXISTS idx_audit_log_user       ON audit_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_returns_status_year  ON returns(client_status, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_proc_year    ON returns(processor, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_updated_at   ON returns(updated_at);
        CREATE INDEX IF NOT EXISTS idx_auth_users_username  ON auth_users(username);
        """
    )
    conn.commit()


def _migrate_existing_tables(conn: sqlite3.Connection) -> None:
    table_columns: Dict[str, List[str]] = {
        "clients": [
            "display_name TEXT",
            "ssn_last4 TEXT",
            "referral_flag INTEGER",
            "referred_by TEXT",
            "updated_at TEXT",
            # intake form fields
            "spouse_last_name TEXT",
            "spouse_first_name TEXT",
            "taxpayer_dob TEXT",
            "spouse_dob TEXT",
            "taxpayer_occupation TEXT",
            "spouse_occupation TEXT",
            "taxpayer_phone TEXT",
            "taxpayer_cell TEXT",
            "taxpayer_work_phone TEXT",
            "spouse_cell TEXT",
            "spouse_work_phone TEXT",
            "taxpayer_email TEXT",
            "spouse_email TEXT",
            "address TEXT",
            "is_new_client INTEGER DEFAULT 0",
            "prior_year_log TEXT",
        ],
        "returns": [
            "processor TEXT",
            "verified INTEGER",
            "client_status TEXT",
            "intake_date TEXT",
            "transfer_2025_flag INTEGER",
            "transfer_2026_flag INTEGER",
            "email_marker TEXT",
            "date_emailed TEXT",
            "pickup_date TEXT",
            "logout_date TEXT",
            "updated_date TEXT",
            "is_amended INTEGER",
            "has_w7 INTEGER",
            "is_extension INTEGER",
            "transfer_flag INTEGER",
            "efile_date TEXT",
            "ack_date TEXT",
            "drake_status_raw TEXT",
            "contact_status TEXT",
            "last_contacted_date TEXT",
            "created_at TEXT",
            # intake form fields
            "filing_status TEXT",
            "interview_by TEXT",
            "promise_date TEXT",
            "delivered_by TEXT",
            "date_signatures_emailed TEXT",
            "date_reports_emailed TEXT",
            "overtime_flag INTEGER DEFAULT 0",
            "insurance_type TEXT",
            "digital_assets INTEGER DEFAULT 0",
            "bank_name TEXT",
            "bank_routing TEXT",
            "bank_account TEXT",
            "bank_account_type TEXT",
            "notes_intake TEXT",
            "estimate_irs REAL",
            "estimate_state REAL",
            "final_irs REAL",
            "final_state REAL",
            # pickup workflow
            "signatures_given INTEGER DEFAULT 0",
            "signatures_received INTEGER DEFAULT 0",
            "adjusted_gross_income REAL",
        ],
        "payments": [
            "refund_amount REAL",
            "balance_due REAL",
            "bank_deposit REAL",
            # intake fee breakdown
            "accounting_fee REAL",
            "w7_fee REAL",
            "form_1099_fee REAL",
            "license_fee REAL",
            "reprocess_fee REAL",
            "discount_amount REAL",
            "special_discount REAL",
            "down_payment REAL",
            "receipt2_number TEXT",
            # pickup workflow
            "payment_method TEXT",
        ],
        "review_queue": [
            "batch_id INTEGER",
            "row_number INTEGER",
            "status TEXT DEFAULT 'pending'",
            "csv_last TEXT",
            "csv_first TEXT",
            "csv_log TEXT",
            "csv_year INTEGER",
            "proposed_client_id INTEGER",
            "match_score INTEGER",
            "match_method TEXT",
            "raw_json TEXT",
            "resolved_client_id INTEGER",
            "reason TEXT",
            "created_at TEXT",
            "resolved_at TEXT",
        ],
        "efile_batches": [
            "transmitted_at TEXT",
            "notes TEXT",
        ],
        "efile_batch_items": [
            "needs_calculation INTEGER NOT NULL DEFAULT 0",
            "cc_fee REAL",
        ],
        "email_classifications": [
            "reviewed_missed INTEGER NOT NULL DEFAULT 0",
            "email_routed_ok INTEGER NOT NULL DEFAULT 0",
            # EMAIL-6: fuzzy-match score (0–100) from name_matcher
            "match_score INTEGER",
            # EMAIL-7: matched client id; pending_review when score < ACCEPT_THRESHOLD
            "matched_client_id INTEGER",
            "match_status TEXT NOT NULL DEFAULT 'auto'",
        ],
        "import_batches": [
            "row_count INTEGER DEFAULT 0",
            "success_count INTEGER DEFAULT 0",
            "error_count INTEGER DEFAULT 0",
            "review_count INTEGER DEFAULT 0",
            "created_clients INTEGER DEFAULT 0",
            "updated_clients INTEGER DEFAULT 0",
            "created_returns INTEGER DEFAULT 0",
            "updated_returns INTEGER DEFAULT 0",
            "events_created INTEGER DEFAULT 0",
            "notes_created INTEGER DEFAULT 0",
        ],
        "return_documents": [
            "file_hash TEXT",
        ],
        "auth_users": [
            # ONBOARD-1: forces password change on first login / after admin reset
            "must_change_password INTEGER NOT NULL DEFAULT 0",
            # ONBOARD-4: orientation screen shown exactly once after first password change
            "has_seen_orientation INTEGER NOT NULL DEFAULT 0",
        ],
    }

    for table_name, columns in table_columns.items():
        existing = _table_columns(conn, table_name)
        for col_def in columns:
            col_name = col_def.split(" ", 1)[0]
            if col_name not in existing:
                try:
                    conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_def}")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise

    # Rename legacy email_sender_rules → known_sender_rules (DOC-3 epic name; one-time).
    _rule_tables = [
        r["name"]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name IN ('email_sender_rules', 'known_sender_rules')"
        ).fetchall()
    ]
    if "email_sender_rules" in _rule_tables and "known_sender_rules" not in _rule_tables:
        conn.execute("ALTER TABLE email_sender_rules RENAME TO known_sender_rules")

    # New-table migrations — safe to run on existing databases
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS return_documents (
          id                INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id         INTEGER NOT NULL REFERENCES returns(id),
          filename          TEXT NOT NULL,
          original_filename TEXT,
          doc_type          TEXT,
          source            TEXT,
          file_path         TEXT NOT NULL,
          file_size_bytes   INTEGER,
          file_hash         TEXT,
          uploaded_by       TEXT,
          uploaded_at       TEXT,
          notes             TEXT,
          is_deleted        INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS email_classifications (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          sender_email    TEXT,
          sender_domain   TEXT,
          subject_snippet TEXT,
          classification  TEXT NOT NULL,
          confirmed_by    TEXT,
          confirmed_at    TEXT,
          created_at      TEXT NOT NULL,
          source          TEXT NOT NULL DEFAULT 'auto'
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS known_sender_rules (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          domain      TEXT NOT NULL UNIQUE,
          rule_type   TEXT NOT NULL DEFAULT 'always_promotional',
          note        TEXT,
          created_by  TEXT,
          created_at  TEXT NOT NULL
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS rule_suggestions (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS domain_classifications (
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          domain              TEXT NOT NULL UNIQUE,
          classification      TEXT NOT NULL,
          confidence_count    INTEGER NOT NULL DEFAULT 1,
          last_seen           TEXT NOT NULL,
          last_confirmed_by   TEXT,
          last_confirmed_at   TEXT,
          graduated           INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS dashboard_saved_filters (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          user_id TEXT NOT NULL,
          name TEXT NOT NULL,
          filter_json TEXT NOT NULL,
          is_default INTEGER NOT NULL DEFAULT 0,
          is_shared INTEGER NOT NULL DEFAULT 0,
          created_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_dash_saved_user ON dashboard_saved_filters(user_id)"
    )
    conn.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_dash_saved_shared_default
        ON dashboard_saved_filters(user_id, is_default)
        """
    )
    for _form_sql in CREATE_TABLE_FRAGMENTS_DOC7.values():
        conn.execute(_form_sql.strip())
    alter_map = get_form_alter_columns_by_table()
    for tbl, col_defs in alter_map.items():
        existing_cols = _table_columns(conn, tbl)
        if not existing_cols:
            continue
        for col_def in col_defs:
            col_name = col_def.split(None, 1)[0]
            if col_name not in existing_cols:
                try:
                    conn.execute(f"ALTER TABLE {tbl} ADD COLUMN {col_def}")
                except sqlite3.OperationalError as exc:
                    if "duplicate column" not in str(exc).lower():
                        raise
                existing_cols.add(col_name)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS extraction_queue (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS audit_log (
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
        """
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS app_settings (
          key        TEXT PRIMARY KEY,
          value      TEXT NOT NULL,
          updated_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_log_entity ON audit_log(entity_type, entity_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_audit_log_created ON audit_log(created_at)"
    )
    # SEC-6: hot-path indexes — safe to add on existing databases (IF NOT EXISTS)
    for _idx_sql in (
        "CREATE INDEX IF NOT EXISTS idx_return_docs_return   ON return_documents(return_id)",
        "CREATE INDEX IF NOT EXISTS idx_return_docs_type     ON return_documents(return_id, doc_type)",
        "CREATE INDEX IF NOT EXISTS idx_payments_return      ON payments(return_id)",
        "CREATE INDEX IF NOT EXISTS idx_notes_return         ON notes(return_id)",
        "CREATE INDEX IF NOT EXISTS idx_missing_docs_return  ON missing_docs(return_id)",
        "CREATE INDEX IF NOT EXISTS idx_missing_docs_open    ON missing_docs(return_id, is_resolved)",
        "CREATE INDEX IF NOT EXISTS idx_extraction_status    ON extraction_queue(status, created_at)",
        "CREATE INDEX IF NOT EXISTS idx_extraction_return    ON extraction_queue(return_id)",
        "CREATE INDEX IF NOT EXISTS idx_email_class_email    ON email_classifications(sender_email)",
        "CREATE INDEX IF NOT EXISTS idx_email_class_domain   ON email_classifications(sender_domain)",
        "CREATE INDEX IF NOT EXISTS idx_audit_log_user       ON audit_log(user_id)",
        "CREATE INDEX IF NOT EXISTS idx_returns_status_year  ON returns(client_status, tax_year)",
        "CREATE INDEX IF NOT EXISTS idx_returns_proc_year    ON returns(processor, tax_year)",
        "CREATE INDEX IF NOT EXISTS idx_returns_updated_at   ON returns(updated_at)",
        "CREATE INDEX IF NOT EXISTS idx_auth_users_username  ON auth_users(username)",
    ):
        conn.execute(_idx_sql)
    # SEC-2: per-user accounts — safe to run on existing databases
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS auth_users (
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
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_auth_users_username ON auth_users(username)"
    )
    # ACCOUNTING-1: receipt OCR queue — new-table migration for existing databases.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS receipt_queue (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_receipt_queue_status ON receipt_queue(status, created_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_receipt_queue_doc ON receipt_queue(return_document_id)"
    )
    # DEBT-6: stamp the schema version so /health can confirm migrations ran.
    set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    conn.commit()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}
