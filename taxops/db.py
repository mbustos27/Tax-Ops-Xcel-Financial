from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

from config import DB_PATH
from form_schema import CREATE_TABLE_FRAGMENTS_DOC7, get_form_alter_columns_by_table


def get_connection(db_path: Optional[str] = None) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path or DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


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


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}
