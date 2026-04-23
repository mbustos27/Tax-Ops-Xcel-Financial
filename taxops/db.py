from __future__ import annotations

import sqlite3
from typing import Dict, List, Optional

from config import DB_PATH


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
        """
    )
    _migrate_existing_tables(conn)
    conn.executescript(
        """
        CREATE INDEX IF NOT EXISTS idx_returns_log_year ON returns(log_number, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_client_year ON returns(client_id, tax_year);
        CREATE INDEX IF NOT EXISTS idx_clients_name ON clients(last_name, first_name);
        CREATE INDEX IF NOT EXISTS idx_status_events_return ON status_events(return_id);
        CREATE INDEX IF NOT EXISTS idx_import_rows_batch ON import_rows(batch_id);
        CREATE INDEX IF NOT EXISTS idx_dependents_return ON dependents(return_id);
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
        ],
        "payments": [
            "refund_amount REAL",
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
        ],
        "review_queue": [
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
    }

    for table_name, columns in table_columns.items():
        existing = _table_columns(conn, table_name)
        for col_def in columns:
            col_name = col_def.split(" ", 1)[0]
            if col_name not in existing:
                conn.execute(f"ALTER TABLE {table_name} ADD COLUMN {col_def}")


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}
