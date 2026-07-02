from __future__ import annotations

import logging
import sqlite3
from typing import Dict, List, Optional

from config import DB_PATH
from form_schema import CREATE_TABLE_FRAGMENTS_DOC7, get_form_alter_columns_by_table

# DEBT-6: increment this integer whenever a new migration block is added to
# _migrate_existing_tables.  The value is stored in app_settings and surfaced
# via /health so ops can confirm a deploy applied all migrations.
CURRENT_SCHEMA_VERSION = 10

_log = logging.getLogger(__name__)

# EMAIL-INBOX-SCHEMA: single source of truth for the email_inbox table definition.
# Referenced by both init_db() and _migrate_existing_tables() to prevent drift.
_EMAIL_INBOX_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS email_inbox (
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
)
"""


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

        CREATE TABLE IF NOT EXISTS extension_batches (
          id               INTEGER PRIMARY KEY,
          filing_date      TEXT NOT NULL,
          notes            TEXT,
          transmitted_at   TEXT,
          status           TEXT NOT NULL DEFAULT 'open',
          created_at       TEXT NOT NULL
        );

        CREATE TABLE IF NOT EXISTS extension_batch_items (
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

        -- Staff sender allow/block rules. `domain` holds either a bare domain or a
        -- full address depending on `rule_scope` (added via migration below).
        CREATE TABLE IF NOT EXISTS email_sender_rules (
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

        -- Part 4: email processing log — tracks outcome and retry count per (uid, folder).
        -- Privacy rules: no email body, no full sender address, no SSN.
        CREATE TABLE IF NOT EXISTS email_processing_log (
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
          return_id        INTEGER REFERENCES returns(id),
          UNIQUE(message_uid, imap_folder)
        );

        -- EMAIL-INBOX: created via _EMAIL_INBOX_CREATE_SQL constant (see top of file).
        """
    )
    conn.execute(_EMAIL_INBOX_CREATE_SQL)
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


def _delete_return_children(conn: sqlite3.Connection, return_id: int) -> None:
    """Delete all child rows that reference returns.id so the return can be safely removed."""
    for table in (
        "notes", "status_events", "return_forms", "missing_docs",
        "dependents", "return_documents", "extraction_queue",
        "efile_batch_items", "review_queue",
    ):
        fk_col = "return_id"
        try:
            conn.execute(f"DELETE FROM {table} WHERE {fk_col} = ?", (return_id,))
        except sqlite3.OperationalError:
            pass
    # payments table uses return_id too
    conn.execute("DELETE FROM payments WHERE return_id = ?", (return_id,))


def _merge_client_fields(conn: sqlite3.Connection, kept_id: int, discard: Dict) -> None:
    """Copy any non-NULL field from discard into kept_id only when kept_id has NULL there."""
    skip = {"id", "created_at", "updated_at", "last_name", "first_name", "ssn_last4"}
    updates = {}
    kept = dict(conn.execute("SELECT * FROM clients WHERE id = ?", (kept_id,)).fetchone() or {})
    for col, val in discard.items():
        if col in skip or val is None:
            continue
        if kept.get(col) is None:
            updates[col] = val
    if updates:
        set_clause = ", ".join(f"{c} = ?" for c in updates)
        conn.execute(
            f"UPDATE clients SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
            list(updates.values()) + [kept_id],
        )


def _merge_return_fields(conn: sqlite3.Connection, kept_id: int, discard: Dict) -> None:
    """Fill NULL fields on kept return from discard return (import fills gaps only)."""
    skip = {"id", "client_id", "tax_year", "created_at", "updated_at"}
    updates = {}
    kept = dict(conn.execute("SELECT * FROM returns WHERE id = ?", (kept_id,)).fetchone() or {})
    for col, val in discard.items():
        if col in skip or val is None:
            continue
        if kept.get(col) is None:
            updates[col] = val
    if updates:
        set_clause = ", ".join(f"{c} = ?" for c in updates)
        conn.execute(
            f"UPDATE returns SET {set_clause}, updated_at = datetime('now') WHERE id = ?",
            list(updates.values()) + [kept_id],
        )


def _deduplicate_existing_records(conn: sqlite3.Connection) -> int:
    """
    One-time cleanup of duplicate client and return records created by the old
    importer.  Keeps the client with the most returns (ties resolved by highest
    id), reassigns or merges conflicting returns, then deletes the extras.

    Safe to run multiple times — exits cleanly when no duplicates exist.
    Returns the count of client rows removed.
    """
    removed = 0

    # Identify duplicate groups: same lower(last_name), lower(first_name), ssn_last4
    dupe_groups = conn.execute(
        """
        SELECT lower(last_name)                    AS ln,
               lower(COALESCE(first_name, ''))     AS fn,
               COALESCE(ssn_last4, '')             AS ssn,
               COUNT(*)                            AS cnt
        FROM clients
        GROUP BY lower(last_name),
                 lower(COALESCE(first_name, '')),
                 COALESCE(ssn_last4, '')
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    for grp in dupe_groups:
        # Rank members: most returns first, then highest id
        members = conn.execute(
            """
            SELECT c.id, COUNT(r.id) AS ret_count
            FROM clients c
            LEFT JOIN returns r ON r.client_id = c.id
            WHERE lower(c.last_name)                = ?
              AND lower(COALESCE(c.first_name, '')) = ?
              AND COALESCE(c.ssn_last4, '')         = ?
            GROUP BY c.id
            ORDER BY ret_count DESC, c.id DESC
            """,
            (grp["ln"], grp["fn"], grp["ssn"]),
        ).fetchall()

        if len(members) < 2:
            continue

        kept_id = members[0]["id"]
        kept_client = dict(
            conn.execute("SELECT * FROM clients WHERE id = ?", (kept_id,)).fetchone()
        )
        to_remove = [m["id"] for m in members[1:]]

        for discard_id in to_remove:
            discard_client = dict(
                conn.execute("SELECT * FROM clients WHERE id = ?", (discard_id,)).fetchone()
            )

            # Merge non-null contact fields from discarded client into kept
            _merge_client_fields(conn, kept_id, discard_client)

            # Reassign or merge returns
            for dr in conn.execute(
                "SELECT * FROM returns WHERE client_id = ?", (discard_id,)
            ).fetchall():
                dr_dict = dict(dr)
                conflict = conn.execute(
                    "SELECT * FROM returns WHERE client_id = ? AND tax_year = ?",
                    (kept_id, dr_dict["tax_year"]),
                ).fetchone()

                if conflict:
                    # Merge non-null fields into kept return, then remove duplicate
                    _merge_return_fields(conn, conflict["id"], dr_dict)
                    _delete_return_children(conn, dr_dict["id"])
                    conn.execute("DELETE FROM returns WHERE id = ?", (dr_dict["id"],))
                    _log.info(
                        "Deduplicated return: %s tax_year=%s — kept id=%s, removed id=%s",
                        kept_client.get("last_name"),
                        dr_dict["tax_year"],
                        conflict["id"],
                        dr_dict["id"],
                    )
                else:
                    conn.execute(
                        "UPDATE returns SET client_id = ? WHERE id = ?",
                        (kept_id, dr_dict["id"]),
                    )
                    _log.info(
                        "Reassigned return id=%s (tax_year=%s) from client %s → %s",
                        dr_dict["id"],
                        dr_dict["tax_year"],
                        discard_id,
                        kept_id,
                    )

            # Reassign review_queue references
            conn.execute(
                "UPDATE review_queue SET proposed_client_id = ? WHERE proposed_client_id = ?",
                (kept_id, discard_id),
            )
            conn.execute(
                "UPDATE review_queue SET resolved_client_id = ? WHERE resolved_client_id = ?",
                (kept_id, discard_id),
            )

            # Reassign all other tables that reference clients(id) via client_id.
            # For tables with a UNIQUE constraint on client_id (spouses,
            # client_spouse_import) delete the discard row when kept already has one.
            for tbl in ("client_dependents", "client_billing"):
                conn.execute(
                    f"UPDATE {tbl} SET client_id = ? WHERE client_id = ?",
                    (kept_id, discard_id),
                )
            for tbl in ("spouses", "client_spouse_import"):
                kept_has = conn.execute(
                    f"SELECT 1 FROM {tbl} WHERE client_id = ?", (kept_id,)
                ).fetchone()
                if kept_has:
                    conn.execute(
                        f"DELETE FROM {tbl} WHERE client_id = ?", (discard_id,)
                    )
                else:
                    conn.execute(
                        f"UPDATE {tbl} SET client_id = ? WHERE client_id = ?",
                        (kept_id, discard_id),
                    )

            conn.execute("DELETE FROM clients WHERE id = ?", (discard_id,))
            _log.info(
                "Deduplicated client: %s %s — kept id=%s, removed id=%s",
                kept_client.get("last_name"),
                kept_client.get("first_name") or "",
                kept_id,
                discard_id,
            )
            removed += 1

    if removed:
        _log.info("Deduplication complete: removed %d duplicate client rows.", removed)
    return removed


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
            # ID type: 1=SSN, 2=ITIN, NULL=unknown
            "id_type INTEGER",
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
            "signatures_given_method TEXT",
            "signatures_received_method TEXT",
            "adjusted_gross_income REAL",
            # LIFE-1: cancellation tracking
            "cancelled_fee REAL",
            "cancelled_reason TEXT",
            "cancelled_at TEXT",
            # Extension filing track (separate from is_extension which marks return type)
            "extension_requested INTEGER NOT NULL DEFAULT 0",
            "extension_filed_date TEXT",
            "extension_ack_status TEXT",
            "extension_ack_date TEXT",
            "extension_due_date TEXT",
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
            "check_number TEXT",
            # LIFE-1: store original fee before cancellation for reversal
            "cancelled_fee REAL",
        ],
        "dependents": [
            # DEP-1: soft-delete so removal from current return doesn't touch history
            "is_deleted INTEGER NOT NULL DEFAULT 0",
            # DEP-3: Medicare status
            "on_medicare INTEGER NOT NULL DEFAULT 0",
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
        "extension_batches": [
            "transmitted_at TEXT",
            "notes TEXT",
        ],
        "extension_batch_items": [
            "rejection_reason TEXT",
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
            # Email-match provenance — Fix 2: track whether a staff member has
            # confirmed this document is on the correct return.
            # DEFAULT 1 so all existing walk-in rows are treated as confirmed.
            "match_confirmed INTEGER NOT NULL DEFAULT 1",
            # Fuzzy-match confidence from name_matcher (0.0–1.0 scale).
            # NULL for walk-in uploads.
            "match_score REAL",
            # 'fuzzy', 'exact', 'manual', or NULL for walk-in uploads.
            "match_method TEXT",
        ],
        "auth_users": [
            # ONBOARD-1: forces password change on first login / after admin reset
            "must_change_password INTEGER NOT NULL DEFAULT 0",
            # ONBOARD-4: orientation screen shown exactly once after first password change
            "has_seen_orientation INTEGER NOT NULL DEFAULT 0",
        ],
        # ← end auth_users
        "email_sender_rules": [
            # Phase 2.1 rules-UI design: whether `domain` holds a bare domain or a
            # full address. All pre-existing rows are domain-level, hence the default.
            "rule_scope TEXT NOT NULL DEFAULT 'domain'",
            # 'allow' or 'block'. All pre-existing rows were always_promotional
            # suppression entries, hence the default.
            "action TEXT NOT NULL DEFAULT 'block'",
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

    # ONBOARD-4: one-time seed — users who already have a login history have used the app
    # before and should not see the orientation screen on their next visit after this deploy.
    # Idempotent: once has_seen_orientation is set to 1 the WHERE clause will not match again.
    conn.execute(
        "UPDATE auth_users SET has_seen_orientation = 1 "
        "WHERE has_seen_orientation = 0 AND last_login_at IS NOT NULL"
    )

    # Phase 0.6 (email-holding-area stabilization): email_sender_rules is the
    # canonical sender-rules table (reversing an earlier, never-completed plan
    # to rename it to known_sender_rules). Drop the orphaned empty duplicate
    # left behind by that abandoned migration — only if it's still empty, so
    # this is a no-op (not a data-loss risk) anywhere it isn't.
    _known_exists = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name='known_sender_rules'"
    ).fetchone()
    if _known_exists:
        _known_count = conn.execute("SELECT COUNT(*) AS c FROM known_sender_rules").fetchone()["c"]
        if _known_count == 0:
            conn.execute("DROP TABLE known_sender_rules")

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
        CREATE TABLE IF NOT EXISTS email_sender_rules (
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
    # Part 4: email processing log — new-table migration for existing databases.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS email_processing_log (
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
          return_id        INTEGER REFERENCES returns(id),
          UNIQUE(message_uid, imap_folder)
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_proc_log_outcome "
        "ON email_processing_log(outcome, last_attempt_at)"
    )
    # EMAIL-INBOX: new-table migration for existing databases (audit finding C2).
    conn.execute(_EMAIL_INBOX_CREATE_SQL)
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_email_inbox_unassigned "
        "ON email_inbox(is_assigned, is_deleted, received_at)"
    )

    # IMPORT-DEDUP: clean historical duplicates before adding the unique index.
    # Safe to run multiple times (idempotent).
    _deduplicate_existing_records(conn)

    # IMPORT-DEDUP: unique constraint — one active (non-cancelled) return per client per year.
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_returns_unique_client_year
        ON returns(client_id, tax_year)
        WHERE client_status != 'CANCELLED'
        """
    )

    # DEP-IMPORT: client-level dependents imported from Drake for TY2026 prefill
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_dependents (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id             INTEGER NOT NULL REFERENCES clients(id),
          drake_dependent_id    TEXT,
          taxpayer_name         TEXT,
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
        )
        """
    )
    # Add taxpayer_name column to existing deployments
    try:
        conn.execute("ALTER TABLE client_dependents ADD COLUMN taxpayer_name TEXT")
    except Exception:
        pass
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_client_deps_client ON client_dependents(client_id)"
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_client_deps_drake_dedup
        ON client_dependents(client_id, drake_dependent_id)
        WHERE drake_dependent_id IS NOT NULL
        """
    )

    # SPOUSE-IMPORT: staging table for Drake married-filer spouse data
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_spouse_import (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_spouse_import_client ON client_spouse_import(client_id)"
    )
    conn.execute(
        """
        CREATE UNIQUE INDEX IF NOT EXISTS idx_spouse_import_dedup
        ON client_spouse_import(client_id)
        """
    )

    # SPOUSES: client-level spouse table from Drake TY2025 Purple Sheet export
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS spouses (
          id                   INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id            INTEGER NOT NULL REFERENCES clients(id),
          drake_spouse_id      TEXT,
          taxpayer_name        TEXT,
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
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_spouses_one_per_client ON spouses(client_id)"
    )
    # Add taxpayer_name to existing deployments
    try:
        conn.execute("ALTER TABLE spouses ADD COLUMN taxpayer_name TEXT")
    except Exception:
        pass

    # ID type for spouses (1=SSN, 2=ITIN)
    try:
        conn.execute("ALTER TABLE spouses ADD COLUMN id_type INTEGER")
    except Exception:
        pass

    # Outstanding billing balance — point-in-time snapshots from Drake
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_billing (
            id             INTEGER PRIMARY KEY AUTOINCREMENT,
            client_id      INTEGER NOT NULL REFERENCES clients(id),
            balance_due    REAL,
            balance_as_of  DATE,
            source         TEXT DEFAULT 'TY2025 Drake import',
            created_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS idx_client_billing_snapshot "
        "ON client_billing(client_id, balance_as_of)"
    )

    # DEBT-6: stamp the schema version so /health can confirm migrations ran.
    set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    conn.commit()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}
