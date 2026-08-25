from __future__ import annotations

import logging
import sqlite3
from datetime import date
from typing import Dict, List, Optional

from config import DB_PATH
from form_schema import CREATE_TABLE_FRAGMENTS_DOC7, get_form_alter_columns_by_table

# DEBT-6: increment this integer whenever a new migration block is added to
# _migrate_existing_tables.  The value is stored in app_settings and surfaced
# via /health so ops can confirm a deploy applied all migrations.
CURRENT_SCHEMA_VERSION = 28

_log = logging.getLogger(__name__)

# EMAIL-INBOX-SCHEMA: single source of truth for the email_inbox table definition.
# Referenced by both init_db() and _migrate_existing_tables() to prevent drift.
_EMAIL_INBOX_CREATE_SQL = """
CREATE TABLE IF NOT EXISTS email_inbox (
  id                  INTEGER PRIMARY KEY AUTOINCREMENT,
  sender_email        TEXT,
  sender_domain       TEXT,
  sender_name         TEXT,
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
  is_deleted          INTEGER NOT NULL DEFAULT 0,
  -- Phase 3.1: non-binding suggested-match cache. Computed on-demand by
  -- /api/email-inbox/items (email_suggest.compute_suggestion), never by the
  -- IMAP poll cycle. Recomputed whenever suggested_return_id IS NULL.
  suggested_return_id INTEGER REFERENCES returns(id),
  suggestion_method   TEXT,
  suggestion_score    INTEGER
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


_ACTIVE_INTAKE_TAX_YEAR_KEY = "active_intake_tax_year"

# How many prior tax years staff may open at walk-in intake relative to the
# season's active year (active + this many prior). Keeps late/multi-year
# catch-up possible without an unbounded free-text year field.
INTAKE_PRIOR_TAX_YEARS = 5


def get_active_intake_tax_year(conn: sqlite3.Connection) -> int:
    """The default tax year for new walk-in intakes (season setting).

    Deliberately NOT derived from the current calendar date on every call —
    that's exactly what caused real intakes to silently land in an empty,
    out-of-sync tax_year bucket (see AUDIT_INTAKE.md follow-up, July 2026).
    Stored in app_settings and changed at season rollover
    (set_active_intake_tax_year / /admin/season-rollover). Staff may still
    pick a prior year per intake within allowed_intake_tax_years().
    Falls back to (this calendar year - 1) — the office's normal
    "extension season" convention — only if no admin has ever set it yet.
    """
    try:
        row = conn.execute(
            "SELECT value FROM app_settings WHERE key = ?",
            (_ACTIVE_INTAKE_TAX_YEAR_KEY,),
        ).fetchone()
        if row and row["value"]:
            return int(row["value"])
    except Exception:
        pass
    return date.today().year - 1


def allowed_intake_tax_years(conn: sqlite3.Connection) -> list[int]:
    """Tax years selectable on the intake form: active year down through N prior."""
    active = get_active_intake_tax_year(conn)
    return list(range(active, active - INTAKE_PRIOR_TAX_YEARS - 1, -1))


def resolve_intake_tax_year(conn: sqlite3.Connection, submitted) -> int:
    """Validate a staff-submitted tax year for intake; default to active if blank.

    Raises ValueError when the value is present but outside the allowed window
    (or not an integer). Never silently remaps a wrong year to active — that
    was the old per-intake dropdown bug mode.
    """
    active = get_active_intake_tax_year(conn)
    allowed = allowed_intake_tax_years(conn)
    if submitted is None:
        return active
    raw = str(submitted).strip()
    if not raw:
        return active
    try:
        year = int(raw)
    except ValueError as exc:
        raise ValueError(
            f"Invalid tax year {raw!r}. Choose a year from {allowed[-1]} to {allowed[0]}."
        ) from exc
    if year not in allowed:
        raise ValueError(
            f"Tax year {year} is outside the allowed range "
            f"({allowed[-1]}–{allowed[0]}). "
            f"Season default is {active}."
        )
    return year


def _iso_calendar_year(value) -> Optional[int]:
    raw = str(value or "").strip()
    if len(raw) >= 4 and raw[:4].isdigit():
        year = int(raw[:4])
        if 1990 <= year <= 2100:
            return year
    return None


def next_season_log_number(
    conn: sqlite3.Connection, *, intake_date: str | None = None
) -> str:
    """Next number in this season's log book (active TY), not per return tax year.

    Prior-year returns logged during this season (Oscar Rivera 2015–2019 as
    1264–1268, Nopaltitla 2022–2024 as 979–981) share the current book.
    MAX is taken across the active tax_year bucket and anything already
    intaken in the same calendar year so a TY2023 login cannot reuse the
    next 2025 sticker number.
    """
    active_ty = get_active_intake_tax_year(conn)
    cal = _iso_calendar_year(intake_date) or date.today().year
    row = conn.execute(
        """
        SELECT MAX(CAST(log_number AS INTEGER)) AS mx
        FROM returns
        WHERE tax_year = ?
           OR strftime('%Y', intake_date) = ?
        """,
        (active_ty, str(cal)),
    ).fetchone()
    return str((row["mx"] or 0) + 1)


def return_already_logged_this_season(
    existing_intake_date,
    this_intake_date,
    *,
    tax_year: int,
    active_tax_year: int,
) -> bool:
    """True when this client+TY is already on this season's work list.

    A prior calendar year's row for the same TY (e.g. Jarmi Lopez TY2023
    LOG #36 from 2024) is not this season — staff may re-log it onto the
    current book. Missing intake_date on the active year is treated as
    already logged (PENDING/PROCESSING shells for this season).
    """
    this_cal = _iso_calendar_year(this_intake_date) or date.today().year
    existing_cal = _iso_calendar_year(existing_intake_date)
    if existing_cal is not None:
        return existing_cal == this_cal
    return int(tax_year) == int(active_tax_year)


def set_active_intake_tax_year(conn: sqlite3.Connection, year: int) -> None:
    """Admin-only write path — see get_active_intake_tax_year() for why this
    is a stored setting rather than a per-request calculation."""
    now_utc = __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime())
    conn.execute(
        """
        INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = excluded.updated_at
        """,
        (_ACTIVE_INTAKE_TAX_YEAR_KEY, str(int(year)), now_utc),
    )


def find_duplicate_log_numbers(conn: sqlite3.Connection) -> List[sqlite3.Row]:
    """Read-only: (log_number, tax_year) pairs shared by more than one return.

    Used two ways: (1) as a startup guard before creating the
    ux_returns_log_year UNIQUE index — creating a UNIQUE index over dirty data
    would raise and could take down app startup, so callers must check this
    first and skip the index (not crash) if it's non-empty; (2) by ops tooling
    to surface duplicates for manual resolution. Never mutates anything.
    """
    return conn.execute(
        """
        SELECT log_number, tax_year, COUNT(*) AS cnt, GROUP_CONCAT(id) AS return_ids
        FROM returns
        WHERE log_number IS NOT NULL
        GROUP BY log_number, tax_year
        HAVING COUNT(*) > 1
        ORDER BY tax_year, CAST(log_number AS INTEGER)
        """
    ).fetchall()


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

        -- email_classifications: archived by Phase 2.2 (see _migrate_existing_tables,
        -- "Phase 2.2" block below) — the 6-layer classifier that wrote to it is gone.
        -- Not created for fresh installs; pre-existing rows live on as
        -- archive_email_classifications for anyone doing archaeology
        -- (see git tag pre-email-simplification).

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

        -- domain_classifications: archived by Phase 2.2 alongside email_classifications
        -- (domain-graduation code path confirmed fully dead — no reader or writer).

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
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          message_uid         TEXT NOT NULL,
          imap_folder         TEXT NOT NULL,
          sender_domain       TEXT,
          subject_snippet     TEXT,
          outcome             TEXT NOT NULL,
          attempt_count       INTEGER NOT NULL DEFAULT 1,
          last_attempt_at     TEXT NOT NULL,
          error_message       TEXT,
          doc_id              INTEGER REFERENCES return_documents(id),
          return_id           INTEGER REFERENCES returns(id),
          -- Phase 2.1: which suppression layer fired (sender_rule_block,
          -- known_promotional, drive_share) — NULL when not suppressed.
          suppression_reason  TEXT,
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
        -- RACE-1: idx_returns_log_year / ux_returns_log_year on (log_number,
        -- tax_year) are created (or upgraded to UNIQUE) conditionally inside
        -- _migrate_existing_tables, above — not here — since a plain
        -- CREATE INDEX would silently recreate the redundant non-unique
        -- index right after that logic drops it.
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
        CREATE INDEX IF NOT EXISTS idx_audit_log_user       ON audit_log(user_id);
        CREATE INDEX IF NOT EXISTS idx_returns_status_year  ON returns(client_status, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_proc_year    ON returns(processor, tax_year);
        CREATE INDEX IF NOT EXISTS idx_returns_updated_at   ON returns(updated_at);
        CREATE INDEX IF NOT EXISTS idx_auth_users_username  ON auth_users(username);
        """
    )
    conn.commit()


def _delete_return_children(conn: sqlite3.Connection, return_id: int) -> None:
    """Delete all child rows that reference returns.id so the return can be safely removed.

    Used by DEL-1's hard-delete endpoint (app.py: api_delete_return) and by
    client hard-delete (cascade). Every table below has a `return_id` FK —
    kept as one explicit list (rather than introspecting sqlite_master) so a
    future new return_id-bearing table is a deliberate addition here, not a
    silent gap. review_queue is NOT here on purpose: it references clients, not
    returns (proposed_client_id / resolved_client_id).
    """
    for table in (
        "notes", "status_events", "return_forms", "missing_docs",
        "dependents",
        # Typed extraction rows (FORMS) — before return_documents
        "w2_records", "f1099_nec_records", "f1099_misc_records",
        "f1099_int_records", "f1099_div_records",
        "extraction_queue",
        "efile_batch_items", "extension_batch_items",
        "filetrack_status_history", "payments",
    ):
        try:
            conn.execute(f"DELETE FROM {table} WHERE return_id = ?", (return_id,))
        except sqlite3.OperationalError:
            pass
    # receipt_queue keys off return_documents.id, not return_id
    try:
        conn.execute(
            """
            DELETE FROM receipt_queue
            WHERE return_document_id IN (
              SELECT id FROM return_documents WHERE return_id = ?
            )
            """,
            (return_id,),
        )
    except sqlite3.OperationalError:
        pass
    try:
        conn.execute("DELETE FROM return_documents WHERE return_id = ?", (return_id,))
    except sqlite3.OperationalError:
        pass
    # email_inbox / email_processing_log reference a return without owning it —
    # null the reference out rather than deleting email/audit history.
    for table, col in (
        ("email_inbox", "assigned_return_id"),
        ("email_inbox", "suggested_return_id"),
        ("email_processing_log", "return_id"),
    ):
        try:
            conn.execute(f"UPDATE {table} SET {col} = NULL WHERE {col} = ?", (return_id,))
        except sqlite3.OperationalError:
            pass


def _delete_client_cascade(conn: sqlite3.Connection, client_id: int) -> list[int]:
    """Hard-delete a client and all of their returns.

    Returns the list of deleted return ids (for audit). Caller owns the
    transaction. Work orders / billing requests keep their rows but lose the
    client_id link (optional FK) so office billing history is not wiped.
    """
    ret_ids = [
        int(r["id"])
        for r in conn.execute(
            "SELECT id FROM returns WHERE client_id = ? ORDER BY id",
            (client_id,),
        ).fetchall()
    ]
    for rid in ret_ids:
        _delete_return_children(conn, rid)
        conn.execute("DELETE FROM returns WHERE id = ?", (rid,))

    # Client-owned tables
    for table in (
        "client_dependents",
        "client_billing",
        "spouses",
        "client_spouse_import",
    ):
        try:
            conn.execute(f"DELETE FROM {table} WHERE client_id = ?", (client_id,))
        except sqlite3.OperationalError:
            pass

    # Soft references — keep history, drop the link
    for table, col in (
        ("review_queue", "proposed_client_id"),
        ("review_queue", "resolved_client_id"),
        ("work_orders", "client_id"),
        ("billing_requests", "client_id"),
    ):
        try:
            conn.execute(f"UPDATE {table} SET {col} = NULL WHERE {col} = ?", (client_id,))
        except sqlite3.OperationalError:
            pass

    conn.execute("DELETE FROM clients WHERE id = ?", (client_id,))
    return ret_ids


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
            # log_number is UNIQUE per tax_year — skip if another row (often the
            # still-living discard return) already holds this pair.
            if col == "log_number":
                clash = conn.execute(
                    """
                    SELECT id FROM returns
                     WHERE log_number = ? AND tax_year = ? AND id != ?
                     LIMIT 1
                    """,
                    (val, kept.get("tax_year"), kept_id),
                ).fetchone()
                if clash:
                    continue
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

    Pass 1: same lower(last_name), lower(first_name), ssn_last4 (incl. both blank).
    Pass 2: same exact name where at most one distinct non-empty ssn_last4 exists
    (typical Drake re-import: one row has SSN, the twin has NULL).

    Skips exact-name groups with conflicting non-empty SSN last4 values — those
    need staff review on /merge-clients.

    Safe to run multiple times — exits cleanly when no duplicates exist.
    Returns the count of client rows removed.
    """
    removed = 0

    def _merge_discard_into_kept(kept_id: int, discard_id: int) -> None:
        nonlocal removed
        kept_client = dict(
            conn.execute("SELECT * FROM clients WHERE id = ?", (kept_id,)).fetchone()
        )
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
                # Merge non-null fields into kept return, then remove duplicate.
                # Null discard log_number first so UNIQUE(log_number, tax_year)
                # does not block copying onto kept while discard still exists.
                conn.execute(
                    "UPDATE returns SET log_number = NULL WHERE id = ?",
                    (dr_dict["id"],),
                )
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

    # ── Pass 1: identical name + identical ssn_last4 (including both blank) ──
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
        for discard_id in [m["id"] for m in members[1:]]:
            _merge_discard_into_kept(kept_id, discard_id)

    # ── Pass 2: exact name, at most one distinct non-empty ssn_last4 ─────────
    name_groups = conn.execute(
        """
        SELECT lower(last_name)                AS ln,
               lower(COALESCE(first_name, '')) AS fn,
               COUNT(*)                        AS cnt
        FROM clients
        GROUP BY lower(last_name), lower(COALESCE(first_name, ''))
        HAVING COUNT(*) > 1
        """
    ).fetchall()

    for grp in name_groups:
        members = conn.execute(
            """
            SELECT c.id,
                   COALESCE(c.ssn_last4, '') AS ssn,
                   COUNT(r.id) AS ret_count
            FROM clients c
            LEFT JOIN returns r ON r.client_id = c.id
            WHERE lower(c.last_name)                = ?
              AND lower(COALESCE(c.first_name, '')) = ?
            GROUP BY c.id
            """,
            (grp["ln"], grp["fn"]),
        ).fetchall()
        if len(members) < 2:
            continue

        nonempty_ssns = {(m["ssn"] or "").strip() for m in members if (m["ssn"] or "").strip()}
        if len(nonempty_ssns) > 1:
            # Conflicting SSN last4 — leave for /merge-clients staff review
            continue

        # Prefer: has SSN, then most returns, then highest id
        ranked = sorted(
            members,
            key=lambda m: (
                1 if (m["ssn"] or "").strip() else 0,
                int(m["ret_count"] or 0),
                int(m["id"]),
            ),
            reverse=True,
        )
        kept_id = ranked[0]["id"]
        for discard in ranked[1:]:
            # Row may already have been removed if a prior group overlapped
            still = conn.execute(
                "SELECT 1 FROM clients WHERE id = ?", (discard["id"],)
            ).fetchone()
            if not still:
                continue
            _merge_discard_into_kept(kept_id, int(discard["id"]))

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
            # Schema v28 — R1 structured address (legacy `address` kept deprecated)
            "address_street TEXT",
            "address_city TEXT",
            "address_state TEXT",
            "address_zip TEXT",
            "address_county TEXT",
            "address_source TEXT",
            "address_verified_at TEXT",
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
            # Schema v28 — Drake export Filing Status (1–5); distinct from intake filing_status
            "filing_status_drake TEXT",
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
            # M3: filetrack (physical file barcode tracking) sticky status,
            # set by filetrack_service.apply_filetrack_status() from scanner
            # events — see filetrack_status_history for the full timeline.
            "filetrack_status TEXT",
            "filetrack_status_updated_at TEXT",
            # Intake scan: staff skipped "scan now" after intake — drives nav badge.
            "scan_deferred INTEGER NOT NULL DEFAULT 0",
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
            # Set to 1 after extracted_fields are flattened into return_documents_fts.
            "ocr_text_indexed INTEGER NOT NULL DEFAULT 0",
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
        "email_processing_log": [
            # Phase 2.1: records which suppression layer fired for skipped mail.
            "suppression_reason TEXT",
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

    # Phase 2.2 (email system revamp): archive the legacy 6-layer-classifier
    # tables. Confirmed dead — no live code path reads or writes either table
    # (db_tools.py's gather_season_dataplane still SELECTs email_classifications
    # for the since-deleted AI chat feature, but that module has no live caller
    # and its _qf() helper degrades to [] on a missing table, so this rename is
    # safe). RENAME, not DROP, so the historical rows (email_classifications had
    # ~175, domain_classifications ~50 as of the Phase 1 audit) stay reachable
    # for archaeology alongside the `pre-email-simplification` git tag.
    for _legacy_table, _archive_table in (
        ("email_classifications", "archive_email_classifications"),
        ("domain_classifications", "archive_domain_classifications"),
    ):
        _legacy_exists = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
            (_legacy_table,),
        ).fetchone()
        if _legacy_exists:
            conn.execute(f"ALTER TABLE {_legacy_table} RENAME TO {_archive_table}")

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
    # email_classifications: archived by Phase 2.2 — see the rename block below.
    # Not (re-)created here so a legacy install that has already been migrated
    # stays migrated across restarts.
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
    # domain_classifications: archived by Phase 2.2 alongside email_classifications.
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
          id                  INTEGER PRIMARY KEY AUTOINCREMENT,
          message_uid         TEXT NOT NULL,
          imap_folder         TEXT NOT NULL,
          sender_domain       TEXT,
          subject_snippet     TEXT,
          outcome             TEXT NOT NULL,
          attempt_count       INTEGER NOT NULL DEFAULT 1,
          last_attempt_at     TEXT NOT NULL,
          error_message       TEXT,
          doc_id              INTEGER REFERENCES return_documents(id),
          return_id           INTEGER REFERENCES returns(id),
          suppression_reason  TEXT,
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
    # Phase 3.1: add sender_name/suggestion columns to email_inbox tables that
    # predate this migration. Must run after _EMAIL_INBOX_CREATE_SQL above so
    # the table is guaranteed to exist first (table_columns loop earlier in
    # this function cannot be used for a table that may not exist yet).
    _email_inbox_existing = _table_columns(conn, "email_inbox")
    for col_def in (
        "sender_name TEXT",
        "suggested_return_id INTEGER REFERENCES returns(id)",
        "suggestion_method TEXT",
        "suggestion_score INTEGER",
    ):
        col_name = col_def.split(" ", 1)[0]
        if col_name not in _email_inbox_existing:
            try:
                conn.execute(f"ALTER TABLE email_inbox ADD COLUMN {col_def}")
            except sqlite3.OperationalError as exc:
                if "duplicate column" not in str(exc).lower():
                    raise

    # Phase 3.3: single-row heartbeat table upserted by mail_watcher at the end
    # of every poll cycle (_upsert_watcher_heartbeat). The CHECK(id = 1)
    # constraint enforces exactly one row; the health panel (Admin only)
    # reads it, it never reads watcher internals directly.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS watcher_heartbeat (
          id                        INTEGER PRIMARY KEY CHECK (id = 1),
          last_poll_at              TEXT,
          last_poll_outcome_counts  TEXT,
          last_error                TEXT
        )
        """
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

    # M3: filetrack status history — every scanner-confirmed status event,
    # mirroring the status_events pattern for returns.client_status. Kept
    # even when log_number doesn't (yet) resolve to a return (return_id NULL)
    # so a mis-scanned/premature scan is still visible for triage instead of
    # silently dropped — see filetrack_service.apply_filetrack_status().
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS filetrack_status_history (
          id          INTEGER PRIMARY KEY AUTOINCREMENT,
          return_id   INTEGER REFERENCES returns(id),
          log_number  TEXT NOT NULL,
          old_status  TEXT,
          new_status  TEXT NOT NULL,
          source      TEXT NOT NULL DEFAULT 'scanner',
          scanned_at  TEXT NOT NULL,
          recorded_at TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_filetrack_history_log "
        "ON filetrack_status_history(log_number, scanned_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_filetrack_history_return "
        "ON filetrack_status_history(return_id, scanned_at)"
    )

    # RACE-1: hard safety net for the intake log-number race (see
    # AUDIT_INTAKE.md, Link 3). The route now serializes assignment with
    # BEGIN IMMEDIATE, but this UNIQUE index turns any future duplicate into
    # a loud sqlite3.IntegrityError instead of a silent duplicate label.
    # A UNIQUE index over dirty data raises immediately, which would crash
    # app startup — so we check first and skip (never auto-fix) if any
    # duplicate (log_number, tax_year) pairs already exist. Idempotent and
    # self-healing: once the underlying duplicates are resolved by hand, the
    # very next startup creates the index automatically.
    if find_duplicate_log_numbers(conn):
        _log.warning(
            "RACE-1: duplicate (log_number, tax_year) pairs exist in returns; "
            "skipping ux_returns_log_year unique index until resolved manually "
            "(run scripts/check_dupe_log_numbers.py for details). The older, "
            "non-unique idx_returns_log_year index remains in place."
        )
    else:
        # The UNIQUE index also serves every lookup idx_returns_log_year did
        # (same columns, same order), so drop the now-redundant plain index
        # rather than maintaining two indexes over identical columns.
        conn.execute("DROP INDEX IF EXISTS idx_returns_log_year")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS ux_returns_log_year "
            "ON returns(log_number, tax_year)"
        )

    # WO-1: Work Order Creator — form-based module for staff to create/print
    # physical Work Orders (separate concept from returns/log_number tracking;
    # see routes/work_orders.py). client_id is an *optional* link to an
    # existing client (for search/reporting) — client_name is always captured
    # directly, mirroring extension_batch_items' client_name snapshot pattern,
    # since a Work Order can be written for someone who isn't an intake
    # client yet. received_by_user_id / processed_by_user_id reference
    # auth_users (the real RBAC staff table) rather than any legacy paper-form
    # staff-initials list — see preparer.py, which this deliberately does NOT
    # reuse per the build spec.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS work_orders (
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
          updated_at            TEXT NOT NULL,
          assigned_to_user_id   INTEGER REFERENCES auth_users(id),
          assigned_by_user_id   INTEGER REFERENCES auth_users(id),
          assigned_at           TEXT
        )
        """
    )
    # WO-7: for DBs created before "assign to an employee" existed, the CREATE
    # TABLE above is a no-op (IF NOT EXISTS), so these columns need adding the
    # same way every other post-hoc column does (see table_columns dict above)
    # — just done here, right after the table's own CREATE, rather than in
    # that earlier dict, since that loop runs BEFORE this CREATE TABLE and
    # would fail with "no such table" on a brand-new DB that hasn't reached
    # this line yet.
    _wo_existing_cols = _table_columns(conn, "work_orders")
    for _col_def in (
        "assigned_to_user_id INTEGER REFERENCES auth_users(id)",
        "assigned_by_user_id INTEGER REFERENCES auth_users(id)",
        "assigned_at TEXT",
    ):
        _col_name = _col_def.split(" ", 1)[0]
        if _col_name not in _wo_existing_cols:
            try:
                conn.execute(f"ALTER TABLE work_orders ADD COLUMN {_col_def}")
            except sqlite3.OperationalError as exc:
                if "duplicate column" not in str(exc).lower():
                    raise
    # RACE-1-style safety net (same pattern as ux_returns_log_year): a UNIQUE
    # index is the hard backstop behind the BEGIN IMMEDIATE + retry numbering
    # in routes/work_orders.py — see _next_work_order_number().
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_work_orders_number "
        "ON work_orders(work_order_number)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_orders_status_date "
        "ON work_orders(status, date_created)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_orders_client ON work_orders(client_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_orders_assigned ON work_orders(assigned_to_user_id)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS work_order_items (
          id              INTEGER PRIMARY KEY AUTOINCREMENT,
          work_order_id   INTEGER NOT NULL REFERENCES work_orders(id) ON DELETE CASCADE,
          description     TEXT NOT NULL,
          fee             NUMERIC NOT NULL DEFAULT 0,
          sort_order      INTEGER NOT NULL DEFAULT 0,
          is_quick_pick   INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_work_order_items_wo "
        "ON work_order_items(work_order_id, sort_order)"
    )

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS work_order_quick_picks (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          label          TEXT NOT NULL,
          default_fee    NUMERIC NOT NULL DEFAULT 0,
          active         INTEGER NOT NULL DEFAULT 1,
          sort_order     INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    # Seed once, idempotently, from the reference paper form — only when the
    # table is completely empty, so this never fights with admin edits or
    # deactivations made after the initial seed on a later startup.
    if conn.execute("SELECT COUNT(*) c FROM work_order_quick_picks").fetchone()["c"] == 0:
        conn.executemany(
            "INSERT INTO work_order_quick_picks (label, default_fee, active, sort_order) "
            "VALUES (?, ?, 1, ?)",
            [
                ("Consultation", 175.00, 0),
                ("Corporate Book", 475.00, 1),
                (
                    "S Corporation Election Form 2553 with Explanation for Late Filing",
                    750.00,
                    2,
                ),
            ],
        )

    # WO-6: a quick pick can carry one-or-more *extra* fee components beyond
    # its own base label/default_fee — e.g. "Statement of Information" ($125)
    # plus a separate "Filing Fee" ($25) line, so staff never have to hand-type
    # a second row for a compound-fee service. Selecting the quick pick in the
    # form (see routes/work_orders.py + work_order_form.html) adds one line
    # item per component IN ADDITION TO the base line. This is intentionally
    # its own table (not a JSON column) so quantity is unbounded and each
    # component gets its own row for editing/deleting in the admin UI.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS work_order_quick_pick_components (
          id             INTEGER PRIMARY KEY AUTOINCREMENT,
          quick_pick_id  INTEGER NOT NULL REFERENCES work_order_quick_picks(id) ON DELETE CASCADE,
          label          TEXT NOT NULL,
          fee            NUMERIC NOT NULL DEFAULT 0,
          sort_order     INTEGER NOT NULL DEFAULT 0
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_wo_qp_components_pick "
        "ON work_order_quick_pick_components(quick_pick_id, sort_order)"
    )
    # Seed the one compound example given at spec time (Statement of
    # Information: $125 base + $25 filing fee) — matched by label so this
    # never re-inserts a duplicate if it's already been added (by this seed
    # or by an admin) and never touches admin-entered quick picks otherwise.
    if conn.execute(
        "SELECT COUNT(*) c FROM work_order_quick_picks WHERE label=?",
        ("Statement of Information",),
    ).fetchone()["c"] == 0:
        next_sort = conn.execute(
            "SELECT COALESCE(MAX(sort_order), -1) + 1 AS n FROM work_order_quick_picks"
        ).fetchone()["n"]
        cur = conn.execute(
            "INSERT INTO work_order_quick_picks (label, default_fee, active, sort_order) "
            "VALUES (?, ?, 1, ?)",
            ("Statement of Information", 125.00, next_sort),
        )
        conn.execute(
            "INSERT INTO work_order_quick_pick_components (quick_pick_id, label, fee, sort_order) "
            "VALUES (?, ?, ?, 0)",
            (cur.lastrowid, "Filing Fee", 25.00),
        )

    # WO-7: generic per-user in-app notifications — first consumer is "you've
    # been assigned a Work Order" (see routes/work_orders.py's _notify_user),
    # but intentionally NOT work-order-specific (entity_type/entity_id +
    # link_url are generic) so any future feature needing "notify this one
    # user" can reuse this table instead of growing its own. There is no
    # outbound-email capability in TaxOps today (mail_watcher.py is inbound
    # IMAP only) — this is an in-app bell/badge, not an email.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS notifications (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_notifications_user_unread "
        "ON notifications(user_id, is_read, created_at)"
    )

    # WO-2: every work order gets an attached billing request carrying the
    # fee, so accounting/front-desk has something to act on ("bill this
    # client for $X") beyond the work order itself. One-to-one with
    # work_orders (ux_billing_requests_wo) — see routes/work_orders.py,
    # which creates this row automatically on work order create and keeps
    # `amount` synced with total_fee while status is still 'pending'.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS billing_requests (
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
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_billing_requests_wo "
        "ON billing_requests(work_order_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_billing_requests_status "
        "ON billing_requests(status, created_at)"
    )
    # Backfill: any work order created before this table existed still needs
    # its one attached billing request, snapshotting its fee as of right now.
    from utils import now as _now
    _backfill_ts = _now()
    conn.execute(
        """
        INSERT INTO billing_requests
            (work_order_id, client_id, client_name, amount, status, created_by_user_id, created_at, updated_at)
        SELECT wo.id, wo.client_id, wo.client_name, wo.total_fee, 'pending', wo.created_by_user_id, ?, ?
        FROM work_orders wo
        LEFT JOIN billing_requests br ON br.work_order_id = wo.id
        WHERE br.id IS NULL
        """,
        (_backfill_ts, _backfill_ts),
    )

    # COMPLIANCE-0: Compliance Tracker module — replaces the hand-maintained
    # ACCOUNTING_LOG_2026.xlsx workbook (CDTFA sales/use tax filings, city
    # business license renewals, monthly SBE/CDTFA prepayment deposits, and
    # misc individual filings). Deliberately named compliance_clients (NOT
    # clients) — this is a distinct business-entity roster (CDTFA/city
    # license accounts, keyed by name/corp_number/FEIN) from the existing
    # `clients` table (tax-return intake, keyed by last/first name + SSN),
    # and the two may or may not overlap for a given real-world person/
    # business. See compliance/crypto.py for the credential encryption used
    # by compliance_credentials.encrypted_password — NEVER plaintext.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_clients (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_clients_name "
        "ON compliance_clients(name)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_clients_active "
        "ON compliance_clients(active)"
    )

    # compliance_credentials — CDTFA/city portal logins. encrypted_password
    # is Fernet ciphertext (BLOB), never plaintext (security requirement #1).
    # shared_login flags logins like the workbook's shared "xcelfin92" so
    # the UI can surface a "shared across N accounts" warning before anyone
    # resets it (security requirement #4). needs_rotation is set by the
    # one-time xlsx import for any credential that was found in plaintext
    # in the spreadsheet, since that value must be treated as already
    # compromised (security requirement — M0 acceptance criteria).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_credentials (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_credentials_shared "
        "ON compliance_credentials(shared_login)"
    )

    # compliance_accounts — one persistent row per (client, filing
    # obligation), e.g. "Acme Corp CDTFA sales tax account" or "Acme Corp
    # City of X business license". filing_periods below hang off this, one
    # row per quarter/month/year as applicable to `frequency`.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_accounts (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_accounts_client "
        "ON compliance_accounts(compliance_client_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_accounts_type "
        "ON compliance_accounts(account_type, active)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_accounts_credential "
        "ON compliance_accounts(credential_id)"
    )

    # compliance_filing_periods — the Kanban card. One row per
    # quarter/month/year per account. `status` replaces the ad hoc column
    # flags from the old workbook (Sales In / CTFA-SBE DONE / Need report /
    # TP Files / DONE) with a single enum + timestamps (M2 spec).
    # period_label is a human/sort key, e.g. "2026-Q1", "2026-04", "2026".
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_filing_periods (
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
        """
    )
    conn.execute(
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_compliance_filing_periods_account_label "
        "ON compliance_filing_periods(compliance_account_id, period_label)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_filing_periods_status "
        "ON compliance_filing_periods(status)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_filing_periods_due "
        "ON compliance_filing_periods(period_due_date)"
    )

    # compliance_correspondence_log — replaces the NOTES sheet grid
    # (client x month). password_correspondence / missing_password note
    # types let the M5 reporting view surface "clients we're missing portal
    # passwords for" without a separate table.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS compliance_correspondence_log (
          id                      INTEGER PRIMARY KEY AUTOINCREMENT,
          compliance_client_id    INTEGER NOT NULL REFERENCES compliance_clients(id) ON DELETE CASCADE,
          month                    TEXT NOT NULL,
          note_type               TEXT NOT NULL DEFAULT 'general',
          note                     TEXT,
          created_by_user_id      INTEGER REFERENCES auth_users(id),
          created_at               TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_compliance_correspondence_client_month "
        "ON compliance_correspondence_log(compliance_client_id, month)"
    )

    # Schema v22 — FTS5 index for scanned/extracted document text.
    # content='' external-content style: we manage rows explicitly from extractor.
    # Do not fail startup if this Python/SQLite build lacks FTS5 — search degrades
    # to metadata until FTS is available on the TaxOpsService venv.
    try:
        conn.execute(
            """
            CREATE VIRTUAL TABLE IF NOT EXISTS return_documents_fts USING fts5(
                doc_text,
                content='',
                tokenize='porter'
            )
            """
        )
    except sqlite3.OperationalError as exc:
        _log.warning(
            "Schema v22: return_documents_fts not created (FTS5 unavailable?): %s",
            exc,
        )

    # Schema v23 — Claude OCR verdict cache (sha256 of page image + prompt).
    # PunchBridge-style: duplicate re-scans must not re-pay for API calls.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS ocr_extraction_cache (
          cache_key     TEXT PRIMARY KEY,
          image_sha256  TEXT NOT NULL,
          fields_json   TEXT,
          confidence    REAL,
          model         TEXT,
          created_at    TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_ocr_extraction_cache_image "
        "ON ocr_extraction_cache(image_sha256)"
    )

    # Schema v24 — TY2024+ Drake CSM↔purple prefill links + JSON form counts.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS drake_prefill_links (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drake_prefill_links_client "
        "ON drake_prefill_links(client_id)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drake_prefill_links_status "
        "ON drake_prefill_links(tax_year, prefill_status, disposition_status)"
    )
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS drake_form_prefill (
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
        """
    )

    # Schema v24 extension — spouse/dependent contact payload keyed by link_id.
    # Separate from spouses/client_dependents (those have their own review flow).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS drake_household_prefill (
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
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_drake_household_prefill_year "
        "ON drake_household_prefill(tax_year)"
    )

    # Schema v25 — Wave 2A merge trail. Written in the same transaction as
    # merge_ops.merge_client_into before DELETE clients(discard). Reconstructs
    # discarded identity after merge (audit_log alone only has keep/discard ids).
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_merge_history (
          id                      INTEGER PRIMARY KEY AUTOINCREMENT,
          keep_id                 INTEGER NOT NULL,
          discard_id              INTEGER NOT NULL,
          operator                TEXT,
          reason_code             TEXT,
          note                    TEXT,
          merged_at               TEXT NOT NULL,
          discard_client_json     TEXT NOT NULL,
          discard_returns_json    TEXT NOT NULL DEFAULT '[]',
          returns_actions_json    TEXT NOT NULL DEFAULT '[]',
          status_events_json      TEXT NOT NULL DEFAULT '[]',
          filetrack_history_json  TEXT NOT NULL DEFAULT '[]'
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_client_merge_history_keep "
        "ON client_merge_history(keep_id, merged_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_client_merge_history_discard "
        "ON client_merge_history(discard_id, merged_at)"
    )

    # Schema v26 — Wave 4: fold clients.spouse_* names into spouses (canonical).
    # Does not overwrite existing spouses rows. Dead clients.spouse_dob/cell/…
    # columns left in place (SQLite drop = rebuild; deferred).
    folded = conn.execute(
        """
        INSERT INTO spouses (
          client_id, first_name, last_name, date_of_birth, source, created_at
        )
        SELECT
          c.id,
          COALESCE(NULLIF(TRIM(c.spouse_first_name), ''), 'UNKNOWN'),
          NULLIF(TRIM(c.spouse_last_name), ''),
          NULLIF(TRIM(c.spouse_dob), ''),
          'wave4_clients_fold',
          datetime('now')
        FROM clients c
        WHERE (
            (c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name) != '')
            OR (c.spouse_first_name IS NOT NULL AND TRIM(c.spouse_first_name) != '')
          )
          AND NOT EXISTS (SELECT 1 FROM spouses s WHERE s.client_id = c.id)
        """
    ).rowcount
    if folded:
        _log.info("Wave 4 spouse fold: inserted %s spouses rows from clients.*", folded)

    # Schema v27 — flag non-production / scanning-test clients (proposed_migration.sql).
    # Exclude from audits/counts; do not delete (merge/filetrack side effects).
    if "is_test" not in _table_columns(conn, "clients"):
        try:
            conn.execute(
                "ALTER TABLE clients ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0"
            )
        except sqlite3.OperationalError as exc:
            if "duplicate column" not in str(exc).lower():
                raise
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_clients_is_test ON clients(is_test)"
    )

    # Schema v28 — R1 client profile backfill prerequisites (audit R1-status.md).
    # Address structured cols + Drake FS mirror are also listed in table_columns
    # above (idempotent ALTER). History table mirrors client_merge_history:
    # same-transaction snapshot for every COALESCE write (Phase 3).
    # No data backfill here — schema only.
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS client_profile_backfill_history (
          id                    INTEGER PRIMARY KEY AUTOINCREMENT,
          client_id             INTEGER NOT NULL,
          run_label             TEXT NOT NULL,
          applied_at            TEXT NOT NULL,
          bare_log_number       INTEGER NOT NULL,
          invoice_number        TEXT NOT NULL,
          source_export_sha256  TEXT,
          fields_written_json   TEXT NOT NULL,
          before_json           TEXT NOT NULL,
          after_json            TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cpbh_client "
        "ON client_profile_backfill_history(client_id, applied_at)"
    )
    conn.execute(
        "CREATE INDEX IF NOT EXISTS idx_cpbh_run "
        "ON client_profile_backfill_history(run_label)"
    )

    # DEBT-6: stamp the schema version so /health can confirm migrations ran.
    set_schema_version(conn, CURRENT_SCHEMA_VERSION)
    conn.commit()


def _table_columns(conn: sqlite3.Connection, table_name: str) -> set[str]:
    rows = conn.execute(f"PRAGMA table_info({table_name})").fetchall()
    return {row["name"] for row in rows}
