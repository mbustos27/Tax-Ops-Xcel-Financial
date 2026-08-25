"""Separate audit SQLite schema — never created inside TaxOps.db."""

from __future__ import annotations

import sqlite3
from pathlib import Path
from typing import Optional


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS audit_run (
  id                INTEGER PRIMARY KEY,
  started_at        TEXT NOT NULL,
  finished_at       TEXT,
  operator          TEXT NOT NULL,
  tool_version      TEXT NOT NULL,
  backup_verified   INTEGER NOT NULL,
  csm_unfiltered    INTEGER NOT NULL,
  drake_path        TEXT NOT NULL,
  drake_sha256      TEXT NOT NULL,
  tax_log_path      TEXT NOT NULL,
  tax_log_sha256    TEXT NOT NULL,
  taxops_snapshot   TEXT NOT NULL,
  taxops_sha256     TEXT,
  preflight_json    TEXT NOT NULL,
  preflight_ok      INTEGER NOT NULL,
  notes             TEXT,
  -- A0 baseline lock (additive; backfilled via _migrate_audit_schema)
  authoritative_taxops_path TEXT,
  taxops_path_resolution    TEXT,
  taxops_page_count         INTEGER,
  drake_row_count           INTEGER,
  tax_log_named_count       INTEGER,
  drake_mtime_utc           TEXT,
  tax_log_mtime_utc         TEXT,
  taxops_mtime_utc          TEXT,
  baseline_json             TEXT,
  csm_baseline_key          TEXT
);

CREATE TABLE IF NOT EXISTS stage_drake (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  source_row        INTEGER NOT NULL,
  id_last4          TEXT,
  client_name_raw   TEXT NOT NULL,
  return_type       TEXT,
  preparer          TEXT,
  status            TEXT,
  is_entity         INTEGER NOT NULL DEFAULT 0,
  dropped           INTEGER NOT NULL DEFAULT 0,
  drop_reason       TEXT,
  UNIQUE(run_id, source_row)
);

CREATE TABLE IF NOT EXISTS stage_log (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  sheet_name        TEXT NOT NULL,
  source_row        INTEGER NOT NULL,
  last_raw          TEXT,
  first_raw         TEXT,
  yr_raw            TEXT,
  yr_norm           INTEGER,
  processor         TEXT,
  client_status     TEXT,
  log_2025          TEXT,
  is_entity_sheet   INTEGER NOT NULL DEFAULT 0,
  dropped           INTEGER NOT NULL DEFAULT 0,
  drop_reason       TEXT,
  UNIQUE(run_id, sheet_name, source_row)
);

CREATE TABLE IF NOT EXISTS stage_taxops_client (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  client_id         INTEGER NOT NULL,
  last_name         TEXT,
  first_name        TEXT,
  display_name      TEXT,
  ssn_last4         TEXT,
  spouse_last_name  TEXT,
  spouse_first_name TEXT,
  UNIQUE(run_id, client_id)
);

CREATE TABLE IF NOT EXISTS stage_taxops_return (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  return_id         INTEGER NOT NULL,
  client_id         INTEGER NOT NULL,
  log_number        TEXT,
  tax_year          INTEGER,
  client_status     TEXT,
  processor         TEXT,
  created_at        TEXT,
  import_batch_id   INTEGER,
  UNIQUE(run_id, return_id)
);

CREATE TABLE IF NOT EXISTS stage_taxops_spouse (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  spouse_row_id     INTEGER NOT NULL,
  client_id         INTEGER NOT NULL,
  last_name         TEXT,
  first_name        TEXT,
  date_of_birth     TEXT,
  needs_review      INTEGER,
  UNIQUE(run_id, spouse_row_id)
);

CREATE TABLE IF NOT EXISTS audit_match (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  pair              TEXT NOT NULL,  -- DRAKE_LOG | DRAKE_TAXOPS | LOG_TAXOPS
  left_kind         TEXT NOT NULL,
  left_id           INTEGER NOT NULL,
  right_kind        TEXT NOT NULL,
  right_id          INTEGER NOT NULL,
  tier              TEXT NOT NULL,
  confidence        REAL NOT NULL,
  sources_agree     TEXT,
  UNIQUE(run_id, pair, left_kind, left_id, right_kind, right_id)
);

CREATE TABLE IF NOT EXISTS audit_spouse (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  drake_stage_id    INTEGER,
  log_stage_id      INTEGER,
  spouse_class      TEXT,
  spouse_first      TEXT,
  spouse_last       TEXT,
  provenance        TEXT NOT NULL,  -- OBSERVED | INFERRED
  needs_review      INTEGER NOT NULL DEFAULT 0,
  notes             TEXT
);

CREATE TABLE IF NOT EXISTS audit_spouse_store_div (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  client_id         INTEGER NOT NULL,
  clients_first     TEXT,
  clients_last      TEXT,
  spouses_first     TEXT,
  spouses_last      TEXT,
  better_store      TEXT,
  notes             TEXT
);

CREATE TABLE IF NOT EXISTS audit_finding (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  finding_type      TEXT NOT NULL,
  subtype           TEXT,
  severity          INTEGER NOT NULL DEFAULT 50,
  subject_kind      TEXT,
  subject_id        INTEGER,
  tax_year          INTEGER,
  detail_json       TEXT,
  source_refs       TEXT
);

CREATE TABLE IF NOT EXISTS audit_gap_ty2025 (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  return_id         INTEGER NOT NULL,
  client_id         INTEGER NOT NULL,
  cause             TEXT NOT NULL,
  client_status     TEXT,
  has_log_number    INTEGER,
  created_at        TEXT,
  import_batch_id   INTEGER,
  has_other_year    INTEGER,
  other_office_plausible INTEGER,
  detail_json       TEXT
);

CREATE TABLE IF NOT EXISTS audit_dropped (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  source            TEXT NOT NULL,
  source_row        INTEGER,
  reason            TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS audit_residual (
  id                INTEGER PRIMARY KEY,
  run_id            INTEGER NOT NULL REFERENCES audit_run(id),
  pair              TEXT NOT NULL,
  left_kind         TEXT NOT NULL,
  left_id           INTEGER NOT NULL,
  failure_cause     TEXT NOT NULL,
  detail_json       TEXT
);
"""


def audit_db_path(stamp: str, root: Optional[Path] = None) -> Path:
    base = root or Path(__file__).resolve().parent
    return base / f"audit_{stamp}.sqlite"


# A0 additive columns on audit_run (CREATE TABLE IF NOT EXISTS will not alter old DBs).
_AUDIT_RUN_A0_COLUMNS: tuple[tuple[str, str], ...] = (
    ("authoritative_taxops_path", "TEXT"),
    ("taxops_path_resolution", "TEXT"),
    ("taxops_page_count", "INTEGER"),
    ("drake_row_count", "INTEGER"),
    ("tax_log_named_count", "INTEGER"),
    ("drake_mtime_utc", "TEXT"),
    ("tax_log_mtime_utc", "TEXT"),
    ("taxops_mtime_utc", "TEXT"),
    ("baseline_json", "TEXT"),
    ("csm_baseline_key", "TEXT"),
)


def _migrate_audit_schema(conn: sqlite3.Connection) -> None:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(audit_run)")}
    for name, decl in _AUDIT_RUN_A0_COLUMNS:
        if name not in cols:
            conn.execute(f"ALTER TABLE audit_run ADD COLUMN {name} {decl}")


def connect_audit(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    conn.executescript(SCHEMA_SQL)
    _migrate_audit_schema(conn)
    conn.commit()
    return conn


def connect_taxops_readonly(db_path: Path) -> sqlite3.Connection:
    """Open TaxOps (or a snapshot copy) read-only. Never write."""
    raw = str(db_path)
    p = raw.replace("\\", "/")
    # SQLite UNC form requires four slashes after 'file:' —
    # file:////server/share/path  (see https://www.sqlite.org/uri.html)
    if p.startswith("//"):
        uri = f"file://{p}?mode=ro"
    elif len(p) >= 2 and p[1] == ":":
        # Windows drive letter: file:///C:/path
        uri = f"file:///{p}?mode=ro"
    else:
        uri = f"file:{p}?mode=ro"
    conn = sqlite3.connect(uri, uri=True)
    conn.row_factory = sqlite3.Row
    # Defensive: even if mode=ro failed somehow, refuse writes.
    try:
        conn.execute("PRAGMA query_only = ON;")
    except sqlite3.Error:
        pass
    return conn
