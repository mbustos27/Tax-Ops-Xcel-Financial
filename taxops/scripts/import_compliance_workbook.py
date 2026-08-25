"""COMPLIANCE-0 M0: one-time import of ACCOUNTING_LOG_2026.xlsx into the
Compliance Tracker schema (compliance_clients / compliance_credentials /
compliance_accounts / compliance_filing_periods / compliance_correspondence_log).

Safe-by-default workflow against the real workbook:

  1. --dump-headers  Scan each sheet for its real header row (not always R1 —
     this workbook puts title banners above the column headers) and print
     which HEADER_ALIASES each column matched.
  2. dry run (default)  Full reconciliation report, ZERO database writes.
  3. --apply  Write rows in a single transaction (all-or-nothing).

SECURITY: every plaintext password found in the workbook is encrypted via
compliance.crypto.encrypt_password() before it reaches SQL, and is flagged
needs_rotation=1. The plaintext is NEVER printed, logged, or written except
into that one encrypt_password() call.

Usage (from taxops/ directory):
    python scripts\\import_compliance_workbook.py --file "ACCOUNTING LOG 2026.xlsx" --dump-headers
    python scripts\\import_compliance_workbook.py --file "ACCOUNTING LOG 2026.xlsx"
    python scripts\\import_compliance_workbook.py --file "ACCOUNTING LOG 2026.xlsx" --apply
"""
from __future__ import annotations

import argparse
import re
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone

sys.path.insert(0, ".")

from db import get_connection, init_db  # noqa: E402

# ── Sheet roles (matched case-insensitively with collapsed whitespace) ─────
ROSTER_SHEETS = ["2026"]
QUARTERLY_SHEETS = [
    "CDTFA 1ST QTR 2026",
    "CDTFA 2ND QTR 2026",
    "CDTFA 3RD QTR 2026",
    "CDTFA 4TH QTR 2026",
]
MONTHLY_DEPOSIT_SHEETS = ["CDTFA - MONTHLY DEPOSITS"]
CITY_LICENSE_SHEETS = ["CITY LICENSE"]
MISC_SALES_TAX_SHEETS = ["SALES TAX MISCELLANEOUS-2025"]
MISC_CITY_LICENSE_SHEETS = ["2025 CITY LICENSE-MISCELLANEOUS"]
NOTES_SHEETS = ["NOTES"]
REPORTING_ONLY_SHEETS = ["DATA DOWNLOAD"]

# Shared portal logins (security requirement #4). Match case-insensitively
# after stripping; also normalize "@" variants of the office shared login.
KNOWN_SHARED_LOGINS = {"xcelfin92", "xcelfin@92"}

# Real workbook column headers → canonical fields. Matched case-insensitively
# with whitespace collapsed. Deliberately NOT fuzzy.
HEADER_ALIASES: dict[str, list[str]] = {
    "client_name": [
        "client", "client name", "customer", "name", "business name",
        "company", "company name",
    ],
    "corp_number": ["corp #", "corp number", "corp no", "corporation number", "entity number"],
    "fein": ["fein", "ein", "fed id", "federal id"],
    "address": ["address", "street address", "addr", "street"],
    "city": ["city", "city name"],
    "zip": ["zip", "zip code"],
    "phone": ["phone", "phone #", "phone number", "telephone"],
    "account_number": [
        "account #", "account number", "acct #", "cdtfa account", "account no",
        " account # ",  # city-license-misc has leading/trailing spaces
    ],
    "login": [
        "login", "username", "user name", "user id", "log in", "log-in",
        "user & pwd",  # monthly deposits Jun+; value often "user / pwd"
    ],
    "password": ["password", "pw", "pass", "pwd"],
    "fee": ["fee", "price", "amount"],
    "due_date": ["due date", "due"],
    "license_number": ["license #", "license number", "license no", "license"],
    "ssn_last4": ["ssn", "ssn last 4", "last 4 ssn", "ssn-4", "last 4 ss#", "last 4 ss"],
    "sales_in": ["sales in"],
    "done": ["done", "ctfa/sbe done", "cdtfa/sbe done", "ctfa done", "sbe done", "done by"],
    "need_report": ["need report", "need sales"],
    "tp_files": ["tp files", "tp filed"],
    "notes": ["notes", "note", "comment", "comments"],
    "frequency": ["freq", "frequency", "sales tax frequency"],
    "month": ["month"],
}

# Markers that identify a real header row (vs. title/banner rows above it).
_HEADER_MARKER_ALIASES = set()
for _aliases in (
    HEADER_ALIASES["client_name"]
    + HEADER_ALIASES["login"]
    + HEADER_ALIASES["account_number"]
    + HEADER_ALIASES["ssn_last4"]
    + ["freq", "#"]
):
    _HEADER_MARKER_ALIASES.add(" ".join(str(_aliases).strip().lower().split()))


def _normalize_header(h) -> str:
    return " ".join(str(h or "").strip().lower().split())


def _normalize_sheet_key(name: str) -> str:
    return " ".join(str(name or "").strip().lower().split())


def _find_sheet(wb, logical_name: str):
    """Resolve a logical sheet name against the workbook, tolerating double
    spaces and case differences (real file has 'CDTFA 1ST  QTR 2026' and
    'CDTFA 4th QTR 2026')."""
    want = _normalize_sheet_key(logical_name)
    for actual in wb.sheetnames:
        if _normalize_sheet_key(actual) == want:
            return actual
    return None


def _find_column(headers: list, canonical: str) -> int | None:
    aliases = {_normalize_header(a) for a in HEADER_ALIASES.get(canonical, [])}
    for i, h in enumerate(headers):
        if _normalize_header(h) in aliases:
            return i
    return None


def _normalize_name_key(name: str) -> str:
    """Dedup key across sheets — lowercase + collapsed whitespace only.
    Deliberately does NOT strip Inc/LLC or punctuation (over-merge risk)."""
    return " ".join(str(name or "").strip().lower().split())


def _normalize_login_key(login: str) -> str:
    return " ".join(str(login or "").strip().lower().split())


def _is_shared_login(login: str) -> bool:
    key = _normalize_login_key(login).replace(" ", "")
    return key in KNOWN_SHARED_LOGINS or key.replace("@", "") in {
        k.replace("@", "") for k in KNOWN_SHARED_LOGINS
    }


def _map_frequency(raw) -> str:
    if raw is None or str(raw).strip() == "":
        return "none"
    s = " ".join(str(raw).strip().lower().split())
    mapping = {
        "q": "quarterly",
        "qtrly": "quarterly",
        "quarterly": "quarterly",
        "q mo dep": "q_mo_dep",
        "qmodep": "q_mo_dep",
        "m": "monthly",
        "monthly": "monthly",
        "a clndr": "annual",
        "a calendar": "annual",
        "annually": "annual",
        "annual": "annual",
        "a fiscal": "fiscal",
        "fiscal": "fiscal",
        "no": "none",
        "none": "none",
        "0": "none",
        "n/a": "none",
    }
    return mapping.get(s, "none")


def _cell_truthy(val) -> bool:
    """Workbook status cells are dates, 'X', 'X ME', 'X LR', etc."""
    if val is None:
        return False
    if isinstance(val, datetime):
        return True
    s = str(val).strip().upper()
    if not s or s in {"N/A", "NA", "-", "NONE"}:
        return False
    return True


def _derive_status(sales_in, done, tp_files, need_report, notes=None) -> str:
    if _cell_truthy(done):
        return "done"
    if notes and "ready" in str(notes).strip().lower():
        return "ready"
    if _cell_truthy(tp_files):
        return "tp_filed"
    if _cell_truthy(sales_in) and not _cell_truthy(need_report):
        return "sales_in"
    return "needs_sales_data"


def _split_user_and_pwd(raw) -> tuple[str | None, str | None]:
    """Parse 'user / pwd' cells from monthly-deposit 'USER & PWD' columns.
    Returns (login, password). If no separator, treat the whole value as login."""
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s:
        return None, None
    # Skip instructional placeholders that aren't credentials.
    upper = s.upper()
    if upper.startswith("CALL ") or upper in {"CALL SAUL", "MAYRA", "DAVID"}:
        return None, None
    if " / " in s:
        left, right = s.split(" / ", 1)
        return left.strip() or None, right.strip() or None
    if "/" in s and " " not in s.split("/", 1)[0]:
        left, right = s.split("/", 1)
        return left.strip() or None, right.strip() or None
    return s, None


def _parse_shared_banner(cell) -> tuple[str | None, str | None]:
    """Parse 'LOG IN: xcelfin92   PWD: Sentry@1108' from monthly banner rows."""
    if cell is None:
        return None, None
    s = str(cell)
    login_m = re.search(r"LOG\s*IN\s*:\s*(\S+)", s, re.IGNORECASE)
    pwd_m = re.search(r"PWD\s*:\s*(\S+)", s, re.IGNORECASE)
    login = login_m.group(1).strip() if login_m else None
    pwd = pwd_m.group(1).strip() if pwd_m else None
    return login, pwd


def _looks_like_header_row(row) -> bool:
    markers = 0
    has_client = False
    for cell in row:
        n = _normalize_header(cell)
        if not n:
            continue
        if n in _HEADER_MARKER_ALIASES or n in {
            _normalize_header(a) for a in HEADER_ALIASES["client_name"]
        }:
            markers += 1
            if n in {_normalize_header(a) for a in HEADER_ALIASES["client_name"]}:
                has_client = True
    return has_client and markers >= 2


def _find_header_row(ws, max_scan: int = 12) -> tuple[int, list] | None:
    """Return (1-based row number, header values) for the first real header row."""
    for i, row in enumerate(ws.iter_rows(min_row=1, max_row=max_scan, values_only=True), start=1):
        cells = list(row)
        if _looks_like_header_row(cells):
            return i, cells
    return None


def _quarter_period_label(sheet_name: str) -> str:
    """'CDTFA 1ST  QTR 2026' → '1ST QTR 2026'."""
    s = " ".join(str(sheet_name).split())
    s = re.sub(r"^CDTFA\s+", "", s, flags=re.IGNORECASE)
    # Normalize '4th' → '4TH'
    s = re.sub(r"\b(\d)(st|nd|rd|th)\b", lambda m: m.group(1) + m.group(2).upper(), s, flags=re.IGNORECASE)
    return s.strip()


@dataclass
class ReconciliationReport:
    sheets_processed: list[str] = field(default_factory=list)
    sheets_skipped_missing: list[str] = field(default_factory=list)
    clients_found: int = 0
    clients_created: int = 0
    accounts_created: int = 0
    credentials_migrated: int = 0
    credentials_flagged_for_rotation: int = 0
    filing_periods_created: int = 0
    correspondence_notes_created: int = 0
    warnings: list[str] = field(default_factory=list)

    def print_report(self) -> None:
        print("\n" + "=" * 72)
        print("COMPLIANCE WORKBOOK IMPORT — RECONCILIATION REPORT")
        print("=" * 72)
        print(f"Sheets processed:              {self.sheets_processed}")
        if self.sheets_skipped_missing:
            print(f"Sheets NOT found in workbook:  {self.sheets_skipped_missing}")
        print(f"Distinct clients found:        {self.clients_found}")
        print(f"compliance_clients rows:       {self.clients_created}")
        print(f"compliance_accounts rows:      {self.accounts_created}")
        print(f"compliance_credentials rows:   {self.credentials_migrated}")
        print(f"  ...flagged needs_rotation:   {self.credentials_flagged_for_rotation}")
        print(f"compliance_filing_periods:     {self.filing_periods_created}")
        print(f"correspondence_log rows:       {self.correspondence_notes_created}")
        if self.warnings:
            print(f"\nWarnings ({len(self.warnings)}) — review before trusting counts above:")
            for w in self.warnings[:200]:
                print(f"  - {w}")
            if len(self.warnings) > 200:
                print(f"  ... and {len(self.warnings) - 200} more")
        print("=" * 72)


def _now() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class Importer:
    """In-memory state for one import run. Dry-run (apply=False) updates
    dedup maps + report counts without writing to the DB."""

    def __init__(self, conn, apply: bool, report: ReconciliationReport):
        self.conn = conn
        self.apply = apply
        self.report = report
        self._client_id_by_key: dict[str, int] = {}
        self._account_id_by_key: dict[tuple, int] = {}
        self._credential_id_by_login: dict[str, int] = {}
        self._next_fake_id = -1

    def get_or_create_client(self, name: str, **fields) -> int | None:
        key = _normalize_name_key(name)
        if not key:
            self.report.warnings.append("Skipped a row with a blank/empty client name")
            return None
        if key in self._client_id_by_key:
            # Backfill restricted fields discovered on a later sheet.
            if self.apply:
                client_id = self._client_id_by_key[key]
                updates = []
                params = []
                for col in ("corp_number", "fein", "address", "city", "zip", "phone", "ssn_last4"):
                    val = fields.get(col)
                    if val is None or str(val).strip() == "":
                        continue
                    row = self.conn.execute(
                        f"SELECT {col} FROM compliance_clients WHERE id=?", (client_id,)
                    ).fetchone()
                    if row and row[col] is None:
                        updates.append(f"{col}=?")
                        params.append(str(val).strip() if col != "ssn_last4" else str(val).strip()[-4:])
                if updates:
                    params.extend([_now(), client_id])
                    self.conn.execute(
                        f"UPDATE compliance_clients SET {', '.join(updates)}, updated_at=? WHERE id=?",
                        params,
                    )
            return self._client_id_by_key[key]

        self.report.clients_found += 1
        ts = _now()
        ssn = fields.get("ssn_last4")
        ssn4 = None
        if ssn is not None and str(ssn).strip():
            digits = re.sub(r"\D", "", str(ssn))
            ssn4 = digits[-4:] if digits else str(ssn).strip()[-4:]

        row = {
            "name": name.strip(),
            "client_type": fields.get("client_type", "business"),
            "corp_number": fields.get("corp_number"),
            "fein": fields.get("fein"),
            "address": fields.get("address"),
            "city": fields.get("city"),
            "zip": fields.get("zip"),
            "phone": fields.get("phone"),
            "ssn_last4": ssn4,
            "active": 1,
            "created_at": ts,
            "updated_at": ts,
        }
        if self.apply:
            cur = self.conn.execute(
                """
                INSERT INTO compliance_clients
                    (name, client_type, corp_number, fein, address, city, zip, phone,
                     ssn_last4, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    row["name"], row["client_type"], row["corp_number"], row["fein"],
                    row["address"], row["city"], row["zip"], row["phone"],
                    row["ssn_last4"], row["active"], row["created_at"], row["updated_at"],
                ),
            )
            client_id = cur.lastrowid
        else:
            client_id = self._next_fake_id
            self._next_fake_id -= 1

        self.report.clients_created += 1
        self._client_id_by_key[key] = client_id
        return client_id

    def get_or_create_credential(
        self, login: str | None, password: str | None = None, shared_login: bool = False,
    ) -> int | None:
        login_s = (str(login).strip() if login else "") or ""
        pwd_s = (str(password).strip() if password else "") or ""
        if not login_s and not pwd_s:
            return None

        # Prefer an existing credential for the same login username so the
        # same xcelfin92 / portal login isn't duplicated across 50 accounts.
        if login_s:
            key = _normalize_login_key(login_s)
            if key in self._credential_id_by_login:
                existing = self._credential_id_by_login[key]
                # If we later discover a real password for a login that was
                # created username-only, backfill the ciphertext once.
                if pwd_s and self.apply:
                    row = self.conn.execute(
                        "SELECT encrypted_password FROM compliance_credentials WHERE id=?",
                        (existing,),
                    ).fetchone()
                    if row and row["encrypted_password"] is None:
                        from compliance.crypto import encrypt_password

                        encrypted = encrypt_password(pwd_s)
                        self.conn.execute(
                            """
                            UPDATE compliance_credentials
                               SET encrypted_password=?, needs_rotation=1, updated_at=?
                             WHERE id=?
                            """,
                            (encrypted, _now(), existing),
                        )
                        self.report.credentials_flagged_for_rotation += 1
                return existing

        if login_s and _is_shared_login(login_s):
            shared_login = True

        ts = _now()
        needs_rotation = 0
        encrypted = None
        if pwd_s:
            from compliance.crypto import encrypt_password

            encrypted = encrypt_password(pwd_s)  # never log/print pwd_s
            needs_rotation = 1
            self.report.credentials_flagged_for_rotation += 1

        if self.apply:
            cur = self.conn.execute(
                """
                INSERT INTO compliance_credentials
                    (login_username, encrypted_password, encryption_key_ref, shared_login,
                     needs_rotation, notes, created_at, updated_at)
                VALUES (?, ?, 'default', ?, ?, ?, ?, ?)
                """,
                (
                    login_s, encrypted, int(shared_login), needs_rotation,
                    "Migrated from ACCOUNTING LOG 2026.xlsx", ts, ts,
                ),
            )
            cred_id = cur.lastrowid
        else:
            cred_id = self._next_fake_id
            self._next_fake_id -= 1

        self.report.credentials_migrated += 1
        if login_s:
            self._credential_id_by_login[_normalize_login_key(login_s)] = cred_id
        return cred_id

    def get_or_create_account(
        self, client_id: int, account_type: str, dedup_key=None, account_number=None,
        city_name=None, frequency="quarterly", credential_id=None, fee=None,
    ) -> int:
        key = (client_id, account_type, dedup_key if dedup_key is not None else (account_number or ""))
        if key in self._account_id_by_key:
            account_id = self._account_id_by_key[key]
            if credential_id is not None and self.apply:
                row = self.conn.execute(
                    "SELECT credential_id FROM compliance_accounts WHERE id=?", (account_id,)
                ).fetchone()
                if row and row["credential_id"] is None:
                    self.conn.execute(
                        "UPDATE compliance_accounts SET credential_id=?, updated_at=? WHERE id=?",
                        (credential_id, _now(), account_id),
                    )
            return account_id

        ts = _now()
        acct_num = str(account_number).strip() if account_number not in (None, "") else None
        fee_val = None
        if fee is not None and str(fee).strip() != "":
            try:
                fee_val = float(fee)
            except (TypeError, ValueError):
                fee_val = None

        if self.apply:
            cur = self.conn.execute(
                """
                INSERT INTO compliance_accounts
                    (compliance_client_id, account_type, account_number, city_name,
                     frequency, credential_id, fee, active, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, 1, ?, ?)
                """,
                (client_id, account_type, acct_num, city_name, frequency, credential_id, fee_val, ts, ts),
            )
            account_id = cur.lastrowid
        else:
            account_id = self._next_fake_id
            self._next_fake_id -= 1
        self.report.accounts_created += 1
        self._account_id_by_key[key] = account_id
        return account_id

    def create_filing_period(
        self, account_id: int, period_type: str, period_label: str, fee=None,
        period_due_date=None, status="needs_sales_data",
    ) -> None:
        ts = _now()
        due = None
        if period_due_date is not None:
            if isinstance(period_due_date, datetime):
                due = period_due_date.date().isoformat()
            else:
                due = str(period_due_date).strip() or None
        fee_val = None
        if fee is not None and str(fee).strip() != "":
            try:
                fee_val = float(fee)
            except (TypeError, ValueError):
                fee_val = None

        if self.apply:
            self.conn.execute(
                """
                INSERT OR IGNORE INTO compliance_filing_periods
                    (compliance_account_id, period_type, period_label, period_due_date,
                     fee, status, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (account_id, period_type, period_label, due, fee_val, status, ts, ts),
            )
        self.report.filing_periods_created += 1

    def create_correspondence_note(self, client_id: int, month: str, note_type: str, note: str) -> None:
        if not note or not str(note).strip():
            return
        ts = _now()
        if self.apply:
            self.conn.execute(
                """
                INSERT INTO compliance_correspondence_log
                    (compliance_client_id, month, note_type, note, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (client_id, month, note_type, str(note).strip(), ts),
            )
        self.report.correspondence_notes_created += 1


def _load_workbook(path: str):
    import openpyxl

    return openpyxl.load_workbook(path, data_only=True, read_only=True)


def _col_map(headers: list) -> dict[str, int | None]:
    return {k: _find_column(headers, k) for k in HEADER_ALIASES}


def _row_val(row, col: dict, key: str):
    idx = col.get(key)
    if idx is None or idx >= len(row):
        return None
    return row[idx]


def _resolve_q1_login_vs_account(col: dict, row) -> tuple:
    """Q1 sheet labels account# as 'Log In' and the portal username as
    'Password'. Q2+ correctly use 'ACCT #' + 'LOG-IN'. Detect the Q1 layout
    when we have a login column but no account_number column, and the login
    cell is all digits (CDTFA account numbers)."""
    account_number = _row_val(row, col, "account_number")
    login = _row_val(row, col, "login")
    password = _row_val(row, col, "password")

    if account_number is None and login is not None and password is not None:
        login_s = str(login).strip()
        if login_s.isdigit() and len(login_s) >= 6:
            # Q1 layout: Log In = account #, Password = portal username
            return password, None, login_s

    # Normal layout (Q2+): LOG-IN is username; Password/PWD (if present) is pwd.
    # Also handle USER & PWD combined cells.
    if login is not None and password is None:
        split_login, split_pwd = _split_user_and_pwd(login)
        return split_login, split_pwd, account_number

    return (
        str(login).strip() if login not in (None, "") else None,
        str(password).strip() if password not in (None, "") else None,
        str(account_number).strip() if account_number not in (None, "") else None,
    )


def dump_headers(path: str) -> None:
    wb = _load_workbook(path)
    print(f"Workbook: {path}")
    print(f"Sheets found: {wb.sheetnames}\n")
    for sheet_name in wb.sheetnames:
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        print(f"--- {sheet_name!r} ---")
        if not found:
            print("  (no header row detected in first 12 rows)")
            print()
            continue
        row_num, headers = found
        print(f"  header at row {row_num}")
        for i, h in enumerate(headers):
            if h is None or str(h).strip() == "":
                continue
            canonical_matches = [k for k in HEADER_ALIASES if _find_column([h], k) is not None]
            tag = (
                f"  -> matched: {canonical_matches}"
                if canonical_matches
                else "  -> UNMATCHED (add to HEADER_ALIASES if needed)"
            )
            print(f"  col {i}: {h!r}{tag}")
        print()


def import_roster_sheets(wb, importer: Importer) -> None:
    for logical in ROSTER_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        if not found:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no header row found — skipped")
            continue
        header_row_num, headers = found
        col = _col_map(headers)
        if col["client_name"] is None:
            importer.report.warnings.append(
                f"Sheet {sheet_name!r}: no client-name column — skipped"
            )
            continue

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = _row_val(row, col, "client_name")
            if not name or not str(name).strip():
                continue
            # Skip spacer / total rows
            if str(name).strip().upper() in {"TOTAL", "TOTALS"}:
                continue
            importer.get_or_create_client(
                str(name),
                corp_number=_row_val(row, col, "corp_number"),
                fein=_row_val(row, col, "fein"),
                address=_row_val(row, col, "address"),
                city=_row_val(row, col, "city"),
                zip=_row_val(row, col, "zip"),
                phone=_row_val(row, col, "phone"),
            )


def import_quarterly_sheets(wb, importer: Importer) -> None:
    for logical in QUARTERLY_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        if not found:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no header row found — skipped")
            continue
        header_row_num, headers = found
        col = _col_map(headers)
        period_label = _quarter_period_label(sheet_name)

        if col["client_name"] is None:
            importer.report.warnings.append(
                f"Sheet {sheet_name!r}: no client-name column — skipped"
            )
            continue

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = _row_val(row, col, "client_name")
            if not name or not str(name).strip():
                continue

            client_id = importer.get_or_create_client(str(name))
            if client_id is None:
                continue

            login, password, account_number = _resolve_q1_login_vs_account(col, row)
            credential_id = None
            if login or password:
                credential_id = importer.get_or_create_credential(login, password)

            freq = _map_frequency(_row_val(row, col, "frequency"))
            # Skip creating a CDTFA account for clients marked NONE with no acct#
            # and no login — they appear on the roster but have no filing obligation.
            if freq == "none" and not account_number and not login:
                continue

            account_id = importer.get_or_create_account(
                client_id, "cdtfa_sales_tax",
                dedup_key=account_number or str(name),
                account_number=account_number,
                frequency=freq if freq != "none" else "quarterly",
                credential_id=credential_id,
                fee=_row_val(row, col, "fee"),
            )

            status = _derive_status(
                _row_val(row, col, "sales_in"),
                _row_val(row, col, "done"),
                _row_val(row, col, "tp_files"),
                _row_val(row, col, "need_report"),
                notes=_row_val(row, col, "notes"),
            )
            importer.create_filing_period(
                account_id, "quarterly", period_label,
                fee=_row_val(row, col, "fee"), status=status,
            )


def import_monthly_deposit_sheets(wb, importer: Importer) -> None:
    """Monthly deposits sheet repeats a header block once per month. Walk
    every row, re-bind columns when a header reappears, and use MONTH OF:
    labels for period_label."""
    for logical in MONTHLY_DEPOSIT_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]

        col: dict[str, int | None] = {}
        current_month = "unknown-month"
        shared_login = None
        shared_pwd = None

        for row in ws.iter_rows(values_only=True):
            cells = list(row)

            # Banner with shared office credentials (only encrypted, never printed).
            for cell in cells:
                login, pwd = _parse_shared_banner(cell)
                if login and pwd:
                    shared_login, shared_pwd = login, pwd
                    importer.get_or_create_credential(shared_login, shared_pwd, shared_login=True)

            # Month section marker
            for i, cell in enumerate(cells):
                if cell is not None and _normalize_header(cell) == "month of:":
                    # Next non-empty cell is the month label
                    for j in range(i + 1, len(cells)):
                        if cells[j] not in (None, ""):
                            current_month = str(cells[j]).strip()
                            break
                    break

            if _looks_like_header_row(cells):
                col = _col_map(cells)
                continue

            if not col or col.get("client_name") is None:
                continue

            name = _row_val(cells, col, "client_name")
            if not name or not str(name).strip():
                continue
            # Skip if this is somehow another header echo
            if _normalize_header(name) in {_normalize_header(a) for a in HEADER_ALIASES["client_name"]}:
                continue

            client_id = importer.get_or_create_client(
                str(name),
                corp_number=_row_val(cells, col, "corp_number"),
                fein=_row_val(cells, col, "fein"),
            )
            if client_id is None:
                continue

            login, password, account_number = _resolve_q1_login_vs_account(col, cells)
            # If row uses the shared office login with no per-row password,
            # attach the banner password via credential dedup.
            if login and _is_shared_login(login) and not password and shared_pwd:
                password = shared_pwd

            credential_id = None
            if login or password:
                credential_id = importer.get_or_create_credential(login, password)

            freq = _map_frequency(_row_val(cells, col, "frequency")) or "q_mo_dep"
            account_id = importer.get_or_create_account(
                client_id, "sbe_deposit",
                dedup_key=account_number or str(name),
                account_number=account_number,
                frequency=freq if freq != "none" else "q_mo_dep",
                credential_id=credential_id,
                fee=_row_val(cells, col, "fee"),
            )
            status = _derive_status(
                _row_val(cells, col, "sales_in"),
                _row_val(cells, col, "done"),
                _row_val(cells, col, "tp_files"),
                _row_val(cells, col, "need_report"),
                notes=_row_val(cells, col, "notes"),
            )
            # Period label includes year context when the month cell is just "FEBRUARY"
            period_label = current_month
            if period_label and "2026" not in period_label.upper() and period_label.upper() != "JANUARY TO DECEMBER 2026":
                period_label = f"{period_label} 2026"
            importer.create_filing_period(
                account_id, "monthly_deposit", period_label,
                fee=_row_val(cells, col, "fee"), status=status,
            )


def import_city_license_sheets(wb, importer: Importer) -> None:
    for logical in CITY_LICENSE_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        if not found:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no header row found — skipped")
            continue
        header_row_num, headers = found
        col = _col_map(headers)
        if col["client_name"] is None:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no client-name column — skipped")
            continue

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = _row_val(row, col, "client_name")
            if not name or not str(name).strip():
                continue
            city = _row_val(row, col, "city")
            # Skip N/A city rows that have no license — still create client for roster completeness
            client_id = importer.get_or_create_client(str(name), city=city, address=_row_val(row, col, "address"))
            if client_id is None:
                continue

            license_number = _row_val(row, col, "license_number")
            city_s = str(city).strip() if city not in (None, "") else ""
            if city_s.upper() in {"N/A", "NA", ""} and not license_number:
                continue

            account_id = importer.get_or_create_account(
                client_id, "city_license",
                dedup_key=str(license_number).strip() if license_number else (city_s or str(name)),
                account_number=license_number,
                city_name=city_s or None,
                frequency="annual",
                fee=_row_val(row, col, "fee"),
            )
            status = _derive_status(
                _row_val(row, col, "sales_in"),
                _row_val(row, col, "done"),
                _row_val(row, col, "tp_files"),
                _row_val(row, col, "need_report"),
                notes=_row_val(row, col, "notes"),
            )
            importer.create_filing_period(
                account_id, "annual_renewal", "2026 renewal",
                fee=_row_val(row, col, "fee"),
                period_due_date=_row_val(row, col, "due_date"),
                status=status,
            )


def import_misc_sheets(wb, importer: Importer) -> None:
    for logical in MISC_SALES_TAX_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        if not found:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no header row found — skipped")
            continue
        header_row_num, headers = found
        col = _col_map(headers)
        if col["client_name"] is None:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no client-name column — skipped")
            continue

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = _row_val(row, col, "client_name")
            if not name or not str(name).strip():
                continue
            client_id = importer.get_or_create_client(str(name), client_type="individual")
            if client_id is None:
                continue

            login = _row_val(row, col, "login")
            password = _row_val(row, col, "password")
            credential_id = None
            if login or password:
                credential_id = importer.get_or_create_credential(
                    str(login).strip() if login else "",
                    str(password).strip() if password else None,
                )

            account_id = importer.get_or_create_account(
                client_id, "misc",
                dedup_key=f"sales_tax_misc:{_normalize_name_key(name)}",
                frequency=_map_frequency(_row_val(row, col, "frequency")),
                credential_id=credential_id,
                fee=_row_val(row, col, "fee"),
            )
            status = _derive_status(
                _row_val(row, col, "sales_in"),
                _row_val(row, col, "done"),
                _row_val(row, col, "tp_files"),
                _row_val(row, col, "need_report"),
                notes=_row_val(row, col, "notes"),
            )
            importer.create_filing_period(
                account_id, "annual_renewal", "SALES TAX MISC 2025",
                fee=_row_val(row, col, "fee"), status=status,
            )

    for logical in MISC_CITY_LICENSE_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]
        found = _find_header_row(ws)
        if not found:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no header row found — skipped")
            continue
        header_row_num, headers = found
        col = _col_map(headers)
        if col["client_name"] is None:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no client-name column — skipped")
            continue

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = _row_val(row, col, "client_name")
            if not name or not str(name).strip():
                continue
            ssn4 = _row_val(row, col, "ssn_last4")
            client_id = importer.get_or_create_client(
                str(name),
                client_type="individual",
                ssn_last4=ssn4,
                address=_row_val(row, col, "address"),
                city=_row_val(row, col, "city"),
                zip=_row_val(row, col, "zip"),
                phone=_row_val(row, col, "phone"),
            )
            if client_id is None:
                continue

            account_id = importer.get_or_create_account(
                client_id, "city_license",
                dedup_key=_row_val(row, col, "account_number") or f"misc_city:{_normalize_name_key(name)}",
                account_number=_row_val(row, col, "account_number"),
                city_name=_row_val(row, col, "city"),
                frequency="annual",
                fee=_row_val(row, col, "fee"),
            )
            status = "done" if _cell_truthy(_row_val(row, col, "done")) else "needs_sales_data"
            importer.create_filing_period(
                account_id, "annual_renewal", "2025 CITY LICENSE MISC",
                fee=_row_val(row, col, "fee"),
                period_due_date=None,
                status=status,
            )


def import_notes_sheet(wb, importer: Importer) -> None:
    for logical in NOTES_SHEETS:
        sheet_name = _find_sheet(wb, logical)
        if not sheet_name:
            importer.report.sheets_skipped_missing.append(logical)
            continue
        importer.report.sheets_processed.append(sheet_name)
        ws = wb[sheet_name]

        # NOTES layout: R3 has CLIENT + month names; client names start R4 col B.
        header_row_num = None
        headers = None
        for i, row in enumerate(ws.iter_rows(min_row=1, max_row=6, values_only=True), start=1):
            cells = list(row)
            # Look for a row containing CLIENT and at least one month name
            norms = [_normalize_header(c) for c in cells]
            if "client" in norms and any(
                m in norms for m in (
                    "january", "february", "march", "april", "may", "june",
                    "july", "august", "september", "october", "november", "december",
                )
            ):
                header_row_num = i
                headers = cells
                break

        if header_row_num is None or headers is None:
            importer.report.warnings.append(f"Sheet {sheet_name!r}: no CLIENT×month header — skipped")
            continue

        name_idx = None
        for i, h in enumerate(headers):
            if _normalize_header(h) in {"client", "client name", "company name"}:
                name_idx = i
                break
        if name_idx is None:
            # Real file puts CLIENT in col B (index 1)
            name_idx = 1

        month_columns = [
            (i, str(h).strip())
            for i, h in enumerate(headers)
            if i != name_idx and h is not None and str(h).strip()
            and _normalize_header(h) not in {"", "description of p/w corresoncence", "missing p/w"}
        ]

        for row in ws.iter_rows(min_row=header_row_num + 1, values_only=True):
            name = row[name_idx] if name_idx < len(row) else None
            if not name or not str(name).strip():
                continue
            client_id = importer.get_or_create_client(str(name))
            if client_id is None:
                continue
            for col_idx, month_label in month_columns:
                if col_idx >= len(row):
                    continue
                note = row[col_idx]
                if not note:
                    continue
                note_str = str(note)
                note_type = "general"
                lowered = note_str.lower()
                if "password" in lowered and ("missing" in lowered or "need" in lowered):
                    note_type = "missing_password"
                elif "password" in lowered or "p/w" in lowered or "pw" in lowered:
                    note_type = "password_correspondence"
                importer.create_correspondence_note(client_id, month_label, note_type, note_str)


def run_import(path: str, apply: bool) -> ReconciliationReport:
    report = ReconciliationReport()
    wb = _load_workbook(path)

    for logical in REPORTING_ONLY_SHEETS:
        actual = _find_sheet(wb, logical)
        if actual:
            report.sheets_processed.append(f"{actual} (reporting-only, not imported)")

    conn = get_connection()
    try:
        init_db(conn)
        importer = Importer(conn, apply=apply, report=report)

        import_roster_sheets(wb, importer)
        import_quarterly_sheets(wb, importer)
        import_monthly_deposit_sheets(wb, importer)
        import_city_license_sheets(wb, importer)
        import_misc_sheets(wb, importer)
        import_notes_sheet(wb, importer)

        if apply:
            conn.commit()
        else:
            conn.rollback()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--file", required=True, help="Path to ACCOUNTING LOG 2026.xlsx")
    parser.add_argument(
        "--dump-headers", action="store_true",
        help="Print each sheet's detected header row and exit — no data read, no DB access",
    )
    parser.add_argument("--apply", action="store_true", help="Actually write rows (default: dry run)")
    args = parser.parse_args()

    if args.dump_headers:
        dump_headers(args.file)
        return 0

    report = run_import(args.file, apply=args.apply)
    report.print_report()
    if not args.apply:
        print(
            "\nDRY RUN — no rows were written. Review the report above, tune "
            "HEADER_ALIASES/warnings as needed, then re-run with --apply."
        )
    else:
        print("\nDone. Rows above were written to the database.")
        if report.credentials_flagged_for_rotation:
            print(
                f"\n*** {report.credentials_flagged_for_rotation} credential(s) were flagged "
                "needs_rotation=1 — every one of these passwords was sitting in the .xlsx and "
                "must be treated as compromised. Rotate them in the actual CDTFA/city portals "
                "and update via the Compliance Tracker admin UI as soon as practical. ***"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
