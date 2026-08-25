"""Drake TAXPAYER.csv (invoice-bearing spouse/household export) — parse + validate.

Amendment 1: Invoice Number is the cross-system key (Tax Log number written into Drake).
Amendment 2 C1/C2: L0 eligibility validates the *bare log* after normalization, not raw
``^\\d{6}$``; collisions recomputed in bare-log space with deduped claimants.

Never writes to TaxOps. Does not change A2 fingerprint recipes.
"""

from __future__ import annotations

import csv
import re
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from audit.util import dumps, sha256_file

INVOICE_COL_COUNT = 11  # legacy spouse/household export
LINK_COL_COUNT = 5  # Wave 0 "INVOICE NUMBER LINK" export
DIGITS_ONLY_RE = re.compile(r"^\d+$")

# Canonical location for audit inputs (Amendment 1 / Wave 0).
DEFAULT_TAXPAYER_INVOICE_PATH = Path(r"T:\audit\investigation\TAXPAYER.csv")
SUPERSEDED_SPOUSE_EXPORTS = (
    Path(r"T:\taxops\CSVFILES\TAXPAYERspouse25.csv"),
    Path(r"C:\TaxOps\taxops\CSVFILES\TAXPAYERspouse25.csv"),
    Path(r"C:\Users\Windows 10\Desktop\TAXPAYERspouse25.csv"),
    Path(r"T:\audit\investigation\TAXPAYERspouse25.csv"),
)

# Legacy 11-col spouse export (Amendment 1).
EXPECTED_HEADER_SPOUSE_11 = [
    "Taxpayer First Name",
    "Taxpayer Last Name",
    "Taxpayer Date of Birth",
    "Taxpayer Daytime Phone",
    "Taxpayer Email Address",
    "Spouse Name",
    "Spouse Daytime Phone",
    "Spouse Date of Birth",
    "Dependent First Name",
    "Dependent Last Name",
    "Invoice Number",
]
# Wave 0 purpose-built link export — Invoice is col 3 (index 2), not last.
EXPECTED_HEADER_LINK_5 = [
    "Taxpayer Last Name",
    "Taxpayer First Name",
    "Invoice Number",
    "Taxpayer Date of Birth",
    "Street Address",
]
# Back-compat alias
EXPECTED_HEADER = EXPECTED_HEADER_SPOUSE_11

LAYOUT_SPOUSE_11 = "spouse_11"
LAYOUT_LINK_5 = "link_5"


def _norm_person(last: str, first: str) -> str:
    s = f"{(last or '').upper()}|{(first or '').upper()}"
    s = re.sub(r"[^A-Z0-9|]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def season_from_title(title: str, as_of_line: str = "") -> Optional[int]:
    """'TY2025 SPOUSE …' → 2025. Do not use As-of calendar year (Aug 2026 ≠ TY2026)."""
    for text in (title or "", as_of_line or ""):
        m = re.search(r"TY\s*(20\d{2})", text, re.I)
        if m:
            return int(m.group(1))
    return None


def detect_layout(header: list[str]) -> tuple[str, int, list[str]]:
    """Return (layout_id, expected_col_count, expected_header)."""
    norm = [h.strip() for h in header]
    if norm == EXPECTED_HEADER_LINK_5:
        return LAYOUT_LINK_5, LINK_COL_COUNT, EXPECTED_HEADER_LINK_5
    if norm == EXPECTED_HEADER_SPOUSE_11:
        return LAYOUT_SPOUSE_11, INVOICE_COL_COUNT, EXPECTED_HEADER_SPOUSE_11
    # Fuzzy: Invoice Number present + 5 cols starting Last/First
    if len(norm) == 5 and "Invoice Number" in norm:
        return LAYOUT_LINK_5, LINK_COL_COUNT, EXPECTED_HEADER_LINK_5
    return LAYOUT_SPOUSE_11, INVOICE_COL_COUNT, EXPECTED_HEADER_SPOUSE_11


def season_prefix(tax_year: int) -> str:
    return str(tax_year)[-2:]


def bare_log_number(raw: str, tax_year: int = 2025) -> str:
    """
    Cross-system log key without season prefix or leading zeros.

    Drake Invoice Number is season-prefixed (TY2025 → ``25`` + padded sequence,
    e.g. ``250141`` / ``25141`` / ``2501177``). TaxOps ``returns.log_number`` and
    Tax Log col B store the bare sequence (``141``, ``1177``).
    """
    digits = re.sub(r"\D", "", str(raw or ""))
    if not digits:
        return ""
    prefix = str(tax_year)[-2:]
    # Season-only tokens (``25``) and zero-padded empties (``250`` → ``0``) → empty bare.
    if digits.startswith(prefix):
        if len(digits) == len(prefix):
            return ""
        digits = digits[len(prefix) :]
    return digits.lstrip("0") or ""


def observe_bare_log_ceiling(
    *,
    taxops_path: Optional[Path] = None,
    tax_log_path: Optional[Path] = None,
    tax_year: int = 2025,
) -> dict[str, Any]:
    """
    Ceiling = max observed bare log# across TaxOps returns (tax_year) and Tax Log
    named rows. Do not hard-code.
    """
    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.db import connect_taxops_readonly

    mem = load_baseline_memory()
    taxops = Path(taxops_path or mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")
    log_path = Path(tax_log_path or mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

    values: list[int] = []
    sources: dict[str, int] = {"taxops": 0, "tax_log": 0}

    if taxops.exists():
        conn = connect_taxops_readonly(taxops)
        try:
            for (logn,) in conn.execute(
                "SELECT log_number FROM returns WHERE tax_year=? "
                "AND log_number IS NOT NULL AND TRIM(log_number)!=''",
                (tax_year,),
            ):
                bare = bare_log_number(str(logn), tax_year)
                if bare.isdigit():
                    values.append(int(bare))
                    sources["taxops"] += 1
        finally:
            conn.close()

    if log_path.exists():
        import openpyxl

        wb = openpyxl.load_workbook(log_path, read_only=True, data_only=True)
        ws = wb[config.SHEET_INDIVIDUALS]
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if i < config.LOG_DATA_START_ROW:
                continue
            vals = list(row)
            v = vals[1] if len(vals) > 1 else None
            if isinstance(v, float) and v == int(v):
                v = int(v)
            last = str(vals[2] or "").strip() if len(vals) > 2 else ""
            first = str(vals[3] or "").strip() if len(vals) > 3 else ""
            if not (last or first):
                continue
            bare = bare_log_number(str(v or ""), tax_year)
            if bare.isdigit():
                values.append(int(bare))
                sources["tax_log"] += 1
        wb.close()

    ceiling = max(values) if values else 0
    return {
        "bare_log_max": ceiling,
        "n_observed": len(values),
        "n_taxops": sources["taxops"],
        "n_tax_log": sources["tax_log"],
        "tax_year": tax_year,
    }


@dataclass
class InvoiceRow:
    source_row: int  # 1-based file line
    raw_field_count: int
    first: str
    last: str
    dob: str
    phone: str
    email: str
    spouse_name: str
    spouse_phone: str
    spouse_dob: str
    dep_first: str
    dep_last: str
    invoice: str
    padded: bool
    is_entity: bool  # blank first name
    bare_log: str = ""
    address: str = ""

    @property
    def is_full_width(self) -> bool:
        return not self.padded


@dataclass
class InvoiceExportReport:
    path: str
    exists: bool
    sha256: Optional[str] = None
    mtime_utc: Optional[str] = None
    size: Optional[int] = None
    title_lines: list[str] = field(default_factory=list)
    tax_year: Optional[int] = None
    season_prefix: Optional[str] = None
    header: list[str] = field(default_factory=list)
    layout: str = LAYOUT_SPOUSE_11
    expected_col_count: int = INVOICE_COL_COUNT
    data_row_count: int = 0
    full_width_count: int = 0
    ragged_count: int = 0
    empty_invoice_full_width: int = 0
    field_count_histogram: dict[str, int] = field(default_factory=dict)
    invoice_coverage_full_width_pct: Optional[float] = None
    entity_blank_first: int = 0
    entity_ragged: int = 0
    entity_full_with_invoice: int = 0
    first_name_max_len: int = 0
    last_name_max_len: int = 0
    first_at_39: int = 0
    first_at_40: int = 0
    last_at_39: int = 0
    last_at_40: int = 0
    invoice_len_histogram: dict[str, int] = field(default_factory=dict)
    malformed_invoices: list[dict[str, Any]] = field(default_factory=list)
    collisions: list[dict[str, Any]] = field(default_factory=list)
    proforma_stale: list[dict[str, Any]] = field(default_factory=list)
    valid_l0_invoice_count: int = 0
    valid_l0_bare_count: int = 0
    # Amendment 2 — side-by-side vs legacy ^\d{6}$ gate
    legacy_l0_invoice_count: Optional[int] = None
    bare_log_max: Optional[int] = None
    bare_log_ceiling_meta: dict[str, Any] = field(default_factory=dict)
    findings: list[dict[str, Any]] = field(default_factory=list)
    rows: list[InvoiceRow] = field(default_factory=list)
    l0_blocked_invoices: set[str] = field(default_factory=set)
    l0_ok_invoices: set[str] = field(default_factory=set)
    l0_ok_bare: set[str] = field(default_factory=set)
    # bare -> list of raw invoice strings that normalize to it
    bare_to_raw: dict[str, list[str]] = field(default_factory=dict)


def _legacy_six_digit_l0_count(full_rows: list[InvoiceRow], prefix: str) -> int:
    """Amendment-1 gate for before/after reporting only."""
    by_inv: dict[str, set[str]] = defaultdict(set)
    malformed: set[str] = set()
    stale: set[str] = set()
    for r in full_rows:
        inv = r.invoice
        if not inv:
            continue
        by_inv[inv].add(_norm_person(r.last, r.first))
        if not re.fullmatch(r"\d{6}", inv):
            malformed.add(inv)
            continue
        if prefix and not inv.startswith(prefix):
            stale.add(inv)
    collisions = {inv for inv, people in by_inv.items() if len(people) > 1}
    ok = 0
    for inv in by_inv:
        if inv in malformed or inv in collisions or inv in stale:
            continue
        if re.fullmatch(r"\d{6}", inv) and (not prefix or inv.startswith(prefix)):
            ok += 1
    return ok


def parse_taxpayer_invoice_csv(
    path: Path,
    *,
    bare_log_max: Optional[int] = None,
    bare_log_ceiling_meta: Optional[dict[str, Any]] = None,
) -> InvoiceExportReport:
    """
    Parse TAXPAYER.csv. L0 eligibility (Amendment 2 C1):

      digits-only → strip season prefix → strip leading zeros → bare in [1, bare_log_max].

    Collisions (C2) are computed in bare-log space with distinct claimants.
    """
    from datetime import datetime, timezone

    report = InvoiceExportReport(path=str(path), exists=path.exists())
    if not path.exists():
        report.findings.append({"type": "INVOICE_EXPORT_MISSING", "detail": str(path)})
        return report

    report.size = path.stat().st_size
    report.mtime_utc = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    report.sha256 = sha256_file(path)

    text = path.read_text(encoding="utf-8-sig")
    lines = text.splitlines()
    if len(lines) < 3:
        report.findings.append({"type": "INVOICE_EXPORT_TOO_SHORT", "detail": len(lines)})
        return report

    report.title_lines = lines[:2]
    report.tax_year = season_from_title(lines[0], lines[1] if len(lines) > 1 else "")
    if report.tax_year:
        report.season_prefix = season_prefix(report.tax_year)
    tax_year = report.tax_year or 2025
    if report.tax_year is None:
        report.tax_year = tax_year  # default season when title has no TY*
        report.season_prefix = season_prefix(tax_year)
    prefix = report.season_prefix or season_prefix(tax_year)

    # Ceiling for bare-log range
    if bare_log_max is None:
        meta = observe_bare_log_ceiling(tax_year=tax_year)
        bare_log_max = int(meta["bare_log_max"] or 0)
        report.bare_log_ceiling_meta = meta
    else:
        report.bare_log_ceiling_meta = bare_log_ceiling_meta or {"bare_log_max": bare_log_max}
    report.bare_log_max = bare_log_max

    header = next(csv.reader([lines[2]]))
    report.header = header
    layout, expected_cols, expected_header = detect_layout(header)
    report.layout = layout
    report.expected_col_count = expected_cols
    if [h.strip() for h in header] != expected_header:
        report.findings.append(
            {
                "type": "INVOICE_HEADER_MISMATCH",
                "detail": {
                    "layout": layout,
                    "expected": expected_header,
                    "observed": header,
                },
            }
        )

    hist: Counter[int] = Counter()
    rows: list[InvoiceRow] = []
    for i, line in enumerate(lines[3:], start=4):  # 1-based line numbers
        if not line.strip():
            hist[0] += 1
            report.findings.append(
                {
                    "type": "RAGGED_EXPORT_ROW",
                    "detail": {"source_row": i, "field_count": 0, "name": ""},
                }
            )
            continue
        raw = next(csv.reader([line]))
        nfields = len(raw)
        hist[nfields] += 1
        padded = nfields < expected_cols
        while len(raw) < expected_cols:
            raw.append("")
        raw = raw[:expected_cols]

        if layout == LAYOUT_LINK_5:
            # Last, First, Invoice, DOB, Address
            last = (raw[0] or "").strip()
            first = (raw[1] or "").strip()
            inv = (raw[2] or "").strip()
            dob = (raw[3] or "").strip()
            address = (raw[4] or "").strip()
            phone = email = spouse_name = spouse_phone = spouse_dob = ""
            dep_first = dep_last = ""
        else:
            # First, Last, DOB, Phone, Email, Spouse…, Invoice
            first = (raw[0] or "").strip()
            last = (raw[1] or "").strip()
            dob = (raw[2] or "").strip()
            phone = (raw[3] or "").strip()
            email = (raw[4] or "").strip()
            spouse_name = (raw[5] or "").strip()
            spouse_phone = (raw[6] or "").strip()
            spouse_dob = (raw[7] or "").strip()
            dep_first = (raw[8] or "").strip()
            dep_last = (raw[9] or "").strip()
            inv = (raw[10] or "").strip()
            address = ""

        is_entity = not first
        if padded:
            report.findings.append(
                {
                    "type": "RAGGED_EXPORT_ROW",
                    "detail": {
                        "source_row": i,
                        "field_count": nfields,
                        "name": f"{last}, {first}".strip(", "),
                    },
                }
            )
        bare = bare_log_number(inv, tax_year) if inv else ""
        rows.append(
            InvoiceRow(
                source_row=i,
                raw_field_count=nfields,
                first=first,
                last=last,
                dob=dob,
                phone=phone,
                email=email,
                spouse_name=spouse_name,
                spouse_phone=spouse_phone,
                spouse_dob=spouse_dob,
                dep_first=dep_first,
                dep_last=dep_last,
                invoice=inv,
                padded=padded,
                is_entity=is_entity,
                bare_log=bare,
                address=address,
            )
        )

    report.rows = rows
    report.data_row_count = len(rows)
    report.field_count_histogram = {str(k): v for k, v in sorted(hist.items())}
    report.full_width_count = sum(1 for r in rows if r.is_full_width)
    report.ragged_count = report.data_row_count - report.full_width_count
    if report.data_row_count:
        report.invoice_coverage_full_width_pct = round(
            100.0 * report.full_width_count / report.data_row_count, 2
        )

    firsts = [r.first for r in rows]
    lasts = [r.last for r in rows]
    report.first_name_max_len = max((len(x) for x in firsts), default=0)
    report.last_name_max_len = max((len(x) for x in lasts), default=0)
    report.first_at_39 = sum(1 for x in firsts if len(x) == 39)
    report.first_at_40 = sum(1 for x in firsts if len(x) == 40)
    report.last_at_39 = sum(1 for x in lasts if len(x) == 39)
    report.last_at_40 = sum(1 for x in lasts if len(x) == 40)

    report.entity_blank_first = sum(1 for r in rows if r.is_entity)
    report.entity_ragged = sum(1 for r in rows if r.is_entity and r.padded)
    report.entity_full_with_invoice = sum(
        1 for r in rows if r.is_entity and r.is_full_width and r.invoice
    )

    full_rows = [r for r in rows if r.is_full_width]
    empty_full = [r for r in full_rows if not r.invoice]
    report.empty_invoice_full_width = len(empty_full)
    # Blank invoice on full-width is expected on the link_5 export (no key).
    # Only flag as unexpected for the legacy spouse layout where Invoice was
    # last-col and empty was rare.
    if empty_full and layout == LAYOUT_SPOUSE_11:
        report.findings.append(
            {
                "type": "UNEXPECTED_EMPTY_INVOICE_ON_FULL_WIDTH",
                "detail": {"n": len(empty_full)},
            }
        )
    elif empty_full:
        report.findings.append(
            {
                "type": "EMPTY_INVOICE_FULL_WIDTH",
                "detail": {
                    "n": len(empty_full),
                    "note": "Explicit blank Invoice Number on full-width row (not ragged).",
                },
            }
        )

    inv_lens: Counter[int] = Counter(len(r.invoice) for r in full_rows if r.invoice)
    report.invoice_len_histogram = {str(k): v for k, v in sorted(inv_lens.items())}
    report.legacy_l0_invoice_count = _legacy_six_digit_l0_count(full_rows, prefix)

    # ── C1 validation on full-width rows ───────────────────────────────
    malformed: list[dict[str, Any]] = []
    # bare -> {person_key: {display, rows}}
    by_bare_people: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    bare_to_raw: dict[str, set[str]] = defaultdict(set)
    format_ok_raw: set[str] = set()  # raw invoices that pass C1 format/range

    for r in full_rows:
        inv = r.invoice
        if not inv:
            continue
        name = f"{r.last}, {r.first}".strip(", ")
        person = _norm_person(r.last, r.first)

        # 1. digits only
        if not DIGITS_ONLY_RE.match(inv):
            detail = {
                "invoice": inv,
                "bare_log": r.bare_log or "",
                "length": len(inv),
                "name": name,
                "reason": "non_digits",
            }
            malformed.append({**detail, "source_row": r.source_row})
            report.findings.append({"type": "MALFORMED_LOG_NUMBER", "detail": detail})
            continue

        bare = bare_log_number(inv, tax_year)
        # 2–3 already in bare_log_number (strip season prefix + leading zeros)
        # 4–5 empty or out of range
        if not bare:
            detail = {
                "invoice": inv,
                "bare_log": "",
                "length": len(inv),
                "name": name,
                "reason": "empty_bare_after_normalize",
                "season_prefix": prefix,
            }
            malformed.append({**detail, "source_row": r.source_row})
            report.findings.append({"type": "MALFORMED_LOG_NUMBER", "detail": detail})
            continue

        try:
            bare_int = int(bare)
        except ValueError:
            bare_int = -1

        if bare_int < 1 or (bare_log_max and bare_int > bare_log_max):
            detail = {
                "invoice": inv,
                "bare_log": bare,
                "length": len(inv),
                "name": name,
                "reason": "out_of_range",
                "bare_log_max": bare_log_max,
                "season_prefix": prefix,
            }
            # Preserve visibility for wrong-season prefixes that fail range
            if len(inv) >= 2 and not inv.startswith(prefix):
                detail["note"] = "raw_does_not_start_with_season_prefix"
            malformed.append({**detail, "source_row": r.source_row})
            report.findings.append({"type": "MALFORMED_LOG_NUMBER", "detail": detail})
            continue

        format_ok_raw.add(inv)
        bare_to_raw[bare].add(inv)
        slot = by_bare_people[bare].setdefault(
            person,
            {"display": name, "rows": 0, "raw_invoices": set()},
        )
        slot["rows"] += 1
        slot["raw_invoices"].add(inv)
        if not slot["display"] and name:
            slot["display"] = name

    # Dedupe malformed list by (invoice, bare, name) for report samples
    seen_m = set()
    malformed_dedup = []
    for m in malformed:
        k = (m.get("invoice"), m.get("bare_log"), m.get("name"))
        if k in seen_m:
            continue
        seen_m.add(k)
        malformed_dedup.append(m)
    report.malformed_invoices = malformed_dedup

    # ── C2 collisions in bare-log space ────────────────────────────────
    collisions: list[dict[str, Any]] = []
    collision_bares: set[str] = set()
    for bare, people in by_bare_people.items():
        if len(people) <= 1:
            continue
        claimants = []
        for person_key, info in sorted(people.items(), key=lambda kv: kv[1]["display"]):
            claimants.append(
                {
                    "name": info["display"],
                    "person_key": person_key,
                    "row_count": info["rows"],
                    "raw_invoices": sorted(info["raw_invoices"]),
                }
            )
        n_taxpayers = len(claimants)
        assert n_taxpayers == len(claimants), "n_taxpayers must equal len(claimants)"
        # Also assert distinct names list length
        names_only = [c["name"] for c in claimants]
        assert n_taxpayers == len(names_only)
        raws = sorted(bare_to_raw.get(bare, set()))
        payload = {
            "bare_log": bare,
            "invoice": raws[0] if len(raws) == 1 else None,
            "raw_invoices": raws,
            "n_taxpayers": n_taxpayers,
            "claimants": claimants,
            # flat names for quick display (deduped)
            "claimant_names": names_only,
        }
        assert payload["n_taxpayers"] == len(payload["claimants"])
        collisions.append(payload)
        collision_bares.add(bare)
        report.findings.append({"type": "LOG_NUMBER_COLLISION", "detail": payload})

    report.collisions = sorted(collisions, key=lambda x: -x["n_taxpayers"])
    report.bare_to_raw = {k: sorted(v) for k, v in bare_to_raw.items()}

    # L0-eligible: format-ok raw invoices whose bare is not a collision
    l0_ok: set[str] = set()
    l0_ok_bare: set[str] = set()
    l0_blocked: set[str] = set()
    for inv in format_ok_raw:
        bare = bare_log_number(inv, tax_year)
        if bare in collision_bares:
            l0_blocked.add(inv)
        else:
            l0_ok.add(inv)
            l0_ok_bare.add(bare)
    # Also block raws that were malformed
    for m in malformed_dedup:
        if m.get("invoice"):
            l0_blocked.add(m["invoice"])

    report.l0_ok_invoices = l0_ok
    report.l0_ok_bare = l0_ok_bare
    report.l0_blocked_invoices = l0_blocked
    report.valid_l0_invoice_count = len(l0_ok)
    report.valid_l0_bare_count = len(l0_ok_bare)

    return report


def report_to_jsonable(report: InvoiceExportReport) -> dict[str, Any]:
    """Drop heavy row list for baseline_json; keep counts + samples."""
    d = asdict(report)
    d.pop("rows", None)
    d["l0_ok_invoices"] = sorted(report.l0_ok_invoices)
    d["l0_ok_bare"] = sorted(report.l0_ok_bare)
    d["l0_blocked_invoices"] = sorted(report.l0_blocked_invoices)
    d["bare_to_raw"] = {k: v for k, v in list(report.bare_to_raw.items())[:80]}
    if len(d.get("findings") or []) > 50:
        d["findings_truncated"] = len(d["findings"])
        d["findings"] = d["findings"][:50]
    d["malformed_invoices"] = report.malformed_invoices[:40]
    d["proforma_stale"] = report.proforma_stale[:40]
    # Serialize collisions with assertion visible
    d["collisions"] = report.collisions[:40]
    for c in d["collisions"]:
        assert c["n_taxpayers"] == len(c["claimants"])
    return d
