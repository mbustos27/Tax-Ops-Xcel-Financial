"""A1 — Identity resolution ladder (Amendment 1: L0 three-way on invoice/log#).

Findings-only: reads TaxOps snapshot RO; writes entity_link into audit DB only.
"""

from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

from rapidfuzz import fuzz

from audit.db import connect_audit, connect_taxops_readonly
from audit.invoice_export import (
    DEFAULT_TAXPAYER_INVOICE_PATH,
    InvoiceExportReport,
    bare_log_number,
    parse_taxpayer_invoice_csv,
)
from audit.normalizer import normalize_person
from audit.util import dumps, utc_now

# Audit accepts at 90 (align with prefill). Import uses 88 — report 88–89 band separately.
AUDIT_FUZZY_ACCEPT = 90
IMPORT_FUZZY_ACCEPT = 88
FUZZY_REVIEW_LOW = 85

A1_REPORT_PATH = Path(r"T:\audit\investigation\A1-ladder.md")


ENTITY_LINK_DDL = """
CREATE TABLE IF NOT EXISTS entity_link (
  id            INTEGER PRIMARY KEY,
  run_id        INTEGER NOT NULL REFERENCES audit_run(id),
  left_kind     TEXT NOT NULL,
  left_key      TEXT NOT NULL,
  right_kind    TEXT NOT NULL,
  right_key     TEXT NOT NULL,
  tier          TEXT NOT NULL,
  score         REAL,
  evidence_json TEXT NOT NULL,
  confidence    REAL NOT NULL,
  needs_human   INTEGER NOT NULL DEFAULT 0,
  UNIQUE(run_id, left_kind, left_key, right_kind, right_key, tier)
);
CREATE INDEX IF NOT EXISTS idx_entity_link_run_tier ON entity_link(run_id, tier);
"""


@dataclass
class Link:
    left_kind: str
    left_key: str
    right_kind: str
    right_key: str
    tier: str
    score: Optional[float]
    evidence: dict[str, Any]
    confidence: float
    needs_human: bool


@dataclass
class LadderStats:
    l0_raw_invoice_keys: int = 0
    l0_after_format: int = 0
    l0_after_prefix: int = 0
    l0_after_collision: int = 0
    l0_drake_taxops: int = 0
    l0_drake_log: int = 0
    l0_taxops_log: int = 0
    # Distinct L0-eligible invoice keys in each intersection
    l0_three_way: int = 0
    l0_drake_taxops_not_log: int = 0
    l0_drake_log_not_taxops: int = 0
    l0_drake_neither: int = 0
    l0_taxops_log_not_drake: int = 0
    taxops_ty_with_log: int = 0
    log_named_with_digits: int = 0
    l1: int = 0
    l2: int = 0
    l3: int = 0
    l4: int = 0
    l5_unmatched_drake: int = 0
    band_88_89: int = 0
    log_restart_cut: int = 0
    log_all_bares: int = 0
    log_canonical_bares: int = 0
    csm_export_lag_note: str = ""
    jul31_venn_note: str = ""
    audit_db: str = ""
    run_id: int = 0
    samples: dict[str, list[dict[str, Any]]] = field(default_factory=dict)


def _ensure_entity_link(conn: sqlite3.Connection) -> None:
    conn.executescript(ENTITY_LINK_DDL)
    conn.commit()


def order_normalize_tokens(name: str) -> str:
    """Canonical bag for fuzzy: upper, strip punct, split joint &, sort tokens."""
    s = (name or "").upper()
    s = s.replace("&", " ")
    s = re.sub(r"[^A-Z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    if not s:
        return ""
    return " ".join(sorted(s.split()))


def _person_display(last: str, first: str) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


def run_ladder(
    *,
    run_id: int,
    audit_db: Path,
    taxops_snapshot: Path,
    invoice_path: Path = DEFAULT_TAXPAYER_INVOICE_PATH,
    tax_year: int = 2025,
) -> tuple[LadderStats, list[Link]]:
    inv = parse_taxpayer_invoice_csv(invoice_path)
    stats = LadderStats()
    links: list[Link] = []

    # Collapse invoice export to one row per *bare log* for L0 (C1/C2).
    # Alternate raw paddings (250141 / 25141) share one bare key.
    by_inv: dict[str, dict[str, Any]] = {}  # keyed by bare log#
    all_full_invs = {r.invoice for r in inv.rows if r.is_full_width and r.invoice}
    stats.l0_raw_invoice_keys = len(all_full_invs)
    # Format gate = C1 bare-log validation (not legacy ^\d{6}$)
    stats.l0_after_format = len(inv.l0_ok_invoices) + sum(
        1
        for c in inv.collisions
        for _ in c.get("raw_invoices") or ([c["invoice"]] if c.get("invoice") else [])
    )
    # Prefer explicit: format-ok = L0-ok ∪ collision raws
    format_ok_n = len(
        set(inv.l0_ok_invoices)
        | {
            raw
            for c in inv.collisions
            for raw in (c.get("raw_invoices") or [])
        }
    )
    stats.l0_after_format = format_ok_n
    stats.l0_after_prefix = format_ok_n  # season strip is part of bare normalize
    l0_ok = set(inv.l0_ok_invoices)
    stats.l0_after_collision = len(l0_ok)

    # Build representative taxpayer per bare log (prefer 6-digit raw invoice)
    for r in inv.rows:
        if not r.is_full_width or r.invoice not in l0_ok:
            continue
        bare = r.bare_log or bare_log_number(r.invoice, tax_year)
        if not bare:
            continue
        prev = by_inv.get(bare)
        prefer = (
            prev is None
            or (len(r.invoice) == 6 and len(prev.get("invoice") or "") != 6)
        )
        if prefer:
            by_inv[bare] = {
                "invoice": r.invoice,
                "first": r.first,
                "last": r.last,
                "is_entity": r.is_entity,
                "source_row": r.source_row,
                "bare_log": bare,
            }

    def _excel_log_cell(v: Any) -> str:
        """Avoid float→'141.0' digit mangling from openpyxl."""
        if v is None:
            return ""
        if isinstance(v, bool):
            return ""
        if isinstance(v, int):
            return str(v)
        if isinstance(v, float) and v == int(v):
            return str(int(v))
        return str(v).strip()

    # TaxOps returns with log numbers for tax_year — index by bare log#
    tconn = connect_taxops_readonly(taxops_snapshot)
    try:
        taxops_by_log: dict[str, list[dict]] = defaultdict(list)
        for row in tconn.execute(
            """
            SELECT r.id AS return_id, r.client_id, r.log_number, r.tax_year, r.client_status,
                   c.last_name, c.first_name, c.ssn_last4
            FROM returns r
            JOIN clients c ON c.id = r.client_id
            WHERE r.tax_year = ?
              AND r.log_number IS NOT NULL AND TRIM(r.log_number) != ''
            """,
            (tax_year,),
        ):
            bare = bare_log_number(str(row["log_number"]), tax_year)
            if bare:
                d = dict(row)
                d["bare_log"] = bare
                taxops_by_log[bare].append(d)

        clients = [
            dict(r)
            for r in tconn.execute(
                "SELECT id, last_name, first_name, ssn_last4, display_name FROM clients "
                "WHERE COALESCE(is_test, 0)=0"
                if "is_test"
                in {r[1] for r in tconn.execute("PRAGMA table_info(clients)")}
                else "SELECT id, last_name, first_name, ssn_last4, display_name FROM clients"
            )
        ]
    finally:
        tconn.close()

    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.tax_log_index import canonical_log_by_num, load_tax_log_index

    mem = load_baseline_memory()
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)
    log_by_num: dict[str, list[dict]] = defaultdict(list)
    log_restart_cut = None
    if log_path.exists():
        idx = load_tax_log_index(log_path, tax_year=tax_year, require_season_for_canonical=True)
        log_by_num = canonical_log_by_num(idx)
        stats.log_restart_cut = idx.restart_cut
        stats.log_canonical_bares = len(idx.canonical_bares)
        stats.log_all_bares = len(idx.all_bares)

    # ── L0: Drake invoice ↔ TaxOps log_number (bare key) ───────────────
    for bare, meta in by_inv.items():
        inv_no = meta["invoice"]
        hits = taxops_by_log.get(bare) or []
        if len(hits) == 1:
            h = hits[0]
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{tax_year}|{meta['source_row']}",
                    right_kind="taxops_return",
                    right_key=str(h["return_id"]),
                    tier="L0",
                    score=1.0,
                    evidence={
                        "invoice": inv_no,
                        "bare_log": bare,
                        "tax_year": tax_year,
                        "log_number": h["log_number"],
                        "client_id": h["client_id"],
                        "drake_name": _person_display(meta["last"], meta["first"]),
                        "taxops_name": _person_display(h["last_name"], h["first_name"]),
                        "guards": [
                            "format",
                            "season_prefix",
                            "not_collision",
                            "tax_year",
                            "bare_log_normalize",
                        ],
                    },
                    confidence=1.0,
                    needs_human=False,
                )
            )
            stats.l0_drake_taxops += 1
        elif len(hits) > 1:
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{tax_year}",
                    right_kind="taxops_return",
                    right_key=",".join(str(h["return_id"]) for h in hits),
                    tier="L4",
                    score=None,
                    evidence={
                        "invoice": inv_no,
                        "bare_log": bare,
                        "reason": "multiple_taxops_returns_same_log_year",
                        "return_ids": [h["return_id"] for h in hits],
                    },
                    confidence=0.5,
                    needs_human=True,
                )
            )
            stats.l4 += 1

    # ── L0: Drake invoice ↔ Tax Log ────────────────────────────────────
    for bare, meta in by_inv.items():
        inv_no = meta["invoice"]
        hits = log_by_num.get(bare) or []
        if len(hits) == 1:
            h = hits[0]
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{tax_year}|{meta['source_row']}",
                    right_kind="tax_log",
                    right_key=f"{h['log']}|{h['source_row']}",
                    tier="L0",
                    score=1.0,
                    evidence={
                        "invoice": inv_no,
                        "bare_log": bare,
                        "tax_year": tax_year,
                        "drake_name": _person_display(meta["last"], meta["first"]),
                        "log_name": _person_display(h["last"], h["first"]),
                    },
                    confidence=1.0,
                    needs_human=False,
                )
            )
            stats.l0_drake_log += 1
        elif len(hits) > 1:
            h = hits[0]
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{tax_year}|{meta['source_row']}",
                    right_kind="tax_log",
                    right_key=f"{h['log']}|multi",
                    tier="L0",
                    score=1.0,
                    evidence={
                        "invoice": inv_no,
                        "bare_log": bare,
                        "note": "multiple_log_rows_same_number",
                        "n_log_rows": len(hits),
                    },
                    confidence=0.95,
                    needs_human=False,
                )
            )
            stats.l0_drake_log += 1

    # ── L0: TaxOps ↔ Tax Log on log_number ─────────────────────────────
    for logn, rets in taxops_by_log.items():
        hits = log_by_num.get(logn) or []
        if not hits:
            continue
        for ret in rets:
            h = hits[0]
            links.append(
                Link(
                    left_kind="taxops_return",
                    left_key=str(ret["return_id"]),
                    right_kind="tax_log",
                    right_key=f"{h['log']}|{h['source_row']}",
                    tier="L0",
                    score=1.0,
                    evidence={
                        "log_number": logn,
                        "tax_year": tax_year,
                        "taxops_name": _person_display(ret["last_name"], ret["first_name"]),
                        "log_name": _person_display(h["last"], h["first"]),
                    },
                    confidence=1.0,
                    needs_human=False,
                )
            )
            stats.l0_taxops_log += 1

    # Distinct-key Venn on bare log numbers (season prefix stripped)
    taxops_keys = set(taxops_by_log.keys())
    log_keys = set(log_by_num.keys())
    drake_bare = {bare_log_number(i, tax_year) for i in l0_ok}
    stats.taxops_ty_with_log = len(taxops_keys)
    stats.log_named_with_digits = len(log_keys)
    stats.l0_three_way = len(drake_bare & taxops_keys & log_keys)
    stats.l0_drake_taxops_not_log = len((drake_bare & taxops_keys) - log_keys)
    stats.l0_drake_log_not_taxops = len((drake_bare & log_keys) - taxops_keys)
    stats.l0_drake_neither = len(drake_bare - taxops_keys - log_keys)
    stats.l0_taxops_log_not_drake = len((taxops_keys & log_keys) - drake_bare)
    stats.jul31_venn_note = (
        "Jul31 name-based CSM Venn (Desktop 1155): all3=1009, taxops_not_log=126, "
        "log_not_taxops=13, neither=7. A1 L0 is bare-log three-way "
        f"(Drake invoice `25xxxx` ↔ TaxOps/Log `xxxx`) on L0-eligible={len(l0_ok)} "
        f"bare_keys={len(drake_bare)}; three_way={stats.l0_three_way}, "
        f"drake∩taxops\\log={stats.l0_drake_taxops_not_log}, "
        f"drake∩log\\taxops={stats.l0_drake_log_not_taxops}, "
        f"drake_neither={stats.l0_drake_neither}."
    )

    linked_inv = {
        lk.left_key.split("|")[0]
        for lk in links
        if lk.tier == "L0" and lk.left_kind == "drake_invoice"
    }
    linked_clients = {
        int(lk.evidence["client_id"])
        for lk in links
        if lk.tier == "L0"
        and lk.left_kind == "drake_invoice"
        and "client_id" in lk.evidence
    }

    # Index TaxOps for L1/L2
    taxops_by_last4: dict[str, list] = defaultdict(list)
    taxops_by_surname: dict[str, list] = defaultdict(list)
    taxops_by_key: dict[tuple[str, str], list] = defaultdict(list)
    for c in clients:
        l4 = (c["ssn_last4"] or "").strip()
        if l4 and len(l4) == 4 and l4.isdigit():
            taxops_by_last4[l4].append(c)
        n = normalize_person(c["last_name"] or "", c["first_name"] or "")
        for sur, given in n.match_keys:
            taxops_by_surname[sur].append(c)
            taxops_by_key[(sur, given)].append(c)

    # ── L1: CSM last4 + normalized surname → TaxOps (never last4 alone) ─
    from audit.baseline import CSM_CANDIDATES
    import openpyxl

    csm_path = Path(mem.get("authoritative_drake_path") or "")
    if not csm_path.exists():
        csm_path = CSM_CANDIDATES["onedrive_ty2025"]
    csm_names: list[tuple[str, str]] = []  # (raw, last4)
    if csm_path.exists():
        wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
        ws = wb.active
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if i == 1:
                continue
            vals = list(row)
            name = str(vals[1] or "").strip() if len(vals) > 1 else ""
            if not name or name.upper().startswith("TOTAL"):
                continue
            last4 = str(vals[0] or "").strip() if vals else ""
            # CSM stores ID as last4 (sometimes with formatting)
            last4 = re.sub(r"\D", "", last4)[-4:] if last4 else ""
            csm_names.append((name, last4))
        wb.close()

    stats.csm_export_lag_note = (
        f"Authoritative CSM `{csm_path}` rows={len(csm_names)}; "
        f"invoice export full-width={inv.full_width_count} / data={inv.data_row_count}. "
        "CSM is later by Last Change through 07/30; TAXPAYER.csv As-of 08-10 — "
        "invoice export is newer for keys/names; CSM still supplies last4 for L1."
    )

    # Parse CSM "LAST, FIRST [& SPOUSE]" into person for surname
    for csm_raw, last4 in csm_names:
        if not last4 or len(last4) != 4:
            continue
        # Split joint at &
        primary = csm_raw.split("&")[0].strip()
        if "," in primary:
            last_part, first_part = primary.split(",", 1)
        else:
            last_part, first_part = primary, ""
        n = normalize_person(last_part.strip(), first_part.strip())
        if not n.match_keys:
            continue
        sur = n.match_keys[0][0]
        cands = [
            c
            for c in taxops_by_last4.get(last4, [])
            if any(mk[0] == sur for mk in normalize_person(c["last_name"] or "", c["first_name"] or "").match_keys)
        ]
        # Dedup candidates
        seen_ids = set()
        uniq = []
        for c in cands:
            if c["id"] in seen_ids:
                continue
            seen_ids.add(c["id"])
            uniq.append(c)
        if len(uniq) == 1 and uniq[0]["id"] not in linked_clients:
            c = uniq[0]
            links.append(
                Link(
                    left_kind="drake_csm",
                    left_key=f"{last4}|{csm_raw[:60]}",
                    right_kind="taxops_client",
                    right_key=str(c["id"]),
                    tier="L1",
                    score=1.0,
                    evidence={
                        "csm_name": csm_raw,
                        "last4": last4,
                        "surname": sur,
                        "taxops_name": _person_display(c["last_name"], c["first_name"]),
                        "rule": "last4+surname",
                    },
                    confidence=0.95,
                    needs_human=False,
                )
            )
            stats.l1 += 1
            linked_clients.add(c["id"])
        elif len(uniq) > 1:
            links.append(
                Link(
                    left_kind="drake_csm",
                    left_key=f"{last4}|{csm_raw[:60]}",
                    right_kind="taxops_client",
                    right_key=",".join(str(c["id"]) for c in uniq[:5]),
                    tier="L4",
                    score=None,
                    evidence={
                        "reason": "last4+surname_ambiguous",
                        "last4": last4,
                        "surname": sur,
                        "n": len(uniq),
                    },
                    confidence=0.6,
                    needs_human=True,
                )
            )
            stats.l4 += 1

    # ── L2: exact match_keys (invoice name → TaxOps) for non-L0 invoices ─
    for bare, meta in by_inv.items():
        inv_no = meta["invoice"]
        if inv_no in linked_inv or bare in linked_inv or meta["is_entity"]:
            continue
        n = normalize_person(meta["last"], meta["first"])
        hits: list = []
        for mk in n.match_keys:
            for c in taxops_by_key.get(mk, []):
                if c["id"] not in linked_clients:
                    hits.append(c)
        # unique
        seen = set()
        uniq = []
        for c in hits:
            if c["id"] in seen:
                continue
            seen.add(c["id"])
            uniq.append(c)
        if len(uniq) == 1:
            c = uniq[0]
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{meta['source_row']}",
                    right_kind="taxops_client",
                    right_key=str(c["id"]),
                    tier="L2",
                    score=1.0,
                    evidence={
                        "method": "exact_match_keys",
                        "drake": _person_display(meta["last"], meta["first"]),
                        "taxops": _person_display(c["last_name"], c["first_name"]),
                        "keys": [list(k) for k in n.match_keys[:3]],
                        "bare_log": bare,
                    },
                    confidence=0.9,
                    needs_human=False,
                )
            )
            stats.l2 += 1
            linked_clients.add(c["id"])
            linked_inv.add(inv_no)
            linked_inv.add(bare)

    # ── L3 truncation: CSM names ≥39 vs invoice export longer names ────
    # Sources actually wired: authoritative CSM ↔ TAXPAYER.csv only.
    # Not wired: purple CSM alternate, Tax Log named rows.
    # Amendment 2 C6: invoice first/last max 35/17 with zero rows at 39–40, so
    # CSM joint/truncated strings are usually *longer* than invoice display —
    # forward prefix rescue yields ~0 links. Matching is L0/L1's job; NAME_TRUNCATED
    # is informational (see A5 / disposition).

    # Index invoice names as FIRST LAST and LAST, FIRST forms
    inv_name_list = []
    for r in inv.rows:
        if not r.is_full_width:
            continue
        disp = _person_display(r.last, r.first)
        bag = order_normalize_tokens(f"{r.first} {r.last}")
        inv_name_list.append({"invoice": r.invoice, "display": disp, "bag": bag, "last": r.last, "first": r.first})

    for csm_raw, last4 in csm_names:
        csm_norm = re.sub(r"\s+", " ", csm_raw.upper().strip())
        if len(csm_norm) < 39:
            continue
        # Find invoice display where csm_norm is prefix of a longer normalized form
        # Build CSM as possible LAST, FIRST — compare to invoice LAST, FIRST
        for inv_rec in inv_name_list:
            longer = inv_rec["display"].upper()
            longer2 = f"{inv_rec['first']} {inv_rec['last']}".upper().strip()
            hit = None
            if len(longer) > len(csm_norm) and longer.startswith(csm_norm):
                hit = longer
            elif len(longer2) > len(csm_norm) and longer2.startswith(csm_norm):
                hit = longer2
            # Also: CSM truncated joint vs invoice
            csm_bag = order_normalize_tokens(csm_raw)
            if not hit and csm_bag and inv_rec["bag"].startswith(csm_bag) and len(inv_rec["bag"]) > len(csm_bag):
                hit = inv_rec["bag"]
            if not hit:
                continue
            # Corroborate with last4 or surname
            sur_ok = False
            if inv_rec["last"] and inv_rec["last"].upper() in csm_norm:
                sur_ok = True
            last4_ok = False
            # find taxops client with this last4
            if last4:
                for c in clients:
                    if (c["ssn_last4"] or "") == last4:
                        last4_ok = True
                        break
            if sur_ok or last4_ok:
                links.append(
                    Link(
                        left_kind="drake_csm",
                        left_key=csm_norm[:80],
                        right_kind="drake_invoice",
                        right_key=inv_rec["invoice"] or inv_rec["display"],
                        tier="L3",
                        score=1.0,
                        evidence={
                            "csm_name": csm_raw,
                            "csm_len": len(csm_norm),
                            "longer": hit,
                            "invoice": inv_rec["invoice"],
                            "corroboration": "surname" if sur_ok else "last4",
                        },
                        confidence=0.92,
                        needs_human=False,
                    )
                )
                stats.l3 += 1
                break
            else:
                links.append(
                    Link(
                        left_kind="drake_csm",
                        left_key=csm_norm[:80],
                        right_kind="drake_invoice",
                        right_key=inv_rec["invoice"] or inv_rec["display"],
                        tier="L4",
                        score=None,
                        evidence={
                            "csm_name": csm_raw,
                            "longer": hit,
                            "reason": "truncation_prefix_without_corroboration",
                        },
                        confidence=0.7,
                        needs_human=True,
                    )
                )
                stats.l4 += 1
                break

    # ── L4/L2 fuzzy for remaining unmatched invoice taxpayers ──────────
    # Surname-token blocking to avoid O(n×m) full scan.
    client_bags: list[tuple[dict, str, set[str]]] = []
    for c in clients:
        bag = order_normalize_tokens(f"{c['first_name'] or ''} {c['last_name'] or ''}")
        toks = set(bag.split()) if bag else set()
        client_bags.append((c, bag, toks))

    for bare, meta in by_inv.items():
        inv_no = meta["invoice"]
        if inv_no in linked_inv or bare in linked_inv:
            continue
        if meta["is_entity"]:
            target = order_normalize_tokens(meta["last"])
            best = None
            best_s = 0
            for c, bag, toks in client_bags:
                if c["first_name"]:
                    continue
                s = fuzz.token_sort_ratio(target, order_normalize_tokens(c["last_name"] or ""))
                if s > best_s:
                    best_s = s
                    best = c
            if best and FUZZY_REVIEW_LOW <= best_s < AUDIT_FUZZY_ACCEPT:
                if IMPORT_FUZZY_ACCEPT <= best_s < AUDIT_FUZZY_ACCEPT:
                    stats.band_88_89 += 1
                links.append(
                    Link(
                        left_kind="drake_invoice",
                        left_key=f"{inv_no}|entity",
                        right_kind="taxops_client",
                        right_key=str(best["id"]),
                        tier="L4",
                        score=float(best_s),
                        evidence={"entity": meta["last"], "score": best_s, "bare_log": bare},
                        confidence=best_s / 100.0,
                        needs_human=True,
                    )
                )
                stats.l4 += 1
            elif best and best_s >= AUDIT_FUZZY_ACCEPT:
                links.append(
                    Link(
                        left_kind="drake_invoice",
                        left_key=f"{inv_no}|entity",
                        right_kind="taxops_client",
                        right_key=str(best["id"]),
                        tier="L2",
                        score=float(best_s),
                        evidence={"entity_exactish": meta["last"], "score": best_s, "bare_log": bare},
                        confidence=0.9,
                        needs_human=False,
                    )
                )
                stats.l2 += 1
                linked_clients.add(best["id"])
                linked_inv.add(inv_no)
                linked_inv.add(bare)
            else:
                stats.l5_unmatched_drake += 1
            continue

        probe = order_normalize_tokens(f"{meta['first']} {meta['last']}")
        probe_toks = set(probe.split()) if probe else set()
        best = None
        best_s = 0
        # Prefer candidates sharing a surname/given token
        blocked = [
            (c, bag)
            for c, bag, toks in client_bags
            if c["id"] not in linked_clients and (not probe_toks or toks & probe_toks)
        ]
        if not blocked:
            blocked = [
                (c, bag)
                for c, bag, toks in client_bags
                if c["id"] not in linked_clients
            ]
        for c, bag in blocked:
            s = fuzz.token_sort_ratio(probe, bag)
            if s > best_s:
                best_s = s
                best = c
                if best_s == 100:
                    break
        if best is None:
            stats.l5_unmatched_drake += 1
            continue
        if FUZZY_REVIEW_LOW <= best_s < AUDIT_FUZZY_ACCEPT:
            if IMPORT_FUZZY_ACCEPT <= best_s < AUDIT_FUZZY_ACCEPT:
                stats.band_88_89 += 1
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{meta['source_row']}",
                    right_kind="taxops_client",
                    right_key=str(best["id"]),
                    tier="L4",
                    score=float(best_s),
                    evidence={
                        "drake": _person_display(meta["last"], meta["first"]),
                        "taxops": _person_display(best["last_name"], best["first_name"]),
                        "score": best_s,
                        "threshold_audit": AUDIT_FUZZY_ACCEPT,
                        "threshold_import": IMPORT_FUZZY_ACCEPT,
                        "bare_log": bare,
                    },
                    confidence=best_s / 100.0,
                    needs_human=True,
                )
            )
            stats.l4 += 1
        elif best_s >= AUDIT_FUZZY_ACCEPT:
            links.append(
                Link(
                    left_kind="drake_invoice",
                    left_key=f"{inv_no}|{meta['source_row']}",
                    right_kind="taxops_client",
                    right_key=str(best["id"]),
                    tier="L2",
                    score=float(best_s),
                    evidence={
                        "method": "order_normalized_token_sort",
                        "score": best_s,
                        "drake": _person_display(meta["last"], meta["first"]),
                        "taxops": _person_display(best["last_name"], best["first_name"]),
                        "bare_log": bare,
                    },
                    confidence=0.88,
                    needs_human=False,
                )
            )
            stats.l2 += 1
            linked_clients.add(best["id"])
            linked_inv.add(inv_no)
            linked_inv.add(bare)
        else:
            stats.l5_unmatched_drake += 1

    # Samples per tier
    by_tier: dict[str, list[Link]] = defaultdict(list)
    for lk in links:
        by_tier[lk.tier].append(lk)
    for tier, lst in by_tier.items():
        stats.samples[tier] = [
            {
                "left": f"{x.left_kind}:{x.left_key}",
                "right": f"{x.right_kind}:{x.right_key}",
                "score": x.score,
                "evidence": x.evidence,
                "needs_human": x.needs_human,
            }
            for x in lst[:10]
        ]

    # Persist
    aconn = connect_audit(audit_db)
    try:
        _ensure_entity_link(aconn)
        aconn.execute("DELETE FROM entity_link WHERE run_id=?", (run_id,))
        for lk in links:
            # Guard: never last4-only evidence
            ev = lk.evidence
            if set(ev.keys()) <= {"last4", "ssn_last4", "csm_ssn_last4"}:
                continue
            aconn.execute(
                """
                INSERT OR REPLACE INTO entity_link (
                  run_id, left_kind, left_key, right_kind, right_key,
                  tier, score, evidence_json, confidence, needs_human
                ) VALUES (?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    run_id,
                    lk.left_kind,
                    lk.left_key,
                    lk.right_kind,
                    lk.right_key,
                    lk.tier,
                    lk.score,
                    dumps(lk.evidence),
                    lk.confidence,
                    1 if lk.needs_human else 0,
                ),
            )
        aconn.commit()
    finally:
        aconn.close()

    stats.audit_db = str(audit_db)
    stats.run_id = run_id
    return stats, links


def write_a1_report(stats: LadderStats, dest: Path = A1_REPORT_PATH) -> Path:
    lines: list[str] = []
    A = lines.append
    A("# A1 — Identity resolution ladder")
    A("")
    A(f"_Generated: {utc_now()}_")
    A(f"_Audit DB: `{stats.audit_db}` run_id={stats.run_id}_")
    A("")
    A("## Threshold policy")
    A("")
    A(f"- Audit auto-accept fuzzy: **{AUDIT_FUZZY_ACCEPT}** (aligned with prefill)")
    A(f"- Import accept (reference): **{IMPORT_FUZZY_ACCEPT}**")
    A(f"- Human review band: **{FUZZY_REVIEW_LOW}–{AUDIT_FUZZY_ACCEPT - 1}**")
    A(f"- Pairs in disagreement band 88–89 this run: **{stats.band_88_89}**")
    A("")
    A("## L0 precondition funnel (Drake invoice keys)")
    A("")
    A("| Stage | Distinct invoices |")
    A("|---|---:|")
    A(f"| Full-width raw keys | {stats.l0_raw_invoice_keys} |")
    A(f"| After bare-log format/range (C1) | {stats.l0_after_format} |")
    A(f"| After normalize (season strip in bare) | {stats.l0_after_prefix} |")
    A(f"| After bare-log collision exclusion (L0-eligible raw) | {stats.l0_after_collision} |")
    A("")
    A("## L0 three-way Venn (distinct invoice/log keys)")
    A("")
    A("| Cell | Count |")
    A("|---|---:|")
    A(f"| Drake ∩ TaxOps ∩ Log | {stats.l0_three_way} |")
    A(f"| Drake ∩ TaxOps \\ Log | {stats.l0_drake_taxops_not_log} |")
    A(f"| Drake ∩ Log \\ TaxOps | {stats.l0_drake_log_not_taxops} |")
    A(f"| Drake only (neither) | {stats.l0_drake_neither} |")
    A(f"| TaxOps ∩ Log \\ Drake L0-eligible | {stats.l0_taxops_log_not_drake} |")
    A(f"| TaxOps TY keys with log# | {stats.taxops_ty_with_log} |")
    A(f"| Tax Log named digit keys | {stats.log_named_with_digits} |")
    A("")
    A(stats.jul31_venn_note)
    A("")
    A("## CSM export lag")
    A("")
    A(stats.csm_export_lag_note)
    A("")
    A("## Per-tier link counts")
    A("")
    A("| Tier | Rule | Links |")
    A("|---|---|---:|")
    A(f"| L0 Drake↔TaxOps | (invoice, tax_year) | {stats.l0_drake_taxops} |")
    A(f"| L0 Drake↔Log | (invoice, tax_year) | {stats.l0_drake_log} |")
    A(f"| L0 TaxOps↔Log | (log_number, tax_year) | {stats.l0_taxops_log} |")
    A(
        f"| Log index (info) | cut={stats.log_restart_cut}; "
        f"all={stats.log_all_bares}→YR25={stats.log_canonical_bares} | — |"
    )
    A(f"| L1 last4+surname | CSM ↔ TaxOps | {stats.l1} |")
    A(f"| L2 exact match_keys / high fuzzy≥90 | | {stats.l2} |")
    A(f"| L3 truncation prefix | CSM↔invoice only | {stats.l3} |")
    A(f"| L4 human review | | {stats.l4} |")
    A(f"| L5 unmatched Drake invoice (L0-eligible leftover) | | {stats.l5_unmatched_drake} |")
    A("")
    A("## C6 — L3 truncation diagnosis")
    A("")
    A("- **Wired sources:** authoritative CSM (≥39-char display) ↔ `TAXPAYER.csv` invoice "
      "first/last only. **Not** purple CSM, **not** Tax Log.")
    A("- **Verdict:** L3 is wired but ineffective for joint/truncated CSM strings (invoice "
      "names are shorter or differently ordered; first/last max 35/17, zero rows at 39–40). "
      "Identity for these rows is carried by **L0 invoice keys** + L1 last4+surname. "
      "`NAME_TRUNCATED` is reclassified **informational** (dropped from Priority worklist).")
    A(f"- L3 link count this run: **{stats.l3}**")
    A("")
    A("## Samples (≤10 per tier)")
    A("")
    for tier, samples in sorted(stats.samples.items()):
        A(f"### {tier}")
        A("")
        for s in samples:
            A(f"- `{s['left']}` → `{s['right']}` score={s['score']} human={s['needs_human']}")
            A(f"  evidence: `{dumps(s['evidence'])[:240]}`")
        A("")
    A("## Notes")
    A("")
    A("- L0 compares **bare log numbers**: Drake Invoice `250141` ≡ TaxOps/Log `141` "
      "(strip season prefix `25` + leading zeros). Raw invoice still recorded in evidence.")
    A("- L0 never matches across tax years; tax_year is required on the TaxOps side.")
    A("- Malformed / collision / proforma-stale invoices are excluded from L0 (drop to name tiers).")
    A("- No link uses last-4 as sole evidence (L1 always pairs last4 with surname).")
    A("- Name precedence: TAXPAYER.csv first/last over CSM joint for identity; CSM for last4 + display.")
    A("- Fuzzy threshold chosen: **90** (prefill-aligned). Import's 88 is reported in the 88–89 band only.")
    A("")
    dest.parent.mkdir(parents=True, exist_ok=True)
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def run_a1_phase(
    *,
    operator: str = "audit",
    audit_db: Optional[Path] = None,
    taxops_path: Optional[Path] = None,
) -> tuple[LadderStats, Path]:
    """Create/open audit DB, insert audit_run, run ladder, write A1-ladder.md."""
    from audit import config
    from audit.baseline import load_baseline_memory
    from audit.db import audit_db_path, connect_audit
    from audit.util import sha256_file, stamp_day

    mem = load_baseline_memory()
    db_path = audit_db or audit_db_path(stamp_day())
    taxops = Path(taxops_path or mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")

    inv_path = Path(mem.get("taxpayer_invoice_path") or DEFAULT_TAXPAYER_INVOICE_PATH)
    drake = Path(mem.get("authoritative_drake_path") or config.DEFAULT_DRAKE_PATH)
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)

    aconn = connect_audit(db_path)
    try:
        cur = aconn.execute(
            """
            INSERT INTO audit_run (
              started_at, operator, tool_version, backup_verified, csm_unfiltered,
              drake_path, drake_sha256, tax_log_path, tax_log_sha256,
              taxops_snapshot, taxops_sha256, preflight_json, preflight_ok, notes,
              authoritative_taxops_path, taxops_path_resolution, csm_baseline_key
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """,
            (
                utc_now(),
                operator,
                "a1-ladder",
                1,
                1,
                str(drake),
                sha256_file(drake) if drake.exists() else "",
                str(log_path),
                sha256_file(log_path) if log_path.exists() else "",
                str(taxops),
                None,  # skip hashing live DB (large); RO URI used
                dumps({"phase": "A1", "invoice": str(inv_path)}),
                1,
                "A1 identity ladder (invoice L0 three-way)",
                str(taxops),
                mem.get("taxops_path_resolution") or "AUTHORITATIVE_DB_UNRESOLVED",
                mem.get("authoritative_drake_key"),
            ),
        )
        run_id = int(cur.lastrowid)
        aconn.commit()
    finally:
        aconn.close()

    stats, _links = run_ladder(
        run_id=run_id,
        audit_db=db_path,
        taxops_snapshot=taxops,
        invoice_path=inv_path,
        tax_year=2025,
    )
    report = write_a1_report(stats)
    aconn = connect_audit(db_path)
    try:
        aconn.execute(
            "UPDATE audit_run SET finished_at=? WHERE id=?",
            (utc_now(), run_id),
        )
        aconn.commit()
    finally:
        aconn.close()
    return stats, report


if __name__ == "__main__":
    st, path = run_a1_phase()
    print(
        f"A1 done run={st.run_id} report={path} "
        f"L0_3way={st.l0_three_way} L1={st.l1} L2={st.l2} L3={st.l3} L4={st.l4} L5={st.l5_unmatched_drake}"
    )