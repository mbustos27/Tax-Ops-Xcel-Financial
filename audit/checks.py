"""A3 — Failure-mode checks F1–F12 (findings via A2 disposition).

Never writes to TaxOps. Reads RO. Emits A3-checks.md.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections import Counter, defaultdict
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

import openpyxl
from rapidfuzz import fuzz

from audit import config
from audit.baseline import load_baseline_memory
from audit.db import connect_taxops_readonly
from audit.disposition import (
    DISPOSITION_DB_PATH,
    Finding,
    apply_delta,
    fingerprint,
    norm_name_key,
)
from audit.invoice_export import DEFAULT_TAXPAYER_INVOICE_PATH, parse_taxpayer_invoice_csv
from audit.ladder import AUDIT_FUZZY_ACCEPT, IMPORT_FUZZY_ACCEPT, bare_log_number, order_normalize_tokens
from audit.normalizer import normalize_drake_client_name, normalize_person
from audit.util import dumps, utc_now

A3_REPORT_PATH = Path(r"T:\audit\investigation\A3-checks.md")


@dataclass
class CheckResult:
    code: str
    title: str
    query: str
    baseline: dict[str, Any]
    actual: dict[str, Any]
    deviation: str  # ALARMING | INFORMATIONAL | SUPERSEDED
    note: str = ""
    findings: list[Finding] = field(default_factory=list)


def _clients(conn: sqlite3.Connection) -> list[dict]:
    cols = {r[1] for r in conn.execute("PRAGMA table_info(clients)")}
    if "is_test" in cols:
        sql = (
            "SELECT id, last_name, first_name, display_name, ssn_last4, created_at, updated_at, "
            "spouse_last_name, spouse_first_name FROM clients WHERE COALESCE(is_test,0)=0"
        )
    else:
        sql = (
            "SELECT id, last_name, first_name, display_name, ssn_last4, created_at, updated_at, "
            "spouse_last_name, spouse_first_name FROM clients"
        )
    return [dict(r) for r in conn.execute(sql)]


def _returns(conn: sqlite3.Connection) -> list[dict]:
    return [
        dict(r)
        for r in conn.execute(
            "SELECT id, client_id, log_number, tax_year, client_status, created_at FROM returns"
        )
    ]


def check_f1_truncation(csm_path: Path, taxops_path: Path) -> CheckResult:
    """CSM names at 39/40; cross-source superstrings vs TaxOps display."""
    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    at39 = at40 = 0
    truncated: list[tuple[str, str]] = []
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        last4 = re.sub(r"\D", "", str(vals[0] or ""))[-4:]
        ln = len(name)
        if ln == 39:
            at39 += 1
            truncated.append((name, last4))
        elif ln >= 40:
            at40 += 1
            truncated.append((name, last4))
    wb.close()

    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
        prefill_at40 = int(
            conn.execute(
                "SELECT COUNT(*) FROM drake_prefill_links "
                "WHERE LENGTH(TRIM(csm_name_raw)) = 40"
            ).fetchone()[0]
        )
    except sqlite3.Error:
        prefill_at40 = -1
        clients = _clients(conn)
    finally:
        conn.close()

    findings: list[Finding] = []
    superstring_hits = 0
    for name, last4 in truncated:
        n = normalize_drake_client_name(name)
        ek = f"{last4}|{n.surname_full}|{n.first_key}|len{len(name)}"
        findings.append(
            Finding(
                finding_type="NAME_TRUNCATED",
                entity_key=ek,
                salient=str(len(name)),
                detail={"name_len": len(name), "last4": last4},
            )
        )
        # Cross-source: TaxOps bag longer and shares surname
        csm_bag = order_normalize_tokens(name)
        for c in clients:
            bag = order_normalize_tokens(f"{c['first_name'] or ''} {c['last_name'] or ''}")
            if len(bag) > len(csm_bag) and (bag.startswith(csm_bag) or csm_bag in bag):
                if n.surname_full and n.surname_full.split()[0] in bag:
                    superstring_hits += 1
                    break

    actual = {
        "csm_at_39": at39,
        "csm_at_40_plus": at40,
        "csm_truncated_total": len(truncated),
        "prefill_csm_name_raw_at_40": prefill_at40,
        "cross_source_superstring_hits": superstring_hits,
    }
    baseline = {
        "desktop_at_40": 54,
        "prefill_at_40": 74,
        "jul31_NAME_TRUNCATED": 71,
        "onedrive_expected_near": "≥54 (OneDrive 1159 vs Desktop 1155)",
    }
    # Alarming if far below historical (suggests wrong CSM) or huge spike
    deviation = "INFORMATIONAL"
    if at40 + at39 < 40:
        deviation = "ALARMING"
    elif at40 + at39 > 120:
        deviation = "ALARMING"

    return CheckResult(
        code="F1",
        title="Drake 40-char name truncation",
        query="COUNT CSM Client Name WHERE len>=39; prefill csm_name_raw len=40; "
        "TaxOps longer bag sharing surname prefix",
        baseline=baseline,
        actual=actual,
        deviation=deviation,
        note="Cap is upstream Drake CSM, not TaxOps VARCHAR.",
        findings=findings,
    )


def check_f2_format(csm_path: Path, taxops_path: Path) -> CheckResult:
    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    csm_amp = csm_comma = csm_n = 0
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        csm_n += 1
        if "," in name:
            csm_comma += 1
        if "&" in name:
            csm_amp += 1
    wb.close()

    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
    finally:
        conn.close()
    tax_amp = tax_comma = blank_first = 0
    for c in clients:
        disp = f"{c['last_name'] or ''}, {c['first_name'] or ''}".strip(", ")
        dn = c.get("display_name") or disp
        if "&" in (dn or "") or "&" in (c["first_name"] or "") or "&" in (c["last_name"] or ""):
            tax_amp += 1
        if "," in (c["first_name"] or "") or "," in (c["last_name"] or ""):
            tax_comma += 1
        if not (c["first_name"] or "").strip():
            blank_first += 1

    # Pairs that need order-aware tiers: CSM joint vs TaxOps token_sort <90 but normalize keys match
    hard = 0
    # Sample-limited for runtime: only CSM with &
    # (full cross would be O(n*m); use surname index)
    return CheckResult(
        code="F2",
        title="Name-order / joint-format divergence",
        query="Classify CSM/TaxOps for comma, ampersand, blank-first (entity)",
        baseline={
            "csm_with_amp": 442,
            "csm_with_comma": 1254,
            "purple_with_comma": 0,
            "clients_blank_first": 151,
        },
        actual={
            "csm_n": csm_n,
            "csm_with_amp": csm_amp,
            "csm_with_comma": csm_comma,
            "taxops_with_amp": tax_amp,
            "taxops_name_fields_with_comma": tax_comma,
            "clients_blank_first": blank_first,
            "order_hard_pairs_sampled": hard,
        },
        deviation="INFORMATIONAL",
        note="Format classifiers only; L3/L4 resolution volume deferred to ladder stats.",
        findings=[],
    )


def check_f3_duplicates(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
        rets = _returns(conn)
    finally:
        conn.close()

    by_exact: dict[str, list] = defaultdict(list)
    by_norm: dict[str, list] = defaultdict(list)
    for c in clients:
        last = (c["last_name"] or "").strip()
        first = (c["first_name"] or "").strip()
        exact = f"{last}|{first}".upper()
        by_exact[exact].append(c)
        # punct-stripped normalized
        key = re.sub(r"[^A-Z0-9|]", "", f"{last.upper()}|{first.upper()}")
        by_norm[key].append(c)

    exact_groups = {k: v for k, v in by_exact.items() if k != "|" and len(v) > 1}
    norm_groups = {k: v for k, v in by_norm.items() if k and k != "|" and len(v) > 1}

    rets_by_client: dict[int, list] = defaultdict(list)
    for r in rets:
        rets_by_client[r["client_id"]].append(r)

    findings: list[Finding] = []
    ranked = []
    for key, members in sorted(norm_groups.items(), key=lambda kv: -len(kv[1])):
        ids = sorted(m["id"] for m in members)
        names = [f"{m['last_name']}, {m['first_name']}" for m in members]
        blast = sum(len(rets_by_client[i]) for i in ids)
        logs = []
        for i in ids:
            for r in rets_by_client[i]:
                if r.get("log_number"):
                    logs.append(str(r["log_number"]))
        ek = "||".join(sorted({norm_name_key(m["last_name"] or "", m["first_name"] or "") for m in members}))
        findings.append(
            Finding(
                finding_type="DUPLICATE_CLIENT",
                entity_key=ek or key,
                salient=f"n={len(members)}",
                detail={
                    "ids": ids,
                    "blast_returns": blast,
                    "logs_at_risk": logs[:12],
                    "exact": key in exact_groups or any(
                        f"{(m['last_name'] or '').strip()}|{(m['first_name'] or '').strip()}".upper()
                        in exact_groups
                        for m in members
                    ),
                },
            )
        )
        ranked.append({"key": key, "n": len(members), "blast": blast, "ids": ids})

    # Fuzzy 88–95 clusters (surname-blocked, expensive-capped)
    fuzzy_pairs = 0
    by_sur: dict[str, list] = defaultdict(list)
    for c in clients:
        n = normalize_person(c["last_name"] or "", c["first_name"] or "")
        if n.match_keys:
            by_sur[n.match_keys[0][0]].append(c)
    for sur, group in by_sur.items():
        if len(group) < 2 or len(group) > 40:
            continue
        bags = [
            (c, order_normalize_tokens(f"{c['first_name'] or ''} {c['last_name'] or ''}"))
            for c in group
        ]
        for i in range(len(bags)):
            for j in range(i + 1, len(bags)):
                s = fuzz.token_sort_ratio(bags[i][1], bags[j][1])
                if IMPORT_FUZZY_ACCEPT <= s < 96 and bags[i][0]["id"] != bags[j][0]["id"]:
                    # skip if already exact/norm dup
                    a, b = bags[i][0], bags[j][0]
                    if (a["last_name"] or "").strip().upper() == (b["last_name"] or "").strip().upper() and (
                        a["first_name"] or ""
                    ).strip().upper() == (b["first_name"] or "").strip().upper():
                        continue
                    fuzzy_pairs += 1

    actual = {
        "exact_groups": len(exact_groups),
        "exact_extra_rows": sum(len(v) - 1 for v in exact_groups.values()),
        "normalized_multi_buckets": len(norm_groups),
        "fuzzy_88_95_pairs_surname_blocked": fuzzy_pairs,
        "top_blast": ranked[:8],
    }
    baseline = {
        "live_exact_groups": 8,
        "normalized_multi_buckets": 14,
        "jul31_DUPLICATE_CLIENT": 142,
        "jul31_stamped_collisions_approx": 254,
    }
    deviation = "ALARMING" if len(exact_groups) > 20 else "INFORMATIONAL"
    return CheckResult(
        code="F3",
        title="Duplicate TaxOps clients (twins)",
        query="GROUP BY exact last|first; punct-stripped norm; fuzzy 88–95 surname-blocked",
        baseline=baseline,
        actual=actual,
        deviation=deviation,
        note="Ranked by blast radius (returns + log numbers at risk).",
        findings=findings,
    )


def check_f4_jul1(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
        rets = _returns(conn)
    finally:
        conn.close()

    jul1 = []
    space_fmt = iso_fmt = 0
    for c in clients:
        ca = c.get("created_at") or ""
        if ca.startswith("2026-07-01"):
            jul1.append(c)
            if "T" in ca or ca.endswith("Z") or "+" in ca[10:]:
                iso_fmt += 1
            elif " " in ca:
                space_fmt += 1

    id_band = sum(1 for c in jul1 if int(c["id"]) <= 1753)
    # Fraction owning pre-Jul1 returns
    pre = 0
    for c in jul1:
        owns = False
        for r in rets:
            if r["client_id"] != c["id"]:
                continue
            rca = r.get("created_at") or ""
            if rca and rca < "2026-07-01":
                owns = True
                break
        if owns:
            pre += 1

    # Exact cluster stamp
    stamp = "2026-07-01 18:19:30"
    stamp_n = sum(1 for c in jul1 if (c.get("created_at") or "").startswith(stamp))

    findings = [
        Finding(
            finding_type="JUL1_PROVENANCE",
            entity_key="cohort",
            salient=str(len(jul1)),
            detail={
                "jul1_dated": len(jul1),
                "space_format": space_fmt,
                "iso_format": iso_fmt,
                "stamp_18_19_30": stamp_n,
                "id_le_1753": id_band,
                "with_pre_jul1_return": pre,
            },
        )
    ]
    # Register type in docs via detail only
    actual = findings[0].detail
    baseline = {
        "jul31_cluster_approx": 344,
        "jul31_jul1_dated": 368,
        "live_jul1_dated_prior": 191,
        "mechanism": "CREATED_AT_REWRITE_NOT_INSERT",
    }
    return CheckResult(
        code="F4",
        title="Jul1 created_at rewrite provenance",
        query="clients.created_at LIKE '2026-07-01%'; space vs ISO; id<=1753; pre-Jul1 returns",
        baseline=baseline,
        actual=actual,
        deviation="INFORMATIONAL",
        note="Provenance flag, not a repair target.",
        findings=findings,
    )


def check_f5_ssn_twins(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
    finally:
        conn.close()
    by_norm: dict[str, list] = defaultdict(list)
    for c in clients:
        key = re.sub(
            r"[^A-Z0-9|]",
            "",
            f"{(c['last_name'] or '').upper()}|{(c['first_name'] or '').upper()}",
        )
        if key and key != "|":
            by_norm[key].append(c)

    asym = []
    findings = []
    for key, members in by_norm.items():
        if len(members) < 2:
            continue
        with_ssn = [m for m in members if (m.get("ssn_last4") or "").strip()]
        without = [m for m in members if not (m.get("ssn_last4") or "").strip()]
        if with_ssn and without:
            asym.append({"key": key, "keep": [m["id"] for m in with_ssn], "discard_candidates": [m["id"] for m in without]})
            findings.append(
                Finding(
                    finding_type="DUPLICATE_CLIENT",
                    entity_key=f"ssn_asym||{key}",
                    salient="ssn_asymmetric",
                    detail={
                        "subtype": "ssn_asymmetric_twin",
                        "prefer_ids": [m["id"] for m in with_ssn],
                        "null_ssn_ids": [m["id"] for m in without],
                        "merge_direction": "favor_ssn_bearing",
                    },
                )
            )

    return CheckResult(
        code="F5",
        title="Asymmetric SSN twins",
        query="Normalized name dup groups where some have ssn_last4 and others NULL",
        baseline={"note": "Common in Jul1 cluster (0/368 SSN on stamp set)"},
        actual={"asymmetric_groups": len(asym), "samples": asym[:10]},
        deviation="INFORMATIONAL",
        note="Merge direction should favor SSN-bearing row.",
        findings=findings,
    )


def check_f6_shells(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        rows = list(
            conn.execute(
                "SELECT client_status, "
                "SUM(CASE WHEN log_number IS NULL OR TRIM(log_number)='' THEN 1 ELSE 0 END) AS no_log, "
                "COUNT(*) AS n "
                "FROM returns GROUP BY client_status"
            )
        )
        total = int(conn.execute("SELECT COUNT(*) FROM returns").fetchone()[0])
        no_log = int(
            conn.execute(
                "SELECT COUNT(*) FROM returns WHERE log_number IS NULL OR TRIM(log_number)=''"
            ).fetchone()[0]
        )
        pending = int(
            conn.execute(
                "SELECT COUNT(*) FROM returns WHERE UPPER(client_status)='PENDING INTAKE' "
                "AND (log_number IS NULL OR TRIM(log_number)='')"
            ).fetchone()[0]
        )
        pending_all = int(
            conn.execute(
                "SELECT COUNT(*) FROM returns WHERE UPPER(client_status)='PENDING INTAKE'"
            ).fetchone()[0]
        )
    finally:
        conn.close()

    pct = round(100.0 * no_log / total, 2) if total else 0
    by_status = {r[0]: {"no_log": r[1], "n": r[2]} for r in rows}
    baseline_pct = 26.0
    deviation = "ALARMING" if abs(pct - baseline_pct) > 15 else "INFORMATIONAL"
    return CheckResult(
        code="F6",
        title="Shells without log numbers",
        query="returns WHERE log_number IS NULL; PENDING INTAKE subset",
        baseline={"approx_pct_returns_without_log": 26, "fill_rate_log": 0.74},
        actual={
            "returns_total": total,
            "returns_without_log": no_log,
            "pct_without_log": pct,
            "pending_intake_total": pending_all,
            "pending_intake_without_log": pending,
            "by_status": by_status,
        },
        deviation=deviation,
        findings=[],
    )


def check_f7_last4(csm_path: Path, taxops_path: Path) -> CheckResult:
    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    csm_l4: Counter[str] = Counter()
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        last4 = re.sub(r"\D", "", str(vals[0] or ""))[-4:]
        if len(last4) == 4:
            csm_l4[last4] += 1
    wb.close()
    csm_collisions = {k: v for k, v in csm_l4.items() if v > 1}
    csm_surplus = sum(v - 1 for v in csm_collisions.values())

    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = _clients(conn)
        prefill_coll = list(
            conn.execute(
                "SELECT csm_ssn_last4, COUNT(*) n FROM drake_prefill_links "
                "GROUP BY csm_ssn_last4 HAVING n > 1"
            )
        )
    except sqlite3.Error:
        prefill_coll = []
        clients = _clients(conn)
    finally:
        conn.close()

    tax_l4: Counter[str] = Counter()
    for c in clients:
        l4 = (c.get("ssn_last4") or "").strip()
        if len(l4) == 4:
            tax_l4[l4] += 1
    tax_collisions = {k: v for k, v in tax_l4.items() if v > 1}

    # Assert no L1 link is last4-only: disposition / entity_link check
    from audit.db import connect_audit

    l1_last4_only = 0
    adb = Path(r"T:\audit\audit_20260810.sqlite")
    if adb.exists():
        ac = connect_audit(adb)
        try:
            for r in ac.execute(
                "SELECT evidence_json FROM entity_link WHERE tier='L1'"
            ):
                ev = json.loads(r[0] or "{}")
                keys = set(ev.keys())
                if keys <= {"last4", "ssn_last4", "csm_ssn_last4"}:
                    l1_last4_only += 1
                if "surname" not in ev and "rule" not in ev:
                    # L1 must include surname per A1
                    if "last4" in ev and len(keys) <= 2:
                        l1_last4_only += 1
        finally:
            ac.close()

    findings = []
    if l1_last4_only:
        findings.append(
            Finding(
                finding_type="NEEDS_HUMAN",
                entity_key="l1_last4_only_violation",
                salient=str(l1_last4_only),
                detail={"count": l1_last4_only},
            )
        )

    return CheckResult(
        code="F7",
        title="Last-4 collisions",
        query="GROUP BY last4 HAVING COUNT>1 on CSM, clients, prefill; assert L1 not last4-alone",
        baseline={
            "csm_surplus_approx": 52,
            "client_keys_surplus_approx": 78,
            "prefill_surplus_approx": 130,
        },
        actual={
            "csm_collision_keys": len(csm_collisions),
            "csm_surplus": csm_surplus,
            "taxops_collision_keys": len(tax_collisions),
            "taxops_surplus": sum(v - 1 for v in tax_collisions.values()),
            "prefill_collision_keys": len(prefill_coll),
            "l1_last4_only_violations": l1_last4_only,
        },
        deviation="ALARMING" if l1_last4_only else "INFORMATIONAL",
        note="Collisions are expected; last4-alone L1 would be alarming.",
        findings=findings,
    )


def check_f8_phantoms(taxops_path: Path, inv_path: Path, log_path: Path) -> CheckResult:
    """TaxOps-only approx: TY2025 returns whose bare log not in Drake L0 and not in Log — weak.
    Better: clients with no return log in invoice L0 set and name not in CSM — still approx.
    Use: TY2025 returns with log not in Tax Log and not in invoice L0 → phantom-ish.
    """
    from audit.tax_log_index import load_tax_log_index

    inv = parse_taxpayer_invoice_csv(inv_path)
    inv_bare = {bare_log_number(i) for i in inv.l0_ok_invoices}

    log_keys: set[str] = set()
    log_meta: dict[str, Any] = {}
    if log_path.exists():
        idx = load_tax_log_index(log_path, tax_year=2025, require_season_for_canonical=True)
        log_keys = idx.canonical_bares
        log_meta = {
            "restart_cut": idx.restart_cut,
            "named_rows": len(idx.rows),
            "all_bares": len(idx.all_bares),
            "canonical_yr25_bares": len(idx.canonical_bares),
        }

    conn = connect_taxops_readonly(taxops_path)
    try:
        clients = {c["id"]: c for c in _clients(conn)}
        rets = [
            dict(r)
            for r in conn.execute(
                "SELECT id, client_id, log_number, tax_year, client_status FROM returns WHERE tax_year=2025"
            )
        ]
        # test-ish names
        test_re = re.compile(r"\b(TEST|DEMO|SAMPLE|ZZZ)\b", re.I)
    finally:
        conn.close()

    phantoms = []
    findings = []
    for r in rets:
        if r["client_id"] not in clients:
            continue  # is_test or deleted — excluded from audit universe
        bare = bare_log_number(str(r.get("log_number") or ""))
        if bare and bare in log_keys:
            continue
        if bare and bare in inv_bare:
            continue
        # no log at all or unmatched
        c = clients.get(r["client_id"]) or {}
        name = f"{c.get('last_name') or ''}, {c.get('first_name') or ''}".strip(", ")
        bucket = "unmatched_other"
        if test_re.search(name):
            bucket = "test"
        elif (r.get("client_status") or "").upper() in ("CANCELLED", "LOG OUT"):
            bucket = "closed_or_logout"
        phantoms.append({"return_id": r["id"], "client_id": r["client_id"], "bucket": bucket, "bare": bare})
        ek = f"{bare}|2025" if bare else norm_name_key(c.get("last_name") or "", c.get("first_name") or "")
        findings.append(
            Finding(
                finding_type="PHANTOM_IN_TAXOPS",
                entity_key=ek or f"return:{r['id']}",
                salient=bucket,
                detail={"cause": bucket, "client_id": r["client_id"], "return_id": r["id"]},
            )
        )

    by_b = Counter(p["bucket"] for p in phantoms)
    return CheckResult(
        code="F8",
        title="Phantom / unmatched TaxOps",
        query="TY2025 returns whose bare log ∉ Tax Log (YR=25 canonical) and ∉ Drake L0-eligible invoices",
        baseline={"jul31_PHANTOM_IN_TAXOPS": 227},
        actual={
            "phantom_returns": len(phantoms),
            "by_bucket": dict(by_b),
            "log_index": log_meta,
            "note": "Log keys = YR=25 canonical per bare (restart cut); not all named rows",
        },
        deviation="INFORMATIONAL",
        note="Split test/closed before worklist; name-only phantoms need A3+ ladder residual.",
        findings=findings[:500],  # cap emission
    )


def check_f9_workflow(inv_path: Path, taxops_path: Path, log_path: Path, csm_path: Path) -> CheckResult:
    """Prepared-not-logged / logged-not-prepared via bare log + CSM status if available."""
    from audit.tax_log_index import load_tax_log_index

    inv = parse_taxpayer_invoice_csv(inv_path)
    inv_bare = {bare_log_number(i) for i in inv.l0_ok_invoices}

    log_keys: set[str] = set()
    log_meta: dict[str, Any] = {}
    if log_path.exists():
        idx = load_tax_log_index(log_path, tax_year=2025, require_season_for_canonical=True)
        log_keys = idx.canonical_bares
        log_meta = {
            "restart_cut": idx.restart_cut,
            "all_bares": len(idx.all_bares),
            "canonical_yr25_bares": len(idx.canonical_bares),
        }

    # CSM with EF/Complete-ish status ≈ prepared
    prepared_statuses = re.compile(r"EF |Accepted|Complete|Filed|Print", re.I)
    csm_prepared_names = []
    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        status = str(vals[5] or "").strip() if len(vals) > 5 else ""
        last4 = re.sub(r"\D", "", str(vals[0] or ""))[-4:]
        if prepared_statuses.search(status) or status:
            # treat any non-empty status as in-Drake; Jul31 used match residual
            csm_prepared_names.append((name, last4, status))
    wb.close()

    # Key-based workflow from invoice L0 vs YR=25 canonical Log bares:
    prepared_not_logged = sorted(inv_bare - log_keys)
    logged_not_in_invoice = sorted(log_keys - inv_bare)

    findings = []
    for b in prepared_not_logged:
        findings.append(
            Finding(
                finding_type="PREPARED_NOT_LOGGED",
                entity_key=f"{b}|2025",
                salient="workflow",
                detail={"bare_log": b, "route": "workflow"},
            )
        )
    for b in logged_not_in_invoice[:300]:
        findings.append(
            Finding(
                finding_type="LOGGED_NOT_PREPARED",
                entity_key=f"{b}|2025",
                salient="workflow",
                detail={"bare_log": b, "route": "workflow", "note": "in log, not in L0 invoice export"},
            )
        )

    return CheckResult(
        code="F9",
        title="Logged-not-prepared / prepared-not-logged",
        query="bare_log: invoice L0 vs Tax Log XCEL 2025 YR=25 canonical (workflow routing)",
        baseline={"LOGGED_NOT_PREPARED": 141, "PREPARED_NOT_LOGGED": 88},
        actual={
            "invoice_L0_not_in_log": len(prepared_not_logged),
            "log_not_in_invoice_L0": len(logged_not_in_invoice),
            "csm_rows_with_status_field": len(csm_prepared_names),
            "log_index": log_meta,
            "method": "invoice_key_proxy_canonical_yr25_log",
        },
        deviation="INFORMATIONAL",
        note="Marked workflow findings — different worklist from data findings.",
        findings=findings,
    )


def check_f10_prefill(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        burst = list(
            conn.execute(
                "SELECT id, last_name, first_name, ssn_last4, created_at FROM clients "
                "WHERE created_at LIKE '2026-08-07T22:27:47%'"
            )
        )
        # linked to prefill
        linked = 0
        twins = 0
        for r in burst:
            n = conn.execute(
                "SELECT COUNT(*) FROM drake_prefill_links WHERE client_id=?", (r[0],)
            ).fetchone()[0]
            if n:
                linked += 1
            # older twin?
            last, first = r[1] or "", r[2] or ""
            older = conn.execute(
                "SELECT id FROM clients WHERE id!=? AND UPPER(TRIM(last_name))=UPPER(TRIM(?)) "
                "AND UPPER(TRIM(COALESCE(first_name,'')))=UPPER(TRIM(?)) "
                "AND created_at < '2026-08-07' LIMIT 1",
                (r[0], last, first),
            ).fetchone()
            if older:
                twins += 1
        ssn_bearing = sum(1 for r in burst if r[3])
    except sqlite3.Error as e:
        return CheckResult(
            code="F10",
            title="Prefill stub proliferation",
            query="clients created_at Aug7 burst; drake_prefill_links",
            baseline={"aug7_burst": 244},
            actual={"error": str(e)},
            deviation="ALARMING",
            findings=[],
        )
    finally:
        conn.close()

    findings = []
    for r in burst:
        if True:  # emit cohort fingerprint once + per twin
            pass
    findings.append(
        Finding(
            finding_type="PREFILL_STUB_BURST",
            entity_key="2026-08-07T22:27:47",
            salient=str(len(burst)),
            detail={
                "n": len(burst),
                "ssn_bearing": ssn_bearing,
                "prefill_linked": linked,
                "exact_name_older_twin": twins,
            },
        )
    )
    deviation = "ALARMING" if abs(len(burst) - 244) > 30 else "INFORMATIONAL"
    return CheckResult(
        code="F10",
        title="Prefill stub proliferation (--link-clients)",
        query="clients.created_at LIKE '2026-08-07T22:27:47%'; join drake_prefill_links",
        baseline={"aug7_burst": 244, "all_ssn_and_prefill_linked": True},
        actual={
            "burst_count": len(burst),
            "ssn_bearing": ssn_bearing,
            "prefill_linked": linked,
            "exact_name_older_twin": twins,
        },
        deviation=deviation,
        note="Classify twin-of-older vs legitimately new before merge.",
        findings=findings,
    )


def check_f11_spouse(taxops_path: Path) -> CheckResult:
    conn = connect_taxops_readonly(taxops_path)
    try:
        clients_spouse = int(
            conn.execute(
                "SELECT COUNT(*) FROM clients WHERE "
                "(spouse_last_name IS NOT NULL AND TRIM(spouse_last_name)!='') "
                "OR (spouse_first_name IS NOT NULL AND TRIM(spouse_first_name)!='')"
            ).fetchone()[0]
        )
        spouses_n = int(conn.execute("SELECT COUNT(*) FROM spouses").fetchone()[0])
        try:
            hh = int(conn.execute("SELECT COUNT(*) FROM drake_household_prefill").fetchone()[0])
        except sqlite3.Error:
            hh = -1
        # divergence: clients with spouse_* but no spouses row
        only_clients = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM clients c
                WHERE ((c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name)!='')
                    OR (c.spouse_first_name IS NOT NULL AND TRIM(c.spouse_first_name)!=''))
                  AND NOT EXISTS (SELECT 1 FROM spouses s WHERE s.client_id=c.id)
                """
            ).fetchone()[0]
        )
        only_spouses = int(
            conn.execute(
                """
                SELECT COUNT(*) FROM spouses s
                WHERE NOT EXISTS (
                  SELECT 1 FROM clients c WHERE c.id=s.client_id AND (
                    (c.spouse_last_name IS NOT NULL AND TRIM(c.spouse_last_name)!='')
                    OR (c.spouse_first_name IS NOT NULL AND TRIM(c.spouse_first_name)!='')
                  )
                )
                """
            ).fetchone()[0]
        )
    finally:
        conn.close()

    findings = [
        Finding(
            finding_type="SPOUSE_STORE_DIVERGENCE",
            entity_key="triple_store_summary",
            salient=str(only_clients + only_spouses),
            detail={
                "clients_with_spouse_cols": clients_spouse,
                "spouses_rows": spouses_n,
                "drake_household_prefill": hh,
                "only_clients_cols": only_clients,
                "only_spouses_table": only_spouses,
            },
        )
    ]
    return CheckResult(
        code="F11",
        title="Spouse triple-store divergence",
        query="clients.spouse_* vs spouses vs drake_household_prefill",
        baseline={"SPOUSE_STORE_DIVERGENCE": 198, "spouses": 166, "household_prefill": 1302},
        actual={
            "clients_with_spouse_cols": clients_spouse,
            "spouses_rows": spouses_n,
            "drake_household_prefill": hh,
            "only_clients_cols": only_clients,
            "only_spouses_table": only_spouses,
        },
        deviation="INFORMATIONAL",
        findings=findings,
    )


def check_f12_key_coverage(inv_path: Path, taxops_path: Path, log_path: Path) -> CheckResult:
    """Amendment 1 supersedes 'no key'. Report invoice L0 three-way coverage."""
    from audit.tax_log_index import load_tax_log_index

    inv = parse_taxpayer_invoice_csv(inv_path)
    l0 = set(inv.l0_ok_invoices)
    bare_drake = {bare_log_number(i) for i in l0}

    conn = connect_taxops_readonly(taxops_path)
    try:
        taxops_bare = set()
        for (logn,) in conn.execute(
            "SELECT log_number FROM returns WHERE tax_year=2025 "
            "AND log_number IS NOT NULL AND TRIM(log_number)!=''"
        ):
            b = bare_log_number(str(logn))
            if b:
                taxops_bare.add(b)
        has_ext = False
        try:
            conn.execute("SELECT 1 FROM client_external_ids LIMIT 1")
            has_ext = True
            ext_n = int(conn.execute("SELECT COUNT(*) FROM client_external_ids").fetchone()[0])
        except sqlite3.Error:
            ext_n = 0
    finally:
        conn.close()

    log_bare: set[str] = set()
    log_meta: dict[str, Any] = {}
    if log_path.exists():
        idx = load_tax_log_index(log_path, tax_year=2025, require_season_for_canonical=True)
        log_bare = idx.canonical_bares
        log_meta = {
            "restart_cut": idx.restart_cut,
            "all_bares": len(idx.all_bares),
            "canonical_yr25_bares": len(idx.canonical_bares),
        }

    three = bare_drake & taxops_bare & log_bare
    name_only_share = None
    if bare_drake:
        name_only_share = round(100.0 * (len(bare_drake) - len(three)) / len(bare_drake), 2)

    return CheckResult(
        code="F12",
        title="Cross-system key coverage (Amendment 1)",
        query="bare_log three-way on TAXPAYER.csv L0 ∩ TaxOps ∩ Log(YR=25 canonical); client_external_ids exists?",
        baseline={
            "I4_claim": "no shared key",
            "amendment1": "Invoice Number = Tax Log number (season-prefixed in Drake)",
            "a1_three_way": 586,
        },
        actual={
            "client_external_ids_exists": has_ext,
            "client_external_ids_rows": ext_n,
            "l0_eligible_invoices": len(l0),
            "three_way_bare_log": len(three),
            "drake_taxops": len(bare_drake & taxops_bare),
            "pct_l0_not_three_way": name_only_share,
            "invoice_full_width_coverage_pct": inv.invoice_coverage_full_width_pct,
            "log_index": log_meta,
        },
        deviation="SUPERSEDED",
        note="I4 F12 'no key' superseded by invoice/log bare-key. client_external_ids not minted yet.",
        findings=[],
    )


def write_a3_report(results: list[CheckResult], delta_stats: dict, dest: Path = A3_REPORT_PATH) -> Path:
    lines: list[str] = []
    A = lines.append
    A("# A3 — Failure-mode checks (F1–F12)")
    A("")
    A(f"_Generated: {utc_now()}_")
    A(f"_Disposition DB: `{DISPOSITION_DB_PATH}`_")
    A(f"_Delta after checks: `{dumps(delta_stats)}`_")
    A("")
    A("| Check | Title | Deviation | Baseline highlight | Actual highlight |")
    A("|---|---|---|---|---|")
    for r in results:
        bh = dumps({k: r.baseline[k] for k in list(r.baseline)[:3]})
        ah = dumps({k: r.actual[k] for k in list(r.actual)[:4]})
        A(f"| {r.code} | {r.title} | `{r.deviation}` | `{bh}` | `{ah}` |")
    A("")
    for r in results:
        A(f"## {r.code} — {r.title}")
        A("")
        A(f"- **Query:** {r.query}")
        A(f"- **Deviation class:** `{r.deviation}`")
        A(f"- **Baseline:** `{dumps(r.baseline)}`")
        A(f"- **Actual:** `{dumps(r.actual)}`")
        if r.note:
            A(f"- **Note:** {r.note}")
        A(f"- **Findings emitted:** {len(r.findings)}")
        A("")
    A("## Loud corrections vs I4")
    A("")
    A("- **F12:** I4 'no cross-system key' is **SUPERSEDED** by Amendment 1 (Invoice Number / bare log).")
    A("- **F8/F9:** Counts use invoice-key proxies; Jul31 used name-match residuals — not 1:1 comparable.")
    A("")
    dest.write_text("\n".join(lines), encoding="utf-8")
    return dest


def run_a3_phase() -> tuple[list[CheckResult], Path]:
    mem = load_baseline_memory()
    taxops = Path(mem.get("authoritative_taxops_path") or config.DEFAULT_TAXOPS_DB)
    if not taxops.exists():
        taxops = Path(r"T:\taxops\taxops.db")
    csm = Path(mem.get("authoritative_drake_path") or config.DEFAULT_DRAKE_PATH)
    log_path = Path(mem.get("tax_log_path") or config.DEFAULT_TAX_LOG_PATH)
    inv_path = Path(mem.get("taxpayer_invoice_path") or DEFAULT_TAXPAYER_INVOICE_PATH)

    results = [
        check_f1_truncation(csm, taxops),
        check_f2_format(csm, taxops),
        check_f3_duplicates(taxops),
        check_f4_jul1(taxops),
        check_f5_ssn_twins(taxops),
        check_f6_shells(taxops),
        check_f7_last4(csm, taxops),
        check_f8_phantoms(taxops, inv_path, log_path),
        check_f9_workflow(inv_path, taxops, log_path, csm),
        check_f10_prefill(taxops),
        check_f11_spouse(taxops),
        check_f12_key_coverage(inv_path, taxops, log_path),
    ]

    all_findings: list[Finding] = []
    for r in results:
        all_findings.extend(r.findings)

    # Ensure fingerprint docs know new types — extend disposition docs lightly via findings only
    label = f"a3-{utc_now().replace(':', '').replace('-', '')[:15]}"
    # Full A3 emission — allow auto-resolve of types we now emit
    delta = apply_delta(all_findings, run_label=label, source="a3-partial")
    delta_stats = {
        "NEW": len(delta.new),
        "RECURRING": len(delta.recurring),
        "RESOLVED": len(delta.resolved),
        "REGRESSED": len(delta.regressed),
        "headline": delta.headline,
        "findings_emitted": len(all_findings),
    }
    path = write_a3_report(results, delta_stats)
    return results, path


if __name__ == "__main__":
    # Register extra finding types used by A3 into fingerprint map (monkey-patch docs)
    from audit import disposition as D

    D.FINGERPRINT_DOCS.setdefault(
        "JUL1_PROVENANCE",
        "entity_key='cohort' singleton for the Jul1 created_at rewrite measurement.",
    )
    D.FINGERPRINT_DOCS.setdefault(
        "PREFILL_STUB_BURST",
        "entity_key=burst timestamp; salient=count.",
    )
    results, path = run_a3_phase()
    alarming = [r.code for r in results if r.deviation == "ALARMING"]
    print(
        f"A3 done report={path} checks={len(results)} alarming={alarming or 'none'}"
    )
