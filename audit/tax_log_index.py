"""Tax Log index with band-restart cut + canonical row selection.

Canonical rule (Wave 1 re-pin):
  1. Prefer YR == season (25 for TY2025)
  2. Prefer Drake/TaxOps name-token agreement when provided
  3. Prefer later source_row
Never prefer non-LOGOUT alone (that selected TY2024 for bare 141).

Band cut = first reappearance of bare log ``1`` (sequence restart), else 772.
"""
from __future__ import annotations

import re
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Iterable, Optional

from audit import config
from audit.invoice_export import bare_log_number


def _excel_log_cell(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, float):
        return str(v).rstrip("0").rstrip(".")
    if isinstance(v, int):
        return str(v)
    return str(v).strip()


def _tokens(name: str) -> set[str]:
    return {t for t in re.findall(r"[A-Z0-9]+", (name or "").upper()) if len(t) > 1}


def _display(last: str, first: str) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


@dataclass
class TaxLogIndex:
    path: Path
    tax_year: int
    season_yy: str
    restart_cut: int
    rows: list[dict] = field(default_factory=list)
    by_bare: dict[str, list[dict]] = field(default_factory=dict)
    # One canonical row per bare (may be None if no YR=season row and require_season)
    canonical: dict[str, dict] = field(default_factory=dict)

    @property
    def canonical_bares(self) -> set[str]:
        return set(self.canonical.keys())

    @property
    def all_bares(self) -> set[str]:
        return set(self.by_bare.keys())


def find_restart_cut(by_bare: dict[str, list[dict]]) -> int:
    rows = sorted(r["source_row"] for r in by_bare.get("1", []))
    if len(rows) >= 2:
        return int(rows[1])
    return 772


def pick_canonical(
    rows: list[dict],
    *,
    season_yy: str = "25",
    drake_tokens: set[str] | None = None,
    taxops_tokens: set[str] | None = None,
    require_season: bool = False,
) -> Optional[dict]:
    if not rows:
        return None
    season_rows = [r for r in rows if str(r.get("yr")) == season_yy]
    pool = season_rows if season_rows else ([] if require_season else list(rows))
    if not pool:
        return None

    def score(r: dict) -> tuple:
        yr_ok = 0 if str(r.get("yr")) == season_yy else 1
        tok = _tokens(r.get("name") or "")
        agree = 1
        if drake_tokens and not tok.isdisjoint(drake_tokens):
            agree = 0
        elif taxops_tokens and not tok.isdisjoint(taxops_tokens):
            agree = 0
        return (yr_ok, agree, -int(r["source_row"]))

    return sorted(pool, key=score)[0]


def load_tax_log_index(
    log_path: Path,
    *,
    tax_year: int = 2025,
    require_season_for_canonical: bool = True,
) -> TaxLogIndex:
    """Load XCEL individuals sheet; build per-bare lists + canonical map."""
    import openpyxl

    season_yy = str(tax_year)[-2:]
    by_bare: dict[str, list[dict]] = defaultdict(list)
    rows: list[dict] = []

    wb = openpyxl.load_workbook(log_path, read_only=True, data_only=True)
    try:
        ws = wb[config.SHEET_INDIVIDUALS]
        for i, row in enumerate(ws.iter_rows(values_only=True), 1):
            if i < config.LOG_DATA_START_ROW:
                continue
            vals = list(row)
            logn = _excel_log_cell(vals[1]) if len(vals) > 1 else ""
            last = (
                str(vals[config.LOG_LAST_COL] or "").strip()
                if len(vals) > config.LOG_LAST_COL
                else ""
            )
            first = (
                str(vals[config.LOG_FIRST_COL] or "").strip()
                if len(vals) > config.LOG_FIRST_COL
                else ""
            )
            yr = vals[config.LOG_YR_COL] if len(vals) > config.LOG_YR_COL else None
            status = str(vals[7] or "").strip() if len(vals) > 7 else ""
            if not logn or not (last or first):
                continue
            bare = bare_log_number(logn, tax_year)
            if not bare:
                continue
            rec = {
                "source_row": i,
                "log_raw": logn,
                "log": bare,
                "bare_log": bare,
                "last": last,
                "first": first,
                "name": _display(last, first),
                "yr": yr,
                "status": status,
            }
            rows.append(rec)
            by_bare[bare].append(rec)
    finally:
        wb.close()

    cut = find_restart_cut(by_bare)
    for r in rows:
        r["band"] = "band1" if r["source_row"] < cut else "band2"

    canonical: dict[str, dict] = {}
    for bare, brows in by_bare.items():
        can = pick_canonical(
            brows, season_yy=season_yy, require_season=require_season_for_canonical
        )
        if can:
            canonical[bare] = can

    return TaxLogIndex(
        path=log_path,
        tax_year=tax_year,
        season_yy=season_yy,
        restart_cut=cut,
        rows=rows,
        by_bare=dict(by_bare),
        canonical=canonical,
    )


def canonical_log_by_num(index: TaxLogIndex) -> dict[str, list[dict]]:
    """Ladder-shaped map: bare → [canonical_row] (single entry)."""
    return {bare: [row] for bare, row in index.canonical.items()}
