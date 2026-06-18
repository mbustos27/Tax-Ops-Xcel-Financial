"""ACCOUNTING-5: QuickBooks export — CSV (QB Online) and IIF (QB Desktop legacy).

Public API
----------
    from services.qb_export import export_receipts
    path, fmt = export_receipts(rows, dest_dir)   # rows = list[dict] from receipt_queue

``rows`` fields used:
    receipt_date, vendor, total_amount, payment_method,
    approved_category, approved_account, review_notes, id

QB CSV format (QuickBooks Online import)
-----------------------------------------
Date, Description, Amount, Memo, Account, Name

IIF format (QuickBooks Desktop)
---------------------------------
Tab-delimited TRNS/SPL/ENDTRNS blocks.
One TRNS per receipt; one SPL for the expense account, one SPL for the payment account.
"""

from __future__ import annotations

import csv
import io
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Sequence

log = logging.getLogger(__name__)

# Default payment account used in IIF when none is specified.
_DEFAULT_PAYMENT_ACCOUNT = "Checking"


def _now_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _fmt_date_csv(raw: str | None) -> str:
    """Normalise date to MM/DD/YYYY for QB CSV/IIF.  Returns today on failure."""
    if raw:
        for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%m-%d-%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(raw.strip(), fmt).strftime("%m/%d/%Y")
            except ValueError:
                pass
    return datetime.now().strftime("%m/%d/%Y")


# ── CSV export ─────────────────────────────────────────────────────────────────

def export_to_csv(rows: Sequence[dict]) -> str:
    """Return QB Online import CSV as a string."""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(["Date", "Description", "Amount", "Memo", "Account", "Name"])
    for r in rows:
        writer.writerow([
            _fmt_date_csv(r.get("receipt_date")),
            r.get("vendor") or "",
            f"-{abs(float(r.get('total_amount') or 0)):.2f}",
            r.get("review_notes") or "",
            r.get("approved_category") or r.get("suggested_category") or "",
            r.get("vendor") or "",
        ])
    return output.getvalue()


# ── IIF export ─────────────────────────────────────────────────────────────────

_IIF_HEADER = (
    "!TRNS\tDATE\tACCNT\tNAME\tAMOUNT\tMEMO\n"
    "!SPL\tDATE\tACCNT\tNAME\tAMOUNT\tMEMO\n"
    "!ENDTRNS\n"
)


def export_to_iif(rows: Sequence[dict]) -> str:
    """Return QB Desktop IIF import file as a string."""
    lines = [_IIF_HEADER]
    for r in rows:
        date_str = _fmt_date_csv(r.get("receipt_date"))
        vendor = r.get("vendor") or ""
        amount = float(r.get("total_amount") or 0)
        expense_acct = r.get("approved_category") or r.get("suggested_category") or "Uncategorized"
        payment_acct = r.get("payment_method") or _DEFAULT_PAYMENT_ACCOUNT
        if payment_acct.lower() in ("credit", "credit card"):
            payment_acct = "Credit Card"
        elif payment_acct.lower() in ("check", "cheque"):
            payment_acct = "Checking"
        elif payment_acct.lower() == "cash":
            payment_acct = "Petty Cash"
        else:
            payment_acct = _DEFAULT_PAYMENT_ACCOUNT
        memo = r.get("review_notes") or ""

        # TRNS: debit from payment account (negative = money going out)
        lines.append(f"TRNS\t{date_str}\t{payment_acct}\t{vendor}\t-{amount:.2f}\t{memo}")
        # SPL: credit to expense account (positive amount on the expense side)
        lines.append(f"SPL\t{date_str}\t{expense_acct}\t{vendor}\t{amount:.2f}\t{memo}")
        lines.append("ENDTRNS")
    return "\n".join(lines) + "\n"


# ── Dispatcher ─────────────────────────────────────────────────────────────────

def export_receipts(rows: Sequence[dict], dest_dir: str | Path) -> tuple[str, str]:
    """Write approved rows to a file in *dest_dir*.

    Returns (file_path, format_used).  ``format_used`` is "csv" or "iif".
    """
    import config as _cfg

    dest = Path(dest_dir)
    dest.mkdir(parents=True, exist_ok=True)

    fmt = _cfg.QB_EXPORT_MODE if _cfg.QB_EXPORT_MODE in ("csv", "iif") else "csv"
    stamp = _now_stamp()

    if fmt == "iif":
        filename = f"taxops_receipts_{stamp}.iif"
        content = export_to_iif(rows)
    else:
        filename = f"taxops_receipts_{stamp}.csv"
        content = export_to_csv(rows)

    file_path = dest / filename
    file_path.write_text(content, encoding="utf-8")
    log.info("qb_export: wrote %d rows to %s (%s)", len(rows), file_path, fmt)
    return str(file_path), fmt
