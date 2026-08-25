"""C6 — diagnose L3 truncation links (read-only)."""
from __future__ import annotations

import re
from pathlib import Path

import openpyxl

from audit.baseline import CSM_CANDIDATES, load_baseline_memory
from audit.invoice_export import DEFAULT_TAXPAYER_INVOICE_PATH, parse_taxpayer_invoice_csv
from audit.ladder import order_normalize_tokens


def _person_display(last: str, first: str) -> str:
    last = (last or "").strip()
    first = (first or "").strip()
    if last and first:
        return f"{last}, {first}"
    return last or first


def main() -> None:
    mem = load_baseline_memory()
    csm_path = Path(mem.get("authoritative_drake_path") or "")
    if not csm_path.exists():
        csm_path = CSM_CANDIDATES["onedrive_ty2025"]
    inv = parse_taxpayer_invoice_csv(DEFAULT_TAXPAYER_INVOICE_PATH)

    inv_name_list = []
    for r in inv.rows:
        if not r.is_full_width:
            continue
        disp = _person_display(r.last, r.first)
        bag = order_normalize_tokens(f"{r.first} {r.last}")
        inv_name_list.append(
            {"invoice": r.invoice, "display": disp, "bag": bag, "last": r.last, "first": r.first}
        )

    wb = openpyxl.load_workbook(csm_path, read_only=True, data_only=True)
    ws = wb.active
    csm_long = []
    for i, row in enumerate(ws.iter_rows(values_only=True), 1):
        if i == 1:
            continue
        vals = list(row)
        name = str(vals[1] or "").strip() if len(vals) > 1 else ""
        if not name or name.upper().startswith("TOTAL"):
            continue
        if len(name) >= 39:
            last4 = re.sub(r"\D", "", str(vals[0] or ""))[-4:]
            csm_long.append((name, last4))
    wb.close()

    prefix_hits = 0
    bag_hits = 0
    reverse_prefix = 0  # CSM starts with invoice display (joint/trunc vs short invoice)
    invoice_longer_any = 0
    no_hit = 0
    samples_no = []
    samples_hit = []
    samples_reverse = []

    for csm_raw, last4 in csm_long:
        csm_norm = re.sub(r"\s+", " ", csm_raw.upper().strip())
        found = False
        for inv_rec in inv_name_list:
            longer = inv_rec["display"].upper()
            longer2 = f"{inv_rec['first']} {inv_rec['last']}".upper().strip()
            hit = None
            if len(longer) > len(csm_norm) and longer.startswith(csm_norm):
                hit = "display"
                prefix_hits += 1
            elif len(longer2) > len(csm_norm) and longer2.startswith(csm_norm):
                hit = "firstlast"
                prefix_hits += 1
            csm_bag = order_normalize_tokens(csm_raw)
            if (
                not hit
                and csm_bag
                and inv_rec["bag"].startswith(csm_bag)
                and len(inv_rec["bag"]) > len(csm_bag)
            ):
                hit = "bag"
                bag_hits += 1
            if hit:
                found = True
                if len(samples_hit) < 3:
                    samples_hit.append((csm_raw, inv_rec["display"], hit))
                break
            if longer and csm_norm.startswith(longer) and len(csm_norm) > len(longer):
                reverse_prefix += 1
                if len(samples_reverse) < 3:
                    samples_reverse.append((csm_raw, inv_rec["display"]))
        if found:
            invoice_longer_any += 1
        else:
            no_hit += 1
            if len(samples_no) < 5:
                samples_no.append(csm_raw)

    print("csm_path", csm_path)
    print("CSM_ge39", len(csm_long))
    print("invoice_first_max", inv.first_name_max_len, "last_max", inv.last_name_max_len)
    print("invoice_at39_40", inv.first_at_39, inv.first_at_40, inv.last_at_39, inv.last_at_40)
    print("L3_forward_hits_any", invoice_longer_any)
    print("prefix_hits", prefix_hits, "bag_hits", bag_hits, "no_hit", no_hit)
    print("reverse_prefix_events", reverse_prefix)
    print("samples_hit", samples_hit)
    print("samples_reverse", samples_reverse)
    print("samples_no", samples_no)
    print(
        "L3_sources_actual: CSM (authoritative) <-> TAXPAYER.csv invoice names only; "
        "NOT purple CSM alternate, NOT Tax Log"
    )


if __name__ == "__main__":
    main()
