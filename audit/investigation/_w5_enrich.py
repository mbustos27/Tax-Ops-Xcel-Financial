"""Enrich W5 queue + check which spouse_unrecovered are now fold-covered."""
from __future__ import annotations

import json
import re
import sqlite3
from pathlib import Path

DISP = Path(r"T:\audit\audit_disposition.sqlite")
TAXOPS = Path(r"T:\taxops\taxops.db")
OUT_MD = Path(r"T:\audit\investigation\W5-needs-human-queue.md")
OUT_JSON = Path(r"T:\audit\investigation\W5-needs-human-queue.json")


def _norm_name(s: str) -> str:
    s = (s or "").upper()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def main() -> None:
    dconn = sqlite3.connect(str(DISP))
    dconn.row_factory = sqlite3.Row
    tconn = sqlite3.connect(str(TAXOPS))
    tconn.row_factory = sqlite3.Row

    # index spouses by client for coverage check
    spouse_clients = {
        int(r[0])
        for r in tconn.execute("SELECT client_id FROM spouses WHERE client_id IS NOT NULL")
    }

    nh = list(
        dconn.execute(
            """
            SELECT finding_id, entity_key, status, sample_detail, note
            FROM audit_disposition
            WHERE finding_type='NEEDS_HUMAN' AND status IN ('OPEN','ACKED')
            ORDER BY entity_key
            """
        )
    )
    amb = list(
        dconn.execute(
            """
            SELECT finding_id, entity_key, status, sample_detail
            FROM audit_disposition
            WHERE finding_type='SPOUSE_AMBIGUOUS' AND status IN ('OPEN','ACKED')
            ORDER BY entity_key
            """
        )
    )
    store_open = dconn.execute(
        """
        SELECT COUNT(*) FROM audit_disposition
        WHERE finding_type='SPOUSE_STORE_DIVERGENCE' AND status IN ('OPEN','ACKED')
        """
    ).fetchone()[0]

    rows_out = []
    now_has_spouse = 0
    for r in nh:
        detail = {}
        try:
            detail = json.loads(r["sample_detail"] or "{}")
        except json.JSONDecodeError:
            detail = {}
        ek = r["entity_key"] or ""
        # entity_key often: spouse_unrecovered|LAST4|NAME or similar
        parts = [p.strip() for p in ek.split("|")]
        subtype = parts[0] if parts else ""
        last4 = None
        name_hint = None
        for p in parts[1:]:
            if re.fullmatch(r"\d{4}", p or ""):
                last4 = p
            elif p and not name_hint:
                name_hint = p
        # detail may hold richer fields
        for k in ("name", "drake_name", "client_name", "norm_name", "display"):
            if detail.get(k) and not name_hint:
                name_hint = str(detail[k])
        client_ids = detail.get("client_ids") or detail.get("ids") or []
        if isinstance(client_ids, int):
            client_ids = [client_ids]
        covered = False
        for cid in client_ids:
            try:
                if int(cid) in spouse_clients:
                    covered = True
                    break
            except (TypeError, ValueError):
                pass
        # try match by last4
        matched_clients = []
        if last4:
            matched_clients = [
                dict(x)
                for x in tconn.execute(
                    "SELECT id, last_name, first_name, ssn_last4 FROM clients WHERE ssn_last4=?",
                    (last4,),
                )
            ]
            for mc in matched_clients:
                if int(mc["id"]) in spouse_clients:
                    covered = True
        if covered:
            now_has_spouse += 1
        rows_out.append(
            {
                "finding_id": r["finding_id"],
                "entity_key": ek,
                "subtype": subtype,
                "last4": last4,
                "name_hint": name_hint,
                "detail": {
                    k: detail[k]
                    for k in detail
                    if k
                    not in (
                        "ssn",
                        "ssn_full",
                        "file_path",
                    )
                    and "ssn" not in k.lower()
                },
                "matched_client_ids": [m["id"] for m in matched_clients],
                "has_spouses_row_now": covered,
                "status": r["status"],
            }
        )

    # Build staff workbook-style markdown (no SSN beyond last4 already in key)
    lines = [
        "# W5 — NEEDS_HUMAN review queue",
        "",
        "_Wave 5. All 42 open items are subtype `spouse_unrecovered`. Human adjudication — no auto-merge._",
        "",
        f"- **NEEDS_HUMAN open:** {len(nh)} (all `spouse_unrecovered`)",
        f"- **Of those, client now has a `spouses` row (post Wave 4 fold):** {now_has_spouse}",
        f"- **SPOUSE_AMBIGUOUS open:** {len(amb)} (still human)",
        f"- **SPOUSE_STORE_DIVERGENCE open:** {store_open}",
        "",
        "## Suggested staff workflow",
        "",
        "1. For rows marked **has spouses row now** — confirm the folded name is the real spouse; "
        "if yes, disposition `NEEDS_HUMAN` → `RESOLVED` with note `wave4_fold_confirmed`.",
        "2. For rows **without** a spouses row — look up the return in Drake; enter spouse on intake "
        "or mark `WONTFIX` if single/no spouse.",
        "3. Do spouse_ambiguous separately (genuine multi-candidate).",
        "",
        "## spouse_unrecovered",
        "",
        "| # | Last4 | Name hint | Has spouses row | Entity key |",
        "|---:|---|---|---|---|",
    ]
    for i, it in enumerate(rows_out, 1):
        lines.append(
            f"| {i} | `{it['last4'] or '—'}` | {it['name_hint'] or '—'} | "
            f"{'yes' if it['has_spouses_row_now'] else 'no'} | `{it['entity_key']}` |"
        )

    lines.extend(
        [
            "",
            "## SPOUSE_AMBIGUOUS (28) — separate lane",
            "",
            "| Entity key |",
            "|---|",
        ]
    )
    for r in amb:
        lines.append(f"| `{r['entity_key']}` |")

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
    OUT_JSON.write_text(
        json.dumps(
            {
                "needs_human_n": len(nh),
                "now_has_spouse_row": now_has_spouse,
                "spouse_ambiguous_n": len(amb),
                "spouse_store_divergence_open": store_open,
                "queue": rows_out,
            },
            indent=2,
            default=str,
        ),
        encoding="utf-8",
    )
    print(
        f"NEEDS_HUMAN={len(nh)} covered_by_spouses_now={now_has_spouse} "
        f"ambiguous={len(amb)} store_div={store_open}"
    )
    print("wrote", OUT_MD)
    dconn.close()
    tconn.close()


if __name__ == "__main__":
    main()
