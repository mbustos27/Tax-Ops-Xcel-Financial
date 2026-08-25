"""Resolve SPOUSE_STORE_DIVERGENCE cured by Wave 4 fold (entity_key = last4|LAST|FIRST)."""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

DISP = Path(r"T:\audit\audit_disposition.sqlite")
TAXOPS = Path(r"T:\taxops\taxops.db")
OUT = Path(r"T:\audit\investigation\W4-spouse-divergence-resolved.json")


def _norm(s: str) -> str:
    s = re.sub(r"[^A-Z0-9 ]", " ", (s or "").upper())
    return re.sub(r"\s+", " ", s).strip()


def main() -> None:
    dconn = sqlite3.connect(str(DISP))
    dconn.row_factory = sqlite3.Row
    tconn = sqlite3.connect(str(TAXOPS))
    tconn.row_factory = sqlite3.Row
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    # client lookup by entity-style keys
    by_key: dict[str, list[dict]] = {}
    for r in tconn.execute(
        "SELECT c.id, c.last_name, c.first_name, c.ssn_last4, "
        "c.spouse_first_name, c.spouse_last_name, "
        "s.first_name AS sp_first, s.last_name AS sp_last, s.source AS sp_source "
        "FROM clients c "
        "LEFT JOIN spouses s ON s.client_id = c.id"
    ):
        last = _norm(r["last_name"] or "")
        first = _norm(r["first_name"] or "")
        l4 = (r["ssn_last4"] or "").strip()
        row = dict(r)
        by_key.setdefault(f"{last}|{first}", []).append(row)
        if len(l4) == 4:
            by_key.setdefault(f"{l4}|{last}|{first}", []).append(row)

    open_rows = list(
        dconn.execute(
            """
            SELECT finding_id, entity_key
            FROM audit_disposition
            WHERE finding_type='SPOUSE_STORE_DIVERGENCE'
              AND status IN ('OPEN','ACKED')
            """
        )
    )
    resolved = []
    for r in open_rows:
        raw_parts = (r["entity_key"] or "").split("|")
        parts = []
        for p in raw_parts:
            p = p.strip()
            if re.fullmatch(r"\d{4}", p or ""):
                parts.append(p)
            else:
                parts.append(_norm(p))
        if len(parts) >= 3 and re.fullmatch(r"\d{4}", parts[0] or ""):
            keys = [f"{parts[0]}|{parts[1]}|{parts[2]}", f"{parts[1]}|{parts[2]}"]
        elif len(parts) >= 2:
            keys = [f"{parts[0]}|{parts[1]}"]
        else:
            continue

        candidates = []
        for k in keys:
            candidates.extend(by_key.get(k) or [])
        # dedupe by id
        seen = set()
        uniq = []
        for c in candidates:
            if c["id"] not in seen:
                seen.add(c["id"])
                uniq.append(c)

        cured = []
        for c in uniq:
            if not c.get("sp_first") and not c.get("sp_last"):
                continue  # still no spouses row
            cf = _norm(c.get("spouse_first_name") or "")
            cl = _norm(c.get("spouse_last_name") or "")
            sf = _norm(c.get("sp_first") or "")
            sl = _norm(c.get("sp_last") or "")
            if sf == "UNKNOWN":
                sf = ""
            # Cured if fold created the spouses row from clients, or names match
            if c.get("sp_source") == "wave4_clients_fold" and (cf or cl):
                cured.append(c["id"])
            elif cf == sf and cl == sl and (sf or sl):
                cured.append(c["id"])

        if cured:
            dconn.execute(
                """
                UPDATE audit_disposition
                   SET status='RESOLVED', resolved_by='wave4_fold', resolved_at=?,
                       note=?
                 WHERE finding_id=?
                """,
                (
                    now,
                    f"Wave4 fold cured clients-only gap; client_ids={cured}",
                    r["finding_id"],
                ),
            )
            resolved.append(
                {"finding_id": r["finding_id"], "entity": r["entity_key"], "client_ids": cured}
            )

    dconn.commit()
    still = dconn.execute(
        """
        SELECT COUNT(*) FROM audit_disposition
        WHERE finding_type='SPOUSE_STORE_DIVERGENCE' AND status IN ('OPEN','ACKED')
        """
    ).fetchone()[0]
    OUT.write_text(
        json.dumps(
            {"resolved_n": len(resolved), "still_open": still, "sample": resolved[:30]},
            indent=2,
        ),
        encoding="utf-8",
    )
    print(f"RESOLVED {len(resolved)}; still_open={still}")
    dconn.close()
    tconn.close()


if __name__ == "__main__":
    main()
