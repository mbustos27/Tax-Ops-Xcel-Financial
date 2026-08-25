"""Schema v27 — add clients.is_test and mark scanning-test batch (2155–2190).

Also FALSE_POSITIVE open PHANTOM_IN_TAXOPS rows whose entity_key is a known
test fingerprint. Fixes documented for 2159 (last4-present vs absent split)
and 2175 (duplicate same-name entity) via new disposition entity_key shape
in disposition.py (client_id|last4|name).
"""
from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")
DISP = Path(r"T:\audit\audit_disposition.sqlite")
OUT = Path(r"T:\audit\investigation\W-is-test-mark.json")

TEST_NAME = re.compile(r"\b(TEST|SCAN|TEZT|DEMO|SAMPLE)\b", re.I)


def main() -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    conn = sqlite3.connect(str(TAXOPS), timeout=60)
    conn.execute("PRAGMA busy_timeout=60000")
    cols = {r[1] for r in conn.execute("PRAGMA table_info(clients)")}
    if "is_test" not in cols:
        conn.execute("ALTER TABLE clients ADD COLUMN is_test INTEGER NOT NULL DEFAULT 0")
        print("added is_test column")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_clients_is_test ON clients(is_test)")

    # Mark scanning batch by id range + name signal; also any TEST/SCAN name outside
    marked = []
    rows = list(
        conn.execute(
            "SELECT id, last_name, first_name, ssn_last4, COALESCE(is_test,0) FROM clients "
            "WHERE id BETWEEN 2155 AND 2190 OR "
            "upper(COALESCE(last_name,'')) LIKE '%TEST%' OR "
            "upper(COALESCE(first_name,'')) LIKE '%TEST%' OR "
            "upper(COALESCE(last_name,'')) LIKE '%SCAN%' OR "
            "upper(COALESCE(first_name,'')) LIKE '%SCAN%' OR "
            "upper(COALESCE(last_name,'')) LIKE '%TEZT%'"
        )
    )
    for r in rows:
        cid, last, first, last4, already = r
        name = f"{last or ''} {first or ''}"
        in_batch = 2155 <= cid <= 2190
        name_hit = bool(TEST_NAME.search(name))
        # 2181 VILLANUEVA with last4 1111 is in the scanning batch id range — mark it
        if not (in_batch or name_hit):
            continue
        if already:
            marked.append({"client_id": cid, "already": True, "name": name.strip()})
            continue
        conn.execute("UPDATE clients SET is_test=1 WHERE id=?", (cid,))
        marked.append(
            {
                "client_id": cid,
                "already": False,
                "name": name.strip(),
                "last4": last4,
                "in_batch": in_batch,
                "name_hit": name_hit,
            }
        )

    # Stamp schema version lightly (avoid full init_db)
    try:
        as_cols = {r[1] for r in conn.execute("PRAGMA table_info(app_settings)")}
        if "updated_at" in as_cols:
            conn.execute(
                "INSERT INTO app_settings (key, value, updated_at) VALUES ('schema_version','27',?) "
                "ON CONFLICT(key) DO UPDATE SET value='27', updated_at=excluded.updated_at",
                (now,),
            )
        else:
            conn.execute(
                "INSERT INTO app_settings (key, value) VALUES ('schema_version','27') "
                "ON CONFLICT(key) DO UPDATE SET value='27'"
            )
    except sqlite3.Error as e:
        print("schema stamp skipped", e)
    conn.commit()
    n_test = conn.execute("SELECT COUNT(*) FROM clients WHERE is_test=1").fetchone()[0]
    conn.close()

    # Disposition: FALSE_POSITIVE open phantoms that are pure test name keys
    disp_updated = []
    if DISP.exists():
        dconn = sqlite3.connect(str(DISP))
        dconn.row_factory = sqlite3.Row
        open_phantoms = list(
            dconn.execute(
                "SELECT finding_id, entity_key, status FROM audit_disposition "
                "WHERE finding_type='PHANTOM_IN_TAXOPS' AND status IN ('OPEN','ACKED')"
            )
        )
        for r in open_phantoms:
            ek = (r["entity_key"] or "").upper()
            if TEST_NAME.search(ek.replace("|", " ")):
                dconn.execute(
                    """
                    UPDATE audit_disposition
                       SET status='FALSE_POSITIVE', resolved_by='is_test', resolved_at=?,
                           note=?
                     WHERE finding_id=?
                    """,
                    (
                        now,
                        "Scanning/test client (clients.is_test=1). Fingerprint excluded from A2/A3.",
                        r["finding_id"],
                    ),
                )
                disp_updated.append(r["entity_key"])
        # Alias note on 2159 split pair
        for ek in ("TEST|TEST", "1111|TEST|TEST"):
            dconn.execute(
                """
                UPDATE audit_disposition
                   SET note=COALESCE(note,'') || ?
                 WHERE entity_key=? AND finding_type='SPOUSE_STORE_DIVERGENCE'
                """,
                (
                    f" [is_test fingerprint-split note 2159: prefer client_id|last4|name; seen as {ek}]",
                    ek,
                ),
            )
        dconn.commit()
        dconn.close()

    payload = {
        "generated_at": now,
        "marked_n": sum(1 for m in marked if not m.get("already")),
        "already_n": sum(1 for m in marked if m.get("already")),
        "clients_is_test_total": n_test,
        "marked": marked,
        "phantom_false_positive_n": len(disp_updated),
        "phantom_false_positive_keys": disp_updated,
        "fingerprint_fix": (
            "disposition SPOUSE_STORE_DIVERGENCE entity_key now "
            "client_id|last4|norm_name (always includes last4 slot + client_id)"
        ),
    }
    OUT.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print("marked", payload["marked_n"], "is_test_total", n_test, "phantoms_fp", len(disp_updated))
    print("wrote", OUT)


if __name__ == "__main__":
    main()
