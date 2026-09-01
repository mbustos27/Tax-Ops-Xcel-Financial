"""Create missing SANDOVAL, RUDY from Drake client-export into TaxOps."""

from __future__ import annotations

import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from client_export_sync import normalize_client_export_row
from config import DRAKE_STATUS_MAP
from db import get_connection
from drake_form_sync import upsert_return_forms
from drake_importer import _match_return, _upsert_client, _upsert_return
from utils import now

EXPORT_ROW = {
    "Display Name": "SANDOVAL, RUDY",
    "Client Type": "Individual",
    "Tax ID (Last 4)": "5470",
    "Primary Email": "RUDSTERSANDOVAL@ATT.NET",
    "Primary Phone": "3237753567",
    "Owner": "",
    "Engagement Status": "Rolled from prior year",
}


def main(*, apply: bool) -> None:
    norm = normalize_client_export_row(EXPORT_ROW, tax_year=2025)
    assert norm is not None
    raw = (norm["returns"].get("drake_status_raw") or "").strip()
    status = DRAKE_STATUS_MAP.get(raw.upper(), "PROCESSING")
    norm["returns"]["client_status"] = status

    conn = get_connection()
    try:
        match = _match_return(conn, norm)
        print("before match:", match)
        if match.get("return_id") and not match.get("ambiguous"):
            print("Already present — nothing to do.")
            return

        if not apply:
            print("DRY-RUN would create client SANDOVAL, RUDY tin=5470 TY2025 status=", status)
            print("Re-run with --apply to commit.")
            return

        client_id, created_c, _ = _upsert_client(conn, norm["clients"], match.get("client_id"))
        # client export fields not handled by _upsert_client
        conn.execute(
            """
            UPDATE clients
            SET taxpayer_email = COALESCE(NULLIF(taxpayer_email,''), ?),
                taxpayer_cell  = COALESCE(NULLIF(taxpayer_cell,''), ?),
                updated_at = ?
            WHERE id = ?
            """,
            (
                norm["clients"].get("taxpayer_email"),
                norm["clients"].get("taxpayer_cell"),
                now(),
                client_id,
            ),
        )
        return_id, created_r, _, _, after = _upsert_return(
            conn, client_id, norm["returns"], match.get("return_id")
        )
        upsert_return_forms(conn, return_id, norm.get("return_forms") or {}, overwrite=True)
        conn.commit()
        print(
            f"CREATED client_id={client_id} (new={created_c}) "
            f"return_id={return_id} (new={created_r}) status={after.get('client_status')} "
            f"drake={after.get('drake_status_raw')}"
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
