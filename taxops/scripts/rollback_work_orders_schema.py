"""WO-1/WO-2/WO-6/WO-7 M0: rollback path for the Work Order Creator + billing
request + quick-pick components + notifications schema.

Drops the tables added for the Work Order Creator (work_orders,
work_order_items, work_order_quick_picks, work_order_quick_pick_components,
billing_requests, notifications) and their indexes. Does NOT touch any other
TaxOps table — returns/clients/auth_users etc. are completely untouched,
since work_orders only *references* them (FK), never modifies them.

Note: `notifications` is intentionally generic (not work-order-specific) —
if some OTHER feature starts writing to it before you run this rollback,
drop it from _TABLES below first so you don't take that feature down too.

This does NOT restore CURRENT_SCHEMA_VERSION in db.py — that's a code
change, not a data change; only run this if you are also reverting the
code that added these tables.

Usage (from taxops/ directory, ideally against a DB COPY first):

    python scripts\\rollback_work_orders_schema.py            # dry run (default)
    python scripts\\rollback_work_orders_schema.py --confirm   # actually drop
"""
from __future__ import annotations

import argparse
import sys

sys.path.insert(0, ".")

from db import get_connection  # noqa: E402

_TABLES = (
    "billing_requests",
    "notifications",
    "work_order_items",
    "work_order_quick_pick_components",
    "work_order_quick_picks",
    "work_orders",
)
_INDEXES = (
    "ux_work_orders_number",
    "idx_work_orders_status_date",
    "idx_work_orders_client",
    "idx_work_orders_assigned",
    "idx_work_order_items_wo",
    "idx_wo_qp_components_pick",
    "ux_billing_requests_wo",
    "idx_billing_requests_status",
    "idx_notifications_user_unread",
)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--confirm", action="store_true", help="Actually drop the tables (default: dry run)")
    args = parser.parse_args()

    conn = get_connection()
    try:
        placeholders = ",".join("?" * len(_TABLES))
        existing = {
            row["name"]
            for row in conn.execute(
                f"SELECT name FROM sqlite_master WHERE type='table' AND name IN ({placeholders})",
                _TABLES,
            ).fetchall()
        }
        counts = {}
        for t in _TABLES:
            if t in existing:
                counts[t] = conn.execute(f"SELECT COUNT(*) c FROM {t}").fetchone()["c"]

        print("Work Order tables found:")
        for t in _TABLES:
            status = f"{counts[t]} row(s)" if t in existing else "(not present)"
            print(f"  {t}: {status}")

        if not args.confirm:
            print("\nDry run only — pass --confirm to actually drop these tables.")
            return 0

        # billing_requests and work_order_items both have ON DELETE CASCADE
        # from work_orders, but drop child-first anyway for clarity and to
        # work even if FK enforcement is off for some reason.
        for t in _TABLES:  # already child-first order
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        for idx in _INDEXES:
            conn.execute(f"DROP INDEX IF EXISTS {idx}")
        conn.commit()
        print("\nDropped work_orders, work_order_items, work_order_quick_picks, "
              "billing_requests and their indexes.")
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
