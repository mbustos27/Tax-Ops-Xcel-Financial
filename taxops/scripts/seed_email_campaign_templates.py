"""CLI wrapper for mass_email_seed — inserts templates only, no sends."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

_TAXOPS_ROOT = Path(__file__).resolve().parent.parent
if str(_TAXOPS_ROOT) not in sys.path:
    sys.path.insert(0, str(_TAXOPS_ROOT))

from db import get_active_intake_tax_year, get_connection
from mass_email_seed import seed_mass_email_platform
from mass_email_templates import ALL_TEMPLATES, TAX_DEADLINE_SEEDS


def main() -> int:
    parser = argparse.ArgumentParser(description="Seed mass-email templates (no sends)")
    parser.add_argument("--db", default=None, help="Database path")
    parser.add_argument("--tax-year", type=int, default=None)
    parser.add_argument("--dry-run", action="store_true", help="Report only; no writes")
    args = parser.parse_args()

    conn = get_connection(args.db)
    try:
        if args.dry_run:
            ty = args.tax_year or get_active_intake_tax_year(conn)
            print(f"Would seed {len(ALL_TEMPLATES)} templates for tax_year={ty}")
            print(f"Would seed up to {len(TAX_DEADLINE_SEEDS)} tax_deadlines rows")
            return 0
        result = seed_mass_email_platform(conn, tax_year=args.tax_year)
    finally:
        conn.close()

    print("=== Mass-email template seed (no sends) ===")
    for key, tid in result["template_ids"].items():
        print(f"  template {key}: id={tid}")
    for name, cid in result["campaign_ids"].items():
        print(f"  draft campaign {name}: id={cid}")
    print(f"  tax_deadlines_added: {result['tax_deadlines_added']}")
    print(f"  tax_year: {result['tax_year']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
