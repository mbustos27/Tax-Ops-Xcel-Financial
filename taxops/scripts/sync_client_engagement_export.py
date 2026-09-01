"""Sync Drake client-export CSV (Engagement Status) into TaxOps."""

from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

_TAXOPS = Path(__file__).resolve().parents[1]
if str(_TAXOPS) not in sys.path:
    sys.path.insert(0, str(_TAXOPS))

from client_export_sync import is_client_export_csv, sync_client_export
from db import get_connection

DEFAULT_INCOMING = _TAXOPS / "data" / "incoming" / "client-export.csv"


def _resolve_csv(explicit: str | None) -> Path:
    if explicit:
        p = Path(explicit)
        if not p.is_file():
            raise FileNotFoundError(p)
        return p
    if DEFAULT_INCOMING.is_file():
        return DEFAULT_INCOMING
    raise FileNotFoundError(
        "No client-export CSV — pass --csv or place file at data/incoming/client-export.csv"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Sync Drake client-export engagement status")
    parser.add_argument("--csv", default=None, help="Path to client-export-*.csv")
    parser.add_argument("--tax-year", type=int, default=2025)
    parser.add_argument("--entity-only", action="store_true", help="Corps / S-corps / partnerships / 990 only")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--install", action="store_true", help="Copy CSV to data/incoming/client-export.csv")
    args = parser.parse_args()

    csv_path = _resolve_csv(args.csv)
    if not is_client_export_csv(csv_path):
        raise SystemExit(f"Unrecognized format: {csv_path}")

    if args.install:
        DEFAULT_INCOMING.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(csv_path, DEFAULT_INCOMING)
        print(f"Installed -> {DEFAULT_INCOMING}")

    conn = get_connection()
    try:
        if args.apply:
            conn.execute("BEGIN")
        result = sync_client_export(
            conn,
            csv_path,
            tax_year=args.tax_year,
            entity_only=args.entity_only,
            dry_run=not args.apply,
        )
        if args.apply:
            conn.commit()
    finally:
        conn.close()

    mode = "APPLY" if args.apply else "DRY-RUN"
    print(f"[{mode}] {csv_path.name} TY{args.tax_year} entity_only={args.entity_only}")
    print(f"  status/form rows touched: {result['updated_status']}")
    print(f"  forms merged:             {result['updated_forms']}")
    print(f"  logout_synced:            {result['logout_synced']}")
    print(f"  skipped (no match):       {result['skipped_no_match']}")
    if args.entity_only:
        print(f"  skipped (not entity):     {result['skipped_not_entity']}")
    for s in result.get("samples") or []:
        if "DENTAL" in (s.get("name") or "").upper():
            print(
                f"  >> {s['name']}: {s.get('from_drake')!r} → {s.get('drake_raw')!r} "
                f"status {s.get('from_status')} (engagement {s.get('engagement')!r})"
            )
    if not args.apply:
        print("\nRe-run with --apply to commit.")


if __name__ == "__main__":
    main()
