import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

TAXOPS = Path(r"T:\taxops\taxops.db")
OUT = Path(r"T:\audit\investigation\W4-contam-triage.json")
OUT_MD = Path(r"T:\audit\investigation\W4-contam-triage.md")

doc = json.loads(OUT.read_text(encoding="utf-8"))
conn = sqlite3.connect(f"file:{TAXOPS}?mode=ro", uri=True)
conn.row_factory = sqlite3.Row
n_sp = conn.execute("SELECT count(*) FROM spouses").fetchone()[0]
n_tp = conn.execute(
    "SELECT count(*) FROM spouses WHERE taxpayer_name IS NOT NULL AND trim(taxpayer_name)!=''"
).fetchone()[0]
huerta = conn.execute(
    "SELECT last_name, first_name, taxpayer_name, source FROM spouses WHERE client_id=495"
).fetchone()
ramon = conn.execute(
    "SELECT last_name, first_name, source FROM spouses WHERE client_id=139"
).fetchone()
print("spouses", n_sp, "with_taxpayer_name", n_tp)
print("huerta", dict(huerta) if huerta else None)
print("ramon", dict(ramon) if ramon else None)

# Annotate residual artifact with prior apply batch note
doc["prior_apply_batch"] = {
    "note": "2026-08-12 apply: 11 REPLACE (joint_first_name) + 51 DETACH "
    "(drake_blank/business); residual 27 NOT_IN_SPOUSE_EXPORT",
    "n_replace": 11,
    "n_detach": 51,
}
doc["post_counts"] = {"spouses": n_sp, "with_taxpayer_name_set": n_tp}
OUT.write_text(json.dumps(doc, indent=2), encoding="utf-8")

lines = [
    "# W4 — contamination triage (DETACH + joint)",
    "",
    f"_Residual after apply · {datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ')}_",
    "",
    "## Apply batch",
    "",
    "| Action | n |",
    "|---|---:|",
    "| REPLACE from joint `first_name` | 11 |",
    "| DETACH (Drake blank spouse / business) | 51 |",
    "| Residual NEEDS_HUMAN | 27 |",
    "",
    "All residual reason: `NOT_IN_SPOUSE_EXPORT` (owner not in TY2025 spouse CSV; "
    "no safe clients.spouse_* / joint parse).",
    "",
    f"Live: spouses={n_sp}, taxpayer_name still set={n_tp}",
    "",
    "## Residual sample",
    "",
]
for p in doc["human_plans"][:20]:
    lines.append(
        f"- `{p['client_id']}` {p['owner']}: `{p['wrong_spouse']}` ← `{p['taxpayer_name']}`"
    )
lines += ["", f"Machine: `{OUT}`"]
OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")
print("wrote", OUT_MD)
conn.close()
