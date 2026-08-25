import json
from pathlib import Path

PACKET = Path(r"T:\audit\investigation\W-office-packet.json")
OUT = Path(r"C:\Users\Windows 10\.cursor\projects\t\canvases\drake-log-office-review.canvas.tsx")
TPL = Path(r"T:\audit\investigation\_office_canvas_template.tsx")

p = json.loads(PACKET.read_text(encoding="utf-8"))
tasks = []
for t in p["tasks"]:
    alts = t.get("alternatives") or []
    pref = t.get("preferred_to")
    alt_s = ""
    if t["verb"] == "REVIEW" and alts:
        parts = []
        for a in alts:
            star = "*" if pref and a["to_bare"] == pref else ""
            parts.append(
                f"{a['to_bare']}{star} {a.get('to_name') or ''} ({a.get('to_status') or ''})"
            )
        alt_s = " | ".join(parts)
    tasks.append(
        {
            "verb": t["verb"],
            "from": t["wrong_bare"],
            "claimant": t["claimant"],
            "to": t["to_bare"] or "",
            "toName": t["to_name"] or "",
            "status": t["to_status"] or "",
            "doAfter": "; ".join(t.get("do_after") or []),
            "alts": alt_s,
            "note": t.get("note") or "",
            "priority": bool(t["priority_case"]),
        }
    )
data = {
    "meta": {"generated": p["generated_at"], "cut": p["pinned_cut"]},
    "counts": p["counts"],
    "fixes": p["lucy_fixes"],
    "tasks": tasks,
    "cases": [
        {"bare": c["wrong_bare"], "keeper": c["canonical_name"] or "", "verbs": c["verbs"]}
        for c in p["cases"]
    ],
}
OUT.write_text(
    TPL.read_text(encoding="utf-8").replace("__DATA__", json.dumps(data, indent=2)),
    encoding="utf-8",
)
print("ok", OUT.stat().st_size)
