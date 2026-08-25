"""Build human-facing Drake collision review packet.

Verbs: MOVE / KEEP / MINT / CLAIM / REVIEW.
Lucy packet fixes (2026-08-12):
  1. QUINTANA 1103 → REVIEW (Jr vs L; prefer 1103 via Kayla/W5)
  2. do_after when destination bare is still being cleared
  3. REVIEW shows all Log alternatives (DURAN 640 + 653)
  4. 1038 MONTIEL → CLAIM vacant 1038 after BARAJAS MOVE (not MINT)
  5. SANTIAGO/DENISE Log transpose note; priority-case MOVEs not re-listed in §2
"""
from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path

SRC = Path(r"T:\audit\investigation\W1-full-log-claimant-search.json")
OUT_MD = Path(r"T:\audit\investigation\W-office-packet.md")
OUT_JSON = Path(r"T:\audit\investigation\W-office-packet.json")
OUT_CSV = Path(r"T:\audit\investigation\W-office-packet.csv")
OFFICE_ORDER = Path(r"T:\audit\investigation\W-office-queue-order.md")
CANVAS = Path(
    r"C:\Users\Windows 10\.cursor\projects\t\canvases\drake-log-office-review.canvas.tsx"
)

VERB = {
    "CORRECT_DRAKE_BAND1_SAFE": "MOVE",
    "CORRECT_DRAKE_BAND2_REVIEW": "REVIEW",
    "KEEPER_CANDIDATE": "KEEP",
    "MINT_NEW_LOG": "MINT",
}

# Manual Lucy overrides keyed by (wrong_bare, claimant upper)
OVERRIDES: dict[tuple[str, str], dict] = {
    ("1103", "QUINTANA, ROGELIO"): {
        "verb": "REVIEW",
        "preferred_to": "1103",
        "note": (
            "Jr vs L ambiguity. W5 last4 5180 has spouse KAYLA -> favors "
            "QUINTANA, ROGELIO JR on 1103 (likely KEEP). Do not MOVE to 464 "
            "(ROGELIO L) without preparer confirm."
        ),
    },
    ("1038", "MONTIEL FREYRE, SAMANTA"): {
        "verb": "CLAIM",
        "to_bare": "1038",
        "to_name": "(vacant after BARAJAS moves to 933)",
        "to_status": "VACANT",
        "note": (
            "Bare 1038 has no Log row; after JORGE BARAJAS MOVE to 933 it is "
            "unissued. Log Samanta at 1038 instead of minting a new number."
        ),
    },
    ("589", "SANTIAGO, DENISE"): {
        "note": (
            "Log stores transposed name DENISE, SANTIAGO -- correct Log to "
            "SANTIAGO, DENISE while editing (name-tier match will keep failing)."
        ),
    },
}


def _bare_key(b: str | int | None) -> tuple:
    s = str(b or "")
    if s == "141":
        return (0, 0)
    try:
        return (1, int(s))
    except ValueError:
        return (2, s)


def _alts(hits: list) -> list[dict]:
    out = []
    seen = set()
    for h in hits or []:
        bare = str(h.get("bare_log") or "")
        if not bare or bare in seen:
            continue
        seen.add(bare)
        out.append(
            {
                "to_bare": bare,
                "to_name": h.get("name"),
                "to_status": h.get("status"),
                "to_band": h.get("band"),
                "to_row": h.get("source_row"),
            }
        )
    return out


def _task(collision_bare: str, o: dict, canonical: dict | None) -> dict:
    action = o.get("action") or ""
    verb = VERB.get(action, action)
    hits = o.get("log_hits") or []
    alts = _alts(hits)
    # Prefer band1 hit for MOVE default destination
    hit = {}
    for h in hits:
        if (h.get("band") or "") == "band1":
            hit = h
            break
    if not hit and hits:
        hit = hits[0]

    to_bare = hit.get("bare_log")
    if verb == "KEEP":
        to_bare = collision_bare
        if not hit and canonical:
            hit = {
                "name": canonical.get("name"),
                "status": canonical.get("status"),
                "band": canonical.get("band"),
                "source_row": canonical.get("source_row"),
                "bare_log": collision_bare,
            }

    task = {
        "priority_case": str(collision_bare) == "141",
        "verb": verb,
        "internal_action": action,
        "wrong_bare": str(collision_bare),
        "claimant": o.get("claimant"),
        "to_bare": str(to_bare) if to_bare is not None else None,
        "to_name": hit.get("name"),
        "to_status": hit.get("status"),
        "to_band": hit.get("band"),
        "to_row": hit.get("source_row"),
        "alternatives": alts,
        "do_after": [],
        "note": None,
        "canonical_name": (canonical or {}).get("name"),
        "fuzzy_only": bool(o.get("fuzzy_only")),
        "in_priority_section": False,  # set later for 141 MOVE dup suppression
    }

    ov = OVERRIDES.get((str(collision_bare), (o.get("claimant") or "").upper()))
    if ov:
        if "verb" in ov:
            task["verb"] = ov["verb"]
        if "to_bare" in ov:
            task["to_bare"] = ov["to_bare"]
        if "to_name" in ov:
            task["to_name"] = ov["to_name"]
        if "to_status" in ov:
            task["to_status"] = ov["to_status"]
        if "note" in ov:
            task["note"] = ov["note"]
        pref = ov.get("preferred_to")
        if pref:
            task["preferred_to"] = str(pref)
            # surface preferred as primary to_* when present in alts
            for a in alts:
                if a["to_bare"] == str(pref):
                    task["to_bare"] = a["to_bare"]
                    task["to_name"] = a["to_name"]
                    task["to_status"] = a["to_status"]
                    task["to_band"] = a["to_band"]
                    task["to_row"] = a["to_row"]
                    break
            # Ensure preferred + other hits both listed
            if not any(a["to_bare"] == str(pref) for a in alts):
                alts.insert(
                    0,
                    {
                        "to_bare": str(pref),
                        "to_name": task["to_name"],
                        "to_status": task["to_status"],
                        "to_band": task.get("to_band"),
                        "to_row": task.get("to_row"),
                    },
                )
            task["alternatives"] = alts

    return task


def _apply_do_after(tasks: list[dict]) -> None:
    """Sequence tasks that depend on other MOVEs clearing a bare first."""
    clearing: dict[str, list[str]] = {}
    for t in tasks:
        if t["verb"] == "MOVE":
            clearing.setdefault(t["wrong_bare"], []).append(t["claimant"])

    for t in tasks:
        if t["verb"] == "CLAIM":
            # Vacant-bare claim: wait until other MOVEs leave this collision bare
            clearers = [n for n in (clearing.get(t["wrong_bare"]) or []) if n != t["claimant"]]
            dest_label = t["wrong_bare"]
        elif t["verb"] in ("MOVE", "REVIEW"):
            dest = t.get("to_bare")
            if not dest or dest == t["wrong_bare"]:
                continue
            clearers = clearing.get(dest) or []
            dest_label = dest
        else:
            continue
        if not clearers:
            continue
        t["do_after"] = [f"{name} off `{dest_label}`" for name in clearers]
        extra = f"Do after: {', '.join(t['do_after'])}."
        t["note"] = f"{t['note']} {extra}".strip() if t.get("note") else extra


def build(doc: dict) -> dict:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    cases = []
    tasks = []

    for coll in doc.get("collisions") or []:
        bare = str(coll.get("bare_log"))
        can = coll.get("canonical_on_bare") or {}
        can_d = can if isinstance(can, dict) else {}
        outcomes = coll.get("claimant_outcomes") or []
        case_tasks = [_task(bare, o, can_d) for o in outcomes]
        # Mark 141 tasks as covered by priority section (suppress MOVE re-list)
        if bare == "141":
            for t in case_tasks:
                t["in_priority_section"] = True
        case_tasks.sort(
            key=lambda t: (
                {"KEEP": 0, "MOVE": 1, "CLAIM": 2, "REVIEW": 3, "MINT": 4}.get(
                    t["verb"], 9
                ),
                t["claimant"] or "",
            )
        )
        cases.append(
            {
                "wrong_bare": bare,
                "priority_case": bare == "141",
                "classification": coll.get("classification"),
                "canonical_name": can_d.get("name"),
                "canonical_row": can_d.get("source_row"),
                "canonical_status": can_d.get("status"),
                "canonical_band": can_d.get("band"),
                "tasks": case_tasks,
                "verbs": sorted({t["verb"] for t in case_tasks}),
            }
        )
        tasks.extend(case_tasks)

    _apply_do_after(tasks)

    cases.sort(key=lambda c: _bare_key(c["wrong_bare"]))
    verb_rank = {"KEEP": 0, "MOVE": 1, "CLAIM": 2, "MINT": 3, "REVIEW": 4}
    tasks.sort(
        key=lambda t: (
            0 if t["priority_case"] else 1,
            verb_rank.get(t["verb"], 9),
            _bare_key(t["wrong_bare"]),
            t["claimant"] or "",
        )
    )

    counts = {
        "cases": len(cases),
        "tasks": len(tasks),
        "MOVE": sum(1 for t in tasks if t["verb"] == "MOVE"),
        "REVIEW": sum(1 for t in tasks if t["verb"] == "REVIEW"),
        "KEEP": sum(1 for t in tasks if t["verb"] == "KEEP"),
        "MINT": sum(1 for t in tasks if t["verb"] == "MINT"),
        "CLAIM": sum(1 for t in tasks if t["verb"] == "CLAIM"),
        "priority_141_tasks": sum(1 for t in tasks if t["priority_case"]),
        "move_checklist": sum(
            1
            for t in tasks
            if t["verb"] == "MOVE" and not t.get("in_priority_section")
        ),
        "with_do_after": sum(1 for t in tasks if t.get("do_after")),
    }
    return {
        "generated_at": now,
        "pinned_cut": doc.get("pinned_cut"),
        "source": "W1-full-log-claimant-search.json",
        "lucy_fixes": [
            "QUINTANA 1103 pulled MOVE→REVIEW (Jr/L; prefer 1103 via Kayla)",
            "do_after on MOVE/CLAIM landing on bares still being cleared",
            "REVIEW lists all Log alternatives (DURAN 640+653)",
            "1038 MONTIEL MINT→CLAIM vacant after BARAJAS MOVE",
            "589 DENISE Log transpose note; 141 MOVEs only in priority section",
        ],
        "hold": [
            "Do not split Tax Log genuine_reuse keys in TaxOps — Drake corrections only.",
        ],
        "how_to": {
            "MOVE": "In Drake, change this client's log/invoice FROM wrong_bare TO to_bare.",
            "KEEP": "Leave on wrong_bare — canonical Log household for that number.",
            "MINT": "Assign a new unused log number (bare already occupied by keeper).",
            "CLAIM": "After clearers leave, assign this client onto the now-vacant collision bare.",
            "REVIEW": "Ambiguous or band2 destination — confirm with preparer; see alternatives.",
        },
        "order": [
            "Priority case bare 141 (I7)",
            "MOVE checklist (excludes 141 — already above)",
            "KEEP + MINT / CLAIM (CLAIM after its do_after clearers)",
            "REVIEW last (suffix / band2 ambiguity)",
        ],
        "counts": counts,
        "cases": cases,
        "tasks": tasks,
    }


def _alt_cell(t: dict) -> str:
    alts = t.get("alternatives") or []
    pref = t.get("preferred_to")
    if len(alts) <= 1 and not pref:
        return "—"
    parts = []
    for a in alts:
        mark = " ★" if pref and a["to_bare"] == pref else ""
        parts.append(
            f"`{a['to_bare']}` {a.get('to_name') or ''} ({a.get('to_status') or '?'}){mark}"
        )
    return "; ".join(parts)


def write_md(packet: dict) -> None:
    c = packet["counts"]
    how = packet["how_to"]
    lines = [
        "# Human review — Drake log collisions",
        "",
        f"_Generated: {packet['generated_at']} · cut={packet['pinned_cut']} · "
        f"{c['cases']} cases / {c['tasks']} tasks_",
        "",
        "## Hold",
        "",
    ]
    for h in packet["hold"]:
        lines.append(f"- {h}")

    lines += [
        "",
        "## Lucy packet fixes",
        "",
    ]
    for f in packet["lucy_fixes"]:
        lines.append(f"- {f}")

    lines += [
        "",
        "## Verbs",
        "",
        "| Verb | n | Meaning |",
        "|---|---:|---|",
        f"| `MOVE` | {c['MOVE']} | {how['MOVE']} |",
        f"| `KEEP` | {c['KEEP']} | {how['KEEP']} |",
        f"| `CLAIM` | {c['CLAIM']} | {how['CLAIM']} |",
        f"| `MINT` | {c['MINT']} | {how['MINT']} |",
        f"| `REVIEW` | {c['REVIEW']} | {how['REVIEW']} |",
        "",
        "## Work order",
        "",
    ]
    for i, step in enumerate(packet["order"], 1):
        lines.append(f"{i}. {step}")

    # §1 Priority 141
    case_141 = next((x for x in packet["cases"] if x["wrong_bare"] == "141"), None)
    lines += ["", "## 1. Priority — bare `141` (I7)", ""]
    if case_141:
        lines.append(
            f"Canonical on Log: `{case_141['canonical_name']}` "
            f"(row {case_141['canonical_row']}, {case_141['canonical_status']})"
        )
        lines += [
            "",
            "| Verb | Claimant | To # | Log name | Status |",
            "|---|---|---|---|---|",
        ]
        for t in case_141["tasks"]:
            lines.append(
                f"| `{t['verb']}` | {t['claimant']} | `{t['to_bare'] or '—'}` | "
                f"{t['to_name'] or '—'} | {t['to_status'] or '—'} |"
            )
        lines.append("")
        lines.append(
            "_VILLAREAL MOVE lives only here — not repeated in the MOVE checklist below._"
        )

    # §2 MOVE (exclude priority-section rows)
    moves = [
        t
        for t in packet["tasks"]
        if t["verb"] == "MOVE" and not t.get("in_priority_section")
    ]
    lines += [
        "",
        f"## 2. MOVE checklist ({len(moves)})",
        "",
        "Change Drake log/invoice **from → to**. Honor **Do after** before checking off.",
        "",
        "| ☐ | From # | Claimant | To # | Log name | Status | Do after |",
        "|---|---|---|---|---|---|---|",
    ]
    for t in moves:
        da = ", ".join(t["do_after"]) if t.get("do_after") else "—"
        lines.append(
            f"| ☐ | `{t['wrong_bare']}` | {t['claimant']} | `{t['to_bare']}` | "
            f"{t['to_name']} | {t['to_status']} | {da} |"
        )

    # §3 KEEP / MINT / CLAIM
    km_bares = sorted(
        {
            t["wrong_bare"]
            for t in packet["tasks"]
            if t["verb"] in ("KEEP", "MINT", "CLAIM")
        },
        key=_bare_key,
    )
    lines += [
        "",
        f"## 3. KEEP / MINT / CLAIM ({c['KEEP']} keep / {c['MINT']} mint / {c['CLAIM']} claim)",
        "",
        "KEEP+MINT: confirm keeper, then mint. CLAIM: wait for Do after, then use vacant bare.",
        "",
    ]
    for bare in km_bares:
        case = next(x for x in packet["cases"] if x["wrong_bare"] == bare)
        lines.append(f"### Bare `{bare}` — Log `{case['canonical_name'] or '(none)'}`")
        lines.append("")
        lines.append("| Verb | Claimant | To # | Notes |")
        lines.append("|---|---|---|---|")
        for t in case["tasks"]:
            if t["verb"] not in ("KEEP", "MINT", "CLAIM"):
                continue
            note = t.get("note") or (
                f"stays on `{bare}`"
                if t["verb"] == "KEEP"
                else (
                    "assign **new** unused log #"
                    if t["verb"] == "MINT"
                    else "use vacant collision bare"
                )
            )
            lines.append(
                f"| `{t['verb']}` | {t['claimant']} | `{t['to_bare'] or '—'}` | {note} |"
            )
        lines.append("")

    # §4 REVIEW
    reviews = [t for t in packet["tasks"] if t["verb"] == "REVIEW"]
    lines += [
        f"## 4. REVIEW before change ({len(reviews)})",
        "",
        "Ambiguous destination or active band2. ★ = preferred when annotated.",
        "",
        "| ☐ | From # | Claimant | Primary to # | Alternatives | Status | Notes |",
        "|---|---|---|---|---|---|---|",
    ]
    for t in reviews:
        lines.append(
            f"| ☐ | `{t['wrong_bare']}` | {t['claimant']} | `{t['to_bare'] or '—'}` | "
            f"{_alt_cell(t)} | {t['to_status'] or '—'} | {t.get('note') or '—'} |"
        )

    # Case index
    lines += [
        "",
        "## Case index (by wrong bare)",
        "",
        "| Wrong # | Canonical on Log | Tasks |",
        "|---|---|---|",
    ]
    for case in packet["cases"]:
        verbs = ", ".join(f"`{v}`" for v in case["verbs"])
        lines.append(
            f"| `{case['wrong_bare']}` | {case['canonical_name'] or '—'} | {verbs} |"
        )

    lines += [
        "",
        "---",
        "",
        f"Machine: `{OUT_JSON}` · checklist CSV: `{OUT_CSV}`",
        "",
    ]
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")


def write_csv(packet: dict) -> None:
    cols = [
        "done",
        "order",
        "verb",
        "wrong_bare",
        "claimant",
        "to_bare",
        "to_name",
        "to_status",
        "to_band",
        "do_after",
        "alternatives",
        "note",
        "priority_case",
        "in_priority_section_only",
    ]
    verb_rank = {"KEEP": 1, "MOVE": 2, "CLAIM": 3, "MINT": 4, "REVIEW": 5}
    rows = sorted(
        packet["tasks"],
        key=lambda t: (
            0 if t["priority_case"] else 1,
            verb_rank.get(t["verb"], 9),
            _bare_key(t["wrong_bare"]),
            t["claimant"] or "",
        ),
    )
    with OUT_CSV.open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        for i, t in enumerate(rows, 1):
            alts = "; ".join(
                f"{a['to_bare']}:{a.get('to_name') or ''}({a.get('to_status') or ''})"
                for a in (t.get("alternatives") or [])
            )
            w.writerow(
                {
                    "done": "",
                    "order": i,
                    "verb": t["verb"],
                    "wrong_bare": t["wrong_bare"],
                    "claimant": t["claimant"],
                    "to_bare": t["to_bare"] or "",
                    "to_name": t["to_name"] or "",
                    "to_status": t["to_status"] or "",
                    "to_band": t["to_band"] or "",
                    "do_after": "; ".join(t.get("do_after") or []),
                    "alternatives": alts,
                    "note": t.get("note") or "",
                    "priority_case": "Y" if t["priority_case"] else "",
                    "in_priority_section_only": (
                        "Y"
                        if t.get("in_priority_section") and t["verb"] == "MOVE"
                        else ""
                    ),
                }
            )


def write_order_stub(packet: dict) -> None:
    c = packet["counts"]
    OFFICE_ORDER.write_text(
        "\n".join(
            [
                "# Office queue — pointer",
                "",
                f"_Cut {packet['pinned_cut']} · **`W-office-packet.md`** ({packet['generated_at']})_",
                "",
                f"- **{c['MOVE']}** MOVE · **{c['KEEP']}** KEEP · **{c['CLAIM']}** CLAIM · "
                f"**{c['MINT']}** MINT · **{c['REVIEW']}** REVIEW",
                f"- MOVE checklist rows: **{c['move_checklist']}** (141 MOVE only in §1)",
                f"- Sequenced (`do_after`): **{c['with_do_after']}**",
                "- Hold: Tax Log `genuine_reuse` split",
                "",
            ]
        ),
        encoding="utf-8",
    )


def write_canvas(packet: dict) -> None:
    """Refresh canvas from template+packet via emit helper."""
    _ = packet  # packet already on disk when emit runs
    emit = Path(r"T:\audit\investigation\_emit_office_canvas.py")
    if not emit.exists():
        return
    import runpy

    runpy.run_path(str(emit), run_name="__main__")


def main() -> None:
    doc = json.loads(SRC.read_text(encoding="utf-8"))
    packet = build(doc)
    OUT_JSON.write_text(json.dumps(packet, indent=2), encoding="utf-8")
    write_md(packet)
    write_csv(packet)
    write_order_stub(packet)
    write_canvas(packet)
    print("counts", packet["counts"])
    for t in packet["tasks"]:
        if t.get("do_after") or t["verb"] in ("REVIEW", "CLAIM") or t.get("note"):
            print(
                t["verb"],
                t["wrong_bare"],
                t["claimant"],
                "->",
                t["to_bare"],
                "do_after=",
                t.get("do_after"),
                "alts=",
                len(t.get("alternatives") or []),
            )


if __name__ == "__main__":
    main()
