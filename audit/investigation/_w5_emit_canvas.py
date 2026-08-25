"""Emit W5 canvas with real queue rows from W5-needs-human-queue.json."""
from __future__ import annotations

import json
from pathlib import Path

src = json.loads(Path(r"T:\audit\investigation\W5-needs-human-queue.json").read_text())
# refresh open store div count
import sqlite3

c = sqlite3.connect(r"T:\audit\audit_disposition.sqlite")
store_open = c.execute(
    "SELECT COUNT(*) FROM audit_disposition WHERE finding_type='SPOUSE_STORE_DIVERGENCE' "
    "AND status IN ('OPEN','ACKED')"
).fetchone()[0]
c.close()

rows = []
for q in src["queue"]:
    parts = (q["entity_key"] or "").split("|")
    # spouse_unrecovered|last4|LAST|FIRST
    name = ", ".join(parts[2:]) if len(parts) >= 4 else (q.get("name_hint") or "")
    rows.append(
        {
            "last4": q.get("last4") or "",
            "name": name,
            "hasSp": bool(q.get("has_spouses_row_now")),
        }
    )

queue_lit = ",\n  ".join(
    "{ last4: %r, name: %r, hasSp: %s }"
    % (r["last4"], r["name"], "true" if r["hasSp"] else "false")
    for r in rows
)

canvas = f'''import {{
  Callout,
  Card,
  CardBody,
  CardHeader,
  Divider,
  Grid,
  H1,
  H2,
  Pill,
  Row,
  Select,
  Stack,
  Stat,
  Table,
  Text,
  useCanvasState,
}} from "cursor/canvas";

const QUEUE = [
  {queue_lit}
] as const;

const STATS = {{
  needsHuman: {src["needs_human_n"]},
  hasSpousesNow: {src["now_has_spouse_row"]},
  needLookup: {src["needs_human_n"] - src["now_has_spouse_row"]},
  spouseAmbiguous: {src["spouse_ambiguous_n"]},
  storeDivOpen: {store_open},
  storeDivResolvedWave4: 37,
}};

export default function W5NeedsHuman() {{
  const [filter, setFilter] = useCanvasState<"all" | "yes" | "no">("spFilter", "all");
  const filtered = QUEUE.filter((r) => {{
    if (filter === "yes") return r.hasSp;
    if (filter === "no") return !r.hasSp;
    return true;
  }});

  return (
    <Stack gap={{24}} style={{{{ padding: 24, maxWidth: 960, margin: "0 auto" }}}}>
      <Stack gap={{6}}>
        <H1>Wave 5 — Human review</H1>
        <Text tone="secondary">
          All open NEEDS_HUMAN are subtype spouse_unrecovered · post Wave 4 · 2026-08-11
        </Text>
      </Stack>

      <Callout tone="info" title="Staff owns disposition">
        Confirm the 33 with a spouses row, or look up the 9 without one in Drake.
        Source of truth: T:\\audit\\investigation\\W5-needs-human-queue.md
      </Callout>

      <Grid columns={{4}} gap={{12}}>
        <Card><CardBody><Stat value={{String(STATS.needsHuman)}} label="NEEDS_HUMAN open" /></CardBody></Card>
        <Card><CardBody><Stat value={{String(STATS.hasSpousesNow)}} label="Have spouses row" tone="success" /></CardBody></Card>
        <Card><CardBody><Stat value={{String(STATS.needLookup)}} label="Need Drake lookup" tone="warning" /></CardBody></Card>
        <Card><CardBody><Stat value={{String(STATS.spouseAmbiguous)}} label="SPOUSE_AMBIGUOUS" /></CardBody></Card>
      </Grid>

      <Card>
        <CardHeader>Wave 4 spillover</CardHeader>
        <CardBody>
          <Text>
            Auto-RESOLVED {{STATS.storeDivResolvedWave4}} SPOUSE_STORE_DIVERGENCE after fold.
            {{STATS.storeDivOpen}} remain (real disagreements).
          </Text>
        </CardBody>
      </Card>

      <Divider />

      <Row gap={{12}} align="center" justify="space-between" wrap>
        <H2>spouse_unrecovered ({{filtered.length}})</H2>
        <Select
          value={{filter}}
          onChange={{(v) => setFilter(v as "all" | "yes" | "no")}}
          options={{[
            {{ value: "all", label: "All" }},
            {{ value: "yes", label: "Has spouses row" }},
            {{ value: "no", label: "Needs lookup" }},
          ]}}
        />
      </Row>

      <Table
        headers={{["Last4", "Taxpayer", "Spouses row?", "Action"]}}
        rows={{filtered.map((r) => [
          r.last4,
          r.name,
          r.hasSp ? (
            <Pill tone="success" size="sm">yes</Pill>
          ) : (
            <Pill tone="warning" size="sm">no</Pill>
          ),
          r.hasSp ? "Confirm fold → RESOLVED" : "Drake lookup or WONTFIX if single",
        ])}}
        rowTone={{filtered.map((r) => (r.hasSp ? undefined : "warning"))}}
      />
    </Stack>
  );
}}
'''

out = Path(r"C:\Users\Windows 10\.cursor\projects\t\canvases\w5-needs-human.canvas.tsx")
out.write_text(canvas, encoding="utf-8")
print("wrote", out, "rows", len(rows))
