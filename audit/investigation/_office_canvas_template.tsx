import {
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
  Spacer,
  Stack,
  Stat,
  Table,
  Text,
  useCanvasState,
  useHostTheme,
} from "cursor/canvas";

const DATA = __DATA__;

type Verb = "ALL" | "MOVE" | "KEEP" | "CLAIM" | "MINT" | "REVIEW";

const VERB_TONE: Record<string, "info" | "success" | "warning" | "neutral"> = {
  MOVE: "info",
  KEEP: "success",
  CLAIM: "warning",
  MINT: "warning",
  REVIEW: "neutral",
};

export default function DrakeLogOfficeReview() {
  useHostTheme();
  const [lane, setLane] = useCanvasState<Verb>("lane", "ALL");
  const c = DATA.counts;
  const filtered =
    lane === "ALL" ? DATA.tasks : DATA.tasks.filter((t) => t.verb === lane);

  return (
    <Stack gap={20}>
      <Stack gap={6}>
        <H1>Drake log collision review</H1>
        <Text tone="secondary" size="small">
          Cut {DATA.meta.cut} · {DATA.meta.generated} · {c.cases} cases / {c.tasks}{" "}
          tasks · MOVE checklist {c.move_checklist} · do_after {c.with_do_after}
        </Text>
      </Stack>

      <Grid columns={5} gap={12}>
        <Stat value={c.MOVE} label="MOVE" />
        <Stat value={c.KEEP} label="KEEP" />
        <Stat value={c.CLAIM} label="CLAIM" />
        <Stat value={c.MINT} label="MINT" />
        <Stat value={c.REVIEW} label="REVIEW" tone="warning" />
      </Grid>

      <Callout tone="warning" title="Lucy fixes applied">
        {DATA.fixes.join(" · ")}
      </Callout>

      <Callout tone="info" title="Start here">
        Bare 141 (I7) only in priority section. Honor Do after on COLLAZO→203,
        YANEZ→862, and CLAIM MONTIEL@1038. QUINTANA + DURAN are REVIEW with
        alternatives.
      </Callout>

      <Row gap={12} align="center">
        <Text weight="semibold">Lane</Text>
        <Select
          value={lane}
          onChange={(v) => setLane(v as Verb)}
          options={[
            { value: "ALL", label: `All (${c.tasks})` },
            { value: "MOVE", label: `MOVE (${c.MOVE})` },
            { value: "KEEP", label: `KEEP (${c.KEEP})` },
            { value: "CLAIM", label: `CLAIM (${c.CLAIM})` },
            { value: "MINT", label: `MINT (${c.MINT})` },
            { value: "REVIEW", label: `REVIEW (${c.REVIEW})` },
          ]}
        />
        <Text tone="secondary" size="small">
          Showing {filtered.length}
        </Text>
      </Row>

      <Card>
        <CardHeader trailing={<Pill tone="neutral">{filtered.length} rows</Pill>}>
          Tasks
        </CardHeader>
        <CardBody style={{ padding: 0 }}>
          <Table
            stickyHeader
            columns={[
              { key: "verb", header: "Verb", width: "80px" },
              { key: "from", header: "From #", width: "64px" },
              { key: "claimant", header: "Claimant" },
              { key: "to", header: "To #", width: "64px" },
              { key: "toName", header: "Log name" },
              { key: "status", header: "Status", width: "88px" },
              { key: "doAfter", header: "Do after" },
            ]}
            rows={filtered.map((t) => ({
              key: `${t.verb}|${t.from}|${t.claimant}`,
              tone: t.priority
                ? ("warning" as const)
                : t.doAfter
                  ? ("info" as const)
                  : undefined,
              cells: [
                <Pill key="v" tone={VERB_TONE[t.verb] || "neutral"} size="small">
                  {t.verb}
                </Pill>,
                t.from,
                t.priority ? (
                  <Text key="c" weight="semibold">
                    {t.claimant}
                  </Text>
                ) : (
                  t.claimant
                ),
                t.to || "—",
                t.alts ? (
                  <Stack key="n" gap={2}>
                    <Text size="small">{t.toName || "—"}</Text>
                    <Text size="small" tone="secondary">
                      {t.alts}
                    </Text>
                  </Stack>
                ) : (
                  t.toName || "—"
                ),
                t.status || "—",
                t.doAfter || "—",
              ],
            }))}
          />
        </CardBody>
      </Card>

      <Divider />

      <Stack gap={8}>
        <H2>Case index</H2>
        <Table
          stickyHeader
          columns={[
            { key: "bare", header: "Wrong #", width: "88px" },
            { key: "keeper", header: "Canonical on Log" },
            { key: "verbs", header: "Verbs", width: "220px" },
          ]}
          rows={DATA.cases.map((cs) => ({
            key: cs.bare,
            tone: cs.bare === "141" ? ("warning" as const) : undefined,
            cells: [
              cs.bare,
              cs.keeper || "—",
              <Row key="v" gap={6}>
                {cs.verbs.map((v) => (
                  <Pill key={v} tone={VERB_TONE[v] || "neutral"} size="small">
                    {v}
                  </Pill>
                ))}
              </Row>,
            ],
          }))}
        />
      </Stack>

      <Spacer />
      <Text tone="secondary" size="small">
        W-office-packet.md · W-office-packet.csv
      </Text>
    </Stack>
  );
}
