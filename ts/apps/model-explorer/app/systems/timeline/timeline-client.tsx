"use client";

import { parseAsStringLiteral, useQueryState } from "nuqs";
import { Suspense } from "react";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { ChoiceGroup } from "@/components/primitives/choice-group";
import { KernelTimelineFrame } from "@/components/viz/kernel-timeline-frame";
import { L } from "@/lib/links";
import type { FrameScenario } from "@/lib/schema";

interface TimelineProps {
  frames: { q40: FrameScenario; q80: FrameScenario };
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}

export function TimelineClient(props: TimelineProps) {
  return (
    <Suspense fallback={<div className="min-h-screen" />}>
      <TimelineInner {...props} />
    </Suspense>
  );
}

function TimelineInner({ frames, snippets, gitCommit, permalinkBase }: TimelineProps) {
  const [frame, setFrame] = useQueryState(
    "frame",
    parseAsStringLiteral(["q40", "q80"] as const).withDefault("q40")
  );
  const scenario = frame === "q80" ? frames.q80 : frames.q40;
  const isQ40 = frame !== "q80";

  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="mx-auto max-w-6xl space-y-8 px-4 py-8">
        <header className="max-w-3xl">
          <h1 className="text-3xl font-bold tracking-tight">Frame timeline</h1>
          <p className="mt-2 text-muted-foreground">
            One Metal command frame — one <strong>Gemma4</strong> decode step — laid out
            Perfetto-style. (Encoder models like GLiNER2/2.5 have no decode frames; this page is the
            Gemma4 frame anatomy.) Both views here are{" "}
            <em>planned</em> mode: representative encoder scopes, operations and barriers, informed
            by historical census reports. Operations use equal widths because these examples contain
            no byte estimates or measured GPU timings. Captured data could instead scale widths by{" "}
            <code className="font-mono text-sm">gpuNanos</code>.
          </p>
        </header>

        <section>
          <div className="mb-3 flex flex-wrap items-center justify-between gap-2">
            <h2 className="text-lg font-semibold">{scenario.title}</h2>
            <ChoiceGroup
              label="Historical frame scenario"
              value={frame}
              onValueChange={setFrame}
              options={[
                { value: "q80", label: "Q8_0 census example" },
                { value: "q40", label: "Q4_0 census example" },
              ]}
            />
          </div>
          {scenario.source && (
            <p className="mb-2">
              <CodeLink link={scenario.source} label="Historical census source" />
            </p>
          )}
          {scenario.description && (
            <p className="mb-4 max-w-3xl text-sm text-muted-foreground">{scenario.description}</p>
          )}
          <div className="rounded-lg border bg-card p-4">
            <KernelTimelineFrame
              scenario={scenario}
              prevScenario={isQ40 ? frames.q40 : undefined}
            />
          </div>
        </section>

        <section className="grid gap-4 lg:grid-cols-3">
          <div className="rounded-lg border p-4">
            <h3 className="mb-2 text-sm font-semibold">Encoder scopes</h3>
            <p className="text-xs text-muted-foreground">
              The brackets over the GPU lane are <code className="font-mono">EncoderScope</code>s —
              the planner's grouping of planned ops under one compute encoder. In the Q8_0 anchor,
              scope boundaries and actual encoder counts differ (36 planned scopes, 41 reported
              encoders); the Q4_0 example keeps scopes as planning structure but submits them all on
              a single serial encoder.
            </p>
            <p className="mt-2 text-xs">
              <CodeLink link={L("planner-encoder-scope")} /> ·{" "}
              <CodeLink link={L("runtime-begin-frame")} />
            </p>
          </div>
          <div className="rounded-lg border p-4">
            <h3 className="mb-2 text-sm font-semibold">Barriers (hover the red ticks)</h3>
            <p className="text-xs text-muted-foreground">
              Each red tick is a memory barrier the planner emitted for a RAW/WAR/WAW hazard between
              tracked byte ranges — a point where the GPU serializes. The anchor frame carried 422
              of them; the barrier reframe used whole-frame scoped suppression on an eligible
              serial-encoder path, and the Q4_0 census reported zero. Ticks shown are
              representative, not all 422.
            </p>
          </div>
          <div className="rounded-lg border p-4">
            <h3 className="mb-2 text-sm font-semibold">The pipelined third lane</h3>
            <p className="text-xs text-muted-foreground">
              On the historical Q4_0 view, a third lane appears: because the sampled token id stays
              device-resident, the CPU encodes frame N+1 <em>before</em> waiting on frame N. The
              submit→wait→encode bubble can shrink. The historical E2B campaign reported +9–12% with
              matching probe tokens; this figure does not measure that gain.
            </p>
            <p className="mt-2 text-xs">
              <CodeLink link={L("executor-pipelined-decode")} />
            </p>
          </div>
        </section>

        <p className="text-xs text-muted-foreground">
          Scope contents, lane offsets, and overlap are representative (one layer plus the tail),
          not emitted traces — equal operation widths show ordering, not relative cost, duration or
          bytes moved.
        </p>
      </div>
    </SnippetProvider>
  );
}
