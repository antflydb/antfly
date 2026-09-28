"use client";

import { Card, CardContent, CardHeader, CardTitle } from "@antfly/design-system";
import { CodeLink } from "@/components/code/code-link";
import { SourceLinkProvider } from "@/components/code/source-link-context";
import { BytesBar, ComparisonBars, JourneyChart } from "@/components/viz/perf-charts";
import {
  bytesBreakdown,
  comparisonSamples,
  finalsM4Pro,
  journeyE2bAir,
  machines,
  PERF_PLAN,
  refutedLevers,
  rooflineCeiling,
  splitGqaRetune,
} from "@/content/perf";

export function PerfClient({ permalinkBase }: { permalinkBase?: string }) {
  return (
    <SourceLinkProvider permalinkBase={permalinkBase}>
      <div className="mx-auto max-w-6xl space-y-12 px-4 py-8">
        <header className="max-w-3xl">
          <h1 className="text-3xl font-bold tracking-tight">Performance &amp; roofline</h1>
          <p className="mt-2 text-muted-foreground">
            A guide to interpreting the historical experiments in the Gemma4 performance ledger.
            These numbers describe the artifacts, builds, machines, and protocols recorded there;
            they are not measurements of the current checkout or a current runtime ranking.
          </p>
          <p className="mt-3">
            <CodeLink link={{ path: PERF_PLAN }} label="Read the benchmark ledger and provenance" />
          </p>
        </header>

        <section className="grid gap-6 lg:grid-cols-[1fr_320px]">
          <div>
            <h2 className="mb-3 text-lg font-semibold">What a bandwidth roofline shows</h2>
            <p className="text-sm text-muted-foreground">
              In bandwidth-limited decode, an ideal throughput ceiling is memory bandwidth divided
              by bytes moved per token. The historical E4B estimate uses 273 GB/s ÷ 2.829 GB/token ≈
              96.5 tok/s. The inverse, bytes per token divided by bandwidth, is the ideal time per
              token: about 10.4 ms.
            </p>
            <p className="mt-3 text-sm text-muted-foreground">
              This is a workload model. Quantization, context length, KV traffic, caching, sampling,
              dispatch overhead, and device utilization change actual throughput. Prefill often has
              a different balance because matrix multiplication reuses weights across many token
              rows.
            </p>
          </div>
          <aside>
            <h3 className="mb-3 text-sm font-semibold">Historical E4B Q4_0 bytes per token</h3>
            <BytesBar entries={bytesBreakdown} />
            <p className="mt-3 text-xs text-muted-foreground">
              Rounded tensor-table estimates before later optimizations. FFN and LM-head traffic
              dominate this example.
            </p>
          </aside>
        </section>

        <section>
          <h2 className="mb-3 text-lg font-semibold">
            Documented strict decode comparison · §16.3
          </h2>
          <p className="mb-4 max-w-3xl text-sm text-muted-foreground">
            {machines.pro}. Q4_0 artifacts, 23 prompt tokens, 256 requested output tokens, greedy
            sampling, f16 KV, EOS ignored. One warmup and six interleaved Antfly AB/BA pairs compare
            the previous and tuned policies; llama.cpp v10342 was refreshed separately over five
            fresh processes. Antfly counts 255 decode evaluations as (output tokens − 1) / inner
            decode seconds. This excludes prefill, loading, and HTTP overhead.
          </p>
          <div className="grid gap-4 sm:grid-cols-2">
            {finalsM4Pro.map((f) => (
              <Card key={f.model}>
                <CardHeader>
                  <CardTitle className="text-base">Gemma4 {f.model}</CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="text-3xl font-bold tabular-nums">
                    {f.antfly} <span className="text-sm font-normal">tok/s</span>
                  </div>
                  <p className="mt-2 text-sm text-muted-foreground">
                    {f.pct} of the recorded llama.cpp {f.llama} tok/s.
                  </p>
                  <p className="mt-2 text-xs text-muted-foreground">
                    {f.context}. Long-context values were carried forward, not rerun in §16.3.
                  </p>
                </CardContent>
              </Card>
            ))}
          </div>
          <p className="mt-4 text-sm text-muted-foreground">
            {splitGqaRetune.detail} The ledger records the campaign and later hardened binaries
            separately; do not attribute these measurements to an unmeasured build.
          </p>
        </section>

        <section>
          <h2 className="mb-3 text-lg font-semibold">
            Initial observations · E4B, 64 output tokens
          </h2>
          <p className="mb-4 text-sm text-muted-foreground">
            Historical planning inputs from §1. Internal decode and end-to-end timing differ; peer
            configurations and weight formats were not fully reconciled. Bar lengths visualize the
            reported values and do not establish an apples-to-apples comparison.
          </p>
          <ComparisonBars samples={comparisonSamples} ceiling={rooflineCeiling} />
        </section>

        <section>
          <h2 className="mb-3 text-lg font-semibold">
            Historical optimization journey · E2B on the Air
          </h2>
          <p className="mb-4 text-sm text-muted-foreground">
            {machines.air}. Points summarize separate ledger stages, including optional
            configurations. They are not additive gains or current defaults.
          </p>
          <JourneyChart entries={journeyE2bAir} />
          <div className="mt-4 grid gap-4 sm:grid-cols-2">
            <div className="rounded-lg border p-4">
              <h3 className="mb-2 text-sm font-semibold">Experiments that did not pay off</h3>
              <ul className="space-y-2 text-xs text-muted-foreground">
                {refutedLevers.map((r) => (
                  <li key={r.label}>
                    <strong>{r.label}</strong>: {r.note}
                  </li>
                ))}
              </ul>
            </div>
            <div className="rounded-lg border p-4">
              <h3 className="mb-2 text-sm font-semibold">
                Why speedup estimates cannot be stacked
              </h3>
              <p className="text-xs text-muted-foreground">
                Time savings and throughput gains are nonlinear, and fusion, dispatch, and memory
                optimizations can overlap. The ledger's early gap estimates are hypotheses. A route
                being implemented does not prove its projected gain or make it the default.
              </p>
            </div>
          </div>
        </section>
      </div>
    </SourceLinkProvider>
  );
}
