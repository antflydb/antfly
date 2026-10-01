"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip } from "@/components/primitives/chips";
import { Divergence, Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { KernelTimelineFrame } from "@/components/viz/kernel-timeline-frame";
import { BytesBar, ComparisonBars, JourneyChart } from "@/components/viz/perf-charts";
import { SankeyFlow } from "@/components/viz/sankey-flow";
import {
  bytesBreakdown,
  comparisonSamples,
  finalsM4Pro,
  journeyE2bAir,
  machines,
  rooflineCeiling,
} from "@/content/perf";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import { Gemma4EarlyChapters } from "./chapters-early";
import { Gemma4LateChapters } from "./chapters-late";
import { ColdOpenFigure, GraphToFrameFigure, LayerStackFigure } from "./figures";

export function Gemma4Chapters({ spec, routes, frames, kernelCensus }: ChaptersProps) {
  const isE4b = spec.id === "gemma4-e4b";
  const tokS = String(spec.stats.tokS ?? "");

  return (
    <div>
      {/* ── Ch 0 · Cold open ─────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-0"
        number={0}
        title="One text decode step, slowed down"
        intro="A schematic tour of the dense E-series Metal path. Timing labels are historical measurements."
      >
        <Scene
          id="replay"
          graphic={
            <ColdOpenFigure
              layers={Number(spec.stats.layers)}
              tokS={`${tokS} · recorded Q4_0 greedy benchmark`}
            />
          }
        >
          <p>
            Watch one decode step. A token id enters on the left; {spec.stats.layers} transformer
            layers, a {Number(spec.stats.vocab).toLocaleString("en-US")}-entry vocabulary matvec,
            and a token selector later, a new token appears in the stream. Eligible prepared Metal
            routes keep selection on the GPU.
          </p>
          <p>
            A recorded Q4_0 E-series frame used one compute encoder, 143 planned scopes, and no
            explicit planned barriers. Those are scenario-specific counters, not guarantees for
            every model or request. The animation illustrates dependencies, not measured stage
            durations.
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 1 · The shape of the model ─────────────────────────── */}
      <ScrollyChapter
        id="ch-1"
        number={1}
        title={`The shape of ${spec.displayName}`}
        intro={
          <p>
            {spec.stats.layers} layers, hidden size {spec.stats.hidden} — but the interesting
            structure is in which layers get which attention, and which layers own a KV cache at
            all.
          </p>
        }
      >
        <Scene id="iswa" graphic={<LayerStackFigure spec={spec} emphasis="attention" />}>
          <p>
            <strong>The {isE4b ? "5:1" : "4:1"} iSWA rhythm.</strong>{" "}
            {isE4b ? "Five of every six" : "Four of every five"} layers attend through a sliding
            window (shuttered rows); every {isE4b ? "sixth" : "fifth"} layer is a full
            global-attention layer. The pattern is one line of config logic —{" "}
            <code>(layer + 1) % sliding_window_pattern != 0</code> — with the pattern defaulting to
            6 but overridden by model metadata (5 for E2B).
          </p>
          <p>
            <CodeLink link={L("config-layer-uses-sliding")} /> ·{" "}
            <CodeLink link={L("config-sliding-pattern")} />
          </p>
        </Scene>
        <Scene id="headdim" graphic={<LayerStackFigure spec={spec} emphasis="headdim" />}>
          <p>
            <strong>Two head sizes.</strong> Global layers run <code>head_dim 512</code>; sliding
            layers run <code>256</code>. Global attention has twice the per-head width, and the
            attention kernels carry both shapes (<code>hd512</code> has its own generated
            flash-prefill variant).
          </p>
          <p>
            <CodeLink link={L("config-global-head-dim")} />
          </p>
        </Scene>
        <Scene id="sharedkv" graphic={<LayerStackFigure spec={spec} emphasis="kv" />}>
          <p>
            <strong>The tail shares earlier K/V.</strong> On {spec.displayName},{" "}
            {spec.stats.kvOwners} of {spec.stats.layers} layers write K/V; the {spec.stats.sharedKv}{" "}
            tail layers skip the K/V projection entirely and read a donor layer's blocks. That's{" "}
            {isE4b ? "18" : "20"} layers without independent K/V computation; physical allocation
            savings depend on the storage route.
          </p>
          <Divergence
            others={<p>a conventional decoder computes independent K/V in each attention layer.</p>}
            antfly={<p>Gemma E-series tail layers reuse donors of the same attention type.</p>}
            link={<CodeLink link={L("config-shared-kv")} />}
          />
        </Scene>
        <Scene
          id="bytes"
          graphic={
            <div className="flex h-full flex-col justify-center">
              {spec.sankey && <SankeyFlow spec={spec.sankey} unit="MB/token" />}
            </div>
          }
        >
          <p>
            <strong>Follow the bytes.</strong> This is the performance plan's historical E4B Q4_0
            weight-size estimate (the E2B page shows the same estimate for comparison): FFN 65.7%,
            Q6_K LM head 19.5%, attention projections 11.7%. It predates optional repacking and is
            not measured GPU traffic or a per-model live profile.
          </p>
        </Scene>
      </ScrollyChapter>

      <Gemma4EarlyChapters
        spec={spec}
        routes={routes}
        frames={frames}
        kernelCensus={kernelCensus}
      />

      {/* ── Ch 7 · From graph to frames ──────────────────────────── */}
      <ScrollyChapter
        id="ch-7"
        number={7}
        title="From graph to frames"
        intro={
          <p>
            Eligible Metal paths prepare operations and resources for a decode frame. Planning,
            encoding commands, submitting them, and executing on the GPU are distinct steps.
          </p>
        }
      >
        <Scene id="graph" graphic={<GraphToFrameFigure step={0} />}>
          <p>
            A decode step starts as a DAG of ops per layer: norm, QKV, rope, attention, projections,
            the gated FFN. This drawing groups conceptual operations; actual fusion and lowering
            depend on the route.
          </p>
          <p>
            Antfly's planner represents eligible work with a <code>FrameDescriptor</code>: an
            ordered list of <code>PlannedOp</code>s grouped into <code>EncoderScope</code>s, with
            family-specific lowerers for attention setup, gated layers, PLE, and the tail.
          </p>
          <p>
            <CodeLink link={L("planner-frame-descriptor")} /> ·{" "}
            <CodeLink link={L("planner-encoder-scope")} />
          </p>
        </Scene>
        <Scene id="barriers" graphic={<GraphToFrameFigure step={1} />}>
          <p>
            <strong>Barriers only at real hazards.</strong> Within a frame, the runtime tracks
            read/write byte ranges and emits a Metal buffer barrier only where a genuine RAW/WAR/WAW
            hazard exists. The old Q8_0 anchor frame still carried{" "}
            <span className="font-mono">41 encoders and 422 barriers</span> — the ticks here are
            illustrative hazards, not a GPU capture.
          </p>
        </Scene>
        <Scene id="zero" graphic={<GraphToFrameFigure step={2} />}>
          <p>
            The later performance-plan snapshot recorded{" "}
            <span className="font-mono text-primary">
              1 compute encoder, 143 planned scopes, planned_barriers = 0
            </span>{" "}
            under whole-frame suppression on a serial encoder. The Q8_0 anchor and Q4_0 snapshot are
            different configurations, not a matched before/after benchmark. Zero explicit barriers
            does not mean GPU operations run without ordering.
          </p>
          <Divergence
            others={
              <p>
                per-operation dispatch requires coordinating dependencies and lifetimes repeatedly.
              </p>
            }
            antfly={
              <p>
                prepared routes group work and bind resources for each step; encoder policy
                determines ordering.
              </p>
            }
            link={<CodeLink link={L("runtime-submit-frame")} />}
          />
        </Scene>
        <Scene
          id="pipelined"
          graphic={
            <div className="flex h-full flex-col justify-center">
              <KernelTimelineFrame scenario={frames.q40} prevScenario={frames.q40} />
            </div>
          }
        >
          <p>
            <strong>And then the frames overlap.</strong> Because the sampled token id stays
            device-resident (chapter 10), frame N+1 can be{" "}
            <em>encoded from a token that doesn't exist yet on the host</em> — the CPU encodes the
            next frame before waiting on the current one. The submit→wait→encode bubble can shrink.
            Historical M4 Air probes reported +9–12% on E2B and +5–8% on E4B with matching tokens.
            This is CPU/GPU overlap, not two dependent tokens executing concurrently.
          </p>
          <p>
            <CodeLink link={L("executor-pipelined-decode")} />{" "}
            <EnvFlagChip name="TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME" defaultOn />
          </p>
          <p className="text-xs text-muted-foreground">
            Default-on is device-qualified; disable flags and route eligibility still apply.
          </p>
          <p className="text-xs">
            Full view:{" "}
            <Link className="text-primary underline" href="/systems/timeline">
              frame timeline →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>

      <Gemma4LateChapters spec={spec} routes={routes} frames={frames} kernelCensus={kernelCensus} />

      {/* ── Ch 12 · Scoreboard ───────────────────────────────────── */}
      <ScrollyChapter
        id="ch-12"
        number={12}
        title="Historical measurements and the bandwidth ceiling"
        intro={
          <p>
            These charts summarize repository benchmark notes, not a new qualification run. An
            idealized bandwidth ceiling in tokens per second is bandwidth divided by bytes per
            token; its reciprocal is the lower bound on seconds per token.
          </p>
        }
      >
        <Scene
          id="journey"
          graphic={
            <div className="flex h-full flex-col justify-center">
              <JourneyChart entries={journeyE2bAir} />
            </div>
          }
        >
          <p>
            <strong>The journey.</strong> On the fanless M4 Air worktree, E2B went from about 44
            tok/s at branch start to 56.5 — roughly +28%. Four changes account for it: pipelined
            decode frames, an opt-in Q4_K repack of the LM head, pair-fusion, and PLE Q8_0 staging.
            The notes also record a sumsq-fusion probe at <em>−12%</em>, measured and struck.
          </p>
          <p className="text-xs text-muted-foreground">{machines.air}</p>
        </Scene>
        <Scene
          id="compare"
          graphic={
            <div className="flex h-full flex-col justify-center gap-4">
              <ComparisonBars samples={comparisonSamples} ceiling={rooflineCeiling} />
            </div>
          }
        >
          <p>
            <strong>The initial comparison</strong> recorded E4B Q4_0, 64-token benchmark-suite
            results on an M4 Pro. Its internal and end-to-end timing boundaries differ, and peer
            runtime settings were not fully reconciled. Treat these as historical observations; they
            do not establish a current ranking or prove another runtime omitted model work.
          </p>
          <p>
            <CodeLink link={L("perf-plan-comparison")} />
          </p>
        </Scene>
        <Scene
          id="finals"
          graphic={
            <div className="flex h-full flex-col justify-center gap-4">
              <BytesBar entries={bytesBreakdown} />
              <div className="grid gap-3">
                {finalsM4Pro.map((f) => (
                  <div key={f.model} className="rounded-lg border p-3">
                    <span className="text-2xl font-bold tabular-nums">{f.antfly}</span>
                    <span className="ml-2 text-sm text-muted-foreground">
                      tok/s · {f.model} · {f.pct} of llama.cpp
                    </span>
                  </div>
                ))}
              </div>
            </div>
          }
        >
          <p>
            <strong>A later recorded campaign.</strong> The performance plan's split-GQA retune
            measured E2B at 80.7 tok/s and E4B at 56.1 on a pinned M4 Pro setup. The accompanying
            llama.cpp comparison reported 74% and 89%, respectively. These are 256-token campaign
            results; do not combine them with the earlier 64-token chart or treat them as
            current-head performance certification.
          </p>
          <p className="text-xs">
            Full data with caveats:{" "}
            <Link className="text-primary underline" href="/systems/perf">
              the scoreboard →
            </Link>{" "}
            · How this model finetunes:{" "}
            <Link className="text-primary underline" href="/training/gemma4">
              Training → DPO &amp; GRPO
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
