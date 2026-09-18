"use client";

import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { EnvFlagChip, FusionChip, OpKindBadge, QuantChip, TensorShapeBadge } from "@/components/primitives/chips";
import { Divergence, Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { SpineStrip } from "@/components/spine-strip";
import {
  ActivationGlyph,
  AttentionGlyph,
  EmbeddingGlyph,
  Figure,
  FlowArrow,
  ForkGlyph,
  MatmulGlyph,
  NormGlyph,
  SamplerGlyph,
  WeightGlyph,
} from "@/components/viz/glyphs";
import { KernelTimelineFrame } from "@/components/viz/kernel-timeline-frame";
import { KvCacheBlocks } from "@/components/viz/kv-cache-blocks";
import { OpDagExplorer } from "@/components/viz/op-dag-explorer";
import { SankeyFlow } from "@/components/viz/sankey-flow";
import type { KernelRoute } from "@/lib/schema";
import { fixtureFrame, fixtureKvTrace, fixtureSankey, fixtureSpec } from "./fixtures";

export function KitchenSinkClient({
  routes,
  snippets,
  gitCommit,
  permalinkBase,
}: {
  routes: KernelRoute[];
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}) {
  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="space-y-10 pb-20">
        <section className="mx-auto max-w-7xl space-y-4 px-4 pt-8">
          <h1 className="text-2xl font-bold">Kitchen sink (dev)</h1>
          <SpineStrip modified={["graph", "frames"]} active="graph" />
          <div className="flex flex-wrap items-center gap-2">
            <QuantChip format="q4_k" />
            <QuantChip format="q8_0" />
            <QuantChip format="f16" />
            <TensorShapeBadge shape={{ dims: ["B", "T", 2048], dtype: "f16" }} />
            <OpKindBadge opKind="gqa_paged_attention" group="fused" />
            <FusionChip ops={["head_rms", "rope"]} />
            <EnvFlagChip name="TERMITE_METAL_ENABLE_PIPELINED_DECODE_FRAME" defaultOn />
            {routes[0] && <CodeLink link={routes[0].source} />}
          </div>
        </section>

        <section className="mx-auto max-w-7xl px-4">
          <h2 className="mb-3 text-lg font-semibold">Glyphs</h2>
          <div className="rounded-lg border bg-card p-4">
            <Figure viewBox="0 0 760 130">
              <EmbeddingGlyph x={10} y={40} label="embed" dtype="f16" />
              <FlowArrow x1={100} y1={57} x2={130} y2={57} />
              <NormGlyph x={130} y={53} label="rms" dtype="f32" />
              <FlowArrow x1={200} y1={57} x2={230} y2={57} />
              <AttentionGlyph x={230} y={37} label="GQA" dtype="f16" shutter />
              <FlowArrow x1={320} y1={57} x2={350} y2={57} />
              <MatmulGlyph x={350} y={41} label="FFN" fused={["gate", "up", "silu"]} dtype="q4_0" />
              <FlowArrow x1={460} y1={57} x2={490} y2={57} />
              <ForkGlyph x={490} y={37} label="MoE" dtype="q4_0" />
              <FlowArrow x1={560} y1={57} x2={590} y2={57} />
              <SamplerGlyph x={590} y={40} label="sample" dtype="f32" />
              <WeightGlyph x={350} y={90} label="W_gate_up" quant="q4_0" w={110} h={24} />
              <ActivationGlyph x={660} y={44} label="token" dtype="f32" w={60} h={26} />
            </Figure>
          </div>
        </section>

        <section className="mx-auto max-w-7xl px-4">
          <h2 className="mb-3 text-lg font-semibold">Sankey</h2>
          <div className="rounded-lg border bg-card p-4">
            <SankeyFlow spec={fixtureSankey} />
          </div>
        </section>

        <section className="mx-auto max-w-7xl px-4">
          <h2 className="mb-3 text-lg font-semibold">Frame timeline</h2>
          <div className="rounded-lg border bg-card p-4">
            <KernelTimelineFrame scenario={fixtureFrame} prevScenario={fixtureFrame} />
          </div>
        </section>

        <section className="mx-auto max-w-7xl px-4">
          <h2 className="mb-3 text-lg font-semibold">KV cache blocks</h2>
          <div className="h-80 rounded-lg border bg-card p-4">
            <KvCacheBlocks trace={fixtureKvTrace} />
          </div>
        </section>

        <ScrollyChapter id="sink-ch" number={7} title="Scrollytelling fixture" intro="Two scenes with a pinned graphic.">
          <Scene
            id="s1"
            graphic={
              <Figure viewBox="0 0 400 200" title="scene one">
                <AttentionGlyph x={140} y={70} label="attention" dtype="f16" highlight />
              </Figure>
            }
          >
            <p>
              First scene prose. As this block crosses the viewport band, the pinned graphic shows the attention
              hexagon. <code>gqa_paged_attention</code> is one of the fused op kinds.
            </p>
            <Divergence
              others={<p>walks a ggml graph, re-encodes per token.</p>}
              antfly={<p>plans FrameDescriptors once, re-binds and pipelines.</p>}
            />
          </Scene>
          <Scene
            id="s2"
            graphic={
              <Figure viewBox="0 0 400 200" title="scene two">
                <MatmulGlyph x={120} y={80} label="FFN" fused={["gate", "up", "silu"]} dtype="q4_0" highlight />
              </Figure>
            }
          >
            <p>Second scene prose — the graphic crossfades to the fused FFN node with the zipper border.</p>
          </Scene>
        </ScrollyChapter>

        <section className="px-4">
          <h2 className="mx-auto mb-3 max-w-7xl text-lg font-semibold">DAG explorer (fixture spec)</h2>
          <div className="overflow-hidden rounded-lg border">
            <OpDagExplorer spec={fixtureSpec} routes={routes} height="32rem" />
          </div>
        </section>
      </div>
    </SnippetProvider>
  );
}
