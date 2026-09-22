"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { EnvFlagChip } from "@/components/primitives/chips";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { Figure } from "@/components/viz/glyphs";
import { L } from "@/lib/links";

/* ------------------------------------------------------------------ */
/* Figures                                                             */
/* ------------------------------------------------------------------ */

function GraphFirstFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="the inference graph, with a loss grafted on"
      caption="Schematic. Training reuses the same DeBERTa forward graph the GLiNER2 model page describes
        and grafts loss nodes onto the end. Two things the graph cannot express — precomputed
        indices and the padding bias — are supplied from outside as placeholders."
    >
      <rect
        x={40}
        y={58}
        width={110}
        height={36}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
        stroke="var(--kfam-attention)"
        strokeWidth={1.25}
      />
      <text x={95} y={80} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        DeBERTa encoder
      </text>
      <path d="M 150 76 h 30" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect
        x={182}
        y={58}
        width={90}
        height={36}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)"
        stroke="var(--kfam-fusion)"
        strokeWidth={1.25}
      />
      <text x={227} y={80} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        span head
      </text>
      <path d="M 272 76 h 30" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect
        x={304}
        y={58}
        width={90}
        height={36}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.5}
        strokeDasharray="6 2 2 2"
      />
      <text x={349} y={80} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        loss nodes
      </text>
      <path
        d="M 95 110 v -16 M 227 110 v -16"
        stroke="var(--kfam-kv)"
        strokeWidth={1}
        strokeDasharray="4 2"
      />
      <rect
        x={38}
        y={112}
        width={114}
        height={22}
        rx={4}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={0.9}
        strokeDasharray="4 2"
      />
      <text
        x={95}
        y={127}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        attn bias (frozen, −1e9 pads)
      </text>
      <rect
        x={168}
        y={112}
        width={118}
        height={22}
        rx={4}
        fill="none"
        stroke="var(--kfam-kv)"
        strokeWidth={0.9}
        strokeDasharray="4 2"
      />
      <text
        x={227}
        y={127}
        textAnchor="middle"
        fontSize={7}
        className="fill-muted-foreground font-mono"
      >
        precomputed indices
      </text>
      <text
        x={220}
        y={170}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        placeholders arrive per step from the host — they are inputs, not ops
      </text>
    </Figure>
  );
}

const OBJECTIVES = [
  { id: "token", label: "token", sub: "masked CE — zero rows for ignore_index", x: 90 },
  { id: "span", label: "span_start", sub: "span starts vs entity labels", x: 220 },
  { id: "total", label: "gliner2-total-loss", sub: "the upstream composite (default)", x: 350 },
];

function ObjectiveTreeFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="three objectives, one default"
      caption="Schematic: the total-loss objective adds three loss nodes — structure (which folds in the entity/span scoring), classification, and count; the recipe path hard-codes it."
    >
      {OBJECTIVES.map((o) => (
        <g key={o.id}>
          <rect
            x={o.x - 55}
            y={40}
            width={110}
            height={40}
            rx={5}
            fill={
              o.id === "total"
                ? "color-mix(in oklch, var(--kfam-fusion) 18%, transparent)"
                : "color-mix(in oklch, var(--kfam-attention) 12%, transparent)"
            }
            stroke={o.id === "total" ? "var(--kfam-fusion)" : "var(--kfam-attention)"}
            strokeWidth={o.id === "total" ? 1.5 : 1}
          />
          <text
            x={o.x}
            y={57}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            {o.label}
          </text>
          <text
            x={o.x}
            y={71}
            textAnchor="middle"
            fontSize={6.5}
            className="fill-muted-foreground font-mono"
          >
            {o.sub}
          </text>
          <path d={`M ${o.x} 80 L 220 118`} stroke="var(--muted-foreground)" strokeWidth={0.9} />
        </g>
      ))}
      <rect
        x={160}
        y={122}
        width={120}
        height={26}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)"
        stroke="var(--kfam-mmsg)"
        strokeWidth={1.25}
      />
      <text
        x={220}
        y={139}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-foreground font-mono"
      >
        GlinerObjective
      </text>
      <text
        x={220}
        y={172}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        unweighted sum of three loss nodes
      </text>
    </Figure>
  );
}

const BCE_GRID = Array.from({ length: 24 }, (_, i) => {
  const col = i % 8;
  const row = Math.floor(i / 8);
  const gated = (i * 7) % 5 === 0;
  const weight = gated ? 0 : 0.4 + ((i * 3) % 4) * 0.2;
  return { id: `m-${col}-${row}`, x: 70 + col * 38, y: 44 + row * 30, gated, weight };
});

function MaskedBceFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="the mask is a per-position loss weight"
      caption="Schematic weights, not real data. An m = 0 position is skipped entirely — a validity gate, so no Inf·0 NaN can leak in. An m > 0 position carries a real weight, and the mean divides by the summed weighted mass only."
    >
      {BCE_GRID.map((c) => (
        <g key={c.id}>
          <rect
            x={c.x}
            y={c.y}
            width={32}
            height={24}
            rx={3}
            fill={
              c.gated
                ? "none"
                : `color-mix(in oklch, var(--kfam-attention) ${Math.round(c.weight * 55)}%, transparent)`
            }
            stroke={c.gated ? "var(--border)" : "var(--kfam-attention)"}
            strokeWidth={0.8}
          />
          {c.gated && (
            <path
              d={`M ${c.x + 4} ${c.y + 20} L ${c.x + 28} ${c.y + 4}`}
              stroke="var(--muted-foreground)"
              strokeWidth={1}
            />
          )}
        </g>
      ))}
      <text
        x={220}
        y={158}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        loss = Σ bce·label_weight·m ÷ (Σ m·label_weight + ε)
      </text>
      <text
        x={220}
        y={176}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        label_weight = label·pos_w + (1−label)·neg_w — per-label weights configurable
      </text>
    </Figure>
  );
}

const PACKED_BANDS = [
  { id: "dq", label: "dQ", h: 24 },
  { id: "dk", label: "dK", h: 24 },
  { id: "dv", label: "dV", h: 24 },
  { id: "dqr", label: "dQr", h: 16 },
  { id: "dkr", label: "dKr", h: 16 },
];

function PackedGradientFigure() {
  let y = 36;
  const bands = PACKED_BANDS.map((b) => {
    const out = { ...b, y };
    y += b.h;
    return out;
  });
  return (
    <Figure
      viewBox="0 0 440 210"
      title="one backward node, one packed gradient"
      caption="Schematic layout: the custom VJP emits a single fused backward node producing a packed [3·B·S + 2·rel, H] gradient; sliceRows splits it into d_qkv and d_qr/d_kr. The attention bias takes no gradient."
    >
      <rect
        x={70}
        y={32}
        width={110}
        height={112}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-fusion) 8%, transparent)"
        stroke="var(--kfam-fusion)"
        strokeWidth={1.5}
      />
      {bands.map((b) => (
        <g key={b.id}>
          <line
            x1={72}
            y1={b.y + b.h}
            x2={178}
            y2={b.y + b.h}
            stroke="var(--kfam-fusion)"
            strokeWidth={0.5}
            strokeDasharray="3 3"
          />
          <text
            x={125}
            y={b.y + b.h / 2 + 3}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            {b.label}
          </text>
          <path
            d={`M 180 ${b.y + b.h / 2} h 60`}
            stroke="var(--muted-foreground)"
            strokeWidth={0.8}
          />
        </g>
      ))}
      <rect
        x={244}
        y={44}
        width={130}
        height={48}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
        stroke="var(--kfam-attention)"
        strokeWidth={1}
      />
      <text x={309} y={71} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        d_qkv → input 0
      </text>
      <rect
        x={244}
        y={104}
        width={130}
        height={36}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)"
        stroke="var(--kfam-mmsg)"
        strokeWidth={1}
      />
      <text x={309} y={126} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        d_qr/d_kr → input 1
      </text>
      <text
        x={220}
        y={178}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        sliceRows does the split; attn_bias (input 2) is frozen — no VJP
      </text>
    </Figure>
  );
}

const BWD_KERNELS = ["bwd_scores", "bwd_dv", "bwd_dq_dk", "bwd_dqr_dkr"];

function BwdKernelsFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="four Metal kernels behind one graph node"
      caption="Schematic: on Metal the fused backward node dispatches four precise-math kernels — the training half of the kernel family the legend documents."
    >
      <rect
        x={120}
        y={28}
        width={200}
        height={32}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)"
        stroke="var(--kfam-fusion)"
        strokeWidth={1.5}
        strokeDasharray="6 2 2 2"
      />
      <text x={220} y={48} textAnchor="middle" fontSize={7} className="fill-foreground font-mono">
        fused_disentangled_attention_backward
      </text>
      {BWD_KERNELS.map((k, i) => (
        <g key={k}>
          <path d={`M 220 60 L ${75 + i * 98} 96`} stroke="var(--border)" strokeWidth={0.8} />
          <rect
            x={30 + i * 98}
            y={100}
            width={90}
            height={24}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 12%, var(--background))"
            stroke="var(--kfam-attention)"
            strokeWidth={1}
          />
          <text
            x={75 + i * 98}
            y={116}
            textAnchor="middle"
            fontSize={7}
            className="fill-foreground font-mono"
          >
            …{k}_f32
          </text>
        </g>
      ))}
      <rect
        x={155}
        y={148}
        width={130}
        height={20}
        rx={10}
        fill="color-mix(in oklch, var(--kfam-kv) 12%, transparent)"
        stroke="var(--kfam-kv)"
        strokeWidth={0.9}
      />
      <text
        x={220}
        y={162}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-foreground font-mono"
      >
        precise-math library
      </text>
    </Figure>
  );
}

function Rank1FusedFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 210"
      title={step === 0 ? "rank-1 LoRA backward, fused by default" : "184 tensors, 180 that matter"}
      caption={
        step === 0
          ? "Schematic: rank-1 adapters take two fused backward dispatches; higher ranks (and the kill-switch) fall back to the general three-kernel path. Defaults: rank 16, alpha 32."
          : "Schematic: the pinned PEFT config registers 184 adapter tensors; four count_embed out-projection pairs are inert upstream, so Zig trains the 180 effective ones and parity waives exactly those four."
      }
    >
      {step === 0 ? (
        <g>
          <rect
            x={60}
            y={56}
            width={130}
            height={70}
            rx={6}
            fill="color-mix(in oklch, var(--dtype-f16) 14%, transparent)"
            stroke="var(--dtype-f16)"
            strokeWidth={1.25}
          />
          <text
            x={125}
            y={94}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-muted-foreground font-mono"
          >
            frozen W
          </text>
          <rect
            x={200}
            y={56}
            width={16}
            height={70}
            rx={3}
            fill="color-mix(in oklch, var(--kfam-sampling) 22%, transparent)"
            stroke="var(--kfam-sampling)"
            strokeWidth={1}
          />
          <rect
            x={224}
            y={84}
            width={70}
            height={16}
            rx={3}
            fill="color-mix(in oklch, var(--kfam-sampling) 22%, transparent)"
            stroke="var(--kfam-sampling)"
            strokeWidth={1}
          />
          <path
            d="M 310 66 L 396 66 L 380 58 M 396 66 L 380 74"
            stroke="var(--kfam-fusion)"
            strokeWidth={2}
            fill="none"
          />
          <text
            x={352}
            y={54}
            textAnchor="middle"
            fontSize={7}
            className="fill-foreground font-mono"
          >
            rank 1: two fused dispatches
          </text>
          <path
            d="M 310 112 h 40 m 8 0 h 38"
            stroke="var(--muted-foreground)"
            strokeWidth={1.25}
            strokeDasharray="5 3"
          />
          <text
            x={352}
            y={128}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            general: three hops
          </text>
          <text
            x={220}
            y={172}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            grad_after_a · grad_a · grad_b come back as private device tensors
          </text>
        </g>
      ) : (
        <g>
          {Array.from({ length: 20 }, (_, i) => ({
            id: `t-${i}`,
            x: 48 + (i % 10) * 35,
            y: 60 + Math.floor(i / 10) * 34,
            inert: i >= 18,
          })).map((t) => (
            <rect
              key={t.id}
              x={t.x}
              y={t.y}
              width={28}
              height={26}
              rx={3}
              fill={
                t.inert ? "none" : "color-mix(in oklch, var(--kfam-attention) 18%, transparent)"
              }
              stroke={t.inert ? "var(--muted-foreground)" : "var(--kfam-attention)"}
              strokeWidth={0.9}
              strokeDasharray={t.inert ? "3 2" : undefined}
            />
          ))}
          <text
            x={220}
            y={140}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            (20 drawn for 184) — dashed = inert count_embed out_proj A/B pairs
          </text>
          <text
            x={220}
            y={170}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            optimizer parity waives only those, and only with proof of zero state
          </text>
        </g>
      )}
    </Figure>
  );
}

const LIFECYCLE = ["train", "validate", "evaluate", "materialize", "inspect"];

function ExecutorGateFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 210"
      title={
        step === 0 ? "the training-graph executor rail switch" : "the five-step recipe lifecycle"
      }
      caption={
        step === 0
          ? "Schematic: inside the compiled training session, an env flag throws the switch from interpreter dispatch to the training-graph executor, which gates at six host outputs per step. --compiled-required makes engine-preparation fallback fatal."
          : "Schematic: the fully-configured GLiNER2 lora-sft recipe expands into up to five tool steps (chips abbreviate the tool names); evaluate and materialize/inspect appear only when their inputs are configured."
      }
    >
      {step === 0 ? (
        <g>
          <path d="M 30 100 h 120" stroke="var(--muted-foreground)" strokeWidth={2} />
          <path d="M 150 100 L 260 60" stroke="var(--kfam-fusion)" strokeWidth={2} />
          <path d="M 150 100 L 260 140" stroke="var(--muted-foreground)" strokeWidth={2} />
          <circle cx={150} cy={100} r={5} fill="var(--kfam-sampling)" />
          <text
            x={150}
            y={86}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            env flag
          </text>
          <rect
            x={264}
            y={44}
            width={150}
            height={30}
            rx={5}
            fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)"
            stroke="var(--kfam-fusion)"
            strokeWidth={1.25}
          />
          <text
            x={339}
            y={63}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            training graph executor
          </text>
          <rect
            x={264}
            y={126}
            width={150}
            height={30}
            rx={5}
            fill="color-mix(in oklch, var(--kfam-attention) 12%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1}
          />
          <text
            x={339}
            y={145}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-foreground font-mono"
          >
            interpreter dispatch (default)
          </text>
          <rect
            x={286}
            y={78}
            width={106}
            height={16}
            rx={8}
            fill="none"
            stroke="var(--kfam-kv)"
            strokeWidth={0.9}
          />
          <text
            x={339}
            y={89}
            textAnchor="middle"
            fontSize={6.5}
            className="fill-muted-foreground font-mono"
          >
            gate: ≤6 host outputs
          </text>
          <text
            x={220}
            y={186}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            --compiled-required: a failed compiled path stops the run instead of falling back
          </text>
        </g>
      ) : (
        <g>
          {LIFECYCLE.map((s, i) => (
            <g key={s}>
              <rect
                x={20 + i * 84}
                y={80}
                width={76}
                height={30}
                rx={5}
                fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)"
                stroke="var(--kfam-matvec)"
                strokeWidth={1.1}
              />
              <text
                x={58 + i * 84}
                y={99}
                textAnchor="middle"
                fontSize={7.5}
                className="fill-foreground font-mono"
              >
                {s}
              </text>
              {i < LIFECYCLE.length - 1 && (
                <path
                  d={`M ${96 + i * 84} 95 h 8`}
                  stroke="var(--muted-foreground)"
                  strokeWidth={1.25}
                />
              )}
            </g>
          ))}
          <text
            x={220}
            y={136}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            train-gliner2-autodiff · validate-gliner2-autodiff-run · eval adapters
          </text>
          <text
            x={220}
            y={150}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            materialize-gliner2-lora · inspect-gliner2-checkpoint
          </text>
        </g>
      )}
    </Figure>
  );
}

function OperatorGatesFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="two lanes of quality gates"
      caption="Schematic: unit and contract tests run in CI; the heavyweight gates — Python-oracle parity, Metal and CUDA hardware matrices — are operator-run with retained artifacts. BF16 is parked behind an explicit FP32 contract."
    >
      <text x={110} y={34} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        CI lane
      </text>
      <rect
        x={40}
        y={44}
        width={140}
        height={26}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
        stroke="var(--kfam-attention)"
        strokeWidth={1.25}
      />
      <text x={110} y={61} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        unit + contract tests
      </text>
      <text
        x={330}
        y={34}
        textAnchor="middle"
        fontSize={8.5}
        className="fill-muted-foreground font-mono"
      >
        operator lane
      </text>
      {["Python-oracle parity", "Metal hardware gate", "CUDA matrix"].map((g, i) => (
        <g key={g}>
          <rect
            x={250}
            y={44 + i * 34}
            width={160}
            height={26}
            rx={5}
            fill="none"
            stroke="var(--muted-foreground)"
            strokeWidth={1}
            strokeDasharray="5 3"
          />
          <text
            x={330}
            y={61 + i * 34}
            textAnchor="middle"
            fontSize={7.5}
            className="fill-muted-foreground font-mono"
          >
            {g}
          </text>
        </g>
      ))}
      <line
        x1={40}
        y1={158}
        x2={410}
        y2={158}
        stroke="var(--border)"
        strokeWidth={0.9}
        strokeDasharray="6 3"
      />
      <rect
        x={165}
        y={166}
        width={110}
        height={20}
        rx={4}
        fill="none"
        stroke="var(--dtype-f16)"
        strokeWidth={0.9}
        opacity={0.7}
      />
      <text
        x={220}
        y={180}
        textAnchor="middle"
        fontSize={7.5}
        className="fill-muted-foreground font-mono"
      >
        BF16 — deferred
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

export function Gliner2TrainingClient({
  snippets,
  gitCommit,
  permalinkBase,
}: {
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}) {
  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="py-8">
        <header className="mx-auto max-w-7xl px-4">
          <p className="font-mono text-xs uppercase tracking-wider text-muted-foreground">
            <Link href="/training" className="hover:text-foreground">
              training
            </Link>{" "}
            / gliner2
          </p>
          <h1 className="mt-1 text-3xl font-bold tracking-tight">GLiNER2 finetuning</h1>
          <p className="mt-2 max-w-3xl text-muted-foreground">
            GLiNER2 trains the very graph it runs: the losses are written as graph operations,
            automatic differentiation derives every gradient (with hand-fused backward kernels where
            it counts), and correctness is proven by matching a pinned PyTorch reference step for
            step. The{" "}
            <Link className="text-primary underline" href="/models/gliner2">
              GLiNER2 model page
            </Link>{" "}
            covers the forward math; this page covers how the same graph learns.
          </p>
        </header>

        {/* ── 1 · Graph-first ──────────────────────────────────────── */}
        <ScrollyChapter
          id="graph-first"
          number={1}
          title="Graph-first: the same graph learns"
          intro="GLiNER2 trains by reusing its inference forward graph and differentiating it — no separate training network."
        >
          <Scene id="plug" graphic={<GraphFirstFigure />}>
            <p>
              The training context plugs the DeBERTa forward graph into the generic{" "}
              <code>RealAutodiffTrainer</code> and appends the GLiNER2 heads. Two things can't be
              expressed as graph ops and arrive as bound placeholders instead: the index derivations
              (which sub-token starts a word, where the entity markers sit, span clamping) and the
              attention bias — a parameter placeholder repopulated every step with −1e9 at padded
              positions, frozen so it never takes a gradient.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-build-loss")} /> ·{" "}
              <CodeLink link={L("train-real-autodiff-trainer")} /> ·{" "}
              <CodeLink link={L("train-graph-backward")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 2 · Objectives ───────────────────────────────────────── */}
        <ScrollyChapter
          id="objectives"
          number={2}
          title="Three objectives, one composite"
          intro="A token objective, a span-start objective, and the composite that trains the whole head."
        >
          <Scene id="enum" graphic={<ObjectiveTreeFigure />}>
            <p>
              <code>GlinerObjective</code> names them: <code>token</code> — legacy token
              classification as masked cross-entropy, where HF's ignore_index becomes an all-zero
              target row that contributes to neither numerator nor denominator;{" "}
              <code>span_start</code> — span-start states scored against entity labels; and the
              default <code>gliner2-total-loss</code> — the upstream composite: three loss nodes
              summed unweighted — structure (with the entity/span scoring folded in),
              classification, and count.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-objectives")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 3 · Masked BCE ───────────────────────────────────────── */}
        <ScrollyChapter
          id="masked-bce"
          number={3}
          title="The mask is a loss weight"
          intro="One fused loss kernel encodes validity and class weighting in a single mask tensor."
        >
          <Scene id="semantics" graphic={<MaskedBceFigure />}>
            <p>
              <code>maskedBceWithLogitsLoss</code> treats the mask as a per-position loss{" "}
              <em>weight</em>, not a multiplier on the logit: an m = 0 position is skipped entirely
              — a validity gate, so an infinite logit at a masked slot can never poison the scalar
              with Inf·0 — while m &gt; 0 scales the term (hard-negative weighting rides here). The
              label weight blends configurable positive and negative class weights, and mean
              reduction divides by the summed weighted mass only. For 0/1 masks the semantics are
              identical to the older formulation.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-masked-bce")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 4 · VJPs ─────────────────────────────────────────────── */}
        <ScrollyChapter
          id="vjps"
          number={4}
          title="Custom VJPs and the packed gradient"
          intro="One fused attention forward gets exactly one fused backward node — whose output is a packed matrix that gets sliced apart."
        >
          <Scene id="packed" graphic={<PackedGradientFigure />}>
            <p>
              Autodiff registers a custom VJP for the fused disentangled attention: instead of
              decomposing into a dozen primitive gradients, it emits a single backward node whose
              output packs every gradient into one <code>[3·B·S + 2·rel, H]</code> matrix. Two{" "}
              <code>sliceRows</code> cuts then route the pieces — d_qkv back to the packed
              projections, d_qr/d_kr back to the relative-position projections. The padding bias is
              input 2 and simply has no VJP.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-attn-vjp")} />
            </p>
          </Scene>
          <Scene id="kernels" graphic={<BwdKernelsFigure />}>
            <p>
              On Metal that one node fans out to four GPU programs: one each for the scores, dV,
              dQ/dK and dQr/dKr. All four are compiled from the precise-math library, so training
              gradients do not inherit the fast-math shortcuts that are fine for inference but can
              bias a derivative. A documented fallback covers the case where the safe-math compile
              itself fails. These are the <code>training</code>-family entries listed in the{" "}
              <Link
                className="text-primary underline"
                href="/systems/kernels?family=training&q=bwd"
              >
                kernel inventory
              </Link>
              .
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-bwd-kernel")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 5 · LoRA ─────────────────────────────────────────────── */}
        <ScrollyChapter
          id="lora"
          number={5}
          title="LoRA on device"
          intro="Rank-1 fused backward by default, and a tensor count with an asterisk."
        >
          <Scene id="rank1" graphic={<Rank1FusedFigure step={0} />}>
            <p>
              When the adapter has rank 1, the Metal backward pass takes a shortcut that is on by
              default: two fused GPU calls instead of the general three. An environment variable
              turns it off if it ever needs to come off. Defaults: rank 16, alpha 32, dropout 0,
              targets spanning the encoder plus span_rep, classifier, count_embed and count_pred.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-rank1-gate")} />{" "}
              <EnvFlagChip
                name="TERMITE_METAL_DISABLE_LORA_BACKWARD_RANK1_FUSED"
                defaultOn={false}
              />
            </p>
          </Scene>
          <Scene id="tensors" graphic={<Rank1FusedFigure step={1} />}>
            <p>
              One asterisk on the tensor count. The pinned adapter configuration registers 184
              tensors, but four of them do nothing in PyTorch: the count_embed attention out-
              projection A/B pairs, which PyTorch bypasses by reading the projection weight
              directly. Zig trains the 180 effective tensors, and optimizer parity waives exactly
              those four, only when the Python dump proves they carry zero steps, gradients, Adam
              state, and an identically-zero lora_B.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-tensors-doc")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 6 · Executors + lifecycle ────────────────────────────── */}
        <ScrollyChapter
          id="executors"
          number={6}
          title="Executors, flags, and the recipe lifecycle"
          intro="An opt-in compiled Metal training executor, a flag CLI, and a five-step recipe."
        >
          <Scene id="gate" graphic={<ExecutorGateFigure step={0} />}>
            <p>
              On a GPU backend, training walks the compiled graph operation by operation. An
              environment flag switches it instead to a single whole-graph executor, which also
              unlocks two faster matrix routines. The same flag pair covers CUDA. The strict Metal
              path budgets at most six host metadata outputs per step (a qualification-contract
              figure enforced by the operator-run perf gate, not a runtime default), and{" "}
              <code>--compiled-required</code> makes an engine-preparation fallback fatal and blocks
              a resumed run from silently crossing compiled↔interpreter. Full-task evaluation stays
              native-only regardless. GLiNER2 is the one training stack here with all three
              backends: native, Metal, <em>and</em> CUDA.
            </p>
            <p>
              <CodeLink link={L("train-executor-flag")} /> ·{" "}
              <CodeLink link={L("gliner2-ft-compiled-required")} />
            </p>
            <p>
              <EnvFlagChip name="TERMITE_ENABLE_TRAINING_GRAPH_EXECUTOR" defaultOn={false} />{" "}
              <EnvFlagChip name="TERMITE_DISABLE_TRAINING_GRAPH_EXECUTOR" defaultOn={false} />
            </p>
          </Scene>
          <Scene id="cli" graphic={<ExecutorGateFigure step={1} />}>
            <p>
              Two front doors: the flag CLI (<code>train run gliner2-autodiff</code>, roughly 55
              flags — epochs 10, batch 2, sequence 256, LR 5e-4 with linear schedule and 10% warmup,
              seed 42), and the recipe front-door, whose GLiNER2 <code>lora-sft</code> recipe
              expands into up to five steps: train, validate the run, evaluate the adapter (when an
              eval dataset is configured), then materialize the merged model and inspect the
              checkpoint (when a materialized directory is set).
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-cli-main")} /> ·{" "}
              <CodeLink link={L("gliner2-ft-cli-registry")} /> ·{" "}
              <CodeLink link={L("gliner2-ft-recipe-steps")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 7 · Gaps ─────────────────────────────────────────────── */}
        <ScrollyChapter
          id="gaps"
          number={7}
          title="Honest gaps"
          intro="What the receipts don't certify."
        >
          <Scene id="gates" graphic={<OperatorGatesFigure />}>
            <p>
              The heavyweight quality gates — real-model Python-oracle parity, the Metal and CUDA
              hardware matrices — are operator-run rather than CI-provisioned; ordinary unit and
              contract tests validate the code surfaces, not model quality. The Metal training-graph
              executor reproduces native step-for-step but with narrower automated coverage. The
              Unicode normalizer implements a conservative subset and errors rather than drifting.
              And BF16 is explicitly deferred: the production contract is FP32 for graph tensors,
              trainables and optimizer state. GLiNER2.5 answers all of this with a different
              philosophy —{" "}
              <Link className="text-primary underline" href="/training/gliner25">
                the contract-first page
              </Link>{" "}
              ends with the side-by-side.
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-gates-doc")} /> ·{" "}
              <CodeLink link={L("gliner2-ft-build-loss")} />
            </p>
          </Scene>
        </ScrollyChapter>
      </div>
    </SnippetProvider>
  );
}
