"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { Figure } from "@/components/viz/glyphs";
import { L } from "@/lib/links";

/* ------------------------------------------------------------------ */
/* Figures                                                             */
/* ------------------------------------------------------------------ */

const DOORS = [
  {
    id: "recipe",
    label: "recipe.json",
    sub: "gemma4 · qwen · gliner2 lifecycle",
    color: "var(--kfam-matvec)",
    x: 20,
  },
  {
    id: "flags",
    label: "flag CLI (~55 flags)",
    sub: "gliner2-autodiff",
    color: "var(--kfam-attention)",
    x: 160,
  },
  {
    id: "job",
    label: "job.json (supervised)",
    sub: "gliner25 one-shot worker",
    color: "var(--kfam-fusion)",
    x: 300,
  },
];

function TrainingMapFigure() {
  return (
    <Figure
      viewBox="0 0 440 220"
      title="three doors, one differentiation engine"
      caption="Schematic map, not a call graph. Each model family enters training through its own front door; every door ends at the same graph autodiff library and an AdamW step."
    >
      {DOORS.map((d) => (
        <g key={d.id}>
          <rect
            x={d.x}
            y={26}
            width={120}
            height={40}
            rx={5}
            fill={`color-mix(in oklch, ${d.color} 16%, transparent)`}
            stroke={d.color}
            strokeWidth={1.25}
          />
          <text
            x={d.x + 60}
            y={43}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            {d.label}
          </text>
          <text
            x={d.x + 60}
            y={57}
            textAnchor="middle"
            fontSize={7}
            className="fill-muted-foreground font-mono"
          >
            {d.sub}
          </text>
          <path d={`M ${d.x + 60} 66 L 220 108`} stroke="var(--muted-foreground)" strokeWidth={1} />
        </g>
      ))}
      <rect
        x={140}
        y={112}
        width={160}
        height={30}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)"
        stroke="var(--kfam-mmsg)"
        strokeWidth={1.5}
      />
      <text x={220} y={131} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        ml.graph autodiff
      </text>
      <path d="M 220 142 v 18" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect
        x={175}
        y={162}
        width={90}
        height={26}
        rx={5}
        fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)"
        stroke="var(--kfam-sampling)"
        strokeWidth={1.25}
      />
      <text x={220} y={179} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        AdamW
      </text>
    </Figure>
  );
}

const FWD_NODES = [
  { id: "w", label: "weights", x: 30 },
  { id: "attn", label: "attention", x: 135 },
  { id: "head", label: "head", x: 240 },
  { id: "loss", label: "loss", x: 345 },
];

function ForwardBackwardFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="the same graph, differentiated"
      caption="Schematic: training reuses the inference forward graph, adds loss nodes, and derives a backward pass. Where a fused forward op exists, a matching custom VJP emits one fused backward node."
    >
      {FWD_NODES.map((n, i) => (
        <g key={n.id}>
          <rect
            x={n.x}
            y={44}
            width={70}
            height={26}
            rx={4}
            fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
            stroke="var(--kfam-attention)"
            strokeWidth={1}
          />
          <text
            x={n.x + 35}
            y={61}
            textAnchor="middle"
            fontSize={8.5}
            className="fill-foreground font-mono"
          >
            {n.label}
          </text>
          {i < FWD_NODES.length - 1 && (
            <path d={`M ${n.x + 70} 57 h 35`} stroke="var(--muted-foreground)" strokeWidth={1.25} />
          )}
        </g>
      ))}
      <text
        x={425}
        y={61}
        textAnchor="end"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        →
      </text>
      {FWD_NODES.map((n, i) => (
        <g key={`b-${n.id}`}>
          <rect
            x={n.x}
            y={112}
            width={70}
            height={26}
            rx={4}
            fill="none"
            stroke={i === 1 ? "var(--kfam-fusion)" : "var(--muted-foreground)"}
            strokeWidth={i === 1 ? 1.5 : 0.9}
            strokeDasharray="5 3"
          />
          <text
            x={n.x + 35}
            y={129}
            textAnchor="middle"
            fontSize={8.5}
            className={i === 1 ? "fill-foreground font-mono" : "fill-muted-foreground font-mono"}
          >
            {i === 1 ? "custom VJP" : `d(${n.label})`}
          </text>
          {i < FWD_NODES.length - 1 && (
            <path
              d={`M ${n.x + 105} 125 h -35`}
              stroke="var(--muted-foreground)"
              strokeWidth={0.9}
              strokeDasharray="5 3"
            />
          )}
        </g>
      ))}
      <path
        d="M 380 70 v 42"
        stroke="var(--muted-foreground)"
        strokeWidth={0.9}
        strokeDasharray="5 3"
      />
      <text
        x={220}
        y={172}
        textAnchor="middle"
        fontSize={8}
        className="fill-muted-foreground font-mono"
      >
        forward (solid) → adjoints flow back (dashed)
      </text>
    </Figure>
  );
}

function SharedSpineFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 230"
      title={step === 0 ? "one gradient engine, two trainer shells" : "AdamW, everywhere"}
      caption={
        step === 0
          ? "Schematic: gradient() differentiates the graph; decoders and GLiNER2 wrap it in RealAutodiffTrainer, GLiNER2.5 wraps it in the seeded gradient trainer with staged publication."
          : "Schematic: the same AdamW configuration (0.9 / 0.999 / 1e-8, weight decay 0.01) closes every loop; clipping defaults to a global norm of 1.0."
      }
    >
      <rect
        x={160}
        y={20}
        width={120}
        height={24}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)"
        stroke="var(--kfam-mmsg)"
        strokeWidth={1.25}
      />
      <text x={220} y={36} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        graph + loss nodes
      </text>
      <path d="M 220 44 v 16" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect
        x={170}
        y={62}
        width={100}
        height={22}
        rx={4}
        fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)"
        stroke="var(--kfam-attention)"
        strokeWidth={1.25}
      />
      <text x={220} y={77} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        gradient()
      </text>
      <path
        d="M 220 84 L 120 106 M 220 84 L 320 106"
        stroke="var(--muted-foreground)"
        strokeWidth={1}
      />
      <g opacity={step === 0 ? 1 : 0.7} className="transition-opacity duration-300">
        <rect
          x={30}
          y={110}
          width={180}
          height={40}
          rx={5}
          fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)"
          stroke="var(--kfam-matvec)"
          strokeWidth={1.25}
        />
        <text
          x={120}
          y={126}
          textAnchor="middle"
          fontSize={8.5}
          className="fill-foreground font-mono"
        >
          RealAutodiffTrainer
        </text>
        <text
          x={120}
          y={140}
          textAnchor="middle"
          fontSize={7}
          className="fill-foreground font-mono"
        >
          decoders · gliner2 — accum, clip @ 1.0, LR schedule
        </text>
        <rect
          x={230}
          y={110}
          width={180}
          height={40}
          rx={5}
          fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)"
          stroke="var(--kfam-fusion)"
          strokeWidth={1.25}
        />
        <text
          x={320}
          y={126}
          textAnchor="middle"
          fontSize={8.5}
          className="fill-foreground font-mono"
        >
          seeded gradient trainer
        </text>
        <text
          x={320}
          y={140}
          textAnchor="middle"
          fontSize={7}
          className="fill-foreground font-mono"
        >
          GLiNER2.5 — staged accumulate → validate → publish
        </text>
      </g>
      <path
        d="M 120 150 L 200 176 M 320 150 L 240 176"
        stroke="var(--muted-foreground)"
        strokeWidth={1}
      />
      <g opacity={step === 1 ? 1 : 0.7} className="transition-opacity duration-300">
        <rect
          x={165}
          y={180}
          width={110}
          height={26}
          rx={5}
          fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)"
          stroke="var(--kfam-sampling)"
          strokeWidth={step === 1 ? 1.75 : 1.25}
        />
        <text
          x={220}
          y={197}
          textAnchor="middle"
          fontSize={9}
          className="fill-foreground font-mono"
        >
          AdamW step
        </text>
      </g>
    </Figure>
  );
}

const CLI_DOORS = [
  {
    id: "recipe",
    cmd: "antfly inference finetune run <recipe.json>",
    sub: "recipe runner: gemma4 DPO/GRPO, gliner2 lifecycle",
    color: "var(--kfam-matvec)",
  },
  {
    id: "flags",
    cmd: "… finetune train run gliner2-autodiff [~55 flags]",
    sub: "flag CLI, in-process",
    color: "var(--kfam-attention)",
  },
  {
    id: "job",
    cmd: "… finetune train run gliner25 <job.json>",
    sub: "supervised one-shot worker (parent ⇄ worker)",
    color: "var(--kfam-fusion)",
  },
];

function CliDoorsFigure() {
  return (
    <div className="flex h-full flex-col justify-center gap-3">
      {CLI_DOORS.map((d) => (
        <div key={d.id} className="rounded-lg border p-3" style={{ borderColor: d.color }}>
          <div className="font-mono text-[11px]">{d.cmd}</div>
          <div className="mt-1 font-mono text-[10px] text-muted-foreground">{d.sub}</div>
        </div>
      ))}
      <p className="text-center font-mono text-[11px] text-muted-foreground">
        the exact commands, schematically grouped · no HTTP training API exists
      </p>
    </div>
  );
}

const RECEIPTS = [
  { id: "dpo", label: "dpo_report.json", sub: "loss · margin · accuracy · β" },
  { id: "run", label: "run.json", sub: "config · digests · executable hash" },
  { id: "adapter", label: "adapter dir + receipt", sub: "PEFT tensors · provenance" },
];

function ReceiptsFigure({ step }: { step: 0 | 1 }) {
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      {step === 0 ? (
        <>
          <div className="mx-auto rounded-lg border px-6 py-3 text-center font-mono text-xs">
            training run <span className="text-muted-foreground">(seed 42)</span>
          </div>
          <div className="grid grid-cols-3 gap-2">
            {RECEIPTS.map((r) => (
              <div key={r.id} className="rounded-md border border-dashed p-2 text-center">
                <div className="font-mono text-[10px]">{r.label}</div>
                <div className="mt-1 font-mono text-[9px] text-muted-foreground">{r.sub}</div>
              </div>
            ))}
          </div>
          <p className="text-center font-mono text-[11px]" style={{ color: "var(--kfam-text-kv)" }}>
            receipts record integrity and provenance — never model quality
          </p>
        </>
      ) : (
        <div className="rounded-lg border border-dashed p-4">
          <div className="mb-2 font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
            outside the fence
          </div>
          <ul className="space-y-1.5 font-mono text-[11px] text-muted-foreground">
            <li>· no PPO, no RLHF, no reward models, no value heads, no GAE</li>
            <li>· decoder training is CPU-only by construction</li>
            <li>· GRPO rewards are string-match rules, not learned scorers</li>
            <li>· Python-oracle / Metal / CUDA quality gates run operator-side, not in CI</li>
            <li>· several finetune modules exist but are unreferenced — not documented here</li>
          </ul>
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

export function TrainingClient({
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
          <h1 className="text-3xl font-bold tracking-tight">Training</h1>
          <p className="mt-2 max-w-3xl text-muted-foreground">
            The same graphs the inference pages walk through are differentiable. This section
            follows how models learn in the Zig runtime: preference optimization for Gemma4, and two
            very different finetuning stacks for GLiNER2 and GLiNER2.5. Everything here anchors to
            real training code; nothing is a benchmark or a quality claim.
          </p>
          <div className="mt-4 flex flex-wrap gap-2 font-mono text-xs">
            <Link
              href="/training/gemma4"
              className="rounded-full border px-3 py-1 transition-colors hover:border-primary/60"
            >
              Gemma4 · DPO &amp; GRPO →
            </Link>
            <Link
              href="/training/gliner2"
              className="rounded-full border px-3 py-1 transition-colors hover:border-primary/60"
            >
              GLiNER2 finetuning →
            </Link>
            <Link
              href="/training/gliner25"
              className="rounded-full border px-3 py-1 transition-colors hover:border-primary/60"
            >
              GLiNER2.5 finetuning →
            </Link>
          </div>
        </header>

        {/* ── 1 · Philosophies ─────────────────────────────────────── */}
        <ScrollyChapter
          id="philosophies"
          number={1}
          title="One autodiff library, two philosophies"
          intro="Every trainable model here backpropagates through the same graph library — but three very different front doors sit on top of it."
        >
          <Scene id="map" graphic={<TrainingMapFigure />}>
            <p>
              Gemma4 (and the Qwen decoders) train through a <em>recipe</em>: a JSON file the recipe
              runner turns into a CPU autodiff training loop. GLiNER2 trains <em>graph-first</em>: a
              flag-driven CLI builds the same DeBERTa forward graph inference uses, grafts loss
              nodes on, and lets autodiff do the rest. GLiNER2.5 trains <em>contract-first</em>: a
              pinned JSON job under a disposable supervised worker, where every random draw and
              detach boundary is named and hashed. All three converge on the same differentiation
              engine.
            </p>
            <p>
              <CodeLink link={L("train-graph-backward")} />
            </p>
          </Scene>
          <Scene id="not-inference" graphic={<ForwardBackwardFigure />}>
            <p>
              <strong>What changes versus the inference pages:</strong> nothing about the forward
              math — the encoder and head graphs are the ones the model pages describe. Training
              adds loss nodes and derives a backward pass. Where inference fused an operation into
              one kernel, training registers a <em>custom VJP</em> so the fused forward gets a
              matching fused backward instead of decomposing into a dozen primitive gradients.
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 2 · The shared spine ─────────────────────────────────── */}
        <ScrollyChapter
          id="spine"
          number={2}
          title="The shared gradient engine"
          intro="Underneath every page in this section: one gradient engine, two trainer shells, one optimizer."
        >
          <Scene id="gradient" graphic={<SharedSpineFigure step={0} />}>
            <p>
              <code>gradient()</code> walks the graph and emits adjoints. Two shells wrap it:
              decoders and GLiNER2 use <code>RealAutodiffTrainer</code> — gradient accumulation,
              global-norm clipping at the default <code>max_grad_norm = 1.0</code>, LR schedules,
              LoRA parameter slices — while GLiNER2.5 uses the seeded gradient trainer, which stages
              every accumulation and update, validates it, and only then publishes.
            </p>
            <p>
              <CodeLink link={L("train-real-autodiff-trainer")} /> ·{" "}
              <CodeLink link={L("train-max-grad-norm")} /> ·{" "}
              <CodeLink link={L("gliner25-adamw-renorm")} />
            </p>
          </Scene>
          <Scene id="adamw" graphic={<SharedSpineFigure step={1} />}>
            <p>
              The optimizer is AdamW everywhere — β₁ 0.9, β₂ 0.999, ε 1e-8, weight decay 0.01 —
              configured once in the graph library. What differs per family is the discipline around
              the step: fixed-divisor accumulation for GLiNER2 and the decoders, and partial-window
              renormalization for GLiNER2.5 (its page explains why that matters).
            </p>
            <p>
              <CodeLink link={L("train-adamw-config")} /> ·{" "}
              <CodeLink link={L("train-adamw-step")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 3 · Three front doors ────────────────────────────────── */}
        <ScrollyChapter
          id="cli"
          number={3}
          title="Three front doors"
          intro="The CLI surface reveals each family's philosophy before a single line of trainer code is read."
        >
          <Scene id="doors" graphic={<CliDoorsFigure />}>
            <p>
              The recipe runner (<code>finetune run &lt;recipe.json&gt;</code>) is the only door
              into Gemma4 preference training — there is no <code>dpo</code> subcommand. GLiNER2's
              flag CLI runs in-process with roughly 55 knobs. GLiNER2.5's door takes a job file and
              exactly two extra flags, then supervises a disposable worker. Training has no HTTP or
              OpenAPI surface at all — it is CLI-only by design.
            </p>
            <p>
              <CodeLink link={L("train-recipe-cli")} /> ·{" "}
              <CodeLink link={L("gliner2-ft-cli-registry")} /> ·{" "}
              <CodeLink link={L("gliner25-train-entry")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 4 · Determinism, receipts, scope ─────────────────────── */}
        <ScrollyChapter
          id="scope"
          number={4}
          title="Determinism, receipts — and what training is not"
          intro="This runtime trains with fixed seeds and writes receipts; it does not do RLHF."
        >
          <Scene id="determinism" graphic={<ReceiptsFigure step={0} />}>
            <p>
              Every trainer seeds at 42 by default and reports what it did: the Gemma4 paths write
              versioned <code>dpo_report.json</code> / <code>grpo_report.json</code> files;
              GLiNER2.5 writes an immutable <code>run.json</code> that pins the config, the source
              digests, and even the hash of the executable that ran, plus export receipts. The
              receipts are integrity and provenance documents — every one of them explicitly
              declines to certify model quality.
            </p>
            <p>
              <CodeLink link={L("gemma4-train-dpo-report")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-export")} />
            </p>
          </Scene>
          <Scene id="not-here" graphic={<ReceiptsFigure step={1} />}>
            <p>
              <strong>And the honest fence:</strong> there is no PPO, no RLHF loop, no learned
              reward model, no value head or GAE anywhere in the tree. Decoder training runs on the
              CPU by construction. The Metal training graph executor for GLiNER2 is opt-in behind an
              environment flag, and the heavyweight quality gates (Python-oracle parity, Metal/CUDA
              hardware matrices) run operator-side rather than in CI. The three model pages carry
              their own boundary chapters with specifics.
            </p>
            <p>
              <CodeLink link={L("train-executor-flag")} />
            </p>
          </Scene>
        </ScrollyChapter>
      </div>
    </SnippetProvider>
  );
}
