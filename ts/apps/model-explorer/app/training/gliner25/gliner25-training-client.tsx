"use client";

import Link from "next/link";
import { useId, useState } from "react";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { Figure } from "@/components/viz/glyphs";
import { L } from "@/lib/links";

/* ------------------------------------------------------------------ */
/* Figures                                                             */
/* ------------------------------------------------------------------ */

const FP_CELLS = Array.from({ length: 16 }, (_, i) => ({ id: `fp-${i}`, x: 176 + i * 6, on: (i * 5) % 3 !== 0 }));

function OneShotWorkerFigure() {
  return (
    <Figure
      viewBox="0 0 440 220"
      title="one disposable worker, one signed contract"
      caption="Schematic: parent and worker each read the job file independently and must agree on its fingerprint; the worker runs exactly once under a watchdog. Exit codes are part of the contract: 124 timeout, 86 hard cancellation."
    >
      <rect x={40} y={50} width={110} height={70} rx={6} fill="color-mix(in oklch, var(--kfam-matvec) 14%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1.25} />
      <text x={95} y={80} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        parent
      </text>
      <text x={95} y={95} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        sole waiter + signaller
      </text>
      <circle cx={128} cy={62} r={9} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} />
      <path d="M 128 56 v 6 l 4 3" stroke="var(--kfam-kv)" strokeWidth={1} fill="none" />
      <rect x={290} y={50} width={110} height={70} rx={6} fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} />
      <text x={345} y={80} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        worker
      </text>
      <text x={345} y={95} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        never restarts
      </text>
      {FP_CELLS.map((c) => (
        <rect key={c.id} x={c.x} y={80} width={4.5} height={9} rx={1} fill={c.on ? "var(--kfam-sampling)" : "color-mix(in oklch, var(--kfam-sampling) 25%, transparent)"} />
      ))}
      <text x={220} y={44} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        32-byte fingerprint, re-derived by the worker
      </text>
      <text x={220} y={106} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        NDJSON progress events →
      </text>
      <rect x={110} y={156} width={70} height={20} rx={4} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} />
      <text x={145} y={170} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        exit 124
      </text>
      <rect x={260} y={156} width={70} height={20} rx={4} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} />
      <text x={295} y={170} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        exit 86
      </text>
      <text x={220} y={198} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        timeout · hard-cancel (2nd signal or expired grace) — ordinary errors exit 1
      </text>
    </Figure>
  );
}

function Mk({ children }: { children: string }) {
  return (
    <span className="rounded px-0.5 font-semibold" style={{ background: "color-mix(in oklch, var(--kfam-fusion) 20%, transparent)", color: "var(--kfam-text-fusion)" }}>
      {children}
    </span>
  );
}

const TARGET_WORDS = [
  { w: "Acme", s: 1, e: 1 },
  { w: "owes", s: 0, e: 0 },
  { w: "1,200", s: 0, e: 0 },
  { w: "EUR.", s: 0, e: 0 },
];

const POOL_CHIPS = [
  { id: "acme", label: '"Acme"', gold: false },
  { id: "owes", label: '"owes"', gold: false },
  { id: "total", label: '"1,200 EUR"', gold: true },
  { id: "eur", label: '"EUR."', gold: false },
];

const LIFECYCLE_STATIONS = [
  {
    id: "row",
    title: "train.jsonl row",
    color: "var(--kfam-matvec)",
    detail:
      "A row arrives carrying the text, the full schema, and exact character offsets for every entity, record, relation and classification annotation. Dataset format v1 — explicit offsets only.",
    example: (
      <div className="space-y-0.5 overflow-hidden whitespace-nowrap">
        <div>{'{"text": "Acme owes 1,200 EUR.", "schema": {…},'}</div>
        <div>{' "entities": [{"type":"company","span":{"start":0,"end":4}}],'}</div>
        <div>{' "records": [{"type":"invoice","fields":[{"name":"total",…}]}]}'}</div>
      </div>
    ),
  },
  {
    id: "validate",
    title: "strict validation",
    color: "var(--kfam-matvec)",
    detail:
      "Checked before anything runs: offsets must be exact (nothing is snapped to the nearest word or inferred), sizes must fit the declared limits, and the schema must be complete. Bad rows fail loudly.",
    example: (
      <div className="space-y-0.5">
        <div style={{ color: "var(--kfam-text-attention)" }}>✓ start 0, end 4 → "Acme" — exact</div>
        <div className="text-muted-foreground">
          <span style={{ color: "var(--kfam-text-kv)" }}>✗</span> start 0, end 3 → "Acm" —{" "}
          <span className="line-through">snap to word</span> rejected
        </div>
      </div>
    ),
  },
  {
    id: "prompt",
    title: "prompt + tokenize",
    color: "var(--kfam-attention)",
    detail:
      "The schema is serialized ahead of the text with marker tokens, and the whole sequence is tokenized. A word→byte map is kept so results can always point back at the source text.",
    example: (
      <div className="leading-relaxed">
        ( <Mk>[P]</Mk> entities ( <Mk>[E]</Mk> company ) ) <Mk>[SEP_STRUCT]</Mk> ( <Mk>[P]</Mk>{" "}
        invoice ( <Mk>[C]</Mk> total ) ) <Mk>[SEP_TEXT]</Mk>{" "}
        <span style={{ color: "var(--kfam-text-attention)" }}>Acme owes 1,200 EUR.</span>
      </div>
    ),
  },
  {
    id: "targets",
    title: "targets compiled",
    color: "var(--kfam-attention)",
    detail:
      "The row's offsets compile into dense, packed training targets. Every schema in the batch must carry a matching fingerprint — mixed schemas are an error, never a merge.",
    example: (
      <div className="space-y-1">
        <div className="flex gap-1">
          {TARGET_WORDS.map((t) => (
            <span key={t.w} className="rounded border px-1.5 py-0.5">
              {t.w}
            </span>
          ))}
        </div>
        <div className="text-muted-foreground">
          company · start [{TARGET_WORDS.map((t) => t.s).join(" ")}] · end [
          {TARGET_WORDS.map((t) => t.e).join(" ")}] — "Acme" starts and ends at word 0
        </div>
      </div>
    ),
  },
  {
    id: "forward",
    title: "forward pass ⚄",
    color: "var(--kfam-fusion)",
    detail:
      "Encoder → boundary head → shared candidate pool. Gold spans are injected into the pool on the hold-then-decay schedule — the first of the two seeded draws (chapter 2).",
    example: (
      <div className="space-y-1">
        <div className="text-muted-foreground">candidate pool:</div>
        <div className="flex flex-wrap gap-1">
          {POOL_CHIPS.map((c) => (
            <span
              key={c.id}
              className="rounded border px-1.5 py-0.5"
              style={c.gold ? { borderColor: "var(--kfam-fusion)", background: "color-mix(in oklch, var(--kfam-fusion) 14%, transparent)" } : undefined}
            >
              {c.label}
              {c.gold && " ⚄ gold, injected"}
            </span>
          ))}
        </div>
      </div>
    ),
  },
  {
    id: "loss",
    title: "losses + matching ⚄ #",
    color: "var(--kfam-fusion)",
    detail:
      "Twelve host loss terms grade the outputs, and the Hungarian matcher pairs record instances with gold (chapters 3–4). Which all-negative queries join in is the second seeded draw; every mask is hashed into the decision fingerprint.",
    example: (
      <div className="space-y-0.5">
        <div>
          "Acme" → σ 0.91 <span style={{ color: "var(--kfam-text-attention)" }}>✓ gold</span>
        </div>
        <div className="text-muted-foreground">"EUR." → σ 0.34 · kept as hard negative</div>
        <div className="text-muted-foreground">instance₁ ⇄ invoice₁ — Hungarian pairing → #</div>
      </div>
    ),
  },
  {
    id: "update",
    title: "backward + update",
    color: "var(--kfam-sampling)",
    detail:
      "Gradients flow back and accumulate. A partial final window is renormalized to the actual batch count, the global norm is clipped at 1.0, and AdamW publishes a staged, verified update (chapter 7).",
    example: (
      <div className="flex flex-wrap items-center gap-1.5">
        <span className="rounded border px-1.5 py-0.5">Σg / 4 batches</span>
        <span className="text-muted-foreground">→</span>
        <span className="rounded border px-1.5 py-0.5">× 6/4 renorm</span>
        <span className="text-muted-foreground">→</span>
        <span className="rounded border px-1.5 py-0.5">‖g‖ 1.7 → clip ×0.59</span>
        <span className="text-muted-foreground">→</span>
        <span className="rounded border px-1.5 py-0.5" style={{ borderColor: "var(--kfam-sampling)" }}>
          AdamW → W′
        </span>
      </div>
    ),
  },
  {
    id: "receipts",
    title: "receipts #",
    color: "var(--kfam-kv)",
    detail:
      "One progress line per microbatch, a checkpoint every 100, and a run identity hash covering the config, inputs, draws and decisions — so the run can be proven, not just trusted.",
    example: (
      <div className="space-y-0.5">
        <div>progress.jsonl · {'{"event":"step","loss":0.42,…}'}</div>
        <div className="text-muted-foreground">latest.safetensors · checkpoint</div>
        <div className="text-muted-foreground">run.json · identity #9f3a… · evaluation_performed: false</div>
      </div>
    ),
  },
];

function RecordLifecycleFigure() {
  const [active, setActive] = useState(0);
  const panelId = useId();
  const station = LIFECYCLE_STATIONS[active];
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      <div className="text-center font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
        one record, start to finish
      </div>
      <div className="flex items-center gap-2">
        <button
          type="button"
          aria-label="Previous station"
          aria-controls={panelId}
          disabled={active === 0}
          onClick={() => setActive((a) => Math.max(0, a - 1))}
          className="rounded-md border px-2 py-1 font-mono text-xs transition-colors hover:border-primary/60 disabled:opacity-40"
        >
          ←
        </button>
        <div className="relative flex flex-1 items-center justify-between px-1">
          <div className="absolute inset-x-2 top-1/2 h-px bg-border" aria-hidden />
          {LIFECYCLE_STATIONS.map((s, i) => (
            <button
              key={s.id}
              type="button"
              aria-label={`Station ${i + 1}: ${s.title}`}
              aria-current={i === active ? "step" : undefined}
              aria-controls={panelId}
              onClick={() => setActive(i)}
              className="relative z-10 flex size-6 items-center justify-center rounded-full border font-mono text-[10px] text-foreground transition-all"
              style={{
                // Tint the surface rather than filling with the saturated hue:
                // foreground-on-tint keeps the digit readable in both themes.
                background: `color-mix(in oklch, ${s.color} ${i === active ? 34 : 10}%, var(--background))`,
                borderColor: i === active ? s.color : "var(--muted-foreground)",
                borderWidth: i === active ? 2 : 1,
                transform: i === active ? "scale(1.25)" : undefined,
              }}
            >
              {i + 1}
            </button>
          ))}
        </div>
        <button
          type="button"
          aria-label="Next station"
          aria-controls={panelId}
          disabled={active === LIFECYCLE_STATIONS.length - 1}
          onClick={() => setActive((a) => Math.min(LIFECYCLE_STATIONS.length - 1, a + 1))}
          className="rounded-md border px-2 py-1 font-mono text-xs transition-colors hover:border-primary/60 disabled:opacity-40"
        >
          →
        </button>
      </div>
      <div
        id={panelId}
        aria-live="polite"
        className="min-h-28 rounded-lg border p-4"
        style={{ borderColor: station.color, background: `color-mix(in oklch, ${station.color} 6%, transparent)` }}
      >
        <div className="font-mono text-xs font-semibold">
          {active + 1} · {station.title}
        </div>
        <p className="mt-1.5 text-[12px] leading-relaxed text-muted-foreground">{station.detail}</p>
        <div className="mt-2.5 overflow-x-auto rounded-md border border-dashed bg-background/70 p-2 font-mono text-[10px]">
          {station.example}
        </div>
      </div>
      <p className="text-center font-mono text-[10px] text-muted-foreground">
        schematic conveyor with an illustrative example record — not a trace · ⚄ = the only seeded
        draws · # = hashed into the run identity
      </p>
    </div>
  );
}

const CONTRACT_CLAUSES = [
  "absolute paths only — no . or ..",
  "mode full|heads|lora|dora ⇔ peft block",
  "quantized sources rejected",
  "execution chosen here: native | resident_metal",
];

function JobContractFigure() {
  return (
    <Figure
      viewBox="0 0 440 230"
      title="the job is the contract"
      caption="Schematic: a validated JSON job with stamped clauses, and two learning-rate lanes — parameters whose canonical name contains 'encoder' train at 1e-5, everything else at 5e-4."
    >
      <rect x={35} y={30} width={190} height={150} rx={6} fill="color-mix(in oklch, var(--kfam-fusion) 8%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} />
      <text x={130} y={48} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        job.json · version 1
      </text>
      {CONTRACT_CLAUSES.map((c, i) => (
        <g key={c}>
          <rect x={46} y={58 + i * 28} width={168} height={20} rx={3} fill="color-mix(in oklch, var(--kfam-fusion) 12%, var(--background))" stroke="var(--kfam-fusion)" strokeWidth={0.6} />
          <text x={130} y={71 + i * 28} textAnchor="middle" fontSize={6} className="fill-muted-foreground font-mono">
            {c}
          </text>
        </g>
      ))}
      <text x={330} y={48} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        two LR groups
      </text>
      <rect x={250} y={58} width={160} height={50} rx={5} fill="color-mix(in oklch, var(--kfam-attention) 12%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1} />
      <text x={330} y={76} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        name contains "encoder"
      </text>
      <text x={330} y={92} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        1e-5 (incl. boundary/candidate_encoder)
      </text>
      <rect x={250} y={118} width={160} height={50} rx={5} fill="color-mix(in oklch, var(--kfam-sampling) 12%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1} />
      <text x={330} y={136} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        everything else
      </text>
      <text x={330} y={152} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        5e-4 · warmup 0.1 · seed 42
      </text>
      <text x={220} y={208} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        the substring split deliberately matches upstream's grouping
      </text>
    </Figure>
  );
}

function GoldScheduleFigure() {
  const gold: string[] = [];
  for (let i = 0; i <= 100; i++) {
    const f = i / 100;
    const g = f <= 0.15 ? 1 : 1 + (0.25 - 1) * ((f - 0.15) / 0.85);
    gold.push(`${50 + i * 3.4},${150 - g * 100}`);
  }
  const cons: string[] = [];
  for (let i = 0; i <= 100; i++) {
    const f = i / 100;
    const c = Math.min(1, f / 0.1);
    cons.push(`${50 + i * 3.4},${150 - c * 100}`);
  }
  const iou: string[] = [];
  for (let i = 0; i <= 100; i++) {
    const f = i / 100;
    const s = Math.max(0, 1 - f / 0.6);
    iou.push(`${50 + i * 3.4},${150 - s * 100}`);
  }
  return (
    <Figure
      viewBox="0 0 440 210"
      title="hold at 1.0, then wean to 0.25"
      caption="Schematic of the configured schedule, not a training log. Gold spans go into the candidate pool with probability 1.0 for the first 15% of optimizer steps, then fall in a straight line to 0.25. Ghosted behind it: two sibling schedules, one fading in and one fading out, drawn to illustrative scales."
    >
      <line x1={50} y1={150} x2={390} y2={150} stroke="var(--muted-foreground)" strokeWidth={1} />
      <line x1={50} y1={150} x2={50} y2={40} stroke="var(--muted-foreground)" strokeWidth={1} />
      <polyline points={cons.join(" ")} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} opacity={0.6} strokeDasharray="4 3" />
      <polyline points={iou.join(" ")} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} opacity={0.6} strokeDasharray="2 3" />
      <polyline points={gold.join(" ")} fill="none" stroke="var(--kfam-fusion)" strokeWidth={2.5} />
      <line x1={101} y1={150} x2={101} y2={46} stroke="var(--kfam-sampling)" strokeWidth={0.75} strokeDasharray="3 3" />
      <text x={104} y={42} fontSize={7} className="fill-muted-foreground font-mono">
        15%: the knee
      </text>
      <text x={385} y={132} textAnchor="end" fontSize={7} className="fill-muted-foreground font-mono">
        → 0.25
      </text>
      <text x={56} y={48} fontSize={7} className="fill-muted-foreground font-mono">
        1.0
      </text>
      <text x={390} y={164} textAnchor="end" fontSize={7.5} className="fill-muted-foreground font-mono">
        fraction of optimizer steps →
      </text>
      <text x={220} y={190} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        gold injection (solid) · consistency warmup ↗ · soft-IoU anneal ↘ (ghosted)
      </text>
    </Figure>
  );
}

const STREAMS = [
  { id: "gold", label: '"gold_injection"', color: "var(--kfam-fusion)" },
  { id: "neg", label: '"negative_queries"', color: "var(--kfam-attention)" },
  { id: "order", label: '"epoch_order"', color: "var(--kfam-sampling)" },
];

function SeededStreamsFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="domain separation: one seed, three taps"
      caption="Schematic: every draw comes from a SplitMix64 stream seeded by SHA-256(seed, counter, domain). Changing batching cannot consume another concern's draws — a native replay protocol, not a claim to reproduce PyTorch's RNG."
    >
      <rect x={40} y={72} width={90} height={36} rx={5} fill="color-mix(in oklch, var(--kfam-mmsg) 16%, transparent)" stroke="var(--kfam-mmsg)" strokeWidth={1.25} />
      <text x={85} y={94} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
        seed 42
      </text>
      <path d="M 130 90 h 28" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <text x={144} y={82} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        SHA-256
      </text>
      {STREAMS.map((s, i) => (
        <g key={s.id}>
          <path d={`M 158 90 L 200 ${52 + i * 40}`} stroke={s.color} strokeWidth={1.25} />
          <rect x={204} y={40 + i * 40} width={130} height={24} rx={4} fill={`color-mix(in oklch, ${s.color} 12%, transparent)`} stroke={s.color} strokeWidth={1} />
          <text x={269} y={56 + i * 40} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
            {s.label}
          </text>
          {[0, 1, 2, 3, 4].map((d) => (
            <circle key={`${s.id}-${d}`} cx={348 + d * 14} cy={52 + i * 40} r={2.5} fill={s.color} opacity={0.4 + ((d * 3 + i) % 4) * 0.15} />
          ))}
        </g>
      ))}
    </Figure>
  );
}

const LOSS_BARS = [
  { id: "start", label: "start marginals (focal)", w: 1.0, group: "boundary" },
  { id: "end", label: "end marginals (focal)", w: 1.0, group: "boundary" },
  { id: "pair", label: "candidate-pair BCE", w: 1.0, group: "pool" },
  { id: "inside", label: "inside BCE", w: 0.5, group: "boundary" },
  { id: "proposal", label: "proposal listwise", w: 0.3, group: "pool" },
  { id: "rerank", label: "listwise rerank", w: 0.3, group: "pool" },
  { id: "softiou", label: "soft-IoU aux (anneals)", w: 0.2, group: "pool", fade: true },
  { id: "abstain", label: "abstention BCE", w: 0.2, group: "calibration" },
  { id: "count", label: "Poisson count", w: 0.2, group: "calibration" },
  { id: "consistency", label: "noisy-OR consistency (warms up)", w: 0.1, group: "calibration", fade: true },
  { id: "classification", label: "classification BCE", w: 1.0, group: "structure" },
  { id: "record", label: "record + relation", w: 1.0, group: "structure" },
];

const GROUP_COLORS: Record<string, string> = {
  boundary: "var(--kfam-attention)",
  pool: "var(--kfam-fusion)",
  calibration: "var(--kfam-sampling)",
  structure: "var(--kfam-mmsg)",
};

function LossStackFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox={`0 0 440 ${LOSS_BARS.length * 20 + 70}`}
      title={step === 0 ? "twelve weighted terms" : "negatives are mined, not rolled"}
      caption={
        step === 0
          ? "Schematic weight bars, grouped by family — bundle-pinned where the bundles set them; the start/end/pair/inside weights are trainer defaults. Two terms are scheduled: soft-IoU anneals to zero, consistency warms up from zero. The masks feeding these losses are hashed into a decision fingerprint."
          : "Schematic: hard negatives are the deterministic top-k by score (ties by index) from an explicit heap — 20 per positive, minimum 16 in the released bundles. Which all-negative queries participate is a separate seeded, replayable draw."
      }
    >
      {step === 0 ? (
        <g>
          {LOSS_BARS.map((b, i) => (
            <g key={b.id} opacity={b.fade ? 0.75 : 1}>
              <text x={168} y={26 + i * 20 + 9} textAnchor="end" fontSize={7} className="fill-foreground font-mono">
                {b.label}
              </text>
              <rect x={176} y={26 + i * 20} width={Math.max(10, b.w * 150)} height={12} rx={3} fill={GROUP_COLORS[b.group]} opacity={b.fade ? 0.7 : 0.9} />
              <text x={182 + Math.max(10, b.w * 150)} y={26 + i * 20 + 9} fontSize={6.5} className="fill-foreground font-mono">
                {b.w}
              </text>
              <text x={372} y={26 + i * 20 + 9} fontSize={6} className="fill-muted-foreground font-mono">
                {b.group}
              </text>
            </g>
          ))}
          <rect x={176} y={LOSS_BARS.length * 20 + 32} width={150} height={18} rx={9} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} />
          <text x={251} y={LOSS_BARS.length * 20 + 44} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
            decision_fingerprint(masks)
          </text>
        </g>
      ) : (
        <g>
          {Array.from({ length: 10 }, (_, i) => ({ id: `n-${i}`, score: [0.91, 0.84, 0.8, 0.71, 0.62, 0.55, 0.4, 0.33, 0.21, 0.12][i], kept: i < 6 })).map((n, i) => (
            <g key={n.id}>
              <rect x={90} y={26 + i * 19} width={n.score * 220} height={12} rx={3} fill={n.kept ? "var(--kfam-attention)" : "color-mix(in oklch, var(--muted-foreground) 30%, transparent)"} opacity={n.kept ? 0.75 : 0.5} />
              <text x={86} y={35 + i * 19} textAnchor="end" fontSize={6.5} className="fill-muted-foreground font-mono">
                {n.score.toFixed(2)}
              </text>
              {n.kept && (
                <text x={318} y={35 + i * 19} fontSize={6.5} className="fill-muted-foreground font-mono">
                  kept
                </text>
              )}
            </g>
          ))}
          <text x={220} y={LOSS_BARS.length * 20 + 44} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
            top-k by (score desc, index asc) — the same list every run
          </text>
        </g>
      )}
    </Figure>
  );
}

const COST_GRID = Array.from({ length: 20 }, (_, i) => {
  const col = i % 5;
  const row = Math.floor(i / 5);
  return { id: `c-${col}-${row}`, col, row, chosen: (row === 0 && col === 2) || (row === 1 && col === 4) || (row === 2 && col === 0) };
});

function HungarianFigure() {
  return (
    <Figure
      viewBox="0 0 440 220"
      title="assignment with an escape column"
      caption="Schematic: the grid is the per-field assignment logits — column 0 is ABSENT, so a field may select no candidate. The instance→gold Hungarian itself runs on a rectangular [instances × gold] cost matrix with no escape column; unmatched instances simply receive no gold. Only assignment membership is detached; matched losses recompute from live logits."
    >
      <text x={70} y={40} textAnchor="end" fontSize={7} className="fill-muted-foreground font-mono">
        fields ↓
      </text>
      <text x={220} y={26} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        ABSENT · candidate columns →
      </text>
      {COST_GRID.map((c) => (
        <g key={c.id}>
          <rect x={90 + c.col * 52} y={36 + c.row * 40} width={46} height={34} rx={3} fill={c.col === 0 ? "color-mix(in oklch, var(--muted-foreground) 16%, transparent)" : "color-mix(in oklch, var(--kfam-fusion) 8%, transparent)"} stroke={c.col === 0 ? "var(--muted-foreground)" : "var(--kfam-fusion)"} strokeWidth={0.7} />
          {c.chosen && <circle cx={113 + c.col * 52} cy={53 + c.row * 40} r={13} fill="none" stroke="var(--kfam-attention)" strokeWidth={1.75} />}
        </g>
      ))}
      <rect x={90} y={153} width={254} height={40} rx={3} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} strokeDasharray="4 3" />
      <path d="M 96 181 L 338 169" stroke="var(--kfam-kv)" strokeWidth={1.25} />
      <text x={370} y={188} fontSize={6.5} className="fill-muted-foreground font-mono">
        invalid: removed
      </text>
      <text x={220} y={210} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        f64 reference vs production f32 costs — exact ties may differ, by design
      </text>
    </Figure>
  );
}

function DoraDetachFigure() {
  return (
    <Figure
      viewBox="0 0 440 210"
      title="DoRA's norm is live math with the gradient snipped"
      caption="Schematic: the direction norm is recomputed from W + scale·BA on every forward, then detached through the stop_gradient intrinsic — the backward arrow bounces off. The magnitude vector is the trained parameter."
    >
      <rect x={40} y={70} width={90} height={30} rx={5} fill="color-mix(in oklch, var(--dtype-f16) 14%, transparent)" stroke="var(--dtype-f16)" strokeWidth={1.25} />
      <text x={85} y={89} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        W + scale·BA
      </text>
      <path d="M 130 85 h 26" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect x={158} y={62} width={120} height={46} rx={23} fill="none" stroke="var(--kfam-kv)" strokeWidth={1.5} strokeDasharray="6 3" />
      <text x={218} y={82} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        norm = √Σrow²
      </text>
      <text x={218} y={97} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        stop_gradient capsule
      </text>
      <path d="M 250 118 q -20 22 -44 6" fill="none" stroke="var(--kfam-attention)" strokeWidth={1.25} strokeDasharray="4 2" />
      <path d="M 206 124 l 8 -2 m -8 2 l 6 6" stroke="var(--kfam-attention)" strokeWidth={1.25} fill="none" />
      <text x={236} y={142} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        backward bounces off
      </text>
      <path d="M 278 85 h 26" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <text x={291} y={78} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        ÷
      </text>
      <rect x={306} y={70} width={104} height={30} rx={5} fill="color-mix(in oklch, var(--kfam-sampling) 20%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1.5} />
      <text x={358} y={89} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        magnitude m (trained)
      </text>
      <text x={220} y={182} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
        stop_gradient is a graph intrinsic, so lowering can never lose the detach boundary
      </text>
    </Figure>
  );
}

const DT_KERNELS = ["validate_f32", "validate_control", "zero", "forward", "rows", "dq", "dkdv", "relative"];

function PackedAttentionFigure() {
  return (
    <Figure
      viewBox="0 0 440 230"
      title="training attention v1: packed operands, eight pipelines"
      caption="Schematic. The queries, keys and values arrive as one packed block, the relative-position weights as another, and a small integer strip carries the control information. All three feed a single graph operation that applies dropout internally. On Metal that one operation is carried out by eight dt_* GPU programs working in fixed 64-key tiles, with no CPU fallback."
    >
      <rect x={36} y={36} width={100} height={44} rx={4} fill="color-mix(in oklch, var(--kfam-attention) 14%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1} />
      <text x={86} y={61} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        qkv [3·B·S, H]
      </text>
      <rect x={36} y={88} width={100} height={32} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 14%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1} />
      <text x={86} y={108} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        relative [2R, H]
      </text>
      <rect x={36} y={128} width={100} height={16} rx={4} fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1} />
      <text x={86} y={139} textAnchor="middle" fontSize={6.5} className="fill-foreground font-mono">
        i32 control strip
      </text>
      {[58, 104, 136].map((y) => (
        <path key={`in-${y}`} d={`M 136 ${y} L 186 92`} stroke="var(--muted-foreground)" strokeWidth={0.9} />
      ))}
      <rect x={190} y={70} width={150} height={44} rx={5} fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)" stroke="var(--kfam-mmsg)" strokeWidth={1.5} />
      <text x={265} y={88} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
        deberta_training_attention_v1
      </text>
      <text x={265} y={103} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        dropout lives inside · stream (layer≪32)|3
      </text>
      {DT_KERNELS.map((k, i) => (
        <g key={k}>
          <rect x={40 + (i % 4) * 95} y={152 + Math.floor(i / 4) * 24} width={88} height={18} rx={3} fill="color-mix(in oklch, var(--kfam-attention) 12%, var(--background))" stroke="var(--kfam-attention)" strokeWidth={0.8} />
          <text x={84 + (i % 4) * 95} y={164 + Math.floor(i / 4) * 24} textAnchor="middle" fontSize={6.5} className="fill-foreground font-mono">
            dt_{k}
          </text>
        </g>
      ))}
      <path d="M 265 114 v 32" stroke="var(--muted-foreground)" strokeWidth={0.9} />
      <text x={220} y={212} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        materialized_v1 (quadratic masks, default) ⇄ replay_tiled_v1 (tiled keys — 64 on Metal; mask inputs become null)
      </text>
    </Figure>
  );
}

function RecomputeFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="retain or recompute"
      caption="Schematic: retained_v1 keeps every encoder activation for backward; layer_recompute_v1 formally cuts the head graph, re-runs encoder regions during backward inside a dedicated 512 MiB arena, and replays dropout from typed leaves."
    >
      <text x={70} y={40} textAnchor="end" fontSize={7.5} className="fill-muted-foreground font-mono">
        retained
      </text>
      {[0, 1, 2, 3].map((i) => (
        <rect key={`r-${i}`} x={90 + i * 76} y={28} width={64} height={24} rx={4} fill="color-mix(in oklch, var(--kfam-attention) 16%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1} />
      ))}
      <text x={70} y={98} textAnchor="end" fontSize={7.5} className="fill-muted-foreground font-mono">
        recompute
      </text>
      {[0, 1, 2, 3].map((i) => (
        <rect key={`e-${i}`} x={90 + i * 76} y={86} width={64} height={24} rx={4} fill="none" stroke="var(--kfam-attention)" strokeWidth={1} strokeDasharray="4 3" />
      ))}
      <path d="M 380 98 q 30 34 -60 40 h -180" fill="none" stroke="var(--kfam-fusion)" strokeWidth={1.25} strokeDasharray="5 3" />
      <text x={250} y={152} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        re-filled during backward · dropout replayed from typed leaves
      </text>
      <rect x={330} y={124} width={80} height={20} rx={4} fill="none" stroke="var(--kfam-kv)" strokeWidth={1} />
      <text x={370} y={138} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        512 MiB arena
      </text>
      <text x={220} y={182} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        "an absent selected gradient never becomes an implicit zero"
      </text>
    </Figure>
  );
}

const ACCUM_TILES = [0, 1, 2, 3, 4, 5].map((i) => ({ id: `mb-${i}`, filled: i < 4 }));

function RenormFigure() {
  return (
    <Figure
      viewBox="0 0 440 200"
      title="the partial window renormalizes"
      caption="Schematic: gradients accumulate as g += new/N; when the final window closes early, every pending gradient is rescaled by N/actual — matching upstream exactly. Parameters absent for the whole window keep grad=None, weight decay included."
    >
      <text x={220} y={30} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
        accumulation window (N = 6, actual = 4)
      </text>
      {ACCUM_TILES.map((t, i) => (
        <rect key={t.id} x={80 + i * 48} y={44} width={40} height={28} rx={4} fill={t.filled ? "color-mix(in oklch, var(--kfam-fusion) 20%, transparent)" : "none"} stroke={t.filled ? "var(--kfam-fusion)" : "var(--border)"} strokeWidth={1} strokeDasharray={t.filled ? undefined : "4 3"} />
      ))}
      <path d="M 220 80 v 18" stroke="var(--muted-foreground)" strokeWidth={1.25} />
      <rect x={150} y={102} width={140} height={24} rx={5} fill="color-mix(in oklch, var(--kfam-sampling) 16%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1.25} />
      <text x={220} y={118} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
        × N / actual
      </text>
      <text x={220} y={150} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
        then clip = max_grad_norm / (‖g‖ + 1e-6), default 1.0 — 0.7 is a parity fixture, not the default
      </text>
      <text x={220} y={172} textAnchor="middle" fontSize={7} className="fill-muted-foreground font-mono">
        hollow slots: params with no gradient skip the step entirely, decay included
      </text>
    </Figure>
  );
}

function TransactionFigure({ step }: { step: 0 | 1 }) {
  return (
    <Figure
      viewBox="0 0 440 210"
      title={step === 0 ? "resident AdamW is a transaction" : "leaving the run: papers, please"}
      caption={
        step === 0
          ? "Schematic: on resident Metal, gradients, moments and weights stay on the device; the update is a two-phase prepare→commit transaction, and a failed transaction leaves the old state usable. Gradients are never downloaded."
          : "Schematic exit papers: an immutable run manifest (with the executable's own digest), a resume that demands a fresh output directory, exports that refuse an unfinished window, and receipts that never claim quality."
      }
    >
      {step === 0 ? (
        <g>
          <rect x={60} y={36} width={210} height={120} rx={8} fill="color-mix(in oklch, var(--kfam-fusion) 8%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.5} />
          <text x={165} y={54} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
            GPU
          </text>
          {["grads", "m, v", "weights"].map((b, i) => (
            <g key={b}>
              <rect x={76 + i * 62} y={64} width={54} height={24} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={0.9} />
              <text x={103 + i * 62} y={80} textAnchor="middle" fontSize={7} className="fill-foreground font-mono">
                {b}
              </text>
            </g>
          ))}
          <path d="M 100 112 h 130" stroke="var(--kfam-sampling)" strokeWidth={1.5} />
          <text x={165} y={106} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
            prepare → commit (no-fail swap)
          </text>
          <rect x={330} y={64} width={80} height={60} rx={6} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} />
          <text x={370} y={98} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
            CPU
          </text>
          <path d="M 274 94 h 40" stroke="var(--kfam-kv)" strokeWidth={1.5} />
          <path d="M 296 82 v 24" stroke="var(--kfam-kv)" strokeWidth={2} />
          <text x={294} y={140} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
            gradients never cross
          </text>
          <text x={220} y={186} textAnchor="middle" fontSize={6.5} className="fill-muted-foreground font-mono">
            declarative envelope: host 6 GiB−128 MiB · backend 4 GiB · combined 12 GiB — OOM is phase-attributed
          </text>
        </g>
      ) : (
        <g>
          {[
            { id: "run", label: "run.json", sub: "config · digests · executable hash · math_policy strict_f32_activations_v1", x: 40 },
            { id: "ckpt", label: "latest.safetensors", sub: "resume → NEW output dir + fingerprint check", x: 176 },
            { id: "model", label: "model/ export", sub: "full·heads·lora·dora — no partial windows", x: 312 },
          ].map((c) => (
            <g key={c.id}>
              <rect x={c.x} y={50} width={110} height={64} rx={5} fill="color-mix(in oklch, var(--kfam-matvec) 10%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1} />
              <text x={c.x + 55} y={74} textAnchor="middle" fontSize={7.5} className="fill-foreground font-mono">
                {c.label}
              </text>
              <foreignObject x={c.x + 6} y={82} width={98} height={30}>
                <div className="text-center font-mono text-[6.5px] leading-tight text-muted-foreground">{c.sub}</div>
              </foreignObject>
            </g>
          ))}
          <text x={220} y={148} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
            merge policy pinned: f32 LoRA deltas, f64 DoRA row norms
          </text>
          <text x={220} y={172} textAnchor="middle" fontSize={7.5} style={{ fill: "var(--kfam-text-kv)" }} className="font-mono">
            calibration/test files: preflighted and digested only — evaluation_performed: false
          </text>
        </g>
      )}
    </Figure>
  );
}

const COMPARE_ROWS = [
  ["Philosophy", "graph-first: losses are graph ops, autodiff does the rest", "contract-first: a signed job under a disposable worker"],
  ["Losses", "graph nodes (masked BCE, composite objective)", "12 host loss terms handing explicit cotangents to the graph"],
  ["Negatives & supervision", "random negative-mask rate 0.5, per-label weights", "Hungarian matching + gold schedule + mined negatives, all fingerprinted"],
  ["PEFT", "LoRA (rank-1 fused backward on Metal)", "LoRA and DoRA, with an in-graph stop_gradient detach"],
  ["Accumulation", "fixed divisor", "partial-window renormalization to the actual count"],
  ["Backends", "native + Metal + CUDA", "native + resident Metal only"],
  ["Front door", "~55-flag CLI (+ recipe lifecycle)", "job.json + two flags, supervised one-shot worker"],
  ["Evaluation", "in-process held-out loss, early stopping", "holdouts digested only — evaluation_performed: false"],
];

function PhilosophyCompareFigure() {
  return (
    <div className="flex h-full flex-col justify-center">
      <table className="w-full table-fixed border-collapse overflow-hidden rounded-lg border text-left">
        <caption className="sr-only">
          GLiNER2 compared with GLiNER2.5 across eight training dimensions
        </caption>
        <thead>
          <tr className="border-b bg-muted/30 font-mono text-[9px] uppercase tracking-wider text-muted-foreground">
            <th scope="col" className="w-[90px] px-2 py-1.5 font-normal">
              <span className="sr-only">Dimension</span>
            </th>
            <th scope="col" className="border-l px-2 py-1.5 font-normal" style={{ color: "var(--kfam-text-attention)" }}>
              GLiNER2
            </th>
            <th scope="col" className="border-l px-2 py-1.5 font-normal" style={{ color: "var(--kfam-text-fusion)" }}>
              GLiNER2.5
            </th>
          </tr>
        </thead>
        <tbody>
          {COMPARE_ROWS.map(([dim, a, b]) => (
            <tr key={dim} className="border-b text-[9.5px] last:border-b-0">
              <th scope="row" className="px-2 py-1.5 align-top font-mono font-normal text-muted-foreground">
                {dim}
              </th>
              <td className="border-l px-2 py-1.5 align-top" style={{ background: "color-mix(in oklch, var(--kfam-attention) 5%, transparent)" }}>
                {a}
              </td>
              <td className="border-l px-2 py-1.5 align-top" style={{ background: "color-mix(in oklch, var(--kfam-fusion) 5%, transparent)" }}>
                {b}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      <p className="mt-2 text-center font-mono text-[10px] text-muted-foreground">
        both stacks: FP32 contract, AdamW, seed 42 — and receipts that never certify quality
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

export function Gliner25TrainingClient({
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
            / GLiNER2.5
          </p>
          <h1 className="mt-1 text-3xl font-bold tracking-tight">GLiNER2.5 finetuning</h1>
          <p className="mt-2 max-w-3xl text-muted-foreground">
            Training GLiNER2.5 starts with a single JSON job file, and a supervised worker runs it
            exactly once. The losses are computed in plain host code and handed back to the graph,
            and everything that could make two runs differ — every random draw, every frozen-off
            gradient, every file it writes — is hashed into the run's identity, so it is always
            provable exactly which run produced which weights. The{" "}
            <Link className="text-primary underline" href="/models/gliner25">
              GLiNER2.5 model page
            </Link>{" "}
            covers the architecture; this page covers how it learns.
          </p>
        </header>

        {/* ── 1 · Contract ─────────────────────────────────────────── */}
        <ScrollyChapter
          id="contract"
          number={1}
          title="Contract-first: a supervised one-shot job"
          intro="GLiNER2.5 doesn't take flags — it takes a signed contract and runs it exactly once."
        >
          <Scene id="worker" graphic={<OneShotWorkerFigure />}>
            <p>
              Training is launched with one command and one file:{" "}
              <code>train run gliner25 &lt;job.json&gt;</code>. Only two optional flags exist — one
              to stop after N batches, one to set how long a shutdown may take. The command then
              starts a separate worker process to do the actual training. Both sides read the job
              file and the worker re-derives a fingerprint of it, so if the file changed between
              them, the run stops before any model is touched. Progress streams out as one JSON line per step,
              and the exit code says what happened: 124 means the time limit hit, 86 means a hard
              cancellation (a second signal or an expired grace), and ordinary failures exit 1. Nothing restarts itself — a run
              that dies leaves its last saved checkpoint and nothing else.
            </p>
            <p>
              <CodeLink link={L("gliner25-train-entry")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-one-shot-parent")} />
            </p>
          </Scene>
          <Scene id="job" graphic={<JobContractFigure />}>
            <p>
              The job file is checked like a contract before anything runs. Paths must be
              absolute. The training mode — full, heads-only, LoRA or DoRA — has to agree with
              whether an adapter section is present. Quantized models are refused: training is
              full-precision only. Even the choice of hardware (CPU or resident Metal) is written
              in the file, never picked up from an environment variable. Sensible defaults ride
              along: 10 epochs, batches of 2, seed 42. There are two learning rates rather than
              one. Anything whose name contains "encoder" learns slowly, at 1e-5; the newer task
              heads learn faster, at 5e-4. That split matches how the original authors grouped the
              parameters — the pretrained encoder only needs nudging, while the heads start from
              scratch.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-job-validate")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-job-execute")} />
            </p>
          </Scene>
          <Scene id="lifecycle" graphic={<RecordLifecycleFigure />}>
            <p>
              <strong>One record, start to finish.</strong> Every training example follows the
              same eight-station conveyor. A row arrives from <code>train.jsonl</code> carrying
              its text, its schema, and exact character offsets for every annotation — and is
              validated strictly: nothing is snapped to the nearest word, inferred, or silently
              truncated. The schema is serialized ahead of the text and tokenized; the offsets
              compile into dense packed targets. The forward pass runs encoder, boundary head and
              shared pool — with gold spans injected on the schedule — then the twelve loss terms
              and the Hungarian matcher grade the outputs, gradients flow back and accumulate
              into an AdamW update, and the run writes its receipts. Only two stations ever draw
              a random number, and both draws are seeded; everything else is arithmetic. The
              chapters below take the stations one at a time.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-dataset-row")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-targets-compile")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 2 · Gold schedule ────────────────────────────────────── */}
        <ScrollyChapter
          id="gold"
          number={2}
          title="The gold-injection schedule"
          intro="Early training cheats — gold spans are force-fed into the candidate pool, then weaned off on a fixed curve."
        >
          <Scene id="curve" graphic={<GoldScheduleFigure />}>
            <p>
              Early in training the model is bad at proposing candidate spans — so bad that the
              correct answers might never even appear in its candidate pool, leaving nothing to
              learn from. The fix is to cheat, on a schedule. The known-correct ("gold") spans
              are dropped into the pool with probability 1.0 for the first 15% of training. After
              that, the probability falls in a straight line to 0.25 by the end. Training wheels
              that come off gradually: the model is handed the answer while it cannot find one,
              then made to find it for itself.
            </p>
            <p>
              Two smaller schedules run on the same clock. One loss term fades in over the first
              2,000 steps; another fades out over 20,000. The whole schedule is hashed into the
              run's identity, so a rerun cannot quietly use a different one.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-gold-schedule")} />
            </p>
          </Scene>
          <Scene id="streams" graphic={<SeededStreamsFigure />}>
            <p>
              Every random decision draws from its own labeled stream of numbers, derived from
              the seed plus a name — "gold_injection", "negative_queries", "epoch_order". Because
              each concern owns its stream, changing one thing (say, the batch layout) can never
              shift the random numbers another part sees. That separation is what makes a run
              exactly repeatable. The code is careful about the claim, though: it replays its own
              randomness bit-for-bit; it does not promise to reproduce PyTorch's.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-streams")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 3 · Losses ───────────────────────────────────────────── */}
        <ScrollyChapter
          id="losses"
          number={3}
          title="Twelve losses, gradients by hand"
          intro="Every loss is ordinary CPU code that hands its gradient back to the graph by hand — the opposite of GLiNER2's in-graph losses."
        >
          <Scene id="stack" graphic={<LossStackFigure step={0} />}>
            <p>
              Twelve loss terms, each pinned to the exact upstream commit it copies. Four of them
              do the heavy lifting. A <em>focal</em> loss on the boundary scores goes easy on
              positives and comes down hard on easy negatives — the released models prefer it to plain binary cross-entropy, because most
                candidate spans are obvious rejects and would otherwise drown out the interesting
                ones. A <em>pairing</em> loss scores candidate
              spans. A <em>consistency</em> term checks that two views of the same question agree:
              the probability assigned to a span, and the probabilities assigned to its start and
              end separately. Two <em>ranking</em> losses push correct answers above wrong ones.
            </p>
            <p>
              The rest are smaller: a loss for the abstain head, a count loss, and the
              classification, record and relation objectives. Each of the twelve is a small CPU
              function returning both the loss value <em>and</em> its gradient, and the exact
              masks it used are hashed into the run's "decision fingerprint". The figure's caption
              carries the precise weights.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-focal")} /> · <CodeLink link={L("gliner25-ft-consistency")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-listwise")} /> · <CodeLink link={L("gliner25-ft-objectives")} />
            </p>
          </Scene>
          <Scene id="negatives" graphic={<LossStackFigure step={1} />}>
            <p>
              The model also needs wrong answers to learn from. It picks them deterministically:
              the highest-scoring wrong spans, meaning the ones it is most confidently wrong
              about, with ties broken by position. Those are the informative mistakes — a span the
              model already rates near zero teaches it nothing. The released models keep 20 such
              spans per correct one, with a floor of 16; the code's own defaults are lower.
            </p>
            <p>
              One thing genuinely is random: which of the queries with no correct answer at all
              join the loss. Even that draw comes from its own seeded stream, so it replays
              identically. Targets are exact — nothing gets snapped or truncated.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-hard-negatives")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 4 · Matching ─────────────────────────────────────────── */}
        <ScrollyChapter
          id="matching"
          number={4}
          title="Hungarian matching with an ABSENT column"
          intro="Before record predictions can be graded, training must decide which prediction corresponds to which correct record — an assignment problem, solved optimally."
        >
          <Scene id="assign" graphic={<HungarianFigure />}>
            <p>
              Say the document holds two real invoices and the model produced three candidate
              record instances — which candidate gets graded against which invoice? Training
              answers with the Hungarian algorithm, the classic optimal-assignment method, over a
              cost table of instances versus gold records. Invalid predictions are removed{" "}
              <em>before</em> the matching runs, so they can never "use up" a real record; extra
              predictions simply match nothing. Each field also has an ABSENT option — column 0 —
              meaning "this field selects no span at all". And one subtlety keeps the gradients
              honest: only the pairing itself is frozen; the losses are then recomputed from the
              live model outputs. (The fast f32 cost path is double-checked against a
              high-precision reference; exact ties may resolve differently, by documented
              design.)
            </p>
            <p>
              <CodeLink link={L("gliner25-hungarian-match")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-costs-f32")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 5 · PEFT ─────────────────────────────────────────────── */}
        <ScrollyChapter
          id="peft"
          number={5}
          title="LoRA and DoRA in the graph"
          intro="Adapters are graph surgery — and DoRA's norm is live math with the gradient snipped."
        >
          <Scene id="dora" graphic={<DoraDetachFigure />}>
            <p>
              Instead of retraining all ~194M weights, small adapters can be attached. LoRA is
              the standard trick: two skinny matrices whose product nudges a frozen weight matrix.
              DoRA goes one step further — it also learns how <em>long</em> each weight row should
              be (a magnitude), separately from its direction. Computing that requires the row
              length of the combined weights, recalculated every step — but the gradient must not
              flow through that calculation, so the graph wraps it in a{" "}
              <code>stop_gradient</code> node the backward pass bounces off. Name an adapter
              target that doesn't exist, or inject twice, and it refuses rather than guessing.
            </p>
            <p>
              <CodeLink link={L("gliner25-peft-inject")} /> · <CodeLink link={L("gliner25-peft-kinds")} /> ·{" "}
              <CodeLink link={L("gliner25-stop-gradient")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 6 · Training attention ───────────────────────────────── */}
        <ScrollyChapter
          id="attention"
          number={6}
          title="Training attention v1 and recompute"
          intro="Attention gets its own training-only implementation — and a switch that trades compute for memory."
        >
          <Scene id="packed" graphic={<PackedAttentionFigure />}>
            <p>
              During training, attention runs through a dedicated operation built just for this
              job. Its inputs arrive packed into a few large blocks, and dropout happens{" "}
              <em>inside</em> the operation on its own random stream, so it replays exactly. On
              the GPU it becomes eight small Metal programs — the <code>dt_*</code> family —
              behind a strict interface that checks every buffer size and refuses to fall back to
              the CPU: if the GPU path can't run, that's an error, not a silent slowdown. A
              second profile, <code>replay_tiled_v1</code>, processes keys in fixed-size tiles
              (64 on Metal) instead of materializing the full attention mask.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-attn-profiles")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-attn-vjp")} /> · <CodeLink link={L("gliner25-ft-dt-kernel")} />
            </p>
          </Scene>
          <Scene id="recompute" graphic={<RecomputeFigure />}>
            <p>
              Training normally keeps every layer's intermediate results around for the backward
              pass, and that costs memory. The <code>layer_recompute_v1</code> profile trades
              compute for memory instead: it throws those intermediates away and recomputes each
              encoder region during the backward pass, inside its own dedicated 512 MiB scratch
              arena, replaying the exact same dropout. The safety rule is written into the module
              docs: a missing gradient is an error — it is never silently treated as zero.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-activation-profiles")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-recompute")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 7 · Optimizer ────────────────────────────────────────── */}
        <ScrollyChapter
          id="optimizer"
          number={7}
          title="The optimizer keeps receipts"
          intro="The weight update is as careful as everything else: staged, verified, and receipted."
        >
          <Scene id="renorm" graphic={<RenormFigure />}>
            <p>
              Gradients from several small batches are averaged together before each weight
              update, and every update is staged and checked before it is published. Sometimes the
              data runs out mid-window: four batches arrive where six were expected. The average is
              then rescaled to the real count, rather than dividing by six and quietly coming out
              too small — which would shrink that step for no reason. This matches the original
              implementation. A parameter that saw no data in the window is skipped
              entirely, weight decay included. Gradients are clipped at a total norm of 1.0 by
              default (the 0.7 that appears in a test fixture is the fixture's setting, not the
              default).
            </p>
            <p>
              <CodeLink link={L("gliner25-adamw-renorm")} />
            </p>
          </Scene>
          <Scene id="transaction" graphic={<TransactionFigure step={0} />}>
            <p>
              When training on the GPU, the optimizer's running averages stay on the GPU too. Each
              update is a two-step transaction: build the new values in fresh buffers, check them,
              then swap. A failure part-way through therefore leaves the old weights intact rather
              than half-written. Gradients never need to be copied back to the CPU at all. Memory limits are
              declared up front in the job file, and when one is exceeded, the error names
              exactly which phase of the step hit it.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-transaction")} />
            </p>
          </Scene>
          <Scene id="exit" graphic={<TransactionFigure step={1} />}>
            <p>
              A finished run leaves a paper trail. <code>run.json</code> records the full
              config, a checksum of every input file, and even a hash of the training executable
              itself. Resuming requires a fresh output directory and re-verifies everything
              first. Exporting the trained model refuses to run in the middle of an accumulation
              window, and every exported artifact carries a receipt — which deliberately
              certifies integrity, never quality. Supplied calibration and test files are
              checksummed but never scored: the manifest literally records{" "}
              <code>evaluation_performed: false</code>.
            </p>
            <p>
              <CodeLink link={L("gliner25-ft-export")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-merge-policy")} />
            </p>
          </Scene>
        </ScrollyChapter>

        {/* ── 8 · Versus ───────────────────────────────────────────── */}
        <ScrollyChapter
          id="versus"
          number={8}
          title="GLiNER2 vs GLiNER2.5: two training philosophies"
          intro="Same encoder family, opposite philosophies — the comparison in one table."
        >
          <Scene id="table" graphic={<PhilosophyCompareFigure />}>
            <p>
              <Link className="text-primary underline" href="/training/gliner2">
                GLiNER2
              </Link>{" "}
              is graph-first: write the loss as ops, let autodiff and custom fused VJPs produce
              every gradient, and prove correctness against a pinned PyTorch oracle. GLiNER2.5 is
              contract-first: host-computed losses hand explicit cotangents into a staged graph,
              every detach boundary and random draw is named and hashed, and receipts follow every
              publication. Neither is "more correct" — they answer different questions. GLiNER2
              asks <em>does my gradient match PyTorch's?</em>; GLiNER2.5 asks{" "}
              <em>can I prove exactly which run produced these weights?</em>
            </p>
            <p>
              <CodeLink link={L("gliner2-ft-objectives")} /> ·{" "}
              <CodeLink link={L("gliner25-ft-objectives")} />
            </p>
          </Scene>
        </ScrollyChapter>
      </div>
    </SnippetProvider>
  );
}
