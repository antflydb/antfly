"use client";

import { cn } from "@antfly/design-system";
import { useEffect, useState } from "react";
import { Figure } from "@/components/viz/glyphs";

/* ------------------------------------------------------------------ */
/* Ch 1 — schema card + spans lighting up (illustrative example)       */
/* ------------------------------------------------------------------ */

const SAMPLE_TOKENS: Array<{ text: string; label?: "company" | "person" | "amount" }> = [
  { text: "Acme Robotics", label: "company" },
  { text: " hired " },
  { text: "Dr. Elena Ruiz", label: "person" },
  { text: " after raising " },
  { text: "$40M", label: "amount" },
  { text: " from " },
  { text: "Northgate Capital", label: "company" },
  { text: "." },
];

const LABEL_COLORS: Record<string, string> = {
  company: "var(--kfam-attention)",
  person: "var(--kfam-fusion)",
  amount: "var(--kfam-sampling)",
};

export function SchemaExtractionFigure({ animate = false }: { animate?: boolean }) {
  const [lit, setLit] = useState(animate ? 0 : 99);
  useEffect(() => {
    if (!animate) return;
    const t = setInterval(() => setLit((v) => (v >= 6 ? 0 : v + 1)), 900);
    return () => clearInterval(t);
  }, [animate]);

  let spanIdx = 0;
  return (
    <div className="flex h-full flex-col justify-center gap-5">
      <div className="rounded-lg border bg-muted/30 p-4">
        <div className="mb-2 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
          illustrative entity schema
        </div>
        <pre className="font-mono text-xs leading-relaxed">
          {`entities:\n`}
          {Object.keys(LABEL_COLORS).map((label) => (
            <span key={label}>
              {"  - "}
              <span style={{ color: LABEL_COLORS[label].replace("--kfam-", "--kfam-text-") }}>{label}</span>
              {"\n"}
            </span>
          ))}
        </pre>
      </div>
      <div className="text-center text-muted-foreground">↓ one encoder pass</div>
      <div className="rounded-lg border p-4 text-sm leading-loose">
        {SAMPLE_TOKENS.map((tok) => {
          if (!tok.label) return <span key={tok.text}>{tok.text}</span>;
          const idx = spanIdx++;
          const on = idx < lit;
          return (
            <span
              key={tok.text}
              className={cn("rounded px-0.5 transition-all duration-500", on ? "text-foreground" : "")}
              style={{
                background: on ? `color-mix(in oklch, ${LABEL_COLORS[tok.label]} 22%, transparent)` : undefined,
                boxShadow: on ? `inset 0 -2px 0 ${LABEL_COLORS[tok.label]}` : undefined,
              }}
            >
              {tok.text}
              {on && (
                <sup className="ml-0.5 font-mono text-[9px]" style={{ color: LABEL_COLORS[tok.label].replace("--kfam-", "--kfam-text-") }}>
                  {tok.label}
                </sup>
              )}
            </span>
          );
        })}
      </div>
      <div className="text-center font-mono text-[11px] text-muted-foreground">
        illustrative highlights, not model output · candidate spans × labels score in parallel
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 2 — encoder vs decoder contrast strip                            */
/* ------------------------------------------------------------------ */

export function EncoderVsDecoderFigure({ emphasis }: { emphasis: "mask" | "kv" }) {
  const cells = 8;
  const cs = 13;
  const grid = (x0: number, y0: number, causal: boolean) => {
    const rects = [];
    for (let r = 0; r < cells; r++) {
      for (let c = 0; c < cells; c++) {
        const visible = causal ? c <= r : true;
        rects.push(
          <rect
            key={`${r}-${c}`}
            x={x0 + c * cs}
            y={y0 + r * cs}
            width={cs - 1.5}
            height={cs - 1.5}
            rx={1.5}
            fill={visible ? "var(--kfam-attention)" : "none"}
            stroke={visible ? "none" : "var(--border)"}
            strokeWidth={0.75}
            opacity={visible ? (causal ? 0.7 : 0.45) : 1}
          />,
        );
      }
    }
    return rects;
  };
  return (
    <Figure
      viewBox="0 0 420 220"
      title="the same attention matrix, two worlds"
      caption={
        emphasis === "mask"
          ? "Left: a GPT decoder masks the upper triangle — token i can never see token i+1. Right: GLiNER2 attends everywhere; an entity's evidence can sit after it in the sentence."
          : "GLiNER2 finishes with a forward pass and span decoding. With no autoregressive loop, it needs no persistent KV cache or token sampler."
      }
    >
      <text x={110} y={30} textAnchor="middle" fontSize={10} className="fill-muted-foreground font-mono">
        GPT decoder (causal)
      </text>
      {grid(58, 42, true)}
      <text x={310} y={30} textAnchor="middle" fontSize={10} className="fill-primary font-mono">
        GLiNER2 encoder (bidirectional)
      </text>
      {grid(258, 42, false)}
      {emphasis === "kv" && (
        <>
          <text x={110} y={175} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
            KV cache · sampler · decode loop
          </text>
          <text x={310} y={175} textAnchor="middle" fontSize={9} className="fill-primary font-mono">
            none of the above
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 3 — C2C + C2P + P2C decomposition                                */
/* ------------------------------------------------------------------ */

function ScoreMatrix({
  x,
  y,
  color,
  mode,
  label,
  sub,
  dim,
}: {
  x: number;
  y: number;
  color: string;
  mode: "content" | "toeplitz" | "toeplitzT" | "sum";
  label: string;
  sub: string;
  dim?: boolean;
}) {
  const n = 6;
  const cs = 12;
  const cells = [];
  for (let r = 0; r < n; r++) {
    for (let c = 0; c < n; c++) {
      let o = 0.55;
      if (mode === "content") o = 0.25 + ((r * 7 + c * 3) % 5) * 0.13;
      if (mode === "toeplitz") o = 0.3 + ((r * 7 + Math.abs(r - c) * 3) % 6) * 0.1;
      if (mode === "toeplitzT") o = 0.25 + ((c * 5 + Math.abs(c - r) * 2) % 6) * 0.1;
      if (mode === "sum") o = 0.35 + ((r * 5 + c * 2) % 4) * 0.1 + (0.5 - Math.abs(r - c) * 0.08);
      cells.push(
        <rect
          key={`${r}-${c}`}
          x={x + c * cs}
          y={y + r * cs}
          width={cs - 1.5}
          height={cs - 1.5}
          rx={1.5}
          fill={color}
          opacity={Math.max(0.06, Math.min(0.95, o))}
        />,
      );
    }
  }
  return (
    <g>
      <g opacity={dim ? 0.3 : 1} className="transition-opacity duration-300">{cells}</g>
      <text x={x + (n * cs) / 2} y={y - 14} textAnchor="middle" fontSize={10} fill={color === "var(--kfam-mmsg)" ? "var(--foreground)" : color.replace("--kfam-", "--kfam-text-").replace("--dtype-", "--dtype-text-")} className="font-mono font-semibold">
        {label}
      </text>
      <text x={x + (n * cs) / 2} y={y - 4} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
        {sub}
      </text>
    </g>
  );
}

export function DisentangledScoresFigure({ highlight }: { highlight: "all" | "c2c" | "c2p" | "p2c" }) {
  const hl = (k: string) => highlight !== "all" && highlight !== k;
  return (
    <Figure
      viewBox="0 0 470 190"
      title="scores = (C2C + C2P + P2C) / √(3·d)"
      caption="Schematic scores, not measured activations. Relative-position indices repeat along diagonals, but C2P/P2C values also depend on content and need not be equal along a diagonal. Each term contributes before scaling, masking and softmax."
    >
      <ScoreMatrix x={20} y={50} color="var(--kfam-attention)" mode="content" label="C2C" sub="Qc · Kcᵀ" dim={hl("c2c")} />
      <text x={110} y={90} fontSize={16} className="fill-muted-foreground">+</text>
      <ScoreMatrix x={130} y={50} color="var(--kfam-fusion)" mode="toeplitz" label="C2P" sub="Qc · Krᵀ" dim={hl("c2p")} />
      <text x={220} y={90} fontSize={16} className="fill-muted-foreground">+</text>
      <ScoreMatrix x={240} y={50} color="var(--kfam-mmsg)" mode="toeplitzT" label="P2C" sub="Qr · Kcᵀ" dim={hl("p2c")} />
      <text x={330} y={90} fontSize={16} className="fill-muted-foreground">=</text>
      <ScoreMatrix x={352} y={50} color="var(--dtype-f16)" mode="sum" label="scores" sub="softmax →" dim={false} />
      <text x={235} y={165} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
        content asks "who?", position asks "how far away?" — separately
      </text>
    </Figure>
  );
}

/** Log-bucket step chart: distance magnitude → bucket index (qualitative). */
export function LogBucketFigure() {
  const mid = 128;
  const maxPos = 512;
  const bucket = (d: number) => {
    if (d < mid) return d;
    return mid + Math.ceil((Math.log(d / mid) / Math.log((maxPos - 1) / mid)) * (mid - 1));
  };
  const W = 380;
  const H = 150;
  const pts: string[] = [];
  for (let d = 0; d <= 511; d += 4) {
    const x = 40 + (d / 511) * (W - 60);
    const y = H - 20 - (bucket(d) / 256) * (H - 50);
    pts.push(`${x.toFixed(1)},${y.toFixed(1)}`);
  }
  return (
    <Figure
      viewBox={`0 0 ${W} ${H + 40}`}
      title="relativePositionBucket: exact near, logarithmic far"
      caption="The curve shows bucket magnitude for nonnegative distance. Antfly adds the signed result to position_buckets=256 to index a 512-row relative table. Distances up to 128 map exactly; larger magnitudes use a ceiling-rounded logarithmic mapping."
    >
      <line x1={40} y1={H - 20} x2={W - 15} y2={H - 20} stroke="var(--border)" strokeWidth={1} />
      <line x1={40} y1={H - 20} x2={40} y2={20} stroke="var(--border)" strokeWidth={1} />
      <polyline points={pts.join(" ")} fill="none" stroke="var(--kfam-fusion)" strokeWidth={2} />
      <line
        x1={40 + (128 / 511) * (W - 60)}
        y1={H - 20}
        x2={40 + (128 / 511) * (W - 60)}
        y2={30}
        stroke="var(--muted-foreground)"
        strokeWidth={0.75}
        strokeDasharray="3 3"
      />
      <text x={40 + (128 / 511) * (W - 60) + 4} y={40} fontSize={8.5} className="fill-muted-foreground font-mono">
        |d| = 128: exact ends, log begins
      </text>
      <text x={W - 15} y={H - 6} textAnchor="end" fontSize={8.5} className="fill-muted-foreground font-mono">
        distance magnitude →
      </text>
      <text x={14} y={26} fontSize={8.5} className="fill-muted-foreground font-mono">
        |bucket|
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 4 — one fused kernel, forward and backward                       */
/* ------------------------------------------------------------------ */

export function FusedKernelPairFigure() {
  const eagerOps = [
    { id: "content", label: "matmul" }, { id: "c2p", label: "gather" },
    { id: "add-c2p", label: "add" }, { id: "p2c", label: "gather" },
    { id: "add-p2c", label: "add" }, { id: "scale", label: "scale" },
    { id: "mask", label: "mask" }, { id: "softmax", label: "softmax" },
    { id: "context", label: "matmul" },
  ];
  const bwd = ["bwd_scores", "bwd_dv", "bwd_dq_dk", "bwd_dqr_dkr"];
  return (
    <Figure
      viewBox="0 0 460 250"
      title="attention decomposition and fused implementation"
      caption="Left: a schematic decomposition of the attention equations, not a framework trace. Right: the fused Metal forward operation and its separate gradient kernels. The backward operation uses multiple dispatches."
    >
      <text x={110} y={22} textAnchor="middle" fontSize={10} className="fill-muted-foreground font-mono">
        decomposed equations
      </text>
      {eagerOps.map((op, i) => (
        <g key={op.id}>
          <rect
            x={40}
            y={34 + i * 22}
            width={140}
            height={17}
            rx={3}
            fill="none"
            stroke="var(--muted-foreground)"
            strokeWidth={0.75}
            opacity={0.6}
          />
          <text x={110} y={34 + i * 22 + 12} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
            {op.label}
          </text>
        </g>
      ))}
      <text x={340} y={22} textAnchor="middle" fontSize={10} className="fill-primary font-mono">
        Antfly Metal
      </text>
      <rect x={260} y={40} width={160} height={64} rx={5} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.5} strokeDasharray="6 2 2 2" />
      <text x={340} y={64} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        disentangled_relative
      </text>
      <text x={340} y={78} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        _attention_f32 (+_flash4)
      </text>
      <text x={340} y={94} textAnchor="middle" fontSize={7.5} className="fill-primary font-mono">
        ⟨c2c ⋄ c2p ⋄ p2c ⋄ softmax ⋄ context⟩
      </text>
      <text x={340} y={130} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
        custom backward kernels:
      </text>
      {bwd.map((k, i) => (
        <g key={k}>
          <rect x={272} y={140 + i * 24} width={136} height={19} rx={3} fill="color-mix(in oklch, var(--kfam-mmsg) 14%, transparent)" stroke="var(--kfam-mmsg)" strokeWidth={1} />
          <text x={340} y={140 + i * 24 + 13} textAnchor="middle" fontSize={8} className="fill-foreground font-mono">
            {k}
          </text>
        </g>
      ))}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 5 — span head pipeline                                           */
/* ------------------------------------------------------------------ */

export function SpanHeadFigure({ step }: { step: 0 | 1 | 2 }) {
  return (
    <Figure
      viewBox="0 0 440 240"
      title={
        step === 0
          ? "labels and words come from the same pass"
          : step === 1
            ? "every span: start ∥ end → [total_spans, 2H]"
            : "one matmul scores everything"
      }
      caption="Schematic span enumeration: the shapes show how start and end projections concatenate into one scored matrix, not measured tensor sizes for any particular input."
    >
      {/* encoder output strip */}
      <rect x={30} y={30} width={380} height={22} rx={4} fill="color-mix(in oklch, var(--dtype-f16) 18%, transparent)" stroke="var(--dtype-f16)" strokeWidth={1} />
      <text x={220} y={45} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
        encoder output [B·T, 768] — schema labels and text encoded together
      </text>
      {step === 0 && (
        <>
          <path d="M 100 52 v 30" stroke="var(--kfam-fusion)" strokeWidth={1.5} />
          <rect x={40} y={86} width={120} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} />
          <text x={100} y={103} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
            label embs [L, H]
          </text>
          <text x={100} y={128} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
            gathered where input_ids == [ENT]
          </text>
          <path d="M 320 52 v 30" stroke="var(--kfam-attention)" strokeWidth={1.5} />
          <rect x={255} y={86} width={130} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-attention) 16%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1.25} />
          <text x={320} y={103} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
            word reps [W, H]
          </text>
          <text x={320} y={128} textAnchor="middle" fontSize={7.5} className="fill-muted-foreground font-mono">
            first sub-token per word
          </text>
        </>
      )}
      {step === 1 && (
        <>
          <text x={90} y={90} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">project_start</text>
          <rect x={40} y={98} width={100} height={20} rx={3} fill="color-mix(in oklch, var(--kfam-matvec) 16%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1} />
          <text x={330} y={90} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">project_end</text>
          <rect x={280} y={98} width={100} height={20} rx={3} fill="color-mix(in oklch, var(--kfam-matvec) 16%, transparent)" stroke="var(--kfam-matvec)" strokeWidth={1} />
          <path d="M 140 108 L 200 140 M 280 108 L 220 140" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect x={140} y={144} width={140} height={24} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} strokeDasharray="6 2 2 2" />
          <text x={210} y={160} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
            concat → [S, 2H] → ReLU
          </text>
      <text x={210} y={190} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
            S = every span up to max_width (12 by default) · out_project → span_rep [S, H]
          </text>
        </>
      )}
      {step === 2 && (
        <>
          <rect x={70} y={90} width={110} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-attention) 16%, transparent)" stroke="var(--kfam-attention)" strokeWidth={1.25} />
          <text x={125} y={107} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">span_rep [S, H]</text>
          <rect x={260} y={90} width={110} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-fusion) 16%, transparent)" stroke="var(--kfam-fusion)" strokeWidth={1.25} />
          <text x={315} y={107} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">label_proj [L, H]</text>
          <path d="M 125 116 L 200 145 M 315 116 L 240 145" stroke="var(--muted-foreground)" strokeWidth={1} />
          <rect x={150} y={150} width={140} height={26} rx={4} fill="color-mix(in oklch, var(--kfam-sampling) 18%, transparent)" stroke="var(--kfam-sampling)" strokeWidth={1.5} />
          <text x={220} y={167} textAnchor="middle" fontSize={8.5} className="fill-foreground font-mono">
            span_rep @ label_projᵀ
          </text>
          <text x={220} y={198} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            [S, L] → sigmoid → threshold → overlap filtering (flat NER)
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 6 — spine strip (which shared stages GLiNER2 uses)               */
/* ------------------------------------------------------------------ */

export function GlinerSpineFigure() {
  const stages = [
    { label: "HTTP", used: true },
    { label: "session", used: true },
    { label: "tokenizer", used: true },
    { label: "graph", used: true },
    { label: "frames", used: true },
    { label: "kernels", used: true },
    { label: "KV", used: false },
    { label: "sample", used: false },
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      <div className="grid grid-cols-4 gap-1.5 xl:grid-cols-8">
        {stages.map((s) => (
          <div
            key={s.label}
            className={cn(
              "min-w-0 rounded-md border px-1 py-2 text-center font-mono text-[10px]",
              s.used ? "border-primary/60 text-foreground" : "border-dashed text-muted-foreground line-through",
            )}
          >
            {s.label}
          </div>
        ))}
      </div>
      <p className="text-center font-mono text-[11px] text-muted-foreground">
        shared server and backend execution · no autoregressive cache or sampler
      </p>
    </div>
  );
}
