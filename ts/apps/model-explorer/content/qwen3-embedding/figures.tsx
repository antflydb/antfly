"use client";

import { Figure } from "@/components/viz/glyphs";

/* ------------------------------------------------------------------ */
/* Ch 1 — a decoder whose output bends away from the LM head           */
/* ------------------------------------------------------------------ */

export function VectorNotTokenFigure() {
  return (
    <Figure
      viewBox="0 0 420 230"
      title="same stack, different exit"
      caption="The 28 decoder layers are stock Qwen3. What changes is the exit ramp: instead of the vocabulary matvec and a sampler, the last hidden state leaves sideways as a 1,024-dim vector."
    >
      {/* compressed layer stack */}
      {Array.from({ length: 9 }, (_, i) => i).map((i) => (
        <rect
          key={i}
          x={60}
          y={30 + i * 14}
          width={180}
          height={10}
          rx={2}
          fill="var(--kfam-attention)"
          opacity={0.28 + (i % 3) * 0.08}
        />
      ))}
      <text x={150} y={22} textAnchor="middle" fontSize={9} className="fill-muted-foreground font-mono">
        28 × Qwen3 decoder layer
      </text>
      {/* ghosted LM head path */}
      <line x1={150} y1={158} x2={150} y2={186} stroke="var(--muted-foreground)" strokeWidth={1.25} strokeDasharray="5 4" opacity={0.4} />
      <g>
        <rect x={95} y={190} width={110} height={24} rx={3} fill="none" stroke="var(--kfam-sampling)" strokeWidth={1} strokeDasharray="4 3" opacity={0.35} />
        <text x={150} y={206} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
          LM head · sampler
        </text>
        <line x1={120} y1={186} x2={180} y2={218} stroke="var(--destructive)" strokeWidth={1.25} opacity={0.35} />
        <line x1={120} y1={218} x2={180} y2={186} stroke="var(--destructive)" strokeWidth={1.25} opacity={0.35} />
      </g>
      {/* the bend */}
      <path d="M 240 152 C 290 152, 300 120, 330 110" fill="none" stroke="var(--primary)" strokeWidth={2} />
      <rect x={318} y={78} width={84} height={56} rx={6} fill="color-mix(in oklch, var(--dtype-f32) 14%, transparent)" stroke="var(--dtype-f32)" strokeWidth={1.5} />
      <text x={360} y={100} textAnchor="middle" fontSize={9} className="fill-foreground font-mono">
        [1024]
      </text>
      <text x={360} y={116} textAnchor="middle" fontSize={8} className="fill-muted-foreground font-mono">
        ‖x‖ = 1
      </text>
      <text x={360} y={150} textAnchor="middle" fontSize={8.5} className="fill-primary font-mono">
        the embedding
      </text>
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 2 — BPE vs SentencePiece (illustrative)                          */
/* ------------------------------------------------------------------ */

const BPE_PIECES = ["ant", "fly", " runs", " on", " Metal"];
const SP_PIECES = ["▁ant", "fly", "▁runs", "▁on", "▁Metal"];

export function TokenizerContrastFigure() {
  const row = (label: string, pieces: string[], color: string, note: string) => (
    <div>
      <div className="mb-1.5 flex items-baseline justify-between">
        <span className="font-mono text-[11px] uppercase tracking-wider text-muted-foreground">{label}</span>
        <span className="font-mono text-[10px] text-muted-foreground">{note}</span>
      </div>
      <div className="flex flex-wrap gap-1">
        {pieces.map((p) => (
          <span
            key={p}
            className="rounded border px-1.5 py-0.5 font-mono text-xs"
            style={{ borderColor: color, color: color.replace("--kfam-", "--kfam-text-") }}
          >
            {p.replace(/ /g, "␣")}
          </span>
        ))}
      </div>
    </div>
  );
  return (
    <div className="flex h-full flex-col justify-center gap-6">
      <div className="rounded-lg border bg-muted/30 p-3 text-center font-mono text-sm">"antfly runs on Metal"</div>
      {row("Qwen3 · byte-level BPE", BPE_PIECES, "var(--kfam-attention)", "byte-level vocabulary coverage")}
      {row("Gemma · SentencePiece", SP_PIECES, "var(--kfam-fusion)", "▁ represents whitespace in pieces")}
      <p className="text-center font-mono text-[10px] text-muted-foreground">
        illustrative split — the point is the two families, not these exact pieces
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 3 — last-token pooling + unit sphere                             */
/* ------------------------------------------------------------------ */

export function LastTokenPoolFigure({ step }: { step: 0 | 1 }) {
  const rows = 9;
  return (
    <Figure
      viewBox="0 0 420 220"
      title={step === 0 ? "T hidden states in, one comes out" : "…then scaled onto the unit sphere"}
      caption={
        step === 0
          ? "qwen3.pooling_type = 3 declares last-token pooling. Select the final non-padding hidden row in each batch item. Antfly ensures a trailing EOS, including when configured truncation fills the sequence buffer."
          : "L2 normalization puts every embedding on the unit sphere, so cosine similarity downstream is a plain dot product."
      }
    >
      {Array.from({ length: rows }, (_, i) => i).map((i) => {
        const last = i === rows - 1;
        return (
          <g key={i}>
            <rect
              x={40}
              y={24 + i * 19}
              width={170}
              height={14}
              rx={3}
              fill="var(--dtype-f32)"
              opacity={last ? 0.85 : Math.max(0.06, 0.35 - i * 0.03)}
              stroke={last ? "var(--primary)" : "none"}
              strokeWidth={1.5}
            />
            {last && (
              <text x={218} y={35 + i * 19} fontSize={8.5} className="fill-primary font-mono">
                ← ⟨EOS⟩: saw everything
              </text>
            )}
          </g>
        );
      })}
      {step === 1 && (
        <>
          <circle cx={330} cy={110} r={52} fill="none" stroke="var(--muted-foreground)" strokeWidth={1} strokeDasharray="3 3" />
          <line x1={330} y1={110} x2={366} y2={73} stroke="var(--primary)" strokeWidth={2} />
          <circle cx={366} cy={73} r={3.5} fill="var(--primary)" />
          <line x1={330} y1={110} x2={352} y2={88} stroke="var(--muted-foreground)" strokeWidth={1.25} strokeDasharray="4 3" />
          <text x={330} y={185} textAnchor="middle" fontSize={8.5} className="fill-muted-foreground font-mono">
            x / ‖x‖ → cos(a,b) = a·b
          </text>
        </>
      )}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 4 — cosine vs reference across context length                    */
/* ------------------------------------------------------------------ */

export function CosineFidelityFigure() {
  const checks = [
    ["Artifact", "same model revision, tensor format and weights"],
    ["Input", "same task prefix, token IDs, EOS and truncation"],
    ["Vector", "last active row, dimensions and L2 normalization"],
    ["Evidence", "cosine, retrieval quality and batch equivalence"],
  ];
  return (
    <div className="flex h-full flex-col justify-center gap-4">
      <p className="text-sm font-semibold">What makes an embedding comparison valid?</p>
      {checks.map(([name, detail]) => (
        <div key={name} className="rounded-lg border p-3">
          <p className="font-mono text-xs text-primary">{name}</p>
          <p className="mt-1 text-sm text-muted-foreground">{detail}</p>
        </div>
      ))}
      <p className="text-xs text-muted-foreground">
        Validation checklist, not measured results. Follow the linked baseline for dated reports,
        exact artifacts and timing boundaries.
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 5 — three stacked perf wins                                      */
/* ------------------------------------------------------------------ */

const WINS = [
  {
    label: "batched FFN",
    detail: "token rows share matrix operations; fused gate/up can reuse tiles",
    color: "var(--kfam-matvec)",
  },
  {
    label: "simdgroup flash attention",
    detail: "sg_q16 tiles, online softmax, no [T,T] score matrix",
    color: "var(--kfam-attention)",
  },
  {
    label: "f16-KV direct load",
    detail: "attention reads f16 K/V straight — half the KV bytes",
    color: "var(--kfam-kv)",
  },
];

export function BatchingWinsFigure() {
  return (
    <div className="flex h-full flex-col justify-center gap-5">
      <div className="font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
        mechanisms that improve prefill efficiency
      </div>
      <div className="grid gap-4">
        {WINS.map((win) => (
          <div key={win.label}>
            <div className="mb-1 flex items-baseline justify-between">
              <span className="font-mono text-xs text-foreground">{win.label}</span>
            </div>
            <div className="h-0.5 rounded-full" style={{ background: win.color }} />
            <div className="mt-1 text-[11px] text-muted-foreground">{win.detail}</div>
          </div>
        ))}
      </div>
      <div className="rounded-lg border bg-muted/30 p-3">
        <p className="text-sm text-muted-foreground">
          Conceptual mechanisms, not a measured speedup breakdown. Dispatch depends on shape,
          precision, hardware and runtime flags; endpoint timing includes more than the encoder graph.
        </p>
      </div>
    </div>
  );
}
