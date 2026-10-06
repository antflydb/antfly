"use client";

import { Button, cn } from "@antfly/design-system";
import { Pause, Play } from "lucide-react";
import { useEffect, useMemo, useState } from "react";
import { Figure } from "@/components/viz/glyphs";
import type { ModelSpec } from "@/lib/schema";

/* ------------------------------------------------------------------ */
/* Ch 0 — cold open: one decode step replayed over the spine           */
/* ------------------------------------------------------------------ */

const REPLAY_STAGES = [
  { id: "embed", label: "embed", color: "var(--dtype-f16)" },
  { id: "ple", label: "PLE", color: "var(--kfam-fusion)" },
  { id: "layers", label: "42 layers", color: "var(--kfam-attention)" },
  { id: "head", label: "LM head", color: "var(--kfam-sampling)" },
  { id: "sample", label: "sample", color: "var(--kfam-sampling)" },
] as const;

const REPLAY_TOKENS = ["The", " ant", "fly", " runs", " on", " Metal", "."];

export function ColdOpenFigure({ tokS, layers }: { tokS: string; layers: number }) {
  const [tick, setTick] = useState(0);
  const [playing, setPlaying] = useState(false);
  useEffect(() => {
    if (!playing) return;
    const t = setInterval(() => setTick((v) => v + 1), 320);
    return () => clearInterval(t);
  }, [playing]);

  const stageIdx = tick % (REPLAY_STAGES.length + 2);
  const tokenCount = Math.min(
    REPLAY_TOKENS.length,
    Math.floor(tick / (REPLAY_STAGES.length + 2)) + 1
  );

  return (
    <div className="flex h-full flex-col justify-center gap-6">
      <div className="flex items-center justify-between">
        <span className="font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
          one text decode step · schematic
        </span>
        <Button
          variant="ghost"
          size="icon"
          className="size-6"
          aria-label={playing ? "Pause decode illustration" : "Play decode illustration"}
          onClick={() => setPlaying((p) => !p)}
        >
          {playing ? <Pause className="size-3" /> : <Play className="size-3" />}
        </Button>
      </div>

      <div className="flex items-center gap-1.5">
        {REPLAY_STAGES.map((s, i) => (
          <div key={s.id} className="flex flex-1 items-center gap-1.5">
            <div
              className={cn(
                "flex-1 rounded-md border px-1 py-2 text-center font-mono text-[10px] transition-all duration-200",
                i === stageIdx
                  ? "scale-105 border-transparent text-foreground"
                  : "text-muted-foreground"
              )}
              style={{
                background:
                  i === stageIdx ? `color-mix(in oklch, ${s.color} 18%, transparent)` : undefined,
              }}
            >
              {s.id === "layers" ? `${layers} layers / KV` : s.label}
            </div>
            {i < REPLAY_STAGES.length - 1 && <span className="text-muted-foreground/50">→</span>}
          </div>
        ))}
      </div>

      {/* mini frame fill */}
      <div className="h-2 overflow-hidden rounded-full bg-muted">
        <div
          className="h-full rounded-full bg-primary transition-all duration-300"
          style={{ width: `${(stageIdx / (REPLAY_STAGES.length + 1)) * 100}%` }}
        />
      </div>

      <div className="rounded-lg border bg-muted/30 p-4">
        <div className="min-h-10 text-sm">
          {REPLAY_TOKENS.slice(0, tokenCount).map((t, i) => (
            <span key={t} className={cn(i === tokenCount - 1 && "rounded bg-primary/20")}>
              {t}
            </span>
          ))}
          <span className="ml-0.5 inline-block h-4 w-1.5 animate-pulse bg-primary align-middle" />
        </div>
        <div className="mt-2 font-mono text-[11px] text-muted-foreground">{tokS}</div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 1 — layer stack small-multiples                                  */
/* ------------------------------------------------------------------ */

type StackEmphasis = "attention" | "kv" | "headdim" | "none";

export function LayerStackFigure({ spec, emphasis }: { spec: ModelSpec; emphasis: StackEmphasis }) {
  const layerStage = spec.stages.find((s) => s.id === "layers");
  const repeat = layerStage?.repeat;
  const layers = useMemo(() => {
    const count = repeat?.count ?? 0;
    const byIdx = new Map<number, string>();
    for (const variant of repeat?.variants ?? []) {
      for (const idx of variant.layerIdxs) {
        if (idx < count) byIdx.set(idx, variant.tag);
      }
    }
    return Array.from({ length: count }, (_, i) => {
      // Variants in the curated file are E4B-indexed; recompute for the
      // actual count so E2B renders correctly too.
      const global = (i + 1) % Number(spec.stats.slidingPattern) === 0;
      const kvOwners = Number(spec.stats.kvOwners ?? count);
      const shared = i >= kvOwners;
      return { idx: i, global, shared, tag: byIdx.get(i) };
    });
  }, [repeat, spec.stats.kvOwners, spec.stats.slidingPattern]);

  const rowH = 9;
  const H = layers.length * rowH + 20;
  const kvOwners = Number(spec.stats.kvOwners ?? layers.length);

  return (
    <Figure
      viewBox={`0 0 400 ${H}`}
      title={`${spec.displayName}: ${layers.length} layers`}
      caption={
        emphasis === "attention"
          ? `Shuttered rows slide a window; every ${spec.stats.slidingPattern}th layer attends globally (${Number(spec.stats.slidingPattern) - 1}:1 iSWA).`
          : emphasis === "kv"
            ? `The first ${kvOwners} layers compute K/V (left column). Tail layers read same-type donors; physical storage depends on the backend.`
            : emphasis === "headdim"
              ? "Global layers run head_dim 512; sliding layers run 256 — twice the per-head width on global attention."
              : undefined
      }
    >
      {layers.map((l) => {
        const y = 10 + l.idx * rowH;
        const isEmph =
          emphasis === "attention"
            ? l.global
            : emphasis === "kv"
              ? !l.shared
              : emphasis === "headdim"
                ? l.global
                : false;
        const color = l.global ? "var(--kfam-attention)" : "var(--dtype-f16)";
        return (
          <g key={l.idx}>
            {/* KV ownership column */}
            <rect
              x={4}
              y={y + 1}
              width={10}
              height={rowH - 3}
              rx={1.5}
              fill={l.shared ? "none" : "var(--kfam-kv)"}
              stroke="var(--kfam-kv)"
              strokeWidth={0.75}
              strokeDasharray={l.shared ? "2 2" : undefined}
              opacity={emphasis === "kv" ? 1 : 0.35}
            />
            {/* layer bar */}
            <rect
              x={20}
              y={y}
              width={l.global ? 320 : 240}
              height={rowH - 2}
              rx={2}
              fill={color}
              opacity={l.global ? 0.75 : 0.35}
              stroke={isEmph ? "var(--primary)" : "none"}
              strokeWidth={1.25}
            />
            {/* SWA shutter glyph */}
            {!l.global && (
              <g stroke={color} strokeWidth={0.75} opacity={0.9}>
                <line x1={26} y1={y + 1.5} x2={26} y2={y + rowH - 3.5} />
                <line x1={30} y1={y + 1} x2={30} y2={y + rowH - 3} />
              </g>
            )}
            {(l.global || (l.shared && emphasis === "kv")) && (
              <text
                x={346}
                y={y + rowH / 2}
                fontSize={6}
                textAnchor="start"
                dominantBaseline="central"
                className="fill-foreground font-mono"
              >
                {emphasis === "headdim" ? "hd 512" : l.global ? "global" : ""}
                {l.shared && emphasis === "kv" ? (l.global ? " · shared" : "shared") : ""}
              </text>
            )}
          </g>
        );
      })}
    </Figure>
  );
}

/* ------------------------------------------------------------------ */
/* Ch 7 — graph → frame morph                                          */
/* ------------------------------------------------------------------ */

export function GraphToFrameFigure({ step }: { step: 0 | 1 | 2 }) {
  const ops = ["rms_norm", "QKV", "rope⋄norm", "attention", "o_proj", "gate+up", "down"];
  const scopes = [
    { label: "attn setup", ops: [0, 1, 2] },
    { label: "attention", ops: [3, 4] },
    { label: "FFN", ops: [5, 6] },
  ];
  return (
    <Figure
      viewBox="0 0 480 240"
      caption="Conceptual grouping with historical scenario counters. The Q8_0 and Q4_0 records use different configurations; the drawing is not a captured plan."
      title={
        step === 0
          ? "the op graph"
          : step === 1
            ? "…grouped into encoder scopes"
            : "…barriers only at real hazards"
      }
    >
      {ops.map((op, i) => {
        const x = 12 + i * 66;
        const inScope = scopes.findIndex((s) => s.ops.includes(i));
        const y = step === 0 ? 60 + (i % 2) * 60 : 110;
        return (
          <g key={op} className="transition-all duration-500">
            <rect
              x={x}
              y={y}
              width={58}
              height={30}
              rx={4}
              fill="color-mix(in oklch, var(--kfam-matvec) 16%, transparent)"
              stroke={
                step >= 1
                  ? ["var(--kfam-attention)", "var(--kfam-mmsg)", "var(--kfam-fusion)"][inScope]
                  : "var(--kfam-matvec)"
              }
              strokeWidth={1.25}
            />
            <text
              x={x + 29}
              y={y + 15}
              textAnchor="middle"
              dominantBaseline="central"
              fontSize={8.5}
              className="fill-foreground font-mono"
            >
              {op}
            </text>
            {i < ops.length - 1 && step === 0 && (
              <line
                x1={x + 58}
                y1={y + 15}
                x2={x + 66}
                y2={60 + ((i + 1) % 2) * 60 + 15}
                stroke="var(--muted-foreground)"
                strokeWidth={1}
              />
            )}
          </g>
        );
      })}
      {step >= 1 &&
        scopes.map((s) => {
          const first = 12 + s.ops[0] * 66;
          const last = 12 + s.ops[s.ops.length - 1] * 66 + 58;
          return (
            <g key={s.label}>
              <path
                d={`M ${first} 100 v -6 h ${last - first} v 6`}
                fill="none"
                stroke="var(--muted-foreground)"
                strokeWidth={1}
              />
              <text
                x={(first + last) / 2}
                y={86}
                textAnchor="middle"
                fontSize={8}
                className="fill-muted-foreground font-mono"
              >
                {s.label}
              </text>
            </g>
          );
        })}
      {step === 1 && (
        <>
          {[210, 342, 408].map((x) => (
            <line
              key={x}
              x1={x}
              y1={100}
              x2={x}
              y2={150}
              stroke="var(--destructive)"
              strokeWidth={2}
            />
          ))}
          <text
            x={240}
            y={190}
            textAnchor="middle"
            fontSize={9}
            className="fill-destructive font-mono"
          >
            historical Q8_0 anchor: 41 encoders · 422 barriers
          </text>
        </>
      )}
      {step === 2 && (
        <text
          x={240}
          y={190}
          textAnchor="middle"
          fontSize={9}
          className="fill-foreground font-mono"
        >
          recorded Q4_0:{" "}
          <tspan className="fill-primary">1 encoder · 143 planned scopes · 0 barriers</tspan>
        </text>
      )}
    </Figure>
  );
}
