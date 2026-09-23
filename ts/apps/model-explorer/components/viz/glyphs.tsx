"use client";

import { cn } from "@antfly/design-system";
import { type ReactNode, useId } from "react";
import { dtypeColorVar, dtypeTextColorVar } from "@/components/primitives/chips";

/**
 * The stable visual vocabulary (see /legend): once a shape means something,
 * it never means anything else.
 *
 *  activation  rounded rect      weight     rect + thick bottom border
 *  matmul      wide rect         norm       thin bar
 *  attention   hexagon (+shutter for SWA)   elementwise  circle
 *  embedding   trapezoid         routing    fork glyph
 *  sampling    die glyph         kv block   grid square
 *  fusion      zipper border
 */

export interface GlyphProps {
  x: number;
  y: number;
  w?: number;
  h?: number;
  label?: string;
  sublabel?: string;
  dtype?: string;
  /** Solid fill = Metal; dashed outline = native CPU. */
  backend?: "metal" | "native";
  highlight?: boolean;
  dim?: boolean;
  onClick?: () => void;
}

const base = (p: GlyphProps) =>
  cn(
    "transition-opacity",
    p.onClick &&
      "cursor-pointer focus-visible:outline-2 focus-visible:outline-offset-4 focus-visible:outline-primary"
  );

/** Preserve plain SVG groups for static glyphs and expose real actions to keyboards. */
function GlyphGroup({ glyph, children }: { glyph: GlyphProps; children: ReactNode }) {
  if (!glyph.onClick) return <g className={base(glyph)}>{children}</g>;
  return (
    <g
      className={base(glyph)}
      role="button"
      tabIndex={0}
      aria-label={glyph.label ? `Inspect ${glyph.label}` : "Inspect diagram node"}
      onClick={glyph.onClick}
      onKeyDown={(event) => {
        if (event.key === "Enter" || event.key === " ") {
          event.preventDefault();
          glyph.onClick?.();
        }
      }}
    >
      {children}
    </g>
  );
}

function GlyphLabel({
  x,
  y,
  w,
  label,
  sublabel,
}: {
  x: number;
  y: number;
  w: number;
  label?: string;
  sublabel?: string;
}) {
  if (!label) return null;
  return (
    <>
      <text
        x={x + w / 2}
        y={y - (sublabel ? 6 : 0)}
        textAnchor="middle"
        className="fill-foreground font-mono"
        fontSize={10}
        dy={-4}
      >
        {label}
      </text>
      {sublabel && (
        <text
          x={x + w / 2}
          y={y + 4}
          textAnchor="middle"
          className="fill-muted-foreground font-mono"
          fontSize={8}
          dy={-4}
        >
          {sublabel}
        </text>
      )}
    </>
  );
}

function strokeProps(p: GlyphProps) {
  const color = dtypeColorVar(p.dtype);
  return {
    stroke: p.highlight ? "var(--primary)" : color,
    strokeWidth: p.highlight ? 2 : 1.25,
    strokeDasharray: p.backend === "native" ? "4 3" : undefined,
    fill: `color-mix(in oklch, ${color} 14%, transparent)`,
    // De-emphasize geometry only; labels must remain readable.
    opacity: p.dim ? 0.45 : 1,
  };
}

/** Activation tensor: rounded rect, width ∝ log(dim). */
export function ActivationGlyph(p: GlyphProps) {
  const { x, y, w = 80, h = 28 } = p;
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <rect x={x} y={y} width={w} height={h} rx={8} {...strokeProps(p)} />
    </GlyphGroup>
  );
}

/** Weight tensor: rect with a thick bottom border ("sits on disk"). */
export function WeightGlyph(p: GlyphProps & { quant?: string }) {
  const { x, y, w = 80, h = 28 } = p;
  const s = strokeProps({ ...p, dtype: p.quant ?? p.dtype });
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <rect x={x} y={y} width={w} height={h} rx={2} {...s} />
      <line
        x1={x}
        y1={y + h}
        x2={x + w}
        y2={y + h}
        stroke={s.stroke}
        strokeWidth={4}
        strokeLinecap="round"
        opacity={s.opacity}
      />
      {p.quant && (
        <text
          x={x + w - 4}
          y={y + h - 6}
          textAnchor="end"
          fontSize={8}
          className="font-mono"
          fill={dtypeTextColorVar(p.quant)}
        >
          {p.quant.toUpperCase()}
        </text>
      )}
    </GlyphGroup>
  );
}

/** Matmul / linear: wide rectangle. Fused ops get the zipper border. */
export function MatmulGlyph(p: GlyphProps & { fused?: string[] }) {
  const { x, y, w = 110, h = 32 } = p;
  const s = strokeProps(p);
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <rect
        x={x}
        y={y}
        width={w}
        height={h}
        rx={3}
        {...s}
        strokeDasharray={p.fused?.length ? "6 2 2 2" : s.strokeDasharray}
      />
      {p.fused && p.fused.length > 0 && (
        <text
          x={x + w / 2}
          y={y + h / 2}
          textAnchor="middle"
          dominantBaseline="central"
          fontSize={9}
          className="font-mono fill-primary"
        >
          ⟨{p.fused.join(" ⋄ ")}⟩
        </text>
      )}
    </GlyphGroup>
  );
}

/** Attention: hexagon; `shutter` adds the sliding-window glyph on the left edge. */
export function AttentionGlyph(p: GlyphProps & { shutter?: boolean }) {
  const { x, y, w = 90, h = 40 } = p;
  const s = strokeProps(p);
  const inset = h / 2;
  const points = [
    [x + inset, y],
    [x + w - inset, y],
    [x + w, y + h / 2],
    [x + w - inset, y + h],
    [x + inset, y + h],
    [x, y + h / 2],
  ]
    .map((pt) => pt.join(","))
    .join(" ");
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <polygon points={points} {...s} />
      {p.shutter && (
        <g stroke={s.stroke} strokeWidth={1}>
          <line x1={x + 10} y1={y + 8} x2={x + 10} y2={y + h - 8} />
          <line x1={x + 15} y1={y + 6} x2={x + 15} y2={y + h - 6} />
          <line x1={x + 20} y1={y + 4} x2={x + 20} y2={y + h - 4} />
        </g>
      )}
    </GlyphGroup>
  );
}

/** Norm: deliberately small thin bar. */
export function NormGlyph(p: GlyphProps) {
  const { x, y, w = 70, h = 8 } = p;
  const s = strokeProps(p);
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} />
      <rect x={x} y={y} width={w} height={h} rx={4} {...s} />
    </GlyphGroup>
  );
}

/** Elementwise / activation fn: small circle. */
export function ElementwiseGlyph(p: GlyphProps) {
  const { x, y, w = 24 } = p;
  const s = strokeProps(p);
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} />
      <circle cx={x + w / 2} cy={y + w / 2} r={w / 2} {...s} />
    </GlyphGroup>
  );
}

/** Embedding lookup: trapezoid (wide -> narrow). */
export function EmbeddingGlyph(p: GlyphProps) {
  const { x, y, w = 90, h = 34 } = p;
  const s = strokeProps(p);
  const points = [
    [x, y],
    [x + w, y],
    [x + w - 18, y + h],
    [x + 18, y + h],
  ]
    .map((pt) => pt.join(","))
    .join(" ");
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <polygon points={points} {...s} />
    </GlyphGroup>
  );
}

/** Routing (MoE / MTP accept): 1→N fork. */
export function ForkGlyph(p: GlyphProps & { branches?: number }) {
  const { x, y, w = 70, h = 40, branches = 3 } = p;
  const s = strokeProps(p);
  const lines = [];
  for (let i = 0; i < branches; i++) {
    const ty = y + (h * (i + 0.5)) / branches;
    lines.push(
      <line
        key={i}
        x1={x + w * 0.35}
        y1={y + h / 2}
        x2={x + w}
        y2={ty}
        stroke={s.stroke}
        strokeWidth={1.5}
      />
    );
  }
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} sublabel={p.sublabel} />
      <line
        x1={x}
        y1={y + h / 2}
        x2={x + w * 0.35}
        y2={y + h / 2}
        stroke={s.stroke}
        strokeWidth={2}
      />
      {lines}
    </GlyphGroup>
  );
}

/** Sampling: die glyph. */
export function SamplerGlyph(p: GlyphProps) {
  const { x, y, w = 34 } = p;
  const s = strokeProps(p);
  const pip = (px: number, py: number) => (
    <circle key={`${px}-${py}`} cx={px} cy={py} r={2} fill={s.stroke} />
  );
  return (
    <GlyphGroup glyph={p}>
      <GlyphLabel x={x} y={y} w={w} label={p.label} />
      <rect x={x} y={y} width={w} height={w} rx={6} {...s} />
      {pip(x + w * 0.28, y + w * 0.28)}
      {pip(x + w * 0.72, y + w * 0.28)}
      {pip(x + w * 0.5, y + w * 0.5)}
      {pip(x + w * 0.28, y + w * 0.72)}
      {pip(x + w * 0.72, y + w * 0.72)}
    </GlyphGroup>
  );
}

/** KV block: grid square. filled | hatched (evicted) | split (shared). */
export function KvBlockGlyph({
  x,
  y,
  size = 14,
  state,
  color = "var(--kfam-kv)",
}: {
  x: number;
  y: number;
  size?: number;
  state: "empty" | "filled" | "evicted" | "shared";
  color?: string;
}) {
  if (state === "empty") {
    return (
      <rect
        x={x}
        y={y}
        width={size}
        height={size}
        rx={2}
        fill="none"
        stroke="var(--border)"
        strokeWidth={1}
      />
    );
  }
  if (state === "evicted") {
    return (
      <g>
        <rect
          x={x}
          y={y}
          width={size}
          height={size}
          rx={2}
          fill="none"
          stroke={color}
          strokeWidth={1}
          opacity={0.5}
        />
        <line
          x1={x + 2}
          y1={y + size - 2}
          x2={x + size - 2}
          y2={y + 2}
          stroke={color}
          strokeWidth={1}
          opacity={0.5}
        />
        <line
          x1={x + 2}
          y1={y + size / 2}
          x2={x + size / 2}
          y2={y + 2}
          stroke={color}
          strokeWidth={1}
          opacity={0.5}
        />
      </g>
    );
  }
  if (state === "shared") {
    return (
      <g>
        <rect
          x={x}
          y={y}
          width={size}
          height={size}
          rx={2}
          fill="none"
          stroke={color}
          strokeWidth={1}
        />
        <path
          d={`M ${x} ${y + size} L ${x + size} ${y} L ${x + size} ${y + size} Z`}
          fill={color}
          opacity={0.55}
        />
      </g>
    );
  }
  return <rect x={x} y={y} width={size} height={size} rx={2} fill={color} opacity={0.75} />;
}

/** Flow arrow between glyphs. */
export function FlowArrow({
  x1,
  y1,
  x2,
  y2,
  dashed,
  ghost,
  label,
}: {
  x1: number;
  y1: number;
  x2: number;
  y2: number;
  dashed?: boolean;
  /** Ghost = a crossed-out path not taken. */
  ghost?: boolean;
  label?: string;
}) {
  const markerId = useId();
  const midX = (x1 + x2) / 2;
  const midY = (y1 + y2) / 2;
  return (
    <g>
      <defs>
        <marker id={markerId} markerWidth={7} markerHeight={7} refX={6} refY={3.5} orient="auto">
          <polygon points="0 0, 7 3.5, 0 7" className="fill-muted-foreground" />
        </marker>
      </defs>
      <line
        x1={x1}
        y1={y1}
        x2={x2}
        y2={y2}
        className="stroke-muted-foreground"
        strokeWidth={1.25}
        strokeDasharray={dashed || ghost ? "5 4" : undefined}
        markerEnd={`url(#${markerId})`}
        opacity={ghost ? 0.45 : 1}
      />
      {ghost && (
        <g className="stroke-destructive" strokeWidth={1.5}>
          <line x1={midX - 5} y1={midY - 5} x2={midX + 5} y2={midY + 5} />
          <line x1={midX - 5} y1={midY + 5} x2={midX + 5} y2={midY - 5} />
        </g>
      )}
      {label && (
        <text
          x={midX}
          y={midY - 6}
          textAnchor="middle"
          fontSize={9}
          className="fill-muted-foreground font-mono"
        >
          {label}
        </text>
      )}
    </g>
  );
}

/** Responsive SVG stage for bespoke figures. */
export function Figure({
  viewBox,
  children,
  title,
  caption,
  className,
}: {
  viewBox: string;
  children: ReactNode;
  title?: string;
  caption?: ReactNode;
  className?: string;
}) {
  const titleId = useId();
  return (
    <figure className={cn("flex h-full flex-col", className)}>
      {title && (
        <div className="mb-2 font-mono text-[11px] uppercase tracking-wider text-muted-foreground">
          {title}
        </div>
      )}
      <svg
        viewBox={viewBox}
        className="min-h-0 w-full flex-1"
        preserveAspectRatio="xMidYMid meet"
        role="group"
        aria-labelledby={titleId}
      >
        <title id={titleId}>{title ?? "Model mechanism diagram"}</title>
        {children}
      </svg>
      {caption && <figcaption className="mt-2 text-xs text-muted-foreground">{caption}</figcaption>}
    </figure>
  );
}
