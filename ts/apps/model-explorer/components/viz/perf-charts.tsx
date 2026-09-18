"use client";

import { cn, Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "@antfly/design-system";
import type { BytesBreakdownEntry, GapSegment, JourneyEntry, PerfSample } from "@/lib/schema";

/* All perf charts are plain SVG/CSS — the data sets are tiny and the layouts
   (annotated bars, waterfall) fight recharts more than they use it. */

export function ComparisonBars({
  samples,
  ceiling,
  className,
}: {
  samples: PerfSample[];
  /** Roofline ceiling in the same metric. */
  ceiling?: { value: number; label: string };
  className?: string;
}) {
  const max = Math.max(ceiling?.value ?? 0, ...samples.map((s) => s.value)) * 1.08;
  return (
    <TooltipProvider>
      <div className={cn("space-y-2", className)}>
        {samples.map((s) => {
          const isAntfly = s.system.toLowerCase().includes("antfly");
          return (
            <Tooltip key={`${s.system}-${s.context ?? ""}`}>
              <TooltipTrigger asChild>
                <button type="button" className="grid w-full text-left grid-cols-[minmax(0,1fr)_minmax(0,2fr)] items-center gap-x-3 gap-y-1 sm:grid-cols-[10rem_minmax(0,1fr)_6rem]">
                  <span className={cn("min-w-0 break-words text-right text-xs", isAntfly ? "font-semibold" : "text-muted-foreground")}>
                    {s.system}
                    {s.caveat && <span className="text-primary">*</span>}
                  </span>
                  <div className="relative h-6 flex-1 overflow-visible rounded-sm bg-muted/40">
                    <div
                      className="h-full rounded-sm"
                      style={{
                        width: `${(s.value / max) * 100}%`,
                        background: isAntfly ? "var(--primary)" : "var(--muted-foreground)",
                        opacity: isAntfly ? 0.9 : 0.45,
                      }}
                    />
                    <span className="absolute inset-y-0 flex items-center pl-2 font-mono text-[11px] font-medium">
                      {s.value} tok/s
                    </span>
                    {ceiling && (
                      <div
                        className="absolute inset-y-[-4px] border-l-2 border-dashed border-destructive/70"
                        style={{ left: `${(ceiling.value / max) * 100}%` }}
                      />
                    )}
                  </div>
                  <span className="col-span-2 font-mono text-[10px] text-muted-foreground sm:col-span-1">{s.machine}</span>
                </button>
              </TooltipTrigger>
              <TooltipContent className="max-w-72 text-xs">
                <div className="font-medium">
                  {s.system}: {s.value} tok/s ({s.phase})
                </div>
                {s.context && <div className="text-muted-foreground">{s.context}</div>}
                {s.caveat && <div className="mt-1 text-primary">* {s.caveat}</div>}
              </TooltipContent>
            </Tooltip>
          );
        })}
        {ceiling && (
          <p className="pt-2 font-mono text-[10px] text-destructive">Dashed line: {ceiling.label}</p>
        )}
      </div>
    </TooltipProvider>
  );
}

export function JourneyChart({ entries, className }: { entries: JourneyEntry[]; className?: string }) {
  const landed = entries.filter((e) => !e.refuted);
  const values = landed.map((e) => e.value);
  // Guard degenerate data: empty → ±Infinity, single/equal values → max===min.
  const rawMin = values.length ? Math.min(...values) : 0;
  const rawMax = values.length ? Math.max(...values) : 1;
  const min = rawMin * 0.92;
  const max = rawMax * 1.05 > min ? rawMax * 1.05 : min + 1;
  const W = 720;
  const H = 220;
  const PAD = { l: 40, r: 16, t: 16, b: 8 };
  const x = (i: number) => PAD.l + (i / Math.max(1, landed.length - 1)) * (W - PAD.l - PAD.r);
  const y = (v: number) => PAD.t + (1 - (v - min) / (max - min)) * (H - PAD.t - PAD.b - 60);
  const path = landed.map((e, i) => `${i === 0 ? "M" : "L"} ${x(i)} ${y(e.value)}`).join(" ");
  return (
    <TooltipProvider>
      <svg viewBox={`0 0 ${W} ${H}`} className={cn("w-full", className)} role="img" aria-label="Perf journey">
        {[min, (min + max) / 2, max].map((v) => (
          <g key={v}>
            <line x1={PAD.l} y1={y(v)} x2={W - PAD.r} y2={y(v)} stroke="var(--border)" strokeWidth={0.5} />
            <text x={PAD.l - 6} y={y(v)} textAnchor="end" dominantBaseline="central" fontSize={9} className="fill-muted-foreground font-mono">
              {v.toFixed(0)}
            </text>
          </g>
        ))}
        <path d={path} fill="none" stroke="var(--primary)" strokeWidth={2} />
        {landed.map((e, i) => (
          <Tooltip key={e.label}>
            <TooltipTrigger asChild>
              {/* biome-ignore lint/a11y/noInteractiveElementToNoninteractiveRole: Focus exposes an informational tooltip; this point has no button action. */}
              <g className="cursor-default" tabIndex={0} role="img" aria-label={`${e.label}: ${e.value} tok/s`}>
                <circle cx={x(i)} cy={y(e.value)} r={5} fill="var(--primary)" />
                <text
                  x={x(i)}
                  y={y(e.value) + (i % 2 === 0 ? 40 : 20)}
                  textAnchor="middle"
                  fontSize={8.5}
                  className="fill-muted-foreground font-mono"
                >
                  {e.label.length > 22 ? `${e.label.slice(0, 20)}…` : e.label}
                </text>
              </g>
            </TooltipTrigger>
            <TooltipContent className="max-w-72 text-xs">
              <div className="font-medium">
                {e.label} — {e.value} tok/s{e.delta && ` (${e.delta})`}
              </div>
              {e.detail && <div className="mt-1 text-muted-foreground">{e.detail}</div>}
            </TooltipContent>
          </Tooltip>
        ))}
      </svg>
    </TooltipProvider>
  );
}

export function GapWaterfall({
  base,
  segments,
  ceiling,
  className,
}: {
  base: { label: string; value: number };
  segments: GapSegment[];
  ceiling: { label: string; value: number };
  className?: string;
}) {
  const max = ceiling.value * 1.05;
  let cursor = base.value;
  return (
    <TooltipProvider>
      <div className={cn("space-y-1.5", className)}>
        <WaterfallRow label={base.label} from={0} to={base.value} max={max} solid />
        {segments.map((seg) => {
          const row = (
            <WaterfallRow
              key={seg.label}
              label={`${seg.landed ? "✓ " : "+ "}${seg.label}`}
              from={cursor}
              to={cursor + seg.tokS}
              max={max}
              landed={seg.landed}
              note={seg.note}
            />
          );
          cursor += seg.tokS;
          return row;
        })}
        <WaterfallRow label={ceiling.label} from={0} to={ceiling.value} max={max} ghost />
      </div>
    </TooltipProvider>
  );
}

function WaterfallRow({
  label,
  from,
  to,
  max,
  solid,
  ghost,
  landed,
  note,
}: {
  label: string;
  from: number;
  to: number;
  max: number;
  solid?: boolean;
  ghost?: boolean;
  landed?: boolean;
  note?: string;
}) {
  const barBody = (
    <>
      <span className={cn("w-52 shrink-0 truncate text-right text-xs", ghost ? "text-destructive/80" : landed ? "text-emerald-600 dark:text-emerald-400" : "text-muted-foreground")}>
        {label}
      </span>
      <div className="relative h-5 flex-1 rounded-sm bg-muted/30">
        <div
          className={cn("absolute h-full rounded-sm", ghost && "border border-dashed border-destructive/60 bg-transparent")}
          style={{
            left: `${(from / max) * 100}%`,
            width: `${(Math.max(0, to - from) / max) * 100}%`,
            background: ghost ? undefined : solid ? "var(--primary)" : landed ? "var(--dtype-q8)" : "var(--muted-foreground)",
            opacity: ghost ? 1 : solid ? 0.9 : 0.55,
          }}
        />
        <span className="absolute inset-y-0 right-2 flex items-center font-mono text-[10px] text-muted-foreground">
          {to.toFixed(1)}
        </span>
      </div>
    </>
  );
  if (!note) return <div className="flex items-center gap-3">{barBody}</div>;
  return (
    <Tooltip>
      <TooltipTrigger asChild>
        <div
          className="flex items-center gap-3"
          role="img"
          aria-label={`${label}: ${from.toFixed(1)} to ${to.toFixed(1)}`}
          // biome-ignore lint/a11y/noNoninteractiveTabindex: focus exposes the note tooltip; the bar carries no button action.
          tabIndex={0}
        >
          {barBody}
        </div>
      </TooltipTrigger>
      <TooltipContent className="max-w-72 text-xs">{note}</TooltipContent>
    </Tooltip>
  );
}

export function BytesBar({ entries, className }: { entries: BytesBreakdownEntry[]; className?: string }) {
  const total = entries.reduce((n, e) => n + e.mbPerToken, 0);
  const colors = ["var(--kfam-matvec)", "var(--kfam-sampling)", "var(--kfam-attention)", "var(--kfam-fusion)", "var(--kfam-kv)", "var(--muted-foreground)"];
  return (
    <TooltipProvider>
      <div className={className}>
        <div className="flex h-7 overflow-hidden rounded-sm">
          {entries.map((e, i) => (
            <Tooltip key={e.label}>
              <TooltipTrigger asChild>
                <div
                  className="h-full"
                  role="img"
                  // biome-ignore lint/a11y/noNoninteractiveTabindex: Focus exposes an informational tooltip; this segment has no button action.
                  tabIndex={0}
                  aria-label={`${e.label}: ${e.mbPerToken.toFixed(0)} MB/token`}
                  style={{ width: `${(e.mbPerToken / total) * 100}%`, background: colors[i % colors.length], opacity: 0.8 }}
                />
              </TooltipTrigger>
              <TooltipContent className="text-xs">
                <div className="font-medium">
                  {e.label}: {e.mbPerToken.toFixed(0)} MB/token ({(e.share * 100).toFixed(1)}%)
                </div>
                {e.note && <div className="text-muted-foreground">{e.note}</div>}
              </TooltipContent>
            </Tooltip>
          ))}
        </div>
        <div className="mt-2 flex flex-wrap gap-x-4 gap-y-1">
          {entries.map((e, i) => (
            <span key={e.label} className="flex items-center gap-1.5 text-[11px] text-muted-foreground">
              <span className="size-2 rounded-full" style={{ background: colors[i % colors.length] }} />
              {e.label} {(e.share * 100).toFixed(1)}%
            </span>
          ))}
        </div>
        <div className="mt-1 font-mono text-[10px] text-muted-foreground">total ≈ {(total / 1024).toFixed(2)} GB/token</div>
      </div>
    </TooltipProvider>
  );
}
