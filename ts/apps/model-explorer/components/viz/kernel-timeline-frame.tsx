"use client";

import {
  cn,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@antfly/design-system";
import { Fragment, useMemo } from "react";
import type { FrameScenario } from "@/lib/schema";

const WIDTH = 900;
const LANE_H = 46;
const OP_H = 30;

const FAMILY_COLOR: Record<string, string> = {
  matvec: "var(--kfam-matvec)",
  mm_sg: "var(--kfam-mmsg)",
  attention: "var(--kfam-attention)",
  fusion: "var(--kfam-fusion)",
  moe: "var(--kfam-moe)",
  sampling: "var(--kfam-sampling)",
  kv: "var(--kfam-kv)",
  norm_rope: "var(--kfam-fusion)",
};

interface Placed {
  scopeId: string;
  scopeStart: number;
  scopeEnd: number;
  ops: Array<{ x0: number; x1: number; label: string; kernel?: string; family: string }>;
}

/**
 * Perfetto-style frame timeline. In "planned" mode, op width is log-scaled
 * from illustrative bytes moved; in "captured" mode, to measured GPU nanos.
 */
export function KernelTimelineFrame({
  scenario,
  prevScenario,
  className,
}: {
  scenario: FrameScenario;
  /** When pipelined decode is on: the previous frame this one's encode overlaps. */
  prevScenario?: FrameScenario;
  className?: string;
}) {
  const placed = useMemo(() => placeScopes(scenario), [scenario]);
  const pipelined = scenario.pipelining?.overlapsPrevFrame && prevScenario;
  const lanes = pipelined ? 3 : 2;
  const height = lanes * (LANE_H + 14) + 26;

  return (
    <TooltipProvider>
      <div className={cn("flex h-full flex-col", className)}>
        <div className="mb-2 flex flex-wrap items-center gap-x-4 gap-y-1 font-mono text-[10px] text-muted-foreground">
          {scenario.stats.encoders !== undefined && <span>{scenario.stats.encoders} encoders</span>}
          {scenario.stats.plannedScopes !== undefined && (
            <span>{scenario.stats.plannedScopes} planned scopes</span>
          )}
          {scenario.stats.plannedBarriers !== undefined && (
            <span
              className={
                scenario.stats.plannedBarriers === 0
                  ? "text-emerald-700 dark:text-emerald-400"
                  : "text-destructive"
              }
            >
              {scenario.stats.plannedBarriers} barriers
            </span>
          )}
          {scenario.stats.frameMs !== undefined && (
            <span>{scenario.stats.frameMs.toFixed(2)} ms/frame</span>
          )}
          <span className="ml-auto">
            {scenario.mode === "planned" ? "planned structure (no timings)" : scenario.machine}
          </span>
        </div>

        <svg
          viewBox={`0 0 ${WIDTH} ${height}`}
          className="min-h-0 w-full flex-1"
          role="group"
          aria-label="Frame timeline"
        >
          {/* CPU encode lane */}
          <TimelineLane y={0} label="CPU encode">
            <rect
              x={0}
              y={14}
              width={WIDTH * (pipelined ? 0.35 : 0.28)}
              height={OP_H}
              rx={3}
              fill="var(--muted-foreground)"
              opacity={0.35}
            />
            <text
              x={6}
              y={14 + OP_H / 2}
              dominantBaseline="central"
              fontSize={9}
              className="fill-foreground font-mono"
            >
              beginFrame → encode → submit (
              {scenario.encoderScopes.reduce((n, s) => n + s.ops.length, 0)} ops illustrated)
            </text>
          </TimelineLane>

          {/* GPU execute lane */}
          <TimelineLane y={LANE_H + 14} label="GPU execute">
            {placed.map((scope) => (
              <Fragment key={scope.scopeId}>
                {/* encoder-scope bracket */}
                <path
                  d={`M ${scope.scopeStart} 10 v -4 h ${scope.scopeEnd - scope.scopeStart} v 4`}
                  fill="none"
                  stroke="var(--muted-foreground)"
                  strokeWidth={1}
                  opacity={0.7}
                />
                {scope.ops.map((op) => (
                  <Tooltip key={`${scope.scopeId}:${op.x0}`}>
                    <TooltipTrigger asChild>
                      {/* biome-ignore lint/a11y/noInteractiveElementToNoninteractiveRole: Focus exposes an informational image tooltip; this image has no button action. */}
                      <rect
                        role="img"
                        tabIndex={0}
                        aria-label={op.label}
                        x={op.x0}
                        y={14}
                        width={Math.max(1.5, op.x1 - op.x0 - 1)}
                        height={OP_H}
                        rx={2}
                        fill={FAMILY_COLOR[op.family] ?? "var(--muted-foreground)"}
                        opacity={0.8}
                      />
                    </TooltipTrigger>
                    <TooltipContent className="font-mono text-xs">
                      {op.label}
                      {op.kernel && <div className="text-muted-foreground">{op.kernel}</div>}
                    </TooltipContent>
                  </Tooltip>
                ))}
              </Fragment>
            ))}
            {/* barriers */}
            {scenario.barriers.map((b) => {
              const scope = placed.find((s) => s.scopeId === b.afterScope);
              if (!scope) return null;
              const x =
                b.afterOpIndex !== undefined && scope.ops[b.afterOpIndex]
                  ? scope.ops[b.afterOpIndex].x1
                  : scope.scopeEnd;
              return (
                <Tooltip key={`${b.afterScope}:${b.afterOpIndex ?? "end"}:${b.hazard}`}>
                  <TooltipTrigger asChild>
                    {/* biome-ignore lint/a11y/noInteractiveElementToNoninteractiveRole: Focus exposes an informational image tooltip; this image has no button action. */}
                    <line
                      role="img"
                      tabIndex={0}
                      aria-label={`${b.hazard.toUpperCase()} barrier${b.tensors.length > 0 ? `: ${b.tensors.join(", ")}` : ""}`}
                      x1={x}
                      y1={10}
                      x2={x}
                      y2={14 + OP_H + 4}
                      stroke="var(--destructive)"
                      strokeWidth={2}
                    />
                  </TooltipTrigger>
                  <TooltipContent className="font-mono text-xs">
                    {b.hazard.toUpperCase()} barrier
                    {b.tensors.length > 0 && `: ${b.tensors.join(", ")}`}
                  </TooltipContent>
                </Tooltip>
              );
            })}
          </TimelineLane>

          {/* Pipelined next-frame encode lane */}
          {pipelined && (
            <TimelineLane y={2 * (LANE_H + 14)} label="CPU encode (frame N+1)">
              <rect
                x={WIDTH * 0.4}
                y={14}
                width={WIDTH * 0.35}
                height={OP_H}
                rx={3}
                fill="var(--primary)"
                opacity={0.3}
              />
              <text
                x={WIDTH * 0.4 + 6}
                y={14 + OP_H / 2}
                dominantBaseline="central"
                fontSize={9}
                className="fill-foreground font-mono"
              >
                encodes from device-resident token — before frame N's wait
              </text>
            </TimelineLane>
          )}
        </svg>
      </div>
    </TooltipProvider>
  );
}

function TimelineLane({
  y,
  label,
  children,
}: {
  y: number;
  label: string;
  children: React.ReactNode;
}) {
  return (
    <g transform={`translate(0, ${y + 12})`}>
      <text
        x={0}
        y={0}
        fontSize={9}
        className="fill-muted-foreground font-mono uppercase tracking-wider"
      >
        {label}
      </text>
      <g transform="translate(0, 4)">{children}</g>
    </g>
  );
}

function placeScopes(scenario: FrameScenario): Placed[] {
  const weight = (op: { estBytes?: number; gpuNanos?: number }) =>
    scenario.mode === "captured"
      ? (op.gpuNanos ?? 0)
      : Math.max(1, Math.log10((op.estBytes ?? 0) + 10));
  const total = scenario.encoderScopes.reduce(
    (sum, s) => sum + s.ops.reduce((n, op) => n + weight(op), 0),
    0
  );
  const scale = (WIDTH - scenario.encoderScopes.length * 6) / Math.max(1, total);
  let x = 0;
  const out: Placed[] = [];
  for (const scope of scenario.encoderScopes) {
    const start = x;
    const ops = scope.ops.map((op) => {
      const w = weight(op) * scale;
      const placedOp = { x0: x, x1: x + w, label: op.label, kernel: op.kernel, family: op.family };
      x += w;
      return placedOp;
    });
    out.push({ scopeId: scope.id, scopeStart: start, scopeEnd: x, ops });
    x += 6;
  }
  return out;
}
