"use client";

import { Button, cn } from "@antfly/design-system";
import { Pause, Play } from "lucide-react";
import { useEffect, useId, useMemo, useRef, useState } from "react";
import { KvBlockGlyph } from "@/components/viz/glyphs";
import type { KvTrace } from "@/lib/schema";

const BLOCK = 16;
const GAP = 4;
const PER_ROW = 24;

type BlockState = "empty" | "filled" | "evicted" | "shared";

/** Replay events up to step t -> per-lane block states. */
function statesAt(trace: KvTrace, t: number): Map<string, Map<number, BlockState>> {
  const lanes = new Map<string, Map<number, BlockState>>();
  for (const lane of trace.config.lanes) lanes.set(lane.id, new Map());
  for (const step of trace.steps) {
    if (step.t > t) break;
    for (const ev of step.events) {
      const lane = lanes.get(ev.lane);
      if (!lane) continue;
      if (ev.kind === "alloc") lane.set(ev.blockId, "filled");
      else if (ev.kind === "evict") lane.set(ev.blockId, "evicted");
      else if (ev.kind === "compact") lane.delete(ev.blockId);
      else if (ev.kind === "share") lane.set(ev.blockId, "shared");
    }
  }
  return lanes;
}

export function KvCacheBlocks({
  trace,
  dtypeId,
  initialStep,
  className,
}: {
  trace: KvTrace;
  dtypeId?: string;
  initialStep?: number;
  className?: string;
}) {
  const maxStep = trace.steps.length > 0 ? trace.steps[trace.steps.length - 1].t : 0;
  const [step, setStep] = useState(initialStep ?? Math.min(64, maxStep));
  const [playing, setPlaying] = useState(false);
  const raf = useRef<ReturnType<typeof setInterval> | null>(null);
  const sliderId = useId();

  useEffect(() => {
    if (!playing) return;
    raf.current = setInterval(() => {
      setStep((s) => {
        if (s >= maxStep) {
          setPlaying(false);
          return s;
        }
        return s + 1;
      });
    }, 60);
    return () => {
      if (raf.current) clearInterval(raf.current);
    };
  }, [playing, maxStep]);

  const lanes = useMemo(() => statesAt(trace, step), [trace, step]);
  const dtype = trace.config.dtypes.find((d) => d.id === dtypeId) ?? trace.config.dtypes[0];

  let totalBytes = 0;
  for (const [laneId, blocks] of lanes) {
    const lane = trace.config.lanes.find((l) => l.id === laneId);
    if (!lane) continue;
    let live = 0;
    for (const state of blocks.values()) if (state === "filled" || state === "shared") live++;
    totalBytes += live * trace.config.blockTokens * lane.layers * dtype.bytesPerTokenLayer;
  }

  return (
    <div className={cn("flex h-full flex-col gap-3", className)}>
      <div className="flex items-center gap-3">
        <Button
          variant="outline"
          size="icon"
          className="size-7"
          aria-label={playing ? "Pause KV replay" : "Play KV replay"}
          onClick={() => {
            if (!playing && step >= maxStep) setStep(0);
            setPlaying((p) => !p);
          }}
        >
          {playing ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
        </Button>
        <label htmlFor={sliderId} className="sr-only">
          KV replay position
        </label>
        <input
          id={sliderId}
          type="range"
          value={step}
          aria-valuetext={`${step} tokens, ${(totalBytes / 1024 / 1024).toFixed(1)} MiB estimated KV payload`}
          min={0}
          max={maxStep}
          step={1}
          disabled={maxStep === 0}
          onChange={(event) => {
            setPlaying(false);
            setStep(event.currentTarget.valueAsNumber);
          }}
          className="h-5 min-w-0 flex-1 cursor-pointer accent-primary focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring focus-visible:ring-offset-2 focus-visible:ring-offset-background disabled:cursor-not-allowed disabled:opacity-50"
        />
        <div className="w-32 text-right font-mono text-xs text-muted-foreground">
          t={step} · {(totalBytes / 1024 / 1024).toFixed(1)} MiB
        </div>
      </div>

      <div className="min-h-0 flex-1 space-y-4 overflow-y-auto">
        {trace.config.lanes.map((lane) => {
          const blocks = lanes.get(lane.id) ?? new Map<number, BlockState>();
          const maxBlock = Math.max(0, ...blocks.keys());
          const rows = Math.max(1, Math.ceil((maxBlock + 1) / PER_ROW));
          const height = rows * (BLOCK + GAP) + 4;
          return (
            <div key={lane.id}>
              <div className="mb-1 flex items-baseline justify-between">
                <span className="text-xs font-medium">{lane.label}</span>
                <span className="font-mono text-[10px] text-muted-foreground">
                  {lane.layers} layers
                  {lane.windowTokens ? ` · window ${lane.windowTokens}` : ""}
                  {lane.sharedWith ? ` · shares KV with ${lane.sharedWith}` : ""}
                </span>
              </div>
              <svg
                viewBox={`0 0 ${PER_ROW * (BLOCK + GAP)} ${height}`}
                className="w-full"
                style={{ maxHeight: rows * 22 }}
                role="img"
                aria-label={`${lane.label} KV blocks`}
              >
                {Array.from({ length: rows * PER_ROW }, (_, blockId) => blockId).map((i) => {
                  const state = blocks.get(i) ?? "empty";
                  const x = (i % PER_ROW) * (BLOCK + GAP);
                  const y = Math.floor(i / PER_ROW) * (BLOCK + GAP);
                  return <KvBlockGlyph key={i} x={x} y={y} size={BLOCK} state={state} />;
                })}
              </svg>
            </div>
          );
        })}
      </div>
      {trace.synthesized && (
        <div className="text-[10px] text-muted-foreground">
          Illustrative logical KV retention (block size {trace.config.blockTokens} tokens,{" "}
          {dtype.label}) — estimated payload only, not physical allocation or a captured trace.
        </div>
      )}
    </div>
  );
}
