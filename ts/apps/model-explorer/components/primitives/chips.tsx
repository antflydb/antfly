"use client";

import {
  cn,
  Tooltip,
  TooltipContent,
  TooltipProvider,
  TooltipTrigger,
} from "@antfly/design-system";
import Link from "next/link";
import type { TensorShape } from "@/lib/schema";

/** dtype/quant string -> precision-ramp CSS var (see globals.css). */
export function dtypeColorVar(dtype: string | undefined): string {
  const d = (dtype ?? "").toLowerCase();
  if (d === "f32") return "var(--dtype-f32)";
  if (d === "f16" || d === "bf16") return "var(--dtype-f16)";
  if (d.startsWith("q8") || d.startsWith("i8")) return "var(--dtype-q8)";
  if (
    d.startsWith("q4") ||
    d.startsWith("q5") ||
    d.startsWith("q6") ||
    d.startsWith("iq4") ||
    d.startsWith("mxfp4") ||
    d === "polar4"
  )
    return "var(--dtype-q4)";
  if (/^(?:q[123](?:_|$)|iq[123](?:_|$))/.test(d)) return "var(--dtype-sub4)";
  return "var(--muted-foreground)";
}

/** Text colors use a stronger light-theme contrast than diagram fills. */
export function dtypeTextColorVar(dtype: string | undefined): string {
  return dtypeColorVar(dtype).replace("--dtype-", "--dtype-text-");
}

export function QuantChip({ format, className }: { format: string; className?: string }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-sm border px-1.5 py-px font-mono text-[10px] font-medium uppercase tracking-wide",
        className
      )}
      style={{ borderColor: dtypeColorVar(format), color: dtypeTextColorVar(format) }}
    >
      {format}
    </span>
  );
}

export function TensorShapeBadge({ shape, className }: { shape: TensorShape; className?: string }) {
  const dims = `[${shape.dims.join(", ")}]`;
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 font-mono text-xs text-muted-foreground",
        className
      )}
    >
      {dims}
      {shape.dtype && (
        <span style={{ color: dtypeTextColorVar(shape.quant ?? shape.dtype) }}>
          · {shape.quant ?? shape.dtype}
        </span>
      )}
    </span>
  );
}

export function OpKindBadge({ opKind, group }: { opKind: string; group?: "primitive" | "fused" }) {
  return (
    <span
      className={cn(
        "inline-flex items-center rounded-sm px-1.5 py-px font-mono text-[11px]",
        group === "fused" ? "bg-primary/10 text-primary" : "bg-muted text-muted-foreground"
      )}
    >
      {opKind}
    </span>
  );
}

export function EnvFlagChip({
  name,
  defaultOn,
  className,
}: {
  name: string;
  /** Current default in the runtime (on = the behavior is enabled by default). */
  defaultOn?: boolean;
  className?: string;
}) {
  return (
    <TooltipProvider>
      <Tooltip>
        <TooltipTrigger asChild>
          <span
            className={cn(
              "inline-flex max-w-full items-center gap-1 rounded-sm border bg-muted/50 px-1.5 py-px font-mono text-[10px]",
              className
            )}
          >
            {defaultOn !== undefined && (
              <span
                aria-hidden="true"
                className={cn(
                  "size-1.5 shrink-0 rounded-full",
                  defaultOn ? "bg-emerald-500" : "bg-muted-foreground/40"
                )}
              />
            )}
            <span className="truncate">{name}</span>
            {/* The dot carries the default state visually; say it too, so colour is not the only cue. */}
            {defaultOn !== undefined && (
              <span className="sr-only">{defaultOn ? " (default on)" : " (default off)"}</span>
            )}
          </span>
        </TooltipTrigger>
        <TooltipContent className="font-mono text-xs">
          {name}
          {defaultOn !== undefined && (
            <span className="ml-2">({defaultOn ? "default on" : "default off"})</span>
          )}
        </TooltipContent>
      </Tooltip>
    </TooltipProvider>
  );
}

/** A fused node's folded-op list, e.g. ⟨head_rms ⋄ rope⟩. */
export function FusionChip({ ops, className }: { ops: string[]; className?: string }) {
  return (
    <span className={cn("inline-flex items-center font-mono text-[11px] text-primary", className)}>
      ⟨{ops.join(" ⋄ ")}⟩
    </span>
  );
}
