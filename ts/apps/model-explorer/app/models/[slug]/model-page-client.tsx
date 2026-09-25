"use client";

import { Button } from "@antfly/design-system";
import { ArrowRight } from "lucide-react";
import Link from "next/link";
import { SourceLinkProvider } from "@/components/code/source-link-context";
import { SpineStrip, type SpineStageId } from "@/components/spine-strip";
import { getChapters, type KernelCensus } from "@/content/registry";
import type { FrameScenario, KernelRoute, ModelSpec } from "@/lib/schema";

export interface ModelFrames {
  q40: FrameScenario;
  q80Anchor: FrameScenario;
}

/** Display names for stats keys that auto-spacing gets wrong. */
const STAT_LABELS: Record<string, string> = {
  ffn: "FFN width",
  tokS: "decode speed",
  relBuckets: "rel-pos buckets",
  kvOwners: "KV owners",
  kvHeads: "KV heads",
  sharedKv: "shared-KV layers",
  pleHidden: "PLE hidden",
  slidingPattern: "sliding:global period",
  ropeTheta: "RoPE θ",
  maxBodyWords: "words per pass",
  quant: "precision",
  deepstackTaps: "DeepStack taps",
  vocab: "vocab size",
};

function statLabel(key: string): string {
  return STAT_LABELS[key] ?? key.replace(/([a-z0-9])([A-Z])/g, "$1 $2").toLowerCase();
}

/** Architecture-first tile ordering; unlisted keys keep their curated order. */
const STAT_ORDER = [
  "layers",
  "hidden",
  "heads",
  "queryHeads",
  "kvHeads",
  "headDim",
  "ffn",
  "vocab",
  "context",
];
function statRank(key: string): number {
  const i = STAT_ORDER.indexOf(key);
  return i === -1 ? STAT_ORDER.length : i;
}

function statValue(v: string | number): string {
  return typeof v === "number" ? v.toLocaleString("en-US") : v;
}

/** Sibling variants documented as separate pages of one family. */
const GEMMA4_VARIANTS = [
  { slug: "gemma4-e2b", label: "E2B" },
  { slug: "gemma4-e4b", label: "E4B" },
];
function variantsFor(slug: string) {
  return slug.startsWith("gemma4") ? GEMMA4_VARIANTS : [];
}

export function ModelPageClient({
  spec,
  routes,
  frames,
  kernelCensus,
  permalinkBase,
}: {
  spec: ModelSpec;
  routes: KernelRoute[];
  frames: ModelFrames;
  kernelCensus: KernelCensus;
  permalinkBase?: string;
}) {
  const Chapters = getChapters(spec.id);
  const modified = [
    ...new Set(spec.stages.map((s) => s.spine).filter((s): s is SpineStageId => !!s)),
  ];

  return (
    <SourceLinkProvider permalinkBase={permalinkBase}>
      <div>
        <header className="border-b bg-muted/20">
          <div className="mx-auto max-w-7xl px-4 py-10">
            <p className="font-mono text-xs uppercase tracking-wider text-muted-foreground">
              {spec.family}
            </p>
            <div className="mt-1 flex flex-wrap items-baseline gap-3">
              <h1 className="text-4xl font-bold tracking-tight">{spec.displayName}</h1>
              {variantsFor(spec.id).length > 0 && (
                <span className="inline-flex items-center gap-1 font-mono text-xs">
                  <span className="text-muted-foreground">variant:</span>
                  {variantsFor(spec.id).map((v) =>
                    v.slug === spec.id ? (
                      <span
                        key={v.slug}
                        aria-current="page"
                        className="rounded border border-primary/60 px-1.5 py-0.5 font-semibold"
                      >
                        {v.label}
                      </span>
                    ) : (
                      <Link
                        key={v.slug}
                        href={`/models/${v.slug}`}
                        className="rounded border px-1.5 py-0.5 text-muted-foreground transition-colors hover:border-primary/60 hover:text-foreground"
                      >
                        {v.label}
                      </Link>
                    )
                  )}
                </span>
              )}
            </div>
            <p className="mt-3 max-w-2xl text-lg text-muted-foreground">{spec.tagline}</p>
            <div className="mt-6 flex flex-wrap gap-2">
              {Object.entries(spec.stats)
                .filter(([k]) => k !== "scope")
                .map(([k, v], i) => [k, v, i] as const)
                .sort(([ka, , ia], [kb, , ib]) => statRank(ka) - statRank(kb) || ia - ib)
                .map(([k, v]) => (
                  <div key={k} className="rounded-md border bg-background/70 px-3 py-1.5">
                    <div className="font-mono text-[10px] uppercase tracking-wider text-muted-foreground">
                      {statLabel(k)}
                    </div>
                    <div className="mt-0.5 font-mono text-sm font-semibold tabular-nums">
                      {statValue(v)}
                    </div>
                  </div>
                ))}
            </div>
            {typeof spec.stats.scope === "string" && (
              <p className="mt-3 max-w-3xl text-xs italic text-muted-foreground">
                {spec.stats.scope}
              </p>
            )}
            <div className="mt-6 flex flex-wrap items-center gap-4">
              <SpineStrip modified={modified} withVision={spec.id === "qwen3-vl"} />
              <Button variant="outline" size="sm" asChild>
                <Link href={`/explore/${spec.id}`}>
                  Open in operation explorer <ArrowRight className="size-3.5" />
                </Link>
              </Button>
            </div>
          </div>
        </header>
        <Chapters spec={spec} routes={routes} frames={frames} kernelCensus={kernelCensus} />
      </div>
    </SourceLinkProvider>
  );
}
