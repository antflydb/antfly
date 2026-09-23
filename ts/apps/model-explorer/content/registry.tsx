"use client";

import dynamic from "next/dynamic";
import type { ComponentType } from "react";
import type { ModelFrames } from "@/app/models/[slug]/model-page-client";
import type { KernelRoute, ModelSpec } from "@/lib/schema";

/**
 * Tiny server-computed summary of the kernel inventory, so chapters can cite
 * census numbers without importing the generated data layer into the client
 * bundle (lib/data is server-only).
 */
export interface KernelCensus {
  total: number;
  byFamily: Record<string, number>;
  routedSourceFiles: number;
}

export interface ChaptersProps {
  spec: ModelSpec;
  routes: KernelRoute[];
  frames: ModelFrames;
  kernelCensus: KernelCensus;
}

const loading = () => (
  <div className="mx-auto max-w-3xl px-4 py-16 text-muted-foreground">Loading chapters…</div>
);

// One dynamic chunk per model family: a model page downloads only its own
// chapters instead of every model's.
const Gemma4Chapters = dynamic(() => import("./gemma4/chapters").then((m) => m.Gemma4Chapters), {
  loading,
});
const Gliner2Chapters = dynamic(() => import("./gliner2/chapters").then((m) => m.Gliner2Chapters), {
  loading,
});
const Gliner25Chapters = dynamic(
  () => import("./gliner25/chapters").then((m) => m.Gliner25Chapters),
  { loading }
);
const Qwen3EmbeddingChapters = dynamic(
  () => import("./qwen3-embedding/chapters").then((m) => m.Qwen3EmbeddingChapters),
  { loading }
);
const Qwen3VlChapters = dynamic(
  () => import("./qwen3-vl/chapters").then((m) => m.Qwen3VlChapters),
  { loading }
);

const registry: Record<string, ComponentType<ChaptersProps>> = {
  "gemma4-e2b": Gemma4Chapters,
  "gemma4-e4b": Gemma4Chapters,
  gliner2: Gliner2Chapters,
  gliner25: Gliner25Chapters,
  "qwen3-embedding": Qwen3EmbeddingChapters,
  "qwen3-vl": Qwen3VlChapters,
};

function MissingChapters({ spec }: ChaptersProps) {
  return (
    <div className="mx-auto max-w-3xl px-4 py-16 text-muted-foreground">
      Chapters for {spec.displayName} are not written yet — explore the graph via the operation
      explorer above.
    </div>
  );
}

export function getChapters(slug: string): ComponentType<ChaptersProps> {
  return registry[slug] ?? MissingChapters;
}
