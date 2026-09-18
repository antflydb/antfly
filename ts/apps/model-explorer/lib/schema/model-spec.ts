import { z } from "zod";
import { SourceLink } from "./source-link.ts";

export const SCHEMA_VERSION = 1;

export const ModelId = z.enum([
  "gliner2",
  "gliner25",
  "gemma4-e2b",
  "gemma4-e4b",
  "qwen3-embedding",
  "qwen3-vl",
]);
export type ModelId = z.infer<typeof ModelId>;

export const TensorShape = z.object({
  /** Symbolic dims allowed: ["B", "T", 2048]. */
  dims: z.array(z.union([z.number(), z.string()])),
  /** A concrete example instance for display. */
  example: z.array(z.number()).optional(),
  dtype: z.string().optional(),
  quant: z.string().optional(),
  /** Approx bytes for the example instance (drives edge thickness). */
  bytes: z.number().optional(),
});
export type TensorShape = z.infer<typeof TensorShape>;

export const OpNode = z.object({
  id: z.string(),
  /** Validated against the generated op vocabulary at merge time. */
  opKind: z.string(),
  label: z.string().optional(),
  stageId: z.string(),
  inputs: z.array(z.string()).default([]),
  outputs: z.array(z.string()).default([]),
  shapes: z
    .object({ in: z.array(TensorShape).default([]), out: z.array(TensorShape).default([]) })
    .default({ in: [], out: [] }),
  attrs: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).default({}),
  /** Where this op is built (graph builder / architecture file). */
  source: SourceLink.optional(),
  /** Where it is lowered (command planner / lowerer). */
  lowererSource: SourceLink.optional(),
  kernelRouteIds: z.array(z.string()).default([]),
  /** Kernel names from the inventory this op dispatches to. */
  kernels: z.array(z.string()).default([]),
  envFlagNames: z.array(z.string()).default([]),
  backend: z.enum(["metal", "native", "both"]).default("metal"),
  /**
   * Descriptive labels for work folded into this node (renders the zipper
   * border). Free-form by design — entries like "c2c" or "swiglu" are prose,
   * not validated op-vocabulary names; use `opKind` for the validated op.
   */
  fusedOps: z.array(z.string()).default([]),
  /** Optional precomputed layout position (hero graphs). */
  position: z.object({ x: z.number(), y: z.number() }).optional(),
});
export type OpNode = z.infer<typeof OpNode>;

export const Edge = z.object({
  id: z.string(),
  from: z.string(),
  to: z.string(),
  kind: z.enum(["data", "residual", "kv", "routing"]).default("data"),
  tensor: TensorShape.optional(),
});
export type Edge = z.infer<typeof Edge>;

const StageBase = {
  id: z.string(),
  title: z.string(),
  /** Spine stage this belongs to, for the spine strip. */
  spine: z
    .enum(["http", "session", "tokenizer", "graph", "frames", "kernels", "kv", "sample", "vision"])
    .optional(),
  summary: z.string().optional(),
  source: SourceLink.optional(),
  repeat: z
    .object({
      count: z.number().int().positive(),
      note: z.string().optional(),
      /**
       * One op-level layer is emitted per *variant kind* (SWA/global,
       * KV-owner/shared, MoE/dense) instead of N copies.
       */
      variants: z
        .array(
          z.object({
            tag: z.string(),
            layerIdxs: z.array(z.number().int().nonnegative()),
            note: z.string().optional(),
          })
        )
        .optional(),
    })
    .optional(),
};

export const Stage = z.discriminatedUnion("kind", [
  z.object({ kind: z.literal("embedding"), ...StageBase }),
  z.object({
    kind: z.literal("encoder"),
    ...StageBase,
    attention: z.enum(["disentangled_relative", "windowed", "full"]).optional(),
  }),
  z.object({
    kind: z.literal("decoder"),
    ...StageBase,
    attention: z.enum(["gqa_paged", "windowed", "full"]).optional(),
  }),
  z.object({ kind: z.literal("moe"), ...StageBase, numExperts: z.number(), topK: z.number() }),
  z.object({ kind: z.literal("mtp"), ...StageBase, draftDepth: z.number().optional() }),
  z.object({ kind: z.literal("vision"), ...StageBase, patch: z.array(z.number()).optional() }),
  z.object({ kind: z.literal("projector"), ...StageBase }),
  z.object({ kind: z.literal("pooling"), ...StageBase, method: z.string().optional() }),
  z.object({
    kind: z.literal("head"),
    ...StageBase,
    headType: z.enum(["lm", "span", "classifier"]).optional(),
  }),
  z.object({ kind: z.literal("kvcache"), ...StageBase, pageSize: z.number().optional() }),
]);
export type Stage = z.infer<typeof Stage>;

export const SankeyNode = z.object({
  id: z.string(),
  label: z.string(),
  stageId: z.string().optional(),
  colorVar: z.string().optional(),
});
export const SankeyLink = z.object({
  source: z.string(),
  target: z.string(),
  /** Relative flow weight (e.g. bytes/token share). */
  value: z.number().positive(),
  label: z.string().optional(),
});
export const SankeySpec = z.object({
  nodes: z.array(SankeyNode),
  links: z.array(SankeyLink),
});
export type SankeySpec = z.infer<typeof SankeySpec>;

export const PhaseGraph = z.object({
  nodes: z.array(OpNode),
  edges: z.array(Edge),
});
export type PhaseGraph = z.infer<typeof PhaseGraph>;

export const ModelSpec = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  id: ModelId,
  family: z.string(),
  displayName: z.string(),
  tagline: z.string(),
  stats: z.record(z.string(), z.union([z.string(), z.number()])).default({}),
  stages: z.array(Stage),
  graphs: z.object({
    decode: PhaseGraph,
    prefill: PhaseGraph.optional(),
  }),
  sankey: SankeySpec.optional(),
  sources: z.object({
    gitCommit: z.string(),
    generatedAt: z.string(),
  }),
});
export type ModelSpec = z.infer<typeof ModelSpec>;

export const Manifest = z.object({
  schemaVersion: z.literal(SCHEMA_VERSION),
  gitCommit: z.string(),
  generatedAt: z.string(),
  /** e.g. "https://github.com/antflydb/antfly/blob" — links become `${base}/${commit}/${path}#L${line}` */
  permalinkBase: z.string().optional(),
  models: z.array(ModelId),
  counts: z.record(z.string(), z.number()).default({}),
});
export type Manifest = z.infer<typeof Manifest>;
