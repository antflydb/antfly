import { z } from "zod";
import { KernelFamily } from "./kernel.ts";
import { ModelId, SCHEMA_VERSION } from "./model-spec.ts";
import { SourceLink } from "./source-link.ts";

/**
 * A planned (or captured) Metal command frame for the timeline view.
 * Timings are optional garnish — the view degrades to "planned" mode
 * (structure only) when they are absent.
 */
export const PlannedOpSpan = z.object({
  label: z.string(),
  kernel: z.string().optional(),
  /** Typed so a typo'd family fails validation instead of rendering gray. */
  family: KernelFamily,
  estBytes: z.number().nonnegative().optional(),
  gpuNanos: z.number().nonnegative().optional(),
});

export const EncoderScopeSpan = z.object({
  id: z.string(),
  kind: z.enum(["compute", "blit"]).default("compute"),
  label: z.string().optional(),
  ops: z.array(PlannedOpSpan),
});

export const BarrierMark = z.object({
  afterScope: z.string(),
  afterOpIndex: z.number().int().nonnegative().optional(),
  hazard: z.enum(["raw", "war", "waw"]),
  tensors: z.array(z.string()).default([]),
});

export const FrameScenario = z
  .object({
    schemaVersion: z.literal(SCHEMA_VERSION),
    id: z.string(), // "decode-q4_0" | "decode-q8_0-anchor" | "decode-pipelined" | "draft-mtp"
    modelId: ModelId,
    title: z.string(),
    description: z.string().optional(),
    machine: z.string().optional(),
    source: SourceLink.optional(),
    mode: z.enum(["planned", "captured"]).default("planned"),
    stats: z
      .object({
        frameMs: z.number().nonnegative().optional(),
        encoders: z.number().int().nonnegative().optional(),
        plannedScopes: z.number().int().nonnegative().optional(),
        plannedBarriers: z.number().int().nonnegative().optional(),
        encodeCpuUs: z.number().nonnegative().optional(),
      })
      .default({}),
    encoderScopes: z.array(EncoderScopeSpan),
    barriers: z.array(BarrierMark).default([]),
    pipelining: z
      .object({ overlapsPrevFrame: z.boolean(), tokenHandoff: z.enum(["device", "host"]) })
      .optional(),
  })
  .refine(
    (frame) => frame.mode !== "captured" || Boolean(frame.machine && frame.source),
    "captured frames require machine and source evidence"
  );
export type FrameScenario = z.infer<typeof FrameScenario>;

/** KV cache visual trace: config generated; steps deterministically synthesized (flagged). */
export const KvEvent = z.object({
  kind: z.enum(["alloc", "evict", "compact", "share"]),
  lane: z.string(), // "global" | "swa" | "shared"
  blockId: z.number(),
  note: z.string().optional(),
});

export const KvTrace = z.object({
  schemaVersion: z.number(),
  modelId: z.string(),
  synthesized: z.boolean().default(true),
  config: z.object({
    blockTokens: z.number(),
    lanes: z.array(
      z.object({
        id: z.string(),
        label: z.string(),
        layers: z.number(),
        windowTokens: z.number().optional(),
        sharedWith: z.string().optional(),
      })
    ),
    dtypes: z.array(
      z.object({ id: z.string(), label: z.string(), bytesPerTokenLayer: z.number() })
    ),
  }),
  steps: z.array(z.object({ t: z.number(), events: z.array(KvEvent) })),
});
export type KvTrace = z.infer<typeof KvTrace>;
