import { z } from "zod";
import { SourceLink } from "./source-link.ts";

/** One row of `metal_production_schedules` in quant_kernel_compiler.zig. */
export const KernelSchedule = z.object({
  threadsPerThreadgroup: z.number().optional(),
  colsPerThreadgroup: z.number().optional(),
  rowsPerThreadgroup: z.number().optional(),
  reduction: z.string().optional(),
  keyChunk: z.number().optional(),
  skipRescale: z.boolean().optional(),
  /** Any extra schedule fields the parser found, verbatim. */
  extra: z.record(z.string(), z.union([z.string(), z.number(), z.boolean()])).optional(),
});
export type KernelSchedule = z.infer<typeof KernelSchedule>;

export const KernelRoute = z.object({
  /** e.g. "q4_k/rows_2_8/none" */
  id: z.string(),
  format: z.string(),
  rowBucket: z.string(),
  epilogue: z.string(),
  opKind: z.string().optional(),
  schedule: KernelSchedule,
  kernelName: z.string().optional(),
  generated: z.boolean().default(false),
  generatedFile: z.string().optional(),
  source: SourceLink,
});
export type KernelRoute = z.infer<typeof KernelRoute>;

export const KernelFamily = z.enum([
  "matvec",
  "mm_sg",
  "attention",
  "fusion",
  "moe",
  "sampling",
  "kv",
  "norm_rope",
  "vision",
  "gliner",
  "training",
  "data_movement",
  "other",
]);
export type KernelFamily = z.infer<typeof KernelFamily>;

export const KernelInventoryEntry = z.object({
  name: z.string(),
  family: KernelFamily,
  generated: z.boolean(),
  source: SourceLink,
});
export type KernelInventoryEntry = z.infer<typeof KernelInventoryEntry>;

export const EnvFlagGate = z.object({
  name: z.string(),
  kind: z.enum(["bool", "int", "mb", "bytes", "enum", "unknown"]).default("unknown"),
  /** Curated overlay; absent for the long tail. */
  description: z.string().optional(),
  occurrences: z.number().int(),
  sources: z.array(SourceLink).min(1),
});
export type EnvFlagGate = z.infer<typeof EnvFlagGate>;

export const KernelsFile = z.object({
  schemaVersion: z.number(),
  routes: z.array(KernelRoute),
  inventory: z.array(KernelInventoryEntry),
});
export type KernelsFile = z.infer<typeof KernelsFile>;

export const EnvFlagsFile = z.object({
  schemaVersion: z.number(),
  flags: z.array(EnvFlagGate),
});
export type EnvFlagsFile = z.infer<typeof EnvFlagsFile>;
