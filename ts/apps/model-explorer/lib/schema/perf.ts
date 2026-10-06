import { z } from "zod";
import { SourceLink } from "./source-link.ts";

export const PerfSample = z.object({
  metric: z.enum(["tok_s", "gb_s", "ms", "gb_per_token", "cosine", "ai"]),
  phase: z.enum(["prefill", "decode", "embed", "e2e"]),
  value: z.number(),
  system: z.string(),
  /** e.g. "M4 Pro mini", "M4 Air (fanless)" — machine identity is mandatory in perf UI. */
  machine: z.string(),
  caveat: z.string().optional(),
  context: z.string().optional(),
  source: SourceLink.optional(),
});
export type PerfSample = z.infer<typeof PerfSample>;

export const JourneyEntry = z.object({
  label: z.string(),
  detail: z.string().optional(),
  /** tok/s after this change landed. */
  value: z.number(),
  delta: z.string().optional(),
  landed: z.boolean().optional(),
  refuted: z.boolean().optional(),
  links: z.array(SourceLink).optional(),
});
export type JourneyEntry = z.infer<typeof JourneyEntry>;

export const BytesBreakdownEntry = z.object({
  label: z.string(),
  mbPerToken: z.number(),
  share: z.number(),
  note: z.string().optional(),
});
export type BytesBreakdownEntry = z.infer<typeof BytesBreakdownEntry>;

export const GapSegment = z.object({
  label: z.string(),
  tokS: z.number(),
  landed: z.boolean().optional(),
  note: z.string().optional(),
  links: z.array(SourceLink).optional(),
});
export type GapSegment = z.infer<typeof GapSegment>;
