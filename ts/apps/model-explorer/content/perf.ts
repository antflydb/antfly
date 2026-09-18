/**
 * Curated perf content, hand-transcribed from zig/pkg/inference/GEMMA4_PERF_PLAN.md.
 * Deliberately human-owned: these numbers carry caveats and editorial judgment
 * a script shouldn't own. Machine identity is mandatory on every number.
 */
import type { BytesBreakdownEntry, JourneyEntry, PerfSample } from "@/lib/schema";

export const PERF_PLAN = "zig/pkg/inference/GEMMA4_PERF_PLAN.md";

/** §1 comparison — E4B Q4_0, single prompt, 64 tokens, temp 0, serial. */
const rawComparisonSamples: PerfSample[] = [
  {
    metric: "tok_s",
    phase: "decode",
    value: 62.4,
    system: "Antfly (internal decode)",
    machine: "M4 Pro · 273 GB/s",
    context: "16.03 ms/tok · 176 GB/s effective · 64.7% of bandwidth",
    source: { path: PERF_PLAN, anchor: "Circus, E4B Q4_0, single prompt, 64 tokens, temp 0, serial:" },
  },
  {
    metric: "tok_s",
    phase: "e2e",
    value: 54.9,
    system: "Antfly (end-to-end)",
    machine: "M4 Pro · 273 GB/s",
    context: "~156 ms fixed per request — serving overhead, not kernels",
  },
  {
    metric: "tok_s",
    phase: "decode",
    value: 72.2,
    system: "llama.cpp (Q4_0)",
    machine: "M4 Pro · 273 GB/s",
    context: "13.85 ms/tok · 204 GB/s · 74.8%",
    caveat: "Historical observation; peer build, feature parity, and timing boundary were not fully reconciled.",
  },
  {
    metric: "tok_s",
    phase: "decode",
    value: 75.3,
    system: "Ollama (Q4_0)",
    machine: "M4 Pro · 273 GB/s",
  },
  {
    metric: "tok_s",
    phase: "decode",
    value: 86.6,
    system: "vLLM-Metal (MLX 4-bit)",
    machine: "M4 Pro · 273 GB/s",
    context: "11.55 ms/tok · 227 GB/s on ~7.5% fewer bytes · 83.0%",
    caveat: "Historical observation with a different weight format and unresolved configuration parity.",
  },
];

// Every imported comparison keeps its provenance and limitations attached.
// Built as a new array — never mutate module-scope exports in place.
export const comparisonSamples: PerfSample[] = rawComparisonSamples.map((sample) => ({
  ...sample,
  source: sample.source ?? rawComparisonSamples[0].source,
  caveat:
    sample.caveat ?? "Historical planning-note observation; not a current benchmark or matched ranking.",
}));

export const rooflineCeiling = {
  value: 96,
  label: "roofline ~96 tok/s (2.829 GB/token @ 273 GB/s)",
};

/** §1 — bytes/token from the actual GGUF tensor table (E4B Q4_0). */
export const bytesBreakdown: BytesBreakdownEntry[] = [
  { label: "FFN (Q4_0)", mbPerToken: 1858, share: 0.657, note: "gate/up/down across 42 layers — why pair-fusion and batched FFN matter most" },
  { label: "LM head (Q6_K)", mbPerToken: 550, share: 0.195, note: "the [2560 × 262144] vocab matvec — historical baseline tensor traffic" },
  { label: "attention (Q4_0)", mbPerToken: 330, share: 0.117, note: "QKV + output projections" },
  { label: "PLE", mbPerToken: 86, share: 0.03, note: "historical baseline includes the 55 MB F16 per_layer_model_proj, before staging savings" },
  { label: "norms / KV", mbPerToken: 7, share: 0.002 },
];

/** §§9–16 journey ledger — E2B on the fanless M4 Air (120 GB/s, 16 GB) worktree. */
export const journeyE2bAir: JourneyEntry[] = [
  { label: "branch start", value: 44.5, detail: "~44–46 tok/s at branch start (§12 retrospective)", landed: true },
  {
    label: "pipelined decode frame",
    value: 51.5,
    delta: "+9–12%",
    detail: "Historical M4-qualified route: encode frame N+1 from the device-resident token before waiting on N. Token-identical. E4B +5–8%.",
    landed: true,
  },
  {
    label: "LM-head Q4_K repack",
    value: 54.8,
    delta: "+4–5%",
    detail: "opt-in Q6_K→Q4_K candidate repack of the vocab matvec (full logits retain Q6_K) (52.5→54.3–55.2). The Q4_0 head variant was refuted — instant-EOT quality collapse.",
    landed: true,
  },
  {
    label: "pre-handoff review",
    value: 56.5,
    delta: "",
    detail: "53.9→56.5 with repack after review fixes (§11).",
    landed: true,
  },
  {
    label: "pair-fusion + PLE Q8 staging",
    value: 56.5,
    delta: "+2.77% / +1.10% (M4 Pro)",
    detail:
      "historical pair-activation fusion campaign (−84 dispatches/frame) and PLE model-proj Q8_0 staging (E2B slot 351 was dense F32 — ~41 MB/token saved). Peak E2B 56.2–56.5 (roughly 25% vs the reported branch-start range).",
    landed: true,
  },
];

/** Refuted levers — struck-through in the journey narrative. */
export const refutedLevers = [
  { label: "Q6_K rows==1 MMV portfolio", note: "no repeatable win; AUTO stays legacy (§9.2)" },
  { label: "sumsq fusion", note: "−12% repeatable on E2B (47.7 vs 54.2) — refuted (§12.1)" },
  { label: "Q4_0 LM head", note: "instant-EOT quality collapse — refuted (§10.1)" },
  { label: "MTP Metal-auto", note: "E2B 75.9 target-only vs 63.7 with BF16 draft at 64% acceptance — net slower on Metal, default-off (§14.1)" },
];

/** §16.3 finals on the pinned M4 Pro reference box (Mac16,11, 24 GiB). */
export const finalsM4Pro = [
  { model: "E2B", antfly: 80.722, llama: 108.47, pct: "74.4%", context: "§16.3 short 23+256; earlier §14.4 long-context 84.5 vs 104.2 (81.1%)" },
  { model: "E4B", antfly: 56.069, llama: 62.98, pct: "89.0%", context: "§16.3 short 23+256; earlier §14.4 long-context 53.6 vs 61.0 (87.9%)" },
];

/** §16 split-GQA floor retune that produced the finals. */
export const splitGqaRetune = {
  label: "split-GQA min-KV floor made runtime-tunable",
  detail: "E2B floor 512→192 tokens: +12.84% (71.5→80.7). E4B floor 512→32: +18.56% (47.3→56.1). M4 Pro.",
};

export const censusStory = {
  q8Anchor: { encoders: 41, barriers: 422, plannedScopes: 36, label: "Q8_0 anchor census (METAL.md)" },
  q4Live: { encoders: 1, plannedScopes: 143, plannedBarriers: 0, label: "historical Q4_0 decode census" },
};

export const machines = {
  air: "fanless base-M4 Air · 120 GB/s · 16 GB (ceiling ~42 tok/s for E4B)",
  pro: "Mac mini Mac16,11 · M4 Pro · 273 GB/s · 24 GiB (pinned reference box)",
};
