import type { FrameScenario, KvTrace, ModelSpec, SankeySpec } from "@/lib/schema";

export const fixtureSankey: SankeySpec = {
  nodes: [
    { id: "tok", label: "tokens", colorVar: "var(--dtype-f32)" },
    { id: "emb", label: "embed", colorVar: "var(--dtype-f16)" },
    { id: "attn", label: "attention", colorVar: "var(--kfam-attention)" },
    { id: "ffn", label: "FFN", colorVar: "var(--kfam-matvec)" },
    { id: "head", label: "LM head", colorVar: "var(--kfam-sampling)" },
  ],
  links: [
    { source: "tok", target: "emb", value: 10 },
    { source: "emb", target: "attn", value: 12 },
    { source: "attn", target: "ffn", value: 66, label: "65.7% of bytes" },
    { source: "ffn", target: "head", value: 20 },
  ],
};

export const fixtureSpec: ModelSpec = {
  schemaVersion: 1,
  id: "gemma4-e2b",
  family: "fixture",
  displayName: "Fixture Model",
  tagline: "A tiny synthetic spec for the kitchen sink.",
  stats: { layers: 4, hidden: 512 },
  stages: [
    { kind: "embedding", id: "embed", title: "Embedding", spine: "graph" },
    {
      kind: "decoder",
      id: "layers",
      title: "Decoder layer",
      spine: "graph",
      attention: "gqa_paged",
      repeat: { count: 4, variants: [{ tag: "swa", layerIdxs: [0, 1, 2] }, { tag: "global", layerIdxs: [3] }] },
    },
    { kind: "head", id: "head", title: "LM head", headType: "lm", spine: "sample" },
  ],
  graphs: {
    decode: {
      nodes: [
        {
          id: "embed.lookup",
          opKind: "embedding_lookup",
          label: "embed tokens",
          stageId: "embed",
          inputs: [],
          outputs: [],
          shapes: { in: [{ dims: ["T"], dtype: "i32" }], out: [{ dims: ["T", 512], dtype: "f16" }] },
          attrs: {},
          kernelRouteIds: [],
          kernels: [],
          envFlagNames: [],
          backend: "metal",
          fusedOps: [],
        },
        {
          id: "l.attn",
          opKind: "gqa_paged_attention",
          label: "GQA attention",
          stageId: "layers",
          inputs: [],
          outputs: [],
          shapes: { in: [{ dims: ["T", 512], dtype: "f16" }], out: [{ dims: ["T", 512], dtype: "f16" }] },
          attrs: {},
          kernelRouteIds: [],
          kernels: [],
          envFlagNames: ["TERMITE_METAL_DECODE_GQA_SPLIT_COUNT"],
          backend: "metal",
          fusedOps: [],
        },
        {
          id: "l.ffn",
          opKind: "linear_no_bias_pair",
          label: "gate+up (fused)",
          stageId: "layers",
          inputs: [],
          outputs: [],
          shapes: { in: [{ dims: ["T", 512], dtype: "f16" }], out: [{ dims: ["T", 2048], dtype: "f16" }] },
          attrs: {},
          kernelRouteIds: ["q4_0/rows_2_8/none"],
          kernels: [],
          envFlagNames: [],
          backend: "metal",
          fusedOps: ["gate", "up", "silu"],
        },
        {
          id: "head.lm",
          opKind: "linear_no_bias",
          label: "LM head",
          stageId: "head",
          inputs: [],
          outputs: [],
          shapes: { in: [{ dims: [1, 512], dtype: "f16" }], out: [{ dims: [1, 32000], dtype: "f32" }] },
          attrs: {},
          kernelRouteIds: [],
          kernels: [],
          envFlagNames: [],
          backend: "metal",
          fusedOps: [],
        },
      ],
      edges: [
        { id: "e1", from: "embed.lookup", to: "l.attn", kind: "data" },
        { id: "e2", from: "l.attn", to: "l.ffn", kind: "residual" },
        { id: "e3", from: "l.ffn", to: "head.lm", kind: "data" },
      ],
    },
  },
  sankey: fixtureSankey,
  sources: { gitCommit: "fixture", generatedAt: "fixture" },
};

export const fixtureKvTrace: KvTrace = {
  schemaVersion: 1,
  modelId: "gemma4-e2b",
  synthesized: true,
  config: {
    blockTokens: 16,
    lanes: [
      { id: "global", label: "Global layers", layers: 5 },
      { id: "swa", label: "SWA layers", layers: 25, windowTokens: 128 },
    ],
    dtypes: [{ id: "f16", label: "f16 KV", bytesPerTokenLayer: 1024 }],
  },
  steps: Array.from({ length: 257 }, (_, t) => {
    const events = [];
    if (t > 0 && t % 16 === 0) {
      const block = t / 16 - 1;
      events.push({ kind: "alloc" as const, lane: "global", blockId: block });
      events.push({ kind: "alloc" as const, lane: "swa", blockId: block });
      const evictBefore = Math.floor((t - 128) / 16) - 1;
      if (evictBefore >= 0) events.push({ kind: "evict" as const, lane: "swa", blockId: evictBefore });
    }
    return { t, events };
  }),
};

export const fixtureFrame: FrameScenario = {
  schemaVersion: 1,
  id: "decode-q4_0",
  modelId: "gemma4-e2b",
  title: "Decode frame (Q4_0)",
  mode: "planned",
  stats: { encoders: 1, plannedScopes: 4, plannedBarriers: 1 },
  encoderScopes: [
    {
      id: "s1",
      kind: "compute",
      label: "attention setup",
      ops: [
        { label: "rms_norm", family: "norm_rope", estBytes: 4096 },
        { label: "qkv project", kernel: "termite_q4_0_linear_1x_reduce", family: "matvec", estBytes: 5_000_000 },
        { label: "head_rms⋄rope", kernel: "termite_apply_head_rms_rope", family: "fusion", estBytes: 65536 },
      ],
    },
    {
      id: "s2",
      kind: "compute",
      label: "attention",
      ops: [{ label: "paged attention", kernel: "termite_paged_attention_kv_decode_gqa_split_stage", family: "attention", estBytes: 9_000_000 }],
    },
    {
      id: "s3",
      kind: "compute",
      label: "FFN",
      ops: [
        { label: "gate+up (pair)", kernel: "termite_q4_0_pair_activation_multiply_rms_scale_1r_ext", family: "fusion", estBytes: 30_000_000 },
        { label: "down", kernel: "termite_q4_0_linear_1x_reduce", family: "matvec", estBytes: 15_000_000 },
      ],
    },
    {
      id: "s4",
      kind: "compute",
      label: "sample",
      ops: [
        { label: "lm head top8", kernel: "termite_lm_head_top8_reduce", family: "sampling", estBytes: 20_000_000 },
        { label: "gumbel-max", kernel: "termite_sample_gumbel_partials", family: "sampling", estBytes: 128_000 },
      ],
    },
  ],
  barriers: [{ afterScope: "s2", hazard: "raw", tensors: ["attn_out"] }],
  pipelining: { overlapsPrevFrame: true, tokenHandoff: "device" },
};
