"use client";

import { parseAsStringLiteral, useQueryState } from "nuqs";
import { Suspense } from "react";
import { CodeLink } from "@/components/code/code-link";
import { type ClientSnippet, SnippetProvider } from "@/components/code/snippet-context";
import { ChoiceGroup } from "@/components/primitives/choice-group";
import { KvCacheBlocks } from "@/components/viz/kv-cache-blocks";
import { L } from "@/lib/links";
import type { KvTrace } from "@/lib/schema";

/* ------------------------------------------------------------------ */
/* Deterministic trace synthesis (page size 16 tokens throughout).     */
/* These illustrate logical retention; they are not allocator traces.  */
/* ------------------------------------------------------------------ */

const PAGE = 16;

type Steps = KvTrace["steps"];
type Events = Steps[number]["events"];

/**
 * Gemma4 E4B: of 42 layers only 24 own KV — 4 global (keep everything),
 * 20 sliding-window (evict whole pages behind a 512-token window) — and the
 * 18 tail layers share a donor's blocks, so their lane stays empty.
 */
function gemma4Trace(): KvTrace {
  const WINDOW = 512;
  const TOTAL = 768;
  const steps: Steps = [];
  for (let t = 0; t <= TOTAL; t++) {
    const events: Events = [];
    if (t > 0 && (t - 1) % PAGE === 0) {
      const block = Math.floor((t - 1) / PAGE);
      events.push({ kind: "alloc", lane: "global", blockId: block });
      events.push({ kind: "alloc", lane: "swa", blockId: block });
    }
    if (t > WINDOW && (t - WINDOW) % PAGE === 0) {
      events.push({ kind: "evict", lane: "swa", blockId: (t - WINDOW) / PAGE - 1 });
    }
    steps.push({ t, events });
  }
  return {
    schemaVersion: 1,
    modelId: "gemma4-e4b",
    synthesized: true,
    config: {
      blockTokens: PAGE,
      lanes: [
        { id: "global", label: "Global layers (own KV)", layers: 4 },
        { id: "swa", label: "Sliding-window layers (own KV)", layers: 20, windowTokens: WINDOW },
        {
          id: "shared",
          label: "Shared-KV tail — no independent writes",
          layers: 18,
          sharedWith: "donor layers above",
        },
      ],
      dtypes: [
        { id: "f16", label: "f16, max-slot estimate (2 KV heads × 512)", bytesPerTokenLayer: 4096 },
      ],
    },
    steps,
  };
}

/**
 * Qwen3-VL: a large prefill burst of image tokens (allocated as fast as the
 * vision tower emits them), then steady one-token-at-a-time decode.
 */
function qwen3VlTrace(): KvTrace {
  const IMAGE_BLOCKS = 64; // ~1024 image tokens
  const DECODE_TOKENS = 256;
  const steps: Steps = [];
  // t 0..IMAGE_BLOCKS: prefill burst, one page per step (plays back fast).
  for (let t = 0; t <= IMAGE_BLOCKS; t++) {
    const events: Events = [];
    if (t > 0)
      events.push({ kind: "alloc", lane: "layers", blockId: t - 1, note: "image-token prefill" });
    steps.push({ t: t * PAGE, events });
  }
  // then decode: one new page every 16 steps.
  for (let d = 1; d <= DECODE_TOKENS; d++) {
    const t = IMAGE_BLOCKS * PAGE + d;
    const events: Events = [];
    if ((d - 1) % PAGE === 0)
      events.push({
        kind: "alloc",
        lane: "layers",
        blockId: IMAGE_BLOCKS + Math.floor((d - 1) / PAGE),
      });
    steps.push({ t, events });
  }
  return {
    schemaVersion: 1,
    modelId: "qwen3-vl",
    synthesized: true,
    config: {
      blockTokens: PAGE,
      lanes: [{ id: "layers", label: "Qwen3-VL 2B decoder layers", layers: 28 }],
      dtypes: [{ id: "f16", label: "f16 KV (8 KV heads × 128)", bytesPerTokenLayer: 4096 }],
    },
    steps,
  };
}

const TRACES: Record<string, KvTrace> = {
  gemma4: gemma4Trace(),
  "qwen3-vl": qwen3VlTrace(),
};

const MODEL_NOTES: Record<string, { title: string; body: string }> = {
  gemma4: {
    title: "Logical retention versus physical storage",
    body: "This schematic separates E4B's four global and twenty sliding-window KV owners from its eighteen sharing layers. It illustrates page-granular retention with a 512-token window. The standard mixed-attention pool retains full history; eligible Metal paths can use split SWA rings. The displayed estimate uses the maximum 512-dimensional slot for each owner, excludes pool packing and metadata, and is not a measurement of allocated GPU memory.",
  },
  "qwen3-vl": {
    title: "An illustrative image prefill",
    body: "This 2B example assumes 1,024 merged image tokens, then 256 decode tokens, omitting text and special tokens to make the allocation pattern clear. Actual image-token count depends on preprocessing and image size. Each block represents 16 decoder positions; m-RoPE sets their positions, while the decoder writes K/V. This is a synthetic page model, not a recording of vision execution or the backend's physical allocation.",
  },
};

const MODEL_CHOICES = [
  { value: "gemma4", label: "gemma4" },
  { value: "qwen3-embedding", label: "qwen3-embedding" },
  { value: "qwen3-vl", label: "qwen3-vl" },
  { value: "gliner2", label: "gliner2" },
  { value: "gliner25", label: "gliner25" },
] as const;

/* ------------------------------------------------------------------ */
/* Page                                                                */
/* ------------------------------------------------------------------ */

interface KvClientProps {
  snippets: Record<string, ClientSnippet>;
  gitCommit: string;
  permalinkBase?: string;
}

export function KvClient(props: KvClientProps) {
  return (
    <Suspense fallback={<div className="min-h-screen" />}>
      <KvInner {...props} />
    </Suspense>
  );
}

function KvInner({ snippets, gitCommit, permalinkBase }: KvClientProps) {
  const [model, setModel] = useQueryState(
    "model",
    parseAsStringLiteral([
      "gemma4",
      "qwen3-embedding",
      "qwen3-vl",
      "gliner2",
      "gliner25",
    ] as const).withDefault("gemma4")
  );
  const trace = TRACES[model];
  const note = MODEL_NOTES[model];

  return (
    <SnippetProvider snippets={snippets} gitCommit={gitCommit} permalinkBase={permalinkBase}>
      <div className="mx-auto max-w-6xl space-y-8 px-4 py-8">
        <header className="max-w-3xl">
          <h1 className="text-3xl font-bold tracking-tight">KV cache</h1>
          <p className="mt-2 text-muted-foreground">
            Explore the difference between persistent decode KV and temporary attention tensors. The
            generation pool uses 16-token pages, while physical retention depends on the execution
            route. These diagrams are logical examples; their byte estimates are not allocator or
            GPU measurements.
          </p>
        </header>

        <ChoiceGroup
          label="Model for KV explanation"
          value={model}
          options={MODEL_CHOICES}
          onValueChange={setModel}
          buttonClassName="h-7 px-3 font-mono"
        />

        {model === "qwen3-embedding" ? (
          <div className="max-w-2xl rounded-lg border bg-muted/20 p-6">
            <h2 className="text-lg font-semibold">Qwen3 Embedding uses one causal forward pass</h2>
            <p className="mt-2 text-sm text-muted-foreground">
              The embedding graph computes temporary keys and values for causal attention over the
              input, pools the last valid hidden state, and normalizes the vector. It has no
              autoregressive decode loop or retained paged decode cache. Attention working memory
              still matters, especially for long inputs.
            </p>
          </div>
        ) : model === "gliner2" ? (
          <div className="max-w-2xl rounded-lg border bg-muted/20 p-6">
            <h2 className="text-lg font-semibold">GLiNER2 has no persistent decode KV cache</h2>
            <p className="mt-2 text-sm text-muted-foreground">
              KV caching exists to avoid recomputing keys and values for past tokens during
              autoregressive decode. GLiNER2 runs its DeBERTa encoder over the whole input in one
              bidirectional pass and emits spans. It computes temporary keys and values for
              attention, but has no autoregressive decode loop or persistent paged decode KV cache.
            </p>
          </div>
        ) : model === "gliner25" ? (
          <div className="max-w-2xl rounded-lg border bg-muted/20 p-6">
            <h2 className="text-lg font-semibold">
              GLiNER2.5 windows long documents — still no KV cache
            </h2>
            <p className="mt-2 text-sm text-muted-foreground">
              Like GLiNER2, GLiNER2.5 is an encoder: no autoregressive loop, no persistent paged
              decode KV cache. Where GLiNER2 stopped at a single 512-token pass, one GLiNER2.5 pass
              takes up to 4,096 body words — as many as 16,384 tokens. Longer documents, up to
              131,072 words, are cut into overlapping 4,096-word windows, and the schema header is
              re- encoded in front of each one. Attention keys and values live only for as long as
              their window. On the optimized Metal path the per-layer relative-position projections
              do stay resident for the session. Nothing else survives from one window or request to
              the next.
            </p>
          </div>
        ) : (
          trace &&
          note && (
            <section className="space-y-4">
              <div className="rounded-lg border bg-card p-4">
                <KvCacheBlocks key={model} trace={trace} initialStep={0} />
              </div>
              <div className="grid gap-4 lg:grid-cols-2">
                <div className="rounded-lg border p-4">
                  <h3 className="mb-2 text-sm font-semibold">{note.title}</h3>
                  <p className="text-xs text-muted-foreground">{note.body}</p>
                </div>
                <div className="rounded-lg border p-4">
                  <h3 className="mb-2 text-sm font-semibold">Bytes per block</h3>
                  <p className="text-xs text-muted-foreground">
                    The estimate assumes f16 K and V. The optional polar4 preset packs keys at four
                    bits and uses int8-style values with per-head scales. For 2 KV heads × 256
                    dimensions, the raw key payload in a 16-token page is 16 KiB in f16 or 4 KiB in
                    polar4. Total savings also depend on values, metadata, geometry, and backend
                    eligibility.
                  </p>
                </div>
              </div>
            </section>
          )
        )}

        <p className="text-xs text-muted-foreground">
          <CodeLink link={L("kv-manager")} /> · <CodeLink link={L("kv-sliding-window")} /> ·{" "}
          <CodeLink link={L("kv-turboquant-polar4")} /> · <CodeLink link={L("config-shared-kv")} />{" "}
          · <CodeLink link={L("generation-kv-config")} /> ·{" "}
          <CodeLink link={L("generation-kv-policy")} />
        </p>
      </div>
    </SnippetProvider>
  );
}
