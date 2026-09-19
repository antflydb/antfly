"use client";

/**
 * Gemma4 chapters 2–6: tokens in, PLE, attention, KV, and the MoE cousin.
 */
import { useMemo } from "react";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip, QuantChip } from "@/components/primitives/chips";
import { Divergence, Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { KvCacheBlocks } from "@/components/viz/kv-cache-blocks";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  AttentionBlockFigure,
  EmbedLookupFigure,
  GqaGroupingFigure,
  KvExtrasFigure,
  MoeForkJoinFigure,
  MoeResidencyFigure,
  MoeRoutingFigure,
  makeGemmaKvTrace,
  PleCostFigure,
  PleRibbonFigure,
  RangeMaskFigure,
  SessionRoutingFigure,
  TokenPiecesFigure,
} from "./figures-early";

export function Gemma4EarlyChapters({ spec }: ChaptersProps) {
  const isE4b = spec.id === "gemma4-e4b";
  const layers = Number(spec.stats.layers);
  const hidden = Number(spec.stats.hidden);
  const kvTrace = useMemo(() => makeGemmaKvTrace(isE4b), [isE4b]);

  return (
    <div>
      {/* ── Ch 2 · Tokens in ─────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-2"
        number={2}
        title="Tokens in"
        intro="Before any tensor exists, a string becomes ids and a request becomes a session."
      >
        <Scene id="pieces" graphic={<TokenPiecesFigure />}>
          <p>
            The prompt is split by a SentencePiece tokenizer into pieces from a 262,144-entry
            vocabulary. Words can span several pieces; the split and ids shown here are
            illustrative, not the output of a tokenizer run. These chapters follow text decoding;
            multimodal inputs add projected image or audio features.
          </p>
          <p>
            <CodeLink link={L("tokenizer-sentencepiece")} />
          </p>
        </Scene>
        <Scene id="routing" graphic={<SessionRoutingFigure />}>
          <p>
            The HTTP handler hands the request to the session factory, which reads the model
            manifest and resolves it to <code>ModelFamily.gemma</code> — one tag in the unified GPT
            config. The generation pipeline and GPT architecture are shared with other decoder
            families, while Gemma-specific helpers and Metal lowerers implement PLE, shared KV,
            channel handling, and prepared decode paths.
          </p>
          <p>
            <CodeLink link={L("server-chat")} /> · <CodeLink link={L("session-factory")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 3 · Embeddings, and then embeddings again ─────────── */}
      <ScrollyChapter
        id="ch-3"
        number={3}
        title="Embeddings, and then embeddings again (PLE)"
        intro={
          <p>
            Gemma4 looks its tokens up twice: once for the hidden state, and once for a per-layer
            embedding lane that supplies a distinct learned input to each layer.
          </p>
        }
      >
        <Scene id="lookup" graphic={<EmbedLookupFigure hidden={hidden} />}>
          <p>
            The ordinary part first: the token id selects one row of the embedding table and becomes
            a {hidden}-wide hidden state, multiplied by the configured embedding scale (Gemma uses{" "}
            <code>√{hidden}</code>).
          </p>
        </Scene>
        <Scene id="ple" graphic={<PleRibbonFigure layers={layers} />}>
          <p>
            <strong>Then the second lookup.</strong> Gemma4's per-layer embeddings (PLE) run a token
            lookup alongside a projection of the initial hidden state. The projection is normalized
            in 256-wide chunks and combined with the scaled token embeddings. The resulting vector
            is sliced across all {layers} layers, each receiving its own gated slice. It's part of
            the model — skipping it means running a different network.
          </p>
          <p>
            <CodeLink link={L("gpt-compute-ple")} /> · <CodeLink link={L("config-ple-hidden")} />
          </p>
        </Scene>
        <Scene id="cost" graphic={<PleCostFigure isE4b={isE4b} />}>
          <p>
            <strong>PLE is not free.</strong> The <code>per_layer_model_proj</code> matvec has{" "}
            {isE4b ? "10752×2560" : "8960×1536"} weights. The qualified Metal path stages eligible
            dense slots to <QuantChip format="q8_0" /> at load time, default-on:{" "}
            {isE4b
              ? "about 55.1 MB in BF16 becomes 29.2 MB in Q8_0, a 25.8 MB weight-read reduction per matvec"
              : "about 55.1 MB in F32 becomes 14.6 MB in Q8_0, a 40.4 MB weight-read reduction per matvec"}
            . These are tensor-size calculations, not measured memory traffic. Historical probes
            retained identical greedy token ids; quantization does not guarantee that for every
            prompt.
          </p>
          <p>
            <EnvFlagChip name="TERMITE_METAL_DISABLE_PLE_MODEL_PROJ_Q8" defaultOn={false} />
          </p>
          <Divergence
            others={<p>a conventional decoder embedding initializes the hidden state once.</p>}
            antfly={
              <p>
                Gemma adds a per-layer input lane; its model projection can be quantized while
                preserving the architectural operation.
              </p>
            }
            link={<CodeLink link={L("gpt-compute-ple")} />}
          />
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 4 · Attention ─────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-4"
        number={4}
        title="Attention: windows, groups, and norms in the right places"
        intro={
          <p>
            One attention block, taken apart: Gemma4 puts RMS norms on the Q and K heads themselves;
            eligible Metal routes fuse each head norm with its RoPE operation.
          </p>
        }
      >
        <Scene id="block" graphic={<AttentionBlockFigure isE4b={isE4b} />}>
          <p>
            After the QKV projection, each query and key head is RMS-normalized <em>per head</em> —
            controlling the scale of attention logits. Antfly can lower each norm-plus-RoPE pair as
            a fused <code>⟨head_rms ⋄ rope⟩</code> dispatch. Q and K are separate calls; this is not
            one combined Q/K dispatch.
          </p>
          <p>
            <CodeLink link={L("gpt-qk-head-norm")} /> ·{" "}
            <CodeLink link={L("kernel-head-rms-rope")} />{" "}
            <EnvFlagChip name="TERMITE_METAL_DISABLE_A4B_HEAD_NORM_ROPE_FUSION" defaultOn={false} />
          </p>
        </Scene>
        <Scene id="gqa" graphic={<GqaGroupingFigure isE4b={isE4b} />}>
          <p>
            <strong>Grouped-query attention.</strong> {spec.displayName} runs 8 query heads against{" "}
            {isE4b ? "2 KV heads" : "a single KV head"} — {isE4b ? "4 queries" : "all 8 queries"}{" "}
            share each K/V bank. Relative to eight distinct KV heads of the same width, that reduces
            KV elements by {isE4b ? "4×" : "8×"}. Its effect on total latency depends on context
            length, weight traffic, and the selected attention kernel.
          </p>
        </Scene>
        <Scene id="mask" graphic={<RangeMaskFigure pattern={isE4b ? 6 : 5} />}>
          <p>
            <strong>The two masks.</strong> A sliding layer may only read a fixed trailing window of
            the sequence; every {isE4b ? "sixth" : "fifth"} layer can read the full causal history.
            The layer types also differ in head dimension and RoPE settings. Attention dispatch
            depends on those shapes, context length, and route policy.
          </p>
          <p>
            <CodeLink link={L("config-layer-uses-sliding")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 5 · The KV cache is a filing system ───────────────── */}
      <ScrollyChapter
        id="ch-5"
        number={5}
        title="The KV cache is a filing system"
        intro={
          <p>
            Drag the slider through a schematic of KV ownership and retention. The replay uses
            16-token blocks and a shortened window; it is not a runtime allocation trace.
          </p>
        }
      >
        <Scene
          id="alloc"
          graphic={
            <div className="flex h-full flex-col">
              <KvCacheBlocks trace={kvTrace} initialStep={96} />
            </div>
          }
        >
          <p>
            The general KV manager uses <code>page_size_tokens = 16</code>, and pages may pack
            several layers. This drawing separates logical layer groups to show which ones write
            K/V. It does not represent the exact number or layout of physical allocations.
          </p>
          <p>
            <CodeLink link={L("kv-pool-config")} />
          </p>
        </Scene>
        <Scene
          id="evict"
          graphic={
            <div className="flex h-full flex-col">
              <KvCacheBlocks trace={kvTrace} initialStep={288} />
            </div>
          }
        >
          <p>
            <strong>Sliding and global retention differ.</strong> Eligible Gemma Metal routes use a
            bounded sliding ring alongside full-history global storage. The general layer-packed
            pool keeps full history for mixed attention models so global layers retain their
            context. Eviction in this drawing illustrates the split retention policy.
          </p>
          <p className="text-xs text-muted-foreground">
            The replay draws a 128-token window so eviction is visible; model and route
            configuration determine actual capacity. The displayed byte count is a schematic
            estimate.
          </p>
        </Scene>
        <Scene
          id="shared"
          graphic={
            <div className="flex h-full flex-col">
              <KvCacheBlocks trace={kvTrace} initialStep={288} />
            </div>
          }
        >
          <p>
            <strong>The third lane never fills.</strong> The {spec.stats.sharedKv} shared-KV tail
            layers produce no new K/V — they read the last non-shared donor of the same attention
            type. The empty lane represents that logical ownership, not a claim that every backend
            allocates zero bytes for shared layers.
          </p>
          <p>
            <CodeLink link={L("config-shared-kv")} /> · <CodeLink link={L("kv-manager")} />
          </p>
        </Scene>
        <Scene id="extras" graphic={<KvExtrasFigure />}>
          <p>
            The runtime also has prompt-prefix reuse and selectable KV codecs. Polar4 stores packed
            4-bit keys with INT8 values and scale overhead; it is not a 4-bit K-and-V cache. These
            features depend on the selected backend and configuration.
          </p>
          <p>
            <CodeLink link={L("kv-prompt-cache")} /> · <CodeLink link={L("kv-turboquant-polar4")} />
          </p>
          <Divergence
            others={<p>contiguous full-history storage reserves space for every retained token.</p>}
            antfly={
              <p>
                paging, split sliding/global retention, KV sharing, and compression are distinct
                mechanisms with separate eligibility checks.
              </p>
            }
            link={<CodeLink link={L("kv-manager")} />}
          />
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 6 · The MoE cousin ────────────────────────────────── */}
      <ScrollyChapter
        id="ch-6"
        number={6}
        title="The MoE cousin: Gemma4 26B-A4B"
        intro={
          <p>
            To be clear: {spec.displayName} is dense — every FFN runs for every token. But the same
            runtime serves Gemma4's 26B-A4B mixture-of-experts variant, and its machinery leaks
            useful ideas back into the dense path.
          </p>
        }
      >
        <Scene id="route" graphic={<MoeRoutingFigure />}>
          <p>
            The qualified A4B architecture has 30 layers, hidden size 2816, and 128 routed experts
            per layer. Its router selects the top 8 for each token. A shared feed-forward branch
            also contributes; most routed expert weights are inactive for that token.
          </p>
        </Scene>
        <Scene id="residency" graphic={<MoeResidencyFigure />}>
          <p>
            <strong>Residency depends on the memory budget.</strong> The Metal path picks an{" "}
            <code>A4bMappedMoeRoute</code>: a qualified high-memory route keeps mapped expert
            weights available to the GPU; streamed routes stage selected weights as needed. Apple
            Silicon uses unified memory, so this is about mapping, residency, and staging, not
            separate physical CPU and GPU RAM.
          </p>
          <p>
            <CodeLink link={L("moe-mapped-route")} />{" "}
            <EnvFlagChip name="TERMITE_METAL_DISABLE_A4B_FUSED_GATE_UP" defaultOn={false} />
          </p>
        </Scene>
        <Scene id="forkjoin" graphic={<MoeForkJoinFigure />}>
          <p>
            The planner describes the A4B decode-FFN as an explicit fork/join: shared-expert and
            routed-expert paths as named resources that join back into the layer output. Today it
            deliberately stops at planning — a typed description no encoder consumes yet — which is
            how this codebase grows: the shape first, the lowering when it's earned.
          </p>
          <p>
            <CodeLink link={L("planner-a4b-forkjoin")} />
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
