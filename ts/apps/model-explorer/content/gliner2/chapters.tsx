"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip } from "@/components/primitives/chips";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  DisentangledScoresFigure,
  EncoderVsDecoderFigure,
  FusedKernelPairFigure,
  GlinerSpineFigure,
  LogBucketFigure,
  SchemaExtractionFigure,
  SpanHeadFigure,
} from "./figures";

export function Gliner2Chapters({ spec }: ChaptersProps) {
  return (
    <div>
      {/* ── Ch 1 · Extraction by schema, not prompt ─────────────────── */}
      <ScrollyChapter
        id="ch-1"
        number={1}
        title="Schema-conditioned extraction"
        intro="Label names and source text enter the same encoder sequence."
      >
        <Scene id="schema" graphic={<SchemaExtractionFigure />}>
          <p>
            The input is a list of label names — <code>company</code>, <code>person</code>,{" "}
            <code>amount</code>, anything — encoded into the <em>same</em> token sequence as the
            text, separated by entity-marker tokens. One encoder pass later, every candidate span in
            the text has a score against every label. The spans above light up when their sigmoid
            score crosses the threshold; the example is illustrative, the mechanism is exactly what
            chapters 3–5 unpack.
          </p>
          <p>
            No autoregressive generation is needed. Zero-shot extraction uses the meaning of the
            supplied label names without training a new output class for each label. Label wording
            still matters: the schema is part of the encoded input, and changing it can change the
            results.
          </p>
          <p>
            <CodeLink link={L("gliner-pipeline")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 2 · An encoder in a decoder world ────────────────────── */}
      <ScrollyChapter
        id="ch-2"
        number={2}
        title="An encoder in a decoder world"
        intro={
          <p>
            Generative models decode token by token; Qwen3 Embedding uses a causal forward pass
            without generation. GLiNER2 uses a bidirectional {spec.stats.layers}-layer, hidden-
            {spec.stats.hidden} DeBERTa-v3 <em>encoder</em> — and that changes what the runtime even
            needs.
          </p>
        }
      >
        <Scene id="mask" graphic={<EncoderVsDecoderFigure emphasis="mask" />}>
          <p>
            <strong>No causal mask.</strong> Every token attends to every other token in both
            directions. For extraction that's not a nicety, it's the point: whether <em>"Ruiz"</em>{" "}
            is a person may hinge on the <em>"Dr."</em> before it and the <em>"hired"</em> before
            that — and on words that come after. The encoder attention mask handles padding;
            separate span masks exclude invalid candidates.
          </p>
        </Scene>
        <Scene id="nokv" graphic={<EncoderVsDecoderFigure emphasis="kv" />}>
          <p>
            <strong>No decode loop, so no KV cache.</strong> The model runs one batched forward pass
            over <code>[B·T]</code> tokens (the illustrated base model has a {spec.stats.context}
            -token limit, including schema tokens). There are no paged pools, no eviction, no
            sampler — the KV and sampling stages of the shared spine simply don't exist for this
            model.
          </p>
          <p>
            Antfly reuses session management, backend operations, and graph planning for this
            encoder. The forward graph has no persistent decode-cache edges.{" "}
            <CodeLink link={L("gliner-deberta-graph")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 3 · Disentangled attention ───────────────────────────── */}
      <ScrollyChapter
        id="ch-3"
        number={3}
        title="Disentangled attention: content and position, separately"
        intro={
          <p>
            DeBERTa's core idea: don't fold position into the token embedding. Keep{" "}
            <em>what a token is</em> and <em>where it is</em> as separate vectors, and let attention
            combine them three ways.
          </p>
        }
      >
        <Scene id="c2c" graphic={<DisentangledScoresFigure highlight="c2c" />}>
          <p>
            <strong>C2C</strong> is ordinary attention: content query dot content key,{" "}
            <code>Q_c · K_cᵀ</code> — the score/softmax/weighted-sum recipe{" "}
            <Link className="text-primary underline" href="/models/gemma4-e4b#ch-4">
              walked through with small numbers
            </Link>{" "}
            on the Gemma4 page. Without any positional terms, attention cannot distinguish reordered
            content by its position alone: permuting the input permutes the outputs rather than
            teaching the model word order.
          </p>
        </Scene>
        <Scene id="c2p" graphic={<DisentangledScoresFigure highlight="all" />}>
          <p>
            <strong>C2P and P2C</strong> add position back — from both sides. Content-to-position
            asks "how much does <em>this word</em> care about something <em>k slots away</em>?";
            position-to-content asks the reverse. The three score matrices sum and scale by{" "}
            <code>1/√(3·d)</code> before softmax.
          </p>
          <p>
            Relative-position <em>indices</em> repeat along diagonals; the scores also depend on the
            query or key content, so their diagonals need not be equal. The score-gather formulation
            uses a <code>[T × (2T−1)]</code> product per side and gathers by <code>qi−ki+T−1</code>.
            The fused graph path instead consumes content and relative projections directly.
          </p>
          <p>
            <CodeLink link={L("gliner-fused-attn-gate")} />
          </p>
        </Scene>
        <Scene id="buckets" graphic={<LogBucketFigure />}>
          <p>
            <strong>Distance is bucketed.</strong>{" "}
            <code>position_buckets = {String(spec.stats.relBuckets)}</code> is the signed-distance
            offset; the relative embedding table has 512 rows. Distances within ±128 map exactly;
            larger magnitudes use logarithmic buckets, shifted by 256 into table indices. Word order
            nearby is preserved precisely; far context blurs gracefully. And because position lives
            entirely inside attention, there are <em>no absolute position embeddings at all</em> —
            the embedding stage is lookup, LayerNorm, mask, nothing else.
          </p>
          <p>
            <CodeLink link={L("gliner-rel-bucket")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 4 · One kernel, forward and backward ─────────────────── */}
      <ScrollyChapter
        id="ch-4"
        number={4}
        title="Fused attention and custom VJPs"
        intro={
          <p>
            The attention core can run as a fused Metal forward kernel. Training adds a custom
            backward operation implemented by several gradient kernels.
          </p>
        }
      >
        <Scene id="fused" graphic={<FusedKernelPairFigure />}>
          <p>
            <code>termite_disentangled_relative_attention_f32</code> (with a <code>_flash4</code>{" "}
            tiled variant) combines the attention score, softmax and context stages. Matching{" "}
            <code>…_bwd_scores_f32</code>, <code>…_bwd_dv_f32</code>, <code>…_bwd_dq_dk_f32</code>{" "}
            and <code>…_bwd_dqr_dkr_f32</code> kernels implement the gradient calculation in{" "}
            <em>training</em>. Antfly connects these through a custom VJP (vector-Jacobian product)
            for the fused graph node; this is multiple backward dispatches, not one forward/backward
            kernel pair.
          </p>
          <p>
            The figure compares a conceptual decomposition with Antfly's fused implementation; it is
            not a profiler trace or a benchmark against another framework. Forward inference and
            fine-tuning have different execution and memory requirements.{" "}
            <CodeLink link={L("gliner-fused-attn-kernel")} />
          </p>
          <p>
            <CodeLink link={L("gliner-fused-attn-bwd-kernel")} />{" "}
            <EnvFlagChip name="TERMITE_METAL_DISABLE_DEBERTA_FLASH_ATTENTION" defaultOn={false} />
          </p>
          <p className="text-xs">
            How these gradients drive a real finetune:{" "}
            <Link className="text-primary underline" href="/training/gliner2">
              Training → GLiNER2
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 5 · The span head ────────────────────────────────────── */}
      <ScrollyChapter
        id="ch-5"
        number={5}
        title="The span head: score everything at once"
        intro="Word and label features feed separate heads before span-label scoring."
      >
        <Scene id="gather" graphic={<SpanHeadFigure step={0} />}>
          <p>
            The schema's label names went through the encoder alongside the text, so their
            representations are <em>contextualized against this document</em>. The head gathers them
            back out — hidden states at every label-marker position — <code>[E]</code>,{" "}
            <code>[C]</code> or <code>[R]</code> — while token states pool into word
            representations. The serving head takes the first sub-token per word, including in its
            dedicated Metal kernel. The separate graph-training helper uses averaging, so these
            source paths should not be treated as identical implementations.
          </p>
          <p>
            <CodeLink link={L("gliner-serving-head")} /> ·{" "}
            <CodeLink link={L("gliner-word-embeddings-kernel")} />
          </p>
        </Scene>
        <Scene id="spans" graphic={<SpanHeadFigure step={1} />}>
          <p>
            <strong>Every candidate span, materialized cheaply.</strong> Word reps run through{" "}
            <code>project_start</code> and <code>project_end</code> MLPs; for each span up to{" "}
            {String(spec.stats.maxSpanWidth)} words wide, its start and end vectors concatenate to{" "}
            <code>[total_spans, 2H]</code>, ReLU, and project back down to a span representation.
            Labels get a parallel treatment: one GRU step and a residual add, followed by a
            downscaled mini-transformer and an output MLP. The GRU combine has a dedicated kernel.
          </p>
          <p>
            <CodeLink link={L("gliner-span-marker")} /> ·{" "}
            <CodeLink link={L("gliner-gru-combine-kernel")} />
          </p>
        </Scene>
        <Scene id="score" graphic={<SpanHeadFigure step={2} />}>
          <p>
            Then the whole extraction problem collapses into <code>span_rep @ label_projᵀ</code> —
            one <code>[S × L]</code> matmul, a sigmoid, a threshold. Those are the highlights from
            chapter 1: candidate span-label pairs score in parallel. The matrix has S×L entries, so
            adding labels increases head work and also lengthens the encoder input. By default, flat
            NER then removes overlapping spans, keeping higher-scoring candidates; thresholding
            alone is not the final output.
          </p>
          <p>
            <CodeLink link={L("gliner-score-spans")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 6 · The shared spine ─────────────────────────────────── */}
      <ScrollyChapter
        id="ch-6"
        number={6}
        title="Where GLiNER2 rides the shared spine"
        intro="Different model class, same machinery."
      >
        <Scene id="spine" graphic={<GlinerSpineFigure />}>
          <p>
            GLiNER2 comes in through the same server, session manager and tokenizer as every decoder
            here, and the same planner batches its encoder layers into Metal frames. Where they are
            enabled, its Q4_K linear layers can also run through the generated quantized- kernel
            routes. What it skips, it skips structurally: no KV stage, no sampler.
          </p>
          <p>
            Batching creates wider matrix operations, but padding, sequence length and head work
            affect efficiency. The CPU/GPU crossover is hardware- and workload-dependent; this
            walkthrough does not establish a performance threshold. Entity extraction is the path
            illustrated here; classification and relation extraction use additional task-specific
            pipeline logic. Its successor,{" "}
            <Link className="text-primary underline" href="/models/gliner25">
              GLiNER2.5
            </Link>
            , replaces the span grid with boundary proposals and a shared candidate pool.
          </p>
          <p className="text-xs">
            The spine in full:{" "}
            <Link className="text-primary underline" href="/runtime">
              runtime walkthrough →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
