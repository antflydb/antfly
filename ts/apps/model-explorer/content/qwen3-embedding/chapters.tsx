"use client";

import Link from "next/link";
import { CodeLink } from "@/components/code/code-link";
import { EnvFlagChip, QuantChip } from "@/components/primitives/chips";
import { Scene, ScrollyChapter } from "@/components/scrollytelling/scrolly";
import { L } from "@/lib/links";
import type { ChaptersProps } from "../registry";
import {
  BatchingWinsFigure,
  CosineFidelityFigure,
  LastTokenPoolFigure,
  TokenizerContrastFigure,
  VectorNotTokenFigure,
} from "./figures";

export function Qwen3EmbeddingChapters({ spec }: ChaptersProps) {
  return (
    <div>
      {/* ── Ch 1 · A decoder that outputs one vector ────────────────── */}
      <ScrollyChapter
        id="ch-1"
        number={1}
        title="A decoder that outputs one vector"
        intro={
          <p>
            {spec.stats.layers} layers, hidden {spec.stats.hidden}, GQA {spec.stats.queryHeads}/
            {spec.stats.kvHeads} — a perfectly ordinary Qwen3 decoder. Then it refuses to decode.
          </p>
        }
      >
        <Scene id="bend" graphic={<VectorNotTokenFigure />}>
          <p>
            An embedding request runs the whole prompt through the stack as one batched prefill and
            stops. There is no LM head matvec, no sampler, no second forward pass — the final hidden
            state exits sideways as a {spec.stats.hidden}-dimensional vector. The decode loop that
            dominates every other decoder page on this site simply never starts.
          </p>
          <p>
            The embedding pipeline wires{" "}
            <code>format task → tokenize → encode → pool → normalize</code>. On the eligible Metal
            path, a planned dense Qwen3 graph executes the encoder and keeps pooling on the backend;
            the shared GPT session is also available as a fallback. No persistent paged decode cache
            is needed. <CodeLink link={L("qwen3-embed-graph")} />
          </p>
          <p>
            <CodeLink link={L("embed-pipeline")} /> <QuantChip format="q8_0" />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 2 · BPE, not SentencePiece ───────────────────────────── */}
      <ScrollyChapter
        id="ch-2"
        number={2}
        title="Tokenization and the embedding task"
        intro="The model receives task-formatted text, byte-level BPE tokens and a trailing EOS."
      >
        <Scene id="tok" graphic={<TokenizerContrastFigure />}>
          <p>
            Qwen3 uses byte-level BPE (the <code>gpt2</code>-style vocab with the <code>qwen2</code>{" "}
            pre-tokenizer). Byte-level vocabulary coverage avoids ordinary out-of-vocabulary words.
            The figure contrasts representative whitespace conventions, not actual tokenizer output.
          </p>
          <p>
            The managed model profile formats queries as <code>Instruct: …\nQuery:…</code>;
            documents have an empty prefix. Choose the query or document task consistently when
            building and searching an index. The bundle declares last-token pooling and
            normalization, and the pipeline ensures a trailing EOS even when the tokenizer sidecar
            does not append one.
          </p>
          <p>
            <CodeLink link={L("tokenizer-hf")} /> · <CodeLink link={L("qwen3-embed-catalog")} /> ·{" "}
            <CodeLink link={L("embed-eos")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 3 · Last-token pooling ───────────────────────────────── */}
      <ScrollyChapter
        id="ch-3"
        number={3}
        title="Last-token pooling"
        intro={
          <p>
            The GGUF metadata says it in one key: <code>qwen3.pooling_type = 3</code>.
          </p>
        }
      >
        <Scene id="pool" graphic={<LastTokenPoolFigure step={0} />}>
          <p>
            <strong>Why the last token?</strong> In a causal model, only the final position has
            attended to everything before it — every earlier hidden state is blind to what follows.
            Mean pooling would mix positions with different visible prefixes; this model was trained
            for last-token pooling. The pipeline selects the last <em>non-padding</em> position in
            each row, which is the EOS after task formatting and any configured truncation.
          </p>
          <p>
            Pooling is part of each model's contract. Antfly resolves it from metadata or the model
            manifest; the same pipeline also implements mean and CLS pooling for models that request
            them. <CodeLink link={L("embed-pooling-strategy")} />
          </p>
        </Scene>
        <Scene id="norm" graphic={<LastTokenPoolFigure step={1} />}>
          <p>
            The pooled vector is L2-normalized onto the unit sphere. Downstream, cosine similarity
            becomes a dot product — which is exactly what a vector index wants to compute millions
            of times.
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 4 · Faithful at 8,192 tokens ─────────────────────────── */}
      <ScrollyChapter
        id="ch-4"
        number={4}
        title="Verify the vector and its input"
        intro="Quantized inference is only useful if the vectors still point the same way."
      >
        <Scene id="cosine" graphic={<CosineFidelityFigure />}>
          <p>
            A plausible vector can still represent the wrong input. Compare exact model artifacts,
            task prefixes, token IDs, EOS placement, active sequence length, pooling and
            normalization before interpreting cosine agreement. The runtime reads{" "}
            <code>qwen3.context_length</code> when present and resolves sequence limits from model
            configuration; a generic 512-token encoder default is not the Qwen3 contract.
          </p>
          <p>
            The illustrated 0.6B model has a {Number(spec.stats.context).toLocaleString("en-US")}
            -token architectural context. Serving limits and truncation may be smaller. The dated
            baseline records an 8,192-token truncation gate and other oracle, batch and retrieval
            checks; those results qualify their recorded artifact and build, not every deployment.{" "}
            <CodeLink link={L("embed-baseline")} />
          </p>
        </Scene>
      </ScrollyChapter>

      {/* ── Ch 5 · The batching path ────────────────────────────────── */}
      <ScrollyChapter
        id="ch-5"
        number={5}
        title="The batching path"
        intro={
          <p>
            Embedding is prefill-shaped: all T tokens arrive at once. The perf story is the opposite
            of decode — compute-dense matmuls, not memory-bound matvecs.
          </p>
        }
      >
        <Scene id="wins" graphic={<BatchingWinsFigure />}>
          <p>
            Several optimizations target this workload. <strong>Batched FFN</strong> runs the
            feed-forward projections as matrix operations over token rows; fused gate/up kernels
            reuse activation tiles and can include the SiLU/multiply epilogue. Q8_0 weights use
            shape-dependent simdgroup matmul routes, including SG-v2 and M64 schedules.{" "}
            <strong>Simdgroup flash attention</strong>: <code>sg_q16</code> tiles with online
            softmax, never materializing the <code>[T, T]</code> score matrix.{" "}
            <strong>F16-KV direct load</strong>: the attention kernel reads K/V in f16 natively,
            halving K/V storage bytes relative to f32 when that route is selected. The arithmetic
            inside those tiles is ordinary attention —{" "}
            <Link className="text-primary underline" href="/models/gemma4-e4b#ch-4">
              walked through with small numbers
            </Link>{" "}
            on the Gemma4 page.
          </p>
          <p>
            <CodeLink link={L("kernel-q8-mm-sg")} /> ·{" "}
            <CodeLink link={L("kernel-sg-q16-f16kv-gqa2")} /> ·{" "}
            <CodeLink link={L("embed-baseline")} />
          </p>
          <p>
            <EnvFlagChip
              name="TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION_Q16"
              defaultOn={false}
            />{" "}
            <EnvFlagChip
              name="TERMITE_METAL_DISABLE_DENSE_CAUSAL_SG_ATTENTION_F16KV"
              defaultOn={false}
            />
          </p>
          <p className="text-xs">
            Kernel selection depends on matrix rows (batch × token positions), precision and output
            width:{" "}
            <Link className="text-primary underline" href="/systems/kernels?batch=32&family=mm_sg">
              the mm_sg kernel family →
            </Link>
          </p>
        </Scene>
      </ScrollyChapter>
    </div>
  );
}
