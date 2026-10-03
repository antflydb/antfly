# Laya

Laya is the open, encoder-only typed-decision model that Antfly serves through
`POST /ai/v1/extract` (`choice`, `score`, and boolean `noul` questions). This
document covers **tree-packed decisions**: an execution layout where many
questions about one state share a single encoding of that state. It records
the evidence behind the design, the design itself, the training methodology,
and how the implementation is verified.

The released checkpoints, integration placement, and their CPU/Metal/CUDA
qualification are described in
[`docs/design/laya-support-investigation.md`](../../../../../docs/design/laya-support-investigation.md)
and [`docs/design/laya-qualification.md`](../../../../../docs/design/laya-qualification.md).
Tree packing changes neither: a released checkpoint keeps `packing: none` and
runs exactly as before.

## Summary

The released Laya encodes one sequence per question:
`[CLS] question [SEP] options [SEP] state [SEP]`. The state is re-encoded for
every question, and the question, options, and state share a 512-token budget,
with 192 tokens reserved for question and options. Tree packing puts the state
in a shared **trunk** that attends only to itself. Each question is a
**branch** that attends to the trunk and to itself. In candidate mode each
option is a further branch under its question. Positions restart after the
parent at every branch. Three properties follow, and each has a test:

- The trunk encoding is independent of the questions, so it is computed once
  per row and is bit-identical across rows.
- Questions (and, in candidate mode, sibling options) cannot influence one
  another. A question's decision is the same packed with others or alone.
- Every root-to-leaf path is laid out exactly like the unpacked sequence
  `[trunk; question; option]`. A single-segment row reproduces the existing
  ModernBERT encoder exactly.

On the released 421M checkpoint (Apple M4 Max, ReleaseFast), sixteen questions
about a ~400-token state take **1,545 ms unpacked and 200 ms packed on Metal**
and **15.6 s unpacked and 2.1 s packed on CPU**. Processed tokens fall from
6,587 to 798. With 64 questions and a cached state, packed takes 382 ms
against 6.1 s unpacked. Attention is segment-masked, so work follows the keys
each token can see, and a state cache reuses a state across requests.

Packing changes what the state tokens can see: they no longer attend to the
question. A packed model therefore needs fine-tuned weights. The native
trainer supports this, starting from a released checkpoint and optionally
distilling from it (see [Training methodology](#training-methodology)).
On LocalLLaMA/typed-decisions, packed question mode loses accuracy at equal
training budget: 0.450 against 0.621 unpacked over three seeds (see
[Packed vs unpacked at equal budget](#packed-vs-unpacked-at-equal-budget-2026-09-26)).
An earlier single-seed parity result did not hold up. Candidate mode learns
77-way Banking77 (0.819), which the unpacked layout cannot express.

## Evidence and motivation

This design came from comparing Laya with TypeSafe's Jev. Jev is a closed
model, so all Jev figures below are third-party or vendor claims, most of them
days old when collected (2026-09-23). Treat them as approximate. Inferences
drawn in this document are labelled as such.

### Jev (TypeSafe, closed weights, API only)

| Property | Value | Source |
| --- | --- | --- |
| Input limit | 64k tokens per request; state plus the *longest* question must fit in 32k | [OpenRouter Jev docs](https://openrouter.ai/docs/guides/community/jev), [OpenTweet limits](https://opentweet.io/jev/limits), [Experiential](https://platform.experientiallabs.ai/models/jev-latest), [Jev AI Guide](https://jevaiguide.com/errors/max-tokens-exceeded/) |
| Adding questions | "20 short yes/no questions added about 256 tokens"; no published maximum question count | [OpenTweet limits](https://opentweet.io/jev/limits) |
| Options per choice | up to 255 | [convaiinnovations/laya model card](https://huggingface.co/convaiinnovations/laya) |
| Latency, one question | 236–276 ms p50 (independent measurements cited on the Laya card) | [Laya model card](https://huggingface.co/convaiinnovations/laya) |
| Price | $0.042 per million input tokens; output free; text only | [OpenRouter Jev 1.13](https://openrouter.ai/typesafe/jev-1.13), [OpenTweet limits](https://opentweet.io/jev/limits) |
| Positioning | "System One" model returning typed decisions rather than text | [Tom's Hardware](https://www.tomshardware.com/tech-industry/artificial-intelligence/typesafe-ais-jev-offers-an-alternative-to-llms-that-claims-to-be-193x-faster-and-445x-cheaper-system-one-type-model-is-bespoke-for-probabilistic-decision-making), [dev.to guide](https://dev.to/valyuai/how-to-use-jev-a-practical-guide-to-typesafes-system-one-model-g5e) |

TypeSafe has not published an architecture. The launch material names a "new
model architecture" and a "parallel sampler" but, per the analysis below,
publishes no attention operator, sampler algorithm, or ablation.

### Laya (open reproduction, Apache-2.0)

| Property | `laya` (English) | `laya-multilingual`, `laya-typed-decisions` | Source |
| --- | --- | --- | --- |
| Encoder | ModernBERT-large (421M total) | mmBERT-base / ModernBERT-large | [model cards](https://huggingface.co/convaiinnovations/laya), [investigation](../../../../../docs/design/laya-support-investigation.md) |
| Sequence / question-head budget | 512 / 192 | 1,024 / 256 | `rl_agent_config.json` in each repository |
| State room | ~320 tokens | ~768 tokens | derived |
| Latency, one question | 32.8 ms (GPU) | | [Laya model card](https://huggingface.co/convaiinnovations/laya) |
| Banking77 (77 options) | 0.425 vs Jev 0.870 | | [Laya model card](https://huggingface.co/convaiinnovations/laya) |
| typed-decisions argmax / soft accuracy | | 0.766 / 0.471 vs Jev 0.727 / 0.580 | [laya-typed-decisions](https://huggingface.co/convaiinnovations/laya-typed-decisions), [Luni/laya-jev-benchmark](https://huggingface.co/datasets/Luni/laya-jev-benchmark) |

The Laya card attributes the Banking77 gap to its layout: options share a
fixed `head_max_len` budget, so "77 options receive only ~3 to 4 tokens per
label". The Luni benchmark also notes that both models trail Claude Haiku 4.5
by about 20 points on its phishing set, and that the fine-tuned Laya exceeds
the 0.735 teacher-agreement ceiling.

### Open reproductions and how they share the state

| Model | Backbone | State sharing | Exact and reusable? | State sees the question? |
| --- | --- | --- | --- | --- |
| [Laya](https://huggingface.co/convaiinnovations/laya) | ModernBERT encoder | none: re-encoded per question | — | yes |
| [AlexWortega/openjev](https://huggingface.co/AlexWortega/openjev) | Qwen3.5 0.8B–35B, 3-way NLI head | causal prefix caching: "compute the common token prefix once, then score every hypothesis in one batched continuation" (recurrent-layer state is copied per suffix) | yes (causal) | no |
| [com-kotobalabs/open-jev-deberta-v3-large](https://huggingface.co/com-kotobalabs/open-jev-deberta-v3-large) | DeBERTa-v3-large | one pass over `[CLS] [STATE] state [Q] … [OPT] … [SEP]`, all questions together, state capped at 256 of 512 tokens | no: depends on the whole question set | yes, all questions at once |
| [ZefanCai/Open-Jev-9B](https://huggingface.co/ZefanCai/Open-Jev-9B) | Qwen3.5-9B LoRA + scalar head | not stated; "4,096 tokens per independently scored candidate" | — | — |
| [di-zhang-fdu/jevre](https://huggingface.co/di-zhang-fdu/jevre) ("MoJev") | Qwen3.5-0.8B, 16k training truncation | tree attention mask over a packed sequence | yes | no |
| **Antfly tree-packed Laya** (this design) | ModernBERT encoder | tree attention mask over a packed sequence | yes, exact | no |

[featherless-ai/simple-jev](https://github.com/featherless-ai/simple-jev)
serves open models, including Laya, behind a Jev-style endpoint.

### The tree-mask hypothesis

[What is RLCD: the secret behind Jev](https://di-zhang-llm.github.io/blog/what-is-rlcd-the-secret-behind-jev/#why-jev-can-run-in-parallel)
argues that "Jev's parallel sampler is sequence packing plus an attention
mask". It packs `Z = [S; Q1; C1,1 … C1,K; Q2; …]` and applies a tree mask. A
question reads the shared state and itself. A candidate reads the shared
state, its own question, and its own tokens. Position IDs reset so that "all
questions start after the same state prefix, and all candidates under a
question start after the same state-plus-question prefix". Its readout
mean-pools the state, question, and candidate spans. It scores each candidate
with a rank-512 bilinear utility and applies a softmax. The post describes
RLCD as Plackett–Luce preference loss plus a Brier calibration constraint. It
attributes a two-stage procedure for very high-cardinality choices to
TypeSafe: "score candidates independently, then make an explicit choice". The
author's reconstruction is the `jevre` checkpoint above. The post is
explicitly a hypothesis: it has no access to Jev's weights.

**Inference (ours):** the hypothesis explains Jev's published limits. With
positions reset per branch, the largest position is the state plus the
longest single branch, which is the 32k "state plus the longest question"
limit. The packed sequence is the 64k request limit. Adding a question costs
only its branch, which is the "20 yes/no questions ≈ 256 tokens" observation.
Isolated candidate branches explain 255 options without a shared option
budget.

### Relation to reranker architectures

Laya is a cross-encoder with the reranker roles reversed. A reranker scores
one short query against many long passages, one forward per (query, passage)
pair, and its scores are only meaningful as a ranking. Laya scores one long
state against many short options inside one sequence, and returns a
calibrated distribution. Two consequences shaped this design:

- **Reranker data would not close the gaps above.** Laya's gaps against Jev
  are state length and option count. Reranking pairs are short passages, so
  they do not train long context, and they do not change the shared option
  budget. They are still useful for a relevance-style `noul`, and our
  rerankers can supply soft labels.
- **The reranker trick worth borrowing is amortization.** Encoding the query
  once and pairing it with many passages is the same idea as encoding the
  state once per row. Tree packing does this without an autoregressive
  backbone.

## Design

### Layout

One packed row holds one state and any number of questions about it.

```
positions:  0 ............ T-1 | T ......... T+B-1 | T ......... T+B'-1 | ...
tokens:     [CLS] state [SEP]   | [CLS] q1 [SEP] opts [SEP] | [CLS] q2 [SEP] opts [SEP] | ...
segment:    0 (trunk)           | 1 (parent 0)              | 2 (parent 0)              | ...
```

- **Trunk (segment 0):** `[CLS] state [SEP]`, positions `0..T-1`, kind −1.
- **Question mode:** each question is one branch,
  `[CLS] <type> question: <instruction> [SEP] ([MASK] option)* [SEP]`,
  with upstream's option formatting and its `head_max_len` budget
  (`laya.questionTokens`). Its options share the branch and see each other,
  as upstream's options do.
- **Candidate mode:** each question branch is `[CLS] <type> question:
  <instruction> [SEP]`, capped at `head_max_len`. Each option is a child
  branch, `[MASK]` followed by at most 48 option tokens, and every child
  starts at the same position. Options never see each other and no longer
  share a budget, so a question may have up to 255 options.
- **Visibility:** a token attends to a key exactly when the key's segment is an
  ancestor of, or equal to, the token's own segment
  (`laya_tree.Row.visible`). The same mask applies to the encoder and to the
  decision head's TransformerEncoder layers.
- **Sliding window:** ModernBERT's local layers apply their ±64 window to
  *logical* positions, so each path sees exactly the window it would see
  unpacked.
- **Type embedding:** each branch token adds the embedding of its question's
  type. Trunk tokens add none, because they are shared across types.
- **Decisions:** each option's `[MASK]` marker is scored by the existing
  scorer. The action head's features read the branch's `[CLS]` anchor instead
  of the sequence's first token.

`max_len` still bounds the **logical** length: the trunk plus the longest
path. A new `max_packed_len` bounds the **physical** row. Its default is
`min(4 * max_len, 32768)` and it is capped at 32,768. Attention is
segment-masked (see below), so no `[L, L]` state limits the row.
packer splits them greedily and repeats the trunk
(`laya_tree.build`). A state that leaves no logical room for some question's
branch is rejected with `ExtractionTextLimitExceeded`, never truncated.

### Configuration

```json
"laya": {
  "max_len": 512,
  "head_max_len": 192,
  "packing": { "mode": "question", "max_packed_len": 2048 }
}
```

`mode` is `none` (default), `question`, or `candidate`. Unknown keys and
out-of-range lengths are rejected. The extraction API admits up to 255 labels
per question. The model enforces its own limit: 20 unless the model is
candidate-packed.

`packing.two_stage` (candidate mode only; see
[Two-stage choice](#two-stage-choice-roadmap-2b)) shortlists a `choice`
question's options before comparing the survivors jointly:

```json
"packing": {
  "mode": "candidate",
  "max_packed_len": 2048,
  "two_stage": { "top_k": 8, "mass_cutoff": 0.9 }
}
```

`top_k` (2–255, required to enable it) is the largest shortlist; `mass_cutoff`
(0–1, optional) can stop shortlisting earlier once that much of stage 1's
probability mass is captured, never below 2 finalists. Disabled by default
(`top_k` absent, equivalent to `top_k: 0`).

`"weight_quantization": "q8_0"` (or `ANTFLY_LAYA_WEIGHT_QUANT=q8_0`) serves
the encoder and decision-head linear weights as Q8_0, quantized from the
dense checkpoint at load (`weight_source.quantizeDenseQ8_0`). Embeddings,
norms, the type embedding, the scorer and the action head stay dense. See
[Weight quantization](#weight-quantization-step-1d).

### Runtime

| Piece | Location |
| --- | --- |
| Packer, visibility, masks, row validation, multi-row coalescing | `src/pipelines/laya_tree.zig` |
| Pipeline: group tasks by state text, batch rows into session calls, request order preserved | `src/pipelines/laya.zig` (`executePacked`) |
| Session contract and one-row (or one merged multi-row) forward | `src/architectures/laya_packed.zig` |
| Encoder with logical positions and per-layer masks | `src/architectures/modern_bert.zig` (`forwardPackedCT`) |
| Decision head with tree mask and anchor features | `src/architectures/laya_head.zig` (`forwardPacked`) |
| State cache (trunk keys and values across rows and requests) | `src/architectures/laya_trunk_cache.zig`, `laya_packed.forwardRow` |

The session takes seven i64 tensors. `input_ids`, `position_ids`,
`token_segment`, and `token_qtype` are `[1, L]`. `segment_parent` is
`[1, S]`, `marker_pos` is `[Q, W]`, and `anchor_pos` is `[Q, 1]`. It returns
`logits [Q, W]` and `action_logits [Q, n_act]`. Every row is validated before
any model work: the parent order must be acyclic, the trunk must have no
kind, every marker must see its question's anchor, and positions and ids
must be in range. RoPE at explicit positions uses the backend's M-RoPE op with
all frequency pairs on the first axis. That is exactly split-half RoPE, and on
Metal it keeps the rotation on the device. Backends without the op rotate on
the host. Usage reports the processed packed tokens, so the saving is visible
to callers.

Backend status:

- **CPU and Metal:** use the generic ModernBERT path with segment attention
  (below). Its linears already run on resident weight slots.
- **Packed decision head:** question-type embeddings are gathered on the
  backend from `[type_emb; 0]`. Scoring reads the hidden state back and
  scores on the host on every backend (see the open issue below).
- **Fused Metal kernels:** the resident Laya kernels (`ops/laya_metal.zig`)
  are not used for packed rows (see roadmap step 1c).
- **CUDA:** does not select the Laya profile for packed configs, just as it
  does not for `max_len > 512`.

### State cache

The trunk never attends to a branch, so its keys and values at every encoder
and decision-head layer depend only on its tokens. Each packed session keeps a
bounded, least-recently-used cache of them, keyed by a hash of the trunk
tokens. When a row's trunk is cached, only the branch tokens are projected and
run through the feed-forward layers. Their attention spans the cached trunk
keys and values plus their own. A miss first encodes the trunk alone, which is
exact for the same reason, and fills the cache. The same state's later rows
and later requests then hit it. The API and the pipeline are unchanged.

- **Storage:** entries are f16 by default. On Metal they are device tensors
  converted in place on the GPU (`MetalCompute.deviceHalfCopy`), so neither a
  miss nor a hit touches the host. On CPU they are host f16. An entry costs
  `2 · (encoder + head layers) · T · hidden · 2` bytes, about 50 MB for a
  400-token state on the released checkpoint. f16 changes cached logits by at
  most ~5.5e-4 against the full row. `ANTFLY_LAYA_TRUNK_CACHE_DTYPE=f32` keeps
  f32 (exact, twice the memory).
- **Budget and admission:** `ANTFLY_LAYA_TRUNK_CACHE_MB` (default 256; 0
  disables) bounds the cache. Every entry also holds a lease from the
  session's model admission controller, charged as backend KV bytes on Metal
  and host KV bytes on CPU. When admission refuses, the cache evicts its least
  recently used unpinned entry and retries. If that is not enough, the state
  is served without caching and counted in `refusals`. Pinned entries are
  never evicted.
- **Short states:** trunks under 96 tokens are not cached
  (`default_min_tokens`). Re-encoding them costs less than the cached path's
  fixed overhead.
- **Row joins on Metal:** a branch-only forward must join cached trunk rows
  with branch rows and take the branch rows back out of attention's output.
  Metal's axis-0 concat blits outside the ordered decode stream, and the first
  version read stale queries through it. The result was wrong decisions
  through a session (max probability error 0.034 against the oracle), even
  though the same code was exact when called directly. Joins and slices on
  Metal now reshape to a flat `[1, rows · width]` view and use the in-stream
  last-dimension concat and slice (`modern_bert.joinRows`/`branchRows`). This
  keeps the encoder's batched command frame on. CPU uses the axis-0 concat and
  row gather directly.
- **Queries:** with segment attention, a cached row computes queries for its
  branch tokens only. The trunk contributes keys and values and no work of
  its own.

### Segment attention

In a packed row, the keys a token may see are the contiguous extents of its
own segment and its ancestors: at most three ranges (trunk, question,
candidate). `ComputeBackend.segmentAttention` (`ops.SegmentAttention`) takes
those ranges per query (`laya_tree.ranges`), the logical positions of queries
and keys, and a sliding window for local layers. It returns attention over
exactly the visible keys. There are no `[L, L]` masks, and queries may be a
suffix of the row, which the state cache uses. Every packed attention call in
the encoder and the decision head goes through it.

- **CPU (`linalg.segmentAttentionHost`):** a flash-style kernel. For each
  64-query block it merges the block's ranges and multiplies only those key
  chunks. Within a chunk, keys outside a query's own ranges or window get a
  zero weight.
- **Metal (`termite_sdpa_f32_segments`):** one threadgroup per (query, head)
  walks the query's ranges in 256-key chunks with an online softmax, so work
  and threadgroup memory depend only on visible keys. Ranges and positions go
  to the GPU in a single staged blob. Outside a command frame, the runtime
  stages every call at offset 0 of one buffer, and the first version's three
  separate staging calls overwrote each other. The symptom was wrong
  attention whenever a query had more than one range.
- **Fallback:** backends without the op use the host kernel.

#### Multi-row batching (step 1b′)

`ComputeBackend.segmentAttention` never assumed one tree per call: ranges are
per query, and a query's visible keys are wherever its ranges point, whatever
else is in the same physical row. `pipelines/laya.zig` (`executePacked`) uses
this to run several rows, from the same or different states, as one physical
row in one session call, instead of one call per row.

- **Layout:** `laya_tree.Row` already allowed one trunk (a root segment,
  `parents[s] == -1`). `validate` now allows more than one: each root starts
  an independent tree, and no non-root segment may itself be a root. A merged
  row is a forest, not a single tree with a wider trunk.
- **Coalescing (`laya_tree.coalesce`):** takes several already-built, already
  validated rows and concatenates their tokens; segment and parent ids are
  renumbered into one global space per row (each row's own root stays a
  root); anchors and markers move with their tokens; `markers` is padded to
  the widest row's option count. **Positions are not shifted** — every tree
  keeps the positions it would have alone, restarting at its own root. RoPE
  and the sliding window only ever compare a query's position to a key in
  its own visible ranges, so two trees can share position values without
  interacting. `owners[k]` records which input row contributed the merged
  row's `k`-th question, for mapping decisions back to their task.
- **Isolation:** unchanged from a single tree. `Row.visible` walks a
  segment's ancestors to `-1`; a token in one tree can never reach a
  segment in another tree's chain, so cross-tree attention is structurally
  impossible, not just masked to zero. `laya_tree.ranges` is unchanged — it
  already computed each token's ranges from its own ancestor chain only.
- **Grouping:** `executePacked` still groups tasks by state text and builds
  one or more rows per state (`laya_tree.build`, splitting only when a
  state's own branches overflow `max_packed_len`). It then walks the built
  rows in request order and greedily adds each next row to the current batch
  while the combined physical length stays within `max_packed_len` — the
  same bound `build` already enforces per state, reused instead of adding a
  second knob. `ANTFLY_LAYA_PACKED_BATCH=0` disables batching (one call per
  row, the previous behavior), for comparison and as a rollback switch.
- **Trunk cache:** a merged row's `laya_tree.treeCount` is greater than one,
  and `laya_packed.forwardRow` uses that to skip the cache and go straight to
  `forwardFull` for a merged row. Segment attention already keeps cost
  proportional to visible keys, so a batch's states cost the same whether or
  not any of them are cached — batching and the state cache are simply two
  independent ways to cut cost, not composed yet. Caching a forest of trunks
  (reusing some trees' trunks while others miss, in one call) is future work.
- **Exactness:** `pipelines/laya_packed_test.zig` ("laya multi-row coalescing
  isolates independent states and matches running them alone") builds three
  distinct states as separate rows, coalesces them, and checks every
  cross-tree pair is invisible and that the merged row's decisions equal
  each row run alone, in both question and candidate mode. A second test
  drives this through the pipeline: many small states batch into fewer
  session calls than `ANTFLY_LAYA_PACKED_BATCH=0`, with identical decisions
  and token counts either way.

## Training methodology

### Graph and data

The native training graph (`src/finetune/laya/graph.zig`) now takes, as
runtime inputs:

- RoPE tables at logical positions;
- separate global and local (windowed) encoder masks;
- a head mask;
- a per-token type-embedding mask;
- one marker row per decision.

An unpacked example is the one-segment tree: positions `0..n-1`, everything
visible, one question. The existing PyTorch gradient parity therefore still
exercises the same graph. With packing enabled, `data.load` groups records
that share `group_id` and state text into packed examples, and records map to
`(example, question)` placements. Metrics, calibration, and prediction files
stay per record.

### Long states (step 2c)

`architecture.buildWithAttention(..., use_fused_attention: true)` replaces the
three dense masks above (`__laya_encoder_bias`, `__laya_local_bias`,
`__laya_head_bias`, each `[batch*heads, S, S]`) with one flash-style op,
`fused_segment_training_attention_v1` (`ml/src/graph/node.zig`), and its
hand-written backward `..._backward_v1`. Both are new: they join the existing
fused training-attention ops (`fused_boundary_training_attention_v1`,
`fused_deberta_training_attention_v1`) that already carry their own VJP
instead of decomposing into primitives.

- **Op contract.** Forward leaves are `qkv` (`[Q;K;V]`, token-major
  `[3*batch*seq_len, num_heads*head_dim]`, upstream's natural QKV-linear
  layout -- no `heads()` transpose needed) and one physical i32 `control`
  leaf: six replay limbs, an `apply_dropout` flag, `batch*seq_len` logical
  positions, then `batch*seq_len*6` `laya_tree`-style ranges
  (ancestor-segment extents, as `ops.SegmentAttention` already uses at
  serving time). `window` and `dropout_probability` are graph-time
  attributes, not runtime tensors, so the same op expresses global
  attention (`window = maxInt`, one full-row range), sliding-window local
  layers (`window` set, same range), and tree-packed rows (the row's actual
  `laya_tree.ranges`) -- whichever the training graph's global/local
  encoder layers and packed rows need. `apply_dropout` *is* a runtime
  control value, unlike `dropout_probability`: because a `Program`'s graph
  (and its baked-in `dropout_probability`) is built once and reused for
  both training steps and `predict` eval, the fused op needs a per-call
  runtime toggle to silence dropout outside training the way the dense
  path's `drop()` does with an all-ones mask -- see "Packed job
  training-vs-serving, and the eval dropout leak" below.
- **Kernel.** `lib/linalg/src/attention.zig`
  (`segmentTrainingAttentionForwardHost`/`BackwardHost`), beside
  `segmentAttentionHost`: tiled online softmax (`BLOCK_Q`x`BLOCK_KV`,
  merged query-block ranges, exactly `segmentAttentionHost`'s masking) saves
  only per-query `(row_max, row_sum)`, never a `[tokens, tokens]` tensor.
  Backward recomputes the forward (for `O` and the stats) and makes one more
  tiled sweep that recomputes scores per tile for the exact `dQ`/`dK`/`dV`
  -- the same recompute-not-persist shape as
  `ops/deberta_training_attention.zig`. Dropout is a seeded counter mix
  (`segmentAttentionDropoutKeep`, independent of
  `deberta_training_attention.mix` so `lib/linalg` has no `pkg/inference`
  dependency) addressed by `(batch, head, query, key)`, replayed identically
  in both sweeps -- no probability mask is ever persisted.
- **Metal: device kernels without dropout, host bridge with it.**
  Without active dropout (`apply_dropout` off or `dropout_probability` 0,
  which covers every trunk layer), `MetalCompute.segmentAttentionOnDevice`
  (`ops/metal_compute.zig`) runs the call on the ModernBERT training
  attention kernels: the visibility, window, scale and empty-row contract
  are the same, so it only rewrites the row-relative ranges as absolute
  token rows. At 2,048 tokens with 12 heads of 64, forward plus backward
  takes 233 ms there against 1,679 ms through the host bridge. Calls with
  dropout (the decision head during training), controls with overlapping
  ranges (the ModernBERT kernels need disjoint ones), and head dimensions
  above 128 keep the host bridge: `segmentTrainingAttentionV1Op`/`BackwardV1Op`
  download `qkv`/`control`/`dOut`, run the exact
  same CPU kernel through a `NativeCompute` instance, and upload the result
  back -- the same `HostFallbackNative` bridge `hostFallbackSdpa` and
  `hostFallbackDisentangledRelativeAttention` already use for their
  device-kernel-missing cases. `MetalTensor.toHostSlice` already flushes any
  active command frame before reading, so this is safe inside
  `training.executeFramed`'s framed forward/backward without further
  changes. No `[tokens, tokens]` tensor is materialized on either path, so
  Metal jobs get the same `seq_len <= 8192` admission bound as CPU.
  `finetune/laya/job.zig` selects fused attention only when a split's
  layout actually needs it -- `exceedsDenseAttentionBound` re-checks the
  dense materialized-bias path's own `batch*L^2*heads` bound with
  `use_fused_attention=false` and takes fused only on
  `error.LayaTrainingAttentionLimitExceeded` -- or `Config.force_fused_attention`
  asks for it explicitly. Ordinary jobs (small states, the released
  checkpoint's `max_len`) keep training on the dense path on both backends.
  With the device kernels that is still the right default on Metal: full
  resident Metal training steps on the released checkpoint (batch 1,
  question packing, ReleaseFast, 2026-09-28) take the same time either way
  until dense no longer fits, and fused is slower on short rows:

  | Packed row | Dense step | Fused step |
  | --- | --- | --- |
  | `prof.json` (512-token budget) | 1,432 ms | 1,669 ms |
  | 1.5k-2.5k tokens | 17.8 s | 18.0 s |
  | about 3k tokens | 38.1 s | 38.0 s |

  So the automatic switch past the dense bound no longer costs a
  host-bridge slowdown, and there is no reason to prefer fused earlier.
  States came from concatenated `td/train.jsonl` texts under a copy of the
  checkpoint with `laya.max_len` 4096; medians exclude the first step.
  Dropout on device (the decision head while training) remains open work.
- **Admission.** `graph.validate` drops the quadratic bound when
  `use_fused_attention` and instead only checks `seq_len <= 8192`
  (ModernBERT's pretraining length, already `laya.max_len`'s ceiling).
  Memory is O(batch*heads*seq_len*head_dim), not O(batch*heads*seq_len^2).
- **Correctness.** `lib/linalg/src/attention.zig` tests the forward against
  a dense masked-softmax reference (global, local window, tree segments) and
  the backward against finite differences (with and without dropout).
  `finetune/laya/fused_attention_test.zig` runs the real training graph
  (`training.inputs` included) through the CPU backend and checks the fused
  and dense builds produce matching logits on an unpacked example and on a
  tree-packed row, with the same random weights. All of the above pass on
  both CPU and Metal, 2026-09-26 (Metal on the device kernels since
  2026-09-28).

**Relfix on the current fixture.** `training_test.zig`'s "every parameter
gradient match PyTorch" test's own hard gate (zero per-tensor mismatches
against a `5e-5 + 0.2%*magnitude` bound) does not pass against the current
`relfix` fixture for **either** attention path -- dense fails it too, with
the identical unmodified tolerance, so this is a pre-existing fixture/dense
gap, not something this track introduced. Reading the printed worst
per-tensor `relative_l2` values instead (LAYA.md's original 0.4-0.6%
number is this same metric):

| Backend | Dense: mismatches / worst relative_l2 | Fused: mismatches / worst relative_l2 |
| --- | --- | --- |
| CPU | 52 / 4.0% | 6 / 4.3% |
| Metal | 42 / 2.3% | 4 / 4.1% |

Fused attention has *fewer* tensors over the strict per-tensor threshold
than dense in both backends, but its single worst tensor is somewhat higher
(4.1-4.3% vs 2.3-4.0%). A dense-only re-measurement on the merged branch
(2026-09-26) gives the same picture: worst over `encoder.layers.*` weights
4.0% (CPU) and 4.1% (Metal), concentrated in the norm weights. The fixture
has not drifted: an earlier "0.4-0.6%" figure in this document was misread
from a partial printout of the mismatches and is withdrawn. The per-tensor
gate in this test is tuned for the tiny synthetic fixture and does not fit
the 28-layer model; use the worst relative L2 over `encoder.layers.*` as the
statistic instead.

**Packed job training-vs-serving, and the eval dropout leak.**
`training_packed_test.zig`'s "converts an unpacked checkpoint into a served
packed model" test compares the *trainer's own* `eval_predictions.json`
(the training graph, evaluated at the run's final weights) against
*serving the exported model* (a real `pipeline.execute` session) -- the
same weights on both sides, so any gap here is a bug, not training drift.
Forcing fused attention unconditionally (the first cut of this track) rose
this from 6.0e-8 to 2.9e-3 max probability error on the 2-layer hidden-64
`ref` fixture (`head_dropout` defaults to 0.1).

Root cause: the dense path's attention dropout is a *runtime* mask
(`graph.zig`'s `drop()` binds an external `__laya_dropout_N` parameter that
`training.inputs` fills with all-ones when `training=false`, i.e. at
`predict`/eval), but the fused op's `dropout_probability` is a *graph-time*
attribute baked into `SegmentTrainingAttentionAttrs` when the `Program` is
built -- and `job.zig`'s `Cache` builds that `Program` once and reuses it
for every training step and every `predict` call. With no runtime toggle,
the fused op kept applying its seeded in-kernel dropout during eval, so the
trainer's own eval of its final weights differed from a dropout-free
serving forward by roughly `head_dropout`'s scale -- and would have
corrupted eval predictions and calibration on every real long-state job,
not just this test.

Fix: `control`'s layout gained one word, `apply_dropout` (word index 6,
before the positions/ranges that follow it -- see the "Op contract" bullet
above and `ops.segment_training_attention.ControlView`).
`finetune/laya/training.zig`'s `inputs` sets it from the same `training:
bool` parameter the dense path already uses for its all-ones mask;
`segment_training_attention.zig`'s `forward`/`backward` pass `0` instead of
`attrs.dropout_probability` into the kernel whenever it is unset. Metal
inherits this automatically: with the flag off the call takes the device
kernels, which have no dropout, and with it on the host bridge forwards the
same control tensor to the same CPU decode. Verified: `training_packed_test.zig` now has
a second case, "... (forced fused attention)", that runs this exact job
with `force_fused_attention=true` and asserts the original `worst < 5e-5`
bound; both it and the default (dense, admission-gated) case measure
~3e-8.

Separately (and *not* the explanation for the gap above, since it compares
different weights, not the same ones): training the identical fixture/config
twice, once with `force_fused_attention=false` and once `=true`, and
diffing the two exported checkpoints, shows a worst relative L2 over
`encoder.layers.*` weight tensors of 2.43% on CPU (dropout-corrected). Both
runs are individually correct -- the tiled online-softmax's summation order
differs from the dense masked softmax's, and on this tiny six-example,
three-epoch fixture that ~1e-7-per-call difference compounds through Adam
into measurable (if small) weight drift. This has no dense baseline to
compare against once a job's layout actually exceeds the dense bound (the
dense path cannot run at that length at all), so it is noted here as a
characterized, expected property of switching attention implementations,
not something the admission gate needs to hide.

**Long-state smoke test (2026-09-26, CPU, released 421M checkpoint).** Data
and job recipe in `.tmp/longstate/` (gitignored): states synthesized by
concatenating distinct `td/train.jsonl` records to ~2k/4k/8k tokens
(word-count estimate; actual tokenization untested), `laya.max_len` raised
to 8192 in a copy of the released checkpoint's `config.json` and
`rl_agent_config.json`, `zig build -Doptimize=ReleaseFast --prefix <dir>`
once, then `/usr/bin/time -l <dir>/bin/antfly-inference finetune train laya
job_2k.json` under the gpu lock.

The 2k run reached 15.3 GB peak memory footprint (21.6 GB max RSS) over
748 s wall before `error.OutOfMemory` on a `dot_general` (a `[6656, 5248]`
MLP intermediate), before finishing even one full step or logging a
`"step"` event -- not a bug in the fused op specifically (nothing here is
attention-shaped; it is the encoder's ordinary MLP matmul), but this
machine's system-wide memory was critically low at the time (`vm_stat`:
under 70 MB free, all nine agents' tracks running full released-model
work concurrently). 4k and 8k were not attempted: they need strictly more
memory than a 2k run that already used 15-22 GB on a shared 36 GB
machine with near-zero headroom, and the task's own guidance is to stop
near 20 GB. Rerun `.tmp/longstate/job_{2k,4k,8k}.json` (currently
`stop_after_microbatches: 3`, reduced from a first attempt at 10 that made
the same throughput point more slowly) once the machine has real headroom,
and watch `vm_stat`/`/usr/bin/time -l` peak footprint, not just this
session's numbers.

Tree packing does not raise the logical length limit. Reaching Jev-like state
lengths additionally needs a long-context fine-tune: the ModernBERT encoder
was pretrained at 8,192 tokens.

### Converting a released checkpoint

Set `packing` in the job JSON. The released weights are the starting point,
and the export writes `laya.packing` into the served config:

```json
{
  "model_dir": "/abs/models/extractors/laya",
  "train_file": "/abs/train.jsonl",
  "eval_file": "/abs/eval.jsonl",
  "calibration_file": "/abs/calibration.jsonl",
  "output_dir": "/abs/runs/laya-packed",
  "packing": "question",
  "max_packed_len": 2048,
  "objective": "rlcd"
}
```

```bash
antfly inference finetune train laya job.json
```

### Recommended recipe

1. **Prepare data.** Convert typed-decisions JSONL with
   `scripts/laya/prepare_laya_finetune.py`. Pass `--max-labels 255` only for
   candidate-packed training. Keep every question of a case in one group, so
   splits stay disjoint and each case packs into one example.
2. **Distill.** Run `scripts/laya/prepare_laya_packed_distillation.py` to blend
   gold targets with the released unpacked model's calibrated distribution:
   `target = w · gold + (1 − w) · teacher`, with `w = 0.5` by default. The
   teacher sees only upstream's 512-token sequence. Records whose state
   upstream would truncate keep their gold target, so the student is never
   taught a distribution computed from a different state. Provenance and
   hashes are written to `<output>.json`.
3. **Train** with `packing` set and a held-out calibration split. Use
   `"objective": "soft_ce"` for questions with many options: RLCD diverged
   on 77-option Banking77. Add
   `freeze_layers` to trade adaptation of the lower encoder for step time. The RLCD
   objective is a strictly proper scoring reward with Gaussian logit
   exploration (`finetune/laya/objective.zig`). Soft CE is also supported.
4. **Accept** the packed model only if, on the same eval split, it matches the
   unpacked released model within agreed tolerances on per-type accuracy,
   soft CE, and ECE. The loss of early fusion (the state can no longer attend
   to the question) is the risk this gate measures. Report Banking77
   separately for candidate mode, because that is where candidate branches
   should help.

Tree packing does not raise the logical length limit. Reaching Jev-like state
lengths additionally needs a long-context fine-tune: the ModernBERT encoder
was pretrained at 8,192 tokens. The CPU trainer can now build the graph with
fused segment attention (`use_fused_attention`, see
[Long states](#long-states-step-2c)) to remove the quadratic admission bound
that used to cap this at about 2k tokens at batch 1; the fine-tune on
teacher-labelled long states itself is still open (step 2a).

### LoRA

Low-rank adaptation cuts optimizer state and lets one small adapter
specialize a released checkpoint without moving its weights. The job config
adds:

```json
"lora": { "rank": 16, "alpha": 32, "targets": ["encoder", "head"], "dropout": 0 }
```

`targets` names which linears get an adapter: `encoder` is every layer's
`attn.Wqkv`, `attn.Wo`, `mlp.Wi`, `mlp.Wo`; `head` is the decision head's
`self_attn.in_proj`, `self_attn.out_proj`, `linear1`, `linear2`. For each
targeted linear the base weight `W` is bound as a frozen runtime input,
exactly like `freeze_layers` binds a frozen layer, and the graph adds
`scale · (x·Aᵀ)·Bᵀ` beside `x·Wᵀ`, with `scale = alpha/rank`. `A` is
`[rank, in]`, Kaiming-uniform with bound `1/sqrt(in)` (`nn.Linear`'s default
initializer, and the formula the codebase's other LoRA injector,
`boundary_peft_graph.initializeModule`, already uses); `B` is `[out, rank]`,
zero, so an adapted model starts identical to the source checkpoint. A
targeted linear's bias, the scorer, the action head, the type embedding, and
every norm are never adapted and train fully whenever they are not otherwise
frozen (`training.frozen`, widened with an `?architecture.Lora` argument
alongside `freeze_layers`).

`freeze_layers` and `lora` compose. A layer below `freeze_layers` is frozen
exactly as before; its adapters, if `lora` also targets it, are frozen too
(`training.frozen` never differentiates a `.lora_A`/`.lora_B` name whose
layer index is frozen), rather than training a delta on top of weights that
never move for no benefit. A layer at or above `freeze_layers` keeps its
adapters trainable even where the targeted group matches, which is ordinary
LoRA-on-a-frozen-encoder.

Export merges every adapted weight into `W + scale·B·A` (`job.zig`'s
`mergeLora`) and writes only the merged dense tensor: `lora_A`/`lora_B` never
appear in the served checkpoint (`model/model.safetensors`), so serving is
unmodified and a downstream reader cannot tell a LoRA run from a full
fine-tune. The `latest.safetensors` resume checkpoint is the ordinary
optimizer-state format (`seeded_gradient_trainer.zig`): adapters are just two
more named parameters in it, with their own AdamW moments, so resume needs no
LoRA-specific handling.

Tests (`finetune/laya/graph.zig`, `training_test.zig`, `job.zig`): the graph
adds exactly one adapter pair per targeted linear and both target groups get
gradients; `isLoraWeight`/`isLoraFrozen`/`loraPrefix` identify the six
adapted linears and nothing else (not the scorer, type embedding, or norms);
`frozen` freezes a targeted weight and bias but leaves its adapter trainable,
and correctly composes with `freeze_layers`; `mergeLora` matches
`base + scale·B·A` exactly on a hand-computed case; and an end-to-end job run
on the reference fixture confirms a targeted weight moves, its bias and every
non-targeted tensor's frozen values are exact, no `lora_A`/`lora_B` tensor
reaches the served checkpoint, and interrupting and resuming a LoRA run
reproduces the uninterrupted run's exported weights bit-for-bit.

## Accuracy (step 0)

Measured 2026-09-24 on an Apple M4 Max, ReleaseFast, Metal trainer.

**Data.** [LocalLLaMA/typed-decisions](https://huggingface.co/datasets/LocalLLaMA/typed-decisions)
at `c76749ec58bd8c3d2ea706b31c333a9059c38f90` (`all` config): 1,200 train and
400 test cases in four workflows, with five questions per case. Train is
shuffled with seed 20260924, and 50 cases per workflow are held out for
calibration. Cases whose state exceeds 316 tokens are dropped from every
split, which guarantees any question fits the 512-token budget packed or
unpacked. The step-0 subsets are 100 train, 20 calibration, and 38 eval cases
per workflow: 400 / 80 / 152 cases, or 2,000 / 400 / 760 decisions.

**Training.** Both runs start from the released `convaiinnovations/laya` and
run one epoch with the RLCD objective, seed 42, and default learning rates,
with temperatures fitted on the calibration split.

- **Packed:** `"packing": "question"`, one packed case per step.
- **Unpacked:** one decision per step with gradient accumulation 5, so every
  optimizer update sees five decisions in both runs.

**Scoring.** Every model is scored on the same 760 decisions through the
serving pipeline (`antfly inference finetune eval laya <model> <records>`,
`finetune/laya/evaluate.zig`), with its calibration applied.

| Model | Layout | Accuracy | Soft CE | ECE | Ordinal MAE | Choice / score / boolean accuracy | Train time |
| --- | --- | ---: | ---: | ---: | ---: | --- | ---: |
| Released `laya`, zero-shot | unpacked | 0.387 | 1.308 | 0.158 | 0.656 | 0.338 / 0.352 / 0.482 | — |
| Released weights, zero-shot | packed | 0.361 | 1.337 | 0.133 | 0.666 | 0.197 / 0.309 / 0.592 | — |
| Unpacked fine-tune | unpacked | 0.572 | 1.007 | 0.055 | 0.461 | 0.627 / 0.467 / 0.658 | 3 h 09 min |
| **Packed fine-tune** | packed | **0.574** | 1.026 | 0.063 | 0.471 | 0.605 / 0.480 / 0.667 | **56 min** |
| Upstream `laya-typed-decisions` (`1a793eb`) | unpacked | 0.754 | 0.885 | 0.193 | 0.248 | 0.724 / 0.688 / 0.873 | — |

**Result.** In these single runs (seed 42) the packed model matched the
unpacked one on every metric. Packed training took 3.4× less wall time, and
packed evaluation processed 2.6× fewer tokens (81,745 vs 211,125). Repeated
seeds later showed that one run of this recipe cannot resolve a difference
this small (next section). Parity is plausible but not established: it needs
several seeds of each layout.

### Run-to-run variance

Measured 2026-09-25, same data, recipe and serving evaluator as step 0,
packed question mode. Each row changes only the training seed.

| Trainer | Seed 42 | Seed 43 | Seed 44 | Mean | SD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Step-0 trainer (host slices) | 0.574 | 0.371 | 0.557 | 0.501 | 0.113 |
| Current trainer | 0.434 | 0.461 | 0.455 | 0.450 | 0.014 |
| Current, `freeze_layers: 11` | 0.518 | 0.545 | 0.472 | 0.512 | 0.037 |
| Current, `freeze_layers: 18` | 0.464 | 0.491 | 0.464 | 0.473 | 0.016 |

- **Seed spread dominates.** The identical step-0 trainer scores 0.574 or
  0.371 depending on the seed. Seed 42 reproduces step 0 bit for bit.
- **Current vs step-0 trainer.** They differ only in how strided slices run.
  The current trainer takes them on the device. Both paths show a worst
  per-tensor gradient error of about 4% against float64 PyTorch on the
  released model (norm weights; an earlier, smaller figure here was a
  misreading). The mean accuracy difference (0.05) is below one standard
  error (~0.07). Without head dropout
  both give the same training curves over 200 steps (three seeds each).
- **Frozen lower layers** cost no accuracy at this budget: freezing 11 of 28
  encoder layers scored at or above full fine-tuning with the same trainer,
  and trains 1.4× faster (7 instead of 10 minutes per run).
- **Implication.** Accuracy claims about this recipe need several seeds. A
  larger training set or more epochs would likely shrink the spread.

### Packed vs unpacked at equal budget (2026-09-26)

Same current trainer, data, recipe (RLCD, 1 epoch, batch 1; unpacked with
gradient accumulation 5 so every update sees five decisions) and serving
evaluator. Three seeds each on the step-0 recipe, one seed on a larger one
(915 training cases, 1,840 eval decisions from `*-fit.jsonl`):

| Recipe | Layout | Seeds | Accuracy | Mean | SD | Soft CE | ECE |
| --- | --- | --- | --- | ---: | ---: | ---: | ---: |
| Step-0 (400 cases) | packed | 42 / 43 / 44 | 0.434 / 0.461 / 0.455 | **0.450** | 0.014 | 1.129 | 0.049 |
| Step-0 (400 cases) | unpacked | 42 / 43 / 44 | 0.599 / 0.628 / 0.637 | **0.621** | 0.020 | 0.983 | 0.088 |
| Larger (915 cases) | packed | 42 | 0.471 | — | — | 1.097 | — |
| Larger (915 cases) | unpacked | 42 | 0.671 | — | — | 0.946 | 0.105 |

**The step-0 gate fails.** At equal data and optimizer budget, question-mode
packing costs about 0.17 accuracy on typed-decisions, well outside the seed
spread, and more data does not close the gap (0.20 on the larger recipe).
The original single-seed parity (0.574 vs 0.572) was a lucky packed run.
Losing early fusion (the state no longer attends to the question) matters on
this dataset. Packed serving stays much cheaper, and candidate mode still
learns 77-way Banking77 (0.819), where no unpacked layout can fit the
options. Before packed question mode is used for accuracy-sensitive
decisions it needs a recipe that closes this gap.

**Distillation does not close it.** An unpacked fine-tune on the same data
(seed 42: 0.599 accuracy, soft CE 0.988) labelled the step-0 training
records, blended 50/50 with gold (`prepare_laya_packed_distillation.py`,
all 2,000 records), and packed students trained on those targets:

| Packed student | Seed 42 | Seed 43 | Seed 44 | Mean | Soft CE | ECE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Gold targets | 0.434 | 0.461 | 0.455 | 0.450 | 1.129 | 0.049 |
| Distilled targets | 0.464 | 0.433 | 0.455 | 0.451 | 1.135 | 0.033 |

Only calibration improves. Both layouts take the same 400 optimizer updates
on the same cases, so the gap is not about the targets: without the question
in view, the state encoding cannot carry what the unpacked model uses. The
remaining options are architectural (let the trunk see the questions, giving
up exact trunk reuse across question sets) or to keep question-mode packing
for cost-sensitive uses and route accuracy-sensitive decisions to the
unpacked layout.

**A question-aware trunk recovers most of it.** With
`packing.trunk_sees: "questions"` the state tokens attend to their whole tree
(the state and every question branch about it); branches still see only
their ancestors, and different states in one call stay isolated. Same recipe,
three seeds:

| Layout | Seed 42 | Seed 43 | Seed 44 | Mean | SD | Soft CE | ECE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| Packed, trunk sees state only | 0.434 | 0.461 | 0.455 | 0.450 | 0.014 | 1.129 | 0.049 |
| Packed, trunk sees its questions | 0.516 | 0.579 | 0.566 | 0.554 | 0.033 | 1.031 | 0.060 |
| Unpacked | 0.599 | 0.628 | 0.637 | 0.621 | 0.020 | 0.983 | 0.088 |

That closes about 60% of the gap (0.104 of 0.171) with the same packed token
count. The cost: the trunk depends on the question set, so the state cache
cannot reuse it across requests with different questions, and questions
about one state can influence each other through the trunk (a decision may
change when another question is added). The remaining 0.07 is plausibly
that interference plus the per-question fusion the unpacked layout gets. A
per-question view of the state in the upper layers (lower layers shared and
cached, question-specific fusion above) would keep isolation and is the next
candidate. Peak footprint 26.0 GB; the device estimate (31.5 GB) is
conservative.

**Caveats.**

- **Upstream checkpoint:** it is far ahead because it trained on all 1,200
  cases for longer and with a 1,024-token budget. It shows what more training
  buys, not a packed-versus-unpacked difference.
- **Released weights in the packed layout:** these score near zero-shot chance,
  like the unpacked released model. Upstream reports 0.362 unpacked zero-shot,
  so the layout change alone does not break the model.
- **Candidate mode:** not yet qualified. Its target, Banking77, is still to
  run.

### How Jev likely closes this gap (research, 2026-09-26)

TypeSafe has not published Jev's architecture. Everything public, though,
suggests that Jev reaches its accuracy **without** letting the state see the
questions. That is our default packed layout, so the gap above is most likely
about training, not the mask.

- **Isolation is a product promise.** TypeSafe says every question is
  "evaluated in parallel and in isolation against the same state"
  ([launch post](https://typesafe.ai/blog/introducing-system-one-models-and-jev)).
  A question-aware state (`trunk_sees: "questions"`) breaks that promise:
  adding a question can change another question's answer.
- **The reproductions mask the state to itself.** MoJev/Jevre's mask
  ([source](https://github.com/MoLeMo-Lab/mojev),
  [card](https://huggingface.co/di-zhang-fdu/jevre)) allows only
  `state → state` attention inside the state, bidirectional and never toward
  the questions. Its docstring gives the reason: spans that see each other
  lose "independent decisions, exact permutation invariance". The
  [RLCD explainer](https://di-zhang-llm.github.io/blog/what-is-rlcd-the-secret-behind-jev/)
  describes the same visibility.
- **They train at 150-200k rows, in the target layout, from an LLM.**
  - MoJev fine-tunes all of Qwen3.5-0.8B on 205,084 rows for one epoch.
  - [Open-Jev](https://zefan-cai.github.io/open-jev/story/) trains LoRA on
    Qwen 2B-27B over 148,639 rows and reports 85.3% on JevBench (Jev: 86.6%).
  - Causal pretraining never let the state attend forward, so for these
    models a question-blind state is the natural one.
- **Their heads differ from Laya's.**
  - MoJev mean-pools the state, question and candidate spans. It scores each
    candidate by a rank-512 bilinear product of a state-plus-question vector
    and a candidate vector, trained with a Plackett-Luce ranking loss plus a
    Brier calibration loss.
  - Open-Jev puts a scalar head on the last token.
  - Laya uses a two-layer transformer head over option marker tokens.

**Inference (ours).** Laya's released encoder was trained unpacked, with the
state *after* the question and attending to it: the reverse of Jev's order. The
packed fine-tunes ask it to unlearn that dependence in 400-915 cases. More data
helped a little (packed 0.450 → 0.471 going from 400 to 915 cases), but the
reproductions use about 200× more rows.

**Next experiment.** Train the default, question-blind packed layout at scale
on teacher-labelled synthetic cases. This keeps the state cache and question
isolation, and tests the scale hypothesis directly. A MoJev-style pooled
bilinear head is an optional ablation. Teacher throughput sets the cost of this
experiment; see [Teacher throughput](#teacher-throughput).

### Lessons from Jeeves (research, 2026-09-29)

[PostHog/jeeves](https://github.com/PostHog/jeeves) is a Jev-like decision
model: Qwen3.5-9B with LoRA (r = 16 on all projections) and a pointer head,
trained with SFT and then reinforcement learning (CISPO) to reason before it
decides. Figures below are the project's own; we have not reproduced them.

| Setting | Accuracy |
| --- | --- |
| Their test split (2,962 items), with reasoning | 0.840 |
| Same split, without reasoning | 0.804 |
| Dev (325 questions), full reasoning (median 3.3 s, p90 17.1 s on one H100) | 0.825 |
| Dev, reasoning only below 0.9 confidence, capped at 768 tokens (median 2.0 s) | 0.806 |
| Dev, no reasoning (about 0.3 s) | 0.775 |
| Test overall, item-weighted, with reasoning (Jev: 0.857) | 0.889 |

What bears on Laya:

- **Their layout is ours, on a causal decoder.** The prompt is `<state> …
  <q> instructions <opt> option </opt> … <decide>`: the state comes first and
  never sees the question.
  - Serving prefills the prefix a state's questions share once and gives
    every question its own copy of that cache (`inference/engine.py`,
    `group`). That is our trunk cache.
  - It works for them because a causal model's state tokens were never
    trained to see what follows them.
  - Our results point the same way. Every layout that keeps the ModernBERT
    state question-blind trails unpacked by about 0.1 or more, even with all
    30 layers fused per question ([Per-question upper layers](#scaling-packed-training-on-open-jev-2026-09-27)).
  - Both open reproductions that keep the state question-blind (MoJev and
    Jeeves) start from Qwen decoders.
- **Base model over data volume.** SFT used 19,126 questions (12 public
  datasets plus synthetic policy data), a tenth of MoJev's 205k rows. That
  fits our Open-Jev result, where 33× more data did not move the packed
  encoder.
- **A pointer head.** Each option is scored by a scaled dot product between a
  query projection (256 wide) of the hidden state at `<decide>` and a key
  projection at that option's `</opt>`, then one temperature fitted on dev.
  Laya instead runs a two-layer transformer head over option markers. A
  pointer head is a cheap ablation on our encoder, and it is the natural
  head for a decoder base.
- **Rare tokens as anchors.** `<state>`, `<q>`, `<opt>`, `</opt>` and
  `<decide>` map to unused Qwen tokens (`<|fim_prefix|>`, …). In their
  ablation, plain text such as "State" did worse. Laya inherits upstream's
  plain-text `"choice question: …"` prefix.
- **Confidence-gated escalation.** `nothink_threshold` answers without
  reasoning when the no-reasoning confidence is at least the threshold, and
  reasons otherwise. For Antfly that suggests Laya as the fast path, with low-
  confidence decisions sent to a reasoning model.
- **Smaller points.** They repeat the question after the reasoning block
  (dropping it hurt). They stopped RL at step 402 of 624, because later steps
  over-sharpened the head on a saturated pool. Their shared-prefix prefill is
  the same as our teacher's `--prefill shared`.

**Inference (ours): what this means for Laya and Antenna.**
- A question-blind, cacheable state on a decoder base is the
  best-supported route to Jev-level accuracy with state reuse. For Laya that
  means a small Qwen decision model (0.8B–2B, as MoJev uses) with a pointer
  head, served with the runtime's prefix KV cache.
- Antenna needs an encoder for embeddings, chunking and extraction
  ([ANTENNA.md](../antenna/ANTENNA.md)), so this does not transfer directly.
  On the encoder, the cheaper open levers are question-first positions
  (running), a pointer head, and rare-token anchors.
- Antenna's step 8 (a schema-blind trunk with task branches) should expect
  the same accuracy loss unless one of those closes it.

### Scaling packed training on Open-Jev (2026-09-27)

This is the test of the scale hypothesis above. It trains the default,
question-blind packed layout on public rule-labelled data, so no teacher is
needed.

**Data.** [Open-Jev](https://huggingface.co/datasets/ZefanCai/Open-Jev),
`release-v2-redistributable` train split (79,116 records, CC0), was converted
with `scripts/laya/prepare_laya_openjev.py`.
- Excluded `customer-control-v1` (4,206 records). Open-Jev notes that its
  question descriptions come from TypeSafe documentation without a verified
  license.
- Dropped 10,460 records whose whole state does not fit Laya's 512-token
  sequence.
- No record overlaps our splits by group, ID or state text.
- Kept 64,450 decisions:
  - painting-geometry: 23,552
  - snake: 12,508
  - vizdoom: 6,354
  - reasoning: 5,442
  - workflow controls (the domains closest to our eval): 11,956
  - tic-tac-toe, platformer and runner games: 4,638
- Mixed with our 2,000 `s0-train` decisions: 66,450 in total, 33× the step-0
  budget.

**Run.** The step-0 recipe (RLCD, encoder learning rate 2.5e-5, constant, batch
1, one epoch, seed 42) with `max_packed_len` 704.
- At the default 2,048, the admission estimate was 137 GB. Open-Jev states
  carry up to 51 questions, and dense attention memory grows with the square
  of the longest row. At 704 those states split into more rows that each
  repeat the trunk, which is exact.
- Estimate 31.5 GB, measured peak 27.4 GB.
- 20,990 packed rows in 6.6 h (1.14 s/step).
- The loader caps were raised to 1 GiB per file and 1M records. Memory stays
  bounded by `max_host_bytes`.

**Result: RLCD, seed 42 — worse on every question type.**

| Run | Decisions | Overall | choice | score | noul | Soft CE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Packed, `s0-train` only (3 seeds) | 2,000 | 0.450 | | | | |
| Packed, seed 43 | 2,000 | 0.461 | 0.368 | 0.414 | 0.614 | 1.121 |
| **Packed, Open-Jev mix, RLCD** | 66,450 | **0.375** | 0.329 | 0.273 | 0.557 | 1.270 |
| Unpacked (3 seeds) | 2,000 | 0.621 | | | | |

**Diagnosis: the run did not learn, so the result says nothing about scale
yet.** With one epoch, each step's loss is measured before the model has seen
that row, so the training loss is a held-out learning curve.
- Mean CE per 2,000 steps: 1.49, 1.28, 1.41, 1.60, 1.65, 1.85, 1.82, 1.50, 1.60,
  1.45, 1.52.
- A uniform guess over the mix scores 1.08. The model never beat it, and the
  loss peaked mid-run.
- The 95th-percentile gradient norm rose from about 500 to 4,400 by the end.
  Clipping to norm 1 kept updates bounded, but the loss still drifted upward.
- The step-0 runs, 400 steps each, stayed near 1.1 with 95th-percentile norms
  of 40-50.
- Two likely causes:
  - RLCD's Gaussian logit exploration, which already diverged on 77-option
    Banking77 ([Recommended recipe](#recommended-recipe)). About a third of Open-Jev
    choice questions have 9 or 16 options.
  - The learning rate: cosine decay from 2.5e-5 with no warmup, at batch 1
    over 21k steps, 50× longer than the runs it was tuned on. (An earlier
    version of this section said the trainer had no schedule; it has had
    cosine decay throughout.)
**Result: soft CE, seed 42 — Open-Jev learned, our eval unchanged.** The same
run with `"objective": "soft_ce"`. Train time 7.2 h, peak 27.4 GB.
- It trained stably. Mean CE per 2,000 steps: 1.11, 0.97, 0.90, 0.90, 0.82,
  0.83, 0.85, 0.83, 0.79, 0.74, 0.77. The 95th-percentile gradient norm was
  33-85 (RLCD: 385-4,400).
- Open-Jev validation: a 2,002-decision sample of the validation split, whole
  cases, converted the same way.
- Label prior: the most common gold answer in train for the same kind and
  label set.

| Eval | Model | Overall | choice | score | noul | Soft CE | ECE |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `s0-eval` (760) | Packed, `s0-train` only (3 seeds) | 0.450 ± 0.014 | | | | ~1.12 | |
| `s0-eval` (760) | **Packed, Open-Jev mix, soft CE** | **0.464** | 0.390 | 0.401 | 0.623 | 1.115 | 0.025 |
| `s0-eval` (760) | Unpacked, `s0-train` only (3 seeds) | 0.621 ± 0.020 | | | | | |
| Open-Jev val (2,002) | Label prior | 0.600 | 0.466 | 0.300 | 0.750 | | |
| Open-Jev val (2,002) | Uniform | | | | | 0.973 | |
| Open-Jev val (2,002) | **Packed, Open-Jev mix, soft CE** | **0.664** | 0.512 | 0.529 | 0.790 | 0.769 | 0.060 |

**Reading.**
- 33× more data, most of it from other domains, neither helped nor hurt our
  eval: 0.464 is within one standard deviation of 0.450.
- The model did learn Open-Jev, but only modestly: +0.064 over the label prior,
  and soft CE 0.77 against 0.97 for a uniform guess. For scale, MoJev reports
  93.2% on its own eval after one epoch over 205k rows, though that is a
  different base model and a different eval.
- So this run cannot separate "the packed encoder learns slowly" from "this
  recipe learns slowly". Candidates:
  - no learning-rate warmup;
  - batch 1;
  - a released encoder whose trunk learned to depend on seeing the question.
- The deciding control is an unpacked run on the same mix, evaluated on Open-Jev
  validation. If unpacked learns Open-Jev much better, the layout is the
  bottleneck. If not, the recipe is.

**Layout control (2026-09-27): the packed layout is the bottleneck.** Packed
and unpacked were trained on the same 16,009-decision subset (`td/oj16k.jsonl`:
14,009 Open-Jev decisions in whole cases, seed 11, plus `s0-train`).
- Setup: soft CE, seed 42, one epoch.
- Packed: `max_packed_len` 704, 5,319 rows. Unpacked: gradient accumulation 3,
  5,336 optimizer steps. Both runs take about the same number of optimizer
  steps.
- Train time: packed 1.6 h, unpacked 4.6 h (peak 25.8 GB).

| Eval | Layout | Overall | choice | score | noul | Soft CE | ECE |
| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Open-Jev val (2,002) | Label prior | 0.600 | 0.466 | 0.300 | 0.750 | | |
| Open-Jev val (2,002) | Packed, 16k | 0.607 | 0.461 | 0.345 | 0.757 | 0.839 | 0.066 |
| Open-Jev val (2,002) | Packed, 66k (above) | 0.664 | 0.512 | 0.529 | 0.790 | 0.769 | 0.060 |
| Open-Jev val (2,002) | **Unpacked, 16k** | **0.701** | 0.528 | 0.726 | 0.808 | 0.748 | 0.096 |
| `s0-eval` (760) | Packed, 16k | 0.463 | 0.386 | 0.418 | 0.601 | 1.122 | 0.043 |
| `s0-eval` (760) | Unpacked, 16k | 0.607 | 0.583 | 0.546 | 0.711 | 0.988 | 0.077 |
| `s0-eval` (760) | Unpacked, `s0-train` only (3 seeds) | 0.621 ± 0.020 | | | | | |

**Reading.**
- **Unpacked learns the same data much faster.**
  - Unpacked on 16k beats packed on 66k (0.701 vs 0.664).
  - Packed on 16k barely clears the label prior (0.607 vs 0.600).
  - The gap is largest on `score` (0.726 vs 0.345). Those questions need the
    state read *for* the question: which pixel, which ordinal rubric.
- **Why, most likely:** a question-blind 28-layer trunk followed by a
  two-layer head has little capacity to condition its reading of the state on
  the question. The released weights were also trained with early fusion. More
  data does not close that gap at this scale.
- **Out-of-domain data does not help our eval with either layout.** Unpacked
  on the 16k mix scores 0.607 on `s0-eval`, within noise of 0.621 on
  `s0-train` alone. Packed stays at 0.46.
- **So the scale hypothesis is rejected for this encoder.** Jev-style
  reproductions that keep the state question-blind start from causal decoders.
  Those never learned to depend on seeing the question, and they read
  question-conditioned features from a deep model rather than a two-layer head.
- **Options that keep caching and isolation:**
  - Per-question upper layers: share and cache the lower trunk layers, and
    fuse question and state in the top few layers per question (the variant
    proposed under the question-aware trunk).
  - A deeper head.
  - A decoder base, as in MoJev.
- The question-aware trunk (0.554) remains the best packed result, at the cost
  of isolation.

**Per-question upper layers (2026-09-28).** `packing.fuse_layers` K runs the
top K layers of the encoder-plus-head stack once per question, over that
question's own copy of the state.
- In those layers the state copy also attends to its question.
- Questions stay isolated: every tree holds one question.
- The layers below stay question-blind and identical across questions, so
  they remain cacheable.
- The top layer's state output is never read, so K must be at least 2.
- Fusing the whole stack reproduces the question-aware trunk exactly on each
  question (`laya_packed_test.zig`).
- Setup: same 16k subset and recipe as the layout control. The released
  stack is 28 encoder layers plus 2 head layers.

| Layout, 16k subset | Open-Jev val | `s0-eval` | Train rows | Train time |
| --- | ---: | ---: | ---: | ---: |
| Packed | 0.607 | 0.463 | 5,319 | 1.6 h |
| Fused, K = 10 (head + top 8 encoder layers) | 0.601 | 0.457 | 8,858 | 3.0 h |
| Fused, K = 30 (whole stack) | 0.605 | 0.476 | 8,858 | 3.2 h |
| Unpacked | 0.701 | 0.607 | 16,009 decisions | 4.6 h |

Fusing the top 10 layers buys nothing on either eval. Per kind, K = 10 scores:
- Open-Jev val: choice 0.441, score 0.363, noul 0.755; soft CE 0.830.
- `s0-eval`: choice 0.395, score 0.398, noul 0.596; soft CE 1.112.

Its training loss started higher than packed (1.10 vs 0.94 over the first
fifth) and ended similar (0.90 vs 0.89). The upper layers had to adapt to
states that suddenly see a question placed *after* them. The released weights
learned the reverse order (question first).

Fusing the whole stack (K = 30) barely helps either:
- Open-Jev val 0.605 (choice 0.442, score 0.363, noul 0.762), soft CE 0.835.
- `s0-eval` 0.476 (choice 0.461, score 0.385, noul 0.614), soft CE 1.090.
- Training loss by fifth: 1.10, 0.99, 1.03, 0.94, 0.88.

K = 30 is exactly the question-aware trunk with one question per tree. Every
layer fuses state and question, yet it still trails unpacked by 0.10 on
Open-Jev and 0.13 on `s0-eval`. So question-blind lower layers are not what
costs the packed layouts their accuracy.

**Inference (ours):** what is left between K = 30 and unpacked is layout, not
fusion.
- Unpacked puts the question and options *first* (`[CLS] head [SEP] options
  [SEP] state [SEP]`); every packed layout puts the state first.
- RoPE therefore sees the opposite relative order between question and state
  tokens from the one the released weights were trained on.
- The recipe differs too (gradient accumulation 3 vs one packed row per
  step), but step counts match.
- A cheap test that keeps caching: give the trunk fixed positions after the
  full head budget (`head_max_len`), so that every question sits *before* the
  state in position space, as in the released layout.

**Question-first positions and a pointer head (2026-09-29): neither closes
the gap.** Same 16k subset and recipe as the layout control.
- *Question-first positions* (`packing.question_first`) give every branch
  logical positions from 0, as the question has in the unpacked layout. The
  trunk starts after the question budget (`head_max_len` + 3), or as far
  right as fits. Only positions change; the trunk stays cacheable.
- *Pointer head* (`laya.decision_head: "pointer"`, from
  [Jeeves](#lessons-from-jeeves-research-2026-09-29)) replaces the scorer
  MLP. It projects the head's output at the question's `[CLS]` anchor to a
  query, and at each option marker to a key, both through a shared
  `pointer.norm`, and scores their dot product over √256.
  - A new pointer head is initialized Kaiming-uniform, with zero biases.
  - It trains in its own optimizer group at `pointer_lr` (1e-3), 10× the
    head's rate.

| Layout, 16k subset | Open-Jev val | `s0-eval` | `s0-eval` choice / score / noul | Final train CE (last fifth) |
| --- | ---: | ---: | --- | ---: |
| Label prior | 0.600 | | | |
| Packed | 0.607 | 0.463 | 0.386 / 0.418 / 0.601 | 0.89 |
| Packed, question-first | 0.621 | 0.476 | 0.439 / 0.418 / 0.592 | 0.91 |
| Packed, pointer head | 0.546 | 0.334 | 0.237 / 0.273 / 0.513 | 0.93 |
| Fused, K = 30 | 0.605 | 0.476 | 0.461 / 0.385 / 0.614 | 0.88 |
| Unpacked | 0.701 | 0.607 | 0.583 / 0.546 / 0.711 | 0.74 |

**Reading.**
- Question-first positions are within one seed's noise of packed (the
  three-seed spread was ±0.014). So the order in which RoPE sees question
  and state is not what costs packed its accuracy.
- The pointer head learned, but it trails the released scorer by 0.06–0.13.
  The scorer is pretrained with the rest of the model; a pointer head
  learned from scratch on 16k decisions does not catch up. Jeeves trains its
  pointer on a 9B decoder whose states already carry the decision.
- Every encoder-side variant tried leaves the gap to unpacked open:
  question-blind lower layers, full fusion per question, token order, and
  the decision head. What remains is the encoder base, as the Jeeves and
  MoJev evidence suggests.

**Two Metal training faults found on the way** (both fixed, with tests):
- *Fused gather of `add(matrix, bias)` with integer indices.* The
  interpreter fuses a gather whose source is `add(matrix, bias)`, which
  includes any LayerNorm output. Metal's fused kernel
  (`primGatherAddBiasAxis0`) reads its indices as floats, so training's
  int32 indices truncated to 0 and every gathered row was row 0. Integer
  indices now take the typed gather and a separate add. Test: "laya resident
  Metal gathers rows of a LayerNorm output".
- *Transposed-left `dot_general`.* Autodiff emits weight gradients as `Aᵀ·B`
  without materializing the transpose. Metal ran those on the host, so a
  Laya step took about 60 s instead of 1.1 s. Fixed in #913.
- The first two pointer runs were lost to these faults and to
  initialization: a saturated softmax from unnormalized inputs, then a
  zero-initialized query that learned too slowly. They are not results.
  Test: "laya pointer head gradients agree between native and resident
  Metal".

### Candidate mode on Banking77 (step 0b)

Measured 2026-09-26. `scripts/laya/prepare_laya_banking77.sh` downloads
Banking77 (PolyAI, pinned commit) and writes one 77-way `choice` question per
message: 20 train and 5 calibration examples per intent (1,540 and 385), and
400 eval messages from the test split, all disjoint by normalized text.
Antfly's unpacked pipeline caps choice questions at 20 options, so only a
candidate-packed model can serve these. The unpacked reference is the
released checkpoint scored with upstream's own code
(`scripts/laya/laya_upstream_baseline.py`), which squeezes all 77 options into
the fixed head budget.

| Model | Training | Accuracy | Soft CE | ECE |
| --- | --- | ---: | ---: | ---: |
| Released `laya`, unpacked (upstream code) | zero-shot | 0.348 | — | — |
| Upstream reported, released / `laya-typed-decisions` | zero-shot, their 400 cases | 0.425 / 0.492 | — | — |
| Jev (published) | zero-shot | 0.870 | — | — |
| Candidate-packed fine-tune, RLCD, seed 42 | 1 epoch | 0.015 (diverged) | 4.335 | 0.002 |
| **Candidate-packed fine-tune, soft CE, seed 42** | 1 epoch | **0.828** | 0.822 | 0.083 |
| Candidate-packed fine-tune, soft CE, seed 43 | 1 epoch | 0.810 | 0.851 | 0.109 |

- **Candidate mode makes 77-way choice learnable.** Upstream attributes its
  0.425 ceiling to the unpacked layout: 77 options share one fixed budget of
  about 4 tokens each. With a branch per option the fine-tune reaches
  0.819 mean over two seeds (0.828, 0.810).
- **Not a like-for-like comparison.** The fine-tune saw 20 in-domain
  examples per intent; the reference numbers, including Jev's, are
  zero-shot. An unpacked fine-tune at equal budget is not possible here,
  because the unpacked layout has no room for 77 options.
- **RLCD diverges with 77 options.** Cross-entropy rose from 3.6 to 5.0 with
  gradient norms in the thousands, then collapsed to uniform (ln 77 = 4.34).
  A quarter of the learning rate did not help. The policy term's Gaussian
  exploration over all 77 logits is too noisy at batch size 1. Soft CE trains
  cleanly (gradient norms ~100). Use `"objective": "soft_ce"` for
  many-option questions.
- **Cost:** 1,540 training steps took ~30 minutes on Metal (~1.2 s per step
  at ~370 tokens per row). Evaluating 400 decisions processed 147,764 tokens
  in ~140 s.

**Training throughput (as measured for step 0).** Steady state on this
machine: ~5–6 s per unpacked decision, and ~6–9 s per packed case of five
decisions. See [Trainer throughput](#trainer-throughput) for what changed
since. Several things turned up along the way:

- **Bucketing:** sequences are bucketed to 64 tokens and options to 4 so
  compiled programs are reused. Parity is unchanged, but it did not shorten
  steps, so graph construction is not the bottleneck.
- **Batch size:** `batch_size` 4 raised throughput only from 0.88 to 1.16
  decisions/s, and 8 gave 1.10. Cost grows about linearly with tokens.
- **Where the time goes:** a profile of the Metal trainer showed ~61% of the
  main thread waiting on a GPU synchronization after each op, ~11% slicing on
  the host, and ~13% downloading gradients and uploading them again for the
  optimizer.
- **Environment:** peak memory is ~27.5 GB. A concurrent 10 GB Zig build got
  the trainer SIGKILLed. macOS also throttles a trainer launched from a
  background shell (nice 5, background scheduling policy, display sleep) to a
  few steps per hour; run it in the foreground or clear the policy
  (`taskpolicy -B -p <pid>`) and keep the display awake. The raw logs are in
  the work log. See [Training memory](#training-memory) for a breakdown of
  this footprint and the admission gate added to refuse a job that will not
  fit before it starts, instead of relying on the OS to kill a concurrent one.

### Two-stage choice (roadmap 2b)

Candidate branches cannot compare options before the final softmax: each
option is scored in isolation, so the model never gets to weigh finalists
against each other directly, only through the shared trunk. [The tree-mask
hypothesis](#the-tree-mask-hypothesis) attributes exactly this two-step
procedure to Jev at high option counts: "score candidates independently, then
make an explicit choice." This section adds it to Laya and measures it on
Banking77 against the 0.819 candidate baseline above.

**Design.** Stage 1 is the unmodified candidate pass: every option is its own
branch under the trunk, as above. When a `choice` question has more options
than `packing.two_stage.top_k`, stage 2 packs the surviving finalists into one
*joint* branch off the same trunk — `[CLS] head [SEP] ([MASK] option)* [SEP]`,
the same layout `question` mode uses, so the finalists attend to each other —
and reruns just that branch. Finalists are the highest-probability options
under stage 1, capped at `top_k` and optionally cut off earlier once
`mass_cutoff` of stage 1's probability mass is captured (never below 2). The
two distributions are blended rather than replaced: writing `m` for the
stage-1 probability mass stage 1 assigned to the shortlist, each finalist's
final probability is `m` times its stage-2 share of that mass, and every
non-finalist keeps its stage-1 probability unchanged. The mix still sums to 1,
and stage 2 can only move mass among the finalists — it cannot let a
low-probability option that stage 1 dropped resurface. Everything decodes
through the existing calibration and readout (`decode`/`finalize`,
`pipelines/laya.zig`), so a shortlist of `k` options is calibrated exactly
like a `question`-mode question with `k` options would be.

**One checkpoint, two branch shapes.** Serving stays one call: `executePacked`
runs the candidate row, decodes it, and for each `choice` question over
`top_k` options builds and runs a second, one-question row with
`BranchStyle.question` forced regardless of the model's own `packing.mode`
(`laya_tree.build`'s new style-override parameter). Both rows share the
trunk's KV cache, so stage 2 costs only the joint branch's own forward. A
checkpoint therefore has to answer both branch shapes well, which needs both
in training: `finetune/laya/data.zig`'s `addStageTwo` adds, for every eligible
`choice` record, one synthetic record holding the gold label plus `top_k - 1`
other options sampled uniformly at random (deterministic in the job's seed),
and `pack` routes it through the joint (`question`) style while the original
record keeps its ordinary candidate branch. The job JSON's `two_stage_top_k`
(and optional `two_stage_mass_cutoff`) enable this and are written into the
served `packing.two_stage` config; leaving them unset trains and serves
exactly as before.

**Measured on Banking77** (400 eval messages, `packing.two_stage.top_k = 8`,
starting from the same released checkpoint and recipe as the candidate
baseline: one epoch, soft CE, `b77/train.jsonl` plus its synthetic stage-2
rows, calibration on `b77/calibration.jsonl`):

| Model | Accuracy | Soft CE | ECE | Top-8 recall | ms/decision | Tokens/decision |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Candidate-only baseline (prior work, mean of 2 seeds) | 0.819 | — | — | — | — | — |
| Two-stage checkpoint, seed 42, **stage 1 alone** (`two_stage` unset) | 0.8525 | 0.632 | 0.057 | 0.995 | 403 | 369 |
| **Two-stage checkpoint, seed 42, stage 1 + stage 2** | 0.8325 | 0.686 | 0.067 | — | 633 | 435 |
| Two-stage checkpoint, seed 43, stage 1 alone | 0.8425 | 0.640 | 0.063 | 0.985 | 374 | 369 |
| **Two-stage checkpoint, seed 43, stage 1 + stage 2** | 0.8375 | 0.785 | 0.071 | — | 625 | 435 |
| Mean, stage 1 alone (2 seeds) | 0.8475 | 0.636 | 0.060 | 0.990 | 389 | 369 |
| **Mean, stage 1 + stage 2 (2 seeds)** | **0.8350** | **0.735** | **0.069** | — | 629 | 435 |

- **Stage 2 makes accuracy worse, not better, on this recipe, and it does so
  on both seeds.** Same checkpoint, same eval set, only the decoding path
  differs: stage 1 alone scores 0.8525 / 0.8425 (seeds 42 / 43); blending in
  stage 2 drops both to 0.8325 / 0.8375, a consistent ~1.25-point mean loss,
  and soft CE and ECE get worse on both seeds too. This is the two-stage
  design's headline result and it is negative.
- **It is not a shortlist-recall problem.** Stage 1's top-8 shortlist
  contains the gold label 99.5% (seed 42) / 98.5% (seed 43) of the time — the
  99.9th-percentile case for a bound, not the bottleneck. Stage 2's own
  joint-branch distribution must be miscalibrated or simply wrong often
  enough, on the finalists it does see, to outweigh the cases where it
  corrects stage 1.
- **Also worth noting: stage 1 alone, on this checkpoint, beats the original
  candidate-only baseline** (0.8475 mean vs 0.819 mean). The two-stage
  training recipe trains on the candidate rows *and* a same-size batch of
  synthetic joint rows, which amounts to roughly double the gradient steps
  over related data; this is not a controlled ablation of stage 2's value,
  only a by-product of the extra training the mixed recipe happens to do. It
  means comparing the full two-stage number against the old baseline (0.835
  vs 0.819) would *understate* how much stage 2 alone costs; the fair,
  controlled comparison is stage 1 alone vs. stage 1 + stage 2 on the same
  weights (0.8475 vs 0.8350), which is unambiguous and still negative.
- **Cost.** Stage 2 adds one joint-branch forward per `choice` decision over
  `top_k` options (all 400 Banking77 eval messages have 77 > 8 options, so
  every decision pays it): +66 tokens per decision exactly (369 → 435,
  reproducible across seeds and runs) and roughly +240 ms per decision on
  this shared, loaded machine (389 → 629 ms mean; per-run wall time ranged
  374–693 ms depending on contention from other agents' jobs, so treat the ms
  figures as indicative only, unlike the token counts).
- **Hypotheses for the regression, untested here:** (a) the joint branch is
  trained on far fewer effective examples per class than the candidate branch
  (one sampled shortlist per record vs. all 77 options), so it may simply be
  undertrained; (b) uniform random negatives are an easy shortlist most of
  the time (top-8 recall is 99.5%), so stage 2 rarely has to do real
  discrimination work in training and may not have learned to firmly prefer
  the right answer among *hard* look-alikes; (c) the calibration gap is
  suspect too — the trainer fits one temperature for the whole `choice` kind
  (`job.zig`'s `calibrate`), mixing 77-way stage-1 rows and 8-way stage-2
  rows in one calibration set, so neither branch's temperature is fit for
  its own distribution shape. (c) is the cheapest to try next: fit
  `temperature_by_options` per count bucket instead of one flat value.
- **Not yet tried:** hard-negative shortlists (from a stage-1 checkpoint's
  own confusions) instead of random ones, a larger `top_k`, and a
  `mass_cutoff` shortlist instead of a fixed size.

### Trainer throughput

Measured 2026-09-25 on the same machine: packed question mode, batch size 1,
14 microbatches of the step-0 training subset (`td/prof.json` from
`scripts/laya/prepare_laya_training_data.sh`), median step after two warm-up
steps, ReleaseFast.

| Trainer | Median step | Speedup |
| --- | ---: | ---: |
| Step-0 trainer (per-op synchronization, host slicing, gradient round trip) | 8.55 s | 1× |
| + strided slices on the device | 6.43 s | 1.3× |
| + gradients kept on the device (unframed) | 5.34 s | 1.6× |
| + one command frame per forward and per backward | 2.02 s | 4.2× |
| + one command batch per optimizer transaction | 1.68 s | 5.1× |
| + runtime inputs uploaded once; gradients handed over without a copy | **1.38 s** | **6.2×** |

Frozen layers, measured on the 2.02 s trainer:

| Trainer | Median step |
| --- | ---: |
| `freeze_layers: 11` (half the encoder) | 1.41 s |
| `freeze_layers: 18` | 1.17 s |

The first four losses are identical in every configuration without frozen
layers. Frozen runs share the first loss and then diverge, as expected.
Freezing saves less than its share of layers, because the forward and the
decision head still run in full.

- **Device slices:** strided slices of device tensors now run on the GPU
  (`slice_plan` on `decoderRuntimeSliceTypedDevice`) instead of downloading.
- **Gradients on the device:** the resident optimizer takes a read-only view
  of each dense device gradient (`resident_training.Request.adopt_f32`), with
  no copy. It falls back to download and upload only for host-backed
  gradients.
- **Optimizer batch:** the resident AdamW transaction
  (`seeded_device_transaction.prepare`) used to submit and wait after every
  snapshot, elementwise op and zero fill. It now owns one command batch
  (`ComputeBackend.residentTrainingBeginBatch`/`EndBatch`). Only the
  finiteness and norm reductions synchronize it, and it commits before the
  transaction returns. A failed or cancelled transaction discards the batch;
  anything already executed wrote only replacement buffers. Resident ops
  still refuse any frame they do not own.
- **Device inputs:** attention biases, RoPE tables, type masks and dropout
  masks are uploaded once per step. As host tensors, every op that read them
  uploaded them again, in every layer of both graphs. This was ~95% of the
  host-side encoding time.
- **Command frames:** `training.executeFramed` runs each forward and backward
  graph in one Metal command frame and synchronizes once at the end.
  `ANTFLY_LAYA_TRAIN_UNFRAMED=1` restores per-op submission.
- **Frozen lower layers:** `freeze_layers: N` in the job keeps the token
  embeddings and encoder layers `0..N-1` at their source values. They are not
  differentiated, have no optimizer state, and are exported unchanged. The
  backward graph stops at layer N. The forward still runs every layer.

Framing exposed a bug in the Metal runtime's in-frame buffer reuse. Buffers
freed during a frame went into a reuse pool whatever their storage mode, and a
later private allocation could receive a shared one. An upload into shared
storage is an immediate host copy, so the previous owner's still-queued GPU
writes landed on top of the new data when the frame ran. In the trainer this
corrupted the index arrays of the embedding-gradient scatter and produced
NaN updates. It showed up only in framed runs, and flushing after almost any
op hid it. The pool now takes private buffers only
(`metal_runtime.zig`, "metal in-frame buffer reuse never hands a
host-writable buffer to a private request").

What remains per step at 1.38 s: GPU work of ~0.13 s forward and ~0.34 s
backward, ~0.21 s in the optimizer transaction (mostly its snapshot copies
and full-state finiteness reads), ~0.07 s building inputs (dropout random
numbers on the host), and ~0.05 s encoding. Cutting the optimizer further
means changing the transaction contract: skip re-validating state the
previous commit already validated, and write AdamW out of place instead of
snapshotting weights and moments first.

Raw per-step logs and the investigation are in
[`work-log/completed/inference/laya/2026-09-25-trainer-throughput.md`](../../../../../work-log/completed/inference/laya/2026-09-25-trainer-throughput.md).

Segment attention in the training graph landed with step 2c (below). Batching still helps little, because cost is per token. LoRA is
implemented ([LoRA](#lora)); its own step-time, memory, and accuracy numbers
are below.

### Half-precision training (not adopted)

An opt-in f16 path (half operands with f32 accumulation through MPS for
every linear's forward and backward matmuls, f32 master weights and
optimizer state) was built and measured on 2026-09-26 and **not merged**:

- **Not faster:** 1.357 s against 1.312 s median step on `td/prof.json`
  (3-4% slower); the per-matmul casts cost more than the half GEMMs save.
- **Not correct at full depth:** worst relative gradient error over
  `encoder.layers.*` rose from 2.25% (f32) to 353% against float64 PyTorch,
  although each isolated half GEMM was within 0.1%. Without loss scaling the
  error compounds over ~340 matmuls per step.
- **Caching the half weights per step was unsafe:** keyed on buffer address,
  it served stale casts after Metal reused a freed activation's address
  within a frame. Half operands for the batched attention matmuls diverged
  within four steps.

A future attempt needs loss scaling or bf16, per-layer precision choices
validated against the float64 gradient check, and a weight cache keyed on
the binding rather than the buffer address. Branch
`laya/half-precision-training` keeps the attempt.

### LoRA throughput and accuracy

LoRA does not shrink the forward graph (every targeted linear still computes
its base `x·Wᵀ` beside the adapter's `scale·(x·Aᵀ)·Bᵀ`), so a step still runs
the full encoder and head plus a small extra matmul pair per targeted linear;
step time should be at or slightly above a full fine-tune's, not below it.
What it removes is optimizer state: a targeted linear's `[out, in]` weight and
its two AdamW moments drop out, replaced by `A` (`[rank, in]`) and `B`
(`[out, rank]`) and their own moments, which is smaller whenever
`rank < out·in/(out+in)`. At rank 16 on the released 421M checkpoint's largest
linears (`hidden 1024`, `mlp 6144`) that is decisively true, so the expected
win is optimizer-transaction bytes and peak memory, not wall time per step.

Measured 2026-09-26 on an Apple M4 Max, ReleaseFast, Metal trainer, released
`convaiinnovations/laya`, `targets: ["encoder","head"]`, `alpha` 32,
`dropout` 0.

**Step time and memory.** `td/prof.json`-style job (packed question mode,
batch size 1, 14 microbatches of the step-0 training subset), median step
after two warm-up steps, peak `/usr/bin/time -l` footprint. The full
fine-tune row is remeasured here (same job, same machine state) rather than
reusing the 1.38 s/step figure from [Trainer
throughput](#trainer-throughput), so the two rows are directly comparable:

| Trainer | Median step | Peak footprint |
| --- | ---: | ---: |
| Full fine-tune (remeasured) | 1.31 s | 25.88 GB |
| LoRA rank 16 | 1.23 s | 14.43 GB |

LoRA is not slower despite the extra adapter matmuls: it tracks far fewer
optimizer-managed parameters (two small `A`/`B` matrices per targeted linear
instead of the full `[out, in]` weight and its two AdamW moments), and the
optimizer transaction was ~15% of full fine-tune's step time (its own
[Trainer throughput](#trainer-throughput) breakdown), so the saving there
outweighs the small added forward/backward cost. Peak memory drops 44%
(1.79x).

**Accuracy (step 0, packed question mode).** Same recipe, data, and serving
evaluator as [Accuracy (step 0)](#accuracy-step-0) and [Run-to-run
variance](#run-to-run-variance), rank 16:

| Trainer | Seed 42 | Seed 43 | Seed 44 | Mean | SD |
| --- | ---: | ---: | ---: | ---: | ---: |
| Current trainer (full fine-tune) | 0.434 | 0.461 | 0.455 | 0.450 | 0.014 |
| LoRA rank 16 | 0.480 | 0.478 | 0.454 | 0.471 | 0.014 |

LoRA rank 16 scores at or above the full fine-tune baseline, with the same
seed-to-seed spread (SD 0.014 in both). It lands between the unfrozen
baseline (0.450) and `freeze_layers: 11` (0.512): training only a small
delta on top of frozen base weights plausibly regularizes this small
(400-case) recipe similarly to freezing lower layers, without freezing any
layer outright. Three seeds is still not enough to resolve a difference this
size against the 0.450 baseline ([Run-to-run
variance](#run-to-run-variance) makes the same point about the existing
rows), so read this as "not worse," not as a confirmed improvement. Rank 64
was not measured (time did not allow it in this session).

### Training memory

Measured 2026-09-25 on an Apple M4 Max (36 GiB), ReleaseFast, `backend: metal`,
`packing: question`, batch 1, seed 42, released `laya` (28 layers, hidden
1024, 16 heads, intermediate 2624, ~394.7M trainable parameters, 0 frozen
layers), the same step-0 subset as [Trainer throughput](#trainer-throughput).
`/usr/bin/time -l` around the whole process, one run stopped after 1
microbatch and one after 14:

| Microbatches | Max RSS | Peak footprint |
| --- | ---: | ---: |
| 1 | 8.39 GiB | 23.46 GiB |
| 14 | 8.39 GiB | 24.11 GiB |

RSS is flat; peak footprint (macOS's combined host-plus-device high-water
mark) grows about 703 MB (2.8%) from 1 to 14 microbatches. The ~15 GiB gap
between RSS and footprint is Metal/unified memory: shared-storage buffers a
kernel-level RSS sample does not attribute to the process the way ordinary
heap pages are. The small growth across steps tracks bucket-size variation
among shuffled training examples (`training.bucketedLayout` rounds each
batch's sequence up to 64 tokens; a later, longer example raises the
high-water mark), not an unbounded per-step leak: step time also stays in a
narrow 1.0-2.5 s band with no widening trend. No safe reduction was found
beyond what [Trainer throughput](#trainer-throughput) already did. The
resident AdamW transaction's weight/m/v snapshot-then-swap
(`seeded_device_transaction.prepare`) is the dominant transient, and changing
it is explicitly out of scope: it exists to keep the transaction all-or-
nothing, and cutting it needs a transaction-contract change (skip
re-validating already-committed state, write AdamW out of place) that is
still open.

**Admission.** The Laya job Config gained `max_backend_bytes` (default 32
GiB; an initial 24 GiB default refused the standard packed recipe, whose
estimate is above that and whose measured peak is ~25.9 GB), a
device/unified-memory ceiling alongside the existing `max_host_bytes`. A
refusal logs the estimate and the ceiling, and every Metal job logs its
estimate. The admission controller's live system-memory check is opt-in
(`check_live_memory`): it reserves serving headroom of a quarter of physical
memory on top of the request, so on a 36 GB laptop it refuses every full
Laya fine-tune (a ~25 GB job would need ~34 GB free). Run one training job at
a time on a small machine.
`execute` estimates the run's device need right after selecting trainable
parameters and before any device weight or optimizer-state allocation (well
before creating the output directory), and refuses with
`LayaBackendMemoryLimitExceeded` if the estimate exceeds the ceiling:

- **Weights and optimizer state (exact).** The resident AdamW transaction
  keeps weight, m, v, and the gradient accumulator resident (4x trainable f32
  bytes) and, on every optimizer step, additionally snapshots weight/m/v into
  new buffers before freeing the old ones, so the transient peak is 7x
  trainable bytes; frozen tensors add 1x with no optimizer state.
- **Activations (approximate).** Per encoder layer: about twelve hidden-width
  buffers, two GeGLU intermediate-width buffers, and eight dense
  `[batch, heads, seq, seq]`-sized buffers (three tree/padding biases, forward
  scores and softmax probabilities, and their backward cotangents), summed
  over the run's admitted layouts and over forward-pass layers plus
  backward-retained (unfrozen) layers.
- **Fixed overhead (measured).** 4 GiB for MPS kernel/temporary caches, the
  in-frame buffer reuse pool, per-step RoPE/bias/dropout runtime inputs, and
  Metal driver bookkeeping, calibrated against the run above.

For this run's layout (~448-token packed rows after bucketing, batch 1): the
estimate is **22.9 GB** (device only). Adding a few GB for host-side model
loading and dataset/tokenizer state (not separately isolated in this
measurement) lands close to the **25.2-25.9 GB** measured combined peak
footprint above, so the estimate is a reasonable, if approximate, stand-in for
the real number. It is not tight: a training graph with dense (non-segment,
non-checkpointed) attention scales the activation term with `sequence^2`, so a
longer packed row than this workload's costs much more (the estimate exceeds
`max_backend_bytes` well before `architecture.validate`'s existing
`batch*sequence^2*heads <= 64M`-element bound would, which only bounds one
attention tensor, not the sum of buffers across every layer).

A separate, unpacked run (`s0-train`, `gradient_accumulation: 5`, so only one
in five microbatches is a stepped optimizer update) showed a 17 GB process
footprint in `top` and ~20 GB of system wired memory. The estimate for that
layout (sequence 512, the full unpacked budget) is 24.9 GB: higher than
observed, as expected, since the estimate assumes every microbatch is a
stepped update (worst case, matching the default `gradient_accumulation: 1`)
while most of that run's microbatches were accumulation-only and never paid
the transaction's transient snapshot.

**Process-wide admission.** `execute` also acquires a lease from the
inference runtime's `memory.AdmissionController` (`.gpu` backend class for
`backend: metal`, `.cpu` otherwise), charging the device estimate as
`backend_scratch_bytes` and enabling the controller's live-memory check only
for Metal. On macOS, `AdmissionController` charges backend bytes to the same
live system-memory sample as host bytes (`memory.zig`'s `liveHostBytes`,
because Metal draws from unified memory), so this check is real cross-process
protection: two concurrent `antfly-inference finetune train laya` processes
each request their own device estimate against one shared, live view of
system memory, and the second is refused once the two would not fit, rather
than both proceeding into OOM. `max_host_bytes` is deliberately not also
charged to this live sample: it is an existing generous safety-net ceiling on
allocator growth (enforced independently by `BoundedAllocator`), not a
measurement of actual usage, and charging the full default there as well
would make a single default job's own declared envelope exceed most machines.
The standalone CLI (`train_laya.zig`) creates one `AdmissionController` per
process; a caller embedding the trainer in a longer-lived process (for
example, alongside serving sessions) can share its own controller instead so
resident models and training compete for the same ledger.

The final `report.json` records `backend_estimated_bytes` (the admission
estimate) and `backend_peak_bytes` (the Metal runtime's own
`device_owned_peak_live_bytes`, reset at admission so it reflects this run
alone), alongside the existing `host_peak_bytes`.

Segment attention in the training graph landed on CPU (step 2c, see
[Long states](#long-states-step-2c)); a device kernel remains open (LoRA is implemented, see [LoRA](#lora)).
Batching still helps little, because cost is per token.

## Verification

All numbers are from 2026-09-24 on an Apple M4 Max (36 GiB), Zig 0.16.0.

| Check | Where | Result |
| --- | --- | --- |
| Packed encoder on a one-segment tree equals the unpacked encoder | `pipelines/laya_packed_test.zig` | max error 0 (CPU), 0 (Metal) |
| Questions isolated: packed together vs one question per row | same | ≤ 4.8e-6 (question and candidate modes, CPU and Metal) |
| Trunk encoding identical across rows | same | 0 |
| Pipeline preserves request order; splitting a request across rows changes no decision | same | < 1e-5 |
| Malformed rows rejected at the session boundary | same | `InvalidLayaPackedRow` |
| Independent PyTorch oracle equals upstream `DecisionModel` on one-segment trees | `scripts/laya/laya_packed_reference.py` | max error 0.0 |
| Zig packer rows equal the oracle's rows; Zig decisions match the oracle | `pipelines/laya_packed_parity_test.zig` | rows exact; max probability error 1.2e-7 (CPU and Metal) |
| Packed training graph equals packed serving, alone and in padded batches | `finetune/laya/training_packed_test.zig` | max logit error 7.0e-6 |
| Unpacked training still matches PyTorch after generalizing the graph | `finetune/laya/training_test.zig` | 45 gradient tensors, max error 2.4e-6 (native), 1.9e-6 (resident Metal) |
| Released-format checkpoint converted by the trainer serves its final eval exactly | `training_packed_test.zig` | max probability error 6.0e-8 |
| State cache: branch-only forward on a cached trunk equals the full row (miss, then hits with other question sets) | `pipelines/laya_packed_test.zig` | ≤ 2.7e-6 (CPU), 0 (Metal, device-resident entries) |
| State cache through a real session: oracle decisions on a miss and on a hit | `pipelines/laya_packed_parity_test.zig` | max probability error 1.2e-7 (CPU and Metal) |
| Session cache hits across pipeline requests; a disabled cache returns the same decisions | same | 1 miss, 1 hit; < 1e-5 |
| Cache pinning, LRU eviction, oversize entries | `architectures/laya_trunk_cache.zig` | unit test |
| f16 and f32 cache slots; admission charged per entry, refused entries evict LRU and retry | same | unit test |
| f16 state cache against the full row | `pipelines/laya_packed_test.zig` | max logit error ≤ 5.5e-4 (CPU and Metal); f32 ≤ 1.7e-6 |
| Framed Metal training: forward, objective and all 45 gradients match PyTorch | `finetune/laya/training_test.zig` | max error 1.9e-6 |
| Frozen lower layers are exported bit-identical; trainable layers move | same | exact (CPU and Metal) |
| LoRA: one adapter pair per targeted linear, both target groups differentiated | `finetune/laya/graph.zig` | exact (12 `lora_A`/12 `lora_B` for rank 4, 2 encoder layers + 1 head layer, both targets) |
| LoRA target/freeze/prefix helpers identify exactly the six adapted linears and their bias, and compose correctly with `freeze_layers` | `finetune/laya/graph.zig`, `training_test.zig` | exact |
| LoRA merge equals `base + scale·B·A` on a hand-computed case; rejects a shape mismatch | `finetune/laya/job.zig` | exact |
| LoRA end-to-end job (synthetic fixture): a targeted weight moves, its bias and every non-targeted frozen tensor stay exact, no `lora_A`/`lora_B` reaches the served checkpoint, interrupted-then-resumed reproduces the uninterrupted export | `finetune/laya/training_test.zig` | exact (digest match), CPU and Metal |
| Full `--test-filter laya` suite unaffected by widening `training.frozen`'s signature and `architecture.build`'s parameter list | all laya tests | 64 selected, 56 passed, 0 failed (8 skipped: CUDA, packed benchmark, export-reference — unrelated to this track), CPU and Metal, including the 45-tensor PyTorch gradient parity test (max abs error 2.4e-6 native / 1.9e-6 resident Metal, unchanged from before this track) |
| q8_0 linears keep every decision; probabilities close to dense | `pipelines/laya_quantized_test.zig` | labels identical; max probability error 9e-7 (CPU), 5e-6 (Metal) on the fixture |
| Metal linears read the current weight after it is replaced | `ops/resident_training_metal_test.zig` | exact; fails without the slot-cache fix |
| Gradients on the released model vs float64 PyTorch, three ~330-token states | `training_test.zig` with a released-model fixture | worst relative L2 over `encoder.layers.*` weights 4.0% (CPU), 4.1% (Metal), largest in norm weights (float32) |
| CPU segment kernel equals dense masked softmax (three ranges, window, fewer queries than keys) | `lib/linalg/src/attention.zig` | < 1e-5 |
| Segment attention equals dense tree-masked attention on the session backend, all queries and branch queries only | `pipelines/laya_packed_test.zig` | 2.4e-7 (CPU), 3.6e-7 (Metal) |
| Multi-row coalescing: no token of one state's tree is visible from another's; a batched call over several states equals each state's row run alone (question and candidate modes) | same, "laya multi-row coalescing isolates independent states and matches running them alone" | max probability error < 1e-5 (CPU and Metal) |
| Pipeline batches many small states into fewer session calls than `ANTFLY_LAYA_PACKED_BATCH=0`, with identical decisions and prompt tokens either way | same, "laya packed pipeline batches many small states into fewer session calls" | exact (< 1e-5) |

| Fused training attention forward equals dense masked softmax (global, local window, tree segments) | `lib/linalg/src/attention.zig` | < 1e-4 |
| Fused training attention backward equals finite differences (with and without dropout) | same | analytic within 2e-2 of central difference |
| Fused training graph (`training.inputs` included) matches the dense-bias graph's logits, unpacked and tree-packed, CPU | `finetune/laya/fused_attention_test.zig` | < 2e-3 |
| Fused training graph matches the dense graph on Metal | same, `SkipZigTest` without a Metal device | < 2e-3 |
| Metal segment attention takes the device kernels without dropout and the host bridge with it, forward and backward matching the CPU op | same ("... runs on the Metal device kernels ...") | within 2e-4 |
| Released-model gradients vs float64 PyTorch, fused attention (relfix) | `finetune/laya/training_test.zig` ("... (fused segment attention)") | see "Relfix on the current fixture" below -- not the pass/fail gate, a measurement |
| Trainer's own eval predictions vs. serving the exported model (same weights), default admission-gated and forced fused attention | `finetune/laya/training_packed_test.zig` (two cases) | max probability error ~3e-8 both ways, unchanged `worst < 5e-5` bound (see "eval dropout leak" below for the bug this catches) |
| Fused training attention graph VJP: three leaf gradients, integer control has none, retained through lowering | `ml/src/graph/segment_training_attention_test.zig` | exact node-shape checks |

Reproduce the fixture-backed tests. The fixtures are regenerated from pinned
inputs rather than committed:

```bash
scripts/laya/prepare_laya_fixtures.sh .tmp/laya
cd zig/pkg/inference
ANTFLY_LAYA_REFERENCE=$PWD/../../../.tmp/laya/ref zig build test -- --test-filter "laya"
ANTFLY_LAYA_REFERENCE=$PWD/../../../.tmp/laya/ref ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_METAL=1 zig build test -- --test-filter "laya"
```

`scripts/laya/prepare_laya_training_data.sh` rebuilds the released checkpoint,
the typed-decisions splits and subsets used in [Accuracy](#accuracy-step-0),
and the trainer timing job.

### Cost

This benchmark measures the released `convaiinnovations/laya` checkpoint
(`c5d78730f3493e4fe16d61507ef4b78eef7318cf`) with the same weights loaded
unpacked and packed. Packed decisions from unadapted weights are meaningless,
so this measures **cost only**. It uses median wall time over five warm
requests and a ReleaseFast build. The workload is Q questions (cycling
choice/boolean/score) about one state of 1, 4, or 12 repeated support-ticket
sentences.

```bash
ANTFLY_LAYA_PACKED_BENCH=/abs/models/extractors/laya [ANTFLY_LAYA_BACKEND=metal] \
  zig build test -Doptimize=ReleaseFast -- --test-filter "laya packed benchmark"
```

Current code: segment attention plus the state cache. Metal's unpacked
baseline uses the fused resident Laya kernels. "Cached" is a repeated request
about the same state after the first (states under 96 tokens are not cached).

| Backend | State tokens | Q | Unpacked ms | Packed ms | Packed, cached ms | Tokens unpacked → packed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Metal | 55 | 16 | 169 | 107 | 106 | 955 → 446 |
| Metal | 151 | 16 | 419 | 132 | 119 | 2,491 → 542 |
| Metal | 407 | 1 | 207 | 135 | 66 | 407 → 408 |
| Metal | 407 | 4 | 477 | 147 | 83 | 1,643 → 486 |
| Metal | 407 | 16 | 1,545 | 200 | 141 | 6,587 → 798 |
| Metal | 407 | 64 | 6,099 | 472 | 382 | 26,363 → 2,046 |
| CPU | 55 | 16 | 1,560 | 931 | 932 | 955 → 446 |
| CPU | 151 | 16 | 4,248 | 1,191 | 995 | 2,491 → 542 |
| CPU | 407 | 1 | 1,162 | 1,145 | 331 | 407 → 408 |
| CPU | 407 | 4 | 3,890 | 1,340 | 521 | 1,643 → 486 |
| CPU | 407 | 16 | 15,621 | 2,102 | 1,503 | 6,587 → 798 |
| CPU | 407 | 64 | 59,180 | 5,077 | 4,332 | 26,363 → 2,046 |

Sixty-four questions about a 400-token state take 6.1 s unpacked and 0.38 s
packed with a cached state on Metal (16×), and 59 s against 4.3 s on CPU
(13.7×). A follow-up question about a cached state takes 66 ms on Metal
against 207 ms unpacked. A single question about a new state costs about the
same packed or unpacked.

How the numbers got here, all on the same machine and checkpoint:

- **RoPE on the device:** the first packed Metal implementation rotated RoPE
  on the host. That took 56 device synchronizations per forward, added
  200–800 ms, and lost to the fused baseline everywhere. The device M-RoPE op
  removed the overhead.
- **Dense masks (before step 1b):** 16 questions on a 407-token state took
  342 ms on Metal and 2.76 s on CPU. Segment attention brought that to 200 ms
  and 2.10 s, and the gap grows with row length.
- **State cache before segment attention:** a cached follow-up was 108 ms on
  Metal and 740 ms on CPU. Removing the trunk's zero query rows brought it to
  66 ms and 331 ms.

**Device scoring for packed rows (2026-09-25, restored 2026-10-03).**
Scoring on the device instead of reading the hidden state back saved 2–4% at
16–64 questions (for example 12 sentences, 64 questions: 406.8 → 399.5 ms
packed) and nothing measurable below. It was reverted after intermittent
wrong outputs on Metal, then restored once the cause, a stale embedding-table
cache, was fixed (see below). Either way, the packed Metal path is bound by
encoder GPU work that the fused resident kernels would compute the same way,
so routing packed rows through them (step 1c) is not expected to pay off on
Metal.

**Stale embedding-table cache (fixed 2026-10-03).** The Metal device
scoring failure was not a race. Metal's embedding lookup copied each table
into a device cache keyed by the table's buffer identity (device buffer
handle or host address) and shape. Gathering marker and anchor rows from the
hidden state put activations into that cache. Once the buffer pool handed a
later table of the same shape the same buffer, the lookup returned the
earlier rows. The batching test failed in 3 of 24 Metal runs with a
probability error up to 0.059. A regression test that reuses a pooled table
fails on every run without the fix.

Device tables are now read in place, and the host-address cache only keeps
immutable model storage. With device scoring back on:
- the regression test passes;
- the batching test passes 40 of 40 runs;
- the Laya parity tests pass on Metal against fresh PyTorch fixtures.

The full raw output of every run is in
[`work-log/completed/inference/laya/2026-09-24-tree-packing.md`](../../../../../work-log/completed/inference/laya/2026-09-24-tree-packing.md).

### Multi-row batching cost (step 1b′)

The table above amortizes a shared **state**: many questions about one
state, packed into one row. Multi-row batching amortizes the opposite shape:
many **states**, each with only a few questions, batched into one row per
call instead of one call per state. This is the common shape for a request
that scores many independent short items (tickets, records, chunks) with
the same handful of questions. Same checkpoint, machine, and build as above;
5 warm requests each of Q=4 questions (cycling choice/boolean/score) about
S distinct states of one repeated sentence, `ANTFLY_LAYA_PACKED_BATCH=0`
forces one packed call per state (the pre-batching behavior) for comparison.

```bash
ANTFLY_LAYA_PACKED_BENCH=/abs/models/extractors/laya [ANTFLY_LAYA_BACKEND=metal] \
  zig build test -Doptimize=ReleaseFast -- --test-filter "laya packed benchmark"
```

| Backend | States | Unpacked ms | Packed, batched ms | Packed, one row per state ms | Batched calls | Unbatched calls |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Metal | 16 | 578 | 321 | 1,155 | 1 | 16 |
| Metal | 64 | 2,564 | 1,401 | 4,779 | 2 | 64 |
| CPU | 16 | 5,897 | 3,548 | 7,154 | 1 | 16 |
| CPU | 64 | 22,425 | 13,181 | 28,229 | 2 | 64 |

At this question count, one packed call per state is **slower than not
packing at all**: 16 (or 64) tiny per-call sessions cost more in fixed
overhead (admission, tensor marshaling, a state's own trunk re-encoded from
scratch each time) than packing saves on shared prefixes when there is
little to share. This holds on both backends: one-row-per-state is 2.0×
(Metal) to 1.2× (CPU) slower than not packing at all. Batching removes that
overhead by merging states into few calls: 16 states go from 16 calls to 1
(1.8× faster than unpacked on Metal, 1.7× on CPU; 3.6× and 2.0× faster than
one-row-per-state), and 64 states from 64 calls to 2 (1.8× and 1.7× faster
than unpacked; 3.4× and 2.1× faster than one-row-per-state).
`laya_tree.treeCount` disables the trunk state cache for a merged row (see
"Segment attention" above), so this gain is from batching alone; a future
step could compose the two.

### Weight quantization (step 1d)

Released `laya` (unpacked) on the 760 step-0 eval decisions through the
serving evaluator, 2026-09-25, Apple M4 Max. Footprint is the process's peak
memory footprint, which includes Metal allocations.

| Weights | Backend | Accuracy | Soft CE | ECE | Peak footprint | Eval time |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | Metal | 0.3868 | 1.3080 | 0.158 | 7.94 GB | 61 s |
| q8_0 | Metal | 0.3842 | 1.3067 | 0.158 | **5.09 GB** | 67 s |
| dense | CPU | 0.3868 | 1.3080 | 0.158 | 2.94 GB | 602 s |
| q8_0 | CPU | 0.3855 | 1.3075 | 0.157 | 3.47 GB | 1,587 s |

On Metal, q8_0 cuts memory by 36% at the same accuracy (two of 760
decisions change) and is about 10% slower. On CPU it was originally a loss:
the native q8_0 path kept prepared layouts beside the quantized bytes, and
its kernels were slower than the dense BLAS path for these shapes. CPU times
overlapped with training runs, so treat them as indicative only. It does not
meet the 5e-5 exact-serving bound and is not meant to; the fixture test
requires identical labels and probabilities within 2e-2
(`pipelines/laya_quantized_test.zig`). The fused resident Metal path reads
dense weights, so a quantized checkpoint runs the generic encoder.

**After the CPU kernel fix (2026-09-26),** same 760 decisions, ReleaseFast,
one job at a time (no other GPU work):

| Weights | Backend | Accuracy | Soft CE | Peak footprint | Eval time |
| --- | --- | ---: | ---: | ---: | ---: |
| dense | Metal | 0.3868 | 1.3080 | 7.92 GB | 48.6 s |
| q8_0 | Metal | 0.3842 | 1.3067 | **5.02 GB** | 48.1 s |
| dense | CPU | 0.3868 | 1.3080 | 1.06 GB | 490.5 s |
| q8_0 | CPU | 0.3842 | 1.3067 | 1.53 GB | 497.4 s |

q8_0 now runs at dense speed on both backends and saves 37% of Metal memory.
The CPU footprint stays above dense because dense weights are read through a
read-only mmap, which the footprint barely counts, while the Q8_0 bytes are
allocated. The earlier table's CPU numbers overlapped other jobs.

Two problems explained the CPU loss:

- **Wrong kernel selected.** `native_compute.zig` already has a
  dequant-once-then-Accelerate-SGEMM fast path for other quantized
  architectures (GLiNER, CLIP/CLAP), gated by weight-name heuristics. Laya's
  ModernBERT-encoder and decision-head linears matched none of them, so every
  Laya Q8_0 linear fell back to the native int8 dot-product kernel, which
  loses to Accelerate's SGEMM (used by the dense path) at Laya's shapes (rows
  in the hundreds to low thousands, `out_dim` 1024-5248).
- **Triple-counted memory.** The native kernel's on-first-use preparation
  (`prepareNativeQuantizedStorage`) keeps three representations of the same
  weight once a Laya linear is touched: the raw compressed bytes, a
  row-major "prepared" copy, and a 4-row panel copy for the int8 kernel.
  Each is close to the size of the compressed weight, so the three together
  land close to the size of the *dense* f32 weight — explaining why the
  table above shows q8_0 CPU footprint (3.47 GB) larger than dense
  (2.94 GB) despite Q8_0 packing to about 1/4 the bytes of f32.

The fix (`ops/native_compute.zig`): a dedicated `shouldUseLayaDequantSgemm`
predicate routes Laya's encoder/head linears through a transient
dequantize-into-scratch-buffer-then-SGEMM path (`layaDequantScratchSgemm`),
reusing the same `dispatchSgemmTransB` call the dense path uses; the scratch
buffer is freed every call, so no persistent dense mirror is kept (unlike the
GLiNER/ClipClap path, which caches dequantized weights up to a 512 MB budget
that would not fit Laya's ~330 M encoder parameters). `loadWeight` skips
`ensurePreparedKBlock` for the same weight names, so the row-major/panel
copies are never built at all: CPU footprint should now be dominated by the
compressed Q8_0 bytes alone (about 1/4 of dense f32) plus one small reused
scratch buffer.

One subtlety cost a wasted first attempt and is worth recording: the natural
predicate to reuse is `models/laya.zig`'s `quantizedLinear`, which is exactly
right for *checkpoint* tensor names (`"encoder.layers.N...."`,
`"head.layers.N...."`). But by the time a weight reaches CPU kernel dispatch,
`session_factory.normalizeWeightKey` has already rewritten those to runtime
keys (`"model.layers.N...."` for the encoder, `"model.head.layers.N...."` for
the head — see `laya_head.weight`). A predicate built on the checkpoint name
space silently never fires at dispatch time. `shouldUseLayaDequantSgemm`
matches the runtime key space instead, with a unit test
(`"laya dequant sgemm predicate matches normalized runtime keys, not
checkpoint names"`) pinning both spellings so this cannot regress silently
again.

**Verification.** `--test-filter "laya"` passes on both CPU and Metal (52 of
60 selected tests; the rest are CUDA-only, benchmark-only, or optional-path
skips), including the existing `pipelines/laya_quantized_test.zig` parity
test (max probability error ~1e-6 on each backend, well inside the 2e-2
gate) and two new `ops/native_compute.zig` tests: the naming-contract test
above, and an end-to-end test that dispatches a Laya-named Q8_0 linear and
checks both that it takes the dequant+SGEMM path (via the dispatch counter)
and that `QuantizedStorage.prepared.ownedBytes()` stays 0 afterward. The
generic `--test-filter "q8_0"` kernel suite (30 tests covering GLiNER,
CLIP/CLAP, and general GGUF Q8_0/Q8_1 decode) is unchanged, since the new
path only activates for Laya's specific runtime weight-name patterns.

**Re-measured after the fix (2026-09-26).** The shared build/GPU lock stayed
held by other agents' training and evaluation runs for most of this session
(one job alone held it 50+ minutes; a mid-session lock-policy change did not
help, since the in-flight holder had already committed to the old
combined-hold behavior). A clean window eventually opened. Time budget did
not allow the full 760-decision set at that point, so this is the first 200
of the 760 step-0 eval decisions (`head -n 200 td/s0-eval.jsonl`), same
checkpoint, ReleaseFast, ordinary Metal/CPU load (no other job running):

| Weights | Backend | Accuracy | Soft CE | Eval time | Max RSS | Peak footprint |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| dense | Metal | 0.415 | 1.3383 | 10.26 s | 1.37 GB | 5.76 GB |
| q8_0 | Metal | 0.415 | 1.3347 | **7.78 s** | 2.22 GB | **3.91 GB** |
| dense | CPU (native) | 0.415 | 1.3383 | 78.32 s | 4.87 GB | 0.97 GB |
| q8_0 | CPU (native) | 0.415 | 1.3347 | **77.33 s** | 5.35 GB | 1.32 GB |

("Max RSS" and "Peak footprint" are `/usr/bin/time -l`'s "maximum resident
set size" and "peak memory footprint" lines; they diverge because the
process mmaps the dense safetensors checkpoint read-only, and macOS's
footprint accounting undercounts clean file-backed pages relative to newly
allocated ones — see below.)

**Timing goal met on both backends.** CPU q8_0 is now on par with dense
(previously 1,587 s vs 602 s on the full set, contended — q8_0 2.6x
*slower*; now 77.33 s vs 78.32 s on this subset, q8_0 marginally *faster*).
Metal q8_0 is 24% faster than dense here, better than the prior table's
"about 10% slower" on the full 760; the difference in backend/subset
between the two measurements (this is a smaller, differently-ordered
subset, and a different codebase revision) means the two Metal numbers are
not directly comparable, but the qualitative result — q8_0 no longer
loses to dense — holds on both backends.

**Footprint: mixed, and worth explaining.** Metal footprint drops 32% (3.91
vs 5.76 GB), close to the prior table's 36% figure and reproducing it
independently. CPU footprint is *higher* for q8_0 (1.32 vs 0.97 GB
"peak footprint"; 5.35 vs 4.87 GB RSS), not lower. The reason is structural,
not a regression from this fix: dense weights are loaded via a read-only
mmap of the safetensors file, and those clean pages barely register in
macOS's footprint/RSS accounting once mapped; `quantizeDenseQ8_0` computes
Q8_0 into a freshly allocated heap buffer, which is real, counted, dirty
memory regardless of how compact it is. This fix still removes the
regression this same table used to show for CPU (footprint *larger* than
dense by keeping three prepared representations of every quantized weight);
what remains is the fixed cost of quantized bytes not being able to share
dense's free mmap ride. A tighter apples-to-apples comparison would need to
isolate weight-storage bytes specifically (e.g. `vmmap`/heap diffing across
the load step) rather than whole-process RSS/footprint, which is left as a
follow-up.

Reproduce with:

```bash
~/bin/zig build -Doptimize=ReleaseFast --prefix <dir>
/usr/bin/time -l <dir>/bin/antfly-inference finetune eval laya \
  <laya-released-dir> <records.jsonl> --backend native   # dense
ANTFLY_LAYA_WEIGHT_QUANT=q8_0 /usr/bin/time -l <dir>/bin/antfly-inference \
  finetune eval laya <laya-released-dir> <records.jsonl> --backend native
```

The full-760-decision re-run at this fixed revision, and a weight-storage-only
footprint breakdown, are still open (see the work log).

## Long-context teacher (step 2a)

Measured 2026-09-26 on an Apple M4 Max (36 GiB, shared with other agents).
Step 2a needs soft labels for typed-decisions states Laya's 512-token budget
cannot see, so a later long-context Laya (step 2c) has something to distil
from besides gold.

### Teacher choice

The roadmap line names "Qwen3.8-27B", which does not exist: Qwen3's released
dense sizes are 0.6B, 1.7B, 4B, 8B, 14B and 32B (plus 30B-A3B and 235B-A22B
mixture-of-experts checkpoints); there is no 27B or "3.8" release. The teacher
used here is **`Qwen/Qwen3-14B`**, run from the **`mlx-community/Qwen3-14B-4bit`**
quantized checkpoint (7.8 GB on disk) via **MLX** (`mlx-lm`), not through
Antfly's own runtime:

- **Antfly cannot give label log-likelihoods.** The generation API documents
  `logprobs` as "not supported, always null"
  (`specs/openapi/inference/api.yaml`), so `antfly inference chat` has no path
  to a label's probability. This step therefore uses the small-Python-path
  fallback the roadmap allows.
- **Context.** `max_position_embeddings` is 40,960 (native, no RoPE scaling
  needed) against the >=32k requirement, and comfortably covers every state in
  the dataset (the longest is 549 Qwen tokens; see below).
- **Size vs. footprint.** Qwen3-14B-4bit is the strongest Qwen3 checkpoint that
  fits the ~18 GB bound with margin. Qwen3-30B-A3B (mixture-of-experts) was
  considered and rejected: even at 4-bit its resident experts run ~15-16 GB,
  too little headroom on a 36 GB machine shared by about nine agents.
  Measured peak resident set (`/usr/bin/time -l`, 300-decision run,
  `maximum resident set size`): **8.94 GB**.
- **No compilation.** `mlx-lm` ships prebuilt wheels for macOS/arm64 and needs
  no CMake/build step, unlike `llama-cpp-python`; loading the model took 1.5-3 s.

### Scoring

`scripts/laya/prepare_laya_longcontext_teacher.py` (new; sibling to, and
partitions a dataset with, `prepare_laya_packed_distillation.py`):

- **Eligibility (which records get a teacher target).** Reuses the same
  `state_fits` check against the target unpacked Laya checkpoint's real
  `max_len`/`head_max_len` (not a raw token count), so a record either gets
  scored by `prepare_laya_packed_distillation.py`'s Laya teacher (Laya can see
  it) or by this script's teacher (Laya cannot), never both, never neither.
- **Prompt.** One prompt per record: the full, untruncated state, the
  question and its labels with descriptions, asking for exactly one label
  verbatim. Uses Qwen3's chat template with `enable_thinking=False`.
- **Label score.** Each label's log-likelihood via teacher forcing, in two
  conventions computed from the same per-token logprobs (free to compute
  together): raw sum, and length-normalized (average log-probability per
  token, since labels tokenize to different lengths). The prompt is encoded
  once per record; each label branches off a saved copy of the prompt's KV
  cache instead of re-encoding the prompt (`mlx_lm.models.cache.KVCache.state`),
  which cut per-label cost from ~2 s (a full prompt-length forward) to
  ~30-60 ms and left the shared cache unmodified (checked directly: cache
  offset and key/value shapes are unchanged after branching, and a
  from-scratch continuation gives the same logits).
- **Calibration (independent of eligibility).** The teacher scores every
  calibration record itself, short or long, so its own confidence calibration
  does not depend on whether *Laya* could see that state — only the
  eligibility check above decides which records receive a teacher-labelled
  target. `--calibration` therefore scores every record in the given file.
  Temperatures are fit per question type and, where a (type, option-count)
  bucket has >=15 calibration examples, per bucket too (the same shape as
  Laya's own `temperature_by_options`); a bucket short on examples falls back
  to its type's temperature.
- **Raw-sum vs. length-normalized.** `--score-mode auto` (default) fits both
  conventions on the calibration split and keeps whichever gives lower mean
  cross-entropy. On the 1,000-record calibration split: raw-sum 1.0149 nats,
  length-normalized 1.0199 nats — raw-sum wins, but narrowly (0.5% relative).
- **Blend.** `target = w * gold + (1 - w) * teacher`, `w = 0.5` by default,
  identical to `prepare_laya_packed_distillation.py`. Provenance (`<output>.json`)
  records the teacher's model path and config hash, the common.py hash, a hash
  of the prompt-building function's source, the chosen score mode, and the
  fitted temperatures (by bucket and by type).

### How much of the dataset is actually invisible to Laya

`prepare_laya_finetune.py` drops any case whose state exceeds 316 *raw* tokens
from every split, a global bound sized for the worst case (a question that
uses the full 192-token head budget, leaving `512 - 192 - 3 ≈ 317` tokens for
state). Most >316-token states belong to questions with fewer labels, which
leave more of the 512-token budget for state and so still fit. Re-checking
with the exact `state_fits` budget math, only a fifth of the >316-token
records are truly invisible to Laya:

| Split | >316-token candidates | Actually invisible to Laya |
| --- | ---: | ---: |
| `eval` | 160 | 34 (21%) |
| `train` | 425 | 69 (16%) |

Only the "actually invisible" column got a teacher target; the roughly 80% of
"long-looking" records Laya can in fact see keep passing through the existing
`prepare_laya_packed_distillation.py` path. Calibration used the full,
1,000-record `td/calibration.jsonl` split regardless of this eligibility split
(300 `choice`, 400 `score`, 300 `noul`; every kind fell into a single
option-count bucket — 3-5, 3-5, and 2 respectively — so the dataset does not
exercise per-bucket temperatures beyond a single bucket per type). **This is
the main limitation of the underlying dataset**: LocalLLaMA/typed-decisions'
states top out at 549 tokens and its option counts don't vary within a
question type, so genuinely long-context and high-cardinality cases are both
rare here. Step 2c's 8k-token states, once available, will produce more of
the former; Banking77-style candidate mode already exercises option-count
variation for a different question.

### Agreement with gold

760 short-state decisions (`td/s0-eval.jsonl`, sampled 300, seed 20260925 —
both teachers can see the full state) and 103 long-state decisions (the
"actually invisible" rows above, `eval` + `train` combined — only the
long-context teacher sees the full state; Laya scores upstream's own
truncated sequence, exactly as serving would today), scored with the teacher's
final calibration (raw-sum scoring, temperatures fit on the full 1,000-record
`calibration.jsonl`). `laya_teacher` here reproduces the released, zero-shot,
unpacked numbers in [Accuracy (step 0)](#accuracy-step-0) closely
(0.393/1.311/0.152 on this short-state sample vs. the reported
0.387/1.308/0.158 on the full 760).

| States | Teacher | Accuracy | Soft CE | ECE | Decisions |
| --- | --- | ---: | ---: | ---: | ---: |
| Short (Laya sees them too) | Long-context teacher, calibrated | **0.640** | **1.018** | **0.104** | 300 |
| Short | Laya, zero-shot | 0.393 | 1.311 | 0.152 | 300 |
| Long (Laya cannot see them) | Long-context teacher, calibrated | **0.767** | **0.980** | 0.233 | 103 |
| Long | Laya, zero-shot, truncated state | 0.291 | 1.424 | **0.200** | 103 |

Per question type, long states:

| Type | Teacher accuracy | Laya accuracy | Teacher soft CE | Laya soft CE | Teacher ECE | Laya ECE |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| choice | 0.824 | 0.196 | 1.058 | 1.630 | 0.323 | 0.166 |
| score | 0.632 | 0.184 | 1.046 | 1.446 | 0.222 | 0.255 |
| noul | 0.929 | 0.929 | 0.518 | 0.616 | 0.225 | 0.342 |

Fitting on the full calibration split (versus the initial run, which fit only
on the ~31 long-state calibration examples eligibility happened to select)
cut the teacher's soft CE by 3-4x on both short and long states and turned an
ECE loss into a near-wash: the teacher now wins accuracy and soft CE outright
in every row above, and ECE in 2 of 3 long-state question types (`choice`
alone still trails Laya's tuned calibration there). This is the expected
result: the earlier run's poor soft CE was a calibration-sample-size artifact,
not evidence the teacher's underlying scores were unreliable.

Timing: 1.6-1.9 s/decision end to end (prompt forward plus branched label
continuations), 21,745-104,890 prompt tokens per run. Peak resident set
(`/usr/bin/time -l`, 300-decision run): 8.94 GB, unchanged from the first
measurement and well under the 18 GB bound. A 1,000-record calibration run
plus 34-decision scoring pass took under 20 minutes of GPU compute (separate
from time spent queued behind other agents' jobs).

### Adopt/reject

**Decision: adopt**, on both short and long states, using the teacher's
calibrated distribution directly (still blended `w = 0.5` with gold per the
existing recipe, for consistency with `prepare_laya_packed_distillation.py`,
not because the blend is needed to offset overconfidence anymore):

- **Accuracy.** Decisive win everywhere: 0.640 vs. 0.393 on short states,
  0.767 vs. 0.291 on the long states this step exists for. Laya's
  truncated-state accuracy on long `choice`/`score` questions (0.196, 0.184)
  is barely above chance.
- **Soft CE.** Also a clear win once temperatures are fit on enough data:
  1.018 vs. 1.311 (short), 0.980 vs. 1.424 (long) — reversed from the
  under-calibrated first pass, where the teacher's soft CE was 3-6x worse
  than Laya's.
- **ECE.** A win on short states (0.104 vs. 0.152) and a near-tie on long
  states (0.233 vs. 0.200), with `choice` the one question type where Laya's
  own tuned calibration still edges out the teacher's single coarse
  temperature. Not a reason to withhold the teacher's targets: `score` and
  `noul` ECE both favor the teacher on long states, and the size of the
  remaining gap is small next to the accuracy and soft-CE swings.

This reverses the initial (pre-recalibration) mixed verdict, which had fit
temperatures only on the ~31 long-state calibration examples eligibility
happened to select and left `score`/`noul` at an unfit `1.0`. The lesson: a
teacher's own calibration is a property of its scoring convention and prompt,
not of which records happen to be labeled-target-eligible, so it should be
fit on the largest gold-labeled pool available, not a subset chosen for a
different reason. Revisit again once step 2c's 8k-token states give a
calibration split that exercises more than one option-count bucket per type.

### Reproduce

```bash
uv run --script scripts/laya/prepare_laya_longcontext_teacher.py \
    td/eval-long.jsonl \
    --teacher-model .tmp/laya/qwen3-14b-4bit \
    --laya-model .tmp/laya/laya-released --common .tmp/laya/common.py \
    --calibration td/calibration.jsonl --compare-laya \
    --output distilled-eval-long.jsonl --metrics-output eval-long-metrics.json
```

`--prefill shared` (the default) reads each case's state once; `--prefill
per-question` restores the original per-question prompts. `--score-all` scores every record regardless of Laya eligibility (used for the
short-state comparison above); `--temperatures <prior output>.json` reuses a
fitted score mode and temperatures instead of recalibrating; `--score-mode
{raw-sum,length-normalized}` overrides the automatic choice. The
long-context-teacher-labelled long-state dataset (585 native records — 585 =
160 `eval` + 425 `train` candidates; 103 actually carry a blended target, the
rest pass through gold unchanged) is at
`.tmp/laya/distilled-longcontext-teacher.jsonl` in the producing worktree, for
step 2c to pick up.

### Teacher throughput

The question: how far can the teacher's labelling rate be pushed? The scaled
packed experiment needs about 10^5 decisions, like the Jev reproductions.

**Setup.**
- Machine: Apple M4 Max, 36 GB, MLX (mlx-lm 0.28 or later).
- Benchmark: `scripts/laya/benchmark_laya_teacher.py`.
- Throughput: 12 cases / 60 decisions from `td/s0-train.jsonl`.
- Quality: all 760 decisions of `td/s0-eval.jsonl` against gold.
- The benchmark does not include `prepare_laya_longcontext_teacher.py`'s
  bookkeeping or its `--compare-laya` pass.

**Where the time goes.**
- The teacher is compute-bound on prefill: about 280-320 tok/s for
  Qwen3-14B-4bit.
- Batching does nothing. Measured at length 256, rates were flat from batch 1
  to batch 16: 325, 283, 271 and 322 tok/s.
- Label continuations are negligible. The prompt forward is essentially the
  whole cost.
- The per-question prompt averages 297 tokens, and the state is 206 of them.
  The teacher re-reads the state once per question.

| Lever | Speed | Quality | Verdict |
| --- | ---: | --- | --- |
| Shared-state prefill: prefill the case's common prefix (the state) once, then fork the KV cache per question | **1.5-1.9×** (0.96-1.07 → 0.52-0.70 s/decision); 2.25× fewer tokens | Exact. The forked cache matches a two-chunk prefill of the same prompt bit for bit (difference 0.0). Both differ from a one-chunk prefill by up to 0.062 in label probability (1 argmax flip in 60, a near tie): bf16/4-bit chunking numerics, not the fork | **adopt** |
| Batching several prompts per forward | none on Metal | — | reject here |
| Smaller teacher (Qwen3-4B-4bit) | 3.3× (0.167 vs 0.534 s/decision, shared prefill) | Accuracy 0.514 vs 0.645 (14B); soft CE 1.117 vs 1.032 (uniform 1.212); agrees with the 14B on 67% of argmaxes. Below the unpacked student it would teach (0.621) | **reject** |
| 4B → 14B cascade (escalate when the 4B's calibrated top probability is below a threshold) | 1.7× at threshold 0.5, 1.2× at 0.6 (the 4B pass runs on every decision) | 0.582 (27% escalated), 0.617 (53%), 0.630 (70%) vs 0.645 | reject: every saving costs accuracy |
| Teacher only where Laya cannot see the state; Laya teacher elsewhere | ~5× fewer teacher calls (16-21% of decisions are invisible to Laya, [above](#how-much-of-the-dataset-is-actually-invisible-to-laya)) | Short states then get Laya-teacher targets, and distilling those did not help (0.451 vs 0.450) | only if quality allows |
| Datacenter GPU with prefix caching (for example vLLM with automatic prefix caching) | estimated ≥30× over this laptop | Same teacher, same targets | **the order-of-magnitude lever** |

The GPU row is an estimate, not a measurement. Prefill on a laptop GPU is
compute-bound, and a datacenter accelerator has well over 10× the
bf16/int8 matmul throughput. Serving stacks with automatic prefix caching get
the shared-state saving for free, and they batch across cases, which pays off
once there is compute headroom.

**Estimated wall-clock for 200k decisions:**
- About 2.5 days per-question on this laptop.
- About 1.5 days with shared-state prefill (0.65 s/decision, measured in the script).
- Hours on one datacenter GPU.

**Recommendation.**
1. Adopt shared-state prefill in `prepare_laya_longcontext_teacher.py`. Done:
   `--prefill shared` is now the default, and `--prefill per-question`
   reproduces earlier runs. Over all 760 decisions of `td/s0-eval.jsonl`
   (`--score-all`, uncalibrated), the script took 0.650 vs 1.304 s/decision
   (**2.0×**) and computed 109,893 vs 266,593 tokens. Accuracy was 0.646 vs
   0.645, and soft CE 5.755 vs 5.759.
2. Keep the 14B teacher.
3. For the 10^5-decision run, move labelling to a rented GPU running the same
   model behind a prefix-caching server. This is the only lever that reaches
   an order of magnitude without giving up label quality.

Stacking the lossy levers (the 4B cascade, Laya-only short states) would reach
about 8-10× on this laptop. The table shows what that costs in accuracy.

The 14B teacher is only 0.024 above the unpacked student on short states
(0.645 vs 0.621), so for short states the teacher's value is calibration and
scale, not accuracy. It is decisively better on long states (0.767 vs 0.291).

Reproduce:

```bash
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-train.jsonl --cases 12
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-eval.jsonl --score-output tacc-14b.jsonl
```

Accuracy is the argmax of the raw label scores against the argmax of `target`.
Soft CE uses one temperature fit on even rows and evaluated on odd rows
(4B: T = 18.2; 14B: T = 14.6).

## Roadmap

Ordered to make Laya more Jev-like at the lowest cost. Each step has a gate.

| Step | Retraining | Status | Gate |
| --- | --- | --- | --- |
| 0. Qualify packed accuracy | fine-tune | question mode **fails** the gate: 0.450 packed vs 0.621 unpacked over three seeds at equal budget (0.471 vs 0.671 on a larger recipe); distillation does not help (0.451); a question-aware trunk (`trunk_sees: "questions"`) reaches 0.554. 33× more data (Open-Jev, soft CE) leaves it at 0.464. A same-data control shows the packed layout itself learns slowly: on Open-Jev validation, unpacked 0.701 vs packed 0.607 at 16k decisions. Per-question upper layers, question-first positions and a pointer head do not close it (best 0.621). Candidate mode: Banking77 0.819 mean over two seeds with soft CE | Packed within noise of unpacked at equal budget on accuracy, soft CE, and ECE, over several seeds |
| 1a. State cache across rows and requests | no | done (CPU and Metal) | Exact against the full row and the oracle; follow-up questions skip trunk projections and feed-forward work |
| 1b. Segment attention | no | done (CPU and Metal); multi-row batching done (CPU and Metal, question and candidate modes) | Work proportional to visible keys; no `[L, L]` masks; physical cap raised to 32,768; cached rows compute branch queries only. Several rows per call: exact against running each row alone, isolated by construction; not yet composed with the trunk cache |
| 1c. Metal and CUDA packed kernels | no | Metal: fused kernels not pursued (encoder GPU work dominates); device scoring gives 2–4%. CUDA: not started | CUDA needs a segment-attention kernel, per-token RoPE, and admission of packed configs before any packed row can run there |
| 1d. Weight quantization (q8_0) | no | done (CPU and Metal). After the CPU kernel fix (dequant+SGEMM), q8_0 matches dense speed on both backends over 760 decisions (CPU 497 vs 491 s, Metal 48 vs 49 s) and saves 37% of Metal memory | Labels identical and probabilities within 2e-2 of dense on the fixture. CPU footprint is still higher than dense (mmap'd dense weights vs allocated quantized bytes; see step 1d), not lower as hoped; a weight-storage-only footprint breakdown is open |
| 2a. Long-context teacher (Qwen3-14B) | labels only | done; see [Long-context teacher (step 2a)](#long-context-teacher-step-2a) | Score each label's likelihood, fit a temperature on gold. Adopt only if it agrees with gold better than the Laya teacher. Extends `prepare_laya_packed_distillation.py` to states Laya cannot see |
| 2b. Two-stage choice for many options | same fine-tune | implemented and measured; **negative result** | Candidate mode shortlists, then one question-mode branch compares the finalists, mirroring Jev's reported procedure. On Banking77, stage 2 made accuracy *worse* than stage 1 alone on the same checkpoint (0.8475 → 0.8350 mean over 2 seeds), despite 99%+ top-8 recall. Not adopted; see [Two-stage choice](#two-stage-choice-roadmap-2b) |
| 2c. 8k states | yes | Fused segment attention op done (forward+backward, no `[L,L]` tensor), admission raised to `seq_len` 8192, both backends (CPU native; Metal on the ModernBERT device kernels without dropout, host-bridged with it), `zig build test -- --test-filter laya` green on CPU and Metal; long-state smoke test at 2k OOM'd under this session's system-wide memory pressure before completing one step (15-22 GB used on a loaded 36 GB machine), 4k/8k not attempted; fine-tune on teacher-labelled long states not started | Forward and gradients match the dense path (unpacked, local window, tree-packed, with and without dropout) on both backends; step time/memory at 4k/8k and a real long-state fine-tune remain open, the former blocked on this machine having headroom to rerun the smoke test |
| 2d. ModernBERT-base student | yes | not started | ~150M parameters, about 2–3× cheaper than Laya-large; keep if its agreement with the teacher stays within tolerance of the large model |

On size and speed: an encoder student beats a small decoder student (for
example Qwen3.5-0.8B, as in `jevre`) at every length targeted here. At 8k it
needs about half the per-token compute. At 32k, ModernBERT-large's ten global
layers make their attention cost comparable to the decoder's. The decoder only
pulls ahead well beyond 32k, where Laya's encoder was not pretrained anyway.

Other open items:

- **Very many options:** candidate branches cannot compare options before the
  softmax. Two-stage choice (step 2b) adds that comparison but currently
  makes accuracy worse, not better, on Banking77 — see
  [Two-stage choice](#two-stage-choice-roadmap-2b) for the negative result
  and untested hypotheses (calibration by shortlist size, hard-negative
  training shortlists).
- **Trainer throughput:** device slices, device-resident gradients, command
  frames, the batched optimizer, device inputs, frozen lower layers, and LoRA
  are done ([Trainer throughput](#trainer-throughput), [LoRA](#lora)).
  LoRA rank 16 on the released model: 1.23 s/step and 14.43 GB peak vs 1.31 s
  and 25.88 GB full fine-tune (remeasured), and step-0 accuracy at or above
  the full-fine-tune baseline over 3 seeds ([LoRA throughput and
  accuracy](#lora-throughput-and-accuracy)); rank 64 not measured. Segment
  attention with a backward pass in the training graph, alongside step 2c,
  is next. A qualification run with frozen layers has not been done.
