# Laya

Laya is the open, encoder-only typed-decision model that Antfly serves through
`POST /ai/v1/extract` (`choice`, `score`, and boolean `noul` questions). This
document covers **tree-packed decisions**: an execution layout where many
questions about one state share a single encoding of that state. It records
the evidence behind the design, the design itself, the training methodology,
and how the implementation is verified.

The released checkpoints, integration placement, and their CPU/Metal/CUDA
qualification are described in
the [support investigation](../../../../../docs/design/inference/history/laya/support-investigation.md)
and the [qualification record](../../../../../docs/design/inference/history/laya/qualification.md).
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
| Encoder | ModernBERT-large (421M total) | mmBERT-base / ModernBERT-large | [model cards](https://huggingface.co/convaiinnovations/laya), [investigation](../../../../../docs/design/inference/history/laya/support-investigation.md) |
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
### Lessons from other decision models

Surveyed 2026-09-26 to 2026-10-04; figures are each project's own. The full
notes are in
[history/laya/2026-09-26-decision-model-research.md](../../../../../docs/design/inference/history/laya/2026-09-26-decision-model-research.md).

- **Isolation is a product choice, and Jev keeps it.** TypeSafe says every
  question is evaluated "in parallel and in isolation against the same
  state". MoJev/Jevre masks the state to itself. Cloudflare Clef instead
  scores questions jointly, so a question-aware trunk
  ([`trunk_sees: "questions"`](#question-aware-trunk)) is a defensible mode.
- **Question-blind, cacheable states work on decoder bases.** MoJev (Qwen3.5-0.8B,
  205k rows), Open-Jev (LoRA on Qwen 2B–27B, 149k rows) and
  [PostHog/jeeves](https://github.com/PostHog/jeeves) (Qwen3.5-9B LoRA,
  pointer head, 19k questions, 0.84 on its test split) all start from
  causal decoders, whose state tokens were never trained to see what follows.
  Jeeves serves a shared-prefix prefill per state, which is our trunk cache.
- **Scale and calibrated teachers separate OpenDecider from Laya.**
  [OpenDecider-nano](https://huggingface.co/manjunathshiva/opendecider-nano)
  is a 400M ModernBERT-family encoder with Laya's scorer and unpacked layout.
  Distilled on about 190,000 questions from calibrated Qwen3-235B and
  DeepSeek V4.1 Flash targets, then fine-tuned, it scores 0.796 on the
  typed-decisions test split against 0.766 for Laya's typed-decisions
  checkpoint.
- **Decoders cost more.** Clef and Amazon Strands Decider 2B show the decoder
  route works, at 2–6× Jev's price per token. An open decoder baseline
  already exists, so building one is not a priority.
- **Confidence-gated escalation** (GLiDE, Jeeves' `nothink_threshold`) fits
  Laya as a fast path in front of a reasoning model.

**Inference (ours).** On the ModernBERT encoder every question-blind layout
tried trails unpacked by about 0.1 or more
([Scaling packed training](#scaling-packed-training-on-open-jev-2026-09-27)),
so a small Qwen decision model with a pointer head and the runtime's prefix
KV cache is the best-supported route to Jev-level accuracy with state reuse.
Antenna needs an encoder ([ANTENNA.md](../antenna/ANTENNA.md)), so its
schema-blind trunk (Antenna step 8) should expect the same accuracy loss.

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
segment-masked (see below), so no `[L, L]` state limits the row. When a
state's branches overflow `max_packed_len`, the packer splits them greedily and repeats the trunk
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
### Experimental layout and head options

These options exist for the accuracy experiments in [Accuracy](#accuracy).
None closes the packed-versus-unpacked gap; all default off.

#### Question-aware trunk

`packing.trunk_sees: "questions"` (default `"state"`) lets state tokens attend
to their whole tree: the state and every question branch about it. Branches
still see only their ancestors, and different states in one call stay
isolated. It restores early fusion at a cost: the trunk depends on the
question set, so the state cache cannot reuse it across requests with
different questions, and adding a question can change another question's
decision. Best packed accuracy so far (0.554 against 0.621 unpacked).

#### Per-question upper layers

`packing.fuse_layers: K` (question mode only, 2 ≤ K ≤ 256, not combined with
`trunk_sees`) runs the top K layers of the encoder-plus-head stack once per
question, over that question's own copy of the state, which also attends to
its question in those layers. Rows hold one question per tree, so questions
stay isolated, and the layers below stay question-blind and cacheable. The
top layer's state output is never read, so K must be at least 2. Fusing the
whole stack reproduces the question-aware trunk on each question
(`laya_packed_test.zig`).

#### Question-first positions

`packing.question_first: true` gives every branch logical positions from 0,
as the question has in the unpacked layout. The trunk starts after the
question budget (`head_max_len` + 3, `laya_tree.trunkOffset`), or as far
right as fits. Only positions change; the trunk stays cacheable.

#### Pointer head

`laya.decision_head: "pointer"` (default `"scorer"`), with `pointer_dim`
(8–4096, default 256), replaces the scorer MLP. It projects the head's output
at the question's `[CLS]` anchor to a query (`pointer.q`) and at each option
marker to a key (`pointer.k`), both through a shared LayerNorm
`pointer.norm`, and scores their dot product over √`pointer_dim`. A new
pointer head is initialized Kaiming-uniform with zero biases and trains in
its own optimizer group at the job's `pointer_lr` (default 1e-3, 10× the
head's rate). The pointer head always scores on the host. A from-scratch
pointer head trails the pretrained scorer by 0.06–0.13 on 16k decisions.

#### OpenDecider format

`laya.format: "opendecider"` serves
[OpenDecider-nano](https://huggingface.co/manjunathshiva/opendecider-nano)
(prepared with `scripts/laya/prepare_opendecider.py`): its scorer is
linear, GELU, LayerNorm, linear, it has no action head, and it refuses
packing, `act_costs`, `head_layers`, and the pointer head. It matches its
PyTorch implementation to 2.8e-6 in probability on every decision of the
typed-decisions test split and reproduces its published 0.796.

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
  backend from `[type_emb; 0]`. On Metal (and WebGPU) the scorer head scores
  on the device from gathered marker and anchor rows; the pointer head and
  other backends read the hidden state back and score on the host.
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
  path's `drop()` does with an all-ones mask (see "Eval dropout" below).
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
  both CPU and Metal.

**Gradient-parity statistic.** On the released-model `relfix` fixture the
per-tensor gate in `training_test.zig` (`5e-5 + 0.2%·magnitude`), tuned for
the tiny synthetic fixture, fails for dense and fused attention alike. Use
the worst relative L2 over `encoder.layers.*` weights instead: about 4% on
CPU and Metal for either path, concentrated in the norm weights. (An earlier
0.4–0.6% figure was a misreading and is withdrawn.)

**Eval dropout.** A job builds its `Program`, with the fused op's graph-time
`dropout_probability`, once and reuses it for training steps and `predict`.
The runtime `apply_dropout` control word, set from the same `training` flag
that gives the dense path its all-ones eval mask, keeps dropout out of the
trainer's own eval and calibration. `training_packed_test.zig` checks
trainer eval against serving the exported model (worst probability error
under 5e-5) on the default and the forced-fused path.

Switching between dense and fused attention changes summation order, so
otherwise identical jobs export slightly different weights (about 2.4% worst
relative L2 on the tiny fixture). That is expected, not a bug.

Step time and memory at 4k and 8k tokens are not yet measured. The one 2k
smoke run, on a 36 GB machine already under memory pressure, ran out of
memory in an encoder MLP matmul before its first step.

> **Relocated:** relfix measurements, the eval-dropout root cause, and the long-state smoke-test log → [history/laya/2026-09-26-fused-training-attention-notes.md](../../../../../docs/design/inference/history/laya/2026-09-26-fused-training-attention-notes.md).

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
was pretrained at 8,192 tokens. The trainer can build the graph with
fused segment attention (`use_fused_attention`, see
[Long states](#long-states-step-2c)) to remove the quadratic admission bound
that used to cap this at about 2k tokens at batch 1; the fine-tune on
teacher-labelled long states itself is still open (step 2c; the step 2a
teacher supplies the labels).

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

### Trainer throughput

Packed question mode, batch 1, released checkpoint, Apple M4 Max,
ReleaseFast: the Metal trainer's median step is **1.38 s**, 6.2× the
original 8.55 s. What makes it fast:

- **Device slices:** strided slices of device tensors run on the GPU
  (`slice_plan` on `decoderRuntimeSliceTypedDevice`).
- **Gradients on the device:** the resident optimizer takes a read-only view
  of each dense device gradient (`resident_training.Request.adopt_f32`), with
  no copy. Only host-backed gradients are downloaded and uploaded.
- **Command frames:** `training.executeFramed` runs each forward and backward
  graph in one Metal command frame and synchronizes once at the end.
  `ANTFLY_LAYA_TRAIN_UNFRAMED=1` restores per-op submission.
- **Optimizer batch:** the resident AdamW transaction
  (`seeded_device_transaction.prepare`) owns one command batch
  (`ComputeBackend.residentTrainingBeginBatch`/`EndBatch`). Only the
  finiteness and norm reductions synchronize it, and it commits before the
  transaction returns. A failed or cancelled transaction discards the batch;
  anything already executed wrote only replacement buffers. Resident ops
  refuse any frame they do not own.
- **Device inputs:** attention biases, RoPE tables, type masks and dropout
  masks are uploaded once per step.
- **Frozen lower layers:** `freeze_layers: N` keeps the token embeddings and
  encoder layers `0..N-1` at their source values. They are not
  differentiated, have no optimizer state, and are exported unchanged. The
  backward graph stops at layer N; the forward still runs every layer.
  `freeze_layers: 11` and `18` cut a 2.02 s step to 1.41 s and 1.17 s.
- **In-frame buffer reuse:** the Metal runtime's in-frame reuse pool takes
  private buffers only. Handing a shared buffer to a private request let a
  previous owner's queued GPU writes land on new data
  (`metal_runtime.zig`, "metal in-frame buffer reuse never hands a
  host-writable buffer to a private request").

What remains per 1.38 s step: about 0.13 s forward and 0.34 s backward on the
GPU, 0.21 s in the optimizer transaction (snapshot copies and full-state
finiteness reads), 0.07 s building inputs, and 0.05 s encoding. Cutting the
optimizer further needs a transaction-contract change (skip re-validating
already committed state, write AdamW out of place). Larger batches help
little, because cost is per token.

**Half precision is not adopted.** An f16-operand path was 3–4% slower, and
without loss scaling its worst gradient error over `encoder.layers.*` rose
from about 2% to 353% against float64 PyTorch. Caching half weights keyed on
buffer address served stale casts after Metal reused an address. A future
attempt needs loss scaling or bf16, per-layer precision validated against the
float64 gradient check, and a cache keyed on the binding. Branch
`laya/half-precision-training` keeps the attempt.

**Running jobs.** Run one training job at a time on a small machine. macOS
throttles a trainer launched from a background shell to a few steps per
hour; run it in the foreground or clear the policy (`taskpolicy -B -p <pid>`)
and keep the display awake.

### LoRA throughput and accuracy

LoRA keeps the full forward graph but drops the optimizer state of every
targeted weight. Rank 16 on encoder and head, same job as above: 1.23 s per
step and 14.43 GB peak footprint, against 1.31 s and 25.88 GB for a full
fine-tune. On the step-0 recipe it scores 0.471 mean over three seeds
against 0.450 for the full fine-tune: not worse, but three seeds cannot
confirm an improvement. Rank 64 is not measured.

### Training memory

The standard packed recipe on the released checkpoint peaks at about 24–26 GB
footprint on a 36 GiB M4 Max. About 15 GiB of that is Metal unified memory
that RSS does not count. Footprint grows slightly with the longest bucketed
example seen, not per step. The resident AdamW snapshot-then-swap is the
dominant transient; it keeps the transaction all-or-nothing.

**Admission.** `execute` estimates the device need right after selecting
trainable parameters, before any device weight or optimizer-state
allocation, logs it on every Metal job, and refuses with
`LayaBackendMemoryLimitExceeded` above `max_backend_bytes` (default 32 GiB,
range 256 MiB–64 GiB; ignored on `backend: cpu`, which is bounded by
`max_host_bytes`, default 24 GiB). The estimate is:

- **Weights and optimizer state (exact):** weight, m, v and the gradient
  accumulator stay resident (4× trainable f32 bytes), and each optimizer step
  snapshots weight/m/v first, so the transient peak is 7×. Frozen tensors add
  1×.
- **Activations (approximate):** per encoder layer, about twelve hidden-width
  buffers, two GeGLU intermediate-width buffers, and eight dense
  `[batch, heads, seq, seq]` buffers, summed over the admitted layouts and
  over forward layers plus backward-retained (unfrozen) layers.
- **Fixed overhead (measured):** 4 GiB for MPS caches, the in-frame reuse
  pool, per-step runtime inputs, and driver bookkeeping.

For the standard packed recipe the estimate is 22.9 GB against a measured
25.2–25.9 GB combined footprint, which includes host-side loading. It assumes
every microbatch is a stepped update, so it overestimates jobs with
`gradient_accumulation` above 1.

**Process-wide admission.** `execute` also takes a lease from the runtime's
`memory.AdmissionController` (`.gpu` class for `backend: metal`, `.cpu`
otherwise), charging the device estimate as `backend_scratch_bytes`.
`max_host_bytes` is a safety-net ceiling enforced by `BoundedAllocator` and
is not charged. The controller's live system-memory check runs only for Metal
jobs with `check_live_memory: true` (default false). It reserves a quarter
of physical memory as serving headroom, so on a 36 GB machine it refuses
every full Laya fine-tune; with it on, two concurrent jobs on macOS are
checked against one live view of unified memory. The CLI
(`train_laya.zig`) creates one controller per process; an embedding process
can share its own so training and resident models compete for one ledger.

`report.json` records `backend_estimated_bytes`, `backend_peak_bytes` (the
Metal runtime's `device_owned_peak_live_bytes`, reset at admission), and
`host_peak_bytes`.

> **Relocated:** the per-change step-time tables, half-precision, LoRA, and memory measurement logs → [history/laya/2026-09-25-trainer-measurements.md](../../../../../docs/design/inference/history/laya/2026-09-25-trainer-measurements.md). Raw logs: [trainer throughput](../../../../../docs/design/inference/history/laya/2026-09-25-trainer-throughput.md), [memory admission](../../../../../docs/design/inference/history/laya/2026-09-25-training-memory-admission.md).

## Accuracy

All accuracy work runs on an Apple M4 Max, ReleaseFast, Metal trainer, and
scores every model through the serving pipeline with its calibration
applied (`antfly inference finetune eval laya <model> <records>`,
`finetune/laya/evaluate.zig`). The dated runs behind this section are in
[history/laya/2026-09-24-packed-accuracy-qualification.md](../../../../../docs/design/inference/history/laya/2026-09-24-packed-accuracy-qualification.md)
and the files it links.

### Accuracy (step 0)

**Data.** [LocalLLaMA/typed-decisions](https://huggingface.co/datasets/LocalLLaMA/typed-decisions)
at `c76749ec58bd8c3d2ea706b31c333a9059c38f90` (`all` config): 1,200 train and
400 test cases in four workflows, with five questions per case. Train is
shuffled with seed 20260924, and 50 cases per workflow are held out for
calibration. Cases whose state exceeds 316 tokens are dropped from every
split, which guarantees any question fits the 512-token budget packed or
unpacked. The step-0 subsets are 100 train, 20 calibration, and 38 eval cases
per workflow: 400 / 80 / 152 cases, or 2,000 / 400 / 760 decisions
(`s0-train`, `s0-calibration`, `s0-eval`).

**Recipe.** Start from the released `convaiinnovations/laya`, one epoch,
RLCD, default learning rates (encoder 2.5e-5, head 1e-4), batch 1,
temperatures fitted on the calibration split. Packed runs take one packed
case per step; unpacked runs use gradient accumulation 5, so every optimizer
update sees five decisions in both.

**Seeds.** Seed-to-seed spread is large at this budget (SD 0.014–0.11
depending on the trainer), so every accuracy claim needs at least three
seeds. Freezing the lower 11 of 28 encoder layers costs no accuracy (0.512
mean against 0.450) and trains 1.4× faster.

Reference points on `s0-eval`: released `laya` zero-shot 0.387 unpacked and
0.361 packed; upstream `laya-typed-decisions` 0.754, trained on all 1,200
cases with a 1,024-token budget.

### Packed vs unpacked at equal budget (2026-09-26)

Same trainer, data, and recipe, three seeds each:

| Layout | Mean accuracy | SD | Soft CE | ECE |
| --- | ---: | ---: | ---: | ---: |
| Packed, trunk sees the state only (default) | 0.450 | 0.014 | 1.129 | 0.049 |
| Packed, distilled targets (50/50 with an unpacked teacher) | 0.451 | | 1.135 | 0.033 |
| Packed, [question-aware trunk](#question-aware-trunk) | 0.554 | 0.033 | 1.031 | 0.060 |
| Unpacked | 0.621 | 0.020 | 0.983 | 0.088 |

**The step-0 gate fails.** At equal data and optimizer budget, question-mode
packing costs about 0.17 accuracy on typed-decisions, and a larger recipe
(915 cases) does not close it (0.471 against 0.671). The original single-seed
parity (0.574 against 0.572) was a lucky packed run. Distillation improves
only calibration. Letting the trunk see its questions closes about 60% of the
gap, at the cost of trunk reuse and question isolation. Packed question mode
is therefore for cost-sensitive uses; route accuracy-sensitive decisions to
the unpacked layout.

### Scaling packed training on Open-Jev (2026-09-27)

This tested whether scale closes the gap, as the decoder-based Jev
reproductions suggest. Data: [Open-Jev](https://huggingface.co/datasets/ZefanCai/Open-Jev)
`release-v2-redistributable` (CC0), converted with
`scripts/laya/prepare_laya_openjev.py`, which excludes `customer-control-v1`
(its question text has no verified license) and drops states that do not fit
512 tokens. Mixed with `s0-train`, it gives 66,450 decisions; a 16,009-decision
subset (`td/oj16k.jsonl`) serves as the same-data control. Open-Jev states
carry up to 51 questions, so these runs use `max_packed_len` 704 (at 2,048
the admission estimate was 137 GB); the loader accepts up to 1 GiB per file
and 1M records.

Soft CE, seed 42, one epoch (Open-Jev validation is a 2,002-decision sample;
its label prior scores 0.600):

| Layout | Training data | Open-Jev val | `s0-eval` |
| --- | --- | ---: | ---: |
| Packed | 66k mix | 0.664 | 0.464 |
| Packed | 16k subset | 0.607 | 0.463 |
| Packed, [question-first positions](#question-first-positions) | 16k subset | 0.621 | 0.476 |
| Packed, [pointer head](#pointer-head) | 16k subset | 0.546 | 0.334 |
| Fused, [K = 10](#per-question-upper-layers) | 16k subset | 0.601 | 0.457 |
| Fused, K = 30 (whole stack) | 16k subset | 0.605 | 0.476 |
| **Unpacked** | 16k subset | **0.701** | **0.607** |

- **RLCD diverges on large mixes.** The same 66k run with RLCD never beat a
  uniform guess (0.375 on `s0-eval`, gradient norms up to 4,400). Use
  `"objective": "soft_ce"` for large mixes and many-option questions.
- **The packed layout learns slowly.** Unpacked on 16k beats packed on 66k.
  Out-of-domain data does not move `s0-eval` for either layout.
- **The gap is the encoder base.** Question-blind lower layers (K = 30 fuses
  every layer), token order (question-first), and the decision head (pointer)
  each leave it open. The scale hypothesis is rejected for this encoder.
- **Rare-token anchors are already in place:** option markers are `[MASK]`,
  decision anchors are `[CLS]`, and `[SEP]` closes trunk, question and
  options. Only the plain-text `"<type> question:"` prefix is untested, and
  the type embedding already carries it.
- **Metal training fixes found here:** a fused gather of `add(matrix, bias)`
  with integer indices now takes the typed gather (test "laya resident Metal
  gathers rows of a LayerNorm output"), and transposed-left `dot_general`
  runs on the device (#913).

> **Relocated:** the full Open-Jev runs, per-kind tables, and training curves → [history/laya/2026-09-27-scaling-packed-training.md](../../../../../docs/design/inference/history/laya/2026-09-27-scaling-packed-training.md).

### Candidate mode on Banking77 (step 0b)

`scripts/laya/prepare_laya_banking77.sh` writes one 77-way `choice` question
per Banking77 message (20 train and 5 calibration examples per intent, 400
eval messages from the test split). Antfly's unpacked pipeline caps choice
questions at 20 options, so only a candidate-packed model can serve them.

A candidate-packed soft-CE fine-tune reaches **0.819** mean over two seeds
(0.828, 0.810), against 0.348 for the released checkpoint through upstream's
own code (`scripts/laya/laya_upstream_baseline.py`) and Jev's published
zero-shot 0.870. The fine-tune saw 20 in-domain examples per intent, so this
is not a like-for-like comparison. RLCD diverges to uniform with 77 options
at batch 1. Training takes about 1.2 s per step on Metal.

> **Relocated:** the Banking77 tables and step-0 trainer-throughput notes → [history/laya/2026-09-26-banking77-candidate-mode.md](../../../../../docs/design/inference/history/laya/2026-09-26-banking77-candidate-mode.md).
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

**Result: not adopted.** On Banking77 (`top_k = 8`, two seeds), stage 2
lowers accuracy against stage 1 alone on the same checkpoint (0.8350 against
0.8475 mean), and soft CE and ECE get worse, even though stage 1's top-8
shortlist holds the gold label 99% of the time. Stage 2 adds 66 tokens per
decision. Stage 1 of the two-stage checkpoint beats the earlier
candidate-only baseline (0.8475 against 0.819) only because the mixed recipe
trains longer. Untested next steps: per-option-count temperatures
(`temperature_by_options`, since one `choice` temperature now mixes 77-way
and 8-way rows), hard-negative shortlists from a stage-1 checkpoint's own
confusions, a larger `top_k`, and `mass_cutoff` shortlists.

> **Relocated:** the Banking77 two-stage measurements → [history/laya/2026-09-25-two-stage-banking77-results.md](../../../../../docs/design/inference/history/laya/2026-09-25-two-stage-banking77-results.md).

### Base-size encoder (step 2d, 2026-10-03)

Two ModernBERT-base trunks with a fresh 768-wide head
(`scripts/antenna/init_decision_head.py`) were trained unpacked on the step-0
recipe, three seeds each: Laya-large 0.621, the Antenna trunk
([ANTENNA.md](../antenna/ANTENNA.md)) 0.505, plain ModernBERT-base 0.439.
Base trunks take half the train time and 44% of the peak memory (11.5 GB
against 26.4 GB). **The gate is not met.** GLiNER feature distillation helps
decisions (Antenna-base beats ModernBERT-base by 0.07), but fine-tuning the
Antenna trunk buys little over a head on the frozen trunk (0.511). A
three-epoch recipe is unstable across seeds; use the step-0 recipe for
comparisons.

> **Relocated:** per-seed tables → [history/laya/2026-10-03-base-size-encoder.md](../../../../../docs/design/inference/history/laya/2026-10-03-base-size-encoder.md).

### Community benchmark

Every model on the full typed-decisions test split (2,000 decisions), scored
against gold labels with `scripts/laya/typed_decisions_bench.py --gold`.
States longer than a checkpoint's budget are cut (`--truncate-state`), as
upstream does. The gold label and the argmax of the gold distribution
disagree on 31 decisions, so these numbers differ slightly from
`finetune eval laya`, which scores the argmax.

| Model | Accuracy | Choice | Score | Yes/no | ECE |
| --- | ---: | ---: | ---: | ---: | ---: |
| OpenDecider-nano, served natively (Metal) | **0.796** | 0.762 | 0.769 | 0.867 | 0.164 |
| Laya typed-decisions checkpoint (published) | 0.766 | 0.733 | 0.723 | 0.857 | |
| Antenna-base, human mix + debiased synthetic, then train split (2 seeds) | 0.750 | | | | |
| Antenna-base, train split only, 3 epochs (3 seeds) | 0.697 | | | | |
| Laya-large, step-0 fine-tune (seed 42) | 0.627 | 0.633 | 0.560 | 0.712 | 0.088 |
| Antenna-base, step-0 fine-tune (3 seeds) | 0.533 | | | | |
| ModernBERT-base, step-0 fine-tune (3 seeds) | 0.472 | | | | |
| Released Laya-large, zero-shot | 0.361 | 0.288 | 0.323 | 0.487 | 0.175 |

**The gap is training data.** OpenDecider-nano and our step-0 fine-tunes share
the unpacked layout, the scorer, and similar base sizes.

### Decision mix

`scripts/laya/build_decision_mix.py` builds a permissively licensed (no
CC BY-SA) mix of human-labelled decisions: MASSIVE, HuffPost, Banking77
(train), GoEmotions, Civil Comments, SMS Spam, WANLI and PAWS, recast as
choice, score and yes/no questions, pinned and SHA-checked, with gold
targets label-smoothed by 0.1. `scripts/laya/generate_decision_cases.py`
adds synthetic agent-workflow cases over 30 workflows absent from the
benchmark, labelled by a calibrated Qwen3 teacher (step 2a's scorer). Rules:

- **Debias synthetic data** with `scripts/laya/debias_records.py`: shuffle
  every choice question's options and balance yes/no questions 50/50. The
  generator puts the right option first and writes statements that hold;
  undebiased, it dropped accuracy to 0.504.
- **Count tokens exactly** with `build_decision_mix.py fits`, including
  yes/no default descriptions and the 48-token option cap: one record over
  budget fails the whole job with `ExtractionTextLimitExceeded`.
- **Two stages:** one epoch on the mix (RLCD, encoder 2.5e-5, head 1e-4,
  accumulation 5), then three epochs on the typed-decisions train split. The
  mix alone does not transfer (0.30–0.47 before stage b).

The mix lifts Antenna-base from 0.697 to 0.750 on two seeds. The Qwen3-32B
teacher is weaker than the student (0.651 on `s0-eval`), so its soft targets
add coverage of agent-style states, not accuracy.

> **Relocated:** priorities, recipe runs, and benchmark notes → [history/laya/2026-10-04-decision-mix-and-benchmarks.md](../../../../../docs/design/inference/history/laya/2026-10-04-decision-mix-and-benchmarks.md).

## Verification

Measured on an Apple M4 Max (36 GiB), Zig 0.16.0.

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
| Released-model gradients vs float64 PyTorch, fused attention (relfix) | `finetune/laya/training_test.zig` ("... (fused segment attention)") | a measurement, not the pass/fail gate; see "Gradient-parity statistic" under [Long states](#long-states-step-2c) |
| Trainer's own eval predictions vs. serving the exported model (same weights), default admission-gated and forced fused attention | `finetune/laya/training_packed_test.zig` (two cases) | max probability error ~3e-8 both ways, unchanged `worst < 5e-5` bound (see "Eval dropout" under [Long states](#long-states-step-2c)) |
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
  zig build test -Doptimize=fast -- --test-filter "laya packed benchmark"
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

Packed Metal rows score on the device: the scorer gathers marker and anchor
rows from the device-resident hidden state, worth 2–4% at 16–64 questions.
Metal embedding lookups read device tables in place, and the host-address
table cache keeps only immutable model storage. (A cache keyed on buffer
identity once returned stale rows after the buffer pool reused a table's
buffer.) The packed Metal path is bound by encoder GPU work, so routing it
through the fused resident Laya kernels (step 1c) is not expected to pay off.

> **Relocated:** how the cost numbers evolved (host RoPE, dense masks, device scoring, the stale embedding-table cache) → [history/laya/2026-09-25-serving-cost-history.md](../../../../../docs/design/inference/history/laya/2026-09-25-serving-cost-history.md). Raw benchmark output: [history/laya/2026-09-24-tree-packing.md](../../../../../docs/design/inference/history/laya/2026-09-24-tree-packing.md).

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
  zig build test -Doptimize=fast -- --test-filter "laya packed benchmark"
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

Released `laya` (unpacked) on the 760 `s0-eval` decisions, one job at a time:

| Weights | Backend | Accuracy | Soft CE | Peak footprint | Eval time |
| --- | --- | ---: | ---: | ---: | ---: |
| dense | Metal | 0.3868 | 1.3080 | 7.92 GB | 48.6 s |
| q8_0 | Metal | 0.3842 | 1.3067 | **5.02 GB** | 48.1 s |
| dense | CPU | 0.3868 | 1.3080 | 1.06 GB | 490.5 s |
| q8_0 | CPU | 0.3842 | 1.3067 | 1.53 GB | 497.4 s |

q8_0 runs at dense speed on both backends and saves 37% of Metal memory;
two of 760 decisions change. It does not meet the 5e-5 exact-serving bound
and is not meant to: `pipelines/laya_quantized_test.zig` requires identical
labels and probabilities within 2e-2. The fused resident Metal path reads
dense weights, so a quantized checkpoint runs the generic encoder. CPU
footprint stays above dense because dense weights are a read-only mmap that
the footprint barely counts, while Q8_0 bytes are allocated.

**CPU kernel routing.** `shouldUseLayaDequantSgemm`
(`ops/native_compute.zig`) routes Laya's encoder and head Q8_0 linears
through `layaDequantScratchSgemm`: dequantize into a scratch buffer freed
every call, then the same `dispatchSgemmTransB` the dense path uses. No
persistent dense mirror is kept, and `loadWeight` skips
`ensurePreparedKBlock` for these weights, so the int8 kernel's row-major and
panel copies are never built. The predicate matches **runtime** weight keys
(`model.layers.N…`, `model.head.layers.N…` after
`session_factory.normalizeWeightKey`), not checkpoint names; the test "laya
dequant sgemm predicate matches normalized runtime keys, not checkpoint
names" pins both spellings.

> **Relocated:** the pre-fix measurements, the diagnosis, and the 200-decision re-measurement → [history/laya/2026-09-25-weight-quantization.md](../../../../../docs/design/inference/history/laya/2026-09-25-weight-quantization.md). Fix log: [history/laya/2026-09-26-q8_0-cpu-kernel-fix.md](../../../../../docs/design/inference/history/laya/2026-09-26-q8_0-cpu-kernel-fix.md).

## Long-context teacher (step 2a)

Step 2a provides soft labels for typed-decisions states that Laya's
512-token budget cannot see, so a long-context Laya (step 2c) has something
to distil from besides gold.

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

### Results and decision

Only about a fifth of the states over 316 raw tokens are truly invisible to
Laya under the exact `state_fits` budget (34 of 160 `eval`, 69 of 425
`train`); the rest fit because their questions use fewer label tokens. The
dataset's states top out at 549 tokens, and option counts do not vary within
a question type, so long-context and per-bucket calibration are both thinly
exercised.

With temperatures fit on the full 1,000-record calibration split (raw-sum
scoring):

| States | Teacher | Accuracy | Soft CE | ECE | Decisions |
| --- | --- | ---: | ---: | ---: | ---: |
| Short (Laya sees them too) | Long-context teacher | **0.640** | **1.018** | **0.104** | 300 |
| Short | Laya, zero-shot | 0.393 | 1.311 | 0.152 | 300 |
| Long (Laya cannot see them) | Long-context teacher | **0.767** | **0.980** | 0.233 | 103 |
| Long | Laya, zero-shot, truncated state | 0.291 | 1.424 | **0.200** | 103 |

**Decision: adopt** on short and long states, blended `w = 0.5` with gold as
in `prepare_laya_packed_distillation.py`. Fit the teacher's temperatures on
the largest gold-labelled pool available, not on the subset that happens to
be eligible for teacher targets: fitting on the ~31 eligible calibration
records alone made its soft CE 3–6× worse. Revisit once 8k-token states give
a calibration split with more than one option-count bucket per type.

The teacher peaks at 8.94 GB resident and takes 1.6–1.9 s per decision with
per-question prefill.

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

The teacher is compute-bound on prefill (about 280–320 tok/s for
Qwen3-14B-4bit on an M4 Max), batching prompts does nothing on Metal, and
label continuations are negligible. Measured with
`scripts/laya/benchmark_laya_teacher.py`:

| Lever | Speed | Verdict |
| --- | ---: | --- |
| Shared-state prefill (prefill the state once, fork the KV cache per question) | 2.0× over all 760 `s0-eval` decisions (0.650 against 1.304 s/decision), same accuracy | **adopted**: `--prefill shared` is the default |
| Smaller teacher (Qwen3-4B-4bit) | 3.3× | rejected: accuracy 0.514 against 0.645, below the student it would teach |
| 4B → 14B cascade by confidence | 1.2–1.7× | rejected: every saving costs accuracy |
| Teacher only where Laya cannot see the state | ~5× fewer calls | only if quality allows (Laya-teacher targets did not help) |
| Datacenter GPU with automatic prefix caching | estimated ≥30× | **the order-of-magnitude lever** for 10^5-decision runs |

Keep the 14B teacher. It is only 0.024 above the unpacked student on short
states (0.645 against 0.621), so there its value is calibration and scale;
on long states it is decisively better (0.767 against 0.291). Labelling
200k decisions takes about 1.5 days on the laptop with shared prefill.

```bash
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-train.jsonl --cases 12
uv run --script scripts/laya/benchmark_laya_teacher.py .tmp/laya/qwen3-14b-4bit td/s0-eval.jsonl --score-output tacc-14b.jsonl
```

Accuracy is the argmax of the raw label scores against the argmax of
`target`. Soft CE uses one temperature fit on even rows and evaluated on odd
rows.

> **Relocated:** the eligibility, agreement, adopt/reject, and throughput measurements → [history/laya/2026-09-26-longcontext-teacher-evaluation.md](../../../../../docs/design/inference/history/laya/2026-09-26-longcontext-teacher-evaluation.md). Raw log: [history/laya/2026-09-26-longcontext-teacher.md](../../../../../docs/design/inference/history/laya/2026-09-26-longcontext-teacher.md).

## Roadmap

Ordered to make Laya more Jev-like at the lowest cost. Each step has a gate.

| Step | Retraining | Status | Gate |
| --- | --- | --- | --- |
| 0. Qualify packed accuracy | fine-tune | **fails**: 0.450 packed against 0.621 unpacked over three seeds; no layout, head, data-scale, or distillation variant closes it (best 0.554, question-aware trunk). Candidate mode: Banking77 0.819 | Packed within noise of unpacked at equal budget on accuracy, soft CE, and ECE, over several seeds |
| 1a. State cache across rows and requests | no | done (CPU and Metal) | Exact against the full row and the oracle; follow-up questions skip trunk projections and feed-forward work |
| 1b. Segment attention | no | done (CPU and Metal); multi-row batching done (question and candidate modes) | Work proportional to visible keys; no `[L, L]` masks; physical cap 32,768; cached rows compute branch queries only. Several rows per call: exact against each row alone; not yet composed with the trunk cache |
| 1c. Metal and CUDA packed kernels | no | Metal: fused kernels not pursued (encoder GPU work dominates); device scoring on. CUDA: not started | CUDA needs a segment-attention kernel, per-token RoPE, and admission of packed configs before any packed row can run there |
| 1d. Weight quantization (q8_0) | no | done (CPU and Metal): dense speed on both, 37% less Metal memory | Labels identical and probabilities within 2e-2 of dense on the fixture |
| 2a. Long-context teacher (Qwen3-14B) | labels only | done, adopted; see [Long-context teacher](#long-context-teacher-step-2a) | Agrees with gold better than the Laya teacher |
| 2b. Two-stage choice for many options | same fine-tune | implemented; **not adopted** (stage 2 lowers Banking77 accuracy); see [Two-stage choice](#two-stage-choice-roadmap-2b) | Better than stage 1 alone on the same checkpoint |
| 2c. 8k states | yes | fused segment training attention done on CPU and Metal, admission raised to `seq_len` 8192; 4k/8k step time and memory unmeasured; fine-tune on teacher-labelled long states not started | Forward and gradients match the dense path on both backends (met); a real long-state fine-tune |
| 2d. ModernBERT-base student | yes | measured; gate **not met** (Antenna-base 0.505, ModernBERT-base 0.439, Laya-large 0.621); see [Base-size encoder](#base-size-encoder-step-2d-2026-10-03) | ~150M parameters within tolerance of the large model |

On size and speed: an encoder student beats a small decoder student (for
example Qwen3.5-0.8B, as in `jevre`) at every length targeted here. At 8k it
needs about half the per-token compute. At 32k, ModernBERT-large's ten global
layers make their attention cost comparable to the decoder's. The decoder only
pulls ahead well beyond 32k, where Laya's encoder was not pretrained anyway.

## Open work

In priority order after the decision-model survey:

1. **Teacher-distilled data at scale.** The [decision mix](#decision-mix)
   lifts Antenna-base to 0.750; a third seed and a 25,000-case debiased
   synthetic set are next. Packed training at scale is untested.
2. **Question-aware trunk as a supported mode,** if scale does not close the
   packed gap.
3. **Confidence-gated escalation** to a reasoning model, at the product
   level.
4. **Long states (step 2c):** step time and memory at 4k and 8k, then a
   fine-tune on teacher-labelled long states. Dropout on the Metal device
   training kernels.
5. **Two-stage choice:** per-option-count temperatures and hard-negative
   shortlists (see [Two-stage choice](#two-stage-choice-roadmap-2b)).
6. **Trainer:** the optimizer transaction-contract change; trunk-cache reuse
   for merged multi-row batches; LoRA rank 64; a qualification run with
   frozen layers.
7. **CUDA packed rows (step 1c).**
8. **q8_0 footprint:** a weight-storage-only breakdown on CPU and a full
   760-decision re-run at the fixed revision.

Done from the survey: OpenDecider-nano is served (`laya.format:
"opendecider"`), and every checkpoint is scored on the
[community benchmark](#community-benchmark).
