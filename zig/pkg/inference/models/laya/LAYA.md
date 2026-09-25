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
At equal training budget on LocalLLaMA/typed-decisions, a packed fine-tune
matches an unpacked one (accuracy 0.574 vs 0.572) and trains 3.4× faster (see
[Accuracy (step 0)](#accuracy-step-0)). Candidate mode is not yet qualified.

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

### Runtime

| Piece | Location |
| --- | --- |
| Packer, visibility, masks, row validation | `src/pipelines/laya_tree.zig` |
| Pipeline: group tasks by state text, one session run per row, request order preserved | `src/pipelines/laya.zig` (`executePacked`) |
| Session contract and one-row forward | `src/architectures/laya_packed.zig` |
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
  (below).
- **Fused Metal kernels:** the resident Laya kernels (`ops/laya_metal.zig`)
  are not used for packed rows.
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

- **Storage:** on Metal, entries are retained device tensors captured in place
  (`MetalCompute.retainDenseDeviceTensor`), so a hit uploads nothing. On CPU
  they are host f32.
- **Budget:** `ANTFLY_LAYA_TRUNK_CACHE_MB` (default 256; 0 disables). An entry
  costs `2 · (encoder + head layers) · T · hidden · 4` bytes, about 100 MB for
  a 400-token state on the released checkpoint. So the default holds only a
  couple of long states. Pinned entries are never evicted. The memory is
  outside model admission, like the native dequantization cache.
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
3. **Train** with `packing` set and a held-out calibration split. The RLCD
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
was pretrained at 8,192 tokens. That in turn needs a non-materialized
attention in the training graph, because the current graph admits
`batch · L² · heads ≤ 64M` elements, about 2k tokens at batch 1.

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

**Result.** At equal data and optimizer budget, the packed model is within
noise of the unpacked one on every metric. That is the gate: losing early
fusion (the state no longer sees the question) did not cost accuracy here.
Packed training took 3.4× less wall time, and packed evaluation processed
2.6× fewer tokens (81,745 vs 211,125).

**Caveats.**

- **Upstream checkpoint:** it is far ahead because it trained on all 1,200
  cases for longer and with a 1,024-token budget. It shows what more training
  buys, not a packed-versus-unpacked difference.
- **Released weights in the packed layout:** these score near zero-shot chance,
  like the unpacked released model. Upstream reports 0.362 unpacked zero-shot,
  so the layout change alone does not break the model.
- **Candidate mode:** not yet qualified. Its target, Banking77, is still to
  run.

**Training throughput.** Steady state on this machine: ~5–6 s per unpacked
decision, and ~6–9 s per packed case of five decisions. Several things turned
up along the way:

- **Bucketing:** sequences are bucketed to 64 tokens and options to 4 so
  compiled programs are reused. Parity is unchanged, but it did not shorten
  steps, so graph construction is not the bottleneck.
- **Batch size:** `batch_size` 4 raised throughput only from 0.88 to 1.16
  decisions/s, and 8 gave 1.10. Cost grows about linearly with tokens.
- **Where the time goes:** the time is per-token work in the trainer's
  materialized attention and host/device traffic, not per-step overhead.
- **Environment:** peak memory is ~27.5 GB. A concurrent 10 GB Zig build got
  the trainer SIGKILLed. macOS also throttles a trainer launched from a
  background shell (nice 5, background scheduling policy, display sleep) to a
  few steps per hour; run it in the foreground or clear the policy
  (`taskpolicy -B -p <pid>`) and keep the display awake. The raw logs are in
  the work log.

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
| CPU segment kernel equals dense masked softmax (three ranges, window, fewer queries than keys) | `lib/linalg/src/attention.zig` | < 1e-5 |
| Segment attention equals dense tree-masked attention on the session backend, all queries and branch queries only | `pipelines/laya_packed_test.zig` | 2.4e-7 (CPU), 3.6e-7 (Metal) |

Reproduce the fixture-backed tests:

```bash
curl -sfLo common.py https://raw.githubusercontent.com/NandhaKishorM/laya/6a5819129eb220570792e417e49723d697efd76f/laya/common.py
cd scripts/laya
uv run --script laya_reference.py --common ../../common.py --output /tmp/laya-ref
uv run --script laya_training_reference.py --fixture /tmp/laya-ref --common ../../common.py
uv run --script laya_packed_reference.py --fixture /tmp/laya-ref --common ../../common.py
cd ../../zig/pkg/inference
ANTFLY_LAYA_REFERENCE=/tmp/laya-ref zig build test -- --test-filter "laya"
ANTFLY_LAYA_REFERENCE=/tmp/laya-ref ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_METAL=1 zig build test -- --test-filter "laya"
```

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

The full raw output of every run is in
[`work-log/completed/inference/laya/2026-09-24-tree-packing.md`](../../../../../work-log/completed/inference/laya/2026-09-24-tree-packing.md).

## Roadmap

Ordered to make Laya more Jev-like at the lowest cost. Each step has a gate.

| Step | Retraining | Status | Gate |
| --- | --- | --- | --- |
| 0. Qualify packed accuracy | fine-tune | question mode passed on typed-decisions (packed 0.574 vs unpacked 0.572 at equal budget); candidate mode on Banking77 not run | Packed within noise of unpacked at equal budget on accuracy, soft CE, and ECE |
| 1a. State cache across rows and requests | no | done (CPU and Metal) | Exact against the full row and the oracle; follow-up questions skip trunk projections and feed-forward work |
| 1b. Segment attention | no | done (CPU and Metal); multi-row calls not started | Work proportional to visible keys; no `[L, L]` masks; physical cap raised to 32,768; cached rows compute branch queries only. Several rows per call remain, which needs a per-row segment contract |
| 1c. Metal and CUDA packed kernels | no | not started | Tree masks and explicit positions in the fused Metal and CUDA kernels (the generic Metal path, device cache, and in-stream row joins are done) |
| 1d. Weight quantization (q8_0) | no | not started | Probability error within the existing 5e-5 qualification bound |
| 2a. Long-context teacher (Qwen3.8-27B) | labels only | not started | Score each label's likelihood, fit a temperature on gold. Adopt only if it agrees with gold better than the Laya teacher. Extends `prepare_laya_packed_distillation.py` to states Laya cannot see |
| 2b. Two-stage choice for many options | same fine-tune | not started | Candidate mode shortlists, then one question-mode branch compares the finalists, mirroring Jev's reported procedure. Measured on Banking77 |
| 2c. 8k states | yes | not started | Memory-efficient attention in the training graph (today about 2k tokens at batch 1), `max_len` 8192 (ModernBERT's pretraining length), fine-tune on teacher-labelled long states |
| 2d. ModernBERT-base student | yes | not started | ~150M parameters, about 2–3× cheaper than Laya-large; keep if its agreement with the teacher stays within tolerance of the large model |

On size and speed: an encoder student beats a small decoder student (for
example Qwen3.5-0.8B, as in `jevre`) at every length targeted here. At 8k it
needs about half the per-token compute. At 32k, ModernBERT-large's ten global
layers make their attention cost comparable to the decoder's. The decoder only
pulls ahead well beyond 32k, where Laya's encoder was not pretrained anyway.

Other open items:

- **Very many options:** candidate branches cannot compare options before the
  softmax (step 2b).
- **Cache memory:** the state cache stores f32. f16 storage or eviction tied to
  model admission would let it hold more states.
- **Trainer throughput:** steps are bound by per-token work (materialized
  attention, host/device traffic), not per-step overhead. Segment attention
  with a backward pass in the training graph, device-resident gradients, and
  LoRA or frozen lower layers are the next levers, alongside step 2c.
