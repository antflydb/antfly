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
about a ~400-token state take **1,535 ms unpacked and 340 ms packed on Metal**
and **16.1 s unpacked and 3.0 s packed on CPU**. Processed tokens fall from
6,587 to 798.

Packing changes what the state tokens can see: they no longer attend to the
question. A packed model therefore needs fine-tuned weights. The native
trainer supports this, starting from a released checkpoint and optionally
distilling from it (see [Training methodology](#training-methodology)).
Accuracy of a packed fine-tune has **not** yet been qualified on a real
dataset. That is the main open gate.

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
`min(4 * max_len, 8192)` and it is capped at 8192, because one dense `[L, L]`
mask per row is materialized. When one row cannot hold every question, the
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

- **CPU and Metal:** use the generic ModernBERT path with dense `[L, L]` masks.
- **Fused Metal kernels:** the resident Laya kernels (`ops/laya_metal.zig`)
  are not used for packed rows.
- **CUDA:** does not select the Laya profile for packed configs, just as it
  does not for `max_len > 512`.

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

Metal: the unpacked baseline uses the fused resident Laya kernels; packed uses
the generic encoder.

| State tokens (unpacked, Q=1) | Q | Unpacked ms | Packed ms | Tokens unpacked → packed |
| ---: | ---: | ---: | ---: | ---: |
| 55 | 1 | 58.5 | 56.5 | 55 → 56 |
| 55 | 16 | 165.4 | 148.4 | 955 → 446 |
| 151 | 4 | 149.3 | 87.4 | 619 → 230 |
| 151 | 16 | 410.8 | 190.7 | 2,491 → 542 |
| 407 | 1 | 195.5 | 139.2 | 407 → 408 |
| 407 | 4 | 453.6 | 165.8 | 1,643 → 486 |
| 407 | 8 | 800.1 | 219.9 | 3,295 → 594 |
| 407 | 16 | 1,534.7 | 340.3 | 6,587 → 798 |

CPU (native BLAS):

| State tokens | Q | Unpacked ms | Packed ms |
| ---: | ---: | ---: | ---: |
| 55 | 1 | 336 | 332 |
| 55 | 16 | 1,644 | 1,274 |
| 151 | 16 | 5,716 | 1,888 |
| 407 | 1 | 1,467 | 1,344 |
| 407 | 4 | 4,020 | 1,566 |
| 407 | 16 | 16,066 | 2,954 |

A single question costs the same either way. The saving grows with both the
number of questions and the state length. The first packed Metal
implementation rotated RoPE on the host: 56 device synchronizations per
forward added 200–800 ms and lost to the fused baseline everywhere. Moving
RoPE onto the device M-RoPE op removed that overhead. The full raw output is
in
[`work-log/completed/inference/laya/2026-09-24-tree-packing.md`](../../../../../work-log/completed/inference/laya/2026-09-24-tree-packing.md).

## Open work

- **Accuracy qualification.** Fine-tune question- and candidate-packed models
  with the recipe above on typed-decisions, AG News, BoolQ, SST-5, and
  Banking77, and compare against the released model (see the acceptance
  gate).
- **Batching rows.** A session run executes one packed row, because the dense
  mask contract is `[L, L]` shared across heads. Batching rows needs a
  per-row mask shape that no backend confuses with the head-shared form.
- **Sparse attention.** Masks are dense `[L, L]`. A block-sparse or
  segment-aware attention kernel would remove the quadratic cost of rows that
  are mostly masked, and lift the 8,192-token physical cap.
- **CUDA and the fused Metal kernels.** Neither implements tree masks or
  explicit positions yet.
- **Long context.** See the note at the end of the training section.
- **Very many options.** Candidate branches cannot compare options with each
  other before the softmax. Jev reportedly adds an explicit second-stage
  choice; that is not implemented here.
