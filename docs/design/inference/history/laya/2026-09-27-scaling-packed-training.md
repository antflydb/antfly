# Laya packed training at scale and layout ablations (2026-09-27 to 2026-10-03)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 1020–1281 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

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
    Banking77 ([Recommended recipe](../../../../../zig/pkg/inference/models/laya/LAYA.md#recommended-recipe)). About a third of Open-Jev
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
  [Jeeves](2026-09-26-decision-model-research.md#lessons-from-jeeves-research-2026-09-29)) replaces the scorer
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

**Rare-token anchors (2026-10-03): already in place.** Jeeves found that
mapping its layout markers to unused tokens beat plain text such as "State"
([Lessons from Jeeves](2026-09-26-decision-model-research.md#lessons-from-jeeves-research-2026-09-29)). Laya's
encoder layout already uses special tokens where it matters:
- every option marker is `[MASK]`;
- every decision anchor is its branch's `[CLS]`;
- `[SEP]` closes the trunk, the question and the options.

The question type is also given twice, as a type embedding and as the
plain-text `"<type> question:"` prefix. Swapping that prefix for an unused
token is the only part left untested. It is not worth a run: the type
embedding already carries it, and an unused token's embedding is untrained.

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
