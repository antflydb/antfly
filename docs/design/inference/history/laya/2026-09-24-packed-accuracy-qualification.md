# Laya packed accuracy qualification (step 0): 2026-09-24 to 2026-09-26

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 718–860 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

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
