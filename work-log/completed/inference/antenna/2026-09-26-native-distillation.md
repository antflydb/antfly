# Antenna native distillation, 2026-09-26

Follows the pilot (2026-09-25-pilot.md), which found that a raw trunk trained
on task labels collapses and that feature distillation from gliner2.5-base's
encoder through a fitted projection does not. This entry covers the PyTorch
stage that completes that recipe, the decision to keep the projection as a
GLiNER neck, and the native (Zig) implementation.

## Stage 3 in PyTorch: task fine-tuning after distillation

The stage-2 student (research harness, 14,000 distillation steps) with
gliner2.5-base's heads behind the projection, fine-tuned on the 4,560 pilot
rows with hard labels: 2 epochs, batch 4 x accumulation 2, encoder lr 1e-5,
head and projection lr 5e-5, AdamW, warmup 10%, clip 1.0, upstream's training
collate (with its schema augmentation).

| Mean (in-domain / held-out) | Classification | NER F1 |
| --- | --- | --- |
| gliner2.5-base, released | 0.728 / 0.483 | 0.543 / 0.515 |
| gliner2.5-base + pilot recipe (upstream trainer) | 0.808 / 0.467 | 0.723 / 0.624 |
| stage 2 (distillation only) | 0.662 / 0.378 | 0.449 / 0.364 |
| stage 3, with a distillation anchor (weight 1) | 0.739 / 0.345 | 0.675 / 0.471 |
| stage 3, task loss only | 0.742 / 0.345 | 0.675 / 0.477 |

Once the trunk is distilled, plain task fine-tuning works; the anchor changes
nothing. In-domain the student passes released gliner2.5-base; held-out
classification (CLINC150 0.42, SST-5 0.27) and MIT movie NER (0.21) stay
short, which points at the narrow news-and-banking text pool.

## Decision: the projection stays as a GLiNER neck

The projection cannot be folded into the heads: the boundary heads first pad
the text states with learned BOS/EOS states and run attention blocks with
normalization. Asked for the best long-term option, it stays as an explicit
layer owned by the GLiNER head family (`gliner_neck.{weight,bias}`, config
`"antenna_neck": "linear"`): other head families read raw trunk states, a
later trunk distills into the same space and reuses the heads, and it costs
about 0.7% of ModernBERT-base's per-token compute. ANTENNA.md decision 9.

## Native implementation

- **Neck** (97de247f5d): optional in the derived ModernBERT inventory, the
  training source and export, and applied to the encoder output before routing
  in the training graph; it trains in the task learning-rate group. Plan and
  run fingerprints hash the pre-neck config fields exactly as before, so
  existing runs keep their identities. `scripts/antenna/neck.py` loads necked
  checkpoints for the upstream oracle.
- **Distillation objective** (93bacbf45a): `step.buildWithObjectives` adds the
  routed (necked) states as outputs and computes the z-space MSE and its
  gradient on the host. Without heads, no head is built or touched.
- **Jobs** (85cd3888b4, 2dfef6289c, 68be2cc656): a job's `distillation`
  section loads a second GLiNER2.5 source as a frozen teacher, prepares each
  microbatch with the teacher's tokenizer, checks the routes align, and
  encodes on the CPU with the serving DeBERTa kernels. Unlabeled rows are
  allowed. Two bugs found on the way: optional "touch" heads received explicit
  zero gradients (weight decay moved them) under pure distillation, and the
  job initialized its optional teacher by assigning `undefined`, which
  ReleaseFast read as null.
- **Neck fit** (ad38d2d06a): `distillation.fit.rows` runs the frozen student
  (identity neck) and the teacher over the first training rows, accumulates
  the ridge normal equations in f64 and seeds the optimizer's neck with the
  solution.
- **Parity** (56a738ed9c): the 22-layer necked student matches PyTorch on
  routed states (within 2e-5) and on gradients of layer 0 attention, the last
  MLP, the final norm and the neck (within 3.7e-4), for both attention
  profiles.
- `scripts/antenna/distill_pool.py` writes the unlabeled pool.

## Root cause: resident Metal dropped the transposed operand of Q·Kᵀ

Native stage 3 on resident Metal first looked much worse than PyTorch from the
same checkpoint and rows (0.620 / 0.342 in-domain against 0.741 / 0.673 for
upstream's trainer). The investigation, in order:

| Check | Result |
| --- | --- |
| Encoder + neck parity on the real 22-layer student (CPU and Metal interpreter) | exact (states 2e-5, gradients 3.7e-4) |
| Export round trip (zero learning rate) | exact (1.8e-12) |
| Gold injection held at 1.0 natively | unchanged (0.619 / 0.328) |
| Upstream without schema augmentation | 0.751 / 0.680, so augmentation is not it |
| Native without negative-query sampling | worse (0.616 / 0.226) |
| One deterministic microbatch, native CPU vs PyTorch | identical terms (43.2458 vs 43.2465) |
| Five deterministic steps, native CPU vs upstream trainer | identical losses and weights (4.8e-6) |
| The same first step on resident Metal | 79.37 instead of 43.25 |
| Trainer's encoder output vs PyTorch, real student | CPU 1.3e-11, resident Metal 3.69 (z-space MSE) |

Probing every node of a layer on resident Metal against the interpreter found
the first divergence at the attention scores. The resident program validated
dots whose right operand is transposed but always launched the device kernels
with `rhs_contract_axis = 0`, so `matmul3DTransB` (Q·Kᵀ) multiplied by K as if
it were untransposed. It fails for every shape; tiny test models hid it because
their scores are near zero. DeBERTa training and ModernBERT's fused attention
profile never emit this dot, which is why DeBERTa jobs and Laya were correct.
Fixed in 530424b2fe; the trainer's encoder output on resident Metal now matches
PyTorch at 1e-11. Every earlier ModernBERT result trained on resident Metal
with the materialized attention profile (the pilot, native stage 3, native
distillation) was trained on corrupted encoder states and is superseded.

Discarded native results, for the record:

| Mean (in-domain / held-out), native on resident Metal before the fix | Classification | NER F1 |
| --- | --- | --- |
| stage 3, soft Decide-mixed targets | 0.617 / 0.345 | 0.374 / 0.273 |
| stage 3, hard labels | 0.620 / 0.349 | 0.342 / 0.251 |
| pure distillation, 2,250 steps | 0.123 / 0.143 | 0.000 / 0.000 |

### Native stage 3 after the fix

The same job (stage-2 checkpoint with its neck, pilot rows with hard labels,
resident Metal, 2 epochs) rebuilt at 530424b2fe:

| Mean (in-domain / held-out) | Classification | NER F1 |
| --- | --- | --- |
| stage 2 start | 0.662 / 0.378 | 0.449 / 0.364 |
| native stage 3, fixed | 0.743 / 0.345 | 0.684 / 0.511 |
| upstream trainer, same start | 0.741 / 0.353 | 0.673 / 0.482 |

Native training now matches upstream's trainer on real data, which the
earlier CUDA-only parity campaign (scripts/gliner25/LOSS_PARITY_FOLLOWUP.md)
never established for CPU or Metal.

### Native distillation after the fix

The neck fit now runs on the job's backend (300 rows in about a minute on
resident Metal instead of 45 s per microbatch on the CPU; explained variance
0.51-0.52). Both runs start from the identity-neck student with gliner2.5-base's
heads, batch 4 x accumulation 2, encoder lr 3e-5, neck lr 1e-4, warmup 10%,
distillation weight 1, no heads:

| Mean (in-domain / held-out) | Optimizer steps | Classification | NER F1 |
| --- | --- | --- | --- |
| 80k pool (news and banking), run14 | 8,000 | 0.603 / 0.353 | 0.323 / 0.285 |
| Wikipedia pool (118,800 rows, 1 epoch), run15 | 14,850 | 0.624 / 0.359 | 0.435 / 0.379 |
| PyTorch stage 2 (for reference) | 14,000 | 0.662 / 0.378 | 0.449 / 0.364 |

The Wikipedia run scored low at 3,000 steps (0.381 / 0.249 and 0.146 / 0.183)
while its longer warmup ended, then matched run14's final NER by 6,000. Its held-out NER is
above PyTorch stage 2's; classification stays 0.02-0.04 short. Run15 took 10.6
hours on the Studio.

### Native end to end: stage 3 from the native student

Stage 3 from run15's student with the job used for the fixed native stage 3
(pilot rows, hard labels, 2 epochs, resident Metal), run16:

| Mean (in-domain / held-out) | Classification | NER F1 |
| --- | --- | --- |
| native stage 3 from PyTorch stage 2 | 0.743 / 0.345 | 0.684 / 0.511 |
| native stage 3 from native distillation (run16) | 0.740 / 0.316 | 0.696 / 0.497 |

Distillation and fine-tuning both native now reach the same place as the
PyTorch-distilled student within noise, except held-out classification
(CLINC150 0.39 against 0.45; SST-5 0.26 against 0.24). MIT movie NER stays
the weakest set (0.26).

### Where the distilled trunk departs from the teacher

The gap to gliner2.5-base with the same fine-tuning is 0.07-0.15 on
classification and 0.03-0.13 on NER, largest held out. A probe measured
run15's z-space error against the teacher per text source (200 test texts
each, one per-dimension teacher std over the whole probe, word rows and
marker rows apart), with each dataset's own schema and with a pool-style one:

| Source | Words (own / pool schema) | Markers (own / pool schema) |
| --- | --- | --- |
| Wikipedia pool | - / 0.237 | - / 0.517 |
| AG News | 0.219 / 0.228 | 0.667 / 0.489 |
| Banking77 | 0.396 / 0.401 | 0.561 / 0.597 |
| CLINC150 | 0.513 / 0.463 | 0.679 / 0.486 |
| SST-5 | 0.424 / 0.353 | 0.858 / 0.518 |
| CrossNER (5 domains) | 0.33-0.36 / 0.28-0.33 | 0.23-0.32 / 0.20-0.24 |
| MIT restaurant | 0.470 / 0.393 | 0.268 / 0.200 |
| MIT movie | 0.456 / 0.338 | 0.401 / 0.193 |

Short utterances carry about twice the text error of news and Wikipedia
(Banking77 too, though it is in the pool), classification markers far more
than entity markers, and unseen label vocabularies (SST-5's sentiment scale,
CLINC150's intents, MIT movie's types) more than the pool's. The worst
sources are the worst evaluation sets.

`distill_pool.py --source` now builds a mix that widens both halves: NuNER
web sentences with their own free-form types (9,927 types after filtering),
MASSIVE commands, GoEmotions comments, SQuAD questions and DBpedia abstracts
with their intent, emotion and topic names, next to news, Banking77 and
Wikipedia: 274,109 rows, median 19 words, 60% entity schemas. All sources
are MIT, Apache 2.0 or CC BY(-SA); none is an evaluation set. Types with
brackets or parentheses are dropped: the native schema compiler reserves them.

### Distillation on the mixed pool

One epoch of the mixed pool (34,264 optimizer steps, 68,528 microbatches,
run17) from the identity-neck student, with run15's settings otherwise:

| Mean (in-domain / held-out) | Optimizer steps | Classification | NER F1 |
| --- | --- | --- | --- |
| Wikipedia pool, run15 | 14,850 | 0.624 / 0.359 | 0.435 / 0.379 |
| mixed pool, run17 | 12,000 | 0.583 / 0.345 | 0.443 / 0.469 |
| mixed pool, run17 | 24,000 | 0.632 / 0.363 | 0.502 / 0.507 |
| mixed pool, run17 | 34,264 | 0.637 / 0.372 | 0.516 / 0.524 |
| PyTorch stage 2 | 14,000 | 0.662 / 0.378 | 0.449 / 0.364 |

NER passes PyTorch stage 2 on both groups (MIT movie 0.24 to 0.46, CrossNER
science 0.40 to 0.57); classification dipped early (Banking77 0.44 at 12,000
steps) and recovered to just above run15, still short of stage 2 on typed
decisions (0.29 against 0.38). The probe on the final student, own schemas,
run15 against run17:

| Source | Words | Markers |
| --- | --- | --- |
| CLINC150 | 0.513 -> 0.368 | 0.679 -> 0.429 |
| SST-5 | 0.424 -> 0.335 | 0.858 -> 0.700 |
| Banking77 | 0.396 -> 0.334 | 0.561 -> 0.556 |
| AG News | 0.219 -> 0.220 | 0.667 -> 0.593 |
| CrossNER (5 domains) | 0.33-0.36 -> 0.26-0.29 | 0.23-0.32 -> 0.18-0.24 |
| MIT restaurant | 0.470 -> 0.330 | 0.268 -> 0.191 |
| MIT movie | 0.456 -> 0.337 | 0.401 -> 0.206 |

Short text and unseen type names closed most of their gap; classification
markers (0.43-0.70) remain the largest error. Marker errors depend on which
label names a draw samples: a second draw of the same probe put CLINC150's
at 0.59, so compare them loosely. The probe is now
`scripts/antenna/gap_probe.py`.

The run stopped at microbatch 57,505 on the trainer's fixed 64 MiB
`progress.jsonl` cap (about 1.2 KB per report) and resumed from the
microbatch-48,000 checkpoint with identical state; the cap now sizes itself
to the run (#915). Resuming needed host 8.5 GiB and backend 11 GiB to pass
the Studio's live-memory admission. The epoch took about 23 hours.

### Stage 3 from the mixed-pool student

The same stage-3 job as before (pilot rows, hard labels, 2 epochs, resident
Metal), run18:

| Mean (in-domain / held-out) | Classification | NER F1 |
| --- | --- | --- |
| from PyTorch stage 2 | 0.743 / 0.345 | 0.684 / 0.511 |
| from the Wikipedia-pool student (run16) | 0.740 / 0.316 | 0.696 / 0.497 |
| from the mixed-pool student (run18) | 0.734 / 0.368 | 0.719 / 0.629 |
| gliner2.5-base + the same recipe (upstream trainer) | 0.808 / 0.467 | 0.723 / 0.624 |

Trained entirely natively, the ModernBERT student now matches its teacher
fine-tuned the same way on NER, in-domain and held out (CrossNER science
0.69, politics 0.71, MIT movie 0.49). Classification trails by 0.07
in-domain and 0.10 held out: typed decisions 0.25, CLINC150 0.47, SST-5
0.38.

### Second pool: real label sets and typed decisions

`distill_pool.py --label-sets` now draws each classification row's labels
from one real set: the source's own (MASSIVE intents, GoEmotions emotions,
DBpedia and AG News topics, Banking77 intents) or one of 65 hand-written sets
in `scripts/antenna/label_sets.json` (sentiment and rating scales, stance,
urgency, departments, document and question types, and so on; none
reproduces an evaluation label list). `--source openjev` adds Open-Jev's
CC0 typed decisions (release-v2-redistributable train, without
customer-control-v1), each question a task over its options, rendered as
Laya's converter renders them; a third of the yes/no questions and a quarter
of the synthetic game rows are kept, and 9,665 decision rows fit the 128-word
limit. Rows sharing a text stay in one split. The pool has 252,352 training
rows, 43% classification with 795 task names and 1,138 labels; run19 distills
on it with run17's settings.

### A Laya decision head on the Antenna trunk

`scripts/antenna/init_decision_head.py` builds a Laya-format checkpoint from
the run17 student: its `encoder.*` tensors copy across unchanged (the names
already match Laya's), the GLiNER heads and neck are dropped, and a fresh
decision head (`type_emb`, `scorer`, `act_head`, two `head.layers`) is
initialized at width 768 as upstream Laya's modules initialize it, with the
released checkpoint's decision settings. Laya's loader, trainer and
evaluator take it unchanged. `freeze_layers = num_hidden_layers + 1` now
freezes the whole encoder, final norm included (without a new job field, so
existing Laya run identities hold), so only the head trains and the trunk
stays exact for the GLiNER heads; the exported encoder tensors are
bit-identical to the source's.

Accuracy on Laya's step-0 eval split (760 typed decisions), RLCD, head
learning rate 1e-4, batch 1, resident Metal:

| | Overall | Choice | Score | Yes/no |
| --- | --- | --- | --- | --- |
| untrained head | 0.286 | | | |
| released Laya (ModernBERT-large, not fine-tuned) | 0.387 | 0.34 | 0.35 | 0.48 |
| step-0 split, 2,000 decisions, 3 epochs (15 min) | 0.434 | 0.39 | 0.34 | 0.61 |
| plus Open-Jev (64,450 decisions), 1 epoch (4 h) | 0.404 | 0.41 | 0.25 | 0.61 |

A head on the frozen base-size trunk beats released Laya-large, but trails
Laya's full step-0 fine-tune (about 0.62, encoder trained). Open-Jev hurts
score questions here, as it left Laya's own eval unchanged (LAYA.md,
"Scaling packed training on Open-Jev").

## Next

- Evaluate run19 and its stage 3 against run18.
- Decision head: Open-Jev first, then the in-domain split for several
  epochs; a decision head on run19's trunk.
- Classification markers are the remaining gap. The mixed pool has 216 real
  class names and fills the rest of each label list from entity types; add
  many real label sets (intents, topics, sentiment and stance scales,
  decision questions) and rerun the probe and stage 3.
- Typed decisions sit below every other student here; the pool has no
  decision-style questions.
