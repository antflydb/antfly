# Laya priorities, decision mix, and community benchmark (2026-10-04 to 2026-10-09)

> Relocated verbatim from `zig/pkg/inference/models/laya/LAYA.md` (lines 2400–2512 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [LAYA.md](../../../../../zig/pkg/inference/models/laya/LAYA.md) before the move.

### Priorities after the decision-model survey (2026-10-04)

From [Other decision models](2026-09-26-decision-model-research.md#other-decision-models-research-2026-10-04),
in order:

1. **Serve OpenDecider-nano.** Done: `laya.format: "opendecider"` with
   `scripts/laya/prepare_opendecider.py`. It matches its PyTorch
   implementation on the typed-decisions test split (see below).
2. **Report on the community benchmark.** Done for every checkpoint so far
   (below). Score new models on the full typed-decisions test split (2,000
   decisions, `scripts/laya/typed_decisions_bench.py`), the split OpenDecider,
   Laya and Jev report, alongside the 760-decision step-0 eval.
3. **Teacher-distilled data at scale.** In progress: a 180,000-decision
   permissive mix with debiased teacher-labelled synthetic cases lifts
   Antenna-base from 0.697 to 0.750 on the typed-decisions test split (see
   [Decision mix](#decision-mix-2026-10-09)). A third seed and a 25,000-case
   synthetic set are running. Packed training at scale is not yet tested.
4. **Question-aware trunk as a supported mode,** if scale does not close the
   gap.
5. **Confidence-gated escalation,** at the product level.

### Decision mix (2026-10-09)

Antenna-base trained first on a large decision mix, then fine-tuned on the
typed-decisions train split, scored on the full test split (2,000 decisions)
against gold labels.

**Data.** Everything is permissively licensed; no CC BY-SA source is used.

- **Human-labelled mix** (`scripts/laya/build_decision_mix.py`, 159,618
  decisions): MASSIVE, HuffPost, Banking77 (train), GoEmotions, Civil
  Comments, SMS Spam (UCI), WANLI and PAWS, recast as choice, score and
  yes/no decisions. Sources are pinned and SHA-checked; gold targets are
  label-smoothed by 0.1.
- **Synthetic cases** (`scripts/laya/generate_decision_cases.py`):
  Qwen3-30B-A3B (MLX, 4-bit) writes agent-workflow cases over 30 workflows
  that do not appear in the benchmark, each a 40–140-word JSON state and a
  question. Qwen3-32B (4-bit) labels them with the calibrated scorer of step
  2a (`synthetic_to_records.py`, then `benchmark_laya_teacher.py
  --score-all`).
- **Debiasing** (`scripts/laya/debias_records.py`): shuffles the options of
  every choice question and subsamples yes/no questions to 50/50. 20,536
  synthetic decisions remain.
- **Exact budget.** `build_decision_mix.py fits` counts tokens the way the
  Laya pipeline does, including yes/no default descriptions and the 48-token
  option cap. Any one record over budget fails the job with
  `ExtractionTextLimitExceeded`, and the first filter undercounted.

**Recipe.** Stage a: one epoch on the mix from `antenna-decision-init-r21`,
`rlcd` objective, encoder LR 2.5e-5, head LR 1e-4, accumulation 5. Stage b:
three epochs on the 4,575 train decisions that fit 512 tokens, as in the
step-0 recipe.

| Recipe | Seed 42 | Seed 43 | Seed 44 | Mean |
| --- | ---: | ---: | ---: | ---: |
| E0b: train split only | 0.700 | 0.709 | 0.682 | 0.697 |
| E1b: human mix, then train split | 0.761 | 0.682 | 0.707 | 0.717 |
| E3: human mix + debiased synthetic, then train split | 0.735 | 0.765 | running | 0.750 |

- **Synthetic data helps once debiased.** E3 leads E0b by 0.05 and E1b by
  0.03 on two seeds. Seed spread is 0.02–0.04, so compare means, not runs.
- **Stage a alone does not transfer.** Before stage b, E1a scores 0.309,
  0.302 and 0.326; E3a 0.454 and 0.471. The mix teaches the trunk, and the
  benchmark's own split still sets the answer format.
- **Biased synthetic data hurts.** E2, trained on the synthetic cases before
  debiasing, fell to 0.504. The generator puts the right option first and
  writes yes/no statements that hold: E2 never chose an option past the
  third and answered "true" 81% of the time. After debiasing, E3 uses every
  position and answers "true" 52% of the time.
- **The teacher is weaker than the student.** Qwen3-32B scores 0.651 on the
  760-decision step-0 eval and agrees with the human labels on 0.705 of a
  sample of the mix. The student beats it after stage b, so the teacher's
  soft targets add coverage of agent-style states rather than accuracy.
- **Baselines.** OpenDecider-nano 0.796; Laya's typed-decisions checkpoint
  0.766; GLiNER2.5-Decide-1B (ModernBERT, zero-shot through its own
  package) 0.470.

Next: the third E3 seed, and E4, which adds 17,000 more synthetic cases
(about 25,000 after debiasing in total).

### Community benchmark (2026-10-04)

Every model on the full typed-decisions test split (2,000 decisions), scored
against gold labels with `scripts/laya/typed_decisions_bench.py --gold`.
States longer than a checkpoint's budget are cut (`--truncate-state`), as
upstream does. Laya's 512 tokens cut 160 decisions; OpenDecider's 2,048 cut
none. Step-0 fine-tunes use the documented recipe of [Base-size encoder](../../../../../zig/pkg/inference/models/laya/LAYA.md#base-size-encoder-step-2d-2026-10-03)
unless noted.

| Model | Accuracy | Choice | Score | Yes/no | ECE |
| --- | ---: | ---: | ---: | ---: | ---: |
| OpenDecider-nano, served natively (Metal) | **0.796** | 0.762 | 0.769 | 0.867 | 0.164 |
| Laya typed-decisions checkpoint (published, trained on the full train split) | 0.766 | 0.733 | 0.723 | 0.857 | |
| Laya-large, step-0 fine-tune (seed 42) | 0.627 | 0.633 | 0.560 | 0.712 | 0.088 |
| Antenna-base, step-0 fine-tune (3 seeds) | 0.533 (0.579 / 0.495 / 0.525) | | | | |
| Antenna-base, frozen trunk + head (dec7) | 0.535 | 0.538 | 0.439 | 0.662 | 0.084 |
| ModernBERT-base, step-0 fine-tune (3 seeds) | 0.472 (0.489 / 0.483 / 0.444) | | | | |
| Released Laya-large, zero-shot | 0.361 | 0.288 | 0.323 | 0.487 | 0.175 |

- **Parity.** Antfly's OpenDecider-nano matches the model's own PyTorch
  implementation to 2.8e-6 in probability on every decision and reproduces its
  published 0.796, 0.762, 0.769 and 0.867.
- **Scoring.** The gold label and the argmax of the gold distribution
  disagree on 31 decisions, so accuracy against gold labels (the Antz AI
  harness, and every published number) differs slightly from `finetune eval
  laya`'s, which scores the argmax.
- **The gap is training data.** OpenDecider-nano and our step-0 fine-tunes
  share the unpacked layout and scorer and have bases of similar size. It
  leads Laya-large's step-0 fine-tune by 0.17 and Antenna-base by 0.26.
  Laya's own checkpoint, trained on the full train split, sits in between.
  That ranks priority 3, teacher-distilled data at scale, first among the
  training work.
