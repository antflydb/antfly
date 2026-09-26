# Laya two-stage choice (roadmap 2b): 2026-09-25

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md)
("Two-stage choice (roadmap 2b)").

Host: Apple M4 Max, 36 GiB, macOS 15 (Darwin 24.6.0), Zig 0.16.0, ReleaseFast,
branch `laya/two-stage-choice` off `9d9627ec3e`.

## What changed

- `zig/pkg/inference/src/models/laya.zig`: `Packing.two_stage` (`TwoStage{top_k,
  mass_cutoff}`), parsed only under `packing.mode: "candidate"`.
- `zig/pkg/inference/src/pipelines/laya_tree.zig`: `build`/`emit` take an
  explicit `BranchStyle` override (`question` or `candidate`) instead of
  reading `cfg.packing.mode` unconditionally, so a candidate-packed config can
  still build a joint (`question`-style) branch on demand.
- `zig/pkg/inference/src/pipelines/laya.zig`: `executePacked` runs stage 2 for
  any `choice` question with more options than `packing.two_stage.top_k`:
  `selectFinalists` picks the shortlist from the stage-1 distribution,
  `refineTwoStage` builds and runs a one-question joint row off the same
  cached trunk, and blends its distribution back into the stage-1
  probabilities (`decode`/`finalize` refactor).
- `zig/pkg/inference/src/finetune/laya/data.zig`: `addStageTwo` synthesizes,
  for every `choice` record with more labels than `top_k`, one extra record
  holding the gold label plus `top_k - 1` random negatives (seeded), packed
  with the joint style via `pack`'s new per-record routing
  (`Record.is_stage_two`).
- `zig/pkg/inference/src/finetune/laya/job.zig`: `two_stage_top_k` /
  `two_stage_mass_cutoff` job fields, threaded into the packing override and
  the served config; `data.load` takes a `seed` for deterministic negative
  sampling.

Existing single-stage behavior is unchanged when `two_stage` is unset
(default): every new parameter defaults to disabled, and `tree.build`'s style
override defaults to `null` (keeps the config's own mode) at every pre-existing
call site.

## Tests

`~/bin/zig build test -- --test-filter "laya"`, CPU and
`ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_METAL=1`, with and without
`ANTFLY_LAYA_REFERENCE=<fixtures>/ref`: 61 selected, all pass (45/61 without
Metal+CUDA-only tests skipped, as before; adds two new tests: a `models/laya.zig`
two-stage config parse/validate test, and
`pipelines/laya_packed_test.zig`'s "laya tree style override builds a joint
branch under a candidate-mode config", which checks the override reproduces a
pure question-mode build byte for byte). A new `finetune/laya/data.zig` test
checks `addStageTwo`'s gold-label preservation, target renormalization, and
seed determinism directly (no tokenizer needed).

## Banking77 measurement

Job (both seeds): `model_dir` = released checkpoint, `train_file` =
`b77/train.jsonl`, `eval_file` (trainer's own before/after eval) =
`b77/eval-tiny.jsonl`, `calibration_file` = `b77/calibration.jsonl`,
`backend: metal`, `epochs: 1`, `batch_size: 1`, `objective: soft_ce`,
`packing: candidate`, `two_stage_top_k: 8`. The authoritative eval is the
standalone `antfly-inference finetune eval laya <model_dir> <records.jsonl>`
on the full `b77/eval.jsonl` (400 messages), which goes through the same
`executePacked` two-stage path as real serving.

See LAYA.md, "Two-stage choice (roadmap 2b)" for the numbers and discussion.

## Known limitations / open issues

- Calibration: the trainer's `calibrate()` fits one temperature per question
  *kind*, not per option count, so the exported model's single "choice"
  temperature is fit over a mix of 77-way stage-1 rows and 8-way stage-2 rows
  in the calibration split. `models/laya.zig`'s `temperature_by_options`
  bucketing (by count) already exists at decode time but the trainer's export
  step currently strips it. A follow-up could fit it per bucket instead of a
  single flat temperature when `two_stage` is enabled.
- Negative sampling for stage-2 training rows is uniform random, not hard
  negatives from a stage-1 checkpoint's own mistakes (the roadmap note allows
  either). Random negatives were simpler to make deterministic and needed no
  extra training round-trip; hard negatives are likely to teach a sharper
  stage-2 comparison and are worth trying next.
- Doubling the training set (one synthetic stage-2 row per stage-1 row, since
  every Banking77 record has 77 > `top_k` options) roughly doubles wall time
  per epoch relative to the single-stage candidate baseline.
