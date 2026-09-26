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

Two bugs found and fixed while qualifying this on real training runs, both
before the negative Banking77 result was trustworthy:

- `job.zig`'s `exportModel` rewrote `packing` to `{mode, max_packed_len}`
  only, silently dropping `two_stage` even when the job trained for it. A
  served checkpoint would therefore never run stage 2. Caught by inspecting
  the seed-42 export's `config.json` before evaluating it. Fixed by factoring
  the packing JSON into `packingConfigJson` (now includes `two_stage`) and
  covered by a new round-trip test. The already-exported seed-42 checkpoint
  was hand-patched (`config.json`/`rl_agent_config.json`) rather than
  retrained, since the weights were already correct.
- `executePacked` did not add the stage-2 joint branch's row length to the
  reported `prompt_tokens`, so the "extra serving cost" measurement would
  have been wrong. Fixed by threading a `*usize` accumulator into
  `refineTwoStage`.

## Tests

`~/bin/zig build test -- --test-filter "laya"`, CPU and
`ANTFLY_LAYA_BACKEND=metal ANTFLY_LAYA_METAL=1`, with and without
`ANTFLY_LAYA_REFERENCE=<fixtures>/ref`: 64 selected, all pass (48 pass, 16
skip — Metal/CUDA-only tests skipped on CPU-only runs, as before). New tests:
a `models/laya.zig` two-stage config parse/validate test; `laya_packed_test.zig`'s
"laya tree style override builds a joint branch under a candidate-mode config"
(the override reproduces a pure question-mode build byte for byte) and "laya
two-stage choice blends a joint shortlist back into the stage-1 distribution"
(an end-to-end pipeline test against a hand-computed mixture, on the seeded
synthetic fixture model); `finetune/laya/data.zig`'s `addStageTwo` test (gold
preservation, target renormalization, seed determinism, no tokenizer needed);
`job.zig`'s packing-export round-trip test; `evaluate.zig`'s `topKRecall` test.

## Banking77 measurement

Job (both seeds): `model_dir` = released checkpoint, `train_file` =
`b77/train.jsonl` (1,540 records; `addStageTwo` adds one synthetic stage-2
record per record, since every Banking77 case has 77 > `top_k` = 8 options,
so training saw 3,080 examples), `eval_file` (trainer's own before/after
eval) = `b77/eval-tiny.jsonl`, `calibration_file` = `b77/calibration.jsonl`,
`backend: metal`, `epochs: 1`, `batch_size: 1`, `objective: soft_ce`,
`packing: candidate`, `two_stage_top_k: 8`. ~3,080 steps, ~35 min per run on
Metal (in line with the candidate-only baseline's ~30 min for half the
examples).

The authoritative eval is the standalone
`antfly-inference finetune eval laya <model_dir> <records.jsonl>` on the full
`b77/eval.jsonl` (400 messages), which goes through the same `executePacked`
two-stage path as real serving. To isolate stage 2's effect on a fixed
checkpoint, each trained model directory was copied and the copy's
`packing.two_stage` stripped from its config (`patch_stage1_only_config.py`,
not checked in — a 4-line JSON edit), then evaluated too. `--top-k-recall 8`
on the stage-1-only copy gives the stage-1 recall bound.

**Result: negative.** Stage 2 reduces accuracy relative to stage 1 alone on
the same weights, on both seeds (mean 0.8475 → 0.8350), despite ~99% top-8
recall. Full numbers, per-seed breakdown, cost, and hypotheses are in
LAYA.md, "Two-stage choice (roadmap 2b)".

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
- The two-stage design itself is negative on this first recipe (see above);
  it should not be adopted as-is. The cheapest untried follow-up is
  per-shortlist-size calibration (`temperature_by_options`, currently
  stripped by the trainer's export step); hard-negative training shortlists
  are the second thing to try.

## Lock usage note

Early runs chained a training run and its evals under one `with-lock gpu`
call (`eval42-then-train43.sh`), which under the *original* lock script also
held the build lock for the whole chain. The coordinator changed the lock
policy mid-track (gpu no longer implies build; builds are memory-gated) and
asked agents to take one `with-lock gpu` per single run/eval going forward.
Everything after that message here does one gpu-locked command per
invocation. `*.safetensors` were deleted from both run directories
immediately after their evals (kept: job/report/prediction JSON, and the
exported `config.json`/`rl_agent_config.json`, tokenizer, and manifest under
each `model/` and `model-stage1-only/`).
