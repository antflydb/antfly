# Laya finetuning production qualification

Status: qualified for the measured profiles: CPU soft CE with system BLAS,
and Metal soft CE/RLCD, on Apple M4 Pro with 24 GiB memory. Runs use batch one,
accumulation four, seed 42, and the pinned selection capped at 512 tokens.
The initial implementation and smoke history are retained in
[the validation report](laya-finetuning-validation.md).
The subsequent hardening review and revised-binary verification are recorded
in [the production review](laya-production-review.md). Sustained timings below
belong to the original sealed implementation snapshot.

## Required gates

1. Numerical correctness: released-checkpoint forward, RLCD/soft-CE cotangents,
   all trainable gradients, and AdamW updates against independent PyTorch.
   Investigate the existing four CPU / three Metal gradient outliers without
   silently relaxing the gate. Test the released source and a trained artifact.
2. Representative quality: pinned typed-decisions data, case-disjoint train,
   calibration, and held-out evaluation sets covering choice, ordinal, and
   boolean decisions. Report baseline and trained soft CE, argmax accuracy,
   ordinal error, and per-type results; compare with a matched PyTorch recipe.
   Require held-out CE improvement and no material accuracy/type regression.
3. Lifecycle: full-size interrupted accumulation and deterministic resume,
   checkpoint integrity and mismatched-input rejection, frozen action-head
   preservation, atomic publication, and serving parity after reload.
4. Resources: record hardware, process/GPU memory where available, swap deltas,
   admitted sequence/batch geometry, measured step time, and sustained-run
   behavior. A memory failure or incomplete process is a failed gate.
5. Reproducibility: retain commands, source/data hashes, exact environment,
   machine-readable pass/fail results and artifact hashes. Distinguish diagnostic
   files in `/tmp` from retained qualification evidence.

Scope: full FP32 finetuning on CPU and Metal. CUDA/DDP, LoRA, mixed precision,
encoder dropout, and action-head training are outside this implementation.

## Measured results

Evidence is retained under `.benchmark-results/laya-finetuning-20260921/`.
`qualification.json` is the aggregate status; individual failed and superseded
attempts remain available. CPU and Metal soft-CE quality, Metal RLCD quality, final export parity, and
portable CPU build verification all pass. CPU RLCD has full-size numerical
and miniature lifecycle coverage; a sustained CPU RLCD quality campaign was
not run.

The source is `convaiinnovations/laya` revision
`c5d78730f3493e4fe16d61507ef4b78eef7318cf`, weights SHA-256
`891102d372688fc2a094dac56a384bc537b87c63f21f9f3dac0be2b7cbc8d86c`.
The upstream objective/model reference is pinned to `common.py` commit
`6a5819129eb220570792e417e49723d697efd76f`.

### Numerical and lifecycle checks

All 201 trainable gradient tensors pass on CPU and Metal for both the released
source and the sustained soft-CE export. The unchanged per-tensor tolerance is
`5e-5 + 0.002 * max(abs(reference_tensor))`. The reference uses FP64 encoder/head
arithmetic with the upstream FP32 decision logits/loss. The accelerated CPU path
was rechecked against both full-size fixtures.

The original FP32 reference has four CPU and three Metal gradient outliers.
PyTorch FP32 versus PyTorch FP64 reproduces all four outliers: two first-head
ReLU inputs change from approximately `7.09e-7` and `3.14e-6` to negative values.
The reference-versus-reference relative L2 difference reaches `0.003774`.
Those FP32 compatibility failures are retained; the tolerance was not relaxed.
Laya also now uses PyTorch's zero subgradient at exact-zero ReLU inputs.

Full-size RLCD resume passes with head dropout 0.1, accumulation three, and an
interruption after the first of four microbatches. Both complete optimizer state
and serving export are byte-identical to uninterrupted training:

- Checkpoint SHA-256: `599234199a44b39917d0809f55a1e71d3736c6a26ef50ea5d188561260e5d16c`.
- Export SHA-256: `9ef15aa4caac255240ff71b323574833b811a0874727ed54f9ccdd24ce29ffaa`.
- All four frozen action-head tensors exactly preserve the FP32 source values.
- A changed-seed resume fails with `TrainingStateFingerprintMismatch` before
  creating its output directory.

Startup restore validates an immutable host snapshot before creating device
state, avoiding a redundant live optimizer allocation. Live transactional
restore retains its existing atomic behavior. Regression coverage includes
allocation failure, cancellation, immutable identities, AdamW groups and
clipping, and partial-window normalization. The CPU suite passes 28 tests with
six optional backend/artifact skips; the enabled Metal suite passes 11 without
skips. A separate build with system BLAS and Metal disabled also passes all
28 CPU checks with six expected optional skips. Thirteen Python conversion
and quality-gate tests pass.

### Held-out soft-CE quality

Data is the published synthetic `LocalLLaMA/typed-decisions` dataset, revision
`ea9306458d6e9563628369a3d1e72e362fb381d2`. The pinned selection has 32 training
cases / 160 decisions, 32 held-out cases / 160 decisions, and 16 calibration
cases / 80 decisions. Each split is balanced across four workflows. Held-out
coverage is 48 choice, 64 score, and 48 boolean decisions. Whole overlength
cases are excluded; text is never truncated. Maximum sequence lengths are
458 training, 474 held-out, and 383 calibration tokens.

The Metal run used two epochs, batch one, accumulation four, dropout zero,
encoder/head learning rates `2.5e-5` / `1e-4`, and seed 42. It completed
320 microbatches and 80 optimizer updates. The independent PyTorch CPU replay
uses the same record order, optimizer recipe, and calibration split.

| Metric | Released source | Native CPU | Native Metal | PyTorch CPU replay |
| --- | ---: | ---: | ---: | ---: |
| Uncalibrated soft CE | 1.484883 | 1.078225 | 1.076120 | 1.135737 |
| Serving-calibrated soft CE | 1.302114 | 1.049589 | 1.046249 | 1.076584 |
| Argmax accuracy | 43.750% | 50.625% | 50.625% | 48.125% |

Native calibrated CE improves 19.65% over the released serving calibration.
All three types improve calibrated CE. The native/reference final CE difference
is `0.030335`, and accuracy differs by 2.5 percentage points; both pass the
predeclared 0.05 thresholds. `quality-gate-final.json` and the CPU
`cpu-quality-gate.json` both pass. CPU calibrated CE improves 19.39%; its
paired-case CE interval is `[-0.314792, -0.188123]`. CPU choice accuracy drops
2.083 percentage points, within the declared per-type regression bound.

A paired bootstrap over all 32 whole cases gives calibrated CE difference
`-0.255864`, with 95% interval `[-0.319684, -0.190049]`. The accuracy difference
is +6.875 percentage points, with interval `[-5.625, 18.75]`; this sample does
not establish a statistically secure accuracy increase. These results cover
this fixed offline profile; application-traffic generalization remains unmeasured.

The trained model SHA-256 is
`52f22a304ada31c10912986081747387d8b45255fe88c40aaf4442f122e5d202`.
Independent PyTorch evaluation reproduces all 160 final predictions within
`1.133e-6` probability error. The actual CPU and Metal serving loaders reproduce
20 held-out decisions in batches one and four, including action probabilities,
with maximum decision-probability error `4.769e-7` against the `5e-5` gate.

### Sustained RLCD quality

The default RLCD objective also passes the predeclared quality gates after two
epochs / 320 microbatches / 80 updates, with head dropout 0.1 and sigma
0.4 then 0.1. Uncalibrated CE improves from `1.484882` to `1.256385`;
serving-calibrated CE improves from `1.302114` to `1.091510` (16.17%).
Accuracy rises from 43.75% to 51.25%. All three types improve calibrated CE,
and none regresses in accuracy. The paired-case calibrated CE interval is
`[-0.274657, -0.147222]`. The accuracy interval still includes zero improvement.

`rlcd-quality-gate.json` uses explicit `source-baseline` mode. It does not claim
an independent full stochastic training replay. RLCD loss/cotangents and full
parameter gradients are independently checked with shared noise; the complete
matched optimizer/training replay is the soft-CE control above.

All four action-head tensors remain exactly equal to the FP32 source in both
CPU and RLCD exports. CPU and Metal soft-CE jobs have exactly the same 320
microbatch record lists and effective configuration, except backend, output
path, and host budget. `final-artifact-validation.json` records those checks,
per-type calibrated comparisons, and model/config hashes. The RLCD model SHA-256
is `6e01eab249a54c077874e382dc7308f25686f2328cb052612c71d55a17dcfcfa`;
the CPU soft-CE model SHA-256 is
`e779438b3062ae684e5f864c131eb7e999bfe1a048a54e889c1d6e643d8545ea`.

Independent PyTorch evaluation reproduces all 160 final CPU and RLCD
predictions with maximum probability errors `1.833e-6` and `1.699e-6`. Both
exports also pass the actual CPU and Metal serving loaders on 20 decisions in
batches one and four, including exact tokenization and action probabilities.
The largest decision-probability error across those serving checks is
`1.133e-6`, below the unchanged `5e-5` gate.

### Resources and supported profile

Hardware: Apple M4 Pro, 24 GiB unified memory, macOS 26.0. The sustained native
Metal run took 1,644.52 seconds, with tracked host peak 10,676,002,141 bytes and
sampled process RSS peak 11,553,964,032 bytes. Its configured host bound was
16 GiB. The CPU campaign took 3,737.15 seconds, with tracked host peak
21,191,421,542 bytes and sampled RSS peak 18,200,166,400 bytes. CPU uses a
24 GiB host bound; it does not qualify the 16 GiB Metal host profile. System
swapouts increased by 145,692 pages (about 2.22 GiB) during CPU execution.
Allocator limits do not include
every driver allocation and are not an admission guarantee for physical RAM.

During the Metal run, the system recorded 632,196 swapouts at 16 KiB per page, about
9.65 GiB of cumulative paging. Final swap usage decreased slightly, which does
not erase this paging. This is a completed paging-tolerant run, not zero-swap
qualification. RSS is not dedicated GPU memory. Optional GPU driver counters
are system-wide; the first Metal run's GPU samples cover only its tail.

During RLCD, the global GPU driver counters peaked at 18,220,711,936 allocated
bytes and 16,361,078,784 in-use bytes; these are not process-attributed or
additive with RSS on unified memory.

The sustained Metal RLCD run took 1,701.47 seconds. Tracked host peak was
10,841,642,313 bytes, sampled RSS peak was 11,215,863,808 bytes, and system
swapouts increased by 582,632 pages (about 8.89 GiB). It also completed within
the configured 16 GiB host bound. These profiles require tolerance for paging
on this 24 GiB machine. Larger batches, longer sequences, other hardware, and
application-specific quality need their own admission and qualification.

The initial scalar CPU profile spent over 99% of sampled time in dot products.
Laya now uses system BLAS for eligible CPU matrix products with FP64
accumulation and retains the portable fallback. This override is local to Laya;
other model backends retain their existing arithmetic. Evaluation scratch is
reclaimed between examples. Timings include loading, evaluation, training,
calibration, checkpointing, and export; the CPU run also includes one second
of sampling-profiler observation. These are measured end-to-end runs, not
isolated throughput comparisons with PyTorch.

## Reproduction and evidence

The retained root contains final binaries `bin/train-laya-cpu-blas` and
`bin/inference-test-cpu-blas`, the prepared
`source/` model, pinned upstream `common.py`, original Parquet files under
`dataset-source/`, case-disjoint `data/`, and `gates.json`. The data manifest
records source hashes, selection seed, excluded cases, and all split hashes.
`implementation-snapshot.tar.gz` and `implementation-manifest.json` retain the
dirty source used for the qualification; no commit is required to reproduce it.

Build with stable Zig 0.16.0, `-Doptimize=ReleaseFast -Dmetal=true -Dcuda=false
-Donnx=false -j1`. Run the standalone `train-laya` build target from
`zig/pkg/inference`; the root `zig` build exposes `inference-test`. The training
target and public
`antfly inference finetune train laya` route execute the same job implementation.

Copy a retained job to `/absolute/new-job.json` and change its `output_dir`
to `/absolute/new-run`. Every run and reference output directory must be new:

```sh
python3 scripts/run_laya_qualification.py \
  --binary "$RUN_ROOT/bin/train-laya-cpu-blas" \
  --job /absolute/new-job.json \
  --evidence /absolute/new-evidence

uv run scripts/laya_quality_reference.py \
  --model "$RUN_ROOT/source" --common "$RUN_ROOT/common.py" \
  --data "$RUN_ROOT/data" --native-run /absolute/new-run \
  --device cpu --output /absolute/new-torch-reference

python3 scripts/qualify_laya_finetune_quality.py \
  --native /absolute/new-run/report.json \
  --reference /absolute/new-torch-reference/report.json \
  --gates "$RUN_ROOT/gates.json" --output /absolute/new-quality-gate.json
```

Set `RUN_ROOT` to an absolute evidence path. The resource runner currently
targets macOS. It records the binary/job hashes, CPU, physical memory, sampled
process RSS, system paging counters, elapsed time, and the actual child exit
status. A paused lifecycle run is recorded as paused, never as a completed
quality campaign. System paging counters include other processes; process RSS
is not a dedicated GPU-memory measurement.

The independent replay uses the native log's record IDs to reproduce order,
batching, epoch boundaries, partial accumulation, AdamW groups, clipping, and
cosine schedule. It requires a fresh completed soft-CE run with dropout zero.
It rejects mismatched source weights/configuration/tokenizer, any data split,
incomplete step logs, stale prepared token sequences, or a different objective. CPU FP64 gradient references independently cover the RLCD objective.
The quality scorer reports paired confidence intervals by resampling whole
cases, not individual questions. It also reconstructs the native run identity
and verifies reported metrics against saved predictions. When re-scoring older
reports without source asset digests, supply `--legacy-manifest` pointing to
their sealed `evidence-manifest.json`. A comparison against a replay bound to
another native backend run additionally requires `--replay-run` pointing to
that original run; recipe and recorded microbatch order must still match.

The first PyTorch MPS replay exhausted its allocator after 129 microbatches
(`MPS allocated: 10.01 GiB`, `other allocations: 20.01 GiB`). Its memory limit
was not disabled. The attempt is retained as failed; the independent replay
uses CPU. This failure occurred in the reference process after the native Metal
campaign had completed, not in native training.

For serving parity, `laya_export_reference.py --records <native-eval.jsonl>`
generates single-example PyTorch predictions. Set
`ANTFLY_LAYA_EXPORT_REFERENCE` to its output and run the test filter
`laya finetuned export`; it checks exact token IDs/marker positions and both
decision and action probabilities in batches of one and four. Set
`ANTFLY_LAYA_METAL=1` to require Metal. The retained serving subset contains
20 held-out decisions, covering all four workflows and all three question types.

Large redundant lifecycle checkpoints are losslessly gzip-compressed after
verifying their decompressed SHA-256. `lifecycle/checkpoint-archives.json`
records original and archive paths and sizes; decompress before replaying a
saved resume job. Both optimizer-state and serving-weight equality were checked
before archiving. These lifecycle runs are mechanical checks, not quality runs.
