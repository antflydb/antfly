# Laya validation, resident Metal, and training measurements

> Historical record: validation recipes and measurements split out of the
> [Laya guide](../../../../guides/models/laya.md) as of 2026-10-12.

## Reference validation

The deterministic fixture exercises the complete encoder, decision heads,
preprocessing, mixed question batches, calibration, HTTP, and embedded extraction.
It uses upstream source with small randomly initialized weights:

```sh
curl -fsSL https://raw.githubusercontent.com/NandhaKishorM/laya/6a5819129eb220570792e417e49723d697efd76f/laya/common.py -o /tmp/laya-common.py
uv run scripts/laya/laya_reference.py --common /tmp/laya-common.py --output /tmp/laya-reference
cd zig
ANTFLY_LAYA_REFERENCE=/tmp/laya-reference python3 tools/run_bounded_zig_build.py \
  build inference-test -Dmetal=false -Dcuda=false -- --test-filter 'laya '
```

Parity tests skip when `ANTFLY_LAYA_REFERENCE` is unset. Set `ANTFLY_LAYA_METAL=1`
and build with `-Dmetal=true` to require Metal, including the managed extraction
route. A missing GPU fails the test rather than silently selecting CPU.

The synthetic fixture tests independent and reordered batches through the
512-question pipeline limit. Released-checkpoint qualification additionally
compares token IDs and probabilities against upstream PyTorch on labeled data,
checks accuracy, and measures warm batches through 128 rows. See
[qualification results and reproduction](qualification.md).

These checks cover native CPU and Metal. CUDA has a separate
[qualification script](../../../../../scripts/laya_cuda_qualify.py) and
[matched PyTorch performance gate](../../../../../scripts/laya_cuda_performance.py), run
by the [L4 CI workflow](../../../../../.github/workflows/zig-inference-l4-spot.yml).

## Resident Metal inference

Set `ANTFLY_LAYA_METAL_RESIDENT=1` in the serving process environment before
loading the model to enable the resident path. It remains opt-in; the existing
inference path is the default. `TERMITE_METAL_DISABLE_LAYA_RESIDENT=1` overrides
the enable flag. Restart the process after changing these settings so loaded
models use a consistent execution path.

Projection weights retain their checkpoint F16/BF16/F32 precision on Metal.
Embedding and normalization constants expand losslessly to F32 once. Encoder
activations, both heads, calibration, and numeric decision decoding stay on the
GPU; each request uploads its inputs and reads back only the final numeric
results. Tokenization, validation, label mapping, and JSON formatting run on CPU.
This applies to inference, including exported finetuned checkpoints; training
still uses the host gradient staging described below.

Size model and request memory budgets for the checkpoint precision and padded
batch geometry. The local full-model API checks used 6 GiB host, 12 GiB backend,
18 GiB combined, and 8 GiB scratch capacity for expanded batches; these are test
settings, not minimum requirements for every workload. Default budgets can
reject FP32 preparation or large requests. Once resident Metal loading is
attempted, admission or execution failures are returned rather than retried on
CPU.

### Measured inference performance

Local qualification on an Apple M4 Pro with 24 GiB memory compared the same
binary with residency disabled and enabled. All 66 paired comparisons passed,
covering 132 paging-free windows and 10,500 timed observations.

| Metric | Released FP16: legacy → resident | Finetuned FP32: legacy → resident |
| --- | ---: | ---: |
| Batch 1, fixed-input p50 | 65.91 → 25.05 ms | 343.65 → 135.48 ms |
| Batch 1, mixed-input p50 | 178.03 → 99.90 ms | 544.69 → 293.79 ms |
| Batch 1, mixed-input p95 | 237.67 → 144.94 ms | 662.29 → 386.03 ms |
| Interactive geometric-mean p50 reduction | 38.1% | 38.2% |
| Batch 16 / 64 / 128 throughput increase | +26.5% / +8.4% / +6.8% | +26.8% / +7.3% / +4.3% |
| Maximum process footprint, including preparation | 5.08 → 5.59 GB | 7.36 → 6.22 GB |

Each profile used three alternating pairs and ten warmups. Interactive batches
1/2/4/8 used 100 observations per window; bulk batches used 25. Fixed inputs had
61 tokens and four options; mixed inputs included sequences through 512 tokens.
Timings include tokenization and decision decoding, but exclude transport and
JSON serialization. Footprints use decimal GB and include more than model weights.
Paging-contaminated attempts were excluded. The final four windows followed a
filesystem-cache reset, with at least 8 GiB free memory and a 30-second paging-free
idle interval before each window; no reset occurred inside a measurement window.

Warm resident requests showed no repeated weight uploads, intermediate activation
readbacks, or CPU fallbacks, and one final numeric readback. These are API transfer
counters, not physical bus-traffic measurements. Released FP16 probability error
versus PyTorch was at most 2.03e-6 against a 5e-5 tolerance. Tests also cover
finetuned FP32 exports, tiny BF16 fixtures, cancellation, allocation failure,
repeated unloads, and embedded/HTTP requests. Performance results apply to the
tested artifacts, hardware, and profiles.

To reproduce the comparison, generate a reference with
`scripts/laya/laya_export_reference.py`, then build `inference-test` with
`-Dmetal=true -Dcuda=false -Doptimize=fast` and the `laya `
test filter. Run each checkpoint precision separately:

```sh
python3 scripts/laya/benchmark_laya_metal.py --binary <test-binary> \
  --reference <reference-directory> --output <new-interactive-directory> \
  --batches 1 2 4 8 --profiles fixed mixed
python3 scripts/laya/benchmark_laya_metal.py --binary <test-binary> \
  --reference <reference-directory> --output <new-throughput-directory> \
  --batches 16 64 128 --profiles fixed
```

The runner retains samples, hashes, logs, and paging counters. The throughput-only
invocation exits nonzero because it lacks interactive coverage; assess its
`measurement_runs_valid` and `profile_regression_gates_passed` fields together
with the interactive invocation's `passed` result.

### Default Metal path on the typed-decisions benchmark

The default path (without `ANTFLY_LAYA_METAL_RESIDENT`) scores the full
typed-decisions test split (2,000 decisions, 578,470 tokens) on an Apple M4
Max as follows. Each run was warm, 64 tasks per pipeline call
(`finetune eval laya --chunk`).

| Model | Before | Now | PyTorch on the same machine |
| --- | ---: | ---: | ---: |
| OpenDecider-nano | 184 s | 54.6 s | 55.9 s (length-sorted batches of 64); 64.4 s (batches of 16) |
| OpenDecider-nano, one decision per call | 149 s | 57.9 s | 70.5 s (batch 1) |
| Laya-large, step-0 fine-tune | 337 s | 85.1 s | |

Probabilities match OpenDecider's PyTorch implementation to 1.3e-6, and the
Laya checkpoint's earlier predictions to 1.7e-6. The ModernBERT encoder on
Metal:
- runs only the real tokens of a batch, with each row its own attention
  segment, instead of padding every row to the longest;
- runs global and sliding-window attention with a tiled kernel that skips
  keys outside a row or its window
  (`TERMITE_METAL_DISABLE_TILED_SEGMENT_ATTENTION=1` restores the scalar
  kernel, `ANTFLY_MODERNBERT_SEGMENT_ATTENTION=0` the dense path);
- runs its linears through MPS in F32, expanding BF16 weights
  (`TERMITE_METAL_DISABLE_MODERNBERT_F32_MPS=1` keeps the BF16 kernels, at
  half the weight memory);
- keeps its LayerNorm weights in fixed slots across requests.

Laya also scores markers on the device, and groups inputs of similar length
into the same call on every backend (`ANTFLY_LAYA_BUCKETING=0` disables it).


### Training parity and lifecycle checks

```sh
python3 scripts/laya/laya_training_reference.py --common /tmp/laya-common.py --fixture /tmp/laya-reference
cd zig
ANTFLY_LAYA_REFERENCE=/tmp/laya-reference python3 tools/run_bounded_zig_build.py \
  --zig /path/to/zig-0.16.0 build inference-test -Dmetal=false -Dcuda=false \
  -- --test-filter 'laya training'
```

Create `/tmp/laya-reference` with the forward reference command above first.
Set `ANTFLY_LAYA_METAL=1` and `-Dmetal=true` for the GPU checks; unavailable
Metal fails explicitly. The fixture checks forward logits, the notebook loss,
every parameter gradient, partial-accumulation resume equivalence, and reopening
the exported artifact through the serving loader.

Training snapshots source weights and tokenizer assets during admission, so
later source-file changes cannot alter the serving export. Optional
`tokenizer_config.json` and `special_tokens_map.json` are preserved when present.
Admission checks the largest padded batch across every split; an oversized
later example fails before a run directory is created. Resume validates both
the optimizer cursor and partial accumulation state. `stop_after_microbatches`
is an absolute position and must be ahead of a resumed checkpoint and within
the configured epochs.

### Measured training quality and scope

Full-checkpoint qualification covers CPU soft CE with system BLAS and Metal
soft CE/RLCD on Apple M4 Pro with 24 GiB memory, using batch one, accumulation
four, seed 42, and sequences capped at 512 tokens. The source checkpoint is
`convaiinnovations/laya` revision `c5d78730f3493e4fe16d61507ef4b78eef7318cf`.
The synthetic `LocalLLaMA/typed-decisions` dataset is pinned to revision
`ea9306458d6e9563628369a3d1e72e362fb381d2`, with 32 training cases / 160 decisions,
32 held-out cases / 160 decisions, and 16 calibration cases / 80 decisions.
Each split covers four workflows and all three question types.

After two epochs, 320 microbatches, and 80 optimizer updates:

| Metric | Released source | CPU soft CE | Metal soft CE | Metal RLCD |
| --- | ---: | ---: | ---: | ---: |
| Serving-calibrated soft CE | 1.302114 | 1.049589 | 1.046249 | 1.091510 |
| Argmax accuracy | 43.75% | 50.625% | 50.625% | 51.25% |

All three training profiles pass their held-out quality gates. Soft CE uses
dropout zero and a matched PyTorch training replay; RLCD uses head dropout 0.1
and is compared against the source baseline, without claiming a full stochastic
training replay. Paired-case accuracy intervals include zero improvement, so
the observed accuracy increases do not establish application-level gains.

All 201 trainable gradient tensors pass on CPU and Metal for source and trained
checkpoints at the unchanged tolerance `5e-5 + 0.002 * max(abs(reference_tensor))`.
The oracle uses FP64 encoder/head arithmetic with upstream FP32 logits/loss;
FP32-only reference discrepancies at near-zero ReLU inputs are not covered by
that parity claim. Full-size interrupted RLCD resume produces byte-identical
optimizer state and serving exports. Exported checkpoints pass CPU/Metal serving
reloads against PyTorch within the 5e-5 probability tolerance.

The measured CPU soft-CE job used a 24 GiB host bound; Metal jobs used 16 GiB.
Training runs observed paging. These bounds exclude some driver allocations and
do not guarantee physical-memory residency or zero-swap operation. Sustained
CPU RLCD, other hardware/recipes, and application traffic require separate
qualification. In particular, inference residency does not make the training
graph fully GPU-resident.
