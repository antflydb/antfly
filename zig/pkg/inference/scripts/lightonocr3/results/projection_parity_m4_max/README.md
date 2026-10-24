# OCR projection and encoder follow-up

Apple M4 Max, 36 GiB unified memory, macOS 27.0.1; shared machine.
**Whole-reader parity remains unfinished for both LightOn models.** Florence
reached approximately MLX throughput in the final repeat. These are successive
exploratory runs under uncontrolled load, not a stable performance guarantee.

## Final runtime measurements

All rows complete 16 mixed fixture pages, greedy generation, 256-token ceiling.
Native uses concurrency/rows/window 8/8/8. Model loading and serial reference
reads are untimed; native timing includes image preparation, queueing and decode.

| Model / runtime | Pages/s | Evidence |
| --- | ---: | --- |
| LightOn 0.8B, native, opt-in weight residency | 1.3102 | [report](native_08_resident/report.json) |
| LightOn 0.8B, vLLM Metal, concurrency 8 | 2.7202 | [report](vllm_08/report.json) |
| LightOn 4B, native, opt-in weight residency | 0.4375 | [report](native_4b_resident/report.json) |
| LightOn 4B, vLLM Metal, concurrency 8 | 0.7570 | [report](vllm_4b/report.json) |
| Florence-2-base Q4_K, native final repeat | 4.6319 | [report](native_florence_f16_repeat/report.json) |
| Florence-2-base FP16, MLX-VLM, serial | 4.6805 | [report](mlx_florence/report.json) |

These runs reach approximately 48%, 58%, and 99% of their references.
Florence's preceding rollback repeat measured 4.0722 pages/s; the earlier
optimized/rollback pair measured 4.0141/1.3942. The repeat supports a smaller
improvement than that first pair suggested. Do not present the first pair's
2.9x ratio as a stable causal gain.

LightOn unwired controls on the preceding runtime measured 0.4303/0.4093
pages/s (`native_08_control`, `native_4b_control`). Residency subsequently
measured 1.3102/0.4375. Logs confirm peaks of 1,503,789,056 and 8,409,579,520
resident bytes, with slot allocations removed before runtime shutdown.
These were successive runs, and only the opt-in residency implementation
changed between those runtime builds. Residency stays opt-in pending broader
memory-pressure and workload qualification.

Earlier small-projection candidate/rollback runs measured 0.9115/0.9623 for
0.8B and 0.3129/0.3508 for 4B. Thus the new projection kernels alone did
**not** establish a full-page gain. All successful native runs are retained,
including these negative results and instrumented profiles.

## Changes and qualification

- Eight-way split-K down projections use TensorOps with BF16 weights and F32
  inputs, accumulation and output. The 0.8B path serves 5–8 rows; 4B serves
  4–8. Partials borrow existing frame-owned GQA split scratch. Missing scratch,
  unsupported shapes, active planned encoders and unavailable pipelines fall
  back. No extra persistent GPU scratch is allocated.
- Four-row 0.8B gate/up uses four-channel SIMD segments; four-row 4B gate/up
  and QKV use a narrower 8-by-16 TensorOps tile.
- Prefill recurrent convolution handles four channels per thread, with the
  same arithmetic order. Scalar/vector comparisons are bit-exact for cold and
  continued history, both models' head counts and sequences 2–554.
- Large F16 encoder mirrors use a 32-by-64 TensorOps tile with fused bias.
  Qualified vision and BART geometries retain F32 inputs/accumulation/output.
  Shapes outside the allowlist and unavailable Metal 4 pipelines retain MPS.
  This dtype/shape-qualified route also covers other models with those same
  geometries. It is currently restricted to Apple M4 Max.
- Optional dense OCR residency registers existing admitted weight buffers
  without copies, deduplicates tied resources, removes cleared/replaced slots,
  and releases residency at shutdown. A 12 GiB cap and at most 32 sets bound
  wiring; failures retain ordinary unwired execution. Existing admission limits
  remain active. This is separate from the existing mapped MoE residency path.

Rollback switches added in this round:

```
TERMITE_METAL_DISABLE_LIGHTONOCR3_DOWN_SPLIT=1
TERMITE_METAL_DISABLE_LIGHTONOCR3_FOUR_ROW_TILE=1
TERMITE_METAL_DISABLE_QWEN35_PREFILL_CONV_VEC4=1
TERMITE_METAL_DISABLE_F16_ENCODER_MPP=1
```

Residency is off by default. Select it with
`TERMITE_METAL_ENABLE_LIGHTONOCR3_WEIGHT_RESIDENCY=1` before model preparation,
or `benchmark_concurrency.py --weight-residency`.

## Kernel measurements

[matrix_comparison.json](matrix_comparison.json) compares 30 representative
matrix cases (six shapes at 1/4/8/128/512 rows). **29/30 are within 20% of MLX
or faster** in this sweep. The remaining 4B four-row gate/up is about 31%
slower. This does not cover every operator or the vocabulary head.

The native checker and MLX use identical nonuniform LCG-generated BF16 weight
bits. Native uses F32 activations/output; MLX uses BF16. Command-buffer GPU
times are measured rather than host dispatch latency. MLX microbenchmarks use
its default unwired policy; normal vLLM page references retain vLLM's wiring.
The native sweep predates the F16/residency additions; its binary provenance is
`native_08/report.json`. See [native_matrices.txt](native_matrices.txt) and
[mlx_matrices.json](mlx_matrices.json). These are successive shared-machine
runs, not a controlled precision-matched parity proof.

Representative mean GPU times, microseconds:

| Shape, rows | Native | MLX |
| --- | ---: | ---: |
| 1024→3584, 4 | 22.46 | 23.64 |
| 3584→1024, 8 | 19.54 | 72.35 |
| 2560→8192, 4 | 244.79 | 238.36 |
| 2560→9216, 4 | 185.41 | 141.07 |
| 9216→2560, 8 | 150.04 | 144.28 |

F16 encoder comparisons alternate old MPS and the new tile with nonzero bias:
25088×128→512 measured 2347.7→1633.2 µs; 578×768→3072 measured
1588.9→1272.3 µs. See [f16_encoder.txt](f16_encoder.txt).

The 0.8B profile confirms the new convolution and split-K kernels execute in
actual reads. Florence's earlier profile left most GPU time outside named
compute encoders, motivating replacement of the mixed-type MPS projection
route. Unattributed time alone does not identify an individual bottleneck.

## Verification and provenance

[conformance.txt](conformance.txt) passes full production-kernel numerical,
nonzero-offset, tail, canary, missing-pipeline, rollback, shared-scratch and
concurrently submitted frame checks. Residency tests cover tied allocations,
no-copy buffers, replacement, clear, cap and opt-out. F16 old/new comparisons
were bit-exact on the tested geometries; independent CPU sample errors stayed
below 1e-6. These checks do not promise bit-exactness for arbitrary inputs.
[focused_gpu_final.txt](focused_gpu_final.txt) records **73/73 focused GPU tests**
with residency enabled on the final runtime, including HTTP, cancellation,
coalescing, source policies and concurrent readers.

[residency_build_manifest.json](residency_build_manifest.json) binds the final
stripped ReleaseFast binary to source hashes and its runtime diff relative to
commit `79641182642b96b136f5142ed4cacb68538a2730`.
[f16_encoder_build_manifest.json](f16_encoder_build_manifest.json) binds the
preceding build used by the controls and first Florence pair. Harness
`--build-manifest` verifies the executable hash and records both historical
build identity and the current worktree identity. Reports without an override
record the live runtime diff. The build log includes five expected sandbox GPU
skips; the separate final GPU test run has zero skips.

Checkpoint revisions and SHA256 values are unchanged from
[the preceding manifest](../kernel_parity_m4_max/checkpoints.json).
vLLM/vLLM Metal 0.32.0, MLX 0.32.2, MLX-VLM 0.7.6. Native LightOn has F32
activations/state with BF16 weights; reference activations/output are BF16.
Installed vLLM Metal has no Florence adapter, so Florence uses MLX-VLM.
Native Florence uses Q4_K weights and F16 encoder mirrors, against original
FP16 reference weights. Native also applies no-repeat n-gram size 3, unlike
the MLX default greedy reference. Precision and generation policy differ.

[output_validation.json](output_validation.json) checks every successful native
report against its original native output exactly and refreshed cross-runtime
content after markup/whitespace removal. Timed outputs match their own serial
references. Three synthetic fixtures are consistency tests, not an OCR quality
evaluation. Reference report hashes are retained in each native report.
[log_normalization.json](log_normalization.json) preserves original/stored hashes
for LF and trailing-whitespace normalization of raw logs.

The [disabled-wiring diagnostic](vllm_4b_no_wiring_diagnostic/README.md) was
terminated incomplete and is excluded from performance targets. Normal vLLM
references retain their ordinary wiring behavior.

## Reproduction

Build the inference tests with Zig 0.17, ReleaseFast and stripping, using the
worktree's `zig/.zig-cache`. Use the preceding artifact's build/reproduction
instructions, then run `benchmark_concurrency.py` with the same three images,
16 requests, concurrency/rows/window 8, max tokens 256, batch wait 20000 µs,
decode wait 1000 µs, and process/shared/backend/KV/scratch limits
24/28/16/4/4 GiB (Florence scratch 8 GiB). Select `--weight-residency` for the
LightOn rows above. Florence's batch collection window is 5000 µs.
The reports preserve all actual arguments. Run GPU workloads successively.

The standalone native checker exposes `--benchmark-projection-parity`,
`--benchmark-down-split`, `--benchmark-prefill-conv` and
`--benchmark-f16-encoder`. Compile `metal_command_timing.m` as an ARC dylib linked
with Foundation/Metal and pass it to `benchmark_mlx_projections.py
--timing-library PATH --output OUTPUT`. The command-buffer observer is benchmark
only and fails measurements with invalid timestamps; it is not linked into
Antfly. Existing `benchmark_vllm_metal.py` and `benchmark_mlx_florence.py` reproduce
page references with checkpoint caching disabled as documented previously.
