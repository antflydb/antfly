# Optimized GLiNER2.5 CUDA versus native PJRT on L4

Measured on NVIDIA L4 SM89, driver 580.159.03, using the same checkpoints and prepared graphs as the [original baseline](RESULTS.md). Native computation remains FP32. This records the completed FP32 campaign; subsequent explicit BF16 experiments are reported in [BF16_RESULTS.md](BF16_RESULTS.md).

## Implementation

The native CUDA route replaces scalar attention with strict FP32 batched GEMMs and a fused relative-gather/softmax kernel. It batches interleaved heads across documents without copying activations, projects only needed relative-position rows for base and original Decide FP32, fuses eligible FP32 projection bias additions, and skips record-head work for CUDA requests that do not ask for records. Projection and attention operations reuse a shared workspace capped at 32 MiB, including vendor scratch and pointer arrays. The new attention dispatch is automatic on SM89 within its bounded geometry (at most eight rows, 512 tokens and 64 dimensions per head); existing routes remain the fallback.

## Method

Three fresh-process rounds rotate control/native/PJRT, native/PJRT/control and PJRT/control/native. Every case has three warmups and eight measured forwards per round, for 24 samples per arm. The control is the frozen pre-optimization native binary; PJRT uses the same cached FP32 executables as the baseline. No other compute process used the GPU.

Timing starts with identical prepared host tensors and ends with synchronized F32 requested-head outputs. It includes input uploads, forward temporary allocations, encoder and heads, and readback. Tokenization, final decoding, HTTP, model load, backend-handle setup and post-readback cleanup are excluded. Classification uses the 14-label hotel schema; extraction uses five entity labels. Synthetic input rows repeat within each batch, so these are geometry benchmarks rather than task-accuracy qualification. All checkpoint parameters stay resident in both workers.

## FP32 latency

Medians in milliseconds. Speedups above 1 favor optimized CUDA.

| Workload | Tokens | Batch | Native control | Optimized CUDA | PJRT FP32 | CUDA / control speedup | CUDA / PJRT speedup |
|---|---:|---:|---:|---:|---:|---:|---:|
| Decide classification | 128 | 1 | 19.73 | 18.29 | 22.53 | 1.08× | 1.23× |
| Decide classification | 128 | 8 | 102.32 | 92.38 | 93.99 | 1.11× | 1.02× |
| Decide classification | 512 | 1 | 96.39 | 62.23 | 63.24 | 1.55× | 1.02× |
| Decide classification | 512 | 8 | 645.69 | 393.66 | 446.84 | 1.64× | 1.14× |
| Base classification | 128 | 1 | 9.24 | 5.67 | 6.47 | 1.63× | 1.14× |
| Base classification | 128 | 8 | 41.38 | 29.03 | 30.48 | 1.43× | 1.05× |
| Base classification | 512 | 1 | 49.97 | 19.42 | 20.14 | 2.57× | 1.04× |
| Base classification | 512 | 8 | 360.41 | 133.99 | 148.56 | 2.69× | 1.11× |
| Base extraction | 128 | 1 | 9.96 | 6.73 | 7.02 | 1.48× | 1.04× |
| Base extraction | 512 | 1 | 50.62 | 20.39 | 20.82 | 2.48× | 1.02× |

All ten measured FP32 medians favor native CUDA. The narrowest gains are about 1.6%; these are L4 measurements, and small margins should not be generalized to other GPUs or software versions.

All 10 FP32 cases pass the frozen maximum raw-output error of 0.002 and exact decisions or canonical candidate-routing checks. Largest observed error across all three arms: 0.000130. The independent CPU reference covers the shortest singleton per profile. Native route counters confirm optimized attention in every fresh process, compact projections for both models, fused bias epilogues, and pointer batches for batched classification.

## Memory

Median of three process peaks, in MiB. Each row covers its entire multi-geometry campaign, including startup, model loading, cached executable deserialization, warmup and inference. Cold compilation is excluded. PSS+GPU is a peak simultaneous sum. Sampling includes the common Python harness and parameter feeder for every arm, at 0.2-second intervals.

| FP32 campaign | Control GPU | CUDA GPU | PJRT GPU | Control PSS+GPU | CUDA PSS+GPU | PJRT PSS+GPU |
|---|---:|---:|---:|---:|---:|---:|
| Decide classification | 2432 | 2440 | 4480 | 3510 | 3183 | 5915 |
| Base classification | 1234 | 1276 | 2304 | 1719 | 1774 | 3526 |
| Base extraction | 1082 | 1124 | 1272 | 1692 | 1680 | 2824 |

Runtime inventories and retained workspaces differ; this comparison does not isolate allocator efficiency.

## BF16 PJRT characterization

Native remains FP32. BF16 PJRT changes precision and does not pass the full strict numerical gate; its timings do not establish a qualified replacement. The raw-output threshold remains 0.02.

| Workload | Tokens | Batch | CUDA FP32 ms | PJRT BF16 ms | CUDA / PJRT speedup | Numerical gate |
|---|---:|---:|---:|---:|---:|---|
| Decide | 128 | 1 | 18.47 | 8.06 | 0.44× | Fail |
| Decide | 128 | 8 | 95.45 | 26.05 | 0.27× | Pass |
| Decide | 512 | 1 | 64.49 | 16.95 | 0.26× | Pass |
| Decide | 512 | 8 | 411.53 | 131.57 | 0.32× | Pass |
| Base | 128 | 1 | 5.79 | 2.82 | 0.49× | Fail |
| Base | 128 | 8 | 29.26 | 7.49 | 0.26× | Fail |
| Base | 512 | 1 | 19.62 | 5.29 | 0.27× | Fail |
| Base | 512 | 8 | 134.82 | 45.62 | 0.34× | Fail |

## Validation and limits

- Eight focused tests pass, including live GPU FP32 attention against CPU oracles, distinct document masks, compact and expanded relative rows, group boundaries, projection workspace plans, request admission, and preservation of requested record outputs.
- CUDA-enabled inference type check, native benchmark build, six Python protocol/numerical tests, Ruff, Zig formatting and diff whitespace checks pass. PJRT library: 38 focused tests pass.
- Kernel artifacts regenerated using the pinned CUDA toolchain for portable PTX/fatbin and SM89 cubin.
- No Q8_0, end-to-end HTTP, other-GPU or diverse task-accuracy campaign was run. Native BF16/F16 eligibility is unchanged.

See [README.md](README.md) for reproduction, [gliner25-optimized-l4.json](gliner25-optimized-l4.json) for compact metrics, hashes and route counters, and [the baseline report](RESULTS.md) for checkpoint provenance and BF16 CPU preflight details. Models, tensors, HLO, executable caches and traces remain ignored local artifacts.
