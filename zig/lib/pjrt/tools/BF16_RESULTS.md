# Experimental GLiNER2.5 BF16 CUDA versus native PJRT on L4

Same NVIDIA L4, checkpoints and prepared geometries as [the FP32 comparison](OPTIMIZATION_RESULTS.md). These results compare the explicit native hybrid BF16 experiment with PJRT BF16. Managed serving remains on its qualified precision policy.

## Method

Three fresh-process rounds rotate control/native/PJRT, native/PJRT/control and PJRT/control/native. Each case has three warmups and eight measured forwards per round (24 samples per arm). The control is the frozen **optimized native FP32** binary, not the original unoptimized baseline. Executable compilation and first-use GEMM tuning are excluded. No other compute process used the GPU.

Both workers start from identical prepared host tensors and end with synchronized F32 requested-head readback. Forward temporary allocations and input uploads are included; rendering, tokenization, final decoding, HTTP, loading, handle setup and post-readback cleanup are excluded. Rows repeat within batches. These are geometry benchmarks, not diverse task-accuracy qualification. All checkpoint parameters, including unused heads, remain resident.

The native experiment uploads encoder and classifier/boundary matrices as BF16, uses direct BF16 attention operands, BF16 score/probability and FFN storage, and retains F32 embedding/norm/bias storage, residuals, reductions and requested outputs. Attention projection biases use RNE BF16 snapshots. This is hybrid precision, distinct from the exported PJRT BF16 graph.

## Latency

Medians in milliseconds. A speedup above 1 favors native CUDA.

| Workload | Tokens | Batch | Native FP32 control | Native BF16 experiment | PJRT BF16 | Native / PJRT speedup | Strict numerical gate |
|---|---:|---:|---:|---:|---:|---:|---|
| Decide classification | 128 | 1 | 18.22 | 4.49 | 7.93 | 1.77× | Fail |
| Decide classification | 128 | 8 | 89.68 | 24.42 | 25.71 | 1.05× | Fail |
| Decide classification | 512 | 1 | 62.12 | 16.29 | 16.49 | 1.01× | Fail |
| Decide classification | 512 | 8 | 393.50 | 106.50 | 129.33 | 1.21× | Pass |
| Base classification | 128 | 1 | 5.45 | 1.82 | 2.84 | 1.56× | Fail |
| Base classification | 128 | 8 | 28.60 | 6.97 | 7.39 | 1.06× | Fail |
| Base classification | 512 | 1 | 19.09 | 4.69 | 5.20 | 1.11× | Fail |
| Base classification | 512 | 8 | 129.68 | 35.82 | 45.23 | 1.26× | Fail |
| Base extraction | 128 | 1 | 6.61 | 2.64 | 3.15 | 1.20× | Fail |
| Base extraction | 512 | 1 | 20.41 | 5.32 | 5.36 | 1.01× | Fail |

Native BF16 medians match or beat PJRT BF16 on **10/10** cases. Narrow margins are specific to this L4 and software stack.
Long extraction is approximately parity: its pooled median advantage is 0.9%, and native wins two of three per-round medians. Small 1–2% margins are not established as robust hardware advantages.

## Memory

Median of three process peaks, in MiB, for each whole multi-geometry campaign. Sampling includes the common Python harness and parameter feeder, loading/deserialization, warmup and inference; cold compilation is excluded. PSS+GPU is the peak simultaneous sum, sampled every 0.2 seconds. Runtime inventories and allocation policies differ.

| Campaign | Native GPU | PJRT GPU | Native PSS+GPU | PJRT PSS+GPU |
|---|---:|---:|---:|---:|
| Decide classification | 1840 | 2362 | 3099 | 3457 |
| Base classification | 1078 | 1276 | 1755 | 2048 |
| Base extraction | 950 | 746 | 1674 | 1740 |
Base extraction uses about 27% more GPU memory than PJRT BF16, while its combined PSS+GPU peak is lower. The experiment does not win every memory comparison.

## Correctness and limits

- Current FP32 latency favors native in 9/10 cases. Long extraction is 20.16 ms native versus 20.03 ms PJRT and 20.00 ms frozen native control; this narrow difference remains visible alongside the passing numerical checks.
- The strict profile gate includes all measured rounds, the FP32 control, native/PJRT outputs and the shortest-case CPU oracle where present. Per-arm differences are retained in the machine-readable report.
- FP32 regression profiles: **10/10 pass**, largest raw-output error 0.00012207; exact decisions and canonical candidate routing are required. The FP32 serving math remains unchanged; pool prefix selection preserves its total ordering.
- BF16 gates remain raw error ≤0.02 plus exact decisions or canonical candidate indices/masks. Failing rows remain failures. Extraction differences can include candidate sets as well as scores; a flattened raw error can include span-coordinate differences. These profiles do not establish task accuracy or qualify BF16 serving.
- Automatic FP32 attention dispatch remains bounded to SM89, at most eight rows and 512 tokens, with 64 dimensions per head. Explicit BF16 attention/projection workspace is capped at 64 MiB; FFN temporary storage is at most 88 MiB. Immutable relative-projection and bias snapshots share a 96 MiB cap; biases have a 256 KiB/128-vector sublimit, and the mutable norm slot has an 8 MiB sublimit within that same total. Shared admission is retained when supplied, and denied reservations use uncached computation.
- Twenty-eight focused checks cover GPU/CPU attention and FFN oracles, direct BF16 projections, head band/padding semantics, serial/F64 parallel-dot and prefix oracles, exact full-stable pool selection including sentinel-score fallbacks, duplicate-free quota selection, cached compatibility reuse and scalar-compatible SIMD, source/generation identities, snapshot independence, alias invalidation, bounds and lease release. Parallel head reductions additionally match serial GPU and F64 ragged/padding oracles; pool reuse and PDQ ordering match the full stable reference bit for bit. Fused norm tests require original F32 bits and exact RNE BF16 snapshots. CUDA type checking and regenerated portable PTX/fatbin/SM89 artifacts pass.
- No HTTP, Q8_0, other-GPU or broad task-accuracy campaign is included.

See [README.md](README.md) for reproduction and [gliner25-bf16-l4.json](gliner25-bf16-l4.json) for checkpoint, executable, runtime and source hashes, per-case gates, memory and route counters. Raw tensors, HLO, executables and traces remain ignored local evidence.
