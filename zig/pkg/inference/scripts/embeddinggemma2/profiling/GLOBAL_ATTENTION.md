# EmbeddingGemma2 global attention on L4

This candidate targets the four global-attention layers with Q head dimension
512, four Q heads and one shared KV head. It uses native CUDA and cuBLASLt;
PJRT/XLA is not involved. The default library workspace is zero, matching
the native executor. The isolated harness can also test an explicit workspace
with `ANTFLY_GLOBAL_BENCH_WORKSPACE_MB`.

The route is opt-in with
`ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GLOBAL_GEMM=1`. Dispatch is limited to
SM89, batch one and sequence lengths 512–8192. Other geometries, insufficient
workspace admission, and unsupported library algorithms retain the prior
attention fallback. The default policy is unchanged.

## Execution and ownership

Each tile packs 256 Q rows below 4096 tokens and 128 rows at longer lengths
into independent head matrices, computes
FP32 QK scores, applies masked FP32 softmax with one BF16 rounding, computes
BF16 PV, and unpacks into the model's BSHD output. Both batched GEMM destinations
are disjoint contiguous matrices. This avoids relying on interleaved output
matrices whose physical bounding ranges overlap between heads.

The score/probability/Q/output arena is 25 MiB at 8192 tokens and reused across
layers. Its qualified maximum over all supported lengths is below 26 MiB.
The default native policy uses zero cuBLAS workspace bytes. An
explicit library-workspace setting remains separately owned. The new arena
owns a shared admission lease while retained, is released on cache eviction or
admission-policy replacement, and declines to the old route under pressure.
CUDA graph keys track global-attention, cuDNN and cuBLAS workspace addresses,
plus the global workspace's allocation generation. Failed growth can retire
scratch before a smaller allocation reuses its address; that new generation
invalidates the old graph even when the pointer is unchanged. Reusing the same
live allocation preserves its generation and replay eligibility.

The arithmetic policy has a separate embedding identity. Results produced with
this BF16 attention schedule must not reuse a pinned identity from another
schedule merely because their final vectors are close.

## Reproduction

Build from the worktree, retaining its own caches and `zig-out` prefix:

```sh
export ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global"
zig build --build-file zig/pkg/inference/build.zig \
  build-embeddinggemma2-bench build-embeddinggemma2-global-bench \
  test-embeddinggemma2-global -Doptimize=ReleaseFast \
  -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/native" --prefix "$PWD/zig-out" -j1
```

Use the same CUDA/cuDNN library paths for both arms. The Python driver requires
NumPy, tokenizers, psutil and nvidia-ml-py. It orchestrates processes and samples
memory; Python does not execute the timed model.

```sh
scripts=zig/pkg/inference/scripts/embeddinggemma2/profiling
python "$scripts/make_global_suite.py" --model "$EMBEDDING_MODEL_DIR" \
  --out evidence/global-performance-suite.json
python "$scripts/compare_global_model.py" \
  --binary zig-out/bin/antfly-embeddinggemma2-bench \
  --model "$EMBEDDING_MODEL_DIR" --suite evidence/global-performance-suite.json \
  --out evidence/global-performance --rounds 3 --iterations 8
python "$scripts/make_global_suite.py" --model "$EMBEDDING_MODEL_DIR" \
  --out evidence/global-validation-suite.json --validation
python "$scripts/compare_global_model.py" \
  --binary zig-out/bin/antfly-embeddinggemma2-bench \
  --model "$EMBEDDING_MODEL_DIR" --suite evidence/global-validation-suite.json \
  --out evidence/global-validation --rounds 1 --iterations 2
```

The performance IDs match the earlier CUDA/PJRT `river` fixtures exactly.
The validation suite adds varied text, odd lengths, right padding, and an identical 2048-token input before and after an 8192-token call.
The driver checks bitwise stability across that workspace-growth sequence.

The isolated harness supports `none`, `holes`, or `all` masks and one or two
concurrent CUDA contexts. Internal mask holes are tested here: the full-model
prepared-input contract requires contiguous masks and rejects holes before
attention executes. The harness compares every output against the prior flash
kernel, samples complete output rows against a CPU FP64 oracle, and requires
bitwise deterministic repeat outputs. Concurrent runs require matching output
hashes across independently owned contexts/streams/library handles.

```sh
zig-out/bin/antfly-embeddinggemma2-global-bench \
  8192 128 evidence/global-kernel.json none 1
zig-out/bin/antfly-embeddinggemma2-global-bench \
  8192 128 evidence/global-concurrent.json none 2
```

These direct timings cover embedding lookup through synchronized normalized
vector readback. They exclude tokenization, HTTP, model loading and warmup.
Kernel concurrency checks are separate from serving concurrency qualification.

## Isolated operator results

NVIDIA L4 SM89, CUDA 13.2 cuBLASLt, zero library workspace. Medians of nine
CUDA-event samples after three warmups; seeded BF16 Q/K/V, batch one, four
512-dimensional Q heads and one KV head. These are operator timings, not model
or HTTP latencies.

| Tokens | Prior flash ms | Packed GEMM ms | Speedup | Scratch |
|---|---:|---:|---:|---:|
| 512 | 0.695 | 0.163 | 4.26x | 5 MiB |
| 2048 | 6.904 | 1.051 | 6.57x | 14 MiB |
| 8192 | 98.871 | 17.760 | 5.57x | 25 MiB |

The 128-row tile is slower at 512/2048, so only longer sequences use it.
The GPU reports 48 MiB L2; the smaller long tile reduces live score/probability
storage as well as measured time. Hardware counters were unavailable, so cache
hit rates and Tensor Core utilization have not been measured.

The standalone checks retain every output and require cosine >=0.9998 against
the prior kernel, finite outputs, sampled CPU FP64 error <=0.01, and bitwise
stable repeated execution. Tested lengths include 1, 17, 65, 255, 257, 511,
512, 513, 2048, 8191 and 8192, including masked and fully masked rows. Sampled
FP64 error is below 0.003 across these tests. Two independently owned contexts
also return equal output hashes at 2048 and 8192 tokens. These checks qualify
the arithmetic and buffer layout; they do not establish retrieval quality on a
representative corpus.

## Projection autotuning and numerical qualification

The comparison driver disables the existing projection autotuner by default
(`TUNED_GEMM=0`). This holds projection arithmetic fixed when isolating the
attention change. All other comparison flags are recorded in each summary.
`--tuned-gemm` permits that separate experiment.

An initial campaign with projection autotuning enabled failed the declared
0.9999 full-vector cosine threshold in one 2048-token run (0.999866); it is
not a qualified result. Even unchanged control arms differed at shorter
lengths as the tuner selected different BF16 algorithms. Fixed-projection
validation is reported separately rather than relaxing the numerical threshold.

The normalized global softmax has its own CUDA entry point. The existing
vision softmax source is unchanged; `check_softmax_scope.py` compares its old
and new compiled entry points bit-for-bit across unmasked, hole-masked and
fully masked cases at 17, 513 and 8192 tokens (12 heads). Supply the baseline
SM89 cubin from the branch base and the rebuilt candidate cubin.

Full-model comparisons disable the existing projection autotuner in both arms.
The initial campaign with autotuning enabled selected different BF16 projection
algorithms between processes and one retained vector missed the strict 0.9999
cosine floor (0.999866). That campaign is not numerical qualification. The
fixed-projection comparisons keep the same non-autotuned projection arithmetic;
`--tuned-gemm` is available for separate experiments. Full-model checks require
finite normalized 768-float vectors, cosine >=0.9999 and maximum absolute
component difference <=0.003 versus the matched native control. Direct timings
and memory must be qualified separately from HTTP serving concurrency or a
representative retrieval/ranking evaluation.

## Full-model results

Final separate-entry-point build, L4, identical prepared token IDs and BF16
weights. Three alternating native process rounds, four warmups and eight timed
forwards per length per arm. PJRT is a fresh native CUDA process with four
warmups and eight timed forwards per length. Projection autotuning is disabled
in both native arms. Times are medians in milliseconds.

| Tokens | Native control | Native global GEMM | Native speedup | Native PJRT |
|---|---:|---:|---:|---:|
| 128 | 5.20 | 5.21 | 1.00x | 3.72 |
| 512 | 15.45 | 13.60 | 1.14x | 6.84 |
| 2048 | 85.62 | 59.59 | 1.44x | 32.42 |
| 8192 | 632.28 | 294.38 | 2.15x | 329.25 |

The 8192-token native forward is 1.12x faster
than the fresh PJRT run. PJRT still leads at the shorter lengths. Peak GPU memory
is 2.37 GB versus 2.34 GB
for native control and 4.54 GB for PJRT. Sampled
process-tree PSS plus NVML peaks are 3.65 GB
and 5.86 GB respectively. Memory covers
startup and inference, including PJRT compilation; its offline weight feeder
is excluded. Native loads the full multimodal checkpoint, while the exported
PJRT program runs the text backbone.

All final rounds pass the vector tolerances. The minimum final native-control
cosine is 0.999916102; varied-text/padding
holdouts reach 0.999975956 or higher. Replaying
2048 tokens after workspace growth to 8192 preserves every output component
bit-for-bit. Nine focused tests pass in CUDA-enabled and CPU-only builds. The
historical vision softmax also matches the original compiled image in all nine
regression cases. All three checked-in CUDA image formats reproduce exactly.

Five warmed Nsight forwards measure a 300.88 ms
median GPU span, with 99.49% GPU
activity and 1652 kernels per forward. The four
global layers now total about 77.81 ms including packing, QK/PV GEMMs,
normalized softmax and unpacking, versus the earlier 403.62 ms. The 20 sliding
layers total 144.07 ms and are the
next dominant target. This attribution uses distinct kernel names and the
expected 256 launches of each global stage per forward. Activity is not SM
occupancy or Tensor Core utilization; hardware counters remain unavailable.

[Machine-readable results](global-attention-l4.json) retain binary/checkpoint/
source hashes, per-case metrics, vector bounds, memory scope and profile totals.
Raw logs, sampled memory, vectors and Nsight exports remain under
`evidence/global-gemm/` in the worktree.
