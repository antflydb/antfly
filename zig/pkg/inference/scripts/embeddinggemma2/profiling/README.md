# L4 GPU profiling: native CUDA versus PJRT

Follow-up: [bounded native global attention implementation and results](GLOBAL_ATTENTION.md).

Captured 2026-11-16 on NVIDIA L4 SM89, driver 580.159.03, Nsight Systems
2025.6.3. Both paths receive identical prepared IDs/masks and return normalized
768-float vectors. Python does not participate in either timed forward.

**The main bottleneck is the native attention kernel, especially the four
512-dimensional global-attention layers.** The GPU spends its time executing
that kernel; request scheduling and launch count do not explain most of the gap.

## Matched measurements

Five warmed forwards per Nsight capture, selected with `cuProfilerStart/Stop`.
CUDA graph node tracing is enabled. Loading, warmup and compilation are outside
the captured range. GPU span runs from the first captured GPU operation to the
final 3072-byte vector readback. CUDA API and GPU durations overlap and must not
be added.

| Tokens | Native CUDA GPU span ms | XLA GPU span ms | Native attention ms | Native GPU activity | XLA GPU activity |
|---|---:|---:|---:|---:|---:|
| 512 | 15.45 | 5.72 | 10.42 | 97.9% | 93.0% |
| 2,048 | 83.08 | 29.01 | 63.81 | 99.5% | 98.5% |
| 8,192 | 624.96 | 322.44 | 545.93 | 99.9% | 99.9% |

Activity means a kernel or copy was active, **not** SM occupancy, Tensor Core
utilization or percent of peak bandwidth. Nsight Compute returned
`ERR_NVGPUCTRPERM`; the driver restricts those hardware counters to an
administrator, and privileged execution is unavailable here. The traces still
provide kernel durations, copy volume, launch geometry and GPU idle intervals.

The native CUDA trace has about 365--376 kernel executions per forward, versus
548--609 for XLA. Thus XLA's lead is not explained by fewer kernel launches.
Named `gemm_fusion_*` kernels and generated pointwise/reduction kernels account
for its execution; fusion benefits alone have not been isolated.

### The global layers dominate long input

At 8192 tokens, native attention breaks down as:

| Layer type | Layers | Device time per forward |
|---|---:|---:|
| Global, head dimension 512 | 4 | 403.62 ms |
| Sliding, head dimension 256 | 20 | 142.31 ms |

Those four global layers alone cost more than XLA's complete forward. At 2048
tokens the split is 30.07 ms global and 33.74 ms sliding, so both matter there.

The generated XLA HLO contains BF16 QK and PV matrix products. For example,
`dot_general.270` produces `[4,8192,8192]` scores and `dot_general.271` produces
`[4,8192,256]` values for a sliding layer. These names correspond to repeated
`gemm_fusion_dot_general_270/271` kernels in the trace. XLA's dense score
materialization trades memory for efficient matrix multiplication; its exact
optimized fusion boundaries need an optimized-HLO dump for complete attribution.

Native's current flash kernel uses a 64-query by 16-key tile, dimension partials,
shared-memory reductions and an online output accumulator. The shipped SM89
artifact reports 64 registers/thread and a 288-byte stack frame. Rebuilding the
same kernel with the pinned compiler reports 48 bytes of spill stores and
108 bytes of spill loads. This establishes spilling; it does not quantify stall
cycles or bandwidth lost to spills without hardware counters.

The installed cuDNN is 9.8. Native's qualified sliding-window cuDNN path requires
9.21 or newer for radius 512, so that path is unavailable on this machine.
Removing a version/qualification gate is not a validated kernel optimization.

### Direct timing narrows the short-request gap

A separate unprofiled native CUDA run used four warmups and eight measured
forwards at every length. The benchmark calls the same resident CUDA executor
used by HTTP; the generic CPU/Metal prepared-text function does not support CUDA.

| Tokens | Native CUDA direct ms | Earlier native PJRT direct ms | PJRT ratio |
|---|---:|---:|---:|
| 128 | 5.17 | 3.73 | 1.39x |
| 512 | 15.32 | 6.94 | 2.21x |
| 2,048 | 85.18 | 32.97 | 2.58x |
| 8,192 | 630.12 | 333.12 | 1.89x |

The earlier HTTP comparisons included extra serving overhead, particularly at
128 tokens. These direct figures give a better baseline for kernel work.

All native runs enabled the existing extended-flash, packed-projection,
fused-backbone, tuned-GEMM and graph-replay flags. The direct run and the traces
are different process lifecycles; the PJRT unprofiled column is the retained
prior campaign, not a randomized paired rerun. Captured timings also carry
profiler overhead. Read the unrounded data in [profile-l4.json](profile-l4.json).

## Native kernel experiments

[attention_tile64.cu](attention_tile64.cu) is an isolated benchmark candidate,
not production dispatch. It uses a 64-key tile, compile-time head dimensions,
shared-memory reuse and padded strides. The global variant uses a register
limit that eliminates its spills in the compiler report. It requires up to
77,568 bytes of dynamic shared memory per block; the driver opts in explicitly.

An alternating original/candidate campaign used three warmup pairs and twelve
measured pairs, CUDA event timings, fixed seeded BF16 inputs, local GQA and
global MQA, with mask-hole cases at 512 tokens:

| Tokens | Geometry | Original ms | Candidate ms | Speedup |
|---|---|---:|---:|---:|
| 512 | local 256 | 0.388 | 0.217 | 1.79x |
| 512 | global 512 | 0.694 | 0.389 | 1.78x |
| 2,048 | local 256 | 1.390 | 0.953 | 1.46x |
| 2,048 | global 512 | 6.668 | 4.381 | 1.52x |
| 8,192 | local 256 | 6.205 | 4.487 | 1.38x |
| 8,192 | global 512 | 94.423 | 58.949 | 1.60x |

Minimum attention-output cosine was 0.999998887; maximum absolute difference
was 0.001953125. Both mask-hole cases passed, with 1.84--1.85x speedups. This is
synthetic operator agreement, not whole-model/retrieval qualification. Only
batch one, SM89 and sequence lengths divisible by 64 are covered by this tool.
No full-model speedup is claimed for the candidate.

Earlier experiments are retained under `evidence/`: shared-memory padding alone
was bit-identical but only 6--14% faster; a 32-key tile was weaker than the
64-key candidate. Keeping queries in shared memory was slower. Replacing the
candidate's `expf` with `__expf` did not materially improve its timings. These
experiments do not support attributing the gap solely to bank conflicts, query
loads or exponential throughput.

### Highest-value implementation work

1. Optimize the global 512-dimensional attention path first. The existing flash
   schedule is much slower than XLA's entire encoder at long input.
2. Evaluate a **bounded native QK GEMM -> masked softmax -> PV GEMM** schedule
   against the wider-key flash candidate. Existing CUDA helpers support strided
   attention GEMMs and a bounded softmax tile. A 256-query, four-head, 8192-key
   tile needs 32 MiB F32 scores plus 16 MiB BF16 probabilities, before GEMM
   workspace. This is a workspace design estimate, not a measured memory peak.
3. The old direct cuBLASLt attention path was disabled after concurrent-result
   drift. Reproduce and resolve that failure, or use an independently validated
   implementation; do not silently re-enable it. Cover masks, tails, concurrent
   requests, graph replay, cancellation, workspace ownership and full-model
   vector agreement before promoting a new path.
4. Tune the 256-dimensional local path separately. At long lengths, native
   sliding attention avoids XLA's dense quadratic score work; preserving that
   memory/computation advantage matters.

## Reproduce

From the worktree root, build the direct benchmark with worktree-local caches:

```sh
ZIG_GLOBAL_CACHE_DIR="$PWD/.zig-cache/global" zig build \
  --build-file zig/pkg/inference/build.zig build-embeddinggemma2-bench \
  -Doptimize=ReleaseFast -Dcuda=true -Dcuda-artifacts=sm89 \
  --cache-dir "$PWD/.zig-cache/native" --prefix "$PWD/zig-out" -j1
```

Use the model manifest produced by the PJRT preparation tool. Convert its cases
to `{ "cases": [{ "name": case.id, "token_ids": case.prompt_ids }] }` for the
native benchmark. Set the same CUDA/cuDNN library paths for both executors.
Enable these CUDA flags, each `=1`:

- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_EXTENDED_FLASH`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_PACKED_PROJECTIONS`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_GRAPH_REPLAY`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_FUSED_BACKBONE`
- `ANTFLY_INFERENCE_CUDA_EMBEDDING_GEMMA2_TUNED_GEMM`

Capture one native case:

```sh
ANTFLY_CUDA_PROFILE=1 nsys profile --trace=cuda --sample=none --cpuctxsw=none \
  --cuda-graph-trace=node --capture-range=cudaProfilerApi --capture-range-end=stop \
  -o evidence/cuda-2048 zig-out/bin/antfly-embeddinggemma2-bench \
  --model "$EMBEDDING_MODEL_DIR" --suite "$PREPARED_SUITE" --case text_2048 \
  --backend cuda --warmups 4 --iterations 5 --long-iterations 5 \
  --output evidence/cuda-2048-results.json
```

The native PJRT runner supports the same `ANTFLY_CUDA_PROFILE=1` range. Use a
single-case manifest with four warmups and five repeats, and stream parameters
before its range begins. Keep native `XLA_FLAGS` unset. Compile/setup can take
minutes but is excluded from the range. To inspect numerical drift, retain every
vector and compare with the unmodified oracle manifest after capture; do not
present a trace with failed numerical checks as release qualification.

Export and summarize:

```sh
nsys export --type=sqlite -o evidence/cuda-2048.sqlite evidence/cuda-2048.nsys-rep
python zig/lib/pjrt/tools/analyze_profile.py evidence/cuda-2048.sqlite \
  --out evidence/cuda-2048-summary.json
```

Compile and run the independent native attention candidate (no XLA required):

```sh
nvcc -cubin -arch=sm_89 -Xptxas=-v \
  zig/pkg/inference/scripts/embeddinggemma2/profiling/attention_tile64.cu \
  -o evidence/attention-tile64.cubin
python zig/pkg/inference/scripts/embeddinggemma2/profiling/compare_attention_tiles.py \
  --cubin evidence/attention-tile64.cubin --out evidence/attention-tile64.json
```

The Python microbenchmark requires NumPy and ml_dtypes, uses the CUDA Driver API
through ctypes, and times only kernel execution with GPU events. Run it on an
idle GPU. It compiles the current checked-in attention header as the control.

Raw traces, SQLite exports, every vector, CUDA API statistics, static artifact
resource reports and unsuccessful experiments are retained in ignored
`evidence/profiles/` and `evidence/attention*`. The earlier strict XLA vector gate
failed on one instrumented compilation before capture; subsequent diagnostic
traces retained all vectors. Its 2048-token oracle cosine was about 0.999894.
This is recorded explicitly in the JSON and is not a claim of exact parity.
