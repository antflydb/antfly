# Gemma 4 26B-A4B Performance Analysis

## 2026-08-25 CUDA resident Q4_0 lane

The CUDA backend now has a fail-closed resident qualification candidate for the same exact
26B-A4B Q4_0 geometry qualified by the Metal lane: 30 MoE layers, 128
experts, top-8 routing, hidden size 2816, and expert intermediate size 704.
The first hardware target is NVIDIA SM89 (L4). Packed expert sources are
uploaded once and deduplicated when gate/up projections alias the same GGUF
storage. Routing, Q8_1 activation quantization, Q4_0 gate/up/down projections,
GELU, and routed reduction stay on device for decode and prefill rows.

CUDA intentionally supports only full residency for this qualification. A
streamed request, a non-SM89 device, an inexact geometry/quantization, a
missing kernel, or a resident footprint outside the configured envelope fails
model load instead of falling back to host MoE execution. The initial command
is:

```sh
TERMITE_SERVER_A4B_TELEMETRY=1 \
  ANTFLY_INFERENCE_CUDA_DECODE_GRAPH_REPLAY=required \
  ANTFLY_INFERENCE_CUDA_CAPTURE_FORCE_KV_CAPACITY=192 \
  ./zig-out/bin/antfly-inference generate "$MODEL" "$PROMPT" \
  --backend cuda --a4b-residency-mode resident \
  --a4b-memory-budget-mb 16384 --backend-budget-mb 16384 \
  --combined-budget-mb 24576 --cache-dtype f16 \
  --raw-prompt --temperature 0 --ignore-eos --max-tokens 128 \
  --print-token-ids --print-prompt-token-ids --print-timing
```

`ANTFLY_INFERENCE_CUDA_DISABLE_A4B_FAST_PATH=1` is the load-time rollback.
It deliberately rejects the qualified CUDA request rather than selecting a
slower implementation.

Server admission additionally charges the encoded GGUF as transient host
memory while the native loader uploads CUDA residency. For the pinned artifact
the peak combined envelope is about 30,154 MiB; after load the lease settles
back to the 16,384 MiB CUDA resident envelope.

The release gate remains the paired deterministic contract used above: the
128-token SHA-256 must be
`d4ee583f092062e7177069de1f35a9cefbaac24848d11d09b64237bc9209b68e`,
every request must report positive CUDA A4B route/decode counters and zero
fallbacks, and warmed decode throughput on the pinned L4 must be at least 80%
of the pinned llama.cpp CUDA comparator. The benchmark requires the expected
llama.cpp executable SHA-256 and a clean tracked checkout, then records both
executables, the model, runner, revision, GPU UUID, and driver in its receipt.
The forced KV capacity covers the 29 prompt tokens, 128 output tokens, and one
32-token replay headroom block, rounded up to the 32-token paged-KV block size.

A local `ReleaseSafe` functional guard on the L4 completed 128 greedy tokens
at 42.61 decode tok/s after a 24-token chat-formatted prompt. It reported 60
resident sources (12,846,366,720 bytes), 150 route calls, 120 decode calls, 30
prefill calls, 124 graph replays, zero replay-capacity skips, and zero
cross-backend copies. The local artifact's fragmented on-disk layout made the
initial cold load take 1,745.7 seconds; generation itself took 3.36 seconds.
The CUDA loader now groups weights by backing mmap, sorts uploads by file
offset, and changes only the exact consumed ranges from random to sequential
advice. On the same cold artifact, dense upload fell from about 197 seconds to
13.6 seconds, the 12.25 GiB expert upload completed in 104.0 seconds, and total
model load fell to 122.4 seconds: a 14.3x cold-load speedup. This is not the
release throughput result because it used a different prompt and was not
paired with the pinned llama.cpp comparator.

An Nsight follow-up found that the A4B router projection was still taking the
generic single-thread-per-output F32 path. For the exact single-row
`2816 -> 128` router shape, that meant only 128 threads each serially walking
the full hidden dimension. The CUDA backend now selects the existing
256-thread tiled F32 reduction only when an A4B runtime is loaded and this
exact decode shape is present. `ANTFLY_INFERENCE_CUDA_DISABLE_A4B_ROUTER_TILED=1`
restores the scalar route for A/B and emergency rollback; prefill and other F32
projections are unchanged.

On the pinned L4 and exact GGUF, a paired 128-token raw-prompt A/B produced
identical token arrays and identical graph behavior (124 replays, 123
persistent replays, zero capacity skips). Scalar-router decode took 2,906 ms
and reported 44.05 tok/s; tiled-router decode took 2,033 ms and reported
62.96 tok/s, a **1.429x / 42.9%** improvement. Normalizing both measurements to
the 127 timed decode transitions gives 43.70 and 62.47 tok/s respectively. A
route-grouped MoE prototype was also measured but not retained: despite
reducing CTA and combine-launch counts, it was 1.3% slower because it reduced
the memory-level parallelism of the resident expert projections.

---

## 2026-08-24 Q4_0 high-memory Metal qualification

**Status**: correctness PASS; throughput improved substantially, but llama.cpp
parity is not yet reached.

**Hardware**: Mac mini (Mac16,11), Apple M4 Pro, 12 CPU cores, 24 GB unified
memory

**Model**: `gemma-4-26B_q4_0-it.gguf`, 14,439,363,584 bytes,
SHA-256 `3eca3b8f6d7baf218a7dd6bba5fb59a56ee25fe2d567b6f5f589b4f697eca51d`

**Build**: Zig 0.16.0, `ReleaseFast`, Metal enabled

**Comparator**: llama.cpp build 10342 at `38278078c`, built by Unsloth

The qualified lane is enabled by one umbrella flag:

```sh
TERMITE_METAL_ENABLE_A4B_HIGH_MEMORY_FAST_PATH=1 \
  ./zig-out/bin/antfly-inference generate "$MODEL" "$PROMPT" \
  --backend metal --mode compiled --compiled-target whole-model \
  --cache-dtype f16 --a4b-residency-mode resident \
  --a4b-memory-budget-mb 16384 --backend-budget-mb 16384 \
  --combined-budget-mb 16384 --raw-prompt --temperature 0 \
  --ignore-eos --max-tokens 128
```

The flag bundles only the measured wins: one no-copy model-wide Metal buffer
with a residency set, mapped routed-expert weights, fused expert gate/up
activation, cached adjusted norm weights, zero-bias elision, shared-FFN
fusion, SIMD-group RMSNorm/head-RoPE kernels, triple parallel-FFN pre-norm,
parallel-FFN post/residual fusion, RMSNorm/residual fusion, and the qualified
M4 Q4_0 NR4/NSG4 row-one schedule. Every bundled component has a
`TERMITE_METAL_DISABLE_A4B_*` rollback override. The umbrella remains off by
default.

### Paired deterministic decode result

All measurements use the same raw 29-token Gemma chat prompt, greedy decode,
F16 KV, ignored EOS, 128 generated tokens, and a fresh process per sample.
The throughput metric is decode-only and covers 127 timed decode steps after
the first generated token.

| Runtime | Samples (tok/s) | Median | ms/token | Relative to llama.cpp |
|---|---:|---:|---:|---:|
| Antfly branch baseline | 8.516867 | 8.516867 | 117.414 | 12.37% |
| Antfly high-memory lane | 39.481801, 39.530574, 39.433148 | **39.481801** | 25.328 | **57.33%** |
| llama.cpp | 69.21, 68.73, 68.87 | **68.87** | 14.520 | 100% |

This is a **4.636x** speedup over the paired Antfly baseline (+363.6%), while
the remaining median gap to llama.cpp is 29.39 tok/s.

The three Antfly runs emitted the same 128 token IDs. The SHA-256 of the
space-separated IDs without a trailing newline is
`d4ee583f092062e7177069de1f35a9cefbaac24848d11d09b64237bc9209b68e`.
The visible answer also matches llama.cpp:

> LSM trees are optimized for write-heavy workloads because they transform
> random writes into sequential I/O by buffering data in memory and flushing
> it to disk in sorted runs. This approach avoids the expensive, immediate
> disk seeks required by B-trees, significantly increasing overall write
> throughput.

The final run reported zero Metal frame and quant-kernel fallbacks, zero
mapped-weight fallbacks or failures, and the qualified Q4_0 NR4/NSG4 route for
all 18,415 row-one Q4_0 dispatches.

### Memory and bottleneck profile

The model-wide no-copy Metal allocation is 14,302,248,960 bytes. The complete
Metal residency set contains 10 allocations totaling 15,542,992,896 bytes
(14.48 GiB), rather than the old roughly 2 GiB compact envelope.

Five sampled decode frames on the final source average:

| Stage | Average per token |
|---|---:|
| GPU total | 21.296 ms |
| FFN | 14.678 ms |
| Attention | 6.545 ms |
| Tail | 0.068 ms |
| Host/submit gap to wall time | about 4.032 ms |

llama.cpp averages 14.520 ms/token end-to-end and reports 126 reused decode
graphs. More residency alone therefore cannot close the remaining gap. The
next high-impact work is persistent decode graph/command reuse plus faster
routed Q4_0 FFN and paged-GQA kernels. A direct fused expert-down/reduce
prototype preserved exact output but measured 0.28% slower and was removed;
future fusion work must beat the separate down and reduction kernels on GPU
time, not merely reduce dispatch count.

### Verification

- `zig build -Doptimize=ReleaseFast -Dmetal=true`: PASS
- Full inference suite with loopback sockets enabled: 3,151 passed, 19 skipped
- Gemma4 benchmark-contract suites: 17/17 and 43/43 PASS
- Final post-cleanup 128-token Metal guard: PASS, exact token hash above
- `git diff --check`: PASS

Raw timing JSON and llama.cpp logs are retained in
`/private/tmp/gemma4-a4b-parity-20260824/` for this machine-local run.

---

## Historical report: 2026-04-08 Q5_K_M lane

**Date**: 2026-04-08
**Commit**: dfaca28
**Model**: gemma-4-26B-A4B-it Q5_K_M GGUF
**Hardware**: Apple Silicon (M-series), 36GB unified memory
**Build**: ReleaseFast, Metal backend

## Current Numbers (30 tokens decode)

| Metric | Non-graph | Graph mode | Notes |
|--------|-----------|-----------|-------|
| Decode total | 232.7s | 242.5s | ~7.8s/tok vs ~8.1s/tok |
| Eval overhead | 96.3s (214ms × 450) | 3.4s (113ms × 30) | 96% reduction |
| FFN (tracked) | 101.8s | 4.7s | Graph only tracks tracing pass |
| Attention (tracked) | 2.0s | 1.7s | Same — tracing only |
| Shared expert FFN | 0.9s | 0.9s | Same |
| Unaccounted | 32.6s | 232.7s | Interpreter replay dominates |
| MoE grouped | 300ms (900 calls) | 39ms (30 calls) | Only during trace |

### Per-token breakdown (non-graph, 30 tokens)

```
Total per token:              ~7,760ms
  Eval sync barriers:        ~6,400ms  (82%)  ← 15 evals/token × 214ms
  FFN compute (MoE+dense):   ~3,400ms  (44%)  ← overlaps with eval
  Attention compute:          ~  67ms   ( 1%)
  MoE routing overhead:      ~   5ms   (<1%)
  Norm + other:               ~   3ms   (<1%)
```

The eval barrier (214ms) is where Metal actually executes the accumulated
lazy computation graph for 2 layers. The 214ms includes GPU matmul time
for all ops in those 2 layers.

### Per-token breakdown (graph mode, 29 replay tokens)

```
Total per token:              ~8,100ms
  Interpreter dispatch:       ~8,000ms  (99%)  ← 2011 ops dispatched
  Eval overhead:              ~  100ms  ( 1%)  ← single eval at end
```

Graph mode eliminates per-layer eval barriers but the interpreter
dispatches 2011 Metal lazy ops per token, each going through vtable →
Metal C API → lazy graph construction. The actual GPU work is identical.

## Why graph mode doesn't help (yet)

The interpreter replaces 15 eval barriers with 2011 vtable calls.
The eval barriers force GPU sync (214ms each = 3.2s/token), but the
vtable dispatch overhead is higher (~8s/token) because:

1. Each of 2011 ops goes through: Zig vtable → C function pointer →
   Metal C wrapper → Metal C++ lazy array construction
2. Metal builds its own internal lazy graph regardless
3. The single eval at the end materializes the same computation

Graph mode would help if the interpreter could emit Metal commands
directly (bypass Metal lazy evaluation) or batch operations.

## Bottleneck Analysis

### Where time actually goes (GPU)

For each of 30 layers per token:

| Operation | Count | Est. time |
|-----------|-------|-----------|
| Attention QKV linear (Q5_K) | 3 | ~33ms |
| Attention output proj (Q5_K) | 1 | ~18ms |
| Dense FFN gate+up+down (Q6_K) | 3 | ~30ms |
| MoE router projection | 1 | ~1ms |
| MoE expert gate+up+down × top_k=2 | 6 | ~10ms |
| RMSNorm × 5 | 5 | ~1ms |
| Activations, adds, rope | ~10 | ~1ms |
| **Layer total** | | **~94ms** |

30 layers × 94ms = 2.8s compute. But actual is ~7.8s/token because:
- Metal kernel dispatch overhead per operation
- Memory bandwidth for loading 26B of quantized weights from unified memory
- Eval sync barriers (GPU↔CPU round-trips)

### Weight memory bandwidth

At Q5_K (5.5 bits/param), 26B params = ~17.9 GB of weights.
Apple Silicon M-series memory bandwidth: ~200 GB/s (M2 Ultra) to ~100 GB/s (M3 Pro).
Theoretical minimum: 17.9 GB / 200 GB/s = ~90ms per token (bandwidth bound).
Actual: ~7.8s per token → **87x slower than bandwidth limit**.

The gap is from:
- **930 separate Metal kernel dispatches** per token (each has fixed overhead)
- **Eval barriers** forcing GPU↔CPU sync 15 times per token
- **Lazy evaluation overhead** in Metal's graph construction

## Optimization Opportunities

### High Impact

1. **Fuse operations in Metal kernels** (target: 2-4x speedup)
   - Fuse RMSNorm + linear into single kernel (eliminate intermediate tensor)
   - Fuse gate+up linear pair for dense FFN (already have Q5_K kernel, need Q6_K)
   - Fuse silu(gate) * up into the linear kernel
   - This reduces 930 kernel dispatches to ~200-300

2. **Increase eval stride / reduce eval count** (target: 30-50% eval savings)
   - Current: eval every 2 layers = 15 evals/token × 214ms = 3.2s
   - Try eval every 4-6 layers for decode (seq_len=1, memory is small)
   - Risk: larger lazy graphs might have diminishing returns

3. **Expert weight residency** (target: reduce weight loading latency)
   - Current: 16 of 128 experts resident, rest lazy-loaded from host memory
   - Gemma 4 uses top_k=2 from 128 experts → most accesses hit non-resident
   - Increase resident budget or implement LRU expert caching

### Medium Impact

4. **Continuous batching** (target: higher throughput, not lower latency)
   - Process multiple requests simultaneously
   - Share weight loading across batch → amortize memory bandwidth
   - Requires paged KV cache (already implemented)

5. **Speculative decoding** (target: 2-3x latency improvement)
   - Use a small draft model (e.g., Gemma 4 2B) to propose tokens
   - Verify in parallel with the 26B model
   - Already have infrastructure in generation.zig

6. **Graph interpreter → Metal command buffer** (target: eliminate dispatch overhead)
   - Instead of dispatching Metal lazy ops, emit Metal compute commands directly
   - Pre-compile the compute pipeline from the graph
   - This is the long-term path for graph mode to actually help

### Lower Impact

7. **Q4_K quantization** (target: ~20% faster, slight quality loss)
   - Reduces weight memory by ~20% (4.5 vs 5.5 bits/param)
   - Less memory bandwidth → faster decode
   - May need to requantize the model

8. **GPU softmax for MoE routing** (target: eliminate 1 sync per layer)
   - Currently downloads router logits to CPU for softmax
   - Move softmax to GPU, only download top_k indices

9. **Shared expert pair matmul** (target: minor FFN speedup)
   - Fuse gate+up for shared expert dense FFN
   - Blocked on Q6_K pair kernel (only Q5_K exists)

## Raw Timing Data

### Non-graph mode (30 tokens)

```
generate_timing_ms: prompt_format=0 tokenize=0 prefill=11818 decode=232729 total=244547
gpt_timing_ms: attention=1994 attn_norm=32 attn_qkv=1238 attn_core=111 attn_rope=46 attn_gqa=62 attn_out_proj=609 ffn=101832
gpt_moe_timing_ms: grouped_attempts=900 grouped_successes=900 moe_grouped=298
gpt_overhead_ms: eval=96259 eval_count=450 shared_expert_ffn=928 norm=59
metal_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

### Graph mode (30 tokens)

```
generate_timing_ms: prompt_format=0 tokenize=0 prefill=11657 decode=242462 total=254119
gpt_timing_ms: attention=1683 attn_norm=17 attn_qkv=1077 attn_core=5 attn_rope=1 attn_gqa=4 attn_out_proj=582 ffn=4659
gpt_moe_timing_ms: grouped_attempts=30 grouped_successes=30 moe_grouped=39
gpt_overhead_ms: eval=3392 eval_count=30 shared_expert_ffn=904 norm=28
metal_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

## Key Insight

The dominant cost is **Metal kernel dispatch overhead** for ~930 small
single-row matmuls per token across 30 layers. Each matmul is tiny
(1×dim matrix-vector multiply) but carries fixed kernel launch cost.
The path to 30 tok/s requires fusing these into fewer, larger kernels
or switching to a direct Metal compute pipeline that avoids per-op dispatch.
