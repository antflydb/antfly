# Gemma 4 26B-A4B Performance Analysis

**Date**: 2026-04-08
**Commit**: dfaca28
**Model**: gemma-4-26B-A4B-it Q5_K_M GGUF
**Hardware**: Apple Silicon (M-series), 36GB unified memory
**Build**: ReleaseFast, MLX backend

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
dispatches 2011 MLX lazy ops per token, each going through vtable →
MLX C API → lazy graph construction. The actual GPU work is identical.

## Why graph mode doesn't help (yet)

The interpreter replaces 15 eval barriers with 2011 vtable calls.
The eval barriers force GPU sync (214ms each = 3.2s/token), but the
vtable dispatch overhead is higher (~8s/token) because:

1. Each of 2011 ops goes through: Zig vtable → C function pointer →
   MLX C wrapper → MLX C++ lazy array construction
2. MLX builds its own internal lazy graph regardless
3. The single eval at the end materializes the same computation

Graph mode would help if the interpreter could emit Metal commands
directly (bypass MLX lazy evaluation) or batch operations.

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
- **Lazy evaluation overhead** in MLX's graph construction

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
   - Instead of dispatching MLX lazy ops, emit Metal compute commands directly
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
mlx_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

### Graph mode (30 tokens)

```
generate_timing_ms: prompt_format=0 tokenize=0 prefill=11657 decode=242462 total=254119
gpt_timing_ms: attention=1683 attn_norm=17 attn_qkv=1077 attn_core=5 attn_rope=1 attn_gqa=4 attn_out_proj=582 ffn=4659
gpt_moe_timing_ms: grouped_attempts=30 grouped_successes=30 moe_grouped=39
gpt_overhead_ms: eval=3392 eval_count=30 shared_expert_ffn=904 norm=28
mlx_quant_counts: provider_calls=6180 provider_grouped_calls=2700 device_native_moe_grouped_calls=2700
```

## Key Insight

The dominant cost is **Metal kernel dispatch overhead** for ~930 small
single-row matmuls per token across 30 layers. Each matmul is tiny
(1×dim matrix-vector multiply) but carries fixed kernel launch cost.
The path to 30 tok/s requires fusing these into fewer, larger kernels
or switching to a direct Metal compute pipeline that avoids per-op dispatch.
