# KV Cache

The KV cache stores key and value tensors from the attention layers during inference. Each generated token reuses all previous K/V pairs, so the cache grows linearly with sequence length and becomes the dominant memory consumer for long contexts.

Termite-zig's KV cache lives in `src/runtime/kv/` and provides paged block storage, multiple quantization formats, and post-prefill compaction.

## Architecture

```
                                      ┌─────────────────────────────┐
                                      │     KvManager (manager.zig) │
                                      │  Sequences, block tables,   │
                                      │  sliding window trimming    │
                                      └─────────┬───────────────────┘
                                                │
                                      ┌─────────▼───────────────────┐
                                      │      KvPool (pool.zig)      │
                                      │  Paged block storage,       │
                                      │  quantize on write,         │
                                      │  dequantize on read         │
                                      └─────────┬───────────────────┘
                                                │
                          ┌─────────────────────┼─────────────────────┐
                          │                     │                     │
                  ┌───────▼──────┐    ┌─────────▼────────┐   ┌───────▼──────┐
                  │   Block 0    │    │     Block 1       │   │   Block N    │
                  │ [layers ×    │    │                   │   │              │
                  │  page_size × │    │       ...         │   │     ...      │
                  │  token_row]  │    │                   │   │              │
                  └──────────────┘    └──────────────────-┘   └──────────────┘
```

**KvPool** (`pool.zig`) allocates fixed-size blocks. Each block holds `page_size` tokens across all packed layers. Tokens are written as f32 and automatically quantized to the pool's dtype; reads dequantize back to f32. The caller never sees quantized data.

**KvManager** (`manager.zig`) maps sequences to block tables (ordered lists of block IDs). It handles sequence lifecycle (attach/release), token append, sliding window trimming, and layer-level gather/scatter for bulk operations like compaction.

## Graph attention contract

KV cache dtype is part of the backend attention contract, not a separate
model-specific Metal branch. Graph-level SDPA remains the semantic attention
operation, while tensor descriptors and `OperatorPlan.attention` describe how
K/V is stored and which backend operator should execute it.

The lowering rule is:

| Logical op | Descriptor metadata | Planned operator |
|------------|---------------------|------------------|
| dense f32 `fused_sdpa` | `kv_format=f32`, `storage=dense` | `attention_flash` |
| paged f32 KV | `kv_format=f32`, `storage=paged` | `attention_paged` |
| Polar4 KV | `kv_format=polar4` | `attention_quantized_kv` |
| Turbo3 KV | `kv_format=turbo3` | `attention_quantized_kv` |

For LLM decode and prefill, the frontend should pass a normal attention
contract plus backend-owned KV metadata: sequence id, layer id, logical
position, KV length, page table, slot/layout, and dtype. The graph/partition
planner attaches the attention operator plan; the backend executor consumes
that plan and selects the correct device kernel. The dense f32 SDPA path should
not grow Polar4/Turbo3-specific branches; compressed KV support belongs behind
`attention_quantized_kv`, using the same graph attention op and descriptor
surface.

This keeps Polar4 and Turbo3 support aligned with the rest of the LLM
architecture: Q is dense/device-resident, K/V live in backend-owned compressed
KV storage, and the attention kernel owns score calculation, mask/softmax, and
PV accumulation over that storage.

### Current descriptor threading

The single-device graph executor now seeds attention K/V tensor descriptors from
the per-call `AttentionContext` before graph partitioning. That means capability
planning can see the actual runtime KV pool dtype and choose the planned
attention operator without model/provider-specific branches:

| Runtime KV dtype | Descriptor format | Descriptor storage | Planned operator |
|------------------|-------------------|--------------------|------------------|
| `f32` | `f32` | `paged` | `attention_paged` |
| `polar4` | `polar4` | `paged` | `attention_quantized_kv` |
| `turbo3` | `turbo3` | `paged` | `attention_quantized_kv` |
| other compressed dtype | `quantized` | `paged` | `attention_paged` or backend fallback until a real kernel exists |

This seeding happens in `src/graph/execution.zig` via
`partition.seedAttentionKvDescriptorsFromContext(...)`. The partition helper
reads the KV dtype from `KvStorageRuntime`, the `KvCacheView` storage handle, or
the `KvManager` pool for both single-sequence and batched KV views.

Cached `Runtime.initSingleDevicePlan` is not fully KV-dtype-aware yet because it
builds the partition plan before per-call `ExecuteOptions.attention` exists.
There are two correct long-term options:

1. Replan or specialize cached runtime plans by attention/KV descriptor metadata.
2. Carry static graph semantic metadata for stateful K/V inputs so the cached
   plan can be built with the same descriptor facts.

Until one of those lands, the per-call graph executor path has the complete
runtime-context threading, while cached runtime plans must not be assumed to
auto-select Polar4/Turbo3 attention kernels from the live KV pool.

## Quantization Formats

All formats are configured via `KvDType` in `pool.zig`. The default is auto-selected per backend; users can override with `--cache-dtype` (CLI) or `cache_dtype` (API).

| Format | Bytes per value | Compression vs f16 | Error tolerance | Best for |
|--------|----------------|---------------------|-----------------|----------|
| f32    | 4.0            | 0.5x (expansion)    | exact           | debugging |
| f16    | 2.0            | 1x (baseline)       | ~1e-3           | default  |
| fp8    | 1.0            | 2x                  | ~0.1            | memory-constrained, tolerant models |
| int8   | ~1.03*         | ~1.9x               | ~0.02           | best quality/compression tradeoff |
| int4   | ~0.56*         | ~3.5x               | ~0.15           | maximum compression |
| polar4 | K: 0.5, V: ~1.03* | experimental       | TBD             | direct compressed-key attention experiments |
| turbo3 | K: ~0.375 + residual, V: ~1.03* | experimental       | TBD             | lower-memory direct compressed-key attention experiments |

\* int8 and int4 store per-head/per-group scale metadata, so effective bytes per value exceed the raw bit width.

### int8: Symmetric per-head quantization

Each KV head is quantized independently. Per head: compute `scale = max(|values|) / 127`, store the f32 scale, then store each value as `round(value / scale)` in i8.

Storage layout per head: `[scale:f32][q0:i8][q1:i8]...[qN:i8]`

Total bytes per token row: `num_kv_heads * (head_dim + 4)`

SIMD: quantize and dequantize use `@Vector(8, f32)` for vectorized max-abs reduction, scale-multiply, and clamp.

### fp8: E4M3 format

IEEE-like 8-bit float with 1 sign bit, 4 exponent bits (bias 7), 3 mantissa bits. Range is approximately ±448 with no infinity or NaN representation. Values beyond ±448 are clamped.

Storage layout: one byte per value, no metadata.

SIMD: dequantization uses a comptime-generated 256-entry lookup table (`fp8_to_f32_table`) for branchless conversion. The 1KB table stays hot in L1 cache.

### int4: Symmetric group quantization

Values are quantized in groups of 32. Per group: compute `scale = max(|values|) / 7`, store the f16 scale, then pack pairs of 4-bit signed integers into bytes (low nibble first).

Storage layout per group of 32 values: `[scale:f16][packed:16 bytes]` = 18 bytes

Total bytes per token row: `ceil(num_kv_heads * head_dim / 32) * 18`

SIMD: quantize uses `@Vector(8, f32)` for max-abs reduction. Dequantize processes 4 packed bytes at a time, producing 8 f32 values via vector multiply.

### polar4: Experimental asymmetric TurboQuant-style cache

`polar4` is the first implementation slice from `TURBOQUANT.md`. It uses
4-bit packed key codes and stores values with the existing int8 per-head value
codec. The native paged-attention path scores queries directly against the
encoded key bytes, then decodes only V rows for the weighted value accumulation.

The initial codec supports `head_dim=64` and `head_dim=128`. Other head
dimensions are rejected so callers can fall back to a stable dtype.

### turbo3: Experimental 3-bit key cache

`turbo3` is the lower-bit shared codec path for the TurboQuant work. It uses
3-bit packed key codes, a deterministic one-bit QJL-style residual sketch, and
the same int8 per-head value codec as `polar4`. Native, WebGPU, and Metal
paged-attention paths score queries directly against the encoded 3-bit key bytes
plus a calibrated residual estimate, then decode only V rows for accumulation.

The residual sketch uses 32 one-bit projections per KV head. For example,
`num_kv_heads=8, head_dim=128` stores 384 base key bytes plus 32 residual bytes
per token. The initial codec supports `head_dim=64` and `head_dim=128`; other
head dimensions are rejected explicitly.

Use the distortion benchmark to calibrate that correction before enabling it in
the hot path:

```bash
zig build bench-turboquant-distortion -Dshared-lib-root=../.. -- --samples 4096 --head-dim 128
```

## Compaction

For long-context inference, even quantized KV caches can exceed memory budgets. Compaction reduces the token count itself using Attention Matching (Zweiger et al., arXiv 2602.16284).

The idea: after prefill, select a subset of M keys that capture the most attention mass, then fit new values via OLS (ordinary least squares) so that `softmax(Q * K_hat^T) * V_hat ≈ softmax(Q * K^T) * V`. This preserves the model's attention behavior while dramatically reducing the sequence length stored in cache.

Compaction runs once, after prefill and before the decode loop. Compacted sequences skip sliding window trimming since compaction replaces eviction.

### Algorithm

Per KV-head, for a sequence of N tokens compressed to M = ceil(N * target_ratio):

1. **Score keys by attention mass**: Compute `S = softmax(Q_ref * K^T / sqrt(d))` using reference queries sampled from layer 0's K values. Column-sum S to get per-key attention mass.
2. **Select top-M keys**: Quickselect partition to find the M keys with highest attention mass. Copy their K rows to `K_hat`.
3. **Compute target output**: `Y = S * V` — what the full attention would produce.
4. **Compute compressed weights**: `A_hat = softmax(Q_ref * K_hat^T / sqrt(d))` — attention with only the retained keys.
5. **OLS solve**: Solve `(A_hat^T * A_hat) * V_hat = A_hat^T * Y` via Cholesky decomposition. The resulting `V_hat` are fitted values that minimize the approximation error.

The linear algebra (`linalg.zig`) uses BLAS for matrix multiplies (Accelerate/OpenBLAS) and SIMD-vectorized Cholesky forward/back substitution.

### Compression stacking

Compaction and quantization compose multiplicatively:

| Configuration | Effective compression vs f16 |
|---------------|------------------------------|
| compaction 0.1 (10x) + f16 | 10x |
| compaction 0.1 + int8 | ~19x |
| compaction 0.02 (50x) + int8 | ~95x |
| compaction 0.1 + int4 | ~35x |

### Configuration

```
# CLI
termite generate model/ "prompt" --cache-compaction-ratio 0.1
termite generate model/ "prompt" --cache-compaction-ratio 0.1 --cache-dtype int8
zig build bench-paged-attention -- --cache-dtype-sweep --head-dim 128

# API (GenerateRequest)
{
  "cache_compaction_ratio": 0.1,
  "cache_dtype": "int8"
}
```

`target_ratio` controls how aggressively to compress: 0.1 retains 10% of tokens (10x), 0.02 retains 2% (50x). The default chunk size is 512 tokens and 64 reference queries.

## Memory estimation

`src/runtime/tier/memory.zig` computes KV cache memory budgets using `KvDType.bytesForTokenRow()`, which accounts for per-format metadata overhead. This is used by the scheduler to determine how many tokens fit in the available memory budget.

## Files

| File | Role |
|------|------|
| `src/runtime/kv/pool.zig` | Block storage, quantize/dequantize routines, `KvDType` enum |
| `src/runtime/kv/manager.zig` | Sequence management, block tables, sliding window, gather/scatter |
| `src/runtime/kv/compaction.zig` | Attention Matching compaction algorithm |
| `src/runtime/kv/linalg.zig` | Cholesky solver, matrix helpers for OLS |
| `src/runtime/kv/block.zig` | Block/pool ID types, block table structures |
| `src/runtime/kv/block_table.zig` | Sequence-to-block mapping |
| `src/runtime/tier/memory.zig` | Memory budget estimation using dtype-aware calculations |
| `src/pipelines/generation.zig` | Integration point: `compactKvCache()` on `NativeDecodeState` |
