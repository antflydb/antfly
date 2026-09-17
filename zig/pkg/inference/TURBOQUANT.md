# TurboQuant

TurboQuant is a compressed KV cache format for paged attention. It is not just
a smaller cache dtype: attention logits are computed directly from compressed
keys, without materializing full f32 key rows in the hot loop.

Primary references:

- Google Research blog: https://research.google/blog/turboquant-redefining-ai-efficiency-with-extreme-compression/
- TurboQuant paper: https://arxiv.org/abs/2504.19874
- QJL paper: https://arxiv.org/abs/2406.03482
- PolarQuant paper: https://arxiv.org/abs/2502.02617

## Goals

- Add a new experimental KV dtype for online compressed KV cache.
- Avoid per-block/per-head scale metadata in the primary compressed key format.
- Preserve the current paged KV cache API and block table model.
- Add a native compressed paged-attention path that scores queries against
  compressed keys without materializing full f32 key rows.
- Add backend-specific fast paths after the native reference path is correct.
- Keep f16, f32, fp8, int8, and int4 behavior unchanged.
- Measure quality, memory, and decode latency against the existing KV formats
  and post-prefill cache compaction.

## Non-Goals

- Do not replace GGUF or model-weight quantization.
- Do not make TurboQuant the default until it has model-level accuracy data.
- Do not require calibration data or fine-tuning.
- Do not remove the current `cache_compaction_ratio` path. TurboQuant composes
  with compaction rather than replacing it.

## Substrate

TurboQuant builds on the existing paged KV cache substrate:

| Area | File | Notes |
|------|--------------|-------|
| KV dtype and block storage | `src/runtime/kv/pool.zig` | Existing formats quantize on write and dequantize on read. |
| Sequence and block table management | `src/runtime/kv/manager.zig` | Sequence IDs and block tables remain the owner of paging. |
| Native paged attention | `src/ops/native_compute.zig` | `gqaPagedAttentionDirect` dispatches to a compressed-key path for TurboQuant dtypes. |
| WASM/WebGPU cached attention | `web/shaders/gqa_cached_attention.wgsl` | Dense K/V storage and f32 scoring for the non-compressed dtypes. |
| Paged attention benchmark | `src/bench/paged_attention.zig` | Dtype sweep including `polar4`/`turbo3`. |
| Post-prefill compaction | `src/runtime/kv/compaction.zig` | Token-count compression stacks with KV quantization. |
| User configuration | `src/pipelines/generation.zig`, `src/native_smoke.zig` | `--cache-dtype` / `cache_dtype` expose the experimental formats. |

## Format Family

Two experimental dtypes ship:

| DType | Purpose | Target bits | Kernel path |
|-------|---------|-------------|----------------------|
| `polar4` | PolarQuant-style key format with a safer value codec | about 4 bits/key value, V codec configurable | direct compressed-key logits |
| `turbo3` | PolarQuant primary key stage plus QJL residual | about 3 to 3.5 bits/key value, V codec configurable | direct compressed-key logits plus residual estimator |

`polar4` is the first, smallest complete step that proves the cache layout and
direct scoring path. `turbo3` layers QJL residual correction on top.

TurboQuant-style cache rows use asymmetric K/V storage because key scoring and
value accumulation have different kernel and quality requirements:

```text
cache dtype polar4 =
  K: polar4 direct-scored key codes
  V: existing int8-style per-head quantization, with f16 available as a debug/quality fallback

cache dtype turbo3 =
  K: polar primary key codes plus QJL residual sketch
  V: configurable value codec, initially the same V policy as polar4
```

This avoids forcing V through the same experimental format as K while the direct
key estimator is still being proven.

### Format Sketch

For each token, layer, and KV head:

1. Apply a deterministic random preconditioner to the head vector.
2. Encode the preconditioned vector with a fixed codebook or fixed angular
   quantizer that does not need per-row scale metadata.
3. Store compact codes in the paged KV block.
4. For `turbo3`, store an additional 1-bit QJL residual sketch for the key.
5. Store values with the configured V codec. v1 should use existing int8-style
   per-head quantization or f16, not the experimental key estimator format.
6. Decode values through a simple path first, then optimize value reads after
   key scoring is correct.

The rotation/preconditioner must be reproducible from pool metadata, not stored
as a dense matrix per pool. Prefer a sign-flipped Hadamard-style transform for
power-of-two head dimensions.

For v1, support only `head_dim=64` and `head_dim=128`. Unsupported dimensions
must fall back explicitly to an existing cache dtype rather than silently
padding. Padding can be added later once the direct kernel ABI and memory
sizing are stable.

## Architecture

An interim Apple-GPU acceleration path built on an "MLX provider" existed
during early bring-up but has since been removed from the codebase along with
the MLX backend; the Metal-native path that exists today
(`src/ops/metal_compute.zig`, `src/ops/metal/`) was built independently of it
and is the current target for a real, non-MLX Metal compressed-key kernel.

### Codec module

`src/runtime/kv/turboquant.zig` is a small codec module.

Responsibilities:

- Define `TurboQuantConfig`.
- Build fixed quantizer tables at comptime or process init.
- Encode f32 K rows into `polar4` and `turbo3` storage.
- Encode f32 V rows through the configured V codec.
- Provide scalar reference decode for tests and fallback paths.
- Provide direct key-dot estimators used by native paged attention.

`pool.zig` stays responsible for allocation and layout; math and bit packing
live in the codec module.

### `KvDType` extension

`src/runtime/kv/pool.zig` has `.polar4` and `.turbo3` variants:

- `bytesPerElement`, `bytesForTokenRow`, and `parseKvDType` cover both.
- Internal key/value sizing helpers:
  - `bytesForKeyRow`
  - `bytesForValueRow`
  - `bytesForTokenRow = bytesForKeyRow + bytesForValueRow`
- Dtype-specific block row layout helpers.
- Existing `readToken` behavior stays available as a slow f32 decode fallback.
- A compressed read API exists for kernel paths:

```zig
pub const KvEncodedRow = union(KvDType) {
    f32: struct { k: []const f32, v: []const f32 },
    f16: struct { k_bytes: []const u8, v_bytes: []const u8 },
    int8: struct { k_bytes: []const u8, v_bytes: []const u8 },
    int4: struct { k_bytes: []const u8, v_bytes: []const u8 },
    fp8: struct { k_bytes: []const u8, v_bytes: []const u8 },
    polar4: struct { k_codes: []const u8, v_encoded: EncodedValueRow },
    turbo3: struct { k_codes: []const u8, k_residual: []const u8, v_encoded: EncodedValueRow },
    bf16: void,
};

pub const EncodedValueRow = union(enum) {
    f16: []const u8,
    int8_per_head: []const u8,
};

pub fn readEncodedToken(...) !KvEncodedRow;
```

Not all callers go through this union: the new attention path uses it, while
existing gather/dequant users stay on `readToken`.

### Compressed paged-attention dispatch

`src/ops/native_compute.zig`'s `gqaPagedAttentionDirect` dispatches by
`pool.config.dtype` between an f32 row path and a compressed-key path.

The compressed path:

1. Iterate the same block table and causal/sliding-window mask as the f32 path.
2. Read encoded K row bytes with `readEncodedToken`.
3. Compute `score = estimator(q, encoded_k) * scale`.
4. Maintain the same online softmax recurrence.
5. Accumulate V through the simplest correct value path at first.

The value path decodes V to scratch f32 because logits are the
bandwidth-critical part for long contexts; direct compressed-V weighted
accumulation remains open work.

For `turbo3`'s residual estimator, query projections should be hoisted out of
the per-KV-token/per-chunk scoring loop and computed once per query/head
instead; this pattern applies across the native, compressed-attention-block,
span, and chunked-span paths and is a consistent, repeatable win each time it
is applied.

### Native kernel path

Implement the native path in stages:

| Stage | Kernel behavior | Expected outcome |
|-------|-----------------|------------------|
| Reference | Scalar compressed-key estimator, f32 V decode | Correctness, memory sizing, integration tests |
| SIMD | Vectorized estimator for `head_dim=64/128` | Decode latency win on CPU |
| Batched | Handle `kv_batch` without falling back per item where possible | Scheduler/microbatch compatibility |

The SIMD implementation should live next to the codec math or in
`src/runtime/kv/turboquant.zig`, not buried inside the attention loop.

### WebGPU kernel path

Dedicated shaders exist instead of overloading `gqa_cached_attention.wgsl`:

- `web/shaders/gqa_cached_attention_polar4.wgsl`
- `web/shaders/gqa_cached_attention_turbo3.wgsl`

Along with:

- `web/webgpu-ops.js`
- `web/inference-worker.js`
- `src/ops/wasm_extern.zig`
- `src/ops/wasm_compute.zig`

Shader requirements:

- Inputs are Q plus encoded K/V buffers.
- Workgroup softmax structure should match the existing cached attention shader.
- Dot-product scoring must run against encoded keys.
- `MAX_KV` limits and workgroup memory use must be re-evaluated because encoded
  K reduces storage bandwidth but may add estimator math.

### Compaction composition

Compaction changes token count; TurboQuant changes bytes per token and scoring
bandwidth. They are independent knobs, and `cache_compaction_ratio` composes
with a TurboQuant dtype without special-casing. Benchmarking
`cache_compaction_ratio + polar4` against `cache_compaction_ratio + int8`
remains open work (see Open work).

## Delivery History

### Design lock

- Decide exact names: `polar4`, `turbo3`.
- Define byte layout for asymmetric K and V rows.
- Define v1 supported head dimensions as `64` and `128`.
- Define explicit fallback behavior for unsupported dimensions.
- Add a short RFC note to this document with final storage formulas.

Exit criteria:

- `bytesForKeyRow`, `bytesForValueRow`, and `KvDType.bytesForTokenRow` can be
  implemented without guessing.
- The kernel ABI is clear for native and WebGPU.

### Codec and storage

- Add `src/runtime/kv/turboquant.zig`.
- Add `.polar4` to `KvDType`.
- Implement key encode/decode round-trip tests.
- Wire `polar4` to an existing V codec, with int8-style per-head V as the
  preferred default and f16 as a debug/quality fallback.
- Add memory sizing tests for common GQA shapes:
  - `num_kv_heads=8, head_dim=128`
  - `num_kv_heads=4, head_dim=128`
  - `num_kv_heads=8, head_dim=64`
- Add unsupported-shape tests for non-64/128 head dimensions.
- Keep `readToken` fallback working.

Exit criteria:

- `zig test` coverage proves storage, sizing, and f32 fallback decode.
- `--cache-dtype polar4` parses but may still dispatch through fallback decode.

### Native direct-key paged attention

- Add `readEncodedToken`.
- Split native paged attention into f32 and compressed-key paths.
- Implement scalar direct-key estimator.
- Preserve online softmax and masking behavior exactly.
- Add tests comparing output to the decode-fallback path.

Exit criteria:

- `polar4` decode outputs match fallback within a documented tolerance.
- Attention tests cover causal mask, sliding window, GQA head grouping, and
  paged block boundaries.

### Native SIMD kernel

- Add vectorized scoring for supported head dimensions.
- Add benchmark knobs for cache dtype:
  - `src/bench/paged_attention.zig --cache-dtype f16|int8|int4|polar4`
- Report:
  - bytes per token row
  - prompt prefill time
  - decode time per token
  - direct compressed scoring time
  - fallback decode scoring time

Exit criteria:

- `polar4` is faster than `int4` fallback decode on long-context decode for at
  least one representative native benchmark.
- No regression for existing dtypes.

### QJL residual and `turbo3`

- Add QJL sketch generation for key residuals.
- Add direct residual estimator to the compressed-key scoring path.
- Add `turbo3` dtype parsing, sizing, and tests.
- Compare `polar4` versus `turbo3` on dot-product distortion and model outputs.

Exit criteria:

- `turbo3` has better attention-logit distortion than `polar4` at lower or
  comparable memory.
- Quality is good enough to keep the dtype exposed as experimental.

### WebGPU kernel

- Add `gqa_cached_attention_polar4.wgsl`.
- Wire WebGPU imports and externs.
- Add WASM tests that compare dense cached attention to compressed cached
  attention on deterministic tensors.
- Add canvas/browser smoke coverage only if the path is exposed in the web demo.

Exit criteria:

- WebGPU compressed path runs without falling back for `polar4`.
- Shader output matches native reference within tolerance.

### Metal kernel

- Add a Metal compressed-key scoring kernel.
- Wire dispatch behind dtype and shape checks.
- Keep unsupported shapes on current f16/f32 behavior.

Exit criteria:

- The Metal path can run a real decode loop with `polar4`.
- Per-token decode latency and memory are reported against f16 and int8.

### End-to-end quality gates (open)

Model-level checks before considering either dtype a default remain open work:
short deterministic generation parity, long-context retrieval prompts,
rerank/generation smoke tests, at least one Gemma-family and one
Mistral/Qwen-style GQA model, compared against f16/int8/int4 cache,
compaction+int8, compaction+`polar4`, and `turbo3`. See Open work.

## Validation Matrix

| Layer | Tests |
|-------|-------|
| Codec | Round-trip, bit layout, deterministic preconditioner, unsupported shape fallback |
| Pool | Row sizing, block allocation, `readToken`, `readEncodedToken`, gather/scatter |
| Attention | Direct compressed scoring versus fallback decode, masks, GQA grouping, page boundaries |
| Benchmark | Native dtype sweep, long-context decode, compaction composition |
| WebGPU | Shader reference comparison, dtype dispatch, unsupported fallback |
| E2E | Numeric kernel gates, short deterministic token checks, long-context retrieval, model-family tolerance table |

## Risks

- The paper's H100 speedup numbers may not transfer to CPU or WebGPU
  without specialized kernels.
- A metadata-free quantizer is only useful if direct scoring avoids f32
  materialization in the hot loop.
- QJL residual correction improves logit estimation but adds code complexity and
  kernel ABI surface.
- Gemma-family cache behavior is already conservative in termite; enabling
  compressed KV by default there is higher risk.
- Existing post-prefill compaction may dominate memory wins for some workloads,
  so benchmarks must measure stacked and unstacked configurations.

## Design Decisions

- Use asymmetric K/V storage internally. Public dtype names such as `polar4` and
  `turbo3` are presets, not proof that K and V use the same codec.
- For `polar4` v1, use direct-scored `polar4` keys and an existing V codec.
  Prefer int8-style per-head V quantization, with f16 available for quality and
  debugging.
- Support `head_dim=64` and `head_dim=128` in v1. Unsupported head dimensions
  must fall back explicitly to an existing cache dtype.
- `turbo3` targets the practical 3 to 3.5 bits/channel quality-neutral range
  from the paper family, not a universal exact 3-bit promise.
- Acceptance uses a combination of numeric kernel gates, short deterministic
  token checks, long-context task quality, memory, and latency. Token parity is
  a smoke signal, not the global acceptance criterion.

> **Relocated:** The chronological `polar4`/`turbo3` implementation log that
> previously lived here (261 lines, including the retired MLX-era
> acceleration path) is preserved verbatim in
> [work-log/completed/inference/turboquant-history.md](../../../work-log/completed/inference/turboquant-history.md).
> Durable decisions from it are in Architecture and Open work above.

## Open work

- The raw whole-token Metal decode path (the backend-owned MLX-era bring-up in
  `mlx_quant_metal.m`) is **not numerically correct**: it emitted `the!!!`
  where the default MLX greedy decode path emitted `the the the the` for the
  same local command on GPT-2. Extending it to more layers does not address
  this; the blocker is correctness of the raw whole-token math/path itself,
  not coverage.
- Batched (`kv_batch > 1`) native compressed-key scoring without falling back
  per item is not confirmed shipped.
- Benchmarking `cache_compaction_ratio + polar4` against
  `cache_compaction_ratio + int8` remains open.
- Model-level end-to-end quality gates (accuracy, long-context retrieval, and
  latency comparisons across model families) have not been run against real
  model weights. Default dtype recommendations should stay conservative until
  that data exists.
- A real, non-MLX Metal compressed-key kernel path (see the Architecture note
  above on the removed MLX provider) should be re-verified against
  `src/ops/metal_compute.zig` / `src/ops/metal/` rather than assumed from the
  MLX-era log entries.
