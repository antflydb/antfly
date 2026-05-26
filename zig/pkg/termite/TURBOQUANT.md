# TurboQuant Plan

This document tracks the plan for adding a TurboQuant-inspired KV cache path to
termite. The target is not just a smaller cache format. The target is a full
paged-attention implementation that writes compressed K/V rows and computes
attention logits directly from compressed keys.

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
- Do not remove the current `cache_compaction_ratio` path. TurboQuant should
  compose with compaction once the base cache path is stable.

## Current State

Termite already has the substrate needed for this work:

| Area | Current file | Notes |
|------|--------------|-------|
| KV dtype and block storage | `src/runtime/kv/pool.zig` | Existing formats quantize on write and dequantize on read. |
| Sequence and block table management | `src/runtime/kv/manager.zig` | Existing sequence IDs and block tables should remain the owner of paging. |
| Native paged attention | `src/ops/native_compute.zig` | `gqaPagedAttentionDirect` currently calls `pool.readToken`, then dots f32 K rows. |
| WASM/WebGPU cached attention | `web/shaders/gqa_cached_attention.wgsl` | Dense K/V storage and f32 scoring today. |
| Paged attention benchmark | `src/bench/paged_attention.zig` | Useful starting point, but needs dtype and correctness extensions. |
| Post-prefill compaction | `src/runtime/kv/compaction.zig` | Token-count compression can stack with KV quantization. |
| User configuration | `src/pipelines/generation.zig`, `src/native_smoke.zig` | Existing `--cache-dtype` / `cache_dtype` knobs can expose experimental formats. |

## Proposed Format Family

Start with two experimental dtypes:

| DType | Purpose | Target bits | Required kernel path |
|-------|---------|-------------|----------------------|
| `polar4` | Practical first PolarQuant-style key format with a safer value codec | about 4 bits/key value, V codec configurable | direct compressed-key logits |
| `turbo3` | PolarQuant primary key stage plus QJL residual | about 3 to 3.5 bits/key value, V codec configurable | direct compressed-key logits plus residual estimator |

`polar4` should land first because it is the smallest complete step that proves
the cache layout and direct scoring path. `turbo3` should layer QJL residual
correction on top once `polar4` has correctness and benchmark coverage.

The public dtype name is a preset. Internally, TurboQuant-style cache rows should
support asymmetric K/V storage because key scoring and value accumulation have
different kernel and quality requirements:

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

### 1. Split Codec From Kernel Access

Add a small codec module:

- `src/runtime/kv/turboquant.zig`

Responsibilities:

- Define `TurboQuantConfig`.
- Build fixed quantizer tables at comptime or process init.
- Encode f32 K rows into `polar4` and `turbo3` storage.
- Encode f32 V rows through the configured V codec.
- Provide scalar reference decode for tests and fallback paths.
- Provide direct key-dot estimators used by native paged attention.

Keep `pool.zig` responsible for allocation and layout. Put math and bit packing
in the codec module.

### 2. Extend `KvDType`

In `src/runtime/kv/pool.zig`:

- Add `.polar4` and `.turbo3`.
- Extend `bytesPerElement`, `bytesForTokenRow`, and `parseKvDType`.
- Add internal key/value sizing helpers:
  - `bytesForKeyRow`
  - `bytesForValueRow`
  - `bytesForTokenRow = bytesForKeyRow + bytesForValueRow`
- Add dtype-specific block row layout helpers.
- Keep existing `readToken` behavior by implementing a slow f32 decode fallback.
- Add a new compressed read API for kernel paths:

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

Do not force all callers through this union immediately. Add it for the new
attention path and leave existing gather/dequant users on `readToken`.

### 3. Add A Compressed Paged-Attention Dispatch

In `src/ops/native_compute.zig`, split `gqaPagedAttentionDirect` into:

- `gqaPagedAttentionDirectF32Rows`
- `gqaPagedAttentionDirectCompressedKeys`

Dispatch by `pool.config.dtype`.

The compressed path should:

1. Iterate the same block table and causal/sliding-window mask as the f32 path.
2. Read encoded K row bytes with `readEncodedToken`.
3. Compute `score = estimator(q, encoded_k) * scale`.
4. Maintain the same online softmax recurrence.
5. Accumulate V through the simplest correct value path at first.

Initial value path should decode V to scratch f32 because logits are the
bandwidth-critical part for long contexts. Later, add direct compressed-V
weighted accumulation.

### 4. Native Kernel Path

Implement the native path in stages:

| Stage | Kernel behavior | Expected outcome |
|-------|-----------------|------------------|
| Reference | Scalar compressed-key estimator, f32 V decode | Correctness, memory sizing, integration tests |
| SIMD | Vectorized estimator for `head_dim=64/128` | Decode latency win on CPU |
| Batched | Handle `kv_batch` without falling back per item where possible | Scheduler/microbatch compatibility |

The SIMD implementation should live next to the codec math or in
`src/runtime/kv/turboquant.zig`, not buried inside the attention loop.

### 5. WebGPU Kernel Path

Add a dedicated shader instead of overloading `gqa_cached_attention.wgsl`:

- `web/shaders/gqa_cached_attention_polar4.wgsl`
- later: `web/shaders/gqa_cached_attention_turbo3.wgsl`

Update:

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

### 6. Metal Path

Once native and WebGPU have a stable ABI, add Metal support:

- `src/backends/metal_native_provider.zig`
- `src/backends/metal_kernels.m`
- `src/ops/metal_compute.zig`

Metal should initially be allowed to fall back to f16/f32 cache dtype for
unsupported model families. Do not route Gemma through `turbo3` by default until
model-level quality is measured, since Gemma currently has special KV dtype
selection behavior.

### 7. Compaction Composition

After the base compressed cache works:

- Run post-prefill attention matching compaction into a compressed pool.
- Add tests for compacted sequence decode with `polar4`.
- Benchmark `cache_compaction_ratio + polar4` against current
  `cache_compaction_ratio + int8`.

Compaction changes token count. TurboQuant changes bytes per token and scoring
bandwidth. They should remain independent knobs.

## Implementation Phases

### Phase 0: Design Lock

- Decide exact names: `polar4`, `turbo3`.
- Define byte layout for asymmetric K and V rows.
- Define v1 supported head dimensions as `64` and `128`.
- Define explicit fallback behavior for unsupported dimensions.
- Add a short RFC note to this document with final storage formulas.

Exit criteria:

- `bytesForKeyRow`, `bytesForValueRow`, and `KvDType.bytesForTokenRow` can be
  implemented without guessing.
- The kernel ABI is clear for native and WebGPU.

### Phase 1: Codec And Storage

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

### Phase 2: Native Direct-Key Paged Attention

- Add `readEncodedToken`.
- Split native paged attention into f32 and compressed-key paths.
- Implement scalar direct-key estimator.
- Preserve online softmax and masking behavior exactly.
- Add tests comparing output to the decode-fallback path.

Exit criteria:

- `polar4` decode outputs match fallback within a documented tolerance.
- Attention tests cover causal mask, sliding window, GQA head grouping, and
  paged block boundaries.

### Phase 3: Native SIMD Kernel

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

### Phase 4: QJL Residual And `turbo3`

- Add QJL sketch generation for key residuals.
- Add direct residual estimator to the compressed-key scoring path.
- Add `turbo3` dtype parsing, sizing, and tests.
- Compare `polar4` versus `turbo3` on dot-product distortion and model outputs.

Exit criteria:

- `turbo3` has better attention-logit distortion than `polar4` at lower or
  comparable memory.
- Quality is good enough to keep the dtype exposed as experimental.

### Phase 5: WebGPU Kernel

- Add `gqa_cached_attention_polar4.wgsl`.
- Wire WebGPU imports and externs.
- Add WASM tests that compare dense cached attention to compressed cached
  attention on deterministic tensors.
- Add canvas/browser smoke coverage only if the path is exposed in the web demo.

Exit criteria:

- WebGPU compressed path runs without falling back for `polar4`.
- Shader output matches native reference within tolerance.

### Phase 6: Metal Kernel

- Add a Metal compressed-key scoring kernel.
- Wire Metal dispatch behind dtype and shape checks.
- Keep unsupported shapes on current f16/f32 behavior.

Exit criteria:

- Metal path can run a real decode loop with `polar4`.
- Per-token decode latency and memory are reported against f16 and int8.

### Phase 7: End-To-End Quality Gates

Run model-level checks before considering defaults:

- Short deterministic generation parity for known prompts.
- Long-context retrieval prompts.
- Rerank/generation smoke tests where applicable.
- At least one Gemma-family model and one Mistral/Qwen-style GQA model.
- Compare against:
  - f16 cache
  - int8 cache
  - int4 cache
  - compaction plus int8
  - compaction plus `polar4`
  - `turbo3`

Exit criteria:

- Kernel-level tests pass numeric tolerances against f32/fallback decode.
- Short temperature-0 prompts are mostly token-identical, but global token
  parity is not required.
- Long-context retrieval has no material drop versus `int8`.
- Long-context decode latency beats current `int4` fallback-decode behavior for
  at least one representative native benchmark.
- Quality deltas are documented by model family.
- Default dtype recommendations remain conservative.

## Validation Matrix

| Layer | Tests |
|-------|-------|
| Codec | Round-trip, bit layout, deterministic preconditioner, unsupported shape fallback |
| Pool | Row sizing, block allocation, `readToken`, `readEncodedToken`, gather/scatter |
| Attention | Direct compressed scoring versus fallback decode, masks, GQA grouping, page boundaries |
| Benchmark | Native dtype sweep, long-context decode, compaction composition |
| WebGPU | Shader reference comparison, dtype dispatch, unsupported fallback |
| Metal | Real decode loop, dtype dispatch, unsupported fallback |
| E2E | Numeric kernel gates, short deterministic token checks, long-context retrieval, model-family tolerance table |

## Risks

- The paper's H100 speedup numbers may not transfer to CPU, WebGPU, or Metal
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

## First Patch Set

The first code patch should be deliberately small:

1. Add `src/runtime/kv/turboquant.zig` with a scalar `polar4` codec and tests.
2. Add `.polar4` to `KvDType` with asymmetric key/value byte sizing and parse
   support.
3. Make `pool.writeToken` and `pool.readToken` support `polar4` keys plus the
   selected V codec through fallback decode.
4. Add explicit fallback tests for unsupported `head_dim` values.
5. Add `--cache-dtype polar4` smoke coverage where existing dtype parsing is
   tested.

Only after that lands should the native direct-key paged-attention path start.

Status:

- Done: `polar4` codec module with supported-shape checks, packing, fallback
  decode, and direct decoded-code dot product.
- Done: `KvDType.polar4` parsing, asymmetric K/V row sizing, pool allocation,
  write/read fallback, encoded-key reads, and V-only decode reads.
- Done: native paged attention dispatches `polar4` to a direct encoded-key
  scoring path while decoding only V for accumulation.
- Done: CLI usage and `KVCACHE.md` mention `polar4`.
- Done: paged-attention benchmark accepts `--cache-dtype polar4` and reports
  asymmetric K/V row bytes plus total per-token-pair bytes.
- Done: native coverage compares the `polar4` direct-key paged-attention path
  against the decode-fallback reference across GQA grouping and page boundaries.
- Done: first native packed-vector `polar4` dot helper for `head_dim=64` and
  `head_dim=128`, with scalar-reference coverage.
- Done: paged-attention benchmark dtype sweep mode reports f32, f16, int8, fp8,
  int4, and `polar4` side by side, and marks unsupported `polar4` head dims
  explicitly.
- Done: WebGPU cached-attention ABI has a format-aware entrypoint with key/value
  format enums, row-byte fields, and an auxiliary key buffer slot for `turbo3`;
  `f32` and `polar4` route to separate shaders behind that ABI.
- Done: WASM GPU KV cache creation/upload can allocate `polar4` packed key
  buffers with f32 values, encode K rows on upload, and expose `cacheDtype:
  "polar4"` through the GPT web cache wrapper.
- Done: standalone WebGPU browser numeric harness compares the `polar4`
  cached-attention shader against a JS packed-key reference on deterministic
  tensors.
- Done: Chrome browser run of the standalone WebGPU `polar4` numeric harness
  reported `maxAbs=1.192093e-7`, `rms=4.375033e-8`, and `PASS`.
- Done: initial shared `turbo3` base-key path has 3-bit packed key storage,
  fallback decode, native direct encoded-key scoring, parsing, CLI usage text,
  and paged-attention benchmark sweep coverage.
- Done: WASM/WebGPU can allocate and upload packed `turbo3` base-key GPU KV
  caches, route `GQA_K_FORMAT_TURBO3` through a dedicated cached-attention
  shader, and expose a standalone browser numeric harness.
- Done: codec-level QJL-style residual sketch support for `turbo3`: fixed
  one-bit projections per KV head, deterministic projection signs, sketch
  generation from base-key residuals, and a scalar query-dependent estimator
  with unit coverage.
- Done: `bench-turboquant-distortion` reports deterministic dot-product
  distortion for `polar4`, base `turbo3`, and residual-scale sweeps so residual
  correction can be calibrated before being wired into attention logits.
- Done: Metal provider ABI has compressed-key score dispatch, Metal fast kernels
  for `polar4` and base `turbo3`, scalar-reference tests, paged-attention
  dispatch checks, and decoded-key fallback when the provider is unavailable.
- Done: Chrome browser run of the standalone WebGPU `turbo3` numeric harness
  reported `maxAbs=1.490116e-7`, `rms=4.560416e-8`, `worstIndex=271`, and
  `PASS`.
- Done: native `.turbo3` KV rows append the residual sketch after the base
  3-bit key bytes, and native direct paged attention adds the calibrated
  residual estimate to key logits.
- Done: WASM/WebGPU `.turbo3` KV upload now stores base key bytes followed by
  the residual sketch, the WebGPU row checks accept the larger row, and the
  dedicated cached-attention shader adds the residual estimate before softmax.
- Done: Metal `.turbo3` compressed-key scoring now consumes the full key row,
  keeps fallback behavior when the provider declines a shape, and adds the Metal
  residual estimate to key logits.
- Done: Chrome browser run of the standalone WebGPU `turbo3` residual numeric
  harness reported `baseBytes=48`, `residualBytes=8`, `totalBytes=56`,
  `maxAbs=1.490116e-7`, `rms=4.972192e-8`, `worstIndex=5`, and `PASS`.
- Done: native residual scoring now precomputes the 32 query projections once
  per query/head instead of once per KV token. On the local native sweep
  (`prompt_len=128`, `decode_steps=32`, `heads=8/2`, `head_dim=64`), `turbo3`
  remained at `kv_pair_bytes=192` and improved from `23.813 ms/token` before
  the hoist to `1.962 ms/token`.
- Done: local Metal dtype sweep on the same shape reported `turbo3`
  `kv_pair_bytes=192` and `decode_paged_ms_per_token=0.312`, versus `polar4`
  `kv_pair_bytes=200` and `decode_paged_ms_per_token=0.201`.
- Done: Chrome browser rerun after the WebGPU projection hoist reported
  `baseBytes=48`, `residualBytes=8`, `totalBytes=56`,
  `maxAbs=1.490116e-7`, `rms=4.972192e-8`, `worstIndex=5`, and `PASS`.
- Done: local `termite generate` smoke on GPT-2 with native `--cache-dtype
  turbo3` generated 64 tokens with `prefill=90 ms`, `decode=3731 ms`, and
  `generate=3822 ms`, or about `17.15 decode tokens/sec`. A matching native
  f32 run generated 64 tokens with `decode=4008 ms`, or about
  `15.97 decode tokens/sec`.
- Done: fixed Metal dense linear orientation for GPT-2 Conv1D-style `[in_dim,
  out_dim]` weights. Local GPT-2 Metal `--cache-dtype turbo3` generate now
  completes 64 tokens with `prefill=474 ms`, `decode=2886 ms`, and
  `generate=3361 ms`, or about `22.18 decode tokens/sec`. A matching Metal f32
  run generated 64 tokens with `decode=2449 ms`, or about
  `26.13 decode tokens/sec`.
- Done: dense Metal decode now defaults to a full decoder-stack eval stride,
  with `TERMITE_DENSE_DECODE_EVAL_STRIDE` as a rollback/tuning knob. On local
  GPT-2 Metal generate, the explicit eval count for 64 tokens dropped from `384`
  to `69`; f32 decode improved from `2449 ms` to `2209 ms`, and `turbo3`
  improved from `2886 ms` to `2081 ms`.
- Done: dense Metal decode now defaults to no explicit decoder-layer evals,
  letting the final token read force evaluation of the lazy full-stack graph.
  `TERMITE_DENSE_DECODE_EVAL_STRIDE=2` restores the old dense-decode barrier
  cadence, and positive values keep the layer-group tuning path available.
  Local GPT-2 Metal generate with the new default reported `eval_count=6`; f32
  decode was `2222 ms`, while `turbo3` decode improved to `1459 ms`, or about
  `43.9 decode tokens/sec`.
- Done: added an experimental Metal greedy decode token path behind
  `TERMITE_METAL_GREEDY_DEVICE_DECODE=1`. It uses the backend argmax path after
  the first generated token for pure greedy, grammar-free paged decode, avoiding
  full-vocab CPU logits downloads. On local GPT-2 this did not beat the default
  no-explicit-eval path: `turbo3` decode was `1558 ms` and f32 decode was
  `2270 ms`, so the path stays opt-in while deeper Metal decode ownership is
  investigated.
- Done: added `METAL.md` and an opt-in direct-Metal LM-head argmax hook behind
  `TERMITE_METAL_GREEDY_DEVICE_DECODE=1`. The Metal-provider unit compares the
  token id against scalar `hidden @ W^T` argmax. With
  `TERMITE_METAL_GREEDY_DEVICE_DECODE=1`, local GPT-2 Metal/turbo3 decode improved
  from the Metal-argmax opt-in result (`1558 ms`) to `1477 ms`, roughly matching
  the default no-explicit-eval path (`1487 ms` in the rerun).
- Done: added an opt-in Metal device-token handoff path behind
  `TERMITE_METAL_DEVICE_TOKEN_HANDOFF=1`. It can seed the generated token as an
  Metal integer tensor, feed backend tensor ids into the next embedding lookup,
  and preserve the direct-Metal LM-head token tensor across greedy paged-decode
  steps. The path remains opt-in because local GPT-2 Metal/turbo3 with
  `TERMITE_METAL_DEVICE_TOKEN_HANDOFF=1 TERMITE_METAL_GREEDY_DEVICE_DECODE=1`
  measured `decode=1543 ms`, slower than the default no-explicit-eval rerun
  (`1487 ms`) and slightly slower than a same-session direct LM-head greedy
  comparison without handoff (`1517 ms`).
- Done: added an opt-in compressed-KV decode attention block behind
  `TERMITE_METAL_COMPRESSED_ATTENTION_BLOCK=1`. For qLen=1 `polar4`/`turbo3`
  paged decode, the Metal provider can fuse compressed key scoring,
  causal/sliding masking, online softmax state update, and V accumulation for
  one KV block. Local GPT-2 Metal/turbo3 generated the same greedy stream, with
  `mlx_paged_decode.mask=0`, but measured `decode=3004 ms`; this remains
  opt-in while the next iteration removes per-block encoded-key uploads and
  per-block kernel launches.
- Done: cached encoded compressed-key Metal arrays on the per-block Metal KV cache
  entry. The opt-in compressed attention block path and the compressed-score
  fallback now reuse those arrays instead of rebuilding/uploading encoded key
  bytes for every block visit. Local GPT-2 Metal/turbo3 with
  `TERMITE_METAL_COMPRESSED_ATTENTION_BLOCK=1` improved slightly to
  `decode=2962 ms`, confirming that per-block kernel launch/object overhead is
  the larger remaining issue.
- Done: added a separate opt-in compressed-KV span path behind
  `TERMITE_METAL_COMPRESSED_ATTENTION_SPAN=1`. It maintains persistent
  per-layer gathered V and encoded-key arrays across qLen=1 decode steps, then
  runs one Metal kernel over the retained KV span instead of launching once per
  KV block. Local correctness smoke passed, but the warmed 64-token GPT-2
  Metal/turbo3 run measured `decode=4473 ms`. The paged-attention counters dropped
  (`mlx_paged_decode.total=15 ms`), but whole-token decode regressed because the
  span kernel serializes too much work per query head.
- Done: split the block and span toggles so the cheaper cached block experiment
  remains available independently. With
  `TERMITE_METAL_COMPRESSED_ATTENTION_BLOCK=1`, the latest local GPT-2
  Metal/turbo3 rerun measured `decode=2872 ms`, still correct and still slower
  than the default Metal/turbo3 path.
- Done: added chunked span partials under the existing
  `TERMITE_METAL_COMPRESSED_ATTENTION_SPAN=1` path for retained spans over
  32 tokens. The partial kernel computes per-head/per-chunk softmax state and
  weighted V, then a reduce kernel merges the chunks. The 64-token GPT-2
  Metal/turbo3 span run improved from `decode=4473 ms` to `decode=3673 ms`, but
  remains slower than the default and cached per-block paths.
- Done: hoisted turbo3 residual query projections out of the per-token scoring
  loops in the compressed attention block, span, and chunked-span kernels. The
  same 64-token GPT-2 Metal/turbo3 smoke now measures default `decode=1512 ms`,
  cached per-block `decode=1605 ms`, and chunked span `decode=1544 ms`
  (`1536 ms` warmed). This makes the span path roughly competitive for GPT-2,
  though still opt-in because it is not a consistent win yet.
- Done: hoisted the same turbo3 residual query projection work out of the
  default `compressedKeyScores` Metal kernel used by the non-span paged decode
  path. A warmed same-session 64-token GPT-2 Metal/turbo3 default rerun measured
  `decode=1240 ms`, which re-establishes the default path as the faster GPT-2
  option in the current code.
- Done: started the backend-owned raw-Metal whole-token bring-up with a
  session-owned decode runtime in `metal_kernels.m`. It now owns persistent
  device/queue/library state plus reservable scratch/token buffers, so the next
  steps can build an actual GPT-2 greedy `qLen=1` decode loop below
  `mlx_fast_metal_kernel`.
- Done: threaded the whole-token bring-up flag through the Metal generation loop.
  `TERMITE_METAL_WHOLE_TOKEN=1` now prepares that runtime during the
  narrow decoder-only greedy paged-decode path and then falls back to the
  existing Metal token execution.
- Done: moved the absolute token-input slice to resident Metal-owned embedding
  tables. The raw whole-token runtime now uploads `wte`/`wpe` once, and the
  per-token entry point passes only `token_id` and `position_id` to produce the
  positioned hidden input inside Metal.
- Done: moved GPT-2 layer-0 attention pre-norm (`h.0.ln_1`) into the same
  raw-Metal runtime as a resident layer-norm slot. The whole-token bring-up now
  runs token embedding, absolute position add, and the first decoder pre-norm
  on Metal before falling back to Metal. The latest unsandboxed GPT-2 Metal/turbo3
  smoke with `TERMITE_METAL_WHOLE_TOKEN=1` showed
  `raw_whole_token_prepare_layer_norm_calls=1`,
  `raw_whole_token_apply_layer_norm_calls=3`, and
  `gpt_timing_ms.attn_norm=0`, confirming the override is live.
- Done: moved GPT-2 layer-0 fused attention projection (`h.0.attn.c_attn`) into
  the same raw-Metal runtime as a resident dense-linear slot. The whole-token
  bring-up now runs `embed -> ln_1 -> c_attn` on Metal for layer 0 before
  falling back to Metal. The latest unsandboxed GPT-2 Metal/turbo3 smoke showed
  `raw_whole_token_prepare_linear_calls=1`,
  `raw_whole_token_apply_linear_calls=3`,
  `input_successes=3`, and decode improved from `1054 ms` to `872 ms` for the
  4-token greedy check.
- Done: moved GPT-2 layer-0 attention output projection (`h.0.attn.c_proj`)
  into the same raw-Metal runtime as a second resident dense-linear slot. The
  latest unsandboxed GPT-2 Metal/turbo3 smoke showed
  `raw_whole_token_prepare_linear_calls=2`,
  `raw_whole_token_apply_linear_calls=6`, and the whole-token greedy check
  remained correct with `decode=918 ms`. That is still better than the earlier
  `1054 ms` baseline, though not better than the `872 ms` run that only owned
  `c_attn`.
- Done: moved the rest of the layer-0 GPT-2 MLP shell into the raw-Metal
  runtime: `h.0.ln_2`, `h.0.mlp.c_fc`, and `h.0.mlp.c_proj`. The latest
  unsandboxed GPT-2 Metal/turbo3 smoke showed
  `raw_whole_token_prepare_layer_norm_calls=2`,
  `raw_whole_token_apply_layer_norm_calls=6`,
  `raw_whole_token_prepare_linear_calls=4`,
  `raw_whole_token_apply_linear_calls=12`, and the whole-token greedy check
  remained correct with `decode=968 ms`.
- Done: moved the layer-0 GPT-2 activation and both residual adds into the
  raw-Metal whole-token runtime. The latest unsandboxed GPT-2 Metal/turbo3 smoke
  showed `raw_whole_token_apply_activation_calls=3`,
  `raw_whole_token_apply_add_calls=8`, preserved the greedy stream
  (`the!!!`), and measured `decode=1016 ms` for the 4-token check. This keeps
  more of layer 0 inside the raw-Metal strip, but the main remaining cost is
  still the Metal-owned attention core and later decoder layers.
- Done: routed qLen=1 paged decode through a whole-token backend attention op
  that updates KV once and then calls a raw-runtime compressed span kernel
  below `mlx_fast_metal_kernel`. The latest unsandboxed GPT-2 Metal/turbo3 smoke
  showed `raw_whole_token_attention_span_calls=48`, preserved the greedy stream
  (`the!!!`), and measured `decode=1026 ms` for the 4-token check. This proves
  the attention core moved onto the raw-runtime span path, but it is not yet a
  win on this short GPT-2 case; the remaining cost is likely the gathered-KV
  host/Metal boundary and later-layer fallback.
- Done: added a suffix-only resident-span update path for the raw whole-token
  attention runtime. When qLen=1 decode just appends one new retained KV row,
  it now slices only the suffix encoded-key row on the Metal side and asks the
  raw runtime to memmove the retained resident span and copy only the appended
  encoded-key/V rows. The latest unsandboxed GPT-2 Metal/turbo3 smoke preserved
  the greedy stream (`the!!!`) and moved `decode` from `1140 ms` to
  `1116 ms`, but `gpt_timing_ms.attn_qkv=854` still dominates. That means the
  next meaningful win is to move more than layer 0 off Metal, not another small
  span-upload tweak.
- Done: extended the raw whole-token GPT-2 slot preparation from layer 0 to
  layers 0 and 1 and added a positioned-embedding greedy override entry so the
  GPT path consumes those raw slots directly instead of generation manually
  applying layer-0 norm/QKV. The latest unsandboxed GPT-2 Metal/turbo3 smoke
  measured `decode=1093 ms`, `raw_whole_token_prepare_layer_norm_calls=4`,
  `raw_whole_token_prepare_linear_calls=8`,
  `raw_whole_token_apply_layer_norm_calls=12`,
  `raw_whole_token_apply_linear_calls=24`, and
  `raw_whole_token_attention_span_calls=48`. This does move more of the token
  loop under the raw-Metal whole-token entry, but it is still not numerically
  correct relative to default Metal greedy decode: raw whole-token emitted
  `the!!!` while the default path emitted `the the the the` for the same local
  command. So the next blocker is correctness of the raw whole-token math/path,
  not adding still more layers.
- Pending: model-level quality gates using real model weights.
