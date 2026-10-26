# TurboQuant

The experimental `polar4` and `turbo3` paged-KV formats compute attention
logits directly from compressed keys. Their names predate a paper-conformance
review: the current codecs use scalar quantization clipped to `[-1, 1]`, with
a fixed 32-bit residual-sign sketch for `turbo3`. They do **not** implement the
paper's random rotation, distribution-aware quantizer, or unbiased QJL
estimator. Neither the paper's quality guarantees nor its speedups apply to
these implementations without independent measurements.

The separate attention-matched `rotated4` checkpoint/CUDA path passed bounded
E4B Q4_0/L4 qualification. See [its implementation and measured scope](#attention-matched-caches-rotated4-checkpoints-and-cuda);
that status does not promote the legacy paged-KV formats above.

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
| `polar4` | Legacy uniform scalar key format | 4 bits/key coordinate, V codec configurable | direct compressed-key logits |
| `turbo3` | Legacy uniform scalar keys plus residual signs | 3 bits/key coordinate plus 32 bits/head-row, V codec configurable | direct compressed-key logits plus residual estimator |

`polar4` is the first, smallest complete step that proves the cache layout and
direct scoring path. `turbo3` adds the existing fixed-scale residual correction.

TurboQuant-style cache rows use asymmetric K/V storage because key scoring and
value accumulation have different kernel and quality requirements:

```text
cache dtype polar4 =
  K: polar4 direct-scored key codes
  V: existing int8-style per-head quantization, with f16 available as a debug/quality fallback

cache dtype turbo3 =
  K: scalar key codes plus residual-sign sketch
  V: configurable value codec, initially the same V policy as polar4
```

This avoids forcing V through the same experimental format as K while the direct
key estimator is still being proven.

### Current codec layout

Keys use uniform 4-bit or 3-bit scalar codes over `[-1, 1]`. Turbo3 adds four
bytes per head-row for residual signs and uses a fixed correction scale of
0.125. Values default to int8 with a four-byte scale per head-row. Supported
head dimensions are 64, 80, 128, 256 and 512. There is no random preconditioner
in this layout. A rotated codec must use a distinct, versioned representation;
changing the meaning of these existing codes would break stored data.

### Attention-matched caches: rotated4 checkpoints and CUDA

`attention_match_engine.zig` can quantize newly fitted old rows using the
`rotated4_checkpoint` option. This path passed the bounded E4B Q4_0/L4/eager-CUDA
production qualification described below. It first validates the FP16 fit,
then checks the combined fitting/quantization error against the original whole
head, using held-out queries from every shared-donor consumer. The existing
mass and output thresholds apply. Rejection retains the validated FP16 fit;
allocation failures and cancellation propagate. Unsupported dimensions remain
FP16. FP8 and rotated4 preparation are mutually exclusive.

Publication also requires the fitted geometry to meet the configured global
savings minimum **before newly quantized rows contribute their byte savings**.
This preserves FP16's original-state fallback: a codec must not make an
otherwise unpublished fit eligible. Previously compressed history retains its
actual byte cost across cycles. The final encoded candidate must independently
meet the same savings minimum. This conservative rule reduces compression
coverage; it does not replace full-model answer-quality qualification.

Protected prefix/recent rows and local windows remain exact. Previously fitted
history retains its representation, including compressed bytes, across later
cycles. Canonical nonadjacent spans allow old compressed history, a gap from an
FP16 fallback, and newly compressed rows to coexist. Adjacent spans merge
without re-encoding earlier rows. Bias, position, and coverage metadata retain
full precision. Host staging includes decoded arrays plus compressed bytes;
both are charged to admission.

`turboquant_rotated.zig` defines rotated4 version 1: two seeded signed Hadamard
transforms, a per-vector norm scale, and a Gaussian Lloyd-Max 4-bit codebook.
Keys occupy `4 + dim / 2` bytes per row; int8 values occupy `4 + dim` bytes.
Supported dimensions are 64, 128, 256, and 512. This is a TurboQuant-inspired
MSE quantizer, with a structured rotation and approximate sphere marginal;
it does not implement the paper's Haar rotation or unbiased QJL residual.
Version 1 fixes seeds, transform order, and codebook. An independent float32
wire oracle guards compatibility. Legacy `polar4`/`turbo3` stay separate.

Checkpoint V4 uses the existing model identity and SHA-256 footer framing.
Each head records codec version, span count, compressed row count, a reserved
zero field, and little-endian `(first, rows, offset)` span triples. Positions
and coverage precede raw compressed K/V, exact FP16 K/V, and FP32 biases.
Restore validates bounds before allocating, charges raw and decoded storage,
checks canonical spans/scales/codes, and reconstructs the host arrays. Export
verifies those arrays agree with the stored bytes. Older wire encodings reject
compressed heads rather than silently dropping their representation.

CUDA keeps compressed K/V and span tables resident alongside flattened exact
rows. A query rotates once per dispatch chunk; compressed keys are scored in
that basis and exact keys use the original query. Values decode during weighted
accumulation. Coverage, grouped-query attention, softcap-before-bias, and local
rings retain their semantics. Exact appends follow compressed history. Clone,
grow, rollback, and download preserve compressed bytes; grow transfers their
ownership only after the replacement allocation and stream fence succeed.
Device accounting includes compressed bytes, exact capacity, spans, metadata,
and query-rotation scratch.

The diagnostic pipeline accepts
`ANTFLY_INFERENCE_ATTENTION_MATCH_FIT_ENCODING=rotated4`, with whole-head FP16
fitting enabled. It records accepted/fallback head counts and newly eligible,
compressed, and retained-exact row counts. Public preparation defaults to `f16`;
`X-Antfly-KV-Encoding: rotated4` selects V4 without an experimental process
switch. The Python SDK supports `encoding="rotated4"` for preparation and
validates the V4 metadata before atomic file publication. Upload/resume validates
the stored file and the server checks complete codec semantics before installation.
FP8 and legacy f32 remain alternative encodings. Legacy `polar4`/`turbo3` paged-KV
formats are separate and remain experimental.
`attention_match_quantized.zig` remains a CPU
comparison adapter for legacy formats and rotated4, not the serving owner.

Validation includes checkpoint round trips, hostile metadata, truncated streams,
cancellation, exhaustive host allocation failures, repeated fitting across an
exact gap, and a CUDA correctness executable. GPU checks cover all four head
dimensions with FP16/FP32 exact rows, mixed and fully compressed heads, masks,
softcap, grouped queries, clone, append, grow, rollback, device accounting,
partial allocation failures, and cancellation. These checks establish component
correctness, not full-model quality or performance.

Run the component checks from the worktree root, with the pinned Zig/CUDA
toolchains on `PATH` and caches inside this worktree:

```sh
mkdir -p zig/.zig-cache/tmp zig/zig-out/bin
export TMPDIR="$PWD/zig/.zig-cache/tmp"
export ZIG_LOCAL_CACHE_DIR="$PWD/zig/.zig-cache"
export ZIG_GLOBAL_CACHE_DIR="$PWD/zig/.zig-cache/global"
zig test zig/pkg/inference/src/runtime/kv/attention_match_quantized.zig -O ReleaseSafe
zig test zig/pkg/inference/src/attention_match_decode_cuda_qualify.zig -O ReleaseSafe -lc
zig build-exe zig/pkg/inference/src/attention_match_decode_cuda_qualify.zig \
  -O ReleaseSafe -lc -fstrip -femit-bin=zig/zig-out/bin/attention-match-decode-cuda-qualify
compute-sanitizer --tool memcheck --error-exitcode 90 zig/zig-out/bin/attention-match-decode-cuda-qualify
compute-sanitizer --tool racecheck --error-exitcode 90 zig/zig-out/bin/attention-match-decode-cuda-qualify
bash zig/pkg/inference/scripts/regen-attention-match-decode.sh --check
```

Qualification requires actual quantization coverage and zero new failures versus
independently prepared FP16 and full KV, reporting fallback and existing model
failures separately. Restore/decode latency and memory savings are measured
independently. A QJL variant needs its own codec identifier and qualification;
it is not implied by rotated4 support.

#### Full-model qualification status (2026-10-23)

Attention-matched rotated4 checkpoints are production qualified for the bounded
Gemma 4 E4B Q4_0 / NVIDIA L4 / eager-CUDA profile. The
[qualification record](../../../docs/design/inference/native-kv-compaction.md#bounded-qualification-2026-10-23)
identifies the model and binaries, numerical-source provenance, runtime settings,
complete measurements and retained local evidence. FP16 remains the default.
This status does not apply to legacy `polar4`/`turbo3`, other models or GPUs,
graph replay, continuous batching or automatic mid-generation compaction.

The quality campaign independently prepared FP16 and V4 from each archive before
appending the future question. Sixteen preselected critical cases per context
covered facts, instructions, code and tool calls. Full KV, FP16 and rotated4 each
scored 13/16 at both 8K and 32K, with the same three baseline model failures and
zero added failures. Actual compaction and rotated4 quantization covered all
four categories: 10/16 cases at 8K and 16/16 at 32K. The six unchanged 8K
fallbacks do not count as compressed-state quality coverage.

The final standalone passed V4 export/restore and restart at both context lengths,
Python SDK preparation, unchanged fallback, checksum/prefix/context-limit
rejection and recovery, physical restore OOM, upload concurrency, cancellation
and SSE abort. Its 100 mixed requests and 10 fresh actual compactions passed
with byte-identical exports. Idle host growth was 1.3 MiB and idle GPU memory
decreased, within the 128 MiB growth diagnostic. Peak public-path host/GPU
memory stayed below 7.47/12.47 GiB, against 8/18 GiB caps. Physical OOM coverage
is restore recovery on the same loaded worker, not live-cache replacement OOM.

| Public fixture | FP16 restore p50 / p95 | Rotated4 restore p50 / p95 | Rotated4 global-KV savings |
| --- | ---: | ---: | ---: |
| 8,189 tokens | 1.872 / 2.029 s | 1.972 / 1.999 s | 1.937x |
| 32,752 tokens | 4.255 / 4.514 s | 4.578 / 4.671 s | 2.811x |

Each encoding used five warmups and 20 measured restores in independent warmed
processes. Total, first-content and remaining-decode p50/p95 all passed the
1.25x/1.5x FP16 limits. Separate interleaved measurements on the selected
quality-suite case also passed at both sizes. These measurements include
portable transfer/validation/upload and a short continuation; they exclude
initial fitting/export and do not establish llama.cpp parity. The record
separately reports matched full replay and the slower appended-input path.

The runner is `scripts/gemma4/qualify_rotated4_checkpoint.py`. Use
`--quality-profile bounded --require-full-kv-preservation` for exactly 16
critical cases, four per category, and `--sampling-profile bounded` for five
warmups and 20 paired samples. The first eligible text case in frozen suite
order supplies paired latency; both checkpoints must compact, rotated4 must
contain compressed rows, and both answers must be correct with equal completion
token counts. Selection precedes timing and cannot skip a slower result.
The 64-case quality, 100-sample latency and 1,000-request reliability profiles
remain available as extended checks. Twenty successive real-model compaction
cycles were explicitly deferred; this bounded qualification does not claim
those campaigns or a long-run tail-latency guarantee.

#### Selection policy and resolved regressions

The initial all-row rotated4 candidate passed numerical output checks but failed
attention-mass validation, yielding no actual quantization coverage. The selective
policy ranks changes in each newly fitted row's unnormalized attention
contribution, relative to the fitted head's partition sum, using 64 uniformly
spaced training queries. The highest-risk quarter stays FP16, rounded down for
tiny intervals; remaining rows retain their encoded bytes in canonical spans.
The whole-head held-out gate runs after selection and does not tune the retention
fraction. Previously compressed history stays immutable. Admission includes
probability matrices, ranking buffers, accelerated attention workspace and
simultaneous encoded/filtered candidates; rejection/cancellation/allocation
failure preserve atomic publication.

An early tool-call regression exposed publication-policy divergence: FP16 kept
the original state while rotated4 used codec savings to publish a fit. The
pre-quantization savings gate now prevents that bypass. A later 32K tool-argument
regression affected both FP16 and rotated4 because historical calibration
windows selected only declarations. Four 64-token historical windows plus 256
recent tokens, within the unchanged 512-token study budget, fixed the tested
regression with actual compaction. The 1,024-row fitting blocks, 5% numerical
limits and fallback rules remain unchanged.

Restore overhead was reduced by scanning exact ranges once during precision
preflight and batching fragmented exact-row uploads into the existing conversion
slab. Wire bytes, validation and cancellation fences were preserved. Earlier
fallback-only pilots and failed quality/latency runs remain historical evidence;
they are not counted as passing release gates.

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
> [docs/design/inference/history/turboquant-history.md](../../../docs/design/inference/history/turboquant-history.md).
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
