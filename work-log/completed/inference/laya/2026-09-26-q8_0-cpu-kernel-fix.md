# Laya q8_0 CPU kernel fix: 2026-09-26

Design and summary: [`zig/pkg/inference/models/laya/LAYA.md`](../../../../zig/pkg/inference/models/laya/LAYA.md)
(Weight quantization, step 1d).

Track: q8_0 kernel performance on CPU and Metal, following on from the
2026-09-25 CPU/Metal footprint-and-accuracy measurement
(`docs`-adjacent table in LAYA.md), which found CPU q8_0 both slower than
dense (602 s -> 1,587 s, contended) and larger (2.94 GB -> 3.47 GB).

## Investigation

Traced every call site between `laya.zig`'s `Config.weight_quantization`
and the CPU linear kernel in `ops/native_compute.zig`:

- `architectures/session_factory.zig`'s `layaQuantizesWeights`/`layaQ8Weight`
  quantize Laya's encoder (`attn.Wqkv`, `attn.Wo`, `mlp.Wi`, `mlp.Wo`) and
  decision-head (`self_attn.in_proj_weight`, `self_attn.out_proj.weight`,
  `linear1.weight`, `linear2.weight`) linears to Q8_0 at load
  (`weight_source.quantizeDenseQ8_0`), storing them directly in
  `resident_weights` — never through the lazy/GGUF-backed path.
- `ops/native_compute.zig` already has a fast path for other quantized
  architectures: `shouldUseQuantizedDequantSgemm` dequantizes a weight once
  (cached up to a 512 MB budget, `TERMITE_QUANT_DEQUANT_SGEMM_CACHE_BYTES`)
  and calls Accelerate's SGEMM (`dispatchSgemmTransB`) instead of the native
  int8 kernel. It is gated by per-architecture name heuristics
  (`shouldUseGlinerRecognizerDequantSgemm`, `shouldUseGlinerEncoderDequantSgemm`,
  `shouldUseClipClapDequantSgemm`). None of them match Laya's weight names
  (ModernBERT `attn.Wqkv`/`mlp.Wi`/`mlp.Wo` naming, not GLiNER's
  `encoder.layer.N.attention.self.query_proj` or CLIP/CLAP's
  `text_model.encoder.layers.N.self_attn.q_proj`), so every Laya Q8_0 linear
  fell through to the native int8 dot-product kernel
  (`linearNoBiasQ8_0WithPolicy` -> `linearQ8_0QuantizedInputDispatch`).
- That native kernel needs `QuantizedStorage.prepared` layouts built once per
  weight (`ensurePreparedKBlock` -> `prepareNativeQuantizedStorage`, called
  from `loadWeight` on first use). For Q8_0 this builds a row-major
  "prepared" copy (`prepareQ8_0PanelPackedStorage`'s input) and a 4-row panel
  copy (`prepareQ8_0PanelPackedStorage`'s output), each close to the size of
  the compressed weight, *in addition to* the original `raw_bytes`. Three
  representations of a weight that packs to ~1/4 of dense f32 land close to
  the size of dense f32 itself — the CPU footprint regression in the prior
  table.
- The persistent dequant-SGEMM cache used by GLiNER/ClipClap is the wrong
  tool for Laya: 512 MB does not fit Laya's ~330 M encoder parameters, so
  most layers would still fall back (to the scratch path, which is
  off by default, `TERMITE_QUANT_DEQUANT_SGEMM_SCRATCH`) or to the native
  kernel, and whatever *does* fit would sit in the cache indefinitely as a
  second dense copy.

## Fix

`ops/native_compute.zig`:

- `shouldUseLayaDequantSgemm(name)`: a Laya-specific predicate, checked
  ahead of the generic dequant-sgemm heuristics in `dispatchQuantizedLinear`
  for `.single_no_bias`/`.single_bias` (the only two dispatch kinds Laya
  uses — its Wqkv/Wi/Wo and head linears are always single tensors, never
  paired).
- `layaDequantScratchSgemm`/`tryLayaDequantSgemmNoBias`/`tryLayaDequantSgemmBias`:
  dequantize the weight from `raw_bytes` into a scratch buffer allocated and
  freed for that one call (no persistent cache, no budget contention with
  GLiNER/ClipClap), then call the same `dispatchSgemmTransB` the dense path
  uses.
- `loadWeight` skips `ensurePreparedKBlock` for weights `shouldUseLayaDequantSgemm`
  matches, so the row-major/panel copies are never built. Only `raw_bytes`
  (the compressed weight) is retained.

**Naming bug found while implementing this.** The first version reused
`models/laya.zig`'s existing `quantizedLinear(name)` predicate, which checks
*checkpoint* tensor names (`"encoder.layers.N...."`, `"head.layers.N...."` —
correct for `layaQ8Weight`'s load-time call). But `dispatchQuantizedLinear`
and `loadWeight` see the *runtime* weight-buffer name, which
`session_factory.normalizeWeightKey` has already rewritten: `"encoder."` ->
`"model."` for the encoder, and `"model."` prepended unconditionally for the
head (`laya_head.weight` builds `"model.{prefix}.{suffix}"` directly). Reusing
`quantizedLinear` at dispatch time would have silently never matched, making
the whole change a no-op with no test failure to catch it (the existing
`laya_quantized_test.zig` parity test would still pass, since the native
kernel path is also correct — just slow and memory-heavy). Caught this by
tracing `getLayerWeight`/`laya_head.weight`'s actual runtime key format
before trusting the reused predicate; fixed with a runtime-key-space
predicate in `native_compute.zig` and a dedicated regression test that pins
both spellings (checkpoint names must *not* match; runtime keys must).

## Verification

`~/bin/zig build test -- --test-filter "q8_0"` (generic kernel suite, no
fixtures needed): 30 selected, 29 passed, 1 skipped (a Laya test that needs
`ANTFLY_LAYA_REFERENCE`) — unchanged from before this change, confirming no
regression to GLiNER/ClipClap/general GGUF Q8_0 decode.

`ANTFLY_LAYA_REFERENCE=<fixtures>/ref zig build test -- --test-filter "laya"`,
CPU and (`ANTFLY_LAYA_METAL=1 ANTFLY_LAYA_BACKEND=metal`) Metal: 60 selected,
52 passed, 8 skipped (CUDA-only, benchmark-only, or optional-path skips) on
both backends. Notably `pipelines.laya_quantized_test` ("laya q8_0 linear
weights keep every decision and stay close to dense serving"): labels
identical, max probability error 9e-7 (CPU) / 5e-6 (Metal) — inside the 2e-2
gate and roughly the same order as before the fix, since both the native
kernel and the new dequant+SGEMM path compute the same dequantized matmul.

Two new `ops/native_compute.zig` tests:

- `"laya dequant sgemm predicate matches normalized runtime keys, not
  checkpoint names"` — pins the naming-bug fix described above.
- `"laya q8_0 encoder linear uses dequant sgemm and skips native panel
  preparation"` — builds a small Q8_0 storage under a Laya-style runtime
  key, dispatches a linear through the full `ComputeBackend.linear` path,
  and checks (a) the result matches a manual dequantize + matmul + bias
  reference, (b) the dequant-sgemm dispatch counter incremented, and (c)
  `QuantizedStorage.prepared.ownedBytes() == 0` afterward (the native
  kernel's prepared copies were never built).

## Not done: fresh ReleaseFast measurement

Could not get a clean ReleaseFast timing/footprint run in this session. The
shared build lock stayed held for the entire session by other agents' long
training/eval runs (one job alone, `eval42-then-train43.sh`, held it for
50+ minutes). A lock-policy change mid-session (`with-lock gpu` no longer
implicitly holding `build`) did not help, because the in-flight holder had
already committed to the old combined-hold behavior for its own lifetime,
and the very next holder was a 20-iteration flake-hunting loop that also
held `build` throughout. See LAYA.md's "Not yet re-measured" note for the
repro command and the expected direction (CPU eval time close to or better
than dense; CPU peak footprint close to dense minus most of the
encoder+head linear weight bytes). This is the one open item for this
track — someone with a clear run of the build lock should re-run the
step 1d table's CPU rows and update both the table and this file.

## Files changed

- `zig/pkg/inference/src/ops/native_compute.zig` — the fix and its tests.
- `zig/pkg/inference/models/laya/LAYA.md` — step 1d section and roadmap row.
