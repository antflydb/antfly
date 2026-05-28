# Native CPU Backend

## Goal

Make the native CPU path portable by default:

- builds and runs without `libopenblas` installed
- shares pure Zig kernels with WASM where practical
- uses system BLAS only as optional acceleration

The public/backend name is `native`. System BLAS remains an optional acceleration layer underneath it.

## Why

Today two concerns are coupled:

1. whether the native CPU backend exists
2. whether the build can link a system BLAS implementation

That makes Linux and cross-platform builds more fragile than they need to be. The code already contains pure Zig SIMD/scalar fallbacks for the hot GEMM entry points, so the right move is to make the portable CPU backend always available and treat OpenBLAS/Accelerate as an optimization layer.

## Current Status

- [x] Native CPU backend availability decoupled from system BLAS linkage.
- [x] `-Dsystem-blas` controls optional system BLAS acceleration.
- [x] Native builds report the `native` backend explicitly.
- [x] Backend identity stays `native`.
- [x] Shared pure Zig kernels live in `lib/linalg` and are reused by native and WASM.
- [x] Add explicit build/docs support for optional system BLAS roots on non-macOS.
- [x] CLI/help/version surfaces describe native vs system BLAS cleanly.

## Phase Plan

### Phase 1: Decouple availability from acceleration

Done in this change.

- Native builds always expose the CPU fallback backend.
- `build_options.enable_native` means the portable CPU backend is available.
- `build_options.enable_system_blas` controls whether `cblas`/Accelerate is imported and linked.

### Phase 2: Shared kernel layer

Done in this change.

Created `lib/linalg/src/mod.zig` as the shared pure Zig linear algebra module for:

- `sgemm`
- `sgemmTransA`
- `sgemmTransB`
- simple normalization/reduction helpers where reuse is clean

`src/backends/native.zig` should become a thin dispatch layer:

- use system BLAS when available
- otherwise call the shared Zig kernels

WASM now calls the same shared kernels directly where that reduces duplication.

### Phase 3: Backend cleanup

Done for the public backend surface:

- backend enums
- backend selection logic
- CLI choices
- server version reporting
- docs

### Phase 4: Optional system BLAS configuration

Done for non-macOS native acceleration:

- `-Dblas-root=/path`
- `-Dsystem-blas=true|false`

This configures include/library/runtime search paths without making ONNX Runtime bundles part of the native backend contract.

## Constraints

- Do not regress WASM portability.
- Do not make the native CPU backend depend on ONNX Runtime packaging details.
- Prefer narrow, verified refactors over a one-shot rename across the whole tree.

## Quantized GGUF Dispatch Policy

Quantized GGUF weights use direct native kernels by default. The native backend
dispatches quantized linear, pair, and triple operations through the shared
quantized kernel dispatcher, which selects the prepared activation/panel route
for the current format and shape.

Dense dequant+SGEMM remains an explicit rollout and benchmark path:

- `TERMITE_QUANT_DEQUANT_SGEMM=1` enables the supported-format dense dequant
  path.
- `TERMITE_QUANT_DEQUANT_SGEMM_CACHE_BYTES` bounds the persistent f32 cache.
- `TERMITE_QUANT_DEQUANT_CACHE=0` disables the persistent dense cache.
- `TERMITE_QUANT_DEQUANT_SGEMM_SCRATCH=1` enables transient dense scratch for
  benchmark/debug runs.

Cache denial falls back to the direct quant kernel instead of silently
materializing transient f32 weights. Low-level per-format force knobs exist in
code for kernel development and benchmark sweeps, but they are not part of the
normal native backend configuration. Production paths should rely on dispatcher
defaults and the bounded dequant controls above.

Quantized direct kernels use the persistent native worker pool by default. Use
`TERMITE_QUANT_PARALLEL=0` for single-threaded debugging,
`TERMITE_QUANT_PARALLEL_WORKERS` to cap worker count, and
`TERMITE_QUANT_PARALLEL_DEBUG=1` to print dispatch decisions.

## Notes

- macOS can keep using `Accelerate` by default when system BLAS acceleration is enabled.
- On non-macOS, `-Dsystem-blas=true` links OpenBLAS, and `-Dblas-root=/path` points the build at an explicit OpenBLAS-style install with `include/` and `lib/`.
- Performance work belongs after the portability boundary is correct.
