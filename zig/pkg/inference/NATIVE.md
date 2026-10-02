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

## Status

- Native CPU backend availability is decoupled from system BLAS linkage.
- `-Dsystem-blas` controls link-time system BLAS acceleration.
- Linux x86 GNU builds prefer runtime-loaded OpenBLAS when installed, with native fallback.
- Native builds report the `native` backend explicitly.
- Backend identity stays `native`.
- Shared pure Zig kernels live in `lib/linalg` and are reused by native and WASM.
- Optional system BLAS roots on non-macOS have explicit build/docs support.
- CLI/help/version surfaces describe native vs system BLAS cleanly.

## Design

### Availability decoupled from acceleration

Native builds always expose the CPU fallback backend.

- `build_options.enable_native` means the portable CPU backend is available.
- `build_options.enable_system_blas` controls whether `cblas`/Accelerate is imported and linked.

### Shared kernel layer

`lib/linalg/src/mod.zig` is the shared pure Zig linear algebra module for:

- `sgemm`
- `sgemmTransA`
- `sgemmTransB`
- simple normalization/reduction helpers where reuse is clean

`src/backends/native.zig` is a thin dispatch layer:

- use system BLAS when available
- otherwise call the shared Zig kernels

WASM calls the same shared kernels directly where that reduces duplication.

### Backend surface

The public backend surface is consistent across:

- backend enums
- backend selection logic
- CLI choices
- server version reporting
- docs

### Optional system BLAS configuration

Linux x86-64 GNU builds enable optional runtime OpenBLAS by default. Install
`libopenblas0-pthread` on Debian/Ubuntu to accelerate native inference. The
official amd64 `zig/Dockerfile.runtime` image includes it. The executable has
no required OpenBLAS dependency and still starts and runs native kernels when
the library is absent. Mac Accelerate and portable musl builds are unchanged.

Runtime controls (read once on first native matrix operation):

- `ANTFLY_INFERENCE_BLAS=auto` (default): prefer compatible OpenBLAS, otherwise
  use the native kernels. `off` disables runtime loading; `openblas` requires
  it and fails explicitly if unavailable.
- `ANTFLY_OPENBLAS_LIBRARY=/absolute/path/libopenblas.so.0` selects a custom
  library. Otherwise the system loader searches for `libopenblas.so.0`.
- `OPENBLAS_NUM_THREADS` requests the BLAS thread count. The default is two,
  clamped to the existing native CPU budget (affinity, cgroup quota, maximum
  eight workers, and `ANTFLY_INFERENCE_CPU_THREADS`). Explicit requests are
  also clamped to that budget. The selected count is logged once.
- `-Druntime-openblas=false` removes runtime loading support at build time.
  This is independent of `-Dsystem-blas`, which retains its link-time meaning.

The runtime path supports LP64 pthread OpenBLAS. It rejects ILP64, OpenMP,
sequential, and incomplete libraries before calling GEMM, using native fallback
in `auto` mode. It initializes the library once and retains it for process
lifetime. OpenBLAS thread settings are process-wide; configure them before
starting inference. These controls do not change explicitly linked BLAS builds.

Run `python3 zig/tools/verify_runtime_openblas.py` from the repository root to
check loading, fallback, thread limits, and numerical behavior. Add
`--openblas-library /path/to/libopenblas.so.0` to test an installed library too.

### Explicit link-time BLAS

Non-macOS native acceleration is configured with:

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
