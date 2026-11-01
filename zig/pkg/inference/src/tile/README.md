# Tile kernels

Describe each kernel once as comptime Zig data and lower it to every backend:

| Lowering | How | Output |
| --- | --- | --- |
| CPU | walks the description at comptime and emits `@Vector` code | compiled in, no text |
| Metal | renders MSL at comptime | `src/ops/metal/generated/tile_*.metal` |
| CUDA | renders CUDA C++ at comptime | `src/ops/cuda/generated/tile_*.cu` |
| OpenCL | test dialect of the GPU templates, run on the CPU with PoCL | written on demand, not checked in |
| Oracle | scalar, f64 accumulation | tests only |

The standard-library-only portable core lives in `zig/lib/tile/src/` and is
imported as `antfly_tile` by inference and vector search. Model adapters and
native schedule generators remain here. File names in this table refer to
the shared core.

Primitives:

| File | Primitive | Notes |
| --- | --- | --- |
| `tile.zig`, `cpu.zig`, `gpu.zig` | `RowReduce` | per-element masks, argmax / log-sum-exp channels (logits post-processing) |
| `gemm.zig` | `Gemm` | `x · wᵀ` with an `FExpr` epilogue: bias, GELU/SiLU, gating, residual, explicit F16/BF16 rounding fuse into the store |
| `norm.zig` | `LayerNorm`, RMSNorm | separate centered and uncentered reductions; strict fused residual schedule |
| `attention.zig` | `Attention` | flash-style online softmax; self, causal (KV cache) and cross attention |
| `layout.zig` | `Im2col`, `Embed` | typed strided copy/gather/split/packing; conv unrolling; token + position gather |
| `fexpr.zig`, `vmath.zig` | epilogues, math | `exp`/`erf`/GELU polynomials shared by all backends |
| `native_math.zig`, `native_normalization.zig` | native numerical profiles and exports | common expression trees with explicit activation association, affine order and statistics algorithms |

`gpu_model.zig` renders the model primitives and documents their GPU launch
contract (binding order, params struct, grid, threads).

## Shared operation contracts

CPU inference uses the shared ordered suppression, SiLU expression, RMS and
residual-RMS schedules, typed row gather/split, and masked pooling. Vector
search uses the unchanged four-accumulator dot schedule; Metal k-means embeds
the generated distance helper. Its direct squared-difference L2 and scalar
accumulation order remain distinct from the SIMD dot schedule.

Epilogues can place explicit F16/BF16 rounding between bias, activation,
gating and residual stages. Gate/up and skip residual are separate operands.
Native GEMMs can consume named operands through `FExpr.renderInputs` and
`evalVecInputs`, or the existing `renderWith`/`evalVecWith` adapters while retaining their tuned tensor-core/SIMD loops and math.
The portable GPU GEMM remains a conformance/prototype schedule; production
GEMMs are not switched to it by these contracts.

`FExpr` is also the native activation recipe representation. Its single
scalar/SIMD evaluator and source renderer preserve tree association, explicit
FMA and storage rounding. `native_math` supplies the existing target math
profiles, including the two cubic GELU orders and guarded projection GELU.
`fexpr.operand("weight")` and other operation-local names bind through
`evaluate`/`renderNative`; scalar/SIMD evaluation uses the `value` field to
establish its type. RMS binds `inverse` and `weight` directly. Native templates
also accept `fexpr.binding` for arbitrary named operands.

Typed `fexpr.Emission` template bindings insert those expressions into tuned
CUDA/Metal epilogues without moving barriers, scratch publication or rounding.

Whole-body sharing includes the native single/paired rotary program, packed
coordinate decoding, paged-key reconstruction, QK MMA, F16 page admission and
relative-attention forward/backward softmax phases. Forward activations reuse
the qualified native activation helper; CPU/CUDA/Metal standard tanh-GELU adjoints share
the ordered derivative tree while native guards and saved values remain local.
Small arithmetic expressions need not acquire a separate binding record when
the record is larger than the expression. Count every profile and replacement
owner before promoting another abstraction.

Normalization exports select their statistics algorithm explicitly: centered
two-pass, raw-moment tree, uncentered RMS and Welford are distinct contracts.
Serial and tree schedules stay separate; RMS weight/inverse association is
also a static policy. `layout.InterleavedRows` describes both CPU split views
and native split stores, while exported historical symbols retain their ABI.

`layout.Rows` specifies element offsets and strides. CPU copy/gather validates
all geometry and indices before storing; GPU callers use `prepareCopy` to validate the same
views, ids, flattened count and launch geometry before launch. Source/destination must not overlap. GPU copy
params contain eight 64-bit fields in declaration order, with an optional
u32 row-id buffer. RMS uses `antfly_tile_norm_params` with rows/dim and one
threadgroup per row; masked mean uses the same params and one thread per
column, with a positive i32 token mask. Empty masked selections return zero.

```sh
zig build test-tile-kernels test-linalg check-kernel-ownership
zig build tile-kernel-codegen -- --emit-portable /tmp/tile-contracts.metal
# From zig/pkg/inference, compile the retained hardware checker:
TMPDIR=/private/tmp clang -O3 -fobjc-arc -framework Foundation -framework Metal \
  -framework MetalPerformanceShaders scripts/check_metal_tile_programs.m -o /tmp/tile-check
/tmp/tile-check --portable /tmp/tile-contracts.metal
# .cu and .cl extensions select CUDA and OpenCL conformance programs.
```

## Whisper end to end

`whisper_model.zig` is the full Whisper encoder-decoder (conv stem,
transformer layers, cross-attention, KV-cached greedy decoding), with every
tensor op one of the tile kernels in `whisper_model.kernels`. It loads the
Hugging Face `model.safetensors` + `config.json` unchanged.

```sh
# CPU lowering; mel.f32 from scripts/tile/whisper_reference.py, or a 16 kHz WAV
zig build tile-whisper -- <hf-model-dir> <audio.wav|mel.f32> [--reference <dir>] [--profile]

# GPU templates (OpenCL dialect) on the CPU through PoCL
zig build tile-kernel-codegen -- --emit-opencl /tmp/tile_whisper_model.cl
python scripts/tile/run_whisper_opencl.py /tmp/tile_whisper_model.cl <hf-model-dir> <reference-dir>
```

The CPU lowering is `@Vector` code, so the `tile-whisper` step builds for
the host CPU unless `-Dcpu`/`-Dtarget` say otherwise; the repository's
default baseline x86-64 target (no AVX/FMA) is about 15x slower.

### The Metal hook

`whisper_logits.zig` keeps the ABI of the hand-written
`termite_whisper_logits_partials`, so on Metal it can replace that pipeline
(`ANTFLY_TILE_WHISPER_LOGITS=1`, built as its own library, falls back on
failure). The model kernels are generated for Metal and CUDA but no runtime
launches them yet; the OpenCL harness is the host-side reference for doing so.

## Production kernel families

Production ownership is registered in `families/native.zig`. Its operation-named
modules preserve the qualified native schedules already shared by inference.
`native_program.zig` and `source_template.zig` expand exact source templates at
comptime; names and static literals are parameters. They introduce no extra
device call, runtime branch, scratch allocation or launch.

The registry declares families with `region.metalFamily`, `cudaFamily`, or
`nativeFamily` for a custom header. `region.regions` flattens their typed source
namespaces once. Each owner exports contiguous `source_0`, `source_1`, etc.;
region names keep their existing numbered suffixes. Empty or gapped namespaces
are rejected at compile time.

Use `native_program.metal` to encode readable MSL at the Objective-C boundary;
`metalAt` retains a different native indentation where required. Keep host ABI
macro splices as raw parts of `native_program.join`, preserving their canonical
native owners instead of duplicating declarations.

The registry declares disjoint source regions. Existing `quant-kernel-codegen` regions
retain their owners; do not register a Tile region inside them. Shared arithmetic,
layout and synchronization helpers stay in their existing native program headers.
This is source specialization, distinct from the portable Tile IR above: the
portable lowerings are not substitutes for the tuned production kernels yet.
Historical device exports remain ABI adapters. New owner names describe the
operation, storage layout and schedule rather than a model.

`tile-kernel-codegen --check` also verifies the sorted `source_inputs.zig`
manifest. Both enabled backend cache identities include that manifest and every
Tile source; Metal additionally includes the opt-in generated logits header.
Disabled backends do not acquire a source identity build edge.

To add a specialization, extend the existing program family, register its exact
region once, and regenerate. Keep generated regions separate from other owners.
Validate against a frozen original and an independent reference, then compare
native resources and hardware timings. CPU emulation alone does not qualify a
GPU schedule. Use the shared model/native qualification harnesses documented
in `scripts/README.md` for additional device and performance comparisons.

Changing CUDA source means regenerating the checked-in PTX/fatbin/cubin
with the pinned toolkit (CUDA 13.2, V13.2.86). Without a local install, the
PyPI wheels reproduce the checked-in artifacts byte for byte:

```sh
pip install nvidia-cuda-nvcc==13.2.86 nvidia-nvvm==13.2.86 nvidia-cuda-runtime==13.2.86 nvidia-cuda-cccl==13.2.86
export CUDA_HOME=$(python -c 'import nvidia, os; print(os.path.join(nvidia.__path__[0], "cu13"))')
PATH=$CUDA_HOME/bin:$PATH scripts/regen-cuda-artifacts.sh --check --all   # or --write
```

## Commands

```sh
zig build check-tile-whisper                 # compile the prototype CPU CLI
zig build test-tile-kernels                  # lowerings vs. the scalar oracles
zig build tile-kernel-codegen                # checked-in sources are fresh
zig build tile-kernel-codegen -- --write     # regenerate them
```

`whisper_timestamps.zig` also checks the logits RowReduce against
`hostLogitsStats`, the existing host oracle for that kernel.

## Notes

- `@exp` on a `@Vector` lowers to one libm call per lane and Zig has no
  `erf`; `vmath.zig` has polynomial versions, and the GPU dialects emit the
  same polynomials so every backend evaluates the same GELU.
- The OpenCL dialect emulates the 32-lane warp sum through local memory
  (PoCL has no sub-groups) and runs attention one query per work-group.
  The logits RowReduce has no OpenCL dialect; the harness picks tokens on
  the host.
- `logsumexp` skips `-inf` inputs. The hand-written Metal kernel and
  `hostLogitsStats` turn a leading `-inf` into a NaN sum.
- Antfly's log-mel (`lib/audio`, used by the production Whisper pipeline)
  normalizes as `(v - (max - 4)) / 4`; the reference extractor uses
  `(v + 4) / 4`. On the JFK sample that shifts every value by `max / 4`
  (0.46) and leaves a further 0.13 RMS difference. The transcript is the
  same either way.
