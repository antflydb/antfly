# Tile kernels (prototype)

Describe each kernel once as comptime Zig data and lower it to every backend:

| Lowering | How | Output |
| --- | --- | --- |
| CPU | walks the description at comptime and emits `@Vector` code | compiled in, no text |
| Metal | renders MSL at comptime | `src/ops/metal/generated/tile_*.metal` |
| CUDA | renders CUDA C++ at comptime | `src/ops/cuda/generated/tile_*.cu` |
| OpenCL | test dialect of the GPU templates, run on the CPU with PoCL | written on demand, not checked in |
| Oracle | scalar, f64 accumulation | tests only |

Primitives:

| File | Primitive | Notes |
| --- | --- | --- |
| `tile.zig`, `cpu.zig`, `gpu.zig` | `RowReduce` | per-element masks, argmax / log-sum-exp channels (logits post-processing) |
| `gemm.zig` | `Gemm` | `x · wᵀ` with an `FExpr` epilogue: bias, GELU, residual, scaling fuse into the store |
| `norm.zig` | `LayerNorm` | two-pass mean / variance per row |
| `attention.zig` | `Attention` | flash-style online softmax; self, causal (KV cache) and cross attention |
| `layout.zig` | `Im2col`, `Embed` | conv-as-GEMM unrolling; token + position gather |
| `fexpr.zig`, `vmath.zig` | epilogues, math | `exp`/`erf`/GELU polynomials shared by all backends |

`gpu_model.zig` renders the model primitives and documents their GPU launch
contract (binding order, params struct, grid, threads).

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
