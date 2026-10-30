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

On whisper-tiny and the JFK sample, both runs produce the same tokens as
Hugging Face transformers (f32) and the same transcript. The CPU lowering's
encoder output is within 5.4e-4 of the reference (values up to 17) and the
first-step logits within 4.1e-5. The OpenCL run of the GPU templates is
within 1.2e-3 and 1.1e-4 (block-tiled sums round differently). On a 4-core x86-64 (AVX-512) sandbox the
CPU lowering runs the 30 s encoder in ~0.9 s and decodes at ~12 ms/token.

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

Most hand-written kernels in `metal_kernels.m`, `inference_cuda_kernels.cu`
and a few `src/ops/cuda/kernels/*.cuh` headers are rendered from Zig
descriptions into marker-delimited regions
(`tile-codegen:begin/end <family> family`). `families.zig` lists every
region, one module per slice under `families/`:

| Slice | Generators | What they describe |
| --- | --- | --- |
| core | `rms_norm.zig`, `argmax.zig` | RMSNorm, LayerNorm, argmax (Metal + CUDA) |
| `reduce` | `reduce_*.zig` | embedding lookup, RoPE, row reductions, scans, gated norms, samplers |
| `elementwise` | `elementwise.zig` | GELU backward, broadcast ops and AdamW, LoRA, LightOn gate wrappers |
| `quant_metal` | `quant_metal_*.zig` | Metal quantized and dense matmul/GEMV, quant get/set/cpy rows |
| `quant_cuda` | `quant_cuda.zig` | CUDA quantized and dense linear, tensor-core epilogues, Q8_1 quantizers |
| `attention` | `attention_*.zig` | split-K GQA decode and paired attention variants (Metal + CUDA) |
| `model` | `model_*.zig` | LightOnOCR-3 batch GEMV, DeBERTa/GLiNER kernels (one MSL source for both backends), small-M dot_general |

Kernel names, buffer slots, params structs and CUDA argument order are
unchanged, so host dispatch is untouched. Each generated kernel is either
character-identical to the hand-written kernel it replaced
(`compare_kernel_text.py`) or bit-identical on the CPU harness below; the
compiled CUDA PTX of every kernel is unchanged. Kernels with no structure to
share stay hand-written.

`scripts/tile/check_kernels.py` checks a family against the previous
hand-written kernels on the CPU, with no GPU: it compiles the old and new
sources as C++ against a small Metal or CUDA shim, runs every block with one
OS thread per GPU thread (real barriers, emulated `simd_sum` and shuffles)
over the shapes in `scripts/tile/kernel_specs.py`, and requires
bit-identical outputs. Argmax inputs force ties, NaN, `-inf` and "no
candidate" ids.

```sh
python3 -I scripts/tile/check_kernels.py <metal|cuda> <family> <old file> <new file>
CHECK_FLAGS="-O2 -march=native -ffp-contract=fast" python3 -I scripts/tile/check_kernels.py ...
```

Other checks: `compare_kernel_text.py <metal|cuda> <old> <new>` lists
kernels whose text changed, and `check_source_order.py <metal_kernels.m>
--baseline <old>` catches a generated region placed above a helper it uses
(MSL is only compiled on macOS).

To hand another family to the generator: describe it, then run
`scripts/tile/adopt_kernels.py <family> <kernels...>` (removes the
hand-written kernels and leaves an empty region), add the region to
its slice module under `families/`, `zig build tile-kernel-codegen -- --write`, add a spec,
and check against the old file.

## MSL templates for near-copy kernels

Hand-written Metal kernels that differed only in integer constants or
scalar types are folded into MSL templates with
`template [[host_name("...")]]` instances, so pipeline names stay the same.
`scripts/tile/verify_msl_templates.py <old> <new>` expands every instance
and requires it to equal the old kernel text exactly.

CUDA kernels can't be `extern "C"` templates, so near-copy CUDA kernels
keep their exact signatures as one-line wrappers around a
`__device__ __forceinline__` `*_impl` template.
`scripts/tile/verify_cuda_templates.py <old> <new>` checks each wrapper
expands to its old body; the compiled PTX is also identical apart from the
mangled names of `__shared__` arrays.

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
