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

## Production RMSNorm family (`rms_norm.zig`)

The 17 Metal RMSNorm kernels in `metal_kernels.m` and the 8 in
`inference_cuda_kernels.cu` are generated from descriptions into
marker-delimited regions (`tile-codegen:begin/end rms_norm family`) with
unchanged names, buffer slots and params structs, so host dispatch is
untouched.

- Metal is generated freely. `scripts/tile/check_metal_rms_norm.py <old.m>
  <new.m>` compiles both versions' MSL as C++ against a small shim, runs every
  kernel with one OS thread per GPU thread (real barriers and `simd_sum`)
  over a grid of shapes and threadgroup sizes, requires bitwise-equal
  outputs, and checks two kernels against an f64 reference.
- CUDA renders byte-identical to the previous hand-written text, so the
  checked-in PTX/fatbin/cubin stay valid without a CUDA toolkit. Merging the
  CUDA table into the Metal one (one description per kernel) needs an
  artifact regeneration with the pinned CUDA 13.2 toolchain.

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
