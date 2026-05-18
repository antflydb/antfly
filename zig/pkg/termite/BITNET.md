# BitNet Support

This tracks the work needed for Termite to run Microsoft BitNet-style GGUF
models, starting with `microsoft/bitnet-b1.58-2B-4T-gguf`.

BitNet support is not just another post-training quantization format. The
released model uses BitLinear layers and was trained around native W1.58A8
execution: ternary weights `{-1, 0, +1}` with 8-bit activations. A slow
correctness path is possible, but useful support needs BitNet-specific matmul
kernels rather than treating the file as a normal LLaMA-family GGUF.

## Target Models

- `microsoft/bitnet-b1.58-2B-4T-gguf`
  - `ggml-model-i2_s.gguf`
- Related upstream runtime:
  - `microsoft/BitNet`
  - vendored `llama.cpp` fork used by BitNet

## Observed GGUF Header

Parsed from the first 64 MiB of
`microsoft/bitnet-b1.58-2B-4T-gguf/ggml-model-i2_s.gguf`. The tensor table and
metadata fit inside that range.

- GGUF version: 3.
- Tensor count: 332.
- Metadata entries: 24.
- Header bytes: 8,351,342.
- `general.architecture`: `bitnet-b1.58`.
- `general.name`: `bitnet2b`.
- `general.file_type`: 40.
- `general.quantization_version`: 2.
- `tokenizer.ggml.model`: `gpt2`.

Tensor-type histogram:

| Type | Name | Count | Example |
| --- | --- | ---: | --- |
| 0 | `F32` | 121 | `blk.0.attn_norm.weight` `[2560]` |
| 1 | `F16` | 1 | `token_embd.weight` `[2560, 128256]` |
| 36 | `I2_S` | 210 | `blk.0.ffn_down.weight` `[6912, 2560]` |

Termite now recognizes type id 36 as `I2_S`. In BitNet's vendored GGML enum,
the relevant type ids are:

| Type | Name |
| ---: | --- |
| 36 | `I2_S` |
| 37 | `I8_S` |
| 38 | `TL1` |
| 39 | `TL2` |

## Current Termite State

- GGUF metadata parsing recognizes `general.architecture = bitnet-b1.58` as a
  BitNet GPT-family variant.
- `src/gguf/tensor_types.zig` defines `I2_S`, `I8_S`, and `TL1`.
- Type id 39 is dialect-aware: upstream GGML parses it as `MXFP4`, while BitNet
  GGUF metadata parses it as `TL2`.
- The quant codec has a slow CPU materialization and row-dequant path for
  `I2_S`.
- Native CPU quantized linear has a slow direct `I2_S` W1.58A8 fallback:
  per-row absmax INT8 activation quantization plus ternary weight dot products.
  Resident quantized BitNet projection weights do not need full tensor expansion
  for correctness.
- MLX/Metal has a direct `I2_S` kernel with the same activation-quantized
  semantics.
- Real-model MLX/Metal validation now exists for
  `microsoft/bitnet-b1.58-2B-4T-gguf/ggml-model-i2_s.gguf`: `termite generate`
  on `--backend mlx` completes end-to-end using the direct `I2_S` path.
  On this machine and build, a 1-token prompt completed in about 4.0s total
  including model load, and an 8-token prompt completed in about 2.7s total
  after a warm rebuild. This is materially faster than the native CPU path,
  which was about 25.8s for a single token in the same environment.
- The current MLX/Metal gap is no longer the BitNet quant formats themselves.
  `I2_S`, `I8_S`, `TL1`, and `TL2` now all have direct device-native paths.
  The remaining GPU work is deeper decode-path optimization. Termite now has
  raw whole-token Metal RMSNorm coverage for the BitNet layer-0 norms, and the
  raw entry path can start from the BitNet token embedding instead of the
  GPT-2 absolute-position path. The larger gap is still quantized layer-0
  weight residency: the BitNet attention/FFN linears are quantized GGUF
  tensors, so they still run through the existing quantized MLX execution path
  rather than a persistent raw whole-token decode runtime.
- WebGPU/WASM has an `I2_S` quantized matmul bridge and WGSL kernel with the
  same activation-quantized semantics. The shader still needs browser-side
  WebGPU validation on a real device.
- `I8_S` has signed-int8 CPU materialization plus native generic quantized
  linear execution, and MLX/Metal now has a direct device-native path for the
  same signed-int8 layout.
- `TL1` and `TL2` have CPU dense materialization fallbacks and native direct
  quantized linear execution for Microsoft BitNet's generated/preset LUT
  layouts. The direct native path builds activation-side LUTs per input row,
  decodes from packed TL bytes, and uses portable vector accumulation without
  expanding the full weight tensor.
- `TL1` now also has a direct MLX/Metal kernel with the same activation-
  quantized semantics as the CPU path, validated against a synthetic GGUF-
  layout tensor using one of the upstream preset matrix shapes.
- `TL2` now also has a direct MLX/Metal kernel with the same activation-
  quantized semantics as the CPU path, including the padded GGUF scale offset,
  validated against a synthetic GGUF-layout tensor using one of the upstream
  preset matrix shapes.
- The shared `TL2` codec/materialization path now also has explicit coverage
  for preset shapes with a nonzero tail-pair section, and the MLX/Metal parity
  tests now exercise multi-row activation quantization and that tail layout.
- The TL1/TL2 native path now follows the upstream table-lookup shape, but it
  still needs real-model validation and platform-tuned AVX/NEON kernels before
  treating it as performance-complete.
- `TL1`/`TL2` layout support is limited to the upstream preset matrix shapes
  from `setup_env.py`/`codegen_tl*.py`: `bitnet_b1_58-large`,
  `bitnet_b1_58-3B`/`BitNet-b1.58-2B-4T`, and the shared Llama3/Falcon preset
  shapes.
- Existing GPT-family code now covers the architecture-level pieces that can be
  wired independently of `I2_S`:
  - LLaMA-style GQA/RoPE metadata.
  - no-bias attention projections.
  - gated FFN with ReLU squared activation.
  - `attn_sub_norm` before attention output projection.
  - `ffn_sub_norm` before FFN down projection.
  - tied token embedding / LM head.

## Work Items

- [x] Record the target model and observed GGUF header.
- [x] Confirm the immediate blockers: unsupported architecture
  `bitnet-b1.58` and unsupported GGUF tensor type `I2_S`.
- [x] Add GGUF type ids and byte sizing for `I2_S`, `I8_S`, and `TL1`.
- [x] Add dialect-aware GGUF type parsing and byte sizing for `TL2`.
- [x] Add tests that verify type id 36 is recognized as `I2_S`.
- [x] Allow `I2_S` through GGUF inspection compatibility checks.
- [x] Add a `bitnet`/`bitnet-b1.58` architecture family or a GPT-family variant
  that preserves BitNet-specific defaults.
- [x] Parse BitNet GGUF metadata into a Termite config:
  - hidden size
  - layer count
  - attention head count
  - KV head count
  - feed-forward size
  - RoPE settings
  - vocab size and tokenizer metadata
- [x] Normalize BitNet GGUF tensor names into Termite's internal weight keys.
- [x] Implement a slow correctness path for `I2_S`:
  - full tensor materialization to f32
  - row dequantization
  - native CPU dense-equivalent linear
- [x] Add synthetic `I2_S` block tests using known ternary values.
- [x] Add native direct `I2_S` dot product for correctness without full tensor
  materialization.
- [x] Add BitLinear execution with activation quantization semantics for the
  native CPU `I2_S` correctness path.
- [x] Add an MLX/Metal direct `I2_S` kernel with activation quantization.
- [x] Validate real-model MLX/Metal `I2_S` generation on
  `microsoft/bitnet-b1.58-2B-4T-gguf/ggml-model-i2_s.gguf`.
- [x] Add WebGPU direct `I2_S` kernels.
- [ ] Validate WebGPU `I2_S` shader dispatch in browser/worker mode.
- [ ] Validate the activation-quantized path against `bitnet.cpp` logits for a
  fixed prompt.
- [x] Add `I8_S` signed-int8 materialization and native CPU generic quantized
  linear execution.
- [x] Add an MLX/Metal direct `I8_S` kernel.
- [x] Add `TL1` dense materialization fallback for upstream preset LUT layouts.
- [x] Add `TL2` dense materialization fallback for upstream preset LUT layouts,
  including the padded scale offset used by BitNet's converter/runtime.
- [x] Add native direct TL1/TL2 quantized linear execution to avoid full dense
  weight materialization.
- [x] Add an MLX/Metal direct `TL1` kernel.
- [x] Add an MLX/Metal direct `TL2` kernel.
- [ ] Validate `TL1` and `TL2` materialization against real GGUF tensors or
  reference `bitnet.cpp` logits.
- [x] Replace the scalar TL1/TL2 direct path with activation-side LUT kernels
  and portable vector accumulation.
- [ ] Add platform-tuned AVX/NEON TL1/TL2 kernels if benchmarks show portable
  vector accumulation is still too slow for practical local execution.
- [ ] Decide how to handle non-preset TL1/TL2 shapes. The GGUF tensor type does
  not encode `BM`/`BK`/`bmm`, so supporting arbitrary generated layouts needs
  side metadata or explicit shape/config registration.
- [x] Add ReLU squared activation support if it is not already expressible
  through existing backend ops.
- [x] Add subln normalization support if existing norm ops do not match the
  released BitNet model.
- [x] Add MLX/Metal kernels for useful local execution.
- [ ] Add WebGPU/WASM kernels only after CPU and MLX correctness are verified.
- [ ] Add a smoke command for
  `microsoft/bitnet-b1.58-2B-4T-gguf/ggml-model-i2_s.gguf` that verifies:
  - architecture detection succeeds
  - no unknown tensor types
  - no unmapped text tensors
  - short generation completes
  - BitNet-specific quantized execution counters are hit

## Implementation Notes

Treat BitNet as an architecture plus quantization/runtime project, not as a
single GGUF tensor-type patch.

The conservative bring-up order should be:

1. Parse the file without unknown tensor types.
2. Detect `bitnet-b1.58` as a supported architecture.
3. Load all weights and normalize all tensor names.
4. Add a slow CPU correctness path, even if it is not fast.
5. Validate logits or short generation against `bitnet.cpp` for a fixed prompt.
6. Add direct native CPU kernels.
7. Add MLX/Metal and WebGPU kernels.

The official BitNet docs warn that generic transformer execution paths do not
deliver the efficiency benefits of BitNet. Termite should keep that distinction
explicit: compatibility means the model runs correctly; complete support means
the BitNet-specific quantized path is actually used.

## References

- BitNet repository: <https://github.com/microsoft/BitNet>
- BitNet GGUF model: <https://huggingface.co/microsoft/bitnet-b1.58-2B-4T-gguf>
- Vendored GGML enum with `I2_S`/`TL*` ids:
  <https://github.com/Eddie-Wang1120/llama.cpp/blob/1f86f058de0c3f4098dedae2ae8653c335c868a1/ggml/include/ggml.h>
