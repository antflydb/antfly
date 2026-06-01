# Unsloth Dynamic GGUF Support

This tracks the work needed for Antfly inference to run Unsloth Dynamic GGUFs for
Gemma 4, starting with `UD-Q4_K_*` variants of
`unsloth/gemma-4-26B-A4B-it-GGUF`.

## Target Models

- `unsloth/gemma-4-26B-A4B-it-GGUF`
  - `gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf`
  - `gemma-4-26B-A4B-it-UD-Q4_K_M.gguf`
  - `gemma-4-26B-A4B-it-UD-Q4_K_S.gguf`
- Companion projector:
  - `mmproj-BF16.gguf`, `mmproj-F16.gguf`, or `mmproj-F32.gguf`

The ggml-org Gemma 4 GGUFs use the same `gemma4` architecture path, but their
published 4-bit artifact is standard `Q4_K_M`. The Unsloth Dynamic files are
file-level quantization recipes: `UD-Q4_K` does not necessarily mean every
tensor has GGML type `Q4_K`. A real tensor histogram is required for every target
file before declaring support complete.

## Current State

- GGUF metadata parsing already recognizes `general.architecture = gemma4`.
- Gemma 4 config parsing covers shared KV, per-layer GQA, sliding attention,
  p-RoPE, MoE expert counts, shared experts, and PLE metadata.
- GGUF tensor-name normalization maps Gemma 4 attention, dense FFN, shared
  expert, routed expert, router scale, expert output scale, and PLE tensors.
- Lazy quantized storage exists, including packed MoE expert views.
- K-quant CPU direct matmul exists for `Q2_K`, `Q3_K`, `Q4_K`, `Q5_K`, `Q6_K`,
  and `Q8_K`.
- Legacy quantized CPU fallback coverage exists for `Q4_1`, `Q5_1`, and
  `Q8_1`: full dequantize, row dequantize, dense native linear, and packed
  expert native linear dispatch.
- Native Metal host-input quantized linear coverage exists for legacy `Q5_1`,
  including Gemma 4 packed MoE down projections. `Q5_0`, `Q8_1`, and `IQ4_NL`
  use the same provider wrapper path.
- GGUF inspection compatibility now allows `Q4_1`, `Q5_1`, and `Q8_1` so
  Unsloth Dynamic files are not rejected before runtime fallback selection.

## Observed `UD-Q4_K` Headers

These were parsed from the first 64 MiB of each Hugging Face file. The tensor
table and metadata fit entirely inside that range; full generation smoke still
requires downloading the complete 16-17 GiB model file.

### `gemma-4-26B-A4B-it-UD-Q4_K_M.gguf`

- File size from Hugging Face HEAD: 16,868,240,704 bytes.
- GGUF version: 3.
- Tensor count: 658.
- Metadata entries: 60.
- Header bytes: 15,822,183.
- Architecture: `gemma4`.
- `general.file_type`: 15.
- `general.quantization_version`: 2.
- Unknown tensor types: 0.

Tensor-type histogram:

| Type | Name | Count |
| --- | --- | ---: |
| 0 | `F32` | 392 |
| 7 | `Q5_1` | 30 |
| 8 | `Q8_0` | 206 |
| 12 | `Q4_K` | 30 |

All 30 `Q5_1` tensors are `blk.N.ffn_down_exps.weight` with shape
`[704, 2816, 128]`.

### `gemma-4-26B-A4B-it-UD-Q4_K_XL.gguf`

- File size from Hugging Face HEAD: 17,090,276,672 bytes.
- GGUF version: 3.
- Tensor count: 658.
- Metadata entries: 60.
- Header bytes: 15,822,183.
- Architecture: `gemma4`.
- `general.file_type`: 15.
- `general.quantization_version`: 2.
- Unknown tensor types: 0.

Tensor-type histogram:

| Type | Name | Count |
| --- | --- | ---: |
| 0 | `F32` | 392 |
| 7 | `Q5_1` | 28 |
| 8 | `Q8_0` | 208 |
| 12 | `Q4_K` | 29 |
| 13 | `Q5_K` | 1 |

The `Q5_1` tensors are `blk.N.ffn_down_exps.weight` with shape
`[704, 2816, 128]`, except layers 1 and 29 are `Q8_0`. The single `Q5_K` tensor
is `blk.1.ffn_gate_up_exps.weight` with shape `[2816, 1408, 128]`.

## Work Items

- [x] Document the Unsloth Dynamic target and support matrix.
- [x] Make `antfly inference pull ...:gguf` auto-selection prefer Unsloth
  `UD-Q4_K_*` files when they are present.
- [x] Use `antfly inference smoke --inspect-only` as the lightweight GGUF inspection
  helper. It reports the exact tensor-type histogram, unsupported tensor types,
  missing required tensors, and unmapped GGUF tensor names for a target model
  directory.
- [x] Run inspection on `UD-Q4_K_M` and `UD-Q4_K_XL`; record the tensor-type
  histogram here.
- [x] Close codec/native gaps from the observed histogram. The blocking observed
  gap was legacy `Q5_1` in packed Gemma 4 MoE down projections.
- [x] Add CPU correctness fallback coverage for legacy `Q5_1` and `Q8_1`
  tensors: full dequantize, row dequantize through the shared row path, direct
  native linear, and packed-expert native linear dispatch.
- [ ] Ensure every supported tensor type has a correctness fallback:
  full dequantize, row dequantize, dense linear fallback, and packed-expert
  packed-row fallback.
- [ ] Ensure every hot path has fast backend support where practical:
  native direct matmul, MLX device-native linear, MLX grouped MoE, and optional
  WebGPU dispatch.
- [ ] Run and record the real text smoke for
  `unsloth/gemma-4-26B-A4B-it-GGUF:gguf:UD-Q4_K_M` using the recipe below.
  It must verify:
  - no unsupported tensor types
  - no unmapped Gemma 4 text tensors
  - a short text-only generation completes
  - quantized execution counters are hit
- [ ] Add a multimodal smoke with the companion `mmproj` once text-only is
  stable.

## Smoke Recipe

`antfly inference smoke` is the canonical command for this validation. No separate
Unsloth-specific smoke command is needed.

Pull the target GGUF:

```sh
antfly inference pull unsloth/gemma-4-26B-A4B-it-GGUF:gguf:UD-Q4_K_M
```

Inspect the downloaded model without running generation:

```sh
antfly inference smoke ~/.antfly/inference/models/unsloth/gemma-4-26B-A4B-it-GGUF "hello" --inspect-only
```

Expected inspection conditions:

- `unsupported_tensor_types=none`
- `missing_required_tensors=none`
- no unexpected `unmapped_gguf_tensor_names`
- tensor histogram matches the recorded `UD-Q4_K_M` header, allowing for
  upstream file revisions

Run a short generation smoke:

```sh
antfly inference smoke ~/.antfly/inference/models/unsloth/gemma-4-26B-A4B-it-GGUF "hello" --max-tokens 4
```

The default backend is `auto`. Do not force `--backend native` for the normal
smoke; `auto` uses the best compiled backend available and is the closest path
to how users run the model. Use `--backend native` only when intentionally
validating CPU fallback behavior, and use `--backend metal` or `--backend mlx`
when isolating those backend-specific paths.

For the 26B `UD-Q4_K_M` artifact, the local Metal smoke needs an explicit
backend budget because the weight file is about 16 GiB and the default Metal
budget is lower:

```sh
antfly inference smoke ~/.antfly/inference/models/unsloth/gemma-4-26B-A4B-it-GGUF "hello" \
  --backend metal --backend-budget-mb 18000 --max-tokens 4
```

Local result:

- `native backend=metal`
- `chat_template=true`
- `unsupported_tensor_types=none`
- `missing_required_tensors=none`
- `unmapped_gguf_tensor_names=none`
- `finish_reason=length tokens=4`
- generated text remained nonsensical (`Sund sameanja same` under the local
  validator run), so this is only an execution smoke pass, not a quality or
  logits-parity pass
- Metal API and GPU validation reported no shader validation failure or crash in
  the local 4-token debug bundle

The smoke still reports packed MoE expert tensors separately as
`packed_moe_expert_tensors`; those are expected Gemma 4 expert-pack samples,
not unmapped decoder weights.

The remaining proof gap is the quantized execution assertion. If `antfly inference smoke`
does not print enough runtime counters to prove direct quantized execution for
this model, add or expose those counters in smoke output before marking the real
text smoke complete.

## Implementation Notes

`UD-Q4_K_*` should be treated as a model-file preference, not a new GGML tensor
type. Antfly inference support is complete only when the actual tensor types inside the
selected file are understood by the loader and by the execution path chosen at
runtime.

Keep the fallback path conservative:

1. Parse GGUF tensor type and block size.
2. Dequantize full tensors and individual rows correctly.
3. Execute quantized linear directly on CPU for correctness and memory safety.
4. Add MLX/Metal and WebGPU kernels only after the CPU path is verified.

For Gemma 4 26B A4B, packed MoE behavior is part of correctness. Densifying
packed expert tensors can make the model appear supported while exceeding the
runtime budget, so smoke tests need to assert that quantized storage and direct
quant execution are actually used.
