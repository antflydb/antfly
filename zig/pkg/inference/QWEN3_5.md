# Qwen3.5 / Chandra OCR 2 Support

Target model: `datalab-to/chandra-ocr-2`

## Model Shape

Chandra OCR 2 is an image-text-to-text `qwen3_5` checkpoint with:

- Qwen3.5 text decoder, not plain Qwen3.
- Hybrid decoder layers: three `linear_attention` layers followed by one `full_attention` layer.
- Qwen3.5 RMSNorm weights stored as zero-centered weights that must be used as `1 + weight`.
- Full-attention RoPE with `partial_rotary_factor = 0.25`, `mrope_interleaved = true`, and `mrope_section = [11, 11, 10]`.
- Qwen3.5 vision tower using Qwen VL-style preprocessing:
  - `patch_size = 16`
  - `temporal_patch_size = 2`
  - `spatial_merge_size = 2`
  - `hidden_size = 1024`
  - `intermediate_size = 4096`
  - `out_hidden_size = 2560`
- Dynamic image token counts derived from resized image grids, not a fixed `mm_tokens_per_image`.

## Implemented Foundation

- `qwen3_5` and `qwen3_5_text` are parsed as their own GPT family.
- The parser records Qwen3.5 linear-attention settings:
  - `layer_types`
  - `full_attention_interval`
  - `linear_conv_kernel_dim`
  - linear key/value head dims and head counts
  - `attn_output_gate`
  - `mrope_interleaved`
  - `mrope_section`
- Qwen3.5 RMSNorm gets the same `1 + weight` adjustment needed by the HF implementation.
- The decoder has a Qwen3.5 linear-attention branch for `linear_attn.*` weights:
  - `in_proj_qkv`, `in_proj_z`, `in_proj_b`, `in_proj_a`
  - depthwise causal `conv1d`
  - `A_log` / `dt_bias`
  - recurrent Gated DeltaNet update
  - gated RMSNorm
  - `out_proj`
- Paged generation state now carries Qwen3.5 convolution and recurrent linear-attention state alongside the normal full-attention KV cache.
- Full-attention Qwen3.5 layers now apply the output gate split out of doubled `q_proj`.
- Qwen3.5 full-attention RoPE now honors `partial_rotary_factor` for full-attention layers.
- Qwen VL preprocessing already handles Chandra-style `processor_config.json` / `image_processor` wrappers.
- Qwen VL vision MLP sizing now accepts `vision_config.intermediate_size`, which Chandra uses instead of `mlp_ratio`.
- Native Qwen image prompt preparation now expands each tokenized `<|image_pad|>` to the dynamic prepared image token count and injects projected image embeddings at those offsets.
- Native Qwen generation inserts visual placeholder token IDs from parsed model config (`vision_start_token_id`, `image_token_id`, `vision_end_token_id`) instead of relying on hardcoded Qwen marker text.
- Native generation routes `qwen3_5` image prompts through the Qwen dynamic-image preparation path instead of the Gemma3 fixed-token image path.
- Web model discovery recognizes `qwen3_5` and `qwen3_5_text`.

## Fine-Tuning Readiness

Qwen3.5 is inference-ready enough for native smoke testing. Text-only
optimizer-backed fine-tuning now has graph coverage for hybrid
full-attention/linear-attention Qwen3.5 decoder layers, but it still needs
real-weight CPU smoke coverage before being declared production-ready. The
unified `termite finetune` recipe layer recognizes Qwen3.5/Chandra model paths
as `qwen3_5` instead of collapsing them to the Qwen2 route.

The training graph now has a Qwen3.5 text slice:

- Qwen3.5 configs can be translated into the Qwen training graph config.
- Full-attention Qwen3.5 layers build with doubled/gated `q_proj`.
- The graph applies Qwen3.5 RMSNorm as `1 + weight`.
- Full-attention RoPE honors `partial_rotary_factor`.
- The linear-attention projected stem now builds `in_proj_qkv`, `in_proj_z`,
  `in_proj_b`, `in_proj_a`, and a differentiable causal depthwise convolution
  without relying on `conv_general`.
- Qwen3.5 recurrent Gated DeltaNet state updates are represented as
  differentiable graph IR for static text training graphs.
- Gated per-head RMSNorm now has standalone graph IR with per-head variance,
  `1 + weight` handling, and the `in_proj_z` SiLU gate.
- `linear_attn.out_proj` is wired after the recurrent update and gated
  per-head RMSNorm.
- Text SFT/DPO/GRPO recipes with Qwen3.5 adapters route into the Qwen
  autodiff trainer and use Qwen3.5 linear-attention LoRA target modules.

This is still text-only. Chandra multimodal fine-tuning remains gated because
training data preparation still needs dynamic image-token expansion, image
embedding injection offsets, and vision/projector weight fixtures.

Qwen3.5 can be marked production fine-tuning-ready when the training path has:

1. Real-weight CPU smoke tests for text Qwen3.5 SFT/DPO/GRPO.
2. Numerical parity checks against the host Qwen3.5 recurrent implementation
   for tiny deterministic linear-attention fixtures.
3. Weight loading validation for all `linear_attn.*` tensors and Chandra
   vision/projector tensors.
4. Text-only SFT/DPO/GRPO parity tests against the Qwen3.5 graph before enabling
   multimodal recipes.
5. Multimodal SFT/DPO/GRPO data preparation that preserves dynamic Qwen image
   token counts and image embedding injection offsets.
6. CPU smoke tests in `test-finetune`, plus MLX/Metal smoke tests on a machine
   with enough memory for Chandra.

### Recurrent Gated DeltaNet Training Checklist

Qwen3.5 recurrent linear-attention now builds in graph IR. Remaining work is
parity and production hardening:

- [x] Define the graph IR tensor contract for projected Q/K/V, normalized Q/K,
  beta gates, A/dt gates, recurrent state input/output, and the per-token
  DeltaNet output.
- [x] Preserve Qwen3.5 linear-attention head layout, including distinct Q/K/V
  feature dims and grouped key/value heads.
- [x] Replace the host-only recurrent update in the training path with
  differentiable graph IR or a graph scan primitive whose state transitions are
  visible to autodiff.
- [x] Apply `A_log`, `dt_bias`, beta, and decay gates in the same order as the
  inference implementation.
- [ ] Reset or mask recurrent state at packed-example boundaries, sequence
  starts, and batch boundaries.
- [ ] Add tiny deterministic forward tests against the existing correctness
  implementation for single-token and multi-token inputs.
- [x] Add gradient checks that cover Q/K/V projections, beta gate, A/dt gate,
  recurrent state input, and recurrent output.
- [x] Integrate the recurrent update into Qwen3.5 linear-attention training
  layers before enabling text LoRA, DPO, or GRPO recipes.

### Gated RMSNorm Training Checklist

The gated RMSNorm work is part of the same readiness gate as recurrent
Gated DeltaNet; implementing only one side is not enough to enable Qwen3.5
fine-tuning.

- [ ] Define graph IR shapes between recurrent DeltaNet output and
  `linear_attn.out_proj`, including batch, sequence, head, and per-head feature
  axes.
- [x] Reduce RMS variance over the per-head feature axis only.
- [x] Apply Qwen3.5 zero-centered norm weights as `1 + weight`.
- [x] Apply the `in_proj_z` output gate with the same activation and
  multiplication order as inference.
- [ ] Preserve accumulation dtype, epsilon behavior, and output casting across
  CPU graph execution and later backend lowering.
- [ ] Add forward tests for gated and ungated references, zero-centered weights,
  non-zero norm weights, and small epsilon-sensitive values.
- [x] Add gradient construction coverage for norm weights, gate projection
  output, and recurrent output input.
- [ ] Add numerical gradient checks for norm weights, gate projection output,
  recurrent output input, and downstream `linear_attn.out_proj` input.
- [x] Wire gated RMSNorm after recurrent Gated DeltaNet and before
  `linear_attn.out_proj` in the training graph.
- [ ] Run real-weight CPU smoke tests for the recurrent update, gated RMSNorm,
  and output projection together.

## Remaining Work

1. Validate and optimize Qwen3.5 Gated DeltaNet / linear attention.
   - Current implementation uses existing backend projections plus a host recurrent DeltaNet core for correctness.
   - Add Metal/WASM kernels for convolution, recurrent update, and gated RMSNorm before expecting production-speed Chandra generation.
   - Add rollback/snapshot support for linear recurrent state before enabling speculative decode with Qwen3.5.

2. Finish Qwen3.5 full-attention details.
   - Validate Qwen3.5 mRoPE behavior against HF for text-only and image-text prompts.
   - Map full-attention KV cache layer indices separately from absolute layer indices.

3. Finish Qwen3.5 vision parity.
   - Confirm all Chandra vision weight names match the current `qwen2vl_vision.zig` lookup path.
   - Validate patch row ordering, rotary embedding layout, and patch merger output against HF.
   - Add golden first-token tests using a small image fixture.

4. Finish Qwen-specific multimodal generation parity.
   - Validate native prompt embedding injection against HF for one image and multiple images.
   - Add browser-side support for Qwen dynamic image token counts; current browser integrated image path is still fixed-token/Gemma-shaped.

5. Wire export and browser support.
   - Validate Qwen3.5 linear-attention tensors are preserved during safetensors/GGUF export.
   - Add browser-side dynamic Qwen image preprocessing or route browser Qwen3.5 multimodal through a WASM API that accepts image bytes.

## Current Smoke Status

- Pulled `datalab-to/chandra-ocr-2` to `~/.termite/models/datalab-to/chandra-ocr-2`.
- Native text generation now gets through Chandra prefill and emits a first token with explicit host memory budget:
  - `termite generate ~/.termite/models/datalab-to/chandra-ocr-2 "hello" --backend native --max-tokens 1 --host-budget-mb 14000 --combined-budget-mb 16000 --scratch-budget-mb 1024`
- Native image generation now completes the Qwen prompt expansion, Chandra vision tower, multimodal embedding injection, and first-token prefill:
  - `termite generate ~/.termite/models/datalab-to/chandra-ocr-2 "Read the image." --image testdata/image/png/basic/red-2x2.png --backend native --max-tokens 1 --host-budget-mb 14000 --combined-budget-mb 18000 --scratch-budget-mb 2048`
- Current native CPU path is correctness-oriented and slow on the full bf16 checkpoint; production use still needs Metal/WASM kernels or a quantized/exported artifact.

## Acceptance Tests

- Parse Chandra `config.json` and assert:
  - family is `qwen3_5`
  - hidden size is 2560
  - image token id is 248056
  - full-attention interval is 4
  - Qwen3.5 linear attention is detected
  - vision patch size is 16
  - vision intermediate size is 4096
- Parse Chandra `processor_config.json` and assert dynamic Qwen preprocessing values.
- Run a Qwen3.5 text forward on a fixture with no linear-attention layers.
- Run Chandra first-token parity against HF for one OCR image.
- Run a paged prefill/decode smoke test that verifies Qwen3.5 linear layers use recurrent state while full-attention layers use KV cache.
