# Termite Quantization Layout

This document describes the publishing layout used by `termite quantize` for
ClipClap model variants.

## Goals

- Keep the base Hugging Face model repo as the canonical model repo.
- Treat the existing ONNX files as the default F32 variant.
- Write alternate ONNX and GGUF variants into the same repo using stable,
  unambiguous file names.
- Make `--output` a staging directory for the same repo layout, not a different
  packaging mode.
- Avoid making a quantized variant the default model just because it was
  generated last.

## File Naming

The default F32 artifacts are unsuffixed.

```text
text_model.onnx
text_model.onnx.data
visual_model.onnx
visual_model.onnx.data
audio_model.onnx
audio_model.onnx.data
text_projection.onnx
text_projection.onnx.data
visual_projection.onnx
visual_projection.onnx.data
audio_projection.onnx
audio_projection.onnx.data
```

ONNX quantized variants insert the canonical format suffix before `.onnx`.
The external data file uses the full ONNX file name plus `.data`.

```text
text_model.Q8_0.onnx
text_model.Q8_0.onnx.data
visual_model.Q8_0.onnx
visual_model.Q8_0.onnx.data
audio_model.Q8_0.onnx
audio_model.Q8_0.onnx.data
text_projection.Q8_0.onnx
text_projection.Q8_0.onnx.data
visual_projection.Q8_0.onnx
visual_projection.Q8_0.onnx.data
audio_projection.Q8_0.onnx
audio_projection.Q8_0.onnx.data
```

The default GGUF pair is also unsuffixed.

```text
clipclap-clip.gguf
clipclap-clap.gguf
```

GGUF quantized variants use the same suffix convention.

```text
clipclap-clip.Q4_K.gguf
clipclap-clap.Q4_K.gguf
clipclap-clip.Q8_0.gguf
clipclap-clap.Q8_0.gguf
```

Format suffixes are canonicalized to uppercase GGUF-style names such as
`Q4_K`, `Q8_0`, and `Q8_K`.

## CLI Behavior

By default, ClipClap quantization writes into the source model directory.

```bash
termite quantize ~/.termite/models/antflydb/clipclap \
  --target gguf \
  --format q4_k
```

This writes:

```text
~/.termite/models/antflydb/clipclap/clipclap-clip.Q4_K.gguf
~/.termite/models/antflydb/clipclap/clipclap-clap.Q4_K.gguf
~/.termite/models/antflydb/clipclap/termite_variants.json
```

Use `--format none` to materialize the default F32 GGUF pair:

```bash
termite quantize ~/.termite/models/antflydb/clipclap \
  --target gguf \
  --format none
```

This writes:

```text
clipclap-clip.gguf
clipclap-clap.gguf
termite_bundle.json
termite_variants.json
```

`termite_bundle.json` is written only for the unsuffixed GGUF default. That
keeps default model loading stable: generating `Q4_K` does not make `Q4_K` the
default model.

For ONNX Q8:

```bash
termite quantize ~/.termite/models/antflydb/clipclap \
  --target onnx \
  --format q8_0
```

This adds the `*.Q8_0.onnx` and `*.Q8_0.onnx.data` files next to the default
F32 ONNX files.

`--output` stages the same single-repo layout elsewhere.

```bash
termite quantize ~/.termite/models/antflydb/clipclap \
  --target gguf \
  --format q4_k \
  --output /tmp/clipclap
```

The output directory is suitable for inspection or upload after any additional
variants have been generated there.

## Variant Index

`termite_variants.json` is generated from files present in the repo directory.
It records the default ONNX files and any complete ONNX or GGUF variants found.
It is intentionally separate from `model_manifest.json`, which continues to
describe the default model.

The runtime loader can later use `termite_variants.json` for explicit variant
selection without changing default loading behavior.

## Hugging Face Upload

The intended publishing flow is to upload the model directory itself:

```bash
hf upload antflydb/clipclap ~/.termite/models/antflydb/clipclap .
```

For large repos or interrupted uploads, use Hugging Face Hub's resumable large
folder upload command against the same directory.

## General Artifact Model

Termite still separates three concerns:

- Source format: where the original weights and graph came from.
- Graph format: how execution is represented when the artifact includes a
  graph.
- Weight format: how parameters are stored and quantized.

The desired support matrix is:

| Source | Emit ONNX | Emit GGUF | Emit safetensors |
| --- | --- | --- | --- |
| ONNX | yes | yes, when the ONNX initializers can be mapped to a Termite architecture or graph bundle | yes, for extracted initializers |
| GGUF | yes, when a graph/architecture wrapper exists | yes | yes, for dense dequantized export or supported packed tensors |
| safetensors | yes, when a graph/architecture wrapper exists | yes | yes |

Task metadata stays in `model_manifest.json`; the artifact format does not
define which tasks the model can serve.

## ONNX Q8_0 Format

The first supported ONNX quantization format is weight-only block Q8:

- candidate tensors must be floating-point, rank >= 2, and have a last
  dimension divisible by 32
- tensors smaller than the threshold remain dense
- each 32-value block stores unsigned 8-bit values, f32 scales, and a zero
  point of 128
- the exported ONNX model reconstructs the original parameter name with an ONNX
  `DequantizeLinear` node using opset 21 `block_size`

This shrinks `.onnx.data` files while staying portable. Backends that only
understand the graph runtime can execute the dequantization graph. Later
backends can fuse or keep Q8 weights resident directly.

## Generic CLI

```text
termite export <model-dir> [--target onnx|gguf|safetensors] [--format <format>] [--output <path>]
termite quantize <model-dir> [--target onnx|gguf|safetensors] [--format <q-format>] [--output <path>]
```

`termite export` is the generic artifact conversion command. Quantization is a
conversion option, not a separate artifact family. `termite quantize` remains as
a convenience/compatibility command over the same dispatcher for users who are
explicitly asking for a quantized variant.

Targets:

- `--target onnx` creates ONNX graph variants. For ClipClap, the default output
  is the single-repo suffixed layout described above.
- `--target gguf` delegates to the GGUF exporter and writes a GGUF artifact or
  bundle. For ClipClap, the default output is the single-repo paired layout.
- `--target safetensors` writes a safetensors artifact or bundle. This is the
  right target for extracted ONNX initializers and for users who want a
  tensor-only artifact while keeping graph/config metadata next to it.

Current implementation status:

- `--target onnx` is implemented for ONNX Q8_0 variants.
- `--target gguf` is implemented for GGUF-exportable safetensors/GGUF-backed
  model families and ClipClap paired GGUF variants.
- `--target safetensors` is implemented for dense tensor export from ONNX,
  GGUF, and safetensors-backed sources. Packed q4/q5 safetensors variants are
  planned.

Safetensors defaults:

- `--format dense` or `--format native` exports the source tensor bytes without
  dtype conversion
- `--format q8_0` is accepted as the current default alias for dense export so
  `termite quantize --target safetensors` works without extra flags
- `--output <source-basename>.safetensors` beside the source directory
- graph/config/tokenizer sidecar bundling is planned; the current command emits
  one safetensors tensor artifact

Examples:

```text
termite export ~/.termite/models/antflydb/clipclap --target safetensors --dry-run
termite export ~/.termite/models/ggml-org/gemma-4-e2b-it-gguf --target gguf --format q4_k
termite quantize ~/.termite/models/antflydb/clipclap --target onnx --format q8_0
termite quantize ~/.termite/models/antflydb/clipclap --target gguf --format q4_k
termite quantize ~/.termite/models/antflydb/clipclap --target safetensors --dry-run
```

Internally, GGUF and safetensors keep format-specific exporter modules because
their file layouts and metadata rules differ. They are implementation details,
not separate public commands. The public CLI should stay centered on
`termite export` and `termite quantize`.

## Code Organization

The command is organized as a small dispatcher plus target-specific
implementations:

- `src/native_quantize.zig` is the CLI compatibility shim.
- `src/native_export.zig` is the generic export CLI shim.
- `src/quantize/root.zig` owns CLI parsing and target dispatch.
- `src/quantize/onnx_variant.zig` owns ONNX variant creation.
- `src/quantize/gguf_export.zig` maps `quantize --target gguf` requests onto
  the GGUF exporter.
- `src/quantize/safetensors_export.zig` maps `quantize --target safetensors`
  requests onto the safetensors exporter.
- `src/quantize/constants.zig` owns shared CLI defaults used by all targets.
- `src/quantize/variants_manifest.zig` owns ClipClap variant file naming and
  `termite_variants.json` generation.
- `src/native_export_gguf.zig` remains the GGUF-specific exporter and
  quantizer.
- `src/native_export_safetensors.zig` is the safetensors-specific dense tensor
  exporter.

## Artifact/API Interaction

Quantized output is a model variant. Resolution should eventually use:

```text
model id + task + backend + graph runtime + weight policy -> runnable artifact
```

The API should not infer task support from a quantized artifact name. Task
metadata remains in the model manifest and `TASKS.md` policy. Weight selection
can be exposed separately as a user or scheduler hint, for example:

```text
--weights dense
--weights q8_0
```

If a requested quantized variant is missing, Termite can lazily derive it from
the dense model or fall back to dense based on policy.

## Near-Term Work

1. Add runtime selection against `termite_variants.json`.
2. Add graph/config/tokenizer sidecar bundling for safetensors exports.
3. Add packed safetensors metadata if Termite needs q4/q5 safetensors artifacts
   that avoid dense expansion.
4. Add registry/runtime selection for `--weights q8_0`, `--weights q4_k`, and
   `--weights q5_k`.
5. Add backend fusions so quantized block weights can remain resident without an
   explicit dequantization copy where supported.
