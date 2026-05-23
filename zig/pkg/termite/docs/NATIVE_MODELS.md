# Native Model Runtimes

This document covers the native (non-ONNX) model runtime implementations in termite-zig: the GLiNER2 distributed MLX path and the LayoutDoc runtime for document classification.

---

## GLiNER2 Distributed MLX

GLiNER2 has a native distributed-MLX tensor-parallel path built on the shared DeBERTa encoder.

### What Is Implemented

- `runtime.distributed.Config` is threaded into the GLiNER2 pipeline config
- `LoadedModel.glinerPipeline()` reads distributed MLX settings from the runtime env
- `GlinerPipeline` exposes `usesDistributedMlx()` and `usesTensorParallelMlx()` for probes and server/reporting surfaces
- The shared native DeBERTa encoder reuses the same MLX TP linear seams as the native BERT reranker path
- Native DeBERTa `Q/K/V`, relative `Q_r/K_r`, attention output, and FFN linears are routed through MLX tensor-parallel helpers
- A dedicated probe exists at `probe-gliner2-recognize`

### Probe

Build:

```bash
zigup run master build probe-gliner2-recognize
```

Run:

```bash
./zig-out/bin/probe-gliner2-recognize \
  /path/to/gliner2-model \
  "John works at Google in California." \
  --label person \
  --label organization \
  --label location \
  --backend mlx
```

### Smoke Script

```bash
bash ./scripts/verify_gliner2_mlx_distributed_smoke.sh
```

### Environment

```
TERMITE_MLX_DISTRIBUTED_ENABLE=1
TERMITE_MLX_DISTRIBUTED_MODE=tensor_parallel
TERMITE_MLX_DISTRIBUTED_BACKEND=ring
TERMITE_MLX_WORLD_SIZE=<n>
TERMITE_MLX_RANK=<rank>
TERMITE_MLX_LOCAL_RANK=<rank>
MLX_WORLD_SIZE=<n>
MLX_RANK=<rank>
MLX_HOSTFILE=<path>
```

### Remaining Work

- Bounded BLAS-vs-TP parity run on a real local GLiNER2 model bundle
- Server-path orchestration for distributed multi-rank GLiNER2 execution
- Thread the same server/reporting semantics through the native `/classify` and `/recognize` paths

---

## LayoutLMv3 / LayoutDoc Runtime

The production document path is native LayoutLMv3 inference: image pixels, OCR
tokens, and normalized bounding boxes are prepared in Zig and executed through
the native session backend. The older `layoutdoc_sequence_head.safetensors` and
`layoutdoc_token_head.safetensors` compact heads remain available as
compatibility mode.

### What Is Implemented

- Native LayoutLMv3 preprocessing from local image path, OCR tokens, and bboxes
- Native LayoutLMv3 sequence-classification session execution
- Native LayoutLMv3 token-classification session execution
- Label discovery from `sequence_head_config.json`, `token_head_config.json`,
  `id2label`, or `label2id`
- Native `layoutdoc_sequence_head` checkpoint loading
- Native `layoutdoc_token_head` checkpoint loading
- Legacy compact-head feature shape parity with `gopeft-zig`
- Legacy image-driven visual stats for the sequence head
- Legacy OCR-token bbox feature reconstruction for the token head
- JPEG and PNG image-path support

### HTTP API

**Sequence classification:**

`POST /api/classify/document`

Request:
```json
{
  "model": "acme/layoutlmv3-invoice-sequence",
  "mode": "layoutlmv3",
  "image_path": "/tmp/page.png",
  "tokens": [
    { "text": "Invoice", "bbox": [0, 0, 120, 24] },
    { "text": "Total", "bbox": [0, 40, 80, 64] }
  ]
}
```

Response includes: resolved model path, preprocessing metadata, best label, and full score list.

For legacy compact-head compatibility, use `mode: "layoutdoc_head"` and pass
`num_tokens` plus labels in checkpoint output order.

**Token classification:**

`POST /api/classify/document_tokens`

Request:
```json
{
  "model": "acme/layoutlmv3-token-tags",
  "mode": "layoutlmv3",
  "image_path": "/tmp/page.png",
  "tokens": [
    { "text": "Invoice", "bbox": [0, 0, 120, 24] },
    { "text": "Total", "bbox": [0, 40, 80, 64] }
  ]
}
```

Response includes: resolved model path and one prediction block per OCR token.

For legacy compact-head compatibility, use `mode: "layoutdoc_head"` and pass
labels in checkpoint output order.

**Current endpoint constraints:**
- Full LayoutLMv3 mode requires caller-provided OCR tokens and boxes
- Endpoints accept local `image_path`, not uploaded bytes
- Legacy compact-head checkpoints still require caller-provided labels

### Local Verification

```bash
zig build test-layoutlmv3-finetune -Donnx=false -Dmlx=false -Dsystem-blas=false
zig build test -Donnx=false -Dmlx=false -Dsystem-blas=false
```

Inspect runtime bundle readiness:

```bash
zig build finetune -- adapter inspect layoutlmv3-bundle \
  /path/to/layoutlmv3_bundle \
  /tmp/layoutlmv3_runtime_report.json
```

Generate Hugging Face golden outputs for a real parity fixture:

```bash
python3 scripts/generate_layoutlmv3_hf_parity.py \
  --model-dir /path/to/layoutlmv3_bundle \
  --image /tmp/page.png \
  --tokens-json /tmp/page_tokens.json \
  --task token \
  --output /tmp/layoutlmv3_hf_golden.json
```

### Remaining Work

1. Check in small real Hugging Face parity fixture outputs for preprocessing and logits
2. Add image-byte/content-part input
3. Add OCR extraction / bbox-producing flow that can feed full LayoutLMv3 directly
```

---
