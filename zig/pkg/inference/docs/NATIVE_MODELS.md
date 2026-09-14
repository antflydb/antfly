# Native Model Runtimes

This document covers the native (non-ONNX) model runtime implementations in antfly-inference-zig: the LayoutDoc runtime for document classification.

---

## LayoutDoc Runtime

The native LayoutDoc runtime loads `gopeft-zig` `layoutdoc_sequence_head.safetensors` and `layoutdoc_token_head.safetensors` artifacts and serves document classification over HTTP.

### What Is Implemented

- Native `layoutdoc_sequence_head` checkpoint loading
- Native `layoutdoc_token_head` checkpoint loading
- Exact feature shape parity with `gopeft-zig`
- Image-driven visual stats for the sequence head
- OCR-token bbox feature reconstruction for the token head
- JPEG and PNG image-path support

### HTTP API

**Sequence classification:**

`POST /api/classify/document`

Request:
```json
{
  "model": "acme/layoutdoc-invoice-sequence",
  "image_path": "/tmp/page.png",
  "num_tokens": 42,
  "labels": ["invoice", "form", "email"]
}
```

Response includes: resolved checkpoint path, extracted document classification features, best label, full score list.

**Token classification:**

`POST /api/classify/document_tokens`

Request:
```json
{
  "model": "acme/layoutdoc-token-tags",
  "labels": ["O", "B-KEY", "I-KEY"],
  "tokens": [
    { "text": "Invoice", "bbox": [0, 0, 120, 24] },
    { "text": "Total", "bbox": [0, 40, 80, 64] }
  ]
}
```

Response includes: resolved checkpoint path, one prediction block per token, reconstructed token features, best label and full score list per token.

**Current endpoint constraints:**
- Labels are caller-provided (the `gopeft-zig` checkpoint does not store the label vocabulary)
- The sequence endpoint accepts a local `image_path`, not uploaded bytes
- Token classification requires caller-provided OCR tokens and boxes

### Probe CLIs

`probe-layoutdoc-sequence` and `probe-layoutdoc-token`

Build:

```bash
zigup run master build probe-layoutdoc-sequence
```

Run:

```bash
./zig-out/bin/probe-layoutdoc-sequence \
  /tmp/layoutdoc_sequence_run \
  /tmp/page.png \
  42 \
  invoice,form,email
```

Positional arguments:
1. `model_dir_or_checkpoint`
2. `image_path`
3. `num_tokens`
4. `label1,label2,...`
5. `prefix` (optional; defaults to `layoutdoc_sequence_head`)

The probe prints JSON with: resolved checkpoint path, extracted LayoutDoc features, best label, full score list.

### Parity Fixtures

Generate real parity fixtures from `gopeft-zig` checkpoints and local LayoutDoc data:

```bash
bash ./scripts/prepare_layoutdoc_parity.sh
```

That script:
- Trains tiny real `gopeft-zig` LayoutDoc sequence and token heads when needed
- Extracts one real sequence example and one real token example
- Writes termite-compatible request fixtures under `/tmp/layoutdoc_termite_parity`
- Attempts to run `probe-layoutdoc-sequence` and `probe-layoutdoc-token` if the `antfly-inference-zig` build is healthy

Generated files:
- `/tmp/layoutdoc_termite_parity/sequence_request.json`
- `/tmp/layoutdoc_termite_parity/token_request.json`
- `/tmp/layoutdoc_termite_parity/token_probe_tokens.json`
- `/tmp/layoutdoc_termite_parity/parity_summary.json`
- Optional probe outputs when the local `antfly-inference-zig` build succeeds

### Remaining Work

1. Add parity fixtures using real `gopeft-zig` checkpoints and example pages
2. Decide whether to keep the path-based sequence endpoint or add image-byte/content-part input
3. Add OCR extraction / bbox-producing flow that can feed `classify_tokens` directly
```

---
