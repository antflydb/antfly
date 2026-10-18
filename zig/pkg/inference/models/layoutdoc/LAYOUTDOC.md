# LayoutDoc Native Runtime

This document covers the native (non-ONNX) model runtime implementations in antfly-inference-zig: the LayoutDoc runtime for document classification.

---

## LayoutDoc Runtime

The native LayoutDoc runtime loads `gopeft-zig` `layoutdoc_sequence_head.safetensors` and `layoutdoc_token_head.safetensors` artifacts for document classification. It is reachable through the probe CLIs; it is not exposed over HTTP (see [HTTP handlers](#http-handlers-not-routed)).

### What Is Implemented

- Native `layoutdoc_sequence_head` checkpoint loading
- Native `layoutdoc_token_head` checkpoint loading
- Exact feature shape parity with `gopeft-zig`
- Image-driven visual stats for the sequence head
- OCR-token bbox feature reconstruction for the token head
- JPEG and PNG image-path support

### HTTP handlers (not routed)

`Node.classifyDocument` and `Node.classifyDocumentTokens` in
`src/server/server.zig` implement sequence and token classification, and
`specs/openapi/inference/api.yaml` defines their request schemas
(`DocumentClassificationRequest`, `DocumentTokenClassificationRequest`).
Neither handler is registered on any route, so there is
no `/api/classify/document` or `/api/classify/document_tokens` endpoint and no
HTTP way to run LayoutDoc today. Exposing them (or removing them) is open work
below.

What the handlers accept, for when they are wired:

- **Sequence:** `model`, a local `image_path`, `num_tokens`, caller-provided
  `labels`, and an optional checkpoint `prefix`; they resolve
  `layoutdoc_sequence_head.safetensors` under the model directory and return
  the resolved checkpoint path, extracted features, best label, and full
  score list.
- **Token:** `model`, `labels`, and caller-provided OCR `tokens` with `bbox`
  boxes; they return one prediction block per token with reconstructed
  features, best label, and scores.
- Labels are caller-provided because the `gopeft-zig` checkpoint does not
  store the label vocabulary.

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

1. Register or remove the document classification handlers: route
   `Node.classifyDocument`/`Node.classifyDocumentTokens` (the likely target is
   a document task on `/extract`, per the [inference roadmap](../../ROADMAP.md))
   or delete them and their request schemas
2. Add parity fixtures using real `gopeft-zig` checkpoints and example pages
3. Decide whether to keep path-based `image_path` input or add image-byte/content-part input
4. Add OCR extraction / bbox-producing flow that can feed `classify_tokens` directly
```

---
