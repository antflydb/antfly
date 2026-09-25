# GLiNER2.5-Decide

[`fastino/GLiNER2.5-Decide`](https://huggingface.co/fastino/GLiNER2.5-Decide)
is Fastino's typed-decision classifier. Despite the name, it is **not** a
GLiNER2.5 boundary checkpoint (see [GLINER25.md](GLINER25.md)). It is a
gliner2 2.x `SpanExtractor`:

| | GLiNER2.5 (boundary) | GLiNER2.5-Decide (span) |
|---|---|---|
| `config.json` | `architecture: "boundary"`, `boundary_head` | `architecture: "span"`, `span_head.span_mode: "markerV0"`, `max_width: 8` |
| encoder | deberta-v3 base/xsmall, digest-qualified | deberta-v3-**large** (1024 hidden, 24 layers, 16 heads) |
| label projection | boundary head | `counting_layer: "count_lstm"` (CountLSTM v1) |
| classification | boundary classifier, model temperature | `classifier` MLP (H→2H, ReLU, 2H→1), temperature 1 |
| prompt | upstream `SchemaTransformer` | the same `SchemaTransformer` |

Because upstream shares one processor between both architectures, the span
route reuses the boundary processor, schema compiler, classification
presentation (activation, thresholds, top_k, constraints) and response
writer. Only the encoder and head differ.

## Runtime routes

- **Classification**: `POST /ai/v1/extract` with a classification-only
  schema. A declared gliner2 2.x span checkpoint is upgraded to
  `schema_version: 2` automatically and runs
  `extractors/gliner_span_v2_executor.zig`: DeBERTa encoder, `[L]` marker
  states, `classifier` MLP, then the shared presentation. Labels support
  `label_definitions` descriptions, a per-task `prompt`, `multi_label` with
  `threshold`, `top_k`, `activation`, and the constraint DSL.
- **Entities / relations**: the legacy span route (`schema_version` 1,
  `antfly inference extract`) with the CountLSTM v1 label projection.
- Mixed classification + span tasks in one `schema_version: 2` request, and
  `long_document` windows, are rejected with
  `UnsupportedGlinerSpanV2Task` / `UnsupportedGlinerSpanLongDocument`.

```bash
curl -s localhost:8090/ai/v1/extract -H 'content-type: application/json' -d '{
  "model": "fastino/GLiNER2.5-Decide",
  "schema": {"classifications": [
    {"name": "intent", "labels": ["maintenance", "room_change", "checkout", "billing"]},
    {"name": "topics", "labels": ["hvac", "billing", "noise"], "multi_label": true, "threshold": 0.4},
    {"name": "answer", "labels": ["yes", "no"], "prompt": "Does the guest want to move rooms?"}
  ]},
  "inputs": [{"content": "Guest in room 1408 says the AC has been out since yesterday and they want to move tonight."}]
}'
```

## Artifacts

The published checkpoint is F32 safetensors (1.9 GB). On a 16 GiB Mac, Metal
admission of the F32 model needs roughly 2.8 GB of live memory and is denied
under ordinary desktop pressure (the loader then falls back to native CPU).
A Q8_0 split bundle (465 MB encoder + 56 MB head) loads on Metal:

```bash
antfly inference export ~/.antfly/inference/models/fastino/GLiNER2.5-Decide \
  --target gguf --format q8_0 \
  --output ~/.antfly/inference/models/fastino/GLiNER2.5-Decide-Q8_0/gliner2-encoder.Q8_0.gguf
```

The exporter reads the encoder geometry from `encoder_config/config.json`
and copies it into the bundle; the loader refuses a non-base wrapper without
that sidecar (`MissingGlinerEncoderConfig`) rather than assuming base size.

## Parity

Reference: `gliner2==2.0.0`, captured by
`scripts/gliner25/decide_oracle.py` into `testdata/gliner25/decide/cases.json`
(8 classification cases covering multi-task, multi-label, descriptions,
prompts and ordinal labels, plus 2 entity cases).

| backend / artifact | ids | max classifier logit error | decisions |
|---|---|---|---|
| native F32 | exact | 4.8e-6 | 8/8 |
| Metal F32 | exact | 8.1e-4 | 8/8 |
| Metal Q8_0 (HTTP) | exact | — | 8/8 (confidence within 3e-3) |

Entity spans (legacy route) match upstream on native and Metal F32 to
four decimals; Q8_0 keeps every entity with confidence within 5e-3.

```bash
ANTFLY_GLINER25_DECIDE_MODEL_DIR=~/.antfly/inference/models/fastino/GLiNER2.5-Decide \
  zig build inference-test -- --test-filter "GLiNER2.5-Decide"
```

The model card's "potential outputs" are illustrative: upstream itself returns
`seat_change` for the travel example and `yes` for the treaty question.
