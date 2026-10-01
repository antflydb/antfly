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
- **Long prompts**: every task's prompt is joined in front of the text in one
  sequence (upstream early fusion). Each sequence is capped at the model's
  position budget (512 tokens for DeBERTa-v3), the same ceiling the legacy
  span route applies. When the combined sequence exceeds 512 tokens the
  executor splits the tasks greedily, in schema order, across several
  sequences and repeats the full text in each, rather than truncating the
  text. Logits are reassembled in schema order and presented once, so
  cross-task constraints still apply. A text that does not fit in 512 tokens
  with even one task is rejected (`413 EXTRACTION_LIMIT_EXCEEDED`); long texts
  need text windowing, not task splitting. At most 8 sequences per input and
  65,536 encoded tokens per request are accepted. Below 512 tokens the route
  is identical to upstream. Splitting is an extension: labels in different
  sequences cannot attend to one another, so scores and threshold-sensitive
  decisions can differ from a hypothetical longer early-fusion run. On an
  8-task, 483-token support ticket split at a 400-token budget, all 8 top
  labels matched the unsplit run; this is a single fixture, not general parity.
- **Upgrading schema-version-less requests**: a classification-only request
  to a declared span checkpoint runs on `schema_version: 2`. A request-level
  `options.threshold` is carried into every classification task that sets no
  `threshold` of its own, preserving its v1 meaning; otherwise the upstream
  per-task default of 0.5 applies. `hypothesis_template` (an NLI concept) is
  rejected rather than ignored.
- Mixed classification + span tasks in one `schema_version: 2` request, and
  `long_document` windows, are rejected with `400
  UNSUPPORTED_EXTRACTION_FEATURE`. Only checkpoints declaring the supported
  span marker contract (`architecture: span`, config version 3, architecture
  version 1, `span_head.span_mode: markerV0`) take this route; other span
  checkpoints keep the prior unsupported-model response on
  `schema_version: 2`.
- Metal requests on this route are admitted like the boundary route: the
  model's executor limits are validated and device scratch is leased before
  the model lock is taken.

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

`classifier.*` stays dense in every exported bundle (~8 MB): a quantized
classifier costs more per request (27 ms vs 5 ms on Metal) than it saves.

The exporter reads the encoder geometry from `encoder_config/config.json`
and copies it into the bundle; the loader refuses a non-base wrapper without
that sidecar (`MissingGlinerEncoderConfig`) rather than assuming base size.

## Metal weight mirrors

Base-size GLiNER encoders use bounded F16 weight mirrors on Metal. For the
large encoder the policy depends on whether the encoder matrices are already
quantized (read from the GGUF tensor types at load):

- **Fully quantized encoder bundle (e.g. Q8_0)**: no encoder mirrors; the bundle's own
  kernels run directly (interleaved A/B on M4, 102 tokens: ~75 ms vs ~110 ms
  with mirrors).
- **Dense or mixed encoder weights (F32 safetensors, dense GGUF, or filtered
  quantization)**: mirrors stay on. Without them Metal stages dense matrices
  to Q8_0 on the fly, which
  changes numerics (logit error 3e-2 vs 8e-4) without the user choosing a
  quantized artifact.

Every route over one session (classification, legacy entities, graph head)
uses the same encoder policy; head mirrors stay on as before.
`TERMITE_METAL_GLINER_LARGE_WEIGHT_MIRRORS=1` forces encoder mirrors for a
large quantized bundle.

## Parity

Reference: `gliner2==2.0.0` (torch 2.14.0, transformers 5.17.0; versions
and model-file SHA-256s are recorded in the fixture), captured by
`scripts/gliner25/decide_oracle.py` into `testdata/gliner25/decide/cases.json`
(8 classification cases covering multi-task, multi-label, descriptions,
prompts and ordinal labels, plus 2 entity cases).

| backend / artifact | ids | max classifier logit error | decisions |
|---|---|---|---|
| native F32 | exact | 4.8e-6 | 8/8 |
| Metal F32 | exact | 8.1e-4 | 8/8 |
| Metal Q8_0 bundle | exact | 3.1e-2 | 8/8 (tasks whose reference margin is inside the tolerance are reported as near-ties) |

Entity spans (legacy route) match upstream on native and Metal F32 to
four decimals; Q8_0 keeps every entity with confidence within 5e-3.

```bash
ANTFLY_GLINER25_DECIDE_MODEL_DIR=~/.antfly/inference/models/fastino/GLiNER2.5-Decide \
ANTFLY_GLINER25_DECIDE_Q8_BUNDLE_DIR=~/.antfly/inference/models/fastino/GLiNER2.5-Decide-Q8_0 \
  zig build inference-test -- --test-filter "GLiNER2.5-Decide"

# Stage latency breakdown (use -Doptimize=ReleaseFast for meaningful numbers)
ANTFLY_GLINER25_DECIDE_BENCH=1 ANTFLY_GLINER25_DECIDE_MODEL_DIR=<model or bundle> \
  zig build inference-test -Doptimize=ReleaseFast -- --test-filter "Decide Metal latency"
```

The model card's "potential outputs" are illustrative: upstream itself returns
`seat_change` for the travel example and `yes` for the treaty question.
