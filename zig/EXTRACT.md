# Extraction API

Antfly exposes one public extraction endpoint:

```text
POST /ai/v1/extract
```

The request is schema-driven. There is no separate recognition endpoint and no
mode discriminator: entities, relations, classifications, and structures are
declared under `schema`.

## Structured extraction

Structured extraction maps text or document images into named structures.

```json
{
  "model": "antflydb/gliner2-base-v1",
  "inputs": [{"id": "doc-1", "content": "John Smith works at Google."}],
  "schema": {
    "structures": {
      "person": {
        "fields": {"name": "str", "company": "str"}
      }
    }
  },
  "options": {"include_confidence": true, "include_spans": true}
}
```

## Entity extraction

Entity extraction accepts text content. Zero-shot extractors use the labels in
`schema.entities`; fixed-label token classifiers may omit them.

```json
{
  "model": "antflydb/gliner2-base-v1",
  "inputs": [{"content": "John Smith works at Google."}],
  "schema": {"entities": ["person", "organization"]},
  "options": {"include_confidence": true, "include_spans": true}
}
```

## Relation extraction

Relation extraction accepts text content and returns both participating entities
and relation edges. Optional source and target types qualify a relation label.

```json
{
  "model": "antflydb/gliner2-base-v1",
  "inputs": [{"content": "John Smith works at Google."}],
  "schema": {
    "entities": ["person", "organization"],
    "relations": [
      {"type": "works_for", "source": "person", "target": "organization"}
    ]
  },
  "options": {
    "include_confidence": true,
    "include_spans": true,
    "resolver": {"similarity_threshold": 0.85}
  }
}
```

`options.resolver` merges equivalent mentions across the input batch while
preserving one response object per input and relation provenance.

## Classification extraction

Classification accepts either an NLI classifier or a classification-capable
extractor. Each named taxonomy owns its NLI hypothesis template because one
request may contain taxonomies with different semantics.

```json
{
  "model": "MoritzLaurer/mDeBERTa-v3-base-mnli-xnli",
  "inputs": [
    {"id": "review-1", "content": "I love this product!"}
  ],
  "schema": {
    "classifications": [
      {
        "name": "sentiment",
        "labels": ["positive", "negative", "neutral"],
        "multi_label": false,
        "hypothesis_template": "This review expresses {} sentiment.",
        "top_k": 1
      }
    ]
  },
  "options": {"include_confidence": true, "threshold": 0.2}
}
```

Single-label taxonomies return the highest-ranked label by default, or up to
`top_k` labels when it is supplied. Multi-label taxonomies ignore `top_k` and
return every label whose score meets `options.threshold`. Classifier models are
listed with the other extraction-capable models under `models.extractors`.

## Response envelope

Responses preserve input order and copy each optional input `id`.

```json
{
  "object": "extraction",
  "model": "antflydb/gliner2-base-v1",
  "data": [
    {
      "id": "doc-1",
      "entities": [
        {
          "text": "John Smith",
          "label": "person",
          "start": 0,
          "end": 10,
          "score": 0.99
        },
        {
          "text": "Google",
          "label": "organization",
          "start": 20,
          "end": 26,
          "score": 0.98
        }
      ],
      "relations": [
        {
          "type": "works_for",
          "source": {"entity_index": 0},
          "target": {"entity_index": 1},
          "score": 0.94
        }
      ]
    }
  ],
  "usage": {
    "prompt_tokens": 7,
    "completion_tokens": 0,
    "total_tokens": 7
  }
}
```

Per-input fields are `entities`, `relations`, `classifications`, and
`structures`. Relation endpoints refer to the input object's entity array by
index, avoiding duplicate entity payloads.

Models that support these operations are listed only in the `extractors`
collection returned by `GET /ai/v1/models`. Managed model manifests use the
`extract` task. The legacy `recognize` task and endpoint are not accepted.

## Model support

`antflydb/gliner2-base-v1` above is the legacy span-architecture GLiNER2
extractor. `fastino/gliner2.5-base-v1` (native/Metal, fp32) is a reviewed
GLiNER2.5 boundary-architecture checkpoint that also serves entities,
relations, classification, and records through this same endpoint and
request shape; see
[`pkg/inference/models/gliner2/GLINER25.md`](pkg/inference/models/gliner2/GLINER25.md)
for its qualification record, evidence, and how to qualify another GLiNER2.5
variant or precision. A boundary checkpoint's `model_manifest.json`
(`antfly inference pull`) only advertises the `extract` task and its
capabilities once its exact weight/sidecar digests have been reviewed; an
unreviewed digest, revision, or variant is refused at request time.

A document larger than the qualified checkpoint's single-window bound can be
served with `options.long_document`, e.g. `{"mode": "window"}`: the request is
split into overlapping windows sized to the model's real per-window word
capacity, entities are deduplicated across overlapping windows, and relations
are resolved within a window and merged document-wide, before being returned
through the same canonical `entities`/`relations` shape above (the response's
`long_document.window_count` field reports how many windows were used). This
is qualified independently of, and more narrowly than, single-window
extraction -- see GLINER25.md's long-document section for the reviewed
document-size bound and feature coverage. A request outside the reviewed
bound, or for a task combination not yet measured with windowing, still fails
closed instead of silently truncating or misbehaving.

For a reproducible fp32 pull, pin the reviewed Transformers-wrapper revision:

```sh
antfly inference pull "fastino/gliner2.5-base-v1:safetensors@54785d51df8d86a0d8f2eb212fec15871aad2c4d"
```

An explicit revision installs a separate variant: use the complete pinned name
in extraction requests. The plain model name also works while upstream
resolves to these reviewed bytes. Reduced-precision conversion uses the
original reviewed revision as described below.
See [the qualification record](pkg/inference/models/gliner2/GLINER25.md) for
supported revisions and runtime bounds.

### Selecting a reduced-precision GLiNER2.5 checkpoint

The published checkpoint is fp32. A reviewed `fp16_encoder` conversion
narrows encoder matrices to F16 and retains biases, normalization parameters,
and the extraction head in FP32. It is qualified on native and Metal for
single-window and long-document requests within its reviewed profiles.
**fp32 remains recommended**; see the [qualification record](pkg/inference/models/gliner2/GLINER25.md)
for tolerances, task coverage, and measured performance.

The qualified conversion requires the **original revision
`72ac19b486cd4557424c8d61114e7530c243e9b0`**, including its original
`config.json`. Converting the new Transformers wrapper copies its different
config digest into the bundle, which is not qualified for fp16 extraction.
Use a dedicated source directory containing only the following pinned pull;
the directory glob below selects that explicit variant's installation:

```sh
antfly inference pull \
  "fastino/gliner2.5-base-v1:safetensors@72ac19b486cd4557424c8d61114e7530c243e9b0" \
  --models-dir ./gliner25-reviewed-source

antfly-inference-gliner25-convert \
  --model-dir ./gliner25-reviewed-source/fastino/gliner2.5-base-v1*/ \
  --output-dir ~/.antfly/inference/models/fastino/gliner2.5-base-v1-fp16 \
  --precision fp16_encoder
```

Conversion writes its own `model_manifest.json` as part of the same atomic
publication, using the reviewed identity gate applied by `pull`. The
converted directory is selectable as `fastino/gliner2.5-base-v1-fp16`.
Requests outside its reviewed feature or geometry profiles still fail closed.
Converting to any other precision
(`q8_0`, `q4_k`, `q4_0`) produces a normal, byte-verified bundle with an
empty `tasks` list until a future reviewer qualifies it the same way.
