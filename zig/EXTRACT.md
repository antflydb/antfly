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
