# Extraction API Design

## Summary

Extraction is the canonical interface for NER, relation extraction, structured
field extraction, text classification, document classification, token
classification, and document-understanding outputs.

Antfly inference should expose one public extraction API instead of separate
`recognize`, `classify`, `classify/document`, and `classify/document_tokens`
APIs. Those names can remain internal model capabilities or CLI conveniences,
but the HTTP/API contract should be schema-driven extraction.

Antfly asset enrichments should add an `extractor` producer role. Classifier and
recognizer behavior should be represented by extraction schema sections instead
of separate artifact producer roles.

## Goals

- Use one API surface for entities, relations, classifications, structures, and
  document extraction.
- Reuse the same multimodal content-part grammar used by generators.
- Keep extraction batch-oriented with one result per input.
- Support local embedded Antfly inference, remote Antfly inference-compatible services,
  and external providers such as Pioneer.
- Store artifact rows as value-only payloads, usually `application/json`.

## Non-Goals

- Do not model extraction as chat by default.
- Do not keep separate long-term public APIs for recognition or classification.
- Do not store extraction skip/provenance state inside artifact payload rows.

## Public Antfly Inference API

Canonical route:

```text
POST /extract
```

Removed or not carried forward as first-class public API routes:

```text
POST /recognize
POST /classify
POST /classify/document
POST /classify/document_tokens
```

These concepts become schema shapes inside `/extract`.

## Request Shape

Extraction inputs reuse generator content parts, but extraction remains
batch-oriented rather than chat-history-oriented.

```json
{
  "model": "fastino/gliner2-base-v1",
  "inputs": [
    {
      "id": "doc:1",
      "content": [
        { "type": "text", "text": "Apple CEO Tim Cook visited Cupertino." }
      ],
      "metadata": {
        "source": "example"
      }
    }
  ],
  "schema": {
    "entities": ["organization", "person", "location"],
    "relations": [
      { "type": "works_at", "source": "person", "target": "organization" }
    ],
    "classifications": [
      {
        "name": "document_type",
        "labels": ["invoice", "receipt", "contract"],
        "multi_label": false
      }
    ],
    "structures": {
      "invoice": {
        "fields": {
          "vendor": "organization",
          "total": "money"
        }
      }
    }
  },
  "options": {
    "threshold": 0.5
  }
}
```

`inputs[].content` should accept:

- A string, as shorthand for one text content part.
- An array of shared AI content parts:
  - `text`
  - `image_url`
  - inline `media`
  - future `audio_url` / `document_url` parts if added to the shared schema.

This should reference the shared OpenAI-compatible message/content-part schemas
under `specs/openapi/ai/`, rather than creating extraction-specific media
shapes.

## Document Inputs

Document classification and document token classification are normal extraction
requests over richer input parts.

```json
{
  "model": "layoutlmv3-doc-classifier",
  "inputs": [
    {
      "id": "doc:invoice-1",
      "content": [
        { "type": "image_url", "image_url": { "url": "data:image/png;base64,..." } }
      ],
      "tokens": [
        { "text": "Invoice", "box": [10, 20, 90, 40] }
      ]
    }
  ],
  "schema": {
    "classifications": [
      {
        "name": "document_type",
        "labels": ["invoice", "receipt"]
      }
    ]
  }
}
```

OCR/read-first extraction can be represented with an optional reader stage:

```json
{
  "model": "fastino/gliner2-base-v1",
  "inputs": [
    {
      "content": [
        { "type": "image_url", "image_url": { "url": "s3://bucket/page.png" } }
      ]
    }
  ],
  "schema": {
    "structures": {
      "invoice": {
        "fields": {
          "vendor": "organization",
          "total": "money"
        }
      }
    }
  },
  "options": {
    "reader": {
      "provider": "antfly",
      "model": "reader-model"
    }
  }
}
```

## Response Shape

The response preserves input order and returns one extraction result per input.

```json
{
  "object": "extraction",
  "model": "fastino/gliner2-base-v1",
  "data": [
    {
      "id": "doc:1",
      "entities": [
        {
          "label": "organization",
          "text": "Apple",
          "start": 0,
          "end": 5,
          "score": 0.99
        }
      ],
      "relations": [
        {
          "type": "works_at",
          "source": { "entity_index": 1 },
          "target": { "entity_index": 0 },
          "score": 0.91
        }
      ],
      "classifications": [
        {
          "name": "document_type",
          "label": "contract",
          "score": 0.84
        }
      ],
      "structures": {
        "invoice": []
      }
    }
  ]
}
```

Fields should be omitted when not requested or unsupported by the provider.

## Chat Messages

Extraction should not use `ChatMessage[]` as the default input type. Most
extractors are batch processors, roles are usually irrelevant, and the output is
expected to align one-to-one with inputs.

However, extraction should reuse the same content-part grammar as chat
generation. If an LLM/tool-calling extractor needs chat context later, add an
optional `messages` input variant as a provider-specific or advanced mode:

```json
{
  "messages": [
    { "role": "system", "content": "Extract invoice fields." },
    {
      "role": "user",
      "content": [
        { "type": "image_url", "image_url": { "url": "data:image/png;base64,..." } }
      ]
    }
  ],
  "schema": {
    "structures": {
      "invoice": {
        "fields": {
          "vendor": "organization"
        }
      }
    }
  }
}
```

The canonical V1 path remains `inputs[].content`.

## Providers

Provider naming follows other Zig AI provider configs:

- `provider: "antfly"` with no `url`: local embedded Antfly inference.
- `provider: "antfly"` with `url`: remote Antfly inference-compatible service.
- `provider: "pioneer"`: Pioneer native extraction inference API.
- `provider: "openai"` or other LLM providers: future schema/tool-call backed
  extraction.

Pioneer’s native API is schema-driven and maps naturally to this interface:
entities, classifications, structures, and relations should be normalized into
the shared extraction request/response shapes.

## Antfly Asset Producer

Add one producer role:

```json
{
  "type": "extractor",
  "config": {
    "provider": "antfly",
    "model": "fastino/gliner2-base-v1",
    "schema": {
      "entities": ["person", "organization"],
      "relations": [
        { "type": "works_at", "source": "person", "target": "organization" }
      ]
    },
    "options": {
      "threshold": 0.5
    }
  }
}
```

Do not add separate `classifier` or `recognizer` asset roles unless there is a
strong UX reason later. Classification and recognition are extraction schemas.

Artifact rows remain value-only. Extraction artifacts should normally use
`application/json`:

```json
{
  "_artifacts": {
    "entities_v1": {
      "content_type": "application/json",
      "value": {
        "entities": [],
        "relations": [],
        "classifications": [],
        "structures": {}
      }
    }
  }
}
```

## Implementation Plan

1. Add shared extraction schemas under `specs/openapi/ai/extraction.yaml`.
2. Update `specs/openapi/inference/api.yaml` to expose `/extract` as the canonical
   route and remove first-class recognize/classify routes.
3. Generate Zig OpenAPI bindings for the shared extraction schemas.
4. Add `zig/lib/extracting` with config parsing, registry, runtime, request, and
   response types.
5. Implement providers:
   - `antfly`: call Antfly inference-compatible `/extract`, or local embedded
     provider when `antfly` has no URL.
   - `pioneer`: call Pioneer native inference and normalize the response.
6. Refactor Antfly inference internals so existing recognizer/classifier/document
   pipelines plug into one extraction runtime.
7. Add `extractor` to Antfly asset producer config and runtime.
8. Extend the local Antfly inference provider with an extraction callback.
9. Add tests:
   - entity-only extraction.
   - entity plus relation extraction in one response.
   - classification-only extraction.
   - document input with image and tokens.
   - Antfly asset producer stores value-only JSON.
   - `provider: "antfly"` without URL routes to local embedded extraction.
   - remote Antfly inference and Pioneer fake-provider normalization.
