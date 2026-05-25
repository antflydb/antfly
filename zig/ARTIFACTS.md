# Artifacts And Enrichments API

This note captures the intended public API and storage boundary for the Zig
artifacts/enrichments work.

## Direction

Artifacts and enrichments are one subsystem with two resource types:

- **Artifacts** are durable outputs attached to documents.
- **Enrichments** are named producers that generate and maintain artifacts.

The public document shape should use one reserved projection namespace:
`_artifacts`. Capability-specific fields such as `_ocr`, `_ner`, `_chunks`,
`_generations`, `_transcripts`, `_edges`, and `_embeddings` should not become the
long-term API surface. Existing `_chunks` and `_embeddings` can remain
compatibility projections, but the generic surface is `_artifacts`.

There is no public artifact kind for one model task. LLM outputs, OCR text,
transcripts, classifications, entity extraction, captions, audio/image
derivatives, and similar model-produced payloads are `asset` artifacts with
explicit `content_type` and optional schema metadata. The artifact name and
enrichment producer describe what made the value; `kind` describes the Antfly
artifact family.

`content_type` is not a replacement for `kind`. `kind` is the storage and
indexing family (`asset`, `chunk`, `embedding`, graph edge families, and later
other first-class artifact families). `content_type` describes how to decode or
project an artifact value (`text/plain`, `application/json`,
`application/vnd.antfly.embedding+binary`, etc.). This is why chunks and
embeddings stay artifact families even though they also have content types.

The rule is:

> Enrichment APIs manage producers. Artifact APIs expose outputs.

## Document Lookup Projection

Artifacts are document-adjacent and should be returned through ordinary document
lookup when requested:

```http
GET /tables/{table}/lookup/{document_id}
GET /tables/{table}/lookup/{document_id}?fields=title,_artifacts
GET /tables/{table}/lookup/{document_id}?fields=_artifacts.*
GET /tables/{table}/lookup/{document_id}?fields=_artifacts.page_ocr_v1.value
```

The default lookup response should not hydrate artifacts. Artifact hydration is
explicit because artifacts may be large, numerous, or binary.

Example response:

```json
{
  "id": "doc:1",
  "title": "Quarterly report",
  "_artifacts": {
    "page_ocr_v1": {
      "artifact_id": "af1:asset:...",
      "artifact_ref": {
        "document_id": "doc:1",
        "name": "page_ocr_v1",
        "kind": "asset"
      },
      "kind": "asset",
      "content_type": "text/plain",
      "status": "ready",
      "value": "Revenue increased..."
    },
    "body_chunks_v1": {
      "kind": "chunk_set",
      "status": "ready",
      "items": [
        {
          "artifact_id": "af1:chunk:...",
          "artifact_ref": {
            "document_id": "doc:1",
            "name": "body_chunks_v1",
            "kind": "chunk",
            "chunk_id": 0
          },
          "kind": "chunk",
          "content_type": "application/json",
          "status": "ready",
          "value": {
            "_chunk_id": 0,
            "_content": "Revenue increased..."
          }
        }
      ]
    },
    "body_dense_v1": {
      "artifact_id": "af1:embedding:...",
      "artifact_ref": {
        "document_id": "doc:1",
        "name": "body_dense_v1",
        "kind": "embedding"
      },
      "kind": "embedding",
      "content_type": "application/vnd.antfly.embedding+binary",
      "status": "ready",
      "dims": 768,
      "value": null
    }
  }
}
```

Asset rows store only the artifact value bytes. They do not embed
`content_type`, producer configuration, schema names, or source metadata in the
row payload. That metadata belongs to the enrichment/catalog configuration and
is joined in when `_artifacts` is projected. For lookup projection:

- `text/plain` assets are returned as JSON strings.
- `application/json` assets are parsed and returned as JSON values.
- other asset content types can be returned as strings, opaque bytes, or direct
  artifact references depending on the API surface and field projection.

## Artifact Identity

`ArtifactRef` remains the structured identity. `artifact_id` remains the opaque,
round-trippable convenience token for search hits, links, and APIs that cannot
carry structured refs.

Public APIs should not expose internal storage keys.

The common user path is document lookup with `_artifacts`. A direct artifact-id
lookup remains useful as an escape hatch for artifact search hits:

```http
GET /tables/{table}/artifacts/{artifact_id}
```

That endpoint can be added later. The important first slice is that artifacts
are visible from document lookup without making derived outputs internal-only.

## Enrichment API

Enrichments are named producers:

```http
GET  /tables/{table}/enrichments
PUT  /tables/{table}/enrichments/{name}
GET  /tables/{table}/enrichments/{name}
PATCH /tables/{table}/enrichments/{name}
DELETE /tables/{table}/enrichments/{name}

POST /tables/{table}/enrichments/{name}/backfill
POST /tables/{table}/enrichments/{name}/retry
GET  /tables/{table}/enrichments/{name}/status
```

Example:

```json
{
  "name": "page_ocr_v1",
  "kind": "asset",
  "input": {
    "source": "document_field",
    "field": "image"
  },
  "output": {
    "artifact_name": "page_ocr_v1",
    "content_type": "text/plain"
  },
  "producer": {
    "type": "reader",
    "config": {
      "provider": "vertex",
      "model": "gemini-2.5-flash",
      "project_id": "my-project",
      "location": "us-central1",
      "credentials_path": "/path/to/service-account.json",
      "prompt": "Read the document text."
    }
  },
  "trigger": {
    "on_write": true,
    "on_backfill": true
  },
  "policy": {
    "dedupe_by_source_hash": true,
    "model": "prefer_model",
    "failure": "skip_document"
  }
}
```

Asset producers have two independent axes:

- `producer.type` describes the operation that produces the asset: `copy`,
  `generator`, `reader`, or `transcriber`.
- `producer.config.provider` describes the implementation provider for that
  operation, following the existing typed config convention used by embedders,
  generators, rerankers, chunkers, readers, and transcribers.

Canonical producer shape:

```json
{
  "type": "reader",
  "config": {
    "provider": "vertex",
    "model": "gemini-2.5-flash"
  }
}
```

Provider-specific fields belong inside `producer.config` and are only valid
when that provider config supports them. For example, `credentials_path`,
`project_id`, and `location` are Vertex/Google fields, not universal asset
enrichment fields. If `producer` is omitted, the enrichment defaults to `copy`
behavior: the source field or rendered source template value is stored directly
as the asset value.

The model-facing producer types are separate from artifact kinds:

- **generators** call LLM-style generation endpoints, including tool-calling
  models and prompt-driven extraction.
- **transcribers** produce text or structured transcript values from audio.
- **readers** produce text or structured values from images/documents, including
  OCR providers and multimodal LLMs.
- **chunkers**, **embedders**, and **rerankers** keep their current index-facing
  roles.

For Zig providers, `antfly` is the canonical local/remote provider name. A
provider config with `provider: "antfly"` and no `url` uses the local Termite
runtime when available. Supplying `url` routes to an Antfly/Termite-compatible
HTTP service. `termite` remains an alias while older configs migrate.

## Distributed System Boundary

The distributed contract should stay consistent with Antfly's current derived
replay model:

1. The writer commits the base document or user-provided artifact.
2. The same commit appends a thin change-journal record.
3. Enrichment workers consume replay in bounded windows.
4. Workers rehydrate current inputs from DocStore.
5. Workers write output artifacts through the owning shard.
6. Artifact writes append replay for downstream consumers.
7. Index workers consume artifact replay and publish index state separately.

Query execution must not synchronously call OCR, transcription, NER, generative
model calls, relation extraction, or embedding models. Queries see the latest
published artifact/index state.

## Index Boundary

Indexes should depend on artifact families, not own enrichment output. Creating
an index may create a managed enrichment for convenience, but the output should
still be a normal artifact family visible through `_artifacts`.

Example:

```json
{
  "name": "relations_graph",
  "kind": "graph",
  "source": {
    "artifact_name": "relations_v1"
  }
}
```

This lets user-written artifacts, imported artifacts, and model-produced
artifacts feed the same index code.

Asset payloads may be scalar, text, binary, or structured JSON. A single
extraction asset can carry multiple related products, such as entities and
relations, when the producer naturally emits them together:

```json
{
  "artifact_name": "entity_graph_v1",
  "content_type": "application/json",
  "schema": "antfly.extraction.v1",
  "value": {
    "entities": [
      { "id": "e1", "type": "company", "text": "Antfly" }
    ],
    "relations": [
      { "source": "e1", "target": "e2", "type": "acquired" }
    ]
  }
}
```

Graph indexing can consume the relation portion of that asset directly or a
follow-on enrichment can normalize it into graph-edge artifacts when stable edge
identity is required.

## Compatibility

`_chunks` and `_embeddings` remain compatibility projections. New capabilities
should prefer `_artifacts`:

- OCR text: `_artifacts.page_ocr_v1`
- NER output: `_artifacts.entities_v1`
- LLM output: `_artifacts.llm_output_v1`
- Transcription: `_artifacts.audio_transcript_v1`
- Relation extraction: `_artifacts.relations_v1`
- Chunks: `_artifacts.body_chunks_v1`
- Embeddings: `_artifacts.body_dense_v1`

The implementation should avoid adding new top-level reserved fields for every
artifact kind.
