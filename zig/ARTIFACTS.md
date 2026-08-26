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

## Catalog Ownership And References

Enrichments are catalog resources. Indexes depend on enrichments; they do not
own artifact rows directly.

Inline index configuration is shorthand for creating or reusing a normal
enrichment catalog entry. This is the long-term model for dense/sparse AKNN
indexes, graph indexes, and future artifact-consuming indexes:

```text
index config
  -> declares enrichment dependency
  -> optional shorthand creates the enrichment if missing
  -> enrichment writes artifacts
  -> index replay consumes artifacts
```

The catalog should track ownership/provenance separately from lifecycle
authority:

```json
{
  "name": "relations_v1",
  "kind": "asset",
  "created_by": "index_shorthand",
  "owner": {
    "kind": "index",
    "name": "relations_graph"
  },
  "config_hash": "sha256:..."
}
```

`owner` and `created_by` explain where the enrichment came from. They do not
grant unilateral delete/update rights once another index depends on the same
enrichment.

Reference rules:

- Referrers are derived from current index configs on catalog load/update.
- Cached referrer lists may be stored or exposed for status/UI, but they are not
  the source of truth.
- Deleting an enrichment is rejected while any index depends on it.
- Deleting an index may delete a shorthand-created enrichment only when no other
  index references it and the enrichment was not user-defined.
- Updating an enrichment config is rejected while dependent indexes require the
  old config, unless the update is a compatible no-op or an explicit rebuild plan
  updates the dependents.
- Renaming an enrichment is remove-and-create unless dependent indexes are
  updated in the same catalog operation.
- Two inline shorthand enrichments with the same name must normalize to the same
  config hash, otherwise catalog validation fails.

This means a graph index follows the same model as an AKNN index: it either
references a user-defined enrichment or includes shorthand that materializes into
a normal enrichment. The graph index consumes the resulting artifacts; it does
not create a private artifact namespace.

## Document Lookup Projection

Artifacts are document-adjacent and should be returned through ordinary document
lookup when requested:

```http
GET /tables/{table}/documents/{document_id}
GET /tables/{table}/documents/{document_id}?fields=title,_artifacts
GET /tables/{table}/documents/{document_id}?fields=_artifacts.*
GET /tables/{table}/documents/{document_id}?fields=_artifacts.page_ocr_v1.value
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
  "field": "image",
  "template": "{{remoteMedia url=image_url}}",
  "content_type": "text/plain",
  "producer_json": {
    "type": "reader",
    "config": {
      "provider": "vertex",
      "model": "gemini-2.5-flash",
      "project_id": "my-project",
      "location": "us-central1",
      "credentials_path": "/path/to/service-account.json",
      "prompt": "Read the document text."
    }
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

Execution policy belongs with the enrichment producer but is separate from the
semantic provider config. The provider `config` describes what output should be
produced: model, prompt, auth target, schema, and other behavior that can change
artifact bytes. The optional `execution` block describes how the worker should
run the producer: batch sizes, byte caps, concurrency hints, and retry/pacing
knobs.

Example reader/OCR producer with per-enrichment batching:

```json
{
  "type": "reader",
  "config": {
    "provider": "antfly",
    "model": "florence2-ocr",
    "prompt": "Read the document text."
  },
  "execution": {
    "batch_items": 4,
    "batch_bytes": 67108864
  }
}
```

`execution` is still catalog configuration, so users can tune different
enrichments and models independently. It is not part of artifact identity. A
change from `batch_items: 4` to `batch_items: 8` should not by itself make an
artifact stale or force a rebuild when the semantic `config`, source document,
rendered template/media parts, and output content type are unchanged.

Effective batching should be resolved as a layered execution policy:

```text
enrichment producer.execution override
  -> model or reader manifest default
  -> process/operator default
  -> built-in fallback
  -> clamped by process/operator maximums and backend limits
```

For reader/OCR assets, the policy supports item and byte caps:

- default OCR batch items: 4
- conservative hard cap: 8 unless the operator raises it
- byte or pixel cap in addition to item count, because one full-page scan can
  cost much more than one cropped receipt
- final inference-side chunking remains a backend safety valve

Suggested operator controls:

```text
ANTFLY_ENRICHMENT_OCR_BATCH_ITEMS=4
ANTFLY_ENRICHMENT_OCR_BATCH_MAX_ITEMS=8
ANTFLY_ENRICHMENT_OCR_BATCH_BYTES=67108864
```

Readers must stay model-neutral at this layer. The artifact producer exposes a
batch request hook, and document-extraction OCR/transcription workers flush
pending generated-text units according to the resolved `execution.batch_items`
and `execution.batch_bytes` policy. The local Antfly reader producer coalesces
compatible reader requests into one `readers.Request.images` call when producer
type, semantic config, prompt, and model options match. Inference decides
whether a concrete reader can execute the batch natively, chunk it, or fall
back. The artifact pipeline must not encode Florence-specific assumptions.
Remote providers, mixed configs, mixed prompts, generators, extractors, and
transcribers can use the same producer batch hook, but they currently fall back
to sequential execution unless their provider implementation exposes a native
batch operation.

Execution policy is scoped to the catalog resource that owns the work. Explicit
enrichments already name one producer operation, so their `execution` block uses
the policy fields directly. Index shorthand can expand into multiple work
owners, but this implementation only exposes namespaces that are wired through
runtime behavior today: `chunking` and `embedding`. The translator copies each
nested policy to the generated resource where it becomes that resource's flat
`execution` policy. Reader, generator, extractor, and transcriber batching for
explicit asset enrichments uses that flat enrichment `execution` block directly.
Graph indexes do not expose a root execution block yet; producer batching for a
graph shorthand relation asset belongs in `artifact.execution`.

Embedding enrichments should use the same execution-policy model. Existing dense
and chunked embedding workers already resolve process-level batch item and byte
limits; per-enrichment `producer.execution` overrides can feed that same
resolution without becoming part of embedding artifact identity. Suggested
fields are the same shape as readers:

```json
{
  "execution": {
    "batch_items": 8,
    "batch_bytes": 262144
  }
}
```

Indexing execution and embedder execution are separate knobs. Indexing execution
controls catalog/index maintenance windows: how many documents, artifacts, or
posting-list writes the indexer processes per pass. Embedder execution controls
inference calls: how many texts/chunks are sent to the embedder in one request
and how large that request may be. They should not share one ambiguous
`batch_items` field.

For embeddings indexes that use the inline managed-embedder shorthand, the
execution policy lives beside `embedder`, not inside it. The public shorthand
surface only accepts namespaces that are wired to generated producer
enrichments:

```json
{
  "type": "embeddings",
  "field": "body",
  "dimension": 384,
  "embedder": {
    "provider": "antfly",
    "model": "bge-base-en-v1.5"
  },
  "execution": {
    "embedding": {
      "batch_items": 16,
      "batch_bytes": 262144
    }
  }
}
```

The index translator copies `execution.embedding` onto the generated embedding
enrichment. The vector index itself consumes the produced embedding artifact;
the embedding batching policy applies to the producer that creates that
artifact. Vector-index ingestion batching is not exposed here until it has a
runtime consumer.

### Multi-source artifact indexes

Full-text, vector, and graph indexes use `sources` to select terminal artifact
streams. Full-text and vector sources have the minimal shape
`{"artifact":"..."}`; graph sources additionally own their payload `path`,
`format`, and optional node/edge/context mappings. Producer inputs such as
`field`, `template`, and `source_artifact_name` remain on enrichments.

The compatibility matrix is:

- full-text sources resolve to chunk or textual/JSON asset enrichments;
- dense and sparse vector sources resolve to embedding enrichments;
- graph sources resolve to chunk or JSON asset enrichments.

`sources` has union semantics. Every record in every selected stream is an
independent member. The singular `artifact_name` (full-text) and `source`
(graph) forms are supported single-source convenience inputs but cannot be
combined with `sources`; normalized responses use `sources`. A graph
`source` owns its `nodes`, `edge`, and `context` mappings just like one item in
`sources`. Graph mappings are source-scoped. Root-level mappings and the
redundant graph-source `kind: "artifact"` discriminator were not part of the
v0.2.0 public API and are rejected rather than introduced as compatibility
surface area.

The embeddings root field `embedding_name` is a supported single-source
convenience input. The index-level `source_artifact_name` is also supported: it
selects the upstream artifact consumed by that single embedding producer, while
`embedding_name`/`sources` select the producer output consumed by the vector
index. Multi-source configurations put `source_artifact_name` on each matching
embedding enrichment because producer inputs can differ. Normalized responses
represent canonical consumed-output identity through `sources`. When a v0.2
single-source request uses `embedding_name` and `source_artifact_name`, those
released fields remain alongside `sources` so v0.2 clients can inspect and
round-trip the effective configuration.

For example, one full-text index can search both extracted units and derived
chunks:

```json
{
  "type": "full_text",
  "sources": [
    { "artifact": "document_units_v1" },
    { "artifact": "document_chunks_v1" }
  ]
}
```

An artifact-backed embeddings index can consume several embedding streams in
one vector space. Each member is identified by `(artifact, source key)`, so a
document vector and any number of chunk vectors can coexist in the same index:

```json
{
  "type": "embeddings",
  "dimension": 384,
  "sources": [
    { "artifact": "document_dense_v1" },
    { "artifact": "document_chunk_dense_v1" }
  ],
  "embedder": {
    "provider": "antfly",
    "model": "bge-base-en-v1.5"
  },
  "enrichments": [
    {
      "name": "document_dense_v1",
      "kind": "embedding",
      "field": "semantic_content",
      "expected_dims": 384
    },
    {
      "name": "document_chunk_dense_v1",
      "kind": "embedding",
      "field": "text",
      "source_artifact_name": "document_chunks_v1",
      "expected_dims": 384
    }
  ]
}
```

The Go, Python, TypeScript, and Rust SDKs expose
`NewArtifactEmbeddingIndexConfig` / `artifact_embedding_index_config` /
`artifactEmbeddingIndexConfig` helpers that build the `sources` array and its
matching embedding enrichments together. They reject empty, duplicate, and
oversized source sets locally.

All sources in one index must have the same dense dimension and inhabit a
compatible vector space. `vector_space` is optional. When every source omits
it, Antfly compares the durable canonical semantic producer identity stored on
every embedding enrichment: provider, model, effective normalized endpoint and
region, dense/sparse mode, multimodal mode, input type, and truncation. Unknown
or incompatible producers are rejected. Credentials, pacing, retries, and
batch limits are execution settings and are excluded from that identity.

To combine intentionally compatible but distinct or externally produced
embeddings, every source must declare the same non-empty `vector_space` on its
matching embedding enrichment. Explicit and implicit modes cannot be mixed,
and dimensions are validated even when an explicit identifier matches. The
identifier is an application-stable compatibility assertion, not a display
label.

The index-level embedder is registered under every source artifact name, while
each enrichment owns its source field or upstream chunk stream. Sources may mix
embeddings produced directly from primary documents with embeddings produced
from chunk artifacts. Parent-level query results use each member's artifact
identity to collapse chunk members to their parent while retaining direct
document members in the same score-ordered result, and expose the winning
member's artifact identity as provenance. Public queries select raw members by
including an empty `hierarchy` object, or group at source level with
`hierarchy.group_by`; grouped provenance is exposed as `matched_artifact`.
Internal workers use `return_mode: "member"`, while `chunk` remains a
rolling-upgrade wire spelling with the same behavior. The former public
hierarchy controls `return_level`, `rollup`, `include`, and
`max_children_per_parent` are compatibility-only, are not present in OpenAPI,
and must not be emitted by new clients. Unit modes require every member to have
durable unit identity. Indexes containing any source without it are rejected
with an actionable 422 response.
`sources` cannot be combined with `external`, `field`, `template`, `chunker`, or
the supported single-source artifact convenience forms.

Embedder batching belongs on the matching embedding enrichment (or the shared
index-level execution policy when the public shorthand is used).

Graph source array order is deterministic precedence when sources emit the
same logical edge key: the first source owns the visible payload. Antfly keeps
the other self-contained manifests, so deleting the winner restores the next
source without rescanning all edges. Batch reconciliation coalesces repeated
artifact mutations, groups them by document and index, and scans each state
prefix once. Graph manifests and edge payloads carry the index generation;
retired generations are never replayed into a recreated index.
Released v0.2.0 key-only graph manifests and v1 edge payloads remain readable
during upgrade; the first source materialization rewrites them in the current
generation-bound format. Missing or malformed legacy payloads are ignored, and
index retirement durably removes both edge payloads and source manifests before
same-name recreation is admitted.
Each manifest has independent entry and byte limits, and one reconciliation is
also subject to aggregate entry and byte budgets. Those aggregate budgets bound
overlap-heavy workloads where many sources emit the same visible identities;
exceeding them is terminal repair debt until the source set or payload changes.
Graph status echoes the configured artifact, path, and format in that same
precedence order and uses `artifact` as the source identity. Catch-up and repair
state remains index-wide; clients use the enclosing catch-up and repair fields.

```json
{
  "type": "graph",
  "sources": [
    {
      "artifact": "title_relations_v1",
      "path": "$.relations[*]",
      "format": "extraction_relation"
    },
    {
      "artifact": "entity_graph_v1",
      "path": "$.graph",
      "format": "extraction_graph",
      "nodes": { "model": "external", "target": "{{ _item.id }}" },
      "edge": { "type": "{{ _item.type }}", "weight": "{{ _item.score }}" },
      "context": { "doc_fields": ["title"] }
    }
  ]
}
```

Existing `embedder.batch_size` should be treated as a compatibility alias for
the embedder-side batch size: `execution.embedding.batch_items` in inline index
configs, or `execution.batch_items` on explicit embedding enrichments. It should
then be normalized out of semantic embedder configuration before deriving
artifact identity. New configs should prefer the `execution` block.

Chunking follows the same split. Chunk shape is semantic: target size, overlap,
tokenizer/model, store-chunks behavior, and full-text side effects can change
the chunk artifacts. Chunker execution is non-semantic: how many source texts or
asset values are sent through the chunker per pass or per remote chunker request.
For an inline embedding index with managed chunking there can be two separate
execution namespaces:

```json
{
  "type": "embeddings",
  "field": "body",
  "dimension": 384,
  "chunker": {
    "provider": "antfly",
    "text": {
      "target_tokens": 512,
      "overlap_tokens": 64
    }
  },
  "embedder": {
    "provider": "antfly",
    "model": "bge-base-en-v1.5"
  },
  "execution": {
    "chunking": {
      "batch_items": 128,
      "batch_bytes": 1048576
    },
    "embedding": {
      "batch_items": 16,
      "batch_bytes": 262144
    }
  }
}
```

The translator copies `execution.chunking` onto the generated chunk enrichment
and `execution.embedding` onto the generated embedding enrichment. For explicit
chunk enrichments, the same chunker-side policy lives directly on the
enrichment:

```json
{
  "name": "body_chunks_v1",
  "kind": "chunk",
  "field": "body",
  "chunker": {
    "provider": "antfly",
    "text": {
      "target_tokens": 512,
      "overlap_tokens": 64
    }
  },
  "execution": {
    "batch_items": 128,
    "batch_bytes": 1048576
  }
}
```

Graph indexes use the same ownership boundary. A graph index consumes edge-like
input from document `_edges`, a user-defined enrichment, or a shorthand-created
asset enrichment. The graph index root does not expose an `execution` block yet,
because graph edge materialization and replay batching are not wired to a public
policy. Asset/extractor execution policy controls model calls that produce
relations.

```json
{
  "type": "graph",
  "source": {
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  }
}
```

If the graph index uses shorthand to create the relation-producing asset,
producer batching belongs on that artifact object. The index translator should
copy `artifact.execution` onto the generated asset enrichment. Do not also put
artifact producer policy under graph root `execution`:

```json
{
  "type": "graph",
  "artifact": {
    "name": "relations_v1",
    "kind": "asset",
    "source": {
      "type": "field",
      "value": "body"
    },
    "content_type": "application/json",
    "producer_json": {
      "type": "extractor",
      "config": {
        "provider": "antfly",
        "model": "gliner2-relations",
        "schema": "relations_v1"
      }
    },
    "execution": {
      "batch_items": 8,
      "batch_bytes": 262144
    }
  },
  "source": {
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  }
}
```

Graph traversal limits are not enrichment batching. Defaults such as max depth,
frontier caps, or result caps may be index or query execution policy, but they
should be named as traversal/query defaults rather than sharing producer
`batch_items`.

Extraction inference also has a batched request shape: multiple text inputs or
image inputs can be submitted together and results are returned by input index.
Recognizer-backed extraction should batch text inputs directly; for GLiNER2 this
means one recognizer batch per schema label set, not one model run per text.
Reader-backed image extraction batches the reader/OCR step before schema
extraction when pending units share a compatible local Antfly reader
configuration. The worker preserves document unit order by flushing pending
generated-text units before non-generated units and at stream end. As with
readers and embedders, extraction batch policy belongs in `execution` and is
clamped by model and operator limits.

The public asset enrichment shape uses `field` and `template`. The older
`source_field`/`source_template` names are internal catalog/replay names and are
not part of the public enrichment config. `template` follows the existing
Handlebars/template remote behavior used by embedders, including data-URI and
remote-media rendering for multimodal producers.

Model-backed assets run in both paths:

- synchronous `.enrichments` write precompute calls the configured producer and
  includes the artifact write in the document commit;
- asynchronous enrichment workers call the same producer from replay and retry
  on transient failures.

For model-backed assets, Antfly stores a separate internal skip-state row keyed
by the source value, rendered multimodal parts, and the semantic producer
configuration. Non-semantic `producer.execution` fields are excluded from this
identity. Asset rows remain value-only.

The model-facing producer types are separate from artifact kinds:

- **generators** call LLM-style generation endpoints, including tool-calling
  models and prompt-driven extraction.
- **extractors** produce schema-driven JSON values for entities, relations,
  classifications, document classification, token classification, and structured
  field extraction.
- **transcribers** produce text or structured transcript values from audio.
- **readers** produce text or structured values from images/documents, including
  OCR providers and multimodal LLMs.
- **chunkers**, **embedders**, and **rerankers** keep their current index-facing
  roles.

For Zig providers, `antfly` is the canonical local/remote provider name. A
provider config with `provider: "antfly"` and no `url` uses the local Antfly
inference runtime when available. Supplying `url` routes to an Antfly
inference-compatible HTTP service.

Vertex/Google auth uses provider-specific config. Explicit `bearer_token` or
provider API key config wins. Otherwise Vertex providers resolve service-account
credentials from `credentials_path`, then the existing Google environment
variables, mint a `https://www.googleapis.com/auth/cloud-platform` token, and
cache it through `lib/google`. `project_id` may be omitted when it is present in
the service-account JSON.

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
still be a normal artifact family visible through `_artifacts`. Once created,
that enrichment follows catalog reference rules: it cannot be deleted or
incompatibly changed while any index depends on it, even if one index originally
created it through shorthand config.

Example:

```json
{
  "name": "relations_graph",
  "type": "graph",
  "source": {
    "artifact": "relations_v1"
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
