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

### Artifact index sources

Every artifact-consuming index uses `sources` for terminal selection. A source
always names an artifact stream, not the enrichment producer or the producer's
input field. `field`, `template`, and `source_artifact_name` therefore remain
exclusively on enrichments. Full-text and vector sources use the minimal
`ArtifactIndexSource` shape, `{"artifact":"..."}`. Graph sources use
`GraphIndexSource`, which adds per-source `path` and `format` because two graph
artifact streams can require different payload interpretations.

The compatibility matrix is:

- full-text sources resolve to `chunk` or textual/JSON `asset` enrichments;
- dense and sparse vector sources resolve to `embedding` enrichments;
- graph sources resolve to `chunk` or JSON `asset` enrichments;
- algebraic indexes do not expose `sources`, because their sidecars are derived
  from table schema and engine-owned materializations rather than artifacts.

For example, an artifact-backed embeddings index declares:

```json
{
  "type": "embeddings",
  "sources": [
    {"artifact": "title_dense_v1"},
    {"artifact": "body_dense_v1"}
  ],
  "dimension": 384,
  "distance_metric": "cosine",
  "embedder": {
    "provider": "antfly",
    "model": "bge-base-en-v1.5"
  },
  "enrichments": [
    {
      "name": "title_dense_v1",
      "kind": "embedding",
      "field": "text",
      "source_artifact_name": "title_chunks_v1",
      "expected_dims": 384
    },
    {
      "name": "body_dense_v1",
      "kind": "embedding",
      "field": "text",
      "source_artifact_name": "body_chunks_v1",
      "expected_dims": 384
    }
  ]
}
```

`sources` has union semantics. Every record in every named artifact stream is
an independent index member. Vector membership identity is `(artifact, source
key)`, so two embeddings derived from the same document do not overwrite one
another. Full-text and graph indexes likewise union every selected terminal
artifact stream.

Adding an artifact-backed dense index over existing streams bootstraps its
authoritative union cardinality from a stable store snapshot. Concurrent
artifact commits contribute signed deltas while the snapshot is counted, so
the scan does not hold the global apply lock and cannot publish a false zero
coverage target. The synchronous target rebuild therefore starts from the same
authoritative union count used by later coverage checks.

Index admission is published behind an `IndexRebuilding` availability barrier.
The rebuild reads a stable artifact snapshot paired with its exact replay
sequence floor, then catches the target index up through every post-snapshot
mutation before clearing the barrier. Catalog mutations are structurally
serialized without holding the global apply lock for the corpus scan, so a
delete/recreate cannot overlap an admission while unrelated document writes
continue normally.

All sources in one index must have the same dense dimension and must inhabit a
compatible vector space. `vector_space` is optional. When every source omits
it, Antfly compares the durable canonical semantic producer identity stored on
every embedding enrichment (provider, model, effective normalized endpoint,
effective region, dense/sparse mode, multimodal mode, input type, and
truncation) and rejects
unknown or incompatible producers. Endpoint is semantic because an
OpenAI-compatible endpoint can serve an unrelated model under the same model
name. Credentials, pacing, retries, and batch limits are execution settings and
are excluded. This identity is persisted with the enrichment rather than
reconstructed from the currently loaded index configs, so stale or externally
written artifact streams cannot silently pass validation.

Managed embedding artifact freshness hashes bind the rendered source content
to that same canonical producer identity. Provider defaults, environment
overrides, configured inference service URLs, and embedded Antfly inference are
resolved through the same code path used for execution before the identity is
persisted. Changing the provider, model, effective endpoint, effective region,
modality, input type, or truncation therefore forces regeneration even when the
source text and artifact name are unchanged.

To combine intentionally compatible but distinct or externally produced
embeddings, every source must declare the same non-empty `vector_space`.
Explicit and implicit modes cannot be mixed, and dimensions are validated even
when an explicit identifier matches. The identifier is an application-stable
compatibility assertion, not a display label. Each source must resolve to a
matching `kind: embedding` enrichment. Producer input remains defined only on
that enrichment (`source_artifact_name`, `field`, or `template`); it is not
repeated on the index.

For vector indexes, `sources` is mutually exclusive with direct managed
`field`/`template`/`chunker` configuration and `external: true`; it is supported
for both dense and sparse indexes. Dense sources share dimensions, metric, and
vector space. Sparse sources share one tokenizer/model token space. Artifact-
backed indexes have one configuration path in the API and SDKs: `sources`.
Removed singular aliases are rejected rather than silently normalized.

Embedder batching belongs on each matching embedding enrichment. When an index
declares multiple sources, each producer may use its own `execution` policy;
vector-index ingestion still uses the index's shared maintenance policy.

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

When multiple graph sources emit the same logical edge key, source array order
is precedence order: the first source owns the visible payload. Antfly retains
the other manifests, so deleting or changing the winning source immediately
restores the next source's payload without rescanning per edge. State variants
within one source use a stable state-key order as the final tie-breaker. This
policy is independent of ingestion/update order.

Within a write batch, Antfly coalesces repeated mutations to each artifact key
(last mutation wins), groups affected artifacts by document and index, scans
the persisted source-state prefix once per group, and materializes only the
final winning edge payloads. This bounds reconciliation work to the affected
group instead of multiplying a full manifest scan by the number of source
mutations. Graph source state is versioned and self-contained: every manifest
entry stores its edge payload. Older key-only manifests are rejected and the
index must be rebuilt; falling back to the currently visible edge could restore
the wrong source's payload.

Every materialized graph-edge payload is also fenced by the owning index
generation. Deleting and recreating an index name therefore makes its retired
edge records inert immediately, including during replay, split reconstruction,
and shadow repair. Exact retired `(index, generation)` pairs are reclaimed by a
coalesced background sweep in bounded delete batches; the cleanup never infers
staleness from the current catalog and therefore cannot delete a concurrently
recreated generation. Index deletion stays independent of corpus size.
Manifests for artifacts no longer listed in `sources` are excluded from winner
selection.

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
      "format": "extraction_graph"
    }
  ]
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
    "field": "body",
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
  "sources": [{
    "artifact": "relations_v1",
    "path": "$.relations[*]",
    "format": "extraction_relation"
  }]
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
