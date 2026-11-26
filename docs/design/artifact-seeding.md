# Artifact seeding

How existing results become accepted enrichment output without invoking a
producer. Tracking: [#1791](https://github.com/antflydb/antfly/issues/1791).

Migrations into Antfly often already have OCR text, transcripts, summaries,
chunks or vectors. A conditional "skip OCR when this field exists" cannot
provide input binding, downstream indexing, completion semantics or safe
invalidation. Instead, an enrichment names an output and optionally a producer;
a seed supplies an already computed value for that output. The seed is
published through the same artifact lifecycle as generated output. Indexes
consume the named artifact regardless of its origin.

## Write shapes

Seeds travel in the document, under reserved fields keyed by enrichment name:

- `_artifacts.<name>`: assets and embeddings, `{ "value", "source"?, "provenance"? }`.
- `_chunks.<name>`: chunk collections, the plain array a `_chunks` read returns
  minus internal fields.

```json
{
  "inserts": {
    "doc:1": {
      "body": "Revenue increased ...",
      "_artifacts": {
        "summary_v1": { "value": "Revenue grew." },
        "body_dense_v1": { "value": [0.12, 0.34, 0.56] },
        "body_chunk_vectors_v1": {
          "value": [
            { "chunk_id": 0, "vector": [0.01, 0.02, 0.03] },
            { "chunk_id": 1, "window_index": 0, "vector": [0.04, 0.05, 0.06] },
            { "chunk_id": 1, "window_index": 1, "vector": [0.07, 0.08, 0.09] }
          ]
        }
      },
      "_chunks": {
        "body_chunks_v1": [
          { "_chunk_id": 0, "_content": "Revenue ", "_start_char": 0, "_end_char": 8 },
          { "_chunk_id": 1, "_content": "increased ...", "_start_char": 8, "_end_char": 21 }
        ]
      }
    }
  }
}
```

| Kind | `value` |
| --- | --- |
| Text asset | String, or `{ "text": ... }`. `""` is a valid complete result, distinct from missing. |
| JSON asset | Any JSON value. |
| Document extraction asset | `{ "text": ... }`: already extracted plain text. No fetch, OCR or transcription runs. |
| Document embedding | Array of finite numbers; length must match the enrichment's dimensions. |
| Chunk-derived embedding | `[{ chunk_id, window_index?, vector }]`. |
| Sparse embedding | `{ "indices": [...], "values": [...] }` with strictly ascending indices, or a token-weight map. |
| Chunk collection | `_chunks.<name>`: `[{ _chunk_id (or _id), _content, _start_char?, _end_char?, _start_time_ms?, _end_time_ms? }]`. |

`"<name>": null` drops accepted output so the producer regenerates it
(chunk-derived vectors are embedded again in the same write; with no embedder
configured the write fails rather than keep the seeded vectors); omitting a
name is a no-op. A name may be seeded through only one reserved
field: an asset or embedding under `_chunks` (or a chunk collection under
`_artifacts`) is a kind mismatch.

Writes accept exactly these shapes. Descriptive read fields (`artifact_id`,
`artifact_ref`, `kind`, `status`, `dims`, `items`, `content_type`) and
internal routing fields (`_parent_doc_key`, `_artifact_range_id`, `_chunk`,
...) fail visibly instead of being ignored. `provenance` is recorded as a
caller assertion only, never as proof that Antfly ran that producer.

`_artifacts` is distinct from `_embeddings`: `_embeddings` is keyed by index
name and serves `external: true` indexes. Seeds are keyed by enrichment name
and target producer-backed artifacts.

## Acceptance

The document mapper strips both reserved fields during the parse-once
extraction (`document_mapper.zig` `extractWrite`), validates their envelopes
(`storage/db/artifact_seeds.zig`) and keeps one canonical JSON copy with the
extracted write. Inserts, transforms, Lite and relational intent sidecars all
reach that path. A document carrying only seeds is rejected: there are no
seed-only rows.

Producer preparation (`mutation_preparation.zig` `prepareGeneratedEnrichments`)
accepts each seed in place of exactly one producer invocation:

1. Every seeded name must match a producer request planned for that document
   (`UnknownArtifactSeed` otherwise). Seeds never fall back to inference.
2. A write that carries seeds always runs preparation, at any `sync_level`.
   Seeded requests are prepared regardless of the precompute mode because they
   need no inference. Unseeded requests keep their ordinary mode: replay at
   `write`/`propose`, synchronous at `enrichments` or with
   `commit_requires: direct_enrichments`.
3. The seeded output is written with exactly the input identity the producer
   would record, through the same keys, codecs, consumer effects and coverage
   outcomes (`mutation_materialization.zig` `computeSeeded*`):
   - generic assets: the asset row plus its state row (the SHA-256 of
     source input and producer recipe);
   - document embeddings: the dense row with the producer's source hash at the
     embedding contract's current provider generation;
   - chunk-derived embeddings: one row per chunk window, each hashed over that
     chunk's text. Chunks without a seeded vector are embedded as usual;
   - chunk collections: the seeded set is installed as this revision's chunker
     output, so chunk records, full-text consumers and inline chunk embeddings
     consume exactly the seeded set.
   - sparse embeddings: the sparse row with the producer's source hash at the
     sparse contract's provider revision;
   - document extraction: the seed replaces the fetch with `text/plain`
     content and runs the in-process native text route, so the manifest,
     units and children are built exactly as for an extracted text file. No
     checksum, OCR or transcription runs. The input fingerprint is the remote
     metadata fingerprint when the source metadata is known, else a
     `seed-sha256:` digest over the URL, extraction config, `source`
     identity and text, so an identical resumed import is `unchanged`.
4. Seeded requests are excluded from the concurrent precommit execution set,
   so they never dispatch a provider call.
5. Generic asset seeds also record the local publication receipt the
   maintenance worker writes (`artifact_local_publication.zig`), so a later
   replay of the document (for example for another, unseeded enrichment)
   reuses the seed instead of invoking the producer.

Because the stored identity equals the generated identity, every reuse check
(synchronous preparation, the replay worker, re-embed recovery) treats seeded
output as accepted output for that input. A later change to the input
invalidates it like generated output; an unrelated field edit does not.

Dense seeds carry the `seeded` header flag (`artifact_codec.zig`). It is
descriptive: the vector is never marked `text_input`, so LEANN transient
storage keeps it, and descriptive `_artifacts` reads report
`origin: seeded | generated | external` on embeddings. Older readers ignore
the bit.

### Binding

- **Same write as the source.** An insert, or an upsert transform that also
  sets source fields, binds its seeds to that version atomically.
- **Seed-only transform.** Requires `expected_version` (the lookup's
  `X-Antfly-Version`). It lowers to the batch version predicate checked
  atomically with the commit; a mismatch rejects the batch. A seed-only
  transform for a missing key is rejected with or without `upsert`
  (`ArtifactSeedDocumentNotFound`), never silently discarded.
- **Same batch.** An insert and a seed transform for one key coalesce: the
  pending insert is the transform base, and later seed values win per name.
- **Distributed batches.** Seeds are document content, so 2PC intents and
  relational intent sidecars carry them; acceptance runs at participant
  resolution from the same bytes.

- **Declared input.** `source.source_hash` (the `source_hash` of an earlier
  `_artifacts` read, `xxh64:<hex>`) must equal the input identity the seed is
  bound to now, or the batch fails with `ArtifactSeedSourceMismatch`. Only
  document-level dense and sparse embeddings have a single source hash;
  other outputs reject a declared hash (`ArtifactSeedNotAccepted`) instead of
  ignoring it.

### Conflicts and outcomes

Each seeded output has one outcome:

| Outcome | Meaning |
| --- | --- |
| `accepted` | No accepted output exists for this input; the seed becomes it. |
| `unchanged` | Identical output is already accepted for this input. Idempotent. |
| `conflict` | Different output is already accepted for this input (`keep`). |
| `replaced` | Different output was replaced by the seed (`replace`). |
| `deleted` | A `null` seed dropped the accepted output. |
| `missing` | A seeded document's unseeded enrichment; its producer runs. |

`seed_conflict: keep | replace` (default `keep`) chooses what a conflict does.
The public batch lowers a request-level `seed_conflict: replace` into a
reserved per-document `_seed_conflict` member (a document's own wins), so the
policy travels with the seeds through replication and 2PC. Under `keep`, a
conflict rejects the batch with `ArtifactSeedConflict`. During distributed
transaction resolution a conflict never fails: the existing output is kept,
because the coordinator has already committed. Conflicts are detected only
against output accepted for the same input; a changed input is a fresh
`accepted`.

### Validation-only planning

`{"validate_only": true}` on a public batch plans every seeded insert and
transform without writing (`api/seed_planning.zig`). Each item is sent to its
owning shard as a private routed lookup (`_seed_plan=true`); the owner runs the
write's exact producer preparation under the apply lock and returns before
commit (`BatchExecutionOptions.seed_plan`). The response is
`{"seeds": [{key, name, outcome, existing_origin?, error?}]}`; items a write
would reject are reported as `rejected` with the error instead of failing the
request. Embedded callers use `DB.planArtifactSeeds`.

### Field mappings

An enrichment can accept existing record fields as seeds:

```json
{
  "name": "summary_v1",
  "kind": "asset",
  "field": "body",
  "producer": { "type": "generator", "config": { ... } },
  "seed": { "field": "legacy.summary", "format": "text", "on_missing": "generate" }
}
```

`format` is `auto | text | json | vector | chunk_vectors | chunks`; `auto`
follows the enrichment kind. The mapped value is decoded like an explicit
seed and accepted through the same path. The field stays an ordinary
document field. An explicit `_artifacts`/`_chunks` seed in the same write
wins. `on_missing: fail` rejects a write whose record lacks the field
(`ArtifactSeedMissing`); `generate` runs the producer. The mapping is stored
as `seed_json` and excluded from the enrichment's artifact identity, so adding
or changing it never rebuilds existing output.

### Transforms

`_artifacts.<name>`, `_chunks.<name>` and `_embeddings.<index>` take exactly
two path segments and only `$set` and `$unset` (`$unset` deletes). Every other
operator fails closed with `UnsupportedTransformOperation`, as for `_edges`.
The coalescer folds these operations into the resolved document as reserved
fields; the mapper then extracts them exactly as for an insert. The schema
validation resolver (`storage/write.zig`) classifies them the same way. The
serverless coordinator rejects seed paths until serverless publication accepts
seeds.

### Reads

Requesting `_chunks.<name>` or `_embeddings.<index>` alone now loads the
reserved projection (`query/projection.zig`).

`artifact_view=seed` (lookup query parameter, `ScanKeysRequest` property)
renders the selected `_artifacts` and `_chunks` in exactly the write shapes,
vectors included, and omits descriptive and internal fields
(`artifact_seed_view.zig`). Writing that response into another table with the
same enrichments publishes the same outputs without invoking producers, and a
second export is byte-identical, so resumed migrations converge. Document
extraction assets export their text; explicit external vectors (exported
through `_embeddings`) and unit-scoped extraction children are omitted. The
view is forwarded through the storage kernel wire, routed lookups and scans.

## Not yet accepted

These fail with `ArtifactSeedNotAccepted` rather than regenerating:

- Chunk and vector seeds downstream of an upstream asset (for example
  extraction → chunks → vectors in one write). Chunk rows over an upstream
  asset are materialized only after commit, with the upstream causal witness;
  seed the upstream asset and let its descendants generate, or seed chunks of
  a source field.
- Rich document extraction seeds (pages, layout, unit-scoped
  `_parent_unit_id` and diarized `_speaker` chunks). Text seeds produce a
  native text manifest; page coordinates are never fabricated.
- Producers whose input is outside the write: neighbor or context sources,
  committed graph adjacency, grouped sources, prior context, privacy-managed
  artifacts, PDF page-image embeddings, chunk-derived sparse vectors.
- `copy` assets (free to regenerate).

## Remaining work

- Retaining seeded chunk collections across later source-unchanged replays.
  The chunk producer has no input-identity record in local mode and re-chunks
  each processed revision, which replaces seeded chunks with generated ones.
- Leader-prepared seed acceptance for Raft. Replicas accept seeds during
  deterministic apply (seed errors are expected apply failures that advance
  the entry on every replica alike); dense seeds resolve the embedder's
  provider generation there, as synchronous generated preparation does.
- Migration progress reporting and a streaming import path beyond the
  4,095-chunk atomic-mutation ceiling and request bounds (#1354).
- The binary `antfly_db_batch_wire` entry point does not carry transforms;
  seed-only writes use the JSON batch path. The serverless coordinator rejects
  seed paths until serverless publication accepts seeds.
