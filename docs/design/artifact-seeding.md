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
| Document embedding | Array of finite numbers; length must match the enrichment's dimensions. |
| Chunk-derived embedding | `[{ chunk_id, window_index?, vector }]`. |
| Chunk collection | `_chunks.<name>`: `[{ _chunk_id (or _id), _content, _start_char?, _end_char?, _start_time_ms?, _end_time_ms? }]`. |

`"<name>": null` drops accepted output so the producer regenerates it;
omitting a name is a no-op. A name may be seeded through only one reserved
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
4. Seeded requests are excluded from the concurrent precommit execution set,
   so they never dispatch a provider call.

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

## Not yet accepted

These fail with `ArtifactSeedNotAccepted` rather than regenerating:

- `document_extraction` assets (OCR/transcription manifests, units and child
  ranges). The text seed adapter must build a native text manifest bound to the
  source identity (`source.generation`/`sha256`) without fabricating page
  coordinates.
- Chunk and vector seeds downstream of an upstream asset (extraction → chunks
  → vectors in one write). Preparation consumes upstream outputs only after
  commit today.
- Producers whose input is outside the write: neighbor or context sources,
  committed graph adjacency, grouped sources, prior context, privacy-managed
  artifacts, PDF page-image embeddings.
- Sparse embeddings, unit-scoped (`_parent_unit_id`) and diarized (`_speaker`)
  chunks.
- Deleting chunk-derived vector seeds with `null`.

## Remaining work

- Enrichment field mappings (`seed: { field, format, on_missing }`) that
  synthesize the same seeds from existing record fields. They reuse this
  acceptance path; explicit seeds win per document.
- A seed view on reads (`artifact_view=seed`) that emits exactly the write
  shapes, vectors included, for Antfly-to-Antfly migration.
- `seed_conflict: keep | replace` and per-item `accepted | unchanged | conflict
  | replaced` reporting. Today a seed replaces accepted output for its input.
- Retaining seeded chunk collections across later source-unchanged replays.
  The chunk producer has no input-identity record in local mode and re-chunks
  each processed revision, which replaces seeded chunks with generated ones.
- Leader-prepared seed acceptance for Raft. Replicas accept seeds during
  deterministic apply; dense seeds resolve the embedder's provider generation
  there, as synchronous generated preparation does.
- Validation-only planning, migration progress reporting, and a streaming
  import path beyond the 4,095-chunk atomic-mutation ceiling and request
  bounds (#1354).
- The binary `antfly_db_batch_wire` entry point does not carry transforms;
  seed-only writes use the JSON batch path.
