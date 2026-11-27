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
| Document extraction asset | `{ "text": ... }`: already extracted plain text, or `{ "units": [...] }`: the extracted units with pages, layout and transcript phrases (see Rich document extraction). No fetch, OCR or transcription runs. |
| Document embedding | Array of finite numbers; length must match the enrichment's dimensions. |
| Chunk-derived embedding | `[{ chunk_id, window_index?, vector }]`. |
| Sparse embedding | `{ "indices": [...], "values": [...] }` with unique indices, or a token-weight map. |
| Chunk-derived sparse embedding | `[{ chunk_id, parent_unit_id?, indices, values }]`, one per chunk (never windowed). |
| Chunk collection | `_chunks.<name>`: `[{ _chunk_id (or _id), _content, _start_char?, _end_char?, _start_time_ms?, _end_time_ms?, _parent_unit_id?, _speaker? }]`. Unit-scoped fields only for chunks of a document extraction's units. |

`"<name>": null` drops accepted output so the producer regenerates it
(chunk-derived vectors, dense or sparse, including those of extraction unit
chunks, are embedded again in the same write; with no embedder configured the
write fails rather than keep the seeded vectors); omitting a name is a no-op. A name may be seeded through only one reserved
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
  `X-Antfly-Version`) unless everything it seeds is externally maintained.
  It lowers to the batch version predicate checked atomically with the
  commit; a mismatch rejects the batch. The database enforces the
  requirement (`ArtifactSeedSourceBindingRequired`), since only the catalog
  knows which outputs bind to an input; validation-only planning honors
  `expected_version` the same way. A seed-only transform for a missing key is
  rejected with or without `upsert` (`ArtifactSeedDocumentNotFound`), never
  silently discarded. It rewrites neither the primary row nor its version;
  leader seed preparation carries those documents
  (`seed_only_documents`) so replicas keep them too.
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
conflict is not applied: the accepted output stays, the rest of the write
commits, and the item is reported as `conflict` with the existing output's
origin. Each outcome except `missing` also reports `via: explicit | mapping`:
an explicit seed in the write wins over the enrichment's field mapping.

Embedded batches report outcomes per item: `DB.batchReportingSeeds`, and the
C API's `antfly_db_batch_json` and `antfly_db_batch_wire` responses carry a
`seeds` array when any write was seeded. Server batch responses do not carry
apply outcomes yet (see remaining work); plan with `validate_only` first.
During distributed transaction resolution the existing output is kept the
same way. Conflicts are detected only
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

`on_missing: pending` runs no producer for a record that lacks the field: the
write reports the output `pending` (via `mapping`), and it stays absent through
later edits and replays, because the shared planner drops a request whose
mapping waits for a field the document does not have. A later write supplies
it, through the field or explicitly.

### Externally maintained artifacts

An asset enrichment with no producer and no input (no `field`, `template` or
source artifact) is maintained outside Antfly:

```json
{ "name": "notes", "kind": "asset", "content_type": "text/plain" }
```

Only seeds write it: `_artifacts.notes`, or its `seed` field mapping. Like
`_embeddings` on an external index it binds to no input, so a seed is accepted
unconditionally: identical is `unchanged`, a different value always replaces
it (`replaced`, `existing_origin: external`, under either policy), and `null`
deletes it. A seed-only transform needs no `expected_version`. Source
rewrites, edits and replays keep it, deleting the document removes it, and
reads report `origin: external`; the seed view exports it. No producer, chunk
collection or full-text projection may consume one yet: nothing would publish
the receipt a downstream producer binds to.

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

### Chains

Seeds chain through upstream outputs in one write. Chunks of an asset
(`source_artifact_name`) bind to that asset's output prepared earlier in the
same write, seeded or generated synchronously, read in the same shape the
replay worker chunks; vectors of those chunks bind to the chunk rows prepared
in the write. An upstream whose output is not in the write cannot bind a
chunk seed (`ArtifactSeedNotAccepted`). A document extraction and the chunks
of its units chain the same way (see below).

### Chunk retention

The chunk producer records no input identity of its own and re-chunks every
revision it processes. An accepted chunk collection is therefore recorded
with the input it was accepted for (`artifact_seed_chunks.zig`): the
digest of the producer's source text and chunker configuration, beside the
seeded chunks, in the document's asset-state family (so deletion, backup
and restore carry it). Synchronous preparation (`getOrCreateChunks`) and the
replay worker (`getOrCreateRequestChunksWithProvider`, including chunks of an
upstream output) consult it before chunking: while the input is unchanged
the seeded set is the output, so unrelated edits and replays keep it. A
changed input or chunker configuration falls back to the chunker. `null`
chunks the input again in that same write and deletes the record.

### Rich document extraction

Besides `{ "text": ... }`, a document extraction seed may carry the
extracted units themselves:

```json
{
  "url": "gs://bucket/interview.pdf",
  "_artifacts": { "extracted": { "value": { "units": [
    { "text": "Quarterly revenue grew.", "page_number": 3, "page_label": "iii", "page_bbox": [0, 0, 612, 792] },
    { "text": "Hello there. Hi.", "spans": [
      { "char_start": 0, "char_end": 12, "start_ms": 0, "end_ms": 900, "speaker": "SPEAKER_00" },
      { "char_start": 13, "char_end": 16, "start_ms": 1000, "end_ms": 1400, "speaker": "SPEAKER_01" } ] }
  ] }, "source": { "generation": "42" } } },
  "_chunks": { "passages": [
    { "_chunk_id": 0, "_content": "Quarterly revenue", "_start_char": 0, "_end_char": 17, "_parent_unit_id": "page:000001" },
    { "_chunk_id": 0, "_content": "Hello there.", "_start_char": 0, "_end_char": 12,
      "_start_time_ms": 0, "_end_time_ms": 900, "_parent_unit_id": "transcript:000002", "_speaker": "SPEAKER_00" }
  ] }
}
```

Each unit becomes an extraction unit exactly as given (`unit_type` defaults to
`page` with a page number, `transcript` with spans, else `document`, the
plain text route's unit; ids are `<unit_type>:<ordinal>`). Page number, label,
bounding box and rotation, transcript phrases with their speakers, and layout
below the page are recorded when present; nothing absent is fabricated.

Layout uses the native shapes: `text_regions` (grounded spans for citations
and highlights) and `text_blocks` (reading-order blocks, listed in reading
order), each `{ span, bbox, font_size?, bold?, italic? }`. Spans are UTF-8 byte
offsets on character boundaries of the unit text, in text order. Boxes are
`[x0, y0, x1, y1]` in the page's own space, the space of `page_bbox` (source
page points, as native extraction records them), and must lie within it; a
unit with layout must therefore carry `page_bbox`. Units keep
`method: "seeded"`, so consumers can tell the geometry came from the caller. Units go through the
ordinary manifest, unit and child materialization with no fetch, decode, OCR
or transcription; the canonical seed value is the input identity, so a
resumed import is `unchanged`.

Chunks of an extraction's chunk consumers are unit-scoped: each names its
unit (`_parent_unit_id`) and may carry its speaker (`_speaker`). They are
accepted by the extraction seeded in the same write, must name one of its
units, and are recorded per unit with the digest of that unit's text, so
every site that derives unit chunks (desired keys, writes, skip checks,
full-text rebuilds, worker materialization) uses the seeded chunks while the
unit text and chunker are unchanged. A plain chunk producer refuses
unit-scoped chunks. The seed view exports both: the units form (with layout
and transcript phrases) whenever the extraction has pages, typed or several
units, and unit-scoped chunks with their unit and speaker.

Vectors of those unit chunks are seeded under the embedding's name with each
entry naming its unit: dense `[{ parent_unit_id, chunk_id, vector }]` (no
`window_index`: unit chunks are embedded whole) and sparse
`[{ parent_unit_id, chunk_id, indices, values }]`. The extraction producer writes a seeded vector in place of
queueing that chunk for its provider batch, with exactly the identity it
would record (the chunk text's hash at the provider generation) and the
`seeded` flag; unseeded chunks are embedded as usual. Each vector must name a
text chunk of an extracted unit (`InvalidArtifactSeed`), the extraction must
be seeded in the same write, and an embedder must be configured so the
identity can name its provider generation (`ArtifactSeedNotAccepted`
otherwise). Replays reuse the vectors through the ordinary hash checks; a
changed chunk text is embedded again. `null` re-materializes the units and
embeds their chunks again in that write (`deleted`). Outcomes are reported per
embedding name, and the seed view exports the vectors with their unit.

Chunk-derived sparse vectors of plain chunk collections bind the same way as
dense ones: each seed names a chunk of this revision (inline chunks, or chunk
rows prepared in the write) and is stored with the identity the sparse
producer records for that chunk text at its revision; unseeded chunks are
embedded.

### Raft: leader seed preparation

On a Raft table the leader accepts a write's explicit seeds before proposing
it (`DB.prepareSeedEnrichments`). Preparation runs exactly as a commit
would, under the apply lock and without inference, and yields a `seeds`-mode
`prepared_enrichments_json` packet: the accepted artifact bytes, seeded chunk
and unit records, pending work for unseeded outputs, guards on each touched
document, and the per-item outcomes. Replicas, the leader included, apply that
packet instead of accepting the seeds themselves, so seeded output never
depends on a replica's embedder or producer configuration, and a replica
without an embedder applies it byte for byte. Guards bind the packet to the
state it was prepared against; a concurrent change fails the entry with
`VersionConflict` on every replica alike.

The packet requires Raft batch protocol 76. It is an optional upgrade: the
leader prepares only once every applying peer can decode protocol 76, and
drops the packet if the proposal is encoded at an older version, in which case
replicas accept the seeds at apply as before. `direct_enrichments` writes are
leader-prepared already: their packet carries the seeded output beside the
generated one, and a seeded output is never embedded (window prewarming skips
it). Transactions (including multi-shard 2PC batches), enrichment-level sync
levels and seeds supplied only through field mappings keep apply-time
acceptance; mapped text and JSON assets depend on no replica-local state, while
mapped vectors bind the replica's embedder identity.

### Late inference results

Generated output that was planned before a seed was accepted or replaced must
not overwrite it. A deferred asset result records the accepted output's
digest (or its absence) when its work is planned; publication rechecks it in
the guarded commit and drops the result with `EnrichmentSourceChanged` when a
seed changed it meanwhile. Chunk and vector results are fenced by their
source digest and per-chunk recipe identity as before.

### Lite and the binary wire

Lite uses the DB batch parser and coalescer, so seeds, transforms and
`seed_conflict` behave identically. Public `expected_version` predicates are
lowered on both C entry points. `antfly_db_batch_wire` version 2 appends the
JSON `transforms` array (with `expected_version` and seed paths) after the
binary writes; version 1 bodies are still accepted
(`go/pkg/embedded`: `EncodeBatchWireWithTransforms`).

## Not yet accepted

These fail with `ArtifactSeedNotAccepted` rather than regenerating:

- Producers whose input is outside the write: neighbor or context sources,
  committed graph adjacency, grouped sources, prior context, privacy-managed
  artifacts, PDF page-image embeddings, chunks of transcription
  (`media_output`) assets.
- Windowed (media-window) sparse chunk vectors, which have no seed shape.
- `copy` assets (free to regenerate).

## Remaining work

These need work this repository cannot verify on a 16.8 GB machine (the data
runtime, storage kernel and server API test builds declare 19-21 GB), or are
subsystem features of their own:

- Per-item outcomes in server batch responses. The proposing leader holds
  them in its seed preparation packet, but the table write source returns no
  value to the HTTP handler (the data runtime and the HTTP handler reach the
  API through different module roots, so thread state cannot carry them), and
  forwarded writes would need the internal forwarding response to carry them.
  Until then, plan with `validate_only`.
- Leader seed preparation for transactions and multi-shard 2PC batches, for
  enrichment-level sync levels, and for seeds that come only from field
  mappings (the leader would need the table's mapping catalog before
  proposing). A cross-shard 2PC seed transform test.
- Generating only the missing pages of a partial extraction seed: the
  document extraction pipeline processes whole documents and has no page
  selection to merge with seeded units.
- Chunk inventories beyond the 4,095-chunk atomic-mutation ceiling through
  staged publication (`docs/design/media-pass.md`), and migration progress
  counters (imported, generated, rejected, pending) beside per-write outcomes.
- Readiness counters for `pending` outputs, and downstream consumers of
  externally maintained assets.
- The serverless coordinator rejects seed paths until serverless publication
  accepts seeds; its fixed pipelines have no named enrichments to seed.
