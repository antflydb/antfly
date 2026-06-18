# Derived Document Hierarchy

## Summary

Antfly should model rich files as a derived document hierarchy: a source row owns a versioned tree of derived artifacts such as pages, sections, slides, sheets, chunks, embeddings, OCR output, and entity mentions.

The hierarchy gives files predictable lifecycle semantics while still allowing large child ranges to split across shards.

In short:

```text
source file row
  -> document units          pages, sections, slides, sheets, email parts
  -> chunks                  retrieval-sized children of units
  -> embeddings/full-text    indexes over units and/or chunks
  -> graph artifacts         entities, mentions, relations, provenance
```

This makes `remotePDF` a useful low-level helper, but not the primary product abstraction. The higher-level abstraction is file extraction into canonical, versioned child artifacts.

## Goals

- Support tables containing many file types without requiring one enrichment per MIME type.
- Keep extraction, chunking, embedding, OCR, and graph extraction aligned around the same document hierarchy.
- Preserve source file lifecycle semantics: updates, deletes, retries, and reindexing should converge the derived child set for a parent.
- Allow very large files to split across shards without losing parent-owned update coordination.
- Track provenance from search results, chunks, entities, and graph edges back to the original file and unit.
- Avoid forcing all extracted content into a single giant field on the parent row.

## Terminology

- **Source document**: the user-written table row, typically one row per file or external object.
- **Derived document hierarchy**: the complete tree of materialized artifacts owned by a source document.
- **Derived artifact**: a named, versioned collection of child records produced by enrichment.
- **Document unit**: a larger child document extracted from a file, such as a PDF page, DOCX section, PPT slide, XLSX sheet, HTML article, image, email part, or transcript segment.
- **Chunk**: a retrieval-sized child of a document unit.
- **Artifact manifest**: parent-owned state describing the current artifact generation, fingerprints, route decisions, child ranges, and merge progress.
- **Interleaved artifact range**: a splittable physical range under a parent-owned logical hierarchy.
- **Generation**: a monotonically increasing version of an artifact for a parent source document.

## Source Rows

Source rows should remain small and stable. They describe the original file and user metadata:

```json
{
  "id": "file_123",
  "url": "s3://legal/contracts/acme.pdf",
  "filename": "acme.pdf",
  "mime_type": "application/pdf",
  "etag": "\"9abc...\"",
  "sha256": "..."
}
```

The source row should not need to hold all extracted text. Extracted text belongs in derived artifacts.

## Canonical Artifact Shape

Different file-type routes should write the same normalized artifact shape. For example, `document_units_v1` can contain PDF pages, DOCX sections, slides, sheets, images, HTML articles, and transcript segments:

```json
{
  "_parent_doc_key": "file_123",
  "_artifact_name": "document_units_v1",
  "_generation": 7,
  "unit_id": "page:000012",
  "unit_type": "page",
  "text": "extracted page text...",
  "content_type": "text/plain",
  "provenance": {
    "source_url": "s3://legal/contracts/acme.pdf",
    "filename": "acme.pdf",
    "page_number": 12,
    "method": "pdf_text",
    "ocr_used": false
  }
}
```

Chunks then reference both the source document and the unit:

```json
{
  "_parent_doc_key": "file_123",
  "_parent_unit_key": "file_123/document_units_v1/page:000012",
  "_artifact_name": "document_chunks_v1",
  "_generation": 7,
  "chunk_id": "chunk:000003",
  "text": "retrieval-sized text...",
  "provenance": {
    "unit_id": "page:000012",
    "char_start": 1024,
    "char_end": 1840
  }
}
```

## File-Type Routing

Rules should route many file types into the same canonical artifact instead of creating a separate artifact per type.

Example configuration shape:

```json
{
  "name": "document_units_v1",
  "kind": "document_extraction",
  "source": {
    "url_field": "url",
    "content_type_field": "mime_type",
    "filename_field": "filename",
    "etag_field": "etag",
    "checksum_field": "sha256"
  },
  "routes": [
    {
      "match": { "content_type": "application/pdf" },
      "extractor": {
        "type": "pdf",
        "unit": "page",
        "mechanical_text": true,
        "ocr_fallback": true
      }
    },
    {
      "match": { "extension": [".docx"] },
      "extractor": {
        "type": "docx",
        "unit": "section"
      }
    },
    {
      "match": { "content_type": "text/html" },
      "extractor": {
        "type": "html",
        "unit": "article"
      }
    },
    {
      "match": { "content_type_prefix": "image/" },
      "extractor": {
        "type": "ocr",
        "unit": "image"
      }
    }
  ],
  "output_artifact": "document_units_v1"
}
```

Routing should use, in order:

1. Explicit content type from the source row.
2. HTTP/S3 response content type.
3. Filename extension.
4. Magic-byte sniffing.
5. A configured default route or unsupported-file result.

The route result should be persisted in the artifact manifest so retries and downstream enrichments are deterministic.

## Relationship To Existing Enrichments

The hierarchy should make enrichments composable:

```text
document_extraction(file.url) -> document_units_v1
chunk(document_units_v1.text) -> document_chunks_v1
embedding(document_chunks_v1.text) -> dense/sparse vectors
full_text(document_units_v1.text) -> unit-level lexical search
full_text(document_chunks_v1.text) -> chunk-level lexical search
entity_extraction(document_units_v1.text) -> entity_mentions_v1
relation_extraction(document_units_v1.text) -> relation_mentions_v1
```

This extends the existing ideas:

- Embedding enrichments already know how to chunk source text.
- Graph enrichments already need extracted entities and relations.
- Asset enrichments already model generated/copy artifacts.

The missing unifying concept is that large files need a first-class child-document layer between the source row and retrieval chunks.

## Sharding Model

The hierarchy should behave like an interleaved table logically, but not force all children onto the parent shard physically.

Logical keyspace:

```text
/file_123
/file_123/document_units_v1/page:000001
/file_123/document_units_v1/page:000002
/file_123/document_chunks_v1/page:000002/chunk:000000
/file_123/entity_mentions_v1/page:000002/mention:000004
```

Physical placement can split child ranges:

```text
shard A: /file_123
shard B: /file_123/document_units_v1/page:000001..page:000500
shard C: /file_123/document_units_v1/page:000501..page:001000
```

The key distinction is control-plane ownership versus physical placement.

The parent shard owns:

- Artifact manifests.
- Source fingerprint and extractor version state.
- Generation clocks.
- Child range descriptors.
- Linear merge plans.
- Delete/retry coordination.

Child range shards own:

- Unit records.
- Chunk records.
- Embedding artifacts for child records.
- Full-text postings for child records.
- Entity and relation mentions attached to child records.

This keeps lifecycle semantics parent-owned while allowing large PDFs, notebooks, spreadsheets, and archives to distribute across shards.

## Update Semantics

Updates should use linear merge semantics over a parent-owned derived child set.

For each source document and artifact:

1. Compute the new source fingerprint:
   - source row fields used by extraction
   - file checksum or ETag when available
   - route decision
   - extractor config
   - extractor implementation version
2. If the fingerprint is unchanged, skip extraction.
3. Extract the new ordered child set.
4. Assign deterministic child IDs.
5. Compare against the existing artifact manifest.
6. Build a merge plan:
   - keep unchanged children
   - upsert changed/new children
   - delete stale children
7. Dispatch idempotent child-range work to the shards that own the affected ranges.
8. Advance the artifact manifest generation once the merge converges.

Example merge plan:

```json
{
  "parent": "file_123",
  "artifact": "document_units_v1",
  "from_generation": 6,
  "to_generation": 7,
  "operations": [
    { "op": "keep", "range": "page:000001..page:000300" },
    { "op": "upsert", "range": "page:000301..page:000340" },
    { "op": "delete", "range": "page:000341..page:000360" }
  ]
}
```

Downstream enrichments should use the same pattern. If page 301 changes, Antfly should re-chunk, re-embed, and re-extract entities for page 301 without touching pages 1 through 300.

## Streaming Extraction And Memory Bounds

Large files must not require Antfly to hold every extracted unit, chunk payload, and derived replay document in memory at the same time.

The extraction API supports a sink-style stream:

```text
downloaded bytes
  -> extractor route begin(content_type, route_type)
  -> unit(page/section/slide/sheet/part)
  -> unit(...)
  -> extractor route end
```

The async enrichment runtime uses this as a bounded multi-pass stream:

1. First pass:
   - route the file
   - emit units one at a time
   - complete generated text for pending OCR/transcription units when configured
   - cache completed generated-text units by unit id for this extraction attempt
   - collect compact unit keys, unit fingerprints, chunk keys, and unit text lengths
   - build source state and manifest range descriptors from compact metadata
2. Second pass:
   - re-run the extractor stream
   - reuse cached OCR/transcription output instead of invoking the producer again
   - materialize each unit and its chunks
   - flush store writes in bounded batches
   - write the converged manifest after materialization
3. Replay publish pass:
   - re-run the extractor stream after the converged manifest commit
   - reuse cached OCR/transcription output
   - enqueue changed artifact keys and full-text replay documents for the normal replay window
   - write the compact source state only after replay publication succeeds

This deliberately trades extra deterministic parsing work for bounded resident memory. Generated text is not recomputed across passes, because OCR and transcription can be expensive and nondeterministic; generated text is cached in-memory for the extraction attempt and counted in the document extraction working-set slice. For PDFs, page text is emitted and freed page by page instead of collecting the full `Result.units` array. Non-PDF routes currently use a buffered compatibility adapter and can move to native streaming incrementally.

The manifest should not need full unit payloads to describe child ranges. It can use unit keys, chunk keys, and unit text lengths to preserve the same range policy:

```text
unit range boundary = min(256 units, 1 MiB unit text)
chunk range boundary = 256 chunks
```

The synchronous precompute path still uses the compatibility `Result.units` API. That path is protected by the hard payload limit described below, while async replay is the path that needs bounded streamed materialization.

### Source Payload Limits

Inline `data:` sources for document-extraction asset inputs, including rendered source templates, are preflighted before persistence and before async replay. Valid data URIs whose decoded size is greater than the configured remote-content maximum are rejected with the same oversized-stream error used by remote fetches.

This is intentionally separate from resource management. A hard source limit prevents a single oversized inline PDF from being persisted, decoded, or routed in the first place. Resource-manager pressure only controls work that is otherwise within the configured document size envelope.

### Durability And Staging

The compact descriptor ledger is in-memory in the current implementation. If extraction crashes before the final manifest/state write, the next replay can recompute descriptors from the source bytes.

However, streamed artifact payloads need stronger semantics before this becomes fully crash-atomic. Writing directly to stable unit/chunk keys before the final manifest can expose mixed old/new content after a crash if readers bypass or outlive the in-progress manifest. The fully correct design is generation-scoped staging:

```text
/_internal/doc_extract_stage/<parent>/<artifact>/<generation>/units/page:000001
/_internal/doc_extract_stage/<parent>/<artifact>/<generation>/chunks/page:000001/chunk:000000
```

Then the final manifest atomically publishes the generation by referencing the staged generation, and readers resolve artifact keys through the manifest generation. A janitor removes stage generations that are not referenced by a converged manifest and whose attempt marker has expired.

Until generation-scoped staging is implemented, streamed writes are a bounded-memory improvement, not a complete atomicity boundary. Search and artifact readers should continue to treat the manifest/state as authoritative. Downstream derived replay is published only after the converged manifest commit, and the source state used by skip-by-hash is written only after replay publication succeeds. That keeps replay publication retryable if the worker fails between the manifest commit and replay publish. The remaining gap is crash atomicity between stable child artifact writes and the final manifest, which generation-scoped staging is intended to close.

### Resource Management

Document extraction has a dedicated `document_extraction.working_set` resource-manager slice. The async streaming runtime accounts:

- downloaded bytes
- current unit text and layout metadata
- generated OCR/transcription cache bytes retained for this extraction attempt
- chunker output for the current unit
- pending store-write batches
- pending derived replay-window payloads

The runtime updates this slice as the current working set changes and releases it on all exits. Temporary pressure from other work returns `ResourceBudgetExceeded`; the enrichment worker treats that as retryable. If the document extraction working set itself exceeds the slice hard limit, the runtime returns a non-retryable extraction error and writes a failed manifest instead of livelocking one oversized document.

The remaining resource-manager improvement is a first-class reservation classification API:

```text
granted
would_fit_later
exceeds_hard_limit
```

The current runtime implements that split locally for document extraction. A later shared API should expose the same distinction directly so other pipelines can avoid open-coded hard-limit checks.

## Delete Semantics

Deleting a source document should tombstone the parent and then remove all artifacts under the parent hierarchy:

```text
delete /file_123
delete /file_123/document_units_v1/*
delete /file_123/document_chunks_v1/*
delete /file_123/entity_mentions_v1/*
delete vectors/postings/graph edges owned by those child records
```

The parent manifest should make this bounded and restartable even when child ranges live on other shards.

## Query Semantics

Queries should be able to target different levels:

- File-level filters: filename, MIME type, owner, timestamps, source metadata.
- Unit-level search: page, section, slide, sheet, transcript segment.
- Chunk-level retrieval: dense/sparse/full-text search over retrieval chunks.
- Graph search: entities and relations with source unit provenance.

Search results should include enough ancestry to roll up:

```json
{
  "doc_key": "file_123/document_chunks_v1/page:000012/chunk:000003",
  "parent_doc_key": "file_123",
  "parent_unit_key": "file_123/document_units_v1/page:000012",
  "artifact_name": "document_chunks_v1",
  "score": 0.82,
  "fields": {
    "text": "..."
  }
}
```

The query layer can then return the chunk, the page/section context, or the source file depending on the request.

## Graph And Entity Extraction

Entity and relation extraction should explicitly choose its source level:

```json
{
  "name": "entity_mentions_v1",
  "source_artifact": "document_units_v1",
  "scope": "unit",
  "extractor": {
    "type": "entity_extraction",
    "schema": ["person", "organization", "date", "location"]
  }
}
```

Entity mentions should retain provenance:

```json
{
  "_parent_doc_key": "file_123",
  "_parent_unit_key": "file_123/document_units_v1/page:000012",
  "_artifact_name": "entity_mentions_v1",
  "entity_text": "Acme Corp",
  "label": "organization",
  "span": { "start": 481, "end": 490 },
  "provenance": {
    "filename": "acme.pdf",
    "page_number": 12,
    "chunk_id": "chunk:000003"
  }
}
```

Graph edges can then point to canonical entities while preserving evidence back to the exact file unit and span.

## Low-Level Template Helpers

Helpers like `remotePDF` remain useful, but they should not be the main mixed-file abstraction.

Useful low-level helpers:

```handlebars
{{remotePDF url=pdf_url}}
{{remoteMedia url=file_url mode="render"}}
{{remoteText url=text_url}}
```

Better high-level helper or producer:

```handlebars
{{remoteDocumentText url=file_url contentType=mime_type filename=filename ocrFallback=true}}
```

or:

```json
{
  "kind": "asset",
  "name": "document_units_v1",
  "producer_json": {
    "type": "document_extraction",
    "config": {
      "routes": []
    }
  }
}
```

For tables with many file types, route dispatch should live in the producer/extractor config, not in user-written templates.

## Implementation Sketch

The feature can be built incrementally.

### Phase 1: Canonical extraction artifact

- Add a document extraction asset producer that fetches a URL and emits normalized document units.
- Support PDF mechanical text, text files, HTML, and data URLs first.
- Store extracted units as named artifacts under the parent document.
- Persist an artifact manifest with source fingerprint and route decision.

Current implementation status:

- `asset` enrichments can now use producer type `document_extraction`.
- The producer is handled internally rather than by the external model-backed asset producer runtime.
- The source field resolves to a URL, including `data:` URLs through the existing remote-content downloader path.
- Extraction currently routes PDF mechanical text, text, JSON/CSV-like text, simple HTML, RFC 822 email, OOXML Office packages, ZIP archives, image media, and audio media into canonical units.
- The route config supports a public `route_preset` contract plus limited ordered overrides. `mixed_files` is the default preset and runs the built-in routes for `pdf`, `html`, `text`, `email`, `docx`, `pptx`, `xlsx`, `archive`, `ocr`/`image`, `audio`/`transcript`, and `unsupported` extractors after caller-provided overrides. `explicit_only` disables built-in fallback and fails closed with a structured unsupported route when no configured route matches. Overrides can match exact content type, content-type prefix, filename/URL extension, or configured magic-byte prefix.
- The default detector now sniffs PDF magic, common HTML prefixes, and valid UTF-8 plain text for missing or generic content-type metadata, so mixed-file tables can still produce canonical units when upstream file metadata is incomplete.
- Route matching can hydrate effective filename and content type from per-row source metadata fields such as `source.filename_field` and `source.content_type_field`, so one mixed-file table can route documents without one enrichment per MIME type.
- Unsupported content types now produce a structured `route_type: "unsupported"` manifest with `unsupported_reason`, zero units, and no searchable child documents instead of failing the enrichment.
- Unit records are stored under the parent document's asset namespace with deterministic unit IDs such as `document:000001`, `article:000001`, and `page:000001`.
- A parent-owned manifest records source fingerprint, content type, route type, and unit count.
- A parent-owned state record tracks the current source fingerprint and unit keys so stale units are deleted when the source changes or is cleared.
- The DB maintenance surface can inspect document artifact manifests with parsed generation, route, child count, child range, and merge-plan summary fields while preserving raw manifest/state JSON.
- The DB maintenance surface can force a synchronous reprocess of a document extraction artifact for the current source row without deleting prior state, so stale-child cleanup remains diff-based.
- Bound table read/write sources now expose manifest inspection and explicit reprocess hooks, so local API callers can use the same maintenance contract without reaching into `DB` directly.
- Public table HTTP handlers now expose per-document artifact manifest inspection and explicit reprocess controls for local/bound table sources.
- Hosted/provisioned table reads route per-document artifact manifest requests to the data group that owns the source document key, including remote internal group fanout when the owner is on another node.
- Hosted/provisioned table writes route explicit artifact reprocess requests through the same owner-group routing, with local group handlers opening the managed shard DB and remote groups using internal HTTP fanout.
- The DB, table read sources, public HTTP handlers, internal group routes, hosted/provisioned routing, generated OpenAPI/httpx routes, and typed Zig clients now expose `GET /db/v1/tables/{table}/documents/{key}/artifacts` to list available artifact manifests for a source document.
- Generated OpenAPI/httpx routing now includes `GET /db/v1/tables/{table}/documents/{key}/artifacts/{artifact}` and `POST /db/v1/tables/{table}/documents/{key}/artifacts/{artifact}:reprocess`, plus typed Zig client methods for both operations.
- Public artifact list, manifest inspection, and reprocess routes now apply the caller's effective source-row filter before exposing or mutating artifact control-plane state; hidden source documents return `404` to avoid leaking artifact existence.
- Artifact inspection responses now expose typed source URL, source fingerprint, content type, manifest version, child range descriptors, and merge-plan generation/granularity summaries in addition to preserving raw manifest/state JSON.
- Public/internal routing now includes a bounded operational table-range repair endpoint, `POST /db/v1/tables/{table}/artifacts/{artifact}:reprocess`, with `from_key`, `to_key`, `limit`, and `shard_cursors` controls plus scanned/reprocessed/skipped/failed counts, per-key failure codes, per-shard continuation cursors, `pending_shards`, and `reprocess_status: in_progress|complete`. Hosted/provisioned implementations fan this out to shard-local group handlers, aggregate the bounded pass response, and can resume caller-supplied shard cursors directly for distributed repair controllers.
- Public authorization now classifies artifact list and manifest inspection as table-read operations, and per-document/table-range artifact reprocess as table-admin operations, before row-filter checks hide source documents the caller cannot see.
- Artifact manifest inspection now has explicit `detail=summary|raw` response modes. Summary mode returns the stable typed manifest fields only; raw mode adds opaque `manifest_json` and `state_json` and requires table-admin permission when public authentication is enabled. Auth-enabled requests default to summary when `detail` is omitted, while unauthenticated/local runtimes keep raw detail for compatibility.
- Public/generated routing now includes durable table artifact reprocess jobs: `POST /db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs` creates a job, `GET /db/v1/tables/{table}/artifacts/{artifact}/reprocess-jobs/{job}` polls status, `POST .../{job}:advance` runs one bounded pass and persists the returned continuation cursors, and `POST .../{job}:cancel` marks a nonterminal job cancelled. Jobs are table-admin operations and can persist status in the API server job store when configured.
- PDF mechanical extraction now emits page-level provenance on units and unit-derived chunks, including `page_number`, a stable `page_label`, `page_bbox`, `page_rotation`, source-document character spans, and unit-level best-effort text-region spans/bounding boxes under `format_provenance.text_regions`. Mechanically empty pages are preserved as stable `page` units with `method: "pdf_ocr_pending"` and `extraction_status: "pending_ocr"` so scanned PDFs have deterministic OCR fallback targets.
- Unit payloads are emitted as derived documents for full-text indexes whose source artifact matches the document-unit artifact name.
- The synchronous precompute path and async enrichment runtime path both use the same artifact key/state contract, including unit fingerprints in state.

### Phase 2: Unit-aware chunking and indexing

- Allow chunk enrichments to read from a source artifact instead of only a source field/template.
- Preserve parent document and parent unit keys on chunks.
- Add full-text and embedding indexing over units/chunks.
- Keep unchanged units/chunks stable across parent updates.

Current implementation status:

- Chunk enrichments can declare `source_artifact_name` pointing at a document-unit asset such as `document_units_v1`.
- Document extraction now fans out those chunk enrichments over each extracted unit immediately in both synchronous precompute and async runtime paths.
- Unit-derived chunks use a unit-scoped chunk key under the parent document's chunk artifact range, avoiding collisions between `chunk:000000` on different pages or sections.
- Chunk payloads carry `_parent_doc_key`, `_parent_unit_key`, `_parent_unit_id`, `_source_artifact_name`, `_artifact_name`, and `_source_field`.
- Full-text indexes whose `chunk_name` matches the unit-derived chunk artifact receive derived documents for those chunks.
- Dense embedding enrichments whose `source_artifact_name` points at the unit-derived chunk artifact now materialize durable derived embedding artifacts under the unit-scoped chunk keys during synchronous document extraction.
- Sparse embedding enrichments whose `source_artifact_name` points at the unit-derived chunk artifact now materialize durable derived embedding artifacts under the unit-scoped chunk keys during synchronous document extraction.
- Managed dense indexes can replay and search those unit-derived chunk embedding artifacts while preserving the unit-scoped chunk key as dense metadata.
- Sparse indexes can plan against explicit sparse embedding enrichments and search unit-derived chunk embedding artifacts using unit-aware public artifact IDs.
- Chunk-backed full-text/vector result shaping can return unit-derived chunks directly with `return_mode: "chunk"` or as nested child hits with `return_mode: "parent_with_chunks"`.
- Public query responses now emit a stable `hierarchy` envelope for derived unit/chunk/embedding hits and source-level rollups, including `level`, `parent_doc_key`, optional `parent_unit_id`, artifact identity, and nested child chunks.
- Hierarchy responses now include a stable `ancestors` envelope. Source-level payloads carry `ancestors.source.document`, unit hits carry `ancestors.unit.document`, chunk hits expose unit ancestry fields and provenance recovered from the chunk payload, and direct chunk searches can hydrate DB-backed source/unit ancestor documents when `hierarchy.include` requests them.
- Public artifact IDs now round-trip document-unit asset artifacts, so unit-level hits can be exposed as first-class derived hierarchy results instead of opaque internal keys.
- Public hierarchy controls now accept `return_level: "unit"` and `return_level: "mention"`. Mention hits are recognized from `antfly.resolution_mention.v1` evidence artifacts and returned with a stable hierarchy evidence envelope.
- Document units now carry source-document character spans where the extractor can compute them, and chunk artifacts now emit a `provenance` envelope with explicit `offset_basis`, chunk-local `char_start`/`char_end`, unit-local offsets for unit chunks, and document-global offsets when available.
- Document-unit and unit-derived chunk provenance now include a nested `format_provenance` contract (`antfly.document_format_provenance.v1`) for format-specific page geometry, coordinate system, extraction method, OCR use, source content type, page labels, page bounding boxes, and rotation while preserving the older flat page fields for compatibility.
- Document extraction state records both `unit_keys` and `chunk_keys`, so source updates and source clearing delete stale units and stale unit-derived chunks.
- Existing chunk-aware scans recognize both legacy whole-document chunk keys and the new unit-scoped chunk keys.
- Derived embedding key recognition now understands embeddings attached to unit-scoped chunk keys, including base-key recovery for cleanup/replay paths.
- Artifact public IDs can round-trip unit-scoped chunks and embeddings derived from unit-scoped chunks without collapsing chunks from different units.
- Public query requests now accept initial `hierarchy` controls that map `source`, `unit`, and `chunk` return levels plus source rollups onto the existing derived-artifact result modes.
- Query results now expose matched chunks through existing chunk return modes and include a stable hierarchy envelope with response-local ancestor hydration plus DB-backed source/unit hydration for direct chunk hits when requested. First-class `mention` return levels are backed by `antfly.resolution_mention.v1` artifacts and response evidence envelopes.
- Incremental chunk reuse is currently key-stable by unit id and chunk id and backed by parent-owned range/merge descriptors with per-unit fingerprints. The local execution path can skip stable artifact/vector-only unit subtrees when existing artifacts prove the subtree is already materialized.

Still remaining in Phase 2:

- OCR fallback confidence and scanned-region coordinates now flow from structured producer output; deeper PDF-reader-native text-region coordinates remain Phase 5 follow-up work.

### Phase 3: Distributed child ranges

- Introduce interleaved artifact range descriptors.
- Route child records by parent plus artifact range.
- Allow large child ranges to split independently from the parent shard.
- Make merge plans idempotent and resumable across child shards.

Current implementation status:

- Document extraction manifests now use `manifest_version: 2` and carry a monotonically advancing artifact `generation`.
- Manifests include deterministic `child_ranges` for unit and chunk child key ranges, including range IDs, start/end keys, child counts, placement, and split-boundary metadata.
- Document extraction state now stores per-unit fingerprints alongside unit keys, while retaining the older key arrays for cleanup compatibility.
- Synchronous precompute and async enrichment runtime manifests include a converged `merge_plan` with `from_generation`, `to_generation`, `operation_granularity: "unit_fingerprint"`, and idempotent keep/upsert/delete summaries derived from previous state versus desired state.
- Stable units whose unit fingerprint matches previous state can skip local artifact rewrites and embedding regeneration when the stored unit, chunk, and embedding artifacts already exist.
- Full-text consumers for stable units are still routed through derived-document upserts that read from the stored unit/chunk artifacts. This avoids silent index holes without requiring the producer to prove every text shard already contains the child document.
- The first implementation records range placement as `parent`; it establishes the parent-owned descriptor contract without moving child writes to separate shards yet.
- Asynchronous document extraction now durably writes an `in_progress` merge plan before child artifact writes and replaces it with the converged plan after the child range batch commits. The in-progress manifest keeps the last committed generation while its merge plan records the intended `to_generation`, so replay after a crash does not skip a generation.
- Child range descriptors now carry additive route/ownership metadata (`owner_group_id`, `placement_generation`, `route_status`, `split_eligible`) through local manifest parsing, public manifest responses, internal group responses, and remote manifest fanout. Current writers mark ranges as `local_committed` and parent-owned, giving future split workers a stable field set to advance when a child range moves away from the parent shard.
- The DB control plane can now durably update a child range's `placement`, `owner_group_id`, `placement_generation`, `route_status`, and `split_eligible` fields in the manifest with replay-backed storage, so split workers have a persistent handoff point before remote routing is enabled.
- The distributed write abstraction, hosted/provisioned sources, HTTP client, and internal group write route now expose that child-range placement update, so a split worker can advance a parent-owned manifest even when the parent group leader is remote to the caller.
- Sync and async document extraction rebuilds preserve existing child-range placement metadata by stable range descriptor identity, preventing a later reprocess from resetting a split range back to parent-owned/local defaults.
- Unit and chunk child artifact payloads now carry descriptor-derived range routing fields (`_artifact_range_id`, `_artifact_range_kind`, `_artifact_route_status`, `_artifact_owner_group_id`). Local writers derive those values from the same deterministic child range ordering as the manifest, and forced reprocess rewrites unchanged child records with the current descriptor route metadata instead of resetting them to local defaults.
- The DB storage layer now has a direct child-range artifact apply primitive that can commit internal artifact writes/deletes plus the derived replay record on a destination shard without reclassifying child artifacts as user source rows.
- The internal group write API now exposes that child-range apply primitive, including route-scope validation for the parent document/artifact and a hosted/provisioned HTTP client path for remote owner groups.
- Hosted/provisioned local write execution now partitions generated document child artifacts by `remote_committed` child-range descriptors before the parent shard writes them locally, then dispatches the remote child batch to the recorded owner group. This covers generated artifact writes/deletes and derived document/vector replay payloads for remote-owned unit and chunk ranges.
- Split finalization now marks parent-owned, split-eligible child ranges that physically move to the split-off shard as `remote_committed`, assigns the new owner group, advances placement generation, and records the manifest update before the parent shard prunes the moved child rows.
- Remote child-range dispatch now uses a durable source-shard outbox. Parent commits atomically enqueue remote child batches under replay metadata, dispatcher success deletes the outbox row, and later writes or explicit drains can retry entries that survived a destination-group outage after the parent commit.
- Manifests now include a durable `coverage_plan` stating that full-text replay remains `stored_artifact_required`, replay suppression is false, and coverage watermarks are required before any future suppression. This keeps stable-unit replay correctness explicit rather than relying on an implicit code-path convention.
- Child range planning now uses deterministic initial thresholds: unit ranges split at 256 units or 1 MiB of unit text, whichever comes first, while keeping any single oversized unit intact as its own range. Chunk ranges keep the 256-child threshold, use `split_boundary: "chunk"` as the explicit second-level split policy under large units, and are numbered after the computed unit ranges so payload route metadata and manifest descriptors stay aligned in sync and async extraction. Manifests also carry a `range_policy` envelope with the active thresholds and oversized-unit policy for debugging and replay audits.

### Phase 4: Graph extraction over units

- Let graph/entity extractors target `document_units_v1` or `document_chunks_v1`.
- Store mentions and relation evidence as child artifacts.
- Link evidence to canonical graph nodes and edges.

Current implementation status:

- Graph artifact sources can now target asset-backed document units and chunk-backed document chunks, not only root asset artifacts.
- Managed graph replay retains chunk artifact changes, matches decoded artifact refs against the configured graph artifact source, and stores per-unit/per-chunk graph materialization state so one source document's child artifacts do not clobber each other's replay state.
- Graph source templates can use `_artifact.value...` paths, allowing unit/chunk payload ancestry such as `_parent_unit_key` and `_parent_unit_id` to become graph edge source IDs and evidence metadata.
- Resolvers can consume dedicated mention artifacts using the `antfly.entity_mention.v1` single-mention schema (`local_id`/`id`, `label`, `text`, optional `confidence`, optional `embedding`) in addition to legacy extraction artifacts with an `entities` array.
- Resolution replay now materializes first-class `antfly.resolution_mention.v1` evidence artifacts for canonical resolver decisions. Each artifact is keyed by source artifact, resolution artifact, and local mention ID, stores the resolver decision, canonical DocRef, mention text/label/confidence, and explicit mention/source/resolution artifact keys, and is retired through durable state alongside the existing doc-to-entity mention edges.
- Resolver replay accepts unit asset artifacts and chunk artifacts as source artifacts and scopes their resolution artifacts under the source artifact key, so unit/chunk-level entity extraction can resolve without colliding at the parent document's resolution artifact key.
- Resolver configs now declare a source subscription as `(source_artifact_kind, source_artifact)`, where the kind is `asset`, `chunk`, or `any`. The legacy default is `asset`, which preserves root/unit asset behavior; chunk-level mention streams opt into `chunk`, and bounded re-resolution repair scans enqueue matching chunk artifacts directly because they are shardable children rather than root asset-source-index entries.
- Resolver catalog admission now validates required local references and candidate modes before replay can start: resolver name, canonical table, source artifact, resolution artifact, and key template must be present; source and resolution artifacts cannot be the same stream; and candidate blocking must be one of the supported modes.
- Public hierarchy controls now accept `return_level: "mention"`. Query responses recognize `antfly.resolution_mention.v1` artifact hits and emit `hierarchy.level: "mention"` plus an `evidence` envelope with mention, canonical, resolver, source artifact, and resolution artifact references.
- Canonical mention provenance edges now roll up the durable evidence behind the edge in metadata: `target_table`, `mention_count`, and `mention_artifact_keys`. Multiple local mentions that resolve to the same canonical entity remain one graph edge, but the graph edge keeps links back to all underlying mention artifacts.
- Graph path response shaping now preserves edge metadata end-to-end, including local shortest-path results, distributed graph result cloning, remote graph result parsing, and public `PathEdge.metadata` JSON serialization. Mention provenance rollups can therefore surface through graph paths, not only through low-level edge reads.
- Graph result nodes and pattern bindings now expose a public `evidence` envelope that keeps raw provenance labels, parsed path-edge metadata, and aggregate mention rollups (`mention_count`, `mention_artifact_keys`) alongside the existing node document and path fields.

Still remaining in Phase 4:

- Future graph API polish can add explicit edge-neighborhood expansion fields, but the canonical mention evidence rollup now has a stable public response envelope on graph result nodes and pattern bindings.

### Phase 5: More file types and OCR fallback

- Add DOCX, PPTX, XLSX, image OCR, scanned PDF fallback, archives, and audio transcripts.
- Track extraction method, OCR use, page/section coordinates, and confidence in provenance metadata.

Current implementation status:

- RFC 822 email extraction now routes `message/rfc822` and `.eml` content into the canonical `document_units_v1` artifact, emitting deterministic `email_headers`, `email_body`, and multipart `email_part` units with email provenance. The email body extractor decodes common `base64` and `quoted-printable` transfer encodings, extracts `text/plain` parts directly, strips simple `text/html` parts, and skips non-text parts such as attachments.
- OOXML Office extraction now routes DOCX, PPTX, and XLSX content by official MIME type, extension, or explicit route config into the same canonical artifact. DOCX emits deterministic `section` units from `word/document.xml`, PPTX emits numerically ordered `slide` units from `ppt/slides/slide*.xml`, and XLSX emits `sheet` units from worksheet XML with shared-string and inline-string support. The implementation uses an in-memory ZIP central-directory reader with stored and raw-deflate entry support, XML entity decoding, and no temp-file dependency.
- ZIP archive extraction now routes `application/zip`, common ZIP aliases, `.zip` filenames, and explicit `archive` route configs into deterministic `archive_entry` units for text and simple HTML entries. Entries are sorted by path, unsafe path traversal names are skipped, unsupported/binary entries are skipped without failing the whole archive, and each unit carries `source_path` in the payload, provenance, format provenance, and unit fingerprint so archive entry renames are visible to incremental cleanup/replay.
- Image files now route by `image/*`, common image extensions, magic bytes, or explicit `ocr`/`image` routes into a deterministic `image` unit. Without an OCR producer they carry empty searchable text, `method: "ocr_pending"`, `extraction_status: "pending_ocr"`, byte length, source SHA-256, and OCR/transcript flags in the payload, provenance, format provenance, and unit fingerprint. When the document extraction config enables `ocr` and the runtime has an asset `reader` producer, sync and async extraction complete those pending units into searchable `ocr_text` units with `extraction_status: "completed"` and `ocr_used: true`.
- Scanned PDF fallback now uses the same pending-OCR contract at page granularity: pages with no mechanical text are kept in order as `page` units with `pdf_ocr_pending`, `pending_ocr`, page geometry, and source character spans, making later OCR completion an incremental unit update instead of a separate orphan artifact.
- Audio files now route by `audio/*`, common audio extensions, audio magic bytes, or explicit `audio`/`transcript` routes into a deterministic audio/transcript unit with empty searchable text, `method: "transcript_pending"`, `extraction_status: "pending_transcription"`, byte length, source SHA-256, and transcript/OCR flags in the same provenance/fingerprint contract. When the document extraction config enables `transcription` and the runtime has an asset `transcriber` producer, sync and async extraction complete those units into searchable `transcript_text` units with `extraction_status: "completed"` and `transcript_used: true`.
- OCR/transcription producers can return either raw text or structured JSON. Structured JSON with `text`, `confidence`, `bbox`/`ocr_bbox`/`coordinates`, and `warning`/`extraction_warning` is normalized into unit payloads, provenance, format provenance, unit fingerprints, generated-text request envelopes, and unit-derived chunk provenance as generic confidence, OCR/transcript-specific confidence, scanned-region coordinates, and extraction warnings.

## Open Questions And Proposed Direction

### Decision Summary

The first production shape should treat the hierarchy as an internal artifact tree with row-like query projections, not as user-authored child rows.

Recommended initial decisions:

- Implement document extraction as an asset producer that writes `document_units_v1` artifacts plus a parent-owned manifest.
- Route heterogeneous file types into the same canonical unit artifact using content type, filename, response headers, and magic-byte sniffing.
- Use document units as the first durable child layer. Chunks, vectors, full-text postings, and graph mentions should derive from units.
- Keep manifests, route decisions, fingerprints, generations, and merge plans parent-owned.
- Allow child artifact ranges to split independently across shards, with splits starting at unit boundaries.
- Return units/chunks/mentions as row-like query results with ancestry metadata rather than exposing them as normal mutable table rows.
- Treat canonical entity resolution as a separate process fed by source-owned mention artifacts.

The remaining design work is mostly API polish and operational policy, not the core storage model.

### Should document units be normal queryable rows, internal artifacts, or both?

Use both, but make internal artifact storage the source of truth.

Document units and chunks should be persisted in Antfly's artifact namespace so lifecycle, generations, deletes, retries, and range ownership remain parent-controlled. Query APIs can project those artifacts as row-like search results with stable keys, fields, ancestry, and scores.

This avoids forcing child records into the same semantics as user-authored rows while still making them searchable and retrievable.

Recommended direction:

- Store units, chunks, mentions, and relations as derived artifacts.
- Index them through normal full-text, vector, and graph index paths.
- Return them as row-like results with `_parent_doc_key`, `_parent_unit_key`, `_artifact_name`, and `_generation`.
- Consider a later virtual-table API for browsing artifacts directly, but do not require child artifacts to be first-class user rows in the initial design.

### What is the public API for requesting hierarchy rollups in query results?

Add explicit hierarchy return controls instead of overloading existing `fields`.

Example:

```json
{
  "table": "files",
  "semantic_search": "termination clause",
  "indexes": ["document_chunks_v1_embedding"],
  "hierarchy": {
    "return_level": "chunk",
    "include": ["unit", "source"],
    "rollup": "source",
    "max_children_per_parent": 5
  }
}
```

Implemented direction:

- `return_level`: accepts `source`, `unit`, `chunk`, and `mention`.
- `include`: accepts `source`, `unit`, and `chunk` ancestor/descendant hydration controls. Direct chunk searches can hydrate DB-backed source and unit ancestors when requested.
- `rollup`: accepts `source` or `none`; source rollups can group matched child chunks under the source result.
- `max_children_per_parent`: limits grouped child hits when rolling up.
- Results expose a stable `hierarchy` envelope with `level`, `parent_doc_key`, optional `parent_unit_id`, artifact identity, and nested child chunks where relevant.
- Results expose an `ancestors` envelope for requested context. Source payloads carry `ancestors.source.document`; unit hits carry `ancestors.unit.document`; chunk hits carry unit ancestry and recovered provenance; mention hits carry a stable evidence envelope for resolver/source artifact references.

This keeps retrieval precise while allowing user-facing search to show file-level results.

### How should artifact manifests be exposed for debugging and reprocessing?

Expose manifests through operational APIs, not normal document query by default.

Artifact manifests are control-plane state. They should be visible for debugging, audits, and manual reprocessing, but users should not accidentally search or mutate them as content.

Recommended direction:

- Use `GET /tables/{table}/documents/{key}/artifacts` to list available artifact manifests for a source document.
- Add artifact detail endpoints for manifest, generations, route decision, fingerprints, child ranges, merge status, and last error.
- Add reprocess controls such as `POST /tables/{table}/documents/{key}/artifacts/{artifact}:reprocess`.
- Add table-level repair/replay commands for an artifact across many source rows.

Implementation note: the DB, bound table-source, local public HTTP, generated OpenAPI/httpx, and hosted/provisioned routing layers now expose per-document manifest listing, per-artifact manifest inspection, forced reprocess for a specific artifact, bounded table-range artifact reprocess, and a durable user-facing repair job envelope for long-running table artifact repair. Per-document public routes enforce source-document row filters before exposing or mutating artifact control-plane state. Manifest inspection now has typed source/fingerprint/range/merge/error summaries, plus explicit summary/raw detail modes; raw manifest/state JSON is an admin/debug detail when public auth is enabled. Failed document extraction writes a failed artifact manifest generation with `route_type: "error"`, `merge_status: "failed"`, typed `last_error_code` / `last_error_message`, and no current child ranges. When a previously successful source is replaced by an unextractable source, stale unit/chunk artifacts and state are deleted; a later successful extraction advances the generation and clears the last error. Table-range repair responses now expose per-shard continuation cursors, completion status, and pending shard count; follow-up requests can pass those cursors back to resume shard-local progress instead of collapsing it into one global key. Reprocess jobs persist those cursors, cumulative counts, phase, completion status, last error, and expiry across bounded advance operations; external hosted schedulers can layer priority and concurrency policy over this stable API.

The manifest should carry enough state to explain why extraction did or did not rerun.

### What is the exact split policy for very large child ranges?

Start with deterministic range splits by artifact child key, then evolve into adaptive splitting.

The first implementation should not need a complex load balancer. Document units already provide natural split boundaries for large files.

Recommended direction:

- Use parent key plus artifact name plus child key as the logical range.
- Split only at document-unit boundaries, not in the middle of a unit.
- Keep chunks under their unit unless a single unit becomes exceptionally large.
- Start with thresholds based on child count and bytes, for example pages/sections and stored artifact bytes.
- Let the parent manifest record child range descriptors and ownership.
- Later, add adaptive split triggers based on write load, query load, and range size.

For very large PDFs, the unit layer is the primary split boundary. For pathological units, such as huge HTML pages or spreadsheet sheets, allow a second-level split under the unit.

### Should canonical entity resolution be separate from mention extraction?

Yes. Mention extraction should be source-owned; canonical entity resolution should be table- or namespace-owned.

Document extraction and entity mention extraction are evidence-producing enrichments. They should not directly decide global identity, because many source documents can mention the same real-world entity concurrently.

Recommended direction:

- Store `entity_mentions_v1` as derived child artifacts under source documents.
- Feed mention artifacts into a resolver process through an explicit `(source_artifact_kind, source_artifact)` subscription for a configured entity namespace.
- Store canonical entities as normal records in an entity table or dedicated graph namespace.
- Store evidence links from canonical entities/edges back to mention artifacts.
- Re-run resolution incrementally when mention artifacts change.
- Scope resolver names to a table by default. A resolver that wants cross-table or cross-team identity must name an explicit shared canonical entity table or graph namespace.
- Treat tenant/project/team isolation as a namespace boundary. No resolver should write canonical entities across that boundary unless the configuration explicitly names a shared namespace and the caller has admin permission on both the source table and target namespace.

This keeps source-document lifecycle separate from global entity identity.

### How much extraction metadata should be standardized versus extractor-specific?

Standardize a small provenance envelope and put format-specific detail under an extractor namespace.

All document units should share enough metadata for search, rollup, highlighting, debugging, and graph evidence. Format-specific detail should remain extensible.

Recommended standard fields:

- `_parent_doc_key`
- `_parent_unit_key`
- `_artifact_name`
- `_generation`
- `unit_id`
- `unit_type`
- `text`
- `content_type`
- `language`
- `provenance.source_url`
- `provenance.filename`
- `provenance.method`
- `provenance.ocr_used`
- `provenance.char_start`
- `provenance.char_end`
- `provenance.page_number`
- `provenance.page_label`
- `provenance.page_bbox`
- `provenance.page_rotation`
- `provenance.confidence`

Extractor-specific fields should live under a namespaced object:

```json
{
  "extractor": {
    "pdf": {
      "text_regions": [
        { "span": [120, 164], "bbox": [72, 144, 240, 160] }
      ],
      "warnings": ["missing ToUnicode map"]
    }
  }
}
```

This gives downstream systems a stable contract without blocking richer extractors.

### Which questions remain open after this direction?

The high-level model is settled enough to start implementation. The pieces that still need concrete product/API decisions are:

- Inspection API shape: collection listing, single-artifact manifest inspection, typed source/fingerprint/range/merge/error summaries, summary/raw admin detail modes, source-row row-filter enforcement for per-document operations, route-level read/admin permission classification, bounded table-range repair, per-shard continuation/resume cursors, hosted/provisioned routing, and durable user-facing repair job status now exist. Future API polish is listing historical jobs and exposing hosted scheduler policy knobs, not the core artifact/status contract.
- Split thresholds: the first unit-level defaults are implemented and manifest-recorded as 256 units or 1 MiB of unit text per range. Oversized single units remain one unit range, while their derived chunks split on chunk boundaries with 256 chunks per range.
- File route config: the first public shape is `route_preset` plus limited ordered overrides, with `mixed_files` as the default built-in route preset and `explicit_only` as the fail-closed mode. Future design work can decide whether to expose named preset variants beyond those two.
- Reprocessing semantics: durable job status and resumable per-shard progress now exist. Remaining scheduler policy decisions are background worker priority, concurrency limits, retry/backoff defaults, and whether hosted deployments should auto-advance queued jobs or require an external controller to call the bounded advance endpoint.
- Entity resolver namespace policy: the durable subscription contract and local config validation now exist. The recommended governance model is table-scoped resolvers with explicit shared canonical namespaces for cross-table identity. Remaining implementation work is validating cross-table/cross-tenant permissions once canonical namespace objects are represented in metadata.

These should be resolved as separate implementation RFCs once Phase 1 proves the artifact layout and extraction lifecycle.

## Design Principle

The product-level abstraction should be:

```text
Extract files into a versioned, splittable derived document hierarchy,
then let normal Antfly enrichments operate on the derived units.
```

This gives Antfly a single story for files, pages, sections, chunks, embeddings, full-text search, OCR, and graph extraction while preserving distributed scale.
