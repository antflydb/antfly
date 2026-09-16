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

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

Document extraction (`asset` producer type `document_extraction`, handled internally) resolves sources to a URL (including `data:` URLs) and routes PDF, text, HTML, email, OOXML, ZIP, image, and audio content into canonical units via a public `route_preset` contract: `mixed_files` (default) runs built-in extractors after caller-provided ordered overrides matched by content type, prefix, extension, or magic bytes; `explicit_only` disables fallback and fails closed with a structured `route_type: "unsupported"` manifest when nothing matches. `GET /db/v1/tables/{table}/documents/{key}/artifacts` lists a document's artifact manifests, `GET .../artifacts/{artifact}` inspects one with `detail=summary|raw` (raw requires table-admin under public auth; summary is the auth-enabled default), and `POST .../artifacts/{artifact}:reprocess` forces a synchronous reprocess; all three apply the caller's row filter first and return `404` for hidden documents. A bounded table-range repair endpoint (`POST /db/v1/tables/{table}/artifacts/{artifact}:reprocess`, with `from_key`/`to_key`/`limit`/`shard_cursors`) and a durable reprocess-job resource (`POST .../reprocess-jobs`, `GET .../reprocess-jobs/{job}`, `:advance`, `:cancel`) support resumable per-shard repair at table scale.

### Phase 2: Unit-aware chunking and indexing

- Allow chunk enrichments to read from a source artifact instead of only a source field/template.
- Preserve parent document and parent unit keys on chunks.
- Add full-text and embedding indexing over units/chunks.
- Keep unchanged units/chunks stable across parent updates.

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

Chunk enrichments can set `source_artifact_name` to a document-unit asset, fanning out per unit with unit-scoped chunk keys and chunk payload fields `_parent_doc_key`, `_parent_unit_key`, `_parent_unit_id`, `_source_artifact_name`, `_artifact_name`, `_source_field`. Query responses carry a stable `hierarchy` envelope (`level`, `parent_doc_key`, `parent_unit_id`, artifact identity, nested children) and an `ancestors` envelope for source/unit/chunk hydration, plus first-class `mention` return levels backed by `antfly.resolution_mention.v1` evidence artifacts. `hierarchy.group_by.level: "unit"` returns relevance-ranked units with bounded matching chunks and rejects `order_by`/cursor controls; sequential unit navigation instead returns an opaque `_hierarchy.position` bound to a composite revision over every participating unit artifact/generation/key/fingerprint, returning `409 hierarchy_cursor_stale` with `restart_hierarchy_traversal` guidance on invalidation. Unit fingerprints use the versioned `duf2:` encoding, and the public cursor exposes only a domain-separated commitment, never the reversible storage fingerprint.

### Phase 3: Distributed child ranges

- Introduce interleaved artifact range descriptors.
- Route child records by parent plus artifact range.
- Allow large child ranges to split independently from the parent shard.
- Make merge plans idempotent and resumable across child shards.

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

Document extraction manifests are versioned (`manifest_version: 2`) with a monotonic `generation` and deterministic `child_ranges` (range IDs, key bounds, counts, placement, split-boundary metadata), plus a `merge_plan` (`from_generation`, `to_generation`, `operation_granularity: "unit_fingerprint"`) recording idempotent keep/upsert/delete decisions; an `in_progress` merge plan is durably written before child writes and replaced by the converged plan after commit, so crash replay never skips a generation. Child ranges carry route/ownership metadata (`owner_group_id`, `placement_generation`, `route_status`, `split_eligible`); ranges move from `local_committed`/parent-owned to `remote_committed` on split, dispatched to the new owner group through a durable source-shard outbox. Unit ranges split at 256 units or 1 MiB of unit text (an oversized unit stays intact); chunk ranges split at 256 children with `split_boundary: "chunk"` under large units, per a `range_policy` envelope recorded in the manifest. Manifests also carry a `coverage_plan` stating full-text replay remains `stored_artifact_required` with no suppression until coverage watermarks exist.

### Phase 4: Graph extraction over units

- Let graph/entity extractors target `document_units_v1` or `document_chunks_v1`.
- Store mentions and relation evidence as child artifacts.
- Link evidence to canonical graph nodes and edges.

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

Graph/entity extractors can target asset-backed document units and chunk-backed document chunks, not only root artifacts, with per-unit/per-chunk graph materialization state so children don't clobber each other's replay state. Resolver configs declare a `(source_artifact_kind, source_artifact)` subscription (kind `asset` (default), `chunk`, or `any`); resolution replay materializes `antfly.resolution_mention.v1` evidence artifacts keyed by source artifact, resolution artifact, and local mention ID. Canonical mention provenance edges roll up evidence as `target_table`, `mention_count`, and `mention_artifact_keys` metadata — one graph edge per canonical entity regardless of mention count — surfaced through graph path/pattern responses via a public `evidence` envelope.

### Phase 5: More file types and OCR fallback

- Add DOCX, PPTX, XLSX, image OCR, scanned PDF fallback, archives, and audio transcripts.
- Track extraction method, OCR use, page/section coordinates, and confidence in provenance metadata.

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

Email (`message/rfc822`/`.eml`), OOXML (DOCX/PPTX/XLSX, via an in-memory ZIP reader), ZIP archives, images, and audio all route into `document_units_v1` with deterministic unit types (`email_headers`/`email_body`/`email_part`, `section`/`slide`/`sheet`, `archive_entry`, `image`, audio/transcript) and per-format provenance. Image and audio units without a configured producer stay pending (`method: "ocr_pending"`/`"transcript_pending"`, `extraction_status: "pending_ocr"`/`"pending_transcription"`) until an asset `reader`/`transcriber` producer completes them (`extraction_status: "completed"`, `ocr_used`/`transcript_used: true`); scanned PDF pages without mechanical text use the same pending-OCR contract at page granularity. OCR/transcription producers may return structured JSON (`text`, `confidence`, `bbox`/`ocr_bbox`/`coordinates`, `warning`) normalized into unit/chunk provenance and fingerprints.

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

> **Relocated:** The implementation-status detail that previously lived here is preserved verbatim in [work-log/completed/derived-documents/implementation-status-history.md](../work-log/completed/derived-documents/implementation-status-history.md).

The DB, bound table-source, local public HTTP, generated OpenAPI/httpx, and hosted/provisioned routing layers all expose per-document manifest listing, per-artifact manifest inspection (typed source/fingerprint/range/merge/error summaries, with `summary`/`raw` detail modes — raw is admin/debug-gated under public auth), forced per-artifact reprocess, bounded table-range artifact reprocess, and a durable reprocess-job envelope for long-running repair; per-document routes enforce source-document row filters first. Failed extraction writes a manifest generation with `route_type: "error"`, `merge_status: "failed"`, and typed `last_error_code`/`last_error_message`; a later successful extraction advances the generation and clears the error. Table-range repair and reprocess-job responses carry per-shard continuation cursors, cumulative counts, phase, and completion status so callers resume shard-local progress instead of collapsing it into one global key.

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
