# Full-Text Plan

## Goal

Keep full-text visibility semantics aligned with the LSM path:

- write/sync waits for search visibility, not full compaction
- segment merges run as background maintenance
- explicit force-compaction stays available as an admin/test hammer

## Current Policy

1. `SyncLevel.full_text` means the affected documents are searchable in the
   relevant full-text indexes.
2. Scheduled text merges are background work owned by the text-merge runtime and
   its resource budgets.
3. `runUntilIdle()` drains scheduled text merges as part of idle maintenance.
4. `drainScheduledTextMerges()` is the explicit blocking API for "finish the
   merges that are already scheduled".
5. `forceCompactTextIndexes()` remains the heavy hammer that aggressively
   rewrites text indexes beyond the normal merge scheduler.

## Why

The old shape mixed two different concerns:

- search visibility / replay catch-up
- segment merge compaction

That made `full_text` sync heavier than it needed to be and hid the real cost of
forced compaction in normal maintenance timings.

## Field Layout

Full-text field names should follow Elasticsearch-style multi-field naming now
that the Zig implementation is still prerelease.

- Exact string companions use `.keyword`.
- `search_as_you_type` emits `._2gram`, `._3gram`, and `._index_prefix`.
- `._2gram` and `._3gram` are shingle fields for multi-token
  search-as-you-type matching.
- Prefix/autocomplete matching should target `._index_prefix`; it is
  phrase-prefix oriented and indexes edge n-grams over
  one-, two-, and three-token shingles so prefixes such as `brown f` and
  `quick brown f` can match without scanning stored documents. This is closer
  to Elasticsearch's `search_as_you_type` prefix behavior than plain per-token
  edge n-grams.
- Schema-less string indexing emits both the analyzed field and a bounded
  `.keyword` companion so term/terms filters can use postings without widening
  vector queries through stored-document scans.
- Public filter rewriting may map term/terms filters to `.keyword` only when
  the target text snapshot contains that field. If the postings are absent, the
  query must fall back or fail closed rather than treating the missing field as
  a valid empty result.

This intentionally breaks from the old Zig-only `__keyword` / `__2gram` suffixes
and from the Go/Bleve naming. The benefit is that the public query surface,
schema-derived fields, dynamic-template variants, and schema-less exact fields
all use one familiar subfield convention before compatibility constraints make
the layout expensive to change.

## Search-As-You-Type Design

The JSON Schema surface should remain valid JSON Schema. Do not overload the
standard `type` field with Elasticsearch field types inside this schema format.
The current shorthand remains:

```json
{
  "type": "string",
  "x-antfly-types": ["search_as_you_type"]
}
```

When configuration is needed, prefer a separate extension object instead of a
list of typed objects inside `x-antfly-types`:

```json
{
  "type": "string",
  "x-antfly-field": {
    "type": "search_as_you_type",
    "analyzer": "standard",
    "max_shingle_size": 3,
    "fields": {
      "keyword": { "type": "keyword" }
    }
  }
}
```

`x-antfly-types` should stay a compact compatibility shorthand that can desugar
to `x-antfly-field` defaults. A list of objects inside `x-antfly-types` would
turn the extension into a second mapping DSL and make validation, schema diffing,
dynamic templates, and compatibility harder.

For now, `search_as_you_type` uses the Elasticsearch default
`max_shingle_size = 3` internally and does not expose a public knob. When exposed,
valid values should be `2..4`, matching Elasticsearch, and the generated fields
should be `._2gram` through `._{max_shingle_size}gram` plus `._index_prefix`.

The query surface should use an Elasticsearch-style `multi_match` query with
`type: "bool_prefix"` rather than an Antfly-only standalone bool-prefix
operator:

```json
{
  "full_text_search": {
    "multi_match": {
      "query": "quick brown f",
      "type": "bool_prefix",
      "fields": ["name"]
    }
  }
}
```

For `search_as_you_type` root fields, Antfly should expand the root field to the
generated autocomplete fields:

```json
{
  "full_text_search": {
    "multi_match": {
      "query": "quick brown f",
      "type": "bool_prefix",
      "fields": ["name", "name._2gram", "name._3gram"]
    }
  }
}
```

The explicit generated-field form should also be accepted for Elasticsearch
familiarity and advanced scoring control. The shorthand `fields: ["name"]`
should remain the normal Antfly path so users do not have to manually list
generated subfields for autocomplete.

Internally this should lower to the existing boolean, term, and prefix query
machinery. Completed terms match the root and shingle fields, and the final
partial phrase is satisfied through `._index_prefix`.

## Task List

- [x] Stop draining scheduled text merges inside normal sync-level waits.
- [x] Add an explicit `drainScheduledTextMerges()` API.
- [x] Route `runUntilIdle()` through the scheduled-merge drain path.
- [x] Keep `forceCompactTextIndexes()` as a separate explicit admin/test path.
- [x] Move broad callers off forced compaction where the goal is just stable
      maintenance rather than minimal segments.

## Follow-Through

- Add per-index segment-count reporting to `replay_bench.zig`.
- Make forced compaction report or reserve text-merge resources so its memory
  behavior is observable instead of bypassing the normal scheduler path.
- Add a `best_effort` forced-compaction path that stops under resource pressure
  and leaves merge debt scheduled instead of always acting like a blocking
  hammer.
- Consider a first-class chunked full-text generator path instead of piggybacking
  on dense-generator config for `generated_chunked_full_text`.

## Segment Build Profiling

Deferred full-text catch-up now reports segment-builder internals through the
existing `antfly_bench_text_index` benchmark log when `ANTFLY_BENCH_METRICS=1`
is enabled. The log includes analyzer time, term accumulation, term-hit
materialization, typed-field collection/build, segment encoding, token counts,
term-hit counts, and emitted segment bytes.

The segment builder writes stored docs, ordinal sidecars, inverted sections, and
typed doc values directly. It does not retain a full intermediate `Batch`.
Temporary per-document term maps borrow analyzer token slices until the document
has been added to each field's inverted builder; the inverted builder then
re-keys terms into its own arena for segment lifetime ownership.

When the mapper has already parsed and sanitized a document for full-text
projection, it passes that parsed typed source into the segment builder instead
of materializing an intermediate typed-field array. Raw schemaless fast-path
documents explicitly skip typed collection.

Built per-field inverted sections are now transferred into the segment writer
without a second section-buffer copy. Stored JSON is borrowed by the segment
writer until `build()` finishes.

Stored fields use a v3 offset-table layout that keeps O(1) doc lookup but writes
raw stored JSON instead of compressing each tiny document independently. Older
v2 per-doc Snappy segments still read and merge correctly. A future Lucene-style
stored-field format should compress blocks of docs, not individual small docs,
so the write path does not spend most of segment assembly in per-document
compression.

Text analysis and per-document term accumulation now use a reusable
document-local arena. Analyzer tokens, temporary per-field term maps, position
lists, and materialized `TermHit` slices are released by resetting the arena
between documents after the persistent inverted builders have copied the needed
terms and positions. This keeps allocator churn out of the large schemaless
replay path without changing term ownership in the durable segment.

Documents whose text projection has no duplicate field names use a direct field
path: analyze one field, aggregate that field's terms, and add it to the
persistent inverted builder immediately. The older per-document field map path
is still used when repeated field names must be concatenated into one logical
field with shared positions.

For short fields whose analyzed token list has no repeated terms, the direct
field path now skips the document-local term hash map entirely and emits one
`TermHit` per token. This follows the Lucene/Tantivy shape of avoiding generic
maps for the common "few unique tokens" document path, while keeping the
hash-map path for repeated terms and unusual analyzers.

Segment assembly stores each section's offset/length directly on the writer's
section records while appending section bytes. The final section index no longer
builds a separate location list or scans that list for every field section.

Field analyzer resolution is cached for the duration of one segment build, so
repeated dynamic field names do not rescan the text-analysis config for every
document.

Analyzer token-list builders now return owned slices directly instead of
duplicating the temporary token array. The lowercase filter also skips allocation
for tokens that contain no ASCII uppercase bytes, and the stop-word filter
passes through the original token slice when it removes nothing.

`IndexWriter.addSegmentWithIdData` updates append-only BM25 field-length stats
incrementally by cloning the previous snapshot stats and reading only the new
segment's inverted-section headers. Segment replacement and merge paths still
rebuild stats from the replacement segment list because those operations remove
or reorder existing segment entries.

The same benchmark log now separates section attachment, stored-doc attachment,
stored-doc compression, and final segment assembly from the broader
`segment_encode_ms` bucket.

Use this breakdown before changing analyzer or segment layout code. The intended
optimization order is:

1. remove duplicated typed-field work and stored-JSON reparsing
2. reduce analyzer token allocation/copying
3. reduce per-document term-map churn
4. improve segment encoding pre-sizing and remaining final-output copy behavior
