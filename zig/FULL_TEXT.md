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
- `._index_prefix` is phrase-prefix oriented: it indexes edge n-grams over
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
