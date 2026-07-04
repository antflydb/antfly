# Sort And Search Design

This document describes Antfly's long-term search and sort design. It is
intended to make the current `order_by` / `search_after` API shape converge on a
native, segment-aware execution model instead of relying on stored-document
materialization and in-memory sorting.

## Goals

- Keep the public query surface Elasticsearch-like where that improves
  operator and client expectations.
- Use Antfly mappings as the source of truth for sortable fields, doc values,
  analyzers, and dynamic field behavior.
- Make `order_by` exact and deterministic without unbounded stored JSON scans.
- Make `search_after` / `search_before` real seek cursors over typed sort
  tuples.
- Preserve vector, text, match-all, filter-only, and distributed query
  correctness while allowing the planner to choose efficient physical paths.
- Fail closed with clear 422 responses when a requested sort cannot be executed
  exactly within production budgets.

## Design Principles

Antfly's long-term search design should keep three things separate:

1. The public query contract.
2. The logical mapped field model.
3. The physical segment/index structures selected by the planner.

The public contract can stay familiar to Elasticsearch users without copying
Elasticsearch internals one-for-one. The important alignment points are:

- mappings describe field type, analyzer, doc values, and sortability
- analyzed text fields are searched, not sorted
- keyword, numeric, date, boolean, and reserved id fields are sortable
- `_sort` is the cursor tuple returned with sorted hits
- `search_after` is a typed tuple cursor, not an offset substitute
- physical index sorting is an optional acceleration path, not the only way to
  support sorted queries

The runtime engine should then choose the most efficient exact plan available:

- inverted postings for full-text terms and phrase constraints
- typed doc values for scalar filters, aggregations, and field sort
- sorted segment order for the dominant configured sort
- vector-native indexes for ANN score order
- bitmap/doc-set intersections for structured filters
- coordinator-side k-way merge for distributed sorted pages

Stored JSON is the source payload. It is not the production search index and
should not be on the hot path for exact filter or sort execution.

## Current State

The public API already has the right high-level shape:

- `order_by` is an array of sort fields.
- `_id` is appended as an implicit ascending stable tie-breaker when omitted.
- Hits can include `_sort` / `sort_values`.
- Clients can pass those values back as `search_after` or `search_before`.
- Count-only requests reject ordered result-page options.
- Semantic vector searches do not currently support stored-field sort because
  ANN order is score-driven and exact ordered filtering requires a different
  native plan.

The implementation is intentionally conservative today:

- Text-backed and match-all stored sort collect a bounded candidate window.
- The engine only returns a sorted page if it can sort the full required
  candidate set exactly.
- If the candidate window exceeds the exact-sort budget, the API returns 422
  instead of returning a partially sorted page.
- The current fallback extracts sort values from stored JSON and sorts in
  memory.

That is correct as a bridge, but it is not the final production design. The
long-term shape should move sort values into native index structures.

## Overall Search Model

Antfly search should be modeled as a planner over exact and approximate sources.

Logical query sources:

- `match_all` / table scan
- structured filters
- full-text search
- dense vector search
- sparse vector search
- graph search
- joins and composed/hybrid queries

Logical outputs:

- matching document identity
- optional document ordinal for native index access
- score, when the source is score-bearing
- typed sort tuple, when `order_by` is requested
- stored source, only when requested by the response shape

Physical index structures:

- stored field blocks for `_source`
- identity/live-doc metadata for visibility, TTL, deletes, and upserts
- inverted-text sections for term/phrase/prefix matching
- typed doc-value sections for scalar lookup, filtering, aggregation, and sort
- vector indexes for approximate nearest-neighbor search
- optional sorted-segment metadata for one configured dominant sort order

The planner should not treat every source as interchangeable. A vector ANN
top-k is an approximate score source, not an exact field-sorted candidate set.
A broad full-text query can produce exact matches, but may still require a
native doc-values collector or sorted-segment scan to avoid materializing and
sorting all stored JSON. A filter-only query should usually be a native doc-set
operation, not a stored-document predicate loop.

### Exactness Classes

Every physical plan should advertise its result exactness:

- `exact`: the plan can prove that all matching documents relevant to the page
  were considered under the requested order.
- `bounded_exact`: the plan is exact because a configured bound was not
  exceeded. If the bound is exceeded, the request fails.
- `approximate`: the plan uses ANN, sampling, heuristic overfetch, or another
  source that cannot prove global exactness.

The public `order_by` API is exact. It may use `exact` or `bounded_exact`
physical plans. It must not silently fall back to an `approximate` plan.

Approximate behavior can exist as a separate explicitly named feature in the
future, but it should not hide behind `order_by`.

### Score Order Versus Field Order

Antfly should be explicit about the requested ordering:

- no `order_by` on full-text or vector search means relevance/score order
- `order_by` on scalar fields means field order with `_id` tie-break
- `_score` in `order_by` means score participates in the tuple
- field order after approximate vector top-k is approximate unless the eligible
  candidate set is exact

This matters because a query like "nearest vectors ordered by `created_at`" is
ambiguous unless the engine can define the eligible set exactly. Sorting only
the first ANN page by `created_at` does not produce the globally newest matching
vectors.

### Native Filters

Structured filters should be native by default:

- term/terms on exact string fields use `.keyword` postings or doc values
- range filters on numeric/date fields use typed doc values or range metadata
- boolean filters use typed doc values or bitsets
- geo filters use geo typed doc values and geo-specific acceleration structures
- text filters use inverted postings with analyzer-aware query lowering

Stored JSON predicate evaluation should remain a compatibility and debug
fallback. It should not be required for production filter correctness.

For search performance, filters and sort cannot be designed independently. A
good field-sort plan often needs a filter doc set, and a good filter plan often
needs sort selectivity information. The planner should consider both.

## Mapping Model

Antfly should not add a separate top-level `sortable_fields` DSL. Sortability
belongs in the existing schema and mapping system described in `SCHEMA.md` and
`FULL_TEXT.md`.

Public schema examples should desugar into runtime mappings with typed field
capabilities:

```json
{
  "properties": {
    "title": {
      "type": "string",
      "x-antfly-field": {
        "type": "text",
        "fields": {
          "keyword": {
            "type": "keyword",
            "doc_values": true,
            "sortable": true
          }
        }
      }
    },
    "created_at": {
      "type": "string",
      "format": "date-time",
      "x-antfly-field": {
        "type": "date",
        "doc_values": true,
        "sortable": true
      }
    },
    "rank": {
      "type": "integer",
      "x-antfly-field": {
        "type": "integer",
        "doc_values": true,
        "sortable": true
      }
    },
    "body": {
      "type": "string",
      "x-antfly-field": {
        "type": "text"
      }
    }
  }
}
```

The runtime schema should compile those declarations into field descriptors:

- logical field path
- physical index field path, including multi-field paths such as
  `title.keyword`
- scalar type
- analyzer, when text-indexed
- `doc_values` capability
- `sortable` capability
- missing/null ordering policy
- multi-value sort mode, if arrays become sortable
- dynamic-template provenance, when a dynamic mapping produced the field

Sortable fields should be limited to scalar, non-analyzed values:

- `keyword`
- `integer`
- floating-point `number`
- `date`
- `boolean`
- `_id`

Analyzed `text` fields are not directly sortable. Users should sort on a
keyword multi-field such as `title.keyword`.

### Mapping Compatibility With Elasticsearch

Elasticsearch's practical model is the right public mental model:

- `text` fields are analyzed and are not sortable by default
- `keyword` fields are exact-match fields with doc values and are sortable
- numeric/date/boolean fields use doc values for sort and aggregations
- `search_after` consumes the returned sort tuple
- `index.sort.*` physically orders segments for one configured sort

Antfly should use Antfly mappings as the source of truth, not a separate
Elasticsearch compatibility layer. The schema compiler should lower public
JSON Schema plus `x-antfly-field` / dynamic-template declarations into the
runtime mapping. That runtime mapping is what query validation and physical
planning consult.

The field name convention should be Elasticsearch-like:

- analyzed field: `title`
- exact multi-field: `title.keyword`
- search-as-you-type fields: `title._2gram`, `title._3gram`,
  `title._index_prefix`

This keeps sort, filters, dynamic templates, generated SDKs, and public docs
aligned around one field namespace.

### Reserved Fields

`_id` is a reserved Antfly document id field and the default final tie-breaker.
It should be sortable without a user mapping. It is not the same thing as a
user field named `id`, and users should not be able to override `_id` mapping
semantics from document schema.

If Antfly later supports multiple logical documents with the same `_id` in a
distributed table namespace, the internal final tie-breaker must extend beyond
`_id` with stable shard/table identity. The public API can still expose `_id`
as the visible tie-breaker only when it is globally unique for that index.

### Dynamic Mappings

Dynamic templates should be able to mark scalar fields as doc-valued and
sortable:

```json
{
  "match_mapping_type": "date",
  "mapping": {
    "type": "date",
    "doc_values": true,
    "sortable": true
  }
}
```

The runtime schema must persist the observed dynamic mapping decision for a
field path. Query-time validation should not guess sortability from the latest
document payload. It should validate against compiled explicit mappings,
compiled dynamic rules, or persisted observed dynamic field metadata.

## Doc Values

Doc values are the first native primitive Antfly needs for production sorting.

For every mapped field with `doc_values: true`, segment build and replay should
write a typed column keyed by document ordinal:

```text
doc_values/<index>/<field>/<segment_id>/<doc_ordinal> -> encoded_value
```

The exact key layout can differ, but the semantics should be:

- one typed value per doc ordinal per field, or a deterministic missing marker
- byte encoding preserves the field's comparison order
- values can be loaded without parsing stored JSON
- values survive segment reopen and compaction
- deletes and identity generation changes are handled through the same live-doc
  visibility model as text and vector indexes

Doc-value encodings must be stable and type-aware:

- integers use sortable signed integer encoding
- floats use sortable IEEE encoding with a defined NaN policy
- dates normalize to epoch nanoseconds or milliseconds before encoding
- keywords use normalized UTF-8 bytes plus length delimiters
- booleans use false < true
- missing/null values use explicit sentinels

The result hit should expose the original JSON-compatible sort value, not the
internal byte encoding.

### Native Sortable Field Path

A native sortable field path consists of all of the following:

- a runtime mapping entry for the field path
- a scalar sort type: keyword, integer, floating number, date, boolean, or `_id`
- `doc_values: true`, except for `_id` which is backed by identity metadata
- a deterministic missing/null policy
- a typed encoder whose byte/comparator order matches query semantics
- segment-level doc-value sections written at index time
- merge/reopen support for those sections
- planner capability metadata that says field sort can be executed exactly
- API serialization that returns JSON-compatible `_sort` values

This is the concrete replacement for stored JSON sort extraction. If any of
those pieces are missing, the planner should either pick a different exact plan
or reject the request with a clear 422.

The first production milestone is not "segments are physically sorted." It is
"mapped scalar sort reads values from typed doc values and never parses stored
JSON on the sort hot path."

### Doc-Values Collector

For arbitrary `order_by`, the general exact plan is a top-N collector over
typed doc values:

1. The query source produces matching doc ordinals.
2. The collector loads the requested typed doc values for each ordinal.
3. The collector maintains the top page window using the sort comparator.
4. The comparator appends `_id` as the final stable tie-breaker.
5. The response returns hits with `_sort` values suitable for `search_after`.

The collector should support forward and reverse paging by comparing against
the cursor tuple before admission. It should not collect all matches just to
drop rows before the cursor.

Collector implementation requirements:

- one comparator implementation for in-memory, segment, and distributed merge
- per-type missing/null handling
- no stored JSON loads unless `_source` is requested
- bounded memory proportional to `limit + offset + shard_window`, not total
  hits
- observability for candidate count, rejected-by-cursor count, doc-value load
  time, and collector heap size

### Sort Encoding

Sort encoding must be canonical. Recommended order-preserving encodings:

- signed integers: flip sign bit before unsigned byte comparison
- unsigned integers/dates: big-endian sortable integer bytes
- floats: IEEE sortable transform with explicit NaN policy
- booleans: `false = 0`, `true = 1`
- keywords: normalized UTF-8 bytes with length-safe delimiters
- missing/null: explicit sentinel outside the value domain according to policy

Doc values may store a compact native layout rather than exactly these bytes,
but all physical comparators must behave as if this canonical order was used.

## Segment-Level Sorting

Doc values make arbitrary field sorting exact. Segment sorting is the next
optimization for common orders.

Antfly should support one physical sort order per index generation or segment
family, similar to Lucene index sorting:

```json
{
  "index_sort": [
    { "field": "created_at", "order": "desc" },
    { "field": "_id", "order": "asc" }
  ]
}
```

This is not a replacement for doc values. It is an acceleration path for the
dominant sort order.

Properties:

- `index_sort` is validated against mapped sortable/doc-value fields.
- The physical order is fixed for an index generation. Changing it requires a
  rebuild or new generation.
- Segment builders order documents by the index-sort tuple.
- Segment merges preserve the configured order.
- Queries whose `order_by` exactly matches the prefix/full index sort can use
  early termination.
- Writes cost more because flush/merge must maintain physical order.

Because a segment can have only one physical order, arbitrary `order_by` still
uses doc values and collectors.

### Lucene-Style Segment Sorting

Lucene's useful lesson is not only that segments can be sorted. It is that
sorted segments enable early termination when the query's requested sort matches
the segment's physical sort and the query can test eligibility while scanning in
that order.

Antfly should follow the same high-level shape:

- new segments are flushed in `index_sort` order
- merges preserve `index_sort` order
- deleted documents remain tombstoned until merge
- sorted scans check live-doc, TTL, and filter membership
- matching sorted scans can terminate after enough hits for the page/shard
  window
- non-matching sorts still use doc-values collectors

The physical sort order should be configured per index generation, not changed
in place. Changing `index_sort` requires building a new generation or
reindexing because existing segments have durable physical order.

### Index Sort Planning

An index-sort path is available when:

- every requested sort field is mapped and sortable
- requested sort is a prefix of the configured `index_sort`, or exactly matches
  it after implicit `_id` normalization
- cursor tuple can be encoded into the same comparator domain
- query source can test document eligibility in sorted order
- the result does not require a conflicting primary order such as ANN score

When those conditions hold, sorted segment seek should be the preferred plan for
broad match-all/filter queries with small pages.

## Sort Tuple And Cursor Semantics

The public cursor value is the typed sort tuple:

```json
{
  "_id": "doc:123",
  "_score": 1.0,
  "_sort": ["2026-01-01T00:00:00Z", "doc:123"]
}
```

The tuple must include the implicit `_id` tie-breaker. Cursor comparison uses
the same typed comparator as sorting.

Rules:

- `search_after` returns rows strictly greater than the cursor in the requested
  sort order.
- `search_before` returns the previous page before the cursor in the requested
  sort order.
- Cursor arity must match the effective `order_by`, including the appended
  `_id`.
- Cursor value types must match mapped field types after coercion.
- `_id` must always be present as the final deterministic tie-breaker unless
  the user already supplied it.

Native execution should lower a cursor into a segment seek key whenever the
requested order has an index-sort path. For doc-values-only sorting, the cursor
is applied by the collector comparator.

### `search_after` And `search_before`

Antfly already exposes `search_after` and `search_before`; the long-term rule is
that both are cursor operations over the effective sort tuple.

For `search_after`:

- compare each candidate tuple to the cursor using the requested sort order
- admit only tuples strictly after the cursor
- return the next page in normal requested order

For `search_before`:

- compare each candidate tuple to the cursor using the requested sort order
- admit only tuples strictly before the cursor
- collect the nearest previous page
- return results in the same user-visible order as the original query unless
  the API explicitly documents reverse order

Offset pagination may remain for small/debug use, but production deep paging
should use cursor pagination. Offset forces the engine to walk and discard
earlier rows, while cursor seek can be lowered into sorted-segment or collector
admission logic.

Cursor validation must be strict. A cursor generated for one `order_by` cannot
be reused with a different `order_by`.

## Query Execution Model

Antfly search has several logical sources:

- match-all / filter-only scans
- full-text search
- dense vector search
- sparse vector search
- graph search
- joins and composed/hybrid searches

The planner should choose a physical plan based on the query, filters, requested
sort, limit, and available mapped structures.

### Match-All

Best path:

1. If `order_by` matches `index_sort`, seek directly into sorted segments.
2. Apply live-doc, TTL, and primary-key visibility filters.
3. Return `limit` hits plus sort values.

This is `O(log segment + limit)` per relevant segment before merge, not
`O(N log N)`.

Fallback path:

1. Use doc values and a top-N collector.
2. Reject if exact execution would exceed configured production budgets.

Stored JSON sorting should remain a compatibility/debug fallback only.

### Structured Filters

Structured filters should prefer native field indexes and doc sets:

1. Compile filters into doc ordinal constraints when possible.
2. If the requested sort matches `index_sort`, scan sorted order and test the
   doc set until enough visible hits are found.
3. Otherwise, iterate matching doc ordinals and use doc values in a top-N
   collector.

The planner should compare:

- filter selectivity
- sort selectivity
- limit
- available doc sets
- segment sort compatibility

For highly selective filters, candidate-first plus doc-values sorting is often
best. For broad filters and small limits, sorted-order scan with filter testing
is often best.

### Full-Text

Full-text relevance remains score-first unless the user explicitly requests
field sort.

When `order_by` is present:

- The text engine produces matching doc ordinals or an iterator over matches.
- Field sort uses doc values, not stored JSON.
- If `order_by` matches `index_sort`, the planner may scan sorted segments and
  test the text match set.
- Otherwise, the planner collects text matches into a top-N field-sort
  collector.

Text queries must not return a partial field sort. If exact sort would exceed
budget and no native plan can prove exactness, return 422.

Score sorting should remain separate:

- `_score` is produced by the text/vector engine.
- `_score, _id` sort is a scorer/top-k problem.
- field sort is a doc-values/sorted-segment problem.

### Vector Search

ANN vector search has a different correctness boundary.

If the query is semantic-only, results are naturally ordered by vector score.
Applying `order_by` to ANN hits after the fact is not equivalent to asking for
the globally top documents by a field under a vector predicate.

Production rules:

- Semantic-only `order_by` should remain unsupported unless the planner has an
  exact bounded candidate set.
- Native filters must be applied inside vector search where supported.
- Overfetch-and-rerank is only valid when documented as approximate. It should
  not back the exact `order_by` API.
- If a vector query has an exact filter that produces a small doc set, the
  planner may execute exact vector scoring over that set and then sort/page
  according to the requested semantics.

### Hybrid And Composed Queries

Hybrid queries combine score-bearing and field-ordering semantics. The planner
must make the ordering explicit:

- relevance merge order, such as RRF or weighted score
- field sort order over the merged eligible set
- reranker order

Field sort after hybrid merge is only exact if the eligible set is exact. If the
eligible set came from approximate vector top-k, field sort is approximate and
should not use the exact `order_by` contract.

## Distributed Search

Distributed sorting should use shard-local sorted execution plus coordinator
merge.

For exact field sort:

1. Each shard validates the mapping and sort tuple.
2. Each shard returns its sorted top window with `_sort` values.
3. The coordinator performs a k-way merge using the same typed comparator.
4. The coordinator returns the global page and cursor tuple.

For `search_after`, the coordinator forwards the cursor to every shard. Each
shard seeks past that tuple in its local ordering and returns its next window.

Total hit relation rules:

- `exact` only when every shard can prove exact matching count under the query.
- `gte` when a shard used a lower-bound path or budgeted early termination.
- Candidate-budget failures should remain 422 rather than silently degrading an
  exact field-sort request.

The `_id` tie-breaker must be globally unique. If future table layouts allow
non-unique `_id` across shards, the final tie-breaker must include a stable
shard/table identity as well.

## Storage Layout Sketch

The exact key layout should be chosen with the LSM and segment formats, but the
logical pieces are:

```text
segment/<index>/<segment_id>/meta
segment/<index>/<segment_id>/live_docs
segment/<index>/<segment_id>/text/postings/...
segment/<index>/<segment_id>/doc_values/<field>/<doc_ordinal>
segment/<index>/<segment_id>/sort/<index_sort_tuple>/<doc_ordinal>
```

For index-sorted segments, physical document order can itself be the sort order,
so a separate sort key may only be needed for seek metadata and merge cursors.

For doc-values-only sorting, collectors need efficient per-doc ordinal access to
sort values.

Deletes should not rewrite the sorted segment immediately. They should use the
existing live-doc/tombstone model and be reclaimed by merge.

## Planner Capabilities

The planner should expose sort capabilities in the same way query planning
already reasons about native filters and index coverage.

Capability fields:

- field is mapped
- field is scalar
- field has doc values
- field is sortable
- requested order matches index sort
- requested order is a prefix of index sort
- cursor can be converted to native seek key
- query source can produce an exact candidate set
- query source is approximate
- distributed shard merge is exact

The planner should select among:

- `sorted_segment_seek`
- `doc_values_top_n`
- `candidate_then_doc_values_sort`
- `score_top_k`
- `distributed_k_way_merge`
- `unsupported_exact_sort`

## API Contract

The API should keep the current user-facing shape:

```json
{
  "full_text_search": { "match": { "title": "antfly" } },
  "order_by": [
    { "field": "created_at", "desc": true }
  ],
  "limit": 20,
  "search_after": ["2026-01-01T00:00:00Z", "doc:123"]
}
```

Validation:

- unknown sort field: 422
- analyzed text field without sortable keyword/doc-value field: 422
- non-sortable field: 422
- cursor arity/type mismatch: 400 or 422, consistently with query validation
- semantic approximate exact sort: 422
- count-only plus ordered page options: 422

Response:

```json
{
  "hits": {
    "total": { "value": 1234, "relation": "exact" },
    "hits": [
      {
        "_id": "doc:123",
        "_score": 1.0,
        "_sort": ["2026-01-01T00:00:00Z", "doc:123"],
        "_source": { "title": "Antfly" }
      }
    ]
  }
}
```

The `_sort` array should be present when `order_by` is present.

## Rollout Plan

1. Keep the current exact budgeted fallback as the safety baseline.
2. Compile sortable/doc-value capability from mappings into the runtime schema.
3. Persist doc values for mapped scalar fields during segment build/replay.
4. Use doc values for stored-field sort instead of parsing stored JSON.
5. Reject unmapped/non-sortable fields by default.
6. Add index-sort configuration and segment build support.
7. Teach match-all and filter-only queries to use sorted segment seek.
8. Teach full-text queries to choose between text-candidate collection,
   doc-values top-N, and sorted-order scan with text-match testing.
9. Add distributed k-way merge over typed sort tuples.
10. Remove or hide stored JSON sorting behind a test/debug-only option.

## Testing Requirements

Unit coverage:

- sort encoding order for every supported scalar type
- missing/null ordering
- `_id` tie-breaker stability
- cursor arity and type validation
- doc-values reopen and compaction
- index-sorted segment merge preserving order
- live-doc and TTL filtering under sorted seek

Integration coverage:

- match-all `order_by` with `search_after`
- match-all `search_before`
- full-text field sort with exact total
- broad full-text field sort budget rejection
- structured filter plus field sort planner choice
- distributed sorted merge across shards
- deletes and upserts changing sort values
- schema change or index generation rebuild for changed sort mappings

Performance coverage:

- large match-all sorted first page should scale with `limit`, not corpus size,
  when `order_by` matches `index_sort`
- doc-values top-N should avoid stored JSON loads
- distributed merge should scale with shard count and page size, not global hit
  count

## Non-Goals

- Do not make analyzed `text` fields directly sortable.
- Do not silently use approximate vector overfetch for exact field sort.
- Do not add a second mapping DSL for sort fields.
- Do not physically sort segments by every sortable field. A segment has one
  physical order; arbitrary sort orders use doc values.

## Related Docs

- `SCHEMA.md`
- `FULL_TEXT.md`
- `DOCID.md`
- `DB.md`
