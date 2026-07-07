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

## Recommended Long-Term Shape

The long-term design should be:

1. Use Antfly mappings as the only user-facing declaration of field
   capabilities.
2. Lower sortable keyword, numeric, date, boolean, and `_id` fields into native
   doc values and planner capability metadata.
3. Keep analyzed `text` fields search-only; users sort on keyword/scalar
   fields such as `title.keyword`, not `title`.
4. Treat `search_after` and `search_before` as typed cursor tuples over the
   effective `order_by`, including the implicit `_id` tie-breaker.
5. Use doc-values top-N collection as the general exact sort path.
6. Use Lucene-style `index_sort` as an optional acceleration path for the
   dominant sort, with segments flushed and merged in that physical order.
7. Push filters into native postings/doc-set/vector structures before sorting
   whenever that preserves the query's exactness contract.
8. Reject exact `order_by` requests that would require approximate ANN
   overfetch/rerank or unbounded stored JSON scans.

This means Antfly should not add a separate sort-index DSL and should not rely
on coordinator reranking as the normal answer for sorted pagination. The
production API remains Elasticsearch-like: mappings define what can be
searched, filtered, aggregated, and sorted; `_sort` values are returned with
hits; clients pass those values back as `search_after`; and the engine chooses
the best exact physical plan available.

The physical implementation should be Antfly-native. Elasticsearch is the right
public mental model, and Lucene's segment sorting/doc-values design is the
right performance model, but Antfly should compile those ideas into its own
segment metadata, typed doc-value sections, identity/live-doc model, and
distributed merge protocol.

## Long-Term Target Architecture

The long-term search architecture should be a native planner over mapped field
capabilities, not a set of request-specific fallbacks. The same mapping metadata
should drive indexing, filtering, sort validation, cursor encoding, distributed
merge, and generated API documentation.

Core layers:

- API schema: validates the user-facing query shape and returns stable errors.
- Runtime mappings: describe field type, analyzer, doc values, sortability,
  dynamic-template provenance, and physical coverage.
- Segment metadata: describes live docs, deletes, min/max values, doc-value
  sections, postings sections, vector sections, and optional `index_sort`.
- Query planner: lowers a logical query into one exact or approximate physical
  plan with explicit capabilities and budgets.
- Executors: scan postings, doc sets, vector indexes, sorted segments, or
  doc-values collectors without parsing `_source` on the hot path.
- Coordinator: merges shard-local hits using the same typed comparator and
  cursor semantics as a single shard.

The production invariant is that exact APIs use exact physical plans. If a
requested `order_by` cannot be executed through a native exact path and would
require an unbounded stored-document scan, the request should fail with a clear
422. Approximate execution should be a separate opt-in feature, not an
implementation detail behind exact `order_by`.

### Canonical Search Pipeline

The long-term engine should route every search through the same planner
pipeline instead of using separate ad hoc paths for match-all, full-text,
semantic, and filter-only queries:

1. Normalize the request into a logical query, filter set, requested order,
   cursor, limit, and response projection.
2. Resolve field references through runtime mappings, including dynamic
   mappings that have been durably promoted.
3. Normalize `order_by` by appending `_id asc` when needed.
4. Validate cursor arity and typed cursor values against the effective order.
5. Build native candidate sources: postings, doc sets, primary-key scans,
   vector candidate iterators, or graph results.
6. Choose exactly one sort executor: score top-k, `_id` seek, sorted segment
   seek, doc-values top-N, distributed k-way merge, or unsupported.
7. Execute without reading stored `_source` until after the page has been
   selected, unless `_source` is itself the requested payload.
8. Return hits with stable `_sort` tuples and profile metadata describing the
   selected plan.

This makes the public API behavior consistent even when different physical
sources are involved. A full-text query with `order_by: created_at`, a
filter-only query with the same order, and a match-all query with the same
order should all validate against the same mapping and cursor rules. They may
choose different candidate sources, but they should share the same comparator,
doc-value access, `_id` tie-breaker, and coordinator merge logic.

### Elasticsearch And Lucene Alignment

Antfly should follow Elasticsearch's public model and Lucene's physical lessons
where they fit Antfly's storage engine:

- Elasticsearch-like mappings define field semantics. `text` is analyzed and
  searched; `keyword`, numeric, date, boolean, and `_id` fields are exact scalar
  values that can be doc-valued and sorted.
- Elasticsearch-like `search_after` uses the returned sort tuple. The cursor is
  tied to the effective `order_by`, including the implicit `_id` tie-breaker.
- Lucene-like doc values provide the general columnar primitive for sorting,
  filtering, and aggregations.
- Lucene-like index sorting physically orders each segment by one configured
  dominant sort and enables early termination when the requested order matches.
- Lucene-like segment merging preserves the configured physical sort order
  while deletes remain tombstones until compaction.

Antfly should not copy Elasticsearch internals blindly. The important contract
is that users can reason about mappings and sorted pagination the same way:
field sort is exact when the field is mapped as sortable/doc-valued, and
`search_after` is stable because `_sort` contains the full typed tuple.

Elasticsearch does not make users declare a second "sort index" for ordinary
field sorting. It uses mappings and doc values. Antfly should do the same:
`keyword`, numeric, date, boolean, and `_id` mappings define the fields that can
participate in exact filters, aggregations, and sort. A separate Antfly-only
sort registry would create drift between schema, OpenAPI, SDKs, docs, and
runtime behavior.

Lucene's sorted segment model should be treated as an optimization layered on
top of those mapped doc values. Index sorting is useful because it changes the
physical document order of a segment, which allows early termination and cursor
seek for the one configured dominant order. It does not replace doc values and
it does not solve arbitrary user sort orders.

### Native Sortable Index Path

A native sortable field/index path is the set of durable structures and planner
metadata required to execute a sort without reading stored JSON:

1. Mapping declares the field as a scalar sortable type.
2. Segment build writes a typed doc-value column for the field.
3. Segment metadata records that the doc-value section is present and complete
   for the live-doc generation.
4. The field has a canonical comparator and cursor encoder.
5. The planner verifies all live segments have the required coverage.
6. The executor loads typed values by document ordinal and applies missing/null
   policy explicitly.
7. The coordinator merges shard-local hits using the same encoded tuple.
8. The response serializes JSON-compatible `_sort` values.

For `_id`, the native sortable path is backed by identity metadata and primary
document key order rather than user-defined doc values. `_id` remains the final
tie-breaker for all explicit field sorts.

For non-`_id` fields, the minimal production path is doc values plus a top-N
collector. The higher-performance path is an `index_sort` configuration that
physically orders segments and allows sorted seek plus early termination.

The native path should be observable as a capability, not inferred from a lucky
payload shape. A field is sortable only when the runtime mapping says it is
sortable and every relevant live segment can prove doc-value coverage for that
field. This lets operators understand whether a query failed because of schema,
segment coverage, mixed-version rollout, or an unsupported physical plan.

### Production Query Planning Contract

Every selected physical plan should make these properties explicit:

- ordering: score, `_id`, native field tuple, or unsupported
- exactness: exact, bounded exact, or approximate
- source: postings, doc set, vector index, sorted segment, primary key scan, or
  doc-values collector
- cursor support: comparator-only, segment seek, distributed seek, or none
- source-load behavior: source-free, projected-source-after-page, or
  stored-source-required
- distributed behavior: shard-local only, coordinator merge, or unsupported

This avoids hidden behavior such as "native sort unless one segment is missing
coverage, then parse stored JSON." Once a plan is chosen as native, missing
native coverage is an execution error unless it is represented by the
field's typed missing/null marker.

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

- Public mappings expose `sortable`; the schema compiler derives internal
  typed doc-values capability from it.
- Public schema requests that try to configure `doc_values` directly are
  rejected; `doc_values` remains a runtime capability and diagnostic term.
- Sortable mappings are rejected for multi-valued JSON shapes and for reserved
  `_id` document fields, including dynamic templates and pattern properties
  that could target `_id`.
- Text-backed, match-all, and filter-backed exact field sorts require a native
  sort plan (`native_doc_values_top_n`, `_id` seek, sorted-segment seek, or
  distributed k-way merge).
- The engine only returns a sorted page when it can prove the candidate set and
  sort tuple are exact under the requested order.
- If native coverage, cursor typing, or exact candidate budgets cannot be
  proven, the API returns 422 instead of returning a partially sorted page.
- Stored JSON sorting is test/debug-only compatibility behavior and is not a
  production fallback for public exact `order_by`.

The remaining production work is mostly performance depth: broader native
filter coverage, physical `index_sort` acceleration, compaction/backfill
tooling for coverage changes, and continued benchmarks over broad and selective
query shapes.

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
- `_score` sort requires every candidate hit to carry a finite score from a
  score-bearing executor; missing, `NaN`, and infinite scores are not coerced
  into sortable values
- match-all and filter-only plans must not manufacture constant scores to
  satisfy `_score` ordering
- text `match_all`, doc-id/range/geo/boolean filter shapes, and bool queries
  with no positive scoring child are not score-bearing sources for `_score`
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

### Native Candidate Sets

Native candidate sets are the bridge between query matching and sort execution.
They should represent exact document eligibility without requiring stored JSON
loads:

- full-text candidates come from inverted postings and analyzer-aware query
  lowering
- structured candidates come from doc values, bitsets, range metadata, or
  exact postings
- `_id` candidates come from identity metadata and primary-key order
- vector candidates come from vector-native iterators with explicit exact or
  approximate semantics
- graph candidates come from graph traversal outputs with explicit bounds

The sort executor should consume these candidates through a common abstraction:
document identity, document ordinal, optional score, and optional segment-local
membership bitmap. This is what allows the planner to choose between
candidate-first doc-values top-N and sorted-order scan with membership testing.

For broad queries whose requested order matches `index_sort`, scanning sorted
segments and testing membership is usually better than collecting and sorting a
large candidate set. For highly selective queries, iterating the candidate doc
set and using a doc-values collector is usually better. Both paths must produce
the same visible order and cursor tuples.

## Mapping Model

Antfly should not add a separate top-level `sortable_fields` DSL. Sortability
belongs in the existing schema and mapping system described in `SCHEMA.md` and
`FULL_TEXT.md`.

Public schema examples should desugar into runtime mappings with typed field
capabilities. `sortable` is the user-facing declaration; Antfly derives the
internal typed doc-value structures needed for exact sort execution:

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
        "sortable": true
      }
    },
    "rank": {
      "type": "integer",
      "x-antfly-field": {
        "type": "integer",
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
- internal typed doc-values capability derived from `sortable`
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

The long-term Antfly answer should be "use the Antfly mappings." There should
not be a parallel sort-field registry that operators have to keep in sync with
schema, SDKs, OpenAPI, and docs. Elasticsearch users already expect
keyword/numeric/date/boolean mapping metadata to determine whether a field is
filterable, aggregatable, and sortable. Antfly should preserve that mental model
while compiling it into Antfly-native segment metadata and doc-value sections.

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

Dynamic templates should mark scalar fields as sortable. Runtime compilation
derives the internal doc-values requirement:

```json
{
  "match_mapping_type": "date",
  "mapping": {
    "type": "date",
    "sortable": true
  }
}
```

Public mappings should not expose a separate `doc_values` switch for sort.
Schemas that contain `doc_values` should be rejected at the public schema
boundary; `doc_values` remains an internal runtime capability and diagnostic
term only.

The runtime schema must persist the observed dynamic mapping decision for a
field path. Query-time validation should not guess sortability from the latest
document payload. It should validate against compiled explicit mappings,
compiled dynamic rules, or persisted observed dynamic field metadata.

### Field Capability Matrix

Mappings should lower into a compact capability matrix that the planner can use
without reinterpreting JSON Schema on every query.

| Field type | Search primitive | Filter primitive | Sort primitive |
| --- | --- | --- | --- |
| `text` | analyzer + postings | postings/query lowering | unsupported directly |
| `keyword` | exact term postings | postings or doc values | doc values / index sort |
| `integer` | numeric range index | doc values/range metadata | doc values / index sort |
| `number` | numeric range index | doc values/range metadata | doc values / index sort |
| `date` | date range index | doc values/range metadata | doc values / index sort |
| `boolean` | boolean bitset | doc values/bitset | doc values / index sort |
| `geo` | geo index | geo structure | unsupported unless explicitly designed |
| `vector` | ANN/exact vector index | vector-native filters | score order only |
| `_id` | identity lookup | identity/doc set | identity key order |

The capability matrix should be persisted with each index generation and
included in segment metadata. A query should validate against the matrix before
opening stored documents. This is how `www-antfly` docs, OpenAPI examples, SDK
validation, and runtime behavior stay aligned: one mapping model describes what
the engine can actually execute.

For arrays and multi-valued fields, sort must remain unsupported until Antfly
has an explicit sort mode such as `min`, `max`, or `median`. Guessing from
payload shape would create unstable cursor semantics.

### Dynamic Field Promotion

Dynamic mappings need a durable promotion path:

1. A dynamic template matches a previously unseen field path.
2. The write path records the inferred field type and mapping provenance.
3. Segment build writes the appropriate postings/doc-values sections.
4. Reopen/merge verifies the section is complete.
5. Query planning treats the field as sortable only after physical coverage is
   present for all relevant live segments.

This prevents a field from becoming sortable just because the newest document
looks scalar. Sortability is a physical guarantee, not a payload observation.

### Sortable Field Lifecycle

A field should move through explicit lifecycle states before public sorted
queries can rely on it:

1. `declared`: schema or dynamic-template metadata says the field is intended
   to be scalar, doc-valued, and sortable.
2. `indexed`: new writes are producing typed doc values for the field.
3. `covered`: every live segment relevant to the index generation has complete
   doc-value coverage or an explicit typed missing/null marker.
4. `queryable`: the planner capability matrix can prove exact sort/filter use
   for the field.
5. `accelerated`: optional `index_sort` or range metadata can make common
   plans cheaper, but the field is already correct through doc values.

Only `queryable` fields should be accepted by public `order_by`. Earlier states
are useful for rollout, backfill, and diagnostics, but they are not enough for
an exact public sort contract.

This lifecycle is also the right shape for operations. A rollout can declare a
field, start writing doc values for new segments, backfill or compact old
segments, and only then expose the field as sortable in the generated docs.
The public `www-antfly` documentation, OpenAPI examples, SDK validation, and
runtime planner should all derive from the same effective mapping/capability
view rather than from hand-maintained release notes.

## Doc Values

Doc values are the first native primitive Antfly uses for production sorting.

For every mapped field whose runtime schema has internal doc-values capability
derived from `sortable: true`, segment build and replay should write a typed
column keyed by document ordinal:

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
- unsigned numeric values use exact unsigned integer comparison and must not be
  coerced through floating point
- floats use sortable IEEE encoding with a defined NaN policy
- dates normalize to epoch nanoseconds before encoding and compare as unsigned
  integers
- keywords use normalized UTF-8 bytes plus length delimiters
- booleans use false < true
- missing/null values use explicit sentinels

The result hit should expose the original JSON-compatible sort value, not the
internal byte encoding. In particular, date/datetime doc values may be stored
as unsigned epoch nanoseconds, but `_sort` should expose a date-time string
that can be fed back unchanged as `search_after` or `search_before`.

### Native Sortable Field Path

A native sortable field path consists of all of the following:

- a runtime mapping entry for the field path
- a scalar sort type: keyword, integer, floating number, date, boolean, or `_id`
- `sortable: true`, which derives typed doc values except for `_id`, which is
  backed by identity metadata
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

Once the planner has selected a native doc-values sort plan, doc-value misses
from missing ordinals, unresolved native document ids, or absent physical
coverage are planning/execution errors unless the value is represented by the
typed missing/null marker. The native path must not silently fall back to stored
JSON extraction because that would hide index coverage bugs and turn a
production exact-sort request into an unbounded source scan.

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
- floats: IEEE sortable transform with NaN ordered after all numeric values in
  ascending order
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
- the effective requested sort tuple, including Antfly's implicit `_id`
  tiebreaker, is a leading prefix of the configured `index_sort`
- cursor tuple can be encoded into the same comparator domain
- query source can test document eligibility in sorted order
- the result does not require a conflicting primary order such as ANN score

This prefix rule is intentionally defined over the effective tuple rather than
only the fields explicitly supplied by the client. For example, a request for
`created_at desc` can use `index_sort=[created_at desc, _id asc]`, because the
effective request is `[created_at desc, _id asc]`. It cannot use
`index_sort=[created_at desc, category asc, _id asc]`, because the physical
order includes `category` before `_id`; scanning that layout would not be sorted
by `[created_at desc, _id asc]` for documents with equal `created_at`.

When those conditions hold, sorted segment seek should be the preferred plan for
broad match-all/filter queries with small pages.

### Sorted Segment Seek

Sorted segment seek is the target path for broad queries whose requested sort
matches physical `index_sort`.

Execution shape:

1. Encode `search_after` or `search_before` into the physical sort tuple.
2. Seek each segment to the first candidate tuple after/before the cursor.
3. Walk segment documents in physical sort order.
4. Skip deleted, expired, or superseded documents through live-doc metadata.
5. Test native filter/postings membership without loading `_source`.
6. Emit hits until the shard page window is full.
7. Stop scanning the segment when early-termination conditions are satisfied.

For match-all with `_id asc`, the existing primary document key order can be a
special case of sorted seek. The executor should seek the primary-key range from
the `_id` cursor and stop after the page window, subject to live-doc, TTL, and
identity generation checks. It should not build and sort an in-memory list of
all document ids for this common path.

For non-`_id` sorts, sorted seek requires durable segment ordering. Doc values
alone are not enough to seek directly by sort tuple; they support exact top-N
collection but still require visiting candidate ordinals.

### Collector Path

When the requested sort does not match `index_sort`, the exact native fallback
is a doc-values collector.

The collector path should:

- iterate the exact candidate source only once
- load typed doc values by ordinal
- compare against `search_after` / `search_before` before admitting a row
- maintain a bounded heap/window sized by page requirement and shard merge
  needs
- defer `_source` loads until after the final page has been selected
- return 422 if exact candidate processing exceeds configured production
  budgets

This path is `O(matches * log(page_window))`, not `O(matches * log(matches))`,
and memory is bounded by the requested page/shard window rather than corpus
size.

### Native Filter And Sort Cooperation

Filter and sort planning should be costed together:

- selective filter + non-matching sort: iterate filter doc set and use
  doc-values top-N
- broad filter + matching `index_sort`: scan sorted segments and test filter
  membership
- full-text + field sort: choose between postings candidate collection and
  sorted-order scan against a text-match doc set
- vector + filter: push filters into the vector source only when the vector
  implementation can preserve the requested exactness class

The planner should use segment statistics such as doc count, live count,
min/max sort values, field cardinality, postings sizes, and filter selectivity
estimates. A fixed "always collect candidates then sort" rule will not scale for
broad queries, while a fixed "always scan sort order" rule will not scale for
highly selective filters.

### Planner Decision Matrix

The long-term planner should make the following decisions explicitly:

| Query shape | Requested order | Preferred exact plan | Fallback exact plan |
| --- | --- | --- | --- |
| match-all | `_id asc` | primary-key seek | doc-values top-N |
| match-all | matches `index_sort` | sorted segment seek | doc-values top-N |
| match-all | arbitrary sortable field | doc-values top-N | reject on budget/coverage |
| selective structured filter | arbitrary sortable field | filter doc set + doc-values top-N | reject on budget/coverage |
| broad structured filter | matches `index_sort` | sorted segment seek + filter membership | doc-values top-N |
| full-text, no `order_by` | relevance | score top-k | reject on scorer failure |
| full-text + field sort | field tuple | doc-values top-N or sorted segment seek + text membership | reject on budget/coverage |
| vector ANN, no `order_by` | vector score | vector top-k | reject on vector failure |
| vector ANN + field sort | field tuple | unsupported unless eligible set is exact | approximate only through explicit future API |
| distributed field sort | field tuple | shard-local exact plan + coordinator k-way merge | reject on unsupported shard |

The table is intentionally conservative. Exact `order_by` should never mean
"sort whatever candidates an approximate source happened to return." When the
eligible set is approximate, field sorting is approximate too and needs a
separate API contract.

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
- Public cursor values must be replayable JSON scalars: string, integer,
  finite float/number, or boolean. `null`, arrays, objects, and non-finite
  numbers are rejected at the API boundary because they cannot form stable
  cursor tokens.
- Cursor value types must match mapped field types after coercion.
- Cursor positions for `_id` must be strings and must match the `_id` value in
  returned hit `_sort` tuples.
- Date/datetime cursors accept returned date-time strings or exact
  non-negative epoch nanoseconds; internal unsigned nanosecond values must not
  be rounded through `float`.
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

The sorted segment path for full-text requires a segment-local text membership
structure. The planner should lower the text query into exact postings/doc-set
membership first, then scan documents in physical sort order and test whether
each local document id is a member. This avoids two bad outcomes:

- collecting every full-text hit into memory before sorting a broad query
- overfetching a score-ordered text page and pretending it is globally sorted by
  a field

The doc-values top-N path remains the right general fallback. It is better for
selective text queries and for field orders that do not match `index_sort`.
Both paths must share the same typed sort tuple, cursor comparison, and `_id`
tie-breaker.

Text queries must not return a partial field sort. If exact sort would exceed
budget and no native plan can prove exactness, return 422.

Score sorting should remain separate:

- `_score` is produced by the text/vector engine.
- `_score` sort fails closed if any candidate lacks a finite score.
- match-all and filter-only executors reject `_score` ordering unless they are
  explicitly wrapped by a real score-bearing query source.
- text planning accepts `_score` only for lexical/scoring query shapes, not for
  filter-shaped text queries that happen to run through the text executor.
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

Native filtering is still the right building block for vector search. The
problem is using a bounded ANN result set as though it were the complete
eligible set for an exact field sort. A native filter can reduce the vector
candidate universe efficiently; it does not by itself prove that the first ANN
page contains the globally earliest or latest documents under an unrelated
field order.

The future production shape for vector-plus-field-order should be one of:

- score order over vector results, with optional native filters pushed into the
  vector index
- exact vector scoring over a proven bounded eligible set, followed by the
  requested exact field order if that is the documented semantics
- an explicitly approximate API that says it overfetches vector hits and reranks
  or sorts that approximate candidate set

The current exact `order_by` contract should use only the first two shapes.

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
- `native_doc_values_top_n`
- `score_top_k`
- `distributed_k_way_merge`
- `unsupported_exact_sort`

The incremental implementation should expose those choices through an explicit
sort execution plan instead of encoding them as nullable loaders or hidden
fallbacks. The current in-process plan names should distinguish at least:
`none`, `id_only`, `id_seek`, `sorted_segment_seek`,
`native_doc_values_top_n`, `score_top_k`, `distributed_k_way_merge`,
`stored_json_debug`, and `unsupported_exact_sort`. `sorted_segment_seek` and
`distributed_k_way_merge` are first-class plan names but must fail closed until
their physical executors are implemented. `stored_json_debug` is a
test/debug-only compatibility plan; once a request has planned as
`native_doc_values_top_n`, execution must require native values and fail closed
if the required ordinal/doc-value coverage is not available.

`native_doc_values_top_n` is the general "candidate source plus typed
doc-values collector" plan. The candidate source can be postings, a filter doc
set, match-all identity iteration, or another exact source. The plan name should
stay focused on the sorting primitive because that is what operators need to
see in profiles and alerts.

### Cost Model

The planner should choose between exact physical plans with a small,
explainable cost model rather than fixed request-shape rules. Inputs should
come from segment metadata and runtime mappings:

- live document count and delete ratio
- candidate estimate for postings, filters, vector sources, or match-all
- field doc-value coverage and expected load cost
- `index_sort` compatibility and segment min/max tuple bounds
- cursor position, when it can be estimated from bounds
- requested `limit`, `offset`, and distributed shard window
- source projection cost

The first production implementation can use conservative heuristics, but the
decision should still be explicit and observable. For example:

- choose `sorted_segment_seek` when the requested order matches `index_sort`,
  the query can test membership in sorted order, and the expected scan to fill
  the page is smaller than collecting candidates
- choose `native_doc_values_top_n` when the candidate set is selective or the
  requested order does not match physical segment order
- choose `_id` primary-key seek for match-all `_id asc` pagination
- reject exact sort when the only available path is unbounded stored source
  extraction or an approximate candidate source

The cost model must never trade correctness for speed. It may choose a slower
exact plan over a faster approximate one for the exact `order_by` API. If a
query needs approximate behavior, that should be exposed through a separate
contract with different naming and profile metadata.

### Production Budgets And Rejection Reasons

Exact sorted search needs bounded failure modes. A request should be rejected
before doing unbounded work when the planner cannot prove an exact native path
within configured budgets.

Recommended budget dimensions:

- maximum candidate ordinals visited for bounded-exact fallback paths
- maximum source documents parsed on test/debug stored-sort paths
- maximum doc-value misses before treating coverage as broken
- maximum coordinator shard windows for distributed sorted merge
- maximum cursor seek/scanned-document ratio for sorted-segment paths
- per-query deadline budget shared across candidate generation, sort, and
  source projection

HTTP rejections should use stable user-facing machine-readable reasons, such as:

- `unmapped_field`
- `non_sortable_field`
- `unsupported_sort_field`
- `mixed_field_type`
- `field_not_sort_ready`
- `filter_not_queryable`
- `invalid_cursor_arity`
- `invalid_cursor_type`
- `invalid_sort_tuple`
- `approximate_candidate_source`
- `candidate_budget_exceeded`
- `missing_null_policy`
- `non_score_bearing_source`
- `invalid_score_value`
- `count_only_ordered_page`
- `stored_json_sort_disabled`
- `unsupported_exact_sort`
- `distributed_merge_unsupported`

Those public reason strings should appear in API errors and SDK-visible error
models. Lower-level diagnostics such as `missing_doc_values_coverage`,
`missing_doc_values_section`, `missing_native_filter_coverage`, or
`invalid_doc_value_type` should remain in logs, metrics, and
`profile.sort.sort_rejection_detail` where a profile can be returned. This
keeps the public API stable while still giving operators enough detail to tell
whether the fix is schema, backfill/compaction, query shape, budget tuning, or a
missing executor.

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

## Documentation, OpenAPI, And SDK Contract

Supported sort behavior should be generated from effective Antfly mappings and
capabilities, not copied into each release by hand.

The durable source of truth is:

1. declared schema and dynamic-template configuration
2. persisted observed dynamic mapping metadata
3. segment capability coverage for doc values and optional `index_sort`
4. planner support for the requested physical plan

Public documentation should expose two related views:

- schema-time support: which field types and mapping options make a field
  sortable in principle
- runtime support: which configured fields are currently queryable in a
  deployed index generation

`www-antfly`, OpenAPI examples, and generated SDK docs should describe the
schema-time rules: sort on `_id` or mapped scalar fields declared
`sortable: true`; do not sort on analyzed `text`; use `.keyword` multi-fields
for exact string sorting; pass returned `_sort` tuples to `search_after` /
`search_before`; expect 422 when exact native execution is unavailable.

Runtime introspection should expose the per-index public field capability matrix
so operators and clients can discover concrete configured fields. That endpoint
or manifest should include:

- field path
- field type
- `query_modes` derived from the field type and runtime capability, such as
  `full_text`, `exact`, `range`, `geo`, and `autocomplete`
- `sortable`
- dynamic/static provenance
- missing/null policy
- whether the field participates in `index_sort`
- current state from the sortable field lifecycle

The public capability surface should not expose `searchable`, `filterable`,
`aggregatable`, `doc_values`, doc-value coverage, or queryability state as
schema toggles. Those are internal runtime facts and may still appear in planner
diagnostics when explaining exact-sort acceptance or rejection.

The OpenAPI contract should keep `order_by.field` as a string because legal
fields are index-specific and can be dynamic. SDKs can add optional helper
types generated from an index manifest, but the base client should still accept
strings and surface server validation errors. This avoids baking one tenant's
schema into the global API while still letting managed workflows generate
strongly typed helpers for known production indexes.

The `order_by` tuple object itself should be closed: unknown properties such as
a misspelled direction key must be rejected by schema-aware clients or by the
server instead of silently defaulting to ascending order.

Release tooling should validate that docs and generated clients describe the
same behavior as the engine:

- schema examples compile into runtime mappings with expected capabilities
- documented cursor examples round-trip through the parser
- public examples do not use unsupported `text` sorting
- generated SDK models include `_sort`, `search_after`, and `search_before`
- release manifests list supported field types and index-sort limitations

This is the long-term replacement for release-by-release migration notes. A
tagged release should publish engine code, mapping capability metadata, OpenAPI,
SDKs, and docs from the same configuration snapshot.

## Production Observability

Sorted search needs first-class telemetry because correctness failures often
look like performance fallbacks. Operators should be able to answer, for any
slow or rejected query, which physical plan ran and why the planner did not
choose a better one.

Every sorted query profile should include a compact stable public surface:

- selected sort plan name
- requested order fields after implicit `_id` normalization
- exactness class
- sort source, cursor support, source-load strategy, distributed behavior, and
  selection reason
- whether the selected public plan requires native typed sort values
- candidate count
- cursor-rejected count
- selected count
- distributed shard count, when applicable
- total sort latency
- budget rejection reason, when rejected
- stable public sort rejection reason, detail, and field

The request profile should expose these under `profile.sort` so they are
available through the normal API response. The stable public fields stay
compact and operationally meaningful: `plan`, `order_by`, `cursor`,
`exactness`, `source`, `candidate_source`, `cursor_support`, `source_load`,
`distributed_behavior`, `selection_reason`, `require_native`,
`sort_lifecycle_state`, `index_sort_coverage`,
candidate/selected/cursor-rejected counts, distributed shard count, total sort
time, and bounded rejection fields such as `budget_rejection_reason`,
`sort_rejection_reason`, `sort_rejection_detail`, and `sort_rejection_field`.

Lower-level executor counters such as doc-value load timings, stored-source
load counts, collector heap peaks, index-sort availability flags, and
implementation-specific window counters are still useful diagnostics, but they
should not be part of the normal SDK-facing query response. Logs, traces,
benchmark telemetry, and explicit debug surfaces can carry the richer internal
detail; OpenAPI and the base SDKs stabilize only the fields that operators can
rely on across executor rewrites.

The stable plan names should be suitable for logs, traces, metrics, and tests.
The current implementation should expose at least `none`, `id_only`, `id_seek`,
`sorted_segment_seek`, `native_doc_values_top_n`, `score_top_k`,
`distributed_k_way_merge`, `stored_json_debug`, and `unsupported_exact_sort`;
unimplemented physical/distributed plans should be visible to diagnostics but
rejected at runtime until their executors exist.

Sort should not need a dedicated dashboard. It should add concise labels and
alerts to the existing query/search observability surface:

- query latency/count by sort plan, exactness, source, candidate source, and
  selection reason
- rejection counts by stable sort rejection reason and budget rejection reason
- native coverage failure counts for missing doc values, missing index-sort
  coverage, and blocked stored-JSON debug fallback
- sampled logs/profiles for detailed executor counters when deeper diagnosis is
  needed

The primary operational signals are abnormal exact-sort rejection rates,
budget failures, or native coverage failures after a rollout. Those should be
actionable from the existing query metrics and logs without a separate sort
dashboard.

The most important invariant is that telemetry must distinguish "native path
was selected and succeeded" from "native path was unavailable." A public sorted
query must not quietly move from native doc values to stored JSON. If native
coverage is missing, the request should fail closed and report the missing
capability: unmapped field, non-sortable field, missing doc-values section,
typed-value kind mismatch, sparse live-doc coverage, unsupported cursor type, or
unsupported distributed merge.

## Release Gates

Each phase should have an explicit production gate before it is relied on by
public APIs:

- correctness tests for the comparator, cursor, and `_id` tie-breaker
- reopen/compaction tests proving doc-value and live-doc coverage survives
- planner tests that reject unsupported exact sorts instead of falling back
- profile/log tests for stable plan names and failure reasons
- benchmark coverage for broad match-all, broad text, selective filter, and
  distributed merge paths
- migration/reindex tooling for layout changes such as `index_sort`

For deployment, new exact sort paths should be enabled behind a runtime
capability check, not only a code version. Rolling upgrades must tolerate mixed
segments where some generations do not yet have the required physical sections.
Those mixed states should reject newly unsupported exact sorts with clear 422s
until backfill, compaction, or reindex has made coverage complete.

## Rollout Plan

### Phase 0: Make Current Behavior Fail Closed

1. Keep the current exact budgeted fallback as the safety baseline.
2. Make public sorted queries validate mapping and physical coverage up front.
3. Reject unmapped, non-sortable, or physically uncovered sort fields.
4. Ensure native sort plans never fall back to stored JSON once selected.
5. Keep stored JSON sorting behind test/debug-only paths.

### Phase 1: Native Doc-Values Sort

1. Compile sortable/doc-value capability from mappings into runtime schema.
2. Persist typed doc values for mapped scalar fields during segment build,
   replay, reopen, and compaction.
3. Add canonical per-type sort encoding and comparator tests.
4. Use doc values for field sort instead of parsing stored JSON.
5. Defer projected `_source` loading until after page selection.
6. Add observability for candidate count, doc-value load time, cursor rejects,
   source loads, and budget rejections.

This phase makes arbitrary mapped scalar sort exact and production-safe, even
before physical segment sorting exists.

### Phase 2: `_id` Storage-Order Seek

1. Treat `_id asc` match-all as a native primary-key ordered scan.
2. Lower `search_after` on `_id` into an exclusive primary-key range lower
   bound.
3. Apply live-doc, TTL, identity generation, filters, and exclusions during the
   scan.
4. Stop after the requested page/shard window.

This phase removes the need to collect every document id for the most basic
sorted pagination path.

### Phase 3: Physical `index_sort`

1. Add `index_sort` configuration to mappings/index metadata.
2. Validate `index_sort` against mapped sortable/doc-value fields.
3. Flush new segments in configured sort order.
4. Preserve sort order during segment merges.
5. Store segment min/max tuple metadata for pruning.
6. Teach match-all and filter-only queries to use sorted segment seek.

Changing `index_sort` requires a new index generation or reindex because it is a
durable physical layout choice.

### Phase 4: Planner And Distributed Search

1. Add explicit sort execution plans: `none`, `id_seek`,
   `sorted_segment_seek`, `native_doc_values_top_n`, `score_top_k`,
   `distributed_k_way_merge`, `stored_json_debug`, and
   `unsupported_exact_sort`.
2. Teach full-text queries to choose between text-candidate collection,
   doc-values top-N, and sorted-order scan with text-match testing.
3. Add distributed k-way merge over typed sort tuples.
4. Forward `search_after` to every shard and merge shard-local windows at the
   coordinator.
5. Return exact or lower-bound total-hit relations based on shard capabilities.

### Phase 5: Cleanup

1. Remove stored JSON sort from public query execution.
2. Keep compatibility/debug hooks only where they are explicitly named.
3. Update SDKs, OpenAPI examples, and docs so supported sort behavior matches
   runtime mappings.
4. Add concise sort labels and alert rules to existing query metrics for plan
   selection, budget failures, doc-value coverage failures, and source-load
   behavior.

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

- `SORT_SLICES.md`
- `SCHEMA.md`
- `FULL_TEXT.md`
- `DOCID.md`
- `DB.md`
