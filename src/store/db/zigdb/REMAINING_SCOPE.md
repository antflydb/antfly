# Remaining zigdb Scope

This file is the triaged backlog for the remaining `zigdb` adapter guard paths.

The rule is:

- `implement_in_zig`: shard-local semantics that should move into the Zig engine
- `distributed_only`: semantics that need a coordinator-owned global final doc set
  or global corpus stats
- `reject_by_contract`: behavior we should keep rejecting unless we adopt an
  exact, explicit contract

## implement_in_zig

These are the remaining local `coreDB` semantics worth adding to Zig rather than
 leaving in Go compatibility code.

- Search query translation gaps in
  [zig_coredb_adapter_zigdb.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/store/db/zig_coredb_adapter_zigdb.go)
  around `Search bleve query type`
  - only add the public request shapes that antfly actually uses
  - execution should stay in Zig; Go should only normalize/translate
  - after the current audit, there are no major missing leaf families left in
    the local full-text path; the remaining work is mostly edge-option cleanup
    and explicit rejection
  - concrete candidates if antfly starts emitting them:
    - dynamic-template schema features beyond `match` / `path_match` in
      `UpdateSchema`
    - broader geo-shape relations or non-polygon GeoJSON forms, but only if the
      local Zig filter layer grows exact support for them
    - mapping-local analysis features that are still only available through the
      Bleve mapping cache and not through compiled schema analysis config

- Remaining mixed local envelopes where one final local doc set can be defined
  exactly
  - current rule: route to Zig only when the adapter can name one authoritative
    local result set
  - add more cases only if Zig can own that final set directly
  - after the current aggregation-guard audit, the remaining local candidates
    are narrow:
    - mixed local envelopes that already materialize one final fused hit set
      before aggregation
    - local aggregation families that Zig already implements, but that are still
      blocked by adapter-side request-shape guards rather than missing engine
      execution

- Pattern query breadth
  - current Zig support is still intentionally narrowed
  - good candidates are richer `node_filter.filter_query` boolean forms and
    more pattern shapes that still execute deterministically in Zig

- Future local pipeline families
  - only after the public aggregation API is explicit
  - likely next candidates: additional bucket pipelines beyond the current set

- Performance / production hardening in Zig
  - use term-doc-freq cache hit/miss counters to profile `significant_terms`
  - only then decide whether a persisted term-stat sidecar is justified
  - keep addressing leak/ownership issues surfaced by the Zig suite

## distributed_only

These should not be solved by a shard-local `coreDB` implementation.

- Cross-shard mixed graph/search/fusion final-doc-set semantics
  - the coordinator owns the global result set
  - local `coreDB` instances should only produce shard-local results

- Distributed corpus-aware aggregations
  - `significant_terms` is the main case
  - shard-local engines can return local term stats, but the coordinator owns
    the global merge and final scoring
  - adapter guards currently rejecting these for local mixed envelopes are the
    correct boundary when full-text corpus stats are not locally authoritative:
    - `Search fusion aggregations requiring full-text corpus stats`
    - `Search graph aggregations requiring full-text corpus stats`
    - `Search vector/sparse aggregations requiring full-text corpus stats`
  - `significant_terms background_filter` also stays here whenever the filter
    cannot be normalized into the exact local subset

- Global sort/paging over merged shard results
  - once a result set is cross-shard, ordering and cursor semantics belong in
    the distributed layer rather than the local adapter

- Any aggregation that requires global membership in the final doc set
  - local Zig can be a building block
  - the coordinator remains the source of truth for the global domain
  - this includes the remaining local adapter guards where there is no one
    authoritative final local doc set:
    - `Search aggregations outside supported vector/sparse path`
    - `Search aggregations outside narrowed full-text path`
    - `Search fusion aggregations without fused result set`
    - `Search graph aggregations outside supported graph-only path`
    - `Search graph aggregations without graph results`

## reject_by_contract

These should stay explicitly unsupported unless we choose to define exact
 semantics for them.

- Arbitrary Bleve sort semantics beyond the exact supported subset
  - mixed scalar type comparisons
  - opaque Bleve-specific ordering quirks
  - broader composite behaviors beyond the current local contract

- Mixed-envelope requests where there is no authoritative final local doc set
  - reject instead of approximating

- Any feature that exists only because Bleve has it, rather than because the
  antfly public contract requires it

- Empty or degenerate Bleve query shapes that do not express a meaningful local
  search contract
  - empty conjunctions / disjunctions
  - empty `BooleanQuery`
  - empty `DocIDQuery`
  - empty numeric/date/term range bounds
  - empty `IPRangeQuery`

- Unsupported geo-shape semantics beyond the current exact local contract
  - invalid polygon geometry
  - unsupported GeoJSON types
  - unsupported relations outside the normalized subset
  - malformed distance / bbox payloads

- Invalid fuzziness / date-parser option payloads
  - unsupported fuzziness values
  - unknown or unresolvable date parser names
  - malformed query-string parse results

- Aggregation request shapes that are intentionally outside the current exact
  local contract
  - unsupported pipeline aggregation types
  - local-only sub-aggregation trees that the adapter cannot translate into Zig
    without inventing semantics
  - malformed `background_filter` query types outside the supported normalized
    subset

## Current user-facing guard shapes

These are the main adapter-level unsupported shapes that still matter:

- custom sort outside the exact supported subset
- aggregations that require full-text corpus stats in non-full-text local
  contexts
- unsupported pipeline aggregation types
- unsupported pattern `node_filter.filter_query` shapes
- unsupported or invalid Bleve query payloads in the remaining tail
  - mostly malformed/degenerate cases rather than missing common antfly query
    families

## Audited aggregation-guard summary

After auditing the remaining aggregation guards in
[zig_coredb_adapter_zigdb.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/store/db/zig_coredb_adapter_zigdb.go):

- already covered locally in zigdb:
  - narrowed full-text backend aggregations
  - single dense / single sparse backend aggregations
  - graph-only node/path/pattern doc-domain aggregations
  - final-hit-set aggregation over local fusion / expand results when the
    adapter has one authoritative local result set

- `distributed_only` guards:
  - anything requiring full-text corpus stats outside a locally authoritative
    full-text result domain
  - any mixed envelope where aggregation semantics depend on a coordinator-owned
    final doc set

- `reject_by_contract` guards:
  - unsupported local pipeline families
  - malformed or unsupported `background_filter` query types
  - request shapes that would force the adapter to reconstruct aggregation
    semantics instead of routing to Zig or to the coordinator

- conclusion:
  - there is no large missing local aggregation family left for parity
  - the remaining gaps are mostly ownership/contract boundaries, not missing
    Zig aggregation math

## Audited query-tail summary

After auditing antfly query construction sites and the remaining
`zigUnsupported("Search bleve query type")` branches:

- already covered locally in zigdb:
  - `match_all`, `match_none`, `match`, `term`, `match_phrase`
  - `phrase`, `multi_phrase`
  - `prefix`, `wildcard`, `regexp`
  - `fuzzy` including `prefix`
  - `numeric_range`, `date_range`, `date_range_string`
  - `doc_id`, `bool_field`
  - `geo_distance`, `geo_bbox`, `geo_bounding_polygon`, polygon /
    multipolygon `geo_shape`
  - `conjunction`, `disjunction`, `boolean`
  - `query_string`
  - boost on the supported local leaves

- current remaining reject sites are mostly:
  - invalid option payloads
  - empty query families
  - malformed query-string parse results
  - malformed geo geometry / unsupported geo-shape relations
  - unknown date parser names when neither analysis config, mapping, nor Bleve
    global cache can resolve them

- conclusion:
  - there is no large missing local Bleve-query family left for parity
- the next real work is lifecycle coverage and continued adapter deletion, not
  adding another broad query class just for completeness

## Audited sort / cursor summary

After auditing the current sort / cursor guards and tests:

- already covered locally in zigdb:
  - `_id`
  - `_score`
  - stored scalar fields
  - string-array fields via their visible Bleve sort token
  - object and non-string-array fields via Bleve-compatible synthetic
    composite sort tokens
  - `nil` / missing values with explicit ordering
  - cursor paging over the visible sort tuple for supported local full-text
    fields
  - graph-only node-result sort / cursor on the same exact subset
  - fusion-result sort / cursor on the same exact subset

- current decision:
  - parity should follow the public antfly `full_text_search` sort / cursor
    surface, not generic internal Bleve behavior
  - if a current antfly API consumer can express a local full-text sort/cursor
    shape, it belongs in the parity backlog rather than `reject_by_contract`
  - treat broader sort / cursor semantics as either `reject_by_contract` or
    coordinator-owned when the result set is cross-shard

The source of truth for those guards is still
[zig_coredb_adapter_zigdb.go](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/store/db/zig_coredb_adapter_zigdb.go).
