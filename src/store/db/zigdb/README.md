# zigdb

This package is the first cgo bridge for running `antfly-zig` behind the Go
`coreDB` contract.

Selection:

- build with `-tags zigdb`
- set `ANTFLY_COREDB=zig` or `ANTFLY_COREDB=zigdb`
- otherwise Go still uses the native `DBImpl`

Current scope:

- `StoreDB` can now instantiate a build-tagged Zig-backed `coreDB` adapter
- transaction begin with explicit txn id and participants
- write intents
- resolve intents
- get transaction status
- get commit version
- get timestamp
- raw batch writes/deletes
- parsed lookup via JSON
- scan, including `filter_query` when documents are requested
- narrowed search bridge for common full-text / dense / sparse / graph request shapes
  - backend-native aggregations now live in Zig for the supported subset:
    `count`, `terms`, `significant_terms`, metrics, `histogram`,
    `date_histogram`, and simple `range` / geo-distance buckets
  - `significant_terms` supports a narrowed `background_filter` subset:
    `match_all`, `match`, and `term`
  - pipeline aggregations now have a narrow public contract:
    `moving_avg`, `cumulative_sum`, and `derivative` over bucket outputs via `bucket_path`
  - unsupported pipeline types are rejected explicitly
- persisted range / split-state / split-delta metadata on reopen
- persisted index listing on reopen
- graph API methods
- enrichment extraction / compute hooks
- shadow index manager lifecycle
- split / finalize split
- snapshot

Supported sort / cursor contract today:

- `_id`
- `_score`
- stored scalar fields (`string`, numeric, `bool`, or `nil`)
- string-array fields, using the visible Bleve sort token for the array
- object and non-string-array fields, using Bleve-compatible synthetic composite
  sort tokens instead of rejecting the request outright
- cursor paging over the visible sort tuple for supported full-text fields
- `nil` / missing values sort after non-`nil` values for ascending order
- mixed scalar field types for a single sort field are rejected explicitly
- adding `_id` as the last sort field is still recommended for exact stable
  pagination across duplicate sort tuples

Sort / cursor decision:

- this should track the public antfly `full_text_search` sort / cursor surface,
  not just an internal narrowed subset
- if an existing antfly API consumer can express a local full-text sort/cursor
  shape, it is parity work rather than a reject-by-contract case
- cross-shard ordering and paging semantics remain a coordinator concern, not a
  shard-local zigdb concern

Mixed-envelope contract today:

- if zigdb can identify one authoritative final doc set, it owns sort / paging /
  aggregation over that set
- this includes narrowed full-text, graph-only node/path/pattern doc domains,
  and supported fusion / expand-strategy cases
- requests that still lack one authoritative final doc set remain explicitly
  unsupported instead of falling back to approximate local execution

Enrichment ownership today:

- Go remains the provider host for generated enrichment work
- the Zig DB owns storage, indexing, chunk / artifact semantics, and query behavior
- the `zigdb` adapter translates Go host enrichment hooks onto Zig’s DB surface
- there is no direct Zig -> Go provider callback layer yet

Still missing for full `coreDB` replacement:

- full public `Search` parity
  - unsupported aggregation families that need more backend state
  - richer pipeline aggregation families beyond `moving_avg` / `cumulative_sum` / `derivative`
  - richer custom Bleve sort / `search_after` / `search_before`
  - richer graph query modes
  - mixed-envelope cases where there is still no single authoritative final doc set
- fuller stats parity
- a concrete Go type that fully satisfies the `coreDB` interface

The intended order is:

1. validate transaction parity through the bridge
2. add read/write/scan methods
3. add a narrowed `Search` bridge for the common full-text/vector request shapes
4. add graph and enrichment adapter methods
5. close the remaining search-envelope gaps
6. finish index lifecycle and stats parity

See also:

- [REMAINING_SCOPE.md](/Users/ajroetker/go/src/github.com/antflydb/antfly/src/store/db/zigdb/REMAINING_SCOPE.md) for the triaged backlog of remaining guard paths, grouped into `implement_in_zig`, `distributed_only`, and `reject_by_contract`
