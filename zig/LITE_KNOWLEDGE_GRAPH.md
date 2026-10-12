# Embedded Lite knowledge graphs

A writable Lite database promotes resolved entity documents into named tables
through an atomic cross-table commit. Create destination tables before adding
resolvers. Exact-key, prefix and ANN candidate blocking read those same tables.
A read-only connection never attaches a promotion sink.

`run_until_idle_json` drains all tables until recursive work converges. Its
`tables` object identifies per-table pending work; enrichment counters include
`last_error_code`. The output buffer can contain diagnostics even when the call
returns an error. Failed asynchronous extraction does not roll back the source
write. Inspect enrichment errors when waiting for graph materialization.

Set `OpenOptions.models_dir` to use application-managed inference models for
database enrichments. The database owns a copy of this path. The C struct keeps
its existing size by consuming reserved slots; older callers remain compatible.

`add_index_json` accepts public `{ "name": "kg", "type": "graph", ... }`
configuration and the existing `{ "name", "kind", "config_json" }` envelope.
`config_json` may be an object or an escaped JSON string. A graph may combine
artifact-derived relations and field-derived edges such as `supersedes`.

For mention graphs, use fresh seeded PageRank with `direction: "both"` in
`graph_metric` or `graph_metric_rerank`. The default remains `out`. Directional
personalization requires seeds and `metric_freshness: "fresh"`; it does not
change the graph's stored edges or published global metric.

A traversal supports `direction: "both"`, `max_depth: 2`, `include_documents`
and `include_paths`, including starts from `$query_results`. Set
`deduplicate_nodes: false` to retain distinct paths from multiple seeds.
Traversal depth, result and work limits still apply.

The C ABI exposes `antfly_last_error_detail()` on the calling thread. Rust
exports `last_error_detail()`, Go exports `LastErrorDetail()`, and Python appends
the detail to raised errors after checking `antfly_last_error_code()`. Native
entry points clear retained diagnostics, including successful calls and missing
document lookups. Read detail immediately after a failed call. Go callers
using the separate accessor must lock their goroutine to the OS thread across
the failed operation and accessor.

GLiNER Decide-1B admits one input per wire request. Decision enrichment
`batch_size` controls concurrent one-document requests; it does not combine
documents into a multi-input Decide request. The default batch size is supported.
Request sizes include the complete schema/question prompt. Requests exceeding a qualified bound fail instead of truncating.

Neighbor context can sample an entity table's local graph. Point a conceptualizer
at a context graph whose sources are independent of that conceptualizer; the
catalog rejects producer feedback cycles. Generated `is_a` edges can live in a
separate taxonomy graph on the same table.

New pulls publish `qualified_limits` through `list_models`, with backend, feature
and geometry profiles. Refresh an older model's pull metadata to discover those
limits. The receipt is informational; serving still verifies the actual loaded
artifact and complete prepared request.
