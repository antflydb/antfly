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


## Canonical query keys, merging and extraction evaluation

Applications can use the same resolver as ingestion without duplicating Unicode
slug helpers or writing temporary documents. The remaining integration surfaces
tracked in #1109 are available:

| Need | Supported path |
| --- | --- |
| Render canonical query seeds | `render_resolver_key_json`, using the durable template and aliases |
| Extract and resolve ad-hoc text | `resolve_query_json`, including the configured extractor, ANN candidates and scorer |
| Preserve Unicode marks | `uslug` retains attached combining marks; use the render API for exact canonical bytes |
| Configure embedding merging | [Public table/index recipe](RESOLUTION.md#embedding-merge-recipe-and-ordering), tested through `add_index_json` |
| Alias keys without entity promotion | Inline resolver `aliases` with `promote: false` |
| Merge keys after ingestion | `merge_resolver_keys_json`, followed by a root drain to replace resolver-owned edges |
| Establish ANN visibility | Successful root `run_until_idle`; submit later ingestion after the drain returns |
| Compare descriptions/configs | [Extraction comparison script and guidance](RESOLUTION.md#comparing-extraction-configs), with per-label counts and labelled exact-span metrics |
| fp16-encoder descriptions | Windowed descriptions are unsupported and not planned; use the qualified FP32 base or multi-Decide bundle |

See [query resolution and aliases](RESOLUTION.md#resolve-ad-hoc-query-text-and-mentions)
and the [full resolution contract](RESOLUTION.md) for request shapes, migration
limits and replay behavior. A root drain does not guarantee that later mentions
in the same ingestion batch see newly promoted embeddings. Drain between
incremental ingestion units, or merge duplicate minted keys explicitly afterward.
