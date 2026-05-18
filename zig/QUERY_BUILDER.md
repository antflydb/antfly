# Query Builder Agent

## Goal

Port the useful parts of the Go query-builder agent while widening the Zig interface so it can build Antfly queries in general, not only native Bleve full-text fragments.

The public surface should stay simple: clients call one query-builder agent with a natural-language intent and optional table/schema context. Internally, the agent can coordinate specialist flows for full-text, semantic, hybrid, filter, tree, graph, and future query shapes.

## Current State

- The Go implementation is strongest as a Bleve/full-text builder. It prompts an LLM to produce native Bleve query JSON with an explanation, confidence, and warnings.
- The Zig implementation already has a deterministic single-pass builder in `pkg/antfly/src/api/retrieval_agent.zig`. It chooses a likely text field, detects simple status filters, and emits a Bleve-style query fragment.
- The Zig HTTP route already exposes `/agents/query-builder` and returns the bounded-agent envelope with `query`, `explanation`, `confidence`, `warnings`, and `steps`.

## Target Shape

The query-builder agent should become a coordinator:

1. Resolve table, schema, example document, and index context.
2. Classify the requested query mode.
3. Delegate low-level construction to specialist builders.
4. Assemble a valid Antfly query request.
5. Validate the generated request before returning it.

The old `query` field should remain as the compatibility Bleve fragment. Newer clients should prefer `query_request` when they want an executable Antfly query object.

## Public Request Fields

Keep existing fields:

- `session_id`
- `decisions`
- `interactive`
- `max_internal_iterations`
- `max_user_clarifications`
- `require_decision_after`
- `example_documents`
- `table`
- `intent`
- `schema_fields`
- `generator`

Add:

- `mode`: Strategy hint. Suggested values: `auto`, `full_text`, `semantic`, `hybrid`, `filter`, `tree`, `graph`.
- `output`: Desired artifact. Suggested values: `query_request`, `bleve`, `filter_query`.
- `constraints`: Loose JSON object for execution constraints such as `limit`, `allowed_fields`, `prefer_indexes`, and `require_executable`.

Example:

```json
{
  "table": "docs",
  "intent": "find published raft articles about snapshot recovery",
  "mode": "auto",
  "output": "query_request",
  "constraints": {
    "limit": 10,
    "require_executable": true,
    "prefer_indexes": ["body_embedding"],
    "allowed_fields": ["title", "body", "status", "published_at"]
  },
  "interactive": true,
  "max_internal_iterations": 2,
  "max_user_clarifications": 1
}
```

## Public Response Fields

Keep existing fields:

- `query`: compatibility native Bleve/filter fragment
- `explanation`
- `confidence`
- `warnings`
- bounded-agent envelope fields such as `session_id`, `status`, `steps`, and counters

Add:

- `query_request`: Antfly `QueryRequest` object assembled from the selected specialist output.
- `specialist`: Specialist or strategy used, such as `full_text`, `filter`, or `hybrid`.
- `plan`: Optional machine-readable coordination plan for observability.

Example:

```json
{
  "status": "completed",
  "specialist": "full_text",
  "query_request": {
    "table": "docs",
    "full_text_search": {
      "conjuncts": [
        { "match": "find published raft articles about snapshot recovery", "field": "body" },
        { "term": "published", "field": "status" }
      ]
    },
    "limit": 10
  },
  "query": {
    "conjuncts": [
      { "match": "find published raft articles about snapshot recovery", "field": "body" },
      { "term": "published", "field": "status" }
    ]
  },
  "confidence": 0.87,
  "warnings": []
}
```

## Internal Architecture

### Coordinator

The coordinator owns:

- request validation
- session/envelope accounting
- schema/index/example context gathering
- mode selection
- specialist selection
- final query assembly
- validation and warnings

It should avoid directly fabricating every low-level query shape once specialist builders exist. Its job is to decide which shape is appropriate and to make the final result executable.

### Specialist Builders

Initial specialists:

- `full_text`: Port the Go Bleve builder. It should produce a native Bleve query fragment and explanation.
- `filter`: Build structured filter/exclusion queries for status, type, tenant, date, or other precise predicates.
- `semantic`: Build `semantic_search`, `indexes`, `embedding_template`, and related vector-search knobs.
- `hybrid`: Combine full-text and semantic search, with merge configuration where useful.

Later specialists:

- `tree`: Build `tree_search` requests.
- `graph`: Build `graph_searches` requests.
- `aggregation`: Build aggregation/query analytics requests.
- `projection_sort`: Build `fields`, `order_by`, pagination, and count/profile options.

### Assembler And Validator

This should be deterministic Zig code.

Responsibilities:

- Map specialist output into `QueryRequest`.
- Preserve the old `query` fragment for compatibility.
- Validate obvious schema errors before returning.
- Reject or warn about unsupported combinations.
- Keep a stable `steps` trace explaining coordinator and specialist choices.

## Execution Plan

1. [done] Document the design in this file.
2. [done] Add backward-compatible API fields: `mode`, `output`, `constraints`, `query_request`, `specialist`, and `plan`.
3. [done] Have the current deterministic builder populate `query_request` in addition to `query`.
4. [done] Add tests that old callers still receive `query`, and new callers receive `query_request`.
5. [done] Port the Go Bleve LLM builder as a `full_text` specialist behind `mode: "full_text"` and coordinator-selected `auto`.
6. [done] Add a deterministic validator for generated Bleve JSON before using it.
7. [done] Add semantic/hybrid query assembly once index context is available to the query-builder route.
8. [done] Add clarification behavior for ambiguous table, field, index, or strategy choices.
9. [done] Split query-builder coordinator and specialists into `pkg/antfly/src/api/query_builder_agent.zig`.

## Metadata Prefetch Plan

The query builder should collect table metadata once at the coordinator boundary, then pass that same context to both the LLM prompts and deterministic validation. Validation and repair feedback should not fetch a second, potentially different view of graph/table/example/index metadata.

Context to prefetch:

- Table schema fields and example documents.
- Full-text indexes, including configured fields when available.
- Embedding indexes, including dense/sparse compatibility, dimensions, and model hints when available.
- Graph indexes, including declared edge types and topology hints when available.

Execution shape:

- The HTTP and HTTPX routes build one `QueryBuilderTableContext` per table-scoped request.
- Specialist prompts receive the relevant slice of that context so the model sees the same index names, fields, and graph edge types the validator will enforce.
- `QueryBuilderPlanValidator` validates generated and assembled plans against that exact context. Repair prompts should include validator feedback rather than triggering new metadata reads.
- Missing metadata remains a warning/degraded prompt condition unless `constraints.require_executable` asks for a hard executable result.

## Runtime Preflight Plan

The remaining validation gap is a runtime-facing preflight API that can bind and plan a query request without loading stored payloads or materializing full results. This should be a specific preflight API, but it should use the same orchestration pattern as distributed BM25 global scoring: the coordinator gathers a consistent table/index/topology view once, asks groups for cheap capability or stats only when needed, and passes the aggregated context into validation.

This should stay separate from the pre-generation metadata/context pass. The query builder needs both stages:

- `collectQueryBuilderContext(...)` runs before LLM generation or deterministic specialist assembly. It fetches schema, index metadata, graph topology, and example documents, then packages that into the stable context shared by prompts and deterministic validation.
- `preflightQueryRequest(...)` runs after a concrete `query_request` exists. It binds the actual request against runtime/catalog state, returns diagnostics, and can later expose plan summaries or cheap estimates.

The key distinction is that context collection answers "what can the model safely generate?" while runtime preflight answers "will this assembled plan actually execute?".

The preflight API should return diagnostics rather than mutating the executable query request. Execution-only inputs such as BM25 global stats can continue to travel through the normal query path when they are required for scoring; preflight data should stay in a validation result.

Implementation tasks:

- [done] Split the query-builder flow into an explicit pre-generation context collection step and a post-generation preflight step.
- [done] Route HTTP and HTTPX query-builder entry points through a collected-context wrapper instead of attaching the metadata validator ad hoc.
- [done] Add a query-builder-facing `preflightQueryRequest(...)` result type with diagnostics and optional plan summary.
- [done] Keep the metadata-backed plan validator on top of the preflight path so generated-plan repair still receives deterministic feedback.
- [done] Expose runtime preflight through a public `storage/db` API boundary instead of calling `search_exec` directly from the API layer.
- [done] Thread `max_work` through the runtime preflight path so structured-filter exact counts can use a caller-controlled work budget instead of a fixed hidden limit.
- [done] Introduce a `storage/db` `PlanningStatsProvider` boundary and route `DB.preflightSearchRequest(...)` through `DB.collectPlanningStats(...)` so query-builder/runtime estimate consumers can depend on a planner-facing stats contract instead of index-specific helpers.
- [done] Pull the DB preflight binding validator cluster into a sibling `storage/db` planning module so `db.zig` stays closer to a facade over planning collection, validation, and execution helpers instead of owning all preflight logic inline.
- [done] Pull the DB-backed planning collector helper bodies into a sibling `storage/db` planning module so `db.zig` only keeps thin callback shims around the collector/provider seams instead of inlining raw index-stat and dense-cost logic.
- [done] Pull the planning collector adapter/wiring into a sibling `storage/db` planning module so `db.zig` no longer owns the collector vtable callback cluster and instead just passes `DBCore` plus the locked search callback into a planning adapter.
- [done] Add a generic `PlanningStatsProvider.init(...)` helper so the last provider object literal also moves out of `db.zig`, leaving only the DB-specific collect callback and public entrypoint there.
- [todo] Extend the DB-facing preflight beyond basic live binding. It now validates lane index existence, text-field bindings for runtime-native and serialized text filter queries, dense/vector dimensions, and graph query shape plus edge-type compatibility against live DB catalog state, checks every local group when a table spans multiple ranges, can preflight hosted remote groups through the internal group-read API, and exposes live index summaries, but it still needs richer cost/selectivity estimates.
- [todo] Extend preflight `estimate` mode beyond live index summaries with cheap runtime-backed cost/selectivity signals. It now carries DB-backed text/embedding/graph index estimates, shard fanout, dense-search work estimates, per-field text term doc-freq stats for main text queries plus filter/exclusion text clauses, non-text filter shape counts for doc-id/id, range, bool-field, and geo clauses, conservative positive-ID upper bounds plus normalized lower-bound/sampled/upper-bound selectivity ratios, and coarse cost bounds such as shard result windows, stored-projection upper bounds, rerank upper bounds, aggregation second-pass flags, and a heuristic latency tier, with builder-facing caps that tighten stored/rerank work when a positive-ID bound is known, explicit risk-factor summaries for both selectivity and latency, estimate-kind/confidence fields that make the current upper-bound/heuristic status explicit, a surfaced corpus-size estimate for selectivity, per-factor latency score components, and the normal query-builder response now threads available plan/estimate preflight summaries into `steps[].details.preflight`. Runtime preflight now derives the base estimate contract itself: text upper bounds, corpus-size estimate, normalized selectivity ratios, result document upper-bound/estimate, and estimated stored/rerank/aggregation work, so the builder consumes those runtime fields instead of recomputing them. For small local shards, runtime preflight also computes an exact structured-filter document count by running a bounded count-only search over purely structured constraints and uses that as a tighter result upper bound; multi-shard/remote merges preserve that count only when every merged shard produced an exact structured count. When that exact structured count is skipped because the shard exceeds the current runtime budget, runtime preflight now also runs a bounded structured-filter probe and surfaces a lower-bound document count plus the applied budget limit. It now also computes a DB-backed sampled structured-filter count estimate over a bounded primary-doc sample when the request is expressible through the stored-filter evaluator, and that sampled estimate is merged across shards and surfaced to the builder as a distinct `sampled` selectivity signal instead of being conflated with exact or upper-bound counts. The latency heuristic now preferentially scores estimated downstream work values instead of only the upper-bound path. The stored-filter evaluator used by that path now also handles more structured `filter_query_json` clauses directly (`bool_field`, `term_range`, `ip_range`, array-form `doc_id`, and the common geo filters `geo_distance`, `geo_bbox`, `geo_shape`), so runtime-backed selectivity is less dependent on top-level structured queries. The remaining gap is richer larger-shard structured selectivity and actual execution-latency prediction.

Suggested shape:

```zig
pub const QueryPreflightOptions = struct {
    mode: Mode = .validate,
    require_executable: bool = false,
    max_work: u32 = 0,

    pub const Mode = enum {
        validate,
        plan,
        estimate,
    };
};

pub const QueryPreflightDiagnostic = struct {
    severity: enum { warning, error },
    code: []const u8,
    path: []const u8,
    message: []const u8,
};

pub const QueryPreflightResult = struct {
    diagnostics: []const QueryPreflightDiagnostic,
    plan_summary: ?QueryPlanSummary = null,
};

pub fn preflightQueryRequest(
    alloc: std.mem.Allocator,
    table_name: []const u8,
    request: metadata_openapi.QueryRequest,
    opts: QueryPreflightOptions,
) !QueryPreflightResult;
```

Initial scope:

- Validate table existence and bind requested full-text, embedding, sparse, graph, and tree indexes.
- Validate fields against the index bindings used by `full_text_search`, filters, exclusions, sorts, projections, and graph node filters.
- Validate embedding index compatibility, including dense/sparse capability and generated embedding requirements.
- Validate graph searches against graph indexes, supported result refs, dependency ordering, cycles, and selector executability.
- Produce stable diagnostic codes and JSON paths that can be fed directly into query-builder repair prompts.

Modes:

- `validate`: shape, catalog, dependency, and capability checks. This is the query-builder requirement.
- `plan`: include a cheap execution-plan summary, such as bound indexes, graph dependency order, and expected result-set names.
- `estimate`: optional later mode for cheap counts, selectivity, or cost hints when the DB layer can provide them without running a full query.

If we ever want one shared internal engine, it can expose explicit stages such as `context`, `validate`, `plan`, and `estimate`, but the public/query-builder-facing entry points should remain separate so prompt-building does not accidentally depend on the heavier runtime validation path.

Longer-term planner shape:

- Per-index stats should stay with the index that owns them:
  - full-text: term doc-freq / BM25 stats
  - dense/sparse: active-count and search-work stats
  - graph: node/edge/type stats
  - future structured secondary indexes: field-specific selectivity stats
- `storage/db` should merge those into one planner-facing contract, starting with `PlanningStatsProvider` and `PlanningStatsSummary`.
- Query-builder, retrieval-agent planning, and future serverless/runtime planners should consume the merged planner contract rather than reaching into full-text or graph-specific helpers directly.
- Latency and selectivity decisions should live above raw stats collection. Indexes report facts; the planner turns those facts into selectivity estimates, cost features, and latency predictions.

## Remaining Follow-ups

- Add the runtime-facing `preflightQueryRequest` API and wire query-builder final validation to it.
- Add live executor-backed graph dry-runs or estimates on top of preflight once the DB layer exposes cheap execution-plan hooks.

## Implementation Notes

- The deterministic builder now returns both the compatibility `query` fragment and an assembled `query_request`.
- The HTTPX handler can pass a generation runner into the query builder. When a request includes `generator` and the mode is `auto` or `full_text`, the coordinator attempts the generated Bleve specialist first.
- The generated full-text specialist is intentionally narrow: it produces native Bleve JSON, validates that the response contains an object-valued `query`, and falls back to the deterministic builder on generation or parse failures.
- Generated Bleve output is now pre-validated before use. The validator recurses through known Bleve query forms, rejects unknown operators/shapes, and enforces explicit `field` values against `constraints.allowed_fields` when present or schema fields otherwise.
- Deterministic `semantic` and `hybrid` modes now use `constraints.prefer_indexes` when supplied, otherwise they infer dense embedding indexes from the requested table metadata. Sparse embedding, full-text, and graph indexes are intentionally ignored for this path.
- Metadata-backed semantic validation now uses structured embedding metadata when available and distinguishes sparse embedding indexes from missing indexes in validation feedback.
- Generated semantic and hybrid specialists now use the prefetched embedding metadata in their prompts, including dense/sparse compatibility, dimensions, and model hints. Preferred indexes are only prompt-eligible when metadata confirms they are dense; generated hybrid plans also validate their native Bleve `full_text_search` before use.
- Interactive `semantic` and `hybrid` requests now ask for `select_semantic_index` when no preferred or table embedding index is available, and a follow-up decision answer can supply the preferred index.
- Interactive full-text requests now ask for `select_text_field` when multiple text fields are plausible, and a follow-up decision answer or `constraints.prefer_field` can steer deterministic field selection.
- `mode: "auto"` now upgrades to deterministic hybrid or semantic assembly when dense indexes are available. Interactive auto requests ask `select_query_strategy` before committing to hybrid/full-text/semantic, and a follow-up decision or `constraints.prefer_strategy` drives the next build.
- Interactive requests with `constraints.require_executable` now ask `select_query_table` when the target table is missing but enough field context exists to draft the query. A follow-up decision or `constraints.table` fills `query_request.table`.
- Deterministic field selection now honors `constraints.allowed_fields`, including fallback builds after generator failure.
- `constraints.fields`, `constraints.order_by`, `constraints.offset`, `constraints.search_after`, `constraints.search_before`, `constraints.count`, and `constraints.profile` now map into the assembled `query_request`. Projection, count, and profile apply broadly; sort, offset, and cursor pagination are only attached when the assembled request has `full_text_search`.
- Deterministic full-text, semantic, and hybrid paths now convert explicit ISO-date phrases such as `after 2024-01-01`, `before 2025-01-01`, and `from 2024-01-01 to 2024-12-31` into a Bleve range `filter_query` when a likely date field is available.
- Deterministic filter assembly now converts explicit field constraints like `tenant_id:acme` and `type=article` into `term` filters, and simple status exclusions like `not archived` into `must_not` filters.
- Structured `constraints.filters`, `constraints.filter`, `constraints.exclude`, and `constraints.exclusions` now assemble exact term, multi-value OR, and range filters after schema/allowed-field checks. Raw `constraints.filter_query`, `constraints.exclusion_query`, and `constraints.filter_prefix` are also mapped into `query_request` when valid.
- `constraints.graph_searches` and `constraints.graphs` now parse into `query_request.graph_searches`, with `constraints.expand_strategy` passed through for graph/search result merging. Graph mode can also infer a graph search from `constraints.graph_index` or a single table graph index, seed it from the assembled lexical, semantic, or fused result set, extract simple `from`/`to` or `between` node keys plus common edge-type hints from the intent, build explicit two-hop or three-hop pattern queries, and use a configured generator for richer graph plans with deterministic validation and fallback.
- Generated graph plans are validated for executable shape before use, including known graph indexes, start/target selectors, supported result refs, schema-safe fields, supported graph params, pattern alias/edge consistency, and `$graph_results.<search_name>` dependency existence/cycle checks.
- `QueryBuilderTableContext.plan_validator` is the extension point for runtime validation feedback. It currently supports generated graph-search feedback, generated Bleve-query feedback, and final `query_request` / `retrieval_query_request` feedback hooks. Graph-search and Bleve-query feedback are both wired into one-pass generated repair loops before deterministic fallback.
- Final plan-validator feedback is appended as a warning by default and becomes `InvalidQueryBuilderRequest` when `constraints.require_executable` is true and no clarification is pending.
- The HTTP and HTTPX query-builder routes now attach a metadata-backed plan validator for table-scoped requests. It validates full-text index availability, semantic index names against table dense embedding indexes, graph index names, tree index names, projected fields, and sort fields before returning the plan.
- Table-scoped query-builder requests now prefetch index metadata in one table context. The context carries legacy flat index-name slices plus structured full-text, embedding, and graph metadata. Structured metadata is preferred when present, while legacy flat semantic and graph index slices remain a compatibility fallback. Graph generation prompts and metadata-backed validation now share graph edge-type metadata from that same context.
- Index metadata prefetch recognizes both generic `embeddings` index configs and explicit `dense_vector` / `sparse_vector` configs, so semantic validation can preserve dense/sparse compatibility across old and new index declarations.
- Generated full-text prompts now include structured full-text index field metadata when the table exposes it, and metadata-backed validation rejects generated Bleve fields or final assembled `query_request.full_text_search`, `query_request.filter_query`, `query_request.exclusion_query`, and `query_request.order_by` fields that are not covered by known full-text index field bindings. Unknown full-text bindings still degrade permissively.
- Metadata-backed tree validation now uses prefetched graph edge topology. A `tree_search` plan remains permissive when topology metadata is absent, but under known graph metadata it rejects indexes whose declared edge types have no tree-compatible topology.
- Generated graph planning now gets one validation-repair pass before deterministic fallback. If the first generated `graph_searches` response fails validation, the coordinator sends the same prompt plus concrete correction rules back to the generator and validates the repaired response. The plan validator can also provide executor/dry-run feedback for otherwise well-formed graph plans, and that feedback is included in the repair prompt.
- Final graph-search validation now runs a parser-backed executor-shape preflight through the query contract before returning the plan, so graph shapes that the query executor cannot parse are surfaced as validation feedback. This is still separate from a live DB dry-run.
- Generated-graph repair and final graph-search validation also check result-ref availability against the assembled query: `$full_text_results`, `$embeddings_results`, and `$fused_results` must correspond to result sets the `query_request` can actually produce, while `$graph_results.<search_name>` must point to another graph search in the same plan.
- Query-builder results now include optional `retrieval_query_request` for retrieval-pipeline-only artifacts. `constraints.tree_search` or shorthand `constraints.tree_index`/`constraints.tree_start_nodes`/`constraints.tree_max_depth`/`constraints.tree_beam_width` populate `retrieval_query_request.tree_search` while preserving the seed `query_request`.
- Tree mode now infers a single graph index from table metadata, extracts simple `from` start nodes from the intent, asks `select_tree_index` when multiple graph indexes are available, and accepts a follow-up decision answer for `retrieval_query_request.tree_search.index`.
- Endpoint-level tests now verify query-builder clarification replay for table, text field, semantic index, tree index, graph index, and strategy decisions through the HTTP handler.
- `constraints.require_executable` now rejects non-executable outputs when clarification is not pending. Today that covers missing table context, unsupported modes, and semantic/hybrid requests that cannot assemble semantic search with indexes.
- Query-builder implementation now lives in `pkg/antfly/src/api/query_builder_agent.zig`; `retrieval_agent.zig` keeps compatibility wrappers for older internal callers.
- The legacy `ApiHttpServer.handle` path still has no generation runner and therefore remains deterministic even if a request supplies `generator`.

## Compatibility Rules

- Do not remove `query`.
- `output: "bleve"` must preserve the old fragment-first behavior.
- `query_request` should be present whenever the coordinator can assemble an executable or nearly executable Antfly `QueryRequest`.
- Missing schema/index context should degrade into warnings, not hard failures, unless `constraints.require_executable` is true.
- The deterministic builder remains the fallback when no generator is configured or the LLM builder fails.
