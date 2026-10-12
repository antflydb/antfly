# Algebraic Prototype Milestones And Capability Log

> Relocated verbatim from `zig/ALGEBRAIC.md` (lines 1283–1311, 1332–1349, and 1364–1481 at commit 3ce5aff4ea) on 2026-10-12. Durable decisions were folded into [ALGEBRAIC.md](../../../../zig/ALGEBRAIC.md) before the move; its "Current Implementation" section now carries a grouped capability summary. Excerpts are separated by horizontal rules.

### Prototype Bootstrap

The bootstrap prototype used the following narrow milestone to prove the idea
without committing the full database to the new execution model:

```text
1. Create an algebraic index catalog entry.
2. Tokenize string, number, and datetime JSON fields.
3. Maintain COUNT by one string field.
4. Maintain SUM and AVG by one string group field and one numeric measure.
5. Maintain daily SUM temporal expression buckets over one datetime field.
6. Route matching aggregation requests to the algebraic index.
7. Fall back to existing aggregation code for everything else.
8. Add reopen/replay tests through the derived log.
```

The demonstration query set should be:

```text
GROUP BY country COUNT(*)
GROUP BY customer SUM(amount)
GROUP BY customer AVG(amount)
GROUP BY customer, day(created_at) SUM(amount)
```

The current implementation has moved past this bootstrap target. Completed
capabilities are tracked in Implementation State, durable implementation rules
live in the Implementation Guide, and the Roadmap now records boundary
conditions rather than an active milestone list.

---

### Success Criteria

The prototype is successful if it can show:

```text
correct results across insert, overwrite, delete, reopen, and replay
faster common group/time aggregations than scanning hit stored_data
clear fallback behavior for unsupported query shapes
bounded support-state behavior for MIN/MAX
simple extension path toward more algebraic operators
```

The prototype has moved beyond that first milestone: it now has an explicit
typed algebraic IR, materialized-expression IDs, tensor-program envelopes, and
proof-gated access paths. Additional bounded query shapes should extend those
artifacts only when they preserve exact semantics and have benchmark evidence;
unsupported text, join, vector, graph, and schemaless shapes stay on explicit
fallback paths.

---

Implemented capability includes:

```text
internal algebraic index kind and derived replay target
canonical tuple/scalar token encoding with symbol-id backed keys
typed value canonicalization module for string/integer/number/bool/time/bytes
algebraic law registry for group, monoid, lattice, and semiring descriptors
explicit document fact-list projection and persisted docfact sidecar rows
sparse tensor row/key module for law-backed coordinate slots
tensor-only materialized-expression row decoding for new algebraic state
direct COUNT/SUM/AVG/MIN/MAX materializations
exact MIN/MAX support rows for duplicate values and delete repair
hour/day/month temporal cylinders
configured composite equi-join aggregate maintenance
explicit join document role and side gating
join-side fact projection to only fields required by configured materializations
LSM-backed bulk join ingest with cursor-capable write-batch matching
bulk maintenance-plan caching for ready adaptive materializations during overwrite/delete bulk ingest
resource-manager accounting for algebraic tensor accumulator working memory through `algebraic.tensor_accumulators`
temporal join bucket/window matching with range-pruned fact scans
overwrite/delete compensation from persisted document and join fact rows
root metric, terms, date_histogram, constrained rollup, and nested metric routing
internal algebraic query IR for metric, bucket, constraint, and configured join shapes
bucket child metric planning through the algebraic query IR
strict configured join materialization matching unless the query names the materialization directly
opt-in implicit join materialization matching when a configured join fold is declared safe for normal query planning
bucket-limit, row-scan budget, and health checks before planner selection
planner-visible schema lifecycle gating for stale/rebuild-required algebraic configs
planner estimates for recent scan rows and result buckets in algebraic status
planner last-decision, fallback, lifecycle readiness, lifecycle blocking, dictionary registry ownership, and vector symbolic-filter routing diagnostics in index status and benchmark JSONL events
semantic dictionary registry ownership that prevents duplicate FST/postings publication for equivalent analyzed-term, canonical-scalar, sparse-token, and graph-label identities
persisted query-shape observation and materialization recommendation scaffolding
scannable persisted materialization-state rows and DB status counters for adaptive recommendations
DB-open and add-index hydration of persisted adaptive observation status
internal DB-level listing of persisted algebraic materialization recommendation states for status assembly
internal DB-level listing of persisted adaptive query observations for status assembly
engine-owned adaptive lifecycle transitions for persisted algebraic materialization recommendation states
validated adaptive lifecycle transitions for materialization recommendation states, with no manual user-facing lifecycle API
deterministic adaptive materialization ids from canonical recommendation hashes
policy-gated adaptive candidate evaluation with persisted ranking and progress rows
persisted adaptive candidate decision history and policy-drift status/benchmark counters
bounded adaptive tensor backfill from persisted docfact rows with ready promotion
planner routing for ready adaptive terms/date histogram tensors with nested metric tensors
cost/savings-aware adaptive dematerialization recommendations with safe tensor cleanup
adaptive lifecycle primitives for observing/recommended/backfilling/ready/stale materializations
adaptive lifecycle recovery tests for interrupted backfill, schema drift, and dictionary-owner conflicts
schemaless path profiles with adaptive typed path promotion, dictionary-backed lookup rows, readiness gating, and mixed-kind/coercion fallback policy
explicit planner fallback and rejection paths for unsupported ranking, non-proven traversal, arbitrary joins, unsupported constraints, and mixed-kind/coercion cases
explicit semiring product hooks for provenance-backed sparse vector contractions
law-compatible tensor row merge primitive for broader tensor execution
set-union lattice law coverage through tensor row mutation/merge and distributed partial merge
planner metric folds routed through the algebraic law registry
proof-gated group, lattice, and provenance-semiring laws for aggregate, MIN/MAX support-row, distributed partial, and graph/path traversal capabilities
fact-only append-only bulk fast path for bounded sidecar fact ingestion
benchmark counters and quick/standard/large profiles for disk usage, query time, write cost, join scans, and flushes
benchmark reporting for planner estimates, observed query shapes, docfact rows, and recommendation candidates
benchmark correctness classification for every algebraic-vs-baseline query comparison, with guardrails for missing classifications
benchmark summary comparisons for adaptive materialized vs static vs fallback algebraic disk, query, and update paths
adaptive coverage benchmark mode for root metrics, terms metrics, date histograms, constrained folds, configured joins, warmup cost, and churn
storage-focused algebraic benchmark root so adaptive coverage smoke builds without full API/server dependencies
bounded LSM analytics smoke mode for durable constrained/adaptive coverage checks
LSM analytics benchmark mode for durable sidecar scale, cardinality, write amplification, churn, fanout, constrained, and adaptive coverage cases
churn row-family benchmark counters for materialized_expr, docfact, pathfact, path_lookup, path_profile, joinfact, docjf, minmax, and sym sidecar rows
public query guardrail query shapes for dense, hybrid, hybrid-filter, hybrid-filter-exclude, and projected hybrid filter vector searches
public query guardrail schema and algebraic toggles so default, schema-only, and schema-plus-algebraic runs are comparable
public query guardrail JSONL summary events for no-schema, schema-only, and schema-plus-algebraic vector-pruning comparisons, including a no-listener handler mode for reproducible archived runs and local/standalone modes for transport/server overhead evidence
benchmark-wide performance evidence summary for dataset, query, correctness, cold/warm, fanout, constrained, wide-key, stats, cardinality, range, histogram, churn, churn row-family, public-query, LSM, symbol/support byte growth, accumulator flush, adaptive bulk-maintenance counters, and public-query RSS coverage
mixed-role benchmark root-cardinality comparisons constrain algebraic sidecar reads by primary document role so derived customer/profile facts do not broaden doc-scan/full-text order-only baselines
db_bench summary performance guardrail thresholds for coverage counts, cold/warm reads, fanout, constrained queries, wide-key composite queries, stats/cardinality/range/histogram queries, correctness failures, query latency, byte cost, symbol/support bytes, accumulator flushes, LSM flush/write-pressure compaction counts, public-query RSS, and churn cost
db_bench summary baseline-file comparison ratios for stable local performance guardrails
`storage_bench summary` compares DB benchmark results, with checked-in coverage and baseline regression fixtures exercised in CI
`scripts/run_db_query_matrix.py --suite analytics` runs the DB analytics and public schema comparisons
`antfly-storage-db-test` includes planner ownership; `antfly-unit-test` retains dynamic-template safety coverage
LSM bulk-session finish direct-ingests the final mutable state as a sorted run when direct bulk ingest is enabled and no immutable flush is pending, so algebraic bulk sidecars avoid a final normal flush and archived runs can guard `total_lsm_sorted_ingest_runs`
algebraic bulk-ingest sessions defer promoted path dictionary FST rebuilds across all flushed coalescer batches and rebuild each dirty promoted dictionary once at DB bulk-session finish, before the primary store publishes the final sorted run
durable planner default policy remains opt-in and conservative until LSM guardrail evidence covers latency, bytes, write cost, churn, cold reads, fanout, and constrained queries
schema capability fingerprints, skipped-unbounded-field metadata, and debug lifecycle classification
schema-derived v2 configs with declared laws and adaptive defaults
runtime-adaptive dynamic templates: bounded table-level dynamic templates (keyword/numeric/boolean/datetime) compile into capability `dynamic_field_rules` and project template-matched fields into typed docfacts at ingest; accepted template changes advance the schema generation, refresh both durable and live configs (`Index.reloadConfigJson`), and gate query acceleration until a matching generation-local completion marker proves rebuild coverage; unbounded text templates stay on the schemaless path-fact path (cardinality guard)
public algebraic index requests constrained to schema-derived capability sidecars with internal materialization fields stripped/rejected
canonical-token shard merge keys for distributed symbol semantics
distributed partial merge helpers that combine shard results by canonical axis and law
distributed partial validation tests for malformed canonical law values
distributed partial merge tests for count, sum, sumsquares, avg-state, min, and max law families on shared canonical bucket axes
distributed partial merge tests for set-union lattice payloads on shared canonical axes
validated distributed partial-envelope merge tests for different shard-local symbol ids resolving to the same canonical axis
distributed partial aggregation tests for independent shard dictionaries merging the same canonical terms/stat/join axes
unit coverage for planner-routed stats, nested metric, range/histogram, cardinality, pathfact predicate, multi-field composite terms, partial distributed tensor programs, derived joins, and canonical distributed law merges
distributed partial tensor-program proof/export counters in algebraic status and benchmark summaries
distributed partial tensor-program proof/export counters are summarized through the compact public algebraic index-status contract
adaptive coverage summaries expose rebuild-required, stale, cleanup-recommended, decision-history, and policy-drift counters
LSM reopen coverage for adaptive dematerialization cleanup proving tensor rows, progress rows, and candidate rows stay removed while materialization state remains stale
compact public algebraic index status exposed as a single `AlgebraicIndexStats` envelope without public `AlgebraicRuntimeHealth` or `AlgebraicAdaptiveProgressStatus` schemas in the normal index-status response
public OpenAPI keeps detailed algebraic runtime health, adaptive progress, adaptive candidate, and candidate-decision records out of the stable status schema; those records remain internal state or diagnostics/benchmark evidence
internal algebraic partial request protocol accepts only tensor expression/program envelopes, rejecting named materialization requests and cardinality-only bodies
public table/index parser support for `type: "algebraic"` only through `derive_from_schema: true`
native dense-vector pruning for safe symbolic doc-id constraints by translating doc ids to HBC vector ids
native sparse-vector pruning for safe symbolic doc-id constraints by filtering doc nums during sparse score accumulation
native vector pruning for structured stored filters by resolving filter doc ids through the text index before dense/sparse ranking
required symbolic vector filters fail closed when algebraic lifecycle state is stale
internal shard-query wire fields for native doc-id include/exclude candidate sets so resolved algebraic constraints survive distributed fanout
proof-gated dense/sparse vector and graph constraint input tensors, including `native_doc_id_constraints` and `graph_target_constraints`, with fail-closed validation of required envelopes
first-class tensor access-path helpers for docfact, pathfact, lexical, sparse-token postings, dense/sparse vector search, and graph layouts
planner-owned dense/sparse vector and graph tensor-program construction, with catalog code limited to access-path discovery
planner-owned docfact, pathfact, and derived-join range/histogram tensor-program construction for distributed partial fanout
planner-owned configured-materialization tensor-program construction for distributed partial fanout, replacing API-local tensor expression derivation
distributed configured-aggregate partial fanout encoded as planner-owned tensor program/access-path envelopes instead of named materialization requests
root cardinality distributed partials can be requested through planner-owned tensor program envelopes while preserving exact distinct-value merge keys
terms-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical bucket and distinct-value merge keys
range-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical range and distinct-value merge keys
histogram-with-nested-cardinality distributed partials can be requested through planner-owned tensor program envelopes with canonical bucket and distinct-value merge keys
production distributed cardinality, terms-cardinality, range-cardinality, and histogram-cardinality aggregation paths fall back to the normal aggregation planner when a tensor program cannot be proven instead of using bespoke partial request bodies
production distributed aggregation request planning is centralized through planner-owned tensor-program selection before provisioned or hosted shard fanout
distributed graph expand/get-edges requests carry tensor program envelopes plus access-path proofs instead of id-only program assertions
distributed graph validation derives expected graph tensor programs from planner-owned envelopes without exposing id-only helper APIs
graph index status exposes bounded algebraic semiring traversal attempts, proofs, rejects, fallbacks, and result-node counts
graph traversal benchmark smoke and summary events expose proof/reject/fallback counters, result-node counts, path bytes, and query latency
```
