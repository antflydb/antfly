# Sort Implementation Slices

This document tracks the concrete work remaining to make `SORT.md` production
complete. The goal is to keep the work sliced by shippable production
outcomes, not by implementation file.

## Current Baseline

The core sort shape is substantially implemented:

- public `order_by`, `_sort`, `search_after`, and `search_before` exist
- `_id` is the implicit deterministic tie-breaker
- mappings expose `sortable`; public schema rejects direct `doc_values`
- typed doc values back mapped scalar field sort
- stored JSON sort is debug/test-only and blocked for public production paths
- native doc-values top-N, `_id` seek, sorted segment seek, score top-k, and
  distributed k-way merge have explicit plan names
- `index_sort` metadata, sorted segment flush/merge, and sorted segment bounds
  exist
- public field capabilities expose `query_modes`, `sortable`, lifecycle state,
  and index-sort membership without public `doc_values` toggles
- OpenAPI/SDKs expose stable sort request, hit, error, and profile contracts

The remaining work is mostly production proof: performance gates, rollout
tooling, distributed end-to-end validation, concise observability, and user
documentation.

## Slice 1: Contract Cleanup And Generated Artifacts

Goal: keep the public contract small, stable, and generated from one source.

Status: in progress in the current worktree.

Remaining:

- Land the shared `sort.yaml` ownership for `SortField` and `SortDirection`.
- Land closed public `SortProfile` and `ExactSortError` schemas.
- Keep SDKs regenerated from OpenAPI.
- Keep sort out of quickstart; document it in focused reference/guide docs.

Done when:

- `make generate` produces no unstaged generated drift after commit.
- SDK tests cover the closed public `SortProfile` shape.
- `order_by`, `_sort`, `search_after`, and `search_before` docs remain aligned
  across OpenAPI and SDKs.

## Slice 2: Production Benchmark Gates

Goal: prove exact sort paths scale with the intended variables.

Remaining:

- Add a broad match-all benchmark where `order_by` matches `index_sort` and
  first-page latency scales with `limit`, not corpus size.
- Add a doc-values top-N benchmark proving source-free selection and no stored
  JSON loads before page selection.
- Add a distributed merge benchmark proving coordinator work scales with shard
  count and page/shard window, not global hit count.
- Gate regressions with stable thresholds or a comparison baseline appropriate
  for CI/release qualification.

Done when:

- Release qualification fails on stored-source fallback, unbounded candidate
  collection, or distributed merge scaling regressions.
- Bench output includes sort plan, exactness, source, candidate count,
  selected count, and source-load behavior.

## Slice 3: Backfill, Reindex, And Rolling Upgrade Operations

Goal: make sortable/index-sort capability changes operationally safe.

Remaining:

- Define the operator workflow for declaring a new sortable field or changing
  `index_sort`.
- Provide backfill, compaction, or reindex tooling that moves fields from
  `declared`/`indexed` to `covered`/`queryable`/`accelerated`.
- Ensure mixed generations reject exact sorts with clear 422s until coverage is
  complete.
- Surface the lifecycle state through table status/field capabilities and
  release manifests.

Done when:

- An integration test creates mixed old/new segments, verifies rejection, runs
  the repair/reindex path, then verifies exact sort acceptance.
- Changing `index_sort` requires a new generation or reindex path rather than
  silently mutating existing physical layout.

## Slice 4: Native Filter And Sort Costing

Goal: choose efficient exact sort plans without broad stored-source scans.

Remaining:

- Broaden native structured filter coverage where gaps remain, especially
  terms/range/doc-set forms that still rely on unresolved stored JSON.
- Cost native filter selectivity against `index_sort` scan selectivity.
- Prefer sorted segment membership scans for broad filters with matching
  `index_sort`.
- Prefer candidate-first doc-values top-N for selective filters or non-matching
  sort orders.
- Keep approximate vector candidates out of exact field-sort semantics unless
  the eligible set is proven exact.

Done when:

- Planner tests cover broad and selective native filters choosing different
  exact plans.
- Unsupported filter/sort combinations fail closed with stable reasons instead
  of falling back to stored JSON.

## Slice 5: Index-Sort Acceleration Hardening

Goal: make physical `index_sort` a dependable acceleration path.

Remaining:

- Prove sorted segment seek across many segments with cursor bounds.
- Prove deletes, upserts, TTL, and identity generation checks under sorted
  segment seek.
- Prove compaction preserves index-sort metadata, bounds, and physical order.
- Prove mismatched or missing index-sort coverage falls back only to exact
  doc-values collection or rejects clearly.

Done when:

- Integration tests cover match-all, structured filter, and full-text membership
  queries using sorted segment seek.
- Benchmarks show broad matching `index_sort` queries avoid full candidate
  materialization.

## Slice 6: Distributed End-To-End Sorted Search

Goal: prove the coordinator/shard behavior, not only the merge primitive.

Remaining:

- Exercise real shard-local sorted execution feeding coordinator k-way merge.
- Forward `search_after` and `search_before` to every shard.
- Enforce incomplete shard-window rejection in the real distributed query path.
- Return exact or `gte` total-hit relation from shard capabilities.
- Validate stable `_id` tie-break semantics across shards.

Done when:

- Distributed integration tests cover field sort, `_id` cursor-only pagination,
  score sort, invalid cursors, incomplete windows, and mixed scalar domains.
- The coordinator never accepts unsorted or incomplete shard windows for exact
  public field sort.

## Slice 7: User Documentation

Goal: give users a focused sort guide without bloating quickstart.

Remaining:

- Add a focused guide for filtering, sorting, and cursor pagination.
- Show sortable mapping examples for keyword, number, date, boolean, and `_id`.
- Show `title.keyword` style exact string sorting rather than analyzed `text`.
- Explain `search_after` and `search_before` using returned `_sort` tuples.
- Explain common 422 reasons and operational fixes.
- Document `index_sort` as optional acceleration with one physical order per
  index generation.

Done when:

- Public examples compile or round-trip through existing parsers/tests.
- Quickstart stays focused on basic query flow and does not carry sort-specific
  detail.

## Slice 8: Concise Metrics And Alerts

Goal: make sort failures diagnosable through existing query observability.

Remaining:

- Verify query metrics include sort plan, exactness, source, candidate source,
  selection reason, rejection reason, and budget rejection reason.
- Add or update alert rules for abnormal exact-sort rejection rates, budget
  failures, and native coverage failures after rollout.
- Keep low-level executor counters in sampled logs/profiles/debug telemetry,
  not in the normal SDK-facing response.

Done when:

- Operators can answer why a sorted query was slow or rejected from existing
  metrics and logs.
- No separate sort dashboard is required.

## Slice 9: Stored JSON Public Path Removal Audit

Goal: ensure stored JSON sort cannot re-enter public exact sort execution.

Remaining:

- Audit all public query entry points for stored-source sort fallbacks.
- Keep `stored_json_debug` only behind explicit test/debug runtime checks.
- Add regression tests for every public route that could request field sort.

Done when:

- Public exact `order_by` either uses native exact execution or returns a stable
  rejection.
- Stored-source loading happens only after page selection unless `_source` is
  explicitly requested.
