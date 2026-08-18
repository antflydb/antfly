# PR #145 Restructure Plan

This document turns the work currently accumulated in `combine-pr-141-143-144`
into a set of reviewable pull requests based on current `origin/main`.

The combined branch remains the integration oracle. New branches must not be
created from it: each root PR starts from `origin/main`, and dependent PRs start
from the tip of their declared parent. Final generated files must be rebuilt
from the specs present in each branch rather than copied wholesale from the
combined branch.

## Goals

- Preserve the final behavior and test coverage of the combined branch.
- Make each PR independently buildable and reviewable.
- Separate mechanical file ownership changes from feature changes.
- Keep generated OpenAPI, SDK, and SQL grammar output attributable to the PR
  that changes its source contract.
- Land independent foundations in parallel, then stack feature work only where
  a real compile-time or semantic dependency exists.

## Ground rules

1. All worktrees live under `./.worktrees/`.
2. Root branches start at current `origin/main`.
3. A child branch starts at its parent branch, not at the combined branch.
4. The combined branch is read-only source material and a behavioral oracle.
5. Do not mix semantic changes into mechanical extraction PRs.
6. Do not copy final generated API clients into an earlier slice. Edit that
   slice's source spec and regenerate.
7. Every PR must pass `git diff --check` plus the focused gates listed below.
8. Before publication, rebase each root on the then-current `origin/main` and
   restack its children.

## Implementation status (2026-08-18)

The following branches are implemented in dedicated, clean worktrees. Root
branches use current `origin/main`; stacked branches use the parent shown in the
dependency graph:

| Status | Branch | Worktree | Focused verification |
| --- | --- | --- | --- |
| Ready | `feature/sql-parser` | `.worktrees/feature-sql-parser` | yacc + deterministic grammar generation + 12 parser tests + 229-test root gate |
| Ready | `perf/algebraic-cardinality` | `.worktrees/perf-algebraic-cardinality` | 3 algebraic execution tests |
| Ready | `perf/mem-ordered-snapshots` | `.worktrees/perf-mem-ordered-snapshots` | 3 snapshot/randomized/concurrency tests |
| Ready | `feature/algebraic-dynamic-templates` | `.worktrees/feature-algebraic-dynamic-templates` | 14 focused projection tests |
| Ready | `feature/objectstore-checksums` | `.worktrees/feature-objectstore-checksums` | provider and metadata checksum tests |
| Ready | `feature/relational-core` | `.worktrees/feature-relational-core` | 3 catalog tests; locked OpenAPI check; 5 projection/codec tests |
| Ready | `feature/graph-metrics-core` | `.worktrees/feature-graph-metrics-core` | 4 PageRank tests; 229-test root gate |
| Scaffolded | `restructure/build-modules` | `.worktrees/restructure-build-modules` | no changes yet |
| Scaffolded | `feature/relational-base-rows` | `.worktrees/feature-relational-base-rows` | inherits R1; no unique changes yet |

“Ready” here means locally committed and focused-test green. It does not mean
published: no branch is pushed and no replacement PR is opened by this plan.

## Branch and dependency graph

```text
origin/main
  |
  +-- restructure/build-modules
  |     |
  |     +-- restructure/storage-db-modules
  |             |
  |             +-- restructure/api-table-modules
  |                     |
  |                     +-- feature/catalog-core
  |                     |     |
  |                     |     +-- feature/catalog-routes
  |                     |           |
  |                     |           +-- feature/tablespaces
  |
  +-- feature/sql-parser
  +-- perf/algebraic-cardinality
  +-- perf/mem-ordered-snapshots
  +-- feature/algebraic-dynamic-templates
  +-- feature/graph-metrics-core
  |     |
  |     +-- feature/graph-metrics-runtime-api
  |           |
  |           +-- feature/graph-hits-rerank
  +-- feature/relational-core
  |     |
  |     +-- feature/relational-base-rows
  |           |
  |           +-- feature/relational-indexes
  |           |     |
  |           |     +-- feature/relational-foreign-keys
  |           |
  |           +-- feature/rows-read-api
  |                 |
  |                 +-- feature/rows-write-api
  |                       |
  |                       +-- feature/sql-lowering-runtime
  +-- feature/objectstore-checksums
        |
        +-- feature/row-source-contract
              |
              +-- feature/lake-parquet
                    |
                    +-- feature/lake-iceberg
                          |
                          +-- feature/lake-sidecars-operations
```

`feature/sql-lowering-runtime` is a merge stack: it requires
`feature/sql-parser`, `feature/rows-write-api`, and
`feature/catalog-routes`. Build it by merging or rebasing those prerequisite
tips into a temporary integration base, not by importing unrelated combined
branch history.

Lake public routing similarly requires the row API and catalog stacks. The
low-level row-source, Parquet, and Iceberg branches should remain free of HTTP,
SQL, and generated-client changes until those prerequisites are available.

## PR sequence

### M1: Split Zig build ownership

- Branch: `restructure/build-modules`
- Worktree: `.worktrees/restructure-build-modules`
- Base: `origin/main`
- Scope:
  - Extract cohesive helpers from `zig/build.zig` into
    `zig/pkg/antfly/build/`.
  - Preserve step names, filters, imports, and build options.
  - Add no relational, graph, SQL, catalog, or lake targets that do not already
    exist on `main`.
- Exclude: feature-specific new build steps.
- Gates: `zig build`, representative existing unit-test targets, and an audit
  of build-step names before and after.

### M2: Split storage DB ownership

- Branch: `restructure/storage-db-modules`
- Worktree: `.worktrees/restructure-storage-db-modules`
- Parent: `restructure/build-modules`
- Scope:
  - Mechanical extraction from `storage/db/db.zig` into existing-runtime
    owners such as lifecycle, write path, transactions, search runtime,
    derived async, HA, and test support.
  - Preserve public facade names and behavior.
- Exclude: relational base rows, graph metric maintenance, and new feature
  semantics.
- Gates: `zig build db-test`, DB lifecycle tests, and merge-audit declaration
  coverage.

### M3: Split API table ownership

- Branch: `restructure/api-table-modules`
- Worktree: `.worktrees/restructure-api-table-modules`
- Parent: `restructure/storage-db-modules`
- Scope:
  - Mechanical extraction of table reads and writes into
    `api/table_reads/` and `api/table_writes/`.
  - Keep existing HTTP behavior and wire shapes unchanged.
- Exclude: row APIs, qualified catalog routes, graph metric endpoints, and lake
  reads.
- Gates: existing API, table-read, and table-write suites.

### S1: SQL parser foundation

- Branch: `feature/sql-parser`
- Worktree: `.worktrees/feature-sql-parser`
- Base: `origin/main`
- Scope:
  - Generic yacc/bison grammar parsing, SLR table construction, diagnostics,
    deterministic Zig emission, CLI, and standalone tests.
  - The Antfly `.y` source, grammar provenance, generated parse tables, and
    conflict-count drift checks.
  - Source-spanned SQL tokens, keywords, lexer, cursor/parser helpers, semantic
    parser facade, syntax-owned AST nodes, diagnostics, fuzzing, and benchmark.
- Exclude: SQL binding/lowering, storage/runtime types, HTTP, pgwire, row APIs,
  and catalog execution.
- Gates: `yacc-test`, `sql-grammar-generated-check`, `sql-parser-test`, parser
  fuzz/benchmark smoke once the semantic facade lands, and root tests.

The commits remain separated internally so generated output is attributable,
but this branch is reviewed and published as one PR. The older
`tooling/zig-yacc`, `feature/sql-grammar-tables`, and
`feature/sql-semantic-parser` worktrees are extraction/development checkpoints
only and are not publication branches.

### A1: Exact algebraic cardinality performance

- Branch: `perf/algebraic-cardinality`
- Worktree: `.worktrees/perf-algebraic-cardinality`
- Base: `origin/main`
- Historical anchors from PR #141:
  - `f3ee99b`, `17ffc72`, `195a69e`, `912fa04`, `e8ca68b`
- Scope: hash-set constraint membership, single-pass child cardinality, and
  allocation reductions whose results remain byte-identical.
- Exclude: HLL, adaptive materialization, memory backend replacement, and
  relational behavior.
- Gates: algebraic unit tests and benchmark checksum comparison.

### A2: Persistent memory-store snapshots

- Branch: `perf/mem-ordered-snapshots`
- Worktree: `.worktrees/perf-mem-ordered-snapshots`
- Base: `origin/main`
- Historical anchors: `c407363`, `34293b0`, `dd06562`.
- Scope: O(1) read snapshots and persistent ordered-tree writes.
- Exclude: HLL and relational features.
- Gates: storage tests, randomized reference-map test, and concurrency test.

### A3: Dynamic-template algebraic projection

- Branch: `feature/algebraic-dynamic-templates`
- Worktree: `.worktrees/feature-algebraic-dynamic-templates`
- Base: `origin/main` or M3 if current file ownership makes that unavoidable.
- Historical source: PR #144 (`538ba13` through `89fe8b6`).
- Scope: bounded dynamic rules, ingest/query symmetry, config reload,
  backfill-pending gate, and tests.
- Exclude: relational storage and HLL.
- Gates: algebraic tests and API schema-update logic tests.

### O1: Object-store checksum provenance

- Branch: `feature/objectstore-checksums`
- Worktree: `.worktrees/feature-objectstore-checksums`
- Base: `origin/main`
- Historical anchors: `c81b318`, `b7f0f21`, `6dd45bf`.
- Scope: checksum algorithm/value/scope metadata and provider propagation.
- Exclude: lake manifests and scanners.
- Gates: object-store tests for memory, filesystem, S3, and GCS adapters.

### R1: Relational schema and row-codec core

- Branch: `feature/relational-core`
- Worktree: `.worktrees/feature-relational-core`
- Base: `origin/main`
- Historical anchors: PR #143 commits `0fb025e`, `f4345d7`, and `2d66400`.
- Scope: relational storage mode, closed schemas, JSON column type, typed
  column catalog/capability, and only the generated schema artifacts implied by
  those changes; document-to-typed-cell projection; required/type validation;
  order-preserving scalar encoding; and the versioned packed row codec.
- Exclude: DB keyspace changes, read-path materialization, public row APIs, and
  indexes, SQL, and constraints.
- Gates: schema tests, generated/OpenAPI checks, and focused relational
  projection and codec round-trip/corruption tests.

The schema and codec commits remain distinct internally for attribution, but
they are reviewed and published as one PR. The later base-keyspace work still
stays separate because its historical commit assumes older DB scan and segment
reconstruction implementations.

### R2: Authoritative relational base rows

- Branch: `feature/relational-base-rows`
- Worktree: `.worktrees/feature-relational-base-rows`
- Parent: R1
- Scope: dedicated base-row keyspace, typed row codec, scans, transactions,
  replay, split/merge behavior, and derived-index hydration from base rows.
- Exclude: public row APIs, SQL, and foreign keys.
- Gates: DB tests, split/merge tests, replay tests, and relational storage
  benchmarks as non-blocking evidence.

### R3: Relational index and constraint lifecycle

- Branch: `feature/relational-indexes`
- Worktree: `.worktrees/feature-relational-indexes`
- Parent: R2
- Scope: primary/unique/covering indexes, generation records, rebuild/repair,
  and schema mutation lifecycle.
- Exclude: distributed foreign-key execution.
- Gates: relational index tests and maintenance benchmark smoke.

### R4: Distributed foreign keys

- Branch: `feature/relational-foreign-keys`
- Worktree: `.worktrees/feature-relational-foreign-keys`
- Parent: R3
- Scope: FK metadata, validation, distributed action jobs, paging, timing, and
  diagnostics.
- Gates: focused FK tests, metadata job tests, and split/merge coverage.

### R5: Typed row read API

- Branch: `feature/rows-read-api`
- Worktree: `.worktrees/feature-rows-read-api`
- Parent: R2
- Scope: row execution contract plus get/query/aggregate/window/join/lateral
  read plans and endpoints.
- Exclude: writes, conflict actions, SQL, and lake routing.
- Gates: `zig build api-rows-test`, OpenAPI checks, and regenerated SDK tests.

### R6: Typed row write API

- Branch: `feature/rows-write-api`
- Worktree: `.worktrees/feature-rows-write-api`
- Parent: R5, optionally merged with R4 when FK enforcement is required.
- Scope: row batch/mutation plans, conflict targets/actions, returning, row
  claims, and temporal mutation contracts.
- Gates: `zig build api-rows-test`, mutation/integrity tests, OpenAPI checks.

### C1: Catalog identity and routing

- Branch: `feature/catalog-core`
- Worktree: `.worktrees/feature-catalog-core`
- Parent: M3
- Scope: durable database/namespace/table identity, `default.public`
  compatibility, typed targets, routing, snapshots, and qualified auth
  resources.
- Exclude: new public routes, tablespaces, SQL session behavior, and lake source
  bindings.
- Gates: catalog and auth tests.

### C2: Explicit catalog routes

- Branch: `feature/catalog-routes`
- Worktree: `.worktrees/feature-catalog-routes`
- Parent: C1
- Scope: database/namespace/table lifecycle and I/O routes, CLI/MCP/A2A
  normalization, OpenAPI, and regenerated clients.
- Exclude: tablespaces and SQL parsing.
- Gates: route parity, generated checks, SDK tests, MCP/A2A tests.

### C3: Tablespaces

- Branch: `feature/tablespaces`
- Worktree: `.worktrees/feature-tablespaces`
- Parent: C2
- Scope: tablespace metadata/lifecycle, bindings, rename/dependency behavior,
  placement validation, SQL plan types only after SQL prerequisites are merged.
- Gates: catalog, auth, route, and generated-client tests.

### S2: SQL binder, lowering, and ingress

- Branch: `feature/sql-lowering-runtime`
- Worktree: `.worktrees/feature-sql-lowering-runtime`
- Prerequisites: S1, R6, and C2.
- Scope: binder, logical plans, DDL/DML/read lowering, durable execution,
  sessions, HTTP SQL endpoint, pgwire, Lite bridge, CLI, and parity corpus.
- Keep graph SQL row sources and lake source routing as later additions.
- Gates: focused SQL planner tests, SQL API parity, pgwire tests, Lite SQL tests,
  and `relational-release-gate`.

### G1: Graph metric core

- Branch: `feature/graph-metrics-core`
- Worktree: `.worktrees/feature-graph-metrics-core`
- Base: `origin/main`
- Scope: a storage-independent weighted PageRank kernel over dense node
  ordinals, including dangling-mass redistribution, convergence reporting,
  validation, and deterministic unit tests.
- Exclude: public API, distributed process harness, HITS, SQL, and relational
  graph sources; also exclude persisted generations and maintenance jobs.
- Gates: focused PageRank tests, root compile/test gate, and small deterministic
  score fixtures.

### G2: Durable graph metric runtime and API

- Branch: `feature/graph-metrics-runtime-api`
- Worktree: `.worktrees/feature-graph-metrics-runtime-api`
- Parent: G1
- Scope: metric schema/config, generation-scoped persisted scores, build
  jobs/pages/leases, coordinator/worker lifecycle, maintenance command,
  action/status API, OpenAPI, and generated clients.
- Gates: `graph-metric-unit-test`, `graph-metric-integration-test`, generated
  checks, and process-harness smoke.

### G3: HITS and graph reranking

- Branch: `feature/graph-hits-rerank`
- Worktree: `.worktrees/feature-graph-hits-rerank`
- Parent: G2
- Scope: HITS authority/hub pair, traversal and reranking, hosted fan-in,
  freshness/profile output, and query API integration.
- Exclude: SQL graph table-function sources until S2 lands.
- Gates: graph metric integration, query fan-out, hosted rejection, and profile
  tests.

### L1: Row-source contract

- Branch: `feature/row-source-contract`
- Worktree: `.worktrees/feature-row-source-contract`
- Parent: O1; merge R5's row execution types when required.
- Scope: generic row-source types plus local/external adapters and inventory
  boundary.
- Exclude: Parquet decoding and public lake routing.
- Gates: local/external adapter tests.

### L2: Parquet object scans

- Branch: `feature/lake-parquet`
- Worktree: `.worktrees/feature-lake-parquet`
- Parent: L1
- Scope: object snapshots/ranges, footer and metadata parsing, row-group/page
  decoding, compression, dictionary/null handling, pruning, coalescing, and
  cache validation.
- Exclude: Iceberg and sidecars.
- Gates: lake scaffold tests, format fixtures, cache/range tests, and credential
  routing tests.

### L3: Iceberg inventory and deletes

- Branch: `feature/lake-iceberg`
- Worktree: `.worktrees/feature-lake-iceberg`
- Parent: L2
- Scope: metadata snapshots, Avro manifests, partition/stat pruning, delete
  manifests, and pinned inventory semantics.
- Gates: Iceberg fixture and pruning tests.

### L4: Lake sidecars and operations

- Branch: `feature/lake-sidecars-operations`
- Worktree: `.worktrees/feature-lake-sidecars-operations`
- Parent: L3 plus R5/C2 integration prerequisites.
- Scope: external source catalog binding, public lake row routing, sidecar
  selection/hydration, rebuild/reconcile/promotion/GC, explain diagnostics, and
  operator-facing APIs.
- Gates: lake scaffold suite, row API tests, catalog tests, generated checks,
  and end-to-end object-store fixtures.

## Generated artifact policy

The combined branch contains generated changes from several domains in the same
files, especially:

- `openapi.yaml`
- `go/pkg/sdk/oapi/client.gen.go`
- `py/packages/sdk/src/antfly/client_generated/`
- `ts/packages/sdk/src/public-api.d.ts`
- `zig/pkg/antfly/src/openapi/generated/`
- `zig/pkg/antfly/src/sql/grammar/generated/root.zig`

For each PR:

1. Port only its source schema or grammar edits.
2. Run the repository generator.
3. Commit the resulting complete generated delta for that branch.
4. Run the generated freshness checks.

This makes large generated diffs deterministic and prevents later features from
leaking into earlier PRs.

## Extraction method

Use three techniques, in this order:

1. Cherry-pick a small historical commit only when it is already coherent and
   does not import unrelated combined-branch structure.
2. Otherwise apply a path-limited patch from the combined branch, then remove
   unrelated declarations and adapt imports to the target branch.
3. For heavily interleaved monoliths, port symbol-by-symbol from the final
   implementation and use the combined branch tests as the behavioral oracle.

Do not merge `combine-pr-141-143-144` into a restructure branch.

## Publication plan

- Publish root PRs first: M1, T1, A1, A2, A3, and O1.
- Publish stacked drafts for M2/M3 and the feature chains once their parent is
  stable.
- Put `Depends on #...` and the exact parent branch in every stacked PR body.
- After a parent merges, rebase the child onto `origin/main`, rerun its gates,
  and update its base branch.
- Replace PR #145 with a tracking issue or retain it only as a temporary
  integration comparison until its residual diff is empty.
