# SQL catalog lifecycle and original-case qualification

Status: follow-up to merged PR #1048, based on `origin/main` at `6339e1519c`
(including PR #1060). This is an implementation and qualification backlog, not
a completion claim. PostgreSQL is the SQL standard and independent test oracle.

## Baseline and priorities

The [original inventory](../sql-parity-inventory.md) remains authoritative:
478 implemented / 136 rejected / 73 superseded / 899 unresolved. Counts below
are unresolved family pools, not promises that one implementation closes them.

| Priority | Pool | Cases | Shared delivery boundary |
| --- | --- | ---: | --- |
| 1 | DDL, catalog and sessions | 340 | Public relation ownership, schema/index activation and retirement, session lifecycle and recovery |
| 2 | Source/joined mutations and MERGE/recursion | 126 | Captured source ownership, bounded selection, atomic commit and distributed failure evidence |
| 3 | Read and query | 132 | Faithful typed fixtures, binding/correlation, endpoint results and diagnostics |
| 4 | Document writes | 70 | Document-specific schema/index profiles, complete postimages and integrity admission |
| 5 | Historically unsupported writes | 92 | Individual PostgreSQL contract review, implementations or proven required rejections |

## First delivery: catalog and index lifecycle

Public index resolution already exists in `api/sql_catalog.zig`. Preserve its
bounded single-cut search-path lookup, logical-table authorization, immutable
owner identity and metadata-owner mutation guard. Do not replace it with a
whole-catalog snapshot or resolve a different owner after admission.

1. Reconcile exact original index/constraint cases against current code and
   fixtures. Separate missing behavior, missing public/lifecycle evidence and
   source contracts that PostgreSQL rejects. Historical checkpoint descriptions
   are not proof that a capability is still absent.
2. Qualify CREATE/DROP index resolution, search-path shadowing, conditional
   absence, relation-kind collisions and authorization. Empty paths must not
   silently fall back to the request namespace. Qualified names ignore the path.
3. Reuse existing metadata authority, index build state, constraint activation
   and retirement machinery. Verify ready/invalid/pending outcomes and retain
   durable recovery handles after uncertain admission; never auto-replay DDL.
4. Exercise mounted SQL through committed native metadata/storage with exact
   schema and index declarations, independent integrity probes, complete table
   postimages and bounded owner access. Table-bound schema-builder tests alone
   cannot establish public index DDL completion.
5. Cover stale owners, concurrent schema publication, restart, dropped/recreated
   names and promoted owners. Multi-owner, CONCURRENTLY and dependency/CASCADE
   operations require their actual protocols, not independently committed loops
   or blocking aliases.

## Reusable exact-case qualification

Keep original statements and parameters unchanged. Each activation must own its
setup and required owner profiles: logical PK, UNIQUE, partial/expression index,
FK, CHECK, defaults and temporal/array types where applicable. Ordinary document
JSON arrays are not SQL typed arrays. An unconstrained or incorrectly typed
baseline cannot certify a schema-dependent case.

Compare PostgreSQL values, labels/types, NULLs, SQLSTATE and validation timing.
Mutation evidence includes all affected table postimages and unchanged storage
after rejected operations, not just RETURNING or affected-row counts. Add native
access counters and scaling tests for point/index selection and captured sources;
avoid full-table capture where bounded owner lookup suffices.

Reuse these fixtures for the next read, document and source-mutation batches.
Close an original ID only when its required public, durable and distributed
boundaries have evidence. Component improvements and new regressions do not
automatically receive original-case credit. Keep exhaustive regex qualification
in [its own design](sql-regex-parity.md), alongside this work rather than using
regex fixture totals as SQL completion metrics.

## Initial regression

The first patch fixes conditional index absence with an empty connection search
path. Native regression checks require no catalog read or mutation for that
no-op, preserve the unconditional missing-object error, reject an unqualified
CREATE with no lookup namespaces, and verify explicitly qualified DROP still
resolves its owner. An isolated PostgreSQL test checks the corresponding public
SQL behavior and that the no-op leaves a qualified existing index intact.

This regression is not an original-case disposition change. Broader activation,
distributed fault qualification and the remaining pools above are still open.

### Reproduction

Run `make sql-catalog-lifecycle-oracle-check` with PostgreSQL 18+ installed,
or set `ANTFLY_PG_BIN` to the directory containing its server binaries. The
oracle owns a temporary database and private Unix socket; missing binaries or
an unavailable oracle fail the test rather than skip it. PostgreSQL 19beta4
is the currently available local oracle, not final-release certification.

The native regression belongs to `zig build public-api-parity-test` in `zig/`.
Runtime selection of the exact `api.sql_catalog.test.SQL index DDL drops the
exact qualified owner without a catalog snapshot` test is useful for development
but does not constitute a passing full public API or distributed release gate.
