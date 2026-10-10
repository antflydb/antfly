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

## SQL uniqueness null-policy activation

The next shared semantic gap is the SQL-to-native declaration boundary for
`NULLS DISTINCT` and `NULLS NOT DISTINCT`. Native public schemas, immutable
constraint fingerprints, integrity tuple encoding and conflict-owner resolution
already represent this policy. SQL CREATE TABLE, ALTER TABLE and CREATE INDEX
must retain it rather than introduce another uniqueness mechanism.

The implementation now carries one null-policy flag through SQL IR and schema
lowering, for inline/named/composite constraints and plain/expression/partial
unique indexes. Included columns remain outside the unique tuple. PostgreSQL
places the clause immediately after UNIQUE for constraints, and after optional
INCLUDE for indexes. A nonunique index may accept the clause without acquiring
unique enforcement. Durable schema formats and per-row encoding are unchanged.

The isolated PostgreSQL oracle covers eight declaration profiles, NULL and
nonnull duplicate rejection, partial-index exclusions, composite/all-NULL keys,
complete ID postimages after failures, nonunique behavior and six malformed
clause placements. Native tests cover parsing, public-schema lowering,
deferrability, NULL-distinct row-specific witnesses versus shared NOT DISTINCT
claims, conflict arbiters and allocation-failure cleanup.

The cross-layer sweep exposed a preexisting schema-cloning leak during partial
full-text field construction. Decoder ownership now transfers complete fields,
rules and path collections to their parent, with one cleanup owner for each
completed subtree. The expanded mapping fault test checks nested allocations,
multiple documents and byte-for-byte serialization round trips. The SQL gate
explicitly selects these cross-layer tests; test discovery is audited, not
assumed from a passing compiler/executor subset.

These are implementation and component qualification steps. Original cases
`sql-0692`, `sql-0706` and `sql-0731` still need unchanged-source mounted catalog
activation and their required lifecycle evidence before disposition changes.
Distributed owner/failover qualification and the larger pools remain open.

### Qualification checkpoint (2026-10-10)

- Full optimized `zig build sql-test`: 17/17 steps, 694 local and 226 native
  tests passed, with no failures or leaks. This includes the unchanged original
  compiler checks and the nested schema allocation-failure regression.
- PostgreSQL 19beta4: all four isolated lifecycle/original-DDL oracle tests pass.
  The reference statements are loaded from the frozen original inventory.
- Formatting, new Python lint, inventory integrity and the 1,991-file Apache
  source boundary pass. Original dispositions remain 478 / 136 / 73 / 899.
- Full debug `zig build sql-test`: 17/17 steps, 691 local tests passed with
  three deliberate benchmark skips, and all 226 native tests passed, with no
  failures or leaks.
- The server-side `antfly-sql-index-ddl-test` is still compiling; its completion
  is not claimed. An earlier broad, pre-change API build was retired, not
  counted as passing.

The next delivery is mounted catalog activation/recovery evidence for the
original index and constraint cohort, including real ownership and readiness
checks. This checkpoint does not close the larger SQL goal or its remaining
distributed, lifecycle and public execution requirements.
