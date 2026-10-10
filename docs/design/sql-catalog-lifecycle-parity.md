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
- Server-side `antfly-sql-index-ddl-test` passed at commit `64b74151ea`:
  47/47 build steps and 26/26 tests. This predates the admission unification
  and mounted native-owner campaign below. An earlier broad, pre-change API
  build was retired, not counted as passing.

### Mounted native-owner admission (validation in progress)

Ordinary ALTER schema admission now resolves the table definition and relation
owner from one bounded immutable catalog cut, then submits the same guarded
metadata-owner CAS used by index DDL. The guard carries table identity, schema
digest/version and metadata incarnation. The SQL helper requires this guard:
there is no alternate unguarded schema-mutation or retirement replacement
callback. Regressions reject missing epoch attestation and wrong object kinds
before mutation, and verify stale admission is attempted once without replay.

A new mounted HTTP campaign executes original `sql-0692`, `sql-0706` and
`sql-0731` unchanged against a production `MetadataService` with an actual
single-voter leader. It checks bounded owner lookup, guarded admission, durable
null policy, pending receipts without fake data readiness, and metadata-service
restart identity. This campaign is being compiled and is not yet claimed
passing. Single-voter metadata recovery is not distributed failover or
data-owner unique-index activation; all three originals remain unresolved.

The independent PostgreSQL oracle now uses the same `(id BIGINT PRIMARY KEY,
email TEXT)` profile for these originals. It checks NULL and non-NULL duplicate
admission, SQLSTATE 23505, and complete ordered postimages after failed writes.
It also verifies SQLSTATE 42809 for ALTER TABLE against ordinary, UNIQUE and
primary-key index relations, preserving rows and index metadata. All five
isolated oracle tests pass; native campaign validation remains pending.

ALTER admission also consumes the connection search path in one bounded
relation lookup, without skipping wrong-kind or unauthorized shadows. Empty
paths prove absence without a catalog request; explicitly qualified names
bypass the path. The selected namespace flows into authorization, the durable
owner guard and the public receipt. The focused native regression exercises
these cases without schema-mutation/snapshot fallback callbacks. A sixth
PostgreSQL oracle test independently verifies search-path selection, index
shadowing (42809), empty-path absence (42P01) and qualified ALTER; all six
oracle tests pass. The native build started before this path change and cannot
certify it; a final current-source gate is still required.

The earlier 26-test focused native gate did not discover the campaign module.
The HTTP runtime discovery root now imports it explicitly, and the focused DDL
gate audits its executable inventory for all three campaign tests plus the
owner/ALTER regressions before running. A passing unit-only gate cannot stand
in for mounted recovery evidence. This audited current-source gate is running;
no campaign completion is claimed yet.

### Next confirmed catalog gap: generated unique-constraint names

The compiler currently generates table-independent names such as
`sql_primary_0` and `sql_unique_1`, while the durable relation registry claims
constraint indexes in the shared namespace. Independent tables can therefore
collide on autogenerated names. The PostgreSQL oracle establishes table-based
names (`first_pkey`, `first_email_key`) and collision suffixing (`third_pkey1`),
while explicit user names must fail on conflicts rather than be renamed.
All seven oracle tests pass. Native naming remains unfinished: retain explicit
versus generated provenance, choose names against the admitted namespace cut,
and commit the chosen names with the table through existing catalog CAS. A
compiler-only prefix change is insufficient for namespace races and collisions.

The native `system_catalog/constraint_names.zig` component now implements
63-byte generated-name formatting with balanced, UTF-8-safe table/key prefix
clipping, complete numeric suffixes, bounded composite-key preparation, and
bounded/cancellable occupancy probes. Only the selected name is allocated;
both standalone native tests, including allocation-failure checks, pass. The
eighth PostgreSQL oracle test independently checks long, composite and UTF-8
names against the component's expected values; all eight oracle tests pass.
This component is not yet wired into DDL admission and does not fix public
autogenerated names on its own.

The oracle also proves occupancy must cover non-index CHECK/FK constraint
names, not just the relation registry: an existing CHECK named `sixth_pkey`
forces a generated PRIMARY name of `sixth_pkey1`. Admission therefore needs a
durable, nonexclusive namespace constraint-name membership projection, coupled
to table mutations and reconciliation/publication. Missing or unactivated
membership must never mean a name is free. Integrate generated-name provenance
through compilation and private catalog requests, choose against the admitted
relation/constraint cut plus names reserved by the same statement, and retain
the existing atomic catalog commit and uncertain-outcome rules. Activation,
restart, concurrency and incomplete-reconciliation fault tests remain required.

The formatter also bounds source identifiers to the native relation registry's
256-byte limit before UTF-8 scanning. Its bounded composite prefix preparation
and single selected-name allocation remain covered by the standalone tests.
The membership projection must use owner-qualified keys (namespace, name,
table identity) so repeated explicit CHECK/FK names do not become exclusive
relation claims. Pin its active generation and covered source epoch with the
relation registry; maintain membership deltas in the same metadata transaction
and rebuild/publish them through the existing bounded reconciliation and GC
machinery. Occupancy should use an exact-name prefix seek with early exit,
never a namespace/table schema scan under the admission lock.

The schema-cut component now exposes an opt-in, names-only constraint
membership projection alongside exclusive relation claims. It includes named
constraint-origin UNIQUE/PRIMARY, CHECK and FK declarations, excludes
index-origin unique rules, deduplicates occupancy within a table, and bounds
aggregate declaration work independently of deduplicated output size.
Successor projection retains separately fenced active/predecessor and
reserved/successor memberships, including namespace and table-identity moves.
Both sets own their names after source buffers are released. Relation-only
callers retain their existing parsing path and do not allocate memberships.
All 30 relation-registry tests pass, including stable allocation-failure
sweeps, cross-table nonexclusivity, declaration bounds and source lifetime.
This is a projection foundation, not activated catalog occupancy: durable
owner-qualified storage, same-transaction producer deltas, reconciliation
candidate verification/root activation, GC and admission lookup remain unwired.
Do not interpret the default empty membership projection as evidence that a
generated candidate is free.

The corrected mounted campaign gate now proves discovery of all three HTTP
campaigns and the guarded ALTER/index tests. Its first complete execution ran
23 native tests: 22 passed, while the real-owner setup failed because its
fixture incorrectly required HTTP 200 for a committed-pending CREATE TABLE.
The production response was HTTP 202 with a pending receipt and schema
version 0. The fixture now asserts 202/pending explicitly for setup and ALTER,
and derives ALTER's expected version from the committed setup receipt rather
than assuming an initial version of 1. The post-main-merge gate is rerunning;
the new real-owner campaign is not yet claimed passing. A subsequent compile
caught the public OpenAPI receipt's signed 64-bit version versus the native
catalog's unsigned 32-bit version; the fixture now keeps the wire type and
checks the native range before deriving the next expected version.

Generated-name provenance now survives parsing and SQL DDL lowering as
compiler-owned data, distinct from the editable public schema. Anonymous
inline and table-level PRIMARY, UNIQUE, CHECK and FK declarations are marked
explicitly; user names that happen to start with `sql_primary_`,
`sql_unique_`, `sql_constraint_` or `sql_inline_` are not. CREATE's internal
catalog request carries only the generated names, in declaration order.
Preparing the hints makes one exact-sized pointer-array allocation (none for
fully explicit declarations), borrowing names from the live compiled
statement rather than copying identifier bytes.
The first optimized SQL gate passed 696 local and 226 native tests, plus 30
relation-name and 56 reconciliation tests. A follow-up gate adds the hint
allocation-failure test and runs the two native formatter tests as an explicit
`sql-test` dependency, also exposed as `system-catalog-constraint-name-test`.
The follow-up passed all 19 steps: 697 local and 226 native tests, plus both
formatter tests; the 30/56 relation and reconciliation results were cached.
Formatting, inventory integrity and the Apache boundary (1991 source files)
also passed. No generated name is yet chosen against
durable namespace occupancy: admission and private native requests remain the
next integration boundary, including the initial FK-create publication path.

Publication must explicitly certify occupancy coverage, not merely reuse a
relation root that happened to contain no nonrelation members. Current native
topology decoder capabilities stop at version 33, and existing relation/live
root seals attest only relation claims. The membership producer must therefore
have a new capability-gated, independently verified root/coverage contract.
Old roots and partially rebuilt candidates cannot satisfy generated-name
admission. Use owner-qualified typed keys in the same staged generation and
bounded source/candidate verification, atomic root publication and GC job;
the exact-name seek must authenticate that complete generation/source epoch
before treating absence as free. Include membership owners in canonical
counts/hashes and replay/backup verification, and force a fresh reconciliation
generation when the coverage format changes. Do not silently reinterpret old
v1 root seals as proof of the new projection.

The shared registry now has a typed owner-qualified key codec for constraint
memberships: namespace/name plus a nonzero table-identity suffix, and a private
`constraint_name` owner kind distinct from relation kinds. Existing relation
key bytes and hashes are unchanged. An exact-name physical prefix contains
the exclusive relation key and every table-qualified membership, enabling a
bounded seek/early-exit occupancy probe once coverage is activated. Empty owner
suffixes and wrong key/owner classes or table identities are rejected. The CAS
planner, key ownership/copying, registry adapters and candidate hash preparation
preserve the qualifier; different tables can retain the same constraint name
without colliding with each other or with a real relation of that name.

The codec alone is deliberately NOT serving authority. Legacy direct registry
and live-root lookup/write/delete reject typed membership keys with
`CatalogOccupancyNotReady`, rather than treating missing keys as free.
Relation-only reconciliation preparation, candidate/restore/replay verifiers
and GC reject extended records until an occupancy-capable coverage contract
exists. No producer emits these typed records yet. This prevents accidental
activation through the old v1 root seal while the next publication stage is
implemented. The new kind is private catalog metadata, not a new public index
type or a table/index relation that should appear in the OpenAPI relation API.

Typed-key tests cover canonical quoted/UTF-8 names, exact-name prefixes,
noncanonical suffixes, whole-suffix truncation paired with a mismatched owner,
cross-table/relation coexistence, exact-owner retirement, rejected replay with
zero writes, input retirement, and allocation-failure cleanup. Current Debug
registry/reconciliation gates pass 95 tests. The optimized SQL gate passed
697 local plus 226 native tests and 93 registry/reconciliation tests, including
the final legacy-adapter guard. The additional prepared-cut tests passed in
Debug; their optimized SQL gate is now running. Native mounted catalog
campaign validation is still running separately.

Combined typed-cut preparation now caches the result per owned schema cut,
retains both active/pending fences for same-table successors, and releases its
temporary lookup map before returning. It borrows already-owned name bytes,
reserves a bounded claim array, and rejects aggregate claim-budget overflow.
A relation-only source cut cannot invoke this path as if an empty membership
list were complete: its projection flag remains explicitly unready. The
source-lifetime/allocation-failure sweep exercises combined preparation too.

For the next coverage-format integration, retain the current fixed-size binary
state layouts but use distinct equal-length format headers (current AFRC04
job state and AFRL01 live root versus occupancy-capable successors). Carry the
coverage kind through decoding, encoding, independent verification, replay and
publication so old cuts round-trip without being promoted. New root seals must
cover both typed relations and memberships, while a new topology decoder floor
gates production of the extended state/entries and writer adoption. Old
decoders must reject the new header rather than ignore a JSON extension or
reinterpret a relation-only seal. Startup, active/pending FK/restore sources,
ordinary writer deltas, bounded candidate verification and GC must all select
the same coverage mode before generated-name admission can be enabled.

The next delivery is mounted catalog activation/recovery evidence for the
original index and constraint cohort, including real ownership and readiness
checks. This checkpoint does not close the larger SQL goal or its remaining
distributed, lifecycle and public execution requirements.
