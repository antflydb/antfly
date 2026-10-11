# SQL catalog lifecycle and original-case qualification

Status: follow-up to merged PR #1048, merged with `origin/main` at `121699433e`.
This is an implementation and qualification backlog, not
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
and GC reject extended records. Shared reconciliation now accepts typed records
only under the owning job, live-root or retirement coverage contract. Native
source and writer producers do not emit these records yet. This prevents accidental
activation through the old v1 root seal while the next publication stage is
implemented. The new kind is private catalog metadata, not a new public index
type or a table/index relation that should appear in the OpenAPI relation API.

Typed-key tests cover canonical quoted/UTF-8 names, exact-name prefixes,
noncanonical suffixes, whole-suffix truncation paired with a mismatched owner,
cross-table/relation coexistence, exact-owner retirement, rejected replay with
zero writes, input retirement, and allocation-failure cleanup. Current Debug
registry/reconciliation gates pass 95 tests. The optimized SQL gate passed
697 local plus 226 native tests and 95 registry/reconciliation tests, including
the final legacy-adapter guard and prepared-cut tests. Native mounted catalog
campaign validation is still running separately.

Combined typed-cut preparation now caches the result per owned schema cut,
retains both active/pending fences for same-table successors, and releases its
temporary lookup map before returning. It borrows already-owned name bytes,
reserves a bounded claim array, and rejects aggregate claim-budget overflow.
A relation-only source cut cannot invoke this path as if an empty membership
list were complete: its projection flag remains explicitly unready. The
source-lifetime/allocation-failure sweep exercises combined preparation too.

Coverage-format preparation retains fixed-size binary state layouts with
distinct equal-length headers: AFRC04/AFRC05 job state, AFRL01/AFRL02 live roots,
and AFRT01/AFRT02 retirement state. Coverage survives source-epoch changes and
retirement; mismatched root/job coverage and downgrades fail closed. The
extended command floor is reserved at topology version 34, but the advertised
version remains 33: this is not production activation. Debug and ReleaseFast
focused control gates pass (15 worker, 3 command, 64 reconciliation tests,
plus the 34-test relation gate reused from cache). Remaining consumers must
carry coverage through replay and publication before enabling writers.
New root seals must
cover both typed relations and memberships, while a new topology decoder floor
gates production of the extended state/entries and writer adoption. Old
decoders must reject the new header rather than ignore a JSON extension or
reinterpret a relation-only seal. Startup, active/pending FK/restore sources,
ordinary writer deltas, bounded candidate verification and GC must all select
the same coverage mode before generated-name admission can be enabled.

The shared reconciliation consumers now carry coverage through source pages,
candidate scans, point reads, exact-owner mutations, independent verification,
authenticated replay and bounded garbage collection. Replay validates key and
owner classes even for unchanged entries, rejects same-generation mode changes,
and retains the original retirement mode after intent deletion. Legacy public
classification remains fail-closed until native restore/replay callers select
coverage from their attested cuts. New tests exercise a relation and a
same-named table-qualified membership through source verification, publication,
membership-only deletion and replay; a separate retired-generation test covers
GC and replay after retirement intent removal. The complete lifecycle also
passes allocation-failure injection. The Debug reconciliation gate passes all
69 tests in both Debug and ReleaseFast, including the unchanged-replay
regression and allocation-failure sweep. This does not activate production
occupancy, generated-name admission, or change original-case dispositions.

The mounted native campaign must adopt source tracking through the actual
membership-fenced reconciliation command before waiting for publication.
CREATE topology readiness alone does not initialize the source epoch; without
it, the worker correctly remains idle. The fixture now performs that durable
adoption and reports compact diagnostics on convergence failure. Its expanded
native gate is still running; this is not yet activation or recovery evidence.

Native integration in progress now carries a pinned job's coverage through the
existing table/hidden-create/staged-restore source cursor, publication scan and
snapshot renewal. Active and pending schema cuts use the same membership
projection; restore contribution filtering applies after typed claims are
materialized. Relation-only cuts still skip constraint membership extraction.
Captured write deltas select coverage from the live root, and independent
source-owner and checkpoint/recovery checks select the corresponding job/root
mode. Structural record dispatch does not authorize a typed record; decoding
still requires its owning coverage contract.

The fresh optimized native run passes all 40 selected tests, including
scratch-read ownership and allocation-failure checks for both modes and the
new real-storage write/checkpoint/restart regression. The fixture supplies adoption barriers externally;
it is not evidence of production all-voter capability activation. Protocol 34
remains unadvertised. Remaining work includes capability/adoption and migration
coordination, full restore/replay fault qualification, generated-name admission
and unchanged original SQL lifecycle/DML qualification.

Coordinator preparation now contains a bounded activation/adoption/upgrade
state machine. An occupancy upgrade creates a new generation and retires the
old seal with its original coverage; it never relabels a relation-only root.
The worker validates prepared migrations against the observed cut, waits on
receipt fences between intents, and resolves ambiguous appends from durable
work. Native publication preparation requests the coverage-specific floor;
standby publication, guarded mutations and activation downgrade checks use
that same floor. Adoption and publication proofs both bind exact incarnation,
member count and membership fingerprint to proposal admission before append.
A newer admitted decoder may satisfy an older fixed-format proof without
changing its wire version.

The native store gate passes 41 tests, including lower-capability adoption
rejection with unchanged source clock, job and standalone revision. Debug and
ReleaseFast control gates pass 18 worker and 5 command tests; these include migration
receipt/leader/uncertainty faults, version/truncation checks and exact-membership
admission. Service coordinator validation is running. The latest service-level
proof-binding changes require a fresh
coordinator run after the already-running older input completes. The coordinator
selects automatic occupancy adoption only once this binary advertises protocol
34; the advertised version deliberately remains 33 pending complete native
restore/replay and distributed capability qualification. This is not yet
production activation or original-case disposition credit.

The optimized native relation-store gate now passes 43 tests. The same
70-table staged-restore reconciliation fixture runs in relation-only and
occupancy modes; the covered cut verifies 490 owner slots, including shared
constraint names on distinct old/new table identities. It exercises bounded
source/candidate scans, snapshot renewal, cancellation in both phases,
source/index fence changes and forged publication ownership. Missing typed
memberships, wrong table qualifiers, wrong owner kinds and forged publication
IDs all fail independent native verification even when scalar publication
fences still match. Hidden CREATE cursors now run in both coverage modes with
CHECK/FK memberships; allocation and page-size assertions remain unchanged
after adding 1,000 terminal plans. These are native metadata tests, not claims
of distributed data-owner activation or PostgreSQL original-case completion.

Compiler-local generated constraint labels now avoid every explicit spelling
in the same CREATE statement, including later declarations, without changing
explicit names or inferring provenance from a prefix. A single bounded hash
table reserves original labels; one increasing fallback counter bounds total
collision probes linearly in the statement's constraints. This prevents
schema lowering from rejecting valid SQL merely because a user chose an
internal label such as `sql_primary_0`. PRIMARY/UNIQUE, CHECK and FK regressions
exercise the DDL dispatch and generated hints, and duplicate explicit names
still reject. The focused SQL DDL gate passes six tests in Debug and ReleaseFast,
including allocation-failure cleanup. The PostgreSQL 19beta4 catalog oracle
passes all nine tests, including independent final-name and duplicate-name
semantics for these statements. This fixes internal identity handling, not the
still-pending public final-name admission. No original dispositions changed;
inventory integrity passes with 899 unresolved cases.

The post-merge mounted campaign exposed a fixture switch missing main's new
global `query_source` kind; it now uses the domain's global parent identity.
The mounted campaign and coordinator builds also failed on a missing local
cached Metal object. Fresh CPU-backed runs (`-Dmetal=false`) are in progress;
neither earlier build is a completed native or service qualification claim.

The next delivery is mounted catalog activation/recovery evidence for the
original index and constraint cohort, including real ownership and readiness
checks. This checkpoint does not close the larger SQL goal or its remaining
distributed, lifecycle and public execution requirements.

### PostgreSQL naming phases and occupancy policy

The shared bounded formatter now covers PRIMARY, UNIQUE, CHECK and FK names.
CHECK includes a column only for exactly one distinct referenced column;
constant and multi-column checks use `table_check`. FK names retain ordered
local columns. Full numeric suffixes and UTF-8 boundaries remain intact,
including the maximum suffix for the longer `check` label. Selection passes
the naming kind to the occupancy callback: PRIMARY/UNIQUE consult relations
and constraint memberships, whereas CHECK/FK consult memberships only.

The PostgreSQL 19 oracle establishes that final-name admission must be phased,
not a blanket pre-reservation of all explicit names. Anonymous PRIMARY plus
an explicit `t_pkey` UNIQUE fails with `42P07` in either textual order. An
anonymous CHECK followed by an explicit duplicate fails with `42710`; the
reverse order lets the anonymous CHECK choose a suffix. Explicit CHECK names
are already visible to generated UNIQUE naming irrespective of textual order.
Plain relations do not force CHECK/FK suffixes, but constraint memberships on
other tables do. Compiler temporary-label disambiguation is intentionally
separate from these final-name rules.

Required integration remains: one pinned covered namespace cut; staged CHECK,
primary/unique-index and FK naming in PostgreSQL visibility order; exact
selected-name predicates revalidated inside the atomic native mutation; and
the same fences in hidden FK CREATE and ALTER publication. A legacy or absent
occupancy root is not evidence that a name is free. Concurrent conflicts must
produce definite nonadmission or a retained uncertain receipt, never ambiguous
automatic replay. The new oracle/component tests do not establish that this
public native integration or distributed activation is complete.

### Normalize index declarations before catalog naming

CREATE now collapses equivalent PRIMARY/UNIQUE declarations before schema
lowering, following PostgreSQL's index-constraint normalization. PRIMARY is
the preferred survivor even if textually later; otherwise the first equivalent
declaration survives. An explicit duplicate can supply an anonymous survivor's
name and clears its generated-name provenance. Ordered keys, NULL-distinct
policy, deferrability and initial timing are part of equivalence, so differing
enforcement domains never collapse. This concerns CREATE declarations only,
not reuse of pre-existing indexes during ALTER.

The normalizer uses hashed structural keys and grows its scratch index with
distinct declarations, not total duplicates. Compaction preserves unrelated
CHECK/FK declarations. Native tests cover explicit-name transfer and allocation
faults, 32 distinct indexes, and identical single-constraint schema output from
64 versus 1,024 redundant declarations. The focused SQL DDL gate passes ten
tests in both Debug and ReleaseFast. These are schema-shape/access-bound tests,
not measured native write-throughput or physical-disk benchmarks.

PostgreSQL 18 passes all twelve catalog oracle tests, including independent
constraint/index counts and policy/name-transfer checks for ten normalization
profiles. The prior temporary PostgreSQL 19 installation is missing; no new
19 certification is claimed for this checkpoint. An earlier broad optimized
SQL gate passes 701 local and 226 native tests; a fresh broad gate for the final
distinct-key growth change is running. The separate mounted catalog campaign
and coordinator CPU-backed gates remain pending. Original dispositions and
production occupancy activation remain unchanged.

### Additive ALTER preparation checkpoint

Single-constraint ALTER preparation now seeds table-local constraint and index
occupancy from an existing schema, then stages only the new constraint. It
preserves unrelated schema fields and existing declarations rather than
re-admitting them as CREATE declarations. Standalone UNIQUE indexes contribute
relation occupancy but not constraint membership, including when their schema
contains an index-owned uniqueness rule. Selected-name validation likewise
does not mistake an index-owned rule for a same-named CHECK/FK constraint.
Replacement-schema and multi-constraint fragments are rejected by this additive
preparation interface. The existing aggregate probe and cancellation bounds
remain in force.

Native preparation accepts an immutable captured relation guard, not a caller's
baseline schema. It checks incarnation, namespace binding, relation kind and
the exact current owner in one read transaction, then loads that owner's schema
by table identity. Changes to unrelated owners use current occupancy without a
global captured-source-epoch CAS. Native regressions cover stale owners, wrong
incarnations/kinds, schema-only changes and physical schema substitution. The
native gate passes 48 tests, using a schema projection whose digest is checked
against the pinned owner rather than decoding unrelated table settings.

The shared naming gate passes eleven tests in Debug and ReleaseFast, including
deterministic allocation-fault sweeps. The sweeps also exposed and fixed an
arena ownership-transfer bug: serialization now completes before arena state
is copied into the returned plan, so a later serialization block is not lost.
All thirteen PostgreSQL 18 catalog oracle tests pass, with four independent
ALTER index/constraint-ownership profiles and unchanged table postimages.

This is preparation, not public ALTER activation. Ordinary schema publication,
rewrite jobs and FK generation publication must carry and atomically recheck
the selected predicates alongside the captured owner before capability 34 is
advertised. Original inventory dispositions remain unchanged.

### Prepared generated-name admission checkpoint

The CREATE naming pipeline now owns its schema and selected-name predicates,
pins a covered catalog read, and stages CHECK, primary/unique and FK names in
PostgreSQL visibility order. Candidate probes share an aggregate budget and
honor cancellation. Namespace occupancy uses bounded seeks rather than
enumerating all owners of a shared constraint name. Explicit index names
check exclusive relations without rejecting other-table CHECK memberships.

Version-two catalog commands carry exact selected-name predicates, checked
again inside the atomic mutation. Ordinary requests and version-one commands
retain their previous JSON representation. Definite naming diagnostics cross
metadata forwarding only after a definite not-proposed outcome; uncertain
receipts retain the existing unknown-outcome semantics.

This is not production activation: advertised capability remains 33 and the
new admission requires 34. Hidden FK CREATE and ALTER integration, distributed
fault qualification and capability activation remain unfinished. No original
parity disposition is changed by these component tests.

Before the latest main merge, the shared reconciliation gate passed 70 tests
in both Debug and ReleaseFast, the naming gate passed eight in both modes,
and the native catalog gate passed 45 tests. The broader SQL gate passed
703 local and 226 native tests before this admission integration. After
merging main at b7e4f43a30, the focused naming gate passes again; the fresh
native catalog gate is still running at this checkpoint.

### Hidden FK CREATE prepared-name integration

Hidden FK CREATE now carries trusted compiler provenance through its private
preparation request. Metadata selects names from the same read transaction
as the catalog identity and child placement, before witness generation and
plan fingerprinting. The returned owned schema and exact name predicates are
retained in the initial plan. Begin revalidates the predicates inside its
reservation transaction, before any hidden identity or support work is written.
The coordinator requires occupancy capability for naming-enabled plans; apply
independently checks durable capability. Defeated admission predicates are
safe committed begin no-ops, while malformed records and storage failures
remain errors. Preparation honors request cancellation and preserves definite
naming diagnostics across the private read-only HTTP call.

Absent provenance/predicates are omitted from legacy request, preparation and
plan JSON. A byte-for-byte legacy plan regression protects existing durable
digest inputs. Native tests cover covered-root preparation, CHECK/FK collision
policy, namespace binding, and preparation without catalog revision changes.
The native catalog gate passes 47 tests, including the final pre-write-only
no-op guard refinement and schema-only competing-writer regression. A separate initial-FK gate exercises
lower-capability preflight rejection and committed nonreservation followed by
successful legacy admission; that gate and the forwarding/coordinator gate
are still running at this checkpoint. Public activation remains disabled,
and no original case disposition is credited by these component tests.

### Atomic relation-mutation name predicates and final decoder admission

The existing relation mutation guard now carries one optional additive-name
predicate with owned cloning, decoding and destruction. Absent predicates retain
the previous JSON bytes. Guard shape validation keeps the existing 8 KiB frame
bound, rejects index-owner/multi-action misuse, and validates names before
allocation. The native replacement guard requires durable occupancy capability
and checks the selected name against the current covered namespace inside the
same transaction as owner/binding validation, before replacement writes.

Naming-enabled relation replacements, catalog commands and hidden FK CREATE
commands now require capability 34 at the final single/batch proposal boundary,
not only the workflow's earlier readiness probe. JSON command classification
projects the capability marker with a fixed 4 KiB allocator; large escaped schema
strings are skipped rather than materialized. Malformed, future and exhausted
projections fail closed to the newer admission requirement. Ordinary legacy
frames keep their predecessor classification.

The native catalog gate passes 48 tests including named-guard codec roundtrip
and lower-capability nonadmission without revision changes. Two guard ownership,
legacy-wire and allocation-fault tests pass in Debug and ReleaseFast. The pure
wire projection gate also passes in both modes with a 64 KiB escaped schema.
An earlier coordinator run exposed a fixture using a synthetic adoption
fingerprint instead of its readiness proof. The fixture now uses the exact
protected count/fingerprint; stale-count and moved-fingerprint rejection tests
remain. The corrected coordinator gate is running, and no pass is claimed yet.

Public ALTER still needs the preparation RPC and publication wiring for ordinary
schema changes, rewrite jobs and FK generation transitions. Advertised capability
remains 33; no original case disposition changes at this checkpoint.

### Private preparation boundary and once-lowered ALTER candidates

Constraint-name preparation now crosses the storage-free facade as an owned
JSON projection, rather than returning a native arena-bearing plan across the
kernel boundary. CREATE and additive ALTER share this private boundary. The
FK CREATE projection also carries a normalized platform deadline. Cancellation
is checked before and after the facade call; native probes check the transported
deadline. This does not claim mid-call cancellation-token propagation across the
archive boundary. Definite name conflicts, occupancy-not-ready and probe-limit
errors retain stable runtime error identities. The preparation RPC is read-only
but requires a body-bound administrative grant and linearizable owner admission.

The compiler now recognizes anonymous ALTER CHECK, UNIQUE, PRIMARY KEY and FK
declarations with explicit internal generated-name provenance. Local preparation
seeds a bounded name set once, chooses a collision-free unpublished label, and
lowers/validates one candidate. The catalog receives only the added rule, not a
caller-supplied baseline schema. Its selected name replaces only that already
lowered rule. Invalid replies cannot change the pinned owner or mutate the
predecessor. Ordinary schema publication retains the returned admission predicate.
FK generation and primary-key rewrite publication explicitly reject a predicate
until their durable plans can retain and enforce it; neither silently drops it.

The candidate gate passes three tests in Debug and ReleaseFast, including allocation
failure sweeps, invalid selected names, explicit-name noninterference and duplicate
primary rejection, malformed ownership and bounded record seeding. The runtime
error ABI gate passes 25 tests. The corrected
coordinator gate passes 29 tests. These are component results, not public ALTER
activation or original-case credit. The native HTTP catalog campaign exposed a
fixture which observed readiness but omitted durable activation before source
adoption; it now proposes and waits for that fence through production leader
admission. The corrected campaign and current facade/API compile qualification
are still running. Advertised capability stays 33 and the original inventory
remains 478 implemented / 136 rejected / 73 superseded / 899 unresolved.

### FK generation predicate retention

The successor FK generation plan now retains the captured relation guard and
selected FK name. Plan validation binds the guard to the exact predecessor
schema bytes/version, rejects non-FK or missing selected rules, and includes
the guard in the plan digest used by owner receipts. An absent guard retains
the predecessor JSON bytes and digest. The builder owns the guard, and SQL
passes it through the ordinary FK generation workflow rather than dropping
it or creating a separate publication path.

Read-only native preflight checks the covered namespace, predecessor owner,
policy state, complete child/parent descriptor cut and every owner lock. The
same read-only admission boundary runs again before begin's first write. A
defeated naming-enabled begin is a committed no-op with no lock or reservation;
errors after lock acquisition still abort. Missing records are distinguished
from actual storage failures. Invalid lock ownership and corrupt prior plans
are not optimistic conflicts. Existing pending-schema reservations then retain
the selected name until publication or completed cancellation.

Final single/batch proposal classification now recognizes the nested FK guard
and requires capability 34, including malformed non-null guard markers. The
fixed-memory wire gate passes in Debug and ReleaseFast. The focused FK gate
passes two tests in both Debug and ReleaseFast covering legacy byte compatibility, named digest/recovery,
invalid bindings and lower-capability begin nonreservation followed by legacy
admission, including rejection of corrupt zero-ID lock ownership. Current facade/API
qualification and the corrected native HTTP campaign remain pending; no public
activation or original-case disposition is claimed. Primary-key rewrite plan
integration, distributed race/fault qualification and capability activation
remain unfinished.

### Primary-key rewrite admission (implementation in progress)

The shared restore staging plan now retains the optional generated-name guard
for the one rewritten primary-key owner. Validation binds it to the predecessor
schema digest/version and selected primary-key rule; serialization omits an
absent guard. Both the frozen plan digest and pre-pin rewrite intent include
the guard, so source preparation cannot change the admitted predicate.

SQL performs a compact read-only name check before admitting a rewrite job.
The metadata owner loads its own predecessor instead of accepting a caller's
copy. Reservation repeats the predicate in the store transaction before the
first owner, range, name or job write. The existing restore reservation owns
the name afterward; no separate naming registry or restore subsystem is added.

The dedicated `antfly-rewrite-generated-name-admission-test` gate covers guard
serialization/recovery, intent binding and invalid predecessor/selected-rule
bindings. The initial plan-only gate passed in Debug and ReleaseFast. The
expanded Debug and ReleaseFast gates each pass two tests with no leaks, including native
lower-capability rejection of both direct staging and compound job admission.
It verifies no staging/job/replacement owner or active reservation, unchanged
source reconciliation state and predecessor, then successfully admits the same
rewrite without the new naming predicate as a positive control.

The focused coordinator Debug and ReleaseFast gates each pass two tests: single and batch
proposal admission reject naming-enabled restore bytes before append, even for
a malformed non-null marker; private name checks preserve exact naming and
extension-ownership diagnostics without treating unrelated failures as definite
rejections. The fixed-memory wire marker test passes in Debug and ReleaseFast.
Integrated qualification is pending. Concurrent name
acquisition, cancellation/recovery and failover qualification
remain required before capability 34 can be advertised. This checkpoint does
not change any original SQL case disposition or enable public generated names.

The PostgreSQL 18 catalog-lifecycle oracle also passes all 13 tests at this
checkpoint. The inventory verifier still reports 478 implemented, 136 rejected,
73 superseded and 899 unresolved original cases.

The follow-up is PR #1061 (after merged #1048). Main was merged again at
`0102c506d1`, containing upstream `121699433e`; in-progress edits were preserved.

### Occupancy activation qualification (local, unpublished)

The working tree now advertises decoder 34 for qualification; this activation
change is not yet published. A native activated-owner test passes in Debug and
ReleaseFast: a competing relation claims a prepared generated UNIQUE name,
read-only preflight rejects it, replicated apply commits a no-op without changing
the source cut, and explicit re-preparation chooses a suffix. The versioned
successor survives checkpoint/restart; a delayed rejected command at the next
applied index cannot overwrite it. The expanded native store gate passed 49
tests before the later rewrite-fixture additions.

Activated primary-key rewrite fixtures additionally exercise pending name
reservation, abort/first-job behavior, guard immutability during freeze,
restart and cancellation retirement. Their first run found that the older
fixture reused the existing sealed reconciliation job. It now allocates a
successor and performs the actual native CAS/retirement transition, preserving
typed coverage instead of resetting the existing job. The corrected Debug gate
passes all five focused tests with no leaks, including explicit pending-name
occupancy, release after cancellation, and rejection of a substituted guard
during freeze. The optimized rerun also passes all five focused tests, and the
native store rerun passes 49 tests with no leaks.

The public campaign bootstraps an independently verified namespace through
real leader/all-voter admission before its first anonymous CREATE. The public
catalog/index Debug gate passes 24 tests and one boundary audit; this establishes
declaration and metadata recovery evidence, not complete original-case DML
qualification. The broader coordinator rerun and mounted primary-key/FK
data-owner campaigns remain pending. Both mounted gates now fail on skipped
tests, so a denied loopback bind cannot masquerade as successful qualification.
The primary-key campaign retains the explicit-name cases and adds anonymous
success, NULL and duplicate rejection plus post-restart enforcement. The FK
campaign exercises SQL name preparation during the existing three-voter hidden
child leadership-transfer scenario. The new native named-FK gate
passes three tests in both Debug and ReleaseFast: it prepares the name from an attested namespace,
begins with typed child/parent schemas and exact metadata cuts, and preserves
both owner locks, the guard and plan digest through restart. Cancellation stays
pending across a second restart and retains the name and locks until real
owner receipts arrive. These standalone
native fixtures supply the capability barrier externally; they are not proof
of quorum failover or data-owner enforcement. Distributed ownership/failover
and data-owner activation qualification remain
required; no original-case dispositions change at this checkpoint.

The expanded PostgreSQL 18 oracle passes all 14 catalog-lifecycle tests. Its
anonymous primary-key cases independently verify NULL rejection (`23502`),
duplicate rejection (`23505`), unchanged source rows and absence of residual
indexes/constraints or NOT NULL changes after failure. Successful additions
choose the ordinary or collision-suffixed name, publish a ready/valid index,
and reject a later duplicate without changing the rows. This is oracle evidence,
not native/distributed original-case completion. The coordinator qualification
also asserts that index retirement and legacy-shaped source writes preserve
the activated occupancy floor instead of downgrading to the older mutation
protocol. The corrected coordinator Debug gate passes all 29 tests with no
skips or leaks; its optimized rerun remains pending. The mounted generated-FK
campaign returned HTTP 501 before publication admission (one capability test
passed, one integration test failed). Its response-diagnostic rerun is pending;
this is an unresolved public-path qualification failure, not successful
distributed activation. The mounted primary-key campaign is still live.

The primary-key campaign subsequently reached its first ALTER before the new
table's relation publication was ready. The server returned the explicit
pre-admission `53300`/503 catalog-proof diagnostic, not a mutation receipt. The
fixture now waits with a bounded read-only `resolve_many` ownership lookup,
validating the same relation proof ALTER uses. It does not replay DDL or fake
catalog activation. Its corrected mounted rerun remains pending.

The SQL CREATE adapter also still rejected every MATCH PARTIAL FK before
calling the shared initial-FK builder. That older guard is removed: the builder
already computes support definitions against a settled parent cut, and begin
owns all exact parent before/after pairs atomically with the hidden child and
name reservation. SQL must use that protocol, not publish parents separately
or keep the obsolete guard. The three-voter generated-FK campaign qualifies
this path; the current corrected run remains pending. MATCH PARTIAL is an
existing native extension, not a claim of PostgreSQL feature parity, and no
original-case disposition changes follow from enabling this route.
