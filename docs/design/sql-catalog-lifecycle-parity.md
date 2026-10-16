# SQL catalog lifecycle and original-case qualification

Status: draft follow-up PR #1061 to merged PR #1048, merged with `origin/main`
at `7aa031f49d` in merge checkpoint `8e962ee4e2`. Earlier implementation and
qualification checkpoints are retained below with their exact scope.
This is an implementation and qualification backlog, not
a completion claim. PostgreSQL is the SQL standard and independent test oracle.

## Baseline and priorities

The [original inventory](sql/parity-inventory.md) remains authoritative:
505 implemented / 132 rejected / 113 superseded / 836 unresolved. Counts below
are unresolved family pools, not promises that one implementation closes them.

| Priority | Pool | Cases | Shared delivery boundary |
| --- | --- | ---: | --- |
| 1 | DDL, catalog and sessions | 308 | Public relation ownership, schema/index activation and retirement, session lifecycle and recovery |
| 2 | Source/joined mutations and MERGE/recursion | 128 | Captured source ownership, bounded selection, atomic commit and distributed failure evidence |
| 3 | Read and query | 130 | Faithful typed fixtures, binding/correlation, endpoint results and diagnostics |
| 4 | Document writes | 70 | Document-specific schema/index profiles, complete postimages and integrity admission |
| 5 | Historically unsupported writes | 92 | Individual PostgreSQL contract review, implementations or proven required rejections |

## First delivery: catalog and index lifecycle

Within the 308-case DDL/catalog/session pool, the largest unresolved statement
groups currently include 38 ALTER TABLE, 27 CREATE FUNCTION, 26 CREATE INDEX /
CREATE UNIQUE INDEX, 19 CREATE TABLE, and 12 COPY cases. These are statement
counts, not implementation estimates: CREATE FUNCTION needs a separate routine
execution architecture, while ALTER TABLE and index/table DDL share the current
publication boundary. Finish and qualify that shared boundary as coherent public
case batches before expanding into another infrastructure-only subproject.
Original-case closure remains the progress metric; passing additional component
tests alone does not reduce the unresolved count.

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
in [its own design](sql/regex-parity.md), alongside this work rather than using
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

The first corrected mounted generated-FK Debug run passes both tests with no
skips or leaks. It exercises SQL anonymous-name preparation, atomic initial
support publication and the real three-voter hidden-child leadership transfer,
checking the persisted generated name and plan binding before and after
publication. The second current-source rerun also passes both tests with no
skips or leaks. Mounted primary-key qualification and optimized coordinator
qualification remain pending. This is distributed
FK-path evidence, not full original SQL-case parity or general activation
completion.

The corrected mounted PK run reaches successful explicit rewrite and the
expected NULL/duplicate terminal failures, but then fails in the anonymous
case's pre-rewrite source SELECT: the fixture still queried the already
rewritten `pk_good` instead of the current table. Both source SELECTs now bind
the case's actual name. Additional assertions check both columns of the complete
successful SQL postimage after publication and after restart, including after
rejected duplicate writes. Failed rewrites retain exact NULL/duplicate rows
and notes across restart. Only explicitly retryable `53300`/503 pure reads may
wait within a bounded interval; no mutation replay or suppression of other
errors is introduced. The expanded rerun remains pending, including the
post-publication SQL visibility check; it has not yet established that the
observed rewritten-owner read unavailability is transient.

The expanded PK check exposes a post-publication SELECT returning internal
`GroupLeaderUnavailable` as HTTP 500 even while duplicate writes are correctly
rejected by the new owner. SQL snapshot admission now reuses the shared
distributed-read availability classifier and translates only pre-write capture
availability into `SqlStatementReadUnavailable` (`53300`/503, retryable). It
does not classify unknown mutation outcomes as retryable or hide malformed
requests, authorization, cancellation, allocation failures or internal defects.
The dedicated `antfly-sql-read-admission-test` passes its Debug regression;
the optimized regression and mounted PK convergence rerun remain pending.
The discovery root explicitly imports the execution module, so this gate fails
on an empty test selection rather than appearing green without testing it.
The broader coordinator gate now passes all 29 tests in both Debug and
ReleaseFast, with no skips or leaks. Full activation remains unpublished until
the data-owner recovery/visibility campaign passes.

### Qualified recovery and main integration checkpoint

The final mounted primary-key campaign passes with zero skips, failures or
leaks. All six explicit/anonymous success, NULL and duplicate cases verify
complete source or successor postimages, enforcement and cold data-owner
restart. Pure-read retries are restricted to typed retryable availability;
each HTTP request is bounded by the remaining read deadline. Mutations are
not replayed.

The native SQL read-admission gate passes both Debug and ReleaseFast. Its real
adapter fault coverage includes compound statement capture, a retained single
scan and delayed guarded snapshot opening. Availability is normalized only at
read admission; unknown mutation outcomes, malformed responses and missing
range protection retain their original failure semantics.

The generated initial-FK three-voter campaign passes twice in Debug, and the
broader coordinator gate passes all 29 tests in both build modes. The PostgreSQL
18 lifecycle oracle passes all 14 cases. These are scoped qualification results:
the original inventory remains 478 implemented, 136 rejected, 73 superseded
and 899 unresolved, with no additional original-case completion claimed.

Main's standalone pgwire DDL publication/ownership fix (PR #1070) is merged
before publication of this follow-up. Its separate result and scratch
allocators are preserved through DDL dispatch, schema alteration and FK
plan validation. Generated names remain compiler provenance until native
owner-bound preparation and atomic admission; the older SQL-side name chooser
is superseded, not run as a second independent naming authority.

Qualification of the resolved merge passes: 708 native SQL contracts (three
benchmark-only skips, zero failures/leaks), 25 catalog DDL tests and their
selection audit, two read-admission tests, two standalone pgwire DDL tests,
the mounted six-case PK restart campaign and the two-test three-voter FK
failover campaign. Mounted campaigns have no skips or leaks. The SQL build
wrapper exits successfully after both selected passes. Its first rerun exposed
missing guidance on duplicate-index/constraint diagnostics; both now carry
actionable hints without changing SQLSTATE or non-retryable semantics.
The PostgreSQL 18 oracle passes all 14 tests again on the merged worktree.
The original parity inventory is unchanged; this is qualified scoped work,
not completion of the remaining SQL architecture.

### Shared mounted owner qualification for index lifecycles

The next delivery extracts the existing PK campaign's real metadata/data-owner
setup into `api/sql_owner_test_fixture.zig`. Campaigns borrow a context only
while both runtimes and their progress drivers are alive. The harness owns
startup, driver-error checks, orderly cold data-owner restart and teardown;
callbacks own their expected postimages and durable table identities. The
linked-control root is named for SQL owner lifecycles rather than one feature.
Existing PK qualification remains a separate, no-skips gate.

The new `antfly-api-sql-index-lifecycle-test` selects exact original statements
`sql-0692`, `sql-0706` and `sql-0705` from the pinned corpus, without editing
their SQL. Isolated namespaces retain all three policies across one restart.
Every source has a logical UUID primary key. Assertions cover complete typed
postimages, duplicate rejection with SQLSTATE 23505, included fields excluded
from the unique key, NULL policy, committed owner identity and enforcement after
cold restart. Only typed retryable pure reads can be repeated; mutation requests
are submitted once. HTTP requests and publication/activation waits are bounded.

The PostgreSQL 18 oracle passes all 16 lifecycle tests with the same owner
profiles and exact original index statements. Its failed-build contract also
checks that a regular failed CREATE UNIQUE INDEX leaves rows and the index
catalog unchanged before a deliberate repair and resubmission. This is an
oracle contract, not a claim of native failed-build cleanup qualification.

Mounted qualification exposed two production error-identity problems. The
runtime failure ABI now preserves CatalogOccupancyNotReady as an append-only
status rather than converting a known publication precondition into an unknown
mutation outcome. Physical snapshot opening for an already-bound logical table
classifies missing serving state as temporary read unavailability. Logical-name
resolution still preserves undefined_table, and corruption and unknown mutation
outcomes remain distinct; neither fix authorizes mutation replay.

The owner campaigns share one compiled linked-owner artifact and independent
no-skips run nodes, also wired into antfly-integration-test. Invalid-build
cleanup, metadata failover and concurrent DDL remain separate required
qualification boundaries, not implied by cold data-owner recovery. The original
inventory remains 899 unresolved; no disposition changes are claimed here.

Qualification checkpoint: the final combined Debug run passed both mounted
campaigns (six PK scenarios and three original unique-index profiles) and both
read-admission tests, with zero skips, failures or leaks. The DISTINCT profiles
also admit an additional NULL row after restart and verify the full postimage;
NOT DISTINCT still rejects it. Read-admission and runtime failure-identity gates
also pass in ReleaseFast; failure identity passes in Debug as well. PostgreSQL
18 passes all 16 lifecycle tests. This is scoped qualification, not a claim
that the full integration suite or every distributed fault boundary passed.

### Regular unique-index publication boundary

The PostgreSQL oracle's failed-build contract is now paired with a focused
mounted native gate, `antfly-api-sql-index-build-atomicity-test`. It is a
development regression, not a qualified CI gate or original-case credit. Its
first run observes duplicate input receiving HTTP 202 / DDL PENDING with a
committed schema receipt. A pending asynchronous response alone does not prove
a PostgreSQL semantic defect. The
strengthened run reached terminal invalid activation: complete SQL rows/column
metadata were unchanged, but the exact owner/query definition had changed.
Both runs failed the PostgreSQL atomicity contract, with zero skips or leaks.
Do not treat an asynchronous invalid-index declaration as regular PostgreSQL
DDL success. The production staged-publication fix is not implemented yet.

The required implementation is an index-only staged publication, sharing owner
fences, metadata authority, bounded scans and durable receipts with existing
integrity machinery. It must not use a full table/dependency-cohort rewrite or
restore an old schema in a late compensating API call. The former copies
unrelated primary data; the latter can overwrite a concurrent committed change.

1. Admit a durable metadata-owned plan with the exact logical/physical owner,
   predecessor schema and catalog digests, successor index definition, schema
   version, namespace name reservation and complete range-owner cut. The
   successor changes only the requested index and its integrity binding; row
   layout, existing constraint generations and FK definitions are unchanged.
   Reserve the index name atomically without exposing a queryable descriptor.
2. Obtain durable source fences from every owner using existing replicated
   topology-fence mechanisms. A regular build fences writes to this table;
   reads retain the old immutable schema and active index catalog. Do not
   enlist unrelated FK tables when their identities/constraints are unchanged.
   Mixed-version admission requires an explicit supported capability, not a
   best-effort owner loop.
3. Build into a plan-owned index generation and checkpoint namespace while the
   active integrity catalog remains unchanged. Decode through the pinned old
   row layout and project only key/expression/predicate dependencies plus
   covering payload fields. Existing unique generations need not be rebuilt.
   Use bounded pages and replicated row/version and progress CAS guards;
   acquire unique claims through the existing distributed coordinator, so
   duplicates on different owners cannot escape local validation. INCLUDE
   payloads never participate in uniqueness, and NULL policy belongs to the
   typed key encoder.
4. Collect exact owner-bound completion receipts covering the same range cut,
   plan digest and candidate generation. Only then may metadata atomically
   replace the public schema/index descriptor and consume its name reservation.
   Each owner installs the verified catalog/activation state and releases its
   fence in an atomic local transaction with its durable outbox record. Old
   readers keep their immutable schema views; no primary row rewrite is needed.
5. Before publication, deterministic failure enters durable cancellation.
   Reclaim only this plan's claims/artifacts, collect cancellation receipts and
   release reservations/fences. Never overwrite the active predecessor or
   delete another generation's claims. After proven cancellation, return 23505
   for duplicate data and allow an explicit repaired resubmission with the same
   index name. An uncertain cancellation retains its recovery handle and must
   not be reported as a safely retryable rejection.
6. Metadata restart, owner promotion and request cancellation resume from
   persisted phase and receipt revisions. Publication and cancellation must be
   mutually exclusive. Request deadlines bound waiting, not durable ownership:
   after uncertain admission the SQL API retains a typed receipt rather than
   inventing success, rejection, or replay authority. A synchronous PostgreSQL
   command completes only at ready or proven canceled; asynchronous maintenance
   receipts remain an explicit API distinction.

Required fault/scaling qualification includes cross-owner duplicate keys,
NULL/INCLUDE/partial-expression policies, concurrent name/schema publication,
promotion and restart in every phase, cancellation/publication races, repeated
cleanup, unchanged full postimages on rejection, and measured primary bytes
written versus projected index bytes. CREATE INDEX CONCURRENTLY needs its own
write-maintained candidate and snapshot/old-transaction barriers; a blocking
build with a different flag is not concurrent-index parity.

Generation reservation is also part of admission, not cleanup. A failed private
candidate must not let a later job reuse its generation. Reserve the candidate
allocation by advancing only the predecessor catalog's allocation high-water
mark, leaving its schema version, active/retired bindings and CHECK coverage
unchanged. First fence ordinary writes and drain predecessor participants; do
not invalidate an unresolved prepared writer by changing its catalog early.
Retain the original source-fence identity and separately attest the reserved
catalog digest. Cancellation must complete this fence/drain/reserve sequence on
every owner, including an owner that never received begin, before releasing the
cohort. Never rewind the allocator. Publication
installs the prevalidated candidate against the exact reserved predecessor;
rebuilding it with the ordinary allocator would silently allocate a different
generation. These transitions still require durable metadata authority and
owner receipts; a catalog helper alone does not authorize publication.

### Catalog preparation and candidate-validation checkpoint

Preparation now uses temporary kind-scoped name and allocation-membership
indexes for large catalogs, with a bounded linear path through 64 definitions.
Decoder allocation-membership scratch no longer lives as long as the immutable
catalog. Definition order, generation allocation, canonical encoding and
retired identities are unchanged. The allocation-free `indexAddition` check
requires exactly one successor UNIQUE generation and byte-identical existing
bindings, including FK payloads and retired descriptors. CHECK changes, owner
changes, schema/counter mismatches and implicit constraint retirement fail
closed. This primitive is not yet wired into a public staged-index controller.

All eleven direct catalog/contract tests pass in Debug and ReleaseFast, including
allocation-failure injection across both small and large membership paths.
Generation-allocation corruption is rejected even with valid physical checksums.
`prepareIndexReservation` burns the candidate allocation without publishing its
schema/binding, and `prepareIndexPublication` requires the exact reserved
predecessor before constructing the installation CAS. Tests prove a repaired
submission cannot reuse the canceled generation and a stale CAS cannot overwrite
a concurrent publication. These are catalog primitives, not metadata authority:
durable plan admission, all-owner burn/fencing, staged builders, receipt-driven
publication/cancellation and the public API integration remain unfinished.
Preparation also preflights the aggregate encoded byte budget before predecessor
decoding or owned payload allocation, and accounts for retained historical
descriptors before cloning them. A fail-at-first-allocation regression failed
with OutOfMemory before the fix and now rejects oversized input with
IntegrityCatalogTooLarge without inducing an allocation failure. Exact-limit
catalogs remain accepted; this closes allocation amplification without reducing
the supported catalog size.

The explicit `relational_catalog_benchmark.zig` harness is outside normal test
aggregates. ReleaseFast measurements compare the exact committed catalog at
`5cb3d7c73e` with the working implementation using identical source fixtures and
harnesses: 1,000 iterations per profile, three alternating before/after runs.
Final measurements include the bounded-allocation fix. Median microseconds per
preparation on the local macOS host (not an isolated service benchmark):

| Definitions | Mutation profile | Before | After |
| --- | --- | ---: | ---: |
| 64 | Retain all | 44.53 | 43.45 |
| 64 | Retire half | 35.47 | 37.19 |
| 256 | Retain all | 193.54 | 159.76 |
| 256 | Retire half | 130.61 | 123.95 |
| 1,000 | Retain all | 1,497.90 | 559.40 |
| 1,000 | Retire half | 803.29 | 451.82 |

This proves a catalog-preparation scaling improvement, not an end-to-end SQL
latency improvement, index-build throughput, or completion of staged publication.
The small retirement profile adds about 1.7 microseconds in this run; do not
describe the change as a measured speedup for every size/profile.
No original SQL disposition changes follow from these component tests.

The combined mounted Debug rerun passed the primary-key campaign but timed out
in the index campaign's attested absence read immediately after CREATE SCHEMA,
before CREATE TABLE. Its root cause is not established. Timeout-only diagnostics
now record the namespace, attempt count, last pending admission error and durable
reconciliation phase/coverage/failure without logging every poll or extending
the deadline. The instrumented index campaign and three subsequent bounded
repetitions passed with zero skips, failures or leaks. Those passing repetitions
do not resolve the earlier timeout or the separate failed-build atomicity defect.

### Bounded staged-index progress checkpoint

`relational_index_publication_progress.zig` implements the private progress
contract for a regular, index-only UNIQUE build. A fixed-size 184-byte head
tracks phase, revision and seven counters; each point-read owner record is
376 bytes. Receipt application is allocation-free and constant work, independent
of the admitted cohort size (bounded at 4,096 owners). It does not reparse or
rewrite the immutable plan on each ACK. `PreparedAck` owns its encoded buffers,
checks both exact durable predecessors before either write, and stages both
outputs in the caller's transaction. The caller must abort on a staging error
and include its outbox/receipt in that same transaction.

Publication requires every owner fenced/drained, every allocation reserved and
every private candidate validated. A distinct revision-CAS decision must switch
the public descriptor/name reservation in the same metadata transaction; after
that decision cancellation is forbidden. All owners install before any success
fence release. Prepublication cancellation instead requires cleanup/burn proof
from every owner, including unvisited owners, before any cancellation release.
Replays preserve counters; stale revisions, conflicting ACKs, substituted
owner/range/fence/candidate/reservation identities and regressing applied cuts
fail closed. Physical checksums detect damaged progress bytes, not forged
authority: the controller must independently verify owner evidence and immutable
membership from its admitted plan.

All nine direct tests pass in Debug and ReleaseFast. They exercise every one of
ten prepublication cancellation cuts, reversed receipt order, restart decoding
after every transition, publication/cancellation races, stale/conflicting ACKs,
dual-record CAS guards, storage-read error propagation, byte corruption and the
maximum owner count. The focused `relational-index-publication-progress-test`
build gate passes in Debug and is included in the storage DB and unit gates.
These are protocol/component tests, not distributed fault qualification.

This progress contract is not yet used by a production metadata controller.
Durable plan admission, name/table locks, capability barriers, verified owner
ports, projected private builders/unique claims and atomic public publication
still need integration. The native failed-build atomicity gate remains red;
there is no original-case disposition credit or claim of public SQL parity.

### Independent private UNIQUE candidate validation

`metadata/relational_index_candidate.zig` now prepares owned candidate and
reservation catalogs only after independently validating the entire schema
delta. Exactly one index and one `origin: index` UNIQUE rule may be added. All
other schema fields, including unknown future fields, CHECKs, defaults, generated
columns and existing declarations, must remain structurally unchanged. Object
key order is irrelevant; unrelated numerical/array rewrites are not authorized.
The schema epoch must advance exactly once, and external/document tables are
rejected before catalog preparation.

Physical and logical keys are independently compiled and compared using typed
equality identity, including expression/collation identity. Sort direction and
covering columns do not change uniqueness. Partial membership must have the
same compiled predicate identity on both sides. A matching name alone is not
sufficient. The supplied predecessor is independently reconciled against its
schema and required to remain byte-identical, retaining CHECK coverage, retired
bindings and previously burned generation allocations. The private reservation
changes neither the public schema epoch nor the active constraint bindings.

Four focused tests pass in Debug and ReleaseFast with zero skips/failures/leaks,
covering independent CHECK preservation, exact publication CAS preparation,
unrelated row/CHECK changes, incomplete pairs, constraint/PRIMARY/deferred
substitution, mismatched physical keys/predicates, stale predecessors, wrong
incarnations and generation non-reuse after cancellation. The focused gate is
also a dependency of `antfly-unit-test`; this does not claim that the full unit
aggregate has run. Inputs are length-bounded before parsing, and compilation is
per candidate, not per row. No SQL latency or throughput improvement is claimed.

This candidate validator is not yet called by production index admission.
The durable plan, complete owner cut, table/name locks, private projected build,
global uniqueness claims and atomic public publication still require integration.
The original SQL inventory is unchanged. The post-main native FK rerun passed
three of four cases: DROP reached ADD FOREIGN KEY but received HTTP 500 from
`MetadataLinearizableReadTimeout`. Thus the native gate remains intermittently
red; neither these component tests nor the main merge resolve that timeout.

### Versioned CHECK catalog identities for regular FK publication

The fail-first metadata regression reproduced a compiler mismatch: the owner
validator produced a nonzero CHECK fingerprint while metadata's catalog compiler
stored zero. This is also a prerequisite for staged UNIQUE publication, whose
candidate must preserve the predecessor's CHECK coverage exactly.

Canonical catalog compilation now uses the owner's CHECK layout and expression
compiler through `relational_declarations.checksFingerprint`. It compiles only
CHECKs, not a second full property/regex validator, and this helper's empty-CHECK
path allocates nothing. Regular FK producers mark new plans and every continuation
command with `catalog_coverage: checks_v1`. The plan and command markers must
agree, and an ACK cannot change a persisted plan's coverage. Owner source
decisions carry the same marker and independently derive the matching catalog.
Unmarked durable plans retain their predecessor derivation; omitted legacy
markers preserve exact plan/command JSON bytes and their existing digests.

CHECK-aware admission and planless ACKs require metadata decoder version 35,
including commands which also carry generated-name guards. The fixed-memory
classifier fails closed for malformed/future markers without materializing large
schema bodies. Ordinary metadata commands keep their existing decoder floors.
Before admitting a new regular FK plan, the builder requires explicit owner
support from every child/parent range identity; a missing capability is not
treated as support. Self-FKs reuse the same checked dual-role cut.

Proposal-time validation rejects new unmarked CHECK-bearing jobs without changing
the replay or continuation of already-committed plans.

The focused metadata gates pass seven tests in Debug and ReleaseFast, including
owner CHECK parity across schema epochs, retained UNIQUE generation identity,
byte-stable legacy serialization and the new decoder barrier. The standalone
wire classifier passes two tests in both Debug and ReleaseFast. These are not evidence that the
complete distributed workflow passed. The mounted CHECK-bearing FK campaign
initially failed three cases before FK admission: first CREATE received typed
53300 with an explicit no-schema-change-admitted diagnostic. The shared fixture
now establishes attested name absence before each CREATE, without mutation replay
or larger deadlines. That campaign passed all four cases once, but the final
combined Debug rerun passed only three: DROP timed out at its initial parent seed
write, before FK admission, with a data read-index timeout. Therefore mounted
qualification is not consistently green. Both DROP/TRUNCATE build aliases now
share one run node, and the combined invocation executed the four cases once.

Legacy initial-FK creation still uses its prior catalog derivation. It needs its
own versioned plan/provisioning/receipt integration before claiming CHECK-aware
initial-create parity. Already-admitted legacy CHECK-bearing jobs must not be
silently reinterpreted under new identities. No original SQL dispositions change
at this checkpoint, and staged UNIQUE publication remains unfinished.

### Shared table-cut exclusion and schema-only publication postimages

`metadata/table_lifecycle_lock.zig` centralizes the existing durable lock format
and namespace. FK generation reads/acquisition/release now use this shared
allocation-free point-record primitive; initial-support finalization retains its
existing exception. Release binds both job ID and lock kind, so another job kind
cannot release a lock merely by presenting the same ID. Unknown/malformed lock
values fail closed and storage-read failures propagate without writes.

An index-build lock kind uses that same exclusion key, rather than introducing
a parallel lock namespace which old schema/topology writers would miss. Real
metadata writer tests exercise ordinary upsert, compare-and-replace and read
schema finalization under both FK and index-build locks. This is not evidence of
index-job admission or checkpoint recovery: no producer admits the new kind yet.
Admission must first integrate the complete job/owner cut, namespace reservation,
decoder capability barrier and checkpoint source verification atomically.

Independent schema-only publication checks also now bind new `checks_v1` FK
plans to their entire table postimage. Table settings may not change, the
predecessor read schema must remain pinned, and the only schema delta permitted
is FK declarations plus version. Unknown future schema fields are preserved.
Legacy durable plans retain their previous validation/identity interpretation.
The structural JSON comparison is shared with private UNIQUE candidates.

The four lock tests, four candidate tests and six FK admission tests pass in
Debug and ReleaseFast with zero skips/failures/leaks. FK plan regressions reject
unrelated table metadata, missing/replaced read layouts, valid but unauthorized
row-type changes and additional CHECKs. An initial adversarial test incorrectly
used the invalid JSON-schema type `float`; it was corrected to the valid type
`number` before qualification. The final mounted CHECK-bearing FK campaign then
passed all four cases with zero skips/failures/leaks, including DROP/TRUNCATE and
cold-owner recovery. This qualifies that run, not a full native-suite or SQL
parity claim; the earlier intermittent metadata read timeout remains open.

### Immutable staged UNIQUE plan preparation

`metadata/relational_index_publication.zig` binds the exact schema-only before
and after table records, logical table guard, proposed namespace/index name,
owner catalog predecessor and complete ordered range cohort in an immutable
format-versioned frame. A counting writer enforces an 8 MiB escaped-frame limit
before allocating the encoded frame; incoming decoder bounds remain separately
required. Owner group membership is checked with a temporary hash map, and the
range cut must cover the whole keyspace without gaps or overlap. Physical
namespaces, admission epochs and every owner catalog digest must match the plan.
The ordinary deterministic schema/index derivation independently checks the
successor index catalog, not just its user-visible schema declaration.

Prepared output owns the immutable frame, candidate and reserved catalogs,
fixed-size progress head, per-owner progress records and reserved relation-name
descriptor. It retains no request-owned slices. Temporary validation allocations
are discarded before returning; nested catalog arenas use the stable caller
allocator, not a pointer to an arena object moved off the stack. Owner descriptor
hashes stream directly into a domain-separated hasher without allocating another
serialized copy for each range. These hashes bind identities, not authentication.

Four focused tests pass in Debug and ReleaseFast with zero skips/failures/leaks.
They verify owned lifetime after releasing input memory, byte-stable persisted
round trips, exact head/owner digest bindings, reserved-but-not-active names,
stale logical guards and owner catalogs, physical namespace substitution,
incomplete/gapped cuts, duplicate groups and valid disjoint multi-owner cuts.
The escaping-limit regression uses a failing allocator to prove rejection before
frame allocation. The control-source catalog is regenerated and its freshness
check passes. The focused gate is a dependency of the unit aggregate, but the
full aggregate has not been run for this checkpoint.

This is plan preparation, not durable metadata admission. The transaction
adapter must independently recheck live namespace/name authority, metadata and
owner capabilities, current table/range identity and conflicting jobs, then
atomically stage plan, head, owner records, shared table lock and pending name
through the verified catalog source/checkpoint machinery. Production owner
builders, distributed uniqueness claims and public publication also remain
unfinished. No original SQL inventory credit or latency/throughput claim is made.

### Durable private UNIQUE admission and namespace recovery

The private `begin_relational_index_publication` metadata transition now
independently prepares the candidate and rechecks the live logical table guard,
metadata incarnation, exact namespace, current table and complete range cut,
conflicting topology/backup/restore work and row-policy admission. It requires a
constraint-membership-capable live namespace root and a distinct metadata decoder
floor (36). This floor does **not** advertise native owner build/install support
or enable a SQL/API producer.

Admission stages the immutable plan, fixed-size head and ordinal owner records,
pending pointer and shared index-build lifecycle lock in one existing metadata
transaction. Pending names are derived through the ordinary successor `TableCut`
and published by the same journaled namespace writer as document/FK/restore
operations. Name collisions roll back all staged job records, the lock, source
clock and buffered effects. The old public table definition remains unchanged.
The index reservation is occupied but is not an active lookup result; an
index-origin uniqueness rule does not invent a separate SQL constraint name.

Immutable plan and pending-pointer changes participate in source epochs,
reconciliation input and semantic checkpoint fingerprints. Head/owner progress
is deliberately excluded from namespace sources. Checkpoint verification
independently recompiles each bounded plan, verifies its canonical frame digest,
candidate/reservation identities and complete owner cohort, recomputes progress
counters, and verifies lifecycle visibility and the exact shared lock. Reverse
record counts reject orphan/extra records without retaining a global job map.
Incoming snapshot validation uses the same private-index record/cohort verifier;
same-group corruptions are rejected before a staging snapshot can be published.
Nonterminal jobs also match the actual full table and range descriptors against
their locked plan cut. Terminal history deliberately skips these comparisons so
later legitimate DDL/topology is not frozen by a completed job. The decoded plan
is owned and reused for these checks rather than parsed a second time.
Global key scans also reject jobs and index locks in unadopted metadata groups.
All four new record classes are registered for Raft snapshots and standby replay.

Only stale table-guard/head CAS failures are expected no-ops during committed
replay, and only after the adopted namespace journal has rolled back storage and
buffered outcomes. Capability/coverage, policy, corruption and storage failures
remain errors. Fresh attempts CAS the exact terminal predecessor head; the same
job ID cannot be reused. Replacement trims superseded owner ordinals, and table
deletion removes the retained terminal job rather than accumulating job history.

The native admission regression exercises actual metadata transactions, reopen
and snapshot verification, a post-staging namespace collision, stale-CAS replay,
decoder downgrade rejection, unchanged public schema, and fourteen deliberate
recovery faults (missing/substituted head, owner, pointer, plan and lock records,
counter disagreement, orphan groups/locks and actual table/range substitution).

A separate real-storage component test alternates one/two-owner plans over 32
terminal replacements, verifies exact retained cardinality, rejects stale heads
and reused job IDs, and verifies complete terminal-job deletion. Synthetic
kernel cleanup receipts exercise storage/CAS only; they are not evidence of
authenticated native owner cleanup or a complete distributed churn workflow.
It also accepts a changed current table and absent historical ranges after
terminal completion, qualifying the retained-history exemption.

Review also reproduced a borrowed-result lifetime bug in `PreparedAck.stage`:
reading the owner could overwrite the head buffer before comparison. A mock
reader that reuses one scratch buffer made the existing staging regression fail.
Staging now consumes each predecessor before the next read; both comparisons
still precede either write. It retains allocation-free, two-point-read/two-write
behavior and exact replay/CAS semantics.

Final qualification passes all six targeted gates in both Debug and ReleaseFast:
`antfly-relational-index-publication-test`, `antfly-table-lifecycle-lock-test`,
`antfly-relational-index-candidate-test`, `antfly-fk-generated-name-admission-test`,
`relational-index-publication-progress-test`, and
`system-catalog-relation-store-test`. The native runs report eight publication
tests plus one decoder-floor test, four lock tests, four candidate tests, six FK
tests and fifty existing namespace/recovery tests, with zero skips/failures/leaks.
The allocation-free progress gate passes all nine tests. These are gate
execution counts, not unique inventory credits or a full native-suite claim.
Formatting, diff checks and control-source catalog freshness also pass. The
PostgreSQL oracle and public SQL failed-build atomicity gate were not rerun for
this metadata-only checkpoint.

Remaining activation work is still substantial: independently attested native
owner capability/catalog/epoch admission, private projected builders, globally
coordinated UNIQUE claims, authenticated owner receipts, the durable worker and
publication/cancellation continuations, and the public SQL producer.
Independent recovery proof of the complete current range cohort and live
namespace/incarnation bindings needs qualification before producer activation.
The known regular `CREATE UNIQUE INDEX` atomicity regression remains open; this checkpoint
does not credit any of the original 899 unresolved inventory cases or claim
distributed build throughput, complete DDL churn qualification or public parity.

### Bounded admission and owner lookup

Admission now stops range-ID enumeration after the expected cohort plus one
rejecting row, bounding allocation by the submitted cohort rather than a stale
table's complete current range list. A failing-allocator regression proves that
an empty expected cohort rejects its first row without allocation; cursor read
failures remain visible.

Private index jobs now persist a group-to-owner-ordinal directory in the same
transaction as their plan and owner records. Dispatch can resolve an owner with
a fixed-size point read without reparsing the plan. Snapshot/checkpoint
verification checks every expected mapping and rejects missing, substituted or
extra directory records. Terminal replacement and deletion remove old mappings;
the 32-attempt retention test checks both owner and directory cardinalities.
These records are not independent authority or namespace-clock sources.

Debug and ReleaseFast qualification pass nine publication tests, one decoder-floor test and
fifty namespace/recovery tests, with no skips, failures or leaks. The recovery
fault set now includes seventeen variants. Reverse verification of *all* live
ranges, native owner activation and the public SQL atomicity fix remain separate
unfinished work; this checkpoint does not claim those capabilities or inventory
credits.

### Complete current-range recovery membership

The forward owner proof alone did not establish that the immutable cohort was
the complete current range set. A new fault regression first reproduced a
checkpoint accepting an additional live range while every declared owner still
matched its descriptor.

Recovery now performs a reverse range-stream pass when a nonterminal private
index job exists. Each range belonging to a live job must resolve through the
fixed-size directory to an owner with the same group, ordinal, plan ID and plan
digest. Canonical range key/value identity is checked as well. The existing
forward proof continues to compare full range descriptors. Together they reject
missing, changed, extra and aliased live ranges without storing a whole-catalog
map or decoding a publication plan per current range.

The reverse operation takes no allocator and performs three fixed-size reads
for a live owner. A scratch-buffer-reusing reader regression checks that each
borrowed result is consumed before the next read and that storage failures stay
visible. Terminal history requires only its head read and does not pin later
topology. Snapshots/checkpoints without active private jobs skip the extra pass;
snapshot cancellation is checked during it. This is offline metadata recovery
verification, not a user-row scan or a new request-path operation.

The recovery fault campaign now includes nineteen variants. Native owner
capability/epoch attestation, live namespace/incarnation recovery binding,
private builders, distributed uniqueness claims and atomic public publication
remain unfinished. No inventory disposition changes are claimed.

Qualification passes both targeted gates in Debug and ReleaseFast: ten
publication tests, one decoder-floor test and fifty namespace/recovery tests,
with zero skips/failures/leaks. The added extra-range regression failed against
the predecessor implementation and passes with the reverse proof; the aliased
range fault also passes in both checkpoint and incoming-snapshot verification.
Formatting, diff and control-source catalog freshness checks pass. These are
metadata recovery results, not public SQL atomicity or full-suite qualification.

### Live logical authority on recovery

A fail-first regression constructed a self-consistent private plan/head/owner
set with an old metadata incarnation. Immutable-frame and physical table/range
verification accepted it. Nonterminal recovery now compares the plan guard's
incarnation with the current durable metadata epoch and resolves the namespace,
database and table resources by stable IDs. It verifies logical names, parent
bindings and the unchanged physical storage name. These are bounded point
reads once per job, not namespace-name scans or per-range plan parsing. The
admission root's old revision is deliberately not required: staging the job
itself advances the namespace source revision.

Default database/public namespace and legacy unbound public table semantics
match admission. Explicit nondefault resources must exist and match; missing
resources are not treated as defaults. Completed job history remains exempt
from current incarnation/catalog-binding checks, just as it is exempt from
later schema/topology changes.

Five new fault variants use a stale plan incarnation, a foreign namespace ID,
or independently rebound table/namespace/database resources. The stale-plan
variants recompute all private frame/head/owner digests, so this exercises live
authority rather than another checksum mismatch. Native checkpoint and incoming
snapshot paths share the verifier. An unpublished native storage fixture also
accepts an explicit nondefault database/namespace and a logical table name
different from its physical name; full namespace-registry recovery remains
separately covered by the surrounding committed campaign.

Native execution remains the next architectural dependency. Existing
`relational_index_jobs.Page` acquires the published write snapshot and commit
requires `isCurrent`; relaxing that check would not grant private-job authority.
Private execution must instead pin a candidate plan independently and recheck
its exact durable source fence, job digest, reserved catalog and maintenance
predecessor when committing a bounded projected page. It should reuse the
existing historical-schema projection and primary-value CAS machinery, without
publishing the candidate write/query snapshot. Generation reservation, progress
and outbox must share a transaction, and activation still requires independently
attested owner support plus distributed UNIQUE validation and a durable metadata
publication decision. None of those owner operations are enabled by this
recovery checkpoint.

Qualification passes the two targeted native gates in Debug and ReleaseFast:
ten publication tests, one decoder-floor test and fifty namespace/recovery
tests, zero skips/failures/leaks. The expanded recovery campaign covers 24 fault
variants. Formatting, diff and control-source catalog freshness checks pass.
These scoped results do not establish native owner support, public UNIQUE
atomicity or completion of any additional SQL inventory case.

### Shared native candidate compilation and physical allocation

Private index candidate validation now lives in the native storage layer.
Metadata uses a thin alias to the same implementation. CHECK-aware/legacy
catalog compilation and bounded structural JSON equality are also shared;
coverage tags and serialized identities retain their prior semantics. A
standalone native test root directly imports the compiler without server
metadata imports. The candidate gate is included in the storage/unit aggregates.
The four original candidate cases moved with the implementation; an additional
ownership test frees all request input buffers before inspecting and preparing
publication from the resulting catalogs. Running them in two contexts is not
two sets of SQL inventory credits.

`relational_index_catalog.PrivateAddition` prepares an owned, unpublished
physical plan and two catalog stages. Reservation preserves the old schema,
definition blob and existing tuple IDs, but advances the durable head revision
past the new candidate's physical generation. The candidate installation stage
CASes against that exact reserved head. A canceled attempt retains the head
revision, so a retry cannot reuse its tuple IDs even if cleanup is delayed.
This uses the existing catalog format rather than a separate allocation service.
First-index bootstrap reserves an empty active catalog, not a prematurely
visible candidate definition.

Preparation rejects an invalid schema epoch, a missing/extra addition, or a
change/removal of existing physical bindings. It retains the candidate schema
view and executable plan independently of request memory. Legacy native UNIQUE
descriptor flags remain unsupported by ordinary write plans; SQL uniqueness is
still the separate routed integrity-rule responsibility. No guard was relaxed
to bind private tuples.

Real memory/LSM transaction tests cover reservation/outbox abort, exact replay,
unchanged public definitions and schema, first-index bootstrap, canceled retry
generation nonreuse, stale reservation/installation rejection and component-only
installation from the reserved predecessor. Equal catalog bytes are replay,
not authenticated job ownership: the future owner operation must independently
check its durable job/fence before staging both integrity and physical burns,
the job record and outbox in one transaction. It must refresh the active runtime
snapshot head after commit under the same apply barrier. This preparation API
does not enable private execution or grant public publication authority.

Remaining native work includes the durable fenced owner binding/receipts,
private projected-page execution, distributed UNIQUE claims, authenticated
metadata decisions, bounded cancellation cleanup and the public SQL producer.
The known failed regular UNIQUE-build atomicity regression remains open.

Qualification passes all six focused gates in Debug and ReleaseFast: seven
native candidate/private-addition tests, thirteen native catalog tests, five
metadata candidate tests, six FK tests, ten publication tests plus one decoder
floor test, and fifty namespace/recovery tests. Each run completed 92 build
steps with zero skips, failures or leaks. Shared cases execute in multiple gates;
these are not additional SQL inventory credits or a full-suite qualification.

The private-addition test exhaustively exercises allocation failures. A
pre-existing catalog allocation campaign was nondeterministic in ReleaseFast
because allocator remapping changed allocation counts. It now uses the existing
platform stable allocation-failure helper, as the adjacent publication campaign
already does. Successful normal-allocator coverage, OOM propagation and leak
assertions remain intact; no test was skipped or weakened.

### Restartable unpublished physical plans

`PrivateAddition.stageReservation` now retains the immutable candidate definition
blob in the same transaction as the physical generation burn. It does not select
the candidate head. Its caller must still stage the authenticated owner binding
and outbox in that transaction and abort on any error. Conflicting bytes at the
content-addressed candidate key are rejected rather than overwritten.

`PrivateAddition.reopen` reconstructs an owned plan using a single read snapshot
and expected heads/name supplied by the future durable owner binding. It checks
the exact active reserved head, the next schema epoch, absence of the private
generation from active bindings, canonical candidate bytes, unchanged existing
bindings, and independent recompilation to the same candidate head. This is a
cold restart operation, not a per-row or per-page parse. No job authentication or
publication authority is inferred from matching physical bytes.

Memory/LSM transaction coverage verifies that abort removes both the burn and
private blob, bootstrap keeps public definitions empty, reload owns its data
after the read transaction closes, and a higher reservation rejects old reloads.
An additional actual LSM close/reopen test discards the prepared object, rebuilds
from durable definitions, exercises every allocation failure, rejects missing or
corrupt private blobs, and checks idempotent reservation and component-only
installation. Candidate-blob cancellation/GC must join the future authenticated
owner cancellation transaction; retaining the blob does not yet enable a native
build worker, public SQL producer, or distributed UNIQUE enforcement.

All six focused gates pass in Debug and ReleaseFast after this change: eight
native candidate/private-addition tests, fourteen native catalog tests, five
metadata candidate tests, six FK tests, ten publication tests plus the decoder
floor test, and fifty namespace/recovery tests. Each mode completed 92 build
steps with zero skips, failures or leaks. Formatting, diff and control-source
catalog freshness checks pass. Repeated cases across gates are not separate SQL
coverage credits; the original inventory and open public atomicity case remain
unchanged.

### Durable native owner admission and dual reservation

The native owner now has a fixed-size, checksummed 656-byte private job binding.
It retains the full job ID, immutable metadata-plan and owner digests, complete
topology fence, owner ordinal, schema identity, integrity-catalog identities,
original optional physical predecessor, reserved/candidate heads and tuple ID.
The job key is protected by ordinary extra-metadata admission. Matching hashes
remain identities, not authentication of a metadata decision.

Admission stages the job and dedicated `index_build` source fence in one caller
transaction after checking the actual predecessor schema and both catalogs.
The existing topology machinery closes new participant admission while old
participants finish. Reservation independently checks that exact durable job and
fence and requires the existing transaction manager to prove the predecessor
drained. Both integrity and physical allocation burns, retained private
definitions and the job phase then stage in one transaction. The caller must
independently attest metadata/local range identity and add its durable outbox
before committing; these primitives do not provide a public command or receipt.

Logical and physical preparations also bind the same canonical declaration
fingerprint and native schema identities. The declaration hash streams directly
into Blake3 without allocating an intermediate JSON copy. A different but valid
physical plan cannot be substituted for the independently compiled UNIQUE rule.
Cold owner recovery checks the durable binding/fence and reserved integrity
catalog, reconstructs the owned private plan, and preserves whether the physical
predecessor was originally absent (including first-index bootstrap).

Generic topology begin/release/cancel cannot acquire or lift index-build
authority. An index fence with a missing or mismatched durable job fails closed.
No owner capability or transport handler advertises these new operations yet.
Authenticated command/receipt integration, phase-aware checkpoint/snapshot
verification, private projected execution, distributed UNIQUE claims, publication
decisions and bounded cancellation/GC remain required before public activation.

The native memory/LSM campaign exercises admitted old transactions, blocked
reservation before drain, new-write exclusion, begin/reserve rollback and replay,
foreign jobs, missing/mismatched fence bindings, substituted physical definitions,
failure of the second catalog CAS after the first burn is staged, and owned
recovery from the reserved binding. This is component-level storage evidence,
not authenticated distributed execution or a new SQL inventory credit.

Qualification passes all six focused gates in Debug and ReleaseFast: nine
native candidate/private-addition tests, fourteen native catalog tests, six
metadata candidate tests, six FK tests, ten publication tests plus the decoder
floor test, and fifty namespace/recovery tests. Each mode completed 92 build
steps with zero skips, failures or leaks. Formatting, diff and generated control
catalog checks pass. Shared cases execute in multiple contexts; this is not a
full-suite or public distributed-activation claim. SQL inventory dispositions
remain unchanged.

### Shared bounded private backfill pages

Private candidates now reuse the existing typed-row page preparation, historical
schema projections, covering/partial plans and primary construction. Public
online rebuilds retain their forward/reverse verification. The private iterator
pins the owned unpublished candidate rather than acquiring the public controller
snapshot. It retains the same row/byte/time budgets; no row is materialized as JSON
or rewritten merely to construct the index. Progress cursors are owned before
subsequent store reads can invalidate borrowed buffers.

Private preparation/application checks the durable owner job, exact source
fence, reserved catalog identities and actual durable namespace. Application
also checks the namespace generation, the reserved lease established after drain,
maintenance control, progress CAS and each observed source value. A changed or
missing source invalidates a private page instead of advancing past it: unlike
a published write-maintained index, the private frozen candidate cannot rely on
an ordinary writer to fill that coverage gap.

`Page.stage` lets the ordered owner commit page writes/progress with its receipt
and outbox in one transaction. Private pages cannot use the local `commit`
convenience. Public pages retain their published-snapshot checks and existing
local commit path. Physical build completion does not authorize UNIQUE
validation, metadata publication or fence release.

The memory/LSM owner campaign now constructs and verifies private tuples from
old-epoch rows with one-record budgets. It covers abort, consumed/stale page
rejection, changed namespace generation, durable namespace substitution, changed
first/second source values, competing progress CAS, historical projection and
unchanged primary bytes/public index definitions. The explicit pages gate joins
the native artifact to four public-owner regressions: bounded build pages,
pre-generation writer drain, prepared tuple reservations and ordinary tuple
replacement. Its test-selection inventory remains strict; native-only cases
are not represented as filters in the server-owned test root.

Remaining activation work includes authenticated native commands/receipts and
capability barriers, phase-aware checkpoint verification, supervised scheduling,
distributed UNIQUE claim construction/validation, publication decisions and
bounded cancellation/GC. The public failed-UNIQUE-build atomicity case remains
open; this component execution does not add SQL inventory disposition credits.

Predecessor transaction-history reconciliation remains a reservation-time
operation, not per-page work. The durable reserved phase plus the exact retained
admission fence proves that drain remains valid; pages do not rescan retained
terminal history under apply-exclusive. The constant-work pending-intent probe
at physical completion remains intact. Ownership hashing reads its borrowed range
bytes last, after decoding the fixed-size namespace, and hashes them before any
further store operation.

Before public activation, read-only participant admission/write upgrades also
need explicit treatment: regular PostgreSQL index creation must permit readers
without allowing a read-only predecessor to upgrade into an unfenced writer.
The current conservative topology admission is not a claim of that final UX.

Fresh private construction completes at primary EOF without the public online
rebuild's two generation-local repair scans. This relies on a newly allocated,
never-reused tuple generation, a drained and continuously fenced source, strict
source-value CAS and atomic tuple/progress writes. Private non-primary progress
is rejected; this is not a shortcut for public rebuilding/repair. Completion is
still physical coverage only, not UNIQUE validation or a publication decision.
The fail-first work counter examined nine records for three indexed source rows;
the single-pass implementation examines exactly three while preserving primary
bytes, unpublished definitions and all source/job/progress fault guards. That is
a scan-work result for the fixture, not a wall-clock throughput claim.

Final-source qualification ran `antfly-relational-index-pages-test` and
`antfly-relational-index-candidate-test` in Debug and ReleaseFast. Each run
completed 62/62 build steps: four public regressions, nine native candidate/job
tests and six metadata candidate tests, with no failures, skips or leaks.
These overlapping component executions are not additional SQL disposition
credits. The earlier seven-gate campaign passed in both modes before the final
localized single-pass changes; it is not claimed as a full-suite rerun of this
checkpoint.

### Phase-aware native private-job recovery

The shared physical snapshot loader now verifies retained private jobs in the
same read transaction before binding or publishing its public execution plan.
Startup, runtime catalog refresh and restore preparation use this boundary.
An index-build fence without its job, or a job without its exact fence and
durable namespace, fails closed. A generic completion receipt cannot silently
release a private build.

Fencing jobs require the original integrity catalog, schema and optional
physical predecessor; reserved jobs require the allocation-burned catalogs and
the retained canonical candidate blob. Recovery proves that the candidate adds
exactly its bound generation while preserving every old physical ID, comparison
identity and declaration. It validates private progress ownership and the
primary-only construction phase without scanning primary rows or rebuilding
historical schema registries. Parsing is bounded by the existing catalog limits
and occurs only at the cold snapshot-loading boundary, not per row or page.

Memory/LSM campaigns exercise bootstrap and already-indexed owners, before
reservation, before the first page and after physical completion. Snapshot-load
faults cover missing jobs/fences, namespace replacement, corrupt catalogs,
generic completion, foreign/non-primary progress and missing/corrupt candidate
definitions. Exhaustive allocation-failure checks cover the cold verifier.
These are local structural recovery guarantees, not authenticated metadata
decisions, native receipt verification or distributed activation evidence. The
public failed-UNIQUE-build atomicity case remains open and inventory dispositions
are unchanged.

The PR's failing license-boundary workflow was reproduced locally: eleven new
files had incomplete or wrong-package notices. The repository normalizer now
applies its existing package rules to those files, without adding exceptions or
weakening traversal. Header validation and the full Apache product boundary
(2,026 source files) pass, as do all 55 boundary/header/provenance/asset unit
tests. Server-owned modules remain ELv2; shared embedded implementations carry
the required Apache notices.

Final normalized-source qualification passes in Debug and ReleaseFast:
`antfly-relational-index-pages-test`, `antfly-relational-index-candidate-test`
and `relational-index-catalog-test`, with 65/65 build steps in each mode. Each
executes four public regressions, nine native candidate/job tests, six metadata
candidate tests and fourteen catalog tests, with zero failures, skips or leaks.
These component selections overlap and are not new SQL case credits or a
full-suite qualification. Formatting, diff and control-catalog freshness checks
also pass; latest fetched `origin/main` remains included.

### Typed, root-authenticated metadata owner acknowledgements

The private progress contract now pins the entire document-identity namespace
and receipt authority (native or Raft) in each admitted owner. Native receipts
carry a namespace-bound commit sequence; replicated receipts carry term/index.
The first receipt cannot choose its own authority, and subsequent transitions
reject namespace changes, mixed position domains, non-increasing positions and
regressing Raft terms. Exact action replay preserves the latest position.

The new PR-local owner codec is AIPO v2, 424 bytes, including an explicitly
encoded position and checked zero padding. The 184-byte head is unchanged.
This replaces the earlier 376-byte private format described above; there is no
deployed-main migration or legacy decoder. The public producer is still disabled.

`ack_relational_index_publication` is a bounded, planless metadata log command
(tag 79, coordinated decoder floor 36, maximum JSON payload 4 KiB). Preflight
and committed apply independently verify the admitted active job, table lock,
owner-group ordinal directory, head revision and exact owner descriptor. A
domain-separated root signature binds the logical receipt, root enrollment,
reporter incarnation, replica identity and physical root generation. Apply
also point-reads the enrolled root, current store header, metadata incarnation,
live range namespace, serving placement and addressed store-report slot. Raft
receipts require an observed applied cut; native sequences are never compared
with Raft indices. Report selection reuses the sparse report machinery: one
directory lookup and at most four page reads, decoding only that owner's slot.
This is a code-path bound, not a measured end-to-end latency improvement.

Only head and owner progress records change. Both exact predecessors pass
before either write, and storage errors during staging abort the transaction.
Losing pre-write revision/job or current-owner checks are safe committed no-ops;
preflight exposes the conflict. Malformed commands, missing decoder barriers,
corrupt records and I/O errors are not suppressed. The immutable plan, public
schema and namespace source revision are not rewritten per receipt.

Initial-child FK root attestations use the same signing implementation with
their original signature domain and unchanged byte transcript. An independent
reference-transcript test protects that existing contract; cross-domain replay
is rejected.

The new real-metadata campaign covers both position domains, enrollment,
reporter/root/placement/range substitutions, unchanged progress on rejection,
stale-CAS replay, exact retry, command encode/decode, unchanged namespace source,
and persisted receipt positions after reopen. This is metadata-adapter evidence,
not proof that a native worker actually fenced/built/cleaned an index. Native
ordered command execution, global UNIQUE claims, publication/cancellation
continuations, public SQL activation and distributed fault qualification remain
unfinished. The original SQL inventory is unchanged.

Qualification completed on the frozen implementation in Debug and ReleaseFast.
The focused Debug build passed 89/89 steps; the separate full catalog/store
gate passed 204/204 tests. The optimized combined build passed 95/95 steps,
including that full catalog/store gate and all thirteen typed-progress tests.
Its standard-runner summary is 217/217 tests; the custom-runner artifacts also
report twelve publication tests, one decoder-floor test, nineteen initial-FK
tests, one placement test and fifty namespace/recovery tests, with no skips,
failures or leaks. These overlapping selections are not SQL inventory credits.
Formatting, diff, control-catalog freshness, license headers and the Apache
boundary pass; the boundary/header unit suites pass 35 and 11 tests respectively.
The inventory integrity checker still reports 478 implemented, 136 rejected,
73 superseded and 899 unresolved original cases. Latest fetched `origin/main`
at `2afbdd306c` is fully included. The public SQL atomicity gate was not rerun
for this metadata-only checkpoint and remains an open activation requirement.

### Native owner effect cuts and recovery

The local private-index binding now pins the receipt authority as well as the
namespace. Its PR-private AIOJ v2 encoding remains 656 bytes; the new authority
byte is checked, padding is zero, and the prior unpublished v1 is rejected.
Provisioned native ownership cannot be inferred from a request or substituted
for Raft ownership, and a native owner rejects a retained Raft apply marker.

Admission, drained-fence sealing and dual-catalog reservation now each stage a
native clock advance and a fixed-size receipt in the SAME storage transaction
as the effect. Admission alone is deliberately not a fenced acknowledgement:
the seal requires the actual transaction manager to report all predecessor
participants drained. Reservation requires that durable seal before burning
either catalog allocation. Aborted effects leave neither a receipt nor a clock
advance; exact retries return the original cut without consuming a new sequence.

There are three reusable 136-byte effect slots, not an append-only per-job
history. Each binds the immutable job context, action, evidence and typed native
namespace/sequence, with a physical checksum. Old-job slots are inert and are
replaced by the next admitted job. These hashes are not authentication. Returned
staged receipts must not be signed or sent until the enclosing transaction and
its durable replication outbox commit successfully.

Cold recovery verifies admitted/fenced/reserved cut ordering against the durable
native clock, provisioned authority and namespace, in addition to the existing
schema, catalog, private definition and page-ownership checks. The recovery fault
campaign now includes missing cuts, clock rollback, authority substitution,
cross-namespace positions, zero sequences, forged evidence, wrong actions,
noncanonical padding, mismatched contexts, checksum errors and invalid fence
ordering. Most malformed-byte cases deliberately retain a correct physical
checksum. Corrupt persisted records are injected below document-store guards;
a separate assertion verifies that ordinary writes reject the forbidden Raft
marker before recovery is exercised. Memory and LSM backends exercise the same
campaign, alongside allocation-failure, abort, retry and drained-participant tests.

Live receipt production and page admission share the same cut-order checks as
cold recovery. A prepared page cannot write after any required receipt vanishes,
even if its controller was opened before that corruption. The guard also checks
the durable schema and provisioned authority. It reads the native clock once
for the three fixed-slot checks and does not revisit transaction history.

This completes local effect staging for admission/seal/reservation, not the
distributed worker. Ordered DB/C-ABI/RPC commands, forwarded standby positions,
outbox/replay integration, independently fetched metadata owner decisions,
page/validation/install/cancel/release continuations, global UNIQUE claims,
public SQL activation and end-to-end distributed faults remain unfinished.
The original SQL inventory dispositions are unchanged.

Qualification on the final frozen native-cut implementation passes all 65 build
steps in both Debug and ReleaseFast for `antfly-relational-index-candidate-test`,
`antfly-relational-index-pages-test` and `relational-index-catalog-test`. The
custom artifacts report four native system/page tests, nine candidate/private
addition tests, six overlapping candidate tests and fourteen catalog tests,
with no skips, failures or leaks. Earlier interrupted optimized compilation is
not counted as a passing gate. Formatting, diff checks, license headers, Apache
boundary and control-catalog freshness pass. The inventory checker remains
478 implemented / 136 rejected / 73 superseded / 899 unresolved; these native
component tests do not qualify the still-open public SQL atomicity gate.

### Bounded immutable worker inputs and live owner permission

Private admission now materializes one shared input manifest per table/job and
one compact immutable dispatch projection per admitted owner. The existing
two-byte owner-group ordinal directory is unchanged. Schema/catalog JSON and
table descriptions appear in the shared input, not in every owner projection;
the full original plan remains the canonical cohort/checkpoint source. All rows
commit with admission. Terminal replacement trims the prior dispatch cohort,
and terminal DROP removes the shared input and owner projections too.

The two new record classes participate in metadata snapshot command masks,
forward canonical verification and reverse orphan counting. Recovery compares
their exact canonical bytes with projections rebuilt from the original plan.
Projection strings are owned before another transactional read can reuse its
borrowed buffer. Admission rebuilds the typed candidate with the same validator
as original plan preparation, and checks both the candidate and reservation
fingerprints against the independently fetched head. A copied plan ID is not
sufficient evidence of valid worker inputs.

`relational_index_owner_decision` is an identity-only private read: table, owner
group, job ID, requested operation and permission/admission projection. Trusted
metadata ingress requires an authenticated service and a body-bound read grant;
the request cannot set its authorization context. Service execution uses a fresh
linearizable metadata read. The storage adapter checks the decoder floor,
current head, directory, owner progress, active-pointer phase, lifecycle lock,
complete addressed range descriptor, stable namespace/database/table bindings,
catalog coverage and current indexed schema claim in one snapshot. Permission
reads do not parse the original owner cohort or schema-sized input manifest.
Admission projection is only valid for fence/cancel operations; it additionally
fetches the shared input once.

An already acknowledged operation returns `acknowledged`, with no dispatch or
input and no authority to execute again. An executable operation returns only
its current owner projection, plus inputs when admission was requested. Phase
permission is not a lease: ordered data apply still checks the local job, fence
and page cursor, and signed metadata ACK apply rechecks its current head/root
authority. Publication removes the pending-name pointer while retaining the
lifecycle lock through install/release; both ACK and read dispatch now use that
same phase-sensitive active-pointer rule.

The storage tests cover projection absence/corruption/orphans in checkpoints
and incoming snapshots, terminal retention, borrowed-read lifetimes, allocation
failure cleanup, typed candidate fingerprint substitution, and permission
versus acknowledged retries on native and Raft owner cohorts. A structural
size test uses a 64 KiB table description: shared inputs exceed 128 KiB while
the owner dispatch stays below 4 KiB. This is a representation/read-work bound,
not a measured distributed throughput or latency result. Component qualification
passes for this checkpoint; the public producer remains disabled.

Final Debug qualification passes 92/92 build steps and 606/606 standard-runner
tests across the publication, namespace/recovery, catalog store, catalog API
and complete catalog transport gates. Custom artifact selections also pass
17 publication/permission tests, the decoder-floor test and the overlapping
namespace/native selections without skips, failures or leaks. The transport
gate includes the actual metadata handler's exact-body owner-grant test:
missing service/read grants and reusing a fence grant for a cancellation body
are rejected before the callback, while the valid grant sets only the private
index authority. ReleaseFast also passes 92/92 build steps and 606/606
standard-runner tests on the same frozen checkpoint, with all custom artifact
selections passing without skips, failures or leaks.
The earlier narrow mock error-set compile failure and sandbox-denied socket
binds are not counted as passing runs; storage error propagation and loopback
tests were retained rather than weakened. Formatting, diff checks, license
headers, Apache boundary and control-catalog freshness pass.

Native ordered DB/C-ABI/RPC effect commands, standby forwarded positions and
atomic outbox/replay, global UNIQUE claims, build/validate/install/cancel/release
continuations, public SQL activation and distributed fault qualification still
remain required. The original SQL inventory has not been reclassified.
Public admission must also independently attest each owner's actual native or
Raft receipt authority, rather than letting a proposed plan choose it. Worker
activation must preserve ordinary read availability through pinned schema
views while write/intent fencing and final publication enforce the index cut.

### Exact native effect positions on standby replay

Native private admission, drained-fence sealing and dual-catalog reservation
now accept a typed forwarded native position through dedicated replay staging
entry points. The ordinary primary entry points continue to derive their cut
from the durable local source clock. Replay requires native authority, the
exact owner namespace and a nonzero sequence before staging effects. A fresh
effect must consume exactly the next durable clock position. An existing
effect must match its original recorded position, not the current clock; this
allows older admission/fence retries after reservation has advanced the clock.
Neither path fabricates a Raft position or advances the clock on a duplicate.

Forwarded positions are internal standby-frame data, not authority supplied by
a SQL caller or coordinator. The receiving worker still needs its independent
metadata permission and local identity checks. These transaction primitives
neither authenticate a frame nor commit or sign a receipt. The eventual ordered
DB entry point must stage the real outbox/frame with the effect in the same
transaction, and abort the transaction on every error.

Memory and LSM tests cover fresh replay and exact duplicates for all three
effects, both absent and existing physical catalogs. Zero/foreign positions,
gaps and incorrect retry positions are rejected. Tests deliberately exercise
a rejected reservation after catalog writes have been staged, then verify that
aborting preserves both catalogs, the durable job/fence and the native clock.
Raft bindings cannot accept forwarded native positions. The existing three
136-byte receipt slots and their durable representation are unchanged; replay
adds no per-retry or per-page history. This is component replay qualification,
not evidence of complete standby transport or public SQL activation.

Debug qualification of these replay changes passes 65/65 build steps across
`antfly-relational-index-candidate-test`, `antfly-relational-index-pages-test`
and `relational-index-catalog-test`. Custom artifacts pass seven candidate,
ten candidate/private-addition, four system/page and fourteen catalog tests,
with no skips, failures or leaks; these overlapping selections are not summed
as independent SQL cases. ReleaseFast also passes 65/65 build steps and all
four custom artifact selections without skips, failures or leaks. The original
SQL inventory remains unchanged, and the remaining activation requirements
above still apply.

The ordered DB entry point also needs an immutable owner-local input manifest
committed with admission and carried in the same standby frame. After a fence
ACK, the metadata permission read intentionally returns no inputs for that
acknowledged operation; it must not be repurposed into execution authority on
restart. The local manifest must retain the exact before/after schema bytes,
predecessor integrity catalog and index identity needed to reconstruct the
candidate, bound to the durable job fingerprints. Recovery must validate those
bindings before rebuilding owned schema views outside the apply lock. Standby
replay must not depend on a fresh metadata network read to reconstruct effects
already admitted by its authenticated primary. The binding-only replay
checkpoint did not establish this durable input/admission/outbox contract. The
next checkpoint adds the local manifest; the ordered DB/standby outbox portion
still remains part of worker activation.

### Durable native worker manifests

Prepared native candidates now own a bounded, length-framed manifest containing
the exact before/after API schema bytes, predecessor integrity catalog and index
name. Framing retains raw bytes without JSON/base64 expansion and checks its
lengths and physical checksum before allocation. The private job binding is now
version 3 (688 bytes), with a manifest digest included in its own checksum and
native receipt context. Earlier PR-local private formats are not accepted.

Admission requires the prepared manifest and commits it with the job, fence and
native admission cut in the caller's transaction. Exact retries verify the
immutable stored bytes; a substituted or orphan manifest cannot authorize a
new job. The manifest key is protected alongside other private index metadata.
Cold recovery owns the manifest before subsequent transactional reads, rebuilds
the typed candidate, and verifies both admitted catalog fingerprints and the
native before-schema identity. API schema JSON is not the serialized native
schema: the latter's identity comes from the shared typed compiler, not a hash
of the API JSON. Live page guards still read fixed-size bindings/receipt slots,
not schema-sized manifests or repeated JSON compilation.
Receipt reconstruction separately checks the durable manifest's framing,
checksum and admitted digest without allocation or schema compilation. Missing
or substituted inputs therefore cannot produce a new ACK proof from a worker
that was already open before corruption. This check is per receipt, not per
row/page.

The component campaign includes absent, corrupt and checksum-valid substituted
manifests on memory/LSM recovery and live receipts, orphan rejection at both
recovery and admission, atomic admission rollback, exact replay, schema
representation identity, borrowed-buffer ownership and allocation-failure
cleanup. Final Debug and ReleaseFast qualification each pass 65/65 build steps
across the candidate, page and catalog gates. Custom artifacts pass eight
candidate, eleven candidate/private-addition, four system/page and fourteen
catalog tests with no skips, failures or leaks; overlapping selections are
not added to SQL inventory credits. Metadata publication integration passes
59/59 Debug build steps, seventeen publication tests and the decoder-floor
test. The public `embedded-capi-check` production gate passes 28/28 Debug
build steps; this is not a private server storage-kernel qualification.
Formatting, diff checks, the 822-source embedded dependency
boundary, control-catalog freshness and original inventory integrity pass.
The initial API-byte/native-schema fingerprint mismatch was fixed by checking
the independently rebuilt native identity, not by weakening schema guards.
This persists the reconstruction inputs;
it does not yet implement the ordered DB/C-ABI/RPC entry point or its atomic
standby outbox/frame, independently attested public admission, global UNIQUE
claims, terminal cleanup, publication/cancellation or distributed fault gates.
The public producer stays disabled and original SQL dispositions stay unchanged.

### Native owner DB execution and standby replay (in qualification)

Private owner controls now have a typed prepared DB boundary for admission,
drained fencing and reservation. Candidate compilation, immutable schema-view
ownership, durable manifest loading and large-envelope serialization happen
outside apply admission. Transactional staging assigns the actual native cut;
only its bounded decimal sequence is appended to the prepared envelope inside
the lock. The existing durable batch outbox, rather than a parallel replication
system, carries the committed effect. Publication failure reports committed
pending replication without fabricating a receipt or implying rollback.

Standby envelope 22 carries a private command and its original native position.
It is not a public `BatchRequest` field. Strict wire decoding rejects unknown
root fields, mixed ordinary effects and alternate receipt domains. Typed ingress
validates the same isolation invariant. Native position and standby transport LSN
commit together. Replay checks the authoritative transport sequence before
preparation, after preparation errors, and again in the serialized write
transaction: a newer delivery must never be overwritten by a stale prepared
command. The transaction-local sequence reader consumes borrowed bytes before
another point read can reuse their storage.

A new LSM integration campaign directly exercises this DB boundary, wire
encoding/decoding, standby ingress, duplicate delivery, stale prepared replay,
reopen and private-schema visibility. It also checks gap rollback, mixed ordinary
effects/receipt rejection, unknown wire fields and exact primary retries. Final
Debug qualification passes all 65 build steps across the candidate/page/catalog
gates: eight candidate, eleven candidate/private-addition, five system/page and
fourteen catalog tests, without skips, failures or leaks. Public C-API production
`embedded-capi-check` passes 28/28 Debug build steps. Apache dependency-boundary
validation passes for 2028 sources and inventory integrity remains unchanged.
ReleaseFast qualification also passes all 65 steps with the same selected
artifact counts and no skips, failures or leaks. These gates do not prove
distributed activation. The first
integration compilation exposed an obsolete Zig reflection API, now corrected
to the repository's field-name interface. The first execution also corrected a
fixture to use the public schema/validator setter rather than weakening its
native schema guard. No SQL inventory credits are claimed.

The native owner integration fixture forces synchronous publication failure
after durable fencing, checks
that pending replication exposes no receipt, and reopens the LSM owner before
recovering the exact saved frame/native cut. The fixture also checks that a
checkpoint can acquire an exclusive mutation barrier during acknowledgement and
that the recovered duplicate reconstructs its original proof without allocating
another cut. This restart/recovery campaign passes all 52 Debug build steps,
with five system/page and eleven candidate/private-addition tests and no skips,
failures or leaks. The first fixture compilation's callback-helper shadowing was
corrected without changing assertions. A strengthened campaign additionally
mirrors successful reservation and acquires the exclusive checkpoint barrier in
the foreground apply acknowledgement callback, not merely the recovery callback;
it verifies exact publication counts on duplicate retry. Final Debug
qualification passes all 52 steps with the same artifact counts and no skips,
failures or leaks. ReleaseFast qualification also passes all 52 steps with the
same artifact counts and no skips, failures or leaks.
This does not add a
SQL disposition or justify enabling the public producer.

Still required: authenticated C-ABI/RPC receivers with independent live metadata
permission, envelope capability admission, actual Raft effect identities, worker
dispatch/ACK integration, global UNIQUE claims, terminal cancellation/publication
and cleanup, and distributed fault qualification. The public producer remains
disabled until those requirements and the public failed-build atomicity gate are
proved, not merely because the private component campaign passes.

### Private native index compiled-owner boundary

The private server-owner ABI now separates raw-manifest admission from bounded
fence/reserve commands. Admission accepts the small typed header and borrowed
AIM1 bytes, not a coordinator-selected physical binding. The actual owner loads
its schema/catalog and chooses the physical allocation locally. Fence/reserve
accept only the corresponding original binding and cannot replace the immutable
inputs or select a standby replay position. Operation/header mismatches, unknown
operations, another table and another actual handle owner group fail before
staging. The ordinary batch contract and public routes are unchanged.

The private ABI has a typed consumer and an explicitly registered storage-kernel
export. Response storage is reserved before committing and JSON uses a fixed
writer; final ownership uses the C allocator's guaranteed allocation-free
in-place shrink. This boundary is internal, so its types are not public OpenAPI
models. Receiving server workers still must independently fetch fresh metadata
owner permission; no caller-set authorization Boolean is introduced.

Initial direct C-ABI tests pass all 51 Debug build steps, including two tests for
prior-version pointer safety and LSM admission/fencing/reservation with raw
inputs, rejected operations/tables/input replacement, committed native proofs
and unchanged public schema. The actual `runtime-unit-storage_kernel` production
build passes 16/16 Debug steps and compiles the new private export. This gate is
distinct from `embedded-capi-check`, which only qualifies the public independent
C-API owner. Apache boundary validation passes for 2028 sources. Final guards and
negative tests for the actual handle owner group pass the combined direct C-ABI
and private storage-kernel Debug gate, all 56 steps and both tests with no skips,
failures or leaks. ReleaseFast qualification passes all 51 direct C-ABI gate
steps and both tests, with no skips, failures or leaks.

The independent authenticated RPC receiver, capability admission, worker
dispatch/root-attested ACK integration, Raft effect-cut implementation, global UNIQUE
validation and terminal publish/cancel/cleanup remain required. A fresh metadata
read is not a lease: cancellation must also fence late admission on previously
unvisited owners, rather than merely acknowledging absence. Public activation
and the original SQL inventory remain unchanged.

### Native owner inspection and pre-fence input integrity

Receivers can now inspect a local native job using only its immutable metadata
identity: plan ID/digest, owner digest/ordinal, namespace and owner group. The
read validates persisted native authority even when the job is absent; absence
in a Raft-owned namespace must not masquerade as native eligibility. An existing
job must match the complete identity and pass the same local fence/catalog/cut
guards used for execution. The snapshot returns the computed physical binding
without reconstructing schema-sized worker inputs or compiling candidates. This
is information, not an execution lease, receipt or cancellation tombstone.

The private ABI/client expose this inspection alongside admission and control,
without adding an ordinary batch or public route. C-ABI tests cover pre-admission
absence, both durable phases, retained input identity and foreign job/namespace
rejection. Fencing now verifies the immutable manifest before staging a new cut,
not merely while reconstructing a receipt after commit. The LSM campaign deletes
the admitted manifest and requires the fence attempt to fail without advancing
the native clock or writing its cut; restoring the exact inputs permits normal
execution. Combined Debug qualification passes all 69 steps, with two C-ABI,
five system/page and eleven candidate/private-addition tests and no skips,
failures or leaks. The additional pre-admission Raft-mode rejection assertion
passes the 51-step direct C-ABI gate, both tests with no skips, failures or leaks.
Apache boundary validation passes for 2028 sources. Raw-manifest byte budgets
are now checked at the ABI boundary before slicing/dereferencing; empty and
max-u64-length invalid-pointer requests reject before admission. The final
bounded-input C-ABI/private-kernel Debug gate passes all 56 steps and both tests
without skips, failures or leaks. Optimized inspection/pre-fence qualification
passes all 64 steps, with two C-ABI, five system/page and eleven candidate/private
tests and no skips, failures or leaks. No SQL inventory credits or public
activation claim are made.

### Provisioned native index owner adapter (in qualification)

The provisioned owner source now offers typed native job inspection, admission
and fence/reservation control through the actual private storage-owner ABI.
Each operation retains its physical-owner lease through response decoding,
checks the exact namespace and owner group, and bounds raw admission inputs
before acquiring an owner. Request cancellation and platform-normalized
deadlines govern cold catalog/owner acquisition and are rechecked immediately
before storage dispatch. No post-commit cancellation check disguises a committed
pending outcome as an uncommitted timeout. Returned parsed values own their
memory independently of the C response and owner lease.

This adapter is not a metadata permission grant or signed ACK producer. Its
caller must independently authenticate and fetch publication permission; actual
storage execution continues to validate native authority and durable job state.
The focused source test requires canceled, expired and foreign-group requests
to reject before touching an uninitialized catalog/owner, and rejects empty
admission inputs before acquisition. The production-linked Debug source gate
passes all 48 build steps. A second test uses a real cold provisioned native
owner, rejects a foreign namespace without damaging subsequent inspection,
then closes and reopens the whole source on the same LSM root. Both focused
tests pass with no skips, failures or leaks. The optimized adapter gate also
passes all 48 steps and both tests with no skips, failures or leaks. This does
not activate public publication or change SQL credits.

### Committed native index receipt recovery (in qualification)

Native owner receipt recovery accepts only the immutable owner identity and a
fenced/reserved action. It reads the actual durable binding and effect cut;
callers cannot supply a physical allocation, candidate digest, source position
or logical digest to certify. Unlike repeating reservation, recovery does not
compile candidates or decode schema-sized worker inputs. It validates the
stored manifest checksum, native authority, fence, catalogs and source cuts.

Replication recovery and acknowledgement waits precede the immutable read
snapshot; receipt hashing does not hold the global apply lock. Crucially, that
snapshot also probes all durable outbox
forms using point-presence checks and one prefix seek, without copying their
payloads. A successful earlier flush or clear in-memory scheduling hint alone
cannot prove that a concurrently visible effect is acknowledged. Pending
obligations suppress the logical receipt, repair the scheduling hint and return
an explicit pending result;
foreign/non-native jobs reject before initiating recovery. The private ABI,
client and provisioned source expose typed recovery while retaining the actual
owner lease. No new native cut or public schema publication is performed.

Focused qualification covers C-ABI equality with committed effect proofs,
cold-source absence, canceled/expired dispatch, LSM restart recovery, and a
deliberately stale pending hint with a real retained outbox. The latter must
return pending without a proof or another publication attempt. Combined Debug
qualification passes all 99 steps with two C-ABI, two provisioned-source, five
system/page and eleven candidate/private-addition tests and no skips, failures
or leaks. Final lock-free snapshot/hint-repair qualification passes all 99 steps,
with three C-ABI, two provisioned-source, five system/page and eleven
candidate/private-addition tests, no skips, failures or leaks. The additional
LSM snapshot test covers every legacy/versioned obligation, including empty
payloads, and verifies that concurrent commit/clear cannot change an existing
snapshot's decision. Optimized receipt-recovery qualification is pending.
These remain unsigned logical receipts, not
metadata authorization, root attestation or public activation. The original
SQL inventory is unchanged.

### Canonical metadata-to-native index admission (in qualification)

An authenticated owner admission projection can now produce the exact typed
native header from the admitted head and independently bound dispatch. The
factory requires a live fencing permission, the admission inputs projection,
native receipt authority, exact owner ordinal/group/namespace, owner-descriptor
fingerprint, successor schema digest and table/namespace identity. Acknowledged
or canceled decisions cannot be converted into a new admission. The factory
does not authenticate its caller and is not a lease against later cancellation.

The full publication ID supplies transition and attempt fields (both 64-bit
halves), matching generation-publication conventions; the separately admitted
source epoch is retained unchanged. These fields never become Raft positions.
Metadata admission rejects IDs that cannot form a valid fence before publishing
any plan. The terminal-retention fixture still covers all 32 CAS retries and
alternating owner cohorts, now with representable two-half IDs; explicit
negative tests cover each zero-half case.

The inputs projection packs an owned, bounded raw manifest without compiling
the logical candidate a second time in the receiving worker. The physical owner
still compiles against its actual immutable catalog and checks both admitted
fingerprints before committing. Tests compare the packed bytes with the
independently prepared candidate manifest, exercise allocation-failure cleanup,
and reject substituted epochs, namespaces, successor inputs and plan identity.
The final metadata publication Debug gate passes all 59 steps, including one
authorization test and eighteen publication tests, with no skips, failures or
leaks. Review added predecessor-schema hash and exact successor-version checks
without another JSON parse; their negative assertions pass in this run.
Optimized qualification remains pending. Authenticated RPC execution,
root-attested ACKs and the later lifecycle phases remain required; public
publication and the original SQL inventory remain unchanged.

### Request-bound native owner controls (in qualification)

The permission conversion now binds the independently parsed receiver request
to the exact table, owner group, publication ID and permitted phase before any
owner acquisition. Hot control conversion needs no admission inputs. It accepts
only fence/reserve operations and a binding obtained from typed physical-owner
inspection, checks its complete canonical fence and both admitted catalog
fingerprints, and retains the owner's physical allocation unchanged. Reserved
retries use the admitted command preimage without regressing durable job state.
Storage still verifies/CASes the actual binding; neither this conversion nor a
fresh metadata observation is authentication or a cancellation lease.

Focused assertions cover substituted request identities, wrong phases,
acknowledged responses, invalid metadata identity, altered durable bindings and
reserved retries. These are projection tests, not signed or committed owner
evidence. Execution qualification is pending. The public producer, failed-build
atomicity gate and SQL inventory remain unchanged.

### Bounded native owner execution (in qualification)

The server-side execution layer accepts only table/group/publication identity
and fence/reserve intent. It obtains its own permission from the configured
metadata transport and validates the exact request and response disposition.
Neither projected decisions nor physical allocations are accepted from an RPC
body. Acknowledged work returns without acquiring storage. Native inspection
verifies the actual persisted authority before an absent owner can be admitted.

Each call performs at most one new owner effect. Only an absent fence admission
fetches schema-sized inputs, using a second fresh metadata read that can reject
intervening cancellation. Admission yields durable progress without claiming a
fenced receipt; subsequent calls recover existing cuts or apply the next bounded
control without downloading inputs again. Owned results retain pending error
strings and do not turn cancellation after commit into an uncommitted outcome.

Component assertions cover staging, retry recovery, owned pending responses,
post-commit cancellation, changed permission, foreign table names, acknowledged
work, missing admission and allocation-failure cleanup. Debug qualification of
the publication gate passes all 59 build steps and 20 tests (19 publication
and one decoder-floor), without skips, failures or leaks, after merging main
at `77a276cd53`. This layer is not yet wired to an authenticated RPC/root-attestation
producer. Durable cancellation/late-admission tombstones, all later phases and
the distributed fault/public atomicity gates remain required before activation.

A composed provisioned-source fixture now samples the real LSM owner's catalog
and admission epoch, runs staged admission/fencing/reservation through the actual
private ABI, checks that the public schema is still the predecessor, and fully
closes/reopens the source before recovering the original reserved receipt. It
also checks that large admission inputs are requested only once. Metadata phase
progression is simulated in this fixture; it does not stand in for root-signed
ACK/enrollment or distributed fault qualification. Debug qualification passes
all three focused native-owner tests, including this composed restart fixture,
without skips, failures or leaks. Optimized qualification remains pending.

### Catalog source coverage after the main merge

The storage-kernel compilation gate exposed a stale allocation-name caller of
the coverage-aware catalog cursor introduced on main. Name allocation now
explicitly selects relation coverage: table/index occupancy includes hidden
create/restore reservations, but excludes table-qualified CHECK/FK memberships.
The bulk-resolution regression includes a CHECK whose name matches an available
index name, preserving that namespace distinction. The Debug namespace/restore
runner passes all 50 selected regressions without skips, failures or leaks.
Combined with the three native-owner tests above, the Debug gate completes all
85 build steps. The original inventory checker still reports 478 implemented,
136 rejected, 73 superseded and 899 unresolved out of 1,586 original cases.
The Apache/server boundary checker passes for 2,028 source files. None of these
component gates changes an original SQL disposition.

### Same-root native source observations (in qualification)

Native index ACK verification now requires a durable owner report containing
native source authority, the exact namespace, and a commit sequence at least as
large as the receipt's native cut. Unknown observations and Raft-mode owners
cannot stand in for a native cut. Existing root enrollment, reporter, replica
placement, root-generation and lifecycle checks still apply; Raft indexes and
native sequences are never compared.

The compiled owner samples this fixed-size source state while holding the same
nonblocking apply-shared guard as its runtime stats. It validates the durable
namespace and rejects mixed native/Raft markers. Status-only observations remain
unknown. The optional state crosses the private JSON boundary, immutable status
cache, cloned reports and a named V19 status codec; V18 schema-epoch framing is
unchanged. Full-record profile selection preserves the strongest sibling fact.
Report deduplication now includes both schema epochs and source watermarks, so
an epoch-only or native-cut-only change cannot disappear as a heartbeat no-op.

Senders include the new observation only after all metadata decoders report V19
readiness. Proposal admission independently rechecks that coordinated barrier;
the first durable report can then activate the codec. Requiring prior activation
on the sender would deadlock that first report. Mandatory authority facts are
never silently stripped to downgrade a proposal, and unknown future profiles
cannot satisfy readiness. The public index producer remains disabled.

The first Debug authority gate passes 59/59 build steps and 33 tests, including
wire corruption/truncation, downgrade refusal, root-receipt observation faults,
cloning/deduplication and service readiness. A combined Debug repeat passes
94/94 steps and 36 tests, including the real compiled-owner observation after
LSM restart. Review then extended monotone readiness retention to historical
profiles: a late weaker probe cannot erase a stronger proof for the same
membership, while replacement membership discards the old proof. All three
standalone protocol tests pass for that final change. The final Debug result is
recorded below; optimized authority qualification remains pending.
Authenticated producer/signing integration, cancellation
tombstones, later publication phases and distributed fault/public atomicity
gates are still unfinished. No original SQL case receives credit from this
component work.

The final Debug repeat completes 105/105 build steps and 44 tests, including
the six private C-ABI native-index/runtime-status contention and cleanup cases,
with no skips, failures or leaks. Separately, the pre-authority main-merged
baseline completes 103/103 optimized build steps and 73 tests (three native
owner, fifty namespace/restore and twenty publication/decoder tests). The
authority change itself still requires optimized qualification; the baseline
does not establish that result.

The data-server integration Debug gate also completes 91/91 build steps and
both selected `runRound` regressions without skips, failures or leaks. This
compiles the reporting integration rather than relying only on metadata codec
and storage adapter component roots.

### Physical-root-bound native receipt recovery (in qualification)

Receipt recovery now returns the generation of the exact owner lease that
recovered the durable native cut. The receiver retains that generation with its
owned recovered outcome. It cannot safely recover a cut, release the owner and
then attach a generation obtained from a separate current-root lookup: a root
replacement between those operations would relabel evidence from the old root.
This adds no scan, additional physical RPC or schema-sized allocation. Pending
replication outcomes remain pending and do not gain a publishable receipt.

The composed LSM fixture now provisions a non-default descriptor generation,
fully closes/reopens it, recovers the original reserved cut and observes the
same generation from the compiled owner's runtime status. A subsequent routing
generation change must not change the generation already attached to the
recovered result. Empty-job, foreign-namespace and pre-acquisition cancellation
checks cover the new adapter. The combined Debug gate passes 94/94 build steps
and all 23 tests (three physical-owner, nineteen publication and one decoder
floor), with no skips, failures or leaks. Optimized qualification is pending;
these are physical receipt/lease tests, not enrollment or distributed placement
evidence. Inventory integrity and the 2,028-source licensing boundary pass;
the original SQL dispositions remain unchanged.

Signing still needs actual durable replica placement, the enrolled store root,
the current reporter incarnation and metadata's matching root-generation
checks. Native standalone ownership must not borrow a synthetic replica ID or
Raft position, nor perform a full replica-catalog scan for each receipt. That
integration, authenticated delivery, cancellation, later lifecycle phases and
the public failed-build atomicity gate remain required before activation.

The shared Antfly replica catalog in `raft/storage/catalog.zig` already exposes
revisioned `readReplica` point reads backed by the resident record map. Native
provisioning and attestation should share that borrowed catalog instance and
its durable placement admission, just as the Raft host does. Reopening
`FileReplicaCatalog` from its path per receipt would reload the whole file and
defeat the point-read boundary. The engine-level generic Raft catalog is not
the relevant serving-owner interface. Missing native placement remains unknown
authority, never permission to invent a replica ID from a group or node number.

### Durable cancellation intent before owner visitation (in qualification)

The native transaction primitives now retain one 368-byte canonical intent per
owner, binding the full plan/owner identity, exact source fence, candidate and
reservation fingerprints, and a real native sequence. Intent, source clock and
source fence are staged together, including for a never-visited owner. That
closes ordinary participant admission and schema mutation immediately, without
first fetching or compiling schema-sized worker inputs. Existing predecessor
participants may still resolve and must drain before any fenced proof or burn.

Normal admission, fence, reservation and private pages check the intent in their
committing snapshot, closing the delayed-dispatch race after metadata canceled
the plan. Cancellation-specific admission/drain/reservation paths require the
exact persisted intent; there is no incoming bypass flag. A new plan cannot
replace the slot until the old epoch is durably consumed. Intent retries retain
their original cut and do not advance the native clock again. Recovery validates
an intent even when no worker job or manifest was ever created, as well as its
namespace, clock, source fence and public predecessor catalog.

This is intent and fence/drain/burn machinery, NOT a canceled publication ACK.
Artifact/claim cleanup, release, authenticated command execution, the dedicated
replication envelope and coordinated intent-aware owner capability barrier are
still required. The caller must commit its durable outbox with these primitives
and abort on every error; direct transaction tests are not production standby
delivery evidence. An older owner cannot safely receive intent-bearing state
until that decoder/executor rollout barrier is implemented. Public production
remains disabled and no original SQL case is credited.

Memory and LSM cases cover never-visited, admitted
and reserved owners, rollback, clock-gap rejection, exact replay, foreign
identity, delayed normal operations, generic cancellation refusal, predecessor
draining, both catalog burns and cold verification. Framing tests include every
truncation, checksum corruption, canonical padding, Raft/native separation and
namespace substitution. Review adds a real private page prepared before intent
whose apply is rejected before candidate/progress writes, and reserves the
intent key against extra-metadata writes and deletes. The first compilation
exposed a shadowed local name. The first runtime run exposed the old assumption
that every index fence already had a worker binding; only an exact, pending
durable intent now authorizes the never-visited intermediate state. Generic
job-less index fences remain rejected. Final Debug qualification passes 43/43
build steps and all 27 candidate/catalog tests, with no skips, failures or
leaks. The licensing boundary passes for 2,029 source files and original SQL
inventory integrity passes unchanged. Final ReleaseFast qualification also
passes 43/43 steps and all 27 tests, with no skips, failures or leaks.

### Cancellation through the shared owner commit path (in qualification)

Cancellation preparation owns only the bounded identity header. Both normal
native index controls and cancellation now enter the same DB mutation pipeline:
write gating, snapshot leases, apply-exclusive, transactional native cut and
transport marker, durable outbox, and completion after lease release. Dedicated
version-23 replay envelopes reject ordinary mutations, mixed control frames,
unknown fields and namespace substitutions. Intent results have no publication
receipt field. The private compiled owner accepts cancellation without a
manifest; its response storage is reserved before commit. The serving adapter
uses the actual leased owner and preserves committed pending outcomes after a
request is canceled.

LSM integration coverage is being qualified for a never-visited primary with a
failed outbox publication, cold reopen before worker admission, exact payload
recovery, duplicate intent, gap rejection, atomic standby replay/reopen and late
normal admission rejection. Compiled-owner coverage also exercises cancellation
after reservation, duplicate retries and restart. The first integration compile
caught an extra level of pointer indirection in the shared DB dispatch; that is
fixed, but these new integration tests are not yet green evidence. Authenticated
delivery, intent-aware capability barriers, cleanup/release and distributed
fault qualification remain open. Public production remains disabled; no SQL
inventory case is credited by this component work.

### Fixed-header cancellation receiver (qualification pending)

The native receiver can now request cancellation permission independently from
metadata and persist one owner-local intent. The permission projection binds
table, group, plan, owner, exact source fence and admitted fingerprints without
fetching or compiling a schema manifest, including on a never-visited owner.
The receiver validates the returned intent identity and native namespace; an
intent result, even when mirrored, is distinct from metadata's full canceled
receipt. A committed pending result is retained if request cancellation follows
the storage boundary. This helper is not mounted as an authenticated RPC and
does not enable the public producer.

Synthetic receiver tests cover fixed permission without admission/inspection,
duplicate retry, pending publication, cancellation after commit, foreign result
identity, wrong group/table and stale metadata phase. They are sequencing tests,
not physical durability or distributed authority evidence. The first compile
caught the shared intent equality method's visibility; exposing that exact
identity comparison fixes the boundary without duplicating its field list.
Final Debug qualification passes 59/59 build steps and all 21 tests (20
publication/receiver contracts and one decoder-floor contract), with no skips,
failures or leaks. Final ReleaseFast qualification also passes 59/59 steps and
all 21 tests, with no skips, failures or leaks. Metadata decoder floor
36 and runtime-status V19 are not substitutes
for intent-aware compiled-owner execution and standby-envelope readiness. Those
rollout barriers, cleanup/release, actual enrolled-root attestation and the
public failed-build atomicity gate remain unfinished. Original SQL dispositions
remain unchanged.

### Shared cancellation artifact retirement (in qualification)

After exact persisted cancellation intent, transaction drain and both allocation
burns, a private generation can enter the existing forward/ownership GC state
machine. A single fixed-size cleanup-admission slot binds the full immutable
intent and a real native cut newer than both cancellation and reservation.
Admission, queue insertion and cut advance share the caller's transaction and
must share its durable outbox. Duplicate admission retains the original cut and
never resets the GC cursor or requeues a completed generation. Recovery checks
the slot even after physical deletion, rejects forged cuts/identity, and permits
an old slot only after its epoch was durably consumed.

Ordinary churn is bounded by the existing retirement limit; the sole active
private job owns one additional cancellation-only slot. This avoids deadlock
when that public queue is full without adding a second cleanup engine or an
unbounded admission scan. The existing GC keeps its 256-record, byte and time
budgets. Final physical verification uses fixed point reads and two generation
prefix seeks, not a primary-table scan. Extra-metadata mutation cannot forge or
delete the cleanup-admission slot.

Memory and LSM tests for never-visited, admitted and reserved cancellation now
seed 300 forward/ownership/reverse companions, require multiple bounded pages,
verify all reverse deletions and retain an unrelated generation. They cover
full public-queue headroom, rollback, replay gaps, exact duplicate admission,
cold structural recovery, corrupt cleanup cuts/identity and preservation of
the source fence. Debug and ReleaseFast qualification each pass 43/43 build
steps and all 27 candidate/catalog tests, with no skips, failures or leaks.
The initial fixture materialized the complete 8,192-entry unrelated public
queue in each owner scenario; it was replaced by a point-only transaction
adapter that rejects any admission cursor scan, alongside the existing public
capacity test. A sampled remaining Debug run was in exhaustive allocation-fault
recovery and allocator stack-trace collection, not a stuck GC loop.

This is physical artifact retirement, NOT full canceled publication proof.
Generation-local GC pages still need authenticated durable owner/standby
delivery; global UNIQUE claims, private manifest/blob and binding retirement,
final completion cuts, epoch release and coordinated intent-aware capability
barriers remain unfinished. No canceled ACK is emitted and the public producer
remains disabled. No original SQL case is reclassified by this component work.

### Unified owner effects and durable cleanup admission

The private owner protocol is newly introduced in this PR and absent from main.
It now carries exactly one tagged effect in the version-22 envelope: ordinary
control, cancellation intent, or cancellation cleanup admission. Separate
top-level cancellation fields/version-23 prototypes are removed rather than
retained as legacy formats. The strict frame rejects mixed tagged effects and
unknown fields; each effect validates its actual native namespace and has its
own byte limit. This is one owner-effect transaction contract, not additional
mutation routes.

Cleanup preparation uses the actual inspected durable job. The private compiled
owner receives immutable identity only, not caller-chosen physical allocation
or a replacement manifest. Queue admission goes through the existing shared DB
commit pipeline and commits its native cut, marker and durable outbox together;
standby replay stages that same queue effect and transport LSN atomically.
Its result has no publication receipt and survives committed mirror failure.
Both C-ABI and leased-owner adapters expose only this internal progress step.

The updated integration gate adds real primary outbox failure,
cold reopen, exact pending-payload recovery, duplicate cleanup admission,
standby clock-gap rejection, replay and standby reopen, plus compiled-owner
cleanup retries. Fixture setup drains/burns the jobs using the independently
tested cancellation primitives; it is not proof of authenticated distributed
drain/burn delivery. GC page delivery, cleanup completion cuts, global claims,
manifest/blob and binding retirement, epoch release and coordinated rollout
barriers remain open. Public production stays disabled and no SQL case is
credited by this protocol consolidation.

Merged-head qualification at `6fad13874a`, with `origin/main` at
`84a0080999`, passed all 137 build steps and 97 tests in Debug with no skips,
failures or leaks. The targets were `antfly-native-owner-authority-test`,
`antfly-relational-index-pages-test`, `native-index-owner-capi-test`,
`antfly-storage-owner-source-test` (filter `native index owner adapter`),
`antfly-relational-index-publication-test` and `relational-index-catalog-test`.
This replaces the invalid first unified run, which encountered a shadowed
fixture identifier and then source changes while other compilers were live.
It qualifies the listed component contracts, not authenticated distributed
activation, optimized merged-head execution or the still-open public failed
CREATE UNIQUE INDEX atomicity gate. The original SQL inventory is unchanged:
478 implemented, 136 rejected, 73 superseded and 899 unresolved of 1,586 cases.

### Bounded standby-local cancellation cleanup

Qualification at `f0f3f24f48` passed all 101 build steps and 41 tests in Debug
with no skips, failures or leaks (`antfly-relational-index-pages-test`,
`native-index-owner-capi-test`, `antfly-storage-owner-source-test` filtered to
`native index owner adapter`, and `relational-index-catalog-test`).

The cancellation worker selects its admitted generation by point lookup rather
than waiting behind unrelated public retirements. It shares the existing
256-record/1-MiB/5-ms GC page engine and stages deletion and continuation in the
same transaction as the exact cancellation, burned allocation and durable
cleanup-admission checks. Ordinary maintenance conflict checks precede opening
the writer. Drain is proved by cleanup admission; pages do not repeat the
potentially large retained-transaction scan under apply-exclusive. The exact
source fence remains active throughout cleanup.

This derived-artifact operation respects read-only backends, portable-runtime
gates, replication/snapshot/replay leases and catalog generation guards, but
does not open ordinary standby write admission. The LSM regression denies
ordinary and background writes, puts an unrelated retirement earlier in the
queue, restarts between cleanup phases, and verifies that the unrelated queue
entry, source clock, replay LSN and active epoch are preserved. Internal ABI
operation `cancel_gc_page` and its leased-owner adapter return physical progress
only; response ownership is allocated before committing any page.

This is not automatic scheduling, authenticated cancellation dispatch, a final
completion cut, global UNIQUE-claim/manifest/job retirement or epoch release.
Those paths and their distributed fault evidence remain unfinished, the public
producer stays disabled, and no original SQL case receives credit here.

### Private UNIQUE claim retirement: generation ownership, not a global scan

Cancellation queue admission retains the exact burned UNIQUE allocation in a
canonical checksummed cleanup record. Its generation is derived from the table
incarnation and allocation; it is not an incoming constraint name or tuple hash.
Admission and recovery independently verify the exact reserved catalog digest,
active owner job and source fence. This is retained scope, not deletion authority.

The existing integrity claim key orders kind, routing hash, generation and tuple
hash. Consequently, seeking a generation prefix cannot enumerate its claims.
The physical index's forward/ownership GC proves only artifact retirement and
must never be treated as proof of distributed UNIQUE retirement.

The remaining implementation must satisfy these invariants:

- Maintain a generation-owned inventory on each actual hash owner. Insert/remove
  inventory entries in the same transaction as claim changes, with exact claim
  identity checks. Normal replay and recovery/restore must preserve or strictly
  reconstruct this invariant; an unexplained missing inventory is not emptiness.
- Freeze the complete hash-owner cohort under independently read metadata
  permission. A row-owner cancellation fence does not fence those hash owners.
  Bind each retirement acknowledgement to the exact generation, plan, owner
  incarnation/root and durable source cut; neither cardinality nor a source
  owner's physical GC result proves coverage.
- Persist retirement before deletion and check it both when preparing claim
  effects and in the committing transaction. This must reject delayed prepared
  establishes, replayed worker requests and duplicate deliveries after GC, while
  retaining ordinary transaction-log replay semantics.
- Traverse only the selected generation inventory using durable, CAS-protected,
  bounded continuations. Validate companion records before deleting; unexpected
  FK references or action jobs cannot be silently discarded. Keep unrelated
  generations and their admission independent.
- Retire immutable inputs, private binding and manifests only after all required
  owner proofs are durable. Persist a final cancellation cut before reporting
  completion. Epoch release is a separate coordinated phase, never an implicit
  consequence of an empty artifact queue or elapsed time.
- Exercise lost replies, committed mirror failures, duplicate/gapped replay,
  owner/root replacement, late claim writes, incomplete cohort acknowledgements,
  concurrent independent generations and interrupted restore under real owner
  storage. Component fixtures do not replace authenticated distributed faults.

These retirement and activation requirements are still open. The public native
index producer remains disabled; no original SQL case is credited by the scope
record or this design.

Qualification checkpoints (no additional original-case credit):

- `69d6f7a368`, including main through `1258e8cfcb`, passed all 120 Debug build
  steps and 63 targeted test executions: owner C-API, catalog, physical pages,
  publication/decoder floor, private candidates and the actual leased owner.
- `c9cd67660b` passed all 117 Debug build steps and 49 test executions for the
  retained scope implementation, including memory/LSM recovery, native standby
  replay, cleanup and compiled-owner inspection. Its separate catalog selection
  passed all 40 build steps and 14 test executions.
- `16637b7ba6` passed all 65 Debug build steps and 50 test executions for catalog,
  candidates and publication/receiver contracts. These add malformed/trailing
  records, zero/missing/orphaned scope values, checksum/incarnation mismatch and
  allocation counter boundaries. The Apache boundary check passed 2,033 source
  files. These overlapping selections are not a full suite or distributed gate.

The combined main-merged checkpoint `ddbe982d5f` passed all 120 Debug build
steps and 64 targeted test executions with zero skips, failures or leaks.
The catalog/candidate contracts at `16637b7ba6` also passed all 43 ReleaseFast
build steps and 28 test executions. These qualify retained claim scope and its
current integration, not distributed claim creation, retirement or activation.

### Generation-owned claim inventory and local retirement

Private reservation now stages a generation registration alongside both catalog
allocation burns. Its digest binds the complete reserved owner binding and exact
generation. Missing registration is corruption, not an inferred empty inventory.
Prepared claim batches own their bytes, cap admission at 128 claims/1 MiB and
recheck the durable job, cancellation fence, registration, claim postimages and
inventory hashes in the committing transaction. Exact duplicates are idempotent;
conflicting owners fail before staging writes.

The inventory orders generation before routing and tuple hashes. Local cleanup
therefore seeks only the selected generation and deletes paired claims and
inventory entries in bounded 256-record/1-MiB/5-ms pages. Aborted pages retain
both sides; committed deletions are the durable continuation. Unexpected FK
references or action jobs reject the page rather than being discarded. The
registration and canceled binding remain after local emptiness.

Internal owner operation `cancel_claim_gc_page` shares the physical-GC lease,
read-only/runtime, maintenance-conflict and allocation-before-commit boundaries.
It neither opens ordinary standby writes nor advances the source clock/replay
LSN. Inspection distinguishes physical artifacts and claim inventory retirement;
the receiver performs at most one cleanup page per step and reports local
retirement only after both independent facts are true.

Transfer validation pins the private generation once per source transaction,
checks each private claim's inventory hash and rejects unpublished FK companions.
Action-job identity is copied before companion reads can invalidate borrowed
bytes. Ordinary cohort restore still reconstructs destination claims from primary
rows; these helpers do not authorize adoption of source private jobs.

Qualification at `7af8f82535` passed all 127 Debug build steps and 90 targeted
executions: two transfer/handoff, forty-four portable backup, twenty-one
publication, one decoder floor, thirteen candidate, three leased-source and six
compiled-owner C-API tests. Cold-restart follow-up `a32af637cd` passed all 52
Debug steps and fifteen transfer/candidate executions. The WAL-backed profile
closes and reopens the owner after cleanup admission and after aborted and
committed pages; it verifies unchanged source clock, retained binding, independent
generation preservation and final local emptiness. Both completed runs have zero
skips, failures or leaks. The source-boundary audit covers 2,034 files; the control
catalog and diff checks pass.

Exact main-merged checkpoint `9fbd088c03` passed the same five Debug targets:
127/127 build steps and ninety executions, zero skips/failures/leaks. The
transfer/candidate gate at `a32af637cd` also passed all 52 ReleaseFast steps and
fifteen executions, including the cold-restart profile and actual topology
handoff. A fresh inventory audit and all sixteen isolated PostgreSQL lifecycle
oracle tests pass. No original-case credit is claimed by these overlapping
component selections or the independent oracle run.

Still required before public activation: ordered authenticated private claim
production on the actual hash owners, complete frozen-cohort retirement proofs,
native restore/adoption completeness, final durable cancellation cut, immutable
input/job retirement and separate coordinated epoch release. Local emptiness is
not a signed owner acknowledgement and cannot substitute for distributed fault
qualification. The public failed-UNIQUE-build atomicity gate remains open, and
the original inventory remains 478 / 136 / 73 / 899.

### Bounded private-claim creation provenance (component qualification complete)

The isolated ordered-claim implementation originally retained one exact-cut
receipt per canonical batch. Reordering and coalescing were idempotent, but
different subsets of the same claims could accumulate receipts without adding
data. Bound retry metadata by claims, not producer batching choices.

Each inventory entry instead retains its immutable native creation cut and a
contextual digest of the normalized job binding, full inventory key, canonical
claim digest and cut. The compact representation is forty bytes: eight for the
cut, thirty-two for the digest. The generation registration owns shared binding
identity; it need not be repeated as plaintext in every entry. No separate batch
receipt key, batch-identity hash or receipt-only cleanup phase is necessary.

For a batch containing new claims, all new members receive the transaction's
next source cut; existing members retain their cuts. For a batch containing only
existing identical claims, return the maximum member creation cut, without
advancing the source clock or creating an outbox effect. This maximum is exactly
the cut of an original effect that introduced a member of that batch. It also
defines an exact idempotent result for regrouped subsets; it is not an invented
acknowledgement of a new write. Forwarded replay must match that cut. Claim
postimages and provenance are validated together in the committing transaction.

Transfer and cancellation GC validate creation cuts strictly after reservation
and strictly before cancellation intent, not merely against the current source
clock. GC captures each immutable cut and rechecks it before paired deletion.
Claims and provenance retire in one bounded page protocol. This remains local
physical evidence, not a signed distributed owner acknowledgement.

Frozen-source qualification at `e7eb0d5fbc` passed 93/93 Debug build steps and
58 executions. Compact revision `94af0fefa9` passed 62/62 ReleaseFast build
steps and thirty executions. Both completed runs have zero skips, failures or
leaks. These are overlapping component selections, not public activation. The new churn
regression admits 258 distinct claims, commits 128 distinct mixed-age regrouped
retries, and checks exactly 258 inventory entries and an unchanged source clock.
The physical accounting is eight extra value bytes per claim versus the earlier
hash-only inventory, replacing batch-count-dependent receipt storage. This is a
storage/work bound, not a measured throughput or latency claim. Original SQL
case dispositions and the public producer activation barriers remain unchanged.

Follow-up `17445a6766` tightens page admission before allocating the next owned
claim. Both command and prepared admission charge the AFC1 fixed bytes, both
paired keys and provenance bytes, using the same accounting. GC stops before
the next record would exceed 1 MiB instead of checking only after appending.
Large-record regressions add two 600-KiB claims, reject oversized command and
prepared admission, and check the complete paired-byte total of every page.
Its frozen-source Debug gate passed 103/103 build steps and 102 targeted
executions, including portable backup coverage. The subsequent regression `8e031be1fe`
prepares overlapping batches before either commits. It verifies identical
concurrent creation is accepted, only new members receive the next cut, and
both batches retain distinct exact retry cuts after the later commit.

The reviewed chain was integrated at `ef7154c10f`, with latest main
`d3bbb9a0c6`. Exact merged-source checkpoint `cbef979866` passed all 103 Debug
build steps and 102 targeted executions, plus all 62 ReleaseFast steps and thirty
candidate/transfer/retained-effects executions. The Debug selections cover
forty-four portable backup, twenty-one publication, one decoder floor, thirteen
candidate, two transfer, fifteen retained-effects and six compiled-owner tests.
These overlapping component selections are not a full suite or distributed gate.

Review also corrected overlapping-test transaction ownership: a later failed
assertion must not abort a transaction already invalidated by successful commit.
That fixture-only follow-up `cdddfddc80` passed all 52 Debug steps and fifteen
candidate/transfer executions. All completed runs have zero skips, failures or
leaks. All sixteen isolated PostgreSQL lifecycle oracle tests and the inventory
audit pass at the merged-source checkpoint. Formatting, diff, control-catalog
freshness and the 2,034-file Apache boundary pass.

The public producer remains disabled. Authenticated hash-owner transport, root
attestation/capability admission, complete build/validation/install/release,
cohort cancellation and native restore/adoption still require integration and
distributed fault evidence. The public failed-UNIQUE-build atomicity gate is
still open. No new original-case credit is claimed: 478 / 136 / 73 / 899.

### Compiled claim delivery and leased-root provenance

Checkpoint `7873b6c95d` adds bounded claim delivery through the existing compiled
owner control interface, not a separate public index route. Request decoding
rejects a 129th claim before parsing or allocating it, enforces the shared byte
budget incrementally and rejects foreign fields. The leased adapter validates
the returned binding, native position, duplicate count and pending/error state.
Seventeen distinct control failures now retain their typed identity across the
compiled boundary; mixed failure/owner ABI versions are rejected explicitly.

Checkpoint `e571659b5d` returns the physical root generation from the same lease
that applied or recovered the claim result. The owned result is suitable input
to later authenticated receipt construction; it is not itself an attestation.
Observational callers may transfer just the parsed result, but signing callers
must retain its root instead of consulting a later routing/status lookup.

The rooted adapter passed all 48 Debug build steps and three focused compiled
owner tests, including fresh and duplicate binary claims, recovered owner state,
foreign-owner rejection and cancellation/deadline checks before acquisition.
No skips, failures or leaks occurred. The independent PostgreSQL lifecycle
oracle passed all sixteen tests; the original inventory audit, selected Zig
formatting, control-catalog freshness and Apache source-boundary checks pass.
The broader frozen-source gate at `7873b6c95d` also passed: 127/127 Debug
build steps and 61 targeted executions (six compiled-owner C API, one decoder
floor, twenty-one publication, thirteen candidate, two transfer, three leased
owner and fifteen retained-effects tests), with zero skips, failures or leaks.
These overlapping selections are not a full-suite or distributed-fault gate.

Public native claim production remains disabled. Authenticated mounted delivery,
root enrollment/signing/capability barriers, complete activation and retirement,
restore/adoption and distributed faults remain required. Neither these three
component tests nor the oracle run close an original SQL case.

### Fresh build-permission claim receiver

Checkpoint `5897dd24ea` connects the internal claim receiver to independently
read metadata build permission. Its request contains claim values and logical
table/group/attempt identity, never a physical allocation, binding or cut.
The receiver obtains the bounded permission projection, inspects the durable
owner job and derives a command only when that reserved job matches the exact
admitted owner, fence, candidate/reservation digests and schema versions. It
shares the existing control-binding checks instead of introducing a weaker
parallel admission path. Claim values retain the shared 128-record/1-MiB limit.

One invocation applies at most one batch and returns an owned result with the
root from the applying lease. Pending replication survives cancellation that
arrives during dispatch; the receiver neither invents an acknowledgement nor
automatically replays an uncertain write. Completed metadata owners short
circuit without acquiring storage. The hot path does not request schema-sized
admission inputs or compile a manifest.

Frozen-source qualification passed 94/94 Debug build steps and 26 targeted
executions: twenty-two publication, one decoder floor and three actual leased
owner tests. The optimized publication gate passed 59/59 steps and twenty-three
executions. Both runs have zero skips, failures or leaks. Synthetic sequencing
checks cover stale phase/owner/table, missing durable admission, early request
cancellation, pending progress and root ownership. The actual compiled-owner
test delivers a duplicate under freshly advanced build permission and verifies
its unchanged source cut and exact leased root. Formatting, diff, generated
control-catalog freshness and the 2,034-file source-boundary audit pass.

This is internal receiver integration, not mounted transport authentication,
root attestation, full build completion or distributed release qualification.
The public producer stays disabled, the public failed-UNIQUE-build gate remains
open, and the original inventory stays 478 / 136 / 73 / 899.

### Authenticated bounded claim transport and callback error identity

Checkpoints `8814b3446f`, `d78050a805`, `be79882bfc` and `ba72e9b356`
mount private claim delivery through the existing internal group/table API.
The receiver requires an explicit service credential even when legacy internal
unauthenticated requests are allowed elsewhere. Logical requests carry only
table/group/attempt identity and bounded claim values. The physical binding,
allocation, fence and source cut still come from independently read metadata
permission and the actual durable owner job, not incoming RPC authority.

Both the decoder and direct-call admission enforce 128 claims and a 1-MiB
paired-record budget. The JSON decoder rejects the next excess record before
parsing or allocating it. Binary tuple values retain their bytes through JSON
transport. Responses are owned until the consumer explicitly releases them;
the handler serializes its borrowed view before releasing the source result.
Native execution is rejected before acquiring an owner on a data-Raft server;
the native ordered effect is not a substitute for a Raft proposal.

The client transmits its capped remaining budget after serialization. The
receiver normalizes its clock and captures a deadline before body decoding,
then honors the earlier existing deadline. Neither side automatically retries
an uncertain mutation. Typed conflicts, cancellation and known corruption
errors retain their identity over HTTP and the actual callback status ABI.
Error decoding uses an explicit status-specific whitelist, not arbitrary
remote error-name conversion. Appended callback detail codes preserve existing
ABI layout and identity assignments.

Frozen-source HTTP qualification at `ba72e9b356` passed all 117 Debug build
steps. The mounted route/client selection passed both tests, including actual
localhost HTTP authentication, binary delivery, bounded ingress, exact typed
failures and no client replay. Its companion selections also passed; these are
overlapping component executions, not distinct original SQL cases. The separate
publication gate passed all 59 Debug steps and 24 executions (23 publication
components plus the decoder-floor test), including strict HTTP failure decoding.
The callback status ABI's direct gate passed all 26 tests.

The data-runtime qualification initially exposed a missing production facade
export, then a multiply-owned callback regression. `be79882bfc` supplies the
production export, and `a70bd32ab7` assigns the new callback regression to one
explicit data consumer suite. The ownership audit remains strict; moving a test
does not authorize ignoring duplicate or empty selections.

The corrected data checkpoint passed all 91 Debug build steps and four tests,
including the actual production data receiver, callback dispatch and mounted
claim client/handler. The merged transport checkpoint `50b01750ca` passed the
optimized publication gate (59/59 steps, 24 executions). Independent fresh
PostgreSQL lifecycle qualification passed all 16 tests; the inventory audit's
own 20 regression tests pass as well. The latter exercise the audit tooling,
not all SQL evidence gates printed by their mocked test fixtures. Formatting,
diff, control-catalog freshness and the 2,034-file Apache boundary pass.

This qualifies private claim transport only. Service authentication does not
prove root enrollment, placement, cohort coverage, build completeness or a
signed publication receipt. Public native production remains disabled. Root
attestation/capability integration, full build/validation/install/release,
cohort cancellation/epoch retirement, native restore/adoption and authenticated
distributed faults remain required; failed-UNIQUE-build atomicity remains open.
No original SQL case is credited by these transport and callback components.

The combined frozen transport source at `50b01750ca` passed all 149 Debug
build steps, including strict linked-owner selection audits, mounted transport
and the production data receiver. Companion suites also passed. This is a
focused combined gate, not full-suite or distributed-fault qualification.

### Applying-root provenance for native fence and reserve

Checkpoint `2e3b68ad53` retains the exact applying lease root with fresh native
fence/reserve control outcomes, matching the existing rooted claim and recovery
paths. The receiver carries the owned outcome and its root together. Later
signing must not relabel that result with a fresh routing/status lookup after
the applying lease has ended. Admission-only results deliberately expose no
receipt root. No extra lookup or allocation is introduced by retaining the
root beside the existing owned response; this is not a measured latency claim.

Frozen-source qualification passed all 94 Debug build steps and 27 component
executions: 23 publication, one decoder-floor and three actual compiled-owner
adapter tests. The latter cover fresh fence/reserve root provenance, reserved
LSM receipt recovery after closing/reopening storage, duplicate claim delivery,
foreign namespace rejection, early cancellation/deadlines and subsequent root
replacement without relabeling an already-owned result. Synthetic sequencing
also verifies that changing the current root does not change a returned root.
All runs have zero skips, failures or leaks. Root provenance is not a signature
or enrollment check; all activation and original-case boundaries above remain.

### Authenticated metadata receipt submission and exact ACK observation

Checkpoint `3fa1d2e707` exposes the existing transactional signed-receipt ACK
through the internal system-catalog dispatcher. Submission and status require
an explicit service credential plus a body-bound read grant; neither operation
accepts an authority Boolean in its body. ACK submission is classified as a
mutation, while exact status is a read and carries the normal metadata identity
headers. The client checks that status belongs to the signing cluster.

Before proposing, submission validates the signature, coordinated decoder
capability, fresh readiness under the catalog mutation gate and the complete
transactional receipt preflight. Committed apply independently repeats current
root enrollment, reporter, placement, namespace, source observation and revision
checks in the head/owner CAS transaction. A preflight is never a lease. Waiting
for apply happens outside the mutation gate; timeout, lost authority or an
unproved postimage remains an explicit unknown outcome.

The bounded status response contains only the fixed head and selected owner.
It neither loads the immutable plan/manifest nor scans the other owners. It
verifies the exact stored receipt digest, not merely a changed revision, a phase
or a count. An original-revision request can therefore resolve its committed
receipt after a lost reply without replaying the mutation. Foreign attempts,
incompatible owner identity and conflicting receipt hashes fail closed. This
point observation remains recoverable after metadata is closed and reopened.

The mutation client gives the transport outcome header precedence over its
body: a conflict-looking body under an unknown outcome cannot become a
definitely-not-proposed failure or trigger a retry. Known receipt rejections
retain their exact HTTP and callback status identities; new callback detail
codes are appended without renumbering existing wire outcomes.

The frozen-source Debug `antfly-relational-index-ack-test` gate passed all 59
build steps and 30 component executions: 26 publication/metadata components and
four focused HTTP/dispatcher tests. These exercise native and Raft receipt
positions, current-root rejection, status before commit, original-revision
lost-reply resolution, reopen, body-bound authentication, callback errors,
mutation-gate release and unknown-outcome handling. The direct callback status
ABI gate passed all 26 tests. An earlier broad metadata-service run passed 137
existing tests, but that target did not select the new HTTP tests; the focused
ACK target above is the qualification for those tests. Inventory integrity,
formatting, diff, control-catalog freshness and the Apache boundary pass.
The same frozen checkpoint passed all 59 optimized ReleaseFast build steps
and 30 component executions, with zero skips, failures or leaks. Fresh
independent PostgreSQL lifecycle qualification passed all 16 oracle tests.

This is the metadata progress connection, not automatic native owner signing,
worker scheduling or full index activation. The public producer remains
disabled. Full build/validation/install/release, hash-owner cohort coordination,
coordinated cancellation/epoch retirement, restore/adoption and distributed
fault qualification still require implementation and evidence. No original
SQL case receives credit from this component gate; totals remain
478 implemented / 136 rejected / 73 superseded / 899 unresolved.

### Native control response validation before attestation

Fresh fence/reserve responses now validate their complete normalized binding
against the dispatched command inside the applying lease. Recovered responses
validate the requested immutable owner identity. Both validate native authority,
namespace and cut before returning an owned rooted result; the receiver repeats
validation at its generic source boundary. Parsing success is not proof validity.

Any returned publication receipt must match its owner, action, candidate and
reservation digests, exact physical position and deterministic effect evidence.
Pending replication or an error cannot coexist with a publishable receipt.
Committed progress whose proof lookup failed remains an owned, receiptless
outcome; validation must not misrepresent it as an uncommitted mutation or
encourage replay. These bounded checks add no metadata lookup or table scan.

The Debug publication and index-page gates passed all 71 build steps and 45
component executions (26 publication, one decoder-floor, 13 candidate and five
LSM/page tests), with zero skips, failures or leaks. The actual standby/reopen
fixture validates fresh, duplicate and recovered cuts, and rejects substituted
positions, owner ordinals, candidate/evidence digests, physical bindings, foreign
recovery identities/actions and receipts carrying pending/error state.

This closes a provider-response validation prerequisite, not root signing,
enrollment, worker activation or distributed-fault qualification. Public native
production remains disabled and original-case dispositions are unchanged.

The same frozen source passed all 71 optimized ReleaseFast build steps and
45 component executions. The separately linked production owner adapter gate
also passed all 48 Debug build steps and three tests covering fresh control,
reserved LSM recovery after restart, foreign namespaces and early cancellation.
All have zero skips, failures or leaks. These component gates do not qualify
automatic signing, enrollment, full activation or distributed faults.

### Authenticated native fence/reserve transport

Checkpoint `274203c6f6` mounts the internal logical-scope fence/reserve receiver
beside the existing claims receiver. Both share explicit service-token checks,
remaining-budget normalization, bounded strict decoding, callback validation
and client transport. Tokenless migration mode is not authority for either
operation. The control request is capped at 4 KiB and cannot carry a schema,
physical allocation, cut, metadata decision or authority flag. The runtime
rejects incompatible data-Raft authority before acquiring a native owner.

The receiver still obtains fresh permission through its configured metadata
source and executes at most one durable owner effect. Its owned response
distinguishes admission from fresh/recovered control, preserves the applying
root and pending error, and validates scope/cut before serialization. Admission
cannot claim a receipt or a root. A remote progress response is unsigned and
does not confer metadata publication authority. Neither the client nor handler
automatically retries an owner effect.

The final frozen Debug HTTP/publication gate passed all 113 build steps and
37 component executions: 26 publication, one decoder-floor, two availability
and the same four HTTP/client tests through two aggregate roots. Those repeated
executions are not eight distinct contracts. The exact production runtime
receiver gate passed all 91 build steps and its one selected test. All have
zero skips, failures or leaks. Tests cover mounted routing, explicit service
authentication even in migration mode, otherwise-valid unknown-field rejection,
zero/over-limit budgets, oversized bodies, deadlines, wrong scope, cancellation,
root retention and no client replay. An earlier combined command rejected an
empty runtime filter; only the corrected non-empty final gates qualify this
checkpoint. The same frozen transport source subsequently passed all 113
ReleaseFast steps and the same 37 executions, with zero skips, failures or
leaks. This optimized checkpoint predates the later main merge, standalone
enrollment and projection-watermark work; it does not qualify those changes.

This connects native controls to serving, not to automatic root signing,
enrollment/capability activation, a complete build worker or metadata ACK
submission. Full lifecycle/cohort cancellation/retirement, restore/adoption and
distributed integrity faults remain open. Public production stays disabled;
original-case dispositions remain 478/136/73/899.

### Standalone owner catalog dispatch and durable ACK boundary

The standalone catalog dispatcher now handles owner decisions, signed ACKs and
exact ACK status instead of leaving the new call variants out of its exhaustive
switch. Internal owner authority is required before catalog access; decision
and status use the same bounded durable projections as clustered metadata.
An ACK joins the existing shared catalog-mutation/standby barrier before taking
the catalog lock. Signature and complete store preflight precede durable apply;
the existing transactional transition independently repeats receipt authority
and CAS checks. No root or capability is implicitly enrolled or activated.

The durable standalone apply/outbox machinery owns the commit. Once dispatched,
an unproved outcome remains unknown, and success requires the exact point-read
receipt digest rather than a revision increment. A normal owner-only increment
advances the local projection epoch without decoding the whole visible catalog;
unexpected concurrent revisions use the existing reconciliation path.

The frozen Debug standalone gate passed all 82 build steps and both selected
tests, with zero skips, failures or leaks. The actual durable-store fixture
proves authority/deadline checks, decoder-floor rejection, corrupt-signature
rejection and unchanged revisions for all rejected operations. The role test
holds catalog and standby locks and proves a standby ACK rejects before either
lock or its undefined request body is inspected. Companion linked inference
checks also passed; they are not index-publication evidence. Successful enrolled
native ACKs through this dispatcher, HA post-commit faults and the shared
standalone commit/post-publication lock split still need activation work and
qualification. Optimized standalone qualification is pending. Public producer
activation and original-case dispositions remain unchanged.

### Enrolled standalone ACK qualification (`b27008d385`)

The storage ACK authority fixture is now shared with the standalone catalog
adapter test, rather than duplicating its enrollment setup or replacing durable
authority with test booleans. The adapter supplies only dispatch of the first
ACK; both native and Raft position domains retain the existing real-store
reporter/root/placement/source-cut faults and reopen verification. The adapter
asserts an initially unacknowledged receipt, the exact committed postimage,
one durable and projected revision increment, rejection of a write replay, and
read-only resolution using the original expected revision. This is a simulated
lost response after successful local dispatch, not a network or HA fault.

The Debug standalone gate passed all 82 build steps and both selected tests,
with zero skips, failures or leaks. This includes the existing rejection test
and the new successful enrolled dispatch for both authority domains. Shared commit/publication lock
separation, automatic signing, enrollment activation, complete native worker
execution and distributed fault campaigns remain required. No public producer
activation or original-case disposition changes are claimed.

The merged branch passes the 16-test isolated PostgreSQL catalog lifecycle
oracle, original inventory integrity and the 2,035-source Apache boundary audit.
These checks do not qualify the pending native dispatcher test or distributed
activation. The initial sandboxed boundary audit was stopped only after a
process sample proved Python was blocked in macOS timezone initialization;
the replacement read-only audit completed successfully outside that sandbox.

### Standalone explicit root enrollment

Standalone enrollment and enrollment-status calls now dispatch through the
existing store-root control protocol instead of returning UnsupportedOperation.
Both require administrator authority before catalog access. Enrollment joins
the shared standby mutation barrier and validates the possession signature,
persisted decoder activation and matching metadata incarnation, and the current
store's advertised root identity. The preflight aborts its write transaction;
durable apply independently repeats identity checks. A committed revision alone
is not success: the exact enrolled identity must be observed after apply.
Normal enrollment advances the projection epoch in constant work without
decoding the full catalog; unexpected revisions retain normal reconciliation.
Neither ordinary registration nor an internal service credential enrolls a root.

The existing topology-activation verifier is shared without changing standby's
stronger relation-publication floor. Enrollment requires its own declared floor,
not a local process capability or an invented membership fingerprint. The new
adapter fixture dispatches both enrollment and ACK against real durable store
authority, and adds foreign-cluster, unauthorized, corrupt-signature, missing
activation and standby-before-lock/body rejection checks. The expanded Debug
gate passed all 102 build steps and 30 component executions: three standalone,
26 publication and one decoder-floor test, with zero skips, failures or leaks.
This includes successful enrollment and ACK dispatch for both position domains.
Optimized qualification is still pending. Automatic signing, complete worker
activation, shared commit/publication lock separation and distributed faults
remain open. Public production and original SQL dispositions are unchanged.

Standalone currently synthesizes local store/placement information for public
catalog listings; that listing is not durable root/reporter or receipt authority.
This new dispatch does not promote those synthetic entries into enrollment or
invent Raft replicas. Production native root/reporter observation and placement
admission still need integration before automatic native publication. An absent
durable store registration remains a rejection, not an implicit local grant.

### Transactional visible-projection watermark (`d274cfbe4d`)

Standalone catalog commits now retain a second durable watermark: the latest
revision that may change the visible table/schema/name projection. It advances
in the same transaction as the global revision, mutation and hot-standby outbox.
Only signed owner ACKs and explicit root enrollment are excluded; every other
and future command conservatively invalidates the visible projection. Missing
watermarks on existing catalogs initialize conservatively, and malformed or
future stamps fail closed before commit.

Catalog/status reads capture both values in one read transaction. If every
commit since the local epoch was owner-only, they update the epoch in constant
work without scanning or allocating a catalog snapshot. Unknown, older,
uninitialized or visibly changed cuts retain complete reconciliation. This
also handles a durable ACK whose response and in-memory epoch update were lost;
it is not limited to the successful-dispatch fast path. Schema/name changes
cannot borrow an owner-only revision classification.

The expanded real-store fixture asserts that enrollment and ACK each advance
the global revision exactly once while preserving the visible watermark. Its
adapter deliberately loses the local epoch update; recovery has no catalog
snapshot available, so an accidental full reload cannot pass. Storage tests
also check empty/missing stamps, visible invalidation, corruption, future
watermarks and abort without a partial restore-job write. The final Debug gate
passed all 102 build steps and 33 component executions: five standalone, 27
publication and one decoder-floor test, with zero skips, failures or leaks.
Optimized validation of this exact source is running in an isolated worktree.
This bounds owner-progress recovery work; it does not split shared
commit/publication locks or qualify distributed activation. Original cases
and public-production barriers are unchanged.

The first combined Debug command passed the owner/standby and decoder-floor
tests but failed the new corruption witness because its restore-job key was
invalid before reaching the watermark check. That command is not a passing
qualification. The corrected witness uses the real restore-job key format and
a valid queued-job value. The final gate additionally exercises visible-table
reload and the existing remote-apply outage/recovery contract. Only that final
corrected command is the complete passing Debug qualification above.

### Remaining native receipt authority boundary at `74c0a6fb96`

Source review confirms `requireIndexOwnerRootReceiptTxn` still requires a
durable placement intent and replica ID even for a native sequence receipt.
Explicit enrollment alone therefore cannot activate the standalone producer:
its public store/placement listing is synthetic, while the physical kernel
owner lease exposes the real applying root generation, not a Raft replica ID.
Do not map a native sequence to a Raft watermark, use a store ID as a fabricated
replica ID, or turn a root-possession signature into placement authority.

Native activation needs an explicitly native physical-owner attestation and
durable current-owner observation: immutable cluster/node/store/root binding,
enrolled public key, current reporter incarnation, exact logical namespace,
physical root generation and native source cut. Startup/report ownership must
be durably fenced; stale reporters and replaced/restored roots cannot refresh
the registry. The verifier must read that native authority in the same
transaction as the progress CAS, independently of caller truth flags. Raft
receipts must retain their actual placement/replica and observed log checks.
The issuer must sign the root generation returned by the applying/recovering
lease, never a later routing lookup. Codec/capability activation, provenance
adoption and failure campaigns must precede public worker activation. These
native authority changes remain implementation work, not claims of this
enrollment or projection-watermark checkpoint.

### Explicit native versus Raft index attestation

Index root attestations now explicitly select an actual Raft replica ID or the
digest of the durable standalone native-owner binding. Their v2 signature domain
binds that discriminant as well as the enrolled identity, reporter, applying
root generation and complete logical receipt digest. A correctly signed proof
using the wrong physical authority is rejected before catalog access. Empty
replica IDs and binding digests cannot be signed. This replaces the unpublished
index receipt format added in this PR; no compatibility path for that format is
introduced. The existing coordinated admission decoder floor remains required.

The transactional verifier keeps shared enrollment, live store/reporter/root,
metadata incarnation, namespace, physical generation and observed-cut checks.
Raft receipts additionally require the actual serving placement/replica and
observed term/index. Native receipts instead require the exact durable native
binding; they cannot borrow a Raft placement or watermark. The binding digest
is canonical, domain-separated and allocation-free. The existing durable
store-report machinery supplies native current-root/source observations, so
no separate parallel report registry is introduced. Missing native ownership
fails closed and corrupt binding bytes remain a catalog-integrity error.

The shared fixture now bootstraps a real standalone catalog and admits its
native binding before publication, while native owners have no Raft placement.
Fault cases check missing/replaced native binding, genuine signatures with a
foreign binding digest, stale reporter/root/source observations, and authority
independence. A test-only reload counter preserves the owner-only recovery
complexity check now that the fixture has a real catalog snapshot. The new
Debug verifier/standalone gate passed all 102 build steps and 33 component
executions (five standalone, 27 publication and one decoder-floor), with zero
skips, failures or leaks. The HTTP ACK/report-authority gate passed all 76 steps
and 66 component executions, also with zero skips, failures or leaks. These
selections overlap the publication fixture; they are not 99 distinct contracts.
The direct canonical binding test passed, including every identity-field
digest substitution. Its corrected isolated harness uses the same empty
source-owner options as the normal build; an initial direct invocation missing
that required module failed at compilation and is not passing evidence.
Optimized gates for earlier checkpoints do not certify this new wire format
or verifier. Production native registration/report publishing, root signing from
the exact applying/recovering lease, complete worker activation, adoption and
distributed fault qualification remain open. Public production is disabled
and original-case dispositions are unchanged.

### Pinned native signing view and receiver receipts

The native owner permission transaction now optionally projects a fixed-size
signing view from the persisted native binding, current live store/reporter and
explicit administrator root enrollment. It does not use synthetic placement
listings or caller truth flags. Unregistered, unready and unenrolled identities
leave progress unsigned; malformed persisted authority fails closed. Raft and
acknowledged decisions carry no native signing view. No schema-sized input or
table scan is added to this hot path.

Fresh and recovered control results own a copy of that permission cut, its
expected head revision and the exact applying/recovering lease root generation.
The standalone data receiver can load an existing physical-root signing key
and attach an index ACK after checking configured node/store/reporter identity.
It never creates a replacement key after applying an effect. Missing identity,
changed authority and signing errors retain the owned committed outcome with
a bounded typed signing failure; they are not apparent mutation rollbacks.
Admission and replication-pending outcomes cannot produce ACKs. HTTP response
validation binds an optional signed ACK to the actual outcome receipt, applying
root and native binding digest. This consistency check does not replace the
independent transactional metadata publication verifier.

The first corrected Debug publication gate passed 59/59 build steps, including
28 publication/component tests and one decoder-floor test, with no skips,
failures or leaks. An initial invocation used the unrelated Homebrew Zig 0.16
on PATH and failed before branch compilation; all qualification uses the
repository Zig 0.17 compiler. The first 0.17 compilation exposed a misplaced
signing-context declaration; it was corrected before the passing gate. After
additional native projection fault checks, the final HTTP/authority Debug gate
passed 76/76 build steps and 67 component executions (28 publication, four
ACK/catalog-identity, 19 plus 12 split-owner runtime-status/durable-report/ACK,
and four service protocol-authority tests), without skips, failures or leaks. The compiled
data-receiver Debug gate passed 91/91 steps and its one selected receiver test,
also without skips, failures or leaks. The selections overlap and are not 68
distinct original SQL contracts. License-boundary (2,035 source files), original
inventory integrity, Zig 0.17 formatting and diff checks passed. These component
tests do not prove
complete public worker activation, durable producer registration/reporting,
full distributed lifecycle/recovery, or original-case SQL coverage. Public
native production remains disabled; the original inventory is unchanged.

The optimized projection-watermark gate for frozen checkpoint `d274cfbe4d`
also completed successfully: 102/102 build steps and 33 component executions
(five standalone, 27 publication and one floor test), without skips, failures
or leaks. That historical checkpoint predates the explicit native proof and
this receiver signing implementation, so it does not certify either change.

### Durable native reporter registration CAS

Standalone native reporter admission now has a signed, domain-separated
startup contract binding the complete root identity, desired nonzero process
reporter and optional expected reporter. An absent predecessor and an explicit
zero reporter have different signature preimages. This grants neither root
enrollment nor publication authority. A steady-state report publisher must not
read a newer preimage and re-register itself; issuing registration belongs to
startup under the actual exclusive physical-owner lease.

The existing standalone catalog transaction accepts this registration only as
a registration-only update, against an already admitted native binding and a
durable root-signing capability floor. It compares node/store/root/cluster and
any previously registered key, then CASes the current reporter. An exact retry
preserves status generation and report baselines; a different reporter requires
the signed predecessor. Store admission reuses the actual StoreRecord registry,
normal capability/drain checks, buffered projection notifications and committed
readback. Registration, standalone revision and any standby outbox are atomic.
Registration-only commits do not invalidate the visible table/schema projection.

The initial Debug publication gate passed 59/59 steps, including 30 publication
tests and the decoder-floor test, with no skips, failures or leaks. This includes
signature substitution cases and an actual committed/reopened registration
test. A stronger final gate additionally checks that an exact duplicate retains
an already advanced status generation and compiles the standalone consumer; it
passed all 102 build steps and 36 component executions (five standalone, 30
publication and one decoder-floor), with zero skips, failures or leaks. The
original inventory integrity and 2,035-file license-boundary audits passed,
along with Zig 0.17 formatting and diff checks. No original SQL-case disposition
changes are earned, and optimized registration qualification remains open.

This is the durable registration primitive, not completed producer startup or
automatic worker activation. Source tracing confirms the present standalone
DataServer registration/status loop returns early without remote metadata.
Startup integration must prove the actual exclusive process/root lease and
publish real native source/root observations through the shared report machinery.
The current reused store replacement can walk an existing report membership;
before activation, reporter turnover must instead invalidate observations with
a constant-size generation switch and retire old/pending uploads through bounded
baseline/GC steps. Do not introduce a parallel observation registry, preserve
old reports under a new reporter, or silently drop incomplete uploads. These
startup, bounded retirement, reporting, worker and distributed-fault obligations
remain open; public native production stays disabled.

### Report retirement safety qualification and staged turnover

The existing baseline GC now rejects an active/garbage generation alias and
invalid fragment/page cursors, failing closed instead of deleting live reports
or leaving an impossible cursor silently stuck. This is a deletion-prevention
guard, not a new namespace, retirement protocol or automatic producer activation.

An isolated temporary-store regression seeds 130 retired reports, a distinct
serving generation, incomplete upload bytes/metadata, another store and an
unrelated user-row sentinel. It commits one fragment retirement, reopens the
actual store and resumes three page steps. Transaction mutation capture checks
every touched key against an independently constructed retired-only allowlist
and enforces a maximum of 76 touched keys per step. Each committed cut preserves
the serving report, pending upload state/bytes, other store and user sentinel.
Active-alias and corrupt cursor faults must fail before touching any key. This
is bounded mutation/scope evidence, not an elapsed-time benchmark or complete
distributed fault qualification. The initial regression-only Debug gate passed
59/59 steps, including 31 publication tests and one decoder-floor test, without
skips, failures or leaks. The final guard/authority Debug gate passed all 76
build steps and 68 component executions (31 publication, one decoder-floor,
19 plus 13 split-owner status/report tests and four service authority tests),
without skips, failures or leaks. The broader catalog/baseline Debug gate passed
47/47 steps and 205/205 tracked tests, plus its separate 25-test owner slice.
These overlapping selections are not original SQL-case dispositions. License
boundary (2,035 files), inventory integrity, Zig 0.17 formatting and diff checks
passed. The optimized registration checkpoint `49ded4183d` independently
passed 59/59 steps (30 publication and one floor), also without skips, failures
or leaks; it predates this guard and cannot certify it.

Auto-review rejected the proposed production namespace/generation-switch and
new cleanup patch because incorrect retirement could lose durable metadata.
That patch was not applied, and no workaround is authorized. The isolated
safety regression and the narrower deletion-prevention guard were approved.
Production turnover remains unchanged pending explicit approval or sufficient
additional evidence for a narrower validated rollout. Required stages are:

1. Keep automatic production disabled. Add a read-only turnover plan bound to
   the durable native binding, exact reporter CAS and actual exclusive startup
   lease. Reject active/upload/garbage aliases and generation-address reuse.
2. Introduce a coordinated decoder floor for the new baseline address/state
   semantics. Existing all-zero legacy addresses and published nonzero sequences
   must retain their current key mappings. A startup empty generation must never
   be interpreted as legacy by any allowed reader, standby or restore decoder.
   Merely reusing root-signing floor 19 is insufficient for this format change.
3. Reuse the one shared baseline state: active, pending upload and one retired
   generation. Signed startup requests distinguish admission from bounded
   collection. Preserve uncertain-commit identities and never report a committed
   collection step as a failed admission or silently discard pending frames.
4. Atomically switch the header and empty active generation with revision and
   standby outbox. Admit only after prior garbage/pending-upload retirement can
   proceed without an unbounded loop. New reporters receive no predecessor
   observations; fresh reports must establish their source/root cuts.
5. Retire only the proven non-live report namespace, one page/frame per step,
   retaining durable restart cursors. Test crashes at every cut, stale reporters,
   root/key replacement, snapshot/reopen, incomplete uploads, legacy reports,
   standby outage and lost replies before wiring automatic startup/worker use.
6. Benchmark admission with increasing old-report cardinality and capture point
   reads, bytes, allocations and apply-lock time. Admission must be independent
   of old membership size; collection keeps the deterministic mutation budget.
   Mutation counts alone cannot establish constant elapsed time or read cost.

None of these remaining stages earns original SQL-case completion by design
intent alone. The 899 unresolved originals and public activation gates remain.

### Native text fingerprint qualification

The next independent SQL cohort adds the shared native text MD5 kernel and
bounded prefix comparison; it does not change the approval-blocked native
report-generation rollout above. The original sql-0257 SQL and parameter remain
unchanged. Its separate `digest_read` profile uses the original mega-branch
MD5 regression's uppercase `ACTIVE` witness, retaining duplicates, lowercase
nonmatches, an explicitly nullable column and unrelated UTF-8 data. The existing
91-read profile/golden is unchanged.

Validation: the final Debug public API gate passed both the original digest and
existing read tests (80/80 build steps, zero skipped/failed/leaked tests). Three
focused native scalar tests passed strict semantics, quota/cancellation,
constant-space linear-work scaling and allocation-fault sweeps. Independent
PostgreSQL 18.6 checks passed the exact digest campaign, all 91 existing reads,
126 scalar contracts and 118 oracle-generator tests. The Apache boundary audit
and 20 inventory-verifier tests passed. This is not PostgreSQL 19 qualification,
bytea-overload activation or a latency benchmark.

The audited original inventory is now 479 implemented / 136 rejected /
73 superseded / 898 unresolved. No other source cases received credit.

### Typed temporal boundary and execution clock foundation

Native date, time, timetz, timestamp, timestamptz and interval remain distinct
typed domains. The allocation-free binary codec validates finite limits,
infinity sentinels, exact framing, signed epoch conversion and precision before
publishing bytes. Intervals retain month/day/microsecond components; 24:00 stays
distinct from midnight. Execution-owned clock views pin statement time, accept
transaction start from the authorized owner and sample wall time through std.Io.
Child pages reuse the view; cached plans must never retain it.

An independent PostgreSQL 18.6 oracle supplies 98 binary/type/precision contracts.
Calendar fields retain exact boundary microseconds where EXTRACT(epoch) rounds;
future SQL EXTRACT support must separately match that observable SQL behavior.
Nine native tests and five oracle tests cover framing, boundaries, roundtrips,
bounded-memory throughput and clock ownership. This is not PostgreSQL 19
qualification or public SQL activation.

Remaining activation requires typed Datum/schema/AROW and index-key integration,
pgwire text/binary codecs, parser/scalar binding, timezone/calendar semantics,
ingress/cursor clock capture from durable transaction ownership, and capability
barriers before persisted/public type activation. No original case receives
credit until its unchanged query passes public execution. Counts remain
479 implemented / 136 rejected / 73 superseded / 898 unresolved.

### Observable generated-result verification without rendering

Generated restore verification now distinguishes observable display from SQL
logical equality. JSONB equality uses a bounded structural walk plus numeric
display scale: equivalent spellings such as 1.20 and 120e-2 match, while 1.20
and 1.2 do not. Object key ordering and JSON escapes do not create false
differences. No canonical output string, exponent expansion, sort vector or
heap allocation is required. Query comparison and semantic hashing are unchanged.

NUMERIC retains borrowed canonical views for both value and scale comparison,
validating each operand once. Scalar floating zero retains its sign. Arrays
use chunked exact-byte equality within the active canonical codec, preserving
scale, zero bits, dimensions and SQL NULL/JSON null distinctions without
decoding JSONB elements again.

The array fast path has an explicit authority boundary: Value.Array admits only
codec-validated pinned bytes; verifyRow's caller has already verified physical
canonicality/checksums, and parsed logical envelopes pass through the owning
canonical encoder. This verifier grants no producer admission and is not a
cross-format migration identity. Strict producer/restore byte validation was
not relaxed.

Logical restore keeps declared-domain coercion semantics. Equivalent display
aliases for NUMERIC(p,s), including NUMERIC arrays, are first checked for any
coefficient change and then normalized only in an unpublished typed shadow.
Generated verification uses that domain-normalized input. The supplied document
is unchanged, defaults are not filled, generated expressions are not repaired,
and coefficient changes are rejected. Cold physical verification has no such
transport-alias allowance.

An independent PostgreSQL 18.6 fixture covers 19 JSONB logical/display pairs;
another oracle test checks NUMERIC scale and floating signed zero. Debug tests
preserve logical hashes, exercise every observed cancellation cut and exact
parent/local quota, and compare a huge positive exponent under 128 work units
without rendering its 131,072-character display. Six native forgery classes
(JSONB, NUMERIC, floating zero and their array forms) remain SQL-equal yet fail
generated verification in both typed values and canonical cold rows. Valid
counterparts continue to pass.

A distinct-buffer Debug operator sample compared 1,000 JSONB cells over eight
iterations: the previous logical-check path took 49.3 ms versus 0.475 ms for
display-aware canonical verification, with zero verifier allocations. Both
operands occupy independent 30,145-byte buffers. This measures the verified
canonical-array boundary, not arbitrary producer bytes or production restore
throughput.

Final production Debug qualification passed 1,043 SQL tests (three existing
skips), 111 protocol tests and 113 transaction tests. The final native rerun
passed all 257 tests, including both scalar and array NUMERIC transport aliases,
without failures or leaks. Two independent PostgreSQL oracle tests passed.
Formatting, control-catalog freshness, the 2,048-file Apache boundary/license
headers and original inventory integrity passed.

Public JSON schema/SDK activation, typed projected consumers, reader/capability
barriers and distributed activation qualification remain open. Original
accounting remains 479 implemented / 136 rejected / 73 superseded / 898 unresolved;
these component proofs do not establish the public original SQL cases.

The shared temporal text boundary now provides allocation-free canonical
ISO/YMD output (UTC for timestamptz) and postgres-style interval output, plus
strict canonical decoding for all six domains. Gregorian conversion spans the
entire DATE domain, including astronomical year zero internally/SQL 1 BC
externally; timestamp decomposition uses floor division before the epoch.
TIME 24:00 remains distinct, while timestamp assembly normalizes it to the next
day and checks the final timestamp bound. Named zones and general PostgreSQL
literal spellings are explicitly not canonical-codec responsibilities.

The oracle now records 174 independent text/binary/field/precision contracts,
including mixed-sign intervals and minimum/maximum finite time components.
All 17 native tests pass in Debug and ReleaseFast; 12,000 randomized full-domain
text/binary roundtrips and 3,000 arbitrary-byte inputs exercise the bounded
decoder. Six independent PostgreSQL tests and fixture regeneration checks pass.
One local ReleaseFast sample formatted/decoded 100,000 mixed values in 19.7 ms,
with fixed caller-owned output and no heap allocation. This microbenchmark is
not a query-latency result or evidence of public temporal activation. The
remaining activation work and original inventory counts above are unchanged.

Exact precision coercion covers timestamp, timestamptz, time, timetz and interval
at every precision from zero through six. Interval months/days and timetz
offsets survive rounding unchanged; finite interval carry overflow is an error,
not wrapping or infinity. PostgreSQL independently returns 22008 for that
overflow; eventual SQL activation must retain that diagnostic. The native gate
is `zig build sql-temporal-test` (also a dependency of `unit-test`); the independent
fixture gate is `scripts/generate_sql_temporal_reference.py --check` with the
declared psycopg runtime and owned PostgreSQL test server.

### Typed temporal operator ownership and comparison

Temporal values now survive scalar dispatch, owned row clones, retained columns,
hash joins, tuple membership, DISTINCT, normalized in-memory sorting, portable
spills and aggregate checkpoints without conversion to JSON or floating point.
Pattern callbacks and temporal payloads share one discriminated pointer-sized
extension: a target-specific regression requires Datum to remain no larger than
its previous layout. This preserves the existing small-statement memory budget;
no quota or allocation-failure assertion was relaxed.

Comparison identity is deliberately separate from physical bytes. Interval
comparison uses 30-day months with widened arithmetic while the owned value and
spill retain months, days and microseconds independently. Timetz compares
unwrapped GMT time, then the stored offset. Equal comparison identities share
the grouping/join hash and sort prefix. Neither this hash nor its temporary
sort key is a durable row-content fingerprint or a versioned native index codec.

The independent PostgreSQL 18.6 oracle now includes 21 pairwise comparisons in
addition to the existing 174 text/binary/precision contracts. Native tests cover
actual hash joins and membership, retained ownership, checkpoint/spill recovery,
truncated and corrupt frames, fixed working-memory limits and allocation faults.
The full Debug SQL gate and focused ReleaseFast temporal operator gate pass;
the standalone temporal gate and six PostgreSQL oracle tests also pass.

The final revision passes 21/21 full Debug SQL build steps (728 main-owner tests
and 226 additional-owner tests, three main-owner skips, no failures or leaks),
all 18 focused temporal tests in Debug and ReleaseFast, and the 80/80-step
public API gate selecting the existing HTTP and pgwire typed-array regressions.
Parameter ingress validates the extension before SQL NULL normalization; an
allocation-free regression rejects contradictory, missing and out-of-domain
payloads. Formatting, the 2,038-file Apache boundary, inventory integrity and
all 20 inventory-verifier tests pass. These overlapping gates are not additional
original-case qualifications or a CI-green claim.

### Execution admission and durable SQL clocks

The production runtime and pull cursor now pin an immutable clock view before
execution binding; derived sources and scalar evaluation inherit it. Describe
does not capture time, and cached programs contain no clock source or cut.
Views are value-owned by statement/cursor contexts, with their live source
borrowed from the backend owner. Pinning and child-page reuse allocate no clock
objects or perform repeated source reads. Source failures precede row access;
invalid inherited cuts and mismatched transaction starts fail closed.

The HTTP executor samples one statement cut before choosing delivery/fallback.
Its internal guarded autocommit retains implicit SQL transaction semantics even
when the implementation's durable wrapper begins later. Explicit sessions obtain
their transaction instant from the authenticated owner, not parameters or JSON.
The existing durable begin timestamp now survives both cached and cold SqlState
reads and the native Session owner contract. Savepoints/failure recovery do not
reset it. Conversion floors signed nanoseconds to microseconds without floating
point.

Pgwire's direct portal path bypasses the main execution method, so it pins its
own statement cut and attaches the durable transaction time only after principal
and session-scope checks. The retained stream keeps this view across FETCH pages.
Wall time remains separately sampled and may move backwards; statement and
transaction values remain unchanged.

Native regressions cover clock-free Describe, 1,000 inherited views without
resampling, released cursor pages, derived execution, materializing fallback,
new statement cuts, backward wall time, source failures and allocation faults.
The full Debug SQL gate passes 21/21 build steps: 732 main-owner tests and 226
additional-owner tests passed, with three main-owner skips and no failures/leaks.
All four focused native clock tests pass in Debug and ReleaseFast. These totals
overlap and do not qualify additional original SQL cases.

The final public API gate passes 80/80 steps and nine selected executions
(four native clock tests plus five API/session/pgwire tests), without skips,
failures or leaks. The direct portal helper's deterministic test verifies
durable/implicit transaction cuts, microsecond quantization, backward wall time
and unchanged values across 1,000 page views. It is not an end-to-end SQL time
function test. All six independent PostgreSQL oracle tests and temporal fixture
reproducibility pass. Formatting, diff checks, the 2,039-file Apache boundary
and original inventory integrity pass.

Public now/CURRENT_TIMESTAMP/statement_timestamp/clock_timestamp activation is
still open. It requires exact temporal type/result identities and pgwire codecs,
volatility-aware binding, session timezone rules, native typed result ownership
and protocol-message receive cuts (earlier than execution admission where
applicable). The clock contract is not a sampled deterministic golden, an
alias for a document-datetime string, or completion of temporal SQL parity.
The original inventory remains 479 implemented / 136 rejected / 73 superseded /
898 unresolved.

### Full-domain timestamptz wire boundary

The pgwire timestamp codec no longer narrows time through unsigned Unix
nanoseconds or unsigned epoch-day arithmetic. Binary OID 1184 now uses the
validated native signed PostgreSQL-epoch codec; finite transport nanoseconds
use i128, and both infinities remain explicit values rather than SQL NULL.
Result normalization accepts canonical PostgreSQL UTC text and the existing
native RFC3339 boundary. Precision finer than a microsecond is still rejected
at wire encoding rather than silently rounded or truncated.

Text output uses canonical PostgreSQL ISO/YMD UTC spelling across the entire
timestamp domain, including BC and large positive years. Direct text and binary
cell streaming use fixed caller storage with no heap allocation. Allocated
encoding owns only its final buffer. This is a result/binary-frame boundary,
not a general PostgreSQL timestamp input-literal parser or session-timezone
implementation.

All 36 independent PostgreSQL timestamptz entries now exercise allocated and
direct streaming codecs, exact text/binary output, result normalization,
truncated frames, output-byte admission and individual allocation-failure
cleanup. Incorrect historical rejections of pre-Unix values and infinity were
replaced by exact PostgreSQL expectations; out-of-domain finite frames and
precision errors still fail before publishing bytes.

The complete pgwire gate passes all 106 tests in Debug and ReleaseFast. A local
ReleaseFast sample encoded 100,000 mixed values in both formats (200,000 cell
encodes) in 13.5 ms with a 128-byte output buffer and zero heap allocation.
This is not a query-latency comparison or a before/after speedup claim. Six
independent PostgreSQL oracle tests, fixture reproducibility, the 2,039-file
Apache boundary and inventory integrity pass.

The focused public API gate also passes all 80 build steps and three selected
regressions: binary datetime parameter coercion, bounded scalar pgwire execution
and original stored-array typed reads, with no failures or leaks.

The native parameter bridge now preserves signed pre-Unix nanoseconds where
the existing document-datetime type permits them. Full-domain typed parameter
admission, temporal schema/row/index capabilities, SQL type binding and public
time-function activation remain open; a wire codec passing is not evidence
that those layers accept every PostgreSQL timestamp or infinity. No additional
original case receives disposition credit.

Public result and storage boundaries continue to reject these new payloads
explicitly until exact schema/type identities, AROW/index capability versions,
pgwire codecs, temporal arrays, parser/binder rules and execution-owned clock
capture are activated together. General temporal literals, named-zone/DST
semantics, arithmetic and SQL JSON formatting also remain open. This delivery
does not qualify any additional original SQL case: the inventory remains
479 implemented / 136 rejected / 73 superseded / 898 unresolved.

### Execution-owned typed clock values

Statement and cursor owners now retain an immutable `Clock.Frame` at a stable
address. Its transaction/statement timestamps provide native timestamptz values
at precisions 0 through 6 without per-row allocation, parsing, rounding or clock
sampling. Wall time deliberately has no cached slot. Finite rounding overflow
is recorded per precision and fails only when that precision is demanded;
unrelated expressions remain admissible. Infinite owner timestamps are rejected.

Runtime results and ordinary cursors own their frames; derived adapters and
parallel scan lanes borrow the enclosing execution's frame. Parallel workers
join before their parent releases that owner. Scalar limits carry the borrowed
typed-value address, never a pointer into a temporary evaluator or cached plan.
Inherited frames must match both pinned timestamps and the exact clock source;
a stale or detached frame cannot silently acquire a new statement cut.

The full SQL Debug gate passes all 21 steps: 734 main-owner and 226 additional
owner tests pass, with three existing main-owner skips and no failures/leaks.
Six focused clock tests pass in Debug and ReleaseFast. The linked public API
gate passes all 80 steps and three selected HTTP/pgwire regressions. Precision,
repeated pointer identity across cursor pages, stale-source/cut rejection,
declined-stream fallback and derived reads are covered. The fault sweep retains
every injected allocation cut using a deterministic no-resize/remap wrapper;
the normal run separately retains the production allocator behavior. Six oracle
tool tests and exact regeneration of 174 PostgreSQL contracts pass.

This activates owned scalar clock storage, not the public SQL functions.
Parser/binder time expressions, exact public result activation, volatility,
protocol receive-time cuts and session-timezone semantics remain unfinished.
No original SQL disposition changes: 898 remain unresolved.

### Native transaction-time expression activation

`now()`, `transaction_timestamp()` and `CURRENT_TIMESTAMP` (precision 0 through
6) now borrow immutable native timestamptz values from the execution-owned
clock frame. They do not allocate, sample wall time, or round each output row.
Stable-function metadata prevents their values from becoming cached plan
constants. Lazy branches preserve demand-driven precision overflow and do not
require a clock when the time expression is not evaluated.

Native timestamptz results retain their typed payload through scalar selectors,
derived reads, cursor pages, materialized results, public HTTP serialization and
pgwire delivery. The existing public `datetime` result identity is OID 1184;
other native temporal kinds cannot be relabeled as that type. Physical storage
and temporal-array capability guards remain in place. This is not full temporal
schema, parameter, row/index or timezone activation.

The independent PostgreSQL 18 oracle corrects an important earlier cursor
assumption: `now()` stays transaction-stable across successive SQL `FETCH`
commands, but `statement_timestamp()` changes for each command. Retaining a
statement cut across internal pages is not sufficient for exposing that
function across separate protocol commands. `statement_timestamp()` therefore
remains unexposed pending per-command rebinding and ownership of retained values;
`clock_timestamp()` also remains unexposed pending volatile evaluation support.
Receive-time cuts, rather than execution-admission cuts, and exact implicit
protocol transaction boundaries remain required for full PostgreSQL parity.

PostgreSQL accepts precision greater than 6 with SQLSTATE 22023 WARNING and
reduces it to 6. Until public notice delivery exists, that shape explicitly
returns unsupported instead of silently dropping the warning. Negative and
noninteger precision syntax remains invalid. PostgreSQL nondecimal integer
tokens now pass the shared lexer with validated prefixes and separators.

The regression scope includes all seven supported precisions, zero-allocation
repeated evaluation, missing clock owners, lazy branches, native result type
checks, allocation-failure cleanup, pgwire full-range/infinity delivery and
public HTTP streaming/materialized paths. These component regressions do not
qualify additional original inventory cases by themselves. Original case
accounting remains unchanged at 898 unresolved; mixed legacy document-datetime
comparisons, clock-bearing EXECUTE argument evaluation, session timezone/input
semantics and the remaining temporal capabilities are still unfinished.

Qualification on the tree including origin/main `b467a7f19f`: full SQL Debug
passes 21/21 build steps, with 741 main-owner and 226 additional-owner tests
passing, three existing skips and no failures/leaks. All 11 focused clock and
nondecimal-integer regressions pass in Debug and ReleaseFast. The public parity
gate passes 80/80 steps and both new native HTTP/pgwire adapter tests; the broader
API gate also passes all 117 steps. These overlapping selections are not
additional original-case credit or a CI-green claim. Nine independent fresh
PostgreSQL 18 oracle tests and reproducibility of all 174 temporal contracts
pass. The control-only source catalog now exposes the same native temporal
contracts as the physical consumer, preserving linked test ownership rather
than dropping the failing control-boundary compilation check.

### Demand-aware immutable generated expressions

Stored generated expressions now use a bounded immutable planning pass after
complete binding, type/input validation and generated-reference checks. The
pass simplifies constant CASE, COALESCE and Boolean demand before checking
volatility. Stable or volatile functions are never sampled to prove a constant;
dead stable calls do not make an otherwise immutable expression inadmissible.
Reachable mutable expressions return PostgreSQL SQLSTATE 42P17. Formatting
casts between row text and timestamptz are stable, while resolved literal input
functions retain their binding-time identity. The supported numeric-only
`to_timestamp(double precision)` overload is immutable; it must not inherit
the distinct text-format overload's stable classification.

The plan retains one bounded arena, memoized typed constants and a shared
scalar evaluator with aggregate work/output admission. Child constants are
borrowed instead of recursively reevaluated for each parent. A second reachability
walk lowers only the rewritten live DAG, so pruned calls cannot leak into the
durable expression VM. Generated expressions fold demanded immutable failures
at DDL planning time; ordinary DEFAULT expressions retain write-time arithmetic
failure semantics. Existing temporal/schema capability guards are not bypassed.

CREATE carries the backend's non-suspending scalar cancellation/deadline control
through schema construction and planning, with a final checkpoint before DDL
dispatch. ALTER uses the same planning limits with its request context and
publishes only a fully prepared candidate. Exhaustive cancellation cuts cover
planner traversal, reachability and lowering: canceled CREATE never
dispatches, and canceled ALTER leaves the original schema byte-for-byte intact.
Malformed root and cyclic/forward dependencies fail as invalid programs rather
than escaping the bound topological DAG contract.

The independent PostgreSQL fixture records 34 complete CREATE TABLE admission
contracts and full stored generated-row results for 19 admitted shapes. Seven
oracle tests cover stable calls/casts, dead branches, binding-before-pruning,
Boolean order/NULLs, overload volatility and generated-versus-default failure
timing. Native schema admission matches all 34 contracts. Fault injection covers
every plan allocation cut. All seven focused native planning regressions pass
in Debug and ReleaseFast. Deterministic scaling uses 255, 1,023 and 4,095 bound
nodes with exactly 509, 2,045 and 8,189 scalar fold steps; only the folded root
remains reachable. This proves linear fold work for that balanced-DAG campaign,
not universal query speed or complete DDL activation.

The native HTTP regressions verify all 15 rejected shapes' exact PostgreSQL
SQLSTATEs. The storage/query regression writes all 19 admitted shapes, closes
and reopens each database, and checks base fields, generated results, types and
SQL NULLs against the oracle. SQL integer transport is deliberately lossless
text, not a JSON number. This exercises native schema preparation, storage and public reads;
it is not public CREATE/ALTER catalog lifecycle or distributed-fault evidence.

Final generated-expression qualification: full SQL Debug passes 21/21 build
steps, with 748 main-owner and 226 additional-owner tests passing, three existing
skips and no failures/leaks. Focused public parity passes 80/80 steps and both
HTTP regressions. The broader API gate passes 117/117 steps, including all 437
main API-owner tests, with no failures, skips or leaks. These selections overlap
and are not additional original-case credit or a remote CI-green claim.
Formatting, generated control-catalog freshness, original
inventory integrity, its 20 audit unit tests and the 2,040-file Apache boundary
pass. A final rerun's old local cache disappeared while compilers were active;
these final gate results use a fresh private cache, not the failed runs.

Original-case accounting remains unchanged. General generated-expression VM
coverage, partial/expression-index admission and distributed catalog lifecycle
qualification remain required; these component contracts do not certify all
340 unresolved DDL originals or complete native-index production activation.
The shared planner's index follow-up must preserve full-versus-empty membership
for constant TRUE versus FALSE/UNKNOWN predicates, alongside immutable key and
predicate admission. An isolated 16-case PostgreSQL probe confirms dead mutable
calls are pruned, reachable mutable calls reject with 42P17, and demanded
constant errors retain their ordering. That exploratory probe is not a checked-in
index fixture or original-case qualification.

### Immutable ordered-index admission and constant membership

Expression keys and partial-index predicates now use the same bounded,
demand-aware immutable planner as stored generated expressions. Binding precedes
pruning; reachable stable/volatile operations reject with PostgreSQL 42P17 in
the appropriate index context, while unreachable branches do not prevent valid
definitions. The existing request cancellation/preparation limits also cover
these index programs. CHECK and ordinary DEFAULT evaluation retain their
different volatility/error-timing contracts.

Partial membership has an explicit constant form, rather than a fabricated
column comparison: TRUE imposes no restriction; FALSE and SQL UNKNOWN select no
rows. The shared native plan binds every remaining ordinary conjunct before
absorption and refuses an empty index for a query unless that query is provably
contradictory. Constant-false write preparation and historical projection skip
key, covering and row-decoding work. The 1,024-row work-count regression requires
zero key/payload bytes and zero scratch capacity, not a timing-based speedup.
Constant-true fingerprints remain identical to full-index comparison semantics;
ordinary native predicate serialization retains its exact previous spelling so
existing durable definition digests do not change.

The generated OpenAPI predicate model follows the existing index-key pattern:
optional variant fields, with authoritative server validation rejecting mixed
or incomplete variants. Go uses an explicitly optional boolean pointer so
`constant: false` is not silently omitted. Python and TypeScript retain the same
public model; TypeScript's index validator handles both forms and rejects mixed
variants. A requirements-only `oneOf` was tested and removed because current
generators erased the Python model and emitted unsuitable Go serialization.

The independent PostgreSQL 18 fixture contains 20 DDL contracts (14 admitted,
six rejected), including logical membership and expression-key results over
NULL, empty-string and ordinary input rows. No native implementation generates
these expected values. The native typed-row oracle test belongs to the physical
index owner, not the lightweight SQL owner: importing the full document mapper
into the latter introduces unrelated vector-module dependencies. Public HTTP
tests independently check all six rejection SQLSTATEs before read capture.

This is not full general partial-predicate support, native worker activation,
distributed index lifecycle certification, or credit for unresolved originals.
General non-conjunctive predicate implication remains guarded; original-case
accounting stays 479 implemented / 136 rejected / 73 superseded / 898 unresolved.

Constant predicate variants require ASCH/reader capability 26, independently of
physical column types or typed expression programs. Derivation checks both
ordered indexes and unique constraints; immutable schema cloning, strict binary
decoding and transactional catalog validation retain the fence. Serialization
refuses a downgrade to capability 25, and text-index projection fingerprints
exclude this unrelated feature. Decoder tests cover every truncation and a
malformed capability boolean without allocating. This is a decoder capability,
not authorization to activate production index workers or namespace turnover.

Qualification at `a4a69e99fe` (2026-10-12) passed the full Debug SQL owner
(750 tests plus 226 linked SQL tests; three existing skips), the full Debug
native index system owner (203 tests), and the public HTTP immutable-index
gate (one HTTP test plus nine linked native tests). The focused ReleaseFast
SQL gate passed, including the schema capability fence; this is not a full
ReleaseFast native-index lifecycle qualification. The PostgreSQL 18 oracle
reproduced all 20 contracts and its eight audit tests passed. Regenerated SDK
checks passed: Go relational tests, Python's 17 relational tests, and TypeScript
type checking plus 482 tests (one existing skip). Generated OpenAPI/source
catalog freshness, all 20 inventory audit tests and the 2,040-file Apache
boundary passed. `make generate` and `make fmt` completed successfully.

The subsequent merge of `origin/main` at `0f7fec50c5` changes documentation
and inference comments only, not executable SQL/index code. Original-case
dispositions remain unchanged; public production activation and the broader
predicate/expression contracts above remain open.

### Shared bounded literal-text execution foundation

The next durable-expression cohort is text transformation, including original
expression indexes using REPLACE and MD5. Query execution now shares a native
literal-text kernel suitable for that VM boundary; durable expression exposure
is not yet enabled. MD5 retains fixed-size lowercase text output and its shared
invocation work owner. REPLACE compiles failure links for linear-time,
non-overlapping literal matching instead of using uncharged repeated search
and a growing output buffer. Short patterns use fixed local scratch; longer
patterns admit their link capacity against the same invocation byte allowance.
No-match and empty-pattern results borrow the pinned input. Changed results use
one exactly sized allocation after a bounded counting pass. Both passes and
output copies poll the existing sticky cancellation/work owner.

The caller-owned immutable Pattern interface permits durable plans to retain
failure links without per-row preparation. Native regressions include 23,808
binary overlap witnesses checked against an independent direct scan, Unicode
and empty-pattern contracts, exact-size/zero-capacity allocators, allocation
fault sweeps, every observed cancellation cut, sticky quota failures and true
OOM distinction. Independent PostgreSQL 18 verification now covers 138
text/JSON scalar contracts, including twelve additional REPLACE witnesses.
One ReleaseFast sample processed 1,024 prepared late-miss searches over 16 MiB
in 60.7 ms with zero per-row heap allocation and 33,489,920 charged work units.
Fourfold input growth (4,096 to 16,384 bytes) used 8,254 to 32,830 units,
including pattern preparation. These are kernel measurements, not end-to-end
latency comparisons or additional original-case qualification.

Still required: typed durable VM operations and SQL lowering, a reader/catalog
capability fence, immutable literal-pattern preparation and complete
generated-column/index/check/restart qualification before public activation.
The original inventory remains 479 implemented / 136 rejected / 73 superseded /
898 unresolved; this shared foundation earns no new original dispositions.

Final qualification: full SQL Debug passed 21/21 build steps with 757 main-owner
and 226 linked-owner passes, three existing skips and no failures/leaks. All
nine focused text/digest tests passed in ReleaseFast. The unchanged original
MD5 public HTTP regression passed (80/80 build steps). PostgreSQL fixture
verification, formatting, generated control-catalog freshness, inventory
integrity and the 2,041-file Apache boundary passed. Earlier test-only failures
that confused result-size admission with heap allocation are excluded; the
corrected witness preserves result-size admission and uses a zero-capacity
allocator to prove borrowing. No remote CI-green or durable activation claim.

### Durable text expression integration

REPLACE and text MD5 now lower into the same immutable VM for stored generated
columns, defaults, CHECKs and ordered/unique expression keys. Public operation
enums come from OpenAPI, with generated Go/Python/TypeScript models and explicit
SDK arity/round-trip checks. Both operations remain immutable and strict in
SQL NULL. Selected arguments execute before NULL propagation, retaining errors;
CASE/COALESCE keep their existing lazy demand semantics.

Literal REPLACE needles compile their failure links once in the immutable
plan arena. Their retained capacity counts toward the schema-wide compiled
literal allowance. Dynamic needles use bounded row scratch and the same
sticky work/cancellation owner; no independent function budget is created.
MD5 admits its fixed-size output before hashing. The normal one-MiB result and
four-MiB row/expression-set allocation bounds remain in force.

ASCH/catalog capability 27 fences text programs independently of physical
column types, including nested Boolean CHECK results. Derivation covers full
runtime schemas and reduced CHECK layouts, defaults/generation, ordinary
expression indexes and unique expression keys. Strict binary validation reads
the capability before allocating, rejects malformed/truncated framing and
refuses capability-26 downgrade. Full-text projection fingerprints remain
independent of this capability.

Independent PostgreSQL 18 fixtures now contain 42 generated-expression
contracts (27 admitted, 15 rejected) and 24 index contracts (18 admitted,
six rejected). Typed native index tests compare their encoded keys against
the PostgreSQL row results. Mounted HTTP qualification writes and reopens all
27 admitted generated shapes, checking base fields, types, values and NULLs.
The native LSM regression additionally exercises text expression indexes and
CHECKs across reopen/portable restore, failed-batch atomicity, subsequent
updates and index-only covered reads.

This does not enable the guarded distributed native-index production worker
or certify its unfinished authority/publication lifecycle. General durable
scalar coverage (including JSON extraction and exact rounding), broader partial
predicates, and the remaining temporal/distributed contracts are still open.
No original source SQL or dispositions change: 898 originals remain unresolved.

Pre-merge qualification: full SQL Debug passed 757 main-owner and 226 linked
tests, with three existing skips and no failures/leaks. Focused ReleaseFast
passed ten tests; mounted HTTP qualification passed 19 linked tests and three
HTTP tests. The corrected LSM reopen/portable-restore regression passed in its
native owner using explicit typed pages and a nullable source field so
the CHECK, rather than structural row validation, owns rejection. Independent
PostgreSQL fixture checks reproduced all 42 generated and 24 index contracts.
Go relational SDK tests, 18 Python tests, TypeScript type checking and 483 tests
(one existing skip), formatting, OpenAPI validation and the 20 inventory tests
passed. This is not a post-main-merge, full native-lifecycle or remote CI claim.

Original-case mapping: `sql-0017`, `sql-0717` and `sql-0718` now have the native
durable text-key foundation, but still need exact public index lifecycle and
distributed UNIQUE/authority evidence before disposition changes. `sql-0671`
additionally needs durable exact rounding and JSON extraction in its unchanged
multi-column generated declaration. The document joined-mutation cases
`sql-0960` through `sql-0966` need their document producer/index-proof contracts;
relational VM support alone cannot certify them.

Post-merge qualification at `054e9e895a`: full native lifecycle passed 210
tests plus one linked test (50/50 build steps); full SQL Debug passed 757
main-owner and 226 linked tests (21/21 steps), with three existing skips and no
failures/leaks. ReleaseFast passed all 14 selected text/planning tests. Mounted
HTTP qualification passed 19 linked and three HTTP tests (80/80 steps).
PostgreSQL independently reproduced all 42 generated and 24 index contracts.
Go relational SDK tests, seven Python expression tests, TypeScript type checking
and 483 tests (one existing skip), formatting, inventory integrity and the
2,050-file Apache boundary passed. Source-catalog freshness passed through the
identical Python audit directly; its `uv` wrapper never launched Python and was
stopped without touching other owners. A mistyped HTTP build-step invocation
was replaced by the successful `public-api-parity-test` run, not counted as
qualification. Cache GC was previewed only to avoid disrupting other active
worktrees. No remote CI or complete original-case parity claim.

### Shared typed numeric function execution

Query execution now delegates ABS, CEIL, FLOOR, ROUND, TRUNC, SIGN and SQRT to
one transport-neutral numeric-function boundary. Exact NUMERIC never converts
through float: quantization retains PostgreSQL display scale and ties-away
rounding; float rounding retains ties-to-even and signed zero. ABS and SIGN
can borrow pinned limbs without allocating a numeric payload. Result encoding,
retained-memory admission, overload resolution and SQL NULL belong to callers;
arithmetic work and cancellation use their existing invocation context.

Strict numeric calls evaluate selected arguments before NULL propagation. A
NULL first argument no longer hides division-by-zero in a scale or divisor
expression. Context capture follows argument evaluation so nested expressions
cannot obtain another copy of the remaining work budget.

An independent PostgreSQL 18 oracle supplies 210 complete numeric/float function
contracts, four selected-argument error contracts and eleven cast-precedence
witnesses. Float witnesses compare float8send bits, including signed zero and
specials; NUMERIC witnesses compare exact text, large integers and scale limits.
Allocation-fault sweeps cover every function; cancellation checks cover every
observed poll cut and retain sticky failure. Unary sign now binds after postfix
casts: unparenthesized overflowing positive casts reject before negation, while
parenthesized signed integer minima remain valid.

This shared boundary is the prerequisite for durable numeric-function VM
activation, not a claim that it has shipped. Remaining activation requires
typed durable operations, SQL lowering, OpenAPI-generated enums/clients, a
reader/catalog fence and generated/default/CHECK/index/restart qualification.
JSON extraction additionally needs a typed JSON operand boundary that preserves
SQL NULL versus JSON null and shares decoded source ownership across a prepared
row/expression set. String aliases or per-function JSON reparsing are not that
architecture. Original dispositions remain unchanged, with 898 unresolved.

Numeric-function checkpoint qualification: the final-source full SQL Debug
gate passed 763 main-owner and 226 linked tests, with three existing skips and
no failures/leaks (21/21 build steps). Focused ReleaseFast passed six shared
numeric tests. Mounted NUMERIC/expression HTTP qualification passed two tests
(80/80 steps). Nine independently regenerated PostgreSQL oracle tests verified
all 210 function contracts, four selected-argument failures and eleven cast
precedence witnesses. Formatting, source-catalog freshness, inventory integrity
and the 2,051-file Apache boundary passed. No public contract changed, so client
generation was not required. Cache GC remains preview-only while unrelated
worktrees have active builds. This is neither remote CI qualification nor
durable numeric-function activation or complete original-case parity.

### Durable typed numeric-function activation

The durable VM and SQL declaration lowering now consume the shared ABS, CEIL,
FLOOR, ROUND, TRUNC, SIGN and SQRT boundary. Lowering preserves the resolved
result domain and promotes operands before persistence; ROUND/TRUNC keep their
separate int32 scale operand rather than coercing it to NUMERIC. Native typed
compilation rejects unpromoted integer/real overloads and mixed-domain public
programs. Selected arguments evaluate before strict NULL propagation.

ASCH/catalog capability 28 fences these functions independently of physical
column types and older arithmetic capabilities. Derivation covers generated
columns, defaults, CHECKs, relational index keys and UNIQUE keys, including an
isolated UNIQUE owner without its companion physical index. Runtime cloning,
serialization/preflight validation and catalog restore retain the bit; text
projection identity explicitly excludes it. Public operations and client enums
come from OpenAPI regeneration, with SDK transport/admission regressions.

Durable ABS borrows canonical positive, zero and special payloads without
allocating, and flips negative canonical signs without decoding coefficients.
SIGN inspects canonical metadata and encodes its tiny result without building
an input limb vector. Other functions use bounded unpublished numeric scratch,
one invocation work/cancellation identity and canonical result encoding. Scalar
floating results retain signed zero; ordered-key equality normalization must
not erase expression semantics. Negative SQRT retains SQLSTATE 2201F through
the semantic ABI, C classification, replicated apply and HTTP mutation boundary;
it is not an unregistered provider failure or a retryable restore condition.

The durable PostgreSQL matrix exercises all 147 exact NUMERIC contracts plus
42 finite/NULL float contracts, for 189 total. Existing durable floating cells
do not admit nonfinite payloads; query-only special-float coverage is not claimed
as durable coverage. New native tests cover zero-allocation payload borrowing,
malformed overloads/scales, selected argument errors, all allocation failures
and observed cancellation cuts. LSM qualification spans exact generated values,
defaults, CHECK domain failures, index-only reads, atomic failed batches, updates,
restart and portable restore. Typed JSON extraction and original-case public/
distributed index authority evidence remain separate open work; no original
inventory disposition changes are justified by this activation alone.

Durable activation qualification uses Debug for correctness: 764 SQL-owner and
226 linked tests passed (three existing skips, 21/21 build steps), 218 native
lifecycle tests plus one linked test passed (50/50 steps), and four mounted
HTTP/public-contract checks passed (80/80 steps), all without failures or leaks.
Nine live PostgreSQL oracle tests passed. Go SDK checks, eight Python expression
tests, TypeScript type checking and 484 TypeScript tests passed (one skip).
OpenAPI generation, formatting, source-catalog freshness, inventory integrity
and the 2,051-file Apache boundary passed. Initial allocation-sweep instability
was corrected by disabling backing-allocator resize, retaining every allocation
cut; unmatched focused-test selectors were corrected before qualification.
Fresh origin/main was already incorporated. This is local qualification, not
remote CI or complete original-case parity; 898 originals remain unresolved.

### Shared JSON path read boundary

JSON path reads now use one allocation-free structural kernel over an already
decoded, pinned operand. Its optional result represents a missing path, while
a present JSON null remains a tagged JSON value. Object lookup, PostgreSQL
int32 array ordinals, negative offsets and exact borrowed numeric tokens form
the common boundary for query execution and upcoming durable activation. Query
calls still evaluate later selected arguments after SQL NULL or a missing component;
work and sticky cancellation remain attached to the enclosing invocation.
The kernel has no allocator and repeated reads preserve payload addresses.

Durable activation remains open. The next boundary is a real JSON operand in
row_value.Value, excluded from ordered scalar keys, with SQL NULL outside the
JSON payload. Parsed ingress should borrow its request-owned subtree; AROW
should decode only referenced JSON columns into a row-scoped prepared-values
owner, once across all defaults/generated/CHECK/index programs. The owner must
not cache by ordinal in an invocation that spans multiple rows. Set.readValues
already supplies a schema dependency mask and a shared values array; cold
restore should use that same boundary. Tuple/index bindings currently execute
plans separately and also need the shared prepared-row owner. Compilation,
canonical literal identity, JSON result equality, generated restore verification,
public grammar/client regeneration and reader capability fencing must all land
before admitting durable JSON functions or claiming original sql-0671 parity.

Shared-path qualification: full final-source SQL Debug passed 766 main-owner
and 226 linked tests, with three existing skips and no failures/leaks (21/21
steps). Both new kernel tests executed, including 10,000 borrowed reads and
every observed cancellation cut. An isolated live PostgreSQL 18 runtime
independently reproduced all 36 existing JSON path contracts, including result
type OIDs, SQL NULL provenance and selected-argument failures. Formatting,
source-catalog freshness, original inventory integrity and the 2,051-file Apache
boundary passed. There is no public contract change requiring generation and
no durable JSON activation or additional original-case credit in this checkpoint.

### Typed JSON operand and row owner foundation

Logical operands now have a distinct JSON tag whose present JSON null is not
SQL NULL. The tag points at an immutable decoded root; an invariant test keeps
the ordinary Value slot the same size as before JSON support. Ordered scalar
key projection rejects JSON explicitly, independently of expression comparison.
Shared structural comparison preserves exact numeric tokens and ignores object
insertion order without converting through floating point.

JsonRow lazily decodes only referenced authenticated physical JSON cells into a
bounded stable-address owner. A sparse ordinal hash map shares each decoded
tree across programs without a schema-width allocation or linear cache lookup.
Cached values borrow immutable source tokens where possible; escaped strings
and containers belong to the row owner. Source-pointer/extent checks reject an
ordinal reused with a different source before an explicit row reset. Reset
invalidates all borrowed values but preserves work, cancellation and memory
admission. Heap-backed owners charge live capacity; initRegion also charges
freed buffers retained by a parent arena, so reset cannot refund inaccessible
capacity. Syntax, text domain, depth, input bytes and allocation admission run
before publishing a cached root. Admission/cancellation failures stay sticky.

Owned output clones every token/container into the request region under shared
work and actual-capacity admission. Managed JSON arrays are rehomed away from
temporary admission allocators. Allocation-failure sweeps cover decode/reset and
clone ownership; cancellation sweeps cover every observed decode/clone poll.
Repeated-read evidence requires one decode and no additional memory for 10,000
reads, with a sparse maximum-u32 ordinal rather than a schema-width vector.

This is not public durable JSON activation. JSON column/literal compilation
and physical JSON rewrite output remain guarded. Activation still requires the
new capability/public operations, SQL lowering and immutable literal identity,
the shared owner across defaults/generated/CHECK/index bindings, and restart/
restore qualification against PostgreSQL. Existing mutation json_null_fields,
AROW SQL-null projections and prepareTypedDocumentValueWithPlan already preserve
root JSON-null provenance; generated/default JSON results must join that same
channel instead of inventing an envelope or flattening JSON null into SQL NULL.
The mapper currently filters submitted generated-field null metadata, so the
derived expression owner must return authoritative replacement provenance.
Original sql-0671 and all 898 unresolved original dispositions remain open.

Pre-merge owner qualification: full SQL Debug passed 771 main-owner and 226
linked tests, with three existing skips and no failures/leaks (21/21 steps).
Full native lifecycle Debug passed 219 tests plus one linked test (50/50 steps),
including exact JSON comparison, output lifetime, memory admission and work
exhaustion during allocator rehoming. An earlier inline-layout run exposed an
existing rewrite sweep's address-dependent allocation count; both the compact
layout's full run and a direct focused sweep passed without changing that test.
Formatting, source-catalog freshness, inventory integrity and the Apache
boundary passed. No new public contract requires generation in this checkpoint.

Merge 7a724db894 incorporates origin/main 553e58317f and 54 upstream commits.
The mapped-batch conflict retains main's complete type identity and delegates
to coerceDatum, which already validates this branch's temporal extension.
The relation adapter retains statement/transaction clocks and main's aggregate
partial cursor together. Two Python E2E files arriving from main required only
formatter cleanup. Post-merge Debug qualification is separate from the
pre-merge results above.

Post-merge qualification: the unchanged merged Zig sources passed full SQL
Debug with 792 main-owner and 226 linked tests, three existing skips and no
failures/leaks (21/21 steps). Full native lifecycle Debug passed 219 tests plus
one linked test (50/50 steps), without failures/leaks. Source-catalog freshness,
inventory integrity and the 2,053-file Apache boundary passed. make fmt and the
subsequent complete fmt-check passed; the two incoming Python formatter diffs
were independently verified to preserve their ASTs. Cache GC was previewed only
while unrelated worktrees had active builds; nothing was deleted. These are
local correctness results, not remote CI or public durable JSON activation.

Repository-header CI repair: remote run 38324716098 exposed a separate gate
that the product-boundary audit does not cover. The repository-wide header
checker reproduced 16 missing/noncanonical notices. Normalization preserves
runtime bodies and the Unicode third-party notice; the Unicode table generator
now emits the canonical first-party header itself. A regression test checks
that emission contract without depending on the host Unicode version. All six
license workflow commands passed locally, including 35 boundary, 12 header,
three provenance and six asset tests. Python formatting and diff checks passed.
This is CI hygiene, not SQL-case activation or new original-case credit.

### Shared composite-expression execution admission

Physical-row and column-batch tuple evaluation now use one execution identity
for all component expressions, source conversion and key encoding. Explicit
appendWithExecution entry points let an enclosing row/program retain work and
cancellation across tuple bindings instead of granting each expression a new
CPU budget. Existing convenience entry points own one context per tuple.
Schema/layout fences, NULL-before-dictionary-addressing, borrowed canonical
coefficients and tuple-length rollback remain intact.

Expression output admission uses the exact encoded size before output
allocation, sharing the encoding pass's already-validated numeric source.
This replaces the physical path's fixed nine-byte NUMERIC reservation and the
batch path's separate estimate without a second coefficient decode. External
numeric conversion charges retained allocator capacity and inherits the same
work, cancellation, input/output byte and coefficient limits.

Regression contracts cover every CPU cut through a mixed direct/expression
tuple in physical and batch form, sticky byte exhaustion, exact NUMERIC
output admission with a failing coefficient allocator, and every observed
external-conversion cancellation poll. These are execution-boundary contracts,
not durable JSON activation. JSON provenance, public capability/lowering and
generated/default/CHECK/index lifecycle qualification remain open, as do all
898 original unresolved dispositions.

Final-source native Debug qualification passed all 222 main-owner tests and
one linked test (50/50 steps), including all three new admission regressions,
with no failures or leaks. Complete formatting, source-catalog freshness,
inventory integrity, repository headers and the 2,053-file product-license
boundary passed. Cache GC was previewed only; no cache data was deleted.

The exact-final-source SQL Debug confirmation also passed: 792 main-owner
tests and 226 linked tests, three existing skips, zero failures/leaks and
21/21 build steps. This rerun followed the last inherited-limit edit; the
earlier SQL run is not substituted for final-source qualification. No public
schema or generated SDK contract changed in this checkpoint.

### Typed root-null provenance and cold verification owners

JsonNulls is a stable-address, row-local owner for authenticated root JSON-null
markers. It owns imported field names, rejects duplicates, and lets typed
generated/default results replace submitted provenance: SQL NULL and nonnull
JSON remove a marker, while present JSON null adds one. Snapshots borrow owned
immutable keys; schema validation must precede import, and consumers must not
let a snapshot outlive its owner. A missing or skipped default does not perform
an assignment. This component does not infer SQL semantics from JSON syntax.

RegionAdmission binds independently retained JSON regions to one sequential
row/program byte counter. Actual peak/spent arena capacity is charged once;
previously admitted capacity can be reused, but another owner cannot mint new
headroom or refund storage retained by a parent arena. Both decoded JSON and
null-provenance owners support this binding and share sticky CPU/cancellation
identity. Arena consolidation during reset may require additional headroom;
the reuse regression explicitly prepays one buffer rather than assuming every
multi-buffer reset is allocation-free.

Cold generated-value verification now keeps one sparse pinned JSON owner for
all bindings, keyed by physical source ordinal. SQL-null cells skip decoding;
only the immutable dependency mask is materialized. All borrowed values retire
after verification. The native regression distinguishes a generated SQL NULL
from stored present JSON null, verifies zero JSON-input admission suffices for
SQL NULL, and covers observed cancellation/allocation cuts on cold JSON cells.

Focused Debug qualification passed all 11 logical row-owner tests and the
native cold generated-JSON regression. An initial SQL-owner filter correctly
failed discovery; the lifecycle regression was moved to its actual native
storage owner and ran with a nonempty audited runtime filter. Full native
Debug qualification passed 223 main-owner tests and one linked test, with
zero failures/leaks (50/50 steps). Full SQL Debug qualification for owner
checkpoint 0dbc48d9bd also passed 797 main-owner and 226 linked tests, with
three existing skips, zero failures/leaks and 21/21 steps. Later preparation
integration changes require their own qualification and are not covered by
these checkpoint results.

Preparation integration remains open: the row owner must span validation,
defaults/generated/CHECK/index execution and physical encoding, returning or
consuming authoritative root-null provenance before retirement. Do not return
a borrowed snapshot from a stack-local owner. Public JSON capability/lowering,
immutable literals and full restart/restore qualification are still guarded;
these owner contracts do not complete original sql-0671 or change any of the
898 original unresolved dispositions.

### Typed preparation publishes null authority to row encoding

The next integration adds a caller-owned typed expression entry point. Pinned
DOM roots own stable headers but borrow the already parsed input payload;
masked-out JSON is not walked or parsed. Roots and root-null provenance must
share the expression invocation's work identity and retained-byte admission.
Mismatched owners fail closed before evaluation.

The public-schema preparation boundary authenticates root-null names against
the pinned compiled property's type and the actual null datum, rejects duplicate
or unknown markers, and lets each published default/generated JSON result
replace the corresponding input semantics. Missing defaults leave supplied
values untouched. Preparation returns individually owned marker names, not a
snapshot borrowed from a retired local owner. The relational mapper retains
that result in the parsed row region through extraction, ordinal encoding and
logical hashing. Representation-only legacy typed helpers reject changed
metadata instead of silently discarding it.

Typed restore evaluates generated-value verification with those authenticated
markers. It never fills defaults or repairs the stored generated result. A
present JSON null cannot impersonate a generated SQL NULL even when their API
JSON bytes are identical. Document-table SQL normalization uses the same
preparation result and translates SQL NULL to absence while preserving explicit
JSON null; it does not infer output semantics from the JSON DOM alone.
Document schemas still reject defaults/generated declarations; this shared
preparation machinery does not bypass that existing activation guard.

Native regressions cover ordinal null bits, preserved unrelated JSON null,
forged restore metadata, marker authentication, all observed preparation
allocation failures, and document normalization/commit/typed projection.
Full Debug qualification for this integration is recorded separately from the
previous owner checkpoint. Public durable JSON functions/lowering, immutable
literal identity, CHECK/index input reuse and complete restart/distributed
activation remain unfinished. No original case receives new inventory credit;
the original accounting remains 479 implemented / 136 rejected / 73 superseded
/ 898 unresolved.

Relational session normalization also derives its outgoing marker list from
actual present typed cells, rather than merely filtering submitted names. This
single cell-iterator pass handles newly derived or retired JSON-null datums
without reparsing payloads or scanning absent columns in sparse rows.

Qualification with origin/main 553e58317f included: full reviewed SQL Debug
passed 798 main-owner and 226 linked tests (three existing skips, zero failures
or leaks, 21/21 steps). Final native Debug including the staging adapter passed
227 main-owner and one linked test (zero failures or leaks, 50/50 steps).
Formatting, source-catalog freshness, license headers, the 2,053-file Apache
boundary and unchanged original inventory integrity passed. Cache GC was
previewed only; nothing was deleted. No OpenAPI contract changed, so this
checkpoint does not require SDK regeneration or claim new public activation.

### Preserve JSONB display independently of logical row identity

Physical JSON cells now retain PostgreSQL numeric display scale. An isolated
PostgreSQL 18 oracle confirmed that 1.20 prints as 1.20, 1.200e2 as 120.0,
and -0.00 as 0.00, while 1.20 and 1.2 compare equal. This matters for both
ordinary row reads and future durable jsonb_extract_path_text expressions.
The former semantic JSON canonicalizer remains unchanged: using it for physical
encoding would irreversibly discard that observable information.

Schema format 29 adds requires_jsonb_display, derived from relational public
schemas with JSON-encoded properties. Catalog admission fences older readers;
schema decoding checks the wire flag before allocation. Document text-index
projections do not inherit this row-storage capability. Physical JSON retains
the existing lexical key order and canonical string syntax, expands numeric
exponents, and preserves decimal scale. PostgreSQL SQL text output remains a
separate formatter with PostgreSQL's length/byte object-key ordering.

Logical row hashing still uses the original length-framed canonical JSON
identity. From authenticated physical bytes it streams numeric normalization,
first counting semantic bytes and then feeding the hash, without constructing
a DOM or payload-sized canonical copy. Per-number scratch is reused. A native
regression hashes a 1 MiB text payload with less than 8 KiB peak scratch and
checks the digest against an independent canonical-byte construction.

Restore establishes canonical physical representation before comparing the
semantic hash. Both field-local validation and materializing validation reject
alternate whitespace/exponent encodings even with a valid physical checksum
and matching logical digest. Actual prepared rows preserve different physical
bytes for 1.20 and 1.2 while retaining the same semantic digest. LSM reopen and
portable restore preserve numeric display in nested JSON values.

With origin/main 543203fbef merged, the full SQL Debug gate passed 1,024 tests
with three existing skips and no failures/leaks. Native Debug before the final
two adversarial regressions passed 232 tests without failures/leaks; the final
focused JSONB Debug gate passed eight tests, including both added regressions.
The first adversarial fixture was rejected by the strict public encoder before
restore; the corrected fixture uses the trusted preparation encoder and
finalizes checksums explicitly. No production validator was relaxed.

This is a storage prerequisite, not public durable-JSON activation. Compiler
guards, immutable JSON literal identity, shared CHECK/index operand ownership,
rewrite evaluation, public capability/lowering, and complete distributed
activation still need implementation and qualification. New expression semantics
must receive a capability distinct from the published display-storage fence.
No original case receives inventory credit from these storage regressions.

The PostgreSQL boundary probe also found and corrected a pre-existing shared
JSONB formatter bug: positive exponents do not give zero a large integer
weight, and leading fractional zeroes do not count toward significant integer
digits. PostgreSQL accepts 0e999999, 0e1073741823, 0.1e131072 and
0.0001e131075; it rejects 0e1073741824, 0e-16384 and 1e131072 with
SQLSTATE 22003. Formatter regressions distinguish those numeric-domain errors
from a caller's output/work ceiling. Negative zero retains display scale but
not its sign. Streaming semantic hashing publishes its continuation only after
both passes succeed; allocation-failure sweeps verify that an existing hash of
preceding row fields is unchanged on failure.

Final reviewed-source qualification passed the complete SQL Debug gate:
799 main-owner plus 226 linked tests, three existing skips, zero failures/leaks,
21/21 steps. Complete native Debug passed 233 main-owner plus one linked test,
zero failures/leaks, 50/50 steps. Formatting, source-catalog freshness, license
headers, the 2,053-file Apache boundary and original inventory integrity passed.
The original inventory remains 479 implemented / 136 rejected / 73 superseded /
898 unresolved; public durable-JSON activation and original sql-0671 remain open.
No OpenAPI contract changed in this storage checkpoint.

### Shared prepared input across CHECKs and tuple expressions

CHECK sets and cold composite-index tuple evaluation now create one stable
PreparedInput for their immutable source row. Selected ingress NUMERIC/array
operands and decoded blob results are prepared lazily and reused across plans.
The cache is sparse and validates declared kind/SQL element identity on reuse.
Ordinary scalars remain borrowed, as do already encoded cold NUMERIC and array
cells; adding cache entries for those views would add allocation without saving
preparation. Blob comparisons retain their streaming base64 path.

The owner shares the invocation's sticky work/cancellation identity and retained
byte counter. Cache admission is refreshed after operand preparation consumes
that same counter, before allocating an entry. A different Execution or byte
counter cannot reuse the owner, even on paths that bypass the cache. All plans,
source payloads and execution state must remain pinned until the owner retires;
cached results cannot escape that lifetime. Advancing to another row requires
another owner, not an ordinal-only cache carried across rows.

The same boundary can read typed JSON provenance from authenticated ingress or
decode a selected physical JSON cell once by its actual source ordinal.
Regressions exercise shared numeric preparation across separately compiled
plans, JSON null versus SQL NULL, exact numeric tokens, historical name lookup,
one cold JSON decode, type fences, sticky budget/cancellation, and all observed
allocation failures. The cancellation fixture explicitly arms the existing
bounded polling interval rather than assuming callback installation resets it.

These are internal evaluator interfaces; no OpenAPI grammar or capability was
activated. Public JSON column/literal/function compilation remains guarded.
Generated/default preparation already has its own shared typed-value boundary;
complete activation still requires joining that boundary to all CHECK/index,
vector-batch and rewrite consumers, safe legacy-result ownership, immutable
literal identity, SQL lowering, public/client generation and reader fencing.
No original case receives parity credit solely from operand-cache regressions.

Cache lookup/insertion hashes are charged to the shared work counter. A
deterministic 64-hit regression checks stable result identity, unchanged byte
admission, one total preparation and only node/key work on reuse. This proves
the preparation/allocation bound; it is not a claim about end-to-end latency.
Cold JSON selection also fences precise SQL element identity before decoding.

Final-source Debug qualification passed 799 SQL main-owner plus 226 linked
tests (three existing skips, zero failures/leaks, 21/21 steps), and 236 native
main-owner plus one linked test (zero failures/leaks, 50/50 steps). The final
three focused ownership regressions passed with allocation sweeps. Formatting,
source-catalog freshness, license headers, the 2,053-file Apache boundary and
unchanged original inventory integrity passed. Original accounting remains
479 implemented / 136 rejected / 73 superseded / 898 unresolved. Public durable
JSON activation is still unfinished; this checkpoint does not claim otherwise.

### Typed operands and results through native row rewrite

Native schema rewrite now reads its already resolved source ordinals through
the shared PreparedInput rather than maintaining a second scalar decoder.
Selected blob operands are decoded from AROW base64 exactly once; ordinary
scalars and encoded NUMERIC/array operands remain borrowed. Unselected JSON
payloads retain their canonical bytes without decoding. Input preparation,
generated/default evaluation and result encoding share one sticky execution
work/cancellation identity and byte allowance. Resolved reads charge work even
when the selected scalar needs no allocation.

This fixes two existing rewrite errors: physical base64 was previously treated
as logical blob bytes, and generated blob results were written as raw bytes.
Generated JSON SQL NULL was also rejected by the scalar-only output adapter.
Typed result encoding now preserves the JSON null bit independently of present
JSON null, uses the schema-selected JSONB display policy, and base64-encodes
logical blob results before physical row encoding. Retained JSON serializer
capacity competes with the same allowance as other expression results.

Focused Debug regressions cover generated blob coalescing and comparison for
binary and SQL NULL inputs, generated JSON SQL NULL, unchanged JSONB display
scale, selected-input counts, direct ordinal cache reuse, typed result fences,
sticky byte-admission failure and all observed allocation failures. These are
rewrite/kernel proofs, not public durable-JSON activation or original sql-0671
completion. Compiler/literal identity, capability and public lowering/client
activation, authenticated CHECK provenance and vector-batch adapters remain
open. Original inventory accounting is unchanged.

Reviewed-source qualification passed all 1,025 SQL Debug tests (three existing
skips, zero failures/leaks, 21/21 steps) and all 239 native Debug tests (zero
failures/leaks, 50/50 steps). The five focused ownership/rewrite tests passed
with allocation sweeps. Formatting, source-catalog freshness, Apache boundary
and license checks, and original inventory integrity passed. Cache GC was
previewed while other builds were active; nothing was deleted. Origin/main
e8666a5b75 was already contained in the branch. No public API changed.

### Authenticated JSON provenance through CHECK and activation pages

Typed document preparation now retains its schema-authenticated JsonNulls owner
through CHECK validation, including preserve/restore preparation. Untyped JSON
callers retain their existing SQL NULL interpretation. Normal and activation
CHECK interfaces share the same PreparedInput path and verify the execution
context and byte-counter identity before using supplied provenance. A present
JSON null cannot become SQL NULL merely because validation crosses another
internal interface.

Distributed native activation responses previously discarded the marker set
already produced by their selected cold projection. Producer and coordinator
now use one shared activation Row contract, including nonempty JSON-null
markers alongside the exact primary version/content observation. Empty marker
sets add no wire field. The coordinator authenticates each marker against its
pinned relational layout and projected root datum, rejects duplicates, and
does not run full-root required-field validation on a narrow CHECK projection.
Source predicates continue to guard the original complete primary row rather
than a hash of its projection.

Projected JSON parsing, provenance ownership and every CHECK plan share one
bounded page execution allowance. Retained parent-arena capacity is not refunded
between rows. Request cancellation/deadline checks propagate into sampled VM
work. An expression quota failure shrinks the worker page where possible; a
singleton returns the resource error with no failed-row publication or cursor
advance. Resource exhaustion is not evidence of a violated constraint.

Focused Debug proofs include ingress/cold null agreement using an internal VM
fixture, owner/counter mismatch rejection, sticky budget/cancellation and
allocation-fault cleanup. Real LSM projections round-trip through the shared
wire contract without losing JSON null or adding empty metadata. Coordinator
projection tests cover malformed provenance, exact source guards, genuine CHECK
diagnostics, all observed allocation failures, and bounded admission across a
page whose individual rows fit. Real worker fault tests cover shrinking/retry
and byte-for-byte unchanged singleton checkpoints with no commit. Their catalog
fixture was corrected to publish production CHECK fingerprints rather than
omitting the authority that native loading requires.

This does not activate public JSON column/literal compilation. The internal VM
fixture is deliberately not evidence for public grammar or original sql-0671.
Capability fencing, immutable JSON literal identity, SQL/public/client lowering,
legacy-result ownership, typed projected tuple/vector consumers and complete
end-to-end activation remain open. Original case dispositions are unchanged.

Full transaction qualification exposed an existing range-guard/savepoint fault
sweep reporting NondeterministicMemoryUsage. The unmodified test also failed in
isolation. Its arena resize/remap fallback now uses the same deterministic
backing allocator as the newer fault suites: no in-place resize success can
change allocation counts between the baseline and injected failures. Every
allocation-failure check and semantic assertion remains enabled; the isolated
Debug sweep passes with this change.

Final reviewed-source Debug qualification passed 1,025 SQL owner/linked tests
(three existing skips, zero failures/leaks, 21/21 steps), 241 native tests
(zero failures/leaks, 50/50 steps), and all 113 transaction contract tests
(zero failures/leaks, 50/50 steps). Focused projection/VM, coordinator and real
worker fault regressions passed. Formatting, source-catalog freshness, Apache
boundary/license headers and original inventory integrity passed. The initial
sandboxed formatter invocation could not reach the pnpm registry; the permitted
network-enabled final check passed. Cache GC was previewed without deletion.
Original accounting remains 479 implemented / 136 rejected / 73 superseded /
898 unresolved. No public OpenAPI grammar was activated by this checkpoint.

### Main integration after CHECK provenance qualification

Origin/main advanced to f986d952d7 during qualification, bringing the design-doc
restructure and native inference changes. The merge preserves the branch's
newer original-case evidence in the relocated SQL campaign history and updates
this ledger's inventory/regex links. Generated Go SDK and antfarm conflicts
were resolved by make generate from the merged specifications, not by selecting
one side's generated output. No handwritten SQL/storage runtime change was
required. Six relocated history documents lost only surplus EOF blank lines.

Make generate and make fmt passed. Merged-tree Debug qualification passed all
1,025 SQL tests (three existing skips, zero failures/leaks, 21/21 steps), 241
native tests (zero failures/leaks, 50/50 steps) and 113 transaction contracts
(zero failures/leaks, 50/50 steps). Go SDK OpenAPI tests and TypeScript SDK
type-checking passed. Formatting, original inventory, the 2,046-file Apache
boundary/license headers and staged whitespace checks passed. The source
catalog checker passed with the existing worktree Python runtime; a redundant
uv wrapper that had not launched Python was stopped without touching shared
cache locks. Original accounting remains unchanged. Public durable-JSON
activation and the full original SQL parity objective are still unfinished.

### Native JSON literal identity and retained evaluator results

Native immutable JSON literal compilation now requires the distinct JSON
expression capability 30. It does not borrow the schema parser's DOM: the
compiled plan owns its header, keys, strings and nested containers. Immutable
array metadata uses a non-allocating sentinel rather than retaining an allocator
pointing at the compiler's movable stack arena. Plan retirement frees its whole
arena; individual literal containers are never mutated or freed independently.

Literal fingerprints use canonical JSONB display bytes, not semantic row hash
bytes. Equivalent key orders and numeric encodings such as 1.20 and 120e-2
have one identity. Changing display scale to 1.2 changes the program fingerprint
even though typed logical equality remains true. Present JSON null has an
explicit native literal marker and differs from typed SQL NULL. Invalid marker
shapes, NUL text, numeric overflow and older reader capabilities fail closed.
Literal preparation shares the compiler's work and retained-byte allowance.

Legacy JSON/named-row/bound-row evaluator entry points now retain JSON results
in the caller's row region before their temporary PreparedInput retires. When
decoding could have produced a borrowed scalar, the result is copied at that
lifetime boundary too. Explicit shared-owner callers retain the borrowing fast
path. A compile-time source-shape mask avoids owner construction and node scans
for ordinary scalar programs and already encoded cold NUMERIC/array operands;
ingress values requiring preparation use the sparse cache instead.

Capability 30 has strict ASCH and catalog fencing independently of capability
29's JSONB display storage. Invalid boolean and truncated metadata are rejected
before allocation; text-only schema projections clear both flags. The JSONB
LSM reopen/portable restore test now requires the current catalog format and
explicitly verifies that physical display storage alone does not activate JSON
expressions. Its scale and text-output assertions remain unchanged.

Eight focused Debug tests cover lifetime retirement, actual allocator reuse,
zero-allocation scalar/explicit-owner paths, display-sensitive identity, native
reader fencing and allocation-fault cleanup. Public schema admission is still
guarded: derived public schemas do not yet request capability 30, and no OpenAPI
or SQL lowering surface has been activated. These tests do not establish
original sql-0671 or change any original case disposition.

Remaining activation includes public feature derivation and generated grammar,
JSON column/path lowering and typed projected tuple/vector consumers, compiled
schema/rewrite definition identity, and generated-result display verification.
An additional existing gap was confirmed during this audit: JSONB array storage
still uses semantic JSON canonicalization, and its logical frame hash currently
depends on stored JSON element bytes. Preserving JSONB array display requires
separating that physical representation from semantic array hashing as well;
it must not be hidden by the scalar JSON literal qualification above.

Final production-source Debug qualification passed all 1,025 SQL tests (three
existing skips, zero failures/leaks, 21/21 steps), all 247 final native tests
(zero failures/leaks, 50/50 steps), and all 113 transaction contracts (zero
failures/leaks, 50/50 steps). Nested literal/result array allocator lifetime is
covered by the final native run. The initial native restart regression failed
only because it hard-coded catalog version 29; its current-format and distinct
capability assertions now pass without relaxing stored scale or restore checks.
Formatting, source-catalog freshness, the 2,046-file Apache boundary/license
headers and original inventory integrity passed. Original accounting remains
479 implemented / 136 rejected / 73 superseded / 898 unresolved. PR #1061 was
verified open and draft; this checkpoint is not public JSON activation or full
SQL parity completion.

### JSONB array physical display and logical identity (2026-10-13)

JSONB array elements now preserve PostgreSQL decimal display scale in the flat
physical array codec. Logical hashing instead frames normalized element lengths
and bytes, preserving the existing array hash domain and distinguishing JSON
null from SQL NULL. The low-level array frame accepts an explicit JSON logical
hash provider; primitive array hashes retain their allocation-free path.
Continuation publication is failure-atomic across every element.

ASCH capability 31 and the transactional catalog fence the new array policy,
including native-only JSONB array columns. Public runtime/CHECK layouts derive
a separate JSONB-array capability from columns and nested declaration programs.
This does not enable capability 30's still-guarded public JSON literal grammar.
Old-format JSONB array layouts are rejected before allocation; no alternate
legacy array decoder was added. Text-index projection identity excludes the
irrelevant capability, with strict Boolean and truncation validation retained.

Strict array restore compares canonical display output directly with the
original bytes, without allocating a second complete JSON payload. A 1 MiB
string verifies under an allocator that cannot allocate; object ordering still
requires bounded key scratch. Formatting shares the enclosing work/cancellation
identity, charges key-ordering work, and checks output limits before allocating
the output payload. Genuine OOM, sticky quota exhaustion and cancellation remain
distinct. Native JSON-literal fingerprint preparation uses the same bounded
formatter. Forged AROW checksums do not authorize noncanonical exponent bytes.

Independent PostgreSQL 18.6 evidence verifies five complete JSONB array binary,
text and element-display references, including decimal scale, exponent expansion,
zero scale/sign, object display order, multidimensional and negative-bound arrays,
and JSON-null versus SQL-NULL elements. Native persistence decodes these binary
references, checks dimensions/provenance and exact element displays after storage,
and verifies canonical re-encoding. These are persistence contracts: the captured
whole-array PostgreSQL text is not claimed as native wire-output qualification.
The native LSM test also proves a scale-only update from 1.2 to 1.20 survives
reopen and portable restore despite equal semantic hashes.

Focused Debug codec/streaming qualification passed 25 tests, and the additional
logical-frame regression passed independently after correcting its expected
existing canonical representation. The first full native run exposed the old
restore fixture treating 1.0 as noncanonical; it now forges 1e0 and retains both
serialization and recomputed-checksum rejection assertions. The exhaustive
rewrite-cache allocation sweep now disables address-dependent backing resizes,
without dropping fault indices or weakening ownership assertions.
Final native/API Debug qualification passed 249 native and 113 transaction tests,
zero failures/leaks, with 60/60 build steps. Final full SQL Debug qualification
passed 1,027 tests with three existing skips, zero failures/leaks and 21/21 steps.
Its initial compatibility-test expectation incorrectly used an exponent where
the existing semantic normalizer emits plain 1.2; the corrected reference passed
both its isolated regression and the fresh full run. PostgreSQL reference tests, formatting,
control-catalog freshness, the 2,046-file Apache boundary/license headers and
original inventory integrity passed. Cache GC was previewed only; nothing was
deleted while other worktrees were building.

Original accounting remains 479 implemented / 136 rejected / 73 superseded /
898 unresolved. PR #1061 remains open and draft. No original case receives credit
from this shared codec work. Remaining work includes PostgreSQL-compatible array
text output and shared wire admission, public JSON-program grammar/lowering and
schema-definition identity, typed projected tuple/vector consumption, and the
remaining exact original-case activation and distributed fault proofs.

### Shared PostgreSQL array wire preparation (2026-10-13)

Text and binary array output now share a request-owned preparation boundary.
JSONB preparation retains a compact 12-byte size/quoting record per element and
one exactly sized maximum-path object-key sorting buffer. The parsed immutable
DOM stays borrowed; no encoded JSON payload, per-element arena or whole-array
output copy is retained. Fixed-buffer sorting scratch is reused during
preflight and emission. Primitive arrays and all-SQL-NULL JSONB preparation
allocate nothing. Callers pass an explicit allocator rather than using a global
allocator or hiding allocation failures behind an optional default.

JSONB output uses its PostgreSQL key order, separators, numeric scale and
exponent expansion. Array text uses PostgreSQL's conditional quoting, including
the distinction between the JSON null datum and SQL NULL, and canonical UUID
spelling. All eleven binary codec goldens now require exact PostgreSQL bytes;
the alternate native JSONB binary golden and the envelope's JSONB-only semantic
comparison exception were removed. All 19 existing text-array inputs gained
independently captured PostgreSQL output text without changing their previously
verified binary values or 19 rejection contracts. Five JSONB persistence goldens
now also verify exact text and binary emission after physical storage.

Admission covers shape/domain validation, key-scratch sizing, formatting,
complete wire size and emission work before touching the destination. A local
prepaid emission context forwards cancellation through the original owner and
parent chain without spending the admitted work twice or resetting its quota.
The numeric context has an explicit polling-only operation for this already
admitted work. Buffered output amortizes small writes, while copying and metadata
scans poll in bounded chunks. Binary input also accepts the shared owner,
preserves cancellation through JSON parsing, checks it before allocating an
owned decode region, and distinguishes genuine OOM from sticky quota exhaustion.
This codec-level ownership contract does not claim that every protocol/session
caller has acquired a statement-wide cancellation owner.

The wire audit also found a hot/cold numeric identity mismatch: rendering an
existing native JSONB float used its short float spelling, while storage and
logical comparison retain the exact value. The shared JSONB writer now uses the
bounded native exact-decimal expansion. SQL float-to-JSON conversion still chooses
its PostgreSQL spelling at the existing sqlJson boundary. Hot/cold text/binary
roundtrips preserve logical identity for ordinary floats, negative zero, large
integral floats, the minimum subnormal and maximum finite binary64.

Focused Debug qualification passed 35 contracts, including every allocation
failure, every observed cancellation phase, exact parent quota accounting,
initial zero-cost cancellation and a one-unit-short admission failure before
publication. A 1 MiB string uses only 12 bytes of preparation memory and no
sorting scratch; a nested-object test proves exact 40-byte path scratch rather
than retaining all objects' sort vectors. Primitive output runs with an allocator
that cannot allocate. For 1,024/4,096 Boolean cells, measured work was
7,170/28,674 units for text and 13,352/53,288 for binary. One local Debug sample
measured 170/688 microseconds for text and 148/578 microseconds for binary;
these are operator scaling diagnostics, not production latency or an A/B win.
Timing uses std.Io.

Final production Debug qualification passed 1,034 SQL tests (three existing
skips), 107 PostgreSQL protocol tests, 249 native relational tests and 113
transaction tests, with no failures/leaks. Both mounted original stored-array
read and joined source-RETURNING contracts passed. Their first build exposed one
stale Datum.patterns field access in an imported server file; the accessor-only
compatibility correction restored the mounted gate without adding lake features.
Six independent PostgreSQL 18.6 oracle tests passed. Formatting, control-catalog
freshness, the 2,047-file Apache boundary/license headers and original inventory
integrity passed. Cache GC was previewed only; nothing was deleted.

Freshly fetched origin/main was already merged. Original accounting remains
479 implemented / 136 rejected / 73 superseded / 898 unresolved; existing original
contracts were revalidated, not credited again. Public JSON-program
grammar/lowering and schema-definition identity, typed projected tuple/vector
consumers, unified JSONB numeric-range admission for non-rendered parameters,
broader statement-wide owner wiring, and the remaining exact original cases and
distributed activation proofs still require implementation.

### Unified PostgreSQL JSONB input-domain admission (2026-10-13)

JSONB input now uses one borrowed numeric-domain layout for admission and
display. Admission checks PostgreSQL's integer-digit, scale and exponent limits
without expanding the physical output or allocating. A valid 1e131071 parameter
can therefore be queried with jsonb_typeof or array cardinality under a 4 KiB
input allowance; 1e131072 is rejected even when never rendered. Display output
is admitted separately. Generic document JSON number hashing remains unrestricted.

Owned and pinned parsing share one cancellation-aware tree assembler. A fixed
nine-byte lexical stack rejects excessive nesting and invalid discarded
duplicate values before any DOM allocation. Duplicate object keys use
PostgreSQL's last-key-wins rule, including escaped-equivalent keys. JSONB zero
characters, invalid UTF-8, numeric range and binary protocol/version failures
retain their distinct PostgreSQL diagnostics rather than being flattened into
a type mismatch. Genuine allocation failure maps to 53200; quota exhaustion
remains 54000. Fallback protocol messages do not expose internal error names.

Independent PostgreSQL 18.6 evidence covers 22 input-domain cases across text,
scalar binary and array binary admission, full display fingerprints, and five
malformed scalar binary payloads. Debug contracts also cover input-owner
retirement, every allocation failure and observed cancellation cut, strict
physical restore, and non-rendered large parameters. Old assertions were
updated only where independently verified PostgreSQL behavior changed; restore
still rejects noncanonical duplicate-key bytes.

Origin/main 0bbd1807c1 was merged without conflicts after checkpointing this
work. Full merged-tree Debug qualification passed 1,040 SQL tests (three existing
skips), 108 protocol tests, 249 native relational tests and 113 transaction
tests. The sandbox-denied socket tests passed when rerun with local socket
access; no production workaround or skipped test was added.

### Cumulative Bind admission and stable portal ownership

One pgwire Bind now owns the work/cancellation context shared by all its
parameters, nested JSONB/NUMERIC/array codecs and conversion to backend
envelopes. A new parameter cannot renew the quota. The listener's existing
statement deadline/cancel signal covers decoding as well as backend execution;
the existing connection allocator remains the retained-memory bound. The
context is invocation-local and never retained by a published portal.

The portal's arena is initialized at its final heap address before decoding.
Managed JSON arrays therefore retain the correct allocator through Execute,
not a pointer to a moved stack arena. All fallible preparation, including
registry capacity, completes before an existing unnamed portal is retired.
Publication itself does not allocate.

Protocol qualification checks parent accounting, sticky quota/cancellation,
genuine OOM, text/binary JSONB arrays, large strings, exact-quota success,
one-unit-short failure before BindComplete, retained allocator identity after
Parse/Bind/Close/Execute, and every allocation-failure cut through admission and
cleanup. A fixed-width decoder scaling contract uses a failing allocator and
exact cumulative work for 1, 16 and 256 parameters; it does not claim a
production latency improvement.

Final Debug protocol qualification passed all 111 tests, with no failures or
leaks. One local scaling sample took 74/692/10,968 microseconds for 1,000
iterations at 1/16/256 parameters, with 8/128/2,048 admitted units per iteration
and zero decoder allocations. These measurements cover the decoder, not the
whole Bind (which also charges one metadata unit per parameter). Formatting,
the 2,048-file Apache boundary/license headers and inventory integrity passed.

This closes Bind-level owner renewal, not the entire statement-wide ownership
campaign. Generic raw JSON codec cooperation, deep envelope clone polling,
public JSON program/schema identity and lowering, typed projected consumers,
distributed activation proofs and the remaining exact original cases still
need implementation. Original accounting remains 479 implemented / 136 rejected /
73 superseded / 898 unresolved; these component contracts grant no original
case credit.

### Cooperative retained array envelopes and clone credit

Array-envelope counting now uses the same bounded, buffered writer as SQL wire
emission. It polls and charges in 256-byte chunks and stops at the wire/work
bound while counting, rather than performing an uninterruptible serialization
before checking the limit. Actual encoding prepays its complete emission cost
before touching the caller's destination. Allocated encoding no longer treats
post-allocation cancellation as unreachable.

Retained envelope construction, owned decoding and borrowed decoding share
that original admission. A private inspection charges bounded materialization
credit; the immediate materializer binds a local prepaid context which can poll
the original owner but cannot renew or double-spend its work allowance.
NUMERIC parsing/formatting continues to charge its actual numeric subkernel
work separately. Validation is not an externally reusable trust token.

JSON payload materialization reuses the row owner's structural clone rather
than the generic uncooperative datum clone. Arrays retain the destination
allocator, and strings, exact numeric tokens and object keys are copied in
bounded chunks after admitting their bytes once. No source DOM, allocator or
borrowed text survives retirement through this retained boundary. Ordinary
primitive encoding still needs no heap allocation or per-cell serialization
buffer. No physical format or PostgreSQL output golden changed.

The fault matrix covers JSONB, exact NUMERIC and large integer values across
borrowed output, allocated output, retained API envelopes, owned decoding and
borrowed decoding. It verifies every observed cancellation cut, every allocation
failure, exact parent/local quota accounting, one-unit-short rejection,
unchanged destinations on quota rejection, genuine OOM without poisoning the
owner, and sticky failures followed by malformed input with a failing allocator.
This found and fixed allocation-before-owner-check in the NUMERIC decode owner.
All 13 focused Debug contracts passed, including the existing independent
PostgreSQL text/binary fixture comparisons and source-retirement tests.

Four interleaved local Debug runs of the unchanged streaming benchmark produced
135–139 ms before and 119–122 ms after for 262,144 cells / 3,439,056 bytes, with
zero encoding allocations in both versions. This is cached-binary operator
evidence under concurrent build load, not an end-to-end production latency claim.
Five isolated PostgreSQL 18.6 oracle tests reverified text/binary arrays, JSONB
physical display and input-domain diagnostics without regenerating goldens.

Final-source Debug qualification passed all 1,041 SQL tests (three existing
skips), 111 protocol tests, 249 native relational tests and 113 transaction
tests, with no failures or leaks (85/85 build steps). Both mounted public
stored-array and joined source-RETURNING contracts passed (80/80 steps).
Formatting, the 2,048-file Apache boundary/license checks and inventory
integrity passed. Cache GC was previewed only; nothing was deleted.

The full SQL parity objective is still incomplete. Generic raw JSON codec
cooperation, public JSON program/schema identity and lowering, typed projected
consumers, distributed activation proofs and remaining original cases still
need work. Component ownership/fault proofs do not change the original
479 implemented / 136 rejected / 73 superseded / 898 unresolved dispositions.

### Native durable JSON columns and structural consumers

The capability-gated native VM now compiles JSON columns and jsonb_typeof,
jsonb_array_length, jsonb_extract_path and jsonb_extract_path_text. Operands
retain JSONB identity; present JSON null differs from SQL NULL and missing
paths. Path components use the existing query kernel, including negative array
ordinals and PostgreSQL's invalid-ordinal-as-missing behavior. Selected arguments
all execute after NULL or a missing child, while lazy ancestors retain their
short-circuit semantics. Scalar-input jsonb_array_length errors retain SQLSTATE 22023, and
cardinality has the PostgreSQL int32 result identity.

All programs still require explicit native JSON-expression capability. Public
schema derivation, generated OpenAPI grammar and SQL DDL lowering have not been
enabled. A test-only structural bridge feeds 25 independently verified existing
PostgreSQL path cases into native programs; it does not bypass a public SQL gate
or grant any original-case credit.

Multiple plans share the same sparse pinned-root cache. Direct path chains
consumed by typeof/cardinality do not allocate intermediate JSON headers.
After one cold-root decode, 20,000 text-path/cardinality evaluations succeed with
a failing allocator and no output allocation; one Debug sample took 11.9 ms.
Hot typed DOM reads pin rather than parse. Retained JSON and cold-derived text
copy at owner retirement; derived text now polls during bounded copy chunks
without charging its byte admission twice. Tests poison the retired physical
source and verify that retained values and allocators remain valid.

Native generated text uses the ordinary typed-row expression set, preserving
123.00 display from a JSONB 1.2300e2 source, overwriting submitted generated
values, and strictly rejecting forged restored text. Program and generated-set
identities change with the path but survive schema-epoch and physical-column
reordering. This is native integration evidence, not activation of original
sql-0671 or a public generated-JSON contract.

Five new contracts cover the independent path goldens, precise argument types,
NULL provenance, lazy/strict selection, source retirement, shared-root reuse,
and every observed cancellation/allocation-failure cut for JSON/text results.
The fault fixture initially used a row schema version different from its
declared layout; correcting that fixture preserved the schema-mismatch guard.
The independent PostgreSQL oracle reverified all 138 text/JSON scalar cases
without regenerating fixtures.

Full production Debug qualification passed 1,041 SQL tests (three existing
skips), 111 protocol tests and 113 transaction tests. The final native rerun,
including the last added generated-text integration test, passed all 254 tests
without failures or leaks. The five new contracts also passed directly from the
final Debug artifact. Formatting, control-catalog freshness, the 2,048-file Apache
boundary/license headers and original inventory integrity passed.

Before public activation, complete the generated JSON/null OpenAPI contract,
SQL lowering, typed projected tuple/vector consumers and reader/capability
barriers. Restore verification for generated JSONB/JSONB-array results must also
preserve observable numeric display, not merely SQL logical equality. That native
requirement is covered by the observable generated-result verification checkpoint
above; it does not establish public schema/SDK or distributed activation.
The public JSON-null literal representation must also survive generated SDKs
which omit a nil optional value; native explicit-null field-presence rules are
not by themselves a complete SDK contract.
Public schema/rewrite identity and distributed activation proofs still need
end-to-end qualification. Original dispositions remain unchanged at
479 implemented / 136 rejected / 73 superseded / 898 unresolved.

### Projected JSON dependencies for scalar index keys

Projected tuple construction now admits JSON dependencies through one sparse,
row-local pinned root owner shared by every component expression. Native JSON
vectors, plain text byte vectors and dictionary byte vectors use the schema's
JSON identity; numeric, boolean and dense-vector physical lanes are not inferred
to be JSON. SQL NULL is checked before dictionary addressing, while present JSON
null retains its own root. Only referenced dependencies are parsed. The root
owner retires after the complete tuple has been encoded, before source advance.

The owner shares the caller's sticky work/cancellation context and monotonic
retained-byte admission. Decoder arena capacity competes with expression results
and key encoding rather than receiving a fresh per-component budget. Failures
restore the output prefix, and exhausted/canceled invocations remain failed
even if a later row is malformed. Direct JSON ordered keys remain unsupported;
this is the projected-source implementation for admitted scalar JSON consumers,
not an expansion of the durable ordered-key format.

Debug contracts compare complete composite key bytes with pinned canonical AROW
execution across three vector representations and native/external batch binding.
They include exact numeric display, JSON null versus SQL NULL, NULL dictionary
slots with arbitrary IDs, reordered vector columns and unused malformed JSON.
Allocation-failure injection covers binding, decoding and canonical row/key
preparation. A separate matrix checks every observed cancellation cut, one-unit
short work and retained-memory limits, sticky failure and malformed-input rollback.

Public JSON expression/schema/SDK activation, SQL DDL lowering, distributed
reader barriers and original public-case qualification remain unfinished. This
component change does not alter original dispositions or claim those features
are activated.

The final focused Debug artifact passed both new regressions without leaks, and
two independent PostgreSQL JSONB oracle tests passed without regenerating
fixtures. Formatting, license and inventory checks passed. The merged baseline
also passed the full Debug gate: 1,043 SQL tests (three existing skips), 117
pgwire tests, 257 native relational tests and 113 transaction tests (85/85 build
steps). The full native rerun including this checkpoint is still in progress;
the focused result is not a substitute for that gate. Cache GC was previewed
only, with no deletions.

### Public JSON expression grammar and SQL lowering

OpenAPI now generates the JSON expression type, the four structural JSONB
consumers and the explicit JSON-null literal marker across public SDKs. A true
marker accepts an absent or null optional value; a conflicting nonnull value is
still rejected. Absent-value and explicit-null JSON-null literals compile to
the same immutable program identity and remain distinct from SQL NULL. Go,
Python, TypeScript and Rust round-trip tests cover SDK omission behavior.

SQL DDL lowering now retains typed JSON constants and lowers jsonb_typeof,
jsonb_array_length, jsonb_extract_path and jsonb_extract_path_text to the native
VM. JSON null survives immutable CASE/coalesce folding rather than adopting the
SQL-null placeholder domain. The real lowering path executes all 25 admitted
independent PostgreSQL JSON path fixtures; it no longer needs the native test's
temporary AST bridge to prove that boundary.

Full runtime schemas, reduced CHECK layouts, definition fingerprints and index
admission share one expression-capability derivation. This fixes reduced-schema
admission which previously dropped reader requirements when recompiling a CHECK
or index expression. JSON dependencies are detected across defaults, generated
columns, CHECKs, ordered index expressions and UNIQUE expressions. Plain JSON
columns do not by themselves claim JSON-expression capability. Catalog checks
reject a JSON-expression schema under reader capability 29 and accept it under
30; this is a local durable capability fence, not a distributed readiness proof.

A mounted SQL contract seeds real native storage from public DDL lowering,
closes and reopens the database, and reads generated JSON consumers through the
public SQL handler. Exact 1.20 display, JSON null, SQL NULL and their result flags
survive restart. The public DDL validator also compiles JSON defaults, generated
consumers, CHECKs and scalar expression indexes, rejects forged generated text
on restore, and preserves JSON-null provenance through typed preparation.

Production-source Debug qualification passed all 98 build steps: 1,045 SQL tests
(three existing skips), 117 pgwire tests, 262 native relational tests and 113
transaction tests, plus the mounted catalog/DDL suite. A final native rerun with
the capability-29/30 assertion passed all 262 tests (50/50 steps), without leaks.
The complete Go SDK suite, 303 Python SDK tests, 489 TypeScript SDK tests (one
existing skip), TypeScript compilation and all 18 Rust SDK library tests passed.
TypeScript's structural admission now includes the new operations/type and a
bounded JSON literal walk, sharing its existing aggregate literal allowance
without serializing a duplicate envelope. Tests cover UTF-8/Unicode, NUL,
cycles, depth, exact arity, marker conflicts and one-unit-over admission.
PostgreSQL independently reverified 138 existing text/JSON
contracts without changing fixtures. Generation, formatting, inventory integrity,
Apache boundary/license checks and control-catalog freshness passed.

Distributed activation is still incomplete. Metadata proposal admission must
bind the new schema-language/physical-reader requirements to a versioned decoder
floor and current term/membership proof, and data-owner installation/restore
must acknowledge the corresponding capabilities rather than only a schema
epoch. The audit found that local table-catalog checks do not prove those remote
requirements; the current ordinary CREATE floor must not be treated as evidence.
Complete that rollout fence and its mixed-version/fault tests before declaring
distributed activation ready. Generic raw-JSON codec cooperation, preparation
ownership improvements and exact original-case campaigns also remain open.
Original dispositions stay 479 implemented / 136 rejected / 73 superseded /
898 unresolved. These public component/restart contracts are not a substitute
for the unchanged original sql-0671 multi-column declaration and its full proof.

### Shared immutable schema-reader requirements

Schema serialization and the durable table catalog now use one minimum-reader
calculation covering all expression flags, precise builtin identities, NUMERIC
modifiers and JSONB arrays. Independent tests exercise every feature floor,
one-version-too-old serialization, exact-version round trips and typed-column
requirements. A compile-time test requires extending that contract when another
requires_* field is added. Catalog mismatch still remains distinct from an
unsupported reader capability; this calculation does not authenticate schemas.

ReaderRequirements separates semantic_version from encoding_version. An old
reader may interpret a layout's logical features but cannot necessarily decode
a newer ASCH frame carrying that layout. Restore/transport admission must check
both ceilings. Text-search projection serialization explicitly drops public row
authority alongside other relational features, retaining its existing legacy
document projection bytes rather than leaking row requirements into that format.

Every immutable schema epoch caches its reader requirements at construction.
Pinned SchemaView access is constant time and remains attached to the original
generation across publication. A measured 10,000-access test verifies unchanged
allocation and reference counts, including after a newer JSON-expression epoch
is installed. This supplies the owner-side primitive for capability observations;
it does not yet add a distributed ACK or publication barrier.

The rollout audit against the merged origin/main snapshot 832c9d6659 establishes
three distinct mechanisms which must not be conflated: metadata command decoding
(that snapshot advertises 33, this branch 36), schema semantics/ASCH decoding
(that snapshot supports 25, this branch 31), and store relational
topology protocol 2 (Raft/FK coordination). The last capability is advertised
conditionally on a data Raft runtime and is not a schema-reader capability for
serving-only owners. Bumping/reusing it would impose the wrong contract.

Remaining distributed activation implementation:

1. Add a dedicated, reporter-incarnation-bound schema-reader capability to store
   registration and reports, separate from topology coordination. Bind the
   validated schema requirement to metadata publication and an explicit new
   decoder profile, preserving predecessor command interpretation. Final append
   classification must cover CREATE, replacement, FK/UNIQUE publication, restore
   staging and topology/extension paths, not only public SQL ingress.
2. Capture readiness before the catalog lane, then revalidate the same term,
   metadata incarnation and complete joint/configured membership under the lane.
   Activate a durable monotonic schema-reader floor atomically with publication;
   subsequent registration and placement must not introduce an older reader.
   Do not derive trust solely from an optional user-controlled capability field.
3. Extend owner installation progress with exact schema/public-definition
   identity and capability information from the same pinned root observation as
   the installed schema/index state. Current table/node/version-only progress
   is insufficient. Bind evidence to the reporter incarnation and current
   group/range/root ownership; retirement, resharding or same-version namespace
   replacement must invalidate stale proofs. Delta suppression must compare the
   full proof, and reconciliation must index it once per immutable round.
4. Use versioned progress commands/storage rather than extending legacy fixed
   record bytes under their old opcode. Require all metadata decoders before
   appending the new frame. Missing/legacy capability observations remain unknown
   for new features and cannot authorize schema cutover.
5. Apply the same contract to document and relational restore publication.
   Manifest admission includes actual physical schema versions, even when their
   semantic requirement is lower. Verify the staged owner before publishing its
   namespace; preserve bounded streaming, cancellation and resumable cleanup.
6. Add mixed-version and distributed fault tests for joint membership changes,
   downgrade/registration races, stale same-version ACKs, reporter/root ABA,
   partial group coverage, cancellation and unknown physical frame versions.
   No failed proof may publish a schema, finalize a rewrite or expose staged rows.

This is still required implementation, not a claim of a completed rollout gate.
Original dispositions remain 479 implemented / 136 rejected / 73 superseded /
898 unresolved; schema-reader component tests grant no original-case credit.

### Dedicated schema-reader transport and decoder admission

Store registration and status now carry independent semantic/ASCH reader
ceilings, bound to the process reporter incarnation rather than data-Raft
topology support. Unknown legacy observations remain absent from serialized
JSON and cannot satisfy a schema requirement. Incomplete, unfenced and unknown
future profiles fail admission. Cloning, report comparison, immutable catalog
capability counts and heartbeat caches preserve the descriptor without rescanning
schemas or runtime inventories.

The binary store extension has an explicit version 9. Unlike versions 7/8,
this version permits unsigned or absent physical roots independently of schema
support, while retaining signed-root validation. Predecessor extensions keep
their original interpretation. Sparse reports use envelope version 2; baseline
singles/batches use versions 3/4. A capability-bearing record cannot be hidden
under the predecessor report envelope. Baseline manifest and binary record
capabilities must agree. Legacy records continue using their old versions.

Metadata decoder profile 37 gates these facts before final single/batch append;
HTTP readiness captures the same floor before taking the report lane. Runtime
profile negotiation cannot strip this safety information as optional telemetry.
The decoder profile does not activate a schema or imply a data-owner ACK.
Automatic advertisement is intentionally not enabled yet: publication's durable
monotonic reader floor, placement/registration admission against that floor,
same-root schema identity acknowledgements and restore integration remain
required. The earlier six-step activation plan therefore remains open.

Tests cover separate ceilings, legacy JSON omission, reporter/stale-generation
fences, owned copies, malformed/truncated extensions, downgraded framing and
registration/sparse replacement through durable restart. The resumable baseline
contract runs with both predecessor and reader-capable profiles, checking
invisibility before activation, partial-upload snapshot recovery, atomic reader
descriptor publication and subsequent sparse/snapshot preservation. Mixed-version
proposal tests exercise the production final admission helpers with a version-36
member and term, membership and metadata-incarnation changes after preflight.
These are rollout-transport proofs, not original SQL-case completion credit.

Broader Debug compilation also exposed a pre-existing owned-schema
canonicalization error: constant partial-index predicates have no column.
Canonicalization now preserves those predicates and only normalizes typed
operands when a column exists, matching the raw-schema path. A regression test
preserves both constant truth values and an exact integer operand.

Debug qualification passed 407 transport/catalog/API tests and the expanded
225-test baseline/coordinator/projection gate. Review additionally aligned
storage's mandatory runtime-profile selection with service admission. Its final
210-test durable catalog rerun passed, including profile activation through
restart. The new fixture initializes a real metadata incarnation before
registration; an uninitialized embedding fixture correctly cannot prove
production profile activation.

The combined SQL/pgwire/storage/transaction/DDL gate passed all 98 build steps:
1,045 SQL tests (three existing skips), 117 pgwire tests, 267 native relational
tests plus the native snapshot contract, 113 transaction tests and 27 mounted
DDL tests plus its linked parser test. Formatting, license/product boundary,
control-catalog freshness and original inventory integrity passed. Cache GC
was previewed only; nothing was deleted. No original disposition changed.

### Durable reader-floor substrate and downgrade enforcement

ReaderFloor now joins semantic and physical requirements independently and
monotonically. An inactive zero/zero floor remains distinct from a schema
identity; incomplete, unknown and future requirements are invalid. Metadata
incarnation extension version 4 preserves this pair alongside the prior runtime,
dense-native and relational-topology floors. Decoder profile 38 advertises this
record interpretation and store enforcement, not a complete publication workflow.
Predecessor incarnation frames retain their exact interpretation.

The internal transactional raise primitive verifies compact active store headers
before advancing the floor. It fails closed without metadata initialization,
eligible readers or complete capability coverage. A publication caller must
validate its mutation and decoder/membership proof first, then raise the floor
and install the schema in the same transaction. No standalone activation API or
automatic producer is added. Already covered requirements use a fixed-size
incarnation lookup without scanning stores: a 10,000-call failing-allocation
contract detects any header re-decoding or escaping allocation on that path.

Registration and ordinary report replacement enforce the floor in service
admission and again in replicated apply. Sparse/baseline HTTP admission reports
a reader upgrade requirement before append; baseline root activation checks the
current floor again, so an upload prepared before an advance cannot expose a
weaker reader afterward. The fault contract also repairs such an upload with a
new qualified cursor rather than changing an immutable pending generation.

Reference heartbeats preserve the registered capability instead of treating
liveness as permission to replace implementation support. Their capability-bearing
wire records now participate in the version-37 final decoder classifier; that
arm was missing in the preceding transport checkpoint. A reference producer
must match the committed reader descriptor and capture the corresponding
decoder readiness before its catalog lane.

Metadata snapshot verification reuses its existing borrowed-row index to check
the incarnation floor against active store headers before installation. Weaker
unpublished baseline roots remain resumable/collectable but cannot become active
without the current admission check. Tests reject incompatible active readers,
partial/future/corrupt floor frames and stale store registrations/reports.
Transaction tests cover denied activation, abort of floor plus actual catalog
mutation, committed publication, snapshot installation and reopen. Restored
name lookups use normal derived-catalog bootstrap; the guard is not bypassed.

The substrate is not yet connected to all schema publication producers.
Server-derived schema/manifest identity binding, an explicit publication decoder
profile and term/membership preflight, automatic capability advertisement,
same-root owner acknowledgements and document/relational restore publication
are still required before activation can be called complete. This does not
change original SQL dispositions or qualify the distributed sql-0671 declaration.

Raising a new floor additionally requires durable metadata decoder activation
version 38 for the same metadata incarnation. Snapshot verification rejects a
floor without that activation, or with a predecessor activation. The final
append guard applies an active reader floor to every metadata command, not only
relation-source mutations, and rechecks an advance after off-lock admission.
Tests cover an ordinary non-relation command through mixed version-37/38 members
and activation between preparation and append. Publication integration must
capture fresh term/membership readiness before its lane; ordinary metadata
mutator preflights must also be audited before enabling automatic activation.
The strict ceiling profiles are paired with ASCH/semantic support 31; increasing
native ceilings requires reviewing and advancing metadata transport/floor
decoder admission too, not merely changing a serializer constant.

Qualification passed the combined 105-step Debug SQL/pgwire/native/transaction/
DDL/catalog gate. After the additional global decoder and activation-binding
checks, a clean in-place cache rebuild passed the final 70-step metadata gate,
398 tests. An intervening run failed because its generated Zig object cache
disappeared; no source error was reported, no external cache cleaner was live
at inspection, and this work used no destructive cache operation.

Zig/Go/Python formatting phases, the pinned Biome 2.5.0 native checker (641 files),
Cargo formatting, license/product-boundary checks, control-catalog freshness and
original inventory integrity passed. The full make fmt-check wrapper stalled
in its PNPM launcher before Biome. The installed pinned Biome and Cargo checks
passed directly; after verifying its process group, only that task-owned stalled
wrapper was stopped. The wrapper itself is not recorded as a completed check.
Original dispositions remain 479 implemented / 136 rejected / 73 superseded /
898 unresolved. The floor/transport tests earn no original-case credit.

### Automatic, qualified schema-reader advertisement

The authenticated internal schema-reader-readiness RPC now reuses the existing
durable decoder activation machinery. Both metadata service implementations
establish the activation receipt before returning readiness, then verify the
same incarnation and complete protected membership and revalidate leadership.
The shared result is StoreDecoderReadiness; existing store-root readiness keeps
its wire representation and uses the same verifier. This is an internal
control-plane RPC, not a new public SQL/OpenAPI schema or index route.

Data registration advertises the current semantic/ASCH ceilings only after that
readiness proof. Missing/unsupported legacy routes permit an unknown initial
claim; malformed replies and ambiguous failures do not. Once a reader claim is
enrolled, unavailable readiness cannot erase it. Only an explicit pre-proposal
decoder rejection allows an un-enrolled claim to fall back, without stripping
the physical-root/signing identity. Transient readiness unavailability follows
the existing bounded bootstrap backoff; malformed proof remains a safety error.

Status collection copies the exact admitted descriptor from its linearizable
metadata snapshot for the same nonzero reporter incarnation. It neither infers
support from the leader's version nor requires data Raft. The heartbeat cache,
sparse/full report comparison and reference fences retain that descriptor.
These implementation capabilities remain separate from actual schema/root
installation acknowledgements.

Production Debug compilation exposed missing facade methods that physical
metadata unit tests could not exercise. Reader-floor lookup, the atomic
standalone revision/projection-revision pair, planless index-owner ACK
status/preflight and root-enrollment protocol validation now cross the existing
owned JSON kernel projection protocol. No physical storage handle escapes the
owner. StandaloneRevisionState is a storage-free shared contract. The new
compiled-owner regression verifies real registration/capability projection,
revision-pair semantics and preserved admission errors; advertising alone leaves
the schema floor inactive. The actual serverless archive also needed the pure
resolver fingerprint dependency already present in native object runtimes;
the shared object-runtime import profile now supplies it to both.

The dedicated reader-admission gate audits the metadata and data source owners
with separate nonempty filters. Its first selection incorrectly combined
unrelated owner filters and failed the audit; the filters were corrected rather
than weakening the audit. The data contracts are also selected by the normal
data-runtime gate, and the opaque projection regression by the existing
compiled storage-owner gate.

Qualification for this advertisement increment used Debug throughout. The
actual server and final reader-admission/data-runtime gate passed 128/128 build
steps. The broader metadata/catalog/coordinator and data-runtime gate passed
125/125 steps, and the compiled storage-owner regression plus actual server
passed 67/67 steps. Formatting, license/Apache boundaries, control-catalog
freshness and original-inventory checks passed; original dispositions remain
479 implemented, 136 rejected, 73 superseded and 898 unresolved.

Server-derived publication/manifest requirements, atomic binding to every
catalog/schema mutation and exact same-root/group/schema owner ACKs remain
unfinished. Native/metadata transport and advertisement proofs do not complete
the unchanged original sql-0671 CREATE or any other original SQL case.

The unchanged sql-0671 declaration now also has an independently captured
PostgreSQL fixture and native/mounted contracts. The complete declaration binds
its UUID PRIMARY KEY, text replacement/lower, exact NUMERIC rounding and JSONB
source extraction together. Inputs cover NULL propagation, half-away-from-zero
rounding, a result beyond binary64's exact integer domain and JSON numeric
display 123.00. Its derived semantic reader floor is 30, not its ASCH encoding
version 31. Native logical restore rejects forged generated source text.

The mounted contract opens a real namespaced native owner, prepares PRIMARY KEY
integrity commands through the production mutation planner and actual owner
catalog/read port, and commits all prepared writes, predicates and claims in
one transaction. It then closes/reopens the LSM database and reads the generated
outputs through the public SQL handler. Initial fixture attempts correctly hit
the table-identity and coordinated-write guards; assigning a stable table/shard/
range namespace and using real integrity preparation preserved both guards.
This still does not execute the original CREATE through distributed catalog
publication or qualify a mixed-version rollout, so the case remains unresolved.

Final production-source Debug qualification passed the full SQL/pgwire/native/
transaction/catalog gate (98/98 steps). The final native run including the
original declaration passed 267 tests with no leaks (50/50 steps); the final
mounted catalog suite passed 27 tests plus its linked parser test (53/53 steps).
Five focused reader/original contracts and all eight independent PostgreSQL
generated-expression tests passed. Formatting, Apache boundary/license checks,
control-catalog freshness and original inventory integrity passed. No original
disposition changed, and no rollout barrier is claimed by these results.

### Storage-independent reader admission contract

ReaderCapabilities, ReaderRequirements and ReaderFloor now share the pure
common/schema_readers contract. Metadata no longer imports physical schema
implementation to name its capability/floor types. Native schema feature
derivation remains in readerRequirementsForSchema; immutable epochs cache its
result exactly as before. The physical ASCH writer version is sourced from the
shared encoding ceiling so transport and durable encoding cannot drift. The
semantic ceiling is independently named rather than inferred from encoding.

The contract validates required dimensions before joining or covering a floor,
retains zero/zero only as an inactive floor or unknown implementation claim,
and requires an actual reporter incarnation for a known claim. Its standalone
Debug test exercises asymmetric ceilings, monotonic joins, unknown/zero,
partial and future-version rejection. This is a prerequisite for typed durable
table/manifest requirements, not their activation: server-derived publication
requirements and exact same-root owner installation ACKs remain unfinished.

Main was merged at 8e962ee4e2. Its error-ABI conflict retained every one of main's
647 Detail entries at the original numeric position, placing branch additions
after them; all 27 Debug error transport tests passed. The initial merged data
gate encountered sandbox EPERM on localhost bind and then a teardown panic.
The same compiled distributed UNIQUE/FK merge/split/restart regression passed
without skips or leaks when rerun with socket access. This does not qualify the
entire initial gate; the complete Debug gates are being rerun with that access.

The final reader-admission gate including the pure CI test passed 64/64 build
steps: one standalone contract test and both two-test metadata/data owners,
without skips or leaks. Apache boundary (2,056 sources), license headers,
control-catalog generation/freshness, Zig formatting and original inventory
checks passed. The broader integrated and native Debug runs remain in flight;
their pending results are not counted as passes. Cache GC was previewed only
(DRY_RUN=1); no caches were deleted.

### Definition-bound requirement transport (not producer activation)

TableRecord now carries optional internal schema_reader_requirements. Null is
omitted from JSON and preserves released legacy binary records and definition
fingerprints. A known pair survives clone/CAS and changes the definition hash;
it uses strict table storage metadata extension v3 for local and object owners.
Both full decode and borrowed point projections reject incomplete, zero,
future or trailing requirement bytes. Existing v1/v2 storage extensions retain
their interpretation. This field is not a public OpenAPI schema property.

Metadata decoder profile 39 protects marked typed upserts/replacements,
topology creates and extension deltas, plus nested catalog/FK/restore/private
index envelopes. A bounded 4 KiB scanner traverses object/array structure,
decodes escaped keys and skips schema/row strings without copying them. Null
or absent markers retain predecessor admission; malformed/exhausted envelopes
fail closed. The same classifier participates in single/batched final append.
This proves current-member decoding, not durable future-member admission.

Public producers do not populate the field yet. Before enabling them, derive
requirements from the validated current/read/historical schema identities and
physical manifests; never trust a caller's pair. Couple the pair, definition
CAS, monotonic floor and publication/outbox in one successful transaction.
Profile 39 must also be required by durable activation and subsequent global
metadata/member admission once these records are published; the earlier
profile-38 floor codec alone is insufficient. Finish exact same-root/group/
schema installation ACKs and shared restore admission before claiming rollout
safety or completing the original CREATE cases.

All correctness qualification used Debug. The focused reader gate passed
73/73 steps (pure, metadata wire/admission, table codec and data contracts).
The stable-source actual server/catalog/coordinator gate passed 120/120 steps
with 216 standard-runner tests plus its linked suites. Service/CHECK/lake
qualification passed 73/73 steps with 13 standard tests and linked suites.
The full metadata gate passed 70/70 steps: 226 and 316 control tests, 239
storage tests, one linked test and 28 replication tests passed without leaks;
two optional live PostgreSQL logical-CDC tests skipped their external-runtime
prerequisites. No original case disposition changed (898 remain unresolved).

The prior pure-contract/native qualification also finished: 268 native tests
plus one linked test passed (51/51 steps). The full merged reader/data/server
gate passed 165/165 steps. That gate reported a 16,100,278,272-byte macOS Debug
implementation-compiler peak against an inherited 13 GiB consumer reservation.
The implementation root now reserves 20 GiB (at least 25% headroom), scoped to
macOS Debug only; Linux/release claims and test coverage remain unchanged. The
rerun passed without the underestimated-bound diagnostic. This is compiler
admission, not a runtime memory-limit increase. Formatting, control-catalog
freshness, license/Apache boundary and inventory checks passed.

Final qualification after preparation validation and error transport changes
passed 169/169 Debug build steps and 218/218 standard-runner tests. This built
the actual server and ran the reader-admission, system-catalog, metadata,
service and compiled storage-owner gates. Linked suites passed 139 service,
317 metadata/control, 226 metadata/runtime, 239 storage and 28 replication
tests, plus the focused reader/codec/owner suites; two optional live PostgreSQL
logical-CDC tests skipped their external-runtime prerequisites. These linked
counts overlap other gate runs and must not be added as unique coverage.
The separate runtime error ABI and kernel failure identity suites passed
28 and five tests respectively. Invalid requirements retain their exact error
identity through the compiled kernel and leave committed revisions unchanged.

Requirement validation runs before coordinated admission takes its runtime
lock and before standalone write transactions. JSON serialization remains
format-only; malformed envelopes retain their existing domain-decoder errors.
Recognized requirement objects use an explicit bounded token length. This
preparation check validates the pair, not its derivation from the schema.
Durable decoder admission must protect pending private plans as well as active
native schemas: merely upgrading the active native reader floor from decoder
38 to 39 would leave pending plans and future membership insufficiently guarded.

Final formatting, control-catalog freshness, Apache boundary, license and
original inventory checks passed. The inventory remains 479 implemented,
136 rejected, 73 superseded and 898 unresolved; none of this transport work
earns original-case completion credit.

### Original-case reconciliation: defaults and array-parameter transport

Original sql-1109 through sql-1140 are now explicitly superseded by PostgreSQL's
prohibition on subqueries in persistent column defaults. This is not support
for 32 executable defaults. The exact statements return SQLSTATE 0A000 in the
independent PostgreSQL 18.6 oracle, preserve schema and complete seeded row
postimages, and fail native compiler/public HTTP admission before any catalog
callback. The compiler also verifies the exact diagnostic. The immutable source
inventory and source-owned statements/parameters are unchanged. The disposition
ledger names separate Debug compiler/public and PostgreSQL gates for this cohort.
The resulting count is 479 implemented, 136 rejected, 105 superseded and
866 unresolved; feature implementations remain at 479.

The independent array-read oracle now obtains parameter identities from actual
PostgreSQL Parse/Describe rather than forcing every API JSON-transported value
to jsonb. Only a server-described text[] slot admits a flat string/SQL-NULL
sequence; JSONB arrays remain JSONB when PostgreSQL binds that domain. Unsupported
array adapters and mismatched parameter counts fail explicitly. Parse/Describe
executes no statement and creates no named prepared resource. All nine existing
golden campaigns still match, including the original 14 stored-array reads;
the full reference-generator regression suite passed 120 tests.

The expanded oracle covers twelve additional unchanged original statements:
sql-0190, sql-0194, sql-0211 through sql-0213, sql-0280, sql-0283,
sql-0285 through sql-0287, sql-0295 and sql-0296. These remain unresolved.
Mounted native HTTP rejects their raw JSON-array parameters with 22023 under
the existing strict public transport contract. That contract requires a
SQLArrayValue envelope or PostgreSQL array text; it must not be silently relaxed
or bypassed by changing the source-owned parameter fixture. A public transport
decision is pending before activating this twelve-case campaign. sql-0292 also
remains unresolved: its exact array-transform predicate needs a dedicated
exercising seed, not an empty-result exception or a change to the earlier golden.

The Debug mounted gate exposed generic optional-read switches that failed to
compile for valid NotFound-only adapters. Owner/cancellation, identity and source
authority reads now retain every non-absence error without an unreachable else
prong. The native regression exercises narrow and fallible adapters through
requireActive, and actual memory/LSM candidate tests retain their fencing,
cancellation and recovery assertions. Updated test provisioning publishes the
live-directory marker and real seeded-row identities in the same transaction;
it does not weaken production directory admission. All 14 candidate tests pass.

Next original-case batches must be selected by shared execution boundary and
case volume, not by infrastructure-test totals. The largest remaining pool is
308 DDL/catalog/session cases, followed by the 131 source/joined mutation and
MERGE/recursion pool. Independent read cohorts must not wait for all catalog
rollout machinery. Compiler discovery is useful routing evidence only: its
admitted-shape counts do not establish public, durable or distributed parity.

Qualification at this checkpoint: the focused default-admission Debug public
gate passed 80/80 steps, the compiler default test passed, and all 20 inventory
auditor tests passed. This is not a passing expanded array-read or full public
API gate: the twelve transport cases remain red and uncredited pending the
contract decision above. The macOS Debug public-owner compiler measured
16,310,960,128 bytes against its older 15 GiB reservation. Its reservation is
now 20 GiB (at least 25% headroom); the focused gate rerun passed without the
underestimated-bound diagnostic. Other platforms and optimization modes keep
their previous scheduling bounds. No runtime memory limit was increased.

### Exact zero-effect mutations and recursive-source reconciliation

The mutation oracle now represents explicitly qualified zero-effect success,
not just positive affected-row counts. sql-1432 (self INSERT SELECT with
ON CONFLICT DO NOTHING) and sql-1462 (VALUES with the same conflict policy)
bind their untouched statements to fixed source digests and a real logical
primary-key profile. Independent observers must find a bounded, nonempty
conflicting population. Initial and final states of every table share the same
streaming row/byte admission; zero affected rows, empty RETURNING and identical
complete postimages are all required. Unlisted zero-effect cases, changed source
text, missing primary ownership, empty populations and quota failures cannot
acquire completion credit. The previous 48 positive mutation contracts remain
unchanged; the checked campaign now contains those 48 plus two qualified no-ops.

Native HTTP execution additionally retains the primary version, content digest
and exact JSON at every seeded key in all three tables. It proves that the
self-source mutation opens exactly one native source range read, and that the
VALUES conflict adds no full-table read or multi-relation capture. The initial
test incorrectly expected the multi-relation callback for a single-owner read;
source inspection established that this path pins its own native range view.
The corrected assertion checks that actual access path, not merely a zero
affected count. No production mutation shortcut or weaker uniqueness profile
was introduced to make these cases pass.

The same Debug gate reran the complete existing source-mutation campaign and
qualified original recursive mutations sql-0012 and sql-0013, whose executable
coverage had not been reconciled into their dispositions. Their exact recursive
source produces duplicate candidates; native set-valued UPDATE/DELETE effects
match PostgreSQL with activated logical PKs and complete three-table postimages.
This closes those original native fixture contracts, not arbitrary distributed
recursive workloads or their remaining fault/scaling obligations.

The combined mounted Debug gate passed 80/80 steps, both selected tests and
owner audits, without skips or leaks. The full PostgreSQL oracle regression
suite passed 122 tests. Read and streaming-mutation parameter adaptation now
share server Parse/Describe identities; a separate regression retains JSONB
arrays as JSONB while typed text-array path arguments stay SQL arrays. The
inventory is now 483 implemented / 136 rejected / 105 superseded / 862 unresolved.
The twelve raw-array transport cases remain unresolved and the expanded native
array campaign in the uncommitted worktree remains red pending its public-contract decision. No broad public
API/SQL release pass is claimed, and nothing has been pushed.

The combined Debug public-owner compile reported MaxRSS:18G. Its macOS Debug
reservation is now 24 GiB, retaining at least 25% headroom against a conservative
19 GiB envelope for that rounded observation. Earlier 20 GiB reservations below
refer to their historical qualification checkpoint or the separate data owner;
Linux and non-Debug bounds remain unchanged. This is build scheduling, not
runtime memory admission. Array-transport experiments are not included in the
verified checkpoint commit; their twelve original dispositions remain open.

### Durable metadata decoding before native schema installation

Metadata admission now also derives its global decoder minimum from the existing
durable topology activation at publication version 39 or newer. This is
independent of the native schema-reader floor: pending private definitions can
need the metadata decoder before a native layout is published. Ordinary appends,
store admission and batches retain the higher decoder minimum even when they
contain no relation mutation. Final append revalidation rejects a readiness
proof acquired before activation. The projection verifies the current metadata
incarnation, a nonempty membership claim and a supported decoder version;
foreign, missing-incarnation and future-version records fail closed. Active
native floor 38 cannot hide a stronger durable metadata activation. Legacy
pre-publication activations retain their previous behavior.

The monotonic existing activation record is reused, not another incarnation
codec or a scan of table/private-job inventories. Its getter now borrows bytes
from a native read transaction instead of allocating a second value copy.
Producer admission, command encoding, apply and reader projection share the
existing 1 KiB activation bound. The native regression verifies reopen while
the native floor remains inactive, rejected downgrade/foreign-incarnation
attempts, oversized-command rollback and fail-closed reads of a corrupt oversized
record. The service regression covers mixed-version members, ordinary commands,
batches, stale pre-activation proofs and both native-floor states.

This is a publication prerequisite, not activation of public producers. They
still must derive requirements from owned schema/layout identities, durably
activate the decoder before retaining marked private plans, bind schema CAS and
native floor publication atomically, and require exact installed-root owner
ACKs. No original SQL disposition changes at this checkpoint; 483 are implemented
and 862 remain unresolved. The final Debug metadata/service gate passed
112/112 steps, including borrowed/bounded projection and the corruption fixture.
Linked suites passed 139 service, 318 metadata/control, 226 metadata/runtime,
240 native metadata-store and 28 replication tests; two optional live PostgreSQL
CDC tests skipped their external prerequisites. The focused reader owners passed
seven service and two native catalog tests, plus the existing data/pure-contract
checks. These suite counts overlap the broader runs and are not unique-test or
SQL-case totals. Formatting, control-catalog freshness, Apache boundary, license
and original inventory checks passed. No public producer is enabled by this
checkpoint, and no push or broader SQL release pass is claimed.

### Explicit table publication outcomes and retained CREATE preparation

Compound placement publication now inspects the authoritative table CAS outcome.
Previously, the common table writer could silently reject a late FK-generation
reservation while its caller continued to publish the logical tablespace delta
and catalog revision. The CAS and common writer now expose acceptance to compound
callers; ordinary single-command replicated rejection remains a deterministic
no-op. Standalone compound replacement also aborts explicitly on rejection.
An identical already-installed replacement is accepted without a table rewrite;
a stale expected definition is not. Rejection branches remain before mutations.

Placement preparation and apply also require the physical replacement and derive
its only permitted fields from the same logical catalog cut. The logical table
binding must identify the expected physical table. Omitted updates, different
target IDs, unrelated table-definition changes and incorrect replica counts are
rejected. Catalog reads/storage failures propagate rather than becoming successful
publication. The native regression prepares a valid command, installs a late FK
reservation, checks unchanged logical and physical cuts, releases the reservation,
reopens the store, retries the original command and checks idempotent replay.

CREATE retains its owned parsed request across readiness and the catalog barrier
instead of parsing it a second time under the serialized catalog lock. This also
avoids minting a second set of private index identities. Storage and object-create
validation stay before that lock; namespace-dependent naming, schema-derived
index expansion and placement still use the admitted cut. The ownership test
overwrites and frees the original ingress buffer before reading retained fields,
and checks invalid command shapes and rejected creation cleanup.

These changes do not enable new schema producers or certify more original SQL
cases. Server-derived reader requirements, atomic reader-floor publication,
exact installed-root owner ACKs and their distributed faults remain required for
the larger DDL activation batch. Counts remain 483 implemented / 136 rejected /
105 superseded / 862 unresolved.

Final `zig build antfly-metadata-test antfly-metadata-service-test
-Doptimize=Debug -j2 --summary all` passed 80/80 steps: 139 service, 318
metadata/control, 226 metadata/runtime, 241 native metadata-store and 28
replication tests passed, plus the selection-owner check. Two optional live
PostgreSQL CDC tests skipped their external prerequisites. The explicit
`zig build antfly-system-catalog-test -Doptimize=Debug -j1 --summary all`
gate passed 54/54 steps and 262/262 tests; its catalog label filter includes
the CREATE-preparation test omitted by broad metadata runtime-prefix filters.
These overlapping suites are not original SQL-case totals. The initial
sandboxed run failed socket bind with errno 1 and its Python auditor blocked
in macOS timezone-service startup; a process sample confirmed that wait and
the diagnosed auditor was terminated before the permission-enabled rerun.
No tests or gates were weakened. Formatting, license and Apache boundary,
control-catalog freshness and inventory integrity checks passed. GC ran in
preview-only mode. No push or full SQL release pass is claimed.

### Owned schema preparation and server-derived definition requirements

`PreparedSchemaDefinition` owns the validated public schema and its compiled
runtime layout together. Runtime JSON inspection and durable encoding use that
same compiler result. The encoded result owns its bytes, layout version and
separate semantic/encoding requirements; the encoding requirement is the frame
version actually emitted by this compiler, not a caller's claim or an assumed
restored-frame identity. Restored physical frames still require header/manifest
validation and their own publication proof.

Marked ALTER/rewrite/FK candidates now recompute their reader requirements from
the accepted active definition and retained read definition. Schema comparison
returns the requirements of the two layouts it already compiled; a newly retained
predecessor uses those facts instead of another compile. An independently retained
read definition is validated separately. An unchanged active schema must still
include a more demanding retained read layout. Unmarked producers remain unmarked:
this does not bypass durable decoder activation, atomic native-floor publication
or exact installed-root owner ACKs. Historical cluster floors remain monotonic
and cannot be derived solely from the current table's two JSON definitions.

Native tests destroy the ingress buffer, encode and decode the retained layout,
compare actual frame requirements, join active/read requirements, and exhaustively
inject allocation failures through preparation, encoding and retained-layout
derivation. The server test checks an added JSONB-array column raises requirements,
an unchanged active definition retains its read layout's requirement, and an
unmarked producer is not silently activated. The SQL test-selection audit caught
the new compiler tests missing from discovery; its root now explicitly imports
the compiler and the gate selects those ownership contracts. The focused Debug
gate passed both tests and 21/21 steps. The Debug catalog gate passed 54/54
steps and 264/264 tests, including the marked-schema requirement-refresh
regression. The earlier full SQL gate passed 819 local tests (three benchmark
skips) and 226 native tests before the new discovery/filter wiring. The final
full Debug SQL gate with that wiring passed 21/21 steps, 821 local tests
(three deliberate benchmark skips) and 226 native tests. Formatting, licensing,
Apache boundary and inventory checks
passed; GC is preview-only. Nothing from this checkpoint has been pushed.
No original SQL dispositions change: 483 implemented / 136 rejected /
105 superseded / 862 unresolved.

### Prepare every table target before publishing compound effects

The common native table writer now has separate admission/encoding and commit
phases. Prepared values own their table frame, optional lake-maintenance frame
and rename bytes; their only borrowed field is the name in the enclosing
command/plan. Fingerprints and source-change facts are prepared before writes.
Decoded predecessor metadata never escapes through a borrowed full TableRecord.
Unchanged names no longer require delete/reinsert of the same name-index key.

Initial FK support prepares every parent before its catalog delta. Restore
prepares every target before logical binding changes or removal of old physical
generations. Extension lifecycle prepares the complete table set before writing
tables or extension rows, distinguishes an accepted unchanged definition from
a rejected upsert, and rejects duplicate target IDs with a linear hash index.
Ordinary replicated upsert still treats rejection as a deterministic no-op;
compound publication does not confuse that outcome with successful admission.
Atomic topology create also requires actual table-write acceptance before it
can install ranges. Published FK writers propagate failed admission rather than
silently advancing their publication state.

Regressions cover a malformed second target after rename allocation, a late FK
lock, cancellation after the first staged table write, complete batch commit and
reopen, an actual two-target restore's late-writer veto (including unchanged
catalog/source revisions and absent ranges), and extension rejection/retry/replay.
Schema finalization now derives a marked definition's requirements from only
the acknowledged active layout before modifying any owned fields; this changes
no durable cluster floor. Its independent fixture retains full-text declared
paths requiring reader v14 after the v31 JSONB-array read layout retires. The
initial assertion incorrectly expected v13 from relational columns alone; it
was corrected against the existing independent format contract, not by lowering
the derived requirement.

This is the shared atomic publication boundary, not completion of schema-reader
activation: the server-derived schema candidates still need durable decoder
activation, one same-transaction monotonic floor join, and exact installed-root
owner ACKs before public producers can be enabled. The final Debug metadata,
service, reader, catalog, initial-FK and restore gates passed together at
150/150 build steps. Standard-protocol catalog owners passed 270/270 tests;
linked runners passed 243 native metadata-store, 318 metadata/control,
226 metadata/runtime, 139 service, 28 replication, 25 restore and 19 initial-FK
tests, plus reader and owner-selection checks. Two optional live PostgreSQL
CDC tests skipped their prerequisites. Four table-reader tests include both
the marked-update and corrected finalization contracts; seven service-reader
tests and the existing data/pure checks passed. These suites overlap and are
not original SQL-case totals. No leaks or failures remained in the final run.
Formatting, licensing, Apache boundary, control-catalog freshness and original
inventory checks passed. GC was preview-only. No original SQL case credit or
push is claimed at this checkpoint.

### Admit marked private definitions only behind the durable decoder barrier

Schema-only index/FK publication shape checks now permit the reader marker to
change with its definition and verify the logical requirements of active and
retained layouts. A marked predecessor cannot become unmarked. The prospective
frame must match the current compiler's encoding; an existing frame's encoding
is not inferred from JSON or silently rewritten. Unrelated table settings remain
immutable, and allocation failures release compiled layouts.

The native admission boundary requires the durable definition-decoder activation
before retaining marked index, FK-initial-create, FK-generation or restore plans,
or writing a marked table frame. Discovery includes predecessor/support tables,
replaced restore targets and external FK parents. This is decoder admission,
not proof of installed data-owner roots or a native reader-floor publication.

Snapshot validation now also requires that barrier for marked definitions and
retained plans even while the native floor remains inactive. Table projections
borrow schema bytes; the regression projects a 64 KiB definition using 4 KiB of
scratch. JSON scanning is confined to actual plan/job keys: opaque restore
active pointers are not parsed as JSON. The index admission regression rejects
an older activation without advancing standby revision or installing locks,
then exercises marked replay/reopen and snapshot validation. The restore
regression round-trips the actual command and checks rejection leaves no job or
physical-name reservations before retrying after activation.

The focused Debug restore gate passed 51/51 steps and 25/25 restore tests.
The final combined Debug metadata/service/reader/catalog/initial-FK/restore/index
publication gate passed 167/167 steps and 278/278 standard-protocol tests. Linked
runners passed 244 metadata-store, 318 metadata/control, 226 runtime, 139 service,
28 replication, 19 initial-FK, 25 restore and 31 index-publication tests, plus
eight table-reader, seven service-reader and two data-reader checks. Two optional
live PostgreSQL CDC tests skipped their prerequisites; no failures or leaks
remained. These overlapping suite counts are not original SQL-case totals. An
earlier broad run overlapped source edits and is not qualification evidence.
Formatting, licensing, Apache boundary, inventory integrity and control-catalog
freshness checks passed. Cache GC was preview-only. No full SQL release pass or
push is claimed. Original SQL dispositions are unchanged; public producers
remain gated on the same-transaction native floor join and exact installed-root
owner acknowledgments.

The next integration should reuse the existing signed index-owner receipts and
enrollment/live-root checks, not add a parallel service-ACK authority. Bind the
required reader pair into a versioned compact publication head and its candidate
identity so owner acknowledgment can validate the current enrolled reporter
without decoding schema-sized plans. Prepare the joined requirement for every
table in a compound publication before any catalog effects; publish that one
monotonic native-floor update in the same transaction as the admitted table set.
Then qualify the original ALTER TABLE and CREATE INDEX cohorts through the public
endpoint, including restart/root replacement and rejected-write poststates.

### Reader-bound compact heads and installed-root acknowledgment admission

The private index-publication head is now AIPH v2: 192 fixed bytes, including
independent u32 semantic/encoding requirements and a checksum covering both.
The head joins requirements from the admitted before/after table images; an
unmarked plan retains an absent pair, not an assumed current capability. Its
plan digest binds those images, projected candidate/native-manifest preparation
rechecks the pair, and checkpoint verification independently recomputes it.
This replaces the unreleased v1 head rather than adding a compatibility path.

Metadata decoder v40 is required for admission, owner decision/status reads and
planless ACKs, including unmarked plans. The service classifier selects it before
the older v39 definition-marker profile. Catalog preflight and native apply use
the same version. Snapshot cuts require v40 for a compact head even when a marked
table earlier in row order needed only v39; an inactive data floor is no bypass.
Ordinary appends already honor the durable activation's actual version.

ACK admission reuses the existing signed receipt, current enrolled reporter,
physical root, live range/placement and ordered source-position checks. The
head's pair is checked against capabilities in the store header already loaded
for those checks, not a request claim or schema-sized plan decode. Cancellation
and cancel-release can still clean an unpublished build after capability loss;
they retain all root/authentication/position checks and cannot publish a schema.
Non-cancellation progress also requires a current table-serving role; changing
the store to metadata-only cannot bypass the floor's table-owner scope.

Debug regressions cover independently joined axes, partial or unsupported
checksummed pairs, changed candidate/head requirements, v39-vs-v40 admission,
snapshot row ordering, missing/insufficient capabilities on both native and Raft
owners, and complete cancel/release progress after capability loss. Recovery
also substitutes a valid pair with a fresh checksum to verify immutable plan
binding. Maximum-owner pure progress stays allocation-free and fixed-size;
no throughput/latency benchmark improvement is claimed. The focused gates passed
102/102 steps; the pure gate passed 16/16 tests. The final combined Debug
metadata/service/reader/catalog/initial-FK/restore/index-publication/ACK/progress
gate passed 179/179 steps and 278/278 standard-protocol tests. Linked runners
passed 244 metadata-store, 318 control, 226 runtime, 139 service, 28 replication,
19 initial-FK, 25 restore and 32 index-publication tests, plus eight table-reader,
seven service-reader, two data-reader and four HTTP ACK contracts. Two optional
live PostgreSQL CDC tests skipped their prerequisites. No failures or leaks
remained; overlapping suite counts are not original SQL-case totals. Formatting,
inventory integrity, Apache boundary, licensing and control-catalog freshness
checks passed. The license checker initially stalled in macOS timezone startup;
sampling identified that block and its read-only rerun passed. GC was preview-only.
No original SQL disposition changes, full SQL release pass, public activation
or push is claimed.

This is ACK admission, not complete publication authority. Owner progress still
stores logical receipt digests, not the accepted physical root attestation.
Publication must retain/revalidate physical evidence and support owner-root
replacement/re-attestation instead of treating an old phase counter as proof
of a newly installed root. That work and the same-transaction joined reader
floor/catalog decision remain prerequisites for enabling public producers.

### Durable physical evidence and latest-stage re-attestation

AIPO v3 owner records retain a physical commitment beside each logical receipt
digest. The commitment binds the signer identity, metadata incarnation, current
reporter, authority, physical root generation, logical receipt and signature.
Exact ACK observation checks both commitments; a logical receipt from another
root is not success for the requested physical acknowledgment.

Signed receipts persist separately as canonical AIPR v1 records, exactly 512
bytes with explicit little-endian fields, reserved-zero validation and a physical
checksum. They are not JSON storage blobs. Parsing and encoding need no heap;
the API still uses its typed JSON envelope. Owner progress remains fixed-size
at 648 bytes, and ACK/status paths do not load schema-sized plans. Recovery keeps
only one fixed-size enrollment cache, avoiding repeated grant lookup/JSON parsing
when consecutive proofs share an immutable root identity.

The same transaction stages head, owner and physical record. Failure before or
after the physical write must abort all three; production and fault-injected
adapters use the same staging helper. A different currently enrolled root may
re-attest only the latest completed logical stage. That changes the physical
commitment and head revision without double-counting progress or advancing the
namespace source. Historical stage evidence cannot be rewritten this way.

Snapshot/recovery projections include proof records and validate signatures,
logical/physical commitments, plan/candidate/owner bindings and retained signer
enrollment. Raw 16-byte catalog incarnations are compared through the canonical
32-byte hexadecimal root identity, not as equal-width arrays. Reverse directory
verification rejects orphan proofs. Terminal replacement/drop deletes only the
proof slots actually retained by its old owner cohort; empty slots add no delete
traffic, and proof history is bounded by the one retained attempt.

Debug tests cover all-byte corruption, valid-checksum padding and signature
faults for both authority kinds, atomic abort before/after proof persistence,
missing/malformed/substituted/unenrolled evidence, orphan records, exact physical
ACK status, latest-stage-only refresh, persisted refresh through reopen and
alternating terminal cohorts. The focused gate passed 72/72 steps, including
16 pure progress tests and linked 32 publication, four HTTP and one decoder
barrier tests. Earlier focused builds exposed a stale test fixture reference,
a raw-vs-hex incarnation mismatch and a private validator visibility error;
those runs are not qualification evidence. The final combined Debug metadata,
service, reader, catalog, initial-FK, restore, publication, ACK and progress gate
passed 179/179 steps and 278/278 standard-protocol tests. Linked runners passed
244 metadata-store, 318 control, 226 runtime, 139 service, 28 replication,
19 initial-FK, 25 restore and 32 publication tests, plus reader and HTTP checks.
Two optional live PostgreSQL CDC tests skipped their prerequisites; no failures
or leaks remained. Counts overlap and are not original-case totals. Formatting,
inventory integrity, licensing, Apache boundary and control-catalog freshness
checks passed; cache GC was preview-only. No full SQL release pass or push is
claimed. Original dispositions remain 483 implemented / 136 rejected /
105 superseded / 862 unresolved; no throughput improvement or public activation
is claimed.

The metadata re-attestation operation is available, but native execution adapters
still return early for acknowledged phases. Next, add read-only latest-receipt
re-attestation without granting mutation permission, then bounded repair when a
replacement root lacks durable stage data. Publication must revalidate the
accepted physical evidence against the current owner cut and commit the joined
reader floor, catalog and schema decision together. Public producers remain off
until those boundaries and their distributed recovery faults are qualified.

### Read-only native receipt re-attestation

Metadata now distinguishes a logically acknowledged live stage whose accepted
physical proof no longer matches its current root. For the existing native
fenced/reserved receipt stages, it issues a compact read-only attestation grant
after repeating the live table/range/name/lifecycle cut and validating current
signer enrollment, role and reader capabilities. The grant contains only the
native identity, table name and signing view: no schema inputs, allocation or
mutation permission. It disappears once the current proof is accepted.

The native control receiver recovers the existing durable receipt under its
physical-root lease and verifies its exact logical digest against metadata.
Signing preserves that lease's root generation and the granted head revision;
a later routing/signing lookup cannot relabel the read. Missing or different
local state returns `PrivateIndexRootRecoveryRequired`, a classified 503, with
zero admissions or controls. Historical stages do not get this grant; stale
data-Raft and unsupported later-native proofs fail closed instead of being
reported as fresh native acknowledgments.

Debug tests exercise automatic grant issuance/retirement, swapped namespace or
signing view, zero-write recovery, immutable recovered outcomes, root-generation
pinning, mutation-method rejection, wrong table, missing local receipts and a
different but internally valid native cut. The focused publication/ACK gates
passed 69/69 steps with linked 32 publication, four HTTP and one decoder tests.
An earlier test-local shadowing error was corrected; that failed build is not
qualification evidence. The final combined Debug metadata/service/reader/catalog/
initial-FK/restore/publication/ACK/progress gate passed 179/179 steps and 278/278
standard-protocol tests. Linked runners passed 244 metadata-store, 318 control,
226 runtime, 139 service, 28 replication, 19 initial-FK, 25 restore and 32
publication tests plus reader and HTTP checks. Two optional live PostgreSQL CDC
tests skipped prerequisites; no failures or leaks remained. Counts overlap and
are not original SQL-case totals. Formatting, inventory integrity, licensing,
Apache boundary and control-catalog freshness checks passed. The latter needed
a read-only rerun after sampling confirmed macOS timezone startup was blocking
Python before repository code. GC was preview-only. No original SQL disposition
changes, full SQL release pass, public activation or push is claimed.

Repair is still a separate prerequisite: a missing private stage cannot be
reconstructed by trusting a phase counter or minting a new position. Use
provenance-checked native root/journal recovery before re-driving control work,
and add the remaining post-reservation native receipt production. Then revalidate
current physical evidence in the atomic reader-floor/catalog/schema decision
before enabling the original public index/ALTER cohorts.

### Native ordered private build pages

The existing private physical builder now participates in the shared native
transaction, durable-outbox and standby replay pipeline. Its private `build`
effect retains the prepared page, not a time/byte scheduling budget that could
select different rows during replay. Primary preparation reuses the compiled
reservation across pages. Replay owns normalized page data and independently
reopens its immutable candidate; it does not rerun a timed source scan. The
existing namespace/catalog/fence, progress CAS and exact physical source-hash
checks remain mandatory. Command decoding bounds candidate count and data size.

An allocation-free, length-framed page digest avoids hashing JSON byte-array
expansion. One fixed 176-byte AIBP v1 checkpoint binds the reserved owner, page
digest, encoded progress and actual native cut. Retry metadata stays bounded by
one slot, replaced by the next page and retired with cancellation GC admission.
Cold recovery verifies retained checkpoint framing, cut and progress consistency.
The separate build outcome has no publication-receipt field: physical readiness
does not certify distributed UNIQUE claims or authorize schema activation.

The focused Debug page gate passed 53/53 steps, including 14 prerequisite and
five native storage tests. Its regression builds three source rows in four
one-record/EOF pages, recovers a pending mirror outbox after reopen, rejects
clock gaps atomically and preserves exact primary/standby progress on duplicate
replay without publishing schema version two. The fixture now seeds the standby
with the primary's actual physical row records rather than independently
generated row metadata; the failed independent-row fixture did not justify
weakening the source-hash CAS. The subsequent combined metadata/service/reader/
catalog/FK/restore/publication/progress Debug run passed 179/179 steps and
278/278 standard tests, plus its linked runners. Two optional live PostgreSQL
CDC tests skipped missing prerequisites. Inventory, Apache boundary, licensing
and control-catalog freshness checks passed.

Review then added a stricter duplicate check against the prepared encoded
progress digest, including substitution of both well-formed stored values with
matching checksums. This also computes the progress digest off-lock instead of
rereading progress during commit. Final qualification of that hardening plus the
page and combined gates passed 191/191 steps and 278/278 standard tests, with
linked 14 private-candidate and five native page tests as well as the metadata,
service, control, runtime, reader, FK, restore and publication runners. Counts
overlap and are not original SQL cases. Two optional live PostgreSQL CDC tests
skipped missing prerequisites; there were no failures or leaks. The final log is
`/private/tmp/antfly-native-build-pages-final-debug.log`. No original-case disposition change,
throughput improvement, public activation, full SQL release pass or push is
claimed; the original counts remain 483 implemented / 862 unresolved.

Still required: identity-only receiving-worker build dispatch and its live
metadata authorization, provenance-backed missing-root repair, complete
distributed UNIQUE validation, native validated/installed/released receipt
production, and the atomic current-proof/reader-floor/catalog/schema decision.
Qualify the original public index/ALTER cohorts only after these boundaries and
their distributed recovery faults are complete.

### Native build receiver and resource-accounted candidate reuse

The existing internal owner-control operation now accepts bounded build work.
The receiving server obtains fresh metadata permission and inspects the actual
native job before constructing an identity/binding-commitment/budget request.
Metadata checks the plan, owner, exact fence, candidate/reservation identities
and schema versions. Request bodies supply neither schema-sized inputs nor
physical allocation choices or replay positions. The compiled owner rechecks
the durable reserved job and cancellation; missing state is
`PrivateIndexRootRecoveryRequired`, not permission to re-admit or mint a cut.
The physical root generation comes from the lease that executed the request.

One resident, immutable candidate cache avoids per-page schema compilation.
Compilation is single-flight outside apply exclusion, uses the shared Resource-
Manager preparation budget, and is reclaimable only when unpinned. A lease
keeps its stable-address allocator alive through page destruction, even after
eviction. Epoch invalidation prevents an in-flight compiler from repopulating
an evicted cache. Queue waits use `std.Io` events and observe semantic request
cancellation/deadlines without OS-thread spin loops. Cancellation intent and
cleanup retire cached preparation outside the commit lock. Cache data never
grants metadata permission or replaces the durable job/fence/progress checks.

The kernel step prepares/applies at most one page or returns terminal physical
progress. Per-row preparation and serialized staging check request activity;
committed/pending outcomes survive cancellation after commit. Completion is
receipt-free, checks the actual index/owner/progress/cut and exposes persisted
outbox debt even when the in-memory pending hint is stale. It does not certify
distributed UNIQUE claims or publish a schema. C-ABI response ownership is
reserved before mutation. Execution controls cross that ABI only as borrowed
synchronous callbacks, never as request-selected authorization.

Focused Debug qualification passed 87/87 steps, including six native page tests,
14 private-candidate prerequisites, six C-ABI tests, 32 publication tests and
46 shared runtime contracts. The native work-count test builds 32 rows in nine
four-record/EOF pages with one candidate compilation. It covers invalid binding,
budget denial and backing-allocation failures, cancellation during preparation,
pinned eviction, reopen with the original cut, canceled jobs, independent queued
cancellation/deadlines and eight contending callers sharing one compilation.
Receiver checks cover fresh permission, no admission on missing state, wrong
table/binding, pinned applying-root identity and committed pending progress after
cancellation. These are component/native tests, not a live distributed SQL
producer or original-case qualification.

Review subsequently preserved root-recovery and invalid-budget errors through
both typed failure registries, retained resource-pressure identity over HTTP,
and advanced the storage-owner ABI from 77 to 78 for the changed request layout.
A regression rejects version 77 before dereferencing any request fields. Final
combined Debug qualification of this source revision passed 206/206 steps and
278/278 standard tests, plus linked native page, candidate, C-ABI, shared runtime,
metadata, service, control, runtime, reader, FK, restore and publication runners.
Two optional live PostgreSQL CDC tests skipped prerequisites; no failures or
leaks remained. Counts overlap and are not original SQL cases. The final log is
`/private/tmp/antfly-native-build-receiver-abi78-debug.log`; the pre-bump broad run
was stopped and is not final qualification. Inventory,
Apache boundary, licensing and control-catalog freshness checks passed. No
throughput improvement, full SQL release pass, public activation, push or
original-case disposition change is claimed: 483 implemented / 862 unresolved.

Next: provenance-backed missing-root repair, durable source-to-claim production
and complete distributed UNIQUE validation, native validated/installed/released
receipt production and the atomic current-proof/reader-floor/catalog/schema
decision. Data-Raft execution is not substituted with native authority. Enable
and qualify the original public index/ALTER cohorts only when these remaining
boundaries and their distributed recovery faults are complete.

### Durable logical claim-source pages

Native private builds now prepare a logical UNIQUE tuple alongside each physical
index member from the same decoded typed row. Mutations and backfills share the
NULL-distinct witness encoder. The cached logical constraint plan remains
independent of physical index direction; source equality keys are not physical
DESC sort keys. Historical layouts use projected plans without reparsing JSON.
The table name comes from the validated physical-root route, not an untrusted
request body or a mutable SQL alias.

Each native page stages its source batch, physical index writes, progress,
source cut, retry checkpoint and durable mirror outbox in one transaction.
Preparation owns and hashes payloads before serialized apply. Source batches
hold at most 128 rows and 1 MiB of payload. Compact headers and separate bodies
are retained in a generation/job-bound queue capped at 64 pending batches or
8 MiB of actual stored batch/key bytes. Capacity rejection rolls back the whole
page, including a would-be EOF/ready transition and native clock advance.
Completion exposes the retained claim debt instead of reporting global UNIQUE
readiness.

The unreleased native control envelope is version 23. The AIBP version-two,
208-byte checkpoint independently retains the latest source-header digest as
well as prepared-command/progress commitments and the native position. Recovery
checks at most 64 compact headers, their chain, exact cut, allocation generation,
progress and terminal state; it does not read large source payloads at startup.
The native source reader verifies that checkpoint and header chain in its pinned
snapshot before reading and checking one owned body. Missing bodies require root
recovery rather than a successful delivery acknowledgment. Cancellation deletes
only the exact bounded queue keys in the same ordered cleanup transaction, before
generation-local physical GC; it never scans unrelated table rows for source debt.

There is deliberately no consumption/ACK producer yet. Nonzero consumed counters
are rejected until ordered authenticated complete-delivery closures are
implemented. A mirror ACK, physical ready result, empty transient RPC response or
source queue counter cannot certify distributed uniqueness or publish the schema.

The initial producer and subsequent queue/fault revision each passed 87/87 focused
Debug steps. The latter includes seven native page tests, six C-ABI tests,
14 private-candidate prerequisites, 32 publication tests and 46 runtime-contract
tests; counts overlap. Native evidence covers retained source bytes through
mirror failure/reopen/duplicate standby replay, corruption/missing bodies,
checksummed header substitution, logical versus DESC physical encoding, a full
64-batch backlog rejecting EOF across reopen, and exact cancellation cleanup.
Final checkpoint/consumption hardening passed 87/87 Debug steps. A subsequent
test-only revision also passed 87/87 steps, directly exercising the native
checkpoint-verified source reader and a NULL-containing backfill row. Its
logical tuple matches ordinary mutation encoding, differs for another primary
witness under NULLS DISTINCT, and drops that witness under NULLS NOT DISTINCT.
No failures or leaks remained. The final log is
`/private/tmp/antfly-native-index-claim-source-reader-debug.log`.
Inventory, Apache-boundary, licensing and control-catalog checks passed;
cache GC was previewed only and deleted nothing. No original case, public
activation, throughput improvement, full release pass or push is claimed.
Original dispositions remain 483 implemented / 862 unresolved.

Next delivery boundary: authenticated source-to-target claim draining and
complete source/target seals, provenance-backed root repair, native validated/
installed/released receipt production, then the atomic current-proof/reader-floor/
catalog/schema decision. Qualify unchanged original public index/ALTER cohorts
against PostgreSQL only after those boundaries and recovery faults are complete.

### Exact target-delivery commitments and pinned-root signatures

Private target claim outcomes now include a canonical set commitment bound to
the actual reserved job. Preparation computes it before apply exclusion.
Commitment validation uses at most 128 stack entries and no allocator or JSON
encoding; request order and identical duplicates do not change the set digest.
Different subsets retain different digests even when their maximum immutable
claim-creation cut happens to coincide. Conflicting parent identities for one
tuple fail as UNIQUE violations. Claimed write counts cannot exceed the
deduplicated set size. Replay/retry outcomes reuse the prepared commitment.

The native receiver can decorate committed, fully mirrored delivery with the
existing enrolled-root signature machinery. The signing context pins the
complete logical delivery digest and the generation of the lease that applied
the claims, not a later routing/root observation. Delivery commits to the job,
exact set and actual native cut. Node/store/reporter identity must match the
fresh permission view; signing loads the existing key and never creates a
replacement. Unavailable or changed signing identity preserves committed
progress as an explicit signing failure. Pending mirror results cannot be
signed. The data runtime shares its post-commit key-loading/error path between
owner-stage results and claim-delivery results.

The signature has a distinct logical domain and is not a validated publication
receipt. Verification of the signature alone also does not prove current
placement, authorization of a source subset, complete source-batch coverage,
target EOF inventory completeness, or journal-backed replacement-root recovery.
The source-consumption guard remains closed. Still required are canonical
source-to-owner partitioning/splitting, authenticated source-subset provenance,
fresh target authority checks, durable complete-delivery closures, source/target
seals and their ordered recovery/cleanup paths before activation.
Partitioning must account for target claim/inventory overhead as well as source
body bytes: a 1-MiB source batch is not necessarily a 1-MiB target request.
Single-row target admission must be checked before persisting an undeliverable
source batch; multi-row batches must split by actual destination and byte budget.

The first revision passed the 87-step focused Debug gates. The combined final
Debug run passed 136/136 steps, including the data-runtime gate and stronger
context/count checks. Tests cover set
reordering/coalescing, conflicting tuple owners, changed commitments, signatures,
cuts and applying-root generations, pending mirrors, retained progress on signing
failure, and rejection of delivery signatures as validated publication receipts.
The final receiver refinement reports root recovery required when fresh reserved
metadata permission refers to a missing physical job; it never re-admits it.
Its additional Debug publication/data-runtime run passed 110/110 steps, including
128-claim set order independence, rejection of a 129th claim before stack
admission, and signed-cut tampering. The combined run's log is
`/private/tmp/antfly-native-claim-delivery-final-debug.log`; the final receiver
log is `/private/tmp/antfly-native-claim-delivery-root-recovery-debug.log`.
Counts overlap and are not original SQL cases. No failures or leaks remained.
Static inventory, Apache-boundary, licensing and control-catalog checks passed.
No original-case credit, public activation,
throughput improvement, release pass or push is claimed; dispositions remain
483 implemented / 862 unresolved.

### Bounded canonical source partitions and target admission headroom

Source preparation now accounts for a single row's actual target inventory
charge before emitting it, including repeated parent-table/key fields and
claim/inventory overhead. A row that fits a physical ordered key but cannot fit
one target request becomes a durable key-too-large build failure, not an
undrainable ready batch or an endlessly retried transport-size failure. The
failure emits no member rows or private forward keys and survives reopen at the
same native cut. Temporary allocation/resource/cancellation errors still do not
poison a generation. Source bodies require strictly increasing primary keys,
rejecting duplicate/noncanonical members before they can inflate coverage.

An owned immutable cohort validates complete, disjoint range coverage, exact
plan/table identity, unique groups and consecutive owner ordinals. It preserves
ordinal identity after range sorting, bounds retained bytes, and routes each
logical tuple hash with binary search. Preparing a page uses fixed 128-entry
scratch rather than a row-by-owner scan or allocation per destination. It checks
the exact header/scope/canonical body and request cancellation outside apply
exclusion, then partitions by destination and actual target-byte charge.
Different partitions for one owner are allowed when their combined target
inventory charge exceeds 1 MiB even though the source body fits 1 MiB.

Each partition has a source-row mask and deterministic descriptor commitment.
Complete coverage rejects omissions, duplicates, overlapping masks and unknown
partitions independently of response order. Structural target checks pin the
owner, shared manifest, predecessor schema, candidate/reservation commitments
and successor schema version. A borrowed internal request constructor preserves
those exact selected claim bytes without choosing a physical allocation.
These are data preparation/coverage commitments, not authorization booleans,
remote-durability proofs, publication receipts or source-consumption permission.

Native integration reads a real checkpoint-verified source batch, partitions
four rows including a NULL-distinct witness, applies those exact target claims
and retries at the same creation cut. Its nine source batches remain pending:
successful target writes alone cannot retire source debt. The additional wide
row regression proves durable target-size failure before source member emission.

Qualification caught two local-name compilation errors and a test-discovery
gap. Routing tests now live in an explicit server test leaf; production routing
stays Apache-only. The registered 87-step Debug run passed with 35 publication
tests rather than the previous 32, plus eight native page tests, six C-ABI tests,
14 private-candidate prerequisites and 46 runtime contracts. The router tests
cover allocation failures, gaps/overlap/duplicate groups, owned input lifetime,
all 4,096 owner boundaries within 13 comparisons per lookup, splitting at actual
target bytes, exact masks, body substitution, cancellation/deadlines and failed
source rejection. Counts overlap and are not original SQL cases.

Final shared-manifest/predecessor-schema hardening passed 87/87 Debug steps,
including all 35 publication/router tests, eight native page tests, six C-ABI
tests, 14 private-candidate prerequisites and 46 runtime contracts, with no
failures or leaks. The final log is
`/private/tmp/antfly-claim-delivery-partitions-scope-debug.log`.
Inventory, Apache-boundary, licensing and control-catalog checks passed; original
dispositions remain 483 implemented / 862 unresolved. No throughput improvement,
public activation, full release pass or push is claimed.

Still required: authenticated source-subset production, fresh target authority
verification and durable complete-delivery ACKs, source/target EOF inventory
seals, provenance-backed root repair and data-Raft execution, native validated/
installed/released evidence and atomic current-proof/reader-floor/catalog/schema
publication. Do not open the source-consumption or public activation guards on
the strength of these structural coverage tests.

### Current-authority verification for signed target deliveries

Target deliveries now have an explicit signed evidence type, distinct from a
publication receipt or source ACK. It checks the exact logical claim set,
committed native position, signature domain and native signing identity. Its
partition verifier also checks the shared manifest/schema commitments and exact
selected source rows. A valid signature for one byte-split partition cannot
acknowledge another partition sent to the same target. The wire response can
extract this evidence only from signed, fully mirrored successful delivery;
unsigned responses, phase acknowledgments and pending mirrors do not qualify.
Response validation hashes the requested claim set and verifies its signature
once, rather than repeating both operations when extracting signed evidence.

A read-only metadata transaction verifier independently checks current job
permission, active pointer, lifecycle lock, admitted owner/range dispatch,
catalog authority and live table claim, then checks enrolled current physical
authority. Physical checks are shared with publication receipts: current
reporter/root identity, root generation, native binding, reader capabilities,
observed namespace and native cut. Delivery cannot borrow the capability-loss
exception reserved for authenticated cleanup of an unpublished candidate. No
full-table scan or schema-sized plan projection is introduced on this path.
The returned commitment is data, not a lease or source-consumption capability;
an accepting durable transaction must repeat the authority reads.

Real-store fault checks reject signature/claim substitution, a genuinely signed
foreign owner/candidate binding, cancellation, active-job/lifecycle/range
replacement, reporter/root/generation changes, reader-role/capability loss,
wrong/missing source authority, insufficient observed native cut and removed
native binding. These fixtures use synthetic physical claim values to qualify
metadata verification, not to replace actual native production/reopen evidence.
The existing native/C-ABI suites remain required separately.

The initial union-tag test typing error was corrected. The 87-step Debug run in
`/private/tmp/antfly-claim-current-authority-final-debug.log` passed with 35
publication/router tests, eight native page tests, six C-ABI tests, 14 private
candidate prerequisites, 46 runtime contracts and the decoder barrier, without
failures or leaks. The subsequent single-pass response-validation refinement
passed the publication, decoder-barrier, runtime-contract, C-ABI and candidate
gates in `/private/tmp/antfly-claim-current-authority-single-pass-debug.log`.
That run ended at 84/87 steps: its last native test hit FileNotFound and the
runner could not create a cache output path. The entire worktree `.zig-cache`
was subsequently confirmed absent, with roughly 150 GiB newly free. Our cache
operation was preview-only and deleted nothing. The unchanged native target
was rebuilt and rerun in the normal worktree cache; all 53/53 steps passed,
including all eight native page tests and 14 candidate prerequisites, with no
failures or leaks. Its log is
`/private/tmp/antfly-claim-current-authority-native-after-cache-loss-debug.log`.
Inventory, licensing, Apache-boundary and control-catalog checks passed.
Original dispositions remain 483 implemented /
862 unresolved; no new original SQL case or throughput improvement is claimed.

Still required: actual pinned-root source-subset signing, durable all-partition
source ACKs, source/target EOF inventory seals, provenance-backed root repair,
data-Raft execution, terminal native publication evidence and atomic schema/
catalog publication. Source-consumption and public activation guards remain
closed; a signature or successful read-only verification is not permission to
discard source batches.

### Authenticated native source observations and current source authority

The existing private owner-control route now supports a `source` read operation
under fresh build permission. The request contains only table/group/job identity:
no caller-selected allocation, source position, body, schema or verification
flag. One pinned compiled owner reads the actual reserved job, mirror debt,
physical checkpoint and bounded source-header chain in one storage snapshot,
then fetches at most one 1 MiB body. It does not compile a candidate, take the
global apply lock, create a commit position or consume a batch. Pending outbox
debt schedules recovery and returns no body to sign. Absence, pending mirrors
and failed batches cannot become successful source proofs; an authenticated
empty batch remains distinct from absence and from a complete-stream seal.

Native control ABI 79 adds the read selector and borrowed deadline/cancellation
controls. The persisted mutation envelope remains version 23: this observation
does not add a replay mutation. Binary payloads cross the control boundary as
bounded base64, preserving arbitrary tuple/key bytes without per-row JSON or
Unicode conversion. Shared payload decoding checks header/scope/body integrity,
framing, exact lengths, row bounds, strictly increasing primary keys and target
admission size. The leased adapter retains both the decoded owned body and the
already encoded wire arena, so network serialization does not re-encode it.
The HTTP client applies the source-specific response cap during transport, not
only after downloading, while retaining the small cap for other controls.

The serving receiver signs only the canonical batch observed from that actual
lease, capturing its digest and root generation before releasing storage. It
uses the existing enrolled root-key loader and never creates a key after the
read. Source, target-delivery and publication signature domains are distinct.
The remote consumer verifies the signature once, then checks the canonical body
against that signed header during decoding. Bad native-binding assertions are
invalid peer receipts, not evidence that the metadata catalog is corrupt.

Metadata source preflight now shares the exact current job/catalog/range
verifier and physical-root verifier with target deliveries. It independently
checks the source's physical table name, enrollment, reporter, generation,
native binding, reader capabilities, namespace and observed cut in the caller's
transaction. The authenticated body digest is retained across these reads,
avoiding another complete-body hash. These methods remain read-only; a durable
ACK must repeat them in its accepting transaction. No boolean or signature by
itself grants source-consumption authority.

Qualification: the optional-result coercion and a local-name shadowing compile
error were fixed without changing test expectations. The 87-step Debug run in
`/private/tmp/antfly-native-source-proof-debug-v2.log` passed all native page,
C-ABI, publication, candidate and runtime-contract gates. Actual leased-owner
coverage then passed 94/94 steps, including cold absence, a checkpoint-backed
signed source read, JSON/body decoding, and retention of the original generation
after a routing/root switch. The final current-authority/receipt-classification
run is `/private/tmp/antfly-native-source-proof-receipt-errors-debug.log`:
94/94 steps, 35 publication tests, three leased-owner/adapter tests and the
decoder barrier, with no failures or leaks. Real-store faults cover source and
target cancellation, job/range/root replacement, capability loss and cut lag.
Native regressions verify no candidate-cache compile/hit changes from source
reads and no body/signature during pending mirror debt. Counts overlap; no
wall-clock throughput improvement or original SQL case completion is claimed.

Original dispositions remain 483 implemented / 862 unresolved. Still required:
durable all-partition source ACKs, source/target EOF inventory seals, provenance-
backed missing-root repair, real data-Raft execution, terminal native publication
evidence and atomic schema/catalog activation. Source consumption and public
activation guards remain closed. No push or full release pass is claimed.

### Compact complete-delivery statements and touched-owner routing

The existing private claims route can now prepare a source-root-signed complete
delivery statement. Its input contains the expected source ordinal/header digest
and at most 128 signed target deliveries, not caller-provided source rows,
allocations, ranges or success flags. The receiver obtains fresh build permission
and destination routes from one metadata snapshot, reads the actual first pending
batch from one pinned compiled owner, reconstructs the exact canonical partitions
and verifies every target signature/set against those actual logical rows.
It rejects omitted/duplicated/unknown partitions, unused destinations, wrong
source batches, pending mirrors and inconsistent target authority. The signed
statement binds all row masks, logical member counts/contributions, exact target
root commitments, the source header and the applying source root generation.

Metadata looks up only the distinct destinations touched by the batch rather
than scanning all admitted owners or returning schema-sized plan inputs. Sparse
global ordinals remain intact. Fixed 32-byte hash keys compare identically against
a range bound and its first 33 bytes, even when the key equals the bound's first
32 bytes; compact routes therefore need no potentially large primary-key bounds.
The owned subset validates disjoint spans and routes in logarithmic time. Unlike
an admitted full cohort it permits gaps, but every actual source row must resolve
to exactly one supplied destination. Empty source batches require no destinations
and still do not imply complete-stream EOF inventory validation.

Source/target logical member hashing is shared with the existing exact claim-set
commitment and remains independent of physical claim-value encoding. The XOR
contribution is diagnostic accounting, not authority or a substitute for exact
delivery proofs, native journal provenance or EOF seals. Bodyless metadata
preflight verifies the source signature, complete masks and independently signed
target deliveries, then repeats current job/catalog/range, enrolled-root,
capability, native-binding and observed-cut checks in the same transaction.
Even a correctly source-signed statement containing an invalid target signature
is rejected. No row bodies enter this compact metadata verification interface.

Requests and responses use bounded parsers and source-specific transport caps.
The result owns its fixed-data target proof copies after releasing the source
batch/route arena; signing retains the original pinned generation and expected
digest. The private route remains standalone-native only, with no fabricated
data-Raft cut or new public index endpoint.

Qualification fixed nullable range ends and use of a private equality helper.
`/private/tmp/antfly-source-complete-delivery-debug.log` passed 94/94 Debug steps,
including 37 publication/router tests, three actual leased-owner/adapter tests
and the decoder barrier. The broader native run
`/private/tmp/antfly-source-complete-delivery-native-debug.log` passed 87/87 steps,
including eight native page tests, six C-ABI tests, 14 candidate prerequisites and
46 runtime contracts. Counts overlap and are not original SQL cases. Coverage
includes allocation failures, sparse 4,096-owner ordinals, missing destinations,
all tested fixed-key prefix comparisons, JSON round trips, source/delivery
substitution, pinned-generation changes, independent target-signature rejection
and metadata authority faults. No throughput improvement is claimed.

This completes compact provenance production/preflight, not durable acceptance.
The metadata acceptance journal, atomic native consumption/outbox replay,
source/target EOF inventory seals, provenance-backed repair, data-Raft execution
and terminal schema/catalog activation remain unfinished. Neither the `closed`
response nor successful preflight permits deleting source batches. Original
dispositions remain 483 implemented / 862 unresolved; consumption/public
activation guards stay closed. No release pass or push is claimed.

### Ordered complete-delivery acceptance postimages

The metadata transaction adapter now prepares an ordered acceptance head and
expected target-inventory postimages after repeating complete-delivery signature,
catalog, placement, capability and observed-root/cut verification in that same
transaction. Preparation stages no writes. The staging primitive propagates
every write failure, including the final source-head write; its caller must
abort the outer transaction on any error.

There is one fixed 416-byte source head and one 184-byte expected inventory per
target, not one persistent metadata record or row body per source batch. Each
new batch updates its own source head and only the destinations it touched.
Heads bind the metadata incarnation, reserved native binding, logical scope,
source ordinal/header chain/cut, cumulative row count, logical delivery, previous
accepted decision and a separate physical-proof provenance prefix. Inventories
count logical contributions, not whether this particular target RPC inserted
new rows: a delivery retry can report zero inserts without losing expected
members. XOR remains diagnostic, never the activation/inventory authority.

Logical delivery identity excludes target physical cuts/signatures but retains
the exact binding, claim sets, masks, routes and member contributions. Re-attested
delivery of the same source batch returns the existing accepted decision without
rewriting either inventory or provenance. An older/skipped/substituted batch,
scope/binding change, broken header chain, row-count overflow or advance beyond
a ready source head is rejected. Acceptance itself does not imply EOF validation.

Advancing a source requires a separately domain-separated, source-root-signed
consumption statement matching the previous accepted decision, source binding,
header, ordinal and cumulative rows. Its applying cut must follow the consumed
source cut, and the metadata observation must have reached that actual cut.
The next source batch may have been built before consumption; requiring its build
cut to follow the consumption cut would incorrectly reject queued native pages.
Consumption and delivery must name the same current physical authority. The
native producer for this consumption statement is still missing; no request
field or mirror ACK is accepted as a substitute for a retained native journal.

The real-store fixture exercises first acceptance, exact retry, changed physical
delivery proof with stable logical identity, ordered second acceptance, missing/
forged/lagging consumption evidence, stale source/target metadata authority,
allocation failure before staging, fixed-record corruption and target-count
deduplication. Its postimages are deliberately confined to an aborted test
transaction: no production/Raft command currently stages the new record family.
Snapshot key registration, forward/reverse cohort verification, the coordinated
decoder floor and proposal/status handling must precede production persistence.
Native consumption/outbox replay, EOF seals, provenance-backed repair, data-Raft
execution and terminal catalog/schema activation also remain unfinished.

The preliminary Debug publication run passed 60/60 steps in
`/private/tmp/antfly-source-acceptance-state-debug.log`. The broader native/C-ABI
run passed 87/87 steps in `/private/tmp/antfly-source-acceptance-full-debug.log`.
The extended ordering/replay faults and actual leased-owner adapters passed
94/94 steps in `/private/tmp/antfly-source-acceptance-final-debug.log`, including
39 publication tests, three leased-owner tests and the decoder barrier. Final
review isolated the ready-state guard from a stale-sequence rejection; that
stronger test passed the 60/60-step publication gate in
`/private/tmp/antfly-source-acceptance-ready-guard-debug.log`. All gates used Debug,
with no failures or leaks; counts overlap. Boundary, license, formatting, control
catalog and original-inventory checks passed. GC preview deleted nothing.
Do not infer original SQL completion from these component gates. Original
dispositions remain 483 implemented / 862 unresolved. No activation, source
deletion, throughput improvement, full release pass or push is claimed.

### Durable ordered acceptance and recoverable owner anchors

Ordered complete-delivery acceptance now has a metadata transition and private
proposal/status path, rather than a preparation-only helper. Preflight and
ordered apply independently repeat current catalog/lifecycle/range, enrollment,
reporter, capability, root-generation and observed-cut checks. Apply stages the
source record, all touched target records and their owner-directory commitments
in one transaction. Staging errors are never swallowed. Stale authority detected
before staging produces no acceptance; a linearizable status observation resolves
uncertain proposal outcomes without treating transport success as a decision.

AIPO v4 adds separate source-delivery and target-inventory commitments to each
owner, preserved by ordinary progress ACKs. This avoids a global acceptance CAS
and makes optional records independently checkable, including zero-row sources.
The v41 coordinated decoder barrier covers admission, owner ACKs and acceptance;
the compiled-owner ABI is v80. No compatibility format was added for the private
development layouts. These gates do not enable native source consumption.

Each source retains its fixed 416-byte accepted head and one canonical compact
complete-delivery statement, not row bodies or an unbounded batch history. Each
target retains its reserved binding and 184-byte expected inventory. The source
statement retains the actual source signature and independently signed target
deliveries for checkpoint verification. Exact logical retries, including new
physical re-attestation, preserve the original durable decision and inventory
contributions. One owner write merges source and target anchors when a source
also owns its own destination, avoiding duplicate owner writes.

Snapshot/recovery forward verification checks anchor presence and hashes,
canonical source bytes, source/target signatures and enrollment, immutable owner
and schema bindings, cumulative count bounds, and source/target row accounting.
Reverse enumeration rejects extra/orphan journal records. Native-owner binding
is included in snapshots, with the decoder requirement protected. Terminal DROP
and predecessor replacement remove the old records through the existing bounded
owner cleanup. Expected inventory and its XOR remain diagnostic/provenance
accounting, not an EOF seal or a substitute for actual physical target validation.

The snapshot/reopen test exposed a shared receipt-preflight bug: metadata
snapshots materialize store reports, but point receipt verification expects
rebuilt report directories. Both owner-ACK and source-acceptance preflight now
enter the existing shared indexed-report read path, which performs the existing
one-time derived-index initialization. Ordered apply already initializes those
same derived indexes in its transaction. No per-receipt fallback store scan or
separate relational restore system was added, and root checks were not relaxed.

Acceptance status also reports actual metadata standby-outbox debt. A locally
committed acceptance is not reported as durable while that debt exists. This is
separate from the already enforced source/target native mirror debt. A retained
historical acceptance remains a durable fact, not current build/consume
permission; native consumption must independently obtain current permission.

A serialization-only upper-bound test maximizes every fixed integer/byte-array
field across 128 destinations, without claiming valid delivery or SQL execution.
The statement is at most 517,306 bytes and acceptance 519,084 bytes under that
conservative fixture. This evidence allowed keeping the existing 512 KiB proof
transport limit and its separately bounded acceptance wrapper. No throughput
benchmark or performance gain is inferred from serialized sizes.

The focused Debug run in
`/private/tmp/antfly-source-acceptance-durable-debug-v6.log` passed 69/69 steps:
40 publication tests, four service/ACK tests and the coordinated decoder barrier,
with no failures or leaks. The actual metadata fixture uses committed Raft
application, exact batch replay, logical duplicate acceptance, standby-pending
status, snapshot exclusion/orphan faults and reopen recovery. Its physical claim
values are synthetic metadata-verifier inputs, not original SQL qualification.
Broader native/C-ABI and leased-owner qualification is recorded separately in
`/private/tmp/antfly-source-acceptance-durable-full-debug.log`: 120/120 Debug
steps passed, including 40 publication, four service/ACK, eight native page,
six C-ABI, three leased-owner, 14 candidate-prerequisite and 46 runtime-contract
tests, plus the decoder barrier. Counts overlap; no failures or leaks were
reported. Formatting, boundary, license, control-catalog and original-inventory
checks passed. GC was preview-only and deleted nothing.

Native consumption/deletion/outbox replay is still absent: the native source
head rejects a nonzero consumed prefix. Source/target EOF seals, provenance-
backed missing-root repair, actual data-Raft execution, terminal native evidence
and atomic schema/catalog activation remain unfinished. Public UNIQUE activation
is still guarded. Original dispositions remain 483 implemented / 862 unresolved;
component tests do not change them. No full release pass or push is claimed.

### Native consumption transaction and independent recovery anchor

The shared Apache storage layer now owns the accepted-decision data contract
and consumption-prefix digest. Metadata heads convert to that same contract;
the metadata decision and signed consumption domains remain unchanged. This
avoids a parallel ELv2/native implementation of the bytes a consumer must bind.
The core data contract is not metadata authentication: the receiving server must
independently fetch durable acceptance and current execution permission before
invoking it. No public consume receiver/selector has been enabled in this step.

Native consumption has a prepared transaction effect using the existing native
apply, durable outbox and standby replay machinery. Preparation verifies the
checkpoint, exact reserved job, first pending canonical body and metadata
decision/header identity off-lock. Serialized staging repeats job/cancellation,
consumed-prefix, cumulative-row, source-header and body-hash checks. It accepts
tail emission after preparation but never advances a changed consumed prefix.
All validation/encoding precedes the first write; deletion, retained record,
source head, actual native sequence and mirror outbox share the outer atomic
transaction. Any write/outbox failure must abort that transaction.

AICQ v2 binds one retained 736-byte consumption record. The record owns the
accepted decision, original consumed header, later applying cut and previous
consumption-record digest. Consumed sequence is the original source cut, not
the ACK cut: queued batches may have been built before ACK publication. Exact
retry returns the retained cut without another sequence or transport record.
Full-prefix consumption still retains header/progress evidence independently
of the deleted queue. Missing/mismatched anchors or a consumption cut beyond
the actual native authority fail recovery. Cancellation retires the record and
the exact pending ordinal keys in its existing atomic cleanup effect.

The private native mutation envelope is v24 and compiled-owner ABI v81. Standby
replays the prepared decision against its actual matching pending body, not a
rescan or a caller-selected cut. A gap does not move its applied marker; duplicate
transport replay keeps the same marker. Source reads include the actual retained
consumption record only after mirror debt clears. Wire validation rejects a
consumed prefix spliced into another first-pending header or scope. This record
is data, not a signature: metadata advancement still needs the separately signed
consumption statement from the actual applying root.

Real native fixtures deliver actual target claims before consuming sources.
They exercise rollback before commit, repeated consumption without a new cut,
all nine queued batches including empty EOF, original physical build-cut
retention, reopen, missing-anchor rejection, mirror failure and outbox recovery,
standby gaps, and exact replay. Their metadata decisions are explicitly synthetic
storage-engine inputs, not independent metadata-server or original SQL proof.
The initial engine gate passed 53/53 Debug steps in
`/private/tmp/antfly-source-consumption-native-debug.log`. Expanded qualification
is recorded in `/private/tmp/antfly-source-consumption-full-debug-v2.log`.
That final Debug run passed 129/129 steps: 40 publication, four service/ACK,
eight native page, six C-ABI, three leased-owner, 14 candidate prerequisite and
46 runtime-contract tests, plus the decoder barrier. Counts overlap; no failures
or leaks were reported. The expanded fixture delivers all actual target claims
before consumption. The added test's module-constant typo was corrected without
changing expectations. Boundary, license, inventory, control-catalog, formatting
and diff checks passed. A sandbox-blocked Python timezone initialization was
confirmed by stack sampling; the read-only checks completed outside that
sandbox. GC was preview-only and deleted nothing.

Still required: a receiving-server consume operation that fetches metadata
acceptance independently, its compiled-owner dispatch and resource/deadline
admission, root-signed consumption publication, producer retry/orchestration,
and end-to-end cross-service fault tests. EOF/inventory seals, missing-root repair,
real data-Raft execution and terminal schema/catalog activation also remain
unfinished. Public UNIQUE activation stays guarded. Original dispositions are
unchanged at 483 implemented / 862 unresolved; no SQL completion, full release
pass, throughput improvement or push is claimed.

### Receiving-server consumption and applying-root attestation

The existing private claims receiver now also accepts a consume operation. Its
wire input contains immutable owner identity and expected source ordinal/header
digest only, never accepted metadata, a physical binding or a caller-selected
cut. The receiver obtains fresh build permission and independently queries its
configured metadata transport for durable source acceptance. Missing acceptance,
metadata mirror debt, query/epoch mismatch and changed batch identity fail before
apply. The source binding and original header come from the actual leased owner;
an exact retry after deletion instead validates its retained consumption record.
Current permission remains separate from the historical accepted decision.

Compiled-owner ABI v82 adds a controlled consume selector. Preparation uses the
relational working-set resource budget, with cancellation/deadline checks before
and after preparation and again inside serialized staging after lock admission.
The applying lease supplies root generation. Commit/pending outcomes survive
post-dispatch ingress cancellation; no second effect is issued to resolve an
uncertain result. Native apply, durable outbox and standby replay remain shared
with the other private index effects.

Only a committed, non-pending retained record can be signed. The signer binds its
consumption-prefix digest to the pinned metadata identity, reporter and actual
applying root generation, using the existing root key loader. A generation change
between source read and apply prevents attestation. The response exposes the
same signed consumption statement the metadata journal already requires for the
next accepted batch. This is not an EOF seal or permission to activate UNIQUE.

The leased-owner fixture uses real canonical source bytes, compiled dispatch and
native commits; its metadata acceptance remains explicitly synthetic. It covers
missing/pending acceptance, changed batch, exact retry after body deletion,
unsigned and mirror-pending results, changed root generation and a tampered
signed cut. Storage fixtures additionally cover cancellation before preparation,
after preparation and at serialized staging. These are component qualifications,
not original SQL or complete cross-service fault evidence.

Still required: bounded producer retry/orchestration, EOF/inventory seals,
provenance-backed missing-root repair, actual data-Raft execution, terminal
schema/catalog activation and end-to-end distributed faults. Public UNIQUE
activation remains guarded; original SQL dispositions remain unchanged.

Final qualification in
`/private/tmp/antfly-consumption-receiver-full-debug-v2.log` passed 129/129
Debug steps: 41 publication, four service/ACK, eight native page, six C-ABI,
three leased-owner, 14 candidate-prerequisite and 46 runtime-contract tests,
plus the decoder barrier. Counts overlap; no failures or leaks were reported.
The final run includes the mirrored-payload budget-error mapping correction;
successful committed responses are preserved. Wire-test fixtures were corrected
to reach the intended mixed-operation rejection without weakening assertions.
Boundary, license, inventory, control-catalog, formatting and diff checks passed.
GC was preview-only and deleted nothing. Original SQL dispositions remain
483 implemented / 862 unresolved. No throughput improvement, full SQL release
qualification or push is claimed.

### Time-sliced source delivery producer and mounted transport

The source producer now advances one remote operation per scheduling turn:
read durable acceptance, recover signed consumption of that exact prefix, read
and decode one signed source batch, deliver each deterministic target partition,
obtain source-root-signed complete coverage, accept it durably, then consume.
The scheduling owner must pin the immutable cohort, provide a bounded allocator
and end-to-end deadline, and link leadership cancellation. No catalog/apply lock
may span a turn's RPC. Uncaught errors seal the local attempt; replacement starts
from durable status rather than a remembered target cursor or success boolean.

Target mirror debt and absent signatures cannot advance the partition cursor.
Decoded row buffers, the prepared plan and target response storage are released
after the owned compact closure is captured, before metadata proposal. A lost
acceptance reply enters read-only resolution. If acceptance committed, native
consumption follows only that exact durable decision; if it did not, only the
same logical closure may be retried. Discarding every volatile buffer after an
unknown reply remains safe. Initial status can recover an already accepted batch
without resending target claims or another acceptance mutation. A failed source
build fails the attempt instead of masquerading as unavailable signing. Completed
results remain readable after cancellation without another remote operation.

The HTTP adapter uses a validated, bounded owner directory with binary lookup and
the existing body-bound private routes/grants. One shared deadline/cancellation
budget covers metadata and owner requests. Source acceptance/status responses
are capped at 16 KiB at transport admission as well as before decoding. Controlled
metadata forwarding preserves not-sent versus unknown-delivery classification.
Actual client instantiation exposed and fixed two latent transport issues: source
status incorrectly treated the owned `CatalogRead` envelope as raw bytes, and the
runtime-dependent owner reply limit lacked an explicit `usize` type. Source status
now releases the owned envelope and verifies its metadata incarnation.

Nonempty synthetic component fixtures cover target debt, uncertain acceptance,
read-only resolution, signed consumption, failure/cancellation, no-op completed
retries and exhaustive allocation failures. The real leased compiled-owner fixture
loses a committed *synthetic metadata* acceptance reply, verifies the actual source
body remains, destroys local producer state, then resumes to actual native
consumption and a signed drained prefix without another acceptance. Its bounded
point-catalog capability is explicit; the production deadline guard against an
unbounded admin fallback was not weakened. Mounted HTTP qualification exercises
typed status ownership, request/grant transport and owner dispatch; it does not
claim a live multi-service cluster or original PostgreSQL SQL parity.

The focused qualification in `/private/tmp/antfly-delivery-producer-debug-v5.log`
passed 106/106 Debug steps, including 43 publication, four service/ACK, three
leased-owner and 46 runtime-contract tests plus the decoder barrier, with no
failures or leaks. Broader qualification is recorded separately below.

Still required: leader-fenced work discovery, bounded fair scheduling/pooling and
automatic service wiring, bounded parallel target fanout, EOF/inventory seals,
provenance-backed missing-root repair, actual data-Raft execution, terminal
schema/catalog activation and complete distributed faults. The session and mounted
transport do not activate themselves. Public UNIQUE activation remains guarded;
original SQL dispositions remain 483 implemented / 862 unresolved.

Broader qualification in `/private/tmp/antfly-delivery-producer-full-debug.log`
passed 129/129 Debug steps: 43 publication, four service/ACK, eight native page,
six C-ABI, three leased-owner, 14 candidate-prerequisite and 46 runtime-contract
tests plus the decoder barrier. Counts overlap; no failures or leaks were
reported. Boundary, license, inventory, control-catalog, formatting and diff
checks passed. GC was preview-only and deleted nothing. This milestone is not
original SQL qualification, a throughput benchmark, automatic activation or a
push.

### Bounded work discovery, paged cohorts and typed local delivery

The compiled metadata facade now exposes non-mutating work discovery, current
head lookup and paged immutable owner routes. Discovery walks the shared live
table-lifecycle lock directory, not retained terminal heads or user tables. That
directory remains authoritative through installing/releasing, when the pending
namespace pointer is already absent. Each turn visits at most 16 lock records;
unrelated live lifecycle kinds consume that same budget. The cursor is an opaque
position in canonical decimal-key order, not a numeric table-ID ordering. A lock
whose index head is missing, terminal or bound to another job is corruption, not
permission to proceed. Point head/route reads verify lifecycle and pending-pointer
state; receiving mutation ports still independently obtain fresh permission.

Cohort capture performs at most 16 owner/dispatch point pairs per page, checks
the durable owner-group directory and admits at most 4 MiB of raw route data in a
page. The wire cap is 32 MiB to accommodate escaped maximum-length keys without
silently shrinking supported key sizes. Capture owns copied table/range strings,
pins plan identity rather than mutable progress revision, caps the complete
cohort at 8 MiB, and validates complete disjoint coverage before use. All 4,096
supported owners can be captured across short read transactions. Duplicate pages,
replaced jobs and incomplete captures fail closed. No new durable work index,
terminal-history scan, schema-input clone or public route was introduced.

In-process delivery now calls the same receiving ControlPort/ClaimPort with
owned typed responses. It transfers the already decoded source batch and signed
closure allocations rather than serializing and reparsing them as local JSON.
Ownership transfer verifies the actual owned proof and generation and clears the
old owner's allocation before teardown. HTTP still decodes its wire response,
then retains only copied fixed closure parts, not its entire parsed JSON arena.
The producer's acceptance/retry protocol is unchanged.

Qualifications include maximum-owner capture, exhaustive small-capture allocation
failures, replaced/duplicate pages, real metadata-store discovery in both native
and Raft-authority fixtures, and bounded discovery through unrelated locks with
1,000 retained terminal heads left unread. Synthetic nonempty producer tests run
both wire and typed-local transport with allocation failures; the actual leased
compiled-owner restart/uncertain-acceptance scenario uses the local receiving
ports. The metadata decision in that native fixture remains explicitly synthetic;
these tests are not original SQL or complete distributed activation evidence.

Still required: leader-fenced scheduler execution/pooling and automatic service
wiring, bounded parallel target fanout, EOF/inventory seals, missing-root repair,
actual data-Raft execution, terminal schema/catalog activation and full distributed
fault qualification. This work provides discovery and local transport, not an
automatically running scheduler. Public UNIQUE activation remains guarded;
original SQL dispositions remain 483 implemented / 862 unresolved.

Final qualification in `/private/tmp/antfly-index-work-local-full-debug.log`
passed 129/129 Debug steps: 46 publication, four service/ACK, eight native page,
six C-ABI, three leased-owner, 14 candidate-prerequisite and 46 runtime-contract
tests plus the decoder barrier. Counts overlap; no failures or leaks were
reported. Boundary, license, original-inventory, control-catalog, formatting and
diff checks passed. The discovery regression includes the shared lifecycle
directory rather than a terminal-head scan. GC was preview-only; nothing was
deleted. No original SQL case, throughput benchmark, service activation or push
is claimed by this milestone.

### Native service scheduling and bounded residency

The standalone control cadence now drives private index discovery, cohort
capture, fence/reserve receipt delivery, bounded backfill and source consumption
through the existing receiving ports. The host captures only an already admitted
native binding, metadata incarnation and writer generation. Raft and hot-standby
authorities remain excluded; receiving operations independently revalidate current
permission. Neither discovery nor cached scheduling state is an execution grant.

The pool retains at most four jobs and four distinct owner actors per job. Each
turn performs one facade/receiving operation outside the pool mutex. Pending
owners back off; expired attempts restart from durable status. After 4,096
successful quanta a non-executing job yields residency so four long-lived jobs
cannot permanently exclude later work. This is a scheduling bound, not a measured
latency/throughput guarantee. Authority changes cancel retained work. Allocations
are charged to the existing relational working-set budget, and service teardown
joins callers and releases their charges before destroying DataServer.

Build failure observations stop the attempt without authorizing consumption,
publication or a synthesized failure ACK, and without terminating the service.
Durable cancellation/failure publication still needs its complete state machine.

Tests exercise distinct owner admission, residency
rotation without evicting an executing job, uncertain acceptance, authority loss,
allocation failures and scheduled recovery through the actual leased compiled
owner. Metadata decisions in that owner fixture remain explicitly synthetic.
The full gate in `/private/tmp/antfly-index-worker-full-debug.log` passed
156/156 Debug steps: 48 publication, four service/ACK, eight native page,
six C-ABI, three leased-owner, 14 prerequisite, 46 runtime-contract and
104 standalone tests, plus shared domain tests and the decoder barrier.
Counts overlap; no failures or leaks were reported. The initial sandboxed
standalone run could not bind the TLS fixtures' localhost sockets; the final
permitted run passed all 104 tests. Boundary, license, original inventory,
control-catalog, formatting and diff checks passed. Cache housekeeping preserved
the active Zig cache and deleted no Zig/Rust output; optional UV pruning was
stopped while waiting on a shared lock, without touching other UV processes.

Still required: EOF/inventory seals, durable failure/cancellation completion,
bounded parallel target fanout, missing-root repair, actual data-Raft execution,
terminal schema/catalog activation and full distributed fault qualification.
Public UNIQUE activation remains guarded; original dispositions remain
483 implemented / 862 unresolved. No original SQL completion or benchmark is
claimed by connecting this private worker.

### Durable consumed-EOF completion

Accepted EOF and physical source consumption remain separate facts. Completion
now joins the retained accepted EOF to an actual root-signed consumption prefix:
same incarnation, binding, ordinal, decision, batch and cumulative rows, with a
later native cut in the exact source namespace. Metadata independently repeats
current owner, range, enrollment and reported-cut checks before committing.
An unreported consumption cut is deferred, not accepted or treated as a service
failure. This is not a caller-supplied "done" flag.

One bounded transaction CASes and stages the retained source proof, owner seal
and compact cohort head. The owner flag prevents double counting; the head
counts completed sources and records a stable revision at the final completion.
The head is 208 bytes regardless of cohort size. Source records retain a fixed
binding prefix, so completion does not reparse the last batch's complete target
proof array. Full checkpoint verification still verifies those proofs, the
consumption signature, enrollment, owner anchors and exact completion count.
The private formats are AIPH v3, AIPO v5 and AIDA v2; coordinated decoder floor
42 guards admission and transition tag 81. No legacy private-format reader was
added, and ordinary document/relational row formats did not change.

The receiving metadata service, compiled facade, standalone adapter and mounted
HTTP transport share the completion contract, body-bound authority and bounded
reply budget. The producer cannot report completed EOF until metadata confirms
completion without standby debt. Unknown replies resolve through a read before
retrying the exact prefix. Restart may recover a consumed EOF after its body is
gone; completion does not require resending claims or acceptance. Once all EOFs
are complete, the worker stops repeating source operations while waiting for
physical target validation. It does not synthesize a validation/install ACK.

Tests cover committed acceptance/completion, actual transaction rollback at each
of three staged writes, bad signatures/prefixes and unreported cuts, duplicate
completion, snapshot count mismatch, standby debt, reopen, lost completion
replies, allocation failures and the actual leased compiled owner's recovered
consumption. The leased-owner fixture's metadata decisions remain synthetic;
independent real metadata tests qualify durable completion separately.

The aggregate gate in `/private/tmp/antfly-index-completion-full-debug-v2.log`
passed 159/159 Debug steps and 17/17 progress-state tests, including publication,
service/ACK, leased-owner, native page, C-ABI, runtime-contract and all 104
standalone tests. The subsequent test-only expansion in
`/private/tmp/antfly-index-completion-fault-debug.log` passed 60/60 Debug steps
with all 48 publication tests. It distinguishes uncertain completion before
commit (one prefix retry, no second consumption) from a lost committed reply
(read-only resolution, no second mutation). Counts overlap; neither gate
reported a failure or leak. Boundary, license, original inventory,
control-catalog, formatting and diff checks passed.

Still required: frozen expected target-inventory projections and physical target
seals, native validation/install/release, durable failure/cancellation completion,
missing-root repair, bounded parallel target fanout, actual data-Raft execution,
terminal catalog publication and complete distributed faults. A completion
counter or XOR inventory is not physical-root validation or authority by itself.
Public UNIQUE activation remains guarded. Original SQL dispositions remain
483 implemented / 862 unresolved; this milestone claims no original SQL case or
throughput benchmark.

### Frozen target expectations and bounded physical claim inventory

The metadata projection now derives a fixed target expectation only after the
durable consumed-EOF cohort barrier. It binds the metadata incarnation, exact
reserved binding, layout generation, completion revision, consumed source
prefix, primary-build progress, table identity and accepted target membership.
The source record retains the fixed EOF header alongside its binding; this
projection does not decode the whole target proof array or scan the owner
cohort. It still reads/hashes the bounded retained source record; fixed-size
source-prefix storage is a remaining read-amplification opportunity, not a
performance improvement claimed here. Anchored reads reject oversized records
before hashing. AIDA v3 and decoder floor 43 cover the private metadata change. The
source header exposes its progress digest through a named accessor so callers
cannot accidentally interpret position padding as part of that digest.

The receiver obtains this expectation independently from current metadata and
inspects the actual native binding under the existing leased-owner facade.
Native envelope 25 carries bounded inventory effects through the existing
atomic mutation, authority-position and durable-outbox machinery. No new C
selector, public route or row format is introduced.

The first native turn freezes new claims. Exact duplicate claims remain
permitted, including batches prepared before freezing. Subsequent turns scan at
most 128 generation-local records and 1 MiB, honoring caller byte/time limits and
cancellation. Each inventory entry must have its exact live claim, schema/table
identity and valid native provenance. Progress retains a cursor, logical count
and XOR, plus a strong ordered chain over physical claim hashes and provenance.
Only fixed owned fields survive preparation; no read snapshot or row buffer
survives between pages. Replay recomputes the actual frozen page and rejects a
supplied cursor/chain that does not match it. Cancellation cleanup retires the
generation-owned progress record.

The real-LSM fixture derives expected membership from the delivered source. It
qualifies freezing, exact duplicates, missing companions, changed expectations,
byte/memory denial, cancellation/deadlines, three-record pages, forged replay
chains and reopening with retained progress. An actual normalized replication
ingress page commits the inventory cursor and transport marker together; exact
replay is inert. Independent durable metadata tests reject projection before
EOF completion and recover the exact expectation after reopen. Receiver tests
preserve the existing typed conflict for a late new claim instead of reporting
a transient receiver outage.

The final-source aggregate gate in
`/private/tmp/antfly-index-inventory-final-debug.log` passed 159/159 Debug steps,
including 48 publication tests, eight native page tests, six C-ABI tests, 46
runtime-contract tests, four ACK tests, three leased-owner tests and all 104
standalone tests. It also passed the publication-progress target; suite counts
overlap and must not be added as independent coverage. Boundary, license,
original-inventory, control-catalog, formatting and diff checks passed. No
ReleaseFast correctness result is substituted for this gate.

Inventory readiness is deliberately **not** a physical-index validation ACK.
The count/XOR is diagnostic, not standalone authenticated membership evidence.
Still required are bounded secondary-generation verification, joining both
proofs into authenticated native validation and its capability barrier,
restore/adoption verification of
the scanned prefix (including claim-only orphans), install/release, durable
failure completion, missing-root repair, bounded parallel target fanout, actual
data-Raft execution, terminal catalog publication and complete distributed
fault coverage. Startup currently verifies the record's structure, binding,
source EOF, progress and cut, not the entire retained scan prefix. Public UNIQUE
activation remains guarded. Original SQL dispositions remain 483 implemented /
862 unresolved; this work does not qualify another original SQL case or claim a
throughput improvement.

### Bounded read-only physical generation verification

The native worker now advances from completed claim inventory to a separate
physical generation verifier. It never calls the repair pass and interprets its
successful repairs as validation. A primary document-prefix pass verifies the
canonical tuple, covering payload, forward entry, reverse entry and ownership
companion for each member. It also detects reverse-only orphan document
prefixes. Generation-local forward and ownership passes reject extras,
malformed keys, wrong tuples, payload mismatches, orphan companions and
out-of-range entries. Completeness binds to the consumed local source member
count, not the independently routed target-claim count. These counts may differ
on distributed owners.

Repair and verification share the exact schema-bound row-projection kernel.
Immutable historical views and projected plans are reused within each page;
child plans are released before their schema view. Per-row scratch retains
capacity without retaining all decoded rows. Document-prefix successors skip
unrelated index/artifact fanout. Each turn examines at most 128 records and
honors cancellation, deadlines and byte/time budgets. Like construction, the
byte/time budget is soft for one indivisible large row; no new small-key or
small-row restriction is imposed to make verification pass.

AIGV v1 separates a bounded binary continuation (up to the existing 1 MiB key
limit) from its fixed summary. Responses never carry that large cursor. Native
envelope 26 carries the continuation compactly as base64 only in the ordered
effect; replay recomputes actual row/pair contents, counts, cursor and chain
before accepting it. Native authority, progress, transport marker and durable
outbox remain one atomic mutation. Exact replay is inert; structural recovery
checks the retained inventory/source/cut binding. Cancellation cleanup retires
both inventory and generation checkpoints. Metadata decoder floor 44 covers
the progress contract, not authenticated native publication capability.

The LSM fixture covers missing forward/reverse/ownership records, a covering
payload altered with its physical checksum recomputed, malformed forward keys,
reverse-only and ownership-only orphans, and a primary row removed with all
its physical companions. Failures are durable across reopen and do not repair
the injected bytes. Valid data completes all three passes with three-record
pages, survives restart and uses the shared compiled reservation. Actual
normalized ingress qualifies generation replay/idempotence. Maximum-size
cursor codec coverage is explicitly separate from continuation authority: a
substituted cursor decodes but is rejected by physical replay. Allocator-failure
and differing source/target cardinality tests qualify their own limited claims.

`/private/tmp/antfly-generation-verifier-full-debug.log` passed 159/159 Debug
steps, including publication, ACK, leased-owner, C-ABI, runtime-contract,
native-page and all 104 standalone tests. After source-catalog regeneration,
`/private/tmp/antfly-generation-verifier-final-debug.log` passed 56/56 steps
with all 46 runtime-contract and nine native-page tests, including checkpoint
retirement on cancellation. The final no-repair assertions passed 53/53 Debug
steps in `/private/tmp/antfly-generation-verifier-no-repair-debug.log`. These
gates overlap and are not independent SQL coverage. Boundary, license,
original-inventory, generated control-catalog, formatting and diff checks pass.

Physical generation readiness is still unsigned progress, not a metadata
validation ACK. Remaining work includes authenticated proof joining and its
native capability barrier, full prefix re-audit on restore/adoption (including
claim-only orphans), install/release, durable failure completion, missing-root
repair, bounded parallel target fanout, actual data-Raft execution, terminal
catalog publication and complete distributed faults. Public UNIQUE activation
remains guarded. Original dispositions remain 483 implemented / 862 unresolved;
no additional original SQL case or throughput improvement is claimed here.

### Bidirectional claim-inventory coverage

Inventory validation now runs both directions before reporting readiness. The
generation-local inventory pass still checks every companion and accepted
logical contribution. A second pass checks authoritative claim records against
their exact inventory companions. A claim-only orphan fails durably even when
the completed forward count/XOR still matches metadata. Both directional
counts/XORs must match, and the strong chain includes the related physical
claim and companion bytes. This remains physical progress, not a signed
publication ACK.

The established integrity key format on main is routing-first. Consequently,
the reverse pass streams the claim namespace and filters generation identities;
it does not claim generation-local I/O or introduce a legacy-format migration.
Each turn visits at most 128 records and honors byte/time/cancellation limits.
Foreign claim values are not decoded or hashed, and inverse-reference/job
namespaces are excluded. One indivisible existing foreign record may exceed
the requested byte quantum, but remains bounded by the existing record limit
plus its key. The audit does not impose a smaller claim storage limit. Overall
reverse work depends on the claim namespace, so
this is not a throughput improvement or constant-time admission claim.

Replay verifies the actual bounded cursor interval and related membership,
not an exact count of unrelated records. Unrelated retired claims may have
been deleted, including the interval endpoint. Additional current records
cannot make replay exceed the original bounded visit allowance, and missing
or changed related members fail verification. EOF is established from the
actual namespace, never from a supplied completion flag. Ordinary private
claim insertion remains frozen; exact duplicates remain permitted.

AICV v2, native envelope 27 and coordinated decoder floor 45 cover the private
progress change. The LSM fixture injects a claim-only orphan after forward
completion, verifies durable failure across reopen, restores its controlled
fixture, restarts during the reverse pass, deletes an unrelated claim between
preparation and actual normalized replay, and qualifies idempotent transport
application. These are explicit corruption/GC fixtures, not authorization for
ordinary writes through the source fence.

The focused gate in `/private/tmp/antfly-claim-reverse-debug-v2.log` passed
72/72 Debug steps. The final-source aggregate gate in
`/private/tmp/antfly-claim-reverse-full-debug.log` passed 159/159 Debug steps,
including all nine native-page, 48 publication, four ACK, three leased-owner,
six C-ABI, 46 runtime-contract and 104 standalone tests. Counts overlap and
are not independent SQL coverage. Boundary, license, original inventory,
control-catalog, formatting and diff checks passed.

Still required are authenticated proof joining and native capability barriers,
full scanned-prefix re-audit on restore/adoption, install/release, durable
failure completion, missing-root repair, bounded parallel target fanout, actual
data-Raft execution, terminal catalog publication and complete distributed
fault coverage. Public UNIQUE activation stays guarded. Original dispositions
remain 483 implemented / 862 unresolved; no original SQL completion is claimed.

### Root-local physical re-audit kernels

Identity-preserving restore must not rewind the imported canonical inventory or
generation checkpoints. Ordered native effects bind to their prior-record
digests; deleting or resetting those records would diverge from the source's
journal. Physical re-audit therefore has separate root-local progress, which
must never be imported as canonical job state or submitted as a native effect.

The inventory kernel begins at the start even when canonical progress already
says ready. It reuses both bounded claim/inventory passes, binds its progress to
the physical root incarnation, expected logical target and exact canonical
record digest, and rejects a changed root or seal rather than adopting the old
scanned prefix. The generation kernel requires the same root's completed
inventory audit and reuses all three existing physical verification passes.
Its owned binary cursor retains the existing legal key limit; summaries remain
bounded. Generation progress additionally binds to the canonical generation
record and the inventory audit's final digest. Cancellation and normal page
budgets apply to both kernels, including calls with terminal checkpoints.

Neither kernel writes canonical records, advances native authority, repairs
physical bytes, signs an ACK or grants publication permission. Progress passed
to these internal functions must belong to the trusted root-local owner; the
structures are not an authenticated external checkpoint interface. The caller
must retain the physical root's lease and existing source fence. Durable local
checkpoint storage/authentication and actual restore/adoption enforcement are
still required; this is not a claim that restart or restore integration is done.

The real LSM fixture recomputes inventory and generation coverage behind
canonical ready records. It checks changed-root and changed-seal rejection,
cancellation, terminal repeat calls, and bounded continuation across short read
transactions. Injected claim-only and missing-forward corruption must fail the
local audits without changing canonical progress or the native sequence and
without repairing the corrupt bytes. These are controlled physical-corruption
fixtures, not authorization for writes through a frozen source.

The aggregate gate in
`/private/tmp/antfly-root-local-audits-full-debug.log` passed 159/159 Debug
steps, including nine native-page, 48 publication, four ACK, three leased-owner,
six C-ABI, 46 runtime-contract and 104 standalone tests. These gates overlap
and do not represent independent SQL coverage. Boundary, license, original
inventory, generated control-catalog, formatting and diff checks passed. No
canonical format, native envelope or capability-floor change is introduced.

Original dispositions remain 483 implemented / 862 unresolved. These internal
tests do not qualify more original SQL cases. Public UNIQUE activation remains
guarded until proof joining, capability barriers, restore/adoption enforcement,
install/release, durable terminal completion and distributed fault coverage
are integrated.

### Durable physical-root audit checkpoints

The root-local inventory/generation kernels now have a signed checkpoint
repository and a bounded DB entry point. `stepNativeIndexOwnerAudit` validates
the actual native job, table and frozen target, uses the relational preparation
budget, advances at most one audit page, and persists its local result without
changing canonical progress, primary rows or native authority. This is local
verification, not a replicated mutation; followers are not excluded by the
replication write gate. Query-readonly DB opens remain readonly.

The receiving root owner supplies the physical root directory/incarnation and
retains its lifecycle lease. Root signing keys must already be provisioned;
audit execution does not mint identities or wait on their creation locks.
Checkpoint signatures bind the complete owner identity, frozen expectation,
root incarnation, canonical seals, inventory audit and binary generation
cursor. The signature is checked before parsing the potentially large cursor.
The private AFRIA v1 framing is root-local only, not a native effect or an
authenticated metadata publication receipt.

Files live in `.relational-index-audits` outside the primary snapshot. There is
one checkpoint slot and stable lock per DB namespace, matching the existing
single canonical native-job slot. Different namespaces can share a logical
job ID without sharing progress or a lock. A valid signed checkpoint from a
different owner starts no local progress: the new job must rescan from the
beginning before atomically replacing the same slot. Corrupt signatures and
changed same-owner seals fail closed. Attempts do not accumulate checkpoint
files. Namespace/root retirement still belongs to the physical owner lifecycle.

Directory/file opens reject symlinks and checkpoint files are private regular
files. Operations use the retained directory handle. Per-namespace advisory
locks wait through cancellation-aware `std.Io` sleeps, not thread primitives or
the apply lock. Writes use file sync, atomic rename and directory sync; first
publication also syncs the containing root to close concurrent mkdir races.
An abandoned exact temporary file is removed only under its namespace lock.
Unchanged terminal saves avoid rewriting and syncing the checkpoint.

The LSM fixture exercises DB restart during inventory and generation audits,
unchanged canonical bytes/native sequence, independent namespace locks, lock
wait cancellation, owner replacement without prefix adoption, abandoned-temp
recovery, signature corruption and rejection of an intact checkpoint copied
from another root. These do not qualify metadata adoption or distributed Raft
faults. Receiving-worker/C-ABI integration, authenticated proof joining,
capability barriers and actual restore/adoption/ACK enforcement remain
unfinished. Public UNIQUE activation stays guarded; original SQL dispositions
remain 483 implemented / 862 unresolved.

`/private/tmp/antfly-root-audit-checkpoint-final-debug.log` passed 159/159
Debug steps, including nine native-page, 48 publication, four ACK, three
leased-owner, six C-ABI, 46 runtime-contract and 104 standalone tests. The
final directory-open adjustment passed 53/53 Debug steps with all nine
native-page tests in `/private/tmp/antfly-root-audit-checkpoint-sync-debug.log`.
These gates overlap and do not add original SQL coverage. Boundary, license,
inventory, generated control-catalog, formatting and diff checks pass. The
sync-capable directory-open flag follows the Linux std.Io implementation;
the executed gates here are macOS Debug, not Linux runtime qualification.

### Receiving-worker audit ingress and independent physical incarnation

Native validation now advances root-local auditing through the existing leased
compiled-owner/C-ABI path. The authenticated receiver supplies a bounded
signing-root context from its configured root and independently obtained
metadata signing view. Remote control requests cannot select filesystem paths.
The private ABI is 83; coordinated decoder floor 46 covers the new unsigned
`audit` progress reply. It is not native publication/install capability.

A newly committed canonical validation page consumes its whole turn. Only an
inert, mirror-debt-free observation of canonical generation readiness may
advance one root-local audit page. The worker no longer retires validation
work on canonical readiness: it waits through partial root-local progress and
checks both inventory and generation completion/failure. Root drift, corrupt
local checkpoints and missing signing identity report root recovery required;
malformed incoming context reports invalid argument. No metadata validation
ACK is fabricated from the new reply.

Signing authority and physical DB identity are separate. One provisioned root
can own many DB namespaces/generations. AFRIA v2 signs the authority's root
incarnation together with the actual pinned DB's physical incarnation, complete
owner binding, frozen target, canonical seals and audit cursors. The latter
identity comes from the compiled DB, never a request. A valid old checkpoint
for a replaced DB starts no progress even when its canonical records and
parent signing key are unchanged. It must be re-audited from scratch. The
one-slot-per-namespace layout still bounds attempt churn. No AFRIA v1 legacy
reader is added for this private development format.

The native response retains local source cardinality separately from target
claim cardinality; distributed ownership does not require them to be equal.
Scope JSON accommodates worst-case escaping of the platform's admitted path
scalar size and rejects oversized borrowed lengths before pointer access.
The escaped-path unit case qualifies the codec, not filesystem path creation.

The real C-ABI LSM fixture consumes an actual EOF source with synthetic metadata
acceptance, validates one target claim with zero local source rows, advances
the signed local audit, rejects a wrong signing root and verifies no native
sequence advancement during audit pages. It then replaces the closed DB's
durable physical identity while retaining the primary/canonical bytes and
parent key, reopens, and proves the old ready checkpoint cannot skip the fresh
audit. This is controlled identity replacement, not staged snapshot restore
or distributed authorization qualification. The scheduling fixture waits
through partial audit progress before retiring its volatile owner.

Authenticated proof joining, its separate native capability barrier, actual
restore/adoption/ACK enforcement, installation/release, durable terminal failure
completion, missing-root repair, actual data-Raft execution, terminal catalog
publication and complete distributed faults remain unfinished. Public UNIQUE
activation remains guarded. Original dispositions remain 483 implemented /
862 unresolved; these internal regressions add no original SQL coverage.

The final aggregate gate in
`/private/tmp/antfly-root-audit-ingress-final-debug.log` passed 159/159 Debug
steps: ten native-page, 48 publication, four ACK, three leased-owner, six C-ABI,
46 runtime-contract and 104 standalone tests, plus their domain/decoder
prerequisites. Counts overlap and are not independent SQL coverage. Boundary,
license, original inventory, generated control-catalog, formatting and diff
checks passed. Runtime qualification here is macOS Debug; Linux's directory
open/sync requirement is additionally checked against the std.Io implementation.

### Authenticated physical-audit witness and frozen-ledger join

A complete native audit can now be signed using the existing post-commit root
decoration path. Partial/failed audits are not signable. The compact witness
contains the complete outcome digest and applying-root proof; the outcome is
carried only once in its enclosing reply. Its domain-separated digest binds
the job/target, source cardinality, signing authority, physical incarnation,
canonical validation cut, canonical generation seal and root-local checkpoint.
The leased generation is retained from the actual compiled call, not reread
after its lease is released. Signing failure preserves completed physical
progress and exposes the failure without fabricating an ACK. Workers retry
complete but unsigned results rather than retiring them as authenticated work.

The physical witness identity deliberately changes across DB replacement. It
is not a logical publication receipt digest: eventual logical re-attestation
must preserve the same committed logical stage while refreshing its physical
root witness. No validated/install/release native effect is added here.

Metadata verification joins the signature to the complete frozen source/target
ledger and current ownership in one transaction's read set. The projection
returns target expectation, consumed local source rows and source cut together
without decoding partition bodies or scanning a cohort. Target counts are not
substituted for source counts. Verification compares the exact frozen target,
the independent source count, a canonical cut strictly after source consumption,
active job/lifecycle/catalog/range authority, enrolled key/current reporter,
standalone writer binding and the current runtime root generation/native cut.
This method stages no writes. The eventual accepting metadata transaction must
call it itself; a successful service preflight is not an authorization token.

Private ABI 84 and coordinated decoder floor 47 cover the new bounded witness
and canonical-cut projection. These remain audit capabilities, not full native
validation/installation/publication activation. AFRIA v2 is unchanged: the
canonical cut already resides in the stored generation progress.

The metadata fixture accepts the correctly signed witness and rejects unsigned
physical changes, validly re-signed target/count/cut mismatches and stale
reporter/root-generation evidence. It checks unchanged head/owner bytes and
absence of a validated receipt. Synthetic physical evidence qualifies these
metadata reads, not a physical scan. Existing real LSM/C-ABI fixtures separately
qualify physical production. Scheduling tests refuse partial signing and check
that signing failure retains the exact completed outcome before a successful
signature is produced.

Still unfinished are the durable logical validation effect/outbox and atomic
accepting ACK integration, separate full native capability barrier, actual
restore/adoption enforcement, install/release, durable terminal failure,
missing-root repair, actual data-Raft execution, terminal catalog publication
and complete distributed faults. Public UNIQUE activation remains guarded.
Original dispositions remain 483 implemented / 862 unresolved.

The focused metadata gate in
`/private/tmp/antfly-signed-audit-metadata-debug-v3.log` passed 60/60 Debug
steps. The final aggregate gate in
`/private/tmp/antfly-signed-audit-full-debug.log` passed 159/159 Debug steps,
including ten native-page, 48 publication, four ACK, three leased-owner,
six C-ABI, 46 runtime-contract and 104 standalone tests. Counts overlap and
are not original SQL coverage. Boundary, license, inventory, generated
control-catalog, formatting and diff checks passed. These are macOS Debug
executions; no additional distributed restore or Linux runtime qualification
is claimed.

### Durable logical validation seal, independent of physical audit witnesses

The native owner now has a bounded validation-seal effect. Its logical evidence
commits the reserved binding, frozen target expectation, canonical inventory
and generation digests, source-member count and canonical generation position.
It deliberately excludes signing identity, physical incarnation and root-local
checkpoint bytes. Identity-preserving restore must preserve this logical receipt
while obtaining a fresh physical audit for the replacement root.

Seal staging rechecks complete inventory and generation records in the same
transaction as the native authority cut. It uses the existing native effect
transaction for standby outbox and replay-marker atomicity. Retries recover the
original cut, and publication receipts are populated only after commit and
successful outbox flushing. One reusable namespace slot bounds retained state;
cancellation cleanup removes it. Recovery checks its canonical dependencies,
including rejecting a seal whose generation record disappeared. The cut does
no table scans; its work is bounded by fixed-size validation records. No measured
throughput gain is claimed.

Ordinary catalog metadata extras now reserve the complete colon-delimited
private owner-key family, not an enumerated set of known slots. This covers the
new seal, existing inventory/generation validation records and future receipt
slots without incorrectly reserving a merely similarly named sibling key.

The private canonical envelope is version 28 and kernel ABI is 85. The metadata
decoder floor remains 47: it still does not advertise complete native index
activation. No legacy reader is added for these unpublished private formats.

Still unfinished are C-ABI/leased-worker seal production and signing, atomic
accepting ACK integration joining the stable receipt to a fresh physical audit,
the separate full native capability barrier, restore/adoption enforcement,
install/release, durable terminal failure, missing-root repair, actual data-Raft
execution, terminal catalog publication and complete distributed faults.
Public UNIQUE activation remains guarded. Original dispositions remain
483 implemented / 862 unresolved; this milestone adds no original SQL coverage.

Qualification: `/private/tmp/antfly-validation-seal-full-debug-v2.log` passed
159/159 Debug steps, including ten native-page, six C-ABI, 48 publication,
four ACK, three leased-owner, 46 runtime-contract and 104 standalone tests.
The real LSM fixture covers incomplete-generation rejection, cancellation,
allocation-failure/abort cleanup, forged commitments, fresh ordered replay,
duplicate receipt recovery, close/reopen, missing canonical generation,
corrupt seal bytes, private-key write/delete guards and cancellation cleanup.
It preserves the existing candidate-cache assertion before the added reopen.
Boundary, license, inventory, generated control-catalog, formatting and diff
checks passed. These are macOS Debug results, not original SQL case credit.
Seal-specific outbox-failure, staged-restore and distributed failover injection
remain unqualified; the private seal does not enable a public producer.

### Paired native validation production and captured-root signing

The existing private native validation route now returns a paired result after
its root-local inventory and generation audits complete. The physical summary
includes the canonical inventory digest, allowing a complete logical seal
command to be reconstructed without decoding row data. Before committing,
the DB checks the actual physical incarnation and rechecks the audit's exact
canonical commitments inside the native transaction. Partial audits remain
receipt-free; completed seals retain their original cut on duplicate requests.

The leased provider bounds response bytes before parsing and retains the actual
applying generation. Host signing produces a logical receipt signature and a
separate physical-audit signature with that same root context. Both decorations
are installed only after both succeed. Responses reject mixed root identities,
writer bindings, reporter incarnations or generations. Replication-pending or
post-commit recovery outcomes retain the cut without a signing/publication
certificate. The worker understands paired progress but retires only volatile
physical work; the metadata head remains building until atomic acceptance is
implemented.

Private ABI 86 and coordinated decoder floor 48 cover paired replies and the
complete inventory commitment in the physical witness. Its digest domain is
`antfly/private-index-complete-physical-audit/v2`; the durable logical seal and
canonical envelope 28 are unchanged. AFRIA v2 already retained the canonical
inventory digest, so its checkpoint encoding is unchanged. Decoder floor 48
still does not advertise complete native install/release activation.

Still unfinished are atomic accepting ACK integration and retained validation
evidence, latest-stage re-attestation, the full native capability barrier,
restore/adoption enforcement, install/release, durable terminal failure,
missing-root repair, actual data-Raft execution, terminal catalog publication
and complete distributed faults. Public UNIQUE activation remains guarded.
Original dispositions remain 483 implemented / 862 unresolved; paired private
production/signing does not qualify an original SQL case by itself.

Qualification: `/private/tmp/antfly-validation-pair-full-debug-v2.log` passed
159/159 Debug steps, including six C-ABI, ten native-page, 48 publication,
four ACK, three leased-owner, 46 runtime-contract and 104 standalone tests.
The real C-ABI fixture retains different source/target cardinalities (zero
local source members, one target claim), rejects changed canonical/physical
audit scopes before sealing, recovers duplicate receipts, and forces a fresh
audit after replacing the closed DB's physical identity. The renewed witness
changes while the logical receipt and cut remain identical. This qualifies a
fenced unpublished generation and privileged physical-identity replacement,
not staged restore or published-table adoption.

Signing fixtures reject a validly re-signed logical proof paired with another
generation's physical proof, retain completed progress on signing failure and
refuse to sign replication-pending receipts. Scheduler OOM enumeration uses the
existing stable backing-allocator wrapper to eliminate remap-dependent counts;
all failpoints and leak assertions remain exercised. Boundary, license,
inventory, generated control-catalog, formatting and diff checks passed.
These are macOS Debug results; no additional original SQL or distributed
failover/restore qualification is claimed.

### Atomic native validation acceptance and retained physical evidence

Native validation ACKs now require both the stable signed logical receipt and
the completed signed physical audit. Accepting apply joins them to the current
native writer/reporter/root generation, indexed runtime seal cut, live table
and range authority, and frozen source/target completion in its own transaction.
Preflight success is not an authorization token. The head, owner and retained
proof are staged atomically through the existing metadata command/outbox path.

Native validation uses a bounded AIVP v1 stage-proof record containing the fixed
logical receipt and canonical physical witness. It occupies the existing proof
key, so snapshot registration, terminal replacement and cleanup do not acquire
a parallel journal. Bare native validation records are rejected; other actions
and Raft proofs keep their existing fixed codec. Historical recovery verifies
the witness and its frozen provenance without granting live execution or root
adoption. Exact ACK status includes the retained physical digest, and a different
witness cannot replace evidence under the same root grant. The metadata decoder
floor is 49, not a complete native activation capability.

ACK status stays pending while the metadata standby outbox is unflushed, and
owner execution permission is withheld during that window. This prevents a
physical stage from consuming an unmirrored metadata phase. Checks are bounded
point reads plus bounded proof decoding; no table/cohort scan is introduced in
the hot applying transaction. Canonical record verification does not repeat
signature verification solely to reconstruct canonical bytes.

Still unfinished are automatic worker submission/recovery of validation ACKs,
full latest-stage re-attestation and native capability barriers, restore/adoption
enforcement beyond retained metadata verification, install/release, durable
terminal failure, missing-root repair, actual data-Raft execution, terminal
catalog publication and complete distributed faults. Public UNIQUE activation
remains guarded. Original dispositions remain 483 implemented / 862 unresolved.

Qualification: `/private/tmp/antfly-validation-ack-full-debug.log` passed
159/159 Debug steps. After tightening the same-root-grant comparison to use
identity/writer/reporter/generation rather than signature bytes, the final
metadata run `/private/tmp/antfly-validation-ack-debug-v3.log` passed 69/69
steps, including all 48 publication and four ACK tests. The durable completion
fixture accepts a joined native validation ACK, requires the indexed runtime
to observe its later seal cut, rejects validly re-signed frozen-scope changes,
and injects failure before/after each of the three staged writes. Aborts leave
head/owner/proof unchanged. Duplicate/lost-reply status and cold reopen preserve
the exact witness; substituted, corrupt and bare witnesses are rejected.

A synthetic pending metadata outbox keeps status pending and withholds owner
execution. Physical witnesses in this metadata fixture are synthetic; this
qualifies the accepting read/write set, not distributed physical production or
standby transport failure. The independent real LSM/C-ABI gates remain green.
Boundary, license, inventory, generated control-catalog, formatting and diff
checks passed. These are macOS Debug results, not original SQL case credit or
complete native publication/restore qualification.

### Automatic native validation ACK scheduling and outcome recovery

The validation receiver carries the metadata revision observed before its
physical turn as an explicit CAS hint. A paired signed reply can be converted
to an ACK only after checking both proofs, their captured applying-root context,
request/owner identity and a valid revision. The revision is not a capability;
accepting metadata still repeats the authority/provenance checks atomically.
Decoder floor 50 covers this private reply field and worker behavior. Native
kernel ABI 86 and canonical envelope 28 are unchanged.

The bounded worker now owns the receipt/witness pair and submits it through its
existing ACK/status stages instead of retiring on physical completion. Unknown
commit outcomes poll exact status. Pending metadata replication stays in status
polling without resubmission. An unaccepted receipt or stale revision/root
refreshes permission and recovers the existing durable seal. Physical-only and
unsigned replies cannot complete validation. Metadata replication backpressure
retains control/validation lanes with bounded retry, is preserved across the
private HTTP error boundary, and is a retryable standalone reconciliation error.

The same recovery policy also removes indefinite stale-revision resubmission
from earlier fence/reservation ACK stages. Successful validation completes only
after metadata acknowledgement; cancellation/expiry still releases volatile
slots. Discovery and per-owner work remain bounded; no
cohort/table scan is added to a physical or metadata applying transaction.

Still unfinished are full latest-stage re-attestation and native capability
barriers, restore/adoption enforcement beyond retained metadata verification,
install/release, durable terminal failure, missing-root repair, actual data-Raft
execution, terminal catalog publication and complete distributed fault tests.
Public UNIQUE activation remains guarded. Original dispositions remain
483 implemented / 862 unresolved; this protocol milestone alone grants no
original SQL case credit.

Qualification: `/private/tmp/antfly-validation-worker-full-debug.log` passed
159/159 Debug steps, including 48 publication, four ACK, six C-ABI, ten native
page, three leased-owner, 46 runtime-contract and 104 standalone tests. Component
scheduler cases cover pre-commit loss, lost post-commit replies, three pending
mirror observations, a revision conflict, physical-only replies, and scheduler
restart before ACK submission or after an uncertain committed ACK. They assert
one seal effect and one metadata acceptance; only pre-commit/revision rejection
causes a second submission. Restart-after-commit must observe fresh metadata
acknowledgement rather than merely count the earlier commit. Completion checks
inspect every resident owner slot, not slot zero alone.

Existing exhaustive OOM/leak checks exercise the baseline and source-completion
failure variants through the stable backing-allocator wrapper. Initial logical
fence/reservation receipts receive genuine fixture signatures rather than
weakening ACK-status attestation checks. These scheduler observations are
synthetic; real native durability and atomic metadata acceptance retain their
independent LSM/C-ABI and metadata-store qualification. They are not complete
distributed restart/restore fault tests. Boundary, license, inventory, generated
control-catalog, formatting and diff checks passed. Results are macOS Debug;
no original SQL disposition or measured throughput change is claimed.

### Reserved-generation schema installation preparation

The shared core schema preparer now has an explicit reserved-index path. It
checks the durable reserved job/fence and immutable input/catalog identities,
requires the exact candidate public validator (including any supplied versioned
copy), and prepares the existing physical and integrity generations rather than
allocating successors. The physical write plan retains the actual candidate
schema epoch and its validator. Warm owners may still hold their unchanged
pre-reservation write snapshot; reopened owners may bind that same plan to the
burned allocation head. Both are checked against the captured binding, while
the durable reservation is independently required.

Compilation, owned catalog bytes and runtime objects are prepared outside
serialized apply. Preparation neither selects a durable candidate head nor
publishes a runtime snapshot. The prepared schema retains its fixed job binding;
ordinary schema commit, child-generation commit and rehydration explicitly
reject it. This closes the generation-allocation gap in cold installation
preparation, not the installation protocol itself.

Still required: the durable metadata publication decision and an authenticated
native install participant that stages schema, both catalogs, activation,
installed job/cut, replay marker and outbox in the same store transaction, then
non-fallibly publishes the prepared runtime objects. Installation must retain
the source fence until the distinct release effect. Existing public activation
guards remain in place. No decoder floor, kernel ABI, canonical envelope or
original SQL disposition changes in this preparation milestone.

Qualification: `/private/tmp/antfly-reserved-schema-final-debug.log` passed
59/59 Debug build steps: ten native lifecycle/page tests, 14 candidate tests,
15 catalog tests and 46 runtime-contract tests. LSM preparation checks the exact
reserved physical ID, integrity CAS bytes, validator-bearing schema epoch,
unchanged durable catalog/clock and rejection through all three ordinary schema
commit paths. Missing/mismatched active validators, inconsistent versioned
validators and stale job bindings are rejected. Reopened LSM physical
preparation additionally runs exhaustive allocation-failure/leak checks on the
owned catalog/plan transfer and proves that neither durable nor runtime heads
are selected. This is preparation/ownership evidence, not an installed native
effect, a distributed publication fault gate or original-case credit. Boundary,
license, inventory, generated control-catalog, formatting and diff checks passed.

### Schema predecessor admission in the atomic publication transaction

The shared schema writer supports an optional `validateBefore` participant
phase inside its write transaction, before any schema history, metadata put or
metadata delete. The existing post-write `stage` phase still joins dependent
effects to that transaction; runtime objects are published only after commit.
Both value and pointer participants work, and existing stage-only participants
retain their behavior. Exact read-only rehydration still compares effects
without granting new mutation admission.

Core lifecycle admission now checks the predecessor before mutations, and
retains the post-mutation guard against extras introducing a new incompatible
state. Previously the guard ran only after metadata changes, so deleting an
active topology fence in schema extras could make ordinary DDL inspect an
unfenced proposed state rather than the fenced durable predecessor. A real LSM
reserved-index regression now rejects that attempted deletion and preserves
the reserved catalogs, schema and native clock.

`/private/tmp/antfly-schema-predecessor-debug.log` passed 59/59 Debug build steps
(ten native lifecycle/page, 14 candidate, 15 catalog and 46 runtime-contract
tests). Memory and LSM transaction fixtures observe old schema/catalog/admission
in the pre-write phase and new schema/catalog plus an outbox in the stage phase.
Failures in either phase preserve the old schema, historical-version absence,
catalog, admission key and runtime snapshot, with no receipt, replay marker or
outbox committed. Successful commit persists the whole cut before explicit
runtime publication. Those fixture receipt/marker values are synthetic: this
is shared atomic-boundary qualification, not native install execution or a
metadata publication decision. Boundary, license, inventory, generated
control-catalog, formatting and diff checks passed. Original dispositions remain
483 implemented / 862 unresolved; native install/release and full public index
activation remain unfinished.

### Atomic metadata index publication decision

`decide_relational_index_publication` selects only the immutable successor
already admitted for an exact table/job/revision. Its fixed-size command carries
no replacement schema, owner list or synthetic receipt. Decoder floor 51 gates
the command; an irreversible installing/releasing/published head also retains
that floor on snapshot export/import. Earlier building heads retain floor 50.
Neither floor advertises full native installation or public producer readiness.

Preparation checks the accepted all-owner validation counter, adopted namespace
writer, exact lifecycle lock, pending source, current predecessor descriptor,
logical binding/incarnation, pending index name and policy/schema-reader
admission. It rejects an unmirrored metadata predecessor. Accepted validation
ACKs remain durable logical prepare facts: this decision does not attest a new
physical root or authorize root adoption. Install receivers must independently
establish their current physical authority and exact retained validation cut.

The shared namespace writer commits the successor descriptor, head transition
and removal of the pending source atomically, consuming the reserved relation
and constraint names in that same transaction. The lifecycle lock remains;
installed/released counters are not manufactured. A stale preflight loses its
CAS before writes; an already committed decision is recovered without replaying
DDL or advancing the namespace/source cut. No owner-cohort or primary-row scan
is introduced. Decision input projection owns only the table/guard fields it
needs, skipping the base64 predecessor catalog rather than allocating another
schema-sized catalog copy under metadata apply.

Automatic decision dispatch, authenticated native install/release, coupled
activation and full public SQL qualification remain unfinished. Public UNIQUE
creation stays guarded. This metadata transition alone changes no original SQL
dispositions (483 implemented / 862 unresolved).

Qualification: `/private/tmp/antfly-index-decision-final-debug.log` passed
159/159 macOS Debug build steps, including 50 publication, four ACK, six C-ABI,
ten native page/lifecycle, three leased-owner, 46 runtime-contract and 104
standalone tests. The decision fixture starts from actual durable accepted
paired validation evidence (its physical audit values are synthetic), exercises
committed Raft replay and standalone commit, and verifies exact namespace/name
consumption, retained lifecycle exclusion and zero fabricated install/release
ACKs. Aborting after full namespace staging preserves the byte-identical prior
snapshot. Duplicate replay at a later Raft index preserves the source/root cut;
lost-reply recovery and reopen preserve the one decision revision. Incomplete
validation, stale job/revision, pending replication, truncated/trailing wire
bytes and a lowered snapshot decoder floor are rejected. Activation downgrade
uses the existing no-op semantics and leaves floor 51 intact.

The decision projection owns borrowed strings across later reads and retains
exhaustive allocation-failure/leak checks. A synthetic 1 MiB omitted catalog
field projects within a 64 KiB fixed allocator; full-input decoding under that
same cap returns OutOfMemory. This measures decoder allocation shape, not
production query/DDL latency or an admitted synthetic catalog. The first broad
run's three TLS failures were sandbox-denied loopback binds; the final run had
loopback permission and passed without skips. Boundary, license, inventory,
generated control-catalog, formatting and diff checks passed. No original SQL
case credit or complete native/public activation is claimed.

### Seal-bound incremental constraint activation preparation

The reserved schema preparation now owns an activation delta only after the
native validation seal exists and matches complete canonical inventory and
generation evidence. The seal's generation must be the exact UNIQUE addition;
the predecessor, reservation and candidate catalog identities are independently
checked. This does not authorize native installation or physical-root adoption.

The delta preserves older constraints' exact activation state, phase, cursor,
row count and diagnostics when their generation set and owner still match.
Enforced FK/CHECK/UNIQUE coverage is not reset for an unrelated index addition,
avoiding a full-table revalidation after a successful build. Invalid or partial
older coverage is never promoted by the new index's proof. Missing/mismatched
older coverage remains validating; only a predecessor with no active
constraints needs no older proof. Ownership and schema/generation identities
are rebound to the candidate while existing semantic bindings remain unchanged.

All activation bytes and CAS ownership are prepared off-lock. The native
installer must call the predecessor check inside its schema transaction before
catalog puts, stage the exact candidate catalogs and activation, then commit
the installed job/cut, replay marker and outbox in the same transaction without
releasing the fence. Activation staging checks the exact seal, active job,
candidate catalog, prior activation bytes and current owned range. Ordinary
schema publication still rejects reserved preparations, including sealed ones;
an unsealed preparation owns no candidate activation.

The authenticated native install participant, installed-state recovery and
wire/replay effect, install/release receiver/scheduler, full capability/adoption
barriers and public SQL qualification remain unfinished. This is the
seal-bound activation component, not a committed native installation or new
original-case credit. Dispositions remain 483 implemented / 862 unresolved.

Qualification: `/private/tmp/antfly-index-activation-final-debug-v2.log` passed
159/159 macOS Debug build steps, including 11 native page/lifecycle, 50
publication, four ACK, six C-ABI, three leased-owner, 46 runtime-contract and
104 standalone tests. The new data-policy regression preserves enforced,
invalid and partial older FK/CHECK/UNIQUE coverage, with conservative reset for
missing/mismatched ownership. The LSM fixture uses its real canonical native
seal after replay and reopen, checks off-lock activation/schema preparation,
and exhaustively exercises allocation-failure/leak cleanup. It rejects missing
seals, stale activation CAS, the wrong catalog and a moved owned range.

Component transaction tests stage schema, both catalogs and activation with
the fence still held, inject failures before/after the activation write, and
abort. The original reserved catalogs/schema, runtime epoch and native clock
remain unchanged. They deliberately commit no installed job/receipt, outbox or
release and are not a distributed installation qualification. A sealed reserved
preparation still cannot use ordinary schema commit; an unsealed one owns no
activation delta. Boundary, license, inventory, generated control-catalog,
formatting and diff checks passed. No throughput measurement, full native
activation or original SQL case credit is claimed.

### Seal-bound installed-state transaction component

The private owner job now has an explicit installed phase (AIOJ v4; kernel
ABI 87). An AIIS v1 installed cut binds the original canonical validation
seal, metadata decision identity, exact native successor schema and exact
prepared activation bytes. The metadata decision digest is an identity, NOT
authentication: the future receiving owner must independently authorize it.
Staging advances the native clock and writes the job/installed record only
after checking the staged successor catalogs, activation ownership and seal.
Exact retries reuse the original cut; old build/reservation/cancellation
entry points cannot reinterpret an installed job as an unfinished build.

Installed structural recovery first verifies the real successor and installed
cut, then checks frozen pre-install proofs through a read-only historical
metadata view. The immutable predecessor schema and reserved catalog must
match the admitted identities; all physical records, namespace/range, clocks
and canonical validation records remain real store reads. Physical catalog
publication retains the one exact job-bound reserved definition blob needed
for this verification instead of deleting it with the ordinary old head.
Terminal release must retire that retained blob; this is not indefinite
catalog-history retention or physical-root adoption permission.

This component neither commits a schema nor publishes resident runtime
objects, authenticates a decision, stages a replication marker/outbox or
releases a fence. The shared Core/DB install transaction, native install
wire/replay effect and receiver, installed ACK, automatic install/release
scheduling, capability/adoption barriers and full distributed fault coverage
remain unfinished. Structural verification in a staged LSM transaction is
not a committed cold-reopen or distributed-install qualification. Original
dispositions remain 483 implemented / 862 unresolved.

Qualification: `/private/tmp/antfly-installed-cut-final-debug-v2.log` passed
159/159 Debug build steps, including the native page/lifecycle, candidate,
publication, ACK, C-ABI, leased-owner, runtime-contract and standalone suites.
The real LSM fixture stages installation without committing, verifies the
installed state and predecessor proofs, checks exact retry/wrong-cut/missing
record handling, exhaustively injects allocation failures, and injects errors
before/after each native clock, job and installed-record write. Every attempted
transaction aborts; the original reserved schema, catalogs, clock and runtime
remain unchanged. Boundary, license, formatting, inventory and generated
control-catalog checks passed. No committed install, throughput result, public
activation or new original-case credit is claimed.

### Shared-schema native installation and replay

Private installation now uses the ordinary shared schema writer, not a second
schema/catalog transaction. Off-lock preparation owns the exact manifest JSON,
active/versioned public schema writes, native schema epoch, existing physical
generations and seal-bound activation. Commit rechecks the reserved predecessor
before writes, then stages native schema history, both catalogs, the durable
table catalog, activation and installed cut in one transaction. The DB owner's
participant stages its replay marker or durable replication outbox in that
same transaction. Resident schema/index publication follows successful commit;
the exact source fence remains held.

The public validator bytes are independently bound to the admitted manifest,
including its immutable versioned copy. A missing versioned copy, substituted
JSON or duplicate public-schema key cannot change semantics between preparation
and commit. This binding is prepared off-lock and does not compile a schema or
scan rows under apply. Failure in the post-install transaction participant
aborts the schema, catalogs, activation, installed cut and participant writes
together, without consuming the prepared publication or changing the live epoch.

Native effect envelope 29 and kernel ABI 88 add installation replay. Already
installed preparation avoids schema compilation/publication; exact retries
verify the current installed successor and original cut. A repeated effect at
a later transport LSN updates only the replay marker, not native time or the
schema generation. Pending outbox publication withholds the logical installed
receipt until flush succeeds. These are internal storage/replay mechanisms;
the command's metadata decision digest is still an identity, not permission.

Independent metadata/root authorization and its native receiver/C-ABI bridge,
installed ACK delivery, release/retained-blob retirement, automatic scheduling,
full capability/adoption barriers and distributed fault qualification remain
unfinished. No public SQL producer is enabled and no original-case credit is
claimed; dispositions remain 483 implemented / 862 unresolved.

Qualification: `/private/tmp/antfly-index-install-replay-final-debug-v2.log`
passed 159/159 Debug build steps, including 11 native page/lifecycle, 50
publication, four ACK, six C-ABI, three leased-owner, 46 runtime-contract,
104 standalone and 15 domain tests. The isolated real-LSM fixture uses a
synthetic metadata decision and copies logical KV state, not signing identity
or audit files; it does not qualify physical-root adoption. It proves callback
failure rollback, manifest/public-history binding, pinned old-schema lifetime,
actual install-frame replay at LSN 5, cold reopen, local installed receipt
retry, marker-only duplicate replay at LSN 6, and wrong-cut rejection without
advancing that marker. Native time, schema generation and the source fence
remain stable on retries. Installed row reads preserve predecessor values.

Boundary, license, inventory, formatting, diff and generated control-catalog
checks passed. The static checker initially stalled in Python's macOS timezone
notification startup before running repository code; a stack sample established
the cause, and the checks passed with host access. Existing outbox regressions
passed, but install-specific primary outbox publication/fault coverage and the
distributed authorization/ACK/release workflow still require qualification.
No throughput result, public activation or new original-case credit is claimed.

### Native installation primary outbox fault qualification

The existing native test publisher and physical fixture seeding are shared by
control and install tests. The real-LSM installation fixture now commits through
the primary DB adapter with synchronous durable mirroring, deliberately fails
publication, and verifies the schema/catalog/installed cut remain committed
with exactly one durable outbox obligation and no installed receipt. The source
fence remains held. No direct nondurable publisher path is accepted.

After restart, successful outbox recovery publishes the identical payload and
drains the obligation. An installed retry returns the original logical cut and
receipt without republishing, advancing native time or changing the schema
generation. The captured primary payload, rather than only a constructed frame,
is replayed on the standby fixture, including duplicate/reopen/wrong-cut checks.
The shared publisher also checks that completion can acquire its checkpoint
barrier; no replication mutation lease survives into completion waiting.

`/private/tmp/antfly-index-install-primary-debug.log` passed the focused 56/56
Debug build steps, including 11 native lifecycle/outbox tests, 14 candidate
tests and 46 runtime contracts. Production code is unchanged from the preceding
159/159-qualified shared-install milestone. This is local storage/replication
fault coverage with synthetic metadata authority, not distributed metadata/root
authorization, installed ACK/release qualification or public SQL activation.
No original-case credit or throughput improvement is claimed.

### Metadata-derived native install grants

Native install permission responses now project the accepted logical validation
seal from the retained paired validation proof in the same metadata read
transaction. Projection requires the irreversible decision decoder floor, no
pending metadata outbox, the current lifecycle lock/range/catalog claim, and
matching frozen source-consumption and target-inventory provenance. It does not
copy schema-sized admission inputs or obtain authority from an RPC body.

The grant's domain-separated decision identity binds metadata group/incarnation,
plan, table, candidate/reservation and the consumed-EOF completion barrier. It
is stable across later ACK revisions, not a lease. Decoding rejoins the seal's
owner, plan, namespace, validation receipt digest and frozen completion scope.
Missing or substituted grants and altered validation cuts fail closed. Grants
are absent from other operations and acknowledged responses. Historical logical
acceptance is not a fresh physical audit or replacement-root adoption permission.

`/private/tmp/antfly-index-install-grant-debug-v3.log` passed 60/60 Debug build
steps, including all 50 publication tests. The real metadata-store fixture
projects after committed publication and reopen, checks malformed/missing grants
and revision stability, withholds projection behind an unmirrored metadata
decision, and exhaustively checks allocation-failure cleanup. Boundary, license,
inventory, formatting and diff checks passed. The authenticated native install
receiver/C-ABI bridge, current-root audits, installed ACK delivery and release
coordination remain unfinished; no public SQL activation or original-case credit
is claimed.

### Storage-derived installation commands

The receiver-facing storage preparation accepts a logical install intent: the
metadata-derived validation seal and irreversible decision identity. Storage
derives the physical native-schema and activation hashes from its own immutable
candidate, exact manifest JSON and seal-bound activation preparation. RPC callers
do not select these physical identities. Complete replication commands use the
same preparation and must match the locally derived result before applying.

Already-installed intent preparation checks the actual successor and original
intent, then reuses the committed command/cut without compiling or publishing
another schema. Metadata constructs intent only after binding permission to the
independently observed reserved or installed job. Another immutable input digest
or an incomplete fencing-phase job is not interchangeable with the accepted
validation binding. This remains logical preparation, not current-root audit,
transport authentication or physical-root adoption permission.

The install receiver/C-ABI bridge, installed-state physical audit support,
installed ACK/release coordination and full distributed activation remain
unfinished. No public producer or original-case credit is enabled by this entry
point. Native install wire format remains envelope 29; no parallel physical
command format or legacy fallback is introduced.

Qualification: `/private/tmp/antfly-index-install-intent-final-debug.log` passed
159/159 Debug build steps after a clean rebuild. Metadata tests reject a changed
observed manifest binding and a fencing-phase job. The native primary fixture
derives the same complete command from logical intent, rejects substituted
schema/activation hashes during preparation, and recovers an installed intent
without another publication. Existing outbox failure/restart, standby replay,
schema-history, C-ABI, ACK, publication and runtime tests passed. Boundary,
license, inventory, formatting and diff checks passed. An earlier native run
was interrupted by disappearing cache/SDK artifacts, not a reported code/test
regression; it was rerun from the worktree-local cache without skipping gates.
No native receiver/root-audit qualification, throughput result or original-case
credit is claimed.

### Read-only installed-state physical audit primitives

Installed recovery and physical auditing share an owned immutable history
object. It first verifies the real installed successor, exact schema/catalog/
activation and committed cut with the original fence held. It then owns and
checks retained predecessor schema/physical definitions and reconstructs the
hash-bound reserved integrity catalog from the admitted immutable manifest.

Its read-only view overrides exactly four historical metadata keys; all data,
cursors, range, namespace, native clocks and canonical proof records remain
reads of the caller's real snapshot. It exposes no puts, deletes or commits.
Recovery uses this same view, and generation auditing can now consume a
caller-pinned read transaction without opening a second snapshot. This is not
an execution capability, synthetic readiness flag or adoption permission.

The real-LSM installed/reopened fixture physically rescans inventory and all
generation passes with bounded pages, checks exhaustive history-allocation
failure cleanup, and detects an aborting corrupt primary row even though its
canonical metadata proof remains unchanged. Ordinary reserved-job admission
continues rejecting the real installed job. Audits do not advance native time
or change that committed phase. The focused Debug gate passed 56/56 steps.

Root-checkpoint/receiver integration must reuse the immutable compiled plan
across pages, open fresh snapshots and recheck the real installed successor;
it is not implemented by these primitives. Authenticated receiver/C-ABI,
installed ACK/release coordination, physical-root adoption and distributed
fault qualification remain unfinished. No public activation or original-case
credit is claimed.

Full qualification: `/private/tmp/antfly-installed-history-final-debug.log`
passed 159/159 Debug steps, including native lifecycle, candidate, standalone,
domain, C-ABI, leased-owner, ACK, publication and runtime suites. Boundary,
license, inventory, formatting, diff and generated control-catalog checks
passed. No throughput improvement or complete receiver qualification is claimed.

### Cached installed-root checkpoint execution

Installed auditing uses an audit-only mode of the existing single-flight,
resource-accounted, evictable candidate cache. A cold acquisition owns the
verified predecessor history and compiled plan. Its key binds the installed
command's evidence identity; normal build and installed-audit entries are not
interchangeable. Warm acquisition checks the actual installed record/intent,
and each scan page opens a fresh read transaction and rechecks the real
successor before constructing the read-only historical metadata view.

Inventory and generation progress use the existing root-local signed checkpoint
machinery. No LSM snapshot survives a request boundary; immutable metadata/plans
remain cacheable without keeping retired storage versions pinned. Cache leases
protect history/plan lifetime across concurrent eviction. Configured working-set
denial remains a distinct ResourceBudgetExceeded result, not a generic process
OOM. Before-install auditing continues using the existing reserved-owner path.

The focused Debug fixture passed 56/56 steps, including bounded checkpoint
completion, one cold compilation across many pages, stable ready-checkpoint
reuse and safe eviction with a live lease. The expanded focused gate also
qualifies restart and warm-cache current-fence failure. This is local audit
execution, not metadata authorization, a portable publication certificate or
replacement-root adoption permission. Authenticated receiver/C-ABI, installed
ACK/release and distributed fault qualification remain unfinished. No public
activation, throughput measurement or original-case credit is claimed.

The bounded host-only installation step now consumes these checkpoints directly.
Its input contains metadata's logical intent and a page budget; filesystem and
signing scope remain host-selected arguments. Incomplete/failed scans return
physical progress only. A complete scan must match the retained logical seal
and the actual physical DB incarnation before resource-accounted preparation
enters the shared schema/replication transaction. Installed retries take the
same physical-audit path, and responses pair the physical witness with the
logical outcome, withholding a receipt while replication is pending.

The expanded Debug regression covers first installation after multiple audit
pages, unchanged schema/reservation and zero publication attempts on incomplete
pages, outbox failure/restart recovery, and complete installed retry responses.
`/private/tmp/antfly-audited-install-step-debug-v2.log` passed 56/56 Debug
steps. The earlier broader run encountered a corrected name-shadowing compile
error in this addition and is not a passing gate. Boundary, licensing, inventory,
control-catalog, formatting and diff checks pass. Original dispositions remain
483 implemented / 862 unresolved; no new case closure or benchmark is claimed.

Full qualification of commit `997fee6f44` subsequently passed 159/159 Debug
steps in `/private/tmp/antfly-audited-install-full-debug.log`.

### Private installation interface

Kernel ABI 89 adds bounded native installation. The compiled boundary requires
a host-selected signing scope, rejects foreign owner groups and malformed or
oversized inputs, propagates deadline/cancellation, and preallocates its response
before a durable effect. The leased-owner adapter supplies its provisioned root
directory and returns the generation of the same applying lease, alongside an
owned, validated physical/logical response. Remote requests do not select paths,
physical schema hashes, replay positions or completed audit summaries.

An isolated C-ABI fixture covers expired requests, missing/oversized root scope,
wrong owner/signing identity, bounded first installation and receipt-stable
installed retries after reopening. The adapter's deadline/cancellation test
also instantiates the new path before any owner acquisition. The focused gate
passed 88/88 Debug steps in `/private/tmp/antfly-install-interface-debug.log`;
boundary, licensing, inventory, formatting and control-catalog checks pass.
Authenticated receiver dispatch, installed witness
acceptance/retention, release coordination and full public/distributed SQL
qualification remain unfinished; no original-case dispositions change.

### Authenticated install dispatch and retained installed acknowledgments

The existing authenticated control port now accepts logical install requests.
Each receiver obtains fresh metadata permission and a bounded install grant,
then invokes the leased kernel path without using reserved-only inspection for
installed retries. Signing pairs the complete physical audit with the committed
installed receipt, under one captured root generation; pending replication and
partial audits produce no installation ACK. Both signatures publish together.

Protocol floor 52 covers the new private paired-proof envelope. Native installed
ACKs require a physical witness and exact install command; bare installed
receipts are invalid. Metadata joins the current successor catalog/range/name
authority, frozen source/target provenance, retained accepted validation seal,
durable publication decision and observed installed native clock in the same
accepting transaction. Head, owner and witness persist atomically. ACK status
checks the retained physical digest and withholds acceptance while metadata
publication is pending. The decision identity remains stable through release.

The bounded scheduler handles already-decided installing owners through the
existing ACK/status resolution state machine. The focused Debug gate passed
75/75 steps in `/private/tmp/antfly-installed-ack-debug-v2.log`: paired-signature
tampering/pending cases; metadata commit/reopen and duplicate ACKs; stale clock
and forged decision rejection; rollback before/after each of the three writes;
lost replies, pending publication, stale revisions and volatile scheduler
restarts. The broader Debug gate is pending. These component/fault fixtures do
not claim end-to-end distributed SQL qualification or another original case.

Automatic publication-decision dispatch, durable native release/retirement,
latest-stage replacement-root adoption and full distributed/public producer
qualification remain unfinished. Original accounting is unchanged at
483 implemented / 862 unresolved.

Full installed-witness qualification subsequently passed 159/159 Debug steps
in `/private/tmp/antfly-installed-ack-full-debug.log`.

### Automatic irreversible publication decisions

Authority-bound private decision/status calls reuse the admitted job's existing
atomic descriptor/name/head transaction. Both metadata-leader and standalone
ports preserve uncertain post-proposal outcomes; status binds the exact table,
job and pre-decision revision and withholds completion while an outbox remains.
Private HTTP replies are capped at 2 KiB, including body-bound read grants and
definitive versus ambiguous pending-publication errors.

When every frozen source and validation ACK is complete, the bounded scheduler
submits the decision and resolves uncertainty through status before selecting
a fresh CAS cut. Restart recovery advances from durable progress, never by
replaying SQL DDL. Standalone publishes the changed descriptor through an O(1)
table upsert on its exact revision, falling back to coherent projection reload
only if another writer overtook that cut.

Coverage includes decision authority/locking, post-proposal uncertainty, pending
publication, commit/reopen status, exact job correlation and scheduler faults
before/after commit, lost replies, stale revisions and volatile restarts. The
initial expanded gate passed its standalone suites but exposed two old phase/
permission expectations in the validation-restart fixture; these were updated
to assert the new durable decision advance while retaining single-seal and
single-ACK checks. The corrected focused Debug gate passed 75/75 steps in
`/private/tmp/antfly-auto-index-decision-debug-v3.log`. Boundary, licensing,
inventory, formatting, diff and control-catalog checks pass; a final broader
gate for this revision remains pending.

Durable native release/retirement, latest-stage replacement-root adoption and
full distributed/public producer qualification remain unfinished. No original
SQL dispositions or throughput claims change.

Full automatic-decision qualification subsequently passed 159/159 Debug steps
in `/private/tmp/antfly-auto-index-decision-full-debug.log`.

### Native release intent and terminal record

The release intent binds the complete installed command and its actual native
position to a distinct metadata release decision. Its released receipt must use
the same namespace and a strictly later native position. Before first execution,
the installed command/position must match the real durable installation slot
and its guarded successor; possession of a checksum is not permission.

The terminal record owns its fixed-size data, uses allocation-free bounded
encoding, and rejects corrupt or noncanonical bytes. Loading checks the durable
native clock even for a different retained intent. Its meaning is historical
release evidence, not a claim that mutable rows still match a prior build audit
or authority to adopt a replacement root. The existing configured page gate
includes its semantic, tampering, canonical-byte and exhaustive allocation-failure
tests; `/private/tmp/antfly-index-release-codec-debug.log` passed 56/56 Debug steps.

This component is not connected to fence lifting yet. Atomic release, recovery,
replication/outbox, bounded job retirement, receiver/metadata release permission
and root-continuity qualification remain required. No original SQL credit or
public activation is claimed.

### Native release, bounded retirement and cancellation dispatch checkpoint

The later release milestones supersede the disconnected-component status above.
Native release now commits the exact installed-fence lift, historical terminal
record, native clock, fixed build-slot retirement, transport marker and durable
outbox atomically. Signed release acknowledgments are reconstructed from retained
installation evidence; metadata retires the exact lifecycle lock with the final
acknowledgment. The bounded worker resumes this path after lost replies and
volatile restarts. Replacement-root adoption remains guarded, not implemented.

Obsolete source-batch and inventory companions are retired through canonical,
replicated pages (128 records / 2 MiB / 5 ms preparation slice), not cancellation
GC of live UNIQUE claims. A durable objective prevents the single terminal slot
from being overwritten by a later build until retirement completes. Qualification
includes per-write abort faults, pending mirror recovery, duplicate standby replay,
reopen, admission barriers and unchanged live claims/user postimages. The combined
Debug gate passed 159/159 build steps in
`/private/tmp/antfly-native-release-companion-gc-full-debug.log`.

Pre-publication cancellation now has an exact durable reason/revision decision,
typed replicated command, bounded authenticated catalog calls and readonly status.
Metadata leadership and standalone use the existing authority, capability and
hot-standby mutation gates. Post-proposal uncertainty is resolved against the exact
job; outstanding replication withholds confirmation. This dispatch checkpoint
passed 111/111 Debug steps in
`/private/tmp/antfly-native-index-cancellation-dispatch-debug-3.log`.

Failure-driven worker cancellation is now qualified in the focused Debug gate: validated build
and pre-decision inventory/generation failures select an exact cancellation CAS;
pending native replication does not select one. Lost mutation replies use status
reads, and losing CAS attempts refresh authoritative progress. Installation audit
failures cannot undo an irreversible publication decision. Decision confirmation
is admission to cleanup, not a canceled-owner acknowledgment or fence release.
The publication/ACK gate passed 69/69 steps in
`/private/tmp/antfly-index-failure-cancellation-debug.log` (55 publication/storage,
four ACK/read-identity and one capability test). It selects all three scheduler
tests explicitly through the metadata test root, including uncertain decision
faults; the receiver fixture exercises failed-build scheduling, pending native
replication and zero source delivery/install/release effects. The earlier
storage-owner-source filter selected no tests and is not validation evidence.

Remaining activation work includes cancellation cleanup/final signed acknowledgments
and cohort release, mounted public online-index admission/status, later-stage
replacement-root recovery, hosted/Raft execution and broader index candidates.
No original-case dispositions change: 483 implemented / 862 unresolved. Component
qualification does not substitute for unchanged-source public SQL evidence.

### Durable native cancellation cleanup completion

The native owner now has a distinct cleanup-completion effect. It checks the
actual cancellation intent, drained reserved job, burned allocation/catalogs,
artifact retirement and private claim-prefix emptiness before committing a new
native cut. Its 1,216-byte canonical record owns the physical binding, cleanup
admission and ordered intent/completion positions. The resulting canceled receipt
binds this record; it is not inferred from queue admission or an absent job.
The job and source fence remain active for the subsequent cohort release barrier.

The effect uses the existing native transaction, replay marker and durable outbox
pipeline (private envelope version 32). Pending replication withholds a receipt;
exact retries recover the original committed position. Cold recovery verifies
the retained record against current owner state or a later completed epoch.
Qualification passes for per-write aborts, early completion rejection,
canonical framing, mirror failure/reopen, duplicate standby replay and unchanged
job/fence ownership.

The direct reopen/retry regression exposed a shared native-effect lock-order bug:
startup outbox recovery could wait for checkpoint-capable completion while holding
the replication mutation shared lease. Recovery now runs before lease acquisition;
write authority is rechecked after acquisition and under apply-exclusive. The
fixture deliberately does not manually flush the recovered outbox before retry.

The focused Debug page/runtime gate passed 56/56 build steps in
`/private/tmp/antfly-index-canceled-completion-debug-5.log`: 13 local storage,
14 native candidate/owner and 46 runtime-contract tests, with no failures or
leaks. Apache boundary (2,061 files), licensing, control-catalog, formatting,
diff and original inventory integrity checks pass. The intermediate failure
after the lease-order fix was a fixture supplying a null physical catalog to
reserved-job recovery; it now loads the actual catalog from the same snapshot.

This is the durable native checkpoint, not complete public cancellation activation.
The leased receiving boundary, signed cleanup witness/metadata verification,
bounded cancellation scheduling and final cohort fence release remain required.
Original SQL accounting and performance claims are unchanged.

### Leased cancellation-completion boundary

The private kernel ABI now has an identity-only cleanup-completion operation
(ABI 91). The receiver supplies a host-selected signing-root scope, checked
against the actual persisted key before mutation; remote callers cannot supply
a physical allocation, cleanup record, native cut or root directory through
the source adapter. Controlled cancellation/deadlines and writer admission
apply before the effect, and the response buffer is reserved before commit.

The source adapter captures the returned root generation from the same applying
lease that executes/retries completion. A subsequent routing switch cannot
relabel the earlier result. The fixed cancellation inspection projects the
durable completion record separately from physical retirement, validates its
identity/cuts, and keeps the reserved job and source fence active.

Debug qualification passes for raw ABI scope/size/version/deadline/root
rejection, denied-writer no-effect checks, cold recovery, exact retries through
the real compiled owner, and pinned-generation behavior. Signed cleanup
witnesses, independent metadata verification, bounded cleanup scheduling and
the final cohort fence-release barrier still require integration. No public
activation or original-case credit is claimed by this boundary alone.

The combined Debug gate passed 88/88 build steps in
`/private/tmp/antfly-index-cancel-complete-leased-debug-4.log`: six ABI/runtime-status,
three actual leased-adapter and 46 runtime-contract tests, with no failures or
leaks. Formatting, diff, licensing, Apache boundary, control-catalog and original
inventory integrity checks pass. Intermediate compile failures were the missing
prepared-operation dispatcher branch and fixture/inspection syntax; no failing
or empty-selection run is counted as qualification.

### Signed cleanup witnesses and bounded metadata verification

Native canceled acknowledgments now require the complete cleanup record covered
by the root signature. A bare canceled receipt is rejected. The private retained
proof format is version 3; validation, installation and cancellation keep their
mandatory witnesses in the same atomic metadata stage record, without a second
journal or support for older private witness formats.

Admission's fixed owner projection (version 2) stores the native manifest digest,
native before-schema digest and burned claim allocation. The shared candidate
compiler derives the native schema identity from its already parsed declaration;
native admission checks it against the actual physical reservation. Public JSON
schema identity remains separate. All forward and cancellation binding checks use
these admitted identities without schema-sized recompilation in the ACK section.

Live cancellation apply joins the witness to the current catalog/range/lock and
the latest visited stage's root. A validated owner's physical binding must remain
identical; unvisited owners need no invented prior receipt. Cold recovery repeats
the same immutable joins and cannot accept a changed claim allocation even when
an injected record has consistent signatures, receipt hashes and owner hashes.
The signed completion receiver uses fresh metadata permission and the applying
lease's generation. Pending replication or signing failure yields no ACK while
preserving the original durable completion cut for retry.

Qualification passes for exact accepted retries, per-write head/owner/proof
abort faults, raw-witness rejection, schema/manifest substitution, cold consistent
forgery rejection, reader-capability cleanup and actual leased-owner signing.
The first real-owner run correctly rejected comparing a native binary schema
digest with a public JSON digest; the shared compiler bridge fixes that mismatch
without weakening either identity check. Mounted scheduling and final cohort
fence release remain unfinished. No original SQL credit or public activation is
claimed by this checkpoint.

The frozen-source full Debug gate passed 159/159 build steps in
`/private/tmp/antfly-index-cancellation-witness-full-debug-5.log`: 261 test executions
across metadata, native/storage, ABI, runtime and standalone lanes, with no failures
or leaks. Formatting, diff, Apache boundary, licensing, control-catalog and original
inventory integrity checks pass. The broader intermediate run exposed a negative
fixture expecting the internal catalog error at the snapshot boundary; the final
fixture asserts both `InvalidCatalogRecord` internally and `InvalidMetadataSnapshot`
at snapshot validation. Production rejection was retained unchanged. Original
accounting remains 483 implemented / 862 unresolved.

### Immutable cancellation barrier and terminal catalog retirement

The private publication head (format 4) retains the exact revision at which all
owners acknowledged durable cleanup. Cancellation release decisions bind this
immutable barrier, the job/cohort and failure reason, not the moving ACK revision
or source EOF. Failed builds need not reach EOF. Later release ACKs and root
reattestations cannot change the decision identity.

Native cancellation-release permission carries the accepted cleanup record and
its root proof. Metadata rechecks the current runtime root before issuing it;
the permission verifies the accepted owner receipt and physical commitment.
Even a freshly signed proof for another root generation cannot substitute for
the accepted cleanup proof. The receiving release operation still needs to pin
that generation before any native fence lift; this grant alone does not activate
native cancellation release.

The final cancellation ACK now deletes the active publication pointer, advances
the tracked catalog source clock and retires the lifecycle lock atomically with
head/owner/proof updates. It participates in the existing relation-command
journal: reserved index names are removed by the same source-derived catalog
projection used by other table/publication operations, without a separate index
name registry. Snapshot projection accounting includes every affected record
class. Ordinary ACKs retain the standalone projection; only a changed source
clock invalidates it.

The frozen-source focused Debug gate passed 72/72 build steps in
`/private/tmp/antfly-index-cancel-terminal-catalog-debug-3.log`: 56 metadata,
four ACK and one service test executions, plus the pure progress lane, with no
failures or leaks. Both Raft and native metadata fixtures exercise the shared
catalog journal, complete checkpoint verification and rollback before/after
each of the six terminal writes. They use synthetic signed owner cuts, not
evidence of native fence release. Formatting, diff, licensing, Apache boundary,
control-catalog and original inventory integrity checks pass.

Native terminal fence release, accepted release-proof reconstruction, mounted
cancellation scheduling and public online-index admission remain unfinished.
No original SQL case credit or end-to-end activation is claimed.

### Native cancellation release and independently reconstructed ACKs

The native terminal cancellation intent contains metadata's accepted cleanup
record and immutable cohort decision. A dedicated topology gate requires the
actual local cleanup record, retired artifacts/private claims/source companions,
the exact reserved binding and drained source. Generic topology release and
cancel still cannot bypass index publication/cancellation ownership.

Release stages a fixed checksummed terminal record, completed-epoch tombstone,
fence lift, native clock and bounded retry-slot retirement in the same native
transaction as the replay marker and durable outbox. The existing pending source
window is already bounded and retired by cancellation cleanup; completion also
checks its job prefix for orphan companions. No primary-row scan, live index
deletion or whole-history cleanup occurs in terminal release. Published and
canceled terminal slots supersede one another only after predecessor companion
retirement has cleared admission.

Exact retries use historical terminal evidence rather than re-auditing mutable
data or depending on the now-retired job. Pending replication returns progress
without a publication receipt. Primary and standby recovery validate the native
clock, completed epoch and absence of the released job/fence.

Metadata independently reconstructs cancellation release evidence from its
accepted cleanup witness and cohort barrier, and requires the same root identity,
generation, reporter and native binding. Both live ACK apply and cold recovery
reject a signed action with substituted evidence, even if the retained proof and
owner receipt/attestation hashes agree with the substitution.

The frozen-source combined Debug gate passed 87/87 build steps in
`/private/tmp/antfly-index-cancel-release-final-debug.log`: 134 test executions
across native candidate, local storage, runtime-contract, metadata, ACK and
service lanes, plus the cached pure progress lane, with no failures or leaks.
Qualification includes aborts before/after all 11 terminal metadata writes,
orphan-source refusal, strict record framing, pending-outbox reopen/retry,
duplicate standby replay, foreign-namespace tombstone rejection, ordinary writes
after release and stable release retries after another restart. Live/cold metadata
forgery tests qualify independent receipt reconstruction. Formatting, diff,
licensing, Apache boundary, control-catalog and inventory integrity checks pass.

The leased ABI/root-generation pre-effect guard, mounted cancellation scheduling,
and public online-index admission still require integration; this private storage
effect is not public activation or SQL credit. Original accounting remains
483 implemented / 862 unresolved.

### Leased cancellation-release boundary

Private kernel ABI 92 adds cancellation release alongside published release in
the same terminal dispatcher. The body is the accepted fixed cleanup intent;
the host chooses the signing-root scope. Cancellation/deadline, group, scope,
bounded-body and writer checks precede the effect, and the common response
buffer is allocated before commit.

The source adapter requires metadata's accepted cleanup root generation and
compares it on the same applying lease before calling storage. A stale generation
or signing-root incarnation cannot lift the fence and then rely on a later
signature failure to reject its ACK. The returned outcome retains that applying
generation rather than rereading routing after the effect.

The receiving release step obtains fresh metadata permission and uses the
accepted cleanup proof's generation. Completion and release share owned result,
pending-replication and signing-failure handling. Release ACKs carry the terminal
receipt, not a second cleanup witness; metadata reconstructs their evidence from
its already accepted witness and immutable cohort barrier.

The frozen-source Debug leased/native gate passed 88/88 build steps in
`/private/tmp/antfly-index-cancel-release-leased-debug-2.log`: six raw ABI/status,
three actual leased-adapter and 46 runtime-contract tests. The separate metadata
regression gate passed 69/69 steps in
`/private/tmp/antfly-index-cancel-release-receiver-metadata-debug.log`: 56 metadata,
four ACK and one service tests. No failures or leaks were reported. The first
compile exposed a missing prepared-type dispatcher branch, corrected before
qualification; the failed run is not counted as evidence.

Coverage includes invalid control versions, oversized scopes, cancellation and
deadlines, foreign groups/signing roots, writer refusal, unchanged source cuts
after stale-generation refusal, native terminal retries, fresh metadata grant
validation and signed terminal receipt reconstruction. Completion/release share
owned result handling; failed signing preserves the durable cut without an ACK.
Formatting, diff, licensing, Apache boundary, control-catalog and original
inventory integrity checks pass.

Mounted private HTTP cancellation controls, bounded scheduler cleanup/release
stages, replacement-root adoption and public online-index admission remain
unfinished. Published release also needs the same pre-effect accepted-generation
guard. No public activation or original SQL case credit is claimed; the inventory
remains 483 implemented / 862 unresolved.

### Shared accepted-generation protection for terminal release

Published release now carries its accepted installation root proof, not only
the logical installed command. The grant checks the root signature and exact
retained owner attestation, then matches the current signing identity, reporter
and native binding. A freshly re-signed different generation cannot substitute
for metadata's accepted proof. Live permission issuance still verifies that the
accepted physical root is current.

Published and canceled source adapters use the same accepted-generation guard
on their applying lease before invoking storage. The published receiving path
passes the accepted generation explicitly and verifies that the returned cut
belongs to it. No caller flag or post-release signature failure can replace this
pre-effect check. The check is constant-sized and adds no schema/index audit or
user-row scan to terminal release.

The frozen-source Debug gate passed 103/103 build steps in
`/private/tmp/antfly-index-terminal-root-guard-debug.log`: 56 metadata, four ACK,
one service and three actual leased-adapter tests, with no failures or leaks.
The metadata fixture rejects re-signed generation substitutions and a changed
current runtime root; the receiving/scheduler fixture rejects a stale applying
generation before any release call/effect. Existing lost-reply, pending,
restart and signed-release checks remain green. Formatting, diff, licensing,
Apache boundary, control-catalog and inventory integrity checks pass.

Private cancellation transport/scheduling, replacement-root adoption and public
online-index admission remain unfinished; original SQL accounting is unchanged.

### Cancellation on the shared private control transport and scheduler

The existing private index-control request now admits cleanup, completion and
cancel-release operations. They use the same HTTP route, in-process receiving
port, service-authority/deadline gates and post-commit signing machinery as
ordinary index controls. There is no separate cancellation route or public
relational-index API. Providers without the complete cancellation contract fail
closed rather than exposing only part of that lifecycle.

Replies carry compact scoped progress and, when available, the complete signed
ACK. They do not duplicate schema/manifest/cleanup outcome bodies. Progress alone,
physical GC completion, pending replication or failed signing cannot acknowledge
an owner. Validation binds request identity, native cut, action, root generation
and witness/signature; local delivery retains ownership until serialization or
consumption finishes.

The bounded worker resumes canceling/cancel-releasing jobs. Each turn executes
one receiving-port or metadata operation: cleanup advances at most one admitted
native effect/page, completion obtains a signed durable cleanup checkpoint, and
release uses metadata's all-owner barrier. ACK proposals and lost-reply status
resolution reuse the existing ACK state machine. The all-owner barrier still
prevents one owner's early local retirement from lifting the cohort's fences.

Residency now reserves cohort capture and completed-owner recovery work in
addition to the fixed useful-work allowance. A 4,096-owner capture/sweep cannot
consume the entire allowance and repeatedly restart at the same completed prefix.
The source-EOF recovery reservation derives from the producer state machine, not
only the cheaper acknowledged-control path. Jobs/actors, per-turn effects and
page memory stay bounded. This is work-count/liveness accounting, not a measured
latency or large-cluster throughput claim; a pending-owner directory remains a
possible improvement over repeated cohort sweeps.

The frozen-source Debug gate passed 103/103 build steps in
`/private/tmp/antfly-index-cancellation-mounted-final-debug-2.log`: 57 metadata,
four ACK, one service and three real leased-adapter tests, without test failures
or leaks. The real compiled-owner fixture uses a synthetic metadata journal,
loses both cleanup and release ACK replies after commit, and requires status
recovery with at most one receiving-port call per quantum. Compact wire roundtrips
fit the 32 KiB control cap and reject foreign groups/root-generation substitutions.
The cohort-budget test covers every admitted owner count through 4,096; it is
arithmetic work-count evidence, not a full 4,096-owner deployment benchmark.

The focused authenticated HTTP route test passed in
`/private/tmp/antfly-index-cancellation-http-debug.log` (117/117 build steps).
It covers the new cleanup/completion/release operations on the existing route,
alongside service authorization, deadlines and body/scope limits. That log also
retains an earlier Debug compile memory-cap warning (13.39 GB peak versus a
12.88 GB declared bound); the final route test and build succeeded. This is not
evidence that the compiler memory issue has been eliminated.

Intermediate failures caught enum-literal inference, an incomplete test-provider
contract and a direct test Job initializer missing its budget. The first real-owner
lost-reply run correctly rejected missing earlier stage attestations in the
synthetic metadata journal; the fixture now retains signatures for the genuine
native fence/reserve cuts. Production ACK-status verification was not weakened.
Formatting, diff, licensing, Apache boundary, control-catalog and original
inventory integrity checks pass. Independent durable metadata verification
remains covered by the metadata lane.
Replacement-root adoption, hosted/Raft execution and public online-index admission
remain unfinished. No original SQL case credit or complete SQL activation is claimed.

### Exact online-index admission observation

The coordinator now has a compact admission-status read through the shared
system-catalog envelope, metadata owner and standalone facade. Its request binds
the table, job ID, immutable plan digest and metadata incarnation. The read uses
one storage snapshot and a constant number of point reads, not schema decoding
or a cohort/catalog scan. It checks the active-pointer invariant and withholds
confirmation while a metadata outbox obligation remains. A different retained
job is a changed observation, never successful admission of the requested job.
Absence is an observation at this cut, not proof that an earlier proposal cannot
still commit; callers must not replay uncertain admission based on absence.

The read requires the existing body-bound index-publication authority and has a
2 KiB response cap. The typed metadata client validates the transport incarnation
against the request and independently validates the returned body. It grants no
physical execution permission. Public SQL/REST admission still needs the trusted
plan producer and recovery-receipt integration; this read does not activate
`CONCURRENTLY` or earn original SQL case credit.

The final frozen-source Debug gate passed 117/117 build steps in
`/private/tmp/antfly-index-admission-status-final-debug-2.log`: 59 metadata,
four ACK transport, four standalone coordinator, three real leased-adapter and
one decoder-barrier tests, without failures or leaks. Evidence includes absent
and admitted observations, recovery after reopening storage, substituted plan
and incarnation rejection, service authority ordering, bounded body-bound reads,
and foreign transport identity rejection. An intermediate transport test exposed
the generic reader's lack of request-incarnation comparison; the typed client
now performs that comparison instead of weakening the test. Original inventory
remains 483 implemented / 862 unresolved.

### Compiled metadata-owner controls and read-only admission preflight

The production metadata facade previously lacked the new admission-status,
publication decision/status and cancellation preflight/status methods even though
the direct-storage lanes passed. These now cross the existing owned JSON catalog
projection boundary, with matching receiver cases and exact semantic errors.
The new compiled-owner fixture exercises those methods, including decoder
readiness, absent jobs, missing occupancy, foreign incarnation, unchanged durable
revision and reopen. It is not evidence of a complete hosted publication job.

Admission preflight shares durable apply's schema, logical owner, namespace,
policy and complete range-cohort checks. Immutable candidate preparation stays
outside that read. The shared read-only head/exclusion check is repeated in
`stageBegin`; successful preflight is never a lease or permission to skip apply
validation. Preflight additionally checks namespace occupancy without staging a
job, advancing a source clock or attaching a mutable command outcome/journal.
Durable apply retains its shared transactional namespace publisher and rollback.

An intermediate compiled-owner test exposed `CatalogSourceUntracked` collapsing
to a generic storage failure. Begin now checks writer readiness first and returns
the existing registered `CatalogOccupancyNotReady` precondition, consistently with
publication and cancellation. No broad ABI error alias or weakened expectation
was introduced. Public online-index plan production/admission, replacement-root
adoption and hosted/Raft execution remain unfinished; original case dispositions
are unchanged.

The frozen-source Debug gate passed 103/103 steps in
`/private/tmp/antfly-index-admission-compiled-final-debug.log`: 59 metadata,
four ACK transport, one decoder-barrier and one actual compiled metadata-owner
test, without failures or leaks. The direct-store lane additionally verifies
successful read-only preflight, definite namespace conflict, foreign incarnation,
and rejection after the job was already admitted. Licensing, Apache boundary,
control-catalog, formatting/diff and original inventory checks pass.

### Mounted coordinator admission and exact uncertain-outcome recovery

`relational_index_begin` now uses the existing private system-catalog envelope,
with administrator and index-publication authority required together. Metadata
prepares and encodes the immutable candidate before taking the catalog mutation
gate, validates decoder readiness and read-only admission at the admitted cut,
and proposes one existing durable begin command. It releases the gate before
waiting for apply. A lost reply, changed job, malformed observation or pending
metadata outbox returns an unknown mutation outcome; no automatic begin replay
or apparent success is synthesized. The recovery request is retained before
submission and uses the previously mounted exact admission-status read.

Standalone uses the same preflight, durable command and exact observation, with
the shared hot-standby mutation lease and write gate. Candidate preparation is
outside its projection mutex; an exact revision advance updates only its epoch,
since admission has not published the successor table. Both ingress paths return
compact admission status, not physical execution permission or ready-index state.
Transport enforces the body-bound administrator grant and 2 KiB response cap.

This mounts coordinator admission, not public SQL/REST online-index activation.
The trusted plan builder still needs to capture the actual physical predecessor
catalog and owner identities, including burned allocations, and public recovery
receipts must be connected before activating the original statement cases.
Replacement-root adoption and hosted/Raft execution also remain unfinished.

The final frozen-source Debug gate passed 117/117 steps in
`/private/tmp/antfly-index-admission-mounted-debug.log`: 60 metadata, four ACK
transport, one decoder-barrier, four standalone coordinator and one actual
compiled metadata-owner tests, without failures or leaks. The new admission
dispatch test uses a synthetic service to qualify authority, definite preflight
conflict, gate release before waiting, lost-reply status recovery with exactly
one proposal, and withholding confirmation while replication is pending. Direct
durable admission and compiled storage projection are qualified separately;
this is not a complete public or hosted online-index execution test. All static
checks pass; original inventory remains 483 implemented / 862 unresolved.

### Trusted table-scoped index preparation

The shared catalog read now captures only the authorized table and its bounded,
indexed owner cohort in one snapshot. Metadata selects native receipt authority;
unmounted hosted/Raft execution is rejected, and pending root enrollment or
replication remains a retryable preparation precondition. The owned builder
addresses captured groups with read-index reads, checks namespace/catalog/epoch
identity, preserves the actual predecessor catalog (including burned canceled
allocations), and reuses one scratch arena between owner observations. It clones
the retained table, ranges and logical guard and prepares the immutable candidate
and exact recovery identity before submission. Neither cut nor preparation grants
execution or changes catalog state; durable begin independently rechecks admission.

The final frozen-source Debug gate passed 117/117 steps in
`/private/tmp/antfly-index-plan-builder-final-debug.log`: 62 metadata, four ACK
transport, one decoder-barrier, four standalone coordinator and one compiled
metadata-owner tests, without failures or leaks. Coverage includes an actual
native metadata preparation cut, readiness/conflict/outbox checks, no revision
advance, compiled projection and administrator-bound transport, plus a synthetic
owner reader proving burned allocations are retained and owner drift rejected.
No latency benchmark or complete public execution is claimed. Static checks pass;
the new file's initial license-header formatting mismatch was corrected. Public
SQL/REST recovery receipts and activation, replacement-root adoption and hosted
execution remain unfinished. Original inventory is still 483 / 862.

### Table-authorized, name-bound publication recovery reads

The private scoped status read now requires the current logical table guard as
well as the exact admission identity and index name. Reads and writes share one
indexed ownership/incarnation boundary. Polling reads no table/schema JSON and
no owner cohort: it consumes the fixed head and one validated owner dispatch.
Dispatch v3 includes an index-name commitment, independently joined to native
inputs during admission and to the retained plan during recovery. Wrong names,
substituted table identities, stale guards and foreign transport incarnations
are rejected. Absence remains an observation, never permission to replay begin.
The shared metadata, standalone, compiled projection and authenticated client
paths all carry this read; it is not yet the public SQL/REST recovery endpoint.

The final frozen-source Debug gate passed 142/142 steps in
`/private/tmp/antfly-index-scoped-recovery-final-debug.log`: 62 metadata/index,
50 relation-store, four transport, four standalone, one decoder-barrier, one
compiled metadata-owner and three real owner-adapter tests, without failures or
leaks. The first compile caught an incorrect decoder helper name. The next run
exposed a Debug stack overflow in the deeply nested lifecycle fixture: the fault
was a function-prologue stack save, not a JSON buffer access. LLDB launch was
blocked by local debugger permissions; disassembly established the unchanged
16 MiB executable stack and the oversized fixture frame. Replacing its inline
authority loop with runtime iteration retains both Raft/native cases and reduces
that frame from 7,934,592 to 5,179,024 bytes. No stack-budget increase or optimized
build was used to make the final gate pass. Public recovery receipts, activation,
replacement-root adoption and hosted execution remain unfinished. Inventory
remains 483 implemented / 862 unresolved.

### Generated public index-publication recovery contract

The owned native plan now prepares a bounded, canonical recovery token before
admission. It binds metadata incarnation, immutable table ID, publication ID and
plan digest. Its versioned checksum detects corruption, not authorization; every
status read still requires the current logical table guard and trusted ingress
capabilities. The read checks cancellation/deadline before and after metadata IO
and projects the bounded exact observation into generated OpenAPI types.

Public states distinguish unknown admission, pending, published, failed and
canceled. An unmirrored terminal observation remains admission_unknown. Published
describes retained-job completion, not current query readiness after later schema
changes. An absent job never authorizes automatic DDL replay. SQL DDL receipts
now describe index recovery tokens and the existing FK publication identity in
the same generated contract; SDK root exports include the public recovery types.

This is the recovery contract and projection boundary, not activation of the
public SQL/REST endpoint. Public admission/status wiring, replacement-root
adoption and hosted execution remain unfinished. Original inventory remains
483 implemented / 862 unresolved; no case credit is assigned to this milestone.

Final frozen-source Debug qualification passed 142/142 build steps in
`/private/tmp/antfly-index-public-recovery-final-debug.log`: 65 metadata/index,
50 relation-store, four transport, four standalone, one compiled metadata-owner,
three real owner-adapter and one decoder-barrier tests, without failures or
leaks. The three added native tests cover canonical bounded tokens, damaged and
truncated handles, authorization-before-IO, replaced table identities, uncertain
replication, terminal outcome projection and generated DDL receipt compatibility.
The focused Go recovery-contract test, 58 Python recovery/maintenance/SQL tests
and TypeScript SDK typecheck pass after final generation and formatting.
`make generate`, `make fmt`, inventory integrity, Apache boundary, license headers,
control catalog and whitespace checks pass. No latency benchmark is claimed.

### Mounted public index-publication recovery reads

Exact recovery is now a read-only subresource of the existing `/indexes` API,
for both default tables and explicit database/namespace targets. The required
token travels in a header rather than a URL. Trusted ingress checks current
logical table administration before granting private capabilities; a token never
authorizes a read. One bounded point lookup captures the logical owner without
schema JSON or a catalog inventory. The following bounded metadata observation
rechecks that exact guard atomically with the retained job. Replacement, ownership
drift and metadata-incarnation mismatch are conflicts, never implicit rerouting
or automatic replay. Successful observations are marked `Cache-Control: no-store`.

Go, Python and TypeScript wrappers expose default and explicit-scope reads,
preserve exact string identities and unknown admission, reject foreign handles
and unconfirmed completion, and cap response bodies at 16 KiB. TypeScript shares
its bounded, cancellation-aware request machinery with writes and maintenance;
the method-neutral helper name now reflects GET/POST/DELETE support.

Frozen-source Debug qualification passed 80/80 public-HTTP build steps (three
selected tests) and 69/69 metadata build steps (66 metadata/index, four transport,
one decoder-barrier tests), without failures or leaks. Logs are
`/private/tmp/antfly-index-public-route-http-final-debug.log` and
`/private/tmp/antfly-index-public-route-metadata-frozen-debug.log`.
The mounted socket test covers table-admin versus read-only authorization,
missing tokens before IO, escaped names, both route forms and replacement tables.
The owner-resolution test also covers stale reads, wrong relation kinds/names
and peers lacking authoritative ownership. Two focused Go tests, 14 Python
recovery/maintenance tests, 69 TypeScript recovery/maintenance/row/SQL tests and
SDK typechecking pass. Generation, formatting, inventory, license, Apache and
control-catalog checks pass. Initial socket attempts required sandbox loopback
access; a sampled Python startup stall was in macOS timezone notification, before
the checker script, and static checks passed with the required OS access.

This activates public recovery reads, not SQL index admission or an end-to-end
distributed build. Public DDL token producers, replacement-root adoption and
hosted/Raft execution remain unfinished. No original-case disposition or latency
benchmark claim changes: 483 implemented / 862 unresolved.

### SQL regular UNIQUE admission and request-shaped completion

Regular SQL `CREATE UNIQUE INDEX` now lowers to the owned private publication
pipeline instead of first publishing the successor schema through ordinary CAS.
The existing bounded namespace lookup and logical-table authorization are kept.
Preparation reads actual captured owner catalogs; the result allocator owns the
complete receipt and token before the single begin call. Backends without the
required reader/runtime or publication capabilities reject before admission;
there is no unsafe schema-first fallback or native alias for Raft execution.

Lost internal begin replies, OOM while constructing acknowledgements, malformed responses and
unmirrored observations reconcile only the exact immutable admission identity.
They never repeat begin. Published terminal progress is required for SQL success.
Duplicate validation becomes SQLSTATE 23505 only after canceled cleanup and fence
release complete; pre-terminal cleanup remains pending. Definite replication
headroom rejection is a retryable catalog-publication precondition, not a generic
internal error or an ambiguous admitted write. The prepared schema/catalog/cohort
is consumed before any recovery read or long wait; only fixed identity, an owned
receipt and bounded observation buffers remain in the admission helper.

`DdlAllocators` carries transport-shaped completion policy. Pgwire selects the
request deadline; HTTP retains bounded observation and durable receipts. Existing
constraint activation waits also honor this policy. Pending wire errors now say
the operation was admitted, without falsely asserting a successor schema already
committed. The [PostgreSQL 18 CREATE INDEX contract](https://www.postgresql.org/docs/18/sql-createindex.html)
keeps regular write-fenced builds distinct from CONCURRENTLY; concurrent syntax
is still rejected before preparation rather than routed through this build.

Integration review also found the StatusSource adapter hiding exact publication
incarnation conflicts behind routing errors. Exact index observations now retain
the typed conflict through that adapter, with a mounted HTTP regression.

This activates the native-capable SQL admission boundary, not complete distributed
index qualification. Hosted/Raft execution, replacement-root adoption, automatic
request-to-job cancellation, and real-owner original-case build/rollback/restart
qualification remain unfinished. Rewrite and FK job completion still retain
their explicit pending receipt paths. No original SQL case credit is claimed.
Client-preowned operation identity or durable request lookup is also still needed
when an entire public response is lost: a server-owned token is not proof that
the client received it. The lost-reply tests here cover metadata admission replies,
not that unresolved public transport boundary.

Qualification: frozen Debug metadata gate passed 60/60 steps with 68 publication
tests and one decoder barrier; the SQL/public gate passed 80/80 steps and four
selected regressions. Logs are
`/private/tmp/antfly-index-sql-admission-metadata-frozen-debug.log` and
`/private/tmp/antfly-index-sql-admission-http-frozen-debug.log`.
The admission test exercises 14 outcome modes, one begin only, exact recovery,
cleanup-before-23505, and tracked ownership: polling starts with at most one
bounded response buffer live, and all helper scratch is freed on return. A
separate test covers forbidden, canceled-before-begin and mismatched-token paths
with zero IO and no retained plan allocations. The SQL integration fixture uses
synthetic metadata/read callbacks with real compiler/candidate preparation; it
does not simulate a committed distributed build. It proves captured-group reads,
no schema-CAS bypass, pre-owned returned tokens and lost-reply reconciliation.
The public socket regression proves incarnation conflicts survive StatusSource.

The Debug pgwire suite passed 117/117 tests; the isolated PostgreSQL 18 oracle
passed all 16 lifecycle tests, including regular failed-build rollback. Initial
attempts required local socket access and the oracle's isolated psycopg dependency.
Generation, formatting, license, Apache, inventory, control-catalog and whitespace
checks pass. Two obsolete owned cached HTTP test binaries were reclaimed; source
and test data were preserved. No whole-request RSS or latency benchmark is
claimed. Original inventory remains 483 implemented / 862 unresolved.

### Request-shaped durable index cancellation

Pgwire cancellation and statement deadlines now enter a bounded, separately
budgeted recovery lane for the exact admitted index job. The lane preserves
request authority, clears only the expired client cancellation/deadline, and
uses a two-second cleanup budget. HTTP bounded observation never cancels an
asynchronous build. Transport/executor failures without a client cancellation
or expired statement deadline do not manufacture cancellation intent.

Each cancel CAS binds table/job/revision plus the observed metadata incarnation
and immutable plan digest. Metadata checks that guard in the same transaction
as cancellation, including apply/replay, not only in a preceding read. Protocol
floor 54 adds an explicit guard-presence encoding; older private cancellation
bytes are rejected and all index-publication stages use the coordinated floor.
Current owner-failure workers retain their current-authority cancellation form.

After a lost cancel reply, the lane observes only the exact job. It never repeats
the same revision-CAS. A strictly newer committed pre-publication head can admit
a new CAS because the old expected revision can no longer match. Cancellation
admission is not rollback: definitive cancellation is returned only after the
terminal canceled head proves cleanup and fence release. Irreversible publication,
unmirrored/foreign observations, or exhausted cleanup budget preserve a pending
receipt and token rather than claiming abort. Unknown admission remains unknown
unless an exact confirmed observation establishes the job.

This connects request cancellation to the durable job, not complete real-owner
wire/distributed cancellation qualification. Hosted/Raft execution, replacement
root adoption and client-preowned recovery for an entirely lost public response
remain open. Original SQL dispositions are unchanged.

Final Debug qualification passed 98/98 build steps in
`/private/tmp/antfly-index-sql-cancel-atomic-debug.log`: 69 metadata/publication,
four authenticated transport, four standalone DDL and one decoder-barrier tests,
without failures or leaks. The interruption fixture covers successful cleanup,
lost and unresolved cancel ACKs, strictly newer CAS cuts, irreversible/published
jobs, foreign/unmirrored/absent observations, statement deadlines, and HTTP
observation that performs no cancellation IO. The real metadata snapshot/restart
fixture round-trips guarded commands, rejects every truncation/trailing bytes,
rejects wrong incarnation/digest during both preflight and status, and verifies
the apply transaction leaves the job head unchanged for either stale guard.
Formatting, license, Apache, inventory, control-catalog and whitespace checks pass.
No new original-case credit or complete wire/distributed rollback claim is made;
the original inventory remains 483 implemented / 862 unresolved.

### Read-only native restart obligations

Tracing actual startup confirms that standalone's public store projection is
synthetic and DataServer's registration/report publisher returns early without
remote metadata. Durable native index admission requires a real current store
reporter, physical-root signing identity and explicit root enrollment; the public
listing cannot supply that evidence. Automatic startup activation remains off.

The metadata owner now exposes a read-only restart observation sharing apply's
signed identity, native binding, decoder-floor and reporter-CAS preimage checks.
One read snapshot captures the standalone revision, request/header/baseline
commitments, enrollment and outbox status, active generation, pending upload
cursor and retired-generation GC cursors. It does not enumerate report members,
decode upload fragments, stage commands, replace a reporter or enroll a root.
Noncanonical legacy addresses, active/pending/retired aliases and invalid cursors
fail closed. This observation is not a lease, admission grant or GC instruction.

Debug qualification passed 60/60 build steps in
`/private/tmp/antfly-native-restart-observation-final-debug.log`: 70 publication
tests and one decoder-barrier test, without failures, skips or leaks. The new
fixture signs with an actual persisted physical-root key, reopens metadata and
the root, and preserves deliberately undecodable report membership, incomplete
upload bytes and an unrelated user sentinel. Seven invalid durable state cuts
reject through the storage read path without repairing or deleting their bytes.
The signed foreign-incarnation/stale-reporter cases reject without revision
changes. Inventory, license and compilation-boundary checks pass.

This is bounded-access source evidence, not a cardinality/latency benchmark or
full real-owner SQL qualification. The exclusive startup lease, coordinated new
generation format, bounded reporter turnover/upload retirement, actual source
reporting and end-to-end SQL build/rollback/restart gates remain unfinished. The
previous safety restriction on production retirement is unchanged. No original
case disposition changes: 483 implemented / 862 unresolved.

### Native physical-root process ownership

Native standalone now retains an exclusive `std.Io` file lease on the replica
root before admitting its physical native binding. This closes the gap in the
catalog-path operator lock: independent catalog directories pointing at the same
replica root previously had independent catalog locks. The root lease lives
until DataServer and metadata work have shut down. Its stable inode is never
truncated or unlinked by production code. Opening uses nonblocking exclusive
locking and rejects a symlinked sentinel; subsequent checks detect a missing or
replaced sentinel. Durable root UUID and signing checks remain separate.

Index coordinator authority requires the held lease. A configured native
receiver's shared owner-decision read also requires and checks it before any
storage effect. That filesystem validation runs outside the metadata mutex.
Metadata-only observations do not invent a physical receiver or acquire a lease.
Hot-standby/unsupported native execution remains guarded.

Final Debug qualification passed 79/79 build steps in
`/private/tmp/antfly-native-root-lease-ci-debug.log`: nine linked native tests
and four standalone relation/DDL tests, without failures, skips or leaks. The
linked suite includes the new independent-catalog ownership test, both lease
unit tests, and all existing initial/ordinary FK, external MATCH PARTIAL,
cancellation and native TRUNCATE/graph restart fixtures. The new receiver test
rejects before job lookup when ownership is absent; failed contention preserves
the second catalog's revision/binding, and complete shutdown permits the next
owner to acquire the same physical UUID. Existing CI storage/integration gates
select these tests through the same linked native artifact.

An earlier sandbox run passed ownership and ordinary native cases but rejected
two HTTP fixtures at loopback bind with EPERM; the final gate uses socket access,
with no implementation or expectation changes to bypass that restriction.
License, inventory and compilation-boundary checks pass. No latency benchmark
or original-case closure is claimed. Coordinated generation-format activation,
bounded reporter turnover/upload retirement, production reporting and complete
SQL index build/rollback/restart qualification remain unfinished. Reporter
replacement and report deletion are not automatically activated by this lease.

### Pinned native producer restart preparation

The read-only restart observation now crosses the same owned JSON projection
boundary as other compiled metadata controls. Native standalone preparation
captures the real durable header, signs with the actual physical-root key and
observes the exact request through that owner. It requires the held root lease;
node/store mismatches and unsupported hosted/Raft compositions fail closed.
No backend handle, signing seed or report inventory crosses the projection.

DataServer pins its first signed startup request for the producer's lifetime.
Repeated preparation returns that exact body, including the original optional
reporter preimage, even after its own admission is observed. A stale producer
cannot sign a successor's preimage by polling again. A fresh producer may capture
the actual durable predecessor under the retained exclusive lease. Signing-key
creation is restricted to roots with no known registered or pinned verifier;
missing known keys are recovery errors, never silent key rotation under a UUID.
Filesystem work remains outside the metadata mutex; the pinned request owns
only fixed-size identity/signature fields.

Final Debug qualification passed 89/89 build steps in
`/private/tmp/antfly-native-reporter-pinned-preparation-debug.log`: nine linked
native tests, 70 metadata/publication tests and one decoder-barrier test, with
no failures, skips or leaks. The native fixture exercises stable preparation,
exact confirmation recovery, fresh-producer preparation with the same physical
key, stale-producer conflict after a successor commits, missing-key preservation
and foreign metadata incarnation rejection through the compiled owner. Its
registration writes seed only an isolated empty test registry using the existing
primitive; they do not qualify bounded turnover of populated report generations.
License, inventory and compilation-boundary audits pass.

This completes more of the lease-bound preparation boundary, not automatic
production registration/reporting. No generation address format or retirement
writer changed. Production namespace switching and bounded retirement remain
subject to the prior safety restriction; explicit approval has been requested.
Cardinality/latency benchmarks, source reporting, real-owner SQL build/rollback/
restart and the broader SQL pools remain open. Inventory is unchanged at
483 implemented / 862 unresolved.

### Exact original output-alias contracts

Eight previously unqualified originals now have exact PostgreSQL and mounted
native diagnostic evidence: `sql-1228`, `sql-1233`, `sql-1234`, `sql-1235`,
`sql-1238`, `sql-1239`, `sql-1333` and `sql-1582`. The PostgreSQL standard does
not expose SELECT output labels inside HAVING or compound ORDER BY expressions;
bare sort labels retain their separate output-name resolution. See
[PostgreSQL SELECT](https://www.postgresql.org/docs/18/sql-select.html).

The current binder already implements that separation. No replacement binder,
blanket unsupported error or statement rewrite was introduced. The new fixture
loads unchanged original SQL and parameters by ID, uses the complete shared
typed relational profile and an explicit document-table schema, and expects
SQLSTATE 42703. A fail-fast data-source adapter proves rejection before lookup,
scan, query or statement-view acquisition. Two native/PostgreSQL positive
controls distinguish a bare output label from a compound input-column sort key
using complete values and the integer result type. The oracle additionally
checks each missing identifier and preserves both source table populations.

The first test-selection audit correctly rejected an unregistered fixture; that
empty selection is not evidence. Explicit API-root registration then passed
80/80 Debug build steps. Both registered ledger gates subsequently passed in
`/private/tmp/antfly-sql-original-name-resolution-evidence.log`: the mounted
name-resolution test, the existing positive 12-original aggregate campaign and
the PostgreSQL 18+ oracle. No skips, native failures or leaks were reported.
License, inventory and compilation-boundary checks pass.

Only these eight historical positive alias contracts are marked superseded by
their tested PostgreSQL-required diagnostics. They are not counted as newly
implemented features or as product-specific unsupported-case rejections.
Inventory is now 483 implemented / 136 rejected / 113 superseded / 854 unresolved.
Production reporter namespace switching/retirement remains disabled pending the
requested explicit approval; broader SQL execution gaps remain open.

### Original join cohort: schema-bound USING/NATURAL and native bag semantics

The 22-original `sql_join_read_campaign.json` executes unchanged source SQL
through mounted HTTP handlers over two immutable typed local tables. A fresh
PostgreSQL 18+ process independently verifies column names/OIDs, values, SQL
NULL flags and complete ordered peer frontiers. Distinct physical keys preserve
duplicate logical rows; the witness includes missing customers, NULL keys,
missing amounts, nullable Boolean cells and cross-table unmatched customers.
OFFSET cuts are independently checked not to split a peer group. Golden output
generation fails closed rather than overwriting a reference with exclusions.

This campaign found actual parser gaps in `JOIN ... USING` and `NATURAL JOIN`.
The AST now preserves declarative keys until catalog binding. Binding produces
ordinary equality predicates through shared composite hash-key extraction,
plus typed merged-key projections. Unqualified keys and plain stars expose one
merged cell; qualified names/stars retain each original input key and order.
INNER/LEFT/RIGHT/FULL, composite keys, chained joins and USING aliases share
this machinery. NATURAL discovers only visible keys on both inputs, never an
implicit physical row ID. An explicitly named hidden ID remains addressable.
Missing, duplicate and ambiguous keys, and colliding USING aliases, fail before
capture; alias collisions use PostgreSQL SQLSTATE 42712.

Merged projections participate in cold-column dependency propagation. At the
statement boundary, a validated pure merged projection is eliminated if no new
identity is consumed (for example COUNT(*) or qualified-only projections).
This occurs after binding and does not change nested join ordinal layouts or
skip validation. A 256-by-256 duplicate-key work-limit test proves composite
hash extraction remains enabled and allocation counts stay below 4096 and
within twice the equivalent ON-join count plus 512. This is an allocation-work
regression, not a throughput/latency benchmark claim. Exhaustive allocator-fault
coverage checks coordinated-capture release, alongside existing outer-join,
CTE, snapshot and residual-predicate regressions in Debug.

Only these 22 exact originals receive implemented credit: inventory is now
505 implemented / 136 rejected / 113 superseded / 832 unresolved. The scope is
local SQL semantics and capture ownership; it does not qualify concurrent
distributed publication or close broader activation/SQL architectural gaps.

Validation records: `/private/tmp/antfly-sql-using-regressions-debug-closed.log`
(14 selected tests, no skips/failures/leaks),
`/private/tmp/antfly-sql-original-joins-evidence-closed.log` (registered mounted
native and fresh PostgreSQL gates), and
`/private/tmp/antfly-sql-join-reference-guards-final.log` (three oracle guards).
Inventory integrity, license headers, Apache product boundary and storage/
control-catalog compilation-boundary audits are separate from release readiness.

### Typed streaming array expansion

The next large document-read cohort depends on array expansion. FROM UNNEST
now has an explicit schema-independent AST and a typed relational operator,
instead of treating a row producer as a scalar call or expanding JSON documents.
Binding shares existing parameter, lexical-scope, shape, decision-capability,
cold-projection and coordinated-capture boundaries. It retains scalar element
identities, including integer widths, UUID, exact NUMERIC and JSONB. EXPLAIN
exposes an Unnest node. Recursive dependency and subquery-scope visitors see
function inputs rather than hiding their references.

Execution evaluates each invocation's input arrays once and streams flat
storage-order elements. Multi-array calls zip to the longest input with SQL
NULL padding; empty and NULL arrays emit no rows by themselves. WITH ORDINALITY
starts at one regardless of array lower bounds. Implicit/explicit lateral
inputs and LEFT null extension use the existing Apply owner and captured cut.
Only emitted cells cross the output ownership boundary, not whole duplicated
arrays per expanded row. Expanded rows are charged to a statement-wide quota,
with cancellation checkpoints and allocator-failure cleanup.

Direct invariant UNNEST operators bypass eager expanded-row replay: LIMIT 1
over a prepared 10,001-element array succeeds with a one-row expansion quota,
both alone and beside a captured table. Scalar input preparation/work limits
remain independent and unchanged. Generic invariant derived-query replay and
other set-returning functions remain separate architectural work; this
checkpoint does not claim every enclosing plan is prefix-lazy.

An owned PostgreSQL 18+ server independently verifies 17 shared operator
contracts, including stored arrays, multi-dimensional/lower-bound order, exact
NUMERIC, UUID, JSON null versus SQL NULL and lateral outer joins. The native
executor consumes that same reference at page sizes 1, 3 and 64, checking exact
element OIDs, values and NULL flags. Mounted HTTP tests use actual typed storage
and bounded native captures. These references preserve bigint/NUMERIC wire
strings exactly; they never round them through a floating-point matcher. The
oracle uses a read-only server-side cursor and bounded fetches. PostgreSQL also
establishes that only the unqualified multi-array spelling is grammar sugar;
schema-qualified UNNEST retains its single-array signature.

Validation: `/private/tmp/antfly-sql-unnest-stored-debug-closed.log`,
`/private/tmp/antfly-sql-unnest-public-debug-closed.log` and
`/private/tmp/antfly-sql-unnest-oracle-check-final.log`. The native gate includes
exhaustive allocation-failure ownership and row-admission tests; the HTTP gate
also reruns all 22 previously qualified original joins. No throughput claim is
assigned to these work-limit tests.

No original case dispositions change: 505 implemented / 136 rejected /
113 superseded / 832 unresolved. In particular, the document-view/index-backed
UNNEST originals still need their actual mapping/catalog and indexed producer
paths proven end to end; typed relational inputs are not a substitute for that
contract. Persisted document SQL views, index-proof pushdown, ROWS FROM/other
table functions, and broader distributed activation remain unfinished.

### Replay ownership and prefix demand across expansion boundaries

A new negative workload reproduced a real gap: wrapping UNNEST in a lateral
derived SELECT caused invariant expanded rows to be eagerly materialized before
LIMIT 1. The 10,001-element prepared-array case failed its one-row expansion
quota in `/private/tmp/antfly-sql-unnest-derived-baseline.log`.

Bound relational nodes now carry a conservative replay policy: captured,
restartable, or streaming. This is an ownership/effect property, not an estimated
row count or a claim that every operator can stream. Binding computes it once
from child owners and bound expression volatility, including hash keys and
late-attached Apply demand masks. Pure expansion projections/filters preserve
the streaming property. Physical cursors, explicit materialization, recursive
state, volatile programs, and unclassified blocking operators retain their
existing replay owner. Thus a derived expansion may restart from its pinned
values, but an exhausted native storage cursor may never be reopened as a
shortcut to replay.

Apply passes consumer demand to its right iterator. Residual filtering also
preserves demand when its input is restartable expansion, without reducing
physical-storage prefetch to one-row I/O. Demand remains advisory: rejected
rows keep pulling until a match, exhaustion or a real quota/error boundary.
Tests prove that division by zero beyond a delivered LIMIT prefix is deferred,
but occurs when that row is requested. Classification tests explicitly retain
captured ownership for physical reads and volatile AI programs. No executor
limits were raised or input validation skipped. The wider audit below returns
four stale parser-negative dispositions to unresolved; current accounting is
505 implemented / 132 rejected / 113 superseded / 836 unresolved.

The generic long-term follow-up is resumable, bounded prefix replay for
captured/volatile producers, with independently owned readers, spill admission
and fault coverage. This checkpoint preserves those owners; it does not replace
them with unsafe rescanning or claim that every enclosing plan is prefix-lazy.

The full SQL gate also exposed stale compile-only rejection evidence for
`sql-0834`, `sql-0835`, `sql-0879` and `sql-0880`: UNNEST grammar now admits
these complete statements. Those four entries return to unresolved, not
implemented or superseded, until their actual document/index producer contract
is qualified. The immutable source inventory and source rejection expectations
remain unchanged. A dedicated grammar-only test records admission without
granting parity credit. The rejection harness now releases unexpectedly
successful compiled owners and still fails its gate, rather than leaking them
while reporting only the first stale expectation.

Validation: `/private/tmp/antfly-sql-expansion-replay-full-debug-recovered.log`
passed 21/21 build steps, with 834 core tests and 227 extracted SQL contracts,
zero failures/leaks and three opt-in benchmark skips. Mounted HTTP expansion
and the 22-original join cohort passed 80/80 Debug build steps in
`/private/tmp/antfly-sql-expansion-replay-public-debug-recovered.log`.
`/private/tmp/antfly-sql-replay-deterministic-witness-debug.log` checks the final
focused fixtures, and `/private/tmp/antfly-sql-expansion-demand-postgres-final.log`
independently confirms PostgreSQL's filtered prefix and SQLSTATE 22012 when the
zero-divisor row is requested. That error witness uses one parent so unordered
join orientation cannot duplicate safe rows and hide the requested error;
multi-parent 10,001-element demand/quota tests remain separate.

### Document SQL-view integration: catalog boundary still required

The current catalog has no persisted view resource. The mega branch discovers
mapped view names by scanning a full table snapshot and reading mappings from
index JSON. That resolver must not be ported as the production architecture.
The desired shape is an indexed, namespace-owned view definition, resolved in
the same catalog epoch as stable source identities, schema layouts and any
index-generation proofs. Its definition and source bindings belong in the
durable catalog, not an implicit index side channel or a global table scan.

SQL binding should expand the pinned view as an authorized derived relation,
retaining dependency identities, RLS/security mode, projected paths and typed
output descriptors. PostgreSQL view-owner versus security-invoker access needs
an explicit owner contract; it must not be approximated by a mutable adapter's last table or
by granting the caller arbitrary physical-source access. Rename/drop/recreate
and source-layout changes must fence stale bindings before opening data cursors.
Public resource/type additions should come from OpenAPI and shared catalog
machinery when that owner contract is implemented. View access and RLS follow
the [PostgreSQL CREATE VIEW rules](https://www.postgresql.org/docs/18/sql-createview.html),
not the superficially similar SECURITY DEFINER function model.

Document JSON arrays remain JSON on ordinary table reads. A mapped SQL-array
output needs an explicit schema-bound conversion and item type, never inference
from sampled payloads. Index-backed originals need real native candidate/proof
paths plus residual evaluation against the captured cut; ordinary relational
scans over a similarly named fixture are not their evidence. These catalog,
authorization, conversion and producer paths remain unfinished.

### Private SQL-view definition preparation

`sql/view_definition.zig` now prepares owned read-only definitions through the
existing compiler and semantic binder. Its resolver has only authorized point
metadata lookup and cancellation capabilities, not data readers or mutation
callbacks. Repeated lexical references resolve once. Distinct qualifications
retain their SQL identities, while references to one physical table must agree
on logical identity, schema version and digest within one catalog revision.

The retained artifact contains SQL text, stable owner identity, security mode,
ordered output names and exact builtin/array/NUMERIC descriptors, and sorted
source identities with publication guards. Parameters, writable statements,
duplicate output names and inconsistent metadata fail before activation.
Source count, query size, syntax complexity and combined temporary/retained
allocation capacity are bounded. Compiler/binder scratch is released before
returning the artifact; it is not retained with saved definitions. Cancellation
and allocator failures release both regions before ownership transfer.

This is private preparation, **not persisted-view activation**. Source version
and digest are publication guards, not a permanent ban on compatible source
schema changes. Durable namespace admission, stable column dependencies,
rename/drop/replace handling, current owner/RLS delegation, authorized view
expansion and generated public APIs still need integration. No original SQL
case dispositions change at this checkpoint.

Validation: `/private/tmp/antfly-sql-view-definition-full-debug.log` passed the
complete Debug SQL gate (841 core tests, 227 extracted SQL tests, three existing
opt-in skips, zero failures/leaks). The subsequent same-identity consistency
guard passed the final eight-test focused Debug run in
`/private/tmp/antfly-sql-view-definition-final-debug.log`. A fresh owned
PostgreSQL 18.6 experiment in
`/private/tmp/antfly-sql-view-definition-postgres.log` confirmed partial output
alias lists, exact integer/array/NUMERIC descriptors, duplicate output rejection
and excess-alias rejection. It did not qualify stored-view execution or RLS.

### Captured Apply prefix replay

Invariant captured inputs under Apply now retain one suspended iterator instead
of draining it before opening the first reader. A reader at the frontier pulls
one new row, appends an owned replay image and advances. Other readers consume
that image without reopening physical storage or repeating producer effects.
EOF closes the producer once; a producer error is sticky at its frontier.
Abandonment closes suspended iterators before destroying their cached inputs.

The replay owner retains a reusable producer scratch arena. Growing memory
replay can promote to one typed sequential spill run while readers remain live.
Readers own delivered prefix rows across another reader's advancement and
promotion. Pending writer rows are read without flushing one physical record
per pull; readers catch up through subsequently flushed blocks in linear order,
without a per-row offset directory. Sealed replays retain their borrowed-row
fast path. Prefix append/read is explicitly serialized, not a concurrent-file
interface. Spill corruption, quota, cancellation and allocation checks remain
fail-closed; storage failures never authorize producer restart or RAM fallback.

Projection demand is separated from physical page admission: one-row evaluation
does not turn captured scans into one-row I/O. A 4096-row test source delivers a
LIMIT 1 prefix after one physical page without evaluating a later division by
zero, but LIMIT 2 reports that error. Its single-parent PostgreSQL witness avoids
unordered join-orientation ambiguity. A separate multi-parent test verifies one
captured iterator and replayed rows, with exhaustive allocation-failure cleanup.
Independent readers also cross pending-tail flushes and memory/spill promotion.

This activates prefix replay for the invariant Apply cache. Explicit
materialization and recursion-specific caches still use their existing eager
owners; migrating those requires their own lifecycle and fault qualification.
Persisted views remain gated on durable attribute identities, dependency
admission and current owner/RLS delegation. No original case dispositions change.

Validation: `/private/tmp/antfly-sql-prefix-replay-full-debug.log` passed the
complete Debug SQL gate (847 core tests, 227 extracted SQL tests, three existing
opt-in skips, zero failures/leaks). The focused prefix/expansion gate passed 14
tests in `/private/tmp/antfly-sql-prefix-replay-runtime-debug.log`; the full gate
was run after the final pending-tail checkpoint change. Mounted native
HTTP regressions passed 80/80 build steps in
`/private/tmp/antfly-sql-prefix-replay-public-debug.log`, checking all 22 qualified
original joins and 17 supplemental array-expansion contracts. The fresh owned
PostgreSQL 18.6 prefix/error witness is recorded in
`/private/tmp/antfly-sql-prefix-replay-postgres.log`. License, Apache boundary and
inventory checks pass. These work/lifetime tests are not throughput benchmarks.

### Shared materialization and resumable recursive frontiers

Relation-runtime statement/scoped materialized replay owners now use the same
prefix producer as invariant Apply inputs. The eager `staticRows` drain is
removed. Correlated producers are pinned to their highest referenced outer
epoch; scope retirement stops every producer before freeing replay records or
outer values. Nested materializations retain independent readers over the same
captured source, not repeated physical scans.

The first scoped regression exposed another blocking shape: the binder attaches
one known outer row through keyless CROSS JOIN nodes. A proven outer-reference
singleton now streams typed concatenation instead of building a hash table over
the physical side. This optimization requires a keyless, condition-free inner
or CROSS join with an actual outer-reference node. Outer joins, membership,
predicated joins and estimated cardinalities do not receive that shortcut.
The two-epoch regression admits only two relation rows and one physical page
over a 4096-row source, and checks fresh correlated values and allocation faults.

Recursive worklists now retain their seed/step iterator and resume at the
requested output frontier. Each recursive step sees a frozen complete delta;
newly emitted rows cannot feed that same step. UNION duplicate elimination and
UNION ALL multiplicities retain their existing semantics. Multiple consumers
share accumulated owned results without restarting seed or step evaluation.
LIMIT can abandon an otherwise unbounded recursive read; blocking sorts and
complete mutation-source consumers still request the required full input.
Suspended iterators are closed before cached readers, worklists and outer
epochs are retired. Failures are sticky and all current row/work/memory limits
remain in force. Worklist/visited rows still use admitted resident storage;
externalizing that storage and broader recursive coercion/grammar qualification
remain separate work, not claimed here.

Strict output-type assertions found a shared binder bug: VALUES/set inference
discarded concrete integer literal identities and widened int4 to int8. It now
uses the existing scalar literal resolver, keeping NULL/string unknown-type
selection separate. Narrow, wide, smallint and mixed integer cases have native
and PostgreSQL descriptor checks; correlated recursive seed widths are checked
too. No original case dispositions change.

The repeatable owned PostgreSQL gate is
`python -m unittest discover -s scripts -p test_sql_replay_prefix.py -v`
(with the scripts environment and psycopg installed). Its supplemental cases
check complete values, labels and OIDs for literal/set widths, materialized
prefixes, correlated epoch changes, recursive prefixes, DISTINCT cycles,
blocking finite ordering and combined materialized/recursive scope retirement,
plus deferred division-by-zero SQLSTATE 22012. These controlled heap/VALUES
witnesses are not an ordering guarantee for arbitrary unordered SQL plans.

Validation: the full Debug SQL gate passed 850 core tests and 229 extracted SQL
tests (three existing opt-in skips, zero failures/leaks) in
`/private/tmp/antfly-sql-materialized-recursive-full-final-debug.log`. The final
cleanup-order tightening was subsequently verified by 27 affected tests in
`/private/tmp/antfly-sql-materialized-recursive-cleanup-debug.log` and all five
LATERAL tests in
`/private/tmp/antfly-sql-materialized-recursive-lateral-cleanup-debug.log`.
Mounted native read/mutation, original-join and array-expansion regressions passed
three tests and 80/80 build steps after that change in
`/private/tmp/antfly-sql-materialized-recursive-public-cleanup-debug.log`.
The checked-in PostgreSQL oracle passes against owned PostgreSQL 18.6;
`/private/tmp/antfly-sql-replay-prefix-oracle-test.log` records the final source.
Ruff, formatting, license, Apache boundary and original-inventory checks pass.
No throughput speedup or additional original-case closure is claimed.
