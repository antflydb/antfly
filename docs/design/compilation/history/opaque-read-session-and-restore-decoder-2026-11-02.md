# Opaque read sessions and complete restore decoder operations

Status: the retained-session baseline passes 132 Debug build steps and its
frozen production cache matrix passes six phases. The subsequent Reader,
QueryReadLease and delayed-document capabilities pass their final guarded
128-step Debug aggregate, including five selected retained-read fixtures. Their
private-field cache qualification is pending. This does not complete all private
DB coupling.

## Retained read ownership

Relational/document sessions and relational statement snapshots now remain in
the storage owner. Issuance crosses the existing checked engine boundary; the
client receives a versioned kind-checked capability, immutable schema version
where needed, and complete-operation callbacks. Pulls, normalization, proofs,
checkpoints and close use the existing named callback ABI and explicit native
outcomes, preserving error names across independent error domains.

The client backend imports common values only. A build-selected source backend
owns private session/statement implementations. Canonical types bridges retain
one DB/Rows/value namespace rather than introducing a second physical graph.
Budget/Row/Page are extracted from the physical reader. Private implementations
and provider test imports no longer occur literally in the DB source file.

Complete pages have provider release callbacks. Physical payloads and native
SQL layouts remain in their provider arena; the response has a separate empty
arena for optional client metadata. Retaining a page shares one release lease,
and pages remain valid after statement/session close. Allocators supplied by
callers retain their identity and lifetime requirements. The exact stateless
libc allocator is canonicalized using owner-issued allocator metadata; custom,
quota, testing and arena allocators remain unchanged. IO receivers exist only
for a complete operation; no temporary IO adapter is stored in a session/page.

The original statement capture fence, immutable read fork, row-policy leases,
cancellation/deadline checks and donor lifetime requirements remain. Closing a
statement after opening a scan releases its own lease without invalidating the
scan's independent fork. No new per-record callback is introduced.

The private engine descriptor advances to 34. It removes concrete relational
and document session contracts only after no method transports those values.
Shared Handle/Operations/Page/Row layouts are checked instead. P5 adds explicit
PreparedCommitDisposition and named State contracts. Enum fields transported
by shallow layout contracts use explicit named types (including restore Format)
so compiler-generated anonymous type names cannot cause false mismatches.

## Restore lifecycle cut

The verified restore-source consumer previously received a DB by value only to
verify/close it, or to import a portable artifact, bind its admitted range and
close it. `materializeRestoreSourceDecoder` performs that complete disposable
lifecycle in the owner. No decoder DB or private store pointer escapes.

Staged-generation path/namespace checks, native manifest verification, portable
cohort proofs, cancellation, file close order, immutable range binding and DB
drain/close remain inside the operation. The caller retains staging authority,
marker creation, seal and publication. Existing DB open/close/query-cut APIs
and legacy layout checks remain for consumers not yet migrated.

## Compile-time callback lookup

The callback caller checked field existence through `std.meta.fieldIndex`,
performing a linear name scan for every instantiated engine method. The
presence-only check now uses `@hasField`; ordinal lookups remain where needed.
The existing callback/native/error ABI suite passes 47 tests, including its
expected diagnostic. This fixes an observed comptime quota failure; build-time
speedup remains unmeasured.

## Qualification and remaining work

The integrated receipt is
`.benchmark-results/compile-costs/opaque-session-restore-owner-debug-v3.log`: all
132 steps succeed, with three session fixture tests, 13 restore/decoder tests,
three opaque query tests and 57 storage owner tests passing. The callback ABI
receipt is `native-callback-field-check-debug-v3.log` in the same directory
(47/47 passing tests, one expected diagnostic). The independently compiled non-test probe
checks stale version/kind/unknown-kind/null handles before poisoned state,
retained exact integers and dynamic strings after close, cancellation, and
custom allocator cleanup. Source fixtures cover document page lifetime and
whole-batch normalization as well.

The strict production cache receipt is
`.benchmark-results/compile-costs/read-session-production-debug-cache-v2.json`.
Every phase completes 88/88 build steps and the three linked session tests.
Initial compilation builds all five observed owners; the warm phase caches all
five. Actual private field insertions into relational, document and statement
implementations each rebuild the physical storage engine and source fixture,
with a different engine archive SHA, while the production API kernel, storage
kernel and independent native session client remain cached. A shared Budget
field insertion rebuilds all five owners. These are real field/layout changes,
not comments or removed dependency checks.

The receipt freezes 4,960 Zig/construction files and 214 repository schema/script
inputs, records compiler version, source/field/log/archive hashes and preserves
raw output. The exact runner is retained beside it as `.runner.py`; its later
license-header normalization changes no qualification logic. Supplemental
`read-session-production-module-profiles-v2.json` records response-file hashes
and the actual API/kernel/native-client module roots: client backend and one
canonical types bridge, with no source-backend/SourceTypes module. The source
fixture selects the physical backend and explicit provider bridge. The first
failed receipt is retained: its overlay omitted repository-relative schema
inputs, and it did not satisfy the API/compiler or successful-build gates.
All isolated matrix caches are removed after completion. The v2 receipt retains
input hashes, not a full source archive or committed candidate; it cannot by
itself reconstruct the complete pre-Reader working tree for replay. Later Reader
qualification will retain compressed authored/construction and repository inputs
beside its receipt before deleting its isolated build cache.

This immutable receipt covers the retained-session/statement graph before the
subsequent raw Reader and boxed QueryReadLease extension. That extension needs
its own typed-page/ranked behavior and private-field cache qualification.

## Reader, query lease and delayed-document capabilities

The actual CAPI SQL SearchCursor now receives an owner-issued relational
Reader. Its typed/JSON pages, native ranked microbatches and single lookups are
complete owner operations. Request/filter/bound/continuation values live in
`read_reader_values.zig`; the private Reader is imported only by the physical
backend. Existing retained RowFilter callbacks keep their named error boundary;
this migration adds no per-record foreign callback.

QueryReadLease likewise remains boxed in the owner. Its source-only state keeps
the original apply fence, foreground-query admission and row-policy lease.
Release preserves the original unlock, foreground completion and policy-release
order. Complete ranking restores the caller's original native predicate in the
captured request before its temporary receiver disappears. Readers clone their
own snapshot/policy state and remain valid after the writer-excluding lease
releases; a new write can proceed while those readers retain the earlier cut.

Delayed document hydration uses another capability for single lookup, bounded
batch, projected batch, explicit releasePage and close. The original metadata
proof cache, directory/range scopes, repair request, byte accounting and fallback
algorithm remain physical. Source/FieldValue/Batch/ProjectedBatch and the named
SourceKind tag are pure shared values. The existing borrowed-window rule stays
explicit: consumers copy selected cells before the next lookup or releasePage.
An LSM projection may consume one row while a range-capable backend consumes
more; callers advance by the returned consumed count.

The native engine table no longer transports concrete Reader, QueryReadLease or
DocumentRows state. The corresponding private frame checks are removed only
after those returns/borrows disappear. Version/compiler checks, public value
contracts, operation signatures and context alignment checks remain. Finite
supplemental guards cover the release pair and TypedCells/Datum in addition to
page/row/request/filter/layout values. Reachable NUMERIC/SQLARRAY values retain
finite guards for numeric.Value/Kind, array.Value/Dimension, builtin Type tags
and stored array View/Validation headers. Their immutable payloads preserve
exact NUMERIC scale, SQL NULL and array bounds; they are not coerced to JSON.
TypedCells.Columnar is named to avoid compiler-generated type identities, but
this does not authorize its private spill graph to cross the boundary.

Actual retained-read producers materialize cells: their TypedCells.columnar is
always null. The provider now checks that invariant before exporting any whole
page, point or ranked batch. It also rejects Datum.patterns before touching its
pointer, including patterns/nested-array slots in flat SQL array elements.
Source View.Pages must have null lease, no retained_pages and no columnar_blocks.
These fail-closed checks prevent private spill/JSON/pattern owners or page lease
state from escaping. A poisoned-metadata regression verifies rejection without
following those pointers. They are finite representation rules plus identified
shared value guards, not recursive private graph checks.

The independent non-test client fixture now covers retained typed pages after
reader close and a later write, borrowed IO, caller allocator cleanup, retained
foreign-filter errors, exact integers above 2^53 in ranked/point cells, lease
version/null/alignment rejection and restored callback identity. A second native
fixture covers actual dense callback success/failure, immutable document reads
after write, projected Source.fields, repeated bounded windows, invalid requests,
explicit page retirement/reuse and copied data after reader close. Two source
fixtures retain the earlier relational/document ownership tests.

`opaque-retained-reader-session-debug-v5.log` records all four fixtures passing
with 78/78 build steps and no leaks. The preceding integrated
`opaque-retained-reader-owner-debug-v2.log` also passes the three opaque query
tests, 13 restore tests and both native storage-owner linkage executables; only
the then-unfixed document fixture assertion failed. The final finite-guard aggregate is
`.benchmark-results/compile-costs/opaque-retained-reader-final-debug.log`: all
128/128 steps pass, including four retained-read fixtures, three opaque query
tests, 13 restore/decoder tests and both native storage-owner linkage executables.
Every fixture reports zero leaks. The guards are therefore checked across
independent provider/client compilations, not only within one test image.

The final concrete nested-value/export-invariant qualification is
`.benchmark-results/compile-costs/opaque-retained-value-export-final-debug.log`:
128/128 steps pass, with five retained-read fixtures, three opaque queries,
13 restore/decoder tests and both native linkage executables. The native
relational fixture reads exact NUMERIC 9007199254740993.2500 and a stored int64
SQLARRAY containing 9007199254740993/SQL NULL with lower bound -3 through typed
page, single lookup and ranked batch after reader close. The fifth fixture
rejects poisoned columnar/pattern/page-lease metadata before pointer access.
It reports zero leaks; all earlier four-fixture receipts precede these final
nested-value guards and do not establish this final candidate by themselves.
The real CAPI SQL Adapter/SearchCursor is exercised from both native consumer
profiles with the exact integer 9007199254740993; its JSON bigint representation
is compared losslessly. Test preparation creates the dense index in the source
fixture with normal runtimes, while native calls exercise the retained reads.

## Final retained-read cache qualification

The six-phase retained-read matrix covers initial/warm compilation, actual
private Reader/QueryReadLease/DocumentRows field insertions and a shared Reader
Request field. It requires the actual API kernel, storage kernel and independent
native client archives to remain cached for private edits, while the physical
engine and source fixture rebuild and the engine archive hash changes. Shared
request edits must rebuild all five participants. Every phase must execute all
five retained-read fixtures and finish its complete build summary.

The first committed-candidate attempt,
`retained-read-production-debug-cache.json`, is **cache-invalid**, not a source
or ABI failure: the shared main cache's o/h/global trees disappeared during the
build and the inference dependency could not open its emitted object. All five
observed compiler artifacts built, but the dependency failure prevented fixture
execution and no phase was accepted. The raw log and exact source-only tar are
retained; no eviction time or deleting process was identified by that attempt.

The identical-program retry adds active leases across the main/global/overlay
cache roots and output/argument directories. Markers pulse every five seconds;
each poll checks root identity and marker presence. Loss invalidates the matrix
and terminates only its owned build group, with cache state/file listings and
compiler arguments preserved before overlay cleanup. Eight qualification-tool
regressions cover attribution, exact test counts, source/archive exclusion and
actual eviction/owned-process termination. The retry's source hash map confirms
all 4,776 program/construction inputs and 214 repository inputs match the first
attempt; only the two qualification Python files differ.

The protected final receipt is
`.benchmark-results/compile-costs/retained-read-production-debug-cache-retry1.json`:
**all six phases pass**, each with 88/88 build steps, five passing fixtures and
zero leaks. Initial compilation builds all five participants; warm compilation
caches all five. Each actual private Reader, QueryReadLease and DocumentRows
field edit preserves all three client archive caches, rebuilds the physical
engine/source fixture and produces a distinct engine archive SHA. The shared
Reader Request field rebuilds all five participants. No lease was lost or cache
eviction detected in this accepted run.

The exact hardened runner and compressed source-only snapshot are retained as
`retained-read-production-debug-cache-retry1.runner.py` and
`retained-read-production-debug-cache-retry1.sources.tar.gz` beside the receipt.
The snapshot contains 4,880 authored/construction/tool inputs, 214 repository
inputs and its manifest, with no caches/build artifacts. Its 40,790,723-byte
archive SHA is
`9eda600b7eb33d04e0b75e6f59fca091eddb310538367eae9a5f5740ea28a356`.
The recorded program baseline is commit `619d435205`; qualification hardening
was subsequently committed as `26151fa366`. The retained program-identity
sidecar proves the retry's program and construction inputs match the original
candidate. Compiler response files and module-root inventories are retained in
`retained-read-production-retry1-compiler-args/` and
`retained-read-production-retry1-module-profiles.json`: actual API/kernel/native
clients select the client backend without a SourceTypes module, while the source
fixture selects the physical backend/provider. Private relative-import
exclusion is established by the real field/cache phases, not solely that
module inventory.

All isolated overlay caches and this run's active lease markers were removed
after acceptance. These Debug checks establish ownership and cache isolation;
they do not measure serving performance or whole-product Release improvements.

## Remaining private boundaries

StatementReadFence still transports two SnapshotAdmission.CaptureLease values
through statement/backup capture and release. A subsequent bounded capability
could preserve busy/null handling, replay-before-primary release and coordinated
all-participant retry behavior. It has not been migrated here.

openQueryCut still returns a DB by value to local_query_provider and
native_retained_cover. The cover stores optional DB values and exposes a
ResidentDbLease containing a DB pointer. A larger next cut should keep the whole
retained-cover/query operation in its owner while preserving continuation,
namespace, cancellation and candidate-budget checks. General DB open/close,
restore setup, DB/Core, index/runtime/resource and registry/storage contracts
remain; their existing checks are retained. Full DB lifecycle conversion is a
separate step, and the complete P4 exit condition is not yet met.

No Release size, total memory, cold-build speedup or serving-performance claim
is made from these Debug checks.
