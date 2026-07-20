# DB Contract And Roadmap

This note is the DB-layer landing page for contract decisions, storage backend
boundaries, and local-shard execution roadmap work.

For the canonical enrichment architecture and artifact identity contract, see
[ENRICHMENTS.md](ENRICHMENTS.md).

## DB Handle And Runtime Ownership Contract

`DB.open()` must not make the caller's role ambiguous. A DB handle can be the
authoritative write owner for a shard, the shared read owner for a visible
generation, or a tightly scoped foreground maintenance handle. Those roles have
different side-effect budgets and must be leased by the serving layer instead
of opened ad hoc.

The serving-layer target shape is:

- one cached write DB owner per table group
- one cached read DB owner per table group and visible root generation
- many read transactions, cursors, lookups, scans, queries, and status reads as
  leases on that read owner
- short-lived maintenance DB opens only inside a callback scoped to an
  exclusive table/group transition lease; raw DB handles cannot escape the
  callback

Split/merge projection work uses a group-exclusive transition lease. It queues
ahead of later writers, briefly drains reads while managed auto-bulk state is
published, then reopens reads from the published generation for snapshot scan
and repair. Sustained traffic therefore cannot starve a transition, and an
instantaneous cache-idle probe is not used as quiescence.

The important restriction is that "read profile" must not create a second cached
DB identity over the same table-group root. Lookup, scan, query, and status may
use different methods or lazy capabilities, but they should share the same read
owner for a generation. Multiple independently cached DB instances over the
same path duplicate index/cache/runtime state and make restore, drop, and schema
mutation correctness depend on every caller remembering the same invalidation
sequence.

A point lookup is the narrow exception to routing all reads through the cached
read owner: when the generation-matched writer/apply DB is already resident, the
lookup leases that owner and reads its primary document store directly. It must
not open the cached query DB merely to load one document, because doing so loads
the complete index catalog and creates an additional backend generation over a
path undergoing WAL publication and compaction. Query-only processes and cold
groups fall back to the generation-owned read cache. The lookup source must
return `null`, rather than adopt a mismatched owner, when its visible root
generation or identity namespace does not match.

Restore preparation and generation publication use separate capabilities.
Preparation allows existing and new readers to continue against the live
generation while Antfly imports, repairs, validates, and syncs an isolated
sibling. It blocks new writes for the restore's full lifetime so acknowledged
writes cannot be discarded when the prepared backup is published. Publication,
drop, and in-place schema mutation use an exclusive
transition lease that:

- blocks new table reads and writes
- drains existing read, write, and pending-open leases
- invalidates read, write, startup-write, runtime-status, and shared
  path-scoped storage caches for the affected generation
- performs only the live file/catalog mutation and unavoidable live-generation
  repair under the exclusive lease
- publishes the new visible generation only after repair/invalidation complete

Restore never reconstructs the live root in place. It acquires a process-wide
preparation capability for the exact shard root, creates a unique sibling
staging generation, imports and validates the primary store there, runs
foreground derived/runtime repair against that staged path, closes and
recursively syncs and seals the staged generation, and only then drains serving
leases and promotes preparation to the exclusive publication capability. A
prepare or repair failure destroys staging and leaves the live generation
untouched. macOS and Linux use
atomic directory exchange when a live generation exists. Platforms or
filesystems without atomic exchange reject replacement before mutating the live
namespace. After durable publication, the exchanged old root is submitted to
the shared backend runtime's cleanup lane; recursive reclamation is not part of
the admission-critical publication path.

Data-Raft snapshot apply follows the same generation boundary. The apply path
validates the canonical snapshot stream without materializing document values
or a descriptor per document, builds a fresh staged DB in fixed-size borrowed
batches, syncs and seals it, then atomically publishes it under a group-scoped
generation capability. It never scans or deletes the old live table. Durable
data-Raft projections are physically partitioned by group, while a root-level
coordinator retains the single process writer authority. This makes snapshot
replacement proportional to the incoming group snapshot and avoids rewriting,
describing, or retaining keys for unrelated groups.
The durable apply-store projection uses a per-group generation-preparation
marker rather than holding its hash-stripe mutex while importing the snapshot.
Operations for the target group wait; unrelated groups continue to apply and
serve even when their IDs collide in the same lock stripe. Only reader drain,
cached-owner retirement, atomic exchange, and allocation-free watermark
publication execute under the stripe lock.
Shared cache-generation bookkeeping is reserved before staging and advanced
without allocation immediately after the namespace exchange, so no fallible
bookkeeping step can leave the new root visible under the old cache identity.
Schema validation is parsed once per snapshot and applied per bounded batch;
derived replay remains durable in the staged generation for normal workers to
resume after publication. Staging excludes only applies for the target group;
other groups, including groups in the same table, continue serving and applying.
Publication retires and drains only the target group's read/write owners and
uses an exact group read-cache admission fence. Table-wide drop, schema, index,
and restore transitions wait for every group preparation and retain table-wide
epochs.

The install's catalog inputs form one explicit contract: metadata Raft group
identity, the metadata cluster's durable 128-bit incarnation, table identity,
table name, schema, index catalog, range topology,
and document-identity namespace are captured after group admission. The
snapshot's encoded range must match that catalog range. Staging owns only this
compact contract, not the complete admin snapshot, so its memory cost is
independent of cluster catalog size. After
serving leases drain, the candidate is atomically exchanged into the filesystem
but remains outside serving admission. The catalog source then performs a Raft
linearizable read; followers forward ReadIndex to the metadata leader and wait
until the returned committed index is applied locally. The compact contract
is compared on the metadata replica, so the publication request and allocation
cost remain independent of the cluster catalog size. It must match that
authoritative projection exactly. A mismatch atomically
restores the retained prior root before admission reopens; a match commits the
exchange and advances the visible generation. Metadata changes committed before
the barrier therefore reject stale publication, while changes committed after
it are correctly ordered after publication. Unrelated metadata changes do not
force a retry.

The fixed metadata Raft group id is not a cluster identity. Metadata initializes
its incarnation exactly once through the replicated state machine; the singleton
projection is included in Raft snapshots and therefore survives restart,
leadership transfer, and replica replacement. Data nodes pin the first valid
incarnation they observe. Every configured metadata endpoint, head/snapshot
pair, and mutating control-plane request must prove the same incarnation before
it can affect provisioning or generation publication. Missing, zero, or
mismatched identities fail closed, preventing a mixed endpoint configuration
from validating a candidate against an unrelated cluster with the same group id.

`DB.restoreSnapshotToDeferredRuntimeRepair()` accepts a `StagedGeneration`
capability and rejects a path that is not the capability's staging root. Raw
path possession is therefore insufficient to mutate a serving root. Startup,
provisioning, and API restore entry points all acquire their capability through
the same process generation-lifecycle manager. `TableWriteSource.restoreTable()`
acquires and releases the table-level restore reservation itself, so direct
callers cannot omit write fencing. Cluster restore, which must hold that
reservation across metadata replacement and remote snapshot staging, uses the
explicit `restoreTableReserved()` operation while retaining the same reservation.
For overwrite mode, cluster restore preserves the existing table ID, ranges,
and target shard IDs. It stages source shard data into each target shard's
sibling generation and repairs it using the backup's schema and index catalog.
Only after that candidate is sealed does the exclusive transition atomically
upsert the table definition and exchange the storage generation. A metadata
publication is rolled back if storage publication fails before the namespace
exchange; after exchange, durability uncertainty is a committed result and
must not roll metadata back. Failed download, import, validation, or repair
therefore leaves both the live definition and generation untouched.
Definition publication and rollback are full-definition compare-and-swap
mutations. A concurrent schema, index, placement, or restore-intent update
therefore makes the transition fail closed instead of being overwritten by a
stale publish or rollback.

The API lifecycle above the generation manager is a bounded durable job. Public
job identifiers are opaque strings even though the local scheduler uses integer
keys; this prevents JavaScript and JSON consumers from rounding a storage
identity. Nonterminal jobs do not publish an expiry. Explicit idempotency keys
are indexed by a hash of authenticated principal, operation scope, and target
resource, so retries deduplicate within one authority boundary without causing
cross-user or cross-table conflicts.

Cluster-wide restore progress is represented by canonical inclusive ranges of
table ordinals. The common sequential path updates one range regardless of
table count, while sparse failures add only the required disjoint ranges. Antfly
supports 4096 tables in one cluster backup/restore and verifies that limit before
backup artifacts are emitted; explicit request lists remain bounded at 256.
These limits keep request memory, response aggregation, and the replicated job
record bounded while allowing large operational restores without the former
256-table recovery ceiling.

Queued structural reconciliation closes new write admission before draining
current writers. This prevents continuous traffic from starving index/catalog
convergence, while reads remain admitted against the currently published
generation until the short publication transition begins.
Each queued request owns one immutable catalog contract captured tentatively
from a single admin snapshot: table identity, desired schema and index
definitions, and the exact range and document-identity namespace for every
group. Before using that plan and before declaring completion, production
catalog sources validate a compact whole-table contract after a Raft
linearizable-read barrier. The contract carries an order-independent SHA-256
multiset digest and range count over the complete range records, so validation
is allocation-free against the metadata owner's immutable projection, O(1) on
the wire, and detects ranges added, removed, or mutated after capture. Its CPU
cost is one catalog-range scan only at plan admission and completion; per-group
fences scan the same projection without cloning it.

A dequeue advances at most a fixed number of pending groups, removes completed
groups in O(1), and rotates busy groups rather than replaying successful work.
Each group is also validated through its compact linearizable publication
contract both before opening storage and after maintenance, immediately before
runtime status publication. Productive quanta return to the queue tail without
an artificial delay; a short delay applies only when every attempted group is
blocked. A changed contract discards only the stale plan and rebuilds it on the
next quantum; the write-admission reservation remains continuous throughout
that handoff, and stale work cannot publish readiness for a replaced topology.
Per-table dirty visibility tracking uses owned exact table identities rather
than collision-prone hashes. Its lifetime is tied to write-cache ownership:
eviction advances the table's read-cache epoch, and the last cache owner also
invalidates cached runtime status and retires the dirty identity. A sibling
startup or serving cache keeps the identity alive until it also evicts the
table. Memory therefore scales with the bounded union of cache working sets,
not with historical writes, while a draining lease that mutates after eviction
re-marks itself through the normal visibility hook.
Runtime-status ordering separates structure from content. The cache is an owned
hash map by table with an inner hash map by group. Each table has an
`invalidation_epoch` and logical `root_generation`; restore, drop, schema/index
replacement, and visible-root transitions advance that epoch and remove the
old group map. Routine writer status does not advance it. A live publisher
captures the table epoch and a monotonic observation generation before opening
or inspecting its DB, then publishes in O(1). Publication rejects only a stale
table generation or an older observation for the same group, so activity on one
table cannot starve another.

A full refresh captures every catalog table epoch and one observation watermark
under a short lock, collects DB status without the lock, and publishes all
epoch-valid tables in one lock acquisition. A live observation newer than the
watermark wins; older cached content does not indefinitely mask an authoritative
refresh. Tables invalidated during collection are returned explicitly and only
those tables are recollected. Catalog-wide absence removal uses a separate
topology revision: if topology changed during collection, valid table updates
still publish but removals are deferred. Partial retries never remove absent
tables. Stable absence removal deletes the table state after advancing the
global epoch allocator, keeping memory proportional to current tables and
groups without allowing an old generation owner to republish if the name is
later recreated.
Writer ownership configuration is published under one ordered transition lock
set: cache-open locks by address, then the source mutation lock, then cache
lifecycle locks by address. Gate or mirror changes reserve both serving and
startup cache retirement before changing any source field, retire both caches,
and only then invalidate read/status observations and dirty identities. Every
read and write cache entry reserves its retirement queue slot when installed,
so the transition and final lease release are allocation-free. HA promotion
also preflights both live and raft-apply write sources before consuming standby
ownership; allocation pressure therefore fails promotion before irreversible
state changes instead of leaving a partially rewired primary.
Standby promotion transfers the already-open receive-log owner into the new
primary after validating the configured paths, durable timeline-switch record,
and slot store. It never opens a second writer over the same WAL root and does
not introduce a close/reopen window between standby and primary ownership.
This includes the embedded
`BoundTableWriteSource`: it closes its current owner, restores into a sibling
generation, publishes atomically, and reopens either the unchanged live
generation after a pre-commit failure or the newly published generation after
success. The provisioned serving path
uses separate prepare and publish operations so all required repair completes
before the namespace mutation. Startup/bootstrap convenience paths publish a
validated primary generation with a durable repair marker, allowing the normal
startup owner to resume derived repair after a crash. Replica bootstrap restore
must finish before `ensureReplica()` activates the group; an active replica is
never force-restored by raw path.

Publication errors are pre-commit errors. After a directory rename or exchange
makes the new generation visible, publication returns either `durable` or
`durability_uncertain` and the caller must finish cache invalidation and reopen
admission bookkeeping in both cases. A post-commit sync failure must never enter
the pre-commit abort or metadata-drop path. It is reported as committed with
durability pending, while raft bootstrap remains inactive. Every staged
candidate contains a durable two-phase publication record that moves into the
live root during the namespace exchange. A `prepared` record names the exact
retained sibling and causes startup to atomically restore that prior generation;
this covers process death after exchange but before catalog validation. Catalog
validation durably changes the record to `committed` before admission advances.
DB open completes committed publication by syncing the parent namespace,
submitting retained or abandoned sibling generations to the runtime cleanup
lane, and clearing the record before the root is admitted. Cleanup failures
leave the generated sibling name as durable retry debt for a later exclusive
transition or process reconciliation; they do not block read admission. Every
open DB retains a shared generation lease and a shared filesystem publication
lock through `DB.close()`. An exclusive transition
blocks new opens and cannot publish until all prior DB owners have closed.
Reconciliation is cached only while a local reader retains the shared
publication lock. The final reader invalidates the cache entry, so a later open
must observe publication debt created by another process. The persistent sibling
lock file also excludes overlapping restore publishers across processes.
Cached read admission acquires the shared filesystem lock before registering
the reader, then revalidates the process reconciliation evidence under the
manager mutex. If the final prior reader drained while the lock was acquired,
the opener releases the shared lock and retries through exclusive
reconciliation; no process-local reader count can bridge a filesystem-lock gap.
Stale-stage GC only considers names with
Antfly's complete generated-stage grammar while holding that exclusive lock;
it schedules marked abandoned candidates and markerless retired roots left by a
completed exchange, while ignoring arbitrary prefix-matching directories. A
manual runtime admits the reconciled read generation and downgrades the
publication lock before recursively reclaiming the exact stale paths identified
under exclusivity. This preserves bounded disk use without holding publication
downtime across potentially large directory deletion.
Retired-root deletion additionally holds one persistent cleanup lock per parent
directory. Duplicate workers and separate processes therefore serialize only
for the same table-group parent, while unrelated shards reclaim in parallel.
After acquiring that lock each worker rechecks path existence, making an
already reclaimed generation a successful idempotent outcome rather than a
failed durable job.

A restore whose namespace exchange committed but whose parent sync failed is
reported as durability pending. Retrying the same backup is idempotent: Antfly
matches the persisted backup, source location, shard, and repair-complete state,
reconciles publication, and reports committed/durable. A retry against an
unrelated existing table is rejected without modifying storage.

The public restore-job journal mirrors that generation state instead of reducing
it to success/failure. Before irreversible table work it persists one active
manifest ordinal. Publication moves that ordinal to either a durability-pending
or published compressed range, and replica convergence moves published ordinals
to the completed range. The ranges are disjoint where ownership differs and are
validated on load, keeping restart decisions explicit while bounding serialized
state. A durability-pending table terminates the attempt with a committed/pending
result; a later idempotent request reconciles the generation marker rather than
blindly replacing or rejecting the visible table. Restore dispatch is likewise
generation-fenced: leadership pauses FIFO admission, drains the old runtime
owner, rebuilds durable attempts, publishes the new term, and only then reopens
admission. Completion callbacks request another dispatch pass without blocking
on the dispatcher that may be destroying them.

Replacing an existing direct-path generation requires an atomic directory
exchange; platforms or filesystems without that primitive reject publication
before changing the live namespace. If the exchange is visible but its
parent-directory sync fails, the previous generation remains under the staging
name until reconciliation confirms the new namespace is durable and submits it
for asynchronous cleanup.

The current root layout is path-relative and may lazily open run files after a
transaction starts. Preparation may overlap reads because it only touches the
sibling root, but serving transitions still stop admission and drain old
read/write owners before publication. Refcounts make close ordering safe, but
they do not make a path swap safe beneath an old reader. A
future zero-downtime transition must place each generation under an immutable
versioned root and atomically publish a separate current-generation pointer;
only then may generation N readers overlap preparation and publication of N+1.
The direct-path layout also cannot make multiple shard directory swaps
crash-atomic. Until table generations are published through one durable table
generation pointer, table backup and restore reject multi-range manifests
immediately after decoding, before metadata creation, remote shard transfer,
staging, or publication.

LSM manifests are relocatable generation metadata. On open, run paths are
reconstructed from the current root and run identity instead of trusting an
absolute path encoded before a snapshot copy or staging rename. Obsolete run
paths are similarly rebased when they refer to the prior root's `runs/`
directory.

Shared caches must be invalidated by the table-group root, not only by the
primary store path. Primary LSM caches and LSM-backed indexes can use path-prefix
invalidation directly. Dense/HBC uses a shared cache namespace per concrete HBC
index root, so the cache registers namespace-to-path ownership and table restore
or drop invalidates every HBC namespace whose path is under the restored/dropped
table-group root. Any future shared index cache should follow the same rule:
cache keys may be engine-specific, but lifecycle invalidation is table-root
scoped and owned by the generation transition.

Shared HBC namespace-to-path registrations are reference counted by open index
owners. A registration remains after the last owner closes only while retained
cache entries still use it; clear, path invalidation, and subsequent index opens
prune unowned registrations once those entries are gone. Per-namespace cache
statistics are retired at the same boundary; only bounded live namespace state
and cumulative global counters remain.

The DB layer therefore separates three concepts:

- storage capability: whether the handle can write the primary/index stores
- runtime initialization: whether helper runtimes are constructed so foreground
  code can use their replay/apply logic
- runtime worker ownership: whether background workers are started and allowed
  to keep running after open

Restore repair is the important example. A restored shard with generated
chunking or generated embeddings needs the enrichment runtime's replay logic to
materialize chunk and embedding artifacts. It must not start every optional
runtime worker just to get that capability, and it must not race a lookup/query
open against half-published restored files. Instead, restore repair runs as
foreground maintenance under the table generation transition, initializes only
the required runtime services, drives generated enrichment replay, drains
derived/index replay, syncs state, and closes before reads become visible again.

The invariant is:

```text
one background runtime owner per shard root
one cached write DB owner per table group
one cached read DB owner per table group/generation
exclusive generation transitions own maintenance opens and publication
queued exclusive transitions reserve both read and write admission
transactions retain the backend generation until abort or commit
cursors retain their parent transaction until cursor close
returned owned buffers are allocated in an explicit caller ownership domain
foreground handles do not wait on background workers they did not start
```

Current implementation maps this contract to generation-owned entries in
`ProvisionedTableWriteCache` and `ProvisionedTableReadCache`, the process
`generation_lifecycle` manager, and a local table generation transition around
restore. Cache entries own their DB and are retired when invalidated; active
leases keep retired entries alive until the last operation releases them. The
transition holds the read-cache exclusive lease through staged restore, repair,
and publication, invalidates/drains both write caches, and keeps maintenance
opens uncached. A maintenance open is nevertheless created by, and remains
inside, the active transition capability; no fallback path releases exclusivity
and independently reopens the same root. Once an exclusive transition queues,
new reads and writes wait behind it while already admitted reads drain, avoiding
writer starvation under sustained query load. At the storage boundary, LSM
transaction opens are rejected
after generation close begins, backend close drains active transaction readers,
and erased cursors retain their transaction box so transaction cleanup cannot
free cursor snapshot state. Storage-backed status and schema-capability
inspection are reads for lifecycle purposes: they acquire the same table read
activity as lookup/query before opening a status-only DB. A transition drains
those activities and blocks new status opens until the repaired generation is
published. Cache-only status snapshots do not require a DB lease. In raft-backed
serving, the apply state machine's write source is the canonical local write
owner. Startup catch-up and other maintenance entry points resolve that owner
before acquiring an activity lease or opening a DB; the startup cache is
attached to that owner and is drained at the end of each catch-up operation. A
forwarding API source must not create a second activity or cache domain over the
same table-group path. Its read-preparation and primary-lookup interfaces must
delegate to the canonical local write owner even when those interfaces were
captured before owner attachment.

Data-Raft snapshot installation uses the narrower group generation transition.
A preparation token serializes the target group's apply stream while allowing
other group operations and table reads to continue. Publication then acquires a
group-exclusive read-cache lease, retires only matching write/startup owners,
drains their leases, exchanges the staged root, advances a reserved visible-root
generation, and releases queued applies. The reservation itself owns a map
reference, so topology pruning cannot remove generation bookkeeping during a
fallible staging operation. Exchanged old apply roots are submitted to the
shared backend runtime cleanup lane rather than reclaimed on the Raft apply
critical path.

The Raft apply store's per-group DB owners are a bounded LRU cache, not a second
unbounded storage registry. Owners and their LSM mutable state are charged to
the provisioned node's shared `ResourceManager` under `lsm.in_memory_state`.
Normal pressure retains the configured per-stripe bound, soft pressure shrinks
it, and hard pressure retains only the most recently needed unpinned owner.
Snapshot staging uses the same manager, so temporary generations cannot bypass
the node memory policy. Eviction closes only the runtime owner; durable group
files and apply watermarks remain available for transparent reopen.

Placement reconciliation is a two-phase admission transition. Before fallible
descriptor and host reconciliation, the apply store prepares exact new maps and
admits the conservative union of old and new groups. Success commits exactly the
new set; partial failure retains the union because the host may already have
created or removed replicas, and the next metadata sync narrows it. Exact commit
retires owners and cached summaries without heap allocation. An owner pinned by
snapshot materialization or generation publication is marked retired, and the
last reader or publisher performs final close. This keeps topology reconciliation
independent of allocation pressure and slow snapshot consumers while making
stale ownership bounded after the next successful sync.

Split and merge control paths follow the same rule. Transition coordinators
borrow the raft host's apply store and retain the managed destination or
receiver DB lease for their full lifetime; observation never reopens either
path. Pending destinations are leased by globally unique group ID so metadata
publication and visible-root generation changes cannot hide an already-open
writer behind a stale table label. A process-local transition admission lock
serializes fallback coordinators while a destination is not yet published.
Destination identity comes from its projected range when available, with the
source range used only for pre-publication bootstrap; identity reassignment is
never performed against a live managed DB. Writer-cache admission validates the
expected namespace before returning a lease. An idle mismatched owner is
retired and closed before replacement; an active mismatch blocks the new open
until its existing lease drains. This prevents a pre-publication destination
open from becoming the serving writer for a differently namespaced range.
Every destination bootstrap and catch-up batch carries a typed split replication
context containing the metadata transition ID, source group, destination group,
inherited identity namespace, operation kind, and source delta sequence where
applicable. The context is encoded in the internal HTTP command and the
data-Raft entry, validated against the catalog-visible source range on every
replica, and used for the destination's first physical DB open. Public batch
parsing rejects this internal context.

The destination stores the completed bootstrap identity and highest applied
delta sequence in the document store. A completed bootstrap fences late chunk
proposals; delta retries at or below the durable sequence are no-ops; and a
forward sequence gap is rejected. Empty source deltas are sent as sequence
barriers, so a gap can never be mistaken for an empty mutation. Document writes,
range publication, bootstrap identity, and sequence advancement share the same
DB apply lock and backend batch. The source acknowledges only after that commit,
and its durable acknowledgement includes the same transition ID. Range buffers
are prepared before commit and installed into the open DB allocation-free after
success, leaving no post-commit error path that can split durable and in-memory
ownership state.

Placement changes must also make stale leadership self-healing without
destabilizing membership expansion. A `draining` source and a `cutover_ready`
target remain valid leaders until the expanded voter set converges. Once a
leader enters `retiring`, local reconciliation transfers leadership to the
lowest-ID retained voter before proposing self-removal. If the retiring leader
has already disappeared, that same lowest-ID serving voter campaigns after
placement reconciliation. Reconciliation resolves transport endpoints for the
union of voters and learners before proposing membership changes; otherwise a
committed learner can remain permanently unable to receive its log or snapshot.
Internal leader-only routes return a typed
unavailable response during that handoff. They do not leave survivors honoring
a removed leader, trigger election storms during expansion, or collapse
retryable topology churn into a generic HTTP failure.

Replicated source split lifecycle commands have one state-machine owner: the
durable data-Raft apply store. They are transition-only entries and are never
forwarded to the document DB executor. The apply store tracks its own durable
entry watermark and filters overlapping committed prefixes before producing
effects, so a Raft applied watermark that temporarily lags another state
machine cannot apply `prepare` twice while advancing to `start`.

Metadata-Raft projection publication is commit-derived. The apply store stages
owned projection signals, committed-key notifications, and split/merge deltas
while the write transaction is open, then publishes them only after the batch
and applied watermark commit. Allocation failure or transaction abort discards
the complete staged outcome. A command rejected by monotonic identity or phase
fencing is a durable no-op and therefore emits no runtime delta; production
listeners and deterministic simulations consume this same committed outcome
instead of reconstructing effects from requested commands. Batch apply,
snapshot installation, and synchronous listener dispatch share one serialized
publication lane, so consumers observe durable projection changes in commit
order. The normal production apply path does not clone transition records when
no caller requests the committed delta list.

Removing an active split or merge record is intentionally a no-op, not a
cancellation protocol. Cancellation first commits rollback intent on the same
transition identity, lets the transition owner restore source/receiver state,
then commits the `rolled_back` terminal phase. Only a terminal record may be
removed. Restart can therefore recover either an actionable rollback intent or
a terminal record, never an absent active transition whose storage side effects
remain live.

Distributed split bootstrap uses an exact destination reservation keyed by
transition identity, attempt epoch, byte range, and source delta sequence. A
replayed begin for that same reservation is idempotent and cannot erase chunks
written by another source-leader attempt. A newer sequence may replace an
incomplete reservation and fences older writes; completion publishes only the
exact reserved sequence. The source acknowledgement is proposed only after
destination completion, so metadata cutover cannot authorize an empty or
partially replaced destination generation.

A source group created before data-Raft projection has no apply watermark yet.
Its first split preparation seeds the source snapshot and a synthetic index-zero
watermark in one DocStore batch under the per-group apply lock. Index zero is
reserved for this baseline; real Raft indexes remain unchanged. A replica
replacement may instead inherit an authoritative document generation while its
new local Raft projection contains only a bootstrap batch. In that case a
non-null watermark does not prove that the projection includes the inherited
documents.

Split preparation reconciles an inherited generation only at an exact durable
apply watermark. It first waits until the document state machine has applied
through that watermark, acquires group-exclusive write admission, finishes
managed bulk state, and scans one point-in-time transaction from the
generation-owned writer. It does not hold the DB apply lock during the scan.
Existing projected values are compared through a cursor instead of being
materialized, and atomic replacement writes directly through one transaction
instead of constructing another shard-sized write list.

Successful reconciliation records the authoritative DB root's durable 128-bit
incarnation beside the projection. This identity is persisted in the staged DB
before generation sealing and atomic publication; it is stable across a process
restart but regenerated whenever a physical root is replaced. A process-local
visible-root generation is not sufficient because it restarts from zero. Later
split handoffs use an O(1) durable fast-path check: both the root incarnation and
the complete Raft watermark identity must match before the apply-store
projection can be captured without rescanning the DB. Missing, zero, malformed,
or mismatched incarnation markers fail closed. Snapshot installation naturally
drops the marker with the replaced group generation. Normal-entry history and
the watermark remain unchanged. If Raft advances during repair, publication
fails and preparation retries from a new snapshot; only replicated split deltas
may mutate an active projection.

The incarnation belongs to the core storage generation, not to a derived-index
repair checkpoint. It is stored in an independently checksummed
`root_identity.checkpoint`, written through the DB runtime's caller-owned
`std.Io`, synced, and atomically renamed. The identity belongs to the directory,
not the process-local visibility generation: cache fencing and in-place metadata
reconciliation preserve it. Every newly staged directory creates its identity
before sealing, so atomic publication carries the new identity with the root.
First creation is serialized by a blocking OS file lock and rechecks the
checkpoint after acquiring that lock. Concurrent first opens therefore observe
one durable incarnation rather than retaining different random candidates.
Derived checkpoint loss or repair cannot change storage identity. A physical
generation replacement creates a new identity even when the logical contents
and index catalog are unchanged.

This checkpoint applies only to `filesystem_managed` roots whose path is the
published storage directory. Embedded and Lite DBs use `external_backend`: the
path is a logical namespace while the enclosing single-file engine owns
locking, durability, and publication. Those handles do not synthesize a
directory incarnation and fail closed if a distributed projection asks for
one.

Replica relocation and replica-count shrink use two committed membership
phases. A `draining` source remains a voter, and may remain leader, while
replacement learners hydrate and the expanded voter set stabilizes. Once any
healthy leader proves that exact, non-joint expanded configuration, metadata
publishes the final peer set and marks removed replicas `retiring`. Local Raft
reconciliation then transfers a retiring leader to a retained voter before
proposing contraction. That exact final peer set is latched in the retiring
placement records; a later planner result cannot replace it midway through the
phase. Newly proposed peers are deferred and omitted final peers are preserved
until the phase completes. A retiring replica remains hosted and reports Raft
status but is excluded from membership and client routing. Its placement row
can be deleted after a healthy surviving leader proves the exact, non-joint
final voter set. A healthy source that still reports itself as a voter continues
to block deletion, but an unavailable removed source cannot deadlock cleanup
forever. Metadata absence is the durable routing and ownership fence; local
reconciliation removes the persisted Raft catalog record before retiring the DB
owner. This prevents filesystem and cache retirement from racing a
still-committed Raft member, including replication-factor-one moves. Membership
planning indexes placement intent by `(group, node)` and runtime evidence by
`(group, store)`. Hydration, cutover, and retirement accept evidence only from
the exact store named by the placement. A zero store identity is usable only
when the node has one unambiguous store; multi-store nodes fail closed. A
duplicate store identity or duplicate `(store, group)` report is malformed
evidence and also fails closed instead of choosing an order-dependent winner.
A reconcile pass builds this evidence index once and remains linear in stores,
status reports, and placements, with only replication-factor-bounded scans per
retiring group.

Relocation hydration is proven by relocation generation, committed Raft apply
boundary, logical document watermark, and stable voter identity. Source disk
bytes are capacity telemetry, not a cutover predicate: compaction, segment
layout, and cache state can make a logically equivalent target generation
smaller or larger than its source. Physical-size comparison must therefore
never strand an otherwise complete learner in replay.

Transition observation reads the source phase, terminal fence,
acknowledgement, and delta sequence atomically under one apply-store shard lock.
It never seeds or scans the document DB. Observation and reconciliation fail
fast with a retryable unavailable result while snapshot generation staging is
active; they do not wait behind generation I/O and consume the HTTP worker pool.
Managed data-Raft status collection follows the same ownership boundary: it
uses writer-published runtime snapshots when available and conservative
Raft/transition facts on a cold cache. It never opens an independent status DB
over a Raft-owned root. Transition readiness comes from observations published
by the transition owner into metadata, then from durable phase/snapshot facts;
status paths never instantiate split or merge coordinators to inspect live
roots. Non-Raft deployments may retain the bounded status-only open because no
competing managed writer exists.

Split execution dispatch follows ownership as well. A data-Raft server uses the
replicated destination route. A server with an injected local transition runtime
delegates to that runtime, and the non-Raft fallback creates a short-lived local
coordinator under transition admission. Only the replicated path requires a
destination URI; local execution must not fail merely because no remote route
exists.

Allocator ownership is part of the same lifetime contract. APIs that return
owned storage/index buffers accept the allocator that must later free them, or
carry their allocator in a typed owner. In particular, full-text stored-document
decompression allocates directly in the request/result allocator instead of the
segment reader's allocator; this prevents cached index generations and HTTP
requests from crossing allocator domains during result teardown.
Production HTTP server construction selects the process request allocator
internally. Arbitrary request-allocator injection is test-only, so a new serving
call site cannot accidentally recreate cross-allocator ownership by choosing an
allocator with a different identity.

## Write Contract

`DB.batch()` is document-first.

For each document write:

- parse once
- strip DB-owned special fields from the stored base document
- derive text/vector/graph directives from that parsed form
- commit the base document state
- hand derived work to index application or, next, the derived-work log

Special-field writes that do not replace the base document must preserve the
current stored document.

## Derived Artifact Contract

Derived artifacts are enrichment-owned, not index-owned.

Storage keys should use:

- `<doc>:e:<type>:<name>:...`

Examples:

- `<doc>:e:chunk:body_chunks_v1:0`
- `<doc>:e:asset:ocr_text_v1`
- `<doc>:e:embedding:body_dense_v1`

This keeps artifact type explicit and allows one chunking or asset-output
pass to feed multiple indexes without duplicating sidecar storage.
Model-produced text, OCR, transcripts, classifications, entity extraction, and
other derived payloads use the generic `asset` artifact type, with the specific
media/schema encoded by `content_type`, artifact name, and enrichment metadata.

Current first implementation:

- chunk artifacts produced by the leased enrichment runtime
- generator configs may declare `chunk_name`
- chunk artifacts are stored under `<doc>:e:chunk:<chunk_name>:<chunk_id>`
- overwrites and deletes clear stale chunk artifacts before regeneration

## Derived Coverage Accounting

Derived artifacts and artifact-backed indexes need coverage accounting that is
stronger than raw artifact or index-entry counts. A managed embeddings index can
legitimately cover only a subset of table documents, for example a conditional
media template over a mixed corpus, while a failed or lagging enrichment must
not be hidden as "ready" simply because partial coverage is allowed.

The DB-level invariant is:

- coverage measures whether every source unit for a derived generation reached a
  terminal outcome
- readiness is separate from health
- query correctness must know when an index is partial, and should not treat a
  partial index as a full-table access path unless the query or caller accepts
  that coverage

This follows the database precedent for partial indexes: eligibility is explicit
and planners avoid using a partial index when doing so would produce an
incomplete result set. For derived work, the corresponding rule is that
eligibility and terminal outcomes are explicit, not inferred from template
syntax or artifact counts.

### Coverage State

Coverage is tracked per derived generation over logical source units, not over
physical index entries. For the current embeddings API the source unit is one
table document, including chunked embeddings: one document may produce many
vectors but receives one `produced` outcome. This keeps the durable numerator
comparable with the table-document denominator. Future artifact-to-artifact
producers may use artifact identities only when their API exposes a matching
source-total denominator. The coverage key identifies:

- table or shard
- derived artifact or index name
- source artifact name, when the producer consumes another artifact stream
- source unit identity
- generation or config version

Each source unit has one current outcome for that generation:

- `pending`: not evaluated yet
- `in_flight`: currently being evaluated
- `produced`: artifact or index entry exists for the generation
- `skipped`: evaluated and intentionally produced nothing
- `terminal_failed`: evaluated and cannot produce after the configured policy
  and retry budget
- `stale`: an older generation result exists, but the current generation has not
  completed for this source unit

Aggregate status is derived from those durable per-source outcomes:

- `source_total`
- `eligible`
- `produced`
- `skipped`
- `terminal_failed`
- `retryable_failed`
- `pending`
- `in_flight`
- `generation`
- `complete`
- `healthy`
- `degraded`

Completion is policy-specific. For the current document-level embeddings
contract:

```text
strict complete      = produced >= source_total
partial complete     = produced + skipped >= source_total
best_effort complete = produced + skipped + terminal_failed >= source_total
```

Counts are mutually exclusive per `(index, generation, source document)`. When
the distributed observation is complete, `pending` is `source_total -
min(source_total, covered-by-policy)`. When any expected shard observation is
missing or unusable, the global pending count is unknown and the public status
reports `pending: null`; it never presents an observed lower bound as an exact
global count. Runtime replay debt remains an independent readiness gate. Health
is stricter:

```text
healthy = complete and terminal_failed == 0
degraded = complete and terminal_failed > 0
```

This keeps "no work remains" distinct from "all desired outputs exist".

`source_total` comes from a durable range-local document cardinality maintained
in the same primary write transaction as document identity. It is O(1) to read
and does not depend on query-visible index entries. The identity namespace and
its `live_ordinals` summary remain shared across split descendants, so they are
not an ownership proof for an individual shard. Split preparation and
finalization therefore rebase both the range cardinality and each active
generation's mutually exclusive outcome counters from the documents and
outcome markers retained by that range. A missing range counter is recovered by
one bounded range scan, then subsequent inserts and deletes update it by the
identity visibility delta in the caller's atomic primary batch.

This distinction is required for chunked projections, where one source document
may produce many physical vector entries. Distributed projections sum
cardinality and outcomes only from fresh shard observations;
`observation_complete` is false if any expected shard is missing, stale,
remotely unknown, reports an incomplete counter summary, or reports a
stored-config fingerprint different from the requested index config.
An incomplete observation can never report complete or healthy coverage and
includes structured reasons such as `missing_group`, `stale_group`,
`summary_unavailable`, and `config_mismatch`. The configuration fingerprint is
a versioned canonical hash of semantic generated-output configuration: object
ordering, credentials, provider rate limits, and top-level execution batching
are excluded. It is encoded as a fixed-width hexadecimal string at the API
boundary and remains stable across shard-local marker generations, preventing
rolling reconfiguration from combining outcomes that describe different
indexes. An idempotent index mutation or an operational-only configuration
change preserves the catalog-owned coverage incarnation while storing the new
operational settings; a generated-output change assigns a fresh incarnation.

### Coverage Policy

Derived configs should declare how missing or non-embeddable source units affect
readiness:

- `strict`: every eligible source unit must produce an output; skipped or failed
  required units keep the derived artifact/index not ready
- `partial`: intentional skips satisfy completion; terminal failures do not
  satisfy healthy readiness
- `best_effort`: intentional skips and terminal failures may satisfy completion,
  but terminal failures make the status degraded

The policy is explicit on each consuming managed embeddings index, for example:

```json
{
  "type": "embeddings",
  "coverage_policy": "partial",
  "template": "{{#if image_url}}{{remoteMedia url=image_url}}{{/if}}"
}
```

Enrichment definitions do not independently expose coverage policy in the
current API. They durably record producer outcomes; the consuming index decides
which terminal outcomes satisfy its readiness contract. This avoids conflicting
producer/index policies and gives one unambiguous status per index. A future
artifact-level readiness API may add producer policy as a separate contract,
but it must not silently override index policy.

Today eligibility is an execution outcome: a managed template that renders no
input records a durable `skipped` result. A future declarative eligibility
predicate must be implemented consistently by planning, execution, repair, and
status before it becomes public. Unknown `applies_when` configuration is
rejected rather than silently ignored.

### Readiness And Repair

Runtime status must use coverage accounting instead of raw `doc_count >=
table_doc_count` for managed derived indexes. In particular:

- `rebuilding` and `backfill_active` remain true while coverage has pending or
  in-flight source units
- `replay_catch_up_required` reflects source/enrichment replay debt and must not
  be cleared solely because at least one artifact or index entry exists
- partial coverage can mark skipped units complete, but it cannot mask pending
  enrichment work
- terminal failures are visible through degraded status and reason counters

Repair also needs durable source-unit outcomes. Aggregate counters are not
enough, because repair must distinguish:

- never evaluated
- intentionally skipped
- produced but lost
- stale generation
- terminal failure

Coverage-gap repair should regenerate missing `produced` artifacts, leave
current-generation `skipped` units alone, and retry or surface
`terminal_failed` units according to policy.

Per-source outcome markers are the durable source of truth. Generation-scoped
`produced`, `skipped`, and `terminal_failed` aggregate counters are updated in
the same DB apply-lock domain as replay mutations, and each marker/counter batch
commits atomically. Outcome transitions remove any prior outcome and update all
affected counters in that one batch. Generated enrichment applies `produced`
only after publishing its replay record, and non-retryable isolated request
errors publish `terminal_failed`; shared embedding failures fan that outcome out
to every consuming index, while retryable failures remain pending. Direct
field-backed vector indexes classify source writes as `produced` or `skipped` at
the successful derived-index apply boundary. Replay windows carry mixed
outcomes and persist them under one apply lock and one store transaction; batch
mutation paths deduplicate source keys with hash sets, keeping cleanup linear in
the number of distinct source units rather than quadratic in batch size.

Each source uses one marker key whose value is the outcome enum. Transitions
therefore require one point read and one marker write rather than probing one key
per possible outcome. Aggregate counters remain separate and atomically updated;
status loads the three counters with O(1) point reads and never scans source
markers. A partial counter tuple is reported as degraded and incomplete. If
counters require repair, a bounded maintenance scan reconstructs them from
marker values outside the status path. External embedding writes participate in
the same accounting:
only a durably applied `_embeddings` value is `produced`, and a source without an
external vector remains pending rather than being assumed covered.
That pending coverage remains visible in status but does not make a usable
external index report that it is rebuilding. External query readiness depends
on current replay and published artifact visibility because callers are not
required to supply a vector for every source document; coverage completeness is
an independent diagnostic contract.

Public status reports physical vector or sparse-entry `doc_count` separately
from source coverage. Readiness must never substitute physical cardinality for
the durable `produced` source count because chunked documents can create many
entries and complete early under that approximation.

### Managed Index Admission

Adding an index to a non-empty managed table is a generation transition, not an
ordinary catalog insert. The admission path uses a transactional outbox in the
primary document store:

1. Under the DB apply lock, read the O(1) document-identity summary and capture
   its live count, identity generation, configuration hash, and replay target.
2. Commit the index catalog row and a fixed-size admission marker in one primary
   store transaction. Do not synchronously scan or rebuild the corpus.
3. Materialize that marker idempotently into the generation repair checkpoint.
   The owner repair scheduler performs the bounded shadow rebuild and replay.
4. Keep the marker while repair is pending. Every DB open captures the marker
   prefix once before catalog open. Writable open uses the O(1) name set to
   suppress in-place backfill and recreate missing sidecar state; read-only and
   status opens use the same snapshot to keep the generation unavailable.
5. After clean shadow activation, delete the primary-store marker before
   removing the sidecar intent. A crash between those writes leaves redundant,
   resumable repair state rather than an admitted generation without debt.

The identity generation captured by the marker is monotonic evidence. Recovery
fails closed if the current identity summary regresses below it. Concurrent
writes after admission are normal: repair targets at least the marker's replay
sequence and raises that target to the current derived sequence when the durable
intent is created. Existing repair IDs are reused, so reconciliation, restart,
and scheduler retries cannot create duplicate rebuilds.

Empty managed tables omit the marker but still use the managed no-backfill add
path. The durable zero-live-document identity summary proves that generation
complete at the current replay head without scanning document keys, including
tombstone-heavy shards. Ordinary unmanaged DB callers retain the explicit
synchronous-add behavior. This keeps the durable lifecycle protocol at the
managed-table boundary instead of inferring migration debt from index
cardinality or replay lag during every reconciliation pass.

Index deletion uses catalog absence as its commit point. The catalog image
without the index and deletion of its admission marker commit in one primary
store transaction while repair remains quiesced and gated. Runtime roots,
coverage metadata, and repair sidecars are post-commit cleanup. A crash can
therefore leave reclaimable files or an orphaned sidecar, but cannot leave a
query-visible catalog entry without admission proof. Writable startup removes
sidecar intents whose catalog entry is absent before workers start.

Generated-artifact reclamation separates page arbitration from terminal
filesystem ownership. Cleanup workers try to claim the short page mutex and
return `busy` rather than waiting. A completed tombstone then acquires a distinct
finalization lease, releases page arbitration, and performs checkpoint and
directory deletion while the durable tombstone continues fencing same-name
admission. Other indexes can keep draining metadata pages during that I/O. A
crash or finalization error leaves the tombstone intact for idempotent retry, and
manual runtimes without a timer leave contention for the next explicit
maintenance poll instead of recursively resubmitting work.

The in-memory identity summary is an optimization for status and query
planning, not admission authority. Admission reads the durable O(1) primary
summary under the apply lock because an HA mirror failure may occur after the
primary commit but before the runtime cache is published. Normal query traffic
does not read admission markers; only a query already rejected by an in-memory
repair gate performs a point recheck to observe repair completion safely.

Catalog admission and repair activation are separate safety proofs. Metadata
cutover additionally requires a fresh target observation, complete document
identity, and target full-text cardinality equal to the source live-document
count. No status path may infer readiness solely from catalog presence.

### Scope

The current durable marker, counter, and public readiness implementation applies
to managed dense and sparse embeddings indexes. The common outcome model is the
required contract for extending coverage accounting to other derived artifact
producers and artifact-backed indexes:

- embeddings
- chunks
- summaries
- image, audio, PDF, and OCR extraction artifacts
- graph extraction
- artifact-backed dense and sparse vector indexes
- algebraic or materialized derived indexes where source-unit coverage matters

Full-text indexes usually have trivial full coverage, but can still project the
same status shape with `eligible == source_total` when useful. The shared model
should live below the public API so each producer can report comparable lifecycle
state without pretending every index is document-count based.

## Search Contract

`DB.search()` is coordinator-style orchestration over named result sets.

Current result-set sources:

- `full_text`
- `full_text_queries`
- `dense`
- `dense_queries`
- `sparse`
- `sparse_queries`
- `graph_queries`
- fusion via `merge_config`

Graph queries may reference:

- `$full_text_results`
- `$full_text_results.<name>`
- `$aknn_results.<index>`
- `$graph_results.<name>`
- `$fused_results`

`expand_strategy` is a DB-level post-graph operation over the top-level hit set.

## Transaction / HLC Contract

Local transaction semantics follow the same visible-version rule used by the
shared compat corpus:

- transaction intents remain invisible until resolution
- `commitTransaction(txn_id, commit_timestamp)` makes the committed value visible
- the visible version/timestamp of the committed key becomes `commit_timestamp`,
  not the original intent timestamp
- transaction records preserve the original begin timestamp separately from the
  visible commit version so distributed resolution can propagate commit version
  without losing coordinator-start metadata
- participant-style resolution should use the propagated commit version directly;
  a node resolving remote intents should not need to infer visibility from local
  wall-clock time or from the original begin timestamp
- optimistic predicates compare against that visible committed timestamp
- `abortTransaction` removes intents without changing the prior committed value
  or its timestamp
- local recovery uses the richer transaction record for single-node cleanup:
  stale pending records auto-abort, finalized records repair any leftover
  intents using the stored status plus visible commit version, and old finalized
  records are cleaned once no local intents remain
- transaction records may also carry participant and resolved-participant
  metadata; cleanup must be deferred while any participant remains unresolved,
  even if the local coordinator intents are already repaired
- coordinator-side recovery may retry unresolved participants through a
  transport-agnostic resolver callback; only successful acknowledgements should
  mark participants resolved
- cleanup remains gated on both conditions:
  local intents repaired and all participants resolved

This keeps ordinary batch predicates and transactional predicates on one
versioning model.

## Next Async Boundary

The async index/enrichment manager should sit between document preprocessing and
derived index mutation.

The intended flow is:

1. parse and extract once
2. commit base docs
3. append one batch-shaped derived-work record
4. let per-index workers advance watermarks from that log

The derived log should be sequence-based and idempotent. Rebuild-from-docstore
remains the fallback, not the normal replay path.

Once every managed index has advanced past a sequence, the derived log may be
truncated at the global minimum applied watermark.

### Sequence-Stamped Projection Checkpoints

Every derived projection should publish a durable checkpoint that says exactly
which base-data sequence is queryable through that projection. This applies to
dense, sparse, full-text, graph, algebraic indexes, and enrichment scopes. The
checkpoint is not an optimization; it is the restart contract.

The checkpoint must atomically couple:

- the visible projection artifact, root, manifest, or generation
- the highest applied derived-log sequence covered by that artifact
- the projection status: clean, rebuilding, degraded, or repair-required
- the projection schema/config/version identity needed to validate reuse

On open, a clean checkpoint means the DB may serve the checkpointed projection
immediately and replay only the derived-log tail after the checkpoint sequence.
Startup must not scan the primary document store to prove that a durable,
cleanly-published projection is queryable. Full corpus scans remain valid only
for explicit repair, incompatible metadata, interrupted rebuilds, or corruption
markers.

The applied sequence must live in the same atomic publish domain as the
projection effects. A separate applied-sequence row is not enough unless the
write protocol can prove that, after a crash, the sequence never advances beyond
the durable projection data visible to queries.

Planned shape by projection type:

- dense/HBC: publish the applied sequence, status, generation, and config
  identity in the HBC metadata record; mirror the same checkpoint into the
  shared sidecar for common status APIs and non-HBC tooling
- full-text, sparse, graph, and algebraic indexes: store the applied sequence in
  each index manifest or LSM/runtime-store manifest and publish it with the
  visible root
- enrichments: checkpoint each enrichment scope only after every source change
  through the sequence has a durable artifact, durable skip, or durable
  failure/repair record; external model side effects make clean versus degraded
  status part of the contract
- artifact counters: maintain per-index target/completed counters
  incrementally on artifact put/delete so coverage checks are O(1) during
  normal startup; corpus-wide recounts are repair tooling

Implementation plan:

1. Add a shared `ProjectionCheckpoint` format with applied sequence, status,
   generation, config identity, and compatibility version.
2. Add storage helpers that read, validate, and atomically publish a checkpoint
   with the owning projection artifact.
3. Migrate dense first: extend the HBC metadata publish path, make dense catchup
   advance the checkpoint only after durable artifact/root publication, and make
   clean restart trust the checkpoint plus replay the derived-log tail.
4. Move full-text, sparse, graph, and algebraic managed indexes onto the same
   checkpoint API through their existing typed index ownership boundaries.
5. Convert enrichment progress to checkpoint semantics so a scope can report
   clean, degraded, or repair-required rather than only "last sequence seen".
6. Replace normal-startup dense artifact recounts with incremental counters and
   reserve full-store scans for explicit repair paths.
7. Reduce read-cache churn so tail replay only invalidates query state when a
   visible root, generation, or config actually changes.

Current implementation notes:

- Dense/HBC metadata format version 2 carries the projection checkpoint fields
  directly in the HBC metadata record. DB checkpoint saves update HBC metadata
  first, then update the shared sidecar mirror; dense checkpoint reads prefer
  the HBC metadata checkpoint when it has a nonzero config identity.
- Shared sidecar checkpoint format `AFPRJCP1` is current-only and stores applied
  sequence, status, generation, and config identity for all managed projection
  types.
- Enrichment progress uses checkpoint semantics for applied sequence and
  clean/degraded/repair-required status, with a deterministic enrichment-catalog
  config identity.
- Normal dense startup trusts a clean, config-matching, caught-up HBC checkpoint
  and does not run a primary document-store artifact recount. Recounts remain
  repair/rebuild tooling for stale config identity, repair-required status,
  interrupted rebuild state, corruption, or watermark regression.
- Dense artifact target counters are durable metadata rows updated in the same
  primary-store batch as artifact writes/deletes. Counter classification uses
  the shared embedding artifact identity decoder, so direct document embeddings
  and derived chunk embeddings feed the same O(1) startup coverage path.
- Applied-sequence and runtime-status-only visibility notifications publish the
  status snapshot without invalidating cached query DBs. Read-cache invalidation
  remains reserved for visible-root/data publishes, blocking publish repair, and
  explicit table invalidation.
- Dense catch-up pacing sizes replay-window coalescing and catch-up session
  reuse waits to the current derived-log tail. Small restart tails no longer pay
  the full 256-record/2s coalesce window or 5s session-idle ceiling; those
  ceilings remain available for large hot-ingest backlogs.

Acceptance criteria:

- clean restart with no new writes opens queryable projections without a primary
  document-store scan
- startup work is proportional to the derived-log tail, not corpus size
- crash tests around checkpoint publication never expose a sequence ahead of
  durable projection effects
- repair markers, incompatible metadata, and corrupted checkpoints still force
  rebuild or degraded status
- enrichment failures do not advance a clean checkpoint past missing durable
  outcomes
- metrics expose checkpoint applied sequence, status, replay tail size, and
  repair-scan counts per projection

## Runtime Ownership

DB background work uses a swappable execution capability instead of each
subsystem owning private threads. Native/server deployments attach a node-owned
runtime shared by DBs and stores on the node. Tests, embedded use, single-DB
usage, and WASM keep synchronous or manually pumped fallbacks.

The current model is generic runtime ownership plus typed subsystem adapters:

- the node owns capacity and lifecycle through `BackendRuntime`
- DBs borrow that runtime through `OpenOptions.backend_runtime`
- standalone DB opens may create an owned fallback runtime
- `BackendRuntime` owns the optional `std.Io.Threaded` provider, the
  owner-scoped `DurableJobLane`, and owner id allocation
- typed adapters own the meaning of their work, including derived replay, LSM
  flush/compaction, text merge, enrichment replay, TTL cleanup, and transaction
  recovery
- request-local fanout uses operation execution context and `std.Io.Group`,
  not the durable background-job lane

The important rule is that background execution is an optimization over the
inline path, not a separate durability or visibility model. Correctness cannot
depend on OS threads existing.

Runtime-backed work follows these boundaries:

- each backend owns its own flush/compaction state, queue limits, shutdown flag,
  and write-pressure policy
- the node runtime owns global worker limits so many DBs and stores do not
  multiply thread counts
- durable jobs carry an owner id so close/drain can target one DB/store/shard
  without disturbing other owners
- `BackendHandle.close()` drains or cancels submitted work before destroying the
  backend
- WASM progress is available through inline execution or explicit bounded
  maintenance/executor polling
- DBs own fallback runtimes only when no node runtime is provided

Current status:

- `BackendRuntime` is heap-owned at node/server construction sites and borrowed
  through DataServer, provisioned, hosted, metadata, and standalone DB open paths
- derived replay, full-text merge, enrichment replay, TTL cleanup, transaction
  recovery, and LSM background flush are under the shared runtime model
- `DurableJobLane` has inline and threaded implementations
- LSM has a typed adapter over the durable lane and blocks writers when bounded
  deferred immutable queues are full
- close/drain, inline/WASM-style progress, native threaded sharing, and
  owner-scoped DB runtime tests cover the lifecycle contract
- raft replica placement reconciliation remains foreground control-loop work,
  while distributed transaction recovery is already runtime-owned

## Storage Backend Boundary

The DB layer should keep most of `antfly-zig` pure Zig and portable while
isolating unavoidable OS-specific storage behavior behind a narrow backend
boundary.

The current shape is:

- higher-level runtimes, indexing, query, and most tooling no longer depend on
  `std.c`
- the remaining dense POSIX surface is concentrated in the LMDB backend
- shared backend contracts and adapters exist
- concrete backends exist for LMDB, in-memory KV, and durable prefix/LSM
- backend conformance coverage exists
- top-level DB primary-backend selection exists
- snapshot export/restore and split semantics are backend-neutral, with
  backend-specific fast paths where useful

DB-level durable-LSM coverage now proves the backend seam across:

- basic read, write, and reopen
- full-text persistence and reopen
- derived replay on reopen
- delete-index persistence
- indexed deletes and overwrites
- dense, sparse, and graph query flows
- `_embeddings` and `_edges` document special-field mutations
- split and merge cutover flows
- snapshot restore
- TTL lease-owned cleanup
- named-query fusion and graph expansion
- chunked dense-index and chunk-enrichment reopen flows

That means the remaining work is mostly confidence and product-boundary work,
not backend abstraction bring-up.

The goal is not to remove LMDB now. The goal is to keep LMDB as one backend
while making a future portable pure-Zig backend possible without leaking LMDB
assumptions upward.

### Backend Contract

Any DB storage backend needs to provide these semantics regardless of its
implementation strategy:

1. Transactions
   - read-only snapshot transactions
   - exclusive write transactions
   - nested child write transactions, or an explicit unsupported contract
   - commit and abort semantics
2. KV and range access
   - logical namespaces or partitions
   - point get, put, and delete
   - ordered iteration over key ranges
   - duplicate-key or multi-value support where callers rely on it
3. Durability
   - no-sync, data-sync-only, and fully durable commit policies
   - crash/reopen semantics documented at the backend boundary
4. Visibility and concurrency
   - when committed writes become visible to later readers
   - what long-lived reader snapshots see while writers commit
   - writer contention behavior
5. Maintenance operations
   - reopen
   - truncate or compaction equivalent
   - split/export/import hooks used by higher-level DB flows

The first backend-neutral code pieces are in:

- [pkg/antfly/src/storage/backend_types.zig](pkg/antfly/src/storage/backend_types.zig)
- [pkg/antfly/src/storage/backend_adapter.zig](pkg/antfly/src/storage/backend_adapter.zig)
- [pkg/antfly/src/storage/backend_lmdb_adapter.zig](pkg/antfly/src/storage/backend_lmdb_adapter.zig)

Those model:

- durability expectations
- read visibility
- read/write transaction modes
- logical namespaces or partitions
- cursor open requests
- ordered range-scan requests
- write-batch capability hints
- cursor start/seek/iteration semantics

### LMDB Boundary

The intentional LMDB/POSIX surface is concentrated in:

- [pkg/antfly/src/lmdb/env.zig](pkg/antfly/src/lmdb/env.zig)
- [pkg/antfly/src/lmdb/commit_support.zig](pkg/antfly/src/lmdb/commit_support.zig)
- [pkg/antfly/src/lmdb/readers.zig](pkg/antfly/src/lmdb/readers.zig)
- [pkg/antfly/src/lmdb/split_support.zig](pkg/antfly/src/lmdb/split_support.zig)
- [pkg/antfly/src/lmdb/writer_lock.zig](pkg/antfly/src/lmdb/writer_lock.zig)
- [pkg/antfly/src/storage/lmdb.zig](pkg/antfly/src/storage/lmdb.zig)

These files encode real storage semantics:

- mmap-backed page storage
- read snapshot visibility
- single-writer coordination
- durability and fsync behavior
- file growth and publication
- lock-table and reader-table behavior

Higher layers should depend on the backend contract instead of those details:

- [pkg/antfly/src/storage/docstore.zig](pkg/antfly/src/storage/docstore.zig)
- [pkg/antfly/src/storage/persistent.zig](pkg/antfly/src/storage/persistent.zig)
- [pkg/antfly/src/storage/wal.zig](pkg/antfly/src/storage/wal.zig)
- [pkg/antfly/src/storage/db/db.zig](pkg/antfly/src/storage/db/db.zig)
- [pkg/antfly/src/storage/hbc_adapter.zig](pkg/antfly/src/storage/hbc_adapter.zig)

They can use transactions and range scans, but should not depend on LMDB reader
tables, mmap assumptions, env refresh mechanics, file naming, or lock-file
details.

LMDB commit/publication stats are useful operational hooks, but they are
backend-specific extensions, not required backend-neutral semantics. The neutral
contract should cover correctness, durability policy, visibility, range access,
and split/export/import semantics.

### Backend Migration Plan

1. Freeze the boundary.
   - keep non-backend code off direct POSIX where practical
   - document transaction, scan, durability, and visibility semantics
   - identify higher-level files that still depend on backend details
2. Define shared backend types.
   - shared durability enum
   - shared backend options
   - shared namespace concept
   - shared write-batch capability
   - explicit transaction and cursor capability surface
   - backend-independent error mapping where possible
3. Move higher layers to the contract.
   - first adopters are `docstore.zig`, `persistent.zig`, and `wal.zig`
   - then reduce direct transaction threading in `hbc_adapter.zig`,
     `persistent.zig`, `docstore.zig`, and `index_manager.zig`
4. Keep proving the abstraction with multiple backends.
   - LMDB remains the mmap/single-writer backend
   - in-memory KV stays useful for tests and constrained environments
   - durable prefix/LSM is the portable backend direction

The likely Zig shape is intentionally narrow:

- a small backend module with shared option and durability enums
- a vtable-backed runtime object for environment open/close, transaction begin,
  namespace selection or binding, and sync/reopen helpers
- backend-specific transaction and cursor handles stored behind opaque pointers

This avoids forcing the whole codebase into a large generic type cascade while
still making backend behavior explicit.

Specialized engines such as the text persistent index, HBC, sparse, and graph
reverse index may continue to carry backend assumptions while the primary DB
store remains backend-selectable. Replatforming them onto the same backend
family is a follow-on decision, not a blocker for the primary store contract.

## Local Shard Backend Roadmap

The local-shard migration target is to make `ZigCoreDB` the real backend while
keeping the Go `DB` interface stable for callers above `StoreDB`.

The migration rule:

1. keep the Go `DB` interface stable
2. move the hot local data plane fully into Zig
3. keep distributed orchestration in Go
4. keep local control methods typed, not generic JSON

Treat the Go `DB` interface as three layers:

1. hot data plane
   - `Get`
   - `Scan`
   - `Batch`
   - `Search`
2. local control plane
   - `Open` and `Close`
   - range and split-state methods
   - schema/index control
   - snapshot, split, and finalize
3. higher-level local features
   - transactions
   - graph traversal
   - enrichment entrypoints

Current state:

- `Batch` already uses a typed binary C boundary
- `Search` has hot binary paths for dense kNN and simple text match
- `Get` and `Scan` have narrower Zig bridge fast paths than before
- remaining overhead is mostly Go rebuilding generic request or response
  structures around the Zig engine

### Data Plane Migration

Phase 1: finish the hot data plane.

- keep `Batch` on the typed C ABI path
- make `Search` binary-by-default internally, with fallback for rich legacy
  shapes
- keep shrinking `Get` and `Scan` result-shaping overhead
- add batch/multi-search support once single-request hot paths are stable

Acceptance:

- local dense/text search no longer pays generic JSON overhead
- `Get`, `Scan`, `Batch`, and narrowed `Search` cross the Go/Zig boundary in
  compact typed formats

Phase 2: port the local control plane.

- keep typed methods for `Open`, `SetRange`, `GetRange`, split-state CRUD,
  `AddIndex`, `DeleteIndex`, `UpdateSchema`, `Snapshot`, `Split`, and
  `FinalizeSplit`
- expose dedicated C API entrypoints instead of generic payloads
- keep Go as a thin shim over Zig C API

Acceptance:

- `ZigCoreDB` is mostly a binding layer, not a compatibility adapter

Phase 3: port remaining local features where profiling says it matters.

- transaction lifecycle and local recovery
- graph edge and traversal ops
- enrichment/local derived-batch entrypoints

The rule is to move these only when they are local-engine work, not distributed
coordinator work. Go keeps raft/distributed ownership; Zig owns local shard
execution for the bulk of `DB`.

## Hot-Path Search Wire

The hot-path search wire reduces Go/Zig boundary cost for local shard search by
replacing generic JSON request/response payloads with a narrow internal binary
codec for the hottest `coreDB.Search` shapes.

The public HTTP/store API stays JSON. Only the internal `coreDB` hot path moves
to a binary codec.

Initial shapes:

- dense kNN
- simple full-text without stored fields, aggregations, graph, explicit sort,
  or cursor

Principles:

- keep JSON fallback for richer search shapes until coverage is complete
- use a fixed header, append-only evolution, and offsets into trailing blobs
  for variable-width fields
- prefer packed IDs and packed hit metadata over per-hit allocation

### Dense Search Wire

Objective:

- remove JSON request build and generic result rebuild for dense search

Plan:

1. add a binary dense request/response codec in Zig C API
2. expose a dense wire entrypoint from
   [pkg/antfly/src/capi/db.zig](pkg/antfly/src/capi/db.zig)
3. add the matching Go-side codec in the zigdb bridge
4. route the narrowed dense path through binary wire first
5. keep the current JSON path as fallback

Acceptance:

- local dense search no longer marshals JSON on the hot path
- the Go adapter no longer rebuilds generic hit payloads before constructing
  `vectorindex.SearchResult`

### Simple Full-Text Search Wire

Objective:

- remove JSON request build and generic result rebuild for simple full-text

Plan:

1. add a binary request/response codec for `query_string`, `match`, `term`, and
   `match_phrase`
2. limit the first slice to no stored fields, aggregations, graph, explicit
   sort, or cursor
3. route the narrowed simple text path through the binary wire first
4. keep JSON fallback for richer full-text shapes

Acceptance:

- local simple full-text search no longer pays JSON/base64 overhead on the hot
  path
- small full-text searches are no longer dominated by bridge overhead

### Search Wire Evolution

The internal search wire should have:

- `magic`
- `version`
- `op`
- `flags`
- append-only evolution rules
- variable-width data in trailing blobs referenced by offset/length
- batch/multi-search support once single-search hot paths are stable

## Shard Split Roadmap

Shard splitting should be cheap enough that the system stops paying for full
logical copy plus index rebuild on every split.

The roadmap optimizes:

1. child shard creation without logical KV replay where possible
2. index handoff without full reindex where possible
3. mixed-range cleanup only where strictly necessary

The current state after recent split work:

- child docstore creation is page-level on Zig LMDB
- parent docstore reclaim is page-level on Zig LMDB
- text indexes use segment handoff and mixed-segment rewrite instead of full
  child rebuild and per-doc parent text deletion
- the next remaining split cost classes are non-text indexes, especially dense
  vector indexes

Principles:

1. Copy immutable state; do not replay documents unless forced.
2. Rewrite only mixed ranges.
3. Keep parent cleanup separate from child image construction in the first
   page-level implementation.
4. Add metadata first so split planning is cheap and deterministic.
5. Prefer subtree, block, or segment handoff over whole-index rebuild.
6. If rebuild is required, rebuild only the mixed remainder, not the full child
   index.

### Raft-Ordered Split Control State

Online split lifecycle state belongs to the source data Raft group. Source
prepare, start, finalize, rollback, and destination acknowledgements are typed
internal batch mutations and use the same committed-entry index domain as
document writes. They must never be written directly to a side store with a
synthetic sequence, because that can reorder lifecycle state and user data after
replay or leadership changes.

The source `RaftApplyStore` durably owns:

- source split phase and split key
- the source-local split attempt epoch allocated in committed range metadata
- source delta sequence
- destination group acknowledgement and applied delta sequence
- one terminal split high-water mark per source group

Source acknowledgements are metadata-only Raft entries. They bypass the
document DB executor, are folded monotonically across the entire committed
apply batch, and are read only from `RaftApplyStore`. Destination checkpoint
markers remain in the destination DB because they gate destination bootstrap;
they are not a second source-progress authority.

Transition IDs remain opaque idempotency identities and are never ordered.
Each accepted split allocates a monotonically increasing `attempt_epoch` from
the source range record before runtime work is published. Every source command,
destination checkpoint, bootstrap marker, and acknowledgement carries both
values. This permits a new attempt after rollback even when deterministic
planning chooses the same transition ID.

Finalize and rollback atomically persist the terminal high-water mark before
removing active split state. Commands below that epoch are stale and absorbed;
commands at that epoch must match the terminal identity and outcome; a prepare
at a higher epoch may begin the successor attempt. Keeping one record per source
group bounds control-state growth without weakening replay fencing.

Raft group snapshots are versioned and include the range, primary documents,
active split state, delta sequence, pending deltas, source acknowledgement, and
terminal high-water mark. Install validates control-key ownership, duplicate
keys, framing, and encoded control values before atomically replacing document
and control state. Publishing the snapshot's applied Raft index remains a
separate state-machine step; partially installed split lifecycle state must
never become visible.

Snapshot compaction is asynchronous, fair, bounded-memory maintenance:

- each group has at most one coalesced candidate, and FIFO admission prevents a
  continuously written group from starving colder groups;
- build and publish failures retain the latest incarnation-fenced candidate and
  retry with capped exponential backoff without blocking Raft apply or transport;
- shutdown cooperatively cancels the active point-in-time source before joining
  its worker, while source cancellation and destruction remain thread-safe;
- data snapshots stream each MVCC cursor once into a temporary artifact with a
  fixed-size buffer. Current-only AFDS v3 uses terminated records, avoiding a
  separate document-count pass before encoding. Followers validate and install
  from borrowed payload slices in fixed-size batches, without a payload-sized
  copy or document-count-sized descriptor allocation;
- durable replica state publishes payloads by `(index, term)` before advancing
  checkpoint metadata, retains the preceding payload until that checkpoint is
  durable, and loads payload bytes only when snapshot transfer requests them.
  Each payload is an `AFRSPAY` v1 envelope whose SHA-256 binds its index, term,
  declared length, and bytes. Artifact publication enforces the declared length,
  and replica startup fails closed when the referenced payload is missing,
  truncated, has the wrong identity, or fails checksum validation;
- WAL/checkpoint state retains snapshot metadata but does not retain or encode a
  second full copy of the document image. Snapshot payload ownership is
  explicit; current readers accept only the exact current format, with no
  compatibility decoders or inline-payload fallback. WAL Ready deltas remain
  v3 because their record shape is unchanged.
- the transferable state snapshot index and Raft log compaction boundary are
  distinct durable identities. A snapshot captures the latest applied state,
  while the boundary retains the configured trailing log window and its term
  for incremental follower catch-up. Checkpoint v4 persists both identities;
  restart reconstructs the applied watermark from the state snapshot and the
  replication suffix from the compacted index and term.

The data and metadata point-in-time views share the same worker contract, but
metadata remains an in-memory payload because its bounded control-plane state is
small. One declarative metadata key registry drives both snapshot collection and
group-ownership validation. Its exhaustive projection enum and unique descriptor
table prevent a registered durable projection from updating one allowlist while
silently omitting the other. Operators receive only the actionable compaction
metrics: queued candidates, completions, and failures; payload volume, build
timing, and detailed scheduler outcomes remain internal diagnostics.

Transition observation reads only this already-open replicated control state.
It does not open the live table DB, initialize indexes, or compete with the
generation-owned writer. Destination bootstrap and catch-up apply through the
destination Raft group, then acknowledge the resulting checkpoint through the
source Raft group. Retries are idempotent and every transition RPC attempt is
bounded so metadata reconciliation cannot starve metadata Raft heartbeats while
a destination group elects its first leader.

Publication requires the destination's complete configured voter set to report
healthy, a known stable leader, and an acknowledged checkpoint at least as new
as the source delta sequence. Physical handoff scans treat the encoded range as
an optimization and explicitly retain only primary documents owned by the
source group; derived records and unrelated primary records are not lifecycle
state.

### Text Segment Handoff

Text index split should classify active segments using persisted key-range
metadata instead of rescanning the source docstore.

Required metadata:

- `min_doc_key`
- `max_doc_key`

Split behavior:

- `right-only` segments are copied unchanged to the child active manifest
- `left-only` segments stay in the parent unchanged
- `mixed` segments are rewritten into left and right replacement segments
- the original mixed segment is retired from both manifests

Mixed-segment rewrite can be driven from the segment blob itself using
`SegmentReader.storedDocDecompressed(...)` and
`buildTextSegmentFromDocuments(...)`, without rescanning the main docstore.

Temporary filtered mixed segments may be useful as an optimization layer: install
a mixed segment with a shard-side filter bitmap, finish the split cheaply, then
let later compaction produce clean segment ownership.

Acceptance:

- child text indexes are built mostly by manifest/segment handoff
- only mixed segments are rebuilt
- split can defer clean mixed-segment rewrite when correctness is preserved

### Page-Level LMDB Child Image

The child shard's main LMDB image should be built without logical KV replay.

Plan:

1. open a read snapshot on the source env
2. descend once to the split key
3. clone fully right-hand subtrees page-for-page into a fresh child env image
4. rebuild only the mixed branch spine and split leaf
5. emit fresh child meta and freeDB state

First version scope:

- unnamed main DB only
- correctness first
- parent cleanup stays separate

Acceptance:

- child docstore image is created from pages/subtrees, not logical key replay
- only the mixed path is rebuilt logically

Parent finalize should later avoid whole-range logical prune where metadata or
page structure can answer the same question, and should reclaim retired page
ranges and retired segment manifests cleanly.

### Dense Vector Split

Dense vector indexes should reuse existing HBC structure where possible instead
of rebuilding the child from scratch.

Use [HBC.md](HBC.md)
for HBC-specific write routing, search/rerank boundaries, vector ownership, and
bulk-build strategy. This section owns the DB-level split sequencing and
handoff requirements.

Recommendation:

- do not default to a full child rebuild from vectors
- classify existing HBC subtrees
- hand off fully right-hand subtrees unchanged
- rebuild only mixed subtrees
- use a bulk-build heuristic only inside mixed subtree rebuilds

Relevant storage already exists in:

- `hbc_nodes`
- `hbc_quant`
- `hbc_vecs`
- `hbc_meta`

Plan:

1. persist node/subtree routing metadata in
   [pkg/antfly/src/storage/hbc_adapter.zig](pkg/antfly/src/storage/hbc_adapter.zig)
   with `min_doc_key`, `max_doc_key`, and possibly `member_count`
2. classify nodes/subtrees as left-only, right-only, or mixed
3. hand off right-only subtrees by copying node records, quantized blobs, raw
   vectors, and attach metadata
4. rebuild only mixed subtrees for the child and parent
5. use bulk-build only for mixed subtree rebuilds when needed

Current status:

- node/subtree split-range metadata is implemented in `hbc_adapter.zig`
- `splitPlanningStats(...)`, `buildSplitReusePlan(...)`,
  `estimateSplitRebuildWork(...)`, and split-member collection are wired into
  [pkg/antfly/src/bench/hbc_bench.zig](pkg/antfly/src/bench/hbc_bench.zig)
- synthetic HBC workloads are a warning: `kmeans` produced almost entirely
  mixed subtrees, `hilbert` was only slightly better, and measured full-rebuild
  and mixed-rebuild costs were nearly identical because there were effectively
  no reusable right-only dense subtrees
- batched child rebuild dropped dense rebuild cost sharply even without subtree
  reuse
- a first doc-key-local leaf split heuristic slightly reduced mixed frontier
  for `kmeans`, but did not create reusable right-only subtrees; treat it as a
  secondary tuning lever rather than the main dense split strategy
- DB split destination rebuilds child dense indexes directly from HBC
  split-member plans and skips generic dense doc replay for handed-off child
  docs

Acceptance:

- child dense split mostly reuses existing HBC subtrees
- mixed rebuild cost scales with boundary-crossing structure, not full child
  size
- full child vector reinsertion is no longer the default split path

### Sparse And Graph Split

Sparse indexes should progress from forward-entry handoff to block/postings
handoff.

Sparse direction:

- treat sparse index data more like text than dense HBC
- add shardable postings/block metadata
- hand off fully right-only postings blocks
- rewrite only mixed blocks

Sparse current status:

- direct split handoff copies child-side `fwd` / `rev` coverage unchanged and
  rebuilds postings from source chunks while preserving doc-number mappings
- sparse posting chunks persist per-chunk key-range metadata
- sparse terms persist term-level key-range metadata
- fully right-only sparse chunks are copied raw into the child index
- only mixed chunks fall back to filtered rebuild
- split-time generic indexing can skip sparse docs already handed off
- `zig build sparse-test` covers Zig sparse unit behavior

Graph split should use edge ownership and direct reverse-index rebuild instead
of generic doc replay.

Graph current status:

- child graph split rebuilds reverse state directly from owned outgoing edge
  keys
- graph split no longer depends on generic doc replay for destination rebuild
- graph boundary coverage checks that reverse rebuild respects split ownership
  bounds

### Immediate DB Roadmap

1. keep backend-neutral DB behavior covered across LMDB, memory, and durable LSM
2. use `hbc_bench` split planning output on more realistic dense datasets
3. prototype dense subtree handoff for clearly right-only cases while assuming
   mixed rebuild remains important
4. replace sparse live chunk planning with persisted postings/block routing
   metadata across larger sparse datasets
5. add a smaller durable split prepare/equivalence target instead of relying on
   the heavyweight DB split bench for debugging
6. keep graph split on the direct ownership path and broaden correctness checks
