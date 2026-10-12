# Dense Indexing Lifecycle: Sections Before the October 2026 Condensation

> Relocated verbatim from `zig/DENSE_INDEXING_LIFECYCLE.md` (lines 52–119, 265–337, 485–509, 829–894, 1046–1207, 1426–1513, 1714–1787, 1920–1944 at commit c6c0ba51cd) on 2026-10-12. Durable decisions were folded into [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md) before the move.

These are the long-form originals of sections the design doc now states as compact rules and tables. Where they differ, the design doc and the code win.

## Problem and Terminology (originally lines 52–119)

### Problem

Normal API writes do not enter an explicit table bulk-ingest window; the public
write layer reserves those windows for rebuild and import paths. Before this
design, the asynchronous dense worker nevertheless opened an HBC bulk-ingest
session while catching up normal writes. Opening that session durably writes:

```text
__bulk_publish_state = incomplete
```

HBC refuses to reopen an index while that marker exists. That is correct when a
session has deferred structural mutations that are unsafe until final
publication, but unnecessarily strong for normal replay, where every committed
batch can be structurally reopenable. The failure mode was disproportionate:

```text
normal writes
    -> asynchronous dense replay
    -> long-lived HBC bulk session
    -> process interruption
    -> IncompleteBulkPublish
    -> index quarantined until explicit reconstruction
```

The primary documents and embedding artifacts remain durable, so the dense
index is reconstructible. The problem is the write-session contract, not loss
of source data. Streaming sessions remove the ordinary-write cause; automatic
repair handles the remaining legitimate incomplete publications.

### Terminology

"Bulk ingest" currently covers several independent optimizations and safety
properties. This design separates them.

#### Bulk-optimized batch

One bounded apply call using write optimizations such as:

- grouped centroid routing
- coalesced leaf mutation
- batch-finish leaf splitting
- quantized routing
- reduced flush and manifest overhead
- empty-index bulk building when the batch is large enough

A bulk-optimized batch does not inherently require an index-wide incomplete
publication marker.

#### Streaming replay session

A short-lived sequence of bulk-optimized, independently valid batches. The LSM
backend may coalesce work and defer flush/maintenance across the session, but
each committed HBC batch leaves a structurally valid state.

An interrupted streaming replay session is recovered by replaying from the last
durable applied sequence.

#### Bulk publication session

A construction session allowed to defer HBC structural state across committed
transactions until a final publication boundary. An interrupted session may be
unsafe to reopen and therefore requires either:

- an incomplete-publication marker and artifact reconstruction, or
- isolation in a shadow generation that has not yet become active

This mode is for explicit import, rebuild, or true bulk construction.

## HBC session capabilities (originally lines 265–337)

### HBC session capabilities

LSM batching and HBC cross-batch deferred state are separate concerns. HBC
tracks `write_session_depth` and `write_session_kind`, and safety decisions use
capability predicates rather than raw depth checks:

```zig
fn lsmSessionBatchingActive(self: *const HBCIndex) bool;
fn crossBatchPublicationActive(self: *const HBCIndex) bool;
fn mustPublishMetadataPerBatch(self: *const HBCIndex) bool;
fn shouldPublishSearchStatePerBatch(self: *const HBCIndex) bool;
fn shouldSuppressRoutineMaintenance(self: *const HBCIndex) bool;
```

The classification is:

- LSM batch mode, mutable-state coalescing, deferred commit flush, and bounded
  routine-maintenance deferral may be enabled for either session kind.
- `stageNodeKeyPut` and `stageNodeKeyDelete` operate only while
  `crossBatchPublicationActive()`.
- `shouldDeferQuantizedRebuildToBulkFinish` and
  `shouldDeferLeafSplitToBulkFinish` are true only while
  `crossBatchPublicationActive()`.
- `persistBulkPublishState` and `clearBulkPublishStateTxn` are used only for an
  in-place `bulk_publication`.
- `flushMetadata` publishes reopen-critical metadata in every streaming batch.
  It may suppress that publication only when the matching topology and
  node-key changes are also isolated behind a bulk publication boundary.
- Flat-RaBitQ centroid-directory node keys publish at streaming batch finish
  rather than being held until session finish.
- Search caches, published root/count state, workspace clearing, and
  maintenance suppression are each classified independently. They do not
  inherit publication semantics merely because LSM coalescing is active.

No safety decision outside session begin/finish code may depend directly on
the raw depth.

#### Streaming session APIs

```zig
pub fn beginStreamingReplaySession(self: *HBCIndex) !void;

pub fn finishStreamingReplaySessionWithOptions(
    self: *HBCIndex,
    options: backend_types.BulkIngestFinishOptions,
) !void;

pub fn abortStreamingReplaySession(self: *HBCIndex) void;
```

The streaming finish:

1. finishes any batch-local HBC work
2. flushes the LSM session to a durable reopenable boundary
3. refreshes published search state
4. releases bounded session workspace

It never clears an incomplete marker because it never creates one.

`abortStreamingReplaySession` closes the LSM batching scope, releases session
workspace, and republishes a consistent view of already committed batches. It
does not roll back committed batches and does not advance the applied sequence;
restart or the next catch-up pass may reapply those batches.

The `beginBulkIngestSession` family retains the stronger publication semantics
for explicit bulk construction.

`IndexManager` exposes the same distinction by index name:
`beginDenseStreamingReplaySessionByName`,
`finishDenseStreamingReplaySessionByNameWithOptions`, and
`abortDenseStreamingReplaySessionByName`. Replay batches keep
`BatchOptions.mode = .bulk_ingest` where it is beneficial; that flag selects
batch algorithms and does not imply an unsafe publication window.

## Recovery owner (originally lines 485–509)

#### Recovery owner

Managed startup catch-up, rather than the HTTP repair-job store, owns automatic
restart repair.

Startup catch-up already:

- runs outside request handling in a background thread
- selects locally owned groups from placement and leadership state
- serializes group operations through the local write owner
- publishes startup, progress, and degraded status
- detects zero-progress retries and applies bounded exponential backoff

The HTTP repair-job store remains the operator-facing mechanism for explicit
repair, cancellation, and observability. Making it the correctness mechanism
for startup would introduce a second ownership and scheduling system, and its
table-scoped job shape does not currently identify the locally owned group that
startup is repairing. It does, however, durably retain unfinished operator
cancellation traversals. The API maintenance supervisor automatically resumes
the oldest queued traversal after process restart, one bounded pass at a time
through `BackendRuntime`; a client does not have to call the advance endpoint to
finish cancellation.

Automatic and operator-triggered repair must call the same DB repair engine and
use the same durable repair intent. They differ only in scheduling and policy.

## Disk-capacity ownership and admission (originally lines 829–894)

#### Disk-capacity ownership and admission

Disk admission is capacity policy and belongs to `ResourceManager`, not to the
executor. `BackendRuntime` owns the threads and I/O lanes that run an admitted
repair; a storage backend may install a capacity source while composing
`DB.OpenOptions`. That source returns a stable identity for the physical
volume/quota namespace plus a current optional capacity observation. Table,
group, index, repair, and logical store identifiers are not capacity domains
unless the deployment guarantees that each maps one-to-one to an independently
exhausted allocation.

Provisioned storage installs a live `statvfs` probe for its replica root into
the shared `ResourceManager`. The same observation feeds store heartbeats and
repair admission, so scheduling data and the final admission decision do not
diverge. A backend with a quota or non-filesystem capacity model replaces that
probe through the same capacity-source interface.

Capacity-source installation is immutable after runtime composition. DBs copy
the source when they open and reservations coordinate through its domain ID, so
runtime replacement would create inconsistent admission views. Idempotently
installing the exact same source is allowed; installing a different source or
installing after admission has begun is rejected.

The durable intent stores `planned_disk_bytes`. It is an estimate and does not
claim that an in-memory reservation survived restart. Each attempt measures any
existing candidate, refreshes the observation, reserves only remaining growth,
and retains a node-owned safety floor for WAL/checkpoint/foreground durability.
Unknown capacity remains accounting-only. Known but stale capacity fails
closed. Shadow construction reconciles actual bytes at bounded durable
publication and catch-up boundaries, refreshes the live capacity observation,
and grows the claim before proceeding when the plan is exceeded. Revalidation
uses the aggregate outstanding claim for the physical domain, so a fall in
available capacity stops the next bounded unit even when the original estimate
has not been exceeded. Full-text segment batches, graph batches, dense/sparse
artifact chunks, replay catch-up chunks, and each HBC deferred-publication
window all cross this fence. Admission never performs a recursive directory
walk on the foreground write path.

The reservation represents future growth, while filesystem availability
already reflects bytes written by the candidate. Every bounded repair boundary
first performs an O(1) capacity-source refresh and aggregate-claim check. If the
conservative claim would deny progress, the executor measures the complete
shadow generation (index artifacts, checkpoint, and publication markers),
shrinks the future-growth reservation by the materialized amount, and
revalidates before deciding. Major phase transitions also reconcile exactly.
This prevents false admission failures as a large rebuild approaches its
estimate without turning segment-heavy builds into repeated recursive walks.
The preliminary fit probe updates the capacity observation but does not record
an admission denial; only a stale observation or the final decision after exact
reconciliation increments denial metrics. Thus operator alerts describe work
that was actually stopped rather than conservative estimates corrected in the
same pass.
Ordinary ingest and query paths never perform this measurement.

Unsupported capacity observation is treated as unknown capacity rather than an
endless repair wait. On a backend that does support observation, transient probe
failure fails new repair admission closed and retries with durable backoff. Store
heartbeats retain the last successful capacity value across that transient
failure and expose a failure counter instead of publishing a false zero-capacity
sample.

Claims coordinate all DBs sharing a manager. They do not coordinate independent
processes on one volume. Production deployments must therefore enforce one
writer process per capacity domain or provide a capacity source backed by an
OS quota, preallocation scheme, or shared allocator. This invariant is part of
the storage backend contract, not an application-facing tuning switch.

## Repair engine and scheduler (originally lines 1046–1207)

#### Repair engine and scheduler

Startup catch-up owns detection and policy, but it must not synchronously run a
multi-minute rebuild inside the serial table/group inspection loop. Separate
cheap discovery from expensive execution.

Add a bounded DB discovery pass:

```zig
pub const StartupIndexRepairDiscovery = struct {
    discovered: usize = 0,
    already_pending: usize = 0,
    terminal: usize = 0,
};

pub fn discoverRecoverableStartupIndexFailures(
    self: *DB,
    alloc: Allocator,
    limit: usize,
) !StartupIndexRepairDiscovery;
```

Discovery enumerates configured indexes, classifies load failures, validates or
creates durable intents, and publishes repair debt. It performs no corpus scan
and returns quickly so one broken group cannot head-of-line block inspection of
all later groups on the node.

A node-local repair executor consumes intents selected by the managed startup
owner. Notifications are latency hints, not a correctness mechanism: the
executor scans durable intents at startup and periodically thereafter, so a
lost notification, executor restart, or temporarily closed DB cannot strand
repair debt. It reopens the DB through the managed owner for each claim and
does not retain an unsafe raw DB pointer across placement or lifecycle changes.
Startup discovery and operator repair controls enqueue or remove the exact
affected group. They do not route through broad startup catch-up, and high-rate
ordinary document writes never mark the repair scanner dirty.
Named-index pause, resume, cancellation, and successful repair passes retain the
group in the queue until an aggregate owner-side audit has checked every index
in that group. This prevents an operation on one index from dropping independent
repair debt for another index; the aggregate audit performs the eventual O(1)
queue removal when the group is clean.
It calls the same refactored DB repair state machine used by explicit operator
repair:

```zig
pub fn advanceIndexRepairIntent(
    self: *DB,
    alloc: Allocator,
    repair_id: u128,
    options: types.ArtifactRepairRunOptions,
) !IndexRepairAdvanceResult;
```

The existing repair engine supplies useful shadow creation, snapshot rebuild,
catch-up, final apply barrier, and pointer-swap code. Before reuse, refactor out
its destructive quarantined-root reopen and add the durable intent, candidate,
and replay-pin transitions defined above.

Start with these scheduler limits:

- at most one active index reconstruction per node
- at most one active reconstruction per group
- bounded round-robin selection across known groups; exact generation sizing
  and capacity admission happen inside the selected DB, avoiding node-wide
  status materialization merely to rank one repair slot
- resource-manager admission for memory and disk capacity, backend-runtime
  execution, and one-per-node scheduling bounds for disk I/O, compaction, and CPU
- a reconstructible disk plan plus a process-local reservation token that accounts for the
  candidate, retained poisoned root, WAL/replay growth, and cleanup reserve;
  free-space preflight alone is not admission because concurrent repairs can
  consume the same bytes
- cancellation and a final fencing check when placement or leadership changes
- a dedicated `BackendRuntime` maintenance owner and cooperative shutdown token;
  shutdown requests cancellation before draining the owner, and cancellation
  preserves resumable durable repair state without recording an attempt failure

The concurrency limit may become configurable after qualification, but
unbounded per-group concurrency is never allowed. Dense repair snapshot batches
are sized from vector dimensions and the currently available `ResourceManager`
repair budget: background planning stays within the soft budget, and the hard
budget remains an authoritative race/safety fence. Dense reconstruction is
time-sliced at reopenable publication boundaries: the source cursor is advanced
only after the candidate slice is durable, and the linked group cursor then
rotates the node slot. The production default is a 15-second scheduler budget;
the scan observes it only after a resource-sized batch, avoiding per-document
clock calls. Pre-activation replay observes the same budget after each durable
replay window. Final replay while activation fences are held does not yield; it
remains bounded by the activation deadline and aborts the swap if it cannot
converge. Other index families retain their existing bounded/cancellable builders
and require the same largest-incident SLO proof before higher concurrency is
enabled.

The scheduler publishes queue depth, oldest-intent age, bounded-scan work,
attempt outcomes, disk-admission waits, and current/peak resource-manager disk
claims. Detailed per-intent phase, retry deadline, sequences, reservation, and
last error remain available through authenticated operator diagnostics and
structured logs. Projected completion is intentionally omitted until measured
throughput makes it defensible. Initial concurrency is one reconstruction per
node. Higher configurable concurrency is enabled only after multi-group
qualification proves that memory, disk, replay retention, query latency, and
activation-pause budgets remain bounded. The periodic lost-wakeup interval and
repair-capacity SLO are internal node policy with conservative defaults. Known
debt is held in an intrusive linked hash queue: enqueue and removal are O(1),
each pass snapshots at most 32 entries, and durable-intent transitions directly
supply the group and table route. Creating, resuming, pausing, or removing an
intent therefore updates scheduling without an administrative snapshot. Metrics
inspect at most the same bounded window; larger queues conservatively keep the
executor runnable instead of performing a full scan.

Retry has two scopes. Failures returned by the DB state machine persist their
deadline in the durable intent. Failures outside that state machine, such as a
DB-open, routing, or allocation failure, use deterministic jittered exponential
backoff from 30 seconds through 10 minutes in the node-local monotonic clock.
Group-specific failures park only that group and preserve any later durable
deadline already observed. Executor-wide allocation and queue-snapshot failures
park the maintenance owner without rewriting every queue entry. Fallback-route
refresh has a separate deadline: its failure suppresses only lost-wakeup
discovery and never blocks an exact durable-intent notification that already
carries its table/group route. A successful pass clears the corresponding
failure state, and an explicit durable wake clears stale per-group scheduler
backoff. Consequently an unhealthy group or metadata fallback cannot turn the
five-second executor poll into a hot retry loop or delay unrelated runnable
groups. Durable retry deadlines are realtime so they survive restart; they are
translated once into monotonic deadlines when inserted into the process-local
scheduler, and the two clock domains are never compared directly.
Filesystem-observation freshness is monotonic end to end. Transient repair-job
dispatch, ownership, routing, and storage failures return the durable API job
to bounded exponential backoff; only malformed or semantically invalid requests
become terminal.

A successful DB pass that still reports durable debt must first restore the
exact group queue entry. If that allocation fails, the executor records global
scheduler backoff and returns before publishing fallback cursor progress. This
ordering makes queue notification and cursor advancement one logical outcome
without putting the fallback cursor itself on the durability path. The periodic
discovery anchor is rewound on that failure, so the same cursor becomes eligible
as soon as scheduler backoff expires rather than waiting another discovery
interval. The same rewind applies when fallback candidate allocation or
failure-backoff enqueue cannot allocate.

Lost-wakeup reconciliation uses a compact node-local routing index rebuilt only
when the metadata epoch changes. Steady-state passes neither clone nor free the
cluster-wide administrative snapshot. The fallback window adapts between 16
and 256 routes per pass to target a 30-minute full rotation at the configured
discovery interval, and advances a separate cursor even when queued debt exists.
The 256-route ceiling bounds DB opens and metadata work; deployments beyond the
resulting supported envelope report an explicit rotation-SLO violation rather
than hiding unbounded recovery latency. If queued work consumes the repair slot,
a safe-consumed prefix advances past non-local or already-queued entries but
stops at the first uninspected local candidate. The fallback is a safety audit;
direct durable-intent notifications and startup reconciliation are the primary
discovery paths, so normal recovery latency does not scale with a full cursor
rotation.

Queue ownership decisions are tri-state. `attempt` schedules eligible debt,
`retry_unknown` retains it in-place while metadata and Raft leadership converge,
and only `skip_nonlocal` removes it. The live membership source produces that
decision only from a known voter set in which the local node is absent; a
known local voter without leadership remains queued through elections. The
lost-wakeup cursor is therefore a
notification safety net, not the correctness path for temporary ownership
ambiguity.

## Automatic repair test matrix (numbered form) (originally lines 1426–1513)

#### Automatic repair test matrix

The minimum deterministic test matrix is:

1. Create an external dense index, persist vectors, leave
   `__bulk_publish_state`, run managed startup catch-up, and verify the index is
   automatically searchable without an API repair request. Repeat with the
   durable artifact target counter removed to exercise snapshot-plus-
   delta bootstrap.
2. Stop after intent persistence and before candidate creation; restart must
   retain the quarantined root and resume.
3. Stop during snapshot construction; restart must either resume a validated
   checkpoint or discard only the candidate and rebuild it.
4. Write documents after the snapshot floor while the shadow is building;
   replay truncation must remain pinned and the final index must include them.
   For a missing-counter quarantined dense index, perform artifact insert,
   replacement, and deletion while the bootstrap snapshot is open; verify the
   signed delta, durable target count, and repaired search result.
   Repeat deletes through the normal document path and TTL cleanup, and route
   generated embedding writes/deletes through a remote document-child batch;
   each primary mutation, replay record, and counter delta must be atomic.
5. Stop after candidate readiness but before pointer swap; restart must validate
   and activate the identified candidate without rebuilding it.
6. Stop after pointer swap but before replacement validation, checkpoint
   publication, replay-pin removal, and intent deletion. Each restart must
   deterministically finish the remaining transition without losing the new
   active root.
7. Inject `UnsupportedVersion`; it must remain terminally degraded and must not
   delete the root automatically. Inject allowlisted `Corrupted`, `NotFound`,
   and `FileNotFound` backend validation failures; they must fail closed and
   reconstruct through a separate generation without mutating the poisoned root.
8. Remove or corrupt required source artifacts; preflight must become terminal
   without creating or activating a candidate.
9. Verify only the active local owner repairs a group, then transfer leadership
   during build and immediately before activation.
10. Fill the filesystem during candidate creation, build, readiness publication,
    activation, and cleanup. The active/quarantined state and intent must remain
    coherent after every failure.
11. Corrupt or truncate the intent and candidate marker. Unknown versions and
    malformed data must fail closed.
12. Cancel repair, drop the index, and replace its configuration during each
    long-running phase; verify replay-pin and candidate cleanup.
13. Run multiple broken indexes across multiple groups and verify the node-wide
    concurrency limit, fairness, and absence of startup inspection head-of-line
    blocking.
14. Reproduce a 100,000-document interrupted external-vector ingest; restart
   without an API call and verify document count, dense count, search results,
   zero remaining load failures, and removal of the repair intent.
15. Run at least one million documents with concurrent writes and queries,
    repeated forced termination, bounded memory/WAL/disk growth, and recall
    comparison against a clean build.
16. Pause truncation immediately before pinned-snapshot acquisition, race the
    truncator against snapshot creation, and verify that either the provisional
    zero pin or final floor protects every required replay record across crash.
17. Exercise every transition in the state table twice and crash between its
    filesystem effect and durable phase update; restart must converge without
    activating an unvalidated candidate or deleting the active root.
18. Run full durability and relaxed/`none` durability modes, including LMDB
    `no_sync`; verify no applied watermark or truncation advances before the
    explicit durable sync and that sync failure preserves replay debt.
19. Replace replica identity and root generation, promote an HA replica, move
    and split a group, and perform logical and physical restore. A local
    candidate may resume only on an exact identity match.
20. Sustain writes above and below candidate catch-up throughput. Barrier hold
    time must remain below the configured maximum; non-convergence must return
    to `waiting_for_convergence` and eventually apply documented backpressure.
21. Drop executor notifications and restart the executor while intents exist;
    adaptive bounded durable discovery must reclaim each eligible intent within the
    configured scan SLO without duplicate execution.
22. Cancel the current attempt, durably pause automatic repair, restart, and
    resume. Only cancel permits automatic retry; pause survives restart.
23. Exhaust soft and hard replay, snapshot-pin, and disk budgets. Verify
    reservation fairness, the stable `dense_repair_backpressure` response with
    `Retry-After`, hysteretic recovery, and absence of filesystem exhaustion.
24. Queue differently sized repairs long enough to exercise size-aware
    selection and aging. Assert bounded oldest-intent age and no starvation.
25. Hold enough dense-repair working-set capacity to cross the soft background
    budget without reaching the hard limit. Counter bootstrap and shadow batch
    planning must defer without consuming additional bytes or recording a hard
    rejection.
26. Under hard replay pressure, verify standalone writes and only the active
    managed-Raft leader reject uncommitted work with the compact retryable 429;
    committed entries must continue applying on every replica. Remove pressure
    and verify admission recovers without a configuration change.

This track prevents wipe/re-ingest and does not require changing the successful
normal-ingest path. The streaming-session track separately prevents ordinary
replay from creating this failure state in the first place.

## V1 mirror versus native-v2 authority (originally lines 1714–1787)

#### V1 mirror versus native-v2 authority

The storage-mode boundary is explicit and must not be inferred from the
presence of an optional posting generation:

- A v1 index remains LSM-authoritative, matching the v0.2.0 lifecycle. It may
  publish an immutable posting mirror for query acceleration, but mutations and
  their inverse rollback continue through the HBC LSM.
- Only a capability-authorized private candidate or an index selected by the
  incompatible v2 pointer may route mutations exclusively through the native
  posting WAL. Enabling that path without transition authority is an
  authorization error.
- A native capture pins its immutable base. Cancellation restores that base
  whether the candidate has published its irreversible authority marker yet or
  not; marker publication and rollback ownership are separate states.
- A delayed source callback below the capture's base coverage fails closed. It
  cannot relabel mutations at the current epoch. Legacy owners apply their true
  LSM inverse before cancellation; native owners restore the pinned generation.

The term `posting sidecar` predates this distinction and is ambiguous. In v2,
the posting segment store is retained because it is the index authority; only
the compatibility HBC LSM is retired. Future API cleanup should use
`native_posting_store` for v2 and reserve `sidecar` for the optional v1 mirror.

##### Rollback capability is capture-local

Mutation-store mode and rollback authority are not equivalent. An authorized
bootstrap or repair candidate can select native mode before its first complete
immutable generation exists. During that window mutations still reach the
compatibility LSM, and cancellation has no native base to restore; skipping
the inverse rollback merely because native mode was selected could retain a
phantom derived vector after the primary mapping transaction failed.

Rollback therefore asks whether the active capture actually owns a pinned
immutable base. A base-owning capture restores that generation in constant
time; a base-less candidate and a v1 mirror perform the real LSM inverse. Tests
cover all three states. Native-store enablement returns a runtime
authorization error instead of relying on `std.debug.assert`, preserving the
authority fence in production `ReleaseFast` builds without adding work to
mutation or query hot loops.

##### Artifact identity is a set

The exact-vector projection does not infer one artifact family from
`embedding_name` or the index name: indexes may union several embedding
sources, and the external single-source form may use an artifact name that
differs from its index name. The durable contract is shared by backfill,
mutation-WAL publication, mmap projection reads, topology projection builds,
snapshot construction, and readiness:

- a single-source index owns exactly its configured `embedding_name`, falling
  back to the index name only when no name was configured;
- a multi-source index owns the configured set of artifact families and each
  posting member retains its exact source artifact key;
- table-wide generation manifests declare the union of those scopes;
- base cardinality readiness sums the per-source certificates for the logical
  index instead of consulting an index-name surrogate.

The common single-source lookup stays at one derived key and adds no query
I/O; multi-source projection lookup uses the already-materialized member key
directly. Tests publish both source families through the native mutation WAL
and certify their combined immutable base coverage.

##### Orphan deletion is reauthorized

The initial canonical-pointer inventory is an efficient filter, but it is not
deletion authority: another manager can publish a repair generation after that
scan and clear its construction marker. Cleanup re-reads all canonical
pointers for each unmarked deletion candidate immediately before touching its
filesystem or algebraic state. The required publication order is a complete
handoff: the construction marker protects the pre-pointer window and the
revalidated pointer protects the post-publication window. Invalid pointers
fail closed. A deterministic hook test publishes and clears the marker exactly
between inventory and deletion and verifies that the live generation survives.

## Rollout counters (originally lines 1920–1944)

### Rollout

Streaming replay is the ordinary-write correctness path, not a user-selectable
index behavior. It is enabled unconditionally and has no public or
resource-configuration toggle. Production rollout uses the normal staged binary
deployment and canary process; rollback means deploying the prior binary rather
than retaining the unsafe ordinary-replay bulk-publication path in the runtime.

Emit counters for:

- streaming sessions opened, finished, and aborted
- bulk publication sessions opened, finished, and aborted
- streaming replay batches reapplied after restart
- incomplete publication quarantines
- automatic reconstruction attempts and outcomes
- repair executor queue depth, oldest age, admission delay, projected
  completion, durable-rescan recoveries, and active count
- repair replay-pin age and retained replay bytes
- repair snapshot age and pinned-version bytes
- candidate actual/reserved bytes, stale candidates removed, and cleanup
  failures
- repair phase duration, retry count, and terminal outcomes
- convergence lag, estimated and actual activation pause, and barrier retries
- dense-repair throttling and backpressure rejections
- session finish latency and maximum replay lag
