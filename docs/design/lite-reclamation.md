# Native Lite reclamation

Issue: [#939](https://github.com/antflydb/antfly/issues/939).

## Decision

Native Lite owns incremental retirement, page reuse, maintenance scheduling,
shrinking, atomic publication, and retained-reader accounting. Applications supply
budgets; ordinary overwrite workloads do not require a full rewrite.

The owner creates revision-4 files (`AFLITE\x04P`) by default. Writable revision-3
owners migrate through an explicitly identified compact image and atomic generation
adoption. Read-only opens preserve revision 3. Raw native format primitives retain
their revision-3 default for compatibility; `CreateOptions.indexed_reclamation`
selects revision 4. Older binaries reject the new signature. `page_reuse = false`
opts out of creating/migrating revision 4; an existing revision-4 file still uses
its required ownership machinery.

The implementation uses journaled ownership counters and a hierarchical free-page
bitmap. This replaces the proposed free-extent tree: dense counters also represent
shared value ownership and packed-slot occupancy, and a bitmap remains compact
under arbitrary fragmentation. Allocation prefers the lowest free page, naturally
consuming contiguous runs, with eight summary levels rather than a file scan.
Journal replay and periodic metadata checkpoints supply durable free capacity;
commits do not serialize the entire free set or traverse historical records.

This is an asymptotic design choice, not a claim of globally optimal measured
latency. The memory/write tradeoff and qualification requirements are below.

## Owner policy and independent shrinking

`storage/lite/reclamation.zig` contains the policy; `docstore.Store` owns its task,
cancel token, snapshots, resource admission, and result. Options propagate through
the native handle and embedded Zig facade. Existing C ABI status serialization
includes the handle's reclamation status without changing ABI function signatures.
Budget and scheduling overrides are currently available through Zig open/create
options; the C ABI uses the defaults. A versioned C options extension and binding
updates are a separate API task rather than an incompatible struct expansion.

For example, an embedded owner can open with:

```zig
var db = try embedded.DB.openLite(allocator, path, .{
    .lite_reclamation = .{
        .max_storage_bytes = 4 * 1024 * 1024 * 1024,
        .disk_headroom_bytes = 128 * 1024 * 1024,
    },
});
defer db.close();
```

Assessment starts once the file reaches 256 MiB by default. After each exact
assessment, the next scan requires file growth of at least the larger of 64 MiB
and one quarter of the assessed physical size, or proportional committed data
retirement. Charging scan work to proportional change prevents repeated full
live-record traversals every 64 MiB on a large, densely live store. Retirement activity includes released
physical data pages and retired inline record bytes, so partially live packed
pages can also trigger compaction. Shared external value lengths are excluded;
retiring an append's old record never counts its entire shared value. Activity is counted at
publication from scalar allocator state; aborted transactions and allocator
housekeeping do not contribute. The retirement interval is the larger of one
quarter of the assessed physical size and the smaller of the configured growth
interval and minimum reclamation threshold, so deleting data can trigger
shrinking while physical size remains stable, including within partially live
packed pages. Both growth and retirement accounting are constant-time scalar
checks; neither requires a fragmentation walk to decide whether a scan is due.
Successful rewrites reset both
assessment baselines; reopen assesses afresh. An assessment walks live ordered-index metadata, not historical chains
or external value payloads. A rewrite is eligible only when both physical size
exceeds twice estimated compact size and estimated savings reach 256 MiB. These
are provisional, configurable defaults; sustained benchmark qualification must
precede a release-level numerical performance promise.

The task is lazy: files without retirement debt or eligible shrinking do not
consume a concurrency lane. Mutations signal one owner task and requests coalesce.
Busy publication, resource deferral, and failed service retry
after the configured interval. An active online capture or assessment parks the
worker until completion signals it; eligible debt never causes a no-progress spin.
Resource retries reuse an exact estimate keyed by owner generation, complete
checkpoint, and destination format, without consuming the proportional scan
budget. If roots change below that budget while resources remain blocked, the
retry checks the known workspace before opening another assessment snapshot.
A stale hint expires after the greater of 30 seconds and 64 times the previous
scan duration. Changed roots are then reassessed even under continued pressure,
so deletions or smaller replacements can make shrinking fit a fixed quota
without further writes. This bounds repeated scan cost under sustained mutation;
unchanged checkpoints keep their exact estimate regardless of elapsed time.
After a writer-busy publication attempt, retries check both the active writer and
queued writer tickets before scanning or copying another image. This admission
check does not reserve the slot across copying: online capture continues to allow
foreground writes, and writer release wakes maintenance. Retirement service runs
independently even while image preparation is gated by a busy writer.
Proportional activity still permits a fresh assessment under pressure; when
resources return, changed roots are reassessed before reserving or copying an
image. Adoption invalidates the estimate, including when page IDs are reused
in the new inode. Assessment counts report actual live-record walks, rather
than resource-admission attempts.
Construction suppresses worker startup until the returned owner has a stable
address. The native backend activates its fully initialized heap owner before
returning, so remaining reopen debt runs without another operation. Direct
value-based `Store` users call `startMaintenance` after placing the store at its
final address, or use cooperative `maintainOnce`. Read-only handles perform no
maintenance. Writable reopen assesses outstanding debt synchronously, preventing repeated short CLI
sessions from discarding a newly launched background task forever. Close requests
cancellation, wakes and joins the task, then closes the storage runtime. The
publication boundary retains the existing atomic adoption and directory-sync
semantics. A finalized disposable restore owner cancels and joins its maintenance
before surrendering its generation; shutdown does not asynchronously interrupt a
header/rename operation.

If the runtime cannot provide concurrency, the status reports it. Reopen and the
cooperative owner operation can still service debt; the implementation does not
promise an uninterrupted long-running workload will plateau in that configuration.

The policy keeps estimates and their checkpoint sequence separate from current
size. Concurrent writes can make estimates stale. Publication invalidates estimate
freshness. An explicit integrity audit remains independent of routine assessment.

The allocator rejects appended pages before they exceed an optional aggregate
storage budget. Admission subtracts rewrite workspace and retained generations.
Revision-4 foreground publication also preserves capacity for one counter/queue
snapshot, the bounded DFS queue expansion, and two recovery-slot advances. The
reserve includes its own counter pages at a fixed point. It uses scalar ledger
sizes without scanning free pages or value graphs, and scales with physical page
count and pending queue size. Maintenance may use the reserve. Near the hard
limit, service reduces its data batch geometrically before touching the ledger;
normal batches remain available when capacity permits. Budgeted owners checkpoint
sparse journals proactively once the old journal is at least twice the new
snapshot size, amortizing metadata construction. Waiting until collector reserve
is threatened can strand a larger write after data debt has already drained.
Creation checks the format
footprint and reserve against the budget and disk headroom before touching an
existing artifact; an undersized limit fails with `LiteStorageBudgetExceeded`.
Portable restore reserves an exclusive generation workspace before creating its
staging owner. Staging inherits a byte limit after subtracting the live inode and
retained generations; physical shrinking is disabled on that disposable owner
to avoid nesting rewrite workspace. Ordinary staging mutations enforce the
limit and collector reserve. Adoption rechecks aggregate bytes, disk headroom,
and collector capacity before rename. Failed staging-file cleanup runs before
the reservation is released. Pinned old readers continue to count against the
live budget after adoption. Native `Connection` options propagate reclamation
settings through open and create.
The rewrite reserves the larger of twice estimated compact size and an assessment
interval. When capacity or an aggregate budget is available, it constrains allocation in
the initial copy and residual change replay. Exhausting the reservation safely
abandons the prepared generation. It never discards the last valid database to
make room for a new one. Resource admission applies to manual rewriting too.

Native runtime owners probe filesystem capacity and preserve configurable disk
headroom. Borrowed or virtual runtimes can provide a capacity callback; they are
never bypassed with an unrelated host filesystem probe. Capacity is an observation,
not a reservation against other processes. Low capacity defers copying and caps
subsequent appends; I/O failure remains possible after another process consumes
space. Budgets do not change durability settings.

Status includes current-file size, retained old-file size/count, retained readers,
an age diagnostic, reserved rewrite capacity, estimated compact/live size, estimate
sequence, assessment/rewrite counts, last reclaimed bytes, state, reason and error.
Retired inode size is charged once per generation until its last reader closes.
The age diagnostic is the age of that generation's uninterrupted pin interval;
it conservatively overstates age when its first reader has exited but later
overlapping readers remain. External-process readers are not counted by the
in-process registry. Filesystem available capacity still reflects their retention.

## Revision-4 ownership and retirement

Ordered document, metadata, and artifact indexes are authoritative for point
lookups, scans, backup, vacuum, and integrity. Document history and namespace-head
roots are zero. Catalog descriptors contain only the ordered-index root; records
have zero predecessor links. Deletes remove the index entry without creating a
history tombstone. Namespace range scans use document-index prefix bounds.

Each logical record owns one occupancy count on its physical packed page and one
reference to its external value root. Packed slots retire independently; the page
becomes free only after its last slot retires. Long keys occupy independently
typed immutable key pages. Index nodes own their key references; leaf updates
retain the existing key identity, so an internal separator cannot retain an old
record/value payload merely to borrow its key bytes.

Immutable value nodes own their child edges once. Records, append roots, and
renamed values can share roots or subtrees. Retiring a value decrements that
reference; only the final owner queues child decrements. Temporary builder roots
are either adopted by a parent/record or queued as orphans, including intermediate
append frontiers. Streaming vacuum catch-up transfers the same ownership and
builder-root accounting into the unpublished image.

COW index edits queue only replaced/pruned physical nodes and changed records.
Unchanged index subtrees remain owned. A delete queues its old record in constant
payload work; retirement subsequently discovers value children in bounded batches.
The default data budget is 128 graph objects per mutation/service iteration,
configured by `retirement_work_pages`. One extent node can queue at most 64 children. The
budget counts graph objects, including logical slots; it is not a hard time limit.

The durable data queue is ordered by ascending retirement epoch and descending
event ID. Children inherit their parent's epoch and run before its older siblings,
bounding depth-first expansion to 63 siblings per extent level (at most 63 levels).
Foreground pending events are capped at 1,048,576; a separate 4,096-event reserve
allows collector expansion and publication at that limit. A collector completes
the parent before queuing children. Admissions fail with
`LiteRetirementBacklogExceeded` when readers or insufficient service prevent
progress. The file-byte budget independently bounds retained data and workspace.
The owner services idle debt and advances fallback checkpoints even when
`reclamation.enabled = false`. That option controls optional physical shrinking.

## Durable allocator encoding

The checkpoint's allocator root is a checksummed `free_map` page containing
`AFL4ALOC`, version 2, and snapshot, pending-queue, and delta-log roots, next event
ID, covered physical page count, accumulated delta bytes, an exclusive journal
stop pointer, and durable partial-checkpoint roots/cursors. Version-1 roots are
accepted and advance to version 2 at the next publication. Older prototypes
reject version-2 roots before modifying the file. Child pages have the
separate `allocator` page kind. Integers are little endian.

- `L4SS`: counter snapshot chunks, first physical page and packed 32-bit counts.
- `L4RQ`: retirement snapshot chunks, 48-byte events (ID, epoch, reference,
  value length, and kind). Kind 5 is an allocator-chain head; kinds 0–4 retain
  their existing meanings. New readers also accept earlier single-page metadata
  retirements. Earlier revision-4 prototypes reject unknown kinds rather than
  modifying a ledger they cannot interpret.
- `L4DL`: 56-byte tagged counter updates, new retirement events, and completed IDs.

A counter is an ownership count, zero for reserved allocator metadata, or
`0x80000000` for a reusable page. Counter changes and queue deltas append per
publication. Once physical delta-page payload capacity reaches the larger of
256 KiB or twice the counter and queue snapshot size (including unused space in
a commit page), owner maintenance starts an incremental metadata checkpoint.
Foreground commits continue appending journals and contribute either a complete
small snapshot or at most two builder pages, so busy writers make progress
independently of worker scheduling. Small counter/queue pairs finish in one
publication, avoiding an extra recovery
generation under a tight budget. This fast path reserves margin for retiring old
metadata and allocating its replacement; the complete snapshot is capped at
four pages. It never runs with an active incremental builder.
For larger tables, a service batch constructs at
most `min(retirement_work_pages, 16)` snapshot pages (with a one-page minimum),
then publishes its partial roots and releases the owner mutex. Counter chunks
sample the current table; journal entries after the builder's start override
those samples. Pending snapshots include only pre-start IDs, with identical
additions and already-completed IDs reconciled idempotently during version-2
replay. Stable ID links allow queue traversal while other batches remove items.
Reopen restores the builder and resumes it. Completion swaps the snapshot roots
and excludes the pre-start journal using its stop pointer; no full-table copy or
walk occurs at that boundary. The stop page remains reserved until the next
checkpoint so recycling its ID cannot make replay terminate at a new page. Private generation construction may still write a
full checkpoint outside the live owner mutex.
Previous metadata snapshots retire as at most three chain-head events. Service
reads and retires one chain page per work unit, rather than enqueueing an entire
snapshot. Metadata roots and chains have separate queues. Their visibility fence
is the recovery/durability slots: internal data transactions never read allocator
metadata. External snapshot and audit handles still prevent physical overwrite
through the inode lock. Each service batch retires at most
`max(8, retirement_work_pages)` allocator-chain pages. Completed data and
chain objects retain zero-count physical pages as single-page metadata events
until the completion epoch crosses both recovery/durability slots. This preserves
the bytes needed by an aborted transaction's queue and by fallback-ledger replay.
Promotion to the free bitmap is separately bounded by
`max(8, retirement_work_pages * (payload_bytes / 18 + 2))`, covering the preceding
batch's maximum typed-key fanout and publication overhead. Collector queue
pressure limits expansion before consuming publication slots. Data never waits
behind an entire metadata chain. Single-page debt beyond two retained promotions
drains at a net reduction per publication; the last two promotions never start an
idle service loop. Existing single-page metadata retirements remain readable.
Metadata checkpoints preserve live document/value
pages. Metadata reservations consume free bits before
serialization, preventing the allocator from advertising its own pages as free.

Memory is approximately four bytes per physical page plus bitmap summaries (about
0.1% of file capacity at 4 KiB pages), current delta maps, and the bounded pending
queue/heap. This deliberately trades a dense ownership table for low lookup cost.
It is not a constant-memory allocator for arbitrarily large files. At very large
capacities, paged counter caching or a sparse representation would require a
separate measured implementation; no such capability is implied by this version.
Metadata checkpoint work is amortized across accumulated changes; an individual
checkpoint is proportional to allocator metadata size in total, but owner
construction and publication are divided into bounded maintenance batches.

## Checkpoint, reader, and crash safety

Reuse waits for both valid recovery slots, durable roots retained behind unsynced
commits, and the oldest in-process reader epoch. Retirement is stamped with the
actual transaction's next publication epoch, including multiple private index
materializations. Data and allocator roots use the same sync and checksummed slot
publication barriers. Failed transactions reload the previous ledger; partial
private tail pages never enter the committed free bitmap. An uncertain publication
fences mutations until recovery.

Index artifact reads and directory cursors retain an owner read pin for their
entire traversal in addition to fencing generation replacement. Owner read-only
opens wait for a current in-place write to release its inode lock; writer and
maintenance admission remain nonblocking. Raw native opens keep fail-fast
admission unless `wait_for_reader_lock` is requested.

In-process readers use a generation-local intrusive epoch list. Pinning and
removing an arbitrary reader, and finding the oldest epoch, take constant time.
The owner invalidates both writer and generation-reader caches before reusing a
physical ID, including decoded views and cached links. It can reuse pages older
than all pins while newer readers remain active.

External readers retain the conservative whole-file shared lock. If the owner
cannot acquire the exclusive rewrite lock, allocations append while the durable
free bitmap remains intact. This preserves external snapshots without pretending
to provide cross-process epoch registration. Generation rewrites keep old inode
descriptors alive until their final reader exits; those bytes remain charged to
the aggregate storage budget.

Opening an allocator verifies root bounds and checks metadata checksums, cycles,
queue IDs, and counter encoding. Before first reuse, a protected-graph validation
checks both recovery slots and reconstructs ownership from current indexes plus
pending retirement. Explicit integrity checks use the same proof. This one-time
open/reuse proof can read retained values; routine mutations and service never
perform a whole-file trace. Private vacuum publication rebases retirement epochs
and writes a metadata checkpoint when discarding earlier private recovery roots.

## Shrinking

Page reuse is the routine mechanism. Optional automatic shrinking retains the
owner's conservative amplification, absolute-savings, hysteresis, resource,
cancellation, and publication policy. Explicit vacuum is also available. Shrinking
rewrites live indexed data into a compact revision-4 generation, bootstraps its
ownership ledger once, streams concurrent changes, and atomically adopts it.
It therefore returns internal holes and packed-page slack to the filesystem
independently of routine retirement. This implementation uses generation rewriting
for shrinking; it does not implement an in-place tail-truncation/relocation protocol.

## Status and qualification

Status exposes reusable pages, pending retirement objects and pending data objects,
reused pages, and serviced objects alongside current/retired/workspace bytes,
reader retention, shrinking state, estimates, and errors. Reuse/service counters
are scoped to the currently loaded ledger, rather than lifetime-persisted metrics.

The expected growth envelope is live data plus index/key/allocator metadata,
packed-page slack, both recovery generations, reader-retained pages, and bounded
retirement debt. Long-lived readers can retain substantial space; a bounded live
key count alone cannot imply bounded physical space.

Qualification covers overwrite/reopen plateaus with full vacuum disabled, large
free sets, long keys and splits, packed-slot deletion, shared append/rename graphs,
bounded retirement batches, concurrent readers, external-lock fallback, unsynced
commits, fallback recovery, transaction abort, legacy migration, online copy and
catch-up, allocator corruption, allocation failures, saturated queue progress
with a one-object budget, worker parking/backoff, and cancellation inside values. Full storage suites
exercise existing backup/import/encryption/replay/resource contracts. Numerical
throughput, latency-percentile, and I/O promises require workload benchmarks;
correctness tests and structural page-growth bounds do not establish those claims.

`zig build lite-native-benchmark -Doptimize=ReleaseFast` includes a capacity
reclamation workload with 1,024 overwrites of four 8 KiB documents under three
fixed budgets. It reports peak physical bytes, retry/service counts, page I/O,
and p50/p99 elapsed write times including cooperative retirement. This workload
uses `no_sync` and a C allocator to isolate allocator/publication overhead from
sync latency and the test allocator. It asserts the capacity envelope and audits
the final ledger; timing values are observations, not pass/fail thresholds.

## Failure and estimation guarantees

Replacement creation initializes and syncs the complete selected format on the
staged inode before rename; initialization failure preserves the original artifact.
Vacuum reports and hysteresis use the final image size after allocator publication,
including the recovery-slot rebase and its metadata checkpoint.
Secret entry/head writes use the owner transaction queue, including its admission
and rollback rules. Definite pre-publication failures preserve the previous
revision, truncate private tail bytes, and return the original error. Failure
after a checkpoint publication attempt, or failed rollback, fences the store
with `OutcomeUnknown` until reopen.
Compact-size estimation models the destination format, including migration, typed
long-key pages, and the fixed point of allocator root and counter snapshot pages.
Revision 4 omits the legacy namespace directory. Graph audits and vacuum ownership
bootstrap check cancellation per value object and throughout counter/queue replay
and validation, so a single large value cannot defer owner shutdown indefinitely.

Shrinking and restore share final adoption admission. The prepared image replaces
the workspace reservation in aggregate physical accounting; collector capacity
is checked against the post-adoption generation plus retained old
inodes. Both paths recheck disk headroom before rename. A scalar reserve forecast rejects
predictably unaffordable shrinking before image allocation/copy; final admission
checks the actual image and catch-up debt. Rejecting a rewrite preserves the live
inode and its reusable pages. After adoption, temporary accounting switches to
the old descriptors until cleanup closes them, unless retained read generations
already account for that inode. Private vacuum retirement is
normalized before catch-up, outside the owner mutex; catch-up retires at epoch
zero and final sequence publication writes only the header slots.

Worker cancellation reaches allocator reservation, counter/queue construction,
journal emission, and every worker debt probe. Generation adoption invalidates
the cached ledger; the following probe checks cancellation after acquiring the
owner mutex and throughout cold counter/queue replay. Each check occurs before checkpoint publication begins.
Worker status accounting and both estimated and prepared-image admission pass
the same operation token through allocator statistics and reserve replay. These
tokens remain local to the worker operation; foreground mutations and independent
cooperative calls do not inherit a worker's shutdown request.
Canceled batches abort their private native transaction and discard its tail;
previously published partial roots remain durable and resumable. Cancellation
is not injected into the final durability boundary, whose existing uncertainty
fencing remains authoritative. Cooperative maintenance calls have independent
cancellation from the owner's worker shutdown token.
