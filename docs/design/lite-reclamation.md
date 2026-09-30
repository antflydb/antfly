# Native Lite reclamation

Issue: [#939](https://github.com/antflydb/antfly/issues/939).

## Decision and implementation boundary

Native Lite owns reclamation, maintenance scheduling, publication, and reader
retention accounting. Applications supply budgets; they do not close/reopen the
database or start a second writer to maintain it.

The current implementation supplies an automatic owner-managed generation rewrite
for existing revision-3 files. It does **not** implement the revision-4 allocator
described below. In revision 3, rewriting provides both reclamation and shrinking;
disabling automatic rewriting therefore disables automatic reclamation too.

The permanent architecture is indexed copy-on-write storage with incremental
retirement, generation-aware reuse, and a persistent extent allocator. A full
rewrite remains useful for migration, repacking sparse record bundles, and returning
capacity to the filesystem. It must not be required to bound ordinary overwrite
history once the new allocator is qualified.

No implementation can be called maximally efficient without measured workload and
latency constraints. The design minimizes asymptotic foreground work and makes the
remaining tradeoffs measurable.

## Revision-3 owner policy

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

Default assessment occurs after 64 MiB of file growth once the file reaches
256 MiB. An assessment walks live ordered-index metadata, not historical chains
or external value payloads. A rewrite is eligible only when both physical size
exceeds twice estimated compact size and estimated savings reach 256 MiB. These
are provisional, configurable defaults; sustained benchmark qualification must
precede a release-level numerical performance promise.

The task is lazy: small files do not consume a concurrency lane. Mutations signal
one owner task and requests coalesce. Busy publication and resource deferral retry
after the configured interval. Read-only handles perform no maintenance. Writable
reopen assesses outstanding debt synchronously, preventing repeated short CLI
sessions from discarding a newly launched background task forever. Close requests
cancellation, wakes and joins the task, then closes the storage runtime. The
publication boundary retains the existing atomic adoption and directory-sync
semantics; shutdown does not asynchronously interrupt a header/rename operation.

If the runtime cannot provide concurrency, the status reports it. Reopen and the
cooperative owner operation can still service debt; the implementation does not
promise an uninterrupted long-running workload will plateau in that configuration.

The policy keeps estimates and their checkpoint sequence separate from current
size. Concurrent writes can make estimates stale. Publication invalidates estimate
freshness. An explicit integrity audit remains independent of routine assessment.

The allocator rejects appended pages before they exceed an optional aggregate
storage budget. Admission subtracts rewrite workspace and retained generations.
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

## Why a freelist alone cannot fix revision 3

Current ordered document and catalog indexes accelerate lookup, but global and
namespace record chains remain authoritative for other operations and integrity
coverage. Superseded records remain reachable. A periodic mark/sweep can release
superseded index paths, but it cannot release all old records while these chains
are retained. The current one-page free map also cannot represent arbitrary free
capacity. Both issues need explicit structural changes.

Never reinterpret old chain pointers as non-owning in place: an older reader or
binary can still traverse them. Upgrade by building an explicitly identified new
format and atomically adopting it through the database owner.

## Revision-4 durable structures

1. **Authoritative ordered indexes.** Document, metadata, and derived-artifact
   lookup, scans, backup, vacuum, and integrity use their ordered indexes. Deletes
   remove entries. Global history and namespace-head chains no longer retain old
   records. Namespace listing is a separate catalog; document ranges use the
   document index prefix bounds.
2. **Independent key ownership.** Long separator keys must not borrow entire
   obsolete document/value records. Store immutable key objects independently,
   with explicit ownership when shared by leaves and separators. Inline short
   keys remain inline. This prevents an updated document's old payload from
   surviving solely because its key remains an internal separator.
3. **Packed-record ownership.** Retirement applies to slots within a bundle;
   reuse applies to physical pages. A page becomes reusable only when no live
   root, retained snapshot, recovery slot, or key object owns any slot. Partially
   live bundles become candidates for bounded repacking, not freelist pages.
4. **Persistent free extents.** Replace the one-page free vector with a
   copy-on-write extent tree, coalescing adjacent free runs and splitting on
   allocation. Keep a bounded in-memory allocation cache. Consume only changed
   paths; never serialize the whole free set on every small commit. Prefer
   contiguous runs for streamed values and retain existing extent-tree locality.
5. **Persistent retirement queue.** Publish retired pages/slots/subtrees tagged
   with their last visible checkpoint sequence in the same transaction as the
   replacement roots. Drain incrementally under byte/time/page budgets. Large
   deleted values enqueue subtree work rather than synchronously traversing all
   payload pages on a foreground delete.
6. **Root identity and sharing.** Appended values share immutable extent subtrees;
   renames can transfer ownership. Retirement must describe removed ownership,
   not assume every page beneath an old root is dead. Persistent ownership
   accounting or a validated old/new structural diff must cover extent subtrees,
   independent keys and packed slots before publishing free capacity.

The sharing representation is an implementation gate, not an unspecified detail:
choose and benchmark explicit ownership counts against structural diff retirement
using real append/rename/delete workloads before accepting format-4 encoding.
Changing opaque internal external-value references must preserve or explicitly
restrict shared ownership; a format writer must never infer uniqueness.

## Visibility, crash safety, and reader generations

The safe reuse frontier is constrained by **all** recoverable checkpoint slots,
active snapshots, and retained durable roots behind unsynced commits. Retiring a
page after replacing the active root is insufficient while the fallback recovery
slot still owns it. Free-space and retirement metadata belong to checkpoint roots
and follow exactly the same durability barriers as data roots.

For each commit:

1. Allocate private pages only from durable, validated free extents or the tail.
2. Build changed data/index/allocator paths and retirement entries privately.
3. Sync new data and metadata according to the existing durability contract.
4. Publish the next complete checksummed checkpoint; then publish its selection.
5. Advance retirement only past the oldest retained recovery/read frontier.

An aborted transaction restores allocator roots. A crash before publication may
leak unreachable tail pages, but cannot make a referenced page free. A crash after
publication recovers the complete new allocator/data roots together. Opening an
uncertain owner fences mutations until recovery; it cannot continue from an
unverified in-memory frontier. Deferred-sync writes must preserve the last durable
allocator roots as well as data roots.

In-process readers register checkpoint/epoch pins. Reuse need not wait for all
readers to disappear: pages older than every relevant pin can be reused while
newer readers continue. External-process readers require a cross-process pin
protocol, or the documented conservative shared-file-lock fallback. Avoid claiming
per-reader reclamation with the existing whole-file shared lock. Shared page caches
must invalidate reused page IDs, including decoded index views and cached links;
epoch/generation identity belongs to any cache shared between descriptors.

Generation replacement retires the old inode and keeps its descriptors until the
last snapshot exits. Old inode bytes remain part of total storage pressure even
when `stat(database_path)` reports the new, smaller file.

## Separate shrinking policy

With routine reuse qualified, shrink defaults to a conservative optional policy
using free-capacity ratio, absolute savings, and hysteresis. Reuse remains enabled
when automatic shrink is disabled. First truncate already-free tail extents when
reader/recovery safety permits. Relocate or rewrite to shrink internal holes only
when expected savings justify I/O and temporary capacity. Do not move pages on
every commit merely to minimize pathname size. Request coalescing, resource
admission, cancellation and owner adoption remain shared with the initial policy.

## Growth envelope and qualification

For rewrite-based reclamation, if successful service starts within D, bounded
append rate is R and the largest transaction appends B bytes, current-file peak
is at most the measured trigger envelope plus R*D+B. Charge replacement workspace
and retained generations separately. This is conditional on successful service;
disabled/unavailable concurrency, retained generations, capacity pressure and
repeated busy capture are observable exceptions.

For incremental reuse, the target envelope is live pages plus index/key/allocator
overhead, packed-page slack, recovery generations, reader-retained pages and
bounded retirement debt. A bounded live key count alone is insufficient when
reader retention or allowed retirement debt is unbounded. Set high/low pressure
watermarks and enforce admission when debt cannot be drained within its budget.

Required qualification includes mixed-size overwrite/delete/reinsert workloads,
metadata and index churn, value append/rename/subtree deletion, packed records,
long separator keys, multiple snapshots, overlapping reader epochs, both recovery
slots, deferred-sync commits, repeated short sessions, cancellation and crashes
around every allocator/publication boundary. Reopen and validate data and indexes.

Report physical/live/retired/temporary bytes over time, bytes written, foreground
page work, throughput, latency percentiles, maintenance CPU/I/O/time, and peak
memory. Compare automatic rewrite, manual rewrite, maintenance disabled, and
incremental reuse with full vacuum disabled. A plateau test must span multiple
service cycles, not one successful vacuum. Set numerical latency/I/O gates from
the application's baseline; the RFC's size thresholds are not benchmark results.

Implementation sequence: qualify the revision-3 owner policy; finalize shared
ownership and format-4 encoding; migrate and validate authoritative indexes;
integrate retirement and extent allocation; qualify reuse with rewriting disabled;
then enable independent shrinking defaults. Existing files stay readable, and an
old binary must reject the new format rather than follow stale historical links.
