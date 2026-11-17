# Lite writer authority

One writer per inode per process. Every Lite write, foreground or background,
is applied by that writer, under the one cross-process lease that writer holds.

Status: in progress on `perf/lwa-integration` (the single-commit write path
PR). Supersedes the per-path lease rules described in
[`lite-single-commit-write-path.md`](lite-single-commit-write-path.md) §3.2
and §8.5, which stay as history.

## 1. Why

The write path has grown several independent writers and lease holders inside
one process:

- writable `NativeFile` instances on the same inode: the Owner's foreground
  root, the maintenance snapshot (a full detached DB handle, or a
  storage-only handle), and a refresh replacement while the previous root
  retires;
- places the cross-process writer lease can live: parked in `WriterLane`,
  retained in the connection gate for a waiter, a maintenance turn's capture
  lease, and `Handle.maintenance_writer_lease` behind
  `enterMaintenanceWriter`;
- threads that write on their own schedule: the ride-along flush loop and the
  idle-turn redo fold, plus a group leader that drives its commit after
  `releaseWriterLaneBeforeWait` has already checked the lane in.

Each combination needed its own rule, and the bugs found while qualifying the
PR are mostly missing rules: publishes from a second instance tripping the
`active_writer` backstop, stale writer bases reaching `publishCheckpoint`,
background folds running with no lease, a lease handed to another process
before its holder's staged commit ran, one lease checked out twice, and
deadlocks between the gate, the lane and the maintenance writer. Several of
these also explain cross-process `InvalidNativeValueChain` and
`InvalidNativeAllocator`: two `NativeFile`s writing one file means one of them
allocates from a stale free map.

## 2. What SQLite does, and what we keep from it

SQLite (WAL) has one writer at a time, serialized by one lock in the
shared-memory WAL index; the calling thread does all the work, including the
checkpoint once the WAL passes its threshold; readers pin a WAL index mark.
There are no background threads and no in-process writer state.

We keep exactly that shape, with two additions SQLite lacks:

1. Group commit. Writers in one process share a commit queue, so a group pays
   one fsync. This is safe only because there is still exactly one writer.
2. Background derived work (indexes, enrichment, resolution, retirement).
   It computes off-lease on a read snapshot and submits its writes to the one
   writer, instead of committing through a second writable instance.

## 3. Invariants

- **W1. One writable instance.** Per inode per process there is at most one
  `NativeFile` that begins transactions: the Owner's current root store.
  Maintenance snapshots, retired roots and read generations are read-only.
- **W2. One lease holder.** The process's cross-process writer lease is held
  only by the writer authority (`WriterLane`). Every other path that needs to
  write checks the lane out; nothing else retains, parks or closes a lease.
- **W3. Every write enters the authority's queue.** All mutation plans, from
  foreground batches, maintenance publications, store maintenance
  (retirement, vacuum, allocator service) and redo folds, are applied by the
  current root store's commit coordinator while the lane is checked out to
  the submitter.
- **W4. No self-scheduled writes.** A store never starts a write from its own
  thread. Ride-along groups are driven by their waiter, by the next group
  leader, or by the authority before it releases the lease.
- **W5. Drain before release.** The authority gives the lease up, to another
  process or on idle, only after its store has no staged or draining commit.
- **W6. Readers never write.** Reads use `ReadView` pins (checkpoint, overlay
  mark) and the shared region's reader slots, shared per process and
  sequence.

W1 and W2 turn the `active_writer` backstop and `refreshIfWriterBaseStale`
rebasing into assertions that cannot fire in-process; they stay as
cross-process checks.

## 4. Design

### 4.1 Writer authority

`WriterLane` becomes the authority. Its lease is in one of three states:

| State | Lease | Meaning |
|---|---|---|
| idle | none | no writer in this process |
| checked out | held | one submitter is preparing and staging under it |
| parked | held | no submitter; staged groups may still be draining |

Transitions: `checkout` (idle → checked out acquires the cross-process lease
fairly; parked → checked out reuses it), `checkin` (checked out → parked),
`release` (parked → idle, after W5's drain). `release` runs when the parked
lease has idled past `parked_idle_release_ns`, or when another process has
waited past `foreign_fairness_quantum_ns`; a writer that checks in with a
foreign waiter overdue marks the release due and the next checkout or owner
turn performs it. There is no other way to acquire or drop the lease.

### 4.2 Maintenance publication: compute on a snapshot, apply on the writer

The maintenance snapshot keeps its detached DB runtime for computation, but
its store forwards writes: `Store.enqueueRequest` on a snapshot stages the
request on the owner's current root store instead of its own. Mutation plans
already apply through the `*NativeFile` they are handed, so the plan runs
against the one writable file and shares the foreground group's fsync.

The snapshot's admission hook changes from "acquire a second lease and rebase
this instance" (`enterMaintenanceWriter`) to "check the lane out and validate
that the inputs this publication read are unchanged at the writer's tip"
(the checks `enterMaintenanceWriter` already performs:
`validateStorageCurrent`, `requireCurrentCatalog`, `indexInputsUnchanged`).
A stale publication is refused as today (`EnrichmentWaitCanceled`) and
recomputed on the next turn.

Store maintenance that mutates the file directly (`maintainOnce`,
retirement service, vacuum, allocator service) runs on the current root
store under a lane checkout, not on a snapshot.

### 4.3 Folds

A writer folds inline when its own commit leaves the redo log under its
capacity threshold, as today. An idle log is folded by the owner's
maintenance turn as a lane-checked-out job. The store's idle-turn fold and
flush loop do not write on lease-managed stores (W4).

### 4.4 What this deletes

`Handle.maintenance_writer_lease` and the reference-counted
`enterMaintenanceWriter`/`leaveMaintenanceWriter` lease logic; the gate's
retained-lease handoff for writers; the maintenance capture lease; the
second writable open in `refreshMaintenanceSnapshot`; the store-level
background write admission.

## 5. Delivery

1. Snapshot stores forward commits to the current root store; snapshot files
   open read-only (W1, W3). Store maintenance moves to the current root.
2. All lease acquisition goes through lane checkout; remove the maintenance
   writer lease, the capture lease and gate retention (W2, W5).
3. Remove store self-scheduled writes on lease-managed stores (W4); folds
   via inline threshold and owner jobs.
4. Delete the dead paths and turn W1/W2 violations into assertions.
5. Plain reads take the lane's reader slot instead of the writer lease (W6).

### 5.1 As implemented

- Step 1: `Store.write_target` forwards a maintenance snapshot's
  `enqueueRequest`, `maintainOnce`, vacuum, redo fold and index sync to the
  owner's current root store; the snapshot adopts the writer's tip after
  each forwarded commit.
- Step 2: `WriterLane.borrowLease` is the one acquisition path outside
  `checkout`. It reuses a parked or gate-retained lease, or acquires through
  the fair queue, and lends it (`PathWriterLock.return_to`): closing a lent
  lease parks it back on the lane with no cached root, so the next checkout
  refreshes. Long detached work (a maintenance turn's capture, the
  allocator proof walk) gives the lease up with `releaseBorrowed` instead.
  Once `Owner.deinit` starts, returned leases are released, not parked.
- Step 3: the owner folds an idle redo log before releasing an idle parked
  lease (`maintainParkedLease` -> `Store.foldIdleRedoLog`); the store's own
  idle turn stays refused on lease-managed stores.
- Step 4: the parked-lease lookups that bypassed the lane are deleted; the
  lane asserts it never parks a second lease or acquires one while parked.
- Step 5: `Connection.admitRead` takes the gate and `WriterLane.beginRead`;
  writers wait for the reader slot, and the read serves from `refresh`'s
  in-memory fast paths. A read that needs a replacement open, pending SQL
  recovery or capacity admission takes the writer path instead.
- W1: a maintenance snapshot's file is marked `forwarding_only`, and Debug
  builds assert it never begins a transaction. A probe run over the
  maintenance and concurrency suites and the Go embedded suite found no
  such write. The snapshot still opens its file writable for allocator
  bookkeeping, and `enterMaintenanceWriter` keeps its rebase as the
  cross-process check.

## 6. Tests

- W1: a test that opens every kind of handle (foreground, maintenance
  snapshot, refreshed replacement) and asserts only one instance per inode
  can begin a transaction.
- W2/W5: lane state-machine tests for every transition, including a foreign
  waiter arriving while a group drains, and release only after drain.
- W3: a maintenance publication and a foreground write committing in the
  same group (one fsync), and a stale publication refused.
- Existing suites: WS3 writer lane, issue1015, maintenance, no-consumer,
  redo log, and the 8-process `scripts/bench/lite_sqlite_comparison.py`
  run with no errors.
