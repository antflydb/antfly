# Lite cross-process commit sealing

Status: implemented (follow-up to #1865). `ANTFLY_LITE_PIPELINED_SEAL=0` turns pipelining off for diagnosis; groups then sync under the lease as before.

## 1. Why

A file with protected redo (`header.protected_redo`) makes a commit durable in
two barriers: a data fsync of the appended redo records, then a frontier slot
write and its fsync. Only the writer-lease holder writes frontiers, so every
cross-process writer holds the lease through both barriers. Eight processes
writing the same file therefore serialize sixteen fsyncs per round, and each
handoff waits for the previous holder's disk.

Same-process threads already pipeline: group G syncs while G+1 accumulates.
This design gives processes the same shape. The lease covers only appends.
Sealing, the barriers that make appends durable, runs under a separate seal
lock and covers every append published before it started, whichever process
made it.

## 2. Terms

- **Appended tail** `A`: the end of the redo records whose bytes are fully
  written, with the sequence of the last one. It lives in the shared region
  and is published by the lease holder after its record writes return.
- **Sealed frontier** `F`: the newest frontier slot on disk whose records
  were fsynced before it was written, and which was itself fsynced. Recovery
  and takeover replay exactly to `F.end_offset`.
- **Run**: the log's salt, its `reset_generation` and its active segment. An
  `A` or `F` from another run says nothing about the current log.
- **Seal lock**: the region's frontier-barrier lock, an in-process mutex
  plus an fcntl byte lock that the kernel releases when its holder dies.
- **Sealer**: any process holding the seal lock to advance `F` to `A`.
- **Poison**: a region word saying a sealer's barrier failed. It names the
  last sealed sequence.

## 3. Invariants

1. A commit is acknowledged only once a sealed frontier of its run covers
   its sequence. The ack never depends on the appender's own barrier.
2. Only a seal lock holder writes a frontier slot. It covers only records
   that were published in `A` before the sealer read it, and its data fsync
   comes after that read. So a frontier never covers bytes that were still
   being written.
3. Appends happen only under the writer lease, at `A.end_offset`. `A`
   advances only after the record's writes return.
4. Takeover, recovery and dead-writer repair never discard records in
   `[F.end_offset, A.end_offset)` that verify. A record that fails
   verification is unacknowledged by construction (invariant 1), so it can be
   cut, and the cut is published before anyone appends after it.
5. A fold, checkpoint, log reset or segment rotation runs under the lease
   and the seal lock, after sealing through `A`. No sealer can write a
   frontier for a run being erased.
6. A failed data or frontier barrier poisons the file for every process:
   - No new appends and no new frontiers.
   - The acknowledged prefix is the last sealed frontier.
   - Every waiter above it gets `OutcomeUnknown`.
   - Only a recovery that starts from disk with no live lease or seal holder
     clears the poison.
7. Sealing is always runnable by a waiter. No commit waits for another
   writer to arrive.

## 4. Shared state

These are new region fields, so the region format version is bumped. All of
them are guarded by one seqlock.

| Field | Meaning |
|---|---|
| `appended_salt`, `appended_reset_generation`, `appended_segment` | the run `A` belongs to |
| `appended_sequence`, `appended_offset` | `A` |
| `sealed_sequence` | the highest sealed commit sequence; monotonic across runs |
| `cut_generation` | bumped by every cut of an unverifiable tail |
| `seal_epoch` (u32 futex word) | bumped and woken after every seal, poison or cut |
| `poison_sequence` | 0, or 1 + the last sealed sequence when a barrier failed |

`F` is the existing synced-frontier proof plus `log_durable_sequence` and
`log_durable_offset`. Today those are published only by the lease holder
under its writer claim. After this change the sealer publishes them under the
seal lock, as a separate sealed-frontier publication. Its readers do not
require the writer claim, because the seal lock, not the lease, serializes
it.

## 5. Protocol

### 5.1 Commit (lease holder)

1. Resync (5.5) if the tail is not this handle's own.
2. Append the group's records at `A.end_offset`, then publish `A` with the
   last sequence. Nothing is synced.
3. Mark the members appended at that sequence and release the store mutex.
   The lease stays with the lane, which may hand it off at once (5.4).
4. The group waits for a seal (5.3).

A single-process writer, a lifetime writer and a `no_sync` file keep today's
path. Only `externally_locked` protected-redo files pipeline.

### 5.2 Seal

Run by a waiter holding the seal lock:

1. If the file is poisoned, stop.
2. Read `A`.
3. If `A` is from another run, or `A.sequence` is at or below `F.sequence`,
   the file is already sealed: release the lock and return.
4. `syncRedoData()`.
5. Write the next frontier slot `{salt, A.sequence, A.end_offset, segment}`,
   then `syncRedoData()`.
6. Publish `F = A`, bump `seal_epoch` and wake the waiters.

A failure in step 4 or 5 sets the poison (with `F.sequence`), bumps
`seal_epoch` and wakes the waiters.

The sealer does not need the appender's memory. A frontier needs only the run
and `A`, and the bytes it covers carry their own record checksums, which
recovery verifies against `F.end_offset`.

### 5.3 Waiting and acknowledgement

Commit sequences are global: a fold or reset changes the run but never
reuses a sequence. Only a cut does (5.5), and every cut bumps
`cut_generation`. So a waiter records its sequence `s` and the
`cut_generation` `c` it appended under, then loops:

- If `cut_generation != c`, it gets `OutcomeUnknown`.
  - A cut may have removed its record, and a later commit may reuse its
    sequence.
  - Cuts only remove records that fail verification, which a live
    appender's records do not. This conservative answer is therefore
    practically unreachable for a live waiter.
- If poison is set, it is acknowledged when `s` is at or below the poisoned
  prefix, and otherwise gets `OutcomeUnknown`.
- If `sealed_sequence >= s`, it is acknowledged.
  - `sealed_sequence` is monotonic across runs, because a fold or reset seals
    through `A` before the run changes (invariant 5).
- Otherwise it tries the seal lock:
  - acquired: seal (5.2);
  - busy: park on `seal_epoch` with a bounded timeout, then retry.

The first waiter to get the lock seals for everyone published so far. Later
waiters find themselves covered, or seal the next batch, which is
cross-process group commit. When nobody else arrives, the waiter seals its
own commit: the idle path.

### 5.4 Lease release

A lease release no longer needs durability. It needs every drained member's
records appended and `A` published, which `ensureDurableForReuse` (renamed to
reflect it) guarantees by draining the queue. The `store.sync()` at the
release sites goes, and the lane can hand off right after a group appends
when a foreign writer waits.

### 5.5 Takeover

A new holder's resync:

1. Read `F` from disk and `A` from the region.
2. If `A` is in `F`'s run and beyond it, replay `[F.end_offset,
   A.end_offset)`:
   - If every record verifies and the replay ends exactly at `A`, adopt it:
     apply the records to the shared overlay and continue appending at `A`.
     The records stay unacknowledged until a seal; this holder does not seal
     them first.
   - If verification stops early, take the seal lock and re-read `A` (a
     concurrent seal may have covered it). Then cut the log at the last
     valid record: zero the next header and sync. Publish the shortened `A`,
     then bump `cut_generation` and `seal_epoch` so that waiters from before
     the cut get `OutcomeUnknown`.
3. If there is no `A` (a fresh region), behave as today.

This settles the question the review raised. The next holder never has to
tell a live handoff from a crashed writer's tail. It treats every published
tail the same way: adopt what verifies, cut what does not. Both cases are
unacknowledged, so either is correct.

### 5.6 Dead writer

`recoverCrashedWriterRedoState` does the same verify-or-cut over
`[F.end_offset, A.end_offset)`. It then seals what verified, under the seal
lock, so a crash with waiters in other processes still resolves them. A
sealer that dies holding the seal lock loses only the lock. The kernel drops
the fcntl lock, a torn frontier copy fails its CRC, and the next sealer
starts again from the newest valid slot.

A crash during takeover leaves `A` and the log as the dead holder found
them, plus any of its own appends behind a later `A`. The next holder
applies 5.5 again.

### 5.7 Folds, rotation and reset

Before it folds, erases a prefix, resets the log or rotates a segment, the
lease holder takes the seal lock, seals through `A`, and keeps the lock until
the new run is published. Waiters see their sequences sealed before the run
changes. Sealers that wake afterwards find `A` in the new run already sealed.

### 5.8 Failure

- **Poison.** Poison is in the region, so every process sees it. A process
  that observes it marks its own handle `redo_frontier_uncertain` (today's
  sticky state) and refuses appends.
- **Clearing it.** A cold open with no live holder recovers from disk to the
  last valid frontier and clears it.
- **Acknowledgements.** Every acknowledged commit is covered by a frontier
  that was synced before the ack, so none is lost.

## 6. Tests

Cross-process fork tests (the `exerciseForeignRedoSuffix` and `xprocRunTest`
patterns), with the child killed at each boundary:

1. after its records are written, before `A` is published;
2. after `A`, before the lease is released;
3. after the lease is released, with no other writer: the parent waiter
   seals (the idle queue);
4. while sealing, after the data fsync and before the frontier write;
5. after the frontier write, before its fsync and the `F` publication;
6. during takeover: after adopting `A`, before its own append; and after
   appending behind an adopted tail, before sealing;
7. a torn tail behind a published `A` (bytes zeroed) is cut, and the waiter
   above the cut gets `OutcomeUnknown`;
8. a barrier failure in one process (`test_fail_redo_sync` or
   `test_fail_redo_frontier_sync`) poisons every process, preserves the
   acknowledged prefix, and refuses later appends until a cold reopen;
9. a fold or rotation with an adopted tail seals it first;
10. two writer processes interleaving: every acknowledged row survives
    SIGKILL of either, and the fsync count per commit falls below two.

Every test checks invariant 1 the same way. After recovery, every
acknowledged row is present. Unacknowledged rows may or may not be present,
but no half-applied group appears.

## 7. Where it lives

- Region: `SealBlock` in `shared_lock_region.zig` (format v7), with
  `publishAppendedTail`, `sealState`, `noteSealed`, `noteCut`, `poisonSeal`,
  `waitSealEpoch` and `tryLockFrontierBarrier`. A cold open with no live
  peer clears poison and the appended tail.
- Native file (`native.zig`):
  - `enablePipelinedSeal`;
  - pipelined appends through the ride-along branch, plus
    `publishAppendedTail`;
  - `sealAppendedTail` and `sealThrough`, the seal and the wait;
  - `settleSealed`;
  - adopt-or-cut in `resyncRedoTailUnderAuthority`, with `cutAppendedTail`;
  - `sealOrCutDeadWriterTail` in dead-writer recovery;
  - the fold fence in `checkpointRedoLogWithDurabilityImpl`;
  - the seal lock in `writeAndSyncRedoFrontier`;
  - `publishSealedLogState`, which advances the watermark readers use.
- Docstore (`docstore.zig`):
  - `applyMutationGroup` pipelines a lease holder's group and returns its
    `SealContext`.
  - `driveMutationCoordinator` seals it outside the store lock and
    completes the members only afterwards.
  - `prepareLeaseHandoff` waits for appends, not seals.
- Lane (`capi/lite_connection.zig`): every release site calls
  `prepareLeaseHandoff` instead of draining and syncing.
- Tests: the `lite commit sealing:` tests in `native.zig`, and the
  region seal-block test.
