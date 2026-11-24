# Lite write throughput

Goal: indexed writes from 8 concurrent writers (threads or processes) within
range of SQLite FTS5 on the same machine. Measured with `lite-mp-bench`
(`zig build lite-mp-bench`) at `full_index`, 8 writers, one document per
write, against `scripts/bench/lite_sqlite_comparison.py`'s FTS5 baseline.
Per-stage times come from `ANTFLY_LITE_WRITE_PROFILE=1`
(`storage/write_stage_profile.zig`).

## Where a write's time goes

Every write takes the writer lease for one turn, in SQLite as here, so
throughput is the inverse of the serialized turn.

| | plain | full-text | index adds |
|---|---|---|---|
| SQLite FTS5 | 0.093 ms (10.8k/s) | 0.139 ms (7.2k/s) | ~0.05 ms |
| Lite, 8 threads | 0.15 ms (6.8k/s) | 0.70 ms (1.4k/s) | ~0.55 ms |
| Lite, 8 processes | | 0.93 ms (1.1k/s) | |

8-thread full-text turn (ReleaseFast, `PR #1797` head), per write:

| stage | µs |
|---|---|
| `wait_sync_level`: full_index catch-up after the commit | 185 |
| `ticket_wait`: group apply and durability | 135 |
| redo data fsync + frontier fsync (×1.4 commits per write) | ~100 |
| inline full-text prepare (tokenize, build level-0) | 53 |
| primary staging | 15 |
| inline install | 11 |
| merges (another thread, ~0.7 ms each, one per ~8 writes) | ~100 |

Plain writes are fast because they release the lease before the durability
wait: about four share each group commit (16.5k groups for 68k writes).
Indexed writes hold it across the wait and get no group commit at all.

## Changes

Ordered by payoff over risk. Each lands as its own commit with tests and a
ReleaseFast measurement.

### 1. full_index fast path and merging at the cap (done)

A write whose indexes all published inline still caught every derived worker
up to its sequence, chased writes committed meanwhile, and ran an
artifact-repair pass. `ManagedSyncTargets.served_inline` now marks a batch
whose inline publications filtered its targets; with none left, no group
sources and no enrichment wait owed, the wait skips catch-up.

That alone changed nothing in ReleaseFast: most writes were not served
inline. The index sat at `max_pending_segments`, where a write takes the
replay path so its admission can assist a merge. A writer now merges an
index at the cap on its own thread before it takes the apply lock
(`TextMergeRuntime.mergeAtCapBeforeWrite`), under the lease it holds, so its
write stays inline. Debug: the full_index wait went from 400 to 1.4 µs per
write and merges from 157 to 56 per 2,000 writes.

### 2. One fsync per commit

Today: append records, fsync, write the next frontier slot, fsync. Recovery
replays only to the newest frontier, so the second fsync is what makes an
acknowledged commit recoverable.

New: append records and write the next frontier slot, then fsync once. A
crash inside that fsync can leave the new slot durable and some of its
records not. Recovery accepts the newest slot only when every record between
the older slot's end and the newest slot's end decodes, checksums, carries
the newest slot's generation, and runs exactly to its sequence; otherwise it
uses the older slot, which the previous fsync made durable. The commit
acknowledged only after its fsync returned, so nothing acknowledged is lost.
A commit whose previous slot is in another log generation (rotation, reset)
keeps the two-fsync order, so verification only ever spans one contiguous
range.

Cost: the disk frontier is read in about nine places (cold recovery, reader
seals, writer tail resync, folds, compaction, checks), and each must stop
treating an unverified newest slot as durable. It saves about 35 µs of a
commit; group commit (3) would divide every per-commit cost instead, so it
waits for 3.

Readers must not treat the slot as a visibility signal before the fsync: the
shared region's durable watermark, published after the fsync, stays the live
signal; the slot is read only by cold opens, which run the verification.

### 3. Lease release before the durability wait for indexed writes (blocked)

The lease is held across the wait because the full-text install runs after
it on the writer's runtime, and the group leader stages commit participants
(full-text `stage`) inside `Ticket.wait`.

Tried: the leader installs each member's text publication once the group is
durable (a thread-local after-commit list opened by
`driveMutationCoordinator`, entries dropped for members whose apply failed),
and the writer releases the lease before waiting when every index served it
inline. The leader install alone passes. With the release, an 8-thread run
retired a root while a write's derived backlog admission was still reserved
(fixed by not committing the admission for inline-served writes), and then
still failed: 2 of 5 thread runs returned one stale search result, dense hit
`InvalidNativeAllocator`, and throughput fell (Debug 247-334 vs 463 writes/s).
Something else on the post-wait path still depends on holding the lease;
finding it comes before another attempt.

### 4. Cheap level-0 records

Today a level-0 record (`l0/<first version>`) is a complete segment: built by
the full segment writer (term dictionary, postings, doc values, versions,
sections), then installed as a new `SegmentReader`, and later merged with
the others.

New: a level-0 record is postings only: per field, sorted terms with
`(doc, version, frequency, positions)`, plus the copies' keys and versions.
An index keeps its level-0 records in one in-memory level-0 set; queries read
it beside the segments; the merge scheduler folds the set into a segment
when it reaches a size or count bound, in one bounded step. Install appends
to the set instead of publishing a new segment.

### 5. Bounded merges outside the lease

A merge builds its output without the writer lease and commits it in one
small transaction whose resolver checks its inputs at the tip (merges already
resolve this way). Merges run in bounded units, like FTS5's automerge, so no
writer waits behind a long merge, and the lease-lending path
(`text_merge_runtime.mergeMayUseLeaseOf`) becomes the exception.

### 6. Incremental cross-process refresh for writers

A write after another process's commit reopens its runtime. Refreshing it in
place works but costs more than it saves while a refresh reloads whole index
mirrors (sparse halved, see `lite-index-write-path.md` stage 4). With level-0
records read from the file and doc map, dead set and level-0 changes applied
record by record, a refresh costs what changed, and writes can use it.

## Status

| change | state |
|---|---|
| 1. full_index fast path, merging at the cap | committed |
| 2. one fsync per commit | designed; after 3 |
| 3. lease release for indexed writes | tried, stale results; blocked |
| 4. cheap level-0 records | planned |
| 5. bounded merges outside the lease | planned |
| 6. incremental writer refresh | after 4 |
