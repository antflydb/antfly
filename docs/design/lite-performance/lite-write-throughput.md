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

### 2. One fsync per commit (done)

Today: append records, fsync, write the next frontier slot, fsync. Recovery
replays only to the newest frontier, so the second fsync is what makes an
acknowledged commit recoverable.

Protected redo syncs the group's WAL records, writes the next sealed
frontier, then syncs that frontier before acknowledging the group. All
writers in the group share those barriers. Live readers use the shared
region's durable watermark, published after both barriers. Cold recovery
uses the sealed frontier and reports corruption if the WAL cannot reach it.

PR #1797 tried writing the record and frontier before a single barrier.
That protocol could persist a frontier whose WAL range was lost during the
barrier. The same on-disk state could result from damage after acknowledgment.
The writer now emits only sealed frontiers. New protected files and vacuum
replacement generations carry a sealed-redo format bit. Old binaries reject
those generations when decoding their headers. An existing file can continue
in its original format until a vacuum publishes a fenced generation. Legacy
combined slots remain readable where that bit is absent, including during
the vacuum's source read. Publishing a private imported generation first
syncs the verified WAL cut, then writes sealed frontier copies and the
format bit before renaming the image. A new sealed commit establishes a
stronger acknowledgment boundary even before the vacuum.

### 3. Indexed writes overlap their durability wait (done)

#1793 made graph, sparse and dense writes yield the writer lane during the
wait and complete through the group leader (`commit_participant.Queued`).
It stalled under runtime replacement: a maintenance turn's forwarded writes
mark the serving runtime stale, the next lane holder replaced it, and a
resuming write failed with `EnrichmentSourceChanged` (or, tolerated, drove
its group after another file took the lease). Fixed by:

- draining the outgoing store before the replacement opens, so the
  replacement loads every queued group from the file;
- letting a write with nothing left on its runtime return without taking
  the lane back (taking it back cost thread mode a second lane crossing);
- full-text joining the queued participants: the leader installs the text
  publication once durable, only try-locking the index guard (the leader may
  hold it further up its own stack), and the writer releases the guard as
  soon as the publication is prepared;
- a level-0 soft cap where writers merge (`max_pending_segments`) and a hard
  cap twice that for inline writes, so a write at the cap whose merge is
  already running elsewhere stays inline instead of paying the replay path.

### 4a. Level-0 encoding (measured, small)

Sampled in Debug (8 threads, full text, `sample(1)`): building a write's
level-0 segment is about 7% of the write's commit path; the group commit's
ticket wait is 47%. The `text_segment_build` span had looked far larger only
because enabling the build profile also took six resident-memory samples per
segment; the profile now leaves those off and splits the span into analysis,
typed values, inverted sections and assembly.

Within the build, 65% of assembly was `combineSectionCrc` shifting a zero
checksum through GF(2) matrices: every section of a small segment is a
single first block. The shift is linear, so a zero left checksum returns the
right one unchanged, which is exact. A separate small-segment encoder is not
worth its second format path at this size.

### 4b. A segment interface for queries

`SegmentEntry.reader` is a `SegmentView` (`segment_view.zig`): the
operations queries, deletions and merge planning use, with the encoded
`SegmentReader` as its only implementation. Code that works on encoded bytes
(merges, cursors, native navigation, integrity checks) asks for `encoded()`.
A level-0 buffer reader (4) becomes a second variant. No behavior change.

### 4. Cheap level-0 publication (replaces the memtable)

A CPU sample of the ReleaseFast 8-thread run put most of the install time in
`nanosleep`: both install steps opened a read transaction to read back
values their own commit had decided (the mirror's dead buckets, and the doc
map versions behind the deletion bitmap), and a Lite read transaction waited
for the store's publication mutex behind the next group's apply.

The read-backs existed because installs run out of commit order (the leader
installs only if the index guard is free; otherwise the writer installs after
its wait). Instead:

- Every resolver run takes the next epoch of its index's mirror inside its
  commit (`Backend.noteResolved`), in commit order. An install applies its
  decided values only for keys whose latest epoch is still its own
  (`Backend.applyCommittedResolution`); merges' `mirrorResolved` does the
  same. A run whose commit did not happen restores the keys' earlier epochs
  (`revertResolved`); a peer delta's keys take a new epoch
  (`noteExternallyDecided`), so a pending local install keeps them.
- A level-0 copy is deleted at install when it was dead at its own commit
  (`VersionResolution.deadAtCommit`) or a later commit's install killed it
  first: that death waits in `VersionLocations.parked` while level-0
  publications are pending.
- The segment is parsed once at prepare and handed to the snapshot.

A queryable in-memory buffer (the memtable this section used to describe)
would need an abstract postings interface across the query layer, which
reads postings through concrete encoded types. FTS5 also writes a segment
per commit; what made ours expensive was publishing it.

Peer deltas no longer refuse while inline installs are pending, so a writer
that refreshes past another process's commit stays incremental.

On the shared write path, a Lite read whose pin takes no reader fence (the
generation already holds it, or a nested read under the writer's lease) is
admitted under `read_mutex` alone instead of the publication mutex, and a
write without graph indexes skips its endpoint-cleanup read.

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

## Measured after 2, 3 and 4a

ReleaseFast, 8 writers, 10 s runs interleaved with the previous head
(`c9dcb994d8`) on the same machine:

| | before | after |
|---|---|---|
| full text, 8 threads | 1,398–1,459/s | 1,665–1,776/s |
| full text, 8 processes | 803–1,061/s | 1,120–2,359/s |
| dense, 8 threads | 3,458–3,541/s | 3,461–3,521/s |
| dense, 8 processes | 785–1,088/s | 717–1,085/s |
| plain, 8 threads | 7,383–7,705/s | 7,287–7,372/s |
| plain, 8 processes | 7,269–7,521/s | 7,622–7,759/s |

Full-text profile after (8 threads, per write): groups average three
writes; the redo fsync is 15 µs and the frontier fsync runs for 2% of
groups; preparing the level-0 segment is 55 µs, 21 of it encoding.
Installing it is now the largest serialized cost: 99 µs syncing the catalog
mirror and 93 µs publishing the segment. That is what the memtable (4)
removes.

## Measured after 4

ReleaseFast, 8 writers, 10 s runs interleaved with main (`81e1d96af5`) at
load average 9-11:

| | main | after |
|---|---|---|
| full text, 8 threads | 1,049–1,309/s | 1,486–1,870/s |
| plain, 8 threads | 4,859–6,746/s | 8,527–10,777/s |
| full text, 8 processes | 5 of 10 runs stalled | 0 of 10 stalled |
| plain, 8 processes | 3,760–4,589/s | 2,974–5,889/s (noise) |

Per full-text write (8 threads): installing the level-0 segment went from
99 + 93 µs to 3 + 5 µs. The 8-process stalls were
`TextMergeBackpressureTimeout`: a merge refused at the tip
(`IndexScopeChanged`) was planned again from the same stale view while a
producer waited for it; it now refreshes the index from the tip first.

Next, from the same profile: groups now average about one write (each pays
both redo barriers), full_index catch-up runs for writes not served inline,
and merges run about once per six writes.

## Measured after the review fixes and lane work

ReleaseFast, 8 writers, 10 s full-text runs, each change interleaved with
the build before it:

| | 8 threads | 8 processes |
|---|---|---|
| after 4 | 1,486–1,870/s | 0 of 10 stalled |
| merges ahead of the cap, small segments folded | 3,511–3,903/s | 2,445–2,838/s |
| maintenance ride-alongs drive their group | 4,804–5,516/s | 2,487–2,790/s |
| store mutex parks on a futex | 6,027–6,247/s | 2,393–2,830/s |
| unverified ledger rebases | 5,662–6,271/s | 2,533–2,568/s |

Merges kept the inline producer under its cap: level-0 records fold at a
quarter of it, pressure starts at three quarters, and a pressured level-0
merge also takes every segment below the floor size. Before that, tiered
tiny segments filled the cap and about 15% of writes fell to replay.

The writer lane was then 92% occupied. Maintenance turns held it about
10 ms each: a maintenance snapshot forwards its writes to the foreground
store, where its ride-along bookkeeping commits waited for a foreground
leader that could not run while the turn held the lane. Forwarded
ride-alongs now drive their own group, and turns hold the lane about 1 ms.
Inside a writer's 167 µs hold, a third was the store mutex sleeping in
100 µs steps behind a group apply; waiters now park on a futex.

8 processes are bounded by the cross-process lease, which a process hands
off only once its writes are durable. Summed over processes, holders keep
it 78% of wall time: writes under it (180 µs each, 80 of them the fsync
wait) take 49%, serving-runtime refreshes 18%, and the rest is handoff.
Most refreshes reopen the runtime, because the 512 KiB redo log folds about
every 180 full-text writes and a fold changes the physical roots. A 4 MiB
log folded 8 times less often and did not change throughput, so the
handoff rule, not the reopen, is the limit.

## Status

| change | state |
|---|---|
| 1. full_index fast path, merging at the cap | committed |
| 2. protected redo barriers | two barriers per group |
| 3. indexed writes overlap their wait (group commit) | committed |
| 4a. level-0 encoding: CRC shortcut, profile split | committed |
| 4b. `SegmentView` interface | committed |
| 4. cheap level-0 publication, read admission | committed |
| 5. bounded merges outside the lease | designed |
| 6. incremental writer refresh | #1830; peer deltas no longer wait for inline installs |
