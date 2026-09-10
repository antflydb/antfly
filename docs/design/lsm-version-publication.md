# LSM version publication

## Read epochs

Read transactions pin mutable state, immutable memtables, and SST membership
under one backend lock. Ordinary reads no longer project that epoch into a
second full run array. Diagnostic/oracle projections remain available and
coalesce with a per-epoch `std.Io.Mutex` outside the backend lock.

Point snapshots, current/replay scans, and bound/namespace write cursors share
the immutable directory. Scans allocate one source per matching L0 SST and
lower level, with cursor-local descriptors/cache hints. A lower-level seek
descends the directory once instead of binary-searching repeated rank lookups.
Current and pinned point probes query
overlapping SST handles directly, without preparing a cold scan projection.
Their scratch is bounded by matching SSTs, with 16 inline candidates. Current
point probes do not freeze mutable state after resolving its keys under the
lock. Disk-only point leases keep their block/value owner, not an additional
whole-epoch pin; immutable in-memory values still pin their supplying generation.
Empty SST sets allocate no epoch.
Sparse batches materialize only the union of exact-key SST candidates, retaining
the existing sorted-by-run table-index and decoded-block reuse. They do not
project all SSTs between the first and last requested key.
Final release queues retirement under the backend mutex; detached reclamation
retains every tree's accounting handle until it reacquires that mutex.

## Incrementally maintained planning indexes

Each immutable directory owns persistent indexes for read precedence,
stable run IDs, augmented key-range ordering, and per-level
counts/bytes/tombstone-run counts.
Payload ownership and SST pins are shared across indexes. A publication stages
all allocations, then updates only changed paths. Level moves, split outputs,
and GC-intent changes use the same directory update path.

Ordinary compaction selects directly from this directory. Subtree maximum key
bounds prune overlap searches; stable payload handles own their metadata,
accounting lifetime, and SST pins independently of directory roots. Selection
materializes only the chosen inputs, with a 4,096-input / 16,384-node fast-path
budget. L0 pressure gathers an oldest-run window before closing dependencies,
and the oversized-job exception applies only to a minimum indivisible closure.
Installation resolves handles against the live root and checks for newly added
target/older-L0 overlaps, not just unchanged input identities.

An exceptional closure exceeding that budget resumes directly on a pinned
directory outside the mutex, with admitted scratch and cooperative cancellation.
Ordinary maintenance retains that job across calls: each slice visits/emits at
most 2,048 nodes/handles and checks a two-millisecond deadline between operations.
Two arena-backed AVL indexes provide identity deduplication and read ordering
without a resizing hash table or final sort. Result materialization also resumes
across slices. Explicit synchronous compaction drains the same continuation.
The small fast path and standalone oracle retain their existing array algorithm.
An unchanged pinned root is an O(1) acceptance certificate at selection return.
GC also uses direct overlap closures; a version/configuration/age-aware cache
keeps negative maintenance probes from rebuilding a global projection.
Scheduling uses root summaries for requested intent and timestamp extrema,
including unknown ages and wall-clock rollback, rather than scanning every run.
Anchor discovery skips tombstone-free subtrees in read order, and an epoch with
no tombstones does not reserve collection scratch at all.
The full positional/domain planner remains only as an oracle/diagnostic adapter.

Planning builds are single-flight and memory-admitted. Plans are revalidated
against live inputs before installation. GC intent is merged from those live
inputs, including requests made while ordinary compaction was building.

### Policy-bound continuation admission

Discovery identity includes the L0 target, source-level restriction, input-byte
limit, and oversized-job policy. There are at most two active closure slots:
ordinary background work and L0-only work. A foreground request never consumes
or replaces the background slot. Changing policy retires only that lane's old
job through the sliced cleanup queue; no slot is replaced while planning is
off-lock. Both slots participate in memory accounting and shutdown cleanup.

Background maintenance alternates between queued slots, including L0 jobs whose
request has returned. Synchronous compaction drains those same continuations.
This preserves background progress without indefinitely pinning an abandoned
foreground epoch. Both lanes retain the existing per-slice work/time bounds;
they do not introduce parallel builders or an unbounded per-request job queue.

Before acquiring an execution grant, admission checks the current caller's
source-level and byte restrictions using the input-byte total already computed
for scheduling. Retry-cache hits use the same check. Exceeding a byte target
requires both caller permission and a minimum-indivisible-closure certificate
from discovery; a queued plan is not itself permission to exceed a budget.
An explicit zero-byte foreground budget admits no work (the internal planner's
zero sentinel still means unlimited). Regression tests cover alternating lanes,
policy changes, in-flight ownership, final admission, synchronous/background
draining, and close with both slots occupied.

The ReleaseFast policy-isolation regression measured about 10.1 microseconds
per rejected foreground call (64 calls, a queued 5,001-run background closure).
It asserts zero background discovery progress and zero executed compactions
during those requests. This measures metadata admission, not SST I/O or an
end-to-end ingestion speedup. Reproduce from `zig/` with:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'compaction policy'
```

## Manifest journal

`manifest.bin` is now a 36-byte checksummed `ALSMSET1` descriptor containing the
checkpoint ID, first journal segment ID, and checkpoint sequence. Immutable
`manifest-ID.checkpoint` files start with `ALSMJNL1` and a full checkpoint frame.
`manifest-ID.journal` files have a checksummed `ALSMSEG1` identity/starting-sequence
header. Their edits contain stable-ID removals, added or updated run metadata,
obsolete-path removals/upserts, and the next run ID.
Journal paths are root-relative: staging-directory publication must not change
the identity used by later obsolete-path removals. Runtime file paths remain
absolute and are rebound when opening the store.

Each 24-byte frame header contains payload length, sequence, checkpoint flag,
and a CRC32 of the preceding header fields. A payload contains removal lists
and an embedded standalone manifest; a separate CRC32 protects the full payload.
The first frame has the descriptor's sequence and the checkpoint flag. Subsequent sequences
must be contiguous. Embedded manifests also retain their own checksum.

Directory differences skip shared subtrees, including after AVL rotations.
Unchanged SST metadata is neither copied into edit descriptors nor serialized.
Run-layout validation checks changed runs and their final neighbors against the
last validated durable root; cold or administrative publications validate the
whole layout. Removing a run cannot introduce an overlap in a valid predecessor.
Obsolete paths and deadlines live in a persistent ledger. Capture retains a
root; diffs visit changed branches; subtree deadline summaries prune future-only
branches. Run-ID lookup also avoids scanning all SSTs for each obsolete-file
membership check. Clean publication
does not allocate encoding buffers or write another record.

The writer reserves encoding/tree scratch before allocating it. Pre-WAL commit
tickets bound mutating writers; the `std.Io.Mutex` publication lane serializes
durable metadata while releasing the
backend mutex around append/fsync, segment handoff, and descriptor publication.
Waiters capture the newest root after admission, coalescing accumulated edits
without an artificial batching delay. Publication wait/coalescing counters and
off-lock phase time are exposed in write statistics.

An edit is appended and synced before the durable-directory pin moves forward.
WAL retirement requires proof that the published directory covers the current
SST root: an older fsync must never retire WAL for a newer concurrent flush.
Build single-flight ownership ends at SST installation, not after its later
manifest fsync. Reader-retirement cleanup cannot recursively enter the held
publication lane. Any failed
append or sync invalidates the append state; the next attempt atomically replaces
set with a full checkpoint. Publication debt remains dirty even if a flush
already installed its SSTs and retired its mutable generation.

After 256 edits or 8 MiB of suffix, maintenance is eligible to checkpoint (with
a smaller suffix budget near the 128 MiB reader limit). The writer first creates
and syncs a successor segment, then atomically publishes a checksummed
`manifest-ID.next` link from the old active segment. Only then may edits append
to the successor. Both the old descriptor and a future new descriptor reach
those edits throughout the handoff.

Checkpoint maintenance pins the durable directory, captures the obsolete-path
ledger, and streams a new checkpoint with 64 KiB encoding scratch outside the
backend mutex. Publication atomically switches the descriptor without replacing
the advancing suffix. A failed/replaced lineage cannot install a stale build.
Retired checkpoints/segments/links enter the durable obsolete ledger, and live
or ambiguously published files are protected from reclamation. Cleanup includes
checkpoint files from interrupted builders on linked segments. Native writable
reopen, behind an exclusive writer lease, also inventories unlinked manifest
files and recognized atomic-write siblings. Inventory never determines recovery
authority; read-only/custom-storage opens do not perform this deletion.

The trigger is not a hard promise to checkpoint every 256 edits: maintenance
must run. Replay is capped at 128 MiB and 64 linked segments. A publication that
would exceed the byte limit falls back to a fenced, pinned streaming checkpoint.
Cold and fenced replacements stream with 64 KiB scratch outside the backend
mutex. Host adapters without atomic streaming sinks additionally admit their
buffered object before allocating it. Administrative prospective split manifests
remain serialized until their ownership transfer commits.
The same fenced fallback permits recovery after failed builders exhaust the
linked-segment bound; the cap must not become a permanent inability to checkpoint.

Recovery replays the complete prefix, accepting only an incomplete final frame.
Complete malformed records, checksum failures, broken sequences, unknown
removals, and regressing run IDs are rejected. Writable reopen adopts the durable
set and truncates an incomplete active tail before appending. Reserved linked
segment IDs advance the recovered file-ID high-water mark even without a later
edit. Read-only opens do not mutate the journal. A changed descriptor is retried;
missing children of an unchanged descriptor are corruption, not an empty store.

Native backup exports a standalone manifest for the pinned live run set,
without obsolete history or a concurrently advancing journal. Existing restore
consumers therefore need no journal suffix or unexported obsolete SSTs.
Inventory tooling can also replay journals directly without opening a backend.

## Cost boundaries

The writer owner is now a persistent rank tree, separate from the immutable
reader directory. Replacing K inputs with M outputs touches changed paths; it
does not copy or sort N survivors or allocate an N-entry removal bitmap.
Revisions share an explicit physical-metadata owner, so a pinned prepared writer
version remains safe across level moves and unrelated publication. Flat arrays
remain explicit diagnostic/oracle or synchronous administrative projections.

Ordinary discovery, GC component discovery, density/age evaluation, bounded
progress selection, GC-intent preparation, and result emission use resumable
jobs. Dependency certification owns both epochs and retains its identity,
scratch-cleanup, and delta cursors across maintenance calls. Each call advances
one off-lock 2 ms / 2,048-credit slice. A persistent-root diff extends a completed certificate through concurrent
edits; genuinely newer L0 writes and disjoint changes do not restart its full
input scan. Changed selected inputs or new mandatory dependencies invalidate
it. Stable handles, not old ranks, address writer inputs at installation.
GC-intent candidates similarly rebase concurrent deltas and publish their
prepared writer/reader roots atomically.

Synchronous build/install callers drain the same validator but give up after
four rebase attempts, safely discarding unpublished output when they cannot
catch the live epoch. They do not wait for global write quiescence indefinitely.
An accepted maintenance certificate carries its publication generation, so an
unchanged generation avoids a second full identity/coverage pass before build.
Small rejected hotspot candidates release their bounded scratch without dropping
the writer fence that protects the borrowed directory. L0 overlap scoring reads
the maintained level aggregate; its cold fallback probes at most the scoring
limit plus one run instead of walking an overloaded L0.

Planner deadlines start after unlock-time reclamation has received its separate
quantum. This prevents a backlog of retired versions from repeatedly consuming
the entire planning budget before the first operation. Shutdown cancels jobs
before draining writer roots, including unpublished GC-intent candidates.

Metadata trees, planner arenas, and retired selection handles reclaim in bounded
slices. Obsolete-file probes and deletion visit at most 128 due candidates per
call, using bookmarks and pin-release epochs. Last-reader release makes an old
epoch eligible; maintenance completes physical reclamation without requiring a
new write. Cleanup itself does not initiate durability I/O or swallow sync errors.

Pre-WAL tickets reserve both commit backlog and future manifest wire capacity.
The estimate groups rows by their actual output partition and accounts for
logical, physical, and entry-count splitting limits; ordinary batches do not
reserve a descriptor per row. The ledger includes active tickets, unflushed WAL,
pending/held maintenance edits, a replacement checkpoint, concurrent journal
suffix, and lifecycle metadata. Compaction and GC reserve metadata before root
publication. Pressure drains/checkpoints before admission, or rejects before
WAL mutation when another publisher/checkpoint prevents relief. Checkpoint
handoff independently verifies its base plus concurrent suffix. Synchronous
administrative publications retain their checked, staged manifest boundary.

If direct bulk ingest appends WAL but fails before completing SST publication,
the backend retains its obligation and fences subsequent writes/publication/WAL
retirement with `RecoveryRequired`. Reopen replays the preserved WAL and removes
the fence. Empty memtables alone must not erase an unmaterialized WAL obligation.

These are cooperative metadata-work bounds, not hard real-time latency promises.
SST building/I/O, explicit administrative rewrites, shutdown, and the K+M changed
records in compaction installation still cost proportional work. Shared physical
owners and persistent indexes trade metadata memory for narrower publication;
device latency, concurrent churn rate, and retained reader epochs still matter.

Native clean sync behind the exclusive writer lock no longer replays the full
manifest just to detect another writer. Production set recovery now uses a
64 KiB range-read buffer, applies records into an unpublished live-metadata map,
checks nested checksums/sequences, and discards the candidate on any corruption.
Removed/replaced records are freed during replay, not retained as journal bytes.
Reopening a writer trims a torn active tail using the same bounded-buffer shape.
The descriptor is rechecked before mounting; a changed descriptor retries rather
than exposing partial metadata. The buffered codec remains an export/oracle path.

## Validation

Tests cover old epoch preparation after publication, shared scan topology,
admission rejection, allocation-failure cleanup, persistent ordering against a
rebuild oracle, level moves, GC-intent reconciliation, every truncated suffix
length, corrupted frames, duplicate sequences, checkpoint/reopen/backup, and
write/sync failures during journal append and each checkpoint handoff stage.
An explicit three-writer interleaving verifies coalescing and protects newer WAL
across successful and failed earlier publication attempts.
Deterministic interleavings publish edits while the checkpoint mutex is released
and verify recovery both after successful installation and lineage replacement.

ReleaseFast benchmarks separately measure cursor setup, directory updates,
planner adaptation, manifest bytes, and physical storage after churn. Directory
microbenchmarks are not evidence of bounded end-to-end writer-lock duration.

The September 9 follow-up ReleaseSafe run covering LSM, manifest, relational,
schema, and backup filters passed 883 tests, with nine skipped and no failures
or leaks. A final LSM rerun after cursor admission and reclamation-progress
accounting passed 377 tests, with seven skipped and no failures or leaks.
The dedicated streaming decoder test exercises every allocation failure and
rejects storage reads above 64 KiB, including records crossing buffer boundaries.
Formatting, Zig generated-output checks, and storage-test discovery audit passed.
The subsequent resumable-planner pass passed 885 broad-filter tests (nine
skipped), then 379 LSM tests (seven skipped) after the GC-pruning and dense
emission changes. The final completed-job deadline edge case also passed the
dedicated all-allocation-failures closure test. All three runs had zero failures
and leaks. The broad run required native socket permission; the sandboxed
attempt failed only the 15 socket-dependent tests. Formatting, generated-output
checks, and the discovery audit also passed for this pass.
Earlier validation (before this follow-up) also included 11 native C API tests,
15 Python journal-inventory tests, and three-by-three cluster backup/restore.
Those native/integration suites were not rerun for this follow-up.

The writer-owner/GC/headroom pass passed 391 applicable ReleaseSafe LSM tests
(nine skipped), including continuous-write GC rebasing, selected-input
replacement above a lower-level tombstone anchor, cached negative-GC settling,
bounded background reclamation, mixed-output wire bounds, pre-WAL checkpoint
pressure, and failed-bulk-WAL fencing/replay. The previously looping native
relational maintenance-to-idle regression also passed. All 15 benchmark-tool
Python tests passed with localhost socket access. Formatting, generated-output
checks, and storage-test discovery passed. The repository-wide license check
reports 711 pre-existing violations; none are in changed files.
The final native broad-filter run passed 897 tests, with 11 skipped and zero
failures or leaks, including the maintenance-to-idle regression and the latest
GC, ownership, headroom, schema, relational, manifest, and backup coverage.
The repository-wide license
check still reports missing or stale headers in untouched Go/inference files;
those unrelated files were not changed for this work.

### Local ReleaseFast measurements

On the development macOS host, the 600-publication MemoryStorage workload wrote
224,746 manifest bytes versus 28,699,889 bytes for equivalent full snapshots
(about 128x less), including three checkpoints and 599 edits. Commit median was
6.49 ms and p99 17.67 ms; this is simulated storage, not a disk-latency SLA, and
maintenance checkpoint time is outside those per-commit samples.

Direct selection of one input took median 0.43 / 0.56 / 0.59 microseconds at
1,000 / 10,000 / 100,000 SSTs. This measures disjoint ranges, not broad overlap
closures or complete compaction installation. At 100,000 SSTs the augmented
directory retained 84,219,120 bytes, root pins took about 21 ns, and a one-run
metadata update about 9 microseconds. The new directory-backed cold scan setup
took 1–2 microseconds with 1,472 bytes of source state at 1,000/10,000/100,000
lower-level SSTs. The old 100,000-run projection took 4.3–4.7 ms in the same
follow-up run. This excludes SST I/O and is not an end-to-end query benchmark.

Streaming-recovery churn with 1,000 live runs and 64/256/1,024 replacements
retained 441,666 bytes at every history length; buffered replay retained
2.43/8.76/34.08 MB. At 1,024 edits, median replay time was 10.66 ms streamed
versus 11.86 ms buffered. At 64 edits streaming was slightly slower (0.93 ms
versus 0.81 ms): the demonstrated win is bounded retained history, not universal
CPU improvement. These MemoryStorage tests alternate measurement order and
track retained allocator bytes, not process RSS or peak memory.

A 100,001-input broad closure fell from roughly 503 ms to 24.7–31.7 ms across
subsequent runs after removing rank searches from its sort comparator
(about 16–20x). A one-path obsolete-ledger diff at 100,000 paths took
2.66 microseconds versus 2.01 ms for a complete comparison walk (about 757x);
root capture took 6 ns. These are local algorithm
microbenchmarks, not end-to-end throughput guarantees.

The follow-up resumable-closure benchmark measured 31.9–32.2 ms total at 100,001
inputs versus 28.0–28.1 ms for the unsliced control. The job used 98 slices;
the maximum measured slice per trial was 0.64–0.67 ms, with 6.01 MB of arena
capacity plus selected-result arrays. An initial implementation took about
49 ms; streaming dense rank assignment removed the extra per-input rank searches.
This is about a 14% CPU tradeoff for scheduler isolation, not a throughput win
or a bound on the complete maintenance call. Deadline checks are cooperative
between operations. Final validation, destruction, and installation are outside
these slice measurements. Directory metadata at 100,000 runs was 85.82 MB after
separating GC, overlap, and ID-index summaries (about 1.60 MB above the earlier
directory without GC summaries). Cold-cursor state remained 1,472 bytes.

The native-filesystem contention control uses the same format/planner with fsync
either held under the backend mutex or released through the publication lane.
Three trials of 200 writes/four workers showed roughly unchanged throughput
(around 0.12–0.13 seconds per trial). A concurrent point reader collected 50–88
samples per trial: median-of-trial p99 was 16.0 ms serialized versus 11.7 ms
coordinated, but one trial regressed and samples are too few for a tail-latency
guarantee. Trial ordering alternates; each case includes one unmeasured seed row.
Coalescing was sparse on this fast local disk;
this does **not** establish a throughput win for group commit. It establishes
correct overlapping publication and moves measured fsync phases outside the
backend mutex. Production device latency and workload concurrency must be
measured before claiming a latency/throughput improvement.

The writer-owner and incremental-certificate follow-up measured:

| Runs / selected inputs | Flat copy + sort | Writer-tree replacement | Initial dependency scan | Maximum scan slice | One-write delta |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 170 µs | 0.97 µs | 185 µs | 185 µs | 0.32 µs |
| 10,000 | 1.75 ms | 1.81 µs | 2.33 ms | 327 µs | 0.65 µs |
| 100,000 | 18.98 ms | 6.61 µs | 38.71 ms | 642 µs | 0.94 µs |

Writer replacement removes four inputs and adds one output, including candidate
capture and destruction, averaged over 31 iterations. Delta certification adds
one newer overlapping L0 run to a full-GC certificate, also averaged over 31
iterations; acceptance and coverage are optimizer-visible, and it visits
37/53/65 changed-path nodes. Initial certification at
100,000 inputs took 98 slices. Writer-owner retained metadata was 63.21 MB at
100,000 runs, in addition to the separate reader directory. These measurements
exclude SST I/O, end-to-end writer-lock latency, and concurrent allocator/device
contention; they establish algorithmic scaling, not throughput guarantees.
An earlier repeat measured 5.39 µs for the 100,000-run replacement and 0.87 µs
for its one-write certificate delta, illustrating ordinary local timing variation.

The owned-validation follow-up (local macOS arm64, ReleaseFast) measured:

| L0 runs / selected inputs | Previous scoring count walk | Aggregate scoring | Complete validation + scratch GC | Turns | Maximum validation turn |
| --- | ---: | ---: | ---: | ---: | ---: |
| 1,000 | 9.81 µs | <0.01 µs | 0.181 ms | 2 | 0.174 ms |
| 10,000 | 187.65 µs | <0.01 µs | 2.384 ms | 15 | 0.338 ms |
| 100,000 | 3.10 ms | <0.01 µs | 40.404 ms | 147 | 0.680 ms |

The scoring control executes the previous repeated rank-lookup loop 31 times;
the aggregate path executes 10,000 times against stable metadata. Its few-ns
measurements are below useful application-level precision, not a claimed
end-to-end speedup ratio. Validation includes the backend unlock/relock driver
and incremental membership-tree cleanup, unlike the earlier scan-only numbers.
The turn deadline is cooperative: allocator calls, mutex reacquisition and
separately budgeted reclamation can extend wall time under contention. These
measurements are not hard-real-time guarantees or sustained-write throughput
results. Deterministic regressions separately publish newer runs between
one-credit validation turns, check unchanged identity progress, reject replaced
inputs, and verify cleanup at allocation failures and shutdown.

The repeated physical-churn controls wrote 16,957,275 versus 16,036 SST bytes
for two-sided metadata updates with domain-aware compaction disabled/enabled.
The payload-family control wrote 6,357,075 versus 7,669 SST bytes, retaining
7,420,418 versus 1,071,495 total file bytes. These are deliberately skewed
metadata-around-payload fixtures, not general workload amplification ratios.

Reproduce from `zig/` with:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'writer owner narrow publication scaling benchmark' --test-filter 'dependency certificate delta scaling benchmark'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'lsm incremental manifest publication benchmark' --test-filter 'lsm persistent directory and lazy cursor scaling benchmark'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'obsolete ledger' --test-filter 'native durability lane contention benchmark'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'lsm overlap scoring aggregate scaling benchmark' --test-filter 'lsm dependency continuation slice scaling benchmark'
python3 tools/run_bounded_zig_build.py build unit-storage-test-audit
python3 tools/run_bounded_zig_build.py build lib-lsm-backend-sim-test -Doptimize=ReleaseSafe
```
