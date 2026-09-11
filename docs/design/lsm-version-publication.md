# LSM version publication

## Logical-generation scheduling and unknown metadata

The directory maintains a persistent L0 generation index alongside its file
indexes. Each entry contains the publication sequence, physical file count and
bytes; subtree summaries expose total files/bytes and the largest generation.
Adding, replacing, moving or removing an SST stages these updates with the same
directory publication. Pinned readers and planners retain the old summaries.
The writer owner also maintains L0 count/byte totals, so foreground pressure
checks and lower-level byte comparisons do not walk SST metadata.

Scheduler probes use generation counts and an epoch/policy-specific negative
cache. They never run the tier selector. Background selection owns one pinned
directory and resumes in 2,048-operation / two-millisecond quanta off the backend
mutex. It preserves the flat oracle's oldest compatible window policy, iterating
logical generations rather than physical files. Selection may revisit generation
windows; it does not claim a linear bound in the number of generations. Its work
and memory are independent of the number of files per generation until a selected
range is emitted. Delta-seal and explicit-window boundaries use logarithmic
aggregate queries. Selected handles are emitted in bounded quanta, admitted by
actual selection size, and pass the existing dependency certificate before use.
Foreground hard-pressure and explicit-window callers drain their own jobs with
`std.Io` yields instead of consuming the background continuation. Cancellation,
policy changes, stale inputs and close reclaim partial ownership; background
cleanup itself resumes in bounded off-lock quanta.

Continuation ownership is independent of pressure admission. Every maintenance
turn retires a background bulk plan whose pressure or captured policy no longer
applies, before foreground-traffic deferral. Retired jobs retain an immediate
cleanup wake until bounded reclamation releases their pins. Still-needed but
foreground-deferred jobs advertise a 100 ms retry instead of spinning; wake
selection preserves earlier WAL and reconciliation deadlines. An in-flight job
does not request another immediate planning turn.

Bulk continuations also own their prepared scheduler work: input IDs, byte
totals and key bounds are collected from immutable handles in the existing
bounded emission phase, with separately admitted storage for IDs. The admission
check consumes this metadata without another input-count-dependent tree walk.
A denied grant leaves the selection and its completed dependency certificate
intact and advertises a 100 ms retry. An unchanged-epoch retry performs no
discovery or validation scan; intervening
publications advance the retained delta certificate before another grant.
Replaced inputs invalidate the job, while unrelated changes preserve it. Policy
changes, pressure relief, cancellation and shutdown still retire owned state.
GC wake deadlines apply the same foreground deferral without delaying earlier
durability deadlines. Domain/GC work preparation reads pinned handles off-lock
in cooperative 2,048-entry / two-millisecond quanta, not through repeated
writer-tree lookups under the mutex. Any intervening publication requires
input identity/closure validation before execution, including tombstone-free
tables and split-GC plans; absence of deletes is not an identity certificate.

### Phase-scoped file pins and independent accounting

Accounting ownership must not extend read visibility. `Directory.Accounting`
retains the six shared allocation accounts, not directory roots or SST payloads.
Capturing/releasing it takes constant work and no allocation. Accounting passes
deduplicate these accounts with live roots and selected handles; atomic byte
counters continue to include payloads during off-lock, sliced cleanup.

Ordinary, L0-only and GC admission owners retain selected handles and an
accounting token, but no redundant discovery root. Bulk jobs need their original
root while selecting/emitting; they retire it and clear both discovery cursors
before admitting validation. Their policy and negative-selection result remain
available independently of those cursors. Consequently, validation-budget or
scheduler denial cannot keep an otherwise unused discovery snapshot alive.

The dependency certificate still owns the epochs required for initial validation
and delta traversal. Once it advances, the superseded epoch goes through bounded
directory reclamation. Selected input files remain pinned until execution or
retirement completes. This bounds *redundant discovery-epoch* retention after
churn to the selected inputs, rather than the entire original table; active
readers, validation cursors, checkpoints and retention deadlines can legitimately
keep additional files alive. This is not a global storage-amplification bound.

Local arm64 ReleaseFast lifetime benchmark, one selected input after the rest of
its original directory becomes obsolete:

| Original runs | Previous retained payload pins | Accounting-only pins | Previous retained metadata | New retained metadata |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 1,000 | 1 | 1,047,328 B | 775 B |
| 10,000 | 10,000 | 1 | 10,470,328 B | 775 B |
| 50,000 | 50,000 | 1 | 52,350,328 B | 775 B |

Token capture/release averaged 9–11 ns across 10,000 repetitions. This synthetic
fixture measures ownership and retained metadata, not physical disk usage or
compaction throughput. A separate backend regression keeps all four admission
lanes denied, removes an unrelated SST, advances the certificate, and verifies
that its physical-file pin is released without rebuilding prepared input IDs.
Allocation-failure coverage verifies token/handle cleanup and exact shared
accounting after all directory roots have been destroyed.

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -- --test-filter 'compaction parked jobs' --test-filter 'directory accounting token'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'directory accounting pin retention scaling benchmark'
```

Tombstone metadata distinguishes known zero, known nonzero and unknown. Mainline
v9/v10 manifests remain readable, but their unknown counts now contribute explicit
maintenance debt. An augmented directory query finds an unknown input without
scanning unrelated files. A separate fair lane pins that input and streams its
sequential index and checksum-verified blocks with an allocation-charged scratch
budget. The optional-work foreground policy and per-turn background I/O budget
apply before scanning. Footer, sequential metadata and each compressed data block
are separately admitted physical read units. A turn reads at most one block and
processes at most 2,048 entries or two milliseconds; buffered entries need no new
I/O credit. Denied admission retains the verified footer/index/scan position and
advertises a 100 ms retry, without counting a corruption failure. The existing
oversized-single-job option permits one physical unit to exceed an otherwise
unused turn budget. These are cooperative bounds: one metadata decode, storage
operation or block decode may exceed the time target. Durable manifest publication
remains a separate durability lane, not part of the SST read budget.

After verifying entry counts, reconciliation releases its I/O scratch and
publishes only updated immutable metadata through the normal manifest protocol.
It does not rewrite the SST. Unknown delete ages remain zero (already eligible),
not a fresh grace period. Concurrent input replacement invalidates the result;
unrelated publications do not. Publication admission retains a completed count
for retry without rereading the file. Read/corruption failures leave counts
unknown, back off and rotate discovery so another input can make progress.
Reopen uses the persisted count instead of repeating the scan. Maintenance
diagnostics expose unknown runs, rows examined, reconciliations completed,
failures and active bulk planning jobs.

Regression coverage includes flat-oracle differential selection with byte caps
and sequence fences, immutable generation replacement/removal/level moves,
allocation failures, concurrent publication, old mainline manifests, partial
close, corrupt blocks and publication-headroom retries. Reproduce from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -- --test-filter 'bulk publication' --test-filter 'unknown tombstone' --test-filter 'tiers committed runs' --test-filter 'snapshot clone has'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'bulk publication no-op scheduling scaling benchmark'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'bulk publication large generation discovery'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'maintenance score aggregate' --test-filter 'bulk continuation' --test-filter 'unknown tombstone'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'bulk admission' --test-filter 'foreground deferred GC' --test-filter 'async batch reads tree'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -- --test-filter 'prepared compaction rejects'
```

Local Apple Silicon / Zig 0.16 ReleaseFast measurements (synthetic hot metadata,
not end-to-end write latency):

| Physical SSTs in one generation | Previous tree-walking no-op check | New scheduler probe |
| --- | ---: | ---: |
| 1,000 | 30.4 µs | ~0.5 ns |
| 10,000 | 802.1 µs | ~0.5 ns |
| 50,000 | 4.68 ms | ~0.5 ns |

All three inputs require one discovery operation and 1,072 bytes for the
generation tree. The sub-nanosecond figure measures the hot constant-time branch
in a tight loop; it is not a request-latency prediction. With 10,000 distinct
generations, selection performed 39,995 visits over 20 slices, taking 2.10 ms
total with a 121 µs maximum measured slice. These results establish file-count
independence and bounded scheduling turns, not a worst-case linear discovery
guarantee or a wall-clock deadline for storage I/O.

The scheduler-helper microbenchmark above did not cover pressure checks in the
actual backend scoring path. Those checks now use the writer owner's maintained
L0 file/byte totals too. A follow-up measuring `Backend.maintenanceScore()` over
10,000 calls (including its mutex probe) took 13.4 / 14.6 / 14.6 ns at 1,000 /
10,000 / 50,000 files in one generation; a repeat took 15.0 / 15.8 / 15.9 ns.
The pre-fix 50,000-file scoring probe took approximately 1.3 ms per call. This
establishes removal of the metadata walk, not a sustained-write throughput or
tail-latency guarantee.

Prepared-admission follow-up, using four logical generations and denying SST
I/O so the control isolates planning/admission (local arm64 ReleaseFast):

| Selected SSTs | Previous final admission turn | Prepared final turn | Maximum prepared turn | Unchanged-epoch retry |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 112 µs | 6 µs | 253 µs | 39 ns |
| 10,000 | 1.872 ms | 12 µs | 437 µs | 37 ns |
| 50,000 | 13.779 ms | 7 µs | 908 µs | 39 ns |

Retries average 1,000 forced-due attempts and assert that planning-slice counts
do not advance. Production retries sleep between denials. Total initial work
still scales with selected inputs; the measured win is removing the final
writer-tree traversal and repeated work during denial, not an end-to-end
compaction throughput claim. Admitted jobs still incur scheduler ownership and
conflict checks. Maximum turns remain cooperative rather than hard real-time
bounds.

### Owned admission through execution

Ordinary leveled, L0-only, and tombstone-GC selections now retain an admission
owner until execution or retirement. The owner prepares IDs, indexed membership,
byte totals, and bounds off-lock in 2,048-input / 2 ms cooperative quanta. Denial
keeps that work and its dependency certificate; unchanged epochs do not repeat
preparation, and later publications are checked against the retained certificate.
Memory, I/O, and scheduler denials advertise a retry deadline. Foreground-paused
discovery also advertises a wake, and retired owners receive cleanup turns even
when no runnable compaction remains. Policy changes and stale identities retire
the job through bounded reclamation. Existing remembered-work metrics include
these owned admission retries.

Persisted execution consumes immutable directory handles directly. These already
own metadata and physical-file pins, so successful admission no longer clones
every run or materializes another ID/pointer array under the writer mutex. Input
aggregation runs off-lock; run-ID reservation and publication remain serialized,
with identity/coverage validation before installation. Foreground finalization
counts completed compactions against its job allowance and every planning turn
against its wall-time allowance, yielding through `std.Io` between turns.

Scheduler admission checks job/byte capacity before conflicts. Prepared run-ID
indexes support membership probes against the smaller input set of each active
job; unprepared callers get a grant-owned, resource-accounted index. Completing
a grant releases its owned index and credit without affecting borrowed indexes.

Local arm64 ReleaseFast follow-up (synthetic metadata, not SST throughput):

| Input SSTs | Previous admitted handoff to first read | Pinned admitted handoff | Concurrent indexed admission | Capacity-full denial |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 0.2–0.4 ms | 39 µs | 2.7 µs | 6 ns |
| 10,000 | 2.2–2.7 ms | 137 µs | 33.9 µs | 6 ns |
| 50,000 | 13.4–14.9 ms | 673 µs | 593 µs | 6 ns |

The admitted benchmark obtains a real grant and stops at the first synthetic
SST read, including off-lock setup in its elapsed time. It is not a direct mutex
hold measurement. Concurrent admission averages 100 disjoint grants beside an
active equal-sized job; capacity denial averages 1,000 attempts. The earlier
nested conflict check took roughly 0.25 / 23 / 590 ms to deny these sizes at full
capacity. Initial index construction, SST opening/merging, and publication are
not included in the scheduler timings; total compaction work still scales with
input size. Cooperative budgets are not hard real-time bounds.

```sh
cd zig
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'compaction admitted pinned' --test-filter 'compaction scheduler prepared membership'
python3 tools/run_bounded_zig_build.py build lsm-backend-test -- --test-filter 'compaction admission' --test-filter 'compaction suspended broad'
```

## Read epochs

Async batch reads use the same representation-independent entry accessors as
scalar reads for live memtables and pinned mutable/immutable snapshots. A rank
from a tree-backed snapshot must never index its empty flat backing array.
Regression coverage includes values, tombstones, misses and an old snapshot
retained across an overwrite.

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
stable run IDs, augmented start-key ordering, end-key ordering, and per-level
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

### Monotonic overlap frontiers and phase-owned GC memory

Resumable closure discovery performs one initial overlap query. Every interval
outside that query is either strictly to its left (end before the initial lower
bound) or strictly to its right (start after the initial upper bound). Two
allocation-free endpoint cursors walk these disjoint sets as the closure grows.
They retain their traversal stacks across one-credit turns, including the initial
seek, and stop without consuming the next out-of-range interval. Newly selected
runs may extend either frontier; already visited ranges are never rescanned.
Inclusive endpoint equality and namespace ordering match the overlap query.
Ineligible levels and newer L0 runs are visited once but never extend coverage.
The selected-run AVL indexes still impose O(K log K) insertion work; the new
frontier traversal removes the quadratic repeated-query cost, not that cost.

The end-key index shares immutable run payloads and is maintained, forked,
accounted, and reclaimed with the other directory indexes. Endpoint-changing
replacements reserve both removal and insertion before mutation; old readers
retain the old endpoint. This deliberately spends one additional metadata-only
tree node per run to avoid building a job-local global projection or repeatedly
visiting nested ranges. No persisted format or legacy compatibility is added.

GC admission now reserves only its fixed owner/epoch headers initially. A
stable-address budgeted allocator charges membership nodes and closure arenas
before allocation. Component/progress arrays receive independent exact-size
reservations; both reservations follow a partial-progress plan and its larger GC
objective through validation, intent publication, execution, and cleanup.
There is no directory-count reservation and no second admission for those same
arrays at publication. The existing rank buffer is reused by validation.

Before starting bounded progress on an oversized GC objective, GC reclaims the
finished component arena and membership scratch in bounded chunks while retaining
its objective arrays. Before validation, another explicit handoff drains all
remaining discovery scratch off-lock (2,048 credits / two milliseconds per turn,
checking time between at-most-64-credit cleanup chunks). Only scalar intent
totals, pinned range bounds, and owned result buffers survive that handoff.
Cancellation, allocation denial, stale inputs, and close use the same ownership
cleanup. These bounds are cooperative, not hard real-time guarantees.

Local ReleaseFast metadata probes on macOS arm64 measured:

| Chained inputs | Previous directory visits | Frontier visits | Previous time | Frontier time |
| --- | ---: | ---: | ---: | ---: |
| 1,000 | 506,517 | 3,018 | 13.3 ms | 0.186 ms |
| 2,000 | 2,014,051 | 6,020 | 55.9 ms | 0.744 ms |
| 4,000 | 8,030,121 | 12,022 | 229.4 ms | 1.157 ms |

These single-sample rightward-chain timings include selection materialization,
not SST reads/builds. Leftward and two-sided chains are also covered by the
regression, along with a fixed-point oracle for nested ranges, namespaces and
mixed levels. At 100,000 runs the new end-key index accounts for 8,805,536 bytes
(88 bytes per node plus spare capacity); directory metadata totals 95,425,608
bytes in the existing directory fixture. One-run pinned metadata replacement
took 9–18 microseconds in the initial follow-up sample, and cold cursor storage
remained 1,488 bytes. These are measured costs, not a claim of free publication
or an end-to-end write-throughput improvement.

A one-input GC objective in 10,000 / 30,000 total runs peaked at 147,200 /
152,312 builder-budget bytes and completed under a 3 MiB cap. The old admission
formula alone requested 3,905,536 / 11,585,536 bytes. A 5,001-input regression
also fits a 1 MiB cap, accepts unrelated publication across the cleanup handoff,
rejects input replacement, preserves the rank buffer, and drains partial cleanup
and budget-denied jobs on close. Allocation-failure tests cover both full GC
and its oversized-component-to-progress handoff.

Reproduce from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'closure frontier' --test-filter 'GC phase' --test-filter 'persistent directory and lazy cursor scaling benchmark'
```

### Policy-bound continuation admission

Discovery identity includes the L0 target, source-level restriction, input-byte
limit, and oversized-job policy. There are at most two active closure slots:
ordinary background work and L0-only work. A foreground request never consumes
or replaces the background slot. Changing policy retires only that lane's old
job through the sliced cleanup queue; no slot is replaced while planning is
off-lock. Both slots participate in memory accounting and shutdown cleanup.

Background maintenance alternates between background discovery/service and L0
service, including jobs whose request has returned. An empty background slot
does not forfeit its discovery turn to a continuously replenished L0 slot.
An empty discovery turn records scheduling progress so no-op-sensitive workers
still reach the pending L0 job's next quantum. Synchronous compaction drains
those same continuations.
This preserves background progress without indefinitely pinning an abandoned
foreground epoch. Both lanes retain the existing per-slice work/time bounds;
they do not introduce parallel builders or an unbounded per-request job queue.

Continuation admission is independent of unrelated run count. A fixed header
and seed reservation covers the owner; a stable, callback-free budgeted
allocator charges arena growth before allocation. Scratch remains charged
through sliced reclamation, independently of the selected plan's lifetime.
Emitted handles and ranks acquire their own exact-size reservation, transferred
with the arrays to the selected plan. Oversized retries drain the superseded
arena before admitting replacement seeds, so progress does not require two
attempts to fit at once. Growth denial is reported as resource admission failure
and all partial ownership remains reclaimable on error or close.

Discovery, discovery-scratch reclamation, and validation are explicit phases.
The final emission slice hands ownership of handles/ranks to the selected plan
and yields. Reclamation gets its own off-lock 2,048-credit / two-millisecond
quantum; only after the arena and seed storage are released may validation
request its budget. Even the unchanged-epoch shortcut crosses this cleanup
boundary before execution, so builders cannot overlap with dead discovery
scratch. The pinned epoch and selected handles remain live throughout.

Validation rewrites the exclusively owned rank buffer in place and returns it
to the selected plan with the original output credit. No second rank array is
allocated. Its epoch is captured after cleanup, so input replacement during the
handoff is still rejected; unrelated publication can proceed. Cancellation and
close use the same idempotent sliced cleanup in every phase. The regression
uses 20,001 inputs under a 3 MiB cap that admits either phase but rejects their
combined scratch, and checks zero-credit/deadline yields, rank reuse, concurrent
publication, and close during partial reclamation.
Validation also shrinks its reservation as membership scratch is reclaimed,
then releases rank credit on ownership transfer. A completed certificate does
not retain an input-count-sized reservation while execution is admitted.

In a ReleaseFast metadata-only run, the 20,001-input handoff peaked at
3,059,512 bytes (2.92 MiB), versus 4,697,148 bytes (4.48 MiB) required by
overlapping discovery scratch and validation admission. Preparation and
validation completed in 91.7 ms; that timing is a single sample, not an
end-to-end ingestion result. Reproduce from `zig/` with:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'compaction phase handoff'
```

The ReleaseFast admission regression held two 5,001-input discoveries under a
4 MiB builder cap. Peak planning reservation was **1,187,312 bytes** at both
10,000 and 100,000 total runs; total preparation took 3.78 ms and 5.08 ms,
respectively. Both completed discoveries are held before transfer to make the
peak independent of retirement timing; the manager's peak also includes
transient fast-path reservations. The old whole-table reservations alone
would request about 49 MiB for two lanes at 100,000 runs. These are metadata-only
measurements, not SST I/O or end-to-end throughput results. Reproduce with:

```sh
python3 tools/run_bounded_zig_build.py build lsm-backend-test -Doptimize=ReleaseFast -- --test-filter 'compaction scratch admission' --test-filter 'compaction discovery receives'
```

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
