# LSM version publication

## Read epochs

Read transactions pin mutable state, immutable memtables, and SST membership
under one backend lock. They prepare that exact SST epoch outside the lock,
without retrying when publication advances. A per-epoch `std.Io.Mutex` coalesces
preparation; platforms without an I/O runtime use the platform sync fallback.

Point snapshots, current/replay scans, and bound/namespace write cursors share
the epoch's projection and topology. Current point probes do not freeze mutable
state after resolving its keys under the lock. Empty SST sets allocate no epoch.
Final release queues retirement under the backend mutex; detached reclamation
retains every tree's accounting handle until it reacquires that mutex.

## Incrementally maintained planning indexes

Each immutable directory owns persistent indexes for read precedence,
namespace/key-family domains, key-range ordering, and per-level counts/bytes.
Payload ownership and SST pins are shared across indexes. A publication stages
all allocations, then updates only changed paths. Level moves, split outputs,
and GC-intent changes use the same directory update path.

The existing positional planner is fed by a linear stable-ID adapter instead
of sorting all runs again. A single already-sorted level/domain bypasses ID-map
construction. GC closure construction uses stable bucket scatter to restore read
precedence without sorting each component. Mixed-domain SSTs keep conservative
overlap closure handling.

Planning builds are single-flight and memory-admitted. Plans are revalidated
against live inputs before installation. GC intent is merged from those live
inputs, including requests made while ordinary compaction was building.

## Manifest journal

The durable file remains `manifest.bin`. New journals start with `ALSMJNL1` and
a full checkpoint frame. Later frames contain stable-ID removals, added or
updated run metadata, obsolete-path removals/upserts, and the next run ID.

Each 24-byte frame header contains payload length, sequence, checkpoint flag,
and a CRC32 of the preceding header fields. A payload contains removal lists
and an embedded standalone manifest; a separate CRC32 protects the full payload.
The first frame has sequence zero and the checkpoint flag. Subsequent sequences
must be contiguous. Embedded manifests also retain their own checksum.

Directory differences skip shared subtrees, including after AVL rotations.
Unchanged SST metadata is neither copied into edit descriptors nor serialized.
Run-layout validation checks changed runs and their final neighbors against the
last validated durable root; cold or administrative publications validate the
whole layout. Removing a run cannot introduce an overlap in a valid predecessor.
Obsolete-path changes are detected against a tracked map. Clean publication
does not allocate encoding buffers or write another record.

The writer reserves encoding/map scratch before allocating it. An edit is
appended and synced before the durable-directory pin moves forward. Any failed
append or sync invalidates the append state; the next attempt atomically replaces
the file with a full checkpoint. Publication debt remains dirty even if a flush
already installed its SSTs and retired its mutable generation.

Checkpoints replace the journal after at most 256 edits or 8 MiB of edit suffix,
with a smaller suffix budget near the 128 MiB manifest reader limit. The base
checkpoint is excluded from the suffix counter so a large store does not
checkpoint on every update. A single edit that would exceed the reader limit is
retried as a checkpoint after its scratch is released.

Recovery replays the complete prefix, accepting only an incomplete final frame.
Complete malformed records, checksum failures, broken sequences, unknown
removals, and regressing run IDs are rejected. Writable reopen begins with a new
checkpoint on its next publication, eliminating any incomplete old tail.
Read-only opens do not mutate the journal.

Native backup exports a standalone manifest for the pinned live run set,
without obsolete history or a concurrently advancing journal. Existing restore
consumers therefore need no journal suffix or unexported obsolete SSTs.
Inventory tooling can also replay journals directly without opening a backend.

## Cost boundaries

Metadata updates and journal edit encoding are incremental. This does not make
all LSM operations constant-time: cold read/planner adapters still materialize
flat arrays, candidate selection scans applicable runs, and administrative
rewrites/checkpoints still process the full run set. Checkpoint replacement is
currently serialized by the publication lock. Obsolete-path diff detection is
linear in tracked obsolete paths, though only changes are written.

A future direct stable-ID planner/cursor API can remove the flat adapters.
Moving checkpoints completely off-lock requires staging a pinned checkpoint
and preserving a concurrently appended suffix at installation; merely dropping
the lock around the current replacement would lose committed edits.

## Validation

Tests cover old epoch preparation after publication, shared scan topology,
admission rejection, allocation-failure cleanup, persistent ordering against a
rebuild oracle, level moves, GC-intent reconciliation, every truncated suffix
length, corrupted frames, duplicate sequences, checkpoint/reopen/backup, and
write/sync failures during both journal append and checkpoint replacement.

ReleaseFast benchmarks separately measure cursor setup, directory updates,
planner adaptation, manifest bytes, and physical storage after churn. Directory
microbenchmarks are not evidence of bounded end-to-end writer-lock duration.

### Local ReleaseFast measurements

On the development macOS host, the 600-publication MemoryStorage workload wrote
258,489 journal bytes versus 28,626,987 bytes for equivalent full snapshots
(about 111x less), including three checkpoints and 597 edits. Commit median was
6.03 ms; this is a simulated-storage workload, not a production disk-latency SLA.

At 100,000 SSTs, maintained planner adaptation took a median 8.27 ms versus
16.67 ms for rebuilding the order. The directory retained 81,817,760 bytes,
about 31% more than the prior single-index directory. Root pins took about
25 ns and a one-run metadata update about 7 microseconds. Cold projection still
took about 8.5 ms and remains linear in the number of runs.

Reproduce from `zig/` with:

```sh
python3 tools/run_bounded_zig_build.py build lib-storage-test -Doptimize=ReleaseFast -- --test-filter 'lsm incremental manifest publication benchmark' --test-filter 'lsm persistent directory and lazy cursor scaling benchmark'
python3 tools/run_bounded_zig_build.py build unit-storage-test-audit
python3 tools/run_bounded_zig_build.py build lib-lsm-backend-sim-test -Doptimize=ReleaseSafe
```
