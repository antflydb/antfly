# Lite write-amplification measurement

Two complementary, reproducible ways to measure Lite's write amplification:

1. An in-tree Zig counting harness (`zig build lite-write-amplification`) that
   instruments durable syncs, write/pwrite calls and bytes, and durable
   commits directly, with no external tracing tool.
2. A SQLite-matched throughput comparison
   (`scripts/bench/lite_sqlite_comparison.py`) that runs the same workload
   against SQLite WAL/FULL and a built `libantfly`.

See [`lite-single-commit-write-path.md`](lite-single-commit-write-path.md) for
the design work this measures against, and
[`write-amplification-baseline.json`](write-amplification-baseline.json) for
the current baseline receipt.

## 1. The counting harness

### Running it

```bash
export PATH=$HOME/bin/zig-0.17.0:$PATH   # or whatever Zig 0.17 toolchain you use
cd zig
zig build lite-write-amplification       # Debug; prints the report to stderr
```

Sync/write/op counts do not depend on build mode, so Debug is the default and
the fast option during iteration. The build step also accepts `-Doptimize`
for a one-liner final throughput run against an optimized build, e.g.:

```bash
zig build lite-write-amplification -Doptimize=ReleaseFast
```

(the counts should not change; only wall-clock timings inside the report's
JSON, if you add any, would.)

To run only the regression-ceiling test (no full report printed), or only
the report test:

```bash
zig build lite-write-amplification -- --test-filter "lite write amplification ceilings"
zig build lite-write-amplification -- --test-filter "lite write amplification report"
```

### What it measures

The harness lives at
`zig/pkg/antfly-embedded/src/storage/lite/write_amplification_bench_test.zig`.
For N (default 128) single-document updates of 1 KiB changing text, it
measures four modes:

- **primary-only**: the default `full_text_index_v0` index is deleted first;
  updates use `sync_level: "write"`.
- **indexed**: the default `full_text_index_v0` index is kept; updates use
  `sync_level: "full_index"`, which blocks until full-text is visible.
- **primary-only, 8 writers** and **indexed, 8 writers**: the same as the
  matching single-writer mode, but 8 real OS threads each submit `128 / 8`
  updates concurrently against one database, through the same
  `insertOne`/`antfly_db_batch_json` path the single-writer modes use. Every
  round is synchronized by a spin barrier (`Barrier` in the harness file) so
  all 8 threads arrive at Lite's single-writer admission queue at
  (as close to) the same moment as the OS scheduler allows on every one of
  the `128 / 8` rounds — real, deterministic contention, rather than
  leaving it to chance (see "Known limitations" below for what this
  currently shows).

Besides `calling_thread_syncs_per_update`/`background_syncs_per_update`,
each mode also reports `fsyncs_per_write_overall` (every durable sync,
calling-thread and background combined, per update),
`durable_commits_per_write` (every durable commit per update),
`mean_primary_group_size` (how many logical updates, on average, one
durable primary-commit transaction covered — the group-commit metric: 1.0
means no grouping at all) and an `indicative_writes_per_second_debug` wall-
clock rate. That last field is the one number in the report that *is*
sensitive to build mode, machine load and scheduling; everything else is
an operation count and is not.

It opens Lite through the same public Zig embedding seam the Go/Python/
Rust/TS SDKs reach via the C ABI (`capi.openLiteHandleWithRuntime`), with a
`.manual` `BackendRuntime` so catch-up/maintenance run synchronously and
deterministically, and its own instrumented `std.Io` (`CountingIo` in the
harness file) standing in for the real one.

### How the counting layer works

`zig/pkg/antfly-embedded/src/storage/write_amplification_trace.zig` is a
diagnostic-only module gated by `builtin.is_test`
(`pub const enabled = builtin.is_test;`), so every call into it is dead code
eliminated outside test binaries — it costs nothing in production.

- **Leaf IO counting** happens in the harness's own `CountingIo` (in
  `write_amplification_bench_test.zig`), which wraps the real `std.Io`
  passed to `capi.openLiteHandleWithRuntime` and intercepts `fileSync` and
  `fileWritePositional` to call `wal_trace.recordSync()` /
  `wal_trace.recordWrite(bytes)`. No production file needs to know about
  byte/sync counting.
- **Stage attribution**: a handful of production call sites push a
  `Stage` tag onto a thread-local stack before doing durable I/O
  (`const s = wal_trace.scope(.some_stage); defer s.end();`), so the leaf
  counters above know which logical stage is currently running on the
  calling thread. See "Files touched" below for the exact list.
- **Calling-thread vs background**: `wal_trace.registerCallingThread()`
  marks the OS thread(s) that issued updates (the harness's main thread, or
  each of the 8 writer threads in the concurrent mode). Any other thread
  that triggers a sync/write — e.g. a background maintenance/derived-worker
  thread — is `background`.
- **Background ops**: `wal_trace.recordOp(.source_turn)`,
  `.derived_worker_turn`, `.maintenance_lease_acquired` and
  `.serving_runtime_refresh` are incremented at a few named call sites
  independent of stage (see the design doc's publication-costs discussion
  for what these correspond to).

### Files touched (production, not test-only)

All hooks are one-to-four-line additions guarded (directly or via the
counting module) so they compile to nothing outside `builtin.is_test`:

| File | What was added |
|---|---|
| `storage/lite/docstore.zig` | `Txn.commit`: `.primary_commit` scope around `store.submitMutation(...)` (~line 3374). |
| `storage/lite/native.zig` | `publishCheckpoint`: one `wal_trace.recordCommit()` call after the commit_sequence/active_checkpoint advance (~line 11807). |
| `storage/persistent.zig` | `indexSegmentOwned`: `.lsm_wal` scope + `recordCommit()` around `self.wal.append(...)`, `.artifact_stage` scope around `self.materializeSegmentData(...)`, `.artifact_link` scope around `self.persistSegment(...)`, `.replay_retirement` scope around `self.wal.truncate(...)`. `pinNativeCheckpoints`: `.artifact_pin` scope. `publishProjectionSeal`: `.status_snapshot` scope. `RetiredSegmentFileDeleter.deleteOrQueue`: `.artifact_cleanup` scope around `self.storage.deleteFileAbsolute(...)`. |
| `storage/db/db.zig` | `PersistReplayProgress.run`: `.progress` scope. |
| `storage/db/derived/derived_worker.zig` | `catchUpIndexFromReplaySource`: one `recordOp(.source_turn)` at entry. |
| `storage/db/derived/derived_executor.zig` | `ManualRuntime.catchUpWorker`: one `recordOp(.derived_worker_turn)` at entry. |
| `capi/lite_connection.zig` | `Owner.refresh`: one `recordOp(.serving_runtime_refresh)` at entry. `Owner.acquireMaintenancePathLeaseAssumeLocked`: one `recordOp(.maintenance_lease_acquired)` on each of its two success paths. |

New files (not production-sensitive): `storage/write_amplification_trace.zig`
(the counting module) and `storage/lite/write_amplification_bench_test.zig`
(the harness itself). Build/catalog plumbing: `zig/build_support/embedded/embedded.zig`
(the `lite-write-amplification` step), `zig/pkg/antfly-embedded/src/source_catalog.zig`
+ `source_catalog_control.zig` (regenerate the latter with
`make -C zig source-catalog-generate` if you add more test files), `zig/pkg/antfly/src/storage/test_manifest.zig`
and `zig/pkg/antfly/src/capi_test_root.zig` (force the harness's tests into
the compiled selection — this repo's root `build.zig` always builds the capi
test aggregate with `server_integration_tests = true`, which uses
`capi_test_root.zig`, not `public_capi_root.zig`, as its root; the harness's
import was also added to `public_capi_root.zig` for standalone-package
consumers, but that path is not what `lite-write-amplification` builds).

### Known limitations (see the baseline receipt's `known_limitations`)

- `artifact_pin`, `lsm_wal`, `progress`, `status_snapshot` and
  `replay_retirement` currently read zero for this workload: the default
  index's segments stay small enough that `indexSegmentOwned` never takes
  the segment-file/segment-WAL branch those scopes are inside. The `other`
  bucket is correspondingly large; narrowing it (attributing more of the
  indexed path precisely) is useful follow-up work, not something this
  harness claims to have finished.
- `maintenance_lease_acquired` and `serving_runtime_refresh` read zero
  because the harness bypasses `capi/lite_connection.zig`'s `Owner`/
  connection-pool layer entirely (it opens through the lower-level
  `capi.openLiteHandleWithRuntime` seam). They would read nonzero through
  the full `antfly_lite_create`/`antfly_lite_open` C ABI path.
- The 8-writer modes' `mean_primary_group_size` currently reads 1.0
  (primary-only) and 0.5 (indexed, matching its baseline 2-commits-per-
  update structure), identical to the matching single-writer mode, even
  with the barrier forcing all 8 threads to arrive together on every
  round. This is not a harness bug: per
  `lite-single-commit-write-path.md`'s "throughput falls as writers are
  added" note, the writer slot, the DB apply lock and the per-call
  cross-process path lease are all held across the fsync, so the existing
  group-commit queue in `submitScopedMutation` cannot yet bundle
  concurrent foreground writers' commits regardless of how they arrive.
  The harness now measures this honestly instead of masking it with
  accidental non-contention; `mean_primary_group_size` will read above 1.0
  once that slot-held-across-fsync limitation is fixed, and the ceiling
  gate protects against the 8-writer modes regressing in the meantime.
  This harness counts operations, not latency; use the SQLite comparison
  (below) or `scripts/bench/lite_contention.py` for throughput/latency
  under contention.

### Reading the JSON report

The report test prints a JSON object between
`lite_write_amplification_json_begin`/`_end` markers, shaped like:

```json
{
  "text_bytes": 1024,
  "primary_only": {
    "updates": 128,
    "calling_thread_syncs_per_update": 3.0,
    "background_syncs_per_update": 12.84,
    "fsyncs_per_write_overall": 15.84,
    "durable_commits_per_write": 5.27,
    "mean_primary_group_size": 1.0,
    "indicative_writes_per_second_debug": 10.8,
    "stages": { "primary_commit": { "sync_calling_thread": 384, ... }, ... },
    "background_ops": { "source_turn": 0, ... }
  },
  "primary_only_8_writers": { ... },
  "indexed": { ... },
  "indexed_8_writers": { ... }
}
```

`sync_*`/`write_calls_*`/`write_bytes_*`/`commits_*` are totals across all
128 (or 1024, for 8 writers x 128) updates, split `_calling_thread` vs
`_background`; divide by `updates` for a per-update rate. The saved baseline
at `write-amplification-baseline.json` wraps this same shape with
`source_commit`, `harness_commit`, `measurement_scope`,
`known_limitations` and `receipt_comparison` fields.

### Regression gate

`zig build lite-write-amplification` also compiles and runs
`test "lite write amplification ceilings"`, which asserts calling-thread
syncs/update stay under the measured baseline plus 25% slack (3.75 for
primary-only, ~83.75 for indexed), and applies the same two ceilings to the
matching primary-only/indexed 8-writer modes so a regression specific to
the concurrent path is still caught even though it cannot yet show a gain
(see "Known limitations" above). These ceilings are intentionally loose —
**TODO(write-amplification)**: tighten them once the publication-shape work
in `lite-single-commit-write-path.md` lands, so this becomes a real
regression gate rather than a check against a large regression only.

## 2. The SQLite-matched comparison

`scripts/bench/lite_sqlite_comparison.py` (copied from the
`perf-lite-sustained-write-qualification` investigation and adapted to run
from this branch) runs matched durable writes against SQLite (WAL,
`synchronous=FULL`, ordinary `fsync`, an external-content FTS5 table
maintained by triggers inside the same transaction) and a built `libantfly`,
alternating engine order across rounds, for one and eight writers, with and
without a full-text index. It is the throughput/latency counterpart to the
counting harness above: diagnostic, not a production performance guarantee,
and not comparable to primary-acknowledged asynchronous indexing.

```bash
cd scripts/bench
uv run --no-project python test_lite_sqlite_comparison.py   # pure-Python unit test, no native library needed

# Full comparison (needs a built libantfly; see `zig build capi` in zig/):
uv run --no-project python lite_sqlite_comparison.py \
  --library /path/to/libantfly.{so,dylib} \
  --output /tmp/sqlite-matched.json \
  --directory /tmp/sqlite-matched-fixtures \
  --seconds 30 --workers 1 8 --rounds 2 \
  --source-commit "$(git rev-parse HEAD)"
```

Read the output the same way as
[`sqlite-matched-7bb.json`](sqlite-matched-7bb.json) from the earlier
investigation (not checked into this branch; see that worktree's receipt for
the shape) — each `case` reports `median_batches_per_second` for `antfly`
vs `sqlite`, `sqlite_to_antfly_ratio`, per-round `latency_p50_ns`/
`latency_p95_ns`, and a `verification` block proving every latest value,
index visibility, overwrite and integrity check passed. A `qualification_passed`
of `false` on any case (nonzero `busy_calls`, or not all workers making
progress) means that round doesn't satisfy the matrix's zero-Busy rule, even
if the rates themselves are informative.

## 3. How the two relate

The counting harness answers "how many syscalls, bytes and background
operations does one update cost, and where do they happen"; the SQLite
comparison answers "how much throughput/latency does that amplification
actually cost against a mature baseline engine doing comparable guarantees."
Neither depends on build mode for its core numbers (sync/byte/op counts and
`qualification_passed`); only the comparison's absolute throughput numbers
are sensitive to `-Doptimize`.
