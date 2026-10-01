# LSM Version Publication Validation Evidence (2026-09)

> Relocated verbatim from `docs/design/lsm-version-publication.md` (lines 953–1115 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`lsm-version-publication.md`](../../../docs/design/lsm-version-publication.md), specifically its "Verification" section. Durable decisions from this log were folded into that document before the move.

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
