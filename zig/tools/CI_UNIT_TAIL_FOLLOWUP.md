# Unit-test tail after #800

Baseline: [run 35385138723, x86 unit job](https://github.com/antflydb/antfly/actions/runs/35385138723/job/105730855848).
Follow-up starts from main `497d957522` (#801). Earlier allocation tracing,
archive batching, centroid delta updates, and fast-directory changes are
already included. This work addresses the remaining costs, rather than
claiming those fixes again.

## Disposition of all ten fixtures

| Fixture | Change / retained boundary |
| --- | --- |
| Applied-state flushed overwrites | Retain all 1,025 flushes, compaction settings, publication bound, and cold reopen. Already opted into fast storage; needs the runner volume below. |
| Provisioner activation deferral | Inject a borrowed realtime clock for durable index-repair scheduling. Check the persisted generation/checkpoint, immediate deferral, deadline minus 1 ms, and successful retry at the deadline. Advance logical time instead of sleeping through the roughly 30-second backoff. Bound attempts to 32 state-machine steps. |
| Wide-vector update/reopen | Production change: retain up to 16 transaction-local source sums instead of evicting a sum whenever updates switch leaves. Retain leaf identity, mutation-version and member-count guards, conservative radius handling, and reconstruction on cancellation. Keep all 4,096 vectors, 1,536 dimensions, updates, queries, sidecar/cache comparisons, and reopening. |
| Columnar decoded reuse | Unit coverage scans cache-off/on once for each backend and shared/unique payload layout. Keep all 128 rows, 64 KiB payloads, at least eight blocks, allocation cleanup, zero primary reads, decoding/hit counts, and the 512 KiB cache bound. The complete warm-up plus nine timed rounds remain in `release-scale-test`. Both use leak-checked allocations with optional backtraces. |
| Reopened PageRank scheduler | Keep 20 maximum iterations and all 90 observed worker/coordinator steps, including closing and reopening every handle. Already opted into fast storage; needs the runner volume. |
| Three-server VOPR history | Drive each node every 10 virtual ms instead of 1 ms (production default is 100 ms). Retain public writes, merge, split, failover, restart, read-barrier failures/cancellation, service-rate checks, durable watermarks, and recorded-history replay. Bound each history to 8,000 transitions and each node to 400 rounds. |
| Relational clean coalescing | Retain both backends, the 512-row/two-block boundary, typed/null/JSON cells, zero primary reads, and reopen checks. Already opted into fast storage; needs the runner volume. |
| Repeated identity restore | Opt into fast fixture directories. Retain all 96 individually synchronized seed writes, run-backed metadata, 32 restore/publication/reopen cycles, identity checks, and the process allocator used by Darwin malloc diagnostics. |
| HTTP auth/admin middleware | Retain all 100 rejected batch requests and response/ownership checks. The first uses the reader's password; subsequent requests use a real restricted API key. Keep unauthenticated probes, Basic challenge, admin restriction, and admin identity checks. Production password hashing is unchanged. |
| Compatible HITS fan-in | Opt into fast directories and leak-checked allocations with optional backtraces. Retain all eight shards, nonuniform hubs, paired metric generations, and stale/active fan-in assertions. |

The wide-vector work contract tightens from fewer than 160,000 centroid
members scanned to fewer than 110,000. Observed work falls from 924
recomputations / 119,530 members to 778 / 100,278. A small alternating-source
regression also checks reuse, reconstructed means, conservative radii, and a
second external payload revision. Reverting to a single cache entry makes the
new work contract fail. Existing regressions cover mutation-version
invalidation, batch isolation, all three metrics, and previous-payload safety.

Index-repair timestamps consistently use the override or the backend runtime's
realtime clock. Ordinary filesystem runtimes still use real time; simulated
backends can use their own clock. The backfill test disables optional runtimes
and advances its clock only between synchronous repair calls. Activation's
real execution budget is unchanged.

## The runner storage change is still required

The baseline CI log explicitly reported `mount ... permission denied`, then
fell back to disk. A read-only inspection of the heavy ARC runners found only
the cache PVC and a **64 MiB** `/dev/shm`. Neither provides the intended bounded
512 MiB fixture workspace. This explains why earlier fixture-directory changes
did not deliver their Linux tmpfs benefit in that run.

`tools/fixture_workspace.py` now prefers a pre-mounted `/mnt/antfly-fixtures`
(or `ANTFLY_CI_FIXTURE_ROOT`). It verifies tmpfs, at most 512 MiB total capacity,
and at least 384 MiB free; it creates a private directory per job and never
unmounts a runner-owned volume. An invalid explicitly configured root fails
early. Without a pre-mounted root it retains the privileged-mount attempt and
explicit disk fallback. It intentionally does not use the runner's undersized
`/dev/shm` or redirect compiler caches and disk-durability fixtures.

Merge the following entries into the existing heavy ARC Helm values. Preserve
the existing container configuration, volumes, and mounts: Helm replaces lists,
so this is a fragment to integrate, not a standalone values-file override.
Match `fsGroup` to the runner's configured group.

```yaml
template:
  spec:
    securityContext:
      fsGroup: 1001
    volumes:
      - name: antfly-test-fixtures
        emptyDir:
          medium: Memory
          sizeLimit: 512Mi
    containers:
      - name: runner
        volumeMounts:
          - name: antfly-test-fixtures
            mountPath: /mnt/antfly-fixtures
```

This volume is charged to pod memory, within the existing headroom above the
22 GiB compiler reservation. Provision it on new runner pods without interrupting
in-flight jobs. This repository does not own the ARC release, and this PR does
not apply that cluster change. Confirm `df -B1 -T /mnt/antfly-fixtures` and a
successful workspace preparation before claiming CI storage acceleration.
The trusted-main workflow change also needs to be merged before it can select
the pre-mounted workspace.

## Local validation

Zig 0.16.0, macOS ARM64, Debug, Metal/CUDA disabled, ordinary local disk,
allocation backtraces disabled where the fixture opts out. Compilation is
excluded. These are **local results, not before/after Linux CI comparisons**.

| Requested fixture | Local seconds |
| --- | ---: |
| Flushed overwrites | 1.78 |
| Activation deferral | 0.43 |
| Wide-vector update/reopen | 10.38 |
| Decoded-reuse unit contract | 1.98 |
| Reopened PageRank | 4.05 |
| Three-server VOPR record + replay | 2.53 |
| Clean coalescing | 2.55 |
| Repeated identity restore | 6.56 |
| HTTP auth/admin middleware | 2.16 |
| Compatible HITS fan-in | 0.91 |

The retained decoded-reuse scale benchmark also passes (5.77 seconds locally).
VOPR records/replays about 4,333 transitions with 186–188 rounds per node,
versus about 15,108 transitions and 1,205–1,207 rounds in the prior CI profile.
The instrumented physical backend still reports 22 syncs and nine positional
writes per history; these counters do not include every filesystem adapter.

Focused owner binaries exercised each changed test and the related centroid
regressions. Temporary local build targets used to select owner roots are not
part of the change. HTTP and VOPR tests were rerun with local listener access
after the sandbox denied their bind calls; skipped tests are not counted as
passes. Workspace lifecycle/capacity checks and selection/ownership tooling
tests also pass. One local Zig compiler invocation exited with SIGSEGV; the
unchanged retry compiled and both backfill tests passed. Full Linux CI remains the acceptance measurement, especially
for the centroid work limit and VOPR budget.

Residual wide-vector time is chiefly insertion, sidecar persistence, and
queries (about 4.8 / 1.9 / 3.4 seconds locally); reopening is about 1 ms. Further
algorithm work should target those phases, not weaken the reopen assertions.


## First Linux CI result

[Run 35405408551, x86 job](https://github.com/antflydb/antfly/actions/runs/35405408551/job/105794404064)
passed. The hermetic unit step took **28m37s**, versus **28m46s** in the baseline.
These are two observed Linux runs, not controlled latency benchmarks. The
retained logs contain 9,734 distinct timing records; only one exceeds 30s.
The SDK job separately failed Ruff formatting for the two new workspace Python
files. They have been formatted, the exact Python formatting gate passes
(1,871 files), and all five workspace tests pass.

| Requested fixture | Baseline CI seconds | Follow-up CI seconds |
| --- | ---: | ---: |
| Flushed overwrites | 49.56 | 62.58 |
| Activation deferral | 32.59 | 0.74 |
| Wide-vector update/reopen | 32.19 | 26.09 |
| Decoded reuse (benchmark -> unit contract) | 30.12 | 10.73 |
| Reopened PageRank | 29.29 | 27.23 |
| Three-server VOPR record/replay | 27.23 | 20.06 |
| Clean coalescing | 26.92 | 26.16 |
| Repeated identity restore | 25.22 | 20.53 |
| HTTP auth/admin middleware | 24.68 | 1.12 |
| Compatible HITS fan-in | 23.79 | 11.29 |

The trusted-main workflow again reported a denied tmpfs mount and disk fallback.
The disk-dependent gains remain unvalidated until the runner volume is deployed.
PageRank still spends 16.52s closing handles out of 27.23s total. The unchanged
1,025-flush workload became slower in this run; do not treat runner-to-runner
wall-clock differences as production regressions without controlled profiling.

Deterministic work improvements did reproduce on Linux: wide-vector centroid
work is exactly 778 recomputations / 100,278 members, and VOPR record/replay each
uses 4,354 transitions and 187–189 rounds per node, within both new contracts.
VOPR's remaining CI time is not explained by excess polling alone.

The longest remaining tests are flushed overwrites (62.58s), graph warm rebuild
across summary pages (28.24s), reopened PageRank (27.23s), clean coalescing
(26.16s), and wide-vector updates (26.09s). Build-summary run steps still report
about nine minutes for partitioned DB-core and eight minutes for storage support.
These overlap other work and must not be added as elapsed time. Runtime graph
metrics total 190.18s (previously 198.82s); HBC totals 128.13s (138.48s), while
`graph.graph` totals 240.54s (204.67s). The improved individual tests therefore
have not yet yielded a material reduction in the whole gate's critical path.
