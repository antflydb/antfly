# Vector Load and Query Performance Goal

## Objective

Make Antfly's vector ingest path competitive on large VDBBench-style loads while staying disciplined about memory and disk IO.

The target is not just "fast upload" or "fast query" in isolation. The durable target is:

- High serial upload throughput without unbounded foreground stalls.
- Low post-load optimize/readiness time.
- Stable query latency after load, including fresh readonly/query handles.
- Low disk write amplification and bounded on-disk run/manifest growth.
- Explicit memory budgets for any optimization that trades RAM for speed.

## Current Baseline

The main comparison case is VDBBench `Performance768D1M`, Cohere 1M, 768 dimensions, cosine, `k=10`, concurrency `[1, 5]`.

Recent observed runs:

| Label | Load | Insert | Optimize | QPS | Serial p99 | Recall | Notes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | --- |
| `antfly-memory-guard-rerun` | 950.735s | 628.441s | 322.295s | 185.869 | 23.0ms | 0.9899 | Best load so far, but left heavy LSM debt. |
| `antfly-knownbest-clean-1m-44924e58` | 1278.402s | 675.368s | 603.035s | 43.033 | 80.7ms | 0.9797 | Clean rollback run; poor query shape. |
| `antfly-e24a9140-cohere-1m` | 1286.462s | 712.267s | 574.195s | 238.953 | 16.5ms | 0.9899 | Current PR shape; better query than clean rollback, not best load. |
| `antfly-status-lsm-sparse-1m-20260607` | 1257.437s | 639.579s | 617.858s | 191.571 | 21.6ms | 0.9899 | Latest instrumented run. Insert is close to the best observed, but optimize/query start with heavy primary LSM debt. |
| `antfly-fresh-1m-rf-v8-filtercache-609195fd` | Aborted | 570.997s | Aborted | - | - | - | New manifest/filter split root. Insert completed faster, but optimize stopped making progress at 512.5k/1M indexed with 851 primary L0 runs; preserved as a diagnostic failure case. |
| `antfly-perbatch-bulk-1m-20260608` | Aborted | Timed out | Aborted | - | - | - | Per-batch primary `.bulk_ingest` experiment. Root reached 17G before completion; insert requests timed out in foreground LSM pressure compaction. |
| `antfly-primary-maint-fallback-1m-20260607` | Aborted | - | - | - | - | - | Primary-only maintenance fallback did not keep up at 1M scale; aborted around 500k uploaded with 1024 total runs, 1022 L0 runs, 16.4 MiB manifest, 5.2 GiB data root, and 894 L0 run-debt. |
| `antfly-currentscan-1m-20260608` | Aborted | 516.661s | Aborted | - | - | - | Current-scan mutable snapshot reuse improved upload and post-shutdown footprint, but optimize became ready/stuck at 987,500 published/query-visible docs while HBC logs later showed ordinal/vector cache at 1,000,000. Root shrank to 5.3G after shutdown cleanup. |
| `antfly-live-write-counters-1m-20260606` | Aborted | 597.728s | Aborted | - | - | - | Upload was fast, but optimize stalled around 887.5k indexed after primary LSM reached 1267 total / 1263 L0 at optimize start. Data root was 36G when stopped. |
| `antfly-finish-budget-1m-20260606` | Aborted | 664.881s | Aborted | - | - | - | One bounded foreground compaction step at bulk/dense catch-up finish controlled L0 (~132 at optimize start), but slowed insert and left dense catch-up only 475k/1M indexed; stopped as a bad tradeoff. |
| `antfly-hard-pressure-only-1m-20260606` | Crashed | 590.626s | Crashed | - | - | - | Letting hard L0 pressure run during active bulk flush kept L0 bounded (~180 at optimize start) and insert was fastest, but crashed in compaction ownership during optimize; unsafe. |

The 50k sanity case on `e24a9140` was healthy:

| Case | Load | Insert | Optimize | QPS | Serial p99 | Recall |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| `Performance1536D50K`, `k=100` | 31.086s | 16.431s | 14.655s | 1294.794 | 3.1ms | 0.9811 |
| `Performance1536D50K` after idle status snapshot fix, `k=100` | 25.678s | 15.425s | 10.253s | 1273.643 | 3.0ms | 0.9813 |
| `Performance1536D50K` after primary-maintenance fallback, `k=100` | 25.091s | 16.620s | 8.471s | 1254.047 | 3.0ms | 0.9813 |
| `Performance1536D50K` with live LSM write counters, `k=100` | 30.231s | 15.450s | 14.781s | 1519.410 | 2.8ms | 0.9813 |
| `Performance1536D50K` with hard-pressure-only patch, `k=100` | 24.327s | 15.991s | 8.335s | 1215.002 | 3.1ms | 0.9813 |
| `Performance1536D50K` with one-step finish budget, `k=100` | 32.760s | 15.915s | 16.845s | 1520.931 | 3.0ms | 0.9818 |
| `Performance1536D50K` with automatic per-batch primary `.bulk_ingest`, `k=10` | 43.203s | 30.759s | 12.444s | 2196.020 | 2.5ms | 0.9871 |
| `Performance1536D50K` with bounded detached LSM maintenance drain, `k=10` | 24.160s | 15.648s | 8.511s | 2364.001 | 2.4ms | 0.9866 |
| `Performance1536D50K` with current-scan mutable snapshot reuse, `k=10` | 25.629s | 15.183s | 10.447s | 2366.325 | 2.2ms | 0.9866 |
| `Performance1536D50K` current branch ReleaseFast after cache rebuild, `k=10` | 21.988s | 17.887s | 4.101s | 2309.275 | 2.2ms | 0.9866 |

## What We Know

- The biggest remaining failure mode is primary table LSM run debt, not dense HBC search logic.
- An earlier 1M run ended with about 1319 primary table run files and a 26G data root.
- A previous clean 1M run had an 88M primary table manifest and slow query-worker readonly opens around 6.8s.
- HBC/dense index state was comparatively small and usually looked ready or nearly drained when query shape was poor.
- `hbc-write-bench --inspect-root` now reports active table bytes, obsolete table bytes, due/future obsolete counts using the storage clock, manifest bytes, and embedded bloom-filter bytes for existing LSM roots.
- Corrected root inspection changed the 50k disk diagnosis: `/private/tmp/antfly-vdbbench-obsolete-due-fix-50k-20260607` is now 368M with zero obsolete primary or dense paths after the retention/shutdown cleanup path.
- The old 1M reclaim-deadline root is still 17G because it has due obsolete files that cannot be reclaimed by reopening that root: primary has 4.19G active table runs plus 8.29G due obsolete files; dense has 257M active table runs plus 4.36G due obsolete files.
- That same 1M primary manifest is 269.5M, of which 269.48M is embedded bloom filters. The dense manifest is 8.39M, also almost entirely bloom filters. The old root fails reopen because the primary manifest exceeds the current 128M manifest read cap; raising the cap would be a recovery workaround, not the underlying memory/disk fix.
- New roots no longer serialize bloom filters into the LSM manifest. In the `antfly-fresh-1m-rf-v8-filtercache-609195fd` root, primary manifest size was 327K and dense manifest size was 3.4K after a stressed 1M partial run.
- Reopening that same new-format 1M root succeeded and reclaimed due obsolete files: root size dropped from 32G to 19G, primary due obsolete bytes dropped from 16.4G to zero, and the remaining primary obsolete bytes were future-retained rather than recovery-blocked.
- The dense HBC index does not directly store raw vectors in the dense LSM for this workload; vector values are served through the external loader path, while the dense LSM stores HBC nodes, quantized payloads, metadata, and vec-id mappings.
- Table status now exposes a richer `storage_status.lsm` snapshot for benchmark artifacts: total/L0/lower-level run shape, compactable and overlapping L0 pressure, configured L0 limits, write-stall debt, overflow debt, obsolete path count, current manifest bytes, active bulk/readers, dirty manifest flags, maintenance score, and maintenance debt hint.
- Full `/metrics` scraping can perturb runs because metrics collection may walk LSM maintenance stats. Use sparse sampling or targeted artifacts during benchmarks.
- VDBBench performance comparisons must use an optimized `antfly` binary and verify the binary timestamp before starting the server. A default debug-build 50k sanity run on 2026-06-08 completed functionally but was not performance-valid: 44.488s insert, 60.472s optimize, 104.960s load, 430.978 QPS, 13.3ms serial p99, 0.9865 recall. After removing the 58G Zig build cache, `zig build install -Doptimize=ReleaseFast -Dmlx=false -Dmetal=false -Donnx=false -Dcuda=false` installed a 36.7M optimized binary at 2026-06-08 14:01:44; the valid 50k rerun loaded in 21.988s with 17.887s insert, 4.101s optimize, 2309.275 QPS, 2.2ms serial p99, and 0.9866 recall.
- The current VDBBench adapter defaults to `/db/v1`; Antfly v0.1 used `/api/v1`.
- The 2026-06-08 ReleaseFast 1M run after the status-snapshot fix completed successfully: insert 392.645s, optimize 101.544s, load 494.190s, QPS 443.369, serial p99 10.2ms, serial p95 9.5ms, recall 0.9899, NDCG 0.9916. During upload, new public/health connections were refused while existing benchmark connections continued; the likely cause is process fd pressure from LSM table-file caching plus hundreds of run files, not an Antfly process crash.
- The same 1M root showed small manifests but large due-obsolete file debt. Inspector output reported primary active table bytes 3.90G plus 5.13G due obsolete bytes, and dense active table bytes 257M plus 6.23G due obsolete bytes. So the scary 17G root was mostly unreclaimed obsolete LSM files; live primary payload is near the expected base64/JSON row size after compression, and live dense LSM payload is small.
- The native LSM fd cache is now capped conservatively per store so primary, dense, and text LSM stores cannot collectively reserve thousands of descriptors and starve HTTP listeners during large loads.

- Normal provisioned API writes no longer consult auto-bulk policy or open primary bulk sessions. They open cached DBs in ordinary async mode and let storage own mutable flush, L0 pressure, and background maintenance. Explicit bulk sessions remain for rebuild/import/final-state publication paths. Backends without an external maintenance waker now route `notePotentialMaintenanceDebt()` into the detached maintenance admission path, so ordinary write debt can wake storage-owned compaction directly.
- Older full `/metrics` snapshots from previous branch shapes showed `sorted_ingest_runs_total = 0`; direct sorted bulk ingest was not the observed source of the primary run explosion in those runs. The large counters were mutable rotations/flushes and manifest writes.
- Latest 1M instrumented run:
  - Upload finished in 639.579s, but optimize took 617.858s because dense catch-up entered optimize at only 487.5k indexed documents.
  - At optimize start, primary table shape was 806 total runs, 802 L0 runs, 71.9 MiB manifest, and 969 MiB retained WAL.
  - At optimize end/query start, primary table shape was 717 total runs, 712 L0 runs, 92.1 MiB manifest, 139 MiB retained WAL, and 584 L0 run-debt.
  - A post-run table status sample still showed 653 total runs, 648 L0 runs, 91.9 MiB manifest, and a 23 GiB data root.
  - Load-phase sample: physical footprint about 5.2 GiB, with hot work in L0 overlap planning, persisted-run merge cursors, compaction output writes, and status snapshot work walking L0 overlap stats.
  - Optimize-phase sample: physical footprint about 7.7 GiB, with hot work split between dense catch-up primary reads (`BoundWriteTxn.get`/`getManySorted`) and compaction merging/writing persisted runs.
- Idle live-writer runtime status publishing no longer refreshes full LSM maintenance stats just to populate startup WAL-retention fields. Active startup catch-up still reports retention, and explicit table status still exposes rich LSM shape. The 50k sanity run improved from 28.550s load / 22.356s insert / 6.194s optimize to 25.678s load / 15.425s insert / 10.253s optimize; final table status drained to 5 total runs, 4 L0 runs, 1.33 MiB manifest, and maintenance score 0.
- Background LSM maintenance can now lease active dense-bulk writer DBs for primary-only maintenance. The normal generic maintenance path still avoids dense index maintenance during active dense bulk work, but primary LSM compaction no longer has to wait for dense catch-up to finish. The 50k sanity run with this fallback finished in 25.091s load / 16.620s insert / 8.471s optimize, and final table status drained to 1 total run, 0 L0 runs, 2.1 MiB manifest, and a 1.2 GiB data root.
- The primary-only maintenance fallback is not enough by itself. The 1M run after that change was aborted around 500k uploaded because the primary table had already reached 1024 total runs, 1022 L0 runs, a 16.4 MiB manifest, a 5.2 GiB data root, and 894 L0 run-debt. This points back to primary run publication and compaction budgeting, not just background maintenance eligibility.
- Table status now reports live in-memory LSM write counters when the local writer DB is leased; warm-open status DBs still provide persisted maintenance shape as a fallback. The first attempt to expose write counters through warm status DBs showed all write counters as zero because those counters are runtime-only and are not persisted in manifests.
- The latest 50k live-counter run shows:
  - At optimize start: 132 L0 runs, 140 flush-output runs, 147 immutable rotations, 354 manifest writes, 228.6 MiB cumulative manifest bytes, 0 sorted ingest runs, 3 direct-bulk attempts/0 successes, and only 1 write-pressure event.
  - At optimize end: 23 L0 runs, 153 flush-output runs, 153 immutable flushes, 394 manifest writes, 279.7 MiB cumulative manifest bytes, 0 sorted ingest runs, 7 direct-bulk attempts/0 successes, and 1 write-pressure compaction.
  - Load improved versus the broken-counter rerun but remained slower than the primary-maintenance-fallback best 50k load. Query shape was good: 1519 QPS, 2.8ms serial p99, recall 0.9813.
  - This points to mutable flush/run-publication and manifest churn, not successful sorted ingest, dense HBC search, or aggressive foreground write-pressure work.
- The 1M live-counter run made the policy failure clearer:
  - Upload completed in 597.728s, but optimize started with dense catch-up at 437.5k/1M indexed and the primary table at 1267 total runs, 1263 L0 runs, 41.1 MiB current manifest, 30.9 GiB cumulative manifest bytes, and 235 MiB retained WAL.
  - At optimize start, live write counters showed 2065 flush-output runs, 2120 immutable rotations, 1837 manifest writes, 0 sorted-ingest successes, 69 direct-bulk attempts/0 successes, only 8 write-pressure events, 12 write-pressure compaction steps, and 3 overloads.
  - During optimize, L0 eventually dropped to 763, but dense catch-up stalled around 887.5k indexed with current manifest still about 103 MiB and data root at 36 GiB. The run was stopped rather than waiting indefinitely on a non-progressing state.
  - Load/midload/optimize samples are saved under `/private/tmp/antfly-1m-live-counters-*.sample.txt`. They show catch-up replay hot in LSM merge/current-scan reads and later in `BoundWriteTxn.commit -> finalizeDeferredRunWork -> enforceWritePressure -> compactPlanAt -> StreamingRunFileWriter`, with status sampling also visible in `lsmStorageStatsFromDb` maintenance-score/stat snapshots.
  - The practical takeaway is that faster upload is not enough. It front-loads L0/manifest debt that catch-up and query readiness then pay back at much higher latency.
- Direct bulk ingest now has a focused fix for one fallback loop: during an active bulk session, if existing backend mutable data plus the incoming bulk batch reaches the effective bulk threshold, the backend drains the existing mutable state into sorted ingest before direct-ingesting the incoming batch. This preserves transient batch coalescing and overwrite-heavy bulk-session coalescing; `lsm-backend-test` covers the matrix. The long-term boundary is now closer to Go/Pebble: provisioned API writes no longer open automatic long-lived bulk sessions for normal upload, and normal `DB.batchInternal` calls do not auto-map large weak-sync write-only batches to `.bulk_ingest`. True external/sorted bulk ingest remains reserved for rebuild/import/final-state publication paths.
- The per-batch primary `.bulk_ingest` experiment is rejected for normal online writes. The 50k run improved query shape but slowed insert; the 1M run then reached a 17G root before completion and timed out inserts. Sample `/private/tmp/antfly-perbatch-1m-timeout.sample.txt` showed the request path in `ProvisionedTableWriteSource.batch -> Backend.enforceWritePressure -> compactPlanAt -> StreamingRunFileWriter.appendEntry`, which confirms foreground caller-paid compaction rather than a Pebble-like background flush/compaction shape.
- Detached LSM maintenance jobs now drain a bounded batch of background steps per wake, matching the internal worker budget. This is the aligned direction for normal online ingest: give storage-owned flush/compaction a chance to catch up before hard L0 pressure reaches API writers, without API bulk sessions or index optimize calls. A fresh 50k run after this change finished load in 24.160s with 15.648s insert, 8.511s optimize, 2364 QPS, 2.4ms serial p99, 0.9866 recall, 78 primary L0 runs at optimize end, and a 1.5G root.
- The follow-up 1M bounded-maintenance run still failed the goal. Upload completed in 557.203s, but optimize entered with dense catch-up only 412.5k/1M indexed, 789 primary L0 runs, 627 L0 run-debt, 942 obsolete paths, and a root already around 18G. During optimize, L0 improved, but the benchmark status view went stale at 950k indexed while server logs showed HBC batches progressing to 962.5k; samples showed `finishDerivedCatchUpSessionAsync -> publishRuntimeStatusSnapshot... -> ApplyRwLock.lockShared` waiting while another thread was hot in `runMaintenanceStepLocked -> enforceWritePressure -> compactPlanAt -> StreamingRunFileWriter.appendEntry`. The root reached 54G with only 53G free, so the run was stopped. Samples are `/private/tmp/antfly-bgmaint-1m-l0spike.sample.txt`, `/private/tmp/antfly-bgmaint-1m-stuck950k.sample.txt`, and `/private/tmp/antfly-bgmaint-1m-stale-status2.sample.txt`. The immediate fix is to release dense catch-up tracking before status-hook notification and make the managed `.publish_consistent` hook use the bounded best-effort publisher while marking the table dirty for a later consistent refresh; this avoids status publication pinning catch-up completion behind apply-lock/LSM maintenance work.
- The current-scan 1M run exposed a different terminal publication bug. Upload completed in 516.661s and HBC eventually logged ordinal/vector cache counts at 1,000,000, but table status became ready while `doc_count`, `published_doc_count`, `query_visible_doc_count`, and `total_indexed` remained at 987,500. The fix is to make successful dense catch-up finish block on the applied-sequence/status flush before clearing the tracked catch-up session and notifying visibility; the terminal boundary can no longer skip the flush because `applied_sequence_mutex` was busy. The failed root was inspected, found to contain 5.3G of live post-shutdown data with no obvious obsolete-file leak, then removed.
- The 1M manifest/filter-split run confirmed the same policy failure after manifest bloat was removed:
  - Upload completed in 570.997s, but optimize reached a non-progressing state around 512.5k/1M indexed.
  - At the stall, primary table shape was 857 total runs, 851 L0 runs, 265K current manifest, 3.96G active run bytes, 21.17G obsolete bytes before reopen cleanup, and 722 L0 run-debt.
  - Load and optimize samples are saved at `/private/tmp/antfly-1m-load.sample.txt` and `/private/tmp/antfly-1m-optimize.sample.txt`. Both show hot work in LSM pressure compaction: `enforceWritePressure -> compactPlanAt -> buildCompactedRunsFromSnapshots`, with optimize specifically blocking dense catch-up commits in `IndexManager.commitDenseVectorMappingsWithRollback -> BoundWriteTxn.commit`.
  - The branch already has shallow document-data level targets (`128 MiB x10` for primary/doc stores and `256 MiB x10` for dense HBC), so the remaining 1M failure is not just the old tiny-level-target bug. Primary writes are still publishing too many small L0 runs; the run reported thousands of flush-output runs and `direct_bulk_ingest_success_count = 0`.
- A one-step foreground compaction budget at auto-bulk/dense catch-up finish is not the right default:
  - 50k query shape recovered to 1521 QPS and 3.0ms serial p99, with final optimize shape around 24 total runs / 22 L0.
  - On 1M, insert slowed to 664.881s and dense catch-up entered optimize at only 475k indexed. The run had good L0 shape (~132 runs at optimize start) but shifted the bottleneck into replay scans and finish work.
  - The midrun sample showed the HTTP insert worker mostly sleeping/waiting while catch-up was hot in primary LSM replay scans: `catchUpIndexFromMatchingCursor -> forEachReplayLaneFrom -> BoundCurrentScanTxn.LocalCursor.seekAtOrAfter -> MergeCursor`.
- Letting hard L0 pressure run from the active-bulk immutable flush path is unsafe as implemented:
  - The 1M run uploaded in 590.626s and started optimize at 625k indexed with ~180 L0 runs and no write-pressure overloads, which was promising.
  - The server then crashed during optimize. Crash report `/Users/ajroetker/Library/Logs/DiagnosticReports/antfly-2026-06-06-233154.ips` shows `SIGABRT` from libmalloc: `POINTER_BEING_FREED_WAS_NOT_ALLOCATED`.
  - Faulting stack: `PersistedOutputRunBuilder.deinit -> makePersistedRunsFromSelectedRuns -> buildCompactedRunsFromSnapshots -> compactPlanAt -> compactL0ToLimit -> Backend.enforceWritePressure -> flushOldestImmutableMemtableUnlockedBuild -> runImmutableFlushJob`.
  - This was not an out-of-disk or OOM termination. The later edit failure was disk pressure from generated benchmark roots, but the Antfly crash itself was a compaction ownership/concurrency bug.
- `PersistedOutputRunBuilder.appendEntry` now updates persisted-run bound metadata transactionally:
  - The crash pointed at freeing builder-owned `largest_namespace_name`. The old append path freed the previous largest bound before replacement allocation and writer append could fail, leaving stale builder pointers for `deinit`.
  - The fix allocates replacement smallest/largest bounds first, appends to the writer, then swaps builder ownership. Failure leaves the previous builder metadata intact for cleanup.
  - `zig build lsm-backend-test --summary all --test-timeout 5m` passed 203 LSM backend tests after this change.
  - This hardens a real ownership bug but does not prove that active-bulk hard L0 pressure is safe to re-enable; we still need a controlled retry with crash/metrics sampling before treating that policy as viable.

## Tradeoffs Already Tried

### Kept

- `ingest_compact` in `lsm-write-bench`.
  This gives a focused storage workload for random ingest plus full compaction drain before changing the end-to-end harness.

- Bulk current-scan mutable cloning with a memory cap.
  This avoids rotating mutable state into extra runs for some bulk current scans, but it must remain budgeted and visible in metrics.

- Document-data LSM level shape:
  primary, text main/WAL, sparse, and graph reverse use 128 MiB L1 target with 10x multiplier; dense HBC uses 256 MiB L1 target with 10x multiplier.
  Isolated storage smoke improved table bytes and max level substantially:
  stock 1 MiB/8x wrote about 650 MB and reached level 4; 128 MiB/10x wrote about 282 MB and reached level 1 on the same 100k ingest-compact smoke.

- Enrichment embedding batch metrics.
  These help separate API/upload stalls from embedder-side latency when load stalls happen in enriched tables.

### Reverted Or Avoided

- Broadly raising L0 run/byte limits as a production default.
  It can make upload look better by deferring pain, but it allows too much read/open debt.

- Disabling table block compression.
  This is not acceptable as a general fix because disk IO and footprint matter.

- Aggressive dense catch-up maintenance cadence.
  It improved some query shape but hurt insert throughput. Cadence alone is not the right fix.

- API-layer "optimize the index" coupling.
  Index maintenance should not leak into the API as a special-case readiness hack. The storage layer should own run publication, compaction budgeting, and drain policy.

- A foreground compaction step at every auto-bulk/dense catch-up finish as a default policy.
  It can control L0 run count, but the 1M run showed it slows insert and lets dense catch-up fall farther behind. Keep this only as an explicit offline/drain experiment, not as normal online behavior.

- Running hard L0 pressure directly from active-bulk run publication as a default policy.
  The 1M result was directionally promising, but the first implementation crashed in compaction output-run ownership during optimize. The `PersistedOutputRunBuilder` ownership bug is fixed and the active-bulk pressure path is now behind `write_pressure_during_bulk_ingest` / `ANTFLY_LSM_WRITE_PRESSURE_DURING_BULK`; keep it default-off until 50k and 1M benchmarks prove the insert/query tradeoff.

## Working Theory

The root cause is compaction shape and run publication policy.

The old global LSM defaults were sized for tiny stores: `level_target_bytes_base = 1 MiB`, multiplier `8`. Large document stores then descend through too many levels, causing unnecessary rewrite amplification.

The current end-to-end issue is sharper: primary table writes can publish too many L0/table runs during serial upload while dense indexing is still catching up from the primary replay stream. Background maintenance exists, but the load/readiness path can reach query execution with a large primary manifest and many table runs. Query workers then pay read/open costs even if dense HBC itself is ready.

The latest 1M abort suggests faster upload can make this worse by publishing L0 runs faster than maintenance can compact them. The next fix should reduce primary run publication frequency or make publication budget-aware, rather than only increasing maintenance cadence.

## Constraints

- Memory overhead must be explicit and bounded.
  Bigger memtables or clone buffers are acceptable only as configurable profile knobs with metrics.

- Disk IO must stay low.
  We should prefer fewer rewrites, fewer manifests, and fewer transient table files over hiding work behind larger tolerances.

- Query latency includes open/readonly handle cost.
  It is not enough for steady-state vector traversal to be fast if each query worker opens through a huge primary manifest.

- Bulk-load improvements must preserve online write behavior.
  A large offline load should not make normal small writes or internal stores worse.

## Roadmap

### 1. Make The Debt Visible

- Add or expose source/primary LSM run count, L0 count, manifest size, and maintenance score in benchmark artifacts without requiring expensive full metrics scraping.
  Current status: table status exposes the LSM run/pressure shape, current manifest bytes, score fields, and live write counters when a writer DB is leased.
- Break down readonly DB open profiling inside `openCoreResourcesFromPrimaryStore`, especially manifest load, range/shard/schema reads, and index manager open.
- Save final primary and dense index LSM shape summaries after VDBBench load.
- Track manifest bloom-filter bytes separately from manifest metadata. Evaluate reducing bloom bits, lazy-loading table filters, or moving filters out of the manifest before increasing the manifest read cap as a default.

Success condition:
Every benchmark result can answer: how many primary runs existed at query start, how large was the primary manifest, and how long did readonly open spend on storage.

### 2. Fix Primary Run Publication And Maintenance Policy

- Audit the primary mutable flush and immutable flush path under VDBBench-sized writes:
  - flush threshold bytes,
  - deferred immutable memtable limit,
  - read-snapshot mutable rotations,
  - foreground write-pressure compaction budget,
  - background maintenance wake/admission behavior while dense catch-up is active.
- Treat API auto-bulk as a rejected experiment for normal upload. The normal provisioned write paths no longer consult auto-bulk policy; any future bulk-style path should be an explicit storage/internal ingest window for ordered replay, snapshot restore, or final-state rebuild.
- Avoid creating hundreds or thousands of primary L0 runs during serial HTTP upload.
- Prefer storage-owned policy over API/index-specific optimize calls.
- Evaluate a bounded foreground drain at safe publication points:
  - max compaction steps,
  - max input bytes,
  - max elapsed time,
  - target L0 run count or manifest-size threshold.
- Benchmark the default-off active-bulk write-pressure gate at immutable flush and sorted/direct ingest run publication points. The gate preserves manifest deferral and only relaxes L0 write-pressure while a bulk session is active.

Success condition:
1M load reaches query phase with bounded primary run count and no multi-second readonly open.

### 3. Tune LSM Shape With Isolated Evidence

- Use `lsm-write-bench --workload-set ingest_compact` to sweep:
  - level target base and multiplier,
  - flush threshold bytes,
  - max run file bytes,
  - compaction input byte budget.
- Keep tiny internal stores on small defaults.
- Keep document/data profiles explicit.

Success condition:
Lower write amplification and max level without raising steady-state memory by default.

### 4. Add Memory-Budgeted Speed Knobs

- Treat larger memtables as an optional deployment/profile knob, not the default first fix.
- Track peak mutable, immutable, clone, compaction scratch, cache, and HBC memory.
- Make clone and compaction budgets visible in metrics and benchmark logs.

Success condition:
Any load-speed improvement comes with a measured peak RSS and tracked in-process memory budget.

### 5. Validate End To End

Run in this order:

1. `lsm-write-bench ingest_compact` storage smoke.
2. VDBBench `Performance1536D50K`, `k=100`, concurrency `[1,5]`.
3. VDBBench `Performance768D1M`, `k=10`, concurrency `[1,5]`.
4. Only then run broader concurrency or alternate datasets.

Primary targets for the 1M Cohere case:

- Load below 950s.
- Query QPS above 275 with serial p99 below 20ms.
- Primary table run count at query start below 128 as an initial target.
- No multi-second readonly DB open.
- Disk root size materially below the current 26G result.

## Open Questions

- What storage-owned write-stall and background-drain policy should bound primary L0 without making HTTP insert callers pay unbounded compaction?
- Can sorted-run ingestion be used for more of the primary VDBBench upload path, or do random doc keys prevent that without a staging/sort phase?
- Is the primary manifest large because of run count alone, or are obsolete path records and repeated manifest publications contributing materially?
- What is the right default max run file size for primary document payloads under this workload?
- Should query workers reuse/pool readonly handles during benchmark/concurrent search, or is per-worker open cost representative enough that storage must solve it?
