# VectorDBBench Findings: Prepared Publication, Recovery, and Subgroup Routing Experiments (2026-09-05 – 2026-09-07)

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/vectordbbench-2026-09-05-07.md@f3fd4d35](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/vectordbbench-2026-09-05-07.md). Originally relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 5421–8518 at commit 3ce5aff4ea) on 2026-10-12; durable decisions and the experiment-flag inventory were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) before the move.

Three days of PR 593 work on the native HBC dense index, measured through the public API with VectorDBBench (1536D 50K and `Performance768D1M` Cohere 1M; batch 100, four load workers, float16 candidate planes with authoritative float32 completion, C1/10/20/30 curves, 30 s mixed updates/queries, cold/warm restart). It covers crash-safe prepared posting publication, sealed WAL extents, accelerated CRC, residency/pool fixes, float16 source ownership and posting-local duplication A/Bs, four "recovery" runtime treatments, and subgroup routing experiments. Nearly every candidate was a tradeoff rather than an all-metric win; all experiments stayed default-off. All runs were on a shared host with concurrent compiler/benchmark work, which the log records as a caveat, never as an established cause.

## Decisions

### Publication, WAL, and lifecycle (2026-09-05 / 06)
- Prepare the next posting manifest/WAL and open/validate readers **before** changing `CURRENT` (background and explicit checkpoints); reject stale prepared WAL boundaries; preallocate the serving publication and destroy old generations outside the query fence — the failed 1M activation had published `CURRENT` before the reader could be admitted.
- Recovery, scan-admission, routing reconstruction, and patch-encoding temporaries are owned per operation, not pinned in the serving cache; native write captures get explicit value leases released before their generation lease.
- Posting `CURRENT` V4 can reference bounded sealed WAL extents; checkpoints covering an exact sealed prefix retain later WAL files instead of rereading/copying the active tail. Backup includes sealed files. The shared-vector manifest gained the same sealed-extent receipts (V5 only when extents exist).
- Background checkpoint handoff opens only immutable segments and rebases live overlay values onto the prepared root (no WAL replay or payload copy; still not O(1)). Unchanged immutable posting segments share reference-counted mappings across generations.
- Online publication reports admission contention/deferral separately from an attempted empty pass so startup does not charge contention to its zero-progress quarantine; initial, final, and broad-debt audits share one replay-ownership predicate.
- Persisted-run compaction inputs use window-scoped private cold readers that close before further admission — the first sealed/proof 1M stalled near 262,500 rows in a primary LSM descriptor-admission cycle (967 admitted descriptors in a 1024 pool).
- No-progress projection deltas abort before fsync/rename (no file, no `CURRENT` update, no chain-depth increase); unchanged no-progress attempts back off from 1 s to 60 s, bypassed by a new HBC publication identity or shared-vector revision — repeated 31,169,318-byte no-op publications had extended the chain toward full rewrites.
- WAL-resident float32 vectors derive the same bounded float16 projection (`nativePostingProjectionFromWal`) — one WAL row had rejected a whole leaf's optional plane, leaving 440 of 448 leaves deferred after restart.
- Optional maintenance selects a streamed full checkpoint when measured obsolete delta scan rows exceed 64 MiB and one third of the retained chain (bounds retained disk; does not remove whole-generation write amplification).
- Identity-preserving overwrites are proven with three sorted batch reads on one snapshot instead of ~three point reads per document; per-leaf immutable mutation-segment index gives constant-time serving-row validity.
- Shared CRC32 library `zig/lib/hash/src/crc32.zig` (`antfly_hash`; ARM CRC instructions or portable slicing-by-eight), later extended to full-text segments, LSM WAL/SST, and atomic sinks. Format, polynomial, and validation unchanged.
- The qualification harness validates every requested concurrency and all four measurement arrays (a partial result can no longer pass on serial recall) and rejects known native lifecycle failures in server logs. Frozen runner snapshot: `scripts/run_vdbbench_qualification_snapshot_20260906.sh`.

### Query execution and memory
- Residual reads use at most eight workers sharing one batch queue with the caller participating; extra std.Io read workers have a node-wide ResourceManager limit (default 2x logical CPUs) and never wait.
- Unfiltered native RaBitQ uses a statically dispatched score sink feeding the SIMD candidate gate (no leaf-sized intermediate arrays).
- Tree scoring reserves scalar score capacity itself — pressure reclamation freed score planes to zero and caused a SIGSEGV at 0x4 in `addChildCandidatesFromIds` during mixed traffic.
- Cosine radii are recomputed on full centroid refresh (`PostingStore.recomputeCentroid` had discarded radii for every non-L2 metric). Already-published NaN radii need maintenance/rebuild.
- Separate rerank metadata capacity from float32 decode capacity (a 900-candidate x 1536 float32 matrix was allocated per query before the batch was known); ~1.5 GiB resident MALLOC_MEDIUM, not mapped files, explained the RSS gap.
- Native-read arenas come from an IndexManager-owned pool under the resource governor (64 MiB aggregate, 32 slots, 4 MiB per slot, reclaimable) instead of per-call OS arenas.
- Generation-bound progressive tree scan admission: admit cumulative selected leaf work, choose tree vs flat from the pinned generation, preserve authenticated base-row costs when only the optional projection is missing — this removed a false ~92 MB/query 50K charge (now ~6.6 MB). Kept independently of the locality experiment.
- RaBitQ remains the routing/ranking hint; boundary rerank and authoritative completion are never weakened to manufacture wins; recall gate is <= 1 percentage point loss.

### Carried experiments (default-off)
- Aggregate caller/helper admission plus phase admission is the strongest repeatable 1M high-concurrency tail tradeoff, but C1 regresses, so not a default.
- Mean-centered quantized routing (`ANTFLY_EXPERIMENT_CENTERED_ROUTING` with `ANTFLY_EXPERIMENT_QUANTIZED_ROUTING`) kept as a candidate; persisted balanced subgroup layout, range-native scorer, weighted selector, and `ANTFLY_EXPERIMENT_GLOBAL_SUBGROUP_ROUTING` retained default-off.
- No-copy (posting-local float16 off) is a promising opt-in disk tradeoff, not a default.

## Rejected approaches
- Inline residual reads (`ANTFLY_EXPERIMENT_INLINE_NATIVE_RESIDUAL_READS=1`) — no material tail improvement (C30 1496.34 vs 1538.94 QPS); keep governed helpers.
- Per-call OS-backed arenas — RSS 1.9172 -> 0.6514 GB but C30 QPS -7.1%, p95 +12.6%; superseded by the pooled arenas.
- Flat-exact routing at 50K (paired tail diagnostic) — server p95 improved but HTTP p95/QPS did not.
- Simply increasing the delta-chain limit to absorb empty deltas (1M generations 12–18 each published 101-byte deltas before a 1,828,993,038-byte full generation 19).
- Four recovery treatments as defaults (`ANTFLY_EXPERIMENT_PROJECTION_PAGES`, `_PHASE_ADMISSION`, `_SCAN_PREDICTION`, `_REPLAY_FINALIZE`) — no repeatable all-metric win; pages cut physical reads only ~3.6% at 1M while bytes rose 16.4%; prediction is a negative control on flat 1M generations.
- Recovery v2 fused no-copy (+1.6% 1M QPS), queued page fallbacks, angular cosine bounds (zero certified stops; frontier minimum lower bound exactly zero on all 1,000 queries), within-page projection clustering (physical reads -0.67% at 1M, slower load).
- Zero-centered quantized routing — loses ~1.58 recall points.
- Per-leaf "three of four subgroups" quota — recall 98.405% -> 90.753%, C30 QPS -12.6%, p95 +14.1% despite -24.9% scored vectors; representative scoring added 0.912 ms/query while leaf scoring only fell 1.099 -> 1.051 ms.
- Global live subgroup plan as a default or 1M candidate — recall loss only 0.427 pp, but C30 QPS -4.04% and p95 +1.75%; planning (0.228 ms) consumed nearly all saved scoring time.
- Lowering effort, relaxing fsync, extending capture lifetimes, or raising the 120 s catch-up timeout to pass gates.

## Key measurements

### 50K qualifications (public API, fresh lifecycle)

| Candidate (date) | Ready (insert + catch-up) | Recall | C30 QPS / p95 | Notes |
| --- | ---: | ---: | ---: | --- |
| Prepared leases (09-05) | 34.2599 s (10.1902 + 24.0697) | .9864 | 2011.01 / 51.506 ms | startup quarantine logged |
| Sealed WAL / read governor (09-05) | 33.0744 s | 98.66% | 2026.93 / 59.746 ms | mixed RSS 2.3565 GB |
| Windowed reader / sealed WAL (09-05) | 32.5095 s | 98.64% | 2170.6132 / 54.418 ms | disk 838,012 KiB post-mixed |
| Retained-state / fused (09-06) | 34.3678 s | 98.56% | 2295.96 / 46.014 ms | best C30 p95 reference |
| Row-block B (reference) | 24.3805 s (15.2988 + 9.0817) | — | — | readiness target |
| No-progress fix (09-06) | 39.0032 s | 98.55% | 2210.8 / 50.993 ms | readiness regression |
| Native CRC + SIMD patch (09-06) | 21.6107 s | 98.69% | 2356.46 / 51.398 ms | failed: SIGSEGV in mixed |
| Shared CRC + tree-scratch fix (09-06) | 17.5995 s (9.5597 + 8.0398) | 98.44% | 2038.19 / 53.628 ms | qualified |
| Pooled read arenas (09-06) | 18.2336 s | 98.68% | 2,117.21 / 55.453 ms | mixed RSS 2.0183 GB |
| Scan/identity/compaction (09-06) | 16.3201 s | 98.61% | 2,509.55 / 39.641 ms | restart RSS 0.6688 vs 0.4282 GB |

Accelerated CRC kernel alone: 64 MiB in 6.36–6.47 ms vs 127.67–127.89 ms (~20x kernel-only, not end-to-end).

WAL-projection repair, same post-update corpus (old control repeat -> fixed): C30 1871.70 -> 2188.61 QPS, p95 35.763 -> 33.266 ms, projection reads 277.490 -> 0/query, total physical vector reads 406.648 -> 131.820/query; RSS 1.1535 -> 1.9164 GB, disk +~164 MB.

### 1M qualifications

| Candidate | Ready (insert + catch-up) | Recall | C30 QPS / p95 | RSS | Post-mixed disk |
| --- | ---: | ---: | ---: | ---: | ---: |
| Earlier regression-recovery (reference) | 617.3662 s | — | 828.00 / 94.317 ms | 7.0534 GB live | — |
| Windowed reader / sealed WAL (09-05) | 743.2529 s (305.5210 + 437.7319) | 99.25% | 890.5581 / 82.506 ms | 7.6647 GB mixed | 8,118,652 KiB |
| Pooled read arenas (09-06) | 327.1028 s (236.8447 + 90.2581) | 99.24% | 615.59 / 97.858 ms | 6.9973 GB | 10.4802 GB |
| Scan/identity/compaction (09-06) | 317.6381 s (207.4219 + 110.2162) | 99.20% | 818.195 / 73.278 ms | 6.0263 GB; kernel peak footprint 5.3066 GB | 9.2075 GB |

- Pooled 1M mixed writes regressed to 613.42 rows/s with 1,718.79 ms write p95. Same-generation pool vs original buffers: governed search memory -~32%, physical footprint only -~3.5%, C30 ~unchanged — the 50K RSS win does not generalize to 1M.
- Scan/identity/compaction: obsolete-row policy fired at 1,144,010,056 and 1,343,446,876 dead bytes; ANN allocation 3.9292 -> 2.6402 GB. Still +10.75% disk vs the 8,313,499,648-byte sealed-WAL run. Stable same-generation A/B: C30 816.146 -> 685.582 QPS, so the fresh-run gain is not attributable to the query change. 547,911,828 obsolete bytes can persist indefinitely below the one-third trigger.
- Across these runs the 1M query profile scores ~239–240K approximate vectors over 2,048 leaves with ~146–148 exact completions; routing proof checks overlap and never stop.

### Float16 source ownership (`dense_embeddings=primary_lsm` vs `vector_store`)
- 50K medians (A/B + B/A): disk 1.0931 -> 0.7021 GB (~35.8% less), C30 2,278.788 -> 2,339.361 QPS, readiness 3.6% slower, restart RSS 44.4% higher.
- 1M first pair: disk 9.240 -> 5.890 GB (36.3% less), C30 858.191 -> 975.918 QPS but p95 62.167 -> 79.212 ms, catch-up 45.350 -> 119.145 s. The reversed source arm **failed** the 120 s mixed catch-up gate (applied 10832 vs target 10905); not a qualified repeated 1M result.
- A complete 1M/768D posting-local float16 plane is 1,536,000,000 payload bytes; the source arm still rebuilt a 1.829-GB posting generation.

### Posting-local float16 plane on vs off (`ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS`)

| | 50K on / off (first prototype) | 50K on / off (progressive admission) | 1M off, two qualified arms |
| --- | ---: | ---: | ---: |
| C30 QPS | 2,597.792 / 648.088 | 2,372.633 / 1,729.279 | 873.905 (p95 67.437 ms) |
| Disk after restart | 0.6990 / 0.3751 GB | 0.6997 / 0.3752 GB | 3.8219 GB (vs 5.9202 on) |
| Recall | 98.535% / 98.425% | 98.570% / 98.380% | 99.055% |

No-copy shrank the 1M posting checkpoint from 1,828,948,817 to 240,893,761 bytes but issued 473.472 projection reads/query post-restart. The final locality-on 1M control failed the 120 s mixed catch-up gate (applied 10,832 vs 10,893), so the 1M matrix is not fully balanced.

### Recovery treatments, same-data 1M diagnostics (profiled, not official QPS)
- Recovery v2 aggregate admission vs control: C30 777.44 vs 748.14 QPS, HTTP p95 64.95 vs 82.88 ms, dense p95 44.66 vs 64.96 ms, C1 85.69 vs 90.72; the driver queue waits 9.74 ms/query.
- Recovery v3: defaults 606.57 C30 QPS at 99.0835% fixed-set recall; admission + zero-centered routing 731.35 QPS but recall 97.5100%; admitted mean-centered RaBitQ vs admitted exact: C30 678.94 vs 634.78, routing 1.590 vs 2.538 ms, recall 98.846% vs 99.040%, but C30/p95 did not improve in both orders.
- Fresh 1M baseline for routing work: 358.3199 s ready (228.3264 + 129.9935), C30 726.27 QPS, recall 99.040%.
- Fresh 1M layout (clustering) A/B: control 338.781 s / 465.60 QPS vs clustered 387.797 s / 556.30 QPS; inner patch preparation ~90% of posting-capture time but only 6.4% of readiness.

### Subgroup routing
- Offline probe (64 partitions, 1/4/16 subgroups): oracle certified bounds still retain 99.5–100% of vectors; 16-way subgroups reach 98.164% recall at 17,543.95 vectors/query vs 98.359% at 25,497.30 (50K, offline only).
- Offline global selection at 75% retention: 0.6406 / 0.6484 pp coverage loss (50K / 1M sample) vs 6.1250 / 8.2266 pp for the per-leaf quota; skips only 1.25 / 2.34 of ~128 parents.
- Representative kernel, 1,024 x 1536D score + sort: scalar f64 1.0554 ms, SIMD f32 0.1285 ms, f16 0.1256 ms, int8 0.0894 ms.
- Weighted partition + range scan: 4.9–5.9% lower combined kernel time vs full scan with 24.94% fewer vectors scanned (most per-leaf setup remains).

## Open items stated in the log
- Independently reusable physical HBC checkpoint chunks with durable and query-lease reference accounting (full HBC rewrite after eight deltas remains); zero-replay reader handoff.
- Mixed-workload replay drain at 1M sits at or beyond the 120 s gate in several arms.
- 1M RSS/physical footprint (e.g., 7.6647 GB mixed RSS in the sealed-WAL run, 5.3066 GB kernel footprint peak in the scan/identity run) and disk vs the 8.31 GB reference remain unresolved; idle consolidation needs its own budgeted policy.
- Tighter routing: current bounds overlap and 2,048 leaves are scanned at 1M; next step is generation-owned compact (e.g., prebuilt int8) representatives with a live recall and combined-cost gate.
- Unresolved degenerate-tree construction stall: 128-row inner-product insertion with branching factor 2 recurses deeply in `splitInternalWithOptions` / `computeNodeSplitRange`.
- The external recall-fixture helper's `dirname(@src().file)` path fails under the vector module root (not fixed).
- `unit-storage-test-audit` was not green: `artifact_payload.zig`, `vector_payload_store.zig`, and `vector_wal_view.zig` lacked manifest entries; a broader 199-test admission sweep had four failures (managed-index readiness/artifact accounting/dependencies and backup validation).
