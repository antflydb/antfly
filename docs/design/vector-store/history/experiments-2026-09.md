# Separate Vector Store: Experiment Log (September 2026)

> Summary of a verbatim log. Full text: [docs/design/vector-store/history/experiments-2026-09.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vector-store/history/experiments-2026-09.md). Originally relocated verbatim from `zig/VECTOR_STORE.md` (lines 5-85 and 280-3480 at commit 271838a195) on 2026-09-16, with the completed pre-refinement 1M repeat qualification (lines 3838-3864) appended; durable decisions were folded into [`VECTOR_STORE.md`](../../../../zig/VECTOR_STORE.md) before the move.

From 2026-09-06 to 2026-09-14 the team built and qualified table-owned exact source payloads (`storage.dense_embeddings: vector_store`) against primary-LSM ownership (`primary_lsm`), then iterated on GC, loading, read-path and repair costs. Each step ran fresh tables in A/B then B/A order on a shared host, with lifecycle gates (churn, cold/warm restart recall, reclamation). The work ended with a scoped default promotion on September 14: new local, single-shard standalone tables without HA/replication now use `vector_store`. Most individual GC/read/cache experiments stayed opt-in. The original log is newest-first; this summary groups it by topic. Paths under `.benchmark-results/` referred to untracked local results.

## Decisions

- **Ownership shape (kept throughout).** Primary artifact metadata and committed stable references, a table-owned exact-payload store (`source-vectors/`, `storage/vector_payload_store.zig`), and ANN-owned version maps plus derived search structures. `indexes/vector-blocks/` holds logical keys, dimensions, source sequence/revision and a 32-byte payload digest, not a second exact-vector corpus. References carry a SHA-256 identity over the logical artifact key plus the complete versioned artifact. Source payloads stay exact float32 (or float16 plus lossless residuals when configured).
- **Prepare-before-primary commit.** Source payloads are appended durably before the primary transaction can commit references. Failed/ambiguous appends fence the source owner until reopen, and an unresolved primary commit blocks collection until recovery. Readers register ownership before taking primary snapshots. Primary source references survive dropping the last ANN index.
- **Scoped default promotion (2026-09-14).** When the create request omits `storage`, new local single-shard standalone tables (HA and replication disabled) get `vector_store`. An explicit `primary_lsm`, or an explicitly supplied empty storage object, keeps LSM ownership. Unsupported deployments keep `primary_lsm` on omission and reject an explicit `vector_store`. The resolved choice is persisted in the catalog before provisioning. Existing tables, including older catalog records with no storage field, stay `primary_lsm`. Snapshot/backup and split on vector-store tables reject with `VectorStoreLifecycleUnsupported`. Restore requests explicitly retain primary ownership. Evidence: the 50K/1M ABBA under Key measurements.
- **Read settings promoted with the default**, each with an override that restores the old behaviour:

  | Default behavior | Override |
  | --- | --- |
  | Float32 source and ANN payload encoding for fresh stores | `ANTFLY_HBC_VECTOR_BLOCK_ENCODING=float16` |
  | Direct ANN member bindings | `ANTFLY_SOURCE_VECTOR_MEMBER_BINDINGS=0` |
  | Exact mapped reads | `ANTFLY_EXPERIMENT_EXACT_MAPPED=0` |
  | Reduction-based query packing | `ANTFLY_EXPERIMENT_QUERY_PACKING=lanes` |
  | One bounded vector-read helper | `ANTFLY_EXPERIMENT_VECTOR_READ_SINGLE_HELPER=0` |
  | Batched source reads and positional batches | `ANTFLY_SOURCE_VECTOR_BATCH_READS=0`, `ANTFLY_SOURCE_VECTOR_POSITIONAL_BATCH_READS=0` |
  | Shared immutable source catalogs | `ANTFLY_SOURCE_VECTOR_SHARED_CATALOG=0` |
  | Replay-aware matrix loads | `ANTFLY_SOURCE_VECTOR_REPLAY_READS=0` |

  Existing source stores keep their persisted encoding (including float16); opening a store does not migrate it. Other experimental GC, admission, cache and read policies remain off.
- **Metadata-only reads are the default** (`ANTFLY_SOURCE_VECTOR_METADATA_ONLY=1`) for freshness checks, dense counter updates and key-only scans. Before the change, one 50K churn sequence resolved 409,560 artifacts (2.516 GB reconstructed) for 54,000 prepared payloads.
- **Collector uses the WAL-only durability boundary, not `sync(true)`.** The first incremental-GC prototype's `sync(true)` flushed scalar documents out of the memtable and cut query throughput by about 20%.
- **Posting-local projection copies default to off** for new posting builds (2026-09-07); `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` opts in.
- **Latest native formats only.** These formats were new in this PR, so only quantized directory V3, vector block V4, vector manifest V6, posting segment V4, posting WAL V5 and posting checkpoint V4 are supported. Earlier experimental stores must be recreated. Pre-PR HBC metadata/LSM migration remains supported. Pre-consolidation experimental tables fail closed with `VectorStoreReferenceFormatRequired`.
- **Background table-owned GC** (2026-09-13). Ordinary source collection moved out of synchronous writable open into snapshot mark turns capped at 16,384 rows and 2 ms with 25% scan duty, outside source/apply locks, plus 8 MiB copy turns and a 30-second periodic fallback. Debt only schedules verification; it never certifies liveness.
- **GC liveness separated from payload integrity.** GC's primary snapshot uses transient block-cache admission, and the all-live fast path checks source metadata in its pinned generation instead of reading and checksumming every payload. An all-live pass is a liveness check, not a data-integrity scrub. Exact consumption and copying keep their checksums.
- **Qualification rules adopted.** A quiet host is not required: run fresh tables sequentially in repeated A/B and B/A order, keep every arm, and report medians of paired ratios plus ranges. Any timed query error (including `503 index_rebuilding`) disqualifies an arm. The harness rejects write errors/retries even when the client exits zero. A recall change greater than 0.0001 between pre-shutdown and cold/warm reopen fails the restart gate. Profiled and clean timing runs are kept separate. A kernel speedup is not reported as a whole-query speedup.

## Rejected or not-promoted approaches

- **Primary-LSM matrix fallback during split/refresh** was replaced by replay-aware reads (`ANTFLY_SOURCE_VECTOR_REPLAY_READS`): 1M primary fallback vectors went from 1,399,904 to 0.
- **Catalog sharing alone, and source batching alone**, did not improve readiness. Sharing-only 1M: 513.6 s vs 349.7 s control, with a 34.6-second source-mutex wait. Batching-only 1M: 457.6 s vs 395.9 s and 547.1 s vs 377.5 s. Both needed immutable read publication and staged checkpoints.
- **Checkpoint-admission candidate (2026-09-10).** 1M readiness regressed 60.4% (404.782 s → 649.376 s) because of resource-admission bookkeeping. The cause was deleted hash slots accumulating in identity ledgers (10,000 absent-key lookups: 1.348 s churned vs 0.033 ms fresh). It was fixed by rehashing after capacity/8 removals.
- **Source location cache and physical rerank ordering** (`ANTFLY_SOURCE_VECTOR_PHYSICAL_RERANK_ORDER`). Median paired throughput change (50K / 1M): cache-only −51.7% / −11.9%, ordering-only +11.4% / −15.5%, both −5.3% / −27.1%. Neither was promoted. The original cache treatment was inactive (8,922,208 bytes allocated, zero hits) because `Store.snapshot` did not rebind `reference_location_cache`.
- **Member bindings, first measurement** (40-byte entries, capped at 1,048,576 / 40 MiB per generation). Despite removing over 99% of rerank metadata fetches, peak throughput was −4.6% at 50K and −7.8% at 1M, with p99 +21.1% / +56.3%. It stayed off then and was promoted on 2026-09-14 alongside the other read settings.
- **Unified vector fetch** (`ANTFLY_EXPERIMENT_UNIFIED_VECTOR_FETCH`): consolidates mixed batches but is not a consistent throughput win. Exact mapped views were the clearer warm-read improvement.
- **Inline vector reads** (`ANTFLY_EXPERIMENT_VECTOR_READ_INLINE`): at 1M, +38.6% C10 QPS but −19.9% C1. Not promoted; the single-helper policy was.
- **Broad read coalescing.** Allowing 64 KiB gaps reduced modeled spans only 1.63% / 0.20% while adding 8.3% / 1.8% bytes (50K / 1M).
- **Global rebuilds for build quality.** At 256 probes, recursive bootstrap reached 95.15% recall with 22,953 rows/query, and full global k-means 95.655% with 24,622 rows/query plus about 71 s of rebuild. Not promoted.
- **Append-only segments plus selective GC** (`ANTFLY_SOURCE_VECTOR_APPEND_ONLY`, `..._SELECTIVE_GC`). Process logical writes fell (1M ratio 0.666/0.660), but fixed 10K churn regressed (ratio 2.433/1.733), so the combination stayed opt-in.
- **Adaptive location cache, snapshot reads and group commit.** Adaptive cache worsened the query-concurrency curve. Snapshot reads lost write throughput in both pairs. Group commit never grouped (979 requests → 979 durable batches) because outer DB serialization prevents it, so it stays off until preparations can overlap outside the outer DB lock.
- **Bounded GC with directory coalescing** (`ANTFLY_SOURCE_VECTOR_MARK_STEP_ROWS`, `..._COALESCE_DIRECTORY`). 1M churn fell (81.2 → 62.8 s; 420.9 → 46.6 s) and churn writes fell 51–57%, but mixed writes fell 23–69%. Kept experimental.
- **Foreground marking** (`..._MARK_STEP_US=2000`, `..._MARK_OUTSIDE_LOCK`). DB batch lock wait fell 97.5%, but readiness was +29.6% (combined) / +60.9% (unlocked only). No 1M run.
- **Independent scan, incremental/lazy/deferred inventory, scan duty, rescue reappends, bitmap marking/locator, debt scheduling, sparse batching, incremental planning, small-table cutoff.** All stayed opt-in; each traded foreground throughput, memory or readiness against its benefit (see the full log for per-treatment tables). Notable figures: rescue reappends cut churn writes 21.6% but raised mixed RSS 30.9% and cut C1 throughput 20.2%. Sparse batching on a matched sparse layout cut cleanup from 336/338 s to 8.8/9.8 s (mark rows 11.56 million → 461,014), but foreground results were inconsistent.
- **Wider read dispatch.** It raised cold QPS from 56 to 80 but was 0.3–3.9% lower in settled QPS. The suggested next step was a cold-work policy with narrow warm dispatch.
- **Receipt reuse.** It halved marking (30.02 → 15.01 million rows) without consistently improving settled QPS or footprint.
- **Streaming directly over interleaved mmap payloads** regressed cold performance. Bounded positional read windows (4 MiB read-ahead) were used instead.

## Key measurements

Scoped-default ABBA (2026-09-14): both 1M pairs reduced disk by about 43% and observed peak RSS by about 28%, with peak QPS up 5.8–7.3% and mixed QPS up 10.2–19.1%. One 50K peak-QPS arm regressed 22.5%, 1M churn took longer, and source GC settled in 149–155 s vs 33 s for LSM ownership. All eight arms passed workload, restart and reclamation gates.

Fresh ownership comparison, 2026-09-11 (median paired `vector_store` vs `primary_lsm`):

| Measurement | 50K | 1M |
|---|---:|---:|
| Readiness time | −10.3% | −33.7% |
| Peak query throughput (C10) | −13.4% | +25.7% |
| Mixed query throughput | +5.2% | +25.7% |
| Fixed-count churn time | +8.7% | −1.3% |
| Total logical disk after restart | −49.2% | −42.4% |
| Process peak physical footprint | −44.6% | −55.7% |

1M cold-restart query p99 increased 24.7% (median paired ratio).

Detached-repair ownership comparison (2026-09-13): at 1M, readiness 14–20% faster and disk about 43% lower. Idle retained footprint was 824–1,207 MiB vs 254–258 MiB, and process-cold resident activation took 33–37 s vs about 3 s. GC liveness/scan-cache fixes then cut saved-1M activation from 33.1–36.7 s to 12.0–12.8 s and idle footprint from 1.10–1.22 GB to 271–278 MB. Background GC cut activation further to 3.6–4.7 s and maximum all-live GC locked intervals from about 4 s to 43 ms.

Pre-refinement 1M repeat (1M × 768 × float32, medians of two arms):

| Measurement | `primary_lsm` | `vector_store` |
| --- | ---: | ---: |
| Initial ready | 322.99 s | 326.20 s |
| Peak query throughput | 444.83/s | 442.72/s |
| Live recall | 0.9900 | 0.9899 |
| Total disk after restart | 7.588 GB | 4.490 GB |
| Mixed query throughput | 111.45/s | 122.14/s |
| Post-restart profile p99 | 91.55 ms | 83.40 ms |

That is a repeated disk reduction of about 41% at comparable recall. At 50K in the same series, the median paired disk ratio was 0.541, read-only throughput 0.919 and profile p99 1.234. Small (4,000-doc, 128-D) enrichment comparisons showed lower disk (paired ratio 0.797 float32, 0.611 float16) with readiness and restart costs (restart ratios 1.976, 1.933 and 1.936).

Read-path and loading figures that drove later work:
- **Query snapshots plus reduction packing (1M, vs R22):** C30 361 → 803 QPS (+122.7%), median p99 about 213 → 43 ms. C10 630 → 790 (+25.5%); C1 106 → 114 (+7.9%). Synchronization/identity changes alone gave +102% (50K) and +130% (1M) at C30. Reduction packing gave 7–11% (50K) and 13–18% (1M).
- **Apply-lock wait (1M):** mean 36.006 ms and p99 196.575 ms at C30, vs 1.642 / 13.749 ms at C10.
- **1M mapped C10 profile:** 12.647 ms of dense search = 5.823 ms leaf scoring, 1.247 ms routing, 3.816 ms scan admission, 0.518 ms exact loading. Eliminating exact loading alone would give only a 1.043x speedup. A 10x cut in candidate work would need about 24K candidates at the same recall, but earlier 20K-candidate runs reached only about 83% (vs about 99.35% with 241K).
- **Storage ceiling:** 1M × 768 × float32 is 3.072 GB of authoritative payload alone, vs roughly 4.1 GB total, so metadata elimination cannot give 10x disk savings at unchanged encoding.
- **1M loading after ledger fix plus replay reads:** readiness 523.179 / 694.012 s → 286.749 / 279.243 s, and split vector loading 137.881 / 269.885 s → 22.277 / 22.946 s.
- **Streaming merges (1M × 768):** source ingest plus final checkpoint 51.8 s → 34.5–36.8 s, and peak RSS 3,633 → 685 MiB. A bounded 256 MiB newest-suffix selection cut the largest write batch from 5.77–6.26 s to 0.86–1.00 s. Update pass plus checkpoint fell from 50.0/59.8 s to 26.2/27.1 s.
- **Streaming service (1M):** readiness 270.1/309.2 s → 246.5/263.8 s; maximum delta staging 4.15/4.62 s → 0.56/0.54 s.
- **Repair:** prepared posting replacement improved repair-phase p99 about 19% (29.83 → 24.06 ms; 27.38 → 22.18 ms). With resumable refresh, candidates cleared stale postings in 13.6/14.0 s under concurrency-10 traffic, while controls stayed dirty until about 63.5 s. Detached capture fell from 5.3–5.5 ms to 3.6–3.8 µs.
- **Source costs:** source-only 1M reopen 6.87 s → 0.10 s (first scan pays the deferred cold I/O); 50K retained search memory 181 → 82 MB.

## Lifecycle fixes and isolated qualification

AFQD V2's L2 directory reader expanded an omitted centroid-dot-product field to 75 zero floats, so a posting patch expecting the original 2,408-byte protobuf (CRC `8b77cabb`) saw 2,711 bytes (CRC `57cf4f95`). AFQD V3 records omission explicitly, and recovery applies patches with strict base length/CRC checks. Redundant generated-coverage initial builds are now retired by checking the durable source-replay boundary, configuration, identity, replay checkpoint, coverage counters and artifact cardinality.

Six isolated 50K comparisons on `vector_store` tables (24 of 24 arms passed) selected **append-only segments plus selective GC** (8 MiB GC step, enrichment batch 64) for 1M. The 1M A/B / B/A ratios were: ready time 0.753/0.901, process logical writes 0.666/0.660, C30 QPS 1.453/1.156, fixed 10K churn 2.433/1.733. Controls wrote 30.63/30.81 GB vs 20.40/20.32 GB for candidates. Recall was 0.9899–0.9903 live/cold/warm. The candidate is recovery-qualified but **not promoted as a balanced 1M performance default**, because of the churn regression. That regression came from more full marks and directory publications: one candidate spent 52.7 s marking and wrote 352.3 MB of directories while copying only 6.7 MB. Ownership indexing cut marking from about 1.5 s to 0.33 s, but its combination was not qualified. Reclamation clones settled in 107.08/117.55 s (controls) vs 34.15/35.68 s (candidates), each retaining exactly 1,000,000 payloads / 3,072,000,000 bytes.

Decision after 1M: keep table-owned source payloads, shared ANN payload ownership and the lifecycle fixes. Adaptive cache and snapshot reads stay out, and group commit stays off.

### Measured second-round results

Six opt-in controls (append-only segments, selective GC, ownership index, group commit, snapshot reads, adaptive cache) plus their combination each ran two pairs on 4,000-document, 128-D tables (28 arms). Payload segments looked promising (initial/update readiness ratios 0.77/0.90). Selective GC copied far fewer bytes, but checkpoint/directory writes offset some of the savings (update readiness 1.27). The ownership index narrowed marking (24/18 ms vs 41/25 ms) at a transactional cost (update readiness 1.26). Group commit never grouped. Snapshot-read QPS ratios of 5.97 and 0.98 were not a repeatable speedup. Demand-based adaptive cache allocation worked (9,312 vs 8,922,208 bytes). The all-six combination had update readiness 1.13 and semantic QPS 0.89, so it was not enabled. The group-commit comparison failed because a control returned one `index_rebuilding` 503 after reporting readiness. A 50K combined diagnostic then failed its restart gate with `PostingPatchBaseMismatch`, which was later explained by the AFQD V2 bug above.

## Defects found and fixed during qualification

- Compiled storage-owner open omitted shared resident-worker startup, so automatic source verification never ran (zero GC steps in three minutes). A roughly 920 peak-QPS screen measured without it is invalid as evidence.
- Incremental inventory was not discarded on read-view allocation failure (`InvalidVectorInventory` on retry).
- A query could search a runtime whose portable activation was pending. Queries now recheck after acquiring apply-shared and the catalog lease.
- Quantized-cache replacement suppressed during publication left stale routing scores. Old snapshots could consume newer cache entries.
- Deferred quantized rebuilds left `payload_dirty` set (128 records cleared at startup changed serving behavior).
- A source-capture cache gap allowed active-count metadata of 50,685 for exactly 50,000 posting members.
- Selective GC starved when every nonempty segment was below the density threshold.
- Source-budget OOMs: a second WAL-sized allocation during GC preparation, and a 77,594,648-byte mark map (332,745,026 live bytes against the 402,653,184-byte slice). Both are now admitted before allocation and retried.
- In-place startup reconstruction competed with capacity-deferred backfill, publishing a chunk at sequence zero.
- An L2 quantizer append left the unused centroid-dot array short (93 entries for 144 vectors).
- A status-cache merge shallow-copied owned strings. A status callback re-acquired the DB apply lock.
- An ASCH V12 vs V13 schema compare used raw bytes. It now compares decoded runtime schemas.
- Write-transaction range futures failed with `ConcurrencyUnavailable` at zero worker capacity.
- CLOCK insertion scanned the full array for holes. Removing that cut same-data cache-replay mean latency about 65%.

## Open items recorded in the log

- Fresh lifecycle qualification is still needed for broader deployments (replication/HA, split, snapshot). Snapshot/backup and split on `vector_store` tables reject until source-reference closure is supported.
- Separate foreground and idle GC copy budgets remain unimplemented. Copy-set construction and sorting still run under the source lock.
- GC publication stall: dominant work is in `PreparedPublication.openReaders` (index/key CRC validation), and the loader validates new blocks twice. Proposed fix: prepare immutable reader catalogs outside both source and DB apply locks and commit under a short fence.
- Small churn still causes two full copies (6.144 GB read, 6.384 GB written). Reclamation should be chosen by benefit, copy cost, disk pressure and maximum garbage age.
- The cause of a retained warm-pass latency outlier with background GC, and the approximately 80 MB mark workspace retained by repeated verification, were unresolved.
