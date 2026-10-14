# VectorDBBench Findings: Confirmed Findings and Native HBC Measurements (August – early September 2026)

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/vectordbbench-native-hbc-2026-08.md@f3fd4d35](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/vectordbbench-native-hbc-2026-08.md). Originally relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 497–4130 at commit 3ce5aff4ea) on 2026-10-12; durable decisions were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md) and [HBC.md](../../../../zig/HBC.md) before the move.

This log covers the public-API VectorDBBench effort (OpenAI 1536D 50K and Cohere 768D 1M, batch 100, four load workers unless noted) that took the 1M load from a coworker-reported ~3,000 seconds to inside the ~13-minute target. It moved through primary-LSM cloning fixes, a posting segment/WAL sidecar, a WAL-authoritative and then whole-tree native HBC generation format, primary geometric-merge policy, a shared float16 exact-vector block store, posting-local float16 scan planes, and bounded-memory checkpoint publication (runs r55–r126). Times are one-machine diagnostics on a contended development host unless a mean/range is given.

## Decisions

### Primary store and public write path
- The VectorDBBench adapter deletes the default `full_text_index_v0` before creating the external dense index (`ANTFLY_VDBBENCH_KEEP_DEFAULT_FULL_TEXT=1` keeps it) — benchmark isolation, not the mixed-workload product fix.
- Preserve `beginProbe`/`beginCurrentScan` and sorted `getMany` through erased storage adapters; `DocStore.get` uses a point probe — type erasure had turned point probes into whole-mutable-state snapshot clones (product bug, not benchmark-specific).
- New transaction revisions publish a durable sorted intent-key manifest in the same batch as their intents; resolution uses current-tip point reads. Older in-flight transactions fall back to one stable prefix scan (rolling-upgrade compatible). Resolution retains a four-byte empty manifest until terminal cleanup so recovery does not re-scan terminal records.
- Dense replay memory estimates use the configured vector dimension (the old 384-D assumption under-reserved 4x/2x for 1536-D/768-D).
- HBC replay batches identity/mapping reads, split metadata, and transformed-vector loads, and keeps maintenance inside bounded publish windows; the offline outer-finish mode is explicit only.
- Separate admission lanes for replay-priority work and soft compaction; derived backlog uses a bounded 16-sequence drain window (high water 200, resume at 100).
- Compaction publication relocates exact immutable input IDs and recomputes overlap closure instead of comparing positional slices — concurrent flushes shifting L0 had discarded valid work and starved compaction.
- The public write coalescer hands drainer ownership to a waiter once its own entry commits — the elected request was starving to the client's exact 120 s timeout.
- Compile reservations reflect observed macOS peaks (API 11 GiB, storage/distributed 18 GiB, inference 16 GiB, CLI 3 GiB, broad data runtime test 12 GiB, metadata public simulation 14 GiB) rather than forcing `-j1`.
- Replay readers snapshot only their append-only mutable replay lane (default full-text + dense 50K: 46.3 MB copied vs 3.51 GB).
- External (`external: true`) dense/sparse indexes are admitted at an activation fence after a streaming key scan proves no caller-owned artifact predates the catalog entry — a single readiness row had caused three needless durable-repair attempts (+~36 s at 50K).
- Foreground hard-pressure compaction honors `max_compaction_input_bytes` (with the oversized minimum-closure escape); the product default stays 2 GiB.
- Keep the 32 MiB WAL checkpoint floor (1 MiB lost the A/B).
- LSM reports job-level compaction telemetry (count, bytes, largest input/output/duration).
- Profile-guided LSM: persisted compaction cursors cache the decoded entry; adaptive Snappy backs off after four consecutive blocks miss its 12.5% savings floor, reprobes periodically. On-disk codec unchanged.

### Native HBC storage format
- Design target: a packed immutable checkpoint plus a shallow, buffered derived WAL, not another general-purpose LSM. An unbounded lazy base/delta tail is rejected even when its load number is attractive.
- Publication order everywhere: immutable segment, next-generation WAL, then checksummed `CURRENT`; old artifacts deleted only after `CURRENT` is durable.
- Per-entry checksum verification is memoized (lazy, once per payload) — recomputing CRC32 per access measured ~52 µs p50.
- Derived WAL encodes replacements as checksummed copy/literal deltas (8-byte anchors; later sampled every 16 base bytes), with explicit coverage records and kind-specific tombstones; admitted only at the exact durable source sequence.
- Every query leases one immutable generation; overlays are installed only after WAL commit.
- WAL-authoritative transition: after one complete checkpoint, a sticky `AUTHORITY` marker makes the posting segment + WAL authoritative for all query-facing HBC state; recovery fails closed and never reopens the legacy HBC LSM. Raw vectors stay source-owned. Legacy derived LSM rows are not deleted eagerly (would manufacture compaction debt).
- Capture plus streaming-session acquisition is atomic under the per-index apply mutex; an active capture without a streaming-session lease is `PostingWalCaptureOwnershipConflict`, never a silent join. Derived catch-up may borrow an active source capture but cannot finish/cancel it.
- Bounds are separate: overlays collapse every 32 MiB; background flatten/checkpoint at 128 MiB; only a 256 MiB ceiling can force replay to join a builder. A max-chain full merge gets the 256 MiB hard budget.
- Bounded posting delta chain instead of repeated full checkpoints (one mmap base plus at most six deltas, 184-byte `CURRENT`); a later fix let managed posting policy use the format's eight-delta limit instead of flattening after two (1M: five full posting rewrites -> two).
- Vector-block manifest: an empty bootstrap chain checkpoints at 24 generations while an established base keeps the eight-generation online limit; the format admits 64 so tightening never makes a valid `CURRENT` unreadable.
- On commit failure under authority, propagate the error and let the capture cancel restore state; inverse HBC deletes are only for pre-authority captures (they caused a ~568K rollback stall).
- Topology rebuild identity is durable (vector base generation, latest vector-WAL mutation sequence, algorithm) so reopen/coverage-only advances do not rebuild again; recursive topology stays opt-in.
- Topology maintenance returns `not_needed`/`completed`/`deferred`; only deferred retains the candidate.
- Full checkpoints stream directly to a durable atomic sink (AFPS outer writer + nested AFQD streaming writer) instead of buffering the whole segment (~1.88 GB double allocation at 1M).
- Cold atomic sink splits appends at 64 MiB writeback boundaries for immutable generations and LSM flush/compaction output; flattening reads one quantized-directory entry at a time through a separate uncached maintenance descriptor rather than the serving mmap.
- Native backup format V4 identifies authority as `hbc-native-v1`, hardlinks immutable segments/vector blocks into a pin generation and copies WALs only through their committed boundary. Shared exact-vector blocks are rebuildable acceleration, separating `queryable` from `native_accelerated`.

### Query path
- Exact rerank uses current-tip point probes; sorted point batches overlap up to four independent run-block reads (later: up to 16 per batch, dividing a 64-read backend-wide target across active sparse batches, falling back to scalar below a share of two).
- Resource-manager cache reclaim callbacks are opportunistic and never wait for an owner lock (HBC and LSM both try-lock) — symmetric deadlock found at five concurrent queries.
- HBC shared cache: compact CLOCK slot array and victim search bounded to 64 entries (warmed 50K p99 440.5 ms -> 12.8 ms).
- Public search retains source blocks under an explicit process memory envelope rather than transient reads; qualifications run with an explicit envelope.
- Routing policy is adaptive: indexes estimated at >= 1,024 postings use the exact persisted flat centroid directory (versioned block-columnar entry in the native segment); smaller indexes keep tree routing. Benchmarks must keep the production `auto` contract.
- Shared table-level, versioned exact-vector block store (one copy, not per index); columnar layout (keys before an aligned vector arena); ordered `(shard, full_hash, key)`; float16 query projection with per-vector float32 scale (`AFVBLK` v3) is the native default, authoritative storage stays float32.
- Exact-vector publication is part of readiness: readiness verifies source sequence, encoding, shard count, and cardinality under one lease; opportunistic publication admits a bounded stable primary tail (no WAL/pressure block, no hard L0 debt, no active compaction, <= 32 immutable memtables and 256 MiB).
- `DB.runUntilIdle` drains every bounded posting page and publishes the vector generation; the adapter requires authoritative readiness, replay watermarks, zero dense debt and zero dirty postings. Query-only reuse phases are read-only (no write sentinel).
- AArch64 RaBitQ popcount reduces 128-bit byte popcounts (`CNT` + `UADDLV`); float16 exact payloads are scored directly in one SIMD pass.
- Posting-local layout: V3 quantized directory places a float16 candidate matrix beside each leaf's membership/RaBitQ rows; V4 adds the source projection checksum; V5 adds a 36-byte generation-bound residual locator (never authority by itself; validated against the leased generation). Two-stage scoring: RaBitQ admits candidates, float16 completes them, lossless residual only for intervals that can cross the top-k boundary.
- Correctness rule: RaBitQ intervals select the complete refinement shell; RaBitQ's stochastic interval is never a certificate to skip unread float16 candidates.
- Skip the adapter-level decoded-vector cache when a sequence-matched native projection generation is leased; keep one-shot rerank metadata in the search transaction.
- Approximate requests retain only `search_width + bounded filter slack` posting probes (an all-probe proof experiment leaked HTTP 500 `ResourceBudgetExceeded`).

### Primary geometric merge (accepted policy)
- Sorted bulk publication at 8 MiB (later 4 MiB direct floor), durable logical L0 sequence per publication (manifest v10, reads v9), and a universal geometric carry: a contiguous generation window is eligible only when output is >= 4x every input generation; hard run-count pressure tries a geometric carry before generic promotion, byte-hard pressure stays leveled.
- Anchor-preserving prefix seal: once total L0 reaches half the lower base, seal a newer prefix only when it is >= 2x its largest component; the anchor is never part of the seal. Do not weaken the 2x bound to force a prettier manifest.
- Nested mutable/immutable bounds: rotate the mutable epoch at 128 MiB while streaming two epochs into one 256 MiB immutable publication; production resident mutable cap stays 256 MiB. Ordinary size rotations accumulate to the window; WAL checkpoint floor has a two-segment lower bound.

## Rejected approaches
- Lock-held current-tip intent cursor — cut bound-read copies to 29.8 MB but serialized writers (69.95 s ready at 50K).
- Namespace probe propagation as the clone fix — clone count unchanged (725 clones / 5.63 GB); kept as a general fix only.
- 1 MiB WAL checkpoint floor — 41.75 s vs 38.35 s, 147 vs 59 final L0 runs, same ~591 MB footprint peak.
- 128 MiB preferred primary run files — finer layout but did not split overlap closures; 1M took 1,753.77 s vs 1,617.08 s, max job still 2.562 GB.
- 768 MiB foreground compaction budget as default — 1M 1,854.21 s, 27.08 GB rewritten, late indivisible closure still 2.753 GB / 58.5 s.
- 4,096-item replay windows (92.41 s at 50K, 27 finalizations) and internal 4,096-item HBC applies (46.45 s, ~10% slower, partial rollback complexity).
- Full posting snapshots per WAL batch — tail four times the 2.10 MB checkpoint; replaced by delta encoding.
- Global quantized rebuild on replacement — +104.8 ms and recall 0.628 vs 0.710.
- Dual-write posting sidecar (posting WAL plus LSM readback) — 1M 2,172.78 s ready, slower than the 1,603 s LSM control.
- Half-segment / fixed 32 MiB synchronous checkpoint policies — repeated full rewrites on replay's critical path at 1M.
- RaBit-only structural split reconstruction — 38.62 s 50K load but recall 0.6325.
- Lowering epsilon/effort as a default; reducing the candidate window from 9x to 8x `k`; metric-ball pruning with exact centroid radii; angular covering radii for cosine (all 208 leaves still explored, zero stops); certified radius stopping (zero certified stops in r101–r104 and r107); adaptive flat stop on heap-full (a correctness bug).
- Flat RaBitQ centroid directory (0.95626 recall at 84.27 ms mean; later 0.97423 and 0.9577) and a four-representative `flat_rabitq` directory (recall 0.02151).
- Recursive binary / global k-means / Hilbert-seeded / recursive HBC topology as defaults — no better leaf-recall curve at fixed work, or worse concurrency (recursive at effort 0.44: 53.25 vs 113.17 QPS at C30). Global k-means is a quality oracle only (needs >3 GB at 1M). Bounded hierarchical k-means (r113) did not match the oracle.
- Unconditional transient primary reads for search — cold mean 53.6 -> 200.3 ms.
- Fixed point-read pipeline of eight (a request exceeded 120 s) and fixed sixteen (C30 p95/p99 630/1341 ms).
- Promising-candidate-first progressive rerank — 50K recall 0.98396 -> 0.95400. Progressive float16 refinement in 64-candidate chunks — recall 0.9504.
- 50% base-growth promotion (1M 788.81 s vs 759.03 s); 2x full-L0 seal including the anchor (never fired); recursive geometric stack (no legal seal); soft-maintenance L0 seal (worse artifact locality: 36.19 -> 46.89 ms mean); raising WAL floor and immutable window both to 256 MiB (8.57 GB cloned); 768 MiB resident mutable cap (858.29 s, 3.17 GB RSS).
- 256 vector shards (cold p50 28.1 ms vs 19.0 ms with 128 shards).
- r64 releasing validation-touched pages after admission (insert +61.74 s, higher RSS).
- Projection read coalescing, direct mmap views, `MADV_WILLNEED` (RSS up to 1.43 GB), process-wide divided 128-read target (C30 4.16 QPS), payload `MADV_DONTNEED` (r83, latency regressed), bounded 32 MiB cross-query float16 projection cache (slower, +96 MB RSS), exact-residual mmap (1M RSS 2.44 -> 3.40 GB, concurrency regressed), advisory mmap reclamation as the 1M memory bound (r123 still 5.841 GB).
- Raising the ordinary maintenance page from 64 to 256 (r82) and retrying finalization from the vector-maintenance lane (r85, 308.2775 s optimize wait).

## Key measurements

### Load lifecycle milestones (public API, batch 100)

| Run / change | 50K ready | 1M ready | Notes |
| --- | ---: | ---: | --- |
| Original public path | 48.42 s | projected ~45–50 min | default full text present |
| Relocated compaction publication | 27.53 s | 1,110.66 s | 103.7 MB clones at 50K; 1M 111 L0 / zero debt |
| Replay-lane snapshot, 3-run mean (50K) / instrumented (1M) | 41.65 s (38.72–43.14) | 1,617.08 s | 1M largest compaction 2.57 GB / 52.6 s |
| Foreground budget wired, 2 GiB default | 41.63 s | 1,603.35 s | 1M 29.31 GB compaction input |
| WAL-authoritative posting store | 40.80 s | 1,774.12 s | remaining cost in source LSM |
| Whole-tree native HBC, corrected | 42.16 s | 1,711.26 s | no HBC LSM remains |
| Bounded delta chain, cost-aware full merge | — | 1,392.62 s | publication 241.59 s / 224 flushes |
| Post-resource-governor (PR-540 follow-ups) | 31.93 s | 1,502.04 s | catch-up 2.05 s at 1M |
| Geometric carry + 4 MiB floor + hard-pressure assist | 21.74 s | 759.03 s | compaction input 92.74 -> 14.55 GB |
| Nested 128/256 MiB mutable/immutable bounds | 22.43 s | 623.16 s | 167 manifests vs 598 |
| Persisted centroid directory | — | 634.64 s | 274.54 QPS peak, 0.9900 recall |
| 2026-08-26 end state (insert + 128-shard build) | 20.05 s | 758.57 s (component sum) | 0.9903 1M recall |
| Proactive tiering, exact-vector publication in readiness | 26.75 s | 717.10 s | 1M 0.9902 recall / 0.9918 NDCG |
| r56 first-load transaction | 26.0385 s (r55) | 620.2469 s | vs r44 925.2819 s |
| r59 64-run bootstrap / r60 24-run policy | — | 528.3834 s / 560.7766 s | r60 kept as policy |
| r69 float16 direct scoring | 28.3389 s (r68) | 571.5951 s | 1M recall 0.9897 |
| r107 posting-local 768D 1M | — | 877.187 s | 5.479 GB RSS; concurrency numbers invalid |
| r117 streaming checkpoint | 35.05 s (r116) | 781.01 s | r117 peak RSS 6.059 GB from primary compaction |
| r124 / r125 durable cold generation I/O | 31.34 s | 775.94 s | 1M RSS peak 3.860 GB vs r123 5.841 GB |
| r126 native authority transactions | 30.09 s | — | 0.9861 recall |

### Clone and memory control points
- Primary-only 5,000-row control: 208 clones / 1,096,456,770 bytes -> 15 calls / 27,613,796 bytes (97.5% less) after the adapter fix.
- Original partial 1M run reached ~105K docs with ~23.6 GB cumulative clone bytes. Primary mutable snapshot copies at 1M were 3.707 GB in the 1,078.48 s baseline and 2.413 GB with proactive tiering (717.10 s).
- r107: primary LSM pressure compaction near 644K raised RSS above 3.5 GB; peak 5.479 GB RSS / 4.800 GB `phys_footprint` despite a 2 GiB envelope; data root 7.7 GB.
- r117: primary document LSM compacted ~1.88 GB at ~609K rows, RSS 2.1 -> 6.06 GB.
- r125: final 1.877 GB posting publication did not raise RSS; cold restart RSS 2.400 GB with 302.3 MB footprint ledger.

### Query quality and latency

| Point | Recall | Latency / QPS |
| --- | ---: | --- |
| 1M effort sweep (r108, flat): 0.30 / 0.40 / 0.41 / 0.50 | 0.83072 / 0.94890 / 0.95642 / 0.99350 | client p50 5.73 / 9.76 / 10.57 / 25.56 ms; 168 / 588 / 666 / 2,048 leaves |
| 50K global k-means oracle (r109), effort 0.47 | 0.97667 | 164 leaves, 20,598.7 vectors, p95 6.01 ms |
| 1M float32 -> float16 projection | 0.99061 -> 0.99047 | server p50/p95/p99 27.41/75.94/108.72 -> 18.30/43.97/71.94 ms; base 3.162 -> 1.626 GB |
| 50K float32 -> float16 | 0.98514 -> 0.98482 | server p50/p95/p99 15.69/29.81/53.68 -> 9.37/12.61/13.97 ms |
| 1M four-read pipeline (same generation) | 0.98535 | client mean 85.71 -> 54.21 ms |
| 1M flat directory vs tree at 0.47 effort | 0.98495 | public p50 13.40 ms; C1–C30 QPS 60.83/215.15/219.67/199.38/183.97 -> 81.04/322.14/332.81/320.96/291.32 |
| 50K V4 two-stage (r108) | 0.98622 | profile 7.23/5.72/8.48 ms; QPS 174.59/331.47/256.30/239.01 |
| 50K cache-free (r113) | 0.98622 | QPS 251.73/947.44/1031.64/1012.31 at C1/10/20/30 |
| 1M cache-free, effort 0.41 (r113) | 0.95545 | QPS 166.64/888.40/977.06/913.65; p50 5.72 ms |
| 50K V5 locator (r114) | 0.98673 | client mean 6.199 -> 5.389 ms; segment +34.85 bytes/vector |
| 1M r125 reopened | 0.9925 | QPS 46.55/478.64/591.84/561.32; 239,507 approx / 146.60 exact per query |
| 50K r126 | 0.9861 | QPS 266.07/1031.50/1116.04/1064.82; client p50 4.49 ms |

### Competitor targets (Circus 1M snapshot, as stated in the log)
At recall >= 0.955: beat pgvector's 731.8 QPS, keep p95 below the low-teens Chroma/Weaviate range, reduce demand toward Elasticsearch's 2.06 GB and disk toward its 3.15 GB. Elasticsearch and Milvus publish 3,104.7 and 3,813.0 QPS (stretch). Chroma 768d/1M: 238.7 QPS / 6.6 ms p95. After r113, 1M throughput was "near pgvector and well above Chroma", behind Elastic, Milvus, and Weaviate.

## Methodology notes
- Continuous macOS `vmmap --summary` sampling caused ~450–520 ms stalls every 75 queries; the runner samples once after each timed phase plus a 200 ms `ps` RSS timeline.
- Report cache-inclusive RSS and attributable demand separately; report first-cold and warmed query phases separately.
- HTTP 429 above the 32-request admission envelope makes nominal VectorDBBench QPS invalid; so do request errors the framework does not fail on (r107).
- The batch-100 public run is an online incremental build; a recursive backfill/import result must be reported separately.
- A stale VectorDBBench checkout POSTing the removed `/api/v1/tables/{name}` got HTTP 405; the runner accepts an explicit API-compatible checkout. Diagnostic profiles are not publication-equivalent.

## Open items stated in the log
- Exact source embeddings remain duplicated between the primary LSM and the shared vector-block projection (~3.07 GB float32 at 1M); one shared table-level authority or reconstructible encoding is needed before removing primary artifact rows.
- The posting-local float16 plane duplicates the central float16 generation (r100/r106/r107); the end state needs one manifest owning posting-local projections plus central residual-only blocks.
- 1M end-to-end 2 GiB envelope not met (r117/r125 peaks from primary-LSM compaction); requires bounded primary compaction buffers and cold-page eviction.
- Better partition geometry (sampled hierarchical clustering with streamed assignment) is needed to shrink the 1M candidate shell at recall parity; topology acceleration should be explicit non-blocking maintenance debt.
- Resource-derived query capacity should participate in public admission so an index-sized workspace cannot leak an internal 500.
- Controlled-host repetition is still required for several throughput and RSS claims (e.g., r114, r125 vs r123, r126 RSS).
