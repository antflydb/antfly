# Full-Text Performance: Implementation Progress Log (2026-07)

> Summary of a verbatim log. Full text: [docs/design/full-text/history/implementation-progress-2026-07.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/full-text/history/implementation-progress-2026-07.md). Originally relocated verbatim from `zig/FULL_TEXT_PERFORMANCE.md` (lines 616–2925 at commit 271838a195) on 2026-09-16; the living design is [`FULL_TEXT.md`](../../../../zig/FULL_TEXT.md).

This is a dated log, 2026-07-12 through 2026-07-16, of Antfly's full-text kernel and product-server qualification against Tantivy 0.25 (embedded kernel) and an archived Quickwit 0.8.2 server run. It covers the strict V1 benchmark protocol and runners, the inverted-index format sequence from v23 (shipped on origin/main) to the accepted v38, query-path CPU work, and a long series of product-ingestion memory and lifetime defects found by full-corpus loads. The outcome: the v38 kernel artifact is 3,385,816,859 bytes against Tantivy's 3,251,494,143 and faster on boolean queries. Term and phrase CPU are about 1.19x and 1.22x, with no platform-specific SIMD. The 5.03M-document product server beat Quickwit on latency, CPU, freshness, and restart time but lost on RSS.

All kernel numbers use the canonical corpus: 5,032,105 documents, 8,473,678,432 bytes, SHA-256 `9078d9ea4783ab764fe246046efdc8062692f07afdd7c8749a74ebfe05c51ff4`. The log calls them engineering measurements from a development machine, not public product claims. The checked-in evidence files are under `bench/full_text/results/`.

## Decisions

### Benchmark protocol (2026-07-12)
- Strict `V1` `TERM`/`UNION`/`INTERSECTION`/`PHRASE` grammar. Legacy syntax is allowed only behind `--allow-legacy-query-syntax`. `VERIFY_TOP_N`/`VERIFY_TOP_N_COUNT` return stable zero-based corpus ordinals and honest total-hit relations. `tools/verify_search_benchmark.py` fails closed on any mismatch except documented cutoff-tie substitutions.
- Two segment modes, `single` (force-compact, fail unless exactly one segment) and `production`. The production-backed kernel API uses normal lowering, snapshots, postings, deletion masks, scorers, and collectors, but bypasses MVCC, projection, and stored bodies.
- The analyzer label is `ascii_lowercase`, because the production `lowercase` filter lowercases ASCII only (`CAFÉ` becomes `cafÉ`). `ANALYZE` returns exact terms, positions, and offsets so no comparator runs Unicode lowercasing under the same label.
- Phrase scoring matches Tantivy: sum constituent-term IDFs (repeats included), use the exact phrase occurrence count as BM25 frequency, apply the field norm once.
- Explicit BM25 `k1`/`b` everywhere. Exact counts come from per-segment live-bitmap cardinality on a separate path and are never inferred from pruned execution.
- The kernel indexer uses an internal index-only ingestion boundary: production analyzer, segment sink, format, publisher, merge planner, and query path, with stored source `{}`. Long loads settle merges every 100,000 documents. Reason: the first full-corpus attempt routed every article through the primary LSM, stored the source twice, and after 2 h 35 min had 1,926 segments and about 23 GB with no manifest. That run is invalid.
- Norms use Tantivy-compatible quantized field-norm IDs (v25) with the same 256-value small-float mapping. Reason: exact `u32` lengths gave long documents different BM25 scores.
- Acceptance uses interleaved matched A/B runs on the preserved full corpus, with strict IDs, scores, counts, and work counters. Archived Tantivy and Quickwit artifacts are reused, not recollected.

### Query execution
- WAND pivot bounds use each term's global theoretical maximum, not the current block maximum, so a later high-impact block can't be pruned. A regression test covers this.
- Segment-bound ordering is reserved for snapshots with more than 16 segments. Healthy tiered layouts rely on in-segment WAND. Reason: the duplicate prepass added about 50 µs to union.
- The production merge tier stays at 10. Reason: tiers of 16 and 24 gave no consistent query win on 60K/120K documents and only a small synthetic indexing gain.
- Postings chunk size stays at 1,024 documents. Reason: 2,048 cut 120K size from 12,723,381 to 11,247,242 bytes but regressed term/union/intersection from 35.7/87.8/67.6 µs to 61.1/169.3/123.7 µs.
- Dedicated single-term Block-Max loop. Term-dictionary lookup was only about 1.3% of active samples, so the generic WAND control was the real cost.
- Deferred positional verification makes phrase a true two-phase executor: positions are unpacked only after the document-level conjunction succeeds.
- `advanceTo` scans decoded document IDs to the target and decodes only the selected scoring hit. WAND uses a position-free `nextScoring`.
- `BM25TermScorer` precomputes `idf * (k1 + 1)`, `k1 * (1 - b)`, `k1 * b / avg_field_length`. A per-snapshot TF-ceiling table (32 frequency IDs × 256 norm IDs) is keyed by the exact bits of average length, `k1`, and `b`, is thread-safe, and is capped at four tables (128 KiB) per snapshot, with a scalar fallback beyond that. Values get an eight-ulp upward bias so bounds stay conservative. All 8,192 pairs were checked across six IDFs.
- Exact two-term phrases intersect bounded packed-bit position views with cumulative-delta cursors. N-term phrases, slop, v23 data, and noncontiguous layouts keep the materialized verifier.
- Portable vertical BP128 with `@Vector(4, u32)` and a fused four-lane inclusive-scan delta decode. Architecture-specific intrinsics, inline assembly, and target branches are out of the default plan. They are allowed only as a separately measured fallback that keeps the scalar reference tests and the portable on-disk format.

### Format and compatibility
- The production reader accepts only origin/main v23 and the current writer format, v38 at the end. Branch-only v24–v37 are rejected rather than carried. (Also stated in the design doc's Format evolution section.)
- A format candidate must report persisted bytes, bytes decoded per query class, CPU/query, query RSS, indexing peak RSS, and merge I/O. It is rejected if it trades the disk gap for selective-query CPU or memory.
- Accepted layout features, in order: v26 three-byte block-max (`u16` max freq + `u8` min norm ID; the unused max norm dropped); v27 positions framed per chunk with no per-document position count; v29 128-posting payload blocks with 1,024-document impact ranges, group-of-eight shared position width, and an omitted-stored-fields kernel mode; v30 contiguous group bitstream plus streamed 1 MiB merge writes; v31 inline single-document postings that keep positions; v32 two-column chunk metadata; v33 constant-frequency blocks; v34 five-bit conservative max-frequency buckets; v35 vertical BP128; v38 headers that omit every derivable field.

### Product ingestion, memory ownership, and admission
- LSM generation lifetime uses exact per-generation pins for persisted runs, flushed immutable memtables, and mutable read snapshots, never a backend-wide reader count. Probe transactions copy results into transaction-owned storage. Reason: three separate "retain while *any* reader exists" bugs let continuous overlapping scans retain obsolete generations without bound (see Defects).
- Ownership boundary: `ResourceManager` owns global accounting, budgets, projected admission, cross-backend fairness, pressure actions, and a wakeable epoch. The backend publishes actual retained bytes and owns pins and reclamation. The layer that owns a higher-level critical section owns any wait. Soft pressure inside the DB apply lock does local reclamation and then admits one bounded write. Hard pressure is a non-blocking `ResourceBudgetExceeded` before WAL append. Waiting happens above the apply lock through `ResourceManager.awaitAdmission`, and the backend re-checks before WAL append.
- Public writes estimate cost as two copies of payload plus 512 bytes per operation and wait on `lsm.in_memory_state` before taking the apply lock. A request larger than the hard slice is rejected immediately. Budget rejection maps to HTTP 429 `Backpressured`.
- Adaptive LSM state is capped at 768 MiB with a 576 MiB soft limit on large hosts (raw default 512 MiB soft / 768 MiB hard). Per-backend immutable byte caps are primary 256 MiB, dense 512 MiB, text/sparse/graph 128 MiB.
- Full-text segment residency is a separate `full_text.segment_residency` slice, 512 MiB soft / 768 MiB hard by default, with provisioned hard limit at 1/8 of memory clamped to 256 MiB–2 GiB and a 75% soft watermark. `ResourceManager` returns `shrink_cache`, and the writer evicts the coldest clean mapping with `madvise(DONTNEED)`, sparing segments touched in the last 30 s (5 s under hard pressure) and never touching active readers.
- The shared primary-index LSM cache is 1/16 of node or cgroup memory, clamped to 64–512 MiB, with soft = 75%. All production read, write, and startup owners use it.
- Compaction scores levels by normalized `current / target` pressure, so L1 bloat no longer starves L1→L2 promotion. Level targets derive from live data scale, and primary runs are cut at namespace and first-key-byte boundaries (table encodings unchanged).
- The schema is persisted before indexes load (`schema_before_index_load`). Provisioned `PUT /schema` enqueues structural reconciliation and returns. Table status serves the immutable runtime-status snapshot and never takes a table-read lease. Schema progress reads the durable `rebuild.state` marker before any DB open.
- Queries hold a ref-counted snapshot for their whole operation. Merge task creation takes the per-index apply mutex before cloning deletion bitmaps and works only on the task-owned clones.
- On macOS, report RSS, physical footprint, malloc live bytes, mmap logical bytes, and `ResourceManager` residency together. Physical footprint is the acceptance metric for anonymous pressure, because clean file-backed pages inflate RSS.

## Rejected approaches

| Experiment | Result |
| --- | --- |
| Bounded-checkpoint prototype (v24) | Repeated delta decoding regressed skewed 1M/k=10 to about 11.8 ms. |
| Postings chunks of 2,048/4,096/8,192 | Smaller (11,247,242 / 10,526,591 / 10,145,646 bytes at 120K) but non-phrase latency degraded sharply. |
| Continuous 128-value position blocks | −5.3% index and −29% positions at 120K, but phrase 11% slower. Its `u64` bit-reservoir decoder was kept. |
| v28 128-posting blocks as the only granularity | Full corpus 4,088,444,648 bytes (−19.29%), but term/union/intersection CPU regressed 2.61x/3.43x/1.91x. `alpha` skipped 8 blocks vs 4,141. Lesson: separate compression granularity from pruning granularity. |
| Impact bounds aligned to payload blocks (every term / through 1,024 postings) | 3,730,205,166 bytes but CPU 1.98x/2.45x/1.62x worse. 3,813,870,561 bytes still regressed term/union 26%/24% and returned peak RSS to 4.47 GB. |
| Independent or adaptive per-document position widths | Grew the tuning index and slowed phrases, or saved no bytes for about 8% more phrase CPU. |
| Binary-search/prefix position seek (v30) | Short phrase jumps got worse. |
| One global bound for low-DF ranges (v34 alt) | 3,279,495,439 bytes (28.0 MB above Tantivy), but term/union/intersection CPU went to 216.3/742.9/430.1 µs. |
| Shared constant-frequency column; four-bit frequency buckets | +24,744 bytes; −10.2 MB but 8–12% more ranking CPU. |
| Machine-word refill for the packed reservoir | Term flat, phrase +4.3%. |
| Random-access frequency extraction; cached single-term ceiling; binary search of the max-doc column; scalar fused delta+prefix | +5.0/3.9/4.0% CPU; no win; no win; +7.0/8.2/3.8% for term/union/intersection. |
| Retained parsed metadata layout | 74.2/257.9/220.0/619.6 µs, worse. |
| v36: one impact bound per 128-posting block | 3,254,674,194 bytes (3.18 MB above Tantivy), but `alpha` pruned 6 blocks and scored 15,459 candidates. Term/union/intersection 128.7/624.5/313.2 µs. |
| v37: per-term palette for (freq ceiling, min norm) pairs | Only 278,776 bytes below v35, and term CPU rose from 64.4 to 85.8 µs. Pairs are too high-entropy. |
| Per-term range packing of impact columns | Projected only 18,296,976 bytes (0.54%) saved. Not worth another hot-path branch. |
| Phrase: monotonic merge over decoded arrays; hybrid lower-bound search; group-jump cursor | Flat (0.08%); +15.76%; +2.94%. |
| Darwin `malloc_zone_pressure_relief` after cache eviction | 22 calls released 0 bytes. RSS looked lower, but footprint was worse (281,250,864 / 995,935,984 vs 197,365,144 / 651,167,424 bytes). Removed: no platform tuning to improve one RSS sample. |
| `madvise` eviction for native LSM runs | Not applicable: LSM runs are range-read, not mmap-backed. |
| Whole-segment eviction on every query | Not accepted without page-in, CPU, and tail-latency evidence. |
| `full_text_index_v2` with the `standard` analyzer | 2,602,844,009 bytes looked like a size win, but stop words and stemming did less semantic work (`alpha` 15,818 vs 15,753). Not a valid comparison. |

## Key measurements

### Kernel size versus Tantivy 0.25 production (3,251,494,143 bytes)

| Format | Antfly bytes | Ratio | Notes |
| --- | ---: | ---: | --- |
| v25 | 5.951 GB | ~1.83x | First valid full production run: 611.9 s, 10 segments, 267 merges |
| v27 | 5,065,897,930 | 1.558x | −885,145,421 vs v25. Gap reconciled to within 6,448 bytes, 64.6% of it postings/index structures |
| v29 | 3,905,870,509 | 1.201x | Build 483.9 s wall; peak RSS 3,922,395,136 (Tantivy 2,810,429,440) |
| v30 | 3,614,059,644 | 1.112x | Positions now smaller than Tantivy; build 443.0 s, peak RSS 2,118,975,488 |
| v31 | 3,459,488,164 | 1.064x | 7,957,061 single-document lists inlined; 429.8 s |
| v32 | 3,440,527,184 | 1.058x | 423.0 s |
| v33 | 3,435,521,272 | 1.0566x | |
| v34 | 3,406,622,680 | 1.0477x | 421.9 s, 23.3% faster than Tantivy |
| v38 | 3,385,816,859 | 1.0413x | 23,552,230 below v35; 421.345 s wall / 407.516 s CPU; peak RSS 1.925 GB |

The v3 product generation (simple analyzer) had 10 segments and 3,599,478,976 bytes. Its index-only normalization is 3,383,059,033 bytes, 2.76 MB below v38 and 131,564,890 bytes (4.05%) above Tantivy. That puts the honest remaining kernel-format gap at about 132 MB, not the former 1.3–1.5 GB. Public reporting must show both the 3.599 GB product footprint and the 3.383 GB index-only figure. Quickwit's split inventory (about 3.138 GB) disables source storage.

### Kernel query latency and CPU

| Point | Term | Union | Intersection | Phrase |
| --- | ---: | ---: | ---: | ---: |
| v25 median latency, Antfly / Tantivy (µs) | 90.4 / 50.0 | 285.8 / 268.3 | 313.5 / 287.5 | 1,244.0 / 490.8 |
| v27 CPU/query, Antfly / Tantivy (µs) | 71.2 / 38.6 | 198.2 / 229.7 | 213.6 / 249.1 | 1,084.9 / 344.0 |
| After direct scoring advance, CPU ratio vs Tantivy | 1.82x | 1.18x | 0.84x | 1.58x |
| After fused BP128 delta scan, CPU (µs) | 47.760 | 190.900 | 161.532 | 400.633 |
| Final v38 direct qualification: latency ratio | 1.121 | 0.793 | 0.662 | 1.070 |
| Final v38 direct qualification: CPU ratio | 1.188 | 0.796 | 0.649 | 1.216 |

In the final run, query-process peak RSS was 103.7 MB for Antfly and 21.0 MB for Tantivy, with 2,468 shared warmup queries and five shuffled repetitions. Antfly used 10 segments and Tantivy 20. The v25 numbers already showed the former roughly 35x gap was gone. Evidence for the final run is under `bench/full_text/results/full-corpus-v38-kernel-production/`.

### Product server (5,032,105 documents, v3 generation)
- Mixed read/write at the former crash point (8 clients / 200 RPS): 6,000 requests, zero errors, p50/p95/p99 1.82/2.15/2.70 ms, 31.9% CPU.
- 1,200 RPS: Antfly 1,199.7 RPS at 0.81/3.05/20.74 ms and 152.8% CPU. Quickwit (archived) 1,198.5 RPS at 9.90/53.79/92.33 ms and 900.1% CPU. At 25 RPS: Antfly 4.5% CPU, 5.88/6.13/6.49 ms. Quickwit 25.4%, 13.92/18.53/20.67 ms.
- Searchable freshness: 68.0 ms median (36.9–89.2 ms) vs Quickwit's 60.2 s. Graceful/crash restart to first search: 0.99/0.48 s vs 2.33/1.31 s.
- Memory was the remaining loss. Peak RSS was 2.22–2.25 GiB vs Quickwit's 305–598 MiB, with physical footprint 602–622 MiB and a 972 MiB lifetime peak. After the shared primary cache fix, a 64-client / 1,200 RPS read-only gate gave 0.718/0.837/1.066 ms, 47.9% CPU, and a 651,167,424-byte lifetime footprint peak. The first-query logical-heap increase fell from about 848 MB to about 35 MB.
- The data directory is about 10.14 GB (6.54 GB primary LSM plus 3.60 GB full-text). Different product boundaries from Quickwit, so the log reports component inventories, not one equivalent size.

### Ingestion gates (accepted)
- 650K: 409.261 s, physical footprint peak 1.486 GB (was 3.107 GB). Accepted the generation-lifetime and two-level admission fix.
- 750K: 124.784 s at 6,010 docs/s, 5.21x faster than the earlier accepted 650.189 s gate, footprint peak 660,637,664 bytes, graceful and crash restart exact.
- 2.2M: 357.528 s at 6,153 docs/s, zero backpressure, footprint peak 1,437,079,384 bytes, exact counts after graceful and crash restart (recovery footprint peak 2,632,374,032 bytes).
- Schema-availability gate (`tools/verify_antfly_schema_availability.py`): v2→v3 completed with zero health, status, or query failures. Table status peaked at 0.93 ms. Schema PUT and the first cold query took 3.78 s and 3.80 s.

## Defects found by full-corpus loads
- **Primary generation leak:** retired compaction inputs were protected by one backend-wide `active_readers` counter. A stopped 3,605,808-document run had 22,573,154,374 bytes in 760 run files, of which only 3,907,080,998 were live, and 2,645,367,909 bytes of WAL. The fix was per-run snapshot pins. WAL got a bounded fallback of one checkpoint flush per 250 ms beyond the 512 MiB soft limit (hard limit 2 GiB).
- **Compaction planner:** absolute L0 debt starved L1→L2 while L1 reached roughly 10x its target. Fixed with normalized pressure scoring.
- **Compaction cursor memory:** every input run kept a full point-query `TableIndex`, about 6.4 GB in total. Fixed with a sequential index.
- **Merge dictionary:** built in a single pass over 48-term blocks.
- **Durable-job reaper:** limited to 640 jobs/s with `orderedRemove`. Fixed with up to 4,096 unordered removals per pass, and the payload is now destroyed right after `job.run`.
- **Health metrics:** walked every posting on each refresh. Now use cached basic layout. Detailed stats sample 1 per 64 batches.
- **Write admission:** ordinary `.write` bypassed the derived-backlog gate (128 MiB soft / 192 MiB hard).
- **`ActiveMemTable`:** a per-key `ArrayList` hash index and arena slack went unaccounted. Fixed with inline entries and actual retained bytes, with logical bytes maintained incrementally.
- **Retired immutable memtables:** kept while any reader existed. Fixed with exact pins and owned probe results.
- **Lock inversion:** soft-pressure sleep inside the DB apply lock. Fixed with two-level admission.
- **Mutable read snapshots:** retained by unrelated probes (stalled at 800,843,922 of 805,306,368 bytes). Fixed with exact pins.
- **Replay delete:** `deleteById` scanned every document per ID. Fixed with one hash set and one pass per segment. Oversized durable records are subchunked, and the sequence advances only after all slices finish.
- **`StreamingEncoder.workingSetBytes()`:** quadratic. Fixed with an O(1) subtotal.
- **Product schema/backfill:** dynamic `body.exact`/`_all` indexing, backfill flushing every 1,024 documents (2,855 files), `DB.stats()` scanning the 6.65 GB primary LSM, status blocking on read admission, and the schema applied after `DB.open`. LSM table v9/v10 dropped unused exact-key hash slots, sized prefix Blooms by distinct prefixes, and switched to `u16` block-local offsets (−60,399,646 bytes). The doc-values merge was quadratic.
- **Concurrency:** `IndexSnapshot.termDocFreq` use-after-free (SIGSEGV at 8 clients/200 RPS), fixed with ref-counted snapshots. A Roaring bitmap clone race was fixed by taking the per-index mutex.
- **Primary cache:** `MergeCursor` decoded a private `TableIndex` outside the shared cache (about 276.8 MB of run Blooms and 143.1 MB of prefix Blooms on the first query).

## Timeline
- **2026-07-12:** protocol, runners, and the Tantivy comparator. WAND bound fix and the v24–v27 formats. First valid full-corpus production and single-segment baselines.
- **2026-07-15:** the first complete 5.03M product load was correct (15,818 `alpha` hits) but not a valid performance result, because of schema equivalence, backfill, and status-scan costs.
- **2026-07-16:** analyzer-equivalence correction (simple v3 generation, with `alpha` 15,753 and `alpha beta` 525 required to match exactly), asynchronous schema migration admission, the server matrix against Quickwit, and the final v38 direct kernel qualification.
- **At consolidation:** the eight-milestone implementation plan was declared complete (frozen benchmark spec, correctness-gated adapter, kernel API, reproducible runners, iterator and phrase executors, server benchmark compared against Quickwit).
