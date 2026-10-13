# VectorDBBench Findings: Bounded Publication, r129, and V2 Directory Experiments (2026-09-03 – 2026-09-05)

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/vectordbbench-2026-09-03-05.md@f3fd4d35](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/vectordbbench-2026-09-03-05.md). Originally relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 313–496, 4148–4190 and 4275–5420 at commit 3ce5aff4ea) on 2026-10-12; durable decisions were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md), and the "V1 mirror versus native-v2 authority" section moved to [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md#physical-index-versioning-and-rolling-upgrades).

PR 593 native HBC/vector-block work, qualified through the public API with official
VectorDBBench cases (one shard, cosine, k=100, batch 100, four load workers, float16
vector blocks, no full-text index, 30-second C1/10/20/30 phases). Over these three days
the branch recovered from a stable-tip flatten regression, bounded publication memory,
built and qualified a row-block V2 vector directory, added node-wide scan admission,
decoupled durable replay from request latency, and audited historical baselines. Several
qualifications passed correctness but failed the performance gate; the final 1M candidate
of the period (`verified-rows-1m`) did not reach ready. Paths under `/private/tmp/...` and
`.benchmark-results/` in the full text are untracked local evidence.

## Decisions

- **Stable-tip flattening allowed for managed V1 mirrors.** Readiness had requested a full
  posting checkpoint only when the native WAL was authoritative, coupling physical layout
  to mutation authority; `make_authoritative` stays a separate capability-fenced transition.
- **Coverage-only WAL records are not query-state debt.** The posting store durably
  distinguishes state-bearing from coverage-only records; repeating the current watermark
  is a no-op, and a newer coverage-only watermark cannot trigger a corpus rewrite (a 188-byte
  coverage record had triggered an identical generation-16 rewrite at 1M).
- **Non-HA standalone authorizes native v2 before exposing the listener.** Standalone used
  `LocalStandaloneMetadata`, so its native-authority gate never opened and it paid both
  compatibility-LSM and native WAL work (259.43 s of 1M dense finalization). Standalone HA
  stays closed until it has an all-peer capability fence.
- **Bounded publication by lifetime.** V1 directory writer streams values and keeps only its
  compact index; flatten reconstructs one patched value at a time outside the query patch
  cache; search scratch is a resource-manager-admitted pool capped at 32 entries per index;
  `sync=true` on an equal coverage watermark now fsyncs an unsynced non-empty WAL.
- **V2 vector directory adopted in row-block form** (unreleased format, no V1 tax): each block
  encodes the ordered union of vector IDs once, presence bitmaps for leaf/metadata planes,
  absolute ID restart every 16 rows, independent per-plane checksums and once-per-generation
  verification bits, adaptive 2/4-byte metadata cumulative ends, bounded sequential writes.
  Passes correctness/compactness/restart-memory gates, not the full replacement performance gate.
- **No per-machine scan cap; FIFO work-weighted node admission in `ResourceManager`.** Fixed caps
  of 24/20/16/12 established the bandwidth knee but would encode this machine and corpus. The
  queue charges estimated scan bytes, waits before acquiring MVCC/generation/scratch, preserves
  cancellation. Admission later became generation-coupled (re-estimate after binding the
  generation) and then route-first: route, sum authenticated per-posting costs from the AFQD
  compact index, then admit. Every published delta carries exact per-leaf scan costs; the
  global maximum is only a fail-safe.
- **Cold maintenance reads use private descriptors** (macOS `F_NOCACHE`, Linux `O_DIRECT` with
  `NOREUSE` fallback), never evicting a foreground mmap; coalesced reads run up to eight reader
  groups with at-most-1-MiB bounce extents (~8 MiB scratch).
- **Single immutable `SearchView`** per search; posting mutations publish append-only MVCC delta
  generations; public API returns the source request identity captured under the search lease.
- **Durable replay admission.** A `sync_level=write` request no longer runs HBC replay
  synchronously past a 200-sequence backlog (one 100-row request spent 34.691 s in
  `backlog_pressure`). Replay reserves exact encoded bytes before primary commit; exhaustion is
  retryable HTTP 429. Native readiness no longer requires a million-vector rewrite; quiescent
  posting publication is fully asynchronous and never joined under the apply fence.
- **`PostingWalTooLarge` is recoverable backpressure**; native authority records legacy LSM
  retirement as pending and drains it at the next safe boundary.
- **Recursive topology experiment removed from production maintenance**: its clustering
  interleaves corpus CPU with live-tree writes. A future optimizer must build an immutable
  `TopologyPlan` outside the mutation fence and publish via epoch/CURRENT compare-and-swap.
- **V16 runtime-status two-phase bootstrap**: initial registration uses the strongest
  already-activated profile; dense-native floor activates afterward.
- **Projection maintenance skips repair generations whose serving gate is blocked**; public
  readiness derives from each index's posting/vector generation token, not the table-wide
  `dense_projection_finalizing` flag.
- **Disk emergency headroom = `max(1 GiB, min(5% of capacity, 16 GiB))`** — a raw 5% reserve
  (~46 GiB on 926 GiB) permanently rejected a fully accounted 320 MiB shadow repair.
- **Checkpoint builders decode metadata from the retained committed generation** (fixes a
  mixed-generation hazard after WAL-capacity rejection); post-repair acceleration uses a
  catalog-lifetime pin, not an apply lease.
- **Initial posting acceleration is a one-time publication requirement for non-empty v2 indexes**
  — vector-file readiness had stood in for scan-plane materialization (see rejected run below).
- **Harness records binary SHA-256 and tracked-diff SHA-256** alongside HEAD; recorded HEAD
  alone does not identify the measured executable. Do not combine different runs' best cells
  into a fictional baseline.

## Rejected approaches

- **Kind-separated V2 directory** (separate leaf/metadata blocks, 29-byte entries removed) —
  encoded the sorted ID stream twice, decoded up to 255 varints per lookup, one sink append per
  leaf value; first run also recomputed CRCs on every lookup. Superseded by row-block V2.
- **Process-monotonic admission floor from header-authenticated costs** — a temporary root-only
  generation raised a floor that never fell: every query charged 214.70 MB, one query fit in the
  402.65-MB budget, queue reached 19, >572 s cumulative wait, C10 collapsed to 288.5 QPS.
- **Maximum-leaf × search-width admission estimate** — ~33.5 MB/request vs ~23 MB actually scanned;
  admitted at most 12 searches at 1M vs 17 with exact routing.
- **Vector-file readiness as initial posting readiness** — upload stayed in a 40-MB WAL/overlay over
  an empty base: 165.3/187.3/167.3/158.5 QPS, 17,019 admission waits. Raising the budget would
  have concealed the missing layout.
- **Increasing client timeouts / describing the 1M mixed write timeouts as admission failure** —
  explicitly rejected; root cause was synchronous replay under `backlog_pressure`.
- **Smaller replay windows and naive internal apply chunks** — both regressed 50K throughput.
- **RaBitQ routing as default** — 1.55-percentage-point recall loss is outside the parity budget.
- **Page-aligning the point-lookup directory, per-block leaf dictionaries, more metadata length
  compression** — not justified before measured distributions; changing the directory format to
  fit cache high-water would conflate layout with serving-cache policy.
- **Bounded-publication candidate (first run)** and **generation-scoped row verification follow-up**
  — readiness and RSS improved but read-only throughput regressed (c30 QPS 19% lower, p95 19%
  higher than the corrected candidate).
- **r129 (`d68605ac7`)** — correctness-qualified but failed performance: 50K mean server time
  3.23→4.83 ms and c30 1,065→348 QPS vs r126; 1M ready 775.94→934.74 s vs r125.

## Key measurements

Stable-tip flatten regression (1M): retained deltas cut native leaf-scan hits 2,048→~35 and forced
~469 sparse projection reads/query; C20 fell 679.28→125.28 QPS, p95 66.62→218.54 ms.

Clean post-fix qualifications (native-authoritative from catalog publication):

| Run | Ready s (insert + optimize) | Recall | QPS c1/10/20/30 | p95 ms c1/10/20/30 | Live / restart peak RSS |
| --- | --- | --- | --- | --- | --- |
| 50K post-fix | 24.8254 (12.5043 + 12.3211) | 0.9863 | 280.50/1092.67/1218.01/1174.47 | 4.03/14.94/34.02/60.25 | 2.54 GB / 339 MB |
| 1M regression-recovery | 617.3662 (548.9080 + 68.4582) | 0.9925 | 78.13/633.96/794.07/828.00 | 12.90/24.72/55.77/94.32 | 7.05 GB / 2.39 GB |
| 50K scratchpool (bounded publication) | 30.3630 (18.9887 + 11.3743) | 0.9885 | 293.82/1,079.76/1,192.00/1,319.31 | 3.88/17.03/49.38/71.27 | 1.52 GB / 341 MB |

Directory A/B, 50K controlled:

| Directory | Ready s | Recall | QPS 1/10/20/30 | Live RSS |
| --- | ---: | ---: | --- | ---: |
| V1 control | 27.37 | 0.9860 | 231 / 910 / 967 / 1,040 | 1.76 GB |
| Kind-separated V2 | 33.52 | 0.9857 | 225 / 920 / 996 / 953 | 1.46 GB |
| Row-block V2, run B | 24.38 | 0.9857 | 284 / 1,084 / 1,203 / 1,245 | 2.08 GB |

At 50K the row-block directory was 1,439,976 bytes (196 blocks) vs 1,781,658 (kind-separated) and
~3,738,930 (V1). At 1M: kind-separated 35,908,804 bytes / 7,814 blocks; row-block 27,516,934 bytes /
3,907 blocks, ~63.7% below equivalent V1.

| Public 1M | Kind-separated V2 | Row-block V2 | Row V2 + admission/cold reads |
| --- | ---: | ---: | ---: |
| Total ready | 716.46 s | 710.24 s | 724.25 s |
| Recall | 0.9927 | 0.9929 | 0.9924 |
| QPS c1/10/20/30 | 58 / 568 / 596 / 585 | 71 / 636 / 642 / 534 | 70.7 / 626.8 / 701.6 / 649.6 |
| p95 ms c30 | 105.61 | 111.86 | 87.48 |
| Live peak RSS | 5.73 GB | 6.72 GB | 6.26 GB |
| Restart peak RSS | 2.37 GB | 2.12 GB | 2.37 GB |

- Row-block 1M C30 drop is a memory-bandwidth/cache-contention knee (all 30 clients active, ~240K
  approximate vectors/query unchanged), not a directory regression. Admission held at most 17
  simultaneous 1M scans, 381 MB of 402.7 MB. Cold reads removed ~462 MB of live RSS high water at a
  cost of 15.45 s publication.
- Search-view/MVCC final: 50K ready 29.9782 s, recall 0.9856, 239.9/986.6/1080.8/1147.1 QPS; 1M ready
  767.8569 s, recall 0.9902, restarted concurrency-only repeat 59.0/596.1/642.4/577.2 QPS, read-only
  peak RSS 5.064 GB (~17.1% below r126's 6.11 GB).
- Mixed baselines (stable-ID rewrites, 10 query / 4 write workers): 50K 121.8 QPS, 619 rows/s,
  0.9740 recall, 3.552 GB; 1M 63.4 QPS, 330 rows/s, 0.9901 recall, 101.3 s catch-up, 5.403 GB.
- After async replay/publication: 50K mixed 136.9 QPS and 1,426.9 rows/s at 0.9762 recall, 2.78 GiB;
  1M final 30-second run 174.9 QPS, 1,397.7 rows/s, 0.9920 recall, server query p95 21.79 ms, catch-up
  57.39 s draining 421 replay batches, RSS 6.01 GiB with only 35.6 MiB HBC cache.
- Dirty-leaf exact costs at 50K mixed: server p95 80.63→39.72 ms, write throughput 568.1→1,057.2 rows/s.
- Corrected native-bootstrap 50K: ready 35.36 s, recall 0.9864, 261/1065/1094/1173 QPS — recovered the
  catastrophic regression but c20/c30 p95 still 14.0%/5.9% above the 29.98-s baseline.

## Audited historical baselines (2026-09-05)

A read-only audit against raw results found no single run wins every metric. Use 50K scratchpool
as the throughput/RSS reference, 50K row-block B as the faster-load/lower-tail reference, 1M
regression-recovery as the primary speed reference (828 QPS with 7.05 GB RSS), and 1M
admission + cold publication as the balanced/c30-tail reference. Corrections:

- r126's live peak RSS was 6,111,182,848 bytes (6.11 GB), not 5.73 GB (that was kind-separated V2).
- r126's original live c30 failed with BrokenProcessPool; its cited 673.5611 QPS / 92.880-ms p95 came
  from a separate restarted retry and must not be combined into a live curve.
- r125's 775.94-s / 3.860-GB 1M result remains a documented lower-memory target; its raw bundle was
  not located, and its original live c30 was 483.43 QPS (561.32 on restart).
- `vdbbench-pr593-verified-rows-1m-20260905` inserted 1M rows in 262.59 s but failed at applied
  sequence 6126 with ResourceBudgetExceeded then PostingWalMutationStoreUnavailable; not a
  load-to-ready result.

## Open follow-ups recorded in the log

The 17-item "Next checks" list was folded into
[VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md). Durable directions stated in
the narrative: stream final publication through bounded extents and account resident bytes by
generation/component; move the monolithic posting segment to a manifest over immutable chunks;
migrate legacy first-generation directories through the staged streaming writer; separate
allocation-byte from bandwidth admission; status UX should distinguish `temporarily_unavailable`
from an empty index.
