# VectorDBBench Findings: No-Copy Default, Posting-Row Experiments, and Ownership Comparison (2026-09-07 – 2026-09-14)

> Summary of a verbatim log. Full text: [docs/design/dense-indexing/history/vectordbbench-2026-09-07-14.md@f3fd4d35](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/dense-indexing/history/vectordbbench-2026-09-07-14.md). Originally relocated verbatim from `zig/VECTORDBBENCH_FINDINGS.md` (lines 8519–10257 at commit 3ce5aff4ea) on 2026-10-12; durable decisions, the no-copy default, and the 1M ownership-comparison table were folded into [VECTORDBBENCH_FINDINGS.md](../../../../zig/VECTORDBBENCH_FINDINGS.md). The closing line ("`primary_lsm` remains the creation default") was true when written; fresh local single-shard standalone tables now default to `vector_store` (see [VECTOR_STORE.md](../../../../zig/VECTOR_STORE.md#scoped-default-promotion)).

PR 593 work in `spfresh-segment-wal`, 2026-09-07 to 2026-09-14. New posting builds switched to
the no-copy projection policy; a series of default-off experiments (locality packing, borrowed
pages, compact routing hints, incremental source publication, completion-lane wakeup, staged
readers/rebase, deferred capture, delete planning, stable scoring origins, native posting row
deltas) were implemented and A/B tested. None was promoted. The period ends with a fresh
`primary_lsm` vs `vector_store` ownership comparison that favored vector-store at 1M. Most runs
were on a shared host; the log repeatedly warns against causal attribution from two-arm medians.
Paths under `.benchmark-results/` and `.benchmark-assets/` are untracked local evidence.

## Decisions

- **No-copy is the default for new posting builds (2026-09-07)**, at the user's request: omit the
  duplicate posting-local projection plane, accepting the documented 50K throughput/restart
  tradeoff for single-copy storage. Not a measured all-metric win.
  `ANTFLY_EXPERIMENT_POSTING_LOCAL_PROJECTIONS=1` keeps locality on; existing planes stay readable.
- **Preserved baselines** in `zig/benchmark-baselines/pr593-locality-matched-20260906.json` and
  `pr593-locality-fast-50k-20260906.json` (catalogs with receipts, binary identities, SHA-256s;
  archives are local, Git-ignored). Experiment A/Bs pin locality explicitly so a default change
  cannot silently alter their contracts.
- **Qualification rules adopted in the runners**: reject arms with missing/zero treatment
  evidence (an enabled flag or unpublished worker is not enough); reject public-write errors,
  retries, source-store poisoning/OOM and native chunk/capture errors even when the client exits
  zero; same binary for on/off arms; record binary and helper SHA-256s; keep the 2-second
  readiness poll identical across arms; equal offered write rates as well as saturation.
- **Darwin rusage CPU** is converted with the Mach timebase (125/3 on this ARM64 host).
- **Private checkpoint reads authenticate their own bytes** (`16a18eb38`) rather than reusing an
  mmap verification bit for a separate cold-read buffer.
- **Completion-driven checkpoint handoff**: ResourceManager-owned coalesced completion signal and a
  joined `std.Io` publication task that only publishes already-completed HBC workers; native backup
  capture closes and drains the catalog lease before pinning generated files.
- **Local-cache admission fix**: local node/quantized/vector/metadata caches reserve both maps and
  allocate ownership before publication (fallible admission could leave the cache pointing at freed
  memory).
- **Duplicate HBC delete removed**: an eager default-options delete preceded the options-aware pass
  in mixed transactions.
- **Same-vector replacement elision excluded**: the latest primary artifact does not prove which
  vector revision the existing HBC payload represents.
- **Main reconciliation (`2ba91d732`, main at `3d3b6ed0a`)**: main's runtime-status V16 (inference
  diagnostics) stays positional; native framing/capabilities moved to V17. Pre-merge experimental
  V16 metadata is not an upgrade fixture.
- **Shared allocation-free admission handoff**: bandwidth and rerank admission share an intrusive
  FIFO; detach and charge under the policy lock, signal, then transfer lifetime with the final
  release store (the old queue published `admitted` before its last event/Io access).
- **StorageBusy keeps its retryable identity and maps to HTTP 503**; ambiguous write outcomes remain
  conflicts and commits are not retried automatically.
- **Periodic owner inspection attempts exclusive admission only when idle**; structural changes and
  explicit repair keep waiting admission. Waiting periodic inspection had installed an exclusive
  pending flag that starved foreground leases past their five-second deadline (1M query HTTP 503).
- **Structural reconciliation captures an owner observation** before releasing the owner generation
  (empty proofs were rejected with EmptyTargetedIndexObservation, looping reconciliation).
- **Unreleased formats** (AFQD schema revisions, AFRC V1, AFRM V1) are not compatibility targets;
  qualify new formats on fresh roots with their own binaries.

## Experiment dispositions (all default-off, none promoted)

| Experiment | Outcome |
| --- | --- |
| Single-copy locality packing (source-trained) | Negative screen: 8.6% fewer reads at 50K, 3.8% at 1M, below the 20% no-extra-bytes gate; page-LRU variants fetched 3.53× / 8.51× requested bytes. No format rewrite. |
| Borrowed clean pages (`ANTFLY_EXPERIMENT_PROJECTION_BORROW`) | Inconsistent at 50K; at 1M only 21.96 borrows/query, worse footprint and catch-up. Off. |
| Compact subgroup routing hints (`ANTFLY_EXPERIMENT_COMPACT_SUBGROUP_ROUTING`) | First mutex cache serialized at C30 (routing 0.250→1.154 ms). Atomic-slot cache: 0.123 ms. Modest paired gains, ~0.43–0.50 pp recall loss, does not consistently beat float32 global hints. Off. |
| Incremental source publication (append-only segments, selective GC) | Passes changed-vector/delete/restore/restart checks; no repeatable disk, catch-up or load win. Not reusable HBC checkpoint chunks. |
| Suffix compaction (`ANTFLY_EXPERIMENT_COMPACT_POSTING_DELTAS`) | Lifecycle-tested; no performance qualification. Still rewrites the selected suffix's rows. |
| Completion-lane wakeup | Initial checkpoint wait 153.397 ms → 0.017/0.015 ms, but C30 QPS lower and p95 higher in both 50K pairs; 1M candidates slower (C30 622.877/672.493 vs 861.319/762.882 QPS). Not an end-to-end win. |
| Staged posting readers (`ANTFLY_EXPERIMENT_STAGE_POSTING_READERS`, `01123472c`) | Moves reader work off the writer; 1M pair directions reverse; aggregate C30 697.860→764.758 QPS with higher RSS (4.922→5.994 GB). Capture lifetime, not final rebase, dominates remaining wait. |
| Staged rebase, certified subgroups, encoded-row reuse | Implemented (`b099035d7`) with focused tests; no performance claim. |
| Deferred source capture (`ANTFLY_EXPERIMENT_DEFER_SOURCE_CAPTURE`) | Matrix stopped on a client `ConnectError` in the first 50K control; unqualified. Collection was only ~0.3–1.4 ms; apply is the real cost. |
| Coalesced replay deletes + single-read eager leaf refresh | 50K: normalized apply −17.9% / −12.9%; not an all-metric win; no 1M run. |
| Stable scoring origins (`ANTFLY_EXPERIMENT_STABLE_POSTING_ORIGINS`) | 50K apply −50.5% / −46.7%, catch-up 0.849→0.115 s, but more smaller windows, mixed p99 regression, larger posting WAL. 64-mutation lag cap is an experimental guard only. |
| Native posting row deltas (`ANTFLY_EXPERIMENT_POSTING_ROW_DELTAS`) | Functional and recovery-tested; 1M readiness and query throughput regress and checkpoint output is ~4× (see below). |

## Rejected approaches

- **Process-wide fixed tuning / unbounded mmap warmup / inlining all reads** to recover 50K warm
  throughput — inlining was tested without a win; residency must stay budgeted.
- **Suppressing maintenance on `boundary_mismatch`** — the observation does not mean repeated vector
  builds (`ensureVectorBlockBaseAtAppliedSequence` returns early); suppressing it could starve HBC
  publication.
- **Attributing the 798.978-s 1M outlier to checkpoint encoding or contention** — its two full
  checkpoint stagings total 7.075 s; the 373.309-s optimize interval remains unexplained.
- **Benchmark-specific batching delays** to hide smaller replay windows after faster deletes.
- **Moving the eager whole-leaf rebuild to another thread or delaying it without a debt limit** —
  not the stable-origin/row-revision design.
- **Treating fewer checkpoint output bytes, requested allocation bytes, or encoded bytes as disk/RSS
  savings** — repeatedly called out as different measures.

## Key measurements

Fresh checkpoint-default vs preserved no-copy (no extra flags), 1M pairs:

| Arm | Ready s | C30 QPS | C30 p95 ms | Mixed write p95 ms | Catch-up s |
| --- | ---: | ---: | ---: | ---: | ---: |
| Pair 1 preserved no-copy | 387.473 | 790.4 | 83.137 | 98.937 | 39.817 |
| Pair 1 current | 354.622 | 661.8 | 94.241 | 461.439 | 105.457 |
| Reversed current | 290.453 | 873.8 | 65.104 | 90.157 | 31.528 |
| Reversed preserved no-copy | 281.802 | 894.1 | 66.010 | 90.779 | 31.235 |

Stable-origin follow-up: both 50K pairs replayed exactly 30,100 rows; window counts rose from 49 to
242/250 and insertion-stage work rose 2.134→4.036 s and 2.108→3.486 s. Each control published three
mixed delta checkpoints, neither candidate did; candidate pair 1 kept an 84.4 MB logical posting WAL
vs 22.9 MB.

Row-store component microbenchmark (synthetic 1,024-row 768D leaf, 977-leaf work count; not a
public dataset): delete prep 18.23–18.51 µs → 0.512–0.521 µs per leaf; replace encoded output
113.08 MB → 8.41 MB; two chunks after replacement cost ~3–5% more to scan than a repacked run.
Shared query preparation per scoring origin: 64 chunks/leaf 21.62 → 13.03 µs. Shared AFRO origins
cut a 64 single-row-chunk fixture to under one eighth of the previous encoded bytes.

Integrated native row deltas, `0326a2bd9` / SHA-256 `8b61fa30…`, two-arm medians:

| Metric | 50K control | 50K row deltas | 1M control | 1M row deltas |
| --- | ---: | ---: | ---: | ---: |
| Full readiness | 15.44 s | 16.01 s | 347.74 s | 382.44 s |
| C30 QPS | 1,673 | 1,639 | 593.5 | 436.2 |
| Live recall | 98.33% | 98.46% | 99.01% | 99.05% |
| Final mixed catch-up | 0.737 s | 0.480 s | 5.420 s | 2.408 s |
| Allocated disk after restart | 440.49 MB | 499.14 MB | 3.810 GB | 3.775 GB |

At 50K, delete time fell 17.78/18.85 s → 2.93/3.02 s but embedding apply rose 2.12/2.43 s →
20.27/19.86 s (cost transfer, not a maintenance win). At 1M through source sequence 10001, controls
wrote 2.048/2.055 GB of checkpoints vs 8.259/7.773 GB for candidates (4.03×/3.78×), six full
checkpoints vs two. Follow-up fixes (shared AFRO origins, AFRM V2 per-leaf revision-scoped debt
bounded to 64 leaves / 64 MiB per pass, AFRR redirects so newer WAL manifests resolve to compacted
rows) were implemented with no new public qualification.

## Ownership comparison (2026-09-13 – 2026-09-14)

Earlier attempts failed: the first 1M LSM control logged batch and query HTTP 500s, after which two
connected defects were repaired (StorageBusy collapsing to RuntimeBoundaryFailure, and the
empty-observation reconciliation loop); after `8337942cb1` a 1M LSM arm
returned HTTP 503 during mixed updates (periodic owner admission). With pinned `5a90e99d6e` all
eight fresh arms passed (50K OpenAI 1536D, 1M Cohere 768D; absolute QPS not comparable across sizes).

| 1M pair/order | Ownership | Readiness s | Peak QPS | Mixed QPS | Mixed query p99 ms |
|---|---|---:|---:|---:|---:|
| 1 / AB | primary_lsm | 273.25 | 1144.2 | 211.29 | 104.30 |
| 1 / AB | vector_store | 247.44 | 1210.3 | 232.80 | 95.38 |
| 2 / BA | vector_store | 265.38 | 1225.6 | 225.02 | 100.64 |
| 2 / BA | primary_lsm | 264.22 | 1141.9 | 189.00 | 113.90 |

- 1M vector-store: peak QPS +5.8%/+7.3%, mixed QPS +10.2%/+19.1%, recall 0.9901–0.9904, allocated
  disk −43.4% (6.74–6.77 GiB → 3.82–3.84 GiB), max RSS ~28% lower (9.35/10.06 → 6.71/7.25 GiB).
- Tradeoffs: at 50K disk −47–49% but peak QPS +5.8%/−22.5%; 1M update/delete churn 4.4%/15.5% slower;
  reclamation settles in 149/155 s vs ~33 s for LSM, each GC reading 3,072,000,000 and writing
  3,192,012,800 bytes; updated-embedding readiness 1.12/1.90 s vs 1.00/1.04 s.

## Open items recorded only in this log

- Vector-store small-table profiles resolve 0.225 vs 45.939 metadata entries per query; trace the
  resolved fast-path miss (also listed in VECTORDBBENCH_FINDINGS.md).
- Existing Debug test `db dense auto bulk finish wakes weak-sync replay and publishes visibility after
  catch-up` failed its `publish_blocking_checkpoint_clean` assertion with the capture flag both on and
  off; cause not established.
- The deferred-capture client `httpx.ConnectError: [Errno 49]` cause was not established.
- Stable-origin mixed p99 regression needs attribution. Row deltas force fused scoring on row leaves
  (a candidate cause of the slower post-restart leaf timer, unproven).
- Remaining long-term shape: revision-aware base/delta row views consumed by queries under generation
  leases, off-writer selective repacking with a bytes/density/age debt policy, and reusable physical
  checkpoint chunks with reference-manifest GC (unimplemented).
