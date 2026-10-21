# Source Ownership Migration Qualification (September 2026)

> Summary of a verbatim log. Full text: [docs/design/vector-store/history/migration-qualification-findings.md@f3fd4d3](https://github.com/antflydb/antfly/blob/f3fd4d35258791fe16ab927a1e7f9b3af247b074/docs/design/vector-store/history/migration-qualification-findings.md). Originally relocated verbatim from `zig/VECTOR_STORAGE_MIGRATION_FINDINGS.md` (lines 1–485 at commit 3ce5aff4ea) on 2026-10-12; durable decisions were folded into [VECTOR_STORE.md](../../../../zig/VECTOR_STORE.md#qualification-and-remaining-scope) before the move.

Qualification of `antfly storage migrate` and the table-scoped `/storage/migrations` job API, comparing primary-LSM tables migrated online/offline against fresh vector-store tables (harness: [`scripts/qualify_vector_migration.py`](../../../../zig/scripts/qualify_vector_migration.py)). It did not change the table-creation default. The screens found and fixed forced per-page flushes, retained inline primary bytes, and descriptor-cache allocation; final 50K/1M screens at `a494f01b21` qualified conversion/recovery and reclamation, but not equivalent query quality or performance.

**Protocol.** 768-D normalized synthetic vectors, cosine, default ANN settings, 256-document write batches, 1,000 updates and 1,000 deletes; online churn after capture admission; semantic availability query every 16 migration steps. Semantic, full-text and 50/50 mixed workloads at concurrency 1/8/32 with 128 warm-up requests. The runner fails if source collection misses the expected live payload count and (for ≥512-D corpora) if migrated primary SSTables exceed half the raw vector plane. These are sequential single screens on one host, not repeated ABBA promotion data; the isotropic corpus has low absolute recall, restart is warm, and lock waits are not measured.

## Decisions
- **Migration page commits use the WAL durability barrier, not `sync(true)`.** `sync(true)` synchronizes the WAL *and* flushes memtables, so each (often receipt-only) page made tiny files: the first online 50K run recorded 1,304 flushes, 1,448 output runs, 1,762 manifest writes and 112 L0 runs. Ownership publication keeps its full storage barrier; a regression checks 32 single-row pages create no extra flushes/SSTables.
- **Measure semantic throughput after restart in every arm, and capture LSM counters before queries.** On reopen, the fresh table lost its ingestion-time advantage (C1 22.6 vs 22.2/22.3 QPS), so the initial gap was not evidence of migration-specific damage.
- **Explicit `reclaiming` phase.** At 1M, online completion left 2,595,108,632 bytes of primary SSTables vs 379,544,230 fresh (old inline values in active lower-level runs, not orphan source payloads). Migration now flushes replacements once and durably requests overlap rewrites through the LSM GC planner; requests qualify without tombstones, survive partial jobs/splits/restart, and are cleared only by a validated full overlap rewrite. Job completion waits for it. Explicit steps bypass optional query-idle deferral (which starved reclamation in a test) but keep memory/I/O admission and yielding, outside the table apply lock — production behavior, not benchmark-only compaction.
- **Descriptor-cache hits are allocation-free.** A hit previously duplicated and freed the path via the page allocator; it now retains the existing descriptor under the shard mutex first.
- **Offline migration supplies a 64 MiB shared LSM block cache** when the caller supplies none (direct offline DB open previously had none); it follows the standalone resource policy and participates in memory admission.
- **Catalog-only admission can be cancelled** without starting the source store or passing the disk reserve, via a durable cancelled receipt written under the apply lock; exact retries return it. Offline cancellation persists a source-identity-bound receipt even before the copy fence exists.
- **Offline copy syncs the whole directory chain** (including the shadow root) before acknowledging a file's first chunk; retries sync existing directories too. Tested with a stricter VoprIo directory-sync adapter (the default model persists the whole namespace on any directory sync and cannot expose the bug); simulated, not physical power-cut tests.
- **Per-table command slot** for online mutation handlers, held across the DB command and catalog reconciliation, so a paused start cannot resume after another handler cancels/replaces its admission; contention returns retryable HTTP 503. Applies to the single-process standalone deployment.
- **Migration pages retain only cursor keys** for unrelated primary values; inline embeddings are hashed into compact references and validated against the committed candidate map, so an oversized vector consumes one page without a full payload copy.
- **A terminal cancelled job on primary ownership is immediately snapshot-eligible**; active, cancelling, replacement and published jobs remain ineligible, and native/portable capture recheck eligibility after their admission boundary.
- **Test completion helpers use deadlines, not step/request counts** (30 s for unit fixtures, 120 s for the E2E neighbor helper), since timed GC admission retries cannot be bounded by counts. No production scheduling change.

## Rejected approaches / non-causes
- **Treating every document ordinal as its vector ID** to avoid identity lookups — not general; breaks one-to-many chunk and multi-source indexes.
- **Attributing the query-15 neighbor mismatch to migration.** It reproduced without migration: three immediate primary-LSM restarts kept `doc:000221`, then five seconds idle replaced it with `doc:002725`. Dirty posting payloads fall back to exact member scoring while clean postings use quantized candidate selection, so neighbors can change across an idle repair; restart alone does not drain it. The fixture confused replay completion with ANN convergence. Fix: the E2E fixture activates the owner with a query, then waits for fresh, complete status and a clean refresh certificate (status-only polling can retire the temporary owner). `hbc_posting.refresh_pending` semantics are in [DENSE_INDEXING_LIFECYCLE.md](../../../../zig/DENSE_INDEXING_LIFECYCLE.md#status-and-metrics).

## Key measurements

Initial 50K screen (ran alongside compilation; diagnostic): churn plus migration 1.07 s fresh / 26.94 s online / 30.50 s offline; semantic QPS C1 330.6 / 22.3 / 21.4. No-churn fresh control reopened at 287 / 1,599 / 1,745 QPS (C1/C8/C32), so the 22-QPS result depends on churn history.

Final 50K (`a494f01b21`):

| Measurement | Fresh | Online | Offline |
|---|---:|---:|---:|
| Churn plus migration, s | 1.55 | 18.49 | 11.52 |
| Restarted semantic QPS C1/C8/C32 | 23.5 / 130.0 / 140.9 | 23.0 / 111.2 / 116.5 | 22.8 / 111.1 / 122.9 |
| Recall@10 | 0.197 | 0.159 | 0.181 |
| Sampled peak RSS, MiB | 1,089.8 | 1,471.1 | 1,161.0 |
| Final primary SSTable bytes, MB | 18.59 | 19.04 | 17.24 |
| Final allocated disk, MB | 206.29 | 207.69 | 204.96 |

1M, before (WAL barrier only) vs final (`a494f01b21`):

| Measurement | Fresh | Online | Offline |
|---|---:|---:|---:|
| Churn plus migration, s (before → final) | 5.2 → 4.3 | 351.1 → 449.4 | 1,792.6 → 530.7 |
| Primary SSTable bytes, GB (before → final) | 0.380 → 0.388 | 2.595 → 0.349 | 3.614 → 0.379 |
| Allocated disk, GB (before → final) | 3.988 → 3.980 | 6.234 → 3.977 | 7.240 → 4.006 |
| Final restarted semantic QPS C1/C8/C32 | 18.0 / 83.2 / 103.9 | 19.0 / 86.8 / 98.3 | 20.1 / 92.1 / 91.6 |
| Final restarted semantic C8 p99, ms | 140.0 | 120.7 | 120.2 |
| Final Recall@10 | 0.1094 | 0.1063 | 0.0875 |
| Final sampled peak RSS, GiB | 6.84 | 11.67 | 7.14 |

- Final 1M: both migrated tables within 1% of fresh total disk; after matched restart, online semantic QPS within about 6% of fresh and offline within about 12%. Online peak RSS is about 71% above fresh, so online operators need temporary memory headroom. The offline speedup measures the combined changes, not the block cache alone.
- Descriptor-cache microbenchmark (Debug, 20,000 cached acquisitions, ABBA ×3): 48.081 ms and 20,000 allocations → 7.394 ms and zero allocations, about 6.5× for that operation; no end-to-end claim.
- Same-index neighbor preservation: 4,096 64-D vectors, 32 top-10 queries; online and offline conversion preserve all ordered neighbor lists exactly across migration and restart. Covers native ANN conversion only, not legacy ANN rebuilding or 1M recall.
- Final validation after merging origin/main `1faa190bd2`: production migration/vector-store suite 19/19, plus three extra restart/online/offline rounds (9/9), with exact ordered-neighbor assertions.

## Open follow-ups recorded in the log
- Shared post-churn query slowdown: live-document constraints map nonvisible document ordinals to ANN member IDs before search, and missing mappings fall through primary and legacy identity lookups without caching misses. Suggested next step: isolate deletes from updates, count those lookups per query, and keep generation-aware visibility when avoiding repeated lookups of absent members.
- Equivalent query quality would need before/after measurements on the same large built index and repeated matched runs.
