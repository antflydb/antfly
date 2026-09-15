# Source ownership migration qualification

The public interface is `antfly storage migrate` and the table-scoped
`/storage/migrations` job API documented in [VECTOR_STORE.md](VECTOR_STORE.md).
Qualification compares existing primary-LSM tables migrated online/offline with
fresh vector-store tables. It does not change the table-creation default.

## Protocol

[`scripts/qualify_vector_migration.py`](scripts/qualify_vector_migration.py) retains
inputs, binary hashes, databases, progress, and results. The screen uses 768-D
normalized synthetic vectors, cosine distance, default ANN settings, 256-document
write batches, 1,000 updates and 1,000 deletes. Online churn happens after capture
admission, and a semantic query checks availability every 16 migration steps.
The reported online duration includes those queries. Every query cell warms up
for 128 requests; the first screen measures 4,096 requests
at concurrency 1, 8, and 32. Workloads are semantic, full-text, and an alternating
50/50 mix. Memory sampling includes the server and offline migration process;
disk includes retained physical roots. After the restarted query cells, the
harness waits up to 180 seconds for source reclamation. That additional wait is
not the elapsed reclamation time since job completion.

This is a sequential screen on the available host. It is not a repeated ABBA
promotion qualification. The isotropic synthetic corpus has low absolute recall
with default ANN settings; its QPS cannot establish equivalent search quality.
Restart is warm, and this harness does not measure lock waits directly.

## Initial 50K screen: forced-flush regression

Preserved receipts are under
`.benchmark-results/vector-migration-implementation/compare-50k/` in the worktree.
All three modes completed, retained the expected 49,000 payloads after churn,
and reopened successfully. They did **not** have equivalent query performance.

| Measurement | Fresh vector-store | Online migration | Offline migration |
|---|---:|---:|---:|
| Initial ready, seconds | 25.68 | 25.07 | 25.53 |
| Churn plus migration, seconds | 1.07 | 26.94 | 30.50 |
| Semantic QPS C1 | 330.6 | 22.3 | 21.4 |
| Semantic QPS C8 | 1,658.6 | 105.9 | 98.5 |
| Semantic QPS C32 | 560.5 | 127.1 | 114.3 |
| Full-text QPS C8 | 1,265.8 | 1,230.1 | 1,277.7 |
| Mixed QPS C8 | 1,777.5 | 189.2 | 182.3 |
| Recall@10 | 0.169 | 0.147 | 0.138 |
| Warm restart, seconds | 0.46 | 1.47 | 1.24 |
| Allocated disk, MiB | 197.9 | 199.6 | 350.2 |

The first screen ran alongside compilation, and the fresh C32 tail was an
outlier. Its offline RSS sampler omitted the command's peak; that harness bug
is fixed for subsequent runs. These are diagnostic results, not promotion data.

The online primary store recorded 1,304 flushes, 1,448 output runs and 1,762
manifest writes, leaving 112 L0 runs. Snapshot-rotation counters stayed at zero.
The actual cause of the tiny files was `sync(true)` after each migration page:
that API synchronizes the WAL **and** flushes pending memtables. Verification
pages often update only the small job receipt. A short sample showed ANN
identity lookups and Snappy decompression in primary-LSM point reads; the sample
was too small to assign a percentage of query time.

Page commits now use the existing WAL durability barrier. Payload preparation
still precedes the atomic primary reference/progress commit, and ownership
publication retains its full storage barrier. A regression checks that 32
single-row pages create no additional flushes or SSTables. Recovery-boundary
tests pass with this change. A production test additionally kills the process
after an acknowledged backfill page, verifies its exact receipt after reopen,
and completes the migration and queries both models. All ten production
migration/vector-store tests pass.

## Separating restart from migration

A follow-up probe reopens each preserved 50K table with the same original
binary, warms 128 queries and measures 256 semantic queries per cell. Receipts
and the probe script are retained under the same implementation result root.

| Reopened table | C1 QPS | C8 QPS |
|---|---:|---:|
| Fresh vector-store | 22.6 | 120.4 |
| Online migration | 22.2 | 116.2 |
| Offline migration | 22.3 | 107.1 |

The fresh table loses its ingestion-time advantage after restart. Therefore the
large initial gap is not evidence that migration uniquely damages steady-state
throughput. The forced page flushes are independently undesirable, but fixing
them cannot be assumed to fix the shared restart/identity-lookup cost. The
corrected harness measures semantic throughput after restart in every arm and
captures LSM counters before queries. The corrected 50K/1M screen uses 1,024
measured queries per cell, keeping all three arms matched within each screen.

## Corrected 50K screen

Receipts: `.benchmark-results/vector-migration-implementation/wal-50k/`.
All modes completed and observed exactly 49,000 retained source payloads with
no pending source collection bytes. The binary includes the WAL page barrier
and rebuilding-index error identity fix.

| Measurement | Fresh vector-store | Online migration | Offline migration |
|---|---:|---:|---:|
| Initial ready, seconds | 25.47 | 25.49 | 25.53 |
| Churn plus migration, seconds | 1.56 | 15.37 | 13.88 |
| Reopened semantic QPS C1 | 22.7 | 22.7 | 22.7 |
| Reopened semantic QPS C8 | 124.1 | 107.8 | 118.5 |
| Reopened semantic QPS C32 | 132.7 | 117.0 | 134.5 |
| Full-text QPS C8, before restart | 1,217.9 | 1,236.1 | 1,244.1 |
| Recall@10, before restart | 0.188 | 0.194 | 0.191 |
| Warm restart, seconds | 1.43 | 1.47 | 1.47 |
| Peak process RSS, MiB | 1,078.0 | 1,340.1 | 1,046.5 |
| Final allocated disk, MiB | 197.7 | 199.4 | 199.8 |

Online migration plus churn fell from 26.9 to 15.4 seconds, and offline from
30.5 to 13.9 seconds. This is a single before/after screen, not a confidence
interval. Query measurements have different fixed counts across these screens;
compare matched arms within each screen.

Immediately after online completion, the primary store had 12 runs and 187 MB
of SSTables, including superseded inline bytes. Existing background compaction
reduced this to 23 MB during queries and 20 MB after restart. No manual flush or
compaction was injected into the comparison. Final total disk was within about
2 MB of fresh storage. Logical completion and physical reclamation are distinct.

Fresh pre-restart throughput itself varied substantially between screens. The
matched reopened results are more informative: C1 agrees, while online C8/C32
are about 13%/12% below fresh in this run. The screen neither establishes a
migration-specific order-of-magnitude loss nor proves complete performance
equivalence. All arms retain the shared post-churn query bottleneck.

### No-churn control

The short fresh-only control in `no-churn-50k/` uses the same binary, 50K rows,
no updates/deletes, and 128 measured queries per cell. Reopened semantic QPS is
287 at C1, 1,599 at C8 and 1,745 at C32. Reopening alone does not reproduce the
22-QPS result; the churn history matters. This control changes both updates and
deletes together, so it does not experimentally distinguish them.

The code provides a concrete next hypothesis: live-document constraints map
nonvisible document ordinals to ANN member IDs before search. Missing mappings
fall through primary and legacy identity lookups and are not cached as misses.
The saved sample visits this path. A follow-up should isolate deletes from
updates, count those lookups per query, and preserve generation-aware visibility
when avoiding repeated mapping of absent ANN members. It must also retain
correct one-to-many behavior for chunk and multi-source indexes; treating every
document ordinal as its vector ID is not a general solution.

## 1M screen: explicit primary reclamation

The first WAL-barrier 1M fresh and online arms completed and recovered, with
999,000 retained source payloads after churn. Online completion did not ensure
physical primary reclamation: it retained 2,595,108,632 bytes of primary SSTables,
versus 379,544,230 for fresh storage. Total allocated disk was 6,233,559,040 versus
3,987,898,368 bytes. Source collection was complete in both, so the difference
was old inline primary values rather than orphan source payloads. These receipts
are retained under `wal-1m/`; the offline arm is still running.

The migration now has an explicit `reclaiming` phase. After candidate cleanup,
it flushes replacements once and durably requests overlap rewrites through the
existing LSM GC planner. Requests qualify even without tombstones and survive
partial level jobs, splits and restart. Only a validated full overlap rewrite
clears them. Job completion waits for that work; reader pins, retention windows
and source collection can still retain files afterward.

Explicit steps bypass optional query-idle deferral, which otherwise starved
reclamation in a test that queried between every step. They retain ordinary
memory/I/O admission and streaming-work yielding, and execute outside the table
apply lock. This is production migration behavior, not a benchmark-only force
compaction. Metadata request preparation reserves its working set before
cloning the directory and publishes its intent before the job receipt.

The regression checks bounded input I/O, an old reader, restart after partial
progress, and removal of superseded values in a store with zero tombstones.
The LSM suite passed 493 tests (23 skipped), and all 12 DB migration tests passed,
including recovery at the manifest-request and primary-receipt boundaries.
Production and 50K/1M qualification of this additional step are pending.
