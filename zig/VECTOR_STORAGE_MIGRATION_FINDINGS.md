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
admission. Every query cell warms up for 128 requests; the first screen measures 4,096 requests
at concurrency 1, 8, and 32. Workloads are semantic, full-text, and an alternating
50/50 mix. Memory sampling includes the server and offline migration process;
disk includes retained physical roots. Source reclamation is observed separately
from job completion, with a 180-second observation window.

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
