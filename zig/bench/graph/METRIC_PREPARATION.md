# Graph metric execution and query benchmarks

Measured 2026-09-07 on Apple M4 Max, 36 GiB RAM, macOS 26.3.1,
Zig 0.16.0, ReleaseFast, using the system SMP allocator. One warmup and five
measured samples per case; tables report medians. This was a shared development
host, not an isolated benchmark machine.
The compact-query comparison below uses 21 measured samples instead of five.

Run from `zig/`:

```sh
zig build -Doptimize=ReleaseFast graph-metric-preparation-bench --summary all > /tmp/graph-metric-bench.jsonl 2> /tmp/graph-metric-bench-build.log
```

The executable emits JSONL including min/max time, allocation count, cumulative
allocated bytes, tracked peak bytes, and workload dimensions. Separate stdout
and stderr preserve the machine-readable measurements.

## Serverless topology preparation

Fixtures are directed degree-eight rings with 48-byte document IDs and one edge
type. Both paths consume the **same current v3 payload**. The reference decodes
owned adjacency strings, discards inbound edges, then compiles through string
hash maps. Production reads borrowed ordinal views directly. Fixture memory,
input payload residency, fetch, encoding, projection and numerical kernels are
excluded from this timed/tracked phase.

| Nodes / outbound edges | Reference median | Packed median | Reference peak | Packed peak |
| --- | ---: | ---: | ---: | ---: |
| 2,000 / 16,000 | 1.580 ms | 0.163 ms | 3,668,016 B | 380,051 B |
| 20,000 / 160,000 | 41.258 ms | 1.484 ms | 36,680,016 B | 3,800,051 B |
| 50,000 / 400,000 | 121.782 ms | 3.498 ms | 91,700,016 B | 9,500,051 B |

At the largest size, preparation was about 35x faster and tracked peak
allocation was 9.65x smaller. Allocations fell from 1,750,046 to 11. Reference
time ranged 98.056–133.335 ms, packed time 3.474–3.617 ms.

The v3 payload was 16,100,033 bytes versus 61,500,014 bytes for the exact size of
the former string-repeating v2 layout: 73.8% smaller on this fixture. The
benchmark computes the old size formula; it does not retain a legacy codec.
Short identifiers, low-degree graphs, or many distinct edge types will have
different compression ratios.

## Non-serverless snapshot score reader

Fixtures request four distinct metrics over 20,000 rows. The reference recreates
the former bounded sorted-key reader with a per-batch key arena and complete
key construction per logical score. Production reuses encoded prefixes and a
key slab, and deduplicates physical rows. A synchronous mock transaction checks
key ordering and hashes keys; it does **not** model LSM/LMDB latency. Every output
cell is checked outside timing. Input fixtures and output arrays are excluded
from allocation tracking.

| Rows | Reference median | Physical reader median | Storage keys, before → after | Allocations, before → after |
| --- | ---: | ---: | ---: | ---: |
| All unique | 6.593 ms | 5.358 ms | 80,000 → 80,000 | 4,625 → 6 |
| Each node repeated twice | 7.180 ms | 3.916 ms | 80,000 → 40,000 | 4,625 → 7 |

The unique-row case reduced median reader CPU time by 18.7%, cumulative allocations
from 13,421,150 to 715,200 bytes, and tracked peak from 838,608 to 715,200 bytes.
The repeated-row case reduced median time by 45.5%, halved storage keys, and used
795,200 peak bytes. Distinct logical aliases also share physical reads, covered
by regression tests rather than included in this timing comparison.
The unique-row production run included a 26.419 ms outlier (minimum 5.300 ms);
the duplicate-row production range was 3.898–3.948 ms. Use repeated runs on an
isolated host for latency guarantees, not these development-host samples.

These are phase microbenchmarks, **not end-to-end query or PageRank speedups**.
Production additionally pays for I/O, authentication, snapshot/status handling,
output ownership and numerical work. Tests cover default non-serverless storage,
durable ordinal jobs, serverless publication/query integration, cancellation,
malformed input, allocation failures and alias ownership.

## Admitted preparation and ordinal execution

The following measurements were added with the bounded census, admission and
compact-query changes, on the same host and toolchain. They exercise production
functions against explicit former-path oracles, not alternate numerical kernels.

| Phase | Former path median | Current path median | Scope |
| --- | ---: | ---: | --- |
| Exhausted serverless projection preparation | 26.469 ms | 3.204 ms | 50,000 nodes, 400,000 edges; 16 rejected group attempts |
| Durable vector writer | 3.637 ms | 0.101 ms | 20,000 rows; synchronous mock storage |
| One-node score snapshot during rebuild | 194 µs | 103 µs | Real default storage; 256 active scan pages |
| 64-node score snapshot during rebuild | 638 µs | 553 µs | Same real-storage fixture |

The rejected-preparation case includes one packed source preparation, then 16
independent projection attempts against an exhausted work budget. The former
oracle constructs each projection before rejecting; production rejects before
projection allocations or census scans. This isolates admission ordering and
does not include the publication grouping/cache, fetch, rejection encoding, or
numerical kernel. Median time fell 87.9%; allocations fell from 123 to 11 and
cumulative allocated bytes from 35,600,691 to 9,900,323. Peak stayed 9,500,051 bytes
because the shared source preparation dominates it. This is not a claim that
rejection can avoid preparing the source itself.

The vector writer compares node-ID rows with rows already carrying their
job-local ordinals. Both execute the production writer and validate output
scores. Storage reads fell from 20,079 to 79, eliminating 20,000 dictionary point
lookups; writes remained 79. Allocations fell from 137 to 8 and tracked peak from
4,775,772 to 806,756 bytes. The roughly 36x writer CPU improvement excludes ordinal
discovery, real storage latency, adjacency reads, and numerical iteration. The
production reducer discovers ordinals with a canonical-node/dictionary range
join; the benchmark does not claim an equivalent whole-PageRank speedup.

The query fixture publishes degree scores over 4,096 nodes and 16,384 edges,
then opens a real 256-page rebuild. Both paths include a read transaction and the
same score reader; the reference builds operator status, while production reads
only publication/freshness metadata. Validation and result freeing are outside
timing. One-node median latency fell 46.9% (p95: 198 → 117 µs); 64-node median
fell 13.3% (p95: 662 → 640 µs). These are storage-level snapshots, not HTTP latency.
Runs overlapped development/test activity; rerun on an isolated host before
setting latency guarantees.

See [execution and resource ownership](../../docs/GRAPH_METRICS_EXECUTION.md)
for the associated admission, checkpoint, and integrity contracts.
