# Graph metric execution and query benchmarks

Measured 2026-09-07 on Apple M4 Max, 36 GiB RAM, macOS 26.3.1,
Zig 0.16.0, ReleaseFast, using the system SMP allocator. One warmup and five
measured samples per case; tables report medians. This was a shared development
host, not an isolated benchmark machine.
The compact-query comparison below uses 21 measured samples instead of five.

## Durable cross-job topology reuse

After increasing default scheduling spans to 4,096 records, the same fixture
and command measured the following on 2026-09-08:

| Complete numerical job | Physical edge records read | Checkpoints | Median time (range) |
| --- | ---: | ---: | ---: |
| Independent topology | 32,768 | 36 | 1.612 s (1.415–1.887 s) |
| Shared topology | 0 | 22 | 0.834 s (0.811–0.892 s) |

The current shared case is 1.93× faster on this fixture. Against the earlier
small-page run below, independent/shared checkpoint counts fall 638 → 36 and
110 → 22. Those work counts are directly checked. The historical wall times
were not collected in a controlled same-run comparison: storage compaction and
other activity on this shared host can materially change elapsed time. This
benchmark calls the low-level numerical runner; it does not measure concurrent
task admission or HTTP latency.

### Historical small-page baseline

Measured 2026-09-08 with:

```sh
zig build graph-metric-preparation-bench -Doptimize=ReleaseFast -- --topology-only
```

The real default-storage fixture has 1,024 nodes and 16,384 directed edges,
with 16 neighbors per node. A first PageRank job prepares topology; a differently
configured PageRank job then executes one numerical iteration. The independent
reference forces a reuse-directory miss; the shared case adopts the sealed
owner. Both use the same implementation, numerical seed, and graph, and verify
every published score equals `1/1024`. Six samples per case, first discarded:

| Complete numerical job | Physical edge records read | Worker/coordinator checkpoints | Median time |
| --- | ---: | ---: | ---: |
| Independent topology | 32,768 | 638 | 79.346 s |
| Shared topology | 0 | 110 | 18.573 s |

This is a 4.27× median improvement on this fixture, with 82.8% fewer checkpoints.
Times include planning, initialization, numerical reduction, publication and
job cleanup. They exclude fixture writes, score verification, and maintenance
of retired score generations and topology owners between samples. The reference
also includes the constant-size transaction forcing a directory miss. Sample
ranges were 58.172–105.075 s and 17.436–19.115 s. Compilers were running on the
shared development host; a profile of an independent build showed substantial
native LSM compaction work. These times are not an isolated-host throughput
claim, and longer numerical runs amortize preparation over more iterations.

Lifecycle tests additionally cover HITS-to-PageRank/eigenvector adoption,
producer cleanup and reopen, filter-set canonicalization, independent concurrent
producers, generation pins, deleted filters/metrics, bounded crash-resumable
reclamation, and rejection of retirement tasks targeting winning packed tiles.

## Adaptive decoded-score joins

Measured 2026-09-08 with
`zig build graph-metric-preparation-bench -Doptimize=ReleaseFast -- --score-join-only`.
The fixture borrows one decoded 1,024-row block and verifies exact results for
each candidate count. Six samples, first discarded, 4,096 repetitions per sample:

| Candidate rows | Binary reference | Adaptive join | Improvement |
| --- | ---: | ---: | ---: |
| 1 | 75 ns | 75 ns | unchanged |
| 16 | 431 ns | 434 ns | within 1% |
| 256 | 16.425 µs | 10.959 µs | 1.50× |
| 1,024 | 84.884 µs | 21.394 µs | 3.97× |

Sparse requests retain binary search; dense candidates merge against sorted
scores. These timings exclude decoding, allocation, authentication and I/O.
Separate parity tests include duplicate, missing and invalid row ordinals.

## Shared admission, sparse work, and publication checkpoints

Measured 2026-09-08 on the same host/toolchain with
`zig build -Doptimize=ReleaseFast graph-metric-preparation-bench --summary all`.
One warmup and five measured samples; medians below. Development tests were
running on this shared host. These are bounded phase measurements, not promises
about whole-build, HTTP, or cloud-network latency.

| Fixture | Reference | Current | Durable work / admission |
| --- | ---: | ---: | --- |
| Publish 8,192 scores, real default storage | 423.880 ms | 96.506 ms | 128 → 2 score/staging/cursor commits |
| 100 sequential 80-entry routing working sets | 0.968 ms | 0.226 ms | 8,000 → 80 cache fills |

Publication compares 64-node and 4,096-node batches using the same current
producer helper. IDs are short, so the 1 MiB node-ID budget does not truncate
either case. Each sample opens fresh storage. Timing includes ordered staging,
primary scores, checkpoint cursor commits, and full primary-score verification;
it excludes numerical computation, worker page fencing, and final top-K merging.
Ranges were 419.238–430.928 ms and 95.571–97.751 ms. Actual production batches
also stop at their partition boundary or byte allowance; the fixture is not an
end-to-end 64× speedup claim.

The routing fixture uses the production cache and equal-size 4 KiB payloads.
A 64-entry-equivalent byte limit models the previous residency ceiling; the
current case allows 1 MiB. Leases are released sequentially. It allocates/fills
owned cache entries but excludes codec decoding, network I/O, and query planning.
Allocation count falls from 16,000 to 160, cumulative allocated bytes from
34,688,000 to 346,880. Tracked peak rises from 281,840 to 346,880 bytes because the
whole working set is retained, still below the allowance. Fixed inline cache
buckets are not heap allocations and are excluded from those byte figures.
Ranges were 0.944–0.987 ms and 0.222–0.227 ms. Separate regression tests retain all
80 leases simultaneously and exercise page-vs-metadata eviction under pressure.

The sparse stateful regression has 4,097 dictionary nodes but only three metric
members in two original leaves. Each later node phase now schedules two data
pages instead of 65; normalization schedules two leaves instead of 65. Empty
iteration-zero node pages receive no worker attempt. The test reopens at iteration
one and compares PageRank, eigenvector, and paired HITS output against the numeric
oracle. These are asserted work counts, not a timing benchmark. Original ordinal
identities remain unchanged.

The sealed-vector gather fixture still fetches only 128 storage chunks across
256 checkpoints (reference: 32,768). Its median is 16.888 ms, with 604,180 tracked
peak bytes. Cache entries and bucket allocations now draw from a shared 64 MiB
process pool, rather than multiplying a full allowance by every populated index.
Failure-injection and retirement tests verify that optional admission failures
and retired metrics release their charged bytes.

## Shared point planning and checkpoint-local folds

Same host/toolchain, one warmup and five samples, using the production helpers:

| Phase / scenario | Allocating reference median | Current median | Tracked heap peak, before → after |
| --- | ---: | ---: | ---: |
| Warm ordinal fold, 4,096 tiles / 1,048,576 edge visits | 7.496 ms | 6.230 ms | 12,288 → 0 bytes |
| Point row planning, 100,000 common-prefix IDs / 1 column | 17.537 ms | 3.622 ms | 1,600,000 → 1,200,000 bytes |
| Point row planning, 100,000 common-prefix IDs / 16 columns | 290.126 ms | 8.552 ms | 25,600,000 → 1,200,000 bytes |
| Point row planning, 100,000 hashed IDs / 1 column | 4.178 ms | 2.517 ms | 1,600,000 → 1,200,000 bytes |
| Point row planning, 100,000 hashed IDs / 16 columns | 70.889 ms | 4.672 ms | 25,600,000 → 1,200,000 bytes |

The fold fixture repeats a 256-edge tile with a warm source-vector chunk and
one target accumulator. Both paths perform the same compensated addition order
and return exactly equal sums. The reference owns decoded edges, source slots,
gathered ranks and contribution rows, using the same topology validation as the
borrowed path. Current execution also replaces per-edge target hash lookups with
a bounded chunk-local slot table. Across these edge visits, 16,384 allocations
and 50,331,648 cumulative allocated bytes become zero. Fixed stack scratch and
fixture/cache residency are **not** zero memory: fixture allocations are excluded
from tracking, while constant fixture setup is included in wall time. Storage
reads, cold cache fills, checkpoint commits and whole-build execution are excluded.
Measured ranges were 7.360–8.155 ms versus 6.011–6.528 ms.

Point fixtures use 391 routing blocks, deterministic permuted row order, and
either 30-byte collection-prefixed IDs or 16-byte hashed hexadecimal IDs. Each
path verifies the same row/block checksum. The reference retains every column's
16-byte row map. The new path admits 8-byte transient comparison keys plus one
4-byte shared permutation; the keys are freed before column preparation, leaving
only 400,000 bytes of row-mapping ownership regardless of column count. It uses
two allocations versus one per reference column. Integer prefix keys improve
both tested single-column cases; sorting full strings alone regressed hashed
single-column IDs and was not retained.

These are **sequential row-planning phase** measurements, not parallel column
execution or end-to-end query latency. They exclude output cells, control/routing
ownership, materialized block spans, score decoding, cache and network work.
Common-prefix single-column ranges were 17.273–18.224 ms versus 3.580–4.352 ms;
16-column ranges were 282.258–295.582 ms versus 8.319–8.814 ms. Hashed-ID ranges
were 4.078–4.624 ms versus 2.495–2.569 ms and 70.246–73.654 ms versus
4.133–4.898 ms. Sparse candidates, duplicate IDs, key lengths and shared-prefix
collisions change the balance; no universal speedup is claimed.

## Initialization and query ownership follow-up

Same host/toolchain, one warmup and five measured samples:

| Phase / scenario | Reference or cold median | New or warm median | Allocations, before → after |
| --- | ---: | ---: | ---: |
| Membership discovery, 64 nodes / 16,384 producer partials | 2.063 ms | 0.069 ms | 16,398 → 90 |
| 64 authenticated 64-KiB disk hits versus warm memory leases | 5.838 ms | 0.009 ms | 64 → 0 request-payload allocations |
| Top-K response conversion, 10,000 IDs of 4,096 bytes | 3.169 ms | 0.021 ms | 10,001 → 1 |

The membership reader is now used by vector initialization as well as iterations,
convergence and publication. This measures discovery and dictionary validation,
not a whole initializer, vector writes or a complete build. Maximum fan-in is a
deliberate stress case; fewer producer duplicates reduce the benefit.

Cache measurements use warm filesystem data in both cases. The cold-memory case
includes verification and promotion; clearing memory between lookups is excluded.
It is a cache-state comparison, **not an exact pre-change implementation**.
Allocation tracking covers request payloads, excluding cache-owned allocations.
Disk-hit times ranged 5.817–5.887 ms; warm leases ranged 0.006–0.010 ms across 64
lookups. Network and score decoding are excluded. A regression also verifies that
a warm leased hit succeeds with an allocator that rejects every allocation.

Response-conversion peak includes the still-resident input: 82,400,000 bytes for
copying versus 41,440,000 for ownership transfer. New cumulative allocation falls
from 41,200,000 to 240,000 bytes. Copy times ranged 1.227–3.237 ms, transfer times
0.019–0.022 ms. Input construction, cleanup, fetching and JSON serialization are
excluded. Long IDs stress the ownership boundary; ordinary shorter IDs benefit
less. These are phase measurements, not end-to-end latency guarantees.

## Sealed membership and output admission (initial measurements)

Additional measurements on the same host/toolchain (one warmup, five samples):

| Phase | Former path median | Current median | Allocations, before → after |
| --- | ---: | ---: | ---: |
| Canonical membership read, 64 nodes / 16,384 producer partials | 1.962 ms | 0.061 ms | 16,398 → 90 |
| Exhausted output quota, 50,000 nodes / 400,000 edges | 10.302 ms | 6.448 ms | 34 → 20 |

Membership uses real default storage and the same ordered dictionary validation
in both paths. It includes transaction and output ownership, but excludes fixture
writes and numerical folds. The fixture deliberately exercises maximum producer
fan-in: speedups will be smaller with fewer duplicate producer rows. Cumulative
allocation fell from 216,797 to 6,259 bytes; tracked peak increased slightly from
2,774 to 3,192 bytes. Times ranged 1.958–2.031 ms versus 0.060–0.063 ms.

Output rejection includes one source and projection preparation in both paths.
The reference computes and encodes PageRank before rejecting an exhausted output
quota; production rejects before numerical allocation or encoding. The symmetric
degree-eight ring can converge early (maximum three iterations); this does not
claim savings for three complete iterations. Cumulative allocation fell from
19,553,871 to 16,306,596 bytes, while peak remained 12,906,304 bytes because shared
preparation dominates. Times ranged 10.264–10.342 ms versus 6.437–6.481 ms.
Fetch, upload, rejection-sidecar encoding and cloud latency are excluded.

Metadata-tail skipping is checked as an operation-count regression: encountering
the metadata namespace issues one range seek regardless of the number of metric
records. No wall-clock speedup is claimed for that regression.

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

## Staged stateful metric queries

Run just this case with:

```sh
zig build graph-metric-preparation-bench -Doptimize=ReleaseFast -- --staged-only
```

The fixture seeds 16 published score columns in the default storage backend,
with 100,000 distinct node IDs. One metric orders the top ten rows; all sixteen
are projected. The eager reference loads every dependency before selection.
The staged path uses the production snapshot reader and stage workspace. Both
must return exactly the same selected ordinals and every projected score.

Final local ReleaseFast rerun (six samples, first discarded):

| Metric | Eager reference | Staged reads |
| --- | ---: | ---: |
| Logical score keys | 1,600,000 | 100,150 |
| Median execution | 17.575 s | 1.101 s |
| Tracked peak allocations | 29,233,056 B | 5,232,576 B |

The timer includes snapshot acquisition, score reads, selection, validation and
scratch cleanup. Fixture writes, traversal, response encoding and backend-owned
allocations are excluded. These are warm-cache results on a shared development
host with concurrent builds, not end-to-end latency guarantees. The reference
and current paths use the same byte-bounded physical score reader. The measured
work-count reduction is independent of host contention.

The preceding full benchmark run measured 21.108 s and 1.085 s respectively;
the difference between runs illustrates why the timing is a local measurement,
not a service-level guarantee. Both runs reported the same key counts and peak
allocation sizes.

## Sparse projection and resource-bounded iterations

ReleaseFast measurements on the same host/toolchain, with a prepared
1,000,000-entry dictionary and two selected edges (two active endpoints).
Each sample contains 256 repetitions; one warmup sample is discarded and the
median of five samples is reported. The inactive dictionary entries are fixture
placeholders; preparation, storage I/O, kernels and upload are excluded.

| Projection | Source-wide scratch reference | Active-endpoint path | Peak scratch, reference → current |
| --- | ---: | ---: | ---: |
| Degree | 795.582 µs | 0.179 µs | 8,125,065 → 73 bytes |
| PageRank | 830.437 µs | 0.218 µs | 4,125,097 → 89 bytes |

Node-ID and CSR checksums must match in every repetition. The dense degree
reference is the previous direct-count path; PageRank's reference retains the
former projected-edge copy (only 16 bytes in this fixture). These are deliberately
sparse phase measurements, not end-to-end speedups. Dense projections continue
to use linear-time maps/counts rather than sorting every edge endpoint.
Concurrent development builds were active; use isolated runs for latency
guarantees. The allocation and output-parity checks do not depend on timing.

Stateful iteration planning now eliminates later adjacency-producer pages
entirely. With 256 edge partitions and 100 iterations this removes 25,344
PageRank/eigenvector no-op page executions and at least 50,688 claim/completion
commits; HITS removes twice those counts. This is a deterministic work-count
comparison, not a measured storage-latency claim. Regression tests verify
absence of later producer pages, immutable adjacency reuse, recovery, retries,
publication barriers and numerical parity.

Serverless regressions also enforce two resource oracles: an 8,192-score prior
larger than 64 KiB can seed a sparse selection within a 64 KiB read/memory budget,
while dense seeds read less than the whole artifact by omitting ranked payloads;
and two concurrent requests for two cold routing pages perform exactly two
decoded fills even with 63 of the 64 fill slots occupied. The latter checks
shared lease identity and completion under fill-table saturation.
