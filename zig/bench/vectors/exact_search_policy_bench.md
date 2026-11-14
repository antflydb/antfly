# Exact search policy benchmark

Measured on Darwin arm64 with a ReleaseFast build and the C allocator. These
are single-threaded internal production-scorer timings on resident fixtures,
not HTTP latency or disk-cold measurements.

The fixture uses seeded random unit vectors, 128 dimensions, and top-20. Each
mode executes 96 queries (four rounds of 24), alternating control/treatment
order. Eight query vectors have independently computed brute-force answers.
Every returned vector ID and distance is checked against those answers:
768 successful query checks across the two corpus sizes.

## Results

Times are milliseconds; lower is better.

| Vectors | Measurement | Control | Fixed | Result |
| --- | --- | ---: | ---: | --- |
| 4,096 | Filtered exact, decoded-cache cold | 21.817 | 23.499 | 7.7% slower |
| 4,096 | Filtered exact, repeated queries | 21.761 | 1.236 | 17.6× faster |
| 4,096 | Full-effort scan | 3.585 | 1.807 | 1.98× faster |
| 30,000 | Filtered exact, decoded-cache cold | 180.673 | 218.980 | 21.2% slower |
| 30,000 | Filtered exact, repeated queries | 177.197 | 12.908 | 13.7× faster |
| 30,000 | Full-effort scan | 39.580 | 21.865 | 1.81× faster |

Cold medians use the first query of each round (four samples per mode);
repeated-query medians use the remaining 92 samples. Full-effort medians use
all 96 samples per mode. These controls run in the same optimized binary.

For filtered exact scoring, control reproduces the previous policy: acquire
shared decoded residency but suppress its fills. Fixed enables those fills
while keeping request-local vector copies disabled. Both use the same
production exact-candidate scorer and admit the same residency capacity.
Decoded entries are invalidated before each round, outside the timer. Timers
include lease acquisition, query execution, and session teardown; correctness
checks are outside the timer.

The cold cost is deliberate cache population, not a cold-query speedup.
Repeated queries amortize it. Corpus-wide HBC scans retain their separate
policy and never populate shared decoded residency.

For full-effort scans, control disables bounded-source screening; fixed enables
it. Both read the same immutable float16 source with lossless residuals. Across
96 queries at 30,000 vectors, control exact-scored 2,880,000 vectors. Certified
search exact-scored 19,788 and certified-pruned 2,860,212 (99.31%). Both returned
the exact brute-force top-k and scores. This remains a linear projection scan.

## Reproduction

The opt-in test is in `storage/db/catalog/index_manager.zig`, named
`benchmark exact search cache and certified scoring`. It is skipped unless
`ANTFLY_BENCH_EXACT_POLICY` is set. From `zig/`, select it in the storage test
target with ReleaseFast optimization:

```sh
ANTFLY_BENCH_EXACT_POLICY=1 ANTFLY_BENCH_EXACT_VECTORS=30000 \
ZIG_GLOBAL_CACHE_DIR=.zig-cache/global \
zig build unit-storage-test -Doptimize=ReleaseFast \
  -Dtest-filter='benchmark exact search cache and certified scoring'
```

Set `ANTFLY_BENCH_EXACT_VECTORS=4096` for the smaller case. The output lines
`exact_policy_bench` and `exact_scan_bench` contain timings in nanoseconds,
query counts, screening counts, and the brute-force-check result. Do not set
latency thresholds in correctness tests; machine load affects timings.
