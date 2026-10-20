# Bloom filter experiment

Isolated worktree: `.worktrees/bloom-experiment`.
Branch: `perf/blocked-bloom-experiment`.
Base: `70fb8dd0ba9b5fd2f6477dd2bc8d91adf4148b3a`.

This experiment compares the current Antfly Bloom filter with the blocked
[one-hash design](https://calabro.io/ohbf) and faster alternatives. The only
production change is a bit-mask fast path for existing power-of-two filters.
Blocked layouts remain experimental; their different bit mapping would require
a versioned persisted format before adoption.

## Implementations

- `baseline`: a verbatim snapshot of the original `lib/bloom/src/mod.zig`.
- `optimized`: the actual modified production module. Replaces modulo with a
  mask for power-of-two filters and advances the hash with wrapping addition.
  Falls back to the existing mapping for other filter sizes. Existing bytes,
  encoding, false positives, and memory requirements are unchanged.
- `ohbf`: 64-byte blocks, seven pairwise-coprime partitions of
  67/71/73/79/83/89/50 bits, early exit on a missing bit. Block selection uses
  multiply-high range reduction; partition moduli are compile-time constants.
- `ohbf_full`: the same mapping, with all probes evaluated before returning.
- `split`: 32-byte blocks with eight 32-bit lanes, salted multiplication for bit
  positions, and branchless membership evaluation. This uses the split-block
  construction and salts from the [Parquet Bloom filter specification](https://parquet.apache.org/docs/file-format/bloomfilter/).
- `split64`: an experiment extending those eight lanes to 64 bits each, reducing
  block-load variance while keeping eight probes. This is not a Parquet format.

The standalone harness aligns its bit arrays to 64 bytes. The storage
prototype supports unaligned serialized buffers, as described below. No atomic
instructions or mutexes are used: these represent privately built, immutable
filters.

## Method

The canonical runs use Zig **0.17.0**, `ReleaseFast`, on macOS ARM64. See
`environment.txt`. Initial exploratory Zig 0.16 results are excluded.

Each configuration inserts 10,000, 1,000,000, or 8,000,000 keys and checks **every
inserted key** for false negatives. Membership timings use one million absent
queries and one million randomized present queries, one warmup, and seven timed
repetitions. Each CSV records the median and range; `aggregate.csv` takes the
median of those medians across three process runs. Counts are consumed to
prevent dead-code elimination.

`prehashed` measures membership with hashes already available. `term64` includes
one Wyhash pass over a 64-byte key and the same splitmix finalizer used by the
text-index hash derivation. Both use the same hash pairs and datasets. Query
records have identical sizes and access patterns across implementations. Keys
and timings are synthetic; these are not complete search-query measurements.

The baseline requests 14 bits/key and 10 probes, matching the LSM default. Its
power-of-two rounding allocates 26.2144 bits/key at 10,000 keys and 16.777216
bits/key at the two larger sizes. Experimental layouts are evaluated at 100%,
110%, 125%, 150%, and 200% of **actual baseline allocated bytes**, rounded to a
block boundary. Compare both equal-memory and equal-or-lower measured
false-positive configurations. Fixed-size blocked filters can require more
memory for equivalent false-positive rates.

The build timing includes deterministic key construction/hash generation and
insertion; it is explicitly named `build_with_keygen_ns_key`. It does not
isolate filter insertion cost. Tests additionally exercise blocked membership
and false positives on three independent datasets at four smaller capacities.

The host was concurrently running unrelated builds and tests. Our timed runs
do not overlap our own compilation. Timing ranges are retained, and repeated
results should be treated as local evidence rather than precise deployment
predictions. The false-positive samples are deterministic and reused across
rounds: repeating timing does not increase the statistical sample size.

## Actual storage workload

The existing LSM benchmark gains `--in-range-misses`. Its original `miss:...`
keys lie outside the `doc:...` range and can be rejected before consulting a
Bloom filter. The new option appends `:missing` to existing document keys,
placing almost all absent keys between adjacent stored keys. Default benchmark
behavior stays unchanged. The output records whether this option is enabled.

The script builds baseline and optimized storage binaries, then runs 20,000
keys, three samples, cache enabled/disabled, memory-backed storage, three hit
passes, and ten miss passes. Other operations remain as correctness/smoke
coverage. It reverses baseline/optimized order on alternate rounds. Inspect
`read_run_probes` and `read_bloom_negatives` to confirm the filter is reached.
The initial storage comparison tests the compatible production optimization.
A separate three-way run also tests `split_module.zig` in the actual backend.
This drop-in experimental module uses eight 32-bit lanes and approximately 130%
of the original allocated bits, rounded to 256-bit blocks. It uses little-endian
unaligned loads/stores, as serialized and ordinary allocated filters are not
necessarily cache-line aligned. It has a distinct `ABLOOM2` magic/version and
rejects existing `ABLOOM1` data; it is only for disposable benchmark databases.
The production module is restored after building. This is a storage experiment,
not a proposed persisted-format migration.

## Reproduce

From this worktree's repository root:

```sh
python3 zig/experiments/bloom/run.py --zig /path/to/zig-0.17.0/zig --rounds 3 --storage
python3 zig/experiments/bloom/analyze.py

# Additional three-way storage comparison, keeping the microbenchmark CSVs:
python3 zig/experiments/bloom/run.py --zig /path/to/zig-0.17.0/zig --rounds 3 --blocked-storage --skip-micro
```

`run.py` temporarily substitutes the baseline Bloom module when building the
baseline storage executable and restores the optimized module in a `finally`
block. Run it in this isolated worktree, without concurrent edits to that
module. It runs the production and experimental tests before benchmarking.
Raw CSVs, storage output, and `run.log` are kept beside the harness. Generated
binaries are ignored.

## Results from three canonical microbenchmark rounds

The following includes 64-byte key hashing. False-positive rates are observed
from one million absent queries, not theoretical estimates or confidence bounds.

| Keys | Implementation | Filter memory vs baseline | Miss ns | Hit ns | Observed false positives |
|---:|---|---:|---:|---:|---:|
| 1,000,000 | Baseline | 1.00x | 20.475 | 14.719 | 0.0347% |
| 1,000,000 | Compatible mask fast path | 1.00x | 17.634 | 14.588 | 0.0347% |
| 1,000,000 | Article OHBF | 1.25x | 13.883 | 11.288 | 0.0309% |
| 1,000,000 | Split 32-bit lanes | 1.25x | 4.602 | 4.843 | 0.0315% |
| 8,000,000 | Baseline | 1.00x | 36.797 | 38.908 | 0.0341% |
| 8,000,000 | Compatible mask fast path | 1.00x | 32.850 | 24.533 | 0.0341% |
| 8,000,000 | Article OHBF | 1.25x | 26.666 | 32.135 | 0.0253% |
| 8,000,000 | Split 32-bit lanes | 1.50x | 17.885 | 18.385 | 0.0119% |

At one million keys, split blocks make absent membership checks 4.45x faster
including key hashing, versus 1.47x for the article's OHBF. At eight million
keys the corresponding speedups are 2.06x and 1.38x. The compatible fast path
improves absent membership checks by about 12–16% including hashing; this is a
component measurement, not a demonstrated database-wide gain.

At equal allocated bytes, blocked filters have more false positives. For
example, at one million keys, the split filter's 0.1010% false-positive rate
is about 2.9x the original 0.0347%. That difference must not be hidden when
comparing filter speeds. Wider split lanes reduce the memory penalty needed
for a given false-positive rate, but did not beat the fastest 32-bit lane
configurations on this machine. Tiny filters have very few false positives in
the sample, making close rate comparisons statistically uncertain.

The storage benchmarks show substantial dilution of the filter-only gains.
The 20,000-key three-way comparison measured median absent reads of 131.00 ns
baseline versus 120.01 ns split with the cache disabled (8.4% less time), and
284.34 ns versus 273.69 ns with it enabled (3.7% less time). At one million
keys the corresponding medians were 258.27 vs 247.62 ns (4.1%) and 439.49 vs
437.85 ns (0.4%). Large timing ranges overlap. Hit-read results are mixed.
The compatible mask fast path has no consistent storage-level improvement
across the small and large runs. These results support a faster filter and a
possible modest absent-read improvement, not a broad throughput claim.

The additional million-key workload is reproducible with:

```sh
python3 zig/experiments/bloom/large.py
```

It uses one sample per process/cache mode, one hit pass, three miss passes, and
three process rounds with rotated implementation order. See `large-*.txt` and
`large-aggregate.csv`; no compilation overlaps these measurements.

## Additional single-hash storage experiment

`one_hash.py` builds a fourth storage executable using the split-block module
and one Wyhash input pass plus the text index's splitmix finalizer, instead of
two independent seeded Wyhash passes. This covers exact-key and prefix Bloom
hashing. Its distinct `ABLOOM3` marker identifies the changed hash mapping.
`one-hash-table.patch` shows the exact temporary table-file changes. Both
production source files are restored after building, including on build failure.

```sh
python3 zig/experiments/bloom/one_hash.py --zig /path/to/zig-0.17.0/zig
python3 zig/experiments/bloom/large.py --one-hash
python3 zig/experiments/bloom/analyze.py
```

This compares baseline, split with the original two hashes, and split with one
input hash over three rotated million-key storage rounds. The document keys
are short and namespace-qualified; the 64-byte synthetic key microbenchmark
is a different workload. Read hits and reopen operations check actual data
retrieval with the experimental mapping. Absent queries check for unexpected
results. All databases are disposable and each format is used only by its
matching executable.

The single-hash storage comparison measured absent reads at 268.45 ns baseline,
223.72 ns split with two hashes, and 258.92 ns split with one hash with the cache
disabled. With it enabled, the medians were 488.52, 430.14, and 473.12 ns.
Ranges overlap, and the one-hash variant did not improve on the original-hash
split variant. Fewer input hash passes alone is not a reliable speedup for
these short namespace-qualified keys. All variants passed the workload's
hit/miss/reopen checks.

For the baseline and split variants shared by both million-key series,
`large-combined-aggregate.csv` pools six samples per cache mode. Missing-read
medians are 261.955 vs 232.340 ns without the cache (11.3% less time), and
457.955 vs 433.995 ns with it (5.2% less time). This is a descriptive summary of
both complete series; the individual series above and raw timing ranges show
how much host noise changes the apparent gain. It is not evidence of an
11% overall Antfly throughput increase. The storage split variant uses about
30% more filter memory, rather than the 25% used in the selected standalone
million-key row.

The narrow production candidate is the format-compatible mask fast path.
The stronger filter-only candidate is the split-block layout, whose storage
adoption needs a deliberate versioned-format/migration design and repeatable
end-to-end results on a quieter machine. The single-hash storage derivation
is retained as a negative experimental result, not enabled in production.

## Bit-mask change review

See [REVIEW.md](REVIEW.md) for the correctness argument, additional differential
coverage, validation commands, and the performance limitations.
