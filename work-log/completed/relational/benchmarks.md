# Relational Mode: LSM Benchmark History

> Relocated verbatim from `zig/RELATIONAL.md` (lines 452–513, 565–575, 655–704, 727–747, 749–774, 791–816, 1251–1269 at commit 271838a195) on 2026-09-16 during the documentation cleanup. This is a historical implementation log kept for context; the living design is [`RELATIONAL.md`](../../../zig/RELATIONAL.md). Durable decisions from this log were folded into that document before the move.

## Write path: bound-scan, dense nested-predicate, and point-projection-lease benchmarks

> From the `### Write path` section (lines 452–513).

The bound-scan benchmark (`--test-filter 'relational columnar bound scan benchmark'`)
compares forced primary scans with the hybrid column path on 768 rows with an
8 KiB unselected string, a scalar predicate, and a small nested JSON column.
Seven measured rounds follow a warmup, alternating execution order. Local
ReleaseFast arm64 macOS medians (milliseconds, primary → hybrid):

| Output / selected rows | LMDB | LSM |
| --- | ---: | ---: |
| Full document / 8 | 11.960 → 0.932 | 12.900 → 2.924 |
| Nested projection / 8 | 15.256 → 0.784 | 12.596 → 2.165 |
| Hyphenated field / 8 | 15.112 → 0.686 | 12.610 → 2.200 |
| Full document / 768 | 96.194 → 33.826 | 85.459 → 35.860 |
| Nested projection / 768 | 46.182 → 23.497 | 46.614 → 25.132 |

Selective full output reads eight primary rows; positive projections read none.
Each hybrid scan builds one schema plan across seven blocks. Dense full output
uses sequential primary ranges without reading column payloads. The hybrid path
trades bounded block workspace for less row decoding: selective nested scans
allocate 138/310 kB (LMDB/LSM), versus 27 kB for the streaming primary baseline;
dense full output allocates about 6.9/7.0 MB versus 35.8 MB. These are fixture
measurements, not universal latency guarantees or timing-based test gates.

The dense nested-predicate benchmark exercises 512 rows whose JSON column has
a matching scalar and an unselected 2,049-element array (~4 KiB per row).
Five measured rounds follow a warmup. On the same local ReleaseFast arm64 macOS
setup, compared with `b021b89de` before selection reuse/borrowed materialization:

| Backend | Median milliseconds, before → after | Cumulative allocated bytes, before → after |
| --- | ---: | ---: |
| LMDB | 81.230 → 29.733 | 263,149,212 → 82,092,620 |
| LSM | 78.278 → 31.151 | 275,850,442 → 102,463,108 |

The subsequent lazy-JSON read path indexes only visited containers' raw child
spans, caches requested scalar/subtree values, and shares navigation between
column predicates and projection. Numeric array lookup caches only the requested
position; whole selected JSON columns are emitted directly. Escaped object keys,
exact number lexemes, dotted array fanout, JSON pointer indices, missing/null
values, and ordered projection replacement retain their existing semantics.
Navigation still scans skipped bytes; it avoids their DOM/string allocations,
not the need to locate their boundaries. The same fixture measured 21.440/23.072
ms and approximately 3.1 MB allocated on LMDB/LSM, versus 29.733/31.151 ms and
82.1/102.5 MB immediately before this change. A deterministic allocation test
projects a leaf and the last index of a 131,073-element array using 16 KiB of
scratch, with no index allocation proportional to the array length.

`--test-filter 'relational point projection lease benchmark'` alternates seven
measured rounds after warmup on a 1 MiB row with one selected integer. It measures
64 warmed store probes with the same compiled projection (milliseconds):

| Backend | Copied + full verification | Leased + full verification | Leased + selected-group verification |
| --- | ---: | ---: | ---: |
| LMDB | 123.189 | 118.829 | 7.101 |
| LSM | 7.418 | 5.214 | 5.241 |

LSM already authenticates its values; its last two modes deliberately follow
the same path. The LSM baseline uses the ordinary owning probe, while the other
modes explicitly request a short value lease. A separate integrity-bypassing
**diagnostic only** measured 6.008 ms on LMDB, motivating grouped checks rather than accepting the remaining
full-row verification cost. Public `DB.lookup` request-allocator traffic is 209
bytes on both backends. This excludes backend/cache allocations and must not be
interpreted as total allocation for the operation; timing above isolates
store/projection work, not request/network cost.

## LSM ownership and physical amplification: output-partitioning benchmark

> From the `### LSM ownership and physical amplification` section (lines 565–575).

The original output-partitioning benchmark kept a reader pinned while performing
16 metadata commits beside 1 MiB of incompressible, unchanged payloads. It uses
the real SST/WAL encoders on memory-backed files, not logical value counters:

| SST partitioning | Additional SST bytes written | Retained file bytes |
| --- | ---: | ---: |
| First-byte only | 8,474,322 | 9,534,372 |
| Payload family | 1,066,714 | 2,127,336 |

Those historical measurements cover output splitting, not domain-aware input
selection, and measure write amplification rather than device latency.

## Shared LSM read versions: mutable-snapshot rotation benchmark

> From the `### Shared LSM read versions and bounded column batches` section (lines 655–704).

Development-host measurements for this redesign:

| Fixture | Before | After |
| --- | ---: | ---: |
| 40,000 single-row writes, 8-byte values, guarded, median | 1.722 s | 0.735 s |
| 8,192 narrow keys / 32 snapshot setups, median | 95.595 ms | 0.003 ms |
| Descriptor bytes copied by those snapshots | 16 MiB | 0 |
| 24 unique-key insert/delete generations after maintenance | Retained delete entries | 0 SSTs |

The write fixture uses ReleaseFast, three samples, memory-backed real WAL/SST
encoding, a 64 MiB byte guard and no intermediate row-count flush. Accounting
alone measured 0.666 s; the preceding shared-root implementation measured
0.691 s. Atomic successor preparation and allocation ownership add about 6%
over that implementation in this narrow-write fixture. The snapshot fixture
uses ReleaseSafe and five alternating samples;
it measures root pin/release and point lookup, not end-to-end request latency.
The churn regression keeps an old reader during GC, reopens the store repeatedly,
and verifies old/current visibility and physical reclamation (539 bytes peak
retained files for this tiny fixture). These are diagnostics, not timing gates
or a universal physical-to-live-byte bound.

The subsequent atomic-publication/allocation-accounting change measured
17,645,096 charged bytes for 17,645,096 allocated bytes in a 4,096-row, 4 KiB,
32-epoch diagnostic (previously 574,603,456 charged for 17,981,488 allocated).
For 100,000 narrow keys, allocated memory fell from 25,064,072 to 17,607,304
bytes. Root handoff performs zero allocations/frees and no tree traversal;
the previous rotation took about 45 ms in that isolated fixture. These measure
allocator-requested bytes, not process RSS or end-to-end request latency.
The final three write samples were 0.745, 0.735 and 0.735 s. Shared-host timing
is diagnostic, not a gate or a general throughput claim.
The production physical-churn fixture retained its previous write reduction:
about 17.28 MB SST output at the default density threshold versus 29.31 MB
with eager standalone GC; WAL output remained 10.09 MB in both cases.

The native production-shaped churn fixture compares eager standalone GC with
the 50% trigger using otherwise identical primary options (ReleaseFast, one
sample per policy, 16 overwrite batches followed by deleting half the rows):

| GC policy | SST bytes written | Peak SST+WAL | Settled SST+WAL |
| --- | ---: | ---: | ---: |
| Eager | 29,307,851 | 8,265,193 | 2,768,258 |
| 50% trigger | 17,283,948 | 8,529,796 | 3,055,160 |

The threshold avoids about 41% of eager-GC SST writes while retaining about
10% more settled bytes in this fixture. WAL writes are identical (10,093,136
bytes). Batch median/max times were 23.115/23.956 ms and 22.826/23.977 ms;
these single-run timings do not establish a latency improvement. Both policies
validate pinned-reader visibility, payload ownership and an integer projection
reading 1,050 payload bytes with zero primary-row reads.


## Shared LSM read versions: shared/domain/batched churn benchmark

> From the `### Shared LSM read versions and bounded column batches` section (lines 727–747).

New differential fixtures use diagnostic-only controls, not alternative
production formats or legacy compatibility modes. A ReleaseFast development
run before merging main's accelerated checksum implementation measured:

| Fixture | Baseline | Shared/domain/batched |
| --- | ---: | ---: |
| Two-sided metadata churn, additional SST bytes | 16,957,275 | 20,625 |
| 256 SSTs / 64 point reads, median milliseconds | 3.390 | 1.151 |
| Topology builds for those 64 point reads | 64 | 0 after warmup |
| 32-column projection, SST block loads | 78 | 52 |
| 32-column projection, SST block bytes | 461,515 | 259,199 |
| 32-column projection, median milliseconds | 6.204 | 4.376 |
| 1,024 × 4 KiB staging, copied snapshot bytes | 42,308,576 | 2,485,568 |

The two-sided fixture uses actual SST/WAL encoders on memory-backed files and
keeps an old reader pinned. The projection fixture uses native files with local
and shared decoded-block caches disabled; OS page-cache state is unspecified.
The staging fixture uses the production 32 MiB threshold and compares deep
versus shared snapshots with the same build-snapshot reuse in both modes. Its
100.337/95.703 ms sample shows that the large byte reduction is not an equivalent
end-to-end throughput multiplier. Timings are diagnostics, not regression gates.

## Shared LSM read versions: native-file domain-selection churn benchmark

> From the `### Shared LSM read versions and bounded column batches` section (lines 749–774).

The native-file churn fixture uses production primary options (only the
obsolete-file grace period is set to zero for deterministic reclamation), pins
an old reader, performs 16 batches of overwrites, releases the reader, deletes
half the rows, and validates column ownership and projected reads. It reports
actual active/obsolete SST file sizes plus retained WAL, cumulative SST/WAL
writes, and foreground batch median/max latency. It checkpoints at measurement
boundaries and is not a concurrent-load or device-cold benchmark. Comparing the
previous commit's output-only partitioning with domain selection measured:

| Native churn | Output-only | Domain selection |
| --- | ---: | ---: |
| Cumulative SST bytes written | 20,745,889 | 16,248,094 |
| Cumulative WAL bytes written | 10,093,136 | 10,093,136 |
| Peak SST+WAL bytes, reader pinned | 6,707,877 | 8,868,177 |
| Settled SST+WAL bytes | 3,095,189 | 3,097,601 |
| Foreground batch median / max, ms | 32.555 / 34.283 | 32.904 / 36.064 |

Both read 1,050 column payload bytes for the final integer projection with zero
primary-row reads. Domain isolation reduced total SST writes by about 22%, but
did not improve foreground write latency in this sample and increased pinned
peak disk usage by about 32%; settled usage was nearly unchanged. Timings were
collected on a development host with other compilation work and are not
isolated-machine latency claims. Independent domains change compaction geometry;
less rewriting is not a universal peak-footprint bound. Long readers still need
retention limits and operational disk headroom. No fixed physical-to-live-byte
ratio is inferred from the logical churn tests.

## Shared LSM read versions: wide-metadata/decoded-reuse payload benchmark

> From the `### Shared LSM read versions and bounded column batches` section (lines 791–816).

Reproducible focused benchmarks (Zig 0.16, ReleaseFast, arm64 macOS):

```sh
zig build lib-storage-test -Doptimize=ReleaseFast -- \
  --test-filter 'relational columnar wide metadata allocation benchmark' \
  --test-filter 'relational columnar decoded reuse benchmark'
```

The payload fixture scans 128 rows containing 64 KiB strings, with either one
shared value or 128 distinct values. Both variants use a nonmatching typed term
predicate, no document materialization, and a 512 KiB cache budget. Nine measured
rounds follow a warmup, alternating cache-off/on order. One local run measured:

| Backend / values | Median ms, off → on | Payload decodes, off → on |
| --- | ---: | ---: |
| LMDB / shared | 1.780 → 0.431 | 8 → 1 |
| LSM / shared | 3.136 → 1.604 | 8 → 1 |
| LMDB / distinct | 23.936 → 23.842 | 128 → 128 |
| LSM / distinct | 50.211 → 50.145 | 128 → 128 |

Peak retained cache bytes were 68,756 for shared values and 462,860 for distinct
values. The one-row, 1,024-column existence-filter fixture allocated 3,337,310
bytes/scan after lazy read state, versus 20,308,708 with the same fixture before
the change. Timings are workload-specific and sensitive to machine load; CI
asserts exact results, allocation budgets, reuse counts, and ownership cleanup,
not latency thresholds. These are not disk-cold or concurrent-query benchmarks.

## LSM metadata epochs: persistent-directory and lazy-cursor scaling benchmark

> From the `## LSM metadata epochs and bounded scan sources` section (lines 1251–1269).

The checked-in ReleaseFast scaling fixture measured the following on an
Apple Silicon development host (three samples; not an end-to-end latency SLA):

| Lower-level SSTs | Shared directory bytes | One-run update, median | Off-lock projection, median | Per-cursor bookkeeping |
| ---: | ---: | ---: | ---: | ---: |
| 1,000 | 628,888 | 2 µs | 46 µs | 512 bytes |
| 10,000 | 6,263,752 | 1 µs | 404 µs | 512 bytes |
| 100,000 | 62,604,760 | 3 µs | 4.97 ms | 512 bytes |

The previous cursor layout required 23,200,232 bytes at 100,000 SSTs. The new
directory adds shared metadata memory (about 626 bytes/SST in this fixture),
charged to the resource manager, in exchange for cheap epoch pins and avoiding
per-reader metadata clones. The physical churn benchmark remained at roughly
23.6–23.7 ms median per batch; SST bytes written were 29.3 MB with a zero-density
GC threshold and 17.3 MB with the 50% threshold. This validates retention of the
existing churn behavior, not a new SST write-amplification improvement from the
directory itself. Both benchmarks are reproducible via `lib-storage-test` with
`-Doptimize=ReleaseFast` and filters `persistent directory and lazy cursor scaling
benchmark` and `production LSM physical churn benchmark`.
