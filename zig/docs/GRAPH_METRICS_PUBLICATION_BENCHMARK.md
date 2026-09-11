# Graph metric publication qualification

The opt-in fixture exercises a complete small-WAL serverless publication, not
only an in-memory graph/tree operation. Run from `zig/`:

```sh
ANTFLY_DOCUMENT_FACTS_BENCH=1 zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'publication qualification benchmark'
```

Set `ANTFLY_DOCUMENT_FACTS_BENCH_DOCS=1024` or `16384`, and optionally
`ANTFLY_DOCUMENT_FACTS_BENCH_DEGREE=1` or `1023`, to select a case. The benchmark
lives in `pkg/antfly/src/serverless/build/document_facts_publication_bench.zig`
and is not included in the production binary.

## Pending-work routing qualification

```sh
ANTFLY_DOCUMENT_FACTS_BENCH=1 zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'pending work index qualification benchmark'
```

The in-memory page-store fixture compares a complete facts scan with the new
per-stage pending cursor. Four pending documents follow a completed prefix.
Before each sample an unrelated completed document is updated and a new facts
root is published. The pending tree must retain its exact identity. Body blobs
deliberately do not exist: this isolates routing work and does **not** measure
enrichment, object-storage latency, or model throughput. One warmup and five
samples are used; the table reports the median full-scan sample and its paired
indexed measurement, locally on September 11, 2026.

| Documents | Pending | Full scan (µs) | Pending index (µs) | Full page reads | Pending page reads |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 4 | 72.791 | 6.167 | 4 | 1 |
| 16,384 | 4 | 1,028.750 | 3.292 | 53 | 1 |

The structural result is one leaf read regardless of completed-prefix size;
the microsecond timings are not a cloud performance promise. A worker no longer
uses its bounded scan allowance on completed documents, including completed
bodies larger than the worker's soft batch allowance. The tradeoff is maintained
stage trees at publication: changed pending facts update their affected trees,
while identical trees and update results share immutable pages. The root grows
from 256 to 512 bytes. Separate regression tests cover source changes, exact
stage counters, shared-page GC, and allocation failures.

## Workload and measurement boundary

- Filesystem-backed artifacts, WAL, manifests and progress/leases; ReleaseFast.
- A namespace contains 1,024 or 16,384 documents. One document has 1 or 1,023
  local outgoing edges; the other documents have no outgoing edges.
- Each measured mutation replaces the hub after-image, changing only the first
  edge's weight. Text/vector content is unchanged. Degree and PageRank metrics
  are configured; PageRank allows 20 iterations.
- The timer includes mutation encoding, WAL append, status/action prediction,
  source-fenced touched-document hydration, facts/graph publication, configured
  metric work, fenced HEAD publication and published-head verification.
- Initial namespace bootstrap and construction of the input after-image are
  outside the timer. Each case performs one warmup followed by five samples;
  the table reports median total latency and counters from that sample.
- GET/PUT counts and bytes are logical calls at the artifact-store boundary.
  Range reads count as GETs and report returned bytes. Manifest, WAL and lease
  I/O latency is included, but their operations are not in the artifact counts.
  These are not provider-internal request counts or peak-memory measurements.

## Local qualification results

Measured September 11, 2026, during this branch's redesign, on local macOS
filesystem storage. This is a development qualification snapshot, not a cloud
latency SLO. Later durability/platform changes and contention may affect timing.

| Documents | Hub degree | Total median (ms) | Prediction (ms) | Artifact GETs | Read bytes | Artifact PUTs | Write bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 6.120 | 0.587 | 22 | 200,906 | 7 | 58,468 |
| 1,024 | 1,023 | 26.814 | 8.114 | 32 | 505,048 | 10 | 249,168 |
| 16,384 | 1 | 8.621 | 0.779 | 30 | 323,214 | 11 | 138,597 |
| 16,384 | 1,023 | 32.116 | 11.776 | 32 | 551,391 | 10 | 253,878 |

All cases made zero separate artifact stat/verify calls. The preceding version
of the same fixture, already using document facts but before semantic edge-delta
planning and compiled facts normalization, measured 399.164 ms and 393.761 ms
for the two hub cases. The complete publication is approximately 14.9x and
12.3x faster in this development comparison. It also includes typed-body
envelope changes, so it is not an isolated single-function benchmark. The
degree-one cases changed from 5.802/7.414 ms to 6.120/8.621 ms; the small sample
does not establish a regression or improvement for that low-latency workload.

The important structural improvement is that canonical unchanged outgoing
edges never become mutations. One changed weight produces six adjacency/index
key changes, instead of revisiting every endpoint of the hub. Unchanged explicit
membership also avoids mutations. Tests compare the incremental result with a
full canonical rebuild, including duplicate edges and implicit nodes, and
assert bounded page writes for a 2,048-edge hub. Facts hydration reads touched
document bodies by a source-fenced point index instead of materializing the
namespace. In this fixture, a 16x larger namespace therefore does not create
16x publication work.

The result does not establish performance for high-density whole-graph PageRank,
large touched batches, cloud object latency, cold provider caches, compaction,
or bootstrap throughput. Initial graph bootstrap separately uses admitted
sorted runs with bounded merge fan-in; its sorting memory is bounded by the run
budget plus one document's edge list and cursor paths. Unreachable scratch runs
remain governed by publication-attempt inventory and garbage collection.

## Metadata-only publication qualification

The same fixture now also measures graph-alias metadata publication before the
WAL samples. Each sample renames the graph alias while retaining the same degree
and PageRank computation settings. The timer covers the builder publication,
including source protection, manifest persistence and fenced HEAD update;
catalog planning is outside this measurement. It uses one warmup and five
samples on the same filesystem-backed namespace.

The facts fingerprint now represents counter semantics rather than raw index
JSON: enabled pipeline versions and canonical, deduplicated chunked full-text
source configurations. Graph aliases, graph metric settings, unrelated dense
settings, JSON ordering and disabled pipeline versions do not invalidate it.
An unchanged source fence retains the exact immutable facts root. Reused
text/vector/sparse indexes and graph aliases do not hydrate document bodies;
derived-presence checks use the authenticated root's exact counters. Changing a
metric computation can still require graph work, but not a document-facts scan.

| Documents | Hub degree | Metadata median (ms) | Artifact GETs | Read bytes | Artifact PUTs | Write bytes |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 1 | 3.194 | 6 | 2,314 | 0 | 0 |
| 16,384 | 1 | 3.551 | 6 | 2,314 | 0 | 0 |

These metadata rows were refreshed after adding the 512-byte pending-index
root. The three facts-root reads add 768 bytes in total, with no additional
artifact requests. The matching degree-one WAL rerun measured 6.537/8.213 ms,
22/30 GETs and 7/11 PUTs for 1,024/16,384 documents. The larger root adds 1,024
read bytes and 256 written bytes per WAL sample; request counts remain unchanged.

These are logical artifact-store counters; they do not count provider-internal
authentication reads. Namespace bootstrap is excluded. No before/after latency
speedup is claimed for this new measurement. A normal CI regression also enables
text, vector and sparse artifacts, rejects document-body reads at every artifact
read callback, and checks zero artifact writes, unchanged facts/topology roots
and unchanged topology generation across repeated alias publications.
The same run's degree-one WAL medians were 6.913 ms and 8.750 ms respectively;
their artifact counts/bytes matched the earlier qualification table. The new
metadata path's artifact work was constant across the 16x namespace increase.

## External metadata retention qualification

An unchanged external source now reconciles sidecars from the old and requested
index definitions instead of publishing an inventory-only manifest. Compatible
text, vector and graph artifacts retain their complete physical metadata;
graph metrics additionally require a retained parent graph, matching source
checksum and matching computation fingerprint. Changed dependencies are
invalidated individually. A changed external source still starts without the
old sidecars.

Catalog readiness and metadata publication use the same binding-aware plan.
`planAlloc` returns retained references and desired actions without cloning a
manifest; `reconcileAlloc` includes that planning work and constructs an owned
updated manifest. Neither API accepts an artifact-store or row-source capability,
so neither can read document bodies or rebuild artifacts. This focused
ReleaseFast benchmark changes read metadata on an owned manifest containing
text, vector, graph, degree and PageRank sidecars. It measures unchanged
`current` selection, `current` to a pin naming the same snapshot, and that pin
back to `current`. Each case supplies an owned resolved source plan with the
published snapshot and inventory identity, modeling the verified evidence from
the guarded publisher. This benchmark does not perform that verification or
remote discovery itself. One warmup precedes five
samples for each API. Timings exclude result destruction, correctness assertions,
discovery, leases, manifest persistence and HEAD publication. Reconciliation
includes planning; the two timings should not be added together.

External publication requires that resolved inventory plan even for a pinned
snapshot. Separate guard regressions verify that missing resolution fails before
managed WAL reads or artifact writes; the source-kind dispatch check is not
treated as a meaningful standalone performance workload. This benchmark
qualifies the unchanged metadata path after that check, not resolver latency or
the guard's failure path.

The rejected variant marks PageRank rejected under the current materializer
policy. The unchanged full admission plan must preserve that rejection; this
exercises the same complete-plan predicate used by actual materialization.
Each sample verifies five retained references and five reuse actions with no
outstanding work, including both selector transitions. Untimed probes remove
the graph and check that its metrics
require rebuilding, then remove all index declarations and verify that real
drops remain pending until reconciliation applies them. The probe then verifies
that a fresh plan reports no outstanding work. A retained rejection is terminal
under an unchanged admission plan; it is not relabeled as a ready computation.

All cases retain five sidecars and accept no artifact-I/O capability. These
results were requalified after making external resolution mandatory; they are
not an isolated measurement of the source-kind dispatch check.

| Document count in manifest | PageRank state | Selector transition | Plan median (ms) | Reconcile median (ms) |
| ---: | --- | --- | ---: | ---: |
| 1,024 | Ready | Unchanged current | 0.308 | 0.367 |
| 1,024 | Ready | Current → pinned | 0.258 | 0.325 |
| 1,024 | Ready | Pinned → current | 0.241 | 0.286 |
| 1,024 | Rejected | Unchanged current | 0.234 | 0.291 |
| 1,024 | Rejected | Current → pinned | 0.211 | 0.258 |
| 1,024 | Rejected | Pinned → current | 0.208 | 0.254 |
| 16,384 | Ready | Unchanged current | 0.190 | 0.241 |
| 16,384 | Ready | Current → pinned | 0.196 | 0.237 |
| 16,384 | Ready | Pinned → current | 0.183 | 0.229 |
| 16,384 | Rejected | Unchanged current | 0.190 | 0.230 |
| 16,384 | Rejected | Current → pinned | 0.181 | 0.221 |
| 16,384 | Rejected | Pinned → current | 0.203 | 0.219 |

This measures metadata work, not a cloud latency SLO or a before/after speedup.
The document count is a manifest statistic in this fixture; the configured
artifact set is deliberately fixed. The relevant scaling property is dependence
on configured artifacts, not the external document count. The small-sample
timings do not establish that rejection handling or larger namespaces are faster;
the structural result is that complete-plan validation adds no artifact reads
or document-count-dependent work. Regression tests separately check that changed
plans drop rejections while retaining compatible ready computations.
These publication cases include resolved source evidence. They do not imply
that discovery-free status can prove a pin-to-current transition still names
the published snapshot; status remains conservative without that evidence.

```sh
ANTFLY_DOCUMENT_FACTS_BENCH=1 zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'external metadata retention qualification benchmark'
```

## Finite enrichment cycle qualification

The current pending index orders entries by `(WAL LSN, document ID)`. Capturing
the last ordering key once per cycle fences out new mutations even when their
document IDs would sort inside the previous document-key range. It uses subtree
record counts to seek the last rank, not a full pending scan. Resumed batches
reuse the durable ordering-key boundary and read the current pinned root.

An in-memory ReleaseFast benchmark compares full pending traversal against
capturing an owned boundary key, excluding bootstrap and body reads. One warmup
precedes five samples. Timing pairs below are from the median boundary sample.

| Documents | Pending | Full scan (µs) | Boundary capture (µs) | Scan pages | Boundary pages |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 1,024 | 4 | 4.209 | 2.375 | 1 | 1 |
| 1,024 | 1,024 | 76.375 | 11.500 | 5 | 2 |
| 16,384 | 4 | 3.917 | 3.000 | 1 | 1 |
| 16,384 | 16,384 | 1,142.208 | 9.083 | 58 | 2 |

The eight-byte LSN prefix increases full-index page count (58 rather than 54
in the preceding all-pending fixture), but boundary capture still reads only
two pages at this size. Four pending entries behind 16,380 completed documents
remain one page / 3.375 µs versus a 53-page / 1,026.333 µs primary-index scan.
These are routing measurements, not end-to-end model or object-store latency.
The ordered pending tree is distinct from the document-ID point tree; equivalent
stage trees still share pages, but no longer share the point tree at bootstrap.

A matching degree-one publication rerun measured 6.723/8.304 ms for
1,024/16,384 documents, with the same 22/30 GETs, 201,930/324,238 read bytes,
7/11 PUTs and 58,724/138,853 written bytes as the preceding qualification.
Managed metadata-only publication measured 3.284/3.479 ms, still six GETs,
2,314 bytes and zero PUTs. This fixture has no pending enrichment population;
it verifies that the queue format does not add work to unrelated publication,
not the write cost of moving a pending entry between LSN positions.

```sh
ANTFLY_DOCUMENT_FACTS_BENCH=1 zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'pending cycle boundary qualification benchmark' --test-filter 'pending work index qualification benchmark'
```

## Focused correctness checks

```sh
zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'document facts' --test-filter 'external graph bootstrap' --test-filter 'paged graph' --test-filter 'visits borrowed body records' --test-filter 'metadata graph alias'
zig build antfly-storage-db-test -Doptimize=ReleaseFast -- --test-filter 'db dense target coverage reads one immutable primary commit epoch' --test-filter 'db shared embedding enrichment feeds multiple dense indexes with durable lsm primary backend' --test-filter 'db inline dense generation remains rebuilding until outcomes cover the live corpus'
```

The stateful counter regression verifies that a derived-coverage tuple and its
range cardinality come from one immutable primary commit epoch, including
atomic first creation and replacement. This addresses the torn-read CI failure
without weakening corruption detection.
