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
| 1,024 | 1 | 3.596 | 6 | 1,546 | 0 | 0 |
| 16,384 | 1 | 3.863 | 6 | 1,546 | 0 | 0 |

These are logical artifact-store counters; they do not count provider-internal
authentication reads. Namespace bootstrap is excluded. No before/after latency
speedup is claimed for this new measurement. A normal CI regression also enables
text, vector and sparse artifacts, rejects document-body reads at every artifact
read callback, and checks zero artifact writes, unchanged facts/topology roots
and unchanged topology generation across repeated alias publications.
The same run's degree-one WAL medians were 6.913 ms and 8.750 ms respectively;
their artifact counts/bytes matched the earlier qualification table. The new
metadata path's artifact work was constant across the 16x namespace increase.

## Focused correctness checks

```sh
zig build antfly-document-facts-test -Doptimize=ReleaseFast -- --test-filter 'document facts' --test-filter 'external graph bootstrap' --test-filter 'paged graph' --test-filter 'visits borrowed body records' --test-filter 'metadata graph alias'
zig build antfly-storage-db-test -Doptimize=ReleaseFast -- --test-filter 'db dense target coverage reads one immutable primary commit epoch' --test-filter 'db shared embedding enrichment feeds multiple dense indexes with durable lsm primary backend' --test-filter 'db inline dense generation remains rebuilding until outcomes cover the live corpus'
```

The stateful counter regression verifies that a derived-coverage tuple and its
range cardinality come from one immutable primary commit epoch, including
atomic first creation and replacement. This addresses the torn-read CI failure
without weakening corruption detection.
