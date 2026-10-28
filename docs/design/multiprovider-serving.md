# Shared multiprovider serving and qualification

This extends issue #1278 and draft PR #1320. One Antfly native engine owns
accepted WAL progress, published manifests, artifacts, receipts and collection.
R2, S3 and GCS supply transport and atomic single-object publication. Host
adapters supply execution, scheduling and credentials; caches are disposable.
This document is an implementation plan until each item has final evidence.

## Revision and publication contract

`objectstore.ObjectRevision` distinguishes an ETag from a GCS generation.
`ObjectMetadata.writeRevision()` borrows a token from metadata captured with the
payload. `Client.compareAndPut` treats a null revision as create-only; a supplied
revision must be atomically checked by the provider. `compareAndDelete` protects
the captured current object. Unsupported revision kinds fail before dispatch.
S3 version IDs are not used as conditional write ETags. Conditional current-name
deletes and explicit old-version deletes remain separate operations.

GCS maps typed generations directly to `ifGenerationMatch`, validates decimal
bounds, preserves cancellation/conflicts, and does not insert a metadata request
on that fast path. WAL, catalog, progress and lease control reads request only
response-derived body/revision metadata, avoiding an extra metadata probe.
Callers that require complete checksum metadata retain the complete-metadata API.
For legacy ETag callers, the adapter can resolve a matching
ETag to a generation and conditionally publish against that generation. The extra
metadata request is a compatibility cost; no path may check then write
unconditionally. Schema/integrity checks and conditional outcome semantics remain
unchanged. Cross-object guarded writes remain a separately advertised capability. The current
WASM host transport targets R2/ETag operations. It preserves revision kinds but
fails closed on generation writes until a host explicitly implements that
capability; native GCS support does not imply a GCS-capable JavaScript bridge.

## Runtime ownership and format selection

The generic serverless bootstrap needs the same indexed WAL and live-session
interfaces as the WASM host. Transport ownership outlives the selected WAL and
all query/build sessions. A runtime-owned bounded `ReadStateCache` retains only
immutable derived overlays; callers own sessions, resolvers, cancellation,
authority and I/O. Teardown follows cache/session, store, then transport order.

Select indexed WAL explicitly. Existing legacy `wal.log` prefixes require an
explicit migration/cutover; startup must reject a format change that would hide
acknowledged records. Compatible writers and collectors must be deployed together.
An immutable, conditionally created `.antfly/WAL_FORMAT` record serializes layout
selection among compatible hosts. Legacy writes claim their mode before writing
records, so a racing indexed activation cannot create a shadow legacy log.
Reopen reads that marker in constant work; it does not list every page. Existing
unmarked prefixes get the conservative legacy inventory once. An activation
scan is not an online migration protocol or a fence against an old incompatible
writer that does not honor the marker. Use a new prefix and a coordinated cutover until an
export/import protocol preserves all receipts, LSNs, retention and identities.
HEAD format 4 persists after feature deactivation; flags do not downgrade data.

## Serving and scheduling

Retain immutable bytes and decoded state across requests under existing total
memory limits. Bind cache identities to provider/bucket/prefix, namespace,
lineage and fresh root identity. With `NATIVE_READ_AFFINITY`, prefer warm compatible idle kernels for a
namespace, without waiting for a busy kernel or bypassing fair admission.
Retain dedicated authority progress and bounded tenant/maintenance fairness.
No queued callback or I/O promise may execute in another caller's context.

Reduce fresh-view dependency depth and avoid redundant planning/reservation
handoffs. Record offered arrival, authority admission, kernel queue, native CPU,
I/O wait/waves and response time separately. Percentiles of separate spans are
not additive. Native and WASM executors use the same data/visibility protocol;
execution placement and bridge costs are benchmark variables.

## Write and maintenance amplification

Use bounded group commit to share immutable output and durable metadata costs
across writes while atomically retaining every keyed operation's receipt.
`WalStore.appendBatch` also admits an atomic batch without a retry identity. The
shared service uses byte/record-bounded chunks for unkeyed bulk ingestion, while
keyed requests retain one all-or-nothing replayable batch. A lost unkeyed response
remains ambiguous; this API does not silently retry or invent a client receipt. Keep model
calls outside the commit critical section. A successful acknowledgement means
the same durable outcome after ambiguous retries and takeover.

Maintain point projections only where replay savings pay for their write and
retention cost. Small tails seed bounded reusable overlays; larger covered tails
can use authenticated point indexes. Optimize cold index/data layout around
sequential remote dependency waves, bounded range reads and useful prefetch.
Keep publication, compaction and collection independently resumable and bounded,
with a reserved progress share and frontend backpressure when lag grows.
Protect record, receipt and document roots and incomplete publication roots under
the same epoch barrier. Sharding requires explicit routing/visibility contracts;
it is not the first remedy for overhead on tiny namespaces.

## Comparative evidence

Turbopuffer is the closest managed search comparison; SlateDB is the closest
embedded object-backed engine. LanceDB and D1 plus Vectorize are relevant feature
comparisons. Neon illustrates memory/local-disk caching over durable remote
storage. Published numbers use different freshness, durability, hardware and
cache contracts and are not a ranking against Antfly.

- https://turbopuffer.com/docs/architecture
- https://benchmark.slatedb.io/
- https://github.com/slatedb/slatedb-benchmark
- https://docs.lancedb.com/performance
- https://blog.cloudflare.com/building-vectorize-a-distributed-vector-database-on-cloudflare-developer-platform/
- https://neon.com/blog/separation-of-storage-and-compute-perf
- https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- https://docs.cloud.google.com/storage/docs/request-preconditions

## Qualification sequence

1. Run provider contract tests for create, captured revision update/delete,
   collisions, cancellation, missing metadata, ambiguous outcomes, stale owners,
   collection and reopen. Keep actual-provider and mocked transport results
   distinct. Qualify special storage classes separately: S3 Express sessions and
   GCS Rapid streaming are not assumed capabilities of standard transports.
2. Refresh final-module native/Node/workerd failures and leak checks, full scale
   and both frozen ANN populations. Hash exact modules, bundles/configuration,
   fixtures, source and reports; preserve every failed/intermediate run.
3. Run matched native/WASM and provider comparisons with fixed dataset, payload,
   offered arrivals, freshness, durable acknowledgement, recall, cache budgets,
   placement and warm/cold state. Include p50/p95/p99, throughput, operation/byte
   amplification, publication visibility lag, maintenance convergence and cost.
4. Run short live diagnostics with complete invocation/resource coverage before
   consuming cold epochs. Historical missing invocation coverage remains open
   until proved fixed; do not substitute outcome hashes for request coverage.
5. Pass three distinct one-hour Cloudflare pairs against corrected control
   `c44896ac8c`, four tenants of 512 documents: read p95 <=1,000 ms with >=1,000
   samples/side; durable write p95 <=3,000 ms with >=300 samples/side; neither
   candidate latency worse than its control. Each candidate needs 100 fresh
   physical epochs, p95 <=5,000 ms, one attempt and correct count. Exact corpus
   and receipt checks, stable queues, zero DLQ, publication convergence, complete
   diagnostics, faults and equal-budget ANN must also pass unchanged.
6. Qualify S3 and GCS separately before claiming deployment parity. Inventory
   disposable resources, retain local evidence and complete authorized cleanup.

The existing local 31% storage-delay read-p95 improvement is not a cloud gate pass.
The zero-injected-delay regression and recovered local `SQLITE_NOMEM` remain
recorded in `native-read-state.md`. No thresholds or resource caps are relaxed.

## Native host controls

General native hosts select `BootstrapConfig.indexed_wal` explicitly, then may
enable `native_read_state` and `wal_document_index`. The standalone host exposes
`--indexed-wal`, `--native-read-state`, and `--wal-document-index`, each taking a
boolean, plus `ANTFLY_SERVERLESS_INDEXED_WAL`,
`ANTFLY_SERVERLESS_NATIVE_READ_STATE`, and
`ANTFLY_SERVERLESS_WAL_DOCUMENT_INDEX`. All default off. The runtime's
`openLiveSession` owns fresh published/WAL admission and supplies the shared
cache to caller-owned sessions. Legacy HTTP diagnostic/export responses retain
all original tail mutations and timestamps; they are not silently rewritten as
collapsed overlays. Other query entry points must adopt the bounded interface
explicitly before claiming the same cache behavior.

Internal native table ingestion now forwards an optional operation identity into
the existing atomic WAL batch primitive. This does not add a public HTTP wire
field or weaken older providers' unsupported-capability errors.

The provider-contract runner (`qualification/provider-contracts.mjs`) provisions
an exclusively disposable private standard bucket, executes transport and full
native runtime reopen/receipt/publication tests, and removes its objects/bucket.
Credentials are acquired into volatile process memory and child environment,
never report bodies or command arguments. The report records creation intent,
resource identity, test outcome and cleanup. A failed test remains a failed
report even when a later corrected run passes. This is correctness qualification
from the local machine, not colocated cloud latency evidence.

## Implementation checkpoint and rollout boundaries

The shared revision contract, catalog/progress/lease adoption, explicit native
indexed-WAL bootstrap, bounded live-session cache ownership, unkeyed batch append,
response-only control reads, immutable layout selection, and independent idle
host affinity control are implemented in this branch. All controls retain their
existing defaults; an existing namespace is not automatically migrated.

The native batching contract measures 127 object operations for 32 scalar
records versus three for the atomic unkeyed batch, preserving order and without
creating receipt entries. This is operation amplification in a controlled unit
fixture, not an end-to-end write-latency claim. Keyed group commit retains the
existing atomic receipt boundary. Unkeyed lost acknowledgements remain ambiguous.

A real standard-GCS run covers create, captured-generation update/delete, stale
revision rejection, runtime bootstrap through ordinary `gs://` configuration,
publication, complete reopen and keyed replay after an injected lost WAL HEAD
acknowledgement. The latest run also includes the immutable layout marker and
legacy media/JSON ETag compatibility tests. Its disposable bucket was deleted
and absence verified. Native contracts and mock transports cover unsupported
capabilities and allocation/fault paths separately. AWS qualification still
requires a locally authenticated profile; no AWS success is implied.

The following work remains before deployment parity or the complete design can
be called qualified:

- Connect the existing durable collector frontier to generic native indexed-WAL
  maintenance. The native pruner currently advances the logical WAL floor; that
  alone does not reclaim obsolete immutable WAL page objects. Reuse the same
  authenticated tracing, epoch barrier and record/receipt/document roots as the
  hosted collector. Reader and in-progress publication protection must remain
  effective across restart and takeover; do not introduce a latest-root-only
  sweep. [The native collection protocol](native-indexed-wal-collection.md)
  specifies new-prefix activation, persisted bounded root use, matching-ID
  lifecycle, transactional child continuations, grace and provider-version
  handling. It is proposed and not implemented. Until this is connected, retain
  WAL pages conservatively.
- Migrate each appropriate generic HTTP query entry point to the bounded live
  session interface with its own freshness contract. Existing diagnostic/export
  responses require mutation LSNs and timestamps and cannot use a collapsed
  overlay as a substitute for original history.
- Provide a coordinated legacy export/import path preserving table identities,
  receipt history, LSNs, retained snapshots and visibility fences if in-place
  upgrades are required. The current supported activation is an empty/new prefix
  or a prefix already marked indexed, with compatible writers and collectors.
- Establish matched native-versus-WASM and colocated R2/S3/GCS performance
  evidence. Local provider correctness and injected-delay experiments do not
  establish cross-cloud latency, cost or placement parity.
- Complete final-module scale/ANN and live Cloudflare diagnostics, then the
  unchanged three paired one-hour gate and physical cold-recovery requirements.

Tenant sharding remains a separate routing/consistency milestone: split only
when measured tenant hot spots exceed bounded publication/serving capacity,
using stable partition ownership, explicit fan-out limits, per-shard accepted
watermarks and a defined merge snapshot. Adding independent writers to one WAL
HEAD or treating per-shard tokens as one global transaction is not a sharding
protocol.

## Qualified shared-runtime checkpoint

Production source `d226557d07` passes 322 Node tests, 31 actual-workerd cases,
610 native serverless tests (seven skips), seven focused multiprovider contracts
(one live-provider skip) and 158 standalone object-store tests (two live-provider
skips), with no native leaks. The exact final full WASM SHA-256 is
`afdf4e9bd1a1ea9ecc8738ce4fa8724f5a28655fcc5a7fd152e452d54def865d`.
The passing workerd log still contains a recovered local platform
`SQLITE_NOMEM`; it does not establish that platform memory pressure is solved.

The committed-source standard-GCS run passes transport conflicts, bootstrap,
publication, lost-acknowledgement recovery, complete reopen, a second publication,
native pruning and retained original receipt replay. Its private disposable
bucket and test objects were deleted and absence verified. This is a small
correctness fixture, not a larger-workload or colocated GCS latency result.

The final 50,000-document file-backed fixture completes all nine stages,
including cold text/vector/sparse queries, updates, deletion and compaction.
It peaks at 869 object operations, 28,826,922 transferred bytes, 11,788,282 native
working bytes and 27,590,656 linear-memory bytes per measured stage/call bounds.
The local fixture writes 21,526,325,594 bytes to its temporary append-only backing
file and removes that file afterward. A separate 50,000-row metadata test passes
three collection cycles, reopen, hot-root and non-mutating observer checks;
peak operations are 115 and peak WASM linear memory is 8 MiB.

Both final frozen ANN populations pass matched recall/GET/byte comparisons at
8, 32 and all probes. Synthetic recall is 0.45/0.86/1.0; model-embedding recall
is 1.0 at every budget. Exact execution reduces mean GETs from 170.7 to 140.3
for synthetic data and 40.9 to 39.4 for model embeddings. These comparisons also
include earlier shared layout work; they do not isolate the new provider adapter
as the cause. All compared mean transferred bytes are no greater than control.

The affinity ablation uses the same final modules and four tenants. Delayed
runs use 512 documents/tenant, 192 reads and 64 writes with fixed 20/100 ms
binding delays; zero-delay runs use 64 documents/tenant, 96 reads and 32 writes.
Values below are read/write p95 milliseconds:

| Control combination | First order | Reverse order | Zero injected delay |
| --- | ---: | ---: | ---: |
| Derived overlays, foreground cache, opportunistic preparation | 909 / 1,167 | 563 / 1,156 | 184 / 184 |
| Also idle-host affinity | 845 / 1,167 | 653 / 1,151 | 200 / 200 |
| Also affinity and WAL document index | 971 / 1,256 | 772 / 1,251 | 395 / 387 |

Affinity is not consistently faster and indexing costs more in this small-tail
population. Keep both off for the smoke candidate; retain independent controls
for other workloads. Read p50 also varies with order. Do not choose only the
favorable run or infer a cloud SLO from these local populations. Fresh proof
I/O and kernel queueing remain substantial spans in the slow requests, motivating
bounded overlap of proof acquisition with execution admission.

The subsequent [proof-admission experiment](metadata-proof-admission.md) passes
328 Node and 31 workerd tests, but its delayed read-p95 improvement does not
repeat in reverse order and both zero-delay orders regress. Keep
`DETACHED_METADATA_PROOFS` off alongside affinity and the document index in the
live candidate. The feature's lifecycle correctness is separate from whether
it improves this workload.

## Native HTTP batch adoption

Ordinary public table replacements and deletes now resolve their input without
opening a current-state query. Previously every batch materialized the entire
namespace even when no transform needed an existing body. Mutation cloning owns
its bytes and unwinds each allocation failure.

Native indexed-WAL hosts configure transforms to use one caller-owned bounded
`LiveSession` per batch. It pins the published base and captures one accepted
WAL cut; all transform keys read that same cut. The runtime's bounded derived
cache is reused when enabled. Request cancellation reaches session and transport
work. Capacity/expired-view errors return retryable backpressure before append;
integrity and transport failures do not select an unbounded fallback. An older
published layout without document facts explicitly retains its existing legacy
materializer. Legacy-WAL hosts retain their existing transform path.

This does not add compare-and-swap isolation to public transforms: a concurrent
writer can still commit between resolution and ingestion, as in the prior API.
It does not reinterpret published search or diagnostic/export history as a fresh
collapsed overlay. Their existing visibility and original-mutation contracts
remain authoritative. The new memory-backed test covers unpublished transforms,
pre-append backpressure, publication, multiple keys and an earlier session that
retains its old cut. Allocation-failure enumeration checks replacement ownership;
real-provider qualification repeats a published-base transform and full reopen.

Reports, exact role bundles/bindings and failed/intermediate runs remain local
under `examples/cloudflare-serverless/dist/qualification/multiprovider-lab/`.
All 1,592 previously sealed artifacts remain hash-valid. No Cloudflare resources
were created for this checkpoint; its prepared smoke pair has no gate result.
