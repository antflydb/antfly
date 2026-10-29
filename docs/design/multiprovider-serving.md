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
The zero-injected-delay regression remains recorded in `native-read-state.md`.
The local `SQLITE_NOMEM` log is intentional fault injection; see the
[SQLite memory audit](cloudflare-sqlite-memory.md). No thresholds or resource caps are relaxed.

## Native host controls

General native hosts select `BootstrapConfig.indexed_wal` explicitly, then may
enable `native_read_state` and `wal_document_index`. The standalone host exposes
`--indexed-wal`, `--native-read-state`, `--wal-document-index`, and `--wal-gc`, each taking a
boolean, plus `ANTFLY_SERVERLESS_INDEXED_WAL`,
`ANTFLY_SERVERLESS_NATIVE_READ_STATE`, and
`ANTFLY_SERVERLESS_WAL_DOCUMENT_INDEX`, and `ANTFLY_SERVERLESS_WAL_GC`. All default off.
The collector requires a pristine managed prefix; its persisted bounded reader
policy remains enforced when the collector flag is disabled. The runtime's
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

- Finish real-provider and final-artifact qualification of the implemented
  [native collection protocol](native-indexed-wal-collection.md). The collector
  now uses persisted bounded root use, epoch-and-ID fencing, native transactional
  frontier/marks, bounded child continuations, restart-safe local grace and typed
  version handling. It remains opt-in on pristine managed prefixes; earlier
  sealed scale results predate this implementation.
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
The passing workerd log contains the intentional legacy `SQLITE_NOMEM`
fault injection, not a newly observed platform allocation failure.

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

At production source `257397f236`, the final native suite passes 612 tests with
seven skips and no failures/leaks; all 328 Node and 31 actual-workerd cases pass.
The workerd log includes the intentional legacy `SQLITE_NOMEM` injection. Rebuilding all
three native WASM modules produces exactly the hashes from the earlier qualified
scale/ANN snapshot. The HTTP changes do not alter those emitted modules.

The new real-GCS run passes all ten focused native contracts, including a public
replacement, publication, bounded transform over that base and full reopen.
It deletes its 56 disposable objects (17,825 bytes) and bucket, then verifies
absence. This remains correctness evidence from a local host. AWS authentication,
matched provider placement/performance and the live Cloudflare gate remain open.

The short-diagnostic pair uses approved corrected control `c44896ac8c` and sealed
candidate `131a703fed`, with affinity, indexing and detached proofs off. Both
stacks are deployed and paused with scheduled maintenance disabled. Automatic
approval review rejected upload of the new disposable API tokens because the
previous approval named other Workers; authorization for this pair is pending.
No live latency result, invocation coverage result or cold epoch is claimed.

Reports, exact role bundles/bindings and failed/intermediate runs remain local
under `examples/cloudflare-serverless/dist/qualification/multiprovider-lab/`.
All 1,592 previously sealed artifacts remain hash-valid. No Cloudflare resources
were created for this checkpoint; its prepared smoke pair has no gate result.


## Native collection checkpoint and integration boundary

At `d10a545001`/`71f79b92ff`, the implemented native collector passes fault,
allocation, restart, version and fairness contracts, a 50,000-record retained-tail
fixture, and actual versioned/unversioned GCS collection/reopen tests. See the
[collection protocol](native-indexed-wal-collection.md) for exact safety bounds
and qualification limitations. The shared runtime also separates fallible
checkpoints from transport cancellation probes: stopping a watchdog must not
renew leases or cache that task's cancellation as failure of a live request.

The rebuilt full/compact/observation modules are 7,329,084 bytes each. Their
SHA-256 hashes are respectively `7946334d2770219463ecfd29913d1254831264dcdbc4e0b1982fd1bd7688b29a`,
`8bfbe4e89a9ca1d8db9a62c83b14d6748c9ef74213155f4113e47d96b43247b0`,
and `ff85bfdf0333eb9be8a991b7c291b52380e1bcf14fad0889dd089ba6417b7865`.
All 330 Node and 31 actual-workerd tests pass. The local `SQLITE_NOMEM`
line comes from the intentional legacy fault-injection endpoint; it was
previously misclassified as a live memory concern. The older compatibility
profile did have a distinct live failure (see the [audit](cloudflare-sqlite-memory.md)).
The engine and the new native collector do not use SQLite.

The final 50,000-document mixed text/vector fixture passes all nine stages:
peak 869 operations, 28,826,922 transferred bytes, 11,788,282 working bytes and
27,590,656 linear-memory bytes. Its 21,526,325,594-byte owned backing file is
removed afterward. A separate 50,000-row metadata fixture passes three
collection cycles, reopen, hot-root and read-only observer checks, with peak
121 operations and 6,029,312 linear bytes. These are local fixtures rather than
cloud latency or total-isolate memory measurements.

Both frozen ANN corpora also pass using the 8 MiB reader module and an explicit
8 MiB request working budget. Synthetic 10,000-document read peaks are
7,208,960 linear bytes; the 1,000-document real-model corpus peaks at 5,963,776.
At probes 8/32/all, synthetic recall is .45/.86/1 with mean GETs
63.2/123.6/140.3; real recall is 1 at every budget with mean GETs
40.9/40.9/39.4. Mean bytes at each budget are no greater than the frozen control.
As in the unchanged ANN gate policy, those GET/byte comparisons are per-budget
means; three individual real-model exact queries transfer more bytes. The
matched previous-candidate comparisons preserve its recall, GET and byte values.

The 2,057 previously sealed artifacts still match their hashes. New checkpoint
reports and binaries remain under `dist/qualification/native-gc-lab/`. The subsequent integration merges all 22 newer main commits, including both
import/ownership-anchor additions; its separately qualified artifacts are
recorded below. AWS authentication and explicit
Cloudflare token/cleanup authorization remain external prerequisites. No live
gate, cold-epoch or cross-cloud latency parity result is implied.


The checkpoint repeats proof overlap in both orders. Delayed read/write p95
(milliseconds) is 746/1168 without proofs versus 467/1176 with proofs, and in
reverse order 561/1164 versus 448/1167. Zero-delay runs are 113/95 versus
101/101, and in reverse order 108/108 versus 121/121. All exact checks pass.
The write and reverse-zero regressions keep `DETACHED_METADATA_PROOFS` off;
`NATIVE_READ_AFFINITY` and `WAL_DOCUMENT_INDEX` also remain off for the selected
Worker profile. These short local ablations do not replace the live gate.


## Integrated multiprovider qualification

Main `d35a1ef33e` is merged through `22c815aabb`. The integrated native collector
now commits up to eight nodes of authenticated marking/frontier progress in one
bounded transaction, with an in-step mutation overlay and a 505-command upper
bound. It reuses Antfly's transactional engine on every native object provider.
The repeated 50,000-record fixture reduces private cleanup objects by 70.5%
and peak storage operations per step by 49.0%, retaining identical final object
counts and exact record/receipt checks. See
[native indexed-WAL collection](native-indexed-wal-collection.md).

At source `10eadf1f24`, the guarded native command passes 631 primary tests with
nine skips and no failures/leaks, and all 334 Node tests pass. Clean-source real
GCS runs pass both versioned and unversioned transport/runtime/collection
contracts, verify fresh managed storage, preserve their executed binary, and
verify bucket absence after cleanup. A fresh AWS profile check still reports
`NoCredentials`; memory/fault fixtures and GCS success do not constitute real
S3 qualification.

The final rebuilt WASM modules are byte-identical to the separately sealed
main-integration scale/ANN/workerd artifacts. Each file is 7,357,736 bytes:

- Full, 32 MiB cap: `872f61127362cd6fdb5a90cd9ea5a892a98a29ce10efef2f7f3b91b0f8b27ed8`.
- Compact, 16 MiB cap: `3b7b1dbae138f3f2f8b74cbd93fac6263bd88c487d4929b84894a22c3a18a1cb`.
- Observe, 8 MiB cap: `b8571892485276fb8a9227e423ba6838ace3e2ce8062b40a7831d71560e99300`.

The generated module closure records the exact artifacts. The unchanged
module/Worker closure
retains 31 passing actual-workerd cases, the nine-stage 50,000-document mixed
fixture, repeated 50,000-row metadata collection, and both frozen ANN corpora
at probes 8/32/all. ANN read peaks remain 7,208,960 and 5,963,776 linear bytes
under the 8 MiB reader/request budget; recall and mean GET/byte comparisons
pass. These are local contract/scale results, not total isolate RSS or cloud
latency. The historical compatibility-profile SQLite failure remains
unreproduced; local injected error output is not a new occurrence.

Provider qualification requires clean committed source before creating a
bucket and verifies the same clean HEAD afterward. Use
`qualification/provider-contracts.mjs --provider=gcs --region=us-central1
--versioned=true --wal-gc=true --cache-dir=ABSOLUTE_WORKTREE_CACHE --out=REPORT_DIR`
(and repeat with versioning false). It records the compiler, executed binaries,
source revision and fresh native data before cleanup. Do not edit source during
a run. Compiler caches stay within the worktree; active-run markers protect
both the selected cache and the standard-library fixture cache until child
processes settle. Earlier cache-loss failures remain separate failed evidence.

`qualification/native-host-contracts.mjs --binary=PATH --out=REPORT_DIR` exercises
the release API-only CLI, explicit HTTP ingestion/publication/latest query, process
restart and file-backed managed WAL policy with collection disabled on reopen.
This smoke does not qualify a new keyed HTTP ingestion protocol or cloud SLO.

Final reports are under `dist/qualification/native-gc-batch-lab/`; previous
native and main-integration evidence remains sealed separately. Legacy-prefix
migration, real S3 access, matched cross-cloud performance and the unchanged
live Cloudflare gate remain separate requirements. The existing two Cloudflare
stacks remain paused pending the explicit disposable-token/cleanup authorization.


The final module family repeats the proof comparison in both orders. With
512 documents per tenant and 20/100 ms fixed-bursty binding delays, read/write
p95 in milliseconds is 1,082/1,250 without proofs versus 556/1,229 with proofs;
in reverse order it is 1,422/1,437 without versus 843/1,235 with. Zero-delay
64-document runs vary sharply under concurrent native compilation and host
contention: 1,841/1,841 without versus 752/750 with in the first order, then
398/385 without versus 2,565/2,565 with in reverse order. Every exact check
passes. These noisy local populations do not establish an isolated latency
baseline or a repeatable all-metric win. Keep detached proofs off; preserve
both orders and the compilation/resource context rather than selecting the
favorable delayed result.


A local replacement proposal for the existing paused diagnostic pair is prepared
under `native-gc-batch-lab/cloudflare-final/gate-mvslyh876561/`. Its approved
`c44896ac8c` control bundles are byte-identical to the current control; only the
candidate is rebuilt from committed source `322ac77832` with the final module
family. All four configurations match the paused deployments apart from local
entry paths, and sealed-run verification passes for both sides. No credential
files were copied, no cloud resources were created and no deployment changed.
The actual candidate deployment therefore remains the earlier `131a703fed`.
After explicit authorization, copy the immutable proposal to an unsealed runtime
directory before installing the approved disposable tokens or changing lifecycle
state; keep the sealed proposal and prior deployments' evidence intact.


The first native release process smoke found a root-prefix compatibility gap:
empty-prefix indexed WAL keys began with `/`, which the filesystem adapter
rejects. [Layout version two](native-indexed-wal-collection.md#empty-logical-prefixes-and-explicit-layout-version-two)
selects a reserved relative physical prefix only through an explicit immutable
marker on a pristine logical root. Existing cloud version-one keys and receipts
retain their exact layout; old filesystem markers fail closed instead of being
rewritten. The new focused tests pass 31 with three environment skips and no
failures/leaks. The failed process report remains preserved. The corrected stripped release
now passes the API-only process lifecycle, including exact publication/reopen
reads, a subsequent write, persisted reader policy and clean shutdown/storage
cleanup. The inspector verifies the filesystem object envelope and SHA-256
before decoding its payload; earlier inspector failures remain preserved. The smoke uses API-only mode with manual publication enabled,
requires live/ready/validated health, isolates fixture configuration from inherited
Antfly environment variables, and waits for process output to close before
recording cleanup. It does not qualify a background maintenance loop.


At corrected implementation `489e28c436`, final qualification passes 635 native
tests (nine environment skips, no failures/leaks), all 334 Node cases and fresh
real GCS contracts in both versioning modes with cleanup/absence verified.
The three rebuilt WASMs remain byte-identical to the sealed main-integration
family, retaining its 31 workerd cases, 50,000-document/metadata scale and frozen
ANN results by exact closure. The native collector and scale-test body also
match the batched checkpoint exactly. Final native binary SHA-256 is
`761cefcb16412ecaf0fd25c1d2dfd97dafbe5ac09b6a085f3edcab607f9786ca`.

The immutable Cloudflare proposal is under
`native-gc-batch-lab/cloudflare-root-layout/gate-mvslyh876561/`, built from
`489e28c436`. Its control bundles remain exactly `c44896ac8c`; all configuration
is unchanged except local entry paths. Following explicit token-upload and
post-test cleanup approval, an operational copy was deployed to the existing
`antfly-1278-g-mvslyh876561-1-{b,c}` pair and both disposable tokens verified.
The current candidate is `489e28c436`; the earlier `131a703fed` deployment and
local proposals remain historical evidence. See the
[SQLite memory and deployment audit](cloudflare-sqlite-memory.md).
AWS's fresh default-profile check still returns `NoCredentials`. The historical
compatibility-profile SQLite failure remains unreproduced. The live Cloudflare
gate and matched colocated provider performance remain open.


The final native 50,000-record repeat passes 32 focused cases with two
live-provider skips and no failures/leaks, reproducing all batched collection
counters and exact retained-record/receipt checks. Its executed binary is
preserved. All 334 Node cases pass again after the envelope-inspector fix.
