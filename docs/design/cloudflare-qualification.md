# Cloudflare hosted profile qualification

This records a disposable live run on 2026-10-24 UTC for issue #1278.
The fixture is deliberately small; it does not establish arbitrary-dataset
capacity or complete native feature parity.

## Durability and collection

The deployed Worker used R2, a SQLite Durable Object coordinator, Queues,
precompiled Asyncify WASM, Workers AI, and AI Gateway. The fixture passed:

- A 300-document atomic batch, idempotent retries, and a fenced lookup.
- Queue publication, text search, and eight concurrent writers.
- Bounded durable collection and preservation of the original retry receipt.
- A real 384-dimension Workers AI embedding through Gateway.

The successful deployment was version
`5744cc1a-17c6-4599-bb7b-98b0031d37d2`. Its WASM SHA-256 was
`bb21c247e36e614d679492c279330d076aeb19b71d3e2b5b201bb831c03fd496`.
The sealed JS bundle SHA-256 was
`5185038684db5b83451c72fa82877888e53199039e1cd2ef448dea2a5f754fbb`.

Lookup latency in this fixture was 139 ms median and 558 ms p95. Captured
Cloudflare tail events for that version reported maximum CPU of 192 ms for
HTTP and 158 ms for Queue invocations, with maximum wall time of 7.8 s and
24.1 s respectively. Captured events had outcome `ok`; application admission
retries are still expected under contention. These are observations, not SLOs.

Initial 256-record publication attempts exceeded the 30-second host deadline
despite modest CPU use. This led to a default 16-record prefix, durable adaptive
batch reduction, separate interactive/maintenance instances, and reuse of the
authenticated packed body snapshot during cold flat-index rebuilds. The local
cold test published 44 changes over 256 existing documents using 30 object GETs
and 125,185 downloaded bytes. Snapshot bytes remain a cache: current facts
determine membership, revision, body digest, and length.

## Native model enrichment

A fresh incarnation on the same disposable resources also passed a one-message
end-to-end native enrichment fixture: publication recorded pending work, the
Queue invoked the configured Workers AI embedding model through Gateway, native
conditional enrichment appended its derived mutation, and publication made the
vector available to a fenced vector query. The configured model was
`@cf/baai/bge-small-en-v1.5`, with 384 dimensions.
After replacement of the deployed Worker version, the same mutation returned
the identical durable visibility receipt and the fenced vector query still
returned the message. The strict comparison used versions
`5d3304c6-6d6b-4065-8881-b29e81fb84a1` and
`12911922-7bd5-4171-8381-00a78feb957c`.

## Bounds and further qualification

Each WASM instance has a 32 MiB linear-memory cap; the shared immutable JS cache
has an 8 MiB cap. Total deployed isolate memory was not measured. Flat indexes
still rebuild in memory, and larger builds may exceed the admitted profile.
The test does not establish sustained production throughput, throttling
behavior, broad fault injection against deployed infrastructure, or capacity
for every model, chunking policy, tenant distribution, and query shape.

The local workerd suite additionally persists R2 and SQLite across complete
runtime restarts and exercises typed enrichment, immutable model profiles,
reader retention, and the collection grace period. Native and WASM tests cover
stale writers, ambiguous commits, pinned pagination, corruption, source changes
during inference, allocation failure, and bounded admission. Those deterministic
tests complement the live fixture; they do not imply every failure was injected
into deployed Cloudflare infrastructure.

Reproduce with the scripts under `examples/cloudflare-serverless/qualification`.
Keep generated configurations, tokens, bundles, and full reports under ignored
`dist/qualification/`. Disable the Queue consumer, drain the generated storage,
and delete the disposable Worker, Queues, and bucket after qualification.
The recorded run's Worker, both Queues, and R2 bucket were deleted after both
incarnations were drained.

Final local checks passed: 553 native serverless tests and 28 owner tests (six
skipped, no failures or leaks), 12 portable native contract tests, 24 WASM/host
tests, and the workerd restart suite. Both the native Binaryen 132 tool and the
pinned npm Binaryen build passed the WASM suite.


## Antfly-native R2 coordinator (2026-10-24)

The separate `wrangler.r2.jsonc` profile replaces application SQLite/KV metadata
with the shared Antfly ordered tree and immutable commit log. Its design,
checkpoint/fencing argument and frozen migration procedure are documented in
[the object-storage backend design](antfly-object-storage-backend.md).

Local validation passed the native serverless suite (536 passed, six skipped,
no failures or leaks), the separately registered shared ordered-tree/log/store
contracts, the Lite public package build, 58 Node tests and the workerd restart
and migration tests. The latter import original receipts, binary records,
embedding profiles and an active collection frontier; they also run both
collectors, resume reindex and erase platform storage before reopening.

The 100,000-record native metadata fixture checks all 99,872 surviving values
after deletion and three complete collections. It peaks at 1,234,266 native
working bytes and 20 MiB WASM linear memory. Retained fixture bytes stabilize
across repeated collection cycles; these are local admission measurements,
not deployed latency, total isolate memory or R2 billing figures. Exact commands
and measurements are in the backend design.

The disposable live Worker and bucket were recreated for the native profile,
using logical incarnation `qualification-antfly-1278-mvmnbmzg-f1070a-r2`.
Native metadata collection preserved the application count and accepted a late
job delivery without starting another collection. The deployment was then
rebound to a fresh `ReopenedR2CommitCoordinator` namespace against the same R2
data, preserving the original operation identities and active frontier.

Live R2 coordination adds substantial latency in this fixture. Initial
publication/racing-write attempts exceeded their deadlines; retries remained
conditional/idempotent. Deadline and lease contention return retryable 503s.
The implementation removes a redundant recovery walk per commit, groups queued
metadata callbacks, batches up to sixteen frontier parents per transaction and
bounds trace work by elapsed time. Rolling deployments fall back to individual
traces when an older coordinator lacks the batch operation. This profile remains
opt-in; these correctness fixtures do not establish a production latency or
sustained-throughput target. The earlier ANN recall limitation also remains.


The final live fixture completed on Worker version
`bb582a87-2e29-4b0c-90b6-026190f30ae7` after the namespace replacement. It passed
the original 300-document receipt, eight racing operation identities, fenced
lookup, publication, compaction, application collection and a finite
384-dimensional Gateway embedding. Native metadata collection passed alongside
the application collector. The quick fixture's three timed lookups had a
7,524 ms median and 10,781 ms maximum; three samples do not establish production
percentiles. The full run took about 8 minutes 39 seconds, including contention,
retries, maintenance and collection grace. This is correctness evidence with a
material latency limitation, not a throughput qualification.


After qualification, Queue delivery was paused, the consumer was explicitly
removed, and the test storage was drained. The Worker, both Queues and R2 bucket
were deleted. Removing a consumer from local Wrangler configuration alone does
not remove the remote Queue consumer. The native-profile drain route avoids
waking an empty coordinator, which would otherwise recreate authority metadata.


## Optimized R2 read and write path (2026-10-24)

The [performance implementation](cloudflare-performance.md) was qualified on
fresh disposable resources using incarnation
`qualification-antfly-1278-mvmnbmzg-f1070a-performance`.
The initial measurement ran on Worker version
`30e1cf47-5c8e-4c5b-b41a-e15f56bb818c`; recovery into a fresh
`ReopenedR2CommitCoordinator` namespace ran on
`4ddbcba7-c403-4595-8c54-fee8eb6b3d6d` against the same R2 data.

Each run checked 100 warm fenced lookups after 32 warmups. Initial median/p95/p99
were 124/386/499 ms; recovered values were 132/350/593 ms. No admission retries
occurred. Every body and the original receipt were verified, with an exact
final count of 264. The initial eight-writer burst took 13.51 seconds. The
single-snapshot path used one coordinator RPC per lookup. The recovered run's
warm measurements must not be presented as cold-start measurements.

The earlier 7,524 ms median came from only three successful lookups in a
different fixture. These results establish a substantial fixture improvement,
not a production speedup ratio or sustained-QPS target. These measurements
precede the native grouped-WAL and upload-identity changes recorded below.

Local checks passed 68 Node tests, the native serverless suite (538 passed, six
skipped, no failures or leaks), thirteen focused document-facts tests, and the
workerd recovery, migration, collection and tenant-isolation fixtures. They
cover packed-body corruption, shared-pack retention, pin reuse, decoded-cache
eviction, parallel-prefetch fencing, publication admission recovery, idle job
suppression and pending-enrichment continuation.


The final fencing audit also tests an old warm owner after 5,000 commits by its
replacement under a frozen checkpoint clock. Recovery without takeover authority
must fail before handing that owner another owner's bounded continuation. Both
the native loop and adapter enforce this invariant. After measurement, Queue
delivery was paused, its consumer removed, storage drained, and the disposable
Worker, both Queues and bucket deleted.


## Grouped WAL, publication leases and cold recovery (2026-10-24)

The final implementation and detailed results are in the
[performance design](cloudflare-performance.md#final-scale-measurements-2026-10-24).
Native WAL construction now shares one record-tree update, receipt-tree update
and HEAD CAS across callers. Publication snapshots reduce metadata reads, and
artifact scoping reuses a fresh native lease identity; final cutover still
renews ownership and performs the fenced HEAD CAS.

Qualification checked 2,048 documents across four tenants, concurrent reads and
writes, compaction interference, exact changed bodies and receipt replay. The
final mixed read median/p95 was 3.98/7.36 seconds, including 644 admission retries
across 96 calls. This exposes a foreground concurrency limit and prevents a
production throughput sign-off. After convergence, 132 count/lookup calls had
a 147 ms median and 453 ms p95 without retries.

A controlled fresh 64-document publication completed in 13.99 seconds with six
metadata RPCs, including four HEAD PUTs and one fencing-floor PUT. All bodies,
count and original receipt passed. The eight-writer burst completed in 2.70
seconds; fixture differences prevent a controlled ratio against the earlier
13.51-second burst.

Cold recovery rebound the same R2 storage IDs to a new physical coordinator
namespace with queue delivery paused and cron disabled. A version-only probe
avoids warming WASM or Durable Objects, and data responses must match the
expected version. The four first count requests took 3.40–4.80 seconds, each
on the intended version on its first attempt. Eight original/latest receipts
replayed unchanged. All 512 documents per tenant, exact payloads and changed
text survived; subsequent verification median/p95 was 159/430 ms without retries.
A first pass containing an old-version response is retained for audit and
excluded from cold measurements.

The final 10,000-document, 384-dimensional native fixture matched all brute-force
top-ten neighbors for ten held-out queries at full effort. Mean recall@10 was
0.22 with eight probes and 0.79 with 32. Candidate/ranking correctness is fixed;
low-probe routing quality remains a separate capacity gate. Final native tests
passed 542 cases with six expected skips and no failures/leaks; 74 Node tests
and three workerd recovery/migration/tenant tests also passed.

These are bounded synthetic qualifications. Total production isolate memory,
actual billed R2 cost, a long sustained workload and production routing/latency
SLA measurements remain unqualified. The reports keep those measurements unset
rather than inferring them from WASM memory or observed R2 attempts.


After the final verification, queue delivery remained paused, the consumer was
explicitly removed, and a quiescent drain removed 3,308 R2 objects totaling
22,634,774 stored bytes. This is final retained storage, not peak storage or
billed GB-months. The tail was stopped before resource deletion.

The disposable Worker, both Queues and the empty R2 bucket were then deleted
successfully. The version probe leaves native runtimes and coordinator state
cold but warms Worker routing/JavaScript; complete platform-isolate cold-start
latency remains outside this measurement.
