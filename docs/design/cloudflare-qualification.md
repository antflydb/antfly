# Cloudflare hosted profile qualification

This records successive disposable live runs on 2026-10-24–25 UTC for issue #1278.
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


## Isolated runtimes and prepared commits (2026-10-25 UTC)

The disposable R2-native deployment now uses tenant reader/build actors, bounded
shared kernels and WAL preparation outside the writer transaction. The workload
uses four independent commit domains, each with 512 documents carrying a 1 KiB
payload, paged text, publication prefixes of 64 and Queue concurrency four.
Automatic recurring GC/cron admission was disabled to control the mixed-load
comparison. This is a bounded load experiment, not arbitrary-scale production
sign-off.

| Phase | Fenced reads | Writes | Read median / p95 | Write median / p95 | Admission retries |
| --- | ---: | ---: | --- | --- | ---: |
| Initial isolated-runtime run, eight rounds | 96 | 32 | 1.43 / 18.17 s | 4.35 / 23.89 s | 0 |
| Repeat with fresh operation IDs, same corpus | 96 | 32 | 0.98 / 5.66 s | 3.59 / 7.14 s | 0 |
| 32-round soak with fresh operation IDs | 384 | 128 | 1.83 / 5.51 s | 4.34 / 6.51 s | 0 |

The soak also completed 32 compactions (median 10.41 s, p95 20.80 s), followed
by 132 exact count/body/payload/update checks (median 315 ms, p95 578 ms).
The previous local-lane comparison had read median/p95 3.98/7.36 s with 644
admission retries and write median/p95 3.10/4.64 s. Isolation removed observed
admission retries and improved the repeat/soak read distribution, but writes
were slower and the first isolated run had materially worse tails. Different
network conditions, cache warmth, deployment recovery and queue state remain
confounders. These runs establish progress and correctness, not a uniform
latency improvement or an SLO.

### Whole-isolate memory and attributed usage

The scoped `qualification/analytics.mjs` tool queries Cloudflare's
`durableObjectsPeriodicGroups` and `r2OperationsAdaptiveGroups` for this Worker
and bucket. At 02:50:07 UTC, before fresh cold namespaces were created, the
periodic memory samples were:

| Namespace class | Samples | p50 bytes | p95 bytes | p99 bytes |
| --- | ---: | ---: | ---: | ---: |
| R2ReadRuntime | 132 | 9,497,067 | 15,272,819 | 17,969,572 |
| R2CommitCoordinator | 68 | 14,998,176 | 19,470,680 | 20,246,908 |
| R2BuildRuntime | 56 | 11,263,917 | 13,339,492 | 13,663,361 |

Cloudflare documents these as [whole hosting-isolate measurements](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/),
which can include colocated objects. The percentiles are not per-object memory,
an absolute high watermark, or proof that every future workload fits 128 MiB.
They replace the earlier absence of actual platform memory evidence for this
bounded fixture; WASM accounting still remains separate.

The same snapshot attributed 11,704 successful GETs, 7,902 GET user-error
responses, 4,443 successful PUTs, three PUT user-error responses and nine LISTs
to the disposable bucket. These are adaptive analytics counts through that
snapshot, not finalized billable quantities. Bootstrap, seeding, multiple
versions and the mixed workloads share the interval. Account invoice cost
remains unmeasured; do not substitute transport counters or apply request prices
to every error response as if these were invoice line items.

### Larger local metadata qualification

`node qualification/r2-scale.mjs 100000 --compact --inline-checkpoint-commits=8` completed the actual native
metadata engine over an asynchronous R2 contract fixture, including reopen,
128 deletes, a complete ordered verification scan and three self-collection
cycles (1,541 bounded steps). It retained the expected 99,872 records. Maximum
native working memory was 1,232,890 bytes, linear memory 12,582,912 bytes, and
one request used at most 105 object operations / 3,492,818 transferred bytes.
The compact module's declared maximum is 16 MiB. The whole local run took
152 seconds with the inline-eight checkpoint experiment; these timings are not deployed R2 latency or whole-isolate memory.

The global vector routing benchmark records per-query GET attempts/bytes,
routing shape, raw-cosine recall and a full-effort exact-control assertion.
Requested probes remain hints, and format 2 intentionally spends a 25% route
margin beyond the existing adaptive budget. Changes to recall must be assessed
alongside that added I/O, not treated as a free clustering improvement.


The final 1,536-dimension fixture accepted and published 3,000 documents, then
passed cold self-neighbor queries, dense/sparse lookup, updates, deletion and
exact-control recall checks. A redundant decoded-training-sample copy initially
caused publication to exhaust the 32 MiB module around the training threshold;
transferring the already-owned sample into native training removed that copy.
The rerun needed no special recycling setting: peak native working memory was
14,344,743 bytes and peak linear memory 31,195,136 bytes. This is close to the
32 MiB limit, so it does not establish much growth headroom for a single step.
Three held-out queries measured mean recall@10 0.50 at eight requested probes
and 1.00 at 32/full effort. There were only 64 local routes; adaptive probing
plus the format-2 margin visits them all at 32, so that result is not a
low-I/O approximation. Average downloaded bytes were 4,732,856 at eight probes
and 11,941,946 at 32/full effort. Local ingestion/publication took 137 seconds;
this remains a storage-contract fixture, not live R2 latency.


At 10,000 rows / 384 dimensions, the ANN qualification module
`85b373c04632689927c98d6f138fa092246377f41d39992494255f2c15e71896`
produced 64 coarse centers, 64 directory shards and 127 local routing leaves.
Ten held-out raw-cosine queries measured:

| Requested probes | Mean recall@10 | Mean physical GETs | Mean downloaded bytes |
| --- | ---: | ---: | ---: |
| 8 | 0.45 | 62.2 | 2,014,707 |
| 32 | 0.86 | 122.6 | 6,221,038 |
| Full effort | 1.00 | 169.7 | 9,408,081 |

The preceding format-1 control measured 0.22/0.79/1.00 at those probe settings;
its report did not record these per-query physical counters. The new policy
uses both global routing and a wider route budget, so the result is not an
isolated clustering-quality comparison. At 1,000 rows the root remains format
1 and the ten-query results stay 0.85/1.00/1.00. The final 10,000-row run also
passed cold dense/sparse queries, updates and deletion. Peak working/linear
memory was 13,977,697 / 27,459,584 bytes; ingestion/publication took 156 seconds
on the shared local machine. Distribution drift and larger route tables still
need workload-specific qualification before production rollout.


### Recovery after the 32-round soak

With Queue delivery paused, fresh physical coordinator, reader and builder
namespaces reused the same logical incarnations and R2 storage identities.
The version-only probe addressed no actor, and every data response was required
to match `9ca77475-3c10-40dd-bd32-1cd6bfa8410e`. All four first counts returned
512; all 132 exact body/count/payload checks and eight old/new receipt replays
passed. The first counts took 35.02 s (two attempts), 13.83 s, 12.69 s and
10.27 s. One initial request needed a retry. Follow-up verification median/p95
was 417/882 ms. This was fresh native/actor recovery with warm Worker routing,
not proof of complete platform-isolate eviction.

The cold result exposed the cost of replaying up to a 64-slot native metadata
epoch. An inline-eight checkpoint experiment retained epoch size, immutable
commit authority, owner fencing and monotonic checkpoint validation. Hints obeyed
the one-second overwrite limit and a failed/throttled hint could not change the
accepted commit's outcome. The native regression verifies short-tail recovery
and retained durability while a hint is throttled.

That experiment passed a further eight rounds on one verified version
`a4ec5d7e-bfe1-49cb-819a-456909c4761e`: 96 reads, 32 writes and 132 exact checks.
Read median/p95 was 1.52/9.70 s and write median/p95 was 7.41/15.09 s, with no
read/write admission retries. Eight compactions had p95 39.18 s including 27
retries. Publication eventually converged; captured background work included
cancellation and lease-loss deferrals. These results do not establish a good
foreground tradeoff for inline-eight checkpoints.

The final R2 policy therefore retains inline-64 checkpoints and coalesces a
hint after eight new revisions and 250 ms of idle time, with a busy check and
one delayed retry. This keeps ordinary hint work out of the foreground ACK.
Physical kernel admission now also precedes publication lease acquisition;
pending jobs can expire without consuming a lease. The inline-eight dataset
remains in the report as evidence of the cost that motivated these changes.

The inline checkpoint experiment passed all 80 Node tests and all three persisted
workerd tests (156 seconds), including tenant capability rejection, grouped
writers, both collectors, erased platform storage and frozen SQLite migration.
Native checkpoint/fencing and global-routing allocation/GC regression gates
passed without leaks. The full main native serverless suite also passed 543
tests (six skipped) before the final checkpoint-policy tuning; the final targeted
gate reran the affected native log and global-routing cases.


### Final idle-checkpoint and lease-admission run

With the final idle policy and lease acquisition inside physical kernel
admission, version `ece291b0-23fd-4022-ade6-efd6e3352633` completed another eight
rounds on the same corpus with fresh operation identities. All 96 fenced reads,
32 writes, eight compactions and 132 exact verification checks passed with zero
admission retries. Read median/p95 was 0.82/6.31 s; write median/p95 was
4.18/8.00 s. Compaction median/p95 was 10.12/12.57 s, and final verification
median/p95 was 251/584 ms. This is a measured improvement over the inline-eight
experiment, with network/cache confounders still present. Write p95 remains
above the preceding local-lane baseline; production tail-latency sign-off is
still open.

`node qualification/r2-scale.mjs 100000 --compact --idle-checkpoint` also passed
with the final native-inline-64 / idle-hint policy. It reopened across eviction,
retained 99,872 expected rows after 128 deletes, performed the full ordered
verification scan and completed three collection cycles (1,543 steps). Peak
working memory was 1,233,355 bytes, linear memory 12,582,912 bytes, and request
traffic stayed within 105 operations / 3,493,001 bytes. The local run took 90
seconds on the shared development machine; comparisons with earlier local wall
times do not isolate a performance gain. The final 82 Node tests and all three
persisted workerd tests passed (132 seconds for workerd), including idle-hint
coalescing and rejection of expired kernel admission before lease acquisition.


The final recovery used another fresh physical coordinator/reader/builder class
set, preserving logical storage identities and rejecting every data response
outside version `13403e01-7697-46df-8d64-b4c3dd79a1d4`. After Queue delivery was
paused, first counts took 14.09, 8.10, 9.34 and 9.25 seconds; each returned 512
on its first attempt. All 132 exact checks and eight original/latest receipt
replays passed, with no admission retries. Follow-up verification median/p95 was
411/831 ms. No checkpoint was manually forced before those first requests.
Worker routing was warmed only by the version probe.

This reduces the worst observed first request from the longer-soak recovery but
does not establish a cold-start SLO: four samples have different metadata tails,
and 8–14 seconds remains a material latency gate. Sustained multi-region load,
large live embedding corpora, distribution-specific ANN recall, complete
platform eviction/failure injection and finalized billing still require further
production qualification. The final Node suite also passed the instantiation-
expiry race regression, bringing its total to 83 tests.


After qualification, Queue delivery stayed paused, the consumer was removed,
and the final quiescent drain deleted 5,492 objects totaling 37,850,602 stored
bytes. This is the end-of-test footprint, not peak occupancy or billed GB-months.
The disposable Worker, both Queues and R2 bucket were deleted successfully; no
test service remains deployed. The final observed tail contained 4,121 events,
five canceled/non-OK events, no flagged truncation and no captured exception
messages. Application-level publication deferrals are recorded separately and
must not be inferred from that non-OK count. Reports, snapshots, hashes and
scoped analytics remain in the ignored local qualification directory.

The branch subsequently merged `origin/main` at `730999bf2d` (29 newer commits)
without file conflicts. Live samples above identify their pre-merge deployment
versions. The combined branch is verified locally after the merge; these samples
do not constitute a new live qualification of unrelated upstream changes.


After merging main, the full native serverless suite passed 544 tests (six
skipped), with no failures or leaks; its ancillary native gates also passed.
The rebuilt module passed all 83 Node tests and all three persisted workerd tests
(166 seconds). Its SHA-256 remained
`b13b5c439a452fe091aee7b4f9c1897c7536e0edc112be4cf20b117c851b66ad`,
identical to the final live-tested WASM artifact. The merge therefore introduced
no native WASM byte changes. These local checks include the final queued/creating-
kernel expiry guards. Repository cache collection completed after validation.


## Split-service and native critical-path qualification

The new profile is implemented and tested separately from the earlier deployed
versions above. `qualification/critical-path.mjs` runs the actual native kernel
against an in-memory R2 contract fixture with 5 ms injected per-operation delay.
It compares recovery windows zero and eight with fixed tails and verifies the
same recovered value. Local wall time is illustrative; I/O waves are the more
portable observation. Speculative recovery can issue extra GETs at short tails.

| Tail commits | Serial waves | Window-8 waves | Serial / window-8 local ms |
| --- | --- | --- | --- |
| 0 | 11 | 10 | 66 / 60 |
| 7 | 26 | 12 | 151 / 70 |
| 31 | 74 | 15 | 426 / 89 |
| 63 | 138 | 19 | 791 / 114 |

Twenty metadata-only WAL/outbox commits used 40 versus
20 PUTs, and 110,765 versus
29,114 written bytes, for generic versus native commit. Local
wall time was 1033 versus 813 ms. Immutable document/WAL data
uploads, HTTP and live R2 latency are excluded. This is not a deployed latency
claim or billing estimate.

The 100,000-row compact-kernel regression with `--hot-root --idle-checkpoint`
passed after correcting temporary lookup allocations to use releasable scratch
memory. It retained 99,872 rows after 128 deletes, reopened repeatedly, preserved
both inline records and completed three collection cycles (1,542 steps). Its
largest branch had 193 children. Peak request working memory was 1,254,122 bytes,
linear memory 12,648,448 bytes, and request traffic stayed within 113 operations /
3,495,313 transferred bytes. Local total time was 114 seconds. The in-memory
bucket is outside native memory accounting; this is metadata scale evidence,
not a live 100,000-document application workload.

### ANN recall versus physical I/O

A synthetic 10,000-document, 384-dimensional paged corpus with ten held-out
queries was measured using `next-ann-all.json` and `next-ann-coarse32.json`.
Each row below averages the ten cold query samples. Full effort retains the
independently asserted brute-force nearest-neighbor control.

| Coarse cell limit | Requested local probes | Recall@10 | GETs/query | Bytes/query |
| --- | --- | --- | --- | --- |
| all (default) | 8 | 0.45 | 63.2 | 2,014,692 |
| all (default) | 32 | 0.86 | 123.6 | 6,221,023 |
| all (default) | all | 1.00 | 170.7 | 9,408,066 |
| 32 | 8 | 0.43 | 62.9 | 2,005,767 |
| 32 | 32 | 0.70 | 106.6 | 4,968,862 |
| 32 | all | 1.00 | 170.7 | 9,408,068 |

The 32-cell option trades recall for fewer bytes at 32 local probes and offers
little benefit at eight probes. It therefore remains explicitly opt-in; the
default retains all-cell route selection. These synthetic samples do not qualify
real embedding distributions or establish an equal-recall performance gain.

Node regressions cover overlapping physical I/O, bounded contiguous recovery,
fresh seal detection at a hole, large UTF-8 WAL heads, atomic outbox recovery,
lost acknowledgement, stale-owner fencing and warm decoded-reader reuse with
fresh retirement/frontier checks. Persisted workerd tests exercise both
collectors, erased platform state, frozen SQLite migration, remote service
bindings and a blocked builder while a new write and fenced read complete.
The latter proves independent admission capacity, not simultaneous CPU execution.


The final native gate passed 545 main-suite tests (six skipped), plus its
ancillary gates, with no failures or leaks. The rebuilt kernel passed all four
persisted workerd integration tests in 146 seconds. The Node suite passed 90
tests, including rejection of an already-expired fast-commit admission before
any recovery I/O. The final WASM SHA-256 is
`563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`.
These local checks do not replace versioned live measurements.


### Disposable split-service live run

Four tenants each contained 512 actual 1 KiB documents. Eight mixed rounds
completed 96 fenced reads, 32 writes, eight compactions and 132 exact checks.
Read/write admission retries were zero. HTTP responses spanned versions
`0edd383e-ab3e-4bf3-bdf0-7eddfe8ba448`, `15121fe1-a005-4dcc-81b4-84526c6c0d35`.
This mixed phase predates the final scratch-allocation and trace-reporting fixes;
it is not a fresh full mixed qualification of the final source. The cold run
below uses the final code and rebuilt kernel.

| Mixed phase | Samples | Median ms | p95 ms | Admission retries |
| --- | --- | --- | --- | --- |
| mixed-read | 96 | 1243 | 9382 | 0 |
| mixed-write | 32 | 3164 | 12020 | 0 |
| maintenance | 8 | 14762 | 32641 | 9 |
| verify | 132 | 253 | 665 | 0 |

Median writes improved relative to the earlier idle-policy run, while read,
write and compaction tails regressed. These uncontrolled deployments have
network/cache/placement confounders; the data does not establish a general
foreground speedup or a reason to switch the default hosting profile. The split
profile demonstrates bounded independent admission, and remains opt-in pending
sustained tail-latency qualification. Captured builder traces identify multiple
serial metadata RPC waits; eliminating that remaining publication chatter is a
separate optimization requiring protocol and failure testing.

Queue delivery was paused and fresh physical coordinator, reader and builder
classes were deployed while preserving logical storage identities. Every data
response in the recovery check was required to match final version
`98b3ee20-4f45-46b2-840c-6b6a2d3a8dda`. The version probe touched only application
routing. No manual checkpoint or database warmup preceded the first counts:

| Tenant | First count ms | Attempts | Documents |
| --- | --- | --- | --- |
| a | 3520 | 1 | 512 |
| b | 4314 | 1 | 512 |
| c | 3260 | 1 | 512 |
| d | 4101 | 1 | 512 |

All 132 exact checks and eight original/latest receipt replays passed with no
admission retries. Follow-up verification median/p95 was 219/501 ms.
These four first counts improve on the earlier 8–14 second observations, but
different tails and placements prevent an equal-work causal latency claim.
Fresh physical classes also do not prove complete platform isolate eviction.

Scoped analytics found all six old/new actor namespaces and returned three
sampled groups with 99 periodic memory samples. The largest reported namespace
p99 was 21,469,810 bytes. These whole-isolate observations can include colocated
objects, can lag, and are neither an absolute memory peak nor a complete sample
of the new classes. Scoped R2 adaptive usage is archived separately; billed cost
remains unavailable. The profile still needs sustained multi-region workloads,
real embedding distributions, broader failure injection and finalized billing.

The final trace-reporting refinement also passed the 90-test Node suite and
reran the split-service persisted recovery test successfully (38 seconds).


After explicit deletion approval, Queue delivery remained paused and its consumer
was removed. The quiescent drain deleted 1,763 objects totaling
14,048,051 stored bytes. These are final retained bytes, not peak
occupancy or billed storage. The application Worker, three private Workers, both
Queues and R2 bucket were deleted successfully. Owned tail processes stopped,
reports/logs remain in the ignored local qualification directory, and the
disposable local API token was removed. Repository cache collection completed.


## Acceptance-gate candidate: local evidence and live status

The next candidate implements two-boundary native publication, pure committed-
prefix observation, fresh read cohorts, read-view preparation before scarce WASM
admission, foreground-aware tenant admission, aggregate streamed ingress limits,
adaptive publication prefixes and no-op compaction preflight for paged roots.
The [gate design](cloudflare-gates.md) defines its protocol and acceptance policy.
This section records local evidence; it does not declare a live gate pass.

The native serverless gate passed 545 tests (six skipped), with no failures or
leaks, after the publication record-count correction. All 115 Node tests and five
persisted Worker integration tests passed on the final candidate. Node coverage includes
lost finalize acknowledgement, expired/replaced ownership, mandatory read-set
validation, fresh observer fencing, zero-write observation at epoch rotation,
late read-cohort admission, original grace deadlines, bounded ingress, lifecycle
scope checks and missing-credential rejection. Persisted Worker checks cover
both collectors, migration, grouped tenant writers, independent runtime admission
and fresh physical actor routing. The legacy SQLite-backed Worker also passes
its intentional queue-failure and SQLite-memory-error injection/recovery test;
those injected logs are not new live SQLite failures.

The 100,000-row compact-kernel test also passed with `--hot-root
--idle-checkpoint --observer`. It retained 99,872 expected rows after 128 deletes,
completed three collection cycles (1,542 steps), reopened repeatedly and performed
a committed-prefix read without any metadata PUT. Peak working/linear memory
was 1,254,069 / 12,582,912 bytes, and peak request traffic was 113 operations /
3,495,565 bytes. Local total time was 96 seconds; this is not a deployed workload
or a comparison that isolates latency gains.

The deterministic synthetic comparison uses the actual 16 MiB reader modules
and their 8 MiB working-set budget. At 10,000 documents and 384 dimensions, ten
held-out queries produced identical baseline/candidate recall and physical GET/
byte counts. The final artifact hash and results are retained in the ignored
gate directory. The reader recall stage peaked at 1,078,034 working bytes and
6,356,992 linear bytes. Deterministic test entropy and a logical fixture clock
remove variation from immutable identities; they do not model live latency or
lease failure. The evaluator additionally requires a frozen model-embedding
corpus, separate from this synthetic evidence.

Three baseline/candidate trial pairs were prepared against baseline commit
`d5102ee703`; its module was independently restored and SHA-256 verified as
`563936db3958bdab50c404db4a6ccef15e095e798e49198ab2304d87322dac97`.
The first pair was created, then cron and Queue delivery were paused and its
public endpoints disabled while destination-specific token/deletion authorization
is pending. Its bundles are sealed locally at the final candidate
revision and must be redeployed before measurements. The other two pairs have local manifests only.
No test token has been installed and no new live workload has been run.

The overall evaluator correctly remains red: three version-pinned one-hour
trials, their recovery populations and the model-embedding comparison are still
required. Short smoke runs, earlier deployment measurements and unit-test
fixtures of the evaluator cannot supply that evidence. No new foreground SLO or
production qualification is claimed here.


### Follow-up harness and larger local qualification

The fixed-arrival driver is shared with a separate capacity staircase. Added
regressions cover reversed HTTP write acknowledgement order, bounded pending
rounds, draining in-flight work on reporting failure, scheduling-versus-execution
latency, changed sealed bundles and incomplete ANN corpus/stage rejection.
All 128 Node tests pass. These changes strengthen the evidence path and do not
change the native serving module or the acceptance thresholds.

The 250,000-row metadata fixture passed with compact kernels, hot roots, idle
checkpoints and the read-only observer enabled. It retained 249,872 expected rows
after deletion, completed three collection cycles (3,828 steps), and verified
repeated reopen and zero-write observation. Peak native working/linear memory
was 1,285,203 / 12,713,984 bytes; the largest request used 137 operations and
3,603,374 transferred bytes. Total local time was 231 seconds. Retained fixture
storage after collection was 441,704,790 bytes. These are local object-fixture
measurements, not live latency, platform memory or billed storage.

The model corpus was captured through the existing Cloudflare account inference
API with AI Gateway routing, without installing a Worker secret. After refreshing
the existing session, all 1,010 embeddings were captured and frozen. Both modules
then passed full publication, cold/warm queries, updates, deletion and compaction
against 1,000 document embeddings and ten held-out query embeddings. At 8, 32
and full probes, both returned recall@10 of 1.0 and identical mean physical
traffic: 40.9 GETs and 1,116,461.4 bytes per query. The small generated-topic corpus
is limited distribution evidence; it does not establish production-corpus recall.

Native/Node/persisted Worker logs and both synthetic/model ANN comparisons are
now bound in the local hashed evidence manifest. The evaluator still rejects the
overall gate because the three paired live trials and fresh-actor populations
have not run. Destination-specific token and cleanup authorization is pending.


The additional 50,000-document, 384-dimensional paged-vector run accepted and
published all 50,000 records. Its format-2 root had 64 coarse centers and 1,024
routes/shards. Cold text/vector/sparse queries, warm lookups, point updates,
deletion and compaction passed with the actual compact reader. Ingest/publication
required 533 local seconds and peaked at 13,953,068 working bytes and 27,262,976
linear bytes in the builder. The reader's ordinary cold-vector stage peaked at
883,287 working bytes and 6,225,920 linear bytes.

The full-effort recall stage failed with `ObjectIoBudgetExceeded` after the first
two approximate operating points. This is **not** a passing 50,000-vector recall
qualification. It exposes the one-request exhaustive-scan limit even though the
working set remains bounded. The old benchmark process returned zero despite a
failed stage; this run's JSON retains the failure, and the corrected harness now
exits nonzero and rejects such evidence. The passing gate ANN populations remain
the separately validated 10,000 synthetic and 1,000 model document comparisons.
The [gate design](cloudflare-gates.md#bounded-exhaustive-vector-search) records the
required continuation work; no request budget or recall target was increased.


### Implemented FP32 exact continuations

The next implementation closes the 50,000-vector single-request failure with a
native, resumable FP32 scan and authenticated app continuations. The [protocol
and client contract](cloudflare-gates.md#bounded-exhaustive-vector-search) describe
snapshot protection, bounded heap/deduplication, expiry, cancellation and scheduling.
No persisted artifact format, SQL table or per-query durable job is added.

Validation passed 547 native tests (six skipped, no failures/leaks), 136 Node
tests and five persisted Worker integration tests. Regressions include complete
ranking over bounded ranges, multiple projections per document, allocation-failure
cleanup, host/actor eviction, publication during a scan, retired generations,
FP16 near-tie correction, cursor tampering/scope/query/expiry/key rotation,
duplicate resumes, cancellation and foreground admission between scan steps.

The deterministic 50,000-document fixture accepted and published every record.
Cold text/vector/sparse reads, warm lookups, updates, deletion and compaction all
passed. All ten held-out exhaustive queries matched the independent brute-force
cosine oracle (recall@10 1.0), each using 98 native steps. The combined recall
stage made 1,000 native requests: 980 exhaustive steps plus 20 approximate
queries. Peak working/linear memory across that stage was 919,111 / 6,422,528
bytes, and peak operations per request were 210. The native request guard retained
its 32 MiB transfer and 1,024-operation caps; incomplete scan steps additionally
asserted their 4 MiB page-read, 128-page and 512-record bounds. No request budget
was increased. The fixture used the actual 16 MiB compact reader and 8 MiB working
budget. The full module SHA-256 is
`15c0e07e42f29cf4f01032eb25f0ad8a295e6bd0c9792ed2202905fe3b6dd94e`;
the reader module is
`07ea32c6bf0d1b6cae323e769ca4c4ae6db532549fa8b433f6bbf6ea642eb1e5`.

| 50,000-document operating point | Recall@10 | Mean physical GETs | Mean bytes |
| --- | --- | --- | --- |
| 8 probes | 0.11 | 153.9 | 3,381,362 |
| 32 probes | 0.27 | 205.9 | 5,820,066.7 |
| FP32 exhaustive, all steps | 1.0 | 4,005.6 | 80,692,975 |

Local ingest/publication took 882 seconds and the combined recall stage took
17.6 seconds. These are object-fixture timings under concurrent local verification;
they are not deployed latency, total isolate memory or causal performance gains.
Exactness remains O(N*d), and the low approximate recall at this larger synthetic
population is an additional measured quality limit rather than a production
recommendation for those probe counts.

An equal-settings 10,000-document comparison (384 dimensions, 1 KiB payload,
publication prefix 64) confirmed unchanged approximate operating points:
recall 0.39 / 0.80 at 8 / 32 probes, with exactly matching physical GET/byte counts.
Both full controls returned recall 1.0, but the new FP32 exhaustive scan used
825.7 GETs / 16,349,359.9 bytes versus the old FP16 control's 171.7 GETs /
9,418,480.9 bytes. The frozen 1,000-model-embedding corpus also passed every stage
and returned recall 1.0 at all three points. Its FP32 full control used 100.9 GETs /
1,805,026.4 bytes; approximate counts stayed 40.9 GETs / 1,116,461.4 bytes.

The larger scan failure is fixed. The original gate's no-extra-I/O condition is
**not passed** by the FP32 full control, and its paired live trials/recovery
populations remain outstanding. The precision distinction and added cumulative
I/O are explicit; thresholds have not been relaxed. Prepared deployments must be
resealed with the new native/host candidate before any live qualification.


### Shared page-engine batching follow-up

The next candidate connects FP32 scanning to the existing native packed-page,
coalesced-range and authenticated-cache capabilities. It preserves the balanced
incremental tree updater and the same page format. The [performance design](cloudflare-performance.md#shared-native-exact-scan-io)
records the bounded capture/fallback and read-ahead rules.

Matched deterministic runs use 384 dimensions, 1 KiB document bodies, a
64-record publication prefix and ten held-out queries. The control is the prior
FP32 continuation implementation (`15c0e07e...` full / `07ea32c6...` reader),
not the older FP16 control. Both candidates use the actual compact reader and
its unchanged memory/transport limits.

| Workload / statistic | Prior FP32 | Shared batching |
| --- | ---: | ---: |
| 10,000 synthetic: ingest/publication PUTs | 75,662 | 57,039 |
| 10,000 synthetic: full mean GETs | 825.7 | 795.7 |
| 10,000 synthetic: full mean bytes | 16,349,359.9 | 16,378,055.9 |
| 1,000 model embeddings: full mean GETs | 100.9 | 91.9 |
| 1,000 model embeddings: full mean bytes | 1,805,026.4 | 1,807,982.4 |
| 50,000 synthetic: ingest/publication PUTs | 528,204 | 390,454 |
| 50,000 synthetic: ingest/publication GETs | 1,491,511 | 1,433,905 |
| 50,000 synthetic: ingest/publication GET bytes | 33,309,534,461 | 33,832,245,460 |
| 50,000 synthetic: native publication transfer accounting | 58,722,242,263 | 64,718,077,457 |
| 50,000 synthetic: full mean GETs | 4,005.6 | 3,857.6 |
| 50,000 synthetic: full mean bytes | 80,692,975 | 80,837,140 |

Every stage passed in the 10,000 synthetic and 1,000 model populations. Full recall@10 remains 1.0; approximate
recall and GET counts are unchanged. Packed-reference metadata slightly increases
bytes, so this is a PUT/GET-count improvement, not a byte non-regression claim.
The 10,000-document combined recall stage peaked at 1,068,493 working bytes and
6,291,456 linear bytes. Builder peaks were 13,948,191 working / 27,459,584 linear
bytes. At 1,000 model documents, recall was 1.0 at all three operating points.
The final 50,000-document run also passed every stage and all 30 recall samples.
All ten exhaustive queries matched the independent FP32 oracle. The format-2
root retained 64 coarse centers and 1,024 routes/shards; approximate recall stayed
0.11 / 0.27 at 8 / 32 probes. The combined recall stage used 1,000 native requests
and peaked at 1,368,616 working / 7,405,568 linear bytes and 210 operations per
request. These remain below the unchanged 8 MiB reader working and 16 MiB linear
limits. Publication used 26.1% fewer PUTs, while its physical GET bytes rose 1.6%
and native transfer accounting rose 10.2%. The latter includes native transport
accounting and is not a measurement of total isolate memory or deployed network
latency. Packing reduces object operations at a byte and memory cost; it does not
establish an across-the-board resource non-regression. The original gate remains
unmet, including at this larger population.

A separate 1,000-document mixed-version run used the previous compact reader
against newly packed roots and passed all stages and exhaustive ranking.

Verification passed 548 native serverless tests and 59 supporting tests (six
skipped, no failures or leaks), 136 Node tests and five persisted Worker tests.
The updated ordered-page suite also passed separately, including allocation-
failure cleanup, upload failures, old-snapshot reads, read-ahead cancellation and
packing overflow. Coalescing regressions verify duplicate/out-of-order references,
corruption rejection, and cleanup before cache publication.

The final full module SHA-256 is
`b739955c120caa5abc7f97e663792bcec90e37138e2b3aa616c0f7704843314b`;
the compact reader is
`b088f0d2090d50aa608c41e0745d8585566ab7c482c819d8a331ddeb698e0a1c`.
Local reports are `dist/qualification/batching-final-10k.json`,
`batching-final-50k.json`, `batching-final-model-1k.json` and
`batching-legacy-reader-1k.json` under the
Cloudflare example. They report physical I/O over all continuation steps, not
just the last request. Fixture elapsed time does not establish deployed latency.

The original FP16-relative no-extra-I/O gate remains unmet. The paired live trials
and fresh-actor populations are still required; existing prepared deployments
must be resealed and deployed with this candidate before live qualification.

### Compact blocks, immutable runs and certified refinement

The [shared object-backed vector design](cloudflare-vector-blocks.md) replaces
whole-base vector rewrites with bounded immutable runs and checkpointed merges.
Exact queries scan compact candidate planes, discard only certified losers and
resolve ambiguous candidates from lossless residuals or revision-bound original
document bodies. Page-local dictionaries remove repeated residual identities.
These are shared native object-store capabilities, including for S3 and GCS;
R2 supplies the transport used by this qualification fixture.

The frozen original FP16 baseline is unchanged. Each pair uses the same corpus,
publication prefix, document body size and ten held-out queries. Synthetic runs
use 10,000 vectors, 384 dimensions, zero padding and a 16-record prefix. Model
runs use 1,000 vectors, 384 dimensions, 1 KiB padding and a 64-record prefix.
The fixture remains `@cf/baai/bge-small-en-v1.5`, SHA-256
`7b086ae74552c90455f2d23cfebe531efff791124f8569ffeb515aeb41fb440d`.
The evidence builder now rejects mismatched workload settings and query sets.

| Corpus / probes | Baseline recall | Candidate recall | Baseline mean GETs | Candidate mean GETs | Baseline mean bytes | Candidate mean bytes |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| Synthetic / 8 | 0.45 | 0.45 | 63.2 | 63.2 | 2,014,720.1 | 1,881,946.4 |
| Synthetic / 32 | 0.86 | 0.86 | 123.6 | 123.6 | 6,221,050.7 | 5,699,761.2 |
| Synthetic / all | 1.0 | 1.0 | 170.7 | 140.3 | 9,408,093.9 | 9,154,594.7 |
| Model / 8 | 1.0 | 1.0 | 40.9 | 40.9 | 1,116,461.4 | 1,029,878.4 |
| Model / 32 | 1.0 | 1.0 | 40.9 | 40.9 | 1,116,461.4 | 1,029,878.4 |
| Model / all | 1.0 | 1.0 | 40.9 | 39.4 | 1,116,461.4 | 1,107,684.7 |

Both corpora meet the original ANN recall/GET/byte conditions at every operating
point. Full scans use the independent FP32 oracle and count physical traffic
across every continuation. No request limit or gate threshold was increased.
This clears the local ANN condition; it does not establish the live latency,
queue stability or recovery conditions.

Verification passed 550 native serverless tests and 61 supporting tests (six
skipped, no failures or leaks), 150 Node tests and six persisted Worker tests.
Regressions cover exact source-bit reconstruction and conservative bounds for all
three metrics, multiple projections, continuation eviction, source-revision
mismatch, namespace isolation, allocation failures, malformed dictionary pages,
residual retention after GC, and maintenance upload/HEAD acknowledgement failures.
A separate 1,000-model-document run with explicit checkpointed reindex passed
all eleven stages (`release-final-reindex-1k.json`). Its reindex peak was 4,399,695
working bytes and 11,862,016 linear bytes.

The final 50,000-document run passed every stage, with all accepted records
published and all ten exhaustive queries matching the independent FP32 oracle.
The format-3 root retained 64 coarse centers and 1,024 routes. Recall at 8 / 32
probes remained 0.11 / 0.27. Exhaustive queries averaged 502.5 physical GETs and
43,870,917.5 bytes, compared with 3,857.6 GETs and 80,837,140 bytes for the earlier
packed format-2 candidate. The first exhaustive query used 98 bounded requests,
refined 40 records and certified 49,960 losers. Publication peaked at 866 native
operations, 28,738,184 transferred bytes, 11,787,994 working bytes and 27,459,584
linear bytes per request. The limits remain unchanged.

Publication and run maintenance used 506,962 PUTs, 1,627,476 GETs and
38,397,579,947 physical GET bytes. Native transfer accounting was 67,223,338,211
bytes. The query improvement comes with extra publication and maintenance work;
it is not a universal object-cost reduction. A scale failure at 39,000 records
identified inherited page-size growth across maintenance checkpoints. Explicit
128 KiB checkpoint pages fixed that failure. Deletion-only merge prefixes now
advance their exclusive checkpoint even when no surviving output is produced.
Clustering still uses original FP32 vectors: a candidate-plane-only prototype
regressed the frozen synthetic recall and was rejected.

The initial live attempts failed during setup, before an hour-long workload or
fresh-actor recovery population was measured. The candidate's previous feedback
controller interpreted fixed R2 I/O wait as per-record execution cost, reduced
publication to one record and could not recover. Those failed reports are
retained. Feedback now uses native operation count, transfer volume, working-set
peak and elapsed execution excluding measured I/O wait. Guard bands remain
inside existing limits. Underfull WAL tails cannot trigger growth, healthy full
prefixes recover a reduced budget, and early cancellation or queue delay does
not reduce it. A native execution exhausting its own deadline remains a capacity
failure. The new local regression tests cover these distinctions. Paired live
latency, queue stability and recovery remain unproven until new trials finish.

Live feedback subsequently demonstrated recovery from one record to the full
64-record prefix without increasing limits. Two partial measured attempts still
showed multi-second read and write tails and were preserved and stopped. A read
trace spent 1,147 ms in coordinator view admission and 216 ms in native execution;
other traces showed read-only admission falling back to the writer queue. A local
regression reproduced `ReadSnapshotNeedsPin` immediately after publication.

Publication now establishes the visible generation's ten-minute read protection
inside the same native metadata transaction as HEAD publication and lease
release. The first reader therefore does not need a pin-write admission behind
maintenance. This uses the existing native metadata store and does not cache a
mutable frontier or weaken fencing. The regression verifies that the first
read-only view performs no metadata writes. Snapshot pools also use the injected
clock consistently, which makes this test exercise the intended admission path.
The same protection applies to individual HEAD CAS calls used by compaction,
reindex and legacy publication. The persisted fixture runs both prepared and
legacy publication and rejects any successful HEAD update lacking read
protection. The updated 148-test Node suite and six persisted Worker checks passed.

An interrupted setup can explicitly replay its acknowledged seed operation IDs.
Resume validation requires all 32 seed acknowledgements across four tenants on
both sides, matching deployment/native identities, no workload samples, no cold
epochs and no successful paired setup convergence. Measured execution identities
remain fresh and the report records the resumed seed identity. The deployment
profiles must identify that report as their latest setup execution. Starting
workload traffic invalidates setup replay. Before measurement, the runner checks
all sealed private-service hashes as well as the app version. This prevents
repeated failed setup from adding another 512-record WAL prefix per tenant;
measured attempts must seed again. No measured latency, sample-count or recovery
condition is relaxed. The atomic-protection change still requires live trials.

A subsequent three-minute diagnostic completed without request errors and checked
counts, written bodies and receipts. Candidate read/write p95 was 2,140 / 6,465 ms,
versus 5,671 / 7,681 ms for the control (72 reads and 24 writes per side).
`all-heads-smoke-2.json` is explicitly not qualification evidence. Both candidate
targets remained unmet. Traces showed actor admission queues and durable writes
waiting behind multi-second collection transactions.

Read views now prepare ahead of actor execution, with a 32 KiB reservation per
request and the original deadline. Reader actors use the existing two kernel
slots concurrently; each native instance remains serialized and builders remain
single-execution. Collection fanout and cleanup checkpoint within 64 metadata
mutations, and metadata tracing yields between complete tasks. A changed fanout
enumeration restarts conservatively without dropping prior discoveries. New
regressions hold both read executions while a third request prepares, and resume
partial collection after eviction and a lost acknowledgement. The 150-test Node
suite and six persisted Worker checks pass; live qualification remains required.

The final full module SHA-256 is
`503c23da5d45fbc245507592f646e5d83142bdee783b5364179a2ba14f155f0e`;
the compact reader is
`12f9ee49df8f4d8857f7ffa131eebccb2b9f571f6e88c97d0e00702f37860dc1`.
Reports are `dist/qualification/release-final-10k.json` and
`release-final-model-1k.json`, `release-final-50k.json` and
`release-final-reindex-1k.json` in the Cloudflare example. Earlier failed prototypes
remain separate evidence. Reader and collector rollout must precede format-3
writers; older readers fail closed on the new roots and dictionary flag.


The five-minute admission/collection diagnostic (`admission-quanta-smoke-2.json`)
completed 120 reads and 40 writes per side with zero request errors and verified
written bodies, receipts and all four 512-document counts. Candidate read/write
p95 was 2,140 / 4,106 ms versus 5,432 / 5,304 ms for the control. This remains a
failed latency precheck, not qualification. No physical cold epochs were used.
Traces still showed serial native object I/O and foreground commits waiting for
collection commits. The next candidate therefore batches private tree-page
uploads and optionally inlines one small latest WAL record in its HEAD, as
specified in `cloudflare-vector-blocks.md`. These changes preserve hard budgets
and require separate binary compatibility activation before live measurement.


The native I/O candidate full module is
`6e1d4f5d11167b54967f70aadcc977184112b9c6b7bf27e561d9d48852f6bc3d`;
its compact reader is
`48686d1eac77bdc0ed213377f0dd2ceaf4cb6a5ecbfd9515f6c800b8e7a85755`.
The native suite passed 616 tests with six skips and no failures or leaks; all 151
Node tests passed. Frozen `wal-final-10k.json` and `wal-final-model-1k.json` retain
the same six recall/physical-I/O results as the previous released candidate.
Both reports explicitly enable the inline-WAL policy and bind these new hashes.
The explicit 1k reindex workload also completed on this candidate. The 50k run
and persisted Worker checks are recorded separately when completed. These local
results do not establish the live latency or fresh-actor gate.


The five-minute native-I/O diagnostic (`native-io-smoke-2.json`) recorded 120 reads
and 40 writes per side. Candidate read/write p95 was 1,699 / 5,006 ms versus
3,939 / 5,617 ms for the control. Final verification timed out on one candidate
body lookup after 57 bodies and eight receipts had been checked. The original
failed report is retained; this is neither a passing smoke nor gate evidence.
Read admission now batches migration and view metadata and reuses decoded rows
only after a fresh observation with an exact revision/epoch/owner match. Hosted
immutable caches also populate after confirmed create-only uploads, within the
unchanged 8 MiB byte budget. All 154 Node checks pass, including ambiguous-upload,
byte-limit, concurrent-revision and owner-replacement cases.

Two grouped persisted-Worker attempts completed five tests but cancelled the
split-runtime test at its original 120-second timeout, after its functional
checkpoints. An isolated rerun passed in 112.9 seconds. Neither grouped attempt
counts as passing evidence. Evidence validation now rejects cancelled, skipped
or incomplete Node/Worker runs even when Node prints `fail 0`; six completed
Worker checks and the current required Node regressions are mandatory.


`wal-final-50k.json` completed all ten stages on the native I/O candidate, with
50,000 accepted/published records, exact recall 1.0 for all ten exhaustive queries,
mean exhaustive-query GETs 502.5 and bytes 43,870,917.5. Publication used 506,962
PUTs and 1,623,466 GETs; peak native operations were 866, peak working bytes
11,787,994 and peak linear bytes 27,459,584. Frozen ANN and explicit reindex
reports use the same full and compact hashes stated above.

The five-minute write-through diagnostic (`write-through-smoke-2.json`) completed
120 reads and 40 writes per side, checked 80 written bodies and eight receipts,
and had no request errors. Candidate read/write p95 was 3,177 / 3,751 ms versus
5,551 / 6,871 ms for the control. It still fails both latency targets. Traces
showed read admission waiting over one second on the shared metadata kernel.
The next revision reserves one of the writer's existing two compact slots for
read-only observations, retaining its 64 MiB total pool ceiling. All 155 Node
checks pass, including a suspended-mutation-kernel observation regression. The
latest grouped Worker attempt on write-through alone again timed out in the
split-runtime fixture and is not passing evidence.


The reserved-observation revision passed all six persisted Worker checks in
228.9 seconds; the split-runtime fixture completed within its unchanged
120-second timeout. Its diagnostics confirm exact counts before eviction,
disposal, and the same receipt after reopening. This is the completed passing
run; earlier timeouts remain recorded separately. A subsequent weighted WAL pool
passed all 157 Node checks, including cancellation and starvation prevention
under a fixed memory reservation ceiling, plus actual compact-writer/reader
compatibility. Its own persisted Worker and live checks remain required.


The reserved-observation five-minute diagnostic
(`snapshot-reserve-smoke-2.json`) completed candidate read/write p95 of
1,241 / 2,372 ms, verified its written bodies/receipts and reported no candidate
errors. The control had three request errors and p95 of 5,567 / 6,694 ms. Both
the read threshold and the error-free paired-run condition remain unmet. This
report remains diagnostic and consumed no cold epochs.

Current main advanced to `9d152a4c78` after the earlier merge. Its media changes
require the combined facts-v10 encoding described in the vector-block design,
audit/dependency-aware bounded collection, shared public mutation validation and
lazy derived-WAL body resolution. The first merged native run passed 583 tests
with six skips; all 157 Node checks pass on rebuilt full module
`5779de2b1e7ce566c0932e9671e2b0a28dc666708eda836a73b018a6a96a3379`
and compact module
`755589db3f043688f1b799862d59c1a20a61a1ba37fd525d906b5b01bff21450`.
Additional test-discovery anchors, persisted Worker tests and frozen corpus
comparisons are being validated separately. Prior reports retain their original
native hashes and cannot qualify this merged build.


The merge into `origin/main` at `9d152a4c78` completed as `fff792c6c0` with the
mandatory formatter hook enabled. The final merged native run passed 654 tests
with six skips and no failures/leaks, including the now explicitly discovered
live-overlay regressions. All 157 Node tests and all six persisted Worker tests
passed; the latter completed in 231.9 seconds. The merged synthetic/model ANN
comparisons pass every original threshold, with 64 additional bytes per query
from the expanded facts root. Explicit reindex also passed on the merged hash.
The merged 50k run remains separate until completed.

`merged-weighted-smoke-2.json` completed five offered minutes, 120 reads and
40 writes per side, verified 80 written bodies and eight receipts, and recorded
zero errors. Candidate read/write p95 was 2,483 / 5,306 ms versus 4,820 / 5,864 ms
for the control. It remains below neither required latency limit. The next
observation-tier revision passed 158 Node checks and both persisted epoch modes;
its complete Worker and live checks remain required. Its observation module is
`fbe713457024a7d55ea81b3022f6184e240d6667dea11593c9ad394c217d053c`.
Earlier fixture runs missing this module are retained as failed test runs.


The merged 50k run completed all ten stages with 50,000 accepted/published
records and exact recall 1.0 for all ten exhaustive queries. Mean exhaustive
GETs are 502.5 and bytes 43,870,981.5. Publication used 515,590 PUTs and 1,637,559
GETs; peak native operations were 871, working bytes 11,788,282, linear bytes
27,525,120 and transferred bytes 28,867,885. `merged-50k.json` binds the merged
full/compact hashes; `merged-reindex-1k.json` also completed all eleven stages.

The observation-tier diagnostic (`observer-tier-smoke-2.json`) completed three
offered minutes and exact final verification. Candidate read/write p95 was
2,586 / 3,964 ms versus 7,034 / 6,268 ms for the control. One Queue-metrics
endpoint returned non-JSON HTTP 500, recorded as request/monitor errors. This
run fails qualification. Read-only probes subsequently observed a non-empty DLQ;
its cause must be investigated and its work recovered before qualification.

The final shared writer pool passed all 159 Node checks and all six persisted
Worker checks in 220.6 seconds. The latter assert three shared writer slots,
two observation slots, their 48/16 MiB budgets and the 64 MiB total ceiling.
Passing logs must contain the pool-reservation and observation regressions.

The shared-writer five-minute diagnostic (`shared-writer-smoke-2.json`) verified
80 written bodies and eight receipts with zero request errors. Candidate
read/write p95 was 2,352 / 6,553 ms versus 4,518 / 5,346 ms for the control.
All measured requests completed in one attempt; client scheduling delay p95 was
4 ms. It fails both latency thresholds and write non-regression. A non-empty DLQ
also disqualifies this run.

The single DLQ task was retained and inspected rather than purged. Its original
publication was obsolete after the tenant reached published/latest LSN 2,155
and a fenced count verified all 512 documents. Only then was that message
acknowledged; `dlq-recovery.json` retains its original task, proof and successful
one-message acknowledgement. The temporary pull consumer was removed. This
recovery does not turn the earlier diagnostic into a passing trial.

Native recovery traces showed that a fresh read unnecessarily acquired writer
ownership. The next revision adds authenticated, zero-write cold observation
and batches the two independent hints after an authoritative seal. The final
native run passed 656 tests with six skips and no failures/leaks; all 160 Node
checks passed. Full, compact and observation module hashes are respectively
`809ca58e5cf5c483e2cb665b9ed5cf958f0b0fa0a663b0f570c0bec2fe78178a`,
`2d86aa2e2ae8f64b7fc319feeedbf260d7b0ca917dccc5bb054dc306a323f214` and
`cfeaa1f3b99366c83a29babcb24d7780d25a4d72cd9b202528132ffabf299ed4`.
Persisted Worker, frozen ANN and live measurements must qualify these exact
artifacts before they can replace historical evidence. The strict gate remains
unproven, and all reserved cold epochs remain unused.

The cold-observer revision passed all six persisted Worker checks in 221.8
seconds. An earlier run cancelled the suspended-builder case at its unchanged
120-second deadline; that failed log remains excluded from evidence. Optional
phase timestamps show the replay after isolate eviction taking about 55 seconds
in the local runtime, while the independent writer committed during the builder
suspension. Both final frozen ANN datasets pass the original recall, physical
GET and byte comparisons on the final full/compact hashes.

`cold-observer-smoke-2.json` completed five offered minutes, 120 reads and 40
writes per side, verified 80 bodies and eight receipts, and recorded no errors
or dead-letter messages. Candidate read/write p95 was 1,464 / 2,733 ms versus
4,620 / 4,702 ms for the baseline. Writes meet the absolute and relative limits;
reads still fail the one-second limit. This is diagnostic evidence, not a gate
pass, and consumes no reserved cold epochs.

The next JavaScript revision moves zero-write metadata observation into the
reader, sharing its existing 32 MiB reservation bound, and overlaps bounded
metadata log retirement proofs. Its 164 Node regressions cover local authority,
owner changes, migration cutover, forged cursors, pin-only fallback, bounded GET
concurrency and draining failed speculative work. Persisted/runtime and live
measurements must qualify this revision before any strict gate claim.

All six persisted direct-reader checks passed in 229.3 seconds after the fixture
was updated to measure local preparations rather than writer RPCs. Old-writer
fencing remains required; read-only observers can follow a fresh authenticated
prefix after ownership changes. The first local-reader diagnostic exposed
serial replay from warm advisory cursors; bounded read-ahead now follows their
new commits and epoch transitions. Stable lookup placement also avoids cold app
isolate counters concentrating point reads. All 166 Node checks pass on these
changes; their new live measurement and the full one-hour trials remain required.

The first direct-reader diagnostic (`direct-reader-smoke-2.json`) completed its
offered workload but failed post-run verification when the next deployment
changed its version. It remains failed evidence. The subsequent clean
`local-tail-smoke-2.json` completed all final checks with 80 bodies, eight
receipts and zero request errors. Its read/write p95 was 2,638 / 6,669 ms against
5,038 / 5,818 ms for the control, so neither absolute threshold passes and writes
regress. No reserved cold epochs were consumed.

Single-commit asynchronous idle checkpoints are being measured separately.
Their 167 Node checks pass. The local split fixture now allows 240 seconds for
its two runtime startups and eviction replay; measured cold replay alone takes
55–64 seconds on this host. Its independent writer must still commit within
five seconds while the builder is suspended. This fixture setup allowance does
not change any live gate latency, duration, sample, retry or correctness limit.

The final native hash also completed `cold-final-50k.json` (all ten stages) and
`cold-final-reindex-1k.json` (all eleven stages). All ten exhaustive queries in
each fixture return exact recall 1.0. The 50k publication used the same 515,590
PUTs and 1,637,559 GETs as the merged fixture, within its original native budgets.

`idle-floor-smoke-2.json` completed exact verification without request errors but
failed latency: candidate read/write p95 was 2,672 / 7,022 ms against
5,392 / 6,335 ms. Traces then identified uninstrumented R2 listing and deletion
inside the exclusive metadata transaction. Durable deletion intents move those
calls outside the transaction. Its first persisted test found full-key intents
exceeding 32 KiB; compact suffix intents fix that limit while retaining support
for in-flight full-key intents. All 170 Node checks and all six persisted Worker
checks pass on compact intents, including a full 128-key batch and eviction.

The first deletion-intent live diagnostic completed exact verification of 80
bodies and eight receipts with no request errors. Candidate read/write p95 was
1,759 / 2,616 ms versus 5,504 / 5,928 ms. Writes pass the absolute and relative
limits; reads still fail. The next comparison uses the hot coordinator snapshot
path by default, with local reader observation opt-in. A complete strict gate
still requires all three one-hour trials and their unused cold epochs.


The compact-intent hot-RPC diagnostic completed with candidate read/write p95
1,281 / 2,571 ms versus 4,708 / 5,534 ms, without request errors. Seven historical
metadata-collection DLQ messages still invalidated that run. Each was recovered
through its authoritative job ID, proved complete, then acknowledged using its
exact lease and retained body hash. A subsequent audit showed zero DLQ messages;
the original failed report remains unchanged.

Confirmed immutable publication hints and variant-aware pool recycling pass
175 Node checks and all six persisted Worker checks (239.6 seconds). Merging
`origin/main` at `0c753dc705` also passes 656 native checks with six skips; the
rebuilt full, compact and observer WASM hashes are unchanged, preserving the
existing frozen ANN and 50k fixture binding.

The seeded five-minute hint diagnostic (`trial-2-smoke.json`) verified all 2,052
body/count checks and eight receipts per side. Candidate read/write p95 was
1,212 / 2,761 ms versus 6,398 / 6,509 ms. One candidate write returned HTTP 409
with an opaque Cloudflare internal-error reference after 11,436 ms. This remains
failed evidence: read latency exceeds one second and zero errors are required.
The run used the hint changes before variant-aware recycling was deployed. No
reserved physical cold epochs were consumed. Fresh live traces are required to
attribute hint hits and classify the infrastructure failure; it is not evidence
of an out-of-memory cause or of a successful strict gate.


The regional-placement, bounded platform-error diagnostics and cold-admission
preflight revisions pass 179 Node checks. All six persisted Worker checks pass
in 214.1 seconds, including a region-hinted compute generation composed with
physical epochs, native fencing, eviction and the suspended-builder check.
A separate 60-second diagnostic of the previous live hint revision completed
without request errors but measured read/write p95 1,704 / 3,195 ms (24 reads and
eight writes); it remains diagnostic and unqualified. Regional live placement
must be measured independently before a performance claim.


All three disposable candidates now run sealed revision `0775540a8c` with
`RUNTIME_GENERATION=regional-1` and `R2_LOCATION_HINT=wnam`; frozen controls retain
their original configuration. The prior bundles and profiles are archived.
The first regional five-minute diagnostic verified 80 bodies and eight receipts
without request errors: candidate read/write p95 was 1,308 / 2,748 ms versus
4,922 / 5,663 ms. Its complete read distribution still fails the target. The
last three minutes measured read p95 896 ms after publication supplied hints;
this subset is diagnostic only and does not replace full-run gate accounting.
Of 108 captured point reads at that point, 75 used two or three immutable hints,
with recent reads using zero to two physical GETs. The final Queue sample had
zero DLQ messages, backlog four and oldest age 12,679 ms.

Three independent strict paired trials have been launched on this revision.
Their results, complete-hour latency distributions, exact verification and
conditional cold admission remain pending; initiation is not a gate pass.
The content-addressed local evidence manifest has been refreshed with the
179-test Node log, six-test persisted Worker log and final native log.


The first three strict regional executions were stopped after disqualifying warm
failures and preserved under their original execution IDs. Trial 1 recorded
`ManifestReadLeaseExpired` with `clock_valid:false`, read timeouts and two writes
failed by a 30-second `blockConcurrencyWhile` initialization reset. Trials 2 and
3 recorded read timeouts. None completed an hour or consumed a physical cold
epoch. They are failed evidence, not completed gate trials.

Investigation reproduced the pool scheduler executing queued actor B work in
actor A's async context. Originating-context admission, pre-entry clock refresh
and bounded authority loading address these distinct failure paths. Their
183 Node regressions pass, including the original fail-closed clock rollback
check. New persisted checks cover queued R2 I/O contexts and control RPCs during
a suspended real R2 coordinator authority load. Live qualification must rerun
on the resulting sealed revision; those tests alone do not establish the cause
of every timeout or a gate pass.


The final context/authority revision passes all eight persisted Worker checks
in 189.3 seconds and all 183 Node checks. The new runtime fixtures verify
originating contexts across actual R2 I/O and release suspended authority
initialization through a control RPC. A post-stop audit found zero DLQ messages
on all three candidates. Failed trial reports, prior bundles and configurations
are archived before the next compute-generation rollout. Native module hashes
and frozen ANN/50k report bindings remain unchanged.


The `context-1` trial 1 completed a full offered hour with 1,439 successful reads
and 480 writes, 60 Queue samples, zero DLQ/resource errors, exact 2,052 body/count
checks and eight receipt replays. One read timed out after 35 seconds. Candidate
read/write p95 was 1,834 / 3,862 ms against 7,405 / 8,894 ms for the frozen control.
The warm preflight rejected it and preserved all 100 cold actors. Trial 2 failed
setup publication convergence; candidate status checks had converged while the
control still lagged. Trial 3 was stopped after timeouts and an opaque internal
error. None of these executions passes the gate.

Durable hints, metadata-only mutation admission and bounded warm transport replay
pass 188 Node checks. All eight persisted Worker checks pass in 266.0 seconds,
including hint recovery on a fresh physical coordinator in prepared and legacy
modes, both collectors, reindex, erased platform storage and migration. Native
module hashes remain unchanged. The next comparison enables local native read
admission using those durable hints. Recovering a stalled control redeploys its
unchanged sealed bundle; it does not alter the frozen native code or parameters.


The ten-minute local-reader diagnostic (`durable-local-reader-smoke-3.json`)
completed 240 reads and 80 writes per side without request/resource errors and
verified 80 bodies per side and eight receipts overall. Candidate read/write p95 was 1,336 /
8,823 ms versus 6,214 / 7,027 ms for the frozen control. This fails both absolute
latency limits and the relative write limit. No reserved cold epoch was used.
Local native metadata admission was active; traces still showed expensive
metadata recovery and commits. Repeated unowned snapshots suggest reconstruction,
but do not establish its cause or prove a memory failure.

The Cloudflare adapter now derives the native owner from the physical Durable
Object ID, deployment version and storage namespace. Provider serialization of
that physical actor permits the same deployment to resume ownership after
reconstruction, avoiding an unnecessary epoch rotation. A changed physical actor
or deployment has a different owner and must pass native fencing. Generic hosts
and adapters missing valid provider identity/version metadata retain random
owners. This policy requires exclusive execution of each provider actor; it is
not a general concurrent-host owner-sharing protocol. Regression checks cover
reconstruction, changed-version fencing and conflicting same-owner roots.
Constructor metrics identify actor instances so live reconstruction can be
measured rather than inferred from missing volatile state.

Read-only recovery now starts with one speculative log slot and doubles its
window only after consuming a complete authenticated contiguous prefix, up to
the existing eight-slot limit. Checkpoint and epoch changes reset the window.
Idle observation therefore reads the checkpoint, seal and next slot rather than
probing eight future slots. Fresh seal checks, native validation, operation/byte
bounds and mutable recovery behavior are unchanged. These changes pass all 191
Node checks and all eight persisted Worker checks in 323.1 seconds, including
receipt replay after reconstruction and progress behind a suspended builder.
Live qualification remains pending.


Revision `56a45c53c2` completed two additional five-minute diagnostics. Local
metadata admission (`owner-adaptive-smoke-3.json`) completed 120 reads and 40
writes per side without request/resource errors, verifying 80 bodies and eight
receipts overall. Candidate read/write p95 was 9,092 / 9,664 ms versus 6,544 /
8,138 ms for its frozen control. Constructor metrics confirm reconstruction of
a physical writer during one deployment, without a reported resource failure.
The RPC diagnostic (`owner-rpc-smoke-2.json`) measured candidate read/write p95
3,049 / 4,348 ms with 120 successful reads and 39 writes; one write returned an
opaque HTTP 409. Its control measured 6,848 / 7,652 ms and failed a compaction
lease. Both reports retain those errors. Neither is a passing trial, and all
300 reserved candidate cold actors remain unused.

An optional `EDGE_IMMUTABLE_CACHE=true` layer uses the Cloudflare Cache API for
confirmed immutable bodies up to 64 KiB. Its key includes the stable storage
namespace, bound bucket, full object key, range and ETag condition. The layer
shares the existing immutable-key allowlist; mutable checkpoints, required-floor
markers, WAL/catalog heads, absence, errors and ambiguous writes are excluded.
Only confirmed create-only uploads and successful GETs populate it. R2 remains
the durable authority, and native checkpoint/fencing, read-view and collection
validation still run. Cache entries grant no ownership or retention rights.

Cache metadata is bounded to an 8 KiB encoded header and every streamed response
is bounded to 64 KiB before allocation and checked against its stored SHA-256.
Corrupt entries and cache outages fall
back to R2; resource failures remain visible. Cache writes use the originating
actor's background lifetime and copied bytes. No cache promise or I/O handle is
shared between actors. Physical R2 instrumentation remains below this layer;
cache hits, misses, bytes and writes have separate metrics. The feature defaults
off and does not change native module hashes or ANN traffic budgets.

All 197 Node checks pass. The persisted Cache API probe confirms actual cross-
actor cache hits with zero R2 calls and fresh reads of changing mutable heads.
All nine persisted Worker checks pass in 388.3 seconds, including both
collectors, migration, erased platform storage, native fencing and receipt replay
after eviction. Sealed cache-enabled profiles also pin the cache setting and
local metadata-admission mode across all four roles. Live qualification remains
pending before a performance claim. The platform cache is an accelerator that can miss
or disappear at any time; see the [Cloudflare Cache API documentation](https://developers.cloudflare.com/workers/runtime-apis/cache/).


The first cache-enabled local-admission diagnostic
(`edge-cache-local-smoke-3.json`, revision `a97b8b98d7`) completed five minutes
with 120 reads and 40 writes per side, no request/resource errors, 80 verified
bodies and eight receipts overall. Candidate read/write p95 was 2,000 / 3,501 ms
versus 5,921 / 6,712 ms for the frozen control. Live metrics confirm Cache API
hits. This remains above both absolute latency limits and is not a gate pass.
The paired RPC/cache diagnostic (`edge-cache-rpc-smoke-2.json`) also completed
120 reads and 40 writes per side with no request/resource errors, 80 verified
bodies and eight receipts overall. Candidate read/write p95 was 1,771 / 2,811 ms
versus 5,688 / 6,690 ms for its frozen control. Writes meet the absolute target,
but reads still fail; neither short diagnostic qualifies the strict gate.

Cache fills now admit at most two background writes and 512 KiB of charged
response data per isolate, with no waiting queue. Saturation skips cache fills
and leaves the confirmed R2 result intact. Only numeric admission counters are
shared between actors; each admitted fill starts in its originating I/O context.
Every operation admits at most 32 Cache API calls, shared across its transport
wrappers and retries. Combined instrumented R2/cache traffic stops at 960 calls,
leaving headroom beneath the paid platform quota for other operation I/O. The
existing 50k native qualification's peak of 871 object operations remains within
this bound with the full cache allowance. Cache hits still incur native validation.
The new admission regressions pass as part of all 199 Node checks; persisted
Worker validation and a fresh live diagnostic remain required. Platform Cache
API calls share the [Workers subrequest quota](https://developers.cloudflare.com/workers/platform/limits/).


The cache now reuses a confirmed full object for range and conditional reads
after checking its complete digest and requested ETag, then slices the bounded
bytes. A failed condition falls through to R2. Partial entries remain isolated
by their exact range/condition. The persisted cross-actor probe verifies this
path with zero R2 calls; no full-object prefetch or extra R2 bytes are introduced.
Cache bytes consumed and bytes served are reported separately.

Cache-enabled deployments require `EDGE_IMMUTABLE_CACHE_SCOPE`, a UUID unique to
the R2 bucket incarnation and shared by all four roles. It is combined with
the logical storage namespace, preventing independent buckets with identical
logical names from sharing entries. Keep it stable across actor/deployment
reconstruction and rotate it when recreating the bucket. Qualification preparation
generates it, and sealed provenance pins it. These regressions bring the passing
Node suite to 200. All nine persisted Worker checks pass in 385.3 seconds,
including cache-enabled collection, eviction, migration and the suspended-builder
check. Combined-I/O budget exhaustion uses typed retryable admission.


A subsequent Queue audit found two dead letters on the frozen control in
`edge-cache-rpc-smoke-2.json`; its candidate had zero. This independently
disqualifies that diagnostic even though its HTTP workload completed without
request/resource errors. The report remains unchanged while those messages are
inspected and recovered. Formal trials must reseed after these measured
diagnostics; setup-only replay is no longer valid for their modified corpora.


The bounded range-cache RPC diagnostic (`range-cache-rpc-smoke-2.json`, revision
`e5690b6b6c`) completed 120 reads and 40 writes per side with no final HTTP or
resource errors, 80 verified bodies and eight receipts overall. Candidate
read/write p95 was 1,692 / 8,256 ms versus 5,391 / 6,381 ms for the control.
Both absolute targets remain unmet. The control still had a dead letter in its
last sample, independently disqualifying the run. The original two inspected
control messages were durably replayed with unchanged maintenance tickets and
acknowledged only after accepted enqueue responses and fresh fenced 512-row
counts for all four tenants. Their message hashes, replay and acknowledgment
proofs remain local. This does not rewrite any failed diagnostic.

A transport-only probe measured version-endpoint p50/p95 46 / 200 ms. A separate
paired idle probe measured p50/p95 50 / 225 ms with persistent connections versus
97 / 283 ms with default connections after a 5.5-second idle gap. These are
diagnostics, not database qualification. The paired client now pins Undici
7.29.1 and retains connections for 60 seconds, with capacity for 128 concurrent
connections per origin. Both sides use the identical transport policy, recorded
in their reports; fixed arrivals, retry accounting, deadlines and cold single-
attempt requirements are unchanged.

The optional direct app reader removes one actor hop while retaining fresh
coordinator admission and the same native kernels. All 200 Node checks pass and
all ten persisted Worker checks pass in 289.0 seconds. The new fixture proves
concurrent tenant isolation, fresh physical coordinator recovery, old-writer
fencing and the 32 MiB app budget without invoking a reader actor. It then
restarts every isolate, erases platform storage, disables the edge cache and
verifies data and receipts again. Native module hashes and frozen ANN evidence
remain unchanged; live direct-reader performance is not yet qualified.


The first direct-app diagnostic (`direct-app-smoke-2.json`, revision
`e7008b3fca`) completed the offered five minutes with 120 reads and 40 writes
per side, no HTTP/resource errors and zero DLQ messages in the final Queue
samples. Candidate read/write p95 was 1,314 / 3,011 ms versus 5,672 / 6,747 ms
for the frozen control. Both absolute limits are still missed. The completed
report verified 80 bodies and eight receipts overall. No reserved cold epoch was used.

Queued observation cohorts now close at native kernel admission rather than
when their short batching timer expires. This coalesces callers waiting for the
same fresh observation without admitting any member after observation starts.
Transport I/O provides a second boundary hook when a host omits pool stages.
New regressions cover that hook, the close boundary, independent view copies,
old completion/new cohort races, separate read/mutation capacity and recovery
after failures. All 203 Node checks and all ten persisted Worker checks pass
(the latter in 279.3 seconds). New live qualification remains required.

The frozen control's DLQ was subsequently observed empty through its pull API,
and its temporary HTTP pull consumer was removed. Recovery proofs retain both
original task hashes and accepted replays; this operational recovery does not
turn the earlier failed runs into passing evidence.

The ten-minute queued-cohort diagnostic (`admitted-cohorts-smoke-2.json`,
revision `97dd2b1a05`) completed 240 reads and 80 writes per side with no final
HTTP/resource errors. It verified 160 exact bodies and eight receipts overall.
Candidate read/write p95 was 1,345 / 3,363 ms versus 8,813 / 10,769 ms for the
frozen control. Both absolute limits remain missed. Final Queue samples had
zero dead letters on both sides. All 300 reserved cold epochs remain unused.

A local capacity probe then completed 256 point lookups over 512 documents with
1 KiB bodies using the existing eight-MiB module; peak linear memory was
4,849,664 bytes. The direct app reader now admits a bounded point-read tier,
with native allocation and completion limits, and explicit promotion to the
general lane for larger reads. Regression coverage verifies four concurrent
eight-MiB reservations, exact 300 KiB body recovery, preserved caller budgets
and cancellation identity, and no promotion of resource/integrity failures.
This optimization has not yet passed the live gate.

All 205 Node checks and all ten persisted Worker checks pass (the latter in
296.7 seconds). The six qualification Worker checks pass again after restricting
the new tier to the explicitly configured app runtime. Native modules and ANN
fixtures are unchanged. Existing one-hour runs retain their sealed earlier
candidate; the new tier must be measured separately before replacing them.

The bounded point-read diagnostic (`point-read-lanes-smoke-2.json`, revision
`ac2be90316`) completed 240 reads and 80 writes per side, with no final HTTP or
resource errors. Candidate read/write p95 was 892 / 3,221 ms versus 6,445 / 9,844 ms
for the frozen control. The read target was met; the write target was missed.
Post-workload publication convergence then timed out before exact verification,
so this is failed evidence, not qualification. The reports and original data
remain retained. Separately, control 1's extended setup audit found three
published tenants and a fourth at LSN 2,962 of 3,216, with a canceled builder and
publication budget one. No live OOM was inferred from that cancellation.

Qualification retries now have a paired fresh-corpus preparation command that
preserves prior histories rather than appending another seed to them. Provenance
pins tenant incarnation/storage identities and rejects corpus changes or reuse
between trials. The evaluator also checks control-side publication and Queue
coverage, bounds, dead letters and stability; previously it checked those
frontiers only for the candidate. Numerical thresholds remain unchanged.

All 209 Node checks pass after the fresh-corpus and control-evidence changes.
The previous pair 1 trial ended with setup publication-convergence failure;
pair 3's earlier-version soak was intentionally stopped before completion, with
its untouched partial report hashed/archived before recording the interruption.
No recovery epoch was consumed. The three retry pairs have distinct prepared
corpus IDs, each covering the default namespace and all four tenant namespaces.
This is not a gate pass; the fresh deployments still need complete live trials.

The optional app-local metadata path passes 211 Node checks and all eleven
persisted Worker checks (452.5 seconds). Both direct-reader modes preserve
fencing and R2-only reconstruction. Native mode's warm reads succeed while
coordinator read RPCs are deliberately blocked; cold counts still reach fresh
physical coordinators. The caller-cancellation unit fixture now uses a referenced
timer, avoiding Node cancellation when an unreferenced timeout is its last event.
The live point-1 profile remains unchanged while these tests run.

The frozen control's caller-context and admission-clock defects were reproduced
independently against its unmodified source/native artifacts. A correctness-only
replacement and hashed evidence are prepared for review in
[the control proposal](cloudflare-control-correctness.md). This is not permission
to modify an existing frozen comparison or reinterpret its failed reports.

Latest main was merged through `9d553bc925` in `f71498ec3f`, including the
serverless/lake performance changes and `lake.read` naming. The sole conflict
ported authenticated child pruning into the extracted shared ordered engine;
lookahead does not speculate past arbitrary pruning hooks. Forward/reverse
pruning and read-ahead regressions pass. Lake rank-cursor coverage now runs from
its existing shared-engine test artifact with strict runtime selection.

The rebuilt candidate passes 657 native tests (six skipped), 211 Node checks and
all eleven persisted Worker checks. Both lake targets complete with 236 passes,
one skip and no failures/leaks. The retained-cut fixture now verifies the full
legacy payload and distinct native/legacy cache entries, then forces bounded
private replay in a separate cold cache. It no longer assumes allocator layout
must make an ordinary legacy replay uncacheable. A final focused replay check
also passes; production cache quotas are unchanged.

Both frozen ANN populations pass unchanged recall/GET/byte requirements with the
new native artifacts. The refreshed 50,000-document, 384-dimensional, 1 KiB-body
report passes all ten stages; sampled maximum linear memory is 27,525,120 bytes,
working set 11,788,282 bytes and 871 operations per step. The 1,000-document
reindex report also passes all eleven stages. These remain local native capacity
measurements, not live Cloudflare latency qualification.

The user approved the separate correctness-only control `21205a7021`, retaining
the original native modules. Original-control reports and sealed bundles are
archived before replacement. New comparisons use the app-local metadata profile
and fresh paired corpus identities. Final live gate evidence is still required;
all 300 reserved recovery epochs remain unused at this checkpoint. Transport
failure reports retain only bounded symbolic cause codes, without copying
headers or relaxing replay safety.

The first approved-control/native-app trial was interrupted after its existing
samples made the fixed one-hour read gate mathematically unattainable: 98 reads
already exceeded one second, versus at most 72 allowed among the full 1,440
offered reads. The preserved snapshot has 312 reads and 104 writes per side;
candidate p95 is 1,674/2,804 ms versus control 6,174/7,905 ms, with zero final
request/resource errors at that snapshot. This is a failed partial trial, not a
one-hour result. Its original report is archived before interruption annotation;
all reserved cold epochs remain unused. Captured app lookups make zero metadata
RPCs, but slow fresh observations replay dozens of records over eight to nine
I/O waves. The background recovery-floor change documented in the performance
design is being validated before another live candidate is sealed.


The checkpoint candidate passes 215 Node checks. The first persisted Worker
attempt timed out in split-role and legacy SQLite fixtures: its generated
outdir retained historical content-addressed WASM files, and fixtures enumerated
all of them for every role. The test preparation now removes only that generated
outdir before bundling the current dependency closure (three WASM modules).
Timeout and diagnostic logs remain preserved. The clean-bundle rerun passes all
12 persisted Worker checks in 142.6 seconds, including both timed-out fixtures.
The final 215 Node checks also pass, including checkpoint execution on the actual
eight-MiB module and sixteen-MiB observation pool. This startup problem does not
establish an OOM in a deployed Worker or a gate pass.

The corrected-control pair-2 short comparison also failed before measurement:
baseline publication did not converge within the unchanged 30-minute setup
limit. Both sides acknowledged all 32 setup batches, the candidate converged,
and neither recorded final request/resource errors. Its report is archived.
The existing setup-only resume path may replay those same operation identities;
it never reuses a corpus after measured workload mutations.


The checkpoint smoke comparison encountered Cloudflare Queue-metrics error
10250 ("Queue is overloaded. Please back off."). That request remains a failure
in its report. The shared qualification wrapper now maps only that exact
platform-overload exception to JSON 503, using the driver's unchanged bounded
logical retries and deadline. Unrecognized errors still fail. A regression checks
successful retry and fatal unknown/resource errors; all 216 Node checks pass.
This changes the same control-plane probe for both sides, without changing data
operations, native modules, numerical gates or any prior report.

Post-setup audits found control DLQ jobs on pairs 2 and 3, including original
publication identities for the current corpora. Recovery re-enqueues their exact
JSON bodies on the corresponding original main Queue before acknowledging each
leased DLQ message. Body hashes and accepted replays are retained locally. Queue
health must be re-audited before any formal workload; this operational recovery
does not turn either setup timeout into a passing trial.

The background-checkpoint five-minute pair-1 diagnostic finished with candidate
read/write p95 of 1,374/2,193 ms versus control 5,672/7,073 ms. The candidate
verified all 2,052 bodies/counts and eight receipts, with no resource errors. Its
recorded Queue-metrics failure and the control's verification-time
`ManifestReadLeaseExpired` invalidate the run, and candidate reads still exceed
the one-second target. No cold epoch was consumed.

A separate actual-workerd reproduction now proves that metadata transaction
groups in the approved control execute subsequent native work under the first
RPC's context. This is distinct from the already corrected kernel pool. The
candidate fix hands each group to a selected waiting submitter and defers a
failed owner's response until its surviving group commits or rolls back. All
217 Node checks and 13 persisted Worker checks pass. A correctness-only control
proposal with unchanged original native artifacts passes its 90 original Node
checks; see [the transaction control proposal](cloudflare-control-transactions.md).
These local correctness results do not qualify live latency or retroactively
repair the failed control comparisons.

The user explicitly approved corrected control `8a61f42df8`. All prior control
bundles/reports were archived before deployment, and the three pairs received
fresh corpora. Both original control WASM hashes remain unchanged. Post-deploy
recovery replayed three remaining control-2 jobs and one control-3 job to their
original Queues before exact-lease ACK; both DLQs were empty on the subsequent
pull audit and temporary HTTP consumers were removed.

The first new pair-1 comparison failed before measurement: the control
acknowledged 32 seed batches, while the candidate acknowledged 30 before one
tenant exhausted the unchanged sixty-second transport deadline. No workload or
cold recovery ran. A separate actual-workerd reproduction identified a
candidate mutation-group continuation defect. The candidate now hands both
mutation groups and metadata response settlement to their owning admissions.
It passes 222 Node checks and 14 persisted Worker checks. The approved control
is unchanged. These tests establish the correction, not that every transport
timeout had this cause or that the live latency gate has passed.

Qualification phases now cancel and await a failed phase's peer before closing
its report, preventing continued setup I/O after a fatal snapshot. Transport
exhaustion retains the last exception as its cause, without copying credentials;
actual HTTP attempts are counted. Stopping during retry backoff sends no further
request. The offered load, successful-run deadlines and retry limits are unchanged.

Live diagnostic capture now uses a tested streaming whitelist. It persists only
finite numeric metrics, fixed operation labels, UUID trace IDs and recognized
error codes. HTTP events, headers, bodies, arbitrary strings and raw CLI stderr
are discarded before writing. This replaces raw log capture rejected by automatic
approval review and introduces no credential upload or new approval requirement.

Latest main, including the regular Cloudflare task providers and Gateway option,
was merged through `a69c0b582e` in `19f4c5226f`. The artifact-store conflict retains
both hosted body packing and shared namespace identity. The merged native
serverless target passes 657 tests (six skipped), and `lib-cloudflare-test` passes.
Its WASM rebuild has new identities, so refreshed ANN/scale evidence is staged
separately from the still-sealed pre-merge comparisons.

The c732 short pair-1 run completes all 2,052 body/count checks and eight receipt
replays on each side with zero final HTTP/resource errors. Candidate read/write
p95 is 1,426/2,755 ms versus control 5,986/7,337 ms. It remains a five-minute
comparison with reads above target. The c732 full pair-3 trial accumulated final
transport failures and was interrupted: its original snapshot is archived before
annotation, with all cold epochs preserved. Pair 2 subsequently exhausted the
unchanged setup deadline because control publication did not converge. These
reports remain failed or incomplete evidence.

New actual-workerd and host fixtures reproduce the remaining runtime-actor
context leak and small frozen-clock gaps. The candidate fixes preserve the
approved control `8a61f42df8`, resource budgets and numerical thresholds. The
merged candidate with those fixes and the partitioned cache passes 226 Node and
15 persisted Worker checks. The provider guide and Worker README now distinguish
regular provider configuration from the typed binding bridge and link the shared
contracts. Extra live model availability or extra WASM task bridges are not
inferred from the merge or these fixtures.

Both refreshed ANN workloads pass the unchanged paired checks: 10,000 synthetic
vectors and 1,000 model embeddings, at 8, 32 and exhaustive probes. Recall is
unchanged, exhaustive recall is one, and R2 GETs and bytes do not exceed the
original control. At exhaustive synthetic search, average GETs fall from 170.7
to 140.3; the model workload at eight probes transfers 1,029,942 bytes versus
1,116,461. The evidence validator rejected initial refreshes that omitted the
candidate's inline-WAL setting. Both workloads were rerun with that setting
enabled before sealing the replacement manifest. These local reports are
separate from the pre-merge live trials.

The merged lake targets pass 236 tests with one skipped. A refreshed 50,000-
document workload completes ingestion/publication, cold scalar/vector/sparse
reads, warm reads, updates, deletion and compaction on the rebuilt module. Its
sampled peak linear memory is 27,525,120 bytes and working memory 11,788,282 bytes.
Both non-inline and deployment-matching inline WAL profiles complete all nine
stages with those sampled peaks. The inline-WAL 1,000-document reindex workload
also completes all ten stages. These are local capacity checks, not live latency
or whole-isolate memory measurements.

Latest main was subsequently merged through `9ace47d363` in `583fe1863b`. The
new macOS precision probe passes; the Worker and serverless runtime source is
identical to sealed candidate `4b8bdb0375`. All three candidate stacks deploy
that candidate, with approved control `8a61f42df8` unchanged and fresh paired
corpora. The former c732 bundles and reports remain archived. Three remaining
control-2 DLQ jobs were replayed with their original bodies/identities before
exact-lease acknowledgement; a subsequent empty pull was recorded and its
temporary consumer removed. The new live trials must finish independently
before any gate result can be claimed.

The rebuilt candidate's first full pair-3 comparison was stopped after a
recorded control write returned HTTP 409: Cloudflare reset its Durable Object
when `blockConcurrencyWhile()` exceeded the platform limit. The original report
was archived before interruption annotation; the candidate had no final request
or resource errors at that checkpoint. No cold epoch ran. This remains failed
evidence, including all observed startup and retry latency.

The shared driver now recognizes only that exact constructor-reset response as
recoverable for warm read-only operations and mutations with a stable operation
ID. It reuses the original input within the unchanged twenty-attempt/sixty-second
budget, and records all elapsed time and actual attempts. Cold requests,
identity-free mutations, compaction, unknown errors and reset messages carrying
resource errors remain fatal. This changes neither control native artifacts nor
its initialization, algorithms, limits or gate thresholds. Regression cases
cover replayed receipts, unsafe and cold exclusions, unknown errors and retained
resource failures. Candidate warm observers also overlap their initial proof
reads; those changes require a new sealed candidate and independent live run.
The updated candidate and driver pass 228 Node checks and 15 persisted Worker
checks. Native modules are unchanged from the merged-source scale runs.

Separate workerd probes also reproduce the two remaining caller-context defects
in approved control `8a61f42df8`: both mutation groups and runtime-actor queues
execute separately admitted `a`, `b`, `c` under contexts `a`, `a`, `a`. A
[two-file correction proposal](cloudflare-control-callers.md) preserves original
capacities and native artifacts, passes all 90 original Node tests, and observes
`a`, `b`, `c` in both probes. The user explicitly approved replacement control
`c44896ac8c`; its deployment and fresh comparisons are separate steps.

The five-minute read-overlap pair-3 comparison completes with zero final request
or resource errors and all 2,052 body/count checks plus eight receipt replays on
each side. Candidate read/write p95 is 1,036/2,628 ms versus control 4,590/6,039 ms.
Reads remain 36 ms above target, and the comparison lacks the full soak and cold
coverage. It does not pass the gate. The prior pair-1 control subsequently
exhausted the thirty-minute publication setup deadline despite both sides
acknowledging all 32 seed batches; the candidate had converged. Its failed report
is preserved before pausing the pair for the approved-control transition.

Approved control `c44896ac8c` was deployed on all three pairs with original
native hashes, alongside candidate `7f16e0d470` on fresh corpora. Exact replay
recovered three control-1 and three control-2 DLQ jobs; both temporary consumers
were removed after empty pulls, and all six DLQs then reported zero.

This cohort still does not qualify. Pair 1 failed setup when candidate writes
exhausted the unchanged sixty-second deadline; pair 2 exhausted the control's
publication setup deadline. Pair 3 was stopped after more than 72 completed
reads exceeded 1,000 ms, making the fixed 1,440-read hour's p95 target impossible.
The original partial report was archived before interruption annotation. No
cold epoch was consumed.

Sanitized traces identified candidate storage suspensions lasting 68–74 seconds
inside thirty-second native requests. The host/pool drain correction described
in [performance design](cloudflare-performance.md) passes 231 Node checks and
16 persisted Worker checks, including late durable-commit recovery and a real
RPC context/reservation handoff. The candidate native modules are unchanged at
this checkpoint; live deadline and latency qualification remains outstanding.


The next candidate adds bounded native read capsules and reader-first activation,
with 234 passing Node tests, 18 passing persisted Worker tests and 660 passing
native tests (six skipped). The full R2 eviction/collection/reindex/storage-erasure
fixture now runs with format-three roots. Preparation bounds unpublished seed
work to one tenant and 64 documents on both sides under the original thirty-minute
setup budget. Native binaries changed, so ANN, capacity evidence, sealed profiles
and live trials must be refreshed. No reserved cold epoch has been used; the gate
remains unpassed at this checkpoint.


The capsule binaries pass the refreshed 50,000-document capacity run: all nine
stages pass, including cold point/vector/sparse reads, updates, deletion and
compaction. Peak linear memory is 27,525,120 bytes and peak working allocation is
11,788,282 bytes, within the existing module budgets. The 1,000-document reindex
run passes all ten stages. Both frozen ANN corpora pass all six comparison
points with unchanged recall, equal/fewer physical GETs and fewer bytes read.
These are local scale results; they do not supply live soak or recovery coverage.

Late physical memory/CPU failures now have fixed-code settlement diagnostics.
The updated qualification harness captures and checks all eight live service
streams, rejecting gaps and resource failures before recovery. The implementation
passes 239 Node tests and 18 persisted Worker tests. A separate read-only live
probe connected all eight streams and closed its exact temporary sessions.
The first capsule live comparison is still a diagnostic, not a gate pass.


The capsule smoke execution `4f7b6188-704c-4649-bc39-b77cf7fc22e6` completes its
five-minute offered workload. The candidate passes all 2,052 body/count checks
and eight receipt replays with zero final/resource errors. Its full-population
read/write p95 is 2,439/3,628 ms versus control 6,073/7,386 ms; startup samples
are included and no gate pass is claimed. Control exact verification fails two
reads with `ManifestReadLeaseExpired`, which the common client had not recognized
as recoverable. The narrowly scoped warm retry is described in the gate design.

Before cold qualification, native review reproduces and fixes oversized selected
pin keys and delete payload coherence. Paired trials 2 and 3 are gracefully
interrupted during setup, preserving their failed reports and sanitized journals;
no measured workload or cold actor is consumed there. All pairs are paused for
new native binaries, refreshed evidence and fresh paired corpora.


Corrected pin binaries pass 18 persisted Worker tests, including both collectors,
reindex and platform-storage-erasure recovery. The common-client and diagnostic
suite now contains 241 tests. Final diagnostic evaluation independently checks
all eight journal paths, byte counts and hashes; truncated or substituted
journals cannot qualify. Hashing streams bounded chunks, with a 256 MiB ceiling
per journal. The native regression logs preserve both pre-fix failures separately.


The first three corrected-pin trials are intentionally interrupted during setup
because the initial diagnostic classifier incorrectly treats a completed native
cancellation as unsettled binding I/O. Two controls emit such completed
cancellations; there are zero observed resource signals across all six profiles,
and all 24 diagnostic journals verify. These reports stay failed and archived.
The host now reports the exact `io_draining` state. Actual-workerd coverage checks
that it is true across a timed-out RPC with held storage and false for the next
completed caller; a completed native cancellation alone remains recoverable.
Native artifacts and acceptance thresholds are unchanged by this diagnostic fix.
The corrected 50,000-document run also completes all nine stages successfully.

The subsequent three `b300673c7438` populations fail qualification. Several tail
connections close near the same wall-clock instant, leaving diagnostic gaps;
their collector did not record a close code, so the cause is unresolved. There
are zero observed resource or unsettled-I/O signals before those gaps, which
does not prove absence of failures afterward. Trial 3 also exceeds the allowed
number of slow requests before the hour finishes. Trial 2's control exhausts
the existing retry budget on compaction with `WorkLeaseLost` and one timeout.
Those are real control failures; no retry budget or control binary is changed.

The pre-interruption reports are preserved with exact bytes and SHA-256 in
`archive-stream-gaps-<execution>/`. The old load driver continued recording
cancelled calls after interruption; those later calls did not add cloud traffic
and must not be interpreted as data-plane failures. The corrected driver stops
arrivals on abort. All 300 reserved recovery epochs remain unused.

The next candidate removes confirmed foreground cache-fill waits using bounded,
request-owned background lifetimes. The common driver makes report persistence
independent of arrivals and records diagnostic disconnect causes. These changes
pass 245 Node regressions; live latency and longer diagnostic capture still need
qualification. The approved control remains `c44896ac8c`, including its original
native modules. The gate remains unpassed.
