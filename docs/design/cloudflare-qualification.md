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
