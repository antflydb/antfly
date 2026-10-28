# Cloudflare native protocol diagnostic — October 27, 2026

The native publication and overlapping read/write protocols work on the live
Cloudflare deployment. The first full-corpus diagnostic improves latency
substantially, but does not pass the qualification gate. Read p95 remains above
one second, complete invocation accounting is missing, and the short diagnostic
is not the required paired hour-long workload or cold population.

The measured candidate is `538ca6d7876749e23c422c0ee23fb7e8ba3eccca`, full WASM
SHA-256 `79c051479981467823ff1eb8afb6f8d72de66e89cb0e207a4bb808c28dab6960`.
The control remains the approved `c44896ac8c13898c549b7896ab633b1b8b25c04c`, with
its original native module. Both sides retain 30,000 ms CPU limits, existing
capacities, publication slice limits and gate thresholds. Diagnostic driver
changes have separate source snapshots; they do not change the measured runtime.

## Large-record publication and recovery

`qualification/native-protocol-live.mjs` uses a fresh root namespace, separately
from tenants a–d. It submits a 108,932-byte document with 12,000 distinct terms
and a sparse projection. It discards the first successful publication response,
discovers the durable job, replaces the physical actor after internal progress,
and finishes the same job. Every unfinished slice preserves the old public WAL
cutoff. Exact body, last term, sparse result, mutation receipt replay, durable
completion, and completion stability after a later publication all pass.

The discarded response models acknowledgement loss; it is not an injected
network failure. Actor replacement is not complete platform isolate eviction.
The completed run contains ten successful checks, 109 publication requests,
and no observed resource failures. Publication-call p50/p95/max are
6,088/7,259/11,897 ms. The entire experiment takes 703.4 seconds, including the
checks and subsequent small-record reads. Resumption is correct, but publication
of one large record taking nearly twelve minutes is not an acceptable throughput
result. This does not establish that the control fails this case in Cloudflare.

A subsequent small-record lookup takes 1,471.6 ms with a new physical actor;
16 reused-actor reads have p50/p95 162.5/443.3 ms. These are a small root-only
sample, not the gate's cold or full-corpus population. The root namespace is
retired and its pending queue messages purged before the paired corpus run;
all four measured tenant identities remain unchanged.

Report, relative to `examples/cloudflare-serverless/dist/qualification/`:
`gate-mvrmmhzie628/antfly-1278-g-mvrmmhzie628-1-c/native-protocol-19f42160-c56e-4b79-95ea-3b612180c928.json`.
The preserved driver snapshot predates additional version-header and terminal
diagnostic assertions in the reusable driver; it must not be rewritten.

## Full-corpus paired diagnostic

Both sides publish 512 roughly 1 KiB documents per tenant across four tenants.
The fixed offered workload runs for ten minutes, with 20 thirty-second rounds,
240 lookup samples and 80 mutation samples per side. Latencies include scheduled
arrival delay and admission retries. They are end-to-end client measurements.

| Operation | Control p50 / p95 | Candidate p50 / p95 |
| --- | ---: | ---: |
| Lookup | 3,343 / 5,227 ms | 680 / 1,138 ms |
| Mutation | 5,256 / 7,035 ms | 1,387 / 1,904 ms |

Both sides finish publication and retain 512 documents per tenant. Sampled
lookup contents and counts pass; this diagnostic does not verify every exact
body or every historical receipt. Neither side reports application or resource
errors. All ten candidate queue samples report zero backlog and zero dead
letters. These observations do not prove that no transient backlog existed.

The initial diagnostic's manually selected ten-minute setup deadline interrupts
the still-progressing control before workload starts. That failed report remains
preserved. The successful resumed setup replays the same operation IDs and body
generator against the same corpus, with the existing thirty-minute setup
allowance. `coverage-seed-resume.mjs` rejects measured, changed, fenced,
resource-failed or incompatible evidence. This is not a fresh independent trial.

Reports: `gate-mvrmmhzie628/coverage-probe-efa06961-2a2b-443a-9194-450403b767f7/report.json`
(initial setup cutoff) and
`gate-mvrmmhzie628/coverage-probe-642bfee6-6579-4c4a-b0d3-676705b3ee7e/report.json`
(resumed full-corpus diagnostic).

The live streams remain connected without resource signals during this run.
For one interior whole minute, all eight persisted role windows reconcile on
two reads after settlement, and acknowledged HTTP attempt/Ray identities match.
Independent role totals still fail: Worker analytics omit the DO RPC population,
and mixed builder totals do not equal Worker-only request counts. Quiet adaptive
results do not establish an unsampled zero. These reports deliberately exit
unsuccessfully for incomplete accounting; no collector change provides a gate
acceptance path.

In the earlier root-test minute, separate DO analytics report 87 writer and 10
builder requests, matching the respective stored RPC counts. Confidence metadata
also reports sample sizes 87 and 10. This is useful diagnostic corroboration,
not a proven complete contract for quiet roles, all invocation types, or the
entire execution. See [Cloudflare DO analytics](https://developers.cloudflare.com/durable-objects/observability/metrics-and-analytics/)
and [confidence metadata](https://developers.cloudflare.com/analytics/graphql-api/features/confidence-intervals/).

Provider event timestamps in these captures occur near response completion,
rather than client request start. Reconciliation can explicitly select
`response_at`; the old `started_at` default and old failed reports remain intact.
This aligns window membership without dropping identities, widening windows,
or forgiving missing events. Clock boundary ambiguity and unacknowledged
transports still cannot establish coverage.

## Decisions from these measurements

Keep the native protocols and their correctness guards. Do not spend on the
hour-long gate yet: the candidate read p95 misses its target and complete
accounting is unresolved. Preserve the reserved cold populations while warm
prerequisites fail. Root-only replacement checks do not substitute for them.

The next publication optimization should collapse repeated lease/checkpoint
metadata round trips into a native prepared slice with one guarded final commit.
Bind source cursor, semantic publication dependencies, current owner, upload
scope and progress in that commit. Persist the checkpoint before acknowledging,
retain old public visibility until final publication, and discard a prepared
slice after takeover or changed dependencies. Use measured time/I/O budgets to
choose a bounded work quantum rather than assuming a fixed 128-unit slice is
optimal. Keep existing CPU, memory, key-size and publication limits; extending
request timeouts would hide the work amplification.

For reads, separate fresh authority admission from immutable facts/body fetches
in the trace. In the candidate app's replica-zero diagnostic observations,
native authority-admission p50/p95 is 246/524 ms; kernel-queue p95 is 11 ms.
These spans overlap other work and cannot be summed as independent latency.
They cover setup and workload, not solely the 240 lookup samples. Compare a
coordinator-authority profile before changing the default. An immutable hint may
accelerate reads only after the fresh native HEAD/floor/pin and WAL overlay prove
it valid; mutable authority must not become an unchecked edge cache.

Finish accounting by reconciling disjoint Worker and DO event populations with
validated namespace/version/time scopes and sampling evidence, including
Queue/cron/alarm events and quiet roles. A durable exporter can preserve delivered
events but cannot manufacture events never delivered. After a controlled
coverage canary and repeatable latency improvement succeed, refresh native-bound
scale/ANN evidence and run fresh paired hour-long and cold trials unchanged.

## Coordinator-authority follow-up

A separate profile disables `LOCAL_METADATA_READS` on all candidate roles while
retaining the same sealed JavaScript/WASM, direct point reads, native protocols,
resource limits, bucket and tenant corpus. The existing corrected control is
unchanged. The profile has its own manifests and pre-cleanup snapshots; it is
not a fresh independent trial. After redeployment, a three-minute paired workload
runs six offered rounds, with 72 reads and 24 writes per side.

| Operation | Control p50 / p95 | Candidate p50 / p95 |
| --- | ---: | ---: |
| Lookup | 2,711 / 4,707 ms | 511 / 1,127 ms |
| Mutation | 4,408 / 6,598 ms | 1,239 / 1,451 ms |

The median improves, but read p95 remains over one second and differs by only
11 ms from the earlier local-authority profile. Different duration, order,
warming and publication history prevent attributing that difference to the flag.
Do not change the default on this evidence. Both sides converge, counts and
sampled bodies pass, no application/resource errors are observed, and live tails
stay connected. All eight persisted role windows are stable on two reads, but
accounting remains incomplete. This follow-up also lacks one acknowledged HTTP
invocation in the audited app minute; two adjacent settled minutes do not contain
its identity either. Its cause is unresolved. Preserve this failure rather than
assuming a timestamp boundary explains it or widening acceptance tolerances.

Report:
`gate-mvrmmhzie628rpc/coverage-probe-c8ef3efb-65b4-466d-b3c7-a832942b004f/report.json`;
adjacent-window evidence: `gate-mvrmmhzie628rpc/boundary-probe.json`.

Periodic candidate isolate memory samples collected during the session report
writer/builder p99 values of 44,298,696/42,928,120 bytes. They are hosting-isolate
percentiles, not per-object memory or absolute peaks; the API supplies no reader
sample in that capture. R2 usage is adaptive and is not a finalized bill. The
raw numeric capture retains these limitations in `analytics-report.json`.

The final local Node suite passes all 298 tests. An earlier parallel rerun
exposed a 25 ms wall-clock race in the existing ambiguous-commit test; the failed
log is preserved. Its clock now advances only after the held commit PUT starts,
so scheduler contention cannot bypass the intended indeterminate-write state.
Runtime code, native modules and deployed resource limits are unchanged.

## Cleanup and evidence preservation

Both disposable stacks are deleted after measurement: eight Workers, six DO
namespaces, four queues, two buckets and their synthetic data. A fresh account
inventory confirms zero remaining resources with these test identities and no
other `antfly-1278-` Workers, queues or buckets in that inventory. Reports remain
local. Remaining local disposable API token copies and this session's copies of
the observability token are removed; existing account authentication is unchanged.
The old profile manifests are marked retired/deleted through the final cleanup
manifest, so they cannot accidentally redeploy the already-deleted aliases.

All 613 artifacts across the five earlier evidence manifests remain hash-valid.
Pre-cleanup manifests/configs, sealed modules/bundles, source snapshots, failed
and successful diagnostic reports, sanitized logs and before/after inventory
reports are retained separately from credentials. The new session manifest is
`gate-mvrmmhzie628/evidence.json`; it records diagnostic evidence and cannot
satisfy the qualification evaluator. No hour-long or reserved cold gate run was
attempted after the warm prerequisite failed.
