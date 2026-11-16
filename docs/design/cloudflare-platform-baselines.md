# Serverless performance findings and next measurements

The native serving design is in [architecture](cloudflare-serverless.md), commit
semantics in [durability](antfly-object-storage-backend.md), and acceptance/operator
procedures in [qualification](cloudflare-independent-http.md). Measurements below
are diagnostic evidence with stated scope. They do not establish a full gate pass,
universal speedup or inherent platform latency floor.

## Measured improvements and misses

The approved-control paired Cloudflare HTTP experiment used exact control
`c44896ac8c` and both execution orders. It measured 944 timed reads and 128 writes,
plus verification/publication calls. Exact bodies, counts, fences and role reconstruction
checks passed. The full original gate did not run and independent trace coverage was incomplete.

| p95 milliseconds, both orders | Approved control | Candidate |
| --- | ---: | ---: |
| Cold read | 8,231–9,381 | 1,424–1,721 |
| Warm read | 483–684 | 456–510 |
| Burst read | 488–563 | 173–394 |
| Mixed read | 1,248–4,840 | 539–1,594 |
| Mixed write | 3,918–6,351 | 1,703–2,326 |

Cold improved 4.8–6.6× and mixed reads/writes approximately 2–3× within those
orders. Reverse mixed reads missed the 1,000 ms limit; reverse warm reads regressed
510 versus 483 ms. Warm improvement is inconsistent. The driver could queue waiting
for headers, and missing service traces prevent an exhaustive error claim. These
pre-main results do not qualify the current rebuilt modules.

## Platform envelopes

Three separate Cloudflare fixtures measured gateway/service/owner RPC, local storage
and conditional R2 operations with 1 KiB, 256 KiB and synthetic-pressure profiles.
Pressure retained 32 MiB WASM plus 8 MiB bytes; no Antfly engine was loaded. Each
unloaded cell has 24 successful samples. Client measurements came from the local
Mac; app colos were SEA/SJC, while actual owner/R2 placement was unverified.

| Client p95, ms | 1 KiB | 256 KiB | Pressure |
| --- | ---: | ---: | ---: |
| Gateway echo | 44.7 | — | — |
| Service echo | 68.8 | — | — |
| Owner echo | 77.4 | 98.9 | 250.3 |
| Owner GET | 73.8 | 105.6 | 100.1 |
| Owner PUT + sync | 116.7 | 145.2 | 291.8 |
| Owner R2 GET | 265.0 | 256.7 | 203.0 |
| Owner conditional R2 PUT | 1,010.8 | 901.0 | 539.7 |
| Gateway R2 GET | 215.5 | 390.9 | 103.0 |
| Gateway conditional R2 PUT | 1,202.5 | 866.5 | 465.1 |

All three overall reports failed offered-load/client checks: 43 explicit unsent
overloads, dispatch-bound failures and one pre-header transport failure remain.
All 1,707 known attempts have intent/terminal pairs; all 27 write-verification
batches pass. The 16-in-flight client cap is not a global R2 QPS limit. Partial
Tail recorded 1,003 events and no observed resource failure; it is not exhaustive.
Run order, different buckets/placement and host load prevent causal pressure claims.

New-object creation had 24 samples, p95 468.6 ms. Existing-owner reconstruction
first reads were 118.0/87.0/196.3 ms, **one sample per profile**, followed by seven
warm reads each. Changed instance/version and persisted seed verified reconstruction;
physical cold isolates were not proven. These are not cold p95 populations.

All six fixture Workers, three namespaces and three buckets were removed, with
12 exact scoped API 404 checks. New disposable tokens were removed. Reports,
failed samples and receipts remain local. No fixture queues/schedules/exports remain.

## Attribution and experimental policy

`QUALIFICATION_PHASE_PROFILE=true` carries bounded same-request HTTP/owner profiles
through existing RPC and emits an authenticated `X-Antfly-Profile` response header.
Profiles exclude keys, bodies, arbitrary error strings and credentials. Span labels,
header size and dropped/error-bearing profiles are checked. The default is off.
The HTTP driver and local runner reject incomplete required profiles.
`analyze-request-phases.mjs` aggregates them offline.

Calculate residuals within individual requests. Do not subtract unrelated percentiles
or timestamps from different isolates; nested spans and shared groups are not additive.
Worker clocks advance around I/O, so a zero span is not zero CPU. Inspect the slow
requests' admission, owner RPC, native execution, bootstrap, cache activation and
physical dependency waves before changing architecture.

The elapsed batching-window experiment completed both orders with 384 reads,
128 writes and 48 reconstruction reads. All 512 mixed-request profiles validate.
Read p95 forward regressed 155.0→176.7 ms; reverse was 128.3→128.2 ms. Write p95
changed 275.7→271.2 and 281.5→255.6 ms. There is no twofold gain or paired read
non-regression. `MUTATION_ELAPSED_BATCH_WINDOW` stays off by default.

## Current hosted rehearsal blocker

The manual Mac-client rehearsal did not reach the offered workload. The exact
approved control seeded and verified all four 512-document tenants. The candidate
accepted 512 documents in tenant `a`, published the first 64-record prefix, then
its second publication returned HTTP 503 `PublicationExecutionBudgetExceeded`
after 34,134 ms. Remaining candidate tenants were not seeded. No paired latency
or full-gate verdict can be inferred from this incomplete setup.

The captured failed operation reports 79 native operations, 1,032,206 transferred
bytes and 27,387 ms native elapsed/I/O wait. Physical data I/O has 20 GETs, 22 PUTs
and 40 waves; three coordinator RPCs account for another 6,681 ms, including
4,068 ms abort/lease cleanup. Kernel queue wait is zero in this trace. The
162,881-byte native working-set peak is not whole-isolate memory evidence.
This failure identifies a serial storage/deadline problem, not a reproduced
`SQLITE_NOMEM` root cause.

Separate postmortem reads verify one published and one accepted unpublished body.
A new count check verifies all 512 accepted records after correcting a client-only
validation error: counts require their HTTP phase profile, while resident lookups
and mutations also require an owner profile. Wrong counts, missing HTTP profiles
and missing owner profiles on instrumented operations still fail. HTTP resource
errors retain their original cause when diagnostic headers are missing. The
original rejected count check and publication failure remain preserved; none is
retroactively qualified.

Publication is the immediate engine priority before another mixed-read trial.
Bound initial and steady-state publication by serial remote dependency waves and
the full probe/admission/build/checkpoint path, including ordinary small-record
fast batches. Yield durable guarded progress before the existing deadline and
retain takeover/receipt/collection safety. Evaluate slow and variable GET/PUT
schedules locally before a fresh hosted attempt. Raising limits or repeatedly
retrying a failing prefix is not evidence that this path is qualified.

The runtime now feeds admitted publication wall time (including the probe and
lease RPCs) into capacity feedback. Finalization adds its preparation and
validation time before computing the next budget; the remaining durable commit
uses the existing headroom reserve. Growth is capped by projected measured
resource/wall capacity rather than blindly doubling a slow successful prefix.
Near-guard successful prefixes shrink before hitting the hard limit. Resource
failure feedback runs in the shared publication runtime after lease cleanup, so
HTTP and Queue callers both reduce capacity once per failed execution. Queue
reindex retains its existing separate feedback path.

New object-backed publishers admit one record, then grow from measured feedback
under the configured ceiling. A versioned admission marker distinguishes current
measurements from an older persisted configured maximum. Both the measured
budget and resource-failure feedback survive eviction. Admission fields use one
batched metadata read, avoiding new serial policy reads. Existing unversioned
feedback is recalibrated; unknown future policy formats fail closed.

When an eligible paged publisher exhausts its budget at one record, it durably
requests the existing guarded native continuation protocol for that prefix,
regardless of payload size. A successful one-record fast prefix near the wall
guard also requests this mode for the next prefix. Intermediate checkpoints do
not advance public HEAD or the published WAL cutoff. The mode survives eviction,
and a committed publication clears it atomically so cheap subsequent prefixes
can return to fast batching. Nonpaged layouts retain bounded-prefix retries and
require compatible paged projections before this continuation mode can apply.
Fast batches still commit atomically; there is no new multirecord checkpoint
format.

Actual-workerd tests use steady 10 ms and variable 2–40 ms GET/PUT delays plus an
explicitly injected publication-budget failure. They verify small-record
continuations, eviction, unchanged cutoff before completion, exact body/count,
receipt replay, indexed search and return to fast batching. The injected failure
checks control flow; these local delays do not recreate the hosted 30-second
exhaustion. No hosted latency improvement or full-gate pass is claimed. Measure
the larger corpus and publication throughput before a fresh hosted rehearsal. The
current Mac reached a one-minute load average around 110 during validation, so
the 512-document timing comparison is deferred until a quieter run; correctness
test durations are not latency benchmark populations.

The consolidated tooling passes 615 Node tests and the full 63-case workerd
suite, including the 20 R2 and 18 qualification cases. Retired fixture tests cover
only the removed diagnostic programs. Epoch setup drains `has_more` before requiring
published indexes; existing query/fencing assertions remain intact. These checks
include simulated slow-storage capacity recovery,
publication failure/retry through HTTP and Queue, exact count/body verification,
and repeated updates in both affected backends. The two earlier failures were
native `unreachable` traps on a second
mutation after publication in conditional-root and bounded-journal tests, also
reproduced with previous committed JavaScript and unchanged WASM. A synchronous
native reproduction isolates the cause: read-ahead succeeds, but its allocated
keys and result array survive serialization. Request teardown then fails the
allocator's zero-live-allocation assertion. The fix releases those descriptors
on both success and serialization failure. Repeated-update coverage exercises
both metadata backends without changing memory limits or assertion behavior.
This is a distinct native allocation-ownership defect; it does not establish
the cause of the historical hosted `SQLITE_NOMEM`.

All 11 Workers, six captured namespaces and four R2 buckets have explicit scoped
404 absence checks; three disposable app-token files are removed. After the
operator changed approval settings, the existing account credential was used only
in memory to delete the two remaining export configurations and verify their
absence. This deployment cleanup is complete. The original sealed manifests keep
their historical partial-cleanup state; a separate final destination receipt
records completion without rewriting failed-run evidence. Local setup reports,
postmortem windows, hashes and cleanup receipts are retained. Diagnostic coverage
is partial.

## Current modules and next sequence

The latest main integration includes lazy navigation and reclaimer-aware quotas.
The full/compact/observe module SHA-256 values used for the retained local
latency comparison are:

- `d7f626e64130eb9253840fe14ff1b7e48b7a4c9fe7118f4adf07a33c143a11ec`
- `e8022a42b86f0996acf132df7a3e19d93c0753c9999f9432fcbb253b375cc4e1`
- `34543dcf22d16918975578daec99bea8e4f2a3b82332af5aac33480d729c4fd8`

The preceding integration passed 623 JavaScript tests, 209 object-store checks
(two provider skips), 96 graph/page, 16 read-state, 14 publication/plan, 27 native
host contracts and six actual-workerd checks. That comparison's rebuilt-module
validation passed all 625 JavaScript tests and 27 native host contracts. The
actual-workerd comparison below exercises reconstruction and
concurrent reads/writes on those exact rebuilt modules; it does not rerun every
earlier native/provider suite.
The approved control remains immutable; rebuilt modules do not replace its bytes.

The retained short local comparison completed both execution orders on the
modules above (before the read-ahead ownership fix), four tenants of 512
documents, 20 ms injected storage delay and
zero-delay setup. It offered 128 reads at 16 reads/s with one write per sixteen
reads, per case, using fresh stores and required fences plus latest-read checks.

| Local p95, ms, both orders | Resident incumbent | Durable cold pipeline |
| --- | ---: | ---: |
| Read | 68.4–69.0 | 39.1–40.0 |
| Write | 372.1–378.9 | 182.6–186.0 |
| Reconstructed read | 400.7–407.7 | 180.1–180.7 |

Four cases contain 512 measured reads, 32 writes and 48 separate reconstruction
reads. All 544 mixed requests have valid HTTP/owner profiles. Exact body/fence/count,
module/bundle hashes, activation, physical settlement and error-history guards pass.
No offered failures occur; maximum dispatch lag is below 3 ms. Batching stays off.
These are small emulator populations, with only eight writes/case and twelve cold
cohort members/case, not independent physical cold boots. They compare the current
resident incumbent with the durable pipeline, **not approved control c44896ac8c**.
They do not replace hosted qualification or establish a statistical SLO. Reports
and negative earlier runs remain under local `dist/qualification/`.

Same-request profiles identify the local tail reduction: the incumbent's slowest
reads spend 121–291 ms in owner admission. The durable candidate's slowest reads
instead mostly contain one 23–27 ms immutable dependency wave. This supports the
existing owner/cold-pipeline path for the next hosted comparison; it is not proof
that the historical hosted tail has the same cause. Kernel queue p95 is zero in
these small local populations, so increasing kernel capacity is not justified by
this evidence.

Run a short 512-document comparison on the current source before longer tests.
For local diagnosis use `npm run qualify -- local ...`, separate fresh output directories,
explicit variant flags, both orders and complete phase profiles. An incumbent
resident variant is not the approved historical control. Avoid overlapping builds
or latency workloads. Preserve negative results and verify actual activation.

For hosted acceptance, use a dedicated independent client and exact approved control.
First eliminate client overload, missing observation and mixed versions. First qualify bounded publication setup; then fix
the dominant mixed/warm read wait while preserving freshness and writer progress.
Accept improvements only if both orders meet absolute limits and paired non-regression.
Expand cold recovery only after warm prerequisites pass.

Reduce required serial R2 commit waves before trying larger write concurrency.
Group commit amortizes throughput costs but cannot remove required durable latency.
An authoritative provider-local log with asynchronous R2 checkpointing changes
acknowledgment/takeover/replication semantics and needs separate protocol qualification.
Keep provider placement, whole-isolate memory and client scheduling as measured
variables; the baseline table is not an inherent lower bound or product SLO.
