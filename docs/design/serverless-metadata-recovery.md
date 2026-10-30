# Bounded metadata recovery and reusable immutable preparation

This extends the shared object-store engine used by Antfly's serverless runtime.
The motivating hosted traces spent 1,202–1,233 ms admitting reads, with nine or
ten GETs in nine or ten physical I/O waves and no kernel queue delay. Existing
fenced leases, fused admission, warm floor/slot batches and native read plans
were already enabled. The next targets are read p95 ≤754 ms and write p95
≤1,044 ms on the original hosted workload. These are targets, not qualified
results.

## Recovery algorithm

The existing immutable journal, accepted checkpoint, owner fencing and
certificate format remain authoritative. Cold observers read the checkpoint
and validate its contiguous journal suffix. Cold/expired recovery starts
prefetch after two positive commit slots; a warm session requires eight.
The native observer requests at most eight independent slot addresses at once.
The window is bounded by the remaining 64-step recovery budget and the current
epoch. Each response is capped at 16 KiB and validated before its state is used.
Idle observations still use one slot probe; short warm tails through seven
commits retain exactly one probe per accepted slot plus the fresh end probe.

Prefetch does not establish order or authority. Every consumed frame must match
the expected epoch, position, parent revision and owner. A seal resets the
window progression, and a replaced writer remains fenced. A speculative missing
slot is freshly read **after** consuming its accepted prefix: an absence read
before its parent was accepted cannot prove the end of the journal. Unused
prefetch errors propagate, and all requests settle and retain their budget
charges. A separate bounded scratch arena keeps batch bodies alive across
individual frame decoding and frees them on success, error and allocation
failure.

An expired observer already fetches the checkpoint and its cached next slot
together. When that checkpoint has advanced, use it as the accepted recovery
floor and validate its own next slot directly. Discard the old cursor's
speculative slot instead of restarting legacy checkpoint/SEALED recovery.
The certificate anchor precedes the original checkpoint/batch I/O; rebasing
must never reset its clock. A delayed-response regression checks this explicitly.
Legacy recovery remains available for the existing unsupported-proof cases.

The algorithm lives in `zig/lib/objectstore/src/immutable_log.zig`, rather than
interpreting authoritative journal frames in JavaScript. Batch catch-up uses
the existing `Client.getObjects` capability. WASM currently allows four binding
calls in parallel, so an eight-object native batch can require two physical
waves. Native S3/GCS adapters currently retain the serial fallback; this change
does not establish a throughput gain for their transports. The checkpoint
rebase and correctness rules are provider independent and preserve typed ETag
and generation semantics. No new provider-specific persisted format is added.

## Checkpoint maintenance

Recovery hints stay outside the durable acknowledgement boundary. The idle
scheduler coalesces dirty revisions and retries admission deferrals, busy
results and transient failures up to four attempts within one original
ten-second wall/monotonic budget. Each native request receives the remaining
deadline; ownership/migration/corruption errors stop retries. Failed attempts
remain observable even if a later attempt succeeds. An exhausted target stays
dirty for the next scheduling/maintenance opportunity; this is bounded repair,
not an infinite background loop or a new durable scheduling guarantee.

The native checkpoint CAS, one-second publication limit, journal recovery and
existing queued maintenance remain in force. If measurements show persistent
lag beyond these bounds, the next step is a durable replay-cost-aware serving
checkpoint or seek index, with an explicit collection fence. Do not replace
fresh validation with a cached root or relax collection grace periods.

## Preparation reuse

The existing opt-in namespace affinity now also covers metadata observation,
and participates in the app read-view configuration identity. It changes no
pool capacity, memory cap, lease lifetime or authority rule. Measure it under
namespace churn before enabling it by default.

Immutable write preparation survives an authority refresh, proof expiry or
epoch rotation. The final native commit still validates the expected WAL ETag,
publication-head body and GC-floor body against the current root and performs
the authoritative append before ACK. A semantic dependency rejection discards
the plan and rebuilds it. Other failures propagate. Preparation and finalization
attempt counts are distinct, making avoided work visible without attributing
the whole write to a single native timing. Idempotent operation receipts and
ownership checks are unchanged.

## Evidence and acceptance

`qualification/metadata-recovery-latency.mjs` compares frozen modules against
identical physical fixtures at 1/8/32 commits of lag, cold/31-second-expired/
advanced-checkpoint observations, and 0/50/100/200 ms binding delay. It alternates
order, records failed calls separately, and joins timed-out physical I/O before
checking settlement. These are metadata-only Node/WASM models, not workerd,
cloud or app SLO measurements.

`qualification/read-state-latency.mjs --twofold` compares frozen control source
and modules with current, affinity and write-read-ahead variants in actual
workerd. Each uses the same logical keys in a fresh isolated store. Reports
separate first-use and warm reads/writes, exclude preparation from measured
phase counters, retain preparation latency separately, and include native
write groups and trace phases. Its offered pattern is closed-loop concurrent
bursts; it is diagnostic, not a substitute for the original hosted gate.

Use reverse-order trials, idle gaps, mixed maintenance, cache loss and namespace
churn before promoting a configuration. Require exact bodies, receipt replay,
fencing, allocation-failure/cancellation coverage and unchanged resource limits.
Qualify the selected candidate on the original offered workload with complete
independent whole-window diagnostics and measured placement. The historical
hosted `SQLITE_NOMEM` remains unreproduced; passing SQL/KV-forbidden native tests
does not establish its allocator-level root cause or satisfy the hosted gate.

## Initial implementation measurements

The first anchor-corrected prototype full module is
`352c3e98742b5330f47f2fe8f8c47b0fde79e55c45f677609ec44a7702ba68ce`;
the frozen previous candidate control is
`9711adb54900fe7c494fb16a8fa0675b1e83ca2eba9a598bb9a6b0d79759f02c`.
The 36-case, three-sample metadata matrix retains 216 observations at
`examples/cloudflare-serverless/dist/qualification/twofold-metadata-final/report.json`.
At 32 commits of lag and 100 ms binding delay, cold recovery p95 is
3,599 → 1,368 ms and expired recovery p95 is 3,634 → 1,380 ms. Both are about
2.6× faster for this stage. Candidate reads increase from 35 to 37 physical
GETs, with peak concurrency four. An advanced floor reduces six GETs to four.
The candidate verifies all 108 observations; the control fails all six
32-entry cold/expired observations at 200 ms delay under the unchanged
five-second deadline. Failed samples are excluded from latency percentiles and
retained explicitly, including physical settlement after timeout.

The first four-variant actual-workerd experiment verifies bodies and visibility
for every variant. It overlapped the integration suite and therefore remains
exploratory: its ratios are not accepted as evidence of an end-to-end gain.
An earlier pre-anchor model and a harness attempt that failed its physical
settlement assertion also remain preserved, rather than being relabeled as
qualified results.

The first ordered comparison exposes a concrete limit of eager warm prefetch.
At 50 ms injected delay, a warm admission with two new commits fetched an
eight-slot speculative window, taking eleven physical GETs and four I/O waves.
Forward-order read p95 regressed 532 → 649 ms while write p95 improved
526 → 283 ms. Affinity and write read-ahead were also mixed. These results are
preserved as prototype evidence, not promoted to a twofold claim. The revised
policy protects short warm tails and keeps aggressive overlap on cold/expired
recovery; its 34-test native suite verifies every warm tail length through seven,
deep cold/warm catch-up, allocation failures, concurrent appends and fencing.
New WASM and end-to-end measurements are required before selecting that policy
for a hosted deployment.

## Final local qualification of the conservative warm policy

Production implementation `e8591354d3`, measured from clean source
`dbe020dc86`, produces full/compact/observation modules:

- `9b86730b4cf0941db1adf22c8876d61c90a0d03643cde2e87fe03d7f2c33d367`
- `fbeac401afd8dc59bd7eabefc5cf6368306b972bd7af277863b3bbbdb6b31e61`
- `fa7e6af392149a884443f1bf27ffad5336d40ff46a7ed9c4ad6f892ef8d35c49`

All 499 Node tests pass against these modules. The shared object-store suite
passes 188/190 tests, with two credential-dependent live-provider cases skipped;
all 34 focused journal tests pass. Three guarded actual-workerd regressions pass:
eviction/collection/reindex recovery, SQL/KV-forbidden bounded native plans, and
write-plan reuse versus semantic dependency rejection. An earlier concurrent
full workerd run had a Tail response-body failure and a legacy-test timeout;
both affected files pass in isolation with unchanged assertions/timeouts. The
frozen previous legacy implementation also passes that long-running test. The
failed concurrent run remains preserved, not relabeled as a clean full run.

The final metadata matrix verifies all 108 candidate observations across 36
cases. At 32 commits of lag and 100 ms binding delay:

| Recovery p95 | Previous candidate | Conservative warm policy |
| --- | ---: | ---: |
| Cold cursor | 3,540 ms | 1,321 ms |
| Expired proof | 3,447 ms | 1,221 ms |
| Advanced checkpoint | 511 ms | 308 ms |

Cold/expired recovery is about 2.7–2.8× faster for this component. The control
still times out on all six 32-entry cold/expired observations at 200 ms delay;
the candidate completes them under the unchanged five-second budget. The
matrix alternates order, clones identical physical fixtures and freezes its
launch source/arguments. See
`dist/qualification/twofold-metadata-short-tails/report.json` under the example.

All 18 final actual-workerd cases pass exact-body, count and visibility checks,
covering 1,728 measured reads and 576 measured writes. The following are complete
order ranges, not selected best samples:

| App workload | Profile | Read p95 | Write p95 |
| --- | --- | ---: | ---: |
| 64 documents/tenant, 50 ms binding delay | Previous candidate | 463–484 ms | 232–254 ms |
| Same, two orders | Current | 467–476 ms | 241–271 ms |
| Same, two orders | Affinity | 456–482 ms | 241–380 ms |
| Same, two orders | Affinity + write read-ahead | 471–478 ms | 237–257 ms |
| 512 documents/tenant, 100 ms binding delay | Previous candidate, two orders | 1,095–3,090 ms | 862–3,089 ms |
| Same, two orders | Current | 1,598–1,617 ms | 1,084–1,303 ms |
| Same, one case | Affinity | 1,856 ms | 883 ms |
| Same, one case | Affinity + write read-ahead | 2,111 ms | 2,001 ms |

The apparent idle app improvement does not survive reversing order. First-read
p95 is control 967 → current 536 ms in one order, but current 1,050 → control
453 ms in the other. Each case has only twelve first-read samples. Treat this
as an order effect, not a reproducible implementation gain. The large workload
also varies substantially; no overall twofold app improvement is established.
Keep affinity and additional write read-ahead disabled in the selected hosted
diagnostic profile.

The larger slow-read traces have zero warm prefetch and typically three to five
metadata GETs, so the original short-tail overfetch is fixed. They still show
substantial shared-pool waiting: one candidate read waits 1,153 ms for its point
kernel after 433 ms admission, then spends 342 ms in native lookup (340 ms I/O).
Do not add independently computed stage percentiles to explain request p95.

`dist/qualification/twofold-results.json` summarizes all final app cases. Reports
retain first/warm timings, native phases, physical counters, source/argument
hashes, bundles and module copies. Three phase resets start with one existing
writer I/O operation; counters preserve that activity rather than resetting it
to zero. Every final counter snapshot has zero active I/O and zero recorded
resource codes. These snapshots are not independent whole-window evidence and
cannot rule out later background failures or establish the hosted allocator's
behavior. The earlier false-positive resource classifier, missing control-shim
import, eager-prefetch regressions and interrupted attempts remain preserved.

## Remaining architectural boundary and hosted acceptance

The native recovery component improves; the shared read queue is still a limit
at larger concurrency. If paired hosted traces confirm it, extend the shared
engine with resumable native metadata observation: a native plan binds the
source root, scope and original clock anchor; request-owned bounded transport
performs its I/O outside the WASM execution slot; native resumption validates
the frames, fresh end probe, owner and expiry before serving. Retain typed
revisions, cumulative operation/byte budgets and settlement of unused failures.
A JavaScript prefetch followed by a newly anchored native certificate is not
correct: the certificate must bind an anchor preceding the I/O. This continuation
protocol is not implemented or claimed as part of the measured result here.

Next run the frozen candidate on the original paired hosted offered workload,
measure compute/storage placement separately, and require complete independent
whole-window diagnostics before proceeding to the three hour-long trials and
100 physical cold samples. The new disposable scope is described in
[the hosted proposal](cloudflare-twofold-trial.md); credential/cleanup approval
is pending. No new cloud resources were deployed for these local experiments.
The historical live `SQLITE_NOMEM` and the hosted gate remain unresolved.

The local work is sealed at
`examples/cloudflare-serverless/dist/qualification/twofold-serving-1793396451217/evidence.json`:
568 verified artifacts / 1,172,967,209 bytes, source `616de21b41`, manifest SHA-256
`70709e3f200a133d98180e588a4da25ad0ed28940ba4f44000548a2022a2141e`.
It includes original and final modules, frozen sources, all local reports,
unpromoted prototypes, interrupted attempts, test logs and the original
undeployed proposal without credential files. All 1,069 artifacts in the prior
hosted-investigation closure re-verify with no missing files or changed hashes.
The earlier path-base error in the audit is explicitly superseded by its
corrected audit; it did not indicate missing experimental evidence. The older
163-artifact availability gap remains historical. Guarded `make gc` was attempted
after the build/test sessions and skipped because a native build was active or
process inspection was unavailable; no guard was bypassed.
