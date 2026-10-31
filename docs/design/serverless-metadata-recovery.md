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

The frozen candidate has now run on the original paired hosted offered workload;
see the hosted results below and [the deployment record](cloudflare-twofold-trial.md).
Complete independent whole-window diagnostics and passing rehearsal latency are
still required before three hour-long trials and 100 physical cold samples.
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


## Hosted follow-up

The frozen candidate completed the original 180-second offered workload:
read p95 1,922 ms versus control 9,766 ms; write p95 1,507 ms versus 11,723 ms.
Both sides verified 2,052 bodies and eight receipt replays, with no final client
errors. Six candidate logical reads retried (78 attempts for 72 samples).
Write passes the unchanged 3,000 ms target; read fails the 1,000 ms target.
This does not establish another twofold gain over the previous candidate.

Selected hosted traces have zero metadata/point queue wait and substantial
native I/O: one expired/rebased observation spends 1,444 ms across 18 GETs and
11 waves; another read spends 832 ms in admission and 773 ms in lookup. Thus
reducing checkpoint/recovery dependencies and point-read I/O takes priority over
assuming resumable observation alone will halve hosted latency. Selected cold
short tails still prefetch eight slots. The existing `read_prefix` hint selects
an inline row and does not cause a metadata tree scan.

The independent collector exhausted its original aggregate deadline after six
first-pass source catalogs. Late preservation of two missing sources does not
supply a complete second observation or repair the original failure. Whole-window
resource coverage, historical `SQLITE_NOMEM` and the hosted gate remain open.
[The hosted record](cloudflare-twofold-trial.md) retains the report hash,
placement measurements, deployed artifact closure and cleanup status.

## Transport-sized recovery and advisory physical locations

The next implementation is shared native storage work, rather than an R2-only
index format. Native clients advertise a preferred speculative read window
(default eight; the WASM transport uses its four-call window). Cold and expired
short tails stop paying for a second speculative transport wave; warm tails
through seven commits retain their original serial behavior.

Checkpoint repair now has a separate `checkpoint_hint` action. It reuses a still
valid native accepted-state certificate, freshly reads the checkpoint floor and
ownership seal together, validates monotonicity and typed conditional publication,
and rechecks the original certificate after publication. The strict checkpoint
operation used by garbage collection is unchanged. Native capability advertisement
keeps old modules on the original path. Four alternating paired samples with
100 ms injected binding delay measure 505–510 ms control versus 303–307 ms
candidate, reducing four GETs plus one PUT to three GETs plus one PUT.

An opt-in `METADATA_COMMIT_HINTS` path shares a physical journal location after a
successful commit: epoch, next slot and revision only. It carries no accepted
root, timestamp or authority. A cold or sufficiently lagged native observer
reads the current checkpoint floor and the actual hinted frame together. Only
an authenticated frame newer than the fresh floor can accelerate discovery;
stale, missing, malformed or retired hints fall back to the floor. A fresh end
probe and original pre-I/O certificate anchor remain mandatory. Ownership seals,
collector grace, typed revisions and cumulative budgets remain authoritative.
Warm short tails and old hints retain the ordinary path.

Private writer acknowledgements carry hints only when enabled. The app strips
the private field before returning JSON and offers a cloned, namespace-scoped
cursor to its local reader. At most one cursor, charged at up to 4 KiB against
the existing row-cache allowance, survives requests. No I/O, caller capability,
clock or proof is cached with it. Eviction removes it; a restarted reader remains
correct without it. The actual-workerd test forbids writer SQL/KV, verifies private
field removal and native acceptance, then recreates all isolates and verifies a
correct cold read with no retained cursor.

The final 60-case native matrix verifies all 240 alternating observations. At
32 entries of lag / 100 ms storage delay, cold/expired recovery reduces 37 GETs
to four and 1,225–1,327 ms to about 310 ms. An advanced checkpoint is unchanged
at about 308 ms. These are component dependency-depth
measurements, not an app or hosted SLO. S3/GCS clients can use the same native
proof protocol with serial transport fallback; no new live-provider performance
result is claimed.

Point lookup sessions now defer opening the immutable base facts root until an
unresolved document actually needs it. Fresh WAL updates, tombstones and resumed
point plans avoid the obsolete base root; base-backed misses still validate it.
Source format, identity, domain, visibility and pin checks remain. Alternating
100 ms binding-delay tests save one GET for fresh updates/deletes (about
1,118 → 1,014–1,025 ms); stable and absent documents retain their required reads.
This does not by itself halve end-to-end read latency.

Native implementation is `66263e40d9`; final full/compact/observe module hashes are
`dc33e4a0ad350e2ea48baca675d2e4a08065f5f4cccc9dd2d32dfd8f970029e2`,
`a599e6a5fc8665bc091fe4dad85c1ff25941aea73325379a9a6b027d79445f74`,
`4f74ff72fa67755a02dcee5e50470ac5242536993fb7b958191eb05d7b05458a`.
Frozen final copies are in `dist/qualification/next-serving-final/modules` under
the Cloudflare example. The shared object-store suite passes 192 tests with two
live-provider cases skipped; 38 focused native checks pass. Full application
comparisons remain separate from these component results.


### Full app comparison of this increment

Twelve actual-workerd cases compare the frozen previous candidate `e8591354d3`
with JavaScript `318428f4b5` and native `66263e40d9`, in both orders with unchanged
capacities. Four tenants × 64 or 512 documents, eight concurrent rounds/case,
1,152 fenced measured reads and 384 writes all pass exact-body/count/visibility
checks. This is a closed-loop local dependency experiment, not the original
hosted offered workload or cold-isolate gate.

| Workload / p95 | Previous candidate | Current | Current + advisory hints |
| --- | ---: | ---: | ---: |
| 64 documents, 50 ms binding delay, read | 466–476 ms | 457–475 ms | 490–585 ms |
| 64 documents, 50 ms binding delay, write | 223–647 ms | 238–309 ms | 261–947 ms |
| 512 documents, 100 ms binding delay, read | 939–1,000 ms | 992–1,655 ms | 902–934 ms |
| 512 documents, 100 ms binding delay, write | 705–779 ms | 720–980 ms | 699–754 ms |

These results do **not** establish another twofold application improvement.
Advisory hints remain opt-in. The first-order large current case regresses;
its slow reads include 940–969 ms point-kernel queue waits followed by 0–1 ms
cache-hit native execution. Other requests spend 825 ms in native I/O. This is
different from the selected hosted zero-queue traces. Split pre-I/O observation
continuations and reusable immutable point preparation remain possible follow-ups
where those request-specific phases justify them, not measured improvements here.

Two cases start the measured counter interval with one existing writer I/O
operation; all final role counters are zero and recorded resource-code arrays
are empty. Counters are diagnostic snapshots, not complete live resource evidence.
The later maintenance/mail RPC ownership completion is integration-tested
separately and is not attributed to these frozen app timings. Reports, bundles,
module copies and the 60-case metadata matrix are preserved under the example's
`dist/qualification/next-*`; `dist/next-final-app-results.json` summarizes the app
cases. Historical failed/prototype reports remain separate.


### Final evidence closure

Local follow-up source `67dc0b1006` is sealed at
`examples/cloudflare-serverless/dist/qualification/next-local-sealed-1793408313866/evidence.json`:
**2,196 verified artifacts / 1,486,863,728 bytes**, including 1,344 verified
window-file references. Manifest SHA-256:
`75b46e673be588d4ee5cb347b13a346b87d98360f29d25407b0907a0f4670bd5`.
It retains full final/measured source archives, final modules, the native test
binary, all reported app/component cases, historical failed replay, synthetic
passing replay, failed prototypes and the initial sealing abort. Early prototype
module paths were overwritten before binary copies were frozen; their intermediate
binary closures remain unavailable and are not final qualification evidence.

The corrected 517-artifact hosted closure and prior 568/1,069-artifact closures
all reverify with zero missing files or hash changes. The invalid original
516-artifact hosted seal and its summary discrepancy remain preserved and
explicitly superseded in [the hosted record](cloudflare-twofold-trial.md).
The older 163-artifact availability gap remains historical. Credentials,
including local fixture token files, are excluded. No hosted gate pass or
resolution of the historical live memory error is claimed.


### Warm observation continuations and completed header reuse

The next implementation adds two independent, disabled-by-default experiments.
`NATIVE_METADATA_CONTINUATIONS=true` detaches the next journal-slot GET from a
warm observation's WASM slot. Native `observe_step` validates the source before
issuing a request-owned plan and authenticates its response on resumption. The
purpose-separated MAC binds namespace, scope, accepted state/root, caller owner,
follow-owner policy, original source and observation anchors, and step count.
This plan cannot serve as a write proof. A missing slot certifies only the
original pre-I/O observation anchor; resumption and replay cannot renew it, and
the source proof must remain valid throughout.

Eight positive frames hand off to native batched recovery within the existing
64-step total allowance. Partial recovery retains the old source deadline.
Cold/expired sources, sufficiently advanced hints, older modules and callers
without an owned background lifetime retain native recovery. Capability
`observation_plan_supported` enables negotiation without a persisted-format
migration. The shared object-store protocol can support other providers; these
measurements establish no S3/GCS latency improvement.

Detached reads use the existing four permits and proof-prefetch byte limit.
Same-namespace shared observation admission is counted and fair to queued
exclusive requests. Exact keys, bounded UTF-8 headers/bodies and typed absence
are checked; resource failures propagate. Logical timeout retains both permit
and caller-owned lifetime through physical settlement, with paired lifecycle
metrics. No request-owned plan, authority, lease or proof is cached.

`READ_VIEW_CACHE_SEEDING=true` offers completed immutable header bytes to the
existing scoped data cache while inside the original read-view lease, condition,
range and size checks. It accepts only full immutable GETs, copies byte buffers
and plain JSON metadata, and uses unchanged entry/byte ceilings. It never moves
cache access outside that guard. Legacy cache adapters without `admit` fall back
normally. Offers and new admissions are separately counted.

Local closed-loop comparisons use four tenants, eight rounds, 64 documents with
50 ms injected binding delay or 512 documents with 100 ms delay. Each table row
covers forward and reverse order, with unchanged capacities and frozen sources,
modules and bundles. Stage A has 12 correct cases (1,152 reads, 384 writes):

| Read p95 | Frozen prior candidate | New module, feature off | Continuations on |
| --- | ---: | ---: | ---: |
| 64 documents | 454–456 ms | 453–454 ms | 412–413 ms |
| 512 documents | 958–964 ms | 867–955 ms | 786–874 ms |

Small-workload read improvement is repeatable at about 9%; the larger on/off
comparison is mixed (955→786 ms in one order, 867→874 ms in reverse).
Writes range from approximately 202–218 ms small and 661–676 ms large. This is not
another twofold application improvement. Request phase timings overlap and
must not be added; retained traces distinguish admission, kernel queue, native
I/O and detached observation. Observed point workspace remains about 80–83 KiB
and observed linear memory at most 7.3 MiB; snapshots do not prove worst-case
allocator bounds. Final role I/O counters are zero; initial background I/O is
reported separately in the original cases.

Stage B compares the same native modules and JS bundles with continuations on,
then toggles only header retention: eight correct cases, 768 reads / 256 writes.

| Read p95 | Continuations only | With header retention |
| --- | ---: | ---: |
| 64 documents, forward | 423 ms | 415 ms |
| 64 documents, reverse | 426 ms | 417 ms |
| 512 documents, forward | 910 ms | 883 ms |
| 512 documents, reverse | 878 ms | 912 ms |

Only eight new headers are admitted per small case and four per large case;
repeat offers are not new admissions. The larger result reverses with order.
Both switches remain opt-in; these results do not justify a new hosted profile.
A separate fake-bucket diagnostic finds generated facts roots of 832–3,270 bytes;
it does not support attributing the limited admissions to a 4 KiB header cap.
Qualification provenance now seals both switches and `METADATA_COMMIT_HINTS`;
changing any declared switch invalidates the sealed profile.

Final native source is `5ae493b815`; frozen module SHA-256 values are:

- Full: `f2e533a2f080eae63b61ec4cfc3a8509883239328404ef3e49a4ef6427adc12f`
- Compact: `811fbf6c5a3081fca2259ee4aad4e448b73fa1cf398077ea7964ee439c8027ec`
- Observe: `6562d4dbbd85621ed1a4152e90cbd7dd4f344bd0afa9819939f892ce312b1d84`

All 198 native object-store tests pass (two live-provider skips), including 43
focused checks. Allocation-failure, malformed/tampered plan, clock, owner,
source expiry, missing/positive slot, concurrent append, handoff and total-budget
checks cover the protocol. The first local app attempt failed an outdated
instrumentation assertion recognizing only `observe_read`; that report remains
preserved separately. Final comparisons recognize fused `observe_step` and
independently require detached GET samples. No original failed run is promoted.

Final compatibility validation also passes all **525 Node tests and 25 actual
workerd tests**: detached observation lifetime, RPC ownership, publication/header
reuse/legacy adapter/restart, five observer tests, sixteen native R2 tests and
mail auth/isolation/search/attachments/restart. Both preparation steps pass.
The native R2 run includes its full collector grace period. These checks apply
to the final compatibility code; the frozen performance runs retain their own
recorded earlier source revisions.


### Continuation evidence closure

Final compatibility source `476723404c` and all measured source revisions are
archived at
`examples/cloudflare-serverless/dist/qualification/continuation-sealed-1793414930483/evidence.json`:
**293 verified artifacts / 1,181,327,714 bytes**, SHA-256
`d6c89efe7f180b704ee1bd3b9a50fa11c048b2d930fe1c9f35c8538fdbc37290`.
The closure includes all twenty correct app cases, failed attempts, frozen
modules/bundles, final native test binary, test logs, diagnostic scripts and
four source archives. Its summary output is excluded before sealing.

The prior 2,196-artifact local, corrected 517-artifact hosted and preceding
568/1,069-artifact closures independently reverify with zero missing or changed
files. Their 1,344 / 224 / 0 / 27 recorded window references also match.
The invalid original 516-artifact seal and older availability gaps remain
historical; no report has been repaired by changing its captured bytes.
Credentials are excluded. Guarded `make gc` was attempted after native artifacts
were copied; it skipped cleanup because a native build was active or process
inspection unavailable. Its guard was not bypassed. No new cloud resources,
hosted gate pass or resolution of the live memory error is claimed.


The next implementation and qualification protocol are described in
[Bounded serving dependencies](serverless-serving-dependencies.md).
