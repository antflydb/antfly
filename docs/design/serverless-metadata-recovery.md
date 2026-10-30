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
and validate its contiguous journal suffix. After two positive commit slots,
the native observer requests at most eight independent slot addresses at once.
The window is bounded by the remaining 64-step recovery budget and the current
epoch. Each response is capped at 16 KiB and validated before its state is used.
Idle observations still use one slot probe; one new commit still needs two.

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

The corrected candidate full module is
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
