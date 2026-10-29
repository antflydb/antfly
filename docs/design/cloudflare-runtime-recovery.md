# Cloudflare runtime recovery and diagnostic completeness

The short live probe recorded in [the memory audit](cloudflare-sqlite-memory.md)
failed. Its original artifacts remain immutable. This follow-up fixes concrete
application/driver defects and separates provider capabilities that still need
live verification. It does not declare the acceptance gate passed.

## Physical I/O outlives logical cancellation

The shared Asyncify host already poisons a suspended instance and retains its
pool reservation until started binding I/O settles. It now also registers that
settlement with the submitting request's background lifetime and emits a UUID
link between the initial `io_draining` metric and `antfly.io_settled`.
Both ordinary runtime calls and coordinator/read-snapshot calls use this port.
No new database operation starts after cancellation, no suspended stack is
reused, and a late write remains indeterminate until native receipt recovery.

Diagnostics retain bounded per-role lifecycle sets, tolerate duplicate tail
replicas and reordered frames, and require each start to have its matching
physical completion. Missing or malformed IDs, missing completions, orphan
completions and capacity overflow fail final coverage. The live health guard
allows a known retained lifetime to finish; final admission to cold tests and
final evaluation require no unresolved lifetimes. A late CPU/memory failure
still fails even after a matching completion and a successful HTTP retry.
Old uncorrelated `io_draining` evidence remains failed; old reports are not
reinterpreted as passes.

## Transient R2 failures and commit ambiguity

The shared R2 adapter normalizes documented binding service codes 10001, 10043,
10054 and 10058 into the existing transport/rate-limit error contract. Authority
creation/activation and metadata sweeping use the same boundary. The adapter
makes one physical attempt; it does not replay an ambiguous conditional write.
Native commit/receipt recovery determines whether a write committed. The
resource classifier follows bounded error causes, so a typed wrapper cannot
hide a CPU/memory failure or accidentally make it retryable.

The common qualification client recognizes these old-host transport envelopes
and the explicit storage-reset/network-loss envelopes for warm idempotent
requests only. Attempt and deadline budgets are unchanged; all physical
attempts remain in the invocation ledger and elapsed time. Cold calls remain
one attempt, unkeyed writes are not newly replayed, arbitrary internal errors
are not treated as transport errors, and resource signals remain counted.
This expands error classification; it is not a claim that retry policy was
unchanged or that platform internal errors have been root-caused.

Reference: [R2 error codes](https://developers.cloudflare.com/r2/api/error-codes/).

## Queue-owned scheduled database work

Previously, the native R2 Cron handler performed metadata collection and
reindex/model status work directly while the Queue consumer could execute
publication or other maintenance. Those independent entry points could overlap
on the same writer. Cron now only sends one scoped `maintenance_tick` hint per
tenant. The Queue owns the existing scheduled recovery routine, including
publication admission, metadata collection, reindex and enrichment recovery.
The existing native leases, maintenance tickets, due checks and continuation
protocols remain authoritative. Duplicate hints are safe to redeliver; they
are not assertions that work completed.

The native qualification endpoint advertises Queue-only dispatch. Cold-test
quiescence requires that capability and verified bundles, pauses delivery,
retains the existing 31-second drain guard, and rechecks clean I/O diagnostics.
It does not redeploy the builder or change code versions. Cron may continue
sending hints while delivery is paused; it performs no database work. The
legacy control keeps its reviewed implementation. Ordinary deployment cleanup
still removes triggers and uses its existing lifecycle.

This removes one concrete source of maintenance overlap; cloud latency and
backlog improvements still need measurement. No resource capacity or numeric
acceptance threshold changes.

## Measured intervals and exact queries

The short probe closes its diagnostic interval before pausing/redeploying
Workers. Teardown-induced tail disconnects are not mixed into the measured
interval. Real disconnects during capture still fail. Verification progress is
saved before final convergence, and a failed live diagnostic now makes the
probe exit unsuccessfully even if other collectors succeed.

Persisted telemetry queries can split a query-sampled window into exact,
disjoint timestamp slices. All slices share the original 64-query, 64-MiB and
120-second budgets, require exact before/after counts and preserve identities.
Ingestion sampling never takes the subdivision path: lost events cannot be
recovered by querying smaller windows. Combined rows are independently
replayed through the same resource/lifetime checks. Counter failures retain
only fixed categories (authorization, rate limit, schema, timeout, provider),
status and numeric codes; credentials and provider error text are not saved.

## External completeness prerequisite

The settled read-only recheck of the failed live minute still has sampled app
populations: control sample interval 1.025974025974026, candidate
1.4615384615384615. Private writer/reader Worker populations are empty without
explicit unsampled-zero proof; the builder Worker populations are unsampled.
Waiting longer does not turn those app populations into complete evidence.

The current credential receives HTTP 403/code 10000 listing Logpush jobs.
The attempted dataset-discovery URL returns 404, which is not an entitlement
finding. Inspecting/configuring an export needs an account credential with the
appropriate Logs permissions or details of an existing unsampled export.
[Workers Logpush](https://developers.cloudflare.com/workers/observability/logs/logpush/)
has different permissions from Workers deployment. No plan, export job, cloud
resource or external credential was changed in these read-only probes.

An export is not automatically qualification evidence. Before using it, prove
all eight script/role populations, terminal invocation identities (including
DO RPCs and quiet roles), versions, late physical resource events, no sampling,
loss/replay behavior, delivery settlement and independent reconciliation with
acknowledged attempts. Reject an export that lacks those contracts. The
[Trace Events field schema](https://developers.cloudflare.com/logs/logpush/logpush-job/datasets/account/workers_trace_events/)
must be checked against actual fetch/RPC fixtures; timestamp or request-count
estimates cannot substitute for unique invocation identities or complete
populations. Existing sampled artifacts remain failed.

After local verification, prepare fresh paired stacks, qualify the export with
a short probe, then run the unchanged three one-hour populations and 100 cold
epochs per candidate only when warm prerequisites pass. No previous retired
stack or consumed corpus is reused as fresh evidence.

## Local validation checkpoint

All 344 Node tests and 34 full actual-workerd checks pass. The final workerd
suite includes native SQL/KV guards, Queue-only Cron, physical cancellation
retention, caller-context handoff, lost maintenance acknowledgements,
publication checkpoints, both collectors, migration and eviction. A focused
settlement/context case and the corrected Cron case also pass separately.
The earlier Cron assertion expected an unnecessary publication from an empty
WAL; its failed log is retained, and the corrected assertion verifies that
Queue delivery invoked the real native metadata collector.

The actual-workerd local experiment uses four tenants of 512 documents and the
selected candidate's flags, with a uniform 20-ms delay on binding operations
including preparation. Preparation completes in 50.65 seconds. Thirty-two
successful reads have p95 174.68 ms; sixteen successful writes have p95
458.16 ms. It verifies the corpus, updated latest bodies and no-op compaction.
These sequential small populations are not fixed-arrival, cloud latency,
exact whole-corpus/receipt qualification or a causal before/after comparison.
The runner accepts an explicit output directory and recorded profile digest,
so it never needs to append evidence into an earlier sealed experiment.

The native modules are unchanged from the previously sealed 32/16/8-MiB
family. No Zig/native-provider implementation changed in this follow-up;
previous native/ANN/scale evidence is retained by artifact identity rather
than being relabeled as newly executed tests.


The bursty 20-ms-delay repeat also passes: four 512-document tenants, 32 reads
(p95 450.95 ms), 16 writes (p95 855.78 ms) and preparation in 92.73 seconds.
The fixture explicitly waits, within a 30-second terminal bound, for all
reported app/writer/builder physical I/O to reach zero before inventory and
teardown; that wait is 116 ms. The earlier bursty run ended its measurement
with one idle-checkpoint operation active and is retained separately. Its
correctness result is not a complete terminal-I/O measurement. These local
experiments remain small sequential diagnostics, not a gate pass.

## Reviewable next trials and capability probe

Three fresh local-only pairs are prepared under
`dist/qualification/gate-mvtt65830d61/`, with candidate runtime `9b81fa0021`
and unchanged approved control source `c44896ac8c`. All six sets of four
Worker bundles verify, including the original control's native module hashes.
They retain the selected profile, capacities and numeric gate thresholds.
No cloud resource was created and no token was uploaded. The generated local
API-token files remain private in the unsealed proposal directory for later
authorized deployment; they are excluded from evidence archives.

`qualification/telemetry-capabilities.mjs` is a read-only preflight for the
exact disposable scope. It checks the documented Workers Trace Events fields
and existing export jobs, retains only fixed/numeric diagnostics, and creates
a **disabled** job template with no destination credentials. It never treats
configuration availability as delivery qualification. It accepts an explicit
private token file or the existing Wrangler credential:

```sh
node qualification/telemetry-capabilities.mjs \
  dist/qualification/gate-mvtt65830d61/gate.json \
  --token-file=/private/path/to/account-logs-token \
  --out=dist/qualification/telemetry-capability-new-run
```

Both documented endpoint probes return HTTP 403/code 10000 with the current
credential. A private file path with Account Logs Read/Edit permissions, or
an existing unsampled export's details, is required to inspect/prepare that
next step. The probe never logs destination URLs, provider error text or
credentials. Missing source identities, sampled totals and unproven quiet-role
zeroes remain qualification failures; neither sibling populations nor matching
estimates substitute for provider completeness evidence.

Current evidence is under
`examples/cloudflare-serverless/dist/qualification/runtime-recovery-1793292255/`.
All earlier sealed reports remain unchanged. The hour-long and cold cloud
populations have not been run on this runtime; the PR remains draft.
