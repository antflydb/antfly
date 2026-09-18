# Workload scheduling validation record

This record separates correctness evidence from the full
[release qualification matrix](WORKLOAD_SCHEDULING_QUALIFICATION.md). No release
row is qualified. Native experiments below use Debug builds on an unconstrained
macOS host. They cannot establish Cloud package throughput or latency limits.

## Current committed correctness checks

The clean Debug build at `95aba99ad4305b6d836125ac32bcf86c622258da` passed.
Its executable SHA-256 is
`8f1d0cf957e5a603fa2cf6dfa6a5049bd8a1ac98c4d93a0fc752fe151645a7c7`.
The admission suite passed 93/93 with zero leaks and zero unexpected error logs.
The independently compiled callback ABI regression also passed, including actual
dense saturation, wait timeout, closure, and all six admission error identities.

A focused native lookup/mixed workload run completed 23,198 operations with 470
expected 429 rejections, zero request errors, zero generator drops, and clean
shutdown. It exercised C1/30/40/80 and fixed arrivals at 100/200 operations per
second. Its 30-second transport deadline and short windows deliberately test
correctness separately from the failed peak-rate overload experiment. They do
not establish the latency, throughput, fairness, or overload-recovery gates.
All 35 files in its checksum manifest were verified.

```text
/tmp/workload-native-debug-95aba99ad4-build.json
/tmp/workload-native-correctness-95aba99ad4-receipts/
```

The same committed binary also completed the calibrated 256 × 16 native vector
smoke. The baseline completed 33,852 queries; the tracked-memory/dense-scheduling
arm completed 26,364 with 6,076 expected 429s. Completed queries had recall 1.0
throughout, both processes exited cleanly, and neither logged an untransportable
error, panic, or error-level message. The earlier HTTP 500 failure did not recur.

This vector run is **generator-invalid**, not qualified: the baseline and
candidate dropped 10,144 and 10,047 offered arrivals, respectively. The harness
correctly returned exit 2 with `correctness_passed=true` and
`release_qualified=false`. All 97 receipt checksums were verified. Receipts are
in `/tmp/workload-native-vector-95aba99ad4-receipts/`.

## Retained native experiments

| Source | Experiment | Result |
| --- | --- | --- |
| `42b8c19983` | Paired lookup/mixed-read-write smoke at C1/30/40/80 and open-loop overload | Failed: background executor saturation terminated the baseline, and request/index allocator mismatch terminated the tracked-memory arm |
| `42b8c19983` plus isolated diagnostic patch | Real stored-table query with separate DebugAllocator backing for each request owner | Located wrong-allocator index cleanup; patched query returned 200 and process exited cleanly; diagnostic build is not a committed artifact |
| `3d9b3aaa1b` | Rebuilt paired lookup/mixed smoke, 48 measured points | Both processes completed and exited cleanly; experiment still failed due to transport deadlines/late responses and generator overload |
| `3d9b3aaa1b` | Independently calibrated 256 × 16 cosine vector smoke, C1/30/40/80 | Completed queries met recall 1.0; experiment failed because dense `AdmissionFull` became HTTP 500 across the callback boundary; generator overload also invalidated load evidence |

The failed runs remain intact. Their checksum manifests were verified: 256 files
for the rebuilt mixed smoke and 97 for the vector smoke. Correcting a bug or
rerunning a narrower correctness test does not retroactively qualify these runs.

Local receipts are under:

```text
/tmp/workload-native-42b8c19983-receipts/
/tmp/workload-native-3d9b3aaa1b-receipts/
/tmp/workload-native-vector-3d9b3aaa1b-receipts/
/tmp/workload-native-debug-3d9b3aaa1b-build.json
/tmp/workload-native-leak-probe.patch
/tmp/workload-query-leak-probe-fixed/probe-provenance.json
```

The committed `3d9b3aaa1b` binary was built from a clean verification checkout;
its SHA-256 is
`adac556d2402b043b2ec91d483c0a68f05a6f78f936231c6adb59f4469cfaebf`.
The harness freezes each arm's executable before starting either lifecycle.

## Telemetry correction

The receipt audit found that the earlier harness requested `/metrics` on the
public API port while disabling the dedicated health/metrics listener. The API
served dashboard HTML with HTTP 200. The `.prom` files in the experiments above
must not be used as Prometheus evidence. Their request samples, process exit
records, and artifact checksums remain valid; they do not establish the
per-stage metrics or memory-reservation release gates.

Harness commit `fc2d5878ec` enables the dedicated health/metrics listener and
rejects HTML or invalid sample text. A real native probe of the `95aba99ad4`
binary then returned valid Prometheus text but failed its capability assertion:
`antfly_admission_query_diagnostics_available` was zero, and configured queue
and memory limits were missing. The standalone handler statistics projection
carried only the four legacy counters. Its zero queue/retained observations
cannot establish ownership retirement. This failed probe is retained unchanged
at `/tmp/workload-metrics-native-95aba99ad4-probe/`.

The full snapshot fix (`811956a541`, API ABI 24) passed the 94/94 admission
suite with zero leaks. Its regression exercises live queue and retained bytes,
policy generation, rejection/allocation diagnostics, completed wait histograms,
and draining through the kernel export and host conversion. The clean production
build exposed a data-runtime caller still using removed flat fields;
`c6ad0692ec` corrects that caller. The failed build receipt remains at
`/tmp/workload-native-debug-811956a541-build.json`.

`488637eb6b` additionally carries full query/write snapshots through the data-node
health exporter. Its focused data-runtime regression passed 1/1 with zero leaks,
using actual runtime collection and real retained query/write leases. Logs are
`/tmp/workload-admission-telemetry-gate.log` and
`/tmp/workload-data-admission-telemetry.log`.

The clean Debug production build at
`488637eb6bca782387c612f8c4d1f6fbe7945a72` passed, with executable SHA-256
`826916e2620f72ce370a5aeab80726dc49e747b780da3f8074cc5340c5dac508`.
The native dedicated-metrics probe then passed: both query and write snapshots
reported diagnostics available, capacity 32, queue capacity 128 / 8 MiB,
retained ceiling 64 MiB, and wait ceiling 100 ms. After observing nonzero query
and write admission peaks, active requests, queued requests, and retained bytes
were zero for both classes. The stored-table query returned 200 and the process
exited cleanly. All 35 receipt checksums were verified.

The health endpoint refreshes its cached body asynchronously every five seconds.
An initial 100 ms post-query assertion saw the unchanged startup snapshot and
failed its nonzero-peak check. That probe remains at
`/tmp/workload-metrics-native-488637eb6b-probe/`. The corrected probe retained
every observation and polled within a 15-second limit until the snapshot had
observed both kinds of admission; this took 5.12 seconds. It establishes correct
projection and eventual idle ownership, not a 50 ms retirement or performance
gate. Retained evidence:

```text
/tmp/workload-native-debug-488637eb6b-build.json
/tmp/workload-metrics-native-488637eb6b-refreshed-probe/
```

## Interpretation

The mixed smoke verifies that the two original process crashes no longer
recurred in that experiment. It does not prove acceptable overload behavior.
The vector failure demonstrates why unit tests and a successful build are
insufficient: error transport across the production callback boundary also
needs pressure coverage.

The harness now counts warmup failures, checks clean process shutdown, and
retains a machine-readable failure list. Exit 1 indicates request/runtime
failure, exit 2 indicates generator-invalid evidence without detected
correctness failure, and exit 3 indicates otherwise-clean execution without
valid Prometheus snapshots. Exit 0 indicates clean evidence only for the tested
subset; `telemetry_complete` attests collection and sample format, not metric
coverage or full release qualification. Expected 429 overload responses remain distinct from failures.

Remaining release work includes storage streaming ownership, protected
execution and recovery progress, operator fairness, remote coordinator
reconciliation, all write-recovery paths, and the optimized resource matrix.
See the [implementation record](WORKLOAD_SCHEDULING_IMPLEMENTATION.md) for
implemented stages and the [operations guide](WORKLOAD_SCHEDULING_OPERATIONS.md)
for configuration and the document-lookup admission behavior change.

## Frontend ownership follow-up

The following focused checks cover the stage 1 follow-up. They are correctness
checks, not release performance results. The combined integration gate passed;
the production Debug rebuild also passed from a clean, isolated checkout.

| Check | Result | Local log |
| --- | --- | --- |
| Combined stage 1 integration, with local TCP access | 137 passed, 1 optional Wasmtime skip, zero failures/leaks, 6 expected / 0 unexpected error logs, exit 0 | `/tmp/workload-stage1-final-unsandboxed.log` |
| Serverless metadata/write policy plus earlier request/session ownership | 117/117, zero leaks, 6 expected / 0 unexpected error logs, exit 0 | `/tmp/workload-serverless-policy-final.log` |
| Ingress hard partitions and drain-safe outstanding leases (`6dca417df5`) | 14/14 standalone tests, exit 0 | Tool receipt |
| Allocation owner safety, ReleaseSafe | 21/21 standalone tests, exit 0 | `/tmp/workload-owner-releasesafe.log` |
| HTTPx ownership hooks (`f050df6ae9`) | 32/32 selected transport/client tests, exit 0 | `/tmp/workload-httpx-ingress-owner.log` |
| Common configuration | 49/49 selected tests, zero leaks, exit 0; workload-prefixed additions run in the combined gate | `/tmp/workload-ingress-config-test.log` |
| A2A retained allocator installation and parse allocation failures | 15/15 library tests, exit 0 | `/tmp/workload-a2a-owner-library.log` |
| Wasmtime finite limits/fuel and C memory-handle ABI (`a371c3c5ce`) | 3/3, including real local Wasmtime 45.0.2 fuel, memory, and table-limit rejection | Agent tool receipt |

The Wasmtime ceiling is per linear memory, with at most 16 memories, 64 tables,
64 instances, and 65,536 elements per table. The existing default 64 MiB per
memory therefore permits up to 1 GiB of linear memory per invocation, not a
64 MiB aggregate process cap. Frontend ingress bounds invocation concurrency;
Wasm engine/runtime allocation is outside the tracked Zig frontend allocator.
The core ABI now installs the same fuel/resource limits as the component path.

Stage 1 production Debug build: clean revision
`7da1e4c7f9196ad3edd477ce842e5ac7b8668cf7`, exit 0, binary SHA-256
`db4a723bb4d3869d3f4bf94059781f8875fd62ae2494a42533c8bbf07244a849`.
Receipt: `/tmp/workload-stage1-production-debug-receipt.json`; build log:
`/tmp/workload-stage1-production-debug.log`. The isolated checkout preserves this
source/binary provenance while stage 2 changes proceed in the implementation worktree.


## Stage 2 shared read execution

Stage 2 implementation ends at `27146c033f`; documentation revision
`3f9d2c481eccad7254e680737f1029dd6454b209` has the same compiled inputs.
The final integration gate and clean production Debug build both passed.

| Check | Result | Local evidence |
| --- | --- | --- |
| Final combined scheduling integration, with local TCP access | 156 passed, 1 optional Wasmtime skip, zero failures/leaks, 6 expected / 0 unexpected error logs, exit 0 | `/tmp/workload-stage2-final-integration.log`, `/tmp/workload-stage2-final-integration-receipt.json` |
| Independently compiled storage-owner boundary, final production tree | 5/5, zero failures/leaks, exit 0 | `/tmp/workload-stage2-production-debug.log` |
| Transition/demotion scheduler, Debug and ReleaseSafe | 29/29 in each focused run, exit 0; recorded before the transition commit, with only `Bundle.sub` visibility changed afterward | Agent tool receipts |
| Absolute stream deadline and H2 flow-control regressions | 15/15 selected transport/client tests, exit 0 | `/tmp/workload-scan-stream-deadline-final.log` |
| Partial physical frame failure and connection shutdown | 10/10 selected socket/client tests, exit 0 | `/tmp/workload-scan-partial-frame.log` |
| Actual HTTP/1 and HTTP/2 streaming, explicit HEAD, and large response transport | 11/11 selected transport/client tests, exit 0 | `/tmp/workload-scan-stream-real-transport.log` |
| Independently compiled scan-sink deadline ABI | 4/4, exit 0 | `/tmp/workload-scan-deadline-abi.log` |
| Formatting and whitespace | All 38 changed Zig files passed `zig fmt --check`; committed diff passed `git diff --check` | Parent tool receipts |

The integration gate exercises actual DB reads, protected LMDB probes under
saturated general capacity, wide-row demotion and cleanup, nested dense-owner
borrowing, original deadlines, serverless query saturation with independent
writes/probes, helper fallback and joined cancellation, and streamed-scan cleanup.
LMDB scan suspension is tested through completion, cancellation, and snapshot
expiry; default LSM scans are checked to retain coarse runnable ownership.
Configuration tests reject scan state that exceeds the general partition after
protected/transition reservations, including a one-byte boundary overflow.
The optional real Wasmtime skip has separate stage 1 engine evidence above.

Earlier stage 2 runs exposed compilation errors and fixture mistakes: the
protected-probe fixture initially used the default LSM backend, and the
serverless fixture retained prior responses and compared live admission estimates
with actual allocation charges. Those failures remain in
`/tmp/workload-stage2-first.log` through `/tmp/workload-stage2-sixth.log`.
The corrected final gate is the authoritative combined result.

Production command, from `zig/`:

```sh
python3 tools/run_bounded_zig_build.py --zig zig -- build antfly antfly-storage-owner-test -Dstorage-owner-test-filter='opaque storage context' -j2
```

The build returned exit 0 with a clean worktree before and after, at revision
`3f9d2c481eccad7254e680737f1029dd6454b209`. Executable SHA-256:
`7b22dbc38d0c5fa78292b328916cb8bd8eecbe7660ae749a866d69b6bb832f09`.
Receipt: `/tmp/workload-stage2-production-debug-receipt.json`.

These checks complete the two follow-up review stages; they do not qualify a
release matrix row. General operators still have nonyielding coarse regions,
and unaudited backends do not suspend. Distributed coordinator ownership,
remaining write-recovery integration, process-wide progress guarantees, and
optimized resource-constrained performance qualification remain later work.


## Recovery and coordination integration checkpoint

The final development checkpoint for source committed through `63f2eb025f` ran
`zig build antfly-workload-admission-test -j2` with loopback networking enabled:
**206 passed, one optional real-Wasmtime test skipped, zero failures and leaks;
six expected error logs and zero unexpected logs; actual process exit 0**.
The production/test files were hash-checked against the tested working tree
before committing. Retained local evidence:

- `/tmp/workload-recovery-coordination-checkpoint9.log`
- `/tmp/workload-recovery-coordination-checkpoint9-source.json`
- Recorded source digest: `30b3d32fefb94f4b83e93956ef265be27221ca8361e45cb65aa0e2905b2460cd`

This checkpoint covers protected preparation/metadata lanes, reentrant recovery,
bounded paging, legacy completion with a small reserve, visibility-wait lease
release, API/join allocation-failure sweeps, durable coordinator uncertainty,
worker namespace/restart fencing, protected authenticated control ingress, and
compiled callback ownership. It includes metrics-cache age and table-policy
identity checks. It does not qualify full LSM/backend completion memory or
optimized performance. The production Debug build subsequently passed at
`0ac3f0af89bbc85a7074ab50ae81d3d44a25649d`; native coordinator fault
qualification remains pending as described below.

Development failures were retained in checkpoint logs 5 through 8: generated
nullable projection and join-default compiler errors, a shadowed local, missing
admission guards on alternate begin entry points, an existing cancellation error
name mismatch in a new test, and allocation failure surfaced as `WriteFailed`.
They were fixed before checkpoint 9. The join failure audit also repaired partial
decoder cleanup and replaced temporary JSON ownership with direct serialization.

The independent fault-proxy stage passed 47 tests with one optional skip; actual
advertised routing, delay, partition, discarded response, and healing ran on the
earlier frozen `3f9d2c481e` Debug binary. The fixed-policy plan stage passed 53
tests with one optional skip. These are harness checks, not evidence that the
new coordinator has passed native fault qualification or that a Cloud release
performance row is qualified.


### Small-node correctness follow-up

Correctness runs use small local processes and tight resource budgets. Cloud
package sizes are performance qualification targets, not correctness prerequisites.

The production Debug binary at `0ac3f0af89` built successfully, SHA-256
`3dc77f9d9e9da972c1e0c29e37cc79d8a5345df08cf28f1ee8fa8744a34e8c62`.
Evidence: `/tmp/workload-recovery-production-debug-receipt.json` and
`/tmp/workload-recovery-production-debug.log`.

Fresh three-process runs found two failures before coordinator fault injection:

- Recovery-enabled table creation returned HTTP 409 with `unknown-v1`. The
  metadata binary codec omitted storage policy. Commit `c10e602778` preserves
  nondefault settings, validates the extension in full and projected readers,
  and gates replicated admission on membership-bound decoder capability 11.
  Default records retain their existing wire bytes and capability floor.
- A fresh policy-free table created and accepted a batch, but lookup returned
  HTTP 500 reporting `MetadataIncarnationMismatch`. Captured metadata identities
  agree across head, status, catalog and routing responses. A debugger breakpoint
  at the sole identity-mismatch guard did not fire. The retained native error
  trace instead proves an LSM `NotFound` crossed `backend_erased.ReadTxn.get`
  without error translation into the API kernel, where its numeric code meant
  `MetadataIncarnationMismatch`. The read-family callback boundary fix is under
  validation; the metadata identity guard remains unchanged.

Receipts are retained under `/tmp/workload-policy-routing-0ac3f0af89-receipts`,
`/tmp/workload-metadata-identity-0ac3f0af89-receipts`, and
`/tmp/workload-metadata-debug-0ac3f0af89-receipts`. Unknown writes were not replayed.
Owned test processes exited cleanly.

The request/runtime admission artifact passed **213 tests, one optional skip,
zero failures/leaks**, with six expected and zero unexpected error logs
(`/tmp/workload-recovery-coordination-checkpoint10.log`). The separate metadata
artifact passed **64 tests, zero failures/leaks**, including actual catalog apply,
extended record roundtrips and capability classification
(`/tmp/workload-recovery-metadata-checkpoint12.log`). These artifacts have
shared tests; their counts must not be added as distinct coverage. The combined
admission gate now depends on both artifacts. Checkpoint 11 exposed a missing
extension decoder in identity/query projections; checkpoint 12 includes its fix.

Commit `f8c71099cd` adds the unused prepaid completion-credit ownership primitive.
Its focused Debug and ReleaseSafe checks each passed 11 tests. It provides atomic
reservation-to-observer accounting, canonical publication state, stale-copy
rejection and retained-allocation ownership. It is not yet connected to durable
transaction tickets or the LSM/WAL publication path, and therefore does not
establish bounded backend completion under foreground exhaustion.


The production Debug rebuild at product revision `c10e602778` passed, SHA-256
`5ca232f17a0a42e8fbeeb4f55177a870029c1573c6fe5e25f6f34d6159a53ad5`.
Receipt: `/tmp/workload-recovery-c10-production-debug-receipt.json`.
The cross-boundary error's original trace is retained in
`/tmp/workload-metadata-debug-origin-lldb.log`; its fresh lifecycle receipts are
`/tmp/workload-metadata-debug-origin-0ac3f0af89-receipts`.

The broad compiled storage-owner fixture initially failed its bulk-search
assertion. Its preceding artifact fixture had left a chunk-backed index with
its child range moved away; parent-only rows did not belong to that index even
before bulk ingest. Commit `f45dd86e94` configures and explicitly queries an
ordinary text index, preserving the bulk search-publication assertion. That
assertion now passes. The focused fixture then fails a later native-backup
quiescence guard (`NativeBackupRepairStateNotQuiescent`), with zero leaks;
therefore the storage-owner gate is **not passed**. Evidence:
`/tmp/workload-storage-bulk-context.log` and
`/tmp/workload-storage-bulk-final.log`. No backup guard was weakened.


### Compiled callbacks and native deadlines

The independently compiled read-handle regression passed at `b6dfbb8409`,
including distinct error ordinals and missing-key `NotFound` through reads,
probes, scopes and forks. The production Debug rebuild passed with SHA-256
`0e3e5b0d0a2e51a3f4287daf5aca9b26bad935e50e64eb6643609c82d1d9c6f5`
(`/tmp/workload-recovery-b6-production-debug-receipt.json`). The combined gate
then passed its 213-test request/runtime artifact and 64-test metadata artifact,
with one optional skip and zero failures/leaks
(`/tmp/workload-recovery-coordination-checkpoint14.log`). These counts overlap.

The next native reconciliation cell reached the worker, which returned a signed
HTTP 504 after approximately 11 ms on a healthy read. Bounded response capture
confirmed the complete `request deadline exceeded` response originated at the
worker. Evidence: `/tmp/workload-worker-response-b6dfbb8409-receipts`.
The cause was a missing native-to-catalog clock translation in provisioned route
fence validation. Commit `04c5f3da89` translates the deadline through the catalog
clock; the focused regression passed with deliberately different clock epochs.
The native reconciliation cell is being rerun with this fix.

A second independently compiled regression proved that reverse replay callbacks
also mistranslated private consumer errors. Commit `9c834c7d45` keeps those errors
in the consumer's synchronous stack and sends only a stable stop bit across the
boundary. The two compiled callback tests passed with zero leaks; the negative
and fixed receipts are `/tmp/workload-replay-callback-private-before.log` and
`/tmp/workload-replay-callback-after.log`. ABI versions are now API 29, storage 67,
and native callback 11. The subsequent production rebuild includes this change.

The exact proposal-guard test (`d542ed4854`) passed: missing or older decoder
activation rejects both single and batch policy proposals without advancing the
Raft log, and matching activation allows policy-preserving publication. Its
metadata artifact runs 123 tests after instantiating the HTTP-service fixture,
all passing without leaks (`/tmp/workload-metadata-proposal-activation.log`).


### Local replicated correctness

`ce3ff4cfe0` adds `scripts/workload_metadata_quorum.py` and five harness tests.
The final native run passed on the frozen `c10e602778` Debug binary:
`/tmp/workload-metadata-quorum-c10-final/receipt.json`. All 30 receipt-file
checksums verified. The wrapper archives its dependencies and rendered topology.

The six local processes form one three-voter metadata group and three actual
data voters. Status checks bind the metadata group, incarnation and voter-set
fingerprint across restart; data-store reports prove actual local voter roles,
a common three-voter fingerprint and applied progress. The test kills the
observed metadata leader, verifies its successor, loses quorum, submits one
mutation, restores the original roots, and verifies new writes and all earlier
sentinels through all three data processes. The no-quorum mutation timed out;
its unknown outcome was observed without replay. All owned processes exited
cleanly, with no cleanup errors.

Data scheduling uses 24 ingress requests/16 MiB, four active query and write
requests with eight queued each, and two read tasks with 4 MiB working memory.
Coordinator/worker attempt ownership and transaction completion reserves are
explicitly disabled in this independent Raft case. These are application
budgets, not enforced process-memory or CPU envelopes. This run does not qualify
performance, data-replica loss, or a durable transaction-decision crash.


### Integrated clock and callback checkpoint

The production Debug build at product revision `04c5f3da89` exited zero. Its
frozen binary SHA-256 is
`b84aa8e91e7b6b2a572187d672f90152e29d19f610bea2d0b7ee5ae9c2399f5a`;
receipt: `/tmp/workload-recovery-04c-production-debug-receipt.json`.
The combined admission gate also exited zero: metadata 123/123 and
request/runtime 213 passed, one optional Wasmtime skip, zero failures/leaks,
six expected and zero unexpected error logs. These artifacts overlap; counts
are not additive. Log: `/tmp/workload-recovery-coordination-checkpoint15.log`.
