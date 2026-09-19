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


### Native reads and backup repair checkpoint

The frozen `04c5f3da89` Debug binary passed the three-process
`local-correctness.json` scenario: exact document lookup, advertised-proxy
partition/heal, API kill/restart on its original root, and correct reads afterward.
All seven checked requests passed, with zero unknown writes; all three cleanup
exits were zero. All 21 receipt checksums verified in
`/tmp/workload-local-correctness-04c-receipts`. This is local correctness only.

The subsequent reconciliation scenario passed its initial exact lookup and
verified signed discovery, initial generation fence and request-bound terminal
HTTP 200. Its strict metrics gate then rejected duplicate
`antfly_lsm_cache_kind_used_bytes` series before any injected fault. Evidence:
`/tmp/workload-reconciliation-04c5f3da89-receipts`, all 20 checksums verified;
independent signed proof check:
`/tmp/workload-reconciliation-04c5f3da89-proof-check.json`. This run does not
establish lost-response reconciliation or restart closure.

Commit `b5f32967be` closes the earlier storage-owner fixture failure. A deliberate
partial index reconciliation had left the sibling full-text repair journal in
`detected` state; document-artifact repair did not resolve that separate debt.
The fixture now proves backup rejects pending repair, explicitly completes
index repair, then verifies backup success. The specific guard error also
survives the status-only compiled boundary (failure ABI 54, storage ABI 68;
API ABI 29 unchanged). The full storage-owner gate passed **33/33**, zero leaks,
one expected and zero unexpected error logs, actual exit zero. Log:
`/tmp/workload-storage-owner-backup-final.log`. The backup guard is unchanged.


Commit `186bd1b0da` removes the duplicate cache metric family. The renderer
uniqueness regression passed 1/1 through its owning `antfly-data-runtime-test`
target, with zero leaks and actual exit zero
(`/tmp/workload-cache-metrics-focused.log`). The combined admission gate also
passed 123 metadata and 213 request/runtime tests with one optional skip; that
separate gate does not import the renderer fixture.

The allocator-provenance prerequisite is integrated as `05bd7c5b0e`; its
[scope and measured structure overhead](WORKLOAD_LSM_COMPLETION.md) remain
explicit. It does not activate backend completion credits or durable tickets.


### Native response-loss and distinct-destination evidence

The production Debug build at product revision `05bd7c5b0e` exited zero. Its
frozen binary SHA-256 is
`f37e9cf92b8fc5299136a10432b540b6e1823d69e820085ae8f36ded7be00c91`;
receipt: `/tmp/workload-recovery-05bd-production-debug-receipt.json`.

The fresh reconciliation run passed initial exact lookup and the strict fresh
metrics gate. Its first injected response loss then failed: the expected HTTP
503 became HTTP 500 (`DistributedQueryUnavailable` escaped the public lookup
adapter). The worker had returned a valid signed HTTP 200 for coordinator 3,
generation 2, sequence 2. The relay observed all 814 response bytes and discarded
them all; none were forwarded. The preceding sequence 1 response was forwarded
normally. No threshold or expected status was relaxed.

Evidence: `/tmp/workload-reconciliation-05bd7c5b0e-receipts`, with all 20 checksums
verified. The independent checker verified four request-associated signed
proofs (discovery, initial fence and two terminals), with zero errors:
`/tmp/workload-reconciliation-05bd7c5b0e-receipts-proof-check.json`.
This proves worker termination and relay-observed response loss. It does not
prove the coordinator received the discarded response or retired its record.
The run stopped before debt sampling, healing or restart closure; all three
processes exited zero, with no cleanup errors or schedule violations.

Commit `ae31b25866` adds a separate four-process destination-isolation wrapper.
Its setup-only run on the frozen `04c5f3da89` Debug binary passed:
`/tmp/workload-destination-04c5f3da89-setup2`, all 24 checksums verified.
Two tables had distinct actual single-voter leader groups on destinations 4 and
2, confirmed by metadata store reports and matching advertised-proxy group
paths. Both seeded document lookups returned their exact expected values.
All four processes exited zero. This setup-only result does not qualify fault
isolation. Full mode requires loss on one destination while three reads on the
other succeed, fresh coordinator debt samples, and retirement after healing.
The coordinator has two total attempts and one per destination. An earlier
setup fixture expected table-space creation status 200 instead of the actual
201; that failed receipt remains preserved, and setup2 used a fresh lifecycle.


### Prepaid memory publication integration

Commit `013b533924` integrates the internal memory-only sealed point-batch path.
Its isolated Debug and ReleaseSafe gates each passed 27 tests without leaks,
including exhaustion, reduced limits after sealing, unchanged-root failure
unwind, pinned readers, stale-root retirement outside the writer mutex, and
close waiting for live ownership. Logs: `/tmp/workload-lsm-prepaid-debug.log`
and `/tmp/workload-lsm-prepaid-release-safe.log`.

The scheduling integration gate then exited zero: compiled storage owner 33/33,
metadata 132/132, request/runtime 222 passed and one optional Wasmtime skip,
all without failures/leaks. Error logs were one expected for storage owner and
six expected for request/runtime, with none unexpected. Artifact counts overlap.
Log: `/tmp/workload-prepaid-integrated.log`. Persistent completion remains
unsupported by this internal entry point.

The public lookup/scan availability regression also passed 1/1 without leaks
(`/tmp/workload-distributed-read-503.log`). Commit `39cd8b34b8` maps distributed
read unavailability to the shared HTTP 503 response with Retry-After. Its frozen
production Debug build exited zero; SHA-256:
`69c36d8db8835a9a7acacc096aed9d901d612126dccc9388680e8b042ac49d64`.
Receipt: `/tmp/workload-recovery-read503-production-debug-receipt.json`.
This frozen binary predates the internal prepaid-memory stage.


### Passed native reconciliation and destination isolation

Both following local correctness cells passed against the frozen production
Debug binary at `39cd8b34b86f6907b64e43d64b8288631519032b`, SHA-256
`69c36d8db8835a9a7acacc096aed9d901d612126dccc9388680e8b042ac49d64`.
Build receipt: `/tmp/workload-recovery-read503-production-debug-receipt.json`
(actual exit zero). This binary predates `013b533924` and subsequent WAL/completion
work; these runs do not validate those later changes or qualify performance.

The complete reconciliation cell passed all 19 checked requests with no schedule
violations, unexpected errors or cleanup failures:
`/tmp/workload-reconciliation-39cd8b34b8-receipts` (21/21 checksums verified).
Each of three discarded-response phases returned HTTP 503 and retained one
coordinator record; healing restored the exact document and zero records under
fresh, stable metrics checks. The worker restart retained its namespace and
increased its epoch. The API restart preserved worker identity and recovered.
All owned processes exited zero during final cleanup; intentional fault kills
are recorded separately.

The independent checker verified 17 request-associated signed proofs with zero
errors in `/tmp/workload-reconciliation-39cd8b34b8-receipts-proof-check.json`.
After API restart, a matching fence covered generation 2 and was completely
forwarded at relay time 130191380750 ns, before the generation 3 request first
arrived at 130192677750 ns. This is signed identity plus relay-observed ordering,
not proof of application consumption. Coordinator retirement is separately
supported by the scenario's fresh record-count checks and successful reads.

The complete two-destination cell also passed:
`/tmp/workload-destination-39cd8b34b8-receipts` (24/24 checksums verified).
Actual single-voter groups and advertised routing proved distinct destinations
4 and 2. After losing a response from destination 4, its lookup returned 503 and
coordinator records stayed at one. Three exact lookups on destination 2 succeeded
through its own proxy while that debt remained. Healing destination 4 restored
its exact lookup and zero records. All four strict metrics gates and 23 semantic
requests passed; all four processes exited zero, with no cleanup or proxy errors.
The independent sidecar verified 14 associated signed proofs with zero errors:
`/tmp/workload-destination-39cd8b34b8-receipts-proof-check.json`.
These are small local correctness results, not replicated performance or
whole-process resource-envelope qualification.


### Prepared WAL and native I/O prerequisites

The prepared WAL append stage (`7e4f72005e`) passed all 22 focused WAL tests
in the scheduling worktree (`/tmp/workload-wal-integrated.log`, actual exit zero).
The native WAL completion I/O scope (`f967ded772`) then passed the combined
storage-I/O and WAL gate: 60/60 tests, no leaks, actual exit zero
(`/tmp/workload-native-wal-integrated.log`). Its native tests exhaust allocation
and ordinary descriptor capacity after preparation, close the original native
owner, append through the retained scope, and reopen the resulting records.
The scope owns two descriptor permits and uses sequential, preallocated path
and atomic-writer state. Provider failures still produce uncertain outcomes.
These are internal prerequisites; no persistent completion ticket or public
mandatory-completion guarantee is enabled by these results.

### Frontend overload failure retained

The strict frontend C40 cell on frozen `39cd8b34b8` failed with 12 exact successful
reads, 20 structured admission rejections, and eight unexpected generic HTTP
503 responses. `antfly_http_request_dispatch_rejections_total` was exactly eight.
The data/API listener still had a fixed 32 request-task limit despite configured
ingress capacity 128. Query active=4 and queued=8 were observed; the API health
and readiness probes succeeded. All owned processes were cleaned up and all
21 receipt checksums verified:
`/tmp/workload-frontend-39cd8b34b8-receipts2`. C80 was not reached.
The generic transport responses remain failures under the qualification contract.
A larger task capacity alone does not establish protected request-task or
connection capacity for control and authenticated recovery traffic.

The data/API transport now derives request-task capacity from enabled ingress
capacity (with the legacy 32-task floor), retaining 32 additional connection
slots for parsing and overload rejection. Disabled ingress retains 32 tasks
and 64 connections. Upload body slots and buffered-byte limits remain at their
previous ceilings. The actual data-runtime owning target passed both listener
configuration tests with no leaks and actual exit zero
(`/tmp/workload-ingress-transport-capacity.log`). This wiring change does not
yet qualify native C40/C80 or protect lanes at transport dispatch.


### Frontend C40/C80 correctness after listener capacity wiring

The strengthened frontend cell passed on frozen Debug product
`3eea19e0277b4edf4df70ec82b991d657350d0ed`, binary
`/tmp/workload-transport-3eea19e027-antfly`, SHA256
`5f94c71d37b309f3b21d622ce42db343dc7b68190dfda5f5e82bc4cc60539aa8`.
The production build exited zero; its receipt is
`/tmp/workload-transport-3ee-production-debug-receipt.json`. This artifact
predates integrated WAL fix `b7a751239a` and protected transport task partitions.

The native run exited zero and retained
`/tmp/workload-frontend-3eea19e027-receipts` (21/21 checksums reverified).
C40 produced 12 exact successful reads and 28 structured HTTP 429 responses;
C80 produced 12 exact reads and 68 structured 429 responses. Rejections carried
`AdmissionQueueFull`, `reason=instance_busy`, `stage=admission`,
`execution_started=false`, and `Retry-After: 1`. There were no unexpected
HTTP statuses or transport failures. The assertions require successful reads
and valid rejections, not these exact observed counts.

Both bursts dispatched within their predeclared 100 ms generator bound:
the last dispatch followed the common submission barrier by 1.644 ms and
7.359 ms respectively. With query active=4 and queued=8 observed, API-listener
`/healthz` and `/readyz` returned semantic `ok`/`ready` responses while twelve
client calls remained pending before and after the probe pair. After healing
the upstream delay, exact lookup plus stable idle ownership completed in
1.289 s and 1.301 s, within the original ten-second recovery bound.

All 75 sampled metric checks passed; maximum measured source age was
0.313 s. Query peak active remained four, and final active, queued, outstanding,
and retained-byte values were zero. Connection and request dispatch rejection
counters stayed zero. Request-task capacity and reservation were both 144
(128 API plus 16 health tasks). All three owned processes exited zero without
forced termination or cleanup errors. General/control ingress gauges were not
exported by this artifact; their configuration is retained, while metric checks
cover the exported query, write, and recovery capacities.

This isolates frontend correctness with remote attempts and backend read
scheduling disabled. It demonstrates control progress during query saturation,
not protected task or connection capacity under transport saturation. It is
not throughput, Cloud resource-envelope, or release qualification. The earlier
failed C40 receipt remains unchanged.


### Uncertain WAL writes and writable tail repair

Commit `b7a751239a` integrates the independently reproduced WAL correctness fix
(`b5097c169f` in the isolated worktree). Before the fix, a partial append followed
by writable reopen could acknowledge a later write, then fail the next reopen
with `CorruptLsmWal`. The initial native regression exited one:
`/tmp/workload-wal-uncertainty-baseline.log`.

Backend appends now use the single-use prepared outcome, fence further mutation
on any uncertain storage failure, preserve unpublished manifest debt, and never
retry an append after `FileNotFound`. Pure preparation failures remain unfenced.
Writable startup checkpoints all successfully replayed state and resets an
ignored torn tail before admitting foreground writes; failed repair rejects
startup. Read-only startup leaves the tail untouched.

The exact isolated source passed 19/19 tests in both Debug and ReleaseSafe with
no leaks (`/tmp/workload-wal-uncertainty-final-debug.log`,
`/tmp/workload-wal-uncertainty-final-release-safe.log`). Cases include partial and
fully synced unknown writes, same-process fencing, debt conservation, failed
repair/retry, a torn first record, existing SSTs, tombstones, multiple segments,
and preserved replay/outbox keys. The scheduling integration then passed the
combined WAL/uncertainty gate: 26/26, no failures/leaks, actual exit zero
(`/tmp/workload-wal-uncertainty-integrated.log`). This fixes ordinary backend
recovery; it does not provide persistent transaction completion reservations.

### Protected request-task dispatch integration

The HTTP transport stage `dce5aa1173` adds nonborrowable general, control and
recovery request-task permits. The protocol follow-up `de60e6f161` rejects
ambiguous HTTP/2 request headers before classification and excess declared-body
DATA before buffering. The combined transport gate passed 122/122 with actual
exit zero (`/tmp/workload-http-dispatch-framing-integrated.log`). This includes
real H1, H2 and h2c request dispatch and permit cleanup; it does not reserve
incoming connections or header parsing capacity.

The first API integration gate passed 141 metadata/storage tests and 223 runtime
tests with one optional skip, no failures or leaks, actual exit zero
(`/tmp/workload-transport-partition-api-integration.log`). It covers API ABI 30,
bounded stack authentication, fail-closed incompatible tables/lane values, the
observer metadata pins and sealed native point-batch helper. This gate preceded
the close-entry and malformed-route follow-ups; their final checks are recorded
separately. Native transport saturation qualification must use a newly frozen
binary containing the dispatch integration; the earlier C40/C80 binary does
not establish protected request-task capacity.

Review follow-ups fixed overlapping group route prefix/suffix bounds
(`584eea6e53`, route gate 2/2, `/tmp/workload-dispatch-route-bounds.log`) and
native completion entry racing a draining close (`5511c11024`, native gate 6/6,
`/tmp/workload-native-prepaid-close-entry-final.log`). Both exited zero without
leaks. The API bridge now preserves HEAD fallback for a selected GET route,
leaving the host method unchanged for transport body suppression; its new
regression and malformed-path classifier cases are part of the final API gate.

The final API gate at `70e2476501` passed 143 metadata/storage and 235 runtime
tests, one optional Wasmtime skip, no failures/leaks, actual exit zero
(`/tmp/workload-dispatch-api-final.log`). The standalone gate passed 133/133,
including the changed listener sizing (`/tmp/workload-dispatch-standalone-config.log`).
The Python qualification harness gate passed 86 with one optional skip
(`/tmp/workload-dispatch-python-final-loopback.log`); the initial sandbox run
could not bind loopback sockets and is not a product failure.

### Native task saturation exposed catalog executor exhaustion

The clean frozen Debug build of `70e24765012e27790a80784ca514f41c4fdbef55`
exited zero (`/tmp/workload-dispatch-70e-production-debug-receipt.json`). Binary
`/tmp/workload-dispatch-70e2476501-antfly` has SHA-256
`c4ca1ea38cc4ef8c9cea4071547c2cf93f28e70791f267975e5d81ef2adc6bb1`.

The first native task-partition run failed qualification, actual exit one;
preserved evidence is `/tmp/workload-dispatch-70e2476501-receipts` (22 checksums
verified, all three owned processes exited zero). The generator dispatched all
32 requests within the existing 100 ms bound, but 14 returned HTTP 500
`RuntimeBoundaryFailure` in 5–10 ms. API logs contain exactly 14 untransportable
`system_catalog` callback errors with cause `ConcurrencyUnavailable`. The other
18 requests remained active and later returned the exact document. Listener
task capacity/reservation was the expected 50; permit, executor and connection
rejection counters stayed zero. The test correctly refused to claim saturation
or protected-lane progress. No acceptance threshold was relaxed.

The metadata client's four instances share the bounded API executor. Local
request-task admission exhaustion escaped the read-only catalog retry loop as
an untransportable terminal error. This is a separate nested execution-capacity
failure from the earlier 32-task public listener limit. The failed receipt is
not evidence of historical benchmark attribution or optimized performance.

### Distributed reconciliation after protected dispatch integration

The same frozen `70e2476501` Debug binary passed the local response-loss and
worker/API restart scenario, actual exit zero:
`/tmp/workload-reconciliation-70e2476501-capture-receipts`. All 19 requests met
their assertions; 21 receipt checksums verified, with no schedule violations,
unknown writes or cleanup errors. The earlier run without bounded response
capture also passed and remains at
`/tmp/workload-reconciliation-70e2476501-receipts`.

The independent proof verifier checked 17 signed exchanges (four discovery,
four fence, nine terminal), including three intentionally dropped terminal
responses. After API restart it observed complete fence delivery for generation
2 before the next generation-3 request on the same relay. Worker restart kept
its namespace and increased its epoch; API restart preserved the worker's
namespace and epoch. Thirty-five fresh metric samples had maximum source age
0.301 seconds. All five owned process IDs were absent after cleanup: the two
fault-injected predecessors were killed as planned, and the three final
processes exited zero.

`/tmp/workload-reconciliation-70e2476501-proof-verification.json` records the
checks outside the sealed receipts. This establishes observed signed transport
ordering plus sampled coordinator accounting for these local faults; it does
not prove application consumption of every observed response, every durable
decision crash boundary, or performance/Cloud qualification.

### Native saturation after catalog admission retry

`4689cf405c` retries local executor admission exhaustion only for read-only
catalog calls, preserving their original deadline and cancellation budget.
The owning gate passed 2/2 with actual exit zero and no leaks
(`/tmp/workload-catalog-executor-retry-final.log`), including real one-worker
exhaustion/recovery, deadline expiry, cancellation and unknown-error propagation.

The next clean Debug build (`/tmp/workload-dispatch-4689-production-debug-receipt.json`)
has SHA-256 `5e6dbf5e451ca5afd25ab25896279257abab1bc8735841fb07a9fca8f6a93c5d`.
Its native run remains **failed**, preserved at
`/tmp/workload-dispatch-4689cf405c-receipts` (22/22 checksums verified, all three
processes exited zero). Initial, saturated and post-probe metric gates passed:
32 query/request tasks active, no queued queries, and exact task capacity 50.
Both excess general requests received the required structured 429. GET and HEAD
health/readiness probes returned 200, and authenticated recovery discovery
returned a nonce-bound protocol-3 proof verified live. All 32 clients remained
pending throughout the 0.524-second probe interval. Executor and connection
dispatch rejection counters stayed zero; permit rejections increased by two.

Completion exposed a second product failure: 29 reads returned the exact
document, while three returned HTTP 404 `not found`. The data process logged
three untransportable `lookup_group_local_routed` errors with cause
`ConcurrencyUnavailable`. The forwarding path treated unexpected HTTP statuses
as absence. The run did not reach its final idle/recovery gate and does not
qualify the full policy. The harness diagnostic fix `b82cf608b6` now reports the
status and bounded body excerpt instead of masking non-JSON failures with a
JSON decoding exception; it changes no acceptance threshold.

### Routed lookup capacity and explicit absence

`fc647c346f` bounds retries of local routed lookup executor exhaustion by the
original route fence and request deadline. Each failed attempt unwinds its
temporary ownership before waiting; cancellation is checked on both request
and fence, and noncapacity errors are not retried. Only an explicit remote 404
is absence. Unexpected HTTP failures propagate instead of becoming a missing
document. The focused availability gate passed 3/3, actual exit zero
(`/tmp/workload-lookup-status-availability-final.log`).

The final admission gate passed 144 metadata/storage tests. Its runtime suite
initially hit sandbox loopback restrictions; the identical already-built runtime
test binary passed with socket permission: 236 tests, one optional Wasmtime skip,
zero failures or leaks, actual exit zero
(`/tmp/workload-routed-lookup-capacity-permitted.log`). A fresh review found no
blocking issue in retry ownership, deadline translation or error mapping.

The clean frozen Debug build of
`fc647c346fc1b969736d8a273c5a33cffc3efd21` exited zero. Binary SHA-256 is
`f63530872fef0fa2edcd60e622c8005769ada73334da3d0aca633dd878a60477`;
build receipt: `/tmp/workload-dispatch-fc647-production-debug-receipt.json`.
The native run **failed during setup**, before any saturation or protected-probe
checks: `/tmp/workload-dispatch-fc647c346f-receipts`. All 22 receipt checksums
verified. The seed batch timed out with an unknown write outcome and was not
replayed. The data process aborted while logging a routed-write error; API and
metadata exited zero. All three owned PIDs were absent after cleanup, but the
receipt correctly records two cleanup errors for the unexpected data crash.

The producer reported `MetadataSnapshotUnavailable` during cache-only routing
before proposal. The routed batch callback passed a compilation-local Zig error
directly into the separately compiled API runtime, where error-name logging
used the wrong error domain. This failure supplies no overload qualification
evidence. Both the unchecked callback boundary and pre-proposal cache-readiness
handling require correction; no acceptance thresholds or failed receipts changed.

Manifest review also found that the dispatch fixture's component-name exclusion
omitted the entire node named `data`, including its server log and config.
`78c284149b` prunes only configured node storage directories and the root binary
directory, and includes node logs, configs and catalog evidence. Nine focused
harness tests and lint/format checks passed. Old receipts remain unchanged;
the failed `fc647c346f` data evidence is separately hashed in
`/tmp/workload-dispatch-fc647c346f-data-evidence-checksums.json`. Its server-log
SHA-256 is `6d1228add2557e5df5ea2246795fec6dc7960cfba54fa0dd3fd613648b7768f9`.

### Checked write callbacks and cache-only startup routing

The follow-up replaces raw routed-write error callbacks with the checked native
callback trampoline, including the sibling validation/cancellation carriers.
API ABI 31 rejects incompatible configuration layouts before reading them.
`MetadataSnapshotUnavailable` and `GroupLeaderUnavailable` have append-only
stable status details; unknown write outcomes retain their separate identity.
The independent archive test exercises optional-void success/absence and exact
write-error reconstruction, in addition to the existing dense admission cases:
`zig build runtime-callback-abi-test -j1` exited zero (two selected tests), log
`/tmp/workload-routed-write-callback-abi-final.log`. Its first build exposed a
missing platform import in that test target, which was corrected before rerun.

Cache-only forwarded writes now request asynchronous metadata synchronization
when the cache is absent, then remain in the existing pre-proposal leader loop.
An absent cache does not mark an unattempted leader unreachable. A known missing
target still follows the existing placement fallback. The change grants no new
deadline, forwarding hop or campaign budget, performs no synchronous metadata
request, and does not retry an ambiguous mutation.
