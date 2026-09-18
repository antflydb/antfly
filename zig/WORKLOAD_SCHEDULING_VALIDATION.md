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

## Interpretation

The mixed smoke verifies that the two original process crashes no longer
recurred in that experiment. It does not prove acceptable overload behavior.
The vector failure demonstrates why unit tests and a successful build are
insufficient: error transport across the production callback boundary also
needs pressure coverage.

The harness now counts warmup failures, checks clean process shutdown, and
retains a machine-readable failure list. Exit 1 indicates request/runtime
failure, exit 2 indicates generator-invalid evidence without detected
correctness failure, and exit 0 indicates clean evidence only for the tested
subset. Expected 429 overload responses remain distinct from failures.

Remaining release work includes full ingress/planning/stream ownership,
protected execution and recovery progress, operator fairness, remote coordinator
reconciliation, all write-recovery paths, and the optimized resource matrix.
See the [implementation record](WORKLOAD_SCHEDULING_IMPLEMENTATION.md) for
implemented stages and the [operations guide](WORKLOAD_SCHEDULING_OPERATIONS.md)
for configuration and the document-lookup admission behavior change.
