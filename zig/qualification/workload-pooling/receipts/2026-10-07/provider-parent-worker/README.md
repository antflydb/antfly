# Provider work under a parent runnable grant

SQL decision runtimes disable provider task fan-out when the adapter owns a
runnable execution or marks ordinary write execution as serial. Each provider
job then runs on the parent worker. Unscheduled callers retain parallel batches.
The test invokes a real embedded provider callback for two requests, checks the
parent thread and input order, and verifies row and batch accounting.

The native ReleaseSafe `functions-test` gate passed all 47 build steps, 17
embedded tests and seven coordinator tests, with no skips or leaks. Source
hashes and the exact command are in `native.json`; the complete passing log is
`native-authorized.log.gz`. The initial sandboxed run failed two existing HTTP
socket fixtures with `Unexpected`; the new parent-worker fixture passed. Both
the direct embedded artifact and the complete build passed with local-network
access. The initial failure is retained rather than counted as a product pass.

This qualifies the provider runtime contract on native macOS only. Linux and
SQL API integration are pending. It does not qualify independent child admission,
provider wait suspension, model/backend inference scheduling, or automatic defaults.

The strengthened provider fixture returns distinct probabilities for each input
and verifies their result order and total input-token usage. The same 47-step,
24-test native gate passes; `native-order.json` pins the new runtime bytes and
`native-order.log.gz` retains the complete output. Added assertions in the real
HTTP SQL fixture cover the adapter selection for scheduled reads, serial writes
and unscheduled callers, but those assertions await the full API gate. They are
not covered by this provider-only result.

Scheduled read provider runtimes now borrow the adapter checkpoint. It is
called during preflight, before/after provider invocation, and before result
normalization. Attaching it forces serial provider execution even with the
parallel default enabled. The native fixture verifies parent-thread confinement
and rejects a result when the parent stops after invocation; the second provider
is never invoked. All 24 selected native tests and 47 build steps pass.
`native-checkpoint.json` pins candidate sources, with the complete output in
`native-checkpoint.log.gz`. The full SQL API and Linux gates remain pending;
provider I/O still holds its grant and backend inference has no new admission.

The same provider fixture now also borrows a real scheduler execution. With
a one-checkpoint quantum, it verifies successful grant reacquisition, parent
operation/byte retention, and protected control grant entry during invocation.
Closing the scheduler after provider completion stops result consumption and
prevents the second invocation. Explicit retirement returns slots, operations
and bytes to zero. `native-scheduler.json` pins this candidate and
`native-scheduler.log.gz` records all 47 build steps and 24 passing native tests.
This is a provider/runtime contract, not a full SQL control or load test.
