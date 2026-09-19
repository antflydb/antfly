# Distributed and client-contract audit

This inventory tracks item 8 of the scheduling completion requirements. It is
not whole-system qualification, and the SDK regressions below do not establish
replicated completion, process-wide progress, or all distributed cancellation
interleavings. The starting implementation was revision `3dc3ac6c4d`.

## Authoritative implementation inventory

| Contract | Implementation and existing evidence | Remaining qualification |
| --- | --- | --- |
| Query retry eligibility and unknown writes | `go/pkg/sdk/read_retries.go`, `py/packages/sdk/src/antfly/read_retries.py`, `ts/packages/sdk/src/read-retries.ts`, `rs/crates/sdk/src/read_retries.rs`. Automatic retries require query POST plus explicit admission-stage, execution-not-started 429 evidence. Arbitrary writes and transport failures do not qualify. | Cross-language malformed/rolling-version response matrix; public frontend mutation error/retry presentation audit. Existing unit tests are not evidence of real lost-write recovery. |
| Original client deadline through response consumption | Go retains the original context until response-body retirement; Rust sets remaining total request timeout after local admission. Python's synchronous contract explicitly caps individual I/O waits, not arbitrary synchronous streaming duration. | Go/Rust current-tree runtime tests were inspected, not rerun in this slice. Synchronous Python cannot interrupt arbitrary custom blocking transports; its documented restriction remains. |
| Local admission, cancellation and stream cleanup | Admission modules in each SDK hold permits through response lifetime. Python/TS deadline and cleanup fixes below add missing stream interleavings. | Broader custom transport shutdown, connection failures, idle abandoned responses, and every generated SDK endpoint are not qualified here. |
| Durable remote attempt ownership | `api/workload_attempt_coordinator.zig`, `api/workload_attempt_worker.zig`, `api/workload_attempt_protocol.zig`, `api/http_client.zig`, `api/workload_coordinator_runtime.zig`. Existing tests cover lost terminal responses, signed identity/digest checks, worker restart, generation fences and durable uncertainty. | Partial fan-out with mixed terminal/unknown outcomes, cancellation during each ownership transition, shutdown with late replies, and mixed-version cluster runs remain separate tasks. Do not repeat prior response-loss fixtures as a substitute. |
| Transaction coordinator recovery handoffs | `api/distributed_txn.zig`: existing tests include same-ID ambiguous decision retry, bounded deadline, participant fan-out, attempted-participant abort, topology change and no abort after durable commit. `api/transactions.zig` retains recovery through coordinator acknowledgement. | Audit each accepted-to-recovery ownership transfer against newly protected ordinary/control records. Production replicated fault matrix belongs to the overall completion work. |
| Version negotiation and fallback | Attempt protocol validates signed versions/identity; DATA physical completion requires canonical protocol support rather than logical fallback. | Real rolling versions, unsupported worker behavior, discovery cancellation and no downgrade across all endpoint variants are not established by SDK tests. |

## SDK deadline and cleanup slice

Python async retries previously ended their timeout scope at response headers.
A delayed streamed success or a paused consumer could exceed the original
operation deadline. Returned streams now retain that deadline, reject late
chunks, and close the transport exactly once. Repeated task cancellation may
return before cleanup, while the underlying admission permit remains owned
until transport closure completes. The synchronous Python limitation above is
unchanged.

TypeScript previously accepted late successful headers from a custom fetch
implementation that ignored cancellation. It could also stall indefinitely
while inspecting the bounded clone of a 429 body. The retry wrapper now rejects
late replies and cancels both response tee branches together. Concurrent abort,
read-error and consumer cancellation exposed a separate admission bug: a second
`reader.cancel()` resolved before the first transport cleanup, returning the
permit too early. All cancellation paths now join one cleanup promise; a
pending read becoming EOF during cancellation cannot bypass that promise.

Validation (actual exit 0):

- Python retry/admission: 25 passed; `/tmp/workload-sdk-python-stream-deadline.log`.
  Command: `uv run --project py/packages/sdk pytest py/packages/sdk/tests/test_read_retries.py py/packages/sdk/tests/test_admission.py -q`.
- TypeScript retry/admission: 22 passed; `/tmp/workload-sdk-ts-deadline.log`.
  Command: `pnpm --dir ts/packages/sdk exec vitest run test/read-retries.test.ts test/admission.test.ts --maxWorkers=1`.
- TypeScript SDK typecheck and scoped Python Ruff checks passed. Scoped Biome
  formatting was applied; preexisting non-null assertion warnings remain.

The new Python tests failed before the fix (2 failed, 1 passed). The initial
TypeScript late-reply/stalled-clone tests failed before the fix (2 failed,
11 passed); the composed cleanup test independently reproduced premature permit
release before the shared-cancellation fix. No new automatic write retry or
fallback behavior was added.

## Late terminal response boundary

`ApiHttpClient.executeCoordinatedRead` now verifies signed terminal evidence and
retires its durable attempt before checking caller cancellation and the original
deadline. A late result is freed and returned as cancellation/deadline failure;
the terminal proof still closes the remote obligation. An unsigned late response
remains uncertain and charged. Expiry alone never retires a remote attempt.

The owning `antfly-api-test` gate passed 1/1, no skips/failures/leaks, actual
exit 0. Its regression covers signed and unsigned responses after both transport-
triggered cancellation and deadline expiry, with one dispatch in every case.
Log: `/tmp/workload-coordinator-late-terminal.log`. Reproduce from `zig/`:

```sh
zig build antfly-api-test -j1 --cache-dir /tmp/zig-local-cache \
  --global-cache-dir /tmp/zig-global-cache -- \
  --test-filter 'workload admission coordinator late terminal'
```

The audit also found that DATA HTTP/coordinator teardown preceded draining native
DB callback users. This was handed to the runtime owner, who is implementing and
testing a separate shutdown-order fix. It is not qualified by this API test.
Existing join fan-out tests cover bounded concurrency, partial worker error,
complete drain and cancellation before launch; inflight cancellation with mixed
signed-terminal/unknown results still needs broader qualification.

## Transaction retry and recovery handoffs

A stable-ID retry can observe an authoritative committed coordinator record and
then receive a conflicting, missing, or malformed response to its resolution
retry. The coordinator previously entered abort cleanup in those branches.
The same problem occurred after a transport failure followed by a contradictory
aborted status. Committed evidence now remains authoritative: these inconsistent
replies stop further dispatch and leave propagation pending. Callers configured
to report post-commit failures receive `CommitPropagationIncomplete`; callers
requesting structured outcomes retain `committed` with `propagation_pending`.
No abort or follower acknowledgement is authorized by the later reply.

The existing stable-ID regression now exercises these four failure schedules
under both reporting policies, asserting one coordinator resolve, no abort,
no re-prepare, and no follower resolve. Its exact owning API inventory filter is
`stable distributed transaction retry resumes a durable commit decision`.

The broader module inventory remains distinct from this regression:

| Schedule | Source coverage | Still required |
| --- | --- | --- |
| Pre-decision transport failure and replica rediscovery | `hosted participant rediscovery retries only pre-decision leader unavailability` distinguishes proven not-sent/not-proposed from unknown timeout/reset and checks forged/legacy response handling. | Real peers across mixed versions, shutdown during dispatch, delayed server queue followed by abort recovery. |
| Ambiguous coordinator commit | Same-ID retry, bounded unresolved retry, and one absolute recovery deadline tests; no new transaction ID or opposite decision after uncertainty. | Process loss between status, retry, durable participant resolution, and acknowledgement; real leader replacement. |
| Partial participant fanout | Bounded concurrency, attempted-participant contact mask, durable abort before acknowledgement of untouched participants. Submitted tasks are joined before their arenas and slot arrays are released, including I/O cancellation. | Inflight cancellation/shutdown in each fanout phase, delayed replies during cleanup, and native ownership transfer under resource exhaustion. |
| Post-commit visibility and acknowledgement | `distributed txn coordinator never aborts after durable commit decision` covers pending visibility, terminal repair, propagation and acknowledgement errors. | Retained coordinator/participant records across actual restart and all new protected record kinds. |
| End-to-end caller deadline | Hosted begin/prepare each preserve one deadline across replica attempts; ambiguous-decision recovery uses a separate bounded deadline and deliberately ignores client cancellation. | `ExecuteOptions` has no transaction-wide original deadline. Later participant waves receive fresh operation budgets; abort/ack cleanup lacks one shared absolute deadline. This slice does not claim whole-request deadline propagation. |

The source inventory above is not a claim that all existing tests were rerun.
Most transaction tests are not in the curated `antfly-api-test` compile inventory;
passing only a runtime filter without adding the owning compile filter can select
zero tests. The exact new regression is included explicitly. Broader qualification
must first establish test discovery, then run the named schedules and real fault
fixtures; item 8 remains open.

Validation of the monotonic-decision fix: owning API gate 1/1 passed, 0 skipped,
0 failed, 0 leaks, actual exit 0. This one test includes the existing successful
resume and all eight injected failure/reporting-policy combinations. Log:
`/tmp/workload-transaction-monotonic-decision2.log`. Reproduce from `zig/`:

```sh
zig build antfly-api-test -j1 --cache-dir /tmp/zig-local-cache \
  --global-cache-dir /tmp/zig-global-cache -- \
  --test-filter 'stable distributed transaction retry resumes a durable commit decision'
```

The initial gate exited 1 before running tests because of a native callback
return-type integration error and a test-local optional-error comparison. Both
were corrected before the successful owning gate; this was not a before-fix
runtime reproduction. The separate runtime owner's subsequent DATA gate also
passed 6/6, including the retained-owner shutdown ordering regression
(`/tmp/workload-completion-capsule-data1.log`); that evidence remains narrower
than the multi-peer shutdown schedules listed above.
