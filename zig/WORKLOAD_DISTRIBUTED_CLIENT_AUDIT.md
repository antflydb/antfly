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
