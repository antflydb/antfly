# Real owner through worker phase pipes

Native macOS and pinned Nix/glibc Linux ReleaseSafe gates passed
`antfly-inference-worker-owner-handoff-test`. One selected integration test runs
three modes: success, original cancellation, and original native-clock deadline.
Native combined execution was cached; the uncached log is also preserved.
Commands, source hashes, and complete output are included.

The fixture starts a real API-kernel owner capsule with a weighted two-slot
inference grant and retained payload bytes. An actual pipe RPC child hits a
contended atomic model gate using the production execution-control helper.
Acknowledged phase frames run the production parent/child phase factory on the
owning caller/child OS threads. Parent parking releases runnable slots from two
to zero, leaves the complete retained charge unchanged (including capsule
bookkeeping), and allows both an ordinary query and protected control operation
to start and finish. Successful reentry restores two slots before the child
computes. Cancellation/deadline rejection prevents computation; cancellation
cleanup joins the actual fixture handler before the original capsule retires.
Operations, runnable slots, and retained bytes all drain to zero.

The production worker now uses the same injected `worker_execution_phase.zig`
factory, preserving its existing backend checkpoint TLS. This gate compiles and
executes that factory, the real owner, and the actual RPC implementation; it
**does not compile `worker.zig` or launch an OS subprocess/native model**. That
file's recorded hash identifies the proposed caller wiring. Production compile,
real subprocess contention/reaping, lost-ack quarantine, owning response cleanup
through this complete chain, and combined JSON-operation integration remain
required. Defaults remain disabled.
