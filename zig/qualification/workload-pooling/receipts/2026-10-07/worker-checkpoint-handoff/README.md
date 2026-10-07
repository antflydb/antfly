# Checkpoint at the physical child boundary

The real owner/pipe gate passed on native macOS and pinned Nix/glibc Linux in
ReleaseSafe. One selected test covers success, original cancellation, original
deadline, and a scheduler quantum crossed during simulated native work.
Commands, exact source hashes, and logs are preserved.

A scheduler checkpoint may yield its entry. Calling it from an RPC polling
parent can therefore release a CPU loan while the child still computes. The
production parent phase helper now polls only cancellation/deadline. The child
sends an acknowledged checkpoint at its cooperative execution-control boundary;
the owning parent invokes the real scheduler checkpoint while that child waits.
Checkpoint requests reject missing owner scopes and parked grants. Private
worker protocol version 8 rejects peers lacking this phase during initialization.

The quantum case advances the real capsule's clock beyond its two-millisecond
quantum while the pipe child models a non-yielding native primitive. Repeated
parent polling must keep both slots charged and the yield counter at zero.
The next child control check crosses the actual RPC and causes exactly one real
scheduler yield before acknowledging resumed computation. Existing weighted
parking, retained-charge, query/control progress, cancellation/deadline, join,
and complete retirement checks run in the same fixture.

This gate executes the production parent/child phase factory and actual pipe
transport with a real scheduler capsule. It does not compile `worker.zig` or
`worker_wire.zig`, nor execute an OS subprocess or real native model/team.
Those recorded hashes are proposed wiring/descriptor provenance. Full production
compilation, real-model safe-boundary contracts,
checkpoint RPC overhead measurement, and broader overload/default qualification
remain pending. Defaults stay disabled.

The separate `antfly-inference-worker-rpc-test` also passed all 28 tests on both
platforms: 23 actual pipe transport contracts and 5 private descriptor contracts.
`transport-*-result.json` and logs preserve this evidence, including compilation
of the version-8 descriptor and updated phase enum. This gate still excludes
the worker caller/child implementation and real OS subprocesses.
