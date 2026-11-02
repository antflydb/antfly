# Acknowledged worker execution phases

The ReleaseSafe `antfly-inference-worker-rpc-test` passed the actual pipe RPC
suite (21 tests) and private worker descriptor suite (5 tests) on native macOS
and pinned Nix/glibc aarch64 Linux. Native RPC execution in the combined result
was cached; `native-rpc-uncached.log.gz` preserves its successful uncached run.
Linux executed all 26 tests. Source hashes, commands, and complete logs are kept.

The new transport uses fixed nine-byte request/reply frames and one registered
phase slot per live request. No ordinary payload credit is required to park or
acknowledge reacquisition. Sequence numbers reject duplicated/stale live replies;
phase actions execute on the original RPC caller's OS thread. Tests exercise
full ordinary control/bulk counters, successful park/reenter acknowledgment,
Forbidden/Canceled/DeadlineExceeded rejection before child computation, missing
owners, and healthy subsequent calls. Protocol version 7 rejects legacy peers
at initialization. Only scalar demand and a handoff flag cross the wire; borrowed
scheduler callback pointers remain in the parent process.

The worker caller/child wiring in `worker.zig` is implemented but **not compiled
or exercised by this gate**. Its hash in the initial gate result is historical
proposal provenance; `proposed-worker-sha256.json` records the subsequent wiring
including caller authority checks. Full production compilation, actual worker
contention with a real scheduler capsule, cancellation/lost-reply cleanup, and
combined JSON-operation integration remain pending. These receipts establish
transport/descriptor contracts, not physical backend or whole-node qualification.
Defaults remain disabled.
