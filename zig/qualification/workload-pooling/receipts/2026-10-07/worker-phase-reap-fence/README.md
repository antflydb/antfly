# Reaping before borrowed capacity retirement

Native macOS and pinned Nix/glibc Linux ReleaseSafe gates passed all 28 tests:
23 actual pipe RPC contracts and 5 private worker descriptors. Commands, exact
source hashes, and logs are preserved. The gate compiles `worker_rpc.zig` and
`worker_wire.zig`; it does not compile the worker caller/child wiring.

`Control.fence_on_error` keeps a dispatched borrowed call alive until the
endpoint owner's `on_closed` callback returns. The real worker callback kills
and reaps its child before releasing resources. A dedicated event exposes that
completion fence; cancellation cannot interrupt the owning caller's wait.
Requests rejected before their finish/dispatch frame preserve a healthy peer.
Failed phase-ack writes also fence the peer, and a closed endpoint cannot emit a
successful reacquisition acknowledgment.

The new tests delay the simulated reaper after both cancellation and lost
reacquisition acknowledgment. The original caller remains blocked with its
ownership marker held until teardown completes; the child cannot compute after
the lost acknowledgment. A separate saturated-credit test proves a rejected
offer neither dispatches a handler nor tears down a healthy transport. These
are actual RPC tests with a simulated process-reaping callback, not a real
scheduler capsule or operating-system subprocess qualification.

`worker.zig` enables the fence for borrowed provider calls. That wiring still
requires production compilation and actual subprocess/scheduler cancellation
and contention tests. A stuck OS process can keep this reap wait blocked;
quarantining an independently retained execution capsule is still required to
prove bounded user waiting in that case. Other concurrent calls in a failed
worker may be interrupted when the supervisor reaps it. Defaults remain off.
