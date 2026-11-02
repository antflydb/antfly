# Io mutex wait handoff and worker-client wiring

The shared InferenceExecutionControl now has a cancellation-aware Io mutex helper. It parks the owning synchronous scope only on contention, continues original control checks while waiting, and reacquires the original grant before returning a held lock. Failed reacquisition or post-lock checks unlock the mutex. Error exits balance the resource wait, with resumption failure taking precedence.

Worker Client.invokePayload uses this helper for its startup/replacement/lifetime mutex. Waiting precedes that caller's dispatch. The worker lifetime mutex is released before callSegments, and the caller's grant stays held throughout its active child RPC. This change does not park a parent while its child computes without a physical admission handshake.

Native macOS and pinned Nix/glibc GNU AArch64 pass all 13 real-owner test functions. The new Io-mutex fixture covers successful weighted reacquisition, original cancellation and native deadline expiry, unchanged retained bytes while parked, query/protected-control progress, unlocked gates on failure, and final operation/CPU/byte drain. GNU uses two CPUs and 3 GiB without extra swap. Exact commands, six source hashes and full logs are retained.

Scope limitation: the focused owner gate compiles and runs lockIo, not host/worker.zig. That worker file is hashed for the proposed wiring but its body still requires production/real subprocess qualification. The current production installation build uses earlier immutable sources and does not qualify this change.

The RPC endpoint permits up to 128 outstanding requests; there is no request-serialization mutex to park around. Physical child resource waits, payload-offer waits, complete backend/memory accounting, numerical/fairness and default qualification remain required.
