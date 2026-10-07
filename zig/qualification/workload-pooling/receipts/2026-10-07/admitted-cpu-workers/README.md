# Admitted native CPU workers

An optional synchronous WorkerScope limits native linalg CPU fanout to an
admitted worker count. Wider nested scopes cannot expand it. Explicit Io jobs
run in bounded waves and each child gets a one-worker scope, preventing nested
kernels from multiplying the team. Sync dispatch respects the same bound; a
one-worker scope avoids initializing the process pool. Process CPU capacity and
shared library initialization stay independent of the first scoped caller.
Scopes remain on the same OS thread until deinit and restore previous limits.
They are not a general task-local context for migrating fiber runtimes.

The native backend bypasses system BLAS while scoped because its CPU team has
no verified bound. Optional OpenBLAS is used only when its measured process
team fits the scope; otherwise existing native kernels run. OpenBLAS initializes
from process capacity, preserving unscoped behavior. Focused backend checks use
the explicitly recorded enable_system_blas=false/true test configurations.
The macOS true configuration links the real Accelerate SDK framework and checks
that a scoped call uses the bounded native path. Loaded OpenBLAS multithreaded
execution is not separately qualified by these receipts.

Native scope/dispatch and matrix-reference checks pass (four tests), alongside
both backend configurations (one each) and all eight existing unscoped matrix
regressions. Pinned Nix/glibc GNU AArch64 passes the same four scope/matrix
checks, its actual native backend check, all eight matrix regressions and all
nine pool/CPU-budget tests. Linux uses two CPUs and 1 GiB without extra swap.
The first broad Linux run passed matrices and eight pool tests but its cgroup
fixture could not create a temporary directory under a read-only cwd. That log
is retained; the unchanged complete passing run uses writable /cache as cwd.

This qualifies the native CPU team primitive and numerical behavior, not whole
inference scheduling. Production callers do not yet install scopes. Parent
propagation, microbatch/broker grants, worker-process budgets, other backends,
whole-production gates and defaults remain required.

Commands from worktree root; add -target aarch64-linux-gnu on GNU runs:

```
ZIG test zig/lib/linalg/src/mod.zig -Osafe --test-filter 'admitted worker scope'
ZIG test zig/lib/linalg/src/mod.zig -Osafe --test-filter sgemmTransB
ZIG test zig/lib/linalg/src/pool.zig -Osafe
ZIG test -Osafe --test-filter 'admitted worker scope' --dep build_options --dep inference_linalg -Mroot=zig/pkg/inference/src/backends/native.zig -Mbuild_options=zig/qualification/workload-pooling/receipts/2026-10-07/admitted-cpu-workers/options-no-system-blas.zig -Minference_linalg=zig/lib/linalg/src/mod.zig
```

GNU backend tests link libc (-lc). The macOS system-BLAS variant uses
options-system-blas.zig and -lc -framework Accelerate -F /System/Library/Frameworks
-L /usr/lib --sysroot /Library/Developer/CommandLineTools/SDKs/MacOSX.sdk.
The complete pool suite is qualified on GNU; macOS uses the focused pool checks
through the matrix module because its existing Linux-only SyncPool test owns a
Linux executor.
