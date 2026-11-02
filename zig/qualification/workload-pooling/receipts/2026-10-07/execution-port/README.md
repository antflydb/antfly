# Shared execution port contracts

The API owner supplies checked C callbacks for weighted execution, checkpoints,
resource/output parking, class changes, and finish. Each capsule retains the
node owner until its scheduler entry and executor receivers are retired. Stored
consumer ports require an explicit retain/release around their joined lifetime.

Native and pinned Nix/glibc GNU AArch64 contract tests pass: five real scheduler
capsule tests, 33 protocol/imported ABI tests, and three API prefix/layout tests
and two inference ABI tests on each OS (43 total). Linux containers use two CPUs and 1 GiB memory without extra swap.
The direct capsule tests select the production common_request_admission catalog
entry through test-sources.zig; they do not compile the API export table.

Covered: weighted resumption, retained memory through resource/output waits,
protected control progress, refused destruction while borrowed or parked,
shutdown and original deadline/cancellation rejection, invalid executor borrows,
impossible demand, allocation failure rollback, and optional table prefix checks.

The complete native API gate passes all 50 build steps and 84 tests (38
embedded, 46 coordinator), with no skips or leaks. It covers actual factory,
getter and wrapper behavior alongside the execution callbacks. The initial
restricted run returned Unexpected in two fixtures and skipped one; that full
log/source snapshot is preserved. The unchanged authorized rerun passes all
fixtures. Inference configure now retains an optional stored owner lease,
releasing it after worker and node destruction; the configure layout is version
28. Actual configure/destroy integration still needs its full gate, and storing
a lease does not yet admit inference execution. These receipts do not prove production archive linkage, inference
or background integration, CPU team enforcement, default sizing, or overload
behavior. The separate frozen f9e1e0c37d GNU production build uses older source.

Commands from the worktree root (ZIG denotes the native or pinned GNU compiler;
add -target aarch64-linux-gnu for Linux):

```
ZIG test -Osafe --dep antfly_local_sources --dep antfly_runtime_abi -Mroot=zig/pkg/antfly/src/api/runnable_owner_execution.zig --dep antfly_local_test_sources -Mantfly_local_sources=zig/pkg/antfly-embedded/src/source_catalog_control.zig -Mantfly_runtime_abi=zig/lib/runtime/src/root.zig -Mantfly_local_test_sources=zig/qualification/workload-pooling/receipts/2026-10-07/execution-port/test-sources.zig
ZIG test zig/lib/runtime/src/runtime_runnable_abi.zig -Osafe
ZIG test -Osafe --dep antfly_runtime_abi --dep antfly_image -Mroot=zig/pkg/inference/src/host/bridge.zig -Mantfly_runtime_abi=zig/lib/runtime/src/root.zig -Mantfly_image=zig/lib/image/src/mod.zig
ZIG test -Osafe --dep antfly_runtime_abi --dep runtime_memory_abi -Mroot=zig/pkg/antfly/src/api/kernel_abi.zig -Mantfly_runtime_abi=zig/lib/runtime/src/root.zig -Mruntime_memory_abi=zig/pkg/antfly-embedded/src/runtime_memory_abi.zig
```
