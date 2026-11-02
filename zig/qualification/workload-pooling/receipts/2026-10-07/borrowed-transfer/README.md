# Borrowed synchronous grant transfer

A borrowed single-worker execution can transfer the parent's existing operation
into a child class and explicitly return to its original class through checked
C callbacks. It carries the admitted CPU slot demand. It neither starts a second
operation nor owns parent retirement; a borrowed value must not outlive its call
or be shared by parallel children. The owning caller supplies the trusted slot
and class metadata; its enter callback must enforce mandatory resumption fences.

All seven real capsule/transfer tests and 34 protocol/imported ABI tests pass
on macOS and pinned Nix/glibc GNU AArch64 (41 per platform). Linux uses two CPUs
and 1 GiB memory without extra swap. The real scheduler admits a two-slot query,
transfers it to inference at full ordinary capacity, allows protected control,
and returns to query without increasing operation count or retained bytes.
Malformed headers, zero demand, stale deadlines, shutdown, and repeated return
are checked. A failed reacquisition can retire its scheduler entry; the original
caller still owns the capsule and its node lease until finish. Tests verify
refused node destruction and zero charges after final caller cleanup.

An initial test incorrectly required the scheduler operation count to remain
one after an expired resumption; that assertion failed and test cleanup leaked
the capsule. The corrected test checks the surviving caller-owned node lease
and adds failure cleanup. The production primitive was unchanged by that fix.

This is a qualified transfer primitive, not an integrated inference scheduler.
SQL/inference invocation propagation, enforcement of the backend's admitted CPU
team demand, cross-process worker execution and full production gates remain
pending. Existing defaults remain disabled.

From the worktree root, with ZIG set to the native or pinned GNU compiler and
-target aarch64-linux-gnu added on Linux:

```
ZIG test -Osafe --dep antfly_local_sources --dep antfly_runtime_abi -Mroot=zig/pkg/antfly/src/api/runnable_owner_execution.zig --dep antfly_local_test_sources -Mantfly_local_sources=zig/pkg/antfly-embedded/src/source_catalog_control.zig -Mantfly_runtime_abi=zig/lib/runtime/src/root.zig -Mantfly_local_test_sources=zig/qualification/workload-pooling/receipts/2026-10-07/execution-port/test-sources.zig
ZIG test zig/lib/runtime/src/runtime_runnable_abi.zig -Osafe
```
