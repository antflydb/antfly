# Retained SQL page policy validation

PG credential identity now uses the atomic current-policy snapshot and carries
persistent user identity. Catalog identity cloning preserves that identity.
Retained reads check current policy both before and after page construction,
rejecting a changed row filter or a replaced account before exposing page rows.
Their original snapshot deadline remains unchanged. This page boundary is
additional protection; it does not replace per-resume backend fences.

Native macOS ReleaseSafe qualification with Zig 0.17.0 passes all 33 local and
38 API pooling tests, without skips or leaks, and all 50 build steps. The first
sandboxed execution failed two localhost socket fixtures; the rerun with socket
access passed. Both logs are preserved.

The focused `antfly-read-authority-test` target executes the one named PG
credential fixture, including filter changes, restored policy, preserved cloned
instance identity, replacement rejection, permission revocation, and password
rotation. It passes on native macOS (50 build steps) and Nix/glibc GNU AArch64
(47 build steps, two CPUs, 4 GiB memory with no extra swap), without skipped
fixtures or leaks. Source hashes and complete compressed logs are adjacent.

The shared per-resume policy/catalog fence is under implementation in an
isolated worktree and is not qualified by these page-boundary results. General
storage/RPC wait suspension, independent parallel-child ownership, all-workload
integration, distributed attempts, and automatic default qualification remain
required. The full design remains active.
