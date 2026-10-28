# Local inference parent phase (final native API gate passed)

Source base: `fd0406adcd`. The pinned source includes SQL borrowed-grant callbacks,
RequestContext/provider bridge propagation, ABI v29, host class transfer and CPU
scopes, authority-return cleanup, and a non-fan-out invocation executor.

Native macOS and pinned Nix/glibc GNU AArch64 both pass the focused host test:

```sh
zig build antfly-inference-parent-phase-test -Doptimize=ReleaseSafe -j1 \
  --maxrss 4294967296 --cache-dir <cache> --summary all
```

Native uses `/Users/ajroetker/bin/zig-0.17.0/zig`, global cache
`/private/tmp/antfly-default-pooling-global`, local cache
`/private/tmp/antfly-parent-phase-focused-cache`. The Linux command additionally
passes `-Dtarget=aarch64-linux-gnu`; it uses pinned image
`sha256:fb3236b6cc7411028d62f76eb418110a6c34e246b9437a9d093d58a53a3e2954`,
entrypoint `/opt/zig-nix/bin/zig`, two CPUs, 4 GiB and no extra swap. Source is
read-only at `/work`, cache at `/cache`, global cache at `/global`. The host
package symlink's target is also mounted read-only at its absolute path.
Both commands exited 0. Full logs explicitly show one passed test and no
skips, failures or leaks; native has 13 build steps and Linux 10.

The host test verifies inference-class execution, the actual native CPU scope,
parent restoration, and disposal of numeric results after failed authority
resumption. Four task jobs use the invocation executor and assert the same
OS thread and CPU scope. This keeps async rewriting work from escaping a
borrowed worker's scope. The real SQL adapter integration test also validates
and transfers the descriptor supplied by decisionRuntime.

The first full native API run passed all 50 build steps and 87 tests (39 local,
48 coordinator), exit 0 and no skips/leaks. Its source predates the added
non-fan-out executor; its logs are preserved as intermediate evidence only.
The final combined source passes the full native API gate: all 50 build steps and
87 tests (39 local, 48 coordinator), no skips/failures/leaks, exit 0. Source
hashes were rechecked against the completed gate metadata. An earlier attempt
ended with both processes absent and no result; its metadata is preserved.
The unchanged-source rerun passed. This qualifies ABI v29 local-parent source,
not the separate ABI v30 worker-protocol candidate.

## Remaining scope

This wires synchronous local SQL decision calls without a nested admission or
new deadline. Borrowed callbacks never enter watchdog controls. The descriptor
is process-local: borrowed subprocess calls currently fail UnsupportedOperation
before class transfer, pending worker-protocol CPU budget handoff. Independently
admitted inference HTTP work, fused-cohort waiting, broader backend teams,
checkpoint quanta, full retained-memory accounting, production archive/runtime
gates and default overload qualification remain required. The independent
broker owner provider remains uninstalled. Defaults remain disabled.
