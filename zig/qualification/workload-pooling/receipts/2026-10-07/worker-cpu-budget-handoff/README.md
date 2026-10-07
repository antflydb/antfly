# Worker CPU budget handoff (focused gates passed)

This candidate builds on the local parent-phase candidate from `fd0406adcd`.
Provider ABI v30 and private worker protocol v6 carry an admitted CPU count.
Root borrowed callbacks stay process-local; the parent retains the original
operation and transfers its class while invoking the worker. The worker validates
metadata before parsing attachments and installs its own native CPU scope and
non-fan-out executor. It neither starts nor retires a parent scheduler entry.
Conflicting borrowed/delegated demand, out-of-range demand and worker relays are
rejected. Original deadline and cancellation continue through existing controls.

Both focused gates exited 0 on final pinned source: 15 native build steps and
12 GNU steps, each with 2 host tests and 5 worker-wire tests; no skips, failures
or leaks. Full logs and source hashes are beside this receipt.

```sh
zig build antfly-inference-parent-phase-test -Doptimize=ReleaseSafe -j1 \
  --maxrss 4294967296 --cache-dir <cache> --summary all
```

Native compiler: `/Users/ajroetker/bin/zig-0.17.0/zig`, global cache
`/private/tmp/antfly-default-pooling-global`, cache
`/private/tmp/antfly-parent-phase-focused-cache`, cwd design worktree `zig/`.
GNU adds `-Dtarget=aarch64-linux-gnu`. Pinned image:
`sha256:fb3236b6cc7411028d62f76eb418110a6c34e246b9437a9d093d58a53a3e2954`,
entry `/opt/zig-nix/bin/zig`, two CPUs, 4 GiB, no extra swap. Source is read-only
at `/work`, cache `/cache`, global `/global`; the host absolute package target
is also mounted read-only to resolve the package symlink.

Host tests check local phase/authority cleanup and delegated scope/failed-result
cleanup. Wire tests exercise JSON demand roundtrip without callbacks, preserved
deadline, malformed and conflicting demand, and existing attachment/numeric
ownership/allocation failures. These gates do not run an actual worker process
or compile the complete production worker dispatcher/archive. Real process,
full ABI/API/production and overload qualification are required next. The
separate ABI v29 local-parent candidate's full API gate is running in the original
worktree; that result cannot qualify these ABI v30 worker bytes.

The independent broker owner adapter remains uninstalled; native scopes alone
do not establish broader foreign-backend team bounds, physical cohort fairness,
resource waiting, or complete retained-memory accounting. Defaults remain disabled.
