# Original commit controls across the private callback boundary

Base `9d0aba2cb8`. CommitControl carries the invocation-owned team and original
pre-decision deadline, clock and cancellation. The separate existing callback
argument retains post-commit visibility cancellation. RequestContext's optional
execution_team field and CommitControl.fromRequest preserve that borrowed team.

The new batch and stable-ID commit callbacks pass the exact control packet
through direct and foreign dispatch. Missing capabilities reject supplied
controls with UnsupportedOperation before any legacy callback runs; an empty
packet retains legacy compatibility. Capability inspection has the same rules.
Tests distinguish an already-canceled post-commit source from the active original
pre-decision token, and preserve a fallible pre-decision fence's exact error.

All 6 compiled callback tests pass on native ARM64 macOS and pinned Nix/glibc
ARM64 Linux. From `zig`, native argv is `zig build
antfly-table-write-execution-contract-test -Doptimize=ReleaseSafe -j1 --maxrss
2147483648 --cache-dir /private/tmp/antfly-parent-phase-focused-cache --summary
all`. Linux argv and terminal exit 0 are retained with logs and source hashes.
The new test forwards 36 controlled calls and verifies only four empty-control
legacy calls. Supplied teams, deadlines, clock-only controls, cancellation and
restore identity never take that fallback.

Hosted-source wiring is a separate working-tree proposal: it forwards original
budgets to the shared worker, freezes one default ceiling for all stateless
attempts, and checks the original budget before retry contact. Its no-skip
production gate is registered, but graph discovery is not execution proof.
Provisioned/bound storage, SQL ingress adoption, physical durability waits,
full API/production gates, and default enablement remain pending.
