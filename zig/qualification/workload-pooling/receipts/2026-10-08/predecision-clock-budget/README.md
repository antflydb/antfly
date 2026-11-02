# Pre-decision clock budget

Base `12f0b5590f`. PreDecisionContext.boundDeadline retains the original
ceiling while translating it into an attempt's clock epoch. It preserves
restore identity, route fences and the semantic cancellation token.

The compiled table-write callback gate passes all 5 tests on native ARM64 macOS
and pinned Nix/glibc ARM64 Linux. Native command from `zig`:
`zig build antfly-table-write-execution-contract-test -Doptimize=ReleaseSafe -j1
--maxrss 2147483648 --cache-dir /private/tmp/antfly-parent-phase-focused-cache
--summary all`. Linux exact argv, source hashes and exit 0 are retained.
The new clock test makes the source clock read consume destination time; that
latency cannot extend the caller's budget. It also checks tighter attempt
ceilings, expired source budgets, cancellation and binary restore identity.
Initial tests incorrectly expected `Cancelled` rather than the existing
semantic token's `Canceled`; both failed outputs are retained.

HostedParticipantWorker shared-deadline integration and its dedicated actual
worker test are proposed in the working tree and need production qualification.
This receipt qualifies the deadline-clamping contract, not those uncompiled
production changes, physical WAL/Raft waits, SQL ingress deadline propagation,
or default enablement.
