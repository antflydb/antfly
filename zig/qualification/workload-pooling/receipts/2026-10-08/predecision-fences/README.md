# Pre-decision contexts cannot fall back without their fences

TableWriteSource BEGIN/PREPARE previously discarded PreDecisionContext whenever
its context-aware callback was absent. The reproduced direct regression supplied
an absolute deadline and observed legacy success instead of UnsupportedOperation:
three tests passed, one failed. Its source and log are preserved.

PreDecisionContext.requiresAwareCallback now identifies deadline/clock,
cancellation, route-fence and hidden restore-owner fields. Supplied inactive or
incomplete cancellation tokens still require the context-aware contract. BEGIN
and PREPARE reject a missing aware callback rather than calling a legacy callback
which cannot carry these fields. Empty contexts retain legacy behavior. Existing
PREPARE route-fence error classifications remain unchanged. Capable callbacks
receive the original context unchanged, including exact cancellation/checkpoint
errors across direct and compiled dispatch. No new wire field or layout is added.

All 41 focused boundary/task/SQL-team/owner/worker/public-cleanup regressions
execute and pass on native macOS and pinned Nix/glibc GNU AArch64, no skips. The
four boundary contracts include the new fallback regression, earlier execution
team forwarding, cancellation/committed error forwarding and discovery. The new
regression verifies zero legacy calls for supplied deadline/clock/token/restore
context, empty-context compatibility, 36 aware callback invocations, exact
fallible token errors and cancellation forwarding under both dispatch paths.
These tests are callback contracts, not physical storage wait qualification.

A shared completion deadline is not yet threaded through the complete coordinator
and participant storage/quorum waits. Task handoff before SQL sealing, physical
wait suspension, ambiguous-outcome joins and authoritative recovery transfer
remain incomplete. Full API/archive/CLI and batched compiler-memory qualification
are pending. No production consumer activates completion; pooling stays disabled
by default. The supplemental production proposal is relative to 2242373c86 plus
this commit's context and boundary changes and contains both process fixture files.
