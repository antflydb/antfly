# Stopped capsules preserve their original failure

Published executions consult their stopped entry's recorded cause before later
checkpoint, park or enter operations. A failed resume cannot be resurrected or
turn original cancellation/deadline/queue timeout into InvalidRunnableTransition.
Checkpoint state and terminal failure are validated under a single scheduler
lock. Reservations and resume generation remain unchanged until owning finish. A later
cleared cancellation token or scheduler shutdown does not replace the first cause.

Native macOS and pinned Nix/glibc GNU AArch64 pass all 26 scheduler/admission
contracts. The strengthened denied-reentry regression covers all three original
causes through repeated phase calls and subsequent shutdown. An isolated copy of
commit 068594beba with that strengthened test fails exactly as expected: Canceled
becomes InvalidRunnableTransition. Source copies, hashes and failure are retained.

Real owner, pipe handoff and actual subprocess/socket gates pass on both platforms:
native 16/16 build steps, GNU 15/15, with 16/16 executed tests each; unchanged
RPC/descriptor contracts are reused from cache. The subprocess fixture retains
its complete reservation before and after actual reaping across H1/H2 original
cancellation/deadline schedules. The supplemental fixture and ABI 33 dependencies
are replayable from ../retained-resume-reap/supplemental-process-fixture.patch.gz.
Production worker/standalone hashes remain provenance only for these focused gates.

Only the two verified idle dispatchers for this worktree's pending production
revision were retired before changing their captured source. Both had no compiler
or child processes and their exact tool handles ended with exit 143. Their source
snapshots are preserved; active compiler jobs were untouched. Updated production
and whole API qualification must use the final source hashes. Production/model
startup, stuck-reap quarantine, protected durability, whole-node bounds/overload
and default enablement remain required. Pooling is disabled. Compiler/container
reservations are not runtime resource bounds.

The preliminary passing two-lock checkpoint revision is preserved separately.
Final native/GNU gates pass on the single-lock implementation recorded above.
